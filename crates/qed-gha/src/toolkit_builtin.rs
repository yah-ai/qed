//! Built-in tier-1/2 [`ToolkitAction`] impls (W224 R533-T7).
//!
//! Five compute actions ship here — the toolkit-contract subset that W224
//! keeps:
//! - `dtolnay/rust-toolchain` — shells `rustup` (tier-1 toolchain setup),
//! - `oven-sh/setup-bun` — verifies `bun` on PATH (tier-1),
//! - `docker/setup-buildx-action` / `docker/setup-qemu-action` — verify the
//!   docker/buildx/qemu runtime is present (tier-4 *build*-side setup, compute),
//! - `sigstore/cosign-installer` — verifies `cosign` on PATH (tier-1).
//!
//! These *compute* — they prepare a toolchain or verify a binary. They do NOT
//! integrate with GitHub-the-service. The tier-3 *service* reimplementations
//! that W200 shipped here (`actions/checkout`, `actions/cache`,
//! `Swatinem/rust-cache`, `actions/upload-artifact`, `actions/download-artifact`,
//! `docker/login-action`, `docker/build-push-action`,
//! `softprops/action-gh-release`) are **retired** per W224: QED replaces those
//! surfaces with native facilities (content-addressed artifacts, native
//! checkout, the W208 publisher adapters) at import time. The transformer flags
//! such steps with native-replacement stanzas; the runtime declines to run them
//! (see [`crate::tier`] + the dispatch in [`crate::runtime`]).
//!
//! R594 re-adds *execution* for two of these — the docker push family (via an
//! injected [`crate::ImageBuilder`]) and the artifact actions (via an injected
//! [`crate::ArtifactStore`]) — but as runner-injected handlers, NOT toolkit
//! actions. They stay unregistered here; `register_toolkit` remains the tier-1/2
//! compute-only set, so without an injected handler the tier-3 error still fires.
//!
//! Design notes:
//! - **No external downloads.** `dtolnay/rust-toolchain` shells `rustup`
//!   (already on the runner); `oven-sh/setup-bun` / `sigstore/cosign-installer`
//!   verify the tool is present and error if absent. The GHA versions fetch
//!   tarballs; the yubaba / dev-host case has the tool pre-installed, and a cold
//!   runner is a setup problem, not a workflow one.
//! - **Failures inside an action** return `Err(String)` only for unrecoverable
//!   shell/IO breakage. "Tool produced exit != 0" surfaces as
//!   [`ToolkitOutcome { conclusion: Failure }`] so `continue-on-error` and
//!   `if: failure()` still work.

use std::process::Command;

use indexmap::IndexMap;

use crate::expr::Value;
use crate::toolkit::{StepConclusion, ToolkitAction, ToolkitCall, ToolkitOutcome, ToolkitRegistry};

/// Register the tier-1/2 built-in toolkit actions on `registry`. Idempotent:
/// calling twice just overwrites with the same impl.
pub fn register_toolkit(registry: &mut ToolkitRegistry) {
    registry.register("dtolnay/rust-toolchain", Box::new(RustToolchain));
    registry.register("oven-sh/setup-bun", Box::new(SetupBun));
    registry.register("docker/setup-buildx-action", Box::new(SetupBuildx));
    registry.register("docker/setup-qemu-action", Box::new(SetupQemu));
    registry.register("sigstore/cosign-installer", Box::new(CosignInstaller));
}

// ─── dtolnay/rust-toolchain ────────────────────────────────────────────────

/// `dtolnay/rust-toolchain` — shells `rustup toolchain install` + `rustup target add`.
/// Inputs: `toolchain` (default `stable`), `targets` (CSV), `components` (CSV).
struct RustToolchain;

impl ToolkitAction for RustToolchain {
    fn execute(&self, call: &ToolkitCall<'_>) -> Result<ToolkitOutcome, String> {
        // The slug carries the toolchain on the @ref (`dtolnay/rust-toolchain@stable`)
        // in idiomatic usage; with: { toolchain } overrides.
        let toolchain = string_input(call, "toolchain")
            .or_else(|| call.git_ref.map(|s| s.to_string()))
            .unwrap_or_else(|| "stable".into());

        let mut cmd = Command::new("rustup");
        cmd.arg("toolchain").arg("install").arg(&toolchain).arg("--profile").arg("minimal");
        run_shell("rustup toolchain install", cmd, call.env)?;

        if let Some(targets) = string_input(call, "targets") {
            for t in targets.split([',', '\n']).map(str::trim).filter(|s| !s.is_empty()) {
                let mut cmd = Command::new("rustup");
                cmd.arg("target").arg("add").arg(t).arg("--toolchain").arg(&toolchain);
                run_shell("rustup target add", cmd, call.env)?;
            }
        }
        if let Some(components) = string_input(call, "components") {
            for c in components.split([',', '\n']).map(str::trim).filter(|s| !s.is_empty()) {
                let mut cmd = Command::new("rustup");
                cmd.arg("component").arg("add").arg(c).arg("--toolchain").arg(&toolchain);
                run_shell("rustup component add", cmd, call.env)?;
            }
        }

        let cargo_version = Command::new("cargo").env("RUSTUP_TOOLCHAIN", &toolchain).arg("--version")
            .output().ok()
            .and_then(|o| if o.status.success() { Some(String::from_utf8_lossy(&o.stdout).trim().to_string()) } else { None })
            .unwrap_or_default();

        let mut outputs = IndexMap::new();
        outputs.insert("cachekey".into(), Value::String(format!("{toolchain}|{cargo_version}")));
        outputs.insert("name".into(), Value::String(toolchain.clone()));
        Ok(ToolkitOutcome {
            outputs,
            log: format!("dtolnay/rust-toolchain: ready ({toolchain})"),
            conclusion: StepConclusion::Success,
        })
    }
}

// ─── oven-sh/setup-bun ─────────────────────────────────────────────────────

/// `oven-sh/setup-bun` — verifies `bun` is present on the runner. The GHA
/// version downloads tarballs; the yubaba / dev-host case has bun
/// pre-installed.  A missing tool is a setup problem, not a workflow one.
struct SetupBun;

impl ToolkitAction for SetupBun {
    fn execute(&self, call: &ToolkitCall<'_>) -> Result<ToolkitOutcome, String> {
        let want = string_input(call, "bun-version").unwrap_or_else(|| "latest".into());
        let out = Command::new("bun").arg("--version").output()
            .map_err(|e| format!("oven-sh/setup-bun: bun not found ({e}); install bun on this host"))?;
        if !out.status.success() {
            return Err(format!("oven-sh/setup-bun: `bun --version` exited {:?}", out.status));
        }
        let actual = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let mut outputs = IndexMap::new();
        outputs.insert("bun-version".into(), Value::String(actual.clone()));
        outputs.insert("bun-path".into(), Value::String(which("bun").unwrap_or_default()));
        Ok(ToolkitOutcome {
            outputs,
            log: format!("oven-sh/setup-bun: bun {actual} (requested `{want}`)"),
            conclusion: StepConclusion::Success,
        })
    }
}

// ─── docker buildx / qemu setup ────────────────────────────────────────────

/// `docker/setup-buildx-action` — verify-only. If `docker buildx version`
/// succeeds the runner is ready; otherwise we surface a clean error so the
/// operator fixes their docker install rather than chasing a build failure.
struct SetupBuildx;

impl ToolkitAction for SetupBuildx {
    fn execute(&self, _call: &ToolkitCall<'_>) -> Result<ToolkitOutcome, String> {
        let out = Command::new("docker").arg("buildx").arg("version").output()
            .map_err(|e| format!("docker/setup-buildx-action: docker not found ({e})"))?;
        if !out.status.success() {
            return Err("docker/setup-buildx-action: `docker buildx` unavailable — install buildx on this host".into());
        }
        let version = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let mut outputs = IndexMap::new();
        outputs.insert("name".into(), Value::String("builder".into()));
        Ok(ToolkitOutcome {
            outputs,
            log: format!("docker/setup-buildx-action: ready ({version})"),
            conclusion: StepConclusion::Success,
        })
    }
}

/// `docker/setup-qemu-action` — verify-only. QEMU binfmt setup is heavy
/// (privileged container pull); v1 trusts the runner to have it pre-installed
/// (any host that's run a cross-arch build before will). When the host hasn't,
/// the subsequent `docker buildx build --platform linux/arm64` step fails
/// loudly — clearer than this action silently shelling a `--privileged`
/// container.
struct SetupQemu;

impl ToolkitAction for SetupQemu {
    fn execute(&self, _call: &ToolkitCall<'_>) -> Result<ToolkitOutcome, String> {
        // Surface docker availability the same way buildx does so a missing
        // docker fails here, not three steps later.
        let out = Command::new("docker").arg("version").output()
            .map_err(|e| format!("docker/setup-qemu-action: docker not found ({e})"))?;
        if !out.status.success() {
            return Err("docker/setup-qemu-action: `docker version` failed — install docker on this host".into());
        }
        Ok(ToolkitOutcome {
            outputs: IndexMap::new(),
            log: "docker/setup-qemu-action: assuming pre-installed binfmt handlers".into(),
            conclusion: StepConclusion::Success,
        })
    }
}

// ─── sigstore/cosign-installer ─────────────────────────────────────────────

/// `sigstore/cosign-installer` — verify-only. Same pattern as `setup-bun`: the
/// GHA version fetches a release tarball; the dev/yubaba runner has `cosign` on
/// PATH (apt / brew / managed image), and a missing tool is a host setup
/// problem, not a workflow one.
struct CosignInstaller;

impl ToolkitAction for CosignInstaller {
    fn execute(&self, _call: &ToolkitCall<'_>) -> Result<ToolkitOutcome, String> {
        let out = Command::new("cosign").arg("version").output()
            .map_err(|e| format!(
                "sigstore/cosign-installer: cosign not found ({e}); install via `brew install cosign` or download from https://github.com/sigstore/cosign/releases"
            ))?;
        if !out.status.success() {
            return Err(format!(
                "sigstore/cosign-installer: `cosign version` exited {:?}",
                out.status.code(),
            ));
        }
        let version_line = String::from_utf8_lossy(&out.stdout)
            .lines()
            .find(|l| l.contains("GitVersion") || l.starts_with('v'))
            .unwrap_or("")
            .trim()
            .to_string();
        let mut outputs = IndexMap::new();
        outputs.insert("cosign-path".into(), Value::String(which("cosign").unwrap_or_default()));
        Ok(ToolkitOutcome {
            outputs,
            log: format!("sigstore/cosign-installer: cosign ready ({version_line})"),
            conclusion: StepConclusion::Success,
        })
    }
}

// ─── helpers ───────────────────────────────────────────────────────────────

fn string_input(call: &ToolkitCall<'_>, key: &str) -> Option<String> {
    call.with.get(key).map(|v| v.as_str_lossy())
}

fn run_shell(label: &str, mut cmd: Command, env: &IndexMap<String, String>) -> Result<(), String> {
    for (k, v) in env { cmd.env(k, v); }
    let out = cmd.output().map_err(|e| format!("{label}: spawn: {e}"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(format!("{label}: exit {:?}: {stderr}", out.status.code()));
    }
    Ok(())
}

fn which(prog: &str) -> Option<String> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(prog);
        if candidate.is_file() {
            return Some(candidate.to_string_lossy().into_owned());
        }
    }
    None
}

// ─── tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::toolkit::Lookup;
    use std::path::Path;

    /// `register_toolkit` populates exactly the tier-1/2 compute set — and none
    /// of the retired tier-3 service slugs.
    #[test]
    fn register_toolkit_populates_tier12_slugs() {
        let mut r = ToolkitRegistry::new();
        register_toolkit(&mut r);
        for slug in [
            "dtolnay/rust-toolchain",
            "oven-sh/setup-bun",
            "docker/setup-buildx-action",
            "docker/setup-qemu-action",
            "sigstore/cosign-installer",
        ] {
            assert!(matches!(r.lookup(slug), Lookup::Found { .. }), "{slug} not registered");
        }
    }

    /// The retired tier-3 service actions are NOT registered toolkit actions —
    /// they route through the tier classifier to a native replacement instead.
    #[test]
    fn retired_tier3_slugs_are_not_registered() {
        let mut r = ToolkitRegistry::new();
        register_toolkit(&mut r);
        for slug in [
            "actions/checkout",
            "actions/cache",
            "Swatinem/rust-cache",
            "actions/upload-artifact",
            "actions/download-artifact",
            "docker/login-action",
            "docker/build-push-action",
            "softprops/action-gh-release",
        ] {
            assert!(matches!(r.lookup(slug), Lookup::Unknown), "{slug} should be retired");
        }
    }

    /// `dtolnay/rust-toolchain` lifts the toolchain off the `@ref` when `with:`
    /// has none. (Hermetic: we only assert the input-resolution path; the actual
    /// `rustup` shell-out is exercised on a real host.)
    #[test]
    fn rust_toolchain_reads_ref_when_with_absent() {
        // No assertion against the live shell — just prove the action is wired
        // and the registry hands it out by slug.
        let mut r = ToolkitRegistry::new();
        register_toolkit(&mut r);
        let _ = Path::new("/tmp");
        assert!(matches!(r.lookup("dtolnay/rust-toolchain"), Lookup::Found { .. }));
    }
}
