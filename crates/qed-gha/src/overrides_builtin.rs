//! Built-in [`Override`] impls for the W200 v1 action set.
//!
//! Ten slugs ship here: `actions/checkout`, `actions/cache`,
//! `actions/upload-artifact`, `actions/download-artifact`,
//! `Swatinem/rust-cache`, `dtolnay/rust-toolchain`, `oven-sh/setup-bun`,
//! `docker/setup-buildx-action`, `docker/setup-qemu-action`,
//! `docker/login-action`, and `docker/build-push-action`.
//! The first seven are F5's build-only subset; the docker family lands here
//! in F6 with a TOML-driven `registry_route` redirect (`ghcr.io` →
//! `registry.yah.dev`) so `release.yml`'s image jobs push to our surface
//! without YAML edits. cosign + gh-release land in F7/F8.
//!
//! Design notes:
//! - **Eager restore, deferred save.** `actions/cache` and `Swatinem/rust-cache`
//!   restore on hit and set `cache-hit`; saving the post-step state is left
//!   to a future post-step-hook addition. v1 workflows still build correctly,
//!   they just refill the cache cold each run.
//! - **No external downloads.** `dtolnay/rust-toolchain` shells `rustup` (already
//!   on the runner); `oven-sh/setup-bun` shells `bun` and errors if absent. The
//!   GHA versions fetch tarballs; the warden / dev-host case has the tool
//!   pre-installed, and a cold runner is a setup problem, not a workflow one.
//! - **Failures inside an override** return `Err(String)` only for unrecoverable
//!   shell/IO breakage. "Tool produced exit != 0" surfaces as
//!   [`OverrideOutcome { conclusion: Failure }`] so `continue-on-error` and
//!   `if: failure()` still work.

use std::path::{Path, PathBuf};
use std::process::Command;

use indexmap::IndexMap;

use crate::expr::Value;
use crate::overrides::{
    Override, OverrideCall, OverrideOutcome, OverrideRegistry, ProducedArtifact, StepConclusion,
};

/// Register the v1 built-in overrides on `registry`. Idempotent: calling twice
/// just overwrites with the same impl. Per-slug TOML config (cache dir,
/// registry routes) still wins via the existing config-blob path.
pub fn register_builtins(registry: &mut OverrideRegistry) {
    registry.register("actions/checkout", Box::new(Checkout));
    registry.register("actions/cache", Box::new(Cache));
    registry.register("actions/upload-artifact", Box::new(UploadArtifact));
    registry.register("actions/download-artifact", Box::new(DownloadArtifact));
    registry.register("Swatinem/rust-cache", Box::new(RustCache));
    registry.register("dtolnay/rust-toolchain", Box::new(RustToolchain));
    registry.register("oven-sh/setup-bun", Box::new(SetupBun));
    registry.register("docker/setup-buildx-action", Box::new(SetupBuildx));
    registry.register("docker/setup-qemu-action", Box::new(SetupQemu));
    registry.register("docker/login-action", Box::new(DockerLogin));
    registry.register("docker/build-push-action", Box::new(DockerBuildPush));
    registry.register("softprops/action-gh-release", Box::new(GhRelease));
    registry.register("sigstore/cosign-installer", Box::new(CosignInstaller));
}

// ─── actions/checkout ──────────────────────────────────────────────────────

/// `actions/checkout` — native git clone into `${workspace}/${path}`.
///
/// When `with.repository` is unset (or matches the current workspace's repo),
/// the step is a no-op: the workspace is already the checkout. When a
/// foreign repository is requested, shells `git clone` with shallow defaults.
struct Checkout;

impl Override for Checkout {
    fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
        let repo = string_input(call, "repository");
        let target_path = string_input(call, "path").unwrap_or_else(|| ".".into());
        let target = call.workspace.join(&target_path);

        // No repository → assume the workspace already contains the checkout.
        // This matches the most common release.yml usage: `uses: actions/checkout@v4`
        // with no inputs, when the runner already has the source.
        if repo.as_deref().map(str::is_empty).unwrap_or(true) {
            let mut outputs = IndexMap::new();
            outputs.insert("ref".into(), Value::String(string_input(call, "ref").unwrap_or_default()));
            outputs.insert("commit".into(), Value::String(String::new()));
            return Ok(OverrideOutcome {
                outputs,
                log: format!("actions/checkout: workspace at {} (no remote clone)", target.display()),
                conclusion: StepConclusion::Success,
            produced: Vec::new(),
            });
        }

        let repo = repo.unwrap();
        let git_url = format!("https://github.com/{repo}.git");
        let git_ref = string_input(call, "ref");
        let fetch_depth = string_input(call, "fetch-depth").unwrap_or_else(|| "1".into());

        // Wipe target if it already exists — checkout's contract is a fresh tree.
        if target.exists() {
            std::fs::remove_dir_all(&target).map_err(|e| format!("clean {}: {e}", target.display()))?;
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
        }

        let mut cmd = Command::new("git");
        cmd.arg("clone");
        if fetch_depth != "0" {
            cmd.arg("--depth").arg(&fetch_depth);
        }
        if let Some(r) = &git_ref {
            cmd.arg("--branch").arg(r);
        }
        cmd.arg(&git_url).arg(&target);
        run_capture("actions/checkout", cmd, &target_outputs(&target, git_ref.as_deref()))
    }
}

fn target_outputs(target: &Path, git_ref: Option<&str>) -> IndexMap<String, Value> {
    let mut m = IndexMap::new();
    m.insert("ref".into(), Value::String(git_ref.unwrap_or("").to_string()));
    m.insert("commit".into(), Value::String(String::new()));
    let _ = target;
    m
}

// ─── actions/cache + Swatinem/rust-cache ───────────────────────────────────

/// `actions/cache` — local-fs backend. Restore-only in v1.
///
/// Config:
/// - `config.backend = "local-fs" | "no-op"` (default `local-fs`)
/// - `config.dir = "<path>"` (default `${HOME}/.cache/yah-qed/gha`)
struct Cache;

impl Override for Cache {
    fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
        let key = string_input(call, "key").ok_or_else(|| "actions/cache: missing `key`".to_string())?;
        let path_input = string_input(call, "path").ok_or_else(|| "actions/cache: missing `path`".to_string())?;
        let paths: Vec<String> = path_input
            .lines()
            .filter_map(|s| {
                let t = s.trim();
                if t.is_empty() { None } else { Some(t.to_string()) }
            })
            .collect();

        let backend = config_string(call, "backend").unwrap_or_else(|| "local-fs".into());
        if backend == "no-op" {
            return Ok(no_hit_outcome("actions/cache: no-op backend"));
        }

        let cache_dir = resolve_cache_dir(call);
        let key_dir = cache_dir.join(&key);

        if !key_dir.exists() {
            return Ok(no_hit_outcome(&format!("actions/cache: miss for key `{key}`")));
        }

        for p in &paths {
            let dst = call.workspace.join(p);
            let src = key_dir.join(p);
            if !src.exists() { continue }
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
            }
            copy_tree(&src, &dst).map_err(|e| format!("restore {p}: {e}"))?;
        }

        let mut outputs = IndexMap::new();
        outputs.insert("cache-hit".into(), Value::String("true".into()));
        Ok(OverrideOutcome {
            outputs,
            log: format!("actions/cache: restored {} path(s) for key `{key}`", paths.len()),
            conclusion: StepConclusion::Success,
            produced: Vec::new(),
        })
    }
}

/// `Swatinem/rust-cache` — same restore-only semantics as [`Cache`], with the
/// effective key derived from `Cargo.lock` digest + `with.key` + the active
/// rust toolchain. Workflows pass `with.workspaces` to scope per-crate caches;
/// v1 honours the first workspace only.
struct RustCache;

impl Override for RustCache {
    fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
        let shared_key = string_input(call, "shared-key").unwrap_or_default();
        let extra_key = string_input(call, "key").unwrap_or_default();
        let workspaces = string_input(call, "workspaces").unwrap_or_else(|| ".".into());
        let ws_root = workspaces.lines().next().unwrap_or(".").trim().to_string();

        let lock_digest = digest_file(&call.workspace.join(&ws_root).join("Cargo.lock"))
            .unwrap_or_else(|_| "no-lock".into());
        let toolchain = active_toolchain().unwrap_or_else(|| "unknown".into());
        let effective_key = format!("rust-cache-{toolchain}-{shared_key}-{extra_key}-{lock_digest}");

        let cache_dir = resolve_cache_dir(call);
        let key_dir = cache_dir.join(&effective_key);

        // Swatinem caches target/ and ~/.cargo registry/git — we only restore
        // target/ here; cargo's home is shared across workflows, not per-key.
        let target_dir = PathBuf::from(&ws_root).join("target");
        let cached_target = key_dir.join(&target_dir);

        if !cached_target.exists() {
            return Ok(no_hit_outcome(&format!(
                "Swatinem/rust-cache: miss for `{effective_key}`"
            )));
        }

        let dst = call.workspace.join(&target_dir);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
        }
        copy_tree(&cached_target, &dst).map_err(|e| format!("restore target/: {e}"))?;

        let mut outputs = IndexMap::new();
        outputs.insert("cache-hit".into(), Value::String("true".into()));
        Ok(OverrideOutcome {
            outputs,
            log: format!("Swatinem/rust-cache: restored target/ for `{effective_key}`"),
            conclusion: StepConclusion::Success,
            produced: Vec::new(),
        })
    }
}

fn resolve_cache_dir(call: &OverrideCall<'_>) -> PathBuf {
    if let Some(dir) = config_string(call, "dir") {
        return PathBuf::from(dir);
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".cache/yah-qed/gha");
    }
    std::env::temp_dir().join("yah-qed-gha-cache")
}

fn no_hit_outcome(log: &str) -> OverrideOutcome {
    let mut outputs = IndexMap::new();
    outputs.insert("cache-hit".into(), Value::String("false".into()));
    OverrideOutcome {
        outputs,
        log: log.into(),
        conclusion: StepConclusion::Success,
        produced: Vec::new(),
    }
}

fn digest_file(path: &Path) -> std::io::Result<String> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let bytes = std::fs::read(path)?;
    let mut h = DefaultHasher::new();
    bytes.hash(&mut h);
    Ok(format!("{:016x}", h.finish()))
}

fn active_toolchain() -> Option<String> {
    let out = Command::new("rustc").arg("--version").output().ok()?;
    if !out.status.success() { return None }
    let s = String::from_utf8_lossy(&out.stdout);
    s.split_whitespace().nth(1).map(|v| v.to_string())
}

// ─── upload-artifact + download-artifact ───────────────────────────────────

/// `actions/upload-artifact` — copy `with.path` into `${workspace}/.qed-artifacts/${name}/`.
struct UploadArtifact;

impl Override for UploadArtifact {
    fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
        let name = string_input(call, "name").ok_or_else(|| "actions/upload-artifact: missing `name`".to_string())?;
        let path_input = string_input(call, "path").ok_or_else(|| "actions/upload-artifact: missing `path`".to_string())?;

        let dest_root = call.workspace.join(".qed-artifacts").join(&name);
        if dest_root.exists() {
            std::fs::remove_dir_all(&dest_root).map_err(|e| format!("clean {}: {e}", dest_root.display()))?;
        }
        std::fs::create_dir_all(&dest_root).map_err(|e| format!("mkdir {}: {e}", dest_root.display()))?;

        let mut count = 0usize;
        for raw in path_input.lines() {
            let p = raw.trim();
            if p.is_empty() { continue }
            let src = call.workspace.join(p);
            if !src.exists() {
                return Err(format!("actions/upload-artifact: path `{p}` does not exist"));
            }
            let dst = dest_root.join(src.file_name().unwrap_or_else(|| std::ffi::OsStr::new("artifact")));
            copy_tree(&src, &dst).map_err(|e| format!("copy {p}: {e}"))?;
            count += 1;
        }

        let mut outputs = IndexMap::new();
        outputs.insert("artifact-id".into(), Value::String(name.clone()));
        outputs.insert("artifact-url".into(), Value::String(format!("qed-artifact://{name}")));
        Ok(OverrideOutcome {
            outputs,
            log: format!("actions/upload-artifact: stored {count} path(s) under `{name}`"),
            conclusion: StepConclusion::Success,
            produced: Vec::new(),
        })
    }
}

/// `actions/download-artifact` — copy from `${workspace}/.qed-artifacts/${name}/`
/// to `with.path` (default `${workspace}`). With no `name`, downloads all
/// artifacts into separate subdirs by name.
struct DownloadArtifact;

impl Override for DownloadArtifact {
    fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
        let name = string_input(call, "name");
        let dest = string_input(call, "path").unwrap_or_else(|| ".".into());
        let dest_dir = call.workspace.join(&dest);
        std::fs::create_dir_all(&dest_dir).map_err(|e| format!("mkdir {}: {e}", dest_dir.display()))?;

        let root = call.workspace.join(".qed-artifacts");
        if !root.exists() {
            return Err("actions/download-artifact: no artifacts stored yet".into());
        }

        let mut count = 0usize;
        if let Some(name) = name {
            let src = root.join(&name);
            if !src.exists() {
                return Err(format!("actions/download-artifact: artifact `{name}` not found"));
            }
            copy_tree_contents(&src, &dest_dir).map_err(|e| format!("restore {name}: {e}"))?;
            count = 1;
        } else {
            for entry in std::fs::read_dir(&root).map_err(|e| format!("scan artifacts: {e}"))? {
                let entry = entry.map_err(|e| format!("scan entry: {e}"))?;
                let entry_name = entry.file_name();
                let dst = dest_dir.join(&entry_name);
                std::fs::create_dir_all(&dst).map_err(|e| format!("mkdir {}: {e}", dst.display()))?;
                copy_tree_contents(&entry.path(), &dst).map_err(|e| format!("restore {}: {e}", entry_name.to_string_lossy()))?;
                count += 1;
            }
        }

        let outputs = IndexMap::new();
        Ok(OverrideOutcome {
            outputs,
            log: format!("actions/download-artifact: restored {count} artifact(s) into {}", dest_dir.display()),
            conclusion: StepConclusion::Success,
            produced: Vec::new(),
        })
    }
}

// ─── dtolnay/rust-toolchain ────────────────────────────────────────────────

/// `dtolnay/rust-toolchain` — shells `rustup toolchain install` + `rustup target add`.
/// Inputs: `toolchain` (default `stable`), `targets` (CSV), `components` (CSV).
struct RustToolchain;

impl Override for RustToolchain {
    fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
        // The slug carries the toolchain on the @ref (`dtolnay/rust-toolchain@stable`)
        // in idiomatic usage; with: { toolchain } overrides.
        let toolchain = string_input(call, "toolchain")
            .or_else(|| call.git_ref.map(|s| s.to_string()))
            .unwrap_or_else(|| "stable".into());

        let mut cmd = Command::new("rustup");
        cmd.arg("toolchain").arg("install").arg(&toolchain).arg("--profile").arg("minimal");
        run_shell("rustup toolchain install", cmd, &call.env)?;

        if let Some(targets) = string_input(call, "targets") {
            for t in targets.split([',', '\n']).map(str::trim).filter(|s| !s.is_empty()) {
                let mut cmd = Command::new("rustup");
                cmd.arg("target").arg("add").arg(t).arg("--toolchain").arg(&toolchain);
                run_shell("rustup target add", cmd, &call.env)?;
            }
        }
        if let Some(components) = string_input(call, "components") {
            for c in components.split([',', '\n']).map(str::trim).filter(|s| !s.is_empty()) {
                let mut cmd = Command::new("rustup");
                cmd.arg("component").arg("add").arg(c).arg("--toolchain").arg(&toolchain);
                run_shell("rustup component add", cmd, &call.env)?;
            }
        }

        let cargo_version = Command::new("cargo").env("RUSTUP_TOOLCHAIN", &toolchain).arg("--version")
            .output().ok()
            .and_then(|o| if o.status.success() { Some(String::from_utf8_lossy(&o.stdout).trim().to_string()) } else { None })
            .unwrap_or_default();

        let mut outputs = IndexMap::new();
        outputs.insert("cachekey".into(), Value::String(format!("{toolchain}|{cargo_version}")));
        outputs.insert("name".into(), Value::String(toolchain.clone()));
        Ok(OverrideOutcome {
            outputs,
            log: format!("dtolnay/rust-toolchain: ready ({toolchain})"),
            conclusion: StepConclusion::Success,
            produced: Vec::new(),
        })
    }
}

// ─── oven-sh/setup-bun ─────────────────────────────────────────────────────

/// `oven-sh/setup-bun` — verifies `bun` is present on the runner. The GHA
/// version downloads tarballs; the warden / dev-host case has bun
/// pre-installed.  A missing tool is a setup problem, not a workflow one.
struct SetupBun;

impl Override for SetupBun {
    fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
        let want = string_input(call, "bun-version").unwrap_or_else(|| "latest".into());
        let out = Command::new("bun").arg("--version").output()
            .map_err(|e| format!("oven-sh/setup-bun: bun not found ({e}); install bun on this host"))?;
        if !out.status.success() {
            return Err(format!("oven-sh/setup-bun: `bun --version` exited {:?}", out.status));
        }
        let actual = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let _ = call; // env passes through; nothing to set
        let mut outputs = IndexMap::new();
        outputs.insert("bun-version".into(), Value::String(actual.clone()));
        outputs.insert("bun-path".into(), Value::String(which("bun").unwrap_or_default()));
        Ok(OverrideOutcome {
            outputs,
            log: format!("oven-sh/setup-bun: bun {actual} (requested `{want}`)"),
            conclusion: StepConclusion::Success,
            produced: Vec::new(),
        })
    }
}

// ─── docker family ─────────────────────────────────────────────────────────

/// `docker/setup-buildx-action` — verify-only. If `docker buildx version`
/// succeeds the runner is ready; otherwise we surface a clean error so the
/// operator fixes their docker install rather than chasing a build failure.
struct SetupBuildx;

impl Override for SetupBuildx {
    fn execute(&self, _call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
        let out = Command::new("docker").arg("buildx").arg("version").output()
            .map_err(|e| format!("docker/setup-buildx-action: docker not found ({e})"))?;
        if !out.status.success() {
            return Err("docker/setup-buildx-action: `docker buildx` unavailable — install buildx on this host".into());
        }
        let version = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let mut outputs = IndexMap::new();
        outputs.insert("name".into(), Value::String("builder".into()));
        Ok(OverrideOutcome {
            outputs,
            log: format!("docker/setup-buildx-action: ready ({version})"),
            conclusion: StepConclusion::Success,
            produced: Vec::new(),
        })
    }
}

/// `docker/setup-qemu-action` — verify-only. QEMU binfmt setup is heavy
/// (privileged container pull); v1 trusts the runner to have it pre-installed
/// (any host that's run a cross-arch build before will). When the host hasn't,
/// the subsequent `docker buildx build --platform linux/arm64` step fails
/// loudly — clearer than this override silently shelling a `--privileged`
/// container.
struct SetupQemu;

impl Override for SetupQemu {
    fn execute(&self, _call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
        // Surface docker availability the same way buildx does so a missing
        // docker fails here, not three steps later.
        let out = Command::new("docker").arg("version").output()
            .map_err(|e| format!("docker/setup-qemu-action: docker not found ({e})"))?;
        if !out.status.success() {
            return Err("docker/setup-qemu-action: `docker version` failed — install docker on this host".into());
        }
        Ok(OverrideOutcome {
            outputs: IndexMap::new(),
            log: "docker/setup-qemu-action: assuming pre-installed binfmt handlers".into(),
            conclusion: StepConclusion::Success,
            produced: Vec::new(),
        })
    }
}

/// `docker/login-action` — applies the registry redirect from
/// `config.registry_route` before shelling `docker login`. An empty password
/// (the `${{ secrets.GITHUB_TOKEN }}` case in `release.yml` — QED doesn't
/// resolve that secret) is treated as "host already logged in" rather than a
/// hard error so the subsequent build-push can still attempt a push and the
/// failure (if any) surfaces at the push site with the real registry's error
/// message.
struct DockerLogin;

impl Override for DockerLogin {
    fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
        let raw_registry = string_input(call, "registry").unwrap_or_default();
        let registry = redirect_registry(&raw_registry, call.config);
        let username = string_input(call, "username").unwrap_or_default();
        let password = string_input(call, "password").unwrap_or_default();

        if password.is_empty() {
            return Ok(OverrideOutcome {
                outputs: IndexMap::new(),
                log: format!(
                    "docker/login-action: skipped (empty password — host's existing docker creds for `{registry}` will be used)"
                ),
                conclusion: StepConclusion::Success,
            produced: Vec::new(),
            });
        }

        let mut cmd = Command::new("docker");
        cmd.arg("login").arg(&registry);
        if !username.is_empty() {
            cmd.arg("-u").arg(&username);
        }
        cmd.arg("--password-stdin");
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| format!("docker/login-action: spawn: {e}"))?;
        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write;
            stdin.write_all(password.as_bytes())
                .map_err(|e| format!("docker/login-action: write stdin: {e}"))?;
        }
        let out = child.wait_with_output().map_err(|e| format!("docker/login-action: wait: {e}"))?;
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        let conclusion = if out.status.success() { StepConclusion::Success } else { StepConclusion::Failure };
        Ok(OverrideOutcome {
            outputs: IndexMap::new(),
            log: format!("docker/login-action: target=`{registry}` user=`{username}`\n{stdout}\n{stderr}"),
            conclusion,
            produced: Vec::new(),
        })
    }
}

/// `docker/build-push-action` — applies registry redirect to each tag, then
/// shells `docker buildx build` with `--push` (when `with.push` is truthy).
/// Captures `digest` + `imageid` outputs via `--metadata-file` so downstream
/// steps that read `steps.build.outputs.digest` (cosign sign, the per-binary
/// DIGEST env block in `release.yml`) keep working.
struct DockerBuildPush;

impl Override for DockerBuildPush {
    fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
        let context = string_input(call, "context").unwrap_or_else(|| ".".into());
        let file = string_input(call, "file");
        let push = string_input(call, "push").map(|s| s == "true").unwrap_or(false);
        let load = string_input(call, "load").map(|s| s == "true").unwrap_or(false);
        let provenance = string_input(call, "provenance");
        let sbom = string_input(call, "sbom");
        let platforms = string_input(call, "platforms");
        let raw_tags = string_input(call, "tags").unwrap_or_default();
        let tags: Vec<String> = raw_tags
            .lines()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|t| redirect_image_ref(t, call.config))
            .collect();
        if tags.is_empty() && push {
            return Err("docker/build-push-action: push=true but no `tags` provided".into());
        }
        let build_args = collect_build_args(call);

        let metadata_dir = tempfile::tempdir()
            .map_err(|e| format!("docker/build-push-action: tempdir: {e}"))?;
        let metadata_path = metadata_dir.path().join("metadata.json");

        let mut cmd = Command::new("docker");
        cmd.arg("buildx").arg("build");
        if push { cmd.arg("--push"); }
        if load { cmd.arg("--load"); }
        if let Some(p) = &platforms {
            cmd.arg("--platform").arg(p);
        }
        if let Some(f) = &file {
            cmd.arg("-f").arg(f);
        }
        if let Some(p) = &provenance {
            cmd.arg("--provenance").arg(p);
        }
        if let Some(s) = &sbom {
            cmd.arg("--sbom").arg(s);
        }
        for (k, v) in &build_args {
            cmd.arg("--build-arg").arg(format!("{k}={v}"));
        }
        for t in &tags {
            cmd.arg("-t").arg(t);
        }
        cmd.arg("--metadata-file").arg(&metadata_path);
        cmd.arg(call.workspace.join(&context));
        for (k, v) in call.env { cmd.env(k, v); }

        let out = cmd.output().map_err(|e| format!("docker/build-push-action: spawn: {e}"))?;
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        let conclusion = if out.status.success() { StepConclusion::Success } else { StepConclusion::Failure };

        let mut outputs = IndexMap::new();
        if conclusion == StepConclusion::Success {
            if let Ok(meta_json) = std::fs::read_to_string(&metadata_path) {
                if let Some((digest, imageid)) = parse_buildx_metadata(&meta_json) {
                    if let Some(d) = digest { outputs.insert("digest".into(), Value::String(d)); }
                    if let Some(i) = imageid { outputs.insert("imageid".into(), Value::String(i)); }
                }
            }
        }
        // tags is what downstream pipelines need for cosign sign (`<repo>@<digest>`).
        outputs.insert("metadata".into(), Value::String(
            std::fs::read_to_string(&metadata_path).unwrap_or_default(),
        ));

        Ok(OverrideOutcome {
            outputs,
            log: format!(
                "docker/build-push-action: tags=[{}] push={} platforms={}\n{stdout}\n{stderr}",
                tags.join(", "),
                push,
                platforms.unwrap_or_else(|| "(host)".into()),
            ),
            conclusion,
            produced: Vec::new(),
        })
    }
}

fn collect_build_args(call: &OverrideCall<'_>) -> IndexMap<String, String> {
    let mut out = IndexMap::new();
    let raw = string_input(call, "build-args").unwrap_or_default();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() { continue }
        if let Some((k, v)) = line.split_once('=') {
            out.insert(k.trim().to_string(), v.to_string());
        }
    }
    out
}

/// Apply `config.registry_route` to a bare registry host. Empty / unknown
/// registries fall through unchanged.
fn redirect_registry(raw: &str, config: &Value) -> String {
    let Value::Object(cfg) = config else { return raw.to_string() };
    let Some(Value::Object(routes)) = cfg.get("registry_route") else { return raw.to_string() };
    if let Some(Value::String(target)) = routes.get(raw) {
        return target.clone();
    }
    raw.to_string()
}

/// Apply `config.registry_route` to a fully-qualified image ref like
/// `ghcr.io/yah-ai/yah-base:latest`. Only the registry host is rewritten; the
/// repo and tag are preserved verbatim.
fn redirect_image_ref(raw: &str, config: &Value) -> String {
    let Value::Object(cfg) = config else { return raw.to_string() };
    let Some(Value::Object(routes)) = cfg.get("registry_route") else { return raw.to_string() };
    // Split on first `/`. If the prefix matches a route key, swap it.
    let Some((host, rest)) = raw.split_once('/') else { return raw.to_string() };
    match routes.get(host) {
        Some(Value::String(target)) => format!("{target}/{rest}"),
        _ => raw.to_string(),
    }
}

/// Pluck `containerimage.digest` + `containerimage.config.digest` out of
/// `docker buildx --metadata-file` output. The shape varies slightly between
/// single-platform and multi-platform builds; we tolerate both.
fn parse_buildx_metadata(json: &str) -> Option<(Option<String>, Option<String>)> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let digest = v.get("containerimage.digest")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());
    let imageid = v.get("containerimage.config.digest")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());
    Some((digest, imageid))
}

// ─── sigstore/cosign-installer ─────────────────────────────────────────────

/// `sigstore/cosign-installer` — verify-only. Same pattern as `setup-bun`:
/// the GHA version fetches a release tarball; the dev/warden runner has
/// `cosign` on PATH (apt / brew / managed image), and a missing tool is a
/// host setup problem, not a workflow one.
///
/// Note on signing semantics in QED-mode vs GHA-mode:
/// - GHA mode (native runner) — `cosign sign --yes <ref>` succeeds via the
///   keyless OIDC flow against `token.actions.githubusercontent.com`. The
///   resulting attestation embeds the workflow-file identity
///   (`https://github.com/yah-ai/yah/.github/workflows/release.yml@<ref>`),
///   which the verifier regex in `task::default_image::pull` already accepts.
///   The identity is keyed on the workflow URL, not the pushed registry,
///   so F6's `ghcr.io` → `registry.yah.dev` redirect needs no consumer-side
///   change.
/// - QED mode (warden / local) — no GHA OIDC token is available, so
///   `cosign sign --yes` either drops into the interactive browser flow or
///   fails. Wiring a QED-managed OIDC path (e.g. a workload-identity token
///   minted from camp's keystore) is out of scope for F8; the v1 expectation
///   is that releases sign while running on GHA, and a QED-side run will
///   `cosign sign` as a best-effort step (failure surfaces in the run log
///   without blocking downstream pulls — same behaviour as GHA today).
struct CosignInstaller;

impl Override for CosignInstaller {
    fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
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
        let _ = call;
        Ok(OverrideOutcome {
            outputs,
            log: format!("sigstore/cosign-installer: cosign ready ({version_line})"),
            conclusion: StepConclusion::Success,
            produced: Vec::new(),
        })
    }
}

// ─── softprops/action-gh-release ───────────────────────────────────────────

/// `softprops/action-gh-release` — stages release tarballs for the parent
/// QED step's `Outcome::Publish` collection. No GitHub call: in QED the same
/// archives flow to `cdn.yah.dev` (W160 release model). Each matched file
/// becomes one [`ProducedArtifact`] with `binary` derived from the leading
/// dash-delimited segment of the filename stem and `triple` parsed from the
/// trailing target-triple segment (when present).
///
/// Honoured `with:` inputs: `files` (newline / glob), `tag_name`,
/// `fail_on_unmatched_files`. The version pin comes from `tag_name`;
/// downstream `publish.rs` uses that as the channel sub-path.
struct GhRelease;

impl Override for GhRelease {
    fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
        let raw_files = string_input(call, "files")
            .ok_or_else(|| "softprops/action-gh-release: missing `files`".to_string())?;
        let tag = string_input(call, "tag_name").unwrap_or_default();
        let fail_unmatched = string_input(call, "fail_on_unmatched_files")
            .map(|s| s == "true")
            .unwrap_or(false);

        let mut produced = Vec::new();
        let mut matched = 0usize;
        let mut missing: Vec<String> = Vec::new();

        for line in raw_files.lines() {
            let pattern = line.trim();
            if pattern.is_empty() { continue }
            let matches = expand_workspace_glob(call.workspace, pattern);
            if matches.is_empty() {
                missing.push(pattern.into());
                continue;
            }
            for path in matches {
                let rel = path.strip_prefix(call.workspace).unwrap_or(&path).to_string_lossy().into_owned();
                let filename = path.file_name().and_then(|f| f.to_str()).unwrap_or_default();
                let (binary, triple) = parse_release_filename(filename);
                produced.push(ProducedArtifact {
                    binary,
                    path: rel,
                    triple,
                });
                matched += 1;
            }
        }

        if fail_unmatched && !missing.is_empty() {
            return Err(format!(
                "softprops/action-gh-release: fail_on_unmatched_files=true but {} pattern(s) matched nothing: {}",
                missing.len(),
                missing.join(", "),
            ));
        }

        let mut outputs = IndexMap::new();
        outputs.insert("upload_url".into(), Value::String(format!("qed-release://{tag}")));
        // `url` is the GHA output the workflow's downstream steps read; populate
        // to the channel URL the publisher will mint at sync time.
        outputs.insert("url".into(), Value::String(format!("https://cdn.yah.dev/releases/{tag}")));
        Ok(OverrideOutcome {
            outputs,
            log: format!(
                "softprops/action-gh-release: staged {matched} artifact(s) for tag `{tag}`{}",
                if missing.is_empty() {
                    String::new()
                } else {
                    format!(" ({} pattern(s) matched nothing)", missing.len())
                },
            ),
            conclusion: StepConclusion::Success,
            produced,
        })
    }
}

/// Split a release filename into `(binary, triple)`.
///
/// Convention from `release.yml`:
///   `cli-v0.8.10-x86_64-unknown-linux-musl.tar.gz`
///    └ binary    └ tag            └ triple    └ archive ext
///
/// We strip the archive extension first, then split off the leading `binary`
/// segment, then treat anything after the tag as the triple. Filenames that
/// don't match this shape fall back to `(stem, None)`.
fn parse_release_filename(name: &str) -> (String, Option<String>) {
    // Strip known archive extensions iteratively (.tar.gz, .tar.xz, .tgz, .zip).
    let stem = strip_archive_ext(name);
    let parts: Vec<&str> = stem.splitn(3, '-').collect();
    match parts.as_slice() {
        [binary, _tag, triple_tail] => (binary.to_string(), Some((*triple_tail).to_string())),
        [binary, _rest] => (binary.to_string(), None),
        _ => (stem.to_string(), None),
    }
}

fn strip_archive_ext(name: &str) -> &str {
    for ext in [".tar.gz", ".tar.xz", ".tar.bz2", ".tgz", ".zip"] {
        if let Some(stem) = name.strip_suffix(ext) {
            return stem;
        }
    }
    name
}

/// Workspace-rooted glob expansion. Supports `*` and `?` in the final segment;
/// anything else resolves as a literal path (`release.yml` uses single-file
/// patterns, so this is enough for v1).
fn expand_workspace_glob(workspace: &Path, pattern: &str) -> Vec<PathBuf> {
    let full = workspace.join(pattern);
    if !pattern.contains('*') && !pattern.contains('?') {
        return if full.exists() { vec![full] } else { vec![] };
    }
    let parent = full.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| workspace.to_path_buf());
    let needle = full.file_name().and_then(|f| f.to_str()).unwrap_or("");
    let Ok(rd) = std::fs::read_dir(&parent) else { return vec![] };
    let mut out = Vec::new();
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if glob_match(needle, &name) {
            out.push(entry.path());
        }
    }
    out
}

/// Minimal `*` + `?` glob matcher. No `**`, no character classes — `release.yml`'s
/// `with: files:` patterns are single tokens.
fn glob_match(pattern: &str, text: &str) -> bool {
    fn go(p: &[u8], t: &[u8]) -> bool {
        match (p.first(), t.first()) {
            (None, None) => true,
            (Some(b'*'), _) => go(&p[1..], t) || (!t.is_empty() && go(p, &t[1..])),
            (Some(b'?'), Some(_)) => go(&p[1..], &t[1..]),
            (Some(pc), Some(tc)) if pc == tc => go(&p[1..], &t[1..]),
            _ => false,
        }
    }
    go(pattern.as_bytes(), text.as_bytes())
}

// ─── helpers ───────────────────────────────────────────────────────────────

fn string_input(call: &OverrideCall<'_>, key: &str) -> Option<String> {
    call.with.get(key).map(|v| v.as_str_lossy())
}

fn config_string(call: &OverrideCall<'_>, key: &str) -> Option<String> {
    if let Value::Object(m) = call.config {
        m.get(key).map(|v| v.as_str_lossy())
    } else {
        None
    }
}

fn copy_tree(src: &Path, dst: &Path) -> std::io::Result<()> {
    let meta = std::fs::symlink_metadata(src)?;
    if meta.file_type().is_dir() {
        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            copy_tree(&entry.path(), &dst.join(entry.file_name()))?;
        }
        Ok(())
    } else if meta.file_type().is_symlink() {
        let target = std::fs::read_link(src)?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, dst)?;
        #[cfg(not(unix))]
        std::fs::copy(src, dst).map(|_| ())?;
        Ok(())
    } else {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(src, dst).map(|_| ())
    }
}

fn copy_tree_contents(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        copy_tree(&entry.path(), &dst.join(entry.file_name()))?;
    }
    Ok(())
}

fn run_capture(
    slug: &str,
    mut cmd: Command,
    seed_outputs: &IndexMap<String, Value>,
) -> Result<OverrideOutcome, String> {
    let out = cmd.output().map_err(|e| format!("{slug}: spawn: {e}"))?;
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    let conclusion = if out.status.success() {
        StepConclusion::Success
    } else {
        StepConclusion::Failure
    };
    let log = format!("{slug}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}");
    Ok(OverrideOutcome {
        outputs: seed_outputs.clone(),
        log,
        conclusion,
        produced: Vec::new(),
    })
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
    use crate::overrides::Lookup;
    use std::fs;
    use tempfile::TempDir;

    fn call<'a>(
        slug: &'a str,
        with: &'a IndexMap<String, Value>,
        env: &'a IndexMap<String, String>,
        workspace: &'a Path,
        config: &'a Value,
    ) -> OverrideCall<'a> {
        OverrideCall {
            slug,
            git_ref: None,
            with,
            env,
            workspace,
            config,
        }
    }

    fn empty_env() -> IndexMap<String, String> { IndexMap::new() }

    fn empty_config() -> Value { Value::Object(IndexMap::new()) }

    #[test]
    fn register_builtins_populates_all_v1_slugs() {
        let mut r = OverrideRegistry::new();
        register_builtins(&mut r);
        for slug in [
            "actions/checkout",
            "actions/cache",
            "actions/upload-artifact",
            "actions/download-artifact",
            "Swatinem/rust-cache",
            "dtolnay/rust-toolchain",
            "oven-sh/setup-bun",
            "docker/setup-buildx-action",
            "docker/setup-qemu-action",
            "docker/login-action",
            "docker/build-push-action",
            "softprops/action-gh-release",
            "sigstore/cosign-installer",
        ] {
            assert!(matches!(r.lookup(slug), Lookup::Found { .. }), "{slug} not registered");
        }
    }

    #[test]
    fn checkout_no_repo_is_noop() {
        // Most common release.yml shape: `uses: actions/checkout@v4` with no
        // inputs. The workspace already holds the source; override succeeds
        // without touching the tree.
        let tmp = TempDir::new().unwrap();
        let with = IndexMap::new();
        let env = empty_env();
        let cfg = empty_config();
        let c = call("actions/checkout", &with, &env, tmp.path(), &cfg);
        let out = Checkout.execute(&c).expect("noop checkout");
        assert_eq!(out.conclusion, StepConclusion::Success);
        assert!(out.log.contains("no remote clone"));
    }

    #[test]
    fn cache_no_op_backend_short_circuits() {
        let tmp = TempDir::new().unwrap();
        let mut with = IndexMap::new();
        with.insert("key".into(), Value::String("k1".into()));
        with.insert("path".into(), Value::String("target".into()));
        let env = empty_env();
        let mut cfg_inner = IndexMap::new();
        cfg_inner.insert("backend".into(), Value::String("no-op".into()));
        let cfg = Value::Object(cfg_inner);
        let c = call("actions/cache", &with, &env, tmp.path(), &cfg);
        let out = Cache.execute(&c).expect("no-op ok");
        assert_eq!(out.conclusion, StepConclusion::Success);
        assert_eq!(out.outputs.get("cache-hit"), Some(&Value::String("false".into())));
    }

    #[test]
    fn cache_miss_then_pre_seeded_hit_restores() {
        let workspace = TempDir::new().unwrap();
        let cache_root = TempDir::new().unwrap();

        let mut with = IndexMap::new();
        with.insert("key".into(), Value::String("the-key".into()));
        with.insert("path".into(), Value::String("artifacts".into()));
        let env = empty_env();
        let mut cfg_inner = IndexMap::new();
        cfg_inner.insert("backend".into(), Value::String("local-fs".into()));
        cfg_inner.insert("dir".into(), Value::String(cache_root.path().to_string_lossy().into()));
        let cfg = Value::Object(cfg_inner);

        // Miss.
        let c = call("actions/cache", &with, &env, workspace.path(), &cfg);
        let out = Cache.execute(&c).expect("first call");
        assert_eq!(out.outputs.get("cache-hit"), Some(&Value::String("false".into())));

        // Pre-seed the cache (simulating a previous successful save).
        let seeded = cache_root.path().join("the-key").join("artifacts");
        fs::create_dir_all(&seeded).unwrap();
        fs::write(seeded.join("blob.txt"), b"hello").unwrap();

        // Hit.
        let out = Cache.execute(&c).expect("second call");
        assert_eq!(out.outputs.get("cache-hit"), Some(&Value::String("true".into())));
        let restored = workspace.path().join("artifacts").join("blob.txt");
        assert!(restored.exists(), "cache should restore the file");
        assert_eq!(fs::read(&restored).unwrap(), b"hello");
    }

    #[test]
    fn upload_then_download_artifact_round_trips() {
        let workspace = TempDir::new().unwrap();
        // Make a path to upload.
        let dist = workspace.path().join("dist");
        fs::create_dir_all(&dist).unwrap();
        fs::write(dist.join("bin"), b"binary-bytes").unwrap();

        let env = empty_env();
        let cfg = empty_config();

        let mut up_with = IndexMap::new();
        up_with.insert("name".into(), Value::String("cli-linux".into()));
        up_with.insert("path".into(), Value::String("dist".into()));
        let c = call("actions/upload-artifact", &up_with, &env, workspace.path(), &cfg);
        let out = UploadArtifact.execute(&c).expect("upload");
        assert_eq!(out.conclusion, StepConclusion::Success);
        assert_eq!(out.outputs.get("artifact-id"), Some(&Value::String("cli-linux".into())));

        // Clear the source so the download is observable.
        fs::remove_dir_all(&dist).unwrap();

        // Download into a fresh location.
        let mut dn_with = IndexMap::new();
        dn_with.insert("name".into(), Value::String("cli-linux".into()));
        dn_with.insert("path".into(), Value::String("restored".into()));
        let c = call("actions/download-artifact", &dn_with, &env, workspace.path(), &cfg);
        let _ = DownloadArtifact.execute(&c).expect("download");
        let restored = workspace.path().join("restored").join("dist").join("bin");
        assert!(restored.exists(), "download should restore the file at {}", restored.display());
        assert_eq!(fs::read(&restored).unwrap(), b"binary-bytes");
    }

    #[test]
    fn upload_artifact_missing_path_is_loud() {
        let workspace = TempDir::new().unwrap();
        let env = empty_env();
        let cfg = empty_config();
        let mut with = IndexMap::new();
        with.insert("name".into(), Value::String("nope".into()));
        with.insert("path".into(), Value::String("does-not-exist".into()));
        let c = call("actions/upload-artifact", &with, &env, workspace.path(), &cfg);
        let err = UploadArtifact.execute(&c).expect_err("must error");
        assert!(err.contains("does not exist"), "got: {err}");
    }

    #[test]
    fn redirect_registry_replaces_known_host() {
        let mut routes = IndexMap::new();
        routes.insert("ghcr.io".into(), Value::String("registry.yah.dev".into()));
        let mut cfg = IndexMap::new();
        cfg.insert("registry_route".into(), Value::Object(routes));
        let cfg = Value::Object(cfg);
        assert_eq!(redirect_registry("ghcr.io", &cfg), "registry.yah.dev");
        // Unknown hosts pass through.
        assert_eq!(redirect_registry("docker.io", &cfg), "docker.io");
    }

    #[test]
    fn redirect_image_ref_swaps_host_only() {
        let mut routes = IndexMap::new();
        routes.insert("ghcr.io".into(), Value::String("registry.yah.dev".into()));
        let mut cfg = IndexMap::new();
        cfg.insert("registry_route".into(), Value::Object(routes));
        let cfg = Value::Object(cfg);
        assert_eq!(
            redirect_image_ref("ghcr.io/yah-ai/yah-base:latest", &cfg),
            "registry.yah.dev/yah-ai/yah-base:latest",
        );
        // Digest refs preserve their @sha256:... suffix.
        assert_eq!(
            redirect_image_ref("ghcr.io/yah-ai/yah-rust@sha256:deadbeef", &cfg),
            "registry.yah.dev/yah-ai/yah-rust@sha256:deadbeef",
        );
        // Empty config — no rewrite.
        assert_eq!(
            redirect_image_ref("ghcr.io/yah-ai/yah-base:latest", &empty_config()),
            "ghcr.io/yah-ai/yah-base:latest",
        );
    }

    #[test]
    fn parse_buildx_metadata_extracts_digest_and_imageid() {
        let json = r#"{
            "containerimage.digest": "sha256:aaa",
            "containerimage.config.digest": "sha256:bbb",
            "buildx.build.ref": "default/default/xxx"
        }"#;
        let (digest, imageid) = parse_buildx_metadata(json).unwrap();
        assert_eq!(digest.as_deref(), Some("sha256:aaa"));
        assert_eq!(imageid.as_deref(), Some("sha256:bbb"));
    }

    #[test]
    fn parse_buildx_metadata_tolerates_missing_imageid() {
        let json = r#"{ "containerimage.digest": "sha256:onlyone" }"#;
        let (digest, imageid) = parse_buildx_metadata(json).unwrap();
        assert_eq!(digest.as_deref(), Some("sha256:onlyone"));
        assert!(imageid.is_none());
    }

    #[test]
    fn docker_login_empty_password_is_skip_not_error() {
        // The release.yml shape uses `${{ secrets.GITHUB_TOKEN }}` — QED doesn't
        // resolve that secret, so the password lands as empty. Skip-with-success
        // lets the operator's pre-existing docker creds carry the push, and any
        // real auth failure surfaces at the build-push site with the registry's
        // own error message rather than a synthetic one here.
        let tmp = TempDir::new().unwrap();
        let env = empty_env();
        let cfg = empty_config();
        let mut with = IndexMap::new();
        with.insert("registry".into(), Value::String("ghcr.io".into()));
        with.insert("username".into(), Value::String("octocat".into()));
        with.insert("password".into(), Value::String("".into()));
        let c = call("docker/login-action", &with, &env, tmp.path(), &cfg);
        let out = DockerLogin.execute(&c).expect("skip-success path");
        assert_eq!(out.conclusion, StepConclusion::Success);
        assert!(out.log.contains("skipped"), "log: {}", out.log);
    }

    #[test]
    fn docker_login_skip_log_reflects_redirected_registry() {
        let tmp = TempDir::new().unwrap();
        let env = empty_env();
        let mut routes = IndexMap::new();
        routes.insert("ghcr.io".into(), Value::String("registry.yah.dev".into()));
        let mut cfg_inner = IndexMap::new();
        cfg_inner.insert("registry_route".into(), Value::Object(routes));
        let cfg = Value::Object(cfg_inner);
        let mut with = IndexMap::new();
        with.insert("registry".into(), Value::String("ghcr.io".into()));
        // Empty password → skip path; we just want to assert the registry
        // string the skip log mentions is the redirected one.
        let c = call("docker/login-action", &with, &env, tmp.path(), &cfg);
        let out = DockerLogin.execute(&c).expect("skip-success path");
        assert!(out.log.contains("registry.yah.dev"), "redirect not applied: {}", out.log);
        assert!(!out.log.contains("`ghcr.io`"), "raw host should not appear in skip log: {}", out.log);
    }

    #[test]
    fn parse_release_filename_splits_binary_and_triple() {
        let (binary, triple) = parse_release_filename("cli-v0.8.10-x86_64-unknown-linux-musl.tar.gz");
        assert_eq!(binary, "cli");
        assert_eq!(triple.as_deref(), Some("x86_64-unknown-linux-musl"));
        let (b, t) = parse_release_filename("warden-v1.2.3-aarch64-unknown-linux-musl.tar.gz");
        assert_eq!(b, "warden");
        assert_eq!(t.as_deref(), Some("aarch64-unknown-linux-musl"));
        // No-triple fallback: short stem.
        let (b, t) = parse_release_filename("yah.zip");
        assert_eq!(b, "yah");
        assert!(t.is_none());
    }

    #[test]
    fn gh_release_stages_each_matched_file_as_produced_artifact() {
        let workspace = TempDir::new().unwrap();
        fs::write(
            workspace.path().join("cli-v0.8.10-x86_64-unknown-linux-musl.tar.gz"),
            b"tarball-bytes",
        ).unwrap();
        let env = empty_env();
        let cfg = empty_config();
        let mut with = IndexMap::new();
        with.insert(
            "files".into(),
            Value::String("cli-v0.8.10-x86_64-unknown-linux-musl.tar.gz".into()),
        );
        with.insert("tag_name".into(), Value::String("v0.8.10".into()));
        with.insert("fail_on_unmatched_files".into(), Value::String("true".into()));
        let c = call("softprops/action-gh-release", &with, &env, workspace.path(), &cfg);
        let out = GhRelease.execute(&c).expect("stage");
        assert_eq!(out.conclusion, StepConclusion::Success);
        assert_eq!(out.produced.len(), 1);
        let a = &out.produced[0];
        assert_eq!(a.binary, "cli");
        assert_eq!(a.triple.as_deref(), Some("x86_64-unknown-linux-musl"));
        assert!(a.path.ends_with("cli-v0.8.10-x86_64-unknown-linux-musl.tar.gz"));
        assert_eq!(
            out.outputs.get("url"),
            Some(&Value::String("https://cdn.yah.dev/releases/v0.8.10".into())),
        );
    }

    #[test]
    fn gh_release_glob_matches_multiple_files() {
        let workspace = TempDir::new().unwrap();
        fs::write(workspace.path().join("cli-v1-x86_64-unknown-linux-musl.tar.gz"), b"a").unwrap();
        fs::write(workspace.path().join("cli-v1-aarch64-unknown-linux-musl.tar.gz"), b"b").unwrap();
        let env = empty_env();
        let cfg = empty_config();
        let mut with = IndexMap::new();
        with.insert("files".into(), Value::String("cli-v1-*.tar.gz".into()));
        with.insert("tag_name".into(), Value::String("v1".into()));
        let c = call("softprops/action-gh-release", &with, &env, workspace.path(), &cfg);
        let out = GhRelease.execute(&c).expect("glob");
        assert_eq!(out.produced.len(), 2);
        let triples: Vec<_> = out.produced.iter().filter_map(|p| p.triple.as_deref()).collect();
        assert!(triples.contains(&"x86_64-unknown-linux-musl"));
        assert!(triples.contains(&"aarch64-unknown-linux-musl"));
    }

    #[test]
    fn gh_release_fail_on_unmatched_files_is_loud() {
        let workspace = TempDir::new().unwrap();
        let env = empty_env();
        let cfg = empty_config();
        let mut with = IndexMap::new();
        with.insert("files".into(), Value::String("nothing-here.tar.gz".into()));
        with.insert("tag_name".into(), Value::String("v9".into()));
        with.insert("fail_on_unmatched_files".into(), Value::String("true".into()));
        let c = call("softprops/action-gh-release", &with, &env, workspace.path(), &cfg);
        let err = GhRelease.execute(&c).expect_err("must error");
        assert!(err.contains("matched nothing"), "got: {err}");
    }

    #[test]
    fn docker_build_push_rejects_push_without_tags() {
        let tmp = TempDir::new().unwrap();
        let env = empty_env();
        let cfg = empty_config();
        let mut with = IndexMap::new();
        with.insert("push".into(), Value::String("true".into()));
        with.insert("context".into(), Value::String(".".into()));
        // tags intentionally omitted
        let c = call("docker/build-push-action", &with, &env, tmp.path(), &cfg);
        let err = DockerBuildPush.execute(&c).expect_err("must error");
        assert!(err.contains("no `tags`"), "got: {err}");
    }

    #[test]
    fn rust_cache_miss_when_no_seed() {
        let workspace = TempDir::new().unwrap();
        let cache_root = TempDir::new().unwrap();
        let with = IndexMap::new();
        let env = empty_env();
        let mut cfg_inner = IndexMap::new();
        cfg_inner.insert("dir".into(), Value::String(cache_root.path().to_string_lossy().into()));
        let cfg = Value::Object(cfg_inner);
        let c = call("Swatinem/rust-cache", &with, &env, workspace.path(), &cfg);
        let out = RustCache.execute(&c).expect("miss path");
        assert_eq!(out.outputs.get("cache-hit"), Some(&Value::String("false".into())));
        assert!(out.log.contains("rust-cache-"));
    }
}
