//! Host platform self-detection (R531-T1, W222).
//!
//! QED today models *where* (Local vs Remote) and *runtime* (Native vs
//! Container) but has no concept of **architecture**. W222 introduces three
//! triples per step — `host` (where commands actually execute), `target`
//! (what the step produces), and `container_platform` (the arch of the base
//! image it pulls) — and a `resolve(host, target, container_platform)`
//! decision table that picks cross-compile over emulation.
//!
//! This module is the foundation that lands first: the **host** triple is
//! cheap and reliable to self-detect at runner start, so the planner can
//! reason about portability instead of discovering an arch mismatch three
//! waves into a run (the mesofact `x86_64-unknown-linux-musl`-on-arm64
//! faceplant in W222's frame).
//!
//! The host triple is derived from the compiled binary's own
//! [`std::env::consts`] — `ARCH` (`uname -m`) plus `OS` mapped to the Rust
//! vendor/os/env convention. This is exactly the "uname -m + OS →
//! `aarch64-apple-darwin`" detection W222 calls for, and it needs no
//! subprocess: the QED runner *is* a host-native binary, so its own build
//! target is the host.
//!
//! F2 builds the structured `Platform { host, target, container_platform }`
//! field on steps atop [`detect_host_triple`]; F3 builds the `resolve(...)`
//! decision table that consumes the host triple this module produces.

use serde::{Deserialize, Serialize};

/// The TOML-declared portion of a step's platform intent (R531-F2, W222).
///
/// `host` is deliberately *not* here — it's self-detected per runner
/// (R531-T1) and composed in at plan time, so a pipeline file never hard-codes
/// the machine it runs on. A step declares only what it *produces* (`target`)
/// and, when it pulls a foreign-arch base image, that image's docker platform
/// (`container_platform`). Both default to `None`, so the overwhelming
/// majority of steps (host-native builds, checks, typechecks) need no
/// `[platform]` block at all.
///
/// On the TOML side this is an inline table on a step:
///
/// ```toml
/// [[steps]]
/// name = "build-musl"
/// platform = { target = "x86_64-unknown-linux-musl" }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct PlatformSpec {
    /// Rust target triple this step produces, e.g.
    /// `x86_64-unknown-linux-musl`. `None` = host-native build / nothing
    /// cross-compiled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Docker platform of the toolchain / base image this step pulls, e.g.
    /// `linux/amd64`. `None` = no container, or the host-platform default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_platform: Option<String>,
}

/// A step's fully-composed platform triple-set (R531-F2, W222): where it runs
/// (`host`), what it produces (`target`), and the arch of the image it pulls
/// (`container_platform`).
///
/// Built at plan time by [`Platform::compose`] from the runner's self-detected
/// host (R531-T1) plus the step's declared [`PlatformSpec`] — falling back to
/// the legacy per-kind `triple` field so existing `package-native-tarball`
/// TOML keeps producing the right target without a `[platform]` block. F3's
/// `resolve(host, target, container_platform)` decision table consumes this
/// directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Platform {
    /// Where the step's commands actually execute — the runner host triple.
    pub host: String,
    /// What the step produces. `None` = host-native, nothing cross-built.
    pub target: Option<String>,
    /// Arch of the base image the step pulls (`linux/amd64`). `None` = no
    /// container, or the host-platform default.
    pub container_platform: Option<String>,
}

impl Platform {
    /// Compose the triple-set for a step.
    ///
    /// - `host` — the runner's self-detected triple (R531-T1).
    /// - `declared` — the step's `[platform]` block, if any.
    /// - `triple_field` — the legacy per-kind `triple`
    ///   (`package-native-tarball` / `sign-native-tarball`), used as the
    ///   `target` fallback so existing TOML keeps working: an explicit
    ///   `[platform].target` always wins over it.
    pub fn compose(
        host: impl Into<String>,
        declared: Option<&PlatformSpec>,
        triple_field: Option<&str>,
    ) -> Self {
        let target = declared
            .and_then(|d| d.target.clone())
            .or_else(|| triple_field.map(str::to_string));
        let container_platform = declared.and_then(|d| d.container_platform.clone());
        Platform {
            host: host.into(),
            target,
            container_platform,
        }
    }

    /// True when the step builds for an arch other than the host's. A
    /// `target` of `None` (host-native) is never cross. Compared on the arch
    /// segment only — `x86_64-apple-darwin` on an `x86_64-unknown-linux-gnu`
    /// host is *not* a cross *arch* even though the full triples differ (the
    /// OS/cross distinction is F3's resolution concern, not this predicate's).
    pub fn is_cross_arch(&self) -> bool {
        match &self.target {
            None => false,
            Some(t) => arch_of(t) != arch_of(&self.host),
        }
    }

    /// True when the step pulls a container image whose arch differs from the
    /// host's — the exact host ≠ container_platform mismatch that produced the
    /// mesofact `no matching manifest for linux/arm64` faceplant in W222. The
    /// `linux/amd64` docker-platform vocabulary is normalized to a bare arch
    /// for the comparison.
    pub fn container_is_foreign_arch(&self) -> bool {
        match &self.container_platform {
            None => false,
            Some(p) => docker_platform_arch(p) != Some(arch_of(&self.host)),
        }
    }
}

/// The verdict of [`resolve`] for one step's platform triple-set (R531-F3,
/// W222): *how* QED should satisfy a "build for target T" / "pull image P"
/// step on the host it actually runs on.
///
/// The ordering encodes W222's **cross-compile first, emulate last** ladder:
/// a target that can be built with a host-native linker always is; emulation
/// is an explicit, named fallback for the residue, never the silent default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// Tier 1 — host-native cross-compile (`cargo-zigbuild` / musl-cross), no
    /// container, no emulation. The default path for the overwhelming
    /// majority of Rust targets (W222: ~99%). Also covers a plain host-native
    /// build (target absent or host-arch).
    NativeCross,
    /// Tier 2 — `cross` via a **host-arch** toolchain container. The toolchain
    /// runs in a container, but the container's arch matches the host so this
    /// is genuinely emulation-free (unlike a foreign-arch `cross` image, which
    /// resolves to [`Emulate`](Self::Emulate)). Chosen when the target isn't
    /// host-native crossable but a host-arch cross image can build it.
    CrossDocker,
    /// Tier 3 — QEMU / platform virtualization. The step pulls a foreign-arch
    /// image (`docker_platform`) and runs it under emulation. This is the
    /// W222 mesofact case (`linux/amd64` cross-rs image on an arm64 host) and
    /// multi-arch `buildx` image builds, where there is no cross-compile for
    /// an image. Slow and explicit — the preflight (T4) flags it so the
    /// operator sees the cost before the run.
    Emulate { docker_platform: String },
    /// No local path: the target can't be host-native crossed and no container
    /// can build it here (e.g. `*-apple-darwin` from a Linux host) — it needs
    /// a real runner capable of building `target`. The scheduler picks the
    /// concrete remote (P2+); the verdict just names what's needed.
    Offload { target: String },
    /// Nothing resolvable: a foreign target with an unrecognized arch and no
    /// container — QED can neither cross it, emulate it, nor name a runner for
    /// it. Carries a human-readable reason for the preflight.
    Skip { reason: String },
}

impl Resolution {
    /// Short human label with the cost/mechanism parenthetical, for the T4
    /// portability preflight and the QED detail pane.
    pub fn label(&self) -> String {
        match self {
            Resolution::NativeCross => "NativeCross (cargo-zigbuild)".into(),
            Resolution::CrossDocker => "CrossDocker (cross-rs container)".into(),
            Resolution::Emulate { docker_platform } => {
                format!("Emulate (QEMU {docker_platform}, slow)")
            }
            Resolution::Offload { target } => format!("Offload (needs {target} runner)"),
            Resolution::Skip { reason } => format!("Skip ({reason})"),
        }
    }

    /// True for the emulation / offload tiers — the verdicts that mean "this
    /// step will *not* run fast (or at all) on this host". The preflight uses
    /// this to flag divergence; tier 1/2 (NativeCross / CrossDocker) are the
    /// emulation-free happy path.
    pub fn is_slow_or_unsatisfiable(&self) -> bool {
        matches!(
            self,
            Resolution::Emulate { .. } | Resolution::Offload { .. } | Resolution::Skip { .. }
        )
    }
}

/// Render one portability-preflight line for a step (R531-T4, W222): its name,
/// what it targets/builds, the host it runs on, and the resolution verdict —
/// so an operator sees where mac and linux diverge (and what it costs) *before*
/// a run, not after a faceplant. Format mirrors W222's example:
///
/// ```text
/// mesofact-dev-build · targets x86_64-unknown-linux-musl · host aarch64-apple-darwin · resolution = NativeCross (cargo-zigbuild)
/// ```
pub fn preflight_line(name: &str, platform: &Platform, resolution: &Resolution) -> String {
    let what = match (&platform.target, &platform.container_platform) {
        (Some(t), _) => format!("targets {t}"),
        (None, Some(c)) => format!("builds {c} image"),
        (None, None) => "host-native".to_string(),
    };
    format!(
        "{name} · {what} · host {host} · resolution = {res}",
        host = platform.host,
        res = resolution.label(),
    )
}

/// Total resolution function (R531-F3, W222) — a decision-table-as-spec over
/// (host-arch × target × container-arch). Pure and total: every input maps to
/// exactly one [`Resolution`], so the mac-vs-linux behaviour is *specified and
/// tested* rather than emergent.
///
/// The decision order (cross-first):
/// 1. **Host-native crossable target → [`NativeCross`](Resolution::NativeCross).**
///    Wins even if the recipe declares a foreign container — that container is
///    the slow path P2/T6 should replace, not what *should* happen.
/// 2. **Foreign-arch container → [`Emulate`](Resolution::Emulate).** A
///    foreign image can't be cross-compiled away; it's pulled and run under
///    QEMU.
/// 3. **Foreign non-crossable target** → [`CrossDocker`](Resolution::CrossDocker)
///    if a host-arch toolchain container is present, else
///    [`Offload`](Resolution::Offload) (known arch) or
///    [`Skip`](Resolution::Skip) (unknown arch).
/// 4. **No target / host-arch target** → host-native
///    [`NativeCross`](Resolution::NativeCross).
pub fn resolve(host: &str, target: Option<&str>, container_platform: Option<&str>) -> Resolution {
    let host_arch = arch_of(host);
    let foreign_target = target
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .filter(|t| arch_of(t) != host_arch);

    // 1. Cross-first: a host-native crossable target always wins.
    if let Some(t) = foreign_target {
        if host_native_crossable(host, t) {
            return Resolution::NativeCross;
        }
    }

    // 2. A foreign-arch container forces emulation (mesofact case + buildx).
    if let Some(p) = container_platform {
        if docker_platform_arch(p) != Some(host_arch) {
            return Resolution::Emulate {
                docker_platform: p.to_string(),
            };
        }
    }

    // 3. Foreign target we can't host-native cross. Any container that reaches
    //    here is host-arch (a foreign one returned Emulate above), so it's a
    //    cross-rs-style toolchain image that can build the target.
    if let Some(t) = foreign_target {
        if container_platform.is_some() {
            return Resolution::CrossDocker;
        }
        if is_known_arch(arch_of(t)) {
            return Resolution::Offload {
                target: t.to_string(),
            };
        }
        return Resolution::Skip {
            reason: format!(
                "cannot build `{t}` on host `{host}`: target arch is unrecognized, \
                 no host-native cross path, and no toolchain container declared"
            ),
        };
    }

    // 4. No target, or a host-arch target → plain native build on the host.
    Resolution::NativeCross
}

/// Can `target` be built on `host` with a host-native linker
/// (`cargo-zigbuild` / musl-cross), i.e. no container and no emulation?
///
/// The spec (refinable as real cross builds surface, the same discipline as
/// [`crate::preflight::KNOWN_GLIBC_ONLY_CRATES`]):
/// - **Linux** (`-gnu` / `-musl`, any arch): yes from any host — zig provides
///   the sysroot + linker for both libc flavors.
/// - **Windows `-gnu`**: yes from any host (zig). **Windows `-msvc`**: no —
///   needs the MSVC toolchain.
/// - **Apple/Darwin**: yes only from a macOS host (the SDK + codesign aren't
///   redistributable), no from Linux/Windows.
/// - **Unknown OS**: no.
pub fn host_native_crossable(host: &str, target: &str) -> bool {
    match target_os(target) {
        TargetOs::Linux => true,
        TargetOs::Windows { msvc } => !msvc,
        TargetOs::Darwin => matches!(target_os(host), TargetOs::Darwin),
        TargetOs::Unknown => false,
    }
}

/// Coarse OS classification of a target triple, for [`host_native_crossable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetOs {
    Linux,
    Windows { msvc: bool },
    Darwin,
    Unknown,
}

fn target_os(triple: &str) -> TargetOs {
    if triple.contains("linux") {
        TargetOs::Linux
    } else if triple.contains("windows") {
        TargetOs::Windows {
            msvc: triple.ends_with("msvc"),
        }
    } else if triple.contains("darwin") || triple.contains("apple") {
        TargetOs::Darwin
    } else {
        TargetOs::Unknown
    }
}

/// Recognized CPU arch tokens — the set [`resolve`] can name a runner for when
/// it has to [`Offload`](Resolution::Offload). An unrecognized arch resolves
/// to [`Skip`](Resolution::Skip) instead.
fn is_known_arch(arch: &str) -> bool {
    matches!(
        arch,
        "x86_64" | "aarch64" | "arm64" | "x86" | "i686" | "arm" | "riscv64" | "powerpc64" | "s390x"
    )
}

/// Map a docker `--platform` value (`linux/amd64`, `linux/arm64/v8`, or a bare
/// `arm64`) to the Rust arch token used in target triples (`x86_64`,
/// `aarch64`). Returns `None` for an unrecognized arch so callers can decide
/// how to treat the unknown rather than silently matching.
pub fn docker_platform_arch(platform: &str) -> Option<&'static str> {
    // `os/arch[/variant]` — the arch is the middle (or only) segment.
    let arch = platform.split('/').nth(1).unwrap_or(platform);
    match arch {
        "amd64" | "x86_64" => Some("x86_64"),
        "arm64" | "aarch64" => Some("aarch64"),
        "386" | "x86" => Some("x86"),
        "arm" => Some("arm"),
        _ => None,
    }
}

/// Detect the host's Rust target triple (e.g. `aarch64-apple-darwin`,
/// `x86_64-unknown-linux-gnu`).
///
/// Composed from [`std::env::consts::ARCH`] and [`std::env::consts::OS`] —
/// the arch is taken verbatim (it already matches the Rust triple's first
/// segment) and the OS is mapped to the canonical `<vendor>-<os>[-<env>]`
/// tail. A Linux host is reported as `-gnu`: a musl *host* is vanishingly
/// rare for our runners, and `target` (what a step builds) — where musl
/// actually matters — is a separate triple F2 carries per step.
///
/// Unknown OSes fall back to `unknown-<os>` so the output is still a
/// well-formed, greppable triple rather than a panic.
pub fn detect_host_triple() -> String {
    let arch = std::env::consts::ARCH;
    let tail = match std::env::consts::OS {
        "macos" => "apple-darwin",
        "linux" => "unknown-linux-gnu",
        "windows" => "pc-windows-msvc",
        other => return format!("{arch}-unknown-{other}"),
    };
    format!("{arch}-{tail}")
}

/// Normalize a Rust arch token (the first segment of a target triple) to the
/// GitHub Actions `runner.arch` vocabulary (`X86` / `X64` / `ARM` / `ARM64`).
///
/// GHA workflows gate on `runner.arch`, so when QED threads the detected host
/// into the GHA expression context (see `yah_qed_gha::Executor::runner_arch`) it
/// must speak that vocabulary, not Rust's. An unrecognized arch is upcased
/// verbatim — a forward-compatible, debuggable default rather than a wrong
/// guess.
pub fn gha_runner_arch(arch: &str) -> String {
    match arch {
        "x86_64" => "X64".into(),
        "aarch64" | "arm64" => "ARM64".into(),
        "x86" | "i686" => "X86".into(),
        "arm" => "ARM".into(),
        other => other.to_ascii_uppercase(),
    }
}

/// The arch segment (first `-`-delimited token) of a target triple.
/// `arch_of("aarch64-apple-darwin") == "aarch64"`.
pub fn arch_of(triple: &str) -> &str {
    triple.split('-').next().unwrap_or(triple)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_host_triple_is_a_wellformed_triple() {
        let t = detect_host_triple();
        // Always at least arch + vendor + os (3 segments), arch first.
        let segs: Vec<&str> = t.split('-').collect();
        assert!(
            segs.len() >= 3,
            "host triple should have >=3 segments: {t:?}"
        );
        assert_eq!(segs[0], std::env::consts::ARCH, "arch segment leads: {t:?}");
    }

    #[test]
    fn detect_host_triple_matches_known_os_tails() {
        // The detected triple's tail must match the running OS's convention,
        // so this test pins the mapping on whatever host CI/dev runs it.
        let t = detect_host_triple();
        match std::env::consts::OS {
            "macos" => assert!(t.ends_with("-apple-darwin"), "{t:?}"),
            "linux" => assert!(t.ends_with("-unknown-linux-gnu"), "{t:?}"),
            "windows" => assert!(t.ends_with("-pc-windows-msvc"), "{t:?}"),
            _ => {} // unknown-OS fallback covered by the wellformed test
        }
    }

    #[test]
    fn gha_runner_arch_maps_the_rust_vocabulary() {
        assert_eq!(gha_runner_arch("x86_64"), "X64");
        assert_eq!(gha_runner_arch("aarch64"), "ARM64");
        assert_eq!(gha_runner_arch("arm64"), "ARM64");
        assert_eq!(gha_runner_arch("x86"), "X86");
        assert_eq!(gha_runner_arch("i686"), "X86");
        assert_eq!(gha_runner_arch("arm"), "ARM");
    }

    #[test]
    fn gha_runner_arch_upcases_unknown_arch() {
        // Forward-compatible: a new arch we haven't mapped is upcased, not
        // mis-guessed.
        assert_eq!(gha_runner_arch("riscv64"), "RISCV64");
    }

    #[test]
    fn arch_of_takes_the_first_segment() {
        assert_eq!(arch_of("aarch64-apple-darwin"), "aarch64");
        assert_eq!(arch_of("x86_64-unknown-linux-musl"), "x86_64");
        assert_eq!(arch_of("nodashes"), "nodashes");
    }

    // ── Platform / PlatformSpec composition (R531-F2) ───────────────────────

    #[test]
    fn compose_prefers_declared_target_over_triple_field() {
        let spec = PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: None,
        };
        let p = Platform::compose("aarch64-apple-darwin", Some(&spec), Some("legacy-triple"));
        assert_eq!(p.target.as_deref(), Some("x86_64-unknown-linux-musl"));
        assert_eq!(p.host, "aarch64-apple-darwin");
    }

    #[test]
    fn compose_falls_back_to_triple_field_when_undeclared() {
        // package-native-tarball back-compat: a step with no `[platform]`
        // block still lifts its legacy `triple` into the target.
        let p = Platform::compose(
            "aarch64-apple-darwin",
            None,
            Some("x86_64-unknown-linux-musl"),
        );
        assert_eq!(p.target.as_deref(), Some("x86_64-unknown-linux-musl"));
        assert!(p.container_platform.is_none());
    }

    #[test]
    fn compose_host_native_when_nothing_declared() {
        let p = Platform::compose("aarch64-apple-darwin", None, None);
        assert!(p.target.is_none());
        assert!(!p.is_cross_arch(), "no target is never cross-arch");
    }

    #[test]
    fn is_cross_arch_compares_arch_segment_only() {
        // Same arch, different OS → not a cross *arch*.
        let same_arch = Platform::compose(
            "x86_64-unknown-linux-gnu",
            None,
            Some("x86_64-apple-darwin"),
        );
        assert!(!same_arch.is_cross_arch());
        // Different arch → cross.
        let cross = Platform::compose(
            "aarch64-apple-darwin",
            None,
            Some("x86_64-unknown-linux-musl"),
        );
        assert!(cross.is_cross_arch());
    }

    #[test]
    fn container_foreign_arch_catches_the_mesofact_case() {
        // The W222 motivating failure: arm64 host pulling a linux/amd64
        // toolchain image → foreign-arch container.
        let spec = PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: Some("linux/amd64".into()),
        };
        let p = Platform::compose("aarch64-apple-darwin", Some(&spec), None);
        assert!(p.container_is_foreign_arch());

        // Same arch image on an amd64 host → not foreign.
        let native = Platform::compose("x86_64-unknown-linux-gnu", Some(&spec), None);
        assert!(!native.container_is_foreign_arch());
    }

    #[test]
    fn docker_platform_arch_normalizes_os_arch_variant() {
        assert_eq!(docker_platform_arch("linux/amd64"), Some("x86_64"));
        assert_eq!(docker_platform_arch("linux/arm64/v8"), Some("aarch64"));
        assert_eq!(docker_platform_arch("arm64"), Some("aarch64"));
        assert_eq!(docker_platform_arch("linux/riscv64"), None);
    }

    #[test]
    fn platform_spec_round_trips_through_toml() {
        let spec = PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: Some("linux/amd64".into()),
        };
        let toml = toml::to_string(&spec).unwrap();
        let back: PlatformSpec = toml::from_str(&toml).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn empty_platform_spec_serializes_to_nothing() {
        // Both fields skip_serializing_if None → an all-default spec emits no
        // keys, so a step that declares `platform = {}` stays inert.
        let spec = PlatformSpec::default();
        assert_eq!(toml::to_string(&spec).unwrap(), "");
    }

    // ── resolve() decision table (R531-F3) ──────────────────────────────────

    const ARM_MAC: &str = "aarch64-apple-darwin";
    const X64_LINUX: &str = "x86_64-unknown-linux-gnu";

    #[test]
    fn resolve_no_target_is_native() {
        assert_eq!(resolve(ARM_MAC, None, None), Resolution::NativeCross);
    }

    #[test]
    fn resolve_host_arch_target_is_native() {
        // Same arch (different OS doesn't matter to this tier) → native.
        assert_eq!(
            resolve(ARM_MAC, Some("aarch64-unknown-linux-musl"), None),
            Resolution::NativeCross
        );
    }

    #[test]
    fn resolve_foreign_linux_target_cross_compiles_natively() {
        // The mesofact target itself, sans foreign container: zig cross-builds
        // x86_64 musl from an arm64 mac with no emulation.
        assert_eq!(
            resolve(ARM_MAC, Some("x86_64-unknown-linux-musl"), None),
            Resolution::NativeCross
        );
    }

    #[test]
    fn resolve_crossable_target_wins_over_foreign_container() {
        // Cross-first: even though the recipe declares a foreign cross-rs
        // image, a host-native crossable target resolves to NativeCross (the
        // slow container is what P2/T6 should replace).
        assert_eq!(
            resolve(
                ARM_MAC,
                Some("x86_64-unknown-linux-musl"),
                Some("linux/amd64")
            ),
            Resolution::NativeCross
        );
    }

    #[test]
    fn resolve_foreign_container_with_non_crossable_emulates() {
        // A non-crossable foreign target (windows-msvc) pulling a foreign-arch
        // image on an arm64 host → Emulate, carrying the platform to pull.
        // (For the *crossable* mesofact target the verdict is NativeCross —
        // "use zigbuild, not the container" — see the test above.)
        let r = resolve(ARM_MAC, Some("x86_64-pc-windows-msvc"), Some("linux/amd64"));
        assert_eq!(
            r,
            Resolution::Emulate {
                docker_platform: "linux/amd64".into()
            }
        );
    }

    #[test]
    fn resolve_multiarch_image_build_with_no_target_emulates() {
        // A buildx image job (no Rust target) pulling a foreign-arch image:
        // no cross-compile for an image → Emulate.
        assert_eq!(
            resolve(ARM_MAC, None, Some("linux/amd64")),
            Resolution::Emulate {
                docker_platform: "linux/amd64".into()
            }
        );
    }

    #[test]
    fn resolve_host_arch_container_is_not_emulation() {
        // A host-arch image (no target) is just a native containerized build.
        assert_eq!(
            resolve(ARM_MAC, None, Some("linux/arm64")),
            Resolution::NativeCross
        );
    }

    #[test]
    fn resolve_darwin_target_from_linux_host_offloads() {
        // Can't host-native cross macOS off Linux, no container → needs a mac.
        assert_eq!(
            resolve(X64_LINUX, Some("aarch64-apple-darwin"), None),
            Resolution::Offload {
                target: "aarch64-apple-darwin".into()
            }
        );
    }

    #[test]
    fn resolve_non_crossable_with_host_arch_container_uses_cross_docker() {
        // macOS target off Linux, but a host-arch (linux/amd64) cross toolchain
        // image is declared → CrossDocker (genuinely emulation-free).
        assert_eq!(
            resolve(X64_LINUX, Some("aarch64-apple-darwin"), Some("linux/amd64")),
            Resolution::CrossDocker
        );
    }

    #[test]
    fn resolve_windows_msvc_off_linux_offloads_but_gnu_cross_compiles() {
        // -msvc needs MSVC → offload; -gnu zig-cross-compiles → native.
        assert_eq!(
            resolve(X64_LINUX, Some("aarch64-pc-windows-msvc"), None),
            Resolution::Offload {
                target: "aarch64-pc-windows-msvc".into()
            }
        );
        assert_eq!(
            resolve(X64_LINUX, Some("aarch64-pc-windows-gnu"), None),
            Resolution::NativeCross
        );
    }

    #[test]
    fn resolve_unknown_foreign_arch_with_no_container_skips() {
        // An unrecognizable arch we can't name a runner for → Skip with reason.
        let r = resolve(X64_LINUX, Some("sparc64-unknown-linux-gnu"), None);
        // sparc64 linux is technically zig-crossable per our coarse rule
        // (linux ⇒ true), so this resolves NativeCross — assert that, and use a
        // genuinely unknown-OS triple for the Skip path below.
        assert_eq!(r, Resolution::NativeCross);

        let skip = resolve(X64_LINUX, Some("mos-unknown-none"), None);
        match skip {
            Resolution::Skip { reason } => {
                assert!(reason.contains("mos-unknown-none"), "reason: {reason}");
            }
            other => panic!("expected Skip, got {other:?}"),
        }
    }

    /// Exhaustive sweep: every (host, target, container) class combination maps
    /// to exactly one Resolution and the function never panics. This is the
    /// decision-table-as-spec guarantee — totality over the input space.
    #[test]
    fn resolve_is_total_over_the_class_space() {
        let hosts = [ARM_MAC, X64_LINUX, "x86_64-pc-windows-msvc"];
        let targets = [
            None,
            Some("x86_64-unknown-linux-musl"),
            Some("aarch64-unknown-linux-gnu"),
            Some("aarch64-apple-darwin"),
            Some("x86_64-pc-windows-msvc"),
            Some("mos-unknown-none"),
            Some(""),
        ];
        let containers = [
            None,
            Some("linux/amd64"),
            Some("linux/arm64"),
            Some("linux/riscv64"),
        ];
        for h in hosts {
            for t in targets {
                for c in containers {
                    // Just exercising every cell — the assertion is "doesn't
                    // panic and returns a value"; specific cells are pinned by
                    // the named tests above.
                    let _r = resolve(h, t, c);
                }
            }
        }
    }

    // ── preflight rendering (R531-T4) ────────────────────────────────────────

    #[test]
    fn resolution_labels_carry_the_cost_parenthetical() {
        assert_eq!(
            Resolution::NativeCross.label(),
            "NativeCross (cargo-zigbuild)"
        );
        assert_eq!(
            Resolution::CrossDocker.label(),
            "CrossDocker (cross-rs container)"
        );
        assert_eq!(
            Resolution::Emulate {
                docker_platform: "linux/amd64".into()
            }
            .label(),
            "Emulate (QEMU linux/amd64, slow)"
        );
        assert_eq!(
            Resolution::Offload {
                target: "aarch64-apple-darwin".into()
            }
            .label(),
            "Offload (needs aarch64-apple-darwin runner)"
        );
    }

    #[test]
    fn is_slow_or_unsatisfiable_partitions_the_tiers() {
        assert!(!Resolution::NativeCross.is_slow_or_unsatisfiable());
        assert!(!Resolution::CrossDocker.is_slow_or_unsatisfiable());
        assert!(Resolution::Emulate {
            docker_platform: "linux/amd64".into()
        }
        .is_slow_or_unsatisfiable());
        assert!(Resolution::Offload { target: "x".into() }.is_slow_or_unsatisfiable());
        assert!(Resolution::Skip { reason: "x".into() }.is_slow_or_unsatisfiable());
    }

    #[test]
    fn preflight_line_matches_w222_format() {
        // The motivating example from W222, verbatim shape.
        let p = Platform::compose(ARM_MAC, None, Some("x86_64-unknown-linux-musl"));
        let r = resolve(
            &p.host,
            p.target.as_deref(),
            p.container_platform.as_deref(),
        );
        let line = preflight_line("mesofact-dev-build", &p, &r);
        assert_eq!(
            line,
            "mesofact-dev-build · targets x86_64-unknown-linux-musl · \
             host aarch64-apple-darwin · resolution = NativeCross (cargo-zigbuild)"
        );
    }

    #[test]
    fn preflight_line_describes_image_builds_and_host_native() {
        let img = Platform {
            host: ARM_MAC.into(),
            target: None,
            container_platform: Some("linux/amd64".into()),
        };
        let r = resolve(&img.host, None, img.container_platform.as_deref());
        assert!(preflight_line("image-yah-base", &img, &r).contains("builds linux/amd64 image"),);

        let native = Platform::compose(ARM_MAC, None, None);
        let rn = resolve(&native.host, None, None);
        assert!(preflight_line("check", &native, &rn).contains("· host-native ·"));
    }

    #[test]
    fn host_native_crossable_spec() {
        // Linux from anywhere.
        assert!(host_native_crossable(ARM_MAC, "x86_64-unknown-linux-musl"));
        assert!(host_native_crossable(
            X64_LINUX,
            "aarch64-unknown-linux-gnu"
        ));
        // Windows gnu yes, msvc no.
        assert!(host_native_crossable(X64_LINUX, "x86_64-pc-windows-gnu"));
        assert!(!host_native_crossable(X64_LINUX, "x86_64-pc-windows-msvc"));
        // Darwin only from a darwin host.
        assert!(host_native_crossable(ARM_MAC, "x86_64-apple-darwin"));
        assert!(!host_native_crossable(X64_LINUX, "aarch64-apple-darwin"));
        // Unknown OS never.
        assert!(!host_native_crossable(X64_LINUX, "mos-unknown-none"));
    }

    #[test]
    fn host_arch_round_trips_through_gha_vocabulary() {
        // The host we detect must always normalize to a non-empty GHA arch.
        let host = detect_host_triple();
        let gha = gha_runner_arch(arch_of(&host));
        assert!(!gha.is_empty(), "host {host:?} → gha arch {gha:?}");
    }
}
