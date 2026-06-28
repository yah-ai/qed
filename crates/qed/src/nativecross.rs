//! The NativeCross tier: host-native cross-compilation (R531-F5, W222).
//!
//! F3's [`resolve`](crate::platform::resolve) decides *that* a step should be
//! satisfied by [`Resolution::NativeCross`](crate::platform::Resolution::NativeCross)
//! — a host-native cross-compile, no container, no emulation. This module is
//! the *how*: which host-native cross toolchain carries the build, the
//! concrete `cargo` invocation it produces, the env it needs, and the
//! actionable hint when the toolchain isn't installed.
//!
//! ## Why this is its own tier
//!
//! W222's preference ladder puts host-native cross-compile first for ~99% of
//! a Rust monorepo's targets — and warns that `cross` on a foreign-arch host
//! is "tier-3 cost wearing a tier-1 label" (it runs the amd64 cross-rs
//! container under QEMU). The mesofact faceplant was exactly this: an arm64
//! mac shelling `cross build --target x86_64-unknown-linux-musl` pulls an
//! amd64-only image and dies resolving the `FROM`. The fix F3's handoff named
//! is "stop using the foreign container, use zigbuild" — and *this* module is
//! what zigbuild-the-verdict routes to.
//!
//! ## The two host-native toolchains
//!
//! - **`cargo-zigbuild`** ([`CrossTool::CargoZigbuild`]) — zig as the
//!   linker + sysroot. Cross-compiles musl *and* glibc Linux from any host,
//!   and Windows-gnu too. This is the default for every foreign-arch Linux /
//!   Windows-gnu target: one tool, no per-target toolchain install.
//! - **musl cross-toolchain** ([`CrossTool::MuslCross`]) — a
//!   `<arch>-linux-musl-gcc` (homebrew `musl-cross`, or the
//!   `messense/<arch>-linux-musl-cross` packages). A *fallback*: only for
//!   musl targets, only when zig isn't on the box but the cross-gcc is. It
//!   needs the `CARGO_TARGET_*_LINKER` / `CC_*` env wired up, which is why
//!   zigbuild is preferred.
//!
//! A host-native or same-OS-arch-cross target (e.g. `x86_64-apple-darwin`
//! from an arm64 mac, where both SDKs are present) needs neither — plain
//! [`CrossTool::CargoNative`] (`cargo build --target …`) links it directly.
//!
//! ## Discipline
//!
//! Selection is a **pure, total decision table** ([`select_cross_tool`])
//! over (host, target, availability) — the same pure-core / shell-seam split
//! as [`crate::preflight`] (`check_dep_list` vs `check_musl_compatibility`):
//! [`ToolAvailability::probe`] is the only impure part, and tests drive the
//! table with hand-built availability so the mac-vs-linux tool choice is
//! *specified*, not emergent. T6 wires [`plan_native_cross`] into the
//! subprocess seam; F5 only defines and tests the mechanism.

use crate::platform::{arch_of, host_native_crossable};

/// A host-native cross-compilation mechanism — the concrete toolchain a
/// [`NativeCross`](crate::platform::Resolution::NativeCross) verdict runs on.
///
/// The program is always `cargo`; the variants differ in the subcommand and
/// the env they need. Ordered by preference: a plain native build needs the
/// least, zigbuild covers the most ground, musl-cross is the narrow fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrossTool {
    /// `cargo build [--target T]` with no foreign sysroot — the target is
    /// host-native, or a same-OS arch-cross the host SDK already covers
    /// (`x86_64-apple-darwin` from an arm64 mac). Just needs the rustup
    /// `target add`.
    CargoNative,
    /// `cargo zigbuild --target T` — zig provides the linker + sysroot,
    /// cross-compiling musl/glibc Linux and Windows-gnu from any host. The
    /// W222 default for the overwhelming majority of foreign-arch targets.
    CargoZigbuild,
    /// `cargo build --target T` with `CARGO_TARGET_<T>_LINKER` / `CC_<t>`
    /// pointed at a `<arch>-linux-musl-gcc` cross toolchain. Fallback for a
    /// musl target on a host that has the cross-gcc but not zig.
    MuslCross,
}

impl CrossTool {
    /// The cargo subcommand this tool drives (`build` or `zigbuild`). The
    /// program itself is always `cargo`.
    pub fn cargo_subcommand(&self) -> &'static str {
        match self {
            CrossTool::CargoZigbuild => "zigbuild",
            CrossTool::CargoNative | CrossTool::MuslCross => "build",
        }
    }

    /// Env vars this tool needs for `target`, as `(key, value)` pairs.
    ///
    /// Only [`MuslCross`](Self::MuslCross) needs any — it points cargo's
    /// per-target linker and the `cc` crate's compiler at the
    /// `<arch>-linux-musl-gcc` cross toolchain. Zigbuild self-contains its
    /// sysroot, and a native build inherits the host toolchain, so both
    /// return empty.
    pub fn env_for(&self, target: &str) -> Vec<(String, String)> {
        match self {
            CrossTool::MuslCross => {
                let prefix = musl_cc_prefix(target);
                let key = cargo_target_env_key(target);
                vec![
                    (
                        format!("CARGO_TARGET_{key}_LINKER"),
                        format!("{prefix}-gcc"),
                    ),
                    (
                        format!("CC_{}", target.replace('-', "_")),
                        format!("{prefix}-gcc"),
                    ),
                    (
                        format!("AR_{}", target.replace('-', "_")),
                        format!("{prefix}-ar"),
                    ),
                ]
            }
            CrossTool::CargoNative | CrossTool::CargoZigbuild => Vec::new(),
        }
    }

    /// The argv that probes whether this tool is installed (its `--version`).
    /// [`ToolAvailability::probe`] runs these; the exit status is the signal.
    pub fn probe_argv(&self) -> Vec<String> {
        match self {
            CrossTool::CargoZigbuild => {
                vec!["cargo".into(), "zigbuild".into(), "--version".into()]
            }
            // A native build only needs the rustup target; there's no extra
            // binary to probe, so its "probe" is `cargo --version` (always
            // present where qed runs cargo at all).
            CrossTool::CargoNative => vec!["cargo".into(), "--version".into()],
            // MuslCross is probed per-target by the linker binary; the bare
            // probe checks the x86_64 gcc as a representative.
            CrossTool::MuslCross => {
                vec!["x86_64-linux-musl-gcc".into(), "--version".into()]
            }
        }
    }

    /// Actionable one-line install hint, surfaced when the tool is selected
    /// but [unavailable](ToolAvailability). Mirrors the
    /// [`preflight`](crate::preflight) discipline of routing the operator to
    /// the fix rather than dying with a raw toolchain error.
    pub fn install_hint(&self) -> &'static str {
        match self {
            CrossTool::CargoZigbuild => {
                "install cargo-zigbuild + zig: `cargo install cargo-zigbuild` and \
                 `brew install zig` (or download from ziglang.org)"
            }
            CrossTool::MuslCross => {
                "install a musl cross toolchain: `brew install FiloSottile/musl-cross/musl-cross` \
                 (macOS) or the `<arch>-linux-musl-cross` package — or install cargo-zigbuild, \
                 which needs no per-target toolchain"
            }
            CrossTool::CargoNative => "add the rustup target: `rustup target add <triple>`",
        }
    }

    /// Short label with the mechanism parenthetical, matching the style of
    /// [`Resolution::label`](crate::platform::Resolution::label) for the T4
    /// preflight / detail pane.
    pub fn label(&self) -> &'static str {
        match self {
            CrossTool::CargoNative => "native (cargo build)",
            CrossTool::CargoZigbuild => "cargo-zigbuild",
            CrossTool::MuslCross => "musl-cross",
        }
    }
}

/// Which host-native toolchains are present on this runner (R531-F5).
///
/// The availability-aware half of the decision table: [`select_cross_tool`]
/// prefers zigbuild but falls back to musl-cross for musl targets when zig
/// isn't installed. Build it from a real probe ([`Self::probe`]) at runtime,
/// or by hand in tests so the fallback ladder is *specified*.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolAvailability {
    /// `cargo zigbuild` resolves (cargo-zigbuild + zig installed).
    pub zigbuild: bool,
    /// A `<arch>-linux-musl-gcc` cross toolchain is on PATH.
    pub musl_cross: bool,
}

impl ToolAvailability {
    /// Assume the full happy path — both toolchains present. Useful as a
    /// planning default and in tests that don't exercise the fallback.
    pub const FULL: ToolAvailability = ToolAvailability {
        zigbuild: true,
        musl_cross: true,
    };

    /// Nothing host-native installed — every cross target errors with an
    /// install hint. The empty end of the table.
    pub const NONE: ToolAvailability = ToolAvailability {
        zigbuild: false,
        musl_cross: false,
    };

    /// Probe the host: run each tool's `--version` and record whether it
    /// exits cleanly. The single impure entry point (shell seam); the rest of
    /// the module is pure over the result.
    pub fn probe() -> ToolAvailability {
        ToolAvailability {
            zigbuild: probe_ok(&CrossTool::CargoZigbuild.probe_argv()),
            musl_cross: probe_ok(&CrossTool::MuslCross.probe_argv()),
        }
    }
}

/// A planned host-native cross build (R531-F5): the rewritten argv and the env
/// it must run under. The terminal output of [`plan_native_cross`] — what T6
/// hands to the subprocess seam in place of the original `cross build` / bare
/// `cargo build` argv.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeCrossPlan {
    /// The toolchain chosen for this target.
    pub tool: CrossTool,
    /// The rewritten build argv (`cargo zigbuild --target T …`).
    pub argv: Vec<String>,
    /// Env that must be set for the build (non-empty only for musl-cross).
    pub env: Vec<(String, String)>,
}

/// Why no host-native cross toolchain could carry a target (R531-F5). Carries
/// the tool we'd have used and its install hint, so the runner can surface an
/// actionable error instead of a raw linker failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "no host-native cross toolchain for `{target}` on host `{host}`: {} is not installed — {}",
    .preferred.label(),
    .preferred.install_hint()
)]
pub struct CrossToolUnavailable {
    pub host: String,
    pub target: String,
    /// The tool [`select_cross_tool`] would have used had it been present.
    pub preferred: CrossTool,
}

/// Select the host-native cross toolchain for `target` on `host`, honoring
/// what's installed (R531-F5) — the decision-table-as-spec for the NativeCross
/// tier. **Total** over the input space; every cell maps to exactly one tool
/// or one [`CrossToolUnavailable`].
///
/// Precondition: only meaningful for a target F3 resolved to
/// [`NativeCross`](crate::platform::Resolution::NativeCross). A target that
/// isn't host-native crossable is *not this function's concern* — it falls
/// back to [`CargoNative`](CrossTool::CargoNative) so the caller still gets a
/// runnable plan (the rustup target may simply be missing), rather than a
/// panic.
///
/// The table:
/// 1. **Host-native / same-OS arch-cross** (target absent, host-arch, or a
///    darwin target from a darwin host) → [`CargoNative`](CrossTool::CargoNative).
///    No foreign sysroot needed.
/// 2. **Foreign-arch Linux / Windows-gnu, zig present** →
///    [`CargoZigbuild`](CrossTool::CargoZigbuild). The W222 default.
/// 3. **Foreign-arch musl, no zig but musl-cross present** →
///    [`MuslCross`](CrossTool::MuslCross). The narrow fallback.
/// 4. **Otherwise** → `Err(CrossToolUnavailable)` naming the preferred tool
///    and its install hint.
pub fn select_cross_tool(
    host: &str,
    target: Option<&str>,
    avail: &ToolAvailability,
) -> Result<CrossTool, CrossToolUnavailable> {
    let target = match target.map(str::trim).filter(|t| !t.is_empty()) {
        // No target → host-native build.
        None => return Ok(CrossTool::CargoNative),
        Some(t) => t,
    };

    // 1. Host-native arch, or a cross the host SDK covers without a foreign
    //    linker (darwin↔darwin) → plain cargo. `host_native_crossable` already
    //    encodes the darwin-only-from-darwin rule; an arch match is the
    //    same-arch case. Neither needs zig.
    let same_arch = arch_of(target) == arch_of(host);
    if same_arch || needs_no_foreign_linker(host, target) {
        return Ok(CrossTool::CargoNative);
    }

    // 2. Foreign-arch crossable target: zig is the preferred carrier.
    if avail.zigbuild {
        return Ok(CrossTool::CargoZigbuild);
    }

    // 3. musl-only fallback when zig is absent but the cross-gcc is present.
    if is_musl_target(target) && avail.musl_cross {
        return Ok(CrossTool::MuslCross);
    }

    // 4. Nothing host-native can carry it. zig is always the recommended fix
    //    even for a musl target with no cross-gcc — one tool, no per-target
    //    install — so it's the tool the error names.
    Err(CrossToolUnavailable {
        host: host.to_string(),
        target: target.to_string(),
        preferred: CrossTool::CargoZigbuild,
    })
}

/// Plan a host-native cross build (R531-F5): select the toolchain, rewrite the
/// original build argv onto it, and gather its env. The top-level F5 API T6
/// wires into the subprocess seam — given the recipe's original `cross build`
/// / `cargo build` argv plus the resolved (host, target), it yields the
/// emulation-free invocation that replaces it.
pub fn plan_native_cross(
    original_argv: &[String],
    host: &str,
    target: &str,
    avail: &ToolAvailability,
) -> Result<NativeCrossPlan, CrossToolUnavailable> {
    let tool = select_cross_tool(host, Some(target), avail)?;
    let argv = rewrite_build_argv(original_argv, &tool, target);
    let env = tool.env_for(target);
    Ok(NativeCrossPlan { tool, argv, env })
}

/// Rewrite an existing build argv onto a host-native `tool` for `target`
/// (R531-F5). This is the concrete "route the cross-rs container to zigbuild"
/// transform F3's handoff named — it takes the recipe's `["cross", "build",
/// "--release"]` or `["cargo", "build", "--release", "--target", T]` and:
///
/// - rewrites the program (`cross` / `cargo`) to **`cargo`**,
/// - rewrites the build subcommand to the tool's
///   ([`zigbuild`](CrossTool::cargo_subcommand) / `build`),
/// - ensures exactly one `--target T` is present (kept if already there,
///   appended if not).
///
/// An argv that doesn't look like a cargo/cross build (no recognizable
/// `<cargo|cross> <build|zigbuild>` head) is returned **unchanged** — we can't
/// know an arbitrary command's flag syntax, so appending `--target` could
/// corrupt it. The caller's seam ([`plan_native_cross`]) only reaches this for
/// a step the operator explicitly tagged with a cross target; a non-build argv
/// there is degenerate and passes through verbatim.
pub fn rewrite_build_argv(argv: &[String], tool: &CrossTool, target: &str) -> Vec<String> {
    // Recognize `<cargo|cross> <build|zigbuild>` at the head and normalize it
    // to `cargo <tool-subcommand>`.
    let prog = argv.first().map(String::as_str);
    let sub = argv.get(1).map(String::as_str);
    let head_is_build = matches!(prog, Some("cargo") | Some("cross"))
        && matches!(sub, Some("build") | Some("zigbuild"));

    if !head_is_build {
        return argv.to_vec();
    }

    let mut out: Vec<String> = Vec::with_capacity(argv.len() + 2);
    out.push("cargo".to_string());
    out.push(tool.cargo_subcommand().to_string());
    out.extend(argv[2..].iter().cloned());
    ensure_target_flag(&mut out, target);
    out
}

/// Ensure `argv` carries exactly one `--target <target>`. If a `--target`
/// (either `--target T` or `--target=T`) is already present it's left as-is
/// (the recipe's target wins — they should agree by construction); otherwise
/// `--target target` is appended.
fn ensure_target_flag(argv: &mut Vec<String>, target: &str) {
    let has_target = argv
        .iter()
        .any(|a| a == "--target" || a.starts_with("--target="));
    if !has_target {
        argv.push("--target".to_string());
        argv.push(target.to_string());
    }
}

/// Does a *cross-arch* `target` link without a foreign linker on `host`? True
/// only for the darwin→darwin case: the macOS SDK ships both arch slices, so
/// `cargo build --target x86_64-apple-darwin` links on an arm64 mac with no
/// zig. Linux/Windows cross-arch always needs a cross linker (zig / musl-gcc),
/// so this is false for them.
fn needs_no_foreign_linker(host: &str, target: &str) -> bool {
    target_is_darwin(target) && target_is_darwin(host)
}

fn target_is_darwin(triple: &str) -> bool {
    triple.contains("darwin") || triple.contains("apple")
}

/// A musl Linux target — the only family the [`MuslCross`](CrossTool::MuslCross)
/// fallback can carry.
fn is_musl_target(target: &str) -> bool {
    target.contains("musl")
}

/// The `<arch>-linux-musl` toolchain prefix for a musl target triple, e.g.
/// `x86_64-unknown-linux-musl` → `x86_64-linux-musl` (homebrew `musl-cross` /
/// `messense/<arch>-linux-musl-cross` naming). Strips the `-unknown` vendor
/// segment that the cross-gcc package names omit.
fn musl_cc_prefix(target: &str) -> String {
    format!("{}-linux-musl", arch_of(target))
}

/// Cargo's per-target env key: the triple upcased with `-` → `_`
/// (`x86_64-unknown-linux-musl` → `X86_64_UNKNOWN_LINUX_MUSL`), used in
/// `CARGO_TARGET_<KEY>_LINKER`.
fn cargo_target_env_key(target: &str) -> String {
    target.to_ascii_uppercase().replace('-', "_")
}

/// Run a probe argv and report whether it exited successfully. Any spawn
/// failure (binary absent) or non-zero exit reads as "unavailable". The shell
/// seam [`ToolAvailability::probe`] is built on.
fn probe_ok(argv: &[String]) -> bool {
    let Some((prog, args)) = argv.split_first() else {
        return false;
    };
    std::process::Command::new(prog)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// True when `target` is host-native crossable *and* foreign-arch — the set
/// this tier exists to carry. A thin predicate over
/// [`host_native_crossable`](crate::platform::host_native_crossable) for
/// callers that want to gate on "is this a NativeCross-tier target" without
/// re-running the full [`resolve`](crate::platform::resolve).
pub fn is_native_cross_target(host: &str, target: &str) -> bool {
    arch_of(target) != arch_of(host) && host_native_crossable(host, target)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARM_MAC: &str = "aarch64-apple-darwin";
    const X64_LINUX: &str = "x86_64-unknown-linux-gnu";
    const X64_MUSL: &str = "x86_64-unknown-linux-musl";

    // ── select_cross_tool decision table ────────────────────────────────────

    #[test]
    fn no_target_is_native() {
        assert_eq!(
            select_cross_tool(ARM_MAC, None, &ToolAvailability::FULL),
            Ok(CrossTool::CargoNative)
        );
        // Empty/whitespace target is treated as absent.
        assert_eq!(
            select_cross_tool(ARM_MAC, Some("  "), &ToolAvailability::NONE),
            Ok(CrossTool::CargoNative)
        );
    }

    #[test]
    fn host_arch_target_is_native_even_with_no_toolchains() {
        // Same arch, different OS → host SDK links it, no zig needed.
        assert_eq!(
            select_cross_tool(
                ARM_MAC,
                Some("aarch64-unknown-linux-musl"),
                &ToolAvailability::NONE
            ),
            Ok(CrossTool::CargoNative)
        );
    }

    #[test]
    fn darwin_cross_off_darwin_host_is_native() {
        // x86_64 darwin from arm64 mac: SDK has both slices, plain cargo.
        assert_eq!(
            select_cross_tool(
                ARM_MAC,
                Some("x86_64-apple-darwin"),
                &ToolAvailability::NONE
            ),
            Ok(CrossTool::CargoNative)
        );
    }

    #[test]
    fn foreign_linux_prefers_zigbuild() {
        // The mesofact target: x86_64 musl from arm64 mac → zigbuild.
        assert_eq!(
            select_cross_tool(ARM_MAC, Some(X64_MUSL), &ToolAvailability::FULL),
            Ok(CrossTool::CargoZigbuild)
        );
        // glibc foreign-arch too.
        assert_eq!(
            select_cross_tool(ARM_MAC, Some(X64_LINUX), &ToolAvailability::FULL),
            Ok(CrossTool::CargoZigbuild)
        );
    }

    #[test]
    fn windows_gnu_foreign_arch_prefers_zigbuild() {
        assert_eq!(
            select_cross_tool(
                ARM_MAC,
                Some("x86_64-pc-windows-gnu"),
                &ToolAvailability::FULL
            ),
            Ok(CrossTool::CargoZigbuild)
        );
    }

    #[test]
    fn musl_falls_back_to_musl_cross_when_no_zig() {
        let avail = ToolAvailability {
            zigbuild: false,
            musl_cross: true,
        };
        assert_eq!(
            select_cross_tool(ARM_MAC, Some(X64_MUSL), &avail),
            Ok(CrossTool::MuslCross)
        );
    }

    #[test]
    fn glibc_does_not_fall_back_to_musl_cross() {
        // musl-cross can't build glibc; with no zig there's no host-native path.
        let avail = ToolAvailability {
            zigbuild: false,
            musl_cross: true,
        };
        let err = select_cross_tool(ARM_MAC, Some(X64_LINUX), &avail).unwrap_err();
        assert_eq!(err.target, X64_LINUX);
        assert_eq!(err.preferred, CrossTool::CargoZigbuild);
    }

    #[test]
    fn nothing_installed_errors_with_install_hint() {
        let err = select_cross_tool(ARM_MAC, Some(X64_MUSL), &ToolAvailability::NONE).unwrap_err();
        assert_eq!(err.preferred, CrossTool::CargoZigbuild);
        let msg = err.to_string();
        assert!(msg.contains("cargo-zigbuild"), "names the install: {msg}");
        assert!(msg.contains(X64_MUSL), "names the target: {msg}");
    }

    /// Totality sweep: every (host, target, availability) class returns a
    /// value and never panics — the decision-table-as-spec guarantee.
    #[test]
    fn select_is_total_over_the_class_space() {
        let hosts = [ARM_MAC, X64_LINUX, "x86_64-pc-windows-msvc"];
        let targets = [
            None,
            Some(X64_MUSL),
            Some(X64_LINUX),
            Some("aarch64-apple-darwin"),
            Some("x86_64-pc-windows-gnu"),
            Some(""),
        ];
        let avails = [
            ToolAvailability::FULL,
            ToolAvailability::NONE,
            ToolAvailability {
                zigbuild: true,
                musl_cross: false,
            },
            ToolAvailability {
                zigbuild: false,
                musl_cross: true,
            },
        ];
        for h in hosts {
            for t in targets {
                for a in avails {
                    let _ = select_cross_tool(h, t, &a);
                }
            }
        }
    }

    // ── argv rewriting ──────────────────────────────────────────────────────

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn rewrites_cross_build_to_cargo_zigbuild() {
        let original = argv(&["cross", "build", "--release"]);
        let out = rewrite_build_argv(&original, &CrossTool::CargoZigbuild, X64_MUSL);
        assert_eq!(
            out,
            argv(&["cargo", "zigbuild", "--release", "--target", X64_MUSL])
        );
    }

    #[test]
    fn rewrites_cargo_build_and_keeps_existing_target() {
        // An explicit --target already present is preserved, not duplicated.
        let original = argv(&["cargo", "build", "--release", "--target", X64_MUSL]);
        let out = rewrite_build_argv(&original, &CrossTool::CargoZigbuild, X64_MUSL);
        assert_eq!(
            out,
            argv(&["cargo", "zigbuild", "--release", "--target", X64_MUSL])
        );
        assert_eq!(out.iter().filter(|a| *a == "--target").count(), 1);
    }

    #[test]
    fn rewrites_equals_form_target_without_duplicating() {
        let original = argv(&["cargo", "build", &format!("--target={X64_MUSL}")]);
        let out = rewrite_build_argv(&original, &CrossTool::CargoZigbuild, X64_MUSL);
        assert_eq!(out.iter().filter(|a| a.starts_with("--target")).count(), 1);
        assert_eq!(out[1], "zigbuild");
    }

    #[test]
    fn native_tool_keeps_build_subcommand() {
        let original = argv(&["cargo", "build", "--release"]);
        let out = rewrite_build_argv(&original, &CrossTool::CargoNative, "x86_64-apple-darwin");
        assert_eq!(out[1], "build");
        assert_eq!(out.last().unwrap(), "x86_64-apple-darwin");
    }

    #[test]
    fn unrecognized_argv_passes_through_unchanged() {
        // A bare script invocation isn't a cargo/cross build — we can't know
        // its flag syntax, so leave it verbatim rather than risk corrupting it.
        let original = argv(&["./build.sh", "--fast"]);
        let out = rewrite_build_argv(&original, &CrossTool::CargoZigbuild, X64_MUSL);
        assert_eq!(out, original);
    }

    // ── env wiring ──────────────────────────────────────────────────────────

    #[test]
    fn zigbuild_and_native_need_no_env() {
        assert!(CrossTool::CargoZigbuild.env_for(X64_MUSL).is_empty());
        assert!(CrossTool::CargoNative.env_for(X64_MUSL).is_empty());
    }

    #[test]
    fn musl_cross_wires_linker_cc_and_ar() {
        let env = CrossTool::MuslCross.env_for(X64_MUSL);
        let map: std::collections::HashMap<_, _> = env.into_iter().collect();
        assert_eq!(
            map.get("CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER")
                .map(String::as_str),
            Some("x86_64-linux-musl-gcc")
        );
        assert_eq!(
            map.get("CC_x86_64_unknown_linux_musl").map(String::as_str),
            Some("x86_64-linux-musl-gcc")
        );
        assert_eq!(
            map.get("AR_x86_64_unknown_linux_musl").map(String::as_str),
            Some("x86_64-linux-musl-ar")
        );
    }

    #[test]
    fn musl_cc_prefix_strips_vendor() {
        assert_eq!(
            musl_cc_prefix("x86_64-unknown-linux-musl"),
            "x86_64-linux-musl"
        );
        assert_eq!(
            musl_cc_prefix("aarch64-unknown-linux-musl"),
            "aarch64-linux-musl"
        );
    }

    // ── plan_native_cross end-to-end ────────────────────────────────────────

    #[test]
    fn plan_routes_the_mesofact_step_to_zigbuild() {
        // The exact W222 motivating case: arm64 mac, the cross-rs musl step.
        let original = argv(&["cross", "build", "--release", "-p", "almanac-serve"]);
        let plan =
            plan_native_cross(&original, ARM_MAC, X64_MUSL, &ToolAvailability::FULL).unwrap();
        assert_eq!(plan.tool, CrossTool::CargoZigbuild);
        assert_eq!(
            plan.argv,
            argv(&[
                "cargo",
                "zigbuild",
                "--release",
                "-p",
                "almanac-serve",
                "--target",
                X64_MUSL
            ])
        );
        assert!(plan.env.is_empty(), "zigbuild self-contains its sysroot");
    }

    #[test]
    fn plan_falls_back_to_musl_cross_with_env() {
        let avail = ToolAvailability {
            zigbuild: false,
            musl_cross: true,
        };
        let original = argv(&["cargo", "build"]);
        let plan = plan_native_cross(&original, ARM_MAC, X64_MUSL, &avail).unwrap();
        assert_eq!(plan.tool, CrossTool::MuslCross);
        assert_eq!(plan.argv, argv(&["cargo", "build", "--target", X64_MUSL]));
        assert!(!plan.env.is_empty(), "musl-cross needs linker env");
    }

    #[test]
    fn plan_errors_when_no_toolchain() {
        let err = plan_native_cross(
            &argv(&["cross", "build"]),
            ARM_MAC,
            X64_MUSL,
            &ToolAvailability::NONE,
        )
        .unwrap_err();
        assert_eq!(err.preferred, CrossTool::CargoZigbuild);
    }

    // ── labels / hints / predicate ──────────────────────────────────────────

    #[test]
    fn labels_and_hints_are_distinct_and_actionable() {
        assert_eq!(CrossTool::CargoZigbuild.label(), "cargo-zigbuild");
        assert_eq!(CrossTool::MuslCross.label(), "musl-cross");
        assert_eq!(CrossTool::CargoNative.label(), "native (cargo build)");
        assert!(CrossTool::CargoZigbuild
            .install_hint()
            .contains("cargo-zigbuild"));
        assert!(CrossTool::MuslCross.install_hint().contains("musl-cross"));
        assert!(CrossTool::CargoNative
            .install_hint()
            .contains("rustup target add"));
    }

    #[test]
    fn is_native_cross_target_matches_the_zigbuild_set() {
        // Foreign-arch crossable → yes.
        assert!(is_native_cross_target(ARM_MAC, X64_MUSL));
        // Same arch → no (it's a plain native build, not the cross tier).
        assert!(!is_native_cross_target(
            ARM_MAC,
            "aarch64-unknown-linux-gnu"
        ));
        // Foreign but non-crossable (darwin off linux) → no.
        assert!(!is_native_cross_target(X64_LINUX, "aarch64-apple-darwin"));
    }

    #[test]
    fn cargo_subcommand_maps_each_tool() {
        assert_eq!(CrossTool::CargoZigbuild.cargo_subcommand(), "zigbuild");
        assert_eq!(CrossTool::CargoNative.cargo_subcommand(), "build");
        assert_eq!(CrossTool::MuslCross.cargo_subcommand(), "build");
    }
}
