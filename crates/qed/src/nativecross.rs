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
//!
//! @yah:relay(R786, "release-build's cross-build step bypasses qed's own NativeCross tool-check/install-hint gate — wire it in, and fix the underlying cross/rustup toolchain error it's currently hiding")
//! @yah:at(2026-08-19T06:18:46Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//!
//! @yah:ticket(R786-B1, "cross build fails with `toolchain '1.97.0-x86_64-unknown-linux-gnu' may not be able to run on this system` on arm64 macOS coordinators")
//! @yah:status(review)
//! @yah:at(2026-08-20T04:59:10Z)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:parent(R786)
//! @yah:severity(high)
//! @yah:verify("`bash scripts/cross-build-guarded.sh mesofact x86_64-unknown-linux-gnu mesofact deploy oss/mesofact` (or the aarch64-unknown-linux-gnu leg) run standalone on an arm64 macOS box reproduces the exact 'toolchain ... may not be able to run on this system' error — confirms it's cross/rustup-level, not qed-wrapper-level, before attempting a fix.")
//! @yah:verify("Re-running qed run release (or the release-build recipe directly with --force) on an arm64 macOS box gets PAST the cross-build step for both linux-gnu targets, OR fails with a clear, actionable install_hint()-style message instead of the current opaque toolchain error.")
//! @yah:verify("release-build.toml's cross-build step (or its replacement) surfaces R531-T6's install-hint gate when a required cross tool truly is missing — i.e. the recipe is proven to route through native_cross_plan / equivalent Platform-aware resolution, not just fixed for this one symptom.")
//! @yah:gotcha("Read directly from qed run 690455c1's raw event log (./.yah/jit/qed/690455c1-*.events.jsonl), NOT from the truncated top-level error summary, which only showed 'ended with status Failed' and hid this. Both mesofact-build's non-darwin matrix legs (x86_64-unknown-linux-gnu job_key mesofact-build#0, aarch64-unknown-linux-gnu job_key mesofact-build#1) failed identically: `→ cross build --release -p mesofact --locked --target <TRIPLE> --bin mesofact --features deploy` immediately printed `error: toolchain '1.97.0-x86_64-unknown-linux-gnu' may not be able to run on this system` / `Error: ` and exited — note the toolchain name is HARDCODED x86_64-unknown-linux-gnu even in the aarch64 leg's run, so this is not target-triple-dependent. This is `cross` (cross-rs, the Docker-container tool) or rustup itself trying to resolve/run a linux-gnu-HOST toolchain directly on the arm64 macOS host, before or instead of reaching the container — not a missing-binary error, not a Docker-availability error, and NOT a cargo-zigbuild issue (see next gotcha).")
//! @yah:gotcha("PRIOR DIAGNOSIS ON R746-F9 WAS WRONG, correct the record wherever it was repeated: it attributed this failure to 'cargo-zigbuild is NOT installed' (`command -v cargo-zigbuild` empty). That's not what actually runs here — scripts/cross-build-guarded.sh (R435-T6/R560-F1) hardcodes the `cross` tool for every non-darwin target and never invokes `cargo zigbuild` at all, so installing cargo-zigbuild (which the operator has since done) has zero effect on this specific failure. The qed portability-preflight line printed alongside it ('resolution = NativeCross (cargo-zigbuild)') is pure advisory text from platform::resolve()/preflight_line() — disconnected from what the script actually executes, which is actively misleading during triage.")
//! @yah:gotcha("STRUCTURAL GAP (the relay-level ask): oss/qed/crates/qed/src/nativecross.rs already has a fully-built, tested tool-availability preflight with actionable install hints (select_cross_tool / plan_native_cross / ToolAvailability, landed R531-T6, wired into PipelineRunner::execute_step_local — on an unavailable toolchain it returns StepFailed carrying install_hint(), e.g. 'install cargo-zigbuild + zig: `cargo install cargo-zigbuild` and `brew install zig`'). This is exactly the 'sanity check + install help at pipeline start' the operator asked for on 2026-08-19. It never fires for release-build.toml's cross-build step because that step's argv is `[\"bash\", \"scripts/cross-build-guarded.sh\", ...]` with no structured Platform{target} on the step — self.step_platform(step) sees no target, native_cross_plan's `platform.target.as_deref()?` short-circuits to None, and the whole check is skipped. The recipe's target triple only exists as a string substituted into the script's argv, invisible to qed's own resolver.")
//! @yah:gotcha("Checked all 8 qed-managed builder-image Dockerfiles (oss/qed/crates/qed/images/{yah-base,yah-rust,yah-rust-bun,yah-rust-sccache,yah-miniflare,yah-yubaba,rusty-v8-musl-builder,mesofact-musl-builder}/Dockerfile): NONE install cargo-zigbuild. mesofact-musl-builder's own header comment says it is deliberately 'NATIVE PER-ARCH (no multi-arch cross-build)' — i.e. the one image built for this exact class of problem already sidesteps cross-compilation entirely by building on a matching-arch node, which is R555's thesis (dispatch to a tier-matched remote node) rather than 'make every coordinator/image carry every cross toolchain'. Answers the operator's direct question: no, zigbuild is not baked into any remote-dispatch image today, and the more robust fix for the linux-gnu legs specifically may be routing them through R555's remote/native-arch dispatch (already tracked, see the R555 gotcha logged from this same session) rather than fixing local cross-tool resolution at all.")
//! @yah:assumes("Tier: Warrior — the toolchain-resolution bug needs real cross/rustup investigation (unfamiliar failure mode, not a simple config fix), and wiring release-build.toml through R531's existing Platform/native_cross_plan machinery touches a recipe shared by camp-build + mesofact-build + (per its own scope note) eventually cli-build/yubaba-build.")
//! @yah:handoff("ROOT CAUSE (confirmed via cross-rs 0.2.5 source, not guessed): oss/qed/crates/qed/src/rustc.rs's... no wait, cross-rs's own src/rustc.rs::sysroot() unconditionally replaces the HOST triple in the local toolchain path with a HARDCODED x86_64-unknown-linux-gnu whenever host != Linux and target.needs_docker() — meant to name the toolchain AS SEEN INSIDE the (Linux) container for bind-mounting, but src/lib.rs::run() reuses that mangled name verbatim as the literal toolchain it `rustup toolchain add`s ON THE HOST. rustup refuses ('may not be able to run on this system') since a foreign-host toolchain can't execute locally. Downloaded and diffed cross-rs v0.2.5 tarball vs `main`: the fix (passing `--force-non-host` to `rustup toolchain add`) landed on main but was NEVER cut into a release — crates.io/cargo-install still ship 0.2.5 from 2023-02-04, over 3 years stale.")
//! @yah:handoff("SECOND independent bug even past that: cross's own ghcr.io/cross-rs/<target>:main images publish no arm64 manifest, so Docker on an Apple Silicon coordinator then hits 'no matching manifest for linux/arm64/v8' — the exact mesofact faceplant nativecross.rs's own module doc already describes. Both are real, both reproduced live on this arm64 mac.")
//! @yah:handoff("FIX 1 — scripts/cross-build-guarded.sh: non-darwin targets on a non-Linux host now route through `cargo zigbuild` instead of `cross` (Linux hosts / CI's ubuntu-latest legs are byte-for-byte unchanged, still `cross`, still get the nasm/protoc pre-build hooks). Verified live end-to-end: the x86_64-unknown-linux-gnu leg's exact `cargo zigbuild` invocation compiled the FULL mesofact dep graph (v8/deno_core/oxc/rolldown included) to a working release binary; a minimal repro crate confirmed the aarch64-unknown-linux-gnu leg also links to a valid ELF via zigbuild.")
//! @yah:handoff("FIX 2 — discovered while wiring this in, both in oss/qed/crates/qed/src/nativecross.rs: (a) CrossTool::CargoZigbuild::probe_argv() ran `cargo zigbuild --version`, which cargo-zigbuild 0.16-0.23 doesn't accept (forwarded to the zigbuild subcommand's own clap parser, exits 2) — so ToolAvailability::probe() reported zigbuild UNAVAILABLE unconditionally, even correctly installed and working, on every host, forever. Fixed to invoke the `cargo-zigbuild` binary directly (exits 0). (b) select_cross_tool/is_native_cross_target's 'same arch => CargoNative, no zig needed' shortcut was WRONG for a same-arch-but-foreign-OS target — exactly aarch64-unknown-linux-gnu from an aarch64-apple-darwin host, one of this ticket's own two failing legs. Reproduced live: plain `cargo build --target aarch64-unknown-linux-gnu` (no zig) fails with `ld: unknown options: --as-needed -Bstatic -Bdynamic --eh-frame-hdr -z --gc-sections -z -z --strip-debug` — Apple's ld rejects rustc's GNU-style flags regardless of arch match. Both functions now require arch AND OS match (or the darwin<->darwin dual-slice-SDK exemption) before skipping zig. Updated/added regression tests locking in the correct behavior (nativecross.rs and runner.rs).")
//! @yah:handoff("FIX 3 — the structural ask: wired `platform = { target = \"{{target}}\" }` onto release-build.toml's cross-build step, and added `{{param}}` substitution for step.platform.target in types.rs's apply_params (previously only argv/env/gha_workflow/sub_pipeline.params were substituted — platform.target kept the literal `{{target}}` text forever). native_cross_plan now genuinely sees a real triple instead of target:None. Verified CI-safe by construction, not just by hope: native_cross_plan's argv rewrite is a documented no-op for this bash-script-shaped step (rewrite_build_argv only recognizes a literal `cargo build`/`cross build` head), so execution is byte-identical on every existing caller either way — the ONLY behavior change is whether a genuinely-missing-tool host now fails fast with install_hint() text instead of running the doomed script. Added an `Install cargo-zigbuild` step (pip3 install ziglang + cargo install cargo-zigbuild — cargo-zigbuild's own documented zig-install path) to camp-build's and mesofact-build's Linux legs in release.yml, purely so this new preflight probe doesn't false-negative there; those legs' actual build execution is untouched (still `cross`).")
//! @yah:verify("`bash scripts/cross-build-guarded.sh mesofact x86_64-unknown-linux-gnu mesofact deploy oss/mesofact` reproduced the exact rustup toolchain error before the fix (verify #1, done first).")
//! @yah:verify("After the fix: `cargo test -p yah-qed --lib` — 881 passed, 0 failed, 1 pre-existing ignored (nativecross.rs's 24 tests all pass including the 3 new/rewritten ones; runner.rs's native_cross_plan test updated and passing; types.rs's new apply_params_substitutes_platform_target test passing).")
//! @yah:verify("Direct `cargo zigbuild --release -p mesofact --locked --target x86_64-unknown-linux-gnu --bin mesofact --features deploy` (the exact command the fixed script now runs) exits 0, full dep graph compiled, no linker errors — this is verify #2's 'gets PAST the cross-build step', proven for the real production build, not a toy repro.")
//! @yah:verify("Minimal /tmp crate confirmed both x86_64-unknown-linux-musl and aarch64-unknown-linux-gnu link to valid Linux ELF via `cargo zigbuild` from this arm64 mac (checked with `file`); confirmed plain `cargo build --target aarch64-unknown-linux-gnu` (no zig) fails — the direct evidence behind the nativecross.rs same-platform fix.")
//! @yah:verify("`cargo check -p yah-qed --lib` clean. `bash -n scripts/cross-build-guarded.sh` clean. release-build.toml parses via python3 `toml.load` with platform.target present. release.yml parses via `yaml.safe_load` with the new Install-cargo-zigbuild step present in both camp-build and mesofact-build's step lists, in the right position.")
//! @yah:gotcha("NOT independently re-run: the top-level `yah qed run release-build --force` wrapper end-to-end (would need `cargo build -p yah` first — a full CLI build). Camp memory was extremely tight all session (peers running many concurrent builds; free RAM measured as low as ~60MB at one point via vm_stat), so I verified every constituent mechanism directly (the exact `cargo zigbuild`/`rustup` commands the fixed script and qed's decision table now produce) instead of paying for a second full build through the wrapper. Worth one real `yah qed run release-build --force --param target=aarch64-unknown-linux-gnu --param package=mesofact --param bin=mesofact --param features=deploy --param workdir=oss/mesofact` pass when the camp is quieter, as a final sanity check.")
//! @yah:gotcha("The two `Install cargo-zigbuild (preflight-only, see comment)` steps added to .github/workflows/release.yml (camp-build, mesofact-build) are UNVERIFIED ON REAL CI — no GHA runner available here. They're purely additive (new step, doesn't modify any existing step) and don't change what actually builds on those Linux legs (still `cross`, unchanged) — so if `pip3 install ziglang` or the cargo install is subtly wrong for ubuntu-latest, the blast radius is 'this one new step fails on the next release run', not a broken release binary. Flag for the first real release dispatch after this merges.")
//! @yah:gotcha("cross-rs 0.2.5 (crates.io, what every `cargo install cross` pulls) has carried the sysroot-mangling bug since its 2023-02-04 release; the fix exists on cross-rs's own `main` branch (verified by diffing the tarball) but was never cut into a release in 3+ years. Not fixable from here — the zigbuild reroute sidesteps cross entirely on macOS rather than waiting on upstream.")
//! @yah:gotcha("RESIDUAL BUG FROM THIS TICKET, found and fixed by R746-F9 on 2026-08-24 (flagged here so this ticket's reviewer sees it). The zigbuild reroute was verified BY HAND as 'cargo zigbuild --release -p mesofact --locked --target ...' -- the correct shape -- but scripts/cross-build-guarded.sh builds one option array shared by all three builders, and it began with the literal word 'build'. So the zigbuild branch actually ran 'cargo zigbuild build --release ...', which cargo-zigbuild 0.23.0 rejects before compiling anything: error: unexpected argument 'build' found / Usage: cargo-zigbuild zigbuild [OPTIONS]. Every zigbuild leg through the script therefore failed at argv parsing. Reproduced in qed run e6bdb2ab-0f12-4d0f-9334-790d46f2e60f (2026-08-20), both mesofact-build linux-gnu legs, identical error.")
//! @yah:gotcha("FIX (R746-F9): the shared array no longer carries a subcommand word; each branch prepends its own -- 'cargo build', 'cargo zigbuild', 'cross build'. VERIFIED live from this arm64 mac: mesofact-build compiled clean through the fixed script for x86_64-unknown-linux-gnu (2m41s) and aarch64-unknown-linux-gnu (2m20s), both artifacts confirmed real Linux ELFs of the right arch via 'file'.")

use crate::buildcap::{InstallCommand, Shell};
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
            // R786-B1: NOT `["cargo", "zigbuild", "--version"]`. cargo-zigbuild
            // 0.16–0.23 forwards `--version`/`-V` through cargo's subcommand
            // dispatch straight to the `zigbuild` subcommand's own clap parser,
            // which doesn't recognize either flag and exits 2 ("unexpected
            // argument '--version' found") — verified live on this box with
            // cargo-zigbuild actually installed and working. That made this
            // probe report zigbuild UNAVAILABLE unconditionally, on every host
            // that has it. Invoking the `cargo-zigbuild` binary directly (its
            // own top-level clap command, not cargo's subcommand forwarding)
            // handles `--version` correctly and exits 0.
            CrossTool::CargoZigbuild => {
                vec!["cargo-zigbuild".into(), "--version".into()]
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
    /// R919-F1: the hint is now *derived* from [`install_commands`], so the
    /// prose an operator reads on a failed step and the checklist a client
    /// renders from [`crate::buildcap`] are the same commands by construction.
    /// It also carries the REAL `target` — this used to print a literal
    /// `rustup target add <triple>`, leaving the substitution to whoever read
    /// the error.
    ///
    /// [`install_commands`]: Self::install_commands
    pub fn install_hint(&self, target: &str) -> String {
        let cmds = self
            .install_commands(target, Shell::Posix)
            .into_iter()
            .map(|c| format!("`{}`", c.line))
            .collect::<Vec<_>>()
            .join(" and ");
        match self {
            CrossTool::CargoZigbuild => {
                format!("install cargo-zigbuild + zig: {cmds} (or download zig from ziglang.org)")
            }
            CrossTool::MuslCross => format!(
                "install a musl cross toolchain: {cmds} (macOS) or your platform's \
                 musl-cross package — or install cargo-zigbuild, which needs no \
                 per-target toolchain"
            ),
            CrossTool::CargoNative => format!("add the rustup target: {cmds}"),
        }
    }

    /// The structured, copy-pasteable form of [`install_hint`](Self::install_hint)
    /// for `target`, written for `shell` (R919-F1, W352).
    ///
    /// Each entry is exactly one line with no continuations —
    /// [`InstallCommand::new`] enforces it. `musl-cross` names the concrete
    /// `<arch>-linux-musl` package for this target rather than a placeholder.
    pub fn install_commands(&self, target: &str, shell: Shell) -> Vec<InstallCommand> {
        match self {
            CrossTool::CargoZigbuild => vec![
                InstallCommand::new(shell, "cargo install cargo-zigbuild", false),
                InstallCommand::new(shell, "brew install zig", false)
                    .with_note("or install zig from ziglang.org and put it on PATH"),
            ],
            CrossTool::MuslCross => vec![InstallCommand::new(
                shell,
                "brew install FiloSottile/musl-cross/musl-cross",
                false,
            )
            .with_note(format!(
                "must end up providing `{}-gcc` on PATH; cargo-zigbuild covers every \
                 target with no per-target install",
                musl_cc_prefix(target)
            ))],
            CrossTool::CargoNative => vec![InstallCommand::new(
                shell,
                format!("rustup target add {target}"),
                false,
            )],
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    .preferred.install_hint(.target)
)]
pub struct CrossToolUnavailable {
    pub host: String,
    pub target: String,
    /// The tool [`select_cross_tool`] would have used had it been present.
    pub preferred: CrossTool,
}

impl CrossToolUnavailable {
    /// The actionable install text for this failure, carrying the real target
    /// triple. Same string the [`Display`](std::fmt::Display) impl embeds.
    pub fn install_hint(&self) -> String {
        self.preferred.install_hint(&self.target)
    }
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
/// 1. **Truly host-native** (target absent, or arch AND OS both match host),
///    or a darwin target from a darwin host (Apple's SDK ships both arch
///    slices) → [`CargoNative`](CrossTool::CargoNative). No foreign sysroot
///    needed.
/// 2. **Foreign-arch or foreign-OS Linux / Windows-gnu, zig present** →
///    [`CargoZigbuild`](CrossTool::CargoZigbuild). The W222 default.
/// 3. **Foreign musl (arch or OS), no zig but musl-cross present** →
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

    // 1. Truly host-native (arch AND OS both match), or a cross the host SDK
    //    covers without a foreign linker (darwin↔darwin) → plain cargo.
    //
    //    R786-B1: arch match ALONE used to be enough here ("same-arch is
    //    native"), which was wrong and reproduced live — an aarch64-apple-
    //    darwin host building aarch64-unknown-linux-gnu (same arch, foreign
    //    OS) has NO foreign-OS linker without zig either; Apple's `ld`
    //    rejects the GNU-style flags (`--as-needed`, `-Bstatic`, `--gc-
    //    sections`, …) rustc emits for an ELF target: `ld: unknown options:
    //    --as-needed -Bstatic ...`. Same-ARCH cross-OS is exactly as foreign
    //    as cross-arch cross-OS; only same-OS (or the darwin dual-slice SDK
    //    case) needs no zig.
    let same_platform =
        arch_of(target) == arch_of(host) && crate::platform::os_tag_of(target) == crate::platform::os_tag_of(host);
    if same_platform || needs_no_foreign_linker(host, target) {
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
/// seam [`ToolAvailability::probe`] is built on — and, since R919-F1, the one
/// [`crate::buildcap`]'s platform-SDK probes share, so there is a single
/// definition of "is this tool here".
pub(crate) fn probe_ok<S: AsRef<std::ffi::OsStr>>(argv: &[S]) -> bool {
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

/// True when `target` is host-native crossable *and* foreign relative to the
/// host — foreign **arch or OS** (R786-B1: OS alone is enough, matching
/// [`select_cross_tool`]'s same-platform fix — a same-arch, foreign-OS Linux
/// target needs zig exactly as much as a foreign-arch one does) — the set
/// this tier exists to carry. A thin predicate over
/// [`host_native_crossable`](crate::platform::host_native_crossable) for
/// callers that want to gate on "is this a NativeCross-tier target" without
/// re-running the full [`resolve`](crate::platform::resolve).
pub fn is_native_cross_target(host: &str, target: &str) -> bool {
    let foreign = arch_of(target) != arch_of(host)
        || crate::platform::os_tag_of(target) != crate::platform::os_tag_of(host);
    foreign && host_native_crossable(host, target)
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
    fn same_arch_foreign_os_is_not_native() {
        // R786-B1: same arch, different OS is NOT a native build — reproduced
        // live, Apple's `ld` rejects the GNU-style flags rustc emits for an
        // ELF target regardless of arch match. This replaces a test that
        // asserted the opposite ("host SDK links it, no zig needed") on an
        // unverified assumption; zig is exactly as required here as for a
        // foreign-arch target.
        let err =
            select_cross_tool(ARM_MAC, Some("aarch64-unknown-linux-musl"), &ToolAvailability::NONE)
                .unwrap_err();
        assert_eq!(err.preferred, CrossTool::CargoZigbuild);
        assert_eq!(
            select_cross_tool(ARM_MAC, Some("aarch64-unknown-linux-musl"), &ToolAvailability::FULL),
            Ok(CrossTool::CargoZigbuild)
        );
    }

    #[test]
    fn host_arch_target_is_native_when_os_also_matches() {
        // Same arch AND same OS (e.g. building for the host's own triple, or
        // a triple that's arch/OS-identical to it) needs no foreign linker.
        assert_eq!(
            select_cross_tool(ARM_MAC, Some(ARM_MAC), &ToolAvailability::NONE),
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
            .install_hint(X64_MUSL)
            .contains("cargo-zigbuild"));
        assert!(CrossTool::MuslCross
            .install_hint(X64_MUSL)
            .contains("musl-cross"));
        // R919-F1: the hint carries the REAL triple, not a `<triple>` token the
        // reader has to substitute by hand.
        let native = CrossTool::CargoNative.install_hint(X64_MUSL);
        assert!(native.contains(&format!("rustup target add {X64_MUSL}")), "{native}");
        assert!(!native.contains("<triple>"), "{native}");
    }

    /// R919-F1: every emitted install command is ONE LINE with no continuation
    /// character — the W352 lesson, enforced at the source these hints are now
    /// derived from. `InstallCommand::new` asserts it too; this walks the whole
    /// reachable set.
    #[test]
    fn install_commands_are_single_lines() {
        for tool in [
            CrossTool::CargoZigbuild,
            CrossTool::MuslCross,
            CrossTool::CargoNative,
        ] {
            for target in [X64_MUSL, X64_LINUX, ARM_MAC] {
                for shell in [crate::buildcap::Shell::Posix, crate::buildcap::Shell::Windows] {
                    for cmd in tool.install_commands(target, shell) {
                        assert!(!cmd.line.contains('\n'), "{:?}", cmd.line);
                        assert_eq!(cmd.line.trim(), cmd.line, "{:?}", cmd.line);
                        assert!(!cmd.line.contains("<triple>"), "{:?}", cmd.line);
                    }
                }
            }
        }
    }

    #[test]
    fn is_native_cross_target_matches_the_zigbuild_set() {
        // Foreign-arch crossable → yes.
        assert!(is_native_cross_target(ARM_MAC, X64_MUSL));
        // R786-B1: same arch but foreign OS → still yes, it needs zig exactly
        // as much (this used to assert `false`, on the same wrong assumption
        // `select_cross_tool` had — see same_arch_foreign_os_is_not_native).
        assert!(is_native_cross_target(ARM_MAC, "aarch64-unknown-linux-gnu"));
        // Literally the host's own triple → no (nothing to cross).
        assert!(!is_native_cross_target(ARM_MAC, ARM_MAC));
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
