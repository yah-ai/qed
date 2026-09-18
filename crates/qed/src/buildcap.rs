//! Build-capacity preflight (R919-F1, W352) — "can **this** machine build
//! target `T`, and if not, what exactly do I type?"
//!
//! ## Why this exists separately from the two preflights next to it
//!
//! QED already answers two adjacent questions and neither is this one:
//!
//! - [`crate::platform::resolve`] answers *which tier* a (host, target) pair
//!   lands in — NativeCross / CrossDocker / Emulate / Offload. It is a routing
//!   verdict; it does not say what is installed or how to fix a gap.
//! - [`crate::nativecross::select_cross_tool`] answers *which cross toolchain*
//!   carries a target and, on a miss, hands back
//!   [`CrossToolUnavailable`](crate::nativecross::CrossToolUnavailable) with a
//!   prose [`install_hint`](crate::nativecross::CrossTool::install_hint). That
//!   is the right mechanism and this module **extends** it rather than
//!   inventing a second one — but the hint is free text produced at the moment
//!   a step is already failing, and a client cannot render a checklist from it.
//!
//! What a client needs is the same facts *before* anything runs, keyed and
//! structured: one row per requirement, each with a probe, a present/absent
//! bit, and the literal command line to fix it. That is [`BuildCapacity`].
//!
//! ## Scope — this module reports, it never installs
//!
//! Every install command here is **text handed to a human**. Nothing in this
//! module spawns an installer, and the only subprocesses it runs are the
//! `--version`-shaped probes in [`HostProbe::probe`]. Several of these commands
//! need an elevated shell and run a third-party installer (`winget`, `brew`,
//! `rustup-init`), so consent has to be explicit and per-command at whatever
//! surface renders them — [`InstallCommand::elevated`] exists so that surface
//! can say so.
//!
//! ## One line per command, always
//!
//! [`InstallCommand::line`] is a single command with no continuations, and
//! [`InstallCommand::new`] panics on a line containing a newline or ending in a
//! continuation character. This is not fussiness: the wrapped, backtick-
//! continued PowerShell form of the winget commands below **failed on first
//! use** (operator, 2026-09-16). PowerShell's backtick must be the final
//! character on the line with no trailing whitespace, and under a `cmd` profile
//! it is not a continuation character at all — so a doc that wraps for
//! readability hands the operator something that does not run. The invariant is
//! enforced at construction and locked by the `every_install_command_is_one_line`
//! test below, which walks the whole reachable command set rather than the few
//! a hand-written test would name.
//!
//! ## Shape
//!
//! [`plan`] is pure and total over ([`host`](str), [`target`](str),
//! [`HostProbe`]) — the same decision-table-as-spec discipline as
//! [`select_cross_tool`](crate::nativecross::select_cross_tool). [`HostProbe`]
//! is the single impure seam.

use serde::{Deserialize, Serialize};

use crate::nativecross::{select_cross_tool, CrossTool, ToolAvailability};
use crate::platform::{os_tag_of, target_is_msvc};

/// Which shell an [`InstallCommand`] is written for. A client picks the set
/// matching the machine it is offering the commands on; it is never a guess
/// about where the command *can* run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Shell {
    /// `sh`/`bash`/`zsh` — macOS and Linux.
    Posix,
    /// Windows PowerShell or `cmd`. Every command emitted for this shell is
    /// written so it runs identically under both, which is why continuations
    /// are forbidden (see the module docs).
    Windows,
}

/// One command a human can copy, paste and run — exactly one line, no
/// continuations, no shell metacharacters that depend on the profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallCommand {
    /// Shell the line is written for.
    pub shell: Shell,
    /// The command. Invariant: one line, no trailing continuation character.
    pub line: String,
    /// Needs an Administrator / `sudo` shell. A rendering surface must say so
    /// before the operator runs it — this is the consent bit.
    pub elevated: bool,
    /// Anything the operator needs to know before running it (download size, a
    /// flag that is load-bearing, a reboot).
    pub note: Option<String>,
}

impl InstallCommand {
    /// Build a command, enforcing the one-line invariant.
    ///
    /// # Panics
    ///
    /// If `line` is empty, contains a newline, or ends in `` ` `` / `\` / `^`
    /// (the three continuation characters of PowerShell, POSIX shells and
    /// `cmd`). Every caller is a `const`-shaped literal in this module, so a
    /// panic here is a compile-adjacent authoring error caught by
    /// the `every_install_command_is_one_line` test, never a runtime condition.
    pub fn new(shell: Shell, line: impl Into<String>, elevated: bool) -> InstallCommand {
        let line = line.into();
        assert!(!line.trim().is_empty(), "install command is empty");
        assert!(
            !line.contains('\n') && !line.contains('\r'),
            "install command must be ONE LINE (W352): {line:?}"
        );
        assert!(
            !line.trim_end().ends_with('`')
                && !line.trim_end().ends_with('\\')
                && !line.trim_end().ends_with('^'),
            "install command must not end in a continuation character (W352): {line:?}"
        );
        InstallCommand {
            shell,
            line,
            elevated,
            note: None,
        }
    }

    /// Attach an operator-facing note.
    pub fn with_note(mut self, note: impl Into<String>) -> InstallCommand {
        self.note = Some(note.into());
        self
    }
}

/// One thing that must be present for a target to build here, with the probe
/// that decides it and the commands that install it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Requirement {
    /// Stable machine key a client can render against or dedupe on
    /// (`msvc-build-tools`, `rustup`, `rustup-target`, `cargo-zigbuild`,
    /// `musl-cross`). Stable across versions; the label is not.
    pub id: String,
    /// Human label.
    pub label: String,
    /// `true` = probed present, `false` = probed absent, `None` = not probed on
    /// this host because the requirement belongs to a *different* machine (see
    /// [`Capacity::ForeignHost`]).
    pub present: Option<bool>,
    /// The argv whose exit status is the presence signal, for an operator who
    /// wants to check by hand. Empty when presence is derived rather than
    /// probed by one command.
    pub probe: Vec<String>,
    /// Commands that make it present. Empty when nothing installable fixes it
    /// (an Apple SDK on a Linux box).
    pub install: Vec<InstallCommand>,
    /// OS tag (`linux` / `darwin` / `windows`) the install commands are written
    /// for — the machine that has to run them, which is not always the machine
    /// asking.
    pub install_on: String,
}

impl Requirement {
    fn missing(&self) -> bool {
        self.present != Some(true)
    }
}

/// The verdict. Total over the input space of [`plan`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Capacity {
    /// Everything this target needs is installed here.
    Ready,
    /// This host *can* build the target once the listed requirements are
    /// installed. The actionable case — every missing requirement carries its
    /// command.
    Missing,
    /// No install on this host helps: the target needs a machine running
    /// `needs_os`. The requirements are still populated (with `present: None`)
    /// so the asking client can hand the checklist to that machine — which is
    /// exactly the camp-peer capacity story in W352.
    ForeignHost {
        /// OS tag of the machine that could build this.
        needs_os: String,
        /// Why this host cannot, in one clause.
        reason: String,
    },
    /// QED cannot name a path to this target at all (unrecognized OS in the
    /// triple). Not a missing-tool problem; carries the reason rather than
    /// pretending a command would help.
    Unsupported {
        /// Why no path exists.
        reason: String,
    },
}

/// The answer: what `target` needs on `host`, and what is missing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildCapacity {
    /// Host triple the question was asked about.
    pub host: String,
    /// Target triple. Empty means "host-native".
    pub target: String,
    /// The verdict.
    pub verdict: Capacity,
    /// Every requirement, present ones included — a client that shows only the
    /// gaps has nothing to show on a healthy machine, and "nothing" reads as
    /// "not checked".
    pub requirements: Vec<Requirement>,
}

impl BuildCapacity {
    /// Requirements that are absent (or unprobed, on a foreign host).
    pub fn missing(&self) -> impl Iterator<Item = &Requirement> {
        self.requirements.iter().filter(|r| r.missing())
    }

    /// `true` only for [`Capacity::Ready`].
    pub fn is_ready(&self) -> bool {
        matches!(self.verdict, Capacity::Ready)
    }

    /// Human-readable report, one line per requirement plus the commands —
    /// what `yah qed capacity` prints. Commands are emitted one per line,
    /// unindented-but-for-the-prefix, so a copy-paste picks up exactly one.
    pub fn report(&self) -> Vec<String> {
        let mut out = Vec::new();
        let what = if self.target.is_empty() {
            "host-native".to_string()
        } else {
            self.target.clone()
        };
        out.push(match &self.verdict {
            Capacity::Ready => format!("{what} on {} — READY", self.host),
            Capacity::Missing => format!(
                "{what} on {} — {} requirement(s) missing",
                self.host,
                self.missing().count()
            ),
            Capacity::ForeignHost { needs_os, reason } => format!(
                "{what} on {} — NOT BUILDABLE HERE: {reason}; needs a {needs_os} machine",
                self.host
            ),
            Capacity::Unsupported { reason } => {
                format!("{what} on {} — UNSUPPORTED: {reason}", self.host)
            }
        });
        for req in &self.requirements {
            let mark = match req.present {
                Some(true) => "✓",
                Some(false) => "✗",
                None => "?",
            };
            out.push(format!("  {mark} {} ({})", req.label, req.id));
            if req.present == Some(true) {
                continue;
            }
            for cmd in &req.install {
                let elev = if cmd.elevated {
                    " [needs an elevated shell]"
                } else {
                    ""
                };
                out.push(format!("      $ {}{elev}", cmd.line));
                if let Some(note) = &cmd.note {
                    out.push(format!("        — {note}"));
                }
            }
            if req.install.is_empty() {
                out.push("      (nothing installable fixes this here)".to_string());
            }
        }
        out
    }
}

/// Everything impure the decision needs, gathered once. Build it with
/// [`probe`](Self::probe) at runtime or by hand in tests so the decision table
/// is *specified* — the same split [`ToolAvailability`] uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostProbe {
    /// Host-native cross toolchains (zig, musl-cross).
    pub cross: ToolAvailability,
    /// `rustup` resolves.
    pub rustup: bool,
    /// Triples `rustup target list --installed` reports. Empty when `rustup` is
    /// absent.
    pub rustup_targets: Vec<String>,
    /// An MSVC build-tools install was found (`vswhere` answered). Always
    /// `false` off Windows.
    pub msvc: bool,
    /// The Xcode command line tools are selected (`xcode-select -p`). Always
    /// `false` off macOS.
    pub xcode_clt: bool,
}

impl HostProbe {
    /// Nothing installed — the empty end of the table.
    pub fn none() -> HostProbe {
        HostProbe {
            cross: ToolAvailability::NONE,
            rustup: false,
            rustup_targets: Vec::new(),
            msvc: false,
            xcode_clt: false,
        }
    }

    /// Everything installed, and `targets` are the rustup targets added.
    pub fn full(targets: &[&str]) -> HostProbe {
        HostProbe {
            cross: ToolAvailability::FULL,
            rustup: true,
            rustup_targets: targets.iter().map(|t| t.to_string()).collect(),
            msvc: true,
            xcode_clt: true,
        }
    }

    /// Probe this host. The one impure entry point: runs the cross-tool
    /// `--version` probes, `rustup target list --installed`, and the
    /// platform-gated MSVC / Xcode probes. Runs no installer.
    pub fn probe() -> HostProbe {
        let rustup_targets = rustup_installed_targets();
        HostProbe {
            cross: ToolAvailability::probe(),
            rustup: rustup_targets.is_some(),
            rustup_targets: rustup_targets.unwrap_or_default(),
            // `vswhere` only exists on Windows and `xcode-select` only on
            // macOS; probing either elsewhere is a guaranteed spawn failure, so
            // skip it and report the honest `false` rather than paying for the
            // miss. The MSVC probe is the existing
            // [`Tool::Msvc`](crate::toolchain::Tool::Msvc) one — reused, not
            // reinvented.
            msvc: cfg!(windows) && crate::toolchain::Tool::Msvc.probe_version().is_some(),
            xcode_clt: cfg!(target_os = "macos")
                && crate::nativecross::probe_ok(&["xcode-select", "-p"]),
        }
    }

    fn has_target(&self, target: &str) -> bool {
        self.rustup_targets.iter().any(|t| t == target)
    }
}

/// `rustup target list --installed`, or `None` when rustup isn't there.
fn rustup_installed_targets() -> Option<Vec<String>> {
    let out = std::process::Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect(),
    )
}

/// The pure, total decision function. Given a host triple, a target triple
/// (empty = host-native) and what is installed, return exactly one
/// [`BuildCapacity`].
///
/// Decision order:
/// 1. **Foreign host** — a `*-apple-darwin` target off a Mac, or a
///    `*-pc-windows-msvc` target off Windows. Neither is an install problem
///    on this box: [`Capacity::ForeignHost`], with the target machine's own
///    checklist attached (`present: None`) so the answer is still actionable
///    for whoever owns that machine.
/// 2. **Unknown OS in the triple** — [`Capacity::Unsupported`].
/// 3. Otherwise gather requirements: the MSVC toolchain (only reached when the
///    host *is* Windows), the cross tool
///    [`select_cross_tool`] picks, `rustup` itself, and the rustup target.
///    [`Capacity::Ready`] iff all are present, else [`Capacity::Missing`].
pub fn plan(host: &str, target: &str, probe: &HostProbe) -> BuildCapacity {
    // An empty target means "host-native", which is the same question asked of
    // the host's own triple — NOT a question with fewer requirements. A bare
    // `cargo build` on Windows still needs the MSVC linker, and on macOS still
    // needs the Xcode command line tools. `requested` keeps the empty string
    // for display so the report says "host-native" rather than echoing a triple
    // the operator never typed.
    let requested = target.trim();
    let target = if requested.is_empty() { host } else { requested };
    let requested = requested.to_string();
    let host_os = os_tag_of(host);
    let target_os = os_tag_of(target);

    // 1. Foreign-host gates. Both of these are structural: the Apple SDK is not
    //    redistributable, and nothing cross-compiles to MSVC.
    if target_os == "darwin" && host_os != "darwin" {
        return BuildCapacity {
            host: host.to_string(),
            target: requested,
            verdict: Capacity::ForeignHost {
                needs_os: "darwin".to_string(),
                reason: "the macOS SDK and codesign are not redistributable".to_string(),
            },
            requirements: foreign_requirements("darwin", target),
        };
    }
    if target_os == "windows" && target_is_msvc(target) && host_os != "windows" {
        return BuildCapacity {
            host: host.to_string(),
            target: requested,
            verdict: Capacity::ForeignHost {
                needs_os: "windows".to_string(),
                reason: "nothing cross-compiles to the MSVC ABI".to_string(),
            },
            requirements: foreign_requirements("windows", target),
        };
    }
    if target_os == "unknown" {
        return BuildCapacity {
            host: host.to_string(),
            target: requested,
            verdict: Capacity::Unsupported {
                reason: format!("`{target}` names an OS QED has no build path for"),
            },
            requirements: Vec::new(),
        };
    }

    // 2. Requirements for a target this host can actually carry.
    let mut requirements = Vec::new();

    // The platform SDK. Each is only reachable with a matching host_os, thanks
    // to the gates above — so these rows are always about THIS machine.
    if target_is_msvc(target) {
        requirements.push(msvc_requirement(Some(probe.msvc)));
    }
    if target_os == "darwin" {
        requirements.push(xcode_clt_requirement(Some(probe.xcode_clt)));
    }

    // The cross toolchain. `Ok(CargoNative)` needs no extra tool — the rustup
    // target below is the whole requirement.
    match select_cross_tool(host, Some(target), &probe.cross) {
        Ok(CrossTool::CargoNative) => {}
        Ok(tool) => requirements.push(cross_requirement(&tool, target, host_os, Some(true))),
        Err(err) => {
            requirements.push(cross_requirement(&err.preferred, target, host_os, Some(false)))
        }
    }

    requirements.push(rustup_requirement(host_os, Some(probe.rustup)));
    // rustup installs the host triple's std with the toolchain, so asking for a
    // `rustup target add <host>` would be a row that can never be actioned and
    // never needs to be.
    if target != host {
        requirements.push(rustup_target_requirement(
            target,
            host_os,
            Some(probe.rustup && probe.has_target(target)),
        ));
    }

    let verdict = if requirements.iter().any(Requirement::missing) {
        Capacity::Missing
    } else {
        Capacity::Ready
    };
    BuildCapacity {
        host: host.to_string(),
        target: requested,
        verdict,
        requirements,
    }
}

/// Probe this host and plan in one call — the surface a client uses.
pub fn probe_and_plan(host: &str, target: &str) -> BuildCapacity {
    plan(host, target, &HostProbe::probe())
}

/// The checklist for a machine we are NOT on: every requirement `present:
/// None`, because we cannot probe someone else's box from here.
fn foreign_requirements(needs_os: &str, target: &str) -> Vec<Requirement> {
    let mut reqs = Vec::new();
    match needs_os {
        "windows" => reqs.push(msvc_requirement(None)),
        "darwin" => reqs.push(xcode_clt_requirement(None)),
        _ => {}
    }
    reqs.push(rustup_requirement(needs_os, None));
    reqs.push(rustup_target_requirement(target, needs_os, None));
    reqs
}

/// The MSVC build tools plus the Windows SDK.
///
/// `--includeRecommended` is what pulls the SDK in alongside the compiler;
/// without it the result is a toolset directory with no `bin/` and no SDK —
/// the exact half-installed state VS 2019 was found in on us-west-002
/// (W352, measured 2026-09-16). `--wait` makes `winget` block until the
/// installer finishes rather than returning when it hands off.
fn msvc_requirement(present: Option<bool>) -> Requirement {
    Requirement {
        id: "msvc-build-tools".to_string(),
        label: "Visual Studio 2022 Build Tools (VCTools workload + Windows SDK)".to_string(),
        present,
        probe: vec![
            "vswhere".to_string(),
            "-property".to_string(),
            "catalog_productDisplayVersion".to_string(),
        ],
        install: vec![InstallCommand::new(
            Shell::Windows,
            "winget install --id Microsoft.VisualStudio.2022.BuildTools -e \
             --accept-package-agreements --accept-source-agreements --override \
             \"--quiet --wait --norestart --add Microsoft.VisualStudio.Workload.VCTools \
             --includeRecommended\"",
            true,
        )
        .with_note(
            "3-5 GB download; --includeRecommended is what pulls in the Windows SDK, \
             and without it you get a toolset directory with no bin/",
        )],
        install_on: "windows".to_string(),
    }
}

/// The Xcode command line tools — the linker and SDK every Darwin build needs.
/// The full Xcode app is a separate, larger thing and is only needed for
/// notarization/simulator work, so this row deliberately names the CLT.
fn xcode_clt_requirement(present: Option<bool>) -> Requirement {
    Requirement {
        id: "xcode-clt".to_string(),
        label: "Xcode command line tools (clang, ld, macOS SDK)".to_string(),
        present,
        probe: vec!["xcode-select".to_string(), "-p".to_string()],
        install: vec![InstallCommand::new(Shell::Posix, "xcode-select --install", false)
            .with_note("opens a GUI installer; on a machine with full Xcode already \
                        installed, `sudo xcode-select -s /Applications/Xcode.app` instead")],
        install_on: "darwin".to_string(),
    }
}

/// `rustup` itself.
fn rustup_requirement(install_on: &str, present: Option<bool>) -> Requirement {
    let install = match install_on {
        "windows" => vec![InstallCommand::new(
            Shell::Windows,
            "winget install --id Rustlang.Rustup -e --accept-package-agreements \
             --accept-source-agreements",
            false,
        )],
        _ => vec![InstallCommand::new(
            Shell::Posix,
            "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh",
            false,
        )],
    };
    Requirement {
        id: "rustup".to_string(),
        label: "rustup (Rust toolchain manager)".to_string(),
        present,
        probe: vec![
            "rustup".to_string(),
            "target".to_string(),
            "list".to_string(),
            "--installed".to_string(),
        ],
        install,
        install_on: install_on.to_string(),
    }
}

/// The rustup std for one triple. Carries the REAL triple, not a `<triple>`
/// placeholder — an operator should never have to substitute by hand.
fn rustup_target_requirement(target: &str, install_on: &str, present: Option<bool>) -> Requirement {
    let shell = shell_for(install_on);
    Requirement {
        id: "rustup-target".to_string(),
        label: format!("rustup std for {target}"),
        present,
        probe: vec![
            "rustup".to_string(),
            "target".to_string(),
            "list".to_string(),
            "--installed".to_string(),
        ],
        install: vec![InstallCommand::new(
            shell,
            format!("rustup target add {target}"),
            false,
        )],
        install_on: install_on.to_string(),
    }
}

/// A [`CrossTool`] as a requirement row. The install lines are the structured
/// form of [`CrossTool::install_hint`] — one source of truth, so the prose hint
/// and this checklist can never drift.
fn cross_requirement(
    tool: &CrossTool,
    target: &str,
    install_on: &str,
    present: Option<bool>,
) -> Requirement {
    Requirement {
        id: match tool {
            CrossTool::CargoZigbuild => "cargo-zigbuild",
            CrossTool::MuslCross => "musl-cross",
            CrossTool::CargoNative => "cargo-native",
        }
        .to_string(),
        label: tool.label().to_string(),
        present,
        probe: tool.probe_argv(),
        install: tool.install_commands(target, shell_for(install_on)),
        install_on: install_on.to_string(),
    }
}

fn shell_for(os_tag: &str) -> Shell {
    match os_tag {
        "windows" => Shell::Windows,
        _ => Shell::Posix,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARM_MAC: &str = "aarch64-apple-darwin";
    const X64_MAC: &str = "x86_64-apple-darwin";
    const X64_LINUX: &str = "x86_64-unknown-linux-gnu";
    const X64_MUSL: &str = "x86_64-unknown-linux-musl";
    const WIN_MSVC: &str = "x86_64-pc-windows-msvc";
    const WIN_GNU: &str = "x86_64-pc-windows-gnu";

    /// Walk every requirement this module can emit and assert the one-line
    /// invariant — the W352 lesson, locked. `InstallCommand::new` also asserts,
    /// so this is belt-and-braces over the *whole reachable set* rather than
    /// over whichever constructor a test happened to call.
    #[test]
    fn every_install_command_is_one_line() {
        let hosts = [ARM_MAC, X64_LINUX, WIN_MSVC];
        let targets = ["", ARM_MAC, X64_MAC, X64_LINUX, X64_MUSL, WIN_MSVC, WIN_GNU];
        let probes = [HostProbe::none(), HostProbe::full(&[]), HostProbe::full(&targets[1..])];
        let mut seen = 0usize;
        for host in hosts {
            for target in targets {
                for probe in &probes {
                    for req in plan(host, target, probe).requirements {
                        for cmd in req.install {
                            seen += 1;
                            assert!(!cmd.line.contains('\n'), "{:?}", cmd.line);
                            assert!(!cmd.line.contains('\r'), "{:?}", cmd.line);
                            assert_eq!(cmd.line.trim(), cmd.line, "untrimmed: {:?}", cmd.line);
                            let last = cmd.line.chars().last().unwrap();
                            assert!(
                                last != '`' && last != '\\' && last != '^',
                                "continuation character at end of {:?}",
                                cmd.line
                            );
                        }
                    }
                }
            }
        }
        assert!(seen > 20, "decision table barely exercised ({seen} commands)");
    }

    /// The winget lines are the load-bearing text from W352 "Concrete changes"
    /// step 1. If someone rewraps them for readability, this fails.
    #[test]
    fn windows_msvc_offers_the_exact_winget_commands() {
        let cap = plan(WIN_MSVC, WIN_MSVC, &HostProbe::none());
        let lines: Vec<&str> = cap
            .requirements
            .iter()
            .flat_map(|r| r.install.iter())
            .map(|c| c.line.as_str())
            .collect();
        assert!(
            lines.iter().any(|l| l.contains("Microsoft.VisualStudio.2022.BuildTools")
                && l.contains("--includeRecommended")
                && l.contains("--wait")),
            "{lines:#?}"
        );
        assert!(
            lines.iter().any(|l| l.contains("Rustlang.Rustup")),
            "{lines:#?}"
        );
        assert!(lines.iter().all(|l| !l.contains('\n')));
    }

    #[test]
    fn msvc_off_windows_is_a_foreign_host_not_a_missing_tool() {
        let cap = plan(ARM_MAC, WIN_MSVC, &HostProbe::full(&[WIN_MSVC]));
        match &cap.verdict {
            Capacity::ForeignHost { needs_os, .. } => assert_eq!(needs_os, "windows"),
            other => panic!("{other:?}"),
        }
        // The checklist is still there, unprobed, for the machine that owns it.
        assert!(cap.requirements.iter().all(|r| r.present.is_none()));
        assert!(cap.requirements.iter().any(|r| r.id == "msvc-build-tools"));
        assert!(cap.requirements.iter().all(|r| r.install_on == "windows"));
    }

    #[test]
    fn darwin_off_a_mac_is_a_foreign_host_with_no_msvc_row() {
        let cap = plan(X64_LINUX, ARM_MAC, &HostProbe::full(&[]));
        match &cap.verdict {
            Capacity::ForeignHost { needs_os, reason } => {
                assert_eq!(needs_os, "darwin");
                assert!(reason.contains("redistributable"), "{reason}");
            }
            other => panic!("{other:?}"),
        }
        assert!(cap.requirements.iter().all(|r| r.id != "msvc-build-tools"));
    }

    /// Windows-**gnu** is a zig target, not a foreign host — the whole point of
    /// the ABI split in W352's four-layer table.
    #[test]
    fn windows_gnu_from_a_mac_wants_zigbuild_not_a_windows_box() {
        let cap = plan(ARM_MAC, WIN_GNU, &HostProbe::none());
        assert!(matches!(cap.verdict, Capacity::Missing), "{:?}", cap.verdict);
        let zig = cap
            .requirements
            .iter()
            .find(|r| r.id == "cargo-zigbuild")
            .expect("zigbuild row");
        assert_eq!(zig.present, Some(false));
        assert!(zig
            .install
            .iter()
            .any(|c| c.line.contains("cargo install cargo-zigbuild")));
    }

    #[test]
    fn a_fully_equipped_mac_is_ready_for_a_darwin_slice() {
        let cap = plan(ARM_MAC, X64_MAC, &HostProbe::full(&[X64_MAC]));
        assert!(cap.is_ready(), "{:#?}", cap);
        assert_eq!(cap.missing().count(), 0);
        // A darwin->darwin slice needs no cross tool at all.
        assert!(cap.requirements.iter().all(|r| r.id != "cargo-zigbuild"));
    }

    #[test]
    fn a_missing_rustup_target_is_the_only_gap_when_the_toolchain_is_there() {
        let cap = plan(ARM_MAC, X64_MUSL, &HostProbe::full(&[]));
        assert!(matches!(cap.verdict, Capacity::Missing));
        let missing: Vec<&str> = cap.missing().map(|r| r.id.as_str()).collect();
        assert_eq!(missing, vec!["rustup-target"]);
        assert_eq!(
            cap.missing().next().unwrap().install[0].line,
            format!("rustup target add {X64_MUSL}")
        );
    }

    /// The `<triple>` placeholder that used to ship in the hint text is a
    /// substitution the operator should never have to make.
    #[test]
    fn no_emitted_command_carries_a_placeholder() {
        for target in [X64_MUSL, WIN_GNU, X64_MAC] {
            for host in [ARM_MAC, X64_LINUX] {
                for cmd in plan(host, target, &HostProbe::none())
                    .requirements
                    .iter()
                    .flat_map(|r| r.install.iter())
                {
                    assert!(
                        !cmd.line.contains("<triple>") && !cmd.line.contains("<arch>"),
                        "unsubstituted placeholder in {:?}",
                        cmd.line
                    );
                }
            }
        }
    }

    #[test]
    fn an_unknown_os_is_unsupported_rather_than_missing() {
        let cap = plan(ARM_MAC, "wasm32-unknown-unknown", &HostProbe::full(&[]));
        assert!(
            matches!(cap.verdict, Capacity::Unsupported { .. }),
            "{:?}",
            cap.verdict
        );
        assert!(cap.requirements.is_empty());
    }

    #[test]
    fn report_renders_the_commands_one_per_line() {
        let cap = plan(WIN_MSVC, WIN_MSVC, &HostProbe::none());
        let report = cap.report();
        assert!(report.iter().all(|l| !l.contains('\n')));
        assert!(report.iter().any(|l| l.contains("elevated shell")));
        assert!(report[0].contains("missing"), "{}", report[0]);
    }

    #[test]
    fn elevation_is_marked_only_where_it_is_needed() {
        let cap = plan(WIN_MSVC, WIN_MSVC, &HostProbe::none());
        let msvc = cap
            .requirements
            .iter()
            .find(|r| r.id == "msvc-build-tools")
            .unwrap();
        assert!(msvc.install[0].elevated);
        let rustup = cap.requirements.iter().find(|r| r.id == "rustup").unwrap();
        assert!(!rustup.install[0].elevated);
    }

    #[test]
    fn host_native_on_a_mac_wants_the_clt_and_rustup_and_nothing_else() {
        let cap = plan(ARM_MAC, "", &HostProbe::full(&[]));
        assert!(cap.is_ready(), "{:#?}", cap);
        let ids: Vec<&str> = cap.requirements.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["xcode-clt", "rustup"]);
        // Display keeps the empty target rather than echoing a triple nobody
        // typed.
        assert_eq!(cap.target, "");
        assert!(cap.report()[0].contains("host-native"), "{}", cap.report()[0]);
    }

    /// An empty target is the HOST's triple, not a question with fewer
    /// requirements: a bare `cargo build` on Windows still needs the MSVC
    /// linker. An earlier cut of `plan` short-circuited `target.is_empty() ||
    /// target == host` straight to a rustup-only answer and told a Windows box
    /// with no compiler that it was READY.
    #[test]
    fn host_native_on_windows_still_requires_msvc() {
        for target in ["", WIN_MSVC] {
            let cap = plan(WIN_MSVC, target, &HostProbe::none());
            assert!(
                cap.requirements.iter().any(|r| r.id == "msvc-build-tools"),
                "target {target:?} dropped the MSVC row: {:#?}",
                cap.requirements
            );
            assert!(matches!(cap.verdict, Capacity::Missing), "{:?}", cap.verdict);
            // …and never asks to `rustup target add` the host's own triple.
            assert!(cap.requirements.iter().all(|r| r.id != "rustup-target"));
        }
    }
}
