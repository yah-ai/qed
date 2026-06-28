//! Declarative toolchain pinning (R507, W208 pillar 3).
//!
//! QED today pins toolchains *implicitly* — `cargo +nightly` in an `argv`, a
//! container `image` that happens to carry Xcode 15.4, an `xcrun` that picks up
//! whatever's selected on the host. None of that is visible to the dashboard or
//! to dependency analysis, and a host that's missing the right Xcode/NDK/MSVC
//! doesn't fail until three waves into a multi-hour release.
//!
//! This module makes the pin *declarative and plan-time-checked*:
//!
//! ```toml
//! [pipeline.toolchain]
//! rust  = "1.84.0"
//! xcode = "15.4"
//! ndk   = "r27"
//! msvc  = "2022-17.8"
//!
//! [[pipeline.steps]]
//! name = "build-android"
//! toolchain.ndk = "r26d"   # per-step override beats the pipeline pin
//! ```
//!
//! The new field declares *what's required*; existing infrastructure provides
//! *how to satisfy it*. Resolution has two arms, mirroring W208's frame:
//!
//! - **Host-side tool managers** (rustup, xcrun, ndk) — the step runs natively,
//!   so QED probes the host's installed version and checks it against the pin.
//!   A miss fails the plan fast with an actionable error (install / select the
//!   pinned version), not a confusing mid-build linker/SDK error.
//! - **Container image** — when the step pulls an explicit `image` (or
//!   `runtime = "container"`), the image *is* the toolchain provider, so the
//!   host-side check is skipped: the pin is satisfied by the image.
//!
//! ## Coordination with W222 (R531)
//!
//! This is the *toolchain-version* axis of plan-time host-capability checking;
//! [`crate::platform`]'s `resolve(host, target, container_platform)` is the
//! *host/target/container-arch* axis. They share the "fail fast at plan time if
//! the host can't satisfy the pin" shape — kept as two pure, total decision
//! functions so each is testable in isolation, composed by the runner at plan
//! time (see [`crate::runner::PipelineRunner::toolchain_preflight`]).
//!
//! ## Surface
//!
//! - [`ToolchainSpec`] — the TOML-declared pins, pipeline- or step-scoped.
//! - [`effective_pins`] — overlay step overrides onto the pipeline pins.
//! - [`Tool`] — the fixed set QED knows how to *probe* (rust/xcode/ndk/msvc);
//!   unknown tool names are still carried, just unverifiable on the host.
//! - [`resolve_pin`] — the pure, total decision function for one pin.
//! - [`version_satisfies`] — segment-prefix version matching.
//! - [`ToolchainPreflight`] — the aggregate verdict the runner gates on.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Pinned tool versions, declared at pipeline scope (`[pipeline.toolchain]`) or
/// per-step (`toolchain.<tool> = "..."`). A flat map of tool name → pinned
/// version string.
///
/// Keys are free-form so a new tool needs no schema change — QED knows how to
/// *probe* the fixed [`Tool`] set, and treats every other key as carried-but-
/// unverifiable rather than rejecting it. An all-empty spec (`toolchain = {}`)
/// is inert: it declares no pins and gates nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolchainSpec {
    pub pins: IndexMap<String, String>,
}

impl ToolchainSpec {
    /// `true` when no pins are declared — treated as "no toolchain block".
    pub fn is_empty(&self) -> bool {
        self.pins.is_empty()
    }
}

/// Overlay a step's toolchain overrides onto the pipeline-level pins. The
/// pipeline pins are the base; any tool the step re-declares wins (the
/// `build-android` → `toolchain.ndk = "r26d"` override case). Declaration
/// order is pipeline-pins-first, then step-only additions, so the rendered
/// preflight is deterministic.
pub fn effective_pins(
    pipeline: Option<&ToolchainSpec>,
    step: Option<&ToolchainSpec>,
) -> IndexMap<String, String> {
    let mut out: IndexMap<String, String> = IndexMap::new();
    if let Some(p) = pipeline {
        for (k, v) in &p.pins {
            out.insert(k.clone(), v.clone());
        }
    }
    if let Some(s) = step {
        for (k, v) in &s.pins {
            out.insert(k.clone(), v.clone()); // step override beats pipeline
        }
    }
    out
}

/// The fixed set of toolchains QED knows how to probe on the host. An
/// unrecognized pin key (`zig`, `emsdk`, …) is still carried through
/// resolution — it just resolves to [`PinResolution::Unverifiable`] when the
/// step runs host-side, since QED has no probe for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tool {
    /// Rust toolchain — probed via `rustc --version` (rustup-managed).
    Rust,
    /// Apple Xcode — probed via `xcodebuild -version` (`xcrun`-selected).
    Xcode,
    /// Android NDK — probed via `$ANDROID_NDK_HOME/source.properties`.
    Ndk,
    /// MSVC build tools — probed via `vswhere` (Windows only).
    Msvc,
}

impl Tool {
    /// Map a pin key to a known [`Tool`]. Case-insensitive on the bare tool
    /// name. Returns `None` for keys outside the probe set.
    pub fn parse(name: &str) -> Option<Tool> {
        match name.trim().to_ascii_lowercase().as_str() {
            "rust" | "rustc" | "rustup" => Some(Tool::Rust),
            "xcode" => Some(Tool::Xcode),
            "ndk" | "android-ndk" => Some(Tool::Ndk),
            "msvc" => Some(Tool::Msvc),
            _ => None,
        }
    }

    /// Probe the host for this tool's installed version, returning `None` when
    /// the tool isn't found (not installed, wrong platform, or no version
    /// could be parsed). Best-effort and side-effect-free beyond reading the
    /// environment / running a `--version` query; the rigor lives in the pure
    /// [`resolve_pin`] decision function this feeds.
    pub fn probe_version(&self) -> Option<String> {
        match self {
            Tool::Rust => probe_cmd_version("rustc", &["--version"], parse_rustc_version),
            Tool::Xcode => probe_cmd_version("xcodebuild", &["-version"], parse_xcodebuild_version),
            Tool::Ndk => probe_ndk_version(),
            Tool::Msvc => probe_cmd_version(
                "vswhere",
                &["-property", "catalog_productDisplayVersion"],
                |s| {
                    s.lines()
                        .next()
                        .map(|l| l.trim().to_string())
                        .filter(|l| !l.is_empty())
                },
            ),
        }
    }
}

/// How one pin resolves against the host (or against the image that provides
/// the toolchain). The decision is total — every (pin, detected, image) input
/// maps to exactly one variant via [`resolve_pin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinResolution {
    /// Host has the tool and its version satisfies the pin.
    Satisfied {
        tool: String,
        want: String,
        got: String,
    },
    /// The step pulls a container image, so the image provides the toolchain —
    /// the host-side check is skipped by design.
    SatisfiedByImage { tool: String, want: String },
    /// Host has the tool but at a version that doesn't satisfy the pin.
    VersionMismatch {
        tool: String,
        want: String,
        got: String,
    },
    /// Host doesn't have the tool at all (and no image provides it).
    Missing { tool: String, want: String },
    /// QED has no probe for this tool (a key outside the [`Tool`] set) and no
    /// image provides it — the pin is carried but can't be verified on the
    /// host. Non-blocking: surfaced in the preflight, never fails the plan.
    Unverifiable { tool: String, want: String },
}

impl PinResolution {
    /// `true` for the verdicts that should fail the plan fast — a host that
    /// lacks the pinned tool or has the wrong version. [`Unverifiable`] is
    /// deliberately *not* blocking: "can't verify" is not "known-bad".
    ///
    /// [`Unverifiable`]: PinResolution::Unverifiable
    pub fn is_blocking(&self) -> bool {
        matches!(
            self,
            PinResolution::VersionMismatch { .. } | PinResolution::Missing { .. }
        )
    }

    /// One human-readable preflight line, with the actionable remediation baked
    /// into the blocking verdicts.
    pub fn line(&self) -> String {
        match self {
            PinResolution::Satisfied { tool, want, got } => {
                format!("{tool} {want} ✓ (host has {got})")
            }
            PinResolution::SatisfiedByImage { tool, want } => {
                format!("{tool} {want} ✓ (provided by container image)")
            }
            PinResolution::VersionMismatch { tool, want, got } => format!(
                "{tool} {want} ✗ — host has {got}; select/install {tool} {want} \
                 (e.g. via rustup/xcode-select/ndk manager) or run this step in a \
                 container image that carries it"
            ),
            PinResolution::Missing { tool, want } => format!(
                "{tool} {want} ✗ — not found on host; install {tool} {want} or run \
                 this step in a container image that carries it"
            ),
            PinResolution::Unverifiable { tool, want } => {
                format!("{tool} {want} ? (no host probe; not verified)")
            }
        }
    }
}

/// The pure, total decision function for one pin (R507). Given a pin
/// (`tool` → `want`), the host's detected version (`None` = tool absent), and
/// whether the step is satisfied by a container image, return exactly one
/// [`PinResolution`].
///
/// Decision order:
/// 1. **Image-provided → [`SatisfiedByImage`].** A step that pulls an image
///    delegates its toolchain to that image; QED doesn't introspect the image,
///    it trusts the declaration (the host-side managers are for the non-image
///    arm). Wins over everything — the host's own tools are irrelevant when the
///    step runs in a container.
/// 2. **Unknown tool → [`Unverifiable`].** No probe exists, so the pin is
///    carried but not checked (non-blocking).
/// 3. **Detected + satisfies → [`Satisfied`]; detected + mismatch →
///    [`VersionMismatch`]; absent → [`Missing`].**
///
/// [`SatisfiedByImage`]: PinResolution::SatisfiedByImage
/// [`Unverifiable`]: PinResolution::Unverifiable
/// [`Satisfied`]: PinResolution::Satisfied
/// [`VersionMismatch`]: PinResolution::VersionMismatch
/// [`Missing`]: PinResolution::Missing
pub fn resolve_pin(
    tool: &str,
    want: &str,
    detected: Option<&str>,
    satisfied_by_image: bool,
) -> PinResolution {
    // 1. A containerized step's toolchain comes from the image.
    if satisfied_by_image {
        return PinResolution::SatisfiedByImage {
            tool: tool.to_string(),
            want: want.to_string(),
        };
    }

    // 2. No probe for this tool → carried but unverifiable.
    if Tool::parse(tool).is_none() {
        return PinResolution::Unverifiable {
            tool: tool.to_string(),
            want: want.to_string(),
        };
    }

    // 3. Host-side check.
    match detected {
        Some(got) if version_satisfies(want, got) => PinResolution::Satisfied {
            tool: tool.to_string(),
            want: want.to_string(),
            got: got.to_string(),
        },
        Some(got) => PinResolution::VersionMismatch {
            tool: tool.to_string(),
            want: want.to_string(),
            got: got.to_string(),
        },
        None => PinResolution::Missing {
            tool: tool.to_string(),
            want: want.to_string(),
        },
    }
}

/// Does the host's `got` version satisfy the pinned `want`? Segment-prefix
/// match after normalizing a leading non-digit prefix (so NDK's `r27` pin
/// matches a detected `27.0.12077973`). The pin's dot-segments must be a prefix
/// of the detected version's:
///
/// - `15.4` is satisfied by `15.4`, `15.4.1` — but not `15.3` or `15`.
/// - `1.84.0` is satisfied by `1.84.0` — but not `1.84`.
/// - `r27` (→ `27`) is satisfied by `27.0.12077973`.
///
/// This is intentionally a *minimum-floor-by-prefix* rule, not full semver
/// ordering: pins in practice name an exact-or-finer version, and a total,
/// obvious rule is worth more than guessing `>=` semantics across four tools
/// with four different version vocabularies. Refinable as real pins surface,
/// the same discipline as [`crate::platform::host_native_crossable`].
pub fn version_satisfies(want: &str, got: &str) -> bool {
    let want_n = normalize_version(want);
    let got_n = normalize_version(got);
    if want_n.is_empty() {
        return true; // a pin with no comparable digits matches anything
    }
    let want_segs: Vec<&str> = want_n.split('.').collect();
    let got_segs: Vec<&str> = got_n.split('.').collect();
    want_segs.len() <= got_segs.len() && want_segs.iter().zip(got_segs.iter()).all(|(w, g)| w == g)
}

/// Extract the comparable dotted-numeric core of a version string: trim
/// surrounding whitespace, skip a leading run of non-digits (the `r` in `r27`,
/// a `v` prefix), then keep only the leading digit-and-dot run — dropping a
/// trailing alpha suffix (NDK's `r26d` → `26`, whose `Pkg.Revision` reads
/// `26.3.11579264`). `15.4` → `15.4`, `1.84.0` → `1.84.0`, `27.0.12077973`
/// passes through whole.
fn normalize_version(v: &str) -> &str {
    let head = v.trim().trim_start_matches(|c: char| !c.is_ascii_digit());
    let end = head
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(head.len());
    &head[..end]
}

/// The aggregate plan-time verdict over every pin of every step (R507). Built
/// by [`crate::runner::PipelineRunner::toolchain_preflight`]; the runner gates
/// `run()` on [`Self::is_satisfied`] and fails fast with [`Self::error_report`]
/// when a host can't satisfy a pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolchainPreflight {
    /// One entry per (step, pin), in step-then-declaration order.
    pub entries: Vec<PreflightEntry>,
}

/// One row of a [`ToolchainPreflight`] — which step pinned which tool, and how
/// it resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreflightEntry {
    pub step: String,
    pub resolution: PinResolution,
}

impl ToolchainPreflight {
    /// The blocking entries — pins the host can't satisfy. Empty ⇒ the plan is
    /// clear to run.
    pub fn blocking(&self) -> impl Iterator<Item = &PreflightEntry> {
        self.entries.iter().filter(|e| e.resolution.is_blocking())
    }

    /// `true` when no pin is blocking — the plan can proceed.
    pub fn is_satisfied(&self) -> bool {
        self.blocking().next().is_none()
    }

    /// Render every entry as a `step · <pin line>` report — used both for the
    /// always-on preflight log and (filtered to blocking entries) for the
    /// fail-fast error message.
    pub fn report(&self) -> Vec<String> {
        self.entries
            .iter()
            .map(|e| format!("{} · {}", e.step, e.resolution.line()))
            .collect()
    }

    /// The actionable fail-fast message: a header plus one line per blocking
    /// pin. `None` when nothing blocks (the plan is satisfiable).
    pub fn error_report(&self) -> Option<String> {
        let blocking: Vec<String> = self
            .blocking()
            .map(|e| format!("  {} · {}", e.step, e.resolution.line()))
            .collect();
        if blocking.is_empty() {
            return None;
        }
        Some(format!(
            "toolchain preflight failed — {} pinned toolchain(s) the host can't satisfy:\n{}",
            blocking.len(),
            blocking.join("\n"),
        ))
    }
}

// ── host probes (best-effort; the decision rigor is in resolve_pin) ──────────

/// Run `cmd args...`, capture stdout, and parse a version out of it. Returns
/// `None` on spawn failure, non-zero exit, or a parse miss.
fn probe_cmd_version(
    cmd: &str,
    args: &[&str],
    parse: impl Fn(&str) -> Option<String>,
) -> Option<String> {
    let output = std::process::Command::new(cmd).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    parse(&stdout)
}

/// Parse `rustc 1.84.0 (abc123 2024-…)` → `1.84.0`.
fn parse_rustc_version(stdout: &str) -> Option<String> {
    stdout.split_whitespace().nth(1).map(str::to_string)
}

/// Parse `Xcode 15.4\nBuild version 15F31d` → `15.4`.
fn parse_xcodebuild_version(stdout: &str) -> Option<String> {
    let first = stdout.lines().next()?;
    first
        .strip_prefix("Xcode")
        .map(|r| r.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Read the NDK version from `$ANDROID_NDK_HOME` (or `$ANDROID_NDK_ROOT`)'s
/// `source.properties` (`Pkg.Revision = 27.0.12077973`).
fn probe_ndk_version() -> Option<String> {
    let root =
        std::env::var_os("ANDROID_NDK_HOME").or_else(|| std::env::var_os("ANDROID_NDK_ROOT"))?;
    let props = std::path::Path::new(&root).join("source.properties");
    let content = std::fs::read_to_string(props).ok()?;
    parse_ndk_revision(&content)
}

/// Extract `Pkg.Revision = 27.0.12077973` → `27.0.12077973` from an NDK
/// `source.properties` body.
fn parse_ndk_revision(content: &str) -> Option<String> {
    for line in content.lines() {
        if let Some((key, val)) = line.split_once('=') {
            if key.trim() == "Pkg.Revision" {
                let v = val.trim();
                if !v.is_empty() {
                    return Some(v.to_string());
                }
            }
        }
    }
    None
}

/// Probe the host for every tool named in `pins`, returning tool-key →
/// detected-version (`None` = absent). Each known tool is probed at most once
/// even if several pins reference it. Unknown tool keys are skipped (they
/// resolve to [`PinResolution::Unverifiable`] without a probe).
pub fn detect_host_versions<'a, I>(pins: I) -> HashMap<String, Option<String>>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut cache: HashMap<Tool, Option<String>> = HashMap::new();
    let mut out: HashMap<String, Option<String>> = HashMap::new();
    for key in pins {
        let Some(tool) = Tool::parse(key) else {
            continue;
        };
        let detected = cache
            .entry(tool)
            .or_insert_with(|| tool.probe_version())
            .clone();
        out.insert(key.to_string(), detected);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(pairs: &[(&str, &str)]) -> ToolchainSpec {
        ToolchainSpec {
            pins: pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }
    }

    // ── parsing ──────────────────────────────────────────────────────────────

    #[test]
    fn toolchain_spec_round_trips_through_toml() {
        // Pipeline-scope: `[pipeline.toolchain]` table of tool = version.
        let src = r#"
rust  = "1.84.0"
xcode = "15.4"
ndk   = "r27"
"#;
        let s: ToolchainSpec = toml::from_str(src).unwrap();
        assert_eq!(s.pins.get("rust").map(String::as_str), Some("1.84.0"));
        assert_eq!(s.pins.get("xcode").map(String::as_str), Some("15.4"));
        let back = toml::to_string(&s).unwrap();
        let reparsed: ToolchainSpec = toml::from_str(&back).unwrap();
        assert_eq!(reparsed, s);
    }

    #[test]
    fn step_inline_toolchain_table_parses() {
        // The per-step `toolchain.ndk = "r26d"` shape is an inline table.
        #[derive(serde::Deserialize)]
        struct StepLike {
            #[serde(default)]
            toolchain: Option<ToolchainSpec>,
        }
        let step: StepLike = toml::from_str(r#"toolchain = { ndk = "r26d" }"#).unwrap();
        assert_eq!(
            step.toolchain.unwrap().pins.get("ndk").map(String::as_str),
            Some("r26d")
        );
    }

    #[test]
    fn empty_toolchain_spec_is_empty() {
        let s: ToolchainSpec = toml::from_str("").unwrap();
        assert!(s.is_empty());
    }

    // ── tool name parsing ────────────────────────────────────────────────────

    #[test]
    fn tool_parse_maps_known_aliases() {
        assert_eq!(Tool::parse("rust"), Some(Tool::Rust));
        assert_eq!(Tool::parse("rustc"), Some(Tool::Rust));
        assert_eq!(Tool::parse("Xcode"), Some(Tool::Xcode));
        assert_eq!(Tool::parse("ndk"), Some(Tool::Ndk));
        assert_eq!(Tool::parse("android-ndk"), Some(Tool::Ndk));
        assert_eq!(Tool::parse("msvc"), Some(Tool::Msvc));
        assert_eq!(Tool::parse("emsdk"), None);
    }

    // ── effective_pins (step override beats pipeline) ────────────────────────

    #[test]
    fn effective_pins_overlays_step_over_pipeline() {
        let pipe = spec(&[("rust", "1.84.0"), ("ndk", "r27")]);
        let step = spec(&[("ndk", "r26d")]);
        let eff = effective_pins(Some(&pipe), Some(&step));
        // Step override wins for ndk; pipeline rust carries through.
        assert_eq!(eff.get("ndk").map(String::as_str), Some("r26d"));
        assert_eq!(eff.get("rust").map(String::as_str), Some("1.84.0"));
    }

    #[test]
    fn effective_pins_handles_missing_either_side() {
        let pipe = spec(&[("rust", "1.84.0")]);
        assert_eq!(
            effective_pins(Some(&pipe), None)
                .get("rust")
                .map(String::as_str),
            Some("1.84.0")
        );
        let step = spec(&[("xcode", "15.4")]);
        assert_eq!(
            effective_pins(None, Some(&step))
                .get("xcode")
                .map(String::as_str),
            Some("15.4")
        );
        assert!(effective_pins(None, None).is_empty());
    }

    // ── version_satisfies ────────────────────────────────────────────────────

    #[test]
    fn version_satisfies_segment_prefix() {
        assert!(version_satisfies("15.4", "15.4"));
        assert!(version_satisfies("15.4", "15.4.1"));
        assert!(!version_satisfies("15.4", "15.3"));
        assert!(!version_satisfies("15.4", "15"));
        assert!(version_satisfies("1.84.0", "1.84.0"));
        assert!(!version_satisfies("1.84.0", "1.84"));
    }

    #[test]
    fn version_satisfies_normalizes_leading_nondigits() {
        // NDK `r27` pin against a detected `27.0.12077973`.
        assert!(version_satisfies("r27", "27.0.12077973"));
        assert!(!version_satisfies("r27", "26.3.11579264"));
        // `r26d` pin's comparable head is `26` — a `26.x` detected satisfies.
        assert!(version_satisfies("r26d", "26.3.11579264"));
    }

    // ── resolve_pin decision table ───────────────────────────────────────────

    #[test]
    fn resolve_pin_satisfied_when_host_matches() {
        let r = resolve_pin("xcode", "15.4", Some("15.4.1"), false);
        assert_eq!(
            r,
            PinResolution::Satisfied {
                tool: "xcode".into(),
                want: "15.4".into(),
                got: "15.4.1".into()
            }
        );
        assert!(!r.is_blocking());
    }

    #[test]
    fn resolve_pin_version_mismatch_blocks() {
        // noisetable's release.apple pins xcode=15.4; a 15.2 host fails fast.
        let r = resolve_pin("xcode", "15.4", Some("15.2"), false);
        assert_eq!(
            r,
            PinResolution::VersionMismatch {
                tool: "xcode".into(),
                want: "15.4".into(),
                got: "15.2".into()
            }
        );
        assert!(r.is_blocking());
    }

    #[test]
    fn resolve_pin_missing_blocks() {
        let r = resolve_pin("xcode", "15.4", None, false);
        assert_eq!(
            r,
            PinResolution::Missing {
                tool: "xcode".into(),
                want: "15.4".into()
            }
        );
        assert!(r.is_blocking());
    }

    #[test]
    fn resolve_pin_image_satisfies_regardless_of_host() {
        // Even with the tool absent on the host, an image-backed step is fine.
        let r = resolve_pin("xcode", "15.4", None, true);
        assert_eq!(
            r,
            PinResolution::SatisfiedByImage {
                tool: "xcode".into(),
                want: "15.4".into()
            }
        );
        assert!(!r.is_blocking());
        // Image wins even over a host version mismatch.
        let r2 = resolve_pin("xcode", "15.4", Some("15.2"), true);
        assert!(matches!(r2, PinResolution::SatisfiedByImage { .. }));
    }

    #[test]
    fn resolve_pin_unknown_tool_is_unverifiable_not_blocking() {
        let r = resolve_pin("emsdk", "3.1.50", None, false);
        assert_eq!(
            r,
            PinResolution::Unverifiable {
                tool: "emsdk".into(),
                want: "3.1.50".into()
            }
        );
        assert!(!r.is_blocking());
    }

    /// Exhaustive sweep: every (tool-class × detected × image) cell maps to
    /// exactly one resolution and never panics — the decision-table-as-spec
    /// totality guarantee.
    #[test]
    fn resolve_pin_is_total_over_the_class_space() {
        let tools = ["rust", "xcode", "ndk", "msvc", "emsdk"]; // last is unknown
        let detected = [None, Some("15.4"), Some("15.2"), Some("27.0.1")];
        for t in tools {
            for d in detected {
                for image in [true, false] {
                    let r = resolve_pin(t, "15.4", d, image);
                    // Image arm always SatisfiedByImage; unknown host-side arm
                    // always Unverifiable; everything else is one of the three
                    // host verdicts.
                    if image {
                        assert!(matches!(r, PinResolution::SatisfiedByImage { .. }));
                    } else if Tool::parse(t).is_none() {
                        assert!(matches!(r, PinResolution::Unverifiable { .. }));
                    }
                }
            }
        }
    }

    // ── aggregate preflight ──────────────────────────────────────────────────

    fn entry(step: &str, res: PinResolution) -> PreflightEntry {
        PreflightEntry {
            step: step.into(),
            resolution: res,
        }
    }

    #[test]
    fn preflight_is_satisfied_when_nothing_blocks() {
        let pf = ToolchainPreflight {
            entries: vec![
                entry(
                    "build",
                    PinResolution::Satisfied {
                        tool: "rust".into(),
                        want: "1.84.0".into(),
                        got: "1.84.0".into(),
                    },
                ),
                entry(
                    "sign",
                    PinResolution::SatisfiedByImage {
                        tool: "xcode".into(),
                        want: "15.4".into(),
                    },
                ),
                entry(
                    "wasm",
                    PinResolution::Unverifiable {
                        tool: "emsdk".into(),
                        want: "3.1".into(),
                    },
                ),
            ],
        };
        assert!(pf.is_satisfied());
        assert!(pf.error_report().is_none());
        assert_eq!(pf.report().len(), 3);
    }

    #[test]
    fn preflight_fails_fast_with_actionable_report() {
        let pf = ToolchainPreflight {
            entries: vec![
                entry(
                    "build-ios",
                    PinResolution::Missing {
                        tool: "xcode".into(),
                        want: "15.4".into(),
                    },
                ),
                entry(
                    "build",
                    PinResolution::Satisfied {
                        tool: "rust".into(),
                        want: "1.84.0".into(),
                        got: "1.84.0".into(),
                    },
                ),
            ],
        };
        assert!(!pf.is_satisfied());
        let report = pf.error_report().expect("blocking ⇒ report");
        assert!(report.contains("build-ios"));
        assert!(report.contains("xcode"));
        assert!(report.contains("15.4"));
        // Only the blocking row is in the failure message.
        assert!(!report.contains("rust 1.84.0 ✓"));
        assert_eq!(pf.blocking().count(), 1);
    }

    // ── probe parsers (pure halves of the host probes) ───────────────────────

    #[test]
    fn parse_rustc_version_extracts_the_semver() {
        assert_eq!(
            parse_rustc_version("rustc 1.84.0 (9fc6b4312 2024-12-04)").as_deref(),
            Some("1.84.0")
        );
    }

    #[test]
    fn parse_xcodebuild_version_extracts_the_xcode_line() {
        assert_eq!(
            parse_xcodebuild_version("Xcode 15.4\nBuild version 15F31d").as_deref(),
            Some("15.4")
        );
    }

    #[test]
    fn parse_ndk_revision_reads_pkg_revision() {
        let props = "Pkg.Desc = Android NDK\nPkg.Revision = 27.0.12077973\n";
        assert_eq!(parse_ndk_revision(props).as_deref(), Some("27.0.12077973"));
        assert_eq!(parse_ndk_revision("nothing here").as_deref(), None);
    }

    #[test]
    fn detect_host_versions_skips_unknown_tools() {
        // Unknown keys never reach a probe (and can't, deterministically, in a
        // test) — they're simply absent from the detected map.
        let detected = detect_host_versions(["emsdk", "zig"]);
        assert!(detected.is_empty());
    }
}
