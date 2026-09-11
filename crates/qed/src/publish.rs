//! Release-channel publishing — the producer leg of the almanac releases feed.
//!
//! When a QED pipeline succeeds with an [`Outcome::Publish`](crate::types::Outcome::Publish),
//! the runner collects every [`ProducedArtifact`] declared by the successful
//! steps, lays them out into a release channel tree, writes a per-binary
//! `release-manifest.json` pointer, uploads the tree to the channel bucket, and
//! fires the almanac revalidate hook.
//!
//! ## Layout
//!
//! ```text
//! [<prefix>/]<binary>/<version>/<triple>/<filename>      ← the built artifacts
//! [<prefix>/]<binary>/release-manifest.json              ← shared pointer (this-stage view)
//! [<prefix>/]<binary>/release-manifest-<triple>.json     ← per-triple stable record
//! ```
//!
//! `release-manifest.json` is the file almanac's `R2Channel` reads
//! (`crates/yah/almanac/src/r2.rs`). Its wire shape is a forward-compatible
//! subset of `updater::ReleaseManifest` (self-updating-binaries.md): the fields
//! almanac needs (`version`, `pub_date`, `notes`, `host.bundle.<triple>`), so
//! the channel doubles as the almanac source AND the self-update pointer root.
//!
//! ## Multi-triple merge (R330-B8)
//!
//! In a multi-platform release, each `yah qed run release-build` invocation runs
//! on its own host (darwin-aarch64, linux-x86_64, …) and only knows about its
//! own triple's artifacts. The shared `<binary>/release-manifest.json` written
//! here therefore contains only this stage's triples — a sequential publish of
//! linux-x86_64 *after* darwin-aarch64 would overwrite the darwin view.
//!
//! To make cross-stage merge possible without R2 read-modify-write (which races
//! between concurrent publishes), `stage_release` ALSO writes a per-triple
//! manifest at `<binary>/release-manifest-<triple>.json` containing just that
//! triple's bundle entry. These keys are stable and idempotent: a re-run of the
//! same triple writes the same key, never clobbering a sibling triple's record.
//! The GHA assembly job (which already owns macOS code signing — yubaba can't
//! sign macOS) reads every `release-manifest-<triple>.json` and writes the
//! authoritative signed shared `release-manifest.json` once all triples land.
//!
//! ## What this module owns vs. delegates
//!
//! This module owns the *layout + manifest assembly* (pure, filesystem-only,
//! unit-tested with a tempdir). The actual bucket upload + hook POST are I/O
//! that vary by host, so they're delegated to a [`ReleasePublisher`] adapter —
//! the CLI supplies a Cloudflare-R2-backed impl that reuses the cloud crate's
//! `publish_to_r2`; tests supply a recording fake.
//!
//! Part of R330-F3 — canonical ticket annotation lives in `builtins.rs`.
//!
//! @yah:ticket(R488-F3, "ProducedArtifact aggregation across children into parent's Outcome::Publish (single revalidate)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:54:15Z)
//! @yah:status(review)
//! @yah:phase(P3)
//! @yah:parent(R488)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @yah:depends_on(R488-F2)
//! @yah:tier(Cleric)
//! @yah:handoff("F3 shipped. (a) End-to-end multi-child publish fan-in test: 3 SubPipeline children producing yah/desktop/mesofact binaries roll up into a single parent Outcome::Publish that fires StageRecorder.sync ONCE (6 staged objects: 3 binaries + 3 per-binary manifests) and StageRecorder.revalidate ONCE. Uses real PublishingOutcomeDispatcher with an Arc-wrapped ReleasePublisher fake — exercises stage_release end-to-end across composite runs. (b) Continue-on-error semantics pinned: SubPipeline step with on_fail=Continue marks itself failed but parent loop proceeds; sibling steps after run. Overall RunStatus stays Failed. Child produces dropped on failure — documented current behaviour. (c) load_and_validate_graph wired into both entry points: app/yah/cli/src/qed.rs (after placement gate, before proxy probe — pre-flight cycle/depth check on every yah qed run) AND app/yah/cli/src/camp.rs qed_run_handler (LoaderSubPipelineResolver attached to PipelineRunner so daemon resolves SubPipelines identically). New top-level re-exports in qed lib.rs: LoaderSubPipelineResolver, validate_sub_pipeline_graph, SubPipelineConfig/Ref/Collect/Resolver/Error, MAX_SUB_PIPELINE_DEPTH. 2 new runner tests (190 pass total, +2 from F2). cargo check --workspace clean.")
//! @yah:next("F4 (named output exposure): QedStep grows outputs: Vec<OutputDecl> and step results carry output values. SubPipelineCollect.outputs already exists from F1 — F4 wires propagation through the child run into parents expression context. Needs W200-F2 (expression engine) for parent-side substitution, OR a minimal qed-side substitution syntax that the W200 engine subsumes later.")
//! @yah:verify("cargo test -p qed --lib runner::tests::sub_pipeline (9 tests)")
//! @yah:verify("cargo test -p qed --lib")
//! @yah:verify("cargo check --workspace")
//!
//! @yah:ticket(R876-F6, "mesofact-musl cannot be run to iterate: succeeding publishes a release to cdn.yah.dev as a side effect of building")
//! @yah:at(2026-09-09T08:24:35Z)
//! @yah:status(review)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R876)
//! @yah:next("Tier: Cleric — the mechanism is small but the decision is about what a release IS, and getting it wrong publishes to a CDN that install.sh reads. THE CONFLICT, in one sentence: R876-T1's iteration loop starts with `yah qed run mesofact-musl`, and that pipeline carries `[[pipeline.on_success]] kind = \\\"publish\\\" provider = \\\"r2\\\" bucket = \\\"yah-dev\\\" base_url = \\\"https://cdn.yah.dev\\\"` — so the build step of an iteration loop cuts an outward-facing release the moment it fully succeeds.")
//! @yah:next("WHY NOBODY HAS HIT IT YET, AND WHY THAT IS ABOUT TO CHANGE. The publish has NEVER fired: the aarch64 leg has failed on all 14 recorded runs, so the pipeline never reaches on_success. The cause is fixed in the tree — `execute_step_local_container` now publishes/injects/discards `source_context` (oss/qed/crates/qed/src/runner.rs, committed 24d24042, three tests) — but the run on 2026-09-09 04:24Z (ac1101c5) still failed identically, because `/Users/leif/.local/bin/yah` was built 2026-09-08 20:57, five minutes BEFORE the fix landed. See app/yah/cli/CLAUDE.md: `cargo build` does not touch the installed binary. So the first `cargo xtask install` after that commit arms the publish, and the next green mesofact-musl run writes 0.8.35 to cdn.yah.dev and merges it into mesofact/index.json.")
//! @yah:next("THE SHAPE TO AIM FOR, and it already exists one repo over: hotship's whole charter is \\\"IT NEVER WRITES THE CDN, AND MUST NOT LEARN HOW\\\", and that refusal is what makes it safe to hand to an agent. The build half of the same loop should be separable on the same axis — a `publish` param defaulting to false, or an `on_success` that fires only under an explicit release context (YAH_RELEASE_VERSION set, or a tag trigger), so `yah qed run mesofact-musl` is a BUILD and cutting a release is a deliberate second act. Weigh that against the R560-T9/T10 intent recorded in the pipeline header, which is that this pipeline IS the musl half of the release and merges into the same manifest release.yml writes: the answer is probably a param, not deleting the outcome.")
//! @yah:gotcha("THE VERSION IT WOULD PUBLISH UNDER IS THE DEV WORKSPACE VERSION, WHICH IS THE SHARP EDGE. `publish::resolve_release_version` takes YAH_RELEASE_VERSION when set, else the yah workspace version — 0.8.35 at filing, the SAME number the tree carries between releases. So an iteration build does not publish something obviously wrong-looking like `0.0.0`; it publishes a plausible release number, into `mesofact/index.json`, which `install.sh` reads and resolves for `curl … | sh`. The pipeline header calls the unset case \\\"the right default for a fleet build that is not part of a tagged release\\\" — that was written when the publish could not fire, and is exactly the assumption this ticket should re-examine.")
//! @yah:verify("The proof this is fixed is a run that BUILDS and does not publish: `yah qed run mesofact-musl` green on both legs, artifacts retrieved into .yah/cache/artifacts/named/, and `curl -s https://cdn.yah.dev/mesofact/index.json` unchanged before and after (diff the bytes, do not eyeball the version). Then the converse: the release path still publishes when asked. Do NOT verify the first half by relying on the aarch64 leg staying broken — reinstall `yah` first (`cargo xtask install`) so both legs actually pass, or the test proves nothing.")
//! @yah:handoff("STEP 0 (sibling verification, @agent:bundle-anthropic-ashguard's service_records.rs change on R876-B3): ran `cargo test -p yubaba --test main` (the raft/rig integration binary the sibling meant by \"main test group\") twice from oss/yubaba — both runs came back 68 passed / 0 failed, clean, no raft/rig flakiness observed at all. A third attempt in between hit a COMPILE error (`resolve_fleet_inventory`/`overlay_source_providers` not found in scope, oss/yubaba/crates/cloud/src/config.rs) caused by an unrelated live peer editing that file mid-run (tagged R870-B13 in a nearby comment) — not a test failure, and unrelated to service_records/mesh_ip. Also ran `cargo test -p yubaba --lib service_records` directly: 49/0 clean, including the sibling's own new `reconcile_*` unit tests. CONCLUSION: I could not reproduce the sibling's reported 54/14 -> 49/19 split or any raft/rig flapping on two clean runs; nothing in either run named service_records or mesh_ip. This does not confirm the sibling's attribution — it simply did not reproduce for me. Flagging as inconclusive/contradictory rather than confirmed, per instruction to report loudly if anything doesn't match.")
//! @yah:handoff("R876-F6 CODE LANDED. `oss/qed/crates/qed/src/types.rs`: new `Outcome::Publish::require_explicit_version: bool` (serde default false, so every pre-existing pipeline/call site is unaffected). `oss/qed/crates/qed/src/publish.rs`: added `resolve_release_version_explicit() -> Option<String>` (YAH_RELEASE_VERSION only, no workspace-version fallback); `resolve_release_version()` now just calls it and falls back — unchanged behavior for existing callers (yah-cli-release, native-tarball packaging). `oss/qed/crates/qed/src/runner.rs` `dispatch_terminal_outcomes`: when `require_explicit_version` is true and `resolve_release_version_explicit()` is `None`, the Publish outcome is SKIPPED with a `tracing::warn!` naming why, and the run still reports Success (a build is a build) — it does not fail the pipeline. `.yah/qed/mesofact-musl.toml`: its one `[[pipeline.on_success]] kind=\"publish\"` now sets `require_explicit_version = true`, and the stale comment that called the unset-fallback case \"the right default for a fleet build\" is rewritten to explain the new gate and why (aarch64 leg's perpetual failure hid the danger until now). DESIGN CHOICE, and why it's per-outcome not global: `yah-cli-release.toml`'s publish outcome deliberately keeps the old fallback-to-workspace-version behavior (`require_explicit_version` left at its default `false`) because invoking THAT pipeline at all is already the deliberate release act (its own doc explicitly supports `yah qed run yah-cli-release` with no env var as a real release) — unlike mesofact-musl, which R876-T1 runs repeatedly as a build-only iteration loop. Checked (per the ticket's own instruction) whether any real release-path script sets YAH_RELEASE_VERSION for mesofact-musl before landing this: `scripts/publish-mesofact-release.sh` does NOT invoke `yah qed run mesofact-musl` at all (it's a separate script covering the 4 gnu/darwin triples); nothing in-tree currently sets YAH_RELEASE_VERSION for a musl release run — so the explicit-param gate is the only mechanism, not a redundant one.")
//! @yah:handoff("Every `Outcome::Publish { .. }` Rust-literal construction site in the tree (15 in runner.rs tests, 1 in export.rs test, 1 in app/yah/cli/tests/camp_qed_image_pins.rs) updated to carry the new field; `app/yah/cli/src/camp.rs`'s two match/construct sites already used `..` and needed no change. `app/yah/cli/tests/camp_qed_image_pins.rs::mesofact_musl_legs_are_arch_matched_and_carry_source_context` extended to assert `require_explicit_version == true` on the real loaded mesofact-musl.toml pipeline — this is the test that would catch a future accidental revert of the TOML flag.")
//! @yah:gotcha("LIVE DANGER FOUND AND DEFUSED DURING VERIFICATION, 2026-09-09 ~08:10-08:21Z: `yah qed run mesofact-musl` does NOT execute against the freshly `cargo xtask install`-ed CLI binary — it submits to the long-lived camp daemon (`/Applications/yah.app/Contents/MacOS/desktop`, the SAME process serving every session on this machine), and THAT process was still running build `24d24042` (has the source_context/aarch64 fix, does NOT have this ticket's require_explicit_version gate). `yah qed list` surfaces this as \"camp build skew\" but only when you happen to run a command that prints it — there is no proactive warning before `yah qed run` dispatches. I started an iteration run with YAH_RELEASE_VERSION unset to prove the gate; under the daemon's stale binary the aarch64 leg finished SUCCESS in under 10 minutes (first time ever — confirms R560-T8's source_context fix is good) and the x86_64 leg was still running when I cancelled it via `qed.cancel` (run 7613916e). Had I let it finish, this pipeline WOULD have hit the old unconditional Outcome::Publish and published 0.8.35 to cdn.yah.dev under the fallback version — the exact bug this ticket exists to close, on a LIVE daemon serving the whole camp, not just my session. Verified via curl+sha256 that cdn.yah.dev/mesofact/index.json is still byte-identical to before I started (62c1c0b2...). THE DAEMON STILL HAS NOT BEEN RESTARTED as of this writing — anyone running `yah qed run mesofact-musl` against this camp's daemon right now is still exposed until it picks up a build at or after 718dfacb (or wherever this ticket's fix lands next). I deliberately did NOT restart yah.app myself: it is shared infrastructure for every live session on this machine (peers observed mid-session included R870/R877/R853 plus several unlabeled build sessions queued behind the same cargo-target lock), and killing/restarting it would interrupt their in-flight work without their consent — that decision belongs to the operator or leader, not a courier. RECOMMEND: restart yah.app (or equivalent daemon reload) before anyone next runs `yah qed run mesofact-musl` for real, and re-run the live green-both-legs-unchanged-CDN proof this ticket's verify section asks for once that's done — my unit-level proof (both directions of the gate) stands in for it here per the ticket's own fallback clause.")
//! @yah:verify("Unit tests directly on the gate, both directions, oss/qed/crates/qed/src/runner.rs: `publish_outcome_with_required_version_skips_when_unset` (YAH_RELEASE_VERSION unset -> RunStatus::Success but dispatcher.publish never called) and `publish_outcome_with_required_version_fires_when_set` (set to \"7.7.7\" -> publish fires carrying exactly that version, not the workspace fallback). oss/qed/crates/qed/src/publish.rs: `resolve_release_version_explicit_is_none_when_unset` (covers both truly-unset and explicitly-empty-string) and `resolve_release_version_explicit_is_some_when_set`. All four new + the existing `resolve_release_version_prefers_env` serialized against a shared `RELEASE_VERSION_ENV_LOCK` Mutex per file (this env var is process-global and cargo runs tests in parallel by default).")
//! @yah:verify("`cargo test -p yah-qed --lib` (from oss/qed workspace): 939 passed / 1 failed / 1 ignored, both before and after this change — the 1 failure (`tests::desktop_release_matrix_routes_each_row_to_its_own_platform`, \"desktop-release pipeline loads: NotFound\") is pre-existing and unrelated: `.yah/qed/desktop-release.toml` was renamed to `yah-desktop-release.toml` in an earlier commit (9e454f03) without updating this test; confirmed via `git log --diff-filter=D` on the old path. My change added exactly 4 new tests (939 vs the pre-change 935 passed), none touch that pipeline. `cargo build -p yah-qed` and `cargo build -p yah`: both clean, 0 errors, only pre-existing warnings unrelated to this change.")
//! @yah:verify("Live: `cargo xtask install` succeeded (yah 0.8.35+718dfacb-dirty, sha256 01db7894...), confirmed on PATH. `yah qed run mesofact-musl` started with YAH_RELEASE_VERSION unset; SEE GOTCHA — the run executed against the camp daemon's STALE pre-fix binary (24d24042), not the one I just installed, so it was cancelled mid-run (aarch64 leg had already gone green, x86_64 still running) rather than let it reach on_success unguarded. `curl -sS https://cdn.yah.dev/mesofact/index.json` sha256 before this session's install/run activity and after cancellation are BYTE-IDENTICAL: 62c1c0b235e26952857139537b65f8272026cd1c385c1bf6dba20481ee8a6619 both times (27150 bytes). The full green-both-legs live proof against a daemon actually running the guarded build was not completed this session — see gotcha for why and the recommended next step.")
//! @yah:handoff("LEADER NOTE ON SCOPE. The ticket's open question (\"a param, not deleting the outcome\") was resolved as an opt-in `require_explicit_version` on `Outcome::Publish`, set true on mesofact-musl.toml and left false on yah-cli-release — because running yah-cli-release AT ALL is already the deliberate release act, so the gate belongs on the pipeline that is also an iteration loop, not on the one that is only a release. The `on_success` publish outcome was not deleted; R560-T9/T10's intent that this pipeline is the musl half of the release is preserved.")
//! @yah:verify("WHAT WAS NOT PROVEN, stated plainly rather than hedged: the ticket's live end-to-end proof (a green-both-legs `yah qed run mesofact-musl` with cdn.yah.dev/mesofact/index.json byte-identical across it) DID NOT RUN TO COMPLETION. It was started and then cancelled mid-flight on discovering the camp daemon executes pipelines from a stale pre-gate build — see R876-B8, which is live and armed. The index.json byte-diff WAS taken across the cancelled run and is identical, and the gate is covered by 4 new unit tests in both directions, but the end-to-end leg is owed and is blocked on B8's daemon restart.")
//! @yah:verify("E2E LEG DISCHARGED 2026-09-09 by @Ashguard:polaris while running R876-F4's timings (session:ca1ad243). Three `yah qed run mesofact-musl --in-process` runs on a self-built release binary 0.8.35+e714a29f-dirty (verified by content: `yah --version` plus grep -a for the gate's own 'skipping Outcome::Publish' string, present). YAH_RELEASE_VERSION UNSET on all three — the iteration case. ALL THREE RUNS: both legs green (x86_64 offloaded to us-west-003, aarch64 NativeCross locally), pipeline Success, so `[[pipeline.on_success]]` was reached rather than skipped upstream. THE aarch64 LEG WENT GREEN, which closes the half this ticket's proof was missing; the arm-worker 2.5 GB rootfs blocker does not apply because R605-B13 moved that leg to the local pinned linux/arm64 container. cdn.yah.dev/mesofact/index.json sha256 62c1c0b235e26952857139537b65f8272026cd1c385c1bf6dba20481ee8a6619 before the first run and 62c1c0b235e26952857139537b65f8272026cd1c385c1bf6dba20481ee8a6619 after the last, `cmp` byte-identical on the saved files. No publish or staging line in any run log. The `--in-process` route was used precisely because the camp daemon is still on the stale pre-gate 0.8.35+24d24042-dirty build; that daemon was NOT restarted, so this ticket's LIVE-AND-ARMED gotcha about the daemon remains true and is still the operator's call.")
//! @yah:verify("THE OWED END-TO-END LEG IS NOW DISCHARGED — retracting this ticket's \"what was not proven\" entry above. R876-F4's three `--in-process` runs of mesofact-musl on 2026-09-09 went green on BOTH legs (the aarch64 leg for the first time on record), with YAH_RELEASE_VERSION unset — i.e. exactly the iteration case the gate exists to refuse — and cdn.yah.dev/mesofact/index.json was cmp-byte-identical before and after at sha256 62c1c0b235e26952857139537b65f8272026cd1c385c1bf6dba20481ee8a6619. That is the ticket's stated pass condition, met by byte comparison rather than by eyeballing a version. The `--in-process` path (found by R876-B8) is what made this runnable without the daemon restart the earlier attempt was blocked on.")
//! @yah:gotcha("THE GATE'S SCOPE IS NARROWER THAN THE STALE-DAEMON WARNING THIS ENTRY ORIGINALLY CARRIED, AND THE HAZARD IS NOW CLOSED FOR THE PATH THAT MATTERS. R876-B8 landed a build-skew refusal on the CLI side and, separately, established `yah qed run <pipeline> --in-process` — which executes with the freshly installed binary, so the F6 gate applies and the camp daemon's stale build is bypassed entirely. Iterating on mesofact-musl is safe TODAY via --in-process. What remains true: a `yah qed run` WITHOUT --in-process still goes to the daemon, and B8's skew gate now refuses exactly that for publish-carrying pipelines rather than letting it through silently.")

use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::runner::{OutcomeDispatcher, RunnerError};
use crate::types::ProducedArtifact;

/// Everything the dispatcher needs to publish one release: the destination,
/// the resolved version, and the artifacts collected from successful steps.
#[derive(Debug, Clone)]
pub struct PublishRequest {
    pub provider: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub base_url: Option<String>,
    pub version: String,
    pub artifacts: Vec<ProducedArtifact>,
}

/// Resolve the release version: `YAH_RELEASE_VERSION` env override (set by the
/// release tag / GHA), falling back to the version this binary was built at
/// (`CARGO_PKG_VERSION`, which is the workspace version — `version.workspace`).
///
/// This fallback is safe only where invoking the pipeline at all IS the
/// deliberate release act (`yah-cli-release`, run by hand to cut a release).
/// A pipeline that also serves as a build-only iteration loop must NOT use
/// this for its publish outcome — see [`resolve_release_version_explicit`]
/// and `Outcome::Publish::require_explicit_version` (R876-F6).
pub fn resolve_release_version() -> String {
    resolve_release_version_explicit().unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
}

/// The same `YAH_RELEASE_VERSION` override, without the workspace-version
/// fallback: `None` means no release version was ever stated, explicitly, by
/// whoever invoked this run. R876-F6 — the invariant an unset release version
/// must never resolve to a publishable number depends on this NOT falling
/// back to anything.
pub fn resolve_release_version_explicit() -> Option<String> {
    std::env::var("YAH_RELEASE_VERSION")
        .ok()
        .filter(|v| !v.trim().is_empty())
}

// ── Channel manifest wire types ─────────────────────────────────────────────
//
// A forward-compatible subset of `updater::ReleaseManifest`. We deliberately do
// not depend on the updater crate here: the producer only fills the fields it
// can know (version, pub_date, notes, per-triple url+size). The signing-only
// fields (`signature`, `ipc_contract`) are layered on by the GHA signing leg
// (yubaba can't sign macOS — see the builtin's gotcha). almanac's `R2Channel`
// reader ignores the signing fields, so the chain works with this subset.

/// `release-manifest.json` as emitted by the producer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelManifest {
    /// Release version, without a leading `v`.
    pub version: String,
    /// ISO-8601 UTC publish timestamp.
    pub pub_date: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    pub host: ChannelHost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelHost {
    /// Per-triple bundle pointers, keyed by triple shorthand.
    pub bundle: BTreeMap<String, ChannelBundle>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelBundle {
    /// Absolute download URL (when `base_url` is set) or a bucket-relative key.
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Canonical asset hash, ALWAYS algorithm-tagged (`blake3:<hex>`) — R330-F40.
    ///
    /// Until this existed the channel manifest carried a URL and a size and
    /// nothing else, so every download this publisher produced rendered with no
    /// way to verify it. A bare hex digest is deliberately not an option here:
    /// the index this feeds is permanent, so an untagged digest written into it
    /// would be untagged forever.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    // The three fields below are what make a published channel INSTALLABLE
    // rather than merely listed, and their absence is why `<binary>/latest.json`
    // had no producer. (The recipe comment used to blame cosign keyless-OIDC and
    // GHA for that; it was wrong on every clause — see .yah/qed/cli-release.toml.)
    // `install.sh` verifies in two stages and needs all three:
    //
    //   1. sha256, because verifying blake3 first would mean downloading an
    //      unverified `b3sum` to verify with, which is circular. Hence the name:
    //      it is the hash that BOOTSTRAPS the check, with a tool every OS ships.
    //   2. the cosign sigstore bundle at `bundle_url` — ONE file carrying
    //      signature and rfc3161 timestamp, verified with `--key <ref>
    //      --insecure-ignore-tlog` under `ReleaseTrust::Key`. No Fulcio
    //      certificate, no `.cert` sidecar, no OIDC, no GitHub.
    //
    // All three are `Option` because a manifest staged before they existed is
    // still valid input, and because a publisher with no signing key configured
    // must still be able to stage — it just cannot write an install pointer.
    /// Tagged `sha256:<hex>` — the bootstrap digest, per the note above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_hash: Option<String>,
    /// The same digest, bare. The deprecated spelling, kept because live
    /// manifests carry it and `install.sh` still falls back to it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// URL of this artifact's cosign sigstore bundle, `<url>.sigstore.json`.
    /// `None` until a signing pass has run over the staged tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_url: Option<String>,
}

/// Bucket key for the per-binary mutable pointer almanac re-fetches on push.
const MANIFEST_FILENAME: &str = "release-manifest.json";

/// Filename of the IMMUTABLE per-version copy of a binary's manifest, written
/// alongside the mutable pointer at `<binary>/<version>/manifest.json`.
///
/// The mutable pointer answers "what is current" and is rewritten by every
/// release; this copy answers "what was 0.8.21" and never changes, so it is
/// safe to cache forever and safe for the version index to link to. Same split
/// (and same key) as `cli-release-manifest` in `.github/workflows/release.yml`,
/// which writes `s3://yah-dev/yah/<version>/manifest.json` for the same reason.
const VERSIONED_MANIFEST_FILENAME: &str = "manifest.json";

/// Per-triple stable manifest filename (R330-B8). One per (binary, triple),
/// containing only that triple's bundle. Cross-stage merge fan-in feeds on these.
fn per_triple_manifest_filename(triple: &str) -> String {
    format!("release-manifest-{triple}.json")
}

/// Result of staging a release tree into a directory: the object keys written
/// (relative to the staging root) and the per-binary manifests.
#[derive(Debug, Clone, Default)]
pub struct StageReport {
    /// Artifact object keys, e.g. `yah/0.8.6/darwin-aarch64/yah`.
    pub object_keys: Vec<String>,
    /// Manifest object keys, e.g. `yah/release-manifest.json`.
    pub manifest_keys: Vec<String>,
    /// The emitted manifests, keyed by binary name.
    pub manifests: BTreeMap<String, ChannelManifest>,
}

/// Resolve a [`ProducedArtifact::triple`], defaulting to the build host's
/// triple in the `<os>-<arch>` shorthand the channel + updater use.
pub fn resolve_triple(triple: Option<&str>) -> String {
    if let Some(t) = triple.filter(|t| !t.is_empty()) {
        return t.to_string();
    }
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    };
    format!("{os}-{}", std::env::consts::ARCH)
}

// ── The accumulating version index (R330-T32) ────────────────────────────────
//
// `release-manifest.json` is ONE version by construction — it answers "what is
// current". The /releases page is a HISTORY, so it reads a separate object that
// accumulates: `<prefix>/<binary>/index.json`, parsed by almanac's `R2Index`
// source. Publishing only the pointer is why that page was blank — there was a
// producer and a consumer and no object between them.
//
// Shape is almanac's `IndexManifest`/`IndexVersion`/`TripleEntry`. Only `url` is
// required over there; everything else is optional, so this stays additive.

/// The whole index object as published at `<binary>/index.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseIndex {
    pub name: String,
    /// Read but never matched on by the consumer — a producer may add fields
    /// without stranding a deployed reader. A breaking change gets a new key.
    pub schema: u32,
    pub updated_at: String,
    pub versions: Vec<IndexVersion>,
}

/// One published version inside [`ReleaseIndex`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexVersion {
    pub version: String,
    pub pub_date: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_url: Option<String>,
    pub triples: BTreeMap<String, IndexTriple>,
}

/// One (version, triple) download inside [`IndexVersion`].
///
/// `PartialEq`/`Eq` so [`TriplesManifest`] — which reuses this shape for the
/// install pointer — can be compared in tests without hand-written asserts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexTriple {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Tagged `blake3:<hex>`. See [`ChannelBundle::hash`] for why never bare.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    /// Tagged `sha256:<hex>` — the digest an installer can check with tools it
    /// is guaranteed to have. See [`ChannelBundle::bootstrap_hash`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_hash: Option<String>,
    /// The same digest, bare. Deprecated spelling kept because live manifests
    /// carry it; readers prefer `bootstrap_hash`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// URL of the cosign sigstore bundle for this artifact. See
    /// [`ChannelBundle::bundle_url`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_url: Option<String>,
}

/// Fold one release into the index, returning the bytes to publish.
///
/// `existing` is the current object's bytes, or `None` for the create case (a
/// missing key is a first release, not an error).
///
/// Two invariants this exists to hold, both learned the hard way:
///
/// 1. **Replace-or-append, never a bare push.** Re-publishing a version updates
///    its entry instead of duplicating it, which is what makes retrying a
///    failed publish safe.
/// 2. **A version keeps the `pub_date` it was FIRST published with.** Consumers
///    order this list by date (almanac re-sorts on read, and the page sorts
///    again), so restamping on a re-publish would not merely edit a field — it
///    would move an old release to the top of /releases and present it as the
///    newest. Publication dates are historical facts.
/// 3. **Replace-or-append applies PER TRIPLE, not to the map wholesale.** A
///    release matrix lands in slices: each platform leg stages only the
///    artifacts it built, so its [`IndexUpdate`] carries only its own triples.
///    Taking the update's map as the version's whole map means the last leg to
///    finish wins and every earlier platform disappears — which is why the live
///    index carried exactly one triple and /releases showed "Not published yet"
///    for Linux and Windows. Worse, the CAS in the caller *guarantees* that
///    outcome rather than racing for it: a loser re-reads the winner's bytes and
///    then discards the very triples the re-read fetched. Unioning here is what
///    makes that conditional write actually converge.
///
///    The cost is that a triple can only be updated, never dropped, by
///    publishing. That is the right trade for a permanent record — a stale entry
///    is a visible wrong URL, a vanished one is a download that silently stops
///    existing — and removing one is a deliberate hand-edit of the object.
///
/// @yah:ticket(R330-T52, "Normalize the legacy-digest residue out of yah/index.json and make the no-bare-digest invariant hold by construction")
/// @yah:at(2026-09-10T18:07:27Z)
/// @yah:assignee(agent:bundle-anthropic-ashguard)
/// @yah:parent(R330)
/// @yah:next("Operator authorized the cleanup on 2026-09-10, reversing the leave-it call recorded on R330-B50 an hour earlier: hand-editing the published object is permitted IF it makes future work cleaner. It does — see the gotcha. Amend R330-B50's closed-decision handoff and the publish.rs:368-374 comment, both of which currently say the residue is permanent by decision.")
/// @yah:verify("yah.dev/releases still renders all five versions after the write.")
/// @yah:next("THE GATE MUST NOT PUT A NETWORK DEPENDENCY IN THE CAMP-WIDE `check` BAR — an offline laptop must not turn the whole camp red. Prefer a post-write assertion inside the release publish path, or a standalone deliberately-run `scripts/check-*.sh` in the style of its siblings. Implementer's judgment, but say which and why.")
/// @yah:gotcha("WHY THIS IS NOT COSMETICS, which is what the earlier leave-it call assumed. While the live `yah/index.json` violates `the_index_carries_no_bare_digest_at_all`, no gate can be pointed at the LIVE object — it would be red on day one and stay red — so the invariant can only ever be checked against a fixture. That is precisely the dead-writer pathology R330-B50 spent a pass eliminating: a guard reading a generated fixture while the real object drifts. Scrubbing the residue is what unlocks a gate on the thing that actually matters.")
/// @yah:assumes("Tier: Warrior — small diff, but it writes a permanent public record and wants someone who verifies against the CDN rather than against a green build.")
/// @yah:handoff("LEG 1 — THE INVARIANT NOW HOLDS BY CONSTRUCTION. `merge_index` (oss/qed/crates/qed/src/publish.rs) strips legacy bare digests from the WHOLE merged object, history included, not just the incoming update: one `strip_legacy_digests(&mut prior)` immediately before serialize. `IndexTriple::without_legacy_digests` became the in-place `strip_legacy_digests`, since the strip now runs over `values_mut()`. The 356-374 comment block is rewritten: it records BOTH rulings in order and dated (2026-09-10 first ruling, 2026-09-10 REVERSED), states the reasoning that changed — the leave-it call priced the residue as cosmetics and missed that a violated live object makes a live gate impossible, which is the dead-guard pathology R330-B50 removed one layer up — and KEEPS the still-true part: the strip is at the index boundary and not in `index_triples_from_manifest` because that function also builds the install pointer, which must keep the field.")
/// @yah:handoff("LEG 2 — THE REPAIR IS A SUBCOMMAND, `yah qed normalize-index`, DRY-RUN BY DEFAULT. Chosen over a flag on an existing command (the release path must not grow a mode that skips publishing) and over a #[test]-gated helper (an operator cannot run a test against a bucket, and this needs to be re-runnable in a year). Blast radius is bounded in code, not by convention, four ways: (1) it computes its own bytes via `yah_qed::normalize_index` — there is no input that makes it write something else; (2) `removal_only_diff` REFUSES unless deleting exactly the forbidden keys from the input yields a document EQUAL to the output, so a reordered version list, a restamped timestamp or an unknown field eaten by the typed round-trip all abort instead of shipping; (3) an already-clean object is not PUT at all, so re-running is free and leaves Last-Modified alone; (4) an ABSENT key is an error, never a create. It reuses `cas_rewrite_index` — the ETag-then-body, IfMatch, 5-attempt retry loop `cas_merge_index` was refactored into — so it inherits the CAS and `CACHE_CONTROL_NO_CACHE` rather than re-deriving them, and re-normalizes whatever each attempt actually read so a racing publisher is re-merged, not clobbered. `normalize_index` deliberately does NOT restamp `updated_at` and does NOT re-sort: a repair is not a publish, and preserving both is what makes it byte-idempotent.")
/// @yah:handoff("THE LIVE WRITE, WITH ITS PROOF. Pre-flight: re-fetched cdn.yah.dev/yah/index.json and it was BYTE-IDENTICAL to app/yah/cli/tests/fixtures/yah-index-live-2026-09-10.json (10601 bytes, `diff` empty) — no re-snapshot needed. Dry run with `--dump` then diffed mechanically, not by eye: 32 removed lines / 16 added, of which 16 removals are exactly a bare `\"sha256\"` key and the other 16 removed + 16 added are the same `bootstrap_hash` lines differing only by a trailing comma (JSON reflow); ZERO changed lines mention any other field. Asserted separately: updated_at, name, schema identical; version list AND ORDER identical; every pub_date, manifest_url, triple-key order, url, size_bytes, hash, bootstrap_hash, filename, platform, bundle_url identical. Then wrote for real. AFTER: live bytes (9193) are byte-identical to the dry run's predicted output; zero bare digest keys; all five versions with 4/4/4/4/1 triples; updated_at still 2026-09-10T02:33:13.491095+00:00 (proof it was not restamped); `curl -sSI` now answers `cache-control: no-cache, max-age=0` where it previously had NO Cache-Control at all. https://yah.dev/releases renders all five history articles (v0.8.36, v0.8.33, v0.8.32, v0.8.29, v0.8.21) with their per-asset blake3 hashes. The install pointer `yah/latest.json` was NOT touched — Last-Modified still 02:33:13 and all four triples keep their bare `sha256`, so the deliberate asymmetry holds. NO OTHER CDN OR BUCKET WRITE OF ANY KIND WAS PERFORMED.")
/// @yah:handoff("LEG 3 — THE GATE IS BOTH SHAPES THE TICKET OFFERED, because they catch different things and neither alone is the payoff. (a) A PRE-WRITE REFUSAL on the publish path: `cas_rewrite_index` probes the bytes it is about to PUT with `yah_qed::index_legacy_bare_digest_paths` and returns Err naming the offending paths rather than writing. Pre-write, not post-write — post-write is a diagnosis of a public record you have already ruined. It needs no network, runs on every release, and cannot be bypassed by adding another index writer because there is exactly one door. (b) `scripts/check-yah-index-live.sh` reads the LIVE object off the CDN — the only guard that catches drift arriving from OUTSIDE this codebase (a hand-rolled PUT, a restored backup, an older binary), which no unit test can. It asserts four things: no legacy bare digest anywhere, every hash blake3:-tagged and every bootstrap_hash sha256:-tagged, a well-formed newest-first multi-version history with pub_date/manifest_url/url on every entry (a tripwire against a \"repair\" that satisfies the invariant by losing the contents), and cache-control containing no-cache. DELIBERATELY NOT in .yah/qed/yah-check.toml — a network dependency in the camp-wide bar turns an offline laptop red for the whole camp, and a gate that goes red for reasons unrelated to the change gets muted, which is how the fixture guards came to mean nothing. It takes an optional URL argument SO IT CAN BE MUTATION-TESTED, and was: against the repaired object 5 passed / 0 failed; against a file:// copy with one bare sha256 spliced back in it FAILS and names the exact path; against the live object BEFORE the write it failed on both the 16 keys and the missing Cache-Control. shellcheck -S warning and bash -n clean. I did not wire it into yah-cli-release.toml: every .yah/qed/*.toml is dirty in this tree and the ticket asked for one of the two shapes, not a recipe edit.")
/// @yah:handoff("THE FIXTURE TEST: KEPT, ARGUED, AND ITS COMMENT MADE TRUTHFUL. `the_index_carries_no_bare_digest_at_all` (oss/mesofact/crates/almanac/tests/producer_shapes.rs:157) stays. It is NOT the same false assurance it was flagged as, because what it now uniquely covers is that release.yml's `del(.sha256, .blake3)` still does what it claims — that workflow is dead as a publisher but live as the REFERENCE for the index's JSON shape, it is the only artifact stating the invariant in a form a non-Rust reader can check, and THE TWO PRODUCERS AGREEING is a property nothing else asserts. Deleting it would lose that for no gain. Its comment is rewritten to a three-guard table naming what each of the three sees that the others cannot (this test → the workflow; qed's two unit tests → merge_index output, fixture-free; check-yah-index-live.sh → the live object) and ending with the one thing it must never again be read as: a green here says NOTHING about what the CDN is serving.")
/// @yah:handoff("THE ONE TEST ASSERTION I CHANGED, AND WHY IT IS NOT A WEAKENING. `merging_the_next_release_preserves_all_five_historical_entries` (app/yah/cli/src/qed_publish.rs) asserted `now == old` for every prior entry after a merge. With the strip widened to history that is false BY DESIGN, so it now asserts `now == old-minus-the-forbidden-keys` — every pub_date, manifest_url, triple key and remaining field still compared exactly, so a merge that drops a pub_date or a triple still fails it. Two assertions were ADDED alongside to stop it degrading: the fixture must still CARRY residue (otherwise the comparison is vacuously the old equality test, and the failure message says to delete the assertion and lean on qed's fixture-free `merging_strips_a_prior_entrys_legacy_bare_digest_too`), and the merged object must probe clean. Note `the_one_triple_0_8_21_entry_is_left_exactly_as_it_is` — the obvious candidate — needed NO change: 0.8.21 has no bare sha256 and no bootstrap_hash at all, so the strip is a no-op on it. It still passes untouched.")
/// @yah:verify("ALL FOUR NAMED BASELINES MET. `cargo test -p yah-qed --lib` (from oss/qed) → 982 passed / 1 failed / 1 ignored against the 979/1/1 baseline: +3, all mine, all passing. The single failure is still `tests::desktop_release_matrix_routes_each_row_to_its_own_platform`, confirmed by name, untouched. `cargo test -p yah --lib qed_publish` → 14 passed / 0 failed against the 9/9 baseline: +5, all mine. `cargo test -p yah-object-store` (from oss/yah-base) → 49 passed / 0 failed, exactly baseline. `cd app/yah/web/marketing && bun test` → 25 pass / 0 fail, exactly baseline. Also `cargo test -p yah-almanac` (from oss/mesofact) → 132 + 5 + 10 passed / 0 failed / 1 ignored, unchanged by the comment edit. `cargo build -p yah` clean. No orphan-gc symptom at any point — no missing-extern, no vanished OUT_DIR file — so nothing was cleaned and R770 has nothing new from this pass.")
/// @yah:verify("LIVE, AFTER THE WRITE: `curl https://cdn.yah.dev/yah/index.json` → 9193 bytes, 0 bare sha256/blake3 keys, versions [0.8.36:4, 0.8.33:4, 0.8.32:4, 0.8.29:4, 0.8.21:1], updated_at unchanged at 2026-09-10T02:33:13.491095+00:00. `curl -sSI` → `cache-control: no-cache, max-age=0` (previously absent entirely). `curl https://yah.dev/releases` → history articles v0.8.36, v0.8.33, v0.8.32, v0.8.29, v0.8.21, each with its assets and blake3 hashes. `scripts/check-yah-index-live.sh` → 5 passed / 0 failed (it was 3 passed / 2 failed before the write). `curl https://cdn.yah.dev/yah/latest.json` → untouched, Last-Modified still 02:33:13, all four triples keep their bare sha256.")
/// @yah:gotcha("THE COMMITTED FIXTURE IS NOW THE PRE-REPAIR SNAPSHOT ON PURPOSE. app/yah/cli/tests/fixtures/yah-index-live-2026-09-10.json no longer matches what the CDN serves, and must not be \"refreshed\" as hygiene: it is the only real, drifted object in the repo, which makes it the only thing that can exercise the history strip and the repair against something other than a tidy synthetic index. Its doc comment in qed_publish.rs says so, and two assertions fail loudly with instructions if someone refreshes it anyway. The CURRENT object is asserted by scripts/check-yah-index-live.sh instead.")
/// @yah:gotcha("THE REPAIR WAS RUN FROM target/debug/yah, NOT FROM AN INSTALLED BINARY, and I did NOT run `cargo xtask install`. Deliberate: the subcommand is brand new so only the freshly-built binary has it, and installing over ~/.local/bin/yah would push a build made from a tree carrying ~180 other sessions' uncommitted edits onto the operator's shell and every QED step's `argv = [\"yah\", ...]`. If you want `yah qed normalize-index` on PATH, that install is yours to run (per app/yah/cli/CLAUDE.md), and note the desktop's copy at /Applications/yah.app/Contents/MacOS/yah is a separate target.")
/// @yah:verify("Leader-verified post-write (@Ashguard:eclipse): live index 0 bare digests / 5 versions / triple counts and pub_dates preserved / cache-control now present; latest.json untouched and still carries its bare sha256 fallback for install.sh; yah.dev/releases renders all five versions; scripts/check-yah-index-live.sh 5/5; yah-qed --lib 982/1/1 (baseline 979/1/1, same single pre-existing failure); yah --lib qed_publish 14/0 (baseline 9/0).")
/// @yah:gotcha("THIS TICKET IS DONE AND ITS COLUMN IS LYING. It reads `handoff`, which means \"a baton is waiting for a picker\" — there is no baton and nothing to pick up. All three legs landed, the live write is performed, and the leader independently re-verified every claim (see the appended verify entry). It could not be moved to `review` because `arch.review_ticket` refuses every ticket anchored under a non-subcamp `oss/` workspace with `conflict: ticket not found`, while `board.show` and `board.update` resolve the same id fine — reproducible from the MCP verb, from `yah board review`, with `--path oss/qed`, and from inside oss/qed. Filed as R511-B7 with the diagnosis. **Do not claim this ticket.** Treat it as review-pending and sign it off directly.")
pub fn merge_index(
    existing: Option<&str>,
    binary: &str,
    version: &str,
    pub_date: &str,
    manifest_url: Option<String>,
    triples: BTreeMap<String, IndexTriple>,
) -> Result<String, serde_json::Error> {
    let version = version.trim_start_matches('v').to_string();
    let mut prior: Vec<IndexVersion> = match existing.map(str::trim).filter(|s| !s.is_empty()) {
        Some(raw) => serde_json::from_str::<ReleaseIndex>(raw)?.versions,
        None => Vec::new(),
    };

    let previous = prior.iter().find(|v| v.version == version).cloned();
    // Prior triples first, this update's on top: a leg re-publishing a triple it
    // already wrote replaces that entry, and every triple it did not build
    // survives untouched.
    let mut merged_triples = previous
        .as_ref()
        .map(|v| v.triples.clone())
        .unwrap_or_default();
    merged_triples.extend(triples);

    prior.retain(|v| v.version != version);
    prior.push(IndexVersion {
        version,
        pub_date: previous
            .as_ref()
            .map(|v| v.pub_date.clone())
            .unwrap_or_else(|| pub_date.to_string()),
        // Same rule one field over: a leg that cannot derive the URL (no
        // `base_url` configured) must not erase one an earlier leg did.
        manifest_url: manifest_url.or_else(|| previous.and_then(|v| v.manifest_url)),
        triples: merged_triples,
    });

    // Newest first, and deterministic: two entries sharing a `pub_date` must
    // not be free to swap places between publishes, or the feed diffs as
    // changed and rebuilds a history that did not move.
    prior.sort_by(|a, b| {
        b.pub_date
            .cmp(&a.pub_date)
            .then_with(|| b.version.cmp(&a.version))
    });

    // THE INDEX BOUNDARY (R330-B50, widened by R330-T52). Every legacy bare
    // digest is stripped here, at the one choke point every index write passes
    // through — see [`IndexTriple::strip_legacy_digests`]. Doing it here rather
    // than in `index_triples_from_manifest` is deliberate and still
    // load-bearing: that function also builds the install POINTER
    // (`<binary>/latest.json`), which MUST keep the bare `sha256` because every
    // `install.sh` already on someone's machine reads it as a fallback.
    //
    // Applied to the WHOLE object — the incoming update AND every prior entry.
    // That is a REVERSAL, made on purpose, and the reasoning it replaces was
    // sound under the ruling it was written for. Both rulings, in order:
    //
    //   2026-09-10, first ruling. The strip covered the incoming update only.
    //   `merge_index` re-serializes every historical entry on every merge, so
    //   stripping `prior` too would rewrite already-published history as a side
    //   effect of the next release — and the 16 bare `sha256` keys the live
    //   `yah/index.json` had accumulated were ruled permanent, a public record
    //   not to be edited.
    //
    //   2026-09-10, REVERSED (R330-T52). The operator authorised the cleanup and
    //   the live object was normalized the same day, through this same code, by
    //   `yah qed normalize-index`. What the first ruling weighed as "rewriting
    //   history for cosmetics" had a cost it did not price: while the live
    //   object violated the invariant, no gate could be pointed AT the live
    //   object — it would be red on day one and stay red — so the invariant
    //   could only ever be asserted against a fixture emitted by a producer that
    //   has never published a release. That is the exact dead-guard pathology
    //   R330-B50 spent a pass removing.
    //
    // So the invariant now holds BY CONSTRUCTION rather than only for new
    // entries: a regression in any other producer of this object cannot leave
    // permanent residue, because the next release normalizes it away. The strip
    // is lossless and is a REMOVAL only — every entry it touches keeps the same
    // digest in its tagged `bootstrap_hash`, and no published value is ever
    // edited. `cas_merge_index` (app/yah/cli/src/qed_publish.rs) refuses to PUT
    // bytes that still carry one, so a future edit that bypasses this line fails
    // the publish instead of publishing the violation.
    strip_legacy_digests(&mut prior);

    serde_json::to_string_pretty(&ReleaseIndex {
        name: binary.to_string(),
        schema: 1,
        updated_at: Utc::now().to_rfc3339(),
        versions: prior,
    })
}

/// Strip every legacy bare digest out of an index's version list, in place.
///
/// The whole-object half of the index boundary — see [`merge_index`] and
/// [`IndexTriple::strip_legacy_digests`].
fn strip_legacy_digests(versions: &mut [IndexVersion]) {
    for version in versions.iter_mut() {
        for entry in version.triples.values_mut() {
            entry.strip_legacy_digests();
        }
    }
}

/// Rewrite an EXISTING index object so it satisfies the no-bare-digest
/// invariant, changing nothing else (R330-T52).
///
/// The repair half of the index boundary. [`merge_index`] holds the invariant
/// for every write a release makes; this holds it for an object that already
/// drifted, without inventing a version to hang the write on.
///
/// Two things it deliberately does NOT do, both of which [`merge_index`] does
/// and both of which would make this something other than a repair:
///
/// - **It does not restamp `updated_at`.** Normalizing removes keys that were
///   never supposed to be published; it does not publish anything. Restamping
///   would report a release that did not happen, and — more usefully — leaving
///   it alone makes this function byte-idempotent, so a second run produces
///   input-identical bytes and the caller can skip the write entirely.
/// - **It does not re-sort.** The version order is part of what the caller
///   promised not to touch, so the only difference between input and output is
///   removed keys. A caller that wants to *prove* that (and the repair command
///   does, before it will write anything) can diff the two directly.
pub fn normalize_index(existing: &str) -> Result<String, serde_json::Error> {
    let mut index: ReleaseIndex = serde_json::from_str(existing)?;
    strip_legacy_digests(&mut index.versions);
    serde_json::to_string_pretty(&index)
}

/// Every JSON path in an index object that carries a legacy bare digest key.
///
/// Empty is the invariant, stated once so a producer, a repair and a live gate
/// can all ask the same question of the same bytes rather than each
/// re-deriving it. See [`IndexTriple::strip_legacy_digests`] for why the index
/// answers "no" here while the install pointer answers "yes".
///
/// Reads the raw JSON rather than [`ReleaseIndex`], deliberately: deserializing
/// into the typed shape silently drops any key the struct has no field for —
/// there is no `blake3` field at all — so a typed probe would report clean on
/// bytes that are not.
pub fn index_legacy_bare_digest_paths(index_json: &str) -> Result<Vec<String>, serde_json::Error> {
    let doc: serde_json::Value = serde_json::from_str(index_json)?;
    let mut found = Vec::new();
    let Some(versions) = doc.get("versions").and_then(|v| v.as_array()) else {
        return Ok(found);
    };
    for version in versions {
        let label = version
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("<unversioned>");
        let Some(triples) = version.get("triples").and_then(|t| t.as_object()) else {
            continue;
        };
        for (triple, entry) in triples {
            for legacy in LEGACY_BARE_DIGEST_KEYS {
                if entry.get(legacy).is_some() {
                    found.push(format!("$.versions[{label}].triples.{triple}.{legacy}"));
                }
            }
        }
    }
    Ok(found)
}

/// The bare-digest key names the index forbids, mirroring the
/// `del(.sha256, .blake3)` in `.github/workflows/release.yml`'s index step.
pub const LEGACY_BARE_DIGEST_KEYS: [&str; 2] = ["sha256", "blake3"];

/// Bucket key for a binary's accumulating index.
pub fn index_key(prefix: Option<&str>, binary: &str) -> String {
    join_key(prefix, &[binary, "index.json"])
}

/// One binary's contribution to the index, handed across the publisher seam.
///
/// Carries the inputs [`merge_index`] needs plus the key to read-modify-write,
/// so the publisher impl supplies only the I/O — the merge semantics (and the
/// two invariants they hold) stay here, in the tested crate, rather than being
/// re-derived by every backend.
#[derive(Debug, Clone)]
pub struct IndexUpdate {
    /// Bucket key of the index object, prefix already applied.
    pub key: String,
    pub binary: String,
    pub version: String,
    /// Publish timestamp for a version appearing here for the FIRST time. A
    /// version already in the index keeps the date it was first published with
    /// — see [`merge_index`].
    pub pub_date: String,
    pub manifest_url: Option<String>,
    pub triples: BTreeMap<String, IndexTriple>,
}

impl IndexTriple {
    /// Drop the legacy bare digests before this entry enters the **index**.
    ///
    /// [`IndexTriple`] is deliberately shared by two published shapes, and they
    /// disagree about exactly this field:
    ///
    /// - the **install pointer** (`<binary>/latest.json`, [`TriplesManifest`])
    ///   MUST keep bare `sha256`. Every `install.sh` already on someone's
    ///   machine falls back to it when `bootstrap_hash` is absent, which is why
    ///   `$.triples.*.sha256` is grandfathered in almanac's
    ///   `LEGACY_BARE_HEX_PATHS`. Removing it there breaks installs we cannot
    ///   reach.
    /// - the **index** (`<binary>/index.json`) must NOT. It is a permanent
    ///   accumulating record with no in-the-wild consumer, so there has never
    ///   been anything to grandfather — the answer is no from its first byte.
    ///
    /// `.github/workflows/release.yml:1930` has always drawn that line in the
    /// same place, building each index entry from the pointer's own per-triple
    /// data through `map_values(del(.sha256, .blake3))`. This is that `del()`,
    /// in Rust, at the same boundary.
    ///
    /// R330-B50 found the two producers had drifted: the workflow stripped and
    /// this crate did not, so the live `yah/index.json` accumulated 16 bare
    /// `sha256` keys while the invariant's own test stayed green — it reads a
    /// fixture generated from the workflow, i.e. from the producer that has
    /// never published. There is no `blake3` field on this type, so `sha256` is
    /// the whole of the strip; [`index_legacy_bare_digest_paths`] checks the raw
    /// JSON for both, because an untyped key is exactly what this type cannot
    /// see.
    ///
    /// In place rather than by value (R330-T52) because the strip now runs over
    /// the whole merged object, historical entries included — see
    /// [`merge_index`].
    fn strip_legacy_digests(&mut self) {
        self.sha256 = None;
    }
}

impl IndexUpdate {
    /// Fold this release into the index's current bytes (`None` = first
    /// publish), returning the bytes to write back.
    pub fn merge(&self, existing: Option<&str>) -> Result<String, serde_json::Error> {
        merge_index(
            existing,
            &self.binary,
            &self.version,
            &self.pub_date,
            self.manifest_url.clone(),
            self.triples.clone(),
        )
    }
}

/// Map a target triple onto the platform token `/releases` keys its labels by.
///
/// The consumer chain is: this token → almanac's `ReleaseAsset::platform` →
/// `PLATFORM_LABELS` in `app/yah/web/marketing/src/releases.ts`. A token with no
/// entry there falls through and the page renders the raw string, so "publish
/// the triple and let something downstream figure it out" shows a visitor
/// `aarch64-apple-darwin` where the label should read "macOS (Apple Silicon)".
///
/// Two spellings arrive here and both must map: the full Rust triple a
/// cross-build declares (`aarch64-apple-darwin`) and the `<os>-<arch>`
/// shorthand [`resolve_triple`] synthesises for a host-native build
/// (`darwin-aarch64`) — the channel manifest is keyed by whichever the
/// producing step used.
///
/// **musl is checked before gnu, deliberately.** almanac's own fallback mapper
/// matches on the `x86_64-unknown-linux` prefix and so collapses the two onto
/// one token, listing a musl and a gnu binary as the same download. They are
/// not interchangeable. Supplying the token from here — rather than leaving
/// `platform` unset and letting that fallback run — is what keeps them apart.
pub fn platform_token(triple: &str) -> String {
    let t = triple.to_ascii_lowercase();
    let musl = t.contains("musl");
    let arm = t.contains("aarch64") || t.contains("arm64");
    let x86 = t.contains("x86_64") || t.contains("amd64");

    if t.contains("apple") || t.contains("darwin") || t.contains("macos") {
        if arm {
            return "macos-arm64".to_string();
        }
        if x86 {
            return "macos-x86_64".to_string();
        }
    } else if t.contains("windows") {
        if x86 {
            return "windows-x86_64".to_string();
        }
    } else if t.contains("linux") {
        return match (arm, x86, musl) {
            (true, _, true) => "linux-aarch64-musl".to_string(),
            (true, _, false) => "linux-arm64".to_string(),
            (_, true, true) => "linux-x86_64-musl".to_string(),
            (_, true, false) => "linux-x86_64".to_string(),
            _ => triple.to_string(),
        };
    }
    // Unrecognised: pass the triple through rather than guess. The page shows
    // it verbatim, which is a visible prompt to add a mapping — better than a
    // wrong label that reads as correct.
    triple.to_string()
}

/// Project a staged [`ChannelManifest`] onto the index's per-triple shape.
pub fn index_triples_from_manifest(manifest: &ChannelManifest) -> BTreeMap<String, IndexTriple> {
    manifest
        .host
        .bundle
        .iter()
        .map(|(triple, bundle)| {
            (
                triple.clone(),
                IndexTriple {
                    url: bundle.url.clone(),
                    platform: Some(platform_token(triple)),
                    filename: bundle
                        .url
                        .rsplit('/')
                        .next()
                        .filter(|f| !f.is_empty())
                        .map(str::to_string),
                    size_bytes: bundle.size,
                    hash: bundle.hash.clone(),
                    bootstrap_hash: bundle.bootstrap_hash.clone(),
                    sha256: bundle.sha256.clone(),
                    bundle_url: bundle.bundle_url.clone(),
                },
            )
        })
        .collect()
}

/// The install-pointer object — `<binary>/latest.json`, what `install.sh`
/// resolves and the single answer to "what does `curl … | sh` fetch right now".
///
/// A DISTINCT shape from [`ChannelManifest`], and the difference is not
/// cosmetic. The channel manifest nests under `host.bundle[triple]` and is what
/// almanac's `R2Channel` reader and the Tauri updater consume. This one is flat
/// — `triples[<triple>]` — because `install.sh` reads `.triples[$t][$f]` with
/// `jq`, and that layout has live consumers on machines we do not control. The
/// producer adapts to the installer, never the other way round; almanac already
/// carries a matching `r2-triples` reader for exactly this object.
///
/// The same shape `scripts/publish-{yubaba,mesofact}-release.sh` write by hand,
/// so all three products' pointers stay one format rather than three.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriplesManifest {
    /// Package name as `install.sh` knows it — the `<pkg>` in `<pkg>/latest.json`.
    pub name: String,
    /// Release version, without a leading `v`.
    pub version: String,
    /// ISO-8601 UTC publish timestamp.
    pub pub_date: String,
    pub triples: BTreeMap<String, IndexTriple>,
}

/// Project a staged [`ChannelManifest`] onto the install-pointer shape.
///
/// Returns `None` unless EVERY triple carries both a bootstrap digest and a
/// signature bundle, which is the one refusal that matters here: a pointer is
/// the object an installer trusts, and one written with a triple missing its
/// `bundle_url` produces a `cosign is installed but the manifest carries no
/// signature bundle for <triple>` die() on the user's machine — a broken
/// install advertised as a release. Staging without signing is legal; pointing
/// the world at it is not.
pub fn triples_manifest(binary: &str, manifest: &ChannelManifest) -> Option<TriplesManifest> {
    let triples = index_triples_from_manifest(manifest);
    if triples.is_empty() {
        return None;
    }
    if triples
        .values()
        .any(|t| t.bundle_url.is_none() || t.bootstrap_hash.is_none())
    {
        return None;
    }
    Some(TriplesManifest {
        name: binary.to_string(),
        version: manifest.version.clone(),
        pub_date: manifest.pub_date.clone(),
        triples,
    })
}

/// Bucket key for a binary's install pointer.
pub fn latest_key(prefix: Option<&str>, binary: &str) -> String {
    join_key(prefix, &[binary, "latest.json"])
}

/// Streaming SHA-256 of a staged artifact, returned as a BARE hex digest.
///
/// Bare, unlike [`blake3_file`], because both spellings are needed downstream
/// and tagging is the caller's job: `bootstrap_hash` wants `sha256:<hex>` and
/// the deprecated `sha256` field wants the digest alone. Streamed for the same
/// reason blake3 is — these are hundred-megabyte tarballs.
fn sha256_file(path: &Path) -> std::io::Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Streaming BLAKE3 of a staged artifact, returned already tagged.
///
/// Streamed rather than slurped because these are release binaries and
/// tarballs — reading a few hundred MB into memory to hash it is a needless
/// way to fail on a small runner.
fn blake3_file(path: &Path) -> std::io::Result<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

fn join_key(prefix: Option<&str>, parts: &[&str]) -> String {
    let mut segs: Vec<&str> = Vec::new();
    if let Some(p) = prefix.map(str::trim).filter(|p| !p.is_empty()) {
        segs.push(p.trim_matches('/'));
    }
    segs.extend_from_slice(parts);
    segs.join("/")
}

/// Lay the artifacts out into `staging_dir` as the release channel tree and
/// write each binary's `release-manifest.json`. Pure filesystem work — no
/// network. The caller hands `staging_dir` to a [`ReleasePublisher`] to upload.
pub fn stage_release(
    staging_dir: &Path,
    artifacts: &[ProducedArtifact],
    version: &str,
    prefix: Option<&str>,
    base_url: Option<&str>,
) -> std::io::Result<StageReport> {
    let version = version.trim_start_matches('v').to_string();
    let mut report = StageReport::default();
    // binary -> (triple -> ChannelBundle), preserving deterministic order.
    let mut bundles: BTreeMap<String, BTreeMap<String, ChannelBundle>> = BTreeMap::new();

    for artifact in artifacts {
        let triple = resolve_triple(artifact.triple.as_deref());
        let src = Path::new(&artifact.path);
        let filename = src.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("artifact path has no filename: {}", artifact.path),
            )
        })?;

        let key = join_key(prefix, &[&artifact.binary, &version, &triple, filename]);
        let dest = staging_dir.join(&key);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Name the file. This is the ONE place staging opens a caller-supplied
        // path, and a bare `No such file or directory (os error 2)` at the end
        // of a multi-minute release build is close to undebuggable — it does
        // not say which artifact, which step declared it, or which tree it was
        // resolved against. Every `produces` bug lands here.
        let bytes = std::fs::copy(src, &dest).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "staging artifact {} (binary={}, triple={}): {e} — the step that \
                     declares this `produces` must leave the file at exactly this path, \
                     resolved against the run's workspace",
                    src.display(),
                    artifact.binary,
                    triple
                ),
            )
        })?;
        // Hash the STAGED copy, not the source: what a consumer downloads is
        // what was uploaded, so the digest has to describe the bytes that
        // actually landed in the channel.
        let hash = blake3_file(&dest)?;
        // Same reasoning as the blake3 above — hash what LANDED, not the source.
        let sha256 = sha256_file(&dest)?;

        let url = match base_url.map(str::trim).filter(|b| !b.is_empty()) {
            Some(base) => format!("{}/{}", base.trim_end_matches('/'), key),
            None => key.clone(),
        };
        bundles.entry(artifact.binary.clone()).or_default().insert(
            triple,
            ChannelBundle {
                // `bundle_url` stays None here on purpose: staging is a pure
                // filesystem operation and signing is not. A signing pass over
                // the staged tree fills it in, and a publisher that cannot sign
                // leaves it None rather than advertising a bundle that 404s.
                bundle_url: None,
                bootstrap_hash: Some(format!("sha256:{sha256}")),
                sha256: Some(sha256),
                url,
                size: Some(bytes),
                hash: Some(hash),
            },
        );
        report.object_keys.push(key);
    }

    let pub_date = Utc::now().to_rfc3339();
    for (binary, bundle) in bundles {
        // Per-triple stable manifests: one file per (binary, triple) containing
        // only that triple's bundle entry. Idempotent under repeated publishes
        // of the same triple; safe under concurrent publishes of different
        // triples (different keys). The GHA assembler reads these to build the
        // signed shared manifest. (R330-B8)
        for (triple, single_bundle) in &bundle {
            let mut per_triple_bundle: BTreeMap<String, ChannelBundle> = BTreeMap::new();
            per_triple_bundle.insert(triple.clone(), single_bundle.clone());
            let manifest = ChannelManifest {
                version: version.clone(),
                pub_date: pub_date.clone(),
                notes: None,
                host: ChannelHost {
                    bundle: per_triple_bundle,
                },
            };
            let manifest_key = join_key(prefix, &[&binary, &per_triple_manifest_filename(triple)]);
            let dest = staging_dir.join(&manifest_key);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let json = serde_json::to_vec_pretty(&manifest)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            std::fs::write(&dest, json)?;
            report.manifest_keys.push(manifest_key);
        }

        // Shared manifest: this-stage's view of all triples it built. For a
        // single-stage publish this is the authoritative manifest; for a
        // multi-stage publish it is "best-effort latest" until the GHA
        // assembler overwrites it with the merged signed version.
        let manifest = ChannelManifest {
            version: version.clone(),
            pub_date: pub_date.clone(),
            notes: None,
            host: ChannelHost { bundle },
        };
        let json = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Two keys, same bytes. The immutable per-version copy is what the
        // version index links to: pointing a HISTORY entry at the mutable
        // pointer means clicking "0.8.21's manifest" hands you whatever is
        // current, which is wrong the moment a second version exists.
        for filename in [
            VERSIONED_MANIFEST_FILENAME,
            // Written LAST so a failed per-version write never leaves the
            // pointer aimed at a version whose manifest is not published.
            MANIFEST_FILENAME,
        ] {
            let manifest_key = if filename == VERSIONED_MANIFEST_FILENAME {
                join_key(prefix, &[&binary, &version, filename])
            } else {
                join_key(prefix, &[&binary, filename])
            };
            let dest = staging_dir.join(&manifest_key);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dest, &json)?;
            report.manifest_keys.push(manifest_key);
        }
        report.manifests.insert(binary, manifest);
    }

    report.object_keys.sort();
    report.manifest_keys.sort();
    Ok(report)
}

// ── The publish adapter seam ────────────────────────────────────────────────

/// Performs the I/O the [`PublishingOutcomeDispatcher`] can't do itself:
/// uploading a staged channel tree to a bucket and firing the revalidate hook.
///
/// The qed crate stays dependency-light by keeping this abstract — the CLI
/// supplies a Cloudflare-R2-backed impl (reusing `cloud::publish_to_r2` + a
/// reqwest POST), and tests supply a recording fake.
#[async_trait]
pub trait ReleasePublisher: Send + Sync {
    /// Upload every file under `staging_dir` (already laid out as the channel
    /// tree) to `bucket` on `provider`, under the optional key `prefix`.
    async fn sync(
        &self,
        staging_dir: &Path,
        provider: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<(), RunnerError>;

    /// Read-modify-write the accumulating version index at `update.key`
    /// (R330-T32) — the object the /releases history renders from, as opposed
    /// to the single-version pointer `sync` uploads.
    ///
    /// The impl performs three steps and owns only the middle one's I/O: read
    /// the current bytes (absent = first release, not an error), call
    /// [`IndexUpdate::merge`], write the result back.
    ///
    /// **The write MUST be a compare-and-swap** — conditional on the ETag the
    /// bytes were read at, retrying the whole read-merge-write on a failed
    /// precondition. This object is a permanent record that every publisher
    /// appends to, so an unconditional PUT loses whichever concurrent release
    /// wrote first, silently and irreversibly. A backend that cannot do a
    /// conditional write should say so rather than emulate one.
    async fn publish_index(
        &self,
        provider: &str,
        bucket: &str,
        update: &IndexUpdate,
    ) -> Result<(), RunnerError>;

    /// Fire the almanac revalidate hook so the feed re-renders from this
    /// release. A no-configured-receiver impl returns `Ok(())`.
    ///
    /// `report` is the release that was just staged and uploaded, manifests
    /// included. It is passed rather than withheld because the poke is
    /// expected to **carry its payload** (R330-F33): the run that just cut the
    /// release holds the fact, so it hands the manifest over instead of
    /// publishing it and waiting for the consumer's poller to notice. An impl
    /// with no manifest to offer can ignore it and poke payload-less — that
    /// stays a supported mode, it just leaves one stale render before the
    /// fetch tier converges.
    ///
    /// An `Err` here is REPORTED, NOT FATAL: implementors should return the
    /// real failure (so it is visible in logs and to direct callers), but
    /// [`PublishingOutcomeDispatcher::publish`] deliberately downgrades it to
    /// a warning. The poke only collapses the staleness window — the
    /// consumer's own feed-fetch tier is what makes the feed correct — so an
    /// unreachable receiver must not fail a release whose artifacts uploaded
    /// fine. Do NOT swallow the error in the impl to get that behaviour.
    async fn revalidate(&self, report: &StageReport) -> Result<(), RunnerError>;
}

/// The real outcome dispatcher (R330-F3): stages produced artifacts into the
/// release channel layout, uploads them via a [`ReleasePublisher`], then fires
/// the revalidate hook (best-effort — see [`ReleasePublisher::revalidate`]).
/// `yubaba_deploy` / `almanac_run` stay logging stubs
/// (those backends are still pending — R040-F4 / the almanac scheduler).
pub struct PublishingOutcomeDispatcher<P: ReleasePublisher> {
    publisher: P,
}

impl<P: ReleasePublisher> PublishingOutcomeDispatcher<P> {
    pub fn new(publisher: P) -> Self {
        Self { publisher }
    }
}

#[async_trait]
impl<P: ReleasePublisher> OutcomeDispatcher for PublishingOutcomeDispatcher<P> {
    async fn yubaba_deploy(&self, service: &str, env: &str) -> Result<(), RunnerError> {
        tracing::info!(
            service,
            env,
            "qed outcome: yubaba-deploy skipped (yubaba deploy RPC not yet stable, R040-F4)"
        );
        Ok(())
    }

    async fn almanac_run(&self, pipeline: &str) -> Result<(), RunnerError> {
        tracing::info!(
            pipeline,
            "qed outcome: almanac-run skipped (cadence scheduler pending)"
        );
        Ok(())
    }

    async fn publish(&self, req: &PublishRequest) -> Result<(), RunnerError> {
        if req.artifacts.is_empty() {
            tracing::warn!(
                bucket = %req.bucket,
                "qed outcome: publish has no produced artifacts — \
                 declare `produces` on the build steps; skipping"
            );
            return Ok(());
        }

        let staging = tempfile::tempdir()?;
        let report = stage_release(
            staging.path(),
            &req.artifacts,
            &req.version,
            req.prefix.as_deref(),
            req.base_url.as_deref(),
        )?;
        tracing::info!(
            provider = %req.provider,
            bucket = %req.bucket,
            version = %req.version,
            objects = report.object_keys.len(),
            manifests = report.manifest_keys.len(),
            "qed outcome: staged release channel"
        );

        self.publisher
            .sync(
                staging.path(),
                &req.provider,
                &req.bucket,
                req.prefix.as_deref(),
            )
            .await?;

        // The index is part of the release record, not a nicety layered on
        // top: `sync` uploads a pointer to THIS version, and the history page
        // reads the index. A release whose artifacts uploaded but whose index
        // write failed is invisible on /releases, so this error propagates
        // (unlike the revalidate poke below, which only affects latency).
        for (binary, manifest) in &report.manifests {
            let update = IndexUpdate {
                key: index_key(req.prefix.as_deref(), binary),
                binary: binary.clone(),
                version: manifest.version.clone(),
                pub_date: manifest.pub_date.clone(),
                // The IMMUTABLE per-version manifest, not the mutable pointer:
                // this is a history entry, so it must keep resolving to the
                // manifest of THIS version after the next release lands.
                //
                // Derived from the same (base_url, prefix, binary, version) the
                // staging pass used, NOT by string surgery on an asset URL —
                // the two must agree, and reconstructing one from the other is
                // the kind of coupling that works until a layout changes.
                manifest_url: req
                    .base_url
                    .as_deref()
                    .map(str::trim)
                    .filter(|b| !b.is_empty())
                    .map(|base| {
                        format!(
                            "{}/{}",
                            base.trim_end_matches('/'),
                            join_key(
                                req.prefix.as_deref(),
                                &[binary, &manifest.version, VERSIONED_MANIFEST_FILENAME]
                            )
                        )
                    }),
                triples: index_triples_from_manifest(manifest),
            };
            self.publisher
                .publish_index(&req.provider, &req.bucket, &update)
                .await?;
            tracing::info!(
                bucket = %req.bucket,
                key = %update.key,
                version = %update.version,
                triples = update.triples.len(),
                "qed outcome: merged release into version index"
            );
        }

        // The poke is a LATENCY OPTIMISATION, not the correctness path
        // (R330-T14, re-scoped by R330-F31): the node's feed-fetch tier
        // re-renders from upstream on its own timer, so a release that never
        // gets poked is still correct within `feed_interval_secs`. Aborting
        // the whole publish on an unreachable receiver would therefore throw
        // away a successful artifact upload to save nothing — warn and let
        // the release stand.
        if let Err(e) = self.publisher.revalidate(&report).await {
            tracing::warn!(
                bucket = %req.bucket,
                version = %req.version,
                error = %e,
                "qed outcome: revalidate poke failed — release stands; the feed-fetch \
                 tier will pick the new version up on its next tick"
            );
        }
        Ok(())
    }
}

/// A [`ReleasePublisher`] that does nothing but log — the default when no real
/// bucket/receiver is wired (e.g. a local `yah qed run` with no credentials).
pub struct LoggingReleasePublisher;

#[async_trait]
impl ReleasePublisher for LoggingReleasePublisher {
    async fn sync(
        &self,
        staging_dir: &Path,
        provider: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<(), RunnerError> {
        tracing::info!(
            provider,
            bucket,
            prefix,
            staging = %staging_dir.display(),
            "qed publish: sync skipped (no real publisher wired)"
        );
        Ok(())
    }

    async fn publish_index(
        &self,
        provider: &str,
        bucket: &str,
        update: &IndexUpdate,
    ) -> Result<(), RunnerError> {
        tracing::info!(
            provider,
            bucket,
            key = %update.key,
            version = %update.version,
            "qed publish: index merge skipped (no real publisher wired)"
        );
        Ok(())
    }

    async fn revalidate(&self, report: &StageReport) -> Result<(), RunnerError> {
        tracing::info!(
            manifests = report.manifests.len(),
            "qed publish: revalidate hook skipped (no receiver configured)"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::TempDir;

    /// First file named `name` anywhere under `root`.
    fn find_file(root: &Path, name: &str) -> Option<std::path::PathBuf> {
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).ok()? {
                let path = entry.ok()?.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.file_name().is_some_and(|f| f == name) {
                    return Some(path);
                }
            }
        }
        None
    }

    fn write_dummy(dir: &Path, rel: &str, contents: &[u8]) -> String {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, contents).unwrap();
        p.to_string_lossy().into_owned()
    }

    fn triple_entry(url: &str) -> BTreeMap<String, IndexTriple> {
        one_triple("darwin-aarch64", url)
    }

    /// A triple entry carrying BOTH legacy bare digests and their tagged
    /// spellings — the shape `index_triples_from_manifest` really produces,
    /// since it copies `sha256` straight off the staged channel bundle.
    fn legacy_laden_triple(triple: &str) -> BTreeMap<String, IndexTriple> {
        let mut m = BTreeMap::new();
        m.insert(
            triple.to_string(),
            IndexTriple {
                url: format!("https://cdn.yah.dev/yah/1.0.0/{triple}/yah.tar.gz"),
                platform: Some(triple.to_string()),
                filename: Some("yah.tar.gz".into()),
                size_bytes: Some(42),
                hash: Some(format!("blake3:{}", "a".repeat(64))),
                bootstrap_hash: Some(format!("sha256:{}", "b".repeat(64))),
                sha256: Some("b".repeat(64)),
                bundle_url: Some("https://cdn.yah.dev/x.sigstore.json".into()),
            },
        );
        m
    }

    /// THE LIVE-PRODUCER GUARD for the index's tagged-only invariant (R330-B50).
    ///
    /// `oss/mesofact/crates/almanac/tests/producer_shapes.rs` asserts the same
    /// property, but against `cli-index.json` — a fixture
    /// `scripts/check-producer-fixtures.sh` generates by executing the
    /// `.github/workflows/release.yml` step. GitHub Actions publishes nothing in
    /// this repo, so that guard was green for weeks while THIS producer, the one
    /// that actually writes `cdn.yah.dev/yah/index.json`, emitted 16 bare
    /// `sha256` keys into the live object. This test is here, next to the code,
    /// with no fixture in between, so the next drift cannot hide the same way.
    #[test]
    fn the_index_never_carries_a_legacy_bare_digest() {
        let merged = merge_index(
            None,
            "yah",
            "1.0.0",
            "2026-09-10T00:00:00Z",
            Some("https://cdn.yah.dev/yah/1.0.0/manifest.json".into()),
            legacy_laden_triple("aarch64-apple-darwin"),
        )
        .expect("merge");

        let doc: serde_json::Value = serde_json::from_str(&merged).unwrap();
        for version in doc["versions"].as_array().unwrap() {
            for (triple, entry) in version["triples"].as_object().unwrap() {
                for legacy in ["sha256", "blake3"] {
                    assert!(
                        entry.get(legacy).is_none(),
                        "{triple} carries a bare `{legacy}` — strip it at the index \
                         boundary in merge_index, do not widen LEGACY_BARE_HEX_PATHS"
                    );
                }
                // The tagged spellings must SURVIVE the strip; dropping the
                // digest entirely would be a far worse fix than leaving it bare.
                assert!(
                    entry["hash"].as_str().unwrap().starts_with("blake3:"),
                    "the strip took the tagged hash with it"
                );
                assert!(
                    entry["bootstrap_hash"]
                        .as_str()
                        .unwrap()
                        .starts_with("sha256:"),
                    "the strip took the tagged bootstrap_hash with it"
                );
            }
        }
    }

    /// The invariant holds for HISTORY too, not just for the entry being
    /// published (R330-T52). This is what makes it self-healing: a bare digest
    /// that reached the object by any route — an older build of this producer,
    /// the workflow, a hand-written PUT — is normalized away by the next
    /// release rather than becoming permanent.
    ///
    /// The residue this reverses was real: `cdn.yah.dev/yah/index.json` carried
    /// 16 bare `sha256` keys across four historical versions until 2026-09-10.
    #[test]
    fn merging_strips_a_prior_entrys_legacy_bare_digest_too() {
        let residual = merge_index(
            None,
            "yah",
            "1.0.0",
            "2026-09-10T00:00:00Z",
            None,
            legacy_laden_triple("aarch64-apple-darwin"),
        )
        .expect("seed");
        // Put the residue back by hand — the shape the live object was in.
        let mut doc: serde_json::Value = serde_json::from_str(&residual).unwrap();
        doc["versions"][0]["triples"]["aarch64-apple-darwin"]["sha256"] =
            serde_json::json!("b".repeat(64));
        let dirty = serde_json::to_string_pretty(&doc).unwrap();
        assert_eq!(
            index_legacy_bare_digest_paths(&dirty).unwrap(),
            ["$.versions[1.0.0].triples.aarch64-apple-darwin.sha256"],
            "the fixture must actually be dirty or this test proves nothing"
        );

        // Publishing an UNRELATED version must clean it.
        let merged = merge_index(
            Some(&dirty),
            "yah",
            "1.1.0",
            "2026-09-11T00:00:00Z",
            None,
            legacy_laden_triple("x86_64-apple-darwin"),
        )
        .expect("merge");
        assert!(
            index_legacy_bare_digest_paths(&merged).unwrap().is_empty(),
            "history kept its bare digest through a merge: {merged}"
        );
        // And the historical entry is otherwise untouched — the strip removes,
        // it does not rewrite.
        let after: serde_json::Value = serde_json::from_str(&merged).unwrap();
        let old = after["versions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["version"] == "1.0.0")
            .expect("1.0.0 was DROPPED");
        let entry = &old["triples"]["aarch64-apple-darwin"];
        assert_eq!(old["pub_date"], "2026-09-10T00:00:00Z");
        assert_eq!(entry["bootstrap_hash"], format!("sha256:{}", "b".repeat(64)));
        assert_eq!(entry["hash"], format!("blake3:{}", "a".repeat(64)));
        assert_eq!(entry["size_bytes"], 42);
    }

    /// `normalize_index` is a REMOVAL and nothing else: same versions, same
    /// order, same `updated_at`, minus the keys the invariant forbids. That is
    /// what makes the repair command's blast radius bounded — see
    /// `yah qed normalize-index`.
    #[test]
    fn normalize_index_removes_the_bare_digests_and_touches_nothing_else() {
        let seeded = merge_index(
            None,
            "yah",
            "1.0.0",
            "2026-09-10T00:00:00Z",
            Some("https://cdn.yah.dev/yah/1.0.0/manifest.json".into()),
            legacy_laden_triple("aarch64-apple-darwin"),
        )
        .expect("seed");
        let mut doc: serde_json::Value = serde_json::from_str(&seeded).unwrap();
        doc["updated_at"] = serde_json::json!("2020-01-01T00:00:00+00:00");
        doc["versions"][0]["triples"]["aarch64-apple-darwin"]["sha256"] =
            serde_json::json!("b".repeat(64));
        let dirty = serde_json::to_string_pretty(&doc).unwrap();

        let clean = normalize_index(&dirty).expect("normalize");
        assert!(index_legacy_bare_digest_paths(&clean).unwrap().is_empty());

        // The ONLY difference: delete the forbidden keys from the input and the
        // two documents must be equal. This is the property the repair command
        // re-checks against the live bytes before it will write.
        let mut expected: serde_json::Value = serde_json::from_str(&dirty).unwrap();
        expected["versions"][0]["triples"]["aarch64-apple-darwin"]
            .as_object_mut()
            .unwrap()
            .remove("sha256");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&clean).unwrap(),
            expected,
            "normalize changed something other than the forbidden keys"
        );
        // Specifically: it did not restamp the object.
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&clean).unwrap()["updated_at"],
            "2020-01-01T00:00:00+00:00",
            "normalize restamped updated_at — it is a repair, not a publish"
        );

        // Byte-idempotent, which is what lets the caller skip a second write.
        assert_eq!(normalize_index(&clean).expect("re-normalize"), clean);
    }

    /// The probe must read the RAW bytes. `ReleaseIndex` has no `blake3` field,
    /// so a typed round-trip drops such a key silently and would report clean on
    /// an object that is not.
    #[test]
    fn the_bare_digest_probe_sees_keys_the_typed_shape_cannot() {
        let seeded = merge_index(
            None,
            "yah",
            "1.0.0",
            "2026-09-10T00:00:00Z",
            None,
            legacy_laden_triple("aarch64-apple-darwin"),
        )
        .expect("seed");
        let mut doc: serde_json::Value = serde_json::from_str(&seeded).unwrap();
        doc["versions"][0]["triples"]["aarch64-apple-darwin"]["blake3"] =
            serde_json::json!("a".repeat(64));
        let raw = serde_json::to_string_pretty(&doc).unwrap();

        assert_eq!(
            index_legacy_bare_digest_paths(&raw).unwrap(),
            ["$.versions[1.0.0].triples.aarch64-apple-darwin.blake3"]
        );
        assert!(
            index_legacy_bare_digest_paths(&normalize_index(&raw).unwrap())
                .unwrap()
                .is_empty(),
            "the typed round-trip must drop it, and the probe must agree it is gone"
        );
    }

    /// The other half of the asymmetry: the INSTALL POINTER keeps the bare
    /// field. `install.sh` copies already in the wild read `.triples[t].sha256`
    /// as their fallback when `bootstrap_hash` is absent, so stripping it there
    /// breaks installs on machines we cannot reach. If this test and the one
    /// above ever agree, one of the two shapes has been broken.
    #[test]
    fn the_install_pointer_keeps_the_legacy_bare_sha256() {
        let manifest: ChannelManifest = serde_json::from_str(
            r#"{
              "version": "1.0.0",
              "pub_date": "2026-09-10T00:00:00Z",
              "host": { "bundle": { "aarch64-apple-darwin": {
                "url": "https://cdn.yah.dev/yah/1.0.0/aarch64-apple-darwin/yah.tar.gz",
                "hash": "blake3:aaaa",
                "bootstrap_hash": "sha256:bbbb",
                "sha256": "bbbb",
                "bundle_url": "https://cdn.yah.dev/x.sigstore.json",
                "size": 42
              } } }
            }"#,
        )
        .expect("parse channel manifest");

        let pointer = triples_manifest("yah", &manifest).expect("pointer projects");
        let entry = pointer.triples.get("aarch64-apple-darwin").unwrap();
        assert_eq!(
            entry.sha256.as_deref(),
            Some("bbbb"),
            "the install pointer lost its legacy bare sha256 — install.sh in the wild reads it"
        );
    }

    /// One platform leg's contribution — what a matrix job actually stages.
    fn one_triple(triple: &str, url: &str) -> BTreeMap<String, IndexTriple> {
        let mut m = BTreeMap::new();
        m.insert(
            triple.to_string(),
            IndexTriple {
                url: url.to_string(),
                platform: Some(triple.to_string()),
                filename: Some("yah.tar.gz".into()),
                size_bytes: Some(42),
                hash: Some("blake3:aa".into()),
                // Unset: these fixtures cover index MERGE semantics, which are
                // indifferent to which optional fields an entry carries.
                bootstrap_hash: None,
                sha256: None,
                bundle_url: None,
            },
        );
        m
    }

    fn triples_of(json: &str, version: &str) -> Vec<String> {
        let idx: ReleaseIndex = serde_json::from_str(json).unwrap();
        idx.versions
            .into_iter()
            .find(|v| v.version == version)
            .unwrap_or_else(|| panic!("version {version} missing from index"))
            .triples
            .into_keys()
            .collect()
    }

    fn versions_of(json: &str) -> Vec<(String, String)> {
        serde_json::from_str::<ReleaseIndex>(json)
            .unwrap()
            .versions
            .into_iter()
            .map(|v| (v.version, v.pub_date))
            .collect()
    }

    #[test]
    fn a_missing_index_is_the_create_case_not_an_error() {
        let out = merge_index(
            None,
            "yah",
            "v0.8.21",
            "2026-07-31T00:00:00Z",
            None,
            triple_entry("https://cdn.yah.dev/yah/0.8.21/darwin-aarch64/yah.tar.gz"),
        )
        .unwrap();
        // The leading `v` is stripped: the page and the install pointer both
        // key on the bare version.
        assert_eq!(
            versions_of(&out),
            vec![("0.8.21".to_string(), "2026-07-31T00:00:00Z".to_string())]
        );
    }

    #[test]
    fn republishing_a_version_replaces_its_entry_rather_than_duplicating_it() {
        let first = merge_index(
            None,
            "yah",
            "0.8.21",
            "2026-07-31T00:00:00Z",
            None,
            triple_entry("https://cdn.yah.dev/a"),
        )
        .unwrap();
        let second = merge_index(
            Some(&first),
            "yah",
            "0.8.21",
            "2026-08-02T00:00:00Z",
            None,
            triple_entry("https://cdn.yah.dev/b"),
        )
        .unwrap();

        let v = versions_of(&second);
        assert_eq!(v.len(), 1, "a re-publish must not duplicate the version");
        // The RE-PUBLISHED payload wins...
        let idx: ReleaseIndex = serde_json::from_str(&second).unwrap();
        assert_eq!(idx.versions[0].triples["darwin-aarch64"].url, "https://cdn.yah.dev/b");
        // ...but the ORIGINAL publication date survives. Restamping it would
        // reorder published history on every consumer, which all sort by date.
        assert_eq!(
            v[0].1, "2026-07-31T00:00:00Z",
            "a re-publish must not restamp pub_date"
        );
    }

    #[test]
    fn the_index_accumulates_and_stays_newest_first() {
        let a = merge_index(None, "yah", "0.8.21", "2026-07-01T00:00:00Z", None, triple_entry("u")).unwrap();
        let b = merge_index(Some(&a), "yah", "0.8.22", "2026-07-20T00:00:00Z", None, triple_entry("u")).unwrap();
        // Appended out of order — the sort, not the caller, decides position.
        let c = merge_index(Some(&b), "yah", "0.8.20", "2026-06-01T00:00:00Z", None, triple_entry("u")).unwrap();

        let got: Vec<String> = versions_of(&c).into_iter().map(|(v, _)| v).collect();
        assert_eq!(got, vec!["0.8.22", "0.8.21", "0.8.20"]);
    }

    /// The regression pin for R330-T35's sliced-release case: a release matrix
    /// publishes one platform per leg, and each leg's update carries only its
    /// own triples. Before this, the last leg to finish took the version's whole
    /// triples map and every earlier platform vanished.
    #[test]
    fn a_release_published_one_platform_at_a_time_accumulates_every_triple() {
        let mac = merge_index(
            None,
            "yah",
            "0.8.22",
            "2026-08-12T00:00:00Z",
            None,
            one_triple("aarch64-apple-darwin", "https://cdn.yah.dev/mac"),
        )
        .unwrap();
        let linux = merge_index(
            Some(&mac),
            "yah",
            "0.8.22",
            "2026-08-12T00:00:01Z",
            None,
            one_triple("x86_64-unknown-linux-musl", "https://cdn.yah.dev/linux"),
        )
        .unwrap();
        let win = merge_index(
            Some(&linux),
            "yah",
            "0.8.22",
            "2026-08-12T00:00:02Z",
            None,
            one_triple("x86_64-pc-windows-msvc", "https://cdn.yah.dev/win"),
        )
        .unwrap();

        assert_eq!(
            triples_of(&win, "0.8.22"),
            vec![
                "aarch64-apple-darwin".to_string(),
                "x86_64-pc-windows-msvc".to_string(),
                "x86_64-unknown-linux-musl".to_string(),
            ],
            "a later platform leg must not drop the triples earlier legs published"
        );
        assert_eq!(
            versions_of(&win).len(),
            1,
            "the slices are one version, not three"
        );
    }

    /// Slicing must not weaken invariant 1: a leg re-publishing a triple it
    /// already wrote still updates that entry in place.
    #[test]
    fn re_publishing_one_slice_updates_that_triple_and_leaves_its_siblings() {
        let mac = merge_index(
            None,
            "yah",
            "0.8.22",
            "2026-08-12T00:00:00Z",
            None,
            one_triple("aarch64-apple-darwin", "https://cdn.yah.dev/mac-v1"),
        )
        .unwrap();
        let linux = merge_index(
            Some(&mac),
            "yah",
            "0.8.22",
            "2026-08-12T00:00:01Z",
            None,
            one_triple("x86_64-unknown-linux-musl", "https://cdn.yah.dev/linux"),
        )
        .unwrap();
        // The mac leg failed its upload and retried.
        let retried = merge_index(
            Some(&linux),
            "yah",
            "0.8.22",
            "2026-08-12T00:00:02Z",
            None,
            one_triple("aarch64-apple-darwin", "https://cdn.yah.dev/mac-v2"),
        )
        .unwrap();

        let idx: ReleaseIndex = serde_json::from_str(&retried).unwrap();
        let v = &idx.versions[0];
        assert_eq!(v.triples["aarch64-apple-darwin"].url, "https://cdn.yah.dev/mac-v2");
        assert_eq!(v.triples["x86_64-unknown-linux-musl"].url, "https://cdn.yah.dev/linux");
        assert_eq!(
            v.pub_date, "2026-08-12T00:00:00Z",
            "a retried slice must not restamp the version"
        );
    }

    /// Two versions releasing in the same window, their legs interleaved — the
    /// shape `cas_merge_index`'s retry loop produces when concurrent publishers
    /// serialise onto one object. Each merge sees the previous winner's bytes.
    #[test]
    fn interleaved_legs_of_two_versions_all_survive() {
        let a1 = merge_index(None, "yah", "0.8.21", "2026-08-01T00:00:00Z", None,
            one_triple("aarch64-apple-darwin", "https://cdn.yah.dev/21-mac")).unwrap();
        let b1 = merge_index(Some(&a1), "yah", "0.8.22", "2026-08-12T00:00:00Z", None,
            one_triple("aarch64-apple-darwin", "https://cdn.yah.dev/22-mac")).unwrap();
        let a2 = merge_index(Some(&b1), "yah", "0.8.21", "2026-08-01T00:00:05Z", None,
            one_triple("x86_64-unknown-linux-musl", "https://cdn.yah.dev/21-linux")).unwrap();
        let b2 = merge_index(Some(&a2), "yah", "0.8.22", "2026-08-12T00:00:05Z", None,
            one_triple("x86_64-unknown-linux-musl", "https://cdn.yah.dev/22-linux")).unwrap();

        let got: Vec<String> = versions_of(&b2).into_iter().map(|(v, _)| v).collect();
        assert_eq!(got, vec!["0.8.22", "0.8.21"]);
        assert_eq!(triples_of(&b2, "0.8.21").len(), 2);
        assert_eq!(triples_of(&b2, "0.8.22").len(), 2);
    }

    /// A leg with no `base_url` derives no `manifest_url`; it must not erase the
    /// one a sibling leg derived. Same replace-or-append rule, one field over.
    #[test]
    fn a_slice_without_a_manifest_url_keeps_the_one_already_recorded() {
        let with_url = merge_index(
            None,
            "yah",
            "0.8.22",
            "2026-08-12T00:00:00Z",
            Some("https://cdn.yah.dev/yah/0.8.22/manifest.json".into()),
            one_triple("aarch64-apple-darwin", "https://cdn.yah.dev/mac"),
        )
        .unwrap();
        let without = merge_index(
            Some(&with_url),
            "yah",
            "0.8.22",
            "2026-08-12T00:00:01Z",
            None,
            one_triple("x86_64-unknown-linux-musl", "https://cdn.yah.dev/linux"),
        )
        .unwrap();

        let idx: ReleaseIndex = serde_json::from_str(&without).unwrap();
        assert_eq!(
            idx.versions[0].manifest_url.as_deref(),
            Some("https://cdn.yah.dev/yah/0.8.22/manifest.json")
        );
    }

    #[test]
    fn every_download_in_the_index_carries_a_tagged_hash() {
        let tmp = TempDir::new().unwrap();
        let src = write_dummy(tmp.path(), "src/yah", b"binary bytes");
        let staging = tmp.path().join("stage");
        let report = stage_release(
            &staging,
            &[ProducedArtifact {
                binary: "yah".into(),
                path: src,
                triple: Some("darwin-aarch64".into()),
            }],
            "0.8.21",
            None,
            Some("https://cdn.yah.dev"),
        )
        .unwrap();

        let triples = index_triples_from_manifest(&report.manifests["yah"]);
        let entry = &triples["darwin-aarch64"];
        let hash = entry.hash.as_deref().expect("staged artifact has no hash");
        assert!(
            hash.starts_with("blake3:") && hash.len() == "blake3:".len() + 64,
            "hash must be a tagged blake3 digest, got {hash:?}"
        );
        assert_eq!(entry.filename.as_deref(), Some("yah"));
        assert_eq!(entry.size_bytes, Some(b"binary bytes".len() as u64));
    }

    #[test]
    fn resolve_triple_uses_host_when_none() {
        let t = resolve_triple(None);
        assert!(t.contains('-'), "host triple shorthand has os-arch: {t}");
        assert_eq!(resolve_triple(Some("linux-x86_64")), "linux-x86_64");
        // Empty string falls back to host too.
        assert_eq!(resolve_triple(Some("")), resolve_triple(None));
    }

    #[test]
    fn platform_token_maps_both_triple_spellings() {
        // Full Rust triples (what a cross-build declares).
        assert_eq!(platform_token("aarch64-apple-darwin"), "macos-arm64");
        assert_eq!(platform_token("x86_64-apple-darwin"), "macos-x86_64");
        assert_eq!(platform_token("x86_64-unknown-linux-gnu"), "linux-x86_64");
        assert_eq!(platform_token("aarch64-unknown-linux-gnu"), "linux-arm64");
        assert_eq!(platform_token("x86_64-pc-windows-msvc"), "windows-x86_64");
        // The `<os>-<arch>` shorthand `resolve_triple` synthesises for a
        // host-native build — the CLI-only release recipe's spelling.
        assert_eq!(platform_token("darwin-aarch64"), "macos-arm64");
        assert_eq!(platform_token("darwin-x86_64"), "macos-x86_64");
        assert_eq!(platform_token("linux-x86_64"), "linux-x86_64");
        assert_eq!(platform_token("linux-aarch64"), "linux-arm64");
        // Whatever the host actually is, it must map to something the page has
        // a label for — this is the token a local `cli-release` publishes.
        assert!(
            [
                "macos-arm64",
                "macos-x86_64",
                "linux-x86_64",
                "linux-arm64",
                "windows-x86_64",
            ]
            .contains(&platform_token(&resolve_triple(None)).as_str()),
            "host {} has no /releases label",
            resolve_triple(None)
        );
    }

    #[test]
    fn platform_token_keeps_musl_and_gnu_apart() {
        // The whole reason the producer supplies `platform` rather than letting
        // almanac's fallback derive it: that mapper matches on the
        // `x86_64-unknown-linux` prefix and collapses these two, listing a musl
        // and a gnu binary as the same download.
        assert_eq!(
            platform_token("x86_64-unknown-linux-musl"),
            "linux-x86_64-musl"
        );
        assert_eq!(
            platform_token("aarch64-unknown-linux-musl"),
            "linux-aarch64-musl"
        );
        assert_ne!(
            platform_token("x86_64-unknown-linux-musl"),
            platform_token("x86_64-unknown-linux-gnu")
        );
    }

    #[test]
    fn platform_token_passes_unknown_triples_through() {
        // Better a visibly-raw token on the page (a prompt to add a mapping)
        // than a confident wrong label.
        assert_eq!(
            platform_token("riscv64gc-unknown-none"),
            "riscv64gc-unknown-none"
        );
    }

    #[test]
    fn index_triples_carry_a_page_label_not_a_raw_triple() {
        let manifest = ChannelManifest {
            version: "0.8.21".into(),
            pub_date: "2026-08-01T00:00:00Z".into(),
            notes: None,
            host: ChannelHost {
                bundle: BTreeMap::from([(
                    "aarch64-apple-darwin".to_string(),
                    ChannelBundle {
                        url: "https://cdn.yah.dev/yah/0.8.21/aarch64-apple-darwin/yah.tar.gz"
                            .into(),
                        size: Some(4),
                        hash: Some("blake3:ff".into()),
                        bootstrap_hash: None,
                        sha256: None,
                        bundle_url: None,
                    },
                )]),
            },
        };
        let triples = index_triples_from_manifest(&manifest);
        // Keyed by triple, but `platform` is the token PLATFORM_LABELS keys on.
        // Emitting the triple here rendered "aarch64-apple-darwin" as the
        // download's visible label on /releases.
        assert_eq!(
            triples["aarch64-apple-darwin"].platform.as_deref(),
            Some("macos-arm64")
        );
        assert_eq!(
            triples["aarch64-apple-darwin"].filename.as_deref(),
            Some("yah.tar.gz")
        );
    }

    /// One triple, with whichever install-pointer fields the caller wants set.
    fn signed_manifest(bundle_url: Option<&str>, bootstrap: Option<&str>) -> ChannelManifest {
        ChannelManifest {
            version: "0.8.29".into(),
            pub_date: "2026-09-01T00:00:00Z".into(),
            notes: None,
            host: ChannelHost {
                bundle: BTreeMap::from([(
                    "aarch64-apple-darwin".to_string(),
                    ChannelBundle {
                        url: "https://cdn.yah.dev/yah/0.8.29/aarch64-apple-darwin/yah.tar.gz"
                            .into(),
                        size: Some(4),
                        hash: Some("blake3:ff".into()),
                        bootstrap_hash: bootstrap.map(str::to_string),
                        sha256: bootstrap.map(|b| b.trim_start_matches("sha256:").to_string()),
                        bundle_url: bundle_url.map(str::to_string),
                    },
                )]),
            },
        }
    }

    #[test]
    fn an_unsigned_manifest_yields_no_install_pointer() {
        // The refusal that matters. Staging without signing is legal — a
        // publisher with no key still produces a channel tree and an index
        // entry — but `latest.json` is the object `curl … | sh` trusts, and one
        // written without a `bundle_url` makes install.sh die with "cosign is
        // installed but the manifest carries no signature bundle". Refusing to
        // emit the pointer leaves the PREVIOUS release installable, which is
        // strictly better than pointing at bytes nobody can verify.
        assert!(triples_manifest("yah", &signed_manifest(None, Some("sha256:ab"))).is_none());
    }

    #[test]
    fn a_manifest_without_a_bootstrap_digest_yields_no_install_pointer() {
        // blake3 alone is not enough: install.sh cannot verify it without first
        // downloading an unverified b3sum, so a pointer carrying only `hash`
        // silently degrades every install to no integrity check at all.
        assert!(triples_manifest("yah", &signed_manifest(Some("https://x/y.sigstore.json"), None))
            .is_none());
    }

    #[test]
    fn a_signed_manifest_projects_onto_the_install_pointer_shape() {
        let m = signed_manifest(
            Some("https://cdn.yah.dev/yah/0.8.29/aarch64-apple-darwin/yah.tar.gz.sigstore.json"),
            Some("sha256:ab"),
        );
        let pointer = triples_manifest("yah", &m).expect("fully signed manifest yields a pointer");
        // `name` is the install.sh `<pkg>` arg, not the triple or the binary path.
        assert_eq!(pointer.name, "yah");
        assert_eq!(pointer.version, "0.8.29");
        // FLAT `triples[<triple>]`, never `host.bundle[...]` — install.sh reads
        // `.triples[$t][$f]` and that layout is a published contract.
        let entry = &pointer.triples["aarch64-apple-darwin"];
        assert_eq!(entry.bootstrap_hash.as_deref(), Some("sha256:ab"));
        assert_eq!(entry.sha256.as_deref(), Some("ab"));
        assert!(entry.bundle_url.as_deref().unwrap().ends_with(".sigstore.json"));
        // Serialized shape: the pointer must not grow a `host` key, or almanac's
        // r2-triples reader and install.sh both stop finding the triples.
        let json = serde_json::to_value(&pointer).unwrap();
        assert!(json.get("host").is_none());
        assert!(json["triples"]["aarch64-apple-darwin"]["bundle_url"].is_string());
    }

    #[test]
    fn an_empty_manifest_yields_no_install_pointer() {
        let mut m = signed_manifest(Some("https://x/y.sigstore.json"), Some("sha256:ab"));
        m.host.bundle.clear();
        assert!(triples_manifest("yah", &m).is_none());
    }

    #[test]
    fn staging_computes_both_digests_over_the_staged_bytes() {
        let src = TempDir::new().unwrap();
        let bin = write_dummy(src.path(), "target/release/yah", b"YAH-BINARY");
        let staging = TempDir::new().unwrap();
        let report = stage_release(
            staging.path(),
            &[ProducedArtifact {
                binary: "yah".into(),
                path: bin,
                triple: Some("aarch64-apple-darwin".into()),
            }],
            "0.8.29",
            None,
            Some("https://cdn.yah.dev"),
        )
        .unwrap();
        let bundle = &report.manifests["yah"].host.bundle["aarch64-apple-darwin"];
        // Literal digests of b"YAH-BINARY", from `sha256sum` / `b3sum` — NOT
        // recomputed here with the same code under test, which would only prove
        // the function agrees with itself. These are the exact tools
        // publish-yubaba-release.sh uses, so agreeing with them is the property
        // that matters: the two producers must hash a release identically.
        assert_eq!(
            bundle.sha256.as_deref(),
            Some("1c47e2c01f3a57fcb22b5cfe8bb6cfa586f27ced4085fd4a0524d43076f278de")
        );
        assert_eq!(
            bundle.hash.as_deref(),
            Some("blake3:69ec0019d64fea0158bdb120a417e72f15b9823f355aa1521b9e55f39d5dc497")
        );
        // Tagged and bare are the SAME digest — two spellings for two readers,
        // not two values.
        assert_eq!(
            bundle.bootstrap_hash.as_deref(),
            Some("sha256:1c47e2c01f3a57fcb22b5cfe8bb6cfa586f27ced4085fd4a0524d43076f278de")
        );
        // Staging never signs, so a freshly staged manifest cannot become a
        // pointer. This pairing is what keeps the refusal honest rather than
        // theoretical.
        assert!(bundle.bundle_url.is_none());
        assert!(triples_manifest("yah", &report.manifests["yah"]).is_none());
    }

    #[test]
    fn stage_release_lays_out_channel_and_manifest() {
        let src = TempDir::new().unwrap();
        let yah_bin = write_dummy(src.path(), "target/release/yah", b"YAH-BINARY");

        let staging = TempDir::new().unwrap();
        let artifacts = vec![ProducedArtifact {
            binary: "yah".into(),
            path: yah_bin,
            triple: Some("darwin-aarch64".into()),
        }];

        let report = stage_release(
            staging.path(),
            &artifacts,
            "v0.8.6",
            None,
            Some("https://releases.yah.dev"),
        )
        .unwrap();

        // Artifact landed at <binary>/<version>/<triple>/<filename> (v stripped).
        assert_eq!(report.object_keys, vec!["yah/0.8.6/darwin-aarch64/yah"]);
        let copied = staging.path().join("yah/0.8.6/darwin-aarch64/yah");
        assert_eq!(std::fs::read(&copied).unwrap(), b"YAH-BINARY");

        // Three manifest keys (report sorts them): the IMMUTABLE per-version
        // copy the index links to, the per-triple stable key (R330-B8), and the
        // mutable pointer almanac re-fetches on push.
        assert_eq!(
            report.manifest_keys,
            vec![
                "yah/0.8.6/manifest.json",
                "yah/release-manifest-darwin-aarch64.json",
                "yah/release-manifest.json",
            ]
        );
        let manifest = &report.manifests["yah"];
        assert_eq!(manifest.version, "0.8.6");
        let bundle = &manifest.host.bundle["darwin-aarch64"];
        assert_eq!(
            bundle.url,
            "https://releases.yah.dev/yah/0.8.6/darwin-aarch64/yah"
        );
        assert_eq!(bundle.size, Some("YAH-BINARY".len() as u64));

        // The on-disk manifest round-trips through the same wire type, and the
        // per-version copy is byte-identical to the pointer at publish time —
        // it just stops changing afterwards.
        let bytes = std::fs::read(staging.path().join("yah/release-manifest.json")).unwrap();
        let parsed: ChannelManifest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(&parsed, manifest);
        let versioned = std::fs::read(staging.path().join("yah/0.8.6/manifest.json")).unwrap();
        assert_eq!(versioned, bytes);
    }

    #[test]
    fn stage_release_relative_urls_without_base() {
        let src = TempDir::new().unwrap();
        let bin = write_dummy(src.path(), "out/desktop", b"x");
        let staging = TempDir::new().unwrap();
        let report = stage_release(
            staging.path(),
            &[ProducedArtifact {
                binary: "desktop".into(),
                path: bin,
                triple: Some("linux-x86_64".into()),
            }],
            "0.9.0",
            Some("channels"),
            None,
        )
        .unwrap();
        // Prefix is applied to both the object and the manifest keys.
        assert_eq!(
            report.object_keys,
            vec!["channels/desktop/0.9.0/linux-x86_64/desktop"]
        );
        assert_eq!(
            report.manifest_keys,
            vec![
                "channels/desktop/0.9.0/manifest.json",
                "channels/desktop/release-manifest-linux-x86_64.json",
                "channels/desktop/release-manifest.json",
            ]
        );
        // No base_url → manifest url is the bucket-relative key.
        assert_eq!(
            report.manifests["desktop"].host.bundle["linux-x86_64"].url,
            "channels/desktop/0.9.0/linux-x86_64/desktop"
        );
    }

    #[test]
    fn stage_release_groups_multiple_binaries() {
        let src = TempDir::new().unwrap();
        let yah = write_dummy(src.path(), "target/release/yah", b"a");
        let desktop = write_dummy(src.path(), "target/release/desktop", b"bb");
        let staging = TempDir::new().unwrap();
        let report = stage_release(
            staging.path(),
            &[
                ProducedArtifact {
                    binary: "yah".into(),
                    path: yah,
                    triple: Some("darwin-aarch64".into()),
                },
                ProducedArtifact {
                    binary: "desktop".into(),
                    path: desktop,
                    triple: Some("darwin-aarch64".into()),
                },
            ],
            "1.0.0",
            None,
            None,
        )
        .unwrap();
        // One shared manifest per binary, plus one per-triple stable manifest
        // per (binary, triple).
        assert_eq!(report.manifests.len(), 2);
        assert!(report
            .manifest_keys
            .contains(&"yah/release-manifest.json".to_string()));
        assert!(report
            .manifest_keys
            .contains(&"desktop/release-manifest.json".to_string()));
        assert!(report
            .manifest_keys
            .contains(&"yah/release-manifest-darwin-aarch64.json".to_string()));
        assert!(report
            .manifest_keys
            .contains(&"desktop/release-manifest-darwin-aarch64.json".to_string()));
    }

    /// R330-B8: simulate two sequential single-triple publishes (darwin then
    /// linux) and assert that the per-triple stable keys provide a non-
    /// clobbering record of both triples. The shared release-manifest.json
    /// would be overwritten by each stage (single-stage view), but the
    /// per-triple `release-manifest-<triple>.json` files coexist — the input
    /// the downstream merger reads.
    #[test]
    fn stage_release_per_triple_keys_survive_sequential_publishes() {
        let src = TempDir::new().unwrap();
        let yah_darwin = write_dummy(src.path(), "target/release/yah-darwin", b"D");
        let yah_linux = write_dummy(src.path(), "target/release/yah-linux", b"LL");

        // Simulate two sequential single-triple publishes into the SAME R2
        // bucket layout by staging both into a shared staging root.
        let staging = TempDir::new().unwrap();

        let report1 = stage_release(
            staging.path(),
            &[ProducedArtifact {
                binary: "yah".into(),
                path: yah_darwin,
                triple: Some("darwin-aarch64".into()),
            }],
            "0.8.6",
            None,
            Some("https://releases.yah.dev"),
        )
        .unwrap();
        let report2 = stage_release(
            staging.path(),
            &[ProducedArtifact {
                binary: "yah".into(),
                path: yah_linux,
                triple: Some("linux-x86_64".into()),
            }],
            "0.8.6",
            None,
            Some("https://releases.yah.dev"),
        )
        .unwrap();

        // Each stage emits its own per-triple key (idempotent, non-colliding).
        assert!(report1
            .manifest_keys
            .iter()
            .any(|k| k == "yah/release-manifest-darwin-aarch64.json"));
        assert!(report2
            .manifest_keys
            .iter()
            .any(|k| k == "yah/release-manifest-linux-x86_64.json"));

        // Both per-triple manifests survive on disk after the second stage
        // (the bug was: shared key clobbered, no record of the first triple).
        let darwin_path = staging
            .path()
            .join("yah/release-manifest-darwin-aarch64.json");
        let linux_path = staging
            .path()
            .join("yah/release-manifest-linux-x86_64.json");
        assert!(
            darwin_path.exists(),
            "darwin per-triple manifest must persist"
        );
        assert!(
            linux_path.exists(),
            "linux per-triple manifest must persist"
        );

        let darwin: ChannelManifest =
            serde_json::from_slice(&std::fs::read(&darwin_path).unwrap()).unwrap();
        let linux: ChannelManifest =
            serde_json::from_slice(&std::fs::read(&linux_path).unwrap()).unwrap();
        assert!(darwin.host.bundle.contains_key("darwin-aarch64"));
        assert_eq!(
            darwin.host.bundle.len(),
            1,
            "per-triple manifest is single-triple"
        );
        assert!(linux.host.bundle.contains_key("linux-x86_64"));
        assert_eq!(
            linux.host.bundle.len(),
            1,
            "per-triple manifest is single-triple"
        );

        // The shared release-manifest.json reflects the LAST stage (best-effort
        // latest single-stage view) — the GHA assembler is what unifies it.
        let shared_path = staging.path().join("yah/release-manifest.json");
        let shared: ChannelManifest =
            serde_json::from_slice(&std::fs::read(&shared_path).unwrap()).unwrap();
        assert!(
            shared.host.bundle.contains_key("linux-x86_64"),
            "shared manifest reflects the most recent stage"
        );
    }

    /// `YAH_RELEASE_VERSION` is process-global env; serialize every test that
    /// mutates it (here and in `runner.rs`'s own copy of this lock) so they
    /// don't race under cargo's default parallel test threads (R876-F6).
    static RELEASE_VERSION_ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn resolve_release_version_prefers_env() {
        let _guard = RELEASE_VERSION_ENV_LOCK.lock().unwrap();
        std::env::set_var("YAH_RELEASE_VERSION", "9.9.9");
        assert_eq!(resolve_release_version(), "9.9.9");
        std::env::remove_var("YAH_RELEASE_VERSION");
        // Falls back to the compiled crate version (non-empty).
        assert!(!resolve_release_version().is_empty());
    }

    /// R876-F6 — the invariant the whole ticket exists to land: with no
    /// explicit `YAH_RELEASE_VERSION`, the explicit resolver returns `None`,
    /// never a plausible-looking fallback number. This is what
    /// `Outcome::Publish::require_explicit_version` checks to decide whether
    /// to skip a publish.
    #[test]
    fn resolve_release_version_explicit_is_none_when_unset() {
        let _guard = RELEASE_VERSION_ENV_LOCK.lock().unwrap();
        std::env::remove_var("YAH_RELEASE_VERSION");
        assert_eq!(resolve_release_version_explicit(), None);

        // An explicitly-empty override is the same as unset — filtered out,
        // not treated as "the empty string is the release version".
        std::env::set_var("YAH_RELEASE_VERSION", "");
        assert_eq!(resolve_release_version_explicit(), None);
        std::env::remove_var("YAH_RELEASE_VERSION");
    }

    #[test]
    fn resolve_release_version_explicit_is_some_when_set() {
        let _guard = RELEASE_VERSION_ENV_LOCK.lock().unwrap();
        std::env::set_var("YAH_RELEASE_VERSION", "3.4.5");
        assert_eq!(
            resolve_release_version_explicit(),
            Some("3.4.5".to_string())
        );
        std::env::remove_var("YAH_RELEASE_VERSION");
    }

    // ── PublishingOutcomeDispatcher with a recording publisher ──────────────

    #[derive(Default)]
    struct RecordingPublisher {
        synced: Mutex<Vec<String>>,
        revalidated: Mutex<u32>,
        /// Manifest contents captured from the staging dir at sync time.
        captured_manifests: Mutex<Vec<String>>,
        /// Binary names the revalidate hook was handed manifests for — the
        /// payload a real hook maps into the poke's `data_inputs`.
        revalidate_saw: Mutex<Vec<String>>,
        /// Make `revalidate` report a failure (still counting the attempt) —
        /// stands in for an unreachable / 5xx receiver.
        fail_revalidate: bool,
        /// Stand-in bucket for index objects: key → current bytes. Persisting
        /// them across calls is the point — accumulation is what the index is
        /// for, and a fake that forgets can't catch a clobbering merge.
        index_objects: Mutex<BTreeMap<String, String>>,
    }

    #[async_trait]
    impl ReleasePublisher for RecordingPublisher {
        async fn sync(
            &self,
            staging_dir: &Path,
            _provider: &str,
            bucket: &str,
            _prefix: Option<&str>,
        ) -> Result<(), RunnerError> {
            // Confirm the staged tree actually exists at sync time (the
            // tempdir must outlive this call). Located by walk rather than a
            // fixed path so a prefixed request (`dl/yah/...`) works too.
            let manifest = find_file(staging_dir, MANIFEST_FILENAME)
                .expect("staged tree carries a shared manifest at sync time");
            let body = std::fs::read_to_string(&manifest).unwrap();
            self.captured_manifests.lock().unwrap().push(body);
            self.synced.lock().unwrap().push(bucket.to_string());
            Ok(())
        }

        async fn publish_index(
            &self,
            _provider: &str,
            _bucket: &str,
            update: &IndexUpdate,
        ) -> Result<(), RunnerError> {
            let mut objects = self.index_objects.lock().unwrap();
            let merged = update
                .merge(objects.get(&update.key).map(String::as_str))
                .map_err(|e| RunnerError::Remote(format!("merge index: {e}")))?;
            objects.insert(update.key.clone(), merged);
            Ok(())
        }

        async fn revalidate(&self, report: &StageReport) -> Result<(), RunnerError> {
            *self.revalidated.lock().unwrap() += 1;
            self.revalidate_saw
                .lock()
                .unwrap()
                .extend(report.manifests.keys().cloned());
            if self.fail_revalidate {
                return Err(RunnerError::Remote(
                    "POST https://yah.dev/revalidate: connection refused".into(),
                ));
            }
            Ok(())
        }
    }

    /// The dispatcher takes its publisher by value; this forwarder lets a test
    /// keep a handle for assertions.
    struct ArcPublisher(std::sync::Arc<RecordingPublisher>);

    #[async_trait]
    impl ReleasePublisher for ArcPublisher {
        async fn sync(
            &self,
            d: &Path,
            p: &str,
            b: &str,
            pre: Option<&str>,
        ) -> Result<(), RunnerError> {
            self.0.sync(d, p, b, pre).await
        }
        async fn publish_index(
            &self,
            p: &str,
            b: &str,
            u: &IndexUpdate,
        ) -> Result<(), RunnerError> {
            self.0.publish_index(p, b, u).await
        }
        async fn revalidate(&self, r: &StageReport) -> Result<(), RunnerError> {
            self.0.revalidate(r).await
        }
    }

    #[tokio::test]
    async fn dispatcher_stages_uploads_and_revalidates() {
        use std::sync::Arc;
        let src = TempDir::new().unwrap();
        let bin = write_dummy(src.path(), "target/release/yah", b"BIN");
        let publisher = Arc::new(RecordingPublisher::default());
        let dispatcher = PublishingOutcomeDispatcher::new(ArcPublisher(publisher.clone()));
        let req = PublishRequest {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: Some("https://releases.yah.dev".into()),
            version: "0.8.6".into(),
            artifacts: vec![ProducedArtifact {
                binary: "yah".into(),
                path: bin,
                triple: Some("darwin-aarch64".into()),
            }],
        };
        dispatcher.publish(&req).await.unwrap();

        assert_eq!(
            publisher.synced.lock().unwrap().as_slice(),
            ["yah-releases"]
        );
        assert_eq!(*publisher.revalidated.lock().unwrap(), 1);
        let manifest = &publisher.captured_manifests.lock().unwrap()[0];
        assert!(
            manifest.contains("0.8.6"),
            "manifest carries version: {manifest}"
        );
        assert!(
            manifest.contains("darwin-aarch64"),
            "manifest carries triple"
        );
        // R330-T14: the hook is handed the release it is poking about, so it
        // can carry the manifest as the poke's `data_inputs` instead of
        // leaving the receiver to render whatever its own node last fetched.
        assert_eq!(
            publisher.revalidate_saw.lock().unwrap().as_slice(),
            ["yah"],
            "revalidate must see the staged manifests, not just the fact of a publish"
        );
    }

    /// R330-T32: two releases through the dispatcher leave BOTH versions in
    /// the index. This is the whole point of the object — `release-manifest`
    /// is overwritten by each publish, so if the index behaved the same way
    /// the /releases page would render a one-row history forever.
    #[tokio::test]
    async fn dispatcher_accumulates_versions_in_the_index() {
        use std::sync::Arc;
        let src = TempDir::new().unwrap();
        let publisher = Arc::new(RecordingPublisher::default());
        let dispatcher = PublishingOutcomeDispatcher::new(ArcPublisher(publisher.clone()));

        for version in ["0.8.6", "0.8.7"] {
            let bin = write_dummy(src.path(), &format!("{version}/yah"), b"BIN");
            dispatcher
                .publish(&PublishRequest {
                    provider: "r2".into(),
                    bucket: "yah-releases".into(),
                    prefix: Some("dl".into()),
                    base_url: Some("https://releases.yah.dev".into()),
                    version: version.into(),
                    artifacts: vec![ProducedArtifact {
                        binary: "yah".into(),
                        path: bin,
                        triple: Some("darwin-aarch64".into()),
                    }],
                })
                .await
                .unwrap();
        }

        let objects = publisher.index_objects.lock().unwrap();
        let raw = objects
            .get("dl/yah/index.json")
            .expect("index published under the request's prefix");
        let index: ReleaseIndex = serde_json::from_str(raw).unwrap();
        assert_eq!(
            index.versions.iter().map(|v| &v.version).collect::<Vec<_>>(),
            ["0.8.7", "0.8.6"],
            "both releases present, newest first"
        );

        let entry = &index.versions[0];
        assert_eq!(
            entry.manifest_url.as_deref(),
            Some("https://releases.yah.dev/dl/yah/0.8.7/manifest.json"),
            "derived from (base_url, prefix, binary, version), prefix included"
        );
        // A HISTORY entry must not link the mutable pointer: 0.8.6's manifest
        // has to keep resolving to 0.8.6 after 0.8.7 lands.
        assert_eq!(
            index.versions[1].manifest_url.as_deref(),
            Some("https://releases.yah.dev/dl/yah/0.8.6/manifest.json"),
        );
        let triple = entry.triples.get("darwin-aarch64").expect("triple entry");
        assert_eq!(
            triple.url,
            "https://releases.yah.dev/dl/yah/0.8.7/darwin-aarch64/yah"
        );
        assert!(
            triple.hash.as_deref().is_some_and(|h| h.starts_with("blake3:")),
            "downloads carry a tagged hash: {:?}",
            triple.hash
        );
    }

    /// R330-T14: an unreachable revalidate receiver must NOT fail a release
    /// whose artifacts uploaded fine. The poke only collapses the staleness
    /// window; the consumer's feed-fetch tier is the correctness path, so
    /// aborting here would discard a good upload to save nothing.
    #[tokio::test]
    async fn dispatcher_publish_survives_a_failing_revalidate() {
        use std::sync::Arc;
        let src = TempDir::new().unwrap();
        let bin = write_dummy(src.path(), "target/release/yah", b"BIN");
        let publisher = Arc::new(RecordingPublisher {
            fail_revalidate: true,
            ..Default::default()
        });
        let dispatcher = PublishingOutcomeDispatcher::new(ArcPublisher(publisher.clone()));
        let req = PublishRequest {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: Some("https://releases.yah.dev".into()),
            version: "0.8.6".into(),
            artifacts: vec![ProducedArtifact {
                binary: "yah".into(),
                path: bin,
                triple: Some("darwin-aarch64".into()),
            }],
        };

        dispatcher
            .publish(&req)
            .await
            .expect("a failed revalidate poke must not abort the publish");

        // The upload still happened and the poke was still attempted — this is
        // "warn and stand", not "skip the hook".
        assert_eq!(
            publisher.synced.lock().unwrap().as_slice(),
            ["yah-releases"]
        );
        assert_eq!(*publisher.revalidated.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn dispatcher_skips_when_no_artifacts() {
        let publisher = RecordingPublisher::default();
        // Move a probe out before constructing the dispatcher: read counters
        // after via a shared Arc instead.
        use std::sync::Arc;
        let probe = Arc::new(publisher);
        let dispatcher = PublishingOutcomeDispatcher::new(ArcPublisher(probe.clone()));
        let req = PublishRequest {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
            version: "0.8.6".into(),
            artifacts: vec![],
        };
        dispatcher.publish(&req).await.unwrap();
        assert!(
            probe.synced.lock().unwrap().is_empty(),
            "no artifacts → no sync"
        );
        assert_eq!(*probe.revalidated.lock().unwrap(), 0);
    }
}
