//! @yah:relay(R623, "Migrate QED run history from .yah/jit/qed/*.json to turso")
//! @yah:assignee(bundle-anthropic-miravel)
//! @yah:kind(task)
//! @yah:at(2026-07-23T03:17:22Z)
//! @yah:gotcha("QED run history is the ODD ONE OUT: task-runs, task-sessions and gnome_queue are all turso-backed (.yah/db/*.turso), but qed runs persist as flat per-run files in .yah/jit/qed/ — <run_id>.json + <run_id>.events.jsonl, 342 of them as of 2026-07-21.")
//! @yah:next("Migrate persist_qed_run + load_qed_history (app/yah/cli/src/camp.rs) onto a turso store, following the task-runs store shape (oss/qed/crates/task-runs/src/store.rs).")
//! @yah:next("Needs a migration story for the 342 existing run files — import-on-boot or a one-shot, not a silent drop.")
//! @yah:next("NOT a blocker for R622 (manual steps). R622's parked-run durability is satisfied by a non-terminal JSON persist reusing the R603 pattern, which a turso migration would carry over anyway. Deliberately decoupled — see W282 'The turso question is real, but separate'.")
//! @arch:see(.yah/docs/working/W282-qed-manual-steps.md)
//! @arch:see(.yah/docs/working/W170-qed-recipe-discipline.md)
//! @arch:see(.yah/docs/working/W170-qed-recipe-discipline.md)
//! @arch:see(.yah/docs/working/W191-qed-pipeline-ux-tweaks.md)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @arch:see(.yah/docs/working/W296-executable-docs-notebook-cells.md)
//! @arch:see(.yah/docs/working/W296-executable-docs-notebook-cells.md)
//! @arch:see(.yah/docs/working/W298-shared-build-admission.md)
//! @arch:see(.yah/docs/working/W170-qed-recipe-discipline.md)
//!
//! @yah:relay(R827, "QED trigger coverage — pipelines can only fire on tag, cron, chain or manual")
//! @yah:at(2026-08-21T01:18:53Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-glimmerstone)
//! @arch:see(oss/qed/crates/qed/src/export.rs)
//! @arch:see(oss/qed/crates/qed/src/config.rs)
//! @yah:ticket(R827-F1, "Trigger::Path { globs } — fire a pipeline when specific files change")
//! @yah:at(2026-08-21T01:19:24Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-glimmerstone)
//! @yah:parent(R827)
//! @arch:see(oss/qed/crates/qed/src/export.rs)
//! @arch:see(oss/qed/crates/qed/src/config.rs)
//! @yah:next("Tier: Cleric — three small sites plus tests; the judgment is in who dispatches, not in what the exporter renders.")
//! @yah:next("Trigger is Manual | Tag { pattern } | Schedule { cron } | Pipeline { id, status } (types.rs:346). Add Path { globs: Vec<String> }: 'run this pipeline when these files change'.")
//! @yah:next("DEFINE IT IN QED'S OWN TERMS. types.rs:365 already states the model: qed has no polling daemon — a Trigger is a declaration of WHEN, and some host fires it. Tag is fired by a git-mirror hook, Schedule by almanac, Manual by the CLI/desktop. Path is a git-diff-aware dispatch and belongs with Tag, i.e. the yubaba git-mirror hook. THAT is the substance of this ticket.")
//! @yah:next("Site 1 — types.rs: the variant. Trigger is a plain serde enum and config.rs takes `triggers: Vec<Trigger>` with #[serde(default)] (config.rs:304), so TOML parsing comes free.")
//! @yah:next("Site 2 — the dispatcher: whichever hook fires Tag today needs to compare the push's changed paths against the globs. This is the real work; the enum variant is not.")
//! @yah:next("Site 3 — export.rs::render_on, LAST and least. Accumulate globs into a Vec inside the match loop as tags and crons already are, render after the loop.")
//! @yah:next("Tests: a config.rs parse test and an export.rs rendered-YAML test, mirroring the Tag/Schedule pairs at config.rs:2442 and config.rs:3196.")
//! @yah:next("MOTIVATING CONSUMER (noisetable camp, R660-T1): a third-party licence manifest generated from Cargo.lock plus an npm tree, with a drift guard. Tag-only means the first time you learn a new copyleft transitive arrived is at release. The interim there is a Schedule{cron} nightly — it works, and it is not the right answer.")
//! @yah:gotcha("CORRECTION TO THIS TICKET'S FIRST DRAFT: it specified Path around what an Actions `on: push: paths:` block can express. That is backwards. crates/qed-gha is an IMPORT front-end — its own header says 'QED imports a workflow, it does not faithfully emulate GitHub', and its tier-3 service overrides were retired rather than extended. R605 is actively taking GitHub off the release critical path, with the engine already done. Do not let a lossy export target define a QED vocabulary word.")
//! @yah:gotcha("Exporter bug, not a design constraint: render_on emits its `push:` block for Tag triggers after the match loop. A Path arm that emits its own `  push:\\n    paths:\\n` produces TWO `push:` keys in one `on:` mapping — invalid YAML that reads as correct in review. The accumulate-then-render shape already used for tags and crons is the fix.")
//! @yah:gotcha("When a card carries BOTH a Tag and a Path trigger, the exporter cannot render it faithfully: inside one Actions `push:` block, ref filters and `paths` are ANDed, so 'on release OR when these files change' is not expressible there. Push a Degradation, exactly as the Pipeline arm does for workflow_run. Do NOT reject the combination at config time — QED's own dispatchers can express it fine, and refusing a valid pipeline because one export target is lossy would be the tail wagging the dog.")
//! @yah:assumes("That the git-mirror hook which fires Tag can see the changed-path list for a push. If it cannot, the dispatch half of this ticket is bigger than the enum half and should be split — confirm before estimating.")
//! @yah:assumes("Trigger::Tag is matched only in export.rs and config.rs tests across external/yah (grepped 2026-08-20), so the hook that types.rs:365 describes was not located in this pass. Find it before writing the dispatch code.")
//! @yah:next("REVERSE EDGE, added from the noisetable camp (R660-S6): when this lands, go flip the motivating consumer yourself. In ~/ss/noisetable, .yah/qed/third-party-notices.toml carries a Schedule{cron} '15 4 * * *' nightly as the interim (comment at lines 18-29 says so); replace it with a Path trigger over Cargo.lock, web/*/package.json and the ATTRIBUTION.md files, then close noisetable's R660-S6. This @yah:next IS the edge — a notify_on cannot carry it, because dep_status (crates/yah/board/src/ticket.rs:1114) returns Unresolvable for an id the local board never saw, so a cross-camp notify_on never fires.")
//! @yah:ticket(R833-F10, "DEFERRED: trigger dispatch — nothing fires Trigger::Tag, and a dispatcher needs a build-storm guard designed in before it ships")
//! @yah:at(2026-08-29T20:54:08Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:phase(P4)
//! @yah:parent(R833)
//! @arch:see(.yah/docs/working/W330-distributing-camp-compute.md)
//! @yah:depends_on(R833-F8)
//! @yah:depends_on(R833-F9)
//! @yah:next("DELIBERATELY DEFERRED -- filed so it is visible and so the reasoning survives, not so it gets picked up next. It is blocked behind R833-F8 and R833-F9 on purpose. W330's argument: the imperative path is nearly done and needs no new service, while a dispatcher is a NEW ALWAYS-ON SERVICE to operate, and qed has deliberately never had a polling daemon.")
//! @yah:next("Tier: Wizard -- the storm guard and the decision to add a durable watcher at all are design judgement about what this camp is willing to operate.")
//! @yah:gotcha("THE STORM GUARD IS THE DESIGN, not a detail. GHA's model assumes human-paced pushes. This camp has ten agents wip-committing constantly, so a branch-triggered dispatcher would produce a build storm. The shape W330 specifies: TAGS ONLY by default -- rare and deliberately cut -- with branch triggers opt-in per branch. Whoever picks this up must design that guard in before shipping, not bolt it on after the first storm.")
//! @yah:gotcha("STATE OF THE CODE, verified in W330's inventory: Trigger::{Manual, Tag, Schedule, Pipeline} are all DECLARED (oss/qed/crates/qed/src/types.rs). Cron and pipeline-chaining fire. NOTHING fires Tag -- the variant exists and its doc comment names a GHA shim or yubaba hook that does not dispatch it. So this is a missing dispatcher, not a missing type.")
//! @yah:gotcha("OPERATOR REQUIREMENT (2026-09-13, Leif): whether a rig acts on the Tag dispatcher must be a PER-RIG opt-in, not global — a local dev workstation must never auto-build on a pushed tag, only a rig explicitly configured as a release runner may. us-west-003 is designated as the first such rig — the first \"CI runner fired by a git tag\" fleet node. This is in addition to, not instead of, the tags-vs-branches storm guard already noted above; whoever designs the dispatcher needs both axes (which trigger kinds fire, and which rigs listen) before it ships.")
//! @arch:see(.yah/docs/working/W235-remote-qed.md)
//! @arch:see(.yah/docs/working/W258-fleet-compute-modes-and-tenant-isolation.md)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use velveteen::TaskRuntime;

pub type QedRunId = String;
pub type ForgeId = String;

/// Schema for a pipeline-manifest field whose type is dynamic or lives in a
/// crate we deliberately don't pull `schemars` through (`matrix::MatrixSpec`'s
/// `toml::Value` blobs, `task::TaskRuntime`, the `manifest-bind` bind/value
/// types). Accepts any JSON so the generated `qed-pipeline.toml.schema.json`
/// stays permissive there rather than forcing a derive across those edges.
/// (R533-T10; tightening these to precise sub-schemas is a tracked follow-up.)
#[cfg(feature = "json-schema")]
pub(crate) fn permissive_schema(
    _gen: &mut schemars::gen::SchemaGenerator,
) -> schemars::schema::Schema {
    schemars::schema::Schema::Bool(true)
}

/// Mint a fresh [`QedRunId`]. Same shape (`Uuid::new_v4`) the [`PipelineRunner`]
/// uses internally, exposed so an orchestrator (e.g. the matrix fan-out parent
/// in R506-F1, which has no runner of its own) can allocate a run id.
pub fn new_run_id() -> QedRunId {
    uuid::Uuid::new_v4().to_string()
}

/// What can cause a pipeline to start.
///
/// Triggers are *declared* in the pipeline TOML but *dispatched* by the appropriate
/// scheduler — qed has no polling daemon. Tag triggers are fired by the GHA shim (or a
/// yubaba git-mirror hook); schedule triggers are fired by almanac; manual is the default.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum Trigger {
    /// `yah qed run <pipeline>` from CLI or desktop — always available.
    Manual,
    /// Git tag push matching a glob (e.g. `v*.*.*`), fired by the GHA shim or yubaba hook.
    Tag { pattern: String },
    /// Cron expression, dispatched by almanac via `["yah", "qed", "run", pipeline]` TaskSpec.
    Schedule { cron: String },
    /// Another pipeline completed with the given status, chained by qed outcomes.
    Pipeline { id: String, status: RunStatus },
}

/// Which *class of host* a recipe is allowed to run on (W155 principle 2). The
/// runner consults this at kick time to refuse out-of-place runs before any
/// step executes — e.g. CLI refuses `Ci` from a developer laptop unless
/// `--force`. The recipe itself never branches on the runner; this is the
/// contract that keeps recipe bodies host-agnostic.
///
/// **This is the PERMISSION axis, never the ROUTING axis** (W235 §Verdict §4).
/// It answers *may this recipe run on this class of host at all*, and it has
/// nothing to say about local-vs-fleet — that is `--where` / [`crate::runner::RunWhere`].
/// It was spelled `placement` until R555-T7, which is precisely the collision
/// that made the two readable as one thing: `QedRunParams::run_where` below is
/// the wire `where`, i.e. routing, and carried the same name.
///
/// Default is [`Environment::Any`] so a recipe that declares nothing keeps
/// working.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum Environment {
    /// Runs on a dev machine; meaningless on CI. The output is "yah.app
    /// installed in /Applications", "files written to the camp tree", etc.
    ///
    /// Spelled `workstation`, not `local-only`, deliberately: `local` is the
    /// one word that collides with `--where=local`, which is routing.
    Workstation,
    /// Needs secrets, signing identity, or a clean runner that don't exist
    /// locally. Publishing, codesigning, notarization.
    Ci,
    /// Pure verification — lint, typecheck, smoke. The gold standard.
    #[default]
    Any,
}

/// How the runner positions the on-disk tree a pipeline's steps build against,
/// relative to the run's target ref (the `ref` run-param — a branch, tag, or
/// SHA; default `HEAD`, i.e. whatever is already checked out).
///
/// A QED run's workspace is normally the live camp root — fine for verifying
/// whatever is on disk, wrong for cutting a release (which must never ship a
/// dev's uncommitted edits). This is the per-pipeline knob that picks the right
/// trade-off; the run carries the *which ref*, the pipeline carries the *how
/// strict*.
///
/// Default is [`WorkspaceMode::Checkout`] — switch to the requested ref but
/// refuse to run over uncommitted changes, so a stray run never silently builds
/// the wrong bytes and never clobbers local work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum WorkspaceMode {
    /// Build against the camp root's live working tree exactly as it is on disk
    /// — no ref switch, no dirty check. For local/dev pipelines that want
    /// "build what I'm looking at right now".
    Live,
    /// Switch the camp root to the run's target ref, but **bail if the tree
    /// is dirty** (any uncommitted change). The safe default: never builds
    /// surprise bytes, never discards local work.
    #[default]
    Checkout,
    /// Build in a dedicated git worktree checked out at the target ref; the
    /// camp root (and any uncommitted work in it) is untouched. The correct
    /// mode for releases — a tag is always cut from clean committed state.
    Isolated,
}

/// The TOML spelling of a [`WorkspaceMode`] — what an author writes for
/// `[pipeline] workspace`, so an error message can quote the key rather than
/// the Rust variant name.
pub fn workspace_mode_key(mode: WorkspaceMode) -> &'static str {
    match mode {
        WorkspaceMode::Live => "live",
        WorkspaceMode::Checkout => "checkout",
        WorkspaceMode::Isolated => "isolated",
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Pipeline {
    pub name: String,
    pub label: String,
    /// Long-form prose about what this pipeline does — the readme the catalog
    /// shows above the steps (R703-F3).
    ///
    /// Normally *not* written as a TOML key. The loader lifts it from the
    /// file's leading `#` comment block, because that block is where every
    /// pipeline in this camp already had its readme: authors write the
    /// rationale at the top of the file where an editor shows it, and until
    /// R703-F3 the daemon threw it away and shipped only the one-line `label`.
    /// An explicit `[pipeline] description = "..."` still wins when present,
    /// for a pipeline synthesised in code rather than parsed from a file.
    ///
    /// `skip_serializing_if` keeps it out of `qed eject`'s generated TOML when
    /// absent; when present it ejects as a key, since eject has no comment
    /// block to put it back into.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Free-form classification tags (`tags = ["smoke", "cloud"]`). Purely a
    /// catalog affordance — the runner never reads them. They exist because a
    /// camp's `.yah/qed/` grows past the point where a flat alphabetical roster
    /// is navigable, and the pipeline *name* is a poor carrier of genre (five
    /// pipelines named `*-smoke` tested five unrelated things). The UI groups
    /// and filters the roster by these; `yah qed pipelines` prints them.
    ///
    /// No vocabulary is enforced. A camp picks its own; yah's own convention is
    /// one genre tag (`check` / `smoke` / `e2e` / `build` / `release` /
    /// `publish` / `chore`) plus any number of subject tags (`desktop`, `cli`,
    /// `cloud`, `oss`, …). Enforcing a closed set here would make the field a
    /// second place to edit every time a camp grows a new kind of pipeline.
    ///
    /// `skip_serializing_if` keeps `tags = []` out of `qed eject`'s generated
    /// TOML — an untagged pipeline should eject to a file with no tags line.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// R751-F2 — the base pipeline this one SPECIALIZES, when it is a
    /// specialization rather than a hand-written recipe.
    ///
    /// A specialization is a steps-less `.yah/qed/*.toml` carrying
    /// `alias_of = "<base>"` and a `[pipeline.pin]` table; the loader
    /// ([`PipelineLoader::load`](crate::config::PipelineLoader::load))
    /// resolves it into a real `Pipeline` — the base's steps under the alias's
    /// own name/label/tags/description, with the pinned keys moved out of
    /// [`Self::params`] and into [`Self::pins`]. By the time anything
    /// downstream sees the value, it is indistinguishable from a hand-written
    /// pipeline, so this field is **provenance only**: nothing in the runner
    /// reads it. It exists so the catalog can say "specialization of `<base>`"
    /// and so a specialization round-trips back to the file it came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias_of: Option<String>,
    pub steps: Vec<QedStep>,
    #[serde(default)]
    pub params: HashMap<String, ParamDef>,
    /// R751-F2 — param values FIXED by a specialization (`[pipeline.pin]`).
    ///
    /// The difference between a pin and a [`ParamDef::default`] is who may
    /// change it: a default is a suggestion the run can override, a pin is
    /// part of the specialization's identity. `desktop-local` isn't
    /// "`local-install` where `target` happens to start at `desktop`", it *is*
    /// `local-install` with `target = "desktop"` — a pin a `--param` could undo
    /// would just be a default wearing a different name.
    ///
    /// Pinned keys are removed from [`Self::params`] at load time (they are no
    /// longer a question to ask the operator) but are re-inserted by
    /// [`Self::resolve_params`], so `{{key}}` substitution and `if =
    /// "params.k == …"` step gating see them exactly as they see any other
    /// resolved param. Supplying a *conflicting* value fails the run with
    /// [`ParamError::PinnedParamOverride`]; supplying the same value is a
    /// no-op, which is what makes re-running a recorded param set work.
    ///
    /// Empty for every pipeline that isn't a specialization.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub pins: HashMap<String, String>,
    #[serde(default)]
    pub on_success: Vec<Outcome>,
    #[serde(default)]
    pub on_fail: Vec<Outcome>,
    /// Triggers that can start this pipeline. Defaults to `[Manual]` when omitted.
    #[serde(default)]
    pub triggers: Vec<Trigger>,
    /// Lock key that serializes concurrent runs. When two runs share a key,
    /// the second one is `Queued` until the first finishes.
    ///
    /// `None` means [`DEFAULT_CONCURRENCY_KEY`] — **camp-global**: an unkeyed
    /// pipeline serializes against every other unkeyed pipeline, not just
    /// against other runs of itself. R719-F1 inverted this; it used to default
    /// to the pipeline's own name.
    ///
    /// The inversion is about which mistake is cheap. Under the old default,
    /// forgetting a key gave you *parallelism* — two unrelated recipes could
    /// stomp each other's `target/` and nothing said so. That is how
    /// `desktop-release` shipped unkeyed and an operator got four concurrent
    /// runners off three launches (R435-T3 audited the stamps once, by hand,
    /// with no guard). Under the new default, forgetting a key gives you
    /// *serialization*: slower, visible, and safe. Opting a genuinely
    /// independent recipe out is a one-line, deliberate act.
    ///
    /// Two other spellings:
    /// - Any other string is a narrow lane. Pipelines that fight over one
    ///   resource (cargo's shared `target/`, a build worker, a cloud apply)
    ///   pin to a common key — `"cargo-target"`, `"pi-image"`, `"cloud-apply"`.
    /// - [`PARALLEL_CONCURRENCY_KEY`] (`"@parallel"`) opts out entirely; runs
    ///   never block each other. For recipes that touch no shared resource —
    ///   read-only fan-outs, fetch-and-compare drift checks.
    #[serde(default)]
    pub concurrency_key: Option<String>,
    /// Ceiling on how many of THIS run's steps execute at once (R605-F3).
    ///
    /// The sibling of [`Self::concurrency_key`] one scope in:
    /// `concurrency_key` bounds how many *runs* overlap, this bounds how many
    /// *steps within one run* overlap once [`QedStep::needs`] says they may.
    /// `None` ⇒ [`crate::dag::DEFAULT_MAX_PARALLEL`]; `Some(1)` pins the run
    /// strictly serial regardless of what the DAG allows, which is the setting
    /// to reach for when a pipeline turns out to contend in a way its
    /// [`QedStep::resource`] keys don't capture yet.
    ///
    /// Raising it does nothing to a pipeline that declares no `needs`: those
    /// resolve to a chain, and a chain never has two ready steps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallel: Option<usize>,
    /// Which class of host this recipe is allowed to run on (W170). Defaults to
    /// [`Environment::Any`]. The runner enforces this at kick time
    /// (R435-F2) — the recipe body itself remains host-agnostic.
    #[serde(default)]
    pub environment: Environment,
    /// How the runner positions the on-disk tree this pipeline builds against
    /// (W224). Defaults to [`WorkspaceMode::Checkout`] (switch to the run's
    /// ref, bail if dirty). Releases set `workspace = "isolated"` so a tag is
    /// always cut from a clean worktree, never a dev's live edits; local-only
    /// pipelines may set `workspace = "live"` to build the tree as-is.
    #[serde(default)]
    pub workspace: WorkspaceMode,
    /// Optional GHA-workflow this pipeline wraps. Set to `"gha:<rel-path>"`
    /// in TOML (e.g. `wraps = "gha:.github/workflows/release.yml"`) when the
    /// pipeline exists *because* it composes a workflow; the daemon then
    /// suppresses that workflow's auto-ingest so the catalog doesn't show
    /// both entries. Purely advisory — not interpreted by the runner.
    #[serde(default)]
    pub wraps: Option<String>,
    /// Native matrix expansion (R505). When present, [`crate::matrix::plan`]
    /// expands the pipeline into one concrete job per matrix row, with
    /// `${{ matrix.<key> }}` substituted across each step's `argv` / `env` /
    /// `cwd`. Absent or empty → single-job plan (no expansion). Mirrors GHA's
    /// `strategy.matrix` semantics (cartesian product + include/exclude).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matrix: Option<crate::matrix::MatrixSpec>,
    /// Declarative toolchain pinning (R507, W208 pillar 3). `[pipeline.toolchain]`
    /// pins tool versions (rust/xcode/ndk/msvc/…) checked against the host at
    /// plan time, so a release fails fast with an actionable error instead of
    /// dying mid-build on a missing SDK. Per-step `toolchain.<tool>` overrides
    /// (see [`QedStep::toolchain`]) layer on top. Absent (the default) ⇒ no
    /// pins, no check. See [`crate::toolchain`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toolchain: Option<crate::toolchain::ToolchainSpec>,
    /// W209: `[[bind]]` tables — pipeline-output → in-tree-manifest write-backs.
    /// Each bind names a target file/path, a producer step output (or URI
    /// escape hatch), and an intent predicate. The runner evaluates them
    /// mid-pipeline as each producing step completes; failed steps simply
    /// skip the binds that reference them. Defaults to empty for pipelines
    /// that don't bind anything.
    #[serde(default)]
    pub binds: Vec<manifest_bind::BindSpec>,
    /// W209/R510-F6: `[[on_change]]` hash-change hooks. Each names a bind
    /// selector (matched against a changed [`manifest_bind::AppliedBind`]'s
    /// `path`) and an action (fire a pipeline, emit an event, or append to a
    /// journal). The runner evaluates them after each step's binds commit,
    /// firing only for binds that actually changed bytes on disk. Empty for
    /// pipelines without hooks.
    #[serde(default)]
    pub on_change: Vec<manifest_bind::OnChangeHook>,
    /// W207 Gap #6 (R513-F4): always-run teardown steps. Every step here runs
    /// unconditionally after the main step loop and the background-sidecar reap
    /// — whether the pipeline passed or failed — making it the home for
    /// diagnostics/artifact teardown that must happen either way (upload
    /// Playwright traces, `docker compose down`, collect logs). Sidecar teardown
    /// itself is already structural (the F2 background reap), so `finally` is for
    /// the *once-after-loop* work the reap doesn't cover.
    ///
    /// Semantics (see [`crate::runner`]): all `finally` steps are attempted
    /// best-effort — a failing one never aborts the rest (teardown should always
    /// run to completion). A `finally` step that fails marks the *run* Failed
    /// (visible in the run tile + `RunFinished`) unless it sets
    /// `on_fail = "continue"`, but it does **not** change which terminal
    /// outcomes fire — `on_success` vs `on_fail` is selected from the
    /// pipeline's *work* result (steps + sidecars), not from teardown. v1
    /// restricts `finally` steps to [`StepKind::Subprocess`] (the teardown
    /// shape); composite/background kinds in `finally` are rejected at load
    /// time.
    #[serde(default)]
    pub finally: Vec<QedStep>,
    /// R823-F2 — `[pipeline.participants]`: the set of hosts that run
    /// **concurrently inside this one run**, each knowing the others'
    /// addresses, producing one verdict.
    ///
    /// Orthogonal to every neighbouring knob, and worth stating why, because
    /// three of them look adjacent:
    ///
    /// - [`environment`](Self::environment) is a *permission* gate — may this
    ///   run happen on this class of host at all.
    /// - [`matrix`](Self::matrix) fans steps into rows that are **independent
    ///   jobs**; no row can address another, which is precisely what a
    ///   rendezvous needs.
    /// - A `native = true` [`platform`](QedStep::platform) step offloads *work*
    ///   to one arch-matched worker and waits for it — one host doing a job for
    ///   the coordinator, not two hosts talking to each other.
    ///
    /// `None` on every pipeline that doesn't declare the block, which is all of
    /// them until one needs a multi-node case. See [`crate::participants`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participants: Option<crate::participants::ParticipantSet>,
    /// R906-F2 — opt out of the operator-gate rule
    /// ([`Self::lint_operator_gates`]): this pipeline may declare several
    /// `audience = "operator"` manual steps, place one after another manual
    /// step, or place one behind a compile.
    ///
    /// Set it when the pipeline is genuinely meant to be *attended throughout*
    /// — a human is at the keyboard for its whole length, so "parks on a person
    /// at an unknown depth into an unbounded run" is not a cost it pays.
    /// Everything else should move the gate instead. It suppresses both halves
    /// at once (the load-time warning and the run-time failure), deliberately:
    /// a pipeline that warns but runs, or runs but warns, is the state this
    /// rule exists to avoid.
    #[serde(default)]
    pub allow_late_operator_block: bool,
}

/// Key an unkeyed pipeline falls back to (R719-F1). Camp-global: every
/// pipeline that declares no `concurrency_key` shares this one lane.
///
/// The `@` prefix marks it as a sentinel rather than a plausible user key,
/// matching [`PARALLEL_CONCURRENCY_KEY`]. Unlike `@parallel` it needs no
/// special handling anywhere — it is an ordinary map key that happens to be
/// spelled so nobody types it by accident.
///
/// R719-T6 considered and declined making this `"cargo-target"` — i.e. having
/// an unkeyed pipeline join the lane that cargo work already shares. The point
/// was real: `@camp` and `cargo-target` are different mutexes, so a recipe that
/// cargo-builds and forgets its key does *not* serialize against the ones that
/// remembered. But the fix is wrong at this altitude. qed is a general pipeline
/// engine (it ships standalone); naming its default after a Rust toolchain's
/// build directory would be a lie for every other kind of recipe, and it only
/// covers recipes whose shared resource happens to be cargo's — one that forgets
/// `cloud-apply` or an image-build lane would just be misfiled instead of
/// unfiled. A default cannot infer which resource a recipe contends on, so the
/// answer is to stop letting recipes decline to say: yah's camp pins it with
/// `app/yah/cli/tests/camp_qed_admission_lanes.rs`, which fails the build when a
/// `.yah/qed/*.toml` omits the key or puts local cargo work in another lane.
/// Downstream users without that guard still get the conservative default,
/// which is what this constant is for.
pub const DEFAULT_CONCURRENCY_KEY: &str = "@camp";

/// Sentinel that opts a pipeline out of serialization entirely.
pub const PARALLEL_CONCURRENCY_KEY: &str = "@parallel";

impl Pipeline {
    /// The effective concurrency key for this pipeline — `concurrency_key` if
    /// set, otherwise [`DEFAULT_CONCURRENCY_KEY`]. The daemon's per-key mutex
    /// map is keyed off this value.
    ///
    /// R719-F1: this used to fall back to `self.name`, which made "I forgot to
    /// set a key" mean "run me concurrently with anything" — see the field doc
    /// on [`Pipeline::concurrency_key`] for why that default was backwards.
    pub fn effective_concurrency_key(&self) -> &str {
        self.concurrency_key
            .as_deref()
            .unwrap_or(DEFAULT_CONCURRENCY_KEY)
    }

    /// `true` when the pipeline opts out of serialization via the sentinel
    /// key `"@parallel"`.
    pub fn is_parallel(&self) -> bool {
        self.effective_concurrency_key() == PARALLEL_CONCURRENCY_KEY
    }

    /// R751-F3 — the params a run MUST be told, sorted: `required` with no
    /// `default` to fall back on and no pin fixing them.
    ///
    /// Pinned keys are already out of [`Self::params`] by the time a
    /// specialization reaches any consumer, so this needs no special case for
    /// them — which is the point of resolving pins in the loader.
    pub fn unbound_params(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .params
            .iter()
            .filter(|(_, def)| def.required && def.default.is_none())
            .map(|(name, _)| name.clone())
            .collect();
        out.sort();
        out
    }

    /// R751-F3 — how this pipeline sits on the template ↔ concrete axis.
    ///
    /// Derived here rather than in each consumer: it is a property of the
    /// pipeline, and the desktop roster deriving its own copy is what let
    /// R751-B1 (a null-vs-undefined seam) empty the Templates group for five
    /// pipelines without anything noticing.
    pub fn classification(&self) -> PipelineClass {
        if !self.unbound_params().is_empty() {
            PipelineClass::Template
        } else if self.alias_of.is_some() {
            PipelineClass::Specialization
        } else {
            PipelineClass::Concrete
        }
    }
}

/// Where a pipeline sits on the template ↔ concrete axis (R751-F3).
///
/// The operator's framing is C++/Rust templates, and the analogy carries all
/// the way down — including the case that decides the precedence below. A
/// *partial* specialization is still a template: it binds some parameters and
/// leaves others open, so it is not a thing you can instantiate. So a
/// specialization that pins one of its base's two required params is reported
/// as [`Self::Template`], not [`Self::Specialization`] — because the question
/// the roster's primary cut asks is "can the operator kick this with no
/// input?", and the honest answer there is no. Its
/// [`Pipeline::alias_of`] still says where it came from, so nothing is lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum PipelineClass {
    /// Has at least one unbound required param, so it cannot be run as-is —
    /// it is a building block something else specializes or supplies args to.
    Template,
    /// Fully bound AND `alias_of` a base: a named binding of a template,
    /// runnable with no input.
    Specialization,
    /// Fully bound and not an alias — an ordinary hand-written recipe.
    Concrete,
}

impl PipelineClass {
    /// The wire spelling, matching the serde rename. Kept as a method so the
    /// daemon's wire mapping and any log line agree by construction.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Template => "template",
            Self::Specialization => "specialization",
            Self::Concrete => "concrete",
        }
    }
}

impl Pipeline {
    /// Resolve the run's supplied params against this pipeline's declarations:
    /// fill in defaults for anything absent, fail naming every required param
    /// that has neither a supplied value nor a default, and reject any value
    /// outside a param's declared `options` set.
    ///
    /// Both entry points (`yah qed run` and the camp daemon's `qed.run`) had
    /// their own copy of the required-param loop and neither knew about
    /// defaults, so this is the one place that decides what a run's params ARE.
    /// Feed the result to [`Self::apply_params`].
    ///
    /// Unknown supplied params are currently passed through rather than
    /// rejected: a typo'd `--param bord=x` substitutes nothing and the pipeline
    /// runs with `{{board}}` intact. That is worth rejecting, but it is a
    /// behaviour change across callers this crate cannot audit (desktop UI, MCP
    /// tools, other repos' workflows), so it is deliberately left alone here.
    pub fn resolve_params(
        &self,
        supplied: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>, ParamError> {
        let mut resolved = supplied.clone();
        // R751-F2: pins first. A pinned key is not in `self.params` (the loader
        // moved it out), so the default/required/options loops below don't see
        // it — but every downstream reader of the resolved map does, which is
        // what makes `{{key}}` substitution and `if = "params.k == …"` gating
        // work identically for a pinned and an operator-supplied value.
        // Sorted so a file pinning several conflicting keys reports the same
        // one every time rather than whichever the hash order surfaced.
        let mut pinned: Vec<(&String, &String)> = self.pins.iter().collect();
        pinned.sort_by(|a, b| a.0.cmp(b.0));
        for (name, value) in pinned {
            if let Some(supplied) = resolved.get(name.as_str()) {
                if supplied != value {
                    return Err(ParamError::PinnedParamOverride {
                        pipeline: self.name.clone(),
                        base: self.alias_of.clone().unwrap_or_else(|| self.name.clone()),
                        name: name.clone(),
                        pinned: value.clone(),
                        value: supplied.clone(),
                    });
                }
                continue;
            }
            resolved.insert(name.clone(), value.clone());
        }
        let mut missing: Vec<String> = Vec::new();
        for (name, def) in &self.params {
            if resolved.contains_key(name.as_str()) {
                continue;
            }
            match &def.default {
                Some(d) => {
                    resolved.insert(name.clone(), d.clone());
                }
                None if def.required => missing.push(name.clone()),
                None => {}
            }
        }
        if !missing.is_empty() {
            missing.sort();
            return Err(ParamError::MissingRequired {
                pipeline: self.name.clone(),
                names: missing,
            });
        }
        // Enumerated params are a closed set — checked after defaults are
        // filled so a bad default fails here too, on the paths that build a
        // Pipeline without going through the config loader's load-time check.
        let mut enumerated: Vec<(&String, &ParamDef)> = self
            .params
            .iter()
            .filter(|(_, def)| !def.options.is_empty())
            .collect();
        enumerated.sort_by(|a, b| a.0.cmp(b.0));
        for (name, def) in enumerated {
            let Some(value) = resolved.get(name.as_str()) else {
                continue;
            };
            if !def.options.iter().any(|o| o == value) {
                return Err(ParamError::NotInOptions {
                    pipeline: self.name.clone(),
                    name: name.clone(),
                    value: value.clone(),
                    options: def.options.clone(),
                });
            }
        }
        Ok(resolved)
    }

    /// Substitute `{{key}}` placeholders in every step's `argv`, `env`, a
    /// `gha-workflow` step's `inputs` and `matrix` (a gha-workflow step has no
    /// argv or env, so these are its only parameterisable surface), a
    /// `sub-pipeline` step's forwarded `params`, a step's `platform.target`,
    /// and a `manual` step's `prompt`, `terminal`, `advance`, and `checklist`
    /// (R906-B3 — `audience` is an enum, not substitutable). Unknown
    /// placeholders are left untouched. Feed this the result of
    /// [`Self::resolve_params`], which fills defaults and checks required params.
    pub fn apply_params(&mut self, params: &HashMap<String, String>) {
        if params.is_empty() {
            return;
        }
        for step in &mut self.steps {
            for arg in &mut step.argv {
                *arg = substitute(arg, params);
            }
            for value in step.env.values_mut() {
                *value = substitute(value, params);
            }
            // A `gha-workflow` step carries no argv and no env — everything a run
            // param could parameterise about it lives here, so without this a
            // wrapped workflow could not be told which row to build or what
            // dispatch input to use. `inputs` was reachable-but-unsubstituted
            // before `matrix` existed.
            if let Some(cfg) = step.gha_workflow.as_mut() {
                for value in cfg.inputs.values_mut() {
                    *value = substitute(value, params);
                }
                for value in cfg.matrix.values_mut() {
                    *value = substitute(value, params);
                }
            }
            // A `sub-pipeline` step's `params` table is how a parent forwards
            // its own params down (e.g. release-wizard's `spec = "{{spec}}"`
            // to version-bump). Without this, `{{spec}}` survived into the
            // child's `resolve_params` call literally, then into its argv —
            // the child never saw the parent's own resolved params, only the
            // raw placeholder text (R755, found exercising a real run).
            if let Some(cfg) = step.sub_pipeline.as_mut() {
                for value in cfg.params.values_mut() {
                    *value = substitute(value, params);
                }
            }
            // R786-B1: a step's `[platform]` block can name its target with a
            // `{{target}}` placeholder (release-build.toml's cross-build step
            // does, since the same recipe serves every matrix leg) — without
            // this, `platform.target` keeps the literal `{{target}}` text
            // forever, so `step_platform`/`native_cross_plan` never see a real
            // triple and the whole NativeCross preflight silently no-ops.
            if let Some(spec) = step.platform.as_mut() {
                if let Some(target) = spec.target.as_mut() {
                    *target = substitute(target, params);
                }
            }
            // R906-B3: a manual step's gate predicate (`advance`) and its
            // human-facing text (`prompt`, `terminal`, `checklist`) are the
            // pipeline's params too — without this a predicate can't see its
            // own run's params (R605-B17's on-disk nonce protocol exists only
            // because of this gap). `audience` is an enum, not a template, so
            // it is deliberately not touched here.
            if let Some(cfg) = step.manual.as_mut() {
                cfg.prompt = substitute(&cfg.prompt, params);
                for line in &mut cfg.terminal {
                    *line = substitute(line, params);
                }
                if let Some(advance) = cfg.advance.as_mut() {
                    *advance = substitute(advance, params);
                }
                for item in &mut cfg.checklist {
                    *item = substitute(item, params);
                }
            }
        }
    }
}

/// Replace each `{{key}}` occurrence in `input` with its param value.
fn substitute(input: &str, params: &HashMap<String, String>) -> String {
    let mut out = input.to_string();
    for (key, value) in params {
        out = out.replace(&format!("{{{{{key}}}}}"), value);
    }
    out
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct QedStep {
    pub name: String,
    #[serde(default)]
    pub argv: Vec<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Per-step budget **in seconds** (R603-B6). Every pipeline TOML has always
    /// written seconds (`timeout = 1800` for a 30-minute `cargo check`,
    /// `timeout = 9000` for the 2.5h rusty-v8 build), but the runner used to
    /// lower this with `Millis::from_ms`, reading 9000 as 9 *milliseconds*-worth
    /// of seconds — i.e. 9s. That stayed invisible for local steps (the local
    /// driver never enforces `spec.timeout`) and silently killed every long
    /// REMOTE step at 1/1000th of its budget. Lower it with
    /// [`Millis::from_secs`], never `from_ms`.
    #[serde(default)]
    pub timeout: Option<u64>,
    #[serde(default)]
    pub on_fail: OnFail,
    /// Release artifacts this step builds, declared so an [`Outcome::Publish`]
    /// can collect + upload them into the R2 release channel (R330-F3). Only
    /// the artifacts of *successful* steps are collected. Defaults to empty —
    /// most steps (check, typecheck) produce nothing publishable.
    #[serde(default)]
    pub produces: Vec<ProducedArtifact>,
    /// How this step is sandboxed.  `None` defers to the pipeline default
    /// (resolved from `--where`: local ⇒ Native, remote ⇒ Container).  Setting
    /// it explicitly in TOML pins the runtime regardless of where the
    /// pipeline runs — used by `build-image` steps that must always be
    /// containerised.
    #[serde(default)]
    #[cfg_attr(feature = "json-schema", schemars(schema_with = "crate::types::permissive_schema"))]
    pub runtime: Option<TaskRuntime>,
    /// Which step variant this is.  Defaults to [`StepKind::Subprocess`] —
    /// existing TOML and Rust literals omit the field.  Set to
    /// [`StepKind::BuildImage`] to build an image instead of running argv.
    #[serde(default)]
    pub kind: StepKind,
    /// Catalog entry name resolved by the image catalog loader (R381-T1).
    /// Required when `kind = build-image`; used by `Subprocess` only as a
    /// nominal hint until the per-step image-override path is wired through
    /// (R381 follow-up — see runner notes).
    #[serde(default)]
    pub image: Option<String>,
    /// Output tag when `kind = build-image`.  Defaults to the step's `name`.
    #[serde(default)]
    pub tag: Option<String>,
    /// When `kind = build-image`, push the resulting image to its registry
    /// after a successful build.  Ignored for other kinds.
    #[serde(default)]
    pub push: bool,
    /// For `kind = build-image`: docker `--platform` values the image is built
    /// for (e.g. `["linux/amd64"]`). Empty (the default) means host-native —
    /// buildx picks the daemon's own platform, which is what every pre-existing
    /// build-image step got.
    ///
    /// This is the *image* platform, distinct from `platform.target` (the Rust
    /// triple a build produces). A foreign-arch entry here does NOT authorize
    /// emulation: the runner refuses to build a foreign platform on a local
    /// docker daemon (that is QEMU by another name) unless the step also
    /// declares `platform = { native = true, … }`, which routes it to an
    /// arch-matched build-worker instead.
    #[serde(default)]
    pub platforms: Vec<String>,
    /// For `kind = package-native-tarball` (R407-T2): filesystem path to the
    /// static musl Rust binary produced by an earlier build step. Resolved
    /// relative to the camp root.
    #[serde(default)]
    pub binary_path: Option<String>,
    /// For `kind = package-native-tarball` (R407-T2): target-triple shorthand
    /// (e.g. `x86_64-unknown-linux-musl`) baked into the tarball stem and the
    /// emitted manifest. `None` resolves to the build host's triple at
    /// packaging time.
    #[serde(default)]
    pub triple: Option<String>,
    /// For `kind = musl-static-preflight` (R407-T3): workspace member name
    /// to gate (e.g. `yubaba`, `yah`). The runner walks its transitive dep
    /// closure and fails if any crate in
    /// [`crate::preflight::KNOWN_GLIBC_ONLY_CRATES`] appears.
    #[serde(default)]
    pub package: Option<String>,
    /// For `kind = build-image`: docker build context directory, resolved
    /// relative to the camp root. Defaults to `.` (camp root itself) when
    /// absent — the same behaviour as before this field existed. Use this
    /// to point at a staging directory assembled by an earlier subprocess
    /// step (e.g. `context = "target/yah-yubaba-ctx"`).
    #[serde(default)]
    pub context: Option<std::path::PathBuf>,
    /// Camp-root-relative subtrees to ship to the worker when this *subprocess*
    /// step is dispatched remotely (R560-T8) — the missing analogue of
    /// [`Self::context`] + `context_url` on the build-image path.
    ///
    /// # The gap this closes
    ///
    /// `build_workload_spec` hands a remote subprocess exactly three things:
    /// the image, the argv, and the `/yah/produced` durable mount. **No
    /// source.** `rusty-v8-musl` gets away with that because its baked
    /// `build-v8.sh` clones V8 from the internet inside the container; a step
    /// that compiles the *camp tree* has nothing to compile. Meanwhile
    /// `kind = build-image` has had a cross-host transport since R636-B1: pack
    /// the context, PUT it somewhere the worker can GET, pass the URL. This
    /// field is that same transport, re-pointed at subprocess steps — same
    /// [`crate::build_context::BuildContextPublisher`] seam, same single-use
    /// run-scoped key, same delete-on-both-legs.
    ///
    /// # What the step sees
    ///
    /// The runner sets **`YAH_SOURCE_CONTEXT_URL`** in the remote step's
    /// environment; the argv fetches and unpacks it. The fetch is deliberately
    /// *in the argv* rather than injected as a shell prelude: a prelude would
    /// assume an entrypoint shape qed does not own, and it would make the TOML
    /// stop describing what actually runs. If the var is set and the argv
    /// ignores it, the build fails on a missing source tree — loudly, in
    /// seconds.
    ///
    /// # What travels
    ///
    /// Only **git-tracked** files under the named paths, read from the working
    /// tree (so uncommitted edits DO travel — this ships the tree as
    /// positioned, which is the point). Tracked-only is not a tidiness
    /// preference: `oss/mesofact` is 28 GB on disk and 5 MB tracked, so a
    /// naive directory walk ships `target/` and `node_modules/` and trips
    /// [`crate::build_context::MAX_CONTEXT_BYTES`] before it ships a single
    /// source file. Entries keep their camp-root-relative paths, so unpacking
    /// the tar into an empty dir reproduces a repo-root-shaped tree and path
    /// deps that escape a workspace still resolve.
    ///
    /// Empty (the default) on every step that predates this field, and the
    /// runner publishes nothing for such a step — no behaviour change, no
    /// upload, for the whole existing corpus.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_context: Vec<std::path::PathBuf>,
    /// Give this *subprocess* step a host-persistent build cache that outlives
    /// the container it runs in (R876-F4) — the missing third thing an
    /// offloaded step gets, alongside [`Self::source_context`] and
    /// `/yah/produced`.
    ///
    /// # The gap this closes
    ///
    /// A container's writable layer is destroyed when it is reaped, and
    /// `/yah/produced` is per-run. So a step that compiles a tree compiles it
    /// from scratch *every* run: `mesofact-musl`'s x86_64 leg measured 9m56s /
    /// 9m57s / 11m10s and every one of those was a cold full release build.
    ///
    /// # What the step sees
    ///
    /// The runner binds a host dir at `/yah/cache` and sets
    /// **`YAH_CACHE_DIR`** to it. The argv points its own toolchain there —
    /// `export CARGO_TARGET_DIR="$YAH_CACHE_DIR/target"`. Deliberately *in the
    /// argv*, exactly as `YAH_SOURCE_CONTEXT_URL`'s fetch is: the mount is
    /// toolchain-agnostic, and injecting a cargo-shaped prelude would make the
    /// TOML stop describing what actually runs.
    ///
    /// # The sharing key is derived, never written here
    ///
    /// This is a `bool`, not a key, and that is the whole safety argument. Two
    /// runs sharing one cargo target dir that should not is a correctness bug;
    /// `concurrency_key` is camp-side scheduling and does not constrain a
    /// second camp or a hand-rolled dispatch at the same worker. The runner
    /// derives the key from **pipeline + step name + target triple**
    /// (`workload_spec::forge_cache::key_from_parts`), so a collision between
    /// two pipelines, or between one pipeline's two triples, is impossible by
    /// construction rather than by convention.
    ///
    /// Two runs of the *same* pipeline+step+triple do share, which is the
    /// entire point; cargo's own `.cargo-lock` in the target dir serializes any
    /// two that overlap in time.
    ///
    /// # Eviction
    ///
    /// Bounded at both ends by `forge_cache::evict_plan`: a cache dir idle past
    /// `forge_cache::RETENTION` (14 days) is removed, and while the filesystem
    /// holding the cache root is under `forge_cache::FREE_FLOOR_BYTES` (10 GiB)
    /// the least-recently-used dirs are removed too. yubaba sweeps the worker
    /// root at deploy; the runner sweeps the camp-local root before a cached
    /// local-container step.
    ///
    /// # Where it applies
    ///
    /// Subprocess steps that run in a container — remote (a `VolumeMount` under
    /// `forge_cache::HOST_ROOT` on the worker) or local (a bind of a camp-cache
    /// dir), same container path either way. A *native* step is refused rather
    /// than ignored: it has no mount namespace, its build scratch already
    /// persists, and a silently-inert cache is the exact failure this field
    /// exists to avoid.
    #[serde(default)]
    pub cache: bool,
    /// For `kind = build-image`: load the finished image into the local
    /// docker daemon with `--load` instead of writing an OCI archive.
    /// Use in dev pipelines where the image must be immediately runnable.
    /// Mutually exclusive with multi-platform builds; ignored when
    /// `push = true`.
    #[serde(default)]
    pub load: bool,
    /// For `kind = sub-pipeline` (W201-F1): the target to resolve as a child
    /// pipeline, params to forward, and what to roll up into the parent. The
    /// runner recurses into the resolved child as a nested `QedRun` parented
    /// to the caller; ProducedArtifacts and named outputs flow back per
    /// [`SubPipelineCollect`].
    #[serde(default)]
    pub sub_pipeline: Option<SubPipelineConfig>,
    /// Named outputs this step may emit (W201-F4). Subprocess steps write
    /// `KEY=VALUE\n` lines to `$YAH_OUTPUTS`; the runner captures them in
    /// [`StepStatus::outputs`] for downstream sibling substitution via
    /// `${{ steps.<name>.outputs.<key> }}`. Declaring outputs here is
    /// advisory — undeclared keys are captured too.
    #[serde(default)]
    pub outputs: Vec<OutputDecl>,
    /// Source files this step's result depends on, content-hashed at run time
    /// (R717-T1, W296). Paths are camp-root-relative. Purely declarative — the
    /// runner never *reads* them as data, it only blake3s them and records the
    /// digests on [`StepStatus::input_hashes`] so a later reader can ask "is
    /// this result still about the bytes that produced it?"
    ///
    /// This is [`ImportConfig::hash`]'s pin re-pointed off [`StepKind::Import`]
    /// onto any step kind — the same guardrail, generalized. What it buys is
    /// **freshness**, the one property prose cannot track: W257's stale-ISO trap
    /// ("the only guard is habit: re-run `build-iso.sh` and reflash after **any**
    /// `.cfg` edit") is a computable relation the moment the build step declares
    /// `inputs = [".yah/infra/preseed/yah-x86-worker.cfg", …]`.
    ///
    /// **Staleness is computed, never stored.** No [`RunStatus`] variant means
    /// stale; the recorded digests are the only persisted half, and
    /// [`crate::staleness::input_freshness`] compares them against the tree at
    /// read time. That is why a stale badge cannot rot: it is a pure function of
    /// the tree plus the recorded run.
    ///
    /// Empty (the default) on every step that has never declared inputs — which
    /// is every step in every pipeline TOML written before this field existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<std::path::PathBuf>,
    /// Opt this step out of every capture path into the run journal (R717-T2,
    /// W296). When `true` the runner records **exit status and timings only**:
    /// no stdout/stderr lines on the event stream, no `$YAH_OUTPUTS` capture, no
    /// `StepStatus::error` stderr tail, no failure `msg` on `StepFinished`.
    ///
    /// Run journals are plain JSON under `.yah/jit/qed/` (`<run_id>.json` plus
    /// `<run_id>.events.jsonl`), so a step that handles a cluster KEK — W296's
    /// `kek-push` cell pipes one through `scp` — would otherwise leave the
    /// material, or a stderr tail quoting it, on disk in cleartext. That is a
    /// constraint, not a nicety.
    ///
    /// **A secret step is a sink, not a source.** It captures nothing, so it
    /// also *passes* nothing downstream: its outputs never reach
    /// `${{ steps.<name>.outputs.* }}`, because a downstream reference would
    /// land the value in that step's `argv`, which is emitted on `StepStarted`
    /// and lands in the journal anyway. If you need a value out of a secret
    /// step, that value is by definition not secret — split the cell.
    ///
    /// **What `secret` does NOT hide is what the step IS.** `argv`, `name`, and
    /// `cwd` are still emitted: they are the step's identity, and blanking them
    /// would make a failing secret step undebuggable. A step that would put a
    /// credential in its own `argv` is mis-shaped — pass it through `env` (whose
    /// *values* are never emitted; [`crate::events::credential_env_keys`] emits
    /// key names only) or a file.
    #[serde(default)]
    pub secret: bool,
    /// For `kind = gha-workflow` (W200-F9): path to a
    /// `.github/workflows/*.yml`, with optional event + dispatch inputs.
    /// Resolved relative to the camp root. Required when `kind = gha-workflow`;
    /// `validate()` rejects misconfiguration at parse time the same way
    /// `sub_pipeline` does.
    #[serde(default)]
    pub gha_workflow: Option<GhaWorkflowConfig>,
    /// For `kind = import` (W224, R533-F1): the imported `workflow.yml` source,
    /// its pinned blake3 content hash, and the virtual/materialize toggle.
    /// Required when `kind = import`; `validate()` rejects misconfiguration at
    /// parse time the same way `gha_workflow` / `sub_pipeline` do. The runner
    /// re-reads the source, recomputes its hash, and expands it into the native
    /// subgraph at plan time (`crate::import`).
    #[serde(default)]
    pub import: Option<ImportConfig>,
    /// Step-level matrix (R505). When present, [`crate::matrix::plan`] fans
    /// this single step out into N step instances within the parent job, each
    /// carrying its row's coord substituted into `argv` / `env` / `cwd` and
    /// its name suffixed with the coord pairs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "json-schema", schemars(schema_with = "crate::types::permissive_schema"))]
    pub matrix: Option<crate::matrix::MatrixSpec>,
    /// Declarative on/off switch (R506). When `false`, the runner skips the
    /// step at plan-time: a `StepStatus` with [`RunStatus::Skipped`] is still
    /// emitted so the dashboard renders the row, but no subprocess / container
    /// / sub-pipeline is launched. Defaults to `true`. Orthogonal to
    /// [`Self::activation`] — `enabled = false` means "I explicitly want this
    /// off for this run"; `status = "stubbed"` means "this is a planned but
    /// not-yet-implemented surface". The runner treats both as skip; the
    /// dashboard renders them distinctly.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Declarative lifecycle state (R506). `active` (the default) runs the
    /// step normally; `stubbed` marks the step as a visible-but-skipped row
    /// — typically a planned target (e.g. `ios-device`, `rpi0`) that hasn't
    /// been wired up yet but should still appear in the dashboard so bit-rot
    /// is observable. The runner skips `stubbed` steps the same way it skips
    /// `enabled = false` steps; the on-demand `--include-stubbed` override
    /// runs them.
    #[serde(default, rename = "status")]
    pub activation: StepActivation,
    /// Runtime conditional (R506) — a `${{ <expr> }}`-style expression
    /// evaluated against the W201-F4 context (matrix coords, env, prior
    /// `steps.<X>.outputs.<Y>`, the run's resolved `params.<name>` (R653-F1),
    /// plus `success()` / `failure()`). `params.<name>` is what makes a run
    /// param a build VARIANT rather than a substitution — a step gated
    /// `if = "params.variant == 'full'"` runs only for that value. When the
    /// expression evaluates to a falsy value the step is skipped at
    /// dispatch-time with [`RunStatus::Skipped`]. Bare expressions without
    /// `${{ }}` delimiters are evaluated as implicit-expression bodies (GHA
    /// semantics). Layered above [`Self::enabled`] / [`Self::activation`]:
    /// a step that is `enabled = false` is skipped before `if` is consulted.
    /// Layered above [`Self::on_fail`]: this gate is *pre-execution*, while
    /// `on_fail` is post-failure propagation.
    #[serde(default, rename = "if", skip_serializing_if = "Option::is_none")]
    pub if_cond: Option<String>,
    /// Run this step as a long-lived sidecar (R513-F2, W207 Gap #4). A
    /// background step is *spawned* — `run()` emits its `StepStarted`, kicks
    /// the subprocess onto its own task, and immediately advances to the next
    /// step instead of awaiting completion. The classic case is a server a
    /// later step talks to: `yah-camp`, `vite preview`, a mock auth broker.
    /// Without this every such step would block the pipeline forever.
    ///
    /// Lifecycle: the sidecar lives until it is *reaped*. With
    /// [`Self::background_until`] unset it is reaped at the end of the step
    /// loop (after the last foreground step, before terminal outcomes); with
    /// `background_until = "<step>"` it is reaped the moment that named step
    /// finishes. Reaping a still-running sidecar kills it (`kill_on_drop`) and
    /// records [`RunStatus::Success`] — a healthy server torn down on schedule
    /// is the expected path, not a failure. A sidecar that *exits on its own*
    /// before reap surfaces its real exit status: clean → `Success`, non-zero
    /// → `Failed` (a sidecar that crashes mid-pipeline is a genuine problem and
    /// flips the run to `Failed` so `on_fail` fires).
    ///
    /// Log story: a background step's stdout/stderr keep streaming as
    /// `StepOutput` events tagged with the step index, identical to a
    /// foreground step — a misbehaving sidecar's logs are exactly what you want
    /// when triaging, so v1 never silences them; collapsing a chatty sidecar's
    /// pane is a consumer concern.
    ///
    /// v1 scope: background is only valid on [`StepKind::Subprocess`] steps run
    /// locally (the [`crate::ForgeExecutor`] spawn path). `validate()` rejects
    /// other kinds; the runner rejects `--where=remote` background steps
    /// (yubaba-supervised remote sidecars are a separate lifecycle). Defaults
    /// to `false` — omitted from every existing pipeline.
    #[serde(default)]
    pub background: bool,
    /// Reap this background step right after the named step finishes, rather
    /// than at the end of the pipeline (R513-F2). Implies [`Self::background`].
    /// The named step must appear *after* this one in the pipeline — the runner
    /// rejects a forward-reference to a missing or earlier step at run start, so
    /// a typo fails loudly instead of silently deferring the reap to pipeline
    /// end. `None` (the default) ⇒ reap at end of the step loop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_until: Option<String>,
    /// The author's declaration that this step is *legitimately* long-running
    /// (R906-F1). Read by `qed.await`'s liveness tick: a tree whose deepest
    /// transition has not moved for the whole wait is normally a concern —
    /// "55m with no milestone means the pipeline needs finer ones" — and this
    /// is the one way to say "not here, and I meant it".
    ///
    /// A DECLARATION, not a measurement. QED has no duration model and should
    /// not acquire one: the measured spread across this camp's history is p50
    /// 6s / p99 46m, four orders of magnitude driven by cache state and machine
    /// contention rather than by step identity, so a learned per-step estimate
    /// would be noise with a confidence interval. A flat tree-transition
    /// timeout plus a reviewable opt-out beats that, for the same reason a fuse
    /// beats a model of your wiring.
    ///
    /// Set it only where a single step genuinely does an hour of uninterrupted
    /// work with nothing meaningful to report in between — the only case found
    /// in this camp's history is `build-v8-musl` (56-60m, three occurrences).
    /// If you are reaching for it because a step "sometimes" takes a while, the
    /// answer is smaller steps.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub expect_slow: bool,
    /// For `kind = wait-for` (R513-F3, W207 Gap #5): the network endpoint to
    /// poll and the timeout/interval budget. Required when `kind = wait-for`;
    /// `validate()` rejects misconfiguration (missing block, no target, both
    /// targets) at parse time the same way `sub_pipeline` / `gha_workflow` do.
    /// `None` for every other step kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_for: Option<WaitForConfig>,
    /// For `kind = manual` (R622, W282): what the human has to accomplish, the
    /// terminal commands to prefill for them, and the optional `advance`
    /// condition that lets the pipeline *verify* they did it. Required when
    /// `kind = manual`; `validate()` rejects a missing block / empty prompt at
    /// parse time the same way `wait_for` does. `None` for every other kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manual: Option<ManualConfig>,
    /// For `kind = manifest-stitch` (R590-F2): the arch-agnostic target tag and
    /// the per-arch source tags to fold into a multi-arch manifest list.
    /// Required when `kind = manifest-stitch`; `validate()` rejects a missing
    /// block / empty target / no sources at parse time the same way `wait_for`
    /// does. `None` for every other step kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_stitch: Option<ManifestStitchConfig>,
    /// Structured platform intent (R531-F2, W222): what target this step
    /// produces and the arch of the base image it pulls. `host` is *not*
    /// declared here — it's self-detected per runner (R531-T1) and composed
    /// in at plan time via [`crate::platform::Platform::compose`]. `None` (the
    /// default, omitted from every existing pipeline file) means host-native /
    /// no foreign-arch container — the common case. An explicit
    /// `platform.target` overrides the legacy per-kind `triple` field as the
    /// composed target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<crate::platform::PlatformSpec>,
    /// Per-step toolchain pin overrides (R507, W208 pillar 3). An inline table
    /// `toolchain.<tool> = "..."` whose entries [`crate::toolchain::effective_pins`]
    /// overlays on the pipeline-level `[pipeline.toolchain]` — so a single
    /// `build-android` step can pin `ndk = "r26d"` while the pipeline pins
    /// `r27`. `None` (the default) ⇒ the step inherits the pipeline pins
    /// unchanged. See [`crate::toolchain`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "json-schema", schemars(schema_with = "crate::types::permissive_schema"))]
    pub toolchain: Option<crate::toolchain::ToolchainSpec>,
    /// Steps that must finish before this one may start (R605-F3). The edge
    /// set that turns `steps` from a list into a DAG — see [`crate::dag`] for
    /// the full model and [`crate::dag::waves`] for how it is grouped.
    ///
    /// Three states, and the distinction between the first two is the whole
    /// backwards-compatibility story:
    ///
    /// - **absent** (`None`) — implicit chain: depends on the immediately
    ///   preceding step. Every pipeline TOML written before this field existed
    ///   omits it on every step, so every such pipeline is the same strict
    ///   serial chain it always was, with the same event stream. Reading an
    ///   absent `needs` as "no dependencies" would have made the entire
    ///   existing corpus fan out at once.
    /// - **`needs = []`** — a root: no dependencies, ready immediately. Not the
    ///   same statement as saying nothing, which is why the field is an
    ///   `Option` rather than a plain `Vec`. An imported workflow has one root
    ///   per independent branch.
    /// - **`needs = ["a", "b"]`** — exactly `a` and `b`, and nothing implicit.
    ///
    /// An entry matches a step by `name`, or by the `"<name> [k=v …]"` shape
    /// [`crate::matrix::plan`] gives a fanned-out matrix step — so
    /// `needs = ["build"]` joins on every row, as `needs:` does in GHA.
    ///
    /// A [`background`](Self::background) step counts as satisfied the moment
    /// it is *spawned*, not when it exits: a sidecar has no exit to wait for,
    /// so `needs = ["server"]` means "after the server is up-ish" and is
    /// normally paired with a `kind = "wait-for"` gate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub needs: Option<Vec<String>>,
    /// A shared resource this step monopolizes while it runs (R605-F3). Two
    /// steps declaring the same key never execute concurrently, even when the
    /// [`needs`](Self::needs) graph says they are independent and the run's
    /// [`max_parallel`](Pipeline::max_parallel) budget would allow it.
    ///
    /// This is [`Pipeline::concurrency_key`] one level down, and it exists for
    /// the same reason: the steps of one run share a host, a cargo `target/`
    /// and a docker daemon, so "independent in the DAG" does not imply
    /// "independent on the disk". Two branches that both run `cargo build` are
    /// genuinely parallel in the graph and would spend the whole time in
    /// cargo's file lock; `resource = "cargo-target"` on both says so.
    ///
    /// `None` (the default, and every pre-existing step) means the step is
    /// bounded only by `max_parallel`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    /// R823-F2 — which participant of the pipeline's
    /// [`participants`](Pipeline::participants) set this step belongs to.
    ///
    /// Names a role in `[pipeline.participants.role.<name>]`. Two consequences,
    /// and only these two:
    ///
    /// - **Placement.** A step whose role declares a `node` is dispatched to
    ///   that named node — pinned, never tag-matched, because the rendezvous
    ///   address has to be known before dispatch rather than decided by it.
    /// - **Identity.** The step is told which participant it is, via
    ///   [`ENV_PARTICIPANT_SELF`](crate::participants::ENV_PARTICIPANT_SELF),
    ///   so one binary can be either half of a case.
    ///
    /// The *set* is visible to every step of a participant run whether or not
    /// it declares this — a build step that has to bake an address in needs the
    /// map as much as the participant does. This field says who you are, not
    /// what you can see.
    ///
    /// Lifecycle is unchanged: a long-lived peer is still an ordinary
    /// [`background`](Self::background) step gated by
    /// [`background_until`](Self::background_until). The participant set adds
    /// addressing, a verdict rule and remote teardown — it does not add a
    /// second way to say "this runs in the background".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participant: Option<String>,
}

fn default_enabled() -> bool {
    true
}

/// Declarative lifecycle state for a [`QedStep`] (R506). See
/// [`QedStep::activation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum StepActivation {
    /// Step runs normally.
    #[default]
    Active,
    /// Step is a visible-but-skipped placeholder — appears in the dashboard
    /// so the full release surface is observable, but the runner doesn't
    /// dispatch it. Use for planned targets that aren't wired up yet.
    /// Overridden by the on-demand `--include-stubbed` runner flag.
    Stubbed,
}

/// What a pipeline step does.
///
/// On the TOML side this is `kind = "subprocess" | "build-image"`. The default
/// — and the value omitted from every existing pipeline file — is
/// [`StepKind::Subprocess`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum StepKind {
    /// Run `argv` (the existing semantics).
    #[default]
    Subprocess,
    /// Build a container image from the catalog (R381).  The image is looked
    /// up by `image` (catalog name); the runner materialises a
    /// `task::ForgeCommand::BuildImage` from the catalog entry.
    BuildImage,
    /// Package a static musl Rust binary + workload-spec manifest into a
    /// `.tar.gz` for the native runtime under Kamaji (R407-T2, W154).
    /// Catalog entry referenced by `image` must declare
    /// [`ProduceTarget::NativeTarball`](crate::images::ProduceTarget::NativeTarball);
    /// `binary_path` points at the cross-compiled binary an earlier step
    /// produced. No systemd unit is emitted — Kamaji directly
    /// fork+exec+cgroup+pidfd-supervises the binary at deploy time.
    PackageNativeTarball,
    /// Gate a workspace member against
    /// [`crate::preflight::KNOWN_GLIBC_ONLY_CRATES`] (R407-T3, W154). Walks
    /// the package's transitive dep closure via `cargo metadata`; fails if
    /// any glibc-only crate appears. Routes the pipeline author to the
    /// container fallback (`runtime = "container"`) with a clear,
    /// actionable error rather than dying mid-cross-build with a linker
    /// error. Pure host file I/O — no remote variant.
    MuslStaticPreflight,
    /// Sign a native tarball produced by an earlier
    /// [`StepKind::PackageNativeTarball`] step (R407-T5, W154). Extends the
    /// Sigstore keyless-OIDC trust model already used for OCI images to the
    /// native-tarball artifact shape via `cosign sign-blob`. The step
    /// resolves the on-disk tarball path the same way packaging writes it
    /// (`<camp_root>/.yah/cache/native/<image>-<triple>.tar.gz`) and emits
    /// `<tarball>.sig`, `<tarball>.crt`, and `<tarball>.bundle` next to it.
    /// Catalog entry referenced by `image` must declare
    /// [`ProduceTarget::NativeTarball`](crate::images::ProduceTarget::NativeTarball).
    /// Pure host file I/O — runs Native even on Remote runners.
    SignNativeTarball,
    /// Invoke another pipeline as a child of this step (W201). Resolution
    /// target + propagation rules live on [`QedStep::sub_pipeline`]. The
    /// runner runs the resolved child as a nested [`QedRun`] parented to the
    /// caller, then aggregates ProducedArtifacts and named outputs per
    /// [`SubPipelineCollect`]. Has no `argv` / `runtime` of its own — runtime
    /// is whichever the child resolves to.
    SubPipeline,
    /// Run a `.github/workflows/*.yml` through the native W200 GHA runtime
    /// (W200-F9). Step config lives on [`QedStep::gha_workflow`]; the runner
    /// dispatches to `yah_qed_gha::execute_workflow`, then lifts each
    /// `yah_qed_gha::ProducedArtifact` into [`ProducedArtifact`] and aggregates
    /// into the parent's `Outcome::Publish` — same surface as a producing
    /// `Subprocess` step or a `SubPipeline` child with `propagate.produces`.
    GhaWorkflow,
    /// Import a `.github/workflows/*.yml` as a QED source and expand it into
    /// the native subgraph at plan time (W224 "import, don't emulate";
    /// R533-F1). Step config lives on [`QedStep::import`]: the source path, a
    /// blake3 content hash pinning that source, and a `materialize` toggle.
    ///
    /// Unlike [`StepKind::GhaWorkflow`] — which treats the YAML as a foreign
    /// runtime to execute as one black-box step — `Import` treats it as an
    /// *interchange format*. The expansion is **virtual by default**
    /// (recomputed at plan time, never persisted ⇒ zero drift by
    /// construction); the pinned hash is the guardrail that detects a drifted
    /// source. The expansion logic lives in [`crate::import`]; F1's expansion
    /// delegates to the recast W200 GHA front-end, and R533-F4 swaps in the
    /// mechanical tier-1/2 → native map.
    Import,
    /// Block until a network endpoint becomes reachable, then advance (R513-F3,
    /// W207 Gap #5). The classic case is a health-gate between a `background`
    /// sidecar (`yah-camp`, `vite preview`) and the step that talks to it: poll
    /// the server's `/health` until it answers, so the consumer step never races
    /// a not-yet-listening port. Config (the target + timeout/interval) lives on
    /// [`QedStep::wait_for`]; the step runs no `argv` of its own and produces
    /// nothing — it is a pure gate. `validate()` rejects `argv` and a missing
    /// `[wait_for]` block the same way [`StepKind::SubPipeline`] does.
    WaitFor,
    /// Stitch N per-arch images (already pushed by earlier `build-image` steps
    /// routed to arch-matched build-workers) into one multi-arch manifest list
    /// (R590-F2). Config lives on [`QedStep::manifest_stitch`]: the arch-agnostic
    /// `target` tag consumers pull, and the arch-specific `sources` to fold in.
    /// The step shells `docker buildx imagetools create` — a registry-only
    /// operation, so it runs host-native even under `--where=remote` (the fleet
    /// does the builds; the stitch runs where qed runs). No `argv` of its own;
    /// `validate()` rejects `argv` and requires a `[manifest_stitch]` block with
    /// a target + at least one source.
    ManifestStitch,
    /// Park the run on a **human** and advance when they say so (R622, W282).
    /// The pure-gate sibling of [`StepKind::WaitFor`]: it runs no `argv` of its
    /// own, produces nothing, and blocks until a condition is met — except the
    /// condition is a person rather than a socket. Config lives on
    /// [`QedStep::manual`]: the prompt, the terminal commands to prefill, an
    /// optional `advance` shell condition, and an advisory checklist.
    ///
    /// The human surface is the AnswerQueue (W111): the step mints a `Form` and
    /// parks on it, so durability, notification, and the approve/revise
    /// affordance all come from the primitive the camp already has. The runner
    /// itself only knows a [`crate::runner::ManualGate`] — the headless
    /// (`yah qed run`) path installs no gate and resolves the step from
    /// `advance` alone.
    ///
    /// A parked step **releases its `concurrency_key`** and reacquires it to
    /// resume: holding `cargo-target` through an overnight park would stall
    /// every cargo pipeline in the camp. The tree can therefore move while
    /// parked, which is why resume re-evaluates `advance` instead of blindly
    /// continuing.
    ///
    /// A manual step is a *coordination* point, not an authorization gate — it
    /// does not know who answered and makes no claim they were entitled to.
    Manual,
}

/// Maximum allowed sub-pipeline nesting depth, counted as the number of
/// SubPipeline edges traversed from the root. Beyond this, [`validate_sub_pipeline_graph`]
/// rejects with [`SubPipelineError::MaxDepthExceeded`] regardless of cycles.
/// Defends against accidental recursion in user-authored TOML; 4 is plenty
/// for full-release → (gha-runtime + desktop-release + ...) layouts.
pub const MAX_SUB_PIPELINE_DEPTH: usize = 4;

/// Configuration for a [`StepKind::SubPipeline`] step — what to invoke and
/// what to roll up. Lives on [`QedStep::sub_pipeline`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct SubPipelineConfig {
    /// What to resolve and run as the child.
    pub target: SubPipelineRef,
    /// Pipeline-level params forwarded to the child (becomes the child's
    /// [`Pipeline::apply_params`] input).
    #[serde(default)]
    pub params: HashMap<String, String>,
    /// What to collect back up from the child run.
    #[serde(default)]
    pub propagate: SubPipelineCollect,
    /// Opaque opt-out of transparent inlining (W223 R532-F3). A wrapped
    /// pipeline is a *disregarded entity* by default — its children (GHA jobs,
    /// or a child pipeline's steps) are attributed to this step's report +
    /// graph as inlined rows. Set `opaque = true` to keep the wrapper a single
    /// black-box node instead: the child still runs and its status still rolls
    /// up, but the per-child rows are suppressed (the `#[inline(never)]`
    /// equivalent). Useful for a stable, rarely-failing sub-stage or a vendored
    /// workflow whose internals are noise. Default `false` (transparent).
    #[serde(default)]
    pub opaque: bool,
    /// Which tree this child builds in — **required whenever the two
    /// answers differ** (R887), unset otherwise.
    ///
    /// - `true`: the child calls its own `prepare_workspace` against the
    ///   PARENT's positioned tree as the git base (so `git worktree add`
    ///   resolves the child's target ref — normally HEAD, i.e. whatever the
    ///   parent just committed/tagged — from the same repository the parent
    ///   is standing in, not a second clone), positioning per its OWN
    ///   pipeline's declared [`WorkspaceMode`].
    /// - `false`: W224/R533-F11 inheritance — skip positioning entirely and
    ///   build from the parent's already-positioned tree, discarding the
    ///   child's own `workspace` declaration. Correct for a child meant to
    ///   share the parent's exact tree, including a `Live` parent that has
    ///   already MUTATED it (`release-wizard`'s `npm-publish` must publish
    ///   the version `version-bump` wrote and has not yet committed).
    /// - unset: inherit, exactly as `false` — *unless* the child declares a
    ///   [`WorkspaceMode`] it would not actually get, which
    ///   [`validate_sub_pipeline_graph`] refuses at load time
    ///   ([`SubPipelineError::WorkspaceInheritanceUnstated`]).
    ///
    /// **Why unset is an error in that one case, rather than a default.**
    /// Both answers are right somewhere and neither is right often enough to
    /// default to: `oss-publish` under the `Live` wizard must build committed
    /// bytes (`true`), `npm-publish` under the same wizard must build the
    /// uncommitted bump (`false`). A silent default therefore mis-composes
    /// half the recipes that hit it, and it did — R755 found `oss-publish`
    /// publishing from the wizard's live tree, 2026-09-04 found every
    /// published dmg bundled from `desktop-channel`'s inherited live tree,
    /// and R887 found `yah-cli-release`'s `stamp-build-id` dirty check
    /// reading the camp root's 38 uncommitted files inside what its own
    /// error message called an impossible state. Three occurrences, one
    /// cause, each fixed by hand on one step while the next composition site
    /// inherited the same trap. Stating it is one word at the call site and
    /// the only thing that makes the mistake unrepresentable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub own_workspace: Option<bool>,
}

/// How a [`StepKind::SubPipeline`] step resolves to a runnable child. The
/// TOML serializer renders this as one of four single-key tables:
///
/// ```toml
/// target = { builtin = "desktop-release" }
/// # or
/// target = { path = ".yah/qed/full-release.toml" }
/// # or
/// target = { gha-workflow = { path = ".github/workflows/release.yml", event = "tag" } }
/// # or
/// target = { peer = { camp = "mesofact", pipeline = "release-build" } }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum SubPipelineRef {
    /// Resolve to a builtin pipeline by name (e.g. `"desktop-release"`).
    Builtin(String),
    /// Resolve to a TOML pipeline file, relative to the camp root
    /// (e.g. `.yah/qed/full-release.toml`).
    Path(std::path::PathBuf),
    /// Resolve to a `.github/workflows/*.yml` executed by the W200 native
    /// GHA runtime. The runner-side glue lands in W201-F6; until then a
    /// resolver returning `None` here is the expected behaviour.
    GhaWorkflow {
        path: std::path::PathBuf,
        #[serde(default)]
        event: Option<String>,
        #[serde(default)]
        inputs: HashMap<String, String>,
    },
    /// Resolve to a pipeline declared in another camp on the same rig (or
    /// brokered to a remote rig via kamaji when the peer registry entry
    /// has a `rig` field). `camp` is the registry key in
    /// `<qed_dir>/peers.toml`; `pipeline` is the named pipeline within that
    /// camp's own `.yah/qed/`. Runner-side resolution lives in R494-F2.
    Peer { camp: String, pipeline: String },
}

/// What the parent rolls up from a SubPipeline child run.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct SubPipelineCollect {
    /// When `true`, [`ProducedArtifact`]s from the child are aggregated into
    /// the parent's [`Outcome::Publish`] and the child's own publish is
    /// suppressed — one stage/sync/revalidate at the parent's terminal
    /// outcome instead of N at the children.
    #[serde(default)]
    pub produces: bool,
    /// Named child outputs to expose on the parent step as
    /// `steps.<step-name>.outputs.<name>` for sibling references (W201-F4).
    /// The runner scans all child steps' collected outputs for each listed
    /// name and surfaces the value under the SubPipeline step's own name so
    /// later steps can reference `${{ steps.<this>.outputs.<name> }}`.
    #[serde(default)]
    pub outputs: Vec<String>,
}

/// Step-level config for [`StepKind::GhaWorkflow`] (W200-F9). Mirrors
/// [`SubPipelineRef::GhaWorkflow`] field-for-field — the SubPipeline-rooted
/// variant goes through a resolver that synthesizes a single GhaWorkflow
/// step under the hood, so both surfaces resolve to the same runner arm.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct GhaWorkflowConfig {
    /// Workflow YAML path, resolved relative to the camp root (e.g.
    /// `.github/workflows/release.yml`).
    pub path: std::path::PathBuf,
    /// GHA event the workflow run impersonates (`push`, `workflow_dispatch`).
    /// `None` defaults to `push` at runtime — matches `release.yml`'s tag-push
    /// primary trigger.
    #[serde(default)]
    pub event: Option<String>,
    /// `workflow_dispatch` inputs, forwarded as `inputs.<name>` in the
    /// expression context. Ignored when `event != "workflow_dispatch"`.
    #[serde(default)]
    pub inputs: HashMap<String, String>,
    /// Narrow the wrapped workflow's matrix fan-out by dimension VALUE, so a
    /// pipeline can wrap a multi-row workflow and run one row:
    ///
    /// ```toml
    /// [pipeline.steps.gha_workflow]
    /// path   = ".github/workflows/appliance-image.yml"
    /// matrix = { board = "{{board}}" }
    /// ```
    ///
    /// Values go through [`Pipeline::apply_params`], so a run param picks the
    /// row. A constraint over a dimension a job does not have is a no-op for that
    /// job — this narrows a fan-out, it does not disable jobs. Lowered onto
    /// `yah_qed_gha::Executor::matrix_filter`, where the full semantics live.
    ///
    /// Distinct from the run-time `selected_matrix_instances` RPC field, which is
    /// POSITIONAL (`build#0`) because the dashboard seeds it from an observed run.
    /// A checked-in pipeline must not depend on row order: inserting a value into
    /// the workflow's matrix silently repoints every index after it.
    #[serde(default)]
    pub matrix: HashMap<String, String>,
}

/// Step-level config for [`StepKind::Import`] (W224, R533-F1). The W224 import
/// primitive: a QED step whose source is a `workflow.yml`, carrying the content
/// hash of that yml plus a toggle for whether the expansion is persisted.
///
/// ```toml
/// [[steps]]
/// name = "release"
/// kind = "import"
/// [steps.import]
/// source = ".github/workflows/release.yml"
/// hash = "af1349b9f5f9a1a6a0404dea36dcc949..."  # blake3 of the source, pinned
/// # materialize = false                          # default — virtual expansion
/// ```
///
/// Whether the expansion is persisted is the migration ramp (W224): virtual
/// (default) recomputes the subgraph at plan time and stores nothing — zero
/// drift by construction; `materialize` ejects it to generated TOML (R533-F6).
/// While the yml is canonical the TOML is virtual; once ejected the yml is
/// gone — never two editable canonical copies at once. The freshness check and
/// plan-time expansion live in [`crate::import`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct ImportConfig {
    /// Path to the imported `.github/workflows/*.yml`, resolved relative to the
    /// camp root.
    pub source: std::path::PathBuf,
    /// blake3 content hash of the `source` bytes, pinned at import time
    /// ([`crate::import::content_hash`]). `None` while unpinned (a first import
    /// or a hand-authored block). On every run the runner recomputes the source
    /// hash and compares via [`Self::freshness`]: a mismatch means the source
    /// drifted since pinning. Under the default virtual expansion a mismatch is
    /// benign (re-expand + re-pin); for a materialized eject it marks the
    /// on-disk generated TOML stale (R533-F6).
    #[serde(default)]
    pub hash: Option<String>,
    /// Persist the plan-time expansion as generated, hash-stamped TOML (the
    /// R533-F6 `eject`), vs. the default virtual expansion computed fresh at
    /// plan time and never stored. Virtual-by-default is zero-drift by
    /// construction (W224). F1 only carries the toggle; the eject/materialize
    /// machinery and its stale-source guard land in R533-F6.
    #[serde(default)]
    pub materialize: bool,
    /// GHA event the expansion impersonates while F1's expansion still routes
    /// through the recast W200 front-end (`push` | `workflow_dispatch`). `None`
    /// defaults to `push` at runtime — matches `release.yml`'s tag-push primary
    /// trigger. Forwarded into the synthesized [`GhaWorkflowConfig`] by
    /// [`crate::import::expand_import`].
    #[serde(default)]
    pub event: Option<String>,
    /// `workflow_dispatch` inputs forwarded into the expansion context. Ignored
    /// when `event != "workflow_dispatch"`.
    #[serde(default)]
    pub inputs: HashMap<String, String>,
}

/// Step-level config for [`StepKind::WaitFor`] (R513-F3, W207 Gap #5). Names a
/// single thing to poll and the time budget for it to become healthy.
///
/// ```toml
/// [[steps]]
/// name = "wait:ready"
/// kind = "wait-for"
/// [steps.wait_for]
/// http = "http://localhost:3000/health"   # plaintext HTTP GET, healthy on 2xx/3xx
/// timeout_secs = 30                        # give up (and fail the step) after this
/// # interval_ms = 500                      # initial poll cadence (default 500ms)
/// # backoff_multiplier = 1.0               # >1.0 grows the interval each miss
/// # max_interval_ms = 500                  # cap on the grown interval
/// # expect_status = 200                    # require an exact status instead of any 2xx/3xx
/// ```
///
/// Exactly one of [`Self::http`] / [`Self::tcp`] / [`Self::shell`] must be
/// set. `http` is a dependency-free plaintext HTTP/1.1 GET (no TLS — an
/// `https://` URL is rejected at runtime; use `shell` with `curl`, or a `tcp`
/// gate); `tcp` is a bare connect to `host:port`, healthy the moment the port
/// accepts; `shell` runs `sh -c <command>`, healthy on exit 0 — the escape
/// hatch for anything the other two can't express (HTTPS, an npm-registry
/// check, a DB query). [`Self::expect_status`] is HTTP-only.
///
/// The actual probe/backoff loop lives in the standalone `pleasehold` crate
/// (`oss/qed/crates/pleasehold`) so a non-qed caller (e.g. almanac) can poll
/// the same way without depending on qed's scheduler stack.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct WaitForConfig {
    /// Plaintext-HTTP URL to GET each poll (e.g. `http://localhost:3000/health`).
    /// Healthy on a 2xx/3xx response, or on an exact match to
    /// [`Self::expect_status`] when set. Mutually exclusive with [`Self::tcp`]
    /// and [`Self::shell`].
    #[serde(default)]
    pub http: Option<String>,
    /// `host:port` to connect to each poll (e.g. `127.0.0.1:5432`). Healthy the
    /// moment the connect succeeds — no bytes are exchanged. Mutually exclusive
    /// with [`Self::http`] and [`Self::shell`].
    #[serde(default)]
    pub tcp: Option<String>,
    /// Shell command to run each poll (`sh -c <command>`), healthy on exit 0.
    /// Mutually exclusive with [`Self::http`] and [`Self::tcp`].
    #[serde(default)]
    pub shell: Option<String>,
    /// Require this exact HTTP status to consider the endpoint healthy, instead
    /// of the default "any 2xx/3xx". HTTP-only — `validate()` rejects it
    /// alongside a `tcp` or `shell` target. `None` ⇒ any 2xx/3xx.
    #[serde(default)]
    pub expect_status: Option<u16>,
    /// Total budget, in seconds, for the endpoint to become healthy. The step
    /// fails with a clear "never became healthy" message once this elapses.
    /// Defaults to 30s.
    #[serde(default = "default_wait_timeout_secs")]
    pub timeout_secs: u64,
    /// Delay before the second attempt, in milliseconds (the first attempt is
    /// immediate). Defaults to 500ms — snappy enough for a fast-booting dev
    /// server without hammering the socket.
    #[serde(default = "default_wait_interval_ms")]
    pub interval_ms: u64,
    /// Each failed attempt's delay is multiplied by this for the next one.
    /// Defaults to `1.0` — a fixed interval, the original `wait-for` behavior.
    /// Set `> 1.0` for exponential backoff (e.g. an eventually-consistent
    /// registry publish, where hammering every 500ms for 3 minutes is wasted
    /// traffic and a slow-growing interval is a better citizen).
    #[serde(default = "default_wait_backoff_multiplier")]
    pub backoff_multiplier: f64,
    /// Cap on the grown interval, in milliseconds, however large
    /// `backoff_multiplier` is. Defaults to [`Self::interval_ms`] (i.e. no
    /// growth) when unset.
    #[serde(default)]
    pub max_interval_ms: Option<u64>,
}

fn default_wait_timeout_secs() -> u64 {
    30
}

fn default_wait_interval_ms() -> u64 {
    500
}

fn default_wait_backoff_multiplier() -> f64 {
    1.0
}

impl WaitForConfig {
    /// `true` when an `https://` URL was given — TLS health-gates are out of
    /// scope for v1 (no HTTP client / TLS stack pulled into qed). The runner
    /// surfaces this as a clean `StepFailed` rather than silently trying a
    /// plaintext GET against a TLS port.
    pub fn http_is_tls(&self) -> bool {
        self.http
            .as_deref()
            .is_some_and(|u| u.trim_start().starts_with("https://"))
    }

    pub fn max_interval_ms(&self) -> u64 {
        self.max_interval_ms.unwrap_or(self.interval_ms)
    }
}

impl Default for WaitForConfig {
    fn default() -> Self {
        WaitForConfig {
            http: None,
            tcp: None,
            shell: None,
            expect_status: None,
            timeout_secs: default_wait_timeout_secs(),
            interval_ms: default_wait_interval_ms(),
            backoff_multiplier: default_wait_backoff_multiplier(),
            max_interval_ms: None,
        }
    }
}

/// Env var carrying this run's id, set in a [`StepKind::Manual`] step's
/// `advance` environment (`probe_manual_advance`, qed runner.rs) alongside
/// [`ENV_MANUAL_STEP_NAME`] (R605-B19). Lets a predicate distinguish "a tag
/// this SAME run cut" from "a tag a PRIOR run cut and left lying around" —
/// the gap that forced R605-B17's on-disk nonce file to exist in the first
/// place. Not injected into ordinary (non-manual) subprocess steps; those
/// already have `${{ steps.*.outputs.* }}` for carrying values forward.
pub const ENV_MANUAL_RUN_ID: &str = "QED_RUN_ID";

/// Env var naming the manual step itself, alongside [`ENV_MANUAL_RUN_ID`] —
/// distinguishes multiple manual steps in one pipeline (e.g. `commit-and-tag`
/// vs `push-tag`) that might otherwise reuse a similarly-shaped predicate.
pub const ENV_MANUAL_STEP_NAME: &str = "QED_STEP_NAME";

/// Who a [`StepKind::Manual`] gate is addressed to (R906-F1).
///
/// The classifying rule, and it is not a matter of taste: **an agent may answer
/// a gate that asks "is this done / is this right"; it may never answer one
/// that asks "may I".** The second kind authorizes the irreversible work that
/// follows, and a thing cannot authorize itself.
///
/// [`Self::Agent`] is the default, which inverts what QED did before this
/// existed — every `kind = "manual"` step was implicitly a human's, and a
/// pipeline could grow as many of them as it liked with nothing saying so
/// (`yah-release-wizard` is deliberately down to exactly one operator gate,
/// `authorize-release`, plus `roll-the-fleet` as an agent-audience manual step).
/// Most gates are in fact the first kind: "the version bump is right", "the tag
/// points at the freeze commit". Those are judgements a supervising agent can
/// make and, with an `advance` predicate, *prove*.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ManualAudience {
    /// A supervising agent may answer this gate. Woken by `qed.await` with
    /// reason `needs_agent`: do the work, satisfy `advance`, then release the
    /// park with `qed.resume`.
    #[default]
    Agent,
    /// Only a human may answer. `qed.await` reports `blocked_on_operator` and a
    /// supervising agent must notify and re-park rather than answer.
    ///
    /// Position matters as much as the flag: an operator gate means "did you
    /// mean to run this, here are the facts", so it belongs at the front of a
    /// pipeline, before anything expensive has been spent. See
    /// [`Pipeline::lint_operator_gates`] for the rule that enforces it (R906-F2)
    /// and [`Pipeline::allow_late_operator_block`] for the opt-out.
    Operator,
}

/// Step-level config for [`StepKind::Manual`] (R622, W282). Describes the
/// human's half of a pipeline: what they must accomplish, what to put in front
/// of them, and how the pipeline confirms they did it.
///
/// ```toml
/// [[pipeline.steps]]
/// name = "commit-and-tag"
/// kind = "manual"
/// [pipeline.steps.manual]
/// prompt = "Review the version bump, commit it, and tag the release."
/// terminal = ["git status", "git diff --stat"]
/// advance = "git describe --tags --exact-match"
/// checklist = ["Diff reviewed", "Version matches intent"]
/// ```
///
/// [`Self::advance`] is the field that matters. Without it a manual step is a
/// button someone clicks to make the yellow box go away — it asserts nothing.
/// With it the pipeline *confirms the human actually did the thing* before
/// spending an irreversible step on it. Treat it as strongly encouraged; a
/// manual step without one should be rare and deliberate.
///
/// [`ENV_MANUAL_RUN_ID`] and [`ENV_MANUAL_STEP_NAME`] are set in `advance`'s
/// environment (R605-B19) so a predicate can ask "did THIS run do the thing"
/// directly, rather than smuggling that question through a file on disk —
/// R605-B17 had to invent exactly such a nonce-file protocol before this
/// existed, because neither a run id nor `${{ steps.*.outputs.* }}`
/// substitution reached `advance` at all.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct ManualConfig {
    /// Rendered on the run card / answer form. Say what the human must
    /// accomplish, not how — the `terminal` prefill covers the how.
    pub prompt: String,
    /// Commands to prefill terminal tiles with. **Not auto-run** — the human
    /// reads, then executes. A pipeline that silently runs `git` commands on
    /// someone's behalf is the opposite of what a manual step is for.
    ///
    /// Rendered into the form's `framing` as a ```sh fence, which the desktop
    /// Markdown renderer already turns into a run-button + inline terminal.
    #[serde(default)]
    pub terminal: Vec<String>,
    /// Shell condition that proves the human did the thing. Polled while
    /// parked (auto-advancing the moment it exits 0) and **re-evaluated on
    /// resume** — the tree can move during a park, so a resume is not a bare
    /// continue. On a non-zero exit after a human answer the step re-parks
    /// carrying the failing command and its stderr.
    ///
    /// Run through `sh -c` in the pipeline workspace, with
    /// [`ENV_MANUAL_RUN_ID`]/[`ENV_MANUAL_STEP_NAME`] in its environment
    /// (R605-B19). `None` ⇒ honour-system advance on the human's word alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advance: Option<String>,
    /// Advisory checkboxes on the card. Purely informational — they gate
    /// nothing (that is [`Self::advance`]'s job).
    #[serde(default)]
    pub checklist: Vec<String>,
    /// Cadence, in seconds, for polling [`Self::advance`] while parked.
    /// Defaults to 5s — a park spans human time, so there is nothing to gain
    /// from a tighter loop and a `git describe` per second is pure noise.
    /// Ignored when `advance` is `None` (nothing to poll).
    #[serde(default = "default_manual_advance_poll_secs")]
    pub advance_poll_secs: u64,
    /// Who may answer this gate (R906-F1). Defaults to
    /// [`ManualAudience::Agent`] — see that type for the classifying rule and
    /// for why the default is the way round it is.
    #[serde(default)]
    pub audience: ManualAudience,
}

/// `pub(crate)` so [`crate::doc_source`] can lower a `manual` cell without
/// hardcoding the same number in a second place (R717-T11).
pub(crate) fn default_manual_advance_poll_secs() -> u64 {
    5
}

/// Step-level config for [`StepKind::ManifestStitch`] (R590-F2). Names the
/// arch-agnostic manifest-list tag to publish and the per-arch source tags to
/// fold into it.
///
/// ```toml
/// [[steps]]
/// name = "stitch:multi-arch"
/// kind = "manifest-stitch"
/// [steps.manifest_stitch]
/// target = "ghcr.io/yah-ai/yah-rust:v1"
/// sources = [
///   "ghcr.io/yah-ai/yah-rust:v1-amd64",   # pushed by the amd64 build-worker
///   "ghcr.io/yah-ai/yah-rust:v1-arm64",   # pushed by the arm64 build-worker
/// ]
/// ```
///
/// `sources` are the arch-specific tags earlier `build-image` steps pushed to
/// the registry; `target` is the tag consumers pull (docker resolves the arch
/// at pull time from the manifest list). `validate()` requires a non-empty
/// `target` and at least one `source`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct ManifestStitchConfig {
    /// Arch-agnostic manifest-list tag to create/overwrite.
    pub target: String,
    /// Per-arch source image tags to fold into the manifest list. Must be
    /// already pushed to their registry before this step runs.
    #[serde(default)]
    pub sources: Vec<String>,
}

/// Declares a named output that a native [`StepKind::Subprocess`] step may
/// emit at runtime (W201-F4).
///
/// At runtime the runner injects a `$YAH_OUTPUTS` environment variable
/// pointing at a temporary file. Steps write `KEY=VALUE\n` lines to that
/// file; the runner reads them back after the step exits and stores the
/// collected values in [`StepStatus::outputs`]. Sibling steps can then
/// reference values as `${{ steps.<step-name>.outputs.<key> }}` in their
/// `argv` or `env` fields.
///
/// The `name` field is advisory — undeclared keys written to `$YAH_OUTPUTS`
/// are captured too. Declaring outputs explicitly helps with documentation
/// and, once the W200 expression engine (R487-F2) lands, with type-checked
/// expression validation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct OutputDecl {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// W209: declared value type. The runner type-checks the captured value
    /// against this shape before letting it reach any `[[bind]]` whose
    /// `from` references this output. Defaults to `string` (i.e. accept
    /// anything non-empty) for backwards compatibility with R488-F4 outputs
    /// declared without a `type` key.
    #[serde(rename = "type", default = "default_value_type")]
    #[cfg_attr(feature = "json-schema", schemars(schema_with = "crate::types::permissive_schema"))]
    pub kind: manifest_bind::ValueType,
    /// Optional override regex for the type's built-in validator (W209).
    /// Authors rarely need this — the built-in shape regex is right by
    /// construction for blake3-hex, semver, oci-digest, etc.
    #[serde(default)]
    pub validate: Option<String>,
}

fn default_value_type() -> manifest_bind::ValueType {
    manifest_bind::ValueType::String
}

/// Errors surfaced when walking a sub-pipeline graph at parse time
/// ([`validate_sub_pipeline_graph`]).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SubPipelineError {
    #[error("sub-pipeline cycle detected: {chain}")]
    Cycle { chain: String },
    #[error("sub-pipeline nesting exceeded max depth of {max}: {chain}")]
    MaxDepthExceeded { max: usize, chain: String },
    /// R887: the child declares a [`WorkspaceMode`] that inheritance would
    /// silently discard, and the step does not say which it meant. See
    /// [`SubPipelineConfig::own_workspace`] for why this is not defaulted.
    #[error(
        "step `{step}` runs `{child}`, which declares workspace = \"{child_mode}\" — but a \
         sub-pipeline child inherits its parent's tree, and this one would build \"{inherited}\" \
         instead. Say which you meant in the step's [sub_pipeline] block: \
         `own_workspace = true` (position the child's own {child_mode} tree — what a release \
         child that must build COMMITTED bytes wants) or `own_workspace = false` (inherit the \
         parent's {inherited} tree — what a child that must see the parent's uncommitted \
         mutations wants)"
    )]
    WorkspaceInheritanceUnstated {
        step: String,
        child: String,
        child_mode: String,
        inherited: String,
    },
}

/// Resolver hook for [`validate_sub_pipeline_graph`]. The validator calls
/// `resolve` for each [`SubPipelineRef`] it encounters; returning `Some`
/// continues the walk into the child, `None` stops walking that subtree
/// (the runtime will report the resolution failure later). This indirection
/// keeps `types.rs` free of any dependency on the builtin registry or
/// filesystem — callers wire their own resolver.
pub trait SubPipelineResolver {
    fn resolve(&self, target: &SubPipelineRef) -> Option<Pipeline>;

    /// Optional companion to [`resolve`]: when `resolve` returns `None`,
    /// the runner consults this to surface a typed reason in the
    /// [`StepKind::SubPipeline`] step's `StepFailed.msg` rather than the
    /// generic "target unresolvable" fallback. Implementors return
    /// `Some(message)` when they can explain the miss (unknown registry
    /// entry, unsupported transport, missing on-disk file) and `None`
    /// when the miss has no actionable reason beyond "target not found".
    ///
    /// Used today by [`crate::config::LoaderSubPipelineResolver`] to
    /// surface the R494-T5 "remote peer not yet supported" path with the
    /// offending `camp` + `rig` names — operators previously got a silent
    /// skip + generic unresolvable error.
    fn unresolved_reason(&self, _target: &SubPipelineRef) -> Option<String> {
        None
    }

    /// The camp root that the resolved child pipeline's steps should
    /// execute against. The runner uses this as the child runner's
    /// `camp_root`, which becomes the working directory for subprocess
    /// steps (and the base for resolving produced-artifact paths).
    ///
    /// For [`SubPipelineRef::Peer`] this is the *peer* camp's root, so a
    /// peer's `cargo` steps run in the peer's workspace rather than the
    /// parent camp's — without this, `peer-binaries` runs yubaba's
    /// `cargo publish -p workload-spec` from yah's root and fails with a
    /// "package ID did not match any packages" error.
    ///
    /// Returns `None` to inherit the parent runner's `camp_root` — the
    /// correct default for `Builtin`/`Path`/`GhaWorkflow` children, which
    /// share the parent's camp.
    fn resolved_camp_root(&self, _target: &SubPipelineRef) -> Option<std::path::PathBuf> {
        None
    }
}

/// Walk a pipeline's SubPipeline graph, rejecting cycles and nesting deeper
/// than [`MAX_SUB_PIPELINE_DEPTH`]. The walker tracks the chain of visited
/// targets by their canonical string form (`builtin:<name>` / `path:<path>` /
/// `gha:<path>`); seeing the same token twice on the active chain is a cycle.
/// Unresolved targets are *not* errors here — that's a runtime resolution
/// concern; the validator only enforces structural properties.
pub fn validate_sub_pipeline_graph(
    pipeline: &Pipeline,
    resolver: &dyn SubPipelineResolver,
) -> Result<(), SubPipelineError> {
    let root = format!("pipeline:{}", pipeline.name);
    let mut chain: Vec<String> = vec![root];
    visit_sub_pipeline(pipeline, resolver, &mut chain, pipeline.workspace)
}

/// `inherited` is the [`WorkspaceMode`] `pipeline`'s own steps actually run
/// under — its declared mode at the root, and whatever its ancestors handed
/// down for a child that did not reposition (see
/// [`SubPipelineConfig::own_workspace`]).
fn visit_sub_pipeline(
    pipeline: &Pipeline,
    resolver: &dyn SubPipelineResolver,
    chain: &mut Vec<String>,
    inherited: WorkspaceMode,
) -> Result<(), SubPipelineError> {
    for step in &pipeline.steps {
        if step.kind != StepKind::SubPipeline {
            continue;
        }
        let Some(cfg) = step.sub_pipeline.as_ref() else {
            // Caught by `QedStep::validate` (SubPipelineMissingConfig); ignore here.
            continue;
        };
        let token = sub_pipeline_ref_token(&cfg.target);
        if chain.contains(&token) {
            let mut full = chain.clone();
            full.push(token);
            return Err(SubPipelineError::Cycle {
                chain: full.join(" -> "),
            });
        }
        // Depth counts SubPipeline edges traversed (chain.len() - 1 = root + edges).
        if chain.len() > MAX_SUB_PIPELINE_DEPTH {
            let mut full = chain.clone();
            full.push(token);
            return Err(SubPipelineError::MaxDepthExceeded {
                max: MAX_SUB_PIPELINE_DEPTH,
                chain: full.join(" -> "),
            });
        }
        chain.push(token);
        if let Some(child) = resolver.resolve(&cfg.target) {
            // R887: a child that declares `Isolated` and would inherit
            // something else has to say which it meant. Deliberately narrow:
            // ONLY `Isolated`, because that is the mode an author writes on
            // purpose (a release cuts from committed bytes) and the only one
            // whose silent loss has shipped wrong artifacts — three times.
            // `Checkout` is excluded even though it is equally "not
            // inherited": it is the serde DEFAULT for an undeclared
            // `[pipeline] workspace`, so firing on it would demand a decision
            // on every child whose author never thought about workspaces at
            // all, and "unset" cannot be told from "chose the default" once
            // the TOML is parsed.
            if child.workspace == WorkspaceMode::Isolated
                && inherited != WorkspaceMode::Isolated
                && cfg.own_workspace.is_none()
            {
                return Err(SubPipelineError::WorkspaceInheritanceUnstated {
                    step: step.name.clone(),
                    child: child.name.clone(),
                    child_mode: workspace_mode_key(child.workspace).to_string(),
                    inherited: workspace_mode_key(inherited).to_string(),
                });
            }
            let child_inherits = if cfg.own_workspace == Some(true) {
                child.workspace
            } else {
                inherited
            };
            visit_sub_pipeline(&child, resolver, chain, child_inherits)?;
        }
        chain.pop();
    }
    Ok(())
}

/// Stable string representation of a [`SubPipelineRef`] used for chip
/// rendering on the wire (`QedEvent::SubPipelineStarted.target`,
/// `QedStepWire.sub_pipeline_target`). One of:
/// `builtin:<name>` | `path:<rel>` | `gha:<rel>` | `peer:<camp>:<pipeline>`.
pub fn sub_pipeline_ref_token(target: &SubPipelineRef) -> String {
    match target {
        SubPipelineRef::Builtin(name) => format!("builtin:{name}"),
        SubPipelineRef::Path(path) => format!("path:{}", path.display()),
        SubPipelineRef::GhaWorkflow { path, .. } => format!("gha:{}", path.display()),
        SubPipelineRef::Peer { camp, pipeline } => format!("peer:{camp}:{pipeline}"),
    }
}

/// Program basenames that mean "this step compiles something" (R906-F2).
///
/// **Derived from `argv`, deliberately.** The two alternatives were both
/// rejected: a new per-step `irreversible`/`expensive` marker is a field every
/// author must remember to set (and the pipelines that most need this rule are
/// exactly the ones nobody re-reads), and `concurrency_key = "cargo-target"` is
/// a *convention* for serializing against the shared target dir, not a
/// guarantee that the step compiles or that a compiling step declares it. The
/// argv is the one description of a step that cannot be out of date with what
/// the step actually runs.
///
/// Matched against the **basename** of each whitespace-separated token in every
/// `argv` element, so `/usr/bin/cargo`, `sh -c "cargo build"` and
/// `["bun", "run", "build"]` all hit.
pub const COMPILER_PROGRAMS: &[&str] = &[
    "cargo", "rustc", "bun", "bunx", "tauri", "npm", "pnpm", "yarn", "tsc", "cc", "clang", "gcc",
    "go",
];

/// Basename of a shell token — the last path segment, either separator.
fn program_basename(token: &str) -> &str {
    token.rsplit(['/', '\\']).next().unwrap_or(token)
}

/// The first entry of [`COMPILER_PROGRAMS`] this step's `argv` invokes, if any.
pub fn compiler_invoked_by(step: &QedStep) -> Option<&'static str> {
    step.argv
        .iter()
        .flat_map(|element| element.split_whitespace())
        .find_map(|token| {
            let base = program_basename(token);
            COMPILER_PROGRAMS.iter().copied().find(|p| *p == base)
        })
}

/// What the operator-gate rule found wrong with a pipeline (R906-F2).
///
/// Returned rather than logged so the rule has a test surface;
/// [`crate::config::PipelineLoader::load_and_validate_graph`] renders each one
/// through `tracing::warn!`. These are **warnings at authoring time** — a
/// pipeline nobody intends to run unattended is legitimate, so the loader never
/// hard-errors on one. The runtime half
/// ([`crate::runner::PipelineRunner`]'s manual arm) is where a late operator
/// gate actually fails a run, and it asks the same predicate
/// ([`operator_gate_is_late`]) so the two cannot drift.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorGateFinding {
    /// Something compiles before the gate is reached.
    Late {
        /// The `audience = "operator"` step.
        gate: String,
        /// The step whose `argv` invokes a compiler.
        blocker: String,
        /// `Some(name)` when `blocker` lives inside a sub-pipeline rather than
        /// in the pipeline that declares the gate.
        blocker_pipeline: Option<String>,
        /// Which entry of [`COMPILER_PROGRAMS`] matched.
        program: String,
    },
    /// The gate is not the first step that parks on a human.
    NotFirstManual {
        gate: String,
        earlier: String,
        earlier_pipeline: Option<String>,
    },
    /// More than one `audience = "operator"` step in one pipeline.
    Duplicate { first: String, extra: String },
}

impl std::fmt::Display for OperatorGateFinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn located(step: &str, pipeline: &Option<String>) -> String {
            match pipeline {
                Some(p) => format!("`{step}` (in sub-pipeline `{p}`)"),
                None => format!("`{step}`"),
            }
        }
        match self {
            Self::Late {
                gate,
                blocker,
                blocker_pipeline,
                program,
            } => write!(
                f,
                "operator gate `{gate}` is late: {} invokes `{program}` before it. \
                 An operator gate asks \"did you mean to run this\"; behind a compile it \
                 instead asks a human to come back at an unknown depth into an unbounded \
                 run, and everything spent before the answer is spent on a run nobody \
                 authorized. Move the gate ahead of {}, or set \
                 `allow_late_operator_block = true` on the pipeline if it is meant to be \
                 attended throughout.",
                located(blocker, blocker_pipeline),
                located(blocker, blocker_pipeline),
            ),
            Self::NotFirstManual {
                gate,
                earlier,
                earlier_pipeline,
            } => write!(
                f,
                "operator gate `{gate}` is not the first manual step: {} parks first. \
                 A human answering the second gate has already been asked once, so the \
                 authorization the operator gate is for was not the run's first question. \
                 Move the gate ahead of {}, or set `allow_late_operator_block = true` on \
                 the pipeline.",
                located(earlier, earlier_pipeline),
                located(earlier, earlier_pipeline),
            ),
            Self::Duplicate { first, extra } => write!(
                f,
                "pipeline declares more than one `audience = \"operator\"` manual step \
                 (`{first}` and `{extra}`): an operator gate authorizes the run, and a run \
                 is authorized once. Make `{extra}` `audience = \"agent\"` (an agent may \
                 answer \"is this done / is this right\"), or set \
                 `allow_late_operator_block = true` on the pipeline."
            ),
        }
    }
}

/// Where something was found ahead of an operator gate: the step's name, and
/// the sub-pipeline it lives in (`None` = the gate's own pipeline).
type Located = (String, Option<String>);

/// **The** operator-gate predicate (R906-F2). Both enforcement points call
/// this one function: [`Pipeline::lint_operator_gates`] at authoring time and
/// the runner's manual arm at run time. Two parallel implementations of
/// "is this gate late?" would drift, and the drift would be invisible — a
/// pipeline that warns but runs, or runs but warns.
///
/// `gate` names an `audience = "operator"` step in `pipeline`. Returns `None`
/// when the gate is fine, when the pipeline sets
/// [`Pipeline::allow_late_operator_block`], or when no step of that name is
/// present (a resume-from-step run hands the runner a drained pipeline; a
/// missing name is not evidence of lateness).
///
/// # What counts as "before"
///
/// Every step declared ahead of the gate, plus every transitive `needs`
/// predecessor of it, minus everything that transitively depends on the gate.
/// On the implicit chain — which is every pipeline that declares no `needs` —
/// that is exactly execution order. On an explicit DAG it also catches a step
/// that merely *may* run concurrently with the gate, which is the right answer:
/// a compile racing the human has still been spent by the time they answer.
///
/// # Sub-pipelines are the whole point
///
/// A `kind = "sub-pipeline"` step ahead of the gate is descended into, because
/// the expensive thing is routinely a child's: `yah-release-wizard`'s gate
/// follows `version-bump`, whose child pipeline runs `cargo run -p xtask`. A
/// rule that only read the parent's own `argv` would call that pipeline clean.
pub fn operator_gate_is_late(
    pipeline: &Pipeline,
    gate: &str,
    resolver: &dyn SubPipelineResolver,
) -> Option<OperatorGateFinding> {
    if pipeline.allow_late_operator_block {
        return None;
    }
    let gate_index = pipeline.steps.iter().position(|s| s.name == gate)?;
    let before = steps_before(&pipeline.steps, gate_index);
    let ahead: Vec<&QedStep> = before.iter().map(|&i| &pipeline.steps[i]).collect();

    let mut chain = vec![format!("pipeline:{}", pipeline.name)];
    let mut compiler: Option<(Located, &'static str)> = None;
    let mut manual: Option<Located> = None;
    scan_for_blockers(
        &ahead,
        None,
        resolver,
        &mut chain,
        &mut compiler,
        &mut manual,
    );

    if let Some(((blocker, blocker_pipeline), program)) = compiler {
        return Some(OperatorGateFinding::Late {
            gate: gate.to_string(),
            blocker,
            blocker_pipeline,
            program: program.to_string(),
        });
    }
    if let Some((earlier, earlier_pipeline)) = manual {
        return Some(OperatorGateFinding::NotFirstManual {
            gate: gate.to_string(),
            earlier,
            earlier_pipeline,
        });
    }
    None
}

/// Indices that run before — or alongside — `gate`. See
/// [`operator_gate_is_late`] for why "alongside" counts.
fn steps_before(steps: &[QedStep], gate: usize) -> Vec<usize> {
    let mut set: std::collections::BTreeSet<usize> = (0..gate).collect();
    if let Ok(preds) = crate::dag::predecessors(steps, crate::dag::Missing::Satisfied) {
        let mut frontier = vec![gate];
        let mut seen: std::collections::HashSet<usize> = std::collections::HashSet::new();
        while let Some(cur) = frontier.pop() {
            for &p in &preds[cur] {
                if seen.insert(p) {
                    set.insert(p);
                    frontier.push(p);
                }
            }
        }
        // A step declared earlier that `needs` the gate genuinely runs after it.
        for after in crate::dag::dependents(&preds, gate) {
            set.remove(&after);
        }
    }
    set.remove(&gate);
    set.into_iter().collect()
}

/// Walk `steps` (and, for sub-pipeline steps, their children) recording the
/// first compiler invocation and the first manual step seen. Stops descending
/// on a repeated target or past [`MAX_SUB_PIPELINE_DEPTH`] — cycles are already
/// rejected by [`validate_sub_pipeline_graph`] on the load path, but the
/// runtime caller has no such guarantee and a validator must not hang.
fn scan_for_blockers(
    steps: &[&QedStep],
    child_of: Option<&str>,
    resolver: &dyn SubPipelineResolver,
    chain: &mut Vec<String>,
    compiler: &mut Option<(Located, &'static str)>,
    manual: &mut Option<Located>,
) {
    for step in steps {
        if compiler.is_none() {
            if let Some(program) = compiler_invoked_by(step) {
                *compiler = Some(((step.name.clone(), child_of.map(str::to_string)), program));
            }
        }
        if manual.is_none() && step.kind == StepKind::Manual {
            *manual = Some((step.name.clone(), child_of.map(str::to_string)));
        }
        if compiler.is_some() && manual.is_some() {
            return;
        }
        if step.kind != StepKind::SubPipeline {
            continue;
        }
        let Some(cfg) = step.sub_pipeline.as_ref() else {
            continue;
        };
        let token = sub_pipeline_ref_token(&cfg.target);
        if chain.contains(&token) || chain.len() > MAX_SUB_PIPELINE_DEPTH {
            continue;
        }
        let Some(child) = resolver.resolve(&cfg.target) else {
            continue;
        };
        chain.push(token);
        let child_steps: Vec<&QedStep> = child.steps.iter().collect();
        scan_for_blockers(
            &child_steps,
            Some(&child.name),
            resolver,
            chain,
            compiler,
            manual,
        );
        chain.pop();
    }
}

impl Pipeline {
    /// Check this pipeline's `audience = "operator"` manual gates (R906-F2).
    ///
    /// The three rules, in the order they are reported:
    /// 1. **At most one** operator gate per pipeline.
    /// 2. It must be the **first** manual step.
    /// 3. **Nothing before it may compile** — including inside sub-pipelines it
    ///    descends into.
    ///
    /// Rules 2 and 3 are [`operator_gate_is_late`], which the runner calls too.
    /// Rule 1's extras are reported as [`OperatorGateFinding::Duplicate`] and
    /// not additionally re-reported as not-first, which they trivially are.
    ///
    /// Findings are returned, never raised: authoring-time this is a warning
    /// (see [`OperatorGateFinding`]), and `allow_late_operator_block` silences
    /// the whole check.
    pub fn lint_operator_gates(
        &self,
        resolver: &dyn SubPipelineResolver,
    ) -> Vec<OperatorGateFinding> {
        if self.allow_late_operator_block {
            return Vec::new();
        }
        let gates: Vec<&str> = self
            .steps
            .iter()
            .filter(|s| {
                s.kind == StepKind::Manual
                    && s.manual
                        .as_ref()
                        .is_some_and(|m| m.audience == ManualAudience::Operator)
            })
            .map(|s| s.name.as_str())
            .collect();
        let Some(first) = gates.first().copied() else {
            return Vec::new();
        };
        let mut out: Vec<OperatorGateFinding> = gates[1..]
            .iter()
            .map(|extra| OperatorGateFinding::Duplicate {
                first: first.to_string(),
                extra: (*extra).to_string(),
            })
            .collect();
        if let Some(finding) = operator_gate_is_late(self, first, resolver) {
            out.push(finding);
        }
        out
    }
}

/// Validation errors surfaced before a pipeline runs.  Returned by
/// [`QedStep::validate`] and threaded through the TOML loader.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StepValidationError {
    #[error("step `{0}`: subprocess steps require non-empty `argv`")]
    SubprocessMissingArgv(String),
    #[error("step `{0}`: build-image steps must omit `argv`")]
    BuildImageHasArgv(String),
    #[error("step `{0}`: build-image steps require `image = \"<catalog-name>\"`")]
    BuildImageMissingImage(String),
    #[error(
        "step `{0}`: build-image steps must run in a container — \
         set `runtime = \"container\"` or omit `runtime` (drop `runtime = \"native\"`)"
    )]
    BuildImageNativeRuntime(String),
    /// `push = true` was set on a step whose tag's registry hostname isn't
    /// declared writable in `.yah/qed/registries.toml`. The fix is either
    /// drop `push = true` (default: OCI archive output, no registry needed)
    /// or add the registry to the camp's `registries.toml` with
    /// `writable = true`. Carries the step name and the host the tag pointed
    /// at so the operator can see exactly which entry to add.
    #[error(
        "step `{step}`: `push = true` targets registry `{host}` which is \
         not declared writable in `.yah/qed/registries.toml` — \
         add `[[registries]]` with `host = \"{host}\"` + `writable = true`, \
         or drop `push = true` to fall back to the OCI archive output"
    )]
    PushRequiresWritableRegistry { step: String, host: String },
    #[error("step `{0}`: package-native-tarball steps must omit `argv`")]
    PackageNativeTarballHasArgv(String),
    #[error("step `{0}`: package-native-tarball steps require `image = \"<catalog-name>\"`")]
    PackageNativeTarballMissingImage(String),
    #[error(
        "step `{0}`: package-native-tarball steps require `binary_path = \"<path>\"` \
         (the static musl binary produced by an earlier build step)"
    )]
    PackageNativeTarballMissingBinaryPath(String),
    #[error(
        "step `{0}`: package-native-tarball steps run native on the host (pure file I/O) — \
         drop `runtime = \"container\"` or set `runtime = \"native\"`"
    )]
    PackageNativeTarballContainerRuntime(String),
    #[error("step `{0}`: musl-static-preflight steps must omit `argv`")]
    MuslStaticPreflightHasArgv(String),
    #[error(
        "step `{0}`: musl-static-preflight steps require `package = \"<workspace-member>\"` \
         (e.g. `package = \"yubaba\"`)"
    )]
    MuslStaticPreflightMissingPackage(String),
    #[error(
        "step `{0}`: musl-static-preflight runs `cargo metadata` on the host — \
         drop `runtime = \"container\"` or set `runtime = \"native\"`"
    )]
    MuslStaticPreflightContainerRuntime(String),
    #[error("step `{0}`: sign-native-tarball steps must omit `argv`")]
    SignNativeTarballHasArgv(String),
    #[error("step `{0}`: sign-native-tarball steps require `image = \"<catalog-name>\"`")]
    SignNativeTarballMissingImage(String),
    #[error(
        "step `{0}`: sign-native-tarball runs `cosign sign-blob` on the host — \
         drop `runtime = \"container\"` or set `runtime = \"native\"`"
    )]
    SignNativeTarballContainerRuntime(String),
    #[error("step `{0}`: sub-pipeline steps must omit `argv`")]
    SubPipelineHasArgv(String),
    #[error("step `{0}`: sub-pipeline steps require a `[sub_pipeline]` block with `target = ...`")]
    SubPipelineMissingConfig(String),
    #[error(
        "step `{0}`: sub-pipeline steps must not declare `produces` directly — \
         ProducedArtifacts come from the child run; set \
         `sub_pipeline.propagate.produces = true` to aggregate them"
    )]
    SubPipelineHasProduces(String),
    #[error("step `{0}`: gha-workflow steps must omit `argv`")]
    GhaWorkflowHasArgv(String),
    #[error("step `{0}`: gha-workflow steps require a `[gha_workflow]` block with `path = ...`")]
    GhaWorkflowMissingConfig(String),
    #[error("step `{0}`: import steps must omit `argv`")]
    ImportHasArgv(String),
    #[error("step `{0}`: import steps require an `[import]` block with `source = \"...\"`")]
    ImportMissingConfig(String),
    #[error(
        "step `{0}`: `background` / `background_until` is only valid on subprocess \
         steps — a background sub-pipeline / gha-workflow / build-image sidecar \
         has no lifecycle yet (R513-F2)"
    )]
    BackgroundRequiresSubprocess(String),
    #[error(
        "step `{0}`: `secret = true` is only valid on subprocess steps — every other \
         kind's output is qed's own text (docker build progress, a wait-for probe's \
         verdict), so the runner has no step-authored capture there to suppress and \
         the flag would silently do nothing (R717-T2)"
    )]
    SecretRequiresSubprocess(String),
    /// R560-T8: `source_context` is the subprocess analogue of build-image's
    /// `context` + `context_url`. On a build-image step the pair already
    /// exists and this key would be a second, silently-ignored spelling of it;
    /// on every other kind there is no argv to consume the published URL, so
    /// the upload would be pure cost. Reject at parse time rather than
    /// uploading a tarball nothing fetches.
    #[error(
        "step `{0}`: `source_context` is a subprocess-only knob — a build-image \
         step ships its context via `context` + the R636-B1 `context_url` \
         transport, and no other step kind has an argv to fetch the published \
         tarball with"
    )]
    SourceContextRequiresSubprocess(String),
    /// R876-F4: `cache` binds a host dir into the step's *container*. Every
    /// other step kind either has no container (`wait-for`, `import`,
    /// `package-native-tarball`) or has its own caching story (`build-image` →
    /// BuildKit), so the mount would be created, bound, and never written —
    /// which is worse than no cache, because it looks like one.
    #[error(
        "step `{0}`: `cache = true` is a subprocess-only knob — it binds a \
         host-persistent dir at `/yah/cache` for the step's argv to point its \
         toolchain at, and no other step kind has an argv to use it (a \
         build-image step caches through BuildKit instead)"
    )]
    CacheRequiresSubprocess(String),
    /// R876-F4: the container half of the same argument. A native step has no
    /// mount namespace to bind into and its build scratch already persists on
    /// the host, so honoring `cache` there would mount nothing while reporting
    /// a cache — the silent-no-op this field is designed not to be.
    #[error(
        "step `{0}`: `cache = true` requires `runtime = \"container\"` — a native \
         step has no mount namespace for the bind and its build scratch already \
         lives on this filesystem, so the cache would be silently inert"
    )]
    CacheRequiresContainerRuntime(String),
    #[error(
        "step `{0}`: `secret = true` cannot be combined with declared `outputs` — a secret step's \
         $YAH_OUTPUTS is dropped unread, so the output would always be empty and any [[bind]] \
         reading it would bind nothing. If you need a value out of this step, that value is not \
         secret: split it into its own step (R717-T2)"
    )]
    SecretCannotDeclareOutputs(String),
    #[error("step `{0}`: wait-for steps must omit `argv` (a wait-for is a pure gate)")]
    WaitForHasArgv(String),
    #[error(
        "step `{0}`: wait-for steps require a `[wait_for]` block with `http = ...`, \
         `tcp = ...`, or `shell = ...`"
    )]
    WaitForMissingConfig(String),
    #[error(
        "step `{0}`: wait-for needs exactly one target — set `http = \"http://…\"`, \
         `tcp = \"host:port\"`, or `shell = \"...\"`, not neither"
    )]
    WaitForNeedsTarget(String),
    #[error(
        "step `{0}`: wait-for accepts only one target — set exactly one of `http`, \
         `tcp`, `shell`"
    )]
    WaitForAmbiguousTarget(String),
    #[error(
        "step `{0}`: `expect_status` only applies to an `http` wait-for — \
         a `tcp`/`shell` gate has no status to match"
    )]
    WaitForStatusNeedsHttp(String),
    #[error("step `{0}`: wait-for `timeout_secs` must be greater than zero")]
    WaitForZeroTimeout(String),
    #[error("step `{0}`: wait-for `interval_ms` must be greater than zero")]
    WaitForZeroInterval(String),
    #[error("step `{0}`: wait-for `backoff_multiplier` must be finite and >= 1.0 (1.0 = fixed interval)")]
    WaitForBadBackoffMultiplier(String),
    #[error("step `{0}`: manifest-stitch steps must omit `argv` (the stitch is a pure registry op)")]
    ManifestStitchHasArgv(String),
    #[error(
        "step `{0}`: manifest-stitch steps require a `[manifest_stitch]` block with \
         `target = \"...\"` and `sources = [...]`"
    )]
    ManifestStitchMissingConfig(String),
    #[error("step `{0}`: manifest-stitch requires a non-empty `target` manifest-list tag")]
    ManifestStitchMissingTarget(String),
    #[error(
        "step `{0}`: manifest-stitch requires at least one `sources` entry \
         (the per-arch tags to fold into the manifest list)"
    )]
    ManifestStitchNeedsSources(String),
    #[error("step `{0}`: manual steps must omit `argv` (a manual step is a pure gate — put the commands in `manual.terminal`, which the human runs, or in `manual.advance`, which verifies them)")]
    ManualHasArgv(String),
    #[error(
        "step `{0}`: manual steps require a `[manual]` block with `prompt = \"...\"` \
         (say what the human must accomplish)"
    )]
    ManualMissingConfig(String),
    #[error("step `{0}`: manual `prompt` must not be empty — an unlabelled gate is unanswerable")]
    ManualEmptyPrompt(String),
    #[error("step `{0}`: manual `advance` must not be blank — omit it entirely for an honour-system gate")]
    ManualBlankAdvance(String),
    #[error("step `{0}`: manual `advance_poll_secs` must be greater than zero")]
    ManualZeroPollInterval(String),
    #[error(
        "step `{0}`: a manual step parks on a human at the qed host and evaluates \
         `advance` there — drop `runtime = \"container\"` or set `runtime = \"native\"`"
    )]
    ManualContainerRuntime(String),
    #[error(
        "finally step `{0}`: v1 `[[finally]]` teardown supports only `kind = subprocess` \
         (and never `background`) — composite / image / sidecar teardown is a follow-up"
    )]
    FinallyRequiresSubprocess(String),
    /// R605-F3: `[[finally]]` steps are unconditional always-run teardown, run
    /// in declaration order after the main graph has drained. There is no
    /// readiness question for a `needs` to answer there, so the key would be
    /// inert config that reads as if it did something.
    #[error(
        "finally step `{0}`: `needs` has no meaning on always-run teardown — \
         `[[finally]]` steps run in declaration order after the step graph drains"
    )]
    FinallyCannotDeclareNeeds(String),
}

/// Deliberately **not** `#[derive(Default)]`.
///
/// `enabled` carries `#[serde(default = "default_enabled")]` = `true`, and a
/// derived `Default` would give it `false` — so `QedStep { name, argv,
/// ..Default::default() }` would build a step that the runner silently *skips*,
/// and a pipeline made only of such steps reports `Success` having run nothing.
/// (R633 hit exactly that: a synthesized image build "succeeded" in 40 ms.)
///
/// Round-tripping serde's own defaults makes the two definitions the same
/// definition, so a future `#[serde(default = …)]` on some other field cannot
/// reintroduce the divergence. `name` is the only field without a serde default.
impl Default for QedStep {
    fn default() -> Self {
        serde_json::from_str(r#"{"name":""}"#)
            .expect("every QedStep field but `name` has a serde default")
    }
}

impl QedStep {
    /// `true` when this step runs as a long-lived sidecar (R513-F2) — either
    /// `background = true` or a `background_until` target is set. See
    /// [`Self::background`] for the lifecycle.
    pub fn is_background(&self) -> bool {
        self.background || self.background_until.is_some()
    }

    /// Validate kind-specific invariants. Called by the TOML loader
    /// (`PipelineLoader::load_from_file`) before the pipeline reaches the
    /// runner — fail loudly at parse time, not at execution time.
    pub fn validate(&self) -> Result<(), StepValidationError> {
        // R513-F2: background is a Subprocess-only knob in v1. A background
        // sub-pipeline / gha-workflow / build-image has no spawn-and-detach
        // lifecycle yet — reject before the runner so the error names the
        // offending step at parse time rather than mid-run.
        if self.is_background() && self.kind != StepKind::Subprocess {
            return Err(StepValidationError::BackgroundRequiresSubprocess(
                self.name.clone(),
            ));
        }
        // R717-T2: `secret` is a Subprocess-only knob, for the same reason
        // `background` is — and rejecting it here is what makes the opt-out a
        // closed set rather than a best effort. The runner gates the three
        // subprocess output sinks (local native, local container, remote); every
        // other kind's output is qed's own text (docker build progress, a
        // wait-for probe's "healthy after 3s"), so accepting `secret` there
        // would promise a suppression the runner does not perform. Better to
        // fail at parse time than to ship a flag that silently does nothing on
        // the step an author actually put it on.
        if self.secret && self.kind != StepKind::Subprocess {
            return Err(StepValidationError::SecretRequiresSubprocess(
                self.name.clone(),
            ));
        }
        // R717-T2: a secret step is a SINK, not a source. Its `$YAH_OUTPUTS` is
        // dropped unread, so a declared output would be permanently empty and a
        // `[[bind]]` reading it would silently bind nothing. Reject the pair at
        // parse time — an author who wants a value out of a secret step is
        // telling us that value is not actually secret, and the fix is to split
        // the step, not to weaken the flag.
        if self.secret && !self.outputs.is_empty() {
            return Err(StepValidationError::SecretCannotDeclareOutputs(
                self.name.clone(),
            ));
        }
        // R560-T8: same closed-set discipline as `background` / `secret` above.
        // The runner only publishes a source context on the remote SUBPROCESS
        // path, because that is the only kind whose argv can fetch the URL.
        if !self.source_context.is_empty() && self.kind != StepKind::Subprocess {
            return Err(StepValidationError::SourceContextRequiresSubprocess(
                self.name.clone(),
            ));
        }
        // R876-F4: same closed-set discipline again, in two parts — the kind
        // has to have an argv to point at the mount, and the runtime has to
        // have a mount namespace to bind it into. Both refusals exist because
        // a cache that is declared and inert is strictly worse than none.
        if self.cache {
            if self.kind != StepKind::Subprocess {
                return Err(StepValidationError::CacheRequiresSubprocess(
                    self.name.clone(),
                ));
            }
            if self.runtime == Some(TaskRuntime::Native) {
                return Err(StepValidationError::CacheRequiresContainerRuntime(
                    self.name.clone(),
                ));
            }
        }
        match self.kind {
            StepKind::Subprocess => {
                if self.argv.is_empty() {
                    return Err(StepValidationError::SubprocessMissingArgv(
                        self.name.clone(),
                    ));
                }
                Ok(())
            }
            StepKind::BuildImage => {
                if !self.argv.is_empty() {
                    return Err(StepValidationError::BuildImageHasArgv(self.name.clone()));
                }
                if self.image.is_none() {
                    return Err(StepValidationError::BuildImageMissingImage(
                        self.name.clone(),
                    ));
                }
                if matches!(self.runtime, Some(TaskRuntime::Native)) {
                    return Err(StepValidationError::BuildImageNativeRuntime(
                        self.name.clone(),
                    ));
                }
                Ok(())
            }
            StepKind::PackageNativeTarball => {
                if !self.argv.is_empty() {
                    return Err(StepValidationError::PackageNativeTarballHasArgv(
                        self.name.clone(),
                    ));
                }
                if self.image.is_none() {
                    return Err(StepValidationError::PackageNativeTarballMissingImage(
                        self.name.clone(),
                    ));
                }
                if self.binary_path.is_none() {
                    return Err(StepValidationError::PackageNativeTarballMissingBinaryPath(
                        self.name.clone(),
                    ));
                }
                if matches!(self.runtime, Some(TaskRuntime::Container)) {
                    return Err(StepValidationError::PackageNativeTarballContainerRuntime(
                        self.name.clone(),
                    ));
                }
                Ok(())
            }
            StepKind::MuslStaticPreflight => {
                if !self.argv.is_empty() {
                    return Err(StepValidationError::MuslStaticPreflightHasArgv(
                        self.name.clone(),
                    ));
                }
                if self.package.is_none() {
                    return Err(StepValidationError::MuslStaticPreflightMissingPackage(
                        self.name.clone(),
                    ));
                }
                if matches!(self.runtime, Some(TaskRuntime::Container)) {
                    return Err(StepValidationError::MuslStaticPreflightContainerRuntime(
                        self.name.clone(),
                    ));
                }
                Ok(())
            }
            StepKind::SignNativeTarball => {
                if !self.argv.is_empty() {
                    return Err(StepValidationError::SignNativeTarballHasArgv(
                        self.name.clone(),
                    ));
                }
                if self.image.is_none() {
                    return Err(StepValidationError::SignNativeTarballMissingImage(
                        self.name.clone(),
                    ));
                }
                if matches!(self.runtime, Some(TaskRuntime::Container)) {
                    return Err(StepValidationError::SignNativeTarballContainerRuntime(
                        self.name.clone(),
                    ));
                }
                Ok(())
            }
            StepKind::SubPipeline => {
                if !self.argv.is_empty() {
                    return Err(StepValidationError::SubPipelineHasArgv(self.name.clone()));
                }
                if self.sub_pipeline.is_none() {
                    return Err(StepValidationError::SubPipelineMissingConfig(
                        self.name.clone(),
                    ));
                }
                if !self.produces.is_empty() {
                    return Err(StepValidationError::SubPipelineHasProduces(
                        self.name.clone(),
                    ));
                }
                Ok(())
            }
            StepKind::GhaWorkflow => {
                if !self.argv.is_empty() {
                    return Err(StepValidationError::GhaWorkflowHasArgv(self.name.clone()));
                }
                if self.gha_workflow.is_none() {
                    return Err(StepValidationError::GhaWorkflowMissingConfig(
                        self.name.clone(),
                    ));
                }
                Ok(())
            }
            StepKind::Import => {
                if !self.argv.is_empty() {
                    return Err(StepValidationError::ImportHasArgv(self.name.clone()));
                }
                if self.import.is_none() {
                    return Err(StepValidationError::ImportMissingConfig(self.name.clone()));
                }
                Ok(())
            }
            StepKind::WaitFor => {
                if !self.argv.is_empty() {
                    return Err(StepValidationError::WaitForHasArgv(self.name.clone()));
                }
                let Some(cfg) = self.wait_for.as_ref() else {
                    return Err(StepValidationError::WaitForMissingConfig(self.name.clone()));
                };
                match (cfg.http.is_some(), cfg.tcp.is_some(), cfg.shell.is_some()) {
                    (false, false, false) => {
                        return Err(StepValidationError::WaitForNeedsTarget(self.name.clone()));
                    }
                    (true, false, false) | (false, true, false) | (false, false, true) => {}
                    _ => {
                        return Err(StepValidationError::WaitForAmbiguousTarget(
                            self.name.clone(),
                        ));
                    }
                }
                if cfg.expect_status.is_some() && cfg.http.is_none() {
                    return Err(StepValidationError::WaitForStatusNeedsHttp(self.name.clone()));
                }
                if cfg.timeout_secs == 0 {
                    return Err(StepValidationError::WaitForZeroTimeout(self.name.clone()));
                }
                if cfg.interval_ms == 0 {
                    return Err(StepValidationError::WaitForZeroInterval(self.name.clone()));
                }
                if cfg.backoff_multiplier < 1.0 || !cfg.backoff_multiplier.is_finite() {
                    return Err(StepValidationError::WaitForBadBackoffMultiplier(
                        self.name.clone(),
                    ));
                }
                Ok(())
            }
            StepKind::Manual => {
                if !self.argv.is_empty() {
                    return Err(StepValidationError::ManualHasArgv(self.name.clone()));
                }
                let Some(cfg) = self.manual.as_ref() else {
                    return Err(StepValidationError::ManualMissingConfig(self.name.clone()));
                };
                if cfg.prompt.trim().is_empty() {
                    return Err(StepValidationError::ManualEmptyPrompt(self.name.clone()));
                }
                if cfg.advance.as_ref().is_some_and(|a| a.trim().is_empty()) {
                    return Err(StepValidationError::ManualBlankAdvance(self.name.clone()));
                }
                if cfg.advance_poll_secs == 0 {
                    return Err(StepValidationError::ManualZeroPollInterval(
                        self.name.clone(),
                    ));
                }
                // A manual step is a person at a keyboard on the qed host, and
                // `advance` is evaluated in the pipeline workspace on that same
                // host. A container has neither. Reject at parse time rather
                // than resolving to Container on a Remote runner and surprising
                // the author mid-release.
                if matches!(self.runtime, Some(TaskRuntime::Container)) {
                    return Err(StepValidationError::ManualContainerRuntime(
                        self.name.clone(),
                    ));
                }
                Ok(())
            }
            StepKind::ManifestStitch => {
                if !self.argv.is_empty() {
                    return Err(StepValidationError::ManifestStitchHasArgv(self.name.clone()));
                }
                let Some(cfg) = self.manifest_stitch.as_ref() else {
                    return Err(StepValidationError::ManifestStitchMissingConfig(
                        self.name.clone(),
                    ));
                };
                if cfg.target.trim().is_empty() {
                    return Err(StepValidationError::ManifestStitchMissingTarget(
                        self.name.clone(),
                    ));
                }
                if cfg.sources.is_empty() {
                    return Err(StepValidationError::ManifestStitchNeedsSources(
                        self.name.clone(),
                    ));
                }
                Ok(())
            }
        }
    }

    /// Validate a step that lives in a pipeline's `[[finally]]` teardown block
    /// (R513-F4). Runs the normal kind-specific [`Self::validate`] first, then
    /// enforces the v1 `finally`-only constraint: teardown is a plain
    /// [`StepKind::Subprocess`] and never a `background` sidecar (a detached
    /// teardown step has no one to reap it). Composite / image / sub-pipeline
    /// teardown is a documented follow-up.
    pub fn validate_finally(&self) -> Result<(), StepValidationError> {
        self.validate()?;
        if self.kind != StepKind::Subprocess || self.is_background() {
            return Err(StepValidationError::FinallyRequiresSubprocess(
                self.name.clone(),
            ));
        }
        if self.needs.is_some() {
            return Err(StepValidationError::FinallyCannotDeclareNeeds(
                self.name.clone(),
            ));
        }
        Ok(())
    }
}

/// One built artifact a step emits, addressed into the release channel as
/// `[<prefix>/]<binary>/<version>/<triple>/<filename>`.
///
/// The producer leg of the almanac releases feed (R330): the QED
/// `release-build` pipeline declares these on its build steps, and
/// [`Outcome::Publish`] copies them into the public-read channel bucket where
/// they double as the self-update pointer source AND almanac's `R2Channel`
/// input (see self-updating-binaries.md, `crates/yah/almanac/src/r2.rs`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct ProducedArtifact {
    /// Logical binary name — becomes the channel sub-path (`yah`, `desktop`,
    /// `camp`). The per-binary `release-manifest.json` lives at this root.
    pub binary: String,
    /// Path to the built file, resolved relative to the step's `cwd`
    /// (defaults to the workspace root). The basename becomes the channel
    /// filename.
    pub path: String,
    /// Target-triple shorthand (e.g. `darwin-aarch64`). `None` resolves to the
    /// build host's triple at publish time — GHA fans out one `yah qed run
    /// release-build` per platform, each publishing its own triple into the
    /// shared bucket.
    #[serde(default)]
    pub triple: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum OnFail {
    Abort,
    Continue,
    Retry { max: u32 },
}

impl Default for OnFail {
    fn default() -> Self {
        OnFail::Abort
    }
}

/// Why a run's params could not be resolved against the pipeline's declarations.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ParamError {
    /// One or more required params have neither a supplied value nor a default.
    /// Names every one of them, not just the first: a caller wiring up a
    /// five-param recipe wants one message, not five round trips.
    #[error(
        "pipeline '{pipeline}': required parameter(s) not provided and no default declared: {}\n\
         pass them with --param <name>=<value>, or give them `default = \"…\"` in the pipeline TOML",
        names.join(", ")
    )]
    MissingRequired {
        pipeline: String,
        names: Vec<String>,
    },
    /// A param declaring `options` was given a value outside that set.
    ///
    /// Rejecting (rather than merely not suggesting) is safe *because*
    /// `options` is opt-in: a pipeline that doesn't declare it cannot start
    /// failing, so no existing `--param` call changes behaviour. The moment an
    /// author writes `options = [...]` they are asserting the set is closed,
    /// and a typo'd variant should stop the run rather than silently substitute
    /// a value no step was written for.
    #[error(
        "pipeline '{pipeline}': parameter '{name}' = {value:?} is not one of its declared options: {}",
        options.join(", ")
    )]
    NotInOptions {
        pipeline: String,
        name: String,
        value: String,
        options: Vec<String>,
    },
    /// R751-F2: a run tried to override a param a specialization PINNED.
    ///
    /// Refused rather than silently ignored, because silently ignoring a
    /// `--param` is the worst of the three options: the operator gets a run
    /// that looks like it honoured them and didn't. Passing the *same* value
    /// is fine (that's what re-running a recorded param set does), so this
    /// only fires on a genuine disagreement.
    #[error(
        "pipeline '{pipeline}': parameter '{name}' is pinned to {pinned:?} by this specialization \
         and cannot be overridden (got {value:?})\n\
         run the base pipeline '{base}' directly to choose a different value"
    )]
    PinnedParamOverride {
        pipeline: String,
        base: String,
        name: String,
        pinned: String,
        value: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct ParamDef {
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub description: Option<String>,
    /// Value used when the run supplies none. This is what makes a param
    /// OPTIONAL in a usable way, because an absent param is not substituted at
    /// all — [`substitute`] leaves unknown placeholders alone, so a step whose
    /// `argv` says `{{features}}` ships the literal string `{{features}}` to the
    /// process it execs.
    ///
    /// That footgun is currently worked around by convention rather than fixed:
    /// `.yah/qed/release-build.toml` declares all five of its params
    /// `required` and documents that callers must "ALWAYS pass (empty string
    /// where N/A) … an omitted param would ship `{{features}}` straight into the
    /// cross argv". `default = ""` expresses that directly, and a param with a
    /// default no longer has to lie about being required.
    #[serde(default)]
    pub default: Option<String>,
    /// The closed set of legal values, e.g.
    /// `options = ["orangepi_zero2w", "rpi_zero2w"]`. Empty (the default) means
    /// the param is free-form text.
    ///
    /// This is what turns a param into a *variant selector* rather than a
    /// substitution: the operator surface renders a dropdown instead of a text
    /// box, and [`Pipeline::resolve_params`] rejects anything outside the set
    /// (see [`ParamError::NotInOptions`]). A `default` that isn't in `options`
    /// is an authoring error, caught at load time by the config loader.
    #[serde(default)]
    pub options: Vec<String>,
    /// A camp-relative path glob whose matches' **file stems** become
    /// [`Self::options`] at READ time — R717-T10, W296 §Q3:
    ///
    /// ```toml
    /// node = { required = true, options_from = ".yah/infra/machines/*.toml" }
    /// ```
    ///
    /// It **composes with** `options` rather than adding a second validation
    /// path: resolution *fills* `options`, so [`Pipeline::resolve_params`] and
    /// [`ParamError::NotInOptions`] keep working unchanged and the desktop
    /// renders the dropdown it already renders for a closed set.
    ///
    /// Read time, not load time, is the point: a machine added to
    /// `.yah/infra/machines/` shows up in the selector without anyone editing
    /// the doc. A glob naming a *directory convention* is also why this is not
    /// `kind = "machine"` — a domain word here would put fleet concepts in
    /// QED's param schema forever, while a glob is reusable by any other
    /// domain (`.yah/qed/*.toml`, `.yah/docs/working/W*.md`).
    ///
    /// Resolution lives in
    /// [`doc_source::resolve_options_from`](crate::doc_source::resolve_options_from);
    /// a glob matching nothing is an authoring error there, never a silent
    /// degrade to free text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options_from: Option<String>,
    /// A shell command whose non-empty stdout lines become [`Self::options`]
    /// at READ time, the first line also becoming [`Self::default`] — for a
    /// closed set that is computed rather than enumerated from files, e.g. the
    /// release wizard's next patch/minor/major version off `Cargo.toml`:
    ///
    /// ```toml
    /// spec = { options_cmd = "scripts/release-versions.sh" }
    /// ```
    ///
    /// Run from the camp root by
    /// [`PipelineLoader::load`](crate::config::PipelineLoader::load) (and a
    /// doc's `resolved_params`), so the catalog, the desktop dropdown and
    /// [`Pipeline::resolve_params`] all read the same filled list. It owns the
    /// list outright: `options`, `options_from` or `default` beside it is a
    /// load error, not a merge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options_cmd: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum Outcome {
    YubabaDeploy {
        service: String,
        env: String,
    },
    AlmanacRun {
        pipeline: String,
    },
    /// Publish the artifacts declared by the run's successful steps
    /// (`QedStep::produces`) into a release channel bucket, then fire the
    /// almanac revalidate hook (R330-F3). This is the producer leg of the
    /// data-driven releases feed.
    Publish {
        /// Storage provider — `"r2"` today (Cloudflare R2 via the S3 API,
        /// reusing the cloud crate's `publish_to_r2`).
        provider: String,
        /// Destination bucket (public-read channel), e.g. `"yah-releases"`.
        bucket: String,
        /// Optional key prefix within the bucket. Channel keys are laid out
        /// as `[<prefix>/]<binary>/<version>/<triple>/<filename>`.
        #[serde(default)]
        prefix: Option<String>,
        /// Public-facing root used to write absolute download URLs into the
        /// emitted `release-manifest.json` (e.g. `"https://releases.yah.dev"`).
        /// When `None`, manifest URLs are written as bucket-relative keys.
        #[serde(default)]
        base_url: Option<String>,
        /// R876-F6: when `true`, this outcome refuses to dispatch unless
        /// `YAH_RELEASE_VERSION` is explicitly set — the workspace-version
        /// fallback in [`crate::publish::resolve_release_version`] is NOT
        /// consulted for this outcome. Pipelines that double as an iteration
        /// loop (`mesofact-musl`, run repeatedly with no args) set this so a
        /// green build never cuts a release under the dev workspace version by
        /// accident; pipelines whose only purpose IS a release act
        /// (`yah-cli-release`, run deliberately) leave it `false` and keep the
        /// existing fallback-to-workspace-version behaviour.
        #[serde(default)]
        require_explicit_version: bool,
        /// R560-F15: publish per LEG rather than per run. A leg is one
        /// weakly-connected component of the step `needs` graph
        /// ([`crate::dag::legs`]) — e.g. mesofact-musl's x86 and arm builds,
        /// each a root with no shared step. When `true`:
        ///
        ///   * a step failure aborts only its own leg (its not-yet-admitted
        ///     steps are never run); the other legs keep going;
        ///   * if the run ends Failed, this outcome still fires, carrying only
        ///     the artifacts of legs whose every step succeeded or was skipped.
        ///     The run still reports `Failed` and `on_fail` still fires, so a
        ///     broken leg never reads as a green release.
        ///
        /// Every other `on_success` outcome keeps requiring the whole run to
        /// succeed. A pipeline with no explicit `needs` is one chain, hence one
        /// leg, so the flag changes nothing there. Do not set it when a failing
        /// leg is meant to GATE the release (a parallel test leg): outcomes
        /// cannot declare `needs`, so per-leg publish cannot see that gate.
        #[serde(default)]
        per_leg: bool,
    },
    /// Dispatch a named vendor release adapter (R509) — Apple notarize/staple,
    /// Authenticode sign, Sparkle appcast, TestFlight/Play/GitHub upload.
    /// Resolved by `provider` name through the runner's
    /// [`crate::provider::ProviderRegistry`]; credentials resolve through the
    /// secrets bridge. Unlike [`Outcome::Publish`] (which syncs a staged tree
    /// to a bucket), these adapters transform an artifact in place or block on a
    /// remote vendor ticket — see [`crate::provider`]. A pipeline may chain
    /// several (`notarize` then `sparkle`): each adapter's transformed
    /// artifacts feed the next outcome's input set.
    Provider {
        /// Adapter name in the [`crate::provider::ProviderRegistry`]
        /// (`"notarize"`, `"authenticode"`, `"sparkle"`, …).
        provider: String,
        /// Vendor-specific config blob (the outcome's `with = { … }` table),
        /// opaque here — each adapter deserializes its own typed config.
        #[serde(default)]
        with: serde_json::Value,
        /// Public-facing root for absolute URLs an adapter emits (appcast feed
        /// base, release page). `None` leaves URL construction to the adapter.
        #[serde(default)]
        base_url: Option<String>,
    },
}

impl Outcome {
    /// The `kind = "…"` token this variant is declared as in a pipeline TOML.
    /// Kept beside the `#[serde(rename_all = "kebab-case")]` that produces it,
    /// so the two cannot drift into different spellings of one outcome.
    pub fn kind_str(&self) -> &'static str {
        match self {
            Outcome::YubabaDeploy { .. } => "yubaba-deploy",
            Outcome::AlmanacRun { .. } => "almanac-run",
            Outcome::Publish { .. } => "publish",
            Outcome::Provider { .. } => "provider",
        }
    }

    /// Does dispatching this outcome change something OUTSIDE this machine?
    /// (R876-B8 — the property `yah qed run`'s build-skew gate keys on.)
    ///
    /// Every variant answers `true` today, and that is not an accident of the
    /// current set: an `Outcome` *is* qed's name for a terminal side effect on
    /// the world — pushing bytes to a bucket, handing an artifact to a vendor,
    /// moving a live service, purging a public cache. A pipeline that only
    /// builds declares none, which is exactly the distinction the gate needs.
    ///
    /// It is written as an EXHAUSTIVE MATCH RATHER THAN `true` on purpose. A
    /// future inward-facing outcome (something that only writes local state)
    /// must be classified deliberately here, and until someone does, adding a
    /// variant fails to compile. R876-B8 exists because a safety gate silently
    /// failed to apply to the thing that actually runs pipelines; a wildcard
    /// arm here would be the same bug in miniature, quietly widening the gate's
    /// blind spot every time the enum grows.
    pub fn is_outward_facing(&self) -> bool {
        match self {
            // Writes release artifacts to a public bucket (cdn.yah.dev), which
            // `install.sh` resolves for `curl | sh`. The R876-F6 hazard itself.
            Outcome::Publish { .. } => true,
            // Hands artifacts to vendor services — notarize, Authenticode,
            // Sparkle appcast, TestFlight/Play/GitHub upload.
            Outcome::Provider { .. } => true,
            // Moves a live service in the camp's cloud.
            Outcome::YubabaDeploy { .. } => true,
            // Fires the almanac revalidate hook, which purges a public site's
            // cache. Classified outward on that basis; no pipeline in this camp
            // currently declares it as an outcome, so the call is free today
            // and is recorded here rather than left to be re-derived.
            Outcome::AlmanacRun { .. } => true,
        }
    }
}

impl Pipeline {
    /// The `kind` tokens of every terminal outcome this pipeline would dispatch
    /// that reaches outside this machine (R876-B8). Empty for a build-only
    /// pipeline.
    ///
    /// Both lists are consulted, not just `on_success`: `dispatch_terminal_outcomes`
    /// selects one of the two from the run's status, so an outward effect
    /// declared under `on_fail` is equally reachable — and a gate that read only
    /// the success list would be exactly the "installed but does not apply"
    /// shape this ticket is about.
    pub fn outward_facing_outcomes(&self) -> Vec<&'static str> {
        let mut kinds: Vec<&'static str> = self
            .on_success
            .iter()
            .chain(self.on_fail.iter())
            .filter(|o| o.is_outward_facing())
            .map(|o| o.kind_str())
            .collect();
        kinds.dedup();
        kinds
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QedRunMeta {
    pub id: QedRunId,
    pub pipeline: String,
    pub status: RunStatus,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub steps: Vec<StepStatus>,
    /// Run-level failure reason for a failure that happened *outside* any
    /// step — a [`RunnerError`] returned before the first `StepStarted`
    /// (workspace positioning, toolchain preflight, background-sidecar
    /// validation) or after the last step. Unlike [`StepStatus::error`],
    /// which explains why a *step* failed, this carries the reason a run
    /// died with an empty (or partial) `steps` list, so `qed.status` /
    /// the desktop can surface *why* instead of a bare "failed" with
    /// nothing to anchor a card on. `None` on success and on step-level
    /// failures (the reason lives on the failing [`StepStatus`] there).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    /// Set on child runs spawned by a [`StepKind::SubPipeline`] step (W201-F5).
    /// Carries the immediate parent's [`QedRunId`] so a consumer can walk
    /// from a child up to its parent (and recursively to the root). `None`
    /// on a top-level run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<QedRunId>,
    /// What this run was *about*, when it came from a doc cell (R717-T3, W296).
    ///
    /// Every other field here says what ran. This says what it was about — and
    /// nothing in the tree indexed a QED run that way before: runs are keyed by
    /// `run_id` and grouped by pipeline name, which is enough to answer "did
    /// `node-onboard` pass?" and useless for answering "did it pass **for
    /// `us-west-003`**?". Those are different questions and a runbook shared
    /// between two operators only has the second one.
    ///
    /// `None` on every non-doc run and on all ~494 run metas already on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cell: Option<CellRef>,
    /// Compact per-row label for a matrix-fan-out child (e.g.
    /// `"target=x86_64-unknown-linux-gnu"`, mirrors [`matrix::PlannedJob::label`]) —
    /// distinguishes sibling children that otherwise share the parent's
    /// `pipeline` name. The qed runner itself never sets this (it has no
    /// notion of the matrix row it's running); the daemon stamps it on the
    /// child's registered meta at fan-out time. `None` on a top-level run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The run's **resolved** params — the same map `Pipeline::resolve_params`
    /// produced and `apply_params` consumed, defaults already filled in.
    ///
    /// Every other kind of inter-step state a resume needs already survived the
    /// run: named step outputs are in [`StepStatus::outputs`], stdout/stderr is
    /// in the task-runs store under [`StepStatus::task_run_id`], and the
    /// filesystem persists by construction for `workspace = "live"`. Params
    /// were the one thing the runner held only in memory, which is why
    /// "resume from step" could not replay a run of a pipeline with a required
    /// param — it re-entered `resolve_params` with nothing supplied and failed
    /// the run before the first step (`release-wizard` needs `spec`).
    ///
    /// Written even on the `Queued` meta the daemon registers before spawning,
    /// so a run that dies *before* any step still hands its params back.
    ///
    /// Empty on every run meta already on disk; an empty map and an unset field
    /// mean the same thing (a run with no params), so `default` is honest here
    /// in a way it would not be for a value-carrying field.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, String>,
    /// The rest of the invocation that produced this run — everything the
    /// caller narrowed or steered with beyond [`Self::params`].
    ///
    /// `params` alone was enough for "resume from step" because that button
    /// already re-supplies the one thing it changes. It is not enough for
    /// "rerun this", which has to reproduce a run the operator *configured*:
    /// a rerun of a step-subset run that quietly executes all forty steps, or
    /// of a `--ref v0.8.29` run that builds `HEAD`, is a different pipeline
    /// wearing the same name — and the operator finds out from the duration.
    ///
    /// `None` on every run meta recorded before this field existed and on runs
    /// launched with nothing but params, which are the same thing to a caller:
    /// rerun the pipeline as declared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch: Option<QedRunLaunch>,
    /// The `workspace = "isolated"` worktree this run built in, kept on disk
    /// because the run FAILED (R766). `None` for every non-Isolated run, for
    /// an Isolated run that succeeded (nothing to resume — retaining it would
    /// be pure cost, a yah worktree's `target/` is GB-scale), and for every
    /// run meta persisted before this field existed.
    ///
    /// The one thing a resume of an Isolated run needs beyond
    /// [`Self::params`]: without it, `qed.rerun --from-step` positions a
    /// *fresh* worktree at the pipeline's target ref, which has no memory of
    /// whatever steps `0..from_step` mutated on disk — wrong for a publish
    /// wave, where later steps depend on earlier ones' filesystem side
    /// effects, not just their named [`StepStatus::outputs`].
    ///
    /// Subject to retention eviction (a count-bounded sweep over the most
    /// recent failed runs, since this is GB-scale unlike the rest of a run
    /// meta) — a path here is not a guarantee the directory still exists;
    /// resume degrades to a fresh worktree when it doesn't.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_workspace: Option<std::path::PathBuf>,
    /// `YAH_BUILD_ID` of the process that actually EXECUTED this run (R876-B8).
    ///
    /// Every other field here describes the pipeline. This one describes the
    /// *interpreter* — and without it a run record cannot answer the question
    /// R876-F6 had to answer by reading binaries: **did the code I just
    /// installed run this?** `cargo xtask install` replaces `~/.local/bin/yah`
    /// while the camp daemon goes on executing whatever build it launched with,
    /// so a fix can be written, tested, installed, and still not be the code
    /// that ran the pipeline. The CLI's stderr skew notice (R330-B42) says so
    /// at the time, but it is ephemeral; this is the durable half, and it is
    /// what makes a *past* run auditable rather than merely a present one
    /// warnable.
    ///
    /// "Executor", not "daemon": a `--in-process` run's executor is the CLI
    /// itself, and stamping that honestly is the point — a record that only
    /// ever named the daemon would be silent on exactly the runs deliberately
    /// routed around it.
    ///
    /// `None` on every run meta already on disk, and on the terminal meta the
    /// runner returns — `yah-qed` is a library with no build stamp of its own,
    /// so the host that owns the process stamps it on the registered meta and
    /// carries it across, exactly as it already does for `label` and `launch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_build_id: Option<String>,
}

/// How a run was launched, beyond its resolved params (which live on
/// [`QedRunMeta::params`], where they predate this struct and where
/// "resume from step" already reads them — a second copy here would be two
/// spellings of one key).
///
/// Every field mirrors the like-named `qed.run` parameter, and an unset field
/// means the caller did not send one. That correspondence is the whole
/// contract: a rerun rebuilds the original request by copying this back out
/// field-for-field, so a launch knob added to `qed.run` without a field here
/// is a knob that silently resets on rerun.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QedRunLaunch {
    /// Camp-relative markdown doc the pipeline was sourced from (R717-F4).
    ///
    /// Recorded here rather than read back off [`QedRunMeta::cell`], which
    /// looks like it would serve: a whole-doc run stores the doc's *pipeline
    /// name* in `CellRef::cell_id` (there was no cell to name), so replaying
    /// that pair would ask for a cell the doc does not declare and be refused.
    /// `CellRef` answers "what was this run about"; this answers "what was
    /// asked for", and they are only sometimes the same string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// The single doc cell the run was narrowed to; `None` ran the whole doc.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cell: Option<String>,
    /// 0-based index the run started from, dropping the steps before it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_step: Option<u32>,
    /// Step-name subset the run was restricted to; `None` ran the full set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_steps: Option<Vec<String>>,
    /// Per-step gha-workflow matrix-instance subset; `None` ran full matrices.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_matrix_instances: Option<HashMap<String, Vec<String>>>,
    /// Whether `status = "stubbed"` steps were included.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_stubbed: Option<bool>,
    /// Target git ref the workspace was positioned at; `None` ⇒ `HEAD`.
    ///
    /// Named `git_ref` rather than `ref` because `ref` is a Rust keyword and a
    /// raw identifier in a serialized struct is a trap for the next reader;
    /// the wire name stays `ref` via `#[serde(rename)]`.
    #[serde(default, rename = "ref", skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,
    /// Routing override — `"auto"` / `"local"` / `"remote"` / `"node:<name>"`.
    ///
    /// Named `run_where` rather than `placement` since R555-T7: this is the
    /// ROUTING axis (where does the run actually go) and [`Pipeline::environment`]
    /// is the PERMISSION axis, and the two carrying one name in one file is the
    /// confusion W235 §Verdict was written to end. The wire name stays `where`.
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub run_where: Option<String>,
    /// Whether the environment gate was bypassed with `force`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,
}

/// What a run was about: the doc cell it came from and the subject it was
/// resolved for (R717-T3, W296).
///
/// The three fields are one key, and the third is the load-bearing one. W257
/// must render **green for `us-west-003` and unrun for `us-west-013` at the same
/// time**; a badge keyed only by `(doc, cell_id)` would show the `us-west-003`
/// result to an operator who opened the doc intending to build `us-west-013` —
/// a green light about the wrong box, which is worse than no light at all.
///
/// The run meta stays the source of truth for this. Any derived index
/// (R717-F6's `cells.json`) is therefore rebuildable by rescanning
/// `.yah/jit/qed/*.json`, the same property `load_qed_history` already relies
/// on — an index that can be regenerated cannot become authoritative by
/// accident, and cannot rot into a lie when a run file is hand-deleted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CellRef {
    /// Camp-root-relative path of the source document, e.g.
    /// `.yah/docs/working/W257-static-node-fleet-onboarding.md`. Relative so the
    /// key means the same thing in two checkouts of the same camp.
    pub doc: String,
    /// The cell's author-assigned `cell=<id>`. **Never positional** — these docs
    /// get reordered constantly, and a positional key would silently reattach a
    /// run to whatever cell later occupied that slot.
    pub cell_id: String,
    /// [`param_fingerprint`] of the run's resolved params — the subject.
    pub param_fingerprint: String,
}

/// blake3 over the canonicalized resolved params, hex-encoded: the subject half
/// of a [`CellRef`] (R717-T3).
///
/// **Canonicalization is the whole point.** Two operators who reach the same
/// effective params by different routes — one passing `--param node=us-west-003`
/// explicitly, one taking a `default = "us-west-003"`, in whatever order their
/// tooling happened to build the map — must produce the same fingerprint, or the
/// badge for one box splits into two half-populated histories and neither reads
/// as the truth.
///
/// The rules, each chosen against a way of getting this wrong:
///
/// - **Sorted by key.** `HashMap` iteration order is not stable across runs, let
///   alone across processes, so hashing in iteration order would give the *same
///   operator* a different fingerprint on a re-run.
/// - **Fed the post-[`Pipeline::resolve_params`] map**, so a param taken from
///   its `default` is indistinguishable from the same value passed explicitly.
///   That is the intended equivalence: the subject is what the run was *about*,
///   not how the operator spelled it.
/// - **Empty values participate.** `node=""` is not the same subject as an unset
///   `node`, and collapsing them would merge two histories.
/// - **Length-prefixed framing** rather than a delimiter. `a=b&c=d` and
///   `a=b&c` + `=d` are different param sets that a naive `join` can render
///   identically; a value containing the delimiter is a real possibility in a
///   free-text param, and a fingerprint collision here means showing one box's
///   verdict for another.
///
/// An empty param map fingerprints to a stable value rather than being rejected:
/// a doc with no params has exactly one subject, and that is a legitimate
/// notebook, not an error.
pub fn param_fingerprint(params: &HashMap<String, String>) -> String {
    let mut keys: Vec<&String> = params.keys().collect();
    keys.sort();
    let mut hasher = blake3::Hasher::new();
    for key in keys {
        let value = &params[key];
        // Length-prefixed: no byte sequence inside a key or value can forge a
        // boundary, so distinct param maps cannot share a preimage.
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key.as_bytes());
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum RunStatus {
    /// Registered but waiting on its `concurrency_key` lock. Emitted as
    /// the very first status after `qed_run_handler` registers the run;
    /// transitions to `Running` when the key's mutex is acquired.
    Queued,
    Running,
    /// Parked on a human (R622, W282) — a [`StepKind::Manual`] step minted a
    /// prompt and is waiting for someone to answer it (or for its `advance`
    /// condition to start passing).
    ///
    /// **Non-terminal**, and deliberately so: like `Queued`/`Running` it
    /// contributes nothing to [`RunStatus::aggregate`]. A parked run has
    /// released its `concurrency_key` and re-enters `Queued` to reacquire it
    /// on resume, so the full shape is
    /// `Running → AwaitingHuman → Queued → Running`.
    AwaitingHuman,
    Success,
    Failed,
    Cancelled,
    /// Step (or run) was skipped without executing (R506). Set when
    /// [`QedStep::enabled`] is `false`, [`QedStep::activation`] is
    /// [`StepActivation::Stubbed`] (and `--include-stubbed` wasn't passed),
    /// or [`QedStep::if_cond`] evaluated to a falsy value. A skipped step
    /// does not flip the run's overall status to `Failed`.
    Skipped,
}

impl RunStatus {
    /// Has this run or step reached a state it can never leave?
    ///
    /// [`Self::AwaitingHuman`] is deliberately NOT terminal even though a park
    /// can outlive everything around it: the run is alive, holds no lock, and
    /// resumes the moment its gate is answered. Treating it as terminal is the
    /// mistake that makes a supervisor report a release "done" while it sits
    /// waiting for someone to look at it.
    pub fn is_terminal(self) -> bool {
        match self {
            RunStatus::Queued | RunStatus::Running | RunStatus::AwaitingHuman => false,
            RunStatus::Success | RunStatus::Failed | RunStatus::Cancelled | RunStatus::Skipped => {
                true
            }
        }
    }

    /// Aggregate child run statuses into a single parent status (R506-F1
    /// matrix fan-out). Mirrors [`yah_qed_gha::JobResult::aggregate`] exactly so a
    /// matrixed parent run reports the same overall verdict the GHA graph would
    /// for the same set of rows: any `Failed` wins, then `Cancelled`, then
    /// `Success`, and only an all-`Skipped` (or empty) set reports `Skipped`.
    ///
    /// `Queued` / `Running` / `AwaitingHuman` contribute nothing — `aggregate`
    /// is meant to be called once every child has reached a terminal state, and
    /// all three are non-terminal (a parked run is *waiting*, not a verdict).
    pub fn aggregate<I: IntoIterator<Item = RunStatus>>(children: I) -> RunStatus {
        let mut seen_success = false;
        let mut seen_failure = false;
        let mut seen_cancelled = false;
        let mut seen_any = false;
        for r in children {
            seen_any = true;
            match r {
                RunStatus::Failed => seen_failure = true,
                RunStatus::Cancelled => seen_cancelled = true,
                RunStatus::Success => seen_success = true,
                RunStatus::Skipped
                | RunStatus::Queued
                | RunStatus::Running
                | RunStatus::AwaitingHuman => {}
            }
        }
        if !seen_any {
            RunStatus::Skipped
        } else if seen_failure {
            RunStatus::Failed
        } else if seen_cancelled {
            RunStatus::Cancelled
        } else if seen_success {
            RunStatus::Success
        } else {
            RunStatus::Skipped
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepStatus {
    pub name: String,
    pub task_run_id: Option<ForgeId>,
    pub status: RunStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    /// Failure reason for a `Failed` step — the `StepFailed.msg` tail (stderr
    /// tail for subprocess steps, a typed reason for resolver/config errors).
    /// Persisted on the terminal run meta so `qed.status` / `qed report` can
    /// surface *why* a step failed long after the live event stream is gone
    /// (the reason was previously only emitted into the `StepFinished` event,
    /// which doesn't survive in the meta json). `None` for non-failed steps,
    /// or when the failure carried no message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Key-value outputs collected from `$YAH_OUTPUTS` after the step ran
    /// (W201-F4). Empty when the step did not write any outputs, when the
    /// step kind doesn't support output collection (container, remote,
    /// sub-pipeline), or when the step failed before writing anything.
    #[serde(default)]
    pub outputs: HashMap<String, String>,
    /// W209: bind results applied immediately after this step succeeded.
    /// Each entry records `file`, `path`, `from`, `old`, `new`, and a
    /// `changed` bool the qed-run tile uses to surface "Bound N values in
    /// <file> — review diff" (F7) and to drive hash-change hooks (F6).
    /// Empty when this step had no binds referencing it, when the predicate
    /// rejected every candidate value (e.g. all binds are pinned), or when
    /// the step failed before any bind could fire.
    #[serde(default)]
    pub applied_binds: Vec<manifest_bind::AppliedBind>,
    /// blake3 digest of every path this step declared in [`QedStep::inputs`],
    /// hashed **immediately before** the step executed (R717-T1, W296). Keyed by
    /// the declared (camp-root-relative) path; a path that did not exist or
    /// could not be read records [`crate::staleness::ABSENT_INPUT`] rather than
    /// being omitted, so "the file was missing when this ran" and "this run
    /// predates the field" stay distinguishable.
    ///
    /// Hashed *before* rather than *after* on purpose: the digest answers "which
    /// bytes produced this result?", and a step that rewrites its own input
    /// would otherwise record the bytes it emitted instead of the ones it read.
    ///
    /// This is the **only** persisted half of staleness — there is no stale
    /// `RunStatus`. A reader compares this map against the tree via
    /// [`crate::staleness::input_freshness`] at read time. `BTreeMap` so the
    /// serialized journal is byte-stable across runs with the same inputs.
    ///
    /// Empty for every step that declares no `inputs`, and for every one of the
    /// ~494 run metas on disk that predate this field.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub input_hashes: std::collections::BTreeMap<String, String>,
    /// Per-job rows for a step that wraps a foreign pipeline (W223 R532-T1).
    /// Non-empty only when this step wraps a GitHub Actions workflow — whether
    /// reached as a [`StepKind::GhaWorkflow`] step or a [`StepKind::SubPipeline`]
    /// whose target is a GHA workflow — which fans out to many jobs. Each row
    /// carries one job's terminal status and (on failure) its stderr-tail
    /// detail, so the report renders the wrapped workflow *transparently* (the
    /// same per-job shape the graph viewer draws) instead of collapsing it into
    /// one flattened failure string. The R516 skip-count folds into per-row
    /// [`RunStatus::Skipped`] state rather than a trailing sentence. Empty for
    /// native steps and for non-GHA sub-pipelines (transparency generalizes to
    /// the other `SubPipelineRef` kinds in a later phase).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<JobRow>,
}

/// One job within a wrapped foreign pipeline's [`StepStatus`] (W223 R532-T1).
///
/// A wrapped GHA workflow is a *disregarded entity*: structurally it is one
/// QED step, but its internal jobs are attributed to that step's report row as
/// if the wrapper weren't there. This is the persisted, structured equivalent
/// of one of those jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRow {
    /// GHA job id (the `jobs.<id>` key). Combined with the wrapping step's name
    /// this yields the stable node address `<step>.<job_id>` that the report,
    /// the graph viewer, and `needs.*` cross-references all name (W223
    /// §identity — mirrors the existing `<job_id>.<output_key>` output-lifting
    /// convention).
    pub id: String,
    /// This job's terminal status: `Success` / `Failed` / `Skipped`.
    /// `Cancelled` maps to `Failed`.
    pub status: RunStatus,
    /// Failure detail for a `Failed` job — the failing step's name plus its
    /// stderr tail (the same text the flattened summary used to concatenate).
    /// `None` for success / skipped rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Cause for a `Skipped` row (R330-B41) — a failed/skipped `needs:`
    /// dependency (named), an `if:` condition that evaluated false, or a
    /// matrix/instance-selector filter that excluded this row. Mirrors
    /// [`yah_qed_gha::InstanceRun::skip_reason`] verbatim. `None` for
    /// non-skipped rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
    /// Logical job ids this job `needs:` — the intra-workflow dependency edges
    /// already computed by `yah_qed_gha::plan` (W223 R532-F2). The graph viewer
    /// renders these as real dependency edges between the inlined job nodes, so
    /// the wave ordering inside the wrapped workflow is visible rather than a
    /// flat list. Empty for a job with no declared `needs`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_step(argv: Vec<&str>, env: &[(&str, &str)]) -> Pipeline {
        Pipeline {
            allow_late_operator_block: false,
            participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![QedStep {
                            expect_slow: false,
                            participant: None,
                needs: None,
                resource: None,
                inputs: Vec::new(),
                secret: false,
                background: false,
                background_until: None,
                wait_for: None,
                manual: None,
                manifest_stitch: None,
                name: "s".into(),
                argv: argv.into_iter().map(String::from).collect(),
                cwd: None,
                env: env
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: StepKind::Subprocess,
                image: None,
                tag: None,
                push: false,
                platforms: Vec::new(),
                binary_path: None,
                triple: None,
                package: None,
                context: None,
                source_context: Vec::new(),
                cache: false,
                load: false,
                sub_pipeline: None,
                gha_workflow: None,
                import: None,
                matrix: None,
                enabled: true,
                activation: StepActivation::Active,
                if_cond: None,
                platform: None,
                toolchain: None,
                outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: Environment::default(),
            workspace: crate::types::WorkspaceMode::default(),
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        }
    }

    // ---- R719-F1: the concurrency-key default ---------------------------
    //
    // These pin an inversion, so they are written to fail loudly if someone
    // restores the old behaviour: the whole point is that forgetting a key now
    // costs you serialization instead of silently costing you safety.

    #[test]
    fn an_unkeyed_pipeline_defaults_to_the_camp_global_key() {
        let p = one_step(vec!["true"], &[]);
        assert_eq!(p.concurrency_key, None, "fixture must be unkeyed");
        assert_eq!(p.effective_concurrency_key(), DEFAULT_CONCURRENCY_KEY);
        assert_ne!(
            p.effective_concurrency_key(),
            p.name,
            "R719-F1 inverted this: the pipeline NAME must no longer be the default"
        );
    }

    /// The behaviour change that matters, stated directly: two *different*
    /// unkeyed pipelines now share one lane. Under the old default they had
    /// two, which is how an unstamped `desktop-release` ran concurrently with
    /// itself-by-another-name.
    #[test]
    fn two_different_unkeyed_pipelines_now_share_one_lane() {
        let mut a = one_step(vec!["true"], &[]);
        a.name = "alpha".into();
        let mut b = one_step(vec!["true"], &[]);
        b.name = "beta".into();
        assert_eq!(a.effective_concurrency_key(), b.effective_concurrency_key());
    }

    #[test]
    fn an_explicit_key_still_wins_and_is_untouched() {
        let mut p = one_step(vec!["true"], &[]);
        p.concurrency_key = Some("cargo-target".into());
        assert_eq!(p.effective_concurrency_key(), "cargo-target");
        assert!(!p.is_parallel());
    }

    #[test]
    fn the_parallel_sentinel_still_opts_out() {
        let mut p = one_step(vec!["true"], &[]);
        p.concurrency_key = Some(PARALLEL_CONCURRENCY_KEY.into());
        assert!(p.is_parallel());

        // …and the new default is NOT an opt-out. A sentinel that accidentally
        // read as parallel would invert the inversion.
        let unkeyed = one_step(vec!["true"], &[]);
        assert!(!unkeyed.is_parallel());
    }

    /// A pipeline literally named `@camp` must not accidentally join the
    /// default lane by name — the fallback is on the *key*, not the name.
    #[test]
    fn the_sentinels_are_spelled_so_a_name_cannot_collide() {
        assert!(DEFAULT_CONCURRENCY_KEY.starts_with('@'));
        assert!(PARALLEL_CONCURRENCY_KEY.starts_with('@'));
        assert_ne!(DEFAULT_CONCURRENCY_KEY, PARALLEL_CONCURRENCY_KEY);
    }

    #[test]
    fn apply_params_substitutes_argv_and_env() {
        let mut p = one_step(
            vec!["run", "--", "{{provider}}"],
            &[("KEY", "{{provider}}-x")],
        );
        let mut params = HashMap::new();
        params.insert("provider".to_string(), "groq".to_string());
        p.apply_params(&params);
        assert_eq!(p.steps[0].argv, vec!["run", "--", "groq"]);
        assert_eq!(p.steps[0].env.get("KEY").unwrap(), "groq-x");
    }

    #[test]
    fn apply_params_substitutes_manual_block() {
        // R906-B3: a manual step's gate predicate couldn't see its own
        // pipeline's params (R605-B17's on-disk nonce protocol and
        // yah-release-wizard's `awk`-out-of-Cargo.toml workaround both exist
        // only because of this gap). Cover all three fields the ticket names.
        let mut p = one_step(vec!["bash", "noop.sh"], &[]);
        p.steps[0].manual = Some(ManualConfig {
            prompt: "Confirm version {{version}} is correct.".into(),
            terminal: vec!["git tag v{{version}}".into()],
            advance: Some("git describe --tags --exact-match v{{version}}".into()),
            checklist: vec!["Tag v{{version}} pushed".into()],
            advance_poll_secs: 5,
            audience: ManualAudience::Agent,
        });
        let mut params = HashMap::new();
        params.insert("version".to_string(), "1.2.3".to_string());
        p.apply_params(&params);
        let manual = p.steps[0].manual.as_ref().unwrap();
        assert_eq!(manual.prompt, "Confirm version 1.2.3 is correct.");
        assert_eq!(manual.terminal, vec!["git tag v1.2.3".to_string()]);
        assert_eq!(
            manual.advance.as_deref(),
            Some("git describe --tags --exact-match v1.2.3")
        );
        assert_eq!(manual.checklist, vec!["Tag v1.2.3 pushed".to_string()]);
    }

    #[test]
    fn apply_params_substitutes_platform_target() {
        // R786-B1: release-build.toml's cross-build step declares
        // `platform = { target = "{{target}}" }` so qed's native_cross_plan
        // preflight sees a real triple. Without substitution here the field
        // keeps the literal `{{target}}` text forever — step_platform would
        // hand `native_cross_plan` a nonsense "target" that never matches a
        // real arch/OS, silently defeating the whole point of declaring it.
        let mut p = one_step(vec!["bash", "build.sh"], &[]);
        p.steps[0].platform = Some(crate::platform::PlatformSpec {
            target: Some("{{target}}".into()),
            container_platform: None,
            native: false,
        });
        let mut params = HashMap::new();
        params.insert(
            "target".to_string(),
            "x86_64-unknown-linux-gnu".to_string(),
        );
        p.apply_params(&params);
        assert_eq!(
            p.steps[0].platform.as_ref().unwrap().target.as_deref(),
            Some("x86_64-unknown-linux-gnu")
        );
    }

    /// A pinned param must reach the RESOLVED map, not just `{{key}}`
    /// substitution — `if = "params.k == …"` step gating reads the same map,
    /// and `.yah/qed/local-install.toml` depends on it: its `mcp-sidecar` step
    /// is skipped when `params.sidecars_flag == '--sidecars'`, which is a value
    /// only `all-local`'s PIN ever supplies. If a pin stopped being visible
    /// here, that gate would silently stop firing and the step would come back
    /// — failing on a macOS App Management grant for a copy that changes
    /// nothing. Nothing else in the tree pins this behaviour.
    #[test]
    fn resolve_params_surfaces_a_pinned_value_for_if_gating() {
        let mut p = one_step(vec!["install", "{{sidecars_flag}}"], &[]);
        p.params.insert(
            "sidecars_flag".to_string(),
            ParamDef {
                required: false,
                description: None,
                default: Some("--no-sidecars".to_string()),
                options: Vec::new(),
                options_from: None,
                options_cmd: None,
            },
        );
        // The loader moves a pinned key OUT of `params` and into `pins`.
        p.params.remove("sidecars_flag");
        p.pins.insert("sidecars_flag".to_string(), "--sidecars".to_string());

        let resolved = p.resolve_params(&HashMap::new()).expect("a pin needs nothing supplied");
        assert_eq!(
            resolved.get("sidecars_flag").map(String::as_str),
            Some("--sidecars"),
            "a pinned param must be readable as params.<key>, or if-gating on it is dead"
        );

        // And it must still substitute, so the two readers cannot diverge.
        p.apply_params(&resolved);
        assert_eq!(p.steps[0].argv, vec!["install", "--sidecars"]);
    }

    #[test]
    fn resolve_params_fills_declared_defaults() {
        // The release-build case, expressed properly. That pipeline declares
        // all five params `required` and documents that callers must always pass
        // them, "empty string where N/A", precisely because an absent param is
        // left as a literal `{{features}}` in the argv. A default says that once,
        // in the pipeline, instead of in every caller.
        let mut p = one_step(vec!["build", "{{package}}", "{{features}}"], &[]);
        p.params.insert(
            "package".to_string(),
            ParamDef { required: true, description: None, default: None, options: Vec::new(), options_from: None, options_cmd: None },
        );
        p.params.insert(
            "features".to_string(),
            ParamDef {
                required: false,
                description: None,
                default: Some(String::new()),
                options: Vec::new(),
                options_from: None,
                options_cmd: None,
            },
        );

        let supplied: HashMap<String, String> =
            [("package".to_string(), "yah".to_string())].into_iter().collect();
        let resolved = p.resolve_params(&supplied).expect("package supplied, features defaulted");
        assert_eq!(resolved.get("features").map(String::as_str), Some(""));

        p.apply_params(&resolved);
        assert_eq!(p.steps[0].argv, vec!["build", "yah", ""]);
    }

    #[test]
    fn resolve_params_names_every_missing_required_param_at_once() {
        // A five-param recipe should cost one error message, not five round
        // trips — and a `required` param that declares a default is satisfied.
        let mut p = one_step(vec!["x"], &[]);
        for name in ["board", "version"] {
            p.params.insert(
                name.to_string(),
                ParamDef { required: true, description: None, default: None, options: Vec::new(), options_from: None, options_cmd: None },
            );
        }
        p.params.insert(
            "channel".to_string(),
            ParamDef {
                required: true,
                description: None,
                default: Some("stable".to_string()),
                options: Vec::new(),
                options_from: None,
                options_cmd: None,
            },
        );

        let err = p.resolve_params(&HashMap::new()).expect_err("two params missing");
        match &err {
            ParamError::MissingRequired { names, .. } => {
                assert_eq!(names, &vec!["board".to_string(), "version".to_string()]);
            }
            other => panic!("expected MissingRequired, got {other:?}"),
        }
        // The message has to name both and say how to fix it.
        let msg = err.to_string();
        assert!(msg.contains("board") && msg.contains("version"), "got: {msg}");
        assert!(msg.contains("--param"), "got: {msg}");
        assert!(!msg.contains("channel"), "a default satisfies required: {msg}");
    }

    #[test]
    fn resolve_params_passes_supplied_values_through_untouched() {
        // A supplied value always wins over a default, and an undeclared param is
        // passed through rather than rejected (documented on `resolve_params`).
        let mut p = one_step(vec!["x"], &[]);
        p.params.insert(
            "channel".to_string(),
            ParamDef {
                required: false,
                description: None,
                default: Some("stable".to_string()),
                options: Vec::new(),
                options_from: None,
                options_cmd: None,
            },
        );
        let supplied: HashMap<String, String> = [
            ("channel".to_string(), "beta".to_string()),
            ("undeclared".to_string(), "kept".to_string()),
        ]
        .into_iter()
        .collect();
        let resolved = p.resolve_params(&supplied).expect("resolves");
        assert_eq!(resolved.get("channel").map(String::as_str), Some("beta"));
        assert_eq!(resolved.get("undeclared").map(String::as_str), Some("kept"));
    }

    /// A param with `options` is a variant selector: in-set values resolve, and
    /// the default it falls back to is one of them.
    #[test]
    fn resolve_params_accepts_a_declared_option() {
        let mut p = one_step(vec!["build", "--board", "{{board}}"], &[]);
        p.params.insert(
            "board".to_string(),
            ParamDef {
                required: false,
                description: Some("Which appliance board to build for".to_string()),
                default: Some("orangepi_zero2w".to_string()),
                options: vec!["orangepi_zero2w".to_string(), "rpi_zero2w".to_string()],
                options_from: None,
                options_cmd: None,
            },
        );

        let supplied: HashMap<String, String> =
            [("board".to_string(), "rpi_zero2w".to_string())].into_iter().collect();
        let resolved = p.resolve_params(&supplied).expect("rpi_zero2w is declared");
        assert_eq!(resolved.get("board").map(String::as_str), Some("rpi_zero2w"));

        // Omitted ⇒ the default, which must itself be in the set.
        let defaulted = p.resolve_params(&HashMap::new()).expect("default is in-set");
        assert_eq!(defaulted.get("board").map(String::as_str), Some("orangepi_zero2w"));
    }

    #[test]
    fn resolve_params_rejects_a_value_outside_the_declared_options() {
        // The whole reason to reject rather than merely not-suggest: with R653-F1
        // shipped, `params.board` can gate a step's `if=`, so a typo'd variant
        // doesn't just substitute wrong text — it silently skips steps.
        let mut p = one_step(vec!["build", "{{board}}"], &[]);
        p.params.insert(
            "board".to_string(),
            ParamDef {
                required: true,
                description: None,
                default: None,
                options: vec!["orangepi_zero2w".to_string(), "rpi_zero2w".to_string()],
                options_from: None,
                options_cmd: None,
            },
        );
        let supplied: HashMap<String, String> =
            [("board".to_string(), "rpi_zero2".to_string())].into_iter().collect();
        let err = p.resolve_params(&supplied).expect_err("not a declared board");
        match &err {
            ParamError::NotInOptions { name, value, options, .. } => {
                assert_eq!(name, "board");
                assert_eq!(value, "rpi_zero2");
                assert_eq!(options.len(), 2);
            }
            other => panic!("expected NotInOptions, got {other:?}"),
        }
        // The message has to show the legal set — that is the whole repair hint.
        let msg = err.to_string();
        assert!(msg.contains("orangepi_zero2w") && msg.contains("rpi_zero2w"), "got: {msg}");
    }

    #[test]
    fn resolve_params_leaves_free_form_params_unconstrained() {
        // Empty `options` is the default and means free-form: opting in is what
        // closes the set, so no pre-existing pipeline starts rejecting values.
        let mut p = one_step(vec!["tag", "{{version}}"], &[]);
        p.params.insert(
            "version".to_string(),
            ParamDef {
                required: true,
                description: None,
                default: None,
                options: Vec::new(),
                options_from: None,
                options_cmd: None,
            },
        );
        let supplied: HashMap<String, String> =
            [("version".to_string(), "v9.9.9-rc1".to_string())].into_iter().collect();
        let resolved = p.resolve_params(&supplied).expect("free-form param takes anything");
        assert_eq!(resolved.get("version").map(String::as_str), Some("v9.9.9-rc1"));
    }

    #[test]
    fn apply_params_substitutes_gha_workflow_matrix_and_inputs() {
        // A gha-workflow step has no argv and no env, so before this these two
        // maps were the only parameterisable surface it had — and neither was
        // substituted. `matrix = { board = "{{board}}" }` is the whole point of
        // the row selector: without substitution it would filter on the literal
        // string "{{board}}" and skip every row.
        let mut p = one_step(vec![], &[]);
        p.steps[0].kind = StepKind::GhaWorkflow;
        p.steps[0].gha_workflow = Some(GhaWorkflowConfig {
            path: ".github/workflows/appliance-image.yml".into(),
            event: Some("workflow_dispatch".into()),
            inputs: [("version".to_string(), "{{version}}".to_string())]
                .into_iter()
                .collect(),
            matrix: [("board".to_string(), "{{board}}".to_string())]
                .into_iter()
                .collect(),
        });
        let params: HashMap<String, String> = [
            ("board".to_string(), "rpi_zero2w".to_string()),
            ("version".to_string(), "v0.1.0".to_string()),
        ]
        .into_iter()
        .collect();
        p.apply_params(&params);
        let cfg = p.steps[0].gha_workflow.as_ref().unwrap();
        assert_eq!(cfg.matrix.get("board").unwrap(), "rpi_zero2w");
        assert_eq!(cfg.inputs.get("version").unwrap(), "v0.1.0");
    }

    #[test]
    fn run_status_aggregate_failure_dominates() {
        use RunStatus::*;
        assert_eq!(
            RunStatus::aggregate([Success, Failed, Skipped, Success]),
            Failed
        );
        assert_eq!(RunStatus::aggregate([Cancelled, Failed]), Failed);
    }

    #[test]
    fn run_status_aggregate_cancelled_beats_success_and_skipped() {
        use RunStatus::*;
        assert_eq!(
            RunStatus::aggregate([Success, Cancelled, Skipped]),
            Cancelled
        );
    }

    #[test]
    fn run_status_aggregate_success_beats_skipped() {
        use RunStatus::*;
        // A matrix where one row ran and others were if=-gated out is green.
        assert_eq!(RunStatus::aggregate([Skipped, Success, Skipped]), Success);
    }

    #[test]
    fn run_status_aggregate_all_skipped_is_skipped() {
        use RunStatus::*;
        assert_eq!(RunStatus::aggregate([Skipped, Skipped]), Skipped);
        // Empty (vacuous) also reports Skipped, mirroring JobResult::aggregate.
        assert_eq!(RunStatus::aggregate(std::iter::empty()), Skipped);
    }

    #[test]
    fn run_status_aggregate_ignores_non_terminal() {
        use RunStatus::*;
        // Queued/Running contribute nothing; a lone Success still wins.
        assert_eq!(RunStatus::aggregate([Queued, Running, Success]), Success);
    }

    #[test]
    fn run_status_aggregate_ignores_awaiting_human() {
        use RunStatus::*;
        // R622: a parked run is *waiting*, not a verdict — it joins
        // Queued/Running on the "contributes nothing" side of the table.
        assert_eq!(
            RunStatus::aggregate([AwaitingHuman, Success]),
            Success,
            "a parked sibling must not downgrade a green aggregate"
        );
        assert_eq!(
            RunStatus::aggregate([AwaitingHuman, Failed]),
            Failed,
            "nor mask a red one"
        );
        assert_eq!(
            RunStatus::aggregate([AwaitingHuman, Skipped]),
            Skipped,
            "nor promote an all-skipped set"
        );
        // And on its own it is vacuous, exactly like [Running].
        assert_eq!(RunStatus::aggregate([AwaitingHuman]), Skipped);
        assert_eq!(RunStatus::aggregate([Running]), Skipped);
    }

    #[test]
    fn run_status_awaiting_human_serializes_lowercase() {
        // The wire mapping in camp.rs and the persisted `<run_id>.json` both
        // depend on this spelling; a rename here silently orphans parked runs
        // written by an older build.
        assert_eq!(
            serde_json::to_string(&RunStatus::AwaitingHuman).unwrap(),
            "\"awaitinghuman\""
        );
        assert_eq!(
            serde_json::from_str::<RunStatus>("\"awaitinghuman\"").unwrap(),
            RunStatus::AwaitingHuman
        );
    }

    // ── R622 (W282): manual steps ──────────────────────────────────────────

    fn manual_step(name: &str, cfg: ManualConfig) -> QedStep {
        let mut step = QedStep::default();
        step.name = name.into();
        step.kind = StepKind::Manual;
        step.manual = Some(cfg);
        step
    }

    fn manual_cfg(prompt: &str) -> ManualConfig {
        ManualConfig {
            prompt: prompt.into(),
            terminal: vec![],
            advance: None,
            checklist: vec![],
            advance_poll_secs: 5,
            audience: ManualAudience::Agent,
        }
    }

    #[test]
    fn manual_step_validates_with_just_a_prompt() {
        // An honour-system gate (no `advance`) is legal — discouraged in the
        // docs, but the schema is not the place to enforce taste.
        assert!(manual_step("commit-and-tag", manual_cfg("Tag the release."))
            .validate()
            .is_ok());
    }

    #[test]
    fn manual_step_rejects_argv() {
        // The pure-gate rule, same as wait-for: a manual step's commands go in
        // `terminal` (the human runs them) or `advance` (the pipeline checks
        // them), never in argv where the runner would run them silently.
        let mut step = manual_step("gate", manual_cfg("Do the thing."));
        step.argv = vec!["git".into(), "tag".into()];
        assert_eq!(
            step.validate(),
            Err(StepValidationError::ManualHasArgv("gate".into()))
        );
    }

    #[test]
    fn manual_step_rejects_missing_config_and_empty_prompt() {
        let mut step = manual_step("gate", manual_cfg("Do the thing."));
        step.manual = None;
        assert_eq!(
            step.validate(),
            Err(StepValidationError::ManualMissingConfig("gate".into()))
        );

        let blank = manual_step("gate", manual_cfg("   "));
        assert_eq!(
            blank.validate(),
            Err(StepValidationError::ManualEmptyPrompt("gate".into()))
        );
    }

    #[test]
    fn manual_step_rejects_blank_advance_but_allows_absent() {
        let mut step = manual_step("gate", manual_cfg("Tag it."));
        step.manual.as_mut().unwrap().advance = Some("  ".into());
        assert_eq!(
            step.validate(),
            Err(StepValidationError::ManualBlankAdvance("gate".into()))
        );
        step.manual.as_mut().unwrap().advance = Some("git describe --tags --exact-match".into());
        assert!(step.validate().is_ok());
    }

    #[test]
    fn manual_step_rejects_zero_poll_interval() {
        let mut step = manual_step("gate", manual_cfg("Tag it."));
        step.manual.as_mut().unwrap().advance_poll_secs = 0;
        assert_eq!(
            step.validate(),
            Err(StepValidationError::ManualZeroPollInterval("gate".into()))
        );
    }

    #[test]
    fn manual_step_rejects_container_runtime() {
        // W282 open question 3, decided: a container has no human at a keyboard
        // and no positioned workspace to evaluate `advance` in. Reject at parse
        // time rather than surprising the author mid-release.
        let mut step = manual_step("gate", manual_cfg("Tag it."));
        step.runtime = Some(TaskRuntime::Container);
        assert_eq!(
            step.validate(),
            Err(StepValidationError::ManualContainerRuntime("gate".into()))
        );
        step.runtime = Some(TaskRuntime::Native);
        assert!(step.validate().is_ok());
    }

    #[test]
    fn manual_step_rejects_background() {
        // `background` is Subprocess-only (R513-F2); a detached human gate is
        // meaningless. Covered by the pre-match guard, asserted here so the
        // interaction is pinned.
        let mut step = manual_step("gate", manual_cfg("Tag it."));
        step.background = true;
        assert_eq!(
            step.validate(),
            Err(StepValidationError::BackgroundRequiresSubprocess(
                "gate".into()
            ))
        );
    }

    #[test]
    fn manual_config_parses_from_toml_with_defaults() {
        let step: QedStep = toml::from_str(
            r#"
name = "commit-and-tag"
kind = "manual"
[manual]
prompt = "Review the bump, commit it, and tag vX.Y.Z."
terminal = ["git status", "git diff --stat"]
advance = "git describe --tags --exact-match"
checklist = ["Diff reviewed"]
"#,
        )
        .expect("manual step parses");
        assert_eq!(step.kind, StepKind::Manual);
        let cfg = step.manual.as_ref().expect("[manual] block");
        assert_eq!(cfg.terminal.len(), 2);
        assert_eq!(cfg.checklist, vec!["Diff reviewed".to_string()]);
        assert_eq!(cfg.advance.as_deref(), Some("git describe --tags --exact-match"));
        assert_eq!(cfg.advance_poll_secs, 5, "poll cadence defaults to 5s");
        assert!(step.validate().is_ok());
    }

    fn build_image_step(name: &str) -> QedStep {
        QedStep {
            expect_slow: false,
            participant: None,
            needs: None,
            resource: None,
            inputs: Vec::new(),
            secret: false,
            background: false,
            background_until: None,
            wait_for: None,
            manual: None,
            manifest_stitch: None,
            name: name.into(),
            argv: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: Some(TaskRuntime::Container),
            kind: StepKind::BuildImage,
            image: Some("yah-rust".into()),
            tag: None,
            push: false,
            platforms: Vec::new(),
            binary_path: None,
            triple: None,
            package: None,
            context: None,
            source_context: Vec::new(),
            cache: false,
            load: false,
            sub_pipeline: None,
            gha_workflow: None,
            import: None,
            matrix: None,
            enabled: true,
            activation: StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
            outputs: Vec::new(),
        }
    }

    fn package_native_tarball_step(name: &str) -> QedStep {
        QedStep {
            expect_slow: false,
            participant: None,
            needs: None,
            resource: None,
            inputs: Vec::new(),
            secret: false,
            background: false,
            background_until: None,
            wait_for: None,
            manual: None,
            manifest_stitch: None,
            name: name.into(),
            argv: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: StepKind::PackageNativeTarball,
            image: Some("yah-yubaba".into()),
            tag: None,
            push: false,
            platforms: Vec::new(),
            binary_path: Some("target/x86_64-unknown-linux-musl/release/yubaba".into()),
            triple: Some("x86_64-unknown-linux-musl".into()),
            package: None,
            context: None,
            source_context: Vec::new(),
            cache: false,
            load: false,
            sub_pipeline: None,
            gha_workflow: None,
            import: None,
            matrix: None,
            enabled: true,
            activation: StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
            outputs: Vec::new(),
        }
    }

    fn musl_static_preflight_step(name: &str) -> QedStep {
        QedStep {
            expect_slow: false,
            participant: None,
            needs: None,
            resource: None,
            inputs: Vec::new(),
            secret: false,
            background: false,
            background_until: None,
            wait_for: None,
            manual: None,
            manifest_stitch: None,
            name: name.into(),
            argv: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: StepKind::MuslStaticPreflight,
            image: None,
            tag: None,
            push: false,
            platforms: Vec::new(),
            binary_path: None,
            triple: None,
            package: Some("yubaba".into()),
            context: None,
            source_context: Vec::new(),
            cache: false,
            load: false,
            sub_pipeline: None,
            gha_workflow: None,
            import: None,
            matrix: None,
            enabled: true,
            activation: StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
            outputs: Vec::new(),
        }
    }

    #[test]
    fn subprocess_with_argv_validates() {
        let step = one_step(vec!["echo", "hi"], &[]).steps.remove(0);
        step.validate().unwrap();
    }

    #[test]
    fn subprocess_without_argv_rejected() {
        let mut step = one_step(vec!["echo"], &[]).steps.remove(0);
        step.argv.clear();
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::SubprocessMissingArgv("s".into())
        );
    }

    #[test]
    fn build_image_happy_path_validates() {
        build_image_step("bake").validate().unwrap();
    }

    #[test]
    fn build_image_with_argv_rejected() {
        let mut step = build_image_step("bake");
        step.argv = vec!["docker".into()];
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::BuildImageHasArgv("bake".into())
        );
    }

    #[test]
    fn build_image_without_image_rejected() {
        let mut step = build_image_step("bake");
        step.image = None;
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::BuildImageMissingImage("bake".into())
        );
    }

    #[test]
    fn build_image_with_native_runtime_rejected() {
        let mut step = build_image_step("bake");
        step.runtime = Some(TaskRuntime::Native);
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::BuildImageNativeRuntime("bake".into())
        );
    }

    #[test]
    fn build_image_with_default_runtime_accepted() {
        // runtime = None means the pipeline default applies; resolve_runtime
        // forces Container for build-image steps at runner time. Parse-time
        // validation lets this through.
        let mut step = build_image_step("bake");
        step.runtime = None;
        step.validate().unwrap();
    }

    // ── R407-T2 package-native-tarball validation ──────────────────────────

    #[test]
    fn package_native_tarball_happy_path_validates() {
        package_native_tarball_step("pack").validate().unwrap();
    }

    #[test]
    fn package_native_tarball_with_argv_rejected() {
        let mut step = package_native_tarball_step("pack");
        step.argv = vec!["tar".into()];
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::PackageNativeTarballHasArgv("pack".into()),
        );
    }

    #[test]
    fn package_native_tarball_without_image_rejected() {
        let mut step = package_native_tarball_step("pack");
        step.image = None;
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::PackageNativeTarballMissingImage("pack".into()),
        );
    }

    #[test]
    fn package_native_tarball_without_binary_path_rejected() {
        let mut step = package_native_tarball_step("pack");
        step.binary_path = None;
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::PackageNativeTarballMissingBinaryPath("pack".into()),
        );
    }

    #[test]
    fn package_native_tarball_with_container_runtime_rejected() {
        let mut step = package_native_tarball_step("pack");
        step.runtime = Some(TaskRuntime::Container);
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::PackageNativeTarballContainerRuntime("pack".into()),
        );
    }

    #[test]
    fn package_native_tarball_with_explicit_native_runtime_accepted() {
        let mut step = package_native_tarball_step("pack");
        step.runtime = Some(TaskRuntime::Native);
        step.validate().unwrap();
    }

    // ── R407-T3 musl-static-preflight validation ───────────────────────────

    #[test]
    fn musl_static_preflight_happy_path_validates() {
        musl_static_preflight_step("preflight").validate().unwrap();
    }

    #[test]
    fn musl_static_preflight_with_argv_rejected() {
        let mut step = musl_static_preflight_step("preflight");
        step.argv = vec!["cargo".into()];
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::MuslStaticPreflightHasArgv("preflight".into()),
        );
    }

    #[test]
    fn musl_static_preflight_without_package_rejected() {
        let mut step = musl_static_preflight_step("preflight");
        step.package = None;
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::MuslStaticPreflightMissingPackage("preflight".into()),
        );
    }

    #[test]
    fn musl_static_preflight_with_container_runtime_rejected() {
        let mut step = musl_static_preflight_step("preflight");
        step.runtime = Some(TaskRuntime::Container);
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::MuslStaticPreflightContainerRuntime("preflight".into()),
        );
    }

    // ── R407-T5 sign-native-tarball validation ─────────────────────────────

    fn sign_native_tarball_step(name: &str) -> QedStep {
        QedStep {
            expect_slow: false,
            participant: None,
            needs: None,
            resource: None,
            inputs: Vec::new(),
            secret: false,
            background: false,
            background_until: None,
            wait_for: None,
            manual: None,
            manifest_stitch: None,
            name: name.into(),
            argv: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: StepKind::SignNativeTarball,
            image: Some("yah-yubaba".into()),
            tag: None,
            push: false,
            platforms: Vec::new(),
            binary_path: None,
            triple: Some("x86_64-unknown-linux-musl".into()),
            package: None,
            context: None,
            source_context: Vec::new(),
            cache: false,
            load: false,
            sub_pipeline: None,
            gha_workflow: None,
            import: None,
            matrix: None,
            enabled: true,
            activation: StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
            outputs: Vec::new(),
        }
    }

    #[test]
    fn sign_native_tarball_happy_path_validates() {
        sign_native_tarball_step("sign").validate().unwrap();
    }

    #[test]
    fn sign_native_tarball_with_argv_rejected() {
        let mut step = sign_native_tarball_step("sign");
        step.argv = vec!["cosign".into()];
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::SignNativeTarballHasArgv("sign".into()),
        );
    }

    #[test]
    fn sign_native_tarball_without_image_rejected() {
        let mut step = sign_native_tarball_step("sign");
        step.image = None;
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::SignNativeTarballMissingImage("sign".into()),
        );
    }

    #[test]
    fn sign_native_tarball_with_container_runtime_rejected() {
        let mut step = sign_native_tarball_step("sign");
        step.runtime = Some(TaskRuntime::Container);
        assert_eq!(
            step.validate().unwrap_err(),
            StepValidationError::SignNativeTarballContainerRuntime("sign".into()),
        );
    }

    #[test]
    fn sign_native_tarball_with_explicit_native_runtime_accepted() {
        let mut step = sign_native_tarball_step("sign");
        step.runtime = Some(TaskRuntime::Native);
        step.validate().unwrap();
    }

    // ── R435-F1 / R555-T7 environment enum serde round-trip ────────────────

    #[test]
    fn environment_round_trip_each_variant() {
        for (variant, kebab) in [
            (Environment::Workstation, "workstation"),
            (Environment::Ci, "ci"),
            (Environment::Any, "any"),
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, format!("\"{kebab}\""), "serialize {variant:?}");
            let parsed: Environment = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, variant, "deserialize {kebab}");
        }
    }

    #[test]
    fn step_platform_block_parses_from_toml() {
        // R531-F2: a step's `[platform]` inline table deserializes into the
        // structured PlatformSpec; omitting it leaves the field None.
        let toml_src = r#"
            name = "p"
            label = "p"
            [[steps]]
            name = "build-musl"
            argv = ["cargo", "build"]
            platform = { target = "x86_64-unknown-linux-musl", container_platform = "linux/amd64" }
            [[steps]]
            name = "check"
            argv = ["cargo", "check"]
        "#;
        let pipeline: Pipeline = toml::from_str(toml_src).unwrap();
        let spec = pipeline.steps[0]
            .platform
            .as_ref()
            .expect("platform parsed");
        assert_eq!(spec.target.as_deref(), Some("x86_64-unknown-linux-musl"));
        assert_eq!(spec.container_platform.as_deref(), Some("linux/amd64"));
        assert!(
            pipeline.steps[1].platform.is_none(),
            "a step without a [platform] block leaves the field None",
        );
    }

    #[test]
    fn environment_defaults_to_any_when_omitted() {
        let toml_src = r#"
            name = "p"
            label = "p"
            steps = []
        "#;
        let pipeline: Pipeline = toml::from_str(toml_src).unwrap();
        assert_eq!(pipeline.environment, Environment::Any);
    }

    #[test]
    fn environment_parses_each_kebab_value_from_toml() {
        for (kebab, expected) in [
            ("workstation", Environment::Workstation),
            ("ci", Environment::Ci),
            ("any", Environment::Any),
        ] {
            let toml_src = format!(
                r#"
                name = "p"
                label = "p"
                environment = "{kebab}"
                steps = []
                "#
            );
            let pipeline: Pipeline = toml::from_str(&toml_src).unwrap();
            assert_eq!(
                pipeline.environment, expected,
                "TOML environment = \"{kebab}\"",
            );
        }
    }

    #[test]
    fn apply_params_leaves_unknown_placeholders_untouched() {
        let mut p = one_step(vec!["{{missing}}"], &[]);
        p.apply_params(&HashMap::new());
        assert_eq!(
            p.steps[0].argv,
            vec!["{{missing}}"],
            "empty params is a no-op"
        );

        let mut params = HashMap::new();
        params.insert("other".to_string(), "v".to_string());
        p.apply_params(&params);
        assert_eq!(
            p.steps[0].argv,
            vec!["{{missing}}"],
            "unknown key left as-is"
        );
    }

    // ----- background sidecar validation (R513-F2) ----------------------------

    #[test]
    fn background_on_subprocess_validates_and_reports_is_background() {
        let mut s = sub_pipeline_step("srv", SubPipelineRef::Builtin("x".into()));
        s.kind = StepKind::Subprocess;
        s.argv = vec!["yah-camp".into()];
        s.background = true;
        assert!(s.is_background());
        assert!(s.validate().is_ok(), "background subprocess step is valid");

        s.background = false;
        s.background_until = Some("test".into());
        assert!(s.is_background(), "background_until implies background");
        assert!(s.validate().is_ok());
    }

    #[test]
    fn background_on_non_subprocess_is_rejected() {
        let mut s = sub_pipeline_step("srv", SubPipelineRef::Builtin("x".into()));
        s.background = true;
        assert!(matches!(
            s.validate(),
            Err(StepValidationError::BackgroundRequiresSubprocess(_))
        ));
    }

    // ----- R717-T1 `inputs` / R717-T2 `secret` --------------------------------

    /// The back-compat contract both fields have to hold: every pipeline TOML in
    /// this camp predates them, so a step that omits them must deserialize, and
    /// re-serializing must not invent keys (`skip_serializing_if` on `inputs`).
    #[test]
    fn a_step_omitting_inputs_and_secret_round_trips_unchanged() {
        let step: QedStep = toml::from_str(
            r#"
            name = "check"
            argv = ["cargo", "check"]
            "#,
        )
        .expect("a pre-R717 step still deserializes");
        assert!(step.inputs.is_empty());
        assert!(!step.secret);
        assert!(step.validate().is_ok());

        let back = toml::to_string(&step).unwrap();
        assert!(
            !back.contains("inputs"),
            "an undeclared `inputs` must not appear in ejected TOML: {back}"
        );
    }

    #[test]
    fn inputs_and_secret_parse_from_toml() {
        let step: QedStep = toml::from_str(
            r#"
            name = "build-iso"
            argv = ["sh", "-c", "build-iso.sh"]
            inputs = [".yah/infra/preseed/build-iso.sh", ".yah/infra/preseed/yah-x86-worker.cfg"]
            secret = true
            "#,
        )
        .unwrap();
        assert_eq!(step.inputs.len(), 2);
        assert_eq!(
            step.inputs[1],
            std::path::PathBuf::from(".yah/infra/preseed/yah-x86-worker.cfg")
        );
        assert!(step.secret);
    }

    /// `secret` promises a suppression the runner only performs on the three
    /// subprocess sinks. Accepting it elsewhere would ship a flag that silently
    /// does nothing on the step an author put it on.
    #[test]
    fn secret_on_non_subprocess_is_rejected() {
        let mut s = sub_pipeline_step("push-kek", SubPipelineRef::Builtin("x".into()));
        s.secret = true;
        assert!(matches!(
            s.validate(),
            Err(StepValidationError::SecretRequiresSubprocess(_))
        ));

        let mut ok = QedStep::default();
        ok.name = "push-kek".into();
        ok.argv = vec!["scp".into(), "kek".into()];
        ok.secret = true;
        assert!(ok.validate().is_ok(), "secret IS valid on a subprocess step");
    }

    /// R560-T8: `source_context` publishes a tarball and sets
    /// `$YAH_SOURCE_CONTEXT_URL` for an argv to fetch. A build-image step
    /// already ships its context through `context` + the R636-B1 `context_url`
    /// transport, and no other kind has an argv at all — so accepting the key
    /// there would upload bytes nothing ever GETs, then delete them. Same
    /// closed-set discipline as `background` and `secret`.
    #[test]
    fn source_context_on_non_subprocess_is_rejected() {
        let mut s = QedStep::default();
        s.name = "image".into();
        s.kind = StepKind::BuildImage;
        s.image = Some("yah-yubaba".into());
        s.source_context = vec![std::path::PathBuf::from("oss/mesofact")];
        assert!(matches!(
            s.validate(),
            Err(StepValidationError::SourceContextRequiresSubprocess(_))
        ));

        let mut ok = QedStep::default();
        ok.name = "build-mesofact".into();
        ok.argv = vec!["build-mesofact.sh …".into()];
        ok.source_context = vec![std::path::PathBuf::from("oss/mesofact")];
        assert!(
            ok.validate().is_ok(),
            "source_context IS valid on a subprocess step",
        );
    }

    /// R876-F4: `cache = true` binds a host dir into the step's container for
    /// its argv to point a toolchain at. Both halves of that sentence are
    /// gates — a kind with no argv cannot use it, and a runtime with no mount
    /// namespace cannot receive it — and both refuse rather than ignore,
    /// because a declared-but-inert cache is the failure this field exists to
    /// avoid (it looks like it works, and every build stays cold).
    #[test]
    fn cache_requires_a_containerised_subprocess_step() {
        let mut wrong_kind = QedStep::default();
        wrong_kind.name = "image".into();
        wrong_kind.kind = StepKind::BuildImage;
        wrong_kind.image = Some("yah-yubaba".into());
        wrong_kind.cache = true;
        assert!(matches!(
            wrong_kind.validate(),
            Err(StepValidationError::CacheRequiresSubprocess(_))
        ));

        let mut native = QedStep::default();
        native.name = "build".into();
        native.argv = vec!["cargo build".into()];
        native.runtime = Some(TaskRuntime::Native);
        native.cache = true;
        assert!(matches!(
            native.validate(),
            Err(StepValidationError::CacheRequiresContainerRuntime(_))
        ));

        let mut ok = QedStep::default();
        ok.name = "build-mesofact".into();
        ok.argv = vec!["build-mesofact.sh …".into()];
        ok.runtime = Some(TaskRuntime::Container);
        ok.cache = true;
        assert!(
            ok.validate().is_ok(),
            "cache IS valid on a containerised subprocess step",
        );
    }

    /// A secret step's `$YAH_OUTPUTS` is dropped unread, so a *declared* output
    /// would be permanently empty and a `[[bind]]` reading it would bind
    /// nothing — silently. Reject the pair where the author can still see it.
    #[test]
    fn secret_cannot_declare_outputs() {
        let mut s = QedStep::default();
        s.name = "kek-push".into();
        s.argv = vec!["true".into()];
        s.secret = true;
        s.outputs = vec![OutputDecl {
            name: "fingerprint".into(),
            description: None,
            kind: manifest_bind::ValueType::String,
            validate: None,
        }];
        assert!(matches!(
            s.validate(),
            Err(StepValidationError::SecretCannotDeclareOutputs(_))
        ));

        s.secret = false;
        assert!(s.validate().is_ok(), "outputs alone are fine");
    }

    /// `inputs` is deliberately NOT kind-restricted: freshness is a property of
    /// the declared sources, not of how the step executes, and a `build-image`
    /// step whose Dockerfile moved is exactly as stale as a subprocess one.
    #[test]
    fn inputs_are_accepted_on_every_step_kind() {
        let mut s = sub_pipeline_step("child", SubPipelineRef::Builtin("x".into()));
        s.inputs = vec![std::path::PathBuf::from("Cargo.toml")];
        assert!(s.validate().is_ok());
    }

    // ----- R717-T3 CellRef + param fingerprint --------------------------------

    /// The equivalence the whole mechanism rests on: two operators who reach the
    /// same effective params — different insertion order, one via `default` —
    /// must land on one subject, or W257's badge for a box splits in half.
    #[test]
    fn fingerprint_is_stable_across_param_orderings() {
        let mut a = HashMap::new();
        a.insert("node".to_string(), "us-west-003".to_string());
        a.insert("channel".to_string(), "stable".to_string());

        let mut b = HashMap::new();
        b.insert("channel".to_string(), "stable".to_string());
        b.insert("node".to_string(), "us-west-003".to_string());

        assert_eq!(param_fingerprint(&a), param_fingerprint(&b));
        assert_eq!(param_fingerprint(&a).len(), 64, "blake3 hex");
    }

    #[test]
    fn fingerprint_separates_two_subjects() {
        let one = HashMap::from([("node".to_string(), "us-west-003".to_string())]);
        let other = HashMap::from([("node".to_string(), "us-west-013".to_string())]);
        assert_ne!(
            param_fingerprint(&one),
            param_fingerprint(&other),
            "W257 must render green for one box and unrun for the other AT THE SAME TIME"
        );
    }

    /// Length-prefixed framing, not a delimiter join. `{node: "a", x: "=b"}` and
    /// `{node: "a=", x: "b"}` would collide under a naive `format!("{k}={v}")`
    /// concatenation, and a collision here shows one box's verdict for another.
    #[test]
    fn fingerprint_cannot_be_forged_by_a_value_containing_the_delimiter() {
        let one = HashMap::from([
            ("node".to_string(), "a".to_string()),
            ("x".to_string(), "=b".to_string()),
        ]);
        let other = HashMap::from([
            ("node".to_string(), "a=".to_string()),
            ("x".to_string(), "b".to_string()),
        ]);
        assert_ne!(param_fingerprint(&one), param_fingerprint(&other));
    }

    #[test]
    fn an_empty_value_is_not_the_same_subject_as_an_absent_one() {
        let empty = HashMap::from([("node".to_string(), String::new())]);
        assert_ne!(param_fingerprint(&empty), param_fingerprint(&HashMap::new()));
        // A doc with no params has exactly one subject — legal, not an error.
        assert_eq!(param_fingerprint(&HashMap::new()).len(), 64);
    }

    /// The ~494 run metas already on disk carry neither `cell` nor per-step
    /// `input_hashes`. They have to keep loading, untouched.
    #[test]
    fn a_pre_r717_run_meta_still_deserializes() {
        let json = r#"{
            "id": "run-1",
            "pipeline": "check",
            "status": "success",
            "created_at": "2026-05-26T04:09:53Z",
            "completed_at": "2026-05-26T04:11:00Z",
            "steps": [{
                "name": "cargo check",
                "task_run_id": null,
                "status": "success",
                "started_at": "2026-05-26T04:09:54Z",
                "completed_at": "2026-05-26T04:10:59Z",
                "outputs": {},
                "applied_binds": []
            }]
        }"#;
        let meta: QedRunMeta = serde_json::from_str(json).expect("pre-R717 meta loads");
        assert!(meta.cell.is_none());
        assert!(meta.steps[0].input_hashes.is_empty());

        // And round-tripping must not invent the new keys on a run that has none.
        let back = serde_json::to_string(&meta).unwrap();
        assert!(!back.contains("\"cell\""), "{back}");
        assert!(!back.contains("input_hashes"), "{back}");
    }

    #[test]
    fn a_cell_ref_round_trips_through_the_run_meta() {
        let json = r#"{
            "id": "run-2",
            "pipeline": "W257",
            "status": "success",
            "created_at": "2026-08-07T00:00:00Z",
            "completed_at": null,
            "steps": [],
            "cell": {
                "doc": ".yah/docs/working/W257-static-node-fleet-onboarding.md",
                "cell_id": "probe-identity",
                "param_fingerprint": "abc123"
            }
        }"#;
        let meta: QedRunMeta = serde_json::from_str(json).unwrap();
        let cell = meta.cell.clone().expect("cell parsed");
        assert_eq!(cell.cell_id, "probe-identity");
        let back: QedRunMeta = serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap();
        assert_eq!(back.cell, meta.cell);
    }

    // ----- SubPipeline (W201-F1) ----------------------------------------------

    fn sub_pipeline_step(name: &str, target: SubPipelineRef) -> QedStep {
        QedStep {
            expect_slow: false,
            participant: None,
            needs: None,
            resource: None,
            inputs: Vec::new(),
            secret: false,
            background: false,
            background_until: None,
            wait_for: None,
            manual: None,
            manifest_stitch: None,
            name: name.into(),
            argv: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: StepKind::SubPipeline,
            image: None,
            tag: None,
            push: false,
            platforms: Vec::new(),
            binary_path: None,
            triple: None,
            package: None,
            context: None,
            source_context: Vec::new(),
            cache: false,
            load: false,
            sub_pipeline: Some(SubPipelineConfig {
                target,
                params: HashMap::new(),
                propagate: SubPipelineCollect::default(),
                opaque: false,
                own_workspace: None,
            }),
            outputs: Vec::new(),
            gha_workflow: None,
            import: None,
            matrix: None,
            enabled: true,
            activation: crate::types::StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
        }
    }

    fn pipeline_with(name: &str, steps: Vec<QedStep>) -> Pipeline {
        Pipeline {
            allow_late_operator_block: false,
            participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: name.into(),
            label: name.into(),
            steps,
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: Environment::default(),
            workspace: crate::types::WorkspaceMode::default(),
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        }
    }

    #[test]
    fn sub_pipeline_step_validates_when_well_formed() {
        let step = sub_pipeline_step("compose", SubPipelineRef::Builtin("desktop-release".into()));
        assert!(step.validate().is_ok());
    }

    #[test]
    fn sub_pipeline_step_rejects_argv() {
        let mut step = sub_pipeline_step("compose", SubPipelineRef::Builtin("x".into()));
        step.argv = vec!["echo".into()];
        assert_eq!(
            step.validate(),
            Err(StepValidationError::SubPipelineHasArgv("compose".into()))
        );
    }

    #[test]
    fn sub_pipeline_step_rejects_missing_config() {
        let mut step = sub_pipeline_step("compose", SubPipelineRef::Builtin("x".into()));
        step.sub_pipeline = None;
        assert_eq!(
            step.validate(),
            Err(StepValidationError::SubPipelineMissingConfig(
                "compose".into()
            ))
        );
    }

    fn gha_workflow_step(name: &str) -> QedStep {
        let mut step = sub_pipeline_step(name, SubPipelineRef::Builtin("x".into()));
        step.kind = StepKind::GhaWorkflow;
        step.sub_pipeline = None;
        step.gha_workflow = Some(GhaWorkflowConfig {
            path: std::path::PathBuf::from(".github/workflows/release.yml"),
            event: Some("push".into()),
            inputs: HashMap::new(),
            matrix: HashMap::new(),
        });
        step
    }

    #[test]
    fn gha_workflow_step_validates_when_well_formed() {
        let step = gha_workflow_step("run-release-yml");
        assert!(step.validate().is_ok());
    }

    #[test]
    fn gha_workflow_step_rejects_argv() {
        let mut step = gha_workflow_step("run");
        step.argv = vec!["echo".into()];
        assert_eq!(
            step.validate(),
            Err(StepValidationError::GhaWorkflowHasArgv("run".into()))
        );
    }

    #[test]
    fn gha_workflow_step_rejects_missing_config() {
        let mut step = gha_workflow_step("run");
        step.gha_workflow = None;
        assert_eq!(
            step.validate(),
            Err(StepValidationError::GhaWorkflowMissingConfig("run".into()))
        );
    }

    // ── R533-F1 (W224): import step ────────────────────────────────────────

    fn import_step(name: &str) -> QedStep {
        let mut step = gha_workflow_step(name);
        step.kind = StepKind::Import;
        step.gha_workflow = None;
        step.import = Some(ImportConfig {
            source: std::path::PathBuf::from(".github/workflows/release.yml"),
            hash: Some("af1349b9f5f9a1a6a0404dea36dcc949".into()),
            materialize: false,
            event: Some("push".into()),
            inputs: HashMap::new(),
        });
        step
    }

    #[test]
    fn import_step_validates_when_well_formed() {
        assert!(import_step("release").validate().is_ok());
    }

    #[test]
    fn import_step_rejects_argv() {
        let mut step = import_step("release");
        step.argv = vec!["echo".into()];
        assert_eq!(
            step.validate(),
            Err(StepValidationError::ImportHasArgv("release".into()))
        );
    }

    #[test]
    fn import_step_rejects_missing_config() {
        let mut step = import_step("release");
        step.import = None;
        assert_eq!(
            step.validate(),
            Err(StepValidationError::ImportMissingConfig("release".into()))
        );
    }

    #[test]
    fn import_step_round_trips_through_toml() {
        // The `[import]` block survives a TOML serialize → deserialize cycle,
        // including the pinned hash and the default-false materialize toggle.
        let step = import_step("release");
        let toml_str = toml::to_string(&step).expect("serialize import step");
        assert!(toml_str.contains("kind = \"import\""), "{toml_str}");
        assert!(
            toml_str.contains("source = \".github/workflows/release.yml\""),
            "{toml_str}"
        );
        let parsed: QedStep = toml::from_str(&toml_str).expect("deserialize import step");
        assert_eq!(parsed.kind, StepKind::Import);
        let cfg = parsed.import.expect("import block present");
        assert_eq!(
            cfg.source,
            std::path::PathBuf::from(".github/workflows/release.yml")
        );
        assert_eq!(
            cfg.hash.as_deref(),
            Some("af1349b9f5f9a1a6a0404dea36dcc949")
        );
        assert!(!cfg.materialize, "materialize defaults false (virtual)");
        assert_eq!(cfg.event.as_deref(), Some("push"));
    }

    #[test]
    fn import_block_defaults_materialize_false_and_unpinned() {
        // A minimal `[import]` with only `source` parses — hash unpinned,
        // materialize off (virtual-by-default).
        let toml_str = r#"
            name = "release"
            kind = "import"
            [import]
            source = ".github/workflows/release.yml"
        "#;
        let step: QedStep = toml::from_str(toml_str).expect("parse minimal import");
        step.validate().expect("minimal import validates");
        let cfg = step.import.expect("import block");
        assert_eq!(cfg.hash, None, "unpinned by default");
        assert!(!cfg.materialize);
        assert_eq!(cfg.event, None);
    }

    // ── R590-F2: manifest-stitch step ──────────────────────────────────────

    fn manifest_stitch_step(name: &str) -> QedStep {
        let mut step = gha_workflow_step(name);
        step.kind = StepKind::ManifestStitch;
        step.gha_workflow = None;
        step.argv = vec![];
        step.manifest_stitch = Some(ManifestStitchConfig {
            target: "ghcr.io/yah-ai/yah-rust:v1".into(),
            sources: vec![
                "ghcr.io/yah-ai/yah-rust:v1-amd64".into(),
                "ghcr.io/yah-ai/yah-rust:v1-arm64".into(),
            ],
        });
        step
    }

    #[test]
    fn manifest_stitch_step_validates_when_well_formed() {
        assert!(manifest_stitch_step("stitch").validate().is_ok());
    }

    #[test]
    fn manifest_stitch_step_rejects_argv() {
        let mut step = manifest_stitch_step("stitch");
        step.argv = vec!["docker".into()];
        assert_eq!(
            step.validate(),
            Err(StepValidationError::ManifestStitchHasArgv("stitch".into()))
        );
    }

    #[test]
    fn manifest_stitch_step_rejects_missing_config() {
        let mut step = manifest_stitch_step("stitch");
        step.manifest_stitch = None;
        assert_eq!(
            step.validate(),
            Err(StepValidationError::ManifestStitchMissingConfig("stitch".into()))
        );
    }

    #[test]
    fn manifest_stitch_step_rejects_empty_target() {
        let mut step = manifest_stitch_step("stitch");
        step.manifest_stitch.as_mut().unwrap().target = "  ".into();
        assert_eq!(
            step.validate(),
            Err(StepValidationError::ManifestStitchMissingTarget("stitch".into()))
        );
    }

    #[test]
    fn manifest_stitch_step_rejects_no_sources() {
        let mut step = manifest_stitch_step("stitch");
        step.manifest_stitch.as_mut().unwrap().sources = vec![];
        assert_eq!(
            step.validate(),
            Err(StepValidationError::ManifestStitchNeedsSources("stitch".into()))
        );
    }

    #[test]
    fn manifest_stitch_step_round_trips_through_toml() {
        let step = manifest_stitch_step("stitch");
        let toml_str = toml::to_string(&step).expect("serialize manifest-stitch step");
        assert!(toml_str.contains("kind = \"manifest-stitch\""), "{toml_str}");
        let parsed: QedStep = toml::from_str(&toml_str).expect("deserialize manifest-stitch step");
        assert_eq!(parsed.kind, StepKind::ManifestStitch);
        let cfg = parsed.manifest_stitch.expect("manifest_stitch block present");
        assert_eq!(cfg.target, "ghcr.io/yah-ai/yah-rust:v1");
        assert_eq!(cfg.sources.len(), 2);
    }

    // ── R513-F3 (W207 Gap #5): wait-for step ───────────────────────────────

    fn wait_for_step(name: &str, cfg: WaitForConfig) -> QedStep {
        let mut step = gha_workflow_step(name);
        step.kind = StepKind::WaitFor;
        step.gha_workflow = None;
        step.argv = vec![];
        step.wait_for = Some(cfg);
        step
    }

    fn http_wait(url: &str) -> WaitForConfig {
        WaitForConfig {
            http: Some(url.into()),
            ..Default::default()
        }
    }

    #[test]
    fn wait_for_step_validates_http_and_tcp() {
        assert!(wait_for_step("gate", http_wait("http://localhost:3000/health"))
            .validate()
            .is_ok());
        let tcp = WaitForConfig {
            http: None,
            tcp: Some("127.0.0.1:5432".into()),
            ..http_wait("ignored")
        };
        // Clear the http set by the spread.
        let mut step = wait_for_step("gate", tcp);
        step.wait_for.as_mut().unwrap().http = None;
        assert!(step.validate().is_ok());
    }

    #[test]
    fn wait_for_step_rejects_argv() {
        let mut step = wait_for_step("gate", http_wait("http://localhost/health"));
        step.argv = vec!["curl".into()];
        assert_eq!(
            step.validate(),
            Err(StepValidationError::WaitForHasArgv("gate".into()))
        );
    }

    #[test]
    fn wait_for_step_rejects_missing_config() {
        let mut step = wait_for_step("gate", http_wait("http://localhost/health"));
        step.wait_for = None;
        assert_eq!(
            step.validate(),
            Err(StepValidationError::WaitForMissingConfig("gate".into()))
        );
    }

    #[test]
    fn wait_for_step_rejects_no_target_and_both_targets() {
        let neither = wait_for_step(
            "gate",
            WaitForConfig::default(),
        );
        assert_eq!(
            neither.validate(),
            Err(StepValidationError::WaitForNeedsTarget("gate".into()))
        );

        let both = wait_for_step(
            "gate",
            WaitForConfig {
                http: Some("http://localhost/health".into()),
                tcp: Some("localhost:80".into()),
                ..Default::default()
            },
        );
        assert_eq!(
            both.validate(),
            Err(StepValidationError::WaitForAmbiguousTarget("gate".into()))
        );
    }

    #[test]
    fn wait_for_step_rejects_expect_status_on_tcp() {
        let step = wait_for_step(
            "gate",
            WaitForConfig {
                tcp: Some("localhost:5432".into()),
                expect_status: Some(200),
                ..Default::default()
            },
        );
        assert_eq!(
            step.validate(),
            Err(StepValidationError::WaitForStatusNeedsHttp("gate".into()))
        );
    }

    #[test]
    fn wait_for_step_accepts_shell_target() {
        let step = wait_for_step(
            "gate",
            WaitForConfig {
                shell: Some("curl -fsS http://example.com".into()),
                ..Default::default()
            },
        );
        assert!(step.validate().is_ok());
    }

    #[test]
    fn wait_for_step_rejects_shell_combined_with_http() {
        let step = wait_for_step(
            "gate",
            WaitForConfig {
                http: Some("http://localhost/health".into()),
                shell: Some("true".into()),
                ..Default::default()
            },
        );
        assert_eq!(
            step.validate(),
            Err(StepValidationError::WaitForAmbiguousTarget("gate".into()))
        );
    }

    #[test]
    fn wait_for_step_rejects_zero_interval() {
        let mut step = wait_for_step("gate", http_wait("http://localhost/health"));
        step.wait_for.as_mut().unwrap().interval_ms = 0;
        assert_eq!(
            step.validate(),
            Err(StepValidationError::WaitForZeroInterval("gate".into()))
        );
    }

    #[test]
    fn wait_for_step_rejects_bad_backoff_multiplier() {
        let mut step = wait_for_step("gate", http_wait("http://localhost/health"));
        step.wait_for.as_mut().unwrap().backoff_multiplier = 0.5;
        assert_eq!(
            step.validate(),
            Err(StepValidationError::WaitForBadBackoffMultiplier("gate".into()))
        );
    }

    #[test]
    fn wait_for_step_rejects_zero_timeout() {
        let mut step = wait_for_step("gate", http_wait("http://localhost/health"));
        step.wait_for.as_mut().unwrap().timeout_secs = 0;
        assert_eq!(
            step.validate(),
            Err(StepValidationError::WaitForZeroTimeout("gate".into()))
        );
    }

    #[test]
    fn wait_for_block_defaults_timeout_and_interval() {
        // A minimal `[wait_for]` with only `http` parses — timeout/interval
        // fall back to their defaults (30s / 500ms).
        let toml_str = r#"
            name = "wait:ready"
            kind = "wait-for"
            [wait_for]
            http = "http://localhost:3000/health"
        "#;
        let step: QedStep = toml::from_str(toml_str).expect("parse minimal wait-for");
        step.validate().expect("minimal wait-for validates");
        let cfg = step.wait_for.expect("wait_for block");
        assert_eq!(cfg.timeout_secs, 30);
        assert_eq!(cfg.interval_ms, 500);
        assert_eq!(cfg.expect_status, None);
    }

    #[test]
    fn wait_for_step_round_trips_through_toml() {
        let step = wait_for_step(
            "wait:ready",
            WaitForConfig {
                http: Some("http://localhost:3000/health".into()),
                expect_status: Some(204),
                timeout_secs: 45,
                interval_ms: 250,
                ..Default::default()
            },
        );
        let toml_str = toml::to_string(&step).expect("serialize wait-for step");
        assert!(toml_str.contains("kind = \"wait-for\""), "{toml_str}");
        let parsed: QedStep = toml::from_str(&toml_str).expect("deserialize wait-for step");
        assert_eq!(parsed.kind, StepKind::WaitFor);
        let cfg = parsed.wait_for.expect("wait_for block present");
        assert_eq!(cfg.http.as_deref(), Some("http://localhost:3000/health"));
        assert_eq!(cfg.expect_status, Some(204));
        assert_eq!(cfg.timeout_secs, 45);
        assert_eq!(cfg.interval_ms, 250);
    }

    // ── R513-F4 (W207 Gap #6): finally teardown step validation ────────────

    /// Build a plain subprocess step from the populated `gha_workflow_step`
    /// literal so new QedStep fields don't need threading here.
    fn subprocess_step(name: &str) -> QedStep {
        let mut step = gha_workflow_step(name);
        step.kind = StepKind::Subprocess;
        step.gha_workflow = None;
        step.argv = vec!["echo".into(), "bye".into()];
        step
    }

    #[test]
    fn finally_accepts_subprocess() {
        assert!(subprocess_step("teardown").validate_finally().is_ok());
    }

    #[test]
    fn finally_rejects_non_subprocess_kind() {
        let wf = wait_for_step("gate", http_wait("http://localhost/health"));
        assert_eq!(
            wf.validate_finally(),
            Err(StepValidationError::FinallyRequiresSubprocess("gate".into()))
        );
    }

    #[test]
    fn finally_rejects_background_subprocess() {
        let mut bg = subprocess_step("bg");
        bg.background = true;
        assert_eq!(
            bg.validate_finally(),
            Err(StepValidationError::FinallyRequiresSubprocess("bg".into()))
        );
    }

    #[test]
    fn sub_pipeline_step_rejects_direct_produces() {
        let mut step = sub_pipeline_step("compose", SubPipelineRef::Builtin("x".into()));
        step.produces = vec![ProducedArtifact {
            binary: "yah".into(),
            path: "target/release/yah".into(),
            triple: None,
        }];
        assert_eq!(
            step.validate(),
            Err(StepValidationError::SubPipelineHasProduces(
                "compose".into()
            ))
        );
    }

    /// Test resolver backed by a HashMap so unit tests can stub the
    /// pipeline graph without touching disk or builtins.
    struct MapResolver(HashMap<String, Pipeline>);

    impl SubPipelineResolver for MapResolver {
        fn resolve(&self, target: &SubPipelineRef) -> Option<Pipeline> {
            let key = match target {
                SubPipelineRef::Builtin(n) => format!("builtin:{n}"),
                SubPipelineRef::Path(p) => format!("path:{}", p.display()),
                SubPipelineRef::GhaWorkflow { path, .. } => format!("gha:{}", path.display()),
                SubPipelineRef::Peer { camp, pipeline } => format!("peer:{camp}:{pipeline}"),
            };
            self.0.get(&key).cloned()
        }
    }

    #[test]
    fn graph_walk_accepts_acyclic_chain() {
        // root -> child-a -> child-b (no cycles)
        let leaf = pipeline_with("child-b", vec![]);
        let mid = pipeline_with(
            "child-a",
            vec![sub_pipeline_step(
                "descend",
                SubPipelineRef::Builtin("child-b".into()),
            )],
        );
        let root = pipeline_with(
            "root",
            vec![sub_pipeline_step(
                "descend",
                SubPipelineRef::Builtin("child-a".into()),
            )],
        );
        let mut map = HashMap::new();
        map.insert("builtin:child-a".to_string(), mid);
        map.insert("builtin:child-b".to_string(), leaf);
        let resolver = MapResolver(map);
        assert!(validate_sub_pipeline_graph(&root, &resolver).is_ok());
    }

    /// R887. `yah-cli-release` declares `workspace = "isolated"` and was
    /// composed by the `live` release wizard with nothing said either way, so
    /// it built in the wizard's live tree — its `stamp-build-id` step read the
    /// camp root's 38 uncommitted files and refused to stamp, inside what its
    /// own error called an impossible state. The third time that shape shipped.
    #[test]
    fn an_isolated_child_under_a_live_parent_must_state_which_tree() {
        let mut child = pipeline_with("cli-release", vec![]);
        child.workspace = WorkspaceMode::Isolated;
        let mut root = pipeline_with(
            "wizard",
            vec![sub_pipeline_step(
                "publish-cli",
                SubPipelineRef::Builtin("cli-release".into()),
            )],
        );
        root.workspace = WorkspaceMode::Live;
        let mut map = HashMap::new();
        map.insert("builtin:cli-release".to_string(), child);
        let resolver = MapResolver(map);

        let err = validate_sub_pipeline_graph(&root, &resolver).unwrap_err();
        match err {
            SubPipelineError::WorkspaceInheritanceUnstated {
                ref step,
                ref child,
                ref child_mode,
                ref inherited,
            } => {
                assert_eq!(step, "publish-cli");
                assert_eq!(child, "cli-release");
                // The message has to name BOTH trees: an author who reads only
                // "declares isolated" fixes it by writing `own_workspace =
                // true` even when the child needed the live tree.
                assert_eq!(child_mode, "isolated");
                assert_eq!(inherited, "live");
                let msg = err.to_string();
                assert!(msg.contains("own_workspace = true"), "{msg}");
                assert!(msg.contains("own_workspace = false"), "{msg}");
            }
            other => panic!("expected WorkspaceInheritanceUnstated, got {other:?}"),
        }

        // Either answer clears it — the rule is that one of them is written
        // down, not which one.
        for stated in [Some(true), Some(false)] {
            let mut stated_root = root.clone();
            stated_root.steps[0].sub_pipeline.as_mut().unwrap().own_workspace = stated;
            assert!(
                validate_sub_pipeline_graph(&stated_root, &resolver).is_ok(),
                "own_workspace = {stated:?} states the intent and must pass"
            );
        }
    }

    /// The other half of the rule: inheritance that already DELIVERS what the
    /// child declared is not a conflict, so composing a release inside a
    /// release stays silent (`yah-release` → `yah-desktop-release`, both
    /// `isolated`) — including through a child that repositioned.
    #[test]
    fn an_isolated_child_of_an_isolated_tree_needs_no_statement() {
        let mut leaf = pipeline_with("desktop-release", vec![]);
        leaf.workspace = WorkspaceMode::Isolated;
        let mut mid = pipeline_with(
            "release",
            vec![sub_pipeline_step(
                "desktop",
                SubPipelineRef::Builtin("desktop-release".into()),
            )],
        );
        mid.workspace = WorkspaceMode::Isolated;
        let mut map = HashMap::new();
        map.insert("builtin:desktop-release".to_string(), leaf);
        map.insert("builtin:release".to_string(), mid.clone());
        let resolver = MapResolver(map);

        // Isolated root -> isolated child, nothing stated.
        assert!(validate_sub_pipeline_graph(&mid, &resolver).is_ok());

        // Live root -> `own_workspace = true` child (now isolated) -> isolated
        // grandchild, nothing stated on the grandchild: the tree it inherits
        // is already a worktree, so there is nothing to decide.
        let mut root = pipeline_with(
            "wizard",
            vec![sub_pipeline_step(
                "compose",
                SubPipelineRef::Builtin("release".into()),
            )],
        );
        root.workspace = WorkspaceMode::Live;
        root.steps[0].sub_pipeline.as_mut().unwrap().own_workspace = Some(true);
        assert!(validate_sub_pipeline_graph(&root, &resolver).is_ok());

        // …and with the same child INHERITING the live tree instead, the
        // grandchild's own `isolated` is the one being discarded — same
        // failure, one level down, which is why the walk threads the
        // effective mode rather than reading the parent's declaration.
        root.steps[0].sub_pipeline.as_mut().unwrap().own_workspace = Some(false);
        let err = validate_sub_pipeline_graph(&root, &resolver).unwrap_err();
        assert!(
            matches!(
                err,
                SubPipelineError::WorkspaceInheritanceUnstated { ref step, .. } if step == "desktop"
            ),
            "expected the GRANDCHILD's step to be named, got {err:?}"
        );
    }

    #[test]
    fn graph_walk_detects_direct_self_cycle() {
        // root -> root (builtin name matches itself's name — irrelevant to the
        // walker, but a likely real-world mistake)
        let root = pipeline_with(
            "self",
            vec![sub_pipeline_step(
                "loop",
                SubPipelineRef::Builtin("self".into()),
            )],
        );
        // child resolves back to root with same ref token => cycle.
        let mut map = HashMap::new();
        map.insert("builtin:self".to_string(), root.clone());
        let resolver = MapResolver(map);
        // Add the SubPipeline step to root so root's body contains the
        // self-reference (above already does — this is just a clarity assertion).
        assert_eq!(root.steps.len(), 1);
        let err = validate_sub_pipeline_graph(&root, &resolver).unwrap_err();
        match err {
            SubPipelineError::Cycle { chain } => {
                assert!(
                    chain.contains("builtin:self"),
                    "cycle chain reports the ref: {chain}"
                );
            }
            other => panic!("expected Cycle, got {other:?}"),
        }
    }

    #[test]
    fn graph_walk_detects_indirect_cycle() {
        // root -> a -> b -> a
        let a_loops_back = pipeline_with(
            "a",
            vec![sub_pipeline_step(
                "descend",
                SubPipelineRef::Builtin("b".into()),
            )],
        );
        let b_back_to_a = pipeline_with(
            "b",
            vec![sub_pipeline_step(
                "loop",
                SubPipelineRef::Builtin("a".into()),
            )],
        );
        let root = pipeline_with(
            "root",
            vec![sub_pipeline_step(
                "enter",
                SubPipelineRef::Builtin("a".into()),
            )],
        );
        let mut map = HashMap::new();
        map.insert("builtin:a".to_string(), a_loops_back);
        map.insert("builtin:b".to_string(), b_back_to_a);
        let resolver = MapResolver(map);
        let err = validate_sub_pipeline_graph(&root, &resolver).unwrap_err();
        match err {
            SubPipelineError::Cycle { chain } => {
                assert!(chain.contains("builtin:a"));
                assert!(chain.contains("builtin:b"));
            }
            other => panic!("expected Cycle, got {other:?}"),
        }
    }

    #[test]
    fn graph_walk_rejects_beyond_max_depth() {
        // Build a linear chain root -> d1 -> d2 -> d3 -> d4 -> d5 with no cycles.
        // MAX_SUB_PIPELINE_DEPTH = 4 so the 5th edge must fail.
        let mut map: HashMap<String, Pipeline> = HashMap::new();
        for n in (1..=5).rev() {
            let next_step = if n < 5 {
                vec![sub_pipeline_step(
                    "descend",
                    SubPipelineRef::Builtin(format!("d{}", n + 1)),
                )]
            } else {
                vec![]
            };
            let p = pipeline_with(&format!("d{n}"), next_step);
            map.insert(format!("builtin:d{n}"), p);
        }
        let root = pipeline_with(
            "root",
            vec![sub_pipeline_step(
                "enter",
                SubPipelineRef::Builtin("d1".into()),
            )],
        );
        let resolver = MapResolver(map);
        let err = validate_sub_pipeline_graph(&root, &resolver).unwrap_err();
        assert!(
            matches!(
                err,
                SubPipelineError::MaxDepthExceeded {
                    max: MAX_SUB_PIPELINE_DEPTH,
                    ..
                }
            ),
            "expected MaxDepthExceeded, got {err:?}"
        );
    }

    #[test]
    fn graph_walk_tolerates_unresolved_refs() {
        // Resolver returns None — the walker should not error; runtime
        // surfaces the resolution failure later.
        let root = pipeline_with(
            "root",
            vec![sub_pipeline_step(
                "enter",
                SubPipelineRef::Builtin("nonexistent".into()),
            )],
        );
        let resolver = MapResolver(HashMap::new());
        assert!(validate_sub_pipeline_graph(&root, &resolver).is_ok());
    }

    #[test]
    fn sub_pipeline_round_trips_through_toml_with_all_three_ref_shapes() {
        for target_toml in [
            r#"target = { builtin = "desktop-release" }"#,
            r#"target = { path = ".yah/qed/full-release.toml" }"#,
            r#"target = { gha-workflow = { path = ".github/workflows/release.yml", event = "tag" } }"#,
            r#"target = { peer = { camp = "mesofact", pipeline = "release-build" } }"#,
        ] {
            let toml_src = format!(
                r#"
                name = "p"
                label = "p"

                [[steps]]
                name = "compose"
                kind = "sub-pipeline"

                [steps.sub_pipeline]
                {target_toml}
                propagate = {{ produces = true }}
                "#
            );
            let pipeline: Pipeline = toml::from_str(&toml_src)
                .unwrap_or_else(|e| panic!("parse failed for `{target_toml}`: {e}"));
            assert_eq!(pipeline.steps.len(), 1);
            let cfg = pipeline.steps[0].sub_pipeline.as_ref().unwrap();
            assert!(cfg.propagate.produces);
        }
    }

    #[test]
    fn graph_walk_detects_peer_cycle() {
        // root -> peer:cheers:publish -> peer:cheers:publish (self-loop via peer ref)
        let cheers = pipeline_with(
            "publish",
            vec![sub_pipeline_step(
                "republish",
                SubPipelineRef::Peer {
                    camp: "cheers".into(),
                    pipeline: "publish".into(),
                },
            )],
        );
        let root = pipeline_with(
            "root",
            vec![sub_pipeline_step(
                "kick",
                SubPipelineRef::Peer {
                    camp: "cheers".into(),
                    pipeline: "publish".into(),
                },
            )],
        );
        let mut map = HashMap::new();
        map.insert("peer:cheers:publish".to_string(), cheers);
        let resolver = MapResolver(map);
        let err = validate_sub_pipeline_graph(&root, &resolver).unwrap_err();
        match err {
            SubPipelineError::Cycle { chain } => {
                assert!(chain.contains("peer:cheers:publish"), "chain: {chain}");
            }
            other => panic!("expected Cycle, got {other:?}"),
        }
    }
}
