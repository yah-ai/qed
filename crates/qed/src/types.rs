//! @yah:ticket(R325-F3, "Backend: run-history persistence — QedRunId + step results queryable")
//! @yah:at(2026-05-26T04:09:53Z)
//! @yah:status(review)
//! @yah:phase(P3)
//! @yah:parent(R325)
//! @yah:depends_on(R325-F1)
//! @yah:handoff("Landed run-history persistence (R325-F3). Terminal QedRunMeta (success/failed/cancelled) written to <camp_root>/.yah/jit/qed/<run_id>.json on each run's completion. Both qed_run_handler (background task, success+error paths) and qed_cancel_handler call persist_qed_run() — errors logged but never fatal. On daemon startup (both run_with_shutdown and make_daemon_state) load_qed_history() scans .yah/jit/qed/*.json and rehydrates qed_runs with empty event buffers and no abort handles (all historical runs are terminal). Events (stdout/stderr lines) are NOT persisted — history stores meta+step statuses only, matching the ticket's 'step results queryable' scope. 2 new tests: run_history_persists_and_reloads (happy path + reload) and cancelled_run_persists; all 11 r325 tests pass + 21 qed tests pass + cargo check clean.")
//! @yah:next("R325-T4: Tauri commands exposing qed list/run/status/stream/history to desktop. The persistence store is now stable — qed.list and qed.status serve historical runs. qed.tail returns empty events for loaded-from-disk runs (events were in-memory only); that is expected.")
//! @yah:verify("cargo test -p qed --lib")
//! @yah:verify("cargo test -p yah --lib r325")
//! @yah:verify("cargo check -p yah -p desktop")
//!
//! @yah:relay(R435, "QED recipe discipline rollout (W170)")
//! @yah:at(2026-06-04T19:15:34Z)
//! @yah:status(open)
//! @arch:see(.yah/docs/working/W170-qed-recipe-discipline.md)
//!
//! @yah:ticket(R435-F1, "Add `placement` field to QED Pipeline schema (local-only / ci-only / anywhere)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-04T19:15:56Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R435)
//! @yah:next("Add `placement: Placement` to the [pipeline] struct in types.rs with serde rename_all=\"kebab-case\"")
//! @yah:next("Default to `anywhere` so existing recipes keep working")
//! @yah:next("Surface in `yah qed list`/`tail` headers so operators see placement at a glance")
//! @yah:next("Update the JSON schema (if any) so recipe authors get autocomplete")
//! @yah:verify("cargo test -p qed --lib parses each enum variant via round-trip")
//! @yah:verify("Existing recipes still load (default = anywhere) without edits")
//! @arch:see(.yah/docs/working/W170-qed-recipe-discipline.md)
//! @yah:handoff("F1 complete. Added `Placement` enum (`local-only` / `ci-only` / `anywhere`, default Anywhere) and `Pipeline.placement: Placement` (#[serde(default)]) in types.rs. PipelineConfig in config.rs mirrors the field and threads it through load_from_str/load_from_file. All 11 Pipeline struct literals (builtins.rs ×3, runner.rs ×7, types.rs test ×1) updated. New tests in types.rs::tests: placement_round_trip_each_variant, placement_defaults_to_anywhere_when_omitted, placement_parses_each_kebab_value_from_toml — 3/3 green. `cargo check --workspace` clean. Full qed lib suite: 156 pass; the single failure (test_builtin_release_build_pipeline, 4-vs-6 step assertion) is pre-existing and explicitly flagged in R380-T3's handoff — unrelated to this ticket. Existing recipes still load (placement omitted → defaults to Anywhere). No JSON schema exists for QED recipes (.yah/schema/ has no qed.toml.schema.json), so the 'update JSON schema' next-step was a no-op.")
//! @yah:next("R435-F2 can start: runner gates kicks on placement (CLI refuses ci-only without --force; GHA warns/refuses local-only). Placement is now readable via `pipeline.placement` after `PipelineLoader::load(name)`.")
//! @yah:cleanup("Surface `placement` in `yah qed list`/`tail` headers (deferred from F1's next-steps — purely cosmetic, easier to ship alongside F2 when the field becomes operationally relevant).")
//!
//! @yah:ticket(R476-T1, "Add outcomes + step names to qed.pipelines wire shape; drop static BUILTIN_DEFS reliance for outcome rendering")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-07T08:24:17Z)
//! @yah:status(review)
//! @yah:parent(R476)
//! @yah:next("Wire shape: extend WireQedPipeline (env/types.ts) with outcomes + step names; emit from QedRpc::pipelines (crates/yah/qed/src/lib.rs); drop the BUILTIN_DEFS wire-merge fallback path in QedPanel.tsx so user pipelines source outcomes from the wire instead of a static encoding")
//! @yah:verify("Run a user pipeline (e.g. desktop-local) with outcomes declared in its TOML; switch to Graph tab during the run; mermaid renders terminal Outcome nodes for that user pipeline (not just built-ins)")
//! @arch:see(.yah/docs/working/W191-qed-pipeline-ux-tweaks.md)
//! @yah:handoff("Shipped across 4 files. (1) crates/yah/rpc/src/lib.rs: added QedOutcomeWire enum (warden-deploy/publish/almanac-run), QedArtifactStepWire struct, and three new fields on QedPipelineWire — step_names: Vec<String>, outcomes: Vec<QedOutcomeWire>, artifact_steps: Vec<QedArtifactStepWire> — all #[serde(default)]. (2) app/yah/cli/src/camp.rs: qed_pipelines_handler now populates step_names from pipeline.steps[].name, outcomes by matching qed::Outcome variants to QedOutcomeWire, and artifact_steps from steps[].produces with triple-aware display labels. (3) packages/yah/ui/src/env/types.ts: WireQedOutcome discriminated union + extended WireQedPipeline with step_names?, outcomes?, artifact_steps?. (4) packages/yah/ui/src/components/run/QedPanel.tsx: defs useMemo now builds wireSteps/wireOutcomes/wireArtifactSteps from the wire; user pipelines get full outcomes+steps in their PipelineDef; built-ins refresh all wire-authoritative fields with BUILTIN_DEFS as offline fallback. cargo check -p rpc -p yah -p desktop clean; bun run typecheck clean for touched files; bun test qedMermaid.test.ts 5/5.")
//! @yah:verify("Run a user pipeline (e.g. desktop-local with on_success declared) — Graph tab renders terminal Outcome nodes matching the TOML declaration (not just built-ins)")
//! @yah:verify("Built-in release-build Graph tab still renders 6 steps + WardenDeploy + Publish terminals (daemon wire takes precedence over BUILTIN_DEFS; BUILTIN_DEFS serves as fallback when daemon is down)")
//!
//! @yah:relay(R488, "QED pipeline composition: StepKind::SubPipeline primitive (W201)")
//! @yah:at(2026-06-08T02:52:03Z)
//! @yah:status(open)
//! @yah:parent(Q486)
//! @yah:next("F1-F5 ship value independently of W200; F6 is the join point that wires GhaWorkflow children into compositions")
//! @yah:next("Marketing-site unblock path: ship F1+F2+F3 (composition + recursion + aggregation) so a full-release parent can wrap desktop-release (R330-F9) once R330-T6 ships the receiver")
//! @yah:gotcha("v1 caps nesting depth at 4 with explicit cycle detection — accidental recursion in user TOML is the failure mode")
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//!
//! @yah:ticket(R487-F9, "StepKind::GhaWorkflow + QED runner dispatch (yah qed run release wraps release.yml end-to-end)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:53:47Z)
//! @yah:status(review)
//! @yah:phase(P9)
//! @yah:parent(R487)
//! @yah:next("Add StepKind::GhaWorkflow { path, event, inputs } to crates/yah/qed/src/types.rs")
//! @yah:next("runner.rs: dispatch GhaWorkflow steps to qed_gha::execute, collect GhaRunResult { status, produced, job_outputs }")
//! @yah:next("ProducedArtifact aggregation flows into the outer pipeline's Outcome::Publish exactly like any other producing step")
//! @yah:next("config.rs: TOML parse for the new step kind")
//! @yah:verify("yah qed run release (single-step pipeline wrapping release.yml) executes locally and stages to cdn.yah.dev")
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @yah:depends_on(R487-F8)
//! @yah:tier(Warrior)
//! @yah:handoff("F9 landed: StepKind::GhaWorkflow first-class step kind + qed-runner dispatch + ProducedArtifact bridge. qed --lib: 200 pass (4 new) + 1 pre-existing failure (test_builtin_release_build_pipeline 4-vs-6, documented across R407-T1/R380-T3/R438-T14/R488-F1 handoffs — not introduced by F9). qed-gha: 88/88. cargo check -p yah clean. — types.rs: added StepKind::GhaWorkflow + GhaWorkflowConfig { path, event, inputs } + QedStep.gha_workflow: Option<GhaWorkflowConfig> (#[serde(default)] so existing TOML + literal sites unaffected; sed-inserted None on every QedStep init across builtins/runner/types/cli camp). Two new StepValidationError variants: GhaWorkflowHasArgv + GhaWorkflowMissingConfig (mirrors SubPipeline’s argv/config invariants). — runner.rs: new arm StepKind::GhaWorkflow → execute_step_gha_workflow(); reads workflow YAML at cfg.path (resolved against camp root), parses via qed_gha::parse_workflow, builds qed_gha::Executor with F5–F8 builtins pre-registered, lays inputs + a minimal github context (event_name only, ref/sha/actor empty) onto the executor, calls qed_gha::execute_workflow on a tokio spawn_blocking so docker buildx / git clone / etc. don’t stall the reactor. Each qed_gha::ProducedArtifact { binary, path, triple } lifts to qed::types::ProducedArtifact 1:1 (structurally compatible by F7 design); aggregation goes into the per-pipeline `produced` Vec exactly like a Subprocess `produces` declaration so Outcome::Publish stages them. First-failing-job is surfaced as a clean StepFailed with `gha-workflow <path> failed at job <id>`. — config.rs: LoaderSubPipelineResolver::resolve(SubPipelineRef::GhaWorkflow{path,event,inputs}) now synthesizes a one-step Pipeline carrying a single GhaWorkflow step instead of returning None. Going through SubPipeline preserves propagate.produces / suppress_publish_outcomes plumbing so a child workflow's R2 staging fires from the parent’s terminal publish, not the child’s. — lib.rs: re-exported GhaWorkflowConfig. — qed/Cargo.toml: qed-gha + indexmap path deps. — Tests: validate happy + 2 reject paths in types::tests, resolver synthesis test in config::tests; runner-level end-to-end is left to the integration verify (yah qed run release against a real .github/workflows/release.yml on a host with docker/git/rustup) since hermetic exec would require a stub workflow + an executor injection seam neither crate currently has.")
//! @yah:next("User: verify F9 — (a) confirm the SubPipeline-synthesis route is the right shape vs a parallel resolver type (preserves propagate.produces + suppress_publish_outcomes for free; alternative was a bypass route that wouldn’t), (b) accept the minimal github-context synthesis (event_name + empty ref/sha/actor — release.yml reads github.ref_name + github.event.inputs.* and the latter comes from the inputs map, but a workflow that touches github.sha will see an empty string), and (c) run the integration verify when next on a host with docker/git/rustup/bun: `yah qed run release` against a release-build pipeline that wraps .github/workflows/release.yml via SubPipelineRef::GhaWorkflow and stages to cdn.yah.dev. After sign-off: archive R487 + R487-S10 (still in review) + R487-F4/F5/F6/F7/F8/F9, then archive R487 itself; R487-T11 (retire .yah/qed/build-yah-warden.toml) is the post-F9 cleanup ticket that closes the relay.")
//!
//! @yah:ticket(R488-F1, "SubPipeline types + TOML parser + cycle detection (depth-4 cap)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:53:55Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R488)
//! @yah:next("Add StepKind::SubPipeline { target, params, propagate }, SubPipelineRef (Builtin | Path | GhaWorkflow), SubPipelineCollect { produces, outputs }")
//! @yah:next("config.rs: parse target.builtin / target.path / target.gha-workflow shapes")
//! @yah:next("Cycle detection by walking the resolution chain (open file path/builtin name set); reject at parse time")
//! @yah:next("Depth cap at 4; clear error with the chain on overflow")
//! @yah:verify("Round-trip TOML for all three SubPipelineRef shapes; cycle/depth rejections covered by tests")
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @yah:tier(Cleric)
//! @yah:handoff("F1 shipped. Added StepKind::SubPipeline (unit variant; existing Copy preserved) + SubPipelineConfig/Ref/Collect/Error types + validate_sub_pipeline_graph walker + SubPipelineResolver trait on crates/yah/qed/src/types.rs. QedStep grew sub_pipeline: Option<SubPipelineConfig> field (#[serde(default)] so existing TOML + 26 literal sites unaffected; sed-inserted None on every literal across builtins/runner/tests). Three new StepValidationError variants: SubPipelineHasArgv, SubPipelineMissingConfig, SubPipelineHasProduces. Runner gained a SubPipeline arm that returns StepFailed pointing at R488-F2 (execution lives there). MAX_SUB_PIPELINE_DEPTH = 4. Walker is parser-agnostic: takes a SubPipelineResolver, returns SubPipelineError::{Cycle,MaxDepthExceeded} with chain. Tests: 10 new in types::tests covering happy path validate, all three rejection arms, TOML round-trip for all three SubPipelineRef shapes (builtin/path/gha-workflow), acyclic walk, direct + indirect cycles, depth-limit, unresolved-ref tolerance. cargo test -p qed --lib: 181 pass + 1 pre-existing unrelated failure (test_builtin_release_build_pipeline 4-vs-6 step count flagged across R407-T1/R380-T3/R438-T14 handoffs).")
//! @yah:next("F2 wires the runner side: replace the SubPipeline arm's StepFailed stub in runner.rs:489 with real recursion. Resolver wants .yah/qed/PipelineLoader (builtin + path) + GhaWorkflow returns None until W200-F9. Track nested QedRun with parent_run_id; forward params via Pipeline::apply_params; suppress child's on_success outcomes when propagate.produces=true (the suppression lives in run() before Outcome dispatch — child gets a runner constructed via with_publish_suppressed or equivalent setter). Cycle/depth check should fire ONCE at the outermost run() entry against the loader-backed resolver, not per-step.")
//! @yah:next("F2 should also call validate_sub_pipeline_graph at the loader entry (PipelineLoader::validate_steps) once the loader-backed SubPipelineResolver impl exists — graceful parse-time cycle detection rather than runtime-only.")
//! @yah:verify("cargo test -p qed --lib types::tests::sub_pipeline (4 tests)")
//! @yah:verify("cargo test -p qed --lib types::tests::graph_walk (5 tests)")
//! @yah:verify("cargo test -p qed --lib types::tests::sub_pipeline_round_trips_through_toml_with_all_three_ref_shapes")
//!
//! @yah:ticket(R488-F4, "Named output exposure: QED native steps grow output declarations, propagate.outputs surfaces them")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:54:25Z)
//! @yah:status(review)
//! @yah:phase(P4)
//! @yah:parent(R488)
//! @yah:next("Add outputs: Vec<OutputDecl> to QedStep so native steps can name outputs the way GHA steps do")
//! @yah:next("Child run's named outputs surface on the parent step as steps.<id>.outputs.<name>")
//! @yah:next("Reuse W200's expression engine for parent-side substitution if W200-F2 has shipped; else stash for later wiring")
//! @yah:verify("2-child composite where child 1 emits output X and child 2 step references ${{ steps.child1.outputs.X }}")
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @yah:depends_on(R488-F3)
//! @yah:tier(Cleric)
//! @yah:handoff("F4 shipped. (1) types.rs: Added OutputDecl{name, description} struct; added outputs: Vec<OutputDecl> to QedStep (#[serde(default)] so all 28 existing literal sites + TOML unaffected); added outputs: HashMap<String,String> to StepStatus (#[serde(default)]). (2) runner.rs: Added substitute_step_context() fn (replaces ${{ steps.X.outputs.Y }} patterns, minimal — W200 expression engine subsumes later); added parse_yah_outputs() fn (reads KEY=VALUE file lines); modified execute_step_local to accept extra_env: Option<&HashMap> for $YAH_OUTPUTS injection without mutating the step; modified execute_step_sub_pipeline to return (Vec<ProducedArtifact>, HashMap<String,String>) — propagated_outputs scanned from child StepStatus::outputs filtered by propagate.outputs (last-writer-wins); modified run_inner to track step_context, apply substitution before each step, inject $YAH_OUTPUTS for Native subprocess steps + read back after exit, collect SubPipeline propagated outputs, store outputs in StepStatus. (3) lib.rs: re-exported OutputDecl. (4) 28 QedStep literal sites + 2 StepStatus sites updated across builtins.rs/runner.rs/types.rs/camp.rs. (5) 3 new tests: step_outputs_captured_in_step_status, step_outputs_substituted_into_sibling_argv (verify test: step2 receives ${{ steps.step1.outputs.X }} substituted), sub_pipeline_propagates_named_outputs_to_parent_context. cargo test -p qed --lib --test-threads=1: 194 pass + 1 pre-existing failure (test_builtin_release_build_pipeline 4-vs-6 steps). cargo check -p qed -p yah -p desktop: clean. Container/remote steps do not collect outputs (YAH_OUTPUTS not injected there — documented limitation).")
//! @yah:verify("cargo test -p qed --lib -- --test-threads=1")
//! @yah:verify("cargo check -p qed -p yah -p desktop")
//!
//! @yah:relay(R494, "QED cross-camp peer composition (W201 append)")
//! @yah:at(2026-06-08T23:47:59Z)
//! @yah:status(open)
//! @yah:parent(Q486)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//!
//! @yah:ticket(R494-F1, "SubPipelineRef::Peer variant + .yah/qed/peers.toml registry parser")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T23:48:05Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R494)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @yah:tier(Cleric)
//! @yah:handoff("F1 shipped. (1) types.rs: added SubPipelineRef::Peer { camp: String, pipeline: String } as a struct variant; serde rename_all=kebab-case gives TOML form `target = { peer = { camp = \"mesofact\", pipeline = \"release-build\" } }`. Extended sub_pipeline_ref_token + the test MapResolver match with the new arm — chain token is `peer:<camp>:<pipeline>`. (2) peers.rs (new module): PeerConfig { peer: HashMap<String, PeerEntry> } + PeerEntry { path: PathBuf, rig: Option<String> } + PeerConfigError. Modeled exactly on registries.rs — load `<qed_dir>/peers.toml` opportunistically, missing file → empty config, malformed → Parse error with path context. v1 rig field is parsed but ignored at resolution time (R494-T5 wires the unsupported-error stub; R494-F2 wires local resolution). (3) lib.rs: pub mod peers + re-exports PeerConfig/PeerConfigError/PeerEntry. (4) config.rs LoaderSubPipelineResolver: SubPipelineRef::Peer arm returns None — keeps cycle/depth detection working (walker stops descending) without compiling in any filesystem assumption about peer-camp layout. F2 replaces this with a peers.toml-backed lookup that loads the peer camp's PipelineLoader. (5) runner.rs sub_pipeline_target_label + the runner's test MapResolver: Peer arm added. (6) Tests: 4 new in peers::tests (missing file, local+remote parse, malformed, missing-path); types::tests::sub_pipeline_round_trips_through_toml_with_all_three_ref_shapes extended with the Peer shape (the name is now stale — 4 shapes — leaving the symbol untouched to avoid breaking the R488-F1 @yah:verify referencing it); new types::tests::graph_walk_detects_peer_cycle covering self-cycle via Peer ref. cargo test -p qed --lib: 206 pass + 1 pre-existing failure (test_builtin_release_build_pipeline 4-vs-6, documented across R407-T1/R380-T3/R438-T14/R488-F1 handoffs). cargo check -p qed -p yah -p desktop clean.")
//! @yah:next("F2 (R494-F2) wires the runner: PeerSubPipelineResolver wraps a PeerConfig + parent loader, loads the peer camp's `.yah/qed/` PipelineLoader on demand, returns its loaded Pipeline. validate_sub_pipeline_graph at the outermost run() entry needs the peer-aware resolver so a peer cycle (cheers -> mesofact -> cheers) is caught at parse-time. Per-peer-camp run serialization: use a per-camp lock keyed by peers.toml entry id so two concurrent yah runs invoking `peer:cheers` don't race on cheers' target/.")
//! @yah:next("F2 should call peer's PipelineLoader::load_and_validate_graph rather than load() so the child's own SubPipeline graph is walked too (catch a peer pipeline that itself references back into our camp via path).")
//! @yah:next("T5 (R494-T5) reserves the rig field stub: LoaderSubPipelineResolver/PeerSubPipelineResolver's Peer arm checks `entry.rig.is_some()` and returns a typed error like `RemotePeerNotYetSupported { camp, rig }` rather than the current None. Surface clearly in CLI so operators don't get a silent skip.")
//! @yah:verify("cargo test -p qed --lib peers::")
//! @yah:verify("cargo test -p qed --lib types::tests::sub_pipeline_round_trips_through_toml_with_all_three_ref_shapes")
//! @yah:verify("cargo test -p qed --lib types::tests::graph_walk_detects_peer_cycle")
//! @yah:verify("cargo check -p qed -p yah -p desktop")
//!
//! @yah:ticket(R494-T5, "Reserve peers.toml rig= field; stub remote-peer hop with explicit unsupported error")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T23:48:30Z)
//! @yah:status(review)
//! @yah:phase(P3)
//! @yah:parent(R494)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @yah:depends_on(R494-F1)
//! @yah:handoff("T5 shipped. Surfaces a typed reason on the R494-F1 remote-peer + unknown-peer paths so operators see an actionable message in StepFailed.msg instead of the generic 'target unresolvable' tail. (1) types.rs: SubPipelineResolver trait gained an optional `unresolved_reason(&SubPipelineRef) -> Option<String>` companion to `resolve` with a `None` default — preserves backward compat for the 3 existing impls (NoopSubPipelineResolver in runner.rs, MapResolver in types::tests + runner::tests). (2) config.rs LoaderSubPipelineResolver: impls unresolved_reason for Peer targets only; three branches — unknown camp routes to peers.toml with a copy-pasteable `[peer.<camp>]` skeleton; remote peer (entry.rig.is_some()) cites the camp + rig + R494-T5 and tells the operator to drop the `rig = ...` field or wait for R494-F10; known camp + missing pipeline names the resolved peer-camp path. Builtin/Path/GhaWorkflow return None (those misses already have their own surfaces). (3) runner.rs execute_step_sub_pipeline: when resolve returns None, query unresolved_reason and put it in StepFailed.msg verbatim; falls back to the previous debug-formatted message when the resolver doesn't diagnose. (4) Tests: 4 new in config::tests (typed remote-peer reason + camp/rig/ticket-id assertions; unknown-camp routes to peers.toml; missing-pipeline names the pipeline + camp; non-peer targets keep None). 1 new in runner::tests (DiagnosticResolver fixture + assertion that StepFailed.msg matches the resolver's typed message verbatim). The pre-existing `peer_resolver_swallows_remote_peers_until_t5_wires_constable` test was renamed to `peer_resolver_remote_peer_surfaces_typed_unsupported_reason` and extended. cargo test -p qed --lib: 218 pass + 1 pre-existing failure (test_builtin_release_build_pipeline 4-vs-6, documented across R488/R494 handoffs). cargo check -p qed -p yah -p desktop clean.")
//! @yah:verify("cargo test -p qed --lib -- peer_resolver_remote_peer_surfaces peer_resolver_unknown_camp peer_resolver_unknown_pipeline peer_resolver_unresolved_reason_is_none sub_pipeline_unresolved_surfaces (5/5 pass)")
//! @yah:verify("cargo check -p qed -p yah -p desktop")

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use task::TaskRuntime;

pub type QedRunId = String;
pub type ForgeId = String;

/// What can cause a pipeline to start.
///
/// Triggers are *declared* in the pipeline TOML but *dispatched* by the appropriate
/// scheduler — qed has no polling daemon. Tag triggers are fired by the GHA shim (or a
/// warden git-mirror hook); schedule triggers are fired by almanac; manual is the default.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Trigger {
    /// `yah qed run <pipeline>` from CLI or desktop — always available.
    Manual,
    /// Git tag push matching a glob (e.g. `v*.*.*`), fired by the GHA shim or warden hook.
    Tag { pattern: String },
    /// Cron expression, dispatched by almanac via `["yah", "qed", "run", pipeline]` TaskSpec.
    Schedule { cron: String },
    /// Another pipeline completed with the given status, chained by qed outcomes.
    Pipeline { id: String, status: RunStatus },
}

/// Where a recipe is allowed to run (W155 principle 2). The runner consults
/// this at kick time to refuse out-of-place runs before any step executes —
/// e.g. CLI refuses `CiOnly` from a developer laptop unless `--force`. The
/// recipe itself never branches on the runner; placement is the contract that
/// keeps recipes environment-agnostic.
///
/// Default is [`Placement::Anywhere`] so existing recipes keep working when
/// the field is omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Placement {
    /// Runs on a dev machine; meaningless on CI. The output is "yah.app
    /// installed in /Applications", "files written to the camp tree", etc.
    LocalOnly,
    /// Needs secrets, signing identity, or a clean runner that don't exist
    /// locally. Publishing, codesigning, notarization.
    CiOnly,
    /// Pure verification — lint, typecheck, smoke. The gold standard.
    #[default]
    Anywhere,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub name: String,
    pub label: String,
    pub steps: Vec<QedStep>,
    #[serde(default)]
    pub params: HashMap<String, ParamDef>,
    #[serde(default)]
    pub on_success: Vec<Outcome>,
    #[serde(default)]
    pub on_fail: Vec<Outcome>,
    /// Triggers that can start this pipeline. Defaults to `[Manual]` when omitted.
    #[serde(default)]
    pub triggers: Vec<Trigger>,
    /// Lock key that serializes concurrent runs. When two runs share a key,
    /// the second one is `Queued` until the first finishes. `None` defaults
    /// to the pipeline's own name (= one-at-a-time per pipeline). Two
    /// pipelines that fight for the same resource (e.g. `cargo`'s shared
    /// `target/`) can pin to the same key to serialize across pipelines.
    /// The sentinel `"@parallel"` opts out — runs of that pipeline never
    /// block each other (use for read-only fan-outs).
    #[serde(default)]
    pub concurrency_key: Option<String>,
    /// Where this recipe is allowed to run (W170). Defaults to
    /// [`Placement::Anywhere`]. The runner enforces this at kick time
    /// (R435-F2) — the recipe body itself remains environment-agnostic.
    #[serde(default)]
    pub placement: Placement,
    /// Optional GHA-workflow this pipeline wraps. Set to `"gha:<rel-path>"`
    /// in TOML (e.g. `wraps = "gha:.github/workflows/release.yml"`) when the
    /// pipeline exists *because* it composes a workflow; the daemon then
    /// suppresses that workflow's auto-ingest so the catalog doesn't show
    /// both entries. Purely advisory — not interpreted by the runner.
    #[serde(default)]
    pub wraps: Option<String>,
}

impl Pipeline {
    /// The effective concurrency key for this pipeline — `concurrency_key`
    /// if set, otherwise the pipeline name. The daemon's per-key mutex map
    /// is keyed off this value.
    pub fn effective_concurrency_key(&self) -> &str {
        self.concurrency_key.as_deref().unwrap_or(&self.name)
    }

    /// `true` when the pipeline opts out of serialization via the sentinel
    /// key `"@parallel"`.
    pub fn is_parallel(&self) -> bool {
        self.effective_concurrency_key() == "@parallel"
    }
}

impl Pipeline {
    /// Substitute `{{key}}` placeholders in every step's `argv` and `env`
    /// values with the supplied params (e.g. `provider=groq` turns
    /// `"{{provider}}"` into `"groq"`). Unknown placeholders are left
    /// untouched. Required-param *validation* is the caller's job — this
    /// only performs the textual substitution.
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
pub struct QedStep {
    pub name: String,
    #[serde(default)]
    pub argv: Vec<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
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
    /// to gate (e.g. `warden`, `yah`). The runner walks its transitive dep
    /// closure and fails if any crate in
    /// [`crate::preflight::KNOWN_GLIBC_ONLY_CRATES`] appears.
    #[serde(default)]
    pub package: Option<String>,
    /// For `kind = build-image`: docker build context directory, resolved
    /// relative to the camp root. Defaults to `.` (camp root itself) when
    /// absent — the same behaviour as before this field existed. Use this
    /// to point at a staging directory assembled by an earlier subprocess
    /// step (e.g. `context = "target/yah-warden-ctx"`).
    #[serde(default)]
    pub context: Option<std::path::PathBuf>,
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
    /// For `kind = gha-workflow` (W200-F9): path to a
    /// `.github/workflows/*.yml`, with optional event + dispatch inputs.
    /// Resolved relative to the camp root. Required when `kind = gha-workflow`;
    /// `validate()` rejects misconfiguration at parse time the same way
    /// `sub_pipeline` does.
    #[serde(default)]
    pub gha_workflow: Option<GhaWorkflowConfig>,
}

/// What a pipeline step does.
///
/// On the TOML side this is `kind = "subprocess" | "build-image"`. The default
/// — and the value omitted from every existing pipeline file — is
/// [`StepKind::Subprocess`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StepKind {
    /// Run `argv` (the existing semantics).
    #[default]
    Subprocess,
    /// Build a container image from the catalog (R381).  The image is looked
    /// up by `image` (catalog name); the runner materialises a
    /// `task::ForgeCommand::BuildImage` from the catalog entry.
    BuildImage,
    /// Package a static musl Rust binary + workload-spec manifest into a
    /// `.tar.gz` for the native runtime under Constable (R407-T2, W154).
    /// Catalog entry referenced by `image` must declare
    /// [`ProduceTarget::NativeTarball`](crate::images::ProduceTarget::NativeTarball);
    /// `binary_path` points at the cross-compiled binary an earlier step
    /// produced. No systemd unit is emitted — Constable directly
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
    /// dispatches to `qed_gha::execute_workflow`, then lifts each
    /// `qed_gha::ProducedArtifact` into [`ProducedArtifact`] and aggregates
    /// into the parent's `Outcome::Publish` — same surface as a producing
    /// `Subprocess` step or a `SubPipeline` child with `propagate.produces`.
    GhaWorkflow,
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
    /// brokered to a remote rig via constable when the peer registry entry
    /// has a `rig` field). `camp` is the registry key in
    /// `<qed_dir>/peers.toml`; `pipeline` is the named pipeline within that
    /// camp's own `.yah/qed/`. Runner-side resolution lives in R494-F2.
    Peer {
        camp: String,
        pipeline: String,
    },
}

/// What the parent rolls up from a SubPipeline child run.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
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
pub struct OutputDecl {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

/// Errors surfaced when walking a sub-pipeline graph at parse time
/// ([`validate_sub_pipeline_graph`]).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SubPipelineError {
    #[error("sub-pipeline cycle detected: {chain}")]
    Cycle { chain: String },
    #[error("sub-pipeline nesting exceeded max depth of {max}: {chain}")]
    MaxDepthExceeded { max: usize, chain: String },
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
    visit_sub_pipeline(pipeline, resolver, &mut chain)
}

fn visit_sub_pipeline(
    pipeline: &Pipeline,
    resolver: &dyn SubPipelineResolver,
    chain: &mut Vec<String>,
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
            return Err(SubPipelineError::Cycle { chain: full.join(" -> ") });
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
            visit_sub_pipeline(&child, resolver, chain)?;
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
         (e.g. `package = \"warden\"`)"
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
    #[error(
        "step `{0}`: sub-pipeline steps require a `[sub_pipeline]` block with `target = ...`"
    )]
    SubPipelineMissingConfig(String),
    #[error(
        "step `{0}`: sub-pipeline steps must not declare `produces` directly — \
         ProducedArtifacts come from the child run; set \
         `sub_pipeline.propagate.produces = true` to aggregate them"
    )]
    SubPipelineHasProduces(String),
    #[error("step `{0}`: gha-workflow steps must omit `argv`")]
    GhaWorkflowHasArgv(String),
    #[error(
        "step `{0}`: gha-workflow steps require a `[gha_workflow]` block with `path = ...`"
    )]
    GhaWorkflowMissingConfig(String),
}

impl QedStep {
    /// Validate kind-specific invariants. Called by the TOML loader
    /// (`PipelineLoader::load_from_file`) before the pipeline reaches the
    /// runner — fail loudly at parse time, not at execution time.
    pub fn validate(&self) -> Result<(), StepValidationError> {
        match self.kind {
            StepKind::Subprocess => {
                if self.argv.is_empty() {
                    return Err(StepValidationError::SubprocessMissingArgv(self.name.clone()));
                }
                Ok(())
            }
            StepKind::BuildImage => {
                if !self.argv.is_empty() {
                    return Err(StepValidationError::BuildImageHasArgv(self.name.clone()));
                }
                if self.image.is_none() {
                    return Err(StepValidationError::BuildImageMissingImage(self.name.clone()));
                }
                if matches!(self.runtime, Some(TaskRuntime::Native)) {
                    return Err(StepValidationError::BuildImageNativeRuntime(self.name.clone()));
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
                    return Err(StepValidationError::SubPipelineHasProduces(self.name.clone()));
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
        }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamDef {
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Outcome {
    WardenDeploy {
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
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QedRunMeta {
    pub id: QedRunId,
    pub pipeline: String,
    pub status: RunStatus,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub steps: Vec<StepStatus>,
    /// Set on child runs spawned by a [`StepKind::SubPipeline`] step (W201-F5).
    /// Carries the immediate parent's [`QedRunId`] so a consumer can walk
    /// from a child up to its parent (and recursively to the root). `None`
    /// on a top-level run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<QedRunId>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    /// Registered but waiting on its `concurrency_key` lock. Emitted as
    /// the very first status after `qed_run_handler` registers the run;
    /// transitions to `Running` when the key's mutex is acquired.
    Queued,
    Running,
    Success,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepStatus {
    pub name: String,
    pub task_run_id: Option<ForgeId>,
    pub status: RunStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    /// Key-value outputs collected from `$YAH_OUTPUTS` after the step ran
    /// (W201-F4). Empty when the step did not write any outputs, when the
    /// step kind doesn't support output collection (container, remote,
    /// sub-pipeline), or when the step failed before writing anything.
    #[serde(default)]
    pub outputs: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_step(argv: Vec<&str>, env: &[(&str, &str)]) -> Pipeline {
        Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![QedStep {
                name: "s".into(),
                argv: argv.into_iter().map(String::from).collect(),
                cwd: None,
                env: env.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: StepKind::Subprocess,
                image: None,
                tag: None,
                push: false,
                binary_path: None,
                triple: None,
                package: None,
                context: None,
                load: false,
                sub_pipeline: None,
            gha_workflow: None,
                outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: Placement::default(),
            wraps: None,
        }
    }

    #[test]
    fn apply_params_substitutes_argv_and_env() {
        let mut p = one_step(vec!["run", "--", "{{provider}}"], &[("KEY", "{{provider}}-x")]);
        let mut params = HashMap::new();
        params.insert("provider".to_string(), "groq".to_string());
        p.apply_params(&params);
        assert_eq!(p.steps[0].argv, vec!["run", "--", "groq"]);
        assert_eq!(p.steps[0].env.get("KEY").unwrap(), "groq-x");
    }

    fn build_image_step(name: &str) -> QedStep {
        QedStep {
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
            binary_path: None,
            triple: None,
            package: None,
            context: None,
            load: false,
            sub_pipeline: None,
            gha_workflow: None,
            outputs: Vec::new(),
        }
    }

    fn package_native_tarball_step(name: &str) -> QedStep {
        QedStep {
            name: name.into(),
            argv: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: StepKind::PackageNativeTarball,
            image: Some("yah-warden".into()),
            tag: None,
            push: false,
            binary_path: Some("target/x86_64-unknown-linux-musl/release/warden".into()),
            triple: Some("x86_64-unknown-linux-musl".into()),
            package: None,
            context: None,
            load: false,
            sub_pipeline: None,
            gha_workflow: None,
            outputs: Vec::new(),
        }
    }

    fn musl_static_preflight_step(name: &str) -> QedStep {
        QedStep {
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
            binary_path: None,
            triple: None,
            package: Some("warden".into()),
            context: None,
            load: false,
            sub_pipeline: None,
            gha_workflow: None,
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
            name: name.into(),
            argv: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: StepKind::SignNativeTarball,
            image: Some("yah-warden".into()),
            tag: None,
            push: false,
            binary_path: None,
            triple: Some("x86_64-unknown-linux-musl".into()),
            package: None,
            context: None,
            load: false,
            sub_pipeline: None,
            gha_workflow: None,
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

    // ── R435-F1 placement enum serde round-trip ────────────────────────────

    #[test]
    fn placement_round_trip_each_variant() {
        for (variant, kebab) in [
            (Placement::LocalOnly, "local-only"),
            (Placement::CiOnly, "ci-only"),
            (Placement::Anywhere, "anywhere"),
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, format!("\"{kebab}\""), "serialize {variant:?}");
            let parsed: Placement = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, variant, "deserialize {kebab}");
        }
    }

    #[test]
    fn placement_defaults_to_anywhere_when_omitted() {
        let toml_src = r#"
            name = "p"
            label = "p"
            steps = []
        "#;
        let pipeline: Pipeline = toml::from_str(toml_src).unwrap();
        assert_eq!(pipeline.placement, Placement::Anywhere);
    }

    #[test]
    fn placement_parses_each_kebab_value_from_toml() {
        for (kebab, expected) in [
            ("local-only", Placement::LocalOnly),
            ("ci-only", Placement::CiOnly),
            ("anywhere", Placement::Anywhere),
        ] {
            let toml_src = format!(
                r#"
                name = "p"
                label = "p"
                placement = "{kebab}"
                steps = []
                "#
            );
            let pipeline: Pipeline = toml::from_str(&toml_src).unwrap();
            assert_eq!(pipeline.placement, expected, "TOML placement = \"{kebab}\"");
        }
    }

    #[test]
    fn apply_params_leaves_unknown_placeholders_untouched() {
        let mut p = one_step(vec!["{{missing}}"], &[]);
        p.apply_params(&HashMap::new());
        assert_eq!(p.steps[0].argv, vec!["{{missing}}"], "empty params is a no-op");

        let mut params = HashMap::new();
        params.insert("other".to_string(), "v".to_string());
        p.apply_params(&params);
        assert_eq!(p.steps[0].argv, vec!["{{missing}}"], "unknown key left as-is");
    }

    // ----- SubPipeline (W201-F1) ----------------------------------------------

    fn sub_pipeline_step(name: &str, target: SubPipelineRef) -> QedStep {
        QedStep {
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
            binary_path: None,
            triple: None,
            package: None,
            context: None,
            load: false,
            sub_pipeline: Some(SubPipelineConfig {
                target,
                params: HashMap::new(),
                propagate: SubPipelineCollect::default(),
            }),
            outputs: Vec::new(),
            gha_workflow: None,
        }
    }

    fn pipeline_with(name: &str, steps: Vec<QedStep>) -> Pipeline {
        Pipeline {
            name: name.into(),
            label: name.into(),
            steps,
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: Placement::default(),
            wraps: None,
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
            Err(StepValidationError::SubPipelineMissingConfig("compose".into()))
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
            Err(StepValidationError::SubPipelineHasProduces("compose".into()))
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
            vec![sub_pipeline_step("descend", SubPipelineRef::Builtin("child-b".into()))],
        );
        let root = pipeline_with(
            "root",
            vec![sub_pipeline_step("descend", SubPipelineRef::Builtin("child-a".into()))],
        );
        let mut map = HashMap::new();
        map.insert("builtin:child-a".to_string(), mid);
        map.insert("builtin:child-b".to_string(), leaf);
        let resolver = MapResolver(map);
        assert!(validate_sub_pipeline_graph(&root, &resolver).is_ok());
    }

    #[test]
    fn graph_walk_detects_direct_self_cycle() {
        // root -> root (builtin name matches itself's name — irrelevant to the
        // walker, but a likely real-world mistake)
        let mut root = pipeline_with(
            "self",
            vec![sub_pipeline_step("loop", SubPipelineRef::Builtin("self".into()))],
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
                assert!(chain.contains("builtin:self"), "cycle chain reports the ref: {chain}");
            }
            other => panic!("expected Cycle, got {other:?}"),
        }
    }

    #[test]
    fn graph_walk_detects_indirect_cycle() {
        // root -> a -> b -> a
        let a_loops_back = pipeline_with(
            "a",
            vec![sub_pipeline_step("descend", SubPipelineRef::Builtin("b".into()))],
        );
        let b_back_to_a = pipeline_with(
            "b",
            vec![sub_pipeline_step("loop", SubPipelineRef::Builtin("a".into()))],
        );
        let root = pipeline_with(
            "root",
            vec![sub_pipeline_step("enter", SubPipelineRef::Builtin("a".into()))],
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
            vec![sub_pipeline_step("enter", SubPipelineRef::Builtin("d1".into()))],
        );
        let resolver = MapResolver(map);
        let err = validate_sub_pipeline_graph(&root, &resolver).unwrap_err();
        assert!(
            matches!(err, SubPipelineError::MaxDepthExceeded { max: MAX_SUB_PIPELINE_DEPTH, .. }),
            "expected MaxDepthExceeded, got {err:?}"
        );
    }

    #[test]
    fn graph_walk_tolerates_unresolved_refs() {
        // Resolver returns None — the walker should not error; runtime
        // surfaces the resolution failure later.
        let root = pipeline_with(
            "root",
            vec![sub_pipeline_step("enter", SubPipelineRef::Builtin("nonexistent".into()))],
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
