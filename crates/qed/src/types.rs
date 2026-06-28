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
//! @yah:handoff("Shipped across 4 files. (1) crates/yah/rpc/src/lib.rs: added QedOutcomeWire enum (yubaba-deploy/publish/almanac-run), QedArtifactStepWire struct, and three new fields on QedPipelineWire — step_names: Vec<String>, outcomes: Vec<QedOutcomeWire>, artifact_steps: Vec<QedArtifactStepWire> — all #[serde(default)]. (2) app/yah/cli/src/camp.rs: qed_pipelines_handler now populates step_names from pipeline.steps[].name, outcomes by matching qed::Outcome variants to QedOutcomeWire, and artifact_steps from steps[].produces with triple-aware display labels. (3) packages/yah/ui/src/env/types.ts: WireQedOutcome discriminated union + extended WireQedPipeline with step_names?, outcomes?, artifact_steps?. (4) packages/yah/ui/src/components/run/QedPanel.tsx: defs useMemo now builds wireSteps/wireOutcomes/wireArtifactSteps from the wire; user pipelines get full outcomes+steps in their PipelineDef; built-ins refresh all wire-authoritative fields with BUILTIN_DEFS as offline fallback. cargo check -p rpc -p yah -p desktop clean; bun run typecheck clean for touched files; bun test qedMermaid.test.ts 5/5.")
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
//! @yah:next("runner.rs: dispatch GhaWorkflow steps to yah_qed_gha::execute, collect GhaRunResult { status, produced, job_outputs }")
//! @yah:next("ProducedArtifact aggregation flows into the outer pipeline's Outcome::Publish exactly like any other producing step")
//! @yah:next("config.rs: TOML parse for the new step kind")
//! @yah:verify("yah qed run release (single-step pipeline wrapping release.yml) executes locally and stages to cdn.yah.dev")
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @yah:depends_on(R487-F8)
//! @yah:tier(Warrior)
//! @yah:handoff("F9 landed: StepKind::GhaWorkflow first-class step kind + qed-runner dispatch + ProducedArtifact bridge. qed --lib: 200 pass (4 new) + 1 pre-existing failure (test_builtin_release_build_pipeline 4-vs-6, documented across R407-T1/R380-T3/R438-T14/R488-F1 handoffs — not introduced by F9). qed-gha: 88/88. cargo check -p yah clean. — types.rs: added StepKind::GhaWorkflow + GhaWorkflowConfig { path, event, inputs } + QedStep.gha_workflow: Option<GhaWorkflowConfig> (#[serde(default)] so existing TOML + literal sites unaffected; sed-inserted None on every QedStep init across builtins/runner/types/cli camp). Two new StepValidationError variants: GhaWorkflowHasArgv + GhaWorkflowMissingConfig (mirrors SubPipeline’s argv/config invariants). — runner.rs: new arm StepKind::GhaWorkflow → execute_step_gha_workflow(); reads workflow YAML at cfg.path (resolved against camp root), parses via yah_qed_gha::parse_workflow, builds yah_qed_gha::Executor with F5–F8 builtins pre-registered, lays inputs + a minimal github context (event_name only, ref/sha/actor empty) onto the executor, calls yah_qed_gha::execute_workflow on a tokio spawn_blocking so docker buildx / git clone / etc. don’t stall the reactor. Each yah_qed_gha::ProducedArtifact { binary, path, triple } lifts to qed::types::ProducedArtifact 1:1 (structurally compatible by F7 design); aggregation goes into the per-pipeline `produced` Vec exactly like a Subprocess `produces` declaration so Outcome::Publish stages them. First-failing-job is surfaced as a clean StepFailed with `gha-workflow <path> failed at job <id>`. — config.rs: LoaderSubPipelineResolver::resolve(SubPipelineRef::GhaWorkflow{path,event,inputs}) now synthesizes a one-step Pipeline carrying a single GhaWorkflow step instead of returning None. Going through SubPipeline preserves propagate.produces / suppress_publish_outcomes plumbing so a child workflow's R2 staging fires from the parent’s terminal publish, not the child’s. — lib.rs: re-exported GhaWorkflowConfig. — qed/Cargo.toml: qed-gha + indexmap path deps. — Tests: validate happy + 2 reject paths in types::tests, resolver synthesis test in config::tests; runner-level end-to-end is left to the integration verify (yah qed run release against a real .github/workflows/release.yml on a host with docker/git/rustup) since hermetic exec would require a stub workflow + an executor injection seam neither crate currently has.")
//! @yah:next("User: verify F9 — (a) confirm the SubPipeline-synthesis route is the right shape vs a parallel resolver type (preserves propagate.produces + suppress_publish_outcomes for free; alternative was a bypass route that wouldn’t), (b) accept the minimal github-context synthesis (event_name + empty ref/sha/actor — release.yml reads github.ref_name + github.event.inputs.* and the latter comes from the inputs map, but a workflow that touches github.sha will see an empty string), and (c) run the integration verify when next on a host with docker/git/rustup/bun: `yah qed run release` against a release-build pipeline that wraps .github/workflows/release.yml via SubPipelineRef::GhaWorkflow and stages to cdn.yah.dev. After sign-off: archive R487 + R487-S10 (still in review) + R487-F4/F5/F6/F7/F8/F9, then archive R487 itself; R487-T11 (retire .yah/qed/build-yah-yubaba.toml) is the post-F9 cleanup ticket that closes the relay.")
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
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
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

/// How the runner positions the on-disk tree a pipeline's steps build against,
/// relative to the run's target branch (the `branch` run-param, default `main`).
///
/// A QED run's workspace is normally the live camp root — fine for verifying
/// whatever is on disk, wrong for cutting a release (which must never ship a
/// dev's uncommitted edits). This is the per-pipeline knob that picks the right
/// trade-off; the run carries the *which branch*, the pipeline carries the *how
/// strict*.
///
/// Default is [`WorkspaceMode::Checkout`] — switch to the requested branch but
/// refuse to run over uncommitted changes, so a stray run never silently builds
/// the wrong bytes and never clobbers local work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum WorkspaceMode {
    /// Build against the camp root's live working tree exactly as it is on disk
    /// — no branch switch, no dirty check. For local/dev pipelines that want
    /// "build what I'm looking at right now".
    Live,
    /// Switch the camp root to the run's target branch, but **bail if the tree
    /// is dirty** (any uncommitted change). The safe default: never builds
    /// surprise bytes, never discards local work.
    #[default]
    Checkout,
    /// Build in a dedicated git worktree checked out at the target branch; the
    /// camp root (and any uncommitted work in it) is untouched. The correct
    /// mode for releases — a tag is always cut from clean committed state.
    Isolated,
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
    /// How the runner positions the on-disk tree this pipeline builds against
    /// (W224). Defaults to [`WorkspaceMode::Checkout`] (switch to the run's
    /// branch, bail if dirty). Releases set `workspace = "isolated"` so a tag is
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
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
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
    /// `steps.<X>.outputs.<Y>`, plus `success()` / `failure()`). When the
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
    /// For `kind = wait-for` (R513-F3, W207 Gap #5): the network endpoint to
    /// poll and the timeout/interval budget. Required when `kind = wait-for`;
    /// `validate()` rejects misconfiguration (missing block, no target, both
    /// targets) at parse time the same way `sub_pipeline` / `gha_workflow` do.
    /// `None` for every other step kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_for: Option<WaitForConfig>,
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
/// single network endpoint to poll and the time budget for it to come up.
///
/// ```toml
/// [[steps]]
/// name = "wait:ready"
/// kind = "wait-for"
/// [steps.wait_for]
/// http = "http://localhost:3000/health"   # plaintext HTTP GET, healthy on 2xx/3xx
/// timeout_secs = 30                        # give up (and fail the step) after this
/// # interval_ms = 500                      # poll cadence (default 500ms)
/// # expect_status = 200                    # require an exact status instead of any 2xx/3xx
/// ```
///
/// Exactly one of [`Self::http`] / [`Self::tcp`] must be set. The `http` probe
/// is a dependency-free plaintext HTTP/1.1 GET (no TLS in v1 — an `https://`
/// URL is rejected at runtime; use a `tcp` gate or terminate TLS in front);
/// the `tcp` probe is a bare connect to `host:port`, healthy the moment the
/// port accepts. [`Self::expect_status`] is HTTP-only.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct WaitForConfig {
    /// Plaintext-HTTP URL to GET each poll (e.g. `http://localhost:3000/health`).
    /// Healthy on a 2xx/3xx response, or on an exact match to
    /// [`Self::expect_status`] when set. Mutually exclusive with [`Self::tcp`].
    #[serde(default)]
    pub http: Option<String>,
    /// `host:port` to connect to each poll (e.g. `127.0.0.1:5432`). Healthy the
    /// moment the connect succeeds — no bytes are exchanged. Mutually exclusive
    /// with [`Self::http`].
    #[serde(default)]
    pub tcp: Option<String>,
    /// Require this exact HTTP status to consider the endpoint healthy, instead
    /// of the default "any 2xx/3xx". HTTP-only — `validate()` rejects it
    /// alongside a `tcp` target. `None` ⇒ any 2xx/3xx.
    #[serde(default)]
    pub expect_status: Option<u16>,
    /// Total budget, in seconds, for the endpoint to become healthy. The step
    /// fails with a clear "never became healthy" message once this elapses.
    /// Defaults to 30s.
    #[serde(default = "default_wait_timeout_secs")]
    pub timeout_secs: u64,
    /// Delay between poll attempts, in milliseconds. Defaults to 500ms — snappy
    /// enough for a fast-booting dev server without hammering the socket.
    #[serde(default = "default_wait_interval_ms")]
    pub interval_ms: u64,
}

fn default_wait_timeout_secs() -> u64 {
    30
}

fn default_wait_interval_ms() -> u64 {
    500
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
    /// parent camp's — without this, `peer-release` runs yubaba's
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
    #[error("step `{0}`: wait-for steps must omit `argv` (a wait-for is a pure gate)")]
    WaitForHasArgv(String),
    #[error(
        "step `{0}`: wait-for steps require a `[wait_for]` block with `http = ...` or `tcp = ...`"
    )]
    WaitForMissingConfig(String),
    #[error(
        "step `{0}`: wait-for needs exactly one target — set `http = \"http://…\"` \
         OR `tcp = \"host:port\"`, not neither"
    )]
    WaitForNeedsTarget(String),
    #[error(
        "step `{0}`: wait-for accepts only one target — set `http` OR `tcp`, not both"
    )]
    WaitForAmbiguousTarget(String),
    #[error(
        "step `{0}`: `expect_status` only applies to an `http` wait-for — \
         a `tcp` gate is healthy on connect, with no status to match"
    )]
    WaitForStatusNeedsHttp(String),
    #[error("step `{0}`: wait-for `timeout_secs` must be greater than zero")]
    WaitForZeroTimeout(String),
    #[error(
        "finally step `{0}`: v1 `[[finally]]` teardown supports only `kind = subprocess` \
         (and never `background`) — composite / image / sidecar teardown is a follow-up"
    )]
    FinallyRequiresSubprocess(String),
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
                match (cfg.http.is_some(), cfg.tcp.is_some()) {
                    (false, false) => {
                        return Err(StepValidationError::WaitForNeedsTarget(self.name.clone()));
                    }
                    (true, true) => {
                        return Err(StepValidationError::WaitForAmbiguousTarget(
                            self.name.clone(),
                        ));
                    }
                    _ => {}
                }
                if cfg.expect_status.is_some() && cfg.tcp.is_some() {
                    return Err(StepValidationError::WaitForStatusNeedsHttp(self.name.clone()));
                }
                if cfg.timeout_secs == 0 {
                    return Err(StepValidationError::WaitForZeroTimeout(self.name.clone()));
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct ParamDef {
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum RunStatus {
    /// Registered but waiting on its `concurrency_key` lock. Emitted as
    /// the very first status after `qed_run_handler` registers the run;
    /// transitions to `Running` when the key's mutex is acquired.
    Queued,
    Running,
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
    /// Aggregate child run statuses into a single parent status (R506-F1
    /// matrix fan-out). Mirrors [`yah_qed_gha::JobResult::aggregate`] exactly so a
    /// matrixed parent run reports the same overall verdict the GHA graph would
    /// for the same set of rows: any `Failed` wins, then `Cancelled`, then
    /// `Success`, and only an all-`Skipped` (or empty) set reports `Skipped`.
    ///
    /// `Queued` / `Running` contribute nothing — `aggregate` is meant to be
    /// called once every child has reached a terminal state.
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
                RunStatus::Skipped | RunStatus::Queued | RunStatus::Running => {}
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
            name: "p".into(),
            label: "p".into(),
            steps: vec![QedStep {
                background: false,
                background_until: None,
                wait_for: None,
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
                binary_path: None,
                triple: None,
                package: None,
                context: None,
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
            placement: Placement::default(),
            workspace: crate::types::WorkspaceMode::default(),
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        }
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

    fn build_image_step(name: &str) -> QedStep {
        QedStep {
            background: false,
            background_until: None,
            wait_for: None,
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
            background: false,
            background_until: None,
            wait_for: None,
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
            binary_path: Some("target/x86_64-unknown-linux-musl/release/yubaba".into()),
            triple: Some("x86_64-unknown-linux-musl".into()),
            package: None,
            context: None,
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
            background: false,
            background_until: None,
            wait_for: None,
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
            package: Some("yubaba".into()),
            context: None,
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
            background: false,
            background_until: None,
            wait_for: None,
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
            binary_path: None,
            triple: Some("x86_64-unknown-linux-musl".into()),
            package: None,
            context: None,
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

    // ----- SubPipeline (W201-F1) ----------------------------------------------

    fn sub_pipeline_step(name: &str, target: SubPipelineRef) -> QedStep {
        QedStep {
            background: false,
            background_until: None,
            wait_for: None,
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
                opaque: false,
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
            name: name.into(),
            label: name.into(),
            steps,
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: Placement::default(),
            workspace: crate::types::WorkspaceMode::default(),
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
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
            tcp: None,
            expect_status: None,
            timeout_secs: 30,
            interval_ms: 500,
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
            WaitForConfig {
                http: None,
                tcp: None,
                expect_status: None,
                timeout_secs: 30,
                interval_ms: 500,
            },
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
                expect_status: None,
                timeout_secs: 30,
                interval_ms: 500,
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
                http: None,
                tcp: Some("localhost:5432".into()),
                expect_status: Some(200),
                timeout_secs: 30,
                interval_ms: 500,
            },
        );
        assert_eq!(
            step.validate(),
            Err(StepValidationError::WaitForStatusNeedsHttp("gate".into()))
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
                tcp: None,
                expect_status: Some(204),
                timeout_secs: 45,
                interval_ms: 250,
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

    #[test]
    fn graph_walk_detects_direct_self_cycle() {
        // root -> root (builtin name matches itself's name — irrelevant to the
        // walker, but a likely real-world mistake)
        let mut root = pipeline_with(
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
