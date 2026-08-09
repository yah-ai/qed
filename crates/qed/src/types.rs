//! @yah:relay(R623, "Migrate QED run history from .yah/jit/qed/*.json to turso")
//! @yah:assignee(bundle-anthropic-miravel)
//! @yah:kind(task)
//! @yah:at(2026-07-23T03:17:22Z)
//! @yah:gotcha("QED run history is the ODD ONE OUT: task-runs, task-sessions and gnome_queue are all turso-backed (.yah/db/*.turso), but qed runs persist as flat per-run files in .yah/jit/qed/ — <run_id>.json + <run_id>.events.jsonl, 342 of them as of 2026-07-21.")
//! @yah:next("Migrate persist_qed_run + load_qed_history (app/yah/cli/src/camp.rs) onto a turso store, following the task-runs store shape (oss/qed/crates/task-runs/src/store.rs).")
//! @yah:next("Needs a migration story for the 342 existing run files — import-on-boot or a one-shot, not a silent drop.")
//! @yah:next("NOT a blocker for R622 (manual steps). R622's parked-run durability is satisfied by a non-terminal JSON persist reusing the R603 pattern, which a turso migration would carry over anyway. Deliberately decoupled — see W282 'The turso question is real, but separate'.")
//!
//! @yah:relay(R622, "QED manual steps: pipelines with a human in the middle (release wizard)")
//! @yah:status(review)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:at(2026-08-05T02:39:18Z)
//! @arch:see(.yah/docs/working/W282-qed-manual-steps.md)
//! @yah:gotcha("A manual step is a COORDINATION point, not an authorization gate. It does not know who clicked Continue. Do not let it grow into a permissions system — that is an explicit non-goal in W282.")
//! @yah:handoff("DONE, all seven tasks. T1 StepKind::Manual + ManualConfig{prompt,terminal,advance,checklist,advance_poll_secs} on QedStep.manual + 6 StepValidationError variants + validate() arm mirroring WaitFor (types.rs). T2 RunStatus::AwaitingHuman, non-terminal, folded into aggregate's ignore-arm with decision-table cases (run_status_aggregate_ignores_awaiting_human) + a serde-spelling pin. T3 park/resume WITH lock release: new `ManualGate` trait in runner.rs (park/release_lock/reacquire_lock, the last two defaulting to no-ops) + `PipelineRunner::with_manual_gate`, inherited by sub-pipeline children. T4 the human surface IS the AnswerQueue — `QedFormManualGate` in camp.rs mints a W111 Form (by/job/session_id all None, which the schema documents as legal for scope-unaware callers) and awaits forms::submit::wait_for_resolution. T5 QedEvent::StepAwaitingHuman{index,name,form_id,advance} + QedEventWire kebab 'step-awaiting-human' + OnSubmit::ResumeQedRun{run_id,step_index} + a `qed.resume` RPC. T6 AnswerModal.tsx form.framing now renders through <Markdown campId={form.campId}> instead of a plain <p> — that one change is what buys the terminal prefill, since Markdown already mounts RunInTerminalButton + InlineTerminalPanel on any shell fence. T7 .yah/qed/release-wizard.toml composes version-bump + oss-publish as sub-pipelines with manual steps between.")
//! @yah:handoff("DESIGN CALLS I made where W282 left them open. (1) Park order is: probe `advance` FIRST and skip the park entirely if it already holds (that is 'auto-advances the moment it exits 0' at its first tick, and it stops the wizard interrupting you to confirm what it can already see); then release the key; then race the human's answer against a poll of `advance`; then RE-EVALUATE `advance` on their answer — a failure RE-PARKS carrying the failing command's stderr rather than failing the step. (2) Headless (no gate installed, i.e. `yah qed run`): `advance` is the only door. Satisfied ⇒ pass; unsatisfied or absent ⇒ fail with a message naming the condition and pointing at the daemon. Deliberately NOT auto-advance — silently passing a human gate because nobody was listening would make the kind a lie. (3) W282 OQ3 (remote runs): decided — validate() rejects `runtime = \"container\"` on a manual step and resolve_runtime forces Native, same shape as SignNativeTarball. (4) W282 OQ1 (timeout): none, as leaned. (5) A dropped gate sender (daemon restart, cancelled form) fails the step loudly rather than hanging.")
//! @yah:handoff("DISCOVERED WORK done in this pass, beyond the ticket. (a) runner.rs:20's stale gotcha CORRECTED in place (it claimed all qed runs are in-memory; R325-F3 landed long ago) — rewritten to state what is actually true, including that R622 adds the second non-terminal persist point after R603's. (b) camp.rs apply_qed_event_to_meta returned early unless status==Running, which would have dropped every event after a park; it now accepts AwaitingHuman and flips back to Running on the next event carrying the PARKED STEP'S OWN index — scoped that way because a `background` sidecar keeps emitting StepOutput throughout a park and would otherwise un-park the run. (c) The `.yah/schema/qed-pipeline.toml.schema.json` drift gate was RED in committed state (the R625-F3 note in xtask parked it as 'belongs to whoever changed the types'); regenerated via `cargo run -p xtask -- emit-schemas`, and `cargo test -p xtask --test schema_drift` is now 3/3 green. (d) `WorkspaceMode` was not re-exported from yah_qed's lib.rs, so no consumer could name the type of `Pipeline::workspace`; added. (e) New test `every_camp_pipeline_loads_and_validates` walks .yah/qed/ and asserts every file with a top-level [pipeline] table loads — nothing guarded that before, which is the class of rot that let oss-publish.toml drift.")
//! @yah:verify("cargo test -p yah-qed --lib (from oss/qed) — 713 passed / 0 failed, incl. 16 new manual-step tests (8 validation in types.rs, 8 runner park/resume in runner.rs against a scripted gate)")
//! @yah:verify("cargo test -p yah --lib — 906 passed / 0 failed, incl. every_camp_pipeline_loads_and_validates + release_wizard_composes_and_gates_on_advance")
//! @yah:verify("cargo check --workspace clean; cargo test -p xtask --test schema_drift 3/3; cargo test -p yah-forms --lib 147/147")
//! @yah:verify("packages/yah/ui: bun run typecheck clean; bun test src/components/forms src/components/terminal 219/220 — the one failure is pre-existing (classifyTool.test.ts:433 asserts a literal is >80 chars; it is 78) and predates this change")
//! @yah:gotcha("OPERATOR ACTION BEFORE THIS IS USABLE: the running camp daemon is the OLD binary and does not know kind = \"manual\", so it will report .yah/qed/release-wizard.toml as a load error in the QED tab until `cargo xtask install` + a daemon restart. Not done here — restarting the daemon interrupts every live session in the camp, which is the operator's call, not mine.")
//! @yah:gotcha("release-wizard.toml's `workspace = \"live\"` is a REAL COMPROMISE, documented at length in the file header — read it before running the wizard. A sub-pipeline child inherits the parent's positioned tree instead of positioning its own (runner.rs run_inner, W224 R533-F11), so the wizard's mode applies to BOTH children and their declared modes are ignored. version-bump REQUIRES `live` (it mutates the tree on purpose; in `isolated` the edits are torn down and it reports success having changed nothing), so `live` wins — which means the oss-publish leg runs against the live camp tree rather than the isolated worktree it declares. The commit-and-tag gate proves a tag exists at HEAD when it advances, but this is a SHARED tree and a peer can dirty it during the publish. For anything but a routine patch, run `release-patch` then `oss-publish` separately instead.")
//! @yah:next("NOT DONE, genuinely separable: a parked run does not survive a DAEMON RESTART. The form does (it is on disk in .yah/forms/ the moment it is minted) and the run's non-terminal <run_id>.json is persisted on StepAwaitingHuman, but nothing reconciles the two on boot — the run TASK that was awaiting wait_for_resolution is gone, so answering the form after a restart resolves the form and nothing else. OnSubmit::ResumeQedRun{run_id,step_index} is stamped on the form precisely so a boot reconcile can find its way back; wiring that is the R603-T2-shaped follow-up (reconcile_inflight_qed_runs already exists as the place it belongs).")
//! @yah:next("NOT DONE: the matrix-fanout (qed_run_matrix_fanout) and cloud-reconcile runner construction sites do not install a manual gate, so a manual step reached through either takes the headless advance-only path. Left deliberately — N matrix rows each parking on a human is a shape nobody has asked for, and guessing at it would be worse than the honest fallback. Wire it when a real case appears.")
//! @yah:next("NOT DONE (W282 OQ2): no desktop notification on park. The run is invisible until someone looks, which W282 rightly says defeats a wizard — but the form DOES land in the AnswerQueue, so the operator gets the normal queue badge. A party.notify on park is the small addition that would close it properly.")
//! @yah:cleanup("QedPanel.tsx has no styling for the new 'awaiting-human' run status — it falls to the `default:` arm and renders as pending, which is safe but reads wrong. QedRunTile.tsx got the one-line status-dot arm (pulsing --color-st-handoff, the board's 'waiting to be picked up' colour); QedPanel deserves the same treatment plus a way to jump from a parked run straight to its form.")
//!
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
//!
//! @yah:ticket(R703-F3, "Pipeline readme: parse a QED TOML's leading comment block into description, render it above inlined steps+graph, retire the sub-tab strip")
//! @yah:status(review)
//! @yah:at(2026-08-03T22:12:29Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R703)
//! @yah:next("Every pipeline in .yah/qed/ already carries a genuinely good README in its leading TOML comment block -- cli-release's 'what it does NOT do, stated rather than discovered later' section is exactly the content an operator wants when they click the row. Today only the one-line `label` reaches the wire; those headers are read by humans in an editor and thrown away by the daemon.")
//! @yah:next("Producer side: parse the contiguous leading `#` comment block of a pipeline TOML into the PipelineDef's `description` (the field already exists and is already carried to the UI). Strip the leading '# ' and preserve paragraph breaks; the blocks are already written as prose with markdown-ish section rules.")
//! @yah:next("Consumer side: QedPanel.tsx:2263 holds `const [detailView, setDetailView] = useState<\"steps\"|\"graph\">(\"graph\")`. Collapse it -- delete the state and the sub-tab strip, stack description -> readme -> steps -> graph inline. Operator's framing: 'have a readme section, then steps, then graph inlined with no tabs.'")
//! @yah:next("Long readmes want a collapse affordance rather than a tab -- default the readme open and let it fold, so the steps stay reachable without a click on a short pipeline.")
//! @yah:verify("Clicking a pipeline row shows its TOML header as prose, followed by steps, followed by the graph, with no tab chrome")
//! @yah:verify("A pipeline whose TOML has no leading comment block renders without an empty readme section")
//! @yah:verify("bun run typecheck clean")
//! @yah:gotcha("QedPanel.tsx is 4567 lines. The sub-tab strip is only rendered when def.steps.length > 1, so a naive removal changes behaviour for single-step pipelines too -- check that path.")
//! @yah:gotcha("Some headers are long (release-build's is ~50 lines of design rationale). Do not truncate silently; that content is the point of the ticket.")
//! @yah:gotcha("Not every .yah/qed/*.toml is a pipeline: peers.toml, registries.toml, baseline.toml, gha-actions.toml and transforms/ live in the same directory. The parser must not assume pipeline shape from location alone.")
//! @yah:handoff("PRODUCER. qed types.rs: Pipeline gains description: Option String. config.rs: new pub fn leading_comment_block lifts the contiguous leading hash-comment block into it. Rules: first non-blank non-comment line ends the block; a line-initial @yah:/@arch: ends it, but a prose line that merely mentions one mid-sentence does not (four headers in this camp do exactly that); a taplo #:schema directive is skipped rather than treated as a terminator (dashboard-e2e.toml opens with one); exactly one leading space is stripped so box-rules and indented sub-lists survive; a bare hash becomes a paragraph break; empty or annotations-only yields None. An explicit [pipeline] description key wins (added to PipelineConfig so qed eject round-trips).")
//! @yah:handoff("REFACTOR. Collapsed the two duplicate PipelineToml-to-Pipeline hoists (load_from_file and the cfg(test) load_from_str) onto one pipeline_from_str. That duplication is exactly what would have let a new field reach one path and not the other. 30 exhaustive Pipeline literal sites updated across config/eject/export/matrix/runner/types plus app/yah/cli/src/camp.rs:7529 (the synthesised cloud-reconcile pipeline).")
//! @yah:handoff("WIRE. crates/yah/rpc/src/lib.rs: QedPipelineWire.description, serde default so an older daemon still deserializes. app/yah/cli/src/camp.rs qed_pipelines_handler populates it for user pipelines; None for auto-ingested GHA workflows, whose leading YAML comments are ceremony rather than a readme.")
//! @yah:handoff("CONSUMER. QedPanel.tsx: detailView state, effectiveView and the whole Steps/Graph sub-tab strip are deleted. Body is now readme then steps then graph, stacked in one scroll. New exported PipelineReadme is collapsible and open by default; collapsed it keeps the first line as its own summary. It renders through the existing agent Markdown component, which is safe outside a TicketsContext provider because that context has a default value. New exported descriptionFromWire normalises absent/empty/whitespace-only to undefined so a single truthiness check decides whether the section exists at all. Single-step pipelines still get no graph, the same cut the retired strip made at steps.length less-than-or-equal 1, kept deliberately per gotcha 1.")
//! @yah:handoff("DISCOVERED WORK, done in this pass. (1) Regenerated .yah/schema via cargo run -p xtask -- emit-schemas, required because PipelineConfig gained a field. That run also swept in @Ashguard:coffee's in-flight W265/R584 types (MirrorConfig::drivers, the local-process and local-pg-dev provider-kind arms) into mirror.toml.schema.json and provider.toml.schema.json. cargo test -p xtask --test schema_drift is now 3/3 green; it had been red since at least the R625-F3 note in xtask/src/main.rs, which parked it on 'belongs to whoever changed the types'. A durable @yah:notify_on(R703-F3) was written onto R584 saying what was regenerated. (2) Real-data probe (throwaway test, removed) over all 24 .yah/qed/*.toml through the new parser caught two defects the unit tests missed: the taplo #:schema directive leaked in as the readme's first line, and the annotation terminator needed to be line-initial rather than trimmed. Both fixed and now covered by tests.")
//! @yah:next("NOT VERIFIED END-TO-END IN THE LIVE APP. Descriptions do not reach the desktop until the camp daemon runs a rebuilt yah binary. Deliberately did not run cargo xtask install or restart the daemon: 8 sessions are live in this camp and peers have uncommitted in-flight work (app/yah/cli/src/cloud.rs was mid-edit during this session), so a rebuild would ship half-landed peer code into ~/.local/bin/yah. Operator: cargo xtask install then restart the camp daemon to see it.")
//! @yah:verify("VERIFIED cargo test -p yah-qed --lib -- --test-threads=1 : 682 pass, 0 fail, 1 ignored (7 new tests in config::tests covering paragraph breaks, box-rules, the annotation terminator, mid-sentence annotation mentions, the #:schema directive, no-header, comments-below-the-header, and the explicit-key override)")
//! @yah:verify("VERIFIED cargo test -p xtask --test schema_drift : 3 pass, 0 fail")
//! @yah:verify("VERIFIED cargo check -p yah, -p desktop : clean")
//! @yah:verify("VERIFIED bun run typecheck in packages/yah/ui : clean")
//! @yah:verify("VERIFIED bun test src/components/run/ : 56 pass, 0 fail (5 new in qedReadme.test.tsx: prose renders open by default, folds to first line and unfolds, a 50-line header is shown whole and never truncated, and absent/empty/whitespace-only all mean no readme section)")
//! @yah:verify("VERIFIED by real-data probe over all 24 .yah/qed/*.toml: cli-release 50 lines, oss-publish 130, release-build 53, check 20 (stops correctly at its @yah: block); provider-smoke and publish-assets are annotations-only headers and correctly yield None; baseline/peers/gha-actions are not pipelines and error at load as before")
//! @yah:cleanup("The MCP qed.pipelines tool (crates/yah/agent-tools/src/qed_tools.rs:397) still does not emit description. Left off on purpose: 24 pipelines times up to 130 lines of prose is a large unconditional tool result. If an agent should be able to read a pipeline readme, that wants an opt-in argument, not an always-on field.")
//!
//! @yah:ticket(R717-T1, "QedStep::inputs — content-hash an arbitrary step's declared source files (staleness, generalized off StepKind::Import)")
//! @yah:status(review)
//! @yah:assignee(agent:bundle-anthropic-glimmerstone)
//! @yah:at(2026-08-08T21:19:30Z)
//! @yah:phase(P1)
//! @yah:parent(R717)
//! @arch:see(.yah/docs/working/W296-executable-docs-notebook-cells.md)
//! @yah:next("Add inputs: Vec<PathBuf> to QedStep (serde default, skip_serializing_if empty) — existing pipeline TOMLs must deserialize unchanged.")
//! @yah:next("At run time blake3 each declared input and record the path->hash map on StepStatus. The recorded hashes are the ONLY persisted half: staleness is computed at read time (blake3(inputs now) != recorded), never stored as a RunStatus variant — W296 'Staleness is computed, never stored as a status'.")
//! @yah:next("Model on ImportConfig's existing blake3 source pin (types.rs:841, 'the pinned hash is the guardrail that detects a drifted source'). This is that mechanism re-pointed off StepKind::Import onto any step kind, not a new one.")
//! @yah:next("Tier: Warrior — small surface, but serde back-compat across 494 on-disk run metas and a hash that other tickets read.")
//! @yah:verify("cargo test -p qed --lib — an existing .yah/qed/*.toml with no inputs= round-trips unchanged")
//! @yah:gotcha("types.rs is under active edit by R622 (@Ashguard:coffee) landing StepKind::Manual + RunStatus::AwaitingHuman. Re-read before editing; keep the diff inside QedStep/StepStatus and do not touch StepKind or RunStatus.")
//! @yah:handoff("SHIPPED (uncommitted). QedStep::inputs: Vec<PathBuf> with serde(default, skip_serializing_if = Vec::is_empty), and StepStatus::input_hashes: BTreeMap<String,String> with the same guard. BTreeMap not HashMap so the serialized journal is byte-stable across runs with identical inputs. Runner hashes in both step loops (main + [[finally]]) and both hash BEFORE the step executes — the digest has to answer 'which bytes produced this result?', and a step that rewrites its own input would otherwise pin the bytes it emitted. Skipped and background steps record an empty map: a skipped step produced no result for a digest to be about, and a sidecar is reaped rather than completed.")
//! @yah:handoff("NEW FILE oss/qed/crates/qed/src/staleness.rs — the pure comparison core, re-exported from lib.rs. input_freshness(recorded, actual) -> InputFreshness::{Unrecorded, Fresh, Stale{changed}} plus hash_declared_inputs(root, declared) (the only fn here that touches the filesystem; import.rs stays side-effect-free, which is why this did not go in there). Three decisions worth keeping: (1) Unrecorded is a distinct variant from Fresh — a run that predates the field must render 'no freshness evidence', not a green badge. (2) A missing input records the sentinel ABSENT_INPUT (\"absent\", which cannot collide with a 64-char blake3 hex) rather than being omitted, so 'the file was missing when this ran' and 'this run predates the field' stay distinguishable. (3) Keys are the paths AS DECLARED, not as resolved, so the record is portable across machines with different camp roots.")
//! @yah:handoff("DISCOVERED WORK, done in this pass. (a) transform.rs:526 carried a doc comment asserting 'QedStep has no Default; its literal sites construct all fields explicitly' — FALSE since R633 (types.rs impl Default for QedStep, which round-trips serde's own defaults and is therefore stricter than a hand-written literal). base_step() and both config.rs gha-synthesis literals now overlay on Default::default() instead of enumerating 30+ fields, and the comment says what is actually true. That is the churn this ticket paid twice. (b) Dropped now-unused imports (OnFail in config.rs; StepActivation/StepKind in transform.rs) and one stale `let mut` in types.rs graph_walk_detects_direct_self_cycle. (c) REGENERATED .yah/schema/qed-pipeline.toml.schema.json — QedStep is inside PipelineToml, which is the schema's source of truth, so the xtask drift gate would have gone red. Diff is exactly the two new fields.")
//! @yah:gotcha("cargo test -p yah-qed --lib DOES NOT TERMINATE on a host without docker. runner::tests::local_container_step_routes_through_docker_path hangs indefinitely (cargo prints 'has been running for over 60 seconds' and never returns) rather than skipping — this is the test the R717 prompt describes as 'legitimately skipped', and that phrasing understates it: it blocks the whole suite, so every run here has to be `-- --skip local_container_step_routes_through_docker_path`. Also: two concurrent runs of the qed test binary deadlock each other, so do not launch a second while one is live.")
//! @yah:handoff("Tree anchor at handoff: 85801e7f6b76b369c0c8ecd2e5c7874990cd9286 — the shared tree as I left it. Diff against it (`git diff 85801e7f6b76b369c0c8ecd2e5c7874990cd9286..HEAD`) to see what landed under you, and quote this SHA rather than 'HEAD' in any revert/restore instruction.")
//! @yah:next("PATHSPEC (T1+T2+T3 land together — they touch the same two structs and one compile pass): oss/qed/crates/qed/src/{types.rs,runner.rs,staleness.rs,lib.rs,config.rs,transform.rs,matrix.rs} .yah/schema/qed-pipeline.toml.schema.json app/yah/cli/src/camp.rs")
//! @yah:verify("cargo test -p yah-qed --lib -- --skip local_container_step_routes_through_docker_path (from oss/qed): 778 passed / 0 failed / 1 ignored / 1 filtered. Baseline quoted at dispatch was 729.")
//! @yah:verify("cargo test -p yah --lib r325_f1: 36 passed / 0 failed — the daemon-side run-history + concurrency tests still green against the widened StepStatus.")
//! @yah:verify("cargo check -p yah --lib --tests: clean (one pre-existing unused-import warning for WorkItemAnno, not mine).")
//! @yah:verify("cargo run -p xtask -- emit-schemas: qed-pipeline.toml.schema.json regenerated; git diff shows only the inputs + secret properties.")
//! @yah:handoff("Reconciliation audit: baton was verify+commit only, no residual noted. QedStep::inputs/StepStatus::input_hashes/staleness.rs confirmed landed in 871fde1c by content (git show). cargo test -p yah-qed --lib -- --skip local_container_step_routes_through_docker_path: 782 passed / 0 failed / 1 ignored (covers T1/T2/T3/T5/F4 together, same pathspec).")
//!
//! @yah:ticket(R717-T3, "CellRef on QedRunMeta: key a run by what it was about (doc, cell_id, param_fingerprint)")
//! @yah:status(review)
//! @yah:assignee(agent:bundle-anthropic-glimmerstone)
//! @yah:at(2026-08-08T21:19:37Z)
//! @yah:phase(P1)
//! @yah:parent(R717)
//! @arch:see(.yah/docs/working/W296-executable-docs-notebook-cells.md)
//! @yah:next("Add CellRef { doc: String, cell_id: String, param_fingerprint: String } and QedRunMeta::cell: Option<CellRef> with serde(default, skip_serializing_if) so the ~494 existing run metas on disk deserialize untouched.")
//! @yah:next("param_fingerprint = blake3 over the CANONICALIZED resolved params. Canonicalization is the whole ticket: sort keys, normalize value rendering, and decide explicitly whether unset-with-default participates — two operators reaching the same effective params must produce the same fingerprint or the badge splits in half.")
//! @yah:next("This is W296's one structurally new mechanism: 'Nothing in the tree indexes a QED run by what it was about — runs are keyed by run_id and grouped by pipeline name.' It is what lets W257 render green for us-west-003 and unrun for us-west-013 at the same time.")
//! @yah:next("Keep run meta the source of truth so the R717-F6 index stays rebuildable by rescan — the same property load_qed_history relies on today.")
//! @yah:next("Tier: Warrior — tiny struct, but a fingerprint that is unstable across equivalent inputs silently shows a green light about the wrong box, which W296 calls out as worse than no light.")
//! @yah:verify("cargo test -p qed --lib — a run meta written before this ticket still loads; two param orderings of the same values fingerprint identically")
//! @yah:gotcha("types.rs is under active edit by R622 (@Ashguard:coffee). Re-read before editing.")
//! @yah:handoff("SHIPPED (uncommitted). types.rs gains CellRef { doc, cell_id, param_fingerprint } and QedRunMeta::cell: Option<CellRef> with serde(default, skip_serializing_if = Option::is_none). Runner carries it as PipelineRunner::cell, set by a new with_cell() setter mirroring with_events/with_camp_root, and stamps it onto the terminal QedRunMeta — which is what keeps R717-F6's index rebuildable by rescanning .yah/jit/qed/*.json. Deliberately NOT inherited by sub-pipeline children (a child is a different pipeline; stamping the parent's key would file the child's verdict under the parent's badge) and NOT inherited by matrix fan-out children in camp.rs (a matrix row's params are a narrowing the doc never declared, so N rows would file N verdicts under one subject).")
//! @yah:handoff("CANONICALIZATION, which the ticket correctly calls the whole job. New pub fn param_fingerprint(&HashMap<String,String>) -> String, blake3 hex. Four rules, each chosen against a specific way of getting it wrong. (1) SORTED BY KEY — HashMap iteration order is not stable across runs let alone processes, so hashing in iteration order would give the SAME operator a different fingerprint on a re-run. (2) FED THE POST-resolve_params MAP, so a param taken from its default is indistinguishable from the same value passed explicitly — that equivalence is the intent: the subject is what the run was about, not how it was spelled. (3) EMPTY VALUES PARTICIPATE — node=\"\" is not the same subject as an unset node, and collapsing them merges two histories. (4) LENGTH-PREFIXED FRAMING, not a delimiter join: {node:'a', x:'=b'} and {node:'a=', x:'b'} collide under a naive format!(\"{k}={v}\") concatenation, and a collision here shows one box's verdict for another — the exact failure W296 calls worse than no light. An empty param map fingerprints to a stable value rather than erroring: a doc with no params has exactly one subject, which is legitimate.")
//! @yah:handoff("Tree anchor at handoff: 85801e7f6b76b369c0c8ecd2e5c7874990cd9286 — the shared tree as I left it. Diff against it (`git diff 85801e7f6b76b369c0c8ecd2e5c7874990cd9286..HEAD`) to see what landed under you, and quote this SHA rather than 'HEAD' in any revert/restore instruction.")
//! @yah:next("R717-F6 consumes this: QedRunMeta::cell is the source of truth, so cells.json is a pure cache and a rescan of .yah/jit/qed/*.json rebuilds it. Nothing writes a CellRef yet — the producing call site is R717-T7 (CLI) / R717-T8 (agent door), which construct one from DocSource::doc + the cell id + param_fingerprint(resolved_params) and pass it to PipelineRunner::with_cell.")
//! @yah:verify("types::tests::a_pre_r717_run_meta_still_deserializes — a hand-written pre-R717 meta JSON (no cell, no input_hashes) loads, and re-serializing it does NOT invent either key. That is the ~494-file back-compat contract, pinned.")
//! @yah:verify("types::tests::fingerprint_is_stable_across_param_orderings / fingerprint_separates_two_subjects / fingerprint_cannot_be_forged_by_a_value_containing_the_delimiter / an_empty_value_is_not_the_same_subject_as_an_absent_one.")
//! @yah:verify("runner::tests::a_cell_ref_reaches_the_terminal_run_meta — with_cell() survives to QedRunMeta::cell, and an ordinary run still reports None (cell is opt-in, not a new default).")
//! @yah:verify("cargo test -p yah-qed --lib -- --skip local_container_step_routes_through_docker_path: 778 passed / 0 failed. cargo test -p yah --lib r325_f1: 36 passed / 0 failed.")
//! @yah:handoff("Reconciliation audit: baton was verify+commit only, no residual noted. CellRef + param_fingerprint confirmed landed in 871fde1c by content. cargo test -p yah-qed --lib -- --skip local_container_step_routes_through_docker_path: 782 passed / 0 failed / 1 ignored.")
//!
//! @yah:relay(R719, "QED admission: serial outer pipelines by default, parallelism by opt-in")
//! @yah:at(2026-08-05T05:38:10Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:gotcha("Operator intent, stated 2026-08-04: MOST of the time the outermost pipeline is serial — the QED runner consumes one at a time until success/failure. Two outer pipelines running concurrently is the OPT-IN, not the default. Multimachine fan-out INSIDE one pipeline stays parallel and is explicitly wanted.")
//! @yah:gotcha("Today the default concurrency key is the pipeline's OWN NAME (types.rs::effective_concurrency_key), so the shipped semantics are 'one run at a time per pipeline', not 'one pipeline at a time'. Every cross-pipeline serialization in this camp is hand-stamped 'cargo-target' and was audited once, by hand, in R435-T3 — with no guard. Recipe #24 (desktop-release) missed the stamp and nobody noticed until an operator launched three releases and got four concurrent runners.")
//! @yah:assumes("The inversion is safe for multimachine work because matrix fan-out already holds the parent's key ONCE and runs rows concurrently under it (camp.rs::qed_run_matrix_fanout), and sub-pipeline children never lock at all — so making the OUTER default stricter does not narrow any intra-pipeline parallelism that exists today.")
//! @arch:see(.yah/docs/working/W298-shared-build-admission.md)
//! @arch:see(.yah/docs/working/W170-qed-recipe-discipline.md)
//! @yah:gotcha("STOPGAP ALREADY LANDED (2026-08-04, uncommitted): .yah/qed/desktop-release.toml now carries concurrency_key = \"cargo-target\". That restores serialization for the ONE recipe that triggered this relay; it does not fix the default, and it does not close R719-F2 (the `release` → sub_pipeline{desktop-release} path still bypasses the lock). Do not read the green behaviour as evidence the relay is done.")
//!
//! @yah:ticket(R719-F1, "Invert the default concurrency key: camp-global instead of pipeline-name")
//! @yah:status(review)
//! @yah:at(2026-08-08T23:16:47Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R719)
//! @yah:next("Change Pipeline::effective_concurrency_key (types.rs:464) to fall back to a camp-global sentinel (propose \"@camp\") instead of &self.name. Keep explicit concurrency_key and the \"@parallel\" sentinel exactly as they are — this changes ONLY the unset case.")
//! @yah:next("Sweep .yah/qed/*.toml: every recipe that today relies on the pipeline-name default now serializes camp-wide. Decide per recipe whether it wants @parallel (read-only / no shared resource), a narrow key, or the new default. The cloud-apply pair (publish-assets, yah-desktop-publish-assets) and pi-image-build already carry explicit keys and are unaffected.")
//! @yah:next("Decide whether 'cargo-target' should simply BECOME the default rather than a distinct key — with the inversion, ~15 of the camp's recipes carry a key that means the same thing the default now means. Collapsing them is a separate, larger cleanup; do not fold it into this ticket without saying so.")
//! @yah:next("Update the doc comment on Pipeline::concurrency_key (types.rs:379-386) — it currently documents the old default in prose and will be actively misleading.")
//! @yah:next("Tier: Wizard — small diff, but it re-points the admission default for every recipe in every camp; the judgement is in the per-recipe sweep, not the edit.")
//! @yah:verify("cargo test -p qed --lib concurrency")
//! @yah:verify("Launch desktop-local + release + desktop-release from the QED tab; exactly one shows Running, the other two hold at Queued, and they drain in launch order (tokio::sync::Mutex is FIFO-fair).")
//! @yah:gotcha("A test asserting the OLD default almost certainly exists — grep effective_concurrency_key across oss/qed/crates/qed/src before assuming a green suite means the inversion is inert.")
//! @yah:handoff("SHIPPED, uncommitted. effective_concurrency_key now falls back to DEFAULT_CONCURRENCY_KEY (\"@camp\") instead of self.name; PARALLEL_CONCURRENCY_KEY named alongside it. Explicit keys and @parallel are untouched. types.rs field doc rewritten to lead with why the old default was backwards: forgetting a key used to buy you PARALLELISM silently, now it buys you serialization visibly.")
//! @yah:next("SWEEP RESULT — after it, ZERO camp pipelines rely on the default, so no live recipe changed lanes. provider-smoke -> cargo-target (its one step is `cargo run -p runner --example hok_smoke`, which compiles against the shared target/; the file comment saying it 'builds nothing' is about the assertion, not the work — it was racing check/release under the old default). gha-schema-drift -> @parallel (curl+cmp, no shared resource). rusty-v8-musl -> \"rusty-v8\" (a multi-hour V8 build in a container on us-west-002; camp-global would park every local cargo recipe behind it for nothing). baseline.toml / gha-actions.toml / peers.toml are not pipelines and were left alone.")
//! @yah:next("DEFERRED, NOT DONE (operator's explicit call): the 'cargo-target becomes the default' collapse. Worth filing as its own ticket, and here is the precise residual hole it closes — @camp and cargo-target are DIFFERENT lanes, so a future recipe that cargo-builds and forgets a key lands in @camp and still races the ~19 cargo-target recipes. Today that costs nothing because the sweep left @camp empty, so this is a latent trap, not a live bug. The fix is one line (DEFAULT_CONCURRENCY_KEY = \"cargo-target\") plus deleting ~19 now-redundant stamps, and the judgement call is whether a non-cargo recipe should inherit the cargo lane by default.")
//! @yah:gotcha("I RENAMED camp::r325_f1_tests::queued_runs_serialize_per_pipeline to ...serialize_on_the_shared_key, because it no longer proves anything about the default (its two runs used to share the key via the pipeline name `slow`; they now share it via @camp, so it stays green either way — exactly the inert-suite trap this ticket's gotcha predicted). .yah/qed/baseline.toml named that test in a [[failures]] entry, so the rename orphaned it; the entry is updated in the same change. New test two_different_unkeyed_pipelines_serialize_camp_wide is the one that actually pins the inversion.")
//! @yah:next("PATHSPEC (F1 only; camp.rs is shared with the uncommitted F4/F5 work, so it lands with them): oss/qed/crates/qed/src/types.rs .yah/qed/provider-smoke.toml .yah/qed/gha-schema-drift.toml .yah/qed/rusty-v8-musl.toml .yah/qed/baseline.toml app/yah/cli/src/camp.rs")
//! @yah:verify("cargo test -p yah-qed --lib (720 pass / 0 fail; 5 formerly-documented pre-existing failures are gone)")
//! @yah:verify("cargo test -p yah --lib r325_f1 (35 pass, incl. every_camp_pipeline_loads_and_validates over the 3 edited recipes)")
//! @yah:verify("cargo test -p xtask --test schema_drift (3 pass — the doc change is on types::Pipeline, not the PipelineToml the schema derives from)")
//! @yah:handoff("Tree anchor at handoff: 85801e7f6b76b369c0c8ecd2e5c7874990cd9286 — the shared tree as I left it. Diff against it (`git diff 85801e7f6b76b369c0c8ecd2e5c7874990cd9286..HEAD`) to see what landed under you, and quote this SHA rather than 'HEAD' in any revert/restore instruction.")
//! @yah:handoff("VERIFIED AND COMMITTED. The F1 change is no longer uncommitted — types.rs, the 3 swept recipes and baseline.toml all landed in 871fde1c/5e86d6d9. Working tree now carries only the assignee flip from this session's claim.")
//! @yah:verify("cargo test -p yah --lib r325_f1 — 36 pass / 0 fail, including two_different_unkeyed_pipelines_serialize_camp_wide (pins the inversion) and queued_runs_serialize_on_the_shared_key.")
//! @yah:handoff("The DEFERRED cargo-target collapse is now filed as R719-T6 (open) rather than living only in this ticket's next-list.")
//! @yah:verify("Re-swept .yah/qed/*.toml at 5e86d6d9: every pipeline file carries an explicit concurrency_key; the only keyless files are baseline.toml / gha-actions.toml / peers.toml, which are not pipelines. The @camp lane is empty, so no live recipe changed lanes.")
//!
//! @yah:ticket(R719-T6, "Collapse cargo-target into the default concurrency key (or decide not to)")
//! @yah:at(2026-08-08T23:16:27Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R719)
//! @yah:next("Decide whether DEFAULT_CONCURRENCY_KEY should become cargo-target instead of @camp. If yes: one line in types.rs plus deleting the ~19 now-redundant cargo-target stamps under .yah/qed/.")
//! @yah:next("The judgement is the operators: should a non-cargo recipe that forgets a key inherit the cargo lane? Collapsing says yes and closes the hole; keeping them separate says no and leaves it. Deferred out of R719-F1 by explicit operator call, not oversight.")
//! @yah:verify("cargo test -p yah --lib r325_f1")
//! @yah:gotcha("Not a live bug today — R719-F1 swept every .yah/qed pipeline onto an explicit key, so the @camp lane is EMPTY. The hole is latent: a FUTURE recipe that cargo-builds and forgets a key lands in @camp and races the ~19 cargo-target recipes.")

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
    /// Where this recipe is allowed to run (W170). Defaults to
    /// [`Placement::Anywhere`]. The runner enforces this at kick time
    /// (R435-F2) — the recipe body itself remains environment-agnostic.
    #[serde(default)]
    pub placement: Placement,
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
}

/// Key an unkeyed pipeline falls back to (R719-F1). Camp-global: every
/// pipeline that declares no `concurrency_key` shares this one lane.
///
/// The `@` prefix marks it as a sentinel rather than a plausible user key,
/// matching [`PARALLEL_CONCURRENCY_KEY`]. Unlike `@parallel` it needs no
/// special handling anywhere — it is an ordinary map key that happens to be
/// spelled so nobody types it by accident.
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

    /// Substitute `{{key}}` placeholders in every step's `argv`, `env`, and a
    /// `gha-workflow` step's `inputs` and `matrix` (a gha-workflow step has no
    /// argv or env, so these are its only parameterisable surface). Unknown
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
    /// Run through `sh -c` in the pipeline workspace. `None` ⇒ honour-system
    /// advance on the human's word alone.
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
}

fn default_manual_advance_poll_secs() -> u64 {
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
    #[error(
        "step `{0}`: `secret = true` is only valid on subprocess steps — every other \
         kind's output is qed's own text (docker build progress, a wait-for probe's \
         verdict), so the runner has no step-authored capture there to suppress and \
         the flag would silently do nothing (R717-T2)"
    )]
    SecretRequiresSubprocess(String),
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
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![QedStep {
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
    fn resolve_params_fills_declared_defaults() {
        // The release-build case, expressed properly. That pipeline declares
        // all five params `required` and documents that callers must always pass
        // them, "empty string where N/A", precisely because an absent param is
        // left as a literal `{{features}}` in the argv. A default says that once,
        // in the pipeline, instead of in every caller.
        let mut p = one_step(vec!["build", "{{package}}", "{{features}}"], &[]);
        p.params.insert(
            "package".to_string(),
            ParamDef { required: true, description: None, default: None, options: Vec::new(), options_from: None },
        );
        p.params.insert(
            "features".to_string(),
            ParamDef {
                required: false,
                description: None,
                default: Some(String::new()),
                options: Vec::new(),
                options_from: None,
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
                ParamDef { required: true, description: None, default: None, options: Vec::new(), options_from: None },
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
