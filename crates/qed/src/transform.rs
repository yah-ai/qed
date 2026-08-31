//! Assisted one-way GHA→QED transformer (R533-F4, W224).
//!
//! W224 ("import, don't emulate") makes a GitHub Actions workflow an *import
//! source*, not a foreign runtime QED reproduces forever. The onboarding path
//! is a **one-way, assisted, lossy-with-warnings** transform: map the tier-1/2
//! ~80% mechanically, and **flag** the tier-3 steps it deliberately declines to
//! imitate — proposing the native QED replacement for each rather than guessing.
//!
//! This module is that transform. It sits on top of the [`yah_qed_gha`] parser +
//! the R533-F2 [tier classifier](yah_qed_gha::classify_step) and emits native
//! [`QedStep`]s for the runnable compute, paired with a list of human-facing
//! flags for everything that needs a decision. It is **pure** — it operates on
//! an already-parsed [`Workflow`] and performs no I/O — so the runner / the
//! `eject` materializer (R533-F6) own the file read and the TOML write.
//!
//! ## What maps mechanically, what gets flagged
//!
//! | Parsed step | Tier (F2) | Result |
//! |---|---|---|
//! | `run:` bash | 1/2 compute | **native** [`StepKind::Subprocess`] step |
//! | `run:` bash reaching `gh`/`api.github.com`/`GITHUB_TOKEN` | 1/2 + service touch | native step **and** an [`FlagKind::EmbeddedServiceTouch`] flag |
//! | `run:` carrying `${{ … }}` | 1/2 compute | native step **and** an [`FlagKind::UnresolvedExpression`] flag |
//! | `uses: org/setup-*`, `dtolnay/rust-toolchain`, … | 1/2 toolkit | [`FlagKind::ToolkitAction`] — runs via the R533-T7 toolkit executor, no subprocess emitted yet |
//! | `uses: actions/checkout`, `cache`, `upload-artifact`, `gh-release`, `build-push`, … | 3 service | [`FlagKind::ReplaceWithNative`] carrying the native stanza |
//! | `uses:` unrecognized | unknown | [`FlagKind::Unknown`] — surfaced for review |
//!
//! ## Job DAG → step DAG
//!
//! GHA workflows are a job DAG. A native [`Pipeline`](crate::types::Pipeline) is
//! a `Vec<QedStep>` — but since R605-F3 that Vec carries edges
//! ([`QedStep::needs`](crate::types::QedStep::needs), scheduled by
//! [`crate::dag`]), so the job graph survives the import instead of being
//! flattened out of it.
//!
//! The mapping is one edge per **job boundary**, not per step:
//!
//! - steps are emitted job by job in [`topo_sort`] order, so the file still
//!   reads top-to-bottom the way the workflow does;
//! - the **first** emitted step of each job gets an explicit `needs` naming the
//!   **last** emitted step of each of its `needs:` predecessors — or
//!   `needs = []` when it is a root;
//! - every other step of a job says nothing, which is the implicit chain edge
//!   to its predecessor *within the same job* — exactly GHA's within-job
//!   sequencing.
//!
//! A predecessor job that emitted **no** native steps (every step tier-3, so
//! all of them flagged) is transparent: its dependents inherit *its*
//! predecessors' tails. Otherwise a job whose only content was
//! `actions/checkout` would sever the branch it sits on.
//!
//! This is what makes an ejected multi-job workflow run its independent
//! branches concurrently rather than in a topological line. Note what it is
//! *not* a claim about: the [`yah_qed_gha`] emulator that runs a **wrapped**
//! workflow (`kind = gha-workflow`) still executes one instance at a time
//! unless its own concurrency cap is raised — see `runtime.rs`. Porting and
//! wrapping are different paths, and only the ported one goes through this
//! module.
//!
//! Matrix expansion and the `workflow_call` port contract are *not* handled
//! here: target lifting out of `strategy.matrix` is R533-F9 (it layers onto the
//! steps emitted here) and the down/up-port mapping is R533-F5.
//!
//! @yah:ticket(R605-F3, "QED executes a job DAG: dependency edges on the step/job layer + a concurrent scheduler")
//! @yah:status(review)
//! @yah:at(2026-08-16T05:46:37Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R605)
//! @yah:verify("A pipeline with two independent branches and one join records overlapping start/end timestamps for the branches, and the join starts only after both finish")
//! @yah:verify("yah qed eject on a multi-job workflow emits the job structure rather than a flat list, and `yah qed validate` still round-trips it")
//! @yah:gotcha("The reporting layer already models this and is NOT the gap: the run-status wire struct carries `needs: Vec<String>` per job (W223 R532-F2) so the graph viewer can draw dependency edges for a WRAPPED workflow. That is display-only, fed from yah_qed_gha::plan - it does not mean native pipelines have dependencies.")
//! @yah:gotcha("Do not file this as a release.yml blocker on parallelism grounds. Porting release.yml flat costs NOTHING against QED-today, because the emulator is serial too. The parallelism gap is against GitHub, and you already pay it the moment you run release.yml through QED at all. The real reasons to hold release.yml are its 52 tier-3 flags (several with no native replacement built yet) and it being a one-way move off the thing that currently ships releases.")
//! @arch:see(oss/qed/crates/qed-gha/src/runtime.rs)
//! @arch:see(oss/qed/crates/qed/src/transform.rs)
//! @yah:gotcha("LIVE DEFECT found from R776-T2 (read-from-call-graph, NOT executed -- confirm with a test first). Step-level matrices never expand on either run path. expand_step has one production caller, matrix::plan (oss/qed/crates/qed/src/matrix.rs:311), and both paths gate it on a PIPELINE-level matrix: app/yah/cli/src/camp.rs:8831-8836 (None arm hands the raw pipeline to PipelineRunner at :8882-8885) and app/yah/cli/src/qed.rs:1446-1454. PipelineRunner never reads QedStep::matrix. The camp.rs:8828-8830 comment claiming step matrices keep runner-side handling is false. Effect: the step runs once with the matrix expression literal in argv.")
//! @yah:gotcha("Sequencing that follows from the above: make step matrices expand BEFORE resolving needs, then resolve needs against the EXPANDED list. background_until already shows the failure -- it resolves by name against the expanded list (runner.rs:2461-2490), so background_until = 'build' against a matrixed 'build' errors 'names unknown step' because the post-expansion name is 'build [k=v]' (matrix.rs:326-350) and no author writes that by hand. A name-keyed needs inherits this exactly. Separately: background_until is POSITIONAL -- runner.rs:2496-2504 rejects a target at or before the declaring step, and a partial order has no 'later'. See .yah/docs/working/W322-plugin-nodes-in-the-build-dag.md section 2.")
//! @yah:gotcha("R776-T3: matrix::apply_matrix_to_step (matrix.rs:366-391) substitutes argv/env/cwd/platform.target/platform.container_platform and nothing else -- notably NOT the new `resource` field. A per-row resource key stays one shared literal across every fanned row, so every instance of a matrixed cargo step serializes against every other, including rows building into genuinely separate target dirs. Conservative rather than racy, but it caps a fan-out at the pessimal answer. Adding `resource` to that substitution list is a one-liner.")
//! @yah:gotcha("R776-T3: `if_cond` is not substituted by apply_matrix_to_step either, and the runner builds ctx.matrix from self.matrix_coord -- the PIPELINE-level coord (runner.rs:1805-1815). So a step-level matrix instance cannot gate on its own coord: an if= naming matrix.<key> resolves to Null and EVERY instance skips. Worth knowing before anyone reaches for if= as the per-instance selection mechanism; W322 section 3 rejects that route for exactly this reason and uses a goal closure over needs instead.")
//! @yah:handoff("BOTH HALVES SHIPPED (uncommitted; parts of it were swept into a peer sync commit mid-session). Half (b), the one that gated everything: QedStep.needs is a three-state Option<Vec<String>> -- absent = implicit chain edge to the previous step, needs = [] = root, needs = [..] = exactly those. The absent case is the whole backwards-compat story: every pipeline TOML in every camp omits the key, so every one of them resolves to the chain 0-1-2-.. and runs byte-identically, same event stream, same rows. Reading absent as no-dependencies would have fanned out the entire corpus overnight.")
//! @yah:handoff("New module oss/qed/crates/qed/src/dag.rs: predecessors() / waves() / dependents() / is_explicit(), Kahn over the step slice, with a Missing policy (Reject for the loader, Satisfied for the runner because resume-from-step hands it a drained prefix). A needs entry matches a step by name OR by the \"<name> [k=v]\" shape matrix::plan gives a fanned-out row, so needs = [\"build\"] joins on every matrix row the way GHA needs: does. 14 unit tests.")
//! @yah:handoff("run_inner()s sequential for-loop is now a ready-queue scheduler (runner.rs ~2605). A step is admitted when every predecessor is done; up to Pipeline.max_parallel (default dag::DEFAULT_MAX_PARALLEL = 4) run at once, minus any whose QedStep.resource key is held. The 400-line loop body moved out to run_one_step(), which takes the step by value and returns a StepOutcome the scheduler folds BY STEP INDEX -- so produced artifacts and status rows stay in declaration order even when the steps overlapped.")
//! @yah:handoff("Every item on the blast-radius list was preserved, not worked around. Event indices unchanged (index + index_offset), so the desktop QED tab and remote-resume both still index by pipeline-local step index. background_until keeps its declaration-order check AND gains a stricter one under a declared DAG: the gate must be a transitive dependent, because \"later in the file\" stops meaning \"after\" the moment two branches exist and a gate on the wrong branch would kill the sidecar mid-use. A background step counts as satisfied at SPAWN (it has no exit to wait for), which makes needs = [\"server\"] a runnable edge rather than a deadlock. Admission-lane handling downgrades Fleet to the runs own lane while any local step is in flight, and the enter_lane await moved INTO the step future so a queued run cannot stall the steps already running.")
//! @yah:handoff("transform.rs now carries the job graph instead of flattening it: one edge per JOB boundary (first native step of a job needs the last native step of each predecessor job; roots get needs = []), with a job that emitted no native steps made transparent so an all-tier-3 job does not sever its branch. Duplicate emitted step names are disambiguated with \" (2)\" -- GHA allows two steps to share a name and QED cannot, since the name is the key a needs edge resolves against. An unresolvable job graph still imports flat with no edges rather than failing.")
//! @yah:handoff("Half (a), the qed-gha emulator: runtime.rs \"Sequential within wave - F4 simplification\" is gone. run_wave() runs a waves instances on std::thread::scope workers up to Executor.max_parallel_jobs, honouring each jobs own strategy.max-parallel underneath (which was already parsed and previously ignored). Results fold back in WAVE order, not completion order, so needs.* aggregation and the returned transcript stay deterministic; the first failure in wave order wins. THE CAP DEFAULTS TO 1 -- unchanged serial behaviour -- and raising it needs a per-job resource key, filed as R605-T4.")
//! @yah:handoff("DISCOVERED WORK fixed in this pass, all outside the ticket title. (1) lib.rs:673 desktop_release_matrix_routes_each_row_to_its_own_platform asserted three matrix rows against a recipe that has had one since 497a8a6b (2026-08-12, which removed both Linux rows deliberately and documented why in the TOML) -- red at HEAD before I started; assertion updated to the real row set with the rationale and the commit. (2) doc_source.rs module docs had three broken doctests from nested ``` fences inside ```text blocks -- outer fences widened to four backticks; cargo test --workspace in oss/qed was red on this before. (3) Regenerated .yah/schema/qed-pipeline.toml.schema.json AND .yah/schema/workload.toml.schema.json plus packages/yah/workload-spec/index.ts. The latter two are NOT mine -- they are R556-T12s MesofactStaticSlot.env, the drift the R625-F3 note in xtask/src/main.rs parked on \"belongs to whoever changed the types\". xtask/tests/schema_drift.rs is green now.")
//! @yah:verify("Verify criterion 1, mechanized as runner::tests::two_branches_overlap_and_the_join_waits_for_both: a root plus two sleep-1 branches plus a join. Asserts the branch rows overlap in recorded start/end timestamps, that the join starts at or after both completed_at, AND that the reported rows are still in declaration order.")
//! @yah:verify("Verify criterion 2, mechanized as eject::tests::a_multi_job_workflow_ejects_the_job_structure_not_a_flat_list: a four-job diamond ejects, the body re-parses as Pipeline TOML, and dag::waves over it is 3 waves with 2 steps in the middle one -- not 4 sequential. eject::tests::needs_survives_the_toml_round_trip pins the serializer. transform::tests::release_yml_transforms_end_to_end now additionally asserts the REAL release.yml ports to a graph with strictly fewer waves than steps.")
//! @yah:verify("The compatibility property has its own test: runner::tests::a_pipeline_without_needs_stays_strictly_serial. If that ever goes green with overlap, every pipeline TOML in every camp just became parallel.")
//! @yah:verify("Suites: cargo test --workspace in oss/qed = 1482 pass / 0 fail across all 8 crates including doctests (869 yah-qed, 133 yah-qed-gha). cargo check --workspace at the camp root = clean. cargo test -p xtask --test schema_drift = 3/3. cargo test -p yah --lib qed = 41/41 (covers the qed_eject round-trip).")
//! @yah:gotcha("No pipeline in .yah/qed/ declares needs yet, deliberately. Porting a real recipe to a DAG is a per-recipe judgement about which of its steps actually contend on target/ or docker, and that is an operator call, not a mechanical edit. The mechanism ships inert: every existing recipe still runs exactly as before.")
//! @yah:gotcha("runner::tests::wait_for_times_out_when_endpoint_never_healthy failed once in ~8 full-suite runs and passed in isolation and in the other 7. It binds an ephemeral port, drops it, and assumes nothing re-binds it -- a pre-existing port-reuse race that the extra load from the new sleep-based concurrency tests makes marginally likelier to lose. Not a scheduler bug: that pipeline is a single step, so it is a one-step wave.")
//! @yah:gotcha("The step-matrix-never-expands finding recorded in the two gotchas above now has its own durable ticket: R605-B5. It was filed separately so the finding survives this ticket archiving. Nothing here is blocked on it -- R605-F3's own mechanism is sound; B5 is the pre-existing gate that keeps dag::name_matches's fan-out branch unreachable on the daemon path.")
//! @yah:handoff("FOLLOW-UP PASS: confirmed and fixed three of the four findings @Ashguard:eclipse appended from R776-T2/T3 while this was in flight. All three were real; the fourth is half-fixed with the remainder recorded as a gotcha. (1) Step-level matrices genuinely never expanded on either run path -- both gated matrix::plan on pipeline.matrix alone and PipelineRunner never reads QedStep::matrix. Latent for the camp (no .yah/qed/*.toml declares a step matrix) but LIVE for this ticket: R533-F9 lift_target emits exactly that shape, so every ported multi-target workflow would have run one step with the expression literal in argv. Added matrix::needs_expansion (pipeline matrix OR any step matrix) and repointed both gates at it -- camp.rs:8846 and qed.rs:1450. A step matrix still yields one PlannedJob, so the multi-row fan-out branch is unreachable from it and that path is unchanged.")
//! @yah:handoff("(2) matrix::apply_matrix_to_step did not substitute the new QedStep.resource, so a per-row key stayed one literal and every fanned row serialized against every other -- the fan-out capped at its pessimal answer. One line, plus matrix::tests::a_matrix_row_gets_its_own_resource_key. (3) background_until matched by exact name, so a gate naming a matrix step failed preflight with \"unknown step\" (post-expansion the name is `client [n=1]`) -- and had it resolved it would have reaped on the FIRST row. Now resolved with dag::name_matches at preflight into a set of step indices carried on BackgroundTask.gate; the reap fires when the last of them is done. BackgroundTask.until is gone, superseded.")
//! @yah:handoff("(4) if_cond: substituted the `${{ matrix.x }}` spelling per row (same one-liner, matrix::tests::a_matrix_row_substitutes_its_own_if_gate) because making step matrices actually expand turned a dead path live and a silent skip-everything is the worst shape to leave it in. The BARE spelling (if = \"matrix.arch == ..\") still resolves through ctx.matrix, which the runner builds from the PIPELINE coord -- see the gotcha. No recipe uses either spelling today.")
//! @yah:gotcha("STILL OPEN from R776-T3, narrowed: a step-level matrix instance cannot gate on its own coord with the BARE spelling. runner.rs:1805-1815 builds ctx.matrix from self.matrix_coord, the PIPELINE-level coord, so `if = \"matrix.arch == 0x86\"` is Null for a step-only matrix and every instance skips. The `${{ matrix.x }}` spelling now works (substituted at expansion). Closing the bare half means threading a per-step coord into the expression context -- a runner change, not a substitution one. W322 section 3 rejects if= as the per-instance selection mechanism for exactly this reason, so nothing depends on it today.")
//! @yah:verify("After the follow-up pass: oss/qed cargo test --workspace = 1487 pass / 0 fail. cargo test -p yah --lib = 1136 pass / 0 fail (covers camp.rs r325_f1 matrix fan-out and the qed_eject round-trip, the two suites the CLI-side gate change could have moved).")
//! @yah:verify("The step-matrix defect has a regression test that would have caught it: matrix::tests::a_step_matrix_alone_needs_expansion_and_gets_it asserts needs_expansion is true for a step matrix under a matrix-less pipeline and that plan() fans the step. background_until over a matrix gate: runner::tests::background_until_gates_on_the_last_row_of_a_matrix_step asserts the sidecar completed_at is at or after BOTH rows.")
//! @yah:handoff("(5) Closed the last inconsistency @Ashguard:eclipse named: `needs` was the only edge-bearing field left out of apply_matrix_to_step once resource and if_cond joined, so a CORRELATED per-row edge (needs = [\"build [arch=${{ matrix.arch }}]\"]) carried the literal into the graph and resolved to nothing -- rejected at load, silently dropped by the runners lenient resolver. Substituted now, with matrix::tests::a_matrix_row_can_declare_a_correlated_per_row_edge asserting the resulting graph is two independent chains rather than a join. The un-correlated needs = [\"build\"] still fans in on every row via dag::name_matches. Final: oss/qed cargo test --workspace = 1488 pass / 0 fail.")
//!
//! @yah:relay(R776, "Design: plugins/sidecars as first-class nodes in QED's build DAG")
//! @yah:at(2026-08-16T02:18:58Z)
//! @yah:kind(spike)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:next("Break into sub-tickets: node granularity, edge expression, composition with the desktop app build, and where R552-F6 blob-resolution plugs into a node's 'already satisfied' check. Land the design as a W### doc before touching transform.rs or config.rs -- this is a modeling question first.")
//! @yah:next("R605-F3's own next-steps already name the blast radius any new edge mechanism has to survive (remote-resume step indexing, background_until's 'after' contract, concurrency_key locking, the desktop QED tab's per-step event stream) -- read those before proposing a shape.")
//! @yah:gotcha("R605-F3 is the prerequisite mechanism, not a duplicate of this: QedStep carries no dependency edges today and PipelineRunner walks steps in strict declared order, so nothing can represent a DAG yet at any granularity. This spike should design the plugin-node shape assuming R605-F3's `needs`-on-step (or job-grouping-above-steps) model lands, not invent a second edge mechanism.")
//! @yah:gotcha("Orthogonal to, not a substitute for, R552-F6 (bundled: -> blake3: blob acquisition). A plugin DAG node can resolve two ways -- build locally, or fetch a published blob -- and the design needs to model both branches, not just the build one, or it re-privileges local build over the R552-F6 direction this was raised alongside.")
//! @yah:gotcha("Today's sidecar build is a single opaque xtask step (`cargo run -p xtask -- build-sidecars`) that loops yah_bundled::BUNDLED with no inter-sidecar edges -- a plugin-depends-on-plugin case is not expressible at all right now, not just inefficiently expressed.")
//! @arch:see(crates/yah/bundled/src/lib.rs)
//! @arch:see(.yah/qed/local-install.toml)
//! @arch:see(.yah/qed/desktop-local.toml)
//! @arch:see(app/yah/desktop/before-build.sh)
//!
//! @yah:ticket(R776-T2, "Edge expression: reuse R605-F3's step-level `needs`, or does plugin-to-plugin dependency need its own layer?")
//! @yah:status(review)
//! @yah:at(2026-08-16T03:01:23Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R776)
//! @yah:depends_on(R605-F3)
//! @yah:notify_on(R605-F3, "Read the SHIPPED needs shape before answering T2. The one thing T2 turns on: can a needs edge name a coord-suffixed matrix INSTANCE (matrix.rs:347 names them `<step> [triple=aarch64-apple-darwin]`), or only the declared step name? Instance-level => plugin-to-plugin edges stay triple-correlated. Step-name-only => every plugin fan-out collapses to all-of-A-needs-all-of-B, and T2 must answer 'own layer' instead of 'reuse'.")
//! @yah:handoff("ANSWER: reuse R605-F3's step-level needs. Plugin-to-plugin dependency does NOT need its own layer. Landed as W322 section 2 (.yah/docs/working/W322-plugin-nodes-in-the-build-dag.md).")
//! @yah:handoff("WHY no second layer: the plugin-to-plugin case has no instance in the tree. The four registry entries each build as cargo build -p NAME from their own workspace_subdir with no edges between them (xtask/src/main.rs:150-154, :384-411). The real edges are plugin->app (desktop bundle needs every sidecar staged) and artifact->install (mcp-sidecar after desktop-bundle), both ordinary step-to-step. A dedicated layer would be speculative generality; when a plugin-to-plugin case appears it is a build-order edge between two steps, which is what needs already is.")
//! @yah:handoff("THE REAL REQUIREMENT on R605-F3: needs must resolve against the EXPANDED step list, and an edge must be able to name either the declared step (meaning all instances) or one instance. Declared-name-only degrades a triple-correlated edge into all-of-A-needs-all-of-B and serializes the fan-out section 1 exists to parallelize. Handed to @Ashguard:griffin (session:3b42fa19) in-session, plus two durable gotchas appended to R605-F3.")
//! @yah:handoff("Found a live defect while answering this: step-level matrices never expand on either run path. Not fixed here -- it lives in camp.rs/qed.rs/runner.rs, which R605-F3 is actively rewriting, so editing them would be a shared-tree hand-fight. Handed over both channels (party.chat + two @yah:gotcha entries on R605-F3), and @yah:notify_on(R605-F3) is registered on this ticket.")
//! @yah:verify("Call graph verified by tree-wide grep: expand_step has exactly one production caller (matrix.rs:311); the other two hits are its definition (:326) and a unit test (:839). Both gate sites read directly (camp.rs:8831-8836, qed.rs:1446-1454). runner.rs .matrix reads are only matrix_coord and the GHA cfg.matrix filter.")
//! @yah:verify("Every cited line was opened and read; four citations that drifted while writing were corrected against grep before filing.")
//! @yah:verify("NOT executed: the never-expands finding is read from the call graph, not observed at runtime. Said so explicitly in the doc and in the handover to @Ashguard:griffin, who should confirm with a test before acting.")
//! @yah:verify("Design-only: no qed source touched. Only edits this session are W322 and one @arch:see line in crates/yah/bundled/src/lib.rs.")
//! @yah:notify_on(R605-F3, "The needs mechanism landed. Read crate::dag in oss/qed/crates/qed/src/dag.rs and QedStep::needs/resource in types.rs before designing the plugin-node edge: needs is a three-state Option (absent = implicit chain, [] = root, [..] = explicit), and QedStep::resource is the shared-resource gate. Both are step-level, so the question this ticket asks is now answerable against real code rather than a proposal.")

use crate::matrix::MatrixSpec;
use crate::platform::PlatformSpec;
use crate::types::{OnFail, QedStep};
use indexmap::IndexMap;
use yah_qed_gha::{
    classify_step, topo_sort, Disposition, ExprString, ExprToken, Job, NativeReplacement,
    ServiceTouch, Step, StepAction, Workflow,
};

/// The result of transforming one workflow — the native steps that mapped
/// mechanically, interleaved (in execution order) with the flags the human must
/// resolve. "Assisted / lossy-with-warnings" made concrete: nothing tier-3 is
/// silently run, and nothing un-mappable is silently dropped.
#[derive(Debug, Clone)]
pub struct TransformReport {
    /// Native pipeline name — the workflow `name:` sanitized to a slug, or
    /// `"imported-workflow"` when the source declares none.
    pub name: String,
    /// Human-readable label — the workflow `name:` verbatim, else the slug.
    pub label: String,
    /// One entry per parsed step, in flattened (topo-job then step) order. Each
    /// carries the mechanically-mapped native step (when one could be emitted)
    /// and/or the flags raised for it.
    pub steps: Vec<TransformedStep>,
}

impl TransformReport {
    /// The mechanically-mapped native steps, in order — the runnable spine F6
    /// materializes and F9 lifts platform targets into.
    pub fn native_steps(&self) -> impl Iterator<Item = &QedStep> {
        self.steps.iter().filter_map(|s| s.native.as_ref())
    }

    /// Owned copy of the native steps, ready to drop into a
    /// [`Pipeline::steps`](crate::types::Pipeline::steps).
    pub fn collect_native(&self) -> Vec<QedStep> {
        self.native_steps().cloned().collect()
    }

    /// Every flag raised across all steps, paired with the step that raised it.
    pub fn flags(&self) -> impl Iterator<Item = (&TransformedStep, &FlagKind)> {
        self.steps.iter().flat_map(|s| s.flags.iter().map(move |f| (s, f)))
    }

    /// `true` when every step mapped to clean native compute with no flag — the
    /// workflow imported losslessly (rare; most real workflows touch tier 3).
    pub fn is_clean(&self) -> bool {
        self.steps.iter().all(|s| s.flags.is_empty() && s.native.is_some())
    }
}

/// One parsed workflow step after transformation. A clean tier-1/2 `run:` step
/// has `native = Some(..)` and `flags = []`; a tier-3 step has `native = None`
/// and a [`FlagKind::ReplaceWithNative`]; a `run:` step reaching the service has
/// **both** a native step (it runs) and a flag (the reach won't resolve on QED).
#[derive(Debug, Clone)]
pub struct TransformedStep {
    /// Owning GHA job id.
    pub job: String,
    /// 0-based index within the job's `steps:` list.
    pub step_index: usize,
    /// The step's `name:` rendered to text, or `None` when unnamed.
    pub step_name: Option<String>,
    /// The mechanically-mapped native step, when one could be emitted. `None`
    /// for purely-flagged steps (tier-3, toolkit `uses:`, unknown) — there is
    /// nothing to run natively yet.
    pub native: Option<QedStep>,
    /// Why this step needs human attention. Empty for clean compute.
    pub flags: Vec<FlagKind>,
}

/// Why a parsed step couldn't be imported as clean native compute — the
/// assisted half of the transform. Each variant proposes what to do instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlagKind {
    /// Tier-3 GitHub-the-service action. Replace with the named native QED
    /// facility; [`stanza_hint`](FlagKind::stanza_hint) carries the guidance.
    ReplaceWithNative(NativeReplacement),
    /// A clean-compute `run:` step that reaches GitHub-the-service from inside
    /// its bash. It still runs on the executor, but the service call won't
    /// resolve on QED — replace the reach with a native facility.
    EmbeddedServiceTouch(Vec<ServiceTouch>),
    /// A tier-1/2 `uses:` toolkit action. Runs via the toolkit-contract
    /// executor (R533-T7); no native subprocess is emitted until that executor's
    /// step surface lands.
    ToolkitAction { slug: String, git_ref: Option<String> },
    /// An unrecognized `uses:` slug — surfaced for review rather than guessed.
    Unknown { slug: String },
    /// A mechanically-mapped `run:` step whose script still carries GHA
    /// `${{ … }}` expressions, which QED's `{{key}}` subprocess substitution
    /// won't expand. Convert to QED params / native outputs, or lift via
    /// import-time target lifting (R533-F9).
    UnresolvedExpression,
    /// A surviving `${{ secrets.NAME }}` reference — split out of
    /// [`UnresolvedExpression`] because its failure mode is categorically
    /// worse than the rest of that bucket.
    ///
    /// Every other unresolved expression degrades to a visibly wrong literal:
    /// a path that doesn't exist, a tag that doesn't match. A secret degrades
    /// to a *non-empty string that looks like a credential*, so the usual
    /// `if [ -z "$TOKEN" ]; then skip; fi` guard passes and the step proceeds
    /// to make authenticated-looking calls with the literal text
    /// `${{ secrets.NAME }}` as its bearer token. A step that would have
    /// safely no-op'd instead fails deep inside a third-party API — which is
    /// exactly the lossy-in-silence outcome this transform exists to prevent.
    ///
    /// The native replacement is real and nameable, which is why this is
    /// `Review` and not `Info`: bridge the name through
    /// `~/.yah/qed/secrets.toml`.
    UnbridgedSecret { names: Vec<String> },
}

/// How loud a [`FlagKind`] is, for preflight summaries and reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlagSeverity {
    /// Tier-3 surface QED declines to imitate — the import *cannot* run this as
    /// authored; a native replacement is required.
    Replace,
    /// A human decision is needed (unrecognized action, embedded service reach).
    Review,
    /// Informational — handled by other relay tickets (T7 executor, F9 lifting).
    Info,
}

impl FlagSeverity {
    pub fn label(self) -> &'static str {
        match self {
            FlagSeverity::Replace => "replace",
            FlagSeverity::Review => "review",
            FlagSeverity::Info => "info",
        }
    }
}

impl FlagKind {
    /// Severity bucket for this flag.
    pub fn severity(&self) -> FlagSeverity {
        match self {
            FlagKind::ReplaceWithNative(_) => FlagSeverity::Replace,
            FlagKind::EmbeddedServiceTouch(_)
            | FlagKind::Unknown { .. }
            | FlagKind::UnbridgedSecret { .. } => FlagSeverity::Review,
            FlagKind::ToolkitAction { .. } | FlagKind::UnresolvedExpression => FlagSeverity::Info,
        }
    }

    /// The "here's the native stanza" guidance surfaced alongside the flag — the
    /// lossy-with-warnings payload W224 calls for.
    pub fn stanza_hint(&self) -> String {
        match self {
            FlagKind::ReplaceWithNative(nr) => nr.stanza_hint().to_string(),
            FlagKind::EmbeddedServiceTouch(touches) => {
                let names: Vec<&str> = touches.iter().map(|t| t.label()).collect();
                format!(
                    "Run step reaches GitHub-the-service ({}); it runs on the executor but the \
                     call won't resolve on QED — replace with a native facility \
                     (content-addressed artifacts / a W208 publisher).",
                    names.join(", ")
                )
            }
            FlagKind::ToolkitAction { slug, .. } => format!(
                "Tier-1/2 toolkit action `{slug}` — runs via the toolkit-contract executor \
                 (R533-T7); no native subprocess emitted yet."
            ),
            FlagKind::Unknown { slug } => format!(
                "Unrecognized action `{slug}` — map it by hand or extend the tier catalog \
                 (qed-gha `classify_uses`); not run silently."
            ),
            FlagKind::UnresolvedExpression => {
                "Step carries GHA `${{ … }}` expressions (in the script or its `env:`) that \
                 QED's subprocess substitution won't expand; convert to QED params \
                 (`{{key}}`) / native outputs, or lift the build target at import time \
                 (R533-F9)."
                    .to_string()
            }
            FlagKind::UnbridgedSecret { names } => format!(
                "References secret(s) {} which lower to the LITERAL text `${{{{ secrets.NAME }}}}`, \
                 not to a value — a non-empty string that defeats an `if [ -z \"$TOKEN\" ]` guard \
                 and is then sent as a live-looking credential. Bridge each name in \
                 `~/.yah/qed/secrets.toml` (`NAME = \"vault:<slot>\"`) before running this step.",
                names.join(", ")
            ),
        }
    }
}

/// Transform a parsed workflow into native QED steps + assisted flags.
///
/// Jobs are emitted in [`topo_sort`] order so `needs:` predecessors precede
/// their dependents in the file, and the job graph is carried onto the steps as
/// [`QedStep::needs`](crate::types::QedStep::needs) edges (R605-F3) so the
/// runner schedules independent branches concurrently rather than in that
/// line. An unresolvable graph (cycle / unknown `needs`) falls back to
/// declaration order with the edges dropped, rather than failing the import —
/// the operator still gets the per-step transform to work from, and a cycle
/// that reached QED's own `needs` would only fail the load later, further from
/// the workflow that caused it.
pub fn transform_workflow(wf: &Workflow) -> TransformReport {
    let label = wf
        .name
        .clone()
        .unwrap_or_else(|| "imported workflow".to_string());
    let name = slugify(&label, "imported-workflow");

    // Topo-linearize the job DAG; on an unresolvable graph, keep declaration
    // order so the import still produces something to edit.
    let sorted = topo_sort(wf).ok();
    let order: Vec<String> = match &sorted {
        Some(waves) => waves.iter().flatten().cloned().collect(),
        None => wf.jobs.keys().cloned().collect(),
    };

    let mut steps: Vec<TransformedStep> = Vec::new();
    // Where each job's emitted native steps sit in `steps`, so the edge pass
    // below can name a job's first and last one.
    let mut spans: Vec<(String, Vec<usize>)> = Vec::new();
    for job_id in &order {
        let Some(job) = wf.jobs.get(job_id) else { continue };
        let mut native_at: Vec<usize> = Vec::new();
        for (step_index, step) in job.steps.iter().enumerate() {
            let t = transform_step(job_id, job, step_index, step);
            if t.native.is_some() {
                native_at.push(steps.len());
            }
            steps.push(t);
        }
        spans.push((job_id.clone(), native_at));
    }

    // Two steps with the same emitted name would make a `needs` edge to that
    // name ambiguous (and `${{ steps.X.outputs }}` / `background_until` too), so
    // disambiguate before anything references them.
    dedupe_native_names(&mut steps);
    if sorted.is_some() {
        apply_job_edges(wf, &mut steps, &spans);
    }

    TransformReport { name, label, steps }
}

/// Suffix repeated native step names with ` (2)`, ` (3)`, … .
///
/// GHA tolerates two steps in one job sharing a `name:`; QED's step namespace
/// does not — the emitted name is the key for a `needs` edge, for
/// `${{ steps.<name>.outputs.* }}`, and for `background_until`. Emitting a
/// duplicate would make the ejected pipeline fail its own load validation
/// (`DagError::AmbiguousName`) the moment anything referenced it, which is a
/// worse outcome than a slightly-renamed step.
fn dedupe_native_names(steps: &mut [TransformedStep]) {
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for t in steps.iter_mut() {
        let Some(native) = t.native.as_mut() else { continue };
        let count = seen.entry(native.name.clone()).or_insert(0);
        *count += 1;
        if *count > 1 {
            native.name = format!("{} ({})", native.name, count);
        }
    }
}

/// Carry the workflow's job edges onto the emitted steps (R605-F3).
///
/// One edge per job boundary: the first native step of a job `needs` the last
/// native step of each predecessor job. A predecessor that emitted nothing (all
/// its steps were tier-3, so all flagged) is transparent — its own
/// predecessors' tails are inherited through it, so a job whose only content
/// was `actions/checkout` doesn't sever the branch it sits on.
///
/// Roots get `needs = []`, which is *not* the same as leaving the key off:
/// absent means "chain to the previous step in the file", and the previous step
/// in the file belongs to some unrelated job that merely sorted earlier. Saying
/// nothing here is precisely the flattening this pass exists to undo.
fn apply_job_edges(wf: &Workflow, steps: &mut [TransformedStep], spans: &[(String, Vec<usize>)]) {
    // job id → the step names a dependent should wait on. A job with native
    // steps answers with its last one; an empty job passes the question up.
    let mut tails: IndexMap<&str, Vec<String>> = IndexMap::new();
    for (job_id, native_at) in spans {
        let tail = match native_at.last() {
            Some(&last) => vec![steps[last]
                .native
                .as_ref()
                .expect("index recorded only for native steps")
                .name
                .clone()],
            None => predecessor_tails(wf, job_id, &tails),
        };
        tails.insert(job_id.as_str(), tail);
    }

    for (job_id, native_at) in spans {
        let Some(&first) = native_at.first() else { continue };
        let deps = predecessor_tails(wf, job_id, &tails);
        steps[first]
            .native
            .as_mut()
            .expect("index recorded only for native steps")
            .needs = Some(deps);
    }
}

/// The step names a job must wait on: the resolved tails of every job in its
/// `needs:`, deduped, in declaration order. Empty for a root.
fn predecessor_tails(wf: &Workflow, job_id: &str, tails: &IndexMap<&str, Vec<String>>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let Some(job) = wf.jobs.get(job_id) else {
        return out;
    };
    for need in &job.needs {
        for name in tails.get(need.as_str()).into_iter().flatten() {
            if !out.contains(name) {
                out.push(name.clone());
            }
        }
    }
    out
}

/// Convenience: parse raw workflow YAML and transform it in one call. Still
/// pure (no file I/O) — the caller supplies the bytes. Used by `eject`
/// (R533-F6) and the tests here.
pub fn transform_workflow_src(src: &str) -> Result<TransformReport, yah_qed_gha::ParseError> {
    Ok(transform_workflow(&yah_qed_gha::parse_workflow(src)?))
}

/// Transform a single classified step.
fn transform_step(job_id: &str, job: &Job, step_index: usize, step: &Step) -> TransformedStep {
    let step_name = step.name.as_ref().map(render_exprstring).map(|s| s.trim().to_string());
    let class = classify_step(step);
    let mut native = None;
    let mut flags = Vec::new();

    match (&step.action, &class.disposition) {
        // Tier-1/2 `run:` compute → mechanical native subprocess. Embedded
        // service touches / surviving expressions ride along as flags.
        (StepAction::Run { body, shell }, Disposition::Compute) => {
            let (step_native, lifted_key) =
                map_run_step(job_id, job, step_index, step, body, shell.as_deref());
            native = Some(step_native);
            if !class.service_touches.is_empty() {
                flags.push(FlagKind::EmbeddedServiceTouch(class.service_touches.clone()));
            }
            // A `${{ matrix.<key> }}` reference that R533-F9 lifted (target
            // dimension carried as a step matrix) resolves natively, so it is
            // *not* an unresolved expression; any other `${{ … }}` still is.
            let key = lifted_key.as_deref();
            if has_unresolved_expression(body, key)
                || step.env.values().any(|v| has_unresolved_expression(v, key))
            {
                flags.push(FlagKind::UnresolvedExpression);
            }
            // Secrets are a subset of the unresolved expressions above, and are
            // reported *in addition to* — not instead of — the general flag:
            // the two want different fixes (params vs. the secrets bridge) and
            // a step can need both.
            let secrets = referenced_secrets(step, body);
            if !secrets.is_empty() {
                flags.push(FlagKind::UnbridgedSecret { names: secrets });
            }
        }
        // Tier-1/2 `uses:` toolkit action → flagged for the T7 executor.
        (StepAction::Uses { slug, git_ref, .. }, Disposition::Compute) => {
            flags.push(FlagKind::ToolkitAction { slug: slug.clone(), git_ref: git_ref.clone() });
        }
        // Tier-3 → replace with the named native facility.
        (_, Disposition::ReplaceWithNative(nr)) => {
            flags.push(FlagKind::ReplaceWithNative(*nr));
        }
        // Unrecognized `uses:` → surface for review.
        (StepAction::Uses { slug, .. }, Disposition::Unknown) => {
            flags.push(FlagKind::Unknown { slug: slug.clone() });
        }
        // A `run:` step is always Compute in the classifier, so this is
        // unreachable in practice; map it natively rather than dropping it.
        (StepAction::Run { body, shell }, Disposition::Unknown) => {
            native = Some(map_run_step(job_id, job, step_index, step, body, shell.as_deref()).0);
        }
    }

    TransformedStep { job: job_id.to_string(), step_index, step_name, native, flags }
}

/// Map a tier-1/2 `run:` step to a native [`StepKind::Subprocess`] step,
/// lifting any build target (R533-F9) into the structured `platform` field.
///
/// Returns the step plus the matrix key whose target dimension was lifted (so
/// the caller can suppress the unresolved-expression flag for that resolved
/// reference). `None` when no matrix-driven target was carried.
fn map_run_step(
    job_id: &str,
    job: &Job,
    step_index: usize,
    step: &Step,
    body: &ExprString,
    shell: Option<&str>,
) -> (QedStep, Option<String>) {
    let name = match step.name.as_ref().map(render_exprstring) {
        Some(n) if !n.trim().is_empty() => format!("{job_id}: {}", n.trim()),
        _ => format!("{job_id}: step {step_index}"),
    };
    let script = render_exprstring(body);
    let argv = shell_argv(shell, &script);
    let env = step
        .env
        .iter()
        .map(|(k, v)| (k.clone(), render_exprstring(v)))
        .collect();
    let cwd = step
        .working_directory
        .as_ref()
        .map(render_exprstring)
        .filter(|s| !s.is_empty());
    let timeout = step.timeout_minutes.map(|m| u64::from(m) * 60);
    let on_fail = if step.continue_on_error == Some(true) {
        OnFail::Continue
    } else {
        OnFail::Abort
    };
    let if_cond = step.if_cond.as_ref().map(render_exprstring);

    // R533-F9: lift the build target out of `--target <triple>` into the
    // structured platform field, so F3's native resolve() reasons about it
    // instead of a runtime bash scrape.
    let (platform, matrix, lifted_key) = lift_target(job, &script);

    let step = QedStep {
        background: false,
        background_until: None,
        wait_for: None,
        manual: None,
        argv,
        cwd,
        env,
        timeout,
        on_fail,
        if_cond,
        platform,
        matrix,
        ..base_step(name)
    };
    (step, lifted_key)
}

/// Lift a `--target <triple>` token from a step's script into a [`PlatformSpec`]
/// (R533-F9). When the target is a `${{ matrix.<key> }}` reference whose job
/// matrix dimension holds concrete triples, the dimension is carried as a
/// step-level [`MatrixSpec`] so QED fans the step out, one native build per
/// target — and the `platform.target` reference concretizes per row.
///
/// Returns `(platform, step_matrix, lifted_matrix_key)`; all `None` when the
/// step declares no `--target`.
fn lift_target(job: &Job, script: &str) -> (Option<PlatformSpec>, Option<MatrixSpec>, Option<String>) {
    let Some(raw_target) = extract_target(script) else {
        return (None, None, None);
    };
    let platform = Some(PlatformSpec {
        target: Some(raw_target.clone()),
        container_platform: None,
        native: false,
    });

    // A concrete triple needs no matrix; a matrix reference whose dimension we
    // can resolve carries the target values so QED expands them natively.
    if let Some(key) = matrix_ref_key(&raw_target) {
        let values = matrix_target_values(job, &key);
        if !values.is_empty() {
            return (platform, Some(target_matrix(&key, &values)), Some(key));
        }
    }
    (platform, None, None)
}

/// Extract the value of the first `--target <X>` / `--target=<X>` flag in a
/// script. `X` is either a concrete triple or a `${{ matrix.<key> }}` reference
/// (returned with normalized spacing). Returns `None` when absent — and is
/// careful not to mistake `--target-dir` for `--target`.
fn extract_target(script: &str) -> Option<String> {
    const FLAG: &str = "--target";
    let mut from = 0;
    while let Some(rel) = script[from..].find(FLAG) {
        let pos = from + rel;
        let after = &script[pos + FLAG.len()..];
        from = pos + FLAG.len();
        let mut chars = after.chars();
        match chars.next() {
            // `--target=<value>`
            Some('=') => {
                if let Some(v) = read_target_value(&after[1..]) {
                    return Some(v);
                }
            }
            // `--target <value>`
            Some(c) if c.is_whitespace() => {
                if let Some(v) = read_target_value(after) {
                    return Some(v);
                }
            }
            // `--target-dir`, `--targets`, … — not the flag we want.
            _ => {}
        }
    }
    None
}

/// Read a target value at the start of `s` (already past `--target`/`=`): a
/// `${{ … }}` block (preserved with normalized spacing) or a non-whitespace run.
fn read_target_value(s: &str) -> Option<String> {
    let s = s.trim_start();
    if let Some(rest) = s.strip_prefix("${{") {
        let end = rest.find("}}")?;
        return Some(format!("${{{{ {} }}}}", rest[..end].trim()));
    }
    let val: String = s.chars().take_while(|c| !c.is_whitespace()).collect();
    (!val.is_empty()).then_some(val)
}

/// The matrix dimension key of a bare `${{ matrix.<key> }}` reference, or `None`
/// for a concrete value or a more complex expression.
fn matrix_ref_key(value: &str) -> Option<String> {
    let inner = value.trim().strip_prefix("${{")?.strip_suffix("}}")?.trim();
    let key = inner.strip_prefix("matrix.")?.trim();
    (!key.is_empty() && !key.contains(char::is_whitespace)).then(|| key.to_string())
}

/// Concrete triple values for a job's `strategy.matrix.<key>` dimension —
/// gathered from both the dimension list and any `include:` rows that carry the
/// key (release-shaped include-only matrices put the target on include rows).
fn matrix_target_values(job: &Job, key: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |s: String| {
        if !out.contains(&s) {
            out.push(s);
        }
    };
    if let Some(matrix) = job.strategy.as_ref().and_then(|s| s.matrix.as_ref()) {
        if let Some(values) = matrix.dimensions.get(key) {
            for v in values {
                if let Some(s) = v.as_str() {
                    push(s.to_string());
                }
            }
        }
        for inc in &matrix.include {
            if let Some(s) = inc.get(key).and_then(|v| v.as_str()) {
                push(s.to_string());
            }
        }
    }
    out
}

/// A single-dimension step matrix over the lifted target triples.
fn target_matrix(key: &str, values: &[String]) -> MatrixSpec {
    let mut dimensions: IndexMap<String, Vec<toml::Value>> = IndexMap::new();
    dimensions.insert(
        key.to_string(),
        values.iter().map(|s| toml::Value::String(s.clone())).collect(),
    );
    MatrixSpec { dimensions, include: Vec::new(), exclude: Vec::new() }
}

/// A `QedStep` with every non-`Subprocess` field at its default — the spine
/// `map_run_step` overlays argv/env/etc. onto.
///
/// This used to spell all 30-odd fields out, with a comment saying `QedStep` has
/// no `Default`. It has one (types.rs, R633) and that impl is *stricter* than a
/// hand-written literal: it round-trips serde's own defaults, so it cannot drift
/// from what a TOML file with only `name =` deserializes to. Enumerating the
/// fields here bought nothing and cost an edit on every new `QedStep` field —
/// including the two that led to this comment being disproved (R717-T1/T2).
fn base_step(name: String) -> QedStep {
    QedStep {
        name,
        ..Default::default()
    }
}

/// Wrap a rendered script body in its shell's argv. GHA's default `bash`/`sh`
/// run with fail-fast (`set -eo pipefail` / `set -e`); preserve that so an
/// imported step fails on the same line it would on GitHub rather than silently
/// swallowing a mid-script error.
fn shell_argv(shell: Option<&str>, script: &str) -> Vec<String> {
    let argv = |prog: &str, flag: &str, body: String| {
        vec![prog.to_string(), flag.to_string(), body]
    };
    match shell.unwrap_or("bash") {
        "bash" => argv("bash", "-c", format!("set -eo pipefail\n{script}")),
        "sh" => argv("sh", "-c", format!("set -e\n{script}")),
        "pwsh" | "powershell" => argv("pwsh", "-Command", script.to_string()),
        "python" | "python3" => argv("python3", "-c", script.to_string()),
        other => argv(other, "-c", script.to_string()),
    }
}

/// Render an [`ExprString`] back to text, reconstructing `${{ … }}` around each
/// expression token. Literal segments pass through verbatim.
pub(crate) fn render_exprstring(s: &ExprString) -> String {
    let mut out = String::new();
    for t in &s.tokens {
        match t {
            ExprToken::Literal(x) => out.push_str(x),
            ExprToken::Expr(x) => {
                out.push_str("${{ ");
                out.push_str(x);
                out.push_str(" }}");
            }
        }
    }
    out
}

/// True when the string carries a `${{ … }}` expression QED won't expand. A
/// `${{ matrix.<key> }}` reference to `resolved_matrix_key` (the target
/// dimension R533-F9 carried as a step matrix) *does* resolve natively, so it
/// is not counted; every other expression — `github.*`, an unlifted matrix key
/// — is unresolved.
fn has_unresolved_expression(s: &ExprString, resolved_matrix_key: Option<&str>) -> bool {
    s.tokens.iter().any(|t| match t {
        ExprToken::Literal(_) => false,
        ExprToken::Expr(raw) => match (raw.trim().strip_prefix("matrix."), resolved_matrix_key) {
            (Some(k), Some(rk)) => k.trim() != rk,
            _ => true,
        },
    })
}

/// Every distinct `secrets.NAME` referenced by a step's script or its `env:`,
/// sorted and deduped.
///
/// Scans both surfaces because the `env:` case is the common and the more
/// dangerous one — `env: { TOKEN: "${{ secrets.X }}" }` is how nearly every
/// workflow hands a credential to a `run:` block, and it is precisely the shape
/// that lowers to a plausible-looking non-empty literal.
fn referenced_secrets(step: &Step, body: &ExprString) -> Vec<String> {
    let mut names: Vec<String> = std::iter::once(body)
        .chain(step.env.values())
        .flat_map(|s| s.tokens.iter())
        .filter_map(|t| match t {
            ExprToken::Expr(raw) => raw.trim().strip_prefix("secrets."),
            ExprToken::Literal(_) => None,
        })
        // `secrets.GITHUB_TOKEN` is tier-3 by nature and already reported by the
        // service-touch classifier; listing it here as "bridge this" would point
        // the human at a fix that cannot work — QED issues no GitHub tokens.
        .map(|n| n.trim().to_string())
        .filter(|n| !n.is_empty() && n != "GITHUB_TOKEN")
        .collect();
    names.sort();
    names.dedup();
    names
}

/// Sanitize a workflow name into a pipeline-name slug: lowercase, non-alnum runs
/// collapsed to a single `-`, trimmed. Empty → `fallback`.
fn slugify(name: &str, fallback: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_dash = false;
        } else if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }
    let slug = out.trim_matches('-').to_string();
    if slug.is_empty() {
        fallback.to_string()
    } else {
        slug
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse + transform an inline workflow.
    fn xf(src: &str) -> TransformReport {
        transform_workflow_src(src).expect("parse")
    }

    /// The native step emitted for `job`'s step at `idx`.
    fn native_at<'a>(r: &'a TransformReport, job: &str, idx: usize) -> &'a QedStep {
        r.steps
            .iter()
            .find(|s| s.job == job && s.step_index == idx)
            .and_then(|s| s.native.as_ref())
            .unwrap_or_else(|| panic!("no native step at {job}[{idx}]"))
    }

    /// The flags on `job`'s step at `idx`.
    fn flags_at<'a>(r: &'a TransformReport, job: &str, idx: usize) -> &'a [FlagKind] {
        &r.steps
            .iter()
            .find(|s| s.job == job && s.step_index == idx)
            .unwrap_or_else(|| panic!("no step at {job}[{idx}]"))
            .flags
    }

    const RUN_JOB: &str = r#"
name: ci
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Compile
        run: cargo build --release
        env:
          RUSTFLAGS: "-D warnings"
        working-directory: app
        timeout-minutes: 20
        continue-on-error: true
"#;

    #[test]
    fn run_step_maps_to_native_subprocess() {
        let r = xf(RUN_JOB);
        let step = native_at(&r, "build", 0);
        assert_eq!(step.name, "build: Compile");
        assert_eq!(step.kind, crate::types::StepKind::Subprocess);
        assert_eq!(step.argv[0], "bash");
        assert_eq!(step.argv[1], "-c");
        assert!(step.argv[2].starts_with("set -eo pipefail\n"));
        assert!(step.argv[2].contains("cargo build --release"));
        assert_eq!(step.env.get("RUSTFLAGS").map(String::as_str), Some("-D warnings"));
        assert_eq!(step.cwd.as_deref(), Some("app"));
        assert_eq!(step.timeout, Some(20 * 60));
        assert!(matches!(step.on_fail, OnFail::Continue));
        assert!(flags_at(&r, "build", 0).is_empty(), "clean compute → no flags");
    }

    #[test]
    fn pipeline_name_is_slugified_from_workflow_name() {
        let r = xf("name: My Release Flow!\non: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: true\n");
        assert_eq!(r.name, "my-release-flow");
        assert_eq!(r.label, "My Release Flow!");
    }

    #[test]
    fn unnamed_workflow_falls_back() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: true\n");
        assert_eq!(r.name, "imported-workflow");
    }

    #[test]
    fn tier3_checkout_is_flagged_not_mapped() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/checkout@v4\n");
        let step = &r.steps[0];
        assert!(step.native.is_none(), "tier-3 emits no native step");
        assert_eq!(
            step.flags,
            vec![FlagKind::ReplaceWithNative(NativeReplacement::Checkout)]
        );
        assert_eq!(step.flags[0].severity(), FlagSeverity::Replace);
        assert!(step.flags[0].stanza_hint().contains("checkout is implicit"));
    }

    #[test]
    fn tier3_upload_artifact_proposes_content_addressed_output() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/upload-artifact@v4\n");
        assert_eq!(
            r.steps[0].flags,
            vec![FlagKind::ReplaceWithNative(NativeReplacement::UploadArtifact)]
        );
        assert!(r.steps[0].flags[0].stanza_hint().contains("content-addressed output"));
    }

    #[test]
    fn compute_uses_is_a_toolkit_action_flag() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/setup-node@v4\n");
        let step = &r.steps[0];
        assert!(step.native.is_none(), "no subprocess until the T7 executor");
        assert_eq!(
            step.flags,
            vec![FlagKind::ToolkitAction {
                slug: "actions/setup-node".into(),
                git_ref: Some("v4".into()),
            }]
        );
        assert_eq!(step.flags[0].severity(), FlagSeverity::Info);
    }

    #[test]
    fn unknown_uses_is_flagged_for_review() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: some-org/exotic@v1\n");
        let step = &r.steps[0];
        assert!(step.native.is_none());
        assert_eq!(step.flags, vec![FlagKind::Unknown { slug: "some-org/exotic".into() }]);
        assert_eq!(step.flags[0].severity(), FlagSeverity::Review);
    }

    #[test]
    fn run_step_with_gh_cli_runs_but_is_flagged() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: gh release create v1 ./dist/*\n");
        let step = &r.steps[0];
        assert!(step.native.is_some(), "still runs on the executor");
        assert_eq!(
            step.flags,
            vec![FlagKind::EmbeddedServiceTouch(vec![ServiceTouch::GhCli])]
        );
        assert_eq!(step.flags[0].severity(), FlagSeverity::Review);
    }

    #[test]
    fn run_step_with_expression_is_flagged_unresolved() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: echo ${{ github.sha }}\n");
        let step = &r.steps[0];
        assert!(step.native.is_some());
        // The rendered script preserves the GHA expression verbatim.
        assert!(step.native.as_ref().unwrap().argv[2].contains("${{ github.sha }}"));
        assert!(step.flags.contains(&FlagKind::UnresolvedExpression));
    }

    #[test]
    fn jobs_flatten_in_topological_order() {
        let src = r#"
on: push
jobs:
  publish:
    needs: build
    runs-on: x
    steps:
      - run: echo publish
  build:
    runs-on: x
    steps:
      - run: echo build
"#;
        let r = xf(src);
        // `build` (no needs) must precede `publish` (needs: build) even though
        // it is declared second.
        let jobs: Vec<&str> = r.steps.iter().map(|s| s.job.as_str()).collect();
        assert_eq!(jobs, vec!["build", "publish"]);
    }

    #[test]
    fn shell_variants_select_the_right_interpreter() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: print(1)\n        shell: python\n");
        let step = native_at(&r, "a", 0);
        assert_eq!(step.argv[0], "python3");
        assert_eq!(step.argv[1], "-c");
        assert_eq!(step.argv[2], "print(1)");
    }

    #[test]
    fn report_accessors_partition_native_and_flagged() {
        let src = r#"
on: push
jobs:
  a:
    runs-on: x
    steps:
      - run: cargo test
      - uses: actions/checkout@v4
"#;
        let r = xf(src);
        assert_eq!(r.collect_native().len(), 1, "only the run step is native");
        assert_eq!(r.flags().count(), 1, "only checkout flags");
        assert!(!r.is_clean(), "a tier-3 step is present");
    }

    #[test]
    fn unnamed_run_step_gets_positional_name() {
        let r = xf("on: push\njobs:\n  b:\n    runs-on: x\n    steps:\n      - run: make\n");
        assert_eq!(native_at(&r, "b", 0).name, "b: step 0");
    }

    // ── R533-F9: import-time target lifting ───────────────────────────────

    #[test]
    fn concrete_target_lifts_into_platform_no_matrix() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: cargo build --target x86_64-unknown-linux-musl --release\n");
        let step = native_at(&r, "a", 0);
        let p = step.platform.as_ref().expect("platform lifted");
        assert_eq!(p.target.as_deref(), Some("x86_64-unknown-linux-musl"));
        assert!(step.matrix.is_none(), "a concrete target needs no matrix");
    }

    #[test]
    fn target_equals_form_is_recognized() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: cross build --target=aarch64-unknown-linux-gnu\n");
        let p = native_at(&r, "a", 0).platform.as_ref().expect("platform");
        assert_eq!(p.target.as_deref(), Some("aarch64-unknown-linux-gnu"));
    }

    #[test]
    fn target_dir_is_not_mistaken_for_target() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: cargo build --target-dir /tmp/out\n");
        assert!(native_at(&r, "a", 0).platform.is_none(), "--target-dir is not --target");
    }

    #[test]
    fn step_without_target_has_no_platform() {
        let r = xf(RUN_JOB);
        assert!(native_at(&r, "build", 0).platform.is_none());
    }

    #[test]
    fn matrix_target_dimension_lifts_and_carries_step_matrix() {
        let src = r#"
on: push
jobs:
  build:
    runs-on: x
    strategy:
      matrix:
        target:
          - x86_64-unknown-linux-musl
          - aarch64-unknown-linux-musl
    steps:
      - run: cross build --target ${{ matrix.target }}
"#;
        let r = xf(src);
        let step = native_at(&r, "build", 0);
        // platform.target holds the (QED-native) matrix reference …
        assert_eq!(
            step.platform.as_ref().unwrap().target.as_deref(),
            Some("${{ matrix.target }}")
        );
        // … and the target dimension rides along as a step matrix so QED fans it.
        let m = step.matrix.as_ref().expect("step matrix carried");
        let vals = m.dimensions.get("target").expect("target dimension");
        assert_eq!(vals.len(), 2);
        // The matrix.target reference is *resolved* natively → no unresolved flag.
        assert!(flags_at(&r, "build", 0).is_empty());
    }

    #[test]
    fn matrix_target_from_include_rows() {
        // Release-shaped include-only matrix: targets live on include rows.
        let src = r#"
on: push
jobs:
  cli:
    runs-on: x
    strategy:
      matrix:
        include:
          - target: x86_64-apple-darwin
          - target: aarch64-apple-darwin
    steps:
      - run: cargo build --target ${{ matrix.target }}
"#;
        let r = xf(src);
        let m = native_at(&r, "cli", 0).matrix.as_ref().expect("matrix from include rows");
        let vals = m.dimensions.get("target").expect("target dimension");
        assert_eq!(vals.len(), 2);
    }

    // (Native step-matrix expansion concretizing platform.target end-to-end is
    // covered by matrix::tests::step_matrix_substitutes_lifted_platform_target.)

    // ── R605-F3: the job DAG survives the import ──────────────────────────

    /// The emitted `needs` on the named step, or `None` when it declares none.
    fn needs_of<'a>(r: &'a TransformReport, step_name: &str) -> Option<&'a Vec<String>> {
        r.native_steps()
            .find(|s| s.name == step_name)
            .unwrap_or_else(|| panic!("no native step named `{step_name}`"))
            .needs
            .as_ref()
    }

    /// Two independent jobs joining into a third. Before R605-F3 this ejected
    /// as a flat six-step line; now the join's first step names both branch
    /// tails and the branch heads are roots, so the runner fans them out.
    #[test]
    fn a_job_diamond_becomes_a_step_dag() {
        let src = r#"
on: push
jobs:
  setup:
    runs-on: x
    steps:
      - name: prep
        run: echo prep
  left:
    needs: setup
    runs-on: x
    steps:
      - name: one
        run: echo l1
      - name: two
        run: echo l2
  right:
    needs: setup
    runs-on: x
    steps:
      - name: one
        run: echo r1
  join:
    needs: [left, right]
    runs-on: x
    steps:
      - name: fin
        run: echo done
"#;
        let r = xf(src);
        // The root job's head depends on nothing — and says so, rather than
        // leaving the key off (which would mean "chain to the previous step").
        assert_eq!(needs_of(&r, "setup: prep"), Some(&vec![]));
        // Both branch heads hang off the root's tail.
        assert_eq!(needs_of(&r, "left: one"), Some(&vec!["setup: prep".to_string()]));
        assert_eq!(needs_of(&r, "right: one"), Some(&vec!["setup: prep".to_string()]));
        // A step that is not its job's head says nothing — the implicit chain
        // edge to the step before it, which is its own job's previous step.
        assert_eq!(needs_of(&r, "left: two"), None);
        // The join waits on the LAST step of each branch.
        assert_eq!(
            needs_of(&r, "join: fin"),
            Some(&vec!["left: two".to_string(), "right: one".to_string()])
        );
    }

    /// A predecessor job that emits nothing (every step tier-3) must not sever
    /// the branch: its dependents inherit its own predecessors' tails.
    #[test]
    fn a_job_with_no_native_steps_is_transparent_in_the_graph() {
        let src = r#"
on: push
jobs:
  build:
    runs-on: x
    steps:
      - name: compile
        run: cargo build
  stage:
    needs: build
    runs-on: x
    steps:
      - uses: actions/upload-artifact@v4
  ship:
    needs: stage
    runs-on: x
    steps:
      - name: publish
        run: echo ship
"#;
        let r = xf(src);
        assert_eq!(
            needs_of(&r, "ship: publish"),
            Some(&vec!["build: compile".to_string()]),
            "the all-tier-3 `stage` job emits nothing, so `ship` hangs off `build`",
        );
    }

    /// The ejected step DAG has to be one QED itself accepts — same resolver
    /// the loader runs, over the transform's own output.
    #[test]
    fn the_emitted_graph_resolves_and_fans_out() {
        let src = r#"
on: push
jobs:
  a:
    runs-on: x
    steps:
      - name: s
        run: echo a
  b:
    runs-on: x
    steps:
      - name: s
        run: echo b
  c:
    needs: [a, b]
    runs-on: x
    steps:
      - name: s
        run: echo c
"#;
        let r = xf(src);
        let steps = r.collect_native();
        let waves = crate::dag::waves(&steps, crate::dag::Missing::Reject)
            .expect("the emitted graph resolves");
        assert_eq!(
            waves,
            vec![vec![0, 1], vec![2]],
            "two independent jobs share a wave; their dependent follows",
        );
    }

    /// Two steps sharing a `name:` are legal in GHA and ambiguous in QED — the
    /// emitted name is the key a `needs` edge resolves against. Disambiguated
    /// at emit, so the ejected file loads.
    #[test]
    fn duplicate_step_names_within_a_job_are_disambiguated() {
        let src = r#"
on: push
jobs:
  a:
    runs-on: x
    steps:
      - name: build
        run: echo one
      - name: build
        run: echo two
  b:
    needs: a
    runs-on: x
    steps:
      - name: ship
        run: echo three
"#;
        let r = xf(src);
        let names: Vec<&str> = r.native_steps().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["a: build", "a: build (2)", "b: ship"]);
        assert_eq!(needs_of(&r, "b: ship"), Some(&vec!["a: build (2)".to_string()]));
        assert!(crate::dag::waves(&r.collect_native(), crate::dag::Missing::Reject).is_ok());
    }

    /// An unresolvable job graph still imports — flat, with no edges — rather
    /// than failing. A cycle carried into QED's own `needs` would only fail the
    /// load later, further from the workflow that caused it.
    #[test]
    fn a_cyclic_job_graph_imports_flat_with_no_edges() {
        let src = r#"
on: push
jobs:
  a:
    needs: b
    runs-on: x
    steps:
      - name: s
        run: echo a
  b:
    needs: a
    runs-on: x
    steps:
      - name: s
        run: echo b
"#;
        let r = xf(src);
        assert!(
            r.native_steps().all(|s| s.needs.is_none()),
            "no synthesized edges off a graph that doesn't resolve",
        );
        assert!(crate::dag::waves(&r.collect_native(), crate::dag::Missing::Reject).is_ok());
    }

    /// Locate yah's live `release.yml` by ascending to the `.github/workflows`
    /// marker. Absent in the standalone export mirror → the fixture test skips.
    ///
    /// `oss/qed` (this crate's own exportable subtree) carries a decoy
    /// `.github/workflows/release.yml` of its own — a crates.io-publish
    /// workflow for the standalone mirror, single `publish` job — which sits
    /// *closer* to `CARGO_MANIFEST_DIR` than the monorepo's real CI workflow.
    /// An ancestor-walk that stops at the first match silently transforms the
    /// wrong file. `.yah/camp.toml` only exists at the true monorepo root
    /// (oss/* subtrees are deliberately un-anchored so they stay transparent
    /// to the camp's ticket index) — require it alongside the workflow file so
    /// the walk skips the decoy and keeps ascending to the real root.
    fn release_yml() -> Option<String> {
        let mut dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        loop {
            let cand = dir.join(".github/workflows/release.yml");
            if cand.is_file() && dir.join(".yah/camp.toml").is_file() {
                return std::fs::read_to_string(cand).ok();
            }
            if !dir.pop() {
                return None;
            }
        }
    }

    #[test]
    fn env_secret_is_flagged_for_review_not_buried_in_info() {
        // The smoke-sweeper shape: a credential handed to bash via `env:`, then
        // guarded with `-z`. Lowered literally, the guard passes and the step
        // calls a live API with `${{ secrets.X }}` as its bearer token.
        let r = xf(r#"
on: [push]
jobs:
  sweep:
    runs-on: ubuntu-latest
    steps:
      - name: Sweep
        env:
          HETZNER_API_TOKEN: ${{ secrets.HETZNER_API_TOKEN }}
        run: |
          if [ -z "$HETZNER_API_TOKEN" ]; then exit 0; fi
          curl -H "Authorization: Bearer $HETZNER_API_TOKEN" https://api.example.com
"#);
        let flag = r
            .flags()
            .map(|(_, f)| f)
            .find(|f| matches!(f, FlagKind::UnbridgedSecret { .. }))
            .expect("env secret is flagged");
        assert_eq!(
            flag,
            &FlagKind::UnbridgedSecret { names: vec!["HETZNER_API_TOKEN".into()] }
        );
        assert_eq!(
            flag.severity(),
            FlagSeverity::Review,
            "a credential that lowers to a plausible non-empty literal is not informational"
        );
        assert!(flag.stanza_hint().contains("secrets.toml"));
    }

    #[test]
    fn secrets_in_the_script_body_are_collected_deduped_and_sorted() {
        let r = xf(r#"
on: [push]
jobs:
  pub:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo "${{ secrets.R2_SECRET }}" > /dev/null
          echo "${{ secrets.CF_TOKEN }}"
          echo "${{ secrets.R2_SECRET }}"
"#);
        let flag = r
            .flags()
            .map(|(_, f)| f)
            .find(|f| matches!(f, FlagKind::UnbridgedSecret { .. }))
            .expect("script secrets are flagged");
        assert_eq!(
            flag,
            &FlagKind::UnbridgedSecret {
                names: vec!["CF_TOKEN".into(), "R2_SECRET".into()]
            }
        );
    }

    #[test]
    fn github_token_is_not_reported_as_bridgeable() {
        // QED issues no GitHub tokens, so "bridge this in secrets.toml" would be
        // advice that cannot be followed. The service-touch classifier already
        // owns this case.
        let r = xf(r#"
on: [push]
jobs:
  rel:
    runs-on: ubuntu-latest
    steps:
      - env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: gh release upload v1 ./dist/x
"#);
        assert!(
            !r.flags().any(|(_, f)| matches!(f, FlagKind::UnbridgedSecret { .. })),
            "GITHUB_TOKEN must not be offered as a bridgeable secret"
        );
    }

    #[test]
    fn a_step_with_no_secrets_raises_no_secret_flag() {
        let r = xf(r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - env:
          RUSTFLAGS: -D warnings
        run: cargo build --release
"#);
        assert!(!r.flags().any(|(_, f)| matches!(f, FlagKind::UnbridgedSecret { .. })));
    }

    #[test]
    fn release_yml_transforms_end_to_end() {
        let Some(src) = release_yml() else {
            eprintln!("skip: yah workflow fixtures not present");
            return;
        };
        let r = xf(&src);
        assert!(!r.steps.is_empty(), "release.yml has steps");

        // checkout → flagged tier-3, never a native step.
        let checkout = r
            .steps
            .iter()
            .find(|s| s.job == "smoke" && s.step_index == 0)
            .expect("smoke step 0");
        assert!(checkout.native.is_none());
        assert!(checkout
            .flags
            .contains(&FlagKind::ReplaceWithNative(NativeReplacement::Checkout)));

        // The image jobs' build-push → registry-publish replacement.
        assert!(r.flags().any(|(_, f)| matches!(
            f,
            FlagKind::ReplaceWithNative(NativeReplacement::RegistryPublish)
        )));

        // The cargo build `run:` steps map to native bash subprocesses.
        assert!(
            r.native_steps().any(|s| s.argv.first().map(String::as_str) == Some("bash")
                && s.argv.last().is_some_and(|c| c.contains("cargo"))),
            "at least one native cargo build step",
        );

        // Topo linearization: cli-build precedes smoke, smoke precedes publish.
        let first_idx = |job: &str| r.steps.iter().position(|s| s.job == job);
        let (build, smoke, publish) =
            (first_idx("cli-build"), first_idx("smoke"), first_idx("publish-cli"));
        if let (Some(b), Some(s), Some(p)) = (build, smoke, publish) {
            assert!(b < s, "cli-build before smoke");
            assert!(s < p, "smoke before publish-cli");
        }

        // R605-F3: the emitted step graph has to be one QED will actually
        // load — the whole corpus of real-workflow shapes in one assertion.
        // Its wave count must be *lower* than its step count, or the "job DAG"
        // claim is just a flat list with extra keys on it.
        let native = r.collect_native();
        let waves = crate::dag::waves(&native, crate::dag::Missing::Reject)
            .expect("release.yml's emitted graph resolves");
        assert!(
            waves.len() < native.len(),
            "release.yml has independent jobs, so the ported graph must have at \
             least one wave with more than one step in it ({} waves over {} steps)",
            waves.len(),
            native.len(),
        );
    }
}
