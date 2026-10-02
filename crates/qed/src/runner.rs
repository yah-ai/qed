//! @arch:see(.yah/docs/working/W063-area-a-ui-design-impl.md)
//! @arch:see(.yah/docs/working/W154-yubaba-dual-runtime.md)
//! @arch:see(.yah/docs/working/W154-yubaba-dual-runtime.md)
//! @arch:see(.yah/docs/working/W164-derived-static-assets.md)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @arch:see(app/yah/cli/src/qed.rs)
//! @arch:see(.yah/docs/working/W296-executable-docs-notebook-cells.md)
//! @arch:see(.yah/docs/working/W330-distributing-camp-compute.md)
//! @yah:ticket(R833-F9, "Artifact retrieval to the invoker: point produces / Outcome::Publish at the Phase 1 LAN store")
//! @yah:at(2026-08-29T20:53:54Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:phase(P4)
//! @yah:parent(R833)
//! @arch:see(.yah/docs/working/W330-distributing-camp-compute.md)
//! @yah:depends_on(R833-T2)
//! @yah:next("Artifacts are build outputs and DO NOT belong in git history -- that is the constraint this ticket exists to respect. ProducedArtifact / produces / Outcome::Publish already work, including object-store staging; the change is pointing that staging at the Phase 1 LAN store (R833-T2) so a remote run's outputs come back to the invoker over the mesh.")
//! @yah:next("Tier: Warrior -- the publish machinery exists and works; this is wiring it to a new destination plus the retrieval leg back to the invoker.")
//! @yah:gotcha("ONE STORE SERVES BOTH. W330 is explicit that the LAN store carries the compile cache AND the QED artifacts -- do not stand up a second store for artifacts. If R833-T2 has not landed yet, that is what this ticket is waiting on; it is the only cross-phase dependency in the relay.")
//! @yah:ticket(R833-T17, "Give isolated runs a stable worktree path so they at least share a cache with each other")
//! @yah:at(2026-08-30T03:01:58Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R833)
//! @yah:next("R833-S1 measured that a workspace='isolated' run gets 0 percent sccache reuse against the camp tree (1 hit / 727 misses on cargo build -p camp-identity), and that this is NOT configurable -- sccache 0.17.0 applies basedirs on the C/C++ path only and hashes cwd plus CARGO_MANIFEST_DIR into every Rust key. Sharing with the camp tree is therefore off the table.")
//! @yah:next("WHAT IS STILL AVAILABLE: sharing between isolated runs. prepare_workspace (runner.rs:1635) builds at std::env::temp_dir()/qed-worktree-{run_id}, so the path is fresh every run and consecutive release runs share nothing with each other either. A path keyed on something stable -- the pipeline name rather than the run id -- would let the second release run of the day reuse the first.")
//! @yah:next("THE REASON THIS IS A TICKET AND NOT A ONE-LINE EDIT: two concurrent isolated runs of the same pipeline would collide on one path, and R766 wants per-run worktrees RETAINED for resume-from-step. Settle those two before changing the path. A per-pipeline path plus a lock, or a small pool of numbered slots, are the shapes worth costing.")
//! @yah:verify("Two consecutive isolated runs of the same pipeline, no source change between them, and the second run's sccache hit rate is materially above zero.")
//! @yah:gotcha("Do not 'fix' this by moving the worktrees under /Users/leif/ss so a common base dir covers them and the camp tree. That was R833-S1's assigned hypothesis and it was measured false: a common ancestor cannot make two different sub-paths hash alike, and sccache strips nothing on the Rust path regardless. Measured with the common ancestor as a single basedir, and with both roots listed as basedirs -- 0 hits either way.")
//! @yah:relay(R906, "Agent-supervised QED runs: qed.await with tree-wide wake, manual-step audience, and the operator-gate position rule")
//! @yah:at(2026-09-14T17:49:53Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-glimmerstone)
//! @yah:next("Design was settled in conversation with the operator 2026-09-14; the full brief is in this relay's handoff entries. Read them before opening a child.")
//! @yah:handoff("THE PROBLEM. An agent can start a QED run (`yah qed run …` over Bash) but cannot supervise one. There is no `qed_*` MCP tool at all — the agent surface is board/camp/party/subagent/inbox/sql and nothing else — so an agent that launches a release wave must either block a shell for hours or poll, and polling costs a full-context API call per poll. The operator's ask: run a QED, park, and let the camp daemon wake the agent for failures, for a periodic liveness tick, and for gates the agent itself can answer. The daemon already holds everything needed: `QedEventWire` carries RunQueued/RunStarted/StepStarted/StepFinished/StepAwaitingHuman/SubPipelineStarted/SubPipelineFinished/RunFinished, there is a ring buffer plus append-only JSONL at `.yah/jit/qed/<run-id>.events.jsonl`, and `yah qed` already proxies to the camp daemon. This is a tool surface over an existing event rail, NOT a new daemon subsystem.")
//! @yah:handoff("THE FOUR WAKE REASONS, and the rule that generated them: a wake reason must map to a DISTINCT agent action, or it is not a wake reason. `finished` -> report, release the park. `errored` -> diagnose at the deepest failing step, patch the live tree, rerun. `blocked_on_operator` -> the run is alive and waiting on a human the agent may not impersonate; notify and re-park. `needs_agent` -> a manual gate the agent IS allowed to answer; do the work, satisfy `advance`, ack. `ticked` -> the periodic liveness return; confirm the tree moved, re-park. Everything else (step started, step succeeded, sub-pipeline entered) maps to NO action and is therefore NOT a wake — waking on green progress costs one full-context API call to learn nothing, and the release wizard has 19 steps. The predicate is declared at park time: `qed.await {run_id, wake_on: [...], max_wait_secs}`. Precedent for the enum shape is in-house: `subagent.wait_for_idle`'s `IdleReason` (assistant_final | user_required | released | budget_exhausted | timeout | terminated), which already treats `timeout` as a named reason rather than an error and already splits `user_required` out from the rest.")
//! @yah:handoff("TREE-WIDE IS A FLOOR, NOT A REFINEMENT — operator was explicit (\"no never do this\" to watching top-level only). Two consequences. (1) LIVENESS: the only timestamp that means anything is the deepest transition ANYWHERE in the run tree. `release-wizard -> release-check -> yah-check -> cargo-build` is three levels; an agent watching top-level steps sees the wave frozen for an hour while cargo-build runs normally. (2) PAYLOAD: `qed.await` must resolve the tree daemon-side and return the current route through the nested pipelines, plus — on `errored` — the deepest FAILING step with its output attached. It must never hand back a run_id to chase. Measured cost of the alternative, from this very session: finding out why the wizard failed took THREE full-context calls — `qed status <parent>` said \"child step check (run_id=f2afafef…)\", `qed status <that>` said \"child step cluster-epoch-drift-guard (run_id=1e71223d…)\", and only parsing that child's events.jsonl gave the actual assertion. Each level answers with a pointer instead of a fact. The daemon has every level in hand (`ChildEventFactory` / `with_child_event_factory` give each child its own channel — they exist precisely because children were once created with `events: None` and silently discarded everything they ran, which is how 0.8.22 through 0.8.26 all failed with no recoverable reason).")
//! @yah:handoff("THE 55m TICK IS A LIVENESS CHECK, NOT A CACHE TRICK — build it for the first reason or it will be built wrong. A park that only ever wakes on an event inherits the failure mode this repo has already paid for: silence from an event stream is indistinguishable from silence from a DEAD event stream (see the `events: None` history above). So the tick re-reads AUTHORITATIVE state and reconciles it against what the bus claimed, rather than pinging to stay warm. The cache refresh is a real but second-order side effect (a cache hit refreshes TTL, so a tick costs ~0.1N against ~1.25N for a lapsed prefix) and it is CONDITIONAL on the harness process surviving the park — unverified, worth measuring before anyone tunes the interval, and `api_calls` from `party.agent_status` is the field to measure with, not `fillFraction`/`ring_depth`, which both under-report on this runner. OPERATOR'S RULE, adopted: 55m with no transition anywhere in the tree is a hard concern — \"if we have a 55m step we've written a poor pipeline that needs finer milestones\". The `ticked` payload must therefore carry the deepest-transition TIMESTAMP, not just current state, so \"same as when I went to sleep\" is a one-glance comparison rather than a diff to eyeball. A tick whose deepest transition has not moved is the `ps aux | grep` trigger.")
//! @yah:handoff("MANUAL-STEP `audience`, DEFAULT \"agent\". New field on ManualConfig: `audience = \"operator\" | \"agent\"`, DEFAULTING TO AGENT — the inversion matters, because today's implicit default is operator and that is how the release wizard accumulated three of them. Classifying rule, from the operator: an agent may answer a gate that asks \"is this done / is this right\"; it may NEVER answer one that asks \"may I\". The second kind authorizes the irreversible work that follows, and a thing cannot authorize itself. This makes `kind = \"manual\"` mean something precise for the first time — a step whose body is JUDGEMENT rather than a command, gated by a predicate proving the judgement landed. Today the only difference between manual and mechanical is who is expected to type. `terminal` stops being \"commands to copy\" and becomes the starting kit. OPERATOR'S CONVENTION, adopted verbatim: \"an operator step beyond step 1 should be an error… 'operator step' means 'did you mean to run this QED, here are some facts'; beyond that it should be agent all the way down or the operator shouldn't have run it.\"")
//! @yah:handoff("THE POSITION RULE, AND WHY IT IS STRUCTURAL RATHER THAN TIMED. Enforce: at most ONE `audience = \"operator\"` step per pipeline; it must be the FIRST manual step; and NO STEP BEFORE IT MAY INVOKE A COMPILER. That last clause is the real content — it is checkable from argv alone, on a machine that has never run the pipeline, and it is the actual reason the operator's \"first 0-5 minutes\" number is right: every sub-5m step in this camp is I/O or a shell, and every step that blew past it was a build. DO NOT justify this rule with measured durations. QED HAS NO TIMINGS — no duration model, no per-step history API, no way to tell anyone a step is overdue. Every number in this relay was mined by hand with a python script over 802 JSONL files and is NOT available to a validator, an agent, or the runner. QED probably should not get a duration model either: the measured spread is p50 6 seconds / p99 46 minutes / p99.9 93 minutes, four orders of magnitude driven by cache state and camp contention rather than step identity, so per-step prediction would be noise with a confidence interval. A flat tree-transition timeout beats a learned one for the same reason a fuse beats a model of your wiring. The one place an author's estimate helps is the EXEMPTION: a step declaring `expect_slow` in the recipe (the only legitimate case found is `build-v8-musl`, measured 56-60m three times) — a declaration, reviewable in a diff, not a measurement.")
//! @yah:handoff("WARN AT AUTHORING, FAIL AT RUNTIME — the pairing is what makes either half credible. A warning that says \"this might be slow\" gets ignored; one that says \"this WILL fail, here is the flag if you meant it\" gets answered. Runtime: a `blocked_on_operator` past the ack FAILS the run rather than parking it, with an explicit per-pipeline opt-out (`allow_late_operator_block`). Authoring: warn, do not hard-error — a pipeline nobody intends to run unattended is legitimate, and a hard error would force the opt-out into every one of them as boilerplate, which is how an opt-out stops meaning anything. Keeping it a warning keeps the flag rare enough that seeing it in a diff is informative. The warning must NOT cite measured medians (see the no-timings entry); the honest form is that NOTHING IS KNOWN about how long the step takes, i.e. \"this parks on a human at an unknown depth into an unbounded run\".")
//! @yah:gotcha("THE EVIDENCE, mined 2026-09-14 by hand from 802 files in `.yah/jit/qed/*.events.jsonl` (pair step-started/step-finished by name per run). Reproduce it rather than trusting these numbers; nothing in QED serves them. 2,279 completed steps. ONLY 18 EVER EXCEEDED 55m, AND 6 OF THOSE ARE HUMAN PARKS: roll-the-fleet 543m/421m/278m, commit-and-tag 324m/208m, push-tag 55m. Mechanical steps only (n=2233): p50 6s, p90 12m, p99 46m, p99.9 93m — so 55m sits near p99.5 and fires on roughly one step in two hundred. THE TWO LONGEST MECHANICAL STEPS ARE BOTH FAILURES — mcp-sidecar failed after 8.9 HOURS, containers-and-linux after 2.2 — and nothing does 8.9 hours of work and then fails; those are hangs, and they are exactly what the tick catches. Separately, the 46 completed HUMAN parks are bimodal and the median (0.8m) is a lie: ~15 of them sit at 0.0m, which is not a fast answer but the gate NOT FIRING (R605-B17's auto-advance bug, timed at 17ms and 13ms in its own gotcha), while 6 sat 55m-543m genuinely unattended. So a late operator gate is a coin flip between abandoned-for-five-hours and silently-skipped. Neither is \"paused briefly\", and that is the empirical case for failing rather than parking.")
//! @yah:gotcha("SHARED-TREE STATE AT FILING (2026-09-14, @Glimmerstone:eclipse). Two files carry UNCOMMITTED R605-B20 work that this relay's children will edit — `.yah/qed/yah-release-wizard.toml` (the whole manual-gate retirement: authorize-release added, commit-and-tag/push-tag/roll-the-fleet made mechanical, sovereign param + options) and `xtask/tests/fleet_sovereign_groups.rs` (the new options-vs-declared-groups test, plus a camp_root() helper machines_dir() now derives from). Also uncommitted and UNRELATED to this relay: `oss/yubaba/crates/yubaba/cluster-epochs.json`, a cluster_protocol_surface re-record that the 0.8.39 release is waiting on. Verify by CONTENT before editing, not by git status — anchor SHA at filing is 0e4fda27 (= tag v0.8.39), but the freeze commit for 0.8.39 will move it. R906-T5 in particular rewrites steps this uncommitted work just created, so read the file rather than the last commit.")
//! @yah:gotcha("ANCHOR CORRECTION — quote bae81d65930fc91672d954b7cc25a9ab6ac1c890, not the 0e4fda27 named in the shared-tree gotcha above. 0e4fda27 was HEAD when this relay was filed minutes earlier; the tree moved before dispatch. bae81d65 is the dispatch anchor for @Ashguard:polaris (session:fe2a19de) and is the SHA to diff against: `git diff --stat bae81d65..HEAD`. Contention check at dispatch was clean — 5 dirty paths, none contended, no peer holding the three task paths (xtask/tests/fleet_sovereign_groups.rs, oss/qed/crates/qed/src/runner.rs, oss/yubaba/crates/yubaba/cluster-epochs.json).")
//! @yah:next("NEXT AGENT STARTS AT R906-F2. F1 is in review (qed.await + qed.resume shipped uncommitted; tree anchor at my start was bae81d65). F2 is now HALF DONE — the `audience` field, its wire plumbing and the schema regen landed under F1, so F2 is the validator only; its own gotcha names exactly what remains. Then B3, F4, T5 in ID order. Nothing is blocked on an operator. Handed off at ~250k context fill after 130 API calls, not because the work stalled.")
//! @yah:handoff("LEADER PASS 2026-09-14 (@Ashguard:eclipse, session:24101295): all five children at review. F2: `operator_gate_is_late` is one shared check used by both the load-time lint (`Pipeline::lint_operator_gates`, warn-only, on `load_and_validate_graph`) and three runtime checks in `execute_step_manual` (child run; resumed or name-subset run, now `with_step_selection` replacing the deleted `with_index_offset`; otherwise the check itself). `allow_late_operator_block` is the only escape. yah-qed 1000/0/1; my independent re-run matched. B3: `apply_params` now substitutes into manual prompt, terminal[], advance and checklist[]; 1001/0/1, confirmed by a second session. F4: `agent` button beside `run` starts the run, then hands `WireQedRun.id` + `.params` to a new `QED_SUPERVISE` job; the shared params helper also feeds `QED_TRIAGE`. packages/yah/ui full suite 2299/14 before, 2301/15 after, same failures both times (one flaky perf ratio); none are F4's. T5: `authorize-release` (the one operator-audience step) is at index 1 ahead of `version-bump`, a new `verify-authorized-version` assert follows it, and `roll-the-fleet` is manual with agent audience. A read-only check found nothing that compiles before the gate and no opt-out flag. After the stash restore: every_camp 1/0, r325_f1 43/0, yah-qed 1003/0/1. B6: per-server `timeout` of 3900000ms on mcpServers.yah (`agent_process.rs`), plus a `park_shortened` guard in `qed_tools.rs`. After the stash restore: desktop 11/0, qed_tools 8/0, workspace check exit 0.")
//! @yah:handoff("CAMP FINDINGS FROM THIS PASS, not R906 work and nothing filed: (a) `party.chat` returned `delivered: false` six times, and every one of those messages was later delivered and acted on. That caused one duplicate courier (B3). (b) Couriers that start a cargo build in the background and end their turn to wait get auto-released and are not reliably woken when it finishes; running the build in the foreground with a long Bash timeout fixed it. (c) No Thief-class character is bookable (all four availableForWork=false), so Thief-tier tickets run as Cleric. (d) Pre-existing packages/yah/ui failures that look like real bugs: `StatusPill` renders an empty body (Pill.test.tsx:14), and the onboarding wizard's `onboarding-step-claude-cli` step is gone (CampOnboardingWizard.test.tsx). (e) An unattributed `git stash` wiped the tree 13:09-13:14 and the operator restored it; T5's annotation in `runner.rs` briefly showed `open` because of it.")
//! @yah:handoff("RELAY DELIVERED, all five children at review, awaiting operator sign-off. Final checks: (1) An independent sniffer re-ran B6 (session:5f6fd91d): desktop write_claude_settings 11/0, yah-agent-tools qed_tools 8/0 (exit 0), cargo check --workspace --all-targets exit 0. It matches the courier; every symbol was found at its line, and no @yah:assumes about the harness cap remains in runner.rs. (2) Comment-only cleanup for T5 landed (session:87385a02): 10 stale comments in yah-release-wizard.toml (manual-step counts, publish-binaries index 13, prose step labels, a note at both version-awk sites that they read by position while xtask reads by key), plus oss/qed/crates/qed/src/types.rs:2071 and crates/yah/rpc/src/lib.rs:7620 (19 changed to 20 steps). cargo test -p yah --lib r325_f1 43/0. Left alone deliberately: app/yah/cli/src/camp.rs:12570 still says 19 steps, because @Ashguard:rune and @Miravel:polaris were live in that uncommitted file. Minor: runner.rs now carries the harness-cap measured facts twice, as @yah:gotcha at :529 and inside R906-B6's own gotcha at :544; both are accurate.")

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use observation::ForgeId as ObsForgeId;
use yah_scryer::service::Scryer;
use velveteen::{
    ForgeCommand, ForgeSpec, ForgeStatus, MeshAccess, TaskLocation, TaskPlacement, TaskRuntime,
};
use velveteen_exec::{
    ExecContext, ExecEvent, ForgeExecutor, ForgeExecutorError, LocalForgeDriver, RemoteForgeDriver,
    WardenClient,
};
use task_runs::Initiator;
use thiserror::Error;
use uuid::Uuid;
use workload_spec::{Millis, TierTag};

use tokio::sync::mpsc::UnboundedSender;

use crate::events::{OutputStream, QedEvent};
use crate::native::{LoggingSigner, SigstoreSigner};
use crate::types::{
    OnFail, Outcome, Pipeline, ProducedArtifact, QedRunId, QedRunMeta, QedStep, RunStatus,
    StepActivation, StepStatus, WorkspaceMode,
};

/// Substituted for a [`QedStep::secret`](crate::types::QedStep::secret) step's
/// failure detail everywhere a stderr tail would otherwise be minted (R717-T2).
///
/// A *replacement* rather than a `None`: a failed step whose card says nothing at
/// all reads as a qed bug, and the next person debugs the runner instead of the
/// step. This says which of the two it is, and why there is nothing more to read.
pub const SECRET_STEP_REDACTED: &str =
    "[redacted: step declares `secret = true`, so no stderr tail is captured]";

/// Dispatches pipeline outcomes (yubaba-deploy, almanac-run) after a pipeline completes.
///
/// Implementations are responsible for the actual side-effect. The default stub logs and
/// no-ops until the respective RPC surfaces stabilise (R040-F4 for yubaba deploy).
#[async_trait]
pub trait OutcomeDispatcher: Send + Sync {
    async fn yubaba_deploy(&self, service: &str, env: &str) -> Result<(), RunnerError>;
    async fn almanac_run(&self, pipeline: &str) -> Result<(), RunnerError>;
    /// Publish the artifacts produced by the run's successful steps into a
    /// release channel bucket, then fire the almanac revalidate hook (R330-F3).
    /// The default no-ops so existing impls don't break; the real behaviour
    /// lives in [`crate::publish::PublishingOutcomeDispatcher`].
    async fn publish(&self, req: &crate::publish::PublishRequest) -> Result<(), RunnerError> {
        tracing::info!(
            provider = %req.provider,
            bucket = %req.bucket,
            version = %req.version,
            artifacts = req.artifacts.len(),
            "qed outcome: publish skipped (no publishing dispatcher wired)"
        );
        Ok(())
    }
}

/// Stub dispatcher — logs what it would do but takes no action.
/// Used by default until yubaba deploy RPC (R040-F4) and almanac are stable.
pub struct LoggingOutcomeDispatcher;

#[async_trait]
impl OutcomeDispatcher for LoggingOutcomeDispatcher {
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
            "qed outcome: almanac-run skipped (almanac not yet implemented)"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Manual steps — the human half of a pipeline (R622, W282)
// ---------------------------------------------------------------------------

/// Everything a [`ManualGate`] needs to put a [`StepKind::Manual`] step in
/// front of a person.
///
/// [`StepKind::Manual`]: crate::types::StepKind::Manual
#[derive(Debug, Clone)]
pub struct ManualParkRequest {
    /// The parked run, so a gate can route the answer back (`qed.resume`).
    pub run_id: String,
    /// Step index as the UI sees it (already `index_offset`-adjusted).
    pub step_index: usize,
    pub step_name: String,
    /// Pipeline name, for a form title a human can recognise out of context.
    pub pipeline: String,
    /// What the human must accomplish — [`crate::types::ManualConfig::prompt`].
    pub prompt: String,
    /// Commands to prefill terminal tiles with. Never auto-run.
    pub terminal: Vec<String>,
    /// Advisory checkboxes; gate nothing.
    pub checklist: Vec<String>,
    /// The `advance` condition, echoed so the gate can show what it's waiting
    /// on. `None` ⇒ honour-system gate.
    pub advance: Option<String>,
    /// Set when this is a **re-park** after a human answered but `advance`
    /// still failed: the failing command's combined output, verbatim. The
    /// difference between "you haven't done it yet" and "you thought you did,
    /// here's why not" is the whole value of re-parking rather than failing.
    pub advance_failure: Option<String>,
}

/// How a human answered a parked step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualAnswer {
    /// Proceed. The runner still re-evaluates `advance` before continuing —
    /// see [`crate::types::ManualConfig::advance`].
    Continue,
    /// Abandon the run. `reason` is surfaced as the step's failure message.
    Abort { reason: String },
}

/// A live park: the minted form's id plus the channel that fires when the
/// human answers it.
pub struct ManualParkHandle {
    /// Opaque id of whatever the gate minted (a W111 form id in the daemon).
    /// Echoed on [`QedEvent::StepAwaitingHuman`] so a consumer can deep-link.
    pub id: Option<String>,
    /// Fires once, when the human answers. A dropped sender (the gate went
    /// away, the daemon restarted) is treated as "still parked", not as a
    /// silent advance — a manual step must never resolve itself by accident.
    pub answer: tokio::sync::oneshot::Receiver<ManualAnswer>,
    /// Withdraw the prompt. Called when the runner leaves the park without a
    /// human answer — because `advance` started passing on its own, or because
    /// the step is re-parking with a fresh failure. Leaving a stale form in the
    /// AnswerQueue after the run has moved on is the failure mode this exists
    /// to prevent.
    pub withdraw: Box<dyn FnOnce() + Send>,
}

impl std::fmt::Debug for ManualParkHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManualParkHandle")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// How a `kind = "manual"` step reaches a human (R622, W282).
///
/// The qed crate deliberately knows nothing about forms, sessions, or the
/// AnswerQueue — it ships standalone. The camp daemon implements this over
/// W111 forms; `yah qed run` installs nothing and takes the headless path.
///
/// The two lock hooks exist because the `concurrency_key` mutex is held by the
/// *caller* (the daemon's run task), not by the runner: a parked step is not
/// using cargo, and holding `cargo-target` through an overnight park would
/// stall every cargo pipeline in the camp. Both default to no-ops, which is
/// correct for any gate whose caller isn't serializing on a key.
#[async_trait]
pub trait ManualGate: Send + Sync {
    /// Put the request in front of a human. Returning `Err` fails the step
    /// (the gate could not ask), which is the honest outcome — a manual step
    /// that can't reach anyone has not been approved.
    async fn park(&self, req: &ManualParkRequest) -> Result<ManualParkHandle, String>;

    /// Release the run's `concurrency_key` for the duration of the park.
    async fn release_lock(&self) {}

    /// Reacquire the `concurrency_key` before resuming. The run is `Queued`
    /// again between this call and the step continuing — the tree may have
    /// moved, which is why the caller re-evaluates `advance` afterwards.
    async fn reacquire_lock(&self) {}
}

/// Which admission lane the run's *current* unit of work belongs in
/// (R719-F7, W298).
///
/// Named symbolically rather than by key, because the runner does not know
/// what key it was admitted on: `qed_run_handler` rewrites a run's declared
/// `concurrency_key` before taking it (R719-F3 routes a fully-offloaded run to
/// the fleet lane). The runner knows *what kind of work comes next*; the
/// [`AdmissionControl`] implementation owns the mapping to a concrete lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionLane {
    /// The lane this run was admitted on — whatever key the caller resolved.
    Base,
    /// This stretch of work lands on a build worker, not on this box — so
    /// whatever the run holds for local work, it does not need it now. What
    /// that resolves to concretely (hold nothing, or keep a lane the caller
    /// already derived from placement) is the implementation's decision.
    Fleet,
    /// A named lane, for a sub-pipeline child whose `concurrency_key` its
    /// parent is not standing in (see [`sub_pipeline_admission_gap`]).
    Named(String),
}

/// Dynamic admission (R719-F7, W298): the runner tells whoever admitted it
/// which lane the *next* unit of work belongs in, so a long stretch of work
/// that does not touch the run's lane can give it back.
///
/// Two holes close on this one surface, which is why it is one trait:
///
/// - A mixed `auto` run interleaving local and offloaded steps used to hold its
///   *local* key across the offloaded stretches — hours of wall clock spent
///   compiling on another machine while every local recipe in the camp queued
///   behind it. R719-F1 made the default lane camp-global, so over-holding now
///   parks the whole camp rather than one pipeline's own name.
/// - A sub-pipeline child whose key its parent is not holding was serialized
///   against nothing at all (R719-F2 reported it and could not fix it, because
///   fixing it needs exactly this: admission that can be handed back and
///   retaken).
///
/// **The implementation must release before it acquires.** Never hold two lanes
/// at once: a run that keeps lane A while queueing for lane B is the hold-and-
/// wait edge a deadlock cycle needs, and there is no lock ordering to impose
/// across recipes an operator writes. Releasing first can only cost throughput.
///
/// `None` on the runner — what `yah qed run` uses — means no admission control
/// at all: every call is a no-op and the run's lane, if any, is whatever its
/// launcher holds for the whole duration.
#[async_trait]
pub trait AdmissionControl: Send + Sync {
    /// Move the run into `lane`, releasing whatever it currently holds.
    ///
    /// Called at every step boundary, so it must be cheap and idempotent:
    /// re-entering the lane already held is a no-op, not a release/reacquire
    /// round-trip (which would re-queue the run behind every waiter for no
    /// reason).
    async fn enter(&self, lane: AdmissionLane);
}

#[derive(Error, Debug)]
pub enum RunnerError {
    #[error("Step '{step}' failed: {msg}")]
    StepFailed { step: String, msg: String },
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid step configuration: {0}")]
    InvalidConfig(String),
    #[error("Remote dispatch error: {0}")]
    Remote(String),
    /// A terminal outcome / release-provider adapter failed (R509): missing
    /// credential slot, unknown provider, vendor API error.
    #[error("Release outcome error: {0}")]
    Outcome(String),
    /// Plan-time toolchain pinning check failed (R507, W208): the host can't
    /// satisfy one or more `[pipeline.toolchain]` / per-step `toolchain.*` pins
    /// and no container image provides them. Carries the actionable per-pin
    /// report from [`crate::toolchain::ToolchainPreflight::error_report`].
    #[error("{0}")]
    ToolchainUnsatisfied(String),
    /// A run's cancellation took effect while this step (or a nested
    /// sub-pipeline child of it) was in flight — `qed.cancel <run_id>` on
    /// this run or on a descendant registered under it. Distinguished from
    /// `StepFailed` so the step loop hard-stops (ignoring `on_fail =
    /// "continue"`) and reports `RunStatus::Cancelled` rather than `Failed`:
    /// an operator-requested stop is not a defect to recover from.
    #[error("run was cancelled")]
    Cancelled,
}

/// Where pipeline steps execute.
///
/// This is now the operator's **force-override lattice** (R590-F4), not the
/// router itself: [`Auto`](Self::Auto) is the default, and per-step placement
/// is *derived* from what each step declares (via
/// [`resolve_placement`](crate::platform::resolve_placement)). `--where` only
/// exists to pin the whole run one way for testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunWhere {
    /// Policy-derived placement (default, no `--where`): each step runs locally
    /// unless its declared platform resolves to
    /// [`Offload`](crate::platform::Resolution::Offload) — a `native = true`
    /// cross-arch build that can't cross/emulate here — in which case it's
    /// dispatched to an arch-matched build-worker.
    Auto,
    /// Force every step local (`--where=local`): a testing override that
    /// suppresses offload even for a `native = true` cross-arch step.
    Local,
    /// Force every step remote (`--where=remote`): dispatch all steps as
    /// `task::remote` workloads on a yubaba node.
    Remote,
}

/// One step's row in [`PipelineRunner::fleet_portability_report`] (R555-F11):
/// the two independent facts an operator needs to read together, plus the third
/// that decides which of them is the *actionable* one.
#[derive(Debug, Clone)]
pub struct StepPortability {
    /// The step's declared name.
    pub step: String,
    /// Would THIS run dispatch it to a build worker? Folds the `--where`
    /// force-mode, the step's own placement resolution (including the R555-B10
    /// capability demotion), and its kind — only `kind = "subprocess"` steps are
    /// ever dispatched, so a sub-pipeline under `--where=remote` reads `false`.
    pub dispatched_to_fleet: bool,
    /// What stands between the step and a build worker, blocking and advisory.
    /// Empty ⇒ fleet-portable with nothing worth remarking on.
    pub gaps: Vec<crate::fleet_portability::PortabilityGap>,
    /// Set when the step is headed for the fleet **because this host lacks a
    /// cross toolchain** (R555-B10), not because the recipe or the operator
    /// asked. Carries the missing tool and its install hint. The one input that
    /// turns "not portable" from a recipe problem into a
    /// this-can-run-nowhere problem — see
    /// [`PipelineRunner::fleet_portability_gate`].
    pub capability_demotion: Option<crate::nativecross::CrossToolUnavailable>,
}

/// Pure placement policy (R590-F4): fold the operator's `--where` force-mode
/// together with a step's platform [`Resolution`] into a concrete
/// [`Local`](RunWhere::Local) / [`Remote`](RunWhere::Remote) decision. Never
/// returns [`Auto`](RunWhere::Auto) — that's the *input* mode, resolved away
/// here.
///
/// - `Local` / `Remote` force-modes pass straight through (the `--where`
///   override wins over policy, by design).
/// - `Auto` derives from the step: an [`Offload`](crate::platform::Resolution::Offload)
///   resolution — a `native = true` cross-arch build — routes to the fleet;
///   every other verdict (NativeCross / CrossDocker / Emulate / Skip) stays
///   local, where its existing cross/emulate handling applies.
pub(crate) fn policy_placement(
    mode: RunWhere,
    resolution: &crate::platform::Resolution,
) -> RunWhere {
    match mode {
        RunWhere::Local => RunWhere::Local,
        RunWhere::Remote => RunWhere::Remote,
        RunWhere::Auto => match resolution {
            crate::platform::Resolution::Offload { .. } => RunWhere::Remote,
            _ => RunWhere::Local,
        },
    }
}

/// True when a node-bound participant is declared — i.e. some role in
/// `[pipeline.participants]` carries a `node`, so a step will be dispatched to
/// that box no matter what `--where` says
/// ([`PipelineRunner::effective_placement`] puts the binding ahead of even a
/// forced `--where=local`).
///
/// Read off the raw declaration rather than off an allocated
/// [`ParticipantPlan`](crate::participants::ParticipantPlan) on purpose: this
/// is asked *before* the run starts, to decide whether to build a dispatcher at
/// all, and a mis-declared set must fail with its own plan-time diagnosis
/// (`participants::plan_for`, run from both the loader and `run_inner`) rather
/// than as a silently driverless run here.
pub fn pipeline_has_node_bound_participant(pipeline: &Pipeline) -> bool {
    pipeline
        .participants
        .as_ref()
        .is_some_and(|set| set.roles.values().any(|role| role.node.is_some()))
}

/// True when any step in `pipeline` resolves to
/// [`Offload`](crate::platform::Resolution::Offload) on `host` — i.e. a default
/// (`--where=auto`) run of it needs fleet access even without `--where=remote`
/// (R590-F4). The CLI uses this to decide whether to stand up a mesh dispatcher
/// (via [`PipelineRunner::new_auto`]) or stay on the driverless local path: a
/// pipeline of ordinary cross-compilable steps needs no cloud wiring at all.
///
/// R823-T3: a node-bound participant counts too, and it is *not* reachable
/// through the per-step platform resolution below — a participant step declares
/// no `platform`, so on the camp Mac every one of them resolves `NativeCross`
/// and this returned `false` for a pipeline that cannot run without a
/// dispatcher. The first participant set ever pointed at real hardware died on
/// exactly that: "no remote dispatcher is wired", from `--where=auto`, on a
/// pipeline whose whole content is a rendezvous. Placement is a property of the
/// *binding* there, not of the target triple.
///
/// R555-B10: `capability` is the third reason, and threading it here is the
/// non-obvious half of that fix. This question is answered from pipeline + host
/// *before* a runner exists, so a per-step Capability demotion the runner makes
/// but this cannot see produces a step resolving `Offload` with no dispatcher
/// wired — the `InvalidConfig` "no remote dispatcher" below, which trades a
/// clear "install cargo-zigbuild" for a confusing one. Callers on the local box
/// pass [`ToolAvailability::probe`](crate::nativecross::ToolAvailability::probe);
/// a caller reasoning about a *remote* host's pipeline passes that host's known
/// set (or `FULL`), exactly as it would to
/// [`PipelineRunner::with_cross_availability`].
///
/// R555-B15: `resolver` is the fourth reason, and the one that made this question
/// wrong for a whole class of recipe. This used to read `pipeline.steps` and
/// stop there, so a `kind = "sub-pipeline"` step — which declares no
/// `platform` of its own and therefore resolves `NativeCross` — hid every
/// offload need underneath it. `yah-release-wizard` is entirely that shape:
/// its x86 musl work lives in `yah-cli-release`, reached through a
/// sub-pipeline step, so this returned `false`, the CLI stood up no fleet
/// client, and the leg died with "no remote dispatcher is wired" on a run
/// that was already `--where=auto`. Running the *child* directly worked,
/// which is what made it read as a placement bug rather than a probe one.
/// Pass the same resolver the runner will get; [`NoopSubPipelineResolver`]
/// only when the pipeline provably has no children.
///
pub fn pipeline_needs_offload(
    pipeline: &Pipeline,
    host: &str,
    capability: &crate::nativecross::ToolAvailability,
    resolver: &dyn crate::types::SubPipelineResolver,
) -> bool {
    let mut chain: Vec<String> = Vec::new();
    needs_offload_walk(pipeline, host, capability, resolver, &mut chain)
}

/// The recursive half of [`pipeline_needs_offload`] — `chain` carries the
/// sub-pipeline targets already entered, mirroring
/// [`crate::types::validate_sub_pipeline_graph`]'s walker.
fn needs_offload_walk(
    pipeline: &Pipeline,
    host: &str,
    capability: &crate::nativecross::ToolAvailability,
    resolver: &dyn crate::types::SubPipelineResolver,
    chain: &mut Vec<String>,
) -> bool {
    if pipeline_has_node_bound_participant(pipeline) {
        return true;
    }
    for step in &pipeline.steps {
        if matches!(
            step_placement(step, host, capability),
            crate::platform::Resolution::Offload { .. }
        ) {
            return true;
        }
        if step.kind != crate::types::StepKind::SubPipeline {
            continue;
        }
        let Some(cfg) = step.sub_pipeline.as_ref() else {
            continue;
        };
        let token = crate::types::sub_pipeline_ref_token(&cfg.target);
        // Cycles are already rejected by `validate_sub_pipeline_graph` on the
        // load path, but this runs from callers with no such guarantee and a
        // pre-flight question must not hang.
        if chain.contains(&token) || chain.len() > crate::types::MAX_SUB_PIPELINE_DEPTH {
            continue;
        }
        let Some(child) = resolver.resolve(&cfg.target) else {
            continue;
        };
        chain.push(token);
        let child_offloads = needs_offload_walk(&child, host, capability, resolver, chain);
        chain.pop();
        if child_offloads {
            return true;
        }
    }
    false
}

/// One step's placement resolution against `host` and `capability` — the
/// pipeline-level (pre-runner) mirror of
/// [`PipelineRunner::resolve_step`](PipelineRunner::resolve_step), shared by
/// [`pipeline_needs_offload`], [`pipeline_is_fully_offloaded`] and
/// [`pipeline_capability_demotions`] so the up-front questions and the runner's
/// per-step routing cannot drift apart (R555-B10).
fn step_placement(
    step: &crate::types::QedStep,
    host: &str,
    capability: &crate::nativecross::ToolAvailability,
) -> crate::platform::Resolution {
    let p =
        crate::platform::Platform::compose(host, step.platform.as_ref(), step.triple.as_deref());
    let native = step.platform.as_ref().map(|s| s.native).unwrap_or(false);
    crate::platform::resolve_placement(
        &p.host,
        p.target.as_deref(),
        p.container_platform.as_deref(),
        native,
        capability,
    )
}

/// Every step of `pipeline` that `host` cannot carry as derived (R555-B10) —
/// `(step name, gap)`, the pre-runner mirror of
/// [`PipelineRunner::capability_demotions`].
///
/// The CLI's `--where=auto` notice reads this so the operator is told *which
/// tool to install* at the moment their build is being shipped off-box. W235
/// §6's non-goal: the demotion must never be silent.
pub fn pipeline_capability_demotions(
    pipeline: &Pipeline,
    host: &str,
    capability: &crate::nativecross::ToolAvailability,
) -> Vec<(String, crate::nativecross::CrossToolUnavailable)> {
    pipeline
        .steps
        .iter()
        .filter_map(|step| {
            let p = crate::platform::Platform::compose(
                host,
                step.platform.as_ref(),
                step.triple.as_deref(),
            );
            let native = step.platform.as_ref().map(|s| s.native).unwrap_or(false);
            let derived = crate::platform::derive_placement(
                &p.host,
                p.target.as_deref(),
                p.container_platform.as_deref(),
                native,
            );
            let gap = crate::platform::capability_demotion(
                &p.host,
                p.target.as_deref(),
                p.container_platform.as_deref(),
                &derived,
                capability,
            )?;
            Some((step.name.clone(), gap))
        })
        .collect()
}

/// True when **every** step in `pipeline` resolves to
/// [`Offload`](crate::platform::Resolution::Offload) on `host` — the dual of
/// [`pipeline_needs_offload`], and the one R719-F3 (W298) needs.
///
/// "Any step offloads" answers *do I need fleet wiring at all*. "Every step
/// offloads" answers a different question: *does this run touch the local
/// build resources its concurrency key is protecting*. A run whose whole body
/// executes on a build worker holds a local cargo lane for hours while
/// compiling nothing locally, starving every local recipe in the camp.
///
/// An empty pipeline is **not** fully offloaded — `all()` over nothing is
/// vacuously true, which would quietly hand a no-op pipeline the fleet lane.
pub fn pipeline_is_fully_offloaded(
    pipeline: &Pipeline,
    host: &str,
    capability: &crate::nativecross::ToolAvailability,
) -> bool {
    !pipeline.steps.is_empty()
        && pipeline.steps.iter().all(|step| {
            matches!(
                step_placement(step, host, capability),
                crate::platform::Resolution::Offload { .. }
            )
        })
}

/// Map a Docker image tag (`reg/repo:ver`) to a filesystem-safe stem for
/// OCI archive output under `.yah/cache/images/`. Replaces every byte that
/// isn't `[A-Za-z0-9_.-]` with `_`. `pub(crate)` so [`crate::image_overlay`]
/// can derive the same kind of collision-free stem for a GHA-emulator
/// build-context publish key (R605-F2) without duplicating the mapping.
pub(crate) fn tag_to_filename(tag: &str) -> String {
    tag.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Catalog lookup + Dockerfile staging output, shared by local and remote
/// build-image dispatch.
struct PreparedBuildImage {
    dockerfile_path: std::path::PathBuf,
    /// Directory BuildKit builds *from*: the step's `context`, resolved under
    /// the camp root, or the camp root itself when the step declares none.
    ///
    /// Both dispatch paths read this one field. They used to disagree — local
    /// honoured `step.context` while remote hardcoded the camp root — which
    /// meant an offloaded build shipped a different (and vastly larger) context
    /// than the same step built locally (R636-B1).
    context_dir: std::path::PathBuf,
    buildkit_dir: std::path::PathBuf,
    archive_path: std::path::PathBuf,
    tag: String,
}

/// A source context that has been uploaded and is waiting to be fetched once
/// and then deleted (R560-T8).
///
/// Both halves are kept because they are used by different sides: the worker
/// only ever sees `url`, and only the runner can `discard` the `key`.
struct PublishedSourceContext {
    key: String,
    url: String,
}

/// Placement mesh-tags for a remote **subprocess** step (R590-F2).
///
/// When the step declares a target arch via `[platform].target`, return the
/// arch-matched build-worker selector (`tag:build-worker` + `arch:x86|arm`) so
/// the run is placed on a node of that arch and executes *natively* — the whole
/// point of the fleet path is that an arm64 host can drive an
/// `x86_64-unknown-linux-musl` build on the x86 box (us-west-002) instead of
/// emulating it locally. No `platform.target` ⇒ empty tags ⇒ any infra node.
///
/// Mirrors the build-image path's placement, but keyed off the step's declared
/// *target* rather than the runner's host triple.
fn remote_subprocess_mesh_tags(step: &crate::types::QedStep) -> Vec<String> {
    match step.platform.as_ref().and_then(|p| p.target.as_deref()) {
        Some(target) => crate::platform::build_worker_mesh_tags(
            crate::platform::arch_of(target),
            crate::platform::os_tag_of(target),
        ),
        None => Vec::new(),
    }
}

/// The build-cache sharing key for a step that declares `cache = true`
/// (R876-F4), or `None` when it does not.
///
/// Derived from **pipeline + step name + target triple**, and derived rather
/// than configured on purpose. The hazard a shared cargo target dir carries is
/// not a slow build, it is a wrong one: two different pipelines, or one
/// pipeline's two triples, writing one target dir. `concurrency_key` does not
/// close that — it is camp-side scheduling, so it constrains this camp's runs
/// of this pipeline and says nothing about a second camp or a hand-rolled
/// dispatch aimed at the same worker. A key nobody can typo makes the collision
/// impossible instead of unlikely.
///
/// Steps with no `[platform].target` fall back to `step.triple`, then to
/// `host` — a host-native build's cache is still per-pipeline+step, and the
/// literal is only ever a component of a key, never a claim about the arch.
fn step_cache_key(pipeline: &str, step: &crate::types::QedStep) -> Option<String> {
    if !step.cache {
        return None;
    }
    let triple = step
        .platform
        .as_ref()
        .and_then(|p| p.target.as_deref())
        .or(step.triple.as_deref())
        .unwrap_or("host");
    Some(workload_spec::forge_cache::key_from_parts(
        pipeline, &step.name, triple,
    ))
}

/// Evict aged-out / disk-pressuring dirs from the camp-local build-cache root
/// (R876-F4). The local-container twin of yubaba's worker-side sweep, sharing
/// its policy through `forge_cache::evict_plan` so the two cannot drift.
///
/// Best-effort and opportunistic — called just before a cached local step, the
/// same way `sweep_stale_produced_dirs` rides the deploy path, so bounded
/// growth costs no background task. A failure here loses disk, never a build,
/// so it warns rather than propagating.
fn sweep_build_cache_root(root: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    let mut dirs = Vec::new();
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if !meta.is_dir() {
            continue;
        }
        let Ok(mtime) = meta.modified() else { continue };
        dirs.push((entry.path(), mtime));
    }
    let plan = workload_spec::forge_cache::evict_plan(
        &dirs,
        std::time::SystemTime::now(),
        workload_spec::forge_cache::RETENTION,
        free_bytes_at(root).unwrap_or(u64::MAX),
        workload_spec::forge_cache::FREE_FLOOR_BYTES,
    );
    for dir in plan {
        match std::fs::remove_dir_all(&dir) {
            // Deleting a build cache is destructive-ish and silently expensive
            // (the next run is cold), so it is legible at default verbosity —
            // the same call `reap_produced_dir` was corrected to make.
            Ok(()) => tracing::info!(dir = %dir.display(), "evicted stale qed build cache"),
            Err(e) => tracing::warn!(dir = %dir.display(), error = %e, "failed to evict build cache"),
        }
    }
}

/// Free bytes on the filesystem holding `path`, via POSIX `df -Pk`.
///
/// `df` rather than `statvfs` for the reason yubaba's `read_filesystem` states:
/// it is dep-free and identical on both platforms. `None` when `df` is
/// unavailable or unparseable — the caller then treats free space as unbounded
/// and falls back to the retention rule alone, which fails toward keeping a
/// cache rather than toward deleting one on a bad read.
fn free_bytes_at(path: &std::path::Path) -> Option<u64> {
    let out = std::process::Command::new("df")
        .arg("-Pk")
        .arg(path)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    // Filesystem  1024-blocks  Used  Available  Capacity  Mounted-on
    let fields: Vec<&str> = text.lines().nth(1)?.split_whitespace().collect();
    if fields.len() < 4 {
        return None;
    }
    fields[3].parse::<u64>().ok()?.checked_mul(1024)
}

/// Per-step container image override (R590-F2, finishing the R381 `step.image`
/// seam) so the argv runs *inside that image* — e.g. `rusty-v8-musl-builder`
/// executing `build-v8.sh`. `None` ⇒ fall back to the default forge image
/// (`yah-rust-bun`), preserving the pre-seam behaviour for plain steps. Used by
/// both the local-container and remote subprocess paths so `image` behaves the
/// same regardless of `--where`.
///
/// Two spellings, distinguished by shape (R590-B5):
///
/// - **Full ref** — anything containing `/` or `@`, e.g.
///   `cr.yah.dev/rusty-v8-musl-builder:v149.4.0-amd64@sha256:…`. Parsed
///   verbatim through [`workload_spec::ImageRef::parse_pinned`]. This is the
///   spelling to reach for: it names the registry explicitly (so a pipeline is
///   not welded to whatever host [`catalog_image`] happens to hard-code) and it
///   carries a real digest, so the pull is content-addressed rather than
///   chasing a floating tag.
/// - **Bare catalog name** — e.g. `yah-rust-bun`. Resolved through
///   [`velveteen_exec::default_image::catalog_image`] to
///   `ghcr.io/yah-ai/<name>:latest` plus the compile-time digest, or the
///   all-zeros [`workload_spec::ImageRef::UNPINNED_DIGEST`] sentinel on dev
///   builds (which `pull_ref` then degrades to a tag-only pull).
///
/// A full ref without a digest is a hard config error, not a silent tag pull —
/// if you went to the trouble of naming a registry, you get pinning with it.
///
/// [`catalog_image`]: velveteen_exec::default_image::catalog_image
fn step_image_override(
    step: &crate::types::QedStep,
) -> Result<Option<workload_spec::ImageRef>, RunnerError> {
    let Some(image) = step.image.as_deref() else {
        return Ok(None);
    };
    if image.contains('/') || image.contains('@') {
        return workload_spec::ImageRef::parse_pinned(image)
            .map(Some)
            .map_err(|reason| {
                RunnerError::InvalidConfig(format!(
                    "step `{}` sets image = {image:?}, which looks like a full registry \
                     reference but does not parse: {reason}. Either spell it as a bare \
                     catalog name (`rusty-v8-musl-builder`) or as a digest-pinned ref \
                     (`cr.yah.dev/rusty-v8-musl-builder:<tag>@sha256:<hex>`).",
                    step.name,
                ))
            });
    }
    Ok(Some(velveteen_exec::default_image::catalog_image(image)))
}

/// Identifies the sub-pipeline child a [`ChildEventFactory`] is being asked to
/// open a channel for (R768). Everything the host needs to register the run
/// before its first event arrives.
#[derive(Debug, Clone)]
pub struct ChildRunInfo {
    /// The child's freshly-minted run id — the same one that shows up in the
    /// parent's `sub-pipeline failed at child step …(run_id=…)` message, and
    /// the id an operator will type into `qed.status`.
    pub run_id: QedRunId,
    /// The child pipeline's declared name (e.g. `release-check`).
    pub pipeline: String,
    /// The immediate parent's run id — the child is registered under this so
    /// the nested tree is walkable from history alone.
    pub parent_run_id: QedRunId,
}

/// Host hook that hands a sub-pipeline child its own event channel.
/// See [`PipelineRunner::with_child_event_factory`].
pub type ChildEventFactory =
    Arc<dyn Fn(&ChildRunInfo) -> Option<UnboundedSender<QedEvent>> + Send + Sync>;

/// Host hook invoked right after a sub-pipeline child's task is spawned,
/// handed the child's run id and its `AbortHandle` so the embedder can
/// register it for independent cancellation — `qed.cancel <child_run_id>`
/// stops just that nested child (and, by the same recursive construction,
/// anything nested under IT) without touching the parent. See
/// [`PipelineRunner::with_child_abort_hook`]. Returning `None` from
/// `with_child_abort_hook` (the default) leaves a nested child cancellable
/// only via the parent's own task-abort, same as before this hook existed.
pub type ChildAbortHook = Arc<dyn Fn(&str, tokio::task::AbortHandle) + Send + Sync>;

pub struct PipelineRunner {
    pipeline: Pipeline,
    run_id: QedRunId,
    remote_driver: Option<Arc<RemoteForgeDriver>>,
    /// Transport for a build-image step's context when the builder is a
    /// different machine than this runner (R636-B1). `None` means the embedder
    /// wired none, and an offloaded build-image step refuses with instructions
    /// rather than bind-mounting a camp path onto a host that has no such path.
    /// See [`crate::build_context`].
    build_context_publisher: Option<Arc<dyn crate::build_context::BuildContextPublisher>>,
    run_where: RunWhere,
    /// R833-F8: the operator's **imperative** target — one named fleet node that
    /// every remotely-placed step of this run is pinned to
    /// (`yah qed run <p> --where=node:us-west-003`).
    ///
    /// Orthogonal to [`run_where`](Self::run_where) on purpose. `run_where`
    /// answers *whether* a step leaves this box (and `Auto` derives that per
    /// step from the platform resolution); this answers *which* box it lands on
    /// once it does. Keeping them separate is what lets an explicit target win
    /// over inference without disturbing inference: `None` — every caller
    /// before this field existed — leaves placement to the arch/tag matcher
    /// exactly as R594 shipped it, and `Some` replaces the *node-selection*
    /// half only. See [`with_pinned_node`](Self::with_pinned_node).
    pinned_node: Option<workload_spec::MeshIdent>,
    outcome_dispatcher: Arc<dyn OutcomeDispatcher>,
    /// Optional live-event sink (R325-F2). When set, `run()` emits a
    /// [`QedEvent`] at each lifecycle boundary; when `None` the runner is
    /// silent and only the terminal [`QedRunMeta`] is observable.
    events: Option<UnboundedSender<QedEvent>>,
    /// Camp root used by build-image steps to locate per-camp images
    /// (`<camp_root>/.yah/qed/images/<name>/`) and write generated artifacts
    /// (`<camp_root>/.yah/cache/{buildkit,images}/`). Falls back to
    /// `std::env::current_dir()` when unset — production callers leave this
    /// alone; tests override via [`Self::with_camp_root`] to avoid leaking
    /// `.yah/cache/` into the working directory.
    camp_root: Option<std::path::PathBuf>,
    /// Environment the embedder wants under **every locally-executed** step,
    /// below the step's own `env` (R744-T2). Empty by default — a bare `qed`
    /// has no opinion about the host's toolchain configuration.
    ///
    /// This exists because a step inherits this process's environment, and
    /// "this process's environment" is the wrong place for a value that the
    /// embedder computes *per camp*. The motivating case is the host's shared
    /// Rust build cache: `.cargo/config.toml` declares `SCCACHE_DIR` relative
    /// to itself, an isolated run copies that file into a worktree under
    /// `$TMPDIR`, and the per-user sccache singleton then gets pinned to a
    /// throwaway directory by whichever build reached it first. The embedder
    /// knows the real absolute path; this is how it says so.
    ///
    /// Deliberately **not** applied to offloaded steps: their env is resolved
    /// against a worker's filesystem, where a coordinator-side absolute path
    /// names nothing. Container steps are excluded for the same reason.
    ///
    /// Inherited by sub-pipeline children — a nested `cargo` step is no less
    /// local than a top-level one.
    base_env: Vec<(String, String)>,
    /// Sigstore signer for `kind = "sign-native-tarball"` steps (R407-T5).
    /// Defaults to [`LoggingSigner`], which writes placeholder bytes and
    /// logs a warning so a local `yah qed run` doesn't fail when cosign
    /// isn't installed.
    ///
    /// R605-F1: callers no longer have to *decide* — every live construction
    /// site passes [`crate::native::resolve_signer`] to
    /// [`Self::with_signer`], which reads the identity out of the environment
    /// and only falls back to the placeholder when none is configured. The
    /// R407-T5 gotcha in this file's header ("release CI MUST wire
    /// CosignSigner explicitly") is answered by that: CI exports
    /// `QED_COSIGN_KEY` and gets a real signer.
    signer: Arc<dyn SigstoreSigner>,
    /// Subprocess executor for local `kind = "subprocess"` steps (R438-T14).
    /// Defaults to [`LocalForgeDriver`]. Override via [`Self::with_executor`]
    /// when a caller wants to share a configured driver (e.g. the cloud
    /// reconciler reuses one across many materialize calls).
    executor: Arc<dyn ForgeExecutor>,
    /// Resolver for `kind = "sub-pipeline"` steps (R488-F2). Defaults to a
    /// no-op resolver that returns `None` for every target — production
    /// callers wire a [`PipelineLoader`]-backed resolver via
    /// [`Self::with_sub_pipeline_resolver`]. With the default resolver, a
    /// SubPipeline step's target is unresolvable and the step fails with a
    /// clear "no resolver configured" message.
    sub_pipeline_resolver: Arc<dyn crate::types::SubPipelineResolver + Send + Sync>,
    /// When `true`, this runner's terminal `Outcome::Publish` outcomes are
    /// suppressed at the end of `run()`. Set on child runners constructed
    /// for a SubPipeline step where the parent declared
    /// `propagate.produces = true` — the parent owns the terminal publish,
    /// so firing it on the child would double-publish and double-revalidate.
    /// All other outcomes (`YubabaDeploy`, `AlmanacRun`) still run.
    suppress_publish_outcomes: bool,
    /// Set on child runners spawned by a SubPipeline step (R488-F5). The
    /// child's terminal [`QedRunMeta`] carries this back to consumers so
    /// the nested tree can be rebuilt from history alone. `None` on
    /// top-level runs.
    parent_run_id: Option<QedRunId>,
    /// Host-supplied factory that gives a sub-pipeline CHILD its own event
    /// channel (R768). Inherited by children, so it reaches every depth.
    ///
    /// Without it a child runs with `events: None` — and because a local
    /// subprocess step's stdout/stderr goes ONLY to the event channel
    /// (`task_run_id` tracks remote dispatch, not local execution), a failing
    /// child step's output is not merely unpersisted, it is discarded. That is
    /// how `release-wizard` came to report `sub-pipeline failed at child step
    /// 'cargo-test' (run_id=…)` for a run id that resolves to nothing and a
    /// log that was never written: five failed releases with no readable
    /// reason.
    ///
    /// A child cannot simply share the parent's sender. The daemon's drain
    /// folds events into the registered meta BY STEP INDEX, so a child's step 0
    /// would overwrite the parent's step 0 — which is why this is a factory
    /// handing back a distinct channel per child run rather than a clone.
    /// Returning `None` restores the old silent behaviour.
    child_event_factory: Option<ChildEventFactory>,
    /// Host hook that registers a sub-pipeline child's `AbortHandle` right
    /// after it's spawned (see [`ChildAbortHook`]). `None` (the default, and
    /// what `yah qed run` without a daemon leaves it at) means a nested
    /// child has no independent abort path — cancelling it falls back to
    /// cancelling the whole run, exactly as before this field existed.
    /// Inherited by children so a grandchild registers too.
    child_abort_hook: Option<ChildAbortHook>,
    /// Mirrors [`crate::types::SubPipelineConfig::own_workspace`] onto the
    /// child runner (R755). `false` on every top-level runner (there is
    /// nothing to opt out of — a top-level run always positions its own
    /// workspace) and on a child by default (W224/R533-F11 inheritance).
    /// `true` only when this child's `[sub_pipeline]` block set it, which
    /// makes [`Self::run_inner`]'s positioning skip
    /// (`self.parent_run_id.is_some()`) additionally check `!own_workspace`
    /// — so an opted-in child repositions per its OWN [`WorkspaceMode`]
    /// instead of building from the parent's already-positioned tree.
    ///
    /// Flattened from the config's `Option<bool>` (R887): unset means
    /// inherit here, because by the time a `Pipeline` reaches a runner the
    /// loader's [`crate::types::validate_sub_pipeline_graph`] has already
    /// refused the one composition where that default would silently discard
    /// the child's own declared mode.
    own_workspace: bool,
    /// Added to every emitted step `index` so that a resume-from-step run
    /// (where the pipeline had its leading steps drained) still shows the
    /// original step position in the UI (e.g. step 6 of 6 instead of 1 of 1).
    /// Set via [`Self::with_step_selection`] in callers that drain steps.
    index_offset: usize,
    /// R906-F2 — a name-subset selection (`selected_steps`) removed arbitrary
    /// steps from [`Self::pipeline`] before the runner was handed it.
    ///
    /// The sibling of [`Self::index_offset`], and it needs its own field
    /// because the two truncations are independent: a resume drops a *prefix*
    /// (describable by a count), a subset drops *arbitrary* steps (not
    /// describable at all), and one run can do both. Set via
    /// [`Self::with_step_selection`].
    steps_are_subset: bool,
    /// R499-F3 phase 2: per-step gha-workflow matrix subset. Keyed by
    /// qed step name; the inner set is the chosen
    /// [`yah_qed_gha::graph::JobInstance::key`] values (`<job>` for
    /// non-matrix, `<job>#<row>` for matrix). When a gha-workflow step
    /// has an entry here, [`Self::execute_step_gha_workflow`] threads
    /// it into [`yah_qed_gha::Executor::included_instance_keys`] so
    /// non-selected rows short-circuit to `Skipped`. Steps missing from
    /// the map run their full matrix. Set via
    /// [`Self::with_gha_matrix_subset`].
    gha_matrix_subset: std::collections::HashMap<String, std::collections::HashSet<String>>,
    /// On-demand override for [`StepActivation::Stubbed`] steps (R506). When
    /// `true`, the runner ignores `status = "stubbed"` and runs the step the
    /// same way an `active` step would. Set via [`Self::with_include_stubbed`].
    /// Defaults to `false`; `enabled = false` is still always honored even
    /// when this flag is on (the two knobs are orthogonal — `enabled` means
    /// "explicitly off for this run", `stubbed` means "not implemented yet").
    include_stubbed: bool,
    /// Opt-in to QEMU emulation for this run (R560, W236). A step whose
    /// foreign-arch container resolves to [`Emulate`](crate::platform::Resolution::Emulate)
    /// is a last-ditch, slow path — a foreign image pulled and run under
    /// emulation, often 10-50× slower, and easy to trip into by declaring a
    /// foreign `container_platform` without meaning to. [`Self::run_inner`]
    /// **refuses to start** a pipeline with such a step unless this is `true`,
    /// so an unintended emulated build fails at second zero with an actionable
    /// error instead of silently costing an hour. Defaults `false` (emulation
    /// is opt-in, never the default); set per-run via [`Self::with_allow_emulate`]
    /// (the `yah qed run --allow-emulate` confirmation) and inherited by
    /// SubPipeline children.
    allow_emulate: bool,
    /// The operator's `--force` (R555-F11): bypass the fleet-portability gate.
    ///
    /// Same escape hatch as the W155 environment gate's `force`, deliberately —
    /// [`crate::placement_gate::evaluate`] already spells "I know, run it
    /// anyway" that way, and inventing a second flag for the same sentence is
    /// how a CLI grows two words for one idea. Defaults `false`; set per-run via
    /// [`Self::with_force`] and inherited by SubPipeline children.
    force: bool,
    /// R823-F2 — the pipeline's allocated participant set, or `None` when it
    /// declares none (which is every pipeline that isn't a multi-host case).
    ///
    /// Lazily derived from `pipeline.participants` because the allocation is a
    /// pure function of the declaration — no host probing, no clock, no I/O —
    /// so there is nothing a constructor could learn that
    /// [`crate::participants::plan_for`] doesn't already know. The `Err` arm
    /// carries the rendered message rather than the typed error only so this
    /// field stays `Clone`-free and cheap; [`Self::participant_plan`] turns it
    /// back into a [`RunnerError::InvalidConfig`] at the one place it matters.
    ///
    /// A `OnceLock` rather than a plain field for one reason: the allocation
    /// can fail, and `new()` doesn't return `Result`. Surfacing the failure at
    /// the run-preflight seam (where every other config refusal already lives)
    /// beats making four constructors fallible.
    participant_plan:
        std::sync::OnceLock<Result<Option<crate::participants::ParticipantPlan>, String>>,
    /// Matrix coordinate this runner is executing for (R506). Set by the
    /// planner when fanning a pipeline over its `[matrix]` block; threaded
    /// into the `if=` expression context so a step can gate on
    /// `matrix.<key>` values. `None` for non-matrix runs — `matrix.<key>`
    /// lookups then return `Null`/falsy via [`yah_qed_gha::Context`] semantics.
    matrix_coord: Option<crate::matrix::MatrixCoord>,
    /// Self-detected host triple this runner executes on (R531-T1, W222),
    /// e.g. `aarch64-apple-darwin`. Detected once at construction via
    /// [`crate::platform::detect_host_triple`] and threaded into the plan
    /// context — the GHA executor's `runner.{os,arch}` for workflow steps,
    /// and (once F2/F3 land) the `host` leg of each step's `Platform` triple
    /// that `resolve(host, target, container_platform)` reasons over. Override
    /// via [`Self::with_host_triple`] when the execution host differs from the
    /// process host (e.g. a remote runner whose triple the daemon knows).
    host_triple: String,
    /// Which host-native cross toolchains are installed (R531-T6, W222).
    /// Probed lazily on first use ([`Self::cross_availability`]) so building a
    /// runner shells out nothing; seedable via [`Self::with_cross_availability`]
    /// for tests and for a daemon that knows a remote runner's toolchain set.
    /// Consumed when a NativeCross step's argv is rewritten onto cargo-zigbuild
    /// / musl-cross (F5's [`crate::nativecross::plan_native_cross`]).
    cross_availability: std::sync::OnceLock<crate::nativecross::ToolAvailability>,
    /// Host-detected toolchain versions for the plan-time pinning check (R507,
    /// W208). Probed lazily ([`Self::host_toolchains`]) — a runner whose
    /// pipeline declares no `[toolchain]` pins never shells out — and seedable
    /// via [`Self::with_host_toolchains`] for tests and for a daemon that knows
    /// a remote runner's installed versions. Maps pin key → detected version
    /// (`None` = tool absent on host).
    host_toolchains: std::sync::OnceLock<std::collections::HashMap<String, Option<String>>>,
    /// Registry of vendor release adapters (R509) dispatched by
    /// [`Outcome::Provider`]. Empty by default — the CLI / daemon construction
    /// sites wire the built-in set via [`Self::with_release_providers`]. An
    /// `Outcome::Provider` naming an unregistered adapter fails with a typed
    /// error listing the known names.
    provider_registry: Arc<crate::provider::ProviderRegistry>,
    /// Credential resolver passed to vendor adapters at dispatch (R509).
    /// Defaults to an empty [`crate::provider::MapSecrets`]; production wires
    /// [`crate::secrets_bridge::SecretsConfig`] over the vault via
    /// [`Self::with_release_providers`].
    secrets: Arc<dyn crate::provider::SecretSource>,
    /// Target git ref for this run (W224, R330-B27) — a branch, tag, or SHA;
    /// `git checkout` / `git worktree add` already accept any committish, so
    /// workspace mode (Live/Checkout/Isolated) and ref kind are orthogonal
    /// axes (don't add a 4th mode for this — fix the ref parameter). Drives
    /// how the runner positions the workspace before a `gha-workflow` step
    /// runs, per the pipeline's [`WorkspaceMode`](crate::types::WorkspaceMode).
    /// `None` ⇒ `HEAD` — build the commit that's already checked out, never
    /// silently jump to `main` (that would ship the wrong bytes for a
    /// tag-triggered release). Set by the launch surface (`yah qed run
    /// --ref`, the QED-tab selector).
    git_ref: Option<String>,
    /// The run's *resolved* params (R653-F1) — `Pipeline::resolve_params`
    /// output, i.e. supplied values with declared defaults filled in. Threaded
    /// into the `if=` expression context as the `params` namespace so a step
    /// can gate on `params.<name>`, which is what turns a run param from a
    /// substitution into a build *variant*.
    ///
    /// Note this is deliberately NOT derivable from `pipeline.params`: that
    /// field holds [`ParamDef`](crate::types::ParamDef) *declarations*, and by
    /// the time a runner exists the resolved values have already been consumed
    /// by `Pipeline::apply_params` and erased into substituted argv/env. Both
    /// launch surfaces (`yah qed run` and the daemon's `qed.run`) therefore
    /// pass the same map they fed to `apply_params` here via
    /// [`Self::with_params`]. Empty when a caller doesn't — `params.<name>`
    /// then evaluates to `Null`/falsy, matching how an unset `matrix` behaves.
    params: std::collections::HashMap<String, String>,
    /// The on-disk tree this run actually builds against, positioned once at
    /// run start per the pipeline's [`WorkspaceMode`] + target ref (W224
    /// R533-F11). Set by [`Self::run_inner`] before any step executes; every
    /// step kind then resolves its root through [`Self::resolve_camp_root`],
    /// which prefers this. Unset until positioned (and on child runners, which
    /// inherit the parent's already-positioned tree via `camp_root`). For
    /// `Isolated` mode this is the throwaway worktree path — so a subprocess
    /// `desktop-release` step builds from the same worktree as the run's
    /// `gha-workflow` step, not the live camp root.
    positioned_workspace: std::sync::OnceLock<std::path::PathBuf>,
    /// An isolated pipeline's previously-retained worktree to re-enter instead
    /// of positioning a fresh one (R766). Set via
    /// [`Self::with_resume_workspace`] by `qed_rerun_handler`, which reads it
    /// off the SOURCE run's `QedRunMeta::retained_workspace` — no caller that
    /// isn't resuming a failed isolated run ever sets this. A missing/vanished
    /// path (evicted by retention, or the source run predates this field)
    /// degrades to exactly what an ordinary `Isolated` run does: `prepare_workspace`
    /// silently `git worktree add`s a fresh one rather than failing the resume.
    /// Ignored outside [`WorkspaceMode::Isolated`](crate::types::WorkspaceMode::Isolated).
    resume_workspace: Option<std::path::PathBuf>,
    /// How a `kind = "manual"` step reaches a human (R622, W282). `None` — the
    /// default, and what `yah qed run` uses — means *headless*: there is no
    /// AnswerQueue to mint a form into, so a manual step advances on its
    /// `advance` condition alone and fails with an actionable message if it has
    /// none. The camp daemon installs a gate that mints a W111 `Form`.
    ///
    /// Deliberately an injected trait rather than a forms dependency: the qed
    /// crate ships standalone (`oss/qed`), and "the system is blocked on a
    /// human" is a camp concept, not a CI-scheduler concept.
    manual_gate: Option<Arc<dyn ManualGate>>,
    /// Dynamic admission (R719-F7, W298). `None` — the default, and what
    /// `yah qed run` uses — means the run's lane is whatever its launcher took
    /// for the whole duration; every lane call below is a no-op. The camp
    /// daemon installs a control backed by its `qed_locks` map. Inherited by
    /// sub-pipeline children.
    admission: Option<Arc<dyn AdmissionControl>>,
    /// The concrete lane this runner's *local* work belongs in, when it is not
    /// the lane its launcher was admitted on (R719-F7).
    ///
    /// `None` for a top-level run: its ordinary work belongs in
    /// [`AdmissionLane::Base`], the lane the daemon resolved for it. `Some(key)`
    /// on a sub-pipeline child that wants a lane its parent is not standing in
    /// — the case [`sub_pipeline_admission_gap`] reports. Every lane decision
    /// this runner makes then names that key instead of `Base`, so a child's own
    /// steps (and its own children) admit against the child's key, not the
    /// parent's.
    admission_lane: Option<String>,
    /// What this run is *about*, when it was launched from a doc cell (R717-T3,
    /// W296). Copied verbatim onto the terminal [`QedRunMeta::cell`] so the run
    /// meta stays the source of truth and any derived cell index is rebuildable
    /// by rescan. Set via [`Self::with_cell`]; `None` for every ordinary
    /// pipeline run, and deliberately NOT inherited by sub-pipeline children —
    /// a child is a different pipeline, and stamping the parent's cell key on it
    /// would file the child's verdict under the parent's badge.
    cell: Option<crate::types::CellRef>,
}

/// Default [`SubPipelineResolver`] for [`PipelineRunner`] — returns `None`
/// for every target. Production callers replace it with a
/// [`PipelineLoader`]-backed resolver via
/// [`PipelineRunner::with_sub_pipeline_resolver`]; tests pass an
/// in-memory map. Keeping the default a no-op means a runner with no
/// SubPipeline steps requires no extra configuration.
///
/// Public because [`pipeline_needs_offload`] takes a resolver too, and a
/// caller reasoning about a pipeline it knows has no `kind = "sub-pipeline"`
/// step needs something to pass. Do NOT reach for it to silence that
/// argument on a pipeline that *might* have children — a no-op resolver
/// there reproduces exactly the blindness R555-B15 fixed.
pub struct NoopSubPipelineResolver;

impl crate::types::SubPipelineResolver for NoopSubPipelineResolver {
    fn resolve(&self, _target: &crate::types::SubPipelineRef) -> Option<Pipeline> {
        None
    }
}

/// A spawned background sidecar step (R513-F2, W207 Gap #4) being tracked by
/// [`PipelineRunner::run_inner`] until it is reaped — either when its
/// `background_until` gate step finishes or at the end of the step loop.
///
/// The `join` handle owns the running subprocess future; aborting it drops the
/// future, which drops the `tokio::process::Child` (spawned with
/// `kill_on_drop(true)`), which kills the process. That is the whole
/// reap-on-cancellation story: even an early `return` out of `run_inner` (a
/// foreground error, or the whole run future being cancelled by `qed.cancel`)
/// drops this Vec and tears down every live sidecar.
struct BackgroundTask {
    /// Index into `run_inner`'s `step_statuses` Vec for the placeholder
    /// `Running` row, finalized in place at reap.
    status_index: usize,
    /// Event index (with offset) for the deferred `StepFinished` emit.
    event_index: usize,
    name: String,
    /// The step indices this sidecar's `background_until` resolved to at
    /// preflight — every row of it, when it names a fanned-out matrix step
    /// (R605-F3). The reap fires once all of them are done, so a gate over N
    /// rows means "after the last row", not "after whichever finished first".
    ///
    /// Indices rather than the raw name because the name is ambiguous under
    /// matrix expansion, and because resolving once at preflight is what lets
    /// the reap be a set-membership check instead of a string compare per
    /// completed step. **Empty ⇒ no gate**: reap at the end of the step loop,
    /// which is what `background_until = None` has always meant.
    gate: Vec<usize>,
    join: tokio::task::JoinHandle<Result<(), RunnerError>>,
    /// R823-F2 — set when this sidecar is a participant dispatched to a fleet
    /// node instead of spawned here. `None` for every local sidecar, which is
    /// every sidecar that existed before participant sets.
    remote: Option<RemoteSidecar>,
}

/// What [`PipelineRunner::execute_step_remote`] hands back (R555-F6).
///
/// The step's outcome and the workload's existence are separate questions, and
/// collapsing them is what let every finished remote step leak. `Ok(forge_id) /
/// Err(_)` could only name the workload on the success leg, so a step that
/// dispatched and then exited non-zero was unreapable — the caller held an
/// error and no ident.
///
/// Same distinction [`RemoteSidecar::forge_id`] draws for participants: a
/// `None` here means dispatch never happened (no dispatcher wired, a spec the
/// runner refused, a source-context publish that failed), so there is nothing
/// on any node to tear down. That is meaningfully different from a run that
/// exists and failed, and only the second one owes a reap.
struct RemoteStepOutcome {
    /// `Some` from the moment `RemoteForgeDriver::start_with_context` returns.
    forge_id: Option<ObsForgeId>,
    result: Result<(), RunnerError>,
}

impl RemoteStepOutcome {
    /// The step never reached a worker, so there is no record to reap.
    fn never_dispatched(err: RunnerError) -> Self {
        Self { forge_id: None, result: Err(err) }
    }
}

/// R823-F2 — what reaping a *remote* participant sidecar needs that reaping a
/// local one does not.
///
/// The whole reason this type exists: [`BackgroundTask`]'s doc explains that
/// dropping the join handle kills a local sidecar, because the future owns a
/// `tokio::process::Child` spawned with `kill_on_drop(true)`. **None of that is
/// true across a host boundary.** Aborting the waiter drops a future that was
/// watching a log stream; the container on the node keeps running, keeps its
/// port bound, and keeps the machine. That is precisely the leak that makes
/// hardware CI read as flaky when it is actually holding hosts — so teardown
/// here is an explicit call, not a consequence of a drop.
struct RemoteSidecar {
    driver: Arc<RemoteForgeDriver>,
    /// The node this participant was pinned to. Carried purely so a teardown
    /// that did not settle can name the box to go look at — "check the node" is
    /// not an instruction anyone can follow (R823-T3).
    node: String,
    /// Written by the dispatch task the moment yubaba accepts the workload.
    ///
    /// Still `None` at reap ⇒ the dispatch never got far enough to create
    /// anything, so there is nothing on any node to tear down — which is a
    /// meaningfully different state from "we lost track of it", and the reason
    /// this is an `Option` behind the lock rather than a value handed over at
    /// spawn time. It cannot be handed over at spawn time regardless: the
    /// dispatch is a network round-trip and the scheduler loop that spawns
    /// sidecars must not block on one.
    forge_id: Arc<std::sync::Mutex<Option<ObsForgeId>>>,
}

/// Everything one finished step hands back to the R605-F3 scheduler.
///
/// With the step loop concurrent, a step's body can no longer reach into the
/// run-level accumulators as it goes — two steps folding into the same
/// `produced` Vec would land in completion order, which is nondeterministic.
/// So [`PipelineRunner::run_one_step`] returns its effects and the scheduler
/// applies them, keyed by [`Self::index`], in declaration order.
struct StepOutcome {
    /// Position in `pipeline.steps` — the scheduler's key for readiness, for
    /// the status row's slot, and for the produced-artifact fold.
    index: usize,
    /// Step name, for the `background_until` gate match and the named-output
    /// context.
    name: String,
    /// The step did LOCAL work, so the run's admission lane had to cover this
    /// host for its duration (see the lane note in the scheduler).
    was_local: bool,
    /// The [`QedStep::resource`](crate::types::QedStep::resource) key this step
    /// held, released back to the scheduler on completion.
    resource: Option<String>,
    /// Failed with `on_fail = abort` — stop admitting new steps.
    abort: bool,
    produced: Vec<ProducedArtifact>,
    outputs: std::collections::HashMap<String, String>,
    row: StepStatus,
}

/// Reap one background sidecar (R513-F2), returning its terminal status.
///
/// - Still running at reap → abort (kill) → [`RunStatus::Success`]: a healthy
///   sidecar torn down on schedule is the expected lifecycle, not a failure.
/// - Already exited on its own with code 0 → `Success`.
/// - Already exited non-zero (or panicked) → [`RunStatus::Failed`] with the
///   failure tail: a sidecar that dies mid-pipeline is a genuine problem.
///
/// R823-F2: a `remote` sidecar is torn down on the node **first**, and
/// unconditionally whenever a workload id exists — including when the waiter
/// has already finished. A redundant teardown of an exited workload costs one
/// no-op RPC; a skipped one costs a held machine, and the two are
/// indistinguishable from here because the waiter's terminal status and the
/// node's actual state can disagree (`Lost` means "we stopped being able to
/// see it", not "it stopped").
///
/// R823-T3, measured: the teardown result used to be dropped on the floor
/// (`let _ = …kill(&id).await`) on the reasoning that turning a best-effort
/// cleanup into a second failure would bury the first. Correct as far as the
/// *status* goes — and it made the one failure this feature exists to prevent
/// completely silent. On the fleet's shipped yubaba 0.8.28, `POST
/// /workloads/{ident}/destroy` answers `{"status":"destroyed"}` and leaves the
/// container RUNNING; three participant runs against a real node each reported
/// Success while leaving a responder alive on us-west-003 holding port 34500,
/// and the second run then went green against the *first* run's leak. So the
/// result is no longer discarded: the reap reports what teardown did without
/// changing the status it returns. Two ways it can be bad news —
///
/// - the `kill` RPC errored, or
/// - the RPC succeeded and the sidecar's waiter did not settle within
///   [`REMOTE_TEARDOWN_SETTLE`], which is what "answered destroyed, kept
///   running" looks like from here.
///
/// Neither turns the reap red. A held machine is an infrastructure fault and
/// the coordinator still owns the verdict (W235); making it *visible* is the
/// whole fix, and a note the operator can act on beats a red run they cannot
/// attribute.
///
async fn reap_background(
    mut join: tokio::task::JoinHandle<Result<(), RunnerError>>,
    remote: Option<RemoteSidecar>,
) -> SidecarReap {
    // R823-F2: a remote sidecar that never got a workload id was never accepted
    // by any node — nothing ran, as opposed to something running and failing.
    // Read structurally, from the absence of the id, rather than by matching on
    // the error text: it is the same fact the teardown below keys on, and it is
    // what lets the participant verdict say "fleet fault" instead of "the test
    // failed" (see `crate::participants::verdict`).
    let mut never_dispatched = false;
    let mut teardown_note = None;
    // R823-B4: set when the settle wait below observed the sidecar stop, which
    // also means it CONSUMED `join`. See the comment at that wait for why the
    // handle must not be awaited again afterwards.
    let mut settled_on_schedule = false;
    if let Some(sidecar) = remote {
        let forge_id = sidecar
            .forge_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        match forge_id {
            Some(id) => {
                // Whether the waiter had already resolved BEFORE the teardown
                // decides whether the settle check below means anything: a
                // sidecar that had already finished has nothing left to stop.
                let already_finished = join.is_finished();
                if let Err(e) = sidecar.driver.kill(&id).await {
                    teardown_note = Some(format!(
                        "teardown of participant workload forge.{id} on {} failed: {e} \
                         — it is probably still running there",
                        sidecar.node,
                    ));
                } else if !already_finished {
                    // Confirm the teardown rather than assume it. The status is
                    // deliberately NOT read off `join` here — after our own kill
                    // every terminal status is expected, and classifying a
                    // killed-on-schedule sidecar by its exit code would turn the
                    // healthy lifecycle red. All that is wanted is whether it
                    // stopped at all.
                    //
                    // R823-B4: the happy path here CONSUMES `join`.
                    // `timeout(_, &mut join)` polls the handle, and on success it
                    // polls it to completion — awaiting it again in `reap_status`
                    // below panics the runner with "JoinHandle polled after
                    // completion", taking the whole pipeline down AFTER the work
                    // and the teardown both succeeded.
                    //
                    // This is why it was never seen: while teardown was a silent
                    // no-op the sidecar never stopped, so this timeout ALWAYS
                    // elapsed, `join` was never polled to completion, and
                    // `reap_status` could take it. The first teardown that
                    // actually worked was the first time this branch was
                    // reachable — measured 2026-09-10, the run that proved the
                    // yubaba destroy-routing fix is the same run that panicked
                    // here. A leak in one layer was hiding a panic in the next.
                    if tokio::time::timeout(REMOTE_TEARDOWN_SETTLE, &mut join)
                        .await
                        .is_ok()
                    {
                        // Stopped on schedule. Deliberately NOT classified by the
                        // sidecar's exit status, for the reason stated above: we
                        // killed it, so every terminal status it can report is
                        // expected, and reading its exit code would paint a
                        // healthy lifecycle red.
                        settled_on_schedule = true;
                    } else {
                        teardown_note = Some(format!(
                            "participant workload forge.{id} did not stop within {}s of a \
                             teardown that reported success — it may still be RUNNING on \
                             {node}, holding its assigned port. Check {node} before the next \
                             run: a live leftover answers at the same address the next run's \
                             peers are handed, which is how a leaked participant makes a \
                             later run pass.",
                            REMOTE_TEARDOWN_SETTLE.as_secs(),
                            node = sidecar.node,
                        ));
                    }
                }
            }
            None => never_dispatched = true,
        }
    }
    // R823-B4: `join` is already spent when the settle wait resolved it, so this
    // must not touch it in that case.
    let (status, msg) = if settled_on_schedule {
        (RunStatus::Success, None)
    } else {
        reap_status(join).await
    };
    SidecarReap {
        status,
        msg,
        never_dispatched,
        teardown_note,
    }
}

/// How long [`reap_background`] waits for a remote sidecar's waiter to settle
/// after a teardown that reported success, before saying it did not stop.
///
/// Bounds the *observation*, not the teardown: `kill` has already returned by
/// the time this starts, so what is being waited on is the log stream closing
/// and the workload reaching a terminal status. Sized above kamaji's own
/// `TASK_REAP_TIMEOUT` (15s) so a node that is legitimately taking its full
/// reap budget is not reported as a leak.
const REMOTE_TEARDOWN_SETTLE: std::time::Duration = std::time::Duration::from_secs(20);

/// What reaping one sidecar established. R823-F2 split this out of a bare
/// tuple so `never_dispatched` — the fact that separates a fleet fault from a
/// test failure — travels with the status instead of being re-derived from a
/// message string somewhere downstream.
struct SidecarReap {
    status: RunStatus,
    msg: Option<String>,
    /// Only ever `true` for a remote participant sidecar whose workload was
    /// never accepted by a node. A local sidecar always started (or failed to
    /// spawn, which the executor reports as a real failure with a real reason).
    never_dispatched: bool,
    /// R823-T3 — what teardown did, when that is worth saying: the `kill` RPC
    /// errored, or it succeeded and the workload did not stop. Separate from
    /// [`msg`](Self::msg), which belongs to the sidecar's own outcome: a leaked
    /// participant is a fault of the fleet, not of the step, and folding the
    /// two would put infrastructure text in a step's `error` field.
    teardown_note: Option<String>,
}

/// R823-F2 — read each participant's outcome off the step rows its steps left
/// behind.
///
/// Pure, so the verdict rule is testable without a fleet — which matters more
/// here than usual, because the whole value of the participant set is what it
/// says when the fleet is *not* there.
///
/// `rows` is `run_inner`'s `main_statuses`: indexed by step, `None` for a step
/// the run never reached. The three outcomes come from three distinguishable
/// states, in this precedence:
///
/// 1. **Never started, structurally** — a sidecar in `never_dispatched` is one
///    no node ever accepted. Ranked first because it is the strongest evidence
///    available and it is about the fleet, not the code.
/// 2. **Never started, by absence** — every one of the participant's steps has
///    no row or a `Skipped` row. The run aborted before reaching it, or its
///    steps were gated off. Either way nothing of this participant executed, so
///    reporting `Completed` would be a lie by vacuous truth.
/// 3. **Failed / Completed** — ordinary step outcomes.
///
/// Known gap, stated rather than hidden: (1) is detected only for *background*
/// participant steps, because the workload-id evidence lives on the sidecar
/// reap path. A foreground participant step whose remote dispatch is refused
/// reports as `Failed`, which is a less precise diagnosis than it could be —
/// but it is the coordinator-shaped case, and a coordinator that cannot be
/// dispatched fails the run either way.
fn participant_reports(
    plan: &crate::participants::ParticipantPlan,
    steps: &[crate::types::QedStep],
    rows: &[Option<StepStatus>],
    never_dispatched: &std::collections::HashSet<usize>,
) -> Vec<crate::participants::ParticipantReport> {
    use crate::participants::{ParticipantOutcome, ParticipantReport};
    plan.participants()
        .iter()
        .map(|participant| {
            let mine: Vec<usize> = steps
                .iter()
                .enumerate()
                .filter(|(_, s)| s.participant.as_deref() == Some(participant.name.as_str()))
                .map(|(i, _)| i)
                .collect();

            let outcome = if let Some(&i) = mine.iter().find(|i| never_dispatched.contains(i)) {
                ParticipantOutcome::NeverStarted {
                    reason: format!(
                        "step `{}` was never accepted by node `{}`",
                        steps[i].name,
                        participant.node.as_deref().unwrap_or("local"),
                    ),
                }
            } else if mine.iter().all(|&i| {
                rows.get(i)
                    .and_then(|r| r.as_ref())
                    .is_none_or(|r| r.status == RunStatus::Skipped)
            }) {
                ParticipantOutcome::NeverStarted {
                    reason: format!(
                        "none of its {} step(s) executed ({})",
                        mine.len(),
                        mine.iter()
                            .map(|&i| steps[i].name.as_str())
                            .collect::<Vec<_>>()
                            .join(", "),
                    ),
                }
            } else if let Some(failed) = mine.iter().find_map(|&i| {
                rows.get(i)
                    .and_then(|r| r.as_ref())
                    .filter(|r| r.status == RunStatus::Failed)
                    .map(|r| (i, r))
            }) {
                let (i, row) = failed;
                ParticipantOutcome::Failed {
                    detail: format!(
                        "step `{}` failed: {}",
                        steps[i].name,
                        row.error.as_deref().unwrap_or("no reason recorded"),
                    ),
                }
            } else {
                ParticipantOutcome::Completed
            };

            ParticipantReport {
                name: participant.name.clone(),
                coordinator: participant.coordinator,
                outcome,
            }
        })
        .collect()
}

async fn reap_status(
    join: tokio::task::JoinHandle<Result<(), RunnerError>>,
) -> (RunStatus, Option<String>) {
    if join.is_finished() {
        match join.await {
            Ok(Ok(())) => (RunStatus::Success, None),
            Ok(Err(e)) => {
                let msg = match e {
                    RunnerError::StepFailed { msg, .. } => Some(msg),
                    RunnerError::InvalidConfig(m) => Some(m),
                    other => Some(other.to_string()),
                };
                (RunStatus::Failed, msg)
            }
            Err(join_err) => (
                RunStatus::Failed,
                Some(format!("background task panicked: {join_err}")),
            ),
        }
    } else {
        join.abort();
        let _ = join.await;
        (RunStatus::Success, None)
    }
}

/// @arch:see(R874)
impl PipelineRunner {
    /// Local execution — steps run as subprocesses on this machine.
    pub fn new(pipeline: Pipeline) -> Self {
        let run_id = Uuid::new_v4().to_string();
        Self {
            pipeline,
            run_id,
            remote_driver: None,
            build_context_publisher: None,
            run_where: RunWhere::Local,
            pinned_node: None,
            outcome_dispatcher: Arc::new(LoggingOutcomeDispatcher),
            events: None,
            camp_root: None,
            base_env: Vec::new(),
            signer: Arc::new(LoggingSigner),
            executor: Arc::new(LocalForgeDriver::new()),
            sub_pipeline_resolver: Arc::new(NoopSubPipelineResolver),
            suppress_publish_outcomes: false,
            parent_run_id: None,
            own_workspace: false,
            // Top-level runs have no factory until a host installs one via
            // `with_child_event_factory`; children inherit it below.
            child_event_factory: None,
            child_abort_hook: None,
            index_offset: 0,
            steps_are_subset: false,
            gha_matrix_subset: std::collections::HashMap::new(),
            include_stubbed: false,
            allow_emulate: false,
            force: false,
            matrix_coord: None,
            host_triple: crate::platform::detect_host_triple(),
            participant_plan: std::sync::OnceLock::new(),
            cross_availability: std::sync::OnceLock::new(),
            host_toolchains: std::sync::OnceLock::new(),
            provider_registry: Arc::new(crate::provider::ProviderRegistry::new()),
            secrets: Arc::new(crate::provider::MapSecrets::default()),
            git_ref: None,
            params: std::collections::HashMap::new(),
            positioned_workspace: std::sync::OnceLock::new(),
            resume_workspace: None,
            manual_gate: None,
            admission: None,
            admission_lane: None,
            cell: None,
        }
    }

    /// Local execution with a custom outcome dispatcher.
    pub fn new_with_dispatcher(pipeline: Pipeline, dispatcher: Arc<dyn OutcomeDispatcher>) -> Self {
        let run_id = Uuid::new_v4().to_string();
        Self {
            pipeline,
            run_id,
            remote_driver: None,
            build_context_publisher: None,
            run_where: RunWhere::Local,
            pinned_node: None,
            outcome_dispatcher: dispatcher,
            events: None,
            camp_root: None,
            base_env: Vec::new(),
            signer: Arc::new(LoggingSigner),
            executor: Arc::new(LocalForgeDriver::new()),
            sub_pipeline_resolver: Arc::new(NoopSubPipelineResolver),
            suppress_publish_outcomes: false,
            parent_run_id: None,
            own_workspace: false,
            // Top-level runs have no factory until a host installs one via
            // `with_child_event_factory`; children inherit it below.
            child_event_factory: None,
            child_abort_hook: None,
            index_offset: 0,
            steps_are_subset: false,
            gha_matrix_subset: std::collections::HashMap::new(),
            include_stubbed: false,
            allow_emulate: false,
            force: false,
            matrix_coord: None,
            host_triple: crate::platform::detect_host_triple(),
            participant_plan: std::sync::OnceLock::new(),
            cross_availability: std::sync::OnceLock::new(),
            host_toolchains: std::sync::OnceLock::new(),
            provider_registry: Arc::new(crate::provider::ProviderRegistry::new()),
            secrets: Arc::new(crate::provider::MapSecrets::default()),
            git_ref: None,
            params: std::collections::HashMap::new(),
            positioned_workspace: std::sync::OnceLock::new(),
            resume_workspace: None,
            manual_gate: None,
            admission: None,
            admission_lane: None,
            cell: None,
        }
    }

    /// Attach a live-event sink (R325-F2). Composes with any constructor:
    /// `PipelineRunner::new(p).with_events(tx)`. The runner emits a
    /// [`QedEvent`] for run start, each step start, every stdout/stderr line,
    /// each step finish, and run finish. Send failures (no receiver) are
    /// ignored — events are best-effort and never block the run.
    pub fn with_events(mut self, sink: UnboundedSender<QedEvent>) -> Self {
        self.events = Some(sink);
        self
    }

    /// Install the factory that gives each sub-pipeline child its own event
    /// channel (R768). Inherited by children, so nesting works at any depth.
    ///
    /// `with_events` covers only THIS run; a child gets `events: None` unless
    /// this is set, and a silent child discards the output of every local step
    /// it runs. The camp daemon supplies a factory that registers the child run
    /// and spawns the same drain a top-level run gets, which is what makes
    /// `qed.status <child_run_id>` resolve and `<child_run_id>.events.jsonl`
    /// exist. `yah qed run` without a daemon leaves it unset and keeps the old
    /// behaviour.
    pub fn with_child_event_factory(mut self, factory: ChildEventFactory) -> Self {
        self.child_event_factory = Some(factory);
        self
    }

    /// Install the hook that lets a host register a sub-pipeline child's
    /// `AbortHandle`, so a nested child can be cancelled independently of
    /// its ancestors. Composes with `with_child_event_factory`; inherited by
    /// children so a grandchild
    /// three levels deep (`release-wizard → release-check → check →
    /// cargo-test`) still gets its own independently-cancellable task.
    pub fn with_child_abort_hook(mut self, hook: ChildAbortHook) -> Self {
        self.child_abort_hook = Some(hook);
        self
    }

    /// Install the human surface for `kind = "manual"` steps (R622, W282).
    /// The camp daemon wires a gate backed by W111 forms; `yah qed run` leaves
    /// this unset and takes the headless path (advance-only). Inherited by
    /// sub-pipeline children.
    pub fn with_manual_gate(mut self, gate: Arc<dyn ManualGate>) -> Self {
        self.manual_gate = Some(gate);
        self
    }

    /// Install dynamic admission (R719-F7, W298): the runner hands its lane
    /// back across work that does not use it — an offloaded step, or a
    /// sub-pipeline child that belongs in a different lane — and retakes it
    /// before the next step that does. Inherited by sub-pipeline children.
    ///
    /// The camp daemon wires a control over its `qed_locks` map; `yah qed run`
    /// leaves this unset, which keeps the pre-F7 behaviour (one lane, held for
    /// the whole run, by whoever launched it).
    pub fn with_admission(mut self, admission: Arc<dyn AdmissionControl>) -> Self {
        self.admission = Some(admission);
        self
    }

    /// Declare what this run is *about* (R717-T3, W296): the doc + cell it was
    /// launched from and the subject its params resolved to. Stamped onto the
    /// terminal [`QedRunMeta::cell`].
    ///
    /// Build the fingerprint with
    /// [`param_fingerprint`](crate::types::param_fingerprint) over the **same
    /// resolved map** fed to `Pipeline::apply_params` — computing it from the
    /// supplied-only map would give a run that took a param's default a
    /// different subject than an identical run that passed it explicitly, and
    /// the two would render as separate histories of the same box.
    pub fn with_cell(mut self, cell: crate::types::CellRef) -> Self {
        self.cell = Some(cell);
        self
    }

    /// Override the [`OutcomeDispatcher`]. Composes with any constructor —
    /// notably [`new_remote`](Self::new_remote), which defaults to the
    /// log-only dispatcher, so a remote run can share the same publishing
    /// dispatcher the local in-process path uses (R590-F2).
    pub fn with_dispatcher(mut self, dispatcher: Arc<dyn OutcomeDispatcher>) -> Self {
        self.outcome_dispatcher = dispatcher;
        self
    }

    /// Override the camp root used to resolve per-camp catalog overrides and
    /// the BuildKit cache + OCI archive output directories. Production
    /// callers leave this unset (falls back to [`std::env::current_dir`]);
    /// tests pass a tempdir so generated `.yah/cache/` files don't leak into
    /// the workspace.
    pub fn with_camp_root(mut self, root: std::path::PathBuf) -> Self {
        self.camp_root = Some(root);
        self
    }

    /// Resume into a previously-retained `Isolated` worktree (R766) instead of
    /// positioning a fresh one. A vanished/missing path degrades to an
    /// ordinary fresh `Isolated` checkout rather than failing the run. No-op
    /// outside [`WorkspaceMode::Isolated`](crate::types::WorkspaceMode::Isolated).
    pub fn with_resume_workspace(mut self, path: std::path::PathBuf) -> Self {
        self.resume_workspace = Some(path);
        self
    }

    /// Environment to underlay beneath every locally-executed step's own `env`
    /// — R744-T2. See the [`base_env`](Self::base_env) field docs for what
    /// belongs here and, more importantly, what does not.
    ///
    /// A step's own `env` wins on a key collision: the recipe is closer to the
    /// work than the embedder is, and a pipeline that explicitly sets a key has
    /// said something the host should not quietly overrule.
    pub fn with_base_env(mut self, env: Vec<(String, String)>) -> Self {
        self.base_env = env;
        self
    }

    /// Set the target git ref for this run (W224, R330-B27) — a branch, tag,
    /// or commit SHA; `git checkout` / `git worktree add` accept any
    /// committish. Drives workspace positioning for `gha-workflow` steps per
    /// the pipeline's [`WorkspaceMode`](crate::types::WorkspaceMode). `None` /
    /// unset ⇒ `HEAD` (whatever is already checked out). Composes with any
    /// constructor.
    pub fn with_ref(mut self, r#ref: Option<String>) -> Self {
        self.git_ref = r#ref.filter(|r| !r.trim().is_empty());
        self
    }

    /// The run's effective target ref — the requested ref, or `HEAD` (build
    /// the commit that's already checked out; never silently jump to `main`,
    /// which would build the wrong bytes for a tag-triggered release).
    fn target_ref(&self) -> &str {
        self.git_ref.as_deref().unwrap_or("HEAD")
    }

    /// Attach a Sigstore signer (R407-T5). Composes with any constructor:
    /// `PipelineRunner::new(p).with_signer(Arc::new(CosignSigner::default()))`.
    /// Release pipelines MUST call this with a real signer; the default
    /// [`LoggingSigner`] writes placeholders so local `yah qed run` flows
    /// don't fail when cosign isn't on PATH.
    pub fn with_signer(mut self, signer: Arc<dyn SigstoreSigner>) -> Self {
        self.signer = signer;
        self
    }

    /// Attach the cross-host build-context transport (R636-B1).
    ///
    /// Required for any build-image step that offloads to a build-worker: the
    /// worker cannot bind-mount this host's camp root, so the context has to
    /// travel to it as bytes. Inert for host-local builds, which still shell
    /// straight to `docker buildx` against the on-disk directory.
    pub fn with_build_context_publisher(
        mut self,
        publisher: Arc<dyn crate::build_context::BuildContextPublisher>,
    ) -> Self {
        self.build_context_publisher = Some(publisher);
        self
    }

    /// On-demand runner of `status = "stubbed"` steps (R506). When `true`,
    /// the runner ignores the stubbed marker and runs the step normally.
    /// `enabled = false` is still honored regardless. Composes with any
    /// constructor: `PipelineRunner::new(p).with_include_stubbed(true)`.
    pub fn with_include_stubbed(mut self, include: bool) -> Self {
        self.include_stubbed = include;
        self
    }

    /// Confirm QEMU emulation for this run (R560, W236 — `yah qed run
    /// --allow-emulate`). Without it, [`Self::run_inner`] refuses to start a
    /// pipeline whose any step resolves to
    /// [`Emulate`](crate::platform::Resolution::Emulate). This is the "are you
    /// sure?" override — pass `true` only when a foreign-arch container build
    /// under emulation is genuinely intended. Composes with any constructor and
    /// is inherited by SubPipeline children.
    pub fn with_allow_emulate(mut self, allow: bool) -> Self {
        self.allow_emulate = allow;
        self
    }

    /// Carry the operator's `--force` into the run (R555-F11).
    ///
    /// Today it bypasses exactly one thing — [`Self::fleet_portability_gate`] —
    /// but the flag it mirrors is the W155 environment gate's, so the field is
    /// named for the flag rather than for the gate. Composes with any
    /// constructor and is inherited by SubPipeline children (a forced parent
    /// must not be re-refused by its own child pipeline).
    pub fn with_force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }

    /// Set the `--where` routing mode on a runner that already exists (R555-F11).
    ///
    /// The three constructors each bake a mode in, because each also wires the
    /// transport that mode needs. This setter exists for the one caller that
    /// wants the *question* without the transport: `yah qed preflight`, which
    /// answers "where would each step of this go, and could it survive the trip"
    /// while executing nothing. **It does not wire a dispatcher** — a run
    /// actually started this way still refuses at dispatch with "no remote
    /// dispatcher is wired", which is the correct outcome, not a gap.
    pub fn with_run_where(mut self, run_where: RunWhere) -> Self {
        self.run_where = run_where;
        self
    }

    /// Bind the runner to a matrix coordinate (R506). Set by the planner
    /// when fanning a pipeline over its `[matrix]` block — the coord shows
    /// up as `matrix.<key>` in `if=` expressions. Composes with any
    /// constructor.
    pub fn with_matrix_coord(mut self, coord: crate::matrix::MatrixCoord) -> Self {
        self.matrix_coord = Some(coord);
        self
    }

    /// Bind the run's resolved params (R653-F1) so steps can gate on
    /// `params.<name>` in `if=`. Pass the same map that was handed to
    /// [`Pipeline::apply_params`] — i.e. the output of
    /// [`Pipeline::resolve_params`], with declared defaults already filled in.
    ///
    /// Without this a run param can only be *substituted* into argv/env; with
    /// it a param can DECIDE whether a step runs, which is what makes a
    /// variable a build variant:
    ///
    /// ```toml
    /// [pipeline.params.variant]
    /// default = "quick"
    ///
    /// [[pipeline.steps]]
    /// name = "full-test-suite"
    /// if = "params.variant == 'full'"
    /// ```
    ///
    /// Composes with any constructor and is inherited (per-child, resolved
    /// against the child's own declarations) by sub-pipeline children.
    pub fn with_params(mut self, params: std::collections::HashMap<String, String>) -> Self {
        self.params = params;
        self
    }

    fn resolve_camp_root(&self) -> Result<std::path::PathBuf, RunnerError> {
        // Once a run has positioned its workspace (W224 R533-F11), every step
        // builds against that tree — for `Isolated` the throwaway worktree, for
        // `Checkout`/`Live` the (possibly ref-switched) camp root. This is
        // the single seam all step kinds share, so threading it here lifts
        // positioning from the gha-workflow step to the whole run.
        if let Some(ws) = self.positioned_workspace.get() {
            return Ok(ws.clone());
        }
        if let Some(root) = &self.camp_root {
            return Ok(root.clone());
        }
        std::env::current_dir()
            .map_err(|e| RunnerError::InvalidConfig(format!("failed to read current dir: {e}")))
    }

    /// The unpositioned camp root — `self.camp_root` (or the current dir),
    /// *ignoring* any positioned workspace. Used by [`Self::run_inner`] to feed
    /// [`Self::prepare_workspace`] the base tree to position from, before the
    /// positioned workspace is set.
    fn base_camp_root(&self) -> Result<std::path::PathBuf, RunnerError> {
        if let Some(root) = &self.camp_root {
            return Ok(root.clone());
        }
        std::env::current_dir()
            .map_err(|e| RunnerError::InvalidConfig(format!("failed to read current dir: {e}")))
    }

    /// Position the on-disk tree this *run* builds against, per the pipeline's
    /// [`WorkspaceMode`] and the run's target ref (W224, R330-B27). Called once
    /// at run start (R533-F11) — every step kind (subprocess, build-image, sign,
    /// sub-pipeline, gha-workflow) then builds from the returned tree, so an
    /// `Isolated` release positions the whole run into one worktree rather than
    /// only its gha-workflow step.
    ///
    /// Returns the effective workspace path plus an optional RAII
    /// [`WorktreeGuard`] — held by the caller for the lifetime of the *run* so
    /// an `Isolated` worktree outlives every step and is torn down once the run
    /// finishes (even on a mid-run error). The dirty check considers tracked
    /// modifications only (`--untracked-files=no`): untracked files don't change
    /// which committed bytes a build sees and would otherwise block every run in
    /// a working camp.
    fn prepare_workspace(
        &self,
        camp_root: &std::path::Path,
    ) -> Result<(std::path::PathBuf, Option<WorktreeGuard>), RunnerError> {
        let git_ref = self.target_ref();
        match self.pipeline.workspace {
            // Build whatever is on disk — no ref switch, no dirty check.
            WorkspaceMode::Live => Ok((camp_root.to_path_buf(), None)),
            // Switch the camp root to the ref, but never over local edits.
            WorkspaceMode::Checkout => {
                if git_tree_is_dirty(camp_root)? {
                    return Err(RunnerError::InvalidConfig(format!(
                        "workspace mode `checkout` won't run over uncommitted changes in {} — \
                         commit or stash them, or set the pipeline to `workspace = \"isolated\"` \
                         (build in a throwaway worktree) or `\"live\"` (build the tree as-is)",
                        camp_root.display()
                    )));
                }
                run_git(camp_root, &["checkout", git_ref]).map_err(|e| {
                    RunnerError::InvalidConfig(format!("git checkout {git_ref}: {e}"))
                })?;
                Ok((camp_root.to_path_buf(), None))
            }
            // Build in a dedicated worktree at the ref; camp root untouched.
            WorkspaceMode::Isolated => {
                // R766: re-enter a retained worktree from a failed run rather
                // than positioning a fresh one — the whole point of resuming
                // is that steps 1..N-1's mutations are still on disk there.
                // `git worktree add` is skipped entirely: running it again
                // would reset the tree to a clean checkout of `git_ref`,
                // destroying exactly the state we're re-entering for. A
                // vanished path (evicted by retention, or a source run that
                // predates this field) degrades to an ordinary fresh
                // `Isolated` run below rather than failing the resume.
                //
                // R874-B2: the SOURCE run's meta keeps tracking this path for
                // the whole resume, so the daemon's retention sweep would
                // otherwise evict it out from under the live build. The lock
                // is what tells the sweep a run is in here — its liveness
                // check is by run id and the resume has a new one. A lock that
                // will not take means another run (a concurrent resume of the
                // same source) already holds the tree; sharing it would be two
                // builds in one directory, so position a fresh one instead.
                if let Some(existing) = self.resume_workspace.as_ref() {
                    if existing.is_dir() {
                        match lock_worktree(camp_root, existing, &self.run_id) {
                            // Re-checked under the lock: a sweep whose remove
                            // started before the lock landed may have taken it.
                            Ok(()) if existing.is_dir() => {
                                tracing::info!(
                                    path = %existing.display(),
                                    "qed: resuming isolated run into its retained worktree"
                                );
                                let guard = WorktreeGuard {
                                    camp_root: camp_root.to_path_buf(),
                                    worktree: existing.clone(),
                                    retain: std::cell::Cell::new(false),
                                    locked: true,
                                };
                                return Ok((existing.clone(), Some(guard)));
                            }
                            Ok(()) => {
                                let _ = run_git(
                                    camp_root,
                                    &["worktree", "unlock", &existing.to_string_lossy()],
                                );
                                tracing::warn!(
                                    path = %existing.display(),
                                    "qed: retained worktree was removed while it was being locked; positioning a fresh one"
                                );
                            }
                            Err(e) => tracing::warn!(
                                path = %existing.display(),
                                error = %e,
                                "qed: could not lock the retained worktree for resume (held by another run?); positioning a fresh one"
                            ),
                        }
                    } else {
                        tracing::warn!(
                            path = %existing.display(),
                            "qed: resume requested a retained worktree that no longer exists on disk; positioning a fresh one"
                        );
                    }
                }
                let worktree = std::env::temp_dir().join(format!("qed-worktree-{}", self.run_id));
                // A prior crashed run may have left this path registered; clear
                // it first so `worktree add` doesn't fail on a stale entry.
                let _ = std::process::Command::new("git")
                    .current_dir(camp_root)
                    .args(["worktree", "remove", "--force"])
                    .arg(&worktree)
                    .output();
                run_git(
                    camp_root,
                    &["worktree", "add", "--force", &worktree.to_string_lossy(), git_ref],
                )
                .map_err(|e| {
                    RunnerError::InvalidConfig(format!("git worktree add at {git_ref}: {e}"))
                })?;
                // R874-B2: a fresh worktree is locked for the same reason a
                // resumed one is — it is how a prune in another process (a
                // daemonless `yah qed prune`, which has no live run map) sees
                // that a build is in here. Unlike the resume, nothing else can
                // hold this brand-new path, so a failed lock degrades to an
                // unlocked build rather than failing the run.
                let locked = match lock_worktree(camp_root, &worktree, &self.run_id) {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!(
                            path = %worktree.display(),
                            error = %e,
                            "qed: could not lock the isolated worktree; building in it unlocked"
                        );
                        false
                    }
                };
                let guard = WorktreeGuard {
                    camp_root: camp_root.to_path_buf(),
                    worktree: worktree.clone(),
                    retain: std::cell::Cell::new(false),
                    locked,
                };
                Ok((worktree, Some(guard)))
            }
        }
    }

    /// W209: evaluate every `[[bind]]` in the pipeline whose `from`
    /// references this step's outputs, write the accepted values into the
    /// source tree, and return the per-bind result list for surfacing in
    /// [`StepStatus::applied_binds`].
    ///
    /// Build → checkin → release inversion in mechanical form: the source
    /// tree IS the step-to-step plumbing. Downstream steps will read these
    /// values from disk like any other tool would.
    ///
    /// Failures are logged at `warn` and surfaced as an empty result list
    /// rather than poisoning the run. Per W209 § Safety the diff is the
    /// review surface; an applier crash on one file doesn't justify
    /// killing the pipeline (the operator can still inspect what landed
    /// and what didn't via `git status`).
    fn apply_step_binds(
        &self,
        step: &QedStep,
        step_outputs: &std::collections::HashMap<String, String>,
    ) -> Vec<manifest_bind::AppliedBind> {
        // Cheap pre-filter so we don't even touch the filesystem when
        // nothing in this pipeline binds against this step.
        let any_match = self.pipeline.binds.iter().any(|b| match &b.from {
            manifest_bind::OutputRef::StepOutput { step: s, .. } => s == &step.name,
            manifest_bind::OutputRef::Uri(_) => false,
        });
        if !any_match {
            return Vec::new();
        }

        let workspace_root = match self.resolve_camp_root() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    step = %step.name,
                    error = %e,
                    "skipping [[bind]] application: cannot resolve workspace root",
                );
                return Vec::new();
            }
        };

        // Build a single-step OutputMap. Each declared output carries its
        // typed shape; undeclared keys default to `String` (matches
        // OutputDecl::kind's serde default) so back-compat steps from
        // R488-F4 still flow through — the per-bind type check stays the
        // hard boundary.
        let mut outputs = manifest_bind::OutputMap::new();
        for (key, raw) in step_outputs {
            let kind = step
                .outputs
                .iter()
                .find(|o| &o.name == key)
                .map(|o| o.kind)
                .unwrap_or(manifest_bind::ValueType::String);
            outputs.insert(
                step.name.clone(),
                key.clone(),
                manifest_bind::OutputValue::new(kind, raw.clone()),
            );
        }

        // Scope to binds that fire from this step. apply_binds itself
        // already filters by `outputs.lookup(&bind.from).is_some()`, but
        // doing it here avoids touching files that bind only from other
        // steps and keeps the AppliedBind list scoped to the step that
        // caused the writes.
        let relevant: Vec<manifest_bind::BindSpec> = self
            .pipeline
            .binds
            .iter()
            .filter(|b| {
                matches!(
                    &b.from,
                    manifest_bind::OutputRef::StepOutput { step: s, .. } if s == &step.name
                )
            })
            .cloned()
            .collect();

        match manifest_bind::apply_binds(&outputs, &relevant, &workspace_root) {
            Ok(applied) => {
                for a in &applied {
                    if a.changed {
                        tracing::info!(
                            step = %step.name,
                            file = %a.file.display(),
                            path = %a.path,
                            from = %a.from,
                            "bound output → manifest (changed)",
                        );
                    }
                }
                // W209/R510-F6: fire hash-change hooks after the bind
                // transaction has committed, for binds that actually changed.
                self.fire_change_hooks(&step.name, &applied, &workspace_root);
                applied
            }
            Err(e) => {
                tracing::warn!(
                    step = %step.name,
                    error = %e,
                    "manifest-bind apply failed; downstream steps will read pre-bind values",
                );
                Vec::new()
            }
        }
    }

    /// W209/R510-F6: evaluate every `[[on_change]]` hook against the binds
    /// this step just committed and perform each matching hook's side effect.
    /// Only binds that actually changed bytes fire (the no-op idempotency
    /// guarantee lives in [`manifest_bind::fired_hooks`]). `journal` / `event`
    /// actions commit to disk inside `dispatch_hook`; `pipeline` actions are
    /// surfaced as a logged request — v1 does not auto-cascade pipelines (the
    /// reserved `rebind_stop` guard is the design's bound on cascade storms),
    /// so the operator enqueues the downstream pipeline explicitly.
    ///
    /// A hook dispatch failure is logged at `warn` and never poisons the run,
    /// mirroring the bind applier's own failure stance (W209 § Safety): the
    /// in-tree bind result is the source of truth; the hook is a downstream
    /// side effect.
    fn fire_change_hooks(
        &self,
        step_name: &str,
        applied: &[manifest_bind::AppliedBind],
        workspace_root: &std::path::Path,
    ) {
        if self.pipeline.on_change.is_empty() {
            return;
        }
        for fired in manifest_bind::fired_hooks(&self.pipeline.on_change, applied) {
            match manifest_bind::dispatch_hook(&fired, workspace_root) {
                Ok(manifest_bind::HookOutcome::Journaled { file }) => tracing::info!(
                    step = %step_name,
                    bind = %fired.bind,
                    journal = %file.display(),
                    "on_change: appended journal line",
                ),
                Ok(manifest_bind::HookOutcome::EventEmitted { file, kind }) => tracing::info!(
                    step = %step_name,
                    bind = %fired.bind,
                    event = %kind,
                    sink = %file.display(),
                    "on_change: emitted event",
                ),
                Ok(manifest_bind::HookOutcome::PipelineRequested { pipeline, params }) => {
                    tracing::info!(
                        step = %step_name,
                        bind = %fired.bind,
                        pipeline = %pipeline,
                        params = ?params,
                        "on_change: pipeline requested (v1 does not auto-cascade — \
                         operator enqueues `yah qed run` explicitly)",
                    )
                }
                Err(e) => tracing::warn!(
                    step = %step_name,
                    bind = %fired.bind,
                    error = %e,
                    "on_change: hook dispatch failed (bind result stands; hook skipped)",
                ),
            }
        }
    }

    /// R506: determine whether a step should be skipped, and why. Returns
    /// `Some(human-readable reason)` to skip, `None` to dispatch normally.
    ///
    /// Precedence (declarative gates run before runtime ones, since they
    /// can't observe step outputs):
    ///   1. `enabled = false` — always wins, even when `include_stubbed`.
    ///   2. `activation = "stubbed"` and `!include_stubbed`.
    ///   3. `if = "<expr>"` evaluates to a falsy value against the W201-F4
    ///      context (matrix coord + accumulated step outputs + env).
    ///
    /// An `if` expression that fails to parse is treated as falsy with a
    /// descriptive reason so the dashboard surfaces the syntax error rather
    /// than the runner crashing the whole pipeline mid-run.
    fn resolve_skip_reason(
        &self,
        step: &crate::types::QedStep,
        step_context: &std::collections::HashMap<String, std::collections::HashMap<String, String>>,
        running_status: RunStatus,
    ) -> Option<String> {
        if !step.enabled {
            return Some("skipped: enabled = false".to_string());
        }
        if matches!(step.activation, StepActivation::Stubbed) && !self.include_stubbed {
            return Some(
                "skipped: status = \"stubbed\" (pass --include-stubbed to run anyway)".to_string(),
            );
        }
        if let Some(raw) = step.if_cond.as_deref() {
            let body = strip_expr_delimiters(raw);
            let ctx = self.build_expr_context(step_context, running_status);
            return match yah_qed_gha::evaluate(body, &ctx) {
                Ok(v) if v.is_truthy() => None,
                Ok(_) => Some(format!("skipped: if = \"{raw}\" evaluated falsy")),
                Err(e) => Some(format!("skipped: if = \"{raw}\" parse error: {e}")),
            };
        }
        None
    }

    /// Map the runner's running aggregate [`RunStatus`] onto the GHA-shaped
    /// [`yah_qed_gha::JobStatus`] consumed by `success()`/`failure()`/`always()`/
    /// `cancelled()` context functions. The runner has no mid-flight
    /// `Cancelled` state (cancel arrives via the abort handle and aborts the
    /// whole future), so only `Success` and `Failure` are reachable here —
    /// `cancelled()` therefore always evaluates to false from inside a step's
    /// `if=` expression, matching GHA semantics where a cancelled job never
    /// reaches the next step's gate.
    fn running_job_status(status: RunStatus) -> yah_qed_gha::JobStatus {
        match status {
            RunStatus::Failed => yah_qed_gha::JobStatus::Failure,
            _ => yah_qed_gha::JobStatus::Success,
        }
    }

    /// Build the [`yah_qed_gha::Context`] passed to `if=` evaluation. Populates:
    ///   - `matrix` from [`Self::matrix_coord`]
    ///   - `steps.<name>.outputs.<key>` from the accumulated step context
    ///   - `env` from the current process environment
    ///   - `params` from [`Self::params`] — the run's resolved params (R653-F1),
    ///     registered as a host namespace on the GHA context (GHA has no
    ///     `params`; `inputs` is workflow_dispatch's, not qed's)
    ///   - `job_status` from the cumulative `RunStatus` so
    ///     `success()`/`failure()`/`always()`/`cancelled()` reflect the
    ///     running aggregate at the moment this step is gated
    fn build_expr_context(
        &self,
        step_context: &std::collections::HashMap<String, std::collections::HashMap<String, String>>,
        running_status: RunStatus,
    ) -> yah_qed_gha::Context<'static> {
        use indexmap::IndexMap;
        let mut ctx = yah_qed_gha::Context::new();

        // env: process env
        let mut env_obj: IndexMap<String, yah_qed_gha::Value> = IndexMap::new();
        for (k, v) in std::env::vars() {
            env_obj.insert(k, yah_qed_gha::Value::String(v));
        }
        ctx.env = yah_qed_gha::Value::Object(env_obj);

        // matrix: from runner coord (None → leave as None so matrix.<key> → Null)
        if let Some(coord) = &self.matrix_coord {
            let mut m: IndexMap<String, yah_qed_gha::Value> = IndexMap::new();
            for (k, v) in coord {
                m.insert(
                    k.clone(),
                    yah_qed_gha::Value::String(crate::matrix::toml_value_to_str(v)),
                );
            }
            ctx.matrix = Some(yah_qed_gha::Value::Object(m));
        }

        // steps.<name>.outputs.<key>
        let mut steps_obj: IndexMap<String, yah_qed_gha::Value> = IndexMap::new();
        for (name, outputs) in step_context {
            let mut out_map: IndexMap<String, yah_qed_gha::Value> = IndexMap::new();
            for (k, v) in outputs {
                out_map.insert(k.clone(), yah_qed_gha::Value::String(v.clone()));
            }
            let mut step_obj: IndexMap<String, yah_qed_gha::Value> = IndexMap::new();
            step_obj.insert("outputs".to_string(), yah_qed_gha::Value::Object(out_map));
            steps_obj.insert(name.clone(), yah_qed_gha::Value::Object(step_obj));
        }
        ctx.steps = yah_qed_gha::Value::Object(steps_obj);

        // params.<name>: the run's resolved params. Always registered, even
        // when empty — an absent key resolves to Null (falsy), same as an
        // unknown `env.` or `matrix.` key, so `if = "params.variant == 'full'"`
        // on a run that supplied no `variant` skips rather than erroring.
        let mut params_obj: IndexMap<String, yah_qed_gha::Value> = IndexMap::new();
        for (k, v) in &self.params {
            params_obj.insert(k.clone(), yah_qed_gha::Value::String(v.clone()));
        }
        ctx = ctx.with_namespace("params", yah_qed_gha::Value::Object(params_obj));

        ctx.job_status = Some(Self::running_job_status(running_status));

        ctx
    }

    /// Emit one event to the sink if attached. A closed receiver is a no-op.
    fn emit(&self, event: QedEvent) {
        if let Some(tx) = &self.events {
            let _ = tx.send(event);
        }
    }

    /// R823-T3 — report what a remote sidecar's teardown actually did.
    ///
    /// Sent as the step's own stderr rather than as a new event variant: it
    /// then travels every rail a step's output already travels (the CLI's live
    /// sink, the desktop pane, scryer) with no schema change, and it lands
    /// attached to the participant it is about. A leaked machine is worth one
    /// loud line on a run that is otherwise green — which is exactly the run it
    /// will happen on, since teardown runs after the coordinator has already
    /// passed.
    fn emit_teardown_note(&self, event_index: usize, step: &str, note: String) {
        self.emit(QedEvent::StepOutput {
            index: event_index,
            name: step.to_string(),
            stream: OutputStream::Stderr,
            line: format!("qed: teardown: {note}"),
        });
    }

    /// Pick the sandboxing runtime for a step.  Explicit `step.runtime`
    /// always wins; otherwise default by location (R380-T3):
    ///
    /// | --where  | runtime |
    /// |----------|---------|
    /// | local    | Native    |
    /// | remote   | Container |
    ///
    /// The CLI's `--runtime native|container` override is applied by mutating
    /// each step's `runtime` field *before* the runner is constructed, so by
    /// the time this method runs the per-step value already reflects the
    /// CLI choice (TOML-declared values still win over CLI defaults).
    fn resolve_runtime(&self, step: &crate::types::QedStep) -> TaskRuntime {
        // build-image steps are always Container — parse-time validation
        // already rejects explicit `runtime = "native"`, this catches the
        // implicit `runtime = None` case where the local default would
        // otherwise resolve to Native.
        if matches!(step.kind, crate::types::StepKind::BuildImage) {
            return TaskRuntime::Container;
        }
        // package-native-tarball is always Native — it's pure host file I/O
        // (read binary, write tar.gz). Parse-time rejects `runtime =
        // "container"`; force Native here so the implicit `None` doesn't
        // resolve to Container on a Remote runner.
        if matches!(step.kind, crate::types::StepKind::PackageNativeTarball) {
            return TaskRuntime::Native;
        }
        // musl-static-preflight shells `cargo metadata` on the host — same
        // reasoning as package-native-tarball, always Native.
        if matches!(step.kind, crate::types::StepKind::MuslStaticPreflight) {
            return TaskRuntime::Native;
        }
        // sign-native-tarball shells `cosign sign-blob` on the host (and
        // writes the .sig/.crt/.bundle next to the artifact). Parse-time
        // rejects `runtime = "container"`; force Native here so the implicit
        // `None` doesn't resolve to Container on a Remote runner.
        if matches!(step.kind, crate::types::StepKind::SignNativeTarball) {
            return TaskRuntime::Native;
        }
        // manifest-stitch shells `docker buildx imagetools create` on the host —
        // a registry-only op (R590-F2). It runs where qed runs even under
        // `--where=remote` (the per-arch builds fan out to the fleet; the stitch
        // does not), so force Native so the implicit `None` doesn't resolve to
        // Container on a Remote runner.
        if matches!(step.kind, crate::types::StepKind::ManifestStitch) {
            return TaskRuntime::Native;
        }
        // A manual step parks on a human at the qed host and evaluates
        // `advance` in the positioned workspace on that same host — a container
        // has neither (R622). Parse-time rejects `runtime = "container"`; force
        // Native so the implicit `None` doesn't resolve to Container on a
        // Remote runner.
        if matches!(step.kind, crate::types::StepKind::Manual) {
            return TaskRuntime::Native;
        }
        // R590-F4: default runtime follows the step's *effective* placement, not
        // the raw run_where — an Auto runner that offloads a step to the fleet
        // must default it to Container (it runs remote), while its local steps
        // stay Native. For a forced Local/Remote runner effective_placement is a
        // constant, so this is byte-identical to the pre-F4 default.
        step.runtime.unwrap_or(match self.effective_placement(step) {
            RunWhere::Remote if self.remote_step_needs_native_userland(step) => TaskRuntime::Native,
            RunWhere::Remote => TaskRuntime::Container,
            // Local, and the unreachable Auto (effective_placement resolves it).
            RunWhere::Local | RunWhere::Auto => TaskRuntime::Native,
        })
    }

    /// Whether an offloaded step must run on the build-worker's own userland
    /// rather than in a container on it (R577-T1 / W254).
    ///
    /// `Remote ⇒ Container` was unconditional before this, and for the Linux
    /// fleet it is right: a Linux build is happy in a Linux container, which is
    /// why the x86 offload leg works. But the container a build-worker can
    /// offer is *always* a Linux container — that is what "you cannot
    /// containerize the Darwin kernel" means once it reaches the dispatch
    /// layer. So an `aarch64-apple-darwin` step offloaded to `us-west-015`
    /// would be handed to a Linux container inside Colima, where `cargo tauri
    /// build`, `codesign` and `xcrun notarytool` cannot run at all.
    ///
    /// The discriminator is therefore the **target's OS**, not `native` alone:
    ///
    /// - `native = true` is necessary — it is the flag that already means "real
    ///   silicon, real userland", and it is what routed the step to a matching
    ///   worker in the first place ([`resolve_placement`]).
    /// - The target OS must be known *and not Linux*. `rusty-v8-musl` is
    ///   `native = true` on a Linux target and keeps its container; that leg is
    ///   proven live on `us-west-002` and this change must not disturb it. An
    ///   unrecognized OS token fails closed to Container — the same both-known
    ///   guard [`foreign_os`](crate::platform) applies one layer up.
    /// - A declared `container_platform` opts back out: the step is explicitly
    ///   asking for a container image, and it carries its own userland.
    ///
    /// An explicit `runtime` in the recipe still wins over all of this — this
    /// only picks the *default*.
    fn remote_step_needs_native_userland(&self, step: &crate::types::QedStep) -> bool {
        let Some(platform) = step.platform.as_ref() else {
            return false;
        };
        if !platform.native || platform.container_platform.is_some() {
            return false;
        }
        let Some(target) = self.step_platform(step).target else {
            return false;
        };
        !matches!(
            crate::platform::os_tag_of(&target),
            "linux" | "unknown"
        )
    }

    /// Remote execution — steps run as `task::remote` workloads dispatched via
    /// the provided `WardenClient`.
    pub fn new_remote(
        pipeline: Pipeline,
        scryer: Arc<Scryer>,
        yubaba: Arc<dyn WardenClient>,
    ) -> Self {
        let run_id = Uuid::new_v4().to_string();
        let remote_driver = Arc::new(RemoteForgeDriver::new(scryer, yubaba));
        Self {
            pipeline,
            run_id,
            remote_driver: Some(remote_driver),
            build_context_publisher: None,
            run_where: RunWhere::Remote,
            pinned_node: None,
            outcome_dispatcher: Arc::new(LoggingOutcomeDispatcher),
            events: None,
            camp_root: None,
            base_env: Vec::new(),
            signer: Arc::new(LoggingSigner),
            executor: Arc::new(LocalForgeDriver::new()),
            sub_pipeline_resolver: Arc::new(NoopSubPipelineResolver),
            suppress_publish_outcomes: false,
            parent_run_id: None,
            own_workspace: false,
            // Top-level runs have no factory until a host installs one via
            // `with_child_event_factory`; children inherit it below.
            child_event_factory: None,
            child_abort_hook: None,
            index_offset: 0,
            steps_are_subset: false,
            gha_matrix_subset: std::collections::HashMap::new(),
            include_stubbed: false,
            allow_emulate: false,
            force: false,
            matrix_coord: None,
            host_triple: crate::platform::detect_host_triple(),
            participant_plan: std::sync::OnceLock::new(),
            cross_availability: std::sync::OnceLock::new(),
            host_toolchains: std::sync::OnceLock::new(),
            provider_registry: Arc::new(crate::provider::ProviderRegistry::new()),
            secrets: Arc::new(crate::provider::MapSecrets::default()),
            git_ref: None,
            params: std::collections::HashMap::new(),
            positioned_workspace: std::sync::OnceLock::new(),
            resume_workspace: None,
            manual_gate: None,
            admission: None,
            admission_lane: None,
            cell: None,
        }
    }

    /// Policy-derived execution (R590-F4, the default `yah qed run` mode). Like
    /// [`new_remote`](Self::new_remote) it wires a fleet dispatcher, but leaves
    /// placement on [`RunWhere::Auto`]: local steps run as local subprocesses and
    /// only a `native = true` cross-arch step (resolving to
    /// [`Offload`](crate::platform::Resolution::Offload)) is dispatched to an
    /// arch-matched build-worker — no `--where=remote` flag required. The CLI
    /// stands this up only when the pipeline actually needs offload (see
    /// [`pipeline_needs_offload`]); a pipeline with no offload step stays on the
    /// driverless local path.
    pub fn new_auto(
        pipeline: Pipeline,
        scryer: Arc<Scryer>,
        yubaba: Arc<dyn WardenClient>,
    ) -> Self {
        let mut runner = Self::new_remote(pipeline, scryer, yubaba);
        runner.run_where = RunWhere::Auto;
        runner
    }

    /// R833-F8: pin every remotely-placed step of this run to one **named**
    /// fleet node (`--where=node:us-west-003`).
    ///
    /// The imperative half of placement. Until this, a target could only be
    /// *inferred* — `RemoteAny` plus the arch/OS mesh tags derived from a
    /// cross-arch step, resolved by declaration order in `.yah/infra/machines/`
    /// — so there was no way to say "run it on that box", and no way to use a
    /// build node the inference did not happen to elect. This composes with
    /// either constructor:
    ///
    /// - with [`new_remote`](Self::new_remote): every step runs on `node`.
    /// - with [`new_auto`](Self::new_auto): inference still decides *which*
    ///   steps leave this host, and the ones that do all land on `node`.
    ///
    /// **Explicit beats inferred**: a pinned run stops emitting the mesh-tag
    /// node-selector altogether (see
    /// [`remote_location`](Self::remote_location)), so the operator's node is
    /// not silently filtered back out by an arch tag derived from the step.
    /// Whether that node can actually serve the step is then a live admission
    /// answer — a refusal naming the node — rather than a quiet re-route.
    pub fn with_pinned_node(mut self, node: workload_spec::MeshIdent) -> Self {
        self.pinned_node = Some(node);
        self
    }

    /// The [`TaskLocation`] a remotely-placed step dispatches to: the pinned
    /// node when the operator named one (R833-F8), otherwise the R594
    /// tag-matched `RemoteAny` the caller derived from the step.
    ///
    /// Single seam for both remote dispatch sites (subprocess and build-image)
    /// so a pin cannot be honoured by one and dropped by the other.
    fn remote_location(&self, mesh_tags: Vec<String>) -> TaskLocation {
        match &self.pinned_node {
            Some(node) => TaskLocation::Remote { node: node.clone() },
            None => TaskLocation::RemoteAny {
                tier: TierTag("infra".into()),
                mesh_tags,
            },
        }
    }

    /// Attach a custom [`ForgeExecutor`] for local subprocess steps
    /// (R438-T14). Composes with any constructor. The default is
    /// [`LocalForgeDriver`]; callers override to share a configured driver
    /// across multiple runs.
    pub fn with_executor(mut self, executor: Arc<dyn ForgeExecutor>) -> Self {
        self.executor = executor;
        self
    }

    /// Attach a [`SubPipelineResolver`](crate::types::SubPipelineResolver)
    /// for `kind = "sub-pipeline"` steps (R488-F2). Composes with any
    /// constructor. The default resolver returns `None` for every target —
    /// any SubPipeline step will fail with a clear "no resolver configured"
    /// message until this is called. Production callers pass a
    /// [`PipelineLoader`](crate::config::PipelineLoader)-backed resolver;
    /// tests pass an in-memory map.
    pub fn with_sub_pipeline_resolver(
        mut self,
        resolver: Arc<dyn crate::types::SubPipelineResolver + Send + Sync>,
    ) -> Self {
        self.sub_pipeline_resolver = resolver;
        self
    }

    /// Wire the vendor release-provider registry + credential source (R509)
    /// used to dispatch [`Outcome::Provider`] outcomes (notarize, authenticode,
    /// sparkle, …). Composes with any constructor and is inherited by
    /// SubPipeline children. The defaults are an empty registry + empty
    /// secrets, so a pipeline with no vendor outcomes needs no wiring; a
    /// pipeline that *does* declare one fails with a typed unknown-provider
    /// error until this is called with a populated registry
    /// ([`crate::provider::ProviderRegistry::production`]).
    pub fn with_release_providers(
        mut self,
        registry: Arc<crate::provider::ProviderRegistry>,
        secrets: Arc<dyn crate::provider::SecretSource>,
    ) -> Self {
        self.provider_registry = registry;
        self.secrets = secrets;
        self
    }

    /// Declare how the step list this runner was handed relates to the
    /// pipeline as AUTHORED. Call it from anywhere that trims `pipeline.steps`
    /// before construction; the default (never calling it) means "complete".
    ///
    /// - `resumed_from` — how many leading steps a resume-from-step run
    ///   `drain(0..n)`-ed off. Added to every emitted step `index` so events
    ///   still carry the original positions (step 5 of 6, not step 0 of 1).
    /// - `subset` — whether a name-subset selection (`selected_steps`) removed
    ///   arbitrary steps. Carries no index consequence; it exists because
    ///   "these steps are not all the steps" is a fact no offset can express.
    ///
    /// R906-F2 widened this from the old `with_index_offset(usize)`. The two
    /// facts travel together because they answer one question —
    /// [`Self::steps_are_complete`] — and a caller that trimmed the list must
    /// not be able to report the half of it that affects UI indices while
    /// silently omitting the half that affects whether an operator gate can be
    /// shown to be first.
    pub fn with_step_selection(mut self, resumed_from: usize, subset: bool) -> Self {
        self.index_offset = resumed_from;
        self.steps_are_subset = subset;
        self
    }

    /// Whether [`Self::pipeline`]'s steps are the authored list, whole and in
    /// order (R906-F2). False after any resume drain or name-subset filter.
    ///
    /// Only a complete list can prove an operator gate was the run's first
    /// manual step: a trimmed one is missing exactly the steps whose absence
    /// would make a late gate look early.
    fn steps_are_complete(&self) -> bool {
        self.index_offset == 0 && !self.steps_are_subset
    }

    /// R499-F3 phase 2: per-step gha-workflow matrix subset. Each entry
    /// maps a qed step name to the chosen instance keys (see
    /// [`yah_qed_gha::graph::JobInstance::key`]). Steps absent from the map
    /// run their full matrix. Inherited by SubPipeline children.
    pub fn with_gha_matrix_subset(
        mut self,
        subset: std::collections::HashMap<String, std::collections::HashSet<String>>,
    ) -> Self {
        self.gha_matrix_subset = subset;
        self
    }

    /// Override the self-detected host triple (R531-T1). Constructors default
    /// to [`crate::platform::detect_host_triple`] (the process host); callers
    /// that know the execution host differs — e.g. a daemon constructing a
    /// runner whose steps will land on a remote runner of a known triple —
    /// set it explicitly. Composes with any constructor.
    pub fn with_host_triple(mut self, triple: impl Into<String>) -> Self {
        self.host_triple = triple.into();
        self
    }

    /// Seed the host-native cross-toolchain availability (R531-T6) instead of
    /// probing it. Tests use this to drive the NativeCross rewrite
    /// deterministically; a daemon constructing a runner for a remote host of a
    /// known toolchain set uses it to avoid a wrong local probe. Composes with
    /// any constructor; takes effect only if set before the first
    /// [`Self::cross_availability`] read.
    pub fn with_cross_availability(self, avail: crate::nativecross::ToolAvailability) -> Self {
        // OnceLock::set errors only if already initialized; a builder call
        // before any step runs is always first, so ignore the result.
        let _ = self.cross_availability.set(avail);
        self
    }

    /// The host-native cross toolchains installed on this runner (R531-T6),
    /// probed once and cached. The lazy half of the F5/T6 wiring: a runner with
    /// no NativeCross-tier step never calls this, so it never shells out.
    fn cross_availability(&self) -> crate::nativecross::ToolAvailability {
        *self
            .cross_availability
            .get_or_init(crate::nativecross::ToolAvailability::probe)
    }

    /// The host triple this runner executes on (R531-T1, W222), e.g.
    /// `aarch64-apple-darwin`. Threaded into the GHA `runner.{os,arch}`
    /// context and (F2/F3) the `host` leg of each step's `Platform` triple.
    pub fn host_triple(&self) -> &str {
        &self.host_triple
    }

    /// Seed the host's detected toolchain versions (R507, W208) instead of
    /// probing them. Tests drive the plan-time pinning check deterministically
    /// with this; a daemon constructing a runner for a remote host of a known
    /// toolchain set uses it to avoid a wrong local probe. Maps pin key →
    /// detected version (`None` = tool absent). Takes effect only if set before
    /// the first [`Self::host_toolchains`] read.
    pub fn with_host_toolchains(
        self,
        detected: std::collections::HashMap<String, Option<String>>,
    ) -> Self {
        let _ = self.host_toolchains.set(detected);
        self
    }

    /// The host's detected toolchain versions, probed once and cached (R507).
    /// The lazy half of the pinning check: a runner whose pipeline declares no
    /// `[toolchain]` pins never calls this, so it never shells out. Probes only
    /// the tools actually named across the pipeline + step pins.
    fn host_toolchains(&self) -> &std::collections::HashMap<String, Option<String>> {
        self.host_toolchains.get_or_init(|| {
            let mut keys: Vec<&str> = Vec::new();
            if let Some(tc) = &self.pipeline.toolchain {
                keys.extend(tc.pins.keys().map(String::as_str));
            }
            for step in &self.pipeline.steps {
                if let Some(tc) = &step.toolchain {
                    keys.extend(tc.pins.keys().map(String::as_str));
                }
            }
            crate::toolchain::detect_host_versions(keys)
        })
    }

    /// Whether a step's toolchain is provided by a container image rather than
    /// the host (R507, W208). A step that pulls an explicit `image` or pins
    /// `runtime = "container"` delegates its toolchain to that image, so the
    /// host-side pin check is skipped. Host-native steps (the default) are
    /// checked against the host's installed versions.
    fn step_satisfied_by_image(&self, step: &crate::types::QedStep) -> bool {
        step.image.is_some() || matches!(step.runtime, Some(TaskRuntime::Container))
    }

    /// Plan-time toolchain pinning check (R507, W208 pillar 3): for every step,
    /// overlay its `toolchain.*` overrides onto the pipeline-level
    /// `[toolchain]` pins, then resolve each pin against the host's detected
    /// versions (or mark it image-provided). Pure given the (seeded or probed)
    /// host versions — builds the verdict from the static pipeline, runs
    /// nothing. The runner gates `run()` on
    /// [`ToolchainPreflight::is_satisfied`](crate::toolchain::ToolchainPreflight::is_satisfied)
    /// and fails fast with its error report.
    pub fn toolchain_preflight(&self) -> crate::toolchain::ToolchainPreflight {
        let host = self.host_toolchains();
        let mut entries = Vec::new();
        for step in &self.pipeline.steps {
            let pins = crate::toolchain::effective_pins(
                self.pipeline.toolchain.as_ref(),
                step.toolchain.as_ref(),
            );
            if pins.is_empty() {
                continue;
            }
            let by_image = self.step_satisfied_by_image(step);
            for (tool, want) in &pins {
                let detected = host.get(tool).and_then(|v| v.as_deref());
                let resolution = crate::toolchain::resolve_pin(tool, want, detected, by_image);
                entries.push(crate::toolchain::PreflightEntry {
                    step: step.name.clone(),
                    resolution,
                });
            }
        }
        crate::toolchain::ToolchainPreflight { entries }
    }

    /// Compose a step's full [`Platform`](crate::platform::Platform) triple-set
    /// (R531-F2, W222): this runner's self-detected `host`, the step's declared
    /// `target` (its `[platform].target`, falling back to the legacy per-kind
    /// `triple` field), and the `container_platform` it pulls. This is the
    /// value F3's `resolve(host, target, container_platform)` decision table
    /// reasons over.
    pub fn step_platform(&self, step: &crate::types::QedStep) -> crate::platform::Platform {
        crate::platform::Platform::compose(
            &self.host_triple,
            step.platform.as_ref(),
            step.triple.as_deref(),
        )
    }

    /// Resolve how a step's build is satisfied on this runner's host (R531-F3,
    /// W222): compose its [`Platform`](crate::platform::Platform) triple-set,
    /// then run the cross-first decision table. Feeds the T4 portability
    /// preflight and (P2) the container-seam wiring.
    ///
    /// R555-B10: this is the **placement** question, so it folds this runner's
    /// probed [`cross_availability`](Self::cross_availability) in — a NativeCross
    /// derivation the host has no toolchain for demotes to
    /// [`Offload`](crate::platform::Resolution::Offload) rather than hard-failing
    /// at execution time. Ask [`derive_step`](Self::derive_step) for the
    /// capability-blind verdict.
    pub fn resolve_step(&self, step: &crate::types::QedStep) -> crate::platform::Resolution {
        let p = self.step_platform(step);
        // R590-F4: thread the step's `native` flag so a `native = true` cross-arch
        // build resolves to Offload (real silicon) rather than NativeCross/Emulate.
        let native = step.platform.as_ref().map(|s| s.native).unwrap_or(false);
        crate::platform::resolve_placement(
            &p.host,
            p.target.as_deref(),
            p.container_platform.as_deref(),
            native,
            &self.cross_availability(),
        )
    }

    /// The **Derivation**-only resolution for a step (R555-B10, W235 §4): what
    /// this host's shape allows, *without* the Capability demotion
    /// [`resolve_step`](Self::resolve_step) applies.
    ///
    /// Two callers need this rather than the placement verdict, and both would
    /// report the wrong thing off the demoted one:
    ///
    /// - [`native_cross_plan`](Self::native_cross_plan), because a step forced
    ///   local (`--where local`) on an under-provisioned host must still route
    ///   through the NativeCross tier so it fails with `cargo-zigbuild`'s install
    ///   hint instead of the raw linker error the demotion was meant to replace.
    /// - the local-container refusal in `execute_step_local_container`, whose
    ///   error says the step "declares a native build" — true of a derived
    ///   Offload, false of a capability demotion.
    pub fn derive_step(&self, step: &crate::types::QedStep) -> crate::platform::Resolution {
        let p = self.step_platform(step);
        let native = step.platform.as_ref().map(|s| s.native).unwrap_or(false);
        crate::platform::derive_placement(
            &p.host,
            p.target.as_deref(),
            p.container_platform.as_deref(),
            native,
        )
    }

    /// Every step whose NativeCross derivation this host cannot carry
    /// (R555-B10) — `(step name, gap)`, where the gap names the missing tool and
    /// its install command.
    ///
    /// The loudness surface for the demotion. W235 §6's deliberate non-goal is a
    /// *silent* offload: shipping a build to the fleet because a laptop lacks a
    /// tool turns a five-second local build into a five-minute round trip nobody
    /// asked for, so `run_inner` warns one line per entry before the run
    /// starts, and the CLI's `--where=auto` notice names it too.
    /// Pure over the static pipeline; executes nothing.
    pub fn capability_demotions(
        &self,
    ) -> Vec<(String, crate::nativecross::CrossToolUnavailable)> {
        pipeline_capability_demotions(
            &self.pipeline,
            &self.host_triple,
            &self.cross_availability(),
        )
    }

    /// Concrete placement for a step (R590-F4): fold this runner's `--where`
    /// force-mode with the step's [`resolve_step`](Self::resolve_step) verdict.
    /// Returns [`Local`](RunWhere::Local) or [`Remote`](RunWhere::Remote) only
    /// (never `Auto`). For a `Local`/`Remote` runner this is a constant — every
    /// step follows the forced mode, preserving the pre-F4 all-local / all-remote
    /// behaviour — so only an `Auto` runner routes per-step.
    fn effective_placement(&self, step: &crate::types::QedStep) -> RunWhere {
        // R823-F2: a participant bound to a named node is placed by that
        // binding, ahead of everything else — including a forced
        // `--where=local`. The set's addressing was allocated against that node
        // before dispatch, so running the step somewhere else would not be a
        // degraded placement, it would be a participant answering at an address
        // no peer was told about. There is no honest local fallback for a
        // rendezvous, which is why this arm precedes the forced-mode fast path
        // rather than following it.
        if self.participant_binding(step).is_some_and(|p| p.node.is_some()) {
            return RunWhere::Remote;
        }
        match self.run_where {
            // Fast path: a forced runner never inspects the step, so we skip the
            // resolve() work (and keep the many resolve_runtime test callers on
            // Local/Remote runners resolving to exactly their old default).
            RunWhere::Local => RunWhere::Local,
            RunWhere::Remote => RunWhere::Remote,
            RunWhere::Auto => policy_placement(RunWhere::Auto, &self.resolve_step(step)),
        }
    }

    /// R823-F2 — the pipeline's allocated participant set, or `None` when it
    /// declares one.
    ///
    /// Computed once, on first ask. The loader already proved this allocates
    /// (`PipelineLoader::pipeline_from_str`), but a runner can be handed a
    /// `Pipeline` built in code that never went through the loader, so the
    /// check is repeated here rather than assumed — cheap, pure, and the
    /// difference between a clear refusal at second zero and a participant
    /// dispatched with no address.
    fn participant_plan(
        &self,
    ) -> Result<Option<&crate::participants::ParticipantPlan>, RunnerError> {
        self.participant_plan
            .get_or_init(|| {
                crate::participants::plan_for(&self.pipeline).map_err(|e| e.to_string())
            })
            .as_ref()
            .map(|opt| opt.as_ref())
            .map_err(|msg| RunnerError::InvalidConfig(msg.clone()))
    }

    /// Which participant `step` belongs to, if any. `None` both for a run with
    /// no participant set and for an ordinary step inside one (a shared build
    /// step belongs to the run, not to a role).
    fn participant_binding(
        &self,
        step: &crate::types::QedStep,
    ) -> Option<&crate::participants::Participant> {
        let role = step.participant.as_deref()?;
        self.participant_plan().ok().flatten()?.get(role)
    }

    /// The rendezvous env every step of a participant run receives — the whole
    /// allocated set, plus this step's own role when it has one.
    ///
    /// Empty for a run with no participant set, which is the only reason this
    /// can be called unconditionally from each of the three env-building sites
    /// (local native, local container, remote). Calling it at all three is the
    /// point: a set injected into two of the three paths would produce a
    /// participant that can see its peers locally and cannot see them once the
    /// step offloads — the exact class of bug R577-F3 fixed for `step.env`.
    fn rendezvous_env(&self, step: &crate::types::QedStep) -> Vec<(String, String)> {
        match self.participant_plan().ok().flatten() {
            Some(plan) => plan.rendezvous_env(step.participant.as_deref()),
            None => Vec::new(),
        }
    }

    /// The lane this runner's own local work belongs in (R719-F7): the lane it
    /// was admitted on, unless it is a sub-pipeline child running in a lane of
    /// its own (see `admission_lane`).
    fn base_lane(&self) -> AdmissionLane {
        match &self.admission_lane {
            Some(key) => AdmissionLane::Named(key.clone()),
            None => AdmissionLane::Base,
        }
    }

    /// The lane a step's work belongs in, given its resolved placement
    /// (R719-F7, W298). Pure, so the policy is testable without a daemon.
    ///
    /// A [`Remote`](RunWhere::Remote) step's work lands on a build worker, so
    /// the run does not need its own lane while it runs — that is all the
    /// runner claims. What `Fleet` *resolves* to is the caller's call: the camp
    /// daemon holds nothing for a mixed `auto` run's offloaded stretch and
    /// keeps the base lane for a run it already placed by placement (R719-F3).
    /// Everything else runs here and takes this runner's own lane.
    ///
    /// Deliberately NOT special-cased: `gha-workflow` and `import` steps. They
    /// read as "dispatched to GitHub" and are not — `execute_step_gha_workflow`
    /// runs the workflow locally through `yah_qed_gha::Executor`, spawning
    /// `bash`/`docker`/`git` on this box, so they contend for the local lane
    /// like any other step.
    fn step_lane(&self, placement: RunWhere) -> AdmissionLane {
        match placement {
            RunWhere::Remote => AdmissionLane::Fleet,
            RunWhere::Local | RunWhere::Auto => self.base_lane(),
        }
    }

    /// Tell the admission control which lane the next unit of work needs. A
    /// no-op when nothing is installed (`yah qed run`), and a no-op on the
    /// implementation side when the lane is unchanged.
    async fn enter_lane(&self, lane: AdmissionLane) {
        if let Some(admission) = &self.admission {
            admission.enter(lane).await;
        }
    }

    /// Plan the host-native cross build for a step that resolves to the
    /// [`NativeCross`](crate::platform::Resolution::NativeCross) tier (R531-F5,
    /// W222) — the concrete cargo-zigbuild / musl-cross invocation that should
    /// *replace* the recipe's `cross build` / bare `cargo build` argv.
    ///
    /// Returns `None` for any step F3 does **not** resolve to NativeCross
    /// (those go through emulate / cross-docker / offload, not this tier), and
    /// for a NativeCross verdict with no concrete `target` (a plain host build
    /// needs no rewrite). For an in-tier step it selects the toolchain against
    /// `avail` and rewrites the step's `argv`, yielding the emulation-free
    /// plan (or a [`CrossToolUnavailable`](crate::nativecross::CrossToolUnavailable)
    /// carrying an install hint).
    ///
    /// This is the seam T6 wires into the subprocess executor; F5 only defines
    /// and tests it — `run()` does not yet route through it.
    pub fn native_cross_plan(
        &self,
        step: &crate::types::QedStep,
        avail: &crate::nativecross::ToolAvailability,
    ) -> Option<Result<crate::nativecross::NativeCrossPlan, crate::nativecross::CrossToolUnavailable>>
    {
        // R555-B10: the DERIVATION verdict, not the placement one. A step whose
        // NativeCross derivation this host has no toolchain for now *places* as
        // Offload — but a run forced local (`--where local`) still arrives here,
        // and it must reach the `Some(Err(unavailable))` arm below so the
        // operator gets `install_hint()` rather than the raw linker error.
        if !matches!(
            self.derive_step(step),
            crate::platform::Resolution::NativeCross
        ) {
            return None;
        }
        let platform = self.step_platform(step);
        // A host-arch / absent target is a plain native build — no foreign
        // toolchain, nothing for this tier to rewrite.
        let target = platform.target.as_deref()?;
        if !crate::nativecross::is_native_cross_target(&platform.host, target) {
            return None;
        }
        Some(crate::nativecross::plan_native_cross(
            &step.argv,
            &platform.host,
            target,
            avail,
        ))
    }

    /// Portability preflight (R531-T4, W222): one rendered line per step
    /// describing what it targets, the host it runs on, and the resolution
    /// verdict — so an operator sees where mac and linux will diverge (and at
    /// what cost) *before* the run. Pure: builds the lines from the static
    /// pipeline, no execution. The `index_offset` is honored so a
    /// resume-from-step run still shows original step positions.
    ///
    /// R555-F11 (W235 §3c) folds a **fleet clause** onto the end of each line.
    /// The existing clauses answer "where will this step build and at what
    /// cost"; the new one answers "could this go to the fleet at all, and what
    /// would it take" — a different question, and the one an operator had no way
    /// to ask short of starting a run and waiting for it to die inside a
    /// container. `yah qed preflight <pipeline>` renders these (plus the full
    /// per-gap remedies) without running anything.
    pub fn portability_preflight(&self) -> Vec<String> {
        // The fleet clause grounds requirement (1) against the real tree (see
        // `fleet_portability::camp_tree_reference`); `None` only when the camp
        // root cannot be resolved at all, which degrades to the pattern rules
        // rather than failing a report.
        let camp_root = self.resolve_camp_root().ok();
        self.pipeline
            .steps
            .iter()
            .map(|step| {
                let platform = self.step_platform(step);
                let resolution = self.resolve_step(step);
                format!(
                    "{} · {}",
                    crate::platform::preflight_line(&step.name, &platform, &resolution),
                    crate::fleet_portability::fleet_clause(step, camp_root.as_deref()),
                )
            })
            .collect()
    }

    /// Per-step fleet-portability report (R555-F11): every step, whether this
    /// run would dispatch it to a worker, what stands in the way, and — the
    /// R555-B10 interaction — whether it is being offloaded *because this host
    /// lacks a toolchain* rather than because anyone asked for the fleet.
    ///
    /// Pure over the static pipeline plus this runner's probed
    /// [`cross_availability`](Self::cross_availability); executes nothing. Both
    /// [`Self::fleet_portability_gate`] and `yah qed preflight` read it, so the
    /// refusal and the advisory cannot drift apart.
    ///
    /// `finally` steps are included: a teardown step dispatched to a worker has
    /// exactly the same problem as a main-loop one, and it takes the same
    /// `(placement, runtime)` dispatch arm.
    pub fn fleet_portability_report(&self) -> Vec<StepPortability> {
        let camp_root = self.resolve_camp_root().ok();
        let demotions: std::collections::HashMap<String, crate::nativecross::CrossToolUnavailable> =
            self.capability_demotions().into_iter().collect();
        self.pipeline
            .steps
            .iter()
            .chain(self.pipeline.finally.iter())
            .map(|step| StepPortability {
                step: step.name.clone(),
                dispatched_to_fleet: crate::fleet_portability::is_dispatchable_kind(step)
                    && self.effective_placement(step) == RunWhere::Remote,
                gaps: crate::fleet_portability::gaps(step, camp_root.as_deref()),
                capability_demotion: demotions.get(&step.name).cloned(),
            })
            .collect()
    }

    /// The fleet-portability gate (R555-F11, W235 §3c) — modelled on
    /// [`Self::emulation_gate`] and called from the same fail-fast preflight
    /// block in [`Self::run_inner`].
    ///
    /// `--where=remote` is a ROUTING force and does not make a step PORTABLE, so
    /// before this gate existed you could point it at any recipe, watch the
    /// container start, and lose minutes to `./scripts/whatever.sh: No such file
    /// or directory` from inside a build worker. Refuse at second zero instead,
    /// naming the step and which requirement it misses. `--force`
    /// ([`Self::with_force`]) bypasses, exactly as it does for the W155
    /// environment gate.
    ///
    /// # The R555-B10 interaction, which is the whole reason this is not a
    /// two-line function
    ///
    /// B10 made host CAPABILITY feed derivation: a coordinator with no
    /// `cargo-zigbuild` now DEMOTES a `NativeCross` step to `Offload` and warns,
    /// instead of hard-failing. So a step can arrive at `Remote` **because a
    /// tool is missing locally**, not because anyone asked for the fleet — and a
    /// bare portability refusal would then answer a missing-toolchain question
    /// with a portability error, which is precisely the confusing-error-for-
    /// actionable-error trade B10 was filed to undo.
    ///
    /// When both are true the message names BOTH facts and BOTH remedies, so the
    /// operator can see the step can run in neither place and pick which one to
    /// fix. `mesofact-musl` is the worked case that has to keep working end to
    /// end: it demotes to `Offload` on a zigbuild-less host **and** passes this
    /// gate, because it is one of only two camp recipes that declare
    /// `source_context`.
    fn fleet_portability_gate(&self) -> Result<(), RunnerError> {
        if self.force {
            return Ok(());
        }
        let report = self.fleet_portability_report();
        let blocked: Vec<&StepPortability> = report
            .iter()
            .filter(|entry| {
                entry.dispatched_to_fleet && entry.gaps.iter().any(|g| g.blocking())
            })
            .collect();
        if blocked.is_empty() {
            return Ok(());
        }
        // Per-step: the short labels plus, when the step is here by DEMOTION,
        // both facts side by side. Naming only the portability half would tell
        // an operator to rewrite a recipe when installing one tool is the
        // cheaper fix; naming only the toolchain half would send them to install
        // a tool that unblocks a step which still cannot go anywhere (R555-B10).
        let mut body = String::new();
        for entry in &blocked {
            let labels = entry
                .gaps
                .iter()
                .filter(|g| g.blocking())
                .map(|g| g.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            match &entry.capability_demotion {
                Some(gap) => body.push_str(&format!(
                    "  · step `{}` can run in NEITHER place:\n    \
                     (a) NOT HERE — this host cannot cross-compile it: {gap}\n    \
                     (b) NOT ON THE FLEET — {labels}\n    \
                     Installing the tool fixes (a) and is usually the cheaper of the two; \
                     fixing (b) is what makes the step genuinely portable.\n",
                    entry.step,
                )),
                None => body.push_str(&format!(
                    "  · step `{}` is routed to the fleet but is not fleet-portable: \
                     {labels}\n",
                    entry.step,
                )),
            }
        }
        // …then each distinct remedy ONCE. A twenty-step recipe otherwise
        // repeats the same 900-character `source_context` explanation twenty
        // times, which is how a good explanation becomes wallpaper.
        let mut remedies = String::new();
        let mut seen: Vec<&'static str> = Vec::new();
        for gap in blocked
            .iter()
            .flat_map(|e| e.gaps.iter())
            .filter(|g| g.blocking())
        {
            if seen.contains(&gap.kind()) {
                continue;
            }
            seen.push(gap.kind());
            remedies.push_str(&format!("  {}: {}\n", gap.kind(), gap.remedy()));
        }
        Err(RunnerError::InvalidConfig(format!(
            "refusing to start — `--where` is a ROUTING force and does not make a step \
             PORTABLE. A remote subprocess gets image + argv + the /yah/produced mount and \
             NOTHING ELSE, in particular no source. {n} step(s) of this run would be \
             dispatched to a build worker and cannot run there:\n\n{body}\n{remedies}\n\
             Run `yah qed preflight <pipeline>` to see this without starting a run, or pass \
             `--force` to dispatch anyway — the run will start, and the step will fail \
             minutes in, inside a container.",
            n = blocked.len(),
        )))
    }

    /// The steps this run would satisfy by QEMU emulation (R560, W236): those
    /// whose resolution is [`Emulate`](crate::platform::Resolution::Emulate) —
    /// a foreign-arch container the runner would pull and run under emulation.
    /// Pure: reads the static pipeline, executes nothing. Yields
    /// `(step name, docker platform)` per emulating step so
    /// [`Self::emulation_gate`] can name them in its error.
    pub fn emulating_steps(&self) -> Vec<(String, String)> {
        self.pipeline
            .steps
            .iter()
            .filter_map(|step| match self.resolve_step(step) {
                crate::platform::Resolution::Emulate { docker_platform } => {
                    Some((step.name.clone(), docker_platform))
                }
                _ => None,
            })
            .collect()
    }

    /// The "are you sure?" gate for QEMU emulation (R560, W236). Emulation is a
    /// last-ditch path — slow, and easy to trip into by declaring a foreign
    /// `container_platform` without meaning to — so a pipeline that would
    /// emulate any step is **refused before it starts** unless the run opted in
    /// via [`Self::with_allow_emulate`] (`yah qed run --allow-emulate`). Pure +
    /// total; [`Self::run_inner`] calls it right after the toolchain preflight,
    /// alongside the other fail-fast gates.
    fn emulation_gate(&self) -> Result<(), RunnerError> {
        if self.allow_emulate {
            return Ok(());
        }
        let emulating = self.emulating_steps();
        if emulating.is_empty() {
            return Ok(());
        }
        let steps = emulating
            .iter()
            .map(|(name, plat)| format!("`{name}` (would emulate {plat})"))
            .collect::<Vec<_>>()
            .join(", ");
        Err(RunnerError::InvalidConfig(format!(
            "refusing to start — QEMU emulation is a last-ditch path (a foreign-arch \
             container pulled and run under emulation, often 10-50× slower). Emulating \
             step(s): {steps}. Route the step to real silicon with `native = true` (QED \
             offloads it to an arch-matched build-worker), drop the foreign \
             `container_platform`, or — only if emulation is genuinely intended — \
             re-run with `--allow-emulate` to confirm."
        )))
    }

    /// The run id assigned at construction. Lets a caller (e.g. the camp
    /// daemon's `qed.run` handler) register a run as `Running` *before*
    /// [`Self::run`] completes, so `qed.status` can observe it in flight.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub async fn run(&self) -> Result<QedRunMeta, RunnerError> {
        let (meta, _produced) = self.run_inner().await?;
        Ok(meta)
    }

    /// Same as [`Self::run`] but also returns the aggregated
    /// [`ProducedArtifact`] list. Used by SubPipeline recursion (R488-F2):
    /// a parent's SubPipeline step calls `run_inner()` on the child runner
    /// so it can roll the child's `produced` into its own collection. Public
    /// `run()` discards it (callers that need artifacts go through
    /// `Outcome::Publish`, not the meta).
    // Returns an explicitly-boxed `dyn Future + Send` rather than plain
    // `async fn` sugar. This is NOT decoration — a
    // sub-pipeline child is now spawned via `tokio::spawn` (so `qed.cancel
    // <child_run_id>` has a real task to abort), which requires the spawned
    // future to be provably `Send`. `run_inner` calls itself recursively
    // (through `execute_step_sub_pipeline`), and rustc's auto-trait inference
    // cannot resolve a `Send` bound on a self-recursive `async fn` — it needs
    // an explicit trait-object bound to break the cycle. The body is
    // otherwise unchanged; only the signature and the `Box::pin(async move
    // { ... })` wrapper around it are new.
    pub(crate) fn run_inner(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(QedRunMeta, Vec<crate::types::ProducedArtifact>), RunnerError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
        let created_at = Utc::now();
        let mut overall_status = RunStatus::Success;
        // Artifacts declared by steps that *succeed* — handed to an
        // Outcome::Publish (R330-F3). A failed step's `produces` is dropped:
        // we never publish an artifact a failing step may not have written.
        // SubPipeline steps (R488-F2) aggregate their child's `produced` into
        // this collection when `propagate.produces = true`.
        let mut produced: Vec<crate::types::ProducedArtifact> = Vec::new();
        // Named outputs accumulated so far (W201-F4): step_name → {key → value}.
        // Used to substitute `${{ steps.X.outputs.Y }}` in later steps'
        // argv / env before execution.
        let mut step_context: std::collections::HashMap<
            String,
            std::collections::HashMap<String, String>,
        > = std::collections::HashMap::new();
        // R513-F2: background sidecar steps spawned but not yet reaped. Reaped
        // either when their `background_until` gate step finishes (mid-loop) or
        // at the end of the step loop, whichever comes first. The Vec owns the
        // `kill_on_drop` task handles, so an early-return drops it and kills
        // every live sidecar.
        let mut background_tasks: Vec<BackgroundTask> = Vec::new();
        // R823-F2: step indices whose remote participant sidecar was never
        // accepted by a node. Distinct from "failed" and collected here rather
        // than inferred later, because the participant verdict reports the two
        // as different diagnoses (see `crate::participants::verdict`).
        let mut never_dispatched_steps: std::collections::HashSet<usize> =
            std::collections::HashSet::new();

        self.emit(QedEvent::RunStarted {
            total_steps: self.index_offset + self.pipeline.steps.len(),
            at: created_at,
        });

        // R531-T4: portability preflight — log the per-step host/target/
        // resolution verdict before executing anything, so divergence (and its
        // cost) is legible up front instead of after a wave-three faceplant.
        // Report-only: this never gates execution.
        for line in self.portability_preflight() {
            tracing::info!(target: "qed::preflight", host = %self.host_triple, "{line}");
        }

        // R555-B10 / W235 §6: a step whose NativeCross derivation this host has
        // no toolchain for is placed on the fleet instead of hard-failing at
        // execution time — but never silently. Shipping a build off-box because
        // a laptop lacks a tool turns a five-second local build into a
        // five-minute round trip nobody asked for, so say so, and name the
        // install command that takes it back.
        for (step, gap) in self.capability_demotions() {
            tracing::warn!(
                target: "qed::preflight",
                host = %self.host_triple,
                step = %step,
                "offloading `{step}` to the build-worker fleet: this host cannot cross-compile \
                 it — {gap}. Install the tool to build it here instead."
            );
        }

        // R507/W208: toolchain pinning preflight — resolve every `[toolchain]`
        // pin against the host's installed versions (or mark it image-provided)
        // and *fail fast* before any step runs when the host can't satisfy a
        // pin. Unlike the portability preflight above this one gates execution:
        // a missing Xcode/NDK should stop a multi-hour release at second zero
        // with an actionable error, not three waves in. Logged either way.
        let toolchain_preflight = self.toolchain_preflight();
        for line in toolchain_preflight.report() {
            tracing::info!(target: "qed::preflight", host = %self.host_triple, "toolchain: {line}");
        }
        if let Some(report) = toolchain_preflight.error_report() {
            tracing::error!(target: "qed::preflight", host = %self.host_triple, "{report}");
            return Err(RunnerError::ToolchainUnsatisfied(report));
        }

        // R560/W236: QEMU emulation gate. A foreign-arch container step resolves
        // to Emulate — pulled and run under QEMU, a slow last-ditch path that's
        // easy to trip into by accident (a stray `container_platform`). Refuse to
        // start unless the run explicitly confirmed with `--allow-emulate`, so an
        // unintended emulated build fails at second zero with an actionable error
        // instead of silently costing an hour. Fail-fast, like the toolchain gate.
        self.emulation_gate()?;

        // R555-F11/W235 §3c: fleet-portability gate. `--where=remote` routes a
        // step to a worker; it does not make the step's work survive the trip
        // (image + argv + /yah/produced, and no source). Refuse here rather than
        // letting a container start and die on a path that was never shipped.
        // Composed with B10's capability demotion so a step offloaded for a
        // MISSING TOOL is not reported as a portability problem alone. Fail-fast,
        // like the two gates above; `--force` bypasses.
        self.fleet_portability_gate()?;

        // R823-F2: allocate the participant set before anything runs. Every
        // later consumer (`participant_binding`, `rendezvous_env`) reads the
        // cached result and treats a failure as "no set", so this call is what
        // turns a mis-declared rendezvous into a refusal at second zero rather
        // than into a run whose peers were silently never told about each
        // other. Fail-fast, like the two gates above.
        let participant_plan = self.participant_plan()?.cloned();

        // R605-F3: resolve the step dependency graph once, before anything
        // runs. `Missing::Satisfied` because a resume-from-step run is handed a
        // pipeline whose leading steps were `drain`ed (see
        // [`Self::with_step_selection`]) — a surviving `needs` that names one of
        // them names a step that genuinely already ran. Typos are still caught,
        // at load time, by `PipelineLoader::validate_dag` against the undrained
        // file; this is the only place that leniency is applied.
        let step_preds =
            crate::dag::predecessors(&self.pipeline.steps, crate::dag::Missing::Satisfied)
                .map_err(|e| RunnerError::InvalidConfig(e.to_string()))?;
        // Cycle check up front: a graph that never drains would otherwise show
        // up as a run that finishes instantly having executed nothing.
        crate::dag::waves(&self.pipeline.steps, crate::dag::Missing::Satisfied)
            .map_err(|e| RunnerError::InvalidConfig(e.to_string()))?;
        let dag_is_explicit = crate::dag::is_explicit(&self.pipeline.steps);

        // R513-F2: background sidecar pre-flight. v1 supports local + native
        // subprocess sidecars only, and a `background_until` target must name a
        // step that appears *later* in the pipeline. Fail loudly here, before
        // any step runs, rather than spawning a sidecar that can never be
        // reaped on schedule (a typo'd `background_until`) or routing one
        // through a runtime that can't honour `kill_on_drop` teardown.
        // R605-F3: per-sidecar gate, resolved to step INDICES at preflight and
        // carried onto the BackgroundTask. Indices rather than the raw name
        // because a gate naming a matrix step covers every row of it, and the
        // reap must fire when the LAST of them finishes — an exact-name compare
        // at reap time would have fired on the first.
        let mut background_gates: Vec<Vec<usize>> =
            (0..self.pipeline.steps.len()).map(|_| Vec::new()).collect();
        for (i, step) in self.pipeline.steps.iter().enumerate() {
            if !step.is_background() {
                continue;
            }
            // R823-F2: a sidecar bound to a node-carrying participant IS the
            // "separate lifecycle" R513-F2's refusal below was holding the door
            // for — `spawn_remote_participant_step` dispatches it and
            // `reap_background` tears it down on the node explicitly. Everything
            // after this point is about local sidecars.
            let remote_participant = self
                .participant_binding(step)
                .is_some_and(|p| p.node.is_some());
            // R590-F4: gate on the step's *effective* placement, not the raw
            // run_where — an Auto runner is fine for a background step that
            // resolves local; only a background step that would offload to the
            // fleet is rejected (remote sidecars are a separate lifecycle).
            if !remote_participant && self.effective_placement(step) != RunWhere::Local {
                return Err(RunnerError::InvalidConfig(format!(
                    "step `{}`: background steps run locally only (R513-F2) — \
                     remote sidecars are yubaba-supervised, a separate lifecycle. \
                     A step that genuinely belongs on another host is a participant: \
                     declare it in [pipeline.participants] and name the role with \
                     `participant = \"…\"` (R823-F2)",
                    step.name,
                )));
            }
            // A remote participant runs in a forge container by construction —
            // that is what the offload path dispatches — so the native-only rule
            // is about local sidecars and does not apply to it.
            if !remote_participant && self.resolve_runtime(step) != TaskRuntime::Native {
                return Err(RunnerError::InvalidConfig(format!(
                    "step `{}`: background steps run native only in v1 (R513-F2) — \
                     drop `runtime = \"container\"`",
                    step.name,
                )));
            }
            if let Some(until) = &step.background_until {
                // R605-F3: resolved with the same matcher `needs` uses, so a
                // gate naming a matrix step resolves to ALL of its rows. Before
                // this it was an exact name compare, and `background_until =
                // "build"` against a fanned-out `build` failed preflight with
                // "unknown step" — the post-expansion name is `build [k=v]`,
                // which no author writes by hand.
                let gate: Vec<usize> = self
                    .pipeline
                    .steps
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| crate::dag::name_matches(&s.name, until))
                    .map(|(j, _)| j)
                    .collect();
                match gate.first().copied() {
                    None => {
                        return Err(RunnerError::InvalidConfig(format!(
                            "step `{}`: background_until names unknown step `{until}`",
                            step.name,
                        )));
                    }
                    Some(pos) if pos <= i => {
                        return Err(RunnerError::InvalidConfig(format!(
                            "step `{}`: background_until must name a *later* step, \
                             but `{until}` is at or before it — a sidecar reaped on a \
                             prior step would never see its gate fire",
                            step.name,
                        )));
                    }
                    // R605-F3: once a pipeline declares `needs`, "later in the
                    // file" stops meaning "after". A gate step on a PARALLEL
                    // branch can finish while the branch that actually talks to
                    // the sidecar is still mid-run, and the reap would kill the
                    // server out from under it — a race whose symptom is a
                    // connection-refused three steps away from its cause.
                    // Require the gate to be a genuine descendant. Not applied
                    // to the implicit chain, where every later step already is
                    // one, so this can only reject something newly expressible.
                    Some(_)
                        if dag_is_explicit && {
                            let deps = crate::dag::dependents(&step_preds, i);
                            !gate.iter().all(|g| deps.contains(g))
                        } =>
                    {
                        return Err(RunnerError::InvalidConfig(format!(
                            "step `{}`: background_until names `{until}`, which does not \
                             depend on it — under a declared `needs` graph the gate step \
                             must be a descendant of the sidecar, or the reap races the \
                             steps that use it",
                            step.name,
                        )));
                    }
                    Some(_) => {}
                }
                background_gates[i] = gate;
            }
        }

        // W224 R533-F11: position the whole run's workspace ONCE, before any
        // step. Top-level runs honour the pipeline's WorkspaceMode (Live /
        // Checkout-bail-if-dirty / Isolated worktree); the resulting tree is
        // recorded in `positioned_workspace` so every step kind resolves its
        // root through it (subprocess `desktop-release` builds from the same
        // Isolated worktree as a `gha-workflow` step, not the live camp root).
        // Child sub-pipeline runners inherit the parent's already-positioned
        // tree via `camp_root` (set at construction), so they skip repositioning
        // — re-running a checkout / spinning a second worktree mid-run would be
        // wrong. `_run_worktree_guard` is held for the entire step loop so an
        // Isolated worktree outlives the whole run and is torn down on drop,
        // even when a step below returns early with an error.
        //
        // R755: `own_workspace` (set from the step's own
        // `[sub_pipeline] own_workspace = true`) opts a child OUT of that
        // inheritance — it repositions per its OWN declared WorkspaceMode
        // instead. `base_camp_root()` still resolves to the PARENT's
        // (already-positioned) tree in that case, so `git worktree add`
        // runs against the same repository the parent is standing in.
        // R766: the Isolated worktree path, held alongside the guard so a
        // FAILED run can persist it onto `QedRunMeta::retained_workspace` for
        // a later resume. `None` for every other WorkspaceMode (nothing to
        // retain — a Live/Checkout run's tree IS the camp root, already
        // durable) and for an inheriting sub-pipeline child (no guard here at
        // all — the PARENT's guard owns that worktree's lifetime).
        let (run_worktree_path, _run_worktree_guard) = if self.parent_run_id.is_some()
            && !self.own_workspace
        {
            (None, None)
        } else {
            let base = self.base_camp_root()?;
            let (workspace, guard) = self.prepare_workspace(&base)?;
            // OnceLock: this is the only writer (run_inner runs once per runner
            // instance) and it fires before the first step, so every step-time
            // resolve_camp_root() sees the positioned tree.
            let _ = self.positioned_workspace.set(workspace.clone());
            let path = guard.is_some().then_some(workspace);
            (path, guard)
        };

        // ── R605-F3: the step scheduler ──────────────────────────────────
        //
        // A step becomes eligible when every predecessor in its `needs` graph
        // is done; up to `max_parallel` eligible steps run at once, minus any
        // whose `resource` key is already held. A pipeline that declares no
        // `needs` resolves to the chain `0 → 1 → 2 → …` (see [`crate::dag`]),
        // so exactly one step is ever eligible and this degenerates to the
        // sequential walk it replaced — same order, same events, same rows.
        // That equivalence is the safety property: every pipeline TOML in
        // every camp predates the field.
        //
        // Rows land in `main_statuses` BY STEP INDEX rather than being pushed,
        // so a concurrent run still reports its steps in declaration order.
        // `None` marks a step the run never reached, which is exactly what the
        // old `break`-on-failure left out of the Vec.
        let total = self.pipeline.steps.len();
        let mut main_statuses: Vec<Option<StepStatus>> = (0..total).map(|_| None).collect();
        // Produced artifacts folded back per step index for the same reason:
        // the publish leg should see them in declaration order whether or not
        // the steps that made them overlapped.
        let mut produced_by_index: Vec<Vec<crate::types::ProducedArtifact>> =
            (0..total).map(|_| Vec::new()).collect();
        let mut done: Vec<bool> = vec![false; total];
        let mut dispatched: Vec<bool> = vec![false; total];
        let max_parallel = self
            .pipeline
            .max_parallel
            .unwrap_or(crate::dag::DEFAULT_MAX_PARALLEL)
            .max(1);
        // Resource keys held by in-flight steps, and how many of those are
        // doing LOCAL work (the lane decision below needs the count, not a
        // boolean — two offloaded steps must not hand the local lane back
        // while a third step is still compiling on this host).
        let mut held_resources: Vec<String> = Vec::new();
        let mut local_inflight: usize = 0;
        // Set when a step fails with `on_fail = abort`: stop admitting, let
        // whatever is already running finish, then leave the loop. The old
        // code's `break` with nothing in flight is the same thing.
        let mut aborting = false;
        // R560-F15: a per-leg publish outcome scopes that abort to the failing
        // step's LEG ([`crate::dag::legs`]) — the leg stops admitting, every
        // other leg runs on, and terminal dispatch publishes the legs that came
        // through whole. Without one, the whole run is the leg, as before.
        let leg_scoped = self
            .pipeline
            .on_success
            .iter()
            .any(|o| matches!(o, Outcome::Publish { per_leg: true, .. }));
        let leg_of = crate::dag::legs(&step_preds);
        let mut leg_aborted: Vec<bool> = vec![false; total];
        let mut leg_failed: Vec<bool> = vec![false; total];
        let mut inflight = futures_util::stream::FuturesUnordered::new();

        loop {
            // ── admit ────────────────────────────────────────────────────
            while !aborting && inflight.len() < max_parallel {
                let ready = (0..total).find(|&i| {
                    !dispatched[i]
                        && !leg_aborted[leg_of[i]]
                        && step_preds[i].iter().all(|&p| done[p])
                        && match &self.pipeline.steps[i].resource {
                            Some(key) => !held_resources.iter().any(|h| h == key),
                            None => true,
                        }
                });
                let Some(index) = ready else { break };
                dispatched[index] = true;
                let event_index = index + self.index_offset;
                let raw = &self.pipeline.steps[index];
                // Apply accumulated step-output + host substitution to argv /
                // env / produces before the step runs. Clones only when there
                // is something to substitute.
                let step_modified = substituted_step(raw, &step_context, &self.host_triple);
                let step = step_modified.as_ref().unwrap_or(raw);

                // R506: declarative + runtime gating. Resolve the reason (if
                // any) *before* emitting StepStarted so a skipped step's
                // lifecycle pair carries a Skipped terminal status with no
                // "Running" intermediate state on the wire.
                // R560-F15: under leg scoping `success()` / `failure()` answer
                // for this step's own leg; another leg's failure is not its gate.
                let gate_status = if !leg_scoped {
                    overall_status
                } else if leg_failed[leg_of[index]] {
                    RunStatus::Failed
                } else {
                    RunStatus::Success
                };
                let skip_reason = self.resolve_skip_reason(step, &step_context, gate_status);

                let started_at = Utc::now();
                self.emit(QedEvent::StepStarted {
                    index: event_index,
                    name: step.name.clone(),
                    argv: step.argv.clone(),
                    env_keys: crate::events::credential_env_keys(std::env::vars()),
                    expect_slow: step.expect_slow,
                    at: started_at,
                });

                if let Some(reason) = skip_reason {
                    let completed_at = Utc::now();
                    self.emit(QedEvent::StepFinished {
                        index: event_index,
                        name: step.name.clone(),
                        status: RunStatus::Skipped,
                        msg: Some(reason.clone()),
                        at: completed_at,
                    });
                    main_statuses[index] = Some(StepStatus {
                        name: step.name.clone(),
                        task_run_id: None,
                        status: RunStatus::Skipped,
                        started_at: Some(started_at),
                        completed_at: Some(completed_at),
                        // R330-B41: persist the same reason onto the terminal
                        // meta that was already computed for the live event
                        // above — previously dropped here, so `qed.status` /
                        // the report could show *that* a step skipped but never
                        // *why*.
                        error: Some(reason),
                        outputs: std::collections::HashMap::new(),
                        applied_binds: Vec::new(),
                        jobs: Vec::new(),
                        // A skipped step ran nothing, so it has no result for
                        // an input digest to be about (R717-T1).
                        input_hashes: std::collections::BTreeMap::new(),
                    });
                    // A skipped step still SATISFIES its dependents — GHA
                    // semantics, and the only reading under which `enabled =
                    // false` on one step doesn't silently strand the rest of
                    // the branch. Downstream steps gate on the run status via
                    // `if = "success()"` if they care.
                    done[index] = true;
                    continue;
                }

                // R717-T1 (W296): digest this step's declared `inputs` BEFORE
                // it runs, so the recorded map answers "which bytes produced
                // this result?" — a step that rewrites its own input would
                // otherwise pin the bytes it emitted. Recorded onto
                // `StepStatus::input_hashes`; the staleness *verdict* is never
                // stored, only computed at read time by
                // `crate::staleness::input_freshness`. Skipped steps above
                // never reach here, and rightly so: they produced no result for
                // an input digest to be about.
                let input_hashes = if step.inputs.is_empty() {
                    std::collections::BTreeMap::new()
                } else {
                    crate::staleness::hash_declared_inputs(&self.resolve_camp_root()?, &step.inputs)
                };

                let runtime = self.resolve_runtime(step);
                // R590-F4: derive this step's concrete placement (Local/Remote)
                // from the `--where` force-mode + its declared platform. Under
                // the default Auto mode a `native = true` cross-arch step routes
                // to the fleet here without any `--where=remote` flag.
                let placement = self.effective_placement(step);

                // R513-F2: a background sidecar is *spawned*, not awaited. Emit
                // only its StepStarted (already done above), kick the subprocess
                // onto its own task, record a `Running` placeholder row finalized
                // at reap, and advance. Pre-flight above guarantees this is a
                // local + native subprocess step. Output collection
                // ($YAH_OUTPUTS) is skipped — a long-lived sidecar has no
                // terminal moment to read it back, and downstream substitution
                // can't wait on a server that never exits.
                if step.is_background() {
                    // R823-F2: a node-bound participant's sidecar is dispatched
                    // to that node instead of spawned here. Both arms return a
                    // join handle and neither awaits — the admission loop must
                    // not block, which is why the remote dispatch itself happens
                    // inside the spawned task rather than on this line.
                    let remote_participant = self
                        .participant_binding(step)
                        .filter(|p| p.node.is_some())
                        .cloned();
                    let (join, remote) = match remote_participant {
                        Some(participant) => {
                            let (join, sidecar) = self
                                .spawn_remote_participant_step(event_index, step, &participant)?;
                            (join, Some(sidecar))
                        }
                        None => {
                            let spec = build_subprocess_spec(step, TaskRuntime::Native, None);
                            let camp_root = self.resolve_camp_root()?;
                            let cwd = match step.cwd.as_ref() {
                                Some(rel) => camp_root.join(rel),
                                None => camp_root,
                            };
                            // R744-T2: same base-env underlay as
                            // `execute_step_local` — a background sidecar is a
                            // local native subprocess too, and a `cargo
                            // run`-shaped one wants the host's build cache
                            // exactly as much as a foreground build does.
                            let mut env: Vec<(String, String)> = step
                                .env
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect();
                            for (k, v) in &self.base_env {
                                if !env.iter().any(|(existing, _)| existing == k) {
                                    env.push((k.clone(), v.clone()));
                                }
                            }
                            // R823-F2: the rendezvous overrides, same rule as
                            // every other env site.
                            let rendezvous = self.rendezvous_env(step);
                            env.retain(|(k, _)| !rendezvous.iter().any(|(rk, _)| rk == k));
                            env.extend(rendezvous);
                            let ctx = ExecContext::default().with_cwd(cwd).with_env(env);
                            (
                                self.spawn_background_step(event_index, step, spec, ctx),
                                None,
                            )
                        }
                    };
                    main_statuses[index] = Some(StepStatus {
                        name: step.name.clone(),
                        task_run_id: None,
                        status: RunStatus::Running,
                        started_at: Some(started_at),
                        completed_at: None,
                        error: None,
                        outputs: std::collections::HashMap::new(),
                        applied_binds: Vec::new(),
                        jobs: Vec::new(),
                        // R717-T1: a sidecar is reaped, not completed — it has
                        // no terminal moment its inputs would be evidence about.
                        input_hashes: std::collections::BTreeMap::new(),
                    });
                    background_tasks.push(BackgroundTask {
                        status_index: index,
                        event_index,
                        name: step.name.clone(),
                        gate: background_gates[index].clone(),
                        join,
                        remote,
                    });
                    // R605-F3: a sidecar satisfies its dependents at SPAWN. It
                    // has no exit to wait for, so any other reading would make
                    // `needs = ["server"]` an unsatisfiable edge. See
                    // [`crate::types::QedStep::needs`].
                    done[index] = true;
                    continue;
                }

                // R719-F7 (W298): resolve the admission lane this step's work
                // needs. Computed after the skip / background arms on purpose:
                // neither does work the run's lane is protecting (a skipped step
                // does nothing; a sidecar is spawned, not awaited), so neither
                // should re-queue.
                //
                // R605-F3: the lane is a property of the RUN, not of a step, so
                // with several steps in flight it has to cover the most
                // demanding of them. One branch offloading to the fleet does not
                // make this host idle while another branch is still compiling on
                // it, so `Fleet` is downgraded to the run's own lane whenever
                // local work is in flight. On the implicit chain `local_inflight`
                // is always 0 here and the arm never fires.
                //
                // The `enter_lane` itself happens inside the step's own future
                // rather than here, so a run that has to QUEUE for its lane
                // doesn't stall the steps already in flight — the admission
                // loop must not block on anything, because nothing else is
                // being polled while it runs. The observable sequence is
                // unchanged on the serial path: StepStarted, then the lane,
                // then execution.
                let lane = match self.step_lane(placement) {
                    AdmissionLane::Fleet if local_inflight > 0 => self.base_lane(),
                    other => other,
                };

                if placement == RunWhere::Local {
                    local_inflight += 1;
                }
                if let Some(key) = &step.resource {
                    held_resources.push(key.clone());
                }
                let owned = step.clone();
                inflight.push(self.run_one_step(
                    index,
                    owned,
                    started_at,
                    input_hashes,
                    runtime,
                    placement,
                    lane,
                ));
            }

            // ── collect ──────────────────────────────────────────────────
            let Some(outcome) = futures_util::StreamExt::next(&mut inflight).await else {
                break;
            };
            let index = outcome.index;
            done[index] = true;
            if outcome.was_local {
                local_inflight -= 1;
            }
            if let Some(key) = &outcome.resource {
                if let Some(pos) = held_resources.iter().position(|h| h == key) {
                    held_resources.remove(pos);
                }
            }
            // Store outputs in the step context for downstream substitution.
            // Stored even when the step failed — a continue-on-error sibling
            // may still reference whatever was written before the failure.
            if !outcome.outputs.is_empty() {
                step_context.insert(outcome.name.clone(), outcome.outputs.clone());
            }
            produced_by_index[index] = outcome.produced;
            // Failed beats Cancelled (matches `RunStatus::aggregate`'s
            // priority for the same reason: a concurrent sibling step's real
            // tool failure is more informative than "an operator also asked
            // this run to stop", so it must not be downgraded by arrival
            // order.
            let row_status = outcome.row.status;
            if row_status == RunStatus::Failed {
                overall_status = RunStatus::Failed;
                leg_failed[leg_of[index]] = true;
            } else if outcome.row.status == RunStatus::Cancelled
                && overall_status != RunStatus::Failed
            {
                overall_status = RunStatus::Cancelled;
            }
            main_statuses[index] = Some(outcome.row);

            // R513-F2: reap any background sidecar whose gate has now fully
            // fired. Reaping here — before the `on_fail` abort below — means a
            // sidecar is torn down right after its gate regardless of whether
            // the gate step passed or failed.
            //
            // R605-F3: "fully" is the operative word. The gate is a set of step
            // indices (every row, when it names a matrix step), so the reap
            // waits for the LAST of them; with a single un-fanned gate step
            // that is exactly the old `until == step.name` compare.
            let mut i = 0;
            while i < background_tasks.len() {
                let fired = !background_tasks[i].gate.is_empty()
                    && background_tasks[i].gate.iter().all(|&g| done[g]);
                if fired {
                    let bg = background_tasks.remove(i);
                    let SidecarReap {
                        status: bg_status,
                        msg: bg_msg,
                        never_dispatched,
                        teardown_note,
                    } = reap_background(bg.join, bg.remote).await;
                    if never_dispatched {
                        never_dispatched_steps.insert(bg.status_index);
                    }
                    if let Some(note) = teardown_note {
                        self.emit_teardown_note(bg.event_index, &bg.name, note);
                    }
                    let bg_completed_at = Utc::now();
                    if bg_status == RunStatus::Failed {
                        overall_status = RunStatus::Failed;
                    }
                    self.emit(QedEvent::StepFinished {
                        index: bg.event_index,
                        name: bg.name.clone(),
                        status: bg_status,
                        msg: bg_msg.clone(),
                        at: bg_completed_at,
                    });
                    if let Some(row) = main_statuses[bg.status_index].as_mut() {
                        row.status = bg_status;
                        row.completed_at = Some(bg_completed_at);
                        row.error = if bg_status == RunStatus::Failed {
                            bg_msg
                        } else {
                            None
                        };
                    }
                } else {
                    i += 1;
                }
            }

            if outcome.abort {
                // Cancelled always stops the whole run; only a step's own
                // failure is scoped to its leg (R560-F15).
                if leg_scoped && row_status == RunStatus::Failed {
                    leg_aborted[leg_of[index]] = true;
                } else {
                    aborting = true;
                }
            }
        }

        // R513-F2: reap every background sidecar still running at the end of the
        // step loop — those with no `background_until` (reap-at-pipeline-end),
        // plus any whose gate step was skipped or never reached. Done before
        // terminal-outcome selection so a sidecar that *crashed* mid-pipeline
        // flips the run to Failed and fires `on_fail`.
        for bg in background_tasks.drain(..) {
            let SidecarReap {
                status: bg_status,
                msg: bg_msg,
                never_dispatched,
                teardown_note,
            } = reap_background(bg.join, bg.remote).await;
            if never_dispatched {
                never_dispatched_steps.insert(bg.status_index);
            }
            if let Some(note) = teardown_note {
                self.emit_teardown_note(bg.event_index, &bg.name, note);
            }
            let bg_completed_at = Utc::now();
            if bg_status == RunStatus::Failed {
                overall_status = RunStatus::Failed;
            }
            self.emit(QedEvent::StepFinished {
                index: bg.event_index,
                name: bg.name.clone(),
                status: bg_status,
                msg: bg_msg.clone(),
                at: bg_completed_at,
            });
            if let Some(row) = main_statuses[bg.status_index].as_mut() {
                row.status = bg_status;
                row.completed_at = Some(bg_completed_at);
                row.error = if bg_status == RunStatus::Failed {
                    bg_msg
                } else {
                    None
                };
            }
        }

        // R823-F2: fold the participant set into ONE verdict, after every
        // sidecar has been reaped (so a peer that died mid-run is visible here)
        // and before the step rows are flattened (so a participant whose steps
        // never produced a row is still distinguishable from one whose steps
        // ran). A participant verdict can only ADD a failure — it never turns a
        // failed run green.
        let mut run_failure_reason: Option<String> = None;
        if let Some(plan) = &participant_plan {
            let reports = participant_reports(
                plan,
                &self.pipeline.steps,
                &main_statuses,
                &never_dispatched_steps,
            );
            if let crate::participants::Verdict::Fail { summary } =
                crate::participants::verdict(&reports)
            {
                tracing::error!(target: "qed::participants", run = %self.run_id, "{summary}");
                overall_status = RunStatus::Failed;
                run_failure_reason = Some(summary);
            }
        }

        // Flatten back to the wire shape: declaration order, with the steps the
        // run never reached simply absent — identical to what the pre-scheduler
        // `break` produced.
        // R560-F15: the artifacts of every leg that came through whole — each of
        // its steps has a row, and every row is Success or Skipped. A step an
        // aborted leg never admitted has no row, so that leg never qualifies.
        // Read here, before the rows flatten away their indices; dispatched
        // only when the run failed. A participant verdict failure names no
        // leg, so it withholds every leg.
        let whole_leg_produced: Vec<crate::types::ProducedArtifact> =
            if leg_scoped && run_failure_reason.is_none() {
                let mut broken = vec![false; total];
                for (i, row) in main_statuses.iter().enumerate() {
                    if !matches!(
                        row.as_ref().map(|r| r.status),
                        Some(RunStatus::Success | RunStatus::Skipped)
                    ) {
                        broken[leg_of[i]] = true;
                    }
                }
                (0..total)
                    .filter(|&i| !broken[leg_of[i]])
                    .flat_map(|i| produced_by_index[i].iter().cloned())
                    .collect()
            } else {
                Vec::new()
            };
        let mut step_statuses: Vec<StepStatus> = main_statuses.into_iter().flatten().collect();
        for slot in produced_by_index {
            produced.extend(slot);
        }

        // R513-F4 (W207 Gap #6): always-run `finally:` teardown. Runs after the
        // sidecar reap and before terminal-outcome dispatch, unconditionally —
        // pass or fail — so artifact/diagnostic teardown (upload Playwright
        // traces, `docker compose down`, collect logs) always happens. Two
        // deliberate semantics:
        //   * Outcome selection uses the *work* status (steps + sidecars),
        //     snapshotted here BEFORE finally runs — a flaky teardown never
        //     redirects `on_success` → `on_fail`.
        //   * Every finally step is attempted (a failure never aborts the rest;
        //     teardown should always run to completion). A failed finally step
        //     still marks the *run* Failed (tile + `RunFinished`) unless it sets
        //     `on_fail = "continue"`.
        // Loader validation (`validate_finally`) guarantees these are Subprocess
        // steps and never background.
        let work_status = overall_status;
        let finally_index_base = self.pipeline.steps.len() + self.index_offset;
        for (j, step) in self.pipeline.finally.iter().enumerate() {
            let event_index = finally_index_base + j;
            // A teardown step may reference a prior step's output (e.g. the path
            // a test step emitted for its trace bundle), so apply the same
            // substitution the main loop uses.
            let step_modified = substituted_step(step, &step_context, &self.host_triple);
            let step = step_modified.as_ref().unwrap_or(step);

            let started_at = Utc::now();
            self.emit(QedEvent::StepStarted {
                index: event_index,
                name: step.name.clone(),
                argv: step.argv.clone(),
                env_keys: crate::events::credential_env_keys(std::env::vars()),
                expect_slow: step.expect_slow,
                at: started_at,
            });

            // Honor declarative disable / stub (cheap parity with main steps); a
            // finally step is otherwise unconditional — no `if` gate is consulted
            // (teardown is always-run by definition).
            if !step.enabled || step.activation == crate::types::StepActivation::Stubbed {
                let completed_at = Utc::now();
                let reason = if !step.enabled {
                    "finally step disabled (enabled = false)"
                } else {
                    "finally step stubbed (status = stubbed)"
                };
                self.emit(QedEvent::StepFinished {
                    index: event_index,
                    name: step.name.clone(),
                    status: RunStatus::Skipped,
                    msg: Some(reason.to_string()),
                    at: completed_at,
                });
                step_statuses.push(StepStatus {
                    name: step.name.clone(),
                    task_run_id: None,
                    status: RunStatus::Skipped,
                    started_at: Some(started_at),
                    completed_at: Some(completed_at),
                    // R330-B41: same fix as the main step loop above.
                    error: Some(reason.to_string()),
                    outputs: std::collections::HashMap::new(),
                    applied_binds: Vec::new(),
                    jobs: Vec::new(),
                    input_hashes: std::collections::BTreeMap::new(),
                });
                continue;
            }

            // R717-T1: hashed BEFORE the step runs, same invariant as the main
            // loop — the digest is about the bytes that produced the result.
            let input_hashes = if step.inputs.is_empty() {
                std::collections::BTreeMap::new()
            } else {
                crate::staleness::hash_declared_inputs(&self.resolve_camp_root()?, &step.inputs)
            };
            let runtime = self.resolve_runtime(step);
            // R590-F4: per-step placement (see the main loop above).
            let placement = self.effective_placement(step);
            let result = match (placement, runtime) {
                (RunWhere::Local, TaskRuntime::Native) => {
                    self.execute_step_local(event_index, step, None).await
                }
                (RunWhere::Local, TaskRuntime::Container) => {
                    // `finally` teardown collects no artifacts — a produced
                    // artifact belongs to the work result, not to teardown.
                    self.execute_step_local_container(event_index, step)
                        .await
                        .map(|_| ())
                }
                (RunWhere::Local, TaskRuntime::MicroVm) => Err(local_microvm_is_refused(step)),
                // Auto is resolved to Local/Remote by effective_placement.
                (RunWhere::Remote | RunWhere::Auto, _) => {
                    let RemoteStepOutcome { forge_id, result } =
                        self.execute_step_remote(event_index, step, runtime).await;
                    // R555-F6: a `finally` step leaks the same workload record a
                    // main-loop step does. It collects no artifacts, so there is
                    // nothing to retrieve first and the reap is unconditional on
                    // the step's outcome.
                    if let Some(forge_id) = &forge_id {
                        self.reap_remote_workload(forge_id, &step.name).await;
                    }
                    result
                }
            };

            let (status, msg) = match &result {
                Ok(_) => (RunStatus::Success, None),
                Err(e) => {
                    // A failed teardown marks the run Failed (so it's visible),
                    // unless the step opted out with `on_fail = "continue"`. It
                    // never aborts the remaining finally steps.
                    if !matches!(step.on_fail, OnFail::Continue) {
                        overall_status = RunStatus::Failed;
                    }
                    let msg = match e {
                        RunnerError::StepFailed { msg, .. } => Some(msg.clone()),
                        RunnerError::InvalidConfig(m) => Some(m.clone()),
                        other => Some(other.to_string()),
                    };
                    (RunStatus::Failed, msg)
                }
            };
            let completed_at = Utc::now();
            // R717-T2: `secret` applies to `[[finally]]` teardown steps too — a
            // teardown that scrubs a key file is exactly the shape that wants it.
            let msg = match (step.secret, status) {
                (true, RunStatus::Failed) => Some(SECRET_STEP_REDACTED.to_string()),
                (true, _) => None,
                (false, _) => msg,
            };
            self.emit(QedEvent::StepFinished {
                index: event_index,
                name: step.name.clone(),
                status,
                msg: msg.clone(),
                at: completed_at,
            });
            step_statuses.push(StepStatus {
                name: step.name.clone(),
                task_run_id: None,
                status,
                started_at: Some(started_at),
                completed_at: Some(completed_at),
                error: msg,
                outputs: std::collections::HashMap::new(),
                applied_binds: Vec::new(),
                jobs: Vec::new(),
                input_hashes,
            });
        }

        // R766: a FAILED Isolated run keeps its worktree instead of tearing it
        // down, so a later `qed.rerun --from-step` can re-enter the exact tree
        // steps 1..N-1 left behind rather than a fresh checkout that has
        // forgotten their mutations. A successful run has nothing to resume,
        // so it tears down as before (retained_workspace stays `None`).
        // Decided here, BEFORE `dispatch_terminal_outcomes` below, so a
        // failing terminal-outcome dispatch (which aborts this function via
        // `?`, before `QedRunMeta` is even built) still leaves the worktree on
        // disk rather than losing it to the guard's ordinary teardown.
        let retained_workspace = if overall_status == RunStatus::Failed {
            if let Some(guard) = _run_worktree_guard.as_ref() {
                guard.retain();
            }
            run_worktree_path.clone()
        } else {
            None
        };

        // Terminal outcomes (publish / vendor ship / warden deploy) fire off
        // the *work* status snapshotted before `finally` ran, so a flaky
        // teardown never redirects `on_success` → `on_fail`. Extracted to
        // `dispatch_terminal_outcomes` (R603-T4) so the boot reconciler can
        // replay this exact chain for a remote run that reached terminal
        // Success while the daemon was down.
        if work_status == RunStatus::Success {
            self.dispatch_terminal_outcomes(&self.pipeline.on_success, &produced)
                .await?;
        } else {
            self.dispatch_terminal_outcomes(&self.pipeline.on_fail, &produced)
                .await?;
            // R560-F15: a FAILED run still ships the legs that succeeded, but
            // only through the outcomes that declared `per_leg`, and only with
            // those legs' artifacts. `on_fail` went first, so an error from
            // this publish cannot swallow the failure report.
            if work_status == RunStatus::Failed && !whole_leg_produced.is_empty() {
                let per_leg: Vec<Outcome> = self
                    .pipeline
                    .on_success
                    .iter()
                    .filter(|o| matches!(o, Outcome::Publish { per_leg: true, .. }))
                    .cloned()
                    .collect();
                self.dispatch_terminal_outcomes(&per_leg, &whole_leg_produced)
                    .await?;
            }
        }

        let completed_at = Utc::now();
        self.emit(QedEvent::RunFinished {
            status: overall_status,
            at: completed_at,
        });

        Ok((
            QedRunMeta {
                id: self.run_id.clone(),
                pipeline: self.pipeline.name.clone(),
                status: overall_status,
                created_at,
                completed_at: Some(completed_at),
                steps: step_statuses,
                // Step-level failures carry their reason on the failing
                // `StepStatus.error`; a run that completes the step loop has
                // no run-level (outside-any-step) failure to report.
                // R823-F2: a participant verdict is a run-level failure computed
                // after the last step, which is exactly what this field is for.
                failure_reason: run_failure_reason,
                parent_run_id: self.parent_run_id.clone(),
                // The runner has no notion of the matrix row it's running;
                // the daemon re-stamps this from the previously-registered
                // meta after `run()` returns (camp.rs qed_run_matrix_fanout).
                label: None,
                // R717-T3: the run meta is the source of truth for cell state,
                // so this write is what makes any derived index rebuildable by
                // rescanning `.yah/jit/qed/*.json`.
                cell: self.cell.clone(),
                // The resolved map, so "resume from step" can replay this run
                // instead of re-entering `resolve_params` empty-handed. The
                // runner already holds it for the `params.<name>` gating
                // namespace; before this it died with the runner.
                params: self.params.clone(),
                // The runner is handed an already-resolved Pipeline and cannot
                // see the request that shaped it (a step subset arrives as
                // steps that simply aren't there). The daemon carries this
                // over from the meta it registered before spawning.
                launch: None,
                // R766: set only for a FAILED `Isolated` run — see the
                // `retained_workspace` computation above.
                retained_workspace,
                // R876-B8: same shape as `label` and `launch` above — the
                // runner is a library and has no build stamp of its own, so
                // the host that owns the process stamps it on the meta it
                // registers before spawning and carries it across this one.
                executor_build_id: None,
            },
            produced,
        ))
        })
    }

    /// Execute one already-admitted, already-substituted step to completion
    /// (R605-F3).
    ///
    /// Lifted verbatim out of `run_inner`'s step loop when that loop became a
    /// DAG scheduler: several of these are in flight at once now, so the body
    /// can no longer reach the run-level accumulators. Everything it used to
    /// mutate in place — the produced-artifact list, the named-output context,
    /// the status row, the abort decision — comes back on a [`StepOutcome`] and
    /// is folded by the scheduler in declaration order.
    ///
    /// Takes the step **by value** so the returned future owns it: the caller's
    /// substituted copy is a temporary, and a borrow would pin the scheduler's
    /// loop body for as long as the step runs.
    ///
    /// `started_at` / `input_hashes` / `runtime` / `placement` are computed at
    /// admission because they must be — the StepStarted event and the input
    /// digest both have to precede execution, and the placement decides which
    /// admission lane the run takes before this is spawned.
    async fn run_one_step(
        &self,
        index: usize,
        step: crate::types::QedStep,
        started_at: chrono::DateTime<Utc>,
        input_hashes: std::collections::BTreeMap<String, String>,
        runtime: TaskRuntime,
        placement: RunWhere,
        lane: AdmissionLane,
    ) -> StepOutcome {
        // R719-F7 (W298): hold the lane this step's work actually uses. An
        // offloaded step gives the local key back for its duration and the next
        // local step retakes it — before this, a mixed `auto` run held its local
        // lane across hours of fleet build, and R719-F1's camp-global default
        // made that park the whole camp. The scheduler resolved WHICH lane at
        // admission (it is the only place that knows how much other local work
        // is in flight); awaiting it here rather than there keeps a queued run
        // from stalling the steps already running.
        self.enter_lane(lane).await;

        let step = &step;
        let event_index = index + self.index_offset;
        let mut produced: Vec<ProducedArtifact> = Vec::new();
        // step_outputs: key → value collected from this step (W201-F4).
        // step_jobs: per-job rows when this step wraps a GHA workflow
        // (W223 R532-T1); stays empty for every other step kind.
        let mut step_jobs: Vec<crate::types::JobRow> = Vec::new();
        // R590-F6: when a remote step's produced artifacts are retrieved off
        // the build-worker, the path-rewritten list lands here and replaces
        // the raw `step.produces` declarations at the aggregation point
        // below. Stays `None` for local steps and remote steps with no
        // produced artifacts (the common case).
        let mut remote_produced: Option<Vec<ProducedArtifact>> = None;
        let (result, task_run_id, step_outputs) = match step.kind {
            crate::types::StepKind::BuildImage => {
                match self.execute_step_build_image(event_index, step).await {
                    Ok(Some(forge_id)) => (
                        Ok(()),
                        Some(forge_id.to_string()),
                        std::collections::HashMap::new(),
                    ),
                    Ok(None) => (Ok(()), None, std::collections::HashMap::new()),
                    Err(e) => (Err(e), None, std::collections::HashMap::new()),
                }
            }
            crate::types::StepKind::PackageNativeTarball => (
                self.execute_step_package_native_tarball(step).await,
                None,
                std::collections::HashMap::new(),
            ),
            crate::types::StepKind::MuslStaticPreflight => (
                self.execute_step_musl_static_preflight(step).await,
                None,
                std::collections::HashMap::new(),
            ),
            crate::types::StepKind::SignNativeTarball => (
                self.execute_step_sign_native_tarball(step).await,
                None,
                std::collections::HashMap::new(),
            ),
            crate::types::StepKind::SubPipeline => {
                match self
                    .execute_step_sub_pipeline(event_index, step, &mut step_jobs)
                    .await
                {
                    Ok((child_produced, child_outputs)) => {
                        // Aggregation happens here (not below) so child
                        // produces flow into the parent's `Outcome::Publish`
                        // exactly like a sibling step's `produces`. The
                        // generic `produced.extend(step.produces.iter())`
                        // below is a no-op for SubPipeline (validate
                        // rejects direct `produces` on this kind).
                        produced.extend(child_produced);
                        (Ok(()), None, child_outputs)
                    }
                    Err(e) => (Err(e), None, std::collections::HashMap::new()),
                }
            }
            crate::types::StepKind::GhaWorkflow => {
                let cfg = step.gha_workflow.clone();
                let dispatch = match cfg.as_ref() {
                    Some(cfg) => self.execute_step_gha_workflow(event_index, step, cfg, &mut step_jobs).await,
                    None => Err(RunnerError::InvalidConfig(format!(
                        "step `{}`: kind=gha-workflow with no [gha_workflow] block (validate() should have caught this)",
                        step.name,
                    ))),
                };
                match dispatch {
                    Ok((workflow_produced, workflow_outputs)) => {
                        // Same aggregation policy as SubPipeline: the
                        // GHA child's artifacts flow into the parent's
                        // Outcome::Publish in one terminal stage/sync,
                        // not N per workflow job. Job-level outputs are
                        // surfaced as `<job_id>.<key>` so the enclosing
                        // SubPipeline parent's `propagate.outputs` can
                        // pick them up (R488-F6).
                        produced.extend(workflow_produced);
                        (Ok(()), None, workflow_outputs)
                    }
                    Err(e) => (Err(e), None, std::collections::HashMap::new()),
                }
            }
            crate::types::StepKind::Import => {
                match self
                    .execute_step_import(event_index, step, &mut step_jobs)
                    .await
                {
                    Ok((import_produced, import_outputs)) => {
                        // The imported workflow's expansion rolls up exactly
                        // like a GhaWorkflow step (W224 keeps the front-end):
                        // produced artifacts into the parent's terminal
                        // Outcome::Publish, job-level outputs as `<job>.<key>`.
                        produced.extend(import_produced);
                        (Ok(()), None, import_outputs)
                    }
                    Err(e) => (Err(e), None, std::collections::HashMap::new()),
                }
            }
            crate::types::StepKind::WaitFor => (
                self.execute_step_wait_for(event_index, step).await,
                None,
                std::collections::HashMap::new(),
            ),
            crate::types::StepKind::ManifestStitch => (
                self.execute_step_manifest_stitch(event_index, step).await,
                None,
                std::collections::HashMap::new(),
            ),
            crate::types::StepKind::Manual => (
                self.execute_step_manual(event_index, step).await,
                None,
                std::collections::HashMap::new(),
            ),
            crate::types::StepKind::Subprocess => match (placement, runtime) {
                (RunWhere::Local, TaskRuntime::Native) => {
                    // Inject $YAH_OUTPUTS so the step can write key=value
                    // output lines (W201-F4). Read back after exit regardless
                    // of success/failure, then clean up the temp file.
                    //
                    // R717-T2: a `secret` step still GETS $YAH_OUTPUTS; the
                    // runner just never reads it back.
                    //
                    // Withholding the variable was the first cut and it was
                    // wrong: `echo k=v >> "$YAH_OUTPUTS"` against an unset
                    // variable is `>> ""`, which fails — so adding `secret`
                    // to a step would have changed its EXIT CODE. A capture
                    // opt-out must change what is *recorded*, never whether
                    // the step works. (Caught by
                    // `a_secret_step_emits_nothing_into_either_on_disk_sink`.)
                    //
                    // Nothing enters memory: the file is dropped unread, so
                    // no value reaches `step_context` and no downstream
                    // `${{ steps.X.outputs.Y }}` can lift it into an argv
                    // that would be journalled. `validate()` rejects `secret`
                    // alongside a declared `outputs` list, so the silent drop
                    // can only ever hit an *undeclared* key.
                    let outputs_path = std::env::temp_dir()
                        .join(format!("yah-qed-{}-{}.env", &self.run_id, index));
                    let mut yah_env = std::collections::HashMap::new();
                    yah_env.insert(
                        "YAH_OUTPUTS".to_string(),
                        outputs_path.display().to_string(),
                    );
                    let result = self
                        .execute_step_local(event_index, step, Some(&yah_env))
                        .await;
                    let collected = if step.secret {
                        std::collections::HashMap::new()
                    } else {
                        parse_yah_outputs(&outputs_path)
                    };
                    let _ = std::fs::remove_file(&outputs_path);
                    (result, None, collected)
                }
                // R560-B12: a local container step's artifacts are collected off
                // its bound produced dir and land in the same CAS the remote leg
                // uses, so they flow into `remote_produced` — which despite the
                // name is simply "the retrieved, path-rewritten set", the thing
                // that must replace the raw container-path declarations.
                (RunWhere::Local, TaskRuntime::Container) => {
                    match self.execute_step_local_container(event_index, step).await {
                        Ok(collected) => {
                            remote_produced = collected;
                            (Ok(()), None, std::collections::HashMap::new())
                        }
                        Err(e) => (Err(e), None, std::collections::HashMap::new()),
                    }
                }
                (RunWhere::Local, TaskRuntime::MicroVm) => (
                    Err(local_microvm_is_refused(step)),
                    None,
                    std::collections::HashMap::new(),
                ),
                // Auto is resolved to Local/Remote by effective_placement.
                (RunWhere::Remote | RunWhere::Auto, _) => {
                    let RemoteStepOutcome { forge_id, result } =
                        self.execute_step_remote(event_index, step, runtime).await;

                    // R590-F6 leg 2: retrieve any produced artifacts off
                    // the build-worker into camp's content-addressed
                    // store before they feed the publish leg. No-op when
                    // the step declares no `produces`.
                    let mut fetch_failed = false;
                    let result = match (&forge_id, result) {
                        (Some(forge_id), Ok(())) if !step.produces.is_empty() => {
                            match self.retrieve_remote_artifacts(forge_id, step).await {
                                Ok(rp) => {
                                    remote_produced = Some(rp);
                                    Ok(())
                                }
                                Err(e) => {
                                    fetch_failed = true;
                                    Err(e)
                                }
                            }
                        }
                        (_, result) => result,
                    };

                    // R555-F6: THE reap, and the only ordering that works.
                    // yubaba's destroy reaps the produced dir with the record
                    // (R603-T5), so this cannot move above the retrieval above
                    // or into velveteen-exec — either way the artifact is gone
                    // before it is fetched. It runs on a FAILED step too: an
                    // exited container is already reaped by kamaji and
                    // `GET /workloads/{ident}/logs` is still a 501, so keeping
                    // the record buys no post-mortem, only a leak.
                    //
                    // The one exception is a produced-fetch that ERRORED. Those
                    // bytes may still be sitting on the worker, and reaping is
                    // the one action that makes an operator's manual retrieval
                    // impossible — a leaked record on an already-loud failed run
                    // is the cheaper of the two.
                    match (&forge_id, fetch_failed) {
                        (Some(forge_id), false) => {
                            self.reap_remote_workload(forge_id, &step.name).await
                        }
                        (Some(forge_id), true) => tracing::warn!(
                            step = %step.name,
                            forge_id = %forge_id,
                            "leaving forge.{forge_id} on the build-worker: its produced \
                             artifacts could not be retrieved and destroy would reap them",
                        ),
                        (None, _) => {}
                    }

                    // Recorded on BOTH legs now. The id exists as soon as the
                    // workload does, `StepRemoteDispatched` already published
                    // it, and a failed step is exactly when knowing which
                    // workload ran is worth most.
                    (
                        result,
                        forge_id.map(|forge_id| forge_id.to_string()),
                        std::collections::HashMap::new(),
                    )
                }
            },
        };

        let (status, msg) = match &result {
            Ok(_) => {
                // R590-F6: a remote step's retrieved (path-rewritten)
                // artifacts replace the raw container-path declarations, so
                // the publish leg reads the bytes landed in camp.
                match remote_produced.take() {
                    Some(rp) => produced.extend(rp),
                    None => produced.extend(step.produces.iter().cloned()),
                }
                (RunStatus::Success, None)
            }
            // Checked before the generic arm: a cancelled step is not a
            // failure to report as one — see `RunnerError::Cancelled`'s doc.
            Err(RunnerError::Cancelled) => (RunStatus::Cancelled, Some("cancelled".to_string())),
            Err(e) => {
                let msg = match e {
                    RunnerError::StepFailed { msg, .. } => Some(msg.clone()),
                    RunnerError::InvalidConfig(m) => Some(m.clone()),
                    other => Some(other.to_string()),
                };
                (RunStatus::Failed, msg)
            }
        };

        // W209: when the step succeeded, evaluate every bind whose
        // `from` references one of its outputs. Each AppliedBind is
        // persisted on the StepStatus so the qed-run tile (F7) and
        // hash-change hooks (F6) can drive off it. A failed step skips
        // its binds entirely — the source tree should only be touched
        // by receipts that came from a clean run. (Prior steps'
        // already-written binds remain on disk; the operator triages
        // via `git diff`, per W209 § Failure handling.)
        let applied_binds = if status == RunStatus::Success {
            self.apply_step_binds(step, &step_outputs)
        } else {
            Vec::new()
        };

        let completed_at = Utc::now();
        // R717-T2: a `secret` step's failure detail is a stderr tail, and a
        // stderr tail is the single most likely place for the material to
        // surface (`scp: ...: Permission denied` is harmless; a tool echoing
        // its argument is not). It is replaced — not merely dropped — so the
        // card still reads "this step failed" rather than "this step failed
        // for no reason", which is the shape that gets misread as a bug in
        // qed. Both sinks take the same substitute: the live event that
        // becomes `<run_id>.events.jsonl`, and the meta that becomes
        // `<run_id>.json`.
        let msg = match (step.secret, status) {
            (true, RunStatus::Failed) => Some(SECRET_STEP_REDACTED.to_string()),
            (true, _) => None,
            (false, _) => msg,
        };
        // Keep the failure reason on the persisted StepStatus (not only in
        // the live StepFinished event) so `qed.status` surfaces *why* a
        // step failed after the run ends.
        let error = if matches!(status, RunStatus::Failed | RunStatus::Cancelled) {
            msg.clone()
        } else {
            None
        };
        self.emit(QedEvent::StepFinished {
            index: event_index,
            name: step.name.clone(),
            status,
            msg,
            at: completed_at,
        });

        StepOutcome {
            index,
            name: step.name.clone(),
            was_local: placement == RunWhere::Local,
            resource: step.resource.clone(),
            // Cancelled always aborts the run, unconditionally — `on_fail =
            // "continue"` is a policy about tolerating a step's own failure,
            // not about overriding an operator's explicit stop.
            abort: status == RunStatus::Cancelled
                || (status == RunStatus::Failed && !matches!(step.on_fail, OnFail::Continue)),
            produced,
            outputs: step_outputs.clone(),
            row: StepStatus {
                name: step.name.clone(),
                task_run_id,
                status,
                started_at: Some(started_at),
                completed_at: Some(completed_at),
                error,
                outputs: step_outputs,
                applied_binds,
                jobs: step_jobs,
                input_hashes,
            },
        }
    }

    /// R603-T4: dispatch the pipeline's terminal outcomes (Publish / Provider /
    /// YubabaDeploy / AlmanacRun) against a run's produced artifacts. Extracted
    /// verbatim from `run_inner` so the boot reconciler can replay the publish
    /// leg for a remote run that reached terminal Success while the daemon was
    /// down — see [`Self::resume_terminal_publish_for_remote_step`].
    ///
    /// The caller selects `outcomes` off the *work* status (steps + sidecars,
    /// snapshotted before `finally`), so a flaky teardown never redirects
    /// `on_success` → `on_fail`. `run_inner` also calls this a second time for
    /// a failed run, with only the `per_leg` outcomes and only the whole legs'
    /// artifacts (R560-F15).
    async fn dispatch_terminal_outcomes(
        &self,
        outcomes: &[Outcome],
        produced: &[ProducedArtifact],
    ) -> Result<(), RunnerError> {

        // Terminal outcomes operate on the run's produced artifacts, resolved
        // against the run's POSITIONED workspace once, so relative paths work
        // when the process CWD isn't the workspace root (e.g. the Tauri desktop
        // app). A vendor adapter that *transforms* artifacts (notarize staples a
        // bundle, authenticode signs an `.exe`) folds its result back into
        // `staged` so a later outcome in the same chain (sparkle ships the
        // stapled bundle, a Publish syncs the signed binary) sees the
        // transformed file (R509).
        //
        // POSITIONED, not `self.camp_root` — that distinction is the whole bug
        // (R330-T32). Under `workspace = "isolated"` the steps run in a
        // throwaway worktree, so a step that declares `produces = "target/…"`
        // writes into the WORKTREE while this resolved the same relative path
        // against the camp root and handed the publish leg a file that isn't
        // there. It went unnoticed because no isolated pipeline had ever
        // declared a relative `produces`: desktop-release declares none,
        // and release aggregates from a gha-workflow child whose paths are already
        // absolute. `cli-release` is the first, and it would have failed at the
        // last step of a 45-minute release build.
        //
        // Deliberately NOT `resolve_camp_root()`: that falls back to the
        // process CWD. Keep the "no root at all ⇒ leave the path alone" arm
        // exactly as it was and only change WHICH root wins when there is one.
        let version = crate::publish::resolve_release_version();
        let workspace_root = self
            .positioned_workspace
            .get()
            .or(self.camp_root.as_ref());
        let mut staged: Vec<ProducedArtifact> = if let Some(root) = workspace_root {
            produced
                .iter()
                .map(|a| {
                    let p = std::path::Path::new(&a.path);
                    if p.is_relative() {
                        ProducedArtifact {
                            path: root.join(p).to_string_lossy().into_owned(),
                            ..a.clone()
                        }
                    } else {
                        a.clone()
                    }
                })
                .collect()
        } else {
            produced.to_vec()
        };

        for outcome in outcomes {
            match outcome {
                Outcome::YubabaDeploy { service, env } => {
                    self.outcome_dispatcher.yubaba_deploy(service, env).await?;
                }
                Outcome::AlmanacRun { pipeline } => {
                    self.outcome_dispatcher.almanac_run(pipeline).await?;
                }
                Outcome::Publish {
                    provider,
                    bucket,
                    prefix,
                    base_url,
                    require_explicit_version,
                    // Consumed by `run_inner`'s outcome selection, not here.
                    per_leg: _,
                } => {
                    // SubPipeline children with `propagate.produces = true`
                    // have their publish suppressed — the parent owns the
                    // terminal stage/sync/revalidate. YubabaDeploy /
                    // AlmanacRun are NOT suppressed (they may need to run
                    // per-child regardless of who fires the publish).
                    if self.suppress_publish_outcomes {
                        tracing::debug!(
                            run_id = %self.run_id,
                            "suppressing Outcome::Publish on child sub-pipeline run; parent owns the terminal publish"
                        );
                        continue;
                    }
                    // R876-F6: a pipeline that doubles as an iteration loop
                    // (mesofact-musl, run repeatedly with no args) declares
                    // `require_explicit_version = true` so a green build never
                    // resolves the workspace-version fallback into a
                    // publishable number. Refuse loudly rather than silently
                    // skipping, so an operator scanning run output sees why.
                    let publish_version = if *require_explicit_version {
                        match crate::publish::resolve_release_version_explicit() {
                            Some(v) => v,
                            None => {
                                tracing::warn!(
                                    run_id = %self.run_id,
                                    "skipping Outcome::Publish: this outcome requires YAH_RELEASE_VERSION to be set explicitly and it is unset — the build succeeded but no release was cut"
                                );
                                continue;
                            }
                        }
                    } else {
                        version.clone()
                    };
                    let req = crate::publish::PublishRequest {
                        provider: provider.clone(),
                        bucket: bucket.clone(),
                        prefix: prefix.clone(),
                        base_url: base_url.clone(),
                        version: publish_version,
                        artifacts: staged.clone(),
                    };
                    self.outcome_dispatcher.publish(&req).await?;
                }
                Outcome::Provider {
                    provider,
                    with,
                    base_url,
                } => {
                    // Vendor adapters are suppressed on SubPipeline children
                    // exactly like Publish — the parent owns the terminal
                    // vendor ship, so a child that notarized its own bundle and
                    // handed it up would double-submit.
                    if self.suppress_publish_outcomes {
                        tracing::debug!(
                            run_id = %self.run_id,
                            provider = %provider,
                            "suppressing Outcome::Provider on child sub-pipeline run; parent owns the terminal publish"
                        );
                        continue;
                    }
                    // Per-dispatch scratch dir for materialized credentials /
                    // generated artifacts; dropped (and cleaned) at arm exit.
                    let work = tempfile::tempdir()?;
                    let report = {
                        let ctx = crate::provider::ProviderContext {
                            version: &version,
                            artifacts: &staged,
                            base_url: base_url.as_deref(),
                            config: with,
                            work_dir: work.path(),
                            secrets: self.secrets.as_ref(),
                            // Live run path; the per-adapter dry-run check is a
                            // unit-test + `qed validate` plan-time concern.
                            dry_run: false,
                        };
                        self.provider_registry.dispatch(provider, &ctx).await?
                    };
                    for line in &report.actions {
                        tracing::info!(run_id = %self.run_id, provider = %provider, "{line}");
                    }
                    for url in &report.published {
                        tracing::info!(run_id = %self.run_id, provider = %provider, url = %url, "vendor publish");
                    }
                    // Fold transformed/new artifacts back into the working set
                    // so the next outcome in the chain addresses them. An
                    // in-place transform (same path) replaces; a new artifact
                    // (appcast/delta) appends.
                    for art in report.produced {
                        match staged.iter_mut().find(|s| s.path == art.path) {
                            Some(slot) => *slot = art,
                            None => staged.push(art),
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// R603-T4: replay the terminal publish for a remote step that finished
    /// while the camp daemon was down. The boot reconciler
    /// (`camp.rs::finalize_reconciled_run`) rebuilds a fleet-wired runner and
    /// calls this once it confirms the persisted yubaba workload reached a
    /// terminal Success — retrieving the artifact the build produced off the
    /// (possibly already-exited) build-worker and pushing it through the same
    /// `on_success` outcome chain a live run would have fired.
    ///
    /// `step_index` is the pipeline-local index of the remote step whose
    /// `produces` we retrieve; `forge_id` is the persisted workload identity
    /// (the bare `ObsForgeId` uuid — the mesh `forge.<uuid>` prefix is derived
    /// internally by `retrieve_remote_artifacts`).
    ///
    /// Best-effort on the retrieval leg: kamaji reaps exited containers, so a
    /// build that finished *during* the outage may be un-retrievable. This
    /// surfaces that as a `RunnerError` (which the caller renders as "artifact
    /// reaped, re-run") rather than silently claiming published. The robust fix
    /// — the build writing its tar to a durable host volume so retrieval
    /// survives reaping — is tracked as follow-up (see the ticket's reaping-
    /// window fork).
    pub async fn resume_terminal_publish_for_remote_step(
        &self,
        step_index: usize,
        forge_id: &ObsForgeId,
    ) -> Result<(), RunnerError> {
        let step = self.pipeline.steps.get(step_index).ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "resume: step index {step_index} out of range for pipeline `{}` ({} steps)",
                self.pipeline.name,
                self.pipeline.steps.len(),
            ))
        })?;
        // A remote step with no `produces` still fires its terminal outcomes
        // (a YubabaDeploy / AlmanacRun that needs no artifact); retrieval is
        // skipped in that case exactly like the live path (run_inner ~1939).
        let produced = if step.produces.is_empty() {
            Vec::new()
        } else {
            self.retrieve_remote_artifacts(forge_id, step).await?
        };
        // R555-F6: same moment and same reason as the live path — the run is
        // over, the bytes are in camp's store, so the workload record can go.
        // Note the `?` above deliberately skips the reap when retrieval failed:
        // on THIS path that error means "the build finished during the outage
        // and may already be reaped", and destroying the record would remove
        // the operator's last chance at those bytes.
        self.reap_remote_workload(forge_id, &step.name).await;
        self.dispatch_terminal_outcomes(&self.pipeline.on_success, &produced)
            .await
    }

    /// Resolve, configure, and run a SubPipeline child step (R488-F2).
    ///
    /// On success returns the child's [`ProducedArtifact`] list — empty
    /// unless `propagate.produces = true` (in which case the parent's
    /// `Outcome::Publish` aggregates these). On failure returns a clean
    /// `StepFailed` whose `msg` carries the child run's failure tail.
    ///
    /// The child runner inherits the parent's `executor`, `signer`,
    /// `camp_root`, `events`, `outcome_dispatcher`, and
    /// `sub_pipeline_resolver` (so nested SubPipelines recurse with the
    /// same wiring). When `propagate.produces = true`, the child has its
    /// own `Outcome::Publish` suppressed so only the parent fires the
    /// terminal stage/sync/revalidate.
    /// Returns `(produced, outputs)`:
    /// - `produced`: child artifacts to roll up into the parent's publish when
    ///   `propagate.produces = true`; empty otherwise.
    /// - `outputs`: named outputs from the child run projected per
    ///   `propagate.outputs` (W201-F4). The runner scans all child
    ///   `StepStatus::outputs` maps and takes the last writer for each
    ///   declared name. Empty when `propagate.outputs` is empty.
    /// `jobs_out` is forwarded to [`Self::execute_step_gha_workflow`] when the
    /// target is a GHA workflow (the short-circuit path), so the wrapping
    /// sub-pipeline step's `StepStatus` carries the inlined workflow's per-job
    /// rows (W223 R532-T1). Left empty for non-GHA sub-pipeline children —
    /// transparency for `Path` / `Builtin` / `Peer` targets is a later phase.
    async fn execute_step_sub_pipeline(
        &self,
        index: usize,
        step: &crate::types::QedStep,
        jobs_out: &mut Vec<crate::types::JobRow>,
    ) -> Result<
        (
            Vec<crate::types::ProducedArtifact>,
            std::collections::HashMap<String, String>,
        ),
        RunnerError,
    > {
        let Some(cfg) = step.sub_pipeline.as_ref() else {
            return Err(RunnerError::InvalidConfig(format!(
                "step `{}`: kind=sub-pipeline with no [sub_pipeline] block (validate() should have caught this)",
                step.name
            )));
        };

        let Some(mut child) = self.sub_pipeline_resolver.resolve(&cfg.target) else {
            let reason = self
                .sub_pipeline_resolver
                .unresolved_reason(&cfg.target)
                .unwrap_or_else(|| format!(
                    "sub-pipeline target unresolvable: {:?} (no resolver configured, or target not found)",
                    cfg.target
                ));
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: reason,
            });
        };

        // Forward params before constructing the child runner — child sees
        // its TOML with `{{key}}` placeholders substituted.
        //
        // R653-F1: resolve against the CHILD's own `[params]` declarations
        // rather than substituting `cfg.params` raw. Two consequences, both
        // wanted: a child param the parent didn't pass now picks up the
        // child's declared `default` (previously its `{{key}}` survived
        // literally into argv), and a *required* child param nobody supplied
        // fails the step by name instead of silently running a command with
        // `{{key}}` in it. The resolved map is also what the child's `if=`
        // expressions see as `params.<name>`, so gating and substitution agree.
        let child_params = child.resolve_params(&cfg.params).map_err(|e| {
            RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("sub-pipeline `{}`: {e}", sub_pipeline_target_label(&cfg.target)),
            }
        })?;
        child.apply_params(&child_params);

        // EXPAND THE CHILD'S MATRIX HERE, BECAUSE NOTHING ELSE DOES.
        //
        // `matrix::plan` had exactly two production callers, both in the CLI
        // (`app/yah/cli/src/qed.rs` and `camp.rs`), so it ran only for a
        // TOP-LEVEL run. A pipeline reached as a sub-pipeline child was handed
        // to the child runner exactly as the loader parsed it, with every
        // `${{ matrix.* }}` still a literal — the same class of defect
        // transform.rs already records for step-level matrices ("the step runs
        // once with the matrix expression literal in argv").
        //
        // How it surfaced, which is not how you would predict: the literal
        // landed in `platform.target`, and a placeholder string can never equal
        // a host triple, so EVERY step of `yah-desktop-release` resolved
        // `Offload` and `fleet_portability_gate` refused the run at kick with
        // 8 steps "routed to the fleet" — a recipe whose matrix is exactly one
        // row, `aarch64-apple-darwin`, which IS the camp host. Nothing had
        // asked for the fleet; the gate was reading an unexpanded pipeline and
        // was right about what it saw. This blocked `yah-release-wizard`'s
        // `desktop-channel` step (2026-09-20).
        //
        // Placed before the child runner is built because `child_lane`,
        // `child_camp_root` and the struct literal below all read `child` —
        // and `effective_concurrency_key()` can itself be a substituted value.
        //
        // The coord flows into the child runner's `matrix_coord` (see that
        // field below, whose comment already anticipated this) so `if =
        // "matrix.target == '…'"` gating evaluates per row, exactly as it does
        // on the top-level path.
        let child_matrix_coord = if crate::matrix::needs_expansion(&child) {
            let mut planned = crate::matrix::plan(&child);
            // FAN-OUT IS NOT SUPPORTED ON THIS PATH, AND IT REFUSES RATHER THAN
            // PICKING A ROW. A multi-row child is N independent jobs; running
            // only the first would publish a partial release that looks
            // complete, which is worse than not starting. The top-level path
            // fans out properly (qed.rs runs one `PlannedJob` per row) — so the
            // fix for a recipe that needs this is to invoke it directly, or to
            // teach this function to loop. No camp recipe hits it today:
            // `yah-desktop-release` is the only pipeline with a
            // `[pipeline.matrix]` and it declares a single target.
            if planned.len() > 1 {
                return Err(RunnerError::StepFailed {
                    step: step.name.clone(),
                    msg: format!(
                        "sub-pipeline `{}` expands to {} matrix rows, and a sub-pipeline step \
                         runs exactly one child. Running row 1 of {} would look like a complete \
                         run and would not be one. Invoke it directly (`yah qed run {}`), which \
                         fans out one run per row, or collapse its `[pipeline.matrix]` to a \
                         single row.",
                        sub_pipeline_target_label(&cfg.target),
                        planned.len(),
                        planned.len(),
                        child.name,
                    ),
                });
            }
            let job = planned.pop().ok_or_else(|| RunnerError::InvalidConfig(format!(
                "step `{}`: matrix::plan returned no jobs for sub-pipeline `{}` (plan always \
                 yields at least one; this is a bug in matrix::plan, not in the recipe)",
                step.name,
                sub_pipeline_target_label(&cfg.target),
            )))?;
            child = job.pipeline;
            job.coord
        } else {
            None
        };

        // Build a child runner that inherits the parent's wiring. We can't
        // use the existing constructors because they reset every field to
        // defaults; instead, clone parent shape explicitly.
        //
        // R487 follow-up: for SubPipelineRef::GhaWorkflow the resolver
        // synthesises a single-step pipeline whose only step is
        // StepKind::GhaWorkflow. Going through a child runner there is
        // pure paperwork that (a) decouples events so the new
        // GhaEvent → QedEvent bridge can never fire and (b) wraps any
        // inner StepFailed in the generic SubPipeline-level "failed at
        // child step `gha-workflow`" string, erasing the per-job +
        // stderr-tail detail. Short-circuit: execute the GhaWorkflow
        // step directly on `self`, with `self.events` live, then mirror
        // the SubPipelineStarted/Finished bookends so consumers still
        // see the delegation chip.
        if let crate::types::SubPipelineRef::GhaWorkflow { .. } = &cfg.target {
            let target_label = sub_pipeline_target_label(&cfg.target);
            let stub_child_run_id = Uuid::new_v4().to_string();
            self.emit(QedEvent::SubPipelineStarted {
                index,
                name: step.name.clone(),
                target: target_label,
                child_run_id: stub_child_run_id.clone(),
                at: Utc::now(),
            });
            // The synthesised pipeline has exactly one step; pull its
            // GhaWorkflowConfig back out for the direct call.
            let synthesised_step = child.steps.into_iter().next().ok_or_else(|| {
                RunnerError::InvalidConfig(format!(
                    "step `{}`: GhaWorkflow resolver returned an empty pipeline",
                    step.name,
                ))
            })?;
            let synthesised_cfg = synthesised_step.gha_workflow.clone().ok_or_else(|| {
                RunnerError::InvalidConfig(format!(
                    "step `{}`: synthesised GhaWorkflow step carried no [gha_workflow] block",
                    step.name,
                ))
            })?;
            let result = self
                .execute_step_gha_workflow(index, &synthesised_step, &synthesised_cfg, jobs_out)
                .await;
            // W223 R532-F3: opaque opt-out — keep the wrapper a single
            // black-box node by dropping the inlined per-job rows. The
            // workflow still ran and its status still rolls up below.
            if cfg.opaque {
                jobs_out.clear();
            }
            let (status, ret): (RunStatus, Result<_, RunnerError>) = match result {
                Ok((produced, outputs)) => {
                    // Honour propagate.produces: roll up artifacts only
                    // when the parent declared it (mirrors the long-path
                    // SubPipeline behavior — the parent's terminal Publish
                    // stages everything in one go).
                    let out_produced = if cfg.propagate.produces {
                        produced
                    } else {
                        Vec::new()
                    };
                    (RunStatus::Success, Ok((out_produced, outputs)))
                }
                Err(e) => (RunStatus::Failed, Err(e)),
            };
            self.emit(QedEvent::SubPipelineFinished {
                index,
                name: step.name.clone(),
                child_run_id: stub_child_run_id,
                status,
                at: Utc::now(),
            });
            return ret;
        }

        // Peer children execute in the *peer* camp's workspace — the
        // resolver reports its root so subprocess steps (`cargo …`) get the
        // right cwd. Builtin/Path/GhaWorkflow children return None here and
        // inherit the parent's *positioned* workspace (W224 R533-F11): an
        // Isolated parent already moved its tree into a worktree, so the child
        // must build there too, not in the live camp root. `resolve_camp_root`
        // returns the positioned tree (set in run_inner before any step), so the
        // child inherits the worktree and — carrying `parent_run_id` — skips its
        // own repositioning. Without the peer override a `peer-binaries` runs
        // yubaba's `cargo publish -p workload-spec` from yah's root and fails
        // (package not in yah's workspace).
        let child_camp_root = self
            .sub_pipeline_resolver
            .resolved_camp_root(&cfg.target)
            .or_else(|| self.resolve_camp_root().ok());

        // R719-F2 (W298): the child runs under the parent's admission grant and
        // takes no key of its own — see `sub_pipeline_admission_gap` for why
        // that is (a) rather than (b) or (c), and for when it stops being
        // sound. This is the check for the unsound case: a child that wants a
        // lane its parent is not standing in is serialized against nothing.
        //
        // Computed before `child` is moved into the child runner.
        //
        // R719-F7 closes the gap where the daemon installed an
        // [`AdmissionControl`]: the child takes its own lane for the duration
        // of the step and the parent's lane is handed back while it runs (the
        // parent is not building — its child is, somewhere else). Without one,
        // nothing can take a second lane and the warning stands, which is the
        // `yah qed run` / headless case.
        let child_lane: Option<String> = {
            let parent_key = match &self.admission_lane {
                // A child of a child admits against the lane THIS runner is
                // standing in, not against the pipeline key its own parent
                // declared — otherwise a nested child re-reports a gap that
                // was already closed one level up.
                Some(lane) => lane.as_str(),
                None => self.pipeline.effective_concurrency_key(),
            };
            let child_key = child.effective_concurrency_key();
            match sub_pipeline_admission_gap(parent_key, child_key) {
                None => None,
                Some(gap) => {
                    if self.admission.is_some() {
                        tracing::info!(
                            parent_pipeline = %self.pipeline.name,
                            parent_key = %gap.parent_key,
                            child_key = %gap.child_key,
                            step = %step.name,
                            "admission: sub-pipeline child takes its own lane `{}` for the \
                             duration of the step; the parent's `{}` is released while it runs \
                             (R719-F7)",
                            gap.child_key,
                            gap.parent_key,
                        );
                        Some(gap.child_key.to_string())
                    } else {
                        tracing::warn!(
                            parent_pipeline = %self.pipeline.name,
                            parent_key = %gap.parent_key,
                            child_key = %gap.child_key,
                            step = %step.name,
                            "{}",
                            gap.message(&sub_pipeline_target_label(&cfg.target), &step.name),
                        );
                        None
                    }
                }
            }
        };

        let child_run_id = Uuid::new_v4().to_string();
        // R768: ask the host for a channel of the child's own before running
        // it. `None` (no daemon, or a host that declines) keeps the historical
        // silent behaviour; a `Some` makes the child's steps and output
        // observable and durable exactly like a top-level run's. Deliberately
        // NOT `self.events.clone()` — the drain folds events into a registered
        // meta by step index, so sharing the parent's channel would have a
        // child's step 0 overwrite the parent's step 0.
        let child_events = self.child_event_factory.as_ref().and_then(|make| {
            make(&ChildRunInfo {
                run_id: child_run_id.clone(),
                pipeline: child.name.clone(),
                parent_run_id: self.run_id.clone(),
            })
        });
        let child_runner = Self {
            pipeline: child,
            run_id: child_run_id.clone(),
            remote_driver: self.remote_driver.clone(),
            build_context_publisher: self.build_context_publisher.clone(),
            run_where: self.run_where,
            // R833-F8: inherited for the same reason `run_where` is — a child's
            // remote step is dispatched by the parent's driver, so it must land
            // on the node the operator named, not on whatever the tag matcher
            // would have picked for it.
            pinned_node: self.pinned_node.clone(),
            outcome_dispatcher: self.outcome_dispatcher.clone(),
            events: child_events,
            camp_root: child_camp_root,
            // Inherited (R744-T2): a `cargo` step buried in a sub-pipeline runs
            // on this same host, in this same tree, and needs the same
            // toolchain environment the parent's steps got.
            base_env: self.base_env.clone(),
            signer: self.signer.clone(),
            executor: self.executor.clone(),
            sub_pipeline_resolver: self.sub_pipeline_resolver.clone(),
            // Parent owns the terminal publish when propagate.produces is
            // set; otherwise the child's own Outcome::Publish (if any)
            // fires normally and the child's produced are *not* rolled up
            // to the parent (returned as empty below).
            suppress_publish_outcomes: cfg.propagate.produces,
            parent_run_id: Some(self.run_id.clone()),
            // R755: mirrors the step's own opt-out (see the field doc) so
            // `run_inner`'s positioning skip can see it on this instance.
            // R887: unset means inherit, same as an explicit `false` — the
            // case where that default would be a silent mis-composition is
            // refused at load time by `validate_sub_pipeline_graph`, not
            // guessed at here.
            own_workspace: cfg.own_workspace.unwrap_or(false),
            // Inherited, not reset: a grandchild is exactly as invisible as a
            // child was, and `release-wizard → release-check → check →
            // cargo-test` is three levels deep.
            child_event_factory: self.child_event_factory.clone(),
            child_abort_hook: self.child_abort_hook.clone(),
            index_offset: 0,
            steps_are_subset: false,
            // Inherit so a SubPipeline whose child is a gha-workflow
            // step still honors the operator's matrix selection.
            gha_matrix_subset: self.gha_matrix_subset.clone(),
            // Inherit so an `--include-stubbed` pickup of a parent pipeline
            // applies recursively to its sub-pipeline children.
            include_stubbed: self.include_stubbed,
            // Inherit the emulation opt-in (R560): a parent run confirmed with
            // `--allow-emulate` carries that confirmation into its nested
            // sub-pipelines rather than tripping the gate mid-tree.
            allow_emulate: self.allow_emulate,
            force: self.force,
            // Child runs don't inherit the parent's matrix coord — they may
            // themselves be matrix-expanded, and now actually are: this is the
            // coord from the child's OWN `matrix::plan` above, or `None` when
            // it declares no matrix. It was hardcoded `None` while nothing
            // expanded the child, which left `if = "matrix.target == '…'"`
            // unevaluable in a child even once the substitution was fixed.
            matrix_coord: child_matrix_coord,
            // Inherit the parent's host triple (R531-T1): a SubPipeline child
            // executes on the same host, so it shares the parent's platform
            // context rather than re-detecting (which would also lose a
            // with_host_triple override the parent carried).
            host_triple: self.host_triple.clone(),
            // Carry the parent's already-probed toolchain set when present so
            // a child sub-pipeline doesn't re-probe; otherwise a fresh lazy
            // cache (it shares the host, so the result would match anyway).
            // R823-F2: a child sub-pipeline has its OWN pipeline, so it allocates
            // its own set (usually none) rather than inheriting the parent's.
            participant_plan: std::sync::OnceLock::new(),
            cross_availability: match self.cross_availability.get() {
                Some(a) => std::sync::OnceLock::from(*a),
                None => std::sync::OnceLock::new(),
            },
            // Same rationale (R507): inherit the parent's probed host toolchain
            // set when present so a child sub-pipeline doesn't re-probe; the
            // child shares the host, so a fresh lazy cache would match anyway.
            host_toolchains: match self.host_toolchains.get() {
                Some(m) => std::sync::OnceLock::from(m.clone()),
                None => std::sync::OnceLock::new(),
            },
            // Inherit the vendor adapter registry + credential source so a
            // child sub-pipeline whose `Outcome::Provider` *isn't* suppressed
            // (propagate.produces = false) can still resolve its adapter.
            provider_registry: self.provider_registry.clone(),
            secrets: self.secrets.clone(),
            // Inherit the run's target ref so a sub-pipeline whose child is a
            // gha-workflow positions its workspace at the same ref the parent
            // run requested (W224).
            git_ref: self.git_ref.clone(),
            // The child's `params` namespace is the child's OWN resolved params
            // (R653-F1) — the parent's map does not leak in, matching the fact
            // that `apply_params` above substituted only these. A child gating
            // on `params.x` reads the value its parent passed for `x` (or the
            // child's declared default), never the parent's unrelated `x`.
            params: child_params,
            // Child skips repositioning (parent_run_id is Some ⇒ run_inner
            // leaves this unset) and inherits the parent's positioned tree via
            // camp_root above (W224 R533-F11).
            positioned_workspace: std::sync::OnceLock::new(),
            resume_workspace: None,
            // Inherit the human surface: a `kind = "manual"` step buried in a
            // sub-pipeline is no less blocked on a person than a top-level one,
            // and dropping the gate here would silently downgrade it to the
            // headless path (advance-only, or a hard fail).
            manual_gate: self.manual_gate.clone(),
            // R719-F7: inherit the admission control, but not necessarily the
            // lane — a child with an admission gap runs in its OWN lane, and
            // everything it does (its steps, its own children) admits against
            // that key instead of the parent's.
            admission: self.admission.clone(),
            admission_lane: child_lane.clone().or_else(|| self.admission_lane.clone()),
            // R717-T3: NOT inherited — see the field docs.
            cell: None,
        };

        let target_label = sub_pipeline_target_label(&cfg.target);
        self.emit(QedEvent::SubPipelineStarted {
            index,
            name: step.name.clone(),
            target: target_label,
            child_run_id: child_run_id.clone(),
            at: Utc::now(),
        });

        // Spawned (not a bare `Box::pin(...).await`) so a nested child can be
        // cancelled independently of its ancestors: `qed.cancel
        // <child_run_id>` needs a real task to abort, not a future buried
        // inline in the parent's own poll tree. `child_abort_hook` — if the
        // host installed one — is called with the fresh `AbortHandle` right
        // after spawn, before anything can race a cancel arriving between
        // "child exists" and "child is abortable". A declined hook (no
        // daemon, `yah qed run` headless) leaves the child exactly as
        // uncancellable-on-its-own as it was before this existed — cancelling
        // the parent's task still tears the whole recursion down via drop.
        let abort_hook = self.child_abort_hook.clone();
        // `run_inner()` already returns `Pin<Box<dyn Future + Send>>` (see its
        // own doc) — no extra boxing needed here, just await it inside the
        // spawned task.
        let join = tokio::spawn(async move { child_runner.run_inner().await });
        if let Some(hook) = &abort_hook {
            hook(&child_run_id, join.abort_handle());
        }
        let outcome = match join.await {
            Ok(result) => result,
            // `is_cancelled()` is the abort path (a targeted `qed.cancel` on
            // this child, or a `qed.cancel` on an ancestor that walked the
            // descendant tree and aborted this one too) — never a panic, so
            // it's the ONLY JoinError variant translated to `Cancelled`
            // rather than a generic step failure.
            Err(join_err) if join_err.is_cancelled() => Err(RunnerError::Cancelled),
            Err(join_err) => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("sub-pipeline task panicked: {join_err}"),
            }),
        };
        // R719-F7: whatever lane the child left the run standing in — its own,
        // or the fleet lane if its last step offloaded — stand back in ours
        // before returning to the step loop. Same discipline as the R622 manual
        // park's `reacquire_lock`: a step hands control back in the lane it was
        // called in, whatever its outcome.
        self.enter_lane(self.base_lane()).await;
        let (meta, child_produced) = match outcome {
            Ok(pair) => pair,
            Err(e) => {
                // Surface the bookend even when the child runner errored
                // before producing a meta — consumers shouldn't see a
                // dangling Started without a matching Finished.
                self.emit(QedEvent::SubPipelineFinished {
                    index,
                    name: step.name.clone(),
                    child_run_id: child_run_id.clone(),
                    status: RunStatus::Failed,
                    at: Utc::now(),
                });
                return Err(e);
            }
        };

        self.emit(QedEvent::SubPipelineFinished {
            index,
            name: step.name.clone(),
            child_run_id: child_run_id.clone(),
            status: meta.status,
            at: Utc::now(),
        });

        // W223 R532-F3: generalize transparent-by-default to the non-GHA
        // child kinds (Builtin / Path / Peer). The child ran as its own
        // pipeline, so its steps are attributed to this wrapping step as
        // inlined rows — the same treatment GHA jobs get — unless the step
        // opted out via `opaque`. Child qed steps are linear-by-ordering
        // (no `depends_on`), so the rows carry no `needs` edges; the report
        // and graph render them as a flat sequence under the wrapper. The
        // failing-step detail stays on the per-row `error`, mirroring the
        // child's own StepStatus.
        if !cfg.opaque {
            jobs_out.clear();
            jobs_out.extend(meta.steps.iter().map(|s| crate::types::JobRow {
                id: s.name.clone(),
                status: s.status,
                error: (s.status == RunStatus::Failed)
                    .then(|| s.error.clone())
                    .flatten(),
                skip_reason: (s.status == RunStatus::Skipped)
                    .then(|| s.error.clone())
                    .flatten(),
                needs: Vec::new(),
            }));
        }

        if meta.status == RunStatus::Cancelled {
            // The child itself came back Cancelled without this step's own
            // task ever being aborted — e.g. `qed.cancel` targeted a
            // GRANDCHILD, which bubbled up to this child's own `run_inner`
            // (same check, one level down) before we ever awaited the
            // JoinHandle above. Bubble it up one more level rather than
            // reporting it as a plain step failure.
            return Err(RunnerError::Cancelled);
        }

        if meta.status != RunStatus::Success {
            // Surface the child's terminal status as a parent step failure
            // with the failing child step's name in the message — operator
            // sees both layers without needing to chase the nested run.
            let failing = meta
                .steps
                .iter()
                .find(|s| s.status == RunStatus::Failed)
                .map(|s| s.name.as_str())
                .unwrap_or("<unknown>");
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "sub-pipeline `{}` failed at child step `{}` (run_id={})",
                    meta.pipeline, failing, meta.id
                ),
            });
        }

        // Collect named outputs from child steps per propagate.outputs (W201-F4).
        // Scan all child StepStatus::outputs maps; last writer wins for each name.
        let propagated_outputs: std::collections::HashMap<String, String> =
            if cfg.propagate.outputs.is_empty() {
                std::collections::HashMap::new()
            } else {
                let mut collected: std::collections::HashMap<String, String> =
                    std::collections::HashMap::new();
                for child_step in &meta.steps {
                    for name in &cfg.propagate.outputs {
                        if let Some(value) = child_step.outputs.get(name) {
                            collected.insert(name.clone(), value.clone());
                        }
                    }
                }
                collected
            };

        let produced = if cfg.propagate.produces {
            child_produced
        } else {
            Vec::new()
        };
        Ok((produced, propagated_outputs))
    }

    /// Dispatch a [`StepKind::GhaWorkflow`] step into the native W200 GHA
    /// runtime (W200-F9). Reads the workflow YAML at the configured path
    /// (resolved relative to the camp root), parses through
    /// [`yah_qed_gha::parse_workflow`], executes via [`yah_qed_gha::execute_workflow`]
    /// with the tier-1/2 toolkit actions pre-registered (W224 R533-T7 — the
    /// tier-3 service overrides were retired). The delegated path produces no
    /// native publish artifacts; release artifacts come from native QED
    /// publisher steps, so the returned produced-artifact list is always empty.
    ///
    /// The qed-gha runtime is synchronous; we cross the seam via
    /// [`tokio::task::spawn_blocking`] so the runner's tokio reactor stays
    /// responsive (long-running workflow legs like `docker buildx build` would
    /// otherwise stall the executor).
    /// `jobs_out` is populated with one [`crate::types::JobRow`] per GHA job
    /// the workflow ran, regardless of overall success/failure, so the wrapping
    /// step's `StepStatus` carries the workflow's per-job structure transparently
    /// (W223 R532-T1). Left untouched when the run never starts (read / parse /
    /// join failure before any job executes).
    ///
    /// `cfg` is passed explicitly rather than read from `step.gha_workflow` so
    /// the same execution path serves both a `kind = gha-workflow` step (which
    /// passes its own `[gha_workflow]` block) and a `kind = import` step (R533-F1),
    /// whose plan-time expansion synthesizes an equivalent [`GhaWorkflowConfig`]
    /// via [`crate::import::expand_import`]. `step` still supplies the step name,
    /// matrix-subset key, and event index.
    async fn execute_step_gha_workflow(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
        cfg: &crate::types::GhaWorkflowConfig,
        jobs_out: &mut Vec<crate::types::JobRow>,
    ) -> Result<
        (
            Vec<crate::types::ProducedArtifact>,
            std::collections::HashMap<String, String>,
        ),
        RunnerError,
    > {
        // W224 R533-F11: the run already positioned its workspace once (in
        // run_inner, per the pipeline's WorkspaceMode + ref); read the
        // effective tree here rather than repositioning per gha step. For an
        // Isolated run this resolves to the run's worktree, so the workflow
        // reads the ref's copy of release.yml from the same tree every other
        // step builds in. The run-scoped WorktreeGuard (held in run_inner)
        // outlives this step.
        let workspace = self.resolve_camp_root()?;
        let workflow_path = if cfg.path.is_absolute() {
            cfg.path.clone()
        } else {
            workspace.join(&cfg.path)
        };
        let step_name = step.name.clone();
        let event = cfg.event.clone().unwrap_or_else(|| "push".into());
        let inputs = cfg.inputs.clone();
        // R531-T1: thread the self-detected host into the GHA plan context so
        // workflow steps gating on `runner.arch` see the real host this runner
        // executes on (the GHA executor detects its own OS, but QED owns the
        // authoritative host triple). Map the Rust arch token to GHA's
        // `runner.arch` vocabulary (`X64` / `ARM64`).
        let host_arch =
            crate::platform::gha_runner_arch(crate::platform::arch_of(&self.host_triple));
        // R499-F3 phase 2: matrix subset for this step (if any). Empty
        // set isn't a runtime concern — the daemon rejects it before
        // ever constructing the runner.
        let matrix_subset = self.gha_matrix_subset.get(&step.name).cloned();
        // The pipeline's declarative row selector. Composes with the positional
        // subset above rather than replacing it: the RPC subset is an operator
        // narrowing a run they are watching, this is the recipe saying what it is
        // for, and an operator who picks rows in the dashboard should not silently
        // widen what a pinned pipeline builds.
        let matrix_filter = cfg.matrix.clone();

        // Step index of THIS gha-workflow step in the parent qed pipeline,
        // including the resume-time index offset (already baked into
        // `event_index` by the call site). The sync sink → async event
        // forwarder stamps this on every bridged GhaEvent so the receiver
        // can scope the per-job subtree under the right parent step.
        let step_index = event_index;
        let parent_step_name = step.name.clone();

        // R605-F2: hand the GHA-emulator's image builder the SAME
        // remote-dispatch substrate the native `build-image` step kind
        // already uses (RemoteForgeDriver + BuildContextPublisher) — captured
        // here, in the async fn, so `Handle::current()` is unambiguous before
        // crossing into the sync `spawn_blocking` closure below. Both are
        // `None` unless this runner was constructed with fleet dispatch
        // wired (`new_remote` / `with_build_context_publisher`), in which
        // case a slug still has to opt in per-camp via the W200 overlay
        // (`config.remote = true`) — the default stays host-local docker.
        let gha_remote_driver = self.remote_driver.clone();
        let gha_build_context_publisher = self.build_context_publisher.clone();
        let gha_tokio_handle = tokio::runtime::Handle::current();

        // Bridge qed_gha's sync std::sync::mpsc sender into our async
        // event sink (R325-F2). We spawn a forwarder *before* the blocking
        // task so the channel is live the moment the runtime starts
        // emitting; the forwarder ends when the blocking task drops its
        // sender.
        let (gha_tx, gha_rx) = std::sync::mpsc::channel::<yah_qed_gha::GhaEvent>();
        let async_events = self.events.clone();
        let forwarder_name = parent_step_name.clone();
        let forwarder = tokio::task::spawn_blocking(move || {
            while let Ok(ev) = gha_rx.recv() {
                if let Some(sink) = &async_events {
                    let qed_ev = bridge_gha_event(step_index, &forwarder_name, ev);
                    let _ = sink.send(qed_ev);
                }
            }
        });

        // Sync execution off the reactor — qed_gha is blocking by design (it
        // spawns `bash`, `docker`, `git`, etc. via std::process::Command).
        let run = tokio::task::spawn_blocking(move || {
            let yaml =
                std::fs::read_to_string(&workflow_path).map_err(|e| RunnerError::StepFailed {
                    step: step_name.clone(),
                    msg: format!("read workflow {}: {e}", workflow_path.display()),
                })?;
            let workflow = yah_qed_gha::parse_workflow(&yaml).map_err(|e| RunnerError::StepFailed {
                step: step_name.clone(),
                msg: format!("parse {}: {e}", workflow_path.display()),
            })?;
            let secrets = crate::secrets_bridge::SecretsConfig::load_default().resolve_all();
            // R594/R605-F2: inject the docker push-family image builder so the
            // runtime actually builds + pushes the workflow's image jobs
            // instead of declining them with a tier-3 error. It reads the
            // W200 overlay (`.yah/qed/gha-actions.toml` registry_route /
            // registry_auth / remote) to retarget the workflow's hard-coded
            // ghcr.io push to a registry the local token can write, and — when
            // a slug's overlay entry sets `config.remote = true` — to dispatch
            // the build itself to a fleet build-worker via the same
            // RemoteForgeDriver + BuildContextPublisher the native
            // `build-image` step kind uses, instead of requiring a live
            // docker/buildx daemon on this host. Local `docker buildx` stays
            // the default when no slug opts in.
            let image_builder = std::sync::Arc::new(
                crate::image_overlay::QedImageBuilder::new(&workspace, secrets.clone())
                    .with_remote(
                        gha_tokio_handle,
                        gha_remote_driver,
                        gha_build_context_publisher,
                    ),
            );
            // R594: single-host content-addressed artifact store so a job that
            // uploads binaries and a later job that downloads them move files
            // through an on-disk store — the retired upload/download-artifact
            // actions, executed for real. Fleet phase swaps in a transport-backed
            // store for cross-host (build-worker) fetches.
            let artifact_store =
                std::sync::Arc::new(crate::artifact_local::LocalArtifactStore::new());
            let mut executor = yah_qed_gha::Executor::new(&workspace)
                .with_events(gha_tx)
                .with_secrets(secrets)
                .with_image_builder(image_builder)
                .with_artifact_store(artifact_store);
            executor.inputs = inputs_to_value(&inputs);
            executor.github = github_context(&event, &inputs, &workspace);
            executor.runner_arch = host_arch;
            executor.included_instance_keys = matrix_subset;
            executor.matrix_filter = matrix_filter;
            let run = yah_qed_gha::execute_workflow(&workflow, &executor).map_err(|e| {
                RunnerError::StepFailed {
                    step: step_name.clone(),
                    msg: format!("execute {}: {e}", workflow_path.display()),
                }
            })?;
            // Lift each job's `needs:` out of the parsed workflow before it's
            // dropped — the graph viewer renders these as intra-workflow
            // dependency edges between the inlined job nodes (W223 R532-F2).
            let needs_by_job: std::collections::HashMap<String, Vec<String>> = workflow
                .jobs
                .iter()
                .map(|(id, job)| (id.clone(), job.needs.clone()))
                .collect();
            // Drop the executor (and its event sender) so the forwarder loop
            // exits cleanly once it has drained the channel.
            drop(executor);
            Ok::<_, RunnerError>((run, needs_by_job))
        })
        .await
        .map_err(|join_err| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("gha-workflow task panicked: {join_err}"),
        })??;
        let (run, needs_by_job) = run;
        // Wait for the forwarder to drain any tail events before we return —
        // otherwise the parent's `StepFinished` could race ahead of the last
        // few GhaStepOutput lines.
        let _ = forwarder.await;

        // W223 R532-T1: persist the wrapped workflow's per-job structure on the
        // wrapping step. Build one row per job regardless of outcome so the
        // report renders the workflow transparently — success and skipped rows
        // are present too, folding the R516 skip-count into per-row Skipped
        // state rather than a trailing sentence. The flattened failure string
        // below is still produced (it remains the step-level `error`), but the
        // structured rows are now the source of truth for per-job detail.
        jobs_out.clear();
        jobs_out.extend(run.instances.iter().map(|inst| {
            let status = match inst.result {
                yah_qed_gha::JobResult::Success => RunStatus::Success,
                yah_qed_gha::JobResult::Failure | yah_qed_gha::JobResult::Cancelled => RunStatus::Failed,
                yah_qed_gha::JobResult::Skipped => RunStatus::Skipped,
            };
            let error = matches!(inst.result, yah_qed_gha::JobResult::Failure)
                .then(|| gha_job_failure_detail(inst));
            crate::types::JobRow {
                id: inst.job_id.clone(),
                status,
                error,
                skip_reason: inst.skip_reason.clone(),
                needs: needs_by_job.get(&inst.job_id).cloned().unwrap_or_default(),
            }
        }));

        // W224 R533-T7: an imported/delegated GHA workflow produces NO native
        // publish artifacts. The tier-3 `gh-release` override that used to stage
        // them is retired; QED's native publisher steps (W208) own release
        // artifacts now. The transformer (R533-F4) flags a workflow's release
        // step with a native-replacement stanza for the human to wire as a
        // native step — those steps emit `produces`, not this delegated path.
        let produced: Vec<crate::types::ProducedArtifact> = Vec::new();

        // Surface a workflow-level failure as a clean StepFailed enumerating
        // EVERY failing job (and the first failing step inside each), with a
        // stderr tail per job so operators see *why* without having to chase
        // the nested WorkflowRun manually. A single gha-workflow step can fan
        // out to many jobs (e.g. image-yah-*); collapsing to just the first
        // failure (the old `.find`) silently dropped the rest.
        let failing: Vec<&_> = run
            .instances
            .iter()
            .filter(|i| matches!(i.result, yah_qed_gha::JobResult::Failure))
            .collect();
        if !failing.is_empty() {
            let per_job: Vec<String> = failing
                .iter()
                .map(|job| format!("job `{}` {}", job.job_id, gha_job_failure_detail(job)))
                .collect();
            // Reconcile the text report with the job graph: the UI renders every
            // skipped job too, so a report that names only the failures reads as
            // "6 failed" while the screen shows ~20 red/grey rows. Count the
            // skips (downstream jobs gated on a failed/skipped dependency) and
            // say so explicitly, so the gap between "failed N" and "graph shows
            // more" is accounted for rather than mysterious (R516).
            let skipped = run
                .instances
                .iter()
                .filter(|i| matches!(i.result, yah_qed_gha::JobResult::Skipped))
                .count();
            let skip_note = if skipped > 0 {
                format!(
                    "\n\n{skipped} downstream job(s) skipped — gated on a failed or \
                     skipped dependency, not independent failures."
                )
            } else {
                String::new()
            };
            let msg = if per_job.len() == 1 {
                format!(
                    "gha-workflow `{}` failed at {}{}",
                    cfg.path.display(),
                    per_job[0],
                    skip_note,
                )
            } else {
                format!(
                    "gha-workflow `{}` failed in {} jobs:\n\n{}{}",
                    cfg.path.display(),
                    per_job.len(),
                    per_job.join("\n\n"),
                    skip_note,
                )
            };
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg,
            });
        }

        // Lift job-level outputs into a flat HashMap so the parent's
        // SubPipelineCollect::outputs can address them. Naming scheme:
        // `<job_id>.<output_key>` (mirrors GHA's `jobs.<id>.outputs.<key>`
        // mental model). The SubPipeline parent declares which names it
        // wants in `propagate.outputs` and reads them via
        // `${{ steps.<gha-workflow-step>.outputs.<job_id>.<key> }}`.
        // R488-F6.
        let mut outputs: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for instance in &run.instances {
            if !matches!(instance.result, yah_qed_gha::JobResult::Success) {
                continue;
            }
            for (key, value) in &instance.outputs {
                outputs.insert(format!("{}.{}", instance.job_id, key), value.as_str_lossy());
            }
        }

        Ok((produced, outputs))
    }

    /// Dispatch a [`StepKind::Import`] step (W224 "import, don't emulate";
    /// R533-F1). Reads the imported `workflow.yml` source, recomputes its
    /// blake3 content hash, checks it against the pinned hash, then expands the
    /// source into the native subgraph and executes it.
    ///
    /// F1's expansion is the single-node [`crate::import::ImportExpansion::Delegated`]
    /// form: route through the recast W200 GHA front-end (so the import step
    /// actually runs while GHA is canonical). The hash pin is the drift
    /// guardrail; under the default **virtual** expansion a drifted source is
    /// benign — we re-expand from whatever is on disk, only logging the drift.
    /// R533-F4 swaps the expansion body for the mechanical tier-1/2 native map;
    /// R533-F6 wires `materialize` (eject to TOML) + the stale-source guard.
    async fn execute_step_import(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
        jobs_out: &mut Vec<crate::types::JobRow>,
    ) -> Result<
        (
            Vec<crate::types::ProducedArtifact>,
            std::collections::HashMap<String, String>,
        ),
        RunnerError,
    > {
        let Some(cfg) = step.import.as_ref() else {
            return Err(RunnerError::InvalidConfig(format!(
                "step `{}`: kind=import with no [import] block (validate() should have caught this)",
                step.name,
            )));
        };

        let camp_root = self.resolve_camp_root()?;
        let source_path = if cfg.source.is_absolute() {
            cfg.source.clone()
        } else {
            camp_root.join(&cfg.source)
        };

        // Read the source so we can pin/verify its hash. A missing source is a
        // hard error (unlike a drifted hash) — there's nothing to expand.
        let bytes = std::fs::read(&source_path).map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("read import source {}: {e}", source_path.display()),
        })?;
        let actual = crate::import::content_hash(&bytes);

        // Freshness against the pin. Virtual-by-default (the F1 path): a stale
        // source is benign — expand from disk and note the drift. The pin is
        // load-bearing for the materialized eject guard (R533-F6), not for the
        // virtual run, so we never fail the run here.
        match cfg.freshness(&actual) {
            crate::import::ImportFreshness::Fresh => {}
            crate::import::ImportFreshness::Unpinned => {
                tracing::debug!(
                    step = %step.name,
                    source = %source_path.display(),
                    hash = %actual,
                    "import: source not yet pinned; expanding virtually (hash recorded for a future eject)",
                );
            }
            crate::import::ImportFreshness::Stale { pinned, actual } => {
                tracing::warn!(
                    step = %step.name,
                    source = %source_path.display(),
                    %pinned,
                    %actual,
                    "import: source drifted from its pinned hash; re-expanding virtually \
                     (zero-drift by construction — nothing stored to diverge)",
                );
            }
        }

        if cfg.materialize {
            tracing::warn!(
                step = %step.name,
                "import: `materialize = true` is a request to eject to generated TOML, which is \
                 an explicit one-time move (`crate::eject::eject` / `qed eject`), not a per-run \
                 side-effect; proceeding with virtual expansion this run (R533-F6)",
            );
        }

        // Plan-time expansion. F1 yields a single delegated GHA front-end node;
        // F4 will generalize this match with a native-steps arm.
        match crate::import::expand_import(cfg) {
            crate::import::ImportExpansion::Delegated(gha) => {
                self.execute_step_gha_workflow(event_index, step, &gha, jobs_out)
                    .await
            }
        }
    }

    /// Dispatch a [`StepKind::WaitFor`] step (R513-F3, W207 Gap #5): poll the
    /// configured target until it is healthy, then return `Ok(())`; fail the
    /// step if it never comes up within `timeout_secs`.
    ///
    /// Emits a live [`QedEvent::StepOutput`] line per attempt so the QED tail
    /// shows "waiting … (attempt N)" and, on success, "healthy after Nms" —
    /// the same streaming contract a subprocess step has. Cancellation is
    /// structural: on `qed.cancel` the whole run future is dropped, which
    /// drops this loop mid-`sleep`/probe — no lingering poller.
    ///
    /// The probe/backoff loop itself lives in the standalone `pleasehold`
    /// crate ([`crate::waitfor`]); this owns only target resolution and event
    /// emission around it.
    async fn execute_step_wait_for(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
    ) -> Result<(), RunnerError> {
        let Some(cfg) = step.wait_for.as_ref() else {
            return Err(RunnerError::InvalidConfig(format!(
                "step `{}`: kind=wait-for with no [wait_for] block (validate() should have caught this)",
                step.name,
            )));
        };

        // Resolve the probe shape once, up front, so a malformed URL fails the
        // step immediately instead of burning the whole timeout budget retrying
        // an un-parseable target.
        let probe = if let Some(url) = cfg.http.as_ref() {
            crate::waitfor::Probe::http(url, cfg.expect_status).map_err(|msg| {
                RunnerError::StepFailed {
                    step: step.name.clone(),
                    msg: format!("wait-for: {msg}"),
                }
            })?
        } else if let Some(addr) = cfg.tcp.as_ref() {
            crate::waitfor::Probe::tcp(addr)
        } else if let Some(command) = cfg.shell.as_ref() {
            crate::waitfor::Probe::shell(command)
        } else {
            // validate() guarantees exactly one target; defensive only.
            return Err(RunnerError::InvalidConfig(format!(
                "step `{}`: wait-for with no http/tcp/shell target (validate() should have caught this)",
                step.name,
            )));
        };
        let target_label = probe.label();

        let backoff = crate::waitfor::BackoffConfig {
            timeout: std::time::Duration::from_secs(cfg.timeout_secs),
            initial_interval: std::time::Duration::from_millis(cfg.interval_ms),
            max_interval: std::time::Duration::from_millis(cfg.max_interval_ms()),
            multiplier: cfg.backoff_multiplier,
        };

        self.emit(QedEvent::StepOutput {
            index: event_index,
            name: step.name.clone(),
            stream: crate::events::OutputStream::Stdout,
            line: format!(
                "wait-for: polling {target_label} (timeout {}s, interval {}ms, backoff x{})",
                cfg.timeout_secs, cfg.interval_ms, cfg.backoff_multiplier,
            ),
        });

        let outcome = crate::waitfor::poll_until(&probe, &backoff, |a| match a.result {
            Ok(()) => {}
            Err(reason) => self.emit(QedEvent::StepOutput {
                index: event_index,
                name: step.name.clone(),
                stream: crate::events::OutputStream::Stderr,
                line: format!(
                    "wait-for: attempt {} not ready ({reason}); retrying",
                    a.number
                ),
            }),
        })
        .await;

        match outcome {
            Ok(success) => {
                self.emit(QedEvent::StepOutput {
                    index: event_index,
                    name: step.name.clone(),
                    stream: crate::events::OutputStream::Stdout,
                    line: format!(
                        "wait-for: {target_label} healthy after {}ms ({} attempt{})",
                        success.elapsed.as_millis(),
                        success.attempts,
                        if success.attempts == 1 { "" } else { "s" },
                    ),
                });
                Ok(())
            }
            Err(failure) => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "wait-for: {target_label} never became healthy within {}s \
                     ({} attempts; last: {})",
                    cfg.timeout_secs, failure.attempts, failure.last_error,
                ),
            }),
        }
    }

    /// Evaluate a manual step's `advance` condition once: `sh -c <cond>` in the
    /// run's positioned workspace. `Ok(())` on exit 0; `Err(tail)` carries the
    /// combined stdout+stderr tail so a re-park can show the human *why* the
    /// pipeline still doesn't believe them.
    async fn probe_manual_advance(
        &self,
        cond: &str,
        cwd: &std::path::Path,
        step_name: &str,
    ) -> Result<(), String> {
        let out = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(cond)
            .current_dir(cwd)
            .env(crate::types::ENV_MANUAL_RUN_ID, &self.run_id)
            .env(crate::types::ENV_MANUAL_STEP_NAME, step_name)
            .output()
            .await
            .map_err(|e| format!("could not run `{cond}`: {e}"))?;
        if out.status.success() {
            return Ok(());
        }
        let mut tail = String::new();
        for stream in [&out.stdout, &out.stderr] {
            let s = String::from_utf8_lossy(stream);
            let s = s.trim();
            if !s.is_empty() {
                if !tail.is_empty() {
                    tail.push('\n');
                }
                tail.push_str(s);
            }
        }
        let code = match out.status.code() {
            Some(c) => c.to_string(),
            None => "signal".to_string(),
        };
        Err(if tail.is_empty() {
            format!("`{cond}` exited {code} (no output)")
        } else {
            format!("`{cond}` exited {code}:\n{tail}")
        })
    }

    /// Dispatch a [`StepKind::Manual`] step (R622, W282): park the run on a
    /// human and advance when they — or the `advance` condition — say so.
    ///
    /// The order of business matters and is not arbitrary:
    ///
    /// 1. **Probe `advance` first, before parking.** A satisfied condition
    ///    means the human already did the thing (a re-run after a tag was cut,
    ///    say), and interrupting them to confirm what the pipeline can already
    ///    see is exactly the kind of ceremony that trains people to click
    ///    through gates. This is also the "auto-advances the moment it exits 0"
    ///    rule at its first tick.
    /// 2. **Release the `concurrency_key`** before parking, via the gate — a
    ///    parked step isn't using cargo, and holding `cargo-target` overnight
    ///    would stall the camp.
    /// 3. **Park**, racing the human's answer against a poll of `advance`.
    /// 4. **Re-evaluate `advance` on the human's answer.** The tree can move
    ///    during a park, so resume is never a bare continue. A failure re-parks
    ///    with the failing command's output attached rather than failing the
    ///    step — "you thought you tagged it, here's why not" is recoverable.
    /// 5. **Reacquire the lock** before returning to the step loop.
    ///
    /// With no [`ManualGate`] installed (`yah qed run`, tests) step 1 is the
    /// only door: a satisfied `advance` passes, anything else fails with a
    /// message naming the condition. That is deliberate — silently advancing a
    /// human gate because nobody was listening would make the whole kind a lie.
    ///
    /// [`StepKind::Manual`]: crate::types::StepKind::Manual
    async fn execute_step_manual(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
    ) -> Result<(), RunnerError> {
        let Some(cfg) = step.manual.as_ref() else {
            return Err(RunnerError::InvalidConfig(format!(
                "step `{}`: kind=manual with no [manual] block (validate() should have caught this)",
                step.name,
            )));
        };
        // R906-F2 — refuse a late operator gate before parking, not after. The
        // authoring-time lint only warns (a pipeline may legitimately be meant
        // to run attended); this is where a run that would actually strand a
        // human behind a compile fails instead. Same predicate as the lint
        // (`operator_gate_is_late`), so the two verdicts cannot diverge.
        //
        // `ManualParkRequest` carries no audience and needs none — by the time
        // one exists the park has already happened.
        //
        // Placed AHEAD of the `advance` probe, deliberately. A late operator
        // gate is a pipeline authoring defect, and a defect that only surfaces
        // on the runs where some unrelated shell predicate happened to fail is
        // a defect that ships: the release wizard's `advance` auto-advances
        // every rerun of an already-acked version, so a check behind it would
        // stay silent on exactly the reruns. Failing deterministically is what
        // makes `allow_late_operator_block` a deliberate act rather than a
        // thing nobody discovered they needed.
        if cfg.audience == crate::types::ManualAudience::Operator
            && !self.pipeline.allow_late_operator_block
        {
            // A child run is late by construction: whatever invoked it ran
            // first, so a gate nested inside a sub-pipeline can never be the
            // root run's first manual step. No predicate can see that from the
            // child's own steps, which is why it is checked here.
            if self.parent_run_id.is_some() {
                return Err(RunnerError::InvalidConfig(format!(
                    "step `{}`: `audience = \"operator\"` inside a sub-pipeline run \
                     (`{}`, invoked as a child) is late by construction — the parent \
                     reached this pipeline by running everything ahead of it, so this \
                     gate cannot be the run's first manual step and the human is being \
                     asked to authorize a run at an unknown depth into it. Hoist the \
                     gate into the root pipeline ahead of the sub-pipeline step, make \
                     it `audience = \"agent\"`, or set \
                     `allow_late_operator_block = true` on `{}`.",
                    step.name, self.pipeline.name, self.pipeline.name,
                )));
            }
            // A run whose step list is not the AUTHORED list is late by
            // construction, for the same reason and with more force — this is
            // the arm that holds on the paths an operator gate actually meets
            // in anger. Two of them, both trimming `pipeline.steps` before the
            // runner is built (app/yah/cli qed.rs + camp.rs):
            //
            //   * a resume-from-step run `drain(0..from)`s the leading steps
            //     and hands over the offset as a bare `usize` — rerunning a
            //     release is routine;
            //   * a `selected_steps` name-subset filter `retain`s an arbitrary
            //     set, which no offset can even describe.
            //
            // Either way the predicate below would reason over a list missing
            // exactly the steps that were skipped, and clear a gate whose
            // compiling predecessor was among them. A trimmed run cannot prove
            // its gate was first, so it does not get to claim it was.
            //
            // The cost is a false positive on a legitimately-first gate that
            // is not at index 0. `allow_late_operator_block` is the escape for
            // this arm as for the others, and that is the right way round:
            // erring toward refusing beats erring toward stranding a human
            // deep inside a rerun.
            if !self.steps_are_complete() {
                let how = match (self.index_offset, self.steps_are_subset) {
                    (0, _) => "a name-subset selection".to_string(),
                    (n, false) => format!("resumed from step {n}"),
                    (n, true) => format!("resumed from step {n}, then narrowed to a name subset"),
                };
                return Err(RunnerError::InvalidConfig(format!(
                    "step `{}`: `audience = \"operator\"` in a run whose step list is \
                     not the pipeline as authored ({}) is late by construction — the \
                     omitted steps are not reachable from the runner, so this gate \
                     cannot be shown to be the run's first, and a trimmed rerun is \
                     exactly the path that would otherwise slip past the check. Run \
                     `{}` whole, make the gate `audience = \"agent\"`, or set \
                     `allow_late_operator_block = true` on `{}`.",
                    step.name, how, self.pipeline.name, self.pipeline.name,
                )));
            }
            if let Some(finding) = crate::types::operator_gate_is_late(
                &self.pipeline,
                &step.name,
                &*self.sub_pipeline_resolver,
            ) {
                return Err(RunnerError::InvalidConfig(finding.to_string()));
            }
        }
        let cwd = self.resolve_camp_root()?;
        let advance = cfg
            .advance
            .as_deref()
            .map(str::trim)
            .filter(|a| !a.is_empty());

        // 1. Does the condition already hold?
        let mut last_failure = match advance {
            None => None,
            Some(cond) => match self.probe_manual_advance(cond, &cwd, &step.name).await {
                Ok(()) => {
                    self.emit(QedEvent::StepOutput {
                        index: event_index,
                        name: step.name.clone(),
                        stream: OutputStream::Stdout,
                        line: format!(
                            "manual: `{cond}` already satisfied — advancing without parking"
                        ),
                    });
                    return Ok(());
                }
                Err(tail) => Some(tail),
            },
        };

        let Some(gate) = self.manual_gate.clone() else {
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: match (advance, &last_failure) {
                    (Some(cond), Some(tail)) => format!(
                        "manual step needs a human and no answer surface is attached; \
                         its `advance` condition does not hold yet.\n{}\n\nDo the thing, then \
                         re-run — or run under the camp daemon, where the step parks in the \
                         AnswerQueue instead of failing. (condition: `{cond}`)",
                        tail,
                    ),
                    _ => format!(
                        "manual step `{}` needs a human and no answer surface is attached, \
                         and it declares no `advance` condition to verify instead. Run under \
                         the camp daemon (the step parks in the AnswerQueue), or give the step \
                         a `manual.advance` command that proves the work was done.",
                        step.name,
                    ),
                },
            });
        };

        // 2. Release the concurrency key for the duration of the park.
        gate.release_lock().await;
        let outcome = self
            .park_on_human(event_index, step, cfg, advance, &cwd, &*gate, &mut last_failure)
            .await;
        // 5. Reacquire before handing control back to the step loop, whatever
        //    the outcome — a failed/aborted manual step still returns through
        //    the normal path, and the caller's `_permit` must be live again.
        gate.reacquire_lock().await;
        outcome
    }

    /// The park loop of [`Self::execute_step_manual`], factored out so the
    /// lock is released and reacquired on every exit path including `?`.
    #[allow(clippy::too_many_arguments)]
    async fn park_on_human(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
        cfg: &crate::types::ManualConfig,
        advance: Option<&str>,
        cwd: &std::path::Path,
        gate: &dyn ManualGate,
        last_failure: &mut Option<String>,
    ) -> Result<(), RunnerError> {
        let poll_interval = std::time::Duration::from_secs(cfg.advance_poll_secs.max(1));
        loop {
            let req = ManualParkRequest {
                run_id: self.run_id.clone(),
                step_index: event_index,
                step_name: step.name.clone(),
                pipeline: self.pipeline.name.clone(),
                prompt: cfg.prompt.clone(),
                terminal: cfg.terminal.clone(),
                checklist: cfg.checklist.clone(),
                advance: advance.map(str::to_string),
                advance_failure: last_failure.clone(),
            };
            let ManualParkHandle {
                id,
                answer,
                withdraw,
            } = gate
                .park(&req)
                .await
                .map_err(|e| RunnerError::StepFailed {
                    step: step.name.clone(),
                    msg: format!("manual: could not reach a human ({e})"),
                })?;

            self.emit(QedEvent::StepAwaitingHuman {
                index: event_index,
                name: step.name.clone(),
                form_id: id.clone(),
                advance: advance.map(str::to_string),
                audience: cfg.audience,
                prompt: cfg.prompt.clone(),
                terminal: cfg.terminal.clone(),
                at: Utc::now(),
            });
            self.emit(QedEvent::StepOutput {
                index: event_index,
                name: step.name.clone(),
                stream: OutputStream::Stdout,
                line: match advance {
                    Some(cond) => format!(
                        "manual: parked on a human (concurrency key released); \
                         auto-advances when `{cond}` exits 0"
                    ),
                    None => "manual: parked on a human (concurrency key released)".to_string(),
                },
            });

            // Race the human against the condition. Whichever lands first wins;
            // a condition that starts passing on its own withdraws the prompt
            // rather than leaving a stale card in the AnswerQueue.
            let poll = async {
                match advance {
                    None => std::future::pending::<()>().await,
                    Some(cond) => loop {
                        tokio::time::sleep(poll_interval).await;
                        if self.probe_manual_advance(cond, cwd, &step.name).await.is_ok() {
                            return;
                        }
                    },
                }
            };
            tokio::pin!(poll);

            let answered = tokio::select! {
                res = answer => res,
                () = &mut poll => {
                    withdraw();
                    self.emit(QedEvent::StepOutput {
                        index: event_index,
                        name: step.name.clone(),
                        stream: OutputStream::Stdout,
                        line: format!(
                            "manual: `{}` started passing while parked — auto-advancing",
                            advance.unwrap_or_default(),
                        ),
                    });
                    return Ok(());
                }
            };

            match answered {
                Ok(ManualAnswer::Continue) => {}
                Ok(ManualAnswer::Abort { reason }) => {
                    return Err(RunnerError::StepFailed {
                        step: step.name.clone(),
                        msg: format!("manual step declined: {reason}"),
                    });
                }
                // The gate dropped the sender without answering — the daemon
                // restarted, or the form was cancelled out from under us. Fail
                // loudly. A manual step must never resolve itself by accident,
                // and a silent hang is worse than a message that says the
                // prompt went away.
                Err(_) => {
                    return Err(RunnerError::StepFailed {
                        step: step.name.clone(),
                        msg: "manual: the prompt was withdrawn without an answer \
                              (gate closed — daemon restart or cancelled form). \
                              Re-run to park again."
                            .to_string(),
                    });
                }
            }

            // 4. Re-evaluate on resume. The tree can have moved during the
            //    park, so the human's "continue" is a claim, not a proof.
            let Some(cond) = advance else { return Ok(()) };
            match self.probe_manual_advance(cond, cwd, &step.name).await {
                Ok(()) => return Ok(()),
                Err(tail) => {
                    self.emit(QedEvent::StepOutput {
                        index: event_index,
                        name: step.name.clone(),
                        stream: OutputStream::Stderr,
                        line: format!("manual: advance still failing after resume — {tail}"),
                    });
                    *last_failure = Some(tail);
                }
            }
        }
    }

    /// Run one step as a local subprocess via [`Self::executor`] (R438-T14).
    ///
    /// Builds a `ForgeSpec{Subprocess, TaskPlacement{Local, Native}}` from
    /// `step.argv`/`step.cwd`/`step.env` and hands it to the configured
    /// `ForgeExecutor`. The executor drains stdout/stderr; an adapter task
    /// forwards each [`ExecEvent::Output`] as [`QedEvent::StepOutput`] so the
    /// per-line live-stream contract from R325-F2 is preserved. Failure
    /// message uses `ExecOutcome.stderr_tail` (same source the inline
    /// implementation captured).
    /// `extra_env` keys are merged on top of `step.env` — used by `run_inner`
    /// to inject `$YAH_OUTPUTS` for output collection (W201-F4) without
    /// mutating the step.
    async fn execute_step_local(
        &self,
        index: usize,
        step: &crate::types::QedStep,
        extra_env: Option<&std::collections::HashMap<String, String>>,
    ) -> Result<(), RunnerError> {
        if step.argv.is_empty() {
            return Err(RunnerError::InvalidConfig("step argv is empty".to_string()));
        }

        // R531-T6: if F3 resolves this step to the NativeCross tier (a
        // foreign-arch crossable target), route its build onto the host-native
        // cross toolchain (cargo-zigbuild / musl-cross) instead of running the
        // recipe's `cross build` verbatim — the mesofact "stop using the amd64
        // container, use zigbuild" fix. Native-only per W224: imported GHA
        // steps lift their target at import time, they don't reach this seam.
        let mut cross_env: Vec<(String, String)> = Vec::new();
        let effective_step;
        let step: &crate::types::QedStep =
            match self.native_cross_plan(step, &self.cross_availability()) {
                Some(Ok(plan)) => {
                    tracing::info!(
                        target: "qed::nativecross",
                        step = %step.name,
                        tool = plan.tool.label(),
                        "rerouting build to host-native cross: {:?}",
                        plan.argv,
                    );
                    cross_env = plan.env;
                    effective_step = crate::types::QedStep {
                        argv: plan.argv,
                        ..step.clone()
                    };
                    &effective_step
                }
                Some(Err(unavailable)) => {
                    // No host-native toolchain for a target the table said *should*
                    // cross-compile — fail with the install hint rather than fall
                    // through to a confusing linker/manifest error.
                    return Err(RunnerError::StepFailed {
                        step: step.name.clone(),
                        msg: unavailable.to_string(),
                    });
                }
                None => step,
            };

        let spec = build_subprocess_spec(step, TaskRuntime::Native, None);
        let camp_root = self.resolve_camp_root()?;
        let cwd = match step.cwd.as_ref() {
            Some(rel) => camp_root.join(rel),
            None => camp_root,
        };
        let mut merged_env: std::collections::HashMap<String, String> = step
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        // Cross-toolchain env (musl-cross linker/CC/AR) underlays the step's own
        // env and the output-collection extras, so an explicit step `env` still
        // wins on a key collision.
        for (k, v) in cross_env {
            merged_env.entry(k).or_insert(v);
        }
        // R744-T2: the embedder's per-camp toolchain env, the bottom layer of
        // all — it is a default the host computed, not an instruction the
        // recipe gave.
        for (k, v) in &self.base_env {
            merged_env.entry(k.clone()).or_insert_with(|| v.clone());
        }
        // R823-F2: the rendezvous OVERRIDES the step's own env rather than
        // underlaying it — same rule as R560-T8's source-context URL, for the
        // same reason. The port numbers were allocated milliseconds ago by this
        // run's plan, so a literal `QED_PARTICIPANTS` in the TOML can only be a
        // stale copy of a previous run's addressing, and honouring it would
        // point a participant at a peer that isn't there.
        merged_env.extend(self.rendezvous_env(step));
        if let Some(extra) = extra_env {
            merged_env.extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        let ctx = ExecContext::default()
            .with_cwd(cwd)
            .with_env(merged_env.into_iter().collect());
        self.drive_subprocess_step(index, step, spec, ctx).await
    }

    /// Run one step inside a one-shot container (local + container quadrant)
    /// via [`Self::executor`].
    ///
    /// Same flow as [`Self::execute_step_local`] but builds `ForgeSpec` with
    /// `runtime = Container` and an [`Subprocess.image`] resolved through
    /// [`task::default_image::default_forge_image`]. The container `cwd` is
    /// resolved to an absolute path before handoff so the executor's bind
    /// mount matches the host's view (matches the prior inline shape from
    /// R380-T6).
    ///
    /// Image: uses [`task::default_image::default_forge_image`] (resolves
    /// to `yah-rust-bun` since R381-T8). A per-step image catalog (the
    /// rest of R381) lets pipelines pick yah-rust / yah-python / yah-cuda
    /// by name via `task::default_image::catalog_image(name)`.
    /// `Ok(Some(..))` carries the step's `produces` rewritten to camp-local CAS
    /// paths (R560-B12); `Ok(None)` means the step declared none.
    async fn execute_step_local_container(
        &self,
        index: usize,
        step: &crate::types::QedStep,
    ) -> Result<Option<Vec<ProducedArtifact>>, RunnerError> {
        if step.argv.is_empty() {
            return Err(RunnerError::InvalidConfig("step argv is empty".to_string()));
        }
        // R590-F4/R546: a `native = true` cross-arch step demands real silicon of
        // its target arch — its whole contract is "no QEMU" (the rusty-v8-musl
        // forcing case OOMs under emulation). If such a step reaches the
        // local-container path anyway — e.g. a forced `--where local` runner, which
        // `effective_placement` resolves to Local WITHOUT inspecting the step and
        // so bypasses the Offload routing — running it here can only mean Docker
        // silently emulating a foreign-arch image (the `WARNING: The requested
        // image's platform (linux/amd64) does not match the detected host platform
        // (linux/arm64/v8)` case). That is a hard failure, not a warning: refuse to
        // emulate rather than start a build that can't succeed on this host.
        // R555-B10: the DERIVATION verdict — this error asserts the step
        // "declares a native `{target}` build", which is true of a derived
        // Offload and false of a Capability demotion (a NativeCross step whose
        // toolchain is merely missing). A demoted step reaching the container
        // path builds in its image and needs no host toolchain at all.
        if let crate::platform::Resolution::Offload { target } = self.derive_step(step) {
            let arch_tag = crate::platform::build_worker_mesh_tags(
                crate::platform::arch_of(&target),
                crate::platform::os_tag_of(&target),
            )
            .into_iter()
            .find(|t| t.starts_with("arch:"))
            .unwrap_or_else(|| "arch:?".to_string());
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "step '{}' declares a native `{target}` build but is running locally on \
                     host `{}`: a native cross-arch build must offload to an arch-matched \
                     build-worker (`{arch_tag}`), not emulate under QEMU. Re-run with `--where auto` \
                     (policy routes native steps to the fleet) or on a `{target}`-arch host.",
                    step.name, self.host_triple,
                ),
            });
        }
        // Resolve the cwd that gets bind-mounted into the container. The
        // step's optional `cwd` (typically a relative path like
        // `packages/yah/ui`) joins onto the camp root so the mount is always
        // an absolute path. If neither is set we mount the camp root itself.
        let camp_root = self.resolve_camp_root()?;
        let mount_cwd = match step.cwd.as_deref() {
            Some(rel) => camp_root.join(rel),
            None => camp_root,
        };
        // R590-F2: honor a per-step `image = "<name>"` override (R381 seam);
        // fall back to the default forge image (`yah-rust-bun`) for plain steps.
        let image = step_image_override(step)?
            .unwrap_or_else(velveteen_exec::default_image::default_forge_image);
        let spec = build_subprocess_spec(step, TaskRuntime::Container, Some(image));
        // R823-F2: rendezvous env last, so it wins over a stale literal — see
        // the sibling comment in `execute_step_local`.
        let mut env: std::collections::HashMap<String, String> = step
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        env.extend(self.rendezvous_env(step));

        // `source_context` used to be published on the REMOTE path only, so a
        // step that declared one and resolved to local execution reached its
        // argv with `$YAH_SOURCE_CONTEXT_URL` unset and died in the first
        // second. That is not a corner: `.yah/qed/mesofact-musl.toml`'s aarch64
        // leg declares `container_platform = "linux/arm64"`, which makes it
        // genuinely native on the arm64 camp Mac and therefore local — and it
        // failed that way on every one of its runs, 0 for 13, while the x86 leg
        // beside it (offloaded, same argv, same declarations) passed. The
        // pipeline as a whole never reached `on_success`, so its publish never
        // fired either.
        //
        // Publishing for a LOCAL container means an upload the container then
        // downloads over the loopback-ish path, which looks redundant and is
        // the point: the two legs' argvs must stay byte-identical or they run
        // two different builds, and that file says so in as many words. The
        // alternative — a bind mount for the local case — is a second transport
        // to keep in agreement with the first.
        let source_context = self.publish_source_context(step).await?;
        if let Some(published) = &source_context {
            // Same precedence as the remote path: a literal in the TOML can
            // only ever be a stale URL, so ours replaces it rather than
            // merging behind it.
            env.insert(
                crate::build_context::SOURCE_CONTEXT_URL_ENV.to_string(),
                published.url.clone(),
            );
        }

        // R560-B12: `docker run --rm` discards the container's writable layer,
        // so a step's declared `produces` have to be written through a bind or
        // they cease to exist at exit — the step passes, `build-mesofact: PASS`
        // and all, and staging then fails on a path that was never on this
        // filesystem. Run-and-step-scoped so two concurrent legs (they are roots
        // now) cannot land on each other's filenames.
        let produced_dir = if step.produces.is_empty() {
            None
        } else {
            let dir = self
                .resolve_camp_root()?
                .join(".yah/cache/qed/produced")
                .join(&self.run_id)
                .join(tag_to_filename(&step.name));
            std::fs::create_dir_all(&dir).map_err(RunnerError::Io)?;
            Some(dir)
        };

        // R876-F4: the local-placement twin of the remote cache mount. Same
        // container path, same derived key, different host root — a worker has
        // `forge_cache::HOST_ROOT` and yubaba to mkdir under it, a camp Mac has
        // its own cache tree and nobody but this function. Emphatically NOT
        // run-scoped like `produced_dir` above: a per-run cache dir is a cold
        // build wearing a mount.
        let cache_dir = match step_cache_key(&self.pipeline.name, step) {
            None => None,
            Some(key) => {
                let root = self.resolve_camp_root()?.join(".yah/cache/qed/build");
                let dir = workload_spec::forge_cache::cache_dir_under(&root, &key).ok_or_else(
                    || {
                        RunnerError::InvalidConfig(format!(
                            "step `{}`: derived build-cache key `{key}` is not a safe path \
                             component (R876-F4)",
                            step.name,
                        ))
                    },
                )?;
                std::fs::create_dir_all(&dir).map_err(RunnerError::Io)?;
                sweep_build_cache_root(&root);
                env.insert(
                    workload_spec::forge_cache::CACHE_DIR_ENV.to_string(),
                    workload_spec::forge_cache::CONTAINER_DIR.to_string(),
                );
                Some(dir)
            }
        };

        let mut ctx = ExecContext::default()
            .with_cwd(mount_cwd)
            .with_env(env.into_iter().collect());
        if let Some(dir) = &produced_dir {
            ctx = ctx.with_produced_dir(dir.clone());
        }
        if let Some(dir) = &cache_dir {
            ctx = ctx.with_cache_dir(dir.clone());
        }
        let result = self.drive_subprocess_step(index, step, spec, ctx).await;
        // Single-use key, dropped on BOTH legs — a failed build is exactly when
        // the temp object is least wanted and most likely to be forgotten.
        self.discard_source_context(source_context.as_ref()).await;

        // Collect only on success: a failed step's half-written tarball is not
        // an artifact, and surfacing a read error here would mask the build
        // failure that actually caused it.
        match (result, produced_dir) {
            (Ok(()), Some(dir)) => self.collect_local_container_artifacts(step, &dir).map(Some),
            (Ok(()), None) => Ok(None),
            (Err(e), _) => Err(e),
        }
    }

    /// Hand a `(ForgeSpec, ExecContext)` to [`Self::executor`] and translate
    /// the outcome back into the qed runner's error vocabulary. An adapter
    /// task forwards every [`ExecEvent::Output`] into [`QedEvent::StepOutput`]
    /// on the runner's live-event sink — the per-line streaming contract
    /// (R325-F2) is preserved through the trait. `Started`/`Finished` events
    /// from the executor are absorbed; `run()` already brackets every step
    /// with its own `StepStarted`/`StepFinished`.
    async fn drive_subprocess_step(
        &self,
        index: usize,
        step: &crate::types::QedStep,
        spec: ForgeSpec,
        ctx: ExecContext,
    ) -> Result<(), RunnerError> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecEvent>();
        let adapter = {
            // R717-T2: a `secret` step's lines are dropped at the adapter rather
            // than filtered further downstream, so they never enter the event
            // channel the daemon drains into `<run_id>.events.jsonl`. The sink
            // is still consumed to completion — abandoning the receiver would
            // make the executor's send fail and could stall it.
            let events = if step.secret { None } else { self.events.clone() };
            let name = step.name.clone();
            tokio::spawn(async move {
                while let Some(ev) = rx.recv().await {
                    let Some(events) = &events else { continue };
                    if let ExecEvent::Output { stream, line } = ev {
                        let qed_stream = match stream {
                            velveteen_exec::OutputStream::Stdout => OutputStream::Stdout,
                            velveteen_exec::OutputStream::Stderr => OutputStream::Stderr,
                        };
                        let _ = events.send(QedEvent::StepOutput {
                            index,
                            name: name.clone(),
                            stream: qed_stream,
                            line,
                        });
                    }
                }
            })
        };

        let outcome_result = self.executor.execute(spec, ctx, Some(tx)).await;
        let _ = adapter.await;

        match outcome_result {
            Ok(outcome) if outcome.succeeded() => Ok(()),
            // R717-T2: the stderr tail is redacted at the point it is minted,
            // not only where it is written. `RunnerError::StepFailed.msg` is
            // returned to callers other than the step loop (the boot reconciler,
            // sub-pipeline recursion), so redacting only at the journal write
            // would leave a live path carrying the tail.
            Ok(_) if step.secret => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: SECRET_STEP_REDACTED.to_string(),
            }),
            Ok(outcome) => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: outcome.stderr_tail,
            }),
            Err(ForgeExecutorError::Spawn(msg)) => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("failed to spawn (is the runtime installed and accessible?): {msg}"),
            }),
            Err(ForgeExecutorError::Io(e)) => Err(RunnerError::Io(e)),
            Err(ForgeExecutorError::Unsupported(what)) => Err(RunnerError::InvalidConfig(format!(
                "subprocess executor: {what}"
            ))),
            // R555-T2 added this variant for the RemoteForgeDriver's
            // ForgeExecutor impl; this is the *local* subprocess path, so it is
            // not expected here — but it maps cleanly onto the runner's own
            // remote-dispatch error rather than being swallowed as a spawn
            // failure ("is the runtime installed?" is the wrong hint for it).
            Err(ForgeExecutorError::Remote(msg)) => Err(RunnerError::Remote(msg)),
        }
    }

    /// Spawn a background sidecar step (R513-F2) onto its own task and return a
    /// [`JoinHandle`] the caller tracks until reap. Mirrors
    /// [`Self::drive_subprocess_step`] — same `ExecEvent` → `QedEvent::StepOutput`
    /// adapter so a sidecar's logs keep streaming under its step index — but
    /// does NOT await completion: the future runs detached so the step loop
    /// advances immediately.
    ///
    /// Only the `executor` + `events` are captured (both cheap `Arc`/`Sender`
    /// clones) so the spawned future is `'static`. The inner
    /// [`ForgeExecutor::execute`] owns the `kill_on_drop` child, so aborting the
    /// returned handle (reap, cancel, or `run_inner` early-return) kills the
    /// process.
    ///
    /// [`JoinHandle`]: tokio::task::JoinHandle
    fn spawn_background_step(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
        spec: ForgeSpec,
        ctx: ExecContext,
    ) -> tokio::task::JoinHandle<Result<(), RunnerError>> {
        let executor = self.executor.clone();
        // R717-T2: same opt-out as `drive_subprocess_step` — a secret sidecar
        // streams nothing into the journal. Taken here rather than inside the
        // spawned future because `step` is borrowed and the future is 'static.
        let secret = step.secret;
        let events = if secret { None } else { self.events.clone() };
        let name = step.name.clone();
        tokio::spawn(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecEvent>();
            let adapter = {
                let events = events.clone();
                let name = name.clone();
                tokio::spawn(async move {
                    while let Some(ev) = rx.recv().await {
                        let Some(events) = &events else { continue };
                        if let ExecEvent::Output { stream, line } = ev {
                            let qed_stream = match stream {
                                velveteen_exec::OutputStream::Stdout => OutputStream::Stdout,
                                velveteen_exec::OutputStream::Stderr => OutputStream::Stderr,
                            };
                            let _ = events.send(QedEvent::StepOutput {
                                index: event_index,
                                name: name.clone(),
                                stream: qed_stream,
                                line,
                            });
                        }
                    }
                })
            };

            let outcome_result = executor.execute(spec, ctx, Some(tx)).await;
            let _ = adapter.await;

            match outcome_result {
                Ok(outcome) if outcome.succeeded() => Ok(()),
                // R717-T2: see the sibling arm in `drive_subprocess_step`.
                Ok(_) if secret => Err(RunnerError::StepFailed {
                    step: name,
                    msg: SECRET_STEP_REDACTED.to_string(),
                }),
                Ok(outcome) => Err(RunnerError::StepFailed {
                    step: name,
                    msg: outcome.stderr_tail,
                }),
                Err(ForgeExecutorError::Spawn(msg)) => Err(RunnerError::StepFailed {
                    step: name,
                    msg: format!(
                        "failed to spawn (is the runtime installed and accessible?): {msg}"
                    ),
                }),
                Err(ForgeExecutorError::Io(e)) => Err(RunnerError::Io(e)),
                Err(ForgeExecutorError::Unsupported(what)) => Err(RunnerError::InvalidConfig(
                    format!("subprocess executor: {what}"),
                )),
                // See the sibling arm in `drive_subprocess_step` (R555-T2).
                Err(ForgeExecutorError::Remote(msg)) => Err(RunnerError::Remote(msg)),
            }
        })
    }

    /// R823-F2 — spawn a **remote** participant sidecar: a long-lived process
    /// on the named fleet node, dispatched but not awaited.
    ///
    /// The sibling of [`Self::spawn_background_step`] across a host boundary,
    /// and it differs from [`Self::execute_step_remote`] in three ways that are
    /// the substance of this ticket rather than incidental:
    ///
    /// - **Nothing is awaited here.** The dispatch is a network round-trip and
    ///   this is called from the scheduler's admission loop, which must not
    ///   block — a stalled loop stops polling every step already in flight. So
    ///   the whole dispatch-then-wait sequence lives inside the spawned task,
    ///   and the workload id it produces is published back through
    ///   [`RemoteSidecar::forge_id`] for the reap to find.
    /// - **The node is pinned, not matched.** `participant.node` comes straight
    ///   from the plan, and the peers were told this participant's address
    ///   *before* dispatch. Letting admission choose the node here would make
    ///   the rendezvous a lie, so this deliberately does not go through
    ///   [`Self::remote_location`] (which would apply the run's `--where=node:`
    ///   pin over the top of the participant's own binding).
    /// - **No source-context publish, no `produces` retrieval.** A participant
    ///   is a peer to talk to, not a build to collect from; its artifact is the
    ///   traffic it answers, and its verdict is carried by the coordinator.
    ///
    /// Reachability is not this function's doing and is worth knowing: every
    /// remote forge subprocess workload already runs with host networking
    /// (`HOST_NETWORK_ANNOTATION`, applied in `velveteen_exec`'s
    /// `build_workload_spec` since R590-B7), so a participant that binds its
    /// assigned port is answering on the node's own network stack — which is
    /// exactly the address its peers were handed.
    fn spawn_remote_participant_step(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
        participant: &crate::participants::Participant,
    ) -> Result<(tokio::task::JoinHandle<Result<(), RunnerError>>, RemoteSidecar), RunnerError>
    {
        let driver = self.remote_driver.clone().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "step `{}` is participant `{}`, pinned to node `{}`, but no remote \
                 dispatcher is wired — a participant set cannot fall back to local, \
                 because its peers were already told this address",
                step.name,
                participant.name,
                participant.node.as_deref().unwrap_or("?"),
            ))
        })?;
        let node = participant.node.clone().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "step `{}`: participant `{}` has no node — this is a caller bug, \
                 `spawn_remote_participant_step` is only for node-bound participants",
                step.name, participant.name,
            ))
        })?;

        let spec = ForgeSpec {
            command: ForgeCommand::Subprocess {
                argv: step.argv.clone(),
                image: step_image_override(step)?,
            },
            where_: TaskPlacement::new(
                TaskLocation::Remote {
                    node: workload_spec::MeshIdent(node.clone()),
                },
                self.resolve_runtime(step),
            ),
            timeout: step.timeout.map(Millis::from_secs),
            label: Some(step.name.clone()),
            initiator: Initiator::Human { camp: "qed".into() },
            mesh_access: MeshAccess::None,
            cache_key: None,
        };

        // Same env discipline as `execute_step_remote`: the step's declared env
        // travels (R577-F3), `base_env` does not (it names host paths a worker
        // has no referent for), and the rendezvous wins over both.
        let mut env: Vec<(String, String)> = step
            .env
            .iter()
            .filter(|(k, _)| {
                k.as_str() != crate::participants::ENV_PARTICIPANTS
                    && k.as_str() != crate::participants::ENV_PARTICIPANT_SELF
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        env.extend(self.rendezvous_env(step));
        let ctx = ExecContext::default().with_env(env);

        let forge_id: Arc<std::sync::Mutex<Option<ObsForgeId>>> = Arc::new(std::sync::Mutex::new(None));
        let sidecar = RemoteSidecar {
            driver: driver.clone(),
            node: node.clone(),
            forge_id: forge_id.clone(),
        };

        // R717-T2: same secret opt-out as the local sidecar path — taken here
        // because `step` is borrowed and the spawned future is 'static.
        let events = if step.secret { None } else { self.events.clone() };
        let name = step.name.clone();
        let join = tokio::spawn(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecEvent>();
            let adapter = {
                let events = events.clone();
                let name = name.clone();
                tokio::spawn(async move {
                    while let Some(ev) = rx.recv().await {
                        let Some(events) = &events else { continue };
                        if let ExecEvent::Output { stream, line } = ev {
                            let qed_stream = match stream {
                                velveteen_exec::OutputStream::Stdout => OutputStream::Stdout,
                                velveteen_exec::OutputStream::Stderr => OutputStream::Stderr,
                            };
                            let _ = events.send(QedEvent::StepOutput {
                                index: event_index,
                                name: name.clone(),
                                stream: qed_stream,
                                line,
                            });
                        }
                    }
                })
            };

            let handle = driver
                .start_with_context(spec, Some(tx), &ctx)
                .await
                .map_err(|e| RunnerError::Remote(e.to_string()))?;
            // Publish the id BEFORE waiting — R603-T1's discipline, and here it
            // is also what makes teardown possible at all: a reap that fires
            // while this task is still inside `wait()` has to find the id
            // somewhere, and the task cannot hand it over after the fact.
            *forge_id
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(handle.id.clone());
            if let Some(events) = &events {
                let _ = events.send(QedEvent::StepRemoteDispatched {
                    index: event_index,
                    name: name.clone(),
                    forge_id: handle.id.to_string(),
                    at: Utc::now(),
                });
            }

            let status = handle.wait().await;
            let _ = adapter.await;
            match status {
                ForgeStatus::Done { exit_code: 0, .. } => Ok(()),
                ForgeStatus::Done { exit_code, .. } => Err(RunnerError::StepFailed {
                    step: name,
                    msg: format!("participant exited with code {exit_code}"),
                }),
                ForgeStatus::TimedOut { .. } => Err(RunnerError::StepFailed {
                    step: name,
                    msg: "participant timed out".into(),
                }),
                ForgeStatus::Killed { signal, .. } => Err(RunnerError::StepFailed {
                    step: name,
                    msg: format!("participant killed by signal {signal}"),
                }),
                ForgeStatus::Lost { reason } => Err(RunnerError::StepFailed {
                    step: name,
                    msg: format!("participant lost: {reason}"),
                }),
                ForgeStatus::Pending | ForgeStatus::Running => {
                    unreachable!("ForgeRunHandle::wait returns a terminal status")
                }
            }
        });

        Ok((join, sidecar))
    }
}

/// Translate a [`yah_qed_gha::GhaEvent`] into the qed-runner's own
/// [`crate::QedEvent::Gha*`] variant, stamping the parent step's index and
/// name so the desktop pane can scope nested rows under the right step
/// (W200 R487 follow-up).
fn bridge_gha_event(
    step_index: usize,
    parent_name: &str,
    ev: yah_qed_gha::GhaEvent,
) -> crate::QedEvent {
    use yah_qed_gha::GhaEvent as G;
    let at = chrono::Utc::now();
    match ev {
        G::JobStarted {
            job_id,
            matrix_index,
            key,
            total_steps,
        } => crate::QedEvent::GhaJobStarted {
            index: step_index,
            name: parent_name.to_string(),
            job_id,
            matrix_index,
            job_key: key,
            total_steps,
            at,
        },
        G::JobFinished {
            job_id: _,
            matrix_index: _,
            key,
            result,
        } => crate::QedEvent::GhaJobFinished {
            index: step_index,
            name: parent_name.to_string(),
            job_key: key,
            result: gha_result_str(result).to_string(),
            at,
        },
        G::StepStarted {
            job_id,
            matrix_index,
            step_index: gha_step_index,
            step_id,
            name: step_name,
            action_kind,
        } => crate::QedEvent::GhaStepStarted {
            index: step_index,
            name: parent_name.to_string(),
            job_key: instance_key(&job_id, matrix_index),
            step_index: gha_step_index,
            step_id,
            step_name,
            action_kind,
            at,
        },
        G::StepOutput {
            job_id,
            matrix_index,
            step_index: gha_step_index,
            stream,
            line,
        } => crate::QedEvent::GhaStepOutput {
            index: step_index,
            name: parent_name.to_string(),
            job_key: instance_key(&job_id, matrix_index),
            step_index: gha_step_index,
            stream: match stream {
                yah_qed_gha::GhaOutputStream::Stdout => crate::events::OutputStream::Stdout,
                yah_qed_gha::GhaOutputStream::Stderr => crate::events::OutputStream::Stderr,
            },
            line,
        },
        G::StepFinished {
            job_id,
            matrix_index,
            step_index: gha_step_index,
            conclusion,
            msg,
            outputs: _,
        } => crate::QedEvent::GhaStepFinished {
            index: step_index,
            name: parent_name.to_string(),
            job_key: instance_key(&job_id, matrix_index),
            step_index: gha_step_index,
            conclusion: gha_conclusion_str(conclusion).to_string(),
            msg,
            at,
        },
    }
}

/// Same key format as [`yah_qed_gha::JobInstance::key`] — `"<job>"` for non-matrix
/// jobs, `"<job>#<row>"` for matrix rows. Kept in sync by construction; the
/// receiver pairs Start / Finish by exact-string compare.
fn instance_key(job_id: &str, matrix_index: Option<usize>) -> String {
    match matrix_index {
        Some(idx) => format!("{job_id}#{idx}"),
        None => job_id.to_string(),
    }
}

fn gha_result_str(r: yah_qed_gha::JobResult) -> &'static str {
    match r {
        yah_qed_gha::JobResult::Success => "success",
        yah_qed_gha::JobResult::Failure => "failure",
        yah_qed_gha::JobResult::Cancelled => "cancelled",
        yah_qed_gha::JobResult::Skipped => "skipped",
    }
}

fn gha_conclusion_str(c: yah_qed_gha::StepConclusion) -> &'static str {
    match c {
        yah_qed_gha::StepConclusion::Success => "success",
        yah_qed_gha::StepConclusion::Failure => "failure",
        yah_qed_gha::StepConclusion::Skipped => "skipped",
    }
}

/// Last `lines` non-blank lines of `stderr`, with qed-gha's internal
/// `$GITHUB_ENV` sidechannel marker stripped (see `pop_env_updates` in
/// yah_qed_gha::runtime). Empty when there is nothing useful left to show.
fn stderr_tail(stderr: &str, lines: usize) -> String {
    const ENV_PREFIX: &str = "__qed_gha_env_updates_BEGIN__";
    const ENV_SUFFIX: &str = "__qed_gha_env_updates_END__";
    let cleaned: String = stderr
        .lines()
        .filter(|l| {
            let t = l.trim();
            !t.starts_with(ENV_PREFIX) && !t.starts_with(ENV_SUFFIX) && !t.is_empty()
        })
        .collect::<Vec<_>>()
        .join("\n");
    if cleaned.is_empty() {
        return String::new();
    }
    let trimmed: Vec<&str> = cleaned.lines().collect();
    let start = trimmed.len().saturating_sub(lines);
    trimmed[start..].join("\n")
}

/// Render the failure detail for one failed GHA job instance: the first failing
/// step's name plus its stderr tail. Shared by the flattened step-level failure
/// summary (which prefixes the job id) and the structured per-job rows (W223
/// R532-T1, where the [`crate::types::JobRow`] already carries the job id, so
/// this is the row's `error` text without the redundant prefix).
fn gha_job_failure_detail(job: &yah_qed_gha::InstanceRun) -> String {
    let failing_step = job
        .steps
        .iter()
        .find(|s| matches!(s.conclusion, yah_qed_gha::StepConclusion::Failure));
    match failing_step {
        Some(s) => {
            let label = s
                .name
                .clone()
                .or_else(|| s.step_id.clone())
                .unwrap_or_else(|| "<unnamed>".to_string());
            let tail = stderr_tail(&s.stderr, 20);
            if tail.is_empty() {
                format!("step `{label}` (no stderr)")
            } else {
                format!("step `{label}`:\n{tail}")
            }
        }
        None => "(no failing step recorded — likely an override / scheduler error)".to_string(),
    }
}

/// Build a minimal `yah_qed_gha::Value` object from a string map. Used to lower
/// `[gha_workflow] inputs = { tag = "v1" }` into the runtime's `inputs.*`
/// expression context.
fn inputs_to_value(inputs: &std::collections::HashMap<String, String>) -> yah_qed_gha::Value {
    let mut m: indexmap::IndexMap<String, yah_qed_gha::Value> = indexmap::IndexMap::new();
    for (k, v) in inputs {
        m.insert(k.clone(), yah_qed_gha::Value::String(v.clone()));
    }
    yah_qed_gha::Value::Object(m)
}

/// RAII guard for a `WorkspaceMode::Isolated` git worktree (W224). Dropping it
/// runs `git worktree remove --force` so a release run — including one that
/// errors mid-step — never leaves an orphaned tree behind. Best-effort: a
/// failed removal is swallowed (the next run's pre-add cleanup clears it).
///
/// R766: `retain` opts a *failed* run's worktree out of that teardown so a
/// resume can re-enter it — see [`Self::retain`]. `Cell` rather than a plain
/// `bool` because the guard is dropped by value (immutable `&self` in every
/// caller's scope; nothing holds `&mut` at drop time) — [`PipelineRunner::run_inner`]
/// only learns the run's terminal status *after* the guard was created, so
/// the flag has to be settable through a shared reference.
///
/// R874-B2: `locked` records that this run holds a `git worktree lock` on the
/// tree (see [`worktree_lock_reason`]). Drop releases it on every path, retained
/// or not — the lock means "a run is building in here", and past drop none is.
#[derive(Debug)]
struct WorktreeGuard {
    camp_root: std::path::PathBuf,
    worktree: std::path::PathBuf,
    retain: std::cell::Cell<bool>,
    locked: bool,
}

impl WorktreeGuard {
    /// Skip teardown on drop — the worktree survives for a later resume.
    fn retain(&self) {
        self.retain.set(true);
    }
}

impl Drop for WorktreeGuard {
    fn drop(&mut self) {
        // Before the remove, too: `git worktree remove --force` refuses a
        // locked tree.
        if self.locked {
            let _ = run_git(
                &self.camp_root,
                &["worktree", "unlock", &self.worktree.to_string_lossy()],
            );
        }
        if self.retain.get() {
            return;
        }
        let _ = std::process::Command::new("git")
            .current_dir(&self.camp_root)
            .args(["worktree", "remove", "--force"])
            .arg(&self.worktree)
            .output();
    }
}

/// The `git worktree lock --reason` a qed run stamps on the isolated worktree
/// it builds in (R874-B2). Carries the pid of the process doing the build, so a
/// reader can tell a live lock from one a crashed run left behind — git keeps
/// the lock forever otherwise. The retention sweep (yah's `qed_worktrees`)
/// parses it back with [`parse_worktree_lock_reason`]; keep the two in step.
pub fn worktree_lock_reason(run_id: &str, pid: u32) -> String {
    format!("qed-run {run_id} pid {pid}")
}

/// Inverse of [`worktree_lock_reason`]: `(run_id, pid)`, or `None` for any
/// lock qed did not write (an operator's bare `git worktree lock`).
pub fn parse_worktree_lock_reason(reason: &str) -> Option<(&str, u32)> {
    let rest = reason.trim().strip_prefix("qed-run ")?;
    let (run_id, pid) = rest.rsplit_once(" pid ")?;
    let pid = pid.parse().ok()?;
    (!run_id.is_empty()).then_some((run_id, pid))
}

/// Lock `worktree` on behalf of `run_id`, held by this process.
fn lock_worktree(
    camp_root: &std::path::Path,
    worktree: &std::path::Path,
    run_id: &str,
) -> Result<(), String> {
    run_git(
        camp_root,
        &[
            "worktree",
            "lock",
            "--reason",
            &worktree_lock_reason(run_id, std::process::id()),
            &worktree.to_string_lossy(),
        ],
    )
}

/// Run a git command in `dir`, mapping a non-zero exit to its trimmed stderr.
fn run_git(dir: &std::path::Path, args: &[&str]) -> Result<(), String> {
    let out = std::process::Command::new("git")
        .current_dir(dir)
        .args(args)
        .output()
        .map_err(|e| format!("spawn git: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

/// Porcelain path prefixes that are camp *runtime* state, not build source:
/// the daemon rewrites the turso databases under `.yah/db/` continuously (and
/// the shared-tree peer model sweeps them into `wip` commits), so they show as
/// tracked-dirty on essentially every run. They never change which source bytes
/// a build compiles or a release tags, so gating a `checkout`/`isolated` run on
/// them would refuse every pipeline in a live camp for no safety benefit.
const DIRTY_CHECK_IGNORED_PREFIXES: &[&str] = &[".yah/db/"];

/// True when the working tree has uncommitted *tracked* changes that matter to
/// a build. Untracked files are ignored (`--untracked-files=no`): they don't
/// change which committed bytes a build sees, and a working camp almost always
/// carries some. Tracked changes confined to [`DIRTY_CHECK_IGNORED_PREFIXES`]
/// (camp runtime DBs) are also ignored — see that constant for why.
fn git_tree_is_dirty(dir: &std::path::Path) -> Result<bool, RunnerError> {
    let out = std::process::Command::new("git")
        .current_dir(dir)
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .map_err(RunnerError::Io)?;
    if !out.status.success() {
        return Err(RunnerError::InvalidConfig(format!(
            "git status failed in {}: {}",
            dir.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    let dirty = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .any(|line| !porcelain_path_is_ignored(line));
    Ok(dirty)
}

/// Given one `git status --porcelain` line (`XY <path>`, or `XY orig -> new`
/// for a rename), return true when its path is under a
/// [`DIRTY_CHECK_IGNORED_PREFIXES`] runtime prefix. Unknown/short lines are
/// treated as *not* ignored (fail safe: a line we can't parse still counts as
/// dirty). A rename is ignored only when its destination path is runtime state.
fn porcelain_path_is_ignored(line: &str) -> bool {
    // Porcelain v1: 2 status columns + a space, then the path (byte 3 on).
    let Some(rest) = line.get(3..) else {
        return false;
    };
    // Rename/copy entries read `orig -> new`; the destination is what the tree
    // now carries, so key the decision off it.
    let path = rest.rsplit(" -> ").next().unwrap_or(rest);
    // Git quotes paths with unusual chars ("path"); strip a leading quote so
    // the prefix match still fires on the (plain-ASCII) runtime DB paths.
    let path = path.trim().trim_start_matches('"');
    DIRTY_CHECK_IGNORED_PREFIXES
        .iter()
        .any(|prefix| path.starts_with(prefix))
}

/// Synthesize a `github` expression context for a GhaWorkflow step from the
/// camp's live git state. `release.yml` references `github.sha` (the
/// `:smoke-<sha>` image tag), `github.ref_name` (tarball stage dirs + the
/// `!contains(ref_name, '-')` smoke gate) and `github.actor` (ghcr login),
/// so leaving these empty produced malformed `:smoke-` tags and `cli--<triple>`
/// stage names. We read them from the workspace's git checkout, mirroring what
/// a real runner gets from the push event:
///   sha      = `git rev-parse HEAD` (full 40-char, matching GHA)
///   ref_name = exact tag if HEAD is tagged, else the current branch
///   actor    = `git config user.name`
/// Each lookup degrades to empty on error (detached/dirty/no-git) rather than
/// failing the step — an empty field is no worse than the old behaviour.
///
/// `inputs` is ALSO laid at `github.event.inputs.*`, not just the `inputs.*`
/// namespace (R330-T32). A `workflow_dispatch` workflow reads its own inputs
/// both ways, and `release.yml` picks the release version with
/// `${{ github.event.inputs.tag || github.ref_name }}`. With `github.event`
/// left empty — as it was — that expression silently fell through to the
/// BRANCH name on any untagged run, so a dispatched release published
/// `yah/main/manifest.json` and wrote `"version": "main"` into the permanent
/// release index. An empty `github.event` is not a harmless stub here; it is
/// the difference between a release and a corrupt one.
fn github_context(
    event_name: &str,
    inputs: &std::collections::HashMap<String, String>,
    workspace: &std::path::Path,
) -> yah_qed_gha::Value {
    let git = |args: &[&str]| -> String {
        std::process::Command::new("git")
            .current_dir(workspace)
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default()
    };

    let sha = git(&["rev-parse", "HEAD"]);
    // Prefer an exact tag (the real release trigger shape) over the branch.
    let exact_tag = git(&["describe", "--tags", "--exact-match"]);
    let (ref_name, ref_full) = if !exact_tag.is_empty() {
        (exact_tag.clone(), format!("refs/tags/{exact_tag}"))
    } else {
        let branch = git(&["rev-parse", "--abbrev-ref", "HEAD"]);
        let full = if branch.is_empty() {
            String::new()
        } else {
            format!("refs/heads/{branch}")
        };
        (branch, full)
    };
    let actor = git(&["config", "user.name"]);
    // `github.repository` — `<owner>/<repo>`, derived from the origin remote so
    // the tier-2 env floor (`$GITHUB_REPOSITORY`) and any `${{ github.repository }}`
    // in a workflow resolve to the same thing GHA would say. Both SSH
    // (`git@host:owner/repo.git`) and HTTPS (`https://host/owner/repo.git`)
    // remote shapes reduce to the trailing two path segments. Empty when there
    // is no origin — the floor skips empty values rather than exporting `""`.
    let repository = repo_slug_from_remote(&git(&["remote", "get-url", "origin"]));

    let mut m: indexmap::IndexMap<String, yah_qed_gha::Value> = indexmap::IndexMap::new();
    m.insert(
        "event_name".into(),
        yah_qed_gha::Value::String(event_name.into()),
    );
    m.insert(
        "repository".into(),
        yah_qed_gha::Value::String(repository),
    );
    m.insert("ref".into(), yah_qed_gha::Value::String(ref_full));
    m.insert("ref_name".into(), yah_qed_gha::Value::String(ref_name));
    m.insert("sha".into(), yah_qed_gha::Value::String(sha));
    m.insert("actor".into(), yah_qed_gha::Value::String(actor));
    let mut event: indexmap::IndexMap<String, yah_qed_gha::Value> = indexmap::IndexMap::new();
    event.insert("inputs".into(), inputs_to_value(inputs));
    m.insert("event".into(), yah_qed_gha::Value::Object(event));
    yah_qed_gha::Value::Object(m)
}

/// Reduce a git remote URL to GHA's `<owner>/<repo>` slug. Returns the empty
/// string for anything that doesn't yield two trailing path segments (no
/// remote, a bare local path) — the caller treats empty as "unknown".
fn repo_slug_from_remote(url: &str) -> String {
    let url = url.trim().trim_end_matches('/');
    let url = url.strip_suffix(".git").unwrap_or(url);
    // Both remote shapes put owner/repo in the last two `/`- or `:`-delimited
    // segments: `git@github.com:yah-ai/yah` and `https://github.com/yah-ai/yah`.
    let tail: Vec<&str> = url.rsplitn(3, ['/', ':']).collect();
    match tail.as_slice() {
        [repo, owner, ..] if !repo.is_empty() && !owner.is_empty() => format!("{owner}/{repo}"),
        _ => String::new(),
    }
}

/// Lower a `QedStep` into a `ForgeSpec` for the local subprocess executor
/// (R438-T14). The image is `Some` for the container path and `None` for
/// native — the executor branches on `where_.runtime` and rejects a missing
/// image when it needs one.
fn build_subprocess_spec(
    step: &crate::types::QedStep,
    runtime: TaskRuntime,
    image: Option<workload_spec::ImageRef>,
) -> ForgeSpec {
    ForgeSpec {
        command: ForgeCommand::Subprocess {
            argv: step.argv.clone(),
            image,
        },
        where_: TaskPlacement::new(TaskLocation::Local, runtime),
        timeout: step.timeout.map(Millis::from_secs),
        label: Some(step.name.clone()),
        initiator: Initiator::Human { camp: "qed".into() },
        mesh_access: MeshAccess::None,
        cache_key: None,
    }
}

/// R719-F2 (W298): a sub-pipeline child that needs admission its parent never
/// paid for.
///
/// # The invariant, and why it is (a) and not (b) or (c)
///
/// The daemon's per-key mutex map lives in `camp.rs`; the runner reaches it
/// only at *top level*, in `qed_run_handler`. `execute_step_sub_pipeline` builds
/// a child runner in-process and calls it directly, so a child never queues on
/// anything. R719-F2 named three ways to close that:
///
/// - **(a) the child inherits the parent's grant.** The parent already holds a
///   key for the whole of its run, including this step. Adopted.
/// - **(b) the child re-locks on its own key.** A trap, and R719-F1 made it a
///   worse one: with the camp-global default, an unkeyed child under an unkeyed
///   parent now shares `@camp`, so (b) would self-deadlock on the *default*
///   configuration rather than on an unlucky one. Do not re-propose this.
/// - **(c) a reentrant admission handle from the daemon.** Correct, and much
///   more machinery than rung 1 justifies — the runner would need a live handle
///   back into `qed_locks` across a process boundary. **Adopted in R719-F7**:
///   that handle is [`AdmissionControl`], and where one is installed the child
///   now takes the lane this function names, for the duration of the step, with
///   the parent's lane released while it runs. It is *not* re-entrant locking —
///   the control releases before it acquires, so no run ever holds two lanes.
///
/// So this predicate has two readings now, and both matter. With an
/// `AdmissionControl` installed (the camp daemon) it names the lane the child
/// should be moved into. Without one (`yah qed run`, headless) nothing can take
/// a second lane, (a) still applies, and it is a warning.
///
/// (a) is sound exactly when the parent's key **covers** the child's: the child
/// must want either the same key, or `@parallel` (no key at all). This function
/// is the predicate for the case where it does not — the child asks for a lane
/// the parent is not standing in, so nothing anywhere serializes it.
///
/// It reports rather than refuses, for two reasons. Peer children are *stamped*
/// `peer:<camp>` by the resolver (R494-F2), so a legitimate `peer-release`
/// orchestrating three peer camps trips this on every step; refusing would break
/// a working shape. And W298 rung 1 is admission *visibility* — turning a
/// previously-silent concurrent run into a hard failure is rung 3's call, not
/// this one's.
pub fn sub_pipeline_admission_gap<'a>(
    parent_key: &'a str,
    child_key: &'a str,
) -> Option<AdmissionGap<'a>> {
    if child_key == parent_key || child_key == crate::types::PARALLEL_CONCURRENCY_KEY {
        return None;
    }
    Some(AdmissionGap {
        parent_key,
        child_key,
    })
}

/// A child sub-pipeline running outside any admission lane. See
/// [`sub_pipeline_admission_gap`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionGap<'a> {
    /// The key the parent run holds for its whole duration.
    pub parent_key: &'a str,
    /// The key this child would have taken had it been launched at top level.
    pub child_key: &'a str,
}

impl AdmissionGap<'_> {
    /// The warning text for a run with no [`AdmissionControl`] installed —
    /// where the gap is real and unclosable, because nothing can take a second
    /// lane. Names both keys and the fix, because "admission gap" alone tells a
    /// reader nothing they can act on.
    ///
    /// Under the camp daemon this text is never emitted: R719-F7 moves the
    /// child into its own lane instead, and the gap is closed rather than
    /// reported.
    pub fn message(&self, target_label: &str, step: &str) -> String {
        format!(
            "admission gap: sub-pipeline `{target_label}` (step `{step}`) wants concurrency key \
             `{}`, but its parent holds `{}`. This run has no admission control installed, so a \
             sub-pipeline child inherits the parent's grant (W298 / R719-F2) and takes no key of \
             its own — this child is serialized against nothing, and a top-level run on `{}` can \
             execute concurrently with it. Run it under the camp daemon, where the child takes \
             its own lane (R719-F7); or fix the recipe by \
             giving the parent `concurrency_key = \"{}\"`, or by setting the child to \
             \"@parallel\" if it genuinely shares no resource.",
            self.child_key, self.parent_key, self.child_key, self.child_key,
        )
    }
}

/// Human-readable token for a [`SubPipelineRef`] (R488-F5). Surfaced on
/// `QedEvent::SubPipelineStarted.target` so a consumer can label the child run
/// without a back-reference to the parent pipeline TOML. Matches the
/// resolver-token discipline used by `validate_sub_pipeline_graph` and the
/// in-memory test resolver: `builtin:<name>`, `path:<path>`, `gha:<path>`.
fn sub_pipeline_target_label(target: &crate::types::SubPipelineRef) -> String {
    match target {
        crate::types::SubPipelineRef::Builtin(n) => format!("builtin:{n}"),
        crate::types::SubPipelineRef::Path(p) => format!("path:{}", p.display()),
        crate::types::SubPipelineRef::GhaWorkflow { path, .. } => {
            format!("gha:{}", path.display())
        }
        crate::types::SubPipelineRef::Peer { camp, pipeline } => {
            format!("peer:{camp}:{pipeline}")
        }
    }
}

/// R506: peel one layer of `${{ … }}` delimiters off an `if=` body so a
/// pipeline author can write either `if = "matrix.target == 'mac'"` or
/// `if = "${{ matrix.target == 'mac' }}"` interchangeably. Mirrors GHA's
/// implicit-expression-body semantics for the job-level `if:` key.
fn strip_expr_delimiters(input: &str) -> &str {
    let t = input.trim();
    if let Some(inner) = t.strip_prefix("${{").and_then(|s| s.strip_suffix("}}")) {
        inner.trim()
    } else {
        t
    }
}

/// Substitute `${{ steps.STEP_NAME.outputs.KEY }}` placeholders in `s`
/// using the accumulated step context (W201-F4). Unknown placeholders are
/// left untouched — downstream tooling (or the W200 expression engine once
/// R487-F2 ships) handles them. The pattern is intentionally minimal: no
/// expression evaluation, no escaping, no nested references.
fn substitute_step_context(
    s: &str,
    context: &std::collections::HashMap<String, std::collections::HashMap<String, String>>,
) -> String {
    let mut out = s.to_string();
    for (step_name, outputs) in context {
        for (key, value) in outputs {
            let pattern = format!("${{{{ steps.{step_name}.outputs.{key} }}}}");
            out = out.replace(&pattern, value);
        }
    }
    out
}

/// The one `${{ host.* }}` field: the triple of the machine actually executing
/// the step, from the runner's own self-detection (R531-T1 — a pipeline never
/// hard-codes the machine it runs on).
///
/// It exists because a host-native *producing* pipeline is otherwise
/// inexpressible. `produces` needs a literal path and a literal triple in TOML,
/// and the one value that cannot be literal in a recipe meant to run on any dev
/// box is that box's triple. Without this, a CLI-only release recipe has to
/// either demand the operator type their triple as a run param or publish every
/// platform's download under one undifferentiated filename.
const HOST_TRIPLE_TOKEN: &str = "${{ host.triple }}";

fn substitute_host_context(s: &str, host_triple: &str) -> String {
    if s.contains(HOST_TRIPLE_TOKEN) {
        s.replace(HOST_TRIPLE_TOKEN, host_triple)
    } else {
        s.to_string()
    }
}

/// Apply both substitution passes (`${{ steps.X.outputs.Y }}` and
/// `${{ host.triple }}`) to a step, returning `None` when neither has anything
/// to do so the caller can skip the clone.
///
/// `produces` is rewritten alongside `argv`/`env`, which the step-output pass
/// alone never did. A step cannot reference its *own* outputs there — they do
/// not exist until it exits — but it can name a path a prior step computed, and
/// it always needs `${{ host.triple }}`, which is the whole reason this pass
/// reaches `produces` at all.
fn substituted_step(
    step: &crate::types::QedStep,
    step_context: &std::collections::HashMap<String, std::collections::HashMap<String, String>>,
    host_triple: &str,
) -> Option<crate::types::QedStep> {
    let mentions_host = step.argv.iter().any(|a| a.contains(HOST_TRIPLE_TOKEN))
        || step.env.values().any(|v| v.contains(HOST_TRIPLE_TOKEN))
        || step.produces.iter().any(|p| {
            p.path.contains(HOST_TRIPLE_TOKEN)
                || p.triple
                    .as_deref()
                    .is_some_and(|t| t.contains(HOST_TRIPLE_TOKEN))
        });
    if step_context.is_empty() && !mentions_host {
        return None;
    }
    let sub =
        |v: &str| substitute_host_context(&substitute_step_context(v, step_context), host_triple);

    let mut s = step.clone();
    s.argv = s.argv.iter().map(|a| sub(a)).collect();
    for v in s.env.values_mut() {
        *v = sub(v);
    }
    for p in s.produces.iter_mut() {
        p.path = sub(&p.path);
        if let Some(t) = p.triple.as_mut() {
            *t = sub(t);
        }
    }
    Some(s)
}

/// The refusal for a `runtime = microvm` step that resolved to local placement
/// (R605-F8).
///
/// `InvalidConfig` rather than `StepFailed`: nothing ran and nothing could,
/// because this is a pipeline-authoring mistake and not a transient condition —
/// exactly the shape the local+container guard already uses (see this module's
/// R325 gotcha about wanting a pre-flight validator hook for both).
///
/// The reason it is a mistake at all: a microVM isolates a build from the *rest
/// of the node*, and on a dev box that is the author's own machine. Booting a
/// guest kernel to protect a developer from their own build costs a boot and
/// buys nothing, so the honest answer is to say so rather than to quietly run
/// it in a container and let the pipeline believe it got isolation.
fn local_microvm_is_refused(step: &crate::types::QedStep) -> RunnerError {
    RunnerError::InvalidConfig(format!(
        "step `{}` declares runtime = microvm, which is remote-only: a microVM isolates a \
         build from whatever else the node is running, and locally that is you. Use \
         runtime = container (or native) for a local run, or place the step remotely \
         (R605-F8 / W325)",
        step.name
    ))
}

/// Parse a `KEY=VALUE\n`-formatted file written by a step to `$YAH_OUTPUTS`.
/// Lines that don't contain `=` are silently skipped (e.g. blank lines or
/// comment lines). Returns an empty map if the file doesn't exist or can't
/// be read — steps that emit no outputs are the common case.
fn parse_yah_outputs(path: &std::path::Path) -> std::collections::HashMap<String, String> {
    let Ok(content) = std::fs::read_to_string(path) else {
        return std::collections::HashMap::new();
    };
    content
        .lines()
        .filter_map(|line| {
            let (k, v) = line.split_once('=')?;
            let k = k.trim().to_string();
            if k.is_empty() {
                return None;
            }
            Some((k, v.to_string()))
        })
        .collect()
}

impl PipelineRunner {
    /// Dispatch a `kind = "build-image"` step.
    ///
    /// Catalog lookup + Dockerfile staging is shared across local and remote:
    ///
    /// 1. Look up the catalog entry named by `step.image` (R381-T1 bundled +
    ///    per-camp).
    /// 2. Compile a Dockerfile via [`crate::images::compile_with_dockerfile_dir`]
    ///    (sibling Dockerfile wins; otherwise the TOML layering shorthand is
    ///    rendered). Per-camp dir is `<camp_root>/.yah/qed/images/<name>/`.
    /// 3. Write the Dockerfile under `.yah/cache/buildkit/<name>.Dockerfile`.
    ///
    /// Local path then calls [`task::local::build_image_command`] (docker
    /// buildx); remote path synthesises a BuildKit-in-containerd workload via
    /// [`task::remote::RemoteForgeDriver`] and waits for the terminal status.
    /// Both paths surface step output through the shared QedEvent sink — for
    /// remote, the per-line stream flows through scryer (`forge.remote`
    /// target) rather than this runner directly, mirroring
    /// [`Self::execute_step_remote`].
    async fn execute_step_build_image(
        &self,
        index: usize,
        step: &crate::types::QedStep,
    ) -> Result<Option<ObsForgeId>, RunnerError> {
        use std::process::Stdio;
        use tokio::io::{AsyncBufReadExt, BufReader};

        let prepared = self.prepare_build_image(step)?;

        // R633: route on the step's EFFECTIVE placement, not the runner's raw
        // `--where`. Under the default `Auto` a `native = true` cross-arch
        // build-image step resolves to Offload → Remote, exactly like a
        // subprocess step; reading `self.run_where` here made every Auto run
        // build on the qed host regardless, which is how a foreign-arch image
        // silently came out host-arch (or emulated).
        if matches!(self.effective_placement(step), RunWhere::Remote) {
            let forge_id = self
                .execute_step_build_image_remote(step, &prepared)
                .await?;
            return Ok(Some(forge_id));
        }

        // Local docker daemon: refuse to build a foreign platform here. buildx
        // would happily do it under QEMU, which for a from-source toolchain
        // image is either wrong-by-construction or an OOM — the same refusal
        // `execute_step_local_container` makes for foreign-arch container steps.
        self.refuse_foreign_platform_locally(step)?;

        let context = prepared.context_dir.as_path();

        let cmd = {
            let opts = velveteen_exec::local::BuildImageOptions {
                dockerfile: &prepared.dockerfile_path,
                context,
                tag: &prepared.tag,
                push: step.push,
                load: step.load,
                cache_dir: Some(&prepared.buildkit_dir),
                oci_archive: if step.push || step.load {
                    None
                } else {
                    Some(&prepared.archive_path)
                },
                // R633: `platforms` now comes from the step (`platforms = [...]`
                // in TOML, `--platform` on `yah qed images build`). Empty keeps
                // the pre-R633 behaviour: buildx builds the daemon's own
                // platform. Foreign entries were rejected above.
                platforms: &step.platforms,
                build_args: &[],
            };
            velveteen_exec::local::build_image_command(&opts)
        };

        let mut cmd = cmd;
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("failed to spawn `docker buildx`: {e}"),
        })?;
        let stdout = child.stdout.take().expect("stdout piped above");
        let stderr = child.stderr.take().expect("stderr piped above");

        let stdout_task = {
            let events = self.events.clone();
            let name = step.name.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(tx) = &events {
                        let _ = tx.send(QedEvent::StepOutput {
                            index,
                            name: name.clone(),
                            stream: OutputStream::Stdout,
                            line,
                        });
                    }
                }
            })
        };

        let stderr_task = {
            let events = self.events.clone();
            let name = step.name.clone();
            tokio::spawn(async move {
                let mut captured: Vec<String> = Vec::new();
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(tx) = &events {
                        let _ = tx.send(QedEvent::StepOutput {
                            index,
                            name: name.clone(),
                            stream: OutputStream::Stderr,
                            line: line.clone(),
                        });
                    }
                    captured.push(line);
                }
                captured
            })
        };

        let status = child.wait().await.map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("waiting on `docker buildx` failed: {e}"),
        })?;
        let _ = stdout_task.await;
        let stderr_lines = stderr_task.await.unwrap_or_default();

        if !status.success() {
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: stderr_lines.join("\n").trim().to_string(),
            });
        }
        Ok(None)
    }

    /// `kind = manifest-stitch` (R590-F2): fold N per-arch source images into
    /// one multi-arch manifest list via `docker buildx imagetools create`.
    ///
    /// Registry-only — the per-arch builds already pushed their arch-specific
    /// tags to the registry (routed to the arch-matched build-worker fleet);
    /// this step just writes the manifest-list tag. It always runs host-native
    /// (see [`Self::resolve_runtime`]) even under `--where=remote`, so it shells
    /// `docker buildx` on the qed host and streams output like the local
    /// build-image path.
    async fn execute_step_manifest_stitch(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
    ) -> Result<(), RunnerError> {
        use std::process::Stdio;
        use tokio::io::{AsyncBufReadExt, BufReader};

        let Some(cfg) = step.manifest_stitch.as_ref() else {
            return Err(RunnerError::InvalidConfig(format!(
                "step `{}`: kind=manifest-stitch with no [manifest_stitch] block (validate() should have caught this)",
                step.name,
            )));
        };

        self.emit(QedEvent::StepOutput {
            index: event_index,
            name: step.name.clone(),
            stream: OutputStream::Stdout,
            line: format!(
                "manifest-stitch: creating `{}` from [{}]",
                cfg.target,
                cfg.sources.join(", "),
            ),
        });

        let mut cmd = velveteen_exec::local::imagetools_create_command(&cfg.target, &cfg.sources);
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("failed to spawn `docker buildx imagetools create`: {e}"),
        })?;
        let stdout = child.stdout.take().expect("stdout piped above");
        let stderr = child.stderr.take().expect("stderr piped above");

        let stdout_task = {
            let events = self.events.clone();
            let name = step.name.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(tx) = &events {
                        let _ = tx.send(QedEvent::StepOutput {
                            index: event_index,
                            name: name.clone(),
                            stream: OutputStream::Stdout,
                            line,
                        });
                    }
                }
            })
        };

        let stderr_task = {
            let events = self.events.clone();
            let name = step.name.clone();
            tokio::spawn(async move {
                let mut captured: Vec<String> = Vec::new();
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(tx) = &events {
                        let _ = tx.send(QedEvent::StepOutput {
                            index: event_index,
                            name: name.clone(),
                            stream: OutputStream::Stderr,
                            line: line.clone(),
                        });
                    }
                    captured.push(line);
                }
                captured
            })
        };

        let status = child.wait().await.map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("waiting on `docker buildx imagetools create` failed: {e}"),
        })?;
        let _ = stdout_task.await;
        let stderr_lines = stderr_task.await.unwrap_or_default();

        if !status.success() {
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: stderr_lines.join("\n").trim().to_string(),
            });
        }
        Ok(())
    }

    /// R633: reject a local build-image step whose declared `platforms` include
    /// an arch this host cannot build natively.
    ///
    /// `docker buildx --platform linux/amd64` on an arm64 daemon does not fail —
    /// it emulates through QEMU, silently and slowly, and for a from-source
    /// toolchain image (rusty-v8-musl-builder) that is either an OOM or a
    /// wrong-by-construction artifact. The honest answer is a hard error naming
    /// the mesh tier that *can* build it, so the operator's next move is
    /// `--where auto` (offload) rather than a six-hour emulated build.
    ///
    /// An unrecognized platform string is let through: buildx owns that
    /// vocabulary and will produce a better message than a guess would.
    fn refuse_foreign_platform_locally(
        &self,
        step: &crate::types::QedStep,
    ) -> Result<(), RunnerError> {
        let host_arch = crate::platform::arch_of(&self.host_triple);
        for platform in &step.platforms {
            let Some(want) = crate::platform::docker_platform_arch(platform) else {
                continue;
            };
            if want == host_arch {
                continue;
            }
            // Docker `--platform` values are always Linux container images
            // (buildx has no darwin/windows image target), so the OS is
            // implicit here rather than derived from a triple.
            let arch_tag = crate::platform::build_worker_mesh_tags(want, "linux")
                .into_iter()
                .find(|t| t.starts_with("arch:"))
                .unwrap_or_else(|| "arch:?".to_string());
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "build-image step '{}' targets platform `{platform}` ({want}) but this host \
                     is `{}`: building it here means QEMU emulation, not a native image. Declare \
                     `platform = {{ native = true, target = \"...\" }}` on the step and run with \
                     `--where auto` so it routes to a `{arch_tag}` build-worker, or run on a {want} host.",
                    step.name, self.host_triple,
                ),
            });
        }
        Ok(())
    }

    /// Shared catalog-lookup + Dockerfile-staging path used by both local and
    /// remote build-image dispatch.
    fn prepare_build_image(
        &self,
        step: &crate::types::QedStep,
    ) -> Result<PreparedBuildImage, RunnerError> {
        let camp_root = self.resolve_camp_root()?;
        let camp_images_dir = camp_root.join(".yah/qed/images");
        let catalog = crate::images::CatalogManifest::load(&camp_images_dir)
            .map_err(|e| RunnerError::InvalidConfig(format!("failed to load catalog: {e}")))?;

        let image_name = step.image.as_deref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "build-image step `{}` is missing the `image` field (parse-time validation should have caught this)",
                step.name
            ))
        })?;

        let entry = catalog.get(image_name).cloned().ok_or_else(|| {
            let known: Vec<&str> = catalog.names();
            RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "unknown catalog image `{image_name}` — known: {known:?}. \
                     Per-camp images live at `.yah/qed/images/<name>/`."
                ),
            }
        })?;

        // R633: resolve the entry's context directory across the whole search
        // path, not just the per-camp slot. A bundled entry's Dockerfile lives
        // in the qed crate's own `images/<name>/`; looking only under
        // `.yah/qed/images/` meant every bundled entry with a real Dockerfile
        // silently fell back to its (near-empty) TOML layering.
        let image_dir = crate::images::resolve_image_dir(&camp_root, image_name)
            .map(|rel| camp_root.join(rel))
            .unwrap_or_else(|| camp_images_dir.join(image_name));
        let dockerfile_text = crate::images::compile_with_dockerfile_dir(
            &entry, &catalog, &image_dir,
        )
        .map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("Dockerfile compile failed for `{image_name}`: {e}"),
        })?;

        let cache_root = camp_root.join(".yah/cache");
        let buildkit_dir = cache_root.join("buildkit");
        let archive_dir = cache_root.join("images");
        std::fs::create_dir_all(&buildkit_dir).map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("failed to create {}: {e}", buildkit_dir.display()),
        })?;
        std::fs::create_dir_all(&archive_dir).map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("failed to create {}: {e}", archive_dir.display()),
        })?;

        let dockerfile_path = buildkit_dir.join(format!("{image_name}.Dockerfile"));
        std::fs::write(&dockerfile_path, &dockerfile_text).map_err(|e| {
            RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("failed to write {}: {e}", dockerfile_path.display()),
            }
        })?;

        let tag = step
            .tag
            .clone()
            .unwrap_or_else(|| format!("{image_name}:dev"));
        let safe_tag = tag_to_filename(&tag);
        let archive_path = archive_dir.join(format!("{safe_tag}.tar"));

        let context_dir = match &step.context {
            Some(ctx) => camp_root.join(ctx),
            None => camp_root,
        };

        Ok(PreparedBuildImage {
            dockerfile_path,
            context_dir,
            buildkit_dir,
            archive_path,
            tag,
        })
    }

    /// The arch a remote build-image step should be routed to (R636).
    ///
    /// A build-image step that resolves to [`Offload`](crate::platform::Resolution::Offload)
    /// — i.e. it declares a foreign-arch target via `platform.native = true` —
    /// must build on a worker of that *target* arch, so the tier is derived from
    /// the Offload target. Any other resolution means the step has no cross-arch
    /// target (a plain host-native build-image forced remote with `--where
    /// remote`), so it builds for the runner's own arch. Returns an owned
    /// `String` because the Offload target is owned.
    fn remote_build_image_arch(&self, step: &crate::types::QedStep) -> String {
        match self.resolve_step(step) {
            crate::platform::Resolution::Offload { target } => {
                crate::platform::arch_of(&target).to_string()
            }
            _ => crate::platform::arch_of(&self.host_triple).to_string(),
        }
    }

    /// Pack the step's build context and hand it to the wired transport,
    /// returning `(publish_key, url)` (R636-B1).
    ///
    /// The Dockerfile is *compiled* from the catalog into `.yah/cache/buildkit/`,
    /// which is outside the context directory, so it is injected into the tar
    /// at the root under its own basename — the same basename the workload's
    /// `--opt filename=` names. That keeps the remote build reading exactly the
    /// Dockerfile `yah qed images show` prints, not whatever file of that name
    /// happened to be sitting in the context.
    async fn publish_build_context(
        &self,
        step: &crate::types::QedStep,
        prepared: &PreparedBuildImage,
        forge_key: &str,
    ) -> Result<String, RunnerError> {
        let publisher = self
            .build_context_publisher
            .clone()
            .unwrap_or_else(|| Arc::new(crate::build_context::NoBuildContextPublisher));

        let dockerfile_name = prepared
            .dockerfile_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "staged Dockerfile {} has no filename component",
                    prepared.dockerfile_path.display()
                ),
            })?
            .to_string();
        let dockerfile_bytes =
            std::fs::read(&prepared.dockerfile_path).map_err(|e| RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("reading {}: {e}", prepared.dockerfile_path.display()),
            })?;

        let tarball = crate::build_context::pack_context(
            &prepared.context_dir,
            &[(dockerfile_name, dockerfile_bytes)],
        )
        // pack_context labels its errors "build-image"; re-stamp with the real
        // step name so the failure points at a line in the operator's pipeline.
        .map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: e.to_string(),
        })?;

        if let Some(tx) = &self.events {
            let _ = tx.send(QedEvent::StepOutput {
                index: 0,
                name: step.name.clone(),
                stream: OutputStream::Stderr,
                line: format!(
                    "build context: {} ({} KiB packed) → uploading for the build-worker",
                    prepared.context_dir.display(),
                    tarball.len() / 1024,
                ),
            });
        }

        publisher.publish(forge_key, tarball).await
    }

    /// Pack + publish a remote subprocess step's declared `source_context`,
    /// returning the key to discard and the URL the step fetches (R560-T8).
    ///
    /// `Ok(None)` for a step that declares none — which is every step in every
    /// pipeline written before the key existed, so the common path does no
    /// work, makes no network call, and cannot fail.
    async fn publish_source_context(
        &self,
        step: &crate::types::QedStep,
    ) -> Result<Option<PublishedSourceContext>, RunnerError> {
        if step.source_context.is_empty() {
            return Ok(None);
        }

        let camp_root = self.resolve_camp_root()?;
        let tarball =
            crate::build_context::pack_source_context(&camp_root, &step.source_context)
                // pack_source_context labels its errors "source-context"; re-stamp
                // with the real step name so the failure points at a line in the
                // operator's pipeline.
                .map_err(|e| RunnerError::StepFailed {
                    step: step.name.clone(),
                    msg: e.to_string(),
                })?;

        self.emit_step_note(
            step,
            format!(
                "source context: {} ({} KiB packed) → uploading for the build-worker",
                step.source_context
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                tarball.len() / 1024,
            ),
        );

        let publisher = self
            .build_context_publisher
            .clone()
            .unwrap_or_else(|| Arc::new(crate::build_context::NoBuildContextPublisher));
        let key = tag_to_filename(&format!("{}-{}-src", self.run_id, step.name));
        let url = publisher.publish(&key, tarball).await?;
        Ok(Some(PublishedSourceContext { key, url }))
    }

    /// Best-effort delete of a published source context. Mirrors
    /// [`crate::build_context::BuildContextPublisher::discard`]'s contract:
    /// the build has already happened, so an undeleted temp object must never
    /// turn a green run red.
    async fn discard_source_context(&self, published: Option<&PublishedSourceContext>) {
        let Some(published) = published else { return };
        let publisher = self
            .build_context_publisher
            .clone()
            .unwrap_or_else(|| Arc::new(crate::build_context::NoBuildContextPublisher));
        publisher.discard(&published.key).await;
    }

    /// Emit an operator-facing stderr note against `step`, honouring
    /// [`QedStep::secret`](crate::types::QedStep::secret).
    fn emit_step_note(&self, step: &crate::types::QedStep, line: String) {
        if step.secret {
            return;
        }
        if let Some(tx) = &self.events {
            let _ = tx.send(QedEvent::StepOutput {
                index: 0,
                name: step.name.clone(),
                stream: OutputStream::Stderr,
                line,
            });
        }
    }

    async fn execute_step_build_image_remote(
        &self,
        step: &crate::types::QedStep,
        prepared: &PreparedBuildImage,
    ) -> Result<ObsForgeId, RunnerError> {
        let driver = self.remote_driver.as_ref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "build-image step `{}` dispatched remote but no remote dispatcher \
                 is wired",
                step.name,
            ))
        })?;

        // R636-B1: the worker is (in general) a different machine, so the
        // context has to reach it as bytes over a URL rather than as a bind
        // mount of a path only this host has. The key is single-use, which is
        // also what keeps a CDN edge from serving a negatively-cached 404.
        let context_key = tag_to_filename(&format!("{}-{}", self.run_id, step.name));
        let context_url = self
            .publish_build_context(step, prepared, &context_key)
            .await?;

        let spec = ForgeSpec {
            command: ForgeCommand::BuildImage {
                dockerfile: prepared.dockerfile_path.clone(),
                context: prepared.context_dir.clone(),
                context_url: Some(context_url),
                tags: vec![prepared.tag.clone()],
                // The catalog build-image path targets the worker's native arch
                // (placement below routes to an arch-matched build-worker), so
                // no explicit `--platform`. Multi-arch is stitched from N native
                // builds by the imagetools step, not requested here. Catalog
                // images take no build-args today.
                platforms: vec![],
                build_args: vec![],
                push: step.push,
                load: step.load,
            },
            where_: TaskPlacement::new(
                // R594/R636: route to a build-worker matching the step's
                // TARGET arch, not the runner's host arch. A `yah qed images
                // build --platform linux/amd64` from an arm64 Mac declares a
                // `native = true` x86 target and must land on a `arch:x86`
                // worker — deriving the tier from `self.host_triple` (arm64)
                // instead sent it to a `arch:arm` RPi that then failed on an
                // unreachable loopback URL. `remote_build_image_arch` reads
                // the step's Offload target and falls back to the host arch
                // only when the step declares no cross-arch target (a plain
                // host-native build-image under `--where remote`).
                // Catalog build-image steps always produce Linux container
                // images (buildx has no darwin/windows target), so the OS
                // is implicit here rather than derived from a triple.
                //
                // R833-F8: unless the operator named a node, in which case that
                // wins over the derived tags — see `remote_location`.
                self.remote_location(crate::platform::build_worker_mesh_tags(
                    &self.remote_build_image_arch(step),
                    "linux",
                )),
                TaskRuntime::Container,
            ),
            timeout: step.timeout.map(Millis::from_secs),
            label: Some(step.name.clone()),
            initiator: Initiator::Human { camp: "qed".into() },
            mesh_access: MeshAccess::None,
            cache_key: None,
        };

        let dispatch = driver
            .start(spec)
            .await
            .map_err(|e| RunnerError::Remote(e.to_string()));

        let outcome = match dispatch {
            Err(e) => Err(e),
            Ok(handle) => {
                let forge_id = handle.id.clone();
                match handle.wait().await {
                    ForgeStatus::Done { exit_code: 0, .. } => Ok(forge_id),
                    ForgeStatus::Done { exit_code, .. } => Err(RunnerError::StepFailed {
                        step: step.name.clone(),
                        msg: format!("buildkit exited with code {exit_code}"),
                    }),
                    ForgeStatus::TimedOut { .. } => Err(RunnerError::StepFailed {
                        step: step.name.clone(),
                        msg: "build-image step timed out".into(),
                    }),
                    ForgeStatus::Killed { signal, .. } => Err(RunnerError::StepFailed {
                        step: step.name.clone(),
                        msg: format!("buildkit killed by signal {signal}"),
                    }),
                    ForgeStatus::Lost { reason } => Err(RunnerError::StepFailed {
                        step: step.name.clone(),
                        msg: format!("buildkit lost: {reason}"),
                    }),
                    ForgeStatus::Pending | ForgeStatus::Running => {
                        unreachable!("ForgeRunHandle::wait returns a terminal status")
                    }
                }
            }
        };

        // Drop the uploaded context on BOTH legs. A failed build is exactly
        // when an operator re-runs, and every re-run uploads a fresh key — so
        // skipping cleanup on failure is how the bucket accumulates the copies
        // nobody will ever look at again.
        if let Some(publisher) = &self.build_context_publisher {
            publisher.discard(&context_key).await;
        }

        outcome
    }

    /// Dispatch a `kind = "package-native-tarball"` step (R407-T2).
    ///
    /// Pure host file I/O — there is no remote variant. Looks up the catalog
    /// entry named by `step.image`, asserts it declares
    /// [`crate::images::ProduceTarget::NativeTarball`], then writes a
    /// `<camp_root>/.yah/cache/native/<image>-<triple>.tar.gz` containing the
    /// static musl binary at `step.binary_path` plus a `manifest.toml`
    /// describing the workload-spec. The manifest carries the catalog entry's
    /// `env` map and `description` so Kamaji knows how to launch the
    /// workload without re-reading the catalog at deploy time.
    ///
    /// Cross-compile preflight (R407-T3) is the gate that ensures
    /// `step.binary_path` is actually musl-static before this step runs — by
    /// the time we get here the binary is assumed to be correctly targeted.
    async fn execute_step_package_native_tarball(
        &self,
        step: &crate::types::QedStep,
    ) -> Result<(), RunnerError> {
        let camp_root = self.resolve_camp_root()?;
        let camp_images_dir = camp_root.join(".yah/qed/images");
        let catalog = crate::images::CatalogManifest::load(&camp_images_dir)
            .map_err(|e| RunnerError::InvalidConfig(format!("failed to load catalog: {e}")))?;

        let image_name = step.image.as_deref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "package-native-tarball step `{}` is missing `image` \
                 (parse-time validation should have caught this)",
                step.name
            ))
        })?;
        let entry = catalog.get(image_name).cloned().ok_or_else(|| {
            let known: Vec<&str> = catalog.names();
            RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "unknown catalog image `{image_name}` — known: {known:?}. \
                     Per-camp images live at `.yah/qed/images/<name>/`."
                ),
            }
        })?;

        if !entry
            .produces
            .contains(&crate::images::ProduceTarget::NativeTarball)
        {
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "catalog entry `{image_name}` does not declare \
                     `produces = [\"native-tarball\"]` — add `native-tarball` to \
                     its `produces` list (alone or alongside `oci-image`) in \
                     `.yah/qed/images/{image_name}.toml`."
                ),
            });
        }

        let binary_rel = step.binary_path.as_deref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "package-native-tarball step `{}` is missing `binary_path` \
                 (parse-time validation should have caught this)",
                step.name
            ))
        })?;
        let binary_path = if std::path::Path::new(binary_rel).is_absolute() {
            std::path::PathBuf::from(binary_rel)
        } else {
            camp_root.join(binary_rel)
        };
        if !binary_path.is_file() {
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "binary not found at `{}` — declare the upstream build step \
                     in `produces` and chain it before this packaging step.",
                    binary_path.display()
                ),
            });
        }

        let triple = step
            .triple
            .clone()
            .unwrap_or_else(|| crate::publish::resolve_triple(None));

        let bin_basename = binary_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                RunnerError::InvalidConfig(format!(
                    "binary path `{}` has no filename component",
                    binary_path.display(),
                ))
            })?
            .to_string();

        let mut env: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
        for (k, v) in &entry.env {
            env.insert(k.clone(), v.clone());
        }

        let manifest = crate::native::NativeTarballManifest {
            name: entry.name.clone(),
            version: crate::publish::resolve_release_version(),
            triple: triple.clone(),
            binary: format!("bin/{bin_basename}"),
            description: if entry.description.is_empty() {
                None
            } else {
                Some(entry.description.clone())
            },
            env,
        };

        let output_path =
            crate::native::native_tarball_output_path(&camp_root, &entry.name, &triple);

        crate::native::pack_native_tarball(&binary_path, &manifest, &output_path).map_err(|e| {
            RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "failed to pack native tarball at {}: {e}",
                    output_path.display()
                ),
            }
        })?;

        Ok(())
    }

    /// Dispatch a `kind = "sign-native-tarball"` step (R407-T5, W154).
    ///
    /// Sigstore signing extends to native-tarball artifacts under the same
    /// keyless-OIDC trust model used for OCI images today (cosign signs the
    /// registry digest; here cosign signs the on-disk blob). The signer
    /// (attached via [`Self::with_signer`]) writes `.sig`, `.crt`, and
    /// `.bundle` next to the artifact; `cosign verify-blob --bundle ...`
    /// at deploy time confirms the GHA workflow identity matches the
    /// release pipeline's expected regex.
    ///
    /// The tarball path is resolved via
    /// [`crate::native::native_tarball_output_path`] — same convention as
    /// packaging, so a pipeline that runs `package-native-tarball` then
    /// `sign-native-tarball` with the same `image` + `triple` always finds
    /// the artifact. A pre-flight check on the catalog entry's `produces`
    /// list refuses to sign tarballs from entries that didn't declare
    /// `native-tarball` (catches a stale step that survived a catalog
    /// rename).
    async fn execute_step_sign_native_tarball(
        &self,
        step: &crate::types::QedStep,
    ) -> Result<(), RunnerError> {
        let camp_root = self.resolve_camp_root()?;
        let camp_images_dir = camp_root.join(".yah/qed/images");
        let catalog = crate::images::CatalogManifest::load(&camp_images_dir)
            .map_err(|e| RunnerError::InvalidConfig(format!("failed to load catalog: {e}")))?;

        let image_name = step.image.as_deref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "sign-native-tarball step `{}` is missing `image` \
                 (parse-time validation should have caught this)",
                step.name
            ))
        })?;
        let entry = catalog.get(image_name).cloned().ok_or_else(|| {
            let known: Vec<&str> = catalog.names();
            RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "unknown catalog image `{image_name}` — known: {known:?}. \
                     Per-camp images live at `.yah/qed/images/<name>/`."
                ),
            }
        })?;

        if !entry
            .produces
            .contains(&crate::images::ProduceTarget::NativeTarball)
        {
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "catalog entry `{image_name}` does not declare \
                     `produces = [\"native-tarball\"]` — sign-native-tarball \
                     refuses to sign artifacts the catalog hasn't opted in to. \
                     Update `.yah/qed/images/{image_name}.toml` (or drop this \
                     sign step)."
                ),
            });
        }

        let triple = step
            .triple
            .clone()
            .unwrap_or_else(|| crate::publish::resolve_triple(None));
        let tarball_path =
            crate::native::native_tarball_output_path(&camp_root, &entry.name, &triple);
        if !tarball_path.is_file() {
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "native tarball not found at `{}` — run \
                     `kind = \"package-native-tarball\"` for `{image_name}` \
                     before signing.",
                    tarball_path.display()
                ),
            });
        }

        let signed =
            self.signer
                .sign_blob(&tarball_path)
                .await
                .map_err(|e| RunnerError::StepFailed {
                    step: step.name.clone(),
                    msg: format!(
                        "cosign sign-blob failed for `{}`: {e}",
                        tarball_path.display(),
                    ),
                })?;

        tracing::info!(
            tarball = %tarball_path.display(),
            signature = %signed.signature_path.display(),
            // `None` for key-based signing (R605-F1) — there is no Fulcio cert.
            certificate = signed.certificate_path.as_ref().map(|p| p.display().to_string()).unwrap_or_default(),
            bundle = signed.bundle_path.as_ref().map(|p| p.display().to_string()).unwrap_or_default(),
            "qed sign-native-tarball: artifact signed"
        );
        Ok(())
    }

    /// Dispatch a `kind = "musl-static-preflight"` step (R407-T3).
    ///
    /// Walks `step.package`'s transitive dep closure via `cargo metadata`
    /// and fails the step (with a `NotMuslSafe` error listing the offenders)
    /// if any crate in [`crate::preflight::KNOWN_GLIBC_ONLY_CRATES`] appears.
    /// The error message routes the pipeline author to the container
    /// fallback (`runtime = "container"`) rather than letting the
    /// downstream `cargo build --target=*-musl` step die with a confusing
    /// linker error.
    async fn execute_step_musl_static_preflight(
        &self,
        step: &crate::types::QedStep,
    ) -> Result<(), RunnerError> {
        let camp_root = self.resolve_camp_root()?;
        let package = step.package.as_deref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "musl-static-preflight step `{}` is missing `package` \
                 (parse-time validation should have caught this)",
                step.name
            ))
        })?;
        let package = package.to_string();
        let step_name = step.name.clone();
        let camp_root_clone = camp_root.clone();
        // cargo metadata blocks while it resolves the dep graph — push it
        // off the async runtime so a slow workspace doesn't starve other
        // tasks (e.g. event drain).
        let result = tokio::task::spawn_blocking(move || {
            crate::preflight::check_musl_compatibility(&camp_root_clone, &package)
        })
        .await
        .map_err(|e| RunnerError::StepFailed {
            step: step_name.clone(),
            msg: format!("preflight task panicked: {e}"),
        })?;
        result.map_err(|e| RunnerError::StepFailed {
            step: step_name,
            msg: e.to_string(),
        })?;
        Ok(())
    }

    /// Run one step on the fleet, reporting BOTH what happened and whether a
    /// workload was left behind for [`Self::reap_remote_workload`] to remove.
    ///
    /// This used to return `Result<ObsForgeId, RunnerError>`, which threw the
    /// id away on the failure leg — so a step that dispatched and then exited
    /// non-zero left a `forge.<uuid>` record the caller could no longer name,
    /// let alone reap (R555-F6). The two facts are orthogonal and the return
    /// type now says so: `forge_id` answers "does a workload exist on a worker",
    /// `result` answers "did the step succeed".
    async fn execute_step_remote(
        &self,
        index: usize,
        step: &crate::types::QedStep,
        runtime: TaskRuntime,
    ) -> RemoteStepOutcome {
        // Everything that can fail BEFORE `start_with_context` returns fails
        // without leaving anything on a worker, so it stays `?`-shaped in the
        // helper below and maps wholesale onto `never_dispatched`.
        let (forge_id, status) = match self.dispatch_remote_step(index, step, runtime).await {
            Ok(dispatched) => dispatched,
            Err(e) => return RemoteStepOutcome::never_dispatched(e),
        };

        let result = match status {
            ForgeStatus::Done { exit_code: 0, .. } => Ok(()),
            ForgeStatus::Done { exit_code, .. } => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("exited with code {exit_code}"),
            }),
            ForgeStatus::TimedOut { .. } => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: "step timed out".into(),
            }),
            ForgeStatus::Killed { signal, .. } => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("killed by signal {signal}"),
            }),
            ForgeStatus::Lost { reason } => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("lost: {reason}"),
            }),
            ForgeStatus::Pending | ForgeStatus::Running => {
                unreachable!("ForgeRunHandle::wait returns a terminal status")
            }
        };
        RemoteStepOutcome { forge_id: Some(forge_id), result }
    }

    /// The dispatching half of [`Self::execute_step_remote`]: build the spec,
    /// deploy it, and block until the run reaches a terminal [`ForgeStatus`].
    ///
    /// Every `Err` out of here means NOTHING WAS DEPLOYED — a missing
    /// dispatcher, a refused spec, a source-context publish that failed, or a
    /// deploy that errored (which unwinds its own published context before
    /// returning). That is the property the caller relies on to decide there is
    /// no workload record to reap, so a new fallible call added AFTER
    /// `start_with_context` succeeds would break it and belongs in the wrapper.
    async fn dispatch_remote_step(
        &self,
        index: usize,
        step: &crate::types::QedStep,
        runtime: TaskRuntime,
    ) -> Result<(ObsForgeId, ForgeStatus), RunnerError> {
        // R590-F4: a forced-remote runner always has a driver, but an Auto runner
        // that policy-routed this step to the fleet needs one wired too. Surface a
        // clear config error instead of panicking when a policy-derived offload
        // ran without a mesh dispatcher.
        let driver = self.remote_driver.as_ref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "step `{}` resolves to Offload (needs an arch-matched build-worker) \
                 but no remote dispatcher is wired — run with fleet access, or force \
                 `--where=local` to build it here",
                step.name,
            ))
        })?;

        // R590-F2: when the step declares a cross-arch target via
        // `[platform].target`, pin placement to an arch-matched build-worker so
        // an arm64 host can drive an x86 build on the x86 box (us-west-002).
        let mesh_tags = remote_subprocess_mesh_tags(step);

        // R590-F2 milestone-1 (2): per-step container image override (finishing
        // the R381 `step.image` seam). A subprocess step may name its own
        // catalog image (e.g. `rusty-v8-musl-builder`) to run its argv inside,
        // instead of the default forge image (`yah-rust-bun`). `None` keeps the
        // default-image behaviour for plain steps.
        let image = step_image_override(step)?;

        // R603-T5: a remote step's declared `produces` must be written under the
        // durable produced dir (`/yah/produced`), which build_workload_spec
        // host-bind-mounts so the bytes survive kamaji reaping the exited
        // container. A produces path outside it would be retrieved off the
        // container rootfs — lost the moment the container is reaped after a
        // daemon outage (the exact R603-T4 failure this ticket closes). Fail
        // fast at dispatch with a clear pointer rather than silently orphan.
        for artifact in &step.produces {
            let path = std::path::Path::new(&artifact.path);
            if !workload_spec::forge_produced::is_durable_path(path) {
                return Err(RunnerError::InvalidConfig(format!(
                    "step `{}` declares produced artifact `{}`, but remote produced \
                     artifacts must be written under `{}` so they survive the \
                     build-worker reaping the container (R603-T5). Point the \
                     build's output path at `{}/…`.",
                    step.name,
                    artifact.path,
                    workload_spec::forge_produced::CONTAINER_DIR,
                    workload_spec::forge_produced::CONTAINER_DIR,
                )));
            }
        }

        // R560-T8: a remote subprocess gets image + argv + the /yah/produced
        // mount and NOTHING ELSE — in particular, no source. A step that
        // compiles the camp tree (the `mesofact-musl` legs) therefore has
        // nothing to compile unless its source travels as bytes, exactly the
        // way R636-B1 made a build-image context travel. Publish it here and
        // hand the step the URL through the env; the argv fetches it.
        let source_context = self.publish_source_context(step).await?;

        // R876-F4: the fourth thing, and the one that makes the other three
        // worth repeating — a host-persistent build cache keyed so no two
        // runs that should not share a cargo target dir can.
        let cache_key = step_cache_key(&self.pipeline.name, step);

        let spec = ForgeSpec {
            cache_key: cache_key.clone(),
            command: ForgeCommand::Subprocess {
                argv: step.argv.clone(),
                image,
            },
            // R833-F8: `remote_location` returns the pinned node when the
            // operator named one and the R594 tag-matched `RemoteAny`
            // otherwise, so inference is untouched for every unpinned run.
            where_: TaskPlacement::new(self.remote_location(mesh_tags), runtime),
            timeout: step.timeout.map(Millis::from_secs),
            label: Some(step.name.clone()),
            // Camp name will be threaded through once yubaba RPC stabilises (R091).
            initiator: Initiator::Human { camp: "qed".into() },
            mesh_access: MeshAccess::None,
        };

        // Adapter: forward yubaba log lines into the runner's live sink as
        // StepOutput, mirroring the local subprocess path (R508). Without this
        // a yubaba-dispatched step only surfaced its log lines post-run via
        // scryer; now they stream into qed.tail / the desktop pane live.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecEvent>();
        let adapter = {
            // R717-T2: the third and last subprocess sink (local native, local
            // container, remote). `secret` is a Subprocess-only knob — validate()
            // rejects it on every other kind — so gating these three closes the
            // set rather than covering most of it.
            //
            // CAVEAT worth knowing: this suppresses qed's own journal only. A
            // remote step's lines are ALSO collected worker-side by scryer,
            // which qed does not own and cannot redact from here. A `secret`
            // step that must also stay out of the worker's log has to run local.
            let events = if step.secret { None } else { self.events.clone() };
            let name = step.name.clone();
            tokio::spawn(async move {
                while let Some(ev) = rx.recv().await {
                    let Some(events) = &events else { continue };
                    if let ExecEvent::Output { stream, line } = ev {
                        let qed_stream = match stream {
                            velveteen_exec::OutputStream::Stdout => OutputStream::Stdout,
                            velveteen_exec::OutputStream::Stderr => OutputStream::Stderr,
                        };
                        let _ = events.send(QedEvent::StepOutput {
                            index,
                            name: name.clone(),
                            stream: qed_stream,
                            line,
                        });
                    }
                }
            })
        };

        // R577-F3: carry the step's declared `[pipeline.steps.env]` to the
        // worker. Until this, the remote path built its driver call from the
        // `ForgeSpec` alone (`start_with_sink` ⇒ `ExecContext::default()`), so
        // every remote step ran with its declared env SILENTLY ABSENT — while
        // the local paths (`execute_step_forge` / the container shim) threaded
        // the same map through `ExecContext::with_env`. A step that worked
        // locally lost its configuration the moment placement offloaded it, with
        // no diagnostic anywhere: `desktop-release`'s terminal `publish` step
        // declares `YAH_ALMANAC_FEED` and never received it on an offloaded row.
        //
        // `cwd` is deliberately NOT threaded here, and that asymmetry is the
        // point rather than an omission. The local paths join `step.cwd` onto
        // the *camp root* to get a bind-mount source; on a worker there is no
        // camp root to join against — the recipe's `workspace = "isolated"`
        // checkout happens worker-side and QED has no coordinator→worker input
        // channel to learn where it landed (see the `desktop-release` header).
        // Passing the bare relative path would resolve against the container
        // image root, or — on a native forge — against the kamaji daemon's own
        // working directory, which `apply_exec_context` refuses outright
        // (R577-T1). Dropping it keeps today's behaviour; wiring it needs the
        // input channel first.
        let mut env: Vec<(String, String)> = step
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        // R560-T8. The step's own spelling of this key is DROPPED, not merged
        // behind ours: the URL is run-scoped and minted milliseconds ago, so a
        // literal in the TOML can only be a stale one, and `with_env` takes a
        // Vec whose duplicate-key precedence is not ours to assume.
        if let Some(published) = &source_context {
            let key = crate::build_context::SOURCE_CONTEXT_URL_ENV;
            env.retain(|(k, _)| k != key);
            env.push((key.to_string(), published.url.clone()));
        }
        // R876-F4. Same drop-then-set discipline: the container path is qed's
        // convention, so a literal in the TOML could only disagree with the
        // mount that was actually made.
        if cache_key.is_some() {
            let key = workload_spec::forge_cache::CACHE_DIR_ENV;
            env.retain(|(k, _)| k != key);
            env.push((
                key.to_string(),
                workload_spec::forge_cache::CONTAINER_DIR.to_string(),
            ));
        }
        let ctx = ExecContext::default().with_env(env);

        let handle = match driver.start_with_context(spec, Some(tx), &ctx).await {
            Ok(handle) => handle,
            Err(e) => {
                // Nothing ever fetched these bytes; don't leave them in the
                // bucket because the dispatch lost a race with the worker.
                self.discard_source_context(source_context.as_ref()).await;
                return Err(RunnerError::Remote(e.to_string()));
            }
        };

        let forge_id = handle.id.clone();
        // R603-T1: publish the workload identity the moment it exists, before we
        // block on `wait()`. The camp daemon persists this as a non-terminal run
        // record so a daemon restart mid-build can reattach to the workload
        // (R603-T2) rather than orphaning it.
        self.emit(QedEvent::StepRemoteDispatched {
            index,
            name: step.name.clone(),
            forge_id: forge_id.to_string(),
            at: Utc::now(),
        });
        let status = handle.wait().await;
        // Drain any remaining buffered lines before the step is marked done.
        let _ = adapter.await;
        // Single-use key, dropped on BOTH legs — same discipline as the
        // build-image context (R636-B1). A failed build is exactly when the
        // temp object is least wanted and most likely to be forgotten.
        self.discard_source_context(source_context.as_ref()).await;

        // The workload record is deliberately NOT torn down here. Mapping the
        // status to a result is the caller's job precisely because the caller
        // is the one that retrieves the produced artifacts first — see
        // [`RemoteForgeDriver::reap`](velveteen_exec::RemoteForgeDriver::reap).
        Ok((forge_id, status))
    }

    /// Remove the yubaba workload record a *finished* remote step left behind
    /// (R555-F6). Best-effort by design.
    ///
    /// # Two things a future edit must not change
    ///
    /// **Where it is called from.** yubaba's destroy handler reaps the
    /// host-persistent produced dir along with the record (R603-T5), so this
    /// must run strictly after [`Self::retrieve_remote_artifacts`]. Moving it
    /// into `execute_step_remote`, or down into velveteen-exec's
    /// `run_log_task` beside the timeout teardown, destroys the artifact bytes
    /// before the coordinator fetches them.
    ///
    /// **That a failure here is only a warning.** A teardown that errors must
    /// not turn a green step red: the step's work is done and its bytes are
    /// already in camp's store, so failing the run over hygiene would trade a
    /// leaked record for a false build failure. The unreaped record is bounded
    /// by nothing today, which is why the warning names the ident.
    async fn reap_remote_workload(&self, forge_id: &ObsForgeId, step_name: &str) {
        let Some(driver) = self.remote_driver.as_ref() else {
            return;
        };
        if let Err(e) = driver.reap(forge_id).await {
            tracing::warn!(
                step = step_name,
                forge_id = %forge_id,
                "reaping the remote workload failed; forge.{forge_id} stays on the \
                 build-worker until something else removes it: {e}",
            );
        }
    }

    /// R590-F6 leg 2: after a remote step exits successfully, pull the files it
    /// declared in [`QedStep::produces`] off the build-worker and land them in
    /// camp's content-addressed store (`<camp_root>/.yah/cache/artifacts/`).
    ///
    /// Returns the produced-artifact list with each `path` rewritten to the
    /// landed local file, so the publish leg ([`crate::types::Outcome::Publish`]
    /// / the W164 derived-static-asset reconciler) reads the retrieved bytes
    /// instead of the unreachable container-side path — this FEEDS R546-T3's
    /// bootstrap publish, it does not duplicate it.
    ///
    /// Only called for remote steps that actually declare `produces`; a step
    /// with none (rusty-v8-musl today) never enters this path, so retrieval
    /// cannot regress the on-box green that R590-B5 unblocks.
    async fn retrieve_remote_artifacts(
        &self,
        forge_id: &ObsForgeId,
        step: &crate::types::QedStep,
    ) -> Result<Vec<ProducedArtifact>, RunnerError> {
        let driver = self.remote_driver.as_ref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "step `{}` declares produced artifacts to retrieve but no remote \
                 dispatcher is wired",
                step.name,
            ))
        })?;
        let store = crate::artifact_retrieval::ContentAddressedStore::new(
            self.resolve_camp_root()?.join(".yah/cache/artifacts"),
        );

        let mut retrieved = Vec::with_capacity(step.produces.len());
        for artifact in &step.produces {
            let remote_path = std::path::Path::new(&artifact.path);
            let bytes = driver
                .fetch_produced_file(forge_id, remote_path)
                .await
                .map_err(|e| RunnerError::StepFailed {
                    step: step.name.clone(),
                    msg: format!(
                        "retrieving produced artifact `{}` off the build-worker: {e}",
                        artifact.path
                    ),
                })?;
            retrieved.push(land_produced_artifact(
                &store, &step.name, artifact, remote_path, &bytes,
            )?);
        }
        Ok(retrieved)
    }

    /// R560-B12: the local-container analogue of
    /// [`Self::retrieve_remote_artifacts`] — read each declared `produces` out
    /// of the host dir that was bound at the container's produced dir, and land
    /// it in the same content-addressed store.
    ///
    /// There is no fetch here because the bind already moved the bytes; what
    /// remains is the half that is NOT about transport, and that half has to be
    /// identical or the two legs of a pipeline diverge downstream. Both end at
    /// [`land_produced_artifact`], so a `mesofact-musl` tarball is CAS-landed,
    /// named-linked and handed to `stage_release` the same way whether it was
    /// built on us-west-003 or in a container on this Mac.
    fn collect_local_container_artifacts(
        &self,
        step: &crate::types::QedStep,
        produced_dir: &std::path::Path,
    ) -> Result<Vec<ProducedArtifact>, RunnerError> {
        let store = crate::artifact_retrieval::ContentAddressedStore::new(
            self.resolve_camp_root()?.join(".yah/cache/artifacts"),
        );

        let mut collected = Vec::with_capacity(step.produces.len());
        for artifact in &step.produces {
            let declared = std::path::Path::new(&artifact.path);
            // Reuses the remote path's traversal guard rather than trusting the
            // bind: `produces` is operator-authored, and `-v` would happily
            // follow `/yah/produced/../../etc` back out onto the host.
            let host_path = workload_spec::forge_produced::host_path_under(produced_dir, declared)
                .ok_or_else(|| RunnerError::StepFailed {
                    step: step.name.clone(),
                    msg: format!(
                        "produced artifact path `{}` is not under {} (or escapes it with `..`) \
                         — a local container step's `produces` must sit in the bound produced \
                         dir, the same constraint the remote leg enforces at dispatch",
                        artifact.path,
                        workload_spec::forge_produced::CONTAINER_DIR,
                    ),
                })?;
            let bytes = std::fs::read(&host_path).map_err(|e| RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "reading produced artifact `{}` from the container's bound produced dir \
                     ({}): {e} — the step exited 0, so it either wrote somewhere else or \
                     wrote nothing",
                    artifact.path,
                    host_path.display(),
                ),
            })?;
            collected.push(land_produced_artifact(
                &store, &step.name, artifact, declared, &bytes,
            )?);
        }
        Ok(collected)
    }
}

/// Land one produced artifact's bytes in the camp's content-addressed store and
/// return the declaration rewritten to point at them.
///
/// Shared by the remote retrieval leg and the local-container collection leg
/// (R560-B12). Everything below the transport is common, and the R560-T9 naming
/// rule in particular is the kind of subtlety that silently rots in a second
/// copy.
fn land_produced_artifact(
    store: &crate::artifact_retrieval::ContentAddressedStore,
    step_name: &str,
    artifact: &ProducedArtifact,
    declared_path: &std::path::Path,
    bytes: &[u8],
) -> Result<ProducedArtifact, RunnerError> {
    let landed = store.land(bytes).map_err(RunnerError::Io)?;
    // R560-T9: hand the publish leg a path whose BASENAME is still the
    // build's own filename. `stage_release` keys a release object as
    // `<binary>/<version>/<triple>/<basename>`, so rewriting `path` to
    // the bare CAS address would publish the tarball under its 64-hex
    // BLAKE3 — a URL no install script constructs. The CAS entry is
    // untouched (it is R546-T3's input and the preservation check); the
    // named path is a hard link into the same bytes.
    let filename = declared_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| RunnerError::StepFailed {
            step: step_name.to_string(),
            msg: format!(
                "produced artifact path `{}` has no filename component",
                artifact.path
            ),
        })?;
    let named = store.link_named(&landed, filename).map_err(RunnerError::Io)?;
    Ok(ProducedArtifact {
        binary: artifact.binary.clone(),
        path: named.to_string_lossy().into_owned(),
        triple: artifact.triple.clone(),
    })
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use yah_scryer::service::{Scryer, ScryerConfig};
    use std::collections::HashMap;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use workload_spec::MeshIdent;

    // ─── R719-F2: sub-pipeline admission ─────────────────────────────────

    /// The adopted semantics: a child wanting the key its parent already holds
    /// is fully covered by the parent's grant. This is the common case and must
    /// stay silent, or the warning becomes noise and stops being read.
    #[test]
    fn a_child_on_the_parents_own_key_is_already_admitted() {
        assert_eq!(
            sub_pipeline_admission_gap("cargo-target", "cargo-target"),
            None
        );
    }

    /// R719-F1 made this the default shape: parent and child both unkeyed both
    /// resolve to `@camp`, so inheritance covers them.
    #[test]
    fn two_unkeyed_pipelines_share_the_camp_lane_and_do_not_warn() {
        let key = crate::types::DEFAULT_CONCURRENCY_KEY;
        assert_eq!(sub_pipeline_admission_gap(key, key), None);
    }

    /// A child that opts out of serialization needs nothing from its parent.
    #[test]
    fn a_parallel_child_needs_no_grant() {
        assert_eq!(
            sub_pipeline_admission_gap("cargo-target", "@parallel"),
            None
        );
        assert_eq!(sub_pipeline_admission_gap("@camp", "@parallel"), None);
    }

    /// The hole R719-F2 exists for: the child wants a lane the parent is not
    /// standing in, so nothing serializes it against a top-level run of the
    /// same thing.
    #[test]
    fn a_child_wanting_a_different_key_is_reported() {
        let gap = sub_pipeline_admission_gap("release-lane", "cargo-target")
            .expect("differing keys must be reported");
        assert_eq!(gap.parent_key, "release-lane");
        assert_eq!(gap.child_key, "cargo-target");
    }

    /// A `@parallel` PARENT covers nothing — a keyed child under it is the
    /// worst version of the gap, and must not be silently excused just because
    /// the parent opted out.
    #[test]
    fn a_parallel_parent_does_not_cover_a_keyed_child() {
        assert!(sub_pipeline_admission_gap("@parallel", "cargo-target").is_some());
    }

    /// Peer children are stamped `peer:<camp>` by the resolver (R494-F2), so
    /// they are the gap by construction. That is why this reports instead of
    /// refusing — a hard failure here would break `peer-release`.
    #[test]
    fn a_peer_stamped_child_is_reported_not_refused() {
        assert!(sub_pipeline_admission_gap("@camp", "peer:cheers").is_some());
    }

    /// The message has to name both keys and the fix. A warning that says only
    /// "admission gap" costs the reader the whole investigation it was meant to
    /// save.
    #[test]
    fn the_warning_names_both_keys_and_the_fix() {
        let gap = sub_pipeline_admission_gap("release-lane", "cargo-target").unwrap();
        let msg = gap.message("builtin:desktop-release", "bundle");
        assert!(msg.contains("builtin:desktop-release"), "{msg}");
        assert!(msg.contains("bundle"), "{msg}");
        assert!(msg.contains("release-lane"), "{msg}");
        assert!(msg.contains("cargo-target"), "{msg}");
        assert!(msg.contains("@parallel"), "must offer the opt-out: {msg}");
    }

    // ─── R605-F3: the step DAG + concurrent scheduler ───────────────────────

    /// A step that sleeps, so overlap is observable in the recorded timestamps.
    fn sleep_step(name: &str, secs: &str, needs: Option<Vec<&str>>) -> crate::types::QedStep {
        let mut s = shell_step(name, vec!["sleep", secs]);
        s.needs = needs.map(|n| n.into_iter().map(String::from).collect());
        s
    }

    /// Did two recorded steps overlap in wall-clock?
    fn overlaps(a: &StepStatus, b: &StepStatus) -> bool {
        let (a0, a1) = (a.started_at.unwrap(), a.completed_at.unwrap());
        let (b0, b1) = (b.started_at.unwrap(), b.completed_at.unwrap());
        a0 < b1 && b0 < a1
    }

    fn row<'a>(meta: &'a QedRunMeta, name: &str) -> &'a StepStatus {
        meta.steps
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("no step row `{name}` in {:?}", meta.steps.iter().map(|s| &s.name).collect::<Vec<_>>()))
    }

    /// The ticket's own verify criterion. Two independent branches off one root
    /// with a join: the branches must overlap in wall-clock, and the join must
    /// start only after both of them finished.
    #[tokio::test]
    async fn two_branches_overlap_and_the_join_waits_for_both() {
        let camp = tempfile::tempdir().unwrap();
        let pipeline = make_pipeline(
            "diamond",
            vec![
                sleep_step("root", "0", Some(vec![])),
                sleep_step("left", "1", Some(vec!["root"])),
                sleep_step("right", "1", Some(vec!["root"])),
                sleep_step("join", "0", Some(vec!["left", "right"])),
            ],
        );
        let meta = PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let (left, right, join) = (row(&meta, "left"), row(&meta, "right"), row(&meta, "join"));
        assert!(
            overlaps(left, right),
            "the two independent branches must run concurrently: left {:?}..{:?}, right {:?}..{:?}",
            left.started_at, left.completed_at, right.started_at, right.completed_at,
        );
        assert!(
            join.started_at.unwrap() >= left.completed_at.unwrap()
                && join.started_at.unwrap() >= right.completed_at.unwrap(),
            "the join must not start before both branches finished",
        );
        // Rows stay in DECLARATION order even though `right` may finish first.
        let names: Vec<&str> = meta.steps.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["root", "left", "right", "join"]);
    }

    /// The compatibility property the whole design rests on: a pipeline that
    /// declares no `needs` is still strictly serial. If this ever goes green
    /// with overlap, every pipeline TOML in every camp just became parallel.
    #[tokio::test]
    async fn a_pipeline_without_needs_stays_strictly_serial() {
        let camp = tempfile::tempdir().unwrap();
        let pipeline = make_pipeline(
            "chain",
            vec![
                sleep_step("one", "1", None),
                sleep_step("two", "1", None),
                sleep_step("three", "0", None),
            ],
        );
        let meta = PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert!(!overlaps(row(&meta, "one"), row(&meta, "two")));
        assert!(
            row(&meta, "two").started_at.unwrap() >= row(&meta, "one").completed_at.unwrap(),
            "an absent `needs` means the implicit chain edge, not 'no dependencies'",
        );
    }

    /// `max_parallel = 1` pins a genuine DAG back to serial — the escape hatch
    /// for a pipeline that turns out to contend in a way its `resource` keys
    /// don't describe yet.
    #[tokio::test]
    async fn max_parallel_one_serializes_a_dag() {
        let camp = tempfile::tempdir().unwrap();
        let mut pipeline = make_pipeline(
            "diamond",
            vec![
                sleep_step("left", "1", Some(vec![])),
                sleep_step("right", "1", Some(vec![])),
            ],
        );
        pipeline.max_parallel = Some(1);
        let meta = PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert!(!overlaps(row(&meta, "left"), row(&meta, "right")));
    }

    /// Two independent steps that both name the same `resource` never overlap,
    /// even though the DAG says they may and the budget would allow it. This is
    /// the shared-`target/` case: parallel in the graph, serial on the disk.
    #[tokio::test]
    async fn a_shared_resource_key_serializes_independent_steps() {
        let camp = tempfile::tempdir().unwrap();
        let mut a = sleep_step("build-a", "1", Some(vec![]));
        let mut b = sleep_step("build-b", "1", Some(vec![]));
        a.resource = Some("cargo-target".into());
        b.resource = Some("cargo-target".into());
        let free = sleep_step("free", "1", Some(vec![]));
        let meta = PipelineRunner::new(make_pipeline("res", vec![a, b, free]))
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert!(
            !overlaps(row(&meta, "build-a"), row(&meta, "build-b")),
            "steps sharing a resource key must not overlap",
        );
        assert!(
            overlaps(row(&meta, "build-a"), row(&meta, "free")),
            "a step holding no key is unaffected by someone else's",
        );
    }

    /// A failed branch stops the run without stranding the branch that was
    /// already in flight: the sibling still records a terminal row, and the
    /// join — whose predecessor failed — never starts at all.
    #[tokio::test]
    async fn an_aborting_branch_stops_admission_but_lets_inflight_work_finish() {
        let camp = tempfile::tempdir().unwrap();
        let pipeline = make_pipeline(
            "fail",
            vec![
                shell_step("boom", vec!["false"]),
                sleep_step("sibling", "1", Some(vec![])),
                sleep_step("join", "0", Some(vec!["boom", "sibling"])),
            ],
        );
        // `boom` is declared first and has no needs, so it is the chain root;
        // `sibling` is an explicit root, so both are admitted together.
        let mut pipeline = pipeline;
        pipeline.steps[0].needs = Some(vec![]);
        let meta = PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(row(&meta, "boom").status, RunStatus::Failed);
        assert_eq!(
            row(&meta, "sibling").status,
            RunStatus::Success,
            "a step already in flight is not cancelled by a sibling's abort",
        );
        assert!(
            meta.steps.iter().all(|s| s.name != "join"),
            "a step the run never reached records no row, same as before the scheduler",
        );
    }

    /// A cycle is a load-time error, not a run that hangs or silently does
    /// nothing. The runner re-checks because it is handed pipelines built in
    /// code as well as parsed from TOML.
    #[tokio::test]
    async fn a_cyclic_needs_graph_fails_the_run_up_front() {
        let camp = tempfile::tempdir().unwrap();
        let mut a = shell_step("a", vec!["true"]);
        let mut b = shell_step("b", vec!["true"]);
        a.needs = Some(vec!["b".into()]);
        b.needs = Some(vec!["a".into()]);
        let err = PipelineRunner::new(make_pipeline("cyc", vec![a, b]))
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .expect_err("a cycle must fail the run");
        assert!(
            matches!(&err, RunnerError::InvalidConfig(m) if m.contains("cycle")),
            "got {err:?}",
        );
    }

    /// Resume-from-step drains the leading steps and offsets the indices, so a
    /// surviving `needs` points at a step that is no longer in the slice — and
    /// genuinely already ran. It must resolve as satisfied, not deadlock.
    #[tokio::test]
    async fn a_resumed_run_treats_a_drained_dependency_as_satisfied() {
        let camp = tempfile::tempdir().unwrap();
        let mut publish = shell_step("publish", vec!["true"]);
        publish.needs = Some(vec!["build".into()]);
        let meta = PipelineRunner::new(make_pipeline("resumed", vec![publish]))
            .with_camp_root(camp.path().to_path_buf())
            .with_step_selection(3, false)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(row(&meta, "publish").status, RunStatus::Success);
    }

    /// A `needs` on a fanned-out matrix step joins on every row, not on none —
    /// `matrix::plan` renames instances `"<name> [k=v]"` and the join has to
    /// still find them.
    #[tokio::test]
    async fn a_join_waits_for_every_row_of_a_matrix_step() {
        let camp = tempfile::tempdir().unwrap();
        let mut build = sleep_step("build", "1", Some(vec![]));
        let mut dims: indexmap::IndexMap<String, Vec<toml::Value>> = indexmap::IndexMap::new();
        dims.insert(
            "arch".to_string(),
            vec![
                toml::Value::String("x86".into()),
                toml::Value::String("arm".into()),
            ],
        );
        build.matrix = Some(crate::matrix::MatrixSpec {
            dimensions: dims,
            include: Vec::new(),
            exclude: Vec::new(),
        });
        let join = sleep_step("join", "0", Some(vec!["build"]));
        let planned = crate::matrix::plan(&make_pipeline("fan", vec![build, join]));
        let expanded = planned.into_iter().next().unwrap().pipeline;
        // Both rows are explicit roots (they inherit the step's own `needs`),
        // so they run together and the join waits for the later of the two.
        let meta = PipelineRunner::new(expanded)
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        let x86 = row(&meta, "build [arch=x86]");
        let arm = row(&meta, "build [arch=arm]");
        assert!(overlaps(x86, arm), "matrix rows sharing a `needs` fan out");
        let join = row(&meta, "join");
        assert!(join.started_at.unwrap() >= x86.completed_at.unwrap());
        assert!(join.started_at.unwrap() >= arm.completed_at.unwrap());
    }

    /// R605-F3 (found by R776-T2): `background_until` naming a matrix step must
    /// resolve to every row of it and reap after the LAST one. It used to be an
    /// exact name compare, so this shape failed preflight with "unknown step" —
    /// the post-expansion name is `client [n=1]`, which no author writes by
    /// hand — and, had it resolved, would have reaped on the first row.
    #[tokio::test]
    async fn background_until_gates_on_the_last_row_of_a_matrix_step() {
        let camp = tempfile::tempdir().unwrap();
        let mut sidecar = shell_step("server", vec!["sleep", "30"]);
        sidecar.background = true;
        sidecar.background_until = Some("client".into());
        sidecar.needs = Some(vec![]);

        let mut client = sleep_step("client", "1", Some(vec!["server"]));
        let mut dims: indexmap::IndexMap<String, Vec<toml::Value>> = indexmap::IndexMap::new();
        dims.insert(
            "n".to_string(),
            vec![toml::Value::Integer(1), toml::Value::Integer(2)],
        );
        client.matrix = Some(crate::matrix::MatrixSpec {
            dimensions: dims,
            include: Vec::new(),
            exclude: Vec::new(),
        });

        let planned = crate::matrix::plan(&make_pipeline("bg-fan", vec![sidecar, client]));
        let expanded = planned.into_iter().next().unwrap().pipeline;
        assert_eq!(expanded.steps.len(), 3, "the client fanned out");

        let meta = PipelineRunner::new(expanded)
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        // The sidecar's terminal row lands at reap, so its completed_at is the
        // observable: it must be at or after BOTH rows finished, not just one.
        let server = row(&meta, "server");
        let r1 = row(&meta, "client [n=1]");
        let r2 = row(&meta, "client [n=2]");
        assert_eq!(server.status, RunStatus::Success);
        assert!(
            server.completed_at.unwrap() >= r1.completed_at.unwrap()
                && server.completed_at.unwrap() >= r2.completed_at.unwrap(),
            "the reap waits for the last row: server {:?}, rows {:?} / {:?}",
            server.completed_at, r1.completed_at, r2.completed_at,
        );
    }

    /// Under a declared DAG, `background_until` pointing at a step on another
    /// branch is rejected: that gate can fire while the branch that actually
    /// uses the sidecar is mid-run.
    #[tokio::test]
    async fn background_until_must_name_a_descendant_under_a_declared_dag() {
        let camp = tempfile::tempdir().unwrap();
        let mut sidecar = shell_step("server", vec!["sleep", "5"]);
        sidecar.background = true;
        sidecar.background_until = Some("other".into());
        sidecar.needs = Some(vec![]);
        let other = sleep_step("other", "0", Some(vec![]));
        let err = PipelineRunner::new(make_pipeline("bg", vec![sidecar, other]))
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .expect_err("a gate on a parallel branch races the reap");
        assert!(
            matches!(&err, RunnerError::InvalidConfig(m) if m.contains("does not depend on it")),
            "got {err:?}",
        );
    }

    /// …and a gate that IS a descendant is fine. Also pins the other half of
    /// the sidecar rule: a background step satisfies its dependents at spawn,
    /// so `needs = ["server"]` is a runnable edge rather than a deadlock.
    #[tokio::test]
    async fn a_sidecar_satisfies_its_dependents_at_spawn() {
        let camp = tempfile::tempdir().unwrap();
        let mut sidecar = shell_step("server", vec!["sleep", "30"]);
        sidecar.background = true;
        sidecar.background_until = Some("client".into());
        sidecar.needs = Some(vec![]);
        let client = sleep_step("client", "0", Some(vec!["server"]));
        let meta = PipelineRunner::new(make_pipeline("bg", vec![sidecar, client]))
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            row(&meta, "server").status,
            RunStatus::Success,
            "a sidecar torn down on its gate is the expected lifecycle",
        );
    }

    // ─── R719-F7 (W298): dynamic admission ──────────────────────────────────

    /// Records the lane sequence a run asks for. The daemon's control does the
    /// actual locking; what the runner owes is the right sequence.
    #[derive(Default)]
    struct RecordingAdmission(std::sync::Mutex<Vec<AdmissionLane>>);

    impl RecordingAdmission {
        fn lanes(&self) -> Vec<AdmissionLane> {
            self.0.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl AdmissionControl for RecordingAdmission {
        async fn enter(&self, lane: AdmissionLane) {
            self.0.lock().unwrap().push(lane);
        }
    }

    /// The mixed-`auto` half of R719-F3: work that lands on a build worker
    /// belongs in the fleet lane for exactly as long as it runs, so the local
    /// key is free meanwhile. Local work keeps the run's own lane.
    #[test]
    fn an_offloaded_step_belongs_in_the_fleet_lane_and_a_local_one_does_not() {
        let runner = PipelineRunner::new(make_pipeline("p", vec![]));
        assert_eq!(runner.step_lane(RunWhere::Remote), AdmissionLane::Fleet);
        assert_eq!(runner.step_lane(RunWhere::Local), AdmissionLane::Base);
        assert_eq!(runner.step_lane(RunWhere::Auto), AdmissionLane::Base);
    }

    /// A sub-pipeline child running in a lane of its own admits against THAT
    /// key for its local work — otherwise its steps would re-enter the parent's
    /// lane and undo the very move that closed the R719-F2 gap.
    #[test]
    fn a_child_in_its_own_lane_names_it_instead_of_base() {
        let mut runner = PipelineRunner::new(make_pipeline("child", vec![]));
        runner.admission_lane = Some("peer:cheers".to_string());
        assert_eq!(
            runner.step_lane(RunWhere::Local),
            AdmissionLane::Named("peer:cheers".to_string())
        );
        // Offload still wins: where the work lands beats which lane owns it.
        assert_eq!(runner.step_lane(RunWhere::Remote), AdmissionLane::Fleet);
    }

    /// One lane call per step that actually runs — and none for a step that
    /// doesn't. A skipped step does no work the lane is protecting, so making
    /// it re-queue would be a pure loss.
    #[tokio::test]
    async fn every_executed_step_admits_and_a_skipped_one_does_not() {
        let camp = tempfile::tempdir().unwrap();
        let mut skipped = shell_step("skipped", vec!["true"]);
        skipped.enabled = false;
        let pipeline = make_pipeline(
            "two-plus-one",
            vec![
                shell_step("one", vec!["true"]),
                skipped,
                shell_step("two", vec!["true"]),
            ],
        );
        let admission = Arc::new(RecordingAdmission::default());
        let meta = PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .with_admission(admission.clone())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            admission.lanes(),
            vec![AdmissionLane::Base, AdmissionLane::Base],
            "two executed steps, two lane calls; the disabled step must not re-queue"
        );
    }

    /// The R719-F2 peer-child hole, closed. The child wants `peer:cheers`, its
    /// parent stands in `cargo-target` — before F7 the child took no key at all
    /// and was serialized against nothing. Now it moves into its own lane, and
    /// the parent stands back in its own when the child returns.
    #[tokio::test]
    async fn a_child_wanting_another_lane_moves_into_it_and_hands_it_back() {
        let camp = tempfile::tempdir().unwrap();
        let mut child = make_pipeline("peer-build", vec![shell_step("build", vec!["true"])]);
        child.concurrency_key = Some("peer:cheers".to_string());
        let resolver = MapResolver(
            [("peer:cheers:peer-build".to_string(), child)]
                .into_iter()
                .collect(),
        );

        let mut parent = make_pipeline(
            "release",
            vec![sub_step(
                "peer",
                SubPipelineRef::Peer {
                    camp: "cheers".into(),
                    pipeline: "peer-build".into(),
                },
                false,
            )],
        );
        parent.concurrency_key = Some("cargo-target".to_string());

        let admission = Arc::new(RecordingAdmission::default());
        let meta = PipelineRunner::new(parent)
            .with_camp_root(camp.path().to_path_buf())
            .with_sub_pipeline_resolver(Arc::new(resolver))
            .with_admission(admission.clone())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            admission.lanes(),
            vec![
                // The parent's own sub-pipeline step.
                AdmissionLane::Base,
                // The child's step, in the child's lane.
                AdmissionLane::Named("peer:cheers".to_string()),
                // Back in the parent's lane before the step returns.
                AdmissionLane::Base,
            ],
        );
    }

    /// A child whose key its parent IS standing in changes nothing — no gap,
    /// no lane move, and none of the re-queueing a move costs.
    #[tokio::test]
    async fn a_child_sharing_the_parents_key_never_leaves_the_lane() {
        let camp = tempfile::tempdir().unwrap();
        let mut child = make_pipeline("inner", vec![shell_step("build", vec!["true"])]);
        child.concurrency_key = Some("cargo-target".to_string());
        let resolver = MapResolver(
            [("builtin:inner".to_string(), child)]
                .into_iter()
                .collect(),
        );

        let mut parent = make_pipeline(
            "outer",
            vec![sub_step("inner", SubPipelineRef::Builtin("inner".into()), false)],
        );
        parent.concurrency_key = Some("cargo-target".to_string());

        let admission = Arc::new(RecordingAdmission::default());
        PipelineRunner::new(parent)
            .with_camp_root(camp.path().to_path_buf())
            .with_sub_pipeline_resolver(Arc::new(resolver))
            .with_admission(admission.clone())
            .run()
            .await
            .unwrap();
        assert!(
            admission
                .lanes()
                .iter()
                .all(|lane| *lane == AdmissionLane::Base),
            "an inherited grant needs no lane move: {:?}",
            admission.lanes()
        );
    }

    #[test]
    fn repo_slug_reduces_both_remote_shapes() {
        assert_eq!(
            repo_slug_from_remote("git@github.com:yah-ai/yah.git"),
            "yah-ai/yah"
        );
        assert_eq!(
            repo_slug_from_remote("https://github.com/yah-ai/yah.git"),
            "yah-ai/yah"
        );
        assert_eq!(
            repo_slug_from_remote("https://github.com/yah-ai/yah/"),
            "yah-ai/yah"
        );
        // Self-hosted forge with a deeper path — still the trailing two.
        assert_eq!(
            repo_slug_from_remote("https://git.example.com/a/b/owner/repo.git"),
            "owner/repo"
        );
    }

    #[test]
    fn repo_slug_is_empty_when_there_is_no_usable_remote() {
        // No origin at all, and a bare local path with nothing to split on.
        // Empty means "unknown"; the tier-2 env floor then skips
        // `$GITHUB_REPOSITORY` rather than exporting an empty one.
        assert_eq!(repo_slug_from_remote(""), "");
        assert_eq!(repo_slug_from_remote("repo"), "");
    }

    /// `release.yml` resolves the version it publishes with
    /// `${{ github.event.inputs.tag || github.ref_name }}`. If `github.event`
    /// is an empty object, that falls through to the branch name on any
    /// untagged run and the release publishes under `yah/main/…` with
    /// `"version": "main"` — into a permanent, accumulating index. So the
    /// dispatch inputs have to reach `github.event.inputs`, not just `inputs`.
    #[test]
    fn dispatch_inputs_reach_github_event_inputs() {
        let tmp = TempDir::new().unwrap();
        let mut inputs = HashMap::new();
        inputs.insert("tag".to_string(), "v0.8.21".to_string());

        let ctx = github_context("workflow_dispatch", &inputs, tmp.path());

        let yah_qed_gha::Value::Object(root) = &ctx else {
            panic!("github context is not an object");
        };
        let Some(yah_qed_gha::Value::Object(event)) = root.get("event") else {
            panic!("github.event missing or not an object");
        };
        let Some(yah_qed_gha::Value::Object(event_inputs)) = event.get("inputs") else {
            panic!("github.event.inputs missing or not an object");
        };
        assert_eq!(
            event_inputs.get("tag"),
            Some(&yah_qed_gha::Value::String("v0.8.21".into())),
            "github.event.inputs.tag must carry the dispatched tag, or the \
             release publishes under the branch name"
        );
    }

    #[test]
    fn stderr_tail_strips_env_markers_and_keeps_last_n_lines() {
        let stderr = "first\nsecond\n__qed_gha_env_updates_BEGIN__\nFOO\tbar\n__qed_gha_env_updates_END__\nthird\nfourth\nfifth\n";
        let out = stderr_tail(stderr, 3);
        // Markers + FOO line stripped (FOO\tbar starts with neither prefix
        // so it'll appear — that's OK as it shows env-update side-effect).
        assert!(!out.contains("__qed_gha_env_updates_BEGIN__"));
        assert!(!out.contains("__qed_gha_env_updates_END__"));
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines, vec!["third", "fourth", "fifth"]);
    }

    #[test]
    fn stderr_tail_returns_empty_when_only_env_markers() {
        let stderr = "__qed_gha_env_updates_BEGIN__\n__qed_gha_env_updates_END__\n";
        assert_eq!(stderr_tail(stderr, 10), "");
    }

    #[test]
    fn stderr_tail_empty_input_is_empty() {
        assert_eq!(stderr_tail("", 10), "");
    }

    fn make_scryer(dir: &TempDir) -> Arc<Scryer> {
        let cfg = ScryerConfig::new(dir.path().join("events.db"));
        Arc::new(Scryer::new(cfg, None).unwrap())
    }

    fn one_step_pipeline(name: &str, argv: Vec<String>) -> Pipeline {
        Pipeline {
            allow_late_operator_block: false,
            participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: name.to_string(),
            label: name.to_string(),
            steps: vec![crate::types::QedStep {
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
                name: "step-1".to_string(),
                argv,
                cwd: None,
                env: HashMap::new(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: crate::types::StepKind::Subprocess,
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
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
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

    // ── R507/W208 toolchain pinning preflight ──────────────────────────────

    fn tc_spec(pairs: &[(&str, &str)]) -> crate::toolchain::ToolchainSpec {
        crate::toolchain::ToolchainSpec {
            pins: pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }
    }

    fn host_map(pairs: &[(&str, Option<&str>)]) -> HashMap<String, Option<String>> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.map(str::to_string)))
            .collect()
    }

    #[test]
    fn toolchain_preflight_passes_when_host_satisfies_pin() {
        let mut pipeline = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        pipeline.toolchain = Some(tc_spec(&[("rust", "1.84.0")]));
        let runner = PipelineRunner::new(pipeline)
            .with_host_toolchains(host_map(&[("rust", Some("1.84.0"))]));
        let pf = runner.toolchain_preflight();
        assert!(pf.is_satisfied(), "{:?}", pf.report());
        assert_eq!(pf.entries.len(), 1);
    }

    #[test]
    fn toolchain_preflight_blocks_on_missing_tool() {
        // noisetable's release.apple pins xcode=15.4; a host without it blocks.
        let mut pipeline = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        pipeline.toolchain = Some(tc_spec(&[("xcode", "15.4")]));
        let runner =
            PipelineRunner::new(pipeline).with_host_toolchains(host_map(&[("xcode", None)]));
        let pf = runner.toolchain_preflight();
        assert!(!pf.is_satisfied());
        let report = pf.error_report().expect("blocking ⇒ report");
        assert!(report.contains("xcode"));
        assert!(report.contains("15.4"));
    }

    #[tokio::test]
    async fn run_fails_fast_when_host_cannot_satisfy_pin() {
        // The gate fires before any step executes — even a bare `echo` never
        // runs when the host can't satisfy the pin.
        let mut pipeline = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        pipeline.toolchain = Some(tc_spec(&[("xcode", "15.4")]));
        let runner = PipelineRunner::new(pipeline)
            .with_host_toolchains(host_map(&[("xcode", Some("15.2"))]));
        let err = runner.run().await.unwrap_err();
        match err {
            RunnerError::ToolchainUnsatisfied(report) => {
                assert!(report.contains("xcode"));
                assert!(report.contains("15.4"));
                assert!(
                    report.contains("15.2"),
                    "report names the host version: {report}"
                );
            }
            other => panic!("expected ToolchainUnsatisfied, got {other:?}"),
        }
    }

    #[test]
    fn step_toolchain_override_beats_pipeline_pin() {
        // Pipeline pins ndk=r27; the step overrides to r26d. Host has ndk 26.3
        // — which the pipeline pin (27) would reject but the step override
        // (r26d → 26) satisfies. A satisfied preflight proves the override won.
        let mut pipeline = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        pipeline.toolchain = Some(tc_spec(&[("ndk", "r27")]));
        pipeline.steps[0].toolchain = Some(tc_spec(&[("ndk", "r26d")]));
        let runner = PipelineRunner::new(pipeline)
            .with_host_toolchains(host_map(&[("ndk", Some("26.3.11579264"))]));
        let pf = runner.toolchain_preflight();
        assert!(
            pf.is_satisfied(),
            "step r26d override should win: {:?}",
            pf.report()
        );
        // Sanity: the *pipeline* pin alone (no override) would block this host.
        let mut blocked = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        blocked.toolchain = Some(tc_spec(&[("ndk", "r27")]));
        let blocked_runner = PipelineRunner::new(blocked)
            .with_host_toolchains(host_map(&[("ndk", Some("26.3.11579264"))]));
        assert!(!blocked_runner.toolchain_preflight().is_satisfied());
    }

    #[test]
    fn containerized_step_satisfies_pin_via_image() {
        // A step that pulls an image delegates its toolchain to that image, so
        // a host missing Xcode entirely still passes the preflight.
        let mut pipeline = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        pipeline.toolchain = Some(tc_spec(&[("xcode", "15.4")]));
        pipeline.steps[0].image = Some("apple-builder:15.4".into());
        let runner =
            PipelineRunner::new(pipeline).with_host_toolchains(host_map(&[("xcode", None)]));
        let pf = runner.toolchain_preflight();
        assert!(pf.is_satisfied());
        assert!(matches!(
            pf.entries[0].resolution,
            crate::toolchain::PinResolution::SatisfiedByImage { .. }
        ));
    }

    #[test]
    fn no_toolchain_block_means_no_preflight_entries() {
        let pipeline = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        // Seed an empty host map so this never shells out.
        let runner = PipelineRunner::new(pipeline).with_host_toolchains(HashMap::new());
        let pf = runner.toolchain_preflight();
        assert!(pf.is_satisfied());
        assert!(pf.entries.is_empty());
    }

    // ── Scripted yubaba for qed tests ──────────────────────────────────────

    struct ScriptedWarden {
        lines: Vec<String>,
        exit_code: i32,
        /// R590-F6: container-path → bytes the finished container produced,
        /// served by `fetch_produced_file`.
        produced_files: HashMap<std::path::PathBuf, Vec<u8>>,
    }

    impl ScriptedWarden {
        fn new(lines: Vec<String>, exit_code: i32) -> Self {
            Self { lines, exit_code, produced_files: HashMap::new() }
        }

        /// Seed a produced file so `fetch_produced_file` serves `bytes` at
        /// `path` (R590-F6 retrieval test).
        fn with_produced_file(mut self, path: impl Into<std::path::PathBuf>, bytes: Vec<u8>) -> Self {
            self.produced_files.insert(path.into(), bytes);
            self
        }
    }

    #[async_trait::async_trait]
    impl WardenClient for ScriptedWarden {
        async fn deploy(
            &self,
            _spec: &workload_spec::WorkloadSpec,
        ) -> Result<(), velveteen_exec::RemoteForgeError> {
            Ok(())
        }

        async fn connect_logs(
            &self,
            _ident: &MeshIdent,
        ) -> Result<mpsc::Receiver<String>, velveteen_exec::RemoteForgeError> {
            let (tx, rx) = mpsc::channel(64);
            let lines = self.lines.clone();
            tokio::spawn(async move {
                for line in lines {
                    let _ = tx.send(line).await;
                }
            });
            Ok(rx)
        }

        async fn teardown(&self, _ident: &MeshIdent) -> Result<(), velveteen_exec::RemoteForgeError> {
            Ok(())
        }

        async fn exit_code(
            &self,
            _ident: &MeshIdent,
        ) -> Result<Option<i32>, velveteen_exec::RemoteForgeError> {
            Ok(Some(self.exit_code))
        }

        async fn fetch_produced_file(
            &self,
            _ident: &MeshIdent,
            remote_path: &std::path::Path,
        ) -> Result<Vec<u8>, velveteen_exec::RemoteForgeError> {
            self.produced_files.get(remote_path).cloned().ok_or_else(|| {
                velveteen_exec::RemoteForgeError::Fetch(format!(
                    "no produced file scripted at {}",
                    remote_path.display()
                ))
            })
        }
    }

    /// A `ScriptedWarden` that also keeps every `WorkloadSpec` it was handed, so
    /// a test can assert on what the runner actually put on the wire rather than
    /// only on the run's outcome.
    ///
    /// Deliberately a sibling of `ScriptedWarden` rather than a field on it:
    /// fourteen call sites build that one by struct literal, and widening it
    /// would churn all of them for the benefit of a single assertion.
    struct DeployCapturingWarden {
        inner: ScriptedWarden,
        deployed: std::sync::Mutex<Vec<workload_spec::WorkloadSpec>>,
    }

    impl DeployCapturingWarden {
        fn new(lines: Vec<String>, exit_code: i32) -> Self {
            Self {
                inner: ScriptedWarden::new(lines, exit_code),
                deployed: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn only_spec(&self) -> workload_spec::WorkloadSpec {
            let specs = self.deployed.lock().unwrap();
            assert_eq!(specs.len(), 1, "expected exactly one deploy");
            specs[0].clone()
        }
    }

    #[async_trait::async_trait]
    impl WardenClient for DeployCapturingWarden {
        async fn deploy(
            &self,
            spec: &workload_spec::WorkloadSpec,
        ) -> Result<(), velveteen_exec::RemoteForgeError> {
            self.deployed.lock().unwrap().push(spec.clone());
            Ok(())
        }

        async fn connect_logs(
            &self,
            ident: &MeshIdent,
        ) -> Result<mpsc::Receiver<String>, velveteen_exec::RemoteForgeError> {
            self.inner.connect_logs(ident).await
        }

        async fn teardown(&self, ident: &MeshIdent) -> Result<(), velveteen_exec::RemoteForgeError> {
            self.inner.teardown(ident).await
        }

        async fn exit_code(
            &self,
            ident: &MeshIdent,
        ) -> Result<Option<i32>, velveteen_exec::RemoteForgeError> {
            self.inner.exit_code(ident).await
        }

        async fn fetch_produced_file(
            &self,
            ident: &MeshIdent,
            remote_path: &std::path::Path,
        ) -> Result<Vec<u8>, velveteen_exec::RemoteForgeError> {
            self.inner.fetch_produced_file(ident, remote_path).await
        }
    }

    /// R577-F3: a remote step's declared `[pipeline.steps.env]` reaches the
    /// worker as literal env on the deployed `WorkloadSpec`.
    ///
    /// It did not before this ticket: `execute_step_remote` called
    /// `start_with_sink`, which defaults the `ExecContext`, so the env map was
    /// dropped between the recipe and the wire while the LOCAL paths threaded
    /// the identical map through `ExecContext::with_env`. The failure was
    /// silent in both directions — nothing logged it, and the step just ran
    /// with the variable unset. `desktop-release`'s terminal `publish` step is
    /// the live instance (`YAH_ALMANAC_FEED`), and it is also the channel the
    /// Darwin `dmg-build` leg needs for its Apple signing/notarization creds.
    #[tokio::test]
    async fn remote_step_carries_its_declared_env_to_the_worker() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(DeployCapturingWarden::new(vec!["ok".into()], 0));

        let mut pipeline = one_step_pipeline("env-remote", vec!["publish.sh".to_string()]);
        pipeline.steps[0]
            .env
            .insert("YAH_ALMANAC_FEED".into(), "yah-desktop".into());

        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba.clone());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let spec = yubaba.only_spec();
        let var = spec
            .env
            .iter()
            .find(|e| e.name == "YAH_ALMANAC_FEED")
            .unwrap_or_else(|| {
                panic!(
                    "declared env must reach the worker; spec carried {:?}",
                    spec.env.iter().map(|e| &e.name).collect::<Vec<_>>()
                )
            });
        match &var.value {
            workload_spec::EnvValue::Literal { value } => assert_eq!(value, "yah-desktop"),
            other => panic!("expected a literal env value, got {other:?}"),
        }
    }

    /// R833-F8: `--where=node:<machine>` reaches the wire. A pinned run's
    /// deployed spec carries the imperative node selector, and NOT the mesh-tag
    /// selector — the operator named a box, so the arch/tag filter that would
    /// otherwise narrow the candidate set must not also apply and risk
    /// excluding it.
    #[tokio::test]
    async fn a_pinned_run_puts_the_named_node_on_the_wire() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(DeployCapturingWarden::new(vec!["ok".into()], 0));

        let mut pipeline = one_step_pipeline("pinned", vec!["true".to_string()]);
        // A cross-arch target: the tag matcher WOULD have inferred a node from
        // this, which is exactly what the pin has to win against.
        pipeline.steps[0].platform = Some(crate::platform::PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: None,
            native: false,
        });

        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba.clone())
            .with_pinned_node(MeshIdent("us-west-003".into()));
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let spec = yubaba.only_spec();
        assert_eq!(
            spec.annotations
                .get(velveteen_exec::remote::NODE_SELECTOR_NODE_ANNOTATION)
                .map(String::as_str),
            Some("us-west-003"),
        );
        assert!(
            !spec
                .annotations
                .contains_key(velveteen_exec::remote::NODE_SELECTOR_MESH_TAGS_ANNOTATION),
            "an explicit target must not be re-filtered by inferred arch tags",
        );
    }

    /// The other half of the same guarantee: with no pin, placement is
    /// byte-for-byte what R594 shipped — the arch-matched mesh-tag selector and
    /// no node annotation. This is the regression test for "inference keeps
    /// working exactly as it does today".
    #[tokio::test]
    async fn an_unpinned_run_still_infers_its_target_from_the_step() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(DeployCapturingWarden::new(vec!["ok".into()], 0));

        let mut pipeline = one_step_pipeline("inferred", vec!["true".to_string()]);
        pipeline.steps[0].platform = Some(crate::platform::PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: None,
            native: false,
        });

        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba.clone());
        assert_eq!(runner.run().await.unwrap().status, RunStatus::Success);

        let spec = yubaba.only_spec();
        assert_eq!(
            spec.annotations
                .get(velveteen_exec::remote::NODE_SELECTOR_MESH_TAGS_ANNOTATION)
                .map(String::as_str),
            Some("tag:build-worker,arch:x86,os:linux"),
        );
        assert!(
            !spec
                .annotations
                .contains_key(velveteen_exec::remote::NODE_SELECTOR_NODE_ANNOTATION),
            "no pin ⇒ no imperative selector",
        );
    }

    /// A camp root that is a real git repo with one tracked file, so
    /// `pack_source_context`'s `git ls-files` has something to find.
    fn git_camp_with_tracked_file() -> TempDir {
        let camp = TempDir::new().unwrap();
        let root = camp.path();
        let git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(root)
                .args(args)
                .output()
                .unwrap();
            assert!(out.status.success(), "git {args:?}: {out:?}");
        };
        git(&["init", "-q"]);
        git(&["config", "user.email", "t@example.com"]);
        git(&["config", "user.name", "t"]);
        std::fs::create_dir_all(root.join("oss/mesofact/src")).unwrap();
        std::fs::write(root.join("oss/mesofact/src/main.rs"), b"fn main() {}\n").unwrap();
        git(&["add", "-A"]);
        git(&["commit", "-qm", "init"]);
        camp
    }

    /// R560-T8 end-to-end on the unit path: a remote subprocess step that
    /// declares `source_context` has its tree packed, published, and the
    /// resulting URL delivered to the worker as `YAH_SOURCE_CONTEXT_URL` — then
    /// the single-use object is discarded once the step ends.
    ///
    /// Each half has its own silent-failure mode. Skip the publish and the step
    /// runs with the variable unset, which is only visible once the argv's `:?`
    /// fires on the worker. Skip the discard and every run of every fleet build
    /// leaves a source tarball in the bucket forever — the exact leak R636-B1
    /// deletes on both legs to avoid.
    #[tokio::test]
    async fn remote_step_publishes_its_source_context_and_reclaims_the_key() {
        let dir = TempDir::new().unwrap();
        let camp = git_camp_with_tracked_file();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(DeployCapturingWarden::new(vec!["ok".into()], 0));
        // The same recorder R636-B1's build-image tests use — one seam, so a
        // regression in either transport shows up against the same fixture.
        let publisher = Arc::new(RecordingContextPublisher::default());

        let mut pipeline =
            one_step_pipeline("mesofact-musl", vec!["build-mesofact.sh".to_string()]);
        pipeline.steps[0].source_context = vec![std::path::PathBuf::from("oss/mesofact")];

        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba.clone())
            .with_camp_root(camp.path().to_path_buf())
            .with_build_context_publisher(publisher.clone());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let published = publisher.published.lock().unwrap().clone();
        assert_eq!(published.len(), 1, "exactly one source context per step");
        let (key, tarball) = &published[0];
        assert!(!tarball.is_empty(), "the packed tar must carry bytes");

        let spec = yubaba.only_spec();
        let var = spec
            .env
            .iter()
            .find(|e| e.name == crate::build_context::SOURCE_CONTEXT_URL_ENV)
            .unwrap_or_else(|| {
                panic!(
                    "the worker must learn where to fetch its source; spec carried {:?}",
                    spec.env.iter().map(|e| &e.name).collect::<Vec<_>>()
                )
            });
        match &var.value {
            workload_spec::EnvValue::Literal { value } => {
                assert_eq!(value, &format!("https://ctx.test/{key}.tar.gz"))
            }
            other => panic!("expected a literal env value, got {other:?}"),
        }

        assert_eq!(
            *publisher.discarded.lock().unwrap(),
            vec![key.clone()],
            "the single-use key must be reclaimed once the step ends",
        );
    }

    /// The same contract on the LOCAL container path, which did not have it.
    ///
    /// `source_context` was published only by `execute_step_remote`, so a step
    /// that declared one and resolved to local execution reached its argv with
    /// `$YAH_SOURCE_CONTEXT_URL` unset. `.yah/qed/mesofact-musl.toml`'s aarch64
    /// leg is exactly that shape — `container_platform = "linux/arm64"` makes
    /// it native on the arm64 camp Mac, so it runs here rather than offloading
    /// — and it failed in ~1.4s on every one of its 13 runs while the x86 leg
    /// beside it passed. The pipeline never reached `on_success`, so its
    /// publish never fired either.
    ///
    /// The fixture reproduces that resolution rather than asserting on a bare
    /// step: same host triple, same platform block. Both halves are asserted
    /// for the same reason the remote sibling asserts both — skip the publish
    /// and the failure is invisible until a container's `:?` fires; skip the
    /// discard and every local build leaks a tarball into the bucket.
    #[tokio::test]
    async fn local_container_step_publishes_its_source_context_and_reclaims_the_key() {
        let camp = git_camp_with_tracked_file();
        let exec = std::sync::Arc::new(CapturingExecutor::default());
        let publisher = Arc::new(RecordingContextPublisher::default());

        let mut pipeline = one_step_pipeline(
            "build-mesofact-aarch64-musl",
            vec!["build-mesofact.sh".to_string()],
        );
        pipeline.steps[0].source_context = vec![std::path::PathBuf::from("oss/mesofact")];
        pipeline.steps[0].platform = Some(crate::platform::PlatformSpec {
            target: Some("aarch64-unknown-linux-musl".to_string()),
            container_platform: Some("linux/arm64".to_string()),
            native: true,
        });

        let runner = PipelineRunner::new(pipeline)
            .with_host_triple("aarch64-apple-darwin")
            .with_camp_root(camp.path().to_path_buf())
            .with_build_context_publisher(publisher.clone())
            .with_executor(exec.clone());

        let step = runner.pipeline.steps[0].clone();
        // Guard the premise: if this ever resolves to Offload the step never
        // reaches this path and the test would pass while asserting nothing.
        assert!(
            !matches!(
                runner.resolve_step(&step),
                crate::platform::Resolution::Offload { .. }
            ),
            "fixture must resolve to a LOCAL verdict or it is not testing this path",
        );
        runner.execute_step_local_container(0, &step).await.unwrap();

        let published = publisher.published.lock().unwrap().clone();
        assert_eq!(published.len(), 1, "exactly one source context per step");
        let (key, tarball) = &published[0];
        assert!(!tarball.is_empty(), "the packed tar must carry bytes");

        let (_argv, env) = exec.seen.lock().unwrap().clone().unwrap();
        let got = env
            .iter()
            .find(|(k, _)| k == crate::build_context::SOURCE_CONTEXT_URL_ENV)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| {
                panic!(
                    "the container must learn where to fetch its source; env carried {:?}",
                    env.iter().map(|(k, _)| k).collect::<Vec<_>>()
                )
            });
        assert_eq!(got, format!("https://ctx.test/{key}.tar.gz"));

        assert_eq!(
            *publisher.discarded.lock().unwrap(),
            vec![key.clone()],
            "the single-use key must be reclaimed once the step ends",
        );
    }

    /// An executor that writes files into `ctx.produced_dir`, which is what a
    /// real container does through the bind mount, and records the dir it was
    /// handed.
    #[derive(Default)]
    struct ProducingExecutor {
        /// Basenames to write under the produced dir, with their bytes.
        writes: Vec<(String, Vec<u8>)>,
        seen_produced_dir: std::sync::Mutex<Option<Option<std::path::PathBuf>>>,
    }

    #[async_trait::async_trait]
    impl ForgeExecutor for ProducingExecutor {
        async fn execute(
            &self,
            _spec: ForgeSpec,
            ctx: ExecContext,
            _sink: Option<tokio::sync::mpsc::UnboundedSender<ExecEvent>>,
        ) -> Result<velveteen_exec::ExecOutcome, ForgeExecutorError> {
            *self.seen_produced_dir.lock().unwrap() = Some(ctx.produced_dir.clone());
            if let Some(dir) = &ctx.produced_dir {
                for (name, bytes) in &self.writes {
                    std::fs::write(dir.join(name), bytes).unwrap();
                }
            }
            Ok(velveteen_exec::ExecOutcome {
                status: ForgeStatus::Done {
                    exit_code: 0,
                    ended_at: 0,
                },
                stderr_tail: String::new(),
            })
        }
    }

    fn produces(binary: &str, path: &str, triple: &str) -> ProducedArtifact {
        ProducedArtifact {
            binary: binary.to_string(),
            path: path.to_string(),
            triple: Some(triple.to_string()),
        }
    }

    /// R560-B12. `docker run --rm` throws the writable layer away at exit, so a
    /// local container step used to exit 0 having written a real tarball the
    /// caller could not read — `mesofact-musl`'s aarch64 leg printed
    /// `build-mesofact: PASS`, produced 60 MB, and then died in staging on
    /// "No such file or directory" for the container path it had just written.
    ///
    /// The assertion that matters is the LAST one: the returned artifacts must
    /// be camp-local paths that exist, not the declared container paths, so the
    /// publish leg reads bytes instead of a hole.
    #[tokio::test]
    async fn local_container_step_collects_its_produced_artifacts_off_the_bind() {
        let camp = git_camp_with_tracked_file();
        let exec = std::sync::Arc::new(ProducingExecutor {
            writes: vec![
                ("mesofact-aarch64-unknown-linux-musl.tar.gz".into(), b"tarball-bytes".to_vec()),
                ("almanac-feed-aarch64-unknown-linux-musl.tar.gz".into(), b"feed-bytes".to_vec()),
            ],
            ..Default::default()
        });

        let mut pipeline =
            one_step_pipeline("build-mesofact-aarch64-musl", vec!["build.sh".to_string()]);
        pipeline.steps[0].produces = vec![
            produces(
                "mesofact",
                "/yah/produced/mesofact-aarch64-unknown-linux-musl.tar.gz",
                "aarch64-unknown-linux-musl",
            ),
            produces(
                "almanac-feed",
                "/yah/produced/almanac-feed-aarch64-unknown-linux-musl.tar.gz",
                "aarch64-unknown-linux-musl",
            ),
        ];

        let runner = PipelineRunner::new(pipeline)
            .with_host_triple("aarch64-apple-darwin")
            .with_camp_root(camp.path().to_path_buf())
            .with_executor(exec.clone());

        let step = runner.pipeline.steps[0].clone();
        let collected = runner
            .execute_step_local_container(0, &step)
            .await
            .expect("the step succeeds and its artifacts are collectable")
            .expect("a step declaring `produces` returns them");

        assert_eq!(collected.len(), 2);
        // The declaration's `binary` and `triple` survive — `stage_release`
        // keys the release object off them.
        assert_eq!(collected[0].binary, "mesofact");
        assert_eq!(
            collected[0].triple.as_deref(),
            Some("aarch64-unknown-linux-musl")
        );
        for artifact in &collected {
            let path = std::path::Path::new(&artifact.path);
            assert!(
                path.is_absolute() && path.exists(),
                "the publish leg must receive a path on THIS filesystem, got {}",
                artifact.path,
            );
            assert!(
                !artifact.path.starts_with("/yah/produced"),
                "the raw container path must not survive — it is what staging failed on",
            );
        }
        // R560-T9: the basename is the build's own filename, not the CAS hash,
        // because the release key ends in it.
        assert!(
            collected[0].path.ends_with("mesofact-aarch64-unknown-linux-musl.tar.gz"),
            "got {}",
            collected[0].path,
        );
        assert_eq!(
            std::fs::read(&collected[0].path).unwrap(),
            b"tarball-bytes",
            "the landed bytes must be the ones the step wrote",
        );
    }

    /// The floor: a step declaring no `produces` gets no bind at all, so this
    /// cannot put a mount (or a mkdir) on every local container step in the
    /// corpus.
    #[tokio::test]
    async fn local_container_step_without_produces_binds_no_produced_dir() {
        let camp = git_camp_with_tracked_file();
        let exec = std::sync::Arc::new(ProducingExecutor::default());
        let pipeline = one_step_pipeline("plain", vec!["true".to_string()]);

        let runner = PipelineRunner::new(pipeline)
            .with_host_triple("aarch64-apple-darwin")
            .with_camp_root(camp.path().to_path_buf())
            .with_executor(exec.clone());

        let step = runner.pipeline.steps[0].clone();
        let collected = runner.execute_step_local_container(0, &step).await.unwrap();

        assert!(collected.is_none(), "no produces means nothing to collect");
        assert_eq!(
            *exec.seen_produced_dir.lock().unwrap(),
            Some(None),
            "a step with no `produces` must be handed no produced dir",
        );
    }

    /// The traversal guard. `produces` is operator-authored and `-v` would
    /// happily follow `..` back out of the bind onto the host, so the local leg
    /// enforces the same containment the remote leg enforces at dispatch.
    #[tokio::test]
    async fn local_container_produces_outside_the_produced_dir_is_refused() {
        let camp = git_camp_with_tracked_file();
        let exec = std::sync::Arc::new(ProducingExecutor::default());

        let mut pipeline = one_step_pipeline("escapee", vec!["true".to_string()]);
        pipeline.steps[0].produces = vec![produces(
            "yah",
            "/yah/produced/../../etc/passwd",
            "aarch64-unknown-linux-musl",
        )];

        let runner = PipelineRunner::new(pipeline)
            .with_host_triple("aarch64-apple-darwin")
            .with_camp_root(camp.path().to_path_buf())
            .with_executor(exec.clone());

        let step = runner.pipeline.steps[0].clone();
        let err = runner
            .execute_step_local_container(0, &step)
            .await
            .expect_err("a `..` escape must be refused, not read");
        assert!(
            err.to_string().contains("/yah/produced"),
            "the refusal must name the dir the path had to be under; got {err}",
        );
    }

    /// A local container step declaring NO `source_context` must still make no
    /// upload and see no new variable — the same floor the remote path has, so
    /// the fix above cannot put a network call on every local container step.
    #[tokio::test]
    async fn local_container_step_without_source_context_publishes_nothing() {
        let camp = git_camp_with_tracked_file();
        let exec = std::sync::Arc::new(CapturingExecutor::default());
        let publisher = Arc::new(RecordingContextPublisher::default());

        let pipeline = one_step_pipeline("plain", vec!["true".to_string()]);
        let runner = PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .with_build_context_publisher(publisher.clone())
            .with_executor(exec.clone());

        let step = runner.pipeline.steps[0].clone();
        runner.execute_step_local_container(0, &step).await.unwrap();

        assert!(publisher.published.lock().unwrap().is_empty());
        let (_argv, env) = exec.seen.lock().unwrap().clone().unwrap();
        assert!(
            !env.iter()
                .any(|(k, _)| k == crate::build_context::SOURCE_CONTEXT_URL_ENV),
            "a step that declares no source_context must see no URL",
        );
    }

    /// The whole existing corpus declares no `source_context`, and must
    /// therefore make no upload and see no new variable. Without this, adding
    /// the field would have quietly put a network call on the critical path of
    /// every remote step in every pipeline.
    #[tokio::test]
    async fn remote_step_without_source_context_publishes_nothing() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(DeployCapturingWarden::new(vec!["ok".into()], 0));
        let publisher = Arc::new(RecordingContextPublisher::default());

        let pipeline = one_step_pipeline("plain-remote", vec!["true".to_string()]);
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba.clone())
            .with_build_context_publisher(publisher.clone());
        assert_eq!(runner.run().await.unwrap().status, RunStatus::Success);

        assert!(publisher.published.lock().unwrap().is_empty());
        assert!(publisher.discarded.lock().unwrap().is_empty());
        assert!(
            !yubaba
                .only_spec()
                .env
                .iter()
                .any(|e| e.name == crate::build_context::SOURCE_CONTEXT_URL_ENV),
            "a step that declares no source context must see no new variable",
        );
    }

    /// Remote path happy: single step exits 0, task_run_id populated in step status.
    #[tokio::test]
    async fn remote_step_success() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec!["build ok".to_string()],
            exit_code: 0,
            produced_files: HashMap::new(),
        });

        let pipeline = one_step_pipeline("test-remote", vec!["true".to_string()]);
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba);
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(meta.steps.len(), 1);
        assert!(
            meta.steps[0].task_run_id.is_some(),
            "remote step should record task_run_id"
        );
    }

    /// R590-F6 leg 2 end-to-end: a remote step that declares `produces` has its
    /// output tarball retrieved off the (scripted) build-worker and landed in
    /// camp's content-addressed store — the landed file's BLAKE3 equals the
    /// hash of the bytes the worker emitted (no bytes lost/rewritten in
    /// transit). This is the retrieval unit path the ticket's verify names.
    #[tokio::test]
    async fn remote_step_retrieves_produced_artifact_content_addressed() {
        let dir = TempDir::new().unwrap();
        let camp = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        // R603-T5: produced artifacts must live under the durable dir so they
        // survive the build-worker reaping the container.
        let container_path = "/yah/produced/librusty_v8-x86_64-unknown-linux-musl.tar.gz";
        let payload = b"deterministic librusty_v8 tar bytes \x00\x01\x02\xff".to_vec();
        let expected_blake3 = blake3::hash(&payload).to_hex().to_string();

        let yubaba = Arc::new(
            ScriptedWarden::new(vec!["v8 build complete".into()], 0)
                .with_produced_file(container_path, payload.clone()),
        );

        let mut pipeline = one_step_pipeline("rusty-v8-musl", vec!["build-v8.sh".to_string()]);
        pipeline.steps[0].produces = vec![ProducedArtifact {
            binary: "rusty-v8".into(),
            path: container_path.into(),
            triple: Some("x86_64-unknown-linux-musl".into()),
        }];

        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba)
            .with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Success, "retrieval must not fail the step");

        // The tar landed content-addressed under camp's artifact store, and its
        // on-disk bytes re-hash to the worker's BLAKE3 — preservation proven.
        let landed = camp.path().join(".yah/cache/artifacts").join(&expected_blake3);
        assert!(landed.exists(), "retrieved artifact must land at <camp>/.yah/cache/artifacts/<blake3>");
        let on_disk = std::fs::read(&landed).unwrap();
        assert_eq!(on_disk, payload, "bytes must survive the transport unchanged");
        assert_eq!(blake3::hash(&on_disk).to_hex().to_string(), expected_blake3);

        // R560-T9: retrieval also materialises a NAMED view carrying the
        // build's own filename, and that is the path handed to the publish leg.
        // `stage_release` keys a release object as
        // `<binary>/<version>/<triple>/<basename>`, so a bare CAS path would
        // publish the tarball under its 64-hex BLAKE3 — a URL install.sh never
        // constructs, discovered only after a multi-hour fleet build.
        let named = camp
            .path()
            .join(".yah/cache/artifacts/named")
            .join(&expected_blake3)
            .join("librusty_v8-x86_64-unknown-linux-musl.tar.gz");
        assert!(
            named.exists(),
            "retrieval must leave a named view at {}",
            named.display(),
        );
        assert_eq!(
            std::fs::read(&named).unwrap(),
            payload,
            "the named view must be the same bytes as the CAS entry",
        );
    }

    // ── R555-F6: one-shot workloads are reaped, and reaped LAST ─────────────

    /// A `ScriptedWarden` that records the ORDER of the server-side calls the
    /// runner makes.
    ///
    /// Order is the whole invariant, not an incidental detail: yubaba's
    /// `destroy` reaps the produced dir along with the workload record
    /// (R603-T5), so a reap issued before `fetch_produced_file` deletes the
    /// bytes the coordinator is about to ask for. A test asserting only "both
    /// calls happened" would pass against exactly that bug — hence a call log
    /// rather than a pair of `bool`s.
    struct CallLogWarden {
        inner: ScriptedWarden,
        calls: std::sync::Mutex<Vec<&'static str>>,
        teardown_fails: bool,
    }

    impl CallLogWarden {
        fn new(exit_code: i32) -> Self {
            Self {
                inner: ScriptedWarden::new(vec!["worker log line".into()], exit_code),
                calls: std::sync::Mutex::new(Vec::new()),
                teardown_fails: false,
            }
        }

        fn with_produced_file(mut self, path: impl Into<std::path::PathBuf>, bytes: Vec<u8>) -> Self {
            self.inner = self.inner.with_produced_file(path, bytes);
            self
        }

        /// Make `teardown` error, standing in for a worker that is unreachable
        /// by the time the coordinator gets around to cleaning up.
        fn with_failing_teardown(mut self) -> Self {
            self.teardown_fails = true;
            self
        }

        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl WardenClient for CallLogWarden {
        async fn deploy(
            &self,
            spec: &workload_spec::WorkloadSpec,
        ) -> Result<(), velveteen_exec::RemoteForgeError> {
            self.calls.lock().unwrap().push("deploy");
            self.inner.deploy(spec).await
        }

        async fn connect_logs(
            &self,
            ident: &MeshIdent,
        ) -> Result<mpsc::Receiver<String>, velveteen_exec::RemoteForgeError> {
            self.inner.connect_logs(ident).await
        }

        async fn teardown(&self, ident: &MeshIdent) -> Result<(), velveteen_exec::RemoteForgeError> {
            self.calls.lock().unwrap().push("teardown");
            if self.teardown_fails {
                return Err(velveteen_exec::RemoteForgeError::Teardown(
                    "scripted teardown failure".into(),
                ));
            }
            self.inner.teardown(ident).await
        }

        async fn exit_code(
            &self,
            ident: &MeshIdent,
        ) -> Result<Option<i32>, velveteen_exec::RemoteForgeError> {
            self.inner.exit_code(ident).await
        }

        async fn fetch_produced_file(
            &self,
            ident: &MeshIdent,
            remote_path: &std::path::Path,
        ) -> Result<Vec<u8>, velveteen_exec::RemoteForgeError> {
            self.calls.lock().unwrap().push("fetch_produced");
            self.inner.fetch_produced_file(ident, remote_path).await
        }
    }

    /// R823-B4 regression: a remote participant sidecar that ACTUALLY STOPS when
    /// torn down must not panic the runner.
    ///
    /// `reap_background`'s settle wait is `timeout(_, &mut join)`, which on
    /// success polls the handle **to completion** — and `reap_status` then
    /// awaited that same handle again, which tokio panics on with "JoinHandle
    /// polled after completion", failing the whole pipeline.
    ///
    /// Why it was never caught: the branch was unreachable for as long as
    /// teardown was a silent no-op. The sidecar never stopped, so the settle
    /// timeout ALWAYS elapsed, `join` was never polled to completion, and
    /// `reap_status` could take it. The first destroy that actually reaped its
    /// container — us-west-003, 2026-09-10, the run proving the yubaba
    /// destroy-routing fix — was the first run ever to reach this code, and it
    /// panicked after both the work and the teardown had succeeded. A leak in
    /// one layer was hiding a panic in the next.
    ///
    /// The sidecar here finishes shortly AFTER the kill rather than before it,
    /// and that is the whole shape of the test: `already_finished` is sampled
    /// before the teardown, so a sidecar that had already exited skips the
    /// settle wait entirely and never touches the hazard.
    #[tokio::test]
    async fn a_sidecar_that_stops_on_schedule_is_reaped_without_panicking() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(CallLogWarden::new(0));
        let driver = Arc::new(RemoteForgeDriver::new(scryer, yubaba.clone()));

        let join = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok::<(), RunnerError>(())
        });
        let sidecar = RemoteSidecar {
            driver,
            node: "us-west-003".into(),
            forge_id: Arc::new(std::sync::Mutex::new(Some(ObsForgeId(Uuid::new_v4())))),
        };

        let reap = reap_background(join, Some(sidecar)).await;

        assert_eq!(
            reap.status,
            RunStatus::Success,
            "a sidecar killed on schedule is a healthy lifecycle, not a failure"
        );
        assert!(
            reap.teardown_note.is_none(),
            "the sidecar stopped inside the settle window, so there is no leak to report: {:?}",
            reap.teardown_note
        );
        assert!(!reap.never_dispatched, "this sidecar had a forge id");
        assert!(
            yubaba.calls().contains(&"teardown"),
            "the sidecar must actually have been torn down, got {:?}",
            yubaba.calls()
        );
    }

    fn durable_artifact(path: &str) -> ProducedArtifact {
        ProducedArtifact {
            binary: "out".into(),
            path: path.into(),
            triple: Some("x86_64-unknown-linux-musl".into()),
        }
    }

    /// The ticket: a successful remote step's forge workload was NEVER torn
    /// down. `run_log_task` tears down only on its timeout arm, and
    /// `RemoteForgeDriver::kill` had no production caller in this runner — so
    /// every green remote step left a `forge.<uuid>` record on the worker
    /// forever, bounded by nothing (the produced-dir TTL sweep reclaims bytes,
    /// not records).
    ///
    /// The ORDER assertion is the load-bearing half. Reaping is only safe after
    /// the produced bytes are off the worker, because destroy reaps the produced
    /// dir with the record; the obvious-looking fix — teardown beside the
    /// existing one in `run_log_task` — would satisfy "was it reaped?" while
    /// breaking R590-F6 leg 2.
    #[tokio::test]
    async fn a_successful_remote_step_is_reaped_after_its_artifacts_are_fetched() {
        let dir = TempDir::new().unwrap();
        let camp = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let container_path = "/yah/produced/out.tar.gz";
        let yubaba = Arc::new(
            CallLogWarden::new(0).with_produced_file(container_path, b"tarball".to_vec()),
        );

        let mut pipeline = one_step_pipeline("reap-me", vec!["build.sh".to_string()]);
        pipeline.steps[0].produces = vec![durable_artifact(container_path)];

        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba.clone())
            .with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            yubaba.calls(),
            vec!["deploy", "fetch_produced", "teardown"],
            "the workload must be reaped, and only once its produced bytes are in camp",
        );
    }

    /// Decision (a): a FAILED remote step is reaped too. Its container is
    /// already gone (kamaji reaps exited containers) and the log transport is
    /// still a 501 (`GET /workloads/{ident}/logs`), so retaining the record buys
    /// no post-mortem — only the same leak. The timeout arm already tore down,
    /// so this makes the policy uniform across every terminal status.
    ///
    /// This is also the case the old `Result<ObsForgeId, _>` return made
    /// impossible to fix: the failure leg threw the id away, so the caller had
    /// nothing to name.
    #[tokio::test]
    async fn a_failed_remote_step_is_still_reaped() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(CallLogWarden::new(17));

        let pipeline = one_step_pipeline("fails", vec!["false".to_string()]);
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba.clone());
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(
            yubaba.calls(),
            vec!["deploy", "teardown"],
            "a non-zero exit leaks the same record a green run does",
        );
        assert_eq!(
            meta.steps[0].task_run_id.as_deref().map(str::len),
            Some(36),
            "the failed step records the workload it ran as, not None",
        );
    }

    /// The other side of the `Option<ObsForgeId>`: a step the runner REFUSED
    /// before deploying has no workload anywhere, so it must not issue a
    /// teardown at all. Reaping a phantom would be harmless against yubaba
    /// (destroy answers `not_found`) and actively misleading in the logs.
    ///
    /// The refusal used is the R603-T5 durable-path guard, which fires after a
    /// dispatcher is wired but before anything is deployed — the interesting
    /// shape, since "no dispatcher at all" cannot observe a warden.
    ///
    /// R555-F11: `with_force(true)` is what keeps this test about the DISPATCH
    /// guard. The same declaration now also trips the kick-time
    /// fleet-portability gate, which would abort the run before a step ever
    /// starts; forcing past it leaves the dispatch guard as the thing under
    /// test, and doubles as proof that `--force` really does let a run through
    /// to fail the slow way.
    #[tokio::test]
    async fn a_step_that_never_dispatched_is_not_reaped() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(CallLogWarden::new(0));

        let mut pipeline = one_step_pipeline("refused", vec!["build.sh".to_string()]);
        // Outside /yah/produced ⇒ refused at dispatch.
        pipeline.steps[0].produces = vec![durable_artifact("/tmp/out.tar.gz")];

        let runner =
            PipelineRunner::new_remote(pipeline, scryer, yubaba.clone()).with_force(true);
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        assert!(
            yubaba.calls().is_empty(),
            "a refused spec touched no worker; calls were {:?}",
            yubaba.calls(),
        );
        assert!(
            meta.steps[0].task_run_id.is_none(),
            "there is no workload identity to record",
        );
    }

    /// Decision (b): the reap is best-effort. A teardown that errors leaves a
    /// record behind and says so in a warning, but it must not turn a step that
    /// did its work — and whose artifacts are already landed in camp — into a
    /// failed build. Trading a leaked record for a false red is the worse deal.
    #[tokio::test]
    async fn a_teardown_error_does_not_fail_an_otherwise_successful_step() {
        let dir = TempDir::new().unwrap();
        let camp = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let container_path = "/yah/produced/out.tar.gz";
        let payload = b"tarball".to_vec();
        let yubaba = Arc::new(
            CallLogWarden::new(0)
                .with_produced_file(container_path, payload.clone())
                .with_failing_teardown(),
        );

        let mut pipeline = one_step_pipeline("reap-fails", vec!["build.sh".to_string()]);
        pipeline.steps[0].produces = vec![durable_artifact(container_path)];

        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba.clone())
            .with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();

        assert_eq!(
            meta.status,
            RunStatus::Success,
            "a failed reap must not redden a step whose work succeeded: {:?}",
            meta.steps[0].error,
        );
        assert_eq!(
            yubaba.calls(),
            vec!["deploy", "fetch_produced", "teardown"],
            "the reap must still have been attempted",
        );
        // The retrieval it must not have disturbed.
        let landed = camp
            .path()
            .join(".yah/cache/artifacts")
            .join(blake3::hash(&payload).to_hex().to_string());
        assert!(landed.exists(), "the artifact stays landed regardless of the reap");
    }

    /// Remote path failure: non-zero exit code propagates as Failed status.
    #[tokio::test]
    async fn remote_step_failure() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec!["error: something went wrong".to_string()],
            exit_code: 1,
            produced_files: HashMap::new(),
        });

        let pipeline = one_step_pipeline("test-remote-fail", vec!["false".to_string()]);
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba);
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// R508: a yubaba-dispatched step streams its log lines into the live
    /// event sink as `StepOutput` *during* the run — not just into scryer
    /// post-completion. The scripted yubaba emits two lines; both surface as
    /// StepOutput events carrying the step's index and name.
    #[tokio::test]
    async fn remote_step_streams_output_to_sink() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec!["remote line 1".to_string(), "remote line 2".to_string()],
            exit_code: 0,
            produced_files: HashMap::new(),
        });

        let (tx, mut rx) = mpsc::unbounded_channel();
        let pipeline = one_step_pipeline("test-remote-stream", vec!["true".to_string()]);
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba).with_events(tx);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let mut lines = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let QedEvent::StepOutput { index, name, line, .. } = ev {
                assert_eq!(index, 0, "single-step pipeline → index 0");
                assert_eq!(name, "step-1", "StepOutput carries the step name");
                lines.push(line);
            }
        }
        assert_eq!(
            lines,
            vec!["remote line 1".to_string(), "remote line 2".to_string()],
            "both yubaba log lines must stream through as StepOutput",
        );
    }

    /// R603-T1: a remote step publishes its yubaba workload id via
    /// `StepRemoteDispatched` BEFORE the step finishes, so the camp daemon can
    /// persist a reattachable non-terminal record. Asserts the event carries a
    /// non-empty forge id and arrives strictly before `StepFinished` for that
    /// index (the ordering boot-reconcile relies on).
    #[tokio::test]
    async fn remote_step_emits_workload_binding_before_finish() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec!["build ok".to_string()],
            exit_code: 0,
            produced_files: HashMap::new(),
        });

        let (tx, mut rx) = mpsc::unbounded_channel();
        let pipeline = one_step_pipeline("test-remote-binding", vec!["true".to_string()]);
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba).with_events(tx);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        // Walk the event stream in order: the dispatch event must appear, carry a
        // non-empty forge id, and precede the StepFinished for index 0.
        let mut dispatched_forge: Option<String> = None;
        let mut saw_finished = false;
        while let Ok(ev) = rx.try_recv() {
            match ev {
                QedEvent::StepRemoteDispatched { index, forge_id, .. } => {
                    assert_eq!(index, 0, "single-step pipeline → index 0");
                    assert!(!forge_id.is_empty(), "dispatch event must carry a workload id");
                    assert!(!saw_finished, "dispatch must precede StepFinished");
                    dispatched_forge = Some(forge_id);
                }
                QedEvent::StepFinished { index: 0, .. } => saw_finished = true,
                _ => {}
            }
        }
        let forge = dispatched_forge.expect("remote step must emit StepRemoteDispatched");
        // The same id ends up on the terminal step status (task_run_id), so the
        // persisted record and the live binding agree.
        assert_eq!(
            meta.steps[0].task_run_id.as_deref(),
            Some(forge.as_str()),
            "dispatched forge id must match the step's recorded task_run_id",
        );
    }

    /// R603-T5: a remote step whose `produces` path is NOT under the durable
    /// dir (`/yah/produced`) is rejected at dispatch — its output would be read
    /// off the container rootfs and lost the moment the worker reaps the exited
    /// container. The run fails with a clear pointer instead of silently
    /// orphaning the artifact.
    ///
    /// R555-F11: `with_force(true)` below keeps this about the DISPATCH guard.
    /// The kick-time fleet-portability gate now catches the same declaration one
    /// layer earlier (which is the improvement), so reaching the dispatch guard
    /// at all requires forcing past it. Both layers stay: the gate can be forced
    /// off, and this guard is what stops the artifact being orphaned when it is.
    #[tokio::test]
    async fn remote_step_rejects_non_durable_produces_path() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 0,
            produced_files: HashMap::new(),
        });

        let mut pipeline = one_step_pipeline("test-bad-produces", vec!["true".to_string()]);
        pipeline.steps[0].produces = vec![ProducedArtifact {
            binary: "rusty-v8".to_string(),
            // Under /tmp, not /yah/produced → not reap-durable.
            path: "/tmp/librusty_v8.tar.gz".to_string(),
            triple: None,
        }];

        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba).with_force(true);
        let meta = runner.run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Failed,
            "a non-durable produces path must fail the run"
        );
        let err = meta.steps[0].error.clone().unwrap_or_default();
        assert!(
            err.contains("/yah/produced"),
            "error must point at the durable dir convention; got {err:?}"
        );
    }

    /// Remote path: second step skipped when first fails with on_fail=Abort.
    #[tokio::test]
    async fn remote_abort_on_fail() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 1,
            produced_files: HashMap::new(),
        });

        let mut pipeline = one_step_pipeline("test-abort", vec!["false".to_string()]);
        pipeline.steps.push(crate::types::QedStep {
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
            name: "step-2".to_string(),
            argv: vec!["true".to_string()],
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: crate::types::StepKind::Subprocess,
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
        });

        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba);
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(
            meta.steps.len(),
            1,
            "step-2 should be skipped after step-1 fails"
        );
    }

    // ── Outcome dispatch tests ─────────────────────────────────────────────

    use crate::types::Outcome;
    use std::sync::Mutex;

    struct RecordingDispatcher {
        calls: Mutex<Vec<String>>,
        /// `req.version` from each `publish()` call, in order — separate from
        /// `calls` so R876-F6's gate tests can assert the resolved version
        /// without disturbing the `publish:{bucket}:{count}` string other
        /// tests already assert on.
        publish_versions: Mutex<Vec<String>>,
    }

    impl RecordingDispatcher {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(vec![]),
                publish_versions: Mutex::new(vec![]),
            })
        }

        fn recorded(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl OutcomeDispatcher for RecordingDispatcher {
        async fn yubaba_deploy(&self, service: &str, env: &str) -> Result<(), RunnerError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("yubaba-deploy:{service}:{env}"));
            Ok(())
        }

        async fn almanac_run(&self, pipeline: &str) -> Result<(), RunnerError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("almanac-run:{pipeline}"));
            Ok(())
        }

        async fn publish(&self, req: &crate::publish::PublishRequest) -> Result<(), RunnerError> {
            // Record the bucket + how many artifacts the run collected, so a
            // test can assert that only *successful* steps' artifacts arrive.
            self.calls.lock().unwrap().push(format!(
                "publish:{}:{}",
                req.bucket,
                req.artifacts.len()
            ));
            self.publish_versions.lock().unwrap().push(req.version.clone());
            Ok(())
        }
    }

    fn pipeline_with_outcomes(
        on_success: Vec<Outcome>,
        on_fail: Vec<Outcome>,
        argv: Vec<String>,
    ) -> Pipeline {
        Pipeline {
            allow_late_operator_block: false,
            participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "test".to_string(),
            label: "test".to_string(),
            steps: vec![crate::types::QedStep {
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
                name: "step-1".to_string(),
                argv,
                cwd: None,
                env: HashMap::new(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: crate::types::StepKind::Subprocess,
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
            on_success,
            on_fail,
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
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

    /// on_success outcomes are dispatched when the pipeline passes.
    #[tokio::test]
    async fn dispatches_on_success() {
        let dispatcher = RecordingDispatcher::new();
        let pipeline = pipeline_with_outcomes(
            vec![
                Outcome::YubabaDeploy {
                    service: "yah".into(),
                    env: "production".into(),
                },
                Outcome::AlmanacRun {
                    pipeline: "update-index".into(),
                },
            ],
            vec![],
            vec!["true".to_string()],
        );
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        let calls = dispatcher.recorded();
        assert_eq!(
            calls,
            vec!["yubaba-deploy:yah:production", "almanac-run:update-index"]
        );
    }

    /// on_fail outcomes are dispatched when the pipeline fails; on_success is not.
    #[tokio::test]
    async fn dispatches_on_fail_not_on_success() {
        let dispatcher = RecordingDispatcher::new();
        let pipeline = pipeline_with_outcomes(
            vec![Outcome::YubabaDeploy {
                service: "yah".into(),
                env: "production".into(),
            }],
            vec![Outcome::AlmanacRun {
                pipeline: "notify-failure".into(),
            }],
            vec!["false".to_string()],
        );
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        let calls = dispatcher.recorded();
        assert_eq!(calls, vec!["almanac-run:notify-failure"]);
    }

    /// No outcomes = nothing dispatched.
    #[tokio::test]
    async fn no_outcomes_no_dispatch() {
        let dispatcher = RecordingDispatcher::new();
        let pipeline = pipeline_with_outcomes(vec![], vec![], vec!["true".to_string()]);
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        runner.run().await.unwrap();
        assert!(dispatcher.recorded().is_empty());
    }

    /// An Outcome::Publish collects the `produces` of *successful* steps and
    /// hands them to `dispatcher.publish` (R330-F3). Here the single step
    /// declares one artifact and succeeds, so publish sees 1 artifact.
    #[tokio::test]
    async fn publish_outcome_collects_produced_artifacts() {
        let dispatcher = RecordingDispatcher::new();
        let mut pipeline = pipeline_with_outcomes(
            vec![Outcome::Publish {
                provider: "r2".into(),
                bucket: "yah-releases".into(),
                prefix: None,
                base_url: None,
                require_explicit_version: false,
                per_leg: false,
            }],
            vec![],
            vec!["true".to_string()],
        );
        pipeline.steps[0].produces = vec![crate::types::ProducedArtifact {
            binary: "yah".into(),
            path: "target/release/yah".into(),
            triple: Some("darwin-aarch64".into()),
        }];
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(dispatcher.recorded(), vec!["publish:yah-releases:1"]);
    }

    /// R876-F6: `YAH_RELEASE_VERSION` is process-global env, and several tests
    /// in this module set/clear it. Serializing the ones that do keeps them
    /// from racing each other under cargo's default parallel test threads.
    static RELEASE_VERSION_ENV_LOCK: Mutex<()> = Mutex::new(());

    /// R876-F6 — the gate itself: `require_explicit_version = true` refuses to
    /// dispatch when `YAH_RELEASE_VERSION` is unset. The run still succeeds
    /// (a build is a build), but nothing reaches `dispatcher.publish`, which is
    /// the whole invariant — a green iteration run must never cut a release
    /// under the fallback workspace version.
    #[tokio::test]
    async fn publish_outcome_with_required_version_skips_when_unset() {
        let _guard = RELEASE_VERSION_ENV_LOCK.lock().unwrap();
        std::env::remove_var("YAH_RELEASE_VERSION");

        let dispatcher = RecordingDispatcher::new();
        let pipeline = pipeline_with_outcomes(
            vec![Outcome::Publish {
                provider: "r2".into(),
                bucket: "yah-dev".into(),
                prefix: None,
                base_url: None,
                require_explicit_version: true,
                per_leg: false,
            }],
            vec![],
            vec!["true".to_string()],
        );
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();

        assert_eq!(
            meta.status,
            RunStatus::Success,
            "the build itself must not fail just because publish was skipped"
        );
        assert!(
            dispatcher.recorded().is_empty(),
            "no publish call may reach the dispatcher when the release version was never stated"
        );
    }

    /// R876-F6 — the converse: an explicit `YAH_RELEASE_VERSION` still
    /// publishes, carrying that exact version, proving the gate is a refusal
    /// of the *implicit* fallback and not a disabled outcome.
    #[tokio::test]
    async fn publish_outcome_with_required_version_fires_when_set() {
        let _guard = RELEASE_VERSION_ENV_LOCK.lock().unwrap();
        std::env::set_var("YAH_RELEASE_VERSION", "7.7.7");

        let dispatcher = RecordingDispatcher::new();
        let pipeline = pipeline_with_outcomes(
            vec![Outcome::Publish {
                provider: "r2".into(),
                bucket: "yah-dev".into(),
                prefix: None,
                base_url: None,
                require_explicit_version: true,
                per_leg: false,
            }],
            vec![],
            vec!["true".to_string()],
        );
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();
        std::env::remove_var("YAH_RELEASE_VERSION");

        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(dispatcher.recorded(), vec!["publish:yah-dev:0"]);
        assert_eq!(
            *dispatcher.publish_versions.lock().unwrap(),
            vec!["7.7.7".to_string()],
            "the explicit version must be the one actually published, not the workspace fallback"
        );
    }

    /// R603-T4: `resume_terminal_publish_for_remote_step` replays the terminal
    /// publish for a remote step that finished while the daemon was down. It
    /// retrieves the step's `produces` off the (scripted) build-worker and fires
    /// the pipeline's `on_success` Outcome::Publish against the LANDED artifact —
    /// exactly one publish carrying the one retrieved artifact — WITHOUT
    /// re-running the build step. This is the durable-resume path R603-T2's boot
    /// reconciler calls once it confirms a persisted remote run reached Success.
    #[tokio::test]
    async fn resume_publishes_retrieved_remote_artifact() {
        let dir = TempDir::new().unwrap();
        let camp = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let container_path = "/tmp/out/librusty_v8-x86_64-unknown-linux-musl.tar.gz";
        let payload = b"resumed build tar bytes \x00\x01\x02\xff".to_vec();
        let expected_blake3 = blake3::hash(&payload).to_hex().to_string();

        let yubaba = Arc::new(
            ScriptedWarden::new(vec!["v8 build complete".into()], 0)
                .with_produced_file(container_path, payload.clone()),
        );

        let mut pipeline = pipeline_with_outcomes(
            vec![Outcome::Publish {
                provider: "r2".into(),
                bucket: "yah-releases".into(),
                prefix: None,
                base_url: None,
                require_explicit_version: false,
                per_leg: false,
            }],
            vec![],
            vec!["build-v8.sh".to_string()],
        );
        pipeline.steps[0].produces = vec![ProducedArtifact {
            binary: "rusty-v8".into(),
            path: container_path.into(),
            triple: Some("x86_64-unknown-linux-musl".into()),
        }];

        let dispatcher = RecordingDispatcher::new();
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba)
            .with_camp_root(camp.path().to_path_buf())
            .with_dispatcher(dispatcher.clone());

        // The daemon persisted this bare-uuid workload id at dispatch (R603-T1);
        // reconcile hands it back as an ObsForgeId. Resume does NOT run the
        // pipeline — the build already finished remotely.
        let forge_id = ObsForgeId(Uuid::new_v4());
        runner
            .resume_terminal_publish_for_remote_step(0, &forge_id)
            .await
            .expect("resume publishes the retrieved artifact");

        // Exactly one publish, carrying the single retrieved artifact.
        assert_eq!(dispatcher.recorded(), vec!["publish:yah-releases:1"]);
        // The bytes the publish leg saw came off the worker and landed
        // content-addressed in camp, not the unreachable container path.
        let landed = camp.path().join(".yah/cache/artifacts").join(&expected_blake3);
        assert!(landed.exists(), "resume must land the retrieved artifact in camp's store");
    }

    /// R603-T4 reaping-window fork: if the build finished DURING the outage and
    /// kamaji already reaped the container, the produced artifact is
    /// un-retrievable. Resume must surface that as an error and fire NO publish —
    /// never silently claim published. (Retrieval runs before outcome dispatch,
    /// so the `?` short-circuits the publish.)
    #[tokio::test]
    async fn resume_errors_and_skips_publish_when_artifact_reaped() {
        let dir = TempDir::new().unwrap();
        let camp = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        // No produced file scripted → fetch_produced_file errors, modelling a
        // container kamaji already reaped.
        let yubaba = Arc::new(ScriptedWarden::new(vec![], 0));

        let mut pipeline = pipeline_with_outcomes(
            vec![Outcome::Publish {
                provider: "r2".into(),
                bucket: "yah-releases".into(),
                prefix: None,
                base_url: None,
                require_explicit_version: false,
                per_leg: false,
            }],
            vec![],
            vec!["build-v8.sh".to_string()],
        );
        pipeline.steps[0].produces = vec![ProducedArtifact {
            binary: "rusty-v8".into(),
            path: "/tmp/out/reaped.tar.gz".into(),
            triple: Some("x86_64-unknown-linux-musl".into()),
        }];

        let dispatcher = RecordingDispatcher::new();
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba)
            .with_camp_root(camp.path().to_path_buf())
            .with_dispatcher(dispatcher.clone());

        let forge_id = ObsForgeId(Uuid::new_v4());
        let err = runner
            .resume_terminal_publish_for_remote_step(0, &forge_id)
            .await
            .expect_err("a reaped artifact must surface as an error, not a silent success");
        assert!(
            matches!(err, RunnerError::StepFailed { .. }),
            "retrieval failure should map to StepFailed, got {err:?}",
        );
        assert!(
            dispatcher.recorded().is_empty(),
            "no publish may fire when the artifact was reaped",
        );
    }

    /// A failing step's `produces` is dropped — publish only ever runs on
    /// on_success outcomes anyway, but guard the collection too.
    #[tokio::test]
    async fn failed_step_artifacts_not_collected() {
        let dispatcher = RecordingDispatcher::new();
        let mut pipeline = pipeline_with_outcomes(
            vec![],
            vec![Outcome::Publish {
                provider: "r2".into(),
                bucket: "yah-releases".into(),
                prefix: None,
                base_url: None,
                require_explicit_version: false,
                per_leg: false,
            }],
            vec!["false".to_string()],
        );
        pipeline.steps[0].produces = vec![crate::types::ProducedArtifact {
            binary: "yah".into(),
            path: "target/release/yah".into(),
            triple: None,
        }];
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        // Publish ran as an on_fail outcome but collected 0 artifacts (the
        // producing step failed).
        assert_eq!(dispatcher.recorded(), vec!["publish:yah-releases:0"]);
    }

    // ── R509 Outcome::Provider dispatch wiring ──────────────────────────────

    /// Test adapter: records that it ran and (live path) returns a transformed
    /// copy of the first input artifact (same path — an in-place transform like
    /// notarize) plus one *new* artifact (an appcast), so a downstream outcome
    /// can be asserted to see the threaded set.
    struct FakeProvider {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::provider::ReleaseProvider for FakeProvider {
        fn name(&self) -> &str {
            "fake-transform"
        }
        async fn dispatch(
            &self,
            ctx: &crate::provider::ProviderContext<'_>,
        ) -> Result<crate::provider::ProviderReport, RunnerError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Echo every input back (in-place transform: same paths) and
            // append a brand-new appcast artifact.
            let mut produced: Vec<ProducedArtifact> = ctx.artifacts.to_vec();
            produced.push(ProducedArtifact {
                binary: "appcast".into(),
                path: "out/appcast.xml".into(),
                triple: None,
            });
            Ok(crate::provider::ProviderReport {
                actions: vec!["transformed".into()],
                produced,
                published: vec!["https://fake/feed.xml".into()],
            })
        }
    }

    fn fake_registry(
        calls: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Arc<crate::provider::ProviderRegistry> {
        Arc::new(crate::provider::ProviderRegistry::new().with(Arc::new(FakeProvider { calls })))
    }

    /// An `Outcome::Provider` dispatches through the wired registry on success.
    #[tokio::test]
    async fn provider_outcome_dispatches_through_registry() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let pipeline = pipeline_with_outcomes(
            vec![Outcome::Provider {
                provider: "fake-transform".into(),
                with: serde_json::Value::Null,
                base_url: None,
            }],
            vec![],
            vec!["true".to_string()],
        );
        let runner = PipelineRunner::new(pipeline).with_release_providers(
            fake_registry(calls.clone()),
            Arc::new(crate::provider::MapSecrets::default()),
        );
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// A provider transform folds its produced artifacts back into the working
    /// set so a *following* `Outcome::Publish` ships the transformed bundle plus
    /// any new artifact (the notarize→sparkle / sign→publish chain).
    #[tokio::test]
    async fn provider_transform_feeds_downstream_publish() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let dispatcher = RecordingDispatcher::new();
        let mut pipeline = pipeline_with_outcomes(
            vec![
                Outcome::Provider {
                    provider: "fake-transform".into(),
                    with: serde_json::Value::Null,
                    base_url: None,
                },
                Outcome::Publish {
                    provider: "r2".into(),
                    bucket: "yah-releases".into(),
                    prefix: None,
                    base_url: None,
                    require_explicit_version: false,
                    per_leg: false,
                },
            ],
            vec![],
            vec!["true".to_string()],
        );
        pipeline.steps[0].produces = vec![ProducedArtifact {
            binary: "yah".into(),
            path: "target/release/yah".into(),
            triple: None,
        }];
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone())
            .with_release_providers(
                fake_registry(calls.clone()),
                Arc::new(crate::provider::MapSecrets::default()),
            );
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        // The original artifact (replaced in place) + the appended appcast = 2.
        assert_eq!(dispatcher.recorded(), vec!["publish:yah-releases:2"]);
    }

    /// An `Outcome::Provider` naming an unregistered adapter fails the run with
    /// a typed error listing the known names (default empty registry).
    #[tokio::test]
    async fn unknown_provider_outcome_is_typed_error() {
        let pipeline = pipeline_with_outcomes(
            vec![Outcome::Provider {
                provider: "ghost".into(),
                with: serde_json::Value::Null,
                base_url: None,
            }],
            vec![],
            vec!["true".to_string()],
        );
        let err = PipelineRunner::new(pipeline).run().await.unwrap_err();
        assert!(
            matches!(err, RunnerError::Outcome(ref m) if m.contains("ghost")),
            "unknown provider surfaces a typed Outcome error: {err}"
        );
    }

    // ── R325-F2 live event-stream tests ────────────────────────────────────

    /// A runner with an attached sink emits the full lifecycle in order, with
    /// the step's stdout captured as a `StepOutput` line.
    #[tokio::test]
    async fn emits_lifecycle_events_with_streamed_output() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pipeline = one_step_pipeline(
            "test-events",
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "echo hello-stdout".to_string(),
            ],
        );
        let runner = PipelineRunner::new(pipeline).with_events(tx);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let mut events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }

        assert!(
            matches!(
                events.first(),
                Some(QedEvent::RunStarted { total_steps: 1, .. })
            ),
            "first event is RunStarted, got {:?}",
            events.first()
        );
        assert!(
            matches!(
                events.last(),
                Some(QedEvent::RunFinished {
                    status: RunStatus::Success,
                    ..
                })
            ),
            "last event is RunFinished/Success, got {:?}",
            events.last()
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, QedEvent::StepStarted { index: 0, .. })),
            "saw StepStarted for step 0"
        );
        assert!(
            events.iter().any(|e| matches!(
                e,
                QedEvent::StepFinished {
                    index: 0,
                    status: RunStatus::Success,
                    ..
                }
            )),
            "saw StepFinished/Success for step 0"
        );
        assert!(
            events.iter().any(|e| matches!(
                e,
                QedEvent::StepOutput { stream: OutputStream::Stdout, line, .. } if line == "hello-stdout"
            )),
            "captured the echoed stdout line; events={events:?}"
        );
    }

    // ── R513-F2 background sidecar steps (W207 Gap #4) ─────────────────────

    /// Build a single subprocess [`QedStep`] named `name` running `argv`,
    /// reusing the fully-populated literal in [`one_step_pipeline`] so new
    /// fields don't need threading through each background test.
    fn mk_step(name: &str, argv: &[&str]) -> crate::types::QedStep {
        let mut p = one_step_pipeline("x", argv.iter().map(|s| s.to_string()).collect());
        let mut s = p.steps.remove(0);
        s.name = name.to_string();
        s
    }

    /// R590-F2: a remote subprocess step's placement mesh-tags come from its
    /// declared `[platform].target` arch — so an arm64 host targeting
    /// x86_64-unknown-linux-musl is pinned to an x86 build-worker (us-west-002),
    /// not left to emulate. No target ⇒ empty (any infra node).
    /// R876-F4: the sharing key must separate the two things that would
    /// otherwise share one cargo target dir on a worker — `mesofact-musl`'s
    /// two triples, and any two pipelines that happen to name a step the same.
    /// A step that does not declare `cache` gets no key at all.
    #[test]
    fn the_build_cache_key_separates_pipelines_steps_and_triples() {
        use crate::platform::PlatformSpec;

        let plat = |t: &str| {
            Some(PlatformSpec {
                target: Some(t.into()),
                container_platform: None,
                native: true,
            })
        };

        let mut uncached = mk_step("build-mesofact-x86_64-musl", &["true"]);
        uncached.platform = plat("x86_64-unknown-linux-musl");
        assert_eq!(
            step_cache_key("mesofact-musl", &uncached),
            None,
            "cold-every-run stays the default",
        );

        let mut x86 = uncached.clone();
        x86.cache = true;
        let mut arm = x86.clone();
        arm.name = "build-mesofact-aarch64-musl".into();
        arm.platform = plat("aarch64-unknown-linux-musl");

        let k_x86 = step_cache_key("mesofact-musl", &x86).expect("cache = true yields a key");
        let k_arm = step_cache_key("mesofact-musl", &arm).expect("cache = true yields a key");
        let k_other = step_cache_key("some-other-pipeline", &x86).expect("key");

        assert_ne!(k_x86, k_arm, "the two legs must not share a target dir");
        assert_ne!(
            k_x86, k_other,
            "a second pipeline must not land on this one's target dir",
        );
        for k in [&k_x86, &k_arm, &k_other] {
            assert!(
                workload_spec::forge_cache::is_valid_key(k),
                "{k} must be a safe single path component",
            );
        }
        assert!(
            k_x86.contains("mesofact-musl") && k_x86.contains("x86_64-unknown-linux-musl"),
            "the key should stay legible on the worker for debugging + eviction: {k_x86}",
        );
    }

    #[test]
    fn remote_subprocess_mesh_tags_pins_arch_matched_worker_from_target() {
        use crate::platform::PlatformSpec;

        let plain = mk_step("plain", &["cargo", "build"]);
        assert!(
            remote_subprocess_mesh_tags(&plain).is_empty(),
            "no platform.target must leave placement unpinned",
        );

        let mut x86 = mk_step("v8", &["build-v8.sh", "x86_64-unknown-linux-musl", "out.tar.gz"]);
        x86.platform = Some(PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: None,
            native: false,
        });
        assert_eq!(
            remote_subprocess_mesh_tags(&x86),
            vec![
                "tag:build-worker".to_string(),
                "arch:x86".to_string(),
                "os:linux".to_string()
            ],
        );

        let mut arm = mk_step("arm", &["true"]);
        arm.platform = Some(PlatformSpec {
            target: Some("aarch64-unknown-linux-musl".into()),
            container_platform: None,
            native: false,
        });
        assert_eq!(
            remote_subprocess_mesh_tags(&arm),
            vec![
                "tag:build-worker".to_string(),
                "arch:arm".to_string(),
                "os:linux".to_string()
            ],
        );
    }

    /// R555-B10: the capability every placement assertion in this module pins.
    /// These tests are about what a step *declares* routing to, so they must not
    /// depend on which cross toolchains the machine running them has installed —
    /// the Capability demotion has its own table in
    /// [`crate::platform`](crate::platform)'s tests.
    const ALL_TOOLS: &crate::nativecross::ToolAvailability =
        &crate::nativecross::ToolAvailability::FULL;

    /// A `native = true` step whose cross-arch target forces the offload branch
    /// of the policy — the `rusty-v8-musl` shape.
    fn native_offload_step() -> crate::types::QedStep {
        use crate::platform::PlatformSpec;
        let mut s = mk_step(
            "build-v8",
            &["build-v8.sh", "x86_64-unknown-linux-musl", "out.tar.gz"],
        );
        s.platform = Some(PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: None,
            native: true,
        });
        s
    }

    /// The `desktop-release` darwin row in miniature: `native = true` on an
    /// `aarch64-apple-darwin` target, which no container can build.
    fn darwin_native_step() -> crate::types::QedStep {
        use crate::platform::PlatformSpec;
        let mut s = mk_step("dmg", &["cargo", "tauri", "build", "--bundles", "app"]);
        s.platform = Some(PlatformSpec {
            target: Some("aarch64-apple-darwin".into()),
            container_platform: None,
            native: true,
        });
        s
    }

    /// R577-T1: an offloaded step whose target OS has no container runs on the
    /// worker's own userland; every other offloaded step keeps its container.
    ///
    /// The negative half is the load-bearing one — `rusty-v8-musl` is also
    /// `native = true`, and its containerized offload to `us-west-002` is the
    /// leg proven live on 2026-07-11. A rule keyed on `native` alone would have
    /// silently converted it to a fork+exec on the build-worker.
    #[test]
    fn remote_runtime_goes_native_only_for_a_target_os_no_container_can_host() {
        use crate::platform::PlatformSpec;

        let remote = |step: crate::types::QedStep| {
            let pipeline = bg_pipeline("rel", vec![step]);
            PipelineRunner {
                run_where: RunWhere::Remote,
                ..PipelineRunner::new(pipeline).with_host_triple("aarch64-unknown-linux-gnu")
            }
        };

        // Darwin target: no container on any build-worker can host it.
        let darwin = remote(darwin_native_step());
        assert_eq!(
            darwin.resolve_runtime(&darwin.pipeline.steps[0]),
            TaskRuntime::Native,
        );

        // Linux target, also native = true: stays containerized.
        let linux = remote(native_offload_step());
        assert_eq!(
            linux.resolve_runtime(&linux.pipeline.steps[0]),
            TaskRuntime::Container,
            "the proven us-west-002 offload leg must not change shape",
        );

        // Darwin target WITHOUT native = true: never offloaded for OS reasons in
        // the first place (resolve_placement exempts it), so no native runtime.
        let mut not_native = darwin_native_step();
        not_native.platform.as_mut().unwrap().native = false;
        let nn = remote(not_native);
        assert_eq!(
            nn.resolve_runtime(&nn.pipeline.steps[0]),
            TaskRuntime::Container,
        );

        // A declared container_platform opts back out: the step is asking for a
        // container image, which brings its own userland.
        let mut with_container = darwin_native_step();
        with_container.platform.as_mut().unwrap().container_platform = Some("linux/arm64".into());
        let wc = remote(with_container);
        assert_eq!(
            wc.resolve_runtime(&wc.pipeline.steps[0]),
            TaskRuntime::Container,
        );

        // Unrecognized OS token: fail closed to Container rather than guess.
        let mut unknown = darwin_native_step();
        unknown.platform = Some(PlatformSpec {
            target: Some("aarch64-unknown-none".into()),
            container_platform: None,
            native: true,
        });
        let unk = remote(unknown);
        assert_eq!(
            unk.resolve_runtime(&unk.pipeline.steps[0]),
            TaskRuntime::Container,
        );

        // An explicit `runtime` in the recipe still wins over the default.
        let mut forced = darwin_native_step();
        forced.runtime = Some(TaskRuntime::Container);
        let f = remote(forced);
        assert_eq!(
            f.resolve_runtime(&f.pipeline.steps[0]),
            TaskRuntime::Container,
        );

        // Local placement is unaffected — it was already Native.
        let local = PipelineRunner::new(bg_pipeline("rel", vec![darwin_native_step()]))
            .with_host_triple("aarch64-apple-darwin");
        assert_eq!(
            local.resolve_runtime(&local.pipeline.steps[0]),
            TaskRuntime::Native,
        );
    }

    /// A step whose foreign-arch container forces the QEMU emulate branch: a
    /// host-arch (here: absent) target with a foreign `container_platform`.
    fn emulate_step() -> crate::types::QedStep {
        use crate::platform::PlatformSpec;
        let mut s = mk_step("build-img", &["docker", "buildx", "build", "."]);
        s.platform = Some(PlatformSpec {
            target: None,
            container_platform: Some("linux/arm64".into()),
            native: false,
        });
        s
    }

    // ── R560/W236 QEMU emulation gate ────────────────────────────────────────

    /// On an x86 host, a step pulling a linux/arm64 container resolves to
    /// Emulate — and the gate refuses to start, naming the step and both the
    /// fix (`native = true`) and the opt-in (`--allow-emulate`).
    #[test]
    fn emulation_gate_refuses_unless_opted_in() {
        let pipeline = bg_pipeline("img", vec![emulate_step()]);
        let runner = PipelineRunner::new(pipeline).with_host_triple("x86_64-unknown-linux-gnu");
        assert_eq!(
            runner.emulating_steps(),
            vec![("build-img".to_string(), "linux/arm64".to_string())],
        );
        let err = runner.emulation_gate().unwrap_err();
        let RunnerError::InvalidConfig(msg) = err else {
            panic!("expected InvalidConfig, got {err:?}");
        };
        assert!(msg.contains("build-img"), "names the step: {msg}");
        assert!(msg.contains("native = true"), "offers the fix: {msg}");
        assert!(msg.contains("--allow-emulate"), "offers the opt-in: {msg}");
    }

    /// `--allow-emulate` (via `with_allow_emulate`) lets the same pipeline past
    /// the gate — the "are you sure?" confirmation.
    #[test]
    fn emulation_gate_allows_when_opted_in() {
        let pipeline = bg_pipeline("img", vec![emulate_step()]);
        let runner = PipelineRunner::new(pipeline)
            .with_host_triple("x86_64-unknown-linux-gnu")
            .with_allow_emulate(true);
        assert!(runner.emulation_gate().is_ok());
    }

    /// A native-offload pipeline (the rusty-v8-musl shape) never trips the gate:
    /// `native = true` forces past Emulate to Offload, so there's nothing to
    /// confirm — and a plain host-native pipeline is likewise untouched.
    #[test]
    fn emulation_gate_ignores_non_emulating_pipelines() {
        let native = PipelineRunner::new(bg_pipeline("v8", vec![native_offload_step()]))
            .with_host_triple("aarch64-apple-darwin");
        assert!(native.emulating_steps().is_empty());
        assert!(native.emulation_gate().is_ok());

        let plain = PipelineRunner::new(bg_pipeline("plain", vec![mk_step("s", &["true"])]))
            .with_host_triple("x86_64-unknown-linux-gnu");
        assert!(plain.emulation_gate().is_ok());
    }

    // ── R555-F11 fleet-portability gate (W235 §3c) ───────────────────────────

    /// A forced-`--where=remote` runner over one step, on the arm64 camp Mac.
    fn forced_remote(step: crate::types::QedStep) -> PipelineRunner {
        let pipeline = bg_pipeline("rel", vec![step]);
        PipelineRunner {
            run_where: RunWhere::Remote,
            ..PipelineRunner::new(pipeline).with_host_triple("aarch64-apple-darwin")
        }
    }

    /// The exact W235 §2 failure this gate exists to convert into a first-second
    /// refusal: `--where=remote` at an ordinary camp recipe whose argv reads the
    /// tree. Before the gate the step dispatched, the container started, and the
    /// script was not there.
    #[test]
    fn fleet_portability_gate_refuses_a_non_portable_forced_remote_step() {
        let runner = forced_remote(mk_step("check", &["./scripts/check-workspace-members.sh"]));
        let err = runner.fleet_portability_gate().unwrap_err();
        let RunnerError::InvalidConfig(msg) = err else {
            panic!("expected InvalidConfig, got {err:?}");
        };
        assert!(msg.contains("`check`"), "names the step: {msg}");
        assert!(msg.contains("source_context"), "names the remedy: {msg}");
        // Decision 3 (leader): a reason that says "add source_context" without
        // saying what travels with it costs the reader an hour someone already
        // paid for.
        assert!(msg.contains("GIT-TRACKED"), "names what travels: {msg}");
        assert!(msg.contains("--no-same-owner"), "names what travels: {msg}");
        assert!(msg.contains("mesofact-musl"), "names the worked example: {msg}");
        assert!(msg.contains("--force"), "names the bypass: {msg}");
    }

    /// `--force` bypasses, exactly like the W155 environment gate's own force.
    #[test]
    fn fleet_portability_gate_is_bypassed_by_force() {
        let runner = forced_remote(mk_step("check", &["./scripts/check.sh"])).with_force(true);
        assert!(runner.fleet_portability_gate().is_ok());
    }

    /// A local run of the same non-portable recipe is untouched — the gate asks
    /// about steps this run would actually DISPATCH, not about every step that
    /// could theoretically be dispatched. Nothing about the default path moves.
    #[test]
    fn fleet_portability_gate_ignores_steps_that_stay_local() {
        let runner = PipelineRunner::new(bg_pipeline(
            "check",
            vec![mk_step("check", &["./scripts/check.sh"])],
        ))
        .with_host_triple("aarch64-apple-darwin");
        assert!(runner.fleet_portability_gate().is_ok());
        // …but the preflight still ANSWERS the question, which is the half the
        // operator actually lacked.
        let line = &runner.portability_preflight()[0];
        assert!(line.contains("fleet = NOT portable"), "{line}");
    }

    /// The `mesofact-musl` shape, and the case the whole chain has to keep
    /// working end to end: `source_context`, produced under `/yah/produced`, a
    /// pinned image. Forced remote, it passes.
    #[test]
    fn the_mesofact_musl_shape_passes_the_gate_and_reads_portable() {
        let mut step = mk_step(
            "build-mesofact-x86_64-musl",
            &["set -eu\ncurl -fsSL \"$YAH_SOURCE_CONTEXT_URL\" -o /tmp/src.tar.gz\ntar --no-same-owner -xzf /tmp/src.tar.gz -C /work\ncd /work/oss/mesofact\ncargo build --release"],
        );
        step.source_context = vec![std::path::PathBuf::from("oss/mesofact")];
        step.image = Some("cr.yah.dev/mesofact-musl-builder:v149.4.0-rust1.97-amd64@sha256:0f87".into());
        step.produces = vec![crate::types::ProducedArtifact {
            binary: "mesofact".into(),
            path: "/yah/produced/mesofact-x86_64-unknown-linux-musl.tar.gz".into(),
            triple: Some("x86_64-unknown-linux-musl".into()),
        }];
        let runner = forced_remote(step);
        assert!(runner.fleet_portability_gate().is_ok());
        assert!(runner.portability_preflight()[0].contains("fleet = portable"));
    }

    /// R555-B10 INTERACTION — the most likely way to get this wrong.
    ///
    /// B10 made a missing `cargo-zigbuild` DEMOTE a NativeCross step to Offload
    /// instead of hard-failing. So a step can arrive at `Remote` because a tool
    /// is missing locally, not because anyone asked for the fleet. Refusing it
    /// with a bare portability error would answer a missing-toolchain question
    /// with a recipe question — the exact confusing-error trade B10 undid. The
    /// message must name BOTH facts and BOTH remedies.
    #[test]
    fn a_capability_demoted_step_names_both_the_missing_tool_and_the_portability_gap() {
        use crate::platform::PlatformSpec;
        // A NativeCross derivation (cross-arch musl, `native` NOT set) on a host
        // with no cross toolchain: B10 demotes it to Offload.
        let mut step = mk_step("build", &["cargo", "build", "--release"]);
        step.platform = Some(PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: None,
            native: false,
        });
        // `--where=auto` — the default, and the only mode a demotion can route
        // through. Under a forced `--where=local` the step stays here and B10's
        // own `native_cross_plan` install-hint error is the right diagnosis.
        let runner = PipelineRunner {
            run_where: RunWhere::Auto,
            ..PipelineRunner::new(bg_pipeline("build", vec![step]))
                .with_host_triple("aarch64-apple-darwin")
                .with_cross_availability(crate::nativecross::ToolAvailability::NONE)
        };

        // Precondition: it really is a demotion, not a declared offload.
        assert_eq!(
            runner.derive_step(&runner.pipeline.steps[0]),
            crate::platform::Resolution::NativeCross,
        );
        assert!(matches!(
            runner.resolve_step(&runner.pipeline.steps[0]),
            crate::platform::Resolution::Offload { .. }
        ));
        assert_eq!(runner.capability_demotions().len(), 1);

        let err = runner.fleet_portability_gate().unwrap_err();
        let RunnerError::InvalidConfig(msg) = err else {
            panic!("expected InvalidConfig, got {err:?}");
        };
        // Fact + remedy #1: the missing toolchain, with its install hint.
        assert!(msg.contains("cargo-zigbuild"), "names the missing tool: {msg}");
        assert!(msg.contains("cargo install cargo-zigbuild"), "install hint: {msg}");
        // Fact + remedy #2: the portability gap.
        assert!(msg.contains("source_context"), "names the portability gap: {msg}");
        // And says plainly that neither place works.
        assert!(msg.contains("NEITHER"), "names both at once: {msg}");
    }

    /// `rusty-v8-musl`'s remote step is genuinely self-sufficient — image +
    /// argv, no camp tree. It is one of only two pipelines that actually
    /// offloads today, so a gate keyed on "declares `source_context`" would
    /// break a working fleet pipeline. It must pass.
    #[test]
    fn a_self_sufficient_offload_step_is_not_refused() {
        let mut step = native_offload_step();
        step.argv = vec![
            "build-v8.sh 'x86_64-unknown-linux-musl' '/yah/produced/rusty-v8-x86_64-unknown-linux-musl.tar.gz'"
                .to_string(),
        ];
        step.image = Some("cr.yah.dev/rusty-v8-musl-builder:v149.4.0-amd64@sha256:7e9f".into());
        step.produces = vec![crate::types::ProducedArtifact {
            binary: "rusty-v8".into(),
            path: "/yah/produced/rusty-v8-x86_64-unknown-linux-musl.tar.gz".into(),
            triple: Some("x86_64-unknown-linux-musl".into()),
        }];
        // `--where=auto` on the arm64 camp Mac: `native = true` + cross arch ⇒
        // this really is dispatched to the fleet.
        let runner = PipelineRunner {
            run_where: RunWhere::Auto,
            ..PipelineRunner::new(bg_pipeline("v8", vec![step]))
                .with_host_triple("aarch64-apple-darwin")
        };
        let report = runner.fleet_portability_report();
        assert!(report[0].dispatched_to_fleet, "{report:?}");
        assert!(runner.fleet_portability_gate().is_ok());
    }

    // ── R590-F4 policy routing ───────────────────────────────────────────────

    /// `policy_placement` folds the `--where` force-mode with a step's
    /// resolution: force-modes pass through, and Auto routes only Offload
    /// verdicts to the fleet.
    #[test]
    fn policy_placement_forces_and_derives() {
        use crate::platform::Resolution;
        let offload = Resolution::Offload {
            target: "x86_64-unknown-linux-musl".into(),
        };
        // Force-modes ignore the resolution entirely.
        assert_eq!(policy_placement(RunWhere::Local, &offload), RunWhere::Local);
        assert_eq!(
            policy_placement(RunWhere::Remote, &Resolution::NativeCross),
            RunWhere::Remote
        );
        // Auto derives: Offload → Remote, everything else → Local.
        assert_eq!(policy_placement(RunWhere::Auto, &offload), RunWhere::Remote);
        assert_eq!(
            policy_placement(RunWhere::Auto, &Resolution::NativeCross),
            RunWhere::Local
        );
        assert_eq!(
            policy_placement(
                RunWhere::Auto,
                &Resolution::Emulate {
                    docker_platform: "linux/amd64".into()
                }
            ),
            RunWhere::Local
        );
    }

    /// On an arm64 host, the default (Auto) runner routes a `native = true`
    /// x86 musl step to the fleet — no `--where=remote` — while an ordinary
    /// cross-compilable step stays local. A forced `--where=local` runner keeps
    /// even the native step local (the testing override).
    #[test]
    fn effective_placement_routes_native_offload_under_auto() {
        let pipeline = bg_pipeline("v8", vec![native_offload_step()]);
        let auto = PipelineRunner::new(pipeline.clone())
            .with_host_triple("aarch64-apple-darwin");
        // new()/new_with_dispatcher default to Local; flip to Auto to model the
        // default CLI mode without standing up a real dispatcher.
        let auto = PipelineRunner {
            run_where: RunWhere::Auto,
            ..auto
        };
        let step = &auto.pipeline.steps[0];
        assert_eq!(auto.effective_placement(step), RunWhere::Remote);
        // Runtime for an offloaded step defaults to Container.
        assert_eq!(auto.resolve_runtime(step), TaskRuntime::Container);

        // An ordinary cross step (native=false) stays local under Auto.
        let mut plain = native_offload_step();
        plain.platform.as_mut().unwrap().native = false;
        let plain_pipeline = bg_pipeline("plain", vec![plain]);
        let auto_plain = PipelineRunner {
            run_where: RunWhere::Auto,
            ..PipelineRunner::new(plain_pipeline).with_host_triple("aarch64-apple-darwin")
        };
        assert_eq!(
            auto_plain.effective_placement(&auto_plain.pipeline.steps[0]),
            RunWhere::Local
        );

        // Force-local keeps the native step local.
        let forced = PipelineRunner::new(pipeline).with_host_triple("aarch64-apple-darwin");
        assert_eq!(
            forced.effective_placement(&forced.pipeline.steps[0]),
            RunWhere::Local
        );
    }

    /// On the x86 build-worker itself the native x86 step is host-arch → it runs
    /// locally, never re-dispatched (the offload target *is* this host).
    #[test]
    fn effective_placement_native_step_runs_local_on_matching_host() {
        let pipeline = bg_pipeline("v8", vec![native_offload_step()]);
        let auto = PipelineRunner {
            run_where: RunWhere::Auto,
            ..PipelineRunner::new(pipeline).with_host_triple("x86_64-unknown-linux-gnu")
        };
        assert_eq!(
            auto.effective_placement(&auto.pipeline.steps[0]),
            RunWhere::Local
        );
    }

    /// `pipeline_needs_offload` tells the CLI whether an Auto run must stand up a
    /// mesh dispatcher: true when any native cross step offloads on this host,
    /// false for an all-cross-compilable pipeline.
    #[test]
    fn pipeline_needs_offload_detects_native_cross_step() {
        let with_native = bg_pipeline("v8", vec![native_offload_step()]);
        assert!(pipeline_needs_offload(
            &with_native,
            "aarch64-apple-darwin",
            ALL_TOOLS,
            &NoopSubPipelineResolver
        ));
        // Same step on the matching host: host-arch build, no offload.
        assert!(!pipeline_needs_offload(
            &with_native,
            "x86_64-unknown-linux-gnu",
            ALL_TOOLS,
            &NoopSubPipelineResolver
        ));

        let mut plain = native_offload_step();
        plain.platform.as_mut().unwrap().native = false;
        let no_native = bg_pipeline("plain", vec![plain]);
        assert!(!pipeline_needs_offload(
            &no_native,
            "aarch64-apple-darwin",
            ALL_TOOLS,
            &NoopSubPipelineResolver
        ));
    }

    /// R555-B15: the offload need lives one level down, inside a
    /// `kind = "sub-pipeline"` child. The parent step declares no `platform`,
    /// so it resolves `NativeCross` and a steps-only scan answers `false` —
    /// which is how `yah-release-wizard` reached its `publish-cli` leg with no
    /// fleet client wired and died on "no remote dispatcher is wired", from a
    /// run that was already `--where=auto`, while `yah-cli-release` run
    /// directly worked.
    #[test]
    fn pipeline_needs_offload_descends_into_sub_pipelines() {
        struct OneChild(Pipeline);
        impl crate::types::SubPipelineResolver for OneChild {
            fn resolve(&self, _target: &crate::types::SubPipelineRef) -> Option<Pipeline> {
                Some(self.0.clone())
            }
        }

        let child = bg_pipeline("cli-release", vec![native_offload_step()]);

        let mut gate = native_offload_step();
        gate.name = "publish-cli".into();
        gate.kind = crate::types::StepKind::SubPipeline;
        gate.argv = Vec::new();
        // The shape that hid the need: the sub-pipeline step has no platform.
        gate.platform = None;
        gate.sub_pipeline = Some(crate::types::SubPipelineConfig {
            target: crate::types::SubPipelineRef::Builtin("cli-release".into()),
            params: HashMap::new(),
            propagate: crate::types::SubPipelineCollect {
                produces: false,
                outputs: Vec::new(),
            },
            opaque: false,
            own_workspace: None,
        });
        let parent = bg_pipeline("release-wizard", vec![gate]);

        assert!(
            pipeline_needs_offload(
                &parent,
                "aarch64-apple-darwin",
                ALL_TOOLS,
                &OneChild(child.clone())
            ),
            "a resolvable child with a native cross step must make the parent need the fleet"
        );

        // A resolver that resolves nothing sees nothing — the old answer, and
        // why NoopSubPipelineResolver is not a safe default for a pipeline that
        // may have children.
        assert!(!pipeline_needs_offload(
            &parent,
            "aarch64-apple-darwin",
            ALL_TOOLS,
            &NoopSubPipelineResolver
        ));

        // On the x86 worker the child's step is host-arch: no offload either way.
        assert!(!pipeline_needs_offload(
            &parent,
            "x86_64-unknown-linux-gnu",
            ALL_TOOLS,
            &OneChild(child)
        ));
    }

    /// R823-T3, measured on hardware: a participant set's steps declare no
    /// `platform`, so the per-step resolution above says `NativeCross` for every
    /// one of them and `pipeline_needs_offload` used to answer `false` — the CLI
    /// then ran a rendezvous with no dispatcher and died on "no remote
    /// dispatcher is wired". A node-bound role is a fleet dependency the step's
    /// target triple cannot express.
    #[test]
    fn a_node_bound_participant_needs_fleet_wiring_on_any_host() {
        let mut step = shell_step("responder", vec!["true"]);
        step.participant = Some("responder".into());
        let mut pipeline = bg_pipeline("rendezvous", vec![step]);
        pipeline.participants = Some(participant_set(
            r#"
            [role.responder]
            node    = "us-west-003"
            address = "100.64.0.9"
            ports   = ["echo"]

            [role.runner]
            coordinator = true
        "#,
        ));

        assert!(pipeline_has_node_bound_participant(&pipeline));
        // True on EITHER host: the binding names a box, so there is no host this
        // could resolve to a local run on — unlike a cross-arch build, which
        // stops needing the fleet the moment you run it on the matching arch.
        assert!(pipeline_needs_offload(
            &pipeline,
            "aarch64-apple-darwin",
            ALL_TOOLS,
            &NoopSubPipelineResolver
        ));
        assert!(pipeline_needs_offload(
            &pipeline,
            "x86_64-unknown-linux-gnu",
            ALL_TOOLS,
            &NoopSubPipelineResolver
        ));

        // A set whose every role is local still needs nothing: those steps run
        // as local subprocesses, rendezvous env and all.
        let mut local_only = pipeline.clone();
        local_only.participants = Some(participant_set(
            r#"
            [role.runner]
            coordinator = true
            ports = ["control"]
        "#,
        ));
        assert!(!pipeline_has_node_bound_participant(&local_only));
        assert!(!pipeline_needs_offload(
            &local_only,
            "aarch64-apple-darwin",
            ALL_TOOLS,
            &NoopSubPipelineResolver
        ));
    }

    /// R719-F3: the dual of the above. `needs_offload` answers "do I need fleet
    /// wiring"; `is_fully_offloaded` answers "does any of this land on THIS
    /// box", which is what decides whether the run should hold a local lane.
    #[test]
    fn pipeline_is_fully_offloaded_separates_all_from_any() {
        let all_remote = bg_pipeline("v8", vec![native_offload_step()]);
        assert!(pipeline_is_fully_offloaded(
            &all_remote,
            "aarch64-apple-darwin",
            ALL_TOOLS
        ));

        // One local step is enough to keep the run in a local lane — the
        // ticket's gotcha: not every "remote" pipeline is free of local work.
        let mut local = native_offload_step();
        local.platform.as_mut().unwrap().native = false;
        let mixed = bg_pipeline("mixed", vec![native_offload_step(), local]);
        assert!(pipeline_needs_offload(
            &mixed,
            "aarch64-apple-darwin",
            ALL_TOOLS,
            &NoopSubPipelineResolver
        ));
        assert!(
            !pipeline_is_fully_offloaded(&mixed, "aarch64-apple-darwin", ALL_TOOLS),
            "a mixed run still competes for local resources"
        );

        // Same steps on the matching host: nothing offloads at all.
        assert!(!pipeline_is_fully_offloaded(
            &all_remote,
            "x86_64-unknown-linux-gnu",
            ALL_TOOLS
        ));
    }

    /// `all()` over an empty iterator is vacuously true, which would hand a
    /// no-op pipeline the fleet lane on a technicality.
    #[test]
    fn an_empty_pipeline_is_not_fully_offloaded() {
        let empty = bg_pipeline("nothing", vec![]);
        assert!(!pipeline_is_fully_offloaded(
            &empty,
            "aarch64-apple-darwin",
            ALL_TOOLS
        ));
    }

    // ── R555-B10: Capability feeds the PRE-RUNNER question too ───────────────

    /// The half of R555-B10 that makes it more than a one-line change (W235 §6).
    /// `needs_fleet` is computed from pipeline + host *before* a runner exists;
    /// if it cannot see a per-step Capability demotion, the run takes the
    /// driverless local path and the demoted step dies on "no remote dispatcher
    /// is wired" — strictly worse than the "install cargo-zigbuild" hard-fail
    /// this ticket replaced.
    #[test]
    fn pipeline_needs_offload_sees_a_capability_demotion() {
        const MAC: &str = "aarch64-apple-darwin";
        const NO_TOOLS: &crate::nativecross::ToolAvailability =
            &crate::nativecross::ToolAvailability::NONE;

        // A plain cross-compiled step: `native = false`, foreign-arch crossable
        // target — the `mesofact-build` / `release-build` shape.
        let mut cross = native_offload_step();
        cross.platform.as_mut().unwrap().native = false;
        let pipeline = bg_pipeline("cross-build", vec![cross]);

        // Fully provisioned: it cross-compiles right here, no fleet wiring at all.
        assert!(!pipeline_needs_offload(&pipeline, MAC, ALL_TOOLS, &NoopSubPipelineResolver));
        assert!(!pipeline_is_fully_offloaded(&pipeline, MAC, ALL_TOOLS));
        assert!(pipeline_capability_demotions(&pipeline, MAC, ALL_TOOLS).is_empty());

        // Under-provisioned: the SAME pipeline now needs a dispatcher, and the
        // up-front question is the one that has to say so.
        assert!(pipeline_needs_offload(&pipeline, MAC, NO_TOOLS, &NoopSubPipelineResolver));
        assert!(pipeline_is_fully_offloaded(&pipeline, MAC, NO_TOOLS));

        // …and it is never silent: the demotion names the step and the tool.
        let demotions = pipeline_capability_demotions(&pipeline, MAC, NO_TOOLS);
        assert_eq!(demotions.len(), 1);
        assert_eq!(demotions[0].0, "build-v8");
        assert!(
            demotions[0].1.to_string().contains("cargo-zigbuild"),
            "the warning must name the missing tool, got: {}",
            demotions[0].1
        );
    }

    /// The routing half: an `Auto` runner on an under-provisioned host sends the
    /// demoted step to the fleet, while its *derivation* is still NativeCross —
    /// that separation is what keeps the forced-local path (next test) honest.
    #[test]
    fn an_under_provisioned_auto_runner_routes_the_demoted_step_remote() {
        let mut cross = native_offload_step();
        cross.platform.as_mut().unwrap().native = false;
        let pipeline = bg_pipeline("cross-build", vec![cross]);
        let auto = PipelineRunner {
            run_where: RunWhere::Auto,
            ..PipelineRunner::new(pipeline)
                .with_host_triple("aarch64-apple-darwin")
                .with_cross_availability(crate::nativecross::ToolAvailability::NONE)
        };
        let step = &auto.pipeline.steps[0];

        assert_eq!(
            auto.derive_step(step),
            crate::platform::Resolution::NativeCross,
            "derivation is toolchain-blind — physics still allows the cross-compile"
        );
        assert_eq!(
            auto.resolve_step(step),
            crate::platform::Resolution::Offload {
                target: "x86_64-unknown-linux-musl".into()
            },
            "placement folds capability in and demotes"
        );
        assert_eq!(auto.effective_placement(step), RunWhere::Remote);
        assert_eq!(auto.capability_demotions().len(), 1);

        // The same runner with the toolchain present keeps the step at home.
        let provisioned = PipelineRunner {
            run_where: RunWhere::Auto,
            ..PipelineRunner::new(bg_pipeline("cross-build", vec![auto.pipeline.steps[0].clone()]))
                .with_host_triple("aarch64-apple-darwin")
                .with_cross_availability(crate::nativecross::ToolAvailability::FULL)
        };
        assert_eq!(
            provisioned.effective_placement(&provisioned.pipeline.steps[0]),
            RunWhere::Local
        );
        assert!(provisioned.capability_demotions().is_empty());
    }

    /// R555-B10 regression guard: `--where local` is the operator saying "not the
    /// fleet", so the demotion must not swallow the diagnosis. A forced-local
    /// runner still routes the step through the NativeCross tier — which fails
    /// with `install_hint()` — rather than shelling out the recipe's raw
    /// `cross build` argv to a linker that cannot serve it. This is why
    /// `native_cross_plan` gates on `derive_step`, not `resolve_step`.
    #[test]
    fn a_forced_local_runner_still_fails_with_the_install_hint() {
        let mut cross = native_offload_step();
        cross.platform.as_mut().unwrap().native = false;
        let pipeline = bg_pipeline("cross-build", vec![cross]);
        let local = PipelineRunner::new(pipeline)
            .with_host_triple("aarch64-apple-darwin")
            .with_cross_availability(crate::nativecross::ToolAvailability::NONE);
        let step = &local.pipeline.steps[0];

        assert_eq!(local.effective_placement(step), RunWhere::Local);
        let err = local
            .native_cross_plan(step, &crate::nativecross::ToolAvailability::NONE)
            .expect("a NativeCross derivation still reaches the tier when forced local")
            .expect_err("with no toolchain installed the plan must fail, not silently pass");
        assert_eq!(err.target, "x86_64-unknown-linux-musl");
        assert!(err.to_string().contains("cargo install cargo-zigbuild"));
    }

    /// An Auto runner that policy-routes a step to Offload but has no dispatcher
    /// wired fails with a clear config error instead of panicking.
    ///
    /// R555-F6 also reads the second half of the outcome here: `forge_id` is
    /// `None`, which is how the caller knows there is nothing to reap. A
    /// dispatch that never happened and a dispatch that happened and failed are
    /// the two states this return type exists to keep apart.
    #[tokio::test]
    async fn offload_without_dispatcher_errors_cleanly() {
        let pipeline = bg_pipeline("v8", vec![native_offload_step()]);
        let auto = PipelineRunner {
            run_where: RunWhere::Auto,
            ..PipelineRunner::new(pipeline).with_host_triple("aarch64-apple-darwin")
        };
        let step = auto.pipeline.steps[0].clone();
        let outcome = auto.execute_step_remote(0, &step, TaskRuntime::Container).await;
        assert!(
            outcome.forge_id.is_none(),
            "nothing was deployed, so nothing may be named as reapable",
        );
        match outcome.result.expect_err("no dispatcher wired must error, not panic") {
            RunnerError::InvalidConfig(m) => {
                assert!(m.contains("Offload"), "message: {m}");
                assert!(m.contains("no remote dispatcher"), "message: {m}");
            }
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    /// A forced `--where=local` runner keeps a `native = true` cross-arch step
    /// Local (effective_placement never inspects the step), so it would reach
    /// the local-container path. That path must REFUSE rather than let Docker
    /// silently emulate the foreign-arch image under QEMU — the "fail not a
    /// warning" contract for the rusty-v8-musl forcing case. No docker daemon
    /// is touched: the guard fires before any container is started.
    #[tokio::test]
    async fn native_offload_step_refuses_local_container_emulation() {
        let pipeline = bg_pipeline("v8", vec![native_offload_step()]);
        // Default runner is forced-Local; arm64 host + x86 native target ⇒ the
        // step resolves to Offload, so local-container execution == emulation.
        let forced = PipelineRunner::new(pipeline).with_host_triple("aarch64-apple-darwin");
        let step = forced.pipeline.steps[0].clone();
        assert_eq!(forced.effective_placement(&step), RunWhere::Local);

        let err = forced
            .execute_step_local_container(0, &step)
            .await
            .expect_err("a native cross-arch step must not emulate locally");
        match err {
            RunnerError::StepFailed { step: s, msg } => {
                assert_eq!(s, "build-v8");
                assert!(msg.contains("must offload"), "message: {msg}");
                assert!(msg.contains("arch:x86"), "message: {msg}");
                assert!(msg.contains("aarch64-apple-darwin"), "message: {msg}");
            }
            other => panic!("expected StepFailed, got {other:?}"),
        }
    }

    // ── R633: build-image placement + platforms ──────────────────────────────

    /// `QedStep::default()` must agree with what serde produces for a step that
    /// declares nothing. The field that bites is `enabled`: a *derived* Default
    /// makes it `false`, so every `..Default::default()` call site would build a
    /// step the runner skips — and a pipeline of skipped steps reports Success,
    /// which is a green light over work that never ran.
    #[test]
    fn qed_step_default_matches_serde_defaults() {
        let d = QedStep::default();
        assert!(
            d.enabled,
            "a default step must be enabled, or `..Default::default()` silently builds a no-op"
        );
        assert_eq!(d.activation, crate::types::StepActivation::Active);
        assert_eq!(d.kind, crate::types::StepKind::Subprocess);
        assert!(d.platforms.is_empty());

        let from_toml: QedStep =
            toml::from_str(r#"name = "x""#).expect("a bare step parses on serde defaults");
        assert_eq!(from_toml.enabled, d.enabled);
        assert_eq!(from_toml.activation, d.activation);
        assert_eq!(from_toml.kind, d.kind);
    }

    /// A `build-image` step for `img`, targeting one docker platform. `native`
    /// mirrors what `yah qed images build` synthesizes for a foreign-arch
    /// platform.
    fn build_image_step(img: &str, platform: &str, target: &str, native: bool) -> QedStep {
        use crate::platform::PlatformSpec;
        QedStep {
            name: format!("build-{img}"),
            kind: crate::types::StepKind::BuildImage,
            image: Some(img.to_string()),
            tag: Some(format!("cr.yah.dev/{img}:dev")),
            platforms: vec![platform.to_string()],
            platform: Some(PlatformSpec {
                target: Some(target.to_string()),
                container_platform: Some(platform.to_string()),
                native,
            }),
            ..Default::default()
        }
    }

    /// The R633 routing fix: under the default `Auto`, a `native = true`
    /// cross-arch build-image step must resolve to Remote (offload to an
    /// arch-matched build-worker) exactly like a subprocess step does.
    ///
    /// Before the fix `execute_step_build_image` read `self.run_where`, which is
    /// `Auto` here and so fell through to the LOCAL docker path — building the
    /// foreign image on the qed host, or emulating it.
    #[test]
    fn build_image_step_offloads_under_auto() {
        let step = build_image_step(
            "rusty-v8-musl-builder",
            "linux/amd64",
            "x86_64-unknown-linux-musl",
            true,
        );
        let pipeline = bg_pipeline("images", vec![step.clone()]);
        let auto = PipelineRunner::new(pipeline).with_host_triple("aarch64-apple-darwin");
        let auto = PipelineRunner {
            run_where: RunWhere::Auto,
            ..auto
        };
        assert_eq!(auto.effective_placement(&step), RunWhere::Remote);

        // The same step on a matching host is a plain local build — no fleet.
        let native_host = PipelineRunner {
            run_where: RunWhere::Auto,
            ..PipelineRunner::new(bg_pipeline("images", vec![step.clone()]))
                .with_host_triple("x86_64-unknown-linux-gnu")
        };
        assert_eq!(native_host.effective_placement(&step), RunWhere::Local);
    }

    /// R636: a remote build-image step must route to a worker of the step's
    /// TARGET arch, not the runner's host arch. An amd64 image build offloaded
    /// from an arm64 Mac has to land on `arch:x86`; deriving the tier from the
    /// host sent it to a `arch:arm` node that failed on an unreachable URL.
    #[test]
    fn remote_build_image_routes_by_target_arch_not_host() {
        let amd64 = build_image_step(
            "rusty-v8-musl-builder",
            "linux/amd64",
            "x86_64-unknown-linux-musl",
            true,
        );
        let runner = PipelineRunner {
            run_where: RunWhere::Auto,
            ..PipelineRunner::new(bg_pipeline("images", vec![amd64.clone()]))
                .with_host_triple("aarch64-apple-darwin")
        };
        // The forcing case: host is arm64, target is x86 → must pick x86.
        assert_eq!(runner.remote_build_image_arch(&amd64), "x86_64");
        assert_eq!(
            crate::platform::build_worker_mesh_tags(&runner.remote_build_image_arch(&amd64), "linux"),
            vec![
                "tag:build-worker".to_string(),
                "arch:x86".to_string(),
                "os:linux".to_string()
            ]
        );

        // A host-native build-image forced remote has no cross target → host arch.
        let host_native = build_image_step(
            "yah-rust",
            "linux/arm64",
            "aarch64-unknown-linux-musl",
            false,
        );
        let remote = PipelineRunner {
            run_where: RunWhere::Remote,
            ..PipelineRunner::new(bg_pipeline("images", vec![host_native.clone()]))
                .with_host_triple("aarch64-apple-darwin")
        };
        assert_eq!(remote.remote_build_image_arch(&host_native), "aarch64");
    }

    // ── R636-B1 cross-host build context ────────────────────────────────────

    /// Records what the runner asked to be uploaded, so a test can assert on
    /// the actual tar rather than on a call count.
    #[derive(Default)]
    struct RecordingContextPublisher {
        published: std::sync::Mutex<Vec<(String, Vec<u8>)>>,
        discarded: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl crate::build_context::BuildContextPublisher for RecordingContextPublisher {
        async fn publish(&self, key: &str, tarball: Vec<u8>) -> Result<String, RunnerError> {
            self.published
                .lock()
                .unwrap()
                .push((key.to_string(), tarball));
            Ok(format!("https://ctx.test/{key}.tar.gz"))
        }
        async fn discard(&self, key: &str) {
            self.discarded.lock().unwrap().push(key.to_string());
        }
    }

    /// Build a camp root with a catalog entry whose Dockerfile sits beside a
    /// file it COPYs — the rusty-v8-musl-builder shape in miniature.
    fn camp_with_catalog_image(name: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let images = dir.path().join(".yah/qed/images");
        let img = images.join(name);
        std::fs::create_dir_all(&img).unwrap();
        std::fs::write(
            images.join(format!("{name}.toml")),
            format!(
                "[image]\nname = \"{name}\"\nbase = \"alpine:edge\"\n\
                 description = \"fixture\"\nproduces = [\"oci-image\"]\n"
            ),
        )
        .unwrap();
        std::fs::write(img.join("Dockerfile"), "FROM alpine:edge\nCOPY build.sh /\n").unwrap();
        std::fs::write(img.join("build.sh"), "#!/bin/sh\necho hi\n").unwrap();
        dir
    }

    /// `prepare_build_image` must resolve the step's `context` the same way for
    /// both dispatch paths. Remote used to ignore it and hardcode the camp root,
    /// which is how an offloaded build ended up shipping (and bind-mounting) a
    /// whole workspace instead of one image directory.
    #[test]
    fn prepared_context_honors_the_steps_context_key() {
        let camp = camp_with_catalog_image("fixture-img");
        let rel = ".yah/qed/images/fixture-img";

        let with_ctx = QedStep {
            context: Some(std::path::PathBuf::from(rel)),
            ..build_image_step("fixture-img", "linux/arm64", "aarch64-unknown-linux-musl", false)
        };
        let runner = PipelineRunner::new(bg_pipeline("images", vec![with_ctx.clone()]))
            .with_camp_root(camp.path().to_path_buf());
        assert_eq!(
            runner.prepare_build_image(&with_ctx).unwrap().context_dir,
            camp.path().join(rel),
        );

        // No `context` key ⇒ camp root, the historical default.
        let bare = build_image_step("fixture-img", "linux/arm64", "aarch64-unknown-linux-musl", false);
        assert_eq!(
            runner.prepare_build_image(&bare).unwrap().context_dir,
            camp.path(),
        );
    }

    /// The uploaded tar must carry BOTH the context's own files and the
    /// Dockerfile qed compiled from the catalog — the compiled one lives in
    /// `.yah/cache/buildkit/`, outside the context, so nothing else would put
    /// it in reach of a worker that only sees the tar.
    #[tokio::test]
    async fn published_context_tar_carries_the_compiled_dockerfile_and_the_context() {
        use std::io::Read;

        let camp = camp_with_catalog_image("fixture-img");
        let step = QedStep {
            context: Some(std::path::PathBuf::from(".yah/qed/images/fixture-img")),
            ..build_image_step("fixture-img", "linux/arm64", "aarch64-unknown-linux-musl", false)
        };
        let recorder = Arc::new(RecordingContextPublisher::default());
        let runner = PipelineRunner::new(bg_pipeline("images", vec![step.clone()]))
            .with_camp_root(camp.path().to_path_buf())
            .with_build_context_publisher(recorder.clone());

        let prepared = runner.prepare_build_image(&step).unwrap();
        let url = runner
            .publish_build_context(&step, &prepared, "k1")
            .await
            .unwrap();
        assert_eq!(url, "https://ctx.test/k1.tar.gz");

        let published = recorder.published.lock().unwrap();
        let (key, tarball) = published.first().expect("one upload");
        assert_eq!(key, "k1");

        let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(tarball.as_slice()));
        let mut seen: Vec<(String, String)> = Vec::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let name = entry.path().unwrap().to_string_lossy().into_owned();
            let mut body = String::new();
            entry.read_to_string(&mut body).unwrap();
            seen.push((name, body));
        }
        let names: Vec<&str> = seen.iter().map(|(n, _)| n.as_str()).collect();
        assert!(
            names.contains(&"build.sh"),
            "the file the Dockerfile COPYs must travel: {names:?}"
        );
        assert!(
            names.contains(&"fixture-img.Dockerfile"),
            "the compiled Dockerfile must be injected at the tar root: {names:?}"
        );
        // Tar root == context root: a nested prefix would break every COPY.
        assert!(
            !names.iter().any(|n| n.contains(".yah/qed/images")),
            "entries must be relative to the context, not the camp: {names:?}"
        );
    }

    /// With no publisher wired, an offloaded build-image step refuses up front
    /// and says what to wire — rather than reinstating the bind-mount shape and
    /// failing inside runc on the worker minutes later.
    #[tokio::test]
    async fn offloaded_build_image_without_a_publisher_refuses_with_instructions() {
        let camp = camp_with_catalog_image("fixture-img");
        let step = QedStep {
            context: Some(std::path::PathBuf::from(".yah/qed/images/fixture-img")),
            ..build_image_step("fixture-img", "linux/arm64", "aarch64-unknown-linux-musl", false)
        };
        let runner = PipelineRunner::new(bg_pipeline("images", vec![step.clone()]))
            .with_camp_root(camp.path().to_path_buf());
        let prepared = runner.prepare_build_image(&step).unwrap();

        let err = runner
            .publish_build_context(&step, &prepared, "k1")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("with_build_context_publisher"),
            "the refusal must name the wiring: {err}"
        );
    }

    /// A foreign `platforms` entry reaching the LOCAL docker path is a hard
    /// error, not a QEMU build. This is the case `--where local` produces:
    /// `effective_placement` returns Local without inspecting the step, so the
    /// refusal has to live at the build-image seam itself.
    #[test]
    fn build_image_refuses_foreign_platform_on_local_daemon() {
        let step = build_image_step(
            "rusty-v8-musl-builder",
            "linux/amd64",
            "x86_64-unknown-linux-musl",
            false,
        );
        let forced = PipelineRunner::new(bg_pipeline("images", vec![step.clone()]))
            .with_host_triple("aarch64-apple-darwin");
        assert_eq!(forced.effective_placement(&step), RunWhere::Local);

        let err = forced
            .refuse_foreign_platform_locally(&step)
            .expect_err("a foreign-platform image build must not emulate locally");
        match err {
            RunnerError::StepFailed { step: s, msg } => {
                assert_eq!(s, "build-rusty-v8-musl-builder");
                assert!(msg.contains("linux/amd64"), "message: {msg}");
                assert!(msg.contains("arch:x86"), "message: {msg}");
                assert!(msg.contains("aarch64-apple-darwin"), "message: {msg}");
            }
            other => panic!("expected StepFailed, got {other:?}"),
        }
    }

    /// The host's own platform is always fine, and an empty `platforms` (every
    /// pre-R633 build-image step) must stay a plain host-native build.
    #[test]
    fn build_image_allows_host_platform_and_empty_platforms() {
        let host_step = build_image_step(
            "yah-rust",
            "linux/arm64",
            "aarch64-unknown-linux-musl",
            false,
        );
        let runner = PipelineRunner::new(bg_pipeline("images", vec![host_step.clone()]))
            .with_host_triple("aarch64-apple-darwin");
        assert!(runner.refuse_foreign_platform_locally(&host_step).is_ok());

        let mut legacy = host_step.clone();
        legacy.platforms.clear();
        assert!(runner.refuse_foreign_platform_locally(&legacy).is_ok());

        // An arch buildx knows but we don't is buildx's to reject, not ours —
        // guessing here would turn a working build into a false blocker.
        let mut exotic = host_step;
        exotic.platforms = vec!["linux/riscv64".to_string()];
        assert!(runner.refuse_foreign_platform_locally(&exotic).is_ok());
    }

    /// A multi-step local pipeline (Live workspace, no outcomes) from the
    /// given steps.
    fn bg_pipeline(name: &str, steps: Vec<crate::types::QedStep>) -> Pipeline {
        let mut p = one_step_pipeline(name, vec!["true".to_string()]);
        p.steps = steps;
        p
    }

    /// Position of the `StepFinished` event for the named step, if any.
    fn finished_pos(events: &[QedEvent], name: &str) -> Option<usize> {
        events.iter().position(
            |e| matches!(e, QedEvent::StepFinished { name: n, .. } if n == name),
        )
    }

    fn drain_events(rx: &mut mpsc::UnboundedReceiver<QedEvent>) -> Vec<QedEvent> {
        let mut events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }
        events
    }

    /// A `background = true` sidecar that never exits on its own is spawned (so
    /// the loop doesn't block on it), runs alongside the foreground step, and is
    /// reaped — killed cleanly, status Success — at the end of the pipeline.
    #[tokio::test]
    async fn background_step_spawns_and_is_reaped_at_pipeline_end() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let server = {
            let mut s = mk_step("server", &["sh", "-c", "sleep 30"]);
            s.background = true;
            s
        };
        let work = mk_step("work", &["sh", "-c", "echo done"]);
        let pipeline = bg_pipeline("bg-end", vec![server, work]);

        let runner = PipelineRunner::new(pipeline).with_events(tx);
        // Completes promptly despite the sidecar's `sleep 30` — proof the loop
        // never awaited it.
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        let server_row = meta.steps.iter().find(|s| s.name == "server").unwrap();
        assert_eq!(
            server_row.status,
            RunStatus::Success,
            "a healthy sidecar killed at teardown is Success, not a failure"
        );
        assert!(server_row.completed_at.is_some());

        let events = drain_events(&mut rx);
        // The sidecar's StepFinished lands after the foreground step's — it was
        // reaped at the end of the loop.
        let server_fin = finished_pos(&events, "server").expect("server finished");
        let work_fin = finished_pos(&events, "work").expect("work finished");
        assert!(
            work_fin < server_fin,
            "background server reaped after foreground work; events={events:?}"
        );
    }

    /// `background_until = "gate"` reaps the sidecar the moment the gate step
    /// finishes — before any later step runs.
    #[tokio::test]
    async fn background_until_reaps_after_named_step() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let server = {
            let mut s = mk_step("server", &["sh", "-c", "sleep 30"]);
            s.background_until = Some("gate".to_string());
            s
        };
        let gate = mk_step("gate", &["sh", "-c", "echo gate"]);
        let after = mk_step("after", &["sh", "-c", "echo after"]);
        let pipeline = bg_pipeline("bg-until", vec![server, gate, after]);

        let meta = PipelineRunner::new(pipeline)
            .with_events(tx)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let events = drain_events(&mut rx);
        let gate_fin = finished_pos(&events, "gate").expect("gate finished");
        let server_fin = finished_pos(&events, "server").expect("server finished");
        let after_fin = finished_pos(&events, "after").expect("after finished");
        assert!(
            gate_fin < server_fin && server_fin < after_fin,
            "server reaped after gate, before after; events={events:?}"
        );
    }

    /// A sidecar that *exits non-zero on its own* before reap is a genuine
    /// failure: its step is Failed and the run flips to Failed (so `on_fail`
    /// fires). The gate step's sleep guarantees the crasher has exited by reap.
    #[tokio::test]
    async fn background_sidecar_crash_fails_the_run() {
        let crasher = {
            let mut s = mk_step("crasher", &["sh", "-c", "exit 7"]);
            s.background_until = Some("gate".to_string());
            s
        };
        let gate = mk_step("gate", &["sh", "-c", "sleep 0.3; echo gate"]);
        let pipeline = bg_pipeline("bg-crash", vec![crasher, gate]);

        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Failed,
            "a sidecar that crashed mid-pipeline flips the run to Failed"
        );
        let crasher_row = meta.steps.iter().find(|s| s.name == "crasher").unwrap();
        assert_eq!(crasher_row.status, RunStatus::Failed);
    }

    /// Pre-flight rejects a `background_until` that names a step at-or-before
    /// the sidecar — the gate would never fire, so fail loudly at run start.
    #[tokio::test]
    async fn background_until_earlier_step_is_rejected() {
        let early = mk_step("early", &["sh", "-c", "echo early"]);
        let server = {
            let mut s = mk_step("server", &["sh", "-c", "sleep 30"]);
            s.background_until = Some("early".to_string());
            s
        };
        let pipeline = bg_pipeline("bg-bad-order", vec![early, server]);

        let err = PipelineRunner::new(pipeline).run().await.unwrap_err();
        assert!(
            matches!(err, RunnerError::InvalidConfig(ref m) if m.contains("later")),
            "expected later-step InvalidConfig, got {err:?}"
        );
    }

    /// Pre-flight rejects a `background_until` naming a nonexistent step.
    #[tokio::test]
    async fn background_until_unknown_step_is_rejected() {
        let server = {
            let mut s = mk_step("server", &["sh", "-c", "sleep 30"]);
            s.background_until = Some("nope".to_string());
            s
        };
        let work = mk_step("work", &["sh", "-c", "echo done"]);
        let pipeline = bg_pipeline("bg-bad-name", vec![server, work]);

        let err = PipelineRunner::new(pipeline).run().await.unwrap_err();
        assert!(
            matches!(err, RunnerError::InvalidConfig(ref m) if m.contains("unknown step")),
            "expected unknown-step InvalidConfig, got {err:?}"
        );
    }

    // ── R513-F3 wait-for health-gate steps (W207 Gap #5) ──────────────────

    /// Build a `kind = wait-for` step from a [`crate::types::WaitForConfig`],
    /// reusing the populated literal from [`mk_step`] so new QedStep fields
    /// don't have to be threaded through each test.
    fn mk_wait_for(name: &str, cfg: crate::types::WaitForConfig) -> crate::types::QedStep {
        let mut s = mk_step(name, &["unused"]);
        s.argv = vec![];
        s.kind = crate::types::StepKind::WaitFor;
        s.wait_for = Some(cfg);
        s
    }

    /// A `tcp` wait-for against a live listener passes immediately and the run
    /// goes green.
    #[tokio::test]
    async fn wait_for_tcp_passes_against_live_listener() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        // Hold the listener alive for the duration of the run.
        let _accept = tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let gate = mk_wait_for(
            "wait:db",
            crate::types::WaitForConfig {
                tcp: Some(addr),
                timeout_secs: 5,
                interval_ms: 50,
                ..Default::default()
            },
        );
        let work = mk_step("work", &["sh", "-c", "echo done"]);
        let pipeline = bg_pipeline("wf-tcp", vec![gate, work]);

        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        let gate_row = meta.steps.iter().find(|s| s.name == "wait:db").unwrap();
        assert_eq!(gate_row.status, RunStatus::Success);
    }

    /// An `http` wait-for polls a server that is initially down, then becomes
    /// healthy mid-budget — the gate passes once the endpoint answers 200.
    #[tokio::test]
    async fn wait_for_http_passes_once_server_comes_up() {
        // Reserve a port, free it, and only start serving after a short delay —
        // so the first poll(s) fail with connect-refused and a later one
        // succeeds, exercising the retry loop.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut scratch = [0u8; 1024];
                let _ = sock.read(&mut scratch).await;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .await;
            }
        });

        let gate = mk_wait_for(
            "wait:ready",
            crate::types::WaitForConfig {
                http: Some(format!("http://{addr}/health")),
                timeout_secs: 5,
                interval_ms: 50,
                ..Default::default()
            },
        );
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pipeline = bg_pipeline("wf-http", vec![gate]);
        let meta = PipelineRunner::new(pipeline).with_events(tx).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        // The success line names the endpoint as healthy.
        let events = drain_events(&mut rx);
        assert!(
            events.iter().any(|e| matches!(
                e,
                QedEvent::StepOutput { line, .. } if line.contains("healthy after")
            )),
            "emitted a 'healthy after' progress line; events={events:?}"
        );
    }

    /// A wait-for whose endpoint never comes up fails the step (and the run)
    /// once the timeout budget elapses, with a "never became healthy" message.
    #[tokio::test]
    async fn wait_for_times_out_when_endpoint_never_healthy() {
        // A port nothing listens on.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap().to_string();
        drop(probe);

        let gate = mk_wait_for(
            "wait:never",
            crate::types::WaitForConfig {
                tcp: Some(addr),
                timeout_secs: 1,
                interval_ms: 100,
                ..Default::default()
            },
        );
        let pipeline = bg_pipeline("wf-timeout", vec![gate]);
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let row = meta.steps.iter().find(|s| s.name == "wait:never").unwrap();
        assert_eq!(row.status, RunStatus::Failed);
        let err = row.error.as_deref().unwrap_or_default();
        assert!(
            err.contains("never became healthy"),
            "timeout surfaces a clear message; got {err:?}"
        );
    }

    /// A `shell` wait-for polls an arbitrary command — the escape hatch for
    /// targets `http`/`tcp` can't express (HTTPS, an npm-registry check).
    /// Backed by a marker file so the probe genuinely fails until a later
    /// attempt, exercising the retry loop the same way the http/tcp tests do.
    #[tokio::test]
    async fn wait_for_shell_passes_once_marker_file_appears() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("ready");
        let marker_for_writer = marker.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            std::fs::write(&marker_for_writer, b"ok").unwrap();
        });

        let gate = mk_wait_for(
            "wait:shell",
            crate::types::WaitForConfig {
                shell: Some(format!("test -f {}", marker.display())),
                timeout_secs: 5,
                interval_ms: 20,
                ..Default::default()
            },
        );
        let pipeline = bg_pipeline("wf-shell", vec![gate]);
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
    }

    /// `backoff_multiplier > 1.0` grows the poll interval — a run whose gate
    /// never becomes healthy takes noticeably longer to reach its `attempts`
    /// count than a fixed-interval gate would, and the failure message still
    /// reports both.
    #[tokio::test]
    async fn wait_for_backoff_grows_interval_on_repeated_misses() {
        let gate = mk_wait_for(
            "wait:backoff",
            crate::types::WaitForConfig {
                shell: Some("exit 1".to_string()),
                timeout_secs: 1,
                interval_ms: 50,
                backoff_multiplier: 3.0,
                max_interval_ms: Some(400),
                ..Default::default()
            },
        );
        let pipeline = bg_pipeline("wf-backoff", vec![gate]);
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let row = meta.steps.iter().find(|s| s.name == "wait:backoff").unwrap();
        let err = row.error.as_deref().unwrap_or_default();
        // 50ms then 150ms (capped under 400ms) sleeps inside a 1s budget: at
        // most a handful of attempts, never the ~20 a fixed 50ms interval
        // would allow.
        assert!(
            err.contains("attempts") && !err.contains("15 attempts") && !err.contains("20 attempts"),
            "expected backoff to sharply cut attempt count; got {err:?}"
        );
    }

    // ── R622 (W282): manual steps ──────────────────────────────────────────

    fn mk_manual(name: &str, cfg: crate::types::ManualConfig) -> crate::types::QedStep {
        let mut s = mk_step(name, &["unused"]);
        s.argv = vec![];
        s.kind = crate::types::StepKind::Manual;
        s.manual = Some(cfg);
        s
    }

    fn manual_cfg(prompt: &str, advance: Option<&str>) -> crate::types::ManualConfig {
        crate::types::ManualConfig {
            prompt: prompt.into(),
            terminal: vec![],
            advance: advance.map(str::to_string),
            checklist: vec![],
            // Tight cadence so the auto-advance race resolves inside a test.
            advance_poll_secs: 1,
            audience: crate::types::ManualAudience::Agent,
        }
    }

    /// A scripted [`ManualGate`] for tests: answers the Nth park with the Nth
    /// scripted answer, and records what it was asked plus the lock traffic.
    struct ScriptedGate {
        answers: std::sync::Mutex<std::collections::VecDeque<ManualAnswer>>,
        parks: Arc<std::sync::Mutex<Vec<ManualParkRequest>>>,
        lock_log: Arc<std::sync::Mutex<Vec<&'static str>>>,
        /// When set, the gate accepts the park but never answers. The sender is
        /// *retained* (not dropped) so the park genuinely blocks — dropping it
        /// would exercise the gate-went-away path instead.
        never_answers: bool,
        held: std::sync::Mutex<Vec<tokio::sync::oneshot::Sender<ManualAnswer>>>,
    }

    impl ScriptedGate {
        fn new(answers: Vec<ManualAnswer>) -> Self {
            Self {
                answers: std::sync::Mutex::new(answers.into()),
                parks: Arc::new(std::sync::Mutex::new(Vec::new())),
                lock_log: Arc::new(std::sync::Mutex::new(Vec::new())),
                never_answers: false,
                held: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ManualGate for ScriptedGate {
        async fn park(&self, req: &ManualParkRequest) -> Result<ManualParkHandle, String> {
            self.parks.lock().unwrap().push(req.clone());
            let (tx, rx) = tokio::sync::oneshot::channel();
            match self.answers.lock().unwrap().pop_front() {
                Some(a) if !self.never_answers => {
                    let _ = tx.send(a);
                }
                // Script exhausted (or a deliberately silent gate): hold the
                // sender so the park blocks rather than resolving by accident.
                _ => self.held.lock().unwrap().push(tx),
            }
            Ok(ManualParkHandle {
                id: Some(format!("form-{}", self.parks.lock().unwrap().len())),
                answer: rx,
                withdraw: Box::new(|| {}),
            })
        }

        async fn release_lock(&self) {
            self.lock_log.lock().unwrap().push("release");
        }

        async fn reacquire_lock(&self) {
            self.lock_log.lock().unwrap().push("reacquire");
        }
    }

    /// A satisfied `advance` advances the step without ever bothering a human —
    /// the pipeline can already see the work was done, and interrupting anyway
    /// is how gates get trained into reflexive clicking.
    #[tokio::test]
    async fn manual_advance_already_satisfied_skips_the_park() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let step = mk_manual("commit-and-tag", manual_cfg("Tag it.", Some("true")));
        let gate = Arc::new(ScriptedGate::new(vec![]));
        let parks = Arc::clone(&gate.parks);

        let meta = PipelineRunner::new(bg_pipeline("m-fast", vec![step]))
            .with_events(tx)
            .with_manual_gate(gate)
            .run()
            .await
            .unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        assert!(
            parks.lock().unwrap().is_empty(),
            "a satisfied condition must not mint a form"
        );
        let events = drain_events(&mut rx);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, QedEvent::StepAwaitingHuman { .. })),
            "no park ⇒ no StepAwaitingHuman event"
        );
        assert!(events.iter().any(|e| matches!(
            e,
            QedEvent::StepOutput { line, .. } if line.contains("already satisfied")
        )));
    }

    /// A manual `advance` must not be satisfiable by an artifact a PRIOR run
    /// left behind — only by one produced during THIS run. The predicate here
    /// is the nonce-file pair R605-B17 gave the release wizard's
    /// `commit-and-tag` gate (`diff -q <run-token> <freeze-ack> && git describe
    /// --tags --exact-match`); NOTE that the wizard no longer uses it, because
    /// on 2026-09-14 that gate stopped being manual at all and became a
    /// mechanical `git commit -a` + `git tag -f`. The property outlived the
    /// predicate, so the test stays: it is the only coverage of
    /// prior-run-artifact staleness at the `probe_manual_advance` level, and
    /// the next manual step someone writes will hit the same trap.
    ///
    /// Why it mattered: before B17, `commit-and-tag`'s advance was just `git
    /// describe --tags --exact-match`, which a tag cut by an EARLIER run
    /// satisfies forever after — so the probe-before-park optimization
    /// exercised by `manual_advance_already_satisfied_skips_the_park` above
    /// skipped the human on every rerun and release-check re-tested stale
    /// bytes. That is what happened to the 0.8.32 release (tree fix authored
    /// 18:13-18:32 PDT, freeze commit 17:25 PDT, rerun at 02:56Z tested the
    /// 17:25 bytes a second time with nothing on screen saying why), and then
    /// to 0.8.39 five more times, which is what retired the gate.
    #[tokio::test]
    async fn manual_advance_ignores_an_ack_left_by_a_prior_run() {
        let tmp = tempfile::tempdir().unwrap();
        let token = tmp.path().join("release-wizard-run-token");
        let ack = tmp.path().join("release-wizard-freeze-ack");
        // A prior run: it wrote its own token, a human copied it into the ack
        // file, and that run went green. Both files still hold that value.
        std::fs::write(&token, b"prior-run-17").unwrap();
        std::fs::write(&ack, b"prior-run-17").unwrap();
        let cond = format!(
            "diff -q {} {} >/dev/null 2>&1",
            token.display(),
            ack.display()
        );
        let step = mk_manual("commit-and-tag", manual_cfg("Tag it.", Some(&cond)));

        // What `snapshot-pre-freeze-token` does mechanically, unconditionally,
        // before this step is ever probed: overwrite the token with a value
        // unique to THIS run. Now `token` and `ack` disagree even though a
        // human already froze a release here once before — the disagreement
        // is the whole fix.
        std::fs::write(&token, b"this-run-42").unwrap();

        // Only the poll can resolve this park (never_answers) — proves the
        // human genuinely has to act again this run, not just click through.
        let mut gate = ScriptedGate::new(vec![]);
        gate.never_answers = true;
        let gate = Arc::new(gate);
        let parks = Arc::clone(&gate.parks);

        // Simulate the human running the prefilled `cp run-token freeze-ack`
        // terminal command once they've seen the park.
        let watch = Arc::clone(&parks);
        let ack_path = ack.clone();
        tokio::spawn(async move {
            loop {
                if !watch.lock().unwrap().is_empty() {
                    std::fs::write(&ack_path, b"this-run-42").unwrap();
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });

        let meta = PipelineRunner::new(bg_pipeline("m-rerun", vec![step]))
            .with_manual_gate(gate)
            .run()
            .await
            .unwrap();

        assert_eq!(
            parks.lock().unwrap().len(),
            1,
            "a mismatched ack from a prior run must still surface the gate to a human, \
             not auto-advance the way `git describe --tags --exact-match` alone did"
        );
        assert_eq!(
            meta.status,
            RunStatus::Success,
            "once the human's ack matches THIS run's token, it still advances normally"
        );
    }

    /// R605-B19. `probe_manual_advance` now injects `QED_RUN_ID`/
    /// `QED_STEP_NAME` into `advance`'s environment, so a predicate can ask
    /// "did THIS run do it" directly — the primitive the test above's
    /// workaround (a hand-rolled nonce file under `.yah/jit/qed/`) had to
    /// invent because neither a run id nor `${{ steps.*.outputs.* }}`
    /// substitution reached `advance` before this. No file anywhere in this
    /// test: two [`PipelineRunner`]s stand in for two separate `qed run`
    /// invocations of the same pipeline (R605-B17's exact rerun scenario),
    /// and the predicate tells them apart purely from the injected env.
    #[tokio::test]
    async fn probe_manual_advance_lets_a_predicate_gate_on_this_run_with_no_file() {
        let run_a = PipelineRunner::new(make_pipeline("p", vec![]));
        let run_b = PipelineRunner::new(make_pipeline("p", vec![]));
        assert_ne!(
            run_a.run_id(),
            run_b.run_id(),
            "fixture sanity: each run gets its own id"
        );

        let cwd = std::env::temp_dir();
        // Stands in for "a human recorded THIS run's id somewhere durable" —
        // a git commit message, an annotated tag, anything but a jit file.
        let cond = format!(r#"[ "$QED_RUN_ID" = "{}" ]"#, run_a.run_id());

        assert!(
            run_a
                .probe_manual_advance(&cond, &cwd, "commit-and-tag")
                .await
                .is_ok(),
            "run_a's own id satisfies a predicate written for run_a"
        );
        assert!(
            run_b
                .probe_manual_advance(&cond, &cwd, "commit-and-tag")
                .await
                .is_err(),
            "run_b must not satisfy a predicate naming run_a's id — a PRIOR \
             run's artifact silently satisfying THIS run's gate is exactly \
             the R605-B17 failure mode"
        );

        // QED_STEP_NAME distinguishes multiple manual steps in one pipeline
        // (commit-and-tag vs push-tag) using similarly-shaped predicates.
        let step_cond = r#"[ "$QED_STEP_NAME" = "push-tag" ]"#;
        assert!(
            run_a
                .probe_manual_advance(step_cond, &cwd, "push-tag")
                .await
                .is_ok()
        );
        assert!(
            run_a
                .probe_manual_advance(step_cond, &cwd, "commit-and-tag")
                .await
                .is_err()
        );
    }

    /// The ordinary path: no `advance` to check, so the step parks, the human
    /// says continue, and the run goes green. The concurrency key is released
    /// for the park and reacquired before the step loop resumes.
    #[tokio::test]
    async fn manual_parks_on_a_human_and_releases_the_lock() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let step = mk_manual("push-tag", manual_cfg("Push the tag.", None));
        let gate = Arc::new(ScriptedGate::new(vec![ManualAnswer::Continue]));
        let lock_log = Arc::clone(&gate.lock_log);
        let parks = Arc::clone(&gate.parks);

        let meta = PipelineRunner::new(bg_pipeline("m-park", vec![step]))
            .with_events(tx)
            .with_manual_gate(gate)
            .run()
            .await
            .unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            *lock_log.lock().unwrap(),
            vec!["release", "reacquire"],
            "W282: a parked step must not hold cargo-target"
        );
        let parked = parks.lock().unwrap();
        assert_eq!(parked.len(), 1);
        assert_eq!(parked[0].prompt, "Push the tag.");
        assert_eq!(parked[0].step_name, "push-tag");
        assert!(parked[0].advance_failure.is_none());

        let events = drain_events(&mut rx);
        let awaiting = events
            .iter()
            .find_map(|e| match e {
                QedEvent::StepAwaitingHuman { form_id, name, .. } => Some((form_id, name)),
                _ => None,
            })
            .expect("StepAwaitingHuman emitted");
        assert_eq!(awaiting.0.as_deref(), Some("form-1"));
        assert_eq!(awaiting.1, "push-tag");
    }

    /// Resume is not a bare continue. The tree can move during a park, so a
    /// human's "continue" is re-checked against `advance` — and a failure
    /// re-parks with the failing command's output rather than failing the step.
    #[tokio::test]
    async fn manual_reparks_when_advance_still_fails_after_resume() {
        let tmp = tempfile::tempdir().unwrap();
        let flag = tmp.path().join("tagged");
        // Passes only once the file exists. The first Continue arrives before
        // it does; the second creates it first.
        let cond = format!("test -f {}", flag.display());
        let step = mk_manual("commit-and-tag", manual_cfg("Tag it.", Some(&cond)));

        let gate = Arc::new(ScriptedGate::new(vec![
            ManualAnswer::Continue,
            ManualAnswer::Continue,
        ]));
        let parks = Arc::clone(&gate.parks);

        // Create the flag once the first park has been recorded, so the first
        // resume fails and the second succeeds.
        let watch = Arc::clone(&parks);
        let flag_path = flag.clone();
        tokio::spawn(async move {
            loop {
                if watch.lock().unwrap().len() >= 2 {
                    std::fs::write(&flag_path, b"").unwrap();
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });

        let meta = PipelineRunner::new(bg_pipeline("m-repark", vec![step]))
            .with_camp_root(tmp.path().to_path_buf())
            .with_manual_gate(gate)
            .run()
            .await
            .unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        let parked = parks.lock().unwrap();
        assert!(
            parked.len() >= 2,
            "a failed advance must re-park, not fail the step; got {} parks",
            parked.len()
        );
        let tail = parked[1]
            .advance_failure
            .as_deref()
            .expect("the re-park carries why the pipeline still doesn't believe them");
        assert!(
            tail.contains("exited 1"),
            "re-park shows the failing command's exit; got {tail:?}"
        );
    }

    /// `advance` starting to pass on its own resolves the park without an
    /// answer — the human did the thing in a terminal and never came back to
    /// the queue, which is the common case for a `git tag`.
    #[tokio::test]
    async fn manual_auto_advances_when_condition_starts_passing_while_parked() {
        let tmp = tempfile::tempdir().unwrap();
        let flag = tmp.path().join("tagged");
        let cond = format!("test -f {}", flag.display());
        let step = mk_manual("commit-and-tag", manual_cfg("Tag it.", Some(&cond)));

        // A gate that never answers — only the poll can resolve this park.
        let mut gate = ScriptedGate::new(vec![]);
        gate.never_answers = true;
        let gate = Arc::new(gate);
        let withdrawn = Arc::clone(&gate.parks);

        let flag_path = flag.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            std::fs::write(&flag_path, b"").unwrap();
        });

        let (tx, mut rx) = mpsc::unbounded_channel();
        let meta = PipelineRunner::new(bg_pipeline("m-auto", vec![step]))
            .with_camp_root(tmp.path().to_path_buf())
            .with_events(tx)
            .with_manual_gate(gate)
            .run()
            .await
            .unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(withdrawn.lock().unwrap().len(), 1, "parked exactly once");
        let events = drain_events(&mut rx);
        assert!(
            events.iter().any(|e| matches!(
                e,
                QedEvent::StepOutput { line, .. } if line.contains("started passing while parked")
            )),
            "the auto-advance is announced on the step log"
        );
    }

    /// Declining a manual step fails it with the human's reason, and the lock
    /// is still reacquired on the way out.
    #[tokio::test]
    async fn manual_abort_fails_the_step_with_the_reason() {
        let step = mk_manual("push-tag", manual_cfg("Push the tag.", None));
        let gate = Arc::new(ScriptedGate::new(vec![ManualAnswer::Abort {
            reason: "wrong version".into(),
        }]));
        let lock_log = Arc::clone(&gate.lock_log);

        let meta = PipelineRunner::new(bg_pipeline("m-abort", vec![step]))
            .with_manual_gate(gate)
            .run()
            .await
            .unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        let row = meta.steps.iter().find(|s| s.name == "push-tag").unwrap();
        let err = row.error.as_deref().unwrap_or_default();
        assert!(
            err.contains("wrong version"),
            "the decline reason reaches the step error; got {err:?}"
        );
        assert_eq!(*lock_log.lock().unwrap(), vec!["release", "reacquire"]);
    }

    /// Headless (`yah qed run`, no gate installed): a manual step whose
    /// `advance` does not hold fails with a message that names the condition
    /// and points at the daemon — it must never silently advance because
    /// nobody was listening.
    #[tokio::test]
    async fn manual_without_a_gate_fails_when_advance_does_not_hold() {
        let step = mk_manual("commit-and-tag", manual_cfg("Tag it.", Some("false")));
        let meta = PipelineRunner::new(bg_pipeline("m-headless", vec![step]))
            .run()
            .await
            .unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        let err = meta.steps[0].error.as_deref().unwrap_or_default();
        assert!(
            err.contains("no answer surface is attached") && err.contains("`false`"),
            "headless failure names the condition and the fix; got {err:?}"
        );
    }

    /// R906-F2 — an `audience = "operator"` gate with a compile ahead of it
    /// fails the run *before* it reaches a human. Note the satisfied `advance`:
    /// this would otherwise be the happy path, which is the point — the check
    /// sits ahead of the probe so an authoring defect cannot hide behind a
    /// predicate that happens to hold.
    #[tokio::test]
    async fn a_late_operator_gate_fails_the_run_instead_of_parking() {
        let mut cfg = manual_cfg("Authorize this release.", Some("true"));
        cfg.audience = crate::types::ManualAudience::Operator;
        // `sh -c "echo cargo"` costs nothing to run and still trips the argv
        // scan on its `cargo` token — the detector reads argv, not exit codes.
        let steps = vec![
            mk_step("build", &["sh", "-c", "echo cargo"]),
            mk_manual("authorize-release", cfg),
        ];
        let gate = Arc::new(ScriptedGate::new(vec![]));
        let parks = Arc::clone(&gate.parks);

        let meta = PipelineRunner::new(bg_pipeline("m-late-gate", steps))
            .with_manual_gate(gate)
            .run()
            .await
            .unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        let err = meta.steps[1].error.as_deref().unwrap_or_default();
        assert!(
            err.contains("authorize-release")
                && err.contains("`build`")
                && err.contains("allow_late_operator_block"),
            "the failure must name the gate, the compiling step and the escape; got {err:?}"
        );
        assert!(
            parks.lock().unwrap().is_empty(),
            "nobody should have been asked"
        );
    }

    /// The same pipeline with the opt-out set runs through — one flag, both
    /// halves (the loader stops warning, the runner stops refusing).
    #[tokio::test]
    async fn allow_late_operator_block_lets_a_late_gate_run() {
        let mut cfg = manual_cfg("Authorize this release.", Some("true"));
        cfg.audience = crate::types::ManualAudience::Operator;
        let steps = vec![
            mk_step("build", &["sh", "-c", "echo cargo"]),
            mk_manual("authorize-release", cfg),
        ];
        let mut pipeline = bg_pipeline("m-late-gate-allowed", steps);
        pipeline.allow_late_operator_block = true;

        let meta = PipelineRunner::new(pipeline)
            .with_manual_gate(Arc::new(ScriptedGate::new(vec![])))
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
    }

    /// R906-F2 — a RESUMED run cannot prove its operator gate was first, so it
    /// is refused even when the surviving steps look clean. Note the pipeline
    /// here is exactly the shape the rule asks for — gate at index 0, nothing
    /// ahead of it — and it still fails: what the runner holds is a truncated
    /// list, and the steps the resume skipped are not reachable from it.
    ///
    /// This is the path that matters. A release rerun is routine, and it is the
    /// one way an operator gate would otherwise sail past the guard.
    #[tokio::test]
    async fn an_operator_gate_in_a_resumed_run_is_late_by_construction() {
        let mut cfg = manual_cfg("Authorize this release.", Some("true"));
        cfg.audience = crate::types::ManualAudience::Operator;
        let gate = Arc::new(ScriptedGate::new(vec![]));
        let parks = Arc::clone(&gate.parks);

        let meta = PipelineRunner::new(bg_pipeline(
            "m-resumed-gate",
            vec![mk_manual("authorize-release", cfg)],
        ))
        .with_manual_gate(gate)
        .with_step_selection(3, false)
        .run()
        .await
        .unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        let err = meta.steps[0].error.as_deref().unwrap_or_default();
        assert!(
            err.contains("authorize-release")
                && err.contains("resumed from step 3")
                && err.contains("allow_late_operator_block"),
            "the failure must name the gate, the resume and the escape; got {err:?}"
        );
        assert!(
            parks.lock().unwrap().is_empty(),
            "nobody should have been asked"
        );
    }

    /// R906-F2 — a NAME-SUBSET run (`selected_steps`) is refused for the same
    /// reason as a resume, and this is the arm an offset cannot express: the
    /// filter `retain`s arbitrary steps, so `index_offset` is still 0 and the
    /// surviving list looks, from inside the runner, exactly like a complete
    /// one. `with_step_selection`'s second argument is the only thing that
    /// distinguishes them.
    #[tokio::test]
    async fn an_operator_gate_in_a_name_subset_run_is_late_by_construction() {
        let mut cfg = manual_cfg("Authorize this release.", Some("true"));
        cfg.audience = crate::types::ManualAudience::Operator;
        let gate = Arc::new(ScriptedGate::new(vec![]));
        let parks = Arc::clone(&gate.parks);

        let meta = PipelineRunner::new(bg_pipeline(
            "m-subset-gate",
            vec![mk_manual("authorize-release", cfg)],
        ))
        .with_manual_gate(gate)
        .with_step_selection(0, true)
        .run()
        .await
        .unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        let err = meta.steps[0].error.as_deref().unwrap_or_default();
        assert!(
            err.contains("authorize-release")
                && err.contains("name-subset selection")
                && err.contains("allow_late_operator_block"),
            "the failure must name the gate, the subset and the escape; got {err:?}"
        );
        assert!(
            parks.lock().unwrap().is_empty(),
            "nobody should have been asked"
        );
    }

    /// The same run from the start is fine — proving the refusals above are the
    /// trimmed-list arm and not the gate itself.
    #[tokio::test]
    async fn the_same_operator_gate_run_from_the_start_is_accepted() {
        let mut cfg = manual_cfg("Authorize this release.", Some("true"));
        cfg.audience = crate::types::ManualAudience::Operator;
        let meta = PipelineRunner::new(bg_pipeline(
            "m-fresh-gate",
            vec![mk_manual("authorize-release", cfg)],
        ))
        .with_manual_gate(Arc::new(ScriptedGate::new(vec![])))
        .run()
        .await
        .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
    }

    /// R906-F2 — an operator gate reached as a SUB-PIPELINE CHILD is refused.
    /// The child's own steps look immaculate (the gate is its only step); the
    /// lateness is a property of how the child was reached, which is why the
    /// runner checks `parent_run_id` rather than asking the predicate.
    #[tokio::test]
    async fn an_operator_gate_inside_a_child_run_is_late_by_construction() {
        let mut cfg = manual_cfg("Authorize this release.", Some("true"));
        cfg.audience = crate::types::ManualAudience::Operator;
        let child = make_pipeline("child", vec![mk_manual("authorize-release", cfg)]);

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let gate = Arc::new(ScriptedGate::new(vec![]));
        let parks = Arc::clone(&gate.parks);
        let root = make_pipeline(
            "root",
            vec![sub_step(
                "descend",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
        );

        let meta = PipelineRunner::new(root)
            .with_sub_pipeline_resolver(resolver)
            .with_manual_gate(gate)
            .run()
            .await
            .unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        // The parent summarises rather than forwarding the child's message, so
        // assert on what it does carry: which child, and which of its steps.
        let err = meta.steps[0].error.as_deref().unwrap_or_default();
        assert!(
            err.contains("sub-pipeline `child`") && err.contains("`authorize-release`"),
            "the parent's step error must name the child and the refusing step; got {err:?}"
        );
        assert!(
            parks.lock().unwrap().is_empty(),
            "nobody should have been asked"
        );
    }

    /// Control for the test above: the identical nesting with the default
    /// `audience = "agent"` runs through. Without this, the failure there could
    /// equally be "a manual step inside a sub-pipeline is broken" — it is the
    /// audience that makes the child's gate a refusal.
    #[tokio::test]
    async fn an_agent_audience_gate_inside_a_child_run_is_accepted() {
        let child = make_pipeline(
            "child",
            vec![mk_manual(
                "authorize-release",
                manual_cfg("Authorize this release.", Some("true")),
            )],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let meta = PipelineRunner::new(make_pipeline(
            "root",
            vec![sub_step(
                "descend",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
        ))
        .with_sub_pipeline_resolver(resolver)
        .with_manual_gate(Arc::new(ScriptedGate::new(vec![])))
        .run()
        .await
        .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
    }

    /// Headless with a *satisfied* condition still passes — that is the whole
    /// point of `advance` being verifiable rather than an honour-system button.
    #[tokio::test]
    async fn manual_without_a_gate_passes_when_advance_holds() {
        let step = mk_manual("commit-and-tag", manual_cfg("Tag it.", Some("true")));
        let meta = PipelineRunner::new(bg_pipeline("m-headless-ok", vec![step]))
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
    }

    /// Headless with no `advance` at all is the honest failure: nothing to
    /// verify and nobody to ask.
    #[tokio::test]
    async fn manual_without_a_gate_or_advance_fails_with_both_routes_named() {
        let step = mk_manual("push-tag", manual_cfg("Push the tag.", None));
        let meta = PipelineRunner::new(bg_pipeline("m-headless-bare", vec![step]))
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let err = meta.steps[0].error.as_deref().unwrap_or_default();
        assert!(
            err.contains("camp daemon") && err.contains("manual.advance"),
            "the error names both ways out; got {err:?}"
        );
    }

    /// R605-B18. A "the tag reached origin" check must compare the OBJECT
    /// origin's tag resolves to against the local tag's object, not just that a
    /// same-named ref exists on origin. This shell is verbatim the wizard's
    /// `push-tag` check; since 2026-09-14 it runs there as a post-push
    /// assertion in a mechanical step rather than as a manual `advance`, but
    /// `probe_manual_advance` is still the harness that can evaluate it against
    /// a real repo pair, so the test keeps that shape. Before this fix it was
    /// `git ls-remote --exit-code --tags origin "$(git describe --tags
    /// --exact-match)"` — a name-only check. R605-B17 made `commit-and-tag`
    /// instruct a delete-and-recut under the SAME version name whenever a
    /// rerun's tag no longer matches a fresh HEAD, and a stale same-named tag
    /// already on origin from the attempt being recut away from satisfies
    /// the old predicate, auto-skipping the push and leaving origin on
    /// pre-fix bytes while every later step believes the release shipped.
    /// Exercised with an ANNOTATED tag deliberately — the named trap is that
    /// an annotated tag's plain `refs/tags/<name>` row on `git ls-remote` is
    /// the tag OBJECT's id, not the commit it points at (the commit is a
    /// separate `refs/tags/<name>^{}` row), so a naive comparison against
    /// that plain row would mismatch on every annotated tag and false-block
    /// every push, not just the recut case this test is about. MEASURED
    /// while building this fix (git 2.50.1, no `-d`/`--dereference` flag on
    /// this `ls-remote` at all): passing the tag name as a query pattern
    /// (`git ls-remote --tags origin "$name"`) suppresses the peeled `^{}`
    /// row entirely — an unfiltered query (`git ls-remote --tags origin`,
    /// no name arg) is what actually returns both rows, matched client-side.
    #[tokio::test]
    async fn push_tag_advance_rejects_a_recut_annotated_tag_until_force_pushed() {
        // The real predicate string from yah-release-wizard.toml's push-tag
        // step, verbatim — this test is only meaningful if it stays in sync.
        const ADVANCE: &str = r#"
name="$(git describe --tags --exact-match)" || exit 1
local_sha="$(git rev-parse "refs/tags/${name}^{commit}")" || exit 1
remote_sha="$(git ls-remote --tags origin | awk -v peeled="refs/tags/${name}^{}" -v plain="refs/tags/${name}" '$2==peeled{print $1; f=1; exit} $2==plain{c=$1} END{if(!f && c!="") print c}')"
[ -n "$remote_sha" ] && [ "$remote_sha" = "$local_sha" ]
"#;

        let remote = tempfile::tempdir().unwrap();
        run_git(remote.path(), &["init", "--bare", "-q"]).unwrap();

        let local = tempfile::tempdir().unwrap();
        let git = |args: &[&str]| run_git(local.path(), args).unwrap();
        git(&["init", "-q", "-b", "main"]);
        git(&["config", "user.email", "t@t.t"]);
        git(&["config", "user.name", "t"]);
        std::fs::write(local.path().join("f.txt"), "A").unwrap();
        git(&["add", "."]);
        git(&["commit", "-q", "-m", "A"]);
        git(&[
            "remote",
            "add",
            "origin",
            remote.path().to_str().unwrap(),
        ]);
        git(&["push", "-q", "origin", "main"]);
        // Annotated tag at A, pushed — a prior, successful attempt.
        git(&["tag", "-a", "v1.0.0", "-m", "release A"]);
        git(&["push", "-q", "origin", "v1.0.0"]);

        let runner = PipelineRunner::new(make_pipeline("p", vec![]));
        assert!(
            runner
                .probe_manual_advance(ADVANCE, local.path(), "push-tag")
                .await
                .is_ok(),
            "sanity: a freshly pushed annotated tag must satisfy advance"
        );

        // R605-B17's delete-and-recut path: same name, new commit B, NOT
        // pushed yet. Origin still has the stale tag pointing at A.
        std::fs::write(local.path().join("f.txt"), "B").unwrap();
        git(&["add", "."]);
        git(&["commit", "-q", "-m", "B"]);
        git(&["tag", "-d", "v1.0.0"]);
        git(&["tag", "-a", "v1.0.0", "-m", "release B"]);

        assert!(
            runner
                .probe_manual_advance(ADVANCE, local.path(), "push-tag")
                .await
                .is_err(),
            "a same-named tag still on origin from the PRIOR commit must not \
             satisfy advance — the old name-only check wrongly passed here"
        );

        // Recutting a tag origin already has requires --force.
        git(&["push", "-q", "--force", "origin", "v1.0.0"]);
        assert!(
            runner
                .probe_manual_advance(ADVANCE, local.path(), "push-tag")
                .await
                .is_ok(),
            "once origin's tag object matches the local recut, advance must pass"
        );
    }

    // ── R717-T11 (W296): a manual CELL, end to end ───────────────────────────

    /// The T11 verify. A doc run that reaches a manual cell parks and stops:
    /// the cell itself never executes, the following cell never starts, and
    /// nothing inside the runner can answer the gate on the caller's behalf —
    /// which is the whole property when the caller is an agent, since an agent
    /// otherwise *can* resolve forms.
    ///
    /// W257's BIOS block is the shape used deliberately: no `advance`, because
    /// a person at the box setting restore-on-AC-power-loss has no
    /// out-of-band proof to offer. That is exactly the cell an agent would
    /// sail through.
    #[tokio::test]
    async fn a_doc_manual_cell_parks_the_run_and_nothing_downstream_executes() {
        let tmp = tempfile::tempdir().unwrap();
        let marker = tmp.path().join("ran-after-the-gate");
        let md = format!(
            "```toml notebook=node-onboard\n```\n\n\
             ```bash cell=bios manual\n\
             # At the box: set Restore-on-AC-power-loss.\n\
             # checklist: Restore-on-AC-power-loss = On\n\
             ```\n\n\
             ```bash cell=after assert\ntouch {}\n```\n",
            marker.display(),
        );
        let doc = crate::parse_doc("W257.md", &md).unwrap();
        let pipeline = doc
            .lower(tmp.path(), &std::collections::HashMap::new(), None)
            .unwrap();

        // A gate that accepts the park and never answers — the agent-initiated
        // case, where the only thing that can move this run is a person.
        let mut gate = ScriptedGate::new(vec![]);
        gate.never_answers = true;
        let gate = Arc::new(gate);
        let parks = Arc::clone(&gate.parks);

        let (tx, mut rx) = mpsc::unbounded_channel();
        let runner = PipelineRunner::new(pipeline)
            .with_camp_root(tmp.path().to_path_buf())
            .with_events(tx)
            .with_manual_gate(gate);
        let elapsed = tokio::time::timeout(
            std::time::Duration::from_millis(600),
            runner.run(),
        )
        .await;
        assert!(
            elapsed.is_err(),
            "the run must STAY parked — it resolved itself without a human"
        );

        let events = drain_events(&mut rx);
        let parked = events
            .iter()
            .find_map(|e| match e {
                QedEvent::StepAwaitingHuman { name, form_id, advance, .. } => {
                    Some((name.clone(), form_id.clone(), advance.clone()))
                }
                _ => None,
            })
            .expect("the doc's manual cell emits StepAwaitingHuman");
        assert_eq!(parked.0, "bios", "the step name IS the cell id");
        assert!(parked.1.is_some(), "and it carries the minted form's id");
        assert!(parked.2.is_none(), "advance is optional — this cell has none");

        let req = &parks.lock().unwrap()[0];
        assert_eq!(req.prompt, "At the box: set Restore-on-AC-power-loss.");
        assert_eq!(req.checklist, vec!["Restore-on-AC-power-loss = On"]);

        assert!(
            finished_pos(&events, "bios").is_none(),
            "a parked step has not finished"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, QedEvent::StepStarted { name, .. } if name == "after")),
            "and the cell after the gate must not start"
        );
        assert!(!marker.exists(), "nothing downstream of the gate ran");
    }

    /// An `https://` URL is rejected up front with a pointed message rather than
    /// silently failing a plaintext GET against a TLS port for the whole budget.
    #[tokio::test]
    async fn wait_for_https_fails_fast() {
        let gate = mk_wait_for(
            "wait:tls",
            crate::types::WaitForConfig {
                http: Some("https://localhost:8443/health".to_string()),
                timeout_secs: 30, // long budget; must NOT be consumed
                interval_ms: 100,
                ..Default::default()
            },
        );
        let pipeline = bg_pipeline("wf-tls", vec![gate]);
        let started = std::time::Instant::now();
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "https rejection is immediate, not after the 30s budget"
        );
        let row = meta.steps.iter().find(|s| s.name == "wait:tls").unwrap();
        let err = row.error.as_deref().unwrap_or_default();
        assert!(err.contains("https"), "names the https limitation; got {err:?}");
    }

    // ── R513-F4 finally: always-run teardown (W207 Gap #6) ────────────────

    /// A `finally` step runs after a passing pipeline, after the main step, and
    /// the run stays green.
    #[tokio::test]
    async fn finally_runs_after_successful_pipeline() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut pipeline = bg_pipeline("fin-ok", vec![mk_step("work", &["sh", "-c", "echo work"])]);
        pipeline.finally = vec![mk_step("teardown", &["sh", "-c", "echo teardown"])];

        let meta = PipelineRunner::new(pipeline).with_events(tx).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        let td = meta.steps.iter().find(|s| s.name == "teardown").unwrap();
        assert_eq!(td.status, RunStatus::Success);

        let events = drain_events(&mut rx);
        let work_fin = finished_pos(&events, "work").expect("work finished");
        let td_fin = finished_pos(&events, "teardown").expect("teardown finished");
        assert!(work_fin < td_fin, "finally runs after the main step; events={events:?}");
    }

    /// A `finally` step runs even when the pipeline body failed — that's the
    /// whole point (upload traces on a failed test run).
    #[tokio::test]
    async fn finally_runs_even_when_pipeline_fails() {
        let mut pipeline =
            bg_pipeline("fin-onfail", vec![mk_step("work", &["sh", "-c", "exit 1"])]);
        pipeline.finally = vec![mk_step("teardown", &["sh", "-c", "echo cleaned"])];

        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed, "body failed → run failed");
        let td = meta.steps.iter().find(|s| s.name == "teardown").unwrap();
        assert_eq!(
            td.status,
            RunStatus::Success,
            "teardown still ran despite the body failure"
        );
    }

    /// A failing `finally` step marks the run Failed, but outcome selection keys
    /// off the *work* status — so a green body still fires `on_success`.
    #[tokio::test]
    async fn finally_failure_marks_run_failed_but_on_success_still_fires() {
        let dispatcher = RecordingDispatcher::new();
        let mut pipeline = pipeline_with_outcomes(
            vec![Outcome::YubabaDeploy {
                service: "yah".into(),
                env: "production".into(),
            }],
            vec![Outcome::AlmanacRun {
                pipeline: "should-not-run".into(),
            }],
            vec!["true".to_string()], // body passes
        );
        pipeline.finally = vec![mk_step("teardown", &["sh", "-c", "exit 3"])];

        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();

        // The run is Failed (teardown broke)…
        assert_eq!(meta.status, RunStatus::Failed);
        // …but the on_success outcome fired (work passed), and on_fail did NOT.
        assert_eq!(
            dispatcher.recorded(),
            vec!["yubaba-deploy:yah:production"],
            "outcome selection uses work-status, not the teardown failure"
        );
    }

    /// `on_fail = "continue"` on a `finally` step keeps a teardown failure from
    /// marking the run Failed.
    #[tokio::test]
    async fn finally_continue_on_fail_keeps_run_green() {
        let mut teardown = mk_step("teardown", &["sh", "-c", "exit 1"]);
        teardown.on_fail = OnFail::Continue;
        let mut pipeline = bg_pipeline("fin-cont", vec![mk_step("work", &["sh", "-c", "true"])]);
        pipeline.finally = vec![teardown];

        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Success,
            "continue-on-fail teardown failure doesn't fail the run"
        );
        let td = meta.steps.iter().find(|s| s.name == "teardown").unwrap();
        assert_eq!(td.status, RunStatus::Failed, "the step itself still records Failed");
    }

    /// Every `finally` step is attempted even if an earlier one fails (best-effort
    /// teardown — a failure never aborts the rest).
    #[tokio::test]
    async fn all_finally_steps_run_even_if_one_fails() {
        let mut pipeline = bg_pipeline("fin-all", vec![mk_step("work", &["sh", "-c", "true"])]);
        pipeline.finally = vec![
            mk_step("teardown-a", &["sh", "-c", "exit 1"]), // fails (Abort default)
            mk_step("teardown-b", &["sh", "-c", "echo b"]), // must still run
        ];

        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let a = meta.steps.iter().find(|s| s.name == "teardown-a").unwrap();
        let b = meta.steps.iter().find(|s| s.name == "teardown-b").unwrap();
        assert_eq!(a.status, RunStatus::Failed);
        assert_eq!(
            b.status,
            RunStatus::Success,
            "teardown-b ran despite teardown-a failing"
        );
    }

    /// A failing step streams stderr; the failure status reaches RunFinished
    /// and the stderr tail surfaces in the StepFailed message.
    #[tokio::test]
    async fn failing_step_streams_stderr_and_finishes_failed() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pipeline = one_step_pipeline(
            "test-events-fail",
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "echo boom >&2; exit 1".to_string(),
            ],
        );
        let runner = PipelineRunner::new(pipeline).with_events(tx);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);

        // The failure reason is persisted on the terminal StepStatus, not only
        // in the live event stream — so `qed.status` can explain *why* a step
        // failed after the run ends.
        let failed = &meta.steps[0];
        assert_eq!(failed.status, RunStatus::Failed);
        let err = failed
            .error
            .as_deref()
            .expect("failed step carries an error reason");
        assert!(
            err.contains("boom"),
            "error tail carries stderr; got {err:?}"
        );

        let mut events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }

        assert!(
            events.iter().any(|e| matches!(
                e,
                QedEvent::StepOutput { stream: OutputStream::Stderr, line, .. } if line == "boom"
            )),
            "captured the stderr line; events={events:?}"
        );
        assert!(
            matches!(
                events.last(),
                Some(QedEvent::RunFinished {
                    status: RunStatus::Failed,
                    ..
                })
            ),
            "last event is RunFinished/Failed, got {:?}",
            events.last()
        );
    }

    /// No sink attached = `run()` still completes and returns terminal meta.
    #[tokio::test]
    async fn no_sink_runs_silently() {
        let pipeline = one_step_pipeline("test-silent", vec!["true".to_string()]);
        let runner = PipelineRunner::new(pipeline);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
    }

    // ── R531-T1 host-triple self-detection ──────────────────────────────────

    /// A runner self-detects its host triple at construction, and the value
    /// is a well-formed triple matching the process host.
    #[test]
    fn runner_self_detects_host_triple() {
        let pipeline = one_step_pipeline("host", vec!["true".to_string()]);
        let runner = PipelineRunner::new(pipeline);
        assert_eq!(runner.host_triple(), crate::platform::detect_host_triple());
        assert_eq!(
            crate::platform::arch_of(runner.host_triple()),
            std::env::consts::ARCH,
        );
    }

    /// `with_host_triple` overrides the detected host — the seam the daemon
    /// uses when a runner's steps land on a remote host of a known triple.
    #[test]
    fn with_host_triple_overrides_detection() {
        let pipeline = one_step_pipeline("host", vec!["true".to_string()]);
        let runner = PipelineRunner::new(pipeline).with_host_triple("x86_64-unknown-linux-gnu");
        assert_eq!(runner.host_triple(), "x86_64-unknown-linux-gnu");
    }

    /// `step_platform` composes the runner's host with the step's declared
    /// target (R531-F2), and falls back to the legacy `triple` field.
    #[test]
    fn step_platform_composes_host_with_step_target() {
        let pipeline = one_step_pipeline("build", vec!["true".to_string()]);
        let runner = PipelineRunner::new(pipeline).with_host_triple("aarch64-apple-darwin");

        // Declared [platform].target wins.
        let mut step = runner.pipeline.steps[0].clone();
        step.platform = Some(crate::platform::PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: Some("linux/amd64".into()),
            native: false,
        });
        let p = runner.step_platform(&step);
        assert_eq!(p.host, "aarch64-apple-darwin");
        assert_eq!(p.target.as_deref(), Some("x86_64-unknown-linux-musl"));
        assert!(p.container_is_foreign_arch(), "amd64 image on arm64 host");

        // Legacy `triple` field is lifted when no [platform] block is set.
        let mut legacy = runner.pipeline.steps[0].clone();
        legacy.triple = Some("x86_64-unknown-linux-musl".into());
        let p2 = runner.step_platform(&legacy);
        assert_eq!(p2.target.as_deref(), Some("x86_64-unknown-linux-musl"));
        assert!(p2.is_cross_arch());
    }

    /// The portability preflight renders one line per step with the resolved
    /// verdict (R531-T4), honoring the runner's host override.
    #[test]
    fn portability_preflight_renders_one_line_per_step() {
        let pipeline = one_step_pipeline("build", vec!["true".to_string()]);
        let mut runner = PipelineRunner::new(pipeline).with_host_triple("aarch64-apple-darwin");
        // Give the single step a cross target.
        let mut steps = runner.pipeline.steps.clone();
        steps[0].platform = Some(crate::platform::PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: None,
            native: false,
        });
        runner.pipeline.steps = steps;

        let lines = runner.portability_preflight();
        assert_eq!(lines.len(), 1);
        assert!(
            lines[0].contains("targets x86_64-unknown-linux-musl")
                && lines[0].contains("host aarch64-apple-darwin")
                && lines[0].contains("NativeCross"),
            "preflight line: {}",
            lines[0]
        );
    }

    /// `native_cross_plan` (R531-F5) gates on the NativeCross verdict and a
    /// foreign (arch OR OS, R786-B1) target, then routes the step's argv to
    /// zigbuild. Only a truly host-matching target and a non-NativeCross
    /// verdict yield `None`.
    #[test]
    fn native_cross_plan_routes_foreign_target_to_zigbuild() {
        let pipeline = one_step_pipeline(
            "build",
            vec!["cross".into(), "build".into(), "--release".into()],
        );
        let runner = PipelineRunner::new(pipeline).with_host_triple("aarch64-apple-darwin");

        // Foreign-arch musl target on an arm64 mac → NativeCross tier.
        let mut foreign = runner.pipeline.steps[0].clone();
        foreign.platform = Some(crate::platform::PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: Some("linux/amd64".into()),
            native: false,
        });
        let plan = runner
            .native_cross_plan(&foreign, &crate::nativecross::ToolAvailability::FULL)
            .expect("foreign-target NativeCross step yields a plan")
            .expect("toolchain available");
        assert_eq!(plan.tool, crate::nativecross::CrossTool::CargoZigbuild);
        assert_eq!(plan.argv[1], "zigbuild");
        assert!(plan.argv.iter().any(|a| a == "x86_64-unknown-linux-musl"));

        // R786-B1: same arch, foreign OS (aarch64-unknown-linux-gnu from an
        // aarch64-apple-darwin host) is ALSO NativeCross-tier, not a plain
        // native build — Apple's `ld` can't produce an ELF binary regardless
        // of arch match (reproduced live; see nativecross.rs's
        // same_arch_foreign_os_is_not_native test). This used to assert
        // `None` on the wrong assumption that arch-match alone was enough.
        let mut foreign_os = runner.pipeline.steps[0].clone();
        foreign_os.platform = Some(crate::platform::PlatformSpec {
            target: Some("aarch64-unknown-linux-gnu".into()),
            container_platform: None,
            native: false,
        });
        let plan = runner
            .native_cross_plan(&foreign_os, &crate::nativecross::ToolAvailability::FULL)
            .expect("same-arch foreign-OS step also yields a plan")
            .expect("toolchain available");
        assert_eq!(plan.tool, crate::nativecross::CrossTool::CargoZigbuild);
        assert!(plan
            .argv
            .iter()
            .any(|a| a == "aarch64-unknown-linux-gnu"));

        // Truly host-matching target (arch AND OS) → plain native build, not
        // this tier → None.
        let mut native = runner.pipeline.steps[0].clone();
        native.platform = Some(crate::platform::PlatformSpec {
            target: Some("aarch64-apple-darwin".into()),
            container_platform: None,
            native: false,
        });
        assert!(runner
            .native_cross_plan(&native, &crate::nativecross::ToolAvailability::FULL)
            .is_none());

        // No target at all → None.
        let bare = runner.pipeline.steps[0].clone();
        assert!(runner
            .native_cross_plan(&bare, &crate::nativecross::ToolAvailability::FULL)
            .is_none());
    }

    /// Captures the argv + env a step is dispatched with, so a test can assert
    /// what the subprocess seam actually received (R531-T6).
    #[derive(Default)]
    struct CapturingExecutor {
        seen: std::sync::Mutex<Option<(Vec<String>, Vec<(String, String)>)>>,
    }

    #[async_trait::async_trait]
    impl ForgeExecutor for CapturingExecutor {
        async fn execute(
            &self,
            spec: ForgeSpec,
            ctx: ExecContext,
            _sink: Option<tokio::sync::mpsc::UnboundedSender<ExecEvent>>,
        ) -> Result<velveteen_exec::ExecOutcome, ForgeExecutorError> {
            let argv = match spec.command {
                ForgeCommand::Subprocess { argv, .. } => argv,
                _ => Vec::new(),
            };
            *self.seen.lock().unwrap() = Some((argv, ctx.env));
            Ok(velveteen_exec::ExecOutcome {
                status: ForgeStatus::Done {
                    exit_code: 0,
                    ended_at: 0,
                },
                stderr_tail: String::new(),
            })
        }
    }

    /// Build a single-step Native runner whose one step carries a cross
    /// `target`, wired to `exec` and a seeded toolchain availability — the
    /// fixture for the T6 execution-path tests.
    fn native_cross_runner(
        camp: &std::path::Path,
        argv: Vec<String>,
        target: &str,
        avail: crate::nativecross::ToolAvailability,
        exec: std::sync::Arc<CapturingExecutor>,
    ) -> PipelineRunner {
        let mut pipeline = one_step_pipeline("build-musl", argv);
        pipeline.steps[0].platform = Some(crate::platform::PlatformSpec {
            target: Some(target.to_string()),
            container_platform: None,
            native: false,
        });
        PipelineRunner::new(pipeline)
            .with_host_triple("aarch64-apple-darwin")
            .with_camp_root(camp.to_path_buf())
            .with_cross_availability(avail)
            .with_executor(exec)
    }

    /// T6 end-to-end: a NativeCross step's `cross build` argv is rewritten to
    /// `cargo zigbuild … --target T` *before* it reaches the executor.
    #[tokio::test]
    async fn execute_step_local_reroutes_native_cross_to_zigbuild() {
        let camp = tempfile::tempdir().unwrap();
        let exec = std::sync::Arc::new(CapturingExecutor::default());
        let runner = native_cross_runner(
            camp.path(),
            vec!["cross".into(), "build".into(), "--release".into()],
            "x86_64-unknown-linux-musl",
            crate::nativecross::ToolAvailability::FULL,
            exec.clone(),
        );
        let step = runner.pipeline.steps[0].clone();
        runner.execute_step_local(0, &step, None).await.unwrap();

        let (argv, _env) = exec.seen.lock().unwrap().clone().unwrap();
        assert_eq!(&argv[..2], &["cargo".to_string(), "zigbuild".to_string()]);
        assert!(argv.iter().any(|a| a == "x86_64-unknown-linux-musl"));
    }

    /// T6: with zig absent but a musl-cross toolchain present, the fallback
    /// keeps `cargo build` and injects the linker/CC/AR env.
    #[tokio::test]
    async fn execute_step_local_musl_cross_fallback_injects_linker_env() {
        let camp = tempfile::tempdir().unwrap();
        let exec = std::sync::Arc::new(CapturingExecutor::default());
        let runner = native_cross_runner(
            camp.path(),
            vec!["cargo".into(), "build".into()],
            "x86_64-unknown-linux-musl",
            crate::nativecross::ToolAvailability {
                zigbuild: false,
                musl_cross: true,
            },
            exec.clone(),
        );
        let step = runner.pipeline.steps[0].clone();
        runner.execute_step_local(0, &step, None).await.unwrap();

        let (argv, env) = exec.seen.lock().unwrap().clone().unwrap();
        assert_eq!(&argv[..2], &["cargo".to_string(), "build".to_string()]);
        assert!(
            env.iter()
                .any(|(k, _)| k == "CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER"),
            "musl-cross linker env injected: {env:?}"
        );
    }

    /// T6: a NativeCross step with no host-native toolchain installed fails
    /// with the actionable install hint instead of a raw linker error.
    #[tokio::test]
    async fn execute_step_local_fails_with_hint_when_no_toolchain() {
        let camp = tempfile::tempdir().unwrap();
        let exec = std::sync::Arc::new(CapturingExecutor::default());
        let runner = native_cross_runner(
            camp.path(),
            vec!["cross".into(), "build".into()],
            "x86_64-unknown-linux-musl",
            crate::nativecross::ToolAvailability::NONE,
            exec.clone(),
        );
        let step = runner.pipeline.steps[0].clone();

        let err = runner.execute_step_local(0, &step, None).await.unwrap_err();
        match err {
            RunnerError::StepFailed { msg, .. } => {
                assert!(msg.contains("cargo-zigbuild"), "actionable hint: {msg}");
            }
            other => panic!("expected StepFailed with hint, got {other:?}"),
        }
    }

    // ── R380-T3 runtime resolution tests ────────────────────────────────────

    /// resolve_runtime defaults from RunWhere when the step doesn't pin a
    /// runtime: local ⇒ Native, remote ⇒ Container.
    #[test]
    fn resolve_runtime_defaults_from_run_where() {
        let local_pipeline = one_step_pipeline("local", vec!["true".to_string()]);
        let local_runner = PipelineRunner::new(local_pipeline);
        assert_eq!(
            local_runner.resolve_runtime(&local_runner.pipeline.steps[0]),
            TaskRuntime::Native,
        );

        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 0,
            produced_files: HashMap::new(),
        });
        let remote_pipeline = one_step_pipeline("remote", vec!["true".to_string()]);
        let remote_runner = PipelineRunner::new_remote(remote_pipeline, scryer, yubaba);
        assert_eq!(
            remote_runner.resolve_runtime(&remote_runner.pipeline.steps[0]),
            TaskRuntime::Container,
        );
    }

    /// An explicit step.runtime always wins over the RunWhere default.
    #[test]
    fn resolve_runtime_step_override_wins() {
        let mut pipeline = one_step_pipeline("override", vec!["true".to_string()]);
        pipeline.steps[0].runtime = Some(TaskRuntime::Container);
        let runner = PipelineRunner::new(pipeline);
        assert_eq!(
            runner.resolve_runtime(&runner.pipeline.steps[0]),
            TaskRuntime::Container,
            "step.runtime=Container must override --where=local default Native",
        );
    }

    /// R590-F2: a subprocess step's `image = "<name>"` resolves to a catalog
    /// ImageRef (the R381 seam) so the argv runs inside that image; no `image`
    /// ⇒ None (driver uses the default forge image).
    #[test]
    fn step_image_override_resolves_catalog_image() {
        let mut step = mk_step("v8", &["build-v8.sh"]);
        assert!(
            step_image_override(&step).expect("no image is not an error").is_none(),
            "no image ⇒ None",
        );

        step.image = Some("rusty-v8-musl-builder".into());
        let img = step_image_override(&step)
            .expect("bare catalog name resolves")
            .expect("image override resolves");
        assert_eq!(img.registry, "ghcr.io");
        assert_eq!(img.repository, "yah-ai/rusty-v8-musl-builder");
    }

    /// R590-B5: a full `registry/repo:tag@sha256:…` ref bypasses the catalog's
    /// hard-coded `ghcr.io/yah-ai` prefix entirely and pulls from the named
    /// registry — the cr.yah.dev path for rusty-v8-musl.
    #[test]
    fn step_image_override_accepts_full_pinned_ref() {
        let mut step = mk_step("v8", &["build-v8.sh"]);
        let digest = "sha256:a1fb9d9cc631dcb844fbbb949dc65a80be1d532fa80868c4df5ed4b21939f9a4";
        step.image = Some(format!(
            "cr.yah.dev/rusty-v8-musl-builder:v149.4.0-amd64@{digest}"
        ));

        let img = step_image_override(&step)
            .expect("full ref parses")
            .expect("image override resolves");
        assert_eq!(img.registry, "cr.yah.dev");
        assert_eq!(img.repository, "rusty-v8-musl-builder");
        assert_eq!(img.tag, "v149.4.0-amd64");
        assert_eq!(img.digest, digest);
        assert!(img.is_pinned(), "a full ref carries a real digest");
        assert_eq!(
            img.pull_ref(),
            format!("cr.yah.dev/rusty-v8-musl-builder:v149.4.0-amd64@{digest}"),
            "the runtime pulls the exact published ref, not a floating :latest",
        );
    }

    /// A full ref without a digest is a config error, not a silent tag pull.
    #[test]
    fn step_image_override_rejects_unpinned_full_ref() {
        let mut step = mk_step("v8", &["build-v8.sh"]);
        step.image = Some("cr.yah.dev/rusty-v8-musl-builder:v149.4.0-amd64".into());

        let err = step_image_override(&step).expect_err("bare-tag full ref rejects");
        assert!(
            matches!(err, RunnerError::InvalidConfig(ref m) if m.contains("digest-pinned")),
            "error must name the missing pin, got {err:?}",
        );
    }

    /// Local + container routes through `task::local::local_container_command`
    /// → `docker run --rm`. The full happy-path (real docker daemon, pull a
    /// public image, exit 0) is exercised by the `#[ignore]` smoke test
    /// `task::local::tests::local_container_run_exits_with_code`.
    ///
    /// Here we only verify the run reaches the local+container branch and
    /// reports a clean step failure on environments without docker — without
    /// regressing back to the pre-T6 InvalidConfig pre-check.
    #[tokio::test]
    async fn local_container_step_routes_through_docker_path() {
        let mut pipeline = one_step_pipeline(
            "local-container",
            // bogus binary so we don't accidentally test against a real
            // docker image even if the CLI happens to be installed
            vec!["__nonexistent_binary_for_docker_test__".to_string()],
        );
        pipeline.steps[0].runtime = Some(TaskRuntime::Container);
        let runner = PipelineRunner::new(pipeline);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
        // task_run_id stays None — that field tracks remote dispatch only.
        assert!(meta.steps[0].task_run_id.is_none());
    }

    fn build_image_pipeline(image: &str) -> Pipeline {
        Pipeline {
            allow_late_operator_block: false,
            participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "image".to_string(),
            label: "Bake image".to_string(),
            steps: vec![crate::types::QedStep {
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
                name: "bake".to_string(),
                argv: Vec::new(),
                cwd: None,
                env: HashMap::new(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: crate::types::StepKind::BuildImage,
                image: Some(image.to_string()),
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
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
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

    /// build-image steps force Container regardless of run_where=Local (which
    /// would otherwise default to Native).
    #[test]
    fn build_image_step_forces_container_runtime() {
        let pipeline = build_image_pipeline("yah-rust");
        let runner = PipelineRunner::new(pipeline);
        assert_eq!(
            runner.resolve_runtime(&runner.pipeline.steps[0]),
            TaskRuntime::Container,
        );
    }

    /// Unknown catalog image surfaces as a StepFailed at dispatch time.
    #[tokio::test]
    async fn build_image_unknown_catalog_entry_fails() {
        let camp = TempDir::new().unwrap();
        let pipeline = build_image_pipeline("yah-bogus-not-real");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// Remote build-image dispatch round-trips through the BuildKit workload
    /// path (R381-T5). The scripted yubaba accepts the deploy, emits no logs,
    /// and reports exit 0; the runner surfaces a Success status and records
    /// the task_run_id of the forge run.
    ///
    /// R636-B1 added the context upload to this path, so the publisher is part
    /// of the round trip now — and the same key that was published must be
    /// discarded when the step ends, or every run leaks a tarball.
    #[tokio::test]
    async fn build_image_remote_dispatch_round_trip() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 0,
            produced_files: HashMap::new(),
        });
        let ctx = Arc::new(RecordingContextPublisher::default());
        let pipeline = build_image_pipeline("yah-rust");
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba)
            .with_camp_root(dir.path().to_path_buf())
            .with_build_context_publisher(ctx.clone());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(meta.steps[0].status, RunStatus::Success);
        assert!(
            meta.steps[0].task_run_id.is_some(),
            "remote build-image step must record its ForgeId as task_run_id",
        );

        let published: Vec<String> = ctx
            .published
            .lock()
            .unwrap()
            .iter()
            .map(|(k, _)| k.clone())
            .collect();
        assert_eq!(published.len(), 1, "one context upload per build-image step");
        assert_eq!(
            *ctx.discarded.lock().unwrap(),
            published,
            "the uploaded context must be dropped when the step ends",
        );
    }

    /// A failed build must still drop its uploaded context — a failure is
    /// precisely when the operator re-runs, and every re-run uploads a fresh
    /// key, so skipping cleanup here is how the bucket fills up.
    #[tokio::test]
    async fn build_image_remote_discards_the_context_after_a_failed_build() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec!["dockerfile parse error".into()],
            exit_code: 2,
            produced_files: HashMap::new(),
        });
        let ctx = Arc::new(RecordingContextPublisher::default());
        let runner = PipelineRunner::new_remote(build_image_pipeline("yah-rust"), scryer, yubaba)
            .with_camp_root(dir.path().to_path_buf())
            .with_build_context_publisher(ctx.clone());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(ctx.discarded.lock().unwrap().len(), 1);
    }

    /// Remote build-image surfaces a non-zero buildkit exit as a step failure.
    #[tokio::test]
    async fn build_image_remote_dispatch_failure_surfaces() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec!["dockerfile parse error".into()],
            exit_code: 2,
            produced_files: HashMap::new(),
        });
        let pipeline = build_image_pipeline("yah-rust");
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba)
            .with_camp_root(dir.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// A per-camp catalog entry that extends a nonexistent parent surfaces
    /// the compile error as a StepFailed *before* we shell to docker.
    #[tokio::test]
    async fn build_image_compile_error_surfaces_before_docker() {
        let camp = TempDir::new().unwrap();
        let images = camp.path().join(".yah/qed/images");
        std::fs::create_dir_all(&images).unwrap();
        std::fs::write(
            images.join("bad-entry.toml"),
            r#"
[image]
name        = "bad-entry"
extends     = "does-not-exist"
description = "extends a typo"
"#,
        )
        .unwrap();

        let pipeline = build_image_pipeline("bad-entry");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
        // No docker artifacts should have been written.
        assert!(!camp.path().join(".yah/cache/buildkit").exists());
    }

    /// tag_to_filename replaces characters that aren't safe for OCI archive
    /// filenames (slashes from registry/repo, colons from tags).
    #[test]
    fn tag_to_filename_makes_oci_archive_path_safe() {
        assert_eq!(tag_to_filename("yah-rust:dev"), "yah-rust_dev");
        assert_eq!(
            tag_to_filename("ghcr.io/yah-ai/yah-python:v1.2.3"),
            "ghcr.io_yah-ai_yah-python_v1.2.3",
        );
    }

    /// End-to-end smoke: build a one-line Dockerfile via the full qed →
    /// task::local::build_image_command path. Requires docker + buildx on
    /// PATH; marked #[ignore] so CI without docker doesn't fail.
    ///
    /// Run locally:
    /// ```sh
    /// cargo test -p qed --lib build_image_local_buildx_actually_builds -- --include-ignored
    /// ```
    #[tokio::test]
    #[ignore]
    async fn build_image_local_buildx_actually_builds() {
        let camp = TempDir::new().unwrap();
        let images = camp.path().join(".yah/qed/images/yah-smoke");
        std::fs::create_dir_all(&images).unwrap();
        // Tiny Dockerfile that should build in a couple seconds against alpine.
        std::fs::write(
            images.join("Dockerfile"),
            "FROM alpine:3\nRUN echo smoke-image\n",
        )
        .unwrap();
        std::fs::write(
            images.join("image.toml"),
            r#"
[image]
name        = "yah-smoke"
base        = "alpine:3"
description = "smoke test image"
"#,
        )
        .unwrap();

        let pipeline = build_image_pipeline("yah-smoke");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Success,
            "build-image should succeed; check docker buildx is available"
        );
        // Generated Dockerfile staged under cache/buildkit.
        assert!(camp
            .path()
            .join(".yah/cache/buildkit/yah-smoke.Dockerfile")
            .is_file());
        // OCI archive should be produced (push=false default).
        assert!(camp
            .path()
            .join(".yah/cache/images/yah-smoke_dev.tar")
            .is_file());
    }

    // ── R407-T2 package-native-tarball runner tests ─────────────────────────

    /// Build a pipeline that packages a pre-built binary into a native
    /// tarball. The test always writes a dummy binary at `binary_rel` so we
    /// don't depend on a real cross build.
    fn package_native_tarball_pipeline(image: &str, binary_rel: &str, triple: &str) -> Pipeline {
        Pipeline {
            allow_late_operator_block: false,
            participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "pack".to_string(),
            label: "Package native tarball".to_string(),
            steps: vec![crate::types::QedStep {
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
                name: "pack".to_string(),
                argv: Vec::new(),
                cwd: None,
                env: HashMap::new(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: crate::types::StepKind::PackageNativeTarball,
                image: Some(image.to_string()),
                tag: None,
                push: false,
                platforms: Vec::new(),
                binary_path: Some(binary_rel.to_string()),
                triple: Some(triple.to_string()),
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
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
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

    fn stage_native_tarball_camp(image_name: &str, produces: &str, binary_rel: &str) -> TempDir {
        let camp = TempDir::new().unwrap();
        let images = camp.path().join(".yah/qed/images");
        std::fs::create_dir_all(&images).unwrap();
        std::fs::write(
            images.join(format!("{image_name}.toml")),
            format!(
                r#"
[image]
name        = "{image_name}"
base        = "scratch"
description = "Native musl-static workload"
produces    = [{produces}]

[image.env]
RUST_LOG = "info"
"#,
            ),
        )
        .unwrap();
        let bin_path = camp.path().join(binary_rel);
        std::fs::create_dir_all(bin_path.parent().unwrap()).unwrap();
        std::fs::write(&bin_path, b"\x7fELF-fake-musl-binary").unwrap();
        camp
    }

    /// Happy path: catalog entry declares `native-tarball`, binary exists,
    /// runner emits `.yah/cache/native/<image>-<triple>.tar.gz`.
    #[tokio::test]
    async fn package_native_tarball_writes_tar_gz_with_manifest() {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let binary_rel = "target/x86_64-unknown-linux-musl/release/yubaba";
        let triple = "x86_64-unknown-linux-musl";
        let camp = stage_native_tarball_camp("yah-yubaba", "\"native-tarball\"", binary_rel);

        let pipeline = package_native_tarball_pipeline("yah-yubaba", binary_rel, triple);
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let out = camp
            .path()
            .join(".yah/cache/native/yah-yubaba-x86_64-unknown-linux-musl.tar.gz");
        assert!(out.is_file(), "tarball at {}", out.display());

        let f = std::fs::File::open(&out).unwrap();
        let gz = GzDecoder::new(f);
        let mut archive = tar::Archive::new(gz);
        let mut seen: Vec<(String, Vec<u8>)> = Vec::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().into_owned();
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf).unwrap();
            seen.push((path, buf));
        }
        seen.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(seen[0].0, "bin/yubaba");
        assert_eq!(seen[0].1, b"\x7fELF-fake-musl-binary");
        assert_eq!(seen[1].0, "manifest.toml");
        let text = std::str::from_utf8(&seen[1].1).unwrap();
        let manifest: crate::native::NativeTarballManifest =
            toml::from_str(text).expect("manifest.toml parses");
        assert_eq!(manifest.name, "yah-yubaba");
        assert_eq!(manifest.triple, triple);
        assert_eq!(manifest.binary, "bin/yubaba");
        // Catalog env propagates into the manifest.
        assert_eq!(
            manifest.env.get("RUST_LOG").map(String::as_str),
            Some("info")
        );
    }

    /// Catalog entry that only declares `produces = ["oci-image"]` (the
    /// default) is rejected at dispatch time — protects against accidentally
    /// packaging a non-musl image as a native tarball.
    #[tokio::test]
    async fn package_native_tarball_rejects_non_native_catalog_entry() {
        let binary_rel = "target/release/yubaba";
        let camp = stage_native_tarball_camp("yah-yubaba", "\"oci-image\"", binary_rel);
        let pipeline = package_native_tarball_pipeline("yah-yubaba", binary_rel, "darwin-aarch64");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// Both-target entries (`["oci-image", "native-tarball"]`) are accepted —
    /// W154's container-and-native peer model.
    #[tokio::test]
    async fn package_native_tarball_accepts_both_targets_entry() {
        let binary_rel = "target/x86_64-unknown-linux-musl/release/yubaba";
        let camp = stage_native_tarball_camp(
            "yah-yubaba",
            "\"oci-image\", \"native-tarball\"",
            binary_rel,
        );
        let pipeline =
            package_native_tarball_pipeline("yah-yubaba", binary_rel, "x86_64-unknown-linux-musl");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert!(camp
            .path()
            .join(".yah/cache/native/yah-yubaba-x86_64-unknown-linux-musl.tar.gz")
            .is_file());
    }

    /// Unknown catalog name surfaces as StepFailed (mirrors build-image
    /// dispatch shape).
    #[tokio::test]
    async fn package_native_tarball_unknown_catalog_fails() {
        let camp = TempDir::new().unwrap();
        let bin = camp.path().join("target/release/yubaba");
        std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
        std::fs::write(&bin, b"x").unwrap();
        let pipeline = package_native_tarball_pipeline(
            "yah-bogus-not-real",
            "target/release/yubaba",
            "darwin-aarch64",
        );
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
    }

    /// Missing binary surfaces a clean StepFailed (not an IO panic).
    #[tokio::test]
    async fn package_native_tarball_missing_binary_fails_cleanly() {
        let camp = TempDir::new().unwrap();
        let images = camp.path().join(".yah/qed/images");
        std::fs::create_dir_all(&images).unwrap();
        std::fs::write(
            images.join("yah-yubaba.toml"),
            r#"
[image]
name        = "yah-yubaba"
base        = "scratch"
description = "Native"
produces    = ["native-tarball"]
"#,
        )
        .unwrap();
        let pipeline = package_native_tarball_pipeline(
            "yah-yubaba",
            "target/x86_64-unknown-linux-musl/release/yubaba",
            "x86_64-unknown-linux-musl",
        );
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        // Nothing should have landed under .yah/cache/native.
        assert!(!camp.path().join(".yah/cache/native").exists());
    }

    /// Triple defaults to the build host when omitted — proves
    /// `publish::resolve_triple(None)` is the fallback used at packaging time.
    #[tokio::test]
    async fn package_native_tarball_triple_defaults_to_host() {
        let binary_rel = "target/release/yubaba";
        let camp = stage_native_tarball_camp("yah-yubaba", "\"native-tarball\"", binary_rel);

        // Same pipeline but with triple=None.
        let mut pipeline = package_native_tarball_pipeline("yah-yubaba", binary_rel, "ignored");
        pipeline.steps[0].triple = None;

        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let host_triple = crate::publish::resolve_triple(None);
        let expected = camp
            .path()
            .join(format!(".yah/cache/native/yah-yubaba-{host_triple}.tar.gz"));
        assert!(
            expected.is_file(),
            "expected {} to exist",
            expected.display()
        );
    }

    /// PackageNativeTarball is always Native runtime, even on a Remote runner —
    /// the implicit `None` must not get auto-forced to Container.
    #[test]
    fn package_native_tarball_step_forces_native_runtime_on_remote() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 0,
            produced_files: HashMap::new(),
        });
        let pipeline = package_native_tarball_pipeline(
            "yah-yubaba",
            "target/x86_64-unknown-linux-musl/release/yubaba",
            "x86_64-unknown-linux-musl",
        );
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba);
        assert_eq!(
            runner.resolve_runtime(&runner.pipeline.steps[0]),
            TaskRuntime::Native,
        );
    }

    // ── R407-T3 musl-static-preflight runner tests ──────────────────────────

    fn musl_preflight_pipeline(package: &str) -> Pipeline {
        Pipeline {
            allow_late_operator_block: false,
            participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "preflight".to_string(),
            label: "musl-static preflight".to_string(),
            steps: vec![crate::types::QedStep {
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
                name: "musl-gate".to_string(),
                argv: Vec::new(),
                cwd: None,
                env: HashMap::new(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: crate::types::StepKind::MuslStaticPreflight,
                image: None,
                tag: None,
                push: false,
                platforms: Vec::new(),
                binary_path: None,
                triple: None,
                package: Some(package.to_string()),
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
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
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

    fn workspace_root() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .find(|p| p.join("Cargo.lock").is_file())
            .expect("workspace root has Cargo.lock")
            .to_path_buf()
    }

    /// Happy path: gating the yah-qed crate itself passes — it's musl-clean
    /// by design (no openssl-sys, no dbus, no cuda).
    #[tokio::test]
    async fn musl_static_preflight_passes_clean_workspace_package() {
        let pipeline = musl_preflight_pipeline("yah-qed");
        let runner = PipelineRunner::new(pipeline).with_camp_root(workspace_root());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
    }

    /// Unknown workspace package surfaces a clean StepFailed (not a panic).
    #[tokio::test]
    async fn musl_static_preflight_unknown_package_fails_cleanly() {
        let pipeline = musl_preflight_pipeline("definitely-not-a-real-package");
        let runner = PipelineRunner::new(pipeline).with_camp_root(workspace_root());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// MuslStaticPreflight is always Native runtime, even on a Remote runner.
    #[test]
    fn musl_static_preflight_forces_native_runtime_on_remote() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 0,
            produced_files: HashMap::new(),
        });
        let pipeline = musl_preflight_pipeline("yubaba");
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba);
        assert_eq!(
            runner.resolve_runtime(&runner.pipeline.steps[0]),
            TaskRuntime::Native,
        );
    }

    /// The actionable container-fallback hint surfaces in the step's failure
    /// message — operators reading the failed StepStatus get the routing
    /// recommendation immediately.
    #[test]
    fn musl_gate_error_message_routes_to_container_fallback() {
        use crate::preflight::{check_dep_list, MuslPreflightError};
        let err = check_dep_list("yubaba", ["openssl-sys"]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("container fallback"),
            "msg routes to container: {msg}"
        );
        assert!(
            msg.contains("runtime = \"container\""),
            "msg names the toml fix: {msg}"
        );
        assert!(
            matches!(err, MuslPreflightError::NotMuslSafe { ref offenders, .. } if offenders == &["openssl-sys".to_string()]),
        );
    }

    // ── R407-T5 sign-native-tarball runner tests ────────────────────────────

    /// Build a pipeline that packages then signs a native tarball, exercising
    /// the same image+triple → on-disk-path convention both steps share.
    fn pack_and_sign_pipeline(image: &str, binary_rel: &str, triple: &str) -> Pipeline {
        Pipeline {
            allow_late_operator_block: false,
            participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "pack-and-sign".to_string(),
            label: "Package + sign native tarball".to_string(),
            steps: vec![
                crate::types::QedStep {
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
                    name: "pack".to_string(),
                    argv: Vec::new(),
                    cwd: None,
                    env: HashMap::new(),
                    timeout: None,
                    on_fail: OnFail::Abort,
                    produces: Vec::new(),
                    runtime: None,
                    kind: crate::types::StepKind::PackageNativeTarball,
                    image: Some(image.to_string()),
                    tag: None,
                    push: false,
                    platforms: Vec::new(),
                    binary_path: Some(binary_rel.to_string()),
                    triple: Some(triple.to_string()),
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
                },
                crate::types::QedStep {
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
                    name: "sign".to_string(),
                    argv: Vec::new(),
                    cwd: None,
                    env: HashMap::new(),
                    timeout: None,
                    on_fail: OnFail::Abort,
                    produces: Vec::new(),
                    runtime: None,
                    kind: crate::types::StepKind::SignNativeTarball,
                    image: Some(image.to_string()),
                    tag: None,
                    push: false,
                    platforms: Vec::new(),
                    binary_path: None,
                    triple: Some(triple.to_string()),
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
                },
            ],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
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

    /// Sign-only pipeline (no pack step) — for asserting the "tarball must
    /// already exist" gate without coupling to the packaging step.
    fn sign_only_pipeline(image: &str, triple: &str) -> Pipeline {
        Pipeline {
            allow_late_operator_block: false,
            participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "sign".to_string(),
            label: "Sign native tarball".to_string(),
            steps: vec![crate::types::QedStep {
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
                name: "sign".to_string(),
                argv: Vec::new(),
                cwd: None,
                env: HashMap::new(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: crate::types::StepKind::SignNativeTarball,
                image: Some(image.to_string()),
                tag: None,
                push: false,
                platforms: Vec::new(),
                binary_path: None,
                triple: Some(triple.to_string()),
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
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
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

    /// Happy path: pack-then-sign in one pipeline writes the tarball and
    /// then `.sig`, `.crt`, `.bundle` next to it. Uses the default
    /// LoggingSigner — exercising the same trust shape as cosign without
    /// requiring a cosign install in the test sandbox.
    #[tokio::test]
    async fn sign_native_tarball_pack_then_sign_writes_sig_crt_bundle() {
        let binary_rel = "target/x86_64-unknown-linux-musl/release/yubaba";
        let triple = "x86_64-unknown-linux-musl";
        let camp = stage_native_tarball_camp("yah-yubaba", "\"native-tarball\"", binary_rel);

        let pipeline = pack_and_sign_pipeline("yah-yubaba", binary_rel, triple);
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(meta.steps[0].status, RunStatus::Success); // pack
        assert_eq!(meta.steps[1].status, RunStatus::Success); // sign

        let tarball = camp
            .path()
            .join(".yah/cache/native/yah-yubaba-x86_64-unknown-linux-musl.tar.gz");
        assert!(tarball.is_file());
        for suffix in [".sig", ".crt", ".bundle"] {
            let mut name = tarball.file_name().unwrap().to_os_string();
            name.push(suffix);
            let p = tarball.with_file_name(name);
            assert!(p.is_file(), "expected {} to exist", p.display());
        }
    }

    /// Catalog entry without `native-tarball` in `produces` is refused at
    /// sign time — same gate as packaging, applied independently so a
    /// signing step picked up from old TOML can't sneak through.
    #[tokio::test]
    async fn sign_native_tarball_rejects_non_native_catalog_entry() {
        let binary_rel = "target/release/yubaba";
        let camp = stage_native_tarball_camp("yah-yubaba", "\"oci-image\"", binary_rel);
        let pipeline = sign_only_pipeline("yah-yubaba", "x86_64-unknown-linux-musl");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// Unknown catalog name surfaces as StepFailed (mirrors packaging dispatch).
    #[tokio::test]
    async fn sign_native_tarball_unknown_catalog_fails() {
        let camp = TempDir::new().unwrap();
        let pipeline = sign_only_pipeline("yah-bogus-not-real", "x86_64-unknown-linux-musl");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
    }

    /// Missing tarball (sign called without pack) surfaces a clean StepFailed
    /// whose message routes the operator to the packaging step.
    #[tokio::test]
    async fn sign_native_tarball_missing_tarball_routes_to_packaging() {
        let camp = TempDir::new().unwrap();
        let images = camp.path().join(".yah/qed/images");
        std::fs::create_dir_all(&images).unwrap();
        std::fs::write(
            images.join("yah-yubaba.toml"),
            r#"
[image]
name        = "yah-yubaba"
base        = "scratch"
description = "Native"
produces    = ["native-tarball"]
"#,
        )
        .unwrap();
        let pipeline = sign_only_pipeline("yah-yubaba", "x86_64-unknown-linux-musl");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        // Nothing should have been signed.
        assert!(!camp.path().join(".yah/cache/native").exists());
    }

    /// SignNativeTarball is always Native runtime, even on a Remote runner —
    /// the implicit `None` must not get auto-forced to Container.
    #[test]
    fn sign_native_tarball_forces_native_runtime_on_remote() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 0,
            produced_files: HashMap::new(),
        });
        let pipeline = sign_only_pipeline("yah-yubaba", "x86_64-unknown-linux-musl");
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba);
        assert_eq!(
            runner.resolve_runtime(&runner.pipeline.steps[0]),
            TaskRuntime::Native,
        );
    }

    /// `with_signer(...)` replaces the default LoggingSigner — release CI
    /// uses this seam to wire a real CosignSigner.
    #[tokio::test]
    async fn sign_native_tarball_uses_attached_signer() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingSigner {
            calls: AtomicUsize,
        }
        #[async_trait]
        impl SigstoreSigner for CountingSigner {
            async fn sign_blob(
                &self,
                blob_path: &std::path::Path,
            ) -> std::io::Result<crate::native::SignedBlob> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                // Mirror the LoggingSigner shape so the runner's success log
                // remains coherent.
                crate::native::LoggingSigner.sign_blob(blob_path).await
            }
        }

        let binary_rel = "target/x86_64-unknown-linux-musl/release/yubaba";
        let triple = "x86_64-unknown-linux-musl";
        let camp = stage_native_tarball_camp("yah-yubaba", "\"native-tarball\"", binary_rel);

        let signer = Arc::new(CountingSigner {
            calls: AtomicUsize::new(0),
        });
        let pipeline = pack_and_sign_pipeline("yah-yubaba", binary_rel, triple);
        let runner = PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .with_signer(signer.clone());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(signer.calls.load(Ordering::SeqCst), 1);
    }

    // ─── SubPipeline recursion (R488-F2) ────────────────────────────────────

    use crate::types::{
        ProducedArtifact, SubPipelineCollect, SubPipelineConfig, SubPipelineRef,
        SubPipelineResolver,
    };

    /// In-memory resolver — maps a ref-token string to a Pipeline. The same
    /// token discipline the walker uses, so resolver + walker stay aligned.
    struct MapResolver(std::collections::HashMap<String, Pipeline>);

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

    fn shell_step(name: &str, argv: Vec<&str>) -> crate::types::QedStep {
        crate::types::QedStep {
            expect_slow: false,
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
            argv: argv.into_iter().map(String::from).collect(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: crate::types::StepKind::Subprocess,
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
            participant: None,
        }
    }

    // ----- R744-T2 `base_env` ------------------------------------------------

    /// The point of the field: a local step sees the embedder's env without the
    /// recipe naming it. This is what carries the host's absolute `SCCACHE_DIR`
    /// into a step whose cwd is a worktree copy — the case where letting the
    /// tree answer "where is the cache" pinned a per-user sccache server to a
    /// `$TMPDIR` path for every camp on the machine.
    #[tokio::test]
    async fn base_env_reaches_a_local_step() {
        let camp = tempfile::tempdir().unwrap();
        let step = shell_step("probe", vec!["sh", "-c", "printf %s \"$R744_PROBE\" > out"]);
        let meta = PipelineRunner::new(make_pipeline("base-env", vec![step]))
            .with_camp_root(camp.path().to_path_buf())
            .with_base_env(vec![("R744_PROBE".into(), "from-host".into())])
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            std::fs::read_to_string(camp.path().join("out")).unwrap(),
            "from-host",
        );
    }

    /// …and loses to the step's own `env`. The recipe is closer to the work
    /// than the host is, so a pipeline that spells a key out has said something
    /// the embedder's default must not quietly overrule.
    #[tokio::test]
    async fn a_steps_own_env_outranks_base_env() {
        let camp = tempfile::tempdir().unwrap();
        let mut step = shell_step("probe", vec!["sh", "-c", "printf %s \"$R744_PROBE\" > out"]);
        step.env
            .insert("R744_PROBE".to_string(), "from-step".to_string());
        let meta = PipelineRunner::new(make_pipeline("base-env-prec", vec![step]))
            .with_camp_root(camp.path().to_path_buf())
            .with_base_env(vec![("R744_PROBE".into(), "from-host".into())])
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            std::fs::read_to_string(camp.path().join("out")).unwrap(),
            "from-step",
        );
    }

    /// Build a single-axis `[pipeline.matrix]` for the tests below.
    fn one_axis_matrix(key: &str, values: &[&str]) -> crate::matrix::MatrixSpec {
        let mut dimensions = indexmap::IndexMap::new();
        dimensions.insert(
            key.to_string(),
            values
                .iter()
                .map(|v| toml::Value::String((*v).to_string()))
                .collect::<Vec<_>>(),
        );
        crate::matrix::MatrixSpec {
            dimensions,
            include: Vec::new(),
            exclude: Vec::new(),
        }
    }

    /// THE REGRESSION TEST FOR THE DEFECT THAT BLOCKED `yah-release-wizard`.
    ///
    /// `matrix::plan` had only top-level-run callers, so a pipeline reached as a
    /// sub-pipeline CHILD kept every `${{ matrix.* }}` as a literal. The visible
    /// symptom was placement, not argv: the literal sat in `platform.target`,
    /// never equalled the host triple, and `fleet_portability_gate` refused
    /// `yah-desktop-release` at kick with all 8 steps "routed to the fleet" —
    /// for a recipe whose matrix is one row naming the camp host's own arch.
    ///
    /// Asserted through argv because that is observable without a fleet: if the
    /// child's matrix expands, the step writes `aarch64-apple-darwin`; if it
    /// does not, it writes the placeholder and this test fails with the literal
    /// in the diff, naming the bug.
    #[tokio::test]
    async fn a_sub_pipeline_childs_own_matrix_is_expanded() {
        let camp = tempfile::tempdir().unwrap();
        let mut child = make_pipeline(
            "inner",
            vec![shell_step(
                "probe",
                vec!["sh", "-c", "printf %s '${{ matrix.target }}' > out"],
            )],
        );
        child.matrix = Some(one_axis_matrix("target", &["aarch64-apple-darwin"]));
        let resolver = MapResolver([("builtin:inner".to_string(), child)].into_iter().collect());
        let parent = make_pipeline(
            "outer",
            vec![sub_step(
                "nested",
                SubPipelineRef::Builtin("inner".into()),
                false,
            )],
        );

        let meta = PipelineRunner::new(parent)
            .with_camp_root(camp.path().to_path_buf())
            .with_sub_pipeline_resolver(Arc::new(resolver))
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            std::fs::read_to_string(camp.path().join("out")).unwrap(),
            "aarch64-apple-darwin",
            "the child's own [pipeline.matrix] must be expanded before it runs",
        );
    }

    /// A multi-row child REFUSES rather than silently running row 1. Running one
    /// row of three would produce a run that looks complete and is not — the
    /// failure mode worth being loud about, since the caller is usually a
    /// release wizard whose next step publishes.
    #[tokio::test]
    async fn a_multi_row_sub_pipeline_child_refuses_instead_of_picking_a_row() {
        let camp = tempfile::tempdir().unwrap();
        let mut child = make_pipeline(
            "inner",
            vec![shell_step("probe", vec!["sh", "-c", "true"])],
        );
        child.matrix = Some(one_axis_matrix(
            "target",
            &["aarch64-apple-darwin", "x86_64-apple-darwin"],
        ));
        let resolver = MapResolver([("builtin:inner".to_string(), child)].into_iter().collect());
        let parent = make_pipeline(
            "outer",
            vec![sub_step(
                "nested",
                SubPipelineRef::Builtin("inner".into()),
                false,
            )],
        );

        let meta = PipelineRunner::new(parent)
            .with_camp_root(camp.path().to_path_buf())
            .with_sub_pipeline_resolver(Arc::new(resolver))
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let err = meta
            .steps
            .iter()
            .find_map(|s| s.error.clone())
            .unwrap_or_default();
        assert!(
            err.contains("2 matrix rows"),
            "the refusal must say how many rows it found; got: {err}",
        );
    }

    /// Inheritance, which is the half that regresses silently: a `cargo` step
    /// buried in a sub-pipeline runs on this same host and needs the same env.
    /// `release-wizard → release-check → check → cargo-test` is three levels
    /// deep, and it is the level that actually compiles.
    #[tokio::test]
    async fn a_sub_pipeline_child_inherits_base_env() {
        let camp = tempfile::tempdir().unwrap();
        let child = make_pipeline(
            "inner",
            vec![shell_step(
                "probe",
                vec!["sh", "-c", "printf %s \"$R744_PROBE\" > out"],
            )],
        );
        let resolver = MapResolver([("builtin:inner".to_string(), child)].into_iter().collect());
        let parent = make_pipeline(
            "outer",
            vec![sub_step(
                "nested",
                SubPipelineRef::Builtin("inner".into()),
                false,
            )],
        );

        let meta = PipelineRunner::new(parent)
            .with_camp_root(camp.path().to_path_buf())
            .with_sub_pipeline_resolver(Arc::new(resolver))
            .with_base_env(vec![("R744_PROBE".into(), "from-host".into())])
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            std::fs::read_to_string(camp.path().join("out")).unwrap(),
            "from-host",
        );
    }

    // ----- R717-T1 `inputs` / R717-T2 `secret`, at the runner ----------------

    /// The W257 stale-ISO relation, end to end: a step declares its sources, the
    /// runner pins them, editing one afterwards makes the recorded result stale.
    /// This is the whole point of the field — freshness is what prose cannot
    /// track ("the only guard is habit") and a content hash can.
    #[tokio::test]
    async fn declared_inputs_are_pinned_before_the_step_runs() {
        let camp = tempfile::tempdir().unwrap();
        std::fs::write(camp.path().join("worker.cfg"), b"no_boot = false\n").unwrap();

        let mut step = shell_step("build-iso", vec!["true"]);
        step.inputs = vec![std::path::PathBuf::from("worker.cfg")];
        let meta = PipelineRunner::new(make_pipeline("iso", vec![step]))
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let pinned = &meta.steps[0].input_hashes;
        assert_eq!(pinned.len(), 1, "one declared input, one digest");
        assert_eq!(
            crate::staleness::input_freshness(
                pinned,
                &crate::staleness::hash_declared_inputs(
                    camp.path(),
                    &[std::path::PathBuf::from("worker.cfg")]
                )
            ),
            crate::staleness::InputFreshness::Fresh
        );

        // Fixing the `no_boot` preseed bug is exactly this edit.
        std::fs::write(camp.path().join("worker.cfg"), b"no_boot = true\n").unwrap();
        assert_eq!(
            crate::staleness::input_freshness(
                pinned,
                &crate::staleness::hash_declared_inputs(
                    camp.path(),
                    &[std::path::PathBuf::from("worker.cfg")]
                )
            ),
            crate::staleness::InputFreshness::Stale {
                changed: vec!["worker.cfg".to_string()],
            },
            "the recorded run is now about bytes that no longer exist in the tree"
        );
    }

    /// A step that pins nothing records nothing — no empty map churn in the
    /// ~494 run journals already on disk, and `Unrecorded` stays meaningful.
    #[tokio::test]
    async fn a_step_declaring_no_inputs_records_no_digests() {
        let camp = tempfile::tempdir().unwrap();
        let meta = PipelineRunner::new(make_pipeline("p", vec![shell_step("s", vec!["true"])]))
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert!(meta.steps[0].input_hashes.is_empty());
    }

    /// The R717-T2 verify, mechanized: run a `secret` step that emits a
    /// recognizable string on both streams, then look for it in everything that
    /// would reach `.yah/jit/qed/` — the event stream (which becomes
    /// `<run_id>.events.jsonl`) and the terminal meta (`<run_id>.json`).
    #[tokio::test]
    async fn a_secret_step_emits_nothing_into_either_on_disk_sink() {
        const MATERIAL: &str = "KEKMATERIAL-a3f19c02";
        let camp = tempfile::tempdir().unwrap();

        // The material comes from a FILE, never from argv. That is the honest
        // shape of the motivating case (W296's `kek-push` scps a key file) and
        // it separates the two halves of the contract: `secret` suppresses what
        // a step PRODUCES, and deliberately does not hide what the step IS.
        std::fs::write(camp.path().join("cluster.kek"), MATERIAL).unwrap();
        let mut step = shell_step(
            "kek-push",
            vec![
                "sh",
                "-c",
                // stdout, stderr, AND a captured output — all three sinks at once.
                "cat cluster.kek; \
                 cat cluster.kek >&2; \
                 echo leaked=\"$(cat cluster.kek)\" >> \"$YAH_OUTPUTS\"",
            ],
        );
        step.secret = true;
        // No declared `outputs` — validate() rejects that pair (see
        // `secret_cannot_declare_outputs`). The step still WRITES an undeclared
        // key, which is the case that has to be dropped silently rather than
        // failing the step.
        assert!(step.validate().is_ok());

        let (tx, mut rx) = mpsc::unbounded_channel();
        let meta = PipelineRunner::new(make_pipeline("kek", vec![step]))
            .with_camp_root(camp.path().to_path_buf())
            .with_events(tx)
            .run()
            .await
            .unwrap();

        // Exit status and timings survive — that is the whole contract, and the
        // step must behave IDENTICALLY: `secret` changes what is recorded, never
        // whether the step works. (An earlier cut withheld $YAH_OUTPUTS
        // entirely, which turned `>> "$YAH_OUTPUTS"` into `>> ""` and flipped
        // this step to Failed. That is the regression this assert pins.)
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(meta.steps[0].status, RunStatus::Success);
        assert!(meta.steps[0].started_at.is_some());
        assert!(meta.steps[0].completed_at.is_some());
        assert!(
            meta.steps[0].outputs.is_empty(),
            "a secret step is a sink, not a source: {:?}",
            meta.steps[0].outputs
        );

        let journal = serde_json::to_string(&meta).unwrap();
        assert!(!journal.contains(MATERIAL), "leaked into <run_id>.json");

        let mut events = String::new();
        while let Ok(ev) = rx.try_recv() {
            events.push_str(&format!("{ev:?}"));
        }
        assert!(
            !events.contains(MATERIAL),
            "leaked into the event stream (which becomes <run_id>.events.jsonl)"
        );
        // The step is still identifiable — `secret` hides what it PRODUCED, not
        // what it IS, or a failing secret step would be undebuggable.
        assert!(events.contains("kek-push"), "step identity is still emitted");
    }

    /// The same string on the same commands, without the flag — proving the test
    /// above is measuring the flag rather than a shell that emitted nothing.
    #[tokio::test]
    async fn the_same_step_without_secret_does_reach_the_sinks() {
        const MATERIAL: &str = "KEKMATERIAL-a3f19c02";
        let camp = tempfile::tempdir().unwrap();
        std::fs::write(camp.path().join("cluster.kek"), MATERIAL).unwrap();
        let step = shell_step(
            "loud",
            vec![
                "sh",
                "-c",
                "cat cluster.kek; echo leaked=\"$(cat cluster.kek)\" >> \"$YAH_OUTPUTS\"",
            ],
        );

        let (tx, mut rx) = mpsc::unbounded_channel();
        let meta = PipelineRunner::new(make_pipeline("loud", vec![step]))
            .with_camp_root(camp.path().to_path_buf())
            .with_events(tx)
            .run()
            .await
            .unwrap();

        assert_eq!(
            meta.steps[0].outputs.get("leaked").map(String::as_str),
            Some(MATERIAL)
        );
        let mut events = String::new();
        while let Ok(ev) = rx.try_recv() {
            events.push_str(&format!("{ev:?}"));
        }
        assert!(events.contains(MATERIAL));
    }

    /// A failing secret step gets a REPLACEMENT reason, not a blank one — a card
    /// that says "failed" with nothing attached gets read as a qed bug, and the
    /// next person debugs the runner instead of the step.
    #[tokio::test]
    async fn a_failing_secret_step_reports_redacted_rather_than_its_stderr_tail() {
        const MATERIAL: &str = "KEKMATERIAL-a3f19c02";
        let camp = tempfile::tempdir().unwrap();
        std::fs::write(camp.path().join("cluster.kek"), MATERIAL).unwrap();
        let mut step = shell_step(
            "kek-push",
            vec!["sh", "-c", "cat cluster.kek >&2; exit 3"],
        );
        step.secret = true;

        let meta = PipelineRunner::new(make_pipeline("kek", vec![step]))
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();

        assert_eq!(meta.steps[0].status, RunStatus::Failed);
        assert_eq!(
            meta.steps[0].error.as_deref(),
            Some(SECRET_STEP_REDACTED),
            "the tail is replaced, not dropped"
        );
        assert!(!serde_json::to_string(&meta).unwrap().contains(MATERIAL));
    }

    /// R717-T3: a doc-launched run carries what it was ABOUT on its terminal
    /// meta, which is what makes any derived cell index rebuildable by rescan.
    #[tokio::test]
    async fn a_cell_ref_reaches_the_terminal_run_meta() {
        let camp = tempfile::tempdir().unwrap();
        let params = HashMap::from([("node".to_string(), "us-west-003".to_string())]);
        let cell = crate::types::CellRef {
            doc: ".yah/docs/working/W257-static-node-fleet-onboarding.md".into(),
            cell_id: "probe-identity".into(),
            param_fingerprint: crate::types::param_fingerprint(&params),
        };

        let meta = PipelineRunner::new(make_pipeline("W257", vec![shell_step("probe", vec!["true"])]))
            .with_camp_root(camp.path().to_path_buf())
            .with_cell(cell.clone())
            .run()
            .await
            .unwrap();

        assert_eq!(meta.cell.as_ref(), Some(&cell));
        // An ordinary run is untouched — `cell` is opt-in, not a new default.
        let plain = PipelineRunner::new(make_pipeline("p", vec![shell_step("s", vec!["true"])]))
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert!(plain.cell.is_none());
    }

    fn producing_step(name: &str, binary: &str, path: &str) -> crate::types::QedStep {
        let mut s = shell_step(name, vec!["true"]);
        s.produces = vec![ProducedArtifact {
            binary: binary.into(),
            path: path.into(),
            triple: None,
        }];
        s
    }

    /// R603-B6: `QedStep::timeout` is SECONDS. The runner used to lower it with
    /// `Millis::from_ms`, so rusty-v8-musl's `timeout = 9000` ("2.5h cap") became 9
    /// seconds and killed every long remote step at 1/1000th of its budget —
    /// the rusty-v8 build died at ~9s after ~57min of real work on the worker.
    /// Latent locally only because the local driver never enforces
    /// `spec.timeout`. Lock the unit at the lowering boundary.
    #[test]
    fn step_timeout_is_seconds_not_millis() {
        let mut s = shell_step("build-v8-musl", vec!["true"]);
        s.timeout = Some(9000); // rusty-v8-musl's real value: a 2.5h cap
        let spec = build_subprocess_spec(&s, TaskRuntime::Container, None);
        assert_eq!(
            spec.timeout.expect("timeout lowered").as_ms(),
            9_000_000,
            "9000s must lower to 9_000_000ms (2.5h); from_ms would give 9000ms = 9s"
        );

        // No timeout stays absent (unbounded), not zero.
        let none = shell_step("no-budget", vec!["true"]);
        assert!(build_subprocess_spec(&none, TaskRuntime::Native, None)
            .timeout
            .is_none());
    }

    fn sub_step(
        name: &str,
        target: SubPipelineRef,
        propagate_produces: bool,
    ) -> crate::types::QedStep {
        crate::types::QedStep {
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
            kind: crate::types::StepKind::SubPipeline,
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
                propagate: SubPipelineCollect {
                    produces: propagate_produces,
                    outputs: Vec::new(),
                },
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

    fn make_pipeline(name: &str, steps: Vec<crate::types::QedStep>) -> Pipeline {
        Pipeline {
            allow_late_operator_block: false,
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
            environment: crate::types::Environment::Any,
            // Test fixtures run in throwaway tempdirs that aren't real git
            // checkouts, so use Live (build the tree as-is) — the default
            // Checkout mode would try `git checkout main` and fail. Workspace
            // positioning itself is covered by the dedicated WorkspaceMode tests.
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
            participants: None,
        }
    }

    // ── W224 WorkspaceMode positioning (decision table) ──────────────────────

    /// Build a `main`-branch git repo with one committed file in a tempdir.
    fn init_git_repo() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let git = |args: &[&str]| {
            let ok = std::process::Command::new("git")
                .current_dir(tmp.path())
                .args(args)
                .output()
                .unwrap()
                .status
                .success();
            assert!(ok, "git {args:?} failed");
        };
        git(&["init", "-b", "main"]);
        git(&["config", "user.email", "t@t.t"]);
        git(&["config", "user.name", "t"]);
        std::fs::write(tmp.path().join("f.txt"), "v1").unwrap();
        git(&["add", "."]);
        git(&["commit", "-m", "init"]);
        tmp
    }

    fn pipeline_with_workspace(mode: crate::types::WorkspaceMode) -> Pipeline {
        let mut p = make_pipeline("ws", vec![]);
        p.workspace = mode;
        p
    }

    #[test]
    fn workspace_live_returns_camp_root_without_touching_git() {
        // Live works even in a non-git dir — no status/checkout is run.
        let tmp = tempfile::tempdir().unwrap();
        let runner = PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Live))
            .with_camp_root(tmp.path().to_path_buf());
        let (ws, guard) = runner.prepare_workspace(tmp.path()).unwrap();
        assert_eq!(ws, tmp.path());
        assert!(guard.is_none(), "Live needs no worktree guard");
    }

    #[test]
    fn workspace_checkout_clean_switches_to_ref_in_place() {
        let repo = init_git_repo();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Checkout))
                .with_camp_root(repo.path().to_path_buf());
        let (ws, guard) = runner.prepare_workspace(repo.path()).unwrap();
        assert_eq!(ws, repo.path(), "checkout positions the camp root itself");
        assert!(guard.is_none());
    }

    #[test]
    fn workspace_checkout_bails_on_dirty_tracked_change() {
        let repo = init_git_repo();
        // Dirty a tracked file → checkout must refuse rather than clobber it.
        std::fs::write(repo.path().join("f.txt"), "dirty").unwrap();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Checkout))
                .with_camp_root(repo.path().to_path_buf());
        let err = runner.prepare_workspace(repo.path()).unwrap_err();
        assert!(
            matches!(&err, RunnerError::InvalidConfig(m) if m.contains("uncommitted")),
            "expected a dirty-tree refusal, got {err:?}"
        );
    }

    #[test]
    fn workspace_checkout_ignores_untracked_files() {
        let repo = init_git_repo();
        // An untracked file is not "dirty" for checkout purposes.
        std::fs::write(repo.path().join("scratch.txt"), "new").unwrap();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Checkout))
                .with_camp_root(repo.path().to_path_buf());
        assert!(runner.prepare_workspace(repo.path()).is_ok());
    }

    #[test]
    fn workspace_checkout_ignores_dirty_runtime_db_only() {
        let repo = init_git_repo();
        let git = |args: &[&str]| {
            assert!(
                std::process::Command::new("git")
                    .current_dir(repo.path())
                    .args(args)
                    .output()
                    .unwrap()
                    .status
                    .success(),
                "git {args:?} failed"
            );
        };
        // Commit a runtime DB file so it is *tracked* (mirrors the real camp,
        // where the daemon's turso DBs are swept into wip commits).
        std::fs::create_dir_all(repo.path().join(".yah/db")).unwrap();
        std::fs::write(repo.path().join(".yah/db/task-runs.turso-wal"), b"v1").unwrap();
        git(&["add", "."]);
        git(&["commit", "-m", "track runtime db"]);
        // Now dirty ONLY the runtime DB — as the daemon does on every run.
        std::fs::write(repo.path().join(".yah/db/task-runs.turso-wal"), b"v2-churn").unwrap();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Checkout))
                .with_camp_root(repo.path().to_path_buf());
        assert!(
            runner.prepare_workspace(repo.path()).is_ok(),
            "a dirty tree confined to .yah/db runtime state must not bail checkout"
        );
        // But a real source edit alongside the DB churn still bails.
        std::fs::write(repo.path().join("f.txt"), "real edit").unwrap();
        assert!(
            matches!(
                runner.prepare_workspace(repo.path()),
                Err(RunnerError::InvalidConfig(m)) if m.contains("uncommitted")
            ),
            "a tracked source edit must still refuse checkout even amid DB churn"
        );
    }

    #[test]
    fn porcelain_path_is_ignored_classifies_runtime_vs_source() {
        // Runtime DB churn → ignored.
        assert!(porcelain_path_is_ignored(" M .yah/db/task-runs.turso-wal"));
        assert!(porcelain_path_is_ignored("MM .yah/db/gnome_queue.turso"));
        // Source edits → not ignored.
        assert!(!porcelain_path_is_ignored(" M src/main.rs"));
        assert!(!porcelain_path_is_ignored(" M .yah/qed/rusty-v8-musl.toml"));
        // A rename INTO the runtime dir keys off the destination.
        assert!(porcelain_path_is_ignored("R  old.db -> .yah/db/task-runs.turso"));
        assert!(!porcelain_path_is_ignored("R  .yah/db/x.turso -> src/moved.rs"));
        // Unparseable/short lines fail safe (counted as dirty).
        assert!(!porcelain_path_is_ignored(""));
        assert!(!porcelain_path_is_ignored("M"));
    }

    #[test]
    fn workspace_isolated_builds_in_a_worktree_and_guard_cleans_up() {
        let repo = init_git_repo();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Isolated))
                .with_camp_root(repo.path().to_path_buf());
        let (ws, guard) = runner.prepare_workspace(repo.path()).unwrap();
        assert_ne!(ws, repo.path(), "isolated builds in a separate worktree");
        assert!(ws.join("f.txt").exists(), "worktree carries the committed tree");
        assert!(guard.is_some());
        let wt = ws.clone();
        drop(guard);
        assert!(!wt.join("f.txt").exists(), "guard tears the worktree down on drop");
    }

    /// R766, low-level: a `retain`ed guard skips teardown, and `prepare_workspace`
    /// re-enters that exact path (no `git worktree add`) when handed it back via
    /// `with_resume_workspace`, rather than resetting to a fresh checkout.
    #[test]
    fn workspace_isolated_resume_reenters_the_retained_worktree_without_a_fresh_checkout() {
        let repo = init_git_repo();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Isolated))
                .with_camp_root(repo.path().to_path_buf());
        let (ws, guard) = runner.prepare_workspace(repo.path()).unwrap();
        // What a prior FAILED run's step left behind: an uncommitted file a
        // fresh `git worktree add` would never carry (it isn't in the repo).
        std::fs::write(ws.join("marker.txt"), "step-1-output").unwrap();
        guard.unwrap().retain();

        let resumed = PipelineRunner::new(pipeline_with_workspace(
            crate::types::WorkspaceMode::Isolated,
        ))
        .with_camp_root(repo.path().to_path_buf())
        .with_resume_workspace(ws.clone());
        let (resumed_ws, resumed_guard) = resumed.prepare_workspace(repo.path()).unwrap();
        assert_eq!(resumed_ws, ws, "resume re-enters the SAME worktree path");
        assert_eq!(
            std::fs::read_to_string(resumed_ws.join("marker.txt")).unwrap(),
            "step-1-output",
            "a fresh `git worktree add` would have wiped this away"
        );
        drop(resumed_guard);
        assert!(!ws.exists(), "an unretained guard on the resumed run still tears down normally");
    }

    /// The lock reason `git worktree list --porcelain` reports for `wt`, if
    /// locked. Paths compare canonicalized: `$TMPDIR` is a symlink on macOS.
    fn worktree_lock_of(repo: &std::path::Path, wt: &std::path::Path) -> Option<String> {
        let out = std::process::Command::new("git")
            .current_dir(repo)
            .args(["worktree", "list", "--porcelain"])
            .output()
            .unwrap();
        let canon = |p: &std::path::Path| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
        let want = canon(wt);
        let mut in_record = false;
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            if let Some(path) = line.strip_prefix("worktree ") {
                in_record = canon(std::path::Path::new(path)) == want;
            } else if in_record {
                if let Some(reason) = line.strip_prefix("locked") {
                    return Some(reason.trim_start().to_string());
                }
            }
        }
        None
    }

    #[test]
    fn worktree_lock_reason_round_trips_and_ignores_foreign_locks() {
        let reason = worktree_lock_reason("3c867d09-3a5c", 4242);
        assert_eq!(parse_worktree_lock_reason(&reason), Some(("3c867d09-3a5c", 4242)));
        // An operator's bare lock, or one with prose, is not ours to judge.
        assert_eq!(parse_worktree_lock_reason(""), None);
        assert_eq!(parse_worktree_lock_reason("on an external disk"), None);
        assert_eq!(parse_worktree_lock_reason("qed-run  pid 1"), None);
        assert_eq!(parse_worktree_lock_reason("qed-run abc pid nope"), None);
    }

    /// R874-B2: an isolated run holds a `git worktree lock` naming itself for
    /// as long as it builds, released on drop whether or not the tree is
    /// retained. That lock is what the daemon's retention sweep reads to leave
    /// a resumed run's tree alone — the SOURCE run's meta keeps tracking the
    /// path, and the sweep cannot otherwise see that a newer run is in it.
    /// A second resume of the same source must not share the tree.
    #[test]
    fn workspace_isolated_run_locks_its_worktree_and_a_concurrent_resume_cannot_share_it() {
        let repo = init_git_repo();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Isolated))
                .with_camp_root(repo.path().to_path_buf());
        let (ws, guard) = runner.prepare_workspace(repo.path()).unwrap();
        let lock = worktree_lock_of(repo.path(), &ws).expect("a fresh isolated worktree is locked");
        assert_eq!(
            parse_worktree_lock_reason(&lock),
            Some((runner.run_id.as_str(), std::process::id()))
        );
        guard.unwrap().retain();
        assert_eq!(
            worktree_lock_of(repo.path(), &ws),
            None,
            "a retained worktree's lock is released with the run"
        );

        let resume = || {
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Isolated))
                .with_camp_root(repo.path().to_path_buf())
                .with_resume_workspace(ws.clone())
        };
        let first = resume();
        let (first_ws, first_guard) = first.prepare_workspace(repo.path()).unwrap();
        assert_eq!(first_ws, ws);
        assert_eq!(
            worktree_lock_of(repo.path(), &ws)
                .as_deref()
                .and_then(parse_worktree_lock_reason)
                .map(|(id, _)| id.to_string()),
            Some(first.run_id.clone()),
            "the lock names the RESUMED run, not the one the path was minted from"
        );

        let second = resume();
        let (second_ws, second_guard) = second.prepare_workspace(repo.path()).unwrap();
        assert_ne!(second_ws, ws, "a held retained worktree degrades the second resume to a fresh one");
        drop(second_guard);
        assert!(ws.exists(), "the second run's teardown must not touch the first run's tree");
        assert!(worktree_lock_of(repo.path(), &ws).is_some(), "nor release its lock");

        drop(first_guard);
        assert!(!ws.exists(), "the first resume still tears down normally");
    }

    /// R766, end-to-end: a failed `Isolated` run's meta carries the worktree
    /// path, and a second runner constructed with `with_resume_workspace` off
    /// that path actually sees the first run's filesystem mutation.
    #[tokio::test]
    async fn workspace_isolated_failed_run_retains_worktree_and_resume_reenters_it() {
        let repo = init_git_repo();
        let mut pipeline = make_pipeline(
            "resume-isolated",
            vec![
                shell_step("write-marker", vec!["sh", "-c", "echo from-step-1 > marker.txt"]),
                failing_step("boom"),
            ],
        );
        pipeline.workspace = crate::types::WorkspaceMode::Isolated;
        // `failing_step` sets `on_fail = Continue` for its own tests; this one
        // wants the ordinary default (a failure ends the run Failed).
        pipeline.steps[1].on_fail = OnFail::Abort;

        let meta = PipelineRunner::new(pipeline)
            .with_camp_root(repo.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let worktree = meta
            .retained_workspace
            .clone()
            .expect("a failed Isolated run must retain its worktree");
        assert!(
            worktree.join("marker.txt").exists(),
            "the retained tree still carries step 1's write"
        );

        // The daemon drains `pipeline.steps[0..from_step]` before constructing
        // the resume runner (camp.rs `qed_run_handler_inner`); mirrored by hand.
        let mut resume_pipeline = make_pipeline(
            "resume-isolated",
            vec![shell_step("check-marker", vec!["sh", "-c", "test -f marker.txt"])],
        );
        resume_pipeline.workspace = crate::types::WorkspaceMode::Isolated;

        let resumed_meta = PipelineRunner::new(resume_pipeline)
            .with_camp_root(repo.path().to_path_buf())
            .with_resume_workspace(worktree)
            .run()
            .await
            .unwrap();
        assert_eq!(
            resumed_meta.status,
            RunStatus::Success,
            "resume must see step 1's marker.txt inside the retained worktree, not a fresh checkout"
        );
        assert!(
            resumed_meta.retained_workspace.is_none(),
            "a successful resume has nothing left to retain"
        );
    }

    /// A relative `produces` under `workspace = "isolated"` must reach the
    /// publish leg pointing INTO the worktree the step wrote it in, not at the
    /// same relative path under the camp root.
    ///
    /// This was live for every isolated producing pipeline and no test caught
    /// it, because none existed: desktop-release declares no `produces`
    /// and release aggregates from a gha-workflow child whose paths are already
    /// absolute. `cli-release` (R330-T32) is the first, and the symptom would
    /// have been a 45-minute release build failing on its very last step.
    #[tokio::test]
    async fn isolated_produces_resolves_against_the_worktree_not_the_camp_root() {
        let repo = init_git_repo();
        let mut pipeline = pipeline_with_outcomes(
            vec![Outcome::Publish {
                provider: "r2".into(),
                bucket: "yah-dev".into(),
                prefix: None,
                base_url: None,
                require_explicit_version: false,
                per_leg: false,
            }],
            vec![],
            // Write the artifact where `produces` says it is — relative to the
            // step's cwd, which for an isolated run is the worktree.
            vec![
                "sh".into(),
                "-c".into(),
                "mkdir -p out && echo built > out/yah.tar.gz".into(),
            ],
        );
        pipeline.workspace = crate::types::WorkspaceMode::Isolated;
        pipeline.steps[0].produces = vec![ProducedArtifact {
            binary: "yah".into(),
            path: "out/yah.tar.gz".into(),
            triple: Some("darwin-aarch64".into()),
        }];

        let dispatcher = Arc::new(ArtifactCapturingDispatcher {
            artifacts: Mutex::new(vec![]),
        });
        let runner = PipelineRunner::new(pipeline)
            .with_camp_root(repo.path().to_path_buf())
            .with_dispatcher(dispatcher.clone());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let got = dispatcher.artifacts.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        let path = std::path::PathBuf::from(&got[0].path);
        assert!(
            path.is_absolute(),
            "publish leg needs an absolute path: {path:?}"
        );
        assert!(
            !path.starts_with(repo.path()),
            "resolved against the camp root instead of the worktree: {path:?}"
        );
        assert!(path.ends_with("out/yah.tar.gz"));
    }

    /// The same isolated run, but through the REAL
    /// [`crate::publish::PublishingOutcomeDispatcher`], so `stage_release`
    /// actually opens and copies the artifact.
    ///
    /// The sibling test above asserts only which *path* the publish leg was
    /// handed — a recording dispatcher never touches the filesystem — so it
    /// passed while a real `yah qed run cli-release` still died with a bare
    /// `IO error: No such file or directory`. A publish test that never
    /// performs the copy cannot tell you the release publishes.
    #[tokio::test]
    async fn isolated_run_actually_stages_the_artifact_it_produced() {
        let repo = init_git_repo();
        let mut pipeline = pipeline_with_outcomes(
            vec![Outcome::Publish {
                provider: "r2".into(),
                bucket: "yah-dev".into(),
                prefix: None,
                base_url: Some("https://cdn.yah.dev".into()),
                require_explicit_version: false,
                per_leg: false,
            }],
            vec![],
            vec![
                "sh".into(),
                "-c".into(),
                "mkdir -p out && echo built > out/yah.tar.gz".into(),
            ],
        );
        pipeline.workspace = crate::types::WorkspaceMode::Isolated;
        pipeline.steps[0].produces = vec![ProducedArtifact {
            binary: "yah".into(),
            path: "out/yah.tar.gz".into(),
            triple: Some("darwin-aarch64".into()),
        }];

        // LoggingReleasePublisher does no I/O of its own, but the dispatcher
        // wrapping it runs the real stage_release — which is the copy that was
        // failing.
        let dispatcher = Arc::new(crate::publish::PublishingOutcomeDispatcher::new(
            crate::publish::LoggingReleasePublisher,
        ));
        let runner = PipelineRunner::new(pipeline)
            .with_camp_root(repo.path().to_path_buf())
            .with_dispatcher(dispatcher);
        let meta = runner.run().await.expect("publish must not error");
        assert_eq!(meta.status, RunStatus::Success);
    }

    /// `.yah/qed/cli-release.toml` in miniature: two steps, `produces`
    /// declared on the SECOND one, artifact under `target/`, `${{ host.triple }}`
    /// in both the argv and the produces path, real staging dispatcher.
    ///
    /// The single-step tests above all passed while the real `cli-release` run
    /// still failed, so this pins the actual shape rather than a simplification
    /// of it.
    #[tokio::test]
    async fn cli_release_shape_stages_from_an_isolated_worktree() {
        let repo = init_git_repo();
        let mut pipeline = pipeline_with_outcomes(
            vec![Outcome::Publish {
                provider: "r2".into(),
                bucket: "yah-dev".into(),
                prefix: None,
                base_url: Some("https://cdn.yah.dev".into()),
                require_explicit_version: false,
                per_leg: false,
            }],
            vec![],
            vec!["true".into()],
        );
        pipeline.workspace = crate::types::WorkspaceMode::Isolated;
        pipeline.steps[0].name = "build".into();
        let mut package = shell_step(
            "package",
            vec![
                "sh",
                "-c",
                "mkdir -p target/qed-release && echo tarball > \
                 target/qed-release/yah-${{ host.triple }}.tar.gz",
            ],
        );
        package.produces = vec![ProducedArtifact {
            binary: "yah".into(),
            path: "target/qed-release/yah-${{ host.triple }}.tar.gz".into(),
            triple: Some("${{ host.triple }}".into()),
        }];
        pipeline.steps.push(package);

        let dispatcher = Arc::new(crate::publish::PublishingOutcomeDispatcher::new(
            crate::publish::LoggingReleasePublisher,
        ));
        let runner = PipelineRunner::new(pipeline)
            .with_camp_root(repo.path().to_path_buf())
            .with_dispatcher(dispatcher);
        let meta = runner.run().await.expect("publish must not error");
        assert_eq!(meta.status, RunStatus::Success);
    }

    #[test]
    fn workspace_isolated_leaves_a_dirty_camp_root_untouched() {
        let repo = init_git_repo();
        // Uncommitted edits in the camp root are fine for isolated — it never
        // touches them, it builds from a fresh worktree at the committed ref.
        std::fs::write(repo.path().join("f.txt"), "dirty").unwrap();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Isolated))
                .with_camp_root(repo.path().to_path_buf());
        let (ws, guard) = runner.prepare_workspace(repo.path()).unwrap();
        assert_eq!(std::fs::read_to_string(repo.path().join("f.txt")).unwrap(), "dirty");
        assert_eq!(
            std::fs::read_to_string(ws.join("f.txt")).unwrap(),
            "v1",
            "worktree has committed bytes"
        );
        drop(guard);
    }

    // ── R330-B27: ref (not hardcoded "main") drives Checkout/Isolated ───────

    /// Build a `main`-branch git repo with two commits: `v1.0.0` tags the
    /// first, `main` moves on to a second. Distinguishes "the tag's commit"
    /// from "main's commit" for the tests below — before this ticket,
    /// `target_branch()` silently fell back to `"main"` whenever no ref was
    /// requested, so a tag-triggered release would build main's bytes.
    fn init_git_repo_with_tag() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let git = |args: &[&str]| {
            let ok = std::process::Command::new("git")
                .current_dir(tmp.path())
                .args(args)
                .output()
                .unwrap()
                .status
                .success();
            assert!(ok, "git {args:?} failed");
        };
        git(&["init", "-b", "main"]);
        git(&["config", "user.email", "t@t.t"]);
        git(&["config", "user.name", "t"]);
        std::fs::write(tmp.path().join("f.txt"), "v1").unwrap();
        git(&["add", "."]);
        git(&["commit", "-m", "v1"]);
        git(&["tag", "v1.0.0"]);
        std::fs::write(tmp.path().join("f.txt"), "v2-on-main").unwrap();
        git(&["add", "."]);
        git(&["commit", "-m", "advance main"]);
        tmp
    }

    fn git_rev_parse(dir: &std::path::Path, rev: &str) -> String {
        let out = std::process::Command::new("git")
            .current_dir(dir)
            .args(["rev-parse", rev])
            .output()
            .unwrap();
        assert!(out.status.success(), "git rev-parse {rev} failed");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[test]
    fn workspace_checkout_with_no_ref_positions_at_head_not_main() {
        let repo = init_git_repo_with_tag();
        // Detach HEAD at the tag — mirrors a CI runner that already checked
        // out a tag push before invoking `yah qed run`.
        run_git(repo.path(), &["checkout", "v1.0.0"]).unwrap();
        let tag_sha = git_rev_parse(repo.path(), "HEAD");
        let main_sha = git_rev_parse(repo.path(), "main");
        assert_ne!(tag_sha, main_sha, "fixture sanity: tag and main differ");

        // No with_ref() call — must not fall back to "main".
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Checkout))
                .with_camp_root(repo.path().to_path_buf());
        let (ws, _guard) = runner.prepare_workspace(repo.path()).unwrap();
        assert_eq!(ws, repo.path());
        assert_eq!(
            git_rev_parse(repo.path(), "HEAD"),
            tag_sha,
            "checkout with no explicit ref stays at the checked-out tag, not main"
        );
    }

    #[test]
    fn workspace_isolated_with_no_ref_positions_at_head_not_main() {
        let repo = init_git_repo_with_tag();
        // Same detached-HEAD-at-a-tag setup as the Checkout test above.
        run_git(repo.path(), &["checkout", "v1.0.0"]).unwrap();
        let tag_sha = git_rev_parse(repo.path(), "HEAD");
        let main_sha = git_rev_parse(repo.path(), "main");
        assert_ne!(tag_sha, main_sha, "fixture sanity: tag and main differ");

        // No with_ref() call — the pre-fix code would `git worktree add
        // <path> main` here and silently build main's bytes.
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Isolated))
                .with_camp_root(repo.path().to_path_buf());
        let (ws, guard) = runner.prepare_workspace(repo.path()).unwrap();
        assert_eq!(
            git_rev_parse(&ws, "HEAD"),
            tag_sha,
            "isolated worktree with no explicit ref positions at HEAD (the tag), not main"
        );
        drop(guard);
    }

    #[test]
    fn workspace_isolated_with_explicit_ref_builds_worktree_at_that_commit() {
        let repo = init_git_repo_with_tag();
        // HEAD stays on main; the run explicitly requests the tag instead —
        // the R330-B27 "plumb the trigger ref" shape (a tag-fired run passing
        // its tag explicitly rather than relying on ambient HEAD state).
        let tag_sha = git_rev_parse(repo.path(), "v1.0.0");
        let main_sha = git_rev_parse(repo.path(), "main");
        assert_ne!(tag_sha, main_sha, "fixture sanity: tag and main differ");

        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Isolated))
                .with_camp_root(repo.path().to_path_buf())
                .with_ref(Some("v1.0.0".to_string()));
        let (ws, guard) = runner.prepare_workspace(repo.path()).unwrap();
        let worktree_sha = git_rev_parse(&ws, "HEAD");
        assert_eq!(
            worktree_sha, tag_sha,
            "explicit ref=<tag> builds the worktree at the tag's commit"
        );
        assert_ne!(
            worktree_sha, main_sha,
            "must not build main's bytes when a tag ref is requested"
        );
        drop(guard);
    }

    // ── W224 R533-F11: whole-run positioning reaches non-gha steps ────────────

    /// An `Isolated` run positions the tree ONCE at run start and every
    /// subprocess step builds in that worktree — not the live camp root — with a
    /// single run-scoped guard that outlives all steps and tears the worktree
    /// down when the run returns. This is the desktop-release-builds-from-the-
    /// worktree fix: before F11 only the gha-workflow step was repositioned.
    #[tokio::test]
    async fn run_level_isolated_positions_every_step_in_the_worktree() {
        let repo = init_git_repo();
        let mut pipeline = one_step_pipeline(
            "iso",
            vec![
                "sh".into(),
                "-c".into(),
                // Record cwd for the assertion and drop a build artifact in it.
                "echo cwd=$(pwd) >> \"$YAH_OUTPUTS\"; echo built > built.txt".into(),
            ],
        );
        pipeline.workspace = crate::types::WorkspaceMode::Isolated;
        // A second step reads the file the first wrote: it only succeeds if the
        // worktree survives BETWEEN steps (one shared guard, not per-step).
        let mut step2 = pipeline.steps[0].clone();
        step2.name = "step-2".into();
        step2.argv = vec![
            "sh".into(),
            "-c".into(),
            "cat built.txt && echo cwd=$(pwd) >> \"$YAH_OUTPUTS\"".into(),
        ];
        pipeline.steps.push(step2);

        let runner = PipelineRunner::new(pipeline).with_camp_root(repo.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success, "{:?}", meta.steps);

        let cwd1 = meta.steps[0].outputs.get("cwd").expect("step-1 cwd");
        let cwd2 = meta.steps[1].outputs.get("cwd").expect("step-2 cwd");
        assert_eq!(cwd1, cwd2, "every step in the run shares the one worktree");
        assert!(
            cwd1.contains("qed-worktree-"),
            "subprocess step ran in the run's isolated worktree, got {cwd1}"
        );
        assert!(
            !repo.path().join("built.txt").exists(),
            "the build artifact landed in the worktree, never the live camp root"
        );
        // run() has returned ⇒ the run-scoped guard dropped ⇒ worktree is gone.
        assert!(
            !std::path::Path::new(cwd1).exists(),
            "the run-scoped worktree is torn down once the run completes"
        );
    }

    /// `Live` leaves every step on the camp root as-is (no git, works in a
    /// non-repo tempdir) — the run-level positioning is a no-op for Live.
    #[tokio::test]
    async fn run_level_live_keeps_steps_on_the_camp_root() {
        let tmp = tempfile::tempdir().unwrap();
        let mut pipeline = one_step_pipeline(
            "live",
            vec![
                "sh".into(),
                "-c".into(),
                "echo cwd=$(pwd) >> \"$YAH_OUTPUTS\"".into(),
            ],
        );
        pipeline.workspace = crate::types::WorkspaceMode::Live;
        let runner = PipelineRunner::new(pipeline).with_camp_root(tmp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success, "{:?}", meta.steps);
        let cwd = meta.steps[0].outputs.get("cwd").expect("cwd");
        assert_eq!(
            std::path::Path::new(cwd).canonicalize().unwrap(),
            tmp.path().canonicalize().unwrap(),
            "Live builds the camp root in place"
        );
    }

    /// Checkout-bail-if-dirty now fires at the *run* level (not only for a
    /// gha-workflow step): an ordinary `run()` of a subprocess pipeline over a
    /// dirty tree refuses rather than silently building surprise bytes.
    #[tokio::test]
    async fn run_level_checkout_bails_on_dirty_tree_before_any_step() {
        let repo = init_git_repo();
        std::fs::write(repo.path().join("f.txt"), "dirty").unwrap();
        let mut pipeline = one_step_pipeline("co", vec!["echo".into(), "hi".into()]);
        pipeline.workspace = crate::types::WorkspaceMode::Checkout;
        let runner = PipelineRunner::new(pipeline).with_camp_root(repo.path().to_path_buf());
        let err = runner.run().await.unwrap_err();
        assert!(
            matches!(&err, RunnerError::InvalidConfig(m) if m.contains("uncommitted")),
            "expected a run-level dirty-tree refusal, got {err:?}"
        );
    }

    /// Counts publish and revalidate calls so we can assert "single publish"
    /// behaviour across composite runs.
    #[derive(Default)]
    struct CountingDispatcher {
        publishes: Mutex<u32>,
    }

    #[async_trait::async_trait]
    impl OutcomeDispatcher for CountingDispatcher {
        async fn yubaba_deploy(&self, _s: &str, _e: &str) -> Result<(), RunnerError> {
            Ok(())
        }
        async fn almanac_run(&self, _p: &str) -> Result<(), RunnerError> {
            Ok(())
        }
        async fn publish(&self, _req: &crate::publish::PublishRequest) -> Result<(), RunnerError> {
            *self.publishes.lock().unwrap() += 1;
            Ok(())
        }
    }

    /// R560-F15: records each publish's artifact paths and each `almanac_run`
    /// name, so a test sees which outcome lists fired and what they carried.
    #[derive(Default)]
    struct LegDispatcher {
        published: Mutex<Vec<Vec<String>>>,
        almanac: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl OutcomeDispatcher for LegDispatcher {
        async fn yubaba_deploy(&self, _s: &str, _e: &str) -> Result<(), RunnerError> {
            Ok(())
        }
        async fn almanac_run(&self, p: &str) -> Result<(), RunnerError> {
            self.almanac.lock().unwrap().push(p.to_string());
            Ok(())
        }
        async fn publish(&self, req: &crate::publish::PublishRequest) -> Result<(), RunnerError> {
            let paths = req.artifacts.iter().map(|a| a.path.clone()).collect();
            self.published.lock().unwrap().push(paths);
            Ok(())
        }
    }

    /// mesofact-musl's shape, two steps per leg: x86 = `x86-build` (slow) →
    /// `x86-stage` (produces); arm = `arm-build` (fails fast) → `arm-stage`
    /// (produces). The legs share no edge. `on_fail` is an almanac marker.
    fn two_leg_pipeline(per_leg: bool, dir: &std::path::Path) -> (Pipeline, String, String) {
        let x86_art = dir.join("mesofact-x86_64.tar.gz");
        let arm_art = dir.join("mesofact-aarch64.tar.gz");
        std::fs::write(&x86_art, b"x86").unwrap();
        std::fs::write(&arm_art, b"arm").unwrap();
        let (x86_art, arm_art) = (
            x86_art.to_string_lossy().into_owned(),
            arm_art.to_string_lossy().into_owned(),
        );
        // Slow enough that arm-build has failed before x86-stage is admitted,
        // so a run-wide abort would strand x86-stage.
        let mut x86_build = shell_step("x86-build", vec!["sleep", "0.5"]);
        x86_build.needs = Some(vec![]);
        let mut x86_stage = producing_step("x86-stage", "mesofact", &x86_art);
        x86_stage.needs = Some(vec!["x86-build".into()]);
        let mut arm_build = shell_step("arm-build", vec!["false"]);
        arm_build.needs = Some(vec![]);
        let mut arm_stage = producing_step("arm-stage", "mesofact", &arm_art);
        arm_stage.needs = Some(vec!["arm-build".into()]);
        let mut p = make_pipeline(
            "two-legs",
            vec![x86_build, x86_stage, arm_build, arm_stage],
        );
        p.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-dev".into(),
            prefix: None,
            base_url: None,
            require_explicit_version: false,
            per_leg,
        }];
        p.on_fail = vec![Outcome::AlmanacRun {
            pipeline: "on-fail".into(),
        }];
        (p, x86_art, arm_art)
    }

    #[tokio::test]
    async fn per_leg_publish_ships_the_green_leg_and_still_fails_the_run() {
        let tmp = TempDir::new().unwrap();
        let (pipeline, x86_art, _arm_art) = two_leg_pipeline(true, tmp.path());
        let dispatcher = Arc::new(LegDispatcher::default());
        let meta = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone())
            .run()
            .await
            .unwrap();

        assert_eq!(meta.status, RunStatus::Failed, "a failed leg still fails the run");
        let row = |name: &str| meta.steps.iter().find(|s| s.name == name).map(|s| s.status);
        assert_eq!(row("arm-build"), Some(RunStatus::Failed));
        assert_eq!(row("arm-stage"), None, "the failed leg's dependent is never run");
        assert_eq!(
            row("x86-stage"),
            Some(RunStatus::Success),
            "the other leg's dependent runs despite arm's abort"
        );
        assert_eq!(
            *dispatcher.almanac.lock().unwrap(),
            vec!["on-fail".to_string()],
            "on_fail fires for the failure"
        );
        assert_eq!(
            *dispatcher.published.lock().unwrap(),
            vec![vec![x86_art]],
            "exactly one publish, carrying only the green leg's artifact"
        );
    }

    #[tokio::test]
    async fn run_wide_publish_ships_nothing_when_any_leg_fails() {
        let tmp = TempDir::new().unwrap();
        let (pipeline, _, _) = two_leg_pipeline(false, tmp.path());
        let dispatcher = Arc::new(LegDispatcher::default());
        let meta = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone())
            .run()
            .await
            .unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        assert!(
            dispatcher.published.lock().unwrap().is_empty(),
            "without per_leg a failed leg withholds every publish"
        );
        assert_eq!(*dispatcher.almanac.lock().unwrap(), vec!["on-fail".to_string()]);
    }

    #[tokio::test]
    async fn sub_pipeline_resolves_unresolvable_with_clear_error() {
        let root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("does-not-exist".into()),
                false,
            )],
        );
        // Default NoopSubPipelineResolver — every resolve returns None.
        let runner = PipelineRunner::new(root);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let step = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        assert_eq!(step.status, RunStatus::Failed);
    }

    /// Resolver that publishes a typed [`unresolved_reason`] — used to assert
    /// the runner surfaces the typed message in `StepFailed.msg` for the
    /// R494-T5 remote-peer path.
    struct DiagnosticResolver(String);
    impl SubPipelineResolver for DiagnosticResolver {
        fn resolve(&self, _target: &SubPipelineRef) -> Option<Pipeline> {
            None
        }
        fn unresolved_reason(&self, _target: &SubPipelineRef) -> Option<String> {
            Some(self.0.clone())
        }
    }

    #[tokio::test]
    async fn sub_pipeline_unresolved_surfaces_resolver_typed_reason() {
        // R494-T5: when the resolver publishes an unresolved_reason (e.g.
        // "remote peer not yet supported"), the runner's StepFailed.msg
        // carries that message verbatim instead of the generic "target
        // unresolvable" debug tail.
        let peer_target = SubPipelineRef::Peer {
            camp: "cheers".into(),
            pipeline: "publish".into(),
        };
        let typed = "remote peer `cheers` lives on rig `rig-tokyo-1` (R494-T5)".to_string();
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> =
            Arc::new(DiagnosticResolver(typed.clone()));
        let runner = PipelineRunner::new(make_pipeline(
            "root",
            vec![sub_step("remote", peer_target.clone(), false)],
        ))
        .with_sub_pipeline_resolver(resolver);
        let err = runner
            .execute_step_sub_pipeline(0, &sub_step("remote", peer_target, false), &mut Vec::new())
            .await
            .expect_err("expected StepFailed");
        match err {
            RunnerError::StepFailed { msg, .. } => assert_eq!(msg, typed),
            other => panic!("expected StepFailed, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn sub_pipeline_runs_child_to_completion() {
        // root has one SubPipeline step → child has one trivial run step.
        let child = make_pipeline("child", vec![shell_step("ok", vec!["true"])]);
        let root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        let step = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        assert_eq!(step.status, RunStatus::Success);
    }

    #[tokio::test]
    async fn sub_pipeline_failure_propagates_to_parent() {
        let child = make_pipeline("child", vec![shell_step("boom", vec!["false"])]);
        let root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
    }

    /// Cancelling JUST the nested child's task (never the root's) still
    /// stops the whole run and reports `Cancelled`, not `Failed` — the
    /// bubble-up path added for `qed.cancel <nested_child_run_id>`.
    #[tokio::test]
    async fn sub_pipeline_child_cancel_bubbles_up_to_parent() {
        let child = make_pipeline("child", vec![shell_step("slow", vec!["sleep", "5"])]);
        let root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        // Stands in for the daemon's `child_abort_hook` (camp.rs): captures
        // the child's real AbortHandle the moment it's spawned, exactly what
        // `qed.cancel <child_run_id>` would reach for.
        let captured: Arc<std::sync::Mutex<Option<tokio::task::AbortHandle>>> =
            Arc::new(std::sync::Mutex::new(None));
        let captured_for_hook = Arc::clone(&captured);
        let hook: ChildAbortHook = Arc::new(move |_run_id: &str, handle: tokio::task::AbortHandle| {
            *captured_for_hook.lock().unwrap() = Some(handle);
        });

        let runner = PipelineRunner::new(root)
            .with_sub_pipeline_resolver(resolver)
            .with_child_abort_hook(hook);

        let run = tokio::spawn(async move { runner.run().await });

        // The hook fires from inside `execute_step_sub_pipeline`, asynchronously
        // relative to this test — poll rather than assume it's already there.
        let handle = loop {
            if let Some(h) = captured.lock().unwrap().clone() {
                break h;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        };
        // Cancel ONLY the child's task — never touches `runner`/the root task.
        handle.abort();

        let meta = run.await.unwrap().unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Cancelled,
            "a cancelled nested child must bubble up as Cancelled, not Failed"
        );
    }

    // R755: `own_workspace` — a child records where it actually built by
    // writing `pwd` to a marker file the test reads back afterward (the
    // child's own worktree/positioned-tree is torn down by the time `run()`
    // returns, so asserting on it post-hoc via `git worktree list` would not
    // see it — this has to be observed from inside the running step).
    fn pwd_marker_step(marker: &std::path::Path) -> crate::types::QedStep {
        let mut step = shell_step("where", vec!["sh", "-c", "pwd > \"$MARKER\""]);
        step.env.insert("MARKER".to_string(), marker.display().to_string());
        step
    }

    #[tokio::test]
    async fn sub_pipeline_own_workspace_true_isolates_from_live_parent() {
        let repo = init_git_repo();
        let marker_dir = tempfile::tempdir().unwrap();
        let marker = marker_dir.path().join("where.txt");

        let mut child = make_pipeline("child", vec![pwd_marker_step(&marker)]);
        child.workspace = crate::types::WorkspaceMode::Isolated;
        let root = make_pipeline(
            "root",
            vec![{
                let mut s = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
                s.sub_pipeline.as_mut().unwrap().own_workspace = Some(true);
                s
            }],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root)
            .with_camp_root(repo.path().to_path_buf())
            .with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let where_built = std::fs::read_to_string(&marker).unwrap();
        let where_built = where_built.trim();
        // Canonicalize: macOS's /tmp is a /private/tmp symlink, so a raw
        // string compare of `pwd`'s resolved output against tempfile's
        // unresolved TempDir path spuriously differs even when they name the
        // same directory.
        let repo_canon = std::fs::canonicalize(repo.path()).unwrap();
        assert_ne!(
            where_built,
            repo_canon.to_string_lossy(),
            "own_workspace=true must build the child in its OWN (isolated) worktree, \
             not the live parent's camp root"
        );
    }

    #[tokio::test]
    async fn sub_pipeline_own_workspace_false_inherits_live_parent() {
        let repo = init_git_repo();
        let marker_dir = tempfile::tempdir().unwrap();
        let marker = marker_dir.path().join("where.txt");

        // Child declares Isolated too — but with `own_workspace = false`,
        // W224/R533-F11 inheritance still wins: this is the pre-R755 default
        // behaviour, it is what `release-wizard`'s `npm-publish` needs (it
        // must publish the version-bump the parent has not committed yet),
        // and it must not regress.
        //
        // R887: what changed is that the step must now SAY so. Leaving
        // `own_workspace` unset here is refused by
        // `validate_sub_pipeline_graph` (see
        // `types::tests::an_isolated_child_under_a_live_parent_must_state_which_tree`)
        // — the runner itself still treats unset as inherit, because by the
        // time a Pipeline reaches it the loader has already had its say.
        let mut child = make_pipeline("child", vec![pwd_marker_step(&marker)]);
        child.workspace = crate::types::WorkspaceMode::Isolated;
        let root = make_pipeline(
            "root",
            vec![{
                let mut s = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
                s.sub_pipeline.as_mut().unwrap().own_workspace = Some(false);
                s
            }],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root)
            .with_camp_root(repo.path().to_path_buf())
            .with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let where_built = std::fs::read_to_string(&marker).unwrap();
        let where_built = where_built.trim();
        let repo_canon = std::fs::canonicalize(repo.path()).unwrap();
        assert_eq!(
            where_built,
            repo_canon.to_string_lossy(),
            "with own_workspace = false, a child inherits the parent's positioned \
             tree even when its own pipeline declares Isolated"
        );
    }

    #[tokio::test]
    async fn sub_pipeline_inlines_child_steps_as_rows_by_default() {
        // W223 R532-F3: transparent-by-default generalizes beyond GHA. A
        // Builtin (or Path / Peer) child's steps are attributed to the wrapping
        // step as inlined rows — one per child step, in order, carrying status
        // and (on failure) the child step's error. Child qed steps are linear,
        // so the rows have no `needs` edges.
        let child = make_pipeline(
            "child",
            vec![
                shell_step("prep", vec!["true"]),
                shell_step("build", vec!["false"]), // fails
                shell_step("publish", vec!["true"]),
            ],
        );
        let root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();

        let step = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        // The wrapping step carries one row per child step that ran (the
        // child aborts after `build` fails, so `publish` never runs).
        assert_eq!(
            step.jobs.iter().map(|j| j.id.as_str()).collect::<Vec<_>>(),
            vec!["prep", "build"],
            "child steps inline as rows in order, stopping at the abort",
        );
        assert_eq!(step.jobs[0].status, RunStatus::Success);
        assert_eq!(step.jobs[1].status, RunStatus::Failed);
        assert!(
            step.jobs[1].error.is_some(),
            "the failed child step's error carries onto the inlined row",
        );
        assert!(
            step.jobs.iter().all(|j| j.needs.is_empty()),
            "linear qed child steps carry no needs edges",
        );
    }

    #[tokio::test]
    async fn opaque_sub_pipeline_suppresses_inlined_rows() {
        // W223 R532-F3: the `opaque` opt-out keeps the wrapper a single
        // black-box node — the child still runs and its status rolls up, but
        // no per-child rows are inlined.
        let child = make_pipeline(
            "child",
            vec![shell_step("a", vec!["true"]), shell_step("b", vec!["true"])],
        );
        let mut wrap = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
        wrap.sub_pipeline.as_mut().unwrap().opaque = true;
        let root = make_pipeline("root", vec![wrap]);
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();

        let step = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        assert_eq!(
            step.status,
            RunStatus::Success,
            "child still ran + rolled up"
        );
        assert!(
            step.jobs.is_empty(),
            "opaque opt-out suppresses the inlined per-child rows",
        );
    }

    /// R487 follow-up: when a `SubPipelineRef::GhaWorkflow` child step's
    /// inner workflow fails, the parent's `StepFailed.msg` must carry the
    /// inner stderr tail and the failing job/step name — NOT the generic
    /// "failed at child step `gha-workflow`" wrapper that the long
    /// SubPipeline path produces. Verifies the short-circuit in
    /// `execute_step_sub_pipeline` (R487 follow-up).
    #[tokio::test]
    async fn gha_workflow_subpipeline_surfaces_stderr_tail_to_parent() {
        let tmp = tempfile::tempdir().unwrap();
        let wf_path = tmp.path().join("fail.yml");
        std::fs::write(
            &wf_path,
            r#"
name: fail
on: push
jobs:
  blow-up:
    runs-on: ubuntu-latest
    steps:
      - name: emit then fail
        run: |
          echo "boom-marker-9b7c"
          echo "fatal: nothing to see here" 1>&2
          exit 17
"#,
        )
        .unwrap();

        // Synthesised one-step pipeline carrying the GhaWorkflow step,
        // exactly as `LoaderSubPipelineResolver::resolve` would build it.
        let step = crate::types::QedStep {
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
            name: "gha-workflow".to_string(),
            argv: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: crate::types::StepKind::GhaWorkflow,
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
            outputs: Vec::new(),
            import: None,
            gha_workflow: Some(crate::types::GhaWorkflowConfig {
                path: wf_path.clone(),
                event: None,
                inputs: HashMap::new(),
                matrix: HashMap::new(),
            }),
            matrix: None,
            enabled: true,
            activation: crate::types::StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
        };
        let child = Pipeline {
            allow_late_operator_block: false,
                        participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "fail".into(),
            label: "fail".into(),
            steps: vec![step],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: Default::default(),
            workspace: crate::types::WorkspaceMode::Live, // test fixture isn't a git checkout
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };

        let mut map = std::collections::HashMap::new();
        map.insert(format!("gha:{}", wf_path.display()), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        // Parent: one SubPipeline step targeting our GHA workflow.
        let root = make_pipeline(
            "root",
            vec![sub_step(
                "wrap",
                SubPipelineRef::GhaWorkflow {
                    path: wf_path.clone(),
                    event: None,
                    inputs: HashMap::new(),
                },
                false,
            )],
        );

        // Capture parent's event stream so we can inspect the
        // StepFinished.msg the consumer would see.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runner = PipelineRunner::new(root)
            .with_sub_pipeline_resolver(resolver)
            .with_camp_root(tmp.path().to_path_buf())
            .with_events(tx);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);

        // Walk the event stream for the parent's StepFinished on step 0.
        let mut step_fail_msg: Option<String> = None;
        let mut saw_subpipeline_started = false;
        let mut saw_subpipeline_finished = false;
        while let Ok(ev) = rx.try_recv() {
            match &ev {
                QedEvent::StepFinished {
                    index: 0,
                    msg,
                    status,
                    ..
                } => {
                    if *status == RunStatus::Failed {
                        step_fail_msg = msg.clone();
                    }
                }
                QedEvent::SubPipelineStarted { index: 0, .. } => {
                    saw_subpipeline_started = true;
                }
                QedEvent::SubPipelineFinished {
                    index: 0, status, ..
                } => {
                    if *status == RunStatus::Failed {
                        saw_subpipeline_finished = true;
                    }
                }
                _ => {}
            }
        }
        assert!(
            saw_subpipeline_started,
            "short-circuit must still emit SubPipelineStarted bookend",
        );
        assert!(
            saw_subpipeline_finished,
            "short-circuit must still emit SubPipelineFinished bookend with failed status",
        );
        let msg = step_fail_msg.expect("parent StepFinished carries a failure msg");
        assert!(
            msg.contains("blow-up"),
            "msg should name the failing job (got: {msg})",
        );
        assert!(
            msg.contains("emit then fail"),
            "msg should name the failing step (got: {msg})",
        );
        assert!(
            msg.contains("fatal: nothing to see here"),
            "msg should carry the stderr tail (got: {msg})",
        );
        // And — critically — the inner tail should NOT be wrapped in the
        // generic SubPipeline "failed at child step `gha-workflow`" string
        // that the long path produces.
        assert!(
            !msg.contains("failed at child step `gha-workflow`"),
            "short-circuit should bypass the SubPipeline-wrapper msg (got: {msg})",
        );
    }

    /// The whole lowering for a pipeline-declared matrix pin, end to end:
    /// `matrix = { board = "{{board}}" }` in the step's `[gha_workflow]` block →
    /// `Pipeline::apply_params` substitutes the run param →
    /// `execute_step_gha_workflow` lowers it onto
    /// `yah_qed_gha::Executor::matrix_filter` → the non-matching row is Skipped.
    ///
    /// The unit tests either side of this cover the substitution and the filter
    /// semantics on their own; this one exists because the two-line wiring
    /// between them is exactly what a unit test cannot see.
    #[tokio::test]
    async fn gha_workflow_matrix_param_pins_one_row() {
        let tmp = tempfile::tempdir().unwrap();
        let wf_path = tmp.path().join("boards.yml");
        std::fs::write(
            &wf_path,
            r#"
name: boards
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        board: [orangepi_zero2w, rpi_zero2w]
    steps:
      - name: build one board
        run: echo "building ${{ matrix.board }}"
"#,
        )
        .unwrap();

        let mut step = crate::types::QedStep::default();
        step.name = "image".to_string();
        step.kind = crate::types::StepKind::GhaWorkflow;
        step.gha_workflow = Some(crate::types::GhaWorkflowConfig {
            path: wf_path.clone(),
            event: None,
            inputs: HashMap::new(),
            matrix: [("board".to_string(), "{{board}}".to_string())]
                .into_iter()
                .collect(),
        });

        let mut pipeline = make_pipeline("pin", vec![step]);
        pipeline.workspace = crate::types::WorkspaceMode::Live; // fixture isn't a git checkout
        pipeline.apply_params(
            &[("board".to_string(), "rpi_zero2w".to_string())]
                .into_iter()
                .collect(),
        );

        let runner = PipelineRunner::new(pipeline).with_camp_root(tmp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        // Both rows are still planned and reported — the unselected one as
        // Skipped, so `needs.*` aggregation and the dashboard both stay honest
        // about what the matrix contained.
        let jobs = &meta.steps[0].jobs;
        assert_eq!(jobs.len(), 2, "both matrix rows reported: {jobs:?}");
        assert_eq!(
            jobs.iter()
                .filter(|j| j.status == RunStatus::Success)
                .count(),
            1,
            "exactly one row ran: {jobs:?}"
        );
        assert_eq!(
            jobs.iter()
                .filter(|j| j.status == RunStatus::Skipped)
                .count(),
            1,
            "the other row was skipped, not run: {jobs:?}"
        );
        // R330-B41: the skipped row's cause survives the gha bridge onto the
        // persisted JobRow, not just the live event stream.
        let skipped = jobs.iter().find(|j| j.status == RunStatus::Skipped).unwrap();
        assert!(
            skipped.skip_reason.is_some(),
            "skipped row should carry a reason: {skipped:?}"
        );
    }

    #[tokio::test]
    async fn gha_workflow_subpipeline_persists_per_job_rows() {
        // W223 R532-T1: a wrapped GHA workflow is a *disregarded entity* — its
        // jobs are persisted as structured per-job rows under the wrapping
        // step's StepStatus, rather than collapsed into one flattened failure
        // string. One job succeeds, one fails (carrying its stderr-tail detail),
        // and one downstream job `needs` the failing one so it is skipped — the
        // R516 skip-count becomes a per-row Skipped state, not a trailing
        // sentence.
        let tmp = tempfile::tempdir().unwrap();
        let wf_path = tmp.path().join("mix.yml");
        std::fs::write(
            &wf_path,
            r#"
name: mix
on: push
jobs:
  ok:
    runs-on: ubuntu-latest
    steps:
      - name: succeed
        run: echo "all good"
  boom:
    runs-on: ubuntu-latest
    steps:
      - name: emit then fail
        run: |
          echo "fatal: kaboom-7f3a" 1>&2
          exit 9
  downstream:
    runs-on: ubuntu-latest
    needs: boom
    steps:
      - name: never runs
        run: echo "should be skipped"
"#,
        )
        .unwrap();

        // Synthesised one-step pipeline carrying the GhaWorkflow step, exactly
        // as `LoaderSubPipelineResolver::resolve` would build it.
        let step = crate::types::QedStep {
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
            name: "gha-workflow".to_string(),
            argv: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: crate::types::StepKind::GhaWorkflow,
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
            outputs: Vec::new(),
            import: None,
            gha_workflow: Some(crate::types::GhaWorkflowConfig {
                path: wf_path.clone(),
                event: None,
                inputs: HashMap::new(),
                matrix: HashMap::new(),
            }),
            matrix: None,
            enabled: true,
            activation: crate::types::StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
        };
        let child = Pipeline {
            allow_late_operator_block: false,
                        participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "mix".into(),
            label: "mix".into(),
            steps: vec![step],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: Default::default(),
            workspace: crate::types::WorkspaceMode::Live, // test fixture isn't a git checkout
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };

        let mut map = std::collections::HashMap::new();
        map.insert(format!("gha:{}", wf_path.display()), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let root = make_pipeline(
            "root",
            vec![sub_step(
                "wrap",
                SubPipelineRef::GhaWorkflow {
                    path: wf_path.clone(),
                    event: None,
                    inputs: HashMap::new(),
                },
                false,
            )],
        );

        let runner = PipelineRunner::new(root)
            .with_sub_pipeline_resolver(resolver)
            .with_camp_root(tmp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);

        // The wrapping step (index 0) carries one row per GHA job.
        let wrap = &meta.steps[0];
        assert_eq!(
            wrap.jobs.len(),
            3,
            "all three jobs should produce rows (got: {:?})",
            wrap.jobs.iter().map(|j| &j.id).collect::<Vec<_>>(),
        );
        let row = |id: &str| {
            wrap.jobs
                .iter()
                .find(|j| j.id == id)
                .unwrap_or_else(|| panic!("missing row for job {id}"))
        };

        assert_eq!(row("ok").status, RunStatus::Success);
        assert!(row("ok").error.is_none(), "success row carries no error");

        let boom = row("boom");
        assert_eq!(boom.status, RunStatus::Failed);
        let err = boom
            .error
            .as_ref()
            .expect("failed job row carries stderr-tail detail");
        assert!(
            err.contains("emit then fail"),
            "row error names the failing step (got: {err})",
        );
        assert!(
            err.contains("kaboom-7f3a"),
            "row error carries the stderr tail (got: {err})",
        );

        let down = row("downstream");
        assert_eq!(
            down.status,
            RunStatus::Skipped,
            "downstream gated on a failed dep is a Skipped row, not a trailing skip-count",
        );
        assert!(down.error.is_none(), "skipped row carries no error");
        // W223 R532-F2: the intra-workflow `needs:` edge is persisted so the
        // graph viewer can render it as a real dependency edge.
        assert_eq!(
            down.needs,
            vec!["boom".to_string()],
            "downstream's needs edge is carried on the row",
        );
        assert!(
            row("ok").needs.is_empty(),
            "a job with no needs has an empty edge list"
        );
    }

    #[tokio::test]
    async fn sub_pipeline_aggregates_produces_when_propagate_set() {
        // Child has a producing step + its own Outcome::Publish that we
        // expect SUPPRESSED because parent claims propagate.produces.
        let mut child = make_pipeline(
            "child",
            vec![producing_step("emit", "yah", "target/release/yah")],
        );
        child.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
            require_explicit_version: false,
            per_leg: false,
        }];

        // Parent: SubPipeline child with propagate.produces=true + its own
        // Outcome::Publish. We expect ONE publish total (the parent's),
        // confirming both suppression on child and aggregation on parent.
        let mut root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                true,
            )],
        );
        root.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
            require_explicit_version: false,
            per_leg: false,
        }];

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let dispatcher = Arc::new(CountingDispatcher::default());
        let runner = PipelineRunner::new_with_dispatcher(root, dispatcher.clone())
            .with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            *dispatcher.publishes.lock().unwrap(),
            1,
            "exactly one publish — parent fires, child suppressed"
        );
    }

    #[tokio::test]
    async fn sub_pipeline_child_publish_fires_when_propagate_unset() {
        // Mirror of the above but propagate.produces=false — child's own
        // Outcome::Publish should fire, parent's too. Total: 2.
        let mut child = make_pipeline(
            "child",
            vec![producing_step("emit", "yah", "target/release/yah")],
        );
        child.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
            require_explicit_version: false,
            per_leg: false,
        }];

        let mut root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
        );
        root.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
            require_explicit_version: false,
            per_leg: false,
        }];

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let dispatcher = Arc::new(CountingDispatcher::default());
        let runner = PipelineRunner::new_with_dispatcher(root, dispatcher.clone())
            .with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            *dispatcher.publishes.lock().unwrap(),
            2,
            "two publishes — child fires its own + parent fires its own"
        );
    }

    #[tokio::test]
    async fn sub_pipeline_nested_two_levels_works() {
        // root -> mid -> leaf. propagate.produces all the way up.
        let leaf = make_pipeline(
            "leaf",
            vec![producing_step("emit", "yah", "target/release/yah")],
        );
        let mid = make_pipeline(
            "mid",
            vec![sub_step(
                "descend",
                SubPipelineRef::Builtin("leaf".into()),
                true,
            )],
        );
        let mut root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("mid".into()),
                true,
            )],
        );
        root.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
            require_explicit_version: false,
            per_leg: false,
        }];

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:leaf".to_string(), leaf);
        map.insert("builtin:mid".to_string(), mid);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let dispatcher = Arc::new(CountingDispatcher::default());
        let runner = PipelineRunner::new_with_dispatcher(root, dispatcher.clone())
            .with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            *dispatcher.publishes.lock().unwrap(),
            1,
            "single revalidate even across two SubPipeline edges"
        );
    }

    // ─── F3: multi-child publish fan-in + continue-on-error ─────────────────

    /// Recording publisher that captures the staged tree on each sync, so
    /// tests can assert "what would have been uploaded" without a real R2
    /// account. Differs from `publish::tests::RecordingPublisher` by
    /// exposing every staged file (not just one manifest) so we can verify
    /// multi-binary fan-in across SubPipeline children.
    #[derive(Default)]
    struct StageRecorder {
        syncs: Mutex<u32>,
        revalidates: Mutex<u32>,
        /// Channel keys (`<binary>/<version>/<triple>/<file>` or
        /// `<binary>/release-manifest.json`) observed across all syncs.
        files: Mutex<Vec<String>>,
        /// Index keys merged into, one per binary in the release.
        index_keys: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl crate::publish::ReleasePublisher for StageRecorder {
        async fn sync(
            &self,
            staging_dir: &std::path::Path,
            _provider: &str,
            _bucket: &str,
            _prefix: Option<&str>,
        ) -> Result<(), RunnerError> {
            *self.syncs.lock().unwrap() += 1;
            let mut walker = vec![staging_dir.to_path_buf()];
            while let Some(dir) = walker.pop() {
                for entry in std::fs::read_dir(&dir).unwrap() {
                    let entry = entry.unwrap();
                    let path = entry.path();
                    if path.is_dir() {
                        walker.push(path);
                    } else {
                        let rel = path
                            .strip_prefix(staging_dir)
                            .unwrap()
                            .to_string_lossy()
                            .into_owned();
                        self.files.lock().unwrap().push(rel);
                    }
                }
            }
            self.files.lock().unwrap().sort();
            Ok(())
        }

        async fn publish_index(
            &self,
            _provider: &str,
            _bucket: &str,
            update: &crate::publish::IndexUpdate,
        ) -> Result<(), RunnerError> {
            let mut keys = self.index_keys.lock().unwrap();
            keys.push(update.key.clone());
            keys.sort();
            Ok(())
        }

        async fn revalidate(
            &self,
            _report: &crate::publish::StageReport,
        ) -> Result<(), RunnerError> {
            *self.revalidates.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn sub_pipeline_multi_child_fan_in_groups_by_binary_with_single_publish() {
        // Three children producing different binaries (yah, desktop,
        // mesofact) all rolled up into the parent. The parent's single
        // Outcome::Publish should fire ONCE with a staged tree containing
        // all three binaries' files + per-binary manifests, and exactly
        // one revalidate POST. Exercises the full chain F2 wired:
        //   parent.run -> child.run_inner (x3) -> aggregate produced
        //              -> parent's PublishingOutcomeDispatcher.publish
        //              -> stage_release (lays out the tree)
        //              -> StageRecorder.sync (one call, sees all binaries)
        //              -> StageRecorder.revalidate (one call total).
        let tmp = TempDir::new().unwrap();
        let yah_path = tmp.path().join("yah");
        std::fs::write(&yah_path, b"YAH").unwrap();
        let desktop_path = tmp.path().join("desktop");
        std::fs::write(&desktop_path, b"DESKTOP").unwrap();
        let mesofact_path = tmp.path().join("mesofact");
        std::fs::write(&mesofact_path, b"MESOFACT").unwrap();

        let child_cli = make_pipeline(
            "child-cli",
            vec![producing_step(
                "build-cli",
                "yah",
                yah_path.to_string_lossy().as_ref(),
            )],
        );
        let child_desktop = make_pipeline(
            "child-desktop",
            vec![producing_step(
                "build-desktop",
                "desktop",
                desktop_path.to_string_lossy().as_ref(),
            )],
        );
        let child_mesofact = make_pipeline(
            "child-mesofact",
            vec![producing_step(
                "build-mesofact",
                "mesofact",
                mesofact_path.to_string_lossy().as_ref(),
            )],
        );

        let mut root = make_pipeline(
            "full-release",
            vec![
                sub_step(
                    "compose-cli",
                    SubPipelineRef::Builtin("child-cli".into()),
                    true,
                ),
                sub_step(
                    "compose-desktop",
                    SubPipelineRef::Builtin("child-desktop".into()),
                    true,
                ),
                sub_step(
                    "compose-mesofact",
                    SubPipelineRef::Builtin("child-mesofact".into()),
                    true,
                ),
            ],
        );
        root.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: Some("https://releases.yah.dev".into()),
            require_explicit_version: false,
            per_leg: false,
        }];

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child-cli".to_string(), child_cli);
        map.insert("builtin:child-desktop".to_string(), child_desktop);
        map.insert("builtin:child-mesofact".to_string(), child_mesofact);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        // Pin the version so the staged path is deterministic. R876-F6:
        // serialized against the other YAH_RELEASE_VERSION-mutating tests via
        // RELEASE_VERSION_ENV_LOCK — this env var is process-global.
        let _env_guard = RELEASE_VERSION_ENV_LOCK.lock().unwrap();
        std::env::set_var("YAH_RELEASE_VERSION", "1.2.3");

        let recorder = Arc::new(StageRecorder::default());
        struct ArcRecorder(Arc<StageRecorder>);
        #[async_trait::async_trait]
        impl crate::publish::ReleasePublisher for ArcRecorder {
            async fn sync(
                &self,
                d: &std::path::Path,
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
                u: &crate::publish::IndexUpdate,
            ) -> Result<(), RunnerError> {
                self.0.publish_index(p, b, u).await
            }
            async fn revalidate(
                &self,
                r: &crate::publish::StageReport,
            ) -> Result<(), RunnerError> {
                self.0.revalidate(r).await
            }
        }
        let dispatcher = Arc::new(crate::publish::PublishingOutcomeDispatcher::new(
            ArcRecorder(recorder.clone()),
        ));
        let runner = PipelineRunner::new_with_dispatcher(root, dispatcher)
            .with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        std::env::remove_var("YAH_RELEASE_VERSION");

        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            *recorder.syncs.lock().unwrap(),
            1,
            "single sync across all children"
        );
        assert_eq!(
            *recorder.revalidates.lock().unwrap(),
            1,
            "single revalidate POST"
        );

        let files = recorder.files.lock().unwrap();
        // Per binary: shared mutable manifest + per-(binary,triple) stable
        // manifest (single triple in this fan-in: darwin-aarch64) + immutable
        // per-version manifest + the binary itself = 4 objects × 3 binaries.
        // The per-triple stable manifests came from R330-B8 (cross-stage merge
        // fan-in); the per-version copy from R330-T32, so a version's index
        // entry links a manifest that never changes under it.
        assert_eq!(files.len(), 12, "staged tree contents: {files:?}");
        assert!(files.iter().any(|f| f == "yah/release-manifest.json"));
        assert!(files.iter().any(|f| f == "desktop/release-manifest.json"));
        assert!(files.iter().any(|f| f == "mesofact/release-manifest.json"));
        assert!(files.iter().any(|f| f == "yah/1.2.3/manifest.json"));
        assert!(files.iter().any(|f| f == "desktop/1.2.3/manifest.json"));
        assert!(files.iter().any(|f| f == "mesofact/1.2.3/manifest.json"));
        assert!(files
            .iter()
            .any(|f| f == "yah/release-manifest-darwin-aarch64.json"));
        assert!(files
            .iter()
            .any(|f| f == "desktop/release-manifest-darwin-aarch64.json"));
        assert!(files
            .iter()
            .any(|f| f == "mesofact/release-manifest-darwin-aarch64.json"));
        assert!(files.iter().any(|f| f.starts_with("yah/1.2.3/")));
        assert!(files.iter().any(|f| f.starts_with("desktop/1.2.3/")));
        assert!(files.iter().any(|f| f.starts_with("mesofact/1.2.3/")));
        drop(files);

        // One index per binary, not one per release: the history object is
        // per-binary, so a three-binary fan-in appends to three of them.
        assert_eq!(
            recorder.index_keys.lock().unwrap().as_slice(),
            [
                "desktop/index.json",
                "mesofact/index.json",
                "yah/index.json"
            ]
        );
    }

    #[tokio::test]
    async fn sub_pipeline_failed_child_with_continue_on_error_does_not_abort_parent() {
        // Pins the F2 open question: a SubPipeline step with on_fail =
        // Continue marks itself failed but the parent loop proceeds to
        // subsequent steps. The child's produced are dropped (current
        // implementation only aggregates on success — documented behaviour).
        let bad_child = make_pipeline("bad", vec![shell_step("boom", vec!["false"])]);
        let mut sub = sub_step("compose", SubPipelineRef::Builtin("bad".into()), false);
        sub.on_fail = OnFail::Continue;
        let after = shell_step("after", vec!["true"]);
        let root = make_pipeline("root", vec![sub, after]);

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:bad".to_string(), bad_child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();

        // Overall status is Failed (any failed step flips it regardless of
        // on_fail policy), but the subsequent `after` step still ran
        // because Continue suppresses the early break.
        assert_eq!(meta.status, RunStatus::Failed);
        let compose = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        assert_eq!(compose.status, RunStatus::Failed);
        let after_step = meta.steps.iter().find(|s| s.name == "after").unwrap();
        assert_eq!(
            after_step.status,
            RunStatus::Success,
            "after step ran despite child failure"
        );
    }

    #[tokio::test]
    async fn sub_pipeline_forwards_params_to_child() {
        // Child step has a `{{greeting}}` arg; parent's SubPipeline params
        // substitute it before the child runs.
        let child = make_pipeline(
            "child",
            vec![shell_step("echo", vec!["true", "{{greeting}}"])],
        );
        let mut step = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
        if let Some(cfg) = step.sub_pipeline.as_mut() {
            cfg.params
                .insert("greeting".to_string(), "hello".to_string());
        }
        let root = make_pipeline("root", vec![step]);

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        // Successful = `true hello` exited 0. We don't capture argv here but
        // a `false {{greeting}}` would fail the same; this proves the step
        // ran post-substitution.
        assert_eq!(meta.status, RunStatus::Success);
    }

    /// R755: the exact release-wizard shape — a parent step's own
    /// `sub_pipeline.params` entry is ITSELF a `{{placeholder}}` referencing
    /// the parent's own declared param (`params = { spec = "{{spec}}" }`),
    /// not a literal value. `Pipeline::apply_params` used to substitute
    /// `{{key}}` in a step's `argv`/`env`/`gha_workflow` fields only, so this
    /// placeholder survived into `resolve_params` and then the child's argv
    /// literally, unrelated to whatever the operator actually passed. Caught
    /// by a real `yah qed run release-wizard --param spec=patch` run failing
    /// with `expected patch|minor|major or a literal X.Y.Z, got {{spec}}`.
    #[tokio::test]
    async fn sub_pipeline_param_placeholder_substitutes_from_parent_before_forwarding() {
        let child = make_pipeline(
            "child",
            vec![shell_step("check", vec!["test", "{{spec}}", "=", "patch"])],
        );
        let mut step = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
        if let Some(cfg) = step.sub_pipeline.as_mut() {
            cfg.params
                .insert("spec".to_string(), "{{spec}}".to_string());
        }
        let mut root = make_pipeline("root", vec![step]);
        root.params
            .insert("spec".to_string(), param_def(None, true));

        // Mirrors the CLI/daemon boundary (qed.rs / camp.rs): resolve the
        // operator-supplied params against root's declarations, then apply —
        // BEFORE the runner ever sees the pipeline.
        let resolved = root
            .resolve_params(&HashMap::from([("spec".to_string(), "patch".to_string())]))
            .unwrap();
        root.apply_params(&resolved);

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let meta = PipelineRunner::new(root)
            .with_sub_pipeline_resolver(resolver)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
    }

    /// R653-F1: a forwarded param gates a step INSIDE the child, and the value
    /// the child's `params` namespace reports is the child's own resolved map
    /// (the parent's params don't leak in).
    #[tokio::test]
    async fn sub_pipeline_child_gates_on_forwarded_param() {
        let mut child_step = shell_step("full-only", vec!["true"]);
        child_step.if_cond = Some("params.variant == 'full'".into());
        let child = make_pipeline("child", vec![child_step]);

        let mut step = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
        if let Some(cfg) = step.sub_pipeline.as_mut() {
            cfg.params
                .insert("variant".to_string(), "full".to_string());
        }
        let root = make_pipeline("root", vec![step]);

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        // The PARENT runner carries variant=quick. If the parent's params
        // leaked into the child's context the gate would read 'quick' and the
        // child's only step would skip, failing the assertion below.
        let meta = PipelineRunner::new(root)
            .with_sub_pipeline_resolver(resolver)
            .with_params(HashMap::from([(
                "variant".to_string(),
                "quick".to_string(),
            )]))
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
    }

    /// R653-F1: the child's own `[params]` default applies when the parent
    /// forwards nothing. Previously `cfg.params` went to `apply_params` raw, so
    /// an unpassed child param left `{{key}}` literally in the child's argv.
    #[tokio::test]
    async fn sub_pipeline_child_param_default_applies_when_parent_omits_it() {
        let mut child = make_pipeline(
            "child",
            vec![shell_step("check", vec!["test", "{{variant}}", "=", "full"])],
        );
        child
            .params
            .insert("variant".to_string(), param_def(Some("full"), false));

        // Parent forwards nothing — the child's declared default has to fill in.
        let step = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
        let root = make_pipeline("root", vec![step]);

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let meta = PipelineRunner::new(root)
            .with_sub_pipeline_resolver(resolver)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
    }

    /// R653-F1: a *required* child param nobody supplied fails the step by
    /// name, rather than silently shelling out a command with `{{key}}` in it.
    #[tokio::test]
    async fn sub_pipeline_missing_required_child_param_fails_the_step() {
        let mut child = make_pipeline("child", vec![shell_step("check", vec!["true"])]);
        child
            .params
            .insert("variant".to_string(), param_def(None, true));

        let step = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
        let root = make_pipeline("root", vec![step]);

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let meta = PipelineRunner::new(root)
            .with_sub_pipeline_resolver(resolver)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let err = meta.steps[0].error.clone().unwrap_or_default();
        assert!(
            err.contains("variant"),
            "the failure must name the missing param, got: {err}"
        );
    }

    // ─── Named output exposure (R488-F4) ────────────────────────────────────

    /// Step 1 writes an output via $YAH_OUTPUTS; step 2 references it in
    /// argv via `${{ steps.step1.outputs.digest }}` — the runner substitutes
    /// the value before execution so step 2 receives the resolved string.
    #[tokio::test]
    async fn step_outputs_substituted_into_sibling_argv() {
        // step1: writes digest=abc123 to $YAH_OUTPUTS via a shell one-liner.
        // step2: echoes the substitution placeholder — if substitution worked,
        //        argv will have been rewritten to "echo abc123" before
        //        execution, and the step exits 0.
        let step1 = shell_step(
            "step1",
            vec!["sh", "-c", "echo digest=abc123 >> \"$YAH_OUTPUTS\""],
        );
        // step2's argv contains the placeholder; the runner rewrites it
        // before passing to the executor.
        let step2 = shell_step(
            "step2",
            vec![
                "sh",
                "-c",
                "test \"$1\" = abc123",
                "--",
                "${{ steps.step1.outputs.digest }}",
            ],
        );
        let pipeline = make_pipeline("p", vec![step1, step2]);
        let runner = PipelineRunner::new(pipeline);
        let meta = runner.run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Success,
            "step2 should receive substituted value"
        );
        let s1 = meta.steps.iter().find(|s| s.name == "step1").unwrap();
        assert_eq!(
            s1.outputs.get("digest").map(|s| s.as_str()),
            Some("abc123"),
            "step1 outputs map should contain captured value"
        );
    }

    /// `${{ host.triple }}` reaches `produces`, which the step-output pass
    /// never touched. Without this a host-native release recipe cannot name the
    /// file it just built: `produces.path` and `produces.triple` are literals in
    /// TOML, and the dev box's triple is the one value that cannot be.
    #[test]
    fn host_triple_substitution_reaches_produces() {
        let mut step = shell_step("package", vec!["tar", "-czf", "yah-${{ host.triple }}.tar.gz"]);
        step.env.insert(
            "TARBALL".to_string(),
            "yah-${{ host.triple }}.tar.gz".to_string(),
        );
        step.produces = vec![ProducedArtifact {
            binary: "yah".into(),
            path: "target/qed-release/yah-${{ host.triple }}.tar.gz".into(),
            triple: Some("${{ host.triple }}".into()),
        }];

        let out = substituted_step(&step, &HashMap::new(), "aarch64-apple-darwin")
            .expect("a step mentioning host.triple must be rewritten");
        assert_eq!(out.argv[2], "yah-aarch64-apple-darwin.tar.gz");
        assert_eq!(out.env["TARBALL"], "yah-aarch64-apple-darwin.tar.gz");
        assert_eq!(
            out.produces[0].path,
            "target/qed-release/yah-aarch64-apple-darwin.tar.gz"
        );
        assert_eq!(
            out.produces[0].triple.as_deref(),
            Some("aarch64-apple-darwin")
        );
    }

    /// A step with nothing to substitute is not cloned — the fast path every
    /// existing pipeline takes.
    #[test]
    fn steps_without_placeholders_are_not_rewritten() {
        let step = shell_step("plain", vec!["true"]);
        assert!(substituted_step(&step, &HashMap::new(), "aarch64-apple-darwin").is_none());
        // A non-empty step context still forces the clone, as before.
        let ctx = HashMap::from([(
            "prior".to_string(),
            HashMap::from([("k".to_string(), "v".to_string())]),
        )]);
        assert!(substituted_step(&step, &ctx, "aarch64-apple-darwin").is_some());
    }

    /// Captures the artifacts a publish outcome actually received, which
    /// `RecordingDispatcher` (count only) cannot show.
    struct ArtifactCapturingDispatcher {
        artifacts: Mutex<Vec<ProducedArtifact>>,
    }

    #[async_trait::async_trait]
    impl OutcomeDispatcher for ArtifactCapturingDispatcher {
        async fn yubaba_deploy(&self, _service: &str, _env: &str) -> Result<(), RunnerError> {
            Ok(())
        }
        async fn almanac_run(&self, _pipeline: &str) -> Result<(), RunnerError> {
            Ok(())
        }
        async fn publish(&self, req: &crate::publish::PublishRequest) -> Result<(), RunnerError> {
            *self.artifacts.lock().unwrap() = req.artifacts.clone();
            Ok(())
        }
    }

    /// End-to-end: the substituted `produces` is what the publish leg stages,
    /// not the raw placeholder. A regression here publishes an artifact at a
    /// path containing a literal `${{ host.triple }}`.
    #[tokio::test]
    async fn published_artifact_carries_the_detected_host_triple() {
        let mut pipeline = pipeline_with_outcomes(
            vec![Outcome::Publish {
                provider: "r2".into(),
                bucket: "yah-dev".into(),
                prefix: None,
                base_url: None,
                require_explicit_version: false,
                per_leg: false,
            }],
            vec![],
            vec!["true".to_string()],
        );
        pipeline.steps[0].produces = vec![ProducedArtifact {
            binary: "yah".into(),
            path: "target/qed-release/yah-${{ host.triple }}.tar.gz".into(),
            triple: Some("${{ host.triple }}".into()),
        }];

        let dispatcher = Arc::new(ArtifactCapturingDispatcher {
            artifacts: Mutex::new(vec![]),
        });
        let runner = PipelineRunner::new(pipeline).with_dispatcher(dispatcher.clone());
        let host = crate::platform::detect_host_triple();
        runner.run().await.unwrap();

        let got = dispatcher.artifacts.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].triple.as_deref(), Some(host.as_str()));
        // Absolute by the time the publish leg sees it (resolved against the
        // run's positioned workspace), so assert the tail rather than pinning
        // whatever root the test happens to run under.
        assert!(
            got[0]
                .path
                .ends_with(&format!("target/qed-release/yah-{host}.tar.gz")),
            "unsubstituted or unresolved produces path: {}",
            got[0].path
        );
        assert!(
            !got[0].path.contains("${{"),
            "a placeholder survived into the publish leg: {}",
            got[0].path
        );
    }

    /// Step 1 writes KEY=VALUE to $YAH_OUTPUTS; the runner collects it into
    /// StepStatus::outputs regardless of whether the step declared it in
    /// the `outputs` field.
    #[tokio::test]
    async fn step_outputs_captured_in_step_status() {
        let step = shell_step(
            "emit",
            vec![
                "sh",
                "-c",
                "printf 'foo=bar\\nbaz=qux\\n' >> \"$YAH_OUTPUTS\"",
            ],
        );
        let pipeline = make_pipeline("p", vec![step]);
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        let s = meta.steps.iter().find(|s| s.name == "emit").unwrap();
        assert_eq!(s.outputs.get("foo").map(|s| s.as_str()), Some("bar"));
        assert_eq!(s.outputs.get("baz").map(|s| s.as_str()), Some("qux"));
    }

    /// W209 F3: a `[[bind]]` whose `from` references step1's output fires
    /// mid-pipeline; step2 reads the new value off disk like any other
    /// tool. Confirms the build→checkin→release inversion at the
    /// mechanical layer: the source tree IS the step-to-step plumbing.
    #[tokio::test]
    async fn pipeline_bind_writes_manifest_mid_run_visible_to_next_step() {
        const HASH_A: &str = "fb0afc9f3d966f5347c6dfd335adab12f1dc8ee6df18cf9e9ff90fe86f0416c0";
        let workspace = TempDir::new().unwrap();
        let manifest_path = workspace.path().join("workload.toml");
        std::fs::write(
            &manifest_path,
            "name = \"whisper\"\nblake3 = \"0000000000000000000000000000000000000000000000000000000000000000\"\n",
        )
        .unwrap();

        let mut step1 = shell_step(
            "publish",
            vec![
                "sh",
                "-c",
                &format!("echo discovered=\"{HASH_A}\" >> \"$YAH_OUTPUTS\""),
            ],
        );
        step1.outputs = vec![crate::types::OutputDecl {
            name: "discovered".into(),
            description: None,
            kind: manifest_bind::ValueType::Blake3Hex,
            validate: None,
        }];

        // Step 2 reads the on-disk manifest and asserts the new hash is
        // there. If apply_binds didn't fire mid-pipeline, this fails.
        let step2 = shell_step(
            "consume",
            vec!["sh", "-c", &format!("grep -q '{HASH_A}' workload.toml")],
        );

        let mut pipeline = make_pipeline("publish-then-consume", vec![step1, step2]);
        pipeline.binds = vec![manifest_bind::BindSpec {
            file: "workload.toml".into(),
            path: "blake3".into(),
            from: manifest_bind::OutputRef::parse("publish.outputs.discovered").unwrap(),
            intent: manifest_bind::Intent::Keyword(manifest_bind::IntentKeyword::Latest),
            cross_workspace: false,
            schema: None,
        }];

        let runner = PipelineRunner::new(pipeline).with_camp_root(workspace.path().to_path_buf());
        let meta = runner.run().await.unwrap();

        assert_eq!(
            meta.status,
            RunStatus::Success,
            "step2 must see the bound value"
        );
        let s1 = meta.steps.iter().find(|s| s.name == "publish").unwrap();
        assert_eq!(
            s1.applied_binds.len(),
            1,
            "publish step should record one bind"
        );
        assert!(
            s1.applied_binds[0].changed,
            "first run flips the placeholder"
        );
        assert_eq!(s1.applied_binds[0].new, HASH_A);

        // Idempotent: a re-run sees the same hash, writes nothing, but
        // still records the AppliedBind entry with changed=false.
        let mut step1b = shell_step(
            "publish",
            vec![
                "sh",
                "-c",
                &format!("echo discovered=\"{HASH_A}\" >> \"$YAH_OUTPUTS\""),
            ],
        );
        step1b.outputs = vec![crate::types::OutputDecl {
            name: "discovered".into(),
            description: None,
            kind: manifest_bind::ValueType::Blake3Hex,
            validate: None,
        }];
        let step2b = shell_step(
            "consume",
            vec!["sh", "-c", &format!("grep -q '{HASH_A}' workload.toml")],
        );
        let mut pipeline2 = make_pipeline("publish-then-consume", vec![step1b, step2b]);
        pipeline2.binds = vec![manifest_bind::BindSpec {
            file: "workload.toml".into(),
            path: "blake3".into(),
            from: manifest_bind::OutputRef::parse("publish.outputs.discovered").unwrap(),
            intent: manifest_bind::Intent::Keyword(manifest_bind::IntentKeyword::Latest),
            cross_workspace: false,
            schema: None,
        }];
        let meta2 = PipelineRunner::new(pipeline2)
            .with_camp_root(workspace.path().to_path_buf())
            .run()
            .await
            .unwrap();
        let s1b = meta2.steps.iter().find(|s| s.name == "publish").unwrap();
        assert_eq!(s1b.applied_binds.len(), 1);
        assert!(!s1b.applied_binds[0].changed, "re-run is a no-op on disk");
    }

    /// W209 F3: a failed step does NOT fire its binds. The source tree is
    /// the ledger; partial states are only written for steps that
    /// succeeded.
    #[tokio::test]
    async fn pipeline_bind_skipped_when_producing_step_fails() {
        const HASH_A: &str = "fb0afc9f3d966f5347c6dfd335adab12f1dc8ee6df18cf9e9ff90fe86f0416c0";
        let workspace = TempDir::new().unwrap();
        let manifest_path = workspace.path().join("workload.toml");
        std::fs::write(
            &manifest_path,
            "name = \"whisper\"\nblake3 = \"0000000000000000000000000000000000000000000000000000000000000000\"\n",
        )
        .unwrap();
        let before = std::fs::read_to_string(&manifest_path).unwrap();

        // Step writes the output line THEN exits non-zero. Output is
        // collected, but apply_binds must be gated on success.
        let mut step1 = shell_step(
            "publish",
            vec![
                "sh",
                "-c",
                &format!("echo discovered=\"{HASH_A}\" >> \"$YAH_OUTPUTS\"; exit 1"),
            ],
        );
        step1.outputs = vec![crate::types::OutputDecl {
            name: "discovered".into(),
            description: None,
            kind: manifest_bind::ValueType::Blake3Hex,
            validate: None,
        }];

        let mut pipeline = make_pipeline("publish-fails", vec![step1]);
        pipeline.binds = vec![manifest_bind::BindSpec {
            file: "workload.toml".into(),
            path: "blake3".into(),
            from: manifest_bind::OutputRef::parse("publish.outputs.discovered").unwrap(),
            intent: manifest_bind::Intent::Keyword(manifest_bind::IntentKeyword::Latest),
            cross_workspace: false,
            schema: None,
        }];

        let meta = PipelineRunner::new(pipeline)
            .with_camp_root(workspace.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let s = meta.steps.iter().find(|s| s.name == "publish").unwrap();
        assert!(
            s.applied_binds.is_empty(),
            "failed step must not fire binds"
        );
        // Manifest on disk is untouched.
        assert_eq!(std::fs::read_to_string(&manifest_path).unwrap(), before);
    }

    /// W209/R510-F6: a `[[on_change]]` journal hook fires exactly once when a
    /// bind changes the manifest, and zero times when a re-run rewrites the
    /// same value (no-op). This is the doc's hash-change-hook verification
    /// criterion driven end-to-end through the runner.
    #[tokio::test]
    async fn on_change_journal_fires_once_on_change_zero_on_noop() {
        const HASH_A: &str = "fb0afc9f3d966f5347c6dfd335adab12f1dc8ee6df18cf9e9ff90fe86f0416c0";
        let workspace = TempDir::new().unwrap();
        let manifest_path = workspace.path().join("workload.toml");
        std::fs::write(
            &manifest_path,
            "name = \"whisper\"\nblake3 = \"0000000000000000000000000000000000000000000000000000000000000000\"\n",
        )
        .unwrap();
        let journal_rel = ".yah/qed/whisper.journal";

        let build_pipeline = || {
            let mut step1 = shell_step(
                "publish",
                vec![
                    "sh",
                    "-c",
                    &format!("echo discovered=\"{HASH_A}\" >> \"$YAH_OUTPUTS\""),
                ],
            );
            step1.outputs = vec![crate::types::OutputDecl {
                name: "discovered".into(),
                description: None,
                kind: manifest_bind::ValueType::Blake3Hex,
                validate: None,
            }];
            let mut pipeline = make_pipeline("publish-with-hook", vec![step1]);
            pipeline.binds = vec![manifest_bind::BindSpec {
                file: "workload.toml".into(),
                path: "blake3".into(),
                from: manifest_bind::OutputRef::parse("publish.outputs.discovered").unwrap(),
                intent: manifest_bind::Intent::Keyword(manifest_bind::IntentKeyword::Latest),
                cross_workspace: false,
                schema: None,
            }];
            pipeline.on_change = vec![manifest_bind::OnChangeHook {
                bind: "blake3".into(),
                action: manifest_bind::OnChangeAction::Journal {
                    journal: journal_rel.into(),
                },
            }];
            pipeline
        };

        // First run: the zero-sentinel flips to HASH_A → bind changed → hook fires.
        let meta = PipelineRunner::new(build_pipeline())
            .with_camp_root(workspace.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        let journal_abs = workspace.path().join(journal_rel);
        let after_first = std::fs::read_to_string(&journal_abs).unwrap();
        assert_eq!(
            after_first.lines().count(),
            1,
            "hook fires once on real change"
        );
        assert!(
            after_first.contains(HASH_A),
            "journal records the new value"
        );

        // Second run: same hash → no-op rewrite → hook must NOT fire again.
        let meta2 = PipelineRunner::new(build_pipeline())
            .with_camp_root(workspace.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta2.status, RunStatus::Success);
        let after_second = std::fs::read_to_string(&journal_abs).unwrap();
        assert_eq!(
            after_second.lines().count(),
            1,
            "no-op rewrite must not append a second journal line",
        );
    }

    /// W212/R518-P4: early cutoff is **value-equality based**, not run-count
    /// based. When a step (re)produces output byte-identical to what the
    /// manifest already holds — even on the *first* run — the bind is
    /// `changed = false`, so the on_change hook never fires. This is the
    /// Bazel/Nix property: a rebuild whose output didn't change does not
    /// propagate downstream, regardless of why the rebuild ran.
    #[tokio::test]
    async fn on_change_early_cutoff_when_output_already_matches() {
        const HASH_A: &str = "fb0afc9f3d966f5347c6dfd335adab12f1dc8ee6df18cf9e9ff90fe86f0416c0";
        let workspace = TempDir::new().unwrap();
        let manifest_path = workspace.path().join("workload.toml");
        // Manifest ALREADY holds HASH_A — no prior run, no sentinel.
        std::fs::write(
            &manifest_path,
            format!("name = \"whisper\"\nblake3 = \"{HASH_A}\"\n"),
        )
        .unwrap();
        let journal_rel = ".yah/qed/whisper.journal";

        let mut step1 = shell_step(
            "publish",
            vec![
                "sh",
                "-c",
                &format!("echo discovered=\"{HASH_A}\" >> \"$YAH_OUTPUTS\""),
            ],
        );
        step1.outputs = vec![crate::types::OutputDecl {
            name: "discovered".into(),
            description: None,
            kind: manifest_bind::ValueType::Blake3Hex,
            validate: None,
        }];
        let mut pipeline = make_pipeline("publish-noop", vec![step1]);
        pipeline.binds = vec![manifest_bind::BindSpec {
            file: "workload.toml".into(),
            path: "blake3".into(),
            from: manifest_bind::OutputRef::parse("publish.outputs.discovered").unwrap(),
            intent: manifest_bind::Intent::Keyword(manifest_bind::IntentKeyword::Latest),
            cross_workspace: false,
            schema: None,
        }];
        pipeline.on_change = vec![manifest_bind::OnChangeHook {
            bind: "blake3".into(),
            action: manifest_bind::OnChangeAction::Journal {
                journal: journal_rel.into(),
            },
        }];

        let meta = PipelineRunner::new(pipeline)
            .with_camp_root(workspace.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        // The predicate accepted the value, but the bytes already matched →
        // changed=false → no hook fired → no journal file at all.
        let s = meta.steps.iter().find(|s| s.name == "publish").unwrap();
        assert!(
            s.applied_binds.iter().all(|b| !b.changed),
            "bind to an already-matching value must be changed=false",
        );
        assert!(
            !workspace.path().join(journal_rel).exists(),
            "early cutoff: an unchanged output must not fire the on_change hook",
        );
    }

    /// SubPipeline step with propagate.outputs propagates named child outputs
    /// to the parent step context so subsequent sibling steps can reference
    /// `${{ steps.<child-step-name>.outputs.<key> }}`.
    #[tokio::test]
    async fn sub_pipeline_propagates_named_outputs_to_parent_context() {
        // Inner child pipeline: one step that writes "result=42" to $YAH_OUTPUTS.
        let child_step = shell_step(
            "inner",
            vec!["sh", "-c", "echo result=42 >> \"$YAH_OUTPUTS\""],
        );
        let child = make_pipeline("child", vec![child_step]);

        // SubPipeline step propagates the "result" output.
        let mut sub = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
        if let Some(cfg) = sub.sub_pipeline.as_mut() {
            cfg.propagate.outputs = vec!["result".to_string()];
        }

        // A sibling step after the SubPipeline step references the propagated output.
        let sibling = shell_step(
            "check",
            vec![
                "sh",
                "-c",
                "test \"$1\" = 42",
                "--",
                "${{ steps.compose.outputs.result }}",
            ],
        );

        let root = make_pipeline("root", vec![sub, sibling]);
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Success,
            "sibling should receive child output via parent step context"
        );
        let compose = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        assert_eq!(
            compose.outputs.get("result").map(|s| s.as_str()),
            Some("42"),
            "SubPipeline step status should carry propagated outputs"
        );
    }

    // ---------- R488-F5: event-stream wiring for sub-pipelines ----------

    #[tokio::test]
    async fn sub_pipeline_emits_started_finished_bookends_with_child_run_id() {
        // Parent has two SubPipeline steps, each invoking a distinct child.
        // Assert: each parent SubPipeline step is wrapped by
        // SubPipelineStarted{child_run_id=X} ... SubPipelineFinished{child_run_id=X, status=Success}.
        // The child's own RunStarted/Step*/RunFinished events do NOT leak
        // onto the parent's stream (the child sink is decoupled).
        let child_a = make_pipeline("child-a", vec![shell_step("ok", vec!["true"])]);
        let child_b = make_pipeline("child-b", vec![shell_step("ok", vec!["true"])]);
        let root = make_pipeline(
            "root",
            vec![
                sub_step(
                    "compose-a",
                    SubPipelineRef::Builtin("child-a".into()),
                    false,
                ),
                sub_step(
                    "compose-b",
                    SubPipelineRef::Path(".yah/qed/child-b.toml".into()),
                    false,
                ),
            ],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child-a".to_string(), child_a);
        map.insert("path:.yah/qed/child-b.toml".to_string(), child_b);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runner = PipelineRunner::new(root)
            .with_events(tx)
            .with_sub_pipeline_resolver(resolver);
        let parent_run_id = runner.run_id().to_string();
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert!(meta.parent_run_id.is_none(), "top-level run has no parent");

        let mut events = Vec::new();
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }

        let starts: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                QedEvent::SubPipelineStarted {
                    name,
                    target,
                    child_run_id,
                    ..
                } => Some((name.clone(), target.clone(), child_run_id.clone())),
                _ => None,
            })
            .collect();
        let finishes: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                QedEvent::SubPipelineFinished {
                    name,
                    child_run_id,
                    status,
                    ..
                } => Some((name.clone(), child_run_id.clone(), *status)),
                _ => None,
            })
            .collect();

        assert_eq!(starts.len(), 2, "two SubPipelineStarted events");
        assert_eq!(finishes.len(), 2, "two SubPipelineFinished events");

        assert_eq!(starts[0].0, "compose-a");
        assert_eq!(starts[0].1, "builtin:child-a");
        assert_eq!(starts[1].0, "compose-b");
        assert_eq!(starts[1].1, "path:.yah/qed/child-b.toml");

        // Each finish pairs with the same step + child_run_id as its start,
        // and both children terminated Success.
        for (start, finish) in starts.iter().zip(finishes.iter()) {
            assert_eq!(start.0, finish.0, "start/finish name match");
            assert_eq!(start.2, finish.1, "start/finish child_run_id match");
            assert_eq!(finish.2, RunStatus::Success);
            assert_ne!(start.2, parent_run_id, "child run_id distinct from parent");
        }

        // Child events DO NOT leak onto the parent's stream: zero RunStarted
        // events for the children (only the parent's own RunStarted).
        let run_started_count = events
            .iter()
            .filter(|e| matches!(e, QedEvent::RunStarted { .. }))
            .count();
        assert_eq!(
            run_started_count, 1,
            "only parent's RunStarted on the parent stream"
        );
    }

    #[tokio::test]
    async fn sub_pipeline_finished_emits_failed_status_when_child_fails() {
        let child = make_pipeline("child", vec![shell_step("boom", vec!["false"])]);
        let root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runner = PipelineRunner::new(root)
            .with_events(tx)
            .with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);

        let mut finished = None;
        while let Ok(e) = rx.try_recv() {
            if let QedEvent::SubPipelineFinished { status, .. } = e {
                finished = Some(status);
            }
        }
        assert_eq!(
            finished,
            Some(RunStatus::Failed),
            "child failure surfaces on SubPipelineFinished.status"
        );
    }

    // ── R506 step gating tests ────────────────────────────────────────────

    fn gating_step(name: &str) -> crate::types::QedStep {
        crate::types::QedStep {
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
            name: name.to_string(),
            // echo always succeeds — distinguishes "ran" from "skipped" by
            // looking at the terminal status, not by relying on a failure.
            argv: vec!["echo".into(), "ran".into()],
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: crate::types::StepKind::Subprocess,
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

    #[tokio::test]
    async fn r506_enabled_false_step_is_skipped() {
        let mut s = gating_step("disabled");
        s.enabled = false;
        let pipeline = Pipeline {
            allow_late_operator_block: false,
                           participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Success,
            "skipped step doesn't fail the run"
        );
        assert_eq!(meta.steps[0].status, RunStatus::Skipped);
        // R330-B41: the persisted meta names the cause, not just the status.
        assert_eq!(
            meta.steps[0].error.as_deref(),
            Some("skipped: enabled = false")
        );
    }

    #[tokio::test]
    async fn r506_stubbed_step_is_skipped_by_default() {
        let mut s = gating_step("stubbed");
        s.activation = crate::types::StepActivation::Stubbed;
        let pipeline = Pipeline {
            allow_late_operator_block: false,
                           participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Skipped);
        // R330-B41: the persisted meta names the cause, not just the status.
        assert!(meta.steps[0]
            .error
            .as_deref()
            .unwrap()
            .contains("stubbed"));
    }

    #[tokio::test]
    async fn r506_include_stubbed_overrides_stubbed_marker() {
        let mut s = gating_step("stubbed");
        s.activation = crate::types::StepActivation::Stubbed;
        let pipeline = Pipeline {
            allow_late_operator_block: false,
                           participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline)
            .with_include_stubbed(true)
            .run()
            .await
            .unwrap();
        assert_eq!(
            meta.steps[0].status,
            RunStatus::Success,
            "--include-stubbed runs a stubbed step like an active one"
        );
    }

    #[tokio::test]
    async fn r506_include_stubbed_does_not_override_enabled_false() {
        let mut s = gating_step("disabled");
        s.enabled = false;
        s.activation = crate::types::StepActivation::Stubbed; // both knobs set
        let pipeline = Pipeline {
            allow_late_operator_block: false,
                           participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline)
            .with_include_stubbed(true)
            .run()
            .await
            .unwrap();
        assert_eq!(
            meta.steps[0].status,
            RunStatus::Skipped,
            "enabled = false always wins over --include-stubbed"
        );
    }

    #[tokio::test]
    async fn r506_if_falsy_skips_step() {
        let mut s = gating_step("conditional");
        s.if_cond = Some("matrix.target == 'ios-device'".into());
        let pipeline = Pipeline {
            allow_late_operator_block: false,
                           participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };
        let mut coord = indexmap::IndexMap::new();
        coord.insert(
            "target".to_string(),
            toml::Value::String("macos-native".into()),
        );
        let meta = PipelineRunner::new(pipeline)
            .with_matrix_coord(coord)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Skipped);
        // R330-B41: the persisted meta names the condition that gated it.
        assert!(meta.steps[0]
            .error
            .as_deref()
            .unwrap()
            .contains("matrix.target == 'ios-device'"));
    }

    #[tokio::test]
    async fn r506_if_truthy_runs_step() {
        let mut s = gating_step("conditional");
        s.if_cond = Some("matrix.target == 'ios-device'".into());
        let pipeline = Pipeline {
            allow_late_operator_block: false,
                           participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };
        let mut coord = indexmap::IndexMap::new();
        coord.insert(
            "target".to_string(),
            toml::Value::String("ios-device".into()),
        );
        let meta = PipelineRunner::new(pipeline)
            .with_matrix_coord(coord)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Success);
    }

    #[tokio::test]
    async fn r506_if_with_expression_delimiters_strips_braces() {
        let mut s = gating_step("conditional");
        s.if_cond = Some("${{ matrix.target == 'ios-device' }}".into());
        let pipeline = Pipeline {
            allow_late_operator_block: false,
                           participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };
        let mut coord = indexmap::IndexMap::new();
        coord.insert(
            "target".to_string(),
            toml::Value::String("ios-device".into()),
        );
        let meta = PipelineRunner::new(pipeline)
            .with_matrix_coord(coord)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Success);
    }

    // ── R653-F1: params.<name> in if= — a variable as a build VARIANT ─────
    //
    // `apply_params` substitutes a param into argv/env; these prove the
    // resolved values also reach the expression context, which is what lets a
    // param DECIDE whether a step runs. The declaration/resolution seam
    // (`resolve_params` filling defaults) is exercised too, because a param
    // that gates one way when supplied and another way when defaulted would be
    // the worst possible bug in this feature.

    /// A one-step pipeline whose only step is `s`, with `params` as the
    /// pipeline's `[params]` DECLARATIONS (not values).
    fn gating_pipeline(
        s: crate::types::QedStep,
        params: HashMap<String, crate::types::ParamDef>,
    ) -> Pipeline {
        Pipeline {
            allow_late_operator_block: false,
            participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params,
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
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

    fn param_def(default: Option<&str>, required: bool) -> crate::types::ParamDef {
        crate::types::ParamDef {
            required,
            description: None,
            default: default.map(|d| d.to_string()),
            options: Vec::new(),
            options_from: None,
            options_cmd: None,
        }
    }

    #[tokio::test]
    async fn r653_param_gates_step_on() {
        let mut s = gating_step("full-suite");
        s.if_cond = Some("params.variant == 'full'".into());
        let pipeline = gating_pipeline(s, HashMap::new());
        let meta = PipelineRunner::new(pipeline)
            .with_params(HashMap::from([("variant".to_string(), "full".to_string())]))
            .run()
            .await
            .unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Success);
    }

    #[tokio::test]
    async fn r653_param_gates_step_off() {
        let mut s = gating_step("full-suite");
        s.if_cond = Some("params.variant == 'full'".into());
        let pipeline = gating_pipeline(s, HashMap::new());
        let meta = PipelineRunner::new(pipeline)
            .with_params(HashMap::from([(
                "variant".to_string(),
                "quick".to_string(),
            )]))
            .run()
            .await
            .unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Skipped);
    }

    /// The acceptance shape from the ticket: the pipeline DECLARES a param with
    /// a default, the launch surface resolves it, and the gate sees the
    /// resolved value. Both legs of `resolve_params` are covered — the
    /// defaulted one here, the supplied one below.
    #[tokio::test]
    async fn r653_declared_default_reaches_the_gate() {
        let mut s = gating_step("full-suite");
        s.if_cond = Some("params.variant == 'full'".into());
        let pipeline = gating_pipeline(
            s,
            HashMap::from([("variant".to_string(), param_def(Some("full"), false))]),
        );
        // Nothing supplied — `resolve_params` fills the declared default, which
        // is exactly what both launch surfaces do before `apply_params`.
        let resolved = pipeline.resolve_params(&HashMap::new()).unwrap();
        assert_eq!(resolved.get("variant").map(String::as_str), Some("full"));
        let meta = PipelineRunner::new(pipeline)
            .with_params(resolved)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Success);
    }

    #[tokio::test]
    async fn r653_supplied_value_overrides_declared_default_at_the_gate() {
        let mut s = gating_step("full-suite");
        s.if_cond = Some("params.variant == 'full'".into());
        let pipeline = gating_pipeline(
            s,
            HashMap::from([("variant".to_string(), param_def(Some("full"), false))]),
        );
        let resolved = pipeline
            .resolve_params(&HashMap::from([(
                "variant".to_string(),
                "quick".to_string(),
            )]))
            .unwrap();
        let meta = PipelineRunner::new(pipeline)
            .with_params(resolved)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Skipped);
    }

    /// An unbound param is `Null` — falsy, not a parse error. Same semantics as
    /// an unset `matrix.<key>`, so a pipeline gated on a param nobody passed
    /// skips the step instead of failing the run.
    #[tokio::test]
    async fn r653_unset_param_is_falsy_not_an_error() {
        let mut s = gating_step("full-suite");
        s.if_cond = Some("params.variant == 'full'".into());
        let pipeline = gating_pipeline(s, HashMap::new());
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Skipped);
        assert_eq!(
            meta.status,
            RunStatus::Success,
            "a skipped step must not fail the run"
        );
    }

    /// `params` is a namespace alongside the existing ones, not a replacement:
    /// a gate can combine it with `matrix.` in one expression.
    #[tokio::test]
    async fn r653_params_compose_with_matrix_in_one_expression() {
        let mut s = gating_step("full-suite");
        s.if_cond = Some("params.variant == 'full' && matrix.target == 'ios-device'".into());
        let pipeline = gating_pipeline(s, HashMap::new());
        let mut coord = indexmap::IndexMap::new();
        coord.insert(
            "target".to_string(),
            toml::Value::String("ios-device".into()),
        );
        let meta = PipelineRunner::new(pipeline)
            .with_params(HashMap::from([("variant".to_string(), "full".to_string())]))
            .with_matrix_coord(coord)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Success);
    }

    // ── R506 phase 2: success()/failure()/always()/cancelled() ────────────
    //
    // The runner tracks the cumulative `overall_status` mid-run and feeds it
    // into the expr context as `job_status` so an `if=` can ask "did anything
    // fail above me?". `cancelled()` is always false from inside a step gate
    // because cancellation aborts the whole future, never reaches the next
    // step (matches GHA semantics).

    fn failing_step(name: &str) -> crate::types::QedStep {
        let mut s = gating_step(name);
        // `false` exits non-zero on every Unix host — simplest deterministic
        // failure that doesn't depend on a missing binary.
        s.argv = vec!["false".into()];
        s.on_fail = OnFail::Continue;
        s
    }

    #[tokio::test]
    async fn r506_if_always_runs_after_failure() {
        let mut gated = gating_step("cleanup");
        gated.if_cond = Some("always()".into());
        let pipeline = Pipeline {
            allow_late_operator_block: false,
                           participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![failing_step("bad"), gated],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
        assert_eq!(
            meta.steps[1].status,
            RunStatus::Success,
            "always() runs even after a prior failure"
        );
    }

    #[tokio::test]
    async fn r506_if_failure_runs_only_after_failure() {
        let mut gated = gating_step("only-on-fail");
        gated.if_cond = Some("failure()".into());
        let pipeline = Pipeline {
            allow_late_operator_block: false,
                           participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![failing_step("bad"), gated],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.steps[1].status, RunStatus::Success);
    }

    #[tokio::test]
    async fn r506_if_failure_skips_when_all_green() {
        let mut gated = gating_step("only-on-fail");
        gated.if_cond = Some("failure()".into());
        let pipeline = Pipeline {
            allow_late_operator_block: false,
                           participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![gating_step("ok"), gated],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.steps[1].status, RunStatus::Skipped);
    }

    #[tokio::test]
    async fn r506_if_success_skips_after_failure() {
        let mut gated = gating_step("only-on-success");
        gated.if_cond = Some("success()".into());
        let pipeline = Pipeline {
            allow_late_operator_block: false,
                           participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![failing_step("bad"), gated],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.steps[1].status, RunStatus::Skipped);
    }

    #[tokio::test]
    async fn r506_if_cancelled_is_always_false_mid_run() {
        let mut gated = gating_step("on-cancel");
        gated.if_cond = Some("cancelled()".into());
        let pipeline = Pipeline {
            allow_late_operator_block: false,
                           participants: None,
            max_parallel: None,
            description: None,
            tags: Vec::new(),
            name: "p".into(),
            label: "p".into(),
            steps: vec![gated],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            environment: crate::types::Environment::Any,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(
            meta.steps[0].status,
            RunStatus::Skipped,
            "cancelled() is unreachable from inside an if= gate; always evaluates false"
        );
    }

    // ── R823-F2 participant sets ─────────────────────────────────────────────

    use crate::participants::{
        Participant, ParticipantOutcome, ParticipantSet, ParticipantSpec, Verdict,
        ENV_PARTICIPANTS, ENV_PARTICIPANT_SELF,
    };

    fn participant_set(src: &str) -> ParticipantSet {
        toml::from_str(src).expect("participant set parses")
    }

    /// The rendezvous actually reaches the process. Asserted by running a real
    /// step rather than by inspecting an `ExecContext`, because the failure this
    /// guards against — env built in one code path and not another — is
    /// invisible to a test that reads the same map the code wrote.
    #[tokio::test]
    async fn rendezvous_env_reaches_a_local_participant_step() {
        let camp = tempfile::tempdir().unwrap();
        let mut step = shell_step(
            "runner",
            vec![
                "sh",
                "-c",
                "printf %s \"$QED_PARTICIPANTS\" > peers; printf %s \"$QED_PARTICIPANT_SELF\" > self",
            ],
        );
        step.participant = Some("runner".into());
        let mut pipeline = make_pipeline("rendezvous", vec![step]);
        pipeline.participants = Some(participant_set(
            r#"
            [role.runner]
            coordinator = true
            ports = ["control"]
        "#,
        ));

        let meta = PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success, "{:?}", meta.failure_reason);

        assert_eq!(
            std::fs::read_to_string(camp.path().join("self")).unwrap(),
            "runner",
        );
        let peers: Vec<Participant> =
            serde_json::from_str(&std::fs::read_to_string(camp.path().join("peers")).unwrap())
                .expect("QED_PARTICIPANTS is the documented JSON array");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].address, crate::participants::LOCAL_ADDRESS);
        assert_eq!(
            peers[0].ports["control"],
            crate::participants::DEFAULT_PORT_BASE
        );
    }

    /// A `QED_PARTICIPANTS` literal in the recipe loses to the allocated one.
    /// The ports were assigned by THIS run, so a value written in the TOML can
    /// only be a copy of a previous run's addressing — honouring it would point
    /// the participant at a peer that isn't there.
    #[tokio::test]
    async fn the_allocated_rendezvous_outranks_a_stale_literal() {
        let camp = tempfile::tempdir().unwrap();
        let mut step = shell_step(
            "runner",
            vec!["sh", "-c", "printf %s \"$QED_PARTICIPANTS\" > peers"],
        );
        step.participant = Some("runner".into());
        step.env
            .insert(ENV_PARTICIPANTS.to_string(), "[STALE]".to_string());
        let mut pipeline = make_pipeline("rendezvous-precedence", vec![step]);
        pipeline.participants = Some(participant_set(
            r#"
            [role.runner]
            coordinator = true
        "#,
        ));

        PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        let written = std::fs::read_to_string(camp.path().join("peers")).unwrap();
        assert_ne!(written, "[STALE]");
        assert!(written.contains("\"runner\""), "{written}");
    }

    /// A step with no `participant` still sees the set — it may have to bake an
    /// address into something — but is told no identity.
    #[tokio::test]
    async fn an_unbound_step_sees_the_set_but_has_no_self() {
        let camp = tempfile::tempdir().unwrap();
        let mut runner_step = shell_step("runner", vec!["true"]);
        runner_step.participant = Some("runner".into());
        let probe = shell_step(
            "probe",
            vec![
                "sh",
                "-c",
                "printf %s \"$QED_PARTICIPANTS\" > peers; printf '[%s]' \"$QED_PARTICIPANT_SELF\" > self",
            ],
        );
        let mut pipeline = make_pipeline("unbound", vec![runner_step, probe]);
        pipeline.participants = Some(participant_set(
            r#"
            [role.runner]
            coordinator = true
        "#,
        ));

        PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert!(std::fs::read_to_string(camp.path().join("peers"))
            .unwrap()
            .contains("\"runner\""));
        assert_eq!(
            std::fs::read_to_string(camp.path().join("self")).unwrap(),
            "[]",
        );
    }

    /// A node-bound participant is placed by its binding even on a runner
    /// forced local. There is no honest local fallback for a rendezvous: the
    /// peers were told this participant's address before dispatch, so running
    /// it here would leave it answering somewhere nobody is calling.
    #[test]
    fn a_node_bound_participant_outranks_a_forced_local_runner() {
        let mut step = shell_step("responder", vec!["true"]);
        step.participant = Some("responder".into());
        let mut pipeline = make_pipeline("pinned", vec![step.clone()]);
        pipeline.participants = Some(participant_set(
            r#"
            [role.responder]
            coordinator = true
            node    = "us-west-011"
            address = "100.64.0.11"
        "#,
        ));
        let runner = PipelineRunner::new(pipeline);
        assert_eq!(runner.run_where, RunWhere::Local);
        assert_eq!(runner.effective_placement(&step), RunWhere::Remote);
    }

    /// A local participant is placed exactly as any other step — the field is
    /// not a placement override in general, only for a node-bound role.
    #[test]
    fn a_local_participant_does_not_disturb_placement() {
        let mut step = shell_step("runner", vec!["true"]);
        step.participant = Some("runner".into());
        let mut pipeline = make_pipeline("local-participant", vec![step.clone()]);
        pipeline.participants = Some(participant_set(
            r#"
            [role.runner]
            coordinator = true
        "#,
        ));
        let runner = PipelineRunner::new(pipeline);
        assert_eq!(runner.effective_placement(&step), RunWhere::Local);
    }

    /// The R513-F2 refusal still fires for a background step that offloads
    /// WITHOUT being a participant, and its message now routes the author to the
    /// feature that does support it.
    #[tokio::test]
    async fn a_non_participant_background_step_may_still_not_offload() {
        let mut step = shell_step("sidecar", vec!["sleep", "60"]);
        step.background = true;
        step.platform = Some(crate::platform::PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            native: true,
            ..Default::default()
        });
        let pipeline = make_pipeline("bg-offload", vec![step]);
        let dir = TempDir::new().unwrap();
        let err = PipelineRunner::new_auto(
            pipeline,
            make_scryer(&dir),
            Arc::new(ScriptedWarden::new(vec![], 0)),
        )
        .with_host_triple("aarch64-apple-darwin")
        .run()
        .await
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("background steps run locally only"), "{msg}");
        assert!(msg.contains("[pipeline.participants]"), "{msg}");
    }

    /// …and a participant sidecar gets PAST that refusal, failing instead on
    /// the thing that is genuinely missing here — a wired dispatcher. Proving
    /// it by the error it reaches is the only way to show the preflight let it
    /// through without standing up a fleet.
    #[tokio::test]
    async fn a_remote_participant_sidecar_clears_the_background_preflight() {
        let camp = tempfile::tempdir().unwrap();
        let mut sidecar = shell_step("responder", vec!["sleep", "60"]);
        sidecar.background = true;
        sidecar.background_until = Some("runner".into());
        sidecar.participant = Some("responder".into());
        let mut runner_step = shell_step("runner", vec!["true"]);
        runner_step.participant = Some("runner".into());

        let mut pipeline = make_pipeline("participant-sidecar", vec![sidecar, runner_step]);
        pipeline.participants = Some(participant_set(
            r#"
            [role.runner]
            coordinator = true
            [role.responder]
            node    = "us-west-011"
            address = "100.64.0.11"
            ports   = ["clock"]
        "#,
        ));

        let err = PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            !msg.contains("background steps run locally only"),
            "preflight should no longer refuse a participant sidecar: {msg}"
        );
        assert!(msg.contains("no remote dispatcher is wired"), "{msg}");
        assert!(msg.contains("us-west-011"), "{msg}");
    }

    /// A mis-declared set refuses the RUN, not just the load — a `Pipeline`
    /// built in code never passes through `PipelineLoader`.
    #[tokio::test]
    async fn a_mis_declared_set_refuses_the_run_before_any_step() {
        let mut step = shell_step("boom", vec!["sh", "-c", "touch ran"]);
        step.participant = Some("nobody".into());
        let camp = tempfile::tempdir().unwrap();
        let mut pipeline = make_pipeline("bad-set", vec![step]);
        pipeline.participants = Some(participant_set(
            r#"
            [role.runner]
            coordinator = true
        "#,
        ));
        let err = PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("names no declared role"), "{err}");
        assert!(
            !camp.path().join("ran").exists(),
            "no step may run once the set is known to be wrong"
        );
    }

    // ── participant_reports: step rows → outcomes ─────────────────────────────

    fn reports_fixture(
        rows: Vec<Option<RunStatus>>,
        never: &[usize],
    ) -> Vec<crate::participants::ParticipantReport> {
        let mut set = ParticipantSet::default();
        set.roles.insert(
            "runner".into(),
            ParticipantSpec {
                coordinator: true,
                ..Default::default()
            },
        );
        set.roles.insert(
            "responder".into(),
            ParticipantSpec {
                node: Some("us-west-011".into()),
                address: Some("100.64.0.11".into()),
                ..Default::default()
            },
        );
        let plan = set.plan().unwrap();

        let names = ["runner", "responder"];
        let steps: Vec<crate::types::QedStep> = names
            .iter()
            .map(|n| {
                let mut s = shell_step(n, vec!["true"]);
                s.participant = Some((*n).to_string());
                s
            })
            .collect();
        let status_rows: Vec<Option<StepStatus>> = rows
            .into_iter()
            .enumerate()
            .map(|(i, status)| {
                status.map(|status| StepStatus {
                    name: names[i].to_string(),
                    task_run_id: None,
                    status,
                    started_at: None,
                    completed_at: None,
                    error: Some("boom".into()),
                    outputs: Default::default(),
                    applied_binds: Vec::new(),
                    jobs: Vec::new(),
                    input_hashes: Default::default(),
                })
            })
            .collect();
        participant_reports(
            &plan,
            &steps,
            &status_rows,
            &never.iter().copied().collect(),
        )
    }

    #[test]
    fn all_rows_green_is_one_passing_verdict() {
        let reports = reports_fixture(
            vec![Some(RunStatus::Success), Some(RunStatus::Success)],
            &[],
        );
        assert_eq!(verdict_of(&reports), Verdict::Pass);
    }

    fn verdict_of(reports: &[crate::participants::ParticipantReport]) -> Verdict {
        crate::participants::verdict(reports)
    }

    #[test]
    fn a_peer_whose_workload_was_never_accepted_reads_as_never_started() {
        // The distinction the whole feature turns on: the responder's step row
        // says Failed (its waiter returned an error), but the workload id was
        // never minted, so nothing ran on any node. Reporting this as a test
        // failure would send an operator to read the code.
        let reports = reports_fixture(
            vec![Some(RunStatus::Success), Some(RunStatus::Failed)],
            &[1],
        );
        assert!(matches!(
            reports[1].outcome,
            ParticipantOutcome::NeverStarted { .. }
        ));
        let Verdict::Fail { summary } = verdict_of(&reports) else {
            panic!("expected a failure")
        };
        assert!(summary.contains("fleet fault"), "{summary}");
        assert!(summary.contains("us-west-011"), "{summary}");
    }

    #[test]
    fn a_participant_the_run_never_reached_reads_as_never_started() {
        let reports = reports_fixture(vec![Some(RunStatus::Success), None], &[]);
        assert!(matches!(
            reports[1].outcome,
            ParticipantOutcome::NeverStarted { .. }
        ));
    }

    #[test]
    fn a_skipped_participant_is_never_started_not_completed() {
        // Vacuous truth is the wrong answer: `if = false` on the only step of a
        // participant means it contributed nothing, and a set that reports Pass
        // on that has verified nothing about the peer.
        let reports = reports_fixture(
            vec![Some(RunStatus::Success), Some(RunStatus::Skipped)],
            &[],
        );
        assert!(matches!(
            reports[1].outcome,
            ParticipantOutcome::NeverStarted { .. }
        ));
    }

    #[test]
    fn a_participant_that_started_and_failed_reads_as_failed() {
        let reports = reports_fixture(
            vec![Some(RunStatus::Success), Some(RunStatus::Failed)],
            &[],
        );
        match &reports[1].outcome {
            ParticipantOutcome::Failed { detail } => {
                assert!(detail.contains("responder"), "{detail}");
                assert!(detail.contains("boom"), "{detail}");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn participant_reports_name_the_coordinator() {
        let reports = reports_fixture(
            vec![Some(RunStatus::Success), Some(RunStatus::Success)],
            &[],
        );
        assert!(reports[0].coordinator);
        assert!(!reports[1].coordinator);
    }

    /// A participant failure surfaces as the run's `failure_reason`, not only
    /// in a log line — that field is what `qed.status` and the desktop card
    /// read, and a verdict nobody can see is not a verdict.
    #[tokio::test]
    async fn a_failing_participant_lands_in_the_runs_failure_reason() {
        let camp = tempfile::tempdir().unwrap();
        let mut coordinator = shell_step("runner", vec!["true"]);
        coordinator.participant = Some("runner".into());
        let mut peer = shell_step("peer", vec!["sh", "-c", "exit 3"]);
        peer.participant = Some("peer".into());
        peer.on_fail = OnFail::Continue;

        let mut pipeline = make_pipeline("failing-peer", vec![coordinator, peer]);
        pipeline.participants = Some(participant_set(
            r#"
            [role.runner]
            coordinator = true
            [role.peer]
        "#,
        ));

        let meta = PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let reason = meta.failure_reason.expect("participant verdict is recorded");
        assert!(reason.contains("peer"), "{reason}");
    }

    /// The set is silent on a pipeline that declares none — no env, no verdict,
    /// no behaviour change. This is the regression guard for every existing
    /// pipeline in every camp.
    #[tokio::test]
    async fn a_pipeline_without_a_set_is_untouched() {
        let camp = tempfile::tempdir().unwrap();
        let step = shell_step(
            "probe",
            vec!["sh", "-c", "printf '[%s]' \"$QED_PARTICIPANTS\" > peers"],
        );
        let meta = PipelineRunner::new(make_pipeline("no-set", vec![step]))
            .with_camp_root(camp.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert!(meta.failure_reason.is_none());
        assert_eq!(
            std::fs::read_to_string(camp.path().join("peers")).unwrap(),
            "[]",
        );
        let _ = ENV_PARTICIPANT_SELF;
    }
}
