//! Live per-step events emitted by [`crate::runner::PipelineRunner`] as a
//! pipeline executes (R325-F2).
//!
//! A runner constructed with [`crate::runner::PipelineRunner::with_events`]
//! pushes a [`QedEvent`] onto an unbounded channel at each lifecycle boundary:
//! the run starts, each step starts, every stdout/stderr line, each step
//! finishes, the run finishes. The camp daemon drains these into a per-run
//! buffer that `qed.tail` serves as a cursor-tailable feed; the CLI prints
//! them to the console as they arrive.
//!
//! Without a sink the runner is silent — `run()` still returns the terminal
//! [`crate::types::QedRunMeta`], so existing callers are unaffected.
//!
//! @yah:ticket(R488-F5, "Event-stream wiring + desktop QED-pane nested-tree render for sub-pipelines")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:54:33Z)
//! @yah:status(review)
//! @yah:phase(P5)
//! @yah:parent(R488)
//! @yah:next("New event variants: sub_pipeline_started / sub_pipeline_finished emit on the parent's run with the child run_id")
//! @yah:next("DB: parent_run_id foreign key on the runs table; read paths join when surfacing the tree")
//! @yah:next("app/yah/desktop: QED pane renders nested sub-pipelines as expandable subtrees with own status pills")
//! @yah:verify("Run a 2-child composite in dev; desktop pane shows both children as expandable rows under the parent")
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @yah:depends_on(R488-F2)
//! @yah:tier(Cleric)
//! @yah:handoff("F5 shipped the structural surface for nested sub-pipeline event tracking. (1) qed::QedEvent gained SubPipelineStarted{index,name,target,child_run_id,at} and SubPipelineFinished{index,name,child_run_id,status,at} variants. (2) runner.rs::execute_step_sub_pipeline now emits these bookends around child recursion; new helper sub_pipeline_target_label() renders the resolver-token string (builtin:<n> | path:<p> | gha:<p>). The child runner's `events` sink is now decoupled (None) — previously F2 shared the parent's sink, which caused apply_qed_event_to_meta to push child steps onto the parent's StepStatus list, corrupting the parent's run snapshot. Bookends restore that information at the right altitude. (3) qed::QedRunMeta gained parent_run_id: Option<QedRunId> with serde(default, skip_serializing_if). Runner threads it into child runs via a new field on PipelineRunner; top-level runs leave it None. Persistence (JSON shards under .yah/jit/qed/) is automatic — no DB schema change needed because run history is JSON, not SQL. (4) rpc::QedRunWire grew parent_run_id; QedEventWire grew SubPipelineStarted/Finished mirrors (kebab-case, RFC3339 ts). camp.rs qed_meta_to_wire + qed_event_to_wire converters updated; apply_qed_event_to_meta ignores the bookends (parent's step list stays untouched — the StepStarted/StepFinished around the SubPipeline parent step already track it). (5) Two new runner tests: sub_pipeline_emits_started_finished_bookends_with_child_run_id (asserts pairing by child_run_id, distinct from parent run_id, child events DO NOT leak onto parent stream) and sub_pipeline_finished_emits_failed_status_when_child_fails. qed --lib: 196 pass + 1 pre-existing unrelated failure (test_builtin_release_build_pipeline 4-vs-6, documented across R380-T3/R381-T2/R407 handoffs). cargo check --workspace clean.")
//! @yah:next("Daemon-side child run registration: today the child's QedRun is run inline by the parent runner inside qed_run_handler; the child's events vanish (sink decoupled to keep parent's meta clean) and the child isn't in the qed_runs HashMap, so qed.tail{run_id=child_run_id} returns null. To make child runs tail-able as their own stream, qed_run_handler needs to (a) intercept the new SubPipelineStarted to register a fresh QedRunState with parent_run_id set, (b) inject a per-child sink so the child runner's events drain into THAT buffer (requires runner-side hook — perhaps OutcomeDispatcher-style 'spawn_child_sink' or a new SubPipelineSinkProvider trait), (c) flush + persist on SubPipelineFinished. Bigger change than F5 — file as a followup ticket under R488 or a fresh relay.")
//! @yah:next("Desktop QED pane render: no TS pane exists yet in app/yah/web or the desktop frontend (R325 was 'QED desktop UI blank slate'). The structural surface is in place; render lands when the pane is built. Treat the F5 verify ('desktop pane shows both children as expandable rows') as deferred until that pane exists.")
//! @yah:next("yah qed run CLI: the live tail loop in app/yah/cli/src/qed.rs prints StepStarted + Output but does not yet handle SubPipelineStarted/Finished. Small followup to print a nested indent (e.g. '↳ sub-pipeline started: builtin:child-a (run_id=...)' / '↲ finished: success').")
//! @yah:verify("cargo test -p qed --lib sub_pipeline  # 17 pass incl. 2 new F5 tests")
//! @yah:verify("cargo test -p qed --lib  # 196 pass + 1 pre-existing unrelated failure")
//! @yah:verify("cargo check --workspace  # clean")
//! @yah:gotcha("Child runner's events sink is intentionally decoupled (None) — previously F2 shared parent's sink and child Step events corrupted the parent's QedRunMeta.steps via apply_qed_event_to_meta. Don't re-couple without first adding per-event run_id discrimination or a per-child sink provider.")
//! @yah:gotcha("QedRunMeta.parent_run_id is serde(default, skip_serializing_if=Option::is_none) so existing on-disk shards in .yah/jit/qed/ load fine; no migration.")
//!
//! @yah:ticket(R365-F20, "session_started: record dispatchKind (assist|dispatch); mirror into meta.json")
//! @yah:status(review)
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-10T01:58:23Z)
//! @yah:parent(R365)
//! @yah:next("Add dispatchKind: 'assist' | 'dispatch' field to session_started event in crates/yah/qed/src/events.rs and emit it from the party.assist / party.dispatch / subagent.spawn paths in crates/yah/agent-tools/src/tools.rs.")
//! @yah:next("Mirror parentSessionId + dispatchDepth + dispatchKind from session_started into the meta.json sidecar so board.session and sidecar-readers see parentage without parsing the jsonl head.")
//! @yah:next("Backfill: existing sessions have parentSessionId in jsonl but no dispatchKind — default unknown sessions to 'dispatch' on read (the common case) so old sessions don't crash the renderer.")
//! @yah:gotcha("Observed 2026-06-09 on R365-T14: party.assist and subagent.spawn produce byte-identical session_started events for the same Yamli character (parentSessionId, dispatchDepth, agentId all match). UI can't tell which back-link should produce nesting in non-compact Party Column rendering. meta.json sidecar carries no parentage info at all — anything reading sessions via sidecar (board.session lookups, future tooling) is parent-blind.")
//! @arch:see(.yah/docs/working/W167-smoke-matrix-plan.md)
//! @yah:handoff("Added dispatchKind (Assist|Dispatch) to AgentEvent::SessionStarted and mirrored parentSessionId/dispatchDepth/dispatchKind into the meta.json sidecar.\n\nPath correction: the ticket's source pointer (crates/yah/qed/src/events.rs, and agent-tools/src/tools.rs for the tool paths) was wrong on both counts -- those are unrelated types (qed's pipeline-run QedEvent; tools.rs is the read-only KG tool registry). The real session_started type is AgentEvent::SessionStarted in crates/yah/party/src/agent.rs; the real party.assist/party.dispatch/subagent.spawn implementations are PartyAssist/PartyDispatch/AgentSpawn in crates/yah/agent-tools/src/agent_dispatch_tools.rs, which all funnel through one daemon-side fn (subagent_spawn in app/yah/cli/src/camp.rs) via party_dispatch.\n\nSemantics: party.assist, party.dispatch, and subagent.spawn ALL mint the identical sub-relationship child today (parent_session_id + dispatch_depth+1, nest-worthy) -- confirmed by the PartyDispatch tool docstring ('sub-relationship... assistant') and by the FE's own isAssistRelationship fallback comment in nestedAssistChildren.test.ts ('gate on parentInfo.dispatchKind === \"assist\"'). So all three stamp DispatchKind::Assist; DispatchKind::Dispatch is reserved for the not-yet-built peer-booking party.book verb.\n\nPlumbing (mirrors the existing parent_session_id/dispatch_depth rails exactly): RPC method match in camp.rs -> subagent_spawn/party_dispatch (new dispatch_kind param) -> LaunchCharacterParams.dispatch_kind -> launch_character_session's 4 engine forks -> start_claude_session/start_runner_session/start_codex_oauth_session/agent_process::start_process_session (new trailing param) -> ClaudeSessionInit/RunnerSessionInit.dispatch_kind -> register_claude_session/register_runner_session stamp AgentEvent::SessionStarted.dispatch_kind AND SessionPromptMeta.dispatch_kind. Also extended recover_dispatch_lineage (desktop/agent.rs, R534-B5 helper) to a 3-tuple so all 6 resume/fork/rewind paths re-emit dispatch_kind, not just parent_session_id/dispatch_depth.\n\nBackfill: dispatch_kind is Option<DispatchKind> with #[serde(default)] end to end -- old JSONL/meta.json missing the field deserialize as None, never crash. Per the ticket's literal instruction, documented on the field that a reader needing a concrete value for a parented session should default a missing value to Dispatch (guidance for the consumer, e.g. R365-F21's renderer).\n\nEvery AgentEvent::SessionStarted construction site in the repo was updated and verified programmatically: party, runner, agent-tools, hub, hub-tauri crates + the yah CLI daemon (camp.rs, 4 RunnerSessionInit sites) + desktop (agent.rs, agent_process.rs, agent_eval.rs, keepalive_subsystem.rs, camp_socket.rs).")
//! @yah:verify("cargo test -p party --lib -- agent:: (5/5 pass)")
//! @yah:verify("cargo test -p runner --lib -- sessions:: session:: sink:: (56/56 pass, incl. list_summaries_preserves_claude_path_dispatch_lineage)")
//! @yah:verify("cargo test -p hub --lib (43/43 pass)")
//! @yah:verify("cargo check clean on party, hub-tauri, hub, agent-tools, yah (lib), desktop (lib)")
//! @yah:gotcha("Two pre-existing/unrelated blockers on this shared tree (confirmed via git diff showing Cargo.toml churn from a concurrent peer, R409-T9): (1) turso-vs-yah #[global_allocator] conflict blocks any test-profile build of yah/desktop (lib-only cargo check unaffected); (2) agent-tools' envoy_tools.rs test module references an undeclared anyhow dev-dependency, blocking cargo test -p agent-tools --lib (verified by hand that all 7 test-fixture edits there set dispatch_kind; cargo check -p agent-tools non-test is clean).")

use chrono::{DateTime, Utc};

use crate::types::RunStatus;

/// Return the sorted set of environment variable names from `env_iter`
/// whose names look like credentials. Names only — values are never read,
/// so this is safe to log to the event stream and the operator UI.
///
/// A name is considered credential-shaped when it either:
/// - ends in `TOKEN`, `KEY`, `SECRET`, `PASSWORD`, or `CREDENTIAL`
///   (case-insensitive, after splitting on `_`); or
/// - starts with a known credential prefix: `HETZNER_`, `CLOUDFLARE_`,
///   `CF_`, `AWS_`, `R2_`, `GITHUB_`, `GH_`, `HUGGINGFACE_`, `HF_`,
///   `ANTHROPIC_`, `OPENAI_`.
///
/// Anything else (PATH, HOME, USER, etc.) is filtered out — the goal is a
/// readable chip row, not an `env` dump.
pub fn credential_env_keys<I, K>(env_iter: I) -> Vec<String>
where
    I: IntoIterator<Item = (K, K)>,
    K: AsRef<str>,
{
    const SUFFIXES: &[&str] = &["TOKEN", "KEY", "SECRET", "PASSWORD", "CREDENTIAL"];
    const PREFIXES: &[&str] = &[
        "HETZNER_",
        "CLOUDFLARE_",
        "CF_",
        "AWS_",
        "R2_",
        "GITHUB_",
        "GH_",
        "HUGGINGFACE_",
        "HF_",
        "ANTHROPIC_",
        "OPENAI_",
    ];
    let mut keys: Vec<String> = env_iter
        .into_iter()
        .map(|(k, _)| k.as_ref().to_string())
        .filter(|k| {
            let upper = k.to_ascii_uppercase();
            if PREFIXES.iter().any(|p| upper.starts_with(p)) {
                return true;
            }
            // Split on '_' so e.g. `MY_API_KEY` matches but `KEYBOARD` doesn't.
            upper
                .rsplit('_')
                .next()
                .map(|tail| SUFFIXES.contains(&tail))
                .unwrap_or(false)
        })
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_env_keys_filters_by_suffix_and_prefix() {
        let env = [
            ("PATH", "/usr/bin"),
            ("HOME", "/home/u"),
            ("KEYBOARD", "us"),               // suffix-like but no underscore
            ("HETZNER_S3_ACCESS_KEY", "xxx"), // prefix + suffix
            ("CF_API_TOKEN", "xxx"),          // prefix + suffix
            ("MY_API_KEY", "xxx"),            // suffix only
            ("HF_TOKEN", "xxx"),              // prefix
            ("DATABASE_PASSWORD", "xxx"),     // suffix
            ("RANDOM_VAR", "xxx"),
        ];
        let keys = credential_env_keys(env.iter().map(|(k, v)| (*k, *v)));
        assert_eq!(
            keys,
            vec![
                "CF_API_TOKEN".to_string(),
                "DATABASE_PASSWORD".to_string(),
                "HETZNER_S3_ACCESS_KEY".to_string(),
                "HF_TOKEN".to_string(),
                "MY_API_KEY".to_string(),
            ]
        );
    }
}

/// Which standard stream a line of step output came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// A live event emitted while a pipeline executes.
///
/// Step `index` is 0-based and aligns with `Pipeline::steps`. Steps run
/// strictly in sequence, so a consumer sees `StepStarted { index: i }` before
/// any `StepOutput { index: i, .. }` and before `StepFinished { index: i }`,
/// and indices arrive monotonically.
#[derive(Debug, Clone)]
pub enum QedEvent {
    /// Emitted once, immediately after registration, when the run is
    /// holding for its `concurrency_key` lock. For pipelines that
    /// opt out (`concurrency_key = "@parallel"`), this event still
    /// fires but is immediately followed by `RunStarted` — the queue
    /// hop is just instantaneous.
    RunQueued { key: String, at: DateTime<Utc> },
    /// Emitted once, before the first step. For queued runs this fires
    /// when the key lock is acquired; for parallel pipelines it fires
    /// right after `RunQueued`.
    RunStarted {
        total_steps: usize,
        at: DateTime<Utc>,
    },
    /// A step is about to execute.
    ///
    /// `argv` is the substituted command line for Subprocess-kind steps
    /// (empty for BuildImage / PackageNativeTarball / etc. — those have
    /// their own command shape). `env_keys` is the set of credential-shaped
    /// environment variable names that were present in the runner's
    /// environment at spawn time (see [`credential_env_keys`]) — KEYS
    /// ONLY, never values. Surfaced in the QED detail pane so an operator
    /// can confirm at a glance which secrets the step inherited without
    /// shell-pasting or re-running.
    StepStarted {
        index: usize,
        name: String,
        argv: Vec<String>,
        env_keys: Vec<String>,
        at: DateTime<Utc>,
    },
    /// A step was dispatched to a remote build-worker and now has a durable
    /// yubaba workload identity (`forge_id`), emitted BEFORE the runner blocks
    /// on `handle.wait()`. This is the reattach anchor (R603-T1): the camp
    /// daemon stamps `forge_id` onto the running step's `task_run_id` and
    /// persists a non-terminal `<run_id>.json` so that, if the daemon restarts
    /// mid-build, boot reconcile (R603-T2) can re-poll the workload by this id
    /// instead of orphaning it. Local steps never emit this.
    StepRemoteDispatched {
        index: usize,
        name: String,
        forge_id: String,
        at: DateTime<Utc>,
    },
    /// One line of stdout/stderr captured from the executing step (local runs).
    StepOutput {
        index: usize,
        name: String,
        stream: OutputStream,
        line: String,
    },
    /// A step reached a terminal status (`Success` or `Failed`).
    ///
    /// `msg` carries the failure tail (e.g. the last lines of cargo stderr)
    /// when `status == Failed`; `None` on success. Consumers render this as a
    /// red banner above the per-step log so the operator sees *why* without
    /// having to scroll through the streamed output. Empty string is treated
    /// the same as `None` by the UI.
    StepFinished {
        index: usize,
        name: String,
        status: RunStatus,
        msg: Option<String>,
        at: DateTime<Utc>,
    },
    /// Emitted once, after the last executed step (or after an aborting failure).
    RunFinished {
        status: RunStatus,
        at: DateTime<Utc>,
    },
    /// A `kind = "sub-pipeline"` step (W201) began recursion into a child
    /// pipeline. Carries the child's run id so a consumer can pivot to
    /// `qed.tail { run_id = child_run_id }` for the child's step-level
    /// detail; the parent's own stream does NOT carry the child's events
    /// (decoupled to keep parent's `apply_qed_event_to_meta` step list
    /// uncorrupted). `target` is the resolver token (`builtin:<name>`,
    /// `path:<path>`, `gha:<path>`) — same discipline as the F1 walker.
    SubPipelineStarted {
        index: usize,
        name: String,
        target: String,
        child_run_id: String,
        at: DateTime<Utc>,
    },
    /// A `kind = "sub-pipeline"` child run reached a terminal status. Pairs
    /// with the prior `SubPipelineStarted` event by `child_run_id`.
    SubPipelineFinished {
        index: usize,
        name: String,
        child_run_id: String,
        status: RunStatus,
        at: DateTime<Utc>,
    },
    /// A job instance inside a `kind = "gha-workflow"` step began executing
    /// (W200 R487 follow-up). `index` is the qed step index of the enclosing
    /// gha-workflow step; `job_key` is `yah_qed_gha::JobInstance::key()`
    /// (`"<job>"` for non-matrix, `"<job>#<row>"` for matrix). The receiver
    /// uses `(index, job_key)` to scope the per-job subtree under the
    /// parent step.
    GhaJobStarted {
        index: usize,
        name: String,
        job_id: String,
        matrix_index: Option<usize>,
        job_key: String,
        total_steps: usize,
        at: DateTime<Utc>,
    },
    /// One step inside a gha-workflow job is about to run. `action_kind` is
    /// `"run"` for bash steps and `"uses:<slug>"` for action invocations.
    GhaStepStarted {
        index: usize,
        name: String,
        job_key: String,
        step_index: usize,
        step_id: Option<String>,
        step_name: Option<String>,
        action_kind: String,
        at: DateTime<Utc>,
    },
    /// One line of stdout/stderr captured from a gha-workflow bash step.
    GhaStepOutput {
        index: usize,
        name: String,
        job_key: String,
        step_index: usize,
        stream: OutputStream,
        line: String,
    },
    /// A gha-workflow step reached a terminal conclusion. `msg` carries a
    /// stderr tail on failure (mirroring the qed-runner StepFinished.msg
    /// convention so the receiver can render a red banner uniformly).
    /// `conclusion` is `"success" | "failure" | "skipped"`.
    GhaStepFinished {
        index: usize,
        name: String,
        job_key: String,
        step_index: usize,
        conclusion: String,
        msg: Option<String>,
        at: DateTime<Utc>,
    },
    /// A gha-workflow job instance reached a terminal result. `result` is
    /// `"success" | "failure" | "cancelled" | "skipped"`. Pairs with the
    /// prior `GhaJobStarted` by `(index, job_key)`.
    GhaJobFinished {
        index: usize,
        name: String,
        job_key: String,
        result: String,
        at: DateTime<Utc>,
    },
}
