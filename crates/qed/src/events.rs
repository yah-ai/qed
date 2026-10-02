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
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @arch:see(.yah/docs/working/W167-smoke-matrix-plan.md)

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
        /// The step's [`crate::types::QedStep::expect_slow`] declaration,
        /// carried on the event so a consumer watching the run's liveness can
        /// tell "this step has been quiet for an hour and its author said it
        /// would be" from "this step has been quiet for an hour" without
        /// re-reading the pipeline TOML — which, for a sub-pipeline child, it
        /// may not even be able to locate.
        expect_slow: bool,
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
    /// A `kind = "manual"` step parked on a human (R622, W282). Emitted every
    /// time the step parks — including a re-park after a failed `advance` — so
    /// the card always names the form currently awaiting an answer.
    ///
    /// `form_id` is the AnswerQueue form the human answers (`None` on the
    /// headless path, where no [`crate::runner::ManualGate`] is installed and
    /// `advance` is the only door). `advance` is echoed so a consumer can show
    /// the condition the step is waiting to satisfy without re-reading the
    /// pipeline TOML.
    ///
    /// The run's `concurrency_key` is **released** for the duration of the park
    /// — see [`crate::types::StepKind::Manual`].
    StepAwaitingHuman {
        index: usize,
        name: String,
        form_id: Option<String>,
        advance: Option<String>,
        /// Who is allowed to answer this park (R906-F1) — the field that
        /// splits a supervising agent's `needs_agent` wake from its
        /// `blocked_on_operator` one. See [`crate::types::ManualAudience`].
        audience: crate::types::ManualAudience,
        /// [`crate::types::ManualConfig::prompt`], echoed so a consumer woken
        /// by this park knows what is being asked without a second lookup. The
        /// form carries the same text, but a headless park has no form.
        prompt: String,
        /// [`crate::types::ManualConfig::terminal`] — the starting kit for
        /// whoever answers. Echoed for the same reason as `prompt`.
        terminal: Vec<String>,
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
