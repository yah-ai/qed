//! Live per-job/per-step events emitted by [`crate::execute_workflow`] when
//! the [`crate::Executor`] is configured with an event sink.
//!
//! W200 R487 follow-up. The qed runner crate bridges these into its own
//! [`QedEvent::Gha*`] variants so the nested GHA-workflow tree is visible
//! mid-flight in the desktop QED pane and the `qed.tail` stream — not just
//! at the terminal "first failing job" bookend.

use indexmap::IndexMap;

use crate::toolkit::StepConclusion;
use crate::graph::JobResult;

/// Which standard stream a captured bash line came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GhaOutputStream {
    Stdout,
    Stderr,
}

/// A live event from a workflow run. Sequential within a job; jobs run
/// sequentially-within-wave today (`execute_workflow` is single-threaded by
/// design) so the receiver sees a clean, in-order stream.
#[derive(Debug, Clone)]
pub enum GhaEvent {
    /// A job instance is about to run. `matrix_index` is `Some` for matrix
    /// rows, `None` for plain jobs. `key` is the [`crate::graph::JobInstance::key`]
    /// stable identifier (`"<job>"` or `"<job>#<row>"`) so the receiver can
    /// pair start/finish without recomputing it.
    JobStarted {
        job_id: String,
        matrix_index: Option<usize>,
        key: String,
        total_steps: usize,
    },
    /// One step in a job is about to run. `action_kind` is `"run"` for bash
    /// steps and `"uses:<slug>"` for action invocations — enough to label the
    /// row in the UI without re-walking the workflow YAML.
    StepStarted {
        job_id: String,
        matrix_index: Option<usize>,
        step_index: usize,
        step_id: Option<String>,
        name: Option<String>,
        action_kind: String,
    },
    /// One line of stdout/stderr captured from a bash step. Only emitted for
    /// `StepAction::Run` steps; `uses:` steps never stream (they log via the
    /// override's own logging convention and surface as the `log` field on
    /// the resulting [`crate::StepResult`]).
    StepOutput {
        job_id: String,
        matrix_index: Option<usize>,
        step_index: usize,
        stream: GhaOutputStream,
        line: String,
    },
    /// One step reached a terminal conclusion. `msg` carries the stderr tail
    /// on `Failure` so the receiver can render a red banner without having
    /// to scrape its own buffered output, mirroring the qed-runner
    /// `StepFinished.msg` convention.
    StepFinished {
        job_id: String,
        matrix_index: Option<usize>,
        step_index: usize,
        conclusion: StepConclusion,
        msg: Option<String>,
        outputs: IndexMap<String, crate::expr::Value>,
    },
    /// A job instance reached a terminal result. Pairs with the prior
    /// `JobStarted` by `(job_id, matrix_index)`.
    JobFinished {
        job_id: String,
        matrix_index: Option<usize>,
        key: String,
        result: JobResult,
    },
}

/// Thin Send-able wrapper around a sender so the executor doesn't have to
/// name a generic transport. We use [`std::sync::mpsc`] because qed-gha is
/// sync-by-design (`execute_workflow` runs blocking under
/// `tokio::task::spawn_blocking` on the qed-runner side); the bridge crate
/// drains this into its async [`tokio::sync::mpsc::UnboundedSender`].
pub type GhaEventSink = std::sync::mpsc::Sender<GhaEvent>;
