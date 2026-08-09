//! `task::executor` — [`ForgeExecutor`] trait + execution context.
//!
//! A `ForgeExecutor` turns a [`ForgeSpec`] into a terminal-status execution.
//! Implementations:
//!
//! - [`crate::local::LocalForgeDriver`] — host subprocess (native or
//!   container). Handles [`ForgeCommand::Subprocess`]; rejects `BuildImage`
//!   and `Workload` with [`ForgeExecutorError::Unsupported`].
//! - [`crate::remote::RemoteForgeDriver`] — yubaba RPC (R555-T2). Blocking
//!   wrapper over its `start()` / `ForgeRunHandle::wait()` shape, which
//!   callers that need the in-flight `ForgeId` still use directly.
//!
//! `ForgeSpec` already says *what* to run and *where/how* to sandbox it.
//! [`ExecContext`] carries host-side execution detail (cwd, env) that the
//! wire-format spec deliberately omits — keep `ForgeSpec` portable across
//! the yubaba/cloud boundary; let drivers receive context out-of-band.

use std::path::PathBuf;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;

use velveteen::{ForgeSpec, ForgeStatus};

/// Host-side execution context for a forge run.
///
/// Empty by default. Driver semantics:
/// - **Native**: `cwd = None` inherits the caller's working directory; `env`
///   is merged on top of the parent environment.
/// - **Container**: `cwd` becomes the bind-mounted host path *and* the
///   container `WORKDIR` (mirroring [`crate::local::local_container_command`]);
///   `env` becomes `-e KEY=VAL` arguments — container env is otherwise empty
///   beyond what the image declares; `platform`, when set, emits `--platform
///   <value>` so a single-arch upstream image can run under host emulation
///   (e.g. Apple Silicon hosts emulating `linux/amd64` via Rosetta).
/// - `produced` is **remote-only** — see [`ProducedFile`].
#[derive(Debug, Default, Clone)]
pub struct ExecContext {
    pub cwd: Option<PathBuf>,
    pub env: Vec<(String, String)>,
    pub platform: Option<String>,
    pub produced: Option<ProducedFile>,
}

/// A file the run writes on the *worker*, to be pulled back onto the caller's
/// filesystem once the run succeeds (R555-F3).
///
/// This exists because the two placements disagree about what "the output
/// path" means, and the disagreement is invisible at the type level. A local
/// run writes its artifact straight onto the caller's disk, so the caller can
/// read it back at the path it handed in. A remote run writes into the
/// worker's `/yah/produced`, and nothing pulls those bytes back — the caller
/// sees exit code 0 and an absent file. Every consumer of a remote forge
/// therefore needs this retrieval leg, and expressing it here (rather than
/// leaving each caller to hold a [`crate::remote::RemoteForgeDriver`] directly
/// so it can call `fetch_produced_file`) is what keeps
/// `Arc<dyn ForgeExecutor>` a complete surface for a remote build.
#[derive(Debug, Clone)]
pub struct ProducedFile {
    /// Worker-side path the run writes. Must be under
    /// `workload_spec::forge_produced::CONTAINER_DIR` — that dir is the only
    /// one bind-mounted onto host-persistent storage, so a path outside it is
    /// gone the moment kamaji reaps the container.
    pub remote_path: PathBuf,
    /// Host path the retrieved bytes are written to on success.
    pub dest: PathBuf,
}

impl ExecContext {
    pub fn with_cwd(mut self, cwd: PathBuf) -> Self {
        self.cwd = Some(cwd);
        self
    }

    pub fn with_env(mut self, env: Vec<(String, String)>) -> Self {
        self.env = env;
        self
    }

    pub fn with_platform(mut self, platform: String) -> Self {
        self.platform = Some(platform);
        self
    }

    /// Ask the driver to retrieve `remote_path` off the worker and write it to
    /// `dest` after a successful run. Remote-only; the local driver refuses a
    /// spec carrying it rather than pretending the retrieval happened.
    pub fn with_produced(mut self, remote_path: PathBuf, dest: PathBuf) -> Self {
        self.produced = Some(ProducedFile { remote_path, dest });
        self
    }
}

/// One streaming event from a running forge.
///
/// Events arrive in causal order on a single sink: exactly one [`Started`],
/// any number of [`Output`], then exactly one [`Finished`]. After
/// [`Finished`] no further events are emitted on this sink.
///
/// [`Started`]: ExecEvent::Started
/// [`Output`]: ExecEvent::Output
/// [`Finished`]: ExecEvent::Finished
#[derive(Debug, Clone)]
pub enum ExecEvent {
    Started,
    Output { stream: OutputStream, line: String },
    Finished { status: ForgeStatus },
}

/// Which of the two process output streams a line came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Terminal outcome of a forge run plus a captured stderr tail.
///
/// `stderr_tail` is the trailing stderr lines collected by the driver
/// (joined by `\n`, trimmed) for callers that want to surface a failure
/// message without re-aggregating from the event sink. The driver populates
/// it whether or not the sink is attached.
#[derive(Debug, Clone)]
pub struct ExecOutcome {
    pub status: ForgeStatus,
    pub stderr_tail: String,
}

impl ExecOutcome {
    /// True when the run finished with exit code 0.
    pub fn succeeded(&self) -> bool {
        matches!(self.status, ForgeStatus::Done { exit_code: 0, .. })
    }
}

#[derive(Debug, Error)]
pub enum ForgeExecutorError {
    /// The driver doesn't know how to run this [`velveteen::ForgeCommand`]
    /// variant (e.g. [`LocalForgeDriver`](crate::local::LocalForgeDriver)
    /// receives `BuildImage` or `Workload`).
    #[error("unsupported ForgeCommand for this executor: {0}")]
    Unsupported(&'static str),
    /// Spawning the underlying process failed — typically `docker` not on
    /// PATH or the container daemon refusing to start the image.
    #[error("spawn failed: {0}")]
    Spawn(String),
    /// Underlying I/O failure mid-run (e.g. broken stdout pipe).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// A remote dispatch failed on the yubaba side — deploy rejected, log
    /// stream broke, teardown failed (R555-T2). Distinct from [`Self::Spawn`]:
    /// nothing was spawned locally, so "is the runtime installed?" is the
    /// wrong thing to tell the operator.
    #[error("remote dispatch: {0}")]
    Remote(String),
}

/// Driver that runs a [`ForgeSpec`] to terminal status.
///
/// `sink` is optional: pass `None` for fire-and-forget execution where only
/// the outcome matters (cloud reconciler materialize step); pass a sink for
/// live-stream forwarding (qed runner adapts each [`ExecEvent`] into a
/// `QedEvent::StepOutput`).
#[async_trait]
pub trait ForgeExecutor: Send + Sync {
    async fn execute(
        &self,
        spec: ForgeSpec,
        ctx: ExecContext,
        sink: Option<UnboundedSender<ExecEvent>>,
    ) -> Result<ExecOutcome, ForgeExecutorError>;
}
