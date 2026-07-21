//! @arch:layer(kg_store)
//! @arch:role(substrate)
//! @arch:see(.yah/docs/working/yah-task-runs.md)
//!
//! PTY subprocess driver — spawn commands, capture output as append-only
//! chunks, handle SIGTERM/SIGKILL with a grace period, and mark stale
//! `Running` runs as `Lost` when the daemon restarts.
//!
//! ## Tier 2 side-channel (yah-log shims)
//!
//! When `SpawnOpts::log_fd_enabled` is true (the default), the driver creates
//! a named pipe (FIFO) and exports two env vars into the child:
//!
//! - `YAH_TASK_RUN`  — the `TaskRunId` as a hyphenated UUID string.
//! - `YAH_LOG_PIPE`  — absolute path to the FIFO.
//!
//! The child opens `YAH_LOG_PIPE` for writing and emits JSON-lines. The
//! driver reads those lines in a background thread and stores them as
//! [`EventSource::Shim`] events.
//!
//! **Why FIFO instead of a raw fd?** `portable-pty` calls `close_random_fds()`
//! in its `pre_exec` hook, closing every fd ≥ 3 before exec. A raw-pipe write
//! fd is always ≥ 3 and would be closed before the child could use it. Opening
//! a FIFO by path requires no fd inheritance.
//!
//! Wire format — one JSON object per line:
//! ```json
//! {"level":"info","target":"myapp::module","msg":"text","fields":{"key":"val"}}
//! ```
//! Optional shim-identity keys: `"_lib"` (string), `"_lib_ver"` (string).
//! Unknown keys in `fields` pass through as freeform JSON.
//!
//! The driver holds the write end of the FIFO open until the run lifecycle
//! task completes, which triggers EOF for the receiver thread. The FIFO file
//! is deleted after the receiver thread drains the last line.
//!
//! On non-Unix platforms `YAH_TASK_RUN` and `YAH_LOG_PIPE` are not exported.
//! Shim libraries must treat absent `YAH_TASK_RUN` as "not inside a TaskRun".
//!
//! @yah:ticket(R617-F6, "Reattach-by-run_id replaces Lost-on-disappear for origin=terminal shells")
//! @yah:at(2026-07-20T18:38:27Z)
//! @yah:status(open)
//! @yah:phase(P3)
//! @yah:parent(R617)
//! @yah:next("TaskDriver::new (driver.rs:199) marks every leftover Running run Lost on construction — correct for ordinary jobs, fatal for a shell meant to survive a restart. Split the behaviour on origin: a terminal shell whose host process is still alive is re-adopted (control channel rebuilt, reader thread restarted against the surviving PTY) rather than tombstoned.")
//! @yah:verify("Manual: open a shell, run `sleep 300`, quit and relaunch the desktop — the run is still Running, not Lost")
//! @yah:gotcha("This is an oss/qed crate — changes land in-tree under oss/task-runs and flow outward via scripts/export-oss.sh. Keep the reattach seam generic (origin-agnostic policy hook), not yah-terminal-specific, since the crate ships standalone.")
//! @yah:gotcha("Reattach only makes sense once the PTY outlives the desktop (S5 decides the host). Landing it before that gives a reattach path with nothing to reattach to.")
//! @arch:see(.yah/docs/working/W280-durable-terminal-sessions.md)
//! @yah:depends_on(R617-S5)
//!
//! @yah:ticket(R617-B9, "Pre-existing: task-runs log_pipe_events_land_in_store never completes (233 pass / 1 fail)")
//! @yah:at(2026-07-20T21:19:08Z)
//! @yah:status(open)
//! @yah:phase(P1)
//! @yah:parent(R617)
//! @yah:next("The run never reaches Done/Lost within the 20s deadline, so the FIFO assertions are never reached. Child writes one JSON line via `printf ... >> \"$YAH_LOG_PIPE\"`; suspect the child blocks or the lifecycle never observes its exit. mkfifo itself works on this machine.")
//! @yah:verify("cd oss/qed && cargo test -p task-runs --lib log_pipe_events_land_in_store")
//! @yah:gotcha("Confirmed pre-existing during R617-B1, not caused by the DriverChannels output tap: swapping driver.rs + lib.rs to their HEAD versions reproduces the identical failure. Anyone touching this file (R617-F6 lands here) will meet a red suite that is not theirs.")

use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task;

use crate::beholders::{registry_with_user_beholders, BeholderSelect};
use crate::store::{RunFilter, StoreError, TaskStore};
use crate::types::{BeholderStatus, Initiator, OutputChunk, RunStatus, Stream, TaskRunId, TaskRunMeta};

const DEFAULT_GRACE: Duration = Duration::from_secs(5);
const READ_BUF_SIZE: usize = 4096;
const SIGTERM: i32 = 15;
const SIGKILL: i32 = 9;

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum DriverError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("pty: {0}")]
    Pty(String),
    #[error("run not found: {0}")]
    NotFound(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

// ─── SpawnOpts ────────────────────────────────────────────────────────────────

/// Options for [`TaskDriver::spawn_run`].
#[derive(Debug, Clone)]
pub struct SpawnOpts {
    pub cwd: PathBuf,
    /// Env vars set on the child process (merged on top of the current env).
    pub env: Vec<(String, String)>,
    pub label: Option<String>,
    pub initiator: Initiator,
    /// PTY column count. Defaults to 80.
    pub pty_cols: u16,
    /// PTY row count. Defaults to 24.
    pub pty_rows: u16,
    /// Enable stdin relay via [`TaskDriver::send_stdin`].
    pub stdin_enabled: bool,
    /// Pin the run so the GC sweep does not drop its output during warm rolloff.
    pub pin: bool,
    /// Beholder attachment policy. Defaults to [`BeholderSelect::Auto`].
    pub beholder_select: BeholderSelect,
    /// `true` when a human-facing terminal tile is attached. Causes `Rewriter`
    /// beholders to decline in `Auto` mode so the human sees unmodified output.
    pub tty_attached: bool,
    /// Create a side-channel FIFO and export `YAH_TASK_RUN` / `YAH_LOG_PIPE`
    /// so Tier-2 shim libraries (yah-log-rust, @yah/log) can emit structured
    /// events. Has no effect on non-Unix platforms. Defaults to `true`.
    pub log_fd_enabled: bool,
    /// Provenance tag stored on the run's `TaskRunMeta.origin` (e.g.
    /// `Some("terminal")` for an interactive shell). `None` is an ordinary job.
    pub origin: Option<String>,
}

impl Default for SpawnOpts {
    fn default() -> Self {
        Self {
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")),
            env: vec![],
            label: None,
            initiator: Initiator::Human { camp: "local".to_string() },
            pty_cols: 80,
            pty_rows: 24,
            stdin_enabled: false,
            pin: false,
            beholder_select: BeholderSelect::Auto,
            tty_attached: false,
            log_fd_enabled: true,
            origin: None,
        }
    }
}

// ─── Driver channels ─────────────────────────────────────────────────────────

/// Optional side-channels a driver can publish to. Both are fire-and-forget:
/// a closed receiver never stalls or fails a run.
#[derive(Default)]
pub struct DriverChannels {
    /// Fires `(run_id, status)` after each run's lifecycle task writes the
    /// terminal status. Drives completion listeners (e.g. a triage worker).
    pub completion: Option<mpsc::UnboundedSender<(TaskRunId, RunStatus)>>,
    /// Mirrors every PTY output chunk as it is captured, *before* any consumer
    /// polls the store. Lets a host attach a live view (VT parser, log
    /// forwarder) to a run without a read-back loop over the store.
    ///
    /// The driver deliberately stays ignorant of what the tap is for — the
    /// chunk carries `run_id`, so the host decides which runs it cares about.
    pub output: Option<mpsc::UnboundedSender<OutputChunk>>,
}

// ─── Internal run-control handle ─────────────────────────────────────────────

struct RunControl {
    kill_tx: mpsc::Sender<KillRequest>,
    stdin_tx: Option<mpsc::Sender<Vec<u8>>>,
    /// Shared with the lifecycle task, which holds the same `Arc` so the PTY fd
    /// outlives `child.wait()`. `MasterPty::resize` takes `&self`, so a mutex is
    /// enough to make the `Box<dyn MasterPty + Send>` `Sync` across the two.
    master: Arc<Mutex<Box<dyn portable_pty::MasterPty + Send>>>,
}

#[derive(Debug)]
struct KillRequest {
    signal: i32,
}

// ─── ShimRecord ───────────────────────────────────────────────────────────────

/// One JSON-line record emitted by a Tier-2 shim to the side-channel FIFO.
///
/// The shim (Rust `yah-log` layer or TS `@yah/log` pino transport) writes one
/// of these per log call. Unknown keys inside `fields` pass through unchanged.
#[cfg(unix)]
#[derive(serde::Deserialize)]
struct ShimRecord {
    level: String,
    target: String,
    msg: String,
    #[serde(default)]
    fields: serde_json::Value,
    /// Shim library name, e.g. `"yah-log-rust"`. Populates
    /// [`EventSource::Shim::lib`].
    #[serde(rename = "_lib", default)]
    lib: Option<String>,
    /// Shim library version string.
    #[serde(rename = "_lib_ver", default)]
    lib_version: Option<String>,
}

// ─── FdCloser ─────────────────────────────────────────────────────────────────

/// RAII wrapper that closes a raw fd on drop.
///
/// Used to hold the write end of the log FIFO open until the lifecycle task
/// completes. Dropping it signals EOF to the receiver thread.
#[cfg(unix)]
struct FdCloser(libc::c_int);

#[cfg(unix)]
impl Drop for FdCloser {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

// SAFETY: a raw fd number is an integer; closing it from any thread is safe
// provided we never duplicate ownership (enforced by move semantics here).
#[cfg(unix)]
unsafe impl Send for FdCloser {}

// ─── TaskDriver ───────────────────────────────────────────────────────────────

/// Manages in-flight task runs for a single camp.
///
/// Wrap in `Arc` to share across tasks; internal state is mutex-protected.
pub struct TaskDriver {
    store: Arc<TaskStore>,
    active: Arc<Mutex<HashMap<String, RunControl>>>,
    /// Side-channels published to by every run this driver owns.
    channels: DriverChannels,
}

impl TaskDriver {
    /// Create a driver backed by `store`, with no side-channels.
    ///
    /// Immediately scans the store for `Running` runs left over from a prior
    /// daemon process and marks them `Lost` ("Lost-on-disappear").
    pub async fn new(store: Arc<TaskStore>) -> Result<Self, DriverError> {
        Self::with_channels(store, DriverChannels::default()).await
    }

    /// Like `new` but wires the optional [`DriverChannels`] side-channels
    /// (completion notifications, live output tap).
    pub async fn with_channels(
        store: Arc<TaskStore>,
        channels: DriverChannels,
    ) -> Result<Self, DriverError> {
        let stale = store.list_runs(&RunFilter {
            status: Some("running".to_string()),
            ..Default::default()
        }).await?;
        for meta in stale {
            store.update_status(
                &meta.id,
                &RunStatus::Lost {
                    reason: "daemon restarted while run was in-flight".to_string(),
                },
            ).await?;
        }
        Ok(Self {
            store,
            active: Arc::new(Mutex::new(HashMap::new())),
            channels,
        })
    }

    /// Spawn `cmd` in a PTY and start capturing its output. Returns immediately
    /// with the new [`TaskRunId`].
    ///
    /// A beholder is selected via `opts.beholder_select` (default `Auto`). When
    /// a `Rewriter` beholder matches, its `adjust_argv` is applied to the
    /// command before spawning and the diff is recorded on `beholder_status`.
    /// When `opts.tty_attached` is `true`, `Rewriter` beholders decline in
    /// `Auto` mode to preserve human-readable output.
    ///
    /// Output is written to the store as `Stream::Stdout` chunks (the PTY
    /// kernel merges stdout and stderr). Signal handling and status updates
    /// run in background tasks.
    pub async fn spawn_run(&self, cmd: &str, opts: SpawnOpts) -> Result<TaskRunId, DriverError> {
        let id = TaskRunId::new();
        let started_at = unix_now_secs();
        let started_at_ms: u64 = started_at.saturating_mul(1000);

        // Attach a beholder (may rewrite argv and produce structured events).
        // Resolve user drop-in directory: $YAH_BEHOLDERS_DIR or $HOME/.yah/beholders.
        let user_dir = std::env::var_os("YAH_BEHOLDERS_DIR")
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(|h| std::path::PathBuf::from(h).join(".yah/beholders"))
            });
        let registry = registry_with_user_beholders(user_dir.as_deref());
        let attach = registry.attach(cmd, &opts.beholder_select, opts.tty_attached);
        // Use the (possibly rewritten) argv to reconstruct the effective command.
        let effective_cmd = if attach.argv.is_empty() {
            cmd.to_string()
        } else {
            attach.argv.join(" ")
        };

        self.store.insert_run(&TaskRunMeta {
            id: id.clone(),
            command: cmd.to_string(),
            cwd: opts.cwd.clone(),
            env: opts.env.clone(),
            started_at,
            status: RunStatus::Running,
            label: opts.label.clone(),
            initiator: opts.initiator.clone(),
            beholder_status: Some(attach.status),
            pinned: opts.pin,
            origin: opts.origin.clone(),
        }).await?;

        // Open PTY pair.
        let pty_sys = native_pty_system();
        let pair = pty_sys
            .openpty(PtySize {
                rows: opts.pty_rows,
                cols: opts.pty_cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| DriverError::Pty(e.to_string()))?;

        // Clone reader before spawning so the fd is ready immediately.
        let pty_reader = pair
            .master
            .try_clone_reader()
            .map_err(|e| DriverError::Pty(e.to_string()))?;

        // Optional stdin relay: take the writer before spawning the child.
        let stdin_tx: Option<mpsc::Sender<Vec<u8>>> = if opts.stdin_enabled {
            let mut writer = pair
                .master
                .take_writer()
                .map_err(|e| DriverError::Pty(e.to_string()))?;
            let (tx, mut rx) = mpsc::channel::<Vec<u8>>(64);
            task::spawn(async move {
                use std::io::Write;
                while let Some(bytes) = rx.recv().await {
                    let _ = writer.write_all(&bytes);
                    let _ = writer.flush();
                }
            });
            Some(tx)
        } else {
            None
        };

        // ── Side-channel log FIFO (Tier 2 / yah-log shims) ──────────────────
        //
        // Create a named pipe (FIFO) so child processes can write structured
        // events without touching stdout/stderr. We export its path via
        // YAH_LOG_PIPE; no fd inheritance is involved, so portable-pty's
        // close_random_fds() pre_exec hook doesn't interfere.
        //
        // The parent opens the FIFO twice:
        //   rfd — O_RDONLY|O_NONBLOCK, then cleared to blocking → read events
        //   wfd — O_WRONLY (wrapped in FdCloser) → keeps the FIFO alive until
        //          the lifecycle task drops it (after run completion), producing
        //          EOF for the receiver thread.
        #[cfg(unix)]
        let log_fifo: Option<(libc::c_int, FdCloser, std::path::PathBuf)> = if opts.log_fd_enabled {
            let fifo_path = std::env::temp_dir().join(format!("yah-log-{}.fifo", id));
            let path_cstr = match std::ffi::CString::new(fifo_path.to_string_lossy().as_bytes()) {
                Ok(s) => s,
                Err(_) => {
                    // Path contained a nul byte — extremely unlikely; skip FIFO.
                    return Err(DriverError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "log FIFO path contained nul byte",
                    )));
                }
            };
            let mkfifo_ret = unsafe { libc::mkfifo(path_cstr.as_ptr(), 0o600) };
            if mkfifo_ret != 0 {
                None // FIFO creation failed; continue without side-channel
            } else {
                // Open read end without blocking (no writer yet).
                let rfd = unsafe {
                    libc::open(path_cstr.as_ptr(), libc::O_RDONLY | libc::O_NONBLOCK)
                };
                if rfd < 0 {
                    let _ = unsafe { libc::unlink(path_cstr.as_ptr()) };
                    None
                } else {
                    // Switch read end to blocking so reads yield proper data.
                    unsafe { libc::fcntl(rfd, libc::F_SETFL, 0) };
                    // Open write end — this succeeds immediately because rfd is open.
                    let wfd = unsafe {
                        libc::open(path_cstr.as_ptr(), libc::O_WRONLY)
                    };
                    if wfd < 0 {
                        unsafe { libc::close(rfd) };
                        let _ = unsafe { libc::unlink(path_cstr.as_ptr()) };
                        None
                    } else {
                        Some((rfd, FdCloser(wfd), fifo_path))
                    }
                }
            }
        } else {
            None
        };

        // Build and spawn the child inside the slave.
        let mut cb = CommandBuilder::new("sh");
        cb.args(["-c", &effective_cmd]);
        cb.cwd(&opts.cwd);
        for (k, v) in &opts.env {
            cb.env(k, v);
        }
        cb.env("TERM", "xterm-256color");

        // Export YAH_TASK_RUN and YAH_LOG_PIPE if the FIFO was created.
        #[cfg(unix)]
        if let Some((_, _, ref fifo_path)) = log_fifo {
            cb.env("YAH_TASK_RUN", id.to_string());
            cb.env("YAH_LOG_PIPE", fifo_path.to_string_lossy().as_ref());
        }

        let child = pair
            .slave
            .spawn_command(cb)
            .map_err(|e| DriverError::Pty(e.to_string()))?;
        // Drop the parent's slave handle so EOF propagates once the child exits.
        drop(pair.slave);

        // Share the master between the lifecycle task (which must outlive
        // `child.wait()` so the fd stays open) and `resize_run`.
        let master: Arc<Mutex<Box<dyn portable_pty::MasterPty + Send>>> =
            Arc::new(Mutex::new(pair.master));

        let pid = child.process_id().unwrap_or(0);

        // ── FIFO: launch receiver thread; pass write-end holder to lifecycle ──
        //
        // The receiver thread reads until EOF. EOF arrives when ALL write-end
        // holders close: the child's own writers (when it exits) plus the
        // FdCloser we hand to the lifecycle task (which drops it after writing
        // the terminal RunStatus). Events written before the last close are
        // still drained by the receiver thread before it exits.
        #[cfg(unix)]
        let log_wfd_holder: Option<FdCloser> = if let Some((rfd, wfd, fifo_path)) = log_fifo {
            let store_log = Arc::clone(&self.store);
            let id_log = id.clone();
            let rt = tokio::runtime::Handle::current();
            // spawn_blocking: lets the runtime track this thread so the
            // Handle::block_on calls inside have a worker to drive futures.
            tokio::task::spawn_blocking(move || {
                run_log_receiver(rt, store_log, id_log, rfd, fifo_path, started_at_ms);
            });
            Some(wfd)
        } else {
            None
        };

        // Channels.
        let (kill_tx, kill_rx) = mpsc::channel::<KillRequest>(4);
        let (reader_done_tx, reader_done_rx) = oneshot::channel::<()>();

        // Reader thread: PTY output → store chunks → beholder events.
        // Runs on a dedicated OS thread because PTY reads are blocking.
        {
            let store_r = Arc::clone(&self.store);
            let id_r = id.clone();
            let mut beholder = attach.beholder;
            let output_tx = self.channels.output.clone();
            let rt = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                let mut buf = [0u8; READ_BUF_SIZE];
                let mut reader = pty_reader;
                loop {
                    match reader.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            let offset = elapsed_ms(started_at_ms);
                            let append_res = rt.block_on(store_r.append_chunk(
                                &id_r,
                                offset,
                                Stream::Stdout,
                                &buf[..n],
                            ));
                            if let Ok(seq) = append_res {
                                /* Both the tap and the beholder want the same
                                   owned chunk; build it once, and only when
                                   someone is listening. */
                                let chunk = (output_tx.is_some() || beholder.is_some()).then(|| {
                                    OutputChunk {
                                        run_id: id_r.clone(),
                                        seq,
                                        offset_ms: offset,
                                        stream: Stream::Stdout,
                                        bytes: buf[..n].to_vec(),
                                    }
                                });
                                /* Tap first: it feeds live views, where latency
                                   is visible to a human. Send failure means the
                                   host dropped its receiver — never fatal. */
                                if let (Some(tx), Some(c)) = (&output_tx, &chunk) {
                                    let _ = tx.send(c.clone());
                                }
                                let mut detach_beholder = false;
                                if let (Some(b), Some(chunk)) = (beholder.as_mut(), &chunk) {
                                    for ev in b.parse_chunk(chunk) {
                                        let _ = rt.block_on(store_r.append_event(
                                            &ev.run_id,
                                            ev.offset_ms,
                                            ev.level,
                                            &ev.target,
                                            &ev.msg,
                                            &ev.fields,
                                            ev.anchor.as_ref().map(|a| a.seq),
                                            &ev.source,
                                        ));
                                    }
                                    if let Some(reason) = b.unknown_format_reason() {
                                        let new_status = BeholderStatus::unknown_format_with_reason(
                                            b.name(),
                                            reason,
                                        );
                                        let _ = rt.block_on(
                                            store_r.update_beholder_status(&id_r, &new_status),
                                        );
                                        detach_beholder = true;
                                    }
                                }
                                if detach_beholder {
                                    beholder = None;
                                }
                            }
                        }
                    }
                }
                if let Some(ref mut b) = beholder {
                    let final_offset = elapsed_ms(started_at_ms);
                    for ev in b.on_done(&id_r, final_offset) {
                        let _ = rt.block_on(store_r.append_event(
                            &ev.run_id,
                            ev.offset_ms,
                            ev.level,
                            &ev.target,
                            &ev.msg,
                            &ev.fields,
                            ev.anchor.as_ref().map(|a| a.seq),
                            &ev.source,
                        ));
                    }
                    if let Some(reason) = b.unknown_format_reason() {
                        let new_status = BeholderStatus::unknown_format_with_reason(b.name(), reason);
                        let _ = rt.block_on(store_r.update_beholder_status(&id_r, &new_status));
                    }
                }
                let _ = reader_done_tx.send(());
            });
        }

        // Lifecycle task: monitor kill requests, wait for exit, update status.
        // The task also holds the log FIFO write-end closer (if any) so that
        // EOF propagates to the receiver thread after RunStatus is written.
        {
            let store_l = Arc::clone(&self.store);
            let active_l = Arc::clone(&self.active);
            let id_l = id.clone();
            let master_l = Arc::clone(&master);
            let completion_tx_l = self.channels.completion.clone();
            #[cfg(unix)]
            let wfd_l = log_wfd_holder;
            task::spawn(async move {
                run_lifecycle(
                    store_l,
                    active_l,
                    id_l,
                    pid,
                    child,
                    master_l,
                    kill_rx,
                    reader_done_rx,
                    completion_tx_l,
                    #[cfg(unix)]
                    wfd_l,
                )
                .await;
            });
        }

        self.active
            .lock()
            .unwrap()
            .insert(id.to_string(), RunControl { kill_tx, stdin_tx, master });

        Ok(id)
    }

    /// Resize a running task's PTY and deliver `SIGWINCH` to the foreground
    /// process group (portable-pty's `resize` does the ioctl, which is what
    /// signals the child).
    ///
    /// Returns `DriverError::NotFound` when the run is not active on this
    /// driver instance — the same contract as [`TaskDriver::send_stdin`].
    pub async fn resize_run(
        &self,
        id: &TaskRunId,
        cols: u16,
        rows: u16,
    ) -> Result<(), DriverError> {
        let master = self
            .active
            .lock()
            .unwrap()
            .get(&id.to_string())
            .map(|c| Arc::clone(&c.master));

        match master {
            Some(m) => {
                let size = PtySize { rows, cols, pixel_width: 0, pixel_height: 0 };
                m.lock()
                    .unwrap()
                    .resize(size)
                    .map_err(|e| DriverError::Pty(e.to_string()))
            }
            None => Err(DriverError::NotFound(id.to_string())),
        }
    }

    /// Send `signal` to a running task. Defaults to SIGTERM (15).
    ///
    /// For SIGTERM, the driver waits up to 5 seconds for the process to exit
    /// before escalating to SIGKILL. Returns `DriverError::NotFound` if the
    /// run is not active (already exited or launched on a different driver
    /// instance).
    pub async fn kill_run(&self, id: &TaskRunId, signal: Option<i32>) -> Result<(), DriverError> {
        let kill_tx = self
            .active
            .lock()
            .unwrap()
            .get(&id.to_string())
            .map(|c| c.kill_tx.clone());

        match kill_tx {
            Some(tx) => tx
                .send(KillRequest { signal: signal.unwrap_or(SIGTERM) })
                .await
                .map_err(|_| DriverError::NotFound(id.to_string())),
            None => Err(DriverError::NotFound(id.to_string())),
        }
    }

    /// Write bytes to the stdin of a running task (requires `stdin_enabled`).
    pub async fn send_stdin(&self, id: &TaskRunId, bytes: Vec<u8>) -> Result<(), DriverError> {
        let stdin_tx = self
            .active
            .lock()
            .unwrap()
            .get(&id.to_string())
            .and_then(|c| c.stdin_tx.clone());

        match stdin_tx {
            Some(tx) => tx
                .send(bytes)
                .await
                .map_err(|_| DriverError::NotFound(id.to_string())),
            None => Err(DriverError::NotFound(id.to_string())),
        }
    }
}

// ─── Log fd receiver ─────────────────────────────────────────────────────────

/// Read JSON-lines from the side-channel FIFO read end and store them as
/// [`EventSource::Shim`] events.
///
/// Runs on a dedicated OS thread; exits when the read end sees EOF. EOF
/// arrives after both the child process AND the lifecycle task have closed
/// their write ends of the FIFO. The FIFO file is deleted on exit.
#[cfg(unix)]
fn run_log_receiver(
    rt: tokio::runtime::Handle,
    store: Arc<TaskStore>,
    run_id: TaskRunId,
    read_fd: libc::c_int,
    fifo_path: std::path::PathBuf,
    started_at_ms: u64,
) {
    use std::io::BufRead;
    use std::os::unix::io::FromRawFd;

    // SAFETY: `read_fd` is a valid, open FIFO fd handed exclusively to this
    // thread. `File` takes ownership and closes the fd on drop.
    let file = unsafe { std::fs::File::from_raw_fd(read_fd) };
    let reader = std::io::BufReader::new(file);

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let rec: ShimRecord = match serde_json::from_str(trimmed) {
            Ok(r) => r,
            Err(_) => continue, // skip malformed lines silently
        };
        let level = rec.level.parse::<crate::types::Level>().unwrap_or(crate::types::Level::Info);
        let source = crate::types::EventSource::Shim {
            lib: rec.lib.unwrap_or_else(|| "unknown".to_string()),
            version: rec.lib_version.unwrap_or_else(|| "0.0.0".to_string()),
        };
        let fields = if rec.fields.is_object() {
            rec.fields
        } else {
            serde_json::Value::Object(Default::default())
        };
        let offset = elapsed_ms(started_at_ms);
        let _ = rt.block_on(store.append_event(
            &run_id,
            offset,
            level,
            &rec.target,
            &rec.msg,
            &fields,
            None,
            &source,
        ));
    }

    // Clean up the FIFO file now that the receiver has drained.
    let _ = std::fs::remove_file(&fifo_path);
}

// ─── Lifecycle task ───────────────────────────────────────────────────────────

async fn run_lifecycle(
    store: Arc<TaskStore>,
    active: Arc<Mutex<HashMap<String, RunControl>>>,
    id: TaskRunId,
    pid: u32,
    child: Box<dyn portable_pty::Child + Send>,
    master: Arc<Mutex<Box<dyn portable_pty::MasterPty + Send>>>,
    mut kill_rx: mpsc::Receiver<KillRequest>,
    reader_done_rx: oneshot::Receiver<()>,
    completion_tx: Option<tokio::sync::mpsc::UnboundedSender<(TaskRunId, RunStatus)>>,
    // Holds the write end of the log FIFO open until this task completes.
    // Dropping it produces EOF for the receiver thread, which happens after
    // the terminal RunStatus is written below.
    #[cfg(unix)]
    _log_wfd: Option<FdCloser>,
) {
    // Pin the reader-done future so it can be polled by reference in
    // nested select! arms without consuming ownership.
    let reader_done = async { reader_done_rx.await.ok(); };
    tokio::pin!(reader_done);

    let sent_signal: Option<i32>;

    tokio::select! {
        req = kill_rx.recv() => {
            match req {
                Some(KillRequest { signal }) => {
                    send_unix_signal(pid, signal);
                    if signal == SIGKILL {
                        sent_signal = Some(SIGKILL);
                    } else {
                        // Grace period: give the process a chance to exit cleanly.
                        tokio::select! {
                            _ = &mut reader_done => {
                                // Exited within grace — no SIGKILL needed.
                                sent_signal = Some(signal);
                            }
                            _ = tokio::time::sleep(DEFAULT_GRACE) => {
                                // Grace expired — escalate.
                                send_unix_signal(pid, SIGKILL);
                                sent_signal = Some(SIGKILL);
                            }
                        }
                    }
                }
                // kill_tx dropped (driver shutting down) — force kill.
                None => {
                    send_unix_signal(pid, SIGKILL);
                    sent_signal = Some(SIGKILL);
                }
            }
        }
        _ = &mut reader_done => {
            sent_signal = None;
        }
    }

    // Reap the child (blocking) on a dedicated thread-pool slot.
    // Move our master handle in here so the PTY fd outlives the wait. The
    // matching `RunControl` (removed from `active` below) holds the other
    // `Arc`, so the fd actually closes once both are gone.
    let exit_code = task::spawn_blocking(move || {
        let mut c = child;
        let _m = master; // dropped after wait() returns
        c.wait().ok().map(|s| s.exit_code())
    })
    .await
    .ok()
    .flatten();

    let ended_at = unix_now_secs();
    let status = match sent_signal {
        Some(sig) => RunStatus::Killed { signal: sig, ended_at },
        None => match exit_code {
            Some(code) => RunStatus::Done { exit_code: code as i32, ended_at },
            None => RunStatus::Lost {
                reason: "process exited without an exit code".to_string(),
            },
        },
    };

    let _ = store.update_status(&id, &status).await;
    if let Some(ref tx) = completion_tx {
        let _ = tx.send((id.clone(), status));
    }
    active.lock().unwrap().remove(&id.to_string());
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn send_unix_signal(pid: u32, signal: i32) {
    #[cfg(unix)]
    unsafe {
        libc::kill(pid as libc::pid_t, signal);
    }
    // On non-Unix platforms signal delivery is not implemented here.
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn elapsed_ms(started_at_ms: u64) -> u32 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    now_ms.saturating_sub(started_at_ms).min(u32::MAX as u64) as u32
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::ChunkFilter;

    async fn open_store(dir: &tempfile::TempDir) -> Arc<TaskStore> {
        Arc::new(TaskStore::open(&dir.path().join("tr.turso")).await.unwrap())
    }

    // ── Lost-on-disappear (pure store, no PTY) ────────────────────────────────

    #[tokio::test]
    async fn lost_on_disappear_marks_stale_running_runs() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;

        // Simulate a run left in "Running" state by a prior daemon.
        let stale_id = TaskRunId::new();
        store
            .insert_run(&TaskRunMeta {
                id: stale_id.clone(),
                command: "sleep 9999".to_string(),
                cwd: "/tmp".into(),
                env: vec![],
                started_at: unix_now_secs() - 60,
                status: RunStatus::Running,
                label: None,
                initiator: Initiator::Human { camp: "test".to_string() },
                beholder_status: None,
                pinned: false,
                origin: None,
            })
            .await
            .unwrap();

        // Creating a new driver must mark stale runs Lost.
        let _driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let meta = store.get_run(&stale_id).await.unwrap().unwrap();
        assert!(
            matches!(meta.status, RunStatus::Lost { .. }),
            "stale run should be Lost, got {:?}",
            meta.status
        );
    }

    #[tokio::test]
    async fn new_driver_does_not_touch_completed_runs() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;

        let done_id = TaskRunId::new();
        store
            .insert_run(&TaskRunMeta {
                id: done_id.clone(),
                command: "true".to_string(),
                cwd: "/tmp".into(),
                env: vec![],
                started_at: unix_now_secs() - 10,
                status: RunStatus::Running,
                label: None,
                initiator: Initiator::Human { camp: "test".to_string() },
                beholder_status: None,
                pinned: false,
                origin: None,
            })
            .await
            .unwrap();
        store
            .update_status(&done_id, &RunStatus::Done { exit_code: 0, ended_at: unix_now_secs() })
            .await
            .unwrap();

        let _driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let meta = store.get_run(&done_id).await.unwrap().unwrap();
        assert!(
            matches!(meta.status, RunStatus::Done { .. }),
            "completed run must not be touched"
        );
    }

    // ── PTY spawn + capture ───────────────────────────────────────────────────

    #[tokio::test]
    async fn spawn_echo_and_read_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let id = driver
            .spawn_run(
                "echo hello_world",
                SpawnOpts { cwd: "/tmp".into(), ..Default::default() },
            )
            .await
            .unwrap();

        // Wait for the run to complete (poll status up to 5 s).
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let meta = store.get_run(&id).await.unwrap().unwrap();
            if matches!(meta.status, RunStatus::Done { .. } | RunStatus::Lost { .. }) {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("run did not complete in time, status={:?}", meta.status);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Chunks must contain "hello_world".
        let chunks = store
            .get_chunks(&id, &ChunkFilter::default())
            .await
            .unwrap();
        let output: Vec<u8> = chunks.into_iter().flat_map(|c| c.bytes).collect();
        let text = String::from_utf8_lossy(&output);
        assert!(
            text.contains("hello_world"),
            "expected 'hello_world' in output, got: {text:?}"
        );

        let meta = store.get_run(&id).await.unwrap().unwrap();
        assert!(
            matches!(meta.status, RunStatus::Done { exit_code: 0, .. }),
            "expected Done(0), got {:?}",
            meta.status
        );
    }

    #[tokio::test]
    async fn spawn_failing_command_records_nonzero_exit() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let id = driver
            .spawn_run(
                "exit 42",
                SpawnOpts { cwd: "/tmp".into(), ..Default::default() },
            )
            .await
            .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let meta = store.get_run(&id).await.unwrap().unwrap();
            if !matches!(meta.status, RunStatus::Running | RunStatus::Pending) {
                match meta.status {
                    RunStatus::Done { exit_code, .. } => {
                        assert_ne!(exit_code, 0, "exit 42 should produce a non-zero exit code");
                    }
                    other => panic!("unexpected status: {other:?}"),
                }
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("run did not complete in time");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // ── Signal handling ───────────────────────────────────────────────────────

    #[cfg(unix)]
    #[tokio::test]
    async fn kill_with_sigterm_transitions_to_killed() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = Arc::new(TaskDriver::new(Arc::clone(&store)).await.unwrap());

        let id = driver
            .spawn_run(
                "sleep 60",
                SpawnOpts { cwd: "/tmp".into(), ..Default::default() },
            )
            .await
            .unwrap();

        // Give the process a moment to start.
        tokio::time::sleep(Duration::from_millis(100)).await;

        driver.kill_run(&id, Some(SIGTERM)).await.unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let meta = store.get_run(&id).await.unwrap().unwrap();
            if matches!(meta.status, RunStatus::Killed { .. } | RunStatus::Lost { .. }) {
                assert!(
                    matches!(meta.status, RunStatus::Killed { .. }),
                    "expected Killed, got {:?}",
                    meta.status
                );
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("run did not become Killed in time, status={:?}", meta.status);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn kill_run_returns_not_found_after_exit() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = Arc::new(TaskDriver::new(Arc::clone(&store)).await.unwrap());

        let id = driver
            .spawn_run(
                "echo done",
                SpawnOpts { cwd: "/tmp".into(), ..Default::default() },
            )
            .await
            .unwrap();

        // Wait for natural exit.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let meta = store.get_run(&id).await.unwrap().unwrap();
            if !matches!(meta.status, RunStatus::Running | RunStatus::Pending) {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("run did not complete");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Kill on a completed run should return NotFound.
        let result = driver.kill_run(&id, None).await;
        assert!(
            matches!(result, Err(DriverError::NotFound(_))),
            "expected NotFound, got {result:?}"
        );
    }

    // ── Stdin relay ───────────────────────────────────────────────────────────

    #[cfg(unix)]
    #[tokio::test]
    async fn stdin_send_reaches_child() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = Arc::new(TaskDriver::new(Arc::clone(&store)).await.unwrap());

        // Shell that reads a line from stdin and echoes it back.
        let id = driver
            .spawn_run(
                "read line && echo got_$line",
                SpawnOpts {
                    cwd: "/tmp".into(),
                    stdin_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(150)).await;
        driver.send_stdin(&id, b"hello\n".to_vec()).await.unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let meta = store.get_run(&id).await.unwrap().unwrap();
            if !matches!(meta.status, RunStatus::Running | RunStatus::Pending) {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("run did not complete after stdin input");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let chunks = store.get_chunks(&id, &ChunkFilter::default()).await.unwrap();
        let raw: Vec<u8> = chunks.into_iter().flat_map(|c| c.bytes).collect();
        let text = String::from_utf8_lossy(&raw);
        assert!(
            text.contains("got_hello"),
            "expected 'got_hello' in output, got: {text:?}"
        );
    }

    /// `resize_run` must change the geometry the *child* sees, not just the
    /// master fd — so the assertion reads `stty size` from inside the PTY
    /// after the resize rather than inspecting the driver's own state.
    #[tokio::test]
    async fn resize_run_changes_geometry_the_child_sees() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = Arc::new(TaskDriver::new(Arc::clone(&store)).await.unwrap());

        // Wait for a line on stdin, then report the geometry as of that moment.
        let id = driver
            .spawn_run(
                "read line && stty size",
                SpawnOpts {
                    cwd: "/tmp".into(),
                    stdin_enabled: true,
                    // Spawn at the default 80x24 so the assertion can't pass by
                    // accident if the resize is a no-op.
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(150)).await;
        driver.resize_run(&id, 120, 40).await.unwrap();
        driver.send_stdin(&id, b"go\n".to_vec()).await.unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let meta = store.get_run(&id).await.unwrap().unwrap();
            if !matches!(meta.status, RunStatus::Running | RunStatus::Pending) {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("run did not complete after stdin input");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let chunks = store.get_chunks(&id, &ChunkFilter::default()).await.unwrap();
        let raw: Vec<u8> = chunks.into_iter().flat_map(|c| c.bytes).collect();
        let text = String::from_utf8_lossy(&raw);
        assert!(
            text.contains("40 120"),
            "expected resized geometry '40 120' in output, got: {text:?}"
        );
    }

    /// A run that is not active on this driver (finished, or never existed) is
    /// `NotFound` rather than a panic — same contract as `send_stdin`.
    #[tokio::test]
    async fn resize_run_returns_not_found_after_exit() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = Arc::new(TaskDriver::new(Arc::clone(&store)).await.unwrap());

        let id = driver
            .spawn_run("true", SpawnOpts { cwd: "/tmp".into(), ..Default::default() })
            .await
            .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let meta = store.get_run(&id).await.unwrap().unwrap();
            if !matches!(meta.status, RunStatus::Running | RunStatus::Pending) {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("run did not exit");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(matches!(
            driver.resize_run(&id, 100, 30).await,
            Err(DriverError::NotFound(_))
        ));
    }

    // ── Tier-2 side-channel log fd ────────────────────────────────────────────

    /// Verify that a child writing a JSON-line to `YAH_LOG_PIPE` (via
    /// `printf ... >> $YAH_LOG_PIPE`) produces a shim event with the correct
    /// fields in the store.
    ///
    /// The child opens the FIFO path for writing — no fd inheritance needed.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn log_pipe_events_land_in_store() {
        use crate::store::EventFilter;

        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = Arc::new(TaskDriver::new(Arc::clone(&store)).await.unwrap());

        // The shell writes one JSON-line to the FIFO by redirecting printf
        // output to the path stored in YAH_LOG_PIPE.
        let cmd = r#"printf '{"level":"warn","target":"test.shim","msg":"hello-from-pipe","fields":{"x":42},"_lib":"test-shim","_lib_ver":"0.1.0"}\n' >> "$YAH_LOG_PIPE""#;

        let id = driver
            .spawn_run(cmd, SpawnOpts { cwd: "/tmp".into(), ..Default::default() })
            .await
            .unwrap();

        // Wait for run completion. Deadline is generous because parallel-test
        // load + the rt.block_on hops from the reader/log threads can slow
        // child-process scheduling.
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let meta = store.get_run(&id).await.unwrap().unwrap();
            if matches!(meta.status, RunStatus::Done { .. } | RunStatus::Lost { .. }) {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("run did not complete in time");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // The log receiver thread drains after the lifecycle task drops the
        // write-end FdCloser; give it a brief moment.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let events = store.query_events(&id, &EventFilter::default()).await.unwrap();
        assert!(
            !events.is_empty(),
            "expected at least one shim event, got none"
        );
        let ev = events.iter().find(|e| e.target == "test.shim");
        let ev = ev.expect("event with target 'test.shim' not found");
        assert_eq!(ev.msg, "hello-from-pipe");
        assert_eq!(ev.level, crate::types::Level::Warn);
        assert!(
            matches!(&ev.source, crate::types::EventSource::Shim { lib, .. } if lib == "test-shim"),
            "unexpected source: {:?}",
            ev.source
        );
        assert_eq!(ev.fields.get("x"), Some(&serde_json::json!(42)));
    }

    /// When `log_fd_enabled` is false, neither `YAH_TASK_RUN` nor
    /// `YAH_LOG_PIPE` are exported, and no shim events are written.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn log_pipe_disabled_produces_no_events() {
        use crate::store::EventFilter;

        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = Arc::new(TaskDriver::new(Arc::clone(&store)).await.unwrap());

        // Try to write to YAH_LOG_PIPE; the conditional guards against
        // the variable being absent, so the command always exits 0.
        let cmd = r#"[ -n "$YAH_LOG_PIPE" ] && printf '{"level":"info","target":"t","msg":"m","fields":{}}\n' >> "$YAH_LOG_PIPE" || true"#;

        let id = driver
            .spawn_run(
                cmd,
                SpawnOpts { cwd: "/tmp".into(), log_fd_enabled: false, ..Default::default() },
            )
            .await
            .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let meta = store.get_run(&id).await.unwrap().unwrap();
            if matches!(meta.status, RunStatus::Done { .. } | RunStatus::Lost { .. }) {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("run did not complete");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        let events = store.query_events(&id, &EventFilter::default()).await.unwrap();
        assert!(
            events.is_empty(),
            "expected no shim events when log_fd_enabled=false, got {}",
            events.len()
        );
    }
}
