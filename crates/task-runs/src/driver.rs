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
//! @yah:status(review)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:at(2026-07-24T01:26:41Z)
//! @yah:phase(P3)
//! @yah:parent(R617)
//! @arch:see(.yah/docs/working/W280-durable-terminal-sessions.md)
//! @yah:depends_on(R617-F13)
//! @yah:handoff("DELIVERED. Verified: `cd oss/qed && cargo test -p task-runs --lib` 243/243 (was 237 — 6 new); `cargo test -p kg-daemon --lib shell_vt` 9/9; `cargo test -p yah --lib r617` 9/9; `cargo test -p desktop --lib` 357 pass / 2 fail, both pre-existing and in files this ticket does not touch (agent.rs rules-view expects 12 rows and a peer's approval-rule change makes 19; agent_process reader-finished is a known timing flake).")
//! @yah:handoff("THE TICKET'S OWN FRAMING WAS WRONG ABOUT THE MECHANISM, and the correction is the design. @yah:next said to 're-adopt' a live shell by 'control channel rebuilt, reader thread restarted against the surviving PTY'. That is not possible and never was: you cannot re-open another process's PTY master fd. The real defect is narrower and worse — a driver was tombstoning runs IT DID NOT OWN. `.yah/db/task-runs.turso` has several writers (desktop, the R617-F13 shell host, one CampService per MCP sidecar), and `TaskDriver::new` assumed any leftover `Running` row must be its own predecessor's corpse. So every attach marked some other LIVE process's shell `Lost`, and that shell kept producing output under a status saying it was dead. The fix is therefore 'do not tombstone what you do not own', not 'reattach'. Actual PTY reattach is unnecessary once F13 puts the PTY in a process that outlives the desktop.")
//! @yah:handoff("HOW OWNERSHIP IS KNOWN: new `TaskRunMeta::host_pid` — the pid of the process whose driver spawned the run, NOT the child's. Stamped by `spawn_run` at INSERT, before the child exists, so a crash between insert and spawn still leaves the row attributable. Store column added by the same idempotent `ALTER TABLE ... ADD COLUMN` pattern `origin` used, and `row_to_meta` reads index 15 with `.ok().flatten()` so a DB with no such column reads `None` rather than erroring.")
//! @yah:handoff("THE SEAM IS ORIGIN-AGNOSTIC, per this ticket's gotcha. New `task_runs::StaleRunPolicy` in oss/qed/crates/task-runs/src/driver.rs: `LostOnDisappear` (the default — `TaskDriver::new` and `with_channels` behave exactly as before, so no existing embedder changed) and `AdoptLiveHosts { origins: Vec<String> }`, which spares a leftover run only when its `host_pid` names a process that still exists. The crate decides on OWNERSHIP and takes the origin list as data — it never learns what 'terminal' means. New `TaskDriver::with_config` is the constructor that takes it.")
//! @yah:handoff("yah side: `crates/yah/kg-daemon/src/service.rs::open_task_store` now passes `AdoptLiveHosts { origins: [ORIGIN_TERMINAL] }`. Also replaced the magic string — new `kg_daemon::shell_vt::ORIGIN_TERMINAL` now backs the two live `origin == \"terminal\"` gates in shell_vt.rs plus the policy, so the VT-parsing gate and the tombstone-exemption gate cannot drift apart by a typo. The constant lives on the yah side, NOT in task-runs, precisely to keep the crate generic.")
//! @yah:handoff("Also stamped at app/yah/desktop/src/terminal.rs:519 — the desktop-local PTY path (terminal_open_local's scrollback mint) owns its own PTYs, so those rows carry the desktop's pid. Without it the shell host's driver would tombstone a live desktop-local session on attach, which is the same bug pointing the other way.")
//! @yah:handoff("PID REUSE is the honest weakness and is why the policy is opt-in and origin-narrowed. `kill(pid, 0)` (EPERM counts as alive — the process exists, it is just not ours to signal) can read a recycled pid as the original owner. The failure mode of a false 'alive' is one run left `Running` until something closes it; the false 'dead' this replaces kills a live session's status. Strictly the better direction for an interactive shell, and the exposure is bounded to origins the embedder opted in. Non-unix has no kill(2), so `host_process_alive` reports false there and the platform keeps the old behaviour rather than stranding runs forever.")
//! @yah:handoff("SIX NEW TESTS, each pinned to a failure rather than a code path: a live-owner terminal run survives a new driver (the ticket's whole point); a run whose owner pid was spawned and reaped in-test IS tombstoned (a crashed host must not leave zombie tiles); origin-less and non-matching origins are tombstoned even with a live owner (an in-flight `cargo build` whose driver is gone has nobody left to record its exit); an unattributed row (pre-migration) is tombstoned; `TaskDriver::new` still tombstones unconditionally (no silent behaviour change for existing embedders); and `spawn_run` stamps this process — the policy is worthless if rows arrive unattributed.")
//! @yah:verify("cd oss/qed && cargo test -p task-runs --lib  # 243/243, 6 new under driver::tests")
//! @yah:verify("cargo test -p kg-daemon --lib shell_vt  # 9/9")
//! @yah:verify("cargo test -p yah --lib r617  # 9/9")
//! @yah:verify("Manual (needs a desktop rebuild): open a shell, run `sleep 300`, quit and relaunch the desktop — the run is still Running, not Lost")
//! @yah:verify("sqlite3 .yah/db/task-runs.turso \"select id, origin, host_pid, status from runs where status='running';\"  # every live row names a pid that ps shows")
//! @yah:gotcha("This is an oss/qed crate — changes land in-tree under oss/qed/crates/task-runs and flow outward via scripts/export-oss.sh. The seam was kept origin-agnostic (StaleRunPolicy decides on host_pid, takes origins as data); the one yah-ism, ORIGIN_TERMINAL, lives in crates/yah/kg-daemon/src/shell_vt.rs instead.")
//! @yah:gotcha("`host_pid` is NOT on the wire. rpc::WireRunMeta does not carry it, so a client cannot ask 'is this run's owner alive'. Nothing needs it today — the policy runs entirely daemon-side — but R617-F7 should check whether reattaching tiles want it before adding a second liveness notion of their own.")
//! @yah:gotcha("pid reuse can make a dead owner read alive, leaving a run `Running` with nobody driving it. Bounded on purpose (opt-in + origin-narrowed) and strictly safer than the false-dead it replaces, but it is a real edge: if zombie terminal rows ever accumulate, this is why.")
//! @yah:gotcha("TaskRunMeta gained a required field, so every struct-literal construction site had to be updated (velveteen-exec x4, scryer, task-runs fixtures, kg-daemon fixtures, desktop/terminal.rs x2). A new construction site added by anyone else will fail to compile until they pick a value — which is the intended forcing function: a run with no recorded owner is a run the policy has to tombstone.")
//!
//! @yah:ticket(R617-B9, "Pre-existing: task-runs log_pipe_events_land_in_store never completes (233 pass / 1 fail)")
//! @yah:status(review)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:at(2026-07-22T19:50:25Z)
//! @yah:phase(P1)
//! @yah:parent(R617)
//! @yah:handoff("Root cause: not the FIFO, not the PTY. The whole pipeline completed correctly every time (child wrote the JSON line, receiver drained it, reader hit EOF, child.wait returned 0) — but the lifecycle's terminal `store.update_status` returned `Sql(Busy(\"database is locked\"))` and run_lifecycle swallowed it with `let _ =`, so the run stayed Running forever and the 20s poll deadline blew. A live run has three concurrent turso writers (PTY chunk appends, shim-FIFO event appends, lifecycle status) on independent connections with no busy handling at all.")
//! @yah:handoff("Fix in oss/qed/crates/task-runs/src/store.rs: (1) `conn()` now sets `busy_timeout(5s)` on every connection; (2) new `exec_retry()` wraps writes in an outer exponential-backoff retry on the `Busy`/`BusySnapshot` class, because turso caps its internal backoff and then hands `Busy` back; (3) insert_run / update_status / update_beholder_status / append_chunk / append_event all routed through it.")
//! @yah:handoff("driver.rs run_lifecycle no longer swallows the terminal status write — a genuine failure after retries now prints `[yah task-runs] failed to record terminal status for run <id>`, matching the crate's existing eprintln convention.")
//! @yah:handoff("New regression test store.rs::concurrent_writers_do_not_lose_the_terminal_status — two background tasks hammer append_chunk/append_event while update_status lands. Verified it has teeth: with busy_timeout and the retry disabled it fails 3/3 with the exact `Busy(\"database is locked\")`; with them it passes 5/5.")
//! @yah:verify("cd oss/qed && cargo test -p task-runs --lib — 237 passed / 0 failed (was 235 pass / 1 fail)")
//! @yah:verify("log_pipe_events_land_in_store run 8x sequentially: 8/8 green in ~0.58s each. Before the fix the same loop was 11/12 red at the 20s timeout.")
//!
//! @yah:ticket(R652-T6, "Login shell: when cmd is the resolved shell, exec it directly (not sh -c) with -l")
//! @yah:at(2026-08-02T00:03:08Z)
//! @yah:status(review)
//! @yah:assignee(agent:bundle-ollama-cloud-boulder)
//! @yah:phase(P1)
//! @yah:parent(R652)
//! @yah:handoff("Login shells now exec directly with -l instead of going through sh -c. SpawnOpts (oss/qed/crates/task-runs/src/driver.rs) gained `argv: Option<Vec<String>>`: when set, spawn_run builds the CommandBuilder from that argv verbatim instead of wrapping `cmd` in `sh -c`. camp-service task_run sets it to [resolved_shell, \"-l\"] whenever the request is a shell request.")
//! @yah:handoff("Why an argv escape hatch rather than a `login_shell: bool` flag in the driver: task-runs is an oss/qed crate and has no business knowing what a login shell is. The caller names the exact process; the driver just execs it. This also made R652-T4 a two-line addition rather than a second flag.")
//! @yah:handoff("Three things this fixes beyond .zprofile finally running. (1) `sh -c \"zsh -l\"` left an inert `sh` as the PTY's foreground process group leader, so job control misbehaved and signals went to the wrong process. (2) That same inert sh is what the foreground-pid cwd probe (R652-T2) would have reported for, so T2 could not have worked without this. (3) -l is now a real argv element instead of text inside a shell string, so no quoting layer can eat it.")
//! @yah:handoff("`cmd` is still what lands on TaskRunMeta.command, so a shell run reads back as \"$SHELL\" -- the rail label and the history re-run path both keep working. Beholder argv rewriting is bypassed when argv is set (the attach runs with BeholderSelect::None): the rewritten argv would be discarded on that path, so recording a `rewrite=...` that never happened would be a lie in the run metadata.")
//! @yah:handoff("An empty argv falls back to the sh -c path rather than spawning nothing -- a caller bug should not become an exec of the empty string.")
//! @yah:verify("cd oss/qed && cargo test -p task-runs --lib  # 246/246 green (3 new: explicit_argv_execs_the_program_directly, explicit_argv_still_records_the_requested_command, empty_argv_falls_back_to_the_shell_path)")
//! @yah:verify("Manual (needs desktop rebuild): add `echo W289-login-test >> /tmp/w289.log` to ~/.zprofile, open a shell tile, confirm the file gets a line")
//! @yah:gotcha("driver.rs is an oss/qed crate -- this lands in-tree under oss/qed/crates/task-runs and flows outward via scripts/export-oss.sh on the next release. SpawnOpts gained a field, but every in-tree construction site uses ..Default::default(), so nothing else needed touching.")

use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    /// `true` when this run's output is read as text by somebody downstream —
    /// a human watching a terminal tile, or a client that promised its caller
    /// byte-identical passthrough. Causes `Rewriter` beholders to decline in
    /// `Auto` mode, since a rewrite changes what that reader gets.
    ///
    /// R739-B10 renamed this from `tty_attached`: a PTY was only ever a proxy
    /// for "someone is reading this", and the proxy broke the moment
    /// `yah build run` moved onto pipes (R739-F6).
    pub verbatim_output: bool,
    /// Create a side-channel FIFO and export `YAH_TASK_RUN` / `YAH_LOG_PIPE`
    /// so Tier-2 shim libraries (yah-log-rust, @yah/log) can emit structured
    /// events. Has no effect on non-Unix platforms. Defaults to `true`.
    pub log_fd_enabled: bool,
    /// Provenance tag stored on the run's `TaskRunMeta.origin` (e.g.
    /// `Some("terminal")` for an interactive shell). `None` is an ordinary job.
    pub origin: Option<String>,
    /// Exec this argv directly instead of wrapping `cmd` in `sh -c`.
    ///
    /// The default `sh -c <cmd>` is right for a job — the caller wrote a
    /// command line and expects a shell to parse it. It is wrong for an
    /// *interactive shell*: `sh -c "zsh -l"` leaves an inert `sh` as the PTY's
    /// foreground process group leader, so job control misbehaves, signals go
    /// to the wrong process, and anything that reads the foreground pid (a
    /// live-cwd probe, say) sees `sh` instead of the shell the operator is
    /// typing into. Handing the exact argv here makes the shell itself the
    /// child, which is also the only way to pass `-l` as a real argv element
    /// so `.zprofile` / `.profile` actually run.
    ///
    /// `cmd` is still what gets recorded on `TaskRunMeta.command`, so the run
    /// reads the way the caller asked for it. Beholder argv rewriting is
    /// bypassed when this is set: the caller has already decided the exact
    /// process to exec, and a recorded `rewrite=…` that didn't happen would be
    /// a lie in the run metadata.
    pub argv: Option<Vec<String>>,
    /// R739-F6 — spawn on **pipes** instead of a PTY. Defaults to `false`,
    /// which is the PTY behaviour every existing caller already has.
    ///
    /// A PTY is right for an interactive terminal tile: the child gets a
    /// controlling terminal, job control works, and `isatty` says yes, which is
    /// what a human sitting in front of it expects. It is wrong for *emulating
    /// a non-interactive shell invocation*, where three PTY properties show up
    /// as divergence from running the same command directly (all three measured
    /// in R739-F4 against `cargo check`):
    ///
    /// 1. `isatty(1)` is true, so tools colorize — plain `error: …` arrives as
    ///    `\x1b[1m\x1b[91merror\x1b[0m: …`, which also defeats `| rg "^error"`.
    /// 2. The line discipline's `ONLCR` rewrites every `\n` the child wrote
    ///    into `\r\n`.
    /// 3. The terminal merges stderr into stdout, so stream separation is gone
    ///    by the time anything reads the capture.
    ///
    /// In pipe mode the child gets `pipe(2)` for stdout and stderr, chunks are
    /// stored under their true [`Stream`], and `TERM` is left alone rather than
    /// forced to `xterm-256color`. [`TaskDriver::resize_run`] and
    /// [`TaskDriver::foreground_pid`] have no PTY to answer for and report
    /// `NotFound` / `None`.
    pub pipe: bool,
    /// R901-B2 — run the `sh -c` line with `pipefail`, so a pipeline reports
    /// the **leftmost** failing stage instead of its last one. Defaults to
    /// `false`, i.e. POSIX behaviour, which is what every existing caller has.
    ///
    /// Without it a pipeline's status is the last stage's and nothing else:
    /// `cargo check 2>&1 | tail -40` exits **0** on a build with 101 errors,
    /// because `tail` succeeded. That is not a wrapper lying — the wrapper is
    /// faithful, and the shell is answering the question it was actually
    /// asked — but it is indistinguishable from a green build to everything
    /// downstream, including the harness task notification an agent reads to
    /// decide whether it is done. On 2026-09-13 two sessions read that 0 as a
    /// pass and left `cargo check -p yah` red camp-wide for ~50 minutes.
    ///
    /// Only meaningful when [`SpawnOpts::argv`] is `None`; an explicit argv is
    /// not a shell line and has no pipeline to take a status from.
    ///
    /// # Known cost, accepted deliberately
    ///
    /// `pipefail` also surfaces a producer killed by `SIGPIPE`, so
    /// `cargo check 2>&1 | head -40` can now report failure once `head` closes
    /// the pipe early on a build that was fine. That is a false RED, and it is
    /// the right trade against the false GREEN above: a red is investigated,
    /// a green ends the turn. Prefer `| tail` over `| head` on a build line.
    pub pipefail: bool,
}

/// Prefix that turns `pipefail` on for the rest of a `sh -c` line.
///
/// Probing in a subshell rather than running `set -o pipefail` directly is
/// load-bearing for portability, not caution. `pipefail` is a bash/ksh/zsh
/// option; `/bin/sh` is bash on macOS but **dash** on most Linux distros, and
/// dash rejects it. `set` is a POSIX *special* builtin, so a failure in one is
/// entitled to terminate a non-interactive shell — which would turn "your
/// pipeline now reports the truth" into "your command never ran at all" on
/// every Linux camp. The subshell absorbs that exit; the outer shell only ever
/// runs `set -o pipefail` on a shell that has already proved it accepts it.
const PIPEFAIL_PRELUDE: &str = "if (set -o pipefail) 2>/dev/null; then set -o pipefail; fi\n";

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
            verbatim_output: false,
            log_fd_enabled: true,
            origin: None,
            argv: None,
            pipe: false,
            pipefail: false,
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

// ─── Stale-run policy ────────────────────────────────────────────────────────

/// What a freshly-constructed [`TaskDriver`] does with `Running` rows it finds
/// already in the store.
///
/// The historical rule — tombstone every one of them — bakes in an assumption
/// that stops being true the moment a second process attaches to the same
/// store: that any `Running` row must be a corpse from *this* process's
/// predecessor. When two processes share a store, a driver starting up in one
/// will happily mark the other's live runs `Lost`, and the run keeps producing
/// output under a status that says it is dead.
///
/// The policy is deliberately origin-agnostic in its mechanism — it decides on
/// **who owns the run** ([`TaskRunMeta::host_pid`]) — and takes the origin list
/// as data, so an embedder names the runs it wants exempted without this crate
/// knowing what any of them mean.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum StaleRunPolicy {
    /// Tombstone every leftover `Running` run as `Lost`.
    ///
    /// Correct, and the default, whenever this process is the only writer:
    /// a run whose driver is gone has no one left to notice it exit.
    #[default]
    LostOnDisappear,
    /// Spare runs whose recorded owner process is still alive.
    ///
    /// A leftover run is tombstoned only when its `host_pid` is absent (owner
    /// unknown — a row from before the column existed) or names a process that
    /// no longer exists. Anything else belongs to a live peer and is left
    /// `Running` for that peer to finish.
    ///
    /// `origins` narrows the exemption to runs whose
    /// [`TaskRunMeta::origin`] is in the list; empty means every origin
    /// qualifies. A run with no origin never matches a non-empty list.
    AdoptLiveHosts { origins: Vec<String> },
}

impl StaleRunPolicy {
    /// Whether `meta` should be tombstoned `Lost` at driver construction.
    fn tombstones(&self, meta: &TaskRunMeta) -> bool {
        match self {
            StaleRunPolicy::LostOnDisappear => true,
            StaleRunPolicy::AdoptLiveHosts { origins } => {
                let exempt_origin = origins.is_empty()
                    || meta
                        .origin
                        .as_deref()
                        .is_some_and(|o| origins.iter().any(|want| want == o));
                if !exempt_origin {
                    return true;
                }
                match meta.host_pid {
                    Some(pid) => !host_process_alive(pid),
                    None => true,
                }
            }
        }
    }
}

/// Is a process with this pid still around?
///
/// `kill(pid, 0)` is the portable liveness probe: it performs the permission
/// check and existence lookup without delivering anything. `EPERM` counts as
/// alive — the process exists, it just is not ours to signal.
///
/// Pid reuse can make a dead owner read as alive. That is why
/// [`StaleRunPolicy::AdoptLiveHosts`] is opt-in and origin-narrowed: the cost
/// of a false "alive" is one run left `Running` until something closes it,
/// which is strictly better for an interactive session than the false "dead"
/// this replaces — which kills a *live* session's status.
#[cfg(unix)]
fn host_process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    if pid == std::process::id() {
        return true;
    }
    // SAFETY: `kill` with signal 0 delivers nothing; it only reports whether
    // the pid exists and is signallable.
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// No `kill(2)` off Unix. Reporting every owner dead keeps the historical
/// Lost-on-disappear behaviour rather than stranding runs `Running` forever.
#[cfg(not(unix))]
fn host_process_alive(_pid: u32) -> bool {
    false
}

// ─── Internal run-control handle ─────────────────────────────────────────────

struct RunControl {
    kill_tx: mpsc::Sender<KillRequest>,
    stdin_tx: Option<mpsc::Sender<Vec<u8>>>,
    /// Shared with the lifecycle task, which holds the same `Arc` so the PTY fd
    /// outlives `child.wait()`. `MasterPty::resize` takes `&self`, so a mutex is
    /// enough to make the `Box<dyn MasterPty + Send>` `Sync` across the two.
    ///
    /// `None` for a [`SpawnOpts::pipe`] run, which has no terminal to resize or
    /// to ask for a foreground process group.
    master: Option<Arc<Mutex<Box<dyn portable_pty::MasterPty + Send>>>>,
    /// R739-B12 — the run's [`SpawnOpts::origin`], copied here so
    /// [`TaskDriver::reap_unattached`] can narrow to an opted-in origin set
    /// without a store round-trip per candidate.
    origin: Option<String>,
    /// R739-B12 — when a client last looked at this run.
    ///
    /// Set at spawn (the caller that asked for the run is attached to it by
    /// definition) and refreshed by [`TaskDriver::note_attached`], which the
    /// embedder calls from whatever its "a client is watching" surface is —
    /// for the camp daemon, `task.tail` and `task.status`.
    ///
    /// Monotonic rather than a wall clock: a clock step must not be able to
    /// make a healthy build look abandoned.
    last_attached_at: Instant,
}

/// Fires the reader-done signal when the LAST holder drops.
///
/// A PTY run has one reader; a piped run has two (stdout and stderr) and the
/// lifecycle must not reap the child until both have hit EOF. Making this a
/// drop guard behind an `Arc` means neither path has to count readers: the
/// signal goes out when the refcount reaches zero, after each pump has finished
/// its own `on_done` work.
struct ReaderDone(Option<oneshot::Sender<()>>);

impl Drop for ReaderDone {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
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
        Self::with_config(store, channels, StaleRunPolicy::default()).await
    }

    /// Full constructor: side-channels plus the [`StaleRunPolicy`] applied to
    /// `Running` rows already in the store.
    ///
    /// R617-F6 — annotation in this file's header. Splitting the sweep out of
    /// the constructor's fixed behaviour is what lets a store be shared: a
    /// process that is not the run's owner can now attach without declaring
    /// the owner's live work dead.
    pub async fn with_config(
        store: Arc<TaskStore>,
        channels: DriverChannels,
        stale_policy: StaleRunPolicy,
    ) -> Result<Self, DriverError> {
        let stale = store
            .list_runs(&RunFilter {
                status: Some("running".to_string()),
                ..Default::default()
            })
            .await?;
        for meta in stale {
            if !stale_policy.tombstones(&meta) {
                continue;
            }
            store
                .update_status(
                    &meta.id,
                    &RunStatus::Lost {
                        reason: "daemon restarted while run was in-flight".to_string(),
                    },
                )
                .await?;
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
    /// When `opts.verbatim_output` is `true`, `Rewriter` beholders decline in
    /// `Auto` mode, because something downstream renders these bytes and a
    /// rewrite would change them.
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
        /* An explicit argv means the caller already chose the exact process
           (an interactive login shell, say). Selecting a beholder there would
           either do nothing — the rewritten argv is discarded on that path —
           or record a rewrite that never happened, so we opt out honestly
           instead. */
        let select = if opts.argv.is_some() {
            &BeholderSelect::None
        } else {
            &opts.beholder_select
        };
        let attach = registry.attach(cmd, select, opts.verbatim_output);
        // Reconstruct the command from argv ONLY when a beholder actually
        // rewrote it. `AttachResult.argv` is always populated — it is
        // `resolve_argv(cmd)` even when nothing attached — so joining it
        // unconditionally ran every run's command through a whitespace
        // normalization nobody asked for: runs of spaces collapse and embedded
        // newlines become spaces, which is silent corruption for a heredoc or
        // any multi-line line. The caller's bytes go to the shell untouched
        // unless a rewrite is the whole point.
        let effective_cmd = match &attach.status.rewrite_added {
            Some(added) if !added.is_empty() && !attach.argv.is_empty() => attach.argv.join(" "),
            _ => cmd.to_string(),
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
            /* R617-F6: stamp the OWNER, before the child exists. Written at
               insert rather than after spawn so a crash between the two still
               leaves the row attributable — an unattributed `Running` row is
               exactly what the conservative arm of `StaleRunPolicy` has to
               tombstone. */
            host_pid: Some(std::process::id()),
        }).await?;

        // The program and argv both spawn modes exec. An explicit argv execs
        // that program directly; otherwise the command line goes through `sh`
        // so the caller's quoting, pipes and redirections mean what they say.
        // An empty argv is a caller bug, not a request for an empty exec — fall
        // back to the shell path rather than spawning nothing.
        // R901-B2: `pipefail` is prepended HERE and not folded into
        // `effective_cmd`, so `TaskRunMeta.command` keeps reading as the line
        // the caller actually wrote. A run's recorded command is re-run by
        // history and audited by agents against the relocation note; a prelude
        // nobody asked for showing up in it would be the same class of lie as
        // recording a beholder `rewrite=…` that never happened.
        let (program, args): (String, Vec<String>) = match opts.argv.as_deref() {
            Some([p, rest @ ..]) => (p.clone(), rest.to_vec()),
            _ => {
                let line = if opts.pipefail {
                    format!("{PIPEFAIL_PRELUDE}{effective_cmd}")
                } else {
                    effective_cmd.clone()
                };
                ("sh".to_string(), vec!["-c".to_string(), line])
            }
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

        // The FIFO env, applied identically by both spawn modes.
        #[cfg(unix)]
        let fifo_env: Option<(String, String)> = log_fifo
            .as_ref()
            .map(|(_, _, path)| (id.to_string(), path.to_string_lossy().into_owned()));
        #[cfg(not(unix))]
        let fifo_env: Option<(String, String)> = None;

        /* Spawn. The two modes differ only in what the child's stdio is
           attached to, and everything downstream — reader pumps, lifecycle,
           kill — is written against the uniform handles produced here:
           `pid`, a `reap` closure that blocks until the child exits, and an
           optional PTY master for resize / foreground-pid. */
        let pid: u32;
        let reap: Box<dyn FnOnce() -> Option<u32> + Send>;
        let stdin_tx: Option<mpsc::Sender<Vec<u8>>>;
        let master: Option<Arc<Mutex<Box<dyn portable_pty::MasterPty + Send>>>>;
        // Each entry is one blocking source to pump into the store. The PTY
        // yields a single merged stream; pipes yield stdout and stderr apart.
        let mut sources: Vec<(Box<dyn Read + Send>, Stream)> = Vec::new();

        if opts.pipe {
            use std::process::{Command, Stdio};

            let mut cmd = Command::new(&program);
            cmd.args(&args);
            cmd.current_dir(&opts.cwd);
            for (k, v) in &opts.env {
                cmd.env(k, v);
            }
            /* Deliberately NOT setting TERM. The PTY path forces
               `xterm-256color` because a child on a terminal that claims no
               terminal type degrades badly; a child on a pipe should see
               whatever the daemon's own environment says, exactly as it would
               under a non-interactive shell. Forcing a terminal type here is
               how a pipe-mode run would talk itself back into colorizing. */
            if let Some((run_id_env, fifo_path)) = &fifo_env {
                cmd.env("YAH_TASK_RUN", run_id_env);
                cmd.env("YAH_LOG_PIPE", fifo_path);
            }
            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());
            cmd.stdin(if opts.stdin_enabled { Stdio::piped() } else { Stdio::null() });

            let mut child = cmd.spawn().map_err(DriverError::Io)?;
            pid = child.id();

            if let Some(out) = child.stdout.take() {
                sources.push((Box::new(out), Stream::Stdout));
            }
            if let Some(err) = child.stderr.take() {
                sources.push((Box::new(err), Stream::Stderr));
            }

            stdin_tx = child.stdin.take().map(|mut writer| {
                let (tx, mut rx) = mpsc::channel::<Vec<u8>>(64);
                task::spawn(async move {
                    use std::io::Write;
                    while let Some(bytes) = rx.recv().await {
                        let _ = writer.write_all(&bytes);
                        let _ = writer.flush();
                    }
                });
                tx
            });

            master = None;
            reap = Box::new(move || child.wait().ok().and_then(|s| s.code()).map(|c| c as u32));
        } else {
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
            sources.push((Box::new(pty_reader), Stream::Stdout));

            // Optional stdin relay: take the writer before spawning the child.
            stdin_tx = if opts.stdin_enabled {
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

            let mut cb = CommandBuilder::new(&program);
            cb.args(&args);
            cb.cwd(&opts.cwd);
            for (k, v) in &opts.env {
                cb.env(k, v);
            }
            cb.env("TERM", "xterm-256color");
            if let Some((run_id_env, fifo_path)) = &fifo_env {
                cb.env("YAH_TASK_RUN", run_id_env);
                cb.env("YAH_LOG_PIPE", fifo_path);
            }

            let child = pair
                .slave
                .spawn_command(cb)
                .map_err(|e| DriverError::Pty(e.to_string()))?;
            // Drop the parent's slave handle so EOF propagates once the child exits.
            drop(pair.slave);

            pid = child.process_id().unwrap_or(0);

            // Share the master between the lifecycle task (which must outlive
            // `child.wait()` so the fd stays open) and `resize_run`.
            let m: Arc<Mutex<Box<dyn portable_pty::MasterPty + Send>>> =
                Arc::new(Mutex::new(pair.master));
            master = Some(Arc::clone(&m));
            reap = Box::new(move || {
                let mut c = child;
                let _m = m; // dropped after wait() returns, closing the PTY fd
                c.wait().ok().map(|s| s.exit_code())
            });
        }

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

        /* Reader threads: child output → store chunks → beholder events. Each
           runs on a dedicated OS thread because the reads are blocking. The
           `ReaderDone` guard is shared across them, so the lifecycle's
           reader-done signal fires only once every source has hit EOF — which
           is what makes the two-pipe case correct without a reader count. */
        {
            let done = Arc::new(ReaderDone(Some(reader_done_tx)));
            /* The beholder goes to stdout only. It parses a structured
               protocol (cargo's JSON, say) that the child writes to stdout by
               definition, and there is exactly one of it — handing the same
               instance to two threads would need a lock for no gain, and
               feeding it stderr would make `unknown_format_reason` fire on
               human-readable diagnostics it was never meant to see. */
            let mut beholder = attach.beholder;
            for (reader, stream) in sources {
                spawn_output_pump(
                    reader,
                    stream,
                    Arc::clone(&self.store),
                    id.clone(),
                    started_at_ms,
                    self.channels.output.clone(),
                    if stream == Stream::Stdout { beholder.take() } else { None },
                    Arc::clone(&done),
                );
            }
        }

        // Lifecycle task: monitor kill requests, wait for exit, update status.
        // The task also holds the log FIFO write-end closer (if any) so that
        // EOF propagates to the receiver thread after RunStatus is written.
        {
            let store_l = Arc::clone(&self.store);
            let active_l = Arc::clone(&self.active);
            let id_l = id.clone();
            let completion_tx_l = self.channels.completion.clone();
            #[cfg(unix)]
            let wfd_l = log_wfd_holder;
            task::spawn(async move {
                run_lifecycle(
                    store_l,
                    active_l,
                    id_l,
                    pid,
                    reap,
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
            .insert(
                id.to_string(),
                RunControl {
                    kill_tx,
                    stdin_tx,
                    master,
                    origin: opts.origin.clone(),
                    last_attached_at: Instant::now(),
                },
            );

        Ok(id)
    }

    /// Resize a running task's PTY and deliver `SIGWINCH` to the foreground
    /// process group (portable-pty's `resize` does the ioctl, which is what
    /// signals the child).
    ///
    /// Returns `DriverError::NotFound` when the run is not active on this
    /// driver instance — the same contract as [`TaskDriver::send_stdin`] — and
    /// also when it is active but was spawned in [`SpawnOpts::pipe`] mode, which
    /// has no terminal to resize.
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
            .and_then(|c| c.master.as_ref().map(Arc::clone));

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

    /// The pid of the run's *foreground* process — the leader of the process
    /// group the PTY currently gives the keyboard to.
    ///
    /// For a shell tile that is the shell itself while it sits at a prompt,
    /// and the command the operator is running while one is in flight. That
    /// distinction is the whole point: asking the spawned child would report
    /// the shell forever, so anything derived from this pid (a live cwd probe,
    /// a "what is this pane doing" label) would answer for the wrong process.
    ///
    /// `None` when the run is not active on this driver instance, when it was
    /// spawned in [`SpawnOpts::pipe`] mode (no controlling terminal, so no
    /// foreground process group to read), or when the platform has no notion of
    /// a foreground process group.
    pub fn foreground_pid(&self, id: &TaskRunId) -> Option<u32> {
        let master = self
            .active
            .lock()
            .unwrap()
            .get(&id.to_string())
            .and_then(|c| c.master.as_ref().map(Arc::clone))?;
        #[cfg(unix)]
        {
            let pid = master.lock().unwrap().process_group_leader()?;
            u32::try_from(pid).ok()
        }
        #[cfg(not(unix))]
        {
            let _ = master;
            None
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

    /// R739-B12 — record that a client just looked at this run.
    ///
    /// A no-op for a run this driver does not own (already finished, or
    /// spawned by another process against the same store): attachment only
    /// means anything for a run something here could still signal.
    pub fn note_attached(&self, id: &TaskRunId) {
        if let Some(control) = self.active.lock().unwrap().get_mut(&id.to_string()) {
            control.last_attached_at = Instant::now();
        }
    }

    /// How long ago a client last looked at `id`, or `None` when this driver
    /// does not own the run. The observable half of [`Self::note_attached`].
    pub fn attached_age(&self, id: &TaskRunId) -> Option<Duration> {
        self.active
            .lock()
            .unwrap()
            .get(&id.to_string())
            .map(|c| c.last_attached_at.elapsed())
    }

    /// R739-B12 — SIGTERM every run of an opted-in origin that no client has
    /// looked at for `idle`. Returns the runs it signalled.
    ///
    /// This exists because a run outlives the client that asked for it. When
    /// `yah build run` is SIGKILLed — its harness dies, the terminal goes away
    /// — the cargo it relocated into the daemon keeps compiling with nobody
    /// attached, holding the build-directory lock until a human finds the pid.
    /// That happened on 2026-08-28 and stalled a whole camp for ~30 minutes.
    /// R739-B9 closed every give-up the client is *alive* to make; this closes
    /// the one it is not.
    ///
    /// **Not [`StaleRunPolicy`], and not that policy on a timer.** The policy
    /// is a construction-time reconciliation of rows a *previous process*
    /// left behind: it decides on `host_pid`, only ever calls
    /// `store.update_status`, and tombstones any run outside its origin list
    /// outright — so running it periodically would mark every in-flight run of
    /// an un-adopted origin `Lost` while it compiles perfectly well, and would
    /// still never signal the process that is the actual problem. This is the
    /// opposite shape: it decides on *attachment*, it signals, and it touches
    /// nothing outside `origins`.
    ///
    /// `origins` is an opt-in list precisely because most runs must never be
    /// reaped on this rule. An interactive terminal tile is legitimately
    /// unpolled for hours, and killing one would be a far worse bug than the
    /// orphan this prevents — so an empty list reaps nothing at all, rather
    /// than meaning "every origin" the way [`StaleRunPolicy`]'s list does.
    pub async fn reap_unattached(&self, idle: Duration, origins: &[String]) -> Vec<TaskRunId> {
        if origins.is_empty() {
            return Vec::new();
        }
        let candidates: Vec<TaskRunId> = {
            let active = self.active.lock().unwrap();
            active
                .iter()
                .filter(|(_, c)| {
                    c.origin
                        .as_deref()
                        .is_some_and(|o| origins.iter().any(|want| want == o))
                        && c.last_attached_at.elapsed() >= idle
                })
                .filter_map(|(id, _)| id.parse::<TaskRunId>().ok())
                .collect()
        };

        let mut reaped = Vec::new();
        for id in candidates {
            // SIGTERM, not SIGKILL: `kill_run` gives the child the same 5s
            // grace a `task.kill` from a live client would, then escalates.
            // A terminal status is also what releases the run's admission
            // enrollment (R739-F7), so a reaped run frees the build key.
            if self.kill_run(&id, None).await.is_ok() {
                reaped.push(id);
            }
        }
        reaped
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

/// Pump one blocking output source into the store, tapping and beholding on the
/// way past.
///
/// Split out of `spawn_run` for R739-F6: a PTY run has one source and a piped
/// run has two, and the only thing that differs between them is which [`Stream`]
/// the chunks are stored under. `done` is the shared [`ReaderDone`] guard —
/// dropping it here, after `on_done`, is what tells the lifecycle this source is
/// finished.
#[allow(clippy::too_many_arguments)]
fn spawn_output_pump(
    reader: Box<dyn Read + Send>,
    stream: Stream,
    store: Arc<TaskStore>,
    id: TaskRunId,
    started_at_ms: u64,
    output_tx: Option<mpsc::UnboundedSender<OutputChunk>>,
    beholder: Option<Box<dyn crate::beholders::Beholder>>,
    done: Arc<ReaderDone>,
) {
    let rt = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let _done = done;
        let mut beholder = beholder;
        let mut buf = [0u8; READ_BUF_SIZE];
        let mut reader = reader;
        loop {
            match reader.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let offset = elapsed_ms(started_at_ms);
                    let append_res =
                        rt.block_on(store.append_chunk(&id, offset, stream, &buf[..n]));
                    if let Ok(seq) = append_res {
                        /* Both the tap and the beholder want the same owned
                           chunk; build it once, and only when someone is
                           listening. */
                        let chunk = (output_tx.is_some() || beholder.is_some()).then(|| {
                            OutputChunk {
                                run_id: id.clone(),
                                seq,
                                offset_ms: offset,
                                stream,
                                bytes: buf[..n].to_vec(),
                            }
                        });
                        /* Tap first: it feeds live views, where latency is
                           visible to a human. Send failure means the host
                           dropped its receiver — never fatal. */
                        if let (Some(tx), Some(c)) = (&output_tx, &chunk) {
                            let _ = tx.send(c.clone());
                        }
                        let mut detach_beholder = false;
                        if let (Some(b), Some(chunk)) = (beholder.as_mut(), &chunk) {
                            for ev in b.parse_chunk(chunk) {
                                let _ = rt.block_on(store.append_event(
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
                                let new_status =
                                    BeholderStatus::unknown_format_with_reason(b.name(), reason);
                                let _ =
                                    rt.block_on(store.update_beholder_status(&id, &new_status));
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
            for ev in b.on_done(&id, final_offset) {
                let _ = rt.block_on(store.append_event(
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
                let _ = rt.block_on(store.update_beholder_status(&id, &new_status));
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_lifecycle(
    store: Arc<TaskStore>,
    active: Arc<Mutex<HashMap<String, RunControl>>>,
    id: TaskRunId,
    pid: u32,
    // `reap` blocks until the child exits and yields its exit code. It owns
    // whatever the spawn mode has to keep alive across the wait — for a PTY run
    // that includes the master fd, which must outlive `wait()`.
    reap: Box<dyn FnOnce() -> Option<u32> + Send>,
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

    // Reap the child (blocking) on a dedicated thread-pool slot. For a PTY run
    // the closure also owns our master handle, so the fd outlives the wait; the
    // matching `RunControl` (removed from `active` below) holds the other `Arc`,
    // so the fd actually closes once both are gone.
    let exit_code = task::spawn_blocking(reap).await.ok().flatten();

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

    /* Losing this write is not cosmetic: the run stays `Running` in the store
       forever and every reader — tail loops, the terminal UI, the next
       daemon's Lost-on-disappear sweep — believes a dead process is alive.
       `update_status` already retries through lock contention, so a failure
       here is terminal and worth saying out loud. */
    if let Err(e) = store.update_status(&id, &status).await {
        eprintln!("[yah task-runs] failed to record terminal status for run {id}: {e}");
    }
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
                host_pid: None,
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

    // ── Stale-run policy (R617-F6) ───────────────────────────────────────────

    /// Plant a `Running` row as if some other process had spawned it.
    async fn plant_running(
        store: &Arc<TaskStore>,
        origin: Option<&str>,
        host_pid: Option<u32>,
    ) -> TaskRunId {
        let id = TaskRunId::new();
        store
            .insert_run(&TaskRunMeta {
                id: id.clone(),
                command: "sleep 9999".to_string(),
                cwd: "/tmp".into(),
                env: vec![],
                started_at: unix_now_secs() - 60,
                status: RunStatus::Running,
                label: None,
                initiator: Initiator::Human {
                    camp: "test".to_string(),
                },
                beholder_status: None,
                pinned: false,
                origin: origin.map(str::to_string),
                host_pid,
            })
            .await
            .unwrap();
        id
    }

    async fn is_lost(store: &Arc<TaskStore>, id: &TaskRunId) -> bool {
        matches!(
            store.get_run(id).await.unwrap().unwrap().status,
            RunStatus::Lost { .. }
        )
    }

    fn adopt_terminal() -> StaleRunPolicy {
        StaleRunPolicy::AdoptLiveHosts {
            origins: vec!["terminal".to_string()],
        }
    }

    /// The property the whole ticket exists for: attaching to a store must not
    /// declare another live process's shell dead.
    #[tokio::test]
    async fn a_run_owned_by_a_live_host_survives_a_new_driver() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        // Our own pid is by definition a live process, and is the cheapest
        // honest stand-in for "a peer that is still running".
        let id = plant_running(&store, Some("terminal"), Some(std::process::id())).await;

        let _driver = TaskDriver::with_config(
            Arc::clone(&store),
            DriverChannels::default(),
            adopt_terminal(),
        )
        .await
        .unwrap();

        assert!(
            !is_lost(&store, &id).await,
            "a terminal run whose owner is alive must stay Running — \
             tombstoning it is what made a surviving shell read as dead"
        );
    }

    /// The other half: a genuinely abandoned shell must still be tombstoned,
    /// or a crashed host leaves permanent zombie tiles.
    #[tokio::test]
    async fn a_run_whose_host_is_gone_is_still_tombstoned() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        // Reaped in-test, so the pid is real-but-dead rather than guessed.
        let dead_pid = {
            let child = std::process::Command::new("true").spawn().unwrap();
            let pid = child.id();
            let mut child = child;
            let _ = child.wait();
            pid
        };
        let id = plant_running(&store, Some("terminal"), Some(dead_pid)).await;

        let _driver = TaskDriver::with_config(
            Arc::clone(&store),
            DriverChannels::default(),
            adopt_terminal(),
        )
        .await
        .unwrap();

        assert!(
            is_lost(&store, &id).await,
            "pid {dead_pid} was reaped; its run has no owner left and must be Lost"
        );
    }

    /// The exemption is narrowed by origin, so ordinary jobs keep the old rule
    /// even when their owner happens to still be alive — an in-flight `cargo
    /// build` whose driver is gone has nobody left to record its exit.
    #[tokio::test]
    async fn a_non_matching_origin_is_tombstoned_even_with_a_live_host() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let job = plant_running(&store, None, Some(std::process::id())).await;
        let other = plant_running(&store, Some("gnome"), Some(std::process::id())).await;

        let _driver = TaskDriver::with_config(
            Arc::clone(&store),
            DriverChannels::default(),
            adopt_terminal(),
        )
        .await
        .unwrap();

        assert!(is_lost(&store, &job).await, "an origin-less job is not exempt");
        assert!(
            is_lost(&store, &other).await,
            "an origin outside the list is not exempt"
        );
    }

    /// A row written before `host_pid` existed reads back `None`. Unknown
    /// ownership must fall back to the old behaviour rather than stranding the
    /// run `Running` forever.
    #[tokio::test]
    async fn an_unattributed_run_is_tombstoned() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let id = plant_running(&store, Some("terminal"), None).await;

        let _driver = TaskDriver::with_config(
            Arc::clone(&store),
            DriverChannels::default(),
            adopt_terminal(),
        )
        .await
        .unwrap();

        assert!(is_lost(&store, &id).await);
    }

    /// `TaskDriver::new` must not have quietly changed behaviour — every
    /// existing embedder still gets Lost-on-disappear.
    #[tokio::test]
    async fn the_default_policy_is_still_lost_on_disappear() {
        assert_eq!(StaleRunPolicy::default(), StaleRunPolicy::LostOnDisappear);

        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let id = plant_running(&store, Some("terminal"), Some(std::process::id())).await;

        let _driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        assert!(
            is_lost(&store, &id).await,
            "the default must tombstone regardless of origin or owner liveness"
        );
    }

    /// The owner is recorded by `spawn_run` itself, not by the caller — the
    /// policy is worthless if rows arrive unattributed.
    #[tokio::test]
    async fn spawn_run_stamps_this_process_as_the_owner() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let id = driver
            .spawn_run(
                "true",
                SpawnOpts {
                    cwd: "/tmp".into(),
                    origin: Some("terminal".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let meta = store.get_run(&id).await.unwrap().unwrap();
        assert_eq!(meta.host_pid, Some(std::process::id()));
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
                host_pid: None,
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

    // ── Pipe mode (R739-F6) ───────────────────────────────────────────────────

    /// Run `cmd` to completion and return its stored chunks.
    async fn run_to_completion(
        store: &Arc<TaskStore>,
        driver: &TaskDriver,
        cmd: &str,
        opts: SpawnOpts,
    ) -> Vec<OutputChunk> {
        let id = driver.spawn_run(cmd, opts).await.unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let meta = store.get_run(&id).await.unwrap().unwrap();
            if matches!(meta.status, RunStatus::Done { .. } | RunStatus::Lost { .. }) {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("run did not complete in time, status={:?}", meta.status);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        store.get_chunks(&id, &ChunkFilter::default()).await.unwrap()
    }

    fn joined(chunks: &[OutputChunk]) -> Vec<u8> {
        chunks.iter().flat_map(|c| c.bytes.clone()).collect()
    }

    fn joined_stream(chunks: &[OutputChunk], stream: Stream) -> Vec<u8> {
        chunks
            .iter()
            .filter(|c| c.stream == stream)
            .flat_map(|c| c.bytes.clone())
            .collect()
    }

    /// Divergence 1 of 3 (R739-F4): the child must not think it is on a
    /// terminal. This is the one that makes cargo colorize.
    #[tokio::test]
    async fn pipe_mode_child_sees_no_tty_on_stdout() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();
        let cmd = "if [ -t 1 ]; then echo TTY; else echo PIPE; fi";

        let piped = run_to_completion(
            &store,
            &driver,
            cmd,
            SpawnOpts { cwd: "/tmp".into(), pipe: true, ..Default::default() },
        )
        .await;
        assert_eq!(joined(&piped), b"PIPE\n");

        // The PTY default is unchanged — the terminal tiles depend on it.
        let ptied = run_to_completion(
            &store,
            &driver,
            cmd,
            SpawnOpts { cwd: "/tmp".into(), ..Default::default() },
        )
        .await;
        assert_eq!(joined(&ptied), b"TTY\r\n");
    }

    /// Divergence 2 of 3: no `ONLCR`, so a `\n` the child wrote stays a `\n`.
    /// This is what `build_run.rs::undo_onlcr` used to compensate for.
    #[tokio::test]
    async fn pipe_mode_does_not_translate_newlines() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let piped = run_to_completion(
            &store,
            &driver,
            r"printf 'a\nb\n'",
            SpawnOpts { cwd: "/tmp".into(), pipe: true, ..Default::default() },
        )
        .await;
        assert_eq!(joined(&piped), b"a\nb\n");
    }

    /// Divergence 3 of 3: stdout and stderr stay apart, under their true
    /// [`Stream`], instead of being merged by the terminal.
    #[tokio::test]
    async fn pipe_mode_keeps_stderr_separate_from_stdout() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();
        let cmd = "printf 'to-out\n'; printf 'to-err\n' >&2";

        let piped = run_to_completion(
            &store,
            &driver,
            cmd,
            SpawnOpts { cwd: "/tmp".into(), pipe: true, ..Default::default() },
        )
        .await;
        assert_eq!(joined_stream(&piped, Stream::Stdout), b"to-out\n");
        assert_eq!(joined_stream(&piped, Stream::Stderr), b"to-err\n");

        // Under a PTY the kernel merges them and everything lands on stdout —
        // the property that made stream separation unrecoverable downstream.
        let ptied = run_to_completion(
            &store,
            &driver,
            cmd,
            SpawnOpts { cwd: "/tmp".into(), ..Default::default() },
        )
        .await;
        assert!(
            joined_stream(&ptied, Stream::Stderr).is_empty(),
            "PTY runs have no stderr chunks; that is the behaviour pipe mode exists to fix",
        );
    }

    /// Both pipes must reach EOF before the child is reaped, or a run whose
    /// last bytes went to stderr would be marked terminal with output still
    /// unread. The 4 KiB write is larger than a pipe's atomic-write buffer, so
    /// this fails if either pump is dropped rather than awaited.
    #[tokio::test]
    async fn pipe_mode_drains_both_streams_before_the_run_is_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let piped = run_to_completion(
            &store,
            &driver,
            "head -c 4096 /dev/zero | tr '\\0' 'x'; head -c 4096 /dev/zero | tr '\\0' 'y' >&2",
            SpawnOpts { cwd: "/tmp".into(), pipe: true, ..Default::default() },
        )
        .await;
        assert_eq!(joined_stream(&piped, Stream::Stdout).len(), 4096);
        assert_eq!(joined_stream(&piped, Stream::Stderr).len(), 4096);
    }

    /// Exit codes have to survive the move to `std::process::Child`, which
    /// reports them through a different type than `portable_pty::Child`.
    #[tokio::test]
    async fn pipe_mode_records_the_childs_exit_code() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let id = driver
            .spawn_run(
                "exit 101",
                SpawnOpts { cwd: "/tmp".into(), pipe: true, ..Default::default() },
            )
            .await
            .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let meta = store.get_run(&id).await.unwrap().unwrap();
            match meta.status {
                RunStatus::Done { exit_code, .. } => {
                    assert_eq!(exit_code, 101);
                    return;
                }
                RunStatus::Lost { .. } | RunStatus::Killed { .. } => {
                    panic!("unexpected terminal status {:?}", meta.status)
                }
                _ => {}
            }
            if std::time::Instant::now() > deadline {
                panic!("run did not complete in time");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// A pipe run has no terminal, and the two PTY-only verbs must say so
    /// rather than reaching into a `None` master.
    #[tokio::test]
    async fn pipe_mode_has_no_terminal_to_resize_or_read_a_foreground_pid_from() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let id = driver
            .spawn_run(
                "sleep 2",
                SpawnOpts { cwd: "/tmp".into(), pipe: true, ..Default::default() },
            )
            .await
            .unwrap();

        assert!(matches!(
            driver.resize_run(&id, 100, 40).await,
            Err(DriverError::NotFound(_))
        ));
        assert_eq!(driver.foreground_pid(&id), None);
        let _ = driver.kill_run(&id, Some(SIGKILL)).await;
    }

    /// Wait for a run to reach a terminal status, or panic.
    async fn await_done(store: &TaskStore, id: &TaskRunId) -> TaskRunMeta {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let meta = store.get_run(id).await.unwrap().unwrap();
            if matches!(meta.status, RunStatus::Done { .. } | RunStatus::Lost { .. }) {
                return meta;
            }
            if std::time::Instant::now() > deadline {
                panic!("run did not complete in time, status={:?}", meta.status);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn output_of(store: &TaskStore, id: &TaskRunId) -> String {
        let chunks = store.get_chunks(id, &ChunkFilter::default()).await.unwrap();
        let bytes: Vec<u8> = chunks.into_iter().flat_map(|c| c.bytes).collect();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    // ── The caller's bytes reach the shell unchanged (R739-S2) ───────────────

    /// `AttachResult.argv` is populated on every run, rewrite or not, so
    /// `spawn_run` used to join it back into the command line unconditionally.
    /// That put every `task.run` command through a whitespace normalization
    /// nobody asked for. A multi-line command is the case where that is not
    /// cosmetic: the newline the caller wrote becomes a space, and two
    /// commands become one nonsense command.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_multi_line_command_is_not_flattened_into_one_line() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = Arc::new(TaskDriver::new(Arc::clone(&store)).await.unwrap());

        // Flattened to one line this is `echo one echo two`, which prints
        // "one echo two" — a different answer, not a failure, which is what
        // makes the old behaviour dangerous rather than merely wrong.
        let id = driver
            .spawn_run(
                "echo one\necho two",
                SpawnOpts { cwd: "/tmp".into(), ..Default::default() },
            )
            .await
            .unwrap();
        await_done(&store, &id).await;

        let out = output_of(&store, &id).await;
        assert!(out.contains("one"), "got: {out:?}");
        assert!(
            out.contains("two"),
            "the second line must have run as its own command; got: {out:?}"
        );
        assert!(
            !out.contains("one echo two"),
            "the newline was flattened into a space; got: {out:?}"
        );
    }

    /// `resolve_argv` strips `bunx`/`npx`/`pnpm` so a beholder's `matches` sees
    /// the bare tool. That is a *matching* concern; it must never reach the
    /// spawn, or the wrapper the caller needed is gone from the command.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_wrapper_the_caller_wrote_is_not_stripped_from_the_spawned_command() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = Arc::new(TaskDriver::new(Arc::clone(&store)).await.unwrap());

        // `npx` is almost certainly absent in test environments, and that is
        // the point: if the wrapper survived, the shell reports it missing. If
        // it were stripped we would be running bare `--version`.
        let id = driver
            .spawn_run(
                "npx r739s2-nonexistent-tool --version",
                SpawnOpts { cwd: "/tmp".into(), ..Default::default() },
            )
            .await
            .unwrap();
        let meta = await_done(&store, &id).await;
        let out = output_of(&store, &id).await;
        assert!(
            !matches!(meta.status, RunStatus::Done { exit_code: 0, .. }),
            "expected a failure, got {:?} with output {out:?}",
            meta.status
        );
        assert!(
            !out.contains("--version: "),
            "the wrapper was stripped and the shell tried to run the flag; got: {out:?}"
        );
    }

    // ── Direct argv (R652-T6) ────────────────────────────────────────────────

    #[tokio::test]
    async fn explicit_argv_execs_the_program_directly() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        /* The distinguishing observation: under `sh -c` the child is `sh` and
           `$0` is `sh`; exec'd directly it is the program itself. Printing
           `$0` is the cheapest way to see which of the two happened. */
        let id = driver
            .spawn_run(
                "unused-because-argv-wins",
                SpawnOpts {
                    cwd: "/tmp".into(),
                    argv: Some(vec![
                        "/bin/sh".into(),
                        "-c".into(),
                        "printf 'argv0=%s\\n' \"$0\"".into(),
                        "direct-exec-marker".into(),
                    ]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        await_done(&store, &id).await;
        let text = output_of(&store, &id).await;
        assert!(
            text.contains("argv0=direct-exec-marker"),
            "argv should have been exec'd verbatim, got: {text:?}"
        );
    }

    #[tokio::test]
    async fn explicit_argv_still_records_the_requested_command() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        /* A shell tile asks for "$SHELL" and the daemon resolves it to a real
           argv. The run must still read back as what was asked for, or the
           rail row and the history re-run both show an implementation
           detail. */
        let id = driver
            .spawn_run(
                "$SHELL",
                SpawnOpts {
                    cwd: "/tmp".into(),
                    argv: Some(vec!["/bin/sh".into(), "-c".into(), "true".into()]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let meta = await_done(&store, &id).await;
        assert_eq!(meta.command, "$SHELL");
        assert!(
            matches!(meta.status, RunStatus::Done { exit_code: 0, .. }),
            "expected Done(0), got {:?}",
            meta.status
        );
    }

    /// R901-B2. The control is the whole point: without the `pipefail: false`
    /// half this would pass if `pipefail` stopped existing, and with only the
    /// `true` half it would pass if every pipeline had always reported its
    /// leftmost failure. The pair pins the *difference*, which is the thing
    /// that cost this camp ~50 minutes of red tree.
    #[tokio::test]
    async fn pipefail_reports_the_failing_stage_and_posix_reports_the_last_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        // `(exit 101) | tail -1` is `cargo check 2>&1 | tail -40` with the
        // compile stripped out: a failing producer feeding a succeeding tail.
        let line = "(exit 101) | tail -1";

        let posix = driver
            .spawn_run(
                line,
                SpawnOpts { cwd: "/tmp".into(), pipefail: false, ..Default::default() },
            )
            .await
            .unwrap();
        let meta = await_done(&store, &posix).await;
        assert!(
            matches!(meta.status, RunStatus::Done { exit_code: 0, .. }),
            "POSIX pipeline status is the LAST stage's — expected Done(0), got {:?}",
            meta.status
        );

        let failing = driver
            .spawn_run(
                line,
                SpawnOpts { cwd: "/tmp".into(), pipefail: true, ..Default::default() },
            )
            .await
            .unwrap();
        let meta = await_done(&store, &failing).await;
        assert!(
            matches!(meta.status, RunStatus::Done { exit_code: 101, .. }),
            "pipefail must surface the producer's 101, got {:?}",
            meta.status
        );
    }

    /// The prelude must not reach [`TaskRunMeta::command`]: that string is what
    /// history re-runs and what an agent audits the relocation note against.
    #[tokio::test]
    async fn pipefail_does_not_leak_into_the_recorded_command() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let id = driver
            .spawn_run(
                "echo recorded-verbatim | cat",
                SpawnOpts { cwd: "/tmp".into(), pipefail: true, ..Default::default() },
            )
            .await
            .unwrap();

        let meta = await_done(&store, &id).await;
        assert_eq!(meta.command, "echo recorded-verbatim | cat");
        assert!(
            !meta.command.contains("pipefail"),
            "the prelude leaked into the recorded command: {:?}",
            meta.command
        );
    }

    /// The portability guard. On a `/bin/sh` that rejects `pipefail` (dash, i.e.
    /// most Linux camps) the probe must degrade to plain POSIX semantics — it
    /// must NOT take the shell down with it, because `set` is a special builtin
    /// and a bare `set -o pipefail` there is entitled to exit before the
    /// caller's command runs at all. Asserting the command still produces its
    /// output is asserting exactly that.
    #[tokio::test]
    async fn the_pipefail_probe_never_costs_the_command_that_follows_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let id = driver
            .spawn_run(
                "echo probe-survived",
                SpawnOpts { cwd: "/tmp".into(), pipefail: true, ..Default::default() },
            )
            .await
            .unwrap();

        let meta = await_done(&store, &id).await;
        let text = output_of(&store, &id).await;
        assert!(
            matches!(meta.status, RunStatus::Done { exit_code: 0, .. }),
            "expected Done(0), got {:?}",
            meta.status
        );
        assert!(text.contains("probe-survived"), "command did not run, got: {text:?}");
        // The probe itself must be silent — it runs on every relocated build.
        assert!(
            !text.contains("pipefail"),
            "the probe printed a diagnostic into the build's own output: {text:?}"
        );
    }

    #[tokio::test]
    async fn empty_argv_falls_back_to_the_shell_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let id = driver
            .spawn_run(
                "echo empty_argv_fallback",
                SpawnOpts { cwd: "/tmp".into(), argv: Some(vec![]), ..Default::default() },
            )
            .await
            .unwrap();

        await_done(&store, &id).await;
        let text = output_of(&store, &id).await;
        assert!(
            text.contains("empty_argv_fallback"),
            "empty argv must not spawn nothing, got: {text:?}"
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

    // ── Unattached-run reaper (R739-B12) ─────────────────────────────────────

    /// The origin `yah build run` tags its relocated builds with. Spelled out
    /// here rather than imported: what the reaper must do is defined by the
    /// string on the wire, not by any constant this crate owns.
    const BUILD_RUN: &str = "build-run";

    fn opted_in() -> Vec<String> {
        vec![BUILD_RUN.to_string()]
    }

    async fn spawn_long_run(driver: &TaskDriver, origin: &str) -> TaskRunId {
        driver
            .spawn_run(
                "sleep 30",
                SpawnOpts {
                    cwd: "/tmp".into(),
                    origin: Some(origin.to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
    }

    async fn await_status(
        store: &Arc<TaskStore>,
        id: &TaskRunId,
        want: fn(&RunStatus) -> bool,
    ) -> RunStatus {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let status = store.get_run(id).await.unwrap().unwrap().status;
            if want(&status) {
                return status;
            }
            if std::time::Instant::now() > deadline {
                panic!("run never reached the expected status, last={status:?}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// The orphan this ticket exists for: the client is gone, so nothing polls
    /// the run, so the daemon must end it.
    #[tokio::test]
    async fn an_unpolled_run_of_an_opted_in_origin_is_reaped() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let id = spawn_long_run(&driver, BUILD_RUN).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let reaped = driver
            .reap_unattached(Duration::from_millis(200), &opted_in())
            .await;
        assert_eq!(reaped, vec![id.clone()], "the unattached run should be reaped");

        let status = await_status(&store, &id, |s| {
            matches!(s, RunStatus::Killed { .. } | RunStatus::Done { .. })
        })
        .await;
        assert!(
            matches!(status, RunStatus::Killed { .. }),
            "a reaped run ends Killed, got {status:?}"
        );
    }

    /// The regression that protects a healthy long build: a client that is
    /// still polling keeps its run alive however long the build takes.
    #[tokio::test]
    async fn a_run_a_client_is_still_polling_is_never_reaped() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let id = spawn_long_run(&driver, BUILD_RUN).await;

        // Six polls across three idle windows — what `yah build run`'s tail
        // loop does, slowed down.
        for _ in 0..6 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            driver.note_attached(&id);
            let reaped = driver
                .reap_unattached(Duration::from_millis(200), &opted_in())
                .await;
            assert!(reaped.is_empty(), "a polled run must survive, reaped {reaped:?}");
        }

        assert!(
            matches!(
                store.get_run(&id).await.unwrap().unwrap().status,
                RunStatus::Running
            ),
            "the polled run should still be running"
        );

        // And the moment the polling stops, it becomes reapable — same run,
        // so this pins the refresh rather than a missing origin match.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let reaped = driver
            .reap_unattached(Duration::from_millis(200), &opted_in())
            .await;
        assert_eq!(reaped, vec![id], "a run that stopped being polled is reapable");
    }

    /// The regression that protects real users' terminals. An interactive tile
    /// sits unpolled for hours by design and must never be touched, however
    /// long the reaper runs.
    #[tokio::test]
    async fn a_terminal_tile_is_never_reaped_however_long_it_idles() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let id = spawn_long_run(&driver, "terminal").await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        for _ in 0..3 {
            let reaped = driver.reap_unattached(Duration::ZERO, &opted_in()).await;
            assert!(
                reaped.is_empty(),
                "a terminal tile is outside the opted-in origins, reaped {reaped:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(
            matches!(
                store.get_run(&id).await.unwrap().unwrap().status,
                RunStatus::Running
            ),
            "the terminal run must still be running"
        );

        driver.kill_run(&id, Some(SIGKILL)).await.unwrap();
    }

    /// A run with no origin at all — an ordinary `task.run` job — is outside
    /// every opt-in list, and an empty list reaps nothing.
    #[tokio::test]
    async fn an_origin_less_run_and_an_empty_opt_in_list_reap_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let plain = driver
            .spawn_run("sleep 30", SpawnOpts { cwd: "/tmp".into(), ..Default::default() })
            .await
            .unwrap();
        let build = spawn_long_run(&driver, BUILD_RUN).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(
            driver.reap_unattached(Duration::ZERO, &[]).await.is_empty(),
            "an empty opt-in list must reap nothing, not everything"
        );
        assert_eq!(
            driver.reap_unattached(Duration::ZERO, &opted_in()).await,
            vec![build],
            "only the opted-in origin is reapable"
        );

        assert!(
            matches!(
                store.get_run(&plain).await.unwrap().unwrap().status,
                RunStatus::Running
            ),
            "the origin-less run must be untouched"
        );
        driver.kill_run(&plain, Some(SIGKILL)).await.unwrap();
    }

    /// `note_attached` is observable, and the age it resets is what the sweep
    /// reads.
    #[tokio::test]
    async fn attached_age_resets_on_a_poll_and_is_none_for_a_foreign_run() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir).await;
        let driver = TaskDriver::new(Arc::clone(&store)).await.unwrap();

        let id = spawn_long_run(&driver, BUILD_RUN).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let aged = driver.attached_age(&id).expect("driver owns this run");
        assert!(aged >= Duration::from_millis(100), "age should have grown, got {aged:?}");

        driver.note_attached(&id);
        let fresh = driver.attached_age(&id).unwrap();
        assert!(fresh < aged, "a poll resets the age: {fresh:?} vs {aged:?}");

        assert!(
            driver.attached_age(&TaskRunId::new()).is_none(),
            "a run this driver does not own has no attachment age"
        );

        driver.kill_run(&id, Some(SIGKILL)).await.unwrap();
    }
}
