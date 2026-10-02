//! Unix socket ingestion server for yah-log service-scope events.
//!
//! Who runs this, and who points workloads at it (R893-B17 — the entry above
//! this one used to say "yubaba" for both halves, and yubaba does neither):
//!
//! - The `yah-scryer` daemon binds the socket, when started with
//!   `--ingest-socket <path>` (typically `/run/yah/scryer.sock`). Without that
//!   flag no socket is bound and nothing can be ingested this way.
//! - **kamaji** puts `YAH_SERVICE_IDENT` + `YAH_SCRYER_SOCKET` in the workload's
//!   env, because kamaji owns workload env — see `kamaji::observe`. Node-local
//!   opt-in via `kamaji --scryer-socket <same path>`. A containerized workload
//!   additionally gets the socket bind-mounted in and reads the in-container
//!   path, since the host path means nothing inside its mount namespace.
//!
//! With both in place a workload using `yah-log` writes structured events into
//! scryer's store with `EventScope::Service(MeshIdent)` scope — without needing
//! a containerd log stream — and a passway door writes spans over the same
//! socket (R893-F16).
//!
//! Wire format is [`observation::IngestLine`] — one signal-tagged JSON object
//! per line. Two signals today:
//! ```text
//! {"signal":"event","scope_kind":"service","scope_id":"<ident>","level":"...","target":"...","msg":"...","fields":{...},"_lib":"yah-log","_lib_ver":"..."}
//! {"signal":"span","scope_kind":"service","scope_id":"<ident>","span":{"trace_id":"<32hex>","span_id":"<16hex>",...}}
//! ```
//!
//! R893-F16 added the `signal` tag and the span arm; the tag is REQUIRED, and
//! an untagged line is dropped rather than assumed to be an event. That is a
//! deliberate break of the pre-tag protocol — see `observation::ingest`'s
//! module doc for why a defaulted tag was the wrong shape. The break was free
//! because at the time nothing injected `YAH_SCRYER_SOCKET` at all; R893-B17
//! then added the producer, so both in-tree writers (`yah-log`'s service layer
//! and passway's span exporter) already emit the tag.
//!
//! Spans do NOT go through the event ring. The ring exists to batch log lines
//! whose per-scope `seq` must stay monotonic; a span already carries its own
//! identity, its own timestamp and its own duration, and
//! `EventStore::insert_spans` is `INSERT OR IGNORE` with the rollup update
//! riding the same transaction (R893-F15 trap (a)). Writing straight through
//! keeps the raw row and its rollup atomic, which a ring flush would not.
//!
//! Each accepted connection gets a dedicated tokio task that reads lines until
//! the client closes the connection.  Per-scope seq counters are shared across
//! all connections so a workload restart doesn't reset the seq stream.
//!

use crate::service::Scryer;
use observation::{Event, EventScope, EventSource, IngestLine, Level, TaskRunId};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixListener;
use workload_spec::MeshIdent;

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum IngestionError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("scryer push: {0}")]
    Push(#[from] crate::service::ScryerError),
}

// ─── Shared per-scope seq counters ────────────────────────────────────────────

#[derive(Default)]
struct SeqCounters(HashMap<String, u32>);

impl SeqCounters {
    fn next(&mut self, scope_key: &str) -> u32 {
        let c = self.0.entry(scope_key.to_string()).or_insert(0);
        let v = *c;
        *c = c.wrapping_add(1);
        v
    }
}

// ─── IngestionServer ──────────────────────────────────────────────────────────

/// Listens on a Unix socket and pushes ingested service-scope events into
/// `Scryer`.  Multiple concurrent clients (workloads) are accepted; each gets
/// its own line-reader task.
pub struct IngestionServer {
    scryer: Arc<Scryer>,
    socket_path: PathBuf,
    started_at: Instant,
    /// Shared across all accepted connections so restarts don't reset seq.
    seq_counters: Arc<Mutex<SeqCounters>>,
}

impl IngestionServer {
    pub fn new(scryer: Arc<Scryer>, socket_path: impl AsRef<Path>) -> Self {
        Self {
            scryer,
            socket_path: socket_path.as_ref().to_owned(),
            started_at: Instant::now(),
            seq_counters: Arc::new(Mutex::new(SeqCounters::default())),
        }
    }

    /// Bind the Unix socket, returning a listener that is already accepting
    /// addresses before any caller believes it is.
    ///
    /// Separate from [`BoundIngestion::serve`] deliberately (R893-B17): the
    /// daemon has to be able to FAIL STARTUP on a bind error. Folding the bind
    /// into a spawned accept loop would make "this node ingests nothing"
    /// indistinguishable from "no workload has written yet" — the exact
    /// silent-nowhere failure this ticket removes. It also retires the
    /// bind-race sleep every test here used to need.
    ///
    /// The parent directory is created and a stale socket file removed first:
    /// `bind(2)` fails with `EADDRINUSE` on a leftover inode, so without this a
    /// daemon that was `SIGKILL`ed could never ingest again. Removing is safe
    /// because a live listener on this path would mean a second `yah-scryer`
    /// owning this node's socket, which is a misconfiguration rather than a
    /// state worth preserving.
    pub async fn bind(&self) -> Result<BoundIngestion, IngestionError> {
        if let Some(parent) = self.socket_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if tokio::fs::metadata(&self.socket_path).await.is_ok() {
            tokio::fs::remove_file(&self.socket_path).await?;
        }
        let listener = UnixListener::bind(&self.socket_path)?;
        // 0666, and this is not hygiene theatre. `connect(2)` on an `AF_UNIX`
        // socket needs WRITE permission on the inode, the daemon runs under
        // systemd `DynamicUser=yes`, and every workload that writes here runs
        // as some other uid entirely. Under the default 022 umask the socket
        // lands 0755 and every one of those connects fails `EACCES` — which
        // presents as "nothing is traced", the failure R893-B17 exists to
        // remove. The socket is a local-agent write port with a per-MeshIdent
        // quota behind it (`quota::ServiceQuotaManager`); node-local processes
        // being able to reach it is the design, not a leak.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&self.socket_path, std::fs::Permissions::from_mode(0o666))?;
        }
        Ok(BoundIngestion {
            listener,
            scryer: Arc::clone(&self.scryer),
            started_at: self.started_at,
            seq_counters: Arc::clone(&self.seq_counters),
        })
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

/// A bound ingestion socket, ready to accept. Produced by
/// [`IngestionServer::bind`].
pub struct BoundIngestion {
    listener: UnixListener,
    scryer: Arc<Scryer>,
    started_at: Instant,
    seq_counters: Arc<Mutex<SeqCounters>>,
}

impl BoundIngestion {
    /// Accept connections until the task is cancelled. Each connection gets its
    /// own reader task; per-scope seq counters are shared across all of them so
    /// a workload restart does not reset the seq stream.
    pub async fn serve(self) -> Result<(), IngestionError> {
        let listener = self.listener;
        loop {
            let (stream, _) = listener.accept().await?;
            let scryer = Arc::clone(&self.scryer);
            let started_at = self.started_at;
            let seq_counters = Arc::clone(&self.seq_counters);

            tokio::spawn(async move {
                let reader = BufReader::new(stream);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let Ok(parsed) = serde_json::from_str::<IngestLine>(&line) else {
                        continue;
                    };
                    let (scope_kind, scope_id) = parsed.scope();
                    // Only `service` is addressable from the socket: a
                    // workload may not write into another run's or forge's
                    // scope. Unknown kinds are skipped, not guessed at.
                    if scope_kind != "service" {
                        continue;
                    }
                    let scope = EventScope::Service(MeshIdent(scope_id.to_string()));
                    let scope_key = format!("service:{scope_id}");

                    match parsed {
                        IngestLine::Span { span, .. } => {
                            // Straight through to the store, deliberately not
                            // via the ring — see this module's doc.
                            let _ = scryer.store().insert_spans(&[(scope, *span)]);
                        }
                        IngestLine::Event { level, target, msg, fields, .. } => {
                            let seq = {
                                let mut c = seq_counters.lock().unwrap();
                                c.next(&scope_key)
                            };
                            let offset_ms =
                                started_at.elapsed().as_millis().min(u32::MAX as u128) as u32;
                            let level = Level::from_str(&level).unwrap_or(Level::Info);
                            let event = Event {
                                run_id: TaskRunId::new(),
                                seq,
                                offset_ms,
                                level,
                                target,
                                msg,
                                fields,
                                anchor: None,
                                source: EventSource::Shim {
                                    lib: "yah-log".to_string(),
                                    version: "unknown".to_string(),
                                },
                            };
                            let _ = scryer.push(scope, event);
                        }
                    }
                }
            });
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::{EventFilter, Scryer, ScryerConfig};
    use observation::Level as ObsLevel;
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

    fn open_scryer(dir: &TempDir) -> Arc<Scryer> {
        let cfg = ScryerConfig::new(dir.path().join("events.db"));
        Arc::new(Scryer::new(cfg, None).unwrap())
    }

    /// R893-B17: a `SIGKILL`ed daemon leaves its socket inode behind, and
    /// `bind(2)` refuses a path that already exists. Without the unlink, a node
    /// that crashed once would ingest nothing forever after — and would present
    /// as "all workload telemetry stopped", not as a bind error.
    #[tokio::test]
    async fn a_stale_socket_file_does_not_wedge_the_next_bind() {
        let dir = TempDir::new().unwrap();
        let socket_path = dir.path().join("nested").join("stale.sock");
        let scryer = open_scryer(&dir);

        // First bind creates the parent directory the daemon's data dir does
        // not own, and the socket.
        let first = IngestionServer::new(Arc::clone(&scryer), &socket_path)
            .bind()
            .await
            .expect("first bind creates its parent dir");
        drop(first);
        assert!(socket_path.exists(), "the socket inode outlives the listener");

        IngestionServer::new(Arc::clone(&scryer), &socket_path)
            .bind()
            .await
            .expect("a stale socket file must be cleared, not fatal");
    }

    /// Verify: events written to the ingestion socket arrive in scryer's store
    /// with `EventScope::Service` scope.
    #[tokio::test]
    async fn ingestion_server_service_scope() {
        let dir = TempDir::new().unwrap();
        let socket_path = dir.path().join("ingest.sock");
        let scryer = open_scryer(&dir);

        let bound = IngestionServer::new(Arc::clone(&scryer), &socket_path)
            .bind()
            .await
            .unwrap();
        let server_task = tokio::spawn(async move { let _ = bound.serve().await; });

        // Client: write one JSON line and close the connection.
        let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        let line = format!(
            "{}\n",
            json!({
                "signal":     "event",
                "scope_kind": "service",
                "scope_id":   "my-service.host",
                "level":      "info",
                "target":     "ingestion::test",
                "msg":        "hello from yah-log",
                "fields":     { "service_ident": "my-service.host" },
                "_lib":       "yah-log",
                "_lib_ver":   "0.1.0"
            })
        );
        stream.write_all(line.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream); // close → connection task exits its read loop

        // Allow the server's connection task to run and push the event.
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        server_task.abort();

        // Events land in the ring; flush to short-disk so `events()` can see them.
        scryer.flush_ring().unwrap();

        let scope = EventScope::Service(MeshIdent("my-service.host".to_string()));
        let events = scryer.events(&scope, &EventFilter::default()).await.unwrap();
        assert_eq!(events.len(), 1, "expected 1 service-scope event; got {:?}", events.len());
        assert_eq!(events[0].level, ObsLevel::Info);
        assert_eq!(events[0].target, "ingestion::test");
        assert_eq!(events[0].msg, "hello from yah-log");
    }

    /// Verify: unknown scope_kind lines are silently skipped without crashing.
    #[tokio::test]
    async fn ingestion_server_skips_unknown_scope() {
        let dir = TempDir::new().unwrap();
        let socket_path = dir.path().join("unknown_scope.sock");
        let scryer = open_scryer(&dir);

        let bound = IngestionServer::new(Arc::clone(&scryer), &socket_path)
            .bind()
            .await
            .unwrap();
        let server_task = tokio::spawn(async move { let _ = bound.serve().await; });

        let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        // unknown scope kind + one valid service line
        let lines = format!(
            "{}\n{}\n",
            json!({"signal":"event","scope_kind":"future","scope_id":"x","level":"info","target":"t","msg":"m","fields":{}}),
            json!({"signal":"event","scope_kind":"service","scope_id":"svc.host","level":"warn","target":"t","msg":"kept","fields":{}})
        );
        stream.write_all(lines.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream);

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        server_task.abort();

        scryer.flush_ring().unwrap();

        let scope = EventScope::Service(MeshIdent("svc.host".to_string()));
        let events = scryer.events(&scope, &EventFilter::default()).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, ObsLevel::Warn);
    }

    /// Verify: per-scope seq is shared across reconnects (no seq reset).
    #[tokio::test]
    async fn ingestion_server_seq_monotonic_across_connections() {
        let dir = TempDir::new().unwrap();
        let socket_path = dir.path().join("seq_mono.sock");
        let scryer = open_scryer(&dir);

        let bound = IngestionServer::new(Arc::clone(&scryer), &socket_path)
            .bind()
            .await
            .unwrap();
        let server_task = tokio::spawn(async move { let _ = bound.serve().await; });

        let write_event = |msg: &'static str| {
            let socket_path = socket_path.clone();
            async move {
                let mut s = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
                let line = format!(
                    "{}\n",
                    json!({"signal":"event","scope_kind":"service","scope_id":"seq.host","level":"info","target":"t","msg":msg,"fields":{}})
                );
                s.write_all(line.as_bytes()).await.unwrap();
                s.flush().await.unwrap();
                drop(s);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };

        write_event("event-0").await;
        write_event("event-1").await;

        server_task.abort();

        scryer.flush_ring().unwrap();

        let scope = EventScope::Service(MeshIdent("seq.host".to_string()));
        let events = scryer.events(&scope, &EventFilter::default()).await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(events[0].seq < events[1].seq, "seq must be monotonically increasing");
    }

    /// R893-F16 — verify: a `signal: span` line lands in the SPANS table (and
    /// its rollup), not in the event ring. Asserting `events == 0` alongside
    /// `spans == 1` is the half that matters: a span silently pushed as an
    /// event would still "arrive" and would be invisible to the hop matrix.
    #[tokio::test]
    async fn ingestion_server_accepts_spans_and_they_do_not_land_as_events() {
        use observation::{IngestLine, Span, SpanId, SpanKind, SpanStatus, TraceId};

        let dir = TempDir::new().unwrap();
        let socket_path = dir.path().join("spans.sock");
        let scryer = open_scryer(&dir);

        let bound = IngestionServer::new(Arc::clone(&scryer), &socket_path)
            .bind()
            .await
            .unwrap();
        let server_task = tokio::spawn(async move { let _ = bound.serve().await; });

        let span = Span {
            trace_id: TraceId([7u8; 16]),
            span_id: SpanId([9u8; 8]),
            parent_span_id: None,
            name: "GET /orders".to_string(),
            kind: SpanKind::Server,
            start_unix_nanos: 1_700_000_000_000_000_000,
            duration_nanos: 4_000_000,
            status: SpanStatus::Ok,
            attributes: Default::default(),
        };
        let line = format!(
            "{}\n",
            serde_json::to_string(&IngestLine::span("span.host", span)).unwrap()
        );

        let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        stream.write_all(line.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream);

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        server_task.abort();
        scryer.flush_ring().unwrap();

        assert_eq!(scryer.store().count_spans().unwrap(), 1, "span must reach the spans table");

        let scope = EventScope::Service(MeshIdent("span.host".to_string()));
        let events = scryer.events(&scope, &EventFilter::default()).await.unwrap();
        assert!(events.is_empty(), "a span must not be recorded as a log event");
    }
}
