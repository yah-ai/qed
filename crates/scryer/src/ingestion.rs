//! Unix socket ingestion server for yah-log service-scope events.
//!
//! Warden binds the socket (typically `/run/yah/scryer.sock`), sets
//! `YAH_SCRYER_SOCKET` in every workload env, and runs this server so that
//! workloads using `yah-log` with `YAH_SERVICE_IDENT` set can write structured
//! events into scryer's store with `EventScope::Service(MeshIdent)` scope —
//! without needing a containerd log stream.
//!
//! Wire format is the JSON-line format written by `YahLogServiceLayer`:
//! ```text
//! {"scope_kind":"service","scope_id":"<ident>","level":"...","target":"...","msg":"...","fields":{...},"_lib":"yah-log","_lib_ver":"..."}
//! ```
//!
//! Each accepted connection gets a dedicated tokio task that reads lines until
//! the client closes the connection.  Per-scope seq counters are shared across
//! all connections so a workload restart doesn't reset the seq stream.

use crate::service::Scryer;
use observation::{Event, EventScope, EventSource, Level, TaskRunId};
use serde::Deserialize;
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

// ─── Wire type ────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct IngestLine {
    scope_kind: String,
    scope_id: String,
    level: String,
    target: String,
    msg: String,
    fields: serde_json::Value,
    // _lib / _lib_ver accepted but not forwarded
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

    /// Bind the Unix socket and accept connections until the task is cancelled.
    ///
    /// Suitable for running as a `tokio::spawn`-ed background task in warden.
    pub async fn run(&self) -> Result<(), IngestionError> {
        let listener = UnixListener::bind(&self.socket_path)?;
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
                    let scope = match parsed.scope_kind.as_str() {
                        "service" => EventScope::Service(MeshIdent(parsed.scope_id.clone())),
                        _ => continue,
                    };
                    let scope_key = format!("service:{}", parsed.scope_id);
                    let seq = {
                        let mut c = seq_counters.lock().unwrap();
                        c.next(&scope_key)
                    };
                    let offset_ms =
                        started_at.elapsed().as_millis().min(u32::MAX as u128) as u32;
                    let level = Level::from_str(&parsed.level).unwrap_or(Level::Info);
                    let event = Event {
                        run_id: TaskRunId::new(),
                        seq,
                        offset_ms,
                        level,
                        target: parsed.target,
                        msg: parsed.msg,
                        fields: parsed.fields,
                        anchor: None,
                        source: EventSource::Shim {
                            lib: "yah-log".to_string(),
                            version: "unknown".to_string(),
                        },
                    };
                    let _ = scryer.push(scope, event);
                }
            });
        }
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
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

    /// Verify: events written to the ingestion socket arrive in scryer's store
    /// with `EventScope::Service` scope.
    #[tokio::test]
    async fn ingestion_server_service_scope() {
        let dir = TempDir::new().unwrap();
        let socket_path = dir.path().join("ingest.sock");
        let scryer = open_scryer(&dir);

        let server = Arc::new(IngestionServer::new(Arc::clone(&scryer), &socket_path));
        let server_task = tokio::spawn({
            let server = Arc::clone(&server);
            async move {
                let _ = server.run().await;
            }
        });

        // Give the server time to bind and start accepting.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Client: write one JSON line and close the connection.
        let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        let line = format!(
            "{}\n",
            json!({
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
        let events = scryer.events(&scope, &EventFilter::default()).unwrap();
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

        let server = Arc::new(IngestionServer::new(Arc::clone(&scryer), &socket_path));
        let server_task = tokio::spawn({
            let server = Arc::clone(&server);
            async move { let _ = server.run().await; }
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        // unknown scope kind + one valid service line
        let lines = format!(
            "{}\n{}\n",
            json!({"scope_kind":"future","scope_id":"x","level":"info","target":"t","msg":"m","fields":{}}),
            json!({"scope_kind":"service","scope_id":"svc.host","level":"warn","target":"t","msg":"kept","fields":{}})
        );
        stream.write_all(lines.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream);

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        server_task.abort();

        scryer.flush_ring().unwrap();

        let scope = EventScope::Service(MeshIdent("svc.host".to_string()));
        let events = scryer.events(&scope, &EventFilter::default()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, ObsLevel::Warn);
    }

    /// Verify: per-scope seq is shared across reconnects (no seq reset).
    #[tokio::test]
    async fn ingestion_server_seq_monotonic_across_connections() {
        let dir = TempDir::new().unwrap();
        let socket_path = dir.path().join("seq_mono.sock");
        let scryer = open_scryer(&dir);

        let server = Arc::new(IngestionServer::new(Arc::clone(&scryer), &socket_path));
        let server_task = tokio::spawn({
            let server = Arc::clone(&server);
            async move { let _ = server.run().await; }
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let write_event = |msg: &'static str| {
            let socket_path = socket_path.clone();
            async move {
                let mut s = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
                let line = format!(
                    "{}\n",
                    json!({"scope_kind":"service","scope_id":"seq.host","level":"info","target":"t","msg":msg,"fields":{}})
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
        let events = scryer.events(&scope, &EventFilter::default()).unwrap();
        assert_eq!(events.len(), 2);
        assert!(events[0].seq < events[1].seq, "seq must be monotonically increasing");
    }
}
