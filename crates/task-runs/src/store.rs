//! Per-camp Turso store for TaskRun bytes (Tier 1).
//!
//! One `TaskStore` per daemon instance; callers share it via `Arc<TaskStore>`.
//! Backed by `turso` (in-process, async) per W195 §Engine. Concurrency model:
//! each `TaskStore` method gets a fresh `turso::Connection` from the shared
//! `Database` and drops it at the end of the call. A single `Connection`
//! cannot be used concurrently in turso 0.6.x — the SDK's `ConcurrentGuard`
//! returns `Misuse("concurrent use forbidden")` rather than serializing —
//! so we don't share one across the reader thread, the tail-poll loop, and
//! the GC sweep. `Database::connect()` is cheap (an `Arc` clone plus a
//! per-connection state struct).
//!
//! Storage contract (W195 §3 / Shape 1): this store owns
//! `.yah/db/task-runs.turso` under the camp daemon.

use crate::types::{
    BeholderStatus, ChunkRef, Diagnostic, Event, EventSource, Initiator, KeepRange, Level,
    OutputChunk, RunStatus, SeqRange, Stream, TaskRunId, TaskRunMeta, Triage,
};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use thiserror::Error;
use tokio::sync::Mutex;
use turso::{params, params_from_iter, Builder, Connection, Database, Value};

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("turso: {0}")]
    Sql(#[from] turso::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("run not found: {0}")]
    NotFound(String),
    #[error("invalid stream value: {0}")]
    InvalidStream(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid field path: {0}")]
    InvalidFieldPath(String),
}

// ─── Schema ───────────────────────────────────────────────────────────────────

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS runs (
    id              TEXT PRIMARY KEY,
    command         TEXT NOT NULL,
    cwd             TEXT NOT NULL,
    env_json        TEXT NOT NULL,
    started_at      INTEGER NOT NULL,
    ended_at        INTEGER,
    exit_code       INTEGER,
    signal          INTEGER,
    status          TEXT NOT NULL,
    status_detail   TEXT,
    label           TEXT,
    initiator       TEXT NOT NULL,
    beholder_status TEXT,
    archived_at     INTEGER,
    pinned          INTEGER NOT NULL DEFAULT 0,
    origin          TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
    run_id      TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    offset_ms   INTEGER NOT NULL,
    stream      TEXT NOT NULL,
    bytes       BLOB NOT NULL,
    PRIMARY KEY (run_id, seq)
);
CREATE INDEX IF NOT EXISTS chunks_by_offset ON chunks(run_id, offset_ms);

CREATE TABLE IF NOT EXISTS events (
    run_id      TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    offset_ms   INTEGER NOT NULL,
    level       TEXT NOT NULL,
    target      TEXT NOT NULL,
    msg         TEXT NOT NULL,
    fields_json TEXT NOT NULL,
    anchor_seq  INTEGER,
    source_kind TEXT NOT NULL,
    source_name TEXT NOT NULL,
    PRIMARY KEY (run_id, seq)
);
CREATE INDEX IF NOT EXISTS events_by_target ON events(run_id, target, level);
CREATE INDEX IF NOT EXISTS events_by_offset ON events(run_id, offset_ms);

CREATE TABLE IF NOT EXISTS triages (
    run_id         TEXT PRIMARY KEY,
    synopsis       TEXT NOT NULL,
    keep_json      TEXT NOT NULL,
    primary_lo     INTEGER NOT NULL,
    primary_hi     INTEGER NOT NULL,
    model          TEXT NOT NULL,
    prompt_version INTEGER NOT NULL,
    cached_at      INTEGER NOT NULL,
    partial        INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS _event_field_indexes (
    field_path  TEXT PRIMARY KEY,
    index_name  TEXT NOT NULL,
    created_at  INTEGER NOT NULL
);
"#;

// ─── Filter types ─────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct RunFilter {
    pub since: Option<u64>,
    pub label: Option<String>,
    pub status: Option<String>,
    pub limit: Option<usize>,
    pub archived: Option<bool>,
    /// Filter by provenance tag (e.g. `Some("terminal")` to list only
    /// terminal-session runs). `None` returns runs of every origin.
    pub origin: Option<String>,
}

#[derive(Debug, Default)]
pub struct ChunkFilter {
    pub stream: Option<Stream>,
    pub seq_range: Option<(u32, u32)>,
    pub offset_range: Option<(u32, u32)>,
    pub limit: Option<usize>,
}

// ─── TaskStore ────────────────────────────────────────────────────────────────

struct SeqCounters {
    next_seq: HashMap<String, u32>,
    next_event_seq: HashMap<String, u32>,
}

pub struct TaskStore {
    db: Database,
    seq: Mutex<SeqCounters>,
}

impl TaskStore {
    /// Open (or create) the per-camp task-runs database at `path`.
    pub async fn open(path: &Path) -> Result<Self, StoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Builder::new_local(path.to_string_lossy().as_ref())
            .build()
            .await?;
        let conn = db.connect()?;
        conn.execute_batch(SCHEMA).await?;
        /* `CREATE TABLE IF NOT EXISTS` won't add a column to a runs table that
           predates `origin`, so add it idempotently for already-created DBs.
           A duplicate-column error means an up-to-date schema — swallow it. */
        let _ = conn.execute("ALTER TABLE runs ADD COLUMN origin TEXT", ()).await;
        Ok(TaskStore {
            db,
            seq: Mutex::new(SeqCounters {
                next_seq: HashMap::new(),
                next_event_seq: HashMap::new(),
            }),
        })
    }

    /// Open an in-memory store (tests only).
    #[cfg(test)]
    pub async fn open_in_memory() -> Result<Self, StoreError> {
        let db = Builder::new_local(":memory:").build().await?;
        db.connect()?.execute_batch(SCHEMA).await?;
        Ok(TaskStore {
            db,
            seq: Mutex::new(SeqCounters {
                next_seq: HashMap::new(),
                next_event_seq: HashMap::new(),
            }),
        })
    }

    /// Open a fresh connection to the underlying database. Each call returns
    /// an independent `Connection` that the caller may use within one logical
    /// operation and drop. Never share a `Connection` across awaiting tasks —
    /// `turso` 0.6.x rejects concurrent use on the same handle.
    fn conn(&self) -> Result<Connection, StoreError> {
        Ok(self.db.connect()?)
    }

    pub async fn insert_run(&self, meta: &TaskRunMeta) -> Result<(), StoreError> {
        let (status, ended_at, exit_code, signal, detail) = status_columns(&meta.status);
        let initiator_json = serde_json::to_string(&meta.initiator)?;
        let env_json = serde_json::to_string(&meta.env)?;
        let beholder_json = meta
            .beholder_status
            .as_ref()
            .map(|b| serde_json::to_string(b))
            .transpose()?;
        let cwd = meta.cwd.to_string_lossy().to_string();

        self.conn()?
            .execute(
                "INSERT INTO runs \
                 (id, command, cwd, env_json, started_at, ended_at, exit_code, signal, \
                  status, status_detail, label, initiator, beholder_status, pinned, origin) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
                params![
                    meta.id.to_string(),
                    meta.command.clone(),
                    cwd,
                    env_json,
                    meta.started_at as i64,
                    ended_at,
                    exit_code.map(|c| c as i64),
                    signal.map(|s| s as i64),
                    status.to_string(),
                    detail,
                    meta.label.clone(),
                    initiator_json,
                    beholder_json,
                    meta.pinned as i64,
                    meta.origin.clone(),
                ],
            )
            .await?;

        let mut seq = self.seq.lock().await;
        seq.next_seq.entry(meta.id.to_string()).or_insert(0);
        seq.next_event_seq.entry(meta.id.to_string()).or_insert(0);
        Ok(())
    }

    pub async fn update_beholder_status(
        &self,
        id: &TaskRunId,
        status: &BeholderStatus,
    ) -> Result<(), StoreError> {
        let json = serde_json::to_string(status)?;
        self.conn()?
            .execute(
                "UPDATE runs SET beholder_status = ?1 WHERE id = ?2",
                params![json, id.to_string()],
            )
            .await?;
        Ok(())
    }

    pub async fn update_status(
        &self,
        id: &TaskRunId,
        status: &RunStatus,
    ) -> Result<(), StoreError> {
        let (status_str, ended_at, exit_code, signal, detail) = status_columns(status);
        self.conn()?
            .execute(
                "UPDATE runs SET status=?1, status_detail=?2, ended_at=?3, exit_code=?4, signal=?5 \
                 WHERE id=?6",
                params![
                    status_str.to_string(),
                    detail,
                    ended_at,
                    exit_code.map(|c| c as i64),
                    signal.map(|s| s as i64),
                    id.to_string(),
                ],
            )
            .await?;
        Ok(())
    }

    /// Append an output chunk. Returns the assigned `seq` number.
    pub async fn append_chunk(
        &self,
        run_id: &TaskRunId,
        offset_ms: u32,
        stream: Stream,
        bytes: &[u8],
    ) -> Result<u32, StoreError> {
        let key = run_id.to_string();
        let seq = {
            let mut g = self.seq.lock().await;
            if let Some(s) = g.next_seq.get_mut(&key) {
                let v = *s;
                *s += 1;
                v
            } else {
                drop(g);
                let max = self.max_seq("chunks", &key).await?;
                let next = max.map(|m| m + 1).unwrap_or(0);
                let mut g = self.seq.lock().await;
                g.next_seq.insert(key.clone(), next + 1);
                next
            }
        };

        self.conn()?
            .execute(
                "INSERT INTO chunks (run_id, seq, offset_ms, stream, bytes) VALUES (?1,?2,?3,?4,?5)",
                params![
                    key,
                    seq as i64,
                    offset_ms as i64,
                    stream.as_str().to_string(),
                    bytes.to_vec(),
                ],
            )
            .await?;
        Ok(seq)
    }

    async fn max_seq(&self, table: &str, run_id: &str) -> Result<Option<u32>, StoreError> {
        let sql = format!("SELECT MAX(seq) FROM {table} WHERE run_id = ?1");
        let mut rows = self.conn()?.query(&sql, params![run_id.to_string()]).await?;
        match rows.next().await? {
            Some(row) => {
                let v: Option<i64> = row.get(0)?;
                Ok(v.map(|n| n as u32))
            }
            None => Ok(None),
        }
    }

    pub async fn get_run(&self, id: &TaskRunId) -> Result<Option<TaskRunMeta>, StoreError> {
        let mut rows = self
            .conn()?
            .query(
                "SELECT id, command, cwd, env_json, started_at, ended_at, exit_code, signal, \
                        status, status_detail, label, initiator, beholder_status, pinned, origin \
                 FROM runs WHERE id = ?1",
                params![id.to_string()],
            )
            .await?;
        match rows.next().await? {
            Some(row) => Ok(Some(row_to_meta(&row)?)),
            None => Ok(None),
        }
    }

    pub async fn chunk_count(&self, run_id: &TaskRunId) -> Result<u32, StoreError> {
        let mut rows = self
            .conn()?
            .query(
                "SELECT COUNT(*) FROM chunks WHERE run_id = ?1",
                params![run_id.to_string()],
            )
            .await?;
        let row = rows.next().await?.expect("COUNT(*) always returns one row");
        let n: i64 = row.get(0)?;
        Ok(n as u32)
    }

    pub async fn list_runs(&self, filter: &RunFilter) -> Result<Vec<TaskRunMeta>, StoreError> {
        let limit = filter.limit.unwrap_or(usize::MAX) as i64;
        let since = filter.since.map(|s| s as i64).unwrap_or(0);

        let archived_clause = match filter.archived {
            Some(true) => "AND archived_at IS NOT NULL",
            _ => "AND archived_at IS NULL",
        };

        let mut where_extra = String::new();
        let mut p: Vec<Value> = vec![Value::Integer(since), Value::Integer(limit)];
        // ?1 = since, ?2 = limit, then extra bindings starting at ?3
        let mut next_param = 3;
        if let Some(ref l) = filter.label {
            where_extra.push_str(&format!(" AND label = ?{next_param}"));
            p.push(Value::Text(l.clone()));
            next_param += 1;
        }
        if let Some(ref s) = filter.status {
            where_extra.push_str(&format!(" AND status = ?{next_param}"));
            p.push(Value::Text(s.clone()));
            next_param += 1;
        }
        if let Some(ref o) = filter.origin {
            where_extra.push_str(&format!(" AND origin = ?{next_param}"));
            p.push(Value::Text(o.clone()));
        }

        let sql = format!(
            "SELECT id, command, cwd, env_json, started_at, ended_at, exit_code, signal, \
                    status, status_detail, label, initiator, beholder_status, pinned, origin \
             FROM runs \
             WHERE started_at >= ?1 {} {} \
             ORDER BY started_at DESC \
             LIMIT ?2",
            archived_clause, where_extra
        );

        let mut rows = self.conn()?.query(&sql, params_from_iter(p)).await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(row_to_meta(&row)?);
        }
        Ok(out)
    }

    pub async fn archive_run(&self, id: &TaskRunId) -> Result<(), StoreError> {
        let now = unix_now() as i64;
        let count = self
            .conn()?
            .execute(
                "UPDATE runs SET archived_at = ?1 WHERE id = ?2 AND archived_at IS NULL",
                params![now, id.to_string()],
            )
            .await?;
        if count == 0 {
            return Err(StoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn pin_run(&self, id: &TaskRunId, pinned: bool) -> Result<(), StoreError> {
        let count = self
            .conn()?
            .execute(
                "UPDATE runs SET pinned = ?1 WHERE id = ?2",
                params![pinned as i64, id.to_string()],
            )
            .await?;
        if count == 0 {
            return Err(StoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn gc_sweep(&self, config: &GcConfig) -> Result<GcResult, StoreError> {
        let now = unix_now();
        let warm_cutoff = (now as i64).saturating_sub(config.warm_secs as i64);
        let mut result = GcResult::default();

        result.archived_runs_cleaned =
            self.count_query("SELECT COUNT(*) FROM runs WHERE archived_at IS NOT NULL", vec![])
                .await?;

        result.warm_rolloff_runs = self
            .count_query(
                "SELECT COUNT(*) FROM runs \
                 WHERE archived_at IS NULL AND pinned = 0 AND started_at < ?1",
                vec![Value::Integer(warm_cutoff)],
            )
            .await?;

        result.chunks_deleted += self
            .conn()?
            .execute(
                "DELETE FROM chunks \
                 WHERE run_id IN (SELECT id FROM runs WHERE archived_at IS NOT NULL)",
                (),
            )
            .await?;

        result.events_deleted += self
            .conn()?
            .execute(
                "DELETE FROM events \
                 WHERE run_id IN (SELECT id FROM runs WHERE archived_at IS NOT NULL)",
                (),
            )
            .await?;

        result.chunks_deleted += self
            .conn()?
            .execute(
                "DELETE FROM chunks WHERE run_id IN \
                 (SELECT id FROM runs WHERE archived_at IS NULL AND pinned = 0 AND started_at < ?1)",
                params![warm_cutoff],
            )
            .await?;

        result.events_deleted += self
            .conn()?
            .execute(
                "DELETE FROM events WHERE run_id IN \
                 (SELECT id FROM runs WHERE archived_at IS NULL AND pinned = 0 AND started_at < ?1)",
                params![warm_cutoff],
            )
            .await?;

        Ok(result)
    }

    async fn count_query(&self, sql: &str, params: Vec<Value>) -> Result<u64, StoreError> {
        let mut rows = self.conn()?.query(sql, params_from_iter(params)).await?;
        let row = rows.next().await?.expect("COUNT(*) returns one row");
        let n: i64 = row.get(0)?;
        Ok(n as u64)
    }

    pub async fn get_chunks(
        &self,
        run_id: &TaskRunId,
        filter: &ChunkFilter,
    ) -> Result<Vec<OutputChunk>, StoreError> {
        let key = run_id.to_string();

        let mut conditions = vec!["run_id = ?1".to_string()];
        let mut p: Vec<Value> = vec![Value::Text(key)];
        if let Some(s) = filter.stream {
            conditions.push("stream = ?2".to_string());
            p.push(Value::Text(s.as_str().to_string()));
        }
        if let Some((lo, hi)) = filter.seq_range {
            conditions.push(format!("seq >= {} AND seq <= {}", lo, hi));
        }
        if let Some((from, to)) = filter.offset_range {
            conditions.push(format!("offset_ms >= {} AND offset_ms <= {}", from, to));
        }
        let where_clause = conditions.join(" AND ");
        let limit_clause = filter
            .limit
            .map(|l| format!("LIMIT {l}"))
            .unwrap_or_default();

        let sql = format!(
            "SELECT seq, offset_ms, stream, bytes FROM chunks WHERE {} ORDER BY seq {}",
            where_clause, limit_clause
        );

        let mut rows = self.conn()?.query(&sql, params_from_iter(p)).await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            let seq: i64 = row.get(0)?;
            let offset_ms: i64 = row.get(1)?;
            let stream_str: String = row.get(2)?;
            let bytes: Vec<u8> = row.get(3)?;
            let stream = stream_str
                .parse::<Stream>()
                .map_err(StoreError::InvalidStream)?;
            out.push(OutputChunk {
                run_id: run_id.clone(),
                seq: seq as u32,
                offset_ms: offset_ms as u32,
                stream,
                bytes,
            });
        }
        Ok(out)
    }

    // ─── Events ───────────────────────────────────────────────────────────────

    pub async fn append_event(
        &self,
        run_id: &TaskRunId,
        offset_ms: u32,
        level: Level,
        target: &str,
        msg: &str,
        fields: &serde_json::Value,
        anchor_seq: Option<u32>,
        source: &EventSource,
    ) -> Result<u32, StoreError> {
        let key = run_id.to_string();
        let seq = {
            let mut g = self.seq.lock().await;
            if let Some(s) = g.next_event_seq.get_mut(&key) {
                let v = *s;
                *s += 1;
                v
            } else {
                drop(g);
                let max = self.max_seq("events", &key).await?;
                let next = max.map(|m| m + 1).unwrap_or(0);
                let mut g = self.seq.lock().await;
                g.next_event_seq.insert(key.clone(), next + 1);
                next
            }
        };

        let fields_json = serde_json::to_string(fields)?;

        self.conn()?
            .execute(
                "INSERT INTO events \
                 (run_id, seq, offset_ms, level, target, msg, fields_json, anchor_seq, source_kind, source_name) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    key,
                    seq as i64,
                    offset_ms as i64,
                    level.as_str().to_string(),
                    target.to_string(),
                    msg.to_string(),
                    fields_json,
                    anchor_seq.map(|s| s as i64),
                    source.kind_str().to_string(),
                    source.name_str().to_string(),
                ],
            )
            .await?;
        Ok(seq)
    }

    pub async fn query_events(
        &self,
        run_id: &TaskRunId,
        filter: &EventFilter,
    ) -> Result<Vec<Event>, StoreError> {
        let run_id_str = run_id.to_string();
        let mut conditions = vec!["run_id = ?1".to_string()];
        let mut p: Vec<Value> = vec![Value::Text(run_id_str)];
        let mut next_param = 2usize;

        if let Some(ref target) = filter.target {
            conditions.push(format!("target = ?{next_param}"));
            p.push(Value::Text(target.clone()));
            next_param += 1;
        }

        if let Some(min_level) = filter.min_level {
            let levels: Vec<String> = ALL_LEVELS
                .iter()
                .copied()
                .filter(|&(l, _)| l >= min_level)
                .map(|(_, s)| s.to_string())
                .collect();
            let placeholders: String = (0..levels.len())
                .map(|i| format!("?{}", next_param + i))
                .collect::<Vec<_>>()
                .join(", ");
            conditions.push(format!("level IN ({placeholders})"));
            for level_str in levels {
                p.push(Value::Text(level_str));
                next_param += 1;
            }
        }

        if let Some(ref range) = filter.seq_range {
            conditions.push(format!("seq >= ?{next_param}"));
            p.push(Value::Integer(range.start as i64));
            next_param += 1;
            conditions.push(format!("seq < ?{next_param}"));
            p.push(Value::Integer(range.end as i64));
            next_param += 1;
        }

        if let Some((from_ms, to_ms)) = filter.offset_range {
            conditions.push(format!("offset_ms >= ?{next_param}"));
            p.push(Value::Integer(from_ms as i64));
            next_param += 1;
            conditions.push(format!("offset_ms <= ?{next_param}"));
            p.push(Value::Integer(to_ms as i64));
            next_param += 1;
        }

        if let Some(ref ff) = filter.field_filter {
            validate_field_path(&ff.path)?;
            let escaped = ff.path.replace('\'', "''");
            conditions.push(format!(
                "json_extract(fields_json, '{escaped}') = ?{next_param}"
            ));
            match &ff.value {
                JsonValue::String(s) => p.push(Value::Text(s.clone())),
                JsonValue::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        p.push(Value::Integer(i));
                    } else if let Some(f) = n.as_f64() {
                        p.push(Value::Real(f));
                    } else {
                        p.push(Value::Text(n.to_string()));
                    }
                }
                JsonValue::Bool(b) => p.push(Value::Integer(*b as i64)),
                JsonValue::Null => p.push(Value::Null),
                other => p.push(Value::Text(other.to_string())),
            }
            let _ = next_param;
        }

        let where_clause = conditions.join(" AND ");
        let limit_clause = filter.limit.map(|n| format!("LIMIT {n}")).unwrap_or_default();
        let sql = format!(
            "SELECT run_id, seq, offset_ms, level, target, msg, fields_json, anchor_seq, \
             source_kind, source_name \
             FROM events WHERE {where_clause} ORDER BY seq {limit_clause}"
        );

        let mut rows = self.conn()?.query(&sql, params_from_iter(p)).await?;
        let mut events = Vec::new();
        while let Some(row) = rows.next().await? {
            events.push(row_to_event(&row)?);
        }
        Ok(events)
    }

    pub async fn event_count(
        &self,
        run_id: &TaskRunId,
        min_level: Option<Level>,
    ) -> Result<u64, StoreError> {
        let run_id_str = run_id.to_string();
        if let Some(min_level) = min_level {
            let levels: Vec<String> = ALL_LEVELS
                .iter()
                .copied()
                .filter(|&(l, _)| l >= min_level)
                .map(|(_, s)| s.to_string())
                .collect();
            let placeholders: String = levels
                .iter()
                .enumerate()
                .map(|(i, _)| format!("?{}", i + 2))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT COUNT(*) FROM events WHERE run_id = ?1 AND level IN ({placeholders})"
            );
            let mut p: Vec<Value> = vec![Value::Text(run_id_str)];
            for s in levels {
                p.push(Value::Text(s));
            }
            self.count_query(&sql, p).await
        } else {
            self.count_query(
                "SELECT COUNT(*) FROM events WHERE run_id = ?1",
                vec![Value::Text(run_id_str)],
            )
            .await
        }
    }

    pub async fn query_diagnostics(
        &self,
        run_id: &TaskRunId,
    ) -> Result<Vec<Diagnostic>, StoreError> {
        let events = self
            .query_events(
                run_id,
                &EventFilter {
                    min_level: Some(Level::Warn),
                    ..EventFilter::default()
                },
            )
            .await?;
        Ok(events.into_iter().map(event_to_diagnostic).collect())
    }

    // ─── json_extract hooks ───────────────────────────────────────────────────

    pub async fn ensure_field_index(&self, field_path: &str) -> Result<(), StoreError> {
        validate_field_path(field_path)?;

        let exists: bool = {
            let mut rows = self
                .conn()?
                .query(
                    "SELECT 1 FROM _event_field_indexes WHERE field_path = ?1",
                    params![field_path.to_string()],
                )
                .await?;
            rows.next().await?.is_some()
        };

        if exists {
            return Ok(());
        }

        let index_name = field_path_to_index_name(field_path);
        let escaped = field_path.replace('\'', "''");
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {index_name} \
             ON events(run_id, json_extract(fields_json, '{escaped}')) \
             WHERE json_extract(fields_json, '{escaped}') IS NOT NULL"
        );
        self.conn()?.execute_batch(&sql).await?;

        let now = unix_now();
        self.conn()?
            .execute(
                "INSERT OR IGNORE INTO _event_field_indexes (field_path, index_name, created_at) \
                 VALUES (?1, ?2, ?3)",
                params![field_path.to_string(), index_name, now as i64],
            )
            .await?;

        Ok(())
    }

    pub async fn timeline_ticks(
        &self,
        run_id: &TaskRunId,
        since_offset_ms: Option<u32>,
        limit: Option<usize>,
    ) -> Result<Vec<TimelineTick>, StoreError> {
        let since = since_offset_ms.unwrap_or(0);
        let cap = limit.unwrap_or(usize::MAX);

        let mut chunks = self
            .get_chunks(
                run_id,
                &ChunkFilter {
                    offset_range: Some((since, u32::MAX)),
                    ..Default::default()
                },
            )
            .await?;
        let mut events = self
            .query_events(
                run_id,
                &EventFilter {
                    offset_range: Some((since, u32::MAX)),
                    ..Default::default()
                },
            )
            .await?;

        chunks.sort_by_key(|c| c.offset_ms);
        events.sort_by_key(|e| e.offset_ms);

        let mut ticks: Vec<TimelineTick> =
            Vec::with_capacity((chunks.len() + events.len()).min(cap));
        let mut ci = 0usize;
        let mut ei = 0usize;

        while ticks.len() < cap && (ci < chunks.len() || ei < events.len()) {
            match (chunks.get(ci), events.get(ei)) {
                (Some(c), Some(e)) if c.offset_ms <= e.offset_ms => {
                    ticks.push(TimelineTick::Chunk(chunks[ci].clone()));
                    ci += 1;
                    let _ = e;
                }
                (Some(_), Some(_)) => {
                    ticks.push(TimelineTick::Event(events[ei].clone()));
                    ei += 1;
                }
                (Some(_), None) => {
                    ticks.push(TimelineTick::Chunk(chunks[ci].clone()));
                    ci += 1;
                }
                (None, Some(_)) => {
                    ticks.push(TimelineTick::Event(events[ei].clone()));
                    ei += 1;
                }
                (None, None) => break,
            }
        }

        Ok(ticks)
    }

    pub async fn aggregate_events(
        &self,
        filter: &AggregateFilter,
    ) -> Result<Vec<AggregateBucket>, StoreError> {
        let limit = filter.limit.unwrap_or(100) as i64;
        let since = filter.since.map(|s| s as i64).unwrap_or(0);
        let group_by = filter.group_by.unwrap_or(AggregateGroupBy::Target);

        let key_expr = match group_by {
            AggregateGroupBy::Target => "e.target".to_string(),
            AggregateGroupBy::Level => "e.level".to_string(),
            AggregateGroupBy::ErrorCode => {
                "json_extract(e.fields_json, '$.error.code')".to_string()
            }
        };

        let mut where_parts = vec!["r.started_at >= ?1".to_string()];
        let mut p: Vec<Value> = vec![Value::Integer(since)];
        let mut next_param = 2usize;

        if let Some(ref label) = filter.label {
            where_parts.push(format!("r.label = ?{next_param}"));
            p.push(Value::Text(label.clone()));
            next_param += 1;
        }

        if matches!(group_by, AggregateGroupBy::ErrorCode) {
            where_parts.push("json_extract(e.fields_json, '$.error.code') IS NOT NULL".to_string());
        }

        if let Some(ref ff) = filter.field_filter {
            validate_field_path(&ff.path)?;
            let escaped = ff.path.replace('\'', "''");
            where_parts.push(format!(
                "json_extract(e.fields_json, '{escaped}') = ?{next_param}"
            ));
            match &ff.value {
                JsonValue::String(s) => p.push(Value::Text(s.clone())),
                JsonValue::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        p.push(Value::Integer(i));
                    } else if let Some(f) = n.as_f64() {
                        p.push(Value::Real(f));
                    } else {
                        p.push(Value::Text(n.to_string()));
                    }
                }
                JsonValue::Bool(b) => p.push(Value::Integer(*b as i64)),
                JsonValue::Null => p.push(Value::Null),
                other => p.push(Value::Text(other.to_string())),
            }
            let _ = next_param;
        }

        let where_clause = where_parts.join(" AND ");
        let limit_param = p.len() + 1;
        let sql = format!(
            "SELECT {key_expr} as key, COUNT(*) as cnt \
             FROM events e \
             JOIN runs r ON r.id = e.run_id \
             WHERE {where_clause} \
             GROUP BY {key_expr} \
             ORDER BY cnt DESC \
             LIMIT ?{limit_param}"
        );
        p.push(Value::Integer(limit));

        let mut rows = self.conn()?.query(&sql, params_from_iter(p)).await?;
        let mut buckets = Vec::new();
        while let Some(row) = rows.next().await? {
            let key: Option<String> = row.get(0)?;
            let count: i64 = row.get(1)?;
            buckets.push(AggregateBucket {
                key: key.unwrap_or_default(),
                count: count as u64,
            });
        }
        Ok(buckets)
    }

    // ── Tier 1.75 — triage ───────────────────────────────────────────────────

    pub async fn upsert_triage(&self, triage: &Triage) -> Result<(), StoreError> {
        let keep_json = serde_json::to_string(&triage.keep)?;
        self.conn()?
            .execute(
                "INSERT OR REPLACE INTO triages \
                 (run_id, synopsis, keep_json, primary_lo, primary_hi, \
                  model, prompt_version, cached_at, partial) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    triage.run_id.to_string(),
                    triage.synopsis.clone(),
                    keep_json,
                    triage.primary.lo as i64,
                    triage.primary.hi as i64,
                    triage.model.clone(),
                    triage.prompt_version as i64,
                    triage.cached_at as i64,
                    triage.partial as i64,
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn get_triage(&self, run_id: &TaskRunId) -> Result<Option<Triage>, StoreError> {
        let mut rows = self
            .conn()?
            .query(
                "SELECT run_id, synopsis, keep_json, primary_lo, primary_hi, \
                 model, prompt_version, cached_at, partial \
                 FROM triages WHERE run_id = ?1",
                params![run_id.to_string()],
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let id: String = row.get(0)?;
        let synopsis: String = row.get(1)?;
        let keep_json: String = row.get(2)?;
        let primary_lo: i64 = row.get(3)?;
        let primary_hi: i64 = row.get(4)?;
        let model: String = row.get(5)?;
        let prompt_version: i64 = row.get(6)?;
        let cached_at: i64 = row.get(7)?;
        let partial: i64 = row.get(8)?;
        let keep: Vec<KeepRange> = serde_json::from_str(&keep_json)?;
        Ok(Some(Triage {
            run_id: id.parse().map_err(|_| StoreError::NotFound(id.clone()))?,
            synopsis,
            keep,
            primary: SeqRange {
                lo: primary_lo as u32,
                hi: primary_hi as u32,
            },
            model,
            prompt_version: prompt_version as u32,
            cached_at: cached_at as u64,
            partial: partial != 0,
        }))
    }

    pub async fn list_field_indexes(&self) -> Result<Vec<FieldIndexInfo>, StoreError> {
        let mut rows = self
            .conn()?
            .query(
                "SELECT field_path, index_name, created_at \
                 FROM _event_field_indexes ORDER BY created_at",
                (),
            )
            .await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            let field_path: String = row.get(0)?;
            let index_name: String = row.get(1)?;
            let created_at: i64 = row.get(2)?;
            out.push(FieldIndexInfo {
                field_path,
                index_name,
                created_at: created_at as u64,
            });
        }
        Ok(out)
    }
}

// ─── Timeline + aggregate ─────────────────────────────────────────────────────

pub enum TimelineTick {
    Chunk(OutputChunk),
    Event(Event),
}

impl TimelineTick {
    pub fn offset_ms(&self) -> u32 {
        match self {
            TimelineTick::Chunk(c) => c.offset_ms,
            TimelineTick::Event(e) => e.offset_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateGroupBy {
    Target,
    Level,
    ErrorCode,
}

#[derive(Debug, Default)]
pub struct AggregateFilter {
    pub label: Option<String>,
    pub since: Option<u64>,
    pub group_by: Option<AggregateGroupBy>,
    pub field_filter: Option<FieldFilter>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct AggregateBucket {
    pub key: String,
    pub count: u64,
}

// ─── Event filter types ───────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct EventFilter {
    pub target: Option<String>,
    pub min_level: Option<Level>,
    pub seq_range: Option<std::ops::Range<u32>>,
    pub offset_range: Option<(u32, u32)>,
    pub field_filter: Option<FieldFilter>,
    pub limit: Option<u32>,
}

#[derive(Debug)]
pub struct FieldFilter {
    pub path: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct FieldIndexInfo {
    pub field_path: String,
    pub index_name: String,
    pub created_at: u64,
}

// ─── GC types ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct GcConfig {
    pub warm_secs: u64,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            warm_secs: 30 * 24 * 3600,
        }
    }
}

#[derive(Debug, Default)]
pub struct GcResult {
    pub archived_runs_cleaned: u64,
    pub warm_rolloff_runs: u64,
    pub chunks_deleted: u64,
    pub events_deleted: u64,
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

const ALL_LEVELS: &[(Level, &str)] = &[
    (Level::Trace, "trace"),
    (Level::Debug, "debug"),
    (Level::Info, "info"),
    (Level::Warn, "warn"),
    (Level::Error, "error"),
    (Level::Fatal, "fatal"),
];

pub fn validate_field_path(path: &str) -> Result<(), StoreError> {
    if path.is_empty() || !path.starts_with('$') {
        return Err(StoreError::InvalidFieldPath(path.to_string()));
    }
    let valid = path[1..]
        .chars()
        .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '[' | ']'));
    if !valid {
        return Err(StoreError::InvalidFieldPath(path.to_string()));
    }
    Ok(())
}

fn field_path_to_index_name(path: &str) -> String {
    let ident: String = path
        .chars()
        .skip(1)
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect::<String>()
        .trim_matches('_')
        .to_string();
    format!("events_field_{ident}")
}

fn row_to_event(r: &turso::Row) -> Result<Event, StoreError> {
    let run_id_str: String = r.get(0)?;
    let run_id = run_id_str
        .parse::<TaskRunId>()
        .map_err(|_| StoreError::NotFound(run_id_str.clone()))?;

    let level_str: String = r.get(3)?;
    let level = Level::from_str(&level_str).map_err(StoreError::InvalidFieldPath)?;

    let fields_json: String = r.get(6)?;
    let fields: JsonValue = serde_json::from_str(&fields_json)?;

    let anchor_seq: Option<i64> = r.get(7)?;
    let anchor = anchor_seq.map(|s| ChunkRef { seq: s as u32 });

    let source_kind: String = r.get(8)?;
    let source_name: String = r.get(9)?;
    let source = match source_kind.as_str() {
        "beholder" => EventSource::Beholder {
            name: source_name,
            version: "unknown".to_string(),
        },
        "shim" => EventSource::Shim {
            lib: source_name,
            version: "unknown".to_string(),
        },
        _ => EventSource::Synth,
    };

    let seq: i64 = r.get(1)?;
    let offset_ms: i64 = r.get(2)?;
    let target: String = r.get(4)?;
    let msg: String = r.get(5)?;

    Ok(Event {
        run_id,
        seq: seq as u32,
        offset_ms: offset_ms as u32,
        level,
        target,
        msg,
        fields,
        anchor,
        source,
    })
}

fn event_to_diagnostic(e: Event) -> Diagnostic {
    let file = e
        .fields
        .get("file")
        .and_then(|f| f.get("path"))
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let line = e
        .fields
        .get("file")
        .and_then(|f| f.get("line"))
        .and_then(|v| v.as_u64())
        .map(|n| n as u32);
    let col = e
        .fields
        .get("file")
        .and_then(|f| f.get("col"))
        .and_then(|v| v.as_u64())
        .map(|n| n as u32);
    let code = e
        .fields
        .get("error")
        .and_then(|f| f.get("code"))
        .and_then(|v| v.as_str())
        .map(str::to_owned);

    Diagnostic {
        severity: e.level,
        file,
        line,
        col,
        code,
        message: e.msg,
        source: format!("{}/{}", e.source.kind_str(), e.source.name_str()),
        run_id: e.run_id,
        event_seq: e.seq,
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn status_columns(
    status: &RunStatus,
) -> (&'static str, Option<i64>, Option<i32>, Option<i32>, Option<String>) {
    match status {
        RunStatus::Pending => ("pending", None, None, None, None),
        RunStatus::Running => ("running", None, None, None, None),
        RunStatus::Done { exit_code, ended_at } => {
            ("done", Some(*ended_at as i64), Some(*exit_code), None, None)
        }
        RunStatus::Killed { signal, ended_at } => {
            ("killed", Some(*ended_at as i64), None, Some(*signal), None)
        }
        RunStatus::Lost { reason } => ("lost", None, None, None, Some(reason.clone())),
    }
}

fn row_to_meta(row: &turso::Row) -> Result<TaskRunMeta, StoreError> {
    let id_str: String = row.get(0)?;
    let command: String = row.get(1)?;
    let cwd: String = row.get(2)?;
    let env_json: String = row.get(3)?;
    let started_at: i64 = row.get(4)?;
    let ended_at: Option<i64> = row.get(5)?;
    let exit_code: Option<i64> = row.get(6)?;
    let signal: Option<i64> = row.get(7)?;
    let status_str: String = row.get(8)?;
    let status_detail: Option<String> = row.get(9)?;
    let label: Option<String> = row.get(10)?;
    let initiator_json: String = row.get(11)?;
    let beholder_json: Option<String> = row.get(12)?;
    let pinned: i64 = row.get(13).unwrap_or(0);
    let origin: Option<String> = row.get(14).ok().flatten();

    let status = reconstruct_status(
        &status_str,
        exit_code.map(|c| c as i32),
        signal.map(|s| s as i32),
        ended_at,
        status_detail,
    );
    let initiator: Initiator = serde_json::from_str(&initiator_json)?;
    let env: Vec<(String, String)> = serde_json::from_str(&env_json)?;
    let beholder_status: Option<BeholderStatus> = beholder_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?;

    Ok(TaskRunMeta {
        id: id_str
            .parse()
            .map_err(|e: uuid::Error| StoreError::NotFound(e.to_string()))?,
        command,
        cwd: cwd.into(),
        env,
        started_at: started_at as u64,
        status,
        label,
        initiator,
        beholder_status,
        pinned: pinned != 0,
        origin,
    })
}

fn reconstruct_status(
    s: &str,
    exit_code: Option<i32>,
    signal: Option<i32>,
    ended_at: Option<i64>,
    detail: Option<String>,
) -> RunStatus {
    let ended_at = ended_at.unwrap_or(0) as u64;
    match s {
        "pending" => RunStatus::Pending,
        "running" => RunStatus::Running,
        "done" => RunStatus::Done {
            exit_code: exit_code.unwrap_or(0),
            ended_at,
        },
        "killed" => RunStatus::Killed {
            signal: signal.unwrap_or(15),
            ended_at,
        },
        "lost" => RunStatus::Lost {
            reason: detail.unwrap_or_default(),
        },
        other => RunStatus::Lost {
            reason: format!("unknown status in db: {other}"),
        },
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Initiator, RunStatus, Stream, TaskRunId, TaskRunMeta};
    use std::path::PathBuf;

    fn make_meta(id: TaskRunId, cmd: &str) -> TaskRunMeta {
        TaskRunMeta {
            id,
            command: cmd.to_string(),
            cwd: PathBuf::from("/tmp"),
            env: vec![],
            started_at: 1_000_000,
            status: RunStatus::Running,
            label: None,
            initiator: Initiator::Human {
                camp: "local".to_string(),
            },
            beholder_status: None,
            pinned: false,
            origin: None,
        }
    }

    #[tokio::test]
    async fn open_creates_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("task-runs.turso");
        let store = TaskStore::open(&path).await.unwrap();
        assert!(path.exists());
        let n = store
            .count_query("SELECT COUNT(*) FROM runs", vec![])
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn insert_and_get_run() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        let meta = make_meta(id.clone(), "cargo check");
        store.insert_run(&meta).await.unwrap();
        let got = store.get_run(&id).await.unwrap().unwrap();
        assert_eq!(got.command, "cargo check");
        assert!(matches!(got.status, RunStatus::Running));
    }

    #[tokio::test]
    async fn append_chunks_monotonic_seq() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "echo hi")).await.unwrap();

        let s0 = store.append_chunk(&id, 0, Stream::Stdout, b"hello\n").await.unwrap();
        let s1 = store.append_chunk(&id, 5, Stream::Stdout, b"world\n").await.unwrap();
        let s2 = store.append_chunk(&id, 10, Stream::Stderr, b"err\n").await.unwrap();

        assert_eq!(s0, 0);
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        assert_eq!(store.chunk_count(&id).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn seq_resumes_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tr.turso");
        let id = TaskRunId::new();

        {
            let store = TaskStore::open(&path).await.unwrap();
            store.insert_run(&make_meta(id.clone(), "cmd")).await.unwrap();
            store.append_chunk(&id, 0, Stream::Stdout, b"a").await.unwrap();
            store.append_chunk(&id, 1, Stream::Stdout, b"b").await.unwrap();
        }

        let store = TaskStore::open(&path).await.unwrap();
        let seq = store.append_chunk(&id, 2, Stream::Stdout, b"c").await.unwrap();
        assert_eq!(seq, 2);
        assert_eq!(store.chunk_count(&id).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn chunk_filter_by_stream() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cmd")).await.unwrap();
        store.append_chunk(&id, 0, Stream::Stdout, b"out").await.unwrap();
        store.append_chunk(&id, 1, Stream::Stderr, b"err").await.unwrap();
        store.append_chunk(&id, 2, Stream::Stdout, b"out2").await.unwrap();

        let chunks = store
            .get_chunks(
                &id,
                &ChunkFilter {
                    stream: Some(Stream::Stdout),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(chunks.len(), 2);
        assert!(chunks.iter().all(|c| c.stream == Stream::Stdout));
    }

    #[tokio::test]
    async fn update_status_to_done() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cmd")).await.unwrap();
        store
            .update_status(
                &id,
                &RunStatus::Done {
                    exit_code: 0,
                    ended_at: 2_000_000,
                },
            )
            .await
            .unwrap();
        let got = store.get_run(&id).await.unwrap().unwrap();
        match got.status {
            RunStatus::Done { exit_code, .. } => assert_eq!(exit_code, 0),
            other => panic!("unexpected status: {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_runs_filter_status() {
        let store = TaskStore::open_in_memory().await.unwrap();

        for cmd in ["a", "b", "c"] {
            let id = TaskRunId::new();
            store.insert_run(&make_meta(id.clone(), cmd)).await.unwrap();
            if cmd == "b" {
                store
                    .update_status(
                        &id,
                        &RunStatus::Done {
                            exit_code: 0,
                            ended_at: 1_000_001,
                        },
                    )
                    .await
                    .unwrap();
            }
        }

        let running = store
            .list_runs(&RunFilter {
                status: Some("running".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(running.len(), 2);
        let done = store
            .list_runs(&RunFilter {
                status: Some("done".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(done.len(), 1);
    }

    // ─── Event tests ──────────────────────────────────────────────────────────

    async fn append_evt(
        store: &TaskStore,
        run_id: &TaskRunId,
        level: Level,
        target: &str,
        msg: &str,
        fields: serde_json::Value,
    ) -> u32 {
        store
            .append_event(
                run_id,
                0,
                level,
                target,
                msg,
                &fields,
                None,
                &EventSource::Beholder {
                    name: "cargo".to_string(),
                    version: "1.78".to_string(),
                },
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn events_schema_has_structural_indexes() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let n = store
            .count_query(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' \
                 AND name IN ('events_by_target', 'events_by_offset')",
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(n, 2, "both structural events indexes must exist after open");
    }

    #[tokio::test]
    async fn append_and_query_events() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cargo check")).await.unwrap();

        append_evt(&store, &id, Level::Info, "cargo::rustc", "compiling", serde_json::json!({})).await;
        append_evt(
            &store,
            &id,
            Level::Warn,
            "cargo::rustc",
            "unused import",
            serde_json::json!({"file": {"path": "src/lib.rs", "line": 10}}),
        )
        .await;
        append_evt(
            &store,
            &id,
            Level::Error,
            "cargo::rustc",
            "type mismatch",
            serde_json::json!({"error": {"code": "E0308"}, "file": {"path": "src/main.rs", "line": 42}}),
        )
        .await;

        let all = store.query_events(&id, &EventFilter::default()).await.unwrap();
        assert_eq!(all.len(), 3);

        let errors = store
            .query_events(
                &id,
                &EventFilter {
                    min_level: Some(Level::Error),
                    ..EventFilter::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].msg, "type mismatch");

        assert_eq!(store.event_count(&id, None).await.unwrap(), 3);
        assert_eq!(store.event_count(&id, Some(Level::Warn)).await.unwrap(), 2);
        assert_eq!(store.event_count(&id, Some(Level::Error)).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn query_diagnostics_maps_reserved_fields() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cargo check")).await.unwrap();

        append_evt(
            &store,
            &id,
            Level::Error,
            "cargo::rustc",
            "type mismatch",
            serde_json::json!({
                "error": {"code": "E0308"},
                "file": {"path": "src/main.rs", "line": 42, "col": 5}
            }),
        )
        .await;

        let diags = store.query_diagnostics(&id).await.unwrap();
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].file.as_deref(), Some("src/main.rs"));
        assert_eq!(diags[0].line, Some(42));
        assert_eq!(diags[0].col, Some(5));
        assert_eq!(diags[0].code.as_deref(), Some("E0308"));
        assert_eq!(diags[0].severity, Level::Error);
    }

    #[test]
    fn field_path_validation_rules() {
        assert!(validate_field_path("$.error.code").is_ok());
        assert!(validate_field_path("$.file.path").is_ok());
        assert!(validate_field_path("$.test_name").is_ok());
        assert!(validate_field_path("$.items[0]").is_ok());

        assert!(validate_field_path("").is_err());
        assert!(validate_field_path("error.code").is_err());
        assert!(validate_field_path("$.'injection").is_err());
        assert!(validate_field_path("$.error code").is_err());
        assert!(validate_field_path("$.a;b").is_err());
    }

    #[tokio::test]
    async fn ensure_field_index_idempotent() {
        let store = TaskStore::open_in_memory().await.unwrap();

        store.ensure_field_index("$.error.code").await.unwrap();
        store.ensure_field_index("$.error.code").await.unwrap();

        let indexes = store.list_field_indexes().await.unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].field_path, "$.error.code");
        assert!(indexes[0].index_name.starts_with("events_field_"));
    }

    #[tokio::test]
    async fn field_index_accelerates_field_filter_query() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cargo check")).await.unwrap();

        for i in 0u32..20 {
            append_evt(
                &store,
                &id,
                Level::Error,
                "t",
                "e",
                serde_json::json!({"error": {"code": if i % 3 == 0 { "E0308" } else { "E0001" }}}),
            )
            .await;
        }

        store.ensure_field_index("$.error.code").await.unwrap();

        let results = store
            .query_events(
                &id,
                &EventFilter {
                    field_filter: Some(FieldFilter {
                        path: "$.error.code".to_string(),
                        value: serde_json::json!("E0308"),
                    }),
                    ..EventFilter::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 7);
    }

    #[tokio::test]
    async fn timeline_interleaves_chunks_and_events_by_offset() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cmd")).await.unwrap();

        store.append_chunk(&id, 10, Stream::Stdout, b"first\n").await.unwrap();
        append_evt(&store, &id, Level::Info, "t", "ev1", serde_json::json!({})).await;
        store
            .append_event(
                &id,
                20,
                Level::Warn,
                "t",
                "ev2",
                &serde_json::json!({}),
                None,
                &EventSource::Beholder {
                    name: "test".into(),
                    version: "0".into(),
                },
            )
            .await
            .unwrap();
        store.append_chunk(&id, 30, Stream::Stdout, b"last\n").await.unwrap();

        let ticks = store.timeline_ticks(&id, None, None).await.unwrap();
        assert_eq!(ticks.len(), 4);
        assert!(matches!(ticks[0], TimelineTick::Event(_)));
        assert!(matches!(ticks[1], TimelineTick::Chunk(_)));
        assert!(matches!(ticks[2], TimelineTick::Event(_)));
        assert!(matches!(ticks[3], TimelineTick::Chunk(_)));
    }

    #[tokio::test]
    async fn timeline_since_offset_filters_correctly() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cmd")).await.unwrap();
        store.append_chunk(&id, 5, Stream::Stdout, b"a").await.unwrap();
        store.append_chunk(&id, 15, Stream::Stdout, b"b").await.unwrap();

        let ticks = store.timeline_ticks(&id, Some(10), None).await.unwrap();
        assert_eq!(ticks.len(), 1);
        if let TimelineTick::Chunk(c) = &ticks[0] {
            assert_eq!(c.offset_ms, 15);
        } else {
            panic!("expected Chunk");
        }
    }

    #[tokio::test]
    async fn aggregate_events_group_by_target() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cmd")).await.unwrap();

        for _ in 0..3 {
            append_evt(&store, &id, Level::Warn, "cargo::rustc", "w", serde_json::json!({})).await;
        }
        for _ in 0..2 {
            append_evt(&store, &id, Level::Error, "tsc", "e", serde_json::json!({})).await;
        }

        let buckets = store
            .aggregate_events(&AggregateFilter {
                group_by: Some(AggregateGroupBy::Target),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].key, "cargo::rustc");
        assert_eq!(buckets[0].count, 3);
        assert_eq!(buckets[1].key, "tsc");
        assert_eq!(buckets[1].count, 2);
    }

    #[tokio::test]
    async fn aggregate_events_group_by_error_code() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cargo check")).await.unwrap();

        append_evt(
            &store,
            &id,
            Level::Error,
            "cargo::rustc",
            "e0308",
            serde_json::json!({"error": {"code": "E0308"}}),
        )
        .await;
        append_evt(
            &store,
            &id,
            Level::Error,
            "cargo::rustc",
            "e0308 again",
            serde_json::json!({"error": {"code": "E0308"}}),
        )
        .await;
        append_evt(
            &store,
            &id,
            Level::Error,
            "cargo::rustc",
            "e0001",
            serde_json::json!({"error": {"code": "E0001"}}),
        )
        .await;
        append_evt(&store, &id, Level::Info, "cargo", "no code", serde_json::json!({})).await;

        let buckets = store
            .aggregate_events(&AggregateFilter {
                group_by: Some(AggregateGroupBy::ErrorCode),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(buckets.len(), 2, "only events with error.code are counted");
        assert_eq!(buckets[0].key, "E0308");
        assert_eq!(buckets[0].count, 2);
        assert_eq!(buckets[1].key, "E0001");
        assert_eq!(buckets[1].count, 1);
    }

    #[tokio::test]
    async fn event_filter_offset_range() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cmd")).await.unwrap();

        store
            .append_event(
                &id,
                5,
                Level::Info,
                "t",
                "early",
                &serde_json::json!({}),
                None,
                &EventSource::Beholder {
                    name: "x".into(),
                    version: "0".into(),
                },
            )
            .await
            .unwrap();
        store
            .append_event(
                &id,
                50,
                Level::Warn,
                "t",
                "mid",
                &serde_json::json!({}),
                None,
                &EventSource::Beholder {
                    name: "x".into(),
                    version: "0".into(),
                },
            )
            .await
            .unwrap();
        store
            .append_event(
                &id,
                100,
                Level::Error,
                "t",
                "late",
                &serde_json::json!({}),
                None,
                &EventSource::Beholder {
                    name: "x".into(),
                    version: "0".into(),
                },
            )
            .await
            .unwrap();

        let events = store
            .query_events(
                &id,
                &EventFilter {
                    offset_range: Some((10, 60)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].msg, "mid");
    }

    #[tokio::test]
    async fn upsert_and_get_triage_round_trips() {
        use crate::types::{KeepRange, SeqRange, Triage};
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cargo check")).await.unwrap();

        let triage = Triage {
            run_id: id.clone(),
            synopsis: "Build failed: missing semicolon on line 42.".into(),
            keep: vec![KeepRange {
                range: SeqRange { lo: 10, hi: 15 },
                reason: "primary error".into(),
            }],
            primary: SeqRange { lo: 10, hi: 15 },
            model: "claude-haiku-4-5".into(),
            prompt_version: 1,
            cached_at: 1_700_000_000,
            partial: false,
        };

        store.upsert_triage(&triage).await.unwrap();
        let got = store.get_triage(&id).await.unwrap().expect("triage should exist");

        assert_eq!(got.run_id.to_string(), id.to_string());
        assert_eq!(got.synopsis, triage.synopsis);
        assert_eq!(got.keep.len(), 1);
        assert_eq!(got.keep[0].range.lo, 10);
        assert_eq!(got.keep[0].range.hi, 15);
        assert_eq!(got.keep[0].reason, "primary error");
        assert_eq!(got.primary.lo, 10);
        assert_eq!(got.primary.hi, 15);
        assert_eq!(got.model, "claude-haiku-4-5");
        assert_eq!(got.prompt_version, 1);
        assert_eq!(got.cached_at, 1_700_000_000);
        assert!(!got.partial);
    }

    #[tokio::test]
    async fn get_triage_returns_none_when_absent() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        assert!(store.get_triage(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn upsert_triage_replace_on_conflict() {
        use crate::types::{KeepRange, SeqRange, Triage};
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cmd")).await.unwrap();

        let t1 = Triage {
            run_id: id.clone(),
            synopsis: "first".into(),
            keep: vec![],
            primary: SeqRange { lo: 0, hi: 0 },
            model: "haiku".into(),
            prompt_version: 1,
            cached_at: 100,
            partial: false,
        };
        let t2 = Triage {
            run_id: id.clone(),
            synopsis: "second".into(),
            keep: vec![KeepRange {
                range: SeqRange { lo: 5, hi: 9 },
                reason: "r".into(),
            }],
            primary: SeqRange { lo: 5, hi: 9 },
            model: "ollama:qwen".into(),
            prompt_version: 2,
            cached_at: 200,
            partial: true,
        };
        store.upsert_triage(&t1).await.unwrap();
        store.upsert_triage(&t2).await.unwrap();

        let got = store.get_triage(&id).await.unwrap().unwrap();
        assert_eq!(got.synopsis, "second");
        assert_eq!(got.prompt_version, 2);
        assert!(got.partial);
    }

    #[tokio::test]
    async fn event_seq_resumes_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tr.turso");
        let id = TaskRunId::new();

        {
            let store = TaskStore::open(&path).await.unwrap();
            store.insert_run(&make_meta(id.clone(), "cmd")).await.unwrap();
            append_evt(&store, &id, Level::Info, "t", "a", serde_json::json!({})).await;
            append_evt(&store, &id, Level::Info, "t", "b", serde_json::json!({})).await;
        }

        let store = TaskStore::open(&path).await.unwrap();
        let seq = append_evt(&store, &id, Level::Info, "t", "c", serde_json::json!({})).await;
        assert_eq!(seq, 2);
        assert_eq!(store.event_count(&id, None).await.unwrap(), 3);
    }

    // ─── GC sweep tests ───────────────────────────────────────────────────────

    fn make_meta_with_age(id: TaskRunId, cmd: &str, age_secs: u64) -> TaskRunMeta {
        let now = unix_now();
        TaskRunMeta {
            id,
            command: cmd.to_string(),
            cwd: PathBuf::from("/tmp"),
            env: vec![],
            started_at: now.saturating_sub(age_secs),
            status: RunStatus::Running,
            label: None,
            initiator: Initiator::Human {
                camp: "local".to_string(),
            },
            beholder_status: None,
            pinned: false,
            origin: None,
        }
    }

    #[tokio::test]
    async fn gc_sweep_drops_output_for_archived_runs() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cmd")).await.unwrap();
        store.append_chunk(&id, 0, Stream::Stdout, b"data").await.unwrap();
        store.archive_run(&id).await.unwrap();

        let result = store.gc_sweep(&GcConfig::default()).await.unwrap();
        assert_eq!(result.archived_runs_cleaned, 1);
        assert_eq!(result.chunks_deleted, 1);

        assert!(store.get_run(&id).await.unwrap().is_some());
        assert_eq!(store.chunk_count(&id).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn gc_sweep_warm_rolloff_drops_old_output() {
        let store = TaskStore::open_in_memory().await.unwrap();

        let old_id = TaskRunId::new();
        let old_meta = make_meta_with_age(old_id.clone(), "cmd", 31 * 24 * 3600);
        store.insert_run(&old_meta).await.unwrap();
        store.append_chunk(&old_id, 0, Stream::Stdout, b"old").await.unwrap();

        let new_id = TaskRunId::new();
        let new_meta = make_meta_with_age(new_id.clone(), "cmd", 60);
        store.insert_run(&new_meta).await.unwrap();
        store.append_chunk(&new_id, 0, Stream::Stdout, b"new").await.unwrap();

        let result = store.gc_sweep(&GcConfig::default()).await.unwrap();
        assert_eq!(result.warm_rolloff_runs, 1);
        assert_eq!(result.chunks_deleted, 1);

        assert!(store.get_run(&old_id).await.unwrap().is_some());
        assert_eq!(store.chunk_count(&old_id).await.unwrap(), 0);
        assert_eq!(store.chunk_count(&new_id).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn gc_sweep_pin_exemption() {
        let store = TaskStore::open_in_memory().await.unwrap();

        let pinned_id = TaskRunId::new();
        let mut pinned_meta = make_meta_with_age(pinned_id.clone(), "cmd", 31 * 24 * 3600);
        pinned_meta.pinned = true;
        store.insert_run(&pinned_meta).await.unwrap();
        store.append_chunk(&pinned_id, 0, Stream::Stdout, b"pinned").await.unwrap();

        let result = store.gc_sweep(&GcConfig::default()).await.unwrap();
        assert_eq!(result.warm_rolloff_runs, 0, "pinned run must not count toward warm rolloff");
        assert_eq!(result.chunks_deleted, 0, "pinned run output must survive gc_sweep");
        assert_eq!(store.chunk_count(&pinned_id).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn pin_run_toggles_pin_flag() {
        let store = TaskStore::open_in_memory().await.unwrap();
        let id = TaskRunId::new();
        store.insert_run(&make_meta(id.clone(), "cmd")).await.unwrap();

        store.pin_run(&id, true).await.unwrap();
        assert!(store.get_run(&id).await.unwrap().unwrap().pinned);

        store.pin_run(&id, false).await.unwrap();
        assert!(!store.get_run(&id).await.unwrap().unwrap().pinned);
    }
}
