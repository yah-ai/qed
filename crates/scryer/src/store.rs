//! Short-disk SQLite store for scryer events.
//!
//! One `EventStore` per machine; path is `/var/lib/yah/scryer/events.db`.
//! Schema is a generalisation of the task-runs `events` table: `run_id` is
//! replaced by `(scope_kind TEXT, scope_id TEXT)` so the same table and
//! indexes serve both TaskRun and Service (and future Forge) scopes without
//! a schema change.
//!
//! WAL mode enables concurrent readers while the ring-flush writer commits.

use observation::{ChunkRef, Event, EventScope, EventSource, ForgeId, Level, TaskRunId};
use rusqlite::{params, Connection};
use serde_json::Value;
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;
use thiserror::Error;

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ScryerStoreError {
    #[error("sqlite: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse level: {0}")]
    Level(String),
    #[error("parse run id: {0}")]
    RunId(#[from] uuid::Error),
}

// ─── Schema ───────────────────────────────────────────────────────────────────

const SCHEMA: &str = r#"
-- Generalised events table: scope_kind + scope_id replace the per-table run_id.
-- scope_kind: 'task_run' | 'service' | 'forge'
-- scope_id:   TaskRunId.to_string() | MeshIdent.0 | ForgeId.to_string()
CREATE TABLE IF NOT EXISTS events (
    scope_kind  TEXT    NOT NULL,
    scope_id    TEXT    NOT NULL,
    seq         INTEGER NOT NULL,
    offset_ms   INTEGER NOT NULL,
    level       TEXT    NOT NULL,
    target      TEXT    NOT NULL,
    msg         TEXT    NOT NULL,
    fields_json TEXT    NOT NULL,
    anchor_seq  INTEGER,
    source_kind TEXT    NOT NULL,
    source_name TEXT    NOT NULL,
    PRIMARY KEY (scope_kind, scope_id, seq)
);
CREATE INDEX IF NOT EXISTS events_by_target
    ON events(scope_kind, scope_id, target, level);
CREATE INDEX IF NOT EXISTS events_by_offset
    ON events(scope_kind, scope_id, offset_ms);

-- Registry of lazily-created json_extract expression indexes on events.fields_json.
CREATE TABLE IF NOT EXISTS _event_field_indexes (
    field_path  TEXT PRIMARY KEY,
    index_name  TEXT NOT NULL,
    created_at  INTEGER NOT NULL
);
"#;

// ─── ScopeInfo ────────────────────────────────────────────────────────────────

/// Summary of a distinct scope in the store.
pub struct ScopeInfo {
    pub scope: EventScope,
    /// Total events stored under this scope.
    pub event_count: i64,
    /// `offset_ms` of the most recent event (wall-clock-relative ms since run start).
    pub last_offset_ms: i64,
}

// ─── Filter ───────────────────────────────────────────────────────────────────

/// Scope selector for event queries.
#[derive(Debug, Clone)]
pub struct ScopeFilter {
    pub scope: EventScope,
    /// Filter by level >= this value.
    pub min_level: Option<Level>,
    /// Filter by exact target prefix (LIKE `target%`).
    pub target_prefix: Option<String>,
    /// Inclusive offset_ms range.
    pub offset_range: Option<(u32, u32)>,
    /// Inclusive seq range.
    pub seq_range: Option<(u32, u32)>,
    pub limit: Option<usize>,
}

impl ScopeFilter {
    pub fn for_scope(scope: EventScope) -> Self {
        Self {
            scope,
            min_level: None,
            target_prefix: None,
            offset_range: None,
            seq_range: None,
            limit: None,
        }
    }
}

// ─── EventStore ───────────────────────────────────────────────────────────────

pub struct EventStore {
    inner: Mutex<Connection>,
}

impl EventStore {
    /// Open (or create) the scryer events database at `path`.
    pub fn open(path: &Path) -> Result<Self, ScryerStoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { inner: Mutex::new(conn) })
    }

    /// Insert a batch of events. Ignores conflicts (idempotent re-flush).
    pub fn insert_events(
        &self,
        items: &[(EventScope, Event)],
    ) -> Result<(), ScryerStoreError> {
        let conn = self.inner.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO events \
                 (scope_kind, scope_id, seq, offset_ms, level, target, msg, \
                  fields_json, anchor_seq, source_kind, source_name) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            )?;
            for (scope, ev) in items {
                let fields_json = serde_json::to_string(&ev.fields)?;
                let anchor_seq: Option<i64> = ev.anchor.as_ref().map(|a| a.seq as i64);
                stmt.execute(params![
                    scope.kind_str(),
                    scope.id_str(),
                    ev.seq as i64,
                    ev.offset_ms as i64,
                    ev.level.as_str(),
                    ev.target,
                    ev.msg,
                    fields_json,
                    anchor_seq,
                    ev.source.kind_str(),
                    ev.source.name_str(),
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Query events matching `filter`. Returns events in `(seq)` order.
    pub fn query_events(
        &self,
        filter: &ScopeFilter,
    ) -> Result<Vec<(EventScope, Event)>, ScryerStoreError> {
        let conn = self.inner.lock().unwrap();
        let scope_kind = filter.scope.kind_str();
        let scope_id = filter.scope.id_str();
        let limit = filter.limit.unwrap_or(1000).min(10_000) as i64;

        // Build query dynamically only on optional filters.
        let mut clauses = vec![
            "scope_kind = ?1".to_string(),
            "scope_id = ?2".to_string(),
        ];
        let mut extra_params: Vec<Box<dyn rusqlite::ToSql>> = vec![];

        let mut idx = 3usize;
        if let Some(ref tp) = filter.target_prefix {
            clauses.push(format!("target LIKE ?{idx}"));
            extra_params.push(Box::new(format!("{}%", tp)));
            idx += 1;
        }
        if let Some(min_level) = filter.min_level {
            // Map level strings to integers for >= comparison.
            clauses.push(format!(
                "CASE level \
                 WHEN 'trace' THEN 0 WHEN 'debug' THEN 1 WHEN 'info' THEN 2 \
                 WHEN 'warn' THEN 3 WHEN 'error' THEN 4 WHEN 'fatal' THEN 5 \
                 ELSE 2 END >= ?{idx}"
            ));
            let level_int: i64 = match min_level {
                Level::Trace => 0,
                Level::Debug => 1,
                Level::Info => 2,
                Level::Warn => 3,
                Level::Error => 4,
                Level::Fatal => 5,
            };
            extra_params.push(Box::new(level_int));
            idx += 1;
        }
        if let Some((lo, hi)) = filter.offset_range {
            clauses.push(format!("offset_ms BETWEEN ?{idx} AND ?{}", idx + 1));
            extra_params.push(Box::new(lo as i64));
            extra_params.push(Box::new(hi as i64));
            idx += 2;
        }
        if let Some((lo, hi)) = filter.seq_range {
            clauses.push(format!("seq BETWEEN ?{idx} AND ?{}", idx + 1));
            extra_params.push(Box::new(lo as i64));
            extra_params.push(Box::new(hi as i64));
            let _ = idx; // last clause — idx not incremented further
        }

        let sql = format!(
            "SELECT scope_kind, scope_id, seq, offset_ms, level, target, msg, \
                    fields_json, anchor_seq, source_kind, source_name \
             FROM events WHERE {} ORDER BY seq ASC LIMIT ?{}",
            clauses.join(" AND "),
            extra_params.len() + 3,
        );

        let mut stmt = conn.prepare(&sql)?;

        // Build rusqlite params slice.
        let mut rows = {
            let p1: &dyn rusqlite::ToSql = &scope_kind;
            let p2: &dyn rusqlite::ToSql = &scope_id;
            let p_limit: &dyn rusqlite::ToSql = &limit;

            let mut params_vec: Vec<&dyn rusqlite::ToSql> = vec![p1, p2];
            for ep in &extra_params {
                params_vec.push(ep.as_ref());
            }
            params_vec.push(p_limit);

            stmt.query(params_vec.as_slice())?
        };

        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let scope = row_to_scope(row)?;
            let event = row_to_event(row)?;
            results.push((scope, event));
        }
        Ok(results)
    }

    /// Query all events with `offset_ms < cutoff_ms` across all scopes.
    ///
    /// Used by the long-tier Parquet rollover to read events before pruning them.
    /// Returns events ordered by `(offset_ms, scope_kind, scope_id, seq)`.
    pub fn query_events_older_than(
        &self,
        cutoff_ms: u64,
    ) -> Result<Vec<(EventScope, Event)>, ScryerStoreError> {
        let conn = self.inner.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT scope_kind, scope_id, seq, offset_ms, level, target, msg, \
                    fields_json, anchor_seq, source_kind, source_name \
             FROM events WHERE offset_ms < ?1 \
             ORDER BY offset_ms ASC, scope_kind ASC, scope_id ASC, seq ASC",
        )?;
        let mut rows = stmt.query(params![cutoff_ms as i64])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let scope = row_to_scope(row)?;
            let event = row_to_event(row)?;
            results.push((scope, event));
        }
        Ok(results)
    }

    /// Delete events older than `older_than_ms` (by offset_ms).
    pub fn prune_older_than(&self, older_than_ms: u64) -> Result<usize, ScryerStoreError> {
        let conn = self.inner.lock().unwrap();
        let n = conn.execute(
            "DELETE FROM events WHERE offset_ms < ?1",
            params![older_than_ms as i64],
        )?;
        Ok(n)
    }

    /// Count of events in the store.
    pub fn count(&self) -> Result<i64, ScryerStoreError> {
        let conn = self.inner.lock().unwrap();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))?;
        Ok(n)
    }

    /// List distinct (scope_kind, scope_id) entries ordered by last-event time desc.
    pub fn list_scopes(&self, limit: usize) -> Result<Vec<ScopeInfo>, ScryerStoreError> {
        let limit = limit.min(1000) as i64;
        let conn = self.inner.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT scope_kind, scope_id, COUNT(*) AS cnt, MAX(offset_ms) AS last_ms \
             FROM events GROUP BY scope_kind, scope_id ORDER BY last_ms DESC LIMIT ?1",
        )?;
        let mut results = Vec::new();
        let mut rows = stmt.query(params![limit])?;
        while let Some(row) = rows.next()? {
            let kind: String = row.get(0)?;
            let id: String = row.get(1)?;
            let event_count: i64 = row.get(2)?;
            let last_offset_ms: i64 = row.get(3)?;
            let scope = match kind.as_str() {
                "task_run" => EventScope::TaskRun(id.parse()?),
                "service" => EventScope::Service(workload_spec::MeshIdent(id)),
                "forge" => EventScope::Forge(id.parse()?),
                _ => EventScope::TaskRun(id.parse().unwrap_or_else(|_| TaskRunId::new())),
            };
            results.push(ScopeInfo { scope, event_count, last_offset_ms });
        }
        Ok(results)
    }
}

// ─── Row helpers ─────────────────────────────────────────────────────────────

fn row_to_scope(row: &rusqlite::Row<'_>) -> Result<EventScope, ScryerStoreError> {
    let kind: String = row.get(0)?;
    let id: String = row.get(1)?;
    match kind.as_str() {
        "task_run" => {
            let run_id: TaskRunId = id.parse()?;
            Ok(EventScope::TaskRun(run_id))
        }
        "service" => Ok(EventScope::Service(workload_spec::MeshIdent(id))),
        "forge" => {
            let forge_id: ForgeId = id.parse()?;
            Ok(EventScope::Forge(forge_id))
        }
        _ => {
            // Unknown scope kind — surface as a synthetic task_run so callers
            // don't panic on future scope variants added by later tickets.
            let run_id = id.parse().unwrap_or_else(|_| TaskRunId::new());
            Ok(EventScope::TaskRun(run_id))
        }
    }
}

fn row_to_event(row: &rusqlite::Row<'_>) -> Result<Event, ScryerStoreError> {
    let run_id_str: String = row.get(1)?;
    let run_id: TaskRunId = run_id_str
        .parse()
        .unwrap_or_else(|_| TaskRunId::new());

    let seq: i64 = row.get(2)?;
    let offset_ms: i64 = row.get(3)?;
    let level_str: String = row.get(4)?;
    let level = Level::from_str(&level_str).map_err(ScryerStoreError::Level)?;
    let target: String = row.get(5)?;
    let msg: String = row.get(6)?;
    let fields_json: String = row.get(7)?;
    let fields: Value = serde_json::from_str(&fields_json)?;
    let anchor_seq: Option<i64> = row.get(8)?;
    let source_kind: String = row.get(9)?;
    let source_name: String = row.get(10)?;

    let anchor = anchor_seq.map(|s| ChunkRef { seq: s as u32 });
    let source = match source_kind.as_str() {
        "beholder" => EventSource::Beholder { name: source_name.clone(), version: String::new() },
        "shim" => EventSource::Shim { lib: source_name.clone(), version: String::new() },
        _ => EventSource::Synth,
    };

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

#[cfg(test)]
mod tests {
    use super::*;
    use observation::{EventSource, Level, TaskRunId};
    use serde_json::json;
    use tempfile::TempDir;

    fn make_event(run_id: TaskRunId, seq: u32) -> Event {
        Event {
            run_id,
            seq,
            offset_ms: seq * 10,
            level: Level::Info,
            target: "test".to_string(),
            msg: format!("msg {seq}"),
            fields: json!({"n": seq}),
            anchor: None,
            source: EventSource::Synth,
        }
    }

    #[test]
    fn store_insert_and_query() {
        let dir = TempDir::new().unwrap();
        let store = EventStore::open(&dir.path().join("events.db")).unwrap();

        let run_id = TaskRunId::new();
        let scope = EventScope::TaskRun(run_id.clone());
        let events: Vec<(EventScope, Event)> = (0..5)
            .map(|i| (scope.clone(), make_event(run_id.clone(), i)))
            .collect();
        store.insert_events(&events).unwrap();

        let filter = ScopeFilter::for_scope(scope.clone());
        let rows = store.query_events(&filter).unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].1.seq, 0);
        assert_eq!(rows[4].1.seq, 4);
    }

    #[test]
    fn store_idempotent_insert() {
        let dir = TempDir::new().unwrap();
        let store = EventStore::open(&dir.path().join("events.db")).unwrap();

        let run_id = TaskRunId::new();
        let scope = EventScope::TaskRun(run_id.clone());
        let events = vec![(scope.clone(), make_event(run_id.clone(), 0))];
        store.insert_events(&events).unwrap();
        store.insert_events(&events).unwrap(); // second insert ignored
        assert_eq!(store.count().unwrap(), 1);
    }

    #[test]
    fn store_prune() {
        let dir = TempDir::new().unwrap();
        let store = EventStore::open(&dir.path().join("events.db")).unwrap();

        let run_id = TaskRunId::new();
        let scope = EventScope::TaskRun(run_id.clone());
        let events: Vec<(EventScope, Event)> = (0..10)
            .map(|i| (scope.clone(), make_event(run_id.clone(), i)))
            .collect();
        store.insert_events(&events).unwrap();
        // Events have offset_ms = seq * 10, so prune_older_than(50) removes 0..4.
        let pruned = store.prune_older_than(50).unwrap();
        assert_eq!(pruned, 5); // seq 0-4 → offset_ms 0..40 < 50
        assert_eq!(store.count().unwrap(), 5);
    }
}
