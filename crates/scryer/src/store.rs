//! Short-disk SQLite store for scryer events.
//!
//! One `EventStore` per machine; path is `/var/lib/yah/scryer/events.db`.
//! Schema is a generalisation of the task-runs `events` table: `run_id` is
//! replaced by `(scope_kind TEXT, scope_id TEXT)` so the same table and
//! indexes serve both TaskRun and Service (and future Forge) scopes without
//! a schema change.
//!
//! WAL mode enables concurrent readers while the ring-flush writer commits.

use observation::{
    rollup_window_start, AttrValue, ChunkRef, Event, EventScope, EventSource, ForgeId, HopRollup,
    LatencyHistogram, Level, Span, SpanId, SpanKind, SpanStatus, TaskRunId, TraceId,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::collections::BTreeMap;
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
    /// A span column that did not parse back into its type — a bad trace/span
    /// id hex, an unknown `kind`, or an unknown `status_code`.
    #[error("parse span: {0}")]
    Span(String),
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

-- R893-F15 — spans, a SECOND signal beside events, not an extension of them.
-- Same (scope_kind, scope_id) scoping so a span and a log line agree on what a
-- service is; everything else is the OTel span data model.
--
-- Identity is (trace_id, span_id): span ids are unique within a trace by spec,
-- so INSERT OR IGNORE makes a re-flush idempotent exactly as it does for events.
-- `attributes_json` is a flat dotted map whose keys are already valid OTLP
-- attribute keys. The peer is NOT denormalised here — `span_rollups` is the
-- query surface for hops, and a second copy of a derived value is a second
-- thing to keep true.
CREATE TABLE IF NOT EXISTS spans (
    scope_kind       TEXT    NOT NULL,
    scope_id         TEXT    NOT NULL,
    trace_id         TEXT    NOT NULL,
    span_id          TEXT    NOT NULL,
    parent_span_id   TEXT,
    name             TEXT    NOT NULL,
    kind             TEXT    NOT NULL,
    start_unix_nanos INTEGER NOT NULL,
    duration_nanos   INTEGER NOT NULL,
    status_code      TEXT    NOT NULL,
    status_message   TEXT,
    attributes_json  TEXT    NOT NULL,
    PRIMARY KEY (scope_kind, scope_id, trace_id, span_id)
);
CREATE INDEX IF NOT EXISTS spans_by_start
    ON spans(scope_kind, scope_id, start_unix_nanos);
-- Un-scoped: assembling one trace means finding its spans across every service.
CREATE INDEX IF NOT EXISTS spans_by_trace
    ON spans(trace_id, start_unix_nanos);

-- Pre-aggregated hop statistics on 60-second tumbling windows
-- (observation::ROLLUP_WINDOW_NANOS), maintained at write time by
-- `insert_spans`. See that function and the ROLLUP_WINDOW_NANOS doc comment for
-- why the cadence is here and not in the view.
--
-- `kind` is in the key because one hop yields a Client span on the caller and a
-- Server span on the callee; summing them double-counts every call.
-- `histogram_json` is an observation::LatencyHistogram — explicit buckets, so
-- merging windows is exact addition. Stored quantiles would not merge.
CREATE TABLE IF NOT EXISTS span_rollups (
    scope_kind              TEXT    NOT NULL,
    scope_id                TEXT    NOT NULL,
    peer                    TEXT    NOT NULL,
    kind                    TEXT    NOT NULL,
    window_start_unix_nanos INTEGER NOT NULL,
    error_count             INTEGER NOT NULL,
    histogram_json          TEXT    NOT NULL,
    PRIMARY KEY (scope_kind, scope_id, peer, kind, window_start_unix_nanos)
);
CREATE INDEX IF NOT EXISTS span_rollups_by_window
    ON span_rollups(window_start_unix_nanos);
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

/// Scope selector for raw span queries.
///
/// The raw table is for looking at individual spans — assembling one trace,
/// eyeballing a slow request. Aggregates come from
/// [`EventStore::query_hop_rollups`], which is the surface that survives fleet
/// volume.
#[derive(Debug, Clone)]
pub struct SpanFilter {
    pub scope: EventScope,
    /// Restrict to one trace.
    pub trace_id: Option<TraceId>,
    pub kind: Option<SpanKind>,
    /// Inclusive-exclusive `start_unix_nanos` range.
    pub start_range: Option<(u64, u64)>,
    pub limit: Option<usize>,
}

impl SpanFilter {
    pub fn for_scope(scope: EventScope) -> Self {
        Self { scope, trace_id: None, kind: None, start_range: None, limit: None }
    }
}

/// `(scope_kind, scope_id, peer, span_kind, window_start_unix_nanos)` — the
/// primary key of `span_rollups`, as the in-memory accumulator sees it.
type RollupKey = (String, String, String, &'static str, u64);

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
            let scope = scope_from_parts(&kind, &id)?;
            results.push(ScopeInfo { scope, event_count, last_offset_ms });
        }
        Ok(results)
    }

    // ─── Spans (R893-F15) ────────────────────────────────────────────────────

    /// Insert a batch of spans and fold them into the 60-second rollup windows,
    /// in one transaction. Returns the number of spans that were NEW.
    ///
    /// **The rollup is maintained here, at write time, not by a background job
    /// and not by the view.** A separate sweeper would need a schedule, a
    /// liveness story and a catch-up path for whatever it missed; riding the
    /// existing flush transaction has none of those and cannot drift from the
    /// raw table, because either both land or neither does.
    ///
    /// The idempotency trap this has to dodge: `insert_events` is deliberately
    /// `INSERT OR IGNORE` so a re-flush of the same ring window is harmless. An
    /// unconditional rollup update would turn that harmless re-flush into
    /// double-counted latency. So only spans whose insert actually changed a
    /// row (`execute` returned 1) contribute.
    pub fn insert_spans(&self, items: &[(EventScope, Span)]) -> Result<usize, ScryerStoreError> {
        let conn = self.inner.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        let mut inserted = 0usize;
        let mut deltas: BTreeMap<RollupKey, (u64, LatencyHistogram)> = BTreeMap::new();

        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO spans \
                 (scope_kind, scope_id, trace_id, span_id, parent_span_id, name, kind, \
                  start_unix_nanos, duration_nanos, status_code, status_message, attributes_json) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            )?;
            for (scope, span) in items {
                let attributes_json = serde_json::to_string(&span.attributes)?;
                let changed = stmt.execute(params![
                    scope.kind_str(),
                    scope.id_str(),
                    span.trace_id.to_hex(),
                    span.span_id.to_hex(),
                    span.parent_span_id.map(|id| id.to_hex()),
                    span.name,
                    span.kind.as_str(),
                    span.start_unix_nanos as i64,
                    span.duration_nanos as i64,
                    span.status.code_str(),
                    span.status.message(),
                    attributes_json,
                ])?;
                if changed == 0 {
                    continue; // already counted on a previous flush
                }
                inserted += 1;

                let key = (
                    scope.kind_str().to_string(),
                    scope.id_str(),
                    span.peer_ident().unwrap_or_default().to_string(),
                    span.kind.as_str(),
                    rollup_window_start(span.start_unix_nanos),
                );
                let entry = deltas.entry(key).or_default();
                if span.is_error() {
                    entry.0 += 1;
                }
                entry.1.record(span.duration_nanos);
            }
        }

        // Fold each window's delta into whatever is already stored for it.
        // One read-modify-write per distinct window touched, not per span.
        for ((scope_kind, scope_id, peer, kind, window), (errors, histogram)) in deltas {
            let existing: Option<(i64, String)> = tx
                .query_row(
                    "SELECT error_count, histogram_json FROM span_rollups \
                     WHERE scope_kind = ?1 AND scope_id = ?2 AND peer = ?3 AND kind = ?4 \
                       AND window_start_unix_nanos = ?5",
                    params![scope_kind, scope_id, peer, kind, window as i64],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;

            let (mut total_errors, mut total_latency) = match existing {
                Some((errs, json)) => (errs as u64, serde_json::from_str(&json)?),
                None => (0u64, LatencyHistogram::new()),
            };
            total_errors += errors;
            total_latency.merge(&histogram);

            tx.execute(
                "INSERT OR REPLACE INTO span_rollups \
                 (scope_kind, scope_id, peer, kind, window_start_unix_nanos, \
                  error_count, histogram_json) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    scope_kind,
                    scope_id,
                    peer,
                    kind,
                    window as i64,
                    total_errors as i64,
                    serde_json::to_string(&total_latency)?,
                ],
            )?;
        }

        tx.commit()?;
        Ok(inserted)
    }

    /// Query raw spans, ordered by `(start_unix_nanos, span_id)`.
    pub fn query_spans(
        &self,
        filter: &SpanFilter,
    ) -> Result<Vec<(EventScope, Span)>, ScryerStoreError> {
        let conn = self.inner.lock().unwrap();
        let mut sql = String::from(
            "SELECT scope_kind, scope_id, trace_id, span_id, parent_span_id, name, kind, \
                    start_unix_nanos, duration_nanos, status_code, status_message, attributes_json \
             FROM spans WHERE scope_kind = ?1 AND scope_id = ?2",
        );
        let mut args: Vec<Box<dyn rusqlite::ToSql>> = vec![
            Box::new(filter.scope.kind_str().to_string()),
            Box::new(filter.scope.id_str()),
        ];
        if let Some(trace_id) = filter.trace_id {
            args.push(Box::new(trace_id.to_hex()));
            sql.push_str(&format!(" AND trace_id = ?{}", args.len()));
        }
        if let Some(kind) = filter.kind {
            args.push(Box::new(kind.as_str().to_string()));
            sql.push_str(&format!(" AND kind = ?{}", args.len()));
        }
        if let Some((from, to)) = filter.start_range {
            args.push(Box::new(from as i64));
            sql.push_str(&format!(" AND start_unix_nanos >= ?{}", args.len()));
            args.push(Box::new(to as i64));
            sql.push_str(&format!(" AND start_unix_nanos < ?{}", args.len()));
        }
        sql.push_str(" ORDER BY start_unix_nanos ASC, span_id ASC");
        if let Some(limit) = filter.limit {
            args.push(Box::new(limit.min(10_000) as i64));
            sql.push_str(&format!(" LIMIT ?{}", args.len()));
        }

        let mut stmt = conn.prepare(&sql)?;
        let params_ref: Vec<&dyn rusqlite::ToSql> = args.iter().map(|b| b.as_ref()).collect();
        let mut rows = stmt.query(params_ref.as_slice())?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            results.push((row_to_scope(row)?, row_to_span(row)?));
        }
        Ok(results)
    }

    /// Merge the rollup windows overlapping `[from_unix_nanos, to_unix_nanos)`
    /// into one [`HopRollup`] per `(scope, peer, kind)` — the hop matrix.
    ///
    /// Passing `scope: None` returns every service on this machine, which is
    /// what a fleet panel wants. The returned window bounds are the ALIGNED
    /// REQUESTED range, not the range that had data, so a hop that fell silent
    /// shows a genuinely lower [`HopRollup::rate_per_sec`] instead of an
    /// unchanged one.
    pub fn query_hop_rollups(
        &self,
        scope: Option<&EventScope>,
        from_unix_nanos: u64,
        to_unix_nanos: u64,
    ) -> Result<Vec<HopRollup>, ScryerStoreError> {
        if to_unix_nanos <= from_unix_nanos {
            return Ok(Vec::new());
        }
        let aligned_from = rollup_window_start(from_unix_nanos);
        let aligned_to = rollup_window_start(to_unix_nanos - 1) + observation::ROLLUP_WINDOW_NANOS;

        let conn = self.inner.lock().unwrap();
        let mut sql = String::from(
            "SELECT scope_kind, scope_id, peer, kind, error_count, histogram_json \
             FROM span_rollups \
             WHERE window_start_unix_nanos >= ?1 AND window_start_unix_nanos < ?2",
        );
        let mut args: Vec<Box<dyn rusqlite::ToSql>> =
            vec![Box::new(aligned_from as i64), Box::new(aligned_to as i64)];
        if let Some(scope) = scope {
            args.push(Box::new(scope.kind_str().to_string()));
            sql.push_str(&format!(" AND scope_kind = ?{}", args.len()));
            args.push(Box::new(scope.id_str()));
            sql.push_str(&format!(" AND scope_id = ?{}", args.len()));
        }

        let mut stmt = conn.prepare(&sql)?;
        let params_ref: Vec<&dyn rusqlite::ToSql> = args.iter().map(|b| b.as_ref()).collect();
        let mut rows = stmt.query(params_ref.as_slice())?;

        // Element-wise histogram addition is exact, which is the whole reason
        // buckets are stored rather than per-window quantiles.
        let mut merged: BTreeMap<(String, String, String, String), (u64, LatencyHistogram)> =
            BTreeMap::new();
        while let Some(row) = rows.next()? {
            let scope_kind: String = row.get(0)?;
            let scope_id: String = row.get(1)?;
            let peer: String = row.get(2)?;
            let kind: String = row.get(3)?;
            let error_count: i64 = row.get(4)?;
            let histogram_json: String = row.get(5)?;
            let histogram: LatencyHistogram = serde_json::from_str(&histogram_json)?;

            let entry = merged
                .entry((scope_kind, scope_id, peer, kind))
                .or_insert_with(|| (0, LatencyHistogram::new()));
            entry.0 += error_count as u64;
            entry.1.merge(&histogram);
        }

        let mut results = Vec::with_capacity(merged.len());
        for ((scope_kind, scope_id, peer, kind), (error_count, latency)) in merged {
            results.push(HopRollup {
                scope: scope_from_parts(&scope_kind, &scope_id)?,
                peer,
                kind: SpanKind::from_str(&kind).map_err(ScryerStoreError::Span)?,
                window_start_unix_nanos: aligned_from,
                window_end_unix_nanos: aligned_to,
                error_count,
                latency,
            });
        }
        Ok(results)
    }

    /// Delete raw spans started before `cutoff_unix_nanos`.
    ///
    /// Rollups are pruned separately and on a much longer clock
    /// ([`EventStore::prune_span_rollups_older_than`]) — that split is the
    /// point of the rollup table. Aggregates keep answering for windows whose
    /// raw spans are long gone.
    pub fn prune_spans_older_than(
        &self,
        cutoff_unix_nanos: u64,
    ) -> Result<usize, ScryerStoreError> {
        let conn = self.inner.lock().unwrap();
        Ok(conn.execute(
            "DELETE FROM spans WHERE start_unix_nanos < ?1",
            params![cutoff_unix_nanos as i64],
        )?)
    }

    /// Delete rollup windows starting before `cutoff_unix_nanos`.
    pub fn prune_span_rollups_older_than(
        &self,
        cutoff_unix_nanos: u64,
    ) -> Result<usize, ScryerStoreError> {
        let conn = self.inner.lock().unwrap();
        Ok(conn.execute(
            "DELETE FROM span_rollups WHERE window_start_unix_nanos < ?1",
            params![cutoff_unix_nanos as i64],
        )?)
    }

    /// Count of raw spans in the store.
    pub fn count_spans(&self) -> Result<i64, ScryerStoreError> {
        let conn = self.inner.lock().unwrap();
        Ok(conn.query_row("SELECT COUNT(*) FROM spans", [], |r| r.get(0))?)
    }
}

// ─── Row helpers ─────────────────────────────────────────────────────────────

/// Rebuild an [`EventScope`] from its `(scope_kind, scope_id)` column pair.
///
/// Single owner for this mapping: `row_to_scope`, `list_scopes` and
/// `query_hop_rollups` all reach it through here rather than each carrying
/// their own copy of the match.
fn scope_from_parts(kind: &str, id: &str) -> Result<EventScope, ScryerStoreError> {
    match kind {
        "task_run" => {
            let run_id: TaskRunId = id.parse()?;
            Ok(EventScope::TaskRun(run_id))
        }
        "service" => Ok(EventScope::Service(workload_spec::MeshIdent(id.to_string()))),
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

fn row_to_scope(row: &rusqlite::Row<'_>) -> Result<EventScope, ScryerStoreError> {
    let kind: String = row.get(0)?;
    let id: String = row.get(1)?;
    scope_from_parts(&kind, &id)
}

fn row_to_span(row: &rusqlite::Row<'_>) -> Result<Span, ScryerStoreError> {
    let trace_id: String = row.get(2)?;
    let span_id: String = row.get(3)?;
    let parent_span_id: Option<String> = row.get(4)?;
    let name: String = row.get(5)?;
    let kind: String = row.get(6)?;
    let start_unix_nanos: i64 = row.get(7)?;
    let duration_nanos: i64 = row.get(8)?;
    let status_code: String = row.get(9)?;
    let status_message: Option<String> = row.get(10)?;
    let attributes_json: String = row.get(11)?;

    let parent_span_id = parent_span_id
        .map(|hex| SpanId::from_hex(&hex))
        .transpose()
        .map_err(ScryerStoreError::Span)?;
    let attributes: BTreeMap<String, AttrValue> = serde_json::from_str(&attributes_json)?;

    Ok(Span {
        trace_id: TraceId::from_hex(&trace_id).map_err(ScryerStoreError::Span)?,
        span_id: SpanId::from_hex(&span_id).map_err(ScryerStoreError::Span)?,
        parent_span_id,
        name,
        kind: SpanKind::from_str(&kind).map_err(ScryerStoreError::Span)?,
        start_unix_nanos: start_unix_nanos as u64,
        duration_nanos: duration_nanos as u64,
        status: SpanStatus::from_parts(&status_code, status_message)
            .map_err(ScryerStoreError::Span)?,
        attributes,
    })
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

    // ─── Spans (R893-F15) ────────────────────────────────────────────────────

    use observation::{
        AttrValue, Span, SpanId, SpanKind, SpanStatus, TraceId, ATTR_HTTP_REQUEST_METHOD,
        ATTR_HTTP_RESPONSE_STATUS_CODE, ATTR_YAH_PEER_SERVICE, ROLLUP_WINDOW_NANOS,
    };
    use std::collections::BTreeMap;

    fn service_scope(name: &str) -> EventScope {
        EventScope::Service(workload_spec::MeshIdent(name.to_string()))
    }

    /// `n` distinguishes spans; `peer` and `kind` drive the rollup key.
    fn make_span(n: u64, peer: &str, kind: SpanKind, start: u64, duration: u64) -> Span {
        let mut span_id = [0u8; 8];
        span_id[0..8].copy_from_slice(&n.to_be_bytes());
        Span {
            trace_id: TraceId([7u8; 16]),
            span_id: SpanId(span_id),
            parent_span_id: None,
            name: "GET /orders/{id}".to_string(),
            kind,
            start_unix_nanos: start,
            duration_nanos: duration,
            status: SpanStatus::Ok,
            attributes: BTreeMap::from([
                (ATTR_YAH_PEER_SERVICE.to_string(), AttrValue::from(peer)),
                (ATTR_HTTP_REQUEST_METHOD.to_string(), AttrValue::from("GET")),
                (ATTR_HTTP_RESPONSE_STATUS_CODE.to_string(), AttrValue::from(200u16)),
            ]),
        }
    }

    #[test]
    fn spans_round_trip_through_sqlite() {
        let dir = TempDir::new().unwrap();
        let store = EventStore::open(&dir.path().join("events.db")).unwrap();
        let scope = service_scope("passway");

        // Every field, including the ones with a nullable column.
        let rich = Span {
            parent_span_id: Some(SpanId([9, 8, 7, 6, 5, 4, 3, 2])),
            status: SpanStatus::Error { message: Some("upstream refused".to_string()) },
            ..make_span(1, "orders", SpanKind::Client, 1_000, 2_000_000)
        };
        let bare = Span {
            parent_span_id: None,
            status: SpanStatus::Unset,
            attributes: BTreeMap::new(),
            ..make_span(2, "", SpanKind::Server, 2_000, 3_000_000)
        };
        let inserted = store
            .insert_spans(&[(scope.clone(), rich.clone()), (scope.clone(), bare.clone())])
            .unwrap();
        assert_eq!(inserted, 2);

        let rows = store.query_spans(&SpanFilter::for_scope(scope.clone())).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1, rich);
        assert_eq!(rows[1].1, bare);
        assert_eq!(rows[0].0, scope);

        // Filters narrow without disturbing the shape.
        let by_kind =
            SpanFilter { kind: Some(SpanKind::Server), ..SpanFilter::for_scope(scope.clone()) };
        assert_eq!(store.query_spans(&by_kind).unwrap().len(), 1);

        let by_trace =
            SpanFilter { trace_id: Some(TraceId([7u8; 16])), ..SpanFilter::for_scope(scope.clone()) };
        assert_eq!(store.query_spans(&by_trace).unwrap().len(), 2);

        let no_such_trace =
            SpanFilter { trace_id: Some(TraceId::INVALID), ..SpanFilter::for_scope(scope.clone()) };
        assert_eq!(store.query_spans(&no_such_trace).unwrap().len(), 0);

        let windowed =
            SpanFilter { start_range: Some((0, 2_000)), ..SpanFilter::for_scope(scope) };
        assert_eq!(store.query_spans(&windowed).unwrap().len(), 1); // 2_000 is exclusive
    }

    /// The trap `insert_spans` exists to dodge: re-flushing the same ring
    /// window must not double-count latency into the rollup.
    #[test]
    fn span_reflush_is_idempotent_in_both_tables() {
        let dir = TempDir::new().unwrap();
        let store = EventStore::open(&dir.path().join("events.db")).unwrap();
        let scope = service_scope("passway");

        let batch: Vec<(EventScope, Span)> = (0..10)
            .map(|i| (scope.clone(), make_span(i, "orders", SpanKind::Client, 1_000, 2_000_000)))
            .collect();

        assert_eq!(store.insert_spans(&batch).unwrap(), 10);
        assert_eq!(store.insert_spans(&batch).unwrap(), 0); // nothing new
        assert_eq!(store.count_spans().unwrap(), 10);

        let hops = store
            .query_hop_rollups(Some(&scope), 0, ROLLUP_WINDOW_NANOS)
            .unwrap();
        assert_eq!(hops.len(), 1);
        assert_eq!(hops[0].count(), 10, "re-flush double-counted the rollup");
        assert_eq!(hops[0].latency.sum_nanos, 20_000_000);
    }

    #[test]
    fn rollups_key_on_peer_and_kind_and_merge_across_windows() {
        let dir = TempDir::new().unwrap();
        let store = EventStore::open(&dir.path().join("events.db")).unwrap();
        let scope = service_scope("passway");

        let mut batch = Vec::new();
        // Same hop, spread over three consecutive 60s windows: 3 x 20 calls.
        for window in 0..3u64 {
            for i in 0..20u64 {
                batch.push((
                    scope.clone(),
                    make_span(
                        window * 100 + i,
                        "orders",
                        SpanKind::Client,
                        window * ROLLUP_WINDOW_NANOS + i,
                        2_000_000,
                    ),
                ));
            }
        }
        // The callee's view of the same hop — a different row, not a merge.
        batch.push((
            scope.clone(),
            make_span(900, "orders", SpanKind::Server, 5, 2_000_000),
        ));
        // A different peer — also a different row.
        let mut failing = make_span(901, "billing", SpanKind::Client, 5, 8_000_000);
        failing.status = SpanStatus::Error { message: None };
        batch.push((scope.clone(), failing));
        // A span naming no peer at all stores under the empty-string key.
        let mut anonymous = make_span(902, "", SpanKind::Client, 5, 1_000_000);
        anonymous.attributes.remove(ATTR_YAH_PEER_SERVICE);
        batch.push((scope.clone(), anonymous));

        assert_eq!(store.insert_spans(&batch).unwrap(), 63);

        // One window only: 20 client calls to orders, plus the three singletons.
        let first = store
            .query_hop_rollups(Some(&scope), 0, ROLLUP_WINDOW_NANOS)
            .unwrap();
        assert_eq!(first.len(), 4);
        let orders_client = first
            .iter()
            .find(|h| h.peer == "orders" && h.kind == SpanKind::Client)
            .unwrap();
        assert_eq!(orders_client.count(), 20);
        assert_eq!(orders_client.rate_per_sec(), 20.0 / 60.0);
        assert!(first.iter().any(|h| h.peer == "orders" && h.kind == SpanKind::Server));
        assert!(first.iter().any(|h| h.peer.is_empty()));

        let billing = first.iter().find(|h| h.peer == "billing").unwrap();
        assert_eq!(billing.error_count, 1);
        assert_eq!(billing.error_rate(), 1.0);

        // All three windows merge into one cell: exact bucket addition.
        let all = store
            .query_hop_rollups(Some(&scope), 0, 3 * ROLLUP_WINDOW_NANOS)
            .unwrap();
        let orders_client = all
            .iter()
            .find(|h| h.peer == "orders" && h.kind == SpanKind::Client)
            .unwrap();
        assert_eq!(orders_client.count(), 60);
        assert_eq!(orders_client.latency.sum_nanos, 120_000_000);
        assert_eq!(orders_client.error_count, 0);
        // Rate is over the REQUESTED 180s, not over the windows that had data.
        assert_eq!(orders_client.rate_per_sec(), 60.0 / 180.0);
        assert_eq!(orders_client.window_start_unix_nanos, 0);
        assert_eq!(orders_client.window_end_unix_nanos, 3 * ROLLUP_WINDOW_NANOS);

        // Scope filtering and empty/degenerate ranges.
        assert!(store
            .query_hop_rollups(Some(&service_scope("nobody")), 0, 3 * ROLLUP_WINDOW_NANOS)
            .unwrap()
            .is_empty());
        assert!(store.query_hop_rollups(None, 0, 0).unwrap().is_empty());
        assert_eq!(store.query_hop_rollups(None, 0, 3 * ROLLUP_WINDOW_NANOS).unwrap().len(), 4);
    }

    /// The two tiers are pruned on two clocks on purpose: aggregates must keep
    /// answering after the raw spans they came from are gone.
    #[test]
    fn pruning_raw_spans_leaves_the_rollups_answering() {
        let dir = TempDir::new().unwrap();
        let store = EventStore::open(&dir.path().join("events.db")).unwrap();
        let scope = service_scope("passway");

        let batch: Vec<(EventScope, Span)> = (0..10)
            .map(|i| {
                (
                    scope.clone(),
                    make_span(i, "orders", SpanKind::Client, i * 1_000, 2_000_000),
                )
            })
            .collect();
        store.insert_spans(&batch).unwrap();

        assert_eq!(store.prune_spans_older_than(5_000).unwrap(), 5);
        assert_eq!(store.count_spans().unwrap(), 5);

        let hops = store
            .query_hop_rollups(Some(&scope), 0, ROLLUP_WINDOW_NANOS)
            .unwrap();
        assert_eq!(hops[0].count(), 10, "rollup must outlive the raw spans");

        assert_eq!(store.prune_span_rollups_older_than(ROLLUP_WINDOW_NANOS).unwrap(), 1);
        assert!(store
            .query_hop_rollups(Some(&scope), 0, ROLLUP_WINDOW_NANOS)
            .unwrap()
            .is_empty());
    }
}
