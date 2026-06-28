//! Scryer service: ring + short-disk backed event query surface.
//!
//! F1 (scryer-1) scope: TaskRun only. For TaskRun queries, scryer delegates
//! to the task-runs store so `scryer.events(TaskRun(id))` returns exactly the
//! same rows as `task.events(id)`. The ring buffer accepts pushes from external
//! ingesters and spills to short-disk on the flush threshold.
//!
//! F2 will add Service scope via the containerd_logs and warden_rpc adapters.

use crate::federation::{FederationPeer, FederationRule};
use crate::long_tier::{LongTierError, LongTierStore};
use crate::ring::{EventRing, RingConfig};
use crate::store::{EventStore, ScryerStoreError, ScopeFilter, ScopeInfo};
use observation::{Event, EventScope, Level};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use task_runs::{EventFilter as TaskEventFilter, StoreError as TaskStoreError, TaskStore};
use thiserror::Error;
use tokio::sync::broadcast;

// ─── Config ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ScryerConfig {
    /// Path to the short-disk SQLite file (typically `/var/lib/yah/scryer/events.db`).
    pub db_path: PathBuf,
    pub ring: RingConfig,
    /// Subscriber broadcast channel capacity.
    pub subscriber_capacity: usize,
    /// Retention window in milliseconds (default 7 days).
    pub retention_ms: u64,
}

impl ScryerConfig {
    pub fn new(db_path: impl AsRef<Path>) -> Self {
        Self {
            db_path: db_path.as_ref().to_owned(),
            ring: RingConfig::default(),
            subscriber_capacity: 256,
            retention_ms: 7 * 24 * 3600 * 1000,
        }
    }
}

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ScryerError {
    #[error("store: {0}")]
    Store(#[from] ScryerStoreError),
    #[error("task-runs store: {0}")]
    TaskStore(#[from] TaskStoreError),
    #[error("long tier: {0}")]
    LongTier(#[from] LongTierError),
}

// ─── Filter / cursor types ────────────────────────────────────────────────────

/// Filter for `Scryer::events`.
#[derive(Debug, Default, Clone)]
pub struct EventFilter {
    pub min_level: Option<Level>,
    /// Exact target match (mirrors task-runs::EventFilter::target).
    pub target: Option<String>,
    pub offset_range: Option<(u32, u32)>,
    /// Inclusive seq range — converted to `std::ops::Range` when delegating.
    pub seq_range: Option<(u32, u32)>,
    pub limit: Option<u32>,
}

/// Opaque cursor returned by `tail` and passed back for subsequent calls.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct QueryCursor {
    /// Ring high-water cursor at the time of the last tail call.
    pub ring_cursor: u64,
    /// Last event seq seen (for store fallback).
    pub last_seq: u32,
}

impl QueryCursor {
    pub fn beginning() -> Self {
        Self { ring_cursor: 0, last_seq: 0 }
    }
}

/// Result of `Scryer::tail`.
pub struct TailResult {
    pub events: Vec<Event>,
    pub next_cursor: QueryCursor,
}

// ─── Scryer ───────────────────────────────────────────────────────────────────

/// Aggregate bucket returned by [`Scryer::aggregate`].
#[derive(Debug, Clone)]
pub struct AggregateBucket {
    /// Group key — depends on `group_by`: level string, first target segment, or hour string.
    pub key: String,
    /// Number of events in this bucket.
    pub count: u64,
}

pub struct Scryer {
    ring: Arc<EventRing>,
    store: Arc<EventStore>,
    /// Task-runs store for TaskRun-scope delegation (same data, different path).
    task_store: Option<Arc<TaskStore>>,
    tx: broadcast::Sender<(EventScope, Event)>,
    _cfg: ScryerConfig,
    /// Long-tier Parquet store and the short-disk retention boundary.
    ///
    /// When set, `aggregate` routes events older than `retention_ms` to the
    /// Parquet tier and reads the rest from short-disk.  Wire via
    /// [`Scryer::with_long_tier`] after construction.
    long_tier: Option<(Arc<LongTierStore>, u64)>,
}

impl Scryer {
    /// Create a new Scryer with its own ring and short-disk store.
    ///
    /// `task_store` is optional but should be provided in-process so that
    /// `scryer.events(TaskRun(id))` delegates to the authoritative task-runs
    /// store rather than scryer's own events.db (which may be ahead or behind
    /// by up to one ring-flush cycle).
    pub fn new(cfg: ScryerConfig, task_store: Option<Arc<TaskStore>>) -> Result<Self, ScryerError> {
        let ring = EventRing::new(cfg.ring.clone());
        let store = Arc::new(EventStore::open(&cfg.db_path)?);
        let (tx, _) = broadcast::channel(cfg.subscriber_capacity);
        Ok(Self { ring, store, task_store, tx, _cfg: cfg, long_tier: None })
    }

    /// Attach a long-tier Parquet store.
    ///
    /// `short_disk_retention_ms` is the boundary: events older than this value
    /// are expected to live in the Parquet tier (post-rollover); newer events
    /// are in short-disk.  Used by [`Scryer::aggregate`] to route queries.
    pub fn with_long_tier(
        mut self,
        store: Arc<LongTierStore>,
        short_disk_retention_ms: u64,
    ) -> Self {
        self.long_tier = Some((store, short_disk_retention_ms));
        self
    }

    /// Push an event into the ring and optionally flush to short-disk.
    ///
    /// Callers should use this for Service-scope events (F2+). TaskRun-scope
    /// events are handled via the task-runs store directly.
    pub fn push(&self, scope: EventScope, event: Event) -> Result<(), ScryerError> {
        let _ = self.tx.send((scope.clone(), event.clone()));
        let (_cursor, should_flush) = self.ring.push(scope, event);
        if should_flush {
            self.flush_ring()?;
        }
        Ok(())
    }

    /// Flush pending ring events to short-disk.
    pub fn flush_ring(&self) -> Result<(), ScryerError> {
        let pending = self.ring.take_pending();
        if !pending.is_empty() {
            self.store.insert_events(&pending)?;
        }
        Ok(())
    }

    /// Query events for `scope` matching `filter`.
    ///
    /// For `TaskRun` scope: delegates to the task-runs store (same rows as
    /// `task.events`). For other scopes: queries scryer's events.db.
    pub async fn events(
        &self,
        scope: &EventScope,
        filter: &EventFilter,
    ) -> Result<Vec<Event>, ScryerError> {
        match (scope, &self.task_store) {
            (EventScope::TaskRun(run_id), Some(ts)) => {
                let tf = TaskEventFilter {
                    target: filter.target.clone(),
                    min_level: filter.min_level,
                    seq_range: filter.seq_range.map(|(lo, hi)| lo..hi),
                    offset_range: filter.offset_range,
                    field_filter: None,
                    limit: filter.limit,
                };
                let events = ts.query_events(run_id, &tf).await?;
                Ok(events)
            }
            _ => {
                // General path: scryer's own events.db.
                let sf = ScopeFilter {
                    scope: scope.clone(),
                    min_level: filter.min_level,
                    target_prefix: filter.target.clone(),
                    offset_range: filter.offset_range,
                    seq_range: filter.seq_range,
                    limit: filter.limit.map(|l| l as usize),
                };
                let rows = self.store.query_events(&sf)?;
                Ok(rows.into_iter().map(|(_, ev)| ev).collect())
            }
        }
    }

    /// Cursor-based live-tail.
    ///
    /// Tries the ring first (no disk I/O if the data is still in-memory),
    /// falls back to the store if the cursor predates the ring window.
    pub async fn tail(
        &self,
        scope: &EventScope,
        cursor: QueryCursor,
        limit: usize,
    ) -> Result<TailResult, ScryerError> {
        let limit = limit.min(1000);
        let (events, next_ring) = self.ring.tail_since(scope, cursor.ring_cursor, limit);

        // If ring didn't have enough and the cursor is for a TaskRun, top up from store.
        let events = if events.is_empty() {
            if let (EventScope::TaskRun(run_id), Some(ts)) = (scope, &self.task_store) {
                let tf = TaskEventFilter {
                    seq_range: Some(cursor.last_seq..u32::MAX),
                    limit: Some(limit as u32),
                    ..Default::default()
                };
                ts.query_events(run_id, &tf).await?
            } else {
                events
            }
        } else {
            events
        };

        let next_seq = events.last().map(|e| e.seq + 1).unwrap_or(cursor.last_seq);
        Ok(TailResult {
            events,
            next_cursor: QueryCursor { ring_cursor: next_ring, last_seq: next_seq },
        })
    }

    /// Subscribe to a push stream of events for `scope`.
    ///
    /// Returns a `broadcast::Receiver`. The sender is notified on each `push`.
    /// Lagged receivers are silently dropped (broadcast semantics).
    pub fn subscribe(&self) -> broadcast::Receiver<(EventScope, Event)> {
        self.tx.subscribe()
    }

    /// Federated query: run `events(scope, filter)` locally then fan out to
    /// all peers matching `rule`, merging results by (offset_ms, seq).
    ///
    /// Peer failures are swallowed — federation is best-effort (arch doc
    /// §Federation across machines).  ACL gating happens at the yubaba gRPC
    /// entry point before this method is called; see [`federation::FederationAcl`].
    pub async fn federated_events(
        &self,
        scope: &EventScope,
        filter: &EventFilter,
        rule: &FederationRule,
        peers: &[Arc<dyn FederationPeer>],
    ) -> Result<Vec<Event>, ScryerError> {
        let local = self.events(scope, filter).await?;
        Ok(crate::federation::federated_events(local, peers, filter, rule).await)
    }

    /// List distinct scopes in the store ordered by last-event time desc.
    ///
    /// Used by `scryer.list_services` to surface what idents currently have events.
    pub fn list_scopes(&self, limit: usize) -> Result<Vec<ScopeInfo>, ScryerError> {
        Ok(self.store.list_scopes(limit)?)
    }

    /// Aggregate events for `scope` across both short-disk and, when a long tier
    /// is configured and `since_ms` predates `short_disk_retention_ms`, the
    /// Parquet tier as well.
    ///
    /// `group_by` accepts `"level"`, `"target"` (first `::` segment), or `"hour"`
    /// (`offset_ms / 3_600_000`).  Mirrors the `scryer.aggregate` agent tool but
    /// lives at the service layer so tests can call it directly.
    pub fn aggregate(
        &self,
        scope: &EventScope,
        since_ms: u64,
        group_by: &str,
    ) -> Result<Vec<AggregateBucket>, ScryerError> {
        let mut all_events = Vec::new();

        // Short-disk: events with offset_ms >= boundary.
        let boundary_ms = self.long_tier.as_ref().map_or(since_ms, |(_, ret)| {
            // If since_ms is already within the retention window, use it as lower bound.
            since_ms.max(*ret)
        });
        let sf = ScopeFilter {
            scope: scope.clone(),
            offset_range: Some((boundary_ms as u32, u32::MAX)),
            ..ScopeFilter::for_scope(scope.clone())
        };
        let disk_rows = self.store.query_events(&sf)?;
        all_events.extend(disk_rows.into_iter().map(|(_, e)| e));

        // Long tier: events with offset_ms in [since_ms, boundary_ms).
        if let Some((lt, ret_ms)) = &self.long_tier {
            let until_ms = (*ret_ms).min(boundary_ms);
            if since_ms < until_ms {
                let lt_rows = lt.query_range(Some(scope), since_ms, until_ms)?;
                all_events.extend(lt_rows.into_iter().map(|(_, e)| e));
            }
        }

        // Group and count.
        let mut counts: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        for ev in &all_events {
            let key = match group_by {
                "level" => ev.level.as_str().to_string(),
                "target" => ev.target.splitn(2, "::").next().unwrap_or(&ev.target).to_string(),
                "hour" => format!("h{}", ev.offset_ms / 3_600_000),
                _ => ev.level.as_str().to_string(),
            };
            *counts.entry(key).or_insert(0) += 1;
        }

        let mut buckets: Vec<AggregateBucket> = counts
            .into_iter()
            .map(|(key, count)| AggregateBucket { key, count })
            .collect();
        buckets.sort_by(|a, b| b.count.cmp(&a.count).then(a.key.cmp(&b.key)));
        Ok(buckets)
    }

    pub fn ring(&self) -> &Arc<EventRing> {
        &self.ring
    }

    pub fn store(&self) -> &Arc<EventStore> {
        &self.store
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use observation::{EventSource, TaskRunId};
    use serde_json::json;
    use tempfile::TempDir;

    fn make_event(run_id: TaskRunId, seq: u32) -> Event {
        Event {
            run_id,
            seq,
            offset_ms: seq * 10,
            level: Level::Info,
            target: "test".to_string(),
            msg: format!("event {seq}"),
            fields: json!({}),
            anchor: None,
            source: EventSource::Synth,
        }
    }

    fn open_scryer(dir: &TempDir) -> Scryer {
        let cfg = ScryerConfig::new(dir.path().join("events.db"));
        Scryer::new(cfg, None).unwrap()
    }

    #[test]
    fn scryer_push_and_events_via_store() {
        let dir = TempDir::new().unwrap();
        let scryer = open_scryer(&dir);
        let run_id = TaskRunId::new();
        let scope = EventScope::TaskRun(run_id.clone());

        for i in 0..5 {
            let ev = make_event(run_id.clone(), i);
            scryer.push(scope.clone(), ev).unwrap();
        }
        scryer.flush_ring().unwrap();

        let events = scryer.events(&scope, &EventFilter::default()).unwrap();
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn scryer_tail_from_ring() {
        let dir = TempDir::new().unwrap();
        let scryer = open_scryer(&dir);
        let run_id = TaskRunId::new();
        let scope = EventScope::TaskRun(run_id.clone());

        for i in 0..5 {
            scryer.push(scope.clone(), make_event(run_id.clone(), i)).unwrap();
        }

        let result = scryer.tail(&scope, QueryCursor::beginning(), 100).unwrap();
        assert_eq!(result.events.len(), 5);
        assert_eq!(result.next_cursor.ring_cursor, 5);
    }

    #[test]
    fn scryer_subscribe_receives_events() {
        let dir = TempDir::new().unwrap();
        let scryer = open_scryer(&dir);
        let run_id = TaskRunId::new();
        let scope = EventScope::TaskRun(run_id.clone());

        let mut rx = scryer.subscribe();
        scryer.push(scope.clone(), make_event(run_id.clone(), 0)).unwrap();

        let (recv_scope, recv_ev) = rx.try_recv().unwrap();
        assert_eq!(recv_scope, scope);
        assert_eq!(recv_ev.seq, 0);
    }

    mod events {
        use super::*;
        use observation::{EventSource, ForgeId, Level};
        use serde_json::json;

        /// Verify: scryer.events(Forge(id)) returns the rows written under that
        /// scope, and does not bleed rows from a different ForgeId.
        ///
        /// This is the R093-F9 acceptance criterion from the arch doc.
        #[test]
        fn forge_scope() {
            let dir = TempDir::new().unwrap();
            let cfg = ScryerConfig::new(dir.path().join("events.db"));
            let scryer = Scryer::new(cfg, None).unwrap();

            let forge_id = ForgeId::new();
            let scope = EventScope::Forge(forge_id.clone());

            for i in 0u32..4 {
                let ev = Event {
                    run_id: forge_id.clone().into(),
                    seq: i,
                    offset_ms: i * 10,
                    level: Level::Info,
                    target: "forge::test".to_string(),
                    msg: format!("forge event {i}"),
                    fields: json!({}),
                    anchor: None,
                    source: EventSource::Synth,
                };
                scryer.push(scope.clone(), ev).unwrap();
            }
            scryer.flush_ring().unwrap();

            let events = scryer.events(&scope, &EventFilter::default()).unwrap();
            assert_eq!(events.len(), 4, "expected 4 forge events");
            assert_eq!(events[0].target, "forge::test");
            assert_eq!(events[3].seq, 3);

            // A different ForgeId must return no rows (isolation check).
            let other_scope = EventScope::Forge(ForgeId::new());
            let other = scryer.events(&other_scope, &EventFilter::default()).unwrap();
            assert!(other.is_empty(), "different forge id must return no events");
        }
    }

    /// Verify: scryer.events(TaskRun(id)) returns the same rows as task.events(id).
    ///
    /// This is the F1 acceptance criterion from the arch doc.
    #[test]
    fn scryer_events_taskrun_matches_task_events() {
        use task_runs::{EventFilter as TF, Initiator, RunStatus, TaskRunMeta, TaskStore};

        let dir = TempDir::new().unwrap();
        let task_store = Arc::new(TaskStore::open(&dir.path().join("task-runs.db")).unwrap());

        // Insert a run + events into the task-runs store.
        let run_id = TaskRunId::new();
        let meta = TaskRunMeta {
            id: run_id.clone(),
            command: "echo hi".to_string(),
            cwd: dir.path().to_path_buf(),
            env: vec![],
            started_at: 1000,
            status: RunStatus::Pending,
            label: None,
            initiator: Initiator::Human { camp: "test".to_string() },
            beholder_status: None,
            pinned: false,
            origin: None,
        };
        task_store.insert_run(&meta).unwrap();
        for i in 0u32..5 {
            use observation::{EventSource, Level};
            use serde_json::json;
            task_store
                .append_event(
                    &run_id,
                    i * 10,
                    Level::Info,
                    "test",
                    &format!("event {i}"),
                    &json!({}),
                    None,
                    &EventSource::Synth,
                )
                .unwrap();
        }

        // Create a scryer with the task_store wired in.
        let cfg = ScryerConfig::new(dir.path().join("scryer-events.db"));
        let scryer = Scryer::new(cfg, Some(task_store.clone())).unwrap();

        let scope = EventScope::TaskRun(run_id.clone());
        let scryer_events = scryer.events(&scope, &EventFilter::default()).unwrap();
        let task_events = task_store.query_events(&run_id, &TF::default()).unwrap();

        assert_eq!(scryer_events.len(), task_events.len());
        for (se, te) in scryer_events.iter().zip(task_events.iter()) {
            assert_eq!(se.seq, te.seq);
            assert_eq!(se.msg, te.msg);
        }
    }

    mod aggregate {
        use super::*;
        use crate::long_tier::{InMemoryObjectStore, LongTierConfig, LongTierStore, MS_PER_DAY};
        use observation::{EventSource, Level, TaskRunId};
        use serde_json::json;
        use workload_spec::MeshIdent;

        fn make_svc_event(run_id: &TaskRunId, seq: u32, offset_ms: u32, level: Level) -> Event {
            Event {
                run_id: run_id.clone(),
                seq,
                offset_ms,
                level,
                target: "svc::db".to_string(),
                msg: format!("event {seq}"),
                fields: json!({}),
                anchor: None,
                source: EventSource::Synth,
            }
        }

        /// Verify condition 2 (R093-F5):
        /// A 30-day aggregate query returns rows from both short-disk and long-tier shards.
        #[test]
        fn cross_boundary() {
            let dir = TempDir::new().unwrap();
            let cfg = ScryerConfig::new(dir.path().join("events.db"));
            let scryer = Scryer::new(cfg, None).unwrap();
            let scope = EventScope::Service(MeshIdent("svc.prod".to_string()));
            let run_id = TaskRunId::new();

            // OLD events: offset_ms = 1 day → below 7-day cutoff → will be rolled.
            let old_offset = MS_PER_DAY as u32; // 86_400_000
            for i in 0u32..3 {
                scryer
                    .push(scope.clone(), make_svc_event(&run_id, i, old_offset, Level::Warn))
                    .unwrap();
            }
            scryer.flush_ring().unwrap();

            // Set up long-tier + rollover (cutoff = 7 days).
            let cutoff_ms = 7 * MS_PER_DAY; // 604_800_000
            let obj_store = Arc::new(InMemoryObjectStore::new());
            let lt_cfg = LongTierConfig { machine_id: "m1".to_string(), retention_ms: cutoff_ms };
            let lt = Arc::new(LongTierStore::new(
                lt_cfg,
                Arc::clone(&obj_store) as Arc<dyn crate::long_tier::ObjectStore>,
            ));
            let promoted = lt.rollover(scryer.store(), cutoff_ms).unwrap();
            assert_eq!(promoted, 3, "3 old events promoted to long tier");

            // Verify short-disk no longer has the old events.
            let after_rollover = scryer.events(&scope, &EventFilter::default()).unwrap();
            assert!(after_rollover.is_empty(), "old events removed from short-disk");

            // RECENT events: offset_ms = 10 days → above cutoff → stays in short-disk.
            let recent_offset = (10 * MS_PER_DAY) as u32;
            for i in 3u32..5 {
                scryer
                    .push(scope.clone(), make_svc_event(&run_id, i, recent_offset, Level::Info))
                    .unwrap();
            }
            scryer.flush_ring().unwrap();

            // Wire the long tier into a new Scryer view (with_long_tier is a builder).
            let dir2 = TempDir::new().unwrap();
            let cfg2 = ScryerConfig::new(dir2.path().join("events2.db"));
            let scryer2 = Scryer::new(cfg2, None)
                .unwrap()
                .with_long_tier(Arc::clone(&lt), cutoff_ms);
            // Copy the recent events into scryer2's short-disk.
            let recent_events: Vec<(EventScope, Event)> = (3u32..5)
                .map(|i| {
                    (scope.clone(), make_svc_event(&run_id, i, recent_offset, Level::Info))
                })
                .collect();
            scryer2.store().insert_events(&recent_events).unwrap();

            // 30-day aggregate: since_ms = 0 covers both tiers.
            let buckets = scryer2.aggregate(&scope, 0, "level").unwrap();
            let total: u64 = buckets.iter().map(|b| b.count).sum();
            assert_eq!(total, 5, "3 old (long-tier) + 2 recent (short-disk) = 5 total");

            // Warn bucket (old events) and Info bucket (recent events) both present.
            let warn_count = buckets
                .iter()
                .find(|b| b.key == "warn")
                .map(|b| b.count)
                .unwrap_or(0);
            let info_count = buckets
                .iter()
                .find(|b| b.key == "info")
                .map(|b| b.count)
                .unwrap_or(0);
            assert_eq!(warn_count, 3, "3 Warn events from long-tier");
            assert_eq!(info_count, 2, "2 Info events from short-disk");
        }

        /// Aggregate with no long tier falls back to short-disk only.
        #[test]
        fn short_disk_only() {
            let dir = TempDir::new().unwrap();
            let cfg = ScryerConfig::new(dir.path().join("events.db"));
            let scryer = Scryer::new(cfg, None).unwrap();
            let scope = EventScope::Service(MeshIdent("svc.local".to_string()));
            let run_id = TaskRunId::new();

            for i in 0u32..4 {
                let offset = if i < 2 { 1000u32 } else { 2000u32 };
                let level = if i < 2 { Level::Error } else { Level::Info };
                scryer.push(scope.clone(), make_svc_event(&run_id, i, offset, level)).unwrap();
            }
            scryer.flush_ring().unwrap();

            let buckets = scryer.aggregate(&scope, 0, "level").unwrap();
            let total: u64 = buckets.iter().map(|b| b.count).sum();
            assert_eq!(total, 4);
        }
    }
}
