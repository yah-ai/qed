//! Mode-2 analytics snapshot producer.
//!
//! Part of **R556-F6** — canonical `@yah:ticket` annotation lives in
//! `crates/yah/hub/src/in_process.rs`; this module is the missing *data leg* of
//! the Mode-2 (published / at-rest) analytics surface described in
//! `.yah/docs/working/W234-analytics-tab-connectivity.md` §Mode-2 and
//! `.yah/docs/working/W225-mesofact-consumer-deployment-model.md` §3a.
//!
//! # Where this sits
//!
//! F5's [`crate::promotion::PromotionConsumer`] rolls aged short-disk events
//! into per-day Parquet shards in an [`ObjectStore`] (R2). Those shards are the
//! at-rest corpus but they are *raw events* — a browser cannot render them, and
//! per W225 §3b analytics is operator-confidential, so the mesh must never be
//! reached from a browser. This producer closes that gap: it reads the Parquet
//! corpus back via [`LongTierStore::query_range`], aggregates it into the exact
//! shapes the Analytics tab renders (`by_level` / `timeseries` / recent
//! `events` — mirroring the frozen `rpc::Analytics*` wire types the Mode-1
//! backend already returns), serializes that to a single JSON **snapshot**, and
//! publishes it to R2 with **content-address + pointer-flip** (W225 §3a):
//!
//! ```text
//!   analytics/snapshots/<sha256>.json   ← immutable, content-addressed blob
//!   analytics/current.json              ← the one mutable pointer (flip target)
//! ```
//!
//! A managed `mesofact serve` (a kamaji-managed service fronted by a Cloudflare
//! tunnel) reads `analytics/current.json`, fetches the referenced blob, and
//! renders it behind cheers auth. The browser touches only R2/CF — never the
//! tailnet mesh — which is the whole point of Mode-2.
//!
//! # Wire compatibility (by convention, not a dependency)
//!
//! The snapshot types below deliberately mirror `rpc::AnalyticsSummaryResult` /
//! `AnalyticsTimeseriesResult` / `AnalyticsEvent`, exactly as scryer's own
//! [`crate::service::AggregateBucket`] already mirrors `rpc::AnalyticsBucket`.
//! `oss/qed` is a standalone workspace and must not depend on the yah-side `rpc`
//! crate, so the shapes are re-declared here and kept in lockstep by review. The
//! `group_by` key formats (`"level"` / `"target"` first `::` segment / `"hour"`
//! as `h<offset_ms/3_600_000>`) match [`crate::service::Scryer::aggregate`] so a
//! Mode-2 timeseries bucket is identical to the Mode-1 one for the same data.
//!
//! # The offset_ms window caveat
//!
//! `offset_ms` is per-scope "ms since run start", not wall-clock epoch (see
//! [`crate::promotion`]'s retention note). Promotion only ever writes events
//! with `offset_ms < retention_ms`, so the entire long-tier corpus lives in
//! `[0, retention_ms)`; this producer aggregates that whole span. A genuine
//! rolling wall-clock window is a refinement of the underlying store's offset
//! model, not of this consumer, and is left as a follow-up — same boundary the
//! promotion consumer draws.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use observation::{Event, EventScope};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::long_tier::{LongTierConfig, LongTierError, LongTierStore, ObjectStore, ObjectStoreError};

/// A scope-tagged event, as [`LongTierStore::query_range`] returns them.
type ScopedRow = (EventScope, Event);

/// R2 key prefix for immutable, content-addressed snapshot blobs.
pub const SNAPSHOT_PREFIX: &str = "analytics/snapshots/";
/// R2 key for the single mutable pointer the mesofact server reads.
pub const POINTER_KEY: &str = "analytics/current.json";
/// Default producer cadence: once every 5 minutes. Cheap — a pass reads the
/// long-tier corpus (bounded by retention) and writes two small objects.
pub const DEFAULT_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(300);
/// Default cap on recent event rows carried in a snapshot.
pub const DEFAULT_EVENT_LIMIT: usize = 200;

// ─── Error ──────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("long tier: {0}")]
    LongTier(#[from] LongTierError),
    #[error("object store: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("encode: {0}")]
    Encode(#[from] serde_json::Error),
}

// ─── Snapshot wire shapes (mirror rpc::Analytics*) ────────────────────────────

/// One `(level, count)` row — mirrors `rpc::AnalyticsLevelCount`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotLevelCount {
    pub level: String,
    pub count: u64,
}

/// One timeseries bucket — mirrors `rpc::AnalyticsBucket` / [`crate::service::AggregateBucket`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBucket {
    pub key: String,
    pub count: u64,
}

/// One event row — mirrors `rpc::AnalyticsEvent` (flattened; `fields` verbatim).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotEvent {
    pub offset_ms: u32,
    pub level: String,
    pub target: String,
    pub msg: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub fields: serde_json::Value,
}

/// A full at-rest analytics snapshot — one JSON blob the mesofact site renders.
///
/// Carries all three surfaces (summary / timeseries / events) so the consumer
/// fetches exactly one object per pointer read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalyticsSnapshot {
    /// Wall-clock epoch millis this snapshot was produced (for staleness display).
    pub generated_at_ms: u64,
    /// Lower `offset_ms` bound of the aggregated corpus (always 0 today).
    pub window_start_ms: u64,
    /// Upper `offset_ms` bound of the aggregated corpus (= `retention_ms`).
    pub window_end_ms: u64,
    /// Total events aggregated across every machine's shards in the window.
    pub total_events: u64,
    /// Event counts by level, descending by count then level.
    pub by_level: Vec<SnapshotLevelCount>,
    /// Echo of the timeseries bucketing dimension actually used.
    pub group_by: String,
    /// Timeseries buckets, descending by count then key.
    pub timeseries: Vec<SnapshotBucket>,
    /// Most-recent event rows (>= `min_level`), capped at `event_limit`.
    pub events: Vec<SnapshotEvent>,
}

/// The mutable pointer object — names the current content-addressed blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotPointer {
    /// SHA-256 hex of the blob body.
    pub hash: String,
    /// Full R2 key of the blob (`analytics/snapshots/<hash>.json`).
    pub blob_key: String,
    pub generated_at_ms: u64,
    pub total_events: u64,
}

// ─── Config ───────────────────────────────────────────────────────────────────

/// Configuration for the snapshot producer.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Machine ids whose shards to aggregate (each keyed `events/<id>/<day>.parquet`).
    /// A single-node deployment passes one; a coordinator-side producer passes
    /// the whole inventory.
    pub machines: Vec<String>,
    /// Upper `offset_ms` bound of the long-tier corpus — the same value threaded
    /// into [`crate::service::Scryer::with_long_tier`] as the tier boundary.
    pub retention_ms: u64,
    /// Timeseries bucketing dimension: `"hour"` (default), `"level"`, `"target"`.
    pub group_by: String,
    /// Cap on recent event rows carried in the snapshot.
    pub event_limit: usize,
    /// Minimum level for the recent-events surface (`"trace"`..`"fatal"`).
    pub min_level: String,
    /// Producer cadence.
    pub interval: Duration,
}

impl SnapshotConfig {
    /// New config for the given machines + retention boundary, other knobs default.
    pub fn new(machines: Vec<String>, retention_ms: u64) -> Self {
        Self {
            machines,
            retention_ms,
            group_by: "hour".to_string(),
            event_limit: DEFAULT_EVENT_LIMIT,
            min_level: "info".to_string(),
            interval: DEFAULT_SNAPSHOT_INTERVAL,
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_group_by(mut self, group_by: impl Into<String>) -> Self {
        self.group_by = group_by.into();
        self
    }

    pub fn with_min_level(mut self, min_level: impl Into<String>) -> Self {
        self.min_level = min_level.into();
        self
    }

    pub fn with_event_limit(mut self, limit: usize) -> Self {
        self.event_limit = limit;
        self
    }
}

// ─── Producer ─────────────────────────────────────────────────────────────────

/// Reads the long-tier Parquet corpus and publishes analytics snapshots to R2.
///
/// Construct with the same [`ObjectStore`] the promotion consumer writes shards
/// to, then either call [`SnapshotProducer::run_once`] once or
/// [`SnapshotProducer::spawn`] to run the interval loop in the background.
pub struct SnapshotProducer {
    object_store: Arc<dyn ObjectStore>,
    cfg: SnapshotConfig,
}

impl SnapshotProducer {
    pub fn new(object_store: Arc<dyn ObjectStore>, cfg: SnapshotConfig) -> Self {
        Self { object_store, cfg }
    }

    /// Aggregate the long-tier corpus across every configured machine into a
    /// single [`AnalyticsSnapshot`]. Pure read — writes nothing.
    pub fn build_snapshot(&self) -> Result<AnalyticsSnapshot, SnapshotError> {
        let until_ms = self.cfg.retention_ms;

        // Reuse LongTierStore's tested Parquet-read + shard-key scheme by
        // instantiating a per-machine view over the shared object store.
        let mut events: Vec<ScopedRow> = Vec::new();
        for machine in &self.cfg.machines {
            let lt = LongTierStore::new(
                LongTierConfig { machine_id: machine.clone(), retention_ms: self.cfg.retention_ms },
                Arc::clone(&self.object_store),
            );
            events.extend(lt.query_range(None, 0, until_ms)?);
        }

        let total_events = events.len() as u64;
        let by_level = aggregate_by_level(&events);
        let timeseries = aggregate_buckets(&events, &self.cfg.group_by);
        let recent = recent_events(&events, &self.cfg.min_level, self.cfg.event_limit);

        Ok(AnalyticsSnapshot {
            generated_at_ms: now_epoch_ms(),
            window_start_ms: 0,
            window_end_ms: until_ms,
            total_events,
            by_level,
            group_by: self.cfg.group_by.clone(),
            timeseries,
            events: recent,
        })
    }

    /// Publish a snapshot to R2: write the content-addressed blob first, then
    /// flip the pointer. Blob-before-pointer ordering means a reader always sees
    /// a pointer that references bytes already present. Returns the blob hash.
    pub fn publish(&self, snapshot: &AnalyticsSnapshot) -> Result<String, SnapshotError> {
        let body = serde_json::to_vec(snapshot)?;
        let hash = sha256_hex(&body);
        let blob_key = format!("{SNAPSHOT_PREFIX}{hash}.json");

        // Idempotent: an unchanged corpus produces an identical blob and an
        // overwrite of the same bytes. The pointer flip is what publishes it.
        self.object_store.put(&blob_key, body)?;

        let pointer = SnapshotPointer {
            hash: hash.clone(),
            blob_key,
            generated_at_ms: snapshot.generated_at_ms,
            total_events: snapshot.total_events,
        };
        self.object_store.put(POINTER_KEY, serde_json::to_vec(&pointer)?)?;
        Ok(hash)
    }

    /// Build + publish one snapshot; returns the published blob hash.
    ///
    /// **Blocking**: touches the object store (R2 uses blocking reqwest). Call
    /// from [`tokio::task::spawn_blocking`] in async contexts —
    /// [`SnapshotProducer::spawn`] does exactly that.
    pub fn run_once(&self) -> Result<String, SnapshotError> {
        let snapshot = self.build_snapshot()?;
        self.publish(&snapshot)
    }

    /// Spawn the interval loop. Each tick runs [`run_once`](Self::run_once) on a
    /// blocking thread; errors are logged and swallowed (best-effort, retried
    /// next tick) so a wedged R2 never crashes the daemon. Runs until the handle
    /// is dropped/aborted or the process exits.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        let producer = Arc::new(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(producer.cfg.interval);
            loop {
                ticker.tick().await;
                let p = Arc::clone(&producer);
                match tokio::task::spawn_blocking(move || p.run_once()).await {
                    Ok(Ok(hash)) => {
                        eprintln!("scryer snapshot: published analytics snapshot {hash}");
                    }
                    Ok(Err(e)) => eprintln!("scryer snapshot: publish error: {e}"),
                    Err(e) => eprintln!("scryer snapshot: pass panicked: {e}"),
                }
            }
        })
    }
}

// ─── Aggregation helpers ──────────────────────────────────────────────────────

/// Numeric rank for a level string, for `min_level` filtering. Unknown → info.
fn level_rank(level: &str) -> u8 {
    match level {
        "trace" => 0,
        "debug" => 1,
        "info" => 2,
        "warn" => 3,
        "error" => 4,
        "fatal" => 5,
        _ => 2,
    }
}

/// Count events by level, descending by count then level.
fn aggregate_by_level(events: &[ScopedRow]) -> Vec<SnapshotLevelCount> {
    let mut counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for (_scope, ev) in events {
        *counts.entry(ev.level.as_str().to_string()).or_insert(0) += 1;
    }
    let mut rows: Vec<SnapshotLevelCount> = counts
        .into_iter()
        .map(|(level, count)| SnapshotLevelCount { level, count })
        .collect();
    rows.sort_by(|a, b| b.count.cmp(&a.count).then(a.level.cmp(&b.level)));
    rows
}

/// Group events into timeseries buckets, matching `Scryer::aggregate` key
/// formats exactly, descending by count then key.
fn aggregate_buckets(events: &[ScopedRow], group_by: &str) -> Vec<SnapshotBucket> {
    let mut counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for (_scope, ev) in events {
        let key = match group_by {
            "level" => ev.level.as_str().to_string(),
            "target" => ev.target.splitn(2, "::").next().unwrap_or(&ev.target).to_string(),
            "hour" => format!("h{}", ev.offset_ms / 3_600_000),
            _ => ev.level.as_str().to_string(),
        };
        *counts.entry(key).or_insert(0) += 1;
    }
    let mut buckets: Vec<SnapshotBucket> = counts
        .into_iter()
        .map(|(key, count)| SnapshotBucket { key, count })
        .collect();
    buckets.sort_by(|a, b| b.count.cmp(&a.count).then(a.key.cmp(&b.key)));
    buckets
}

/// Most-recent events at or above `min_level`, most-recent (highest offset)
/// first, capped at `limit`.
fn recent_events(events: &[ScopedRow], min_level: &str, limit: usize) -> Vec<SnapshotEvent> {
    let floor = level_rank(min_level);
    let mut rows: Vec<&ScopedRow> = events
        .iter()
        .filter(|(_scope, ev)| level_rank(ev.level.as_str()) >= floor)
        .collect();
    // Most recent within the per-scope offset model = highest offset_ms.
    rows.sort_by(|(_, a), (_, b)| b.offset_ms.cmp(&a.offset_ms));
    rows.into_iter()
        .take(limit)
        .map(|(scope, ev)| SnapshotEvent {
            offset_ms: ev.offset_ms,
            level: ev.level.as_str().to_string(),
            target: ev.target.clone(),
            msg: ev.msg.clone(),
            scope_kind: scope.kind_str().to_string(),
            scope_id: scope.id_str(),
            fields: ev.fields.clone(),
        })
        .collect()
}

fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::long_tier::{InMemoryObjectStore, LongTierConfig, LongTierStore, MS_PER_DAY, ObjectStore};
    use crate::service::{Scryer, ScryerConfig};
    use observation::{Event, EventScope, EventSource, Level, TaskRunId};
    use serde_json::json;
    use tempfile::TempDir;
    use workload_spec::MeshIdent;

    const RETENTION_MS: u64 = 7 * MS_PER_DAY;

    fn make_event(run_id: &TaskRunId, seq: u32, offset_ms: u32, level: Level) -> Event {
        Event {
            run_id: run_id.clone(),
            seq,
            offset_ms,
            level,
            target: format!("cargo::stage{}", seq % 3),
            msg: format!("msg {seq}"),
            fields: json!({ "seq": seq }),
            anchor: None,
            source: EventSource::Synth,
        }
    }

    /// Promote `events` for `scope` into `machine`'s Parquet shards on `store`.
    fn promote_into(
        store: &Arc<InMemoryObjectStore>,
        machine: &str,
        scope: &EventScope,
        events: Vec<Event>,
    ) {
        let dir = TempDir::new().unwrap();
        let cfg = ScryerConfig::new(dir.path().join("events.db"));
        let scryer = Scryer::new(cfg, None).unwrap();
        let items: Vec<(EventScope, Event)> =
            events.into_iter().map(|e| (scope.clone(), e)).collect();
        scryer.store().insert_events(&items).unwrap();
        let lt = LongTierStore::new(
            LongTierConfig { machine_id: machine.to_string(), retention_ms: RETENTION_MS },
            Arc::clone(store) as Arc<dyn ObjectStore>,
        );
        // All events sit at offset = 1 day < cutoff, so every one promotes.
        lt.rollover(scryer.store(), RETENTION_MS).unwrap();
    }

    /// A single-machine corpus aggregates into by_level + hour buckets + events,
    /// and publishes a content-addressed blob + a pointer that references it.
    #[test]
    fn build_and_publish_single_machine() {
        let store = Arc::new(InMemoryObjectStore::new());
        let scope = EventScope::Service(MeshIdent("svc.prod".to_string()));
        let run_id = TaskRunId::new();
        // 3 warn + 2 error + 1 info, all at offset = 1 day (hour bucket h24).
        let one_day = MS_PER_DAY as u32;
        let events = vec![
            make_event(&run_id, 0, one_day, Level::Warn),
            make_event(&run_id, 1, one_day, Level::Warn),
            make_event(&run_id, 2, one_day, Level::Warn),
            make_event(&run_id, 3, one_day, Level::Error),
            make_event(&run_id, 4, one_day, Level::Error),
            make_event(&run_id, 5, one_day, Level::Info),
        ];
        promote_into(&store, "m1", &scope, events);

        let producer = SnapshotProducer::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            SnapshotConfig::new(vec!["m1".to_string()], RETENTION_MS),
        );
        let snap = producer.build_snapshot().unwrap();

        assert_eq!(snap.total_events, 6);
        // by_level: warn(3) then error(2) then info(1), descending by count.
        assert_eq!(
            snap.by_level,
            vec![
                SnapshotLevelCount { level: "warn".into(), count: 3 },
                SnapshotLevelCount { level: "error".into(), count: 2 },
                SnapshotLevelCount { level: "info".into(), count: 1 },
            ]
        );
        // Every event is at 1 day = 24h → a single "h24" bucket.
        assert_eq!(snap.group_by, "hour");
        assert_eq!(snap.timeseries, vec![SnapshotBucket { key: "h24".into(), count: 6 }]);
        // min_level defaults to info → all 6 rows kept.
        assert_eq!(snap.events.len(), 6);
        assert_eq!(snap.window_end_ms, RETENTION_MS);

        // Publish writes the blob then the pointer.
        let hash = producer.publish(&snap).unwrap();
        let blob_key = format!("{SNAPSHOT_PREFIX}{hash}.json");
        assert!(store.contains_key(&blob_key), "content-addressed blob present");
        assert!(store.contains_key(POINTER_KEY), "pointer present");

        // Pointer references the blob; blob round-trips to the same snapshot.
        let ptr: SnapshotPointer =
            serde_json::from_slice(&store.get(POINTER_KEY).unwrap().unwrap()).unwrap();
        assert_eq!(ptr.hash, hash);
        assert_eq!(ptr.blob_key, blob_key);
        assert_eq!(ptr.total_events, 6);
        let round_trip: AnalyticsSnapshot =
            serde_json::from_slice(&store.get(&blob_key).unwrap().unwrap()).unwrap();
        assert_eq!(round_trip, snap);
    }

    /// Shards from two machines merge into one snapshot.
    #[test]
    fn aggregates_across_machines() {
        let store = Arc::new(InMemoryObjectStore::new());
        let one_day = MS_PER_DAY as u32;
        let run_a = TaskRunId::new();
        let run_b = TaskRunId::new();
        promote_into(
            &store,
            "m1",
            &EventScope::Service(MeshIdent("svc.a".to_string())),
            vec![make_event(&run_a, 0, one_day, Level::Error)],
        );
        promote_into(
            &store,
            "m2",
            &EventScope::Service(MeshIdent("svc.b".to_string())),
            vec![
                make_event(&run_b, 0, one_day, Level::Error),
                make_event(&run_b, 1, one_day, Level::Info),
            ],
        );

        let producer = SnapshotProducer::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            SnapshotConfig::new(vec!["m1".to_string(), "m2".to_string()], RETENTION_MS),
        );
        let snap = producer.build_snapshot().unwrap();
        assert_eq!(snap.total_events, 3);
        assert_eq!(
            snap.by_level,
            vec![
                SnapshotLevelCount { level: "error".into(), count: 2 },
                SnapshotLevelCount { level: "info".into(), count: 1 },
            ]
        );
        // Both scopes surface in the events.
        let mut scope_ids: Vec<&str> = snap.events.iter().map(|e| e.scope_id.as_str()).collect();
        scope_ids.sort();
        scope_ids.dedup();
        assert_eq!(scope_ids, vec!["svc.a", "svc.b"]);
    }

    /// `min_level` drops rows below the floor from the events surface, but not
    /// from the by_level rollup (which always counts everything).
    #[test]
    fn min_level_filters_events_not_rollup() {
        let store = Arc::new(InMemoryObjectStore::new());
        let scope = EventScope::Service(MeshIdent("svc.filter".to_string()));
        let run_id = TaskRunId::new();
        let one_day = MS_PER_DAY as u32;
        promote_into(
            &store,
            "m1",
            &scope,
            vec![
                make_event(&run_id, 0, one_day, Level::Debug),
                make_event(&run_id, 1, one_day, Level::Info),
                make_event(&run_id, 2, one_day, Level::Error),
            ],
        );

        let producer = SnapshotProducer::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            SnapshotConfig::new(vec!["m1".to_string()], RETENTION_MS).with_min_level("warn"),
        );
        let snap = producer.build_snapshot().unwrap();
        // Rollup counts all 3.
        assert_eq!(snap.total_events, 3);
        // Events surface keeps only >= warn → just the one error.
        assert_eq!(snap.events.len(), 1);
        assert_eq!(snap.events[0].level, "error");
    }

    /// An empty corpus produces a valid, empty snapshot and still publishes a
    /// pointer (a reader always finds a current snapshot).
    #[test]
    fn empty_corpus_publishes_empty_snapshot() {
        let store = Arc::new(InMemoryObjectStore::new());
        let producer = SnapshotProducer::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            SnapshotConfig::new(vec!["m1".to_string()], RETENTION_MS),
        );
        let snap = producer.build_snapshot().unwrap();
        assert_eq!(snap.total_events, 0);
        assert!(snap.by_level.is_empty());
        assert!(snap.timeseries.is_empty());
        assert!(snap.events.is_empty());

        let hash = producer.publish(&snap).unwrap();
        assert!(store.contains_key(POINTER_KEY));
        assert!(store.contains_key(&format!("{SNAPSHOT_PREFIX}{hash}.json")));
    }

    /// `run_once` builds and publishes in one call; the pointer's total matches.
    #[test]
    fn run_once_builds_and_publishes() {
        let store = Arc::new(InMemoryObjectStore::new());
        let scope = EventScope::Service(MeshIdent("svc.once".to_string()));
        let run_id = TaskRunId::new();
        promote_into(
            &store,
            "m1",
            &scope,
            vec![make_event(&run_id, 0, MS_PER_DAY as u32, Level::Info)],
        );
        let producer = SnapshotProducer::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            SnapshotConfig::new(vec!["m1".to_string()], RETENTION_MS),
        );
        let hash = producer.run_once().unwrap();
        let ptr: SnapshotPointer =
            serde_json::from_slice(&store.get(POINTER_KEY).unwrap().unwrap()).unwrap();
        assert_eq!(ptr.hash, hash);
        assert_eq!(ptr.total_events, 1);
    }
}
