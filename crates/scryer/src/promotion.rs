//! Long-tier promotion consumer — R556-F5.
//!
//! [`LongTierStore::rollover`] is the *primitive* that moves aged events from
//! short-disk SQLite into per-day Parquet shards in an [`ObjectStore`] (R2 /
//! MinIO). Until now nothing drove it — the long tier was designed (A049
//! §Storage tiers) but unconsumed. This module is the missing consumer: a
//! background loop that calls `rollover` on an interval so short-disk stays
//! bounded and the R2 shards accrue as the at-rest snapshot source Mode-2
//! (R556-F6) reads from.
//!
//! # Retention model (offset_ms space)
//!
//! `rollover(store, cutoff_ms)` promotes every event with `offset_ms <
//! cutoff_ms`. `offset_ms` is the store's wall-clock-relative "ms since run
//! start", so within a scope the *earlier* events carry the *smaller* offsets.
//! The consumer passes a fixed `retention_ms` cutoff — the same value threaded
//! into [`Scryer::with_long_tier`] as the tier boundary — which matches the
//! primitive's existing contract and the `long_tier::rollover` tests.
//!
//! A genuine rolling window (`cutoff = current_max_offset - retention_ms`)
//! would track a forever-running service's high-water mark instead of a fixed
//! offset; that is a refinement of the primitive, not of the consumer, and is
//! left as a follow-up (the offset_ms model is muddy across scopes with
//! different run-start epochs). See the R556-F5 cleanup note.
//!
//! [`ObjectStore`]: crate::long_tier::ObjectStore

use std::sync::Arc;
use std::time::Duration;

use crate::long_tier::{LongTierError, LongTierStore};
use crate::service::Scryer;

/// Default promotion cadence: once an hour. Cheap — a pass over already-aged
/// events is bounded by the short-disk retention window, and produces zero
/// object-store traffic when nothing crossed the cutoff.
pub const DEFAULT_PROMOTE_INTERVAL: Duration = Duration::from_secs(3600);

/// Configuration for the promotion loop.
#[derive(Debug, Clone)]
pub struct PromotionConfig {
    /// How often to run a promotion pass.
    pub interval: Duration,
    /// Cutoff in `offset_ms` space: events with `offset_ms < retention_ms` are
    /// promoted to Parquet. Should equal the `short_disk_retention_ms` passed
    /// to [`Scryer::with_long_tier`] so the query router and the promoter agree
    /// on the tier boundary.
    pub retention_ms: u64,
}

impl PromotionConfig {
    /// New config with the default interval and the given retention cutoff.
    pub fn new(retention_ms: u64) -> Self {
        Self { interval: DEFAULT_PROMOTE_INTERVAL, retention_ms }
    }

    /// Builder-style interval override.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
}

/// Drives [`LongTierStore::rollover`] on an interval.
///
/// Construct with the same [`Scryer`] whose short-disk store feeds the tier and
/// the [`LongTierStore`] wired into it via [`Scryer::with_long_tier`], then call
/// [`PromotionConsumer::spawn`] to run the loop in the background.
pub struct PromotionConsumer {
    scryer: Arc<Scryer>,
    long_tier: Arc<LongTierStore>,
    cfg: PromotionConfig,
}

impl PromotionConsumer {
    pub fn new(
        scryer: Arc<Scryer>,
        long_tier: Arc<LongTierStore>,
        cfg: PromotionConfig,
    ) -> Self {
        Self { scryer, long_tier, cfg }
    }

    /// Run a single promotion pass and return the number of events promoted.
    ///
    /// **Blocking**: touches SQLite and the object store (R2 uses blocking
    /// reqwest). Call it from [`tokio::task::spawn_blocking`] in async contexts
    /// — [`PromotionConsumer::spawn`] does exactly that.
    pub fn run_once(&self) -> Result<usize, LongTierError> {
        self.long_tier.rollover(self.scryer.store(), self.cfg.retention_ms)
    }

    /// Spawn the interval loop.
    ///
    /// Each tick runs [`run_once`](Self::run_once) on a blocking thread; errors
    /// (object-store unreachable, SQLite busy, a panic in the pass) are logged
    /// and swallowed — promotion is best-effort by design, and a failed pass is
    /// retried on the next tick. The task runs until the returned handle is
    /// dropped/aborted or the process exits.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        let consumer = Arc::new(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(consumer.cfg.interval);
            loop {
                ticker.tick().await;
                let c = Arc::clone(&consumer);
                match tokio::task::spawn_blocking(move || c.run_once()).await {
                    Ok(Ok(0)) => {}
                    Ok(Ok(n)) => {
                        eprintln!("scryer promotion: {n} events promoted to long tier");
                    }
                    Ok(Err(e)) => eprintln!("scryer promotion: rollover error: {e}"),
                    Err(e) => eprintln!("scryer promotion: pass panicked: {e}"),
                }
            }
        })
    }
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

    fn make_event(run_id: &TaskRunId, seq: u32, offset_ms: u32, level: Level) -> Event {
        Event {
            run_id: run_id.clone(),
            seq,
            offset_ms,
            level,
            target: format!("test::{seq}"),
            msg: format!("msg {seq}"),
            fields: json!({ "seq": seq }),
            anchor: None,
            source: EventSource::Synth,
        }
    }

    fn setup(dir: &TempDir) -> (Arc<Scryer>, Arc<LongTierStore>, Arc<InMemoryObjectStore>) {
        let cfg = ScryerConfig::new(dir.path().join("events.db"));
        let retention_ms = 7 * MS_PER_DAY;
        let scryer = Arc::new(Scryer::new(cfg, None).unwrap());
        let obj_store = Arc::new(InMemoryObjectStore::new());
        let lt_cfg = LongTierConfig { machine_id: "m1".to_string(), retention_ms };
        let lt = Arc::new(LongTierStore::new(
            lt_cfg,
            Arc::clone(&obj_store) as Arc<dyn ObjectStore>,
        ));
        (scryer, lt, obj_store)
    }

    /// A single promotion pass moves aged events out of short-disk into Parquet
    /// shards; recent events stay put.
    #[test]
    fn run_once_promotes_aged_events() {
        let dir = TempDir::new().unwrap();
        let (scryer, lt, obj_store) = setup(&dir);
        let retention_ms = 7 * MS_PER_DAY;

        let scope = EventScope::Service(MeshIdent("svc.prod".to_string()));
        let run_id = TaskRunId::new();

        // 4 aged events (offset = 1 day, below the 7-day cutoff).
        let aged: Vec<(EventScope, Event)> = (0u32..4)
            .map(|i| (scope.clone(), make_event(&run_id, i, MS_PER_DAY as u32, Level::Warn)))
            .collect();
        // 1 recent event (offset = 10 days, above the cutoff).
        let recent = vec![(
            scope.clone(),
            make_event(&run_id, 99, (10 * MS_PER_DAY) as u32, Level::Info),
        )];
        scryer.store().insert_events(&aged).unwrap();
        scryer.store().insert_events(&recent).unwrap();
        assert_eq!(scryer.store().count().unwrap(), 5);

        let consumer = PromotionConsumer::new(
            Arc::clone(&scryer),
            Arc::clone(&lt),
            PromotionConfig::new(retention_ms),
        );
        let promoted = consumer.run_once().unwrap();

        assert_eq!(promoted, 4, "4 aged events promoted");
        assert_eq!(scryer.store().count().unwrap(), 1, "recent event remains");
        assert!(
            obj_store.contains_key("events/m1/1.parquet"),
            "day-1 Parquet shard written to the object store"
        );
    }

    /// A pass with nothing past the cutoff promotes zero and writes no shards.
    #[test]
    fn run_once_noop_when_nothing_aged() {
        let dir = TempDir::new().unwrap();
        let (scryer, lt, obj_store) = setup(&dir);
        let retention_ms = 7 * MS_PER_DAY;

        let scope = EventScope::Service(MeshIdent("svc.fresh".to_string()));
        let run_id = TaskRunId::new();
        let fresh = vec![(
            scope.clone(),
            make_event(&run_id, 0, (10 * MS_PER_DAY) as u32, Level::Info),
        )];
        scryer.store().insert_events(&fresh).unwrap();

        let consumer = PromotionConsumer::new(scryer.clone(), lt, PromotionConfig::new(retention_ms));
        assert_eq!(consumer.run_once().unwrap(), 0, "nothing past the cutoff");
        assert_eq!(scryer.store().count().unwrap(), 1);
        assert!(obj_store.keys().is_empty(), "no shards written");
    }

    /// The spawned loop performs at least one pass (interval's first tick fires
    /// immediately) and promotes aged events without external prodding.
    #[tokio::test]
    async fn spawned_loop_runs_a_pass() {
        let dir = TempDir::new().unwrap();
        let (scryer, lt, obj_store) = setup(&dir);
        let retention_ms = 7 * MS_PER_DAY;

        let scope = EventScope::Service(MeshIdent("svc.loop".to_string()));
        let run_id = TaskRunId::new();
        let aged: Vec<(EventScope, Event)> = (0u32..3)
            .map(|i| (scope.clone(), make_event(&run_id, i, MS_PER_DAY as u32, Level::Error)))
            .collect();
        scryer.store().insert_events(&aged).unwrap();

        let cfg = PromotionConfig::new(retention_ms).with_interval(Duration::from_millis(20));
        let handle = PromotionConsumer::new(scryer.clone(), lt, cfg).spawn();

        // Poll for the first pass to land (first tick is immediate; give the
        // blocking rollover a moment to complete).
        let mut promoted = false;
        for _ in 0..50 {
            if obj_store.contains_key("events/m1/1.parquet") {
                promoted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        handle.abort();
        assert!(promoted, "spawned loop promoted aged events to a shard");
        assert_eq!(scryer.store().count().unwrap(), 0, "short-disk drained");
    }
}
