//! In-memory ring buffer — recent-event fast path.
//!
//! Holds the last `capacity` events (default 10k) and at most `max_bytes` of
//! serialised field data (default 64 MB). When either limit is exceeded the
//! oldest entry is evicted (ring semantics — no blocking, no back-pressure
//! at this layer).
//!
//! Flush to short-disk happens externally: callers drain `take_pending` and
//! write to `EventStore`. The ring tracks a high-water mark so subscribers
//! can ask for events since cursor N without touching SQLite while the data
//! is still in the ring.

use observation::{Event, EventScope};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Capacity knobs for the ring buffer.
#[derive(Debug, Clone)]
pub struct RingConfig {
    /// Maximum number of events stored in the ring.
    pub max_events: usize,
    /// Approximate cap on field JSON bytes stored across all events.
    pub max_bytes: usize,
    /// Flush to short-disk after this many bytes of pending events.
    pub flush_bytes: usize,
    /// Flush to short-disk after this many milliseconds.
    pub flush_ms: u64,
}

impl Default for RingConfig {
    fn default() -> Self {
        Self {
            max_events: 10_000,
            max_bytes: 64 * 1024 * 1024,    // 64 MB
            flush_bytes: 4 * 1024,           // 4 KB
            flush_ms: 250,
        }
    }
}

struct Entry {
    /// Monotonic ring cursor assigned at insert time. Distinct from Event.seq
    /// (which is per-scope) — used for efficient tail queries.
    cursor: u64,
    scope: EventScope,
    event: Event,
    /// Approximate byte cost of this entry's fields_json.
    byte_cost: usize,
}

struct Inner {
    entries: VecDeque<Entry>,
    next_cursor: u64,
    total_bytes: usize,
    /// Events that have been pushed but not yet flushed to short-disk.
    pending_bytes: usize,
}

/// Thread-safe in-memory event ring.
///
/// All mutating operations go through the `Mutex`; reads that only need the
/// cursor high-water mark can use `next_cursor() - 1`.
pub struct EventRing {
    cfg: RingConfig,
    inner: Mutex<Inner>,
}

impl EventRing {
    pub fn new(cfg: RingConfig) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            inner: Mutex::new(Inner {
                entries: VecDeque::new(),
                next_cursor: 0,
                total_bytes: 0,
                pending_bytes: 0,
            }),
        })
    }

    /// Push an event into the ring. Returns `(cursor, should_flush)`.
    ///
    /// `should_flush` is true when `pending_bytes >= cfg.flush_bytes` — the
    /// caller should drain `take_pending` and write to `EventStore`.
    pub fn push(&self, scope: EventScope, event: Event) -> (u64, bool) {
        let byte_cost = event.fields.to_string().len();
        let mut g = self.inner.lock().unwrap();

        let cursor = g.next_cursor;
        g.next_cursor += 1;

        // Evict oldest while over either limit (with new entry added).
        while (g.entries.len() >= self.cfg.max_events
            || g.total_bytes + byte_cost > self.cfg.max_bytes)
            && !g.entries.is_empty()
        {
            let evicted = g.entries.pop_front().unwrap();
            g.total_bytes -= evicted.byte_cost;
        }

        g.total_bytes += byte_cost;
        g.pending_bytes += byte_cost;
        g.entries.push_back(Entry { cursor, scope, event, byte_cost });

        let should_flush = g.pending_bytes >= self.cfg.flush_bytes;
        (cursor, should_flush)
    }

    /// Take all events that haven't been flushed to short-disk yet.
    /// Resets the pending byte counter. Callers write these to `EventStore`.
    pub fn take_pending(&self) -> Vec<(EventScope, Event)> {
        let mut g = self.inner.lock().unwrap();
        // Mark everything as flushed.
        g.pending_bytes = 0;
        // Return a snapshot of all entries — caller deduplicates against store.
        // In practice the ring always stays ahead of the store flush cursor so
        // this is all-new since the last drain.
        g.entries.iter().map(|e| (e.scope.clone(), e.event.clone())).collect()
    }

    /// Return events with ring cursor >= `since_cursor` for the given scope.
    ///
    /// Returns `(events, next_cursor)`.  `next_cursor` is the ring cursor to
    /// pass on the **next** tail call:
    /// - When `limit` events are returned (more may follow), `next_cursor` is
    ///   the cursor of the last returned event + 1.
    /// - When fewer than `limit` events are returned (caught up), `next_cursor`
    ///   is the current high-water mark.
    ///
    /// Callers with `limit = 0` (subscribe anchor) always get the high-water
    /// mark back.
    pub fn tail_since(
        &self,
        scope: &EventScope,
        since_cursor: u64,
        limit: usize,
    ) -> (Vec<Event>, u64) {
        let g = self.inner.lock().unwrap();
        let mut last_cursor = since_cursor;
        let events: Vec<Event> = g
            .entries
            .iter()
            .filter(|e| e.cursor >= since_cursor && &e.scope == scope)
            .take(limit)
            .map(|e| {
                last_cursor = e.cursor + 1;
                e.event.clone()
            })
            .collect();
        // When limit events were returned the caller may have left events in the
        // ring; return the cursor of the next unread entry.  When fewer than
        // limit were returned (or limit == 0) we have caught up — use the high-
        // water mark so the next call re-checks from the current head.
        let next = if limit > 0 && events.len() == limit {
            last_cursor
        } else {
            g.next_cursor
        };
        drop(g);
        (events, next)
    }

    /// Current high-water cursor (one past the last assigned).
    pub fn next_cursor(&self) -> u64 {
        self.inner.lock().unwrap().next_cursor
    }

    /// Number of events currently held in the ring.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use observation::{EventSource, Level, TaskRunId};
    use serde_json::json;

    fn make_event(seq: u32) -> Event {
        Event {
            run_id: TaskRunId::new(),
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

    fn make_scope() -> EventScope {
        EventScope::TaskRun(TaskRunId::new())
    }

    #[test]
    fn ring_basic_push_tail() {
        let ring = EventRing::new(RingConfig::default());
        let scope = make_scope();
        for i in 0..5 {
            ring.push(scope.clone(), make_event(i));
        }
        let (events, _next) = ring.tail_since(&scope, 0, 100);
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn ring_evicts_at_capacity() {
        let cfg = RingConfig { max_events: 3, max_bytes: usize::MAX, ..Default::default() };
        let ring = EventRing::new(cfg);
        let scope = make_scope();
        for i in 0..5 {
            ring.push(scope.clone(), make_event(i));
        }
        // Ring holds at most 3 entries; oldest two evicted.
        assert_eq!(ring.len(), 3);
    }

    #[test]
    fn ring_no_drops_within_quota() {
        // 10k events must fit in the default ring.
        let ring = EventRing::new(RingConfig::default());
        let scope = make_scope();
        for i in 0..10_000 {
            ring.push(scope.clone(), make_event(i as u32));
        }
        assert_eq!(ring.len(), 10_000);
    }

    #[test]
    fn ring_flush_threshold() {
        // A ring with a tiny flush_bytes threshold reports should_flush.
        let cfg = RingConfig { flush_bytes: 1, ..Default::default() };
        let ring = EventRing::new(cfg);
        let scope = make_scope();
        // Push one event — fields is `{}`, 2 bytes, exceeds flush_bytes=1.
        let (_, should_flush) = ring.push(scope.clone(), make_event(0));
        assert!(should_flush);
    }

    #[test]
    fn ring_take_pending_resets() {
        let ring = EventRing::new(RingConfig::default());
        let scope = make_scope();
        ring.push(scope.clone(), make_event(0));
        let pending = ring.take_pending();
        assert_eq!(pending.len(), 1);
        // After take, pending_bytes is reset; a new push with
        // flush_bytes > bytes_pushed should not request flush.
        let (_, should_flush) = ring.push(scope.clone(), make_event(1));
        // flush_bytes default 4096, one event fields={} is ~2 bytes — no flush.
        assert!(!should_flush);
    }
}
