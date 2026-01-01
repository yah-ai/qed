//! Per-`MeshIdent` event quota — rate limiting with drop-count Synth events.
//!
//! Each service identity gets an independent sliding-window budget of
//! `quota_per_second` events (default 1000 per arch doc Open questions).
//! When the budget is exceeded the excess is counted; on the next window
//! boundary a `Synth` drop-count event is injected so agents can see the
//! discontinuity without losing visibility.
//!
//! Reservoir sampling is not yet implemented (deferred); the current policy
//! is "keep the first N, drop the rest and report". This is sufficient for
//! P1 — revisit if a tail-latency or burst-smoothing requirement materialises.

use observation::{Event, EventScope, EventSource, Level, TaskRunId};
use serde_json::json;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use workload_spec::MeshIdent;

/// Default per-service events-per-second budget per arch doc §Open questions.
pub const DEFAULT_QUOTA_PER_SECOND: u32 = 1_000;

/// Per-identity window state.
struct IdentState {
    window_start: Instant,
    events_this_window: u32,
    dropped_this_window: u32,
    /// Monotonic sequence counter for synthetic drop events, allocated from
    /// u32::MAX downward to avoid collision with the beholder seq stream.
    synth_seq: u32,
    /// Stable per-ident anchor for `Event.run_id`.
    run_id_anchor: TaskRunId,
}

impl IdentState {
    fn new(now: Instant) -> Self {
        Self {
            window_start: now,
            events_this_window: 0,
            dropped_this_window: 0,
            synth_seq: u32::MAX,
            run_id_anchor: TaskRunId::new(),
        }
    }

    fn next_synth_seq(&mut self) -> u32 {
        let s = self.synth_seq;
        self.synth_seq = self.synth_seq.saturating_sub(1);
        s
    }
}

/// Decision returned by [`ServiceQuotaManager::check`].
pub enum QuotaDecision {
    /// Caller may forward the event.
    Allow,
    /// Event exceeds quota — discard.
    Drop,
    /// Window boundary rolled over with pending drops.  Discard the triggering
    /// event; inject the returned Synth event first, then resume normally.
    DropWithSynth { synth: Event, scope: EventScope },
}

/// Per-machine quota manager.  One instance covers all identities on a machine.
pub struct ServiceQuotaManager {
    per_ident: HashMap<String, IdentState>,
    quota_per_second: u32,
}

impl ServiceQuotaManager {
    pub fn new() -> Self {
        Self::with_quota(DEFAULT_QUOTA_PER_SECOND)
    }

    pub fn with_quota(quota_per_second: u32) -> Self {
        Self { per_ident: HashMap::new(), quota_per_second }
    }

    /// Check whether the event for `ident` fits within the quota window.
    ///
    /// Call once per event before pushing to the ring.  If the result is
    /// [`QuotaDecision::DropWithSynth`], push the Synth event first, then
    /// discard the original event for this call.  The next `check` call will
    /// start a fresh window.
    pub fn check(&mut self, ident: &MeshIdent) -> QuotaDecision {
        let now = Instant::now();
        let quota = self
            .per_ident
            .entry(ident.0.clone())
            .or_insert_with(|| IdentState::new(now));

        let window_expired =
            now.duration_since(quota.window_start) >= Duration::from_secs(1);

        if window_expired {
            let dropped = quota.dropped_this_window;
            quota.events_this_window = 0;
            quota.window_start = now;
            quota.dropped_this_window = 0;

            if dropped > 0 {
                let seq = quota.next_synth_seq();
                let run_id = quota.run_id_anchor.clone();
                let synth = Event {
                    run_id,
                    seq,
                    offset_ms: 0,
                    level: Level::Warn,
                    target: ident.0.clone(),
                    msg: format!("scryer: dropped {dropped} events (quota exceeded)"),
                    fields: json!({ "scryer.dropped": dropped }),
                    anchor: None,
                    source: EventSource::Synth,
                };
                return QuotaDecision::DropWithSynth {
                    synth,
                    scope: EventScope::Service(ident.clone()),
                };
            }
        }

        if quota.events_this_window >= self.quota_per_second {
            quota.dropped_this_window += 1;
            QuotaDecision::Drop
        } else {
            quota.events_this_window += 1;
            QuotaDecision::Allow
        }
    }

    /// Reset state for an identity (e.g. on service restart).
    pub fn reset(&mut self, ident: &MeshIdent) {
        self.per_ident.remove(&ident.0);
    }
}

impl Default for ServiceQuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(s: &str) -> MeshIdent {
        MeshIdent(s.to_string())
    }

    #[test]
    fn quota_allows_up_to_budget() {
        let mut mgr = ServiceQuotaManager::with_quota(5);
        let id = ident("svc.a");
        for _ in 0..5 {
            assert!(matches!(mgr.check(&id), QuotaDecision::Allow));
        }
    }

    #[test]
    fn quota_drops_over_budget() {
        let mut mgr = ServiceQuotaManager::with_quota(3);
        let id = ident("svc.a");
        for _ in 0..3 {
            mgr.check(&id);
        }
        assert!(matches!(mgr.check(&id), QuotaDecision::Drop));
        assert!(matches!(mgr.check(&id), QuotaDecision::Drop));
    }

    #[test]
    fn quota_independent_per_ident() {
        let mut mgr = ServiceQuotaManager::with_quota(2);
        let a = ident("svc.a");
        let b = ident("svc.b");
        // Fill svc.a budget.
        mgr.check(&a);
        mgr.check(&a);
        assert!(matches!(mgr.check(&a), QuotaDecision::Drop));
        // svc.b still has budget.
        assert!(matches!(mgr.check(&b), QuotaDecision::Allow));
    }

    #[test]
    fn quota_reset_clears_state() {
        let mut mgr = ServiceQuotaManager::with_quota(1);
        let id = ident("svc.a");
        mgr.check(&id);
        mgr.check(&id); // → Drop
        mgr.reset(&id);
        assert!(matches!(mgr.check(&id), QuotaDecision::Allow));
    }
}
