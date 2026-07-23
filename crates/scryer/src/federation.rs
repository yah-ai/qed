//! Federation layer for cross-machine scryer queries — R093-F4.
//!
//! Shape (arch doc §Federation across machines):
//!   - `FederationRule`  — selects which yubaba-mesh peers to fan out to.
//!   - `FederationPeer`  — abstracts a remote scryer (gRPC in prod, mock in tests).
//!   - `FederationAcl`   — operator-tag guard.  Sits at the yubaba gRPC entry
//!                         point, not in scryer query logic, per the arch doc:
//!                         "Implement via Tailscale-tag check at the yubaba RPC
//!                         entry point, not in scryer code."
//!   - `merge_events`    — merge two time-ordered lists by (offset_ms, seq).
//!   - `federated_events` — local query + fan-out + merge (best-effort).
//!
//! Time ordering is best-effort: clocks on different machines can skew by ~ms,
//! so events that land near-simultaneously can interleave in either order.
//! Agents needing causal order must use correlation IDs (R091-F1).

use crate::service::{EventFilter, ScryerError};
use async_trait::async_trait;
use observation::{Event, EventScope};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

// ─── ScopedEvent ──────────────────────────────────────────────────────────────

/// An event together with the scope it was stored under (R585-F2).
///
/// Scryer is scope-keyed all the way down — `push`, `events`, and the store
/// indexes all take an [`EventScope`] — but the *federation* layer used to drop
/// it: `FederationPeer::events` returned a bare `Vec<Event>`, so a cross-scope
/// rollup came back as an undifferentiated pile and every consumer downstream
/// had to render the scope as blank. `AnalyticsEvent.scope_kind` / `scope_id`
/// were hardcoded empty strings for exactly this reason.
///
/// Carrying the envelope costs one enum per row and makes the cross-scope
/// rollup — the whole point of `scopes: None` — actually usable: a mesh-wide
/// query can now say *which* task run or service each event came from.
///
/// The wire form is `{"scope": …, "event": {…}}`, a named struct rather than a
/// tuple so the JSON stays self-describing for non-Rust readers of
/// `/federate/events`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopedEvent {
    pub scope: EventScope,
    pub event: Event,
}

impl ScopedEvent {
    pub fn new(scope: EventScope, event: Event) -> Self {
        Self { scope, event }
    }

    /// Tag a whole batch from one scope — the shape every scope-keyed query
    /// result takes on its way into a federated merge.
    pub fn tag_all(scope: &EventScope, events: Vec<Event>) -> Vec<Self> {
        events
            .into_iter()
            .map(|event| Self::new(scope.clone(), event))
            .collect()
    }

    /// Drop the envelope. For callers that genuinely don't care about scope
    /// (the scope-keyed `Scryer::events` path, whose caller already knows it).
    pub fn into_events(rows: Vec<Self>) -> Vec<Event> {
        rows.into_iter().map(|r| r.event).collect()
    }
}

// ─── FederationRule ───────────────────────────────────────────────────────────

/// Which peers in the yubaba mesh to include in a fan-out query.
#[derive(Debug, Clone)]
pub enum FederationRule {
    /// All connected peers.
    All,
    /// Only peers whose name contains `tag` (e.g., `"tier=public"`).
    Tag(String),
}

// ─── PeerIdentity + ACL ───────────────────────────────────────────────────────

/// Calling identity presented at the yubaba gRPC entry point.
#[derive(Debug, Clone, Default)]
pub struct PeerIdentity {
    /// Tailscale node tags (e.g. `["tag:operator"]`).
    pub tags: Vec<String>,
}

impl PeerIdentity {
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }
}

/// ACL guard for federated queries.
///
/// Yubaba's gRPC dispatcher injects a concrete impl; scryer query logic is
/// ACL-agnostic so operators can swap policy without recompiling scryer.
pub trait FederationAcl: Send + Sync {
    fn is_authorized(&self, identity: &PeerIdentity) -> bool;
}

/// Allows only identities carrying `tag:operator` (Tailscale operator tag
/// per R091 §three-layer network plane).  Default ACL at the RPC entry point.
pub struct OperatorTagAcl;

impl FederationAcl for OperatorTagAcl {
    fn is_authorized(&self, identity: &PeerIdentity) -> bool {
        identity.tags.iter().any(|t| t == "tag:operator")
    }
}

/// Rejects all identities — used in tests to verify unauthorized peers are
/// refused at the RPC entry point.
pub struct DenyAllAcl;

impl FederationAcl for DenyAllAcl {
    fn is_authorized(&self, _: &PeerIdentity) -> bool {
        false
    }
}

// ─── FederationError ─────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum FederationError {
    #[error("rpc: {0}")]
    Rpc(String),
    #[error("unauthorized: operator tag required for federated queries")]
    Unauthorized,
    #[error("{0}")]
    Scryer(#[from] ScryerError),
}

// ─── FederationPeer ───────────────────────────────────────────────────────────

/// Abstracts a remote scryer peer.
///
/// Production impl: HTTP `POST /federate/events` over the yubaba mesh
/// (see [`crate::federation_http::HttpFederationPeer`]).
/// Test impls: in-process mock that holds a `Vec<ScopedEvent>`.
#[async_trait]
pub trait FederationPeer: Send + Sync {
    /// Stable display name for this peer (e.g. `"yubaba-pdx-1"`).
    fn name(&self) -> &str;
    /// Query events from this peer using `filter`.
    ///
    /// Scope-omitted by design — a peer query is a cross-scope rollup, so each
    /// row carries its own [`EventScope`] in the [`ScopedEvent`] envelope
    /// rather than the caller supplying one (R585-F2).
    async fn events(&self, filter: &EventFilter) -> Result<Vec<ScopedEvent>, FederationError>;
}

// ─── Merge ────────────────────────────────────────────────────────────────────

/// Rows a federated merge can order — anything with an event's
/// `(offset_ms, seq)` position. Implemented for both the bare [`Event`] (the
/// scope-keyed local path, where the caller already knows the scope) and
/// [`ScopedEvent`] (the cross-scope / federated path).
pub trait TimeOrdered {
    fn order_key(&self) -> (u32, u32);
}

impl TimeOrdered for Event {
    fn order_key(&self) -> (u32, u32) {
        (self.offset_ms, self.seq)
    }
}

impl TimeOrdered for ScopedEvent {
    fn order_key(&self) -> (u32, u32) {
        (self.event.offset_ms, self.event.seq)
    }
}

/// Merge two time-ordered slices by `(offset_ms, seq)`.  O(n+m).
///
/// Both inputs must already be sorted; the output is sorted.  Near-simultaneous
/// events from different machines can appear in either order (best-effort clock).
pub fn merge_ordered<T: TimeOrdered>(a: Vec<T>, b: Vec<T>) -> Vec<T> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let mut ia = a.into_iter().peekable();
    let mut ib = b.into_iter().peekable();

    loop {
        match (ia.peek(), ib.peek()) {
            (None, None) => break,
            (Some(_), None) => result.push(ia.next().unwrap()),
            (None, Some(_)) => result.push(ib.next().unwrap()),
            (Some(ea), Some(eb)) => {
                if ea.order_key() <= eb.order_key() {
                    result.push(ia.next().unwrap());
                } else {
                    result.push(ib.next().unwrap());
                }
            }
        }
    }
    result
}

/// [`merge_ordered`] at the bare-`Event` type. Kept as a named function because
/// it is the scope-keyed local merge and reads better at those call sites.
pub fn merge_events(a: Vec<Event>, b: Vec<Event>) -> Vec<Event> {
    merge_ordered(a, b)
}

fn peer_matches_rule(peer: &dyn FederationPeer, rule: &FederationRule) -> bool {
    match rule {
        FederationRule::All => true,
        FederationRule::Tag(tag) => peer.name().contains(tag.as_str()),
    }
}

// ─── Fan-out ──────────────────────────────────────────────────────────────────

/// Fan `filter` out to all peers matching `rule`, merge with `local_events`.
///
/// Every row keeps its [`EventScope`] envelope through the merge (R585-F2), so
/// a mesh-wide rollup can still say which task run or service each event came
/// from. Local callers that hold a bare `Vec<Event>` for a known scope wrap it
/// with [`ScopedEvent::tag_all`] first.
///
/// Peer failures are swallowed — federation is best-effort.  Callers that need
/// error visibility should call `FederationPeer::events` directly.
pub async fn federated_events(
    local_events: Vec<ScopedEvent>,
    peers: &[Arc<dyn FederationPeer>],
    filter: &EventFilter,
    rule: &FederationRule,
) -> Vec<ScopedEvent> {
    let mut result = local_events;
    for peer in peers {
        if peer_matches_rule(peer.as_ref(), rule) {
            if let Ok(remote) = peer.events(filter).await {
                result = merge_ordered(result, remote);
            }
        }
    }
    result
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod acl {
    use super::*;

    /// Unauthorized peer (no operator tag) is rejected at RPC entry.
    /// R093-F4 verify: cargo test yah_scryer::acl
    #[test]
    fn unauthorized_peer_is_rejected() {
        let acl = OperatorTagAcl;
        let identity = PeerIdentity::default(); // no tags
        assert!(!acl.is_authorized(&identity));
    }

    #[test]
    fn operator_tagged_peer_is_allowed() {
        let acl = OperatorTagAcl;
        let identity = PeerIdentity::default().with_tag("tag:operator");
        assert!(acl.is_authorized(&identity));
    }

    #[test]
    fn non_operator_tag_is_rejected() {
        let acl = OperatorTagAcl;
        let identity = PeerIdentity::default().with_tag("tag:workload");
        assert!(!acl.is_authorized(&identity));
    }

    #[test]
    fn deny_all_rejects_any_identity() {
        let acl = DenyAllAcl;
        let identity = PeerIdentity::default().with_tag("tag:operator");
        assert!(!acl.is_authorized(&identity));
    }
}

#[cfg(test)]
mod merge_tests {
    use super::*;
    use observation::{EventSource, Level, TaskRunId};
    use serde_json::json;

    fn ev(offset_ms: u32, seq: u32) -> Event {
        Event {
            run_id: TaskRunId::new(),
            seq,
            offset_ms,
            level: Level::Info,
            target: "t".to_string(),
            msg: format!("e{offset_ms}"),
            fields: json!({}),
            anchor: None,
            source: EventSource::Synth,
        }
    }

    #[test]
    fn merge_interleaves_by_offset() {
        let a = vec![ev(1, 0), ev(3, 1), ev(5, 2)];
        let b = vec![ev(2, 0), ev(4, 1), ev(6, 2)];
        let result = merge_events(a, b);
        let offsets: Vec<u32> = result.iter().map(|e| e.offset_ms).collect();
        assert_eq!(offsets, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn merge_empty_inputs() {
        assert!(merge_events(vec![], vec![]).is_empty());
        let a = vec![ev(1, 0)];
        assert_eq!(merge_events(a.clone(), vec![]).len(), 1);
        assert_eq!(merge_events(vec![], a).len(), 1);
    }

    #[test]
    fn merge_tie_breaks_by_seq() {
        // Same offset_ms, different seq → lower seq wins.
        let a = vec![ev(10, 1)];
        let b = vec![ev(10, 0)];
        let result = merge_events(a, b);
        assert_eq!(result[0].seq, 0);
        assert_eq!(result[1].seq, 1);
    }
}
