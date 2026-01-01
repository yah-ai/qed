//! Federation layer for cross-machine scryer queries — R093-F4.
//!
//! Shape (arch doc §Federation across machines):
//!   - `FederationRule`  — selects which warden-mesh peers to fan out to.
//!   - `FederationPeer`  — abstracts a remote scryer (gRPC in prod, mock in tests).
//!   - `FederationAcl`   — operator-tag guard.  Sits at the warden gRPC entry
//!                         point, not in scryer query logic, per the arch doc:
//!                         "Implement via Tailscale-tag check at the warden RPC
//!                         entry point, not in scryer code."
//!   - `merge_events`    — merge two time-ordered lists by (offset_ms, seq).
//!   - `federated_events` — local query + fan-out + merge (best-effort).
//!
//! Time ordering is best-effort: clocks on different machines can skew by ~ms,
//! so events that land near-simultaneously can interleave in either order.
//! Agents needing causal order must use correlation IDs (R091-F1).

use crate::service::{EventFilter, ScryerError};
use async_trait::async_trait;
use observation::Event;
use std::sync::Arc;
use thiserror::Error;

// ─── FederationRule ───────────────────────────────────────────────────────────

/// Which peers in the warden mesh to include in a fan-out query.
#[derive(Debug, Clone)]
pub enum FederationRule {
    /// All connected peers.
    All,
    /// Only peers whose name contains `tag` (e.g., `"tier=public"`).
    Tag(String),
}

// ─── PeerIdentity + ACL ───────────────────────────────────────────────────────

/// Calling identity presented at the warden gRPC entry point.
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
/// Warden's gRPC dispatcher injects a concrete impl; scryer query logic is
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
/// Production impl: gRPC streaming over the warden mesh (WireGuard).
/// Test impls: in-process mock that holds a `Vec<Event>`.
#[async_trait]
pub trait FederationPeer: Send + Sync {
    /// Stable display name for this peer (e.g. `"warden-pdx-1"`).
    fn name(&self) -> &str;
    /// Query events from this peer using `filter`.
    async fn events(&self, filter: &EventFilter) -> Result<Vec<Event>, FederationError>;
}

// ─── Merge ────────────────────────────────────────────────────────────────────

/// Merge two event slices ordered by `(offset_ms, seq)`.  O(n+m).
///
/// Both inputs must already be sorted; the output is sorted.  Near-simultaneous
/// events from different machines can appear in either order (best-effort clock).
pub fn merge_events(a: Vec<Event>, b: Vec<Event>) -> Vec<Event> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let mut ia = a.into_iter().peekable();
    let mut ib = b.into_iter().peekable();

    loop {
        match (ia.peek(), ib.peek()) {
            (None, None) => break,
            (Some(_), None) => result.push(ia.next().unwrap()),
            (None, Some(_)) => result.push(ib.next().unwrap()),
            (Some(ea), Some(eb)) => {
                if (ea.offset_ms, ea.seq) <= (eb.offset_ms, eb.seq) {
                    result.push(ia.next().unwrap());
                } else {
                    result.push(ib.next().unwrap());
                }
            }
        }
    }
    result
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
/// Peer failures are swallowed — federation is best-effort.  Callers that need
/// error visibility should call `FederationPeer::events` directly.
pub async fn federated_events(
    local_events: Vec<Event>,
    peers: &[Arc<dyn FederationPeer>],
    filter: &EventFilter,
    rule: &FederationRule,
) -> Vec<Event> {
    let mut result = local_events;
    for peer in peers {
        if peer_matches_rule(peer.as_ref(), rule) {
            if let Ok(remote) = peer.events(filter).await {
                result = merge_events(result, remote);
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
    /// R093-F4 verify: cargo test scryer::acl
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
