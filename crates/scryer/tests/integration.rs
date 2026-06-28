//! Integration tests for scryer federation — R093-F4.
#![allow(non_snake_case)] // double-underscore convention mirrors verify condition names
//!
//! Verify conditions:
//!   1. `scryer_federation__local` — query from machine A returns interleaved
//!      events from peer B (mock) merged by (offset_ms, seq).
//!   2. ACL tests live in `yah_scryer::federation::acl` (unit tests in federation.rs).

use async_trait::async_trait;
use observation::{Event, EventScope, EventSource, Level, TaskRunId};
use yah_scryer::federation::{
    DenyAllAcl, FederationAcl, FederationError, FederationPeer, FederationRule, OperatorTagAcl,
    PeerIdentity, federated_events,
};
use yah_scryer::service::{EventFilter, Scryer, ScryerConfig};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

// ─── Helper ───────────────────────────────────────────────────────────────────

fn make_event(run_id: &TaskRunId, seq: u32, offset_ms: u32) -> Event {
    Event {
        run_id: run_id.clone(),
        seq,
        offset_ms,
        level: Level::Info,
        target: "federation::test".to_string(),
        msg: format!("event at offset {offset_ms}"),
        fields: json!({}),
        anchor: None,
        source: EventSource::Synth,
    }
}

// ─── Mock peer ────────────────────────────────────────────────────────────────

struct MockPeer {
    peer_name: String,
    events: Vec<Event>,
}

#[async_trait]
impl FederationPeer for MockPeer {
    fn name(&self) -> &str {
        &self.peer_name
    }
    async fn events(&self, _filter: &EventFilter) -> Result<Vec<Event>, FederationError> {
        Ok(self.events.clone())
    }
}

struct FailingPeer;

#[async_trait]
impl FederationPeer for FailingPeer {
    fn name(&self) -> &str {
        "failing-peer"
    }
    async fn events(&self, _filter: &EventFilter) -> Result<Vec<Event>, FederationError> {
        Err(FederationError::Rpc("simulated gRPC error".to_string()))
    }
}

// ─── Federation tests ─────────────────────────────────────────────────────────

/// Verify: query from machine A returns interleaved events from peer B over
/// yubaba mesh.  R093-F4 acceptance criterion (arch doc §Sequencing scryer-4).
#[tokio::test]
async fn scryer_federation__local() {
    let run_id = TaskRunId::new();

    // Machine A's local events at odd offset_ms (1, 3, 5).
    let local = vec![
        make_event(&run_id, 0, 1),
        make_event(&run_id, 2, 3),
        make_event(&run_id, 4, 5),
    ];

    // Machine B (peer) has events at even offset_ms (2, 4, 6).
    let peer_b: Arc<dyn FederationPeer> = Arc::new(MockPeer {
        peer_name: "yubaba-pdx-2".to_string(),
        events: vec![
            make_event(&run_id, 1, 2),
            make_event(&run_id, 3, 4),
            make_event(&run_id, 5, 6),
        ],
    });

    let filter = EventFilter::default();
    let result = federated_events(local, &[peer_b], &filter, &FederationRule::All).await;

    assert_eq!(result.len(), 6, "3 local + 3 from peer B");
    let offsets: Vec<u32> = result.iter().map(|e| e.offset_ms).collect();
    assert_eq!(offsets, vec![1, 2, 3, 4, 5, 6], "events must be interleaved in time order");
}

/// Verify: Scryer::federated_events delegates local query + fan-out correctly.
#[tokio::test]
async fn scryer_federated_events_method() {
    let dir = TempDir::new().unwrap();
    let cfg = ScryerConfig::new(dir.path().join("events.db"));
    let scryer = Scryer::new(cfg, None).unwrap();

    let run_id = TaskRunId::new();
    let scope = EventScope::TaskRun(run_id.clone());

    // Push 3 local events (offset_ms 10, 30, 50).
    for (i, offset) in [(0u32, 10u32), (2, 30), (4, 50)] {
        scryer.push(scope.clone(), make_event(&run_id, i, offset)).unwrap();
    }
    scryer.flush_ring().unwrap();

    // Peer B has events at offset_ms 20, 40, 60.
    let peer_b: Arc<dyn FederationPeer> = Arc::new(MockPeer {
        peer_name: "peer-b".to_string(),
        events: vec![
            make_event(&run_id, 1, 20),
            make_event(&run_id, 3, 40),
            make_event(&run_id, 5, 60),
        ],
    });

    let filter = EventFilter::default();
    let result = scryer
        .federated_events(&scope, &filter, &FederationRule::All, &[peer_b])
        .await
        .unwrap();

    assert_eq!(result.len(), 6);
    let offsets: Vec<u32> = result.iter().map(|e| e.offset_ms).collect();
    assert_eq!(offsets, vec![10, 20, 30, 40, 50, 60]);
}

/// Verify: a failing peer is skipped (best-effort); local results still returned.
#[tokio::test]
async fn scryer_federation__failing_peer_is_skipped() {
    let run_id = TaskRunId::new();
    let local = vec![make_event(&run_id, 0, 1)];

    let peer: Arc<dyn FederationPeer> = Arc::new(FailingPeer);
    let filter = EventFilter::default();
    let result = federated_events(local, &[peer], &filter, &FederationRule::All).await;

    assert_eq!(result.len(), 1, "local event survives; failing peer is ignored");
}

/// Verify: FederationRule::Tag only fans out to matching peers.
#[tokio::test]
async fn scryer_federation__tag_rule_filters_peers() {
    let run_id = TaskRunId::new();
    let local = vec![make_event(&run_id, 0, 1)];

    let matching_peer: Arc<dyn FederationPeer> = Arc::new(MockPeer {
        peer_name: "yubaba-tier=public-1".to_string(),
        events: vec![make_event(&run_id, 1, 2)],
    });
    let excluded_peer: Arc<dyn FederationPeer> = Arc::new(MockPeer {
        peer_name: "yubaba-private-1".to_string(),
        events: vec![make_event(&run_id, 2, 3)],
    });

    let filter = EventFilter::default();
    let result = federated_events(
        local,
        &[matching_peer, excluded_peer],
        &filter,
        &FederationRule::Tag("tier=public".to_string()),
    )
    .await;

    assert_eq!(result.len(), 2, "local + matching peer only");
    assert_eq!(result[0].offset_ms, 1);
    assert_eq!(result[1].offset_ms, 2);
}

// ─── ACL integration (mirrors unit tests in federation.rs::acl) ──────────────

/// Verify: unauthorized peer (no operator tag) is rejected at RPC entry.
/// R093-F4 verify: cargo test yah_scryer::acl
#[test]
fn scryer_acl__unauthorized_peer_rejected() {
    let acl = OperatorTagAcl;
    let identity = PeerIdentity::default(); // no tags
    assert!(!acl.is_authorized(&identity), "untagged peer must be rejected");
}

#[test]
fn scryer_acl__operator_tag_is_allowed() {
    let acl = OperatorTagAcl;
    let identity = PeerIdentity::default().with_tag("tag:operator");
    assert!(acl.is_authorized(&identity));
}

#[test]
fn scryer_acl__deny_all_blocks_operator() {
    let acl = DenyAllAcl;
    let identity = PeerIdentity::default().with_tag("tag:operator");
    assert!(!acl.is_authorized(&identity), "DenyAllAcl must reject all");
}
