//! R556-F7-T2 — integration tests for `yah_scryer::federation_http`.
//!
//! Spins up the federation HTTP listener bound to an ephemeral localhost port,
//! exercises each route end-to-end through `reqwest`, asserts ACL behavior.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use observation::{Event, EventScope, EventSource, Level, TaskRunId};
use serde_json::json;
use tempfile::TempDir;
use workload_spec::MeshIdent;
use yah_scryer::{
    DenyAllAcl, EventFilter, FederateAggregateReq, FederateAggregateResp, FederateEventsReq,
    FederateEventsResp, FederationAcl, FederationPeer, FederationState, HealthResp,
    HttpFederationPeer, OPERATOR_TAG_HEADER, OperatorTagAcl, Scryer, ScryerConfig,
    serve_federation,
};

fn make_event(seq: u32) -> Event {
    Event {
        run_id: TaskRunId::new(),
        seq,
        offset_ms: seq * 10,
        level: Level::Info,
        target: "svc::db".to_string(),
        msg: format!("event {seq}"),
        fields: json!({}),
        anchor: None,
        source: EventSource::Synth,
    }
}

async fn boot(
    acl: Arc<dyn FederationAcl>,
) -> (Arc<Scryer>, SocketAddr, tokio::task::JoinHandle<()>, TempDir) {
    let dir = TempDir::new().unwrap();
    let cfg = ScryerConfig::new(dir.path().join("events.db"));
    let scryer = Arc::new(Scryer::new(cfg, None).unwrap());
    let scope = EventScope::Service(MeshIdent("svc.test".to_string()));
    for i in 0..3 {
        scryer.push(scope.clone(), make_event(i)).unwrap();
    }
    scryer.flush_ring().unwrap();
    let state = FederationState::new(Arc::clone(&scryer), acl);
    let (local, handle) =
        serve_federation(state, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .unwrap();
    (scryer, local, handle, dir)
}

#[tokio::test]
async fn federate_events_returns_payload_with_operator_tag() {
    let (_scryer, addr, handle, _dir) = boot(Arc::new(OperatorTagAcl)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/federate/events"))
        .header(OPERATOR_TAG_HEADER, "tag:operator")
        .json(&FederateEventsReq { filter: EventFilter::default(), scopes: None })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: FederateEventsResp = resp.json().await.unwrap();
    assert_eq!(body.events.len(), 3);
    // R585-F2: each row carries the scope it was stored under, so a
    // cross-scope rollup (`scopes: None`) is no longer scope-blind.
    for row in &body.events {
        assert_eq!(
            row.scope,
            EventScope::Service(MeshIdent("svc.test".to_string())),
        );
    }
    handle.abort();
}

#[tokio::test]
async fn federate_events_rejects_request_without_operator_tag() {
    let (_scryer, addr, handle, _dir) = boot(Arc::new(OperatorTagAcl)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/federate/events"))
        .json(&FederateEventsReq { filter: EventFilter::default(), scopes: None })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    handle.abort();
}

#[tokio::test]
async fn deny_all_acl_blocks_even_operator_tag() {
    let (_scryer, addr, handle, _dir) = boot(Arc::new(DenyAllAcl)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/federate/events"))
        .header(OPERATOR_TAG_HEADER, "tag:operator")
        .json(&FederateEventsReq { filter: EventFilter::default(), scopes: None })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    handle.abort();
}

#[tokio::test]
async fn health_is_open_without_tag() {
    let (_scryer, addr, handle, _dir) = boot(Arc::new(OperatorTagAcl)).await;
    let resp = reqwest::get(format!("http://{addr}/health")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: HealthResp = resp.json().await.unwrap();
    assert_eq!(body.status, "ok");
    handle.abort();
}

#[tokio::test]
async fn aggregate_cross_scope_rolls_up() {
    let (_scryer, addr, handle, _dir) = boot(Arc::new(OperatorTagAcl)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/federate/aggregate"))
        .header(OPERATOR_TAG_HEADER, "tag:operator")
        .json(&FederateAggregateReq {
            filter: EventFilter::default(),
            group_by: "level".to_string(),
            since_ms: 0,
            scopes: None,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: FederateAggregateResp = resp.json().await.unwrap();
    let total: u64 = body.buckets.iter().map(|b| b.count).sum();
    assert_eq!(total, 3, "all three pushed events should surface in the cross-scope rollup");
    handle.abort();
}

#[tokio::test]
async fn http_federation_peer_roundtrip() {
    let (_scryer, addr, handle, _dir) = boot(Arc::new(OperatorTagAcl)).await;
    let peer =
        HttpFederationPeer::new("peer-test", format!("http://{addr}"), "tag:operator").unwrap();
    let events = peer.events(&EventFilter::default()).await.unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(peer.name(), "peer-test");
    // The envelope survives the full HTTP round-trip through the production
    // FederationPeer impl — this is what populates AnalyticsEvent.scope_* .
    assert_eq!(events[0].scope.kind_str(), "service");
    assert_eq!(events[0].scope.id_str(), "svc.test");
    handle.abort();
}
