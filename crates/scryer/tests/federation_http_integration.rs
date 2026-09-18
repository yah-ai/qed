//! R556-F7-T2 — integration tests for `yah_scryer::federation_http`.
//!
//! Spins up the federation HTTP listener bound to an ephemeral localhost port,
//! exercises each route end-to-end through `reqwest`, asserts ACL behavior.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use std::collections::BTreeMap;

use observation::{
    ATTR_YAH_PEER_SERVICE, AttrValue, Event, EventScope, EventSource, Level, Span, SpanId,
    SpanKind, SpanStatus, TaskRunId, TraceId,
};
use serde_json::json;
use tempfile::TempDir;
use workload_spec::MeshIdent;
use yah_scryer::{
    DenyAllAcl, EventFilter, FederateAggregateReq, FederateAggregateResp, FederateEventsReq,
    FederateEventsResp, FederateHopsReq, FederateHopsResp, FederationAcl, FederationPeer,
    FederationState, HealthResp, HttpFederationPeer, OPERATOR_TAG_HEADER, OperatorTagAcl, Scryer,
    ScryerConfig, serve_federation,
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

// ─── Hop matrix (R893-F19) ────────────────────────────────────────────────────

/// One span, keyed so it lands in a known `(scope, peer, kind)` rollup cell.
fn make_span(n: u64, peer: &str, kind: SpanKind, start_unix_nanos: u64, duration: u64) -> Span {
    let mut span_id = [0u8; 8];
    span_id.copy_from_slice(&n.to_be_bytes());
    Span {
        trace_id: TraceId([3u8; 16]),
        span_id: SpanId(span_id),
        parent_span_id: None,
        name: "GET /app".to_string(),
        kind,
        start_unix_nanos,
        duration_nanos: duration,
        status: SpanStatus::Ok,
        attributes: BTreeMap::from([(
            ATTR_YAH_PEER_SERVICE.to_string(),
            AttrValue::from(peer),
        )]),
    }
}

/// The federation route must key cells the same way the store does — including
/// `kind`, which is why one hop's Client and Server legs stay two rows instead
/// of being summed into one that double-counts every call (R893-F15 trap b).
#[tokio::test]
async fn federate_hops_returns_one_cell_per_peer_and_kind() {
    let (scryer, addr, handle, _dir) = boot(Arc::new(OperatorTagAcl)).await;
    let scope = EventScope::Service(MeshIdent("passway.pub".to_string()));
    let base = 1_800_000_000_000_000_000u64;
    scryer
        .store()
        .insert_spans(&[
            (scope.clone(), make_span(1, "app", SpanKind::Client, base, 3_000_000)),
            (scope.clone(), make_span(2, "app", SpanKind::Client, base + 1_000, 4_000_000)),
            (scope.clone(), make_span(3, "app", SpanKind::Server, base + 2_000, 9_000_000)),
        ])
        .unwrap();

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/federate/hops"))
        .header(OPERATOR_TAG_HEADER, "tag:operator")
        .json(&FederateHopsReq {
            from_unix_nanos: base,
            to_unix_nanos: base + 60_000_000_000,
            bucket_nanos: None,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: FederateHopsResp = resp.json().await.unwrap();

    assert_eq!(body.hops.len(), 2, "Client and Server legs are separate cells");
    let client = body.hops.iter().find(|h| h.kind == SpanKind::Client).unwrap();
    assert_eq!(client.peer, "app");
    assert_eq!(client.count(), 2);
    let server = body.hops.iter().find(|h| h.kind == SpanKind::Server).unwrap();
    assert_eq!(server.count(), 1);
    // The histogram survives the wire whole, which is what lets a federating
    // caller merge several nodes exactly instead of averaging quantiles.
    assert!(server.latency.p50().is_some());
    handle.abort();
}

/// The ACL is one gate for the whole surface, not a per-route decision — a
/// route that forgot `ensure_authorized` would serve span data to an untagged
/// caller, and spans carry route and peer identity.
#[tokio::test]
async fn federate_hops_is_refused_without_an_operator_tag() {
    let (_scryer, addr, handle, _dir) = boot(Arc::new(OperatorTagAcl)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/federate/hops"))
        .json(&FederateHopsReq {
            from_unix_nanos: 0,
            to_unix_nanos: 60_000_000_000,
            bucket_nanos: None,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    handle.abort();
}

/// A quiet window is an empty matrix, never an error — the panel has to be able
/// to say "no spans recorded" without a failed read looking the same.
#[tokio::test]
async fn http_federation_peer_hop_rollups_roundtrip_on_a_quiet_window() {
    let (_scryer, addr, handle, _dir) = boot(Arc::new(OperatorTagAcl)).await;
    let peer =
        HttpFederationPeer::new("peer-test", format!("http://{addr}"), "tag:operator").unwrap();
    let hops = peer.hop_rollups(0, 60_000_000_000, None).await.unwrap();
    assert!(hops.is_empty());
    handle.abort();
}

/// The second card's input: with `bucket_nanos` the same range comes back as a
/// grid of sub-windows, each self-describing, so a latency regression reads as
/// the distribution changing shape rather than as one quantile moving.
#[tokio::test]
async fn federate_hops_buckets_the_range_into_self_describing_sub_windows() {
    let (scryer, addr, handle, _dir) = boot(Arc::new(OperatorTagAcl)).await;
    let scope = EventScope::Service(MeshIdent("passway.pub".to_string()));
    let base = 1_800_000_000_000_000_000u64;
    let minute = 60_000_000_000u64;
    scryer
        .store()
        .insert_spans(&[
            (scope.clone(), make_span(1, "app", SpanKind::Client, base, 3_000_000)),
            (scope.clone(), make_span(2, "app", SpanKind::Client, base + 2 * minute, 800_000_000)),
        ])
        .unwrap();

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/federate/hops"))
        .header(OPERATOR_TAG_HEADER, "tag:operator")
        .json(&FederateHopsReq {
            from_unix_nanos: base,
            to_unix_nanos: base + 3 * minute,
            bucket_nanos: Some(minute),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: FederateHopsResp = resp.json().await.unwrap();

    // Two populated minutes; the quiet one in between yields no row at all,
    // which is why the caller reconstructs the grid from the bounds rather than
    // from the row order.
    assert_eq!(body.hops.len(), 2);
    let mut bounds: Vec<u64> = body.hops.iter().map(|h| h.window_start_unix_nanos).collect();
    bounds.sort_unstable();
    assert_eq!(bounds[1] - bounds[0], 2 * minute);
    for h in &body.hops {
        assert_eq!(h.window_end_unix_nanos - h.window_start_unix_nanos, minute);
    }
    // Same hop, two very different shapes — the thing one merged cell hides.
    let slow = body.hops.iter().map(|h| h.latency.p50().unwrap()).max().unwrap();
    let fast = body.hops.iter().map(|h| h.latency.p50().unwrap()).min().unwrap();
    assert!(slow > fast * 10);
    handle.abort();
}

/// A fine bucket over a long range coarsens the bucket; it must never clip the
/// range, or a 24h lookback would report its far end as silent.
#[tokio::test]
async fn a_fine_bucket_over_a_long_range_coarsens_instead_of_truncating() {
    let (scryer, addr, handle, _dir) = boot(Arc::new(OperatorTagAcl)).await;
    let scope = EventScope::Service(MeshIdent("passway.pub".to_string()));
    let base = 1_800_000_000_000_000_000u64;
    let minute = 60_000_000_000u64;
    let day = 24 * 60 * minute;
    // One span in the very last hour of a 24h window.
    scryer
        .store()
        .insert_spans(&[(
            scope.clone(),
            make_span(1, "app", SpanKind::Client, base + day - minute, 5_000_000),
        )])
        .unwrap();

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/federate/hops"))
        .header(OPERATOR_TAG_HEADER, "tag:operator")
        .json(&FederateHopsReq {
            from_unix_nanos: base,
            to_unix_nanos: base + day,
            bucket_nanos: Some(minute),
        })
        .send()
        .await
        .unwrap();
    let body: FederateHopsResp = resp.json().await.unwrap();
    assert_eq!(body.hops.len(), 1, "the far-end span must still be reported");
    assert!(
        body.hops[0].window_end_unix_nanos - body.hops[0].window_start_unix_nanos > minute,
        "the bucket should have been coarsened rather than the range cut short",
    );
    handle.abort();
}
