//! HTTP federation surface — R556-F7-T2.
//!
//! Scryer exposes a small HTTP API that other mesh nodes (and the local hub)
//! call directly. Per W264 §Discovery, yubaba advertises this endpoint via
//! `/services` but is **not** in the data path; consumers connect here.
//!
//! Routes:
//!   - `POST /federate/events`    `{filter, scopes?}` → `{events}`
//!   - `POST /federate/aggregate` `{filter, group_by, since_ms, scopes?}` → `{buckets}`
//!   - `GET  /scopes?limit=N`     → `{scopes}`
//!   - `GET  /health`             → `{status:"ok"}`
//!
//! `scopes` omitted = cross-scope rollup (the missing mesh-wide-by-level case
//! at `crates/yah/hub/src/in_process.rs:46`).
//!
//! ## ACL
//!
//! Per W264 §Trust boundary, the operator-tag check sits at scryer's listener,
//! not at yubaba. The calling identity is presented in an `X-Yah-Operator-Tag`
//! header (one tag per occurrence) set by the local Tailscale sidecar that
//! resolved the peer identity via `tailscaled` before forwarding the request.
//! Scryer trusts the header but only because the listener binds the mesh
//! interface — workloads can't reach it without an operator-tagged identity in
//! the first place. [`OperatorTagAcl`] checks `tag:operator` by default.
//!
//! ## Client
//!
//! [`HttpFederationPeer`] is the production `FederationPeer` impl — a small
//! `reqwest`-based client whose `name()` is the node tailnet hostname.
//!
//! @yah:ticket(R585-F2, "scope envelope on the federation wire: populate AnalyticsEvent.scope_kind / scope_id end-to-end")
//! @yah:at(2026-06-30T06:57:44Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R585)
//! @yah:next("Federated events lose their scope envelope on the wire: yah_scryer::federation::FederationPeer::events returns Vec<Event>, and POST /federate/events maps to {events: Vec<Event>} — no (scope_kind, scope_id) attached. As a result AnalyticsEvent.scope_kind / scope_id are empty strings under Mode-1 (see in_process.rs analytics_events: 'Scope kind/id are tracked on the wire row but not yet surfaced').")
//! @yah:next("Pick a wire shape: either (a) widen FederateEventsResp to {events: Vec<(EventScope, Event)>} (clean, breaking change — trait extension), or (b) add a parallel envelope route POST /federate/events_envelope returning {rows: [{scope, event}]} and keep events unchanged (additive). Recommend (a) given the pre-release cut-and-bleed posture; it's one trait fn signature plus the JSON tuple.")
//! @yah:next("Mirror through HttpFederationPeer (or a new trait method like events_with_scope) and through hub::in_process::federated_events_for_analytics + the AnalyticsEvent projection so scope_kind/scope_id are no longer hardcoded empty.")
//! @yah:next("scryer-local: Scryer::events already returns Vec<Event> without scope; the cross-scope rollup helpers (events_all) iterate per-scope but throw the scope away when merging. Preserve it through the rollup so the federation path can hand it back.")
//! @yah:next("Tests: extend tests/federation_http_integration.rs with an assertion that an event pushed under Service(MeshIdent(\"svc.test\")) comes back over /federate/events carrying scope_kind=service / scope_id=svc.test; extend hub analytics_tests to prove AnalyticsEvent fields populate.")
//! @yah:next("Tier: Cleric — wire-shape change + plumbing through 3 layers; spec is clear, mechanical edits in 4 files + tests.")
//! @arch:see(.yah/docs/working/W264-kamaji-managed-scryer.md)

use crate::federation::{FederationAcl, FederationError, FederationPeer, PeerIdentity};
use crate::service::{AggregateBucket, EventFilter, Scryer};
use crate::store::ScopeInfo;
use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use observation::{Event, EventScope};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ─── Wire DTOs ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederateEventsReq {
    pub filter: EventFilter,
    /// When `None`, query rolls up across every scope in the store.
    #[serde(default)]
    pub scopes: Option<Vec<EventScope>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederateEventsResp {
    pub events: Vec<Event>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederateAggregateReq {
    pub filter: EventFilter,
    pub group_by: String,
    #[serde(default)]
    pub since_ms: u64,
    #[serde(default)]
    pub scopes: Option<Vec<EventScope>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketDto {
    pub key: String,
    pub count: u64,
}

impl From<AggregateBucket> for BucketDto {
    fn from(b: AggregateBucket) -> Self {
        Self { key: b.key, count: b.count }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederateAggregateResp {
    pub buckets: Vec<BucketDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopeInfoDto {
    pub scope: EventScope,
    pub event_count: i64,
    pub last_offset_ms: i64,
}

impl From<ScopeInfo> for ScopeInfoDto {
    fn from(s: ScopeInfo) -> Self {
        Self { scope: s.scope, event_count: s.event_count, last_offset_ms: s.last_offset_ms }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopesResp {
    pub scopes: Vec<ScopeInfoDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResp {
    pub status: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct ScopesQuery {
    #[serde(default)]
    pub limit: Option<usize>,
}

// ─── Header constants ─────────────────────────────────────────────────────────

/// Header name carrying the calling identity's Tailscale operator tags.
/// Set by the local Tailscale sidecar after `WhoIs` resolution.
pub const OPERATOR_TAG_HEADER: &str = "x-yah-operator-tag";

// ─── Server ───────────────────────────────────────────────────────────────────

/// Shared state for the federation router.
pub struct FederationState {
    pub scryer: Arc<Scryer>,
    pub acl: Arc<dyn FederationAcl>,
}

impl FederationState {
    pub fn new(scryer: Arc<Scryer>, acl: Arc<dyn FederationAcl>) -> Arc<Self> {
        Arc::new(Self { scryer, acl })
    }
}

/// Build the axum router for the federation surface.
pub fn router(state: Arc<FederationState>) -> Router {
    Router::new()
        .route("/federate/events", post(handle_events))
        .route("/federate/aggregate", post(handle_aggregate))
        .route("/scopes", get(handle_scopes))
        .route("/health", get(handle_health))
        .with_state(state)
}

fn identity_from(headers: &HeaderMap) -> PeerIdentity {
    let mut id = PeerIdentity::default();
    for value in headers.get_all(OPERATOR_TAG_HEADER) {
        if let Ok(s) = value.to_str() {
            id = id.with_tag(s.to_string());
        }
    }
    id
}

fn ensure_authorized(
    state: &FederationState,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, Json<ErrorBody>)> {
    let identity = identity_from(headers);
    if state.acl.is_authorized(&identity) {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            Json(ErrorBody {
                error: "unauthorized: operator tag required".to_string(),
            }),
        ))
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

async fn handle_health() -> Json<HealthResp> {
    Json(HealthResp { status: "ok".to_string() })
}

async fn handle_scopes(
    State(state): State<Arc<FederationState>>,
    headers: HeaderMap,
    Query(q): Query<ScopesQuery>,
) -> Result<Json<ScopesResp>, (StatusCode, Json<ErrorBody>)> {
    ensure_authorized(&state, &headers)?;
    let limit = q.limit.unwrap_or(1000).min(10_000);
    let scopes = state.scryer.list_scopes(limit).map_err(scryer_err)?;
    Ok(Json(ScopesResp {
        scopes: scopes.into_iter().map(ScopeInfoDto::from).collect(),
    }))
}

async fn handle_events(
    State(state): State<Arc<FederationState>>,
    headers: HeaderMap,
    Json(req): Json<FederateEventsReq>,
) -> Result<Json<FederateEventsResp>, (StatusCode, Json<ErrorBody>)> {
    ensure_authorized(&state, &headers)?;
    let events = match req.scopes {
        None => state.scryer.events_all(&req.filter).await.map_err(scryer_err)?,
        Some(scopes) => {
            let mut acc: Vec<Event> = Vec::new();
            for scope in &scopes {
                let part = state
                    .scryer
                    .events(scope, &req.filter)
                    .await
                    .map_err(scryer_err)?;
                acc = crate::federation::merge_events(acc, part);
            }
            acc
        }
    };
    Ok(Json(FederateEventsResp { events }))
}

async fn handle_aggregate(
    State(state): State<Arc<FederationState>>,
    headers: HeaderMap,
    Json(req): Json<FederateAggregateReq>,
) -> Result<Json<FederateAggregateResp>, (StatusCode, Json<ErrorBody>)> {
    ensure_authorized(&state, &headers)?;
    let buckets = match req.scopes {
        None => state
            .scryer
            .aggregate_all(req.since_ms, &req.group_by)
            .map_err(scryer_err)?,
        Some(scopes) => {
            let mut counts: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();
            for scope in &scopes {
                for b in state
                    .scryer
                    .aggregate(scope, req.since_ms, &req.group_by)
                    .map_err(scryer_err)?
                {
                    *counts.entry(b.key).or_insert(0) += b.count;
                }
            }
            let mut buckets: Vec<AggregateBucket> = counts
                .into_iter()
                .map(|(key, count)| AggregateBucket { key, count })
                .collect();
            buckets.sort_by(|a, b| b.count.cmp(&a.count).then(a.key.cmp(&b.key)));
            buckets
        }
    };
    Ok(Json(FederateAggregateResp {
        buckets: buckets.into_iter().map(BucketDto::from).collect(),
    }))
}

fn scryer_err(err: crate::service::ScryerError) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody { error: err.to_string() }),
    )
}

// ─── Client ───────────────────────────────────────────────────────────────────

/// Production `FederationPeer` impl — `reqwest` POST to a peer scryer's
/// `/federate/events`. The local sidecar (or hub) is expected to add the
/// `X-Yah-Operator-Tag` header before this client's request leaves the host.
pub struct HttpFederationPeer {
    name: String,
    base_url: String,
    operator_tag: String,
    client: reqwest::Client,
}

impl HttpFederationPeer {
    /// `name` is the peer's tailnet hostname (used for logging + rule matching);
    /// `base_url` is the scryer endpoint root (e.g. `http://100.64.0.7:6543`).
    pub fn new(
        name: impl Into<String>,
        base_url: impl Into<String>,
        operator_tag: impl Into<String>,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            name: name.into(),
            base_url: base_url.into(),
            operator_tag: operator_tag.into(),
            client: reqwest::Client::builder().build()?,
        })
    }

    pub fn with_client(
        name: impl Into<String>,
        base_url: impl Into<String>,
        operator_tag: impl Into<String>,
        client: reqwest::Client,
    ) -> Self {
        Self {
            name: name.into(),
            base_url: base_url.into(),
            operator_tag: operator_tag.into(),
            client,
        }
    }

    /// Cross-scope aggregate query — convenience for hub callers that need the
    /// rollup but don't go through the `FederationPeer` trait (which is
    /// events-only by design).
    pub async fn aggregate(
        &self,
        since_ms: u64,
        group_by: &str,
    ) -> Result<Vec<BucketDto>, FederationError> {
        let req = FederateAggregateReq {
            filter: EventFilter::default(),
            group_by: group_by.to_string(),
            since_ms,
            scopes: None,
        };
        let url = format!("{}/federate/aggregate", self.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .post(&url)
            .header(OPERATOR_TAG_HEADER, &self.operator_tag)
            .json(&req)
            .send()
            .await
            .map_err(|e| FederationError::Rpc(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(FederationError::Rpc(format!(
                "aggregate http {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }
        let body: FederateAggregateResp = resp
            .json()
            .await
            .map_err(|e| FederationError::Rpc(e.to_string()))?;
        Ok(body.buckets)
    }
}

#[async_trait]
impl FederationPeer for HttpFederationPeer {
    fn name(&self) -> &str {
        &self.name
    }

    async fn events(&self, filter: &EventFilter) -> Result<Vec<Event>, FederationError> {
        let req = FederateEventsReq { filter: filter.clone(), scopes: None };
        let url = format!("{}/federate/events", self.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .post(&url)
            .header(OPERATOR_TAG_HEADER, &self.operator_tag)
            .json(&req)
            .send()
            .await
            .map_err(|e| FederationError::Rpc(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(FederationError::Rpc(format!(
                "events http {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }
        let body: FederateEventsResp = resp
            .json()
            .await
            .map_err(|e| FederationError::Rpc(e.to_string()))?;
        Ok(body.events)
    }
}

/// Convenience: bind a tokio listener on `addr` and serve the federation
/// router. Returns the local socket address and a join handle on success so
/// callers (kamaji service manifest, integration tests) can shut it down.
pub async fn serve(
    state: Arc<FederationState>,
    addr: std::net::SocketAddr,
) -> Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>), std::io::Error> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let app = router(state);
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok((local, handle))
}

