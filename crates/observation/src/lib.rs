//! @arch:layer(kg_store)
//! @arch:role(substrate)
//! @arch:see(.yah/docs/architecture/A049-yah-scryer.md)
//! @arch:see(.yah/docs/working/yah-task-runs.md)
//!
//! `observation` — shared event types for the scryer observation substrate.
//!
//! Holds the types that both `task-runs` and `scryer` depend on.
//! `task-runs` re-exports everything from here for backward compat.

pub mod ingest;
pub mod types;

// R893-F16 — the ingestion socket's line protocol, named once so the emitter
// and the receiver cannot drift. See `ingest` for why spans ride the existing
// Unix socket rather than a new transport.
pub use ingest::IngestLine;

pub use types::{
    ChunkRef, Diagnostic, Event, EventScope, EventSource, ForgeId, Initiator, Level, RunStatus,
    TaskRunId, RESERVED_FIELD_PATHS,
};

// Span signal (R893-F15) — OTel data model, no OTel SDK. See `types`.
pub use types::{
    rollup_window_start, AttrValue, HopRollup, LatencyHistogram, Span, SpanId, SpanKind, SpanStatus,
    TraceId, ATTR_CLIENT_ADDRESS, ATTR_DB_QUERY_TEXT, ATTR_DB_SYSTEM_NAME, ATTR_ERROR_TYPE,
    ATTR_HTTP_REQUEST_METHOD, ATTR_HTTP_RESPONSE_STATUS_CODE, ATTR_HTTP_ROUTE, ATTR_SERVER_ADDRESS,
    ATTR_SERVER_PORT, ATTR_SERVICE_NAME, ATTR_URL_PATH, ATTR_YAH_PEER_SERVICE, ATTR_YAH_TENANT,
    LATENCY_BUCKET_BOUNDS_NANOS, LATENCY_BUCKET_COUNT, RESERVED_SPAN_ATTRIBUTES,
    ROLLUP_WINDOW_NANOS,
};
