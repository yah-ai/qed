//! Core observation types shared between task-runs and scryer.
//!
//! Types here are intentionally free of I/O — store layers own persistence.
//!
//! @arch:see(.yah/docs/working/W346-services-tab-three-views-and-the-tab-boundary.md)
//! @arch:see(.yah/docs/working/W346-services-tab-three-views-and-the-tab-boundary.md)

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;
use workload_spec::MeshIdent;

// ─── TaskRunId ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskRunId(pub Uuid);

impl TaskRunId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TaskRunId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TaskRunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for TaskRunId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

// ─── ForgeId ──────────────────────────────────────────────────────────────────

/// Stable identity for a forge run (UUID), shared across all three forge
/// species (local, remote, integration).  Stable across yah restarts.
///
/// For local-forge runs `ForgeId` and `TaskRunId` share the same underlying
/// UUID — the `From` impls below are the identity conversion.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ForgeId(pub Uuid);

impl ForgeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ForgeId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ForgeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ForgeId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

/// For local-forge: the TaskRunId IS the ForgeId.  Same UUID, no translation.
impl From<ForgeId> for TaskRunId {
    fn from(id: ForgeId) -> Self {
        Self(id.0)
    }
}

impl From<TaskRunId> for ForgeId {
    fn from(id: TaskRunId) -> Self {
        Self(id.0)
    }
}

// ─── Initiator ────────────────────────────────────────────────────────────────

/// Who or what initiated a run.
///
/// Lives here rather than in `task-runs` for the same reason [`ForgeId`] does:
/// it is pure data that describers of work (queues, schedulers, wire clients)
/// need without dragging in a SQLite store. `task-runs` re-exports it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Initiator {
    Human { camp: String },
    Agent { camp: String, agent: String, session: String },
    Gnome { camp: String, shift: String },
    Cron { camp: String, schedule: String },
}

// ─── RunStatus ────────────────────────────────────────────────────────────────

/// Terminal + in-flight status of a run.
///
/// Hoisted alongside [`Initiator`] so the task vocabulary can describe a run's
/// lifecycle without depending on the store crate. `task-runs` re-exports it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Done { exit_code: i32, ended_at: u64 },
    Killed { signal: i32, ended_at: u64 },
    Lost { reason: String },
}

// ─── EventScope ───────────────────────────────────────────────────────────────

/// The scope an event row belongs to — stored as `(scope_kind, scope_id)` in
/// scryer's events.db so the index generalizes across all species.
///
/// `TaskRun` corresponds to per-run task-run.db rows (existing local-forge
/// store).  `Service` corresponds to long-lived service events keyed by mesh
/// identity.  `Forge` is the unified scope for all three forge species (local,
/// remote, integration); for local-forge runs `Forge(id)` and
/// `TaskRun(id.into())` refer to the same underlying UUID.  `TaskRun` is kept
/// as a backward-compatible alias so existing local-forge queries continue
/// working without a migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum EventScope {
    TaskRun(TaskRunId),
    Service(MeshIdent),
    /// Unified scope for forge runs.  Preferred over `TaskRun` for new code;
    /// scryer stores both so old queries still resolve.
    Forge(ForgeId),
}

impl EventScope {
    pub fn kind_str(&self) -> &'static str {
        match self {
            EventScope::TaskRun(_) => "task_run",
            EventScope::Service(_) => "service",
            EventScope::Forge(_) => "forge",
        }
    }

    pub fn id_str(&self) -> String {
        match self {
            EventScope::TaskRun(id) => id.to_string(),
            EventScope::Service(ident) => ident.0.clone(),
            EventScope::Forge(id) => id.to_string(),
        }
    }
}

// ─── Level ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Trace => "trace",
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
            Level::Fatal => "fatal",
        }
    }
}

impl std::str::FromStr for Level {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "trace" => Ok(Level::Trace),
            "debug" => Ok(Level::Debug),
            "info" => Ok(Level::Info),
            "warn" => Ok(Level::Warn),
            "error" => Ok(Level::Error),
            "fatal" => Ok(Level::Fatal),
            other => Err(format!("unknown level: {other}")),
        }
    }
}

// ─── EventSource ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventSource {
    Beholder { name: String, version: String },
    Shim { lib: String, version: String },
    Synth,
}

impl EventSource {
    pub fn kind_str(&self) -> &'static str {
        match self {
            EventSource::Beholder { .. } => "beholder",
            EventSource::Shim { .. } => "shim",
            EventSource::Synth => "synth",
        }
    }

    pub fn name_str(&self) -> &str {
        match self {
            EventSource::Beholder { name, .. } => name,
            EventSource::Shim { lib, .. } => lib,
            EventSource::Synth => "synth",
        }
    }
}

// ─── ChunkRef + Event ─────────────────────────────────────────────────────────

/// Back-pointer from an event into the raw byte stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub seq: u32,
}

/// A structured event row — written by beholders (Tier 1.5) or shims (Tier 2).
///
/// `fields` holds open-shape JSON; reserved key prefixes follow a loose
/// OTel-semconv flavor (`error.*`, `file.*`, `test.*`, `build.*`, etc.).
/// See `RESERVED_FIELD_PATHS` for the canonical list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub run_id: TaskRunId,
    pub seq: u32,
    pub offset_ms: u32,
    pub level: Level,
    /// Dot-namespaced producer identity, e.g. `"cargo::rustc"`, `"tsc"`.
    pub target: String,
    pub msg: String,
    /// Freeform JSON object. Reserved key prefixes are listed in `RESERVED_FIELD_PATHS`.
    pub fields: serde_json::Value,
    pub anchor: Option<ChunkRef>,
    pub source: EventSource,
}

/// Normalized diagnostic shape — produced by beholders and shims where it makes sense.
/// Agents query `task.diagnostics` rather than parsing raw events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub severity: Level,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub col: Option<u32>,
    pub code: Option<String>,
    pub message: String,
    pub source: String,
    pub run_id: TaskRunId,
    pub event_seq: u32,
}

// ─── Reserved field paths (OTel-semconv-lite) ─────────────────────────────────

/// Field paths in `Event.fields` that have defined semantics.
pub const RESERVED_FIELD_PATHS: &[&str] = &[
    "$.error.kind",
    "$.error.code",
    "$.file.path",
    "$.file.line",
    "$.file.col",
    "$.http.status_code",
    "$.db.statement",
    "$.test.name",
    "$.test.suite",
    "$.build.unit",
];

// ─── Span (OTel data model, no OTel SDK) ─────────────────────────────────────

// This section is the SECOND application of the position reasoned out at
// `oss/yubaba/crates/yubaba/src/node.rs:18-58`: **we adopt OpenTelemetry's data
// model and semantic-convention attribute names; we do not link the OTel SDK.**
// No `opentelemetry`, `opentelemetry_sdk` or `opentelemetry-otlp` dependency
// appears here or in this crate's manifest, and none should be added — the
// binary-size argument that justified skipping the SDK for node metrics applies
// unchanged to yubaba/kamaji/passway, which all ship as curl-fetched musl-static
// artifacts.
//
// What that buys: [`TraceId`]/[`SpanId`] serialize as the lowercase hex the
// `traceparent` header and OTLP/JSON both use, and [`Span::attributes`] keys are
// already valid OTLP attribute keys. A real OTLP exporter later is a rename-free
// loop over these keys, not a re-modelling.
//
// A `Span` is deliberately NOT an [`Event`] with extra fields. `Event` is a log
// line keyed to a [`TaskRunId`] with no span identity and no duration, and
// [`RESERVED_FIELD_PATHS`] has no duration slot. Scoping IS shared: spans are
// stored as `(EventScope, Span)` pairs exactly as events are, so a span and a
// log line agree on what a service is.

/// W3C Trace Context trace id — 16 bytes. All-zero is the spec's "invalid"
/// sentinel, so [`TraceId::is_valid`] is the check a receiver makes before
/// adopting an inbound `traceparent`.
///
/// Serializes as a 32-char lowercase hex string: the same text that appears in
/// the `traceparent` header and in OTLP/JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TraceId(pub [u8; 16]);

/// W3C Trace Context span id — 8 bytes. All-zero is the "invalid" sentinel;
/// a root span carries `parent_span_id: None` rather than an all-zero parent.
///
/// Serializes as a 16-char lowercase hex string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SpanId(pub [u8; 8]);

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    s
}

fn hex_decode<const N: usize>(s: &str, what: &str) -> Result<[u8; N], String> {
    let bytes = s.as_bytes();
    if bytes.len() != N * 2 {
        return Err(format!("{what}: expected {} hex chars, got {}", N * 2, bytes.len()));
    }
    let mut out = [0u8; N];
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = (bytes[i * 2] as char)
            .to_digit(16)
            .ok_or_else(|| format!("{what}: not hex: {s}"))?;
        let lo = (bytes[i * 2 + 1] as char)
            .to_digit(16)
            .ok_or_else(|| format!("{what}: not hex: {s}"))?;
        *slot = ((hi << 4) | lo) as u8;
    }
    Ok(out)
}

macro_rules! hex_id {
    ($ty:ident, $n:expr, $what:literal) => {
        impl $ty {
            /// The spec's all-zero "invalid" value.
            pub const INVALID: Self = Self([0u8; $n]);

            /// False for the all-zero sentinel, which the W3C spec forbids
            /// propagating.
            pub fn is_valid(&self) -> bool {
                self.0 != [0u8; $n]
            }

            /// Lowercase hex, as it appears in `traceparent` and OTLP/JSON.
            pub fn to_hex(&self) -> String {
                hex_encode(&self.0)
            }

            pub fn from_hex(s: &str) -> Result<Self, String> {
                Ok(Self(hex_decode::<$n>(s, $what)?))
            }
        }

        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.to_hex())
            }
        }

        impl std::str::FromStr for $ty {
            type Err = String;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::from_hex(s)
            }
        }

        impl Serialize for $ty {
            fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
                ser.serialize_str(&self.to_hex())
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
                let s = String::deserialize(de)?;
                Self::from_hex(&s).map_err(serde::de::Error::custom)
            }
        }
    };
}

hex_id!(TraceId, 16, "trace_id");
hex_id!(SpanId, 8, "span_id");

/// OTel `SpanKind` — which side of a hop this span describes.
///
/// One hop produces TWO spans: a `Client` span on the caller and a `Server`
/// span on the callee. They are not interchangeable and must not be merged,
/// which is why `kind` is part of the rollup key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SpanKind::Internal => "internal",
            SpanKind::Server => "server",
            SpanKind::Client => "client",
            SpanKind::Producer => "producer",
            SpanKind::Consumer => "consumer",
        }
    }
}

impl std::str::FromStr for SpanKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "internal" => Ok(SpanKind::Internal),
            "server" => Ok(SpanKind::Server),
            "client" => Ok(SpanKind::Client),
            "producer" => Ok(SpanKind::Producer),
            "consumer" => Ok(SpanKind::Consumer),
            other => Err(format!("unknown span kind: {other}")),
        }
    }
}

/// OTel span status. `Unset` is the default and is NOT an error — only an
/// explicit `Error` counts toward a hop's error rate.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum SpanStatus {
    #[default]
    Unset,
    Ok,
    Error { message: Option<String> },
}

impl SpanStatus {
    pub fn code_str(&self) -> &'static str {
        match self {
            SpanStatus::Unset => "unset",
            SpanStatus::Ok => "ok",
            SpanStatus::Error { .. } => "error",
        }
    }

    pub fn message(&self) -> Option<&str> {
        match self {
            SpanStatus::Error { message } => message.as_deref(),
            _ => None,
        }
    }

    /// Rebuild from the `(status_code, status_message)` column pair a store writes.
    pub fn from_parts(code: &str, message: Option<String>) -> Result<Self, String> {
        match code {
            "unset" => Ok(SpanStatus::Unset),
            "ok" => Ok(SpanStatus::Ok),
            "error" => Ok(SpanStatus::Error { message }),
            other => Err(format!("unknown span status code: {other}")),
        }
    }
}

/// An OTel attribute value.
///
/// Typed rather than freeform `serde_json::Value` because consumers compare
/// these: `http.response.status_code` has to be an integer for a `>= 500`
/// predicate to mean anything. Arrays are deliberately absent — nothing emits
/// one yet, and adding a variant later is a non-breaking widening.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AttrValue {
    Bool(bool),
    Int(i64),
    Double(f64),
    Str(String),
}

impl From<bool> for AttrValue {
    fn from(v: bool) -> Self {
        AttrValue::Bool(v)
    }
}
impl From<i64> for AttrValue {
    fn from(v: i64) -> Self {
        AttrValue::Int(v)
    }
}
impl From<u16> for AttrValue {
    fn from(v: u16) -> Self {
        AttrValue::Int(v as i64)
    }
}
impl From<f64> for AttrValue {
    fn from(v: f64) -> Self {
        AttrValue::Double(v)
    }
}
impl From<&str> for AttrValue {
    fn from(v: &str) -> Self {
        AttrValue::Str(v.to_string())
    }
}
impl From<String> for AttrValue {
    fn from(v: String) -> Self {
        AttrValue::Str(v)
    }
}

impl AttrValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            AttrValue::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            AttrValue::Int(i) => Some(*i),
            _ => None,
        }
    }
}

/// Semconv attribute keys this codebase emits, by their standard OTel spelling.
///
/// Keys with no standard equivalent are namespaced under `yah.`, which is what
/// the conventions prescribe for non-standard attributes. Emitters should use
/// these consts rather than re-spelling the strings.
pub const ATTR_SERVICE_NAME: &str = "service.name";
pub const ATTR_SERVER_ADDRESS: &str = "server.address";
pub const ATTR_SERVER_PORT: &str = "server.port";
pub const ATTR_CLIENT_ADDRESS: &str = "client.address";
pub const ATTR_HTTP_REQUEST_METHOD: &str = "http.request.method";
pub const ATTR_HTTP_RESPONSE_STATUS_CODE: &str = "http.response.status_code";
pub const ATTR_HTTP_ROUTE: &str = "http.route";
pub const ATTR_URL_PATH: &str = "url.path";
pub const ATTR_DB_SYSTEM_NAME: &str = "db.system.name";
pub const ATTR_DB_QUERY_TEXT: &str = "db.query.text";
pub const ATTR_ERROR_TYPE: &str = "error.type";

/// yah-specific: the LOGICAL service name of the other end of the hop.
///
/// `server.address` is a host or IP, which is not a service — two tenants
/// behind one mesh address are one row in a hop matrix keyed on it. The mesh
/// knows the logical name, so it is recorded separately and preferred by
/// [`Span::peer_ident`].
pub const ATTR_YAH_PEER_SERVICE: &str = "yah.peer.service";
/// yah-specific: the tenant a span was produced on behalf of.
pub const ATTR_YAH_TENANT: &str = "yah.tenant";

/// Attribute keys with defined semantics, the span-side analogue of
/// [`RESERVED_FIELD_PATHS`].
pub const RESERVED_SPAN_ATTRIBUTES: &[&str] = &[
    ATTR_SERVICE_NAME,
    ATTR_SERVER_ADDRESS,
    ATTR_SERVER_PORT,
    ATTR_CLIENT_ADDRESS,
    ATTR_HTTP_REQUEST_METHOD,
    ATTR_HTTP_RESPONSE_STATUS_CODE,
    ATTR_HTTP_ROUTE,
    ATTR_URL_PATH,
    ATTR_DB_SYSTEM_NAME,
    ATTR_DB_QUERY_TEXT,
    ATTR_ERROR_TYPE,
    ATTR_YAH_PEER_SERVICE,
    ATTR_YAH_TENANT,
];

/// One OTel span — a timed operation with a caller and a callee.
///
/// Carries no scope of its own: stores and transports pair it with an
/// [`EventScope`] exactly as they do [`Event`], so `Service(MeshIdent)` means
/// the same thing for both signals.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    /// `None` for a root span. The all-zero [`SpanId::INVALID`] is not used here.
    pub parent_span_id: Option<SpanId>,
    /// Low-cardinality operation name — `GET /orders/{id}`, not `GET /orders/42`.
    pub name: String,
    pub kind: SpanKind,
    /// Wall-clock start, nanoseconds since the Unix epoch.
    pub start_unix_nanos: u64,
    /// Elapsed time. Stored rather than an end timestamp because that is the
    /// number every aggregate wants and it cannot go negative across a clock step.
    pub duration_nanos: u64,
    #[serde(default)]
    pub status: SpanStatus,
    /// Flat, dotted, already-valid-OTLP attribute keys. See
    /// [`RESERVED_SPAN_ATTRIBUTES`].
    #[serde(default)]
    pub attributes: BTreeMap<String, AttrValue>,
}

impl Span {
    pub fn end_unix_nanos(&self) -> u64 {
        self.start_unix_nanos.saturating_add(self.duration_nanos)
    }

    /// Only an explicit `Error` status counts. An HTTP 500 that the emitter did
    /// not mark `Error` is not an error here — the emitter owns that call, so
    /// the aggregate does not second-guess it.
    pub fn is_error(&self) -> bool {
        matches!(self.status, SpanStatus::Error { .. })
    }

    pub fn attr(&self, key: &str) -> Option<&AttrValue> {
        self.attributes.get(key)
    }

    /// The other end of the hop, for the rollup key and the hop matrix.
    ///
    /// `yah.peer.service` (logical) wins over `server.address` (an address).
    /// `None` when neither is present — the caller stores that as the empty
    /// string so the key stays NOT NULL.
    pub fn peer_ident(&self) -> Option<&str> {
        self.attr(ATTR_YAH_PEER_SERVICE)
            .and_then(AttrValue::as_str)
            .or_else(|| self.attr(ATTR_SERVER_ADDRESS).and_then(AttrValue::as_str))
    }
}

// ─── Rollup (windowed aggregates over spans) ─────────────────────────────────

/// Rollup cadence: **60-second tumbling windows**, computed at write time.
///
/// Decided in R893-F15 rather than left to the view, because the view cannot
/// fix it: at `quota::ServiceQuotaManager`'s 1000 ev/s per-`MeshIdent` ceiling a
/// one-hour panel load would scan up to 3.6M raw spans per service. Sixty
/// seconds is the floor that serves every entry in the Analytics
/// `LOOKBACK_OPTIONS` list by merging buckets, and it makes the rollup
/// independent of the raw-span retention clock — the panel keeps answering for
/// windows whose raw spans have already been pruned to the Parquet tier.
pub const ROLLUP_WINDOW_NANOS: u64 = 60 * 1_000_000_000;

/// Floor a timestamp to the start of its rollup window.
pub fn rollup_window_start(unix_nanos: u64) -> u64 {
    unix_nanos - (unix_nanos % ROLLUP_WINDOW_NANOS)
}

/// Explicit bucket boundaries (inclusive upper bounds, nanoseconds) for latency
/// histograms — OTel semconv's advised `http.server.request.duration` buckets,
/// converted from seconds.
///
/// Changing these is a SCHEMA change, not a tuning knob: stored histograms are
/// arrays positional against this list and old rows become unmergeable. A
/// length mismatch fails deserialization loudly rather than silently
/// misattributing counts.
pub const LATENCY_BUCKET_BOUNDS_NANOS: [u64; 14] = [
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    75_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    750_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    7_500_000_000,
    10_000_000_000,
];

/// Number of histogram buckets — one per boundary plus the overflow bucket.
pub const LATENCY_BUCKET_COUNT: usize = LATENCY_BUCKET_BOUNDS_NANOS.len() + 1;

/// An OTel explicit-bucket histogram of span durations.
///
/// Buckets rather than stored quantiles because **quantiles do not merge**:
/// averaging two windows' p95 is not the p95 of their union, so a panel that
/// spans 60 windows would be reading a fabricated number. Element-wise bucket
/// addition IS exact, which is what makes a 60s cadence usable for a 30-day
/// lookback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyHistogram {
    /// Positional against [`LATENCY_BUCKET_BOUNDS_NANOS`]; last entry is the
    /// overflow bucket for durations above the final boundary.
    pub counts: [u64; LATENCY_BUCKET_COUNT],
    pub sum_nanos: u64,
    pub min_nanos: Option<u64>,
    pub max_nanos: Option<u64>,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            counts: [0u64; LATENCY_BUCKET_COUNT],
            sum_nanos: 0,
            min_nanos: None,
            max_nanos: None,
        }
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn count(&self) -> u64 {
        self.counts.iter().sum()
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn record(&mut self, duration_nanos: u64) {
        let idx = LATENCY_BUCKET_BOUNDS_NANOS
            .iter()
            .position(|b| duration_nanos <= *b)
            .unwrap_or(LATENCY_BUCKET_BOUNDS_NANOS.len());
        self.counts[idx] += 1;
        self.sum_nanos = self.sum_nanos.saturating_add(duration_nanos);
        self.min_nanos = Some(self.min_nanos.map_or(duration_nanos, |m| m.min(duration_nanos)));
        self.max_nanos = Some(self.max_nanos.map_or(duration_nanos, |m| m.max(duration_nanos)));
    }

    /// Exact element-wise merge — this is why the buckets are stored.
    pub fn merge(&mut self, other: &LatencyHistogram) {
        for (slot, add) in self.counts.iter_mut().zip(other.counts.iter()) {
            *slot += add;
        }
        self.sum_nanos = self.sum_nanos.saturating_add(other.sum_nanos);
        self.min_nanos = match (self.min_nanos, other.min_nanos) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max_nanos = match (self.max_nanos, other.max_nanos) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
    }

    pub fn mean_nanos(&self) -> Option<u64> {
        let n = self.count();
        (n > 0).then(|| self.sum_nanos / n)
    }

    /// Quantile by linear interpolation inside the containing bucket, clamped
    /// to the observed `[min, max]`.
    ///
    /// This is bucket-resolution estimation, the same semantics Prometheus's
    /// `histogram_quantile` has: 100 spans that were all exactly 1ms report a
    /// p50 of 2.55ms, the midpoint of the `<= 5ms` bucket they share. The
    /// clamp only recovers an exact value where a bucket's samples are also the
    /// histogram's own min or max. Landing in the unbounded overflow bucket
    /// returns `max_nanos`, the only defensible value available.
    pub fn quantile(&self, q: f64) -> Option<u64> {
        let total = self.count();
        if total == 0 {
            return None;
        }
        let q = q.clamp(0.0, 1.0);
        let target = ((q * total as f64).ceil() as u64).max(1);
        let mut cumulative = 0u64;
        for (i, bucket) in self.counts.iter().enumerate() {
            if *bucket == 0 {
                continue;
            }
            cumulative += bucket;
            if cumulative < target {
                continue;
            }
            let Some(upper) = LATENCY_BUCKET_BOUNDS_NANOS.get(i).copied() else {
                return self.max_nanos;
            };
            let lower = if i == 0 { 0 } else { LATENCY_BUCKET_BOUNDS_NANOS[i - 1] };
            let rank_in_bucket = target - (cumulative - bucket);
            let frac = rank_in_bucket as f64 / *bucket as f64;
            let value = lower as f64 + (upper - lower) as f64 * frac;
            let value = value.round() as u64;
            return Some(value.clamp(
                self.min_nanos.unwrap_or(0),
                self.max_nanos.unwrap_or(u64::MAX),
            ));
        }
        self.max_nanos
    }

    pub fn p50(&self) -> Option<u64> {
        self.quantile(0.50)
    }
    pub fn p95(&self) -> Option<u64> {
        self.quantile(0.95)
    }
    pub fn p99(&self) -> Option<u64> {
        self.quantile(0.99)
    }
}

/// One cell of the hop matrix: a `(service, peer, kind)` edge aggregated over a
/// window range.
///
/// `kind` is part of the identity on purpose — the same hop shows up as a
/// `Client` span on the caller and a `Server` span on the callee, and summing
/// them double-counts every call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HopRollup {
    pub scope: EventScope,
    /// [`Span::peer_ident`], or the empty string when the span named no peer.
    pub peer: String,
    pub kind: SpanKind,
    /// Aligned start of the merged range, inclusive.
    pub window_start_unix_nanos: u64,
    /// Aligned end of the merged range, exclusive. Reflects the RANGE ASKED
    /// FOR, not the windows that happened to have data, so a hop that went
    /// silent reads as a genuine drop in [`HopRollup::rate_per_sec`].
    pub window_end_unix_nanos: u64,
    pub error_count: u64,
    pub latency: LatencyHistogram,
}

impl HopRollup {
    pub fn count(&self) -> u64 {
        self.latency.count()
    }

    pub fn error_rate(&self) -> f64 {
        let n = self.count();
        if n == 0 {
            0.0
        } else {
            self.error_count as f64 / n as f64
        }
    }

    pub fn rate_per_sec(&self) -> f64 {
        let span_nanos = self
            .window_end_unix_nanos
            .saturating_sub(self.window_start_unix_nanos);
        if span_nanos == 0 {
            0.0
        } else {
            self.count() as f64 / (span_nanos as f64 / 1e9)
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod scope {
    use super::*;

    /// Verify that EventScope::Forge round-trips through the (scope_kind,
    /// scope_id) column representation used by scryer's events.db.
    #[test]
    fn forge() {
        let id = ForgeId::new();
        let scope = EventScope::Forge(id.clone());

        // Column values are what the store writes and reads back.
        assert_eq!(scope.kind_str(), "forge");
        assert_eq!(scope.id_str(), id.to_string());

        // The id_str must parse back to an identical ForgeId.
        let recovered: ForgeId = scope.id_str().parse().unwrap();
        assert_eq!(recovered, id);

        // Serde round-trip (used by RPC wire format).
        let json = serde_json::to_string(&scope).unwrap();
        let back: EventScope = serde_json::from_str(&json).unwrap();
        assert_eq!(back, scope);

        // TaskRun and Forge with the same underlying UUID produce distinct scopes.
        let task_scope = EventScope::TaskRun(TaskRunId(id.0));
        assert_ne!(task_scope.kind_str(), scope.kind_str());
        assert_eq!(task_scope.id_str(), scope.id_str()); // same UUID, different kind
    }
}

#[cfg(test)]
mod span {
    use super::*;

    fn sample() -> Span {
        Span {
            trace_id: TraceId([
                0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e,
                0x47, 0x36,
            ]),
            span_id: SpanId([0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7]),
            parent_span_id: Some(SpanId([1, 2, 3, 4, 5, 6, 7, 8])),
            name: "GET /orders/{id}".to_string(),
            kind: SpanKind::Server,
            start_unix_nanos: 1_757_000_000_000_000_000,
            duration_nanos: 12_345_678,
            status: SpanStatus::Error { message: Some("upstream refused".to_string()) },
            attributes: BTreeMap::from([
                (ATTR_HTTP_REQUEST_METHOD.to_string(), AttrValue::from("GET")),
                (ATTR_HTTP_RESPONSE_STATUS_CODE.to_string(), AttrValue::from(503u16)),
                (ATTR_SERVER_ADDRESS.to_string(), AttrValue::from("100.64.0.2")),
                (ATTR_YAH_PEER_SERVICE.to_string(), AttrValue::from("orders")),
            ]),
        }
    }

    /// The hex form is the wire contract shared with `traceparent` and OTLP/JSON,
    /// so it is asserted literally rather than only round-tripped.
    #[test]
    fn trace_and_span_ids_are_lowercase_hex() {
        let span = sample();
        assert_eq!(span.trace_id.to_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(span.span_id.to_hex(), "00f067aa0ba902b7");

        assert_eq!(TraceId::from_hex(&span.trace_id.to_hex()).unwrap(), span.trace_id);
        assert_eq!(SpanId::from_hex(&span.span_id.to_hex()).unwrap(), span.span_id);

        // Serde sees a bare string, not a byte array.
        assert_eq!(
            serde_json::to_string(&span.span_id).unwrap(),
            "\"00f067aa0ba902b7\""
        );

        // Wrong length and non-hex both fail rather than truncating.
        assert!(TraceId::from_hex("4bf92f35").is_err());
        assert!(SpanId::from_hex("zzf067aa0ba902b7").is_err());

        // All-zero is the spec's invalid sentinel.
        assert!(!TraceId::INVALID.is_valid());
        assert!(!SpanId::INVALID.is_valid());
        assert!(span.trace_id.is_valid());
    }

    #[test]
    fn span_serde_round_trip() {
        let span = sample();
        let json = serde_json::to_string(&span).unwrap();
        let back: Span = serde_json::from_str(&json).unwrap();
        assert_eq!(back, span);

        // A root span with no parent, no attributes and an unset status is the
        // other extreme of the shape.
        let root = Span {
            parent_span_id: None,
            status: SpanStatus::Unset,
            attributes: BTreeMap::new(),
            kind: SpanKind::Client,
            ..sample()
        };
        let back: Span = serde_json::from_str(&serde_json::to_string(&root).unwrap()).unwrap();
        assert_eq!(back, root);
    }

    #[test]
    fn status_round_trips_through_its_column_pair() {
        for status in [
            SpanStatus::Unset,
            SpanStatus::Ok,
            SpanStatus::Error { message: None },
            SpanStatus::Error { message: Some("boom".to_string()) },
        ] {
            let back: SpanStatus =
                serde_json::from_str(&serde_json::to_string(&status).unwrap()).unwrap();
            assert_eq!(back, status);

            // The (status_code, status_message) representation a store writes.
            let rebuilt =
                SpanStatus::from_parts(status.code_str(), status.message().map(str::to_string))
                    .unwrap();
            assert_eq!(rebuilt, status);
        }
        assert!(SpanStatus::from_parts("weird", None).is_err());
        assert!(sample().is_error());
        assert!(!Span { status: SpanStatus::Ok, ..sample() }.is_error());
    }

    /// Attribute values must keep their JSON type — a status code compared as a
    /// string sorts "1000" before "500".
    #[test]
    fn attr_values_keep_their_type() {
        let attrs: BTreeMap<String, AttrValue> = serde_json::from_str(
            r#"{"a": 503, "b": "503", "c": 1.5, "d": true, "e": 2.0}"#,
        )
        .unwrap();
        assert_eq!(attrs["a"], AttrValue::Int(503));
        assert_eq!(attrs["b"], AttrValue::Str("503".to_string()));
        assert_eq!(attrs["c"], AttrValue::Double(1.5));
        assert_eq!(attrs["d"], AttrValue::Bool(true));
        assert_eq!(attrs["e"], AttrValue::Double(2.0));
        assert_eq!(attrs["a"].as_i64(), Some(503));
        assert_eq!(attrs["a"].as_str(), None);
    }

    #[test]
    fn peer_ident_prefers_the_logical_name() {
        let span = sample();
        assert_eq!(span.peer_ident(), Some("orders"));

        // Falls back to the address when no logical name was recorded.
        let mut attrs = span.attributes.clone();
        attrs.remove(ATTR_YAH_PEER_SERVICE);
        let addr_only = Span { attributes: attrs, ..sample() };
        assert_eq!(addr_only.peer_ident(), Some("100.64.0.2"));

        let bare = Span { attributes: BTreeMap::new(), ..sample() };
        assert_eq!(bare.peer_ident(), None);
    }

    #[test]
    fn rollup_windows_are_60s_tumbling() {
        assert_eq!(ROLLUP_WINDOW_NANOS, 60_000_000_000);
        assert_eq!(rollup_window_start(0), 0);
        assert_eq!(rollup_window_start(59_999_999_999), 0);
        assert_eq!(rollup_window_start(60_000_000_000), 60_000_000_000);
        assert_eq!(rollup_window_start(60_000_000_001), 60_000_000_000);
    }

    #[test]
    fn histogram_merge_is_exact_and_quantiles_are_bounded() {
        assert_eq!(LATENCY_BUCKET_COUNT, LATENCY_BUCKET_BOUNDS_NANOS.len() + 1);

        let mut a = LatencyHistogram::new();
        assert!(a.is_empty());
        assert_eq!(a.quantile(0.5), None);

        // 100 spans at 1ms, one at 30s (the unbounded overflow bucket).
        for _ in 0..100 {
            a.record(1_000_000);
        }
        let mut b = LatencyHistogram::new();
        b.record(30_000_000_000);

        let mut merged = a.clone();
        merged.merge(&b);
        assert_eq!(merged.count(), 101);
        assert_eq!(merged.count(), a.count() + b.count());
        assert_eq!(merged.sum_nanos, a.sum_nanos + b.sum_nanos);
        assert_eq!(merged.min_nanos, Some(1_000_000));
        assert_eq!(merged.max_nanos, Some(30_000_000_000));

        // Merging is order-independent and element-wise exact.
        let mut other_way = b.clone();
        other_way.merge(&a);
        assert_eq!(other_way, merged);

        // p50 and p99 both land in the `<= 5ms` bucket the 1ms crowd shares, so
        // they report that bucket's interpolated position — 2.55ms at rank 51
        // of 100, its 5ms upper bound at rank 100. Bucket resolution, not a
        // bug: the histogram cannot distinguish 1ms from 4ms after the fact.
        assert_eq!(merged.p50(), Some(2_550_000));
        assert_eq!(merged.p99(), Some(5_000_000));
        // The 30s outlier is only reachable at q=1.0, and comes back exact
        // because the overflow bucket has no upper bound to interpolate against
        // and falls back to the recorded max.
        assert_eq!(merged.quantile(1.0), Some(30_000_000_000));

        // The clamp does recover an exact value when the bucket's sample is
        // also the histogram's own extreme.
        let mut lone = LatencyHistogram::new();
        lone.record(1_000_000);
        assert_eq!(lone.p50(), Some(1_000_000));

        // An empty merge changes nothing.
        let before = merged.clone();
        merged.merge(&LatencyHistogram::new());
        assert_eq!(merged, before);
    }

    #[test]
    fn histogram_interpolates_inside_a_bucket() {
        let mut h = LatencyHistogram::new();
        // Straddle the 25ms boundary: 1 fast, 1 slow.
        h.record(1_000_000);
        h.record(200_000_000);
        assert_eq!(h.count(), 2);
        assert_eq!(h.mean_nanos(), Some(100_500_000));
        // p50 is rank 1 → the fast sample's `<= 5ms` bucket, reported at its
        // upper bound. The observed min does not clamp it, because 1ms is not
        // the value being estimated — the bucket is.
        assert_eq!(h.p50(), Some(5_000_000));
        // p95 is rank 2 → the slow sample's `<= 250ms` bucket, clamped back
        // down to the observed max of 200ms.
        assert_eq!(h.p95(), Some(200_000_000));

        let json = serde_json::to_string(&h).unwrap();
        let back: LatencyHistogram = serde_json::from_str(&json).unwrap();
        assert_eq!(back, h);

        // A bucket-count change is a schema change and must fail loudly, not
        // silently misattribute counts to shifted boundaries.
        let short = r#"{"counts":[1,2,3],"sum_nanos":6,"min_nanos":1,"max_nanos":3}"#;
        assert!(serde_json::from_str::<LatencyHistogram>(short).is_err());
        let long = format!(
            r#"{{"counts":[{}],"sum_nanos":6,"min_nanos":1,"max_nanos":3}}"#,
            vec!["0"; LATENCY_BUCKET_COUNT + 1].join(",")
        );
        assert!(serde_json::from_str::<LatencyHistogram>(&long).is_err());
    }

    #[test]
    fn hop_rollup_rates_use_the_requested_window() {
        let mut latency = LatencyHistogram::new();
        for _ in 0..120 {
            latency.record(2_000_000);
        }
        let hop = HopRollup {
            scope: EventScope::Service(MeshIdent("passway".to_string())),
            peer: "orders".to_string(),
            kind: SpanKind::Client,
            window_start_unix_nanos: 0,
            window_end_unix_nanos: ROLLUP_WINDOW_NANOS,
            error_count: 12,
            latency,
        };
        assert_eq!(hop.count(), 120);
        assert_eq!(hop.rate_per_sec(), 2.0); // 120 calls over a 60s window
        assert!((hop.error_rate() - 0.1).abs() < f64::EPSILON);

        let back: HopRollup =
            serde_json::from_str(&serde_json::to_string(&hop).unwrap()).unwrap();
        assert_eq!(back, hop);

        // An empty hop reports zeroes rather than dividing by zero.
        let empty = HopRollup {
            error_count: 0,
            latency: LatencyHistogram::new(),
            window_end_unix_nanos: 0,
            ..hop
        };
        assert_eq!(empty.error_rate(), 0.0);
        assert_eq!(empty.rate_per_sec(), 0.0);
    }
}
