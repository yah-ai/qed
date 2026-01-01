//! Core observation types shared between task-runs and scryer.
//!
//! Types here are intentionally free of I/O — store layers own persistence.

use serde::{Deserialize, Serialize};
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
