//! Service beholders — line-by-line parsers for long-running service log streams.
//!
//! A `ServiceBeholder` is the analog of a `task_runs::Beholder` for the
//! Service event scope. The differences (per arch doc §"Relationship to the
//! existing pieces"):
//!
//! - **Lifetime**: bound to a `MeshIdent`, lives as long as the service does.
//! - **Ingress**: reads pre-line-split log lines from an adapter, not pty bytes.
//! - **Storage key**: `(service_ident, seq)` rather than `(run_id, seq)`.
//!
//! Bundled set (F2 + F3):
//! - [`pino::PinoBeholder`] — Node.js pino NDJSON parser (most common for Node services).
//! - [`tracing_json::TracingJsonBeholder`] — Rust `tracing-subscriber` JSON output.
//! - [`vanilla::VanillaBeholder`] — rfc5424-ish `[LEVEL] message` parser.
//! - [`unstructured::UnstructuredBeholder`] — passthrough; never declines.
//!
//! Schema versioning: beholders expose [`ServiceBeholder::unknown_format_reason`].
//! When `Some`, the supervisor detaches and falls back to the next registry entry.

use observation::{Event, EventSource, Level, TaskRunId};
use std::collections::HashMap;
use workload_spec::{ImageRef, MeshIdent};

pub mod pino;
pub mod tracing_json;
pub mod unstructured;
pub mod vanilla;

pub use pino::{PinoBeholder, PinoFactory};
pub use tracing_json::{TracingJsonBeholder, TracingJsonFactory};
pub use unstructured::UnstructuredBeholder;
pub use vanilla::VanillaBeholder;

// ─── LogLine ──────────────────────────────────────────────────────────────────

/// A single log line handed from an adapter to a beholder.
///
/// `offset_ms` is monotonic-from-adapter-start; service-scope events don't
/// have a TaskRun's deterministic start anchor, so the supervisor records its
/// own start time and assigns offsets relative to that.
#[derive(Debug, Clone)]
pub struct LogLine {
    pub line: String,
    pub offset_ms: u32,
}

// ─── ServiceHints ─────────────────────────────────────────────────────────────

/// Out-of-band hints a beholder uses to decide whether to attach.
///
/// `image_labels` carries OCI labels; `yah.beholder=<name>` is the
/// declarative escape hatch — when present it forces a specific beholder
/// without scryer needing per-app rules.
#[derive(Debug, Clone, Default)]
pub struct ServiceHints {
    pub image_labels: HashMap<String, String>,
    pub env: HashMap<String, String>,
}

impl ServiceHints {
    pub fn forced_beholder(&self) -> Option<&str> {
        self.image_labels.get("yah.beholder").map(|s| s.as_str())
    }
}

// ─── BeholderCtx ──────────────────────────────────────────────────────────────

/// Per-attachment state that the beholder mutates as it parses.
///
/// `seq` is monotonic per `(scope_kind, scope_id)`; the supervisor advances it
/// as it commits emitted events.
pub struct BeholderCtx {
    pub seq: u32,
    /// Synthetic `run_id` field on `Event`; service-scope events still carry a
    /// `TaskRunId` for backward-compat with the row shape, but it's a stable
    /// per-service ident in disguise. Storage keys off `(scope_kind, scope_id)`.
    pub run_id_anchor: TaskRunId,
}

impl BeholderCtx {
    pub fn new() -> Self {
        Self { seq: 0, run_id_anchor: TaskRunId::new() }
    }

    pub fn next_seq(&mut self) -> u32 {
        let s = self.seq;
        self.seq += 1;
        s
    }

    /// Helper: build a stock `Event` from level/target/msg/fields with the
    /// running counters. Beholders compose this rather than open-coding it.
    pub fn make_event(
        &mut self,
        line: &LogLine,
        level: Level,
        target: String,
        msg: String,
        fields: serde_json::Value,
        source: EventSource,
    ) -> Event {
        Event {
            run_id: self.run_id_anchor.clone(),
            seq: self.next_seq(),
            offset_ms: line.offset_ms,
            level,
            target,
            msg,
            fields,
            anchor: None,
            source,
        }
    }
}

impl Default for BeholderCtx {
    fn default() -> Self {
        Self::new()
    }
}

// ─── ServiceBeholder ──────────────────────────────────────────────────────────

/// Per-service stateful line parser.
///
/// `matches` is consulted at attach time, not per-line. `parse_line` runs once
/// per log line and may emit zero or more events. Implementations must be
/// streaming — buffering until EOF defeats the live-tail use case.
///
/// Schema versioning: if `unknown_format_reason` returns `Some`, the supervisor
/// detaches this beholder and tries the next registry entry (or falls back to
/// `unstructured`).  Beholders set this on the first line that definitively
/// fails to match their format.
pub trait ServiceBeholder: Send {
    fn name(&self) -> &'static str;
    fn version(&self) -> &'static str;
    /// True when this beholder claims the workload. Earlier-registered
    /// beholders win ties.
    fn matches(&self, ident: &MeshIdent, image: &ImageRef, hints: &ServiceHints) -> bool;
    fn parse_line(&mut self, line: &LogLine, ctx: &mut BeholderCtx) -> Vec<Event>;
    /// Returns a reason string when the beholder has detected that the log
    /// format is unrecognised.  `Some` → supervisor should detach.
    /// Default: `None` (format ok, or not yet probed).
    fn unknown_format_reason(&self) -> Option<&str> {
        None
    }
}

// ─── ServiceBeholderRegistry ──────────────────────────────────────────────────

/// Registry of available service beholders consulted at adapter-attach time.
///
/// Walks entries in priority order; first `matches` hit wins. The
/// `unstructured` beholder lives at the tail so nothing is ever silently
/// dropped.
pub struct ServiceBeholderRegistry {
    factories: Vec<Box<dyn ServiceBeholderFactory>>,
}

/// Factory for a `ServiceBeholder`. Holds version metadata and creates fresh
/// per-attachment instances.
pub trait ServiceBeholderFactory: Send + Sync {
    fn name(&self) -> &'static str;
    fn version(&self) -> &'static str;
    fn matches(&self, ident: &MeshIdent, image: &ImageRef, hints: &ServiceHints) -> bool;
    fn create(&self) -> Box<dyn ServiceBeholder>;
}

impl ServiceBeholderRegistry {
    pub fn new() -> Self {
        Self { factories: Vec::new() }
    }

    pub fn register(&mut self, factory: Box<dyn ServiceBeholderFactory>) {
        self.factories.push(factory);
    }

    /// Resolve a beholder for the given workload. Walks `image_labels`
    /// first (`yah.beholder=<name>` is a hard override), then tries each
    /// factory in priority order. If no factory matches, `unstructured`
    /// is the documented fallback — callers are expected to register it.
    pub fn attach(
        &self,
        ident: &MeshIdent,
        image: &ImageRef,
        hints: &ServiceHints,
    ) -> Option<(Box<dyn ServiceBeholder>, &'static str)> {
        if let Some(forced) = hints.forced_beholder() {
            if let Some(f) = self.factories.iter().find(|f| f.name() == forced) {
                return Some((f.create(), f.name()));
            }
        }
        for f in &self.factories {
            if f.matches(ident, image, hints) {
                return Some((f.create(), f.name()));
            }
        }
        None
    }

    /// Convenience constructor with all bundled factories in priority order:
    /// pino → tracing-json → vanilla → unstructured.
    ///
    /// Specific parsers (pino, tracing-json) match on env/label heuristics and
    /// decline via `unknown_format_reason` if the format turns out to be wrong.
    /// Vanilla matches on explicit label only.  Unstructured is the tail
    /// fallback that ensures nothing is silently dropped.
    pub fn with_bundled() -> Self {
        let mut r = Self::new();
        r.register(Box::new(pino::PinoFactory));
        r.register(Box::new(tracing_json::TracingJsonFactory));
        r.register(Box::new(vanilla::VanillaFactory));
        r.register(Box::new(unstructured::UnstructuredFactory));
        r
    }
}

impl Default for ServiceBeholderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_hints() -> ServiceHints {
        ServiceHints::default()
    }

    fn label_hints(beholder: &str) -> ServiceHints {
        let mut h = ServiceHints::default();
        h.image_labels.insert("yah.beholder".to_string(), beholder.to_string());
        h
    }

    fn env_hints(key: &str, val: &str) -> ServiceHints {
        let mut h = ServiceHints::default();
        h.env.insert(key.to_string(), val.to_string());
        h
    }

    fn ident(s: &str) -> MeshIdent {
        MeshIdent(s.to_string())
    }

    fn image() -> ImageRef {
        ImageRef {
            registry: "ghcr.io".to_string(),
            repository: "test/svc".to_string(),
            tag: "latest".to_string(),
            digest: workload_spec::testing::test_digest(),
        }
    }

    // ─── image_label tests ───────────────────────────────────────────────────

    /// `yah.beholder=pino` forces pino regardless of content.
    #[test]
    fn image_label_forces_pino() {
        let reg = ServiceBeholderRegistry::with_bundled();
        let hints = label_hints("pino");
        let result = reg.attach(&ident("svc.a"), &image(), &hints);
        assert!(result.is_some());
        let (_, name) = result.unwrap();
        assert_eq!(name, pino::NAME);
    }

    /// `yah.beholder=tracing-json` forces tracing-json regardless of content.
    #[test]
    fn image_label_forces_tracing_json() {
        let reg = ServiceBeholderRegistry::with_bundled();
        let hints = label_hints("tracing-json");
        let result = reg.attach(&ident("svc.a"), &image(), &hints);
        assert!(result.is_some());
        let (_, name) = result.unwrap();
        assert_eq!(name, tracing_json::NAME);
    }

    /// `yah.beholder=vanilla` forces vanilla.
    #[test]
    fn image_label_forces_vanilla() {
        let reg = ServiceBeholderRegistry::with_bundled();
        let hints = label_hints("vanilla");
        let result = reg.attach(&ident("svc.a"), &image(), &hints);
        assert!(result.is_some());
        let (_, name) = result.unwrap();
        assert_eq!(name, "vanilla");
    }

    /// Unknown label falls through to match by env heuristics or unstructured.
    #[test]
    fn unknown_image_label_falls_through() {
        let reg = ServiceBeholderRegistry::with_bundled();
        let hints = label_hints("not-a-real-beholder");
        // No env heuristic → unstructured catches it.
        let result = reg.attach(&ident("svc.a"), &image(), &hints);
        assert!(result.is_some());
        let (_, name) = result.unwrap();
        assert_eq!(name, "unstructured");
    }

    /// Service with no hints → unstructured fallback.
    #[test]
    fn no_hints_falls_through_to_unstructured() {
        let reg = ServiceBeholderRegistry::with_bundled();
        let result = reg.attach(&ident("svc.a"), &image(), &no_hints());
        assert!(result.is_some());
        let (_, name) = result.unwrap();
        assert_eq!(name, "unstructured");
    }

    /// NODE_ENV env var → pino wins over tracing-json and vanilla.
    #[test]
    fn node_env_selects_pino() {
        let reg = ServiceBeholderRegistry::with_bundled();
        let hints = env_hints("NODE_ENV", "production");
        let result = reg.attach(&ident("svc.a"), &image(), &hints);
        assert!(result.is_some());
        let (_, name) = result.unwrap();
        assert_eq!(name, pino::NAME);
    }

    /// RUST_LOG env var → tracing-json (pino didn't match because no NODE_ENV).
    #[test]
    fn rust_log_selects_tracing_json() {
        let reg = ServiceBeholderRegistry::with_bundled();
        let hints = env_hints("RUST_LOG", "info");
        let result = reg.attach(&ident("svc.a"), &image(), &hints);
        assert!(result.is_some());
        let (_, name) = result.unwrap();
        assert_eq!(name, tracing_json::NAME);
    }
}
