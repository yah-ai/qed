//! The scryer **ingestion line protocol** — the wire shape a local emitter
//! writes to `YAH_SCRYER_SOCKET`, one JSON object per line.
//!
//! R893-F16 decided the open question F15 left open ("how do spans reach
//! scryer on the wire") and this module is the answer: **the existing
//! Unix-socket line protocol, made explicitly signal-tagged.** The reasoning,
//! recorded here rather than in a handoff so the next editor finds it at the
//! code site:
//!
//! - **Reuse, not a second channel.** scryer already owns a local-agent
//!   ingestion socket ([`crate`]-adjacent, `scryer::ingestion::IngestionServer`)
//!   with a per-`MeshIdent` quota, a store behind it and a deploy-time env
//!   contract (`YAH_SERVICE_IDENT` + `YAH_SCRYER_SOCKET`) that R893-B17 is
//!   already fixing. A span emitter needs exactly that: a same-machine,
//!   non-blocking, no-discovery sink. A second transport would need its own
//!   address, its own quota and its own deploy-time answer to "where is my
//!   collector" — three problems the socket has already solved once.
//! - **Not the `:6543` HTTP listener.** That is scryer's *federation* surface
//!   (cross-machine reads). A per-request emitter on the same box paying HTTP
//!   framing, a connection pool and a TLS-or-not decision to reach a process
//!   whose socket is in the same mount namespace is strictly worse.
//! - **Tagged, not shape-sniffed.** The pre-tag protocol was a bare event
//!   object, so adding spans could have been done by probing for a `span` key.
//!   That is the compatibility shim CLAUDE.md's "Below v1.0.0" section forbids:
//!   the discriminator would live in the *absence* of a field and every future
//!   signal would widen the guess. `signal` is required on every line. There
//!   are no live producers to break — `YAH_SCRYER_SOCKET` is injected nowhere
//!   today (R893-B17, re-grepped by R893-F16) — so the break costs nothing now
//!   and would cost a migration later.
//!
//! Both sides of the protocol name this one type: `scryer::ingestion`
//! deserializes it, and emitters serialize it. `crates/yah/log`'s
//! `YahLogServiceLayer` is the exception and deliberately so — it builds its
//! line as a `serde_json::Map` because a `tracing` layer assembles fields
//! dynamically; it writes the same `"signal":"event"` tag and is covered by
//! [`tests::the_yah_log_line_shape_still_parses`].

use serde::{Deserialize, Serialize};

use crate::types::Span;

/// One line on the ingestion socket.
///
/// Unknown keys are ignored (`_lib` / `_lib_ver` from `yah-log` ride along
/// harmlessly), but `signal` is mandatory: a line without it is a malformed
/// line, not an event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "signal", rename_all = "snake_case")]
pub enum IngestLine {
    /// A log line — the original and, until R893-F16, only shape.
    Event {
        scope_kind: String,
        scope_id: String,
        level: String,
        target: String,
        msg: String,
        #[serde(default)]
        fields: serde_json::Value,
    },
    /// One finished OTel span, already carrying its own duration and status.
    ///
    /// Scoped by the envelope rather than by the span, exactly as an event is
    /// — see [`Span`]'s doc: a span and a log line must agree on what a
    /// service is.
    Span {
        scope_kind: String,
        scope_id: String,
        span: Box<Span>,
    },
}

impl IngestLine {
    /// `(scope_kind, scope_id)` — the pair the receiver turns into an
    /// [`crate::EventScope`].
    pub fn scope(&self) -> (&str, &str) {
        match self {
            IngestLine::Event { scope_kind, scope_id, .. }
            | IngestLine::Span { scope_kind, scope_id, .. } => (scope_kind, scope_id),
        }
    }

    /// Build the span variant for a service-scoped emitter.
    pub fn span(service_ident: impl Into<String>, span: Span) -> Self {
        IngestLine::Span {
            scope_kind: "service".to_string(),
            scope_id: service_ident.into(),
            span: Box::new(span),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SpanId, SpanKind, SpanStatus, TraceId};

    fn a_span() -> Span {
        Span {
            trace_id: TraceId([1u8; 16]),
            span_id: SpanId([2u8; 8]),
            parent_span_id: None,
            name: "GET /".to_string(),
            kind: SpanKind::Server,
            start_unix_nanos: 1_700_000_000_000_000_000,
            duration_nanos: 3_000_000,
            status: SpanStatus::Ok,
            attributes: Default::default(),
        }
    }

    #[test]
    fn span_lines_round_trip_through_the_tag() {
        let line = IngestLine::span("svc.host", a_span());
        let text = serde_json::to_string(&line).unwrap();
        assert!(text.contains(r#""signal":"span""#), "tag must be on the wire: {text}");
        assert_eq!(serde_json::from_str::<IngestLine>(&text).unwrap(), line);
        assert_eq!(line.scope(), ("service", "svc.host"));
    }

    /// The exact object `crates/yah/log`'s `YahLogServiceLayer` writes, built
    /// by hand there rather than through this type. If this stops parsing, that
    /// producer and this protocol have diverged.
    #[test]
    fn the_yah_log_line_shape_still_parses() {
        let text = r#"{"signal":"event","scope_kind":"service","scope_id":"my-service.host","level":"info","target":"t","msg":"m","fields":{"service_ident":"my-service.host"},"_lib":"yah-log","_lib_ver":"0.1.0"}"#;
        let line: IngestLine = serde_json::from_str(text).unwrap();
        assert_eq!(line.scope(), ("service", "my-service.host"));
        assert!(matches!(line, IngestLine::Event { .. }));
    }

    /// The break R893-F16 took deliberately: an untagged line is rejected, not
    /// guessed at. See this module's doc for why a default was the wrong shape.
    #[test]
    fn an_untagged_line_is_rejected_rather_than_assumed_to_be_an_event() {
        let text = r#"{"scope_kind":"service","scope_id":"x","level":"info","target":"t","msg":"m","fields":{}}"#;
        assert!(serde_json::from_str::<IngestLine>(text).is_err());
    }
}
