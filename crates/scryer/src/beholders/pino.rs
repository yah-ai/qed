//! Pino beholder — Node.js NDJSON logger parser (pino v1 schema).
//!
//! Recognises lines of the form `{"level":30,"time":1617955768193,"msg":"..."}`.
//! Level integers map to observation levels as:
//!   >=60 → Fatal, >=50 → Error, >=40 → Warn, >=30 → Info, >=20 → Debug, else → Trace.
//!
//! `matches` attaches on explicit `yah.beholder=pino` image label or when the
//! workload env contains `NODE_ENV` (heuristic: this service is a Node.js app).
//! The first line is probed; if it doesn't parse as pino NDJSON, `unknown_format_reason`
//! is set so the supervisor can detach and fall back to the next beholder.
//!
//! Core pino fields (`level`, `time`, `pid`, `hostname`, `name`, `v`) are lifted
//! onto the Event struct; all remaining fields become `Event.fields`.

use observation::{Event, EventSource, Level};
use serde_json::{Map, Value};
use workload_spec::{ImageRef, MeshIdent};

use super::{BeholderCtx, LogLine, ServiceBeholder, ServiceBeholderFactory, ServiceHints};

pub const NAME: &str = "pino";
const VERSION: &str = "1";

/// Fields consumed by pino itself — not forwarded to `Event.fields`.
const CORE_KEYS: &[&str] = &["level", "time", "pid", "hostname", "v", "msg", "name"];

pub struct PinoBeholder {
    /// Default target when the log line carries no `name` field.
    target: String,
    unknown_format: Option<String>,
}

impl PinoBeholder {
    pub fn for_ident(ident: &MeshIdent) -> Self {
        Self { target: ident.0.clone(), unknown_format: None }
    }

    fn pino_level(n: i64) -> Level {
        if n >= 60 {
            Level::Fatal
        } else if n >= 50 {
            Level::Error
        } else if n >= 40 {
            Level::Warn
        } else if n >= 30 {
            Level::Info
        } else if n >= 20 {
            Level::Debug
        } else {
            Level::Trace
        }
    }

    /// Try to parse one pino NDJSON line.
    ///
    /// Returns `(level, target, msg, extra_fields)` or `None` if the line is
    /// not a valid pino record.
    fn try_parse(line: &str) -> Option<(Level, String, String, Value)> {
        let obj: Map<String, Value> = serde_json::from_str(line).ok()?;
        let level_num = obj.get("level")?.as_i64()?;
        let level = Self::pino_level(level_num);
        let msg = obj.get("msg")?.as_str()?.to_string();
        let target = obj
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let mut fields = Map::new();
        for (k, v) in &obj {
            if !CORE_KEYS.contains(&k.as_str()) {
                fields.insert(k.clone(), v.clone());
            }
        }
        Some((level, target, msg, Value::Object(fields)))
    }
}

impl ServiceBeholder for PinoBeholder {
    fn name(&self) -> &'static str {
        NAME
    }
    fn version(&self) -> &'static str {
        VERSION
    }
    fn matches(&self, _ident: &MeshIdent, _image: &ImageRef, hints: &ServiceHints) -> bool {
        hints.forced_beholder() == Some(NAME)
            || hints.env.contains_key("NODE_ENV")
            || hints.env.contains_key("npm_package_version")
    }
    fn parse_line(&mut self, line: &LogLine, ctx: &mut BeholderCtx) -> Vec<Event> {
        if self.unknown_format.is_some() || line.line.is_empty() {
            return Vec::new();
        }
        match Self::try_parse(&line.line) {
            None => {
                self.unknown_format =
                    Some("line did not parse as pino NDJSON (missing level/msg)".to_string());
                Vec::new()
            }
            Some((level, name_target, msg, fields)) => {
                let target =
                    if name_target.is_empty() { self.target.clone() } else { name_target };
                let ev = ctx.make_event(
                    line,
                    level,
                    target,
                    msg,
                    fields,
                    EventSource::Beholder {
                        name: NAME.to_string(),
                        version: VERSION.to_string(),
                    },
                );
                vec![ev]
            }
        }
    }
    fn unknown_format_reason(&self) -> Option<&str> {
        self.unknown_format.as_deref()
    }
}

pub struct PinoFactory;

impl ServiceBeholderFactory for PinoFactory {
    fn name(&self) -> &'static str {
        NAME
    }
    fn version(&self) -> &'static str {
        VERSION
    }
    fn matches(&self, _ident: &MeshIdent, _image: &ImageRef, hints: &ServiceHints) -> bool {
        hints.forced_beholder() == Some(NAME)
            || hints.env.contains_key("NODE_ENV")
            || hints.env.contains_key("npm_package_version")
    }
    fn create(&self) -> Box<dyn ServiceBeholder> {
        Box::new(PinoBeholder { target: String::new(), unknown_format: None })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    fn ctx() -> BeholderCtx {
        BeholderCtx::new()
    }

    fn ident(s: &str) -> MeshIdent {
        MeshIdent(s.to_string())
    }

    /// Fixture: a representative set of pino NDJSON lines covering all standard
    /// level integers.
    pub fn pino_fixtures() -> &'static [(&'static str, Level, &'static str)] {
        &[
            (r#"{"level":10,"time":1617955768193,"pid":1,"hostname":"box","msg":"trace msg"}"#, Level::Trace, "trace msg"),
            (r#"{"level":20,"time":1617955768194,"pid":1,"hostname":"box","msg":"debug msg"}"#, Level::Debug, "debug msg"),
            (r#"{"level":30,"time":1617955768195,"pid":1,"hostname":"box","msg":"info msg"}"#, Level::Info, "info msg"),
            (r#"{"level":40,"time":1617955768196,"pid":1,"hostname":"box","msg":"warn msg"}"#, Level::Warn, "warn msg"),
            (r#"{"level":50,"time":1617955768197,"pid":1,"hostname":"box","msg":"error msg"}"#, Level::Error, "error msg"),
            (r#"{"level":60,"time":1617955768198,"pid":1,"hostname":"box","msg":"fatal msg"}"#, Level::Fatal, "fatal msg"),
        ]
    }

    #[test]
    fn pino_parses_all_standard_levels() {
        for &(raw, expected_level, expected_msg) in pino_fixtures() {
            let mut b = PinoBeholder::for_ident(&ident("api.pdx"));
            let mut c = ctx();
            let line = LogLine { line: raw.to_string(), offset_ms: 0 };
            let events = b.parse_line(&line, &mut c);
            assert_eq!(events.len(), 1, "should parse: {raw}");
            assert_eq!(events[0].level, expected_level, "level mismatch: {raw}");
            assert_eq!(events[0].msg, expected_msg, "msg mismatch: {raw}");
            assert!(b.unknown_format_reason().is_none(), "should not decline: {raw}");
        }
    }

    #[test]
    fn pino_lifts_extra_fields() {
        let mut b = PinoBeholder::for_ident(&ident("svc.local"));
        let mut c = ctx();
        let raw = r#"{"level":30,"time":1617955768195,"pid":1,"hostname":"box","msg":"req","url":"/api","status":200}"#;
        let line = LogLine { line: raw.to_string(), offset_ms: 0 };
        let events = b.parse_line(&line, &mut c);
        assert_eq!(events.len(), 1);
        let fields = &events[0].fields;
        assert_eq!(fields["url"], "/api");
        assert_eq!(fields["status"], 200);
        // Core keys must not appear in fields.
        assert!(fields.get("level").is_none());
        assert!(fields.get("time").is_none());
        assert!(fields.get("pid").is_none());
        assert!(fields.get("hostname").is_none());
        assert!(fields.get("msg").is_none());
    }

    #[test]
    fn pino_uses_name_field_as_target() {
        let mut b = PinoBeholder::for_ident(&ident("svc.local"));
        let mut c = ctx();
        let raw = r#"{"level":30,"time":1617955768195,"pid":1,"hostname":"box","name":"myapp","msg":"hello"}"#;
        let line = LogLine { line: raw.to_string(), offset_ms: 0 };
        let events = b.parse_line(&line, &mut c);
        assert_eq!(events[0].target, "myapp");
    }

    #[test]
    fn pino_falls_back_to_ident_target_when_no_name() {
        let mut b = PinoBeholder::for_ident(&ident("svc.local"));
        let mut c = ctx();
        let raw = r#"{"level":30,"time":1617955768195,"pid":1,"hostname":"box","msg":"hello"}"#;
        let line = LogLine { line: raw.to_string(), offset_ms: 0 };
        let events = b.parse_line(&line, &mut c);
        assert_eq!(events[0].target, "svc.local");
    }

    #[test]
    fn pino_declines_non_ndjson() {
        let mut b = PinoBeholder::for_ident(&ident("svc.local"));
        let mut c = ctx();
        let line = LogLine { line: "plain text log line".to_string(), offset_ms: 0 };
        let events = b.parse_line(&line, &mut c);
        assert!(events.is_empty());
        assert!(b.unknown_format_reason().is_some(), "should set unknown_format");
    }

    #[test]
    fn pino_declines_json_without_required_fields() {
        let mut b = PinoBeholder::for_ident(&ident("svc.local"));
        let mut c = ctx();
        // JSON but missing the required `msg` field.
        let line = LogLine { line: r#"{"level":30,"time":12345}"#.to_string(), offset_ms: 0 };
        let events = b.parse_line(&line, &mut c);
        assert!(events.is_empty());
        assert!(b.unknown_format_reason().is_some());
    }

    #[test]
    fn pino_drops_lines_after_decline() {
        let mut b = PinoBeholder::for_ident(&ident("svc.local"));
        let mut c = ctx();
        // Trigger decline.
        b.parse_line(&LogLine { line: "not pino".to_string(), offset_ms: 0 }, &mut c);
        assert!(b.unknown_format_reason().is_some());
        // Valid pino line after decline → still returns empty.
        let valid = r#"{"level":30,"time":1,"msg":"hello"}"#;
        let events =
            b.parse_line(&LogLine { line: valid.to_string(), offset_ms: 0 }, &mut c);
        assert!(events.is_empty());
    }

    #[test]
    fn pino_level_boundaries() {
        // Custom levels just above/below standard boundaries.
        let cases = [
            (9i64, Level::Trace),
            (10, Level::Trace),
            (15, Level::Trace),
            (19, Level::Trace),
            (20, Level::Debug),
            (25, Level::Debug),
            (29, Level::Debug),
            (30, Level::Info),
            (35, Level::Info),
            (39, Level::Info),
            (40, Level::Warn),
            (45, Level::Warn),
            (49, Level::Warn),
            (50, Level::Error),
            (55, Level::Error),
            (59, Level::Error),
            (60, Level::Fatal),
            (70, Level::Fatal),
        ];
        for (n, expected) in cases {
            assert_eq!(PinoBeholder::pino_level(n), expected, "level {n}");
        }
    }
}
