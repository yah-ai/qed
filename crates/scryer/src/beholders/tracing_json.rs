//! tracing-json beholder — Rust `tracing-subscriber` JSON output parser.
//!
//! Recognises the default format emitted by `tracing-subscriber`'s JSON layer:
//!
//! ```json
//! {"timestamp":"2021-04-20T04:52:48.645420Z","level":"INFO",
//!  "fields":{"message":"hello","key":"val"},"target":"myapp::module",
//!  "span":{"id":1,"name":"req"},"spans":[...]}
//! ```
//!
//! Level strings: "TRACE", "DEBUG", "INFO", "WARN", "ERROR".  tracing's max
//! level is ERROR; there is no FATAL, so unknown strings fall to Info.
//!
//! `matches` attaches on explicit `yah.beholder=tracing-json` image label or
//! when the workload env contains `RUST_LOG` (heuristic: Rust tracing service).
//! On the first line that fails to parse as tracing-subscriber JSON,
//! `unknown_format_reason` is set so the supervisor can fall back.
//!
//! Extracted fields: `fields.*` from the tracing event become `Event.fields`;
//! `message` (inside `fields`) becomes `Event.msg`.  Span info, if present,
//! is forwarded as `fields.span.*`.

use observation::{Event, EventSource, Level};
use serde_json::{Map, Value};
use workload_spec::{ImageRef, MeshIdent};

use super::{BeholderCtx, LogLine, ServiceBeholder, ServiceBeholderFactory, ServiceHints};

pub const NAME: &str = "tracing-json";
const VERSION: &str = "1";

pub struct TracingJsonBeholder {
    target: String,
    unknown_format: Option<String>,
}

impl TracingJsonBeholder {
    pub fn for_ident(ident: &MeshIdent) -> Self {
        Self { target: ident.0.clone(), unknown_format: None }
    }

    fn parse_level(s: &str) -> Level {
        match s.to_ascii_uppercase().as_str() {
            "TRACE" => Level::Trace,
            "DEBUG" => Level::Debug,
            "INFO" => Level::Info,
            "WARN" | "WARNING" => Level::Warn,
            "ERROR" => Level::Error,
            _ => Level::Info,
        }
    }

    /// Try to parse one tracing-subscriber JSON line.
    ///
    /// Returns `(level, target, msg, fields)` or `None` if the line is not
    /// a valid tracing-subscriber record.
    fn try_parse(line: &str, default_target: &str) -> Option<(Level, String, String, Value)> {
        let obj: Map<String, Value> = serde_json::from_str(line).ok()?;
        let level_str = obj.get("level")?.as_str()?;
        let level = Self::parse_level(level_str);
        let target = obj
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or(default_target)
            .to_string();

        // `message` lives inside the `fields` map.
        let fields_obj = obj.get("fields").and_then(|v| v.as_object());
        let msg = fields_obj
            .and_then(|f| f.get("message"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if msg.is_empty() {
            return None;
        }

        // Build Event.fields: carry everything from `fields` except `message`;
        // carry span info under `span.*` if present.
        let mut out_fields = Map::new();
        if let Some(fmap) = fields_obj {
            for (k, v) in fmap {
                if k != "message" {
                    out_fields.insert(k.clone(), v.clone());
                }
            }
        }
        if let Some(span) = obj.get("span") {
            out_fields.insert("span".to_string(), span.clone());
        }

        Some((level, target, msg, Value::Object(out_fields)))
    }
}

impl ServiceBeholder for TracingJsonBeholder {
    fn name(&self) -> &'static str {
        NAME
    }
    fn version(&self) -> &'static str {
        VERSION
    }
    fn matches(&self, _ident: &MeshIdent, _image: &ImageRef, hints: &ServiceHints) -> bool {
        hints.forced_beholder() == Some(NAME) || hints.env.contains_key("RUST_LOG")
    }
    fn parse_line(&mut self, line: &LogLine, ctx: &mut BeholderCtx) -> Vec<Event> {
        if self.unknown_format.is_some() || line.line.is_empty() {
            return Vec::new();
        }
        match Self::try_parse(&line.line, &self.target) {
            None => {
                self.unknown_format = Some(
                    "line did not parse as tracing-subscriber JSON (missing level/fields.message)"
                        .to_string(),
                );
                Vec::new()
            }
            Some((level, target, msg, fields)) => {
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

pub struct TracingJsonFactory;

impl ServiceBeholderFactory for TracingJsonFactory {
    fn name(&self) -> &'static str {
        NAME
    }
    fn version(&self) -> &'static str {
        VERSION
    }
    fn matches(&self, _ident: &MeshIdent, _image: &ImageRef, hints: &ServiceHints) -> bool {
        hints.forced_beholder() == Some(NAME) || hints.env.contains_key("RUST_LOG")
    }
    fn create(&self) -> Box<dyn ServiceBeholder> {
        Box::new(TracingJsonBeholder { target: String::new(), unknown_format: None })
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

    /// Representative fixture lines emitted by `tracing_subscriber::fmt::format::Json`.
    pub fn tracing_fixtures() -> &'static [(&'static str, Level, &'static str, &'static str)] {
        &[
            (
                r#"{"timestamp":"2021-04-20T04:52:48.645420Z","level":"TRACE","fields":{"message":"trace detail"},"target":"myapp::inner","span":{"id":1,"name":"root"}}"#,
                Level::Trace, "trace detail", "myapp::inner",
            ),
            (
                r#"{"timestamp":"2021-04-20T04:52:48.645420Z","level":"DEBUG","fields":{"message":"debug info","key":"val"},"target":"myapp"}"#,
                Level::Debug, "debug info", "myapp",
            ),
            (
                r#"{"timestamp":"2021-04-20T04:52:48.645420Z","level":"INFO","fields":{"message":"server listening","addr":"0.0.0.0:3000"},"target":"myapp::server"}"#,
                Level::Info, "server listening", "myapp::server",
            ),
            (
                r#"{"timestamp":"2021-04-20T04:52:48.645420Z","level":"WARN","fields":{"message":"slow query","latency_ms":500},"target":"myapp::db"}"#,
                Level::Warn, "slow query", "myapp::db",
            ),
            (
                r#"{"timestamp":"2021-04-20T04:52:48.645420Z","level":"ERROR","fields":{"message":"connection refused","code":"ECONNREFUSED"},"target":"myapp::client"}"#,
                Level::Error, "connection refused", "myapp::client",
            ),
        ]
    }

    #[test]
    fn tracing_json_parses_all_levels() {
        for &(raw, expected_level, expected_msg, expected_target) in tracing_fixtures() {
            let mut b = TracingJsonBeholder::for_ident(&ident("svc.local"));
            let mut c = ctx();
            let line = LogLine { line: raw.to_string(), offset_ms: 0 };
            let events = b.parse_line(&line, &mut c);
            assert_eq!(events.len(), 1, "should parse: {raw}");
            assert_eq!(events[0].level, expected_level, "level: {raw}");
            assert_eq!(events[0].msg, expected_msg, "msg: {raw}");
            assert_eq!(events[0].target, expected_target, "target: {raw}");
            assert!(b.unknown_format_reason().is_none(), "should not decline: {raw}");
        }
    }

    #[test]
    fn tracing_json_lifts_extra_fields() {
        let mut b = TracingJsonBeholder::for_ident(&ident("svc.local"));
        let mut c = ctx();
        let raw = r#"{"timestamp":"2021-04-20T04:52:48.645420Z","level":"INFO","fields":{"message":"req","url":"/api","status":200},"target":"myapp"}"#;
        let line = LogLine { line: raw.to_string(), offset_ms: 0 };
        let events = b.parse_line(&line, &mut c);
        assert_eq!(events.len(), 1);
        let fields = &events[0].fields;
        assert_eq!(fields["url"], "/api");
        assert_eq!(fields["status"], 200);
        assert!(fields.get("message").is_none(), "message must not appear in fields");
    }

    #[test]
    fn tracing_json_forwards_span() {
        let mut b = TracingJsonBeholder::for_ident(&ident("svc.local"));
        let mut c = ctx();
        let raw = r#"{"timestamp":"2021-04-20T04:52:48.645420Z","level":"INFO","fields":{"message":"ok"},"target":"myapp","span":{"id":1,"name":"request"}}"#;
        let line = LogLine { line: raw.to_string(), offset_ms: 0 };
        let events = b.parse_line(&line, &mut c);
        assert_eq!(events.len(), 1);
        let span = &events[0].fields["span"];
        assert_eq!(span["name"], "request");
    }

    #[test]
    fn tracing_json_falls_back_to_ident_target() {
        let mut b = TracingJsonBeholder::for_ident(&ident("svc.local"));
        let mut c = ctx();
        // No `target` field in the line.
        let raw = r#"{"timestamp":"2021-04-20T04:52:48.645420Z","level":"INFO","fields":{"message":"hello"}}"#;
        let line = LogLine { line: raw.to_string(), offset_ms: 0 };
        let events = b.parse_line(&line, &mut c);
        assert_eq!(events[0].target, "svc.local");
    }

    #[test]
    fn tracing_json_declines_non_json() {
        let mut b = TracingJsonBeholder::for_ident(&ident("svc.local"));
        let mut c = ctx();
        let line = LogLine { line: "plain text output".to_string(), offset_ms: 0 };
        let events = b.parse_line(&line, &mut c);
        assert!(events.is_empty());
        assert!(b.unknown_format_reason().is_some());
    }

    #[test]
    fn tracing_json_declines_json_without_message() {
        let mut b = TracingJsonBeholder::for_ident(&ident("svc.local"));
        let mut c = ctx();
        // JSON but `fields` has no `message` key.
        let raw = r#"{"level":"INFO","fields":{"key":"val"},"target":"app"}"#;
        let line = LogLine { line: raw.to_string(), offset_ms: 0 };
        let events = b.parse_line(&line, &mut c);
        assert!(events.is_empty());
        assert!(b.unknown_format_reason().is_some());
    }

    #[test]
    fn tracing_json_drops_lines_after_decline() {
        let mut b = TracingJsonBeholder::for_ident(&ident("svc.local"));
        let mut c = ctx();
        b.parse_line(&LogLine { line: "not tracing json".to_string(), offset_ms: 0 }, &mut c);
        assert!(b.unknown_format_reason().is_some());
        let valid = r#"{"timestamp":"2021-04-20T04:52:48.645420Z","level":"INFO","fields":{"message":"hello"},"target":"app"}"#;
        let events =
            b.parse_line(&LogLine { line: valid.to_string(), offset_ms: 0 }, &mut c);
        assert!(events.is_empty());
    }
}
