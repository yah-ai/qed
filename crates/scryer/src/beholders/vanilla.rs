//! Vanilla beholder — `[LEVEL] message` rfc5424-ish parser.
//!
//! Recognises a leading bracketed level token (`[INFO]`, `[warn]`, etc.) and
//! lifts it onto `Event.level`. Everything after the bracket becomes `msg`.
//! When the line lacks a recognisable bracket prefix, declines to parse and
//! returns an empty vec — the unstructured beholder picks up the slack.
//!
//! `matches` is intentionally narrow: this beholder only attaches when an
//! image opts in via `LABEL yah.beholder=vanilla` or when the registry
//! decides to try it speculatively. The first line of output decides whether
//! it stays attached (out of scope for F2 — F3 adds decline rules).

use observation::{Event, EventSource, Level};
use serde_json::Value;
use workload_spec::{ImageRef, MeshIdent};

use super::{
    BeholderCtx, LogLine, ServiceBeholder, ServiceBeholderFactory, ServiceHints,
};

const NAME: &str = "vanilla";
const VERSION: &str = "1";

pub struct VanillaBeholder {
    target: String,
}

impl VanillaBeholder {
    pub fn for_ident(ident: &MeshIdent) -> Self {
        Self { target: ident.0.clone() }
    }

    fn parse_bracket_level(line: &str) -> Option<(Level, &str)> {
        let trimmed = line.trim_start();
        let rest = trimmed.strip_prefix('[')?;
        let close = rest.find(']')?;
        let level_str = rest[..close].trim().to_lowercase();
        let level = match level_str.as_str() {
            "trace" => Level::Trace,
            "debug" => Level::Debug,
            "info" | "notice" => Level::Info,
            "warn" | "warning" => Level::Warn,
            "error" | "err" => Level::Error,
            "fatal" | "crit" | "alert" | "emerg" => Level::Fatal,
            _ => return None,
        };
        let msg = rest[close + 1..].trim_start();
        Some((level, msg))
    }
}

impl ServiceBeholder for VanillaBeholder {
    fn name(&self) -> &'static str {
        NAME
    }
    fn version(&self) -> &'static str {
        VERSION
    }
    fn matches(&self, _ident: &MeshIdent, _image: &ImageRef, hints: &ServiceHints) -> bool {
        // Conservative default: only attach on explicit opt-in via image label.
        // F3 will broaden this with content sniffing.
        hints.forced_beholder() == Some(NAME)
    }
    fn parse_line(&mut self, line: &LogLine, ctx: &mut BeholderCtx) -> Vec<Event> {
        if line.line.is_empty() {
            return Vec::new();
        }
        let (level, msg) = match Self::parse_bracket_level(&line.line) {
            Some(parsed) => parsed,
            None => return Vec::new(),
        };
        let event = ctx.make_event(
            line,
            level,
            self.target.clone(),
            msg.to_string(),
            Value::Object(Default::default()),
            EventSource::Beholder { name: NAME.to_string(), version: VERSION.to_string() },
        );
        vec![event]
    }
}

pub struct VanillaFactory;

impl ServiceBeholderFactory for VanillaFactory {
    fn name(&self) -> &'static str {
        NAME
    }
    fn version(&self) -> &'static str {
        VERSION
    }
    fn matches(&self, _ident: &MeshIdent, _image: &ImageRef, hints: &ServiceHints) -> bool {
        hints.forced_beholder() == Some(NAME)
    }
    fn create(&self) -> Box<dyn ServiceBeholder> {
        Box::new(VanillaBeholder { target: String::new() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> BeholderCtx {
        BeholderCtx::new()
    }

    #[test]
    fn vanilla_parses_bracket_levels() {
        let mut b = VanillaBeholder::for_ident(&MeshIdent("api.pdx".to_string()));
        let mut c = ctx();

        for (raw, expected) in [
            ("[INFO] hello", Level::Info),
            ("[WARN] something", Level::Warn),
            ("[ERROR] boom", Level::Error),
            ("[debug] details", Level::Debug),
            ("[trace] noisy", Level::Trace),
            ("[fatal] dead", Level::Fatal),
        ] {
            let line = LogLine { line: raw.to_string(), offset_ms: 0 };
            let events = b.parse_line(&line, &mut c);
            assert_eq!(events.len(), 1, "{raw}");
            assert_eq!(events[0].level, expected, "{raw}");
        }
    }

    #[test]
    fn vanilla_declines_unrecognised_lines() {
        let mut b = VanillaBeholder::for_ident(&MeshIdent("api.pdx".to_string()));
        let mut c = ctx();
        for raw in [
            "no bracket here",
            "[unknown_level] msg",
            "[]] bad bracket",
            "",
        ] {
            let line = LogLine { line: raw.to_string(), offset_ms: 0 };
            let events = b.parse_line(&line, &mut c);
            assert!(events.is_empty(), "{raw}");
        }
    }

    #[test]
    fn vanilla_strips_bracket_from_msg() {
        let mut b = VanillaBeholder::for_ident(&MeshIdent("api.pdx".to_string()));
        let mut c = ctx();
        let line = LogLine { line: "[INFO] connected to db".to_string(), offset_ms: 0 };
        let events = b.parse_line(&line, &mut c);
        assert_eq!(events[0].msg, "connected to db");
    }
}
