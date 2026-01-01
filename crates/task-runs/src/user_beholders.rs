//! @arch:layer(kg_store)
//! @arch:role(substrate)
//! @arch:see(.yah/docs/working/yah-task-runs.md)
//!
//! User-extensible drop-in beholder definitions.
//!
//! Users place `.toml` files in `~/.yah/beholders/`. Each file describes one
//! beholder: which command it matches, its mode (parser | rewriter), and —
//! for parser mode — a set of regex patterns that turn output lines into
//! structured events.
//!
//! ## File format
//!
//! ```toml
//! name    = "my-linter"
//! version = "1.0"
//!
//! # argv0 after wrapper stripping; string or array of strings
//! argv0 = "my-linter"
//!
//! # Decline when any of these flags appear in argv (optional)
//! decline_if_has = ["--version", "--help"]
//!
//! mode = "parser"         # "parser" | "rewriter"
//!
//! # Rewriter only: args to append to argv when not already present
//! add_args = ["--json"]
//!
//! # Parser: one or more line-matching patterns (tried in order; all matches fire)
//! [[patterns]]
//! regex  = '(.+):(\d+): (error|warning): (.+)'
//! level  = "$3"           # literal "error"/"warn"/etc., or "$N" capture group
//! target = "my-linter"   # optional; defaults to beholder name
//! msg    = "$4"           # optional; defaults to the full matched line
//! [patterns.fields]
//! "file.path" = "$1"
//! "file.line" = "$2"
//! ```
//!
//! ## Discovery
//!
//! [`load_user_beholders`] reads every `*.toml` file in the given directory.
//! Files that fail to parse or are semantically invalid are skipped; a warning
//! is written to stderr so users can debug their definitions without crashing
//! the daemon.

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use regex::Regex;
use serde::Deserialize;

use crate::beholders::{Beholder, BeholderFactory, BeholderMode};
use crate::types::{ChunkRef, Event, EventSource, Level, OutputChunk};

// ─── TOML config shapes ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct UserBeholderFile {
    name: String,
    version: String,
    #[serde(default)]
    argv0: Argv0Spec,
    #[serde(default)]
    decline_if_has: Vec<String>,
    mode: ModeStr,
    #[serde(default)]
    add_args: Vec<String>,
    #[serde(default)]
    patterns: Vec<PatternDef>,
}

/// argv0 match spec: a single command name or a list of aliases.
#[derive(Debug, Deserialize, Default)]
#[serde(untagged)]
enum Argv0Spec {
    #[default]
    None,
    One(String),
    Many(Vec<String>),
}

impl Argv0Spec {
    fn is_match(&self, argv0: &str) -> bool {
        match self {
            Argv0Spec::None => false,
            Argv0Spec::One(s) => s == argv0,
            Argv0Spec::Many(v) => v.iter().any(|s| s == argv0),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ModeStr {
    Parser,
    Rewriter,
}

#[derive(Debug, Deserialize)]
struct PatternDef {
    regex: String,
    level: String,
    #[serde(default)]
    target: String,
    /// Explicit message text. `"$N"` captures group N; literal otherwise.
    /// Defaults to the full matched line when absent.
    msg: Option<String>,
    #[serde(default)]
    fields: HashMap<String, String>,
}

// ─── Compiled representations ─────────────────────────────────────────────────

struct CompiledPattern {
    re: Regex,
    level: FieldSpec,
    target: String,
    msg: Option<FieldSpec>,
    fields: Vec<(String, FieldSpec)>,
}

/// A value that is either a literal string or a regex capture-group reference.
enum FieldSpec {
    Literal(String),
    /// Index into regex captures (1-based, matching `$N` syntax).
    Capture(usize),
}

fn parse_field_spec(s: &str) -> FieldSpec {
    if let Some(rest) = s.strip_prefix('$') {
        if let Ok(n) = rest.parse::<usize>() {
            return FieldSpec::Capture(n);
        }
    }
    FieldSpec::Literal(s.to_owned())
}

fn resolve_field(spec: &FieldSpec, caps: &regex::Captures) -> Option<String> {
    match spec {
        FieldSpec::Literal(s) => Some(s.clone()),
        FieldSpec::Capture(n) => caps.get(*n).map(|m| m.as_str().to_owned()),
    }
}

fn parse_level_str(s: &str) -> Level {
    match s.to_ascii_lowercase().as_str() {
        "error" | "err" | "fatal" => Level::Error,
        "warn" | "warning" => Level::Warn,
        "info" | "information" | "notice" => Level::Info,
        "debug" | "verbose" => Level::Debug,
        "trace" => Level::Trace,
        _ => Level::Info,
    }
}

impl CompiledPattern {
    fn try_compile(def: &PatternDef, beholder_name: &str) -> Result<Self, String> {
        let re = Regex::new(&def.regex)
            .map_err(|e| format!("invalid regex {:?}: {e}", def.regex))?;
        let level = parse_field_spec(&def.level);
        let target = if def.target.is_empty() {
            beholder_name.to_owned()
        } else {
            def.target.clone()
        };
        let msg = def.msg.as_deref().map(parse_field_spec);
        let fields = def.fields.iter()
            .map(|(k, v)| (k.clone(), parse_field_spec(v)))
            .collect();
        Ok(Self { re, level, target, msg, fields })
    }

    fn apply(&self, line: &str, chunk: &OutputChunk, source: &EventSource) -> Option<Event> {
        let caps = self.re.captures(line)?;

        let level = match &self.level {
            FieldSpec::Literal(s) => Level::from_str(s).unwrap_or_else(|_| parse_level_str(s)),
            FieldSpec::Capture(n) => {
                let s = caps.get(*n)?.as_str();
                parse_level_str(s)
            }
        };

        let msg = match &self.msg {
            Some(spec) => resolve_field(spec, &caps).unwrap_or_else(|| line.to_owned()),
            None => line.to_owned(),
        };

        let mut fields_map = serde_json::Map::new();
        for (key, spec) in &self.fields {
            if let Some(val) = resolve_field(spec, &caps) {
                insert_nested(&mut fields_map, key, val);
            }
        }

        Some(Event {
            run_id: chunk.run_id.clone(),
            seq: 0,
            offset_ms: chunk.offset_ms,
            level,
            target: self.target.clone(),
            msg,
            fields: serde_json::Value::Object(fields_map),
            anchor: Some(ChunkRef { seq: chunk.seq }),
            source: source.clone(),
        })
    }
}

/// Insert `val` at a dot-delimited `key` path into `map`.
///
/// `"file.path"` becomes `{"file": {"path": val}}`.
/// Existing intermediate objects are merged; non-object intermediates are
/// replaced.
fn insert_nested(map: &mut serde_json::Map<String, serde_json::Value>, key: &str, val: String) {
    match key.split_once('.') {
        None => {
            map.insert(key.to_owned(), serde_json::Value::String(val));
        }
        Some((head, tail)) => {
            let inner = map
                .entry(head.to_owned())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
            if let serde_json::Value::Object(ref mut m) = inner {
                insert_nested(m, tail, val);
            } else {
                // Replace non-object with a fresh object.
                let mut m = serde_json::Map::new();
                insert_nested(&mut m, tail, val);
                *inner = serde_json::Value::Object(m);
            }
        }
    }
}

// ─── UserBeholderFactory ──────────────────────────────────────────────────────

/// Dynamic beholder factory loaded from a user TOML drop-in.
pub struct UserBeholderFactory {
    name: &'static str,
    version: &'static str,
    argv0: Argv0Spec,
    decline_if_has: Vec<String>,
    mode: BeholderMode,
    patterns: Arc<Vec<CompiledPattern>>,
}

fn leak_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

impl BeholderFactory for UserBeholderFactory {
    fn name(&self) -> &'static str { self.name }
    fn version(&self) -> &'static str { self.version }

    fn matches(&self, resolved_argv: &[String]) -> bool {
        let argv0 = match resolved_argv.first() {
            Some(s) => s.as_str(),
            None => return false,
        };
        if !self.argv0.is_match(argv0) {
            return false;
        }
        // Decline if any of the user-listed flags appear in argv.
        if self.decline_if_has.iter().any(|f| resolved_argv.contains(f)) {
            return false;
        }
        true
    }

    fn mode(&self) -> BeholderMode { self.mode.clone() }

    fn create(&self) -> Box<dyn Beholder> {
        Box::new(UserBeholder {
            name: self.name,
            version: self.version,
            patterns: Arc::clone(&self.patterns),
            buf: Vec::new(),
        })
    }
}

// ─── UserBeholder ─────────────────────────────────────────────────────────────

/// Per-run instance for a user-defined beholder. Buffers output and applies
/// all compiled regex patterns to each complete line.
struct UserBeholder {
    name: &'static str,
    version: &'static str,
    patterns: Arc<Vec<CompiledPattern>>,
    buf: Vec<u8>,
}

impl Beholder for UserBeholder {
    fn name(&self) -> &'static str { self.name }
    fn version(&self) -> &'static str { self.version }
    fn mode(&self) -> BeholderMode { BeholderMode::Parser }

    fn parse_chunk(&mut self, chunk: &OutputChunk) -> Vec<Event> {
        self.buf.extend_from_slice(&chunk.bytes);
        let mut events = Vec::new();
        let source = EventSource::Beholder {
            name: self.name.to_owned(),
            version: self.version.to_owned(),
        };

        while let Some(nl) = self.buf.iter().position(|&b| b == b'\n') {
            let raw: Vec<u8> = self.buf.drain(..=nl).collect();
            let line = match std::str::from_utf8(&raw) {
                Ok(s) => s.trim_end(),
                Err(_) => continue,
            };
            if line.is_empty() { continue }

            for pat in self.patterns.as_ref() {
                if let Some(ev) = pat.apply(line, chunk, &source) {
                    events.push(ev);
                }
            }
        }

        events
    }
}

// ─── Loader ───────────────────────────────────────────────────────────────────

/// Load all user-defined beholder factories from `*.toml` files in `dir`.
///
/// Missing or unreadable directories return an empty list. Files that fail
/// to parse or compile are skipped with a warning on stderr.
pub fn load_user_beholders(dir: &Path) -> Vec<Box<dyn BeholderFactory>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };

    let mut factories: Vec<Box<dyn BeholderFactory>> = Vec::new();

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("toml") {
            continue;
        }

        let src = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[yah beholders] could not read {}: {e}", path.display());
                continue;
            }
        };

        let def: UserBeholderFile = match toml::from_str(&src) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("[yah beholders] parse error in {}: {e}", path.display());
                continue;
            }
        };

        match compile_factory(def, &path) {
            Ok(f) => factories.push(f),
            Err(e) => {
                eprintln!("[yah beholders] invalid definition in {}: {e}", path.display());
            }
        }
    }

    factories
}

fn compile_factory(
    def: UserBeholderFile,
    path: &Path,
) -> Result<Box<dyn BeholderFactory>, String> {
    if def.name.is_empty() {
        return Err("name must not be empty".into());
    }

    let mode = match def.mode {
        ModeStr::Parser => BeholderMode::Parser,
        ModeStr::Rewriter => BeholderMode::DynamicRewriter { add_args: def.add_args },
    };

    let mut compiled_patterns = Vec::new();
    for (i, pat_def) in def.patterns.iter().enumerate() {
        let cp = CompiledPattern::try_compile(pat_def, &def.name)
            .map_err(|e| format!("patterns[{i}]: {e}"))?;
        compiled_patterns.push(cp);
    }

    if matches!(def.mode, ModeStr::Parser) && compiled_patterns.is_empty() {
        eprintln!(
            "[yah beholders] warning: parser beholder {:?} in {} has no patterns — \
             it will attach but emit no events",
            def.name,
            path.display()
        );
    }

    Ok(Box::new(UserBeholderFactory {
        name: leak_str(def.name),
        version: leak_str(def.version),
        argv0: def.argv0,
        decline_if_has: def.decline_if_has,
        mode,
        patterns: Arc::new(compiled_patterns),
    }))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::beholders::BeholderSelect;
    use crate::types::{Initiator, RunStatus, Stream, TaskRunId};

    fn make_chunk(run_id: &TaskRunId, bytes: &[u8]) -> OutputChunk {
        OutputChunk {
            run_id: run_id.clone(),
            seq: 0,
            offset_ms: 0,
            stream: Stream::Stdout,
            bytes: bytes.to_vec(),
        }
    }

    fn toml_factory(src: &str) -> Result<Box<dyn BeholderFactory>, String> {
        let def: UserBeholderFile = toml::from_str(src)
            .map_err(|e| e.to_string())?;
        compile_factory(def, std::path::Path::new("<test>"))
    }

    #[test]
    fn parser_beholder_matches_argv0() {
        let f = toml_factory(r#"
            name = "mytool"
            version = "1.0"
            argv0 = "mytool"
            mode = "parser"
        "#).unwrap();

        assert!(f.matches(&["mytool".to_owned(), "--check".to_owned()]));
        assert!(!f.matches(&["cargo".to_owned()]));
        assert!(matches!(f.mode(), BeholderMode::Parser));
    }

    #[test]
    fn parser_beholder_argv0_list() {
        let f = toml_factory(r#"
            name = "mytool"
            version = "1.0"
            argv0 = ["mytool", "mt"]
            mode = "parser"
        "#).unwrap();

        assert!(f.matches(&["mt".to_owned()]));
        assert!(f.matches(&["mytool".to_owned()]));
        assert!(!f.matches(&["other".to_owned()]));
    }

    #[test]
    fn decline_if_has_flag() {
        let f = toml_factory(r#"
            name = "mytool"
            version = "1.0"
            argv0 = "mytool"
            decline_if_has = ["--version", "--help"]
            mode = "parser"
        "#).unwrap();

        assert!(f.matches(&["mytool".to_owned(), "--check".to_owned()]));
        assert!(!f.matches(&["mytool".to_owned(), "--version".to_owned()]));
        assert!(!f.matches(&["mytool".to_owned(), "--help".to_owned()]));
    }

    #[test]
    fn rewriter_mode_add_args() {
        let f = toml_factory(r#"
            name = "mytool"
            version = "1.0"
            argv0 = "mytool"
            mode = "rewriter"
            add_args = ["--json"]
        "#).unwrap();

        assert!(matches!(f.mode(), BeholderMode::DynamicRewriter { .. }));
        if let BeholderMode::DynamicRewriter { add_args } = f.mode() {
            assert_eq!(add_args, vec!["--json"]);
        }
    }

    #[test]
    fn parser_extracts_events_from_chunk() {
        let f = toml_factory(r#"
            name = "mytool"
            version = "1.0"
            argv0 = "mytool"
            mode = "parser"

            [[patterns]]
            regex = '^(.+):(\d+): (error|warning): (.+)$'
            level = "$3"
            msg = "$4"
            [patterns.fields]
            "file.path" = "$1"
            "file.line" = "$2"
        "#).unwrap();

        let run_id = TaskRunId::new();
        let mut beholder = f.create();
        let line = b"src/main.rs:42: error: type mismatch\n";
        let chunk = make_chunk(&run_id, line);
        let events = beholder.parse_chunk(&chunk);

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert!(matches!(ev.level, Level::Error));
        assert_eq!(ev.msg, "type mismatch");
        assert_eq!(ev.fields["file"]["path"], "src/main.rs");
        assert_eq!(ev.fields["file"]["line"], "42");
    }

    #[test]
    fn parser_defaults_msg_to_full_line() {
        let f = toml_factory(r#"
            name = "mytool"
            version = "1.0"
            argv0 = "mytool"
            mode = "parser"

            [[patterns]]
            regex = 'ERROR'
            level = "error"
        "#).unwrap();

        let run_id = TaskRunId::new();
        let mut beholder = f.create();
        let chunk = make_chunk(&run_id, b"ERROR: something went wrong\n");
        let events = beholder.parse_chunk(&chunk);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].msg, "ERROR: something went wrong");
    }

    #[test]
    fn parser_all_patterns_fire_on_same_line() {
        let f = toml_factory(r#"
            name = "mytool"
            version = "1.0"
            argv0 = "mytool"
            mode = "parser"

            [[patterns]]
            regex = 'error'
            level = "error"

            [[patterns]]
            regex = 'warning'
            level = "warn"
        "#).unwrap();

        let run_id = TaskRunId::new();
        let mut beholder = f.create();
        // Line matches neither — one event only from the first pattern? No, "error warning" matches both.
        let chunk = make_chunk(&run_id, b"error warning foo\n");
        let events = beholder.parse_chunk(&chunk);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn insert_nested_dot_path() {
        let mut map = serde_json::Map::new();
        insert_nested(&mut map, "file.path", "src/main.rs".to_owned());
        insert_nested(&mut map, "file.line", "42".to_owned());
        insert_nested(&mut map, "error.code", "E001".to_owned());
        insert_nested(&mut map, "top", "value".to_owned());

        assert_eq!(map["file"]["path"], "src/main.rs");
        assert_eq!(map["file"]["line"], "42");
        assert_eq!(map["error"]["code"], "E001");
        assert_eq!(map["top"], "value");
    }

    #[test]
    fn load_from_dir_skips_non_toml() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("not-toml.txt"), "irrelevant").unwrap();
        std::fs::write(dir.path().join("beholder.toml"), r#"
            name = "loaded"
            version = "1.0"
            argv0 = "loaded"
            mode = "parser"
        "#).unwrap();

        let factories = load_user_beholders(dir.path());
        assert_eq!(factories.len(), 1);
        assert_eq!(factories[0].name(), "loaded");
    }

    #[test]
    fn load_from_missing_dir_returns_empty() {
        let factories = load_user_beholders(Path::new("/nonexistent/path/to/beholders"));
        assert!(factories.is_empty());
    }

    #[test]
    fn load_skips_malformed_toml() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("bad.toml"), "not valid toml [[[").unwrap();
        std::fs::write(dir.path().join("good.toml"), r#"
            name = "good"
            version = "1.0"
            argv0 = "good"
            mode = "parser"
        "#).unwrap();

        let factories = load_user_beholders(dir.path());
        assert_eq!(factories.len(), 1);
        assert_eq!(factories[0].name(), "good");
    }
}
