//! Beholder framework — observer-side parsers and rewriters for structured output.
//!
//! A *beholder* attaches to a `TaskRun` at spawn time and turns raw output
//! chunks into structured `Event` rows. There are two modes:
//!
//! - **Rewriter:** mutates `argv` before the subprocess starts (e.g. cargo
//!   gets `--message-format=json-render-diagnostics`).
//! - **Parser:** reads human-formatted output and scrapes events out of it.
//!
//! # Integration point
//!
//! ```ignore
//! let result = registry.attach(raw_cmd, &BeholderSelect::Auto, tty_attached);
//! // spawn the process with result.argv (may be rewritten)
//! // for each chunk captured:
//! if let Some(b) = result.beholder.as_mut() {
//!     for event in b.parse_chunk(&chunk) {
//!         store.append_event(run_id, ...);
//!     }
//! }
//! // persist result.status on the run:
//! store.update_beholder_status(run_id, &result.status)?;
//! ```

use crate::types::{BeholderStatus, ChunkRef, Event, EventSource, Level, OutputChunk};

// ─── ToolVersionRange ─────────────────────────────────────────────────────────

/// Declared support range for the *tool's* structured-output schema (not the
/// beholder's own version). Used to surface `unknown_format` when drift is
/// detected at parse time.
///
/// Strings are the underlying tool's version: e.g. `"1.38.0"` for cargo
/// (when `--message-format=json-render-diagnostics` was stabilised).
/// `None` means "no known lower/upper bound".
#[derive(Debug, Clone)]
pub struct ToolVersionRange {
    pub min: Option<&'static str>,
    pub max: Option<&'static str>,
}

// ─── BeholderMode ─────────────────────────────────────────────────────────────

/// Whether a beholder modifies the invocation (rewriter) or reads human output
/// (parser).
#[derive(Clone)]
pub enum BeholderMode {
    /// Rewrites argv before spawn to enable structured output.
    Rewriter {
        /// Applied to argv in-place when the beholder attaches.
        adjust_argv: fn(&mut Vec<String>),
    },
    /// Rewrites argv by appending a fixed set of args. Used by user-defined
    /// drop-in beholders whose `add_args` are loaded from TOML at runtime.
    DynamicRewriter {
        /// Args to append when not already present in argv.
        add_args: Vec<String>,
    },
    /// Reads the tool's human-formatted output without rewriting argv.
    Parser,
}

// ─── BeholderSelect ───────────────────────────────────────────────────────────

/// Caller-supplied attachment policy for `task.run`.
#[derive(Debug, Clone, Default)]
pub enum BeholderSelect {
    /// Walk the registry in priority order; first matching beholder wins.
    #[default]
    Auto,
    /// Pin to a specific beholder by name; bypass `matches` check.
    ///
    /// The beholder runs even if it would normally decline (e.g. explicit
    /// `--message-format=human` for cargo). Recorded as `forced-against-flags`
    /// on `beholder_status` in that case.
    Force(String),
    /// Bytes-only, regardless of registry contents.
    None,
}

// ─── AttachResult ─────────────────────────────────────────────────────────────

/// Returned by [`BeholderRegistry::attach`].
pub struct AttachResult {
    /// Attached per-run beholder instance, if any.
    pub beholder: Option<Box<dyn Beholder>>,
    /// Status string to store on `TaskRunMeta.beholder_status`.
    pub status: BeholderStatus,
    /// Argv to pass to the subprocess launcher.
    ///
    /// When a `Rewriter` beholder attaches, this reflects the adjusted argv.
    /// Otherwise identical to the input tokens from `raw_cmd`.
    pub argv: Vec<String>,
}

// ─── BeholderFactory ──────────────────────────────────────────────────────────

/// Static descriptor + factory for a beholder type. Held in the registry.
///
/// `create()` produces a fresh, independent per-run instance each time.
/// Implementors should be stateless — all per-run state belongs in the
/// instance returned by `create`.
pub trait BeholderFactory: Send + Sync {
    fn name(&self) -> &'static str;
    fn version(&self) -> &'static str;
    /// Return `true` if this beholder handles the given invocation.
    ///
    /// Receives the *resolved* argv (wrappers stripped). May inspect argv to
    /// detect user-set conflicting flags (e.g. `--message-format=human`) and
    /// return `false` to decline gracefully.
    fn matches(&self, resolved_argv: &[String]) -> bool;
    fn mode(&self) -> BeholderMode;
    /// Create a fresh per-run instance.
    fn create(&self) -> Box<dyn Beholder>;
    /// Declared support range for the underlying tool's structured output format.
    ///
    /// Informational: surfaced in `beholder_status` when format drift is detected
    /// so agents can see which version window the beholder was built for.
    /// `None` means the beholder makes no version claim.
    fn tool_version_range(&self) -> Option<ToolVersionRange> { None }
}

// ─── Beholder ─────────────────────────────────────────────────────────────────

/// Per-run stateful chunk processor.
///
/// The instance is private to a single `TaskRun`. `parse_chunk` is called for
/// every captured chunk in arrival order and may produce zero or more structured
/// events. Implementations must be incremental — buffering everything to EOF
/// defeats the lossless-during-execution property.
///
/// For tools that dump a single JSON document at exit (e.g. ESLint, Biome),
/// implement `parse_chunk` as a simple buffer accumulator and do all parsing
/// in `on_done`, which the driver calls once the PTY reader exits.
pub trait Beholder: Send {
    fn name(&self) -> &'static str;
    fn version(&self) -> &'static str;
    fn mode(&self) -> BeholderMode;
    /// Extract structured events from one output chunk.
    fn parse_chunk(&mut self, chunk: &OutputChunk) -> Vec<Event>;
    /// Called once after the last chunk has been delivered (PTY EOF).
    ///
    /// Use this for tools that write a single JSON document at exit rather than
    /// streaming line-by-line. The driver passes the run id and a final
    /// timestamp so the returned events can be fully populated.
    ///
    /// Default: returns no events (correct for streaming beholders).
    fn on_done(&mut self, _run_id: &crate::types::TaskRunId, _offset_ms: u32) -> Vec<Event> {
        Vec::new()
    }
    /// Returns a reason string if the beholder detected that the tool's output
    /// format is unrecognized (schema drift). When `Some`, the driver detaches
    /// this beholder and updates `beholder_status` to `unknown_format:name`.
    ///
    /// Default: `None` — format OK or not yet probed.
    fn unknown_format_reason(&self) -> Option<&str> { None }
}

// ─── BeholderRegistry ────────────────────────────────────────────────────────

/// Registry of available beholders, consulted at `task.run` time.
///
/// Entries are tried in insertion (priority) order. Earlier registrations have
/// higher priority when multiple beholders' `matches` predicates would fire.
pub struct BeholderRegistry {
    entries: Vec<Box<dyn BeholderFactory>>,
}

impl BeholderRegistry {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Register a beholder factory. Earlier entries have higher priority.
    pub fn register(&mut self, factory: Box<dyn BeholderFactory>) {
        self.entries.push(factory);
    }

    /// Determine and attach a beholder for the given invocation.
    ///
    /// - Tokenizes `raw_cmd` and strips wrapper binaries (bunx, npx, pnpm, npm exec).
    /// - `tty_attached` signals that a human-facing terminal tile is watching:
    ///   `Rewriter` beholders decline in `Auto` mode to avoid clobbering human
    ///   output. `Force` always attaches but records `forced-against-tty`.
    /// - Behaviour depends on `select`:
    ///   - `Auto` — walk in priority order, first `matches` hit wins.
    ///   - `Force(name)` — find by name; bypass `matches` (records
    ///     `forced-against-flags` if the beholder would have declined, or
    ///     `forced-against-tty` if a TTY is attached and mode is Rewriter).
    ///   - `None` — bytes-only; no beholder attached.
    /// - If a `Rewriter` beholder attaches, its `adjust_argv` is applied to the
    ///   returned `AttachResult.argv` and the diff is surfaced on `status`.
    pub fn attach(&self, raw_cmd: &str, select: &BeholderSelect, tty_attached: bool) -> AttachResult {
        let mut argv = resolve_argv(raw_cmd);

        match select {
            BeholderSelect::None => AttachResult {
                beholder: None,
                status: BeholderStatus::none_explicit(),
                argv,
            },

            BeholderSelect::Force(name) => {
                match self.entries.iter().find(|e| e.name() == name.as_str()) {
                    None => AttachResult {
                        beholder: None,
                        status: BeholderStatus::none_auto(),
                        argv,
                    },
                    Some(factory) => {
                        let would_decline = !factory.matches(&argv);
                        let is_rewriter = matches!(factory.mode(), BeholderMode::Rewriter { .. } | BeholderMode::DynamicRewriter { .. });
                        let base_status = if would_decline {
                            BeholderStatus::forced_against_flags(factory.name(), factory.version())
                        } else if tty_attached && is_rewriter {
                            BeholderStatus::forced_against_tty(factory.name(), factory.version())
                        } else {
                            BeholderStatus::forced(factory.name(), factory.version())
                        };
                        let added = rewrite_if_rewriter(factory.mode(), &mut argv);
                        let status = base_status.with_rewrite(added);
                        AttachResult { beholder: Some(factory.create()), status, argv }
                    }
                }
            }

            BeholderSelect::Auto => {
                for factory in &self.entries {
                    if factory.matches(&argv) {
                        let is_rewriter = matches!(factory.mode(), BeholderMode::Rewriter { .. } | BeholderMode::DynamicRewriter { .. });
                        // TTY-attached: Rewriter beholders decline to preserve human output.
                        if tty_attached && is_rewriter {
                            return AttachResult {
                                beholder: None,
                                status: BeholderStatus::declined(factory.name(), "tty-attached"),
                                argv,
                            };
                        }
                        let added = rewrite_if_rewriter(factory.mode(), &mut argv);
                        let status = BeholderStatus::attached(factory.name(), factory.version())
                            .with_rewrite(added);
                        return AttachResult {
                            beholder: Some(factory.create()),
                            status,
                            argv,
                        };
                    }
                }
                AttachResult {
                    beholder: None,
                    status: BeholderStatus::none_auto(),
                    argv,
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for BeholderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── helpers ─────────────────────────────────────────────────────────────────

/// Apply argv rewriting if `mode` is `Rewriter` or `DynamicRewriter`, and
/// return the args that were added. Returns an empty `Vec` for `Parser` mode
/// or when the rewriter added nothing.
fn rewrite_if_rewriter(mode: BeholderMode, argv: &mut Vec<String>) -> Vec<String> {
    match mode {
        BeholderMode::Rewriter { adjust_argv } => {
            let before = argv.clone();
            adjust_argv(argv);
            argv.iter().filter(|a| !before.contains(a)).cloned().collect()
        }
        BeholderMode::DynamicRewriter { add_args } => {
            let mut added = Vec::new();
            for arg in add_args {
                if !argv.contains(&arg) {
                    argv.push(arg.clone());
                    added.push(arg);
                }
            }
            added
        }
        BeholderMode::Parser => Vec::new(),
    }
}

// ─── argv resolution ─────────────────────────────────────────────────────────

/// Tokenize `raw_cmd` and strip common wrapper binaries so beholders see the
/// bare tool name as `argv[0]`.
///
/// Wrappers stripped:
/// - `bunx` / `npx` — drop the wrapper token.
/// - `pnpm` / `npm exec` — drop the wrapper and optional `exec` subcommand.
pub fn resolve_argv(raw_cmd: &str) -> Vec<String> {
    let mut argv: Vec<String> = tokenize(raw_cmd);
    loop {
        match argv.first().map(String::as_str) {
            Some("bunx") | Some("npx") => {
                argv.remove(0);
            }
            Some("pnpm") | Some("npm") => {
                argv.remove(0);
                if argv.first().map(String::as_str) == Some("exec") {
                    argv.remove(0);
                }
                break;
            }
            _ => break,
        }
    }
    argv
}

/// Split on ASCII whitespace. Sufficient for well-formed command strings;
/// does not handle shell quoting.
fn tokenize(s: &str) -> Vec<String> {
    s.split_ascii_whitespace().map(str::to_owned).collect()
}

// ─── Cargo beholder ───────────────────────────────────────────────────────────

/// Cargo subcommands that produce rustc diagnostic output.
const CARGO_DIAG_SUBCOMMANDS: &[&str] = &[
    "check", "build", "test", "clippy", "run", "fix", "doc", "bench", "publish",
];

/// Factory for the bundled cargo beholder (Tier 1.5, Rewriter mode).
///
/// Matches `cargo <sub>` invocations for diagnostic-producing subcommands and
/// rewrites argv to add `--message-format=json-render-diagnostics`, which
/// makes cargo emit one JSON object per line rather than human-formatted text.
///
/// Declines when the user already specified `--message-format` (respects
/// explicit intent; see R070-T5 for TTY-aware behaviour rules).
pub struct CargoBeholderFactory;

impl BeholderFactory for CargoBeholderFactory {
    fn name(&self) -> &'static str { "cargo" }
    fn version(&self) -> &'static str { "1.38" }

    fn matches(&self, argv: &[String]) -> bool {
        if argv.first().map(String::as_str) != Some("cargo") {
            return false;
        }
        let has_diag_sub = argv.iter().skip(1)
            .any(|a| CARGO_DIAG_SUBCOMMANDS.contains(&a.as_str()));
        if !has_diag_sub {
            return false;
        }
        // Decline when the user explicitly chose a message format.
        !argv.iter().any(|a| a.starts_with("--message-format"))
    }

    fn mode(&self) -> BeholderMode {
        BeholderMode::Rewriter {
            adjust_argv: |argv| {
                argv.push("--message-format=json-render-diagnostics".to_owned());
            },
        }
    }

    fn create(&self) -> Box<dyn Beholder> {
        Box::new(CargoBeholder::default())
    }

    fn tool_version_range(&self) -> Option<ToolVersionRange> {
        // --message-format=json-render-diagnostics was stabilised in cargo 1.38.0.
        // No upper bound — the format has been additive since stabilisation.
        Some(ToolVersionRange { min: Some("1.38.0"), max: None })
    }
}

/// After this many JSON-shaped lines fail to match the `CargoLine` schema
/// (missing `reason` field, etc.) without a single recognizable line, the
/// beholder declares `unknown_format` and stops emitting events.
const FORMAT_PROBE_LIMIT: u8 = 5;

/// Per-run cargo JSON parser.
///
/// Buffers incoming bytes and extracts complete newline-delimited JSON records
/// as they arrive. Non-JSON lines (blank lines, unexpected text) are silently
/// skipped so a stray progress line never stalls the stream.
///
/// If the first [`FORMAT_PROBE_LIMIT`] JSON-shaped lines all fail to match the
/// expected cargo schema (missing `reason` field), `unknown_format_reason`
/// returns a non-`None` value and `parse_chunk` stops emitting events.
pub struct CargoBeholder {
    buf: Vec<u8>,
    /// JSON-object lines seen that don't match the `CargoLine` schema.
    /// Only counted before a recognizable cargo line has been seen.
    json_lines_unrecognized: u8,
    /// Set once a line successfully parses as `CargoLine` (has `reason` field).
    /// After this point unrecognized lines are silently dropped (forward compat).
    recognized_line_seen: bool,
    /// Set when `json_lines_unrecognized >= FORMAT_PROBE_LIMIT` and no cargo
    /// line has been seen yet — indicates the output format is unrecognized.
    unknown_format: Option<String>,
}

impl Default for CargoBeholder {
    fn default() -> Self {
        Self {
            buf: Vec::new(),
            json_lines_unrecognized: 0,
            recognized_line_seen: false,
            unknown_format: None,
        }
    }
}

impl Beholder for CargoBeholder {
    fn name(&self) -> &'static str { "cargo" }
    fn version(&self) -> &'static str { "1.38" }
    fn mode(&self) -> BeholderMode {
        BeholderMode::Rewriter {
            adjust_argv: |argv| {
                argv.push("--message-format=json-render-diagnostics".to_owned());
            },
        }
    }

    fn parse_chunk(&mut self, chunk: &OutputChunk) -> Vec<Event> {
        if self.unknown_format.is_some() {
            return Vec::new();
        }
        self.buf.extend_from_slice(&chunk.bytes);
        let mut events = Vec::new();
        while let Some(nl) = self.buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.buf.drain(..=nl).collect();
            // Strip trailing \n (and optional \r for Windows cargo output).
            let line = line.trim_ascii_end();
            if line.is_empty() || line[0] != b'{' {
                continue;
            }
            let Ok(s) = std::str::from_utf8(line) else { continue };

            // Try to parse as a CargoLine (requires `reason: String`).
            match serde_json::from_str::<CargoLine>(s) {
                Ok(cargo_line) => {
                    self.recognized_line_seen = true;
                    if let Some(ev) = parse_cargo_line_inner(&cargo_line, chunk) {
                        events.push(ev);
                    }
                }
                Err(_) => {
                    // JSON object but doesn't fit CargoLine schema.
                    // Only counts toward the probe threshold before we've seen
                    // a recognized line; after that it's just forward compat.
                    if !self.recognized_line_seen {
                        self.json_lines_unrecognized =
                            self.json_lines_unrecognized.saturating_add(1);
                        if self.json_lines_unrecognized >= FORMAT_PROBE_LIMIT {
                            self.unknown_format = Some(format!(
                                "no recognizable cargo JSON lines in first {} JSON-object lines",
                                FORMAT_PROBE_LIMIT
                            ));
                            return Vec::new();
                        }
                    }
                }
            }
        }
        events
    }

    fn unknown_format_reason(&self) -> Option<&str> {
        self.unknown_format.as_deref()
    }
}

/// Construct a [`BeholderRegistry`] pre-loaded with all bundled beholders.
pub fn default_registry() -> BeholderRegistry {
    let mut r = BeholderRegistry::new();
    r.register(Box::new(CargoBeholderFactory));
    r.register(Box::new(TscBeholderFactory));
    r.register(Box::new(EslintBeholderFactory));
    r.register(Box::new(BiomeBeholderFactory));
    r.register(Box::new(VitestBeholderFactory));
    r.register(Box::new(JestBeholderFactory));
    r.register(Box::new(BunTestBeholderFactory));
    r.register(Box::new(PytestBeholderFactory));
    r.register(Box::new(ViteBuildBeholderFactory));
    r
}

/// Construct a [`BeholderRegistry`] with bundled beholders plus any user-defined
/// beholders loaded from `user_dir` (typically `~/.yah/beholders/`).
///
/// User beholders are appended after bundled ones, so bundled beholders have
/// higher priority when both would match the same command. Malformed or
/// unreadable TOML files in `user_dir` are skipped silently.
pub fn registry_with_user_beholders(user_dir: Option<&std::path::Path>) -> BeholderRegistry {
    let mut r = default_registry();
    if let Some(dir) = user_dir {
        for factory in crate::user_beholders::load_user_beholders(dir) {
            r.register(factory);
        }
    }
    r
}

// ─── tsc beholder ─────────────────────────────────────────────────────────────

/// Factory for the bundled tsc beholder (Tier 1.5, Rewriter + Parser).
///
/// Matches `tsc` invocations (bare or via wrapper: `bunx tsc`, `pnpm tsc`,
/// `npx tsc`) and rewrites argv to add `--pretty=false` when not already set,
/// enabling the machine-parseable diagnostic format:
///
/// ```text
/// src/foo.ts(10,5): error TS2345: Argument of type 'string' is not assignable…
/// Found 1 error.
/// ```
///
/// Declines when:
/// - `--pretty` or `--pretty=true` is already set (user wants colored output).
/// - `--version` / `-v` / `--init` are present (non-diagnostic invocations).
/// - `--pretty=false` is already present (rewriter is a no-op; beholder still
///   attaches in Parser mode so diagnostics are captured — argv unchanged, no
///   `rewrite_added` entry on status).
pub struct TscBeholderFactory;

impl BeholderFactory for TscBeholderFactory {
    fn name(&self) -> &'static str { "tsc" }
    fn version(&self) -> &'static str { "3.0" }

    fn matches(&self, argv: &[String]) -> bool {
        if argv.first().map(String::as_str) != Some("tsc") {
            return false;
        }
        // Decline for non-diagnostic invocations.
        if argv.iter().any(|a| matches!(a.as_str(), "--version" | "-v" | "--init")) {
            return false;
        }
        // Decline if the user explicitly requested colored (pretty) output.
        // `--pretty` alone means true; `--pretty=true` is explicit. We do NOT
        // decline for `--pretty=false` — the rewriter becomes a no-op but we
        // still want to parse the output.
        !argv.iter().any(|a| a == "--pretty" || a == "--pretty=true")
    }

    fn mode(&self) -> BeholderMode {
        BeholderMode::Rewriter {
            adjust_argv: |argv| {
                // Only inject if the user hasn't already set --pretty=false.
                if !argv.iter().any(|a| a == "--pretty=false") {
                    argv.push("--pretty=false".to_owned());
                }
            },
        }
    }

    fn create(&self) -> Box<dyn Beholder> {
        Box::new(TscBeholder::default())
    }

    fn tool_version_range(&self) -> Option<ToolVersionRange> {
        // --pretty=false has been available since tsc 1.x; 3.0.0 is a
        // conservative lower bound for the stable output format.
        Some(ToolVersionRange { min: Some("3.0.0"), max: None })
    }
}

/// Per-run tsc parser.
///
/// Buffers incoming bytes and extracts complete lines. Diagnostic lines in the
/// `--pretty=false` format are turned into structured events; watch-mode
/// timestamp headers and blank lines are silently skipped. Summary lines
/// ("Found N errors.") produce an Info event so the agent can see the final
/// outcome without polling run status.
pub struct TscBeholder {
    buf: Vec<u8>,
}

impl Default for TscBeholder {
    fn default() -> Self {
        Self { buf: Vec::new() }
    }
}

impl Beholder for TscBeholder {
    fn name(&self) -> &'static str { "tsc" }
    fn version(&self) -> &'static str { "3.0" }
    fn mode(&self) -> BeholderMode {
        BeholderMode::Rewriter {
            adjust_argv: |argv| {
                if !argv.iter().any(|a| a == "--pretty=false") {
                    argv.push("--pretty=false".to_owned());
                }
            },
        }
    }

    fn parse_chunk(&mut self, chunk: &OutputChunk) -> Vec<Event> {
        self.buf.extend_from_slice(&chunk.bytes);
        let mut events = Vec::new();
        while let Some(nl) = self.buf.iter().position(|&b| b == b'\n') {
            let raw: Vec<u8> = self.buf.drain(..=nl).collect();
            let Ok(line) = std::str::from_utf8(raw.trim_ascii_end()) else { continue };
            if line.is_empty() { continue }
            // Skip watch-mode timestamp headers: "[12:00:00 AM] …"
            if line.starts_with('[') { continue }

            let source = EventSource::Beholder {
                name: "tsc".to_owned(),
                version: "3.0".to_owned(),
            };

            if let Some(diag) = parse_tsc_diagnostic(line) {
                let mut fields = serde_json::json!({});
                fields["error"] = serde_json::json!({ "code": diag.code });
                fields["file"] = serde_json::json!({
                    "path": diag.file,
                    "line": diag.line,
                    "col":  diag.col,
                });
                events.push(Event {
                    run_id: chunk.run_id.clone(),
                    seq: 0,
                    offset_ms: chunk.offset_ms,
                    level: diag.level,
                    target: "tsc".to_owned(),
                    msg: diag.msg,
                    fields,
                    anchor: Some(ChunkRef { seq: chunk.seq }),
                    source,
                });
            } else if let Some(summary) = parse_tsc_summary(line) {
                events.push(Event {
                    run_id: chunk.run_id.clone(),
                    seq: 0,
                    offset_ms: chunk.offset_ms,
                    level: if summary.errors == 0 { Level::Info } else { Level::Error },
                    target: "tsc".to_owned(),
                    msg: line.to_owned(),
                    fields: serde_json::json!({ "build": { "errors": summary.errors } }),
                    anchor: Some(ChunkRef { seq: chunk.seq }),
                    source,
                });
            }
        }
        events
    }
}

// ─── tsc parsing helpers ──────────────────────────────────────────────────────

struct TscDiagnostic {
    file: String,
    line: u32,
    col: u32,
    level: Level,
    code: String,
    msg: String,
}

struct TscSummary {
    errors: u32,
}

/// Parse a single `--pretty=false` diagnostic line of the form:
/// `<file>(<line>,<col>): <level> TS<code>: <message>`
fn parse_tsc_diagnostic(line: &str) -> Option<TscDiagnostic> {
    const MARKERS: &[(&str, Level)] = &[
        ("): error TS", Level::Error),
        ("): warning TS", Level::Warn),
        ("): message TS", Level::Info),
    ];

    for (marker, level) in MARKERS {
        let Some(marker_pos) = line.find(marker) else { continue };

        // Everything before ")" is "file(line,col".
        let file_pos_str = &line[..marker_pos];
        let Some((file, ln, col)) = parse_file_pos(file_pos_str) else { continue };

        // After "): error TS" (etc.) starts the numeric code.
        let after = &line[marker_pos + marker.len()..];
        let code_len = after.chars().take_while(|c| c.is_ascii_digit()).count();
        if code_len == 0 { continue; }
        let code = format!("TS{}", &after[..code_len]);

        let rest = &after[code_len..];
        if !rest.starts_with(": ") { continue; }
        let msg = rest[2..].to_owned();

        return Some(TscDiagnostic { file, line: ln, col, level: *level, code, msg });
    }
    None
}

/// Extract `(file, line, col)` from the `file(line,col` part of a tsc diagnostic.
///
/// `s` is the substring before the closing `)`, e.g. `"src/foo.ts(10,5"`.
fn parse_file_pos(s: &str) -> Option<(String, u32, u32)> {
    let comma = s.rfind(',')?;
    let col: u32 = s[comma + 1..].parse().ok()?;

    let before_comma = &s[..comma];
    let open_paren = before_comma.rfind('(')?;
    let ln: u32 = before_comma[open_paren + 1..].parse().ok()?;

    let file = s[..open_paren].to_owned();
    if file.is_empty() { return None; }

    Some((file, ln, col))
}

/// Parse `"Found N errors."` / `"Found N errors in M files."` summary lines.
fn parse_tsc_summary(line: &str) -> Option<TscSummary> {
    let rest = line.strip_prefix("Found ")?;
    // Extract the leading digit sequence (error count).
    let count_len = rest.chars().take_while(|c| c.is_ascii_digit()).count();
    if count_len == 0 { return None; }
    let errors: u32 = rest[..count_len].parse().ok()?;
    // Remainder must start with " error" to distinguish from other "Found …" lines.
    if !rest[count_len..].starts_with(" error") { return None; }
    Some(TscSummary { errors })
}

// ─── Cargo JSON parsing ───────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct CargoLine {
    reason: String,
    message: Option<CargoMessage>,
    success: Option<bool>,
}

#[derive(serde::Deserialize)]
struct CargoMessage {
    message: String,
    level: String,
    code: Option<CargoCode>,
    spans: Vec<CargoSpan>,
}

#[derive(serde::Deserialize)]
struct CargoCode {
    code: String,
}

#[derive(serde::Deserialize)]
struct CargoSpan {
    file_name: String,
    is_primary: bool,
    line_start: u32,
    column_start: u32,
}

/// Convert an already-parsed `CargoLine` into a structured `Event`, or `None`
/// for reasons we intentionally skip (e.g. `compiler-artifact`).
fn parse_cargo_line_inner(line: &CargoLine, chunk: &OutputChunk) -> Option<Event> {
    let source = EventSource::Beholder {
        name: "cargo".to_owned(),
        version: "1.38".to_owned(),
    };

    match line.reason.as_str() {
        "compiler-message" => {
            let msg = line.message.as_ref()?;
            let level = cargo_level(&msg.level);
            let primary = msg.spans.iter().find(|s| s.is_primary);

            let mut fields = serde_json::json!({});
            if let Some(code) = &msg.code {
                fields["error"] = serde_json::json!({ "code": code.code });
            }
            if let Some(span) = primary {
                fields["file"] = serde_json::json!({
                    "path": span.file_name,
                    "line": span.line_start,
                    "col":  span.column_start,
                });
            }

            Some(Event {
                run_id: chunk.run_id.clone(),
                seq: 0, // assigned by store on insert
                offset_ms: chunk.offset_ms,
                level,
                target: "cargo::rustc".to_owned(),
                msg: msg.message.clone(),
                fields,
                anchor: Some(ChunkRef { seq: chunk.seq }),
                source,
            })
        }
        "build-finished" => {
            let ok = line.success.unwrap_or(false);
            Some(Event {
                run_id: chunk.run_id.clone(),
                seq: 0,
                offset_ms: chunk.offset_ms,
                level: if ok { Level::Info } else { Level::Error },
                target: "cargo".to_owned(),
                msg: if ok { "build finished".to_owned() } else { "build failed".to_owned() },
                fields: serde_json::json!({ "build": { "success": ok } }),
                anchor: Some(ChunkRef { seq: chunk.seq }),
                source,
            })
        }
        _ => None,
    }
}

fn cargo_level(s: &str) -> Level {
    match s {
        "error" | "failure-note" => Level::Error,
        "warning" => Level::Warn,
        "note" | "help" => Level::Info,
        _ => Level::Debug,
    }
}

// ─── ESLint beholder ─────────────────────────────────────────────────────────

/// Factory for the bundled ESLint beholder (Tier 1.5, Rewriter mode).
///
/// Matches `eslint` invocations and rewrites argv to add `--format=json`, which
/// makes ESLint emit a single JSON array at exit rather than human-formatted
/// text. Parsing happens in `on_done` because ESLint writes the entire document
/// at process exit, not line-by-line.
///
/// Declines when:
/// - `--version`, `--env-info`, or `--print-config` are present (non-lint).
/// - `--format=<value>` is already set to something other than `json` (respects
///   explicit user intent; if it's already `json`, the rewriter is a no-op and
///   the beholder still attaches as a parser).
pub struct EslintBeholderFactory;

impl BeholderFactory for EslintBeholderFactory {
    fn name(&self) -> &'static str { "eslint" }
    fn version(&self) -> &'static str { "8.0" }

    fn matches(&self, argv: &[String]) -> bool {
        if argv.first().map(String::as_str) != Some("eslint") {
            return false;
        }
        // Non-lint invocations.
        if argv.iter().any(|a| matches!(a.as_str(), "--version" | "--env-info" | "--print-config")) {
            return false;
        }
        // Decline if the user explicitly chose a non-json formatter.
        // `--format=json` is fine — rewriter is a no-op; beholder still parses.
        // `-f json` / `-f compact` etc. are also handled.
        let format_arg = argv.windows(2)
            .find(|w| w[0] == "-f" || w[0] == "--format")
            .map(|w| w[1].as_str());
        let format_eq = argv.iter()
            .find(|a| a.starts_with("--format="))
            .map(|a| a.trim_start_matches("--format="));
        let explicit_format = format_arg.or(format_eq);
        matches!(explicit_format, None | Some("json"))
    }

    fn mode(&self) -> BeholderMode {
        BeholderMode::Rewriter {
            adjust_argv: |argv| {
                if !argv.iter().any(|a| a == "--format=json") {
                    argv.push("--format=json".to_owned());
                }
            },
        }
    }

    fn create(&self) -> Box<dyn Beholder> {
        Box::new(EslintBeholder::default())
    }

    fn tool_version_range(&self) -> Option<ToolVersionRange> {
        // --format=json has been available since ESLint 1.x; 8.0 is the
        // ESLint version range this beholder was built and tested against.
        Some(ToolVersionRange { min: Some("8.0.0"), max: None })
    }
}

/// Per-run ESLint JSON parser. Buffers all output; parses in `on_done`.
///
/// ESLint writes the entire JSON array to stdout at process exit. There is no
/// streaming line-by-line format in the `json` formatter, so incremental
/// parsing is not possible. The beholder accumulates raw bytes and flushes
/// structured events once the PTY closes.
pub struct EslintBeholder {
    buf: Vec<u8>,
    unknown_format: Option<String>,
}

impl Default for EslintBeholder {
    fn default() -> Self {
        Self { buf: Vec::new(), unknown_format: None }
    }
}

impl Beholder for EslintBeholder {
    fn name(&self) -> &'static str { "eslint" }
    fn version(&self) -> &'static str { "8.0" }
    fn mode(&self) -> BeholderMode {
        BeholderMode::Rewriter {
            adjust_argv: |argv| {
                if !argv.iter().any(|a| a == "--format=json") {
                    argv.push("--format=json".to_owned());
                }
            },
        }
    }

    fn parse_chunk(&mut self, chunk: &OutputChunk) -> Vec<Event> {
        self.buf.extend_from_slice(&chunk.bytes);
        Vec::new()
    }

    fn on_done(&mut self, run_id: &crate::types::TaskRunId, offset_ms: u32) -> Vec<Event> {
        if self.buf.is_empty() {
            return Vec::new();
        }
        let Ok(s) = std::str::from_utf8(&self.buf) else {
            self.unknown_format = Some("non-UTF-8 output".to_owned());
            return Vec::new();
        };
        // ESLint JSON output is an array; strip any leading/trailing ANSI escapes
        // or shell prompt noise that a PTY might inject before/after the document.
        let s = s.trim();
        match serde_json::from_str::<Vec<EslintFile>>(s) {
            Ok(files) => eslint_to_events(files, run_id, offset_ms),
            Err(e) => {
                self.unknown_format = Some(format!("failed to parse ESLint JSON: {e}"));
                Vec::new()
            }
        }
    }

    fn unknown_format_reason(&self) -> Option<&str> {
        self.unknown_format.as_deref()
    }
}

// ─── ESLint JSON structs ──────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct EslintFile {
    #[serde(rename = "filePath")]
    file_path: String,
    messages: Vec<EslintMessage>,
}

#[derive(serde::Deserialize)]
struct EslintMessage {
    #[serde(rename = "ruleId")]
    rule_id: Option<String>,
    severity: u8,
    message: String,
    line: Option<u32>,
    column: Option<u32>,
}

fn eslint_to_events(
    files: Vec<EslintFile>,
    run_id: &crate::types::TaskRunId,
    offset_ms: u32,
) -> Vec<Event> {
    let mut events = Vec::new();
    for file in files {
        for msg in file.messages {
            let level = match msg.severity {
                2 => Level::Error,
                1 => Level::Warn,
                _ => Level::Info,
            };
            let mut fields = serde_json::json!({});
            if let Some(ref rule) = msg.rule_id {
                fields["error"] = serde_json::json!({ "code": rule });
            }
            let mut file_fields = serde_json::json!({ "path": file.file_path });
            if let Some(l) = msg.line { file_fields["line"] = serde_json::json!(l); }
            if let Some(c) = msg.column { file_fields["col"] = serde_json::json!(c); }
            fields["file"] = file_fields;

            events.push(Event {
                run_id: run_id.clone(),
                seq: 0,
                offset_ms,
                level,
                target: "eslint".to_owned(),
                msg: msg.message,
                fields,
                anchor: None,
                source: EventSource::Beholder {
                    name: "eslint".to_owned(),
                    version: "8.0".to_owned(),
                },
            });
        }
    }
    events
}

// ─── Biome beholder ──────────────────────────────────────────────────────────

/// Subcommands that produce lint diagnostics.
const BIOME_LINT_SUBCOMMANDS: &[&str] = &["check", "lint", "ci"];

/// Factory for the bundled Biome beholder (Tier 1.5, Rewriter mode).
///
/// Matches `biome check/lint/ci` invocations and rewrites argv to add
/// `--reporter=json`, which makes Biome emit a single JSON object at exit.
/// Parsing happens in `on_done`.
///
/// Declines when:
/// - Subcommand is not one of `check`, `lint`, `ci`.
/// - `--reporter=<value>` is already set to something other than `json`.
/// - `--version` is present.
pub struct BiomeBeholderFactory;

impl BeholderFactory for BiomeBeholderFactory {
    fn name(&self) -> &'static str { "biome" }
    fn version(&self) -> &'static str { "1.0" }

    fn matches(&self, argv: &[String]) -> bool {
        if argv.first().map(String::as_str) != Some("biome") {
            return false;
        }
        if argv.iter().any(|a| a == "--version") {
            return false;
        }
        // Must have a lint-producing subcommand.
        let has_lint_sub = argv.iter().skip(1)
            .any(|a| BIOME_LINT_SUBCOMMANDS.contains(&a.as_str()));
        if !has_lint_sub {
            return false;
        }
        // Decline if the user explicitly chose a non-json reporter.
        let reporter = argv.iter()
            .find(|a| a.starts_with("--reporter="))
            .map(|a| a.trim_start_matches("--reporter="));
        matches!(reporter, None | Some("json"))
    }

    fn mode(&self) -> BeholderMode {
        BeholderMode::Rewriter {
            adjust_argv: |argv| {
                if !argv.iter().any(|a| a == "--reporter=json") {
                    argv.push("--reporter=json".to_owned());
                }
            },
        }
    }

    fn create(&self) -> Box<dyn Beholder> {
        Box::new(BiomeBeholder::default())
    }

    fn tool_version_range(&self) -> Option<ToolVersionRange> {
        // --reporter=json was available from Biome 1.0.
        Some(ToolVersionRange { min: Some("1.0.0"), max: None })
    }
}

/// Per-run Biome JSON parser. Buffers all output; parses in `on_done`.
///
/// Biome writes a single JSON object to stdout at process exit. Like ESLint,
/// the entire document appears at once, so incremental parsing is not possible.
pub struct BiomeBeholder {
    buf: Vec<u8>,
    unknown_format: Option<String>,
}

impl Default for BiomeBeholder {
    fn default() -> Self {
        Self { buf: Vec::new(), unknown_format: None }
    }
}

impl Beholder for BiomeBeholder {
    fn name(&self) -> &'static str { "biome" }
    fn version(&self) -> &'static str { "1.0" }
    fn mode(&self) -> BeholderMode {
        BeholderMode::Rewriter {
            adjust_argv: |argv| {
                if !argv.iter().any(|a| a == "--reporter=json") {
                    argv.push("--reporter=json".to_owned());
                }
            },
        }
    }

    fn parse_chunk(&mut self, chunk: &OutputChunk) -> Vec<Event> {
        self.buf.extend_from_slice(&chunk.bytes);
        Vec::new()
    }

    fn on_done(&mut self, run_id: &crate::types::TaskRunId, offset_ms: u32) -> Vec<Event> {
        if self.buf.is_empty() {
            return Vec::new();
        }
        let Ok(s) = std::str::from_utf8(&self.buf) else {
            self.unknown_format = Some("non-UTF-8 output".to_owned());
            return Vec::new();
        };
        let s = s.trim();
        match serde_json::from_str::<BiomeOutput>(s) {
            Ok(output) => biome_to_events(output.diagnostics, run_id, offset_ms),
            Err(e) => {
                self.unknown_format = Some(format!("failed to parse Biome JSON: {e}"));
                Vec::new()
            }
        }
    }

    fn unknown_format_reason(&self) -> Option<&str> {
        self.unknown_format.as_deref()
    }
}

// ─── Biome JSON structs ───────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct BiomeOutput {
    #[serde(default)]
    diagnostics: Vec<BiomeDiagnostic>,
}

#[derive(serde::Deserialize)]
struct BiomeDiagnostic {
    category: Option<String>,
    severity: String,
    description: String,
    location: Option<BiomeLocation>,
}

#[derive(serde::Deserialize)]
struct BiomeLocation {
    path: Option<BiomePath>,
}

#[derive(serde::Deserialize)]
struct BiomePath {
    file: Option<String>,
}

fn biome_to_events(
    diagnostics: Vec<BiomeDiagnostic>,
    run_id: &crate::types::TaskRunId,
    offset_ms: u32,
) -> Vec<Event> {
    diagnostics.into_iter().map(|d| {
        let level = match d.severity.as_str() {
            "fatal" => Level::Fatal,
            "error" => Level::Error,
            "warning" => Level::Warn,
            "information" => Level::Info,
            "hint" => Level::Debug,
            _ => Level::Warn,
        };
        let mut fields = serde_json::json!({});
        if let Some(ref cat) = d.category {
            fields["error"] = serde_json::json!({ "code": cat });
        }
        if let Some(loc) = d.location {
            if let Some(path) = loc.path {
                if let Some(file) = path.file {
                    fields["file"] = serde_json::json!({ "path": file });
                }
            }
        }
        Event {
            run_id: run_id.clone(),
            seq: 0,
            offset_ms,
            level,
            target: "biome".to_owned(),
            msg: d.description,
            fields,
            anchor: None,
            source: EventSource::Beholder {
                name: "biome".to_owned(),
                version: "1.0".to_owned(),
            },
        }
    }).collect()
}

// ─── JS test beholder (vitest / jest / bun-test) ─────────────────────────────

/// Argv rewriter for vitest: adds `--reporter=json` when not already present.
fn vitest_adjust_argv(argv: &mut Vec<String>) {
    if !argv.iter().any(|a| a == "--reporter=json") {
        argv.push("--reporter=json".to_owned());
    }
}

/// Argv rewriter for jest: adds `--json` when not already present.
fn jest_adjust_argv(argv: &mut Vec<String>) {
    if !argv.iter().any(|a| a == "--json") {
        argv.push("--json".to_owned());
    }
}

/// Argv rewriter for bun test: adds `--reporter=json` when not already present.
fn bun_test_adjust_argv(argv: &mut Vec<String>) {
    if !argv.iter().any(|a| a == "--reporter=json") {
        argv.push("--reporter=json".to_owned());
    }
}

/// Factory for the bundled vitest beholder (Tier 1.5, Rewriter mode).
///
/// Matches `vitest` invocations and rewrites argv to add `--reporter=json`,
/// which makes vitest emit a single Jest-compatible JSON document to stdout
/// at exit. Parsing happens in `on_done`.
///
/// Declines when:
/// - `--version` / `-v` are present (non-test invocations).
/// - `--reporter=<value>` is already set to a non-json reporter (respects
///   explicit user intent; if it's already `json`, rewriter is a no-op).
pub struct VitestBeholderFactory;

impl BeholderFactory for VitestBeholderFactory {
    fn name(&self) -> &'static str { "vitest" }
    fn version(&self) -> &'static str { "1.0" }

    fn matches(&self, argv: &[String]) -> bool {
        if argv.first().map(String::as_str) != Some("vitest") {
            return false;
        }
        if argv.iter().any(|a| matches!(a.as_str(), "--version" | "-v")) {
            return false;
        }
        // Decline if user set a non-json reporter (--reporter=<x> or --reporter <x>).
        let reporter_eq = argv.iter()
            .find(|a| a.starts_with("--reporter="))
            .map(|a| a.trim_start_matches("--reporter="));
        let reporter_space = argv.windows(2)
            .find(|w| w[0] == "--reporter")
            .map(|w| w[1].as_str());
        let explicit = reporter_eq.or(reporter_space);
        matches!(explicit, None | Some("json"))
    }

    fn mode(&self) -> BeholderMode {
        BeholderMode::Rewriter { adjust_argv: vitest_adjust_argv }
    }

    fn create(&self) -> Box<dyn Beholder> {
        Box::new(JsTestBeholder::new("vitest", "1.0", vitest_adjust_argv))
    }

    fn tool_version_range(&self) -> Option<ToolVersionRange> {
        Some(ToolVersionRange { min: Some("1.0.0"), max: None })
    }
}

/// Factory for the bundled jest beholder (Tier 1.5, Rewriter mode).
///
/// Matches `jest` invocations and rewrites argv to add `--json`, which makes
/// jest emit a JSON test report to stdout at exit. Parsing happens in `on_done`.
///
/// Declines when:
/// - `--version` / `-v` are present.
/// - `--outputFile` is present (JSON would go to a file, not stdout).
pub struct JestBeholderFactory;

impl BeholderFactory for JestBeholderFactory {
    fn name(&self) -> &'static str { "jest" }
    fn version(&self) -> &'static str { "27.0" }

    fn matches(&self, argv: &[String]) -> bool {
        if argv.first().map(String::as_str) != Some("jest") {
            return false;
        }
        if argv.iter().any(|a| matches!(a.as_str(), "--version" | "-v")) {
            return false;
        }
        // --outputFile sends JSON to a file rather than stdout; we can't capture that.
        if argv.iter().any(|a| a.starts_with("--outputFile")) {
            return false;
        }
        true
    }

    fn mode(&self) -> BeholderMode {
        BeholderMode::Rewriter { adjust_argv: jest_adjust_argv }
    }

    fn create(&self) -> Box<dyn Beholder> {
        Box::new(JsTestBeholder::new("jest", "27.0", jest_adjust_argv))
    }

    fn tool_version_range(&self) -> Option<ToolVersionRange> {
        Some(ToolVersionRange { min: Some("27.0.0"), max: None })
    }
}

/// Factory for the bundled bun-test beholder (Tier 1.5, Rewriter mode).
///
/// Matches `bun test` invocations (argv[0]="bun", argv[1]="test") and rewrites
/// argv to add `--reporter=json`. Parsing happens in `on_done`.
///
/// Declines when:
/// - `--version` is present.
/// - `--reporter=<value>` is already set to a non-json reporter.
pub struct BunTestBeholderFactory;

impl BeholderFactory for BunTestBeholderFactory {
    fn name(&self) -> &'static str { "bun-test" }
    fn version(&self) -> &'static str { "1.0" }

    fn matches(&self, argv: &[String]) -> bool {
        if argv.first().map(String::as_str) != Some("bun") {
            return false;
        }
        if argv.get(1).map(String::as_str) != Some("test") {
            return false;
        }
        if argv.iter().any(|a| a == "--version") {
            return false;
        }
        let reporter = argv.iter()
            .find(|a| a.starts_with("--reporter="))
            .map(|a| a.trim_start_matches("--reporter="));
        matches!(reporter, None | Some("json"))
    }

    fn mode(&self) -> BeholderMode {
        BeholderMode::Rewriter { adjust_argv: bun_test_adjust_argv }
    }

    fn create(&self) -> Box<dyn Beholder> {
        Box::new(JsTestBeholder::new("bun-test", "1.0", bun_test_adjust_argv))
    }

    fn tool_version_range(&self) -> Option<ToolVersionRange> {
        Some(ToolVersionRange { min: Some("1.0.0"), max: None })
    }
}

/// Per-run JS test JSON parser shared by vitest, jest, and bun-test.
///
/// All three produce a Jest-compatible JSON report on stdout at process exit
/// (via `--reporter=json` for vitest/bun-test, `--json` for jest). There is no
/// streaming line-by-line format, so incremental parsing is not possible: the
/// beholder accumulates raw bytes and flushes structured events in `on_done`.
///
/// Events emitted:
/// - One summary event (Info on success, Error on failure) with pass/fail counts.
/// - One Error event per failed assertion with the test name and failure message.
pub struct JsTestBeholder {
    tool: &'static str,
    version: &'static str,
    adjust_argv_fn: fn(&mut Vec<String>),
    buf: Vec<u8>,
    unknown_format: Option<String>,
}

impl JsTestBeholder {
    fn new(tool: &'static str, version: &'static str, adjust_argv_fn: fn(&mut Vec<String>)) -> Self {
        Self { tool, version, adjust_argv_fn, buf: Vec::new(), unknown_format: None }
    }
}

impl Beholder for JsTestBeholder {
    fn name(&self) -> &'static str { self.tool }
    fn version(&self) -> &'static str { self.version }
    fn mode(&self) -> BeholderMode {
        BeholderMode::Rewriter { adjust_argv: self.adjust_argv_fn }
    }

    fn parse_chunk(&mut self, chunk: &OutputChunk) -> Vec<Event> {
        self.buf.extend_from_slice(&chunk.bytes);
        Vec::new()
    }

    fn on_done(&mut self, run_id: &crate::types::TaskRunId, offset_ms: u32) -> Vec<Event> {
        if self.buf.is_empty() {
            return Vec::new();
        }
        let Ok(s) = std::str::from_utf8(&self.buf) else {
            self.unknown_format = Some("non-UTF-8 output".to_owned());
            return Vec::new();
        };
        let s = s.trim();
        match serde_json::from_str::<JsTestReport>(s) {
            Ok(report) => js_test_to_events(report, run_id, offset_ms, self.tool, self.version),
            Err(e) => {
                self.unknown_format = Some(format!("failed to parse {} JSON: {e}", self.tool));
                Vec::new()
            }
        }
    }

    fn unknown_format_reason(&self) -> Option<&str> {
        self.unknown_format.as_deref()
    }
}

// ─── JS test JSON structs ─────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct JsTestReport {
    #[serde(default)]
    success: bool,
    #[serde(rename = "numTotalTests", default)]
    num_total: u32,
    #[serde(rename = "numPassedTests", default)]
    num_passed: u32,
    #[serde(rename = "numFailedTests", default)]
    num_failed: u32,
    #[serde(rename = "numPendingTests", default)]
    num_pending: u32,
    #[serde(rename = "testResults", default)]
    test_results: Vec<JsTestSuiteResult>,
}

#[derive(serde::Deserialize)]
struct JsTestSuiteResult {
    #[serde(rename = "testFilePath")]
    file_path: String,
    #[serde(rename = "assertionResults", default)]
    assertions: Vec<JsTestAssertionResult>,
}

#[derive(serde::Deserialize)]
struct JsTestAssertionResult {
    #[serde(rename = "fullName", default)]
    full_name: String,
    status: String,
    #[serde(rename = "failureMessages", default)]
    failure_messages: Vec<String>,
}

fn js_test_to_events(
    report: JsTestReport,
    run_id: &crate::types::TaskRunId,
    offset_ms: u32,
    tool: &'static str,
    version: &'static str,
) -> Vec<Event> {
    let mut events = Vec::new();

    // Summary event: one per run.
    let summary_msg = if report.success {
        format!("{} passed", report.num_passed)
    } else {
        format!("{} failed, {} passed", report.num_failed, report.num_passed)
    };
    events.push(Event {
        run_id: run_id.clone(),
        seq: 0,
        offset_ms,
        level: if report.success { Level::Info } else { Level::Error },
        target: tool.to_owned(),
        msg: summary_msg,
        fields: serde_json::json!({
            "test": {
                "total": report.num_total,
                "passed": report.num_passed,
                "failed": report.num_failed,
                "pending": report.num_pending,
            },
            "build": { "success": report.success }
        }),
        anchor: None,
        source: EventSource::Beholder {
            name: tool.to_owned(),
            version: version.to_owned(),
        },
    });

    // One Error event per failed assertion.
    for suite in &report.test_results {
        for assertion in &suite.assertions {
            if assertion.status != "failed" {
                continue;
            }
            // Use the first failure message (first line only to keep events compact).
            let msg = assertion.failure_messages
                .first()
                .and_then(|s| s.lines().next())
                .map(str::to_owned)
                .unwrap_or_else(|| assertion.full_name.clone());
            events.push(Event {
                run_id: run_id.clone(),
                seq: 0,
                offset_ms,
                level: Level::Error,
                target: format!("{}::test", tool),
                msg,
                fields: serde_json::json!({
                    "test": { "name": assertion.full_name },
                    "file": { "path": suite.file_path },
                }),
                anchor: None,
                source: EventSource::Beholder {
                    name: tool.to_owned(),
                    version: version.to_owned(),
                },
            });
        }
    }

    events
}

// ─── pytest beholder ─────────────────────────────────────────────────────────

/// Factory for the bundled pytest beholder (Tier 1.5, Parser mode).
///
/// Matches `pytest`, `py.test`, and `python[-3] -m pytest` invocations and
/// parses the standard human-readable output for structured events. Parser mode
/// is used because pytest-json-report is not universally available; the standard
/// text format is stable across all supported pytest versions (≥ 7).
///
/// Events emitted:
/// - One Error event per `FAILED`/`ERROR` line in the short test summary.
/// - One summary event (Info on success, Error on failure) from the final
///   `N failed, M passed[…] in Ws` separator line.
///
/// Declines when:
/// - `--version` / `-V` / `--help` / `-h` are present.
/// - `--collect-only` / `--co` are present (no test execution, different output).
pub struct PytestBeholderFactory;

impl BeholderFactory for PytestBeholderFactory {
    fn name(&self) -> &'static str { "pytest" }
    fn version(&self) -> &'static str { "7.0" }

    fn matches(&self, argv: &[String]) -> bool {
        let first = argv.first().map(String::as_str);
        let is_direct = matches!(first, Some("pytest") | Some("py.test"));
        let is_python_m = matches!(first, Some("python") | Some("python3"))
            && argv.windows(2).any(|w| w[0] == "-m" && w[1] == "pytest");
        if !is_direct && !is_python_m {
            return false;
        }
        for a in argv {
            match a.as_str() {
                "--version" | "-V" | "--help" | "-h" => return false,
                "--collect-only" | "--co" => return false,
                _ => {}
            }
        }
        true
    }

    fn mode(&self) -> BeholderMode { BeholderMode::Parser }

    fn create(&self) -> Box<dyn Beholder> { Box::new(PytestBeholder::default()) }

    fn tool_version_range(&self) -> Option<ToolVersionRange> {
        Some(ToolVersionRange { min: Some("7.0.0"), max: None })
    }
}

/// Per-run pytest text-output parser.
///
/// Processes lines incrementally. Emits:
/// - Error events for `FAILED`/`ERROR` entries in the short test summary section.
/// - A summary event from the final `=== N failed, M passed … ===` line.
pub struct PytestBeholder {
    buf: Vec<u8>,
    /// The inner text of the last `=== … ===` summary separator seen.
    summary_line: Option<String>,
    unknown_format: Option<String>,
}

impl Default for PytestBeholder {
    fn default() -> Self {
        Self { buf: Vec::new(), summary_line: None, unknown_format: None }
    }
}

impl Beholder for PytestBeholder {
    fn name(&self) -> &'static str { "pytest" }
    fn version(&self) -> &'static str { "7.0" }
    fn mode(&self) -> BeholderMode { BeholderMode::Parser }

    fn parse_chunk(&mut self, chunk: &OutputChunk) -> Vec<Event> {
        self.buf.extend_from_slice(&chunk.bytes);
        let mut events = Vec::new();

        let source = EventSource::Beholder {
            name: "pytest".to_owned(),
            version: "7.0".to_owned(),
        };

        while let Some(nl) = self.buf.iter().position(|&b| b == b'\n') {
            let raw: Vec<u8> = self.buf.drain(..=nl).collect();
            let Ok(line) = std::str::from_utf8(raw.trim_ascii_end()) else { continue };
            if line.is_empty() { continue }

            if let Some(ev) = parse_pytest_failure_line(line, chunk, &source) {
                events.push(ev);
                continue;
            }

            // Track the final `=== N failed, M passed … ===` line; may be updated
            // multiple times in watch/loop mode — we keep the latest.
            if line.starts_with("==") && line.ends_with("==") {
                let inner = line.trim_matches('=').trim();
                if inner.contains(" passed") || inner.contains(" failed")
                    || inner.contains(" error")
                {
                    self.summary_line = Some(inner.to_owned());
                }
            }
        }

        events
    }

    fn on_done(&mut self, run_id: &crate::types::TaskRunId, offset_ms: u32) -> Vec<Event> {
        let Some(ref summary) = self.summary_line else {
            return Vec::new();
        };
        let Ok(parsed) = parse_pytest_summary(summary) else {
            self.unknown_format = Some(format!("could not parse pytest summary: {summary:?}"));
            return Vec::new();
        };

        let level = if parsed.failed > 0 || parsed.errors > 0 { Level::Error } else { Level::Info };
        let msg = format_pytest_summary_msg(&parsed);

        vec![Event {
            run_id: run_id.clone(),
            seq: 0,
            offset_ms,
            level,
            target: "pytest".to_owned(),
            msg,
            fields: serde_json::json!({
                "test": {
                    "total":   parsed.passed + parsed.failed + parsed.skipped + parsed.errors,
                    "passed":  parsed.passed,
                    "failed":  parsed.failed,
                    "skipped": parsed.skipped,
                    "errors":  parsed.errors,
                },
                "build": { "success": parsed.failed == 0 && parsed.errors == 0 }
            }),
            anchor: None,
            source: EventSource::Beholder {
                name: "pytest".to_owned(),
                version: "7.0".to_owned(),
            },
        }]
    }

    fn unknown_format_reason(&self) -> Option<&str> { self.unknown_format.as_deref() }
}

// ─── pytest parsing helpers ───────────────────────────────────────────────────

/// Parse a `FAILED` or `ERROR` short-summary line into an Event, or return None.
///
/// Formats handled:
/// - `FAILED tests/foo.py::test_name - AssertionError: assert 1 == 2`
/// - `ERROR tests/broken.py - ImportError: No module named 'x'`
fn parse_pytest_failure_line(
    line: &str,
    chunk: &OutputChunk,
    source: &EventSource,
) -> Option<Event> {
    let (label, rest) = if let Some(r) = line.strip_prefix("FAILED ") {
        ("FAILED", r)
    } else if let Some(r) = line.strip_prefix("ERROR ") {
        ("ERROR", r)
    } else {
        return None;
    };

    // Require a `.py` in the nodeid portion to avoid false positives on
    // arbitrary lines that happen to start with these words.
    let (nodeid, reason) = if let Some(idx) = rest.find(" - ") {
        let nodeid = &rest[..idx];
        if !nodeid.contains(".py") { return None; }
        (nodeid, rest[idx + 3..].trim())
    } else {
        if !rest.contains(".py") { return None; }
        (rest, "")
    };

    let (file_path, test_name) = if let Some(idx) = nodeid.find("::") {
        (&nodeid[..idx], &nodeid[idx + 2..])
    } else {
        (nodeid, "")
    };

    let msg = if reason.is_empty() {
        format!("{label} {nodeid}")
    } else {
        reason.to_owned()
    };

    Some(Event {
        run_id: chunk.run_id.clone(),
        seq: 0,
        offset_ms: chunk.offset_ms,
        level: Level::Error,
        target: "pytest::test".to_owned(),
        msg,
        fields: serde_json::json!({
            "test": { "name": test_name },
            "file": { "path": file_path },
        }),
        anchor: Some(ChunkRef { seq: chunk.seq }),
        source: source.clone(),
    })
}

struct PytestSummary {
    passed:  u32,
    failed:  u32,
    skipped: u32,
    errors:  u32,
}

/// Parse `"2 failed, 5 passed, 1 skipped in 1.23s"` → `PytestSummary`.
fn parse_pytest_summary(s: &str) -> Result<PytestSummary, ()> {
    // Strip duration suffix " in W.XXs" (optional — may be absent on early exit).
    let s = if let Some(idx) = s.rfind(" in ") { &s[..idx] } else { s };

    let mut passed  = 0u32;
    let mut failed  = 0u32;
    let mut skipped = 0u32;
    let mut errors  = 0u32;
    let mut any = false;

    for part in s.split(", ") {
        let part = part.trim();
        let mut it = part.splitn(2, ' ');
        let count: u32 = it.next().and_then(|n| n.parse().ok()).ok_or(())?;
        let kind = it.next().ok_or(())?;
        any = true;
        if kind.starts_with("passed")  { passed  = count; }
        else if kind.starts_with("failed")  { failed  = count; }
        else if kind.starts_with("skipped") { skipped = count; }
        else if kind.starts_with("error")   { errors  = count; }
        // Other kinds (warnings, deselected, xfailed, xpassed) pass through.
    }

    if !any { return Err(()); }
    Ok(PytestSummary { passed, failed, skipped, errors })
}

fn format_pytest_summary_msg(s: &PytestSummary) -> String {
    let mut parts: Vec<String> = Vec::new();
    if s.failed  > 0 { parts.push(format!("{} failed", s.failed)); }
    if s.errors  > 0 { parts.push(format!("{} error{}", s.errors, if s.errors == 1 { "" } else { "s" })); }
    if s.passed  > 0 { parts.push(format!("{} passed", s.passed)); }
    if s.skipped > 0 { parts.push(format!("{} skipped", s.skipped)); }
    if parts.is_empty() { return "no tests ran".to_owned(); }
    parts.join(", ")
}

// ─── vite-build beholder ─────────────────────────────────────────────────────

/// Factory for the bundled vite-build beholder (Tier 1.5, Parser mode).
///
/// Matches `vite build` invocations (bare or via wrapper: `bunx vite build`,
/// `pnpm vite build`). Parses the human-formatted output for `(!)` warnings,
/// per-asset bundle sizes, and the final `built in X.XXs` timing line.
///
/// Declines when:
/// - `argv[0]` (post-wrapper-stripping) is not `vite`.
/// - No `build` subcommand is present in argv.
/// - `--help` / `-h` / `--version` flags are present.
pub struct ViteBuildBeholderFactory;

impl BeholderFactory for ViteBuildBeholderFactory {
    fn name(&self) -> &'static str { "vite-build" }
    fn version(&self) -> &'static str { "4.0" }

    fn matches(&self, argv: &[String]) -> bool {
        if argv.first().map(String::as_str) != Some("vite") { return false; }
        if !argv.iter().skip(1).any(|a| a == "build") { return false; }
        for a in argv {
            match a.as_str() {
                "--help" | "-h" | "--version" => return false,
                _ => {}
            }
        }
        true
    }

    fn mode(&self) -> BeholderMode { BeholderMode::Parser }

    fn create(&self) -> Box<dyn Beholder> { Box::new(ViteBuildBeholder::default()) }

    fn tool_version_range(&self) -> Option<ToolVersionRange> {
        Some(ToolVersionRange { min: Some("4.0.0"), max: None })
    }
}

/// Per-run vite build output parser.
///
/// Processes lines incrementally. Emits:
/// - `Warn` events for `(!) <text>` warning lines (oversized chunks, eval, etc.)
/// - `Info` events for `dist/<file>  X kB  │ gzip: Y kB` bundle-output lines.
/// - An `Info` timing event from the `✓ built in X.XXs` line.
pub struct ViteBuildBeholder {
    buf: Vec<u8>,
    unknown_format: Option<String>,
}

impl Default for ViteBuildBeholder {
    fn default() -> Self {
        Self { buf: Vec::new(), unknown_format: None }
    }
}

impl Beholder for ViteBuildBeholder {
    fn name(&self) -> &'static str { "vite-build" }
    fn version(&self) -> &'static str { "4.0" }
    fn mode(&self) -> BeholderMode { BeholderMode::Parser }

    fn parse_chunk(&mut self, chunk: &OutputChunk) -> Vec<Event> {
        self.buf.extend_from_slice(&chunk.bytes);
        let mut events = Vec::new();

        let source = EventSource::Beholder {
            name: "vite-build".to_owned(),
            version: "4.0".to_owned(),
        };

        while let Some(nl) = self.buf.iter().position(|&b| b == b'\n') {
            let raw: Vec<u8> = self.buf.drain(..=nl).collect();
            let Ok(line) = std::str::from_utf8(raw.trim_ascii_end()) else { continue };
            if line.is_empty() { continue }

            // (!) warning lines
            if let Some(warn_text) = line.strip_prefix("(!) ") {
                events.push(Event {
                    run_id: chunk.run_id.clone(),
                    seq: 0,
                    offset_ms: chunk.offset_ms,
                    level: Level::Warn,
                    target: "vite-build::warning".to_owned(),
                    msg: warn_text.trim().to_owned(),
                    fields: serde_json::json!({ "build": { "warning": true } }),
                    anchor: Some(ChunkRef { seq: chunk.seq }),
                    source: source.clone(),
                });
                continue;
            }

            // dist/<file>  X.XX kB  [│ gzip:  Y.YY kB]
            if let Some(ev) = parse_vite_bundle_line(line, chunk, &source) {
                events.push(ev);
                continue;
            }

            // "✓ built in X.XXs" or "built in X.XXs"
            if let Some(ms) = parse_vite_built_ms(line) {
                events.push(Event {
                    run_id: chunk.run_id.clone(),
                    seq: 0,
                    offset_ms: chunk.offset_ms,
                    level: Level::Info,
                    target: "vite-build".to_owned(),
                    msg: format!("built in {:.2}s", ms as f64 / 1000.0),
                    fields: serde_json::json!({
                        "build": { "duration_ms": ms, "success": true }
                    }),
                    anchor: Some(ChunkRef { seq: chunk.seq }),
                    source: source.clone(),
                });
            }
        }

        events
    }

    fn unknown_format_reason(&self) -> Option<&str> { self.unknown_format.as_deref() }
}

// ─── vite-build parsing helpers ───────────────────────────────────────────────

/// Parse `dist/<file>  X.XX kB [│ gzip:  Y.YY kB]` into an Event, or None.
///
/// The `│` (U+2502) separator and gzip column are optional.
fn parse_vite_bundle_line(line: &str, chunk: &OutputChunk, source: &EventSource) -> Option<Event> {
    let trimmed = line.trim();
    if !trimmed.starts_with("dist/") { return None; }
    if !trimmed.contains("kB") { return None; }

    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    let file = tokens.first()?;

    // Collect all `float kB` pairs in order: first is size, second (if any) is gzip.
    let mut values: Vec<f64> = Vec::new();
    let mut i = 1;
    while i < tokens.len() {
        if let Ok(v) = tokens[i].parse::<f64>() {
            if tokens.get(i + 1).copied() == Some("kB") {
                values.push(v);
                i += 2;
                continue;
            }
        }
        i += 1;
    }
    let size_kb = *values.first()?;
    let gzip_kb = values.get(1).copied();

    let msg = if let Some(g) = gzip_kb {
        format!("{file}: {size_kb:.2} kB (gzip: {g:.2} kB)")
    } else {
        format!("{file}: {size_kb:.2} kB")
    };

    let mut fields = serde_json::json!({
        "file": { "path": file },
        "build": { "size_kb": size_kb },
    });
    if let Some(g) = gzip_kb {
        fields["build"]["gzip_kb"] = serde_json::json!(g);
    }

    Some(Event {
        run_id: chunk.run_id.clone(),
        seq: 0,
        offset_ms: chunk.offset_ms,
        level: Level::Info,
        target: "vite-build::bundle".to_owned(),
        msg,
        fields,
        anchor: Some(ChunkRef { seq: chunk.seq }),
        source: source.clone(),
    })
}

/// Parse `"✓ built in 2.43s"` or `"built in 2.43s"` → duration in ms.
fn parse_vite_built_ms(line: &str) -> Option<u64> {
    let idx = line.find("built in ")?;
    let rest = line[idx + "built in ".len()..].trim();
    let secs_str = rest.trim_end_matches(|c: char| c.is_alphabetic());
    let secs: f64 = secs_str.parse().ok()?;
    Some((secs * 1000.0).round() as u64)
}


// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    // ─── Test fixtures ────────────────────────────────────────────────────────

    /// A no-op parser beholder that matches commands starting with a prefix.
    struct PrefixFactory {
        prefix: &'static str,
        name: &'static str,
    }

    struct NoopBeholder {
        name: &'static str,
    }

    impl BeholderFactory for PrefixFactory {
        fn name(&self) -> &'static str { self.name }
        fn version(&self) -> &'static str { "1.0" }
        fn matches(&self, argv: &[String]) -> bool {
            argv.first().map(|s| s.starts_with(self.prefix)).unwrap_or(false)
        }
        fn mode(&self) -> BeholderMode { BeholderMode::Parser }
        fn create(&self) -> Box<dyn Beholder> { Box::new(NoopBeholder { name: self.name }) }
    }

    impl Beholder for NoopBeholder {
        fn name(&self) -> &'static str { self.name }
        fn version(&self) -> &'static str { "1.0" }
        fn mode(&self) -> BeholderMode { BeholderMode::Parser }
        fn parse_chunk(&mut self, _chunk: &OutputChunk) -> Vec<Event> { vec![] }
    }

    /// A rewriter beholder that declines when --no-json is present.
    struct RewriterFactory;
    struct RewriterBeholder;

    impl BeholderFactory for RewriterFactory {
        fn name(&self) -> &'static str { "cargo" }
        fn version(&self) -> &'static str { "1.78" }
        fn matches(&self, argv: &[String]) -> bool {
            argv.first().map(|s| s == "cargo").unwrap_or(false)
                && !argv.iter().any(|a| a == "--no-json")
        }
        fn mode(&self) -> BeholderMode {
            BeholderMode::Rewriter {
                adjust_argv: |argv| {
                    argv.push("--message-format=json-render-diagnostics".to_string());
                },
            }
        }
        fn create(&self) -> Box<dyn Beholder> { Box::new(RewriterBeholder) }
    }

    impl Beholder for RewriterBeholder {
        fn name(&self) -> &'static str { "cargo" }
        fn version(&self) -> &'static str { "1.78" }
        fn mode(&self) -> BeholderMode {
            BeholderMode::Rewriter {
                adjust_argv: |argv| {
                    argv.push("--message-format=json-render-diagnostics".to_string());
                },
            }
        }
        fn parse_chunk(&mut self, _chunk: &OutputChunk) -> Vec<Event> { vec![] }
    }

    // ─── Registry tests ───────────────────────────────────────────────────────

    #[test]
    fn empty_registry_returns_none_auto() {
        let registry = BeholderRegistry::new();
        let result = registry.attach("cargo check", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_none());
        assert_eq!(result.status.text, "none:auto");
        assert_eq!(result.argv, vec!["cargo", "check"]);
    }

    #[test]
    fn auto_attaches_first_matching() {
        let mut registry = BeholderRegistry::new();
        registry.register(Box::new(PrefixFactory { prefix: "cargo", name: "cargo" }));
        let result = registry.attach("cargo check --workspace", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some());
        assert_eq!(result.status.text, "attached:cargo@1.0");
    }

    #[test]
    fn rewriter_adjusts_argv_in_result() {
        let mut registry = BeholderRegistry::new();
        registry.register(Box::new(RewriterFactory));
        let result = registry.attach("cargo check --workspace", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some());
        // Rewrite must be surfaced in status text and in rewrite_added.
        assert!(
            result.status.text.starts_with("attached:cargo@1.78"),
            "got: {}",
            result.status.text
        );
        assert!(
            result.status.text.contains("rewrite="),
            "rewrite must be surfaced in status; got: {}",
            result.status.text
        );
        assert!(
            result.argv.contains(&"--message-format=json-render-diagnostics".to_string()),
            "rewriter must append the JSON flag"
        );
        assert_eq!(
            result.status.rewrite_added.as_deref(),
            Some(vec!["--message-format=json-render-diagnostics".to_string()].as_slice()),
            "rewrite_added must list the injected arg"
        );
    }

    #[test]
    fn auto_respects_decline_from_matches() {
        let mut registry = BeholderRegistry::new();
        registry.register(Box::new(RewriterFactory));
        let result = registry.attach("cargo check --no-json", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_none());
        assert_eq!(result.status.text, "none:auto");
        // argv unchanged — no rewrite when beholder declined
        assert!(!result.argv.contains(&"--message-format=json-render-diagnostics".to_string()));
    }

    #[test]
    fn explicit_none_bypasses_registry() {
        let mut registry = BeholderRegistry::new();
        registry.register(Box::new(PrefixFactory { prefix: "cargo", name: "cargo" }));
        let result = registry.attach("cargo check", &BeholderSelect::None, false);
        assert!(result.beholder.is_none());
        assert_eq!(result.status.text, "none:explicit");
    }

    #[test]
    fn force_pins_by_name() {
        let mut registry = BeholderRegistry::new();
        registry.register(Box::new(PrefixFactory { prefix: "tsc", name: "tsc" }));
        registry.register(Box::new(PrefixFactory { prefix: "cargo", name: "cargo" }));
        // Force "cargo" even though Auto would have matched "tsc" with "tsc" prefix
        let result = registry.attach("cargo check", &BeholderSelect::Force("cargo".to_string()), false);
        assert!(result.beholder.is_some());
        assert!(result.status.text.starts_with("forced:cargo"), "got: {}", result.status.text);
    }

    #[test]
    fn force_against_flags_when_would_decline() {
        let mut registry = BeholderRegistry::new();
        registry.register(Box::new(RewriterFactory));
        // cargo with --no-json would normally decline; Force overrides
        let result =
            registry.attach("cargo check --no-json", &BeholderSelect::Force("cargo".to_string()), false);
        assert!(result.beholder.is_some());
        assert!(
            result.status.text.contains("forced-against-flags"),
            "got: {}",
            result.status.text
        );
    }

    #[test]
    fn force_unknown_name_returns_none() {
        let registry = BeholderRegistry::new();
        let result =
            registry.attach("cargo check", &BeholderSelect::Force("unknown".to_string()), false);
        assert!(result.beholder.is_none());
    }

    #[test]
    fn priority_order_first_match_wins() {
        let mut registry = BeholderRegistry::new();
        registry.register(Box::new(PrefixFactory { prefix: "cargo", name: "first" }));
        registry.register(Box::new(PrefixFactory { prefix: "cargo", name: "second" }));
        let result = registry.attach("cargo check", &BeholderSelect::Auto, false);
        assert_eq!(result.status.text, "attached:first@1.0");
    }

    // ─── TTY-aware behavior tests ─────────────────────────────────────────────

    #[test]
    fn tty_attached_causes_rewriter_to_decline_in_auto() {
        let mut registry = BeholderRegistry::new();
        registry.register(Box::new(RewriterFactory));
        let result = registry.attach("cargo check --workspace", &BeholderSelect::Auto, true);
        // Beholder declines to preserve human output on the TTY.
        assert!(result.beholder.is_none(), "rewriter must not attach when tty_attached");
        assert!(
            result.status.text.contains("declined:cargo"),
            "got: {}",
            result.status.text
        );
        assert!(
            result.status.text.contains("tty-attached"),
            "got: {}",
            result.status.text
        );
        // argv must be unchanged — no rewrite applied.
        assert!(!result.argv.contains(&"--message-format=json-render-diagnostics".to_string()));
    }

    #[test]
    fn tty_attached_does_not_affect_parser_beholder() {
        let mut registry = BeholderRegistry::new();
        registry.register(Box::new(PrefixFactory { prefix: "cargo", name: "cargo" }));
        // Parser beholders are fine on a TTY — they don't rewrite argv.
        let result = registry.attach("cargo check --workspace", &BeholderSelect::Auto, true);
        assert!(result.beholder.is_some(), "parser beholder must attach even on a TTY");
        assert_eq!(result.status.text, "attached:cargo@1.0");
    }

    #[test]
    fn force_overrides_tty_decline_and_records_forced_against_tty() {
        let mut registry = BeholderRegistry::new();
        registry.register(Box::new(RewriterFactory));
        // Force overrides the TTY-decline rule; operator explicitly wants structured output.
        let result =
            registry.attach("cargo check", &BeholderSelect::Force("cargo".to_string()), true);
        assert!(result.beholder.is_some(), "Force must attach even on a TTY");
        assert!(
            result.status.text.contains("forced-against-tty"),
            "got: {}",
            result.status.text
        );
        // Rewrite still applied (forced) and surfaced.
        assert!(
            result.argv.contains(&"--message-format=json-render-diagnostics".to_string()),
            "rewrite must still be applied when forced on TTY"
        );
    }

    #[test]
    fn rewrite_surfaced_on_force_normal() {
        let mut registry = BeholderRegistry::new();
        registry.register(Box::new(RewriterFactory));
        let result =
            registry.attach("cargo check", &BeholderSelect::Force("cargo".to_string()), false);
        assert!(result.beholder.is_some());
        assert!(
            result.status.text.contains("forced:cargo"),
            "got: {}",
            result.status.text
        );
        assert!(
            result.status.text.contains("rewrite="),
            "rewrite must be surfaced on forced attach; got: {}",
            result.status.text
        );
        assert!(result.status.rewrite_added.is_some());
    }

    // ─── argv resolution tests ────────────────────────────────────────────────

    #[test]
    fn resolve_bare_command() {
        assert_eq!(resolve_argv("cargo check --workspace"), ["cargo", "check", "--workspace"]);
    }

    #[test]
    fn resolve_strips_bunx() {
        let argv = resolve_argv("bunx vitest --run");
        assert_eq!(argv[0], "vitest");
        assert_eq!(argv[1], "--run");
    }

    #[test]
    fn resolve_strips_npx() {
        let argv = resolve_argv("npx tsc --noEmit");
        assert_eq!(argv[0], "tsc");
        assert_eq!(argv[1], "--noEmit");
    }

    #[test]
    fn resolve_strips_pnpm() {
        let argv = resolve_argv("pnpm tsc");
        assert_eq!(argv[0], "tsc");
    }

    #[test]
    fn resolve_strips_npm_exec() {
        let argv = resolve_argv("npm exec tsc --noEmit");
        assert_eq!(argv[0], "tsc");
        assert_eq!(argv[1], "--noEmit");
    }

    #[test]
    fn resolve_pnpm_without_exec_subcommand() {
        // `pnpm tsc` — no exec subcommand, just strip pnpm
        let argv = resolve_argv("pnpm vitest --reporter=verbose");
        assert_eq!(argv[0], "vitest");
    }

    #[test]
    fn resolve_empty_command() {
        let argv = resolve_argv("");
        assert!(argv.is_empty());
    }

    // ─── CargoBeholderFactory tests ───────────────────────────────────────────

    fn dummy_chunk(bytes: &[u8]) -> OutputChunk {
        use crate::types::{Stream, TaskRunId};
        OutputChunk {
            run_id: TaskRunId::new(),
            seq: 0,
            offset_ms: 0,
            stream: Stream::Stdout,
            bytes: bytes.to_vec(),
        }
    }

    #[test]
    fn cargo_factory_matches_check() {
        let f = CargoBeholderFactory;
        let argv: Vec<String> = vec!["cargo".into(), "check".into(), "--workspace".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn cargo_factory_matches_clippy() {
        let f = CargoBeholderFactory;
        let argv: Vec<String> = vec!["cargo".into(), "clippy".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn cargo_factory_declines_explicit_message_format() {
        let f = CargoBeholderFactory;
        let argv: Vec<String> =
            vec!["cargo".into(), "check".into(), "--message-format=human".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn cargo_factory_declines_non_diag_subcommand() {
        let f = CargoBeholderFactory;
        let argv: Vec<String> = vec!["cargo".into(), "fmt".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn cargo_factory_declines_non_cargo() {
        let f = CargoBeholderFactory;
        let argv: Vec<String> = vec!["rustc".into(), "--edition=2021".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn cargo_factory_rewriter_adds_flag() {
        let registry = default_registry();
        let result = registry.attach("cargo check --workspace", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some(), "cargo beholder must attach");
        assert!(
            result.argv.contains(&"--message-format=json-render-diagnostics".to_string()),
            "rewriter must inject the JSON flag; got: {:?}",
            result.argv
        );
        // Rewrite must be visible on status.
        assert!(
            result.status.text.contains("rewrite="),
            "rewrite must be surfaced in status; got: {}",
            result.status.text
        );
    }

    #[test]
    fn cargo_beholder_parses_compiler_error() {
        let json = r#"{"reason":"compiler-message","package_id":"foo","manifest_path":"foo","target":{"kind":["lib"],"name":"foo","src_path":"src/lib.rs","edition":"2021","doctest":true,"test":true,"doc":true},"message":{"$message_type":"diagnostic","message":"mismatched types","code":{"code":"E0308","explanation":""},"level":"error","spans":[{"file_name":"src/lib.rs","byte_start":0,"byte_end":1,"line_start":5,"line_end":5,"column_start":1,"column_end":14,"is_primary":true,"text":[],"label":null,"suggested_replacement":null,"suggestion_applicability":null,"expansion":null}],"children":[],"rendered":"error[E0308]: mismatched types\n"}}"#;
        let chunk = dummy_chunk(format!("{json}\n").as_bytes());
        let mut b = CargoBeholder::default();
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.target, "cargo::rustc");
        assert_eq!(ev.level, Level::Error);
        assert_eq!(ev.msg, "mismatched types");
        assert_eq!(ev.fields["error"]["code"], "E0308");
        assert_eq!(ev.fields["file"]["path"], "src/lib.rs");
        assert_eq!(ev.fields["file"]["line"], 5u32);
    }

    #[test]
    fn cargo_beholder_parses_warning() {
        let json = r#"{"reason":"compiler-message","package_id":"foo","manifest_path":"foo","target":{"kind":["lib"],"name":"foo","src_path":"src/lib.rs","edition":"2021","doctest":true,"test":true,"doc":true},"message":{"$message_type":"diagnostic","message":"unused variable: `x`","code":{"code":"unused_variables","explanation":""},"level":"warning","spans":[{"file_name":"src/lib.rs","byte_start":0,"byte_end":1,"line_start":10,"line_end":10,"column_start":9,"column_end":10,"is_primary":true,"text":[],"label":null,"suggested_replacement":null,"suggestion_applicability":null,"expansion":null}],"children":[],"rendered":"warning: unused variable"}}"#;
        let chunk = dummy_chunk(format!("{json}\n").as_bytes());
        let mut b = CargoBeholder::default();
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, Level::Warn);
        assert_eq!(events[0].fields["file"]["line"], 10u32);
    }

    #[test]
    fn cargo_beholder_parses_build_finished_success() {
        let json = r#"{"reason":"build-finished","success":true}"#;
        let chunk = dummy_chunk(format!("{json}\n").as_bytes());
        let mut b = CargoBeholder::default();
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, Level::Info);
        assert_eq!(events[0].msg, "build finished");
    }

    #[test]
    fn cargo_beholder_parses_build_finished_failure() {
        let json = r#"{"reason":"build-finished","success":false}"#;
        let chunk = dummy_chunk(format!("{json}\n").as_bytes());
        let mut b = CargoBeholder::default();
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, Level::Error);
        assert_eq!(events[0].msg, "build failed");
    }

    #[test]
    fn cargo_beholder_skips_artifacts_and_unknown() {
        // compiler-artifact and build-script-executed should produce no events
        let artifact = r#"{"reason":"compiler-artifact","package_id":"foo","manifest_path":"foo","target":{"kind":["lib"],"name":"foo","src_path":"src/lib.rs","edition":"2021","doctest":true,"test":true,"doc":true},"profile":{"opt_level":"0","debuginfo":2,"debug_assertions":true,"overflow_checks":true,"test":false},"features":[],"filenames":[],"executable":null,"fresh":false}"#;
        let script = r#"{"reason":"build-script-executed","package_id":"foo 0.1.0","linked_libs":[],"linked_paths":[],"cfgs":[],"env":[],"out_dir":"/tmp"}"#;
        let mut b = CargoBeholder::default();
        let chunk = dummy_chunk(format!("{artifact}\n{script}\n").as_bytes());
        let events = b.parse_chunk(&chunk);
        assert!(events.is_empty(), "got: {events:?}");
    }

    #[test]
    fn cargo_beholder_buffers_partial_lines() {
        let json = r#"{"reason":"build-finished","success":true}"#;
        let half = json.len() / 2;
        let mut b = CargoBeholder::default();
        // First half — no newline yet, no events
        let chunk1 = dummy_chunk(json[..half].as_bytes());
        let ev1 = b.parse_chunk(&chunk1);
        assert!(ev1.is_empty(), "should buffer partial line");
        // Second half + newline — now the event appears
        let chunk2 = dummy_chunk(format!("{}\n", &json[half..]).as_bytes());
        let ev2 = b.parse_chunk(&chunk2);
        assert_eq!(ev2.len(), 1);
        assert_eq!(ev2[0].msg, "build finished");
    }

    // ─── Schema versioning + unknown-format fallback tests ────────────────────

    #[test]
    fn cargo_beholder_unknown_format_reason_none_initially() {
        let b = CargoBeholder::default();
        assert!(b.unknown_format_reason().is_none(), "fresh beholder must not flag unknown format");
    }

    #[test]
    fn cargo_factory_declares_tool_version_range() {
        let f = CargoBeholderFactory;
        let range = f.tool_version_range().expect("cargo factory must declare a version range");
        assert_eq!(range.min, Some("1.38.0"), "min version must be 1.38.0");
        assert!(range.max.is_none(), "no upper bound declared");
    }

    #[test]
    fn cargo_beholder_format_probe_fires_after_limit() {
        // Feed FORMAT_PROBE_LIMIT JSON-object lines that lack a `reason` field.
        let bad_line = r#"{"not_reason":"something","value":42}"#;
        let input: String = (0..super::FORMAT_PROBE_LIMIT)
            .map(|_| format!("{bad_line}\n"))
            .collect();
        let chunk = dummy_chunk(input.as_bytes());
        let mut b = CargoBeholder::default();
        let events = b.parse_chunk(&chunk);
        assert!(events.is_empty(), "no events from unrecognized lines");
        let reason = b.unknown_format_reason();
        assert!(
            reason.is_some(),
            "unknown_format_reason must be Some after {FORMAT_PROBE_LIMIT} unrecognized JSON lines"
        );
        assert!(
            reason.unwrap().contains("no recognizable cargo JSON"),
            "reason text must describe the problem; got: {:?}",
            reason
        );
    }

    #[test]
    fn cargo_beholder_format_probe_not_triggered_below_limit() {
        // One fewer than the limit — not yet flagged.
        let bad_line = r#"{"not_reason":"something"}"#;
        let input: String = (0..super::FORMAT_PROBE_LIMIT - 1)
            .map(|_| format!("{bad_line}\n"))
            .collect();
        let chunk = dummy_chunk(input.as_bytes());
        let mut b = CargoBeholder::default();
        b.parse_chunk(&chunk);
        assert!(
            b.unknown_format_reason().is_none(),
            "must not flag unknown format before probe limit"
        );
    }

    #[test]
    fn cargo_beholder_format_probe_suppressed_after_recognized_line() {
        // One valid cargo line clears the probe; subsequent bad lines are silently dropped.
        let good = r#"{"reason":"build-finished","success":true}"#;
        let bad_line = r#"{"not_reason":"something"}"#;
        // Build more bad lines than the threshold.
        let mut input = format!("{good}\n");
        for _ in 0..super::FORMAT_PROBE_LIMIT + 2 {
            input.push_str(bad_line);
            input.push('\n');
        }
        let chunk = dummy_chunk(input.as_bytes());
        let mut b = CargoBeholder::default();
        let events = b.parse_chunk(&chunk);
        // The good line produces one event.
        assert_eq!(events.len(), 1, "expected one event from the recognized line");
        assert_eq!(events[0].msg, "build finished");
        assert!(
            b.unknown_format_reason().is_none(),
            "format probe must not fire after a recognized line has been seen"
        );
    }

    #[test]
    fn cargo_beholder_stops_emitting_after_unknown_format() {
        // Trigger unknown format, then verify subsequent chunks produce no events.
        let bad_line = r#"{"not_reason":"x"}"#;
        let trigger: String = (0..super::FORMAT_PROBE_LIMIT)
            .map(|_| format!("{bad_line}\n"))
            .collect();
        let mut b = CargoBeholder::default();
        b.parse_chunk(&dummy_chunk(trigger.as_bytes()));
        assert!(b.unknown_format_reason().is_some(), "format must be flagged");

        // Now feed a valid cargo line — must produce no events (beholder silenced).
        let valid = r#"{"reason":"build-finished","success":true}"#.to_string() + "\n";
        let events = b.parse_chunk(&dummy_chunk(valid.as_bytes()));
        assert!(events.is_empty(), "beholder must not emit events after unknown_format is set");
    }

    #[test]
    fn cargo_beholder_non_json_lines_dont_count_toward_probe() {
        // Non-JSON lines (and empty lines) are skipped; they must not consume probe budget.
        let mut input = String::new();
        // More non-JSON lines than FORMAT_PROBE_LIMIT.
        for _ in 0..super::FORMAT_PROBE_LIMIT + 3 {
            input.push_str("  warning: some human text\n");
        }
        // Then a valid cargo line.
        input.push_str(r#"{"reason":"build-finished","success":true}"#);
        input.push('\n');
        let mut b = CargoBeholder::default();
        let events = b.parse_chunk(&dummy_chunk(input.as_bytes()));
        assert_eq!(events.len(), 1, "valid cargo line must still produce an event");
        assert!(b.unknown_format_reason().is_none(), "non-JSON lines must not count toward probe");
    }

    // ─── TscBeholderFactory tests ─────────────────────────────────────────────

    #[test]
    fn tsc_factory_matches_bare_tsc() {
        let f = TscBeholderFactory;
        let argv: Vec<String> = vec!["tsc".into(), "--noEmit".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn tsc_factory_matches_tsc_with_project() {
        let f = TscBeholderFactory;
        let argv: Vec<String> = vec!["tsc".into(), "-p".into(), "tsconfig.json".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn tsc_factory_matches_when_pretty_false_already_set() {
        // Should still attach (rewriter becomes a no-op; we still parse).
        let f = TscBeholderFactory;
        let argv: Vec<String> = vec!["tsc".into(), "--noEmit".into(), "--pretty=false".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn tsc_factory_declines_pretty_true() {
        let f = TscBeholderFactory;
        let argv: Vec<String> = vec!["tsc".into(), "--pretty=true".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn tsc_factory_declines_bare_pretty() {
        let f = TscBeholderFactory;
        let argv: Vec<String> = vec!["tsc".into(), "--pretty".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn tsc_factory_declines_version() {
        let f = TscBeholderFactory;
        let argv: Vec<String> = vec!["tsc".into(), "--version".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn tsc_factory_declines_init() {
        let f = TscBeholderFactory;
        let argv: Vec<String> = vec!["tsc".into(), "--init".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn tsc_factory_declines_non_tsc_command() {
        let f = TscBeholderFactory;
        let argv: Vec<String> = vec!["node".into(), "build.js".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn tsc_factory_rewriter_adds_pretty_false() {
        let registry = default_registry();
        let result = registry.attach("tsc --noEmit", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some(), "tsc beholder must attach");
        assert!(
            result.argv.contains(&"--pretty=false".to_string()),
            "rewriter must inject --pretty=false; got: {:?}",
            result.argv
        );
        assert!(
            result.status.text.contains("rewrite="),
            "rewrite must be surfaced in status; got: {}",
            result.status.text
        );
    }

    #[test]
    fn tsc_factory_no_rewrite_when_pretty_false_present() {
        let registry = default_registry();
        let result = registry.attach("tsc --noEmit --pretty=false", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some(), "tsc beholder must attach");
        // Argv already had --pretty=false; rewriter is a no-op, no duplicate added.
        let count = result.argv.iter().filter(|a| a.as_str() == "--pretty=false").count();
        assert_eq!(count, 1, "--pretty=false must appear exactly once; got: {:?}", result.argv);
        // No rewrite_added because the flag was already present.
        assert!(
            result.status.rewrite_added.as_ref().map(|v| v.is_empty()).unwrap_or(true),
            "rewrite_added must be empty when flag already present"
        );
    }

    #[test]
    fn tsc_beholder_parses_error_line() {
        let line = "src/foo.ts(10,5): error TS2345: Argument of type 'string' is not assignable to parameter of type 'number'.\n";
        let mut b = TscBeholder::default();
        let events = b.parse_chunk(&dummy_chunk(line.as_bytes()));
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.target, "tsc");
        assert_eq!(ev.level, Level::Error);
        assert_eq!(ev.msg, "Argument of type 'string' is not assignable to parameter of type 'number'.");
        assert_eq!(ev.fields["error"]["code"], "TS2345");
        assert_eq!(ev.fields["file"]["path"], "src/foo.ts");
        assert_eq!(ev.fields["file"]["line"], 10u32);
        assert_eq!(ev.fields["file"]["col"], 5u32);
    }

    #[test]
    fn tsc_beholder_parses_warning_line() {
        let line = "src/bar.tsx(42,3): warning TS6133: 'x' is declared but its value is never read.\n";
        let mut b = TscBeholder::default();
        let events = b.parse_chunk(&dummy_chunk(line.as_bytes()));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, Level::Warn);
        assert_eq!(events[0].fields["error"]["code"], "TS6133");
        assert_eq!(events[0].fields["file"]["path"], "src/bar.tsx");
        assert_eq!(events[0].fields["file"]["line"], 42u32);
        assert_eq!(events[0].fields["file"]["col"], 3u32);
    }

    #[test]
    fn tsc_beholder_parses_summary_with_errors() {
        let line = "Found 2 errors.\n";
        let mut b = TscBeholder::default();
        let events = b.parse_chunk(&dummy_chunk(line.as_bytes()));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, Level::Error);
        assert_eq!(events[0].fields["build"]["errors"], 2u32);
    }

    #[test]
    fn tsc_beholder_parses_summary_zero_errors() {
        let line = "Found 0 errors.\n";
        let mut b = TscBeholder::default();
        let events = b.parse_chunk(&dummy_chunk(line.as_bytes()));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, Level::Info);
        assert_eq!(events[0].fields["build"]["errors"], 0u32);
    }

    #[test]
    fn tsc_beholder_parses_summary_in_n_files() {
        let line = "Found 3 errors in 2 files.\n";
        let mut b = TscBeholder::default();
        let events = b.parse_chunk(&dummy_chunk(line.as_bytes()));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].fields["build"]["errors"], 3u32);
    }

    #[test]
    fn tsc_beholder_skips_watch_mode_header() {
        let line = "[12:00:00 AM] Starting compilation in watch mode...\n";
        let mut b = TscBeholder::default();
        let events = b.parse_chunk(&dummy_chunk(line.as_bytes()));
        assert!(events.is_empty(), "watch-mode header must be skipped");
    }

    #[test]
    fn tsc_beholder_skips_blank_lines() {
        let input = "\n\n";
        let mut b = TscBeholder::default();
        let events = b.parse_chunk(&dummy_chunk(input.as_bytes()));
        assert!(events.is_empty());
    }

    #[test]
    fn tsc_beholder_buffers_partial_lines() {
        let line = "src/foo.ts(1,1): error TS2304: Cannot find name 'foo'.";
        let half = line.len() / 2;
        let mut b = TscBeholder::default();
        let ev1 = b.parse_chunk(&dummy_chunk(line[..half].as_bytes()));
        assert!(ev1.is_empty(), "partial line must not produce events");
        let ev2 = b.parse_chunk(&dummy_chunk(format!("{}\n", &line[half..]).as_bytes()));
        assert_eq!(ev2.len(), 1, "complete line must produce one event");
        assert_eq!(ev2[0].fields["error"]["code"], "TS2304");
    }

    #[test]
    fn tsc_beholder_multiple_errors_in_one_chunk() {
        let input = concat!(
            "src/a.ts(1,1): error TS2304: Cannot find name 'a'.\n",
            "src/b.ts(2,3): error TS2304: Cannot find name 'b'.\n",
            "Found 2 errors.\n",
        );
        let mut b = TscBeholder::default();
        let events = b.parse_chunk(&dummy_chunk(input.as_bytes()));
        assert_eq!(events.len(), 3, "two diagnostics + one summary");
        assert_eq!(events[0].fields["file"]["path"], "src/a.ts");
        assert_eq!(events[1].fields["file"]["path"], "src/b.ts");
        assert_eq!(events[2].fields["build"]["errors"], 2u32);
    }

    #[test]
    fn tsc_beholder_full_watch_cycle() {
        let input = concat!(
            "[12:00:00 AM] Starting compilation in watch mode...\n",
            "\n",
            "src/foo.ts(5,9): error TS2345: wrong type.\n",
            "\n",
            "[12:00:01 AM] Found 1 error. Watching for file changes.\n",
        );
        let mut b = TscBeholder::default();
        let events = b.parse_chunk(&dummy_chunk(input.as_bytes()));
        // Only the diagnostic line produces an event; watch headers are skipped.
        assert_eq!(events.len(), 1, "only diagnostic line should produce event; got: {events:?}");
        assert_eq!(events[0].level, Level::Error);
        assert_eq!(events[0].fields["file"]["path"], "src/foo.ts");
    }

    #[test]
    fn tsc_factory_declares_tool_version_range() {
        let f = TscBeholderFactory;
        let range = f.tool_version_range().expect("tsc factory must declare a version range");
        assert_eq!(range.min, Some("3.0.0"));
        assert!(range.max.is_none());
    }

    // ─── EslintBeholderFactory tests ─────────────────────────────────────────

    #[test]
    fn eslint_factory_matches_bare_eslint() {
        let f = EslintBeholderFactory;
        let argv: Vec<String> = vec!["eslint".into(), "src/".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn eslint_factory_matches_when_format_json_already_set() {
        let f = EslintBeholderFactory;
        let argv: Vec<String> = vec!["eslint".into(), "--format=json".into(), "src/".into()];
        assert!(f.matches(&argv), "must match when --format=json already present");
    }

    #[test]
    fn eslint_factory_declines_non_json_format() {
        let f = EslintBeholderFactory;
        let argv: Vec<String> = vec!["eslint".into(), "--format=compact".into(), "src/".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn eslint_factory_declines_version() {
        let f = EslintBeholderFactory;
        let argv: Vec<String> = vec!["eslint".into(), "--version".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn eslint_factory_declines_env_info() {
        let f = EslintBeholderFactory;
        let argv: Vec<String> = vec!["eslint".into(), "--env-info".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn eslint_factory_declines_non_eslint_command() {
        let f = EslintBeholderFactory;
        let argv: Vec<String> = vec!["tsc".into(), "--noEmit".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn eslint_factory_rewriter_adds_format_json() {
        let registry = default_registry();
        let result = registry.attach("eslint src/", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some(), "eslint beholder must attach");
        assert!(
            result.argv.contains(&"--format=json".to_string()),
            "rewriter must inject --format=json; got: {:?}",
            result.argv
        );
        assert!(
            result.status.text.contains("rewrite="),
            "rewrite must be surfaced in status; got: {}",
            result.status.text
        );
    }

    #[test]
    fn eslint_factory_no_rewrite_when_format_json_present() {
        let registry = default_registry();
        let result = registry.attach("eslint --format=json src/", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some(), "eslint beholder must attach");
        let count = result.argv.iter().filter(|a| a.as_str() == "--format=json").count();
        assert_eq!(count, 1, "--format=json must appear exactly once; got: {:?}", result.argv);
    }

    #[test]
    fn eslint_beholder_parse_chunk_buffers_only() {
        let json = r#"[{"filePath":"/src/a.js","messages":[]}]"#;
        let mut b = EslintBeholder::default();
        let events = b.parse_chunk(&dummy_chunk(json.as_bytes()));
        assert!(events.is_empty(), "parse_chunk must not emit events; ESLint parses at EOF");
    }

    #[test]
    fn eslint_beholder_on_done_parses_errors_and_warnings() {
        use crate::types::TaskRunId;
        let json = r#"[
            {
                "filePath": "/src/foo.ts",
                "messages": [
                    {
                        "ruleId": "no-unused-vars",
                        "severity": 2,
                        "message": "'x' is defined but never used.",
                        "line": 10,
                        "column": 5
                    },
                    {
                        "ruleId": "no-console",
                        "severity": 1,
                        "message": "Unexpected console statement.",
                        "line": 20,
                        "column": 1
                    }
                ],
                "errorCount": 1,
                "warningCount": 1
            }
        ]"#;
        let mut b = EslintBeholder::default();
        b.parse_chunk(&dummy_chunk(json.as_bytes()));
        let run_id = TaskRunId::new();
        let events = b.on_done(&run_id, 0);
        assert_eq!(events.len(), 2);
        let err = &events[0];
        assert_eq!(err.level, Level::Error);
        assert_eq!(err.target, "eslint");
        assert_eq!(err.msg, "'x' is defined but never used.");
        assert_eq!(err.fields["error"]["code"], "no-unused-vars");
        assert_eq!(err.fields["file"]["path"], "/src/foo.ts");
        assert_eq!(err.fields["file"]["line"], 10u32);
        assert_eq!(err.fields["file"]["col"], 5u32);
        let warn = &events[1];
        assert_eq!(warn.level, Level::Warn);
        assert_eq!(warn.fields["error"]["code"], "no-console");
    }

    #[test]
    fn eslint_beholder_on_done_empty_output() {
        use crate::types::TaskRunId;
        let mut b = EslintBeholder::default();
        let events = b.on_done(&TaskRunId::new(), 0);
        assert!(events.is_empty(), "empty buffer must produce no events");
    }

    #[test]
    fn eslint_beholder_on_done_invalid_json_flags_unknown_format() {
        use crate::types::TaskRunId;
        let bad = b"not json at all";
        let mut b = EslintBeholder::default();
        b.parse_chunk(&dummy_chunk(bad));
        let events = b.on_done(&TaskRunId::new(), 0);
        assert!(events.is_empty(), "bad JSON must not produce events");
        assert!(
            b.unknown_format_reason().is_some(),
            "bad JSON must set unknown_format_reason"
        );
    }

    #[test]
    fn eslint_beholder_on_done_chunks_split_across_multiple_calls() {
        use crate::types::TaskRunId;
        let json = r#"[{"filePath":"/a.ts","messages":[{"ruleId":"eqeqeq","severity":2,"message":"Use ===.","line":3,"column":7}],"errorCount":1,"warningCount":0}]"#;
        let mid = json.len() / 2;
        let mut b = EslintBeholder::default();
        b.parse_chunk(&dummy_chunk(json[..mid].as_bytes()));
        b.parse_chunk(&dummy_chunk(json[mid..].as_bytes()));
        let events = b.on_done(&TaskRunId::new(), 0);
        assert_eq!(events.len(), 1, "split-chunk delivery must still parse correctly");
        assert_eq!(events[0].fields["error"]["code"], "eqeqeq");
    }

    #[test]
    fn eslint_factory_declares_tool_version_range() {
        let f = EslintBeholderFactory;
        let range = f.tool_version_range().expect("eslint factory must declare a version range");
        assert_eq!(range.min, Some("8.0.0"));
        assert!(range.max.is_none());
    }

    // ─── BiomeBeholderFactory tests ───────────────────────────────────────────

    #[test]
    fn biome_factory_matches_check() {
        let f = BiomeBeholderFactory;
        let argv: Vec<String> = vec!["biome".into(), "check".into(), "src/".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn biome_factory_matches_lint() {
        let f = BiomeBeholderFactory;
        let argv: Vec<String> = vec!["biome".into(), "lint".into(), "src/".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn biome_factory_matches_ci() {
        let f = BiomeBeholderFactory;
        let argv: Vec<String> = vec!["biome".into(), "ci".into(), "src/".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn biome_factory_declines_format_subcommand() {
        let f = BiomeBeholderFactory;
        let argv: Vec<String> = vec!["biome".into(), "format".into(), "src/".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn biome_factory_declines_version() {
        let f = BiomeBeholderFactory;
        let argv: Vec<String> = vec!["biome".into(), "--version".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn biome_factory_declines_non_json_reporter() {
        let f = BiomeBeholderFactory;
        let argv: Vec<String> = vec!["biome".into(), "check".into(), "--reporter=github".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn biome_factory_matches_when_reporter_json_present() {
        let f = BiomeBeholderFactory;
        let argv: Vec<String> = vec!["biome".into(), "check".into(), "--reporter=json".into()];
        assert!(f.matches(&argv), "must match when --reporter=json already set");
    }

    #[test]
    fn biome_factory_declines_non_biome_command() {
        let f = BiomeBeholderFactory;
        let argv: Vec<String> = vec!["eslint".into(), "src/".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn biome_factory_rewriter_adds_reporter_json() {
        let registry = default_registry();
        let result = registry.attach("biome check src/", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some(), "biome beholder must attach");
        assert!(
            result.argv.contains(&"--reporter=json".to_string()),
            "rewriter must inject --reporter=json; got: {:?}",
            result.argv
        );
        assert!(
            result.status.text.contains("rewrite="),
            "rewrite must be surfaced in status; got: {}",
            result.status.text
        );
    }

    #[test]
    fn biome_factory_no_rewrite_when_reporter_json_present() {
        let registry = default_registry();
        let result = registry.attach("biome lint --reporter=json src/", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some(), "biome beholder must attach");
        let count = result.argv.iter().filter(|a| a.as_str() == "--reporter=json").count();
        assert_eq!(count, 1, "--reporter=json must appear exactly once; got: {:?}", result.argv);
    }

    #[test]
    fn biome_beholder_parse_chunk_buffers_only() {
        let json = r#"{"diagnostics":[],"summary":{}}"#;
        let mut b = BiomeBeholder::default();
        let events = b.parse_chunk(&dummy_chunk(json.as_bytes()));
        assert!(events.is_empty(), "parse_chunk must not emit events; Biome parses at EOF");
    }

    #[test]
    fn biome_beholder_on_done_parses_error() {
        use crate::types::TaskRunId;
        let json = r#"{
            "diagnostics": [
                {
                    "category": "lint/suspicious/noDoubleEquals",
                    "severity": "error",
                    "description": "Use === instead of ==",
                    "location": {
                        "path": {"file": "src/foo.ts"}
                    }
                }
            ],
            "summary": {"changed": 0, "unchanged": 1, "errors": 1}
        }"#;
        let mut b = BiomeBeholder::default();
        b.parse_chunk(&dummy_chunk(json.as_bytes()));
        let events = b.on_done(&TaskRunId::new(), 0);
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.level, Level::Error);
        assert_eq!(ev.target, "biome");
        assert_eq!(ev.msg, "Use === instead of ==");
        assert_eq!(ev.fields["error"]["code"], "lint/suspicious/noDoubleEquals");
        assert_eq!(ev.fields["file"]["path"], "src/foo.ts");
    }

    #[test]
    fn biome_beholder_on_done_parses_warning() {
        use crate::types::TaskRunId;
        let json = r#"{
            "diagnostics": [
                {
                    "category": "lint/style/useConst",
                    "severity": "warning",
                    "description": "Prefer const over let.",
                    "location": {"path": {"file": "src/bar.ts"}}
                }
            ],
            "summary": {}
        }"#;
        let mut b = BiomeBeholder::default();
        b.parse_chunk(&dummy_chunk(json.as_bytes()));
        let events = b.on_done(&TaskRunId::new(), 0);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, Level::Warn);
        assert_eq!(events[0].fields["file"]["path"], "src/bar.ts");
    }

    #[test]
    fn biome_beholder_on_done_empty_diagnostics() {
        use crate::types::TaskRunId;
        let json = r#"{"diagnostics":[],"summary":{"changed":0,"unchanged":5,"errors":0}}"#;
        let mut b = BiomeBeholder::default();
        b.parse_chunk(&dummy_chunk(json.as_bytes()));
        let events = b.on_done(&TaskRunId::new(), 0);
        assert!(events.is_empty(), "zero diagnostics must produce no events");
    }

    #[test]
    fn biome_beholder_on_done_invalid_json_flags_unknown_format() {
        use crate::types::TaskRunId;
        let mut b = BiomeBeholder::default();
        b.parse_chunk(&dummy_chunk(b"not json"));
        let events = b.on_done(&TaskRunId::new(), 0);
        assert!(events.is_empty());
        assert!(b.unknown_format_reason().is_some());
    }

    #[test]
    fn biome_factory_declares_tool_version_range() {
        let f = BiomeBeholderFactory;
        let range = f.tool_version_range().expect("biome factory must declare a version range");
        assert_eq!(range.min, Some("1.0.0"));
        assert!(range.max.is_none());
    }

    // ─── VitestBeholderFactory tests ──────────────────────────────────────────

    #[test]
    fn vitest_factory_matches_bare_vitest() {
        let f = VitestBeholderFactory;
        let argv: Vec<String> = vec!["vitest".into(), "--run".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn vitest_factory_matches_with_reporter_json_already_set() {
        let f = VitestBeholderFactory;
        let argv: Vec<String> = vec!["vitest".into(), "--reporter=json".into()];
        assert!(f.matches(&argv), "must match when --reporter=json already present");
    }

    #[test]
    fn vitest_factory_declines_non_json_reporter_eq() {
        let f = VitestBeholderFactory;
        let argv: Vec<String> = vec!["vitest".into(), "--reporter=verbose".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn vitest_factory_declines_non_json_reporter_space() {
        let f = VitestBeholderFactory;
        let argv: Vec<String> = vec!["vitest".into(), "--reporter".into(), "verbose".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn vitest_factory_declines_version() {
        let f = VitestBeholderFactory;
        let argv: Vec<String> = vec!["vitest".into(), "--version".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn vitest_factory_declines_non_vitest_command() {
        let f = VitestBeholderFactory;
        let argv: Vec<String> = vec!["jest".into(), "--run".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn vitest_factory_rewriter_adds_reporter_json() {
        let registry = default_registry();
        let result = registry.attach("vitest --run", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some(), "vitest beholder must attach");
        assert!(
            result.argv.contains(&"--reporter=json".to_string()),
            "rewriter must inject --reporter=json; got: {:?}",
            result.argv
        );
        assert!(
            result.status.text.contains("rewrite="),
            "rewrite must be surfaced in status; got: {}",
            result.status.text
        );
    }

    #[test]
    fn vitest_factory_no_rewrite_when_reporter_json_present() {
        let registry = default_registry();
        let result = registry.attach("vitest --reporter=json --run", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some());
        let count = result.argv.iter().filter(|a| a.as_str() == "--reporter=json").count();
        assert_eq!(count, 1, "--reporter=json must appear exactly once; got: {:?}", result.argv);
    }

    #[test]
    fn vitest_factory_declares_tool_version_range() {
        let f = VitestBeholderFactory;
        let range = f.tool_version_range().expect("vitest factory must declare a version range");
        assert_eq!(range.min, Some("1.0.0"));
        assert!(range.max.is_none());
    }

    // ─── JestBeholderFactory tests ────────────────────────────────────────────

    #[test]
    fn jest_factory_matches_bare_jest() {
        let f = JestBeholderFactory;
        let argv: Vec<String> = vec!["jest".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn jest_factory_matches_jest_with_path() {
        let f = JestBeholderFactory;
        let argv: Vec<String> = vec!["jest".into(), "src/foo.test.ts".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn jest_factory_matches_when_json_already_set() {
        let f = JestBeholderFactory;
        let argv: Vec<String> = vec!["jest".into(), "--json".into()];
        assert!(f.matches(&argv), "must match when --json already present");
    }

    #[test]
    fn jest_factory_declines_version() {
        let f = JestBeholderFactory;
        let argv: Vec<String> = vec!["jest".into(), "--version".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn jest_factory_declines_output_file() {
        let f = JestBeholderFactory;
        let argv: Vec<String> = vec!["jest".into(), "--outputFile=results.json".into()];
        assert!(!f.matches(&argv), "must decline when --outputFile is set (JSON goes to file)");
    }

    #[test]
    fn jest_factory_declines_non_jest_command() {
        let f = JestBeholderFactory;
        let argv: Vec<String> = vec!["vitest".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn jest_factory_rewriter_adds_json_flag() {
        let registry = default_registry();
        let result = registry.attach("jest src/", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some(), "jest beholder must attach");
        assert!(
            result.argv.contains(&"--json".to_string()),
            "rewriter must inject --json; got: {:?}",
            result.argv
        );
        assert!(
            result.status.text.contains("rewrite="),
            "rewrite must be surfaced in status; got: {}",
            result.status.text
        );
    }

    #[test]
    fn jest_factory_no_rewrite_when_json_present() {
        let registry = default_registry();
        let result = registry.attach("jest --json src/", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some());
        let count = result.argv.iter().filter(|a| a.as_str() == "--json").count();
        assert_eq!(count, 1, "--json must appear exactly once; got: {:?}", result.argv);
    }

    #[test]
    fn jest_factory_declares_tool_version_range() {
        let f = JestBeholderFactory;
        let range = f.tool_version_range().expect("jest factory must declare a version range");
        assert_eq!(range.min, Some("27.0.0"));
        assert!(range.max.is_none());
    }

    // ─── BunTestBeholderFactory tests ─────────────────────────────────────────

    #[test]
    fn bun_test_factory_matches_bun_test() {
        let f = BunTestBeholderFactory;
        let argv: Vec<String> = vec!["bun".into(), "test".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn bun_test_factory_matches_bun_test_with_path() {
        let f = BunTestBeholderFactory;
        let argv: Vec<String> = vec!["bun".into(), "test".into(), "src/".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn bun_test_factory_matches_when_reporter_json_already_set() {
        let f = BunTestBeholderFactory;
        let argv: Vec<String> = vec!["bun".into(), "test".into(), "--reporter=json".into()];
        assert!(f.matches(&argv));
    }

    #[test]
    fn bun_test_factory_declines_bare_bun() {
        let f = BunTestBeholderFactory;
        let argv: Vec<String> = vec!["bun".into(), "run".into(), "build.ts".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn bun_test_factory_declines_non_json_reporter() {
        let f = BunTestBeholderFactory;
        let argv: Vec<String> = vec!["bun".into(), "test".into(), "--reporter=junit".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn bun_test_factory_declines_version() {
        let f = BunTestBeholderFactory;
        let argv: Vec<String> = vec!["bun".into(), "test".into(), "--version".into()];
        assert!(!f.matches(&argv));
    }

    #[test]
    fn bun_test_factory_rewriter_adds_reporter_json() {
        let registry = default_registry();
        let result = registry.attach("bun test", &BeholderSelect::Auto, false);
        assert!(result.beholder.is_some(), "bun-test beholder must attach");
        assert!(
            result.argv.contains(&"--reporter=json".to_string()),
            "rewriter must inject --reporter=json; got: {:?}",
            result.argv
        );
    }

    #[test]
    fn bun_test_factory_declares_tool_version_range() {
        let f = BunTestBeholderFactory;
        let range = f.tool_version_range().expect("bun-test factory must declare a version range");
        assert_eq!(range.min, Some("1.0.0"));
        assert!(range.max.is_none());
    }

    // ─── JsTestBeholder on_done tests ─────────────────────────────────────────

    fn js_test_report_json(success: bool, passed: u32, failed: u32) -> String {
        format!(
            r#"{{
                "success": {success},
                "numTotalTests": {total},
                "numPassedTests": {passed},
                "numFailedTests": {failed},
                "numPendingTests": 0,
                "testResults": []
            }}"#,
            total = passed + failed,
        )
    }

    #[test]
    fn js_test_beholder_on_done_success_summary() {
        use crate::types::TaskRunId;
        let json = js_test_report_json(true, 5, 0);
        let mut b = JsTestBeholder::new("vitest", "1.0", vitest_adjust_argv);
        b.parse_chunk(&dummy_chunk(json.as_bytes()));
        let events = b.on_done(&TaskRunId::new(), 0);
        assert_eq!(events.len(), 1, "success run must produce one summary event");
        let ev = &events[0];
        assert_eq!(ev.target, "vitest");
        assert_eq!(ev.level, Level::Info);
        assert_eq!(ev.msg, "5 passed");
        assert_eq!(ev.fields["build"]["success"], true);
        assert_eq!(ev.fields["test"]["passed"], 5u32);
        assert_eq!(ev.fields["test"]["failed"], 0u32);
    }

    #[test]
    fn js_test_beholder_on_done_failure_summary() {
        use crate::types::TaskRunId;
        let json = js_test_report_json(false, 3, 2);
        let mut b = JsTestBeholder::new("jest", "27.0", jest_adjust_argv);
        b.parse_chunk(&dummy_chunk(json.as_bytes()));
        let events = b.on_done(&TaskRunId::new(), 0);
        assert_eq!(events.len(), 1, "no assertion results → only summary event");
        let ev = &events[0];
        assert_eq!(ev.target, "jest");
        assert_eq!(ev.level, Level::Error);
        assert_eq!(ev.msg, "2 failed, 3 passed");
    }

    #[test]
    fn js_test_beholder_on_done_emits_failed_assertion_events() {
        use crate::types::TaskRunId;
        let json = r#"{
            "success": false,
            "numTotalTests": 2,
            "numPassedTests": 1,
            "numFailedTests": 1,
            "numPendingTests": 0,
            "testResults": [
                {
                    "testFilePath": "src/foo.test.ts",
                    "assertionResults": [
                        {
                            "fullName": "suite > passes",
                            "status": "passed",
                            "failureMessages": []
                        },
                        {
                            "fullName": "suite > fails",
                            "status": "failed",
                            "failureMessages": ["Error: expected 1 to equal 2\n  at foo (src/foo.test.ts:10)"]
                        }
                    ]
                }
            ]
        }"#;
        let mut b = JsTestBeholder::new("vitest", "1.0", vitest_adjust_argv);
        b.parse_chunk(&dummy_chunk(json.as_bytes()));
        let run_id = TaskRunId::new();
        let events = b.on_done(&run_id, 0);
        // summary + one failure event (passed assertion is skipped)
        assert_eq!(events.len(), 2, "expected summary + one failure event; got: {events:?}");
        let failure = &events[1];
        assert_eq!(failure.level, Level::Error);
        assert_eq!(failure.target, "vitest::test");
        assert_eq!(failure.msg, "Error: expected 1 to equal 2");
        assert_eq!(failure.fields["test"]["name"], "suite > fails");
        assert_eq!(failure.fields["file"]["path"], "src/foo.test.ts");
    }

    #[test]
    fn js_test_beholder_on_done_multiple_suites_and_failures() {
        use crate::types::TaskRunId;
        let json = r#"{
            "success": false,
            "numTotalTests": 4,
            "numPassedTests": 2,
            "numFailedTests": 2,
            "numPendingTests": 0,
            "testResults": [
                {
                    "testFilePath": "src/a.test.ts",
                    "assertionResults": [
                        { "fullName": "A passes", "status": "passed", "failureMessages": [] },
                        { "fullName": "A fails", "status": "failed", "failureMessages": ["err A"] }
                    ]
                },
                {
                    "testFilePath": "src/b.test.ts",
                    "assertionResults": [
                        { "fullName": "B passes", "status": "passed", "failureMessages": [] },
                        { "fullName": "B fails", "status": "failed", "failureMessages": ["err B"] }
                    ]
                }
            ]
        }"#;
        let mut b = JsTestBeholder::new("vitest", "1.0", vitest_adjust_argv);
        b.parse_chunk(&dummy_chunk(json.as_bytes()));
        let events = b.on_done(&TaskRunId::new(), 0);
        // summary + 2 failure events
        assert_eq!(events.len(), 3);
        assert_eq!(events[1].fields["file"]["path"], "src/a.test.ts");
        assert_eq!(events[2].fields["file"]["path"], "src/b.test.ts");
    }

    #[test]
    fn js_test_beholder_on_done_empty_buffer_returns_no_events() {
        use crate::types::TaskRunId;
        let b = JsTestBeholder::new("vitest", "1.0", vitest_adjust_argv);
        // Do not call parse_chunk — buffer is empty.
        let mut b = b;
        let events = b.on_done(&TaskRunId::new(), 0);
        assert!(events.is_empty());
    }

    #[test]
    fn js_test_beholder_on_done_invalid_json_flags_unknown_format() {
        use crate::types::TaskRunId;
        let mut b = JsTestBeholder::new("vitest", "1.0", vitest_adjust_argv);
        b.parse_chunk(&dummy_chunk(b"not json"));
        let events = b.on_done(&TaskRunId::new(), 0);
        assert!(events.is_empty());
        assert!(b.unknown_format_reason().is_some());
    }

    #[test]
    fn js_test_beholder_on_done_chunks_split_across_calls() {
        use crate::types::TaskRunId;
        let json = r#"{"success":true,"numTotalTests":1,"numPassedTests":1,"numFailedTests":0,"numPendingTests":0,"testResults":[]}"#;
        let mid = json.len() / 2;
        let mut b = JsTestBeholder::new("jest", "27.0", jest_adjust_argv);
        b.parse_chunk(&dummy_chunk(json[..mid].as_bytes()));
        b.parse_chunk(&dummy_chunk(json[mid..].as_bytes()));
        let events = b.on_done(&TaskRunId::new(), 0);
        assert_eq!(events.len(), 1, "split chunks must still parse correctly");
        assert_eq!(events[0].level, Level::Info);
    }

    #[test]
    fn js_test_beholder_failure_msg_uses_first_line_only() {
        use crate::types::TaskRunId;
        let json = r#"{
            "success": false,
            "numTotalTests": 1, "numPassedTests": 0, "numFailedTests": 1, "numPendingTests": 0,
            "testResults": [{
                "testFilePath": "x.test.ts",
                "assertionResults": [{
                    "fullName": "fails",
                    "status": "failed",
                    "failureMessages": ["Error: oops\n  at Object.<anonymous> (x.test.ts:5:5)\n  at ...]"]
                }]
            }]
        }"#;
        let mut b = JsTestBeholder::new("vitest", "1.0", vitest_adjust_argv);
        b.parse_chunk(&dummy_chunk(json.as_bytes()));
        let events = b.on_done(&TaskRunId::new(), 0);
        assert_eq!(events.len(), 2);
        // Failure event msg must be the first line only, not the full stack trace.
        assert_eq!(events[1].msg, "Error: oops");
    }

    // ─── PytestBeholderFactory tests ──────────────────────────────────────────

    fn args(s: &str) -> Vec<String> { s.split_whitespace().map(str::to_owned).collect() }

    #[test]
    fn pytest_factory_matches_direct() {
        let f = PytestBeholderFactory;
        assert!(f.matches(&args("pytest")));
        assert!(f.matches(&args("pytest tests/")));
        assert!(f.matches(&args("py.test -v")));
    }

    #[test]
    fn pytest_factory_matches_python_m_pytest() {
        let f = PytestBeholderFactory;
        assert!(f.matches(&args("python -m pytest")));
        assert!(f.matches(&args("python3 -m pytest tests/")));
    }

    #[test]
    fn pytest_factory_declines_version_and_help() {
        let f = PytestBeholderFactory;
        assert!(!f.matches(&args("pytest --version")));
        assert!(!f.matches(&args("pytest -V")));
        assert!(!f.matches(&args("pytest --help")));
        assert!(!f.matches(&args("pytest -h")));
    }

    #[test]
    fn pytest_factory_declines_collect_only() {
        let f = PytestBeholderFactory;
        assert!(!f.matches(&args("pytest --collect-only")));
        assert!(!f.matches(&args("pytest --co")));
    }

    #[test]
    fn pytest_factory_declines_non_pytest() {
        let f = PytestBeholderFactory;
        assert!(!f.matches(&args("cargo test")));
        assert!(!f.matches(&args("python script.py")));
        assert!(!f.matches(&args("python -m flask run")));
    }

    #[test]
    fn pytest_factory_mode_is_parser() {
        assert!(matches!(PytestBeholderFactory.mode(), BeholderMode::Parser));
    }

    // ─── PytestBeholder parse tests ───────────────────────────────────────────

    fn pytest_output_chunk(lines: &str, seq: u32) -> OutputChunk {
        use crate::types::TaskRunId;
        OutputChunk {
            run_id: TaskRunId::new(),
            seq,
            offset_ms: 0,
            stream: crate::types::Stream::Stdout,
            bytes: lines.as_bytes().to_vec(),
        }
    }

    #[test]
    fn pytest_beholder_parses_failure_lines() {
        let mut b = PytestBeholder::default();
        let chunk = pytest_output_chunk(
            "FAILED tests/test_foo.py::test_bar - AssertionError: assert 1 == 2\n\
             FAILED tests/test_baz.py::test_qux - ZeroDivisionError: division by zero\n",
            1,
        );
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].level, Level::Error);
        assert_eq!(events[0].target, "pytest::test");
        assert_eq!(events[0].msg, "AssertionError: assert 1 == 2");
        assert_eq!(events[0].fields["test"]["name"], "test_bar");
        assert_eq!(events[0].fields["file"]["path"], "tests/test_foo.py");
        assert_eq!(events[1].msg, "ZeroDivisionError: division by zero");
    }

    #[test]
    fn pytest_beholder_parses_error_lines() {
        let mut b = PytestBeholder::default();
        let chunk = pytest_output_chunk(
            "ERROR tests/test_broken.py - ImportError: No module named 'foo'\n",
            1,
        );
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].msg, "ImportError: No module named 'foo'");
        assert_eq!(events[0].fields["file"]["path"], "tests/test_broken.py");
    }

    #[test]
    fn pytest_beholder_failure_without_reason() {
        let mut b = PytestBeholder::default();
        let chunk = pytest_output_chunk("FAILED tests/test_foo.py::test_bar\n", 1);
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].msg, "FAILED tests/test_foo.py::test_bar");
    }

    #[test]
    fn pytest_beholder_ignores_lines_without_py() {
        let mut b = PytestBeholder::default();
        // These should not produce events — no `.py` in the nodeid.
        let chunk = pytest_output_chunk(
            "FAILED some_other_thing\nERROR not_a_module\n",
            1,
        );
        let events = b.parse_chunk(&chunk);
        assert!(events.is_empty());
    }

    #[test]
    fn pytest_beholder_on_done_success_summary() {
        use crate::types::TaskRunId;
        let mut b = PytestBeholder::default();
        let chunk = pytest_output_chunk(
            "collected 5 items\n\
             ========================= 5 passed in 0.85s ==========================\n",
            1,
        );
        b.parse_chunk(&chunk);
        let events = b.on_done(&TaskRunId::new(), 0);
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.level, Level::Info);
        assert_eq!(ev.target, "pytest");
        assert_eq!(ev.msg, "5 passed");
        assert_eq!(ev.fields["test"]["passed"], 5u32);
        assert_eq!(ev.fields["test"]["failed"], 0u32);
        assert_eq!(ev.fields["build"]["success"], true);
    }

    #[test]
    fn pytest_beholder_on_done_failure_summary() {
        use crate::types::TaskRunId;
        let mut b = PytestBeholder::default();
        let chunk = pytest_output_chunk(
            "FAILED tests/a.py::t1 - err\n\
             ======= 1 failed, 4 passed in 1.23s =======\n",
            1,
        );
        b.parse_chunk(&chunk);
        let events = b.on_done(&TaskRunId::new(), 0);
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.level, Level::Error);
        assert_eq!(ev.msg, "1 failed, 4 passed");
        assert_eq!(ev.fields["test"]["failed"], 1u32);
        assert_eq!(ev.fields["test"]["passed"], 4u32);
        assert_eq!(ev.fields["build"]["success"], false);
    }

    #[test]
    fn pytest_beholder_on_done_mixed_summary() {
        use crate::types::TaskRunId;
        let mut b = PytestBeholder::default();
        let chunk = pytest_output_chunk(
            "====== 2 failed, 1 error, 5 passed, 1 skipped in 3.14s ======\n",
            1,
        );
        b.parse_chunk(&chunk);
        let events = b.on_done(&TaskRunId::new(), 0);
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.level, Level::Error);
        assert_eq!(ev.msg, "2 failed, 1 error, 5 passed, 1 skipped");
        assert_eq!(ev.fields["test"]["failed"], 2u32);
        assert_eq!(ev.fields["test"]["errors"], 1u32);
        assert_eq!(ev.fields["test"]["passed"], 5u32);
        assert_eq!(ev.fields["test"]["skipped"], 1u32);
    }

    #[test]
    fn pytest_beholder_on_done_no_summary_returns_empty() {
        use crate::types::TaskRunId;
        let mut b = PytestBeholder::default();
        let events = b.on_done(&TaskRunId::new(), 0);
        assert!(events.is_empty());
    }

    #[test]
    fn pytest_beholder_chunks_split_across_calls() {
        let line = "FAILED tests/foo.py::bar - AssertionError\n";
        let mid = line.len() / 2;
        let mut b = PytestBeholder::default();
        b.parse_chunk(&pytest_output_chunk(&line[..mid], 1));
        let ev1 = b.parse_chunk(&pytest_output_chunk(&line[mid..], 2));
        // The complete FAILED line should be emitted from the second chunk.
        assert_eq!(ev1.len(), 1);
        assert_eq!(ev1[0].msg, "AssertionError");
    }

    // ─── parse_pytest_summary unit tests ──────────────────────────────────────

    #[test]
    fn parse_pytest_summary_all_passed() {
        let s = parse_pytest_summary("5 passed in 0.85s").unwrap();
        assert_eq!(s.passed, 5);
        assert_eq!(s.failed, 0);
    }

    #[test]
    fn parse_pytest_summary_mixed() {
        let s = parse_pytest_summary("2 failed, 5 passed, 1 skipped in 3.14s").unwrap();
        assert_eq!(s.failed, 2);
        assert_eq!(s.passed, 5);
        assert_eq!(s.skipped, 1);
        assert_eq!(s.errors, 0);
    }

    #[test]
    fn parse_pytest_summary_with_errors() {
        let s = parse_pytest_summary("1 error, 3 passed in 1.0s").unwrap();
        assert_eq!(s.errors, 1);
        assert_eq!(s.passed, 3);
    }

    #[test]
    fn parse_pytest_summary_rejects_garbage() {
        assert!(parse_pytest_summary("not a summary").is_err());
        assert!(parse_pytest_summary("").is_err());
    }

    // ─── ViteBuildBeholderFactory tests ───────────────────────────────────────

    #[test]
    fn vite_build_factory_matches_bare() {
        let f = ViteBuildBeholderFactory;
        assert!(f.matches(&args("vite build")));
        assert!(f.matches(&args("vite build --outDir dist")));
    }

    #[test]
    fn vite_build_factory_declines_no_build_subcommand() {
        let f = ViteBuildBeholderFactory;
        assert!(!f.matches(&args("vite")));
        assert!(!f.matches(&args("vite preview")));
        assert!(!f.matches(&args("vite dev")));
    }

    #[test]
    fn vite_build_factory_declines_help_and_version() {
        let f = ViteBuildBeholderFactory;
        assert!(!f.matches(&args("vite build --help")));
        assert!(!f.matches(&args("vite build -h")));
        assert!(!f.matches(&args("vite build --version")));
    }

    #[test]
    fn vite_build_factory_declines_non_vite() {
        let f = ViteBuildBeholderFactory;
        assert!(!f.matches(&args("cargo build")));
        assert!(!f.matches(&args("tsc")));
    }

    #[test]
    fn vite_build_factory_mode_is_parser() {
        assert!(matches!(ViteBuildBeholderFactory.mode(), BeholderMode::Parser));
    }

    // ─── ViteBuildBeholder parse tests ────────────────────────────────────────

    fn vite_chunk(lines: &str, seq: u32) -> OutputChunk {
        use crate::types::TaskRunId;
        OutputChunk {
            run_id: TaskRunId::new(),
            seq,
            offset_ms: 100,
            stream: crate::types::Stream::Stdout,
            bytes: lines.as_bytes().to_vec(),
        }
    }

    #[test]
    fn vite_beholder_parses_warning_line() {
        let mut b = ViteBuildBeholder::default();
        let chunk = vite_chunk(
            "(!) Some chunks are larger than 500 kB after minification.\n",
            1,
        );
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, Level::Warn);
        assert_eq!(events[0].target, "vite-build::warning");
        assert_eq!(events[0].msg, "Some chunks are larger than 500 kB after minification.");
        assert_eq!(events[0].fields["build"]["warning"], true);
    }

    #[test]
    fn vite_beholder_parses_bundle_line_with_gzip() {
        let mut b = ViteBuildBeholder::default();
        let chunk = vite_chunk(
            "dist/assets/index-CKBFsjV8.js   141.01 kB \u{2502} gzip:  45.33 kB\n",
            1,
        );
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.level, Level::Info);
        assert_eq!(ev.target, "vite-build::bundle");
        assert_eq!(ev.fields["file"]["path"], "dist/assets/index-CKBFsjV8.js");
        assert!((ev.fields["build"]["size_kb"].as_f64().unwrap() - 141.01).abs() < 0.01);
        assert!((ev.fields["build"]["gzip_kb"].as_f64().unwrap() - 45.33).abs() < 0.01);
    }

    #[test]
    fn vite_beholder_parses_bundle_line_no_gzip() {
        let mut b = ViteBuildBeholder::default();
        let chunk = vite_chunk("dist/index.html   0.46 kB\n", 1);
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].target, "vite-build::bundle");
        assert!((events[0].fields["build"]["size_kb"].as_f64().unwrap() - 0.46).abs() < 0.01);
        assert!(events[0].fields["build"].get("gzip_kb").is_none()
            || events[0].fields["build"]["gzip_kb"].is_null());
    }

    #[test]
    fn vite_beholder_parses_built_timing() {
        let mut b = ViteBuildBeholder::default();
        let chunk = vite_chunk("\u{2713} built in 2.43s\n", 1);
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.level, Level::Info);
        assert_eq!(ev.target, "vite-build");
        assert_eq!(ev.fields["build"]["duration_ms"], 2430u64);
        assert_eq!(ev.fields["build"]["success"], true);
    }

    #[test]
    fn vite_beholder_parses_built_timing_no_checkmark() {
        let mut b = ViteBuildBeholder::default();
        let chunk = vite_chunk("built in 0.99s\n", 1);
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].fields["build"]["duration_ms"], 990u64);
    }

    #[test]
    fn vite_beholder_ignores_unrelated_lines() {
        let mut b = ViteBuildBeholder::default();
        let chunk = vite_chunk(
            "vite v5.4.0 building for production...\n\
             \u{2713} 1234 modules transformed.\n",
            1,
        );
        let events = b.parse_chunk(&chunk);
        assert!(events.is_empty(), "got unexpected events: {events:?}");
    }

    #[test]
    fn vite_beholder_multiple_events_one_chunk() {
        let mut b = ViteBuildBeholder::default();
        let chunk = vite_chunk(
            "(!) Use of eval is strongly discouraged.\n\
             dist/assets/index.js   50.00 kB \u{2502} gzip: 15.00 kB\n\
             \u{2713} built in 1.50s\n",
            1,
        );
        let events = b.parse_chunk(&chunk);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].level, Level::Warn);
        assert_eq!(events[1].target, "vite-build::bundle");
        assert_eq!(events[2].target, "vite-build");
    }

    #[test]
    fn vite_beholder_chunks_split_across_calls() {
        let line = "(!) Large chunk warning.\n";
        let mid = line.len() / 2;
        let mut b = ViteBuildBeholder::default();
        let ev1 = b.parse_chunk(&vite_chunk(&line[..mid], 1));
        let ev2 = b.parse_chunk(&vite_chunk(&line[mid..], 2));
        assert!(ev1.is_empty(), "partial line should not emit");
        assert_eq!(ev2.len(), 1);
        assert_eq!(ev2[0].level, Level::Warn);
    }

    // ─── parse_vite_built_ms unit tests ───────────────────────────────────────

    #[test]
    fn parse_vite_built_ms_with_checkmark() {
        assert_eq!(parse_vite_built_ms("\u{2713} built in 2.43s"), Some(2430));
    }

    #[test]
    fn parse_vite_built_ms_bare() {
        assert_eq!(parse_vite_built_ms("built in 0.50s"), Some(500));
    }

    #[test]
    fn parse_vite_built_ms_not_a_timing_line() {
        assert!(parse_vite_built_ms("vite v5.0.0 building for production...").is_none());
        assert!(parse_vite_built_ms("").is_none());
    }
}
