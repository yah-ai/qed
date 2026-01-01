//! Parsed forms of `task-events:` and `events:` gnome rule check expressions.
//!
//! A rule TOML file uses the `check` field to declare what event query must
//! return *zero* results for the rule to be satisfied:
//!
//! ```toml
//! check = "task-events:cargo::rustc error.code=E0308"
//! check = "events:scope=service(noisetable-api.pdx) level=error"
//! ```
//!
//! [`TaskEventsRuleFilter`] is the original parsed form for `task-events:`.
//! [`EventsRuleFilter`] is the generalized form for both `events:` and
//! `task-events:` (which desugars to `events:` with `scope: ScopeSpec::CurrentTaskRun`).
//!
//! @yah:ticket(Q068-F1, "Broaden task-events: rule grammar to unified events: form (scryer-aware)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-09T00:00:00Z)
//! @yah:status(review)
//! @yah:parent(Q068)
//! @yah:handoff("Q068-F1 landed 2026-05-09. New EventsRuleFilter + ScopeSpec types in crates/yah/task-runs/src/rule_filter.rs. EventsRuleFilter accepts both 'events:' and 'task-events:' prefixes; scope=service(<mesh-ident>) routes to scryer, scope=taskrun(...)  desugars to CurrentTaskRun. gnomes/src/rules.rs: RuleCheck::Events { filter: EventsRuleFilter } added; task-events: and events: both parse to this variant. gnomes/src/verify.rs: ForgeVerifyDispatch gained optional scryer: Option<Arc<Scryer>> (via with_scryer() builder); Service-scope queries route to scryer.events(Service(MeshIdent(...))), absent scryer → N/A. task-runs/src/lib.rs re-exports EventsRuleFilter + ScopeSpec. gnomes/Cargo.toml: workload-spec dep added. 186 gnomes tests pass (3 new verify tests); 29 rule_filter tests pass (10 new EventsRuleFilter tests); cargo check --workspace clean.")
//! @yah:verify("cargo test -p task-runs rule_filter — all EventsRuleFilter parse tests pass.")
//! @yah:verify("cargo test -p gnomes verify:: — forge_dispatch, events_prefix_current_taskrun_scope, events_service_scope all pass.")
//! @arch:see(.yah/docs/architecture/A036-yah-gnomes.md)
//! @arch:see(.yah/docs/architecture/A049-yah-scryer.md)
//! @arch:see(.yah/docs/working/yah-task-runs.md)

use crate::types::Level;

// ─── FieldPredicate ───────────────────────────────────────────────────────────

/// One field-level equality predicate within a `task-events:` check.
///
/// `path` is a dot-separated key into `Event.fields` (e.g. `"error.code"`).
/// Evaluators prepend `$.` when translating to a JSONPath filter.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FieldPredicate {
    /// Dot-separated field path, e.g. `"error.code"`, `"file.path"`.
    pub path: String,
    /// Expected value — matched for equality. String, number, or bool.
    pub value: serde_json::Value,
}

// ─── TaskEventsRuleFilter ─────────────────────────────────────────────────────

/// Parsed predicate from a `task-events:<expr>` rule `check` field.
///
/// Semantics: after the verify [`TaskRun`][crate::types::TaskRunMeta] completes,
/// call `task.events` with these params. **Any match means the rule is violated.**
///
/// ## Grammar
///
/// ```text
/// expr       := [target] (WS field-pred)*
/// target     := word-without-'='          e.g. "cargo::rustc"
/// field-pred := key '=' scalar-value
/// key        := "level" | "min_level"     special: parsed as Level
///             | dotted-path               e.g. "error.code", "file.path"
/// scalar     := bare-number | "true" | "false" | bare-string
/// ```
///
/// ## Examples
///
/// ```text
/// "task-events:cargo::rustc error.code=E0308"
///   → target: cargo::rustc, field: error.code == "E0308"
///
/// "task-events:clippy::warning level=error"
///   → target: clippy::warning, min_level: Error
///
/// "task-events:cargo::rustc"
///   → target: cargo::rustc, any level, any field
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TaskEventsRuleFilter {
    /// Event target prefix, e.g. `"cargo::rustc"` or `"clippy::warning"`.
    ///
    /// When set, the evaluator passes this as the `target` param to
    /// `task.events`, which does a prefix match. `None` means any target.
    pub target: Option<String>,
    /// Minimum severity to include. `None` defaults to `warn` at the
    /// call site so rule checks don't fire on debug/trace noise.
    pub min_level: Option<Level>,
    /// Field equality predicates. The evaluator ANDs them: all must match.
    pub field_filters: Vec<FieldPredicate>,
}

impl FieldPredicate {
    /// Returns `true` when this predicate matches the given event `fields` JSON object.
    ///
    /// The path is traversed as dot-separated keys (e.g. `"error.code"` →
    /// `fields["error"]["code"]`). A missing key or a type mismatch returns `false`.
    pub fn matches(&self, fields: &serde_json::Value) -> bool {
        let mut current = fields;
        for key in self.path.split('.') {
            match current.get(key) {
                Some(v) => current = v,
                None => return false,
            }
        }
        current == &self.value
    }
}

impl TaskEventsRuleFilter {
    /// Returns `true` when **all** field predicates match the given event `fields`
    /// JSON object. An empty `field_filters` list always returns `true`.
    ///
    /// The gnome verify pass calls this after fetching events via `task.events`
    /// (which already pre-filters by `target` and `min_level`). The combined
    /// protocol is:
    ///
    /// 1. Call `task.events(run_id, target=filter.target, min_level=...)`.
    /// 2. For each returned event, call `filter.matches_fields(&event.fields)`.
    /// 3. If any event passes → **rule violated**; if none pass → satisfied.
    ///
    /// This client-side pass is necessary because `task.events` only supports
    /// a single `jsonpath` server-side filter; AND-ing multiple field predicates
    /// is done here.
    pub fn matches_fields(&self, fields: &serde_json::Value) -> bool {
        self.field_filters.iter().all(|fp| fp.matches(fields))
    }

    /// Parse the part of a rule check string after stripping `"task-events:"`.
    ///
    /// Returns `Err` if any token fails to parse (e.g. malformed `key=value`).
    pub fn parse(expr: &str) -> Result<Self, ParseError> {
        let mut tokens = expr.split_ascii_whitespace().peekable();
        let mut target: Option<String> = None;
        let mut min_level: Option<Level> = None;
        let mut field_filters: Vec<FieldPredicate> = Vec::new();

        // First token: if it doesn't contain '=' it's the target.
        if let Some(first) = tokens.peek() {
            if !first.contains('=') {
                target = Some(tokens.next().unwrap().to_owned());
            }
        }

        for token in tokens {
            let (key, val_str) = token.split_once('=').ok_or_else(|| {
                ParseError(format!("expected key=value, got {token:?}"))
            })?;
            match key {
                "level" | "min_level" => {
                    min_level = Some(parse_level(val_str)?);
                }
                "" => return Err(ParseError("empty key before '='".into())),
                _ => {
                    field_filters.push(FieldPredicate {
                        path: key.to_owned(),
                        value: parse_scalar(val_str),
                    });
                }
            }
        }

        Ok(Self { target, min_level, field_filters })
    }

    /// Parse a full `task-events:<expr>` string (including the prefix).
    pub fn parse_check_field(check: &str) -> Result<Self, ParseError> {
        let inner = check.strip_prefix("task-events:").ok_or_else(|| {
            ParseError(format!("check field does not start with 'task-events:': {check:?}"))
        })?;
        Self::parse(inner)
    }
}

// ─── ScopeSpec ────────────────────────────────────────────────────────────────

/// Which event store to query when evaluating an `events:` rule check.
///
/// `CurrentTaskRun` is the default: the verify pass queries events from the
/// task run that was just executed (same semantics as `task-events:`).
/// `Service(ident)` queries scryer for long-running service emissions keyed
/// by the given mesh identity string (e.g. `"noisetable-api.pdx"`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeSpec {
    /// Default: use the current verify TaskRun (populated at evaluate time).
    /// Equivalent to `task-events:` semantics.
    CurrentTaskRun,
    /// Named service: query scryer's service-scope events for this mesh ident.
    Service(String),
}

impl Default for ScopeSpec {
    fn default() -> Self {
        ScopeSpec::CurrentTaskRun
    }
}

// ─── EventsRuleFilter ─────────────────────────────────────────────────────────

/// Parsed predicate from an `events:<expr>` (or `task-events:<expr>`) rule check.
///
/// Extends [`TaskEventsRuleFilter`] with an optional [`ScopeSpec`].
/// `task-events:` is a permanent alias that desugars to
/// `scope: ScopeSpec::CurrentTaskRun` — identical semantics, backward-compatible.
///
/// ## Grammar
///
/// ```text
/// check      := ("events:" | "task-events:") expr
/// expr       := [scope-clause] [target] (WS field-pred)*
/// scope-clause := "scope=taskrun(" uuid ")"
///              |  "scope=service(" mesh-ident ")"
/// target     := word-without-'='           e.g. "cargo::rustc"
/// field-pred := key '=' scalar-value
/// key        := "level" | "min_level"      special: parsed as Level
///             | dotted-path                e.g. "error.code", "file.path"
/// ```
///
/// `scope=taskrun(...)` desugars to `CurrentTaskRun` (the explicit UUID is
/// filled in at evaluate time — static rule files never contain literal run IDs).
///
/// ## Examples
///
/// ```text
/// "events:cargo::rustc level=error"
///   → scope: CurrentTaskRun, target: cargo::rustc, min_level: Error
///
/// "events:scope=service(noisetable-api.pdx) level=error"
///   → scope: Service("noisetable-api.pdx"), min_level: Error
///
/// "task-events:cargo::rustc error.code=E0308"
///   → scope: CurrentTaskRun (alias), target: cargo::rustc, field: error.code=="E0308"
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EventsRuleFilter {
    /// Which store to query. Default is `CurrentTaskRun`.
    #[serde(default)]
    pub scope: ScopeSpec,
    /// Event target prefix, e.g. `"cargo::rustc"`. `None` means any target.
    pub target: Option<String>,
    /// Minimum severity. `None` defaults to `warn` at the call site.
    pub min_level: Option<Level>,
    /// Field equality predicates — all must match (AND-ed).
    pub field_filters: Vec<FieldPredicate>,
}

impl EventsRuleFilter {
    /// Returns `true` when all field predicates match the given event `fields` object.
    pub fn matches_fields(&self, fields: &serde_json::Value) -> bool {
        self.field_filters.iter().all(|fp| fp.matches(fields))
    }

    /// Parse both `events:<expr>` and `task-events:<expr>` check strings.
    ///
    /// `task-events:` is a permanent alias for `events:` with `scope: CurrentTaskRun`.
    pub fn parse_check_field(check: &str) -> Result<Self, ParseError> {
        let inner = if let Some(rest) = check.strip_prefix("events:") {
            rest
        } else if let Some(rest) = check.strip_prefix("task-events:") {
            rest
        } else {
            return Err(ParseError(format!(
                "check field does not start with 'events:' or 'task-events:': {check:?}"
            )));
        };
        Self::parse(inner)
    }

    fn parse(expr: &str) -> Result<Self, ParseError> {
        let mut tokens = expr.split_ascii_whitespace().peekable();
        let mut scope = ScopeSpec::CurrentTaskRun;
        let mut target: Option<String> = None;
        let mut min_level: Option<Level> = None;
        let mut field_filters: Vec<FieldPredicate> = Vec::new();

        // First token: scope= clause OR bare target (word without '=').
        if let Some(&first) = tokens.peek() {
            if let Some(rest) = first.strip_prefix("scope=") {
                tokens.next();
                scope = parse_scope_spec(rest)?;
                // After scope clause, optional bare target follows.
                if let Some(&next) = tokens.peek() {
                    if !next.contains('=') {
                        target = Some(tokens.next().unwrap().to_owned());
                    }
                }
            } else if !first.contains('=') {
                target = Some(tokens.next().unwrap().to_owned());
            }
        }

        for token in tokens {
            let (key, val_str) = token.split_once('=').ok_or_else(|| {
                ParseError(format!("expected key=value, got {token:?}"))
            })?;
            match key {
                "level" | "min_level" => {
                    min_level = Some(parse_level(val_str)?);
                }
                "" => return Err(ParseError("empty key before '='".into())),
                _ => {
                    field_filters.push(FieldPredicate {
                        path: key.to_owned(),
                        value: parse_scalar(val_str),
                    });
                }
            }
        }

        Ok(Self { scope, target, min_level, field_filters })
    }
}

fn parse_scope_spec(s: &str) -> Result<ScopeSpec, ParseError> {
    if let Some(inner) = s.strip_prefix("taskrun(").and_then(|s| s.strip_suffix(')')) {
        // scope=taskrun(<uuid>) — the explicit UUID is evaluated at verify time.
        // In static rule TOML files this always desugars to CurrentTaskRun.
        let _ = inner;
        Ok(ScopeSpec::CurrentTaskRun)
    } else if let Some(inner) = s.strip_prefix("service(").and_then(|s| s.strip_suffix(')')) {
        if inner.is_empty() {
            return Err(ParseError("scope=service() requires a mesh ident".into()));
        }
        Ok(ScopeSpec::Service(inner.to_owned()))
    } else {
        Err(ParseError(format!(
            "unknown scope: {s:?}; expected taskrun(<uuid>) or service(<mesh-ident>)"
        )))
    }
}

// ─── ParseError ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ParseError {}

// ─── helpers ─────────────────────────────────────────────────────────────────

fn parse_level(s: &str) -> Result<Level, ParseError> {
    match s.to_ascii_lowercase().as_str() {
        "trace" => Ok(Level::Trace),
        "debug" => Ok(Level::Debug),
        "info" => Ok(Level::Info),
        "warn" | "warning" => Ok(Level::Warn),
        "error" => Ok(Level::Error),
        "fatal" => Ok(Level::Fatal),
        other => Err(ParseError(format!("unknown level {other:?}"))),
    }
}

/// Parse a scalar value: bool → bool, integer → i64, float → f64, else String.
fn parse_scalar(s: &str) -> serde_json::Value {
    if s == "true" { return serde_json::Value::Bool(true); }
    if s == "false" { return serde_json::Value::Bool(false); }
    if let Ok(n) = s.parse::<i64>() { return serde_json::json!(n); }
    if let Ok(n) = s.parse::<f64>() { return serde_json::json!(n); }
    serde_json::Value::String(s.to_owned())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_target_only() {
        let f = TaskEventsRuleFilter::parse("cargo::rustc").unwrap();
        assert_eq!(f.target.as_deref(), Some("cargo::rustc"));
        assert!(f.min_level.is_none());
        assert!(f.field_filters.is_empty());
    }

    #[test]
    fn parse_target_and_field() {
        let f = TaskEventsRuleFilter::parse("cargo::rustc error.code=E0308").unwrap();
        assert_eq!(f.target.as_deref(), Some("cargo::rustc"));
        assert!(f.min_level.is_none());
        assert_eq!(f.field_filters.len(), 1);
        assert_eq!(f.field_filters[0].path, "error.code");
        assert_eq!(f.field_filters[0].value, serde_json::Value::String("E0308".into()));
    }

    #[test]
    fn parse_level_key() {
        let f = TaskEventsRuleFilter::parse("clippy::warning level=error").unwrap();
        assert_eq!(f.target.as_deref(), Some("clippy::warning"));
        assert_eq!(f.min_level, Some(Level::Error));
        assert!(f.field_filters.is_empty());
    }

    #[test]
    fn parse_min_level_alias() {
        let f = TaskEventsRuleFilter::parse("min_level=warn").unwrap();
        assert!(f.target.is_none());
        assert_eq!(f.min_level, Some(Level::Warn));
    }

    #[test]
    fn parse_multiple_field_filters() {
        let f = TaskEventsRuleFilter::parse(
            "cargo::rustc error.code=E0308 file.path=src/lib.rs"
        ).unwrap();
        assert_eq!(f.field_filters.len(), 2);
        assert_eq!(f.field_filters[0].path, "error.code");
        assert_eq!(f.field_filters[1].path, "file.path");
        assert_eq!(
            f.field_filters[1].value,
            serde_json::Value::String("src/lib.rs".into())
        );
    }

    #[test]
    fn parse_boolean_value() {
        let f = TaskEventsRuleFilter::parse("build.success=false").unwrap();
        assert!(f.target.is_none());
        assert_eq!(f.field_filters[0].value, serde_json::Value::Bool(false));
    }

    #[test]
    fn parse_numeric_value() {
        let f = TaskEventsRuleFilter::parse("cargo::rustc file.line=42").unwrap();
        assert_eq!(f.field_filters[0].value, serde_json::json!(42i64));
    }

    #[test]
    fn parse_empty_is_unconstrained() {
        let f = TaskEventsRuleFilter::parse("").unwrap();
        assert!(f.target.is_none());
        assert!(f.min_level.is_none());
        assert!(f.field_filters.is_empty());
    }

    #[test]
    fn parse_check_field_prefix() {
        let f = TaskEventsRuleFilter::parse_check_field(
            "task-events:cargo::rustc error.code=E0308"
        ).unwrap();
        assert_eq!(f.target.as_deref(), Some("cargo::rustc"));
    }

    #[test]
    fn parse_check_field_wrong_prefix() {
        assert!(TaskEventsRuleFilter::parse_check_field("ast:foo").is_err());
    }

    #[test]
    fn parse_missing_eq_returns_err() {
        assert!(TaskEventsRuleFilter::parse("cargo::rustc noequalssign").is_err());
    }

    #[test]
    fn parse_level_warning_alias() {
        let f = TaskEventsRuleFilter::parse("level=warning").unwrap();
        assert_eq!(f.min_level, Some(Level::Warn));
    }

    #[test]
    fn parse_unknown_level_returns_err() {
        assert!(TaskEventsRuleFilter::parse("level=critical").is_err());
    }

    // ─── matches_fields ───────────────────────────────────────────────────────

    #[test]
    fn field_predicate_matches_present_key() {
        let fp = FieldPredicate {
            path: "error.code".into(),
            value: serde_json::Value::String("E0308".into()),
        };
        let fields = serde_json::json!({"error": {"code": "E0308"}});
        assert!(fp.matches(&fields));
    }

    #[test]
    fn field_predicate_rejects_wrong_value() {
        let fp = FieldPredicate {
            path: "error.code".into(),
            value: serde_json::Value::String("E0308".into()),
        };
        let fields = serde_json::json!({"error": {"code": "E0309"}});
        assert!(!fp.matches(&fields));
    }

    #[test]
    fn field_predicate_rejects_missing_key() {
        let fp = FieldPredicate {
            path: "error.code".into(),
            value: serde_json::Value::String("E0308".into()),
        };
        let fields = serde_json::json!({"error": {}});
        assert!(!fp.matches(&fields));
    }

    #[test]
    fn matches_fields_empty_predicates_always_true() {
        let f = TaskEventsRuleFilter { target: None, min_level: None, field_filters: vec![] };
        assert!(f.matches_fields(&serde_json::json!({})));
    }

    #[test]
    fn matches_fields_all_must_match() {
        let f = TaskEventsRuleFilter::parse(
            "cargo::rustc error.code=E0308 file.path=src/lib.rs"
        ).unwrap();
        // Both match
        let ok = serde_json::json!({"error": {"code": "E0308"}, "file": {"path": "src/lib.rs"}});
        assert!(f.matches_fields(&ok));
        // One missing
        let bad = serde_json::json!({"error": {"code": "E0308"}});
        assert!(!f.matches_fields(&bad));
    }

    #[test]
    fn matches_fields_boolean_predicate() {
        let f = TaskEventsRuleFilter::parse("build.success=false").unwrap();
        assert!(f.matches_fields(&serde_json::json!({"build": {"success": false}})));
        assert!(!f.matches_fields(&serde_json::json!({"build": {"success": true}})));
    }

    // ─── EventsRuleFilter ─────────────────────────────────────────────────────

    #[test]
    fn events_filter_default_scope_is_current_taskrun() {
        let f = EventsRuleFilter::parse_check_field("events:cargo::rustc level=error").unwrap();
        assert_eq!(f.scope, ScopeSpec::CurrentTaskRun);
        assert_eq!(f.target.as_deref(), Some("cargo::rustc"));
        assert_eq!(f.min_level, Some(Level::Error));
    }

    #[test]
    fn events_filter_task_events_alias() {
        // task-events: desugars to CurrentTaskRun scope — identical semantics.
        let alias = EventsRuleFilter::parse_check_field("task-events:cargo::rustc level=error").unwrap();
        let canonical = EventsRuleFilter::parse_check_field("events:cargo::rustc level=error").unwrap();
        assert_eq!(alias, canonical);
    }

    #[test]
    fn events_filter_service_scope() {
        let f = EventsRuleFilter::parse_check_field(
            "events:scope=service(noisetable-api.pdx) level=error"
        ).unwrap();
        assert_eq!(f.scope, ScopeSpec::Service("noisetable-api.pdx".into()));
        assert!(f.target.is_none());
        assert_eq!(f.min_level, Some(Level::Error));
    }

    #[test]
    fn events_filter_service_scope_with_target_and_fields() {
        let f = EventsRuleFilter::parse_check_field(
            "events:scope=service(api.prod) cargo::rustc error.code=E0308"
        ).unwrap();
        assert_eq!(f.scope, ScopeSpec::Service("api.prod".into()));
        assert_eq!(f.target.as_deref(), Some("cargo::rustc"));
        assert_eq!(f.field_filters.len(), 1);
        assert_eq!(f.field_filters[0].path, "error.code");
    }

    #[test]
    fn events_filter_taskrun_scope_desugars_to_current() {
        // scope=taskrun(...) always desugars to CurrentTaskRun in static rules.
        let f = EventsRuleFilter::parse_check_field(
            "events:scope=taskrun(00000000-0000-0000-0000-000000000000)"
        ).unwrap();
        assert_eq!(f.scope, ScopeSpec::CurrentTaskRun);
    }

    #[test]
    fn events_filter_service_empty_ident_is_err() {
        assert!(EventsRuleFilter::parse_check_field("events:scope=service()").is_err());
    }

    #[test]
    fn events_filter_unknown_scope_is_err() {
        assert!(EventsRuleFilter::parse_check_field("events:scope=forge(abc)").is_err());
    }

    #[test]
    fn events_filter_wrong_prefix_is_err() {
        assert!(EventsRuleFilter::parse_check_field("ast:foo").is_err());
    }

    #[test]
    fn events_filter_no_scope_no_target() {
        let f = EventsRuleFilter::parse_check_field("events:").unwrap();
        assert_eq!(f.scope, ScopeSpec::CurrentTaskRun);
        assert!(f.target.is_none());
        assert!(f.min_level.is_none());
        assert!(f.field_filters.is_empty());
    }

    #[test]
    fn events_filter_matches_fields_service_scope() {
        let f = EventsRuleFilter::parse_check_field(
            "events:scope=service(api.prod) level=error"
        ).unwrap();
        // field_filters is empty; matches_fields always true for empty predicates.
        assert!(f.matches_fields(&serde_json::json!({})));
    }
}
