//! Action [`Override`] registry — the W200 hook for replacing GHA actions
//! with native Rust impls.
//!
//! F4 ships the trait + the registry container + a TOML loader; the
//! per-action impls (`actions/checkout`, `Swatinem/rust-cache`, the docker
//! family, etc.) land in F5–F8. Until then, any `uses:` whose slug doesn't
//! match a registered impl or a TOML `deny:` entry is a loud error — the
//! W200 promise that v1 never silently skips an unknown action.

use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use thiserror::Error;

use crate::expr::Value;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("toml: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// What an override implementation produces when it finishes.
#[derive(Debug, Clone, Default)]
pub struct OverrideOutcome {
    /// Step outputs feeding `steps.<id>.outputs.*`.
    pub outputs: IndexMap<String, Value>,
    /// Free-form text the executor logs after the step runs. Useful for
    /// "uploaded N artifacts" summary lines.
    pub log: String,
    /// Whether the step failed. Defaults to `Success`.
    pub conclusion: StepConclusion,
    /// Built artifacts the override staged for the parent QED step to
    /// `Outcome::Publish`. F7's `softprops/action-gh-release` override is
    /// the primary producer; F9 walks these out of every step and rolls
    /// them up on `WorkflowRun::produced`.
    pub produced: Vec<ProducedArtifact>,
}

/// One built artifact addressed into the release channel as
/// `<binary>/<version>/<triple>/<filename>`. Structurally compatible with
/// `qed::types::ProducedArtifact` — F9 maps between them at the qed-runner
/// boundary; qed-gha intentionally doesn't depend on qed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducedArtifact {
    /// Logical binary name — becomes the channel sub-path. Derived from the
    /// archive filename's leading dash-delimited segment (`cli-*.tar.gz` →
    /// `cli`); override impls may override this explicitly.
    pub binary: String,
    /// Path to the built file, relative to the executor's workspace.
    pub path: String,
    /// Target-triple shorthand (e.g. `darwin-aarch64`). `None` resolves to
    /// the build host's triple at publish time.
    pub triple: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StepConclusion {
    #[default]
    Success,
    Failure,
    Skipped,
}

impl StepConclusion {
    pub fn as_str(self) -> &'static str {
        match self {
            StepConclusion::Success => "success",
            StepConclusion::Failure => "failure",
            StepConclusion::Skipped => "skipped",
        }
    }
}

/// Per-call inputs handed to an [`Override`].
pub struct OverrideCall<'a> {
    /// The original `uses:` slug minus `@ref`.
    pub slug: &'a str,
    /// Whatever followed `@` in the original `uses:` value.
    pub git_ref: Option<&'a str>,
    /// Already-evaluated `with:` inputs (ExprString → Value).
    pub with: &'a IndexMap<String, Value>,
    /// Composed step env (workflow + job + step, last write wins).
    pub env: &'a IndexMap<String, String>,
    /// Working directory the executor runs steps in.
    pub workspace: &'a Path,
    /// Per-slug TOML config blob — looked up by [`OverrideRegistry::config`]
    /// and passed in unchanged. F5+ overrides read their bucket/registry
    /// map / cache-dir overrides from here.
    pub config: &'a Value,
}

/// Implementor contract for a registered override. F5+ ships built-in impls;
/// tests + downstream callers can register their own.
pub trait Override: Send + Sync {
    fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String>;
}

/// Three-state lookup outcome for [`OverrideRegistry::lookup`].
pub enum Lookup<'a> {
    /// Registered impl + (possibly empty) TOML config blob.
    Found {
        ovr: &'a (dyn Override + 'a),
        config: &'a Value,
    },
    /// Slug was explicitly denied via TOML config.
    Denied { message: &'a str },
    /// No impl, no deny — F4 policy is to surface this as an error.
    Unknown,
}

/// Override registry. Built-in impls live in code; the TOML overlay supplies
/// per-slug config (buckets, registry maps), deny rules, and per-camp opt-outs.
#[derive(Default)]
pub struct OverrideRegistry {
    impls: IndexMap<String, Box<dyn Override>>,
    configs: IndexMap<String, Value>,
    denied: IndexMap<String, String>,
    null_config: Value,
}

impl OverrideRegistry {
    pub fn new() -> Self {
        Self {
            impls: IndexMap::new(),
            configs: IndexMap::new(),
            denied: IndexMap::new(),
            null_config: Value::Object(IndexMap::new()),
        }
    }

    /// Register an in-code [`Override`] for `slug` (no `@ref`).
    pub fn register(&mut self, slug: impl Into<String>, ovr: Box<dyn Override>) {
        self.impls.insert(slug.into(), ovr);
    }

    /// Look up `slug` in the registry. Deny rules win over impls (so a TOML
    /// `deny=true` for a built-in slug forces the error path even when the
    /// impl is loaded).
    pub fn lookup(&self, slug: &str) -> Lookup<'_> {
        if let Some(msg) = self.denied.get(slug) {
            return Lookup::Denied { message: msg.as_str() };
        }
        match self.impls.get(slug) {
            Some(ovr) => Lookup::Found {
                ovr: ovr.as_ref(),
                config: self.configs.get(slug).unwrap_or(&self.null_config),
            },
            None => Lookup::Unknown,
        }
    }

    /// Read-only access to a slug's config blob (empty Object if unset).
    pub fn config(&self, slug: &str) -> &Value {
        self.configs.get(slug).unwrap_or(&self.null_config)
    }

    /// Parse a TOML config blob and merge it into the registry. Format:
    ///
    /// ```toml
    /// [overrides."docker/build-push-action"]
    /// config.registry_route = { "ghcr.io" = "registry.yah.dev" }
    ///
    /// [overrides."actions/github-script"]
    /// deny = true
    /// deny_message = "github-script requires a JS runtime QED doesn't have"
    /// ```
    pub fn load_toml_str(&mut self, contents: &str) -> Result<(), RegistryError> {
        let parsed: RawConfig = toml::from_str(contents)?;
        for (slug, entry) in parsed.overrides.unwrap_or_default() {
            if entry.deny.unwrap_or(false) {
                let msg = entry.deny_message.unwrap_or_else(|| {
                    format!("override `{slug}` denied by configuration")
                });
                self.denied.insert(slug.clone(), msg);
            }
            if let Some(cfg) = entry.config {
                self.configs.insert(slug, toml_to_value(&cfg));
            }
        }
        Ok(())
    }

    /// Load a TOML file from disk and merge it into the registry. Missing
    /// files are silently OK — the W200 spec describes the per-camp + the
    /// per-machine overlay as both optional.
    pub fn load_toml_file(&mut self, path: &Path) -> Result<(), RegistryError> {
        match std::fs::read_to_string(path) {
            Ok(s) => self.load_toml_str(&s),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(serde::Deserialize)]
struct RawConfig {
    #[serde(default)]
    overrides: Option<IndexMap<String, RawOverride>>,
}

#[derive(serde::Deserialize)]
struct RawOverride {
    #[serde(default)]
    deny: Option<bool>,
    #[serde(default)]
    deny_message: Option<String>,
    #[serde(default)]
    config: Option<toml::Value>,
}

/// Convert a `toml::Value` into our expression [`Value`] so override impls
/// can read their config through the same tree-walker the evaluator uses.
fn toml_to_value(v: &toml::Value) -> Value {
    match v {
        toml::Value::String(s) => Value::String(s.clone()),
        toml::Value::Integer(n) => Value::Number(*n as f64),
        toml::Value::Float(f) => Value::Number(*f),
        toml::Value::Boolean(b) => Value::Bool(*b),
        toml::Value::Datetime(d) => Value::String(d.to_string()),
        toml::Value::Array(a) => Value::Array(a.iter().map(toml_to_value).collect()),
        toml::Value::Table(t) => {
            let mut out = IndexMap::new();
            for (k, v) in t {
                out.insert(k.clone(), toml_to_value(v));
            }
            Value::Object(out)
        }
    }
}

/// Default workspace-relative + home-relative TOML overlay paths per W200.
pub fn default_overlay_paths(workspace: &Path) -> Vec<PathBuf> {
    let mut out = vec![workspace.join(".yah/qed/gha-actions.toml")];
    if let Some(home) = std::env::var_os("HOME") {
        out.push(PathBuf::from(home).join(".yah/qed/gha-actions.toml"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    struct Echo;
    impl Override for Echo {
        fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
            let mut outputs = IndexMap::new();
            outputs.insert("echoed-ref".into(), Value::String(call.git_ref.unwrap_or("").into()));
            for (k, v) in call.with.iter() {
                outputs.insert(format!("in_{k}"), v.clone());
            }
            Ok(OverrideOutcome {
                outputs,
                log: "echoed".into(),
                conclusion: StepConclusion::Success,
            produced: Vec::new(),
            })
        }
    }

    #[test]
    fn lookup_unknown_returns_unknown() {
        let r = OverrideRegistry::new();
        match r.lookup("foo/bar") {
            Lookup::Unknown => {}
            other => panic!("expected Unknown, got {}", matches_to_name(&other)),
        }
    }

    #[test]
    fn register_then_lookup_returns_found() {
        let mut r = OverrideRegistry::new();
        r.register("test/echo", Box::new(Echo));
        match r.lookup("test/echo") {
            Lookup::Found { ovr: _, config: _ } => {}
            other => panic!("expected Found, got {}", matches_to_name(&other)),
        }
    }

    #[test]
    fn toml_deny_wins_over_registered_impl() {
        // Deny config short-circuits even when an impl is loaded. Matters
        // for camps that want to *prevent* a built-in from being used (the
        // W200 docs spell this out — "the ability to deny or replace a
        // built-in").
        let mut r = OverrideRegistry::new();
        r.register("test/echo", Box::new(Echo));
        let toml = r#"
            [overrides."test/echo"]
            deny = true
            deny_message = "no echo in production"
        "#;
        r.load_toml_str(toml).unwrap();
        match r.lookup("test/echo") {
            Lookup::Denied { message } => assert_eq!(message, "no echo in production"),
            other => panic!("expected Denied, got {}", matches_to_name(&other)),
        }
    }

    #[test]
    fn toml_config_blob_threaded_to_override() {
        let mut r = OverrideRegistry::new();
        r.register("docker/build-push-action", Box::new(Echo));
        let toml = r#"
            [overrides."docker/build-push-action"]
            config.registry_route = { "ghcr.io" = "registry.yah.dev" }
        "#;
        r.load_toml_str(toml).unwrap();
        let cfg = r.config("docker/build-push-action");
        if let Value::Object(m) = cfg {
            let route = m.get("registry_route").expect("registry_route");
            if let Value::Object(inner) = route {
                assert_eq!(
                    inner.get("ghcr.io"),
                    Some(&Value::String("registry.yah.dev".into()))
                );
            } else {
                panic!("registry_route should be an object: {route:?}");
            }
        } else {
            panic!("config should be Object: {cfg:?}");
        }
    }

    #[test]
    fn missing_toml_file_is_silent_ok() {
        let mut r = OverrideRegistry::new();
        let res = r.load_toml_file(Path::new("/no/such/file/here.toml"));
        assert!(res.is_ok(), "missing file should be silent OK, got {res:?}");
    }

    fn matches_to_name(l: &Lookup<'_>) -> &'static str {
        match l {
            Lookup::Found { .. } => "Found",
            Lookup::Denied { .. } => "Denied",
            Lookup::Unknown => "Unknown",
        }
    }
}
