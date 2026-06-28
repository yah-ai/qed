//! Tier-1/2 **toolkit-contract executor** registry (W224 R533-T7).
//!
//! This is the recast of W200's "override registry." W224 settles the QED↔GHA
//! boundary as *import, not emulate*: QED no longer reimplements GitHub's
//! tier-3 *services* (`checkout`, `cache`, `upload-artifact`, `gh-release`, the
//! docker push family). Those are **replaced with native QED facilities at
//! import time** — the tier-3 boundary catalog lives in [`crate::tier`] and the
//! transformer ([`crate::transform`]) flags them with native-replacement
//! stanzas.
//!
//! What survives here is the narrow, fully-specified, stable part: **tier-1/2
//! toolkit-contract compute actions** (`dtolnay/rust-toolchain`,
//! `oven-sh/setup-bun`, the docker buildx/qemu *setup* verifiers,
//! `sigstore/cosign-installer`). A [`ToolkitAction`] gets its evaluated `with:`
//! inputs + composed env in a workspace sandbox and *computes* — it does not
//! integrate with GitHub-the-service. A `uses:` slug that isn't a registered
//! toolkit action is routed through the tier classifier by the runtime: a
//! tier-3 slug becomes a "replace with native" error, an unrecognized slug a
//! plain unknown-action error.
//!
//! Gone from the W200 surface (retired with the tier-3 impls): the per-slug
//! TOML deny/config overlay (`registry_route` / `registry_auth` were docker-
//! push machinery), the `Denied` lookup state, and the `produced` artifact
//! channel (only the tier-3 `gh-release` override fed it; native publishers own
//! release artifacts now).

use indexmap::IndexMap;
use std::path::Path;

use crate::expr::Value;

/// What a [`ToolkitAction`] produces when it finishes.
#[derive(Debug, Clone, Default)]
pub struct ToolkitOutcome {
    /// Step outputs feeding `steps.<id>.outputs.*`.
    pub outputs: IndexMap<String, Value>,
    /// Free-form text the executor logs after the step runs.
    pub log: String,
    /// Whether the step failed. Defaults to `Success`.
    pub conclusion: StepConclusion,
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

/// Per-call inputs handed to a [`ToolkitAction`].
pub struct ToolkitCall<'a> {
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
}

/// Implementor contract for a registered tier-1/2 toolkit action. Built-in
/// impls ship in [`crate::toolkit_builtin`]; tests + downstream callers can
/// register their own.
pub trait ToolkitAction: Send + Sync {
    fn execute(&self, call: &ToolkitCall<'_>) -> Result<ToolkitOutcome, String>;
}

/// Two-state lookup outcome for [`ToolkitRegistry::lookup`]. (W200's third
/// `Denied` state is retired with the TOML overlay — a tier-3 slug is now
/// declined structurally by the tier classifier, not by per-camp config.)
pub enum Lookup<'a> {
    /// Registered tier-1/2 toolkit action.
    Found { action: &'a (dyn ToolkitAction + 'a) },
    /// No registered toolkit action for this slug. The runtime consults
    /// [`crate::tier::classify_uses`] to decide whether it's a tier-3
    /// replace-with-native slug or a genuinely unrecognized action.
    Unknown,
}

/// Registry of tier-1/2 toolkit-contract compute actions, keyed by `uses:` slug
/// (no `@ref`). Built-in impls live in code via
/// [`crate::toolkit_builtin::register_toolkit`].
#[derive(Default)]
pub struct ToolkitRegistry {
    impls: IndexMap<String, Box<dyn ToolkitAction>>,
}

impl ToolkitRegistry {
    pub fn new() -> Self {
        Self {
            impls: IndexMap::new(),
        }
    }

    /// Register an in-code [`ToolkitAction`] for `slug` (no `@ref`).
    pub fn register(&mut self, slug: impl Into<String>, action: Box<dyn ToolkitAction>) {
        self.impls.insert(slug.into(), action);
    }

    /// Look up `slug` in the registry.
    pub fn lookup(&self, slug: &str) -> Lookup<'_> {
        match self.impls.get(slug) {
            Some(action) => Lookup::Found {
                action: action.as_ref(),
            },
            None => Lookup::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    struct Echo;
    impl ToolkitAction for Echo {
        fn execute(&self, call: &ToolkitCall<'_>) -> Result<ToolkitOutcome, String> {
            let mut outputs = IndexMap::new();
            outputs.insert(
                "echoed-ref".into(),
                Value::String(call.git_ref.unwrap_or("").into()),
            );
            for (k, v) in call.with.iter() {
                outputs.insert(format!("in_{k}"), v.clone());
            }
            Ok(ToolkitOutcome {
                outputs,
                log: "echoed".into(),
                conclusion: StepConclusion::Success,
            })
        }
    }

    #[test]
    fn lookup_unknown_returns_unknown() {
        let r = ToolkitRegistry::new();
        assert!(matches!(r.lookup("foo/bar"), Lookup::Unknown));
    }

    #[test]
    fn register_then_lookup_returns_found() {
        let mut r = ToolkitRegistry::new();
        r.register("test/echo", Box::new(Echo));
        assert!(matches!(r.lookup("test/echo"), Lookup::Found { .. }));
    }
}
