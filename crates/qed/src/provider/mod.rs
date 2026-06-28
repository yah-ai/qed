//! Vendor release-provider adapter seam (R509).
//!
//! [`crate::publish`] owns the *almanac channel* producer — laying artifacts
//! into an R2 tree and firing the revalidate hook. That [`ReleasePublisher`]
//! shape (`sync` + `revalidate`) is specific to the content-addressed channel
//! and does **not** fit the heterogeneous vendor publishers tier-3 releases
//! need: Apple notarization blocks on a remote ticket, Authenticode mutates a
//! `.exe` in place, Sparkle emits an appcast + signed delta, TestFlight and
//! Play upload to a vendor API. None of those is "upload a staged tree to a
//! bucket".
//!
//! This module is the shared seam those adapters plug into. Each vendor
//! adapter (Sparkle, winsparkle, notarize, Authenticode, TestFlight, Play,
//! GitHub Release) implements [`ReleaseProvider`] in its own child ticket
//! under R509 and is registered into a [`ProviderRegistry`] by name. The
//! adapters are independent — they share only this contract — so they fan out
//! and merge in any order.
//!
//! ## Contract
//!
//! - A pipeline references an adapter by **name** (`provider = "sparkle"`),
//!   alongside a vendor-specific `with` config blob and the credential
//!   **slot names** the adapter reads. The slot names resolve through the
//!   existing [`crate::secrets_bridge`] (`~/.yah/qed/secrets.toml`) — no new
//!   credential mechanism. Each adapter declares its slots via
//!   [`ReleaseProvider::required_slots`] so the runner can do a plan-time
//!   presence check and a dry-run can report what it *would* read.
//! - [`ReleaseProvider::dispatch`] performs the sign / notarize / upload. It
//!   honors [`ProviderContext::dry_run`]: a dry run validates config + checks
//!   credential presence and returns the actions it *would* take, performing
//!   no network I/O and no artifact mutation.
//! - Adapters that *transform* an artifact (sign, staple, notarize) return the
//!   transformed artifacts in [`ProviderReport::produced`]; adapters that
//!   *ship* an artifact return the destination URLs in
//!   [`ProviderReport::published`]. An adapter may do both (Sparkle emits an
//!   appcast artifact *and* uploads it).
//!
//! ## Wiring (deferred to the first landing adapter)
//!
//! This module lands the contract + registry only. Threading a named provider
//! through `Outcome` dispatch in [`crate::runner`] is intentionally left to the
//! first vendor F-ticket that needs a live run path, so the dispatch shape is
//! designed against a *real* adapter rather than speculatively. Until then the
//! registry is exercised by unit tests and by `qed validate`'s plan-time slot
//! check. See `.yah/docs/working/W208-qed-tier3-release-gap-closure.md` §5.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::runner::RunnerError;
use crate::types::ProducedArtifact;

pub mod appcast;
pub mod apple;
pub mod authenticode;
pub mod edsign;
pub mod event_log;
pub mod github_release;
pub mod notarize;
pub mod play;
pub mod sparkle;
pub mod testflight;
pub mod winsparkle;

pub use authenticode::AuthenticodeProvider;
pub use event_log::{EventLogConfig, EventLogProvider, EVENT_LOG_PROVIDER};
pub use github_release::GithubReleaseProvider;
pub use notarize::NotarizeProvider;
pub use play::PlayProvider;
pub use sparkle::SparkleProvider;
pub use testflight::TestFlightProvider;
pub use winsparkle::WinSparkleProvider;

/// Resolves a credential slot name to its current secret value. Implemented by
/// [`crate::secrets_bridge::SecretsConfig`] over the real vault; tests supply a
/// map-backed fake. Kept abstract so the adapter seam doesn't pull the vault /
/// `keys` crate into adapter unit tests.
pub trait SecretSource: Send + Sync {
    /// Resolve `name` (a bridged slot name, e.g. `"APPLE_API_KEY"`) to its
    /// value, or `None` when it isn't declared / doesn't resolve.
    fn resolve(&self, name: &str) -> Option<String>;
}

impl SecretSource for crate::secrets_bridge::SecretsConfig {
    fn resolve(&self, name: &str) -> Option<String> {
        self.resolve_one(name)
    }
}

/// A [`SecretSource`] backed by an in-memory map. Test fixture + the escape
/// hatch for callers that already hold resolved secrets.
#[derive(Debug, Clone, Default)]
pub struct MapSecrets(pub BTreeMap<String, String>);

impl SecretSource for MapSecrets {
    fn resolve(&self, name: &str) -> Option<String> {
        self.0.get(name).cloned()
    }
}

/// Everything an adapter needs to run one publish: the release version, the
/// artifacts produced by the run's successful steps, the vendor-specific
/// config blob, a scratch dir, a secret resolver, and the dry-run flag.
pub struct ProviderContext<'a> {
    /// Resolved release version (no leading `v`).
    pub version: &'a str,
    /// Artifacts collected from the run's successful `produces` declarations.
    pub artifacts: &'a [ProducedArtifact],
    /// Public-facing root for absolute URLs (e.g. `https://releases.yah.dev`).
    pub base_url: Option<&'a str>,
    /// Vendor-specific config (the pipeline's `with = { ... }` table), opaque
    /// to the seam. Each adapter deserializes its own typed config from this.
    pub config: &'a JsonValue,
    /// Scratch dir the adapter may write into (appcast XML, deltas, signed
    /// bundles). The caller owns its lifetime (a tempdir) and cleans it up.
    pub work_dir: &'a Path,
    /// Credential resolver over the secrets bridge.
    pub secrets: &'a dyn SecretSource,
    /// When `true`, validate + report intended actions only; perform no
    /// network I/O and mutate no artifacts.
    pub dry_run: bool,
}

impl ProviderContext<'_> {
    /// Resolve a required slot, mapping a miss to a typed [`RunnerError`] so an
    /// adapter can `?`-propagate a missing-credential failure with a message
    /// that names the slot and points at the secrets bridge.
    pub fn require_secret(&self, slot: &str) -> Result<String, RunnerError> {
        self.secrets
            .resolve(slot)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                RunnerError::Outcome(format!(
                    "release provider: credential slot `{slot}` is unset — declare it in \
                 ~/.yah/qed/secrets.toml (e.g. `{slot} = \"vault:<slot>\"`)"
                ))
            })
    }
}

/// What an adapter did (or, under `dry_run`, would do).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderReport {
    /// Human-readable log of actions taken (or planned, under dry-run). One
    /// line per discrete action; surfaced into the run's event stream.
    pub actions: Vec<String>,
    /// Artifacts the adapter produced or transformed in place — signed
    /// bundles, stapled `.app`s, generated appcast XML / deltas. These replace
    /// or augment the inputs for any downstream provider.
    pub produced: Vec<ProducedArtifact>,
    /// Destination URLs / vendor record locators the adapter shipped to
    /// (appcast URL, TestFlight build link, GitHub Release URL). For the
    /// dashboard + the run summary.
    pub published: Vec<String>,
}

impl ProviderReport {
    /// A report carrying a single action line and nothing else — the common
    /// shape for a notarize/sign step + the canonical dry-run skeleton.
    pub fn action(line: impl Into<String>) -> Self {
        Self {
            actions: vec![line.into()],
            ..Default::default()
        }
    }
}

/// A named vendor release adapter. One impl per vendor (Sparkle, notarize,
/// Authenticode, TestFlight, Play, GitHub Release, winsparkle), each landing in
/// its own R509 child ticket.
#[async_trait]
pub trait ReleaseProvider: Send + Sync {
    /// Stable name referenced in pipeline TOML (`provider = "<name>"`). Must be
    /// unique within a [`ProviderRegistry`].
    fn name(&self) -> &str;

    /// Credential slot names this adapter reads from the secrets bridge. Used
    /// for the plan-time presence check ([`ProviderRegistry::missing_slots`])
    /// and dry-run reporting. Empty for adapters that need no credentials.
    fn required_slots(&self) -> Vec<&str> {
        Vec::new()
    }

    /// Perform the sign / notarize / upload. Must honor
    /// [`ProviderContext::dry_run`] — a dry run does config validation +
    /// credential-presence checks and returns the intended actions only.
    async fn dispatch(&self, ctx: &ProviderContext<'_>) -> Result<ProviderReport, RunnerError>;
}

/// Registry of release-provider adapters, keyed by [`ReleaseProvider::name`].
/// The runner builds one default registry (with every wired adapter) and looks
/// up the named provider when dispatching a vendor publish outcome.
#[derive(Default, Clone)]
pub struct ProviderRegistry {
    providers: BTreeMap<String, Arc<dyn ReleaseProvider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// The built-in vendor adapter set (R509). The CLI + daemon construction
    /// sites pass this into [`crate::runner::PipelineRunner::with_release_providers`]
    /// so an `Outcome::Provider { provider = "notarize", … }` resolves to a real
    /// adapter. **This is the single registration point**: every R509 child
    /// adapter (authenticode, sparkle, winsparkle, testflight, play,
    /// github-release) lands one `.with(Arc::new(...))` line here as it merges.
    pub fn production() -> Self {
        Self::new()
            .with(Arc::new(NotarizeProvider::default()))
            .with(Arc::new(AuthenticodeProvider::default()))
            .with(Arc::new(SparkleProvider))
            .with(Arc::new(WinSparkleProvider))
            .with(Arc::new(TestFlightProvider::default()))
            .with(Arc::new(PlayProvider::default()))
            .with(Arc::new(GithubReleaseProvider::default()))
            // R508: declares the `event-log` outcome so a pipeline opting into
            // a persistent-log spool doesn't fail at dispatch. The byte upload
            // is performed by the host daemon that owns the JSONL — see
            // [`event_log`].
            .with(Arc::new(EventLogProvider::default()))
    }

    /// Register an adapter. Later registrations with the same name win (so a
    /// host can override a built-in adapter). Returns `self` for chaining.
    pub fn with(mut self, provider: Arc<dyn ReleaseProvider>) -> Self {
        self.providers.insert(provider.name().to_string(), provider);
        self
    }

    /// Register an adapter in place.
    pub fn register(&mut self, provider: Arc<dyn ReleaseProvider>) {
        self.providers.insert(provider.name().to_string(), provider);
    }

    /// Look up an adapter by name.
    pub fn get(&self, name: &str) -> Option<&Arc<dyn ReleaseProvider>> {
        self.providers.get(name)
    }

    /// Registered provider names, sorted.
    pub fn names(&self) -> Vec<&str> {
        self.providers.keys().map(String::as_str).collect()
    }

    /// Plan-time credential check: the slots the named provider declares that
    /// do **not** currently resolve to a non-empty value. An empty vec means
    /// every required slot is present. `None` when the provider isn't
    /// registered (an unknown-provider error the caller reports separately).
    pub fn missing_slots(&self, name: &str, secrets: &dyn SecretSource) -> Option<Vec<String>> {
        let provider = self.get(name)?;
        Some(
            provider
                .required_slots()
                .into_iter()
                .filter(|slot| secrets.resolve(slot).filter(|v| !v.is_empty()).is_none())
                .map(str::to_string)
                .collect(),
        )
    }

    /// Dispatch the named provider, or a typed unknown-provider error listing
    /// the registered names.
    pub async fn dispatch(
        &self,
        name: &str,
        ctx: &ProviderContext<'_>,
    ) -> Result<ProviderReport, RunnerError> {
        let provider = self.get(name).ok_or_else(|| {
            RunnerError::Outcome(format!(
                "release provider `{name}` is not registered (known: {})",
                self.names().join(", ")
            ))
        })?;
        provider.dispatch(ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial adapter: requires one slot, echoes a planned action, and (when
    /// live) reports a published URL.
    struct FakeProvider;

    #[async_trait]
    impl ReleaseProvider for FakeProvider {
        fn name(&self) -> &str {
            "fake"
        }
        fn required_slots(&self) -> Vec<&str> {
            vec!["FAKE_TOKEN"]
        }
        async fn dispatch(&self, ctx: &ProviderContext<'_>) -> Result<ProviderReport, RunnerError> {
            // Credential is required even for the action plan.
            let _token = ctx.require_secret("FAKE_TOKEN")?;
            if ctx.dry_run {
                return Ok(ProviderReport::action(format!(
                    "would publish {} artifact(s) at v{}",
                    ctx.artifacts.len(),
                    ctx.version
                )));
            }
            Ok(ProviderReport {
                actions: vec!["published".into()],
                produced: vec![],
                published: vec![format!("https://fake/{}", ctx.version)],
            })
        }
    }

    fn ctx<'a>(
        secrets: &'a dyn SecretSource,
        work: &'a Path,
        cfg: &'a JsonValue,
        dry_run: bool,
    ) -> ProviderContext<'a> {
        ProviderContext {
            version: "1.2.3",
            artifacts: &[],
            base_url: None,
            config: cfg,
            work_dir: work,
            secrets,
            dry_run,
        }
    }

    #[test]
    fn registry_get_and_names() {
        let reg = ProviderRegistry::new().with(Arc::new(FakeProvider));
        assert_eq!(reg.names(), vec!["fake"]);
        assert!(reg.get("fake").is_some());
        assert!(reg.get("nope").is_none());
    }

    #[test]
    fn missing_slots_reports_unresolved_only() {
        let reg = ProviderRegistry::new().with(Arc::new(FakeProvider));
        let empty = MapSecrets::default();
        assert_eq!(
            reg.missing_slots("fake", &empty).unwrap(),
            vec!["FAKE_TOKEN".to_string()]
        );
        let mut m = BTreeMap::new();
        m.insert("FAKE_TOKEN".to_string(), "abc".to_string());
        let present = MapSecrets(m);
        assert!(reg.missing_slots("fake", &present).unwrap().is_empty());
        // Unknown provider → None (not an empty vec).
        assert!(reg.missing_slots("nope", &empty).is_none());
    }

    #[test]
    fn empty_slot_value_counts_as_missing() {
        let reg = ProviderRegistry::new().with(Arc::new(FakeProvider));
        let mut m = BTreeMap::new();
        m.insert("FAKE_TOKEN".to_string(), String::new());
        assert_eq!(
            reg.missing_slots("fake", &MapSecrets(m)).unwrap(),
            vec!["FAKE_TOKEN".to_string()]
        );
    }

    #[tokio::test]
    async fn dispatch_dry_run_plans_without_publishing() {
        let reg = ProviderRegistry::new().with(Arc::new(FakeProvider));
        let work = tempfile::tempdir().unwrap();
        let cfg = JsonValue::Null;
        let mut m = BTreeMap::new();
        m.insert("FAKE_TOKEN".to_string(), "abc".to_string());
        let secrets = MapSecrets(m);
        let report = reg
            .dispatch("fake", &ctx(&secrets, work.path(), &cfg, true))
            .await
            .unwrap();
        assert_eq!(report.actions.len(), 1);
        assert!(report.actions[0].contains("would publish"));
        assert!(report.published.is_empty(), "dry run ships nothing");
    }

    #[tokio::test]
    async fn dispatch_live_reports_published_url() {
        let reg = ProviderRegistry::new().with(Arc::new(FakeProvider));
        let work = tempfile::tempdir().unwrap();
        let cfg = JsonValue::Null;
        let mut m = BTreeMap::new();
        m.insert("FAKE_TOKEN".to_string(), "abc".to_string());
        let secrets = MapSecrets(m);
        let report = reg
            .dispatch("fake", &ctx(&secrets, work.path(), &cfg, false))
            .await
            .unwrap();
        assert_eq!(report.published, vec!["https://fake/1.2.3".to_string()]);
    }

    #[tokio::test]
    async fn dispatch_missing_credential_is_typed_error() {
        let reg = ProviderRegistry::new().with(Arc::new(FakeProvider));
        let work = tempfile::tempdir().unwrap();
        let cfg = JsonValue::Null;
        let secrets = MapSecrets::default();
        let err = reg
            .dispatch("fake", &ctx(&secrets, work.path(), &cfg, true))
            .await
            .unwrap_err();
        assert!(
            format!("{err}").contains("FAKE_TOKEN"),
            "error names the slot: {err}"
        );
    }

    #[tokio::test]
    async fn dispatch_unknown_provider_lists_known() {
        let reg = ProviderRegistry::new().with(Arc::new(FakeProvider));
        let work = tempfile::tempdir().unwrap();
        let cfg = JsonValue::Null;
        let secrets = MapSecrets::default();
        let err = reg
            .dispatch("ghost", &ctx(&secrets, work.path(), &cfg, true))
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("ghost") && msg.contains("fake"),
            "names unknown + known: {msg}"
        );
    }
}
