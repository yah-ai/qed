//! `event-log` release provider (R508 / W208 §4) — declares that a run's
//! persistent QED event log should be spooled to object storage on completion.
//!
//! **The byte upload itself is performed by the host that owns the persistent
//! log, not by this adapter.** The qed crate emits [`crate::QedEvent`]s to an
//! in-memory sink; it does not own their durable on-disk form. In yah that
//! durable log is `.yah/jit/qed/<run_id>.events.jsonl`, written and owned by
//! the camp daemon's drain task — so the daemon performs the spool at
//! drain-completion (where the JSONL is guaranteed fully flushed), reusing this
//! adapter's [`EventLogConfig`] to know the destination. A standalone `qed` run
//! with no daemon keeps no persistent log, so there is nothing to spool.
//!
//! Keeping the opt-in on the same [`Outcome::Provider`](crate::Outcome::Provider)
//! seam as the vendor adapters means `qed validate`'s plan-time checks and the
//! dashboard see it, while the bytes ship from where they live. Because the log
//! of a *failed* multi-hour release is exactly what you want to inspect, the
//! daemon spools on terminal completion regardless of pass/fail — declaring the
//! outcome under either `on_success` or `on_fail` opts the run in.

use async_trait::async_trait;
use serde::Deserialize;

use super::{ProviderContext, ProviderReport, ReleaseProvider};
use crate::runner::RunnerError;

/// Stable provider name referenced in pipeline TOML (`provider = "event-log"`).
pub const EVENT_LOG_PROVIDER: &str = "event-log";

/// Where to spool the run's event log. Parsed from the outcome's `with` table.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct EventLogConfig {
    /// Storage tier — `"r2"` (default, Cloudflare R2) or `"pond"` /
    /// `"local-sim"` (local MinIO). Matches the `provider` values the almanac
    /// [`Outcome::Publish`](crate::Outcome::Publish) path accepts.
    #[serde(default = "default_provider")]
    pub provider: String,
    /// Destination bucket (required).
    pub bucket: String,
    /// Optional key prefix within the bucket. The log lands at
    /// `[<prefix>/]<run_id>.events.jsonl`.
    #[serde(default)]
    pub prefix: Option<String>,
}

fn default_provider() -> String {
    "r2".to_string()
}

impl EventLogConfig {
    /// Parse from an [`Outcome::Provider`](crate::Outcome::Provider) `with`
    /// blob. Returns a typed config error (naming what's wrong) on a malformed
    /// table.
    pub fn from_with(with: &serde_json::Value) -> Result<Self, RunnerError> {
        serde_json::from_value(with.clone()).map_err(|e| {
            RunnerError::InvalidConfig(format!(
                "event-log outcome `with` is malformed: {e} (need at least `bucket = \"...\"`)"
            ))
        })
    }

    /// The destination root (`<bucket>[/<prefix>]`) for log/dashboard display.
    pub fn dest(&self) -> String {
        match &self.prefix {
            Some(p) if !p.is_empty() => format!("{}/{p}", self.bucket),
            _ => self.bucket.clone(),
        }
    }
}

/// The `event-log` provider. See module docs: validates config + declares
/// intent; the host daemon performs the actual spool of its owned JSONL. R2
/// credentials resolve through the keystore (`cloudflare-r2-*`) exactly like
/// [`Outcome::Publish`](crate::Outcome::Publish), not through the secrets
/// bridge, so it declares no [`ReleaseProvider::required_slots`].
#[derive(Debug, Default)]
pub struct EventLogProvider;

#[async_trait]
impl ReleaseProvider for EventLogProvider {
    fn name(&self) -> &str {
        EVENT_LOG_PROVIDER
    }

    async fn dispatch(&self, ctx: &ProviderContext<'_>) -> Result<ProviderReport, RunnerError> {
        // Validate the destination even on a live run — a bad `with` table
        // should surface as a config error, not a silent no-spool. No upload
        // here: the persistent log is owned by the host daemon (see module
        // docs), which spools it at run-completion.
        let cfg = EventLogConfig::from_with(ctx.config)?;
        Ok(ProviderReport::action(format!(
            "event log spools to {}://{}/<run_id>.events.jsonl on completion (uploaded by the daemon that owns the log)",
            cfg.provider,
            cfg.dest(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::MapSecrets;
    use serde_json::json;

    #[test]
    fn parses_minimal_config_defaulting_provider_to_r2() {
        let cfg = EventLogConfig::from_with(&json!({ "bucket": "yah-qed-logs" })).unwrap();
        assert_eq!(cfg.provider, "r2");
        assert_eq!(cfg.bucket, "yah-qed-logs");
        assert_eq!(cfg.prefix, None);
        assert_eq!(cfg.dest(), "yah-qed-logs");
    }

    #[test]
    fn parses_full_config_and_builds_prefixed_dest() {
        let cfg = EventLogConfig::from_with(
            &json!({ "provider": "pond", "bucket": "logs", "prefix": "qed/events" }),
        )
        .unwrap();
        assert_eq!(cfg.provider, "pond");
        assert_eq!(cfg.dest(), "logs/qed/events");
    }

    #[test]
    fn missing_bucket_is_typed_config_error() {
        let err = EventLogConfig::from_with(&json!({ "provider": "r2" })).unwrap_err();
        assert!(format!("{err}").contains("bucket"), "names the field: {err}");
    }

    #[tokio::test]
    async fn dispatch_declares_intent_without_uploading() {
        let provider = EventLogProvider::default();
        let work = tempfile::tempdir().unwrap();
        let cfg = json!({ "bucket": "logs", "prefix": "qed" });
        let secrets = MapSecrets::default();
        let ctx = ProviderContext {
            version: "1.2.3",
            artifacts: &[],
            base_url: None,
            config: &cfg,
            work_dir: work.path(),
            secrets: &secrets,
            dry_run: false,
        };
        let report = provider.dispatch(&ctx).await.unwrap();
        assert_eq!(report.actions.len(), 1);
        assert!(report.actions[0].contains("logs/qed"));
        assert!(report.published.is_empty(), "the adapter ships nothing itself");
        assert!(report.produced.is_empty());
    }
}
