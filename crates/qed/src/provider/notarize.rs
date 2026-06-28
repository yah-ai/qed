//! Apple notarization + stapling adapter (R509-F1).
//!
//! Wraps `xcrun notarytool submit --wait` followed by `xcrun stapler staple`.
//! Takes the run's produced `.app` / `.dmg` / `.pkg` bundles, submits each to
//! Apple's notary service with an App Store Connect API key, blocks on the
//! notarization ticket, staples the ticket into the bundle, and returns the
//! stapled artifacts in [`ProviderReport::produced`] so a downstream
//! `Outcome::Provider { provider = "sparkle" }` ships the stapled bundle.
//!
//! Notarization is a *transform*, not a ship — it mutates the bundle in place
//! (stapling) and publishes nothing, so [`ProviderReport::published`] stays
//! empty. The pipeline orders `notarize` before the channel-ship outcome.
//!
//! ## Credentials
//!
//! Three slots, resolved through [`crate::secrets_bridge`]:
//! - `APPLE_API_KEY_ID` — the App Store Connect API key id (`-d` / `--key-id`).
//! - `APPLE_API_ISSUER` — the issuer UUID (`--issuer`).
//! - `APPLE_API_KEY_P8` — the `.p8` private key *contents*. Materialized to a
//!   `0600` temp file under `ctx.work_dir` (never logged) for `--key`.
//!
//! ## Dry run
//!
//! `ctx.dry_run` validates that all three slots resolve and that at least one
//! notarizable artifact is selected, then reports `would submit <file> to
//! notarytool` per artifact — spawning no `xcrun` and mutating nothing.

use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use crate::provider::apple::{write_p8_key, SLOT_ISSUER, SLOT_KEY_ID, SLOT_KEY_P8};
use crate::provider::{ProviderContext, ProviderReport, ReleaseProvider};
use crate::runner::RunnerError;
use crate::types::ProducedArtifact;

/// Bundle extensions Apple's notary service accepts. `.app` is a directory
/// bundle; `.dmg` / `.pkg` / `.zip` are files. Matched case-insensitively.
const NOTARIZABLE_EXTS: &[&str] = &["app", "dmg", "pkg", "zip"];

/// The `with = { … }` config block for a `notarize` outcome.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct NotarizeConfig {
    /// Binary-name / basename globs selecting which produced artifacts to
    /// notarize (`*` is the only wildcard). An entry matches an artifact when
    /// it globs the artifact's `binary` field *or* its file basename. When
    /// empty, every produced artifact with a notarizable extension is taken.
    artifacts: Vec<String>,
}

/// `notarize` — Apple notarization + stapling. See module docs.
#[derive(Debug, Default)]
pub struct NotarizeProvider {
    /// `xcrun` binary; overridable for tests / non-default toolchains.
    xcrun_bin: Option<String>,
}

impl NotarizeProvider {
    /// Path to the `xcrun` binary (defaults to `xcrun` on `PATH`).
    fn xcrun(&self) -> &str {
        self.xcrun_bin.as_deref().unwrap_or("xcrun")
    }
}

#[async_trait]
impl ReleaseProvider for NotarizeProvider {
    fn name(&self) -> &str {
        "notarize"
    }

    fn required_slots(&self) -> Vec<&str> {
        vec![SLOT_KEY_ID, SLOT_ISSUER, SLOT_KEY_P8]
    }

    async fn dispatch(&self, ctx: &ProviderContext<'_>) -> Result<ProviderReport, RunnerError> {
        let cfg: NotarizeConfig = parse_config(ctx.config)?;

        // Credentials are required even to build the action plan — a dry run
        // is the plan-time credential-presence check.
        let key_id = ctx.require_secret(SLOT_KEY_ID)?;
        let issuer = ctx.require_secret(SLOT_ISSUER)?;
        let key_p8 = ctx.require_secret(SLOT_KEY_P8)?;

        let selected = select_artifacts(ctx.artifacts, &cfg.artifacts)?;

        let mut report = ProviderReport::default();

        if ctx.dry_run {
            for art in &selected {
                report.actions.push(format!(
                    "would submit {} to notarytool (key-id {}) + staple",
                    basename(&art.path),
                    key_id,
                ));
            }
            return Ok(report);
        }

        // Live path: write the .p8 once (0600, never logged) and reuse it for
        // every submission.
        let key_path = write_p8_key(ctx.work_dir, &key_id, &key_p8)?;

        for art in selected {
            notarize_one(self.xcrun(), &art, &key_path, &key_id, &issuer).await?;
            staple_one(self.xcrun(), &art).await?;
            report
                .actions
                .push(format!("notarized + stapled {}", basename(&art.path)));
            // Stapling mutates in place — the path is unchanged, but threading
            // the artifact through `produced` is how a downstream ship outcome
            // (sparkle) addresses the now-stapled bundle.
            report.produced.push(art);
        }

        Ok(report)
    }
}

/// Deserialize the opaque `with` blob into [`NotarizeConfig`]. A `null` config
/// (no `with` table) is the empty default — notarize every notarizable bundle.
fn parse_config(value: &serde_json::Value) -> Result<NotarizeConfig, RunnerError> {
    if value.is_null() {
        return Ok(NotarizeConfig::default());
    }
    serde_json::from_value(value.clone())
        .map_err(|e| RunnerError::Outcome(format!("notarize: invalid `with` config: {e}")))
}

/// Pick the artifacts to notarize: the config globs filtered to notarizable
/// extensions, or — when no globs are given — every notarizable artifact.
/// Errors when the selection is empty (a notarize outcome that processes
/// nothing is a config bug, not a no-op).
fn select_artifacts(
    artifacts: &[ProducedArtifact],
    globs: &[String],
) -> Result<Vec<ProducedArtifact>, RunnerError> {
    let selected: Vec<ProducedArtifact> = artifacts
        .iter()
        .filter(|a| is_notarizable(&a.path))
        .filter(|a| globs.is_empty() || globs.iter().any(|g| artifact_matches(a, g)))
        .cloned()
        .collect();

    if selected.is_empty() {
        let detail = if globs.is_empty() {
            format!(
                "no notarizable artifact (.app/.dmg/.pkg/.zip) among {} produced",
                artifacts.len()
            )
        } else {
            format!(
                "no notarizable artifact matched config globs {globs:?} (of {} produced)",
                artifacts.len()
            )
        };
        return Err(RunnerError::Outcome(format!("notarize: {detail}")));
    }
    Ok(selected)
}

/// Whether `path` has a notarizable extension (case-insensitive).
fn is_notarizable(path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| NOTARIZABLE_EXTS.iter().any(|n| e.eq_ignore_ascii_case(n)))
        .unwrap_or(false)
}

/// A config glob matches an artifact when it globs the `binary` field or the
/// file basename.
fn artifact_matches(art: &ProducedArtifact, glob: &str) -> bool {
    glob_match(glob, &art.binary) || glob_match(glob, basename(&art.path))
}

/// File basename of a path (the part after the last `/`).
fn basename(path: &str) -> &str {
    Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(path)
}

/// Minimal glob matcher — `*` matches any run (including empty); every other
/// char is literal. Sufficient for `desktop`, `*.dmg`, `yah-*` style filters.
fn glob_match(pattern: &str, text: &str) -> bool {
    // Classic two-pointer wildcard match with backtracking on `*`.
    let (p, t): (Vec<char>, Vec<char>) = (pattern.chars().collect(), text.chars().collect());
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut star_t): (Option<usize>, usize) = (None, 0);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            star_t = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            star_t += 1;
            ti = star_t;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// Submit one bundle to the notary service and block until the ticket lands.
/// `notarytool submit --wait` exits non-zero when the ticket is rejected.
async fn notarize_one(
    xcrun: &str,
    art: &ProducedArtifact,
    key_path: &Path,
    key_id: &str,
    issuer: &str,
) -> Result<(), RunnerError> {
    let out = tokio::process::Command::new(xcrun)
        .arg("notarytool")
        .arg("submit")
        .arg(&art.path)
        .arg("--key")
        .arg(key_path)
        .arg("--key-id")
        .arg(key_id)
        .arg("--issuer")
        .arg(issuer)
        .arg("--wait")
        .output()
        .await
        .map_err(|e| RunnerError::Outcome(format!("notarize: spawning `{xcrun} notarytool`: {e}")))?;
    if !out.status.success() {
        return Err(RunnerError::Outcome(format!(
            "notarize: notarytool rejected {} (status {}): {}",
            basename(&art.path),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim(),
        )));
    }
    Ok(())
}

/// Staple the notarization ticket into the bundle so it validates offline.
async fn staple_one(xcrun: &str, art: &ProducedArtifact) -> Result<(), RunnerError> {
    let out = tokio::process::Command::new(xcrun)
        .arg("stapler")
        .arg("staple")
        .arg(&art.path)
        .output()
        .await
        .map_err(|e| RunnerError::Outcome(format!("notarize: spawning `{xcrun} stapler`: {e}")))?;
    if !out.status.success() {
        return Err(RunnerError::Outcome(format!(
            "notarize: stapler failed on {} (status {}): {}",
            basename(&art.path),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim(),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::MapSecrets;
    use std::collections::BTreeMap;

    fn art(binary: &str, path: &str) -> ProducedArtifact {
        ProducedArtifact {
            binary: binary.into(),
            path: path.into(),
            triple: Some("darwin-aarch64".into()),
        }
    }

    fn full_secrets() -> MapSecrets {
        let mut m = BTreeMap::new();
        m.insert(SLOT_KEY_ID.into(), "ABC123".into());
        m.insert(SLOT_ISSUER.into(), "issuer-uuid".into());
        m.insert(SLOT_KEY_P8.into(), "-----BEGIN PRIVATE KEY-----\nx\n".into());
        MapSecrets(m)
    }

    fn ctx<'a>(
        secrets: &'a dyn crate::provider::SecretSource,
        work: &'a Path,
        cfg: &'a serde_json::Value,
        artifacts: &'a [ProducedArtifact],
        dry_run: bool,
    ) -> ProviderContext<'a> {
        ProviderContext {
            version: "1.2.3",
            artifacts,
            base_url: None,
            config: cfg,
            work_dir: work,
            secrets,
            dry_run,
        }
    }

    #[test]
    fn declares_apple_api_key_slots() {
        let p = NotarizeProvider::default();
        assert_eq!(p.name(), "notarize");
        assert_eq!(
            p.required_slots(),
            vec![SLOT_KEY_ID, SLOT_ISSUER, SLOT_KEY_P8]
        );
    }

    #[tokio::test]
    async fn dry_run_plans_per_artifact_without_spawning_xcrun() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let artifacts = vec![art("desktop", "out/Desktop.dmg"), art("yah", "out/yah")];
        let cfg = serde_json::Value::Null;
        let report = NotarizeProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap();
        // Only the .dmg is notarizable; the bare `yah` binary is skipped.
        assert_eq!(report.actions.len(), 1);
        assert!(report.actions[0].contains("would submit Desktop.dmg"));
        assert!(report.produced.is_empty(), "dry run mutates nothing");
        assert!(report.published.is_empty());
        // No .p8 written on the dry-run path.
        assert!(!work.path().join("AuthKey_ABC123.p8").exists());
    }

    #[tokio::test]
    async fn dry_run_missing_slot_is_typed_error_naming_slot() {
        let work = tempfile::tempdir().unwrap();
        let mut m = BTreeMap::new();
        m.insert(SLOT_KEY_ID.into(), "ABC123".into());
        m.insert(SLOT_ISSUER.into(), "issuer-uuid".into());
        // APPLE_API_KEY_P8 missing.
        let secrets = MapSecrets(m);
        let artifacts = vec![art("desktop", "out/Desktop.dmg")];
        let cfg = serde_json::Value::Null;
        let err = NotarizeProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains(SLOT_KEY_P8), "names the slot: {err}");
    }

    #[tokio::test]
    async fn no_notarizable_artifact_is_a_config_error() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let artifacts = vec![art("yah", "out/yah"), art("camp", "out/camp")];
        let cfg = serde_json::Value::Null;
        let err = NotarizeProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no notarizable artifact"));
    }

    #[tokio::test]
    async fn config_glob_filters_to_named_bundle() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let artifacts = vec![
            art("desktop", "out/Desktop.dmg"),
            art("helper", "out/Helper.pkg"),
        ];
        let cfg = serde_json::json!({ "artifacts": ["desktop"] });
        let report = NotarizeProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap();
        assert_eq!(report.actions.len(), 1);
        assert!(report.actions[0].contains("Desktop.dmg"));
    }

    #[tokio::test]
    async fn config_glob_with_no_match_errors() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let artifacts = vec![art("desktop", "out/Desktop.dmg")];
        let cfg = serde_json::json!({ "artifacts": ["nonexistent-*"] });
        let err = NotarizeProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no notarizable artifact matched"));
    }

    #[test]
    fn glob_match_supports_star() {
        assert!(glob_match("*.dmg", "Desktop.dmg"));
        assert!(glob_match("desktop", "desktop"));
        assert!(glob_match("yah-*", "yah-helper"));
        assert!(glob_match("*", "anything"));
        assert!(!glob_match("*.dmg", "Desktop.pkg"));
        assert!(!glob_match("desktop", "desktop-helper"));
    }

    #[test]
    fn notarizable_extension_is_case_insensitive() {
        assert!(is_notarizable("App.DMG"));
        assert!(is_notarizable("Foo.app"));
        assert!(is_notarizable("Bar.pkg"));
        assert!(!is_notarizable("yah"));
        assert!(!is_notarizable("notes.txt"));
    }
}
