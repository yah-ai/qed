//! TestFlight / App Store Connect upload adapter (R509-F5).
//!
//! Uploads a built `.ipa` to App Store Connect (TestFlight) via
//! `xcrun altool --upload-app`, authenticating with the App Store Connect API
//! key family shared with the `notarize` adapter (R509-F1). On success the
//! build link / locator is returned in [`ProviderReport::published`].
//!
//! TestFlight is a *ship*, not a transform — it mutates no artifact, so
//! [`ProviderReport::produced`] stays empty.
//!
//! ## Credentials
//!
//! The three App Store Connect API key slots from [`crate::provider::apple`]:
//! - `APPLE_API_KEY_ID` — the key id (`--apiKey`).
//! - `APPLE_API_ISSUER` — the issuer UUID (`--apiIssuer`).
//! - `APPLE_API_KEY_P8` — the `.p8` contents, materialized to a `0600` file in
//!   `ctx.work_dir`; `altool` resolves it from `API_PRIVATE_KEYS_DIR`.
//!
//! ## Dry run
//!
//! `ctx.dry_run` resolves the three slots, confirms the `.ipa` artifact exists
//! and is a structurally valid IPA (a zip — `PK` magic), and reports
//! `would upload <ipa> to app <bundle-id>` — spawning no `altool` and
//! submitting nothing. Apple's `altool --validate-app` is a *network* call, so
//! it is deliberately not part of the dry run.
//!
//! ## Deferred: server-side processing poll
//!
//! `altool --upload-app` blocks until Apple *receives* the binary; TestFlight
//! then processes it asynchronously. Observing that processing state (and
//! resolving the precise build URL) requires the App Store Connect REST API —
//! an ES256-JWT-signed HTTP client. That's network-only (untestable in CI) and
//! a heavy dependency for this crate, so it is left as a follow-up; this
//! adapter returns a bundle-id\@version locator noting the build is processing.

use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use crate::provider::apple::{write_p8_key, SLOT_ISSUER, SLOT_KEY_ID, SLOT_KEY_P8};
use crate::provider::{ProviderContext, ProviderReport, ReleaseProvider};
use crate::runner::RunnerError;
use crate::types::ProducedArtifact;

/// The `with = { … }` config block for a `testflight` outcome.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct TestFlightConfig {
    /// App bundle identifier (`com.example.App`) — used in the upload action
    /// log and the published locator.
    bundle_id: String,
    /// Binary-name / basename globs selecting which produced artifact to
    /// upload. After filtering to `.ipa` exactly one must remain; otherwise
    /// narrow with this. Empty → the sole `.ipa` among the artifacts.
    artifacts: Vec<String>,
    /// Platform passed to `altool -t` (`ios`, `tvos`, `osx`). Default `ios`.
    #[serde(default = "default_platform")]
    platform: String,
}

fn default_platform() -> String {
    "ios".to_string()
}

/// `testflight` — App Store Connect `.ipa` upload. See module docs.
#[derive(Debug, Default)]
pub struct TestFlightProvider {
    /// `xcrun` binary; overridable for tests / non-default toolchains.
    xcrun_bin: Option<String>,
}

impl TestFlightProvider {
    /// Path to the `xcrun` binary (defaults to `xcrun` on `PATH`).
    fn xcrun(&self) -> &str {
        self.xcrun_bin.as_deref().unwrap_or("xcrun")
    }
}

#[async_trait]
impl ReleaseProvider for TestFlightProvider {
    fn name(&self) -> &str {
        "testflight"
    }

    fn required_slots(&self) -> Vec<&str> {
        vec![SLOT_KEY_ID, SLOT_ISSUER, SLOT_KEY_P8]
    }

    async fn dispatch(&self, ctx: &ProviderContext<'_>) -> Result<ProviderReport, RunnerError> {
        let cfg: TestFlightConfig = parse_config(ctx.config)?;

        // Credentials required even to build the action plan.
        let key_id = ctx.require_secret(SLOT_KEY_ID)?;
        let issuer = ctx.require_secret(SLOT_ISSUER)?;
        let key_p8 = ctx.require_secret(SLOT_KEY_P8)?;

        let ipa = select_ipa(ctx.artifacts, &cfg.artifacts)?;

        // Structural IPA check (an IPA is a zip) — confirms the artifact exists
        // and is plausibly an IPA without a network round-trip.
        validate_ipa(&ipa.path)?;

        let app = app_label(&cfg);

        if ctx.dry_run {
            return Ok(ProviderReport::action(format!(
                "would upload {} to app {app} via altool (key-id {key_id})",
                basename(&ipa.path),
            )));
        }

        // Live: materialize the .p8 (0600) and point altool at its dir.
        let key_path = write_p8_key(ctx.work_dir, &key_id, &key_p8)?;
        let key_dir = key_path
            .parent()
            .ok_or_else(|| RunnerError::Outcome("testflight: key path has no parent dir".into()))?;

        upload(self.xcrun(), &ipa, &cfg.platform, &key_id, &issuer, key_dir).await?;

        Ok(ProviderReport {
            actions: vec![format!("uploaded {} to {app}", basename(&ipa.path))],
            produced: Vec::new(),
            // A precise build URL needs the ASC REST API (deferred — see module
            // docs); the locator records the build that's now processing.
            published: vec![format!("testflight:{app} (processing)")],
        })
    }
}

/// Deserialize the opaque `with` blob into [`TestFlightConfig`].
fn parse_config(value: &serde_json::Value) -> Result<TestFlightConfig, RunnerError> {
    if value.is_null() {
        return Ok(TestFlightConfig {
            platform: default_platform(),
            ..Default::default()
        });
    }
    serde_json::from_value(value.clone())
        .map_err(|e| RunnerError::Outcome(format!("testflight: invalid `with` config: {e}")))
}

/// Human label for the target app — the configured bundle-id, or a placeholder
/// when unset (altool derives the app from the IPA's own bundle-id).
fn app_label(cfg: &TestFlightConfig) -> String {
    if cfg.bundle_id.is_empty() {
        "<ipa bundle-id>".to_string()
    } else {
        cfg.bundle_id.clone()
    }
}

/// Pick the single `.ipa`: the config globs filtered to `.ipa`, or the sole
/// `.ipa` artifact. Errors when zero or more-than-one remain.
fn select_ipa(
    artifacts: &[ProducedArtifact],
    globs: &[String],
) -> Result<ProducedArtifact, RunnerError> {
    let selected: Vec<&ProducedArtifact> = artifacts
        .iter()
        .filter(|a| is_ipa(&a.path))
        .filter(|a| globs.is_empty() || globs.iter().any(|g| artifact_matches(a, g)))
        .collect();
    match selected.as_slice() {
        [one] => Ok((*one).clone()),
        [] if globs.is_empty() => Err(RunnerError::Outcome(format!(
            "testflight: no .ipa among {} produced artifacts",
            artifacts.len()
        ))),
        [] => Err(RunnerError::Outcome(format!(
            "testflight: no .ipa matched config globs {globs:?} (of {} produced)",
            artifacts.len()
        ))),
        many => Err(RunnerError::Outcome(format!(
            "testflight: {} .ipa artifacts selected ({}); narrow to one with `with.artifacts`",
            many.len(),
            many.iter().map(|a| basename(&a.path)).collect::<Vec<_>>().join(", "),
        ))),
    }
}

/// Whether `path` has the `.ipa` extension (case-insensitive).
fn is_ipa(path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("ipa"))
        .unwrap_or(false)
}

/// Confirm the `.ipa` exists and starts with the zip local-file-header magic
/// (`PK\x03\x04`) — a cheap, offline sanity check that it's a real IPA.
fn validate_ipa(path: &str) -> Result<(), RunnerError> {
    let mut magic = [0u8; 4];
    use std::io::Read;
    let mut f = std::fs::File::open(path)
        .map_err(|e| RunnerError::Outcome(format!("testflight: .ipa {path} not found: {e}")))?;
    let n = f
        .read(&mut magic)
        .map_err(|e| RunnerError::Outcome(format!("testflight: reading .ipa {path}: {e}")))?;
    if n < 4 || &magic != b"PK\x03\x04" {
        return Err(RunnerError::Outcome(format!(
            "testflight: {} is not a valid IPA (missing zip magic)",
            basename(path)
        )));
    }
    Ok(())
}

/// Run `xcrun altool --upload-app`, pointing `altool` at the materialized key
/// via `API_PRIVATE_KEYS_DIR`. Blocks until Apple receives the binary.
async fn upload(
    xcrun: &str,
    ipa: &ProducedArtifact,
    platform: &str,
    key_id: &str,
    issuer: &str,
    key_dir: &Path,
) -> Result<(), RunnerError> {
    let out = tokio::process::Command::new(xcrun)
        .arg("altool")
        .arg("--upload-app")
        .arg("-f")
        .arg(&ipa.path)
        .arg("-t")
        .arg(platform)
        .arg("--apiKey")
        .arg(key_id)
        .arg("--apiIssuer")
        .arg(issuer)
        .env("API_PRIVATE_KEYS_DIR", key_dir)
        .output()
        .await
        .map_err(|e| RunnerError::Outcome(format!("testflight: spawning `{xcrun} altool`: {e}")))?;
    if !out.status.success() {
        return Err(RunnerError::Outcome(format!(
            "testflight: altool rejected {} (status {}): {}",
            basename(&ipa.path),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim(),
        )));
    }
    Ok(())
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
/// char is literal.
fn glob_match(pattern: &str, text: &str) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::MapSecrets;
    use std::collections::BTreeMap;

    /// Write a fixture `.ipa` (zip magic + padding) and return an artifact.
    fn fixture_ipa(dir: &Path, name: &str) -> ProducedArtifact {
        let p = dir.join(name);
        std::fs::write(&p, b"PK\x03\x04rest-of-zip").unwrap();
        ProducedArtifact {
            binary: "app".into(),
            path: p.to_str().unwrap().into(),
            triple: Some("ios-arm64".into()),
        }
    }

    fn art(binary: &str, path: &str) -> ProducedArtifact {
        ProducedArtifact {
            binary: binary.into(),
            path: path.into(),
            triple: Some("ios-arm64".into()),
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
        let p = TestFlightProvider::default();
        assert_eq!(p.name(), "testflight");
        assert_eq!(p.required_slots(), vec![SLOT_KEY_ID, SLOT_ISSUER, SLOT_KEY_P8]);
    }

    #[tokio::test]
    async fn dry_run_reports_upload_without_spawning_altool() {
        let work = tempfile::tempdir().unwrap();
        let ipa = fixture_ipa(work.path(), "App.ipa");
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "bundle_id": "com.yah.app" });
        let report = TestFlightProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&ipa), true))
            .await
            .unwrap();
        assert_eq!(report.actions.len(), 1);
        assert!(report.actions[0].contains("would upload App.ipa"));
        assert!(report.actions[0].contains("com.yah.app"));
        assert!(report.published.is_empty(), "dry run ships nothing");
        // No .p8 written on the dry-run path.
        assert!(!work.path().join("AuthKey_ABC123.p8").exists());
    }

    #[tokio::test]
    async fn dry_run_missing_slot_is_typed_error_naming_slot() {
        let work = tempfile::tempdir().unwrap();
        let ipa = fixture_ipa(work.path(), "App.ipa");
        let mut m = BTreeMap::new();
        m.insert(SLOT_KEY_ID.into(), "ABC123".to_string());
        m.insert(SLOT_ISSUER.into(), "issuer-uuid".to_string());
        // APPLE_API_KEY_P8 missing.
        let secrets = MapSecrets(m);
        let cfg = serde_json::Value::Null;
        let err = TestFlightProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&ipa), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains(SLOT_KEY_P8), "names the slot: {err}");
    }

    #[tokio::test]
    async fn no_ipa_is_a_config_error() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let artifacts = vec![art("yah", "out/yah"), art("desktop", "out/Desktop.dmg")];
        let cfg = serde_json::Value::Null;
        let err = TestFlightProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no .ipa"));
    }

    #[tokio::test]
    async fn ambiguous_ipa_requires_glob() {
        let work = tempfile::tempdir().unwrap();
        let a = fixture_ipa(work.path(), "A.ipa");
        let b = fixture_ipa(work.path(), "B.ipa");
        let secrets = full_secrets();
        let cfg = serde_json::Value::Null;
        let err = TestFlightProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &[a, b], true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("narrow to one"));
    }

    #[tokio::test]
    async fn config_glob_selects_named_ipa() {
        let work = tempfile::tempdir().unwrap();
        let a = fixture_ipa(work.path(), "App.ipa");
        let b = fixture_ipa(work.path(), "Helper.ipa");
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "artifacts": ["App.ipa"] });
        let report = TestFlightProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &[a, b], true))
            .await
            .unwrap();
        assert!(report.actions[0].contains("App.ipa"));
    }

    #[tokio::test]
    async fn invalid_ipa_magic_is_an_error() {
        let work = tempfile::tempdir().unwrap();
        let p = work.path().join("Bogus.ipa");
        std::fs::write(&p, b"not a zip").unwrap();
        let ipa = art("app", p.to_str().unwrap());
        let secrets = full_secrets();
        let cfg = serde_json::Value::Null;
        let err = TestFlightProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&ipa), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("not a valid IPA"));
    }

    #[test]
    fn ipa_extension_is_case_insensitive() {
        assert!(is_ipa("App.ipa"));
        assert!(is_ipa("App.IPA"));
        assert!(!is_ipa("App.dmg"));
        assert!(!is_ipa("App.apk"));
    }

    #[test]
    fn platform_defaults_to_ios() {
        let cfg = parse_config(&serde_json::Value::Null).unwrap();
        assert_eq!(cfg.platform, "ios");
        let cfg2 = parse_config(&serde_json::json!({ "platform": "tvos" })).unwrap();
        assert_eq!(cfg2.platform, "tvos");
    }
}
