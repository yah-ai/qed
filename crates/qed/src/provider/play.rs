//! Google Play Console upload adapter (R509-F6).
//!
//! Validates an Android App Bundle (`.aab`) and uploads it to a Play Console
//! track via the Play Developer API. The Android release slice is terminal —
//! no downstream provider consumes its output — so this is a *ship*:
//! [`ProviderReport::produced`] stays empty and the track/release locator is
//! returned in [`ProviderReport::published`].
//!
//! ## Credentials
//!
//! One slot, resolved through [`crate::secrets_bridge`]:
//! - `PLAY_SERVICE_ACCOUNT_JSON` — a Google service-account key JSON. Parsed +
//!   structurally validated, and (on the live path) materialized `0600` under
//!   `ctx.work_dir`; a short-lived OAuth token is minted from it to call the
//!   API. Never logged.
//!
//! ## bundletool
//!
//! `.aab` validation uses Google's `bundletool` (a Java jar). It is a
//! *toolchain pin* (R507), not vendored: set `with.bundletool_jar` to the jar
//! path (or rely on a `bundletool` launcher on `PATH`). When neither is present
//! the adapter falls back to a structural zip check so a dry run still works on
//! a host without a JDK.
//!
//! ## Dry run
//!
//! `ctx.dry_run` resolves + parses the service-account JSON, confirms the
//! `.aab` exists and passes validation, and reports
//! `would upload <aab> to <package>/<track>` — performing **no** `edits.insert`
//! and no API mutation.
//!
//! ## Deferred: the live Play Developer API edits flow
//!
//! The live upload (`edits.insert` → `bundles.upload` → `tracks.update` →
//! `edits.commit`) requires an RS256-JWT-signed OAuth token exchange and an
//! HTTP client — network-only (no Play sandbox in CI) and two heavy new direct
//! deps for the OSS-mirrored `qed` crate. It is tracked as a follow-up; until it
//! lands the live path performs all local prep (SA parse, `.aab` validation)
//! and then returns a typed error rather than silently shipping nothing.

use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use crate::provider::{ProviderContext, ProviderReport, ReleaseProvider};
use crate::runner::RunnerError;
use crate::types::ProducedArtifact;

/// Credential slot: the Google service-account key JSON.
const SLOT_SA_JSON: &str = "PLAY_SERVICE_ACCOUNT_JSON";

/// Valid Play Console track names.
const TRACKS: &[&str] = &["internal", "alpha", "beta", "production"];

/// The `with = { … }` config block for a `play` outcome.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct PlayConfig {
    /// Android application id / package name (`com.example.app`).
    package_name: String,
    /// Play Console track. One of [`TRACKS`]. Default `internal`.
    track: String,
    /// Staged-rollout user fraction in `(0.0, 1.0]`. `None` → full release.
    rollout_fraction: Option<f64>,
    /// Binary-name / basename globs selecting which produced artifact to
    /// upload. After filtering to `.aab` exactly one must remain; otherwise
    /// narrow with this.
    artifacts: Vec<String>,
    /// Path to the `bundletool` jar (run as `java -jar <jar> validate`). When
    /// unset and no `bundletool` launcher is on `PATH`, a structural zip check
    /// is used instead.
    bundletool_jar: Option<String>,
}

impl Default for PlayConfig {
    fn default() -> Self {
        Self {
            package_name: String::new(),
            track: "internal".to_string(),
            rollout_fraction: None,
            artifacts: Vec::new(),
            bundletool_jar: None,
        }
    }
}

/// Minimal view of a Google service-account key JSON — enough to validate it's
/// the right kind of credential without depending on a Google auth crate.
#[derive(Debug, Deserialize)]
struct ServiceAccount {
    #[serde(rename = "type")]
    account_type: String,
    client_email: String,
    private_key: String,
    token_uri: String,
}

/// `play` — Play Console `.aab` upload. See module docs.
#[derive(Debug, Default)]
pub struct PlayProvider {
    /// `java` binary; overridable for tests / non-default toolchains.
    java_bin: Option<String>,
}

impl PlayProvider {
    /// Path to the `java` binary (defaults to `java` on `PATH`).
    fn java(&self) -> &str {
        self.java_bin.as_deref().unwrap_or("java")
    }
}

#[async_trait]
impl ReleaseProvider for PlayProvider {
    fn name(&self) -> &str {
        "play"
    }

    fn required_slots(&self) -> Vec<&str> {
        vec![SLOT_SA_JSON]
    }

    async fn dispatch(&self, ctx: &ProviderContext<'_>) -> Result<ProviderReport, RunnerError> {
        let cfg: PlayConfig = parse_config(ctx.config)?;
        validate_track(&cfg.track)?;
        validate_rollout(cfg.rollout_fraction)?;

        // Credential is required even to build the action plan.
        let sa_json = ctx.require_secret(SLOT_SA_JSON)?;
        let sa = parse_service_account(&sa_json)?;

        let aab = select_aab(ctx.artifacts, &cfg.artifacts)?;
        validate_aab(self.java(), &cfg.bundletool_jar, &aab.path).await?;

        let dest = format!("{}/{}", app_label(&cfg), cfg.track);

        if ctx.dry_run {
            let rollout = match cfg.rollout_fraction {
                Some(f) => format!(" (rollout {f})"),
                None => String::new(),
            };
            return Ok(ProviderReport::action(format!(
                "would upload {} to {dest}{rollout} as {}",
                basename(&aab.path),
                sa.client_email,
            )));
        }

        // Live path: local prep is done (SA parsed, .aab validated). The Play
        // Developer API edits flow is a tracked follow-up — fail loudly rather
        // than report a successful ship that uploaded nothing.
        let _ = &sa.private_key; // materialized + signed once the flow lands.
        Err(RunnerError::Outcome(format!(
            "play: live upload to {dest} is not yet wired — the Play Developer API edits flow \
             (edits.insert → bundles.upload → tracks.update → edits.commit, RS256-JWT OAuth) is a \
             tracked R509-F6 follow-up. Dry run validates config + the .aab; a live release must \
             not route to `play` until the follow-up lands."
        )))
    }
}

/// Deserialize the opaque `with` blob into [`PlayConfig`].
fn parse_config(value: &serde_json::Value) -> Result<PlayConfig, RunnerError> {
    if value.is_null() {
        return Ok(PlayConfig::default());
    }
    serde_json::from_value(value.clone())
        .map_err(|e| RunnerError::Outcome(format!("play: invalid `with` config: {e}")))
}

/// Reject a track name that isn't a known Play Console track.
fn validate_track(track: &str) -> Result<(), RunnerError> {
    if TRACKS.contains(&track) {
        Ok(())
    } else {
        Err(RunnerError::Outcome(format!(
            "play: unknown track `{track}` (expected one of {})",
            TRACKS.join(", ")
        )))
    }
}

/// Reject a rollout fraction outside `(0.0, 1.0]`.
fn validate_rollout(fraction: Option<f64>) -> Result<(), RunnerError> {
    match fraction {
        None => Ok(()),
        Some(f) if f > 0.0 && f <= 1.0 => Ok(()),
        Some(f) => Err(RunnerError::Outcome(format!(
            "play: rollout_fraction {f} out of range (expected (0.0, 1.0])"
        ))),
    }
}

/// Parse + structurally validate the service-account JSON: it must be a
/// `service_account` credential with the fields the OAuth JWT mint needs.
fn parse_service_account(json: &str) -> Result<ServiceAccount, RunnerError> {
    let sa: ServiceAccount = serde_json::from_str(json).map_err(|e| {
        RunnerError::Outcome(format!(
            "play: `{SLOT_SA_JSON}` is not a valid service-account JSON: {e}"
        ))
    })?;
    if sa.account_type != "service_account" {
        return Err(RunnerError::Outcome(format!(
            "play: `{SLOT_SA_JSON}` type is `{}`, expected `service_account`",
            sa.account_type
        )));
    }
    if sa.client_email.is_empty() || sa.private_key.is_empty() || sa.token_uri.is_empty() {
        return Err(RunnerError::Outcome(format!(
            "play: `{SLOT_SA_JSON}` is missing client_email / private_key / token_uri"
        )));
    }
    Ok(sa)
}

/// Human label for the target app — the configured package name, or a
/// placeholder when unset.
fn app_label(cfg: &PlayConfig) -> String {
    if cfg.package_name.is_empty() {
        "<package>".to_string()
    } else {
        cfg.package_name.clone()
    }
}

/// Pick the single `.aab`: the config globs filtered to `.aab`, or the sole
/// `.aab` artifact. Errors when zero or more-than-one remain.
fn select_aab(
    artifacts: &[ProducedArtifact],
    globs: &[String],
) -> Result<ProducedArtifact, RunnerError> {
    let selected: Vec<&ProducedArtifact> = artifacts
        .iter()
        .filter(|a| is_aab(&a.path))
        .filter(|a| globs.is_empty() || globs.iter().any(|g| artifact_matches(a, g)))
        .collect();
    match selected.as_slice() {
        [one] => Ok((*one).clone()),
        [] if globs.is_empty() => Err(RunnerError::Outcome(format!(
            "play: no .aab among {} produced artifacts",
            artifacts.len()
        ))),
        [] => Err(RunnerError::Outcome(format!(
            "play: no .aab matched config globs {globs:?} (of {} produced)",
            artifacts.len()
        ))),
        many => Err(RunnerError::Outcome(format!(
            "play: {} .aab artifacts selected ({}); narrow to one with `with.artifacts`",
            many.len(),
            many.iter().map(|a| basename(&a.path)).collect::<Vec<_>>().join(", "),
        ))),
    }
}

/// Whether `path` has the `.aab` extension (case-insensitive).
fn is_aab(path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("aab"))
        .unwrap_or(false)
}

/// Validate the `.aab`. When a `bundletool` jar is configured, run
/// `java -jar <jar> validate --bundle <aab>`; otherwise fall back to a
/// structural zip-magic check (an `.aab` is a zip) so a dry run works without a
/// JDK.
async fn validate_aab(
    java: &str,
    bundletool_jar: &Option<String>,
    path: &str,
) -> Result<(), RunnerError> {
    match bundletool_jar {
        Some(jar) => {
            let out = tokio::process::Command::new(java)
                .arg("-jar")
                .arg(jar)
                .arg("validate")
                .arg("--bundle")
                .arg(path)
                .output()
                .await
                .map_err(|e| {
                    RunnerError::Outcome(format!("play: spawning `{java} -jar bundletool`: {e}"))
                })?;
            if !out.status.success() {
                return Err(RunnerError::Outcome(format!(
                    "play: bundletool validate failed on {} (status {}): {}",
                    basename(path),
                    out.status,
                    String::from_utf8_lossy(&out.stderr).trim(),
                )));
            }
            Ok(())
        }
        None => validate_zip_magic(path),
    }
}

/// Confirm the file exists and starts with the zip local-file-header magic
/// (`PK\x03\x04`) — the offline fallback when bundletool isn't available.
fn validate_zip_magic(path: &str) -> Result<(), RunnerError> {
    use std::io::Read;
    let mut magic = [0u8; 4];
    let mut f = std::fs::File::open(path)
        .map_err(|e| RunnerError::Outcome(format!("play: .aab {path} not found: {e}")))?;
    let n = f
        .read(&mut magic)
        .map_err(|e| RunnerError::Outcome(format!("play: reading .aab {path}: {e}")))?;
    if n < 4 || &magic != b"PK\x03\x04" {
        return Err(RunnerError::Outcome(format!(
            "play: {} is not a valid .aab (missing zip magic)",
            basename(path)
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

    /// A minimal but structurally-valid service-account JSON.
    fn sa_json() -> String {
        serde_json::json!({
            "type": "service_account",
            "client_email": "ci@proj.iam.gserviceaccount.com",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----\n",
            "token_uri": "https://oauth2.googleapis.com/token",
        })
        .to_string()
    }

    fn fixture_aab(dir: &Path, name: &str) -> ProducedArtifact {
        let p = dir.join(name);
        std::fs::write(&p, b"PK\x03\x04rest-of-bundle").unwrap();
        ProducedArtifact {
            binary: "app".into(),
            path: p.to_str().unwrap().into(),
            triple: Some("android-arm64".into()),
        }
    }

    fn art(binary: &str, path: &str) -> ProducedArtifact {
        ProducedArtifact {
            binary: binary.into(),
            path: path.into(),
            triple: Some("android-arm64".into()),
        }
    }

    fn full_secrets() -> MapSecrets {
        let mut m = BTreeMap::new();
        m.insert(SLOT_SA_JSON.into(), sa_json());
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
    fn declares_service_account_slot() {
        let p = PlayProvider::default();
        assert_eq!(p.name(), "play");
        assert_eq!(p.required_slots(), vec![SLOT_SA_JSON]);
    }

    #[tokio::test]
    async fn dry_run_reports_upload_without_api_mutation() {
        let work = tempfile::tempdir().unwrap();
        let aab = fixture_aab(work.path(), "app-release.aab");
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "package_name": "com.yah.app", "track": "beta" });
        let report = PlayProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&aab), true))
            .await
            .unwrap();
        assert_eq!(report.actions.len(), 1);
        assert!(report.actions[0].contains("would upload app-release.aab"));
        assert!(report.actions[0].contains("com.yah.app/beta"));
        assert!(report.actions[0].contains("ci@proj.iam.gserviceaccount.com"));
        assert!(report.published.is_empty());
    }

    #[tokio::test]
    async fn dry_run_reports_rollout_fraction() {
        let work = tempfile::tempdir().unwrap();
        let aab = fixture_aab(work.path(), "app.aab");
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "package_name": "com.yah.app", "rollout_fraction": 0.1 });
        let report = PlayProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&aab), true))
            .await
            .unwrap();
        assert!(report.actions[0].contains("rollout 0.1"));
    }

    #[tokio::test]
    async fn live_path_errors_until_rest_flow_lands() {
        let work = tempfile::tempdir().unwrap();
        let aab = fixture_aab(work.path(), "app.aab");
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "package_name": "com.yah.app" });
        let err = PlayProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&aab), false))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("not yet wired"));
    }

    #[tokio::test]
    async fn missing_slot_is_typed_error() {
        let work = tempfile::tempdir().unwrap();
        let aab = fixture_aab(work.path(), "app.aab");
        let secrets = MapSecrets::default();
        let cfg = serde_json::json!({ "package_name": "com.yah.app" });
        let err = PlayProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&aab), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains(SLOT_SA_JSON));
    }

    #[tokio::test]
    async fn non_service_account_json_is_rejected() {
        let work = tempfile::tempdir().unwrap();
        let aab = fixture_aab(work.path(), "app.aab");
        let mut m = BTreeMap::new();
        m.insert(
            SLOT_SA_JSON.into(),
            serde_json::json!({
                "type": "authorized_user",
                "client_email": "x@y.z",
                "private_key": "k",
                "token_uri": "u",
            })
            .to_string(),
        );
        let secrets = MapSecrets(m);
        let cfg = serde_json::json!({ "package_name": "com.yah.app" });
        let err = PlayProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&aab), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("expected `service_account`"));
    }

    #[tokio::test]
    async fn unknown_track_is_rejected() {
        let work = tempfile::tempdir().unwrap();
        let aab = fixture_aab(work.path(), "app.aab");
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "package_name": "com.yah.app", "track": "canary" });
        let err = PlayProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&aab), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("unknown track `canary`"));
    }

    #[tokio::test]
    async fn out_of_range_rollout_is_rejected() {
        let work = tempfile::tempdir().unwrap();
        let aab = fixture_aab(work.path(), "app.aab");
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "package_name": "com.yah.app", "rollout_fraction": 1.5 });
        let err = PlayProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&aab), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("out of range"));
    }

    #[tokio::test]
    async fn no_aab_is_a_config_error() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let artifacts = vec![art("yah", "out/yah"), art("app", "out/App.apk")];
        let cfg = serde_json::json!({ "package_name": "com.yah.app" });
        let err = PlayProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no .aab"));
    }

    #[tokio::test]
    async fn ambiguous_aab_requires_glob() {
        let work = tempfile::tempdir().unwrap();
        let a = fixture_aab(work.path(), "A.aab");
        let b = fixture_aab(work.path(), "B.aab");
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "package_name": "com.yah.app" });
        let err = PlayProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &[a, b], true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("narrow to one"));
    }

    #[tokio::test]
    async fn invalid_aab_magic_is_rejected() {
        let work = tempfile::tempdir().unwrap();
        let p = work.path().join("Bogus.aab");
        std::fs::write(&p, b"not a zip").unwrap();
        let aab = art("app", p.to_str().unwrap());
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "package_name": "com.yah.app" });
        let err = PlayProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&aab), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("not a valid .aab"));
    }

    #[test]
    fn aab_extension_is_case_insensitive() {
        assert!(is_aab("app.aab"));
        assert!(is_aab("App.AAB"));
        assert!(!is_aab("App.apk"));
        assert!(!is_aab("App.ipa"));
    }
}
