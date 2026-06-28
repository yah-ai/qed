//! WinSparkle (Windows) appcast adapter (R509-F4).
//!
//! WinSparkle is the Windows port of Sparkle and reads the **same appcast.xml
//! shape** (see [`crate::provider::appcast`]), so this adapter reuses the shared
//! appcast builder and produces a feed over the Authenticode-signed installer
//! (`.exe` / `.msi`) that the `authenticode` outcome (R509-F2) signed upstream.
//!
//! ## Trust model & signing
//!
//! In its classic flow WinSparkle trusts the installer's **Authenticode**
//! signature — Windows verifies it on launch — so the appcast needs no separate
//! enclosure signature. This adapter therefore declares the Authenticode
//! credential slots as [`required_slots`](ReleaseProvider::required_slots): a
//! plan-time *coherence guard* that the release is configured to produce signed
//! installers (the values aren't read here — the installer arrives already
//! signed by the upstream outcome).
//!
//! A pipeline can additionally opt into an **appcast-level** EdDSA signature
//! (modern WinSparkle ≥ 0.7, same `sparkle:edSignature` as Sparkle) by setting
//! `with.sign_appcast = true`; the adapter then signs the installer bytes with
//! the ed25519 key in `with.ed_key_slot` (default `WINSPARKLE_ED_PRIVATE_KEY`).
//! The legacy DSA (`sparkle:dsaSignature`) flow is intentionally out of scope —
//! ed25519 is WinSparkle's current recommendation.
//!
//! ## Boundary (same as sparkle, R509-F3)
//!
//! Generates + signs only; the appcast XML is returned in
//! [`ProviderReport::produced`] for the channel sync to ship, and the feed URL
//! in [`ProviderReport::published`] as the locator.
//!
//! ## Dry run
//!
//! Confirms the installer artifact exists and (when `sign_appcast`) that the ed
//! key resolves, then reports the appcast entry it *would* emit — writing no
//! files and publishing nothing.

use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use crate::provider::appcast::{render_appcast, AppcastEntry, ReleaseNotes};
use crate::provider::edsign::{parse_signing_key, sign_b64, sig_prefix};
use crate::provider::{ProviderContext, ProviderReport, ReleaseProvider};
use crate::runner::RunnerError;
use crate::types::ProducedArtifact;

/// Authenticode credential slots — declared as a coherence guard (the feed is
/// only meaningful over a signed installer). Shared verbatim with the
/// authenticode adapter (R509-F2); the values are not read here.
const SLOT_CERT: &str = "AUTHENTICODE_CERT";
const SLOT_CERT_PASSWORD: &str = "AUTHENTICODE_CERT_PASSWORD";

/// Default slot for the optional appcast-level ed25519 key.
const DEFAULT_ED_KEY_SLOT: &str = "WINSPARKLE_ED_PRIVATE_KEY";

/// Windows installer extensions WinSparkle ships. Matched case-insensitively.
const INSTALLER_EXTS: &[&str] = &["exe", "msi"];

/// The `with = { … }` config block for a `winsparkle` outcome.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct WinSparkleConfig {
    /// Binary-name / basename globs selecting which produced artifact to ship.
    /// After filtering to installer extensions exactly one must remain;
    /// otherwise narrow with this.
    artifacts: Vec<String>,
    /// Public base URL for the enclosure + feed. Falls back to the pipeline's
    /// `base_url`. The enclosure URL is `{base}/{installer-basename}`.
    base_url: Option<String>,
    /// Full appcast feed URL. Defaults to `{base}/appcast.xml`. Returned as the
    /// `published` locator.
    feed_url: Option<String>,
    /// Output appcast filename written into `work_dir`. Default `appcast.xml`.
    appcast_filename: String,
    /// Feed `<channel><title>`. Default `Yah`.
    channel_title: String,
    /// `sparkle:channel` (`stable`, `beta`) for a multi-channel feed.
    channel: Option<String>,
    /// `sparkle:version` — the monotonic build. Defaults to the release version.
    build: Option<String>,
    /// `sparkle:minimumSystemVersion`, when constrained.
    minimum_system_version: Option<String>,
    /// Release-notes URL (`<sparkle:releaseNotesLink>`). Wins over `_html`.
    release_notes_url: Option<String>,
    /// Inline release-notes HTML (`<description><![CDATA[…]]>`).
    release_notes_html: Option<String>,
    /// MIME type for the enclosure. Default `application/octet-stream`.
    mime_type: String,
    /// RFC822 `<pubDate>` (the seam has no clock — pass it explicitly).
    pub_date: Option<String>,
    /// Opt into an appcast-level ed25519 `sparkle:edSignature` over the
    /// installer bytes (modern WinSparkle). Default `false` — trust Authenticode.
    sign_appcast: bool,
    /// Credential slot holding the base64 ed25519 key used when `sign_appcast`.
    /// Default [`DEFAULT_ED_KEY_SLOT`].
    ed_key_slot: String,
}

impl Default for WinSparkleConfig {
    fn default() -> Self {
        Self {
            artifacts: Vec::new(),
            base_url: None,
            feed_url: None,
            appcast_filename: "appcast.xml".to_string(),
            channel_title: "Yah".to_string(),
            channel: None,
            build: None,
            minimum_system_version: None,
            release_notes_url: None,
            release_notes_html: None,
            mime_type: "application/octet-stream".to_string(),
            pub_date: None,
            sign_appcast: false,
            ed_key_slot: DEFAULT_ED_KEY_SLOT.to_string(),
        }
    }
}

/// `winsparkle` — Windows appcast generation. See module docs.
#[derive(Debug, Default)]
pub struct WinSparkleProvider;

#[async_trait]
impl ReleaseProvider for WinSparkleProvider {
    fn name(&self) -> &str {
        "winsparkle"
    }

    fn required_slots(&self) -> Vec<&str> {
        // Coherence guard: the installer must have been Authenticode-signable.
        // The optional ed key (sign_appcast) is a config-conditional runtime
        // requirement, checked in dispatch.
        vec![SLOT_CERT, SLOT_CERT_PASSWORD]
    }

    async fn dispatch(&self, ctx: &ProviderContext<'_>) -> Result<ProviderReport, RunnerError> {
        let cfg: WinSparkleConfig = parse_config(ctx.config)?;

        // Coherence presence check — values unused; the installer arrives
        // already Authenticode-signed by the upstream outcome.
        let _cert = ctx.require_secret(SLOT_CERT)?;
        let _cert_pw = ctx.require_secret(SLOT_CERT_PASSWORD)?;

        let installer = select_installer(ctx.artifacts, &cfg.artifacts)?;
        let base = base_url(&cfg, ctx)?;
        let filename = basename(&installer.path);
        let enclosure_url = join_url(&base, filename);
        let build = cfg.build.clone().unwrap_or_else(|| ctx.version.to_string());

        // Confirm the installer exists and get its length (stat — no full read
        // unless we need the bytes to sign).
        let length = std::fs::metadata(&installer.path)
            .map_err(|e| {
                RunnerError::Outcome(format!(
                    "winsparkle: installer {} not found: {e}",
                    installer.path
                ))
            })?
            .len();

        // Optional appcast-level ed25519 signature over the installer bytes.
        let ed_signature = if cfg.sign_appcast {
            let key_b64 = ctx.require_secret(&cfg.ed_key_slot)?;
            let key = parse_signing_key(&key_b64, &cfg.ed_key_slot)?;
            let bytes = std::fs::read(&installer.path).map_err(|e| {
                RunnerError::Outcome(format!("winsparkle: reading installer to sign: {e}"))
            })?;
            Some(sign_b64(&key, &bytes))
        } else {
            None
        };

        let feed_url = cfg
            .feed_url
            .clone()
            .unwrap_or_else(|| join_url(&base, &cfg.appcast_filename));

        if ctx.dry_run {
            let sig_note = match &ed_signature {
                Some(s) => format!(", edSignature {}…", sig_prefix(s)),
                None => " (trusts Authenticode)".to_string(),
            };
            return Ok(ProviderReport {
                actions: vec![
                    format!(
                        "would emit WinSparkle appcast entry for v{} (build {build}, installer {filename}{sig_note})",
                        ctx.version
                    ),
                    format!("would publish appcast feed at {feed_url}"),
                ],
                ..Default::default()
            });
        }

        let mut entry = AppcastEntry::new(
            format!("Version {}", ctx.version),
            ctx.version,
            &build,
            &enclosure_url,
            length,
        );
        entry.ed_signature = ed_signature;
        entry.mime_type = cfg.mime_type.clone();
        entry.min_system_version = cfg.minimum_system_version.clone();
        entry.channel = cfg.channel.clone();
        entry.pub_date = cfg.pub_date.clone();
        entry.release_notes = release_notes(&cfg);

        let xml = render_appcast(&cfg.channel_title, std::slice::from_ref(&entry));
        let appcast_path = ctx.work_dir.join(&cfg.appcast_filename);
        std::fs::write(&appcast_path, xml).map_err(|e| {
            RunnerError::Outcome(format!(
                "winsparkle: writing appcast {}: {e}",
                cfg.appcast_filename
            ))
        })?;

        Ok(ProviderReport {
            actions: vec![format!(
                "wrote WinSparkle appcast {} for v{}",
                cfg.appcast_filename, ctx.version
            )],
            produced: vec![ProducedArtifact {
                binary: installer.binary.clone(),
                path: appcast_path.to_string_lossy().into_owned(),
                triple: installer.triple.clone(),
            }],
            published: vec![feed_url],
        })
    }
}

/// Deserialize the opaque `with` blob into [`WinSparkleConfig`]. A `null` config
/// is the default.
fn parse_config(value: &serde_json::Value) -> Result<WinSparkleConfig, RunnerError> {
    if value.is_null() {
        return Ok(WinSparkleConfig::default());
    }
    serde_json::from_value(value.clone())
        .map_err(|e| RunnerError::Outcome(format!("winsparkle: invalid `with` config: {e}")))
}

/// Pick the single Windows installer: the config globs filtered to installer
/// extensions, or every installer artifact. Errors when zero or more-than-one
/// remain.
fn select_installer(
    artifacts: &[ProducedArtifact],
    globs: &[String],
) -> Result<ProducedArtifact, RunnerError> {
    let selected: Vec<&ProducedArtifact> = artifacts
        .iter()
        .filter(|a| is_installer(&a.path))
        .filter(|a| globs.is_empty() || globs.iter().any(|g| artifact_matches(a, g)))
        .collect();
    match selected.as_slice() {
        [one] => Ok((*one).clone()),
        [] if globs.is_empty() => Err(RunnerError::Outcome(format!(
            "winsparkle: no Windows installer (.exe/.msi) among {} produced",
            artifacts.len()
        ))),
        [] => Err(RunnerError::Outcome(format!(
            "winsparkle: no installer matched config globs {globs:?} (of {} produced)",
            artifacts.len()
        ))),
        many => Err(RunnerError::Outcome(format!(
            "winsparkle: {} installers selected ({}); narrow to one with `with.artifacts`",
            many.len(),
            many.iter().map(|a| basename(&a.path)).collect::<Vec<_>>().join(", "),
        ))),
    }
}

/// Whether `path` has a Windows-installer extension (case-insensitive).
fn is_installer(path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| INSTALLER_EXTS.iter().any(|n| e.eq_ignore_ascii_case(n)))
        .unwrap_or(false)
}

/// A config glob matches an artifact when it globs the `binary` field or the
/// file basename.
fn artifact_matches(art: &ProducedArtifact, glob: &str) -> bool {
    glob_match(glob, &art.binary) || glob_match(glob, basename(&art.path))
}

/// Resolve the URL base from config then the pipeline `base_url`.
fn base_url(cfg: &WinSparkleConfig, ctx: &ProviderContext<'_>) -> Result<String, RunnerError> {
    cfg.base_url
        .clone()
        .or_else(|| ctx.base_url.map(str::to_string))
        .map(|b| b.trim_end_matches('/').to_string())
        .ok_or_else(|| {
            RunnerError::Outcome(
                "winsparkle: no base URL — set `with.base_url` or the pipeline `base_url`"
                    .to_string(),
            )
        })
}

/// Join a base and a path segment with a single `/`.
fn join_url(base: &str, seg: &str) -> String {
    format!("{}/{}", base.trim_end_matches('/'), seg.trim_start_matches('/'))
}

/// Build the [`ReleaseNotes`] from config — link wins over inline HTML.
fn release_notes(cfg: &WinSparkleConfig) -> Option<ReleaseNotes> {
    if let Some(url) = &cfg.release_notes_url {
        Some(ReleaseNotes::Link(url.clone()))
    } else {
        cfg.release_notes_html.clone().map(ReleaseNotes::Html)
    }
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
    use base64::Engine;
    use ed25519_dalek::{Verifier, VerifyingKey};
    use std::collections::BTreeMap;

    const SEED: [u8; 32] = [5u8; 32];

    fn art(binary: &str, path: &str) -> ProducedArtifact {
        ProducedArtifact {
            binary: binary.into(),
            path: path.into(),
            triple: Some("windows-x86_64".into()),
        }
    }

    /// Authenticode coherence slots only (default trust-Authenticode mode).
    fn cert_secrets() -> MapSecrets {
        let mut m = BTreeMap::new();
        m.insert(SLOT_CERT.into(), "Y2VydA==".into());
        m.insert(SLOT_CERT_PASSWORD.into(), "pw".into());
        MapSecrets(m)
    }

    /// Cert coherence slots + the appcast ed key.
    fn signing_secrets() -> MapSecrets {
        let mut m = BTreeMap::new();
        m.insert(SLOT_CERT.into(), "Y2VydA==".into());
        m.insert(SLOT_CERT_PASSWORD.into(), "pw".into());
        m.insert(
            DEFAULT_ED_KEY_SLOT.into(),
            base64::engine::general_purpose::STANDARD.encode(SEED),
        );
        MapSecrets(m)
    }

    fn fixture_exe(dir: &Path, name: &str, contents: &[u8]) -> ProducedArtifact {
        let p = dir.join(name);
        std::fs::write(&p, contents).unwrap();
        art("desktop", p.to_str().unwrap())
    }

    fn ctx<'a>(
        secrets: &'a dyn crate::provider::SecretSource,
        work: &'a Path,
        cfg: &'a serde_json::Value,
        artifacts: &'a [ProducedArtifact],
        base_url: Option<&'a str>,
        dry_run: bool,
    ) -> ProviderContext<'a> {
        ProviderContext {
            version: "1.2.3",
            artifacts,
            base_url,
            config: cfg,
            work_dir: work,
            secrets,
            dry_run,
        }
    }

    #[test]
    fn declares_authenticode_coherence_slots() {
        let p = WinSparkleProvider;
        assert_eq!(p.name(), "winsparkle");
        assert_eq!(p.required_slots(), vec![SLOT_CERT, SLOT_CERT_PASSWORD]);
    }

    #[tokio::test]
    async fn dry_run_default_trusts_authenticode_no_signature() {
        let work = tempfile::tempdir().unwrap();
        let installer = fixture_exe(work.path(), "Setup-1.2.3.exe", b"installer bytes");
        let secrets = cert_secrets();
        let cfg = serde_json::Value::Null;
        let report = WinSparkleProvider
            .dispatch(&ctx(
                &secrets,
                work.path(),
                &cfg,
                std::slice::from_ref(&installer),
                Some("https://releases.yah.dev/desktop"),
                true,
            ))
            .await
            .unwrap();
        assert!(report.actions[0].contains("would emit WinSparkle appcast entry for v1.2.3"));
        assert!(report.actions[0].contains("Setup-1.2.3.exe"));
        assert!(report.actions[0].contains("trusts Authenticode"));
        assert!(report.actions.iter().any(|a| a.contains("appcast.xml")));
        assert!(report.produced.is_empty());
        assert!(report.published.is_empty());
        assert!(!work.path().join("appcast.xml").exists());
    }

    #[tokio::test]
    async fn live_default_emits_unsigned_appcast() {
        let work = tempfile::tempdir().unwrap();
        let installer = fixture_exe(work.path(), "Setup.msi", b"msi bytes");
        let secrets = cert_secrets();
        let cfg = serde_json::Value::Null;
        let report = WinSparkleProvider
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&installer), Some("https://r/d"), false))
            .await
            .unwrap();
        let xml = std::fs::read_to_string(work.path().join("appcast.xml")).unwrap();
        assert!(xml.contains("url=\"https://r/d/Setup.msi\""));
        assert!(xml.contains(&format!("length=\"{}\"", "msi bytes".len())));
        assert!(!xml.contains("edSignature"), "default mode adds no signature");
        assert!(report.published.contains(&"https://r/d/appcast.xml".to_string()));
        assert!(report.produced.iter().any(|p| p.path.ends_with("appcast.xml")));
    }

    #[tokio::test]
    async fn opt_in_appcast_signature_validates() {
        let work = tempfile::tempdir().unwrap();
        let payload = b"signed installer payload";
        let installer = fixture_exe(work.path(), "Setup.exe", payload);
        let secrets = signing_secrets();
        let cfg = serde_json::json!({ "sign_appcast": true });
        WinSparkleProvider
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&installer), Some("https://r"), false))
            .await
            .unwrap();
        let xml = std::fs::read_to_string(work.path().join("appcast.xml")).unwrap();
        let sig_b64 = xml
            .split("sparkle:edSignature=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .expect("edSignature present when sign_appcast");
        let sig = ed25519_dalek::Signature::from_slice(
            &base64::engine::general_purpose::STANDARD.decode(sig_b64).unwrap(),
        )
        .unwrap();
        let vk: VerifyingKey = ed25519_dalek::SigningKey::from_bytes(&SEED).verifying_key();
        assert!(vk.verify(payload, &sig).is_ok());
    }

    #[tokio::test]
    async fn sign_appcast_without_key_is_typed_error() {
        let work = tempfile::tempdir().unwrap();
        let installer = fixture_exe(work.path(), "Setup.exe", b"x");
        let secrets = cert_secrets(); // no ed key
        let cfg = serde_json::json!({ "sign_appcast": true });
        let err = WinSparkleProvider
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&installer), Some("https://r"), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains(DEFAULT_ED_KEY_SLOT));
    }

    #[tokio::test]
    async fn missing_cert_coherence_slot_is_typed_error() {
        let work = tempfile::tempdir().unwrap();
        let installer = fixture_exe(work.path(), "Setup.exe", b"x");
        let mut m = BTreeMap::new();
        m.insert(SLOT_CERT.into(), "Y2VydA==".to_string());
        // password missing
        let secrets = MapSecrets(m);
        let cfg = serde_json::Value::Null;
        let err = WinSparkleProvider
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&installer), Some("https://r"), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains(SLOT_CERT_PASSWORD));
    }

    #[tokio::test]
    async fn no_installer_is_a_config_error() {
        let work = tempfile::tempdir().unwrap();
        let secrets = cert_secrets();
        let artifacts = vec![art("yah", "out/yah"), art("desktop", "out/Desktop.dmg")];
        let cfg = serde_json::Value::Null;
        let err = WinSparkleProvider
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, Some("https://r"), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no Windows installer"));
    }

    #[tokio::test]
    async fn ambiguous_installer_requires_glob() {
        let work = tempfile::tempdir().unwrap();
        let a = fixture_exe(work.path(), "A.exe", b"a");
        let b = fixture_exe(work.path(), "B.msi", b"b");
        let secrets = cert_secrets();
        let cfg = serde_json::Value::Null;
        let err = WinSparkleProvider
            .dispatch(&ctx(&secrets, work.path(), &cfg, &[a, b], Some("https://r"), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("narrow to one"));
    }

    #[tokio::test]
    async fn config_glob_selects_named_installer() {
        let work = tempfile::tempdir().unwrap();
        let a = fixture_exe(work.path(), "Desktop.exe", b"a");
        let b = fixture_exe(work.path(), "Helper.exe", b"b");
        let secrets = cert_secrets();
        let cfg = serde_json::json!({ "artifacts": ["Desktop.exe"] });
        let report = WinSparkleProvider
            .dispatch(&ctx(&secrets, work.path(), &cfg, &[a, b], Some("https://r"), true))
            .await
            .unwrap();
        assert!(report.actions[0].contains("Desktop.exe"));
    }

    #[test]
    fn is_installer_case_insensitive() {
        assert!(is_installer("Setup.EXE"));
        assert!(is_installer("App.msi"));
        assert!(!is_installer("App.dmg"));
        assert!(!is_installer("yah"));
    }
}
