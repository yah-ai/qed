//! Sparkle (macOS) appcast + signed-update adapter (R509-F3).
//!
//! Sparkle is the macOS desktop auto-updater. A release ships by publishing an
//! **appcast** — an RSS feed (see [`crate::provider::appcast`]) — whose
//! `<enclosure>` points at the update archive and carries an EdDSA
//! (`sparkle:edSignature`) signature Sparkle verifies before applying. This
//! adapter takes the notarized + stapled `.dmg` / `.zip` produced upstream by
//! the `notarize` outcome (R509-F1), signs its bytes with an ed25519 key, and
//! emits the appcast entry.
//!
//! ## Boundary (decided in-ticket)
//!
//! Sparkle *generates and signs* — it does not push bytes. The signed appcast
//! XML (and any delta) is returned in [`ProviderReport::produced`] so the
//! existing channel sync (`publish.rs`, the R2 producer) ships it alongside the
//! archive; the public feed URL is returned in [`ProviderReport::published`] as
//! the locator. Keeping upload out of the adapter keeps it pure and unit-
//! testable, and avoids a second copy of the R2 layout logic.
//!
//! ## Credentials
//!
//! One slot, resolved through [`crate::secrets_bridge`]:
//! - `SPARKLE_ED_PRIVATE_KEY` — the ed25519 private key for `sign_update`,
//!   **base64-encoded**. Accepts either the 32-byte seed or Sparkle's 64-byte
//!   `seed‖public` export (the leading 32 bytes are the seed). Never logged.
//!
//! ## Dry run
//!
//! `ctx.dry_run` resolves the key, confirms the update archive exists, and
//! computes the signature it *would* emit (a local, deterministic operation —
//! no network), reporting the appcast entry — but writes no files and
//! publishes nothing.

use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use crate::provider::appcast::{render_appcast, AppcastEntry, DeltaEnclosure, ReleaseNotes};
use crate::provider::edsign::{parse_signing_key, sign_b64, sig_prefix};
use crate::provider::{ProviderContext, ProviderReport, ReleaseProvider};
use crate::runner::RunnerError;
use crate::types::ProducedArtifact;

/// Credential slot: the ed25519 private key (base64) for `sign_update`.
const SLOT_ED_KEY: &str = "SPARKLE_ED_PRIVATE_KEY";

/// Update-archive extensions Sparkle accepts. `.dmg` / `.zip` are the common
/// macOS update bundles; `.pkg` and the tarballs are also valid. Matched
/// case-insensitively.
const ARCHIVE_EXTS: &[&str] = &["dmg", "zip", "pkg", "tar.gz", "tar.xz", "tar.bz2"];

/// The `with = { … }` config block for a `sparkle` outcome.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct SparkleConfig {
    /// Binary-name / basename globs selecting which produced artifact to sign.
    /// After filtering to archive extensions exactly one must remain (Sparkle
    /// ships one update per appcast item); otherwise narrow with this.
    artifacts: Vec<String>,
    /// Public base URL for the enclosure + feed. Falls back to the pipeline's
    /// `base_url` ([`ProviderContext::base_url`]). The enclosure URL is
    /// `{base}/{archive-basename}`.
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
    /// `sparkle:version` — the monotonic build / `CFBundleVersion`. Defaults to
    /// the release version when unset (acceptable when versions sort).
    build: Option<String>,
    /// `sparkle:minimumSystemVersion` (`11.0`).
    minimum_system_version: Option<String>,
    /// Release-notes URL (`<sparkle:releaseNotesLink>`). Mutually exclusive
    /// with `release_notes_html`; the link wins if both are set.
    release_notes_url: Option<String>,
    /// Inline release-notes HTML (`<description><![CDATA[…]]>`).
    release_notes_html: Option<String>,
    /// MIME type for the enclosure. Default `application/octet-stream`.
    mime_type: String,
    /// RFC822 `<pubDate>` (the seam has no clock — pass it explicitly).
    pub_date: Option<String>,
    /// Prior update archive to diff against for a delta. When set (and live),
    /// the delta tool produces `<name>.delta`, signed and added to the item.
    previous_bundle: Option<String>,
    /// The `sparkle:version` (build) the previous bundle corresponds to —
    /// required when `previous_bundle` is set (it's the delta's `deltaFrom`).
    previous_build: Option<String>,
    /// Sparkle `BinaryDelta` tool path. Default `BinaryDelta` on `PATH`.
    binary_delta_tool: String,
}

impl Default for SparkleConfig {
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
            previous_bundle: None,
            previous_build: None,
            binary_delta_tool: "BinaryDelta".to_string(),
        }
    }
}

/// `sparkle` — macOS appcast generation + EdDSA signing. See module docs.
#[derive(Debug, Default)]
pub struct SparkleProvider;

#[async_trait]
impl ReleaseProvider for SparkleProvider {
    fn name(&self) -> &str {
        "sparkle"
    }

    fn required_slots(&self) -> Vec<&str> {
        vec![SLOT_ED_KEY]
    }

    async fn dispatch(&self, ctx: &ProviderContext<'_>) -> Result<ProviderReport, RunnerError> {
        let cfg: SparkleConfig = parse_config(ctx.config)?;

        // Credential is required even to build the action plan (the signature
        // is computed locally in both dry-run and live paths).
        let key_b64 = ctx.require_secret(SLOT_ED_KEY)?;
        let signing_key = parse_signing_key(&key_b64, SLOT_ED_KEY)?;

        let archive = select_archive(ctx.artifacts, &cfg.artifacts)?;
        let base = base_url(&cfg, ctx)?;
        let filename = basename(&archive.path);
        let enclosure_url = join_url(&base, filename);
        let build = cfg.build.clone().unwrap_or_else(|| ctx.version.to_string());

        // Read + sign the archive (local, deterministic — fine under dry-run).
        let bytes = std::fs::read(&archive.path).map_err(|e| {
            RunnerError::Outcome(format!(
                "sparkle: reading update archive {}: {e}",
                archive.path
            ))
        })?;
        let signature = sign_b64(&signing_key, &bytes);

        let mut entry = AppcastEntry::new(
            format!("Version {}", ctx.version),
            ctx.version,
            &build,
            &enclosure_url,
            bytes.len() as u64,
        );
        entry.ed_signature = Some(signature.clone());
        entry.mime_type = cfg.mime_type.clone();
        entry.min_system_version = cfg.minimum_system_version.clone();
        entry.channel = cfg.channel.clone();
        entry.pub_date = cfg.pub_date.clone();
        entry.release_notes = release_notes(&cfg);

        let feed_url = cfg
            .feed_url
            .clone()
            .unwrap_or_else(|| join_url(&base, &cfg.appcast_filename));

        if ctx.dry_run {
            let mut actions = vec![format!(
                "would emit appcast entry for v{} (build {build}, archive {filename}, edSignature {}…)",
                ctx.version,
                sig_prefix(&signature),
            )];
            if let Some(prev) = &cfg.previous_bundle {
                actions.push(format!(
                    "would emit delta from build {} against {}",
                    cfg.previous_build.as_deref().unwrap_or("?"),
                    basename(prev),
                ));
            }
            actions.push(format!("would publish appcast feed at {feed_url}"));
            return Ok(ProviderReport {
                actions,
                ..Default::default()
            });
        }

        let mut report = ProviderReport::default();
        let mut produced: Vec<ProducedArtifact> = Vec::new();

        // Optional delta: diff the previous bundle, sign it, add the enclosure.
        if let Some(prev) = &cfg.previous_bundle {
            let from = cfg.previous_build.clone().ok_or_else(|| {
                RunnerError::Outcome(
                    "sparkle: `previous_bundle` set without `previous_build` (the delta's deltaFrom)"
                        .to_string(),
                )
            })?;
            let delta = make_delta(&cfg.binary_delta_tool, prev, &archive.path, ctx.work_dir, &build)
                .await?;
            let delta_bytes = std::fs::read(&delta).map_err(|e| {
                RunnerError::Outcome(format!("sparkle: reading generated delta: {e}"))
            })?;
            let delta_name = basename(delta.to_str().unwrap_or_default()).to_string();
            entry.deltas.push(DeltaEnclosure {
                delta_from: from,
                url: join_url(&base, &delta_name),
                length: delta_bytes.len() as u64,
                ed_signature: Some(sign_b64(&signing_key, &delta_bytes)),
            });
            report.actions.push(format!("generated + signed delta {delta_name}"));
            produced.push(ProducedArtifact {
                binary: archive.binary.clone(),
                path: delta.to_string_lossy().into_owned(),
                triple: archive.triple.clone(),
            });
        }

        // Write the appcast XML into work_dir and return it as a produced
        // artifact for the channel sync to ship.
        let xml = render_appcast(&cfg.channel_title, std::slice::from_ref(&entry));
        let appcast_path = ctx.work_dir.join(&cfg.appcast_filename);
        std::fs::write(&appcast_path, xml).map_err(|e| {
            RunnerError::Outcome(format!("sparkle: writing appcast {}: {e}", cfg.appcast_filename))
        })?;
        report
            .actions
            .push(format!("wrote + signed appcast {} for v{}", cfg.appcast_filename, ctx.version));
        produced.push(ProducedArtifact {
            binary: archive.binary.clone(),
            path: appcast_path.to_string_lossy().into_owned(),
            triple: archive.triple.clone(),
        });

        report.produced = produced;
        report.published = vec![feed_url];
        Ok(report)
    }
}

/// Deserialize the opaque `with` blob into [`SparkleConfig`]. A `null` config
/// is the default.
fn parse_config(value: &serde_json::Value) -> Result<SparkleConfig, RunnerError> {
    if value.is_null() {
        return Ok(SparkleConfig::default());
    }
    serde_json::from_value(value.clone())
        .map_err(|e| RunnerError::Outcome(format!("sparkle: invalid `with` config: {e}")))
}

/// Pick the single update archive: the config globs filtered to archive
/// extensions, or every archive artifact. Errors when zero or more-than-one
/// remain (Sparkle ships exactly one update per appcast item).
fn select_archive(
    artifacts: &[ProducedArtifact],
    globs: &[String],
) -> Result<ProducedArtifact, RunnerError> {
    let selected: Vec<&ProducedArtifact> = artifacts
        .iter()
        .filter(|a| is_archive(&a.path))
        .filter(|a| globs.is_empty() || globs.iter().any(|g| artifact_matches(a, g)))
        .collect();
    match selected.as_slice() {
        [one] => Ok((*one).clone()),
        [] if globs.is_empty() => Err(RunnerError::Outcome(format!(
            "sparkle: no update archive (.dmg/.zip/.pkg/.tar.*) among {} produced",
            artifacts.len()
        ))),
        [] => Err(RunnerError::Outcome(format!(
            "sparkle: no update archive matched config globs {globs:?} (of {} produced)",
            artifacts.len()
        ))),
        many => Err(RunnerError::Outcome(format!(
            "sparkle: {} update archives selected ({}); narrow to one with `with.artifacts`",
            many.len(),
            many.iter().map(|a| basename(&a.path)).collect::<Vec<_>>().join(", "),
        ))),
    }
}

/// Whether `path` has a Sparkle update-archive extension (case-insensitive,
/// honoring the two-part `.tar.*` forms).
fn is_archive(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    ARCHIVE_EXTS.iter().any(|ext| lower.ends_with(&format!(".{ext}")))
}

/// A config glob matches an artifact when it globs the `binary` field or the
/// file basename.
fn artifact_matches(art: &ProducedArtifact, glob: &str) -> bool {
    glob_match(glob, &art.binary) || glob_match(glob, basename(&art.path))
}

/// Resolve the URL base from config then the pipeline `base_url`.
fn base_url(cfg: &SparkleConfig, ctx: &ProviderContext<'_>) -> Result<String, RunnerError> {
    cfg.base_url
        .clone()
        .or_else(|| ctx.base_url.map(str::to_string))
        .map(|b| b.trim_end_matches('/').to_string())
        .ok_or_else(|| {
            RunnerError::Outcome(
                "sparkle: no base URL — set `with.base_url` or the pipeline `base_url`".to_string(),
            )
        })
}

/// Join a base and a path segment with a single `/`.
fn join_url(base: &str, seg: &str) -> String {
    format!("{}/{}", base.trim_end_matches('/'), seg.trim_start_matches('/'))
}

/// Build the [`ReleaseNotes`] from config — link wins over inline HTML.
fn release_notes(cfg: &SparkleConfig) -> Option<ReleaseNotes> {
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

/// Run Sparkle's `BinaryDelta create <old> <new> <out>` and return the delta
/// path. Live-only.
async fn make_delta(
    tool: &str,
    previous: &str,
    current: &str,
    work_dir: &Path,
    build: &str,
) -> Result<std::path::PathBuf, RunnerError> {
    let out = work_dir.join(format!("update-{build}.delta"));
    let result = tokio::process::Command::new(tool)
        .arg("create")
        .arg(previous)
        .arg(current)
        .arg(&out)
        .output()
        .await
        .map_err(|e| RunnerError::Outcome(format!("sparkle: spawning `{tool} create`: {e}")))?;
    if !result.status.success() {
        return Err(RunnerError::Outcome(format!(
            "sparkle: BinaryDelta failed (status {}): {}",
            result.status,
            String::from_utf8_lossy(&result.stderr).trim(),
        )));
    }
    Ok(out)
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
    use ed25519_dalek::{SigningKey, Verifier, VerifyingKey};
    use std::collections::BTreeMap;

    /// Deterministic 32-byte seed for tests.
    const SEED: [u8; 32] = [7u8; 32];

    fn key_b64() -> String {
        base64::engine::general_purpose::STANDARD.encode(SEED)
    }

    fn full_secrets() -> MapSecrets {
        let mut m = BTreeMap::new();
        m.insert(SLOT_ED_KEY.into(), key_b64());
        MapSecrets(m)
    }

    fn art(binary: &str, path: &str) -> ProducedArtifact {
        ProducedArtifact {
            binary: binary.into(),
            path: path.into(),
            triple: Some("darwin-aarch64".into()),
        }
    }

    /// Write a fixture archive file and return a ProducedArtifact pointing at
    /// it (so the adapter can read + sign real bytes).
    fn fixture_dmg(dir: &Path, name: &str, contents: &[u8]) -> ProducedArtifact {
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
    fn declares_ed_key_slot() {
        let p = SparkleProvider;
        assert_eq!(p.name(), "sparkle");
        assert_eq!(p.required_slots(), vec![SLOT_ED_KEY]);
    }

    #[tokio::test]
    async fn dry_run_reports_entry_and_feed_without_writing() {
        let work = tempfile::tempdir().unwrap();
        let archive = fixture_dmg(work.path(), "Desktop-1.2.3.dmg", b"fake dmg payload");
        let secrets = full_secrets();
        let cfg = serde_json::Value::Null;
        let report = SparkleProvider
            .dispatch(&ctx(
                &secrets,
                work.path(),
                &cfg,
                std::slice::from_ref(&archive),
                Some("https://releases.yah.dev/desktop"),
                true,
            ))
            .await
            .unwrap();
        assert!(report.actions.iter().any(|a| a.contains("would emit appcast entry for v1.2.3")));
        assert!(report.actions.iter().any(|a| a.contains("Desktop-1.2.3.dmg")));
        assert!(report
            .actions
            .iter()
            .any(|a| a.contains("https://releases.yah.dev/desktop/appcast.xml")));
        assert!(report.produced.is_empty(), "dry run writes nothing");
        assert!(report.published.is_empty());
        assert!(!work.path().join("appcast.xml").exists());
    }

    #[tokio::test]
    async fn signature_validates_with_matching_public_key() {
        let work = tempfile::tempdir().unwrap();
        let payload = b"the actual update archive bytes";
        let archive = fixture_dmg(work.path(), "Desktop-1.2.3.zip", payload);
        let secrets = full_secrets();
        let cfg = serde_json::Value::Null;
        let report = SparkleProvider
            .dispatch(&ctx(
                &secrets,
                work.path(),
                &cfg,
                std::slice::from_ref(&archive),
                Some("https://r/d"),
                false,
            ))
            .await
            .unwrap();
        // The appcast was written and returned.
        let appcast = work.path().join("appcast.xml");
        assert!(appcast.exists());
        let xml = std::fs::read_to_string(&appcast).unwrap();
        // Extract the edSignature and verify it over the payload bytes.
        let sig_b64 = xml
            .split("sparkle:edSignature=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .unwrap();
        let sig_bytes = base64::engine::general_purpose::STANDARD
            .decode(sig_b64)
            .unwrap();
        let sig = ed25519_dalek::Signature::from_slice(&sig_bytes).unwrap();
        let vk: VerifyingKey = SigningKey::from_bytes(&SEED).verifying_key();
        assert!(vk.verify(payload, &sig).is_ok(), "edSignature validates");
        assert!(report.published.contains(&"https://r/d/appcast.xml".to_string()));
        assert!(report.produced.iter().any(|p| p.path.ends_with("appcast.xml")));
    }

    #[tokio::test]
    async fn missing_key_slot_is_typed_error() {
        let work = tempfile::tempdir().unwrap();
        let archive = fixture_dmg(work.path(), "D.dmg", b"x");
        let secrets = MapSecrets::default();
        let cfg = serde_json::Value::Null;
        let err = SparkleProvider
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&archive), Some("https://r"), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains(SLOT_ED_KEY));
    }

    #[tokio::test]
    async fn no_archive_is_a_config_error() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let artifacts = vec![art("yah", "out/yah"), art("desktop", "out/Desktop.exe")];
        let cfg = serde_json::Value::Null;
        let err = SparkleProvider
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, Some("https://r"), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no update archive"));
    }

    #[tokio::test]
    async fn ambiguous_archive_requires_glob() {
        let work = tempfile::tempdir().unwrap();
        let a = fixture_dmg(work.path(), "A.dmg", b"a");
        let b = fixture_dmg(work.path(), "B.zip", b"b");
        let secrets = full_secrets();
        let cfg = serde_json::Value::Null;
        let err = SparkleProvider
            .dispatch(&ctx(&secrets, work.path(), &cfg, &[a, b], Some("https://r"), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("narrow to one"));
    }

    #[tokio::test]
    async fn missing_base_url_is_a_config_error() {
        let work = tempfile::tempdir().unwrap();
        let archive = fixture_dmg(work.path(), "D.dmg", b"x");
        let secrets = full_secrets();
        let cfg = serde_json::Value::Null;
        let err = SparkleProvider
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&archive), None, true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no base URL"));
    }

    #[tokio::test]
    async fn config_drives_channel_notes_and_min_version() {
        let work = tempfile::tempdir().unwrap();
        let archive = fixture_dmg(work.path(), "D.dmg", b"payload");
        let secrets = full_secrets();
        let cfg = serde_json::json!({
            "channel": "beta",
            "minimum_system_version": "12.0",
            "release_notes_url": "https://r/notes.html",
            "channel_title": "Yah Desktop",
        });
        SparkleProvider
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&archive), Some("https://r"), false))
            .await
            .unwrap();
        let xml = std::fs::read_to_string(work.path().join("appcast.xml")).unwrap();
        assert!(xml.contains("<sparkle:channel>beta</sparkle:channel>"));
        assert!(xml.contains("<sparkle:minimumSystemVersion>12.0</sparkle:minimumSystemVersion>"));
        assert!(xml.contains("releaseNotesLink>https://r/notes.html"));
        assert!(xml.contains("<title>Yah Desktop</title>"));
    }

    #[test]
    fn is_archive_handles_tar_forms() {
        assert!(is_archive("Foo.dmg"));
        assert!(is_archive("Foo.ZIP"));
        assert!(is_archive("Foo.tar.gz"));
        assert!(!is_archive("Foo.exe"));
        assert!(!is_archive("yah"));
    }
}
