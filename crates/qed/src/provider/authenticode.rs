//! Authenticode code-signing adapter (R509-F2).
//!
//! Signs a Windows `.exe` / `.msi` (and the other PE-family installers) in
//! place using [`osslsigncode`] — a cross-platform Authenticode signer that
//! needs no Windows host, so a release can sign Windows artifacts from the same
//! Linux/macOS runner that built them. On a Windows host the caller can point
//! `tool` at `signtool` instead; the argument shape is normalized behind the
//! [`Signer`] enum.
//!
//! Authenticode is a *transform*, not a ship: it mutates the binary's embedded
//! signature and returns the signed artifacts in [`ProviderReport::produced`]
//! so a downstream `Outcome::Provider { provider = "winsparkle" }` ships the
//! now-signed installer. It publishes nothing, so
//! [`ProviderReport::published`] stays empty. The pipeline orders
//! `authenticode` before the channel-ship outcome (mirrors notarize → sparkle
//! on the mac slice).
//!
//! ## Credentials
//!
//! Two slots, resolved through [`crate::secrets_bridge`] (shared verbatim with
//! the winsparkle adapter R509-F4 — same cert, no code dependency):
//! - `AUTHENTICODE_CERT` — the PKCS#12 / `.pfx` cert+key, **base64-encoded**
//!   (the secrets bridge resolves to a `String`; a `.pfx` is binary). Decoded
//!   and materialized to a `0600` temp file under `ctx.work_dir`, never logged.
//! - `AUTHENTICODE_CERT_PASSWORD` — the `.pfx` import password.
//!
//! ## Dry run
//!
//! `ctx.dry_run` validates that both slots resolve, that the cert decodes from
//! base64, and that at least one signable artifact is selected, then reports
//! `would sign <file> (sha256, ts <tsa>)` per artifact — spawning no signer and
//! mutating nothing.
//!
//! [`osslsigncode`]: https://github.com/mtrojnar/osslsigncode

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use base64::Engine;
use serde::Deserialize;

use crate::provider::{ProviderContext, ProviderReport, ReleaseProvider};
use crate::runner::RunnerError;
use crate::types::ProducedArtifact;

/// Credential slot names this adapter reads. Shared verbatim with winsparkle
/// (R509-F4) — same cert family, resolved independently per adapter.
const SLOT_CERT: &str = "AUTHENTICODE_CERT";
const SLOT_CERT_PASSWORD: &str = "AUTHENTICODE_CERT_PASSWORD";

/// Default RFC3161 timestamp authority — DigiCert's public TSA. Overridable via
/// `with = { timestamp_url = "…" }`. A timestamp is what lets a signature stay
/// valid after the signing cert expires, so it is on by default.
const DEFAULT_TSA: &str = "http://timestamp.digicert.com";

/// Default digest algorithm. SHA-1 Authenticode is deprecated; SHA-256 is the
/// modern baseline.
const DEFAULT_DIGEST: &str = "sha256";

/// PE-family extensions Authenticode can sign. `.exe` / `.dll` / `.sys` are raw
/// PE; `.msi` / `.cab` / `.cat` are the installer/catalog containers
/// osslsigncode also handles. Matched case-insensitively.
const SIGNABLE_EXTS: &[&str] = &["exe", "msi", "dll", "sys", "cab", "cat", "ps1"];

/// The `with = { … }` config block for an `authenticode` outcome.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct AuthenticodeConfig {
    /// Binary-name / basename globs selecting which produced artifacts to sign
    /// (`*` is the only wildcard). An entry matches an artifact when it globs
    /// the artifact's `binary` field *or* its file basename. When empty, every
    /// produced artifact with a signable extension is taken.
    artifacts: Vec<String>,
    /// RFC3161 timestamp-authority URL. Defaults to [`DEFAULT_TSA`].
    timestamp_url: String,
    /// File-digest algorithm passed to the signer's `-h`. Defaults to
    /// [`DEFAULT_DIGEST`] (`sha256`).
    digest: String,
}

impl Default for AuthenticodeConfig {
    fn default() -> Self {
        Self {
            artifacts: Vec::new(),
            timestamp_url: DEFAULT_TSA.to_string(),
            digest: DEFAULT_DIGEST.to_string(),
        }
    }
}

/// Which signer binary drives the sign — `osslsigncode` (default, cross
/// platform) or Windows-native `signtool`. The argument shape differs; this
/// enum normalizes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Signer {
    Osslsigncode,
    Signtool,
}

/// `authenticode` — Windows code signing. See module docs.
#[derive(Debug, Default)]
pub struct AuthenticodeProvider {
    /// Signer binary override; defaults to `osslsigncode` on `PATH`. When the
    /// basename is `signtool` (or `signtool.exe`) the Windows argument shape is
    /// used.
    tool_bin: Option<String>,
}

impl AuthenticodeProvider {
    /// Construct an adapter that drives an explicit signer binary (tests /
    /// non-default toolchains / Windows `signtool`).
    #[allow(dead_code)]
    fn with_tool(tool: impl Into<String>) -> Self {
        Self {
            tool_bin: Some(tool.into()),
        }
    }

    /// Path to the signer binary (defaults to `osslsigncode` on `PATH`).
    fn tool(&self) -> &str {
        self.tool_bin.as_deref().unwrap_or("osslsigncode")
    }

    /// Which signer flavor the configured binary is, inferred from its
    /// basename. Anything that isn't `signtool` is treated as osslsigncode.
    /// Splits on both `/` and `\` so a Windows `signtool.exe` path is detected
    /// regardless of which host inspects it (`Path` is separator-sensitive to
    /// the host OS; a `signtool` binary is only ever a Windows path).
    fn signer(&self) -> Signer {
        let tool = self.tool();
        let base = tool
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or(tool)
            .trim_end_matches(".exe")
            .trim_end_matches(".EXE");
        if base.eq_ignore_ascii_case("signtool") {
            Signer::Signtool
        } else {
            Signer::Osslsigncode
        }
    }
}

#[async_trait]
impl ReleaseProvider for AuthenticodeProvider {
    fn name(&self) -> &str {
        "authenticode"
    }

    fn required_slots(&self) -> Vec<&str> {
        vec![SLOT_CERT, SLOT_CERT_PASSWORD]
    }

    async fn dispatch(&self, ctx: &ProviderContext<'_>) -> Result<ProviderReport, RunnerError> {
        let cfg: AuthenticodeConfig = parse_config(ctx.config)?;

        // Credentials are required even to build the action plan — a dry run is
        // the plan-time credential-presence check. Decode the cert here too so
        // a bad base64 blob fails the dry run, not only the live sign.
        let cert_b64 = ctx.require_secret(SLOT_CERT)?;
        let password = ctx.require_secret(SLOT_CERT_PASSWORD)?;
        let cert_der = decode_cert(&cert_b64)?;

        let selected = select_artifacts(ctx.artifacts, &cfg.artifacts)?;

        let mut report = ProviderReport::default();

        if ctx.dry_run {
            for art in &selected {
                report.actions.push(format!(
                    "would sign {} ({}, ts {})",
                    basename(&art.path),
                    cfg.digest,
                    cfg.timestamp_url,
                ));
            }
            return Ok(report);
        }

        // Live path: materialize the .pfx once (0600, never logged) and reuse
        // it for every signature.
        let cert_path = write_cert(ctx.work_dir, &cert_der)?;

        for art in selected {
            sign_one(self.tool(), self.signer(), &art, &cert_path, &password, &cfg).await?;
            report
                .actions
                .push(format!("signed {} ({})", basename(&art.path), cfg.digest));
            // Signing mutates the binary in place — the path is unchanged, but
            // threading the artifact through `produced` is how a downstream
            // ship outcome (winsparkle) addresses the now-signed installer.
            report.produced.push(art);
        }

        Ok(report)
    }
}

/// Deserialize the opaque `with` blob into [`AuthenticodeConfig`]. A `null`
/// config (no `with` table) is the default — sign every signable artifact with
/// the default TSA + sha256.
fn parse_config(value: &serde_json::Value) -> Result<AuthenticodeConfig, RunnerError> {
    if value.is_null() {
        return Ok(AuthenticodeConfig::default());
    }
    serde_json::from_value(value.clone())
        .map_err(|e| RunnerError::Outcome(format!("authenticode: invalid `with` config: {e}")))
}

/// Decode the base64-stored PKCS#12 cert blob into raw `.pfx` bytes. Tolerates
/// surrounding whitespace/newlines (a multi-line vault blob). Never logs the
/// decoded bytes.
fn decode_cert(b64: &str) -> Result<Vec<u8>, RunnerError> {
    let cleaned: String = b64.split_whitespace().collect();
    base64::engine::general_purpose::STANDARD
        .decode(cleaned.as_bytes())
        .map_err(|e| {
            RunnerError::Outcome(format!(
                "authenticode: `{SLOT_CERT}` is not valid base64 \
                 (expected a base64-encoded PKCS#12/.pfx blob): {e}"
            ))
        })
}

/// Pick the artifacts to sign: the config globs filtered to signable
/// extensions, or — when no globs are given — every signable artifact. Errors
/// when the selection is empty (an authenticode outcome that signs nothing is a
/// config bug, not a no-op).
fn select_artifacts(
    artifacts: &[ProducedArtifact],
    globs: &[String],
) -> Result<Vec<ProducedArtifact>, RunnerError> {
    let selected: Vec<ProducedArtifact> = artifacts
        .iter()
        .filter(|a| is_signable(&a.path))
        .filter(|a| globs.is_empty() || globs.iter().any(|g| artifact_matches(a, g)))
        .cloned()
        .collect();

    if selected.is_empty() {
        let detail = if globs.is_empty() {
            format!(
                "no signable artifact (.exe/.msi/.dll/.sys/.cab/.cat/.ps1) among {} produced",
                artifacts.len()
            )
        } else {
            format!(
                "no signable artifact matched config globs {globs:?} (of {} produced)",
                artifacts.len()
            )
        };
        return Err(RunnerError::Outcome(format!("authenticode: {detail}")));
    }
    Ok(selected)
}

/// Whether `path` has a signable PE-family extension (case-insensitive).
fn is_signable(path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| SIGNABLE_EXTS.iter().any(|n| e.eq_ignore_ascii_case(n)))
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
/// char is literal. Sufficient for `desktop`, `*.exe`, `yah-*` style filters.
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

/// Materialize the decoded `.pfx` cert to a `0600` file under the scratch dir.
/// Never logs the contents.
fn write_cert(work_dir: &Path, der: &[u8]) -> Result<PathBuf, RunnerError> {
    let path = work_dir.join("authenticode-cert.pfx");
    std::fs::write(&path, der)
        .map_err(|e| RunnerError::Outcome(format!("authenticode: writing cert file: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| RunnerError::Outcome(format!("authenticode: chmod 600 cert file: {e}")))?;
    }
    Ok(path)
}

/// Sign one artifact in place. osslsigncode can't sign over itself, so it signs
/// to a sibling `<name>.signed` and renames it back over the input; signtool
/// signs in place natively.
async fn sign_one(
    tool: &str,
    signer: Signer,
    art: &ProducedArtifact,
    cert_path: &Path,
    password: &str,
    cfg: &AuthenticodeConfig,
) -> Result<(), RunnerError> {
    match signer {
        Signer::Osslsigncode => {
            let signed = sibling_signed_path(&art.path);
            let out = tokio::process::Command::new(tool)
                .arg("sign")
                .arg("-pkcs12")
                .arg(cert_path)
                .arg("-pass")
                .arg(password)
                .arg("-h")
                .arg(&cfg.digest)
                .arg("-t")
                .arg(&cfg.timestamp_url)
                .arg("-in")
                .arg(&art.path)
                .arg("-out")
                .arg(&signed)
                .output()
                .await
                .map_err(|e| {
                    RunnerError::Outcome(format!("authenticode: spawning `{tool} sign`: {e}"))
                })?;
            if !out.status.success() {
                return Err(RunnerError::Outcome(format!(
                    "authenticode: osslsigncode failed on {} (status {}): {}",
                    basename(&art.path),
                    out.status,
                    String::from_utf8_lossy(&out.stderr).trim(),
                )));
            }
            std::fs::rename(&signed, &art.path).map_err(|e| {
                RunnerError::Outcome(format!(
                    "authenticode: replacing {} with signed output: {e}",
                    basename(&art.path)
                ))
            })?;
            Ok(())
        }
        Signer::Signtool => {
            // signtool sign /fd sha256 /f cert.pfx /p PASS /tr TSA /td sha256 file
            let out = tokio::process::Command::new(tool)
                .arg("sign")
                .arg("/fd")
                .arg(&cfg.digest)
                .arg("/f")
                .arg(cert_path)
                .arg("/p")
                .arg(password)
                .arg("/tr")
                .arg(&cfg.timestamp_url)
                .arg("/td")
                .arg(&cfg.digest)
                .arg(&art.path)
                .output()
                .await
                .map_err(|e| {
                    RunnerError::Outcome(format!("authenticode: spawning `{tool} sign`: {e}"))
                })?;
            if !out.status.success() {
                return Err(RunnerError::Outcome(format!(
                    "authenticode: signtool failed on {} (status {}): {}",
                    basename(&art.path),
                    out.status,
                    String::from_utf8_lossy(&out.stderr).trim(),
                )));
            }
            Ok(())
        }
    }
}

/// `<path>.signed` — the osslsigncode scratch output renamed back over the
/// input.
fn sibling_signed_path(path: &str) -> PathBuf {
    PathBuf::from(format!("{path}.signed"))
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
            triple: Some("windows-x86_64".into()),
        }
    }

    /// A valid base64 blob (contents are opaque to the adapter until handed to
    /// the signer; the dry-run path only needs it to decode).
    fn cert_b64() -> String {
        base64::engine::general_purpose::STANDARD.encode(b"fake-pkcs12-der")
    }

    fn full_secrets() -> MapSecrets {
        let mut m = BTreeMap::new();
        m.insert(SLOT_CERT.into(), cert_b64());
        m.insert(SLOT_CERT_PASSWORD.into(), "hunter2".into());
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
    fn declares_cert_slots() {
        let p = AuthenticodeProvider::default();
        assert_eq!(p.name(), "authenticode");
        assert_eq!(p.required_slots(), vec![SLOT_CERT, SLOT_CERT_PASSWORD]);
    }

    #[test]
    fn signer_inferred_from_tool_basename() {
        assert_eq!(AuthenticodeProvider::default().signer(), Signer::Osslsigncode);
        assert_eq!(
            AuthenticodeProvider::with_tool("/usr/bin/osslsigncode").signer(),
            Signer::Osslsigncode
        );
        assert_eq!(
            AuthenticodeProvider::with_tool("signtool").signer(),
            Signer::Signtool
        );
        assert_eq!(
            AuthenticodeProvider::with_tool(r"C:\sdk\signtool.exe").signer(),
            Signer::Signtool
        );
    }

    #[tokio::test]
    async fn dry_run_plans_per_artifact_without_spawning_signer() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let artifacts = vec![
            art("desktop", "out/Desktop.exe"),
            art("yah", "out/yah"),
            art("installer", "out/Setup.msi"),
        ];
        let cfg = serde_json::Value::Null;
        let report = AuthenticodeProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap();
        // The .exe and .msi are signable; the bare `yah` binary is skipped.
        assert_eq!(report.actions.len(), 2);
        assert!(report.actions[0].contains("would sign Desktop.exe"));
        assert!(report.actions[0].contains("sha256"));
        assert!(report.actions[0].contains(DEFAULT_TSA));
        assert!(report.actions[1].contains("would sign Setup.msi"));
        assert!(report.produced.is_empty(), "dry run mutates nothing");
        assert!(report.published.is_empty());
        // No cert written on the dry-run path.
        assert!(!work.path().join("authenticode-cert.pfx").exists());
    }

    #[tokio::test]
    async fn dry_run_missing_slot_is_typed_error_naming_slot() {
        let work = tempfile::tempdir().unwrap();
        let mut m = BTreeMap::new();
        m.insert(SLOT_CERT.into(), cert_b64());
        // AUTHENTICODE_CERT_PASSWORD missing.
        let secrets = MapSecrets(m);
        let artifacts = vec![art("desktop", "out/Desktop.exe")];
        let cfg = serde_json::Value::Null;
        let err = AuthenticodeProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap_err();
        assert!(
            format!("{err}").contains(SLOT_CERT_PASSWORD),
            "names the slot: {err}"
        );
    }

    #[tokio::test]
    async fn dry_run_bad_base64_cert_is_typed_error() {
        let work = tempfile::tempdir().unwrap();
        let mut m = BTreeMap::new();
        m.insert(SLOT_CERT.into(), "not base64!!!@@@".into());
        m.insert(SLOT_CERT_PASSWORD.into(), "hunter2".into());
        let secrets = MapSecrets(m);
        let artifacts = vec![art("desktop", "out/Desktop.exe")];
        let cfg = serde_json::Value::Null;
        let err = AuthenticodeProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("not valid base64"), "{err}");
    }

    #[test]
    fn whitespace_in_cert_blob_decodes() {
        let raw = b"fake-pkcs12-der";
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
        // Simulate a multi-line vault blob.
        let multiline = format!("  {}\n  {}\n", &b64[..4], &b64[4..]);
        assert_eq!(decode_cert(&multiline).unwrap(), raw);
    }

    #[tokio::test]
    async fn no_signable_artifact_is_a_config_error() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let artifacts = vec![art("yah", "out/yah"), art("desktop", "out/Desktop.dmg")];
        let cfg = serde_json::Value::Null;
        let err = AuthenticodeProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no signable artifact"));
    }

    #[tokio::test]
    async fn config_glob_filters_to_named_binary() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let artifacts = vec![
            art("desktop", "out/Desktop.exe"),
            art("helper", "out/Helper.exe"),
        ];
        let cfg = serde_json::json!({ "artifacts": ["desktop"] });
        let report = AuthenticodeProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap();
        assert_eq!(report.actions.len(), 1);
        assert!(report.actions[0].contains("Desktop.exe"));
    }

    #[tokio::test]
    async fn config_overrides_digest_and_tsa() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let artifacts = vec![art("desktop", "out/Desktop.exe")];
        let cfg = serde_json::json!({
            "digest": "sha384",
            "timestamp_url": "http://tsa.example/rfc3161",
        });
        let report = AuthenticodeProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap();
        assert!(report.actions[0].contains("sha384"));
        assert!(report.actions[0].contains("http://tsa.example/rfc3161"));
    }

    #[tokio::test]
    async fn config_glob_with_no_match_errors() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let artifacts = vec![art("desktop", "out/Desktop.exe")];
        let cfg = serde_json::json!({ "artifacts": ["nonexistent-*"] });
        let err = AuthenticodeProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &artifacts, true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no signable artifact matched"));
    }

    #[test]
    fn signable_extension_is_case_insensitive() {
        assert!(is_signable("App.EXE"));
        assert!(is_signable("Setup.msi"));
        assert!(is_signable("driver.SYS"));
        assert!(!is_signable("yah"));
        assert!(!is_signable("Desktop.dmg"));
    }

    #[test]
    fn glob_match_supports_star() {
        assert!(glob_match("*.exe", "Desktop.exe"));
        assert!(glob_match("desktop", "desktop"));
        assert!(glob_match("yah-*", "yah-helper"));
        assert!(glob_match("*", "anything"));
        assert!(!glob_match("*.exe", "Desktop.msi"));
        assert!(!glob_match("desktop", "desktop-helper"));
    }

    #[test]
    fn sibling_signed_path_appends_suffix() {
        assert_eq!(
            sibling_signed_path("out/Desktop.exe"),
            PathBuf::from("out/Desktop.exe.signed")
        );
    }
}
