//! Native-tarball packaging (R407-T2, W154).
//!
//! Emits a `.tar.gz` containing a static musl Rust binary plus a workload-spec
//! manifest. Kamaji consumes the tarball at deploy time and directly
//! fork+exec+cgroup+pidfd-supervises the binary — no systemd Portable Service,
//! no per-workload `.service` unit. The tarball doubles as the deploy artifact
//! and the manifest-of-record describing how to launch the workload.
//!
//! ## Layout inside the tarball
//!
//! ```text
//! bin/<basename>          ← the static musl binary, 0o755
//! manifest.toml           ← [`NativeTarballManifest`] serialized
//! ```
//!
//! Pure filesystem work; the runner-side dispatch (catalog lookup, validation
//! that the catalog entry actually declares `produces = ["native-tarball"]`)
//! lives in [`crate::runner::PipelineRunner::execute_step_package_native_tarball`].

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};

/// The `manifest.toml` written into every native-tarball.
///
/// Forward-compatible — Kamaji readers should accept additive fields. Today
/// this carries the bare minimum needed to launch a workload: the binary's
/// in-tarball path, the target triple it was built for, and the env vars the
/// catalog entry declared. Capabilities, drain hooks, and probe shape land
/// alongside the Kamaji workload-spec proper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeTarballManifest {
    /// Catalog entry name (matches `[image].name` in the source TOML).
    pub name: String,
    /// Release version. Resolved at packaging time from
    /// `YAH_RELEASE_VERSION` env or the qed crate's compiled version
    /// (see [`crate::publish::resolve_release_version`]).
    pub version: String,
    /// Target-triple shorthand the binary was compiled for, e.g.
    /// `x86_64-unknown-linux-musl`.
    pub triple: String,
    /// Path to the executable *inside the tarball* (e.g. `bin/yubaba`).
    pub binary: String,
    /// Short human description (mirrors the catalog entry's `description`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Env vars the catalog entry declared. Kamaji applies these to the
    /// child before exec.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

/// Write `<binary>` and `manifest.toml` into a `.tar.gz` at `output_path`.
///
/// The output directory is created if missing. The binary is stored at
/// `bin/<filename>` inside the tarball with mode `0o755`; the manifest is
/// stored at the top level as `manifest.toml`. Existing `output_path` is
/// truncated.
pub fn pack_native_tarball(
    binary_path: &Path,
    manifest: &NativeTarballManifest,
    output_path: &Path,
) -> std::io::Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let manifest_toml = toml::to_string_pretty(manifest)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let file = fs::File::create(output_path)?;
    let gz = GzEncoder::new(file, Compression::default());
    let mut builder = tar::Builder::new(gz);

    let bin_basename = binary_path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("binary path has no filename: {}", binary_path.display()),
        )
    })?;
    let in_tar_path = format!("bin/{}", bin_basename.to_string_lossy());

    let mut bin = fs::File::open(binary_path)?;
    let bin_meta = bin.metadata()?;
    let mut bin_header = tar::Header::new_gnu();
    bin_header.set_size(bin_meta.len());
    bin_header.set_mode(0o755);
    bin_header.set_mtime(0);
    bin_header.set_cksum();
    builder.append_data(&mut bin_header, &in_tar_path, &mut bin)?;

    let manifest_bytes = manifest_toml.as_bytes();
    let mut manifest_header = tar::Header::new_gnu();
    manifest_header.set_size(manifest_bytes.len() as u64);
    manifest_header.set_mode(0o644);
    manifest_header.set_mtime(0);
    manifest_header.set_cksum();
    builder.append_data(&mut manifest_header, "manifest.toml", manifest_bytes)?;

    let gz = builder.into_inner()?;
    gz.finish()?;
    Ok(())
}

/// Filesystem-safe tarball stem for a catalog image + triple pair. The runner
/// uses this for the on-disk filename so packaging and signing both resolve
/// the same path without re-deriving it (R407-T2 / R407-T5).
pub fn tarball_stem(image_name: &str, triple: &str) -> String {
    let raw = format!("{image_name}-{triple}");
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Path the packaging step writes (and the signing step reads) under
/// `<camp_root>/.yah/cache/native/<safe-stem>.tar.gz`. Single source of truth
/// for the on-disk convention so signing never drifts from packaging.
pub fn native_tarball_output_path(camp_root: &Path, image_name: &str, triple: &str) -> PathBuf {
    camp_root
        .join(".yah/cache/native")
        .join(format!("{}.tar.gz", tarball_stem(image_name, triple)))
}

// ── Sigstore signing seam (R407-T5, W154; identity split R605-F1) ──────────
//
// W154: "Sigstore signing extends to native tarballs (same trust model,
// different artifact shape)." For OCI images, cosign signs the registry
// digest (`cosign sign --yes <ref>@<digest>`). For tarballs, the equivalent
// is `cosign sign-blob --yes`, which produces a detached signature and an
// associated certificate / Rekor bundle.
//
// R605-F1 — *which* identity backs that signature is now explicit
// ([`SigningIdentity`]), because it is exactly the thing that changes when a
// release stops being cut on GitHub:
//
//   * [`SigningIdentity::Keyless`] — Fulcio mints a short-lived cert against
//     an OIDC token. This is what GHA does today and stays the default. It is
//     NOT portable off GitHub on its own: the public-good Fulcio only issues
//     certs for OIDC issuers on its own configured allowlist (per Sigstore's
//     "OIDC in Fulcio" docs — Dex/Google/GitHub/GitLab/SPIFFE/Kubernetes), so
//     a camp-minted issuer cannot get a cert from it. The `identity_token`
//     field carries a pre-minted token (`cosign --identity-token`) for the day
//     a trusted issuer exists — a private Fulcio, or an upstream-registered
//     one — but it does not conjure that trust into being.
//
//   * [`SigningIdentity::Key`] — a cosign key pair (`cosign.key` file or a KMS
//     URI: `awskms://…`, `hashivault://…`, `k8s://…`). No OIDC, no Fulcio, so
//     it works anywhere QED runs. This is the posture W235 §"Off-GHA forfeits
//     GitHub OIDC keyless cosign" already committed to: "container signing
//     moves to key-based cosign with the key vaulted in kamaji … a deliberate
//     supply-chain posture change, not a transparent swap." The per-run vault
//     grant that hands the key to a remote run is R555-F5.
//
// This module owns the abstraction; the runner attaches a concrete signer
// via [`crate::runner::PipelineRunner::with_signer`]. The default in every
// constructor is [`LoggingSigner`] — local `yah qed run` flows write
// placeholder bytes and log a warning rather than fail when cosign isn't on
// PATH. A release pipeline wires a real signer via [`resolve_signer`], which
// reads the identity out of the environment and refuses to silently downgrade
// to placeholders once one is configured.

/// On-disk paths emitted by a successful [`SigstoreSigner::sign_blob`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedBlob {
    /// Detached signature, conventionally `<blob>.sig`.
    pub signature_path: PathBuf,
    /// Signing certificate (the leaf cert with the OIDC identity), `<blob>.crt`.
    ///
    /// `None` for [`SigningIdentity::Key`] — a key-based signature has no
    /// Fulcio certificate to emit, and verification pins the public key
    /// instead of a certificate identity.
    pub certificate_path: Option<PathBuf>,
    /// Cosign bundle (signature + cert + Rekor inclusion proof), `<blob>.bundle`.
    /// `None` when the signer doesn't emit a bundle.
    pub bundle_path: Option<PathBuf>,
}

/// Which Sigstore identity backs a signature (R605-F1).
///
/// See the module comment for why the two arms are not interchangeable off
/// GitHub. `Default` is [`Self::Keyless`] with no pre-minted token — byte-for
/// byte the pre-R605 behaviour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SigningIdentity {
    /// Fulcio-issued short-lived certificate bound to an OIDC identity.
    Keyless {
        /// Pre-minted OIDC token passed as `--identity-token`. `None` lets
        /// cosign discover one from the ambient CI environment (what GHA's
        /// `id-token: write` permission provides).
        identity_token: Option<String>,
    },
    /// Long-lived cosign key pair. `key_ref` is a path to a `cosign.key` or a
    /// KMS URI — whatever `cosign sign-blob --key` accepts.
    Key { key_ref: String },
}

impl Default for SigningIdentity {
    fn default() -> Self {
        Self::Keyless {
            identity_token: None,
        }
    }
}

/// Env var naming the cosign key (file path or KMS URI) for key-based signing.
pub const ENV_COSIGN_KEY: &str = "QED_COSIGN_KEY";
/// Env var carrying a pre-minted OIDC token for keyless signing.
pub const ENV_COSIGN_IDENTITY_TOKEN: &str = "QED_COSIGN_IDENTITY_TOKEN";

impl SigningIdentity {
    /// Read the identity out of the environment.
    ///
    /// `QED_COSIGN_KEY` wins when both are set — an operator who vaulted a key
    /// into the run meant to use it, and silently preferring an ambient OIDC
    /// token would sign with a different identity than they configured.
    /// Returns `None` when neither is set, which is the signal that no signing
    /// identity was configured at all (see [`resolve_signer`]).
    pub fn from_env() -> Option<Self> {
        Self::from_env_with(|k| std::env::var(k).ok())
    }

    /// [`Self::from_env`] with an injectable lookup, so tests don't mutate
    /// process-global env (which races across the test harness's threads).
    pub fn from_env_with(lookup: impl Fn(&str) -> Option<String>) -> Option<Self> {
        let non_empty = |k: &str| lookup(k).filter(|v| !v.trim().is_empty());
        if let Some(key_ref) = non_empty(ENV_COSIGN_KEY) {
            return Some(Self::Key { key_ref });
        }
        non_empty(ENV_COSIGN_IDENTITY_TOKEN).map(|t| Self::Keyless {
            identity_token: Some(t),
        })
    }
}

/// Sign a single blob (a native tarball, conventionally) with the same
/// Sigstore keyless OIDC trust model that signs the OCI images today. The
/// signer writes the resulting `.sig` / `.crt` / `.bundle` files next to the
/// blob and reports their paths back so the caller can publish them.
#[async_trait]
pub trait SigstoreSigner: Send + Sync {
    async fn sign_blob(&self, blob_path: &Path) -> std::io::Result<SignedBlob>;
}

/// Append `suffix` (e.g. `.sig`) to the blob's full filename — extends, does
/// not replace. `Path::with_extension` would turn `foo.tar.gz` into
/// `foo.tar.sig`; we want `foo.tar.gz.sig` so the channel layout shows the
/// signature next to the artifact it covers.
fn append_suffix(blob: &Path, suffix: &str) -> PathBuf {
    let mut s = blob.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

/// Default production signer — shells out to `cosign sign-blob --yes`.
/// Set `cosign_bin` to `"cosign"` (PATH lookup) or an absolute path; a
/// missing binary surfaces as a `NotFound` IO error so the runner reports a
/// clean step-failure message at the call site.
///
/// `identity` picks the trust model (R605-F1). For
/// [`SigningIdentity::Key`] with a passphrase-protected key file, cosign
/// reads the passphrase from `COSIGN_PASSWORD` in the inherited environment —
/// the signer does not plumb it, so the caller (or the vault grant that
/// materialised the key) must set it, including to the empty string for a
/// passphrase-less key.
pub struct CosignSigner {
    pub cosign_bin: PathBuf,
    pub identity: SigningIdentity,
}

impl Default for CosignSigner {
    fn default() -> Self {
        Self {
            cosign_bin: PathBuf::from("cosign"),
            identity: SigningIdentity::default(),
        }
    }
}

impl CosignSigner {
    /// Keyless against the ambient CI OIDC token — today's GHA behaviour.
    pub fn keyless() -> Self {
        Self::default()
    }

    /// Key-based signing. `key_ref` is a `cosign.key` path or a KMS URI.
    pub fn with_key(key_ref: impl Into<String>) -> Self {
        Self {
            identity: SigningIdentity::Key {
                key_ref: key_ref.into(),
            },
            ..Self::default()
        }
    }

    /// Point at a non-PATH cosign binary (tests, pinned installs).
    pub fn with_bin(mut self, cosign_bin: impl Into<PathBuf>) -> Self {
        self.cosign_bin = cosign_bin.into();
        self
    }

    /// Whether this identity produces a Fulcio certificate alongside the
    /// signature. Key-based signing does not.
    fn emits_certificate(&self) -> bool {
        matches!(self.identity, SigningIdentity::Keyless { .. })
    }

    /// Build the `cosign sign-blob` argv (minus the binary itself).
    ///
    /// Split out from [`SigstoreSigner::sign_blob`] so the flag shape per
    /// identity is unit-testable without a cosign install — the flags are the
    /// whole of what R605-F1 changes, and they only ever run for real on a
    /// release cut.
    fn sign_blob_argv(
        &self,
        blob_path: &Path,
        sig: &Path,
        crt: &Path,
        bundle: &Path,
    ) -> Vec<OsString> {
        let mut argv: Vec<OsString> = vec!["sign-blob".into(), "--yes".into()];
        match &self.identity {
            SigningIdentity::Keyless { identity_token } => {
                if let Some(token) = identity_token {
                    argv.push("--identity-token".into());
                    argv.push(token.into());
                }
                argv.push("--output-certificate".into());
                argv.push(crt.into());
            }
            SigningIdentity::Key { key_ref } => {
                argv.push("--key".into());
                argv.push(key_ref.into());
                // Key-based signing has no OIDC identity for Rekor to attest
                // to, and the whole point of this arm (W235) is not
                // depending on public Sigstore infra to cut a release. The
                // keyless arm keeps tlog upload on — that public record IS
                // the point of Fulcio/Rekor certificate transparency.
                argv.push("--tlog-upload=false".into());
            }
        }
        argv.push("--output-signature".into());
        argv.push(sig.into());
        argv.push("--bundle".into());
        argv.push(bundle.into());
        argv.push(blob_path.into());
        argv
    }
}

#[async_trait]
impl SigstoreSigner for CosignSigner {
    async fn sign_blob(&self, blob_path: &Path) -> std::io::Result<SignedBlob> {
        let sig = append_suffix(blob_path, ".sig");
        let crt = append_suffix(blob_path, ".crt");
        let bundle = append_suffix(blob_path, ".bundle");

        let status = tokio::process::Command::new(&self.cosign_bin)
            .args(self.sign_blob_argv(blob_path, &sig, &crt, &bundle))
            .status()
            .await?;
        if !status.success() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "cosign sign-blob exited with status {} (blob: {})",
                    status,
                    blob_path.display(),
                ),
            ));
        }
        Ok(SignedBlob {
            signature_path: sig,
            certificate_path: self.emits_certificate().then_some(crt),
            bundle_path: Some(bundle),
        })
    }
}

/// Pick the signer a pipeline run should use, from the environment.
///
/// The R605-F1 anti-footgun: once a signing identity is configured
/// ([`SigningIdentity::from_env`]), this returns a real [`CosignSigner`] and a
/// missing cosign binary becomes a hard step failure at sign time. With no
/// identity configured it falls back to [`LoggingSigner`] — a local
/// `yah qed run` still completes, loudly, with placeholder bytes.
///
/// This is what closes the runner.rs gotcha "release CI MUST wire CosignSigner
/// explicitly … picking up the default in CI ships a tarball with stub files":
/// CI now only has to export `QED_COSIGN_KEY`.
pub fn resolve_signer() -> Arc<dyn SigstoreSigner> {
    match SigningIdentity::from_env() {
        Some(identity) => {
            tracing::info!(
                identity = match &identity {
                    SigningIdentity::Key { .. } => "key",
                    SigningIdentity::Keyless { .. } => "keyless",
                },
                "qed: signing with cosign"
            );
            Arc::new(CosignSigner {
                identity,
                ..CosignSigner::default()
            })
        }
        None => {
            tracing::debug!(
                "qed: no signing identity configured ({ENV_COSIGN_KEY} / \
                 {ENV_COSIGN_IDENTITY_TOKEN} unset) — using LoggingSigner placeholders"
            );
            Arc::new(LoggingSigner)
        }
    }
}

/// Test / local-dev fake — writes deterministic placeholder bytes so a
/// pipeline's `sign-native-tarball` step succeeds without a real cosign
/// install. NOT suitable for releases; release CI must wire [`CosignSigner`]
/// explicitly.
pub struct LoggingSigner;

#[async_trait]
impl SigstoreSigner for LoggingSigner {
    async fn sign_blob(&self, blob_path: &Path) -> std::io::Result<SignedBlob> {
        if !blob_path.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("blob to sign not found: {}", blob_path.display()),
            ));
        }
        let sig = append_suffix(blob_path, ".sig");
        let crt = append_suffix(blob_path, ".crt");
        let bundle = append_suffix(blob_path, ".bundle");
        fs::write(
            &sig,
            b"# yah logging-signer: placeholder signature (NOT a real cosign signature)\n",
        )?;
        fs::write(
            &crt,
            b"# yah logging-signer: placeholder certificate (NOT a real cosign cert)\n",
        )?;
        fs::write(
            &bundle,
            b"{\"_comment\":\"yah logging-signer placeholder bundle\"}\n",
        )?;
        tracing::warn!(
            blob = %blob_path.display(),
            "qed sign-native-tarball: LoggingSigner emitted placeholder \
             .sig/.crt/.bundle (cosign not wired)"
        );
        Ok(SignedBlob {
            signature_path: sig,
            certificate_path: Some(crt),
            bundle_path: Some(bundle),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::{Read, Write};
    use tempfile::TempDir;

    fn write_dummy_binary(dir: &Path, name: &str, body: &[u8]) -> std::path::PathBuf {
        let path = dir.join(name);
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(body).unwrap();
        path
    }

    fn sample_manifest() -> NativeTarballManifest {
        NativeTarballManifest {
            name: "yah-yubaba".into(),
            version: "0.8.6".into(),
            triple: "x86_64-unknown-linux-musl".into(),
            binary: "bin/yubaba".into(),
            description: Some("Native musl-static yubaba".into()),
            env: BTreeMap::from([("RUST_LOG".into(), "info".into())]),
        }
    }

    fn list_tar_entries(path: &Path) -> Vec<(String, Vec<u8>, u32)> {
        let f = fs::File::open(path).unwrap();
        let gz = GzDecoder::new(f);
        let mut archive = tar::Archive::new(gz);
        let mut out = Vec::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let header_path = entry.path().unwrap().to_string_lossy().into_owned();
            let mode = entry.header().mode().unwrap();
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf).unwrap();
            out.push((header_path, buf, mode));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    #[test]
    fn pack_writes_binary_and_manifest_with_expected_modes() {
        let dir = TempDir::new().unwrap();
        let bin = write_dummy_binary(dir.path(), "yubaba", b"\x7fELF-fake-musl-binary");
        let out = dir
            .path()
            .join("out/yah-yubaba-x86_64-unknown-linux-musl.tar.gz");

        let manifest = sample_manifest();
        pack_native_tarball(&bin, &manifest, &out).unwrap();

        assert!(out.is_file(), "tarball materialised at {}", out.display());
        let entries = list_tar_entries(&out);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "bin/yubaba");
        assert_eq!(entries[0].1, b"\x7fELF-fake-musl-binary");
        assert_eq!(entries[0].2, 0o755);
        assert_eq!(entries[1].0, "manifest.toml");
        assert_eq!(entries[1].2, 0o644);
    }

    #[test]
    fn pack_manifest_roundtrips_through_toml() {
        let dir = TempDir::new().unwrap();
        let bin = write_dummy_binary(dir.path(), "yubaba", b"x");
        let out = dir.path().join("yubaba.tar.gz");
        let manifest = sample_manifest();
        pack_native_tarball(&bin, &manifest, &out).unwrap();

        let entries = list_tar_entries(&out);
        let manifest_entry = entries
            .iter()
            .find(|(p, _, _)| p == "manifest.toml")
            .expect("manifest.toml present");
        let text = std::str::from_utf8(&manifest_entry.1).unwrap();
        let parsed: NativeTarballManifest = toml::from_str(text).expect("manifest.toml parses");
        assert_eq!(parsed, manifest);
    }

    #[test]
    fn pack_creates_missing_parent_dirs() {
        let dir = TempDir::new().unwrap();
        let bin = write_dummy_binary(dir.path(), "yubaba", b"x");
        let out = dir.path().join("deeply/nested/path/yubaba.tar.gz");
        pack_native_tarball(&bin, &sample_manifest(), &out).unwrap();
        assert!(out.is_file());
    }

    #[test]
    fn pack_missing_binary_is_io_error() {
        let dir = TempDir::new().unwrap();
        let bogus = dir.path().join("does-not-exist");
        let out = dir.path().join("out.tar.gz");
        let err = pack_native_tarball(&bogus, &sample_manifest(), &out).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    // ── R407-T5 path helper + signer ──────────────────────────────────────

    #[test]
    fn tarball_stem_replaces_unsafe_chars() {
        assert_eq!(
            tarball_stem("yah-yubaba", "x86_64-unknown-linux-musl"),
            "yah-yubaba-x86_64-unknown-linux-musl",
        );
        // `/` and `:` are not in the [A-Za-z0-9_.-] allowlist — both rewrite.
        assert_eq!(
            tarball_stem("ghcr.io/yah-ai/yah-yubaba", "linux:musl"),
            "ghcr.io_yah-ai_yah-yubaba-linux_musl",
        );
    }

    #[test]
    fn native_tarball_output_path_matches_runner_convention() {
        // The runner writes/reads `<camp>/.yah/cache/native/<stem>.tar.gz` —
        // this helper is the single source of truth, so packaging (T2) and
        // signing (T5) never drift.
        let camp = Path::new("/camp");
        let out = native_tarball_output_path(camp, "yah-yubaba", "x86_64-unknown-linux-musl");
        assert_eq!(
            out,
            Path::new("/camp/.yah/cache/native/yah-yubaba-x86_64-unknown-linux-musl.tar.gz"),
        );
    }

    #[tokio::test]
    async fn logging_signer_writes_placeholder_sig_crt_bundle_next_to_blob() {
        let dir = TempDir::new().unwrap();
        let blob = dir
            .path()
            .join("yah-yubaba-x86_64-unknown-linux-musl.tar.gz");
        fs::write(&blob, b"<fake tarball bytes>").unwrap();

        let signer = LoggingSigner;
        let signed = signer.sign_blob(&blob).await.unwrap();

        // Suffixes are appended, not substituted — keep the `.tar.gz` so the
        // signature reads next to its artifact in the channel layout.
        assert_eq!(
            signed.signature_path,
            blob.with_file_name(format!(
                "{}.sig",
                blob.file_name().unwrap().to_string_lossy()
            )),
        );
        let cert = signed
            .certificate_path
            .clone()
            .expect("LoggingSigner mirrors the keyless shape, cert included");
        assert_eq!(
            cert,
            blob.with_file_name(format!(
                "{}.crt",
                blob.file_name().unwrap().to_string_lossy()
            )),
        );
        let bundle = signed.bundle_path.expect("LoggingSigner emits a bundle");
        assert_eq!(
            bundle,
            blob.with_file_name(format!(
                "{}.bundle",
                blob.file_name().unwrap().to_string_lossy()
            )),
        );

        // The placeholder files are non-empty so downstream tooling that
        // counts bytes / hashes contents doesn't get an empty-file footgun.
        assert!(fs::read(&signed.signature_path).unwrap().len() > 10);
        assert!(fs::read(&cert).unwrap().len() > 10);
        assert!(fs::read(&bundle).unwrap().len() > 10);
    }

    #[tokio::test]
    async fn logging_signer_missing_blob_is_not_found_error() {
        let dir = TempDir::new().unwrap();
        let bogus = dir.path().join("does-not-exist.tar.gz");
        let err = LoggingSigner.sign_blob(&bogus).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn cosign_signer_missing_binary_surfaces_not_found() {
        // No cosign on PATH in the test sandbox — point at an absolute path
        // we know doesn't exist. Exercises the std::io::ErrorKind::NotFound
        // branch the runner converts into a `StepFailed` with a clean
        // operator-facing message.
        let dir = TempDir::new().unwrap();
        let blob = dir.path().join("artifact.tar.gz");
        fs::write(&blob, b"x").unwrap();
        let signer = CosignSigner::keyless().with_bin("/definitely/not/a/real/cosign-binary");
        let err = signer.sign_blob(&blob).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    // ── R605-F1 signing identity ──────────────────────────────────────────

    fn argv_of(signer: &CosignSigner) -> Vec<String> {
        signer
            .sign_blob_argv(
                Path::new("/a/x.tar.gz"),
                Path::new("/a/x.tar.gz.sig"),
                Path::new("/a/x.tar.gz.crt"),
                Path::new("/a/x.tar.gz.bundle"),
            )
            .into_iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn keyless_argv_is_unchanged_from_the_gha_shape() {
        // The pre-R605 argv, byte for byte — GHA's release.yml signs with
        // exactly `sign-blob --yes --output-signature … --output-certificate …`,
        // so the default arm must not drift.
        assert_eq!(
            argv_of(&CosignSigner::keyless()),
            vec![
                "sign-blob",
                "--yes",
                "--output-certificate",
                "/a/x.tar.gz.crt",
                "--output-signature",
                "/a/x.tar.gz.sig",
                "--bundle",
                "/a/x.tar.gz.bundle",
                "/a/x.tar.gz",
            ],
        );
    }

    #[test]
    fn keyless_with_token_passes_identity_token() {
        let signer = CosignSigner {
            identity: SigningIdentity::Keyless {
                identity_token: Some("eyJhbGc.camp-minted".into()),
            },
            ..CosignSigner::default()
        };
        let argv = argv_of(&signer);
        let i = argv.iter().position(|a| a == "--identity-token").unwrap();
        assert_eq!(argv[i + 1], "eyJhbGc.camp-minted");
    }

    #[test]
    fn key_argv_uses_key_and_emits_no_certificate() {
        // A key-based signature has no Fulcio cert; asking cosign to write one
        // is what would fail the step on a real release cut.
        let argv = argv_of(&CosignSigner::with_key("awskms:///alias/yah-release"));
        assert_eq!(
            argv,
            vec![
                "sign-blob",
                "--yes",
                "--key",
                "awskms:///alias/yah-release",
                "--tlog-upload=false",
                "--output-signature",
                "/a/x.tar.gz.sig",
                "--bundle",
                "/a/x.tar.gz.bundle",
                "/a/x.tar.gz",
            ],
        );
        assert!(!argv.iter().any(|a| a == "--output-certificate"));
    }

    #[test]
    fn key_argv_disables_tlog_upload_but_keyless_does_not() {
        // Live-verified 2026-08-16 against a real cosign v2.6.5 binary + a
        // real (test) key pair: omitting --tlog-upload=false on the Key arm
        // silently submits a public, permanent Rekor entry for a signature
        // that has no OIDC identity to attest to — the opposite of what
        // moving to key-based signing (off public Sigstore infra) is for.
        assert!(argv_of(&CosignSigner::with_key("/vaulted/cosign.key"))
            .iter()
            .any(|a| a == "--tlog-upload=false"));
        assert!(!argv_of(&CosignSigner::keyless())
            .iter()
            .any(|a| a == "--tlog-upload=false"));
    }

    #[tokio::test]
    async fn key_signing_reports_no_certificate_path() {
        // Can't run cosign in the sandbox, so assert the mapping directly:
        // the Key arm must not claim a `.crt` the signer never wrote.
        assert!(!CosignSigner::with_key("cosign.key").emits_certificate());
        assert!(CosignSigner::keyless().emits_certificate());
    }

    #[test]
    fn from_env_prefers_key_over_ambient_token() {
        let both = SigningIdentity::from_env_with(|k| match k {
            ENV_COSIGN_KEY => Some("cosign.key".into()),
            ENV_COSIGN_IDENTITY_TOKEN => Some("tok".into()),
            _ => None,
        });
        assert_eq!(
            both,
            Some(SigningIdentity::Key {
                key_ref: "cosign.key".into()
            })
        );
    }

    #[test]
    fn from_env_is_none_when_unset_or_blank() {
        assert_eq!(SigningIdentity::from_env_with(|_| None), None);
        // A CI `export QED_COSIGN_KEY=` (unset secret) must read as "not
        // configured", not as a key literally named empty-string.
        assert_eq!(
            SigningIdentity::from_env_with(|_| Some("   ".into())),
            None
        );
    }

    #[test]
    fn from_env_token_only_is_keyless() {
        assert_eq!(
            SigningIdentity::from_env_with(|k| (k == ENV_COSIGN_IDENTITY_TOKEN)
                .then(|| "tok".to_string())),
            Some(SigningIdentity::Keyless {
                identity_token: Some("tok".into())
            })
        );
    }
}
