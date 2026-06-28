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
use std::fs;
use std::path::{Path, PathBuf};

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

// ── Sigstore signing seam (R407-T5, W154) ──────────────────────────────────
//
// W154: "Sigstore signing extends to native tarballs (same trust model,
// different artifact shape)." For OCI images, cosign signs the registry
// digest (`cosign sign --yes <ref>@<digest>`). For tarballs, the equivalent
// is `cosign sign-blob --yes`, which produces a detached signature and an
// associated certificate / Rekor bundle. The trust model is the same:
// keyless OIDC via the GHA token, identity matched at verification time by
// regex against the workflow identity, transparency log entry in Rekor.
//
// This module owns the abstraction; the runner attaches a concrete signer
// via [`crate::runner::PipelineRunner::with_signer`]. The default in every
// constructor is [`LoggingSigner`] — local `yah qed run` flows write
// placeholder bytes and log a warning rather than fail when cosign isn't on
// PATH. A real release pipeline (GHA or yubaba-run) wires [`CosignSigner`]
// explicitly so an unsigned tarball never silently ships.

/// On-disk paths emitted by a successful [`SigstoreSigner::sign_blob`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedBlob {
    /// Detached signature, conventionally `<blob>.sig`.
    pub signature_path: PathBuf,
    /// Signing certificate (the leaf cert with the OIDC identity), `<blob>.crt`.
    pub certificate_path: PathBuf,
    /// Cosign bundle (signature + cert + Rekor inclusion proof), `<blob>.bundle`.
    /// `None` when the signer doesn't emit a bundle.
    pub bundle_path: Option<PathBuf>,
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
pub struct CosignSigner {
    pub cosign_bin: PathBuf,
}

impl Default for CosignSigner {
    fn default() -> Self {
        Self {
            cosign_bin: PathBuf::from("cosign"),
        }
    }
}

#[async_trait]
impl SigstoreSigner for CosignSigner {
    async fn sign_blob(&self, blob_path: &Path) -> std::io::Result<SignedBlob> {
        let sig = append_suffix(blob_path, ".sig");
        let crt = append_suffix(blob_path, ".crt");
        let bundle = append_suffix(blob_path, ".bundle");

        let status = tokio::process::Command::new(&self.cosign_bin)
            .arg("sign-blob")
            .arg("--yes")
            .arg("--output-signature")
            .arg(&sig)
            .arg("--output-certificate")
            .arg(&crt)
            .arg("--bundle")
            .arg(&bundle)
            .arg(blob_path)
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
            certificate_path: crt,
            bundle_path: Some(bundle),
        })
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
            certificate_path: crt,
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
        assert_eq!(
            signed.certificate_path,
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
        assert!(fs::read(&signed.certificate_path).unwrap().len() > 10);
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
        let signer = CosignSigner {
            cosign_bin: PathBuf::from("/definitely/not/a/real/cosign-binary"),
        };
        let err = signer.sign_blob(&blob).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }
}
