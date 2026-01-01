//! Release-channel publishing — the producer leg of the almanac releases feed.
//!
//! When a QED pipeline succeeds with an [`Outcome::Publish`](crate::types::Outcome::Publish),
//! the runner collects every [`ProducedArtifact`] declared by the successful
//! steps, lays them out into a release channel tree, writes a per-binary
//! `release-manifest.json` pointer, uploads the tree to the channel bucket, and
//! fires the almanac revalidate hook.
//!
//! ## Layout
//!
//! ```text
//! [<prefix>/]<binary>/<version>/<triple>/<filename>      ← the built artifacts
//! [<prefix>/]<binary>/release-manifest.json              ← shared pointer (this-stage view)
//! [<prefix>/]<binary>/release-manifest-<triple>.json     ← per-triple stable record
//! ```
//!
//! `release-manifest.json` is the file almanac's `R2Channel` reads
//! (`crates/yah/almanac/src/r2.rs`). Its wire shape is a forward-compatible
//! subset of `updater::ReleaseManifest` (self-updating-binaries.md): the fields
//! almanac needs (`version`, `pub_date`, `notes`, `host.bundle.<triple>`), so
//! the channel doubles as the almanac source AND the self-update pointer root.
//!
//! ## Multi-triple merge (R330-B8)
//!
//! In a multi-platform release, each `yah qed run release-build` invocation runs
//! on its own host (darwin-aarch64, linux-x86_64, …) and only knows about its
//! own triple's artifacts. The shared `<binary>/release-manifest.json` written
//! here therefore contains only this stage's triples — a sequential publish of
//! linux-x86_64 *after* darwin-aarch64 would overwrite the darwin view.
//!
//! To make cross-stage merge possible without R2 read-modify-write (which races
//! between concurrent publishes), `stage_release` ALSO writes a per-triple
//! manifest at `<binary>/release-manifest-<triple>.json` containing just that
//! triple's bundle entry. These keys are stable and idempotent: a re-run of the
//! same triple writes the same key, never clobbering a sibling triple's record.
//! The GHA assembly job (which already owns macOS code signing — warden can't
//! sign macOS) reads every `release-manifest-<triple>.json` and writes the
//! authoritative signed shared `release-manifest.json` once all triples land.
//!
//! ## What this module owns vs. delegates
//!
//! This module owns the *layout + manifest assembly* (pure, filesystem-only,
//! unit-tested with a tempdir). The actual bucket upload + hook POST are I/O
//! that vary by host, so they're delegated to a [`ReleasePublisher`] adapter —
//! the CLI supplies a Cloudflare-R2-backed impl that reuses the cloud crate's
//! `publish_to_r2`; tests supply a recording fake.
//!
//! Part of R330-F3 — canonical ticket annotation lives in `builtins.rs`.
//!
//! @yah:ticket(R488-F3, "ProducedArtifact aggregation across children into parent's Outcome::Publish (single revalidate)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:54:15Z)
//! @yah:status(review)
//! @yah:phase(P3)
//! @yah:parent(R488)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @yah:depends_on(R488-F2)
//! @yah:tier(Cleric)
//! @yah:handoff("F3 shipped. (a) End-to-end multi-child publish fan-in test: 3 SubPipeline children producing yah/desktop/mesofact binaries roll up into a single parent Outcome::Publish that fires StageRecorder.sync ONCE (6 staged objects: 3 binaries + 3 per-binary manifests) and StageRecorder.revalidate ONCE. Uses real PublishingOutcomeDispatcher with an Arc-wrapped ReleasePublisher fake — exercises stage_release end-to-end across composite runs. (b) Continue-on-error semantics pinned: SubPipeline step with on_fail=Continue marks itself failed but parent loop proceeds; sibling steps after run. Overall RunStatus stays Failed. Child produces dropped on failure — documented current behaviour. (c) load_and_validate_graph wired into both entry points: app/yah/cli/src/qed.rs (after placement gate, before proxy probe — pre-flight cycle/depth check on every yah qed run) AND app/yah/cli/src/camp.rs qed_run_handler (LoaderSubPipelineResolver attached to PipelineRunner so daemon resolves SubPipelines identically). New top-level re-exports in qed lib.rs: LoaderSubPipelineResolver, validate_sub_pipeline_graph, SubPipelineConfig/Ref/Collect/Resolver/Error, MAX_SUB_PIPELINE_DEPTH. 2 new runner tests (190 pass total, +2 from F2). cargo check --workspace clean.")
//! @yah:next("F4 (named output exposure): QedStep grows outputs: Vec<OutputDecl> and step results carry output values. SubPipelineCollect.outputs already exists from F1 — F4 wires propagation through the child run into parents expression context. Needs W200-F2 (expression engine) for parent-side substitution, OR a minimal qed-side substitution syntax that the W200 engine subsumes later.")
//! @yah:verify("cargo test -p qed --lib runner::tests::sub_pipeline (9 tests)")
//! @yah:verify("cargo test -p qed --lib")
//! @yah:verify("cargo check --workspace")

use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::runner::{OutcomeDispatcher, RunnerError};
use crate::types::ProducedArtifact;

/// Everything the dispatcher needs to publish one release: the destination,
/// the resolved version, and the artifacts collected from successful steps.
#[derive(Debug, Clone)]
pub struct PublishRequest {
    pub provider: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub base_url: Option<String>,
    pub version: String,
    pub artifacts: Vec<ProducedArtifact>,
}

/// Resolve the release version: `YAH_RELEASE_VERSION` env override (set by the
/// release tag / GHA), falling back to the version this binary was built at
/// (`CARGO_PKG_VERSION`, which is the workspace version — `version.workspace`).
pub fn resolve_release_version() -> String {
    std::env::var("YAH_RELEASE_VERSION")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
}

// ── Channel manifest wire types ─────────────────────────────────────────────
//
// A forward-compatible subset of `updater::ReleaseManifest`. We deliberately do
// not depend on the updater crate here: the producer only fills the fields it
// can know (version, pub_date, notes, per-triple url+size). The signing-only
// fields (`signature`, `ipc_contract`) are layered on by the GHA signing leg
// (warden can't sign macOS — see the builtin's gotcha). almanac's `R2Channel`
// reader ignores the signing fields, so the chain works with this subset.

/// `release-manifest.json` as emitted by the producer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelManifest {
    /// Release version, without a leading `v`.
    pub version: String,
    /// ISO-8601 UTC publish timestamp.
    pub pub_date: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    pub host: ChannelHost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelHost {
    /// Per-triple bundle pointers, keyed by triple shorthand.
    pub bundle: BTreeMap<String, ChannelBundle>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelBundle {
    /// Absolute download URL (when `base_url` is set) or a bucket-relative key.
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

/// Bucket key for the per-binary mutable pointer almanac re-fetches on push.
const MANIFEST_FILENAME: &str = "release-manifest.json";

/// Per-triple stable manifest filename (R330-B8). One per (binary, triple),
/// containing only that triple's bundle. Cross-stage merge fan-in feeds on these.
fn per_triple_manifest_filename(triple: &str) -> String {
    format!("release-manifest-{triple}.json")
}

/// Result of staging a release tree into a directory: the object keys written
/// (relative to the staging root) and the per-binary manifests.
#[derive(Debug, Clone, Default)]
pub struct StageReport {
    /// Artifact object keys, e.g. `yah/0.8.6/darwin-aarch64/yah`.
    pub object_keys: Vec<String>,
    /// Manifest object keys, e.g. `yah/release-manifest.json`.
    pub manifest_keys: Vec<String>,
    /// The emitted manifests, keyed by binary name.
    pub manifests: BTreeMap<String, ChannelManifest>,
}

/// Resolve a [`ProducedArtifact::triple`], defaulting to the build host's
/// triple in the `<os>-<arch>` shorthand the channel + updater use.
pub fn resolve_triple(triple: Option<&str>) -> String {
    if let Some(t) = triple.filter(|t| !t.is_empty()) {
        return t.to_string();
    }
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    };
    format!("{os}-{}", std::env::consts::ARCH)
}

fn join_key(prefix: Option<&str>, parts: &[&str]) -> String {
    let mut segs: Vec<&str> = Vec::new();
    if let Some(p) = prefix.map(str::trim).filter(|p| !p.is_empty()) {
        segs.push(p.trim_matches('/'));
    }
    segs.extend_from_slice(parts);
    segs.join("/")
}

/// Lay the artifacts out into `staging_dir` as the release channel tree and
/// write each binary's `release-manifest.json`. Pure filesystem work — no
/// network. The caller hands `staging_dir` to a [`ReleasePublisher`] to upload.
pub fn stage_release(
    staging_dir: &Path,
    artifacts: &[ProducedArtifact],
    version: &str,
    prefix: Option<&str>,
    base_url: Option<&str>,
) -> std::io::Result<StageReport> {
    let version = version.trim_start_matches('v').to_string();
    let mut report = StageReport::default();
    // binary -> (triple -> ChannelBundle), preserving deterministic order.
    let mut bundles: BTreeMap<String, BTreeMap<String, ChannelBundle>> = BTreeMap::new();

    for artifact in artifacts {
        let triple = resolve_triple(artifact.triple.as_deref());
        let src = Path::new(&artifact.path);
        let filename = src
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("artifact path has no filename: {}", artifact.path),
                )
            })?;

        let key = join_key(prefix, &[&artifact.binary, &version, &triple, filename]);
        let dest = staging_dir.join(&key);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = std::fs::copy(src, &dest)?;

        let url = match base_url.map(str::trim).filter(|b| !b.is_empty()) {
            Some(base) => format!("{}/{}", base.trim_end_matches('/'), key),
            None => key.clone(),
        };
        bundles
            .entry(artifact.binary.clone())
            .or_default()
            .insert(triple, ChannelBundle { url, size: Some(bytes) });
        report.object_keys.push(key);
    }

    let pub_date = Utc::now().to_rfc3339();
    for (binary, bundle) in bundles {
        // Per-triple stable manifests: one file per (binary, triple) containing
        // only that triple's bundle entry. Idempotent under repeated publishes
        // of the same triple; safe under concurrent publishes of different
        // triples (different keys). The GHA assembler reads these to build the
        // signed shared manifest. (R330-B8)
        for (triple, single_bundle) in &bundle {
            let mut per_triple_bundle: BTreeMap<String, ChannelBundle> = BTreeMap::new();
            per_triple_bundle.insert(triple.clone(), single_bundle.clone());
            let manifest = ChannelManifest {
                version: version.clone(),
                pub_date: pub_date.clone(),
                notes: None,
                host: ChannelHost { bundle: per_triple_bundle },
            };
            let manifest_key = join_key(
                prefix,
                &[&binary, &per_triple_manifest_filename(triple)],
            );
            let dest = staging_dir.join(&manifest_key);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let json = serde_json::to_vec_pretty(&manifest).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
            })?;
            std::fs::write(&dest, json)?;
            report.manifest_keys.push(manifest_key);
        }

        // Shared manifest: this-stage's view of all triples it built. For a
        // single-stage publish this is the authoritative manifest; for a
        // multi-stage publish it is "best-effort latest" until the GHA
        // assembler overwrites it with the merged signed version.
        let manifest = ChannelManifest {
            version: version.clone(),
            pub_date: pub_date.clone(),
            notes: None,
            host: ChannelHost { bundle },
        };
        let manifest_key = join_key(prefix, &[&binary, MANIFEST_FILENAME]);
        let dest = staging_dir.join(&manifest_key);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_vec_pretty(&manifest).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        std::fs::write(&dest, json)?;
        report.manifest_keys.push(manifest_key);
        report.manifests.insert(binary, manifest);
    }

    report.object_keys.sort();
    report.manifest_keys.sort();
    Ok(report)
}

// ── The publish adapter seam ────────────────────────────────────────────────

/// Performs the I/O the [`PublishingOutcomeDispatcher`] can't do itself:
/// uploading a staged channel tree to a bucket and firing the revalidate hook.
///
/// The qed crate stays dependency-light by keeping this abstract — the CLI
/// supplies a Cloudflare-R2-backed impl (reusing `cloud::publish_to_r2` + a
/// reqwest POST), and tests supply a recording fake.
#[async_trait]
pub trait ReleasePublisher: Send + Sync {
    /// Upload every file under `staging_dir` (already laid out as the channel
    /// tree) to `bucket` on `provider`, under the optional key `prefix`.
    async fn sync(
        &self,
        staging_dir: &Path,
        provider: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<(), RunnerError>;

    /// Fire the almanac revalidate hook so the feed re-fetches from this
    /// channel. A no-configured-receiver impl returns `Ok(())`.
    async fn revalidate(&self) -> Result<(), RunnerError>;
}

/// The real outcome dispatcher (R330-F3): stages produced artifacts into the
/// release channel layout, uploads them via a [`ReleasePublisher`], then fires
/// the revalidate hook. `warden_deploy` / `almanac_run` stay logging stubs
/// (those backends are still pending — R040-F4 / the almanac scheduler).
pub struct PublishingOutcomeDispatcher<P: ReleasePublisher> {
    publisher: P,
}

impl<P: ReleasePublisher> PublishingOutcomeDispatcher<P> {
    pub fn new(publisher: P) -> Self {
        Self { publisher }
    }
}

#[async_trait]
impl<P: ReleasePublisher> OutcomeDispatcher for PublishingOutcomeDispatcher<P> {
    async fn warden_deploy(&self, service: &str, env: &str) -> Result<(), RunnerError> {
        tracing::info!(
            service,
            env,
            "qed outcome: warden-deploy skipped (warden deploy RPC not yet stable, R040-F4)"
        );
        Ok(())
    }

    async fn almanac_run(&self, pipeline: &str) -> Result<(), RunnerError> {
        tracing::info!(pipeline, "qed outcome: almanac-run skipped (cadence scheduler pending)");
        Ok(())
    }

    async fn publish(&self, req: &PublishRequest) -> Result<(), RunnerError> {
        if req.artifacts.is_empty() {
            tracing::warn!(
                bucket = %req.bucket,
                "qed outcome: publish has no produced artifacts — \
                 declare `produces` on the build steps; skipping"
            );
            return Ok(());
        }

        let staging = tempfile::tempdir()?;
        let report = stage_release(
            staging.path(),
            &req.artifacts,
            &req.version,
            req.prefix.as_deref(),
            req.base_url.as_deref(),
        )?;
        tracing::info!(
            provider = %req.provider,
            bucket = %req.bucket,
            version = %req.version,
            objects = report.object_keys.len(),
            manifests = report.manifest_keys.len(),
            "qed outcome: staged release channel"
        );

        self.publisher
            .sync(staging.path(), &req.provider, &req.bucket, req.prefix.as_deref())
            .await?;
        self.publisher.revalidate().await?;
        Ok(())
    }
}

/// A [`ReleasePublisher`] that does nothing but log — the default when no real
/// bucket/receiver is wired (e.g. a local `yah qed run` with no credentials).
pub struct LoggingReleasePublisher;

#[async_trait]
impl ReleasePublisher for LoggingReleasePublisher {
    async fn sync(
        &self,
        staging_dir: &Path,
        provider: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<(), RunnerError> {
        tracing::info!(
            provider,
            bucket,
            prefix,
            staging = %staging_dir.display(),
            "qed publish: sync skipped (no real publisher wired)"
        );
        Ok(())
    }

    async fn revalidate(&self) -> Result<(), RunnerError> {
        tracing::info!("qed publish: revalidate hook skipped (no receiver configured)");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::TempDir;

    fn write_dummy(dir: &Path, rel: &str, contents: &[u8]) -> String {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, contents).unwrap();
        p.to_string_lossy().into_owned()
    }

    #[test]
    fn resolve_triple_uses_host_when_none() {
        let t = resolve_triple(None);
        assert!(t.contains('-'), "host triple shorthand has os-arch: {t}");
        assert_eq!(resolve_triple(Some("linux-x86_64")), "linux-x86_64");
        // Empty string falls back to host too.
        assert_eq!(resolve_triple(Some("")), resolve_triple(None));
    }

    #[test]
    fn stage_release_lays_out_channel_and_manifest() {
        let src = TempDir::new().unwrap();
        let yah_bin = write_dummy(src.path(), "target/release/yah", b"YAH-BINARY");

        let staging = TempDir::new().unwrap();
        let artifacts = vec![ProducedArtifact {
            binary: "yah".into(),
            path: yah_bin,
            triple: Some("darwin-aarch64".into()),
        }];

        let report = stage_release(
            staging.path(),
            &artifacts,
            "v0.8.6",
            None,
            Some("https://releases.yah.dev"),
        )
        .unwrap();

        // Artifact landed at <binary>/<version>/<triple>/<filename> (v stripped).
        assert_eq!(report.object_keys, vec!["yah/0.8.6/darwin-aarch64/yah"]);
        let copied = staging.path().join("yah/0.8.6/darwin-aarch64/yah");
        assert_eq!(std::fs::read(&copied).unwrap(), b"YAH-BINARY");

        // Manifests: both the shared key and a per-triple stable key (R330-B8).
        // Order: per-triple entries land first (inner loop), shared last.
        assert_eq!(
            report.manifest_keys,
            vec![
                "yah/release-manifest-darwin-aarch64.json",
                "yah/release-manifest.json",
            ]
        );
        let manifest = &report.manifests["yah"];
        assert_eq!(manifest.version, "0.8.6");
        let bundle = &manifest.host.bundle["darwin-aarch64"];
        assert_eq!(bundle.url, "https://releases.yah.dev/yah/0.8.6/darwin-aarch64/yah");
        assert_eq!(bundle.size, Some("YAH-BINARY".len() as u64));

        // The on-disk manifest round-trips through the same wire type.
        let bytes = std::fs::read(staging.path().join("yah/release-manifest.json")).unwrap();
        let parsed: ChannelManifest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(&parsed, manifest);
    }

    #[test]
    fn stage_release_relative_urls_without_base() {
        let src = TempDir::new().unwrap();
        let bin = write_dummy(src.path(), "out/desktop", b"x");
        let staging = TempDir::new().unwrap();
        let report = stage_release(
            staging.path(),
            &[ProducedArtifact { binary: "desktop".into(), path: bin, triple: Some("linux-x86_64".into()) }],
            "0.9.0",
            Some("channels"),
            None,
        )
        .unwrap();
        // Prefix is applied to both the object and the manifest keys.
        assert_eq!(report.object_keys, vec!["channels/desktop/0.9.0/linux-x86_64/desktop"]);
        assert_eq!(
            report.manifest_keys,
            vec![
                "channels/desktop/release-manifest-linux-x86_64.json",
                "channels/desktop/release-manifest.json",
            ]
        );
        // No base_url → manifest url is the bucket-relative key.
        assert_eq!(
            report.manifests["desktop"].host.bundle["linux-x86_64"].url,
            "channels/desktop/0.9.0/linux-x86_64/desktop"
        );
    }

    #[test]
    fn stage_release_groups_multiple_binaries() {
        let src = TempDir::new().unwrap();
        let yah = write_dummy(src.path(), "target/release/yah", b"a");
        let desktop = write_dummy(src.path(), "target/release/desktop", b"bb");
        let staging = TempDir::new().unwrap();
        let report = stage_release(
            staging.path(),
            &[
                ProducedArtifact { binary: "yah".into(), path: yah, triple: Some("darwin-aarch64".into()) },
                ProducedArtifact { binary: "desktop".into(), path: desktop, triple: Some("darwin-aarch64".into()) },
            ],
            "1.0.0",
            None,
            None,
        )
        .unwrap();
        // One shared manifest per binary, plus one per-triple stable manifest
        // per (binary, triple).
        assert_eq!(report.manifests.len(), 2);
        assert!(report.manifest_keys.contains(&"yah/release-manifest.json".to_string()));
        assert!(report.manifest_keys.contains(&"desktop/release-manifest.json".to_string()));
        assert!(report
            .manifest_keys
            .contains(&"yah/release-manifest-darwin-aarch64.json".to_string()));
        assert!(report
            .manifest_keys
            .contains(&"desktop/release-manifest-darwin-aarch64.json".to_string()));
    }

    /// R330-B8: simulate two sequential single-triple publishes (darwin then
    /// linux) and assert that the per-triple stable keys provide a non-
    /// clobbering record of both triples. The shared release-manifest.json
    /// would be overwritten by each stage (single-stage view), but the
    /// per-triple `release-manifest-<triple>.json` files coexist — the input
    /// the downstream merger reads.
    #[test]
    fn stage_release_per_triple_keys_survive_sequential_publishes() {
        let src = TempDir::new().unwrap();
        let yah_darwin = write_dummy(src.path(), "target/release/yah-darwin", b"D");
        let yah_linux = write_dummy(src.path(), "target/release/yah-linux", b"LL");

        // Simulate two sequential single-triple publishes into the SAME R2
        // bucket layout by staging both into a shared staging root.
        let staging = TempDir::new().unwrap();

        let report1 = stage_release(
            staging.path(),
            &[ProducedArtifact {
                binary: "yah".into(),
                path: yah_darwin,
                triple: Some("darwin-aarch64".into()),
            }],
            "0.8.6",
            None,
            Some("https://releases.yah.dev"),
        )
        .unwrap();
        let report2 = stage_release(
            staging.path(),
            &[ProducedArtifact {
                binary: "yah".into(),
                path: yah_linux,
                triple: Some("linux-x86_64".into()),
            }],
            "0.8.6",
            None,
            Some("https://releases.yah.dev"),
        )
        .unwrap();

        // Each stage emits its own per-triple key (idempotent, non-colliding).
        assert!(report1
            .manifest_keys
            .iter()
            .any(|k| k == "yah/release-manifest-darwin-aarch64.json"));
        assert!(report2
            .manifest_keys
            .iter()
            .any(|k| k == "yah/release-manifest-linux-x86_64.json"));

        // Both per-triple manifests survive on disk after the second stage
        // (the bug was: shared key clobbered, no record of the first triple).
        let darwin_path = staging.path().join("yah/release-manifest-darwin-aarch64.json");
        let linux_path = staging.path().join("yah/release-manifest-linux-x86_64.json");
        assert!(darwin_path.exists(), "darwin per-triple manifest must persist");
        assert!(linux_path.exists(), "linux per-triple manifest must persist");

        let darwin: ChannelManifest =
            serde_json::from_slice(&std::fs::read(&darwin_path).unwrap()).unwrap();
        let linux: ChannelManifest =
            serde_json::from_slice(&std::fs::read(&linux_path).unwrap()).unwrap();
        assert!(darwin.host.bundle.contains_key("darwin-aarch64"));
        assert_eq!(darwin.host.bundle.len(), 1, "per-triple manifest is single-triple");
        assert!(linux.host.bundle.contains_key("linux-x86_64"));
        assert_eq!(linux.host.bundle.len(), 1, "per-triple manifest is single-triple");

        // The shared release-manifest.json reflects the LAST stage (best-effort
        // latest single-stage view) — the GHA assembler is what unifies it.
        let shared_path = staging.path().join("yah/release-manifest.json");
        let shared: ChannelManifest =
            serde_json::from_slice(&std::fs::read(&shared_path).unwrap()).unwrap();
        assert!(
            shared.host.bundle.contains_key("linux-x86_64"),
            "shared manifest reflects the most recent stage"
        );
    }

    #[test]
    fn resolve_release_version_prefers_env() {
        // SAFETY: single-threaded test; we set + clear the override locally.
        std::env::set_var("YAH_RELEASE_VERSION", "9.9.9");
        assert_eq!(resolve_release_version(), "9.9.9");
        std::env::remove_var("YAH_RELEASE_VERSION");
        // Falls back to the compiled crate version (non-empty).
        assert!(!resolve_release_version().is_empty());
    }

    // ── PublishingOutcomeDispatcher with a recording publisher ──────────────

    #[derive(Default)]
    struct RecordingPublisher {
        synced: Mutex<Vec<String>>,
        revalidated: Mutex<u32>,
        /// Manifest contents captured from the staging dir at sync time.
        captured_manifests: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ReleasePublisher for RecordingPublisher {
        async fn sync(
            &self,
            staging_dir: &Path,
            _provider: &str,
            bucket: &str,
            _prefix: Option<&str>,
        ) -> Result<(), RunnerError> {
            // Confirm the staged tree actually exists at sync time (the
            // tempdir must outlive this call).
            let manifest = staging_dir.join("yah/release-manifest.json");
            let body = std::fs::read_to_string(&manifest).unwrap();
            self.captured_manifests.lock().unwrap().push(body);
            self.synced.lock().unwrap().push(bucket.to_string());
            Ok(())
        }

        async fn revalidate(&self) -> Result<(), RunnerError> {
            *self.revalidated.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn dispatcher_stages_uploads_and_revalidates() {
        use std::sync::Arc;
        let src = TempDir::new().unwrap();
        let bin = write_dummy(src.path(), "target/release/yah", b"BIN");
        let publisher = Arc::new(RecordingPublisher::default());

        // Build a dispatcher around a publisher we can inspect. The dispatcher
        // owns the publisher, so use an Arc clone for assertions.
        struct ArcPublisher(Arc<RecordingPublisher>);
        #[async_trait]
        impl ReleasePublisher for ArcPublisher {
            async fn sync(&self, d: &Path, p: &str, b: &str, pre: Option<&str>) -> Result<(), RunnerError> {
                self.0.sync(d, p, b, pre).await
            }
            async fn revalidate(&self) -> Result<(), RunnerError> {
                self.0.revalidate().await
            }
        }

        let dispatcher = PublishingOutcomeDispatcher::new(ArcPublisher(publisher.clone()));
        let req = PublishRequest {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: Some("https://releases.yah.dev".into()),
            version: "0.8.6".into(),
            artifacts: vec![ProducedArtifact {
                binary: "yah".into(),
                path: bin,
                triple: Some("darwin-aarch64".into()),
            }],
        };
        dispatcher.publish(&req).await.unwrap();

        assert_eq!(publisher.synced.lock().unwrap().as_slice(), ["yah-releases"]);
        assert_eq!(*publisher.revalidated.lock().unwrap(), 1);
        let manifest = &publisher.captured_manifests.lock().unwrap()[0];
        assert!(manifest.contains("0.8.6"), "manifest carries version: {manifest}");
        assert!(manifest.contains("darwin-aarch64"), "manifest carries triple");
    }

    #[tokio::test]
    async fn dispatcher_skips_when_no_artifacts() {
        let publisher = RecordingPublisher::default();
        // Move a probe out before constructing the dispatcher: read counters
        // after via a shared Arc instead.
        use std::sync::Arc;
        let probe = Arc::new(publisher);
        struct ArcPublisher(Arc<RecordingPublisher>);
        #[async_trait]
        impl ReleasePublisher for ArcPublisher {
            async fn sync(&self, d: &Path, p: &str, b: &str, pre: Option<&str>) -> Result<(), RunnerError> {
                self.0.sync(d, p, b, pre).await
            }
            async fn revalidate(&self) -> Result<(), RunnerError> {
                self.0.revalidate().await
            }
        }
        let dispatcher = PublishingOutcomeDispatcher::new(ArcPublisher(probe.clone()));
        let req = PublishRequest {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
            version: "0.8.6".into(),
            artifacts: vec![],
        };
        dispatcher.publish(&req).await.unwrap();
        assert!(probe.synced.lock().unwrap().is_empty(), "no artifacts → no sync");
        assert_eq!(*probe.revalidated.lock().unwrap(), 0);
    }
}
