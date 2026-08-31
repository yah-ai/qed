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
//! The GHA assembly job (which already owns macOS code signing — yubaba can't
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
// (yubaba can't sign macOS — see the builtin's gotcha). almanac's `R2Channel`
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
    /// Canonical asset hash, ALWAYS algorithm-tagged (`blake3:<hex>`) — R330-F40.
    ///
    /// Until this existed the channel manifest carried a URL and a size and
    /// nothing else, so every download this publisher produced rendered with no
    /// way to verify it. A bare hex digest is deliberately not an option here:
    /// the index this feeds is permanent, so an untagged digest written into it
    /// would be untagged forever.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

/// Bucket key for the per-binary mutable pointer almanac re-fetches on push.
const MANIFEST_FILENAME: &str = "release-manifest.json";

/// Filename of the IMMUTABLE per-version copy of a binary's manifest, written
/// alongside the mutable pointer at `<binary>/<version>/manifest.json`.
///
/// The mutable pointer answers "what is current" and is rewritten by every
/// release; this copy answers "what was 0.8.21" and never changes, so it is
/// safe to cache forever and safe for the version index to link to. Same split
/// (and same key) as `cli-release-manifest` in `.github/workflows/release.yml`,
/// which writes `s3://yah-dev/yah/<version>/manifest.json` for the same reason.
const VERSIONED_MANIFEST_FILENAME: &str = "manifest.json";

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

// ── The accumulating version index (R330-T32) ────────────────────────────────
//
// `release-manifest.json` is ONE version by construction — it answers "what is
// current". The /releases page is a HISTORY, so it reads a separate object that
// accumulates: `<prefix>/<binary>/index.json`, parsed by almanac's `R2Index`
// source. Publishing only the pointer is why that page was blank — there was a
// producer and a consumer and no object between them.
//
// Shape is almanac's `IndexManifest`/`IndexVersion`/`TripleEntry`. Only `url` is
// required over there; everything else is optional, so this stays additive.

/// The whole index object as published at `<binary>/index.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseIndex {
    pub name: String,
    /// Read but never matched on by the consumer — a producer may add fields
    /// without stranding a deployed reader. A breaking change gets a new key.
    pub schema: u32,
    pub updated_at: String,
    pub versions: Vec<IndexVersion>,
}

/// One published version inside [`ReleaseIndex`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexVersion {
    pub version: String,
    pub pub_date: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_url: Option<String>,
    pub triples: BTreeMap<String, IndexTriple>,
}

/// One (version, triple) download inside [`IndexVersion`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexTriple {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Tagged `blake3:<hex>`. See [`ChannelBundle::hash`] for why never bare.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

/// Fold one release into the index, returning the bytes to publish.
///
/// `existing` is the current object's bytes, or `None` for the create case (a
/// missing key is a first release, not an error).
///
/// Two invariants this exists to hold, both learned the hard way:
///
/// 1. **Replace-or-append, never a bare push.** Re-publishing a version updates
///    its entry instead of duplicating it, which is what makes retrying a
///    failed publish safe.
/// 2. **A version keeps the `pub_date` it was FIRST published with.** Consumers
///    order this list by date (almanac re-sorts on read, and the page sorts
///    again), so restamping on a re-publish would not merely edit a field — it
///    would move an old release to the top of /releases and present it as the
///    newest. Publication dates are historical facts.
/// 3. **Replace-or-append applies PER TRIPLE, not to the map wholesale.** A
///    release matrix lands in slices: each platform leg stages only the
///    artifacts it built, so its [`IndexUpdate`] carries only its own triples.
///    Taking the update's map as the version's whole map means the last leg to
///    finish wins and every earlier platform disappears — which is why the live
///    index carried exactly one triple and /releases showed "Not published yet"
///    for Linux and Windows. Worse, the CAS in the caller *guarantees* that
///    outcome rather than racing for it: a loser re-reads the winner's bytes and
///    then discards the very triples the re-read fetched. Unioning here is what
///    makes that conditional write actually converge.
///
///    The cost is that a triple can only be updated, never dropped, by
///    publishing. That is the right trade for a permanent record — a stale entry
///    is a visible wrong URL, a vanished one is a download that silently stops
///    existing — and removing one is a deliberate hand-edit of the object.
pub fn merge_index(
    existing: Option<&str>,
    binary: &str,
    version: &str,
    pub_date: &str,
    manifest_url: Option<String>,
    triples: BTreeMap<String, IndexTriple>,
) -> Result<String, serde_json::Error> {
    let version = version.trim_start_matches('v').to_string();
    let mut prior: Vec<IndexVersion> = match existing.map(str::trim).filter(|s| !s.is_empty()) {
        Some(raw) => serde_json::from_str::<ReleaseIndex>(raw)?.versions,
        None => Vec::new(),
    };

    let previous = prior.iter().find(|v| v.version == version).cloned();
    // Prior triples first, this update's on top: a leg re-publishing a triple it
    // already wrote replaces that entry, and every triple it did not build
    // survives untouched.
    let mut merged_triples = previous
        .as_ref()
        .map(|v| v.triples.clone())
        .unwrap_or_default();
    merged_triples.extend(triples);

    prior.retain(|v| v.version != version);
    prior.push(IndexVersion {
        version,
        pub_date: previous
            .as_ref()
            .map(|v| v.pub_date.clone())
            .unwrap_or_else(|| pub_date.to_string()),
        // Same rule one field over: a leg that cannot derive the URL (no
        // `base_url` configured) must not erase one an earlier leg did.
        manifest_url: manifest_url.or_else(|| previous.and_then(|v| v.manifest_url)),
        triples: merged_triples,
    });

    // Newest first, and deterministic: two entries sharing a `pub_date` must
    // not be free to swap places between publishes, or the feed diffs as
    // changed and rebuilds a history that did not move.
    prior.sort_by(|a, b| {
        b.pub_date
            .cmp(&a.pub_date)
            .then_with(|| b.version.cmp(&a.version))
    });

    serde_json::to_string_pretty(&ReleaseIndex {
        name: binary.to_string(),
        schema: 1,
        updated_at: Utc::now().to_rfc3339(),
        versions: prior,
    })
}

/// Bucket key for a binary's accumulating index.
pub fn index_key(prefix: Option<&str>, binary: &str) -> String {
    join_key(prefix, &[binary, "index.json"])
}

/// One binary's contribution to the index, handed across the publisher seam.
///
/// Carries the inputs [`merge_index`] needs plus the key to read-modify-write,
/// so the publisher impl supplies only the I/O — the merge semantics (and the
/// two invariants they hold) stay here, in the tested crate, rather than being
/// re-derived by every backend.
#[derive(Debug, Clone)]
pub struct IndexUpdate {
    /// Bucket key of the index object, prefix already applied.
    pub key: String,
    pub binary: String,
    pub version: String,
    /// Publish timestamp for a version appearing here for the FIRST time. A
    /// version already in the index keeps the date it was first published with
    /// — see [`merge_index`].
    pub pub_date: String,
    pub manifest_url: Option<String>,
    pub triples: BTreeMap<String, IndexTriple>,
}

impl IndexUpdate {
    /// Fold this release into the index's current bytes (`None` = first
    /// publish), returning the bytes to write back.
    pub fn merge(&self, existing: Option<&str>) -> Result<String, serde_json::Error> {
        merge_index(
            existing,
            &self.binary,
            &self.version,
            &self.pub_date,
            self.manifest_url.clone(),
            self.triples.clone(),
        )
    }
}

/// Map a target triple onto the platform token `/releases` keys its labels by.
///
/// The consumer chain is: this token → almanac's `ReleaseAsset::platform` →
/// `PLATFORM_LABELS` in `app/yah/web/marketing/src/releases.ts`. A token with no
/// entry there falls through and the page renders the raw string, so "publish
/// the triple and let something downstream figure it out" shows a visitor
/// `aarch64-apple-darwin` where the label should read "macOS (Apple Silicon)".
///
/// Two spellings arrive here and both must map: the full Rust triple a
/// cross-build declares (`aarch64-apple-darwin`) and the `<os>-<arch>`
/// shorthand [`resolve_triple`] synthesises for a host-native build
/// (`darwin-aarch64`) — the channel manifest is keyed by whichever the
/// producing step used.
///
/// **musl is checked before gnu, deliberately.** almanac's own fallback mapper
/// matches on the `x86_64-unknown-linux` prefix and so collapses the two onto
/// one token, listing a musl and a gnu binary as the same download. They are
/// not interchangeable. Supplying the token from here — rather than leaving
/// `platform` unset and letting that fallback run — is what keeps them apart.
pub fn platform_token(triple: &str) -> String {
    let t = triple.to_ascii_lowercase();
    let musl = t.contains("musl");
    let arm = t.contains("aarch64") || t.contains("arm64");
    let x86 = t.contains("x86_64") || t.contains("amd64");

    if t.contains("apple") || t.contains("darwin") || t.contains("macos") {
        if arm {
            return "macos-arm64".to_string();
        }
        if x86 {
            return "macos-x86_64".to_string();
        }
    } else if t.contains("windows") {
        if x86 {
            return "windows-x86_64".to_string();
        }
    } else if t.contains("linux") {
        return match (arm, x86, musl) {
            (true, _, true) => "linux-aarch64-musl".to_string(),
            (true, _, false) => "linux-arm64".to_string(),
            (_, true, true) => "linux-x86_64-musl".to_string(),
            (_, true, false) => "linux-x86_64".to_string(),
            _ => triple.to_string(),
        };
    }
    // Unrecognised: pass the triple through rather than guess. The page shows
    // it verbatim, which is a visible prompt to add a mapping — better than a
    // wrong label that reads as correct.
    triple.to_string()
}

/// Project a staged [`ChannelManifest`] onto the index's per-triple shape.
pub fn index_triples_from_manifest(manifest: &ChannelManifest) -> BTreeMap<String, IndexTriple> {
    manifest
        .host
        .bundle
        .iter()
        .map(|(triple, bundle)| {
            (
                triple.clone(),
                IndexTriple {
                    url: bundle.url.clone(),
                    platform: Some(platform_token(triple)),
                    filename: bundle
                        .url
                        .rsplit('/')
                        .next()
                        .filter(|f| !f.is_empty())
                        .map(str::to_string),
                    size_bytes: bundle.size,
                    hash: bundle.hash.clone(),
                },
            )
        })
        .collect()
}

/// Streaming BLAKE3 of a staged artifact, returned already tagged.
///
/// Streamed rather than slurped because these are release binaries and
/// tarballs — reading a few hundred MB into memory to hash it is a needless
/// way to fail on a small runner.
fn blake3_file(path: &Path) -> std::io::Result<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
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
        let filename = src.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
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
        // Name the file. This is the ONE place staging opens a caller-supplied
        // path, and a bare `No such file or directory (os error 2)` at the end
        // of a multi-minute release build is close to undebuggable — it does
        // not say which artifact, which step declared it, or which tree it was
        // resolved against. Every `produces` bug lands here.
        let bytes = std::fs::copy(src, &dest).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "staging artifact {} (binary={}, triple={}): {e} — the step that \
                     declares this `produces` must leave the file at exactly this path, \
                     resolved against the run's workspace",
                    src.display(),
                    artifact.binary,
                    triple
                ),
            )
        })?;
        // Hash the STAGED copy, not the source: what a consumer downloads is
        // what was uploaded, so the digest has to describe the bytes that
        // actually landed in the channel.
        let hash = blake3_file(&dest)?;

        let url = match base_url.map(str::trim).filter(|b| !b.is_empty()) {
            Some(base) => format!("{}/{}", base.trim_end_matches('/'), key),
            None => key.clone(),
        };
        bundles.entry(artifact.binary.clone()).or_default().insert(
            triple,
            ChannelBundle {
                url,
                size: Some(bytes),
                hash: Some(hash),
            },
        );
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
                host: ChannelHost {
                    bundle: per_triple_bundle,
                },
            };
            let manifest_key = join_key(prefix, &[&binary, &per_triple_manifest_filename(triple)]);
            let dest = staging_dir.join(&manifest_key);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let json = serde_json::to_vec_pretty(&manifest)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
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
        let json = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Two keys, same bytes. The immutable per-version copy is what the
        // version index links to: pointing a HISTORY entry at the mutable
        // pointer means clicking "0.8.21's manifest" hands you whatever is
        // current, which is wrong the moment a second version exists.
        for filename in [
            VERSIONED_MANIFEST_FILENAME,
            // Written LAST so a failed per-version write never leaves the
            // pointer aimed at a version whose manifest is not published.
            MANIFEST_FILENAME,
        ] {
            let manifest_key = if filename == VERSIONED_MANIFEST_FILENAME {
                join_key(prefix, &[&binary, &version, filename])
            } else {
                join_key(prefix, &[&binary, filename])
            };
            let dest = staging_dir.join(&manifest_key);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dest, &json)?;
            report.manifest_keys.push(manifest_key);
        }
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

    /// Read-modify-write the accumulating version index at `update.key`
    /// (R330-T32) — the object the /releases history renders from, as opposed
    /// to the single-version pointer `sync` uploads.
    ///
    /// The impl performs three steps and owns only the middle one's I/O: read
    /// the current bytes (absent = first release, not an error), call
    /// [`IndexUpdate::merge`], write the result back.
    ///
    /// **The write MUST be a compare-and-swap** — conditional on the ETag the
    /// bytes were read at, retrying the whole read-merge-write on a failed
    /// precondition. This object is a permanent record that every publisher
    /// appends to, so an unconditional PUT loses whichever concurrent release
    /// wrote first, silently and irreversibly. A backend that cannot do a
    /// conditional write should say so rather than emulate one.
    async fn publish_index(
        &self,
        provider: &str,
        bucket: &str,
        update: &IndexUpdate,
    ) -> Result<(), RunnerError>;

    /// Fire the almanac revalidate hook so the feed re-renders from this
    /// release. A no-configured-receiver impl returns `Ok(())`.
    ///
    /// `report` is the release that was just staged and uploaded, manifests
    /// included. It is passed rather than withheld because the poke is
    /// expected to **carry its payload** (R330-F33): the run that just cut the
    /// release holds the fact, so it hands the manifest over instead of
    /// publishing it and waiting for the consumer's poller to notice. An impl
    /// with no manifest to offer can ignore it and poke payload-less — that
    /// stays a supported mode, it just leaves one stale render before the
    /// fetch tier converges.
    ///
    /// An `Err` here is REPORTED, NOT FATAL: implementors should return the
    /// real failure (so it is visible in logs and to direct callers), but
    /// [`PublishingOutcomeDispatcher::publish`] deliberately downgrades it to
    /// a warning. The poke only collapses the staleness window — the
    /// consumer's own feed-fetch tier is what makes the feed correct — so an
    /// unreachable receiver must not fail a release whose artifacts uploaded
    /// fine. Do NOT swallow the error in the impl to get that behaviour.
    async fn revalidate(&self, report: &StageReport) -> Result<(), RunnerError>;
}

/// The real outcome dispatcher (R330-F3): stages produced artifacts into the
/// release channel layout, uploads them via a [`ReleasePublisher`], then fires
/// the revalidate hook (best-effort — see [`ReleasePublisher::revalidate`]).
/// `yubaba_deploy` / `almanac_run` stay logging stubs
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
    async fn yubaba_deploy(&self, service: &str, env: &str) -> Result<(), RunnerError> {
        tracing::info!(
            service,
            env,
            "qed outcome: yubaba-deploy skipped (yubaba deploy RPC not yet stable, R040-F4)"
        );
        Ok(())
    }

    async fn almanac_run(&self, pipeline: &str) -> Result<(), RunnerError> {
        tracing::info!(
            pipeline,
            "qed outcome: almanac-run skipped (cadence scheduler pending)"
        );
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
            .sync(
                staging.path(),
                &req.provider,
                &req.bucket,
                req.prefix.as_deref(),
            )
            .await?;

        // The index is part of the release record, not a nicety layered on
        // top: `sync` uploads a pointer to THIS version, and the history page
        // reads the index. A release whose artifacts uploaded but whose index
        // write failed is invisible on /releases, so this error propagates
        // (unlike the revalidate poke below, which only affects latency).
        for (binary, manifest) in &report.manifests {
            let update = IndexUpdate {
                key: index_key(req.prefix.as_deref(), binary),
                binary: binary.clone(),
                version: manifest.version.clone(),
                pub_date: manifest.pub_date.clone(),
                // The IMMUTABLE per-version manifest, not the mutable pointer:
                // this is a history entry, so it must keep resolving to the
                // manifest of THIS version after the next release lands.
                //
                // Derived from the same (base_url, prefix, binary, version) the
                // staging pass used, NOT by string surgery on an asset URL —
                // the two must agree, and reconstructing one from the other is
                // the kind of coupling that works until a layout changes.
                manifest_url: req
                    .base_url
                    .as_deref()
                    .map(str::trim)
                    .filter(|b| !b.is_empty())
                    .map(|base| {
                        format!(
                            "{}/{}",
                            base.trim_end_matches('/'),
                            join_key(
                                req.prefix.as_deref(),
                                &[binary, &manifest.version, VERSIONED_MANIFEST_FILENAME]
                            )
                        )
                    }),
                triples: index_triples_from_manifest(manifest),
            };
            self.publisher
                .publish_index(&req.provider, &req.bucket, &update)
                .await?;
            tracing::info!(
                bucket = %req.bucket,
                key = %update.key,
                version = %update.version,
                triples = update.triples.len(),
                "qed outcome: merged release into version index"
            );
        }

        // The poke is a LATENCY OPTIMISATION, not the correctness path
        // (R330-T14, re-scoped by R330-F31): the node's feed-fetch tier
        // re-renders from upstream on its own timer, so a release that never
        // gets poked is still correct within `feed_interval_secs`. Aborting
        // the whole publish on an unreachable receiver would therefore throw
        // away a successful artifact upload to save nothing — warn and let
        // the release stand.
        if let Err(e) = self.publisher.revalidate(&report).await {
            tracing::warn!(
                bucket = %req.bucket,
                version = %req.version,
                error = %e,
                "qed outcome: revalidate poke failed — release stands; the feed-fetch \
                 tier will pick the new version up on its next tick"
            );
        }
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

    async fn publish_index(
        &self,
        provider: &str,
        bucket: &str,
        update: &IndexUpdate,
    ) -> Result<(), RunnerError> {
        tracing::info!(
            provider,
            bucket,
            key = %update.key,
            version = %update.version,
            "qed publish: index merge skipped (no real publisher wired)"
        );
        Ok(())
    }

    async fn revalidate(&self, report: &StageReport) -> Result<(), RunnerError> {
        tracing::info!(
            manifests = report.manifests.len(),
            "qed publish: revalidate hook skipped (no receiver configured)"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::TempDir;

    /// First file named `name` anywhere under `root`.
    fn find_file(root: &Path, name: &str) -> Option<std::path::PathBuf> {
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).ok()? {
                let path = entry.ok()?.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.file_name().is_some_and(|f| f == name) {
                    return Some(path);
                }
            }
        }
        None
    }

    fn write_dummy(dir: &Path, rel: &str, contents: &[u8]) -> String {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, contents).unwrap();
        p.to_string_lossy().into_owned()
    }

    fn triple_entry(url: &str) -> BTreeMap<String, IndexTriple> {
        one_triple("darwin-aarch64", url)
    }

    /// One platform leg's contribution — what a matrix job actually stages.
    fn one_triple(triple: &str, url: &str) -> BTreeMap<String, IndexTriple> {
        let mut m = BTreeMap::new();
        m.insert(
            triple.to_string(),
            IndexTriple {
                url: url.to_string(),
                platform: Some(triple.to_string()),
                filename: Some("yah.tar.gz".into()),
                size_bytes: Some(42),
                hash: Some("blake3:aa".into()),
            },
        );
        m
    }

    fn triples_of(json: &str, version: &str) -> Vec<String> {
        let idx: ReleaseIndex = serde_json::from_str(json).unwrap();
        idx.versions
            .into_iter()
            .find(|v| v.version == version)
            .unwrap_or_else(|| panic!("version {version} missing from index"))
            .triples
            .into_keys()
            .collect()
    }

    fn versions_of(json: &str) -> Vec<(String, String)> {
        serde_json::from_str::<ReleaseIndex>(json)
            .unwrap()
            .versions
            .into_iter()
            .map(|v| (v.version, v.pub_date))
            .collect()
    }

    #[test]
    fn a_missing_index_is_the_create_case_not_an_error() {
        let out = merge_index(
            None,
            "yah",
            "v0.8.21",
            "2026-07-31T00:00:00Z",
            None,
            triple_entry("https://cdn.yah.dev/yah/0.8.21/darwin-aarch64/yah.tar.gz"),
        )
        .unwrap();
        // The leading `v` is stripped: the page and the install pointer both
        // key on the bare version.
        assert_eq!(
            versions_of(&out),
            vec![("0.8.21".to_string(), "2026-07-31T00:00:00Z".to_string())]
        );
    }

    #[test]
    fn republishing_a_version_replaces_its_entry_rather_than_duplicating_it() {
        let first = merge_index(
            None,
            "yah",
            "0.8.21",
            "2026-07-31T00:00:00Z",
            None,
            triple_entry("https://cdn.yah.dev/a"),
        )
        .unwrap();
        let second = merge_index(
            Some(&first),
            "yah",
            "0.8.21",
            "2026-08-02T00:00:00Z",
            None,
            triple_entry("https://cdn.yah.dev/b"),
        )
        .unwrap();

        let v = versions_of(&second);
        assert_eq!(v.len(), 1, "a re-publish must not duplicate the version");
        // The RE-PUBLISHED payload wins...
        let idx: ReleaseIndex = serde_json::from_str(&second).unwrap();
        assert_eq!(idx.versions[0].triples["darwin-aarch64"].url, "https://cdn.yah.dev/b");
        // ...but the ORIGINAL publication date survives. Restamping it would
        // reorder published history on every consumer, which all sort by date.
        assert_eq!(
            v[0].1, "2026-07-31T00:00:00Z",
            "a re-publish must not restamp pub_date"
        );
    }

    #[test]
    fn the_index_accumulates_and_stays_newest_first() {
        let a = merge_index(None, "yah", "0.8.21", "2026-07-01T00:00:00Z", None, triple_entry("u")).unwrap();
        let b = merge_index(Some(&a), "yah", "0.8.22", "2026-07-20T00:00:00Z", None, triple_entry("u")).unwrap();
        // Appended out of order — the sort, not the caller, decides position.
        let c = merge_index(Some(&b), "yah", "0.8.20", "2026-06-01T00:00:00Z", None, triple_entry("u")).unwrap();

        let got: Vec<String> = versions_of(&c).into_iter().map(|(v, _)| v).collect();
        assert_eq!(got, vec!["0.8.22", "0.8.21", "0.8.20"]);
    }

    /// The regression pin for R330-T35's sliced-release case: a release matrix
    /// publishes one platform per leg, and each leg's update carries only its
    /// own triples. Before this, the last leg to finish took the version's whole
    /// triples map and every earlier platform vanished.
    #[test]
    fn a_release_published_one_platform_at_a_time_accumulates_every_triple() {
        let mac = merge_index(
            None,
            "yah",
            "0.8.22",
            "2026-08-12T00:00:00Z",
            None,
            one_triple("aarch64-apple-darwin", "https://cdn.yah.dev/mac"),
        )
        .unwrap();
        let linux = merge_index(
            Some(&mac),
            "yah",
            "0.8.22",
            "2026-08-12T00:00:01Z",
            None,
            one_triple("x86_64-unknown-linux-musl", "https://cdn.yah.dev/linux"),
        )
        .unwrap();
        let win = merge_index(
            Some(&linux),
            "yah",
            "0.8.22",
            "2026-08-12T00:00:02Z",
            None,
            one_triple("x86_64-pc-windows-msvc", "https://cdn.yah.dev/win"),
        )
        .unwrap();

        assert_eq!(
            triples_of(&win, "0.8.22"),
            vec![
                "aarch64-apple-darwin".to_string(),
                "x86_64-pc-windows-msvc".to_string(),
                "x86_64-unknown-linux-musl".to_string(),
            ],
            "a later platform leg must not drop the triples earlier legs published"
        );
        assert_eq!(
            versions_of(&win).len(),
            1,
            "the slices are one version, not three"
        );
    }

    /// Slicing must not weaken invariant 1: a leg re-publishing a triple it
    /// already wrote still updates that entry in place.
    #[test]
    fn re_publishing_one_slice_updates_that_triple_and_leaves_its_siblings() {
        let mac = merge_index(
            None,
            "yah",
            "0.8.22",
            "2026-08-12T00:00:00Z",
            None,
            one_triple("aarch64-apple-darwin", "https://cdn.yah.dev/mac-v1"),
        )
        .unwrap();
        let linux = merge_index(
            Some(&mac),
            "yah",
            "0.8.22",
            "2026-08-12T00:00:01Z",
            None,
            one_triple("x86_64-unknown-linux-musl", "https://cdn.yah.dev/linux"),
        )
        .unwrap();
        // The mac leg failed its upload and retried.
        let retried = merge_index(
            Some(&linux),
            "yah",
            "0.8.22",
            "2026-08-12T00:00:02Z",
            None,
            one_triple("aarch64-apple-darwin", "https://cdn.yah.dev/mac-v2"),
        )
        .unwrap();

        let idx: ReleaseIndex = serde_json::from_str(&retried).unwrap();
        let v = &idx.versions[0];
        assert_eq!(v.triples["aarch64-apple-darwin"].url, "https://cdn.yah.dev/mac-v2");
        assert_eq!(v.triples["x86_64-unknown-linux-musl"].url, "https://cdn.yah.dev/linux");
        assert_eq!(
            v.pub_date, "2026-08-12T00:00:00Z",
            "a retried slice must not restamp the version"
        );
    }

    /// Two versions releasing in the same window, their legs interleaved — the
    /// shape `cas_merge_index`'s retry loop produces when concurrent publishers
    /// serialise onto one object. Each merge sees the previous winner's bytes.
    #[test]
    fn interleaved_legs_of_two_versions_all_survive() {
        let a1 = merge_index(None, "yah", "0.8.21", "2026-08-01T00:00:00Z", None,
            one_triple("aarch64-apple-darwin", "https://cdn.yah.dev/21-mac")).unwrap();
        let b1 = merge_index(Some(&a1), "yah", "0.8.22", "2026-08-12T00:00:00Z", None,
            one_triple("aarch64-apple-darwin", "https://cdn.yah.dev/22-mac")).unwrap();
        let a2 = merge_index(Some(&b1), "yah", "0.8.21", "2026-08-01T00:00:05Z", None,
            one_triple("x86_64-unknown-linux-musl", "https://cdn.yah.dev/21-linux")).unwrap();
        let b2 = merge_index(Some(&a2), "yah", "0.8.22", "2026-08-12T00:00:05Z", None,
            one_triple("x86_64-unknown-linux-musl", "https://cdn.yah.dev/22-linux")).unwrap();

        let got: Vec<String> = versions_of(&b2).into_iter().map(|(v, _)| v).collect();
        assert_eq!(got, vec!["0.8.22", "0.8.21"]);
        assert_eq!(triples_of(&b2, "0.8.21").len(), 2);
        assert_eq!(triples_of(&b2, "0.8.22").len(), 2);
    }

    /// A leg with no `base_url` derives no `manifest_url`; it must not erase the
    /// one a sibling leg derived. Same replace-or-append rule, one field over.
    #[test]
    fn a_slice_without_a_manifest_url_keeps_the_one_already_recorded() {
        let with_url = merge_index(
            None,
            "yah",
            "0.8.22",
            "2026-08-12T00:00:00Z",
            Some("https://cdn.yah.dev/yah/0.8.22/manifest.json".into()),
            one_triple("aarch64-apple-darwin", "https://cdn.yah.dev/mac"),
        )
        .unwrap();
        let without = merge_index(
            Some(&with_url),
            "yah",
            "0.8.22",
            "2026-08-12T00:00:01Z",
            None,
            one_triple("x86_64-unknown-linux-musl", "https://cdn.yah.dev/linux"),
        )
        .unwrap();

        let idx: ReleaseIndex = serde_json::from_str(&without).unwrap();
        assert_eq!(
            idx.versions[0].manifest_url.as_deref(),
            Some("https://cdn.yah.dev/yah/0.8.22/manifest.json")
        );
    }

    #[test]
    fn every_download_in_the_index_carries_a_tagged_hash() {
        let tmp = TempDir::new().unwrap();
        let src = write_dummy(tmp.path(), "src/yah", b"binary bytes");
        let staging = tmp.path().join("stage");
        let report = stage_release(
            &staging,
            &[ProducedArtifact {
                binary: "yah".into(),
                path: src,
                triple: Some("darwin-aarch64".into()),
            }],
            "0.8.21",
            None,
            Some("https://cdn.yah.dev"),
        )
        .unwrap();

        let triples = index_triples_from_manifest(&report.manifests["yah"]);
        let entry = &triples["darwin-aarch64"];
        let hash = entry.hash.as_deref().expect("staged artifact has no hash");
        assert!(
            hash.starts_with("blake3:") && hash.len() == "blake3:".len() + 64,
            "hash must be a tagged blake3 digest, got {hash:?}"
        );
        assert_eq!(entry.filename.as_deref(), Some("yah"));
        assert_eq!(entry.size_bytes, Some(b"binary bytes".len() as u64));
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
    fn platform_token_maps_both_triple_spellings() {
        // Full Rust triples (what a cross-build declares).
        assert_eq!(platform_token("aarch64-apple-darwin"), "macos-arm64");
        assert_eq!(platform_token("x86_64-apple-darwin"), "macos-x86_64");
        assert_eq!(platform_token("x86_64-unknown-linux-gnu"), "linux-x86_64");
        assert_eq!(platform_token("aarch64-unknown-linux-gnu"), "linux-arm64");
        assert_eq!(platform_token("x86_64-pc-windows-msvc"), "windows-x86_64");
        // The `<os>-<arch>` shorthand `resolve_triple` synthesises for a
        // host-native build — the CLI-only release recipe's spelling.
        assert_eq!(platform_token("darwin-aarch64"), "macos-arm64");
        assert_eq!(platform_token("darwin-x86_64"), "macos-x86_64");
        assert_eq!(platform_token("linux-x86_64"), "linux-x86_64");
        assert_eq!(platform_token("linux-aarch64"), "linux-arm64");
        // Whatever the host actually is, it must map to something the page has
        // a label for — this is the token a local `cli-release` publishes.
        assert!(
            [
                "macos-arm64",
                "macos-x86_64",
                "linux-x86_64",
                "linux-arm64",
                "windows-x86_64",
            ]
            .contains(&platform_token(&resolve_triple(None)).as_str()),
            "host {} has no /releases label",
            resolve_triple(None)
        );
    }

    #[test]
    fn platform_token_keeps_musl_and_gnu_apart() {
        // The whole reason the producer supplies `platform` rather than letting
        // almanac's fallback derive it: that mapper matches on the
        // `x86_64-unknown-linux` prefix and collapses these two, listing a musl
        // and a gnu binary as the same download.
        assert_eq!(
            platform_token("x86_64-unknown-linux-musl"),
            "linux-x86_64-musl"
        );
        assert_eq!(
            platform_token("aarch64-unknown-linux-musl"),
            "linux-aarch64-musl"
        );
        assert_ne!(
            platform_token("x86_64-unknown-linux-musl"),
            platform_token("x86_64-unknown-linux-gnu")
        );
    }

    #[test]
    fn platform_token_passes_unknown_triples_through() {
        // Better a visibly-raw token on the page (a prompt to add a mapping)
        // than a confident wrong label.
        assert_eq!(
            platform_token("riscv64gc-unknown-none"),
            "riscv64gc-unknown-none"
        );
    }

    #[test]
    fn index_triples_carry_a_page_label_not_a_raw_triple() {
        let manifest = ChannelManifest {
            version: "0.8.21".into(),
            pub_date: "2026-08-01T00:00:00Z".into(),
            notes: None,
            host: ChannelHost {
                bundle: BTreeMap::from([(
                    "aarch64-apple-darwin".to_string(),
                    ChannelBundle {
                        url: "https://cdn.yah.dev/yah/0.8.21/aarch64-apple-darwin/yah.tar.gz"
                            .into(),
                        size: Some(4),
                        hash: Some("blake3:ff".into()),
                    },
                )]),
            },
        };
        let triples = index_triples_from_manifest(&manifest);
        // Keyed by triple, but `platform` is the token PLATFORM_LABELS keys on.
        // Emitting the triple here rendered "aarch64-apple-darwin" as the
        // download's visible label on /releases.
        assert_eq!(
            triples["aarch64-apple-darwin"].platform.as_deref(),
            Some("macos-arm64")
        );
        assert_eq!(
            triples["aarch64-apple-darwin"].filename.as_deref(),
            Some("yah.tar.gz")
        );
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

        // Three manifest keys (report sorts them): the IMMUTABLE per-version
        // copy the index links to, the per-triple stable key (R330-B8), and the
        // mutable pointer almanac re-fetches on push.
        assert_eq!(
            report.manifest_keys,
            vec![
                "yah/0.8.6/manifest.json",
                "yah/release-manifest-darwin-aarch64.json",
                "yah/release-manifest.json",
            ]
        );
        let manifest = &report.manifests["yah"];
        assert_eq!(manifest.version, "0.8.6");
        let bundle = &manifest.host.bundle["darwin-aarch64"];
        assert_eq!(
            bundle.url,
            "https://releases.yah.dev/yah/0.8.6/darwin-aarch64/yah"
        );
        assert_eq!(bundle.size, Some("YAH-BINARY".len() as u64));

        // The on-disk manifest round-trips through the same wire type, and the
        // per-version copy is byte-identical to the pointer at publish time —
        // it just stops changing afterwards.
        let bytes = std::fs::read(staging.path().join("yah/release-manifest.json")).unwrap();
        let parsed: ChannelManifest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(&parsed, manifest);
        let versioned = std::fs::read(staging.path().join("yah/0.8.6/manifest.json")).unwrap();
        assert_eq!(versioned, bytes);
    }

    #[test]
    fn stage_release_relative_urls_without_base() {
        let src = TempDir::new().unwrap();
        let bin = write_dummy(src.path(), "out/desktop", b"x");
        let staging = TempDir::new().unwrap();
        let report = stage_release(
            staging.path(),
            &[ProducedArtifact {
                binary: "desktop".into(),
                path: bin,
                triple: Some("linux-x86_64".into()),
            }],
            "0.9.0",
            Some("channels"),
            None,
        )
        .unwrap();
        // Prefix is applied to both the object and the manifest keys.
        assert_eq!(
            report.object_keys,
            vec!["channels/desktop/0.9.0/linux-x86_64/desktop"]
        );
        assert_eq!(
            report.manifest_keys,
            vec![
                "channels/desktop/0.9.0/manifest.json",
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
                ProducedArtifact {
                    binary: "yah".into(),
                    path: yah,
                    triple: Some("darwin-aarch64".into()),
                },
                ProducedArtifact {
                    binary: "desktop".into(),
                    path: desktop,
                    triple: Some("darwin-aarch64".into()),
                },
            ],
            "1.0.0",
            None,
            None,
        )
        .unwrap();
        // One shared manifest per binary, plus one per-triple stable manifest
        // per (binary, triple).
        assert_eq!(report.manifests.len(), 2);
        assert!(report
            .manifest_keys
            .contains(&"yah/release-manifest.json".to_string()));
        assert!(report
            .manifest_keys
            .contains(&"desktop/release-manifest.json".to_string()));
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
        let darwin_path = staging
            .path()
            .join("yah/release-manifest-darwin-aarch64.json");
        let linux_path = staging
            .path()
            .join("yah/release-manifest-linux-x86_64.json");
        assert!(
            darwin_path.exists(),
            "darwin per-triple manifest must persist"
        );
        assert!(
            linux_path.exists(),
            "linux per-triple manifest must persist"
        );

        let darwin: ChannelManifest =
            serde_json::from_slice(&std::fs::read(&darwin_path).unwrap()).unwrap();
        let linux: ChannelManifest =
            serde_json::from_slice(&std::fs::read(&linux_path).unwrap()).unwrap();
        assert!(darwin.host.bundle.contains_key("darwin-aarch64"));
        assert_eq!(
            darwin.host.bundle.len(),
            1,
            "per-triple manifest is single-triple"
        );
        assert!(linux.host.bundle.contains_key("linux-x86_64"));
        assert_eq!(
            linux.host.bundle.len(),
            1,
            "per-triple manifest is single-triple"
        );

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
        /// Binary names the revalidate hook was handed manifests for — the
        /// payload a real hook maps into the poke's `data_inputs`.
        revalidate_saw: Mutex<Vec<String>>,
        /// Make `revalidate` report a failure (still counting the attempt) —
        /// stands in for an unreachable / 5xx receiver.
        fail_revalidate: bool,
        /// Stand-in bucket for index objects: key → current bytes. Persisting
        /// them across calls is the point — accumulation is what the index is
        /// for, and a fake that forgets can't catch a clobbering merge.
        index_objects: Mutex<BTreeMap<String, String>>,
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
            // tempdir must outlive this call). Located by walk rather than a
            // fixed path so a prefixed request (`dl/yah/...`) works too.
            let manifest = find_file(staging_dir, MANIFEST_FILENAME)
                .expect("staged tree carries a shared manifest at sync time");
            let body = std::fs::read_to_string(&manifest).unwrap();
            self.captured_manifests.lock().unwrap().push(body);
            self.synced.lock().unwrap().push(bucket.to_string());
            Ok(())
        }

        async fn publish_index(
            &self,
            _provider: &str,
            _bucket: &str,
            update: &IndexUpdate,
        ) -> Result<(), RunnerError> {
            let mut objects = self.index_objects.lock().unwrap();
            let merged = update
                .merge(objects.get(&update.key).map(String::as_str))
                .map_err(|e| RunnerError::Remote(format!("merge index: {e}")))?;
            objects.insert(update.key.clone(), merged);
            Ok(())
        }

        async fn revalidate(&self, report: &StageReport) -> Result<(), RunnerError> {
            *self.revalidated.lock().unwrap() += 1;
            self.revalidate_saw
                .lock()
                .unwrap()
                .extend(report.manifests.keys().cloned());
            if self.fail_revalidate {
                return Err(RunnerError::Remote(
                    "POST https://yah.dev/revalidate: connection refused".into(),
                ));
            }
            Ok(())
        }
    }

    /// The dispatcher takes its publisher by value; this forwarder lets a test
    /// keep a handle for assertions.
    struct ArcPublisher(std::sync::Arc<RecordingPublisher>);

    #[async_trait]
    impl ReleasePublisher for ArcPublisher {
        async fn sync(
            &self,
            d: &Path,
            p: &str,
            b: &str,
            pre: Option<&str>,
        ) -> Result<(), RunnerError> {
            self.0.sync(d, p, b, pre).await
        }
        async fn publish_index(
            &self,
            p: &str,
            b: &str,
            u: &IndexUpdate,
        ) -> Result<(), RunnerError> {
            self.0.publish_index(p, b, u).await
        }
        async fn revalidate(&self, r: &StageReport) -> Result<(), RunnerError> {
            self.0.revalidate(r).await
        }
    }

    #[tokio::test]
    async fn dispatcher_stages_uploads_and_revalidates() {
        use std::sync::Arc;
        let src = TempDir::new().unwrap();
        let bin = write_dummy(src.path(), "target/release/yah", b"BIN");
        let publisher = Arc::new(RecordingPublisher::default());
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

        assert_eq!(
            publisher.synced.lock().unwrap().as_slice(),
            ["yah-releases"]
        );
        assert_eq!(*publisher.revalidated.lock().unwrap(), 1);
        let manifest = &publisher.captured_manifests.lock().unwrap()[0];
        assert!(
            manifest.contains("0.8.6"),
            "manifest carries version: {manifest}"
        );
        assert!(
            manifest.contains("darwin-aarch64"),
            "manifest carries triple"
        );
        // R330-T14: the hook is handed the release it is poking about, so it
        // can carry the manifest as the poke's `data_inputs` instead of
        // leaving the receiver to render whatever its own node last fetched.
        assert_eq!(
            publisher.revalidate_saw.lock().unwrap().as_slice(),
            ["yah"],
            "revalidate must see the staged manifests, not just the fact of a publish"
        );
    }

    /// R330-T32: two releases through the dispatcher leave BOTH versions in
    /// the index. This is the whole point of the object — `release-manifest`
    /// is overwritten by each publish, so if the index behaved the same way
    /// the /releases page would render a one-row history forever.
    #[tokio::test]
    async fn dispatcher_accumulates_versions_in_the_index() {
        use std::sync::Arc;
        let src = TempDir::new().unwrap();
        let publisher = Arc::new(RecordingPublisher::default());
        let dispatcher = PublishingOutcomeDispatcher::new(ArcPublisher(publisher.clone()));

        for version in ["0.8.6", "0.8.7"] {
            let bin = write_dummy(src.path(), &format!("{version}/yah"), b"BIN");
            dispatcher
                .publish(&PublishRequest {
                    provider: "r2".into(),
                    bucket: "yah-releases".into(),
                    prefix: Some("dl".into()),
                    base_url: Some("https://releases.yah.dev".into()),
                    version: version.into(),
                    artifacts: vec![ProducedArtifact {
                        binary: "yah".into(),
                        path: bin,
                        triple: Some("darwin-aarch64".into()),
                    }],
                })
                .await
                .unwrap();
        }

        let objects = publisher.index_objects.lock().unwrap();
        let raw = objects
            .get("dl/yah/index.json")
            .expect("index published under the request's prefix");
        let index: ReleaseIndex = serde_json::from_str(raw).unwrap();
        assert_eq!(
            index.versions.iter().map(|v| &v.version).collect::<Vec<_>>(),
            ["0.8.7", "0.8.6"],
            "both releases present, newest first"
        );

        let entry = &index.versions[0];
        assert_eq!(
            entry.manifest_url.as_deref(),
            Some("https://releases.yah.dev/dl/yah/0.8.7/manifest.json"),
            "derived from (base_url, prefix, binary, version), prefix included"
        );
        // A HISTORY entry must not link the mutable pointer: 0.8.6's manifest
        // has to keep resolving to 0.8.6 after 0.8.7 lands.
        assert_eq!(
            index.versions[1].manifest_url.as_deref(),
            Some("https://releases.yah.dev/dl/yah/0.8.6/manifest.json"),
        );
        let triple = entry.triples.get("darwin-aarch64").expect("triple entry");
        assert_eq!(
            triple.url,
            "https://releases.yah.dev/dl/yah/0.8.7/darwin-aarch64/yah"
        );
        assert!(
            triple.hash.as_deref().is_some_and(|h| h.starts_with("blake3:")),
            "downloads carry a tagged hash: {:?}",
            triple.hash
        );
    }

    /// R330-T14: an unreachable revalidate receiver must NOT fail a release
    /// whose artifacts uploaded fine. The poke only collapses the staleness
    /// window; the consumer's feed-fetch tier is the correctness path, so
    /// aborting here would discard a good upload to save nothing.
    #[tokio::test]
    async fn dispatcher_publish_survives_a_failing_revalidate() {
        use std::sync::Arc;
        let src = TempDir::new().unwrap();
        let bin = write_dummy(src.path(), "target/release/yah", b"BIN");
        let publisher = Arc::new(RecordingPublisher {
            fail_revalidate: true,
            ..Default::default()
        });
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

        dispatcher
            .publish(&req)
            .await
            .expect("a failed revalidate poke must not abort the publish");

        // The upload still happened and the poke was still attempted — this is
        // "warn and stand", not "skip the hook".
        assert_eq!(
            publisher.synced.lock().unwrap().as_slice(),
            ["yah-releases"]
        );
        assert_eq!(*publisher.revalidated.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn dispatcher_skips_when_no_artifacts() {
        let publisher = RecordingPublisher::default();
        // Move a probe out before constructing the dispatcher: read counters
        // after via a shared Arc instead.
        use std::sync::Arc;
        let probe = Arc::new(publisher);
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
        assert!(
            probe.synced.lock().unwrap().is_empty(),
            "no artifacts → no sync"
        );
        assert_eq!(*probe.revalidated.lock().unwrap(), 0);
    }
}
