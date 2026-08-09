//! Cross-host build contexts for `StepKind::BuildImage` (R636-B1).
//!
//! # The bug this exists for
//!
//! A remote build-image step used to hand yubaba the *composer's* paths —
//! `--local context=/yah/build/context` backed by a bind mount of the qed
//! host's camp root. That is coherent only while the builder and the composer
//! share a filesystem. Offload the step to a real build-worker and runc fails
//! at mount setup with `open /Users/leif/ss/yah: no such file or directory`,
//! because of course the Linux worker has no such directory. Every offloaded
//! catalog image build died there.
//!
//! # The shape that works
//!
//! Pack the context (plus the *compiled* Dockerfile, which lives outside it in
//! `.yah/cache/buildkit/`) into one gzipped tar, put it somewhere the worker
//! can GET, and pass BuildKit `--opt context=<url>`. The dockerfile frontend
//! fetches and unpacks it worker-side and resolves `--opt filename=` inside it,
//! so the workload references nothing host-local except the OCI output dir,
//! which is worker-local already.
//!
//! Verified against `moby/buildkit:v0.12.5-rootless` — the pinned default in
//! [`velveteen_exec`]'s `default_buildkit_image` — with the exact argv the
//! synthesis emits: `#1 [internal] load remote build context` → `#2 copy
//! /context /` → the Dockerfile's `COPY` resolving out of the tar.
//!
//! # Why the transport is a trait
//!
//! qed has no cloud credentials and should not grow any: it is the scheduler,
//! not the publisher. [`BuildContextPublisher`] is the same seam
//! [`crate::publish::ReleasePublisher`] uses — declared here, implemented by
//! whoever owns the bytes-in-a-bucket relationship (the `yah` CLI, over R2).
//! A camp with a different reachable store implements this instead; a camp
//! with none gets [`NoBuildContextPublisher`] and a refusal that says so.

use std::path::Path;

use async_trait::async_trait;

use crate::runner::RunnerError;

/// Ceiling on the packed context, counted as bytes *read from disk* so the
/// walk aborts early instead of after compressing several GB.
///
/// This exists because the runner's historical default context was the whole
/// camp root, and a camp root contains `target/`. Rather than guess at
/// ignore rules (a half-implemented `.dockerignore` would silently change what
/// gets built), refuse loudly and point at the `context` key — narrowing it is
/// both the fix and the thing the operator wanted anyway.
pub const MAX_CONTEXT_BYTES: u64 = 512 * 1024 * 1024;

/// Ships a build-image step's context to somewhere the *build worker* can
/// fetch it, returning the URL BuildKit should load it from.
#[async_trait]
pub trait BuildContextPublisher: Send + Sync {
    /// Upload `tarball` under `key`, returning a URL that GETs those bytes.
    ///
    /// `key` is an opaque, single-use, collision-free stem (the forge id). The
    /// implementation owns the bucket, prefix and public host. Single-use
    /// matters: a key that has never been requested cannot be serving a
    /// negatively-cached 404 from a CDN edge.
    async fn publish(&self, key: &str, tarball: Vec<u8>) -> Result<String, RunnerError>;

    /// Best-effort delete once the build no longer needs the bytes.
    ///
    /// Returns `()`, not `Result`: the build has already happened by the time
    /// this runs, and failing a green build over an undeleted temp object
    /// would be strictly worse than leaking it. Implementations log.
    async fn discard(&self, key: &str);
}

/// The no-transport default. Every `publish` is a refusal naming what to wire.
///
/// A silent fallback to the bind-mount shape would be the wrong default: it
/// fails inside runc on the worker, minutes later, with a message about a path
/// that means nothing to the reader.
pub struct NoBuildContextPublisher;

#[async_trait]
impl BuildContextPublisher for NoBuildContextPublisher {
    async fn publish(&self, _key: &str, _tarball: Vec<u8>) -> Result<String, RunnerError> {
        Err(RunnerError::InvalidConfig(
            "this build-image step must run on a different host than qed, so its build \
             context has to be fetched by the worker rather than bind-mounted — but no \
             build-context publisher is wired into this runner. The `yah` CLI wires an \
             R2-backed one; a bare `yah-qed` embedding must supply its own via \
             `PipelineRunner::with_build_context_publisher`."
                .into(),
        ))
    }

    async fn discard(&self, _key: &str) {}
}

/// Pack `context_dir` into a gzipped tar, appending `extra` entries at the
/// tar root afterwards.
///
/// `extra` lands last on purpose: tar extraction is last-write-wins, so the
/// compiled Dockerfile overrides any same-named file already in the context
/// rather than being shadowed by it. That is the intent — the runner compiled
/// that Dockerfile from the catalog and it is what the step means to build.
///
/// Paths inside the tar are relative to `context_dir`, so the tar root *is*
/// the build context — the shape BuildKit's remote-context unpack expects.
pub fn pack_context(
    context_dir: &Path,
    extra: &[(String, Vec<u8>)],
) -> Result<Vec<u8>, RunnerError> {
    let mut budget = MAX_CONTEXT_BYTES;
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut tar = tar::Builder::new(gz);
    // Follow symlinks: a context that reaches outside itself through one would
    // otherwise arrive on the worker as a dangling link and fail the COPY with
    // a file-not-found that names a path only the camp host has.
    tar.follow_symlinks(true);

    append_dir(&mut tar, context_dir, Path::new(""), &mut budget)?;

    for (name, bytes) in extra {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(0);
        header.set_cksum();
        tar.append_data(&mut header, name, bytes.as_slice())
            .map_err(|e| pack_err(format!("adding {name} to the context tar: {e}")))?;
    }

    let gz = tar
        .into_inner()
        .map_err(|e| pack_err(format!("finishing the context tar: {e}")))?;
    gz.finish()
        .map_err(|e| pack_err(format!("compressing the context tar: {e}")))
}

/// Recursive walk. Written by hand rather than with `Builder::append_dir_all`
/// so the byte budget can abort mid-walk — `append_dir_all` on a camp root
/// would happily stream `target/` for minutes before anyone could object.
fn append_dir<W: std::io::Write>(
    tar: &mut tar::Builder<W>,
    dir: &Path,
    prefix: &Path,
    budget: &mut u64,
) -> Result<(), RunnerError> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| pack_err(format!("reading build context {}: {e}", dir.display())))?;

    // Deterministic order: the same context must pack to the same bytes across
    // hosts, or a future content-addressed cache key over this tar would miss
    // on directory-iteration order alone.
    let mut names: Vec<std::ffi::OsString> = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|e| pack_err(format!("walking {}: {e}", dir.display())))?;
        names.push(entry.file_name());
    }
    names.sort();

    for name in names {
        let path = dir.join(&name);
        let rel = prefix.join(&name);
        let meta = std::fs::metadata(&path)
            .map_err(|e| pack_err(format!("stat {}: {e}", path.display())))?;

        if meta.is_dir() {
            append_dir(tar, &path, &rel, budget)?;
            continue;
        }

        let len = meta.len();
        *budget = budget.checked_sub(len).ok_or_else(|| {
            RunnerError::StepFailed {
                step: "build-image".into(),
                msg: format!(
                    "build context {} exceeds the {} MiB cross-host limit (tripped at {}). \
                     A remote build ships its context over the network, so the whole \
                     directory has to travel — set `context = \"<dir>\"` on the step to the \
                     directory the Dockerfile actually COPYs from instead of the camp root.",
                    path.display(),
                    MAX_CONTEXT_BYTES / (1024 * 1024),
                    rel.display(),
                ),
            }
        })?;

        let mut file = std::fs::File::open(&path)
            .map_err(|e| pack_err(format!("opening {}: {e}", path.display())))?;
        tar.append_file(&rel, &mut file)
            .map_err(|e| pack_err(format!("adding {} to the context tar: {e}", rel.display())))?;
    }
    Ok(())
}

fn pack_err(msg: String) -> RunnerError {
    RunnerError::StepFailed {
        step: "build-image".into(),
        msg,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn entries_of(tarball: &[u8]) -> Vec<(String, Vec<u8>)> {
        let gz = flate2::read::GzDecoder::new(tarball);
        let mut archive = tar::Archive::new(gz);
        let mut out = Vec::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let name = entry.path().unwrap().to_string_lossy().into_owned();
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).unwrap();
            out.push((name, bytes));
        }
        out
    }

    /// The tar root is the context root (no leading directory component), and
    /// nested files keep their relative path. BuildKit unpacks the tar *as*
    /// the context, so an extra top-level directory would break every COPY.
    #[test]
    fn packs_relative_to_the_context_root() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("build-v8.sh"), b"#!/bin/sh\n").unwrap();
        std::fs::create_dir(dir.path().join("patches")).unwrap();
        std::fs::write(dir.path().join("patches/a.patch"), b"diff\n").unwrap();

        let names: Vec<String> = entries_of(&pack_context(dir.path(), &[]).unwrap())
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert_eq!(names, vec!["build-v8.sh", "patches/a.patch"]);
    }

    /// R636-B2: the packed tar must carry NO extended attributes.
    ///
    /// This is a load-bearing property, not a nicety. BuildKit unpacks a
    /// remote (`--opt context=<url>`) tarball into a snapshot and replays every
    /// xattr it finds; a Linux kernel rejects any xattr outside the
    /// `user.`/`trusted.`/`security.` namespaces outright. Every source file in
    /// this repo carries `com.apple.provenance` on the camp Mac, so a context
    /// packed by macOS `tar` — which emits `SCHILY.xattr.*` pax headers —
    /// fails the build at `copy /context /` with
    /// `lsetxattr <file>: operation not supported`, before the Dockerfile is
    /// even read. That failure looked exactly like a BuildKit snapshotter
    /// problem and cost this ticket a full diagnostic cycle chasing one.
    ///
    /// `pack_context` is safe because the `tar` crate builds headers from
    /// `fs::Metadata` alone and has no xattr support — but "safe by accident of
    /// a dependency's feature set" is precisely the kind of property that
    /// silently regresses, so pin it.
    #[test]
    fn packs_without_extended_attributes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("verify-consumer.sh"), b"#!/bin/sh\n").unwrap();

        // Best-effort: set a real xattr so this test has teeth on platforms
        // that support one. The assertion below holds either way.
        #[cfg(target_os = "macos")]
        {
            let path = dir.path().join("verify-consumer.sh");
            let _ = std::process::Command::new("xattr")
                .args(["-w", "com.apple.provenance", "x"])
                .arg(&path)
                .status();
        }

        let tarball = pack_context(dir.path(), &[]).unwrap();
        let gz = flate2::read::GzDecoder::new(tarball.as_slice());
        let mut archive = tar::Archive::new(gz);
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let name = entry.path().unwrap().to_string_lossy().into_owned();
            assert!(
                !name.starts_with("._"),
                "AppleDouble sidecar leaked into the context tar: {name}"
            );
            let pax: Vec<String> = entry
                .pax_extensions()
                .unwrap()
                .into_iter()
                .flatten()
                .map(|e| e.unwrap().key().unwrap().to_string())
                .collect();
            assert!(
                !pax.iter().any(|k| k.contains("xattr")),
                "{name} carries xattr pax headers {pax:?} — BuildKit will fail \
                 the remote-context unpack with lsetxattr: operation not supported"
            );
        }
    }

    /// The compiled Dockerfile is appended last so it WINS over a same-named
    /// file in the context — the runner compiled it from the catalog and that
    /// is what the step means to build.
    #[test]
    fn extra_entries_override_same_named_context_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("x.Dockerfile"), b"FROM stale\n").unwrap();

        let tarball = pack_context(
            dir.path(),
            &[("x.Dockerfile".to_string(), b"FROM compiled\n".to_vec())],
        )
        .unwrap();

        let entries = entries_of(&tarball);
        // Both are present; extraction is last-write-wins, so ours must be last.
        let last = entries.last().unwrap();
        assert_eq!(last.0, "x.Dockerfile");
        assert_eq!(last.1, b"FROM compiled\n");
    }

    /// Entry order is stable across runs regardless of readdir order.
    #[test]
    fn packs_in_sorted_order() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["zeta", "alpha", "mid"] {
            std::fs::write(dir.path().join(name), name.as_bytes()).unwrap();
        }
        let names: Vec<String> = entries_of(&pack_context(dir.path(), &[]).unwrap())
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert_eq!(names, vec!["alpha", "mid", "zeta"]);
    }

    /// The size ceiling aborts the walk and the message names both the file it
    /// tripped on and the knob that fixes it — an operator who points a remote
    /// build at the camp root must not just see "too big".
    #[test]
    fn oversized_context_is_refused_with_an_actionable_message() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("big.bin"), vec![0u8; 4096]).unwrap();

        // Re-run the budget logic at a size the fixture can trip without
        // writing half a gigabyte: shrink by packing twice the file size away.
        let mut budget = 1024u64;
        let err = append_dir(
            &mut tar::Builder::new(Vec::new()),
            dir.path(),
            Path::new(""),
            &mut budget,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("big.bin"), "must name the file: {msg}");
        assert!(msg.contains("context ="), "must name the fix: {msg}");
    }

    /// The unwired default refuses rather than silently falling back to the
    /// bind-mount shape that fails minutes later inside runc.
    #[tokio::test]
    async fn no_publisher_refuses_with_the_wiring_instruction() {
        let err = NoBuildContextPublisher
            .publish("k", vec![])
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("with_build_context_publisher"), "{msg}");
    }
}
