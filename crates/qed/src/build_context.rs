//! Cross-host build contexts (R636-B1 for `StepKind::BuildImage`; R560-T8 for
//! remote `StepKind::Subprocess` source trees).
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
//!
//! # The same gap, one layer over: remote subprocess steps (R560-T8)
//!
//! `build_workload_spec` gives an offloaded subprocess exactly three things —
//! the image, the argv, and the `/yah/produced` durable mount. No source. That
//! is fine for `rusty-v8-musl`, whose baked script clones V8 from the internet
//! inside the container, and fatal for any step that compiles the *camp tree*:
//! the `mesofact-musl` legs need `oss/mesofact` plus the two sibling subtrees
//! its path deps escape into. [`pack_source_context`] is the transport,
//! deliberately reusing [`BuildContextPublisher`] rather than growing a second
//! bytes-to-the-worker path — same trait, same single-use run-scoped key, same
//! delete-on-both-legs. The only difference is what goes in the tar and who
//! unpacks it: git-tracked files at their camp-root-relative paths, unpacked
//! by the step's own argv out of `$YAH_SOURCE_CONTEXT_URL`.

use std::path::{Path, PathBuf};

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
            "this step runs on a different host than qed, so the bytes it needs (a \
             build-image context, or an offloaded subprocess's `source_context` \
             subtrees) have to be fetched by the worker rather than bind-mounted — but \
             no build-context publisher is wired into this runner. The `yah` CLI wires \
             an R2-backed one on every fleet-capable runner; a bare `yah-qed` embedding \
             must supply its own via `PipelineRunner::with_build_context_publisher`."
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

/// Env var naming the URL a remote subprocess step fetches its source tree
/// from (R560-T8).
///
/// Set by the runner on the offloaded-subprocess path whenever the step
/// declares [`QedStep::source_context`](crate::types::QedStep::source_context);
/// never set otherwise, so a step that does not declare one sees exactly the
/// environment it saw before this existed.
pub const SOURCE_CONTEXT_URL_ENV: &str = "YAH_SOURCE_CONTEXT_URL";

/// Pack the **git-tracked** files under `paths` into one gzipped tar whose
/// entries keep their `camp_root`-relative paths (R560-T8).
///
/// # Why `git ls-files` and not a directory walk
///
/// Because the numbers are not close. `oss/mesofact` is 28 GB on disk and 5 MB
/// tracked — the difference is `target/` and seven `node_modules/`. A walk
/// would trip [`MAX_CONTEXT_BYTES`] long before it reached a source file, and
/// the fix would be to reinvent ignore rules that git already computes
/// correctly for this tree. Reading from the *working tree* rather than from a
/// git object means uncommitted edits travel, which is the behaviour a build
/// dispatched from a live camp has to have.
///
/// # Why the paths stay camp-root-relative
///
/// Unlike a build-image context — where the tar root *is* the context — the
/// consumer here unpacks into an empty directory and expects a repo-shaped
/// tree. `oss/mesofact`'s path deps escape its own workspace into
/// `oss/yah-base` and `oss/cheers` (that is why the image's `build-mesofact.sh`
/// takes a repo root and refuses without both siblings). Flattening to a
/// per-path tar root would break exactly that.
///
/// A tracked path that has been deleted in the working tree is skipped: the
/// tree really does not have it, and the point is to ship the tree as
/// positioned.
pub fn pack_source_context(camp_root: &Path, paths: &[PathBuf]) -> Result<Vec<u8>, RunnerError> {
    let rel = source_context_files(camp_root, paths)?;

    let mut budget = MAX_CONTEXT_BYTES;
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut tar = tar::Builder::new(gz);
    tar.follow_symlinks(true);

    for entry in &rel {
        let abs = camp_root.join(entry);
        let meta = match std::fs::metadata(&abs) {
            Ok(meta) => meta,
            // Tracked but deleted in the working tree, or a dangling symlink.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(source_err(format!("stat {}: {e}", abs.display()))),
        };
        if meta.is_dir() {
            // A tracked gitlink (submodule) shows up as a path with no file
            // behind it. Nothing to ship, and recursing would leave the
            // camp-root-relative contract.
            continue;
        }
        let len = meta.len();
        budget = budget.checked_sub(len).ok_or_else(|| {
            source_err(format!(
                "source context exceeds the {} MiB cross-host limit (tripped at {}). \
                 Narrow `source_context` to the subtrees the build actually compiles.",
                MAX_CONTEXT_BYTES / (1024 * 1024),
                entry.display(),
            ))
        })?;

        let mut file = std::fs::File::open(&abs)
            .map_err(|e| source_err(format!("opening {}: {e}", abs.display())))?;
        tar.append_file(entry, &mut file).map_err(|e| {
            source_err(format!("adding {} to the source tar: {e}", entry.display()))
        })?;
    }

    let gz = tar
        .into_inner()
        .map_err(|e| source_err(format!("finishing the source tar: {e}")))?;
    gz.finish()
        .map_err(|e| source_err(format!("compressing the source tar: {e}")))
}

/// The git-tracked files under `paths`, camp-root-relative and sorted.
///
/// Shared by [`pack_source_context`] and [`source_context_fingerprint`] so the
/// two cannot disagree about *which* files a step's source context is. A
/// fingerprint that covered a different set than the pack ships would be a
/// cache key that misses the change it exists to detect.
fn source_context_files(camp_root: &Path, paths: &[PathBuf]) -> Result<Vec<PathBuf>, RunnerError> {
    if paths.is_empty() {
        return Err(source_err(
            "source context requested with no paths".to_string(),
        ));
    }

    let mut cmd = std::process::Command::new("git");
    cmd.arg("-C").arg(camp_root).args(["ls-files", "-z", "--"]);
    for path in paths {
        cmd.arg(path);
    }
    let out = cmd.output().map_err(|e| {
        source_err(format!(
            "running git ls-files in {}: {e}",
            camp_root.display()
        ))
    })?;
    if !out.status.success() {
        return Err(source_err(format!(
            "git ls-files in {} failed: {}",
            camp_root.display(),
            String::from_utf8_lossy(&out.stderr).trim(),
        )));
    }

    // Deterministic order, for the same reason `append_dir` sorts: two packs of
    // an unchanged tree must produce the same bytes. `git ls-files` already
    // sorts, but sorting here means that stays true if the source of the list
    // ever changes.
    let mut rel: Vec<PathBuf> = out
        .stdout
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| PathBuf::from(String::from_utf8_lossy(s).into_owned()))
        .collect();
    rel.sort();

    if rel.is_empty() {
        return Err(source_err(format!(
            "`source_context` matched no git-tracked files under {:?} — the step would \
             fetch an empty source tree and fail at the build. Check the paths are \
             camp-root-relative and tracked.",
            paths,
        )));
    }

    Ok(rel)
}

/// BLAKE3 over the *content* of everything [`pack_source_context`] would ship —
/// the identity of a step's build inputs, as a cache key (R746-F2).
///
/// # Why not just hash the packed tar
///
/// Because the tar is not content-only. [`tar::Builder::append_file`] copies the
/// file's mode, uid/gid and **mtime** into each header, so a `git checkout` that
/// rewrites timestamps without changing a byte produces a different archive. As
/// a transport that is fine; as a cache key it would dispatch an hour-long fleet
/// build for a branch switch that changed nothing. This hashes `(relative path,
/// BLAKE3 of bytes)` pairs instead, so it moves when and only when the source
/// the build compiles moves.
///
/// Executable bit and symlink target are deliberately *not* covered: nothing in
/// a Rust source tree builds differently for them, and including mode would
/// reintroduce a filesystem-dependent key on a tree that gets checked out on
/// both macOS and Linux.
///
/// A tracked path deleted in the working tree is skipped, exactly as the pack
/// skips it — the key describes the tree as positioned.
pub fn source_context_fingerprint(
    camp_root: &Path,
    paths: &[PathBuf],
) -> Result<String, RunnerError> {
    let rel = source_context_files(camp_root, paths)?;

    let mut acc = blake3::Hasher::new();
    for entry in &rel {
        let abs = camp_root.join(entry);
        let meta = match std::fs::metadata(&abs) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(source_err(format!("stat {}: {e}", abs.display()))),
        };
        // A tracked gitlink (submodule) has no file behind it — same skip the
        // pack makes.
        if meta.is_dir() {
            continue;
        }
        let bytes = std::fs::read(&abs)
            .map_err(|e| source_err(format!("reading {} for fingerprint: {e}", abs.display())))?;
        // Length-delimited so no rename can collide with a content change:
        // `a/bc` + `d` and `a/b` + `cd` hash differently.
        let path_bytes = entry.to_string_lossy();
        acc.update(&(path_bytes.len() as u64).to_le_bytes());
        acc.update(path_bytes.as_bytes());
        acc.update(blake3::hash(&bytes).as_bytes());
    }
    Ok(acc.finalize().to_hex().to_string())
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
        let entry = entry.map_err(|e| pack_err(format!("walking {}: {e}", dir.display())))?;
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
        *budget = budget
            .checked_sub(len)
            .ok_or_else(|| RunnerError::StepFailed {
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

fn source_err(msg: String) -> RunnerError {
    RunnerError::StepFailed {
        step: "source-context".into(),
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

    // ── pack_source_context (R560-T8) ────────────────────────────────────────

    /// A throwaway git repo with `tracked/` committed and `ignored/` matching
    /// `.gitignore` — the miniature of the real shape (`oss/mesofact` at 5 MB
    /// tracked inside 28 GB of `target/` + `node_modules/`).
    fn source_repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(root)
                .args(args)
                .output()
                .unwrap();
            assert!(out.status.success(), "git {args:?}: {out:?}");
        };
        git(&["init", "-q"]);
        git(&["config", "user.email", "t@example.com"]);
        git(&["config", "user.name", "t"]);

        std::fs::create_dir_all(root.join("pkg/src")).unwrap();
        std::fs::create_dir_all(root.join("pkg/target/debug")).unwrap();
        std::fs::create_dir_all(root.join("sibling/src")).unwrap();
        std::fs::create_dir_all(root.join("elsewhere")).unwrap();
        std::fs::write(root.join(".gitignore"), "target/\n").unwrap();
        std::fs::write(root.join("pkg/src/lib.rs"), b"// lib\n").unwrap();
        std::fs::write(root.join("pkg/Cargo.toml"), b"[package]\n").unwrap();
        std::fs::write(root.join("pkg/target/debug/huge.bin"), vec![0u8; 4096]).unwrap();
        std::fs::write(root.join("sibling/src/dep.rs"), b"// dep\n").unwrap();
        std::fs::write(root.join("elsewhere/nope.rs"), b"// nope\n").unwrap();
        git(&["add", "-A"]);
        git(&["commit", "-qm", "init"]);
        dir
    }

    /// The two properties the whole transport rests on: entries keep their
    /// **camp-root-relative** paths (so an escaping path dep still resolves
    /// after unpacking), and only **git-tracked** files travel (so `target/`
    /// cannot blow the size ceiling before a single source file ships).
    ///
    /// The path shape is the one that differs from `pack_context`, where the
    /// tar root *is* the context. Getting it wrong here produces a tar that
    /// unpacks to `src/lib.rs` instead of `pkg/src/lib.rs`, and cargo then
    /// fails on a missing workspace member rather than on anything that names
    /// the real cause.
    #[test]
    fn source_context_ships_tracked_files_at_camp_relative_paths() {
        let repo = source_repo();
        let tarball = pack_source_context(
            repo.path(),
            &[PathBuf::from("pkg"), PathBuf::from("sibling")],
        )
        .unwrap();

        let names: Vec<String> = entries_of(&tarball).into_iter().map(|(n, _)| n).collect();
        assert_eq!(
            names,
            vec!["pkg/Cargo.toml", "pkg/src/lib.rs", "sibling/src/dep.rs"],
            "camp-relative paths, sorted, tracked-only",
        );
        assert!(
            !names.iter().any(|n| n.contains("target/")),
            "an ignored build dir must not travel: {names:?}",
        );
        assert!(
            !names.iter().any(|n| n.starts_with("elsewhere/")),
            "only the named subtrees travel: {names:?}",
        );
    }

    /// Two packs of an unchanged tree produce identical bytes. A future
    /// content-addressed cache over this tar would otherwise miss on
    /// directory-iteration order alone — the same property `pack_context`'s
    /// sort exists for.
    #[test]
    fn source_context_packs_deterministically() {
        let repo = source_repo();
        let paths = [PathBuf::from("pkg"), PathBuf::from("sibling")];
        assert_eq!(
            pack_source_context(repo.path(), &paths).unwrap(),
            pack_source_context(repo.path(), &paths).unwrap(),
        );
    }

    /// A tracked file deleted in the working tree is skipped rather than
    /// erroring: the tree really does not have it, and the point of reading the
    /// working tree instead of a git object is to ship the tree as positioned.
    #[test]
    fn source_context_skips_files_deleted_in_the_working_tree() {
        let repo = source_repo();
        std::fs::remove_file(repo.path().join("pkg/src/lib.rs")).unwrap();
        let names: Vec<String> =
            entries_of(&pack_source_context(repo.path(), &[PathBuf::from("pkg")]).unwrap())
                .into_iter()
                .map(|(n, _)| n)
                .collect();
        assert_eq!(names, vec!["pkg/Cargo.toml"]);
    }

    /// A path that matches nothing tracked is refused at pack time, not
    /// discovered on the worker. The failure mode it prevents is the expensive
    /// one: dispatch, a ~2.5 GB image pull, a container start, and then cargo
    /// failing on an empty directory with a message about a workspace root.
    #[test]
    fn source_context_matching_nothing_is_refused_with_an_actionable_message() {
        let repo = source_repo();
        let err = pack_source_context(repo.path(), &[PathBuf::from("oss/typo")]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no git-tracked files"), "{msg}");
        assert!(msg.contains("camp-root-relative"), "{msg}");
    }

    /// The fingerprint is content-only: touching every file (what a `git
    /// checkout` or a `cargo` pass does to mtimes) must not move it. This is the
    /// whole reason it is not `blake3(pack_source_context(..))` — the tar
    /// carries mtimes in its headers, so hashing it would rebuild on a branch
    /// switch that changed nothing.
    #[test]
    fn fingerprint_ignores_mtime() {
        let repo = source_repo();
        let paths = [PathBuf::from("pkg"), PathBuf::from("sibling")];
        let before = source_context_fingerprint(repo.path(), &paths).unwrap();

        let file = repo.path().join("pkg/src/lib.rs");
        let content = std::fs::read(&file).unwrap();
        std::fs::remove_file(&file).unwrap();
        std::fs::write(&file, &content).unwrap();

        assert_eq!(
            before,
            source_context_fingerprint(repo.path(), &paths).unwrap()
        );
        // …and the packed tar really does move, which is the thing being
        // guarded against rather than assumed.
        assert_ne!(
            before,
            blake3::hash(&pack_source_context(repo.path(), &paths).unwrap())
                .to_hex()
                .to_string(),
        );
    }

    /// One changed byte in one tracked file moves the fingerprint — the
    /// "the Rust side actually changed" signal the build-dispatch arm keys on.
    #[test]
    fn fingerprint_moves_on_a_content_change() {
        let repo = source_repo();
        let paths = [PathBuf::from("pkg")];
        let before = source_context_fingerprint(repo.path(), &paths).unwrap();
        std::fs::write(repo.path().join("pkg/src/lib.rs"), b"pub fn f() {}\n").unwrap();
        assert_ne!(
            before,
            source_context_fingerprint(repo.path(), &paths).unwrap()
        );
    }

    /// An untracked file does not move it. Everything under `oss/mesofact`'s
    /// 28 GB of `target/` and `node_modules/` is untracked; a fingerprint that
    /// moved with them would never hit.
    #[test]
    fn fingerprint_ignores_untracked_files() {
        let repo = source_repo();
        let paths = [PathBuf::from("pkg")];
        let before = source_context_fingerprint(repo.path(), &paths).unwrap();
        std::fs::create_dir_all(repo.path().join("pkg/target")).unwrap();
        std::fs::write(repo.path().join("pkg/target/junk.bin"), b"\x00\x01").unwrap();
        assert_eq!(
            before,
            source_context_fingerprint(repo.path(), &paths).unwrap()
        );
    }

    /// Same refusal as the pack, from the same enumeration — a typo'd subtree
    /// must not silently fingerprint as "no inputs" and then reuse a stale
    /// binary forever.
    #[test]
    fn fingerprint_matching_nothing_is_refused() {
        let repo = source_repo();
        let err =
            source_context_fingerprint(repo.path(), &[PathBuf::from("oss/typo")]).unwrap_err();
        assert!(err.to_string().contains("no git-tracked files"), "{err}");
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
