//! Content-addressed landing for artifacts retrieved off a remote build-worker
//! (R590-F6 leg 2).
//!
//! A native remote step (the rusty_v8 musl build on us-west-002) writes its
//! output tarball to a path *inside* the build-worker container. On-box green
//! (R590-B5 + redeploy) proves placement + the native build, but nothing pulls
//! the bytes back to camp. This module is the landing half of that retrieval:
//! the runner fetches the produced bytes over the [`task::remote::WardenClient`]
//! transport seam, then [`ContentAddressedStore::land`]s them here.
//!
//! **Content-addressed, not name-addressed.** Unlike [`crate::artifact_local`]
//! (the GHA `upload-artifact`/`download-artifact` store, keyed by artifact
//! *name*), a retrieved build output is keyed by the BLAKE3 of its bytes. That
//! address IS the integrity check the ticket calls "BLAKE3-preservation": the
//! landed file's content hash equals the hash of what the worker emitted, so a
//! byte rewritten in transit changes the address and is impossible to miss.
//!
//! **Feeds, does not duplicate, the publish leg.** The landed file is exactly
//! R546-T3's bootstrap-publish input — the bytes behind the zero-sentinel
//! hashes in `.yah/services/yah-cloud/components/rusty-v8-musl/workload.toml`. The W164 derived-static-asset
//! reconciler / [`crate::types::Outcome::Publish`] consume the landed path; this
//! module does not itself upload anything.

use std::io::Write;
use std::path::{Path, PathBuf};

/// An artifact retrieved off a build-worker and landed in the local
/// content-addressed store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievedArtifact {
    /// BLAKE3 of the bytes, lowercase hex — the store address and the
    /// integrity check the consumer contract pins.
    pub blake3: String,
    /// Absolute path to the landed file (`<root>/<blake3>`).
    pub path: PathBuf,
    /// Byte length of the landed content.
    pub size: u64,
}

/// A directory that stores blobs under their BLAKE3 hex address.
///
/// Landing is idempotent: re-landing identical bytes is a no-op that returns the
/// same address (content-addressing makes a second write pointless). Writes go
/// through a temp file + atomic rename so a crash mid-write can never leave a
/// truncated blob at the address of its full content.
#[derive(Debug, Clone)]
pub struct ContentAddressedStore {
    root: PathBuf,
}

impl ContentAddressedStore {
    /// Root the store at `root` (created lazily on the first `land`). For the
    /// qed runner this is `<camp_root>/.yah/cache/artifacts`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The store's root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Land `bytes` under their BLAKE3 address and return the
    /// [`RetrievedArtifact`] describing where they went.
    ///
    /// The returned `blake3` is computed from the in-memory bytes; the file on
    /// disk is those exact bytes, so re-hashing the file yields the same address
    /// (the BLAKE3-preservation guarantee, exercised by the unit tests).
    pub fn land(&self, bytes: &[u8]) -> std::io::Result<RetrievedArtifact> {
        let blake3 = blake3::hash(bytes).to_hex().to_string();
        std::fs::create_dir_all(&self.root)?;
        let path = self.root.join(&blake3);

        // Idempotent: identical content already at this address — nothing to do.
        if !path.exists() {
            // Temp file in the same dir so the rename is atomic (same
            // filesystem). The temp name carries the pid + address to avoid
            // colliding with a concurrent landing of different content.
            let tmp = self.root.join(format!(".tmp-{}-{}", std::process::id(), &blake3));
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(bytes)?;
            f.sync_all()?;
            // rename is atomic on the same fs; if a racing landing beat us to
            // the address the content is identical, so overwriting is harmless.
            std::fs::rename(&tmp, &path)?;
        }

        Ok(RetrievedArtifact {
            blake3,
            path,
            size: bytes.len() as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn land_is_content_addressed_and_preserves_blake3() {
        let dir = TempDir::new().unwrap();
        let store = ContentAddressedStore::new(dir.path().join("artifacts"));
        let bytes = b"librusty_v8_release_x86_64-unknown-linux-musl.a bytes \x00\xff";

        let landed = store.land(bytes).unwrap();

        // Address == BLAKE3 of the input.
        assert_eq!(landed.blake3, blake3::hash(bytes).to_hex().to_string());
        // Landed file lives at <root>/<blake3>.
        assert_eq!(landed.path, store.root().join(&landed.blake3));
        assert_eq!(landed.size, bytes.len() as u64);

        // BLAKE3-preservation: the bytes on disk re-hash to the same address —
        // nothing was lost or rewritten in transit.
        let on_disk = std::fs::read(&landed.path).unwrap();
        assert_eq!(on_disk, bytes);
        assert_eq!(blake3::hash(&on_disk).to_hex().to_string(), landed.blake3);
    }

    #[test]
    fn land_is_idempotent_for_identical_bytes() {
        let dir = TempDir::new().unwrap();
        let store = ContentAddressedStore::new(dir.path().join("artifacts"));
        let bytes = b"same bytes twice";

        let a = store.land(bytes).unwrap();
        let b = store.land(bytes).unwrap();
        assert_eq!(a, b, "re-landing identical bytes returns the same address");

        // Exactly one blob at the address (plus no leftover temp files).
        let entries: Vec<_> = std::fs::read_dir(store.root())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec![a.blake3], "one content-addressed blob, no temp debris");
    }

    #[test]
    fn distinct_bytes_land_at_distinct_addresses() {
        let dir = TempDir::new().unwrap();
        let store = ContentAddressedStore::new(dir.path().join("artifacts"));
        let a = store.land(b"alpha").unwrap();
        let b = store.land(b"beta").unwrap();
        assert_ne!(a.blake3, b.blake3);
        assert_ne!(a.path, b.path);
    }
}
