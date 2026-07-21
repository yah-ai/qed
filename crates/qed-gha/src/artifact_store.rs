//! Injectable artifact store for `actions/upload-artifact` /
//! `actions/download-artifact` (R594).
//!
//! W224 retired *running* the artifact actions in the runtime (they're tier-3
//! GitHub services — [`crate::tier`] flags them "replace with a content-
//! addressed output / `needs:` edge"). But to actually execute a real workflow
//! where one job uploads binaries and a later job downloads them, the qed runner
//! injects an [`ArtifactStore`] via [`crate::Executor::with_artifact_store`].
//!
//! Same discipline as [`crate::image_builder`]: when a store is injected the
//! runtime routes the two artifact slugs to it; when absent the tier-3 error is
//! preserved, so bare qed-gha still never touches an artifact store.
//!
//! The single-host implementation (qed runner's `LocalArtifactStore`) is a
//! content-addressed directory under the workspace; the fleet phase swaps in a
//! transport that ships artifacts to a build-worker (cross-host `download` is
//! how an image job on one node consumes binaries built on another).

use std::path::Path;

use indexmap::IndexMap;

use crate::expr::Value;
use crate::toolkit::ToolkitOutcome;

/// Per-call inputs for an artifact action — the evaluated `with:` block plus the
/// workflow workspace (upload sources and download targets resolve against it).
pub struct ArtifactCall<'a> {
    pub with: &'a IndexMap<String, Value>,
    pub workspace: &'a Path,
}

/// Implementor contract for the injected artifact store. Implemented by the qed
/// runner (`LocalArtifactStore`).
///
/// A missing artifact on `download` (or a missing source path on `upload`) is a
/// *step* failure — return `Ok(ToolkitOutcome { conclusion: Failure, … })`, not
/// `Err`, so `needs.<job>.result` / `if:` gating stays correct instead of
/// aborting the whole run. `Err` is reserved for unrecoverable IO/wiring faults.
pub trait ArtifactStore: Send + Sync {
    fn upload(&self, call: &ArtifactCall<'_>) -> Result<ToolkitOutcome, String>;
    fn download(&self, call: &ArtifactCall<'_>) -> Result<ToolkitOutcome, String>;
}

/// The artifact slugs the runtime routes to an injected [`ArtifactStore`].
pub fn is_artifact_action(slug: &str) -> bool {
    matches!(slug, "actions/upload-artifact" | "actions/download-artifact")
}
