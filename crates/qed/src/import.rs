//! The W224 import primitive's pure core (R533-F1).
//!
//! W224 settles "what is a GitHub Actions workflow to QED?" as **import, not
//! emulate**: a `workflow.yml` is an *import source* QED expands into its own
//! native subgraph, not a foreign runtime QED faithfully reproduces forever.
//! This module holds the side-effect-free heart of that primitive:
//!
//! 1. [`content_hash`] — the blake3 pin of a source yml's raw bytes. The pin
//!    lives in [`ImportConfig::hash`](crate::types::ImportConfig::hash); on
//!    every run the runner recomputes the source's hash and compares.
//! 2. [`ImportFreshness`] + [`ImportConfig::freshness`] — the staleness
//!    decision the pin enables. There are never two editable canonical copies
//!    at once (W224): while the yml is canonical the expansion is *virtual*
//!    (recomputed at plan time, never stored — zero drift by construction), so
//!    a drifted source is benign (re-expand + re-pin). Once a generated TOML is
//!    materialized (`eject`, R533-F6) the pin instead marks that on-disk
//!    derivative stale.
//! 3. [`expand_import`] — the plan-time expansion seam: parsed workflow →
//!    native QED subgraph.
//!
//! ## F1 scope of the expansion
//!
//! The mechanical tier-1/2 → native step mapping is **R533-F4** (the assisted
//! transformer). Until it lands, [`expand_import`] produces the single-node
//! [`ImportExpansion::Delegated`] form: route the whole workflow through the
//! recast W200 GHA front-end (the `qed-gha` parser + tier-1/2 executor, which
//! W224 keeps and re-points). This is the migration ramp — while GHA is
//! canonical the import step still *runs* — and it keeps the runner seam,
//! freshness check, and re-pin loop settled here so F4 swaps only the
//! expansion body, not the surrounding machinery.

use crate::types::{GhaWorkflowConfig, ImportConfig};

/// blake3 content hash of a source workflow's raw bytes, hex-encoded. The pin
/// stored in [`ImportConfig::hash`](crate::types::ImportConfig::hash) is
/// exactly this string.
///
/// Hashing the raw bytes (not the parsed AST) is deliberate: it catches every
/// edit — including comment / whitespace churn that a re-serialized AST would
/// erase — so "is this byte-for-byte the yml I pinned?" is answered without
/// re-parsing, and a hand-edit can never be silently honored.
pub fn content_hash(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// Freshness of an imported source relative to its pinned hash.
///
/// The disposition of [`Stale`](ImportFreshness::Stale) depends on the import's
/// `materialize` toggle, not on this enum: virtual expansion re-expands and
/// re-pins (benign); a materialized eject treats it as a stale derivative
/// (R533-F6). This type only reports the comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportFreshness {
    /// No hash pinned yet — first import, or a hand-authored `[import]` block.
    /// The caller should expand and adopt the freshly-computed hash as the pin.
    Unpinned,
    /// The source's current hash matches the pin. Safe to expand.
    Fresh,
    /// The source drifted since it was pinned. Carries both hashes so a
    /// caller (or `qed validate`, R533-F6) can report the divergence.
    Stale { pinned: String, actual: String },
}

impl ImportFreshness {
    /// Whether the on-disk source still matches its pin (or was never pinned).
    /// `false` only for [`Stale`](ImportFreshness::Stale).
    pub fn is_current(&self) -> bool {
        !matches!(self, ImportFreshness::Stale { .. })
    }
}

impl ImportConfig {
    /// Compare a freshly-computed source hash against the pinned one.
    ///
    /// `actual` is the [`content_hash`] of the bytes currently on disk; the
    /// caller computes it (the runner has just read the file, so it owns the
    /// bytes). Pure — no I/O here.
    pub fn freshness(&self, actual: &str) -> ImportFreshness {
        match self.hash.as_deref() {
            None => ImportFreshness::Unpinned,
            Some(pinned) if pinned == actual => ImportFreshness::Fresh,
            Some(pinned) => ImportFreshness::Stale {
                pinned: pinned.to_string(),
                actual: actual.to_string(),
            },
        }
    }
}

/// The result of expanding an imported workflow at plan time.
///
/// Modeled as an enum from the start so the runner seam stays stable across the
/// F1 → F4 transition: F1 only ever yields [`Delegated`](ImportExpansion::Delegated);
/// R533-F4 adds a native-steps variant carrying the mechanical tier-1/2 map,
/// and the runner's `match` grows one arm rather than changing the call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportExpansion {
    /// Single-node delegation: run the whole workflow through the recast W200
    /// GHA front-end. The F1 default and the migration ramp while GHA is
    /// canonical. R533-F4 introduces the native-steps form alongside this.
    Delegated(GhaWorkflowConfig),
}

/// Expand an imported workflow into a QED subgraph at plan time (W224 "import,
/// don't emulate").
///
/// F1 SCOPE: returns [`ImportExpansion::Delegated`] — the single-node form that
/// routes through the W200 GHA front-end. The `event` / `inputs` carried on the
/// [`ImportConfig`] are forwarded into the synthesized [`GhaWorkflowConfig`] so
/// the expansion impersonates the same trigger the source declares. R533-F4
/// replaces this body with the mechanical tier-1/2 native map (and, for tier-3
/// steps, native-replacement stanzas); the pin + virtual/eject toggle around it
/// are already owned by the caller, so nothing else moves.
pub fn expand_import(cfg: &ImportConfig) -> ImportExpansion {
    ImportExpansion::Delegated(GhaWorkflowConfig {
        path: cfg.source.clone(),
        event: cfg.event.clone(),
        inputs: cfg.inputs.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn cfg(hash: Option<&str>) -> ImportConfig {
        ImportConfig {
            source: PathBuf::from(".github/workflows/release.yml"),
            hash: hash.map(str::to_string),
            materialize: false,
            event: None,
            inputs: Default::default(),
        }
    }

    #[test]
    fn content_hash_is_stable_and_byte_sensitive() {
        let a = content_hash(b"name: release\n");
        let b = content_hash(b"name: release\n");
        let c = content_hash(b"name: release \n"); // one extra space
        assert_eq!(a, b, "same bytes hash identically");
        assert_ne!(a, c, "a one-byte edit changes the pin");
        // blake3 hex is 64 chars.
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn freshness_unpinned_when_no_hash() {
        assert_eq!(cfg(None).freshness("deadbeef"), ImportFreshness::Unpinned);
    }

    #[test]
    fn freshness_fresh_on_match() {
        let h = content_hash(b"on: push\n");
        assert_eq!(cfg(Some(&h)).freshness(&h), ImportFreshness::Fresh);
    }

    #[test]
    fn freshness_stale_on_drift_carries_both_hashes() {
        let pinned = content_hash(b"on: push\n");
        let actual = content_hash(b"on: workflow_dispatch\n");
        let f = cfg(Some(&pinned)).freshness(&actual);
        assert_eq!(
            f,
            ImportFreshness::Stale {
                pinned: pinned.clone(),
                actual: actual.clone(),
            }
        );
        assert!(!f.is_current(), "stale is not current");
        assert!(ImportFreshness::Fresh.is_current());
        assert!(ImportFreshness::Unpinned.is_current());
    }

    #[test]
    fn expand_forwards_source_event_and_inputs() {
        let mut c = cfg(None);
        c.event = Some("workflow_dispatch".into());
        c.inputs.insert("tag".into(), "v1.2.3".into());
        let ImportExpansion::Delegated(gha) = expand_import(&c);
        assert_eq!(gha.path, c.source);
        assert_eq!(gha.event.as_deref(), Some("workflow_dispatch"));
        assert_eq!(gha.inputs.get("tag").map(String::as_str), Some("v1.2.3"));
    }
}
