//! Declared-input freshness (R717-T1, W296) — is a step's recorded result still
//! about the bytes that produced it?
//!
//! This is [`crate::import`]'s blake3 source pin **re-pointed off
//! [`StepKind::Import`](crate::types::StepKind::Import) onto any step kind**. It
//! is the same mechanism and the same doctrine, generalized: a step declares
//! [`QedStep::inputs`](crate::types::QedStep::inputs), the runner digests each
//! one before executing and records the map on
//! [`StepStatus::input_hashes`](crate::types::StepStatus::input_hashes), and a
//! reader compares that map against the tree later.
//!
//! ## Why this exists at all
//!
//! W257 (static node fleet onboarding) names the trap this closes, in prose,
//! because prose was the only thing available:
//!
//! > There's no version stamp to check here the way step 4 checks the Pi's
//! > identity — the only guard is **habit**: re-run `build-iso.sh` and reflash
//! > the stick after **any** `.cfg` edit.
//!
//! Habit is not a guard. `blake3(the .cfg) != the digest recorded when the ISO
//! was built` is a guard, and it is a *computable relation* — which is exactly
//! the class of thing a runbook's prose cannot track and a content hash can.
//!
//! ## Two invariants worth not breaking
//!
//! 1. **Staleness is computed, never stored.** No `RunStatus` variant means
//!    stale. The recorded digests are the only persisted half; the verdict is
//!    derived at read time from `(recorded digests, tree now)`. That is why a
//!    stale badge cannot rot — it is a pure function of two things that are both
//!    still present when someone reads it.
//! 2. **This module is pure.** Comparison does no I/O.
//!    [`hash_declared_inputs`] is the one function here that touches the
//!    filesystem, and it is kept separate so the decision logic is testable
//!    without a fixture tree.

use std::collections::BTreeMap;
use std::path::Path;

/// Recorded in place of a digest when a declared input did not exist, or could
/// not be read, at the moment the step ran.
///
/// Deliberately not a blake3 hex string (those are always 64 hex characters), so
/// it can never collide with a real digest. Deliberately *recorded* rather than
/// omitted: an absent key would be indistinguishable from "this run predates the
/// field", and the two mean opposite things — the first is a fact about that
/// run, the second is an absence of evidence.
pub const ABSENT_INPUT: &str = "absent";

/// Verdict for one step's declared inputs, compared against the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputFreshness {
    /// The step declared no inputs, or the run predates
    /// [`StepStatus::input_hashes`](crate::types::StepStatus::input_hashes).
    /// **Not** a synonym for fresh: a reader must render "no freshness evidence"
    /// rather than a green badge, or an unrecorded run would look verified.
    Unrecorded,
    /// Every recorded digest still matches the tree.
    Fresh,
    /// At least one declared input moved since the run. Carries the paths that
    /// changed, sorted, so a consumer can name them ("`yah-x86-worker.cfg`
    /// changed since this ISO was built") instead of saying only "stale".
    Stale { changed: Vec<String> },
}

impl InputFreshness {
    /// `true` unless something is known to have drifted. [`Unrecorded`] counts
    /// as current — absence of evidence is not evidence of staleness — but see
    /// its own docs before rendering it as a pass.
    ///
    /// [`Unrecorded`]: InputFreshness::Unrecorded
    pub fn is_current(&self) -> bool {
        !matches!(self, InputFreshness::Stale { .. })
    }
}

/// Compare the digests recorded when a step ran against the digests of the same
/// paths now. Pure — the caller does the reading (see [`hash_declared_inputs`]).
///
/// A path present in `recorded` but missing from `actual` is treated as changed,
/// not skipped: the caller is expected to hash exactly the recorded key set, so
/// a gap means the caller could not resolve a path it previously could, which is
/// drift. Extra keys in `actual` are ignored — the recorded set defines what
/// this run was about, and a step that grew a new `inputs =` entry after the
/// fact has simply not been re-run against it yet.
pub fn input_freshness(
    recorded: &BTreeMap<String, String>,
    actual: &BTreeMap<String, String>,
) -> InputFreshness {
    if recorded.is_empty() {
        return InputFreshness::Unrecorded;
    }
    let mut changed: Vec<String> = recorded
        .iter()
        .filter(|(path, pinned)| actual.get(*path).map(|a| a != *pinned).unwrap_or(true))
        .map(|(path, _)| path.clone())
        .collect();
    if changed.is_empty() {
        InputFreshness::Fresh
    } else {
        changed.sort();
        InputFreshness::Stale { changed }
    }
}

/// blake3 each declared input, resolved against `root`, into the map recorded on
/// [`StepStatus::input_hashes`](crate::types::StepStatus::input_hashes).
///
/// Keys are the paths **as declared** (not as resolved), so the record stays
/// portable across machines with different camp roots and a reader can re-resolve
/// them against its own root. An unreadable or missing path records
/// [`ABSENT_INPUT`]; this never fails, because a declared input going missing is
/// a fact to record about the run, not a reason to abort it — the step's own exit
/// code is the verdict on whether that mattered.
///
/// Digests the raw bytes rather than any parsed form, exactly as
/// [`crate::import::content_hash`] does and for the same reason: comment and
/// whitespace churn is a real edit, and "is this byte-for-byte what I pinned?"
/// should not require knowing the file's format.
pub fn hash_declared_inputs(root: &Path, declared: &[std::path::PathBuf]) -> BTreeMap<String, String> {
    declared
        .iter()
        .map(|rel| {
            let key = rel.display().to_string();
            let digest = match std::fs::read(root.join(rel)) {
                Ok(bytes) => crate::import::content_hash(&bytes),
                Err(_) => ABSENT_INPUT.to_string(),
            };
            (key, digest)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn empty_recorded_is_unrecorded_not_fresh() {
        // The distinction that keeps an un-instrumented run from rendering green.
        assert_eq!(
            input_freshness(&BTreeMap::new(), &map(&[("a", "deadbeef")])),
            InputFreshness::Unrecorded
        );
        assert!(InputFreshness::Unrecorded.is_current());
    }

    #[test]
    fn matching_digests_are_fresh() {
        let m = map(&[("a.cfg", "aaa"), ("b.sh", "bbb")]);
        assert_eq!(input_freshness(&m, &m), InputFreshness::Fresh);
    }

    #[test]
    fn drift_names_every_changed_path_sorted() {
        let recorded = map(&[("z.cfg", "zzz"), ("a.sh", "aaa"), ("m.txt", "mmm")]);
        let actual = map(&[("z.cfg", "CHANGED"), ("a.sh", "aaa"), ("m.txt", "ALSO")]);
        assert_eq!(
            input_freshness(&recorded, &actual),
            InputFreshness::Stale {
                changed: vec!["m.txt".to_string(), "z.cfg".to_string()],
            }
        );
        assert!(!input_freshness(&recorded, &actual).is_current());
    }

    #[test]
    fn a_path_the_reader_could_not_resolve_counts_as_drift() {
        let recorded = map(&[("gone.cfg", "aaa")]);
        assert_eq!(
            input_freshness(&recorded, &BTreeMap::new()),
            InputFreshness::Stale {
                changed: vec!["gone.cfg".to_string()],
            }
        );
    }

    #[test]
    fn extra_actual_keys_are_ignored() {
        // A step that grew a new `inputs =` entry hasn't been re-run against it;
        // that is not the same as the old inputs having drifted.
        let recorded = map(&[("a.sh", "aaa")]);
        let actual = map(&[("a.sh", "aaa"), ("new.cfg", "nnn")]);
        assert_eq!(input_freshness(&recorded, &actual), InputFreshness::Fresh);
    }

    #[test]
    fn hashing_records_declared_keys_and_detects_a_one_byte_edit() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(root.join("nested/a.cfg"), b"boot = true\n").unwrap();

        let declared = vec![PathBuf::from("nested/a.cfg")];
        let first = hash_declared_inputs(root, &declared);
        // Key is the DECLARED path, not the absolute resolved one.
        assert_eq!(first.keys().collect::<Vec<_>>(), vec!["nested/a.cfg"]);
        assert_eq!(first["nested/a.cfg"].len(), 64, "blake3 hex");
        assert_eq!(input_freshness(&first, &first), InputFreshness::Fresh);

        std::fs::write(root.join("nested/a.cfg"), b"boot = false\n").unwrap();
        let second = hash_declared_inputs(root, &declared);
        assert_eq!(
            input_freshness(&first, &second),
            InputFreshness::Stale {
                changed: vec!["nested/a.cfg".to_string()],
            },
            "the W257 stale-ISO relation: edit the .cfg, the build cell goes stale"
        );
    }

    #[test]
    fn a_missing_input_records_the_sentinel_and_appearing_later_is_drift() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let declared = vec![PathBuf::from("not-there.sh")];

        let recorded = hash_declared_inputs(root, &declared);
        assert_eq!(recorded["not-there.sh"], ABSENT_INPUT);
        // Recorded, not omitted — so it is still a comparable fact, not a gap.
        assert_eq!(input_freshness(&recorded, &recorded), InputFreshness::Fresh);

        std::fs::write(root.join("not-there.sh"), b"#!/bin/sh\n").unwrap();
        assert_eq!(
            input_freshness(&recorded, &hash_declared_inputs(root, &declared)),
            InputFreshness::Stale {
                changed: vec!["not-there.sh".to_string()],
            }
        );
    }

    #[test]
    fn the_sentinel_can_never_collide_with_a_digest() {
        assert_ne!(ABSENT_INPUT.len(), 64);
    }
}
