//! Step dependency graph for a native pipeline (R605-F3).
//!
//! Until this module existed a [`Pipeline`](crate::types::Pipeline) was a flat
//! `Vec<QedStep>` the runner walked in declaration order, and that order *was*
//! the dependency model: there was nowhere to write "these two builds are
//! independent" and nothing that would have acted on it. A GHA import
//! therefore had to linearize the job DAG and throw the edges away
//! ([`crate::transform`]), and every ejected pipeline was an honest lie — the
//! ordering was real, the *reason* for it was not.
//!
//! # The model
//!
//! [`QedStep::needs`](crate::types::QedStep::needs) is a three-state field, and
//! the three states are the whole design:
//!
//! | TOML | Meaning |
//! |---|---|
//! | key absent (`None`) | **implicit chain** — depends on the immediately preceding step |
//! | `needs = []` | **root** — depends on nothing; may start immediately |
//! | `needs = ["a", "b"]` | depends on exactly `a` and `b` |
//!
//! The absent case is what makes this backwards-compatible rather than a
//! flag day. Every pipeline TOML written before this field existed omits it on
//! every step, so every such pipeline resolves to the chain `0 → 1 → 2 → …` —
//! one step ready at a time, the identical serial execution and the identical
//! event stream it had before. Reading an absent `needs` as "no dependencies"
//! would instead have made every existing pipeline fully parallel overnight,
//! which is not a migration, it's an outage.
//!
//! `needs = []` has to be spellable separately because a *root* is not the same
//! statement as "I didn't say". The first step of every independent branch in
//! an imported workflow is a root, and there is more than one of them.
//!
//! # Matrix fan-out
//!
//! [`crate::matrix::plan`] expands a step carrying `[matrix]` into N instances
//! renamed `"<name> [k=v …]"`. A `needs` entry therefore matches a step whose
//! name is *either* the entry verbatim or the entry followed by ` [` — so
//! `needs = ["build"]` joins on every row of a fanned-out `build`, which is
//! what GHA's `needs:` means for a matrix job too. See [`name_matches`].
//!
//! # What this module does not decide
//!
//! Scheduling. This is a pure graph over a step slice: predecessors, waves,
//! reachability, and the errors an author can make. The runner owns readiness,
//! the concurrency cap and the shared-resource gate; [`crate::eject`] and the
//! loader's `validate` own their own use of the same graph.

use crate::types::QedStep;
use std::collections::{BTreeSet, HashSet};

/// Default ceiling on steps executing at once within one run when the pipeline
/// declares no [`max_parallel`](crate::types::Pipeline::max_parallel).
///
/// Deliberately small, and deliberately not `num_cpus`. QED step concurrency is
/// not CPU fan-out: the steps share one host, one cargo `target/`, one docker
/// daemon and one network. Four independent `cargo build`s on one target dir
/// spend their time in cargo's own file lock, and the operator sees a run that
/// got *slower* with a scheduler in it. Steps that genuinely contend should say
/// so with [`QedStep::resource`](crate::types::QedStep::resource); this cap is
/// the coarse backstop for the ones that forgot.
pub const DEFAULT_MAX_PARALLEL: usize = 4;

/// An authoring error in a pipeline's `needs` graph.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DagError {
    #[error(
        "step `{step}`: `needs` names unknown step `{missing}` — \
         it must match another step's `name` in this pipeline (a matrix step \
         is matched by its un-suffixed name)"
    )]
    UnknownNeed { step: String, missing: String },
    #[error("step `{0}`: `needs` names the step itself")]
    SelfDependency(String),
    #[error(
        "steps `{0}` form a dependency cycle — no step in the cycle can ever \
         become ready"
    )]
    Cycle(String),
    #[error(
        "step name `{0}` is used by more than one step and is referenced by a \
         `needs` — rename one, or the edge is ambiguous"
    )]
    AmbiguousName(String),
}

/// What [`predecessors`] does with a `needs` entry that names no step in the
/// slice it was handed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Missing {
    /// [`DagError::UnknownNeed`]. The loader's `validate` uses this: against a
    /// whole pipeline an unresolvable name is a typo, and a typo that silently
    /// drops an edge is worse than one that fails the load.
    Reject,
    /// Treat the edge as already satisfied and drop it. The **runner** uses
    /// this, because a resume-from-step run hands it a pipeline whose leading
    /// steps were `drain`ed (`with_index_offset`): a surviving `needs` pointing
    /// into the drained prefix names a step that genuinely already ran.
    Satisfied,
}

/// Does `step_name` satisfy a `needs` entry of `need`?
///
/// Exact match, or the matrix fan-out shape `"<need> [k=v …]"` that
/// [`crate::matrix::plan`] produces. The space before `[` is load-bearing: it
/// keeps `needs = ["build"]` from matching an unrelated step named
/// `build[legacy]`.
pub fn name_matches(step_name: &str, need: &str) -> bool {
    step_name == need
        || step_name
            .strip_prefix(need)
            .is_some_and(|rest| rest.starts_with(" ["))
}

/// Resolve each step's predecessor indices.
///
/// Returns one entry per step, in declaration order; each is the sorted,
/// deduped set of indices that must complete before it may start. Applies the
/// three-state rule from the module docs — absent `needs` yields the implicit
/// chain edge, `Some([])` yields no edges.
///
/// A `needs` may name a *later* step; that is not rejected here (it produces a
/// back edge and [`waves`] reports it as a cycle only if it actually closes
/// one). Forward references are how a diamond written bottom-up still works.
pub fn predecessors(steps: &[QedStep], missing: Missing) -> Result<Vec<Vec<usize>>, DagError> {
    let mut out: Vec<Vec<usize>> = Vec::with_capacity(steps.len());
    for (i, step) in steps.iter().enumerate() {
        let Some(needs) = step.needs.as_ref() else {
            // Implicit chain: the previous step, if any.
            out.push(if i == 0 { Vec::new() } else { vec![i - 1] });
            continue;
        };
        let mut preds: BTreeSet<usize> = BTreeSet::new();
        for need in needs {
            let matched: Vec<usize> = steps
                .iter()
                .enumerate()
                .filter(|(_, s)| name_matches(&s.name, need))
                .map(|(j, _)| j)
                .collect();
            if matched.contains(&i) {
                return Err(DagError::SelfDependency(step.name.clone()));
            }
            // More than one match is only ambiguous when the matches are
            // *identically named*; a matrix fan-out is many matches on purpose.
            if matched.len() > 1 && matched.iter().filter(|&&j| steps[j].name == *need).count() > 1
            {
                return Err(DagError::AmbiguousName(need.clone()));
            }
            if matched.is_empty() {
                match missing {
                    Missing::Reject => {
                        return Err(DagError::UnknownNeed {
                            step: step.name.clone(),
                            missing: need.clone(),
                        })
                    }
                    Missing::Satisfied => continue,
                }
            }
            preds.extend(matched);
        }
        out.push(preds.into_iter().collect());
    }
    Ok(out)
}

/// Group the steps into dependency waves: every index in wave `n` depends only
/// on indices in waves `< n`, and within a wave indices are ascending so a
/// deterministic executor still walks them in declaration order.
///
/// Kahn's algorithm — so a graph that never drains is a cycle, and the error
/// names the steps still in it.
pub fn waves(steps: &[QedStep], missing: Missing) -> Result<Vec<Vec<usize>>, DagError> {
    let preds = predecessors(steps, missing)?;
    let mut done: Vec<bool> = vec![false; steps.len()];
    let mut remaining = steps.len();
    let mut out: Vec<Vec<usize>> = Vec::new();
    while remaining > 0 {
        let wave: Vec<usize> = (0..steps.len())
            .filter(|&i| !done[i] && preds[i].iter().all(|&p| done[p]))
            .collect();
        if wave.is_empty() {
            let stuck: Vec<&str> = (0..steps.len())
                .filter(|&i| !done[i])
                .map(|i| steps[i].name.as_str())
                .collect();
            return Err(DagError::Cycle(stuck.join(", ")));
        }
        for &i in &wave {
            done[i] = true;
        }
        remaining -= wave.len();
        out.push(wave);
    }
    Ok(out)
}

/// `true` when any step declares `needs` explicitly — i.e. this pipeline is a
/// DAG rather than the implicit chain every pre-R605-F3 pipeline is.
///
/// Used to scope the stricter checks (`background_until` must name a genuine
/// descendant) to pipelines that opted in: on a chain the strict rule and the
/// loose one coincide, so applying it there could only break something that was
/// already fine.
pub fn is_explicit(steps: &[QedStep]) -> bool {
    steps.iter().any(|s| s.needs.is_some())
}

/// Every index transitively reachable *from* `root` by following edges forward
/// (i.e. the steps that depend on `root`, directly or through others).
/// `root` itself is not included.
pub fn dependents(preds: &[Vec<usize>], root: usize) -> HashSet<usize> {
    let mut seen: HashSet<usize> = HashSet::new();
    let mut frontier = vec![root];
    while let Some(cur) = frontier.pop() {
        for (i, p) in preds.iter().enumerate() {
            if p.contains(&cur) && seen.insert(i) {
                frontier.push(i);
            }
        }
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A step with a name and optional needs; everything else default.
    fn s(name: &str, needs: Option<&[&str]>) -> QedStep {
        QedStep {
            name: name.to_string(),
            argv: vec!["true".into()],
            needs: needs.map(|n| n.iter().map(|x| x.to_string()).collect()),
            ..Default::default()
        }
    }

    #[test]
    fn absent_needs_is_the_implicit_serial_chain() {
        let steps = vec![s("a", None), s("b", None), s("c", None)];
        let preds = predecessors(&steps, Missing::Reject).unwrap();
        assert_eq!(preds, vec![vec![], vec![0], vec![1]]);
        // …and therefore one singleton wave per step: byte-identical serial
        // execution for every pipeline written before `needs` existed.
        assert_eq!(
            waves(&steps, Missing::Reject).unwrap(),
            vec![vec![0], vec![1], vec![2]]
        );
    }

    #[test]
    fn empty_needs_is_a_root_not_an_absent_needs() {
        // Two roots + a join: the shape the whole feature exists for.
        let steps = vec![
            s("setup", Some(&[])),
            s("left", Some(&["setup"])),
            s("right", Some(&["setup"])),
            s("join", Some(&["left", "right"])),
        ];
        assert_eq!(
            waves(&steps, Missing::Reject).unwrap(),
            vec![vec![0], vec![1, 2], vec![3]]
        );
    }

    #[test]
    fn two_independent_roots_share_the_first_wave() {
        let steps = vec![s("a", Some(&[])), s("b", Some(&[]))];
        assert_eq!(waves(&steps, Missing::Reject).unwrap(), vec![vec![0, 1]]);
    }

    #[test]
    fn a_step_after_an_explicit_one_still_chains_implicitly() {
        // `tail` says nothing, so it inherits the chain edge to its immediate
        // predecessor — not to everything before it, and not to nothing.
        let steps = vec![s("a", Some(&[])), s("b", Some(&["a"])), s("tail", None)];
        let preds = predecessors(&steps, Missing::Reject).unwrap();
        assert_eq!(preds[2], vec![1]);
    }

    #[test]
    fn needs_matches_every_matrix_row_of_the_named_step() {
        let steps = vec![
            s("build [target=x86]", Some(&[])),
            s("build [target=arm]", Some(&[])),
            s("join", Some(&["build"])),
        ];
        let preds = predecessors(&steps, Missing::Reject).unwrap();
        assert_eq!(preds[2], vec![0, 1], "the join waits for every row");
    }

    #[test]
    fn matrix_prefix_match_requires_the_space_bracket() {
        let steps = vec![s("build-extra", Some(&[])), s("join", Some(&["build"]))];
        let err = predecessors(&steps, Missing::Reject).unwrap_err();
        assert_eq!(
            err,
            DagError::UnknownNeed { step: "join".into(), missing: "build".into() }
        );
    }

    #[test]
    fn unknown_need_is_rejected_or_dropped_by_policy() {
        // Resume-from-step drains the prefix, so the surviving step's `needs`
        // names a step that already ran — satisfied, not missing.
        let steps = vec![s("publish", Some(&["build"]))];
        assert!(matches!(
            predecessors(&steps, Missing::Reject),
            Err(DagError::UnknownNeed { .. })
        ));
        assert_eq!(
            predecessors(&steps, Missing::Satisfied).unwrap(),
            vec![Vec::<usize>::new()]
        );
    }

    #[test]
    fn self_dependency_is_rejected() {
        let steps = vec![s("a", Some(&["a"]))];
        assert_eq!(
            predecessors(&steps, Missing::Reject).unwrap_err(),
            DagError::SelfDependency("a".into())
        );
    }

    #[test]
    fn a_cycle_names_the_steps_stuck_in_it() {
        let steps = vec![s("a", Some(&["b"])), s("b", Some(&["a"]))];
        let err = waves(&steps, Missing::Reject).unwrap_err();
        assert_eq!(err, DagError::Cycle("a, b".into()));
    }

    #[test]
    fn duplicate_names_are_ambiguous_only_when_referenced() {
        let dup = vec![s("x", None), s("x", None), s("y", None)];
        // Nobody references `x`, so the chain resolves fine.
        assert!(predecessors(&dup, Missing::Reject).is_ok());

        let referenced = vec![s("x", Some(&[])), s("x", Some(&[])), s("y", Some(&["x"]))];
        assert_eq!(
            predecessors(&referenced, Missing::Reject).unwrap_err(),
            DagError::AmbiguousName("x".into())
        );
    }

    #[test]
    fn forward_reference_is_allowed_when_it_closes_no_cycle() {
        // `first` declared before the step it needs — legal, and the waves put
        // them in dependency order regardless of declaration order.
        let steps = vec![s("first", Some(&["second"])), s("second", Some(&[]))];
        assert_eq!(waves(&steps, Missing::Reject).unwrap(), vec![vec![1], vec![0]]);
    }

    #[test]
    fn dependents_is_transitive_and_excludes_the_root() {
        let steps = vec![
            s("a", Some(&[])),
            s("b", Some(&["a"])),
            s("c", Some(&["b"])),
            s("island", Some(&[])),
        ];
        let preds = predecessors(&steps, Missing::Reject).unwrap();
        let d = dependents(&preds, 0);
        assert_eq!(d, HashSet::from([1, 2]));
        assert!(dependents(&preds, 3).is_empty());
    }

    #[test]
    fn is_explicit_distinguishes_a_dag_from_the_legacy_chain() {
        assert!(!is_explicit(&[s("a", None), s("b", None)]));
        assert!(is_explicit(&[s("a", None), s("b", Some(&[]))]));
    }
}
