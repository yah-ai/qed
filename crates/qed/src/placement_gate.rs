//! @yah:ticket(R435-F2, "Runner gates kicks on placement: CLI refuses ci-only without --force; GHA warns/refuses local-only")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-04T19:15:56Z)
//! @yah:status(in-progress)
//! @yah:phase(P1)
//! @yah:parent(R435)
//! @yah:depends_on(R435-F1)
//! @arch:see(.yah/docs/working/W170-qed-recipe-discipline.md)
//!
//! Single source of truth for the W155 environment × runner matrix. Both the
//! CLI entry (`yah qed run`) and the camp daemon `qed.run` handler consult
//! [`evaluate`] before kicking a pipeline, so adding a new runner class or
//! flipping a matrix cell only requires editing this file.
//!
//! The matrix (W155 §GHA-compat decision matrix):
//!
//! ```text
//!                | workstation | any   | ci
//!  Local runner  | Allow       | Allow | Refuse (unless --force)
//!  CI runner     | Warn        | Allow | Allow
//! ```
//!
//! R555-T7 renamed the recipe key this reads from `placement` to `environment`
//! (W235 §Verdict): the axis is PERMISSION — *may this recipe run on this class
//! of host* — and never routing. The module keeps its filename so the W235 §3b
//! follow-up (teaching the gate the `leaves_box` question) lands where canon
//! says it will.

use crate::types::Environment;

/// Classification of the host kicking the pipeline. The CLI detects this
/// from `$CI` / `$GITHUB_ACTIONS`; the camp daemon inherits its env from
/// whatever launched it (typically a dev laptop ⇒ [`RunnerEnv::Local`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerEnv {
    /// Developer machine, desktop daemon, or anywhere `$CI` / `$GITHUB_ACTIONS`
    /// are unset.
    Local,
    /// CI host — `$CI=true` or `$GITHUB_ACTIONS=true`.
    Ci,
}

impl RunnerEnv {
    /// Classify from environment variables. Mirrors GitHub Actions' own
    /// convention plus the generic `$CI` flag set by most CI providers.
    pub fn detect() -> Self {
        if env_truthy("GITHUB_ACTIONS") || env_truthy("CI") {
            RunnerEnv::Ci
        } else {
            RunnerEnv::Local
        }
    }
}

fn env_truthy(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("True"),
    )
}

/// Outcome of an environment check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateOutcome {
    /// Allow the kick. The optional warning is informational — the caller
    /// should print it (stderr for CLI; log for daemon) but proceed.
    Allow { warning: Option<String> },
    /// Refuse the kick. The message is the operator-facing reason.
    Refuse { reason: String },
}

/// Evaluate the W155 environment × runner matrix.
///
/// `force` corresponds to the CLI's `--force` flag and only matters for the
/// one cell the matrix marks as "Refuse (unless --force)" — `ci` on a
/// `Local` runner. When `force = true` that cell flips to [`GateOutcome::Allow`]
/// with a warning so the bypass is loud.
pub fn evaluate(environment: Environment, env: RunnerEnv, force: bool) -> GateOutcome {
    match (environment, env) {
        // Allow: same-class matches and the universal `any`.
        (Environment::Workstation, RunnerEnv::Local)
        | (Environment::Any, _)
        | (Environment::Ci, RunnerEnv::Ci) => GateOutcome::Allow { warning: None },

        // Warn-but-allow: `workstation` on CI is almost certainly a mistake
        // (the artifact lives on the host that ran it, which CI throws away),
        // but refusing would silently fail any GHA workflow that drifts onto
        // a workstation recipe — better to surface the smell loudly.
        (Environment::Workstation, RunnerEnv::Ci) => GateOutcome::Allow {
            warning: Some(
                "recipe environment = workstation but runner is CI — the run's output \
                 (installed binary, files in the camp tree, …) is meaningless on a \
                 CI runner. Consider splitting the recipe or flipping environment to \
                 `any`."
                    .to_string(),
            ),
        },

        // Refuse: `ci` on a Local runner needs secrets / signing identity
        // / a clean machine. `--force` exists to support local rehearsals.
        (Environment::Ci, RunnerEnv::Local) => {
            if force {
                GateOutcome::Allow {
                    warning: Some(
                        "recipe environment = ci but --force was passed; running \
                         locally. Steps that depend on CI secrets or signing identity \
                         will fail unless your environment already provides them."
                            .to_string(),
                    ),
                }
            } else {
                GateOutcome::Refuse {
                    reason: "recipe environment = ci and this is not a CI runner — \
                         the recipe needs secrets, signing identity, or a clean \
                         runner that don't exist locally. Pass --force to run \
                         anyway, or run it from a CI workflow."
                        .to_string(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_allowed(p: Environment, e: RunnerEnv, force: bool) {
        match evaluate(p, e, force) {
            GateOutcome::Allow { .. } => {}
            other => panic!("expected Allow for ({p:?}, {e:?}, force={force}); got {other:?}"),
        }
    }

    fn assert_allowed_with_warning(p: Environment, e: RunnerEnv, force: bool) {
        match evaluate(p, e, force) {
            GateOutcome::Allow { warning: Some(_) } => {}
            other => panic!(
                "expected Allow {{warning: Some(_)}} for ({p:?}, {e:?}, force={force}); got {other:?}"
            ),
        }
    }

    fn assert_refused(p: Environment, e: RunnerEnv, force: bool) {
        match evaluate(p, e, force) {
            GateOutcome::Refuse { .. } => {}
            other => panic!("expected Refuse for ({p:?}, {e:?}, force={force}); got {other:?}"),
        }
    }

    // ── W155 matrix: each of the 6 cells, plus the --force escape hatch ────

    #[test]
    fn matrix_workstation_on_local_allows_silently() {
        assert_allowed(Environment::Workstation, RunnerEnv::Local, false);
    }

    #[test]
    fn matrix_any_on_local_allows_silently() {
        assert_allowed(Environment::Any, RunnerEnv::Local, false);
    }

    #[test]
    fn matrix_any_on_ci_allows_silently() {
        assert_allowed(Environment::Any, RunnerEnv::Ci, false);
    }

    #[test]
    fn matrix_ci_on_ci_allows_silently() {
        assert_allowed(Environment::Ci, RunnerEnv::Ci, false);
    }

    #[test]
    fn matrix_workstation_on_ci_warns_but_allows() {
        assert_allowed_with_warning(Environment::Workstation, RunnerEnv::Ci, false);
    }

    #[test]
    fn matrix_ci_on_local_refuses_without_force() {
        assert_refused(Environment::Ci, RunnerEnv::Local, false);
    }

    #[test]
    fn matrix_ci_on_local_with_force_allows_with_warning() {
        assert_allowed_with_warning(Environment::Ci, RunnerEnv::Local, true);
    }

    // ── force is a no-op on cells that don't refuse ────────────────────────

    #[test]
    fn force_is_no_op_on_already_allowed_cells() {
        for p in [Environment::Workstation, Environment::Any, Environment::Ci] {
            for e in [RunnerEnv::Local, RunnerEnv::Ci] {
                let without = evaluate(p, e, false);
                if matches!(without, GateOutcome::Allow { warning: None }) {
                    let with_force = evaluate(p, e, true);
                    assert_eq!(
                        with_force, without,
                        "force should not change behavior for ({p:?}, {e:?})"
                    );
                }
            }
        }
    }

    // ── env detection: smoke around env_truthy ─────────────────────────────

    #[test]
    fn env_truthy_recognizes_canonical_values() {
        // Don't mutate process env (other tests may race) — exercise the
        // pure helper directly via temp_env-style ScopedEnv would be nicer
        // but adds a dep. Test the matrix routing instead; detect() coverage
        // is integration-tested via the manual `yah qed run` verify step.
        for val in ["1", "true", "TRUE", "True"] {
            unsafe { std::env::set_var("__QED_GATE_TEST", val) };
            assert!(env_truthy("__QED_GATE_TEST"), "truthy: {val}");
        }
        for val in ["0", "false", "no", ""] {
            unsafe { std::env::set_var("__QED_GATE_TEST", val) };
            assert!(!env_truthy("__QED_GATE_TEST"), "not truthy: {val}");
        }
        unsafe { std::env::remove_var("__QED_GATE_TEST") };
    }
}
