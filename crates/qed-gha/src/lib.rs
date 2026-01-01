//! GitHub Actions YAML runtime — F1 scope.
//!
//! Public surface today is a parser only. [`parse_workflow`] takes the raw
//! YAML text of a workflow file (e.g. `.github/workflows/release.yml`) and
//! returns a typed [`Workflow`] tree. Subsequent phases (F2 expressions, F3
//! graph/matrix, F4+ step execution + override registry) extend the runtime
//! end-to-end; F1 only proves the audit-target workflows parse without loss.

mod events;
mod expr;
mod expr_str;
mod graph;
mod overrides;
mod overrides_builtin;
mod parse;
mod runtime;
mod workflow;

pub use events::{GhaEvent, GhaEventSink, GhaOutputStream};

pub use expr::{
    eval, evaluate, obj, parse as parse_expr, BinOp, Context, Expr, ExprError, JobStatus, Value,
};
pub use expr_str::{ExprString, ExprToken};
pub use graph::{
    build_context_for_instance, build_needs_value, eval_exprstring, evaluate_outputs,
    expand_matrix, plan, should_run_job, topo_sort, CompletedInstance, GraphError, JobInstance,
    JobResult, Plan,
};
pub use overrides::{
    default_overlay_paths, Lookup, Override, OverrideCall, OverrideOutcome, OverrideRegistry,
    ProducedArtifact, RegistryError, StepConclusion,
};
pub use overrides_builtin::register_builtins;
pub use parse::{parse_workflow, ParseError};
pub use runtime::{execute_workflow, Executor, InstanceRun, RuntimeError, StepResult, WorkflowRun};
pub use workflow::*;

#[cfg(test)]
mod roundtrip_tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn workflows_dir() -> PathBuf {
        // CARGO_MANIFEST_DIR = crates/yah/qed-gha → repo root is three levels up.
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        manifest
            .parent().unwrap()
            .parent().unwrap()
            .parent().unwrap()
            .join(".github")
            .join("workflows")
    }

    fn parse_file(name: &str) -> Workflow {
        let path = workflows_dir().join(name);
        let src = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        parse_workflow(&src)
            .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
    }

    #[test]
    fn release_yml_parses() {
        let wf = parse_file("release.yml");
        assert_eq!(wf.name.as_deref(), Some("release"));
        assert!(wf.triggers.push.as_ref().map(|p| !p.tags.is_empty()).unwrap_or(false),
                "release.yml should have push.tags");
        assert!(wf.triggers.workflow_dispatch.is_some());
        assert!(wf.jobs.contains_key("smoke"));
        // Sanity: at least one matrix job and one image-fan-out job.
        let smoke = &wf.jobs["smoke"];
        assert!(!smoke.steps.is_empty());
        // image-yah-base has outputs.digest backed by ${{ steps.build.outputs.digest }}.
        let img = wf.jobs.get("image-yah-base").expect("image-yah-base job");
        let digest = img.outputs.get("digest").expect("digest output");
        assert!(!digest.is_pure_literal(), "outputs.digest should be an expression");
    }

    #[test]
    fn ci_yml_parses() {
        let wf = parse_file("ci.yml");
        assert!(wf.jobs.contains_key("workspace"));
        let workspace = &wf.jobs["workspace"];
        let strategy = workspace.strategy.as_ref().expect("workspace strategy");
        let matrix = strategy.matrix.as_ref().expect("workspace matrix");
        let os = matrix.dimensions.get("os").expect("os dimension");
        assert_eq!(os.len(), 2, "ubuntu-latest + macos-latest");
        match &workspace.runs_on {
            RunsOn::Label(s) => {
                assert!(!s.is_pure_literal(), "runs-on uses ${{{{ matrix.os }}}}");
            }
            RunsOn::Group(_) => panic!("runs-on is a single expression"),
        }
    }

    #[test]
    fn smoke_yml_parses() {
        let wf = parse_file("smoke.yml");
        assert_eq!(wf.name.as_deref(), Some("warden-smoke"));
        assert!(wf.concurrency.is_some());
        assert!(!wf.env.is_empty());
        let job = wf.jobs.values().next().expect("at least one job");
        assert_eq!(job.timeout_minutes, Some(30));
    }

    #[test]
    fn smoke_sweeper_yml_parses() {
        let wf = parse_file("smoke-sweeper.yml");
        assert_eq!(wf.name.as_deref(), Some("warden-smoke-sweeper"));
        assert_eq!(wf.triggers.schedule.len(), 1);
        assert_eq!(wf.triggers.schedule[0].cron, "0 */12 * * *");
        assert!(wf.triggers.workflow_dispatch.is_some());
    }

    #[test]
    fn step_uses_splits_slug_and_ref() {
        let wf = parse_file("release.yml");
        // The first step in `smoke` is `actions/checkout@v4`.
        let smoke = &wf.jobs["smoke"];
        let first = smoke.steps.first().expect("smoke has steps");
        match &first.action {
            StepAction::Uses { slug, git_ref, .. } => {
                assert_eq!(slug, "actions/checkout");
                assert_eq!(git_ref.as_deref(), Some("v4"));
            }
            StepAction::Run { .. } => panic!("first smoke step should be a uses"),
        }
    }

    #[test]
    fn step_with_inputs_carry_expressions() {
        let wf = parse_file("release.yml");
        let img = &wf.jobs["image-yah-base"];
        // Find the docker/login-action step.
        let login = img.steps.iter().find(|s| match &s.action {
            StepAction::Uses { slug, .. } => slug == "docker/login-action",
            _ => false,
        }).expect("docker/login-action step");
        match &login.action {
            StepAction::Uses { with, .. } => {
                let username = with.get("username").expect("username input");
                // ${{ github.actor }} — should be a single Expr token.
                assert!(!username.is_pure_literal());
                let registry = with.get("registry").expect("registry input");
                assert_eq!(registry.as_pure_literal().as_deref(), Some("ghcr.io"));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn release_yml_plan_orders_waves() {
        // F3 verify: dry-run `release.yml` produces the expected wave order.
        let wf = parse_file("release.yml");
        let plan = crate::plan(&wf).expect("plan succeeds");

        // Wave 0 is the seed: smoke (no needs).
        let w0: Vec<_> = plan.waves[0].iter().map(|i| i.job_id.clone()).collect();
        assert_eq!(w0, vec!["smoke".to_string()], "smoke is wave 0");

        // image-yah-base / image-yah-rust / etc. follow smoke; image-yah-rust-bun
        // sits behind image-yah-rust per its needs list.
        let job_to_wave: std::collections::HashMap<String, usize> = plan
            .waves
            .iter()
            .enumerate()
            .flat_map(|(w, row)| row.iter().map(move |i| (i.job_id.clone(), w)))
            .collect();
        let base_wave = job_to_wave["image-yah-base"];
        let rust_wave = job_to_wave["image-yah-rust"];
        let bun_wave = job_to_wave["image-yah-rust-bun"];
        assert_eq!(base_wave, 1);
        assert_eq!(rust_wave, 1);
        assert!(bun_wave > rust_wave, "image-yah-rust-bun depends on image-yah-rust");

        // cli-release expands its include-only matrix into per-row instances.
        let cli_instances: Vec<_> = plan
            .iter_instances()
            .filter(|i| i.job_id == "cli-release")
            .collect();
        assert_eq!(cli_instances.len(), 3, "cli-release has 3 matrix rows");
        assert!(cli_instances.iter().all(|i| i.matrix.is_some()));
    }

    #[test]
    fn run_body_preserves_multiline_script() {
        let wf = parse_file("smoke-sweeper.yml");
        let sweep = wf.jobs.values().next().unwrap();
        let run_step = sweep.steps.iter().find(|s| matches!(s.action, StepAction::Run { .. }))
            .expect("at least one run step");
        match &run_step.action {
            StepAction::Run { body, shell } => {
                assert_eq!(shell.as_deref(), Some("bash"));
                let literal = body.as_pure_literal().expect("script body is pure literal");
                assert!(literal.contains("HETZNER_API_TOKEN"));
                assert!(literal.contains("Sweep complete"));
            }
            _ => unreachable!(),
        }
    }
}
