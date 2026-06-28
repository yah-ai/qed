//! GitHub Actions YAML **import front-end** + tier-1/2 toolkit executor.
//!
//! W224 (R533) recasts this crate from W200's "native GHA runtime + unbounded
//! override registry" into the QED↔GHA *import* boundary: QED imports a
//! workflow, it does not faithfully emulate GitHub. The surface here is:
//! - the **parser / expression engine / graph+matrix** ([`parse_workflow`],
//!   [`eval`], [`plan`]) — you must parse the YAML to transform it;
//! - the **tier classifier** ([`classify_workflow`]) — the tier-3 boundary
//!   catalog naming the native QED replacement for each GitHub-service action;
//! - the **tier-1/2 toolkit-contract executor** ([`ToolkitRegistry`],
//!   [`Executor`]) — runs *compute* actions (`rust-toolchain`, `setup-bun`,
//!   buildx/qemu setup, `cosign-installer`) and bash `run:` steps; a tier-3
//!   `uses:` errors with a native-replacement hint instead of being imitated.
//!
//! The tier-3 *service* override impls W200 shipped (`checkout`, `cache`,
//! `upload-artifact`, `gh-release`, the docker push family) are **retired** —
//! QED replaces those with native facilities at import time.

mod events;
mod expr;
mod expr_str;
mod graph;
mod parse;
mod runtime;
mod schema;
mod tier;
mod toolkit;
mod toolkit_builtin;
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
pub use parse::{parse_workflow, ParseError};
pub use runtime::{execute_workflow, Executor, InstanceRun, RuntimeError, StepResult, WorkflowRun};
pub use schema::{
    schema_blake3, schema_bytes, schema_json, SCHEMA_BLAKE3, SCHEMA_LICENSE, SCHEMA_SOURCE_URL,
    SCHEMA_VENDORED_AT,
};
pub use tier::{
    classify_step, classify_uses, classify_workflow, ClassifiedStep, Disposition,
    NativeReplacement, ServiceTouch, StepClass, Tier,
};
pub use toolkit::{Lookup, StepConclusion, ToolkitAction, ToolkitCall, ToolkitOutcome, ToolkitRegistry};
pub use toolkit_builtin::register_toolkit;
pub use workflow::*;

#[cfg(test)]
mod roundtrip_tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    /// Locate the `.github/workflows` dir that owns yah's `release.yml`.
    ///
    /// These tests parse yah's *live* workflow files, which live at the yah
    /// monorepo root. In-tree this crate is nested at `oss/qed/crates/qed-gha`,
    /// so a fixed parent-hop count is wrong (it was written for the old
    /// `crates/yah/qed-gha` location); ascend until the marker is found. When
    /// this crate is consumed as the standalone github.com/yah-ai/qed export
    /// mirror there are no yah workflows, so the marker is absent and the
    /// fixture-dependent tests skip instead of failing.
    fn workflows_dir() -> Option<PathBuf> {
        let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        loop {
            let candidate = dir.join(".github").join("workflows");
            if candidate.join("release.yml").is_file() {
                return Some(candidate);
            }
            if !dir.pop() {
                return None;
            }
        }
    }

    /// Returns `None` when the yah workflow fixtures aren't present (standalone
    /// export); a parse error of a *present* file still panics.
    fn parse_file(name: &str) -> Option<Workflow> {
        let path = workflows_dir()?.join(name);
        let src = fs::read_to_string(&path).ok()?;
        Some(parse_workflow(&src).unwrap_or_else(|e| panic!("parse {}: {e}", path.display())))
    }

    #[test]
    fn release_yml_parses() {
        let Some(wf) = parse_file("release.yml") else { eprintln!("skip: yah workflow fixtures not present"); return; };
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
        let Some(wf) = parse_file("ci.yml") else { eprintln!("skip: yah workflow fixtures not present"); return; };
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
        let Some(wf) = parse_file("smoke.yml") else { eprintln!("skip: yah workflow fixtures not present"); return; };
        assert_eq!(wf.name.as_deref(), Some("yubaba-smoke"));
        assert!(wf.concurrency.is_some());
        assert!(!wf.env.is_empty());
        let job = wf.jobs.values().next().expect("at least one job");
        assert_eq!(job.timeout_minutes, Some(30));
    }

    #[test]
    fn smoke_sweeper_yml_parses() {
        let Some(wf) = parse_file("smoke-sweeper.yml") else { eprintln!("skip: yah workflow fixtures not present"); return; };
        assert_eq!(wf.name.as_deref(), Some("yubaba-smoke-sweeper"));
        assert_eq!(wf.triggers.schedule.len(), 1);
        assert_eq!(wf.triggers.schedule[0].cron, "0 */12 * * *");
        assert!(wf.triggers.workflow_dispatch.is_some());
    }

    #[test]
    fn step_uses_splits_slug_and_ref() {
        let Some(wf) = parse_file("release.yml") else { eprintln!("skip: yah workflow fixtures not present"); return; };
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
        let Some(wf) = parse_file("release.yml") else { eprintln!("skip: yah workflow fixtures not present"); return; };
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
        // F3 verify: dry-run `release.yml` produces a wave order consistent with
        // the build → smoke → publish three-layer shape (R330-F24). Assert the
        // *relative* invariants that the topology guarantees rather than
        // absolute wave numbers, so adding an image job doesn't churn this test.
        let Some(wf) = parse_file("release.yml") else { eprintln!("skip: yah workflow fixtures not present"); return; };
        let plan = crate::plan(&wf).expect("plan succeeds");

        let job_to_wave: std::collections::HashMap<String, usize> = plan
            .waves
            .iter()
            .enumerate()
            .flat_map(|(w, row)| row.iter().map(move |i| (i.job_id.clone(), w)))
            .collect();
        let wave = |id: &str| *job_to_wave.get(id).unwrap_or_else(|| panic!("job `{id}` in plan"));

        // image-yah-base / -rust have no needs → seed wave; image-yah-rust-bun
        // sits behind image-yah-rust per its FROM-chain `needs`.
        assert_eq!(wave("image-yah-base"), 0, "image-yah-base seeds wave 0");
        assert_eq!(wave("image-yah-rust"), 0, "image-yah-rust seeds wave 0");
        assert!(
            wave("image-yah-rust-bun") > wave("image-yah-rust"),
            "image-yah-rust-bun depends on image-yah-rust",
        );

        // Build → smoke → publish layering: cli-build precedes smoke, and the
        // publish jobs gate behind smoke (R330-F24).
        assert!(wave("cli-build") < wave("smoke"), "smoke runs after cli-build");
        assert!(
            wave("publish-cli") > wave("smoke"),
            "publish-cli gates behind smoke",
        );

        // cli-build expands its include-only matrix into per-row instances.
        let cli_instances: Vec<_> = plan
            .iter_instances()
            .filter(|i| i.job_id == "cli-build")
            .collect();
        assert_eq!(cli_instances.len(), 3, "cli-build has 3 matrix rows");
        assert!(cli_instances.iter().all(|i| i.matrix.is_some()));
    }

    #[test]
    fn release_yml_tier_classification() {
        // R533-F2 over a real workflow: every step classifies, checkout is
        // flagged tier-3 (replace native), and the cargo `run:` builds are clean
        // compute. Guards the catalog against release.yml's actual action set.
        let Some(wf) = parse_file("release.yml") else { eprintln!("skip: yah workflow fixtures not present"); return; };
        let classified = crate::classify_workflow(&wf);
        assert!(!classified.is_empty(), "release.yml has steps");

        // The first `smoke` step is `actions/checkout` → tier-3 replace-with-native.
        let checkout = classified
            .iter()
            .find(|c| c.job == "smoke" && c.step_index == 0)
            .expect("smoke step 0");
        assert_eq!(
            checkout.class.disposition,
            crate::Disposition::ReplaceWithNative(crate::NativeReplacement::Checkout),
        );

        // docker/build-push-action (image jobs) → tier-3 registry publish.
        assert!(classified.iter().any(|c| matches!(
            c.class.disposition,
            crate::Disposition::ReplaceWithNative(crate::NativeReplacement::RegistryPublish)
        )));

        // No step is left unclassified into a panic; the whole workflow walks.
        // Every step has a disposition that is exactly one of the three buckets.
        for c in &classified {
            let d = c.class.disposition;
            assert!(
                d.is_compute() || d.is_tier3() || d == crate::Disposition::Unknown,
                "{}.{} has a real disposition",
                c.job,
                c.step_index
            );
        }
    }

    #[test]
    fn run_body_preserves_multiline_script() {
        let Some(wf) = parse_file("smoke-sweeper.yml") else { eprintln!("skip: yah workflow fixtures not present"); return; };
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
