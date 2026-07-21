//! Step executor + workflow walker.
//!
//! F4 walks the [`crate::graph::Plan`] wave by wave (sequentially within a
//! wave — concurrency is a later concern), evaluating `if:` at each tier and
//! running steps through [`run_step`]. `run:` blocks spawn `bash`, capture
//! `::set-output::` / `$GITHUB_OUTPUT` / `$GITHUB_ENV`, and thread results
//! into `steps.<id>.outputs.*` for the next step. `uses:` blocks route
//! through the tier-1/2 [`ToolkitRegistry`] (W224 R533-T7). A slug that isn't a
//! registered toolkit action is classified by [`crate::tier`]: a tier-3
//! service action becomes a [`RuntimeError::Tier3RequiresNative`] (import it as
//! a native QED step, don't run it); an unrecognized slug stays a loud
//! [`RuntimeError::UnknownAction`].
//!
//! @yah:ticket(R605-F2, "Docker/buildx-capable QED runner substrate for the image-yah-{base,rust,rust-bun} jobs (retire GitHub-hosted builders)")
//! @yah:at(2026-07-16T01:32:20Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-glimmerstone)
//! @yah:parent(R605)
//! @yah:next("The setup-buildx/qemu `uses:` verifiers are already overridden in qed-gha (toolkit_builtin), but the image jobs still need a live docker/buildx daemon to run the builds. Provision QED runners with docker on the remote-runner tier rather than re-implementing a builder here.")
//! @yah:next("Route the image-yah-{base,rust,rust-bun} legs to a docker-capable node via the R555 remote-run placement + tier/quota grant, kamaji-admitted (signed recipes only, R555-F4).")
//! @yah:next("Reuse the R546 build-worker tier pattern (us-west-002) as the amd64 docker-capable substrate proof; measure pull+build+push to ghcr/registry.yah.dev.")
//! @yah:verify("A `yah qed run release` image slice builds and pushes image-yah-base to the registry from a QED-provisioned docker-capable runner with no GitHub-hosted builder in the loop")
//! @yah:depends_on(R555)
//! @yah:depends_on(R546)
//! @yah:depends_on(R563)

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

use indexmap::IndexMap;
use thiserror::Error;

use crate::expr::{self, Context, ExprError, JobStatus, Value};
use crate::expr_str::{ExprString, ExprToken};
use crate::graph::{
    build_context_for_instance, evaluate_outputs, plan as build_plan, CompletedInstance,
    GraphError, JobInstance, JobResult, Plan,
};
use crate::tier::{classify_uses, Disposition, NativeReplacement};
use crate::toolkit::{Lookup, StepConclusion, ToolkitCall, ToolkitRegistry};
#[cfg(test)]
use crate::toolkit::ToolkitOutcome;
use crate::workflow::{Job, Step, StepAction, Workflow};

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("expression error in {site}: {source}")]
    Expr {
        site: String,
        #[source]
        source: ExprError,
    },
    #[error("graph error: {0}")]
    Graph(#[from] GraphError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("unrecognized action `{slug}` — not a tier-1/2 toolkit action and not in the tier-3 native-replacement catalog. Import it as a native QED step (W224 R533-T7)")]
    UnknownAction { slug: String },
    #[error("tier-3 action `{slug}` is replaced by a native QED facility — {replacement}. Import it, don't run it: {stanza}")]
    Tier3RequiresNative {
        slug: String,
        replacement: String,
        stanza: String,
    },
    #[error("toolkit action `{slug}` failed: {message}")]
    ToolkitFailed { slug: String, message: String },
}

/// Public executor handle. Workflow-level inputs (github / inputs / runner_os)
/// stay on the executor so a single instance can run several workflows; the
/// tier-1/2 [`ToolkitRegistry`] is owned here so callers wire the built-in
/// toolkit actions once (via [`Executor::new`]).
pub struct Executor {
    pub workspace: PathBuf,
    pub registry: ToolkitRegistry,
    pub github: Value,
    pub inputs: Value,
    pub runner_os: String,
    /// Host arch in the GHA `runner.arch` vocabulary (`X64` / `ARM64` / …).
    /// Defaults to the running host (see [`detect_runner_arch`]). The QED
    /// runner overwrites this from its self-detected host triple (R531-T1)
    /// so a workflow gating on `runner.arch` sees the real host it's running
    /// on, not just `runner.os`.
    pub runner_arch: String,
    /// Forward the parent process env into step subprocesses. Tests usually
    /// want this off so the workflow env is hermetic; production wants it on
    /// so steps see PATH, HOME, etc.
    pub env_passthrough: bool,
    /// R499-F3 (phase 2): restrict execution to specific matrix instances.
    /// Keys are [`JobInstance::key`] (`<job_id>` for non-matrix jobs,
    /// `<job_id>#<row>` for matrix rows). `None` runs every planned
    /// instance (back-compat). `Some(set)` skips any instance whose key
    /// is not in the set — they surface as [`JobResult::Skipped`] so
    /// `needs.X.result` aggregation stays correct (skipped is neither
    /// success nor failure, so dependent jobs behave the same as a GHA
    /// `if:` skip). Empty set is a caller bug; validate at the qed-runner
    /// boundary, not here.
    pub included_instance_keys: Option<std::collections::HashSet<String>>,
    /// Optional live-event sink (W200 R487 follow-up). When set,
    /// [`execute_workflow`] emits a [`crate::GhaEvent`] at each job/step
    /// boundary so the qed-runner can mirror the nested tree into its own
    /// [`crate::QedEvent`] stream. When `None` the runtime is silent and
    /// the only observable surface is the returned [`WorkflowRun`].
    pub events: Option<crate::events::GhaEventSink>,
    /// Pre-resolved `secrets.*` context (R487 follow-up). On GHA, the
    /// runner injects secrets from the repo + org settings. On QED the
    /// caller resolves them up-front from a name-bridge mapping (e.g.
    /// `~/.yah/qed/secrets.toml`) and lays them onto the executor as a
    /// plain `Value::Object` so the expression evaluator sees the same
    /// `${{ secrets.X }}` shape it sees on GHA. `Value::Object(empty)` by
    /// default — a workflow that references an undefined secret evaluates
    /// to the empty string (matches GHA behavior for unset secrets).
    pub secrets: crate::expr::Value,
    /// Optional injected image builder for the docker push family (R594). When
    /// `Some`, `docker/login-action` + `docker/build-push-action` route here
    /// (registry route/auth + local-buildx or remote-fleet build) instead of
    /// the tier-3 `RegistryPublish` error. `None` (the default) preserves the
    /// honest "replace with a native build-image step" error, so the bare crate
    /// still never shells `docker`. Set via [`Executor::with_image_builder`].
    pub image_builder: Option<std::sync::Arc<dyn crate::image_builder::ImageBuilder>>,
    /// Optional injected artifact store for `actions/upload-artifact` /
    /// `actions/download-artifact` (R594). Same injection gate as
    /// [`Self::image_builder`]: `None` (default) keeps the tier-3 error.
    pub artifact_store: Option<std::sync::Arc<dyn crate::artifact_store::ArtifactStore>>,
}

impl Executor {
    /// New executor with the tier-1/2 toolkit actions pre-registered. This is
    /// the right default for production callers — a workflow whose `uses:`
    /// only references toolkit-contract compute slugs (`rust-toolchain`,
    /// `setup-bun`, the buildx/qemu setup verifiers, `cosign-installer`) runs
    /// straight through without extra wiring.
    pub fn new(workspace: impl Into<PathBuf>) -> Self {
        let mut e = Self::bare(workspace);
        crate::toolkit_builtin::register_toolkit(&mut e.registry);
        e
    }

    /// Empty-registry executor for tests that want hermetic dispatch (no
    /// built-ins, no `rustup`/`bun`/`docker` shelled out by accident). Used to
    /// assert the unknown-action / tier-3 dispatch errors fire.
    pub fn bare(workspace: impl Into<PathBuf>) -> Self {
        Self {
            workspace: workspace.into(),
            registry: ToolkitRegistry::new(),
            github: Value::object(),
            inputs: Value::object(),
            runner_os: detect_runner_os().into(),
            runner_arch: detect_runner_arch().into(),
            env_passthrough: true,
            included_instance_keys: None,
            events: None,
            secrets: Value::object(),
            image_builder: None,
            artifact_store: None,
        }
    }

    /// Configure an event sink. The returned executor emits one
    /// [`crate::GhaEvent`] per job/step boundary plus one per captured bash
    /// output line. Pre-existing callers that don't care leave it as `None`.
    pub fn with_events(mut self, sink: crate::events::GhaEventSink) -> Self {
        self.events = Some(sink);
        self
    }

    /// Configure the pre-resolved `secrets.*` context. See [`Executor::secrets`].
    pub fn with_secrets(mut self, secrets: crate::expr::Value) -> Self {
        self.secrets = secrets;
        self
    }

    /// Inject an image builder for the docker push family (R594). Enables the
    /// runtime to actually build + push the image jobs in a workflow instead of
    /// declining them. See [`Executor::image_builder`].
    pub fn with_image_builder(
        mut self,
        builder: std::sync::Arc<dyn crate::image_builder::ImageBuilder>,
    ) -> Self {
        self.image_builder = Some(builder);
        self
    }

    /// Inject an artifact store for `actions/upload-artifact` /
    /// `actions/download-artifact` (R594). See [`Executor::artifact_store`].
    pub fn with_artifact_store(
        mut self,
        store: std::sync::Arc<dyn crate::artifact_store::ArtifactStore>,
    ) -> Self {
        self.artifact_store = Some(store);
        self
    }
}

fn detect_runner_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "macOS",
        "linux" => "Linux",
        "windows" => "Windows",
        _ => "Linux",
    }
}

/// Host arch in the GHA `runner.arch` vocabulary. Mirrors the mapping in
/// `qed::platform::gha_runner_arch`, kept here so qed-gha stays free of a
/// dep edge back onto the qed runner crate.
fn detect_runner_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "X64",
        "aarch64" | "arm64" => "ARM64",
        "x86" | "i686" => "X86",
        "arm" => "ARM",
        _ => "X64",
    }
}

// ─── results ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct WorkflowRun {
    pub instances: Vec<InstanceRun>,
}

impl WorkflowRun {
    pub fn instance(&self, job_id: &str) -> Option<&InstanceRun> {
        self.instances.iter().find(|i| i.job_id == job_id)
    }

    pub fn instance_at(&self, job_id: &str, matrix_index: usize) -> Option<&InstanceRun> {
        self.instances
            .iter()
            .find(|i| i.job_id == job_id && i.matrix_index == Some(matrix_index))
    }
}

#[derive(Debug, Clone)]
pub struct InstanceRun {
    pub job_id: String,
    pub matrix_index: Option<usize>,
    pub result: JobResult,
    pub steps: Vec<StepResult>,
    pub outputs: IndexMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct StepResult {
    pub step_id: Option<String>,
    pub name: Option<String>,
    pub conclusion: StepConclusion,
    pub outputs: IndexMap<String, Value>,
    pub stdout: String,
    pub stderr: String,
}

// ─── workflow walker ───────────────────────────────────────────────────────

pub fn execute_workflow(
    workflow: &Workflow,
    executor: &Executor,
) -> Result<WorkflowRun, RuntimeError> {
    let plan: Plan = build_plan(workflow)?;
    let mut completed: Vec<CompletedInstance> = Vec::new();
    let mut runs: Vec<InstanceRun> = Vec::new();

    for wave in &plan.waves {
        // Sequential within wave — F4 simplification. Real parallelism is a
        // scheduling concern, not a correctness one, so we punt to F4+.
        for instance in wave {
            // R499-F3 phase 2: instance-key filter. Non-selected rows
            // short-circuit to Skipped — same path as a GHA `if: false`
            // — so needs aggregation (failure > cancelled > skipped >
            // success) and downstream `if:` checks still see them.
            let run = if executor
                .included_instance_keys
                .as_ref()
                .map(|set| !set.contains(&instance.key()))
                .unwrap_or(false)
            {
                InstanceRun {
                    job_id: instance.job_id.clone(),
                    matrix_index: instance.matrix_index,
                    result: JobResult::Skipped,
                    steps: vec![],
                    outputs: IndexMap::new(),
                }
            } else {
                emit_job_started(executor, instance, workflow);
                let r = run_instance(instance, workflow, executor, &completed)?;
                emit_job_finished(executor, instance, &r);
                r
            };
            completed.push(CompletedInstance {
                job_id: run.job_id.clone(),
                matrix_index: run.matrix_index,
                result: run.result,
                outputs: run.outputs.clone(),
            });
            runs.push(run);
        }
    }

    Ok(WorkflowRun { instances: runs })
}

fn run_instance(
    instance: &JobInstance,
    workflow: &Workflow,
    executor: &Executor,
    completed: &[CompletedInstance],
) -> Result<InstanceRun, RuntimeError> {
    let job = workflow
        .jobs
        .get(&instance.job_id)
        .expect("plan only references known jobs");

    // GHA implicit needs-gate: a job with no explicit `if:` runs only when
    // every job in its `needs:` succeeded. A failed / cancelled / skipped
    // dependency short-circuits the job to Skipped — matching GHA, where a
    // dependent job is skipped unless it opts in via an explicit `if:`
    // (`always()` / a `needs.X.result` check). Without this gate a consumer
    // job (e.g. `image-yah-yubaba`, which downloads `yubaba-bins-*`) runs
    // even when its producer (`yubaba-build`) failed and uploaded nothing,
    // so its `actions/download-artifact` step hits an empty store. The fix
    // is structural: the consumer never runs, so the failure surfaces at the
    // producing job — not as a bogus download error three waves later
    // (R516-B1).
    if !needs_gate_passes(job, completed) {
        return Ok(InstanceRun {
            job_id: instance.job_id.clone(),
            matrix_index: instance.matrix_index,
            result: JobResult::Skipped,
            steps: vec![],
            outputs: IndexMap::new(),
        });
    }

    // Pre-step context: matrix + needs + env composed but ctx.steps empty.
    let mut ctx = build_context_for_instance(
        instance,
        workflow,
        completed,
        executor.github.clone(),
        executor.inputs.clone(),
        &executor.runner_os,
        &executor.runner_arch,
        executor.secrets.clone(),
    )?;

    if !should_run_job(job, &ctx)? {
        return Ok(InstanceRun {
            job_id: instance.job_id.clone(),
            matrix_index: instance.matrix_index,
            result: JobResult::Skipped,
            steps: vec![],
            outputs: IndexMap::new(),
        });
    }

    // Per-job step accumulator + env file (updates from $GITHUB_ENV flow
    // forward to subsequent steps).
    let mut steps_obj: IndexMap<String, Value> = IndexMap::new();
    let mut env_overlay: IndexMap<String, String> = IndexMap::new();
    let mut step_results: Vec<StepResult> = Vec::new();
    let mut job_failed = false;
    let job_cancelled = false;

    for (idx, step) in job.steps.iter().enumerate() {
        // Refresh ctx.steps from the accumulator before each step so the
        // current step can see outputs from prior steps in this job.
        ctx.steps = Value::Object(steps_obj.clone());
        ctx.env = merge_env_overlay(&ctx.env, &env_overlay);
        ctx.job_status = Some(if job_failed {
            JobStatus::Failure
        } else if job_cancelled {
            JobStatus::Cancelled
        } else {
            JobStatus::Success
        });

        let run_this = should_run_step(step, &ctx, job_failed || job_cancelled)?;
        let synthetic_id = step
            .id
            .clone()
            .unwrap_or_else(|| format!("__step{idx}"));

        if !run_this {
            let skipped = StepResult {
                step_id: step.id.clone(),
                name: step.name.as_ref().and_then(exprstring_static),
                conclusion: StepConclusion::Skipped,
                outputs: IndexMap::new(),
                stdout: String::new(),
                stderr: String::new(),
            };
            steps_obj.insert(synthetic_id, step_value(&skipped));
            step_results.push(skipped);
            continue;
        }

        emit_step_started(executor, instance, idx, step);
        let res = run_step(step, &ctx, executor, &env_overlay, instance, idx)?;
        emit_step_finished(executor, instance, idx, &res);
        // Workflow commands setting env (`echo "K=V" >> $GITHUB_ENV`) bleed
        // into subsequent steps in the same job.
        if let Some(env_updates) = pop_env_updates(&res) {
            for (k, v) in env_updates {
                env_overlay.insert(k, v);
            }
        }
        let failed = matches!(res.conclusion, StepConclusion::Failure);
        let continue_on_error = step.continue_on_error.unwrap_or(false);
        steps_obj.insert(synthetic_id, step_value(&res));
        step_results.push(res);
        if failed && !continue_on_error {
            job_failed = true;
            // Remaining steps still get to run if their if: opts in to
            // failure() / always() — we keep iterating but with job_status
            // flipped so success() short-circuits.
        }
    }

    // Job outputs evaluate against final steps context.
    ctx.steps = Value::Object(steps_obj.clone());
    ctx.env = merge_env_overlay(&ctx.env, &env_overlay);
    let outputs = evaluate_outputs(&job.outputs, &ctx)?;

    let result = if job_failed {
        JobResult::Failure
    } else if job_cancelled {
        JobResult::Cancelled
    } else {
        JobResult::Success
    };

    Ok(InstanceRun {
        job_id: instance.job_id.clone(),
        matrix_index: instance.matrix_index,
        result,
        steps: step_results,
        outputs,
    })
}

fn merge_env_overlay(env: &Value, overlay: &IndexMap<String, String>) -> Value {
    let mut map = match env {
        Value::Object(m) => m.clone(),
        _ => IndexMap::new(),
    };
    for (k, v) in overlay {
        map.insert(k.clone(), Value::String(v.clone()));
    }
    Value::Object(map)
}

fn step_value(step: &StepResult) -> Value {
    let mut entry = IndexMap::new();
    entry.insert(
        "outputs".to_string(),
        Value::Object(step.outputs.clone()),
    );
    entry.insert(
        "conclusion".to_string(),
        Value::String(step.conclusion.as_str().into()),
    );
    // GHA distinguishes outcome (pre-continue-on-error) from conclusion
    // (post-) — they coincide unless continue-on-error is set. Coincidence
    // is the right default for F4; F5 can split when we add a fixture that
    // needs it.
    entry.insert(
        "outcome".to_string(),
        Value::String(step.conclusion.as_str().into()),
    );
    Value::Object(entry)
}

// ─── if-cond eval (job + step) ─────────────────────────────────────────────

/// GHA status-check functions. Their presence anywhere in a job `if:` is what
/// makes GHA drop the implicit `success()` needs-gate — *not* the mere presence
/// of an `if:`. An `if:` built only from event/ref filters keeps the gate.
const STATUS_FUNCTIONS: [&str; 4] = ["always", "success", "failure", "cancelled"];

/// Whether an `if:` condition references a GHA status-check function as a call
/// (`always()`, `failure()`, …). Scans the raw token bodies; matches the name
/// only when it stands as its own identifier immediately followed by `(`, so
/// `needs.failure_count` or a `success_url` field don't trip it.
fn references_status_function(expr_str: &ExprString) -> bool {
    expr_str.tokens.iter().any(|t| {
        let body = match t {
            ExprToken::Literal(s) | ExprToken::Expr(s) => s.as_str(),
        };
        STATUS_FUNCTIONS.iter().any(|f| body_calls(body, f))
    })
}

/// True if `body` contains `name` as a standalone identifier followed (after
/// optional whitespace) by `(`.
fn body_calls(body: &str, name: &str) -> bool {
    let bytes = body.as_bytes();
    let mut from = 0;
    while let Some(rel) = body[from..].find(name) {
        let start = from + rel;
        let end = start + name.len();
        let prev_ok = start == 0
            || !matches!(bytes[start - 1], b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_');
        let next_is_paren = body[end..].trim_start().starts_with('(');
        if prev_ok && next_is_paren {
            return true;
        }
        from = end;
    }
    false
}

/// GHA implicit needs-gate. Returns `false` when at least one `needs:`
/// dependency did not aggregate to success (matrix rows aggregate per
/// [`JobResult::aggregate`]: any failure wins).
///
/// GHA injects an implicit `success()` (all-needs-succeeded) into a job's `if:`
/// *unless* the author's condition references a status-check function — so
/// `if: <event/ref filter>` is really `success() && (<filter>)` and a job with
/// such a filter is still skipped when a `needs:` producer failed or was
/// skipped. Only `always()` / `failure()` / `cancelled()` / explicit
/// `success()` opt out of the auto gate (which is why `if: always() && …`
/// publish jobs still run after a skipped dependency). The earlier
/// `if_cond.is_some()` short-circuit was too coarse: it let `smoke` (an
/// event/ref `if:` with no status function) run after its `cli-build` producer
/// was skipped, then fail on an empty artifact store three waves later (R516-B1).
fn needs_gate_passes(job: &Job, completed: &[CompletedInstance]) -> bool {
    if let Some(cond) = &job.if_cond {
        if references_status_function(cond) {
            return true;
        }
    }
    for need in &job.needs {
        let agg = JobResult::aggregate(
            completed
                .iter()
                .filter(|c| &c.job_id == need)
                .map(|c| c.result),
        );
        if agg != JobResult::Success {
            return false;
        }
    }
    true
}

fn should_run_job(job: &Job, ctx: &Context) -> Result<bool, RuntimeError> {
    let Some(expr_str) = &job.if_cond else { return Ok(true) };
    eval_implicit_expr(expr_str, ctx, "job.if").map(|v| v.is_truthy())
}

fn should_run_step(
    step: &Step,
    ctx: &Context,
    prior_failure: bool,
) -> Result<bool, RuntimeError> {
    let Some(expr_str) = &step.if_cond else { return Ok(!prior_failure) };
    eval_implicit_expr(expr_str, ctx, "step.if").map(|v| v.is_truthy())
}

/// Implicit-expression body extractor for `if:`-style scalars (whole body is
/// an expression, with or without `${{ }}` delimiters). Mirrors the
/// `graph::should_run_job` helper but lives here so the step executor can
/// reuse the path.
fn eval_implicit_expr(s: &ExprString, ctx: &Context, site: &str) -> Result<Value, RuntimeError> {
    let body = match s.tokens.as_slice() {
        [ExprToken::Literal(b)] | [ExprToken::Expr(b)] => b.clone(),
        _ => {
            // Mixed-token if: is malformed GHA but worth a graceful path —
            // fall back to ExprString eval rather than panicking.
            return crate::graph::eval_exprstring(s, ctx).map_err(|source| RuntimeError::Expr {
                site: site.into(),
                source,
            });
        }
    };
    expr::evaluate(&body, ctx).map_err(|source| RuntimeError::Expr {
        site: site.into(),
        source,
    })
}

// ─── step exec ─────────────────────────────────────────────────────────────

fn run_step(
    step: &Step,
    ctx: &Context,
    executor: &Executor,
    env_overlay: &IndexMap<String, String>,
    instance: &JobInstance,
    step_index: usize,
) -> Result<StepResult, RuntimeError> {
    let env = compose_step_env(step, ctx, env_overlay, executor)?;
    let res = match &step.action {
        StepAction::Run { body, shell } => {
            run_bash_step(step, body, shell.as_deref(), &env, ctx, executor, instance, step_index)?
        }
        StepAction::Uses { slug, git_ref, with } => {
            run_uses_step(step, slug, git_ref.as_deref(), with, &env, ctx, executor)?
        }
    };
    Ok(res)
}

/// Compose the env passed to a step: workflow.env + job.env are already in
/// ctx.env; merge per-step env (typed values lowered to strings) on top. The
/// `env_overlay` argument is the prior-step `$GITHUB_ENV` accumulator — it's
/// already folded into ctx.env by [`run_instance`], so this just lays the
/// step's own ExprString-evaluated env on the result.
fn compose_step_env(
    step: &Step,
    ctx: &Context,
    _env_overlay: &IndexMap<String, String>,
    executor: &Executor,
) -> Result<IndexMap<String, String>, RuntimeError> {
    let mut out: IndexMap<String, String> = IndexMap::new();
    // Lowest-precedence host default (inserted first so workflow / job / step
    // `env:` below override it): when the runner host isn't x86_64, point docker
    // at linux/amd64. Steps that pull the amd64-only cross base images
    // (`cross build`) otherwise fail on an arm64 host with "no match for
    // platform in manifest"; with this they resolve under emulation. Explicit
    // `docker buildx build --platform …` in the multi-arch image jobs still
    // wins over this default, and it's a no-op on x86_64 hosts (and on real
    // GHA, which never executes through this runner).
    if executor.runner_arch != "X64" {
        out.insert("DOCKER_DEFAULT_PLATFORM".into(), "linux/amd64".into());
    }
    if let Value::Object(m) = &ctx.env {
        for (k, v) in m {
            out.insert(k.clone(), v.as_str_lossy());
        }
    }
    for (k, v) in &step.env {
        let value = crate::graph::eval_exprstring(v, ctx).map_err(|source| RuntimeError::Expr {
            site: format!("step.env.{k}"),
            source,
        })?;
        out.insert(k.clone(), value.as_str_lossy());
    }
    Ok(out)
}

fn run_bash_step(
    step: &Step,
    body: &ExprString,
    shell: Option<&str>,
    env: &IndexMap<String, String>,
    ctx: &Context,
    executor: &Executor,
    instance: &JobInstance,
    step_index: usize,
) -> Result<StepResult, RuntimeError> {
    let body_str = crate::graph::eval_exprstring(body, ctx)
        .map_err(|source| RuntimeError::Expr { site: "step.run".into(), source })?
        .as_str_lossy();

    let shell = shell.unwrap_or("bash");
    if shell != "bash" {
        return Err(RuntimeError::Expr {
            site: "step.shell".into(),
            source: ExprError::Eval(format!("unsupported shell `{shell}` (F4 supports bash only)")),
        });
    }

    let tmp = tempfile::tempdir()?;
    let script_path = tmp.path().join("step.sh");
    {
        let mut f = std::fs::File::create(&script_path)?;
        // `set -e` matches GHA default. `set -o pipefail` mirrors what the
        // GHA bash invocation does so failures inside `|` chains surface.
        writeln!(f, "#!/usr/bin/env bash")?;
        writeln!(f, "set -eo pipefail")?;
        f.write_all(body_str.as_bytes())?;
        if !body_str.ends_with('\n') {
            writeln!(f)?;
        }
    }

    let output_path = tmp.path().join("output");
    let env_path = tmp.path().join("env");
    let step_summary_path = tmp.path().join("step_summary");
    std::fs::File::create(&output_path)?;
    std::fs::File::create(&env_path)?;
    std::fs::File::create(&step_summary_path)?;

    let mut cmd = Command::new(shell);
    cmd.arg(&script_path);
    cmd.current_dir(&executor.workspace);
    if !executor.env_passthrough {
        cmd.env_clear();
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.env("GITHUB_OUTPUT", &output_path);
    cmd.env("GITHUB_ENV", &env_path);
    cmd.env("GITHUB_STEP_SUMMARY", &step_summary_path);
    cmd.env("RUNNER_OS", &executor.runner_os);
    cmd.env("RUNNER_ARCH", &executor.runner_arch);

    // Pipe stdout + stderr so we can stream lines through the event sink
    // (when configured) and still capture full buffers for the returned
    // StepResult. Two reader threads drain each pipe; both join before we
    // wait on the child so the script's exit status reflects the final
    // command and we don't lose tail bytes.
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;
    let child_stdout = child.stdout.take().expect("piped");
    let child_stderr = child.stderr.take().expect("piped");

    let sink_out = executor.events.clone();
    let sink_err = executor.events.clone();
    let job_id = instance.job_id.clone();
    let matrix_index = instance.matrix_index;
    let job_id_err = job_id.clone();

    let stdout_handle = std::thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        let mut buf = String::new();
        let reader = BufReader::new(child_stdout);
        for line in reader.lines().flatten() {
            if let Some(s) = &sink_out {
                let _ = s.send(crate::events::GhaEvent::StepOutput {
                    job_id: job_id.clone(),
                    matrix_index,
                    step_index,
                    stream: crate::events::GhaOutputStream::Stdout,
                    line: line.clone(),
                });
            }
            buf.push_str(&line);
            buf.push('\n');
        }
        buf
    });
    let stderr_handle = std::thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        let mut buf = String::new();
        let reader = BufReader::new(child_stderr);
        for line in reader.lines().flatten() {
            if let Some(s) = &sink_err {
                let _ = s.send(crate::events::GhaEvent::StepOutput {
                    job_id: job_id_err.clone(),
                    matrix_index,
                    step_index,
                    stream: crate::events::GhaOutputStream::Stderr,
                    line: line.clone(),
                });
            }
            buf.push_str(&line);
            buf.push('\n');
        }
        buf
    });

    let status = child.wait()?;
    let stdout = stdout_handle.join().unwrap_or_default();
    let stderr = stderr_handle.join().unwrap_or_default();
    let success = status.success();

    // Capture outputs: legacy `::set-output name=K::V` on stdout + modern
    // `K=V` (or `K<<EOF\n…\nEOF`) lines in $GITHUB_OUTPUT. Both are valid;
    // workflows in the wild mix them.
    let mut outputs = parse_set_output_lines(&stdout);
    let output_file = std::fs::read_to_string(&output_path)?;
    for (k, v) in parse_env_file(&output_file) {
        outputs.insert(k, Value::String(v));
    }

    // Stash $GITHUB_ENV updates inside stderr-style sidechannel by reading
    // and folding into the StepResult as a special marker — see
    // `pop_env_updates`.
    let env_file = std::fs::read_to_string(&env_path)?;
    let env_updates = parse_env_file(&env_file);

    let conclusion = if success {
        StepConclusion::Success
    } else {
        StepConclusion::Failure
    };

    let mut step_res = StepResult {
        step_id: step.id.clone(),
        name: step.name.as_ref().and_then(exprstring_static),
        conclusion,
        outputs,
        stdout,
        stderr,
    };
    if !env_updates.is_empty() {
        // Encode env updates as a magic prefix on stderr so `pop_env_updates`
        // can pluck them back out without a separate plumbing field. F5 can
        // promote this to a typed field if more state grows here.
        let mut payload = String::from(ENV_UPDATE_PREFIX);
        for (k, v) in &env_updates {
            payload.push_str(&format!("{k}\t{v}\n"));
        }
        payload.push_str(ENV_UPDATE_SUFFIX);
        step_res.stderr.push_str(&payload);
    }
    Ok(step_res)
}

const ENV_UPDATE_PREFIX: &str = "__qed_gha_env_updates_BEGIN__\n";
const ENV_UPDATE_SUFFIX: &str = "__qed_gha_env_updates_END__\n";

fn pop_env_updates(res: &StepResult) -> Option<Vec<(String, String)>> {
    let stderr = &res.stderr;
    let start = stderr.find(ENV_UPDATE_PREFIX)?;
    let body_start = start + ENV_UPDATE_PREFIX.len();
    let end = stderr[body_start..].find(ENV_UPDATE_SUFFIX)?;
    let body = &stderr[body_start..body_start + end];
    let mut out = Vec::new();
    for line in body.lines() {
        if let Some((k, v)) = line.split_once('\t') {
            out.push((k.to_string(), v.to_string()));
        }
    }
    Some(out)
}

/// Parse `::set-output name=KEY::VALUE` lines from a step's stdout.
fn parse_set_output_lines(stdout: &str) -> IndexMap<String, Value> {
    let mut out = IndexMap::new();
    for line in stdout.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(rest) = line.strip_prefix("::set-output name=") {
            if let Some((key, value)) = rest.split_once("::") {
                out.insert(key.to_string(), Value::String(value.to_string()));
            }
        }
    }
    out
}

/// Parse the modern `$GITHUB_OUTPUT` / `$GITHUB_ENV` file format:
///   - `KEY=VALUE`            (single-line)
///   - `KEY<<DELIM\n...\nDELIM` (multi-line, with a user-chosen DELIM)
fn parse_env_file(contents: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let lines: Vec<&str> = contents.lines().collect();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i].trim_end_matches('\r');
        if line.is_empty() {
            i += 1;
            continue;
        }
        if let Some((key, rest)) = line.split_once("<<") {
            let delim = rest.trim();
            let key = key.trim().to_string();
            let mut buf = String::new();
            i += 1;
            while i < lines.len() {
                let body_line = lines[i].trim_end_matches('\r');
                if body_line == delim {
                    i += 1;
                    break;
                }
                if !buf.is_empty() {
                    buf.push('\n');
                }
                buf.push_str(body_line);
                i += 1;
            }
            out.push((key, buf));
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            out.push((key.trim().to_string(), value.to_string()));
        }
        i += 1;
    }
    out
}

fn exprstring_static(s: &ExprString) -> Option<String> {
    s.as_pure_literal()
}

// ─── uses dispatch ─────────────────────────────────────────────────────────

/// True when an `actions/checkout` step targets a *different* repository (a
/// non-empty `repository:` input) — the one case W224 says still needs a native
/// clone. A same-repo checkout (the overwhelming default: no `repository:`, or an
/// empty one) is implicit on QED and skipped as a no-op.
fn checks_out_foreign_repo(with: &IndexMap<String, Value>) -> bool {
    matches!(with.get("repository"), Some(Value::String(repo)) if !repo.trim().is_empty())
}

/// Read an `actions/checkout` `with:` input as a trimmed non-empty string.
/// Numbers (`fetch-depth: 0`) come through `as_str_lossy`, so this works for
/// both string and numeric YAML scalars.
fn checkout_input(with: &IndexMap<String, Value>, key: &str) -> Option<String> {
    let s = with.get(key)?.as_str_lossy();
    (!s.trim().is_empty()).then(|| s.trim().to_string())
}

/// W224 R533-T12: emit an explicit native `git clone` for a *foreign-repo*
/// `actions/checkout`. Same-repo checkout is a no-op (the workspace already IS
/// the checkout); only a `with: repository:` naming a different repo lands here.
///
/// Honors the three inputs the [`NativeReplacement::Checkout`] stanza promises:
/// - `ref` → `git clone --branch <ref>` (a branch or tag; a bare commit SHA is
///   not supported by `--branch` and fails here — pin foreign repos to a
///   branch/tag, or import an explicit fetch+checkout step for a SHA),
/// - `path` → the clone subdir under the workspace (default: the repo's short
///   name, never the workspace root, so a foreign checkout can't clobber the
///   run's own positioned tree),
/// - `fetch-depth` → `--depth N` (GHA's default is `1` = shallow; `0` = full
///   history, no `--depth`).
///
/// A non-zero `git` exit surfaces as a [`StepConclusion::Failure`] step (with
/// the git stderr captured), exactly like a failing `run:` step — not a hard
/// [`RuntimeError`] — so normal job-failure handling applies. A spawn failure
/// (no `git` on PATH) is the one [`RuntimeError::Io`] case.
fn run_native_checkout(
    step: &Step,
    with: &IndexMap<String, Value>,
    executor: &Executor,
) -> Result<StepResult, RuntimeError> {
    let repository = checkout_input(with, "repository")
        .expect("caller verified a non-empty repository via checks_out_foreign_repo");

    // `owner/repo` → `https://github.com/owner/repo.git`. A literal URL (any
    // scheme) or a filesystem path (absolute or `.`-relative) passes through
    // unchanged — the latter lets a clone target a local repo with no network,
    // which is also what the unit test exercises.
    let url = if repository.contains("://")
        || repository.starts_with('/')
        || repository.starts_with('.')
    {
        repository.clone()
    } else {
        format!("https://github.com/{repository}.git")
    };

    let dest_rel = checkout_input(with, "path").unwrap_or_else(|| {
        repository
            .trim_end_matches(".git")
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or("checkout")
            .to_string()
    });
    let dest = executor.workspace.join(&dest_rel);

    let depth: u64 = checkout_input(with, "fetch-depth")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let mut cmd = Command::new("git");
    cmd.arg("clone");
    if depth > 0 {
        cmd.arg("--depth").arg(depth.to_string());
    }
    if let Some(git_ref) = checkout_input(with, "ref") {
        cmd.arg("--branch").arg(git_ref);
    }
    cmd.arg("--").arg(&url).arg(&dest);
    cmd.current_dir(&executor.workspace);
    if !executor.env_passthrough {
        cmd.env_clear();
    }

    let output = cmd.output()?; // spawn failure ⇒ RuntimeError::Io
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let conclusion = if output.status.success() {
        StepConclusion::Success
    } else {
        StepConclusion::Failure
    };
    Ok(StepResult {
        step_id: step.id.clone(),
        name: step.name.as_ref().and_then(exprstring_static),
        conclusion,
        outputs: IndexMap::new(),
        stdout,
        stderr,
    })
}

fn run_uses_step(
    step: &Step,
    slug: &str,
    git_ref: Option<&str>,
    with: &IndexMap<String, ExprString>,
    env: &IndexMap<String, String>,
    ctx: &Context,
    executor: &Executor,
) -> Result<StepResult, RuntimeError> {
    let mut typed_with: IndexMap<String, Value> = IndexMap::new();
    for (k, v) in with {
        let value = crate::graph::eval_exprstring(v, ctx).map_err(|source| RuntimeError::Expr {
            site: format!("step.with.{k}"),
            source,
        })?;
        typed_with.insert(k.clone(), value);
    }

    let outcome = match executor.registry.lookup(slug) {
        Lookup::Found { action } => {
            let call = ToolkitCall {
                slug,
                git_ref,
                with: &typed_with,
                env,
                workspace: &executor.workspace,
            };
            action
                .execute(&call)
                .map_err(|message| RuntimeError::ToolkitFailed {
                    slug: slug.into(),
                    message,
                })?
        }
        // Not a tier-1/2 toolkit action. W224 R533-T7: decline to imitate
        // tier-3 GitHub services — the tier classifier names the native QED
        // replacement so the failure is honest ("import this as a native step")
        // instead of mysterious. A slug in neither bucket is a genuine unknown.
        Lookup::Unknown => {
            let (_, disposition) = classify_uses(slug);
            // W224: `actions/checkout` against the *same* repo is implicit on QED
            // — it already owns the workspace (the camp root IS the checkout), so
            // re-cloning over a live tree is wrong. Treat a same-repo checkout as
            // a successful no-op rather than erroring; the workflow stays valid on
            // GitHub-the-service (where the step does real work) while running
            // unchanged here. Only a *foreign-repo* checkout (a `repository:`
            // input naming another repo) genuinely needs a native clone, so that
            // case still falls through to the tier-3 error below.
            if matches!(disposition, Disposition::ReplaceWithNative(NativeReplacement::Checkout)) {
                // W224: a *same-repo* checkout is implicit on QED — the camp root
                // (or the run's positioned worktree) IS the checkout, so re-cloning
                // over the live tree is wrong. Treat it as a successful no-op.
                if !checks_out_foreign_repo(&typed_with) {
                    return Ok(StepResult {
                        step_id: step.id.clone(),
                        name: step.name.as_ref().and_then(exprstring_static),
                        conclusion: StepConclusion::Success,
                        outputs: IndexMap::new(),
                        stdout: "checkout is implicit on QED (workspace already present) — step skipped"
                            .into(),
                        stderr: String::new(),
                    });
                }
                // R533-T12: a *foreign-repo* checkout (`with: repository: other/repo`)
                // genuinely needs a clone — the NativeReplacement::Checkout stanza
                // promises one. Emit an explicit native git clone into a subdir of
                // the workspace, honoring `ref` / `path` / `fetch-depth`.
                return run_native_checkout(step, &typed_with, executor);
            }
            // R594: retired tier-3 *services* the qed runner executes for real
            // when it injects a handler — the docker push family (via
            // `image_builder`) and the artifact actions (via `artifact_store`).
            // Each is gated on injection: with no handler (the bare crate, most
            // tests) we fall through to the honest tier-3 error below, so
            // qed-gha on its own still never shells docker or touches a store.
            let injected: Option<Result<crate::toolkit::ToolkitOutcome, String>> =
                if crate::image_builder::is_image_push_action(slug) {
                    executor.image_builder.as_ref().map(|builder| {
                        let call = crate::image_builder::ImageBuildCall {
                            slug,
                            with: &typed_with,
                            env,
                            workspace: &executor.workspace,
                        };
                        builder.handle(&call)
                    })
                } else if crate::artifact_store::is_artifact_action(slug) {
                    executor.artifact_store.as_ref().map(|store| {
                        let call = crate::artifact_store::ArtifactCall {
                            with: &typed_with,
                            workspace: &executor.workspace,
                        };
                        if slug == "actions/upload-artifact" {
                            store.upload(&call)
                        } else {
                            store.download(&call)
                        }
                    })
                } else {
                    None
                };
            match injected {
                Some(res) => res.map_err(|message| RuntimeError::ToolkitFailed {
                    slug: slug.into(),
                    message,
                })?,
                None => {
                    return Err(match disposition {
                        Disposition::ReplaceWithNative(nr) => RuntimeError::Tier3RequiresNative {
                            slug: slug.into(),
                            replacement: nr.label().into(),
                            stanza: nr.stanza_hint().into(),
                        },
                        // Compute/Unknown with no registered toolkit impl: we
                        // have no executor for it. Surface for human review
                        // rather than guessing.
                        Disposition::Compute | Disposition::Unknown => {
                            RuntimeError::UnknownAction { slug: slug.into() }
                        }
                    });
                }
            }
        }
    };

    // A failing toolkit action (conclusion=Failure) mirrors its log into stderr
    // so the qed-runner's stderr-tail diagnostic surfaces *why* the step failed
    // (it reads `StepResult::stderr`, not stdout). Without this, a graceful
    // action failure shows up downstream as "(no stderr)".
    let stderr = if matches!(outcome.conclusion, StepConclusion::Failure) {
        outcome.log.clone()
    } else {
        String::new()
    };
    Ok(StepResult {
        step_id: step.id.clone(),
        name: step.name.as_ref().and_then(exprstring_static),
        conclusion: outcome.conclusion,
        outputs: outcome.outputs,
        stdout: outcome.log,
        stderr,
    })
}

// ─── event emit helpers ────────────────────────────────────────────────────

fn emit_job_started(executor: &Executor, instance: &JobInstance, workflow: &Workflow) {
    let Some(sink) = executor.events.as_ref() else { return };
    let total_steps = workflow
        .jobs
        .get(&instance.job_id)
        .map(|j| j.steps.len())
        .unwrap_or(0);
    let _ = sink.send(crate::events::GhaEvent::JobStarted {
        job_id: instance.job_id.clone(),
        matrix_index: instance.matrix_index,
        key: instance.key(),
        total_steps,
    });
}

fn emit_job_finished(executor: &Executor, instance: &JobInstance, run: &InstanceRun) {
    let Some(sink) = executor.events.as_ref() else { return };
    let _ = sink.send(crate::events::GhaEvent::JobFinished {
        job_id: instance.job_id.clone(),
        matrix_index: instance.matrix_index,
        key: instance.key(),
        result: run.result,
    });
}

fn emit_step_started(
    executor: &Executor,
    instance: &JobInstance,
    step_index: usize,
    step: &Step,
) {
    let Some(sink) = executor.events.as_ref() else { return };
    let action_kind = match &step.action {
        StepAction::Run { .. } => "run".to_string(),
        StepAction::Uses { slug, .. } => format!("uses:{slug}"),
    };
    let _ = sink.send(crate::events::GhaEvent::StepStarted {
        job_id: instance.job_id.clone(),
        matrix_index: instance.matrix_index,
        step_index,
        step_id: step.id.clone(),
        name: step.name.as_ref().and_then(exprstring_static),
        action_kind,
    });
}

fn emit_step_finished(
    executor: &Executor,
    instance: &JobInstance,
    step_index: usize,
    res: &StepResult,
) {
    let Some(sink) = executor.events.as_ref() else { return };
    let msg = if matches!(res.conclusion, StepConclusion::Failure) {
        let tail: Vec<&str> = res
            .stderr
            .lines()
            .filter(|l| {
                let t = l.trim();
                !t.starts_with(ENV_UPDATE_PREFIX.trim())
                    && !t.starts_with(ENV_UPDATE_SUFFIX.trim())
                    && !t.is_empty()
            })
            .collect();
        let start = tail.len().saturating_sub(20);
        let s = tail[start..].join("\n");
        if s.is_empty() { None } else { Some(s) }
    } else {
        None
    };
    let _ = sink.send(crate::events::GhaEvent::StepFinished {
        job_id: instance.job_id.clone(),
        matrix_index: instance.matrix_index,
        step_index,
        conclusion: res.conclusion,
        msg,
        outputs: res.outputs.clone(),
    });
}

// ─── tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::toolkit::ToolkitAction;
    use crate::parse_workflow;

    fn workflow(yaml: &str) -> Workflow {
        parse_workflow(yaml).unwrap_or_else(|e| panic!("parse: {e}"))
    }

    fn executor() -> Executor {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = Executor::new(tmp.path());
        // Tests run hermetic — env_passthrough off so PATH-leak doesn't
        // change behavior. Re-inject PATH explicitly so bash + coreutils
        // still resolve.
        e.env_passthrough = false;
        let path = std::env::var("PATH").unwrap_or_default();
        e.runner_os = "Linux".into();
        // Stash PATH on a struct field-less side — we add it to the env via
        // a workflow-level env entry instead.
        std::mem::forget(tmp); // keep workspace alive for the test
        let _ = path;
        e
    }

    fn workspace_path() -> PathBuf {
        // Tests share one tmpdir-style cwd. The executor doesn't depend on
        // the workspace contents (we only spawn bash on a script we wrote
        // to its own tmp dir), so the CWD is mostly aesthetic.
        std::env::temp_dir()
    }

    fn run_with_path(wf: &Workflow) -> WorkflowRun {
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true; // need PATH for bash/coreutils
        e.runner_os = "Linux".into();
        execute_workflow(wf, &e).unwrap_or_else(|err| panic!("execute: {err}"))
    }

    #[test]
    fn bash_run_step_succeeds_and_captures_legacy_set_output() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: build
        run: |
          echo '::set-output name=digest::sha256:abc'
          echo 'ok'
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst = run.instance("one").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        assert_eq!(inst.steps.len(), 1);
        let s = &inst.steps[0];
        assert_eq!(s.conclusion, StepConclusion::Success);
        assert_eq!(
            s.outputs.get("digest"),
            Some(&Value::String("sha256:abc".into()))
        );
    }

    #[test]
    fn matrix_expr_in_run_body_is_interpolated() {
        // Regression: render_run_body used to drop ${{ }} tokens, so
        // `cargo build --target ${{ matrix.target }}` shipped to bash as
        // `cargo build --target ` (trailing flag, no value).
        let yaml = r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [x86_64-unknown-linux-musl, aarch64-unknown-linux-musl]
    steps:
      - id: emit
        run: |
          echo "::set-output name=t::${{ matrix.target }}"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst0 = run.instance_at("build", 0).unwrap();
        let inst1 = run.instance_at("build", 1).unwrap();
        assert_eq!(
            inst0.steps[0].outputs.get("t"),
            Some(&Value::String("x86_64-unknown-linux-musl".into()))
        );
        assert_eq!(
            inst1.steps[0].outputs.get("t"),
            Some(&Value::String("aarch64-unknown-linux-musl".into()))
        );
    }

    #[test]
    fn bash_run_step_captures_github_output_file() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: build
        run: |
          echo "digest=sha256:def" >> "$GITHUB_OUTPUT"
          printf 'changelog<<EOF\nline1\nline2\nEOF\n' >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let s = &run.instance("one").unwrap().steps[0];
        assert_eq!(
            s.outputs.get("digest"),
            Some(&Value::String("sha256:def".into()))
        );
        assert_eq!(
            s.outputs.get("changelog"),
            Some(&Value::String("line1\nline2".into()))
        );
    }

    #[test]
    fn bash_failure_marks_job_failure() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - run: |
          exit 7
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst = run.instance("one").unwrap();
        assert_eq!(inst.result, JobResult::Failure);
        assert_eq!(inst.steps[0].conclusion, StepConclusion::Failure);
    }

    #[test]
    fn continue_on_error_lets_job_keep_running() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: bad
        continue-on-error: true
        run: |
          exit 2
      - id: ok
        if: always()
        run: |
          echo "still running"
          echo "ran=yes" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst = run.instance("one").unwrap();
        // continue-on-error: true means the failing step does NOT fail the
        // job; downstream steps keep running and the job's aggregate result
        // is success. The step itself still records conclusion=failure so
        // `steps.bad.conclusion` is visible to expressions.
        assert_eq!(inst.result, JobResult::Success);
        assert_eq!(inst.steps.len(), 2);
        assert_eq!(inst.steps[0].conclusion, StepConclusion::Failure);
        assert_eq!(inst.steps[1].conclusion, StepConclusion::Success);
        assert_eq!(
            inst.steps[1].outputs.get("ran"),
            Some(&Value::String("yes".into()))
        );
    }

    #[test]
    fn github_env_propagates_between_steps() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo "MYVAR=hello" >> "$GITHUB_ENV"
      - run: |
          echo "MYVAR=$MYVAR"
          echo "saw=$MYVAR" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst = run.instance("one").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        assert_eq!(
            inst.steps[1].outputs.get("saw"),
            Some(&Value::String("hello".into()))
        );
    }

    #[test]
    fn step_outputs_flow_into_downstream_job_via_needs() {
        let yaml = r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    outputs:
      digest: ${{ steps.b.outputs.digest }}
    steps:
      - id: b
        run: |
          echo "digest=sha256:42" >> "$GITHUB_OUTPUT"
  publish:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - id: echo
        run: |
          echo "got=$DIGEST" >> "$GITHUB_OUTPUT"
        env:
          DIGEST: ${{ needs.build.outputs.digest }}
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        assert_eq!(run.instance("build").unwrap().result, JobResult::Success);
        let pub_inst = run.instance("publish").unwrap();
        assert_eq!(pub_inst.result, JobResult::Success);
        assert_eq!(
            pub_inst.steps[0].outputs.get("got"),
            Some(&Value::String("sha256:42".into()))
        );
    }

    #[test]
    fn non_amd64_host_injects_docker_default_platform() {
        // On an arm64 host, steps see DOCKER_DEFAULT_PLATFORM=linux/amd64 so
        // `cross build` can pull the amd64-only cross base image under emulation
        // instead of failing with "no match for platform in manifest".
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: a
        run: |
          echo "plat=$DOCKER_DEFAULT_PLATFORM" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        e.runner_arch = "ARM64".into();
        let run = execute_workflow(&wf, &e).unwrap_or_else(|err| panic!("execute: {err}"));
        let inst = run.instance("one").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        assert_eq!(
            inst.steps[0].outputs.get("plat"),
            Some(&Value::String("linux/amd64".into())),
        );
    }

    #[test]
    fn workflow_env_overrides_injected_docker_default_platform() {
        // The injected value is a *default* — an explicit workflow/job/step
        // `env:` still wins (lowest-precedence insertion).
        let yaml = r#"
on: [push]
env:
  DOCKER_DEFAULT_PLATFORM: linux/arm64
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: a
        run: |
          echo "plat=$DOCKER_DEFAULT_PLATFORM" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        e.runner_arch = "ARM64".into();
        let run = execute_workflow(&wf, &e).unwrap_or_else(|err| panic!("execute: {err}"));
        let inst = run.instance("one").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        assert_eq!(
            inst.steps[0].outputs.get("plat"),
            Some(&Value::String("linux/arm64".into())),
        );
    }

    // ── uses dispatch

    struct FakeAction {
        slug: &'static str,
    }
    impl ToolkitAction for FakeAction {
        fn execute(&self, call: &ToolkitCall<'_>) -> Result<ToolkitOutcome, String> {
            let mut outputs = IndexMap::new();
            outputs.insert(
                "slug".into(),
                Value::String(call.slug.to_string()),
            );
            outputs.insert(
                "ref".into(),
                Value::String(call.git_ref.unwrap_or("").to_string()),
            );
            // Reflect the with: inputs back so the caller can assert eval
            // happened.
            for (k, v) in call.with.iter() {
                outputs.insert(format!("in_{k}"), v.clone());
            }
            // Use the slug to identify which action fired.
            let _ = self.slug;
            Ok(ToolkitOutcome {
                outputs,
                log: "fake".into(),
                conclusion: StepConclusion::Success,
            })
        }
    }

    #[test]
    fn uses_unknown_action_raises_error() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: nope/missing@v1
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        let err = execute_workflow(&wf, &e).expect_err("must error on unknown action");
        match err {
            RuntimeError::UnknownAction { slug } => assert_eq!(slug, "nope/missing"),
            other => panic!("expected UnknownAction, got {other}"),
        }
    }

    #[test]
    fn uses_registered_override_receives_with_inputs() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: act
        uses: test/echo@v3
        with:
          registry: ghcr.io
          tag: ${{ inputs.tag }}
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.inputs = crate::obj([("tag", "v1.2.3")]);
        e.registry
            .register("test/echo", Box::new(FakeAction { slug: "test/echo" }));
        let run = execute_workflow(&wf, &e).unwrap();
        let s = &run.instance("one").unwrap().steps[0];
        assert_eq!(s.outputs.get("slug"), Some(&Value::String("test/echo".into())));
        assert_eq!(s.outputs.get("ref"), Some(&Value::String("v3".into())));
        assert_eq!(
            s.outputs.get("in_registry"),
            Some(&Value::String("ghcr.io".into()))
        );
        // `tag` came in via `${{ inputs.tag }}` — confirms ExprString eval
        // happened against the per-step ctx before the override saw the
        // value.
        assert_eq!(
            s.outputs.get("in_tag"),
            Some(&Value::String("v1.2.3".into()))
        );
    }

    #[test]
    fn uses_tier3_action_requires_native_replacement() {
        // W224 R533-T7: a tier-3 service action (here `actions/upload-artifact`)
        // is no longer reimplemented — it routes through the tier classifier to a
        // Tier3RequiresNative error naming the native QED facility, so the
        // failure says "import this as a native step" instead of running a
        // half-faithful clone of the GitHub service.
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v4
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        let err = execute_workflow(&wf, &e).expect_err("tier-3 must not run");
        match err {
            RuntimeError::Tier3RequiresNative { slug, replacement, stanza } => {
                assert_eq!(slug, "actions/upload-artifact");
                assert!(!replacement.is_empty(), "replacement label present");
                assert!(!stanza.is_empty(), "native stanza hint present");
            }
            other => panic!("expected Tier3RequiresNative, got {other}"),
        }
    }

    #[test]
    fn docker_build_push_without_builder_is_tier3() {
        // R594: with no injected image builder, the docker push family stays a
        // tier-3 error — the bare crate never shells docker.
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: docker/build-push-action@v5
        with:
          push: true
          tags: ghcr.io/yah-ai/x:dev
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        let err = execute_workflow(&wf, &e).expect_err("no builder ⇒ tier-3");
        match err {
            RuntimeError::Tier3RequiresNative { slug, .. } => {
                assert_eq!(slug, "docker/build-push-action");
            }
            other => panic!("expected Tier3RequiresNative, got {other}"),
        }
    }

    #[test]
    fn docker_build_push_with_injected_builder_runs_and_surfaces_outputs() {
        // R594: an injected ImageBuilder handles the docker push family — the
        // step runs (not a tier-3 error) and its outputs (digest/…) flow through
        // to steps.<id>.outputs.* exactly like a toolkit action's.
        use crate::image_builder::{ImageBuildCall, ImageBuilder};
        use crate::toolkit::{StepConclusion, ToolkitOutcome};

        struct FakeBuilder;
        impl ImageBuilder for FakeBuilder {
            fn handle(&self, call: &ImageBuildCall<'_>) -> Result<ToolkitOutcome, String> {
                assert_eq!(call.slug, "docker/build-push-action");
                // The `with:` inputs are evaluated before we see them.
                assert_eq!(
                    call.with.get("tags"),
                    Some(&Value::String("ghcr.io/yah-ai/x:dev".into()))
                );
                let mut outputs = IndexMap::new();
                outputs.insert("digest".into(), Value::String("sha256:deadbeef".into()));
                Ok(ToolkitOutcome {
                    outputs,
                    log: "built".into(),
                    conclusion: StepConclusion::Success,
                })
            }
        }

        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: build
        uses: docker/build-push-action@v5
        with:
          push: true
          tags: ghcr.io/yah-ai/x:dev
"#;
        let wf = workflow(yaml);
        let e = Executor::new(workspace_path())
            .with_image_builder(std::sync::Arc::new(FakeBuilder));
        let run = execute_workflow(&wf, &e).expect("builder handles the step");
        let s = &run.instance("one").unwrap().steps[0];
        assert_eq!(s.conclusion, StepConclusion::Success);
        assert_eq!(
            s.outputs.get("digest"),
            Some(&Value::String("sha256:deadbeef".into()))
        );
    }

    #[test]
    fn uses_same_repo_checkout_is_implicit_noop() {
        // W224: `actions/checkout` against the same repo is implicit on QED —
        // the camp root IS the workspace, so the step is a successful no-op
        // rather than a Tier3RequiresNative error. This keeps a stock release
        // workflow (every job opens with `- uses: actions/checkout@v4`) runnable
        // on QED unchanged while staying valid on GitHub-the-service.
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        let run = execute_workflow(&wf, &e).expect("same-repo checkout no-ops");
        let step = &run.instances[0].steps[0];
        assert_eq!(step.conclusion, StepConclusion::Success);
        assert!(step.stdout.contains("implicit"), "no-op note surfaced: {}", step.stdout);
    }

    #[test]
    fn uses_foreign_repo_checkout_emits_a_native_clone() {
        // R533-T12: a `repository:` input naming another repo can't be the
        // implicit workspace — QED emits an explicit native `git clone` into a
        // subdir of the workspace, honoring `ref` / `path` / `fetch-depth`,
        // instead of the old Tier3RequiresNative refusal.
        let git = |dir: &std::path::Path, args: &[&str]| {
            let ok = Command::new("git")
                .current_dir(dir)
                .args(args)
                .output()
                .unwrap()
                .status
                .success();
            assert!(ok, "git {args:?} failed in {}", dir.display());
        };
        // A local source repo with a committed file, also on branch `release`.
        // An absolute path is treated as a literal clone URL, so the test needs
        // no network.
        let src = tempfile::tempdir().unwrap();
        git(src.path(), &["init", "-b", "main"]);
        git(src.path(), &["config", "user.email", "t@t.t"]);
        git(src.path(), &["config", "user.name", "t"]);
        std::fs::write(src.path().join("hello.txt"), "from-foreign-repo").unwrap();
        git(src.path(), &["add", "."]);
        git(src.path(), &["commit", "-m", "init"]);
        git(src.path(), &["branch", "release"]);

        // A fresh, empty workspace to clone into (not the shared temp_dir).
        let ws = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          repository: {src}
          ref: release
          path: vendored/dep
          fetch-depth: 1
"#,
            src = src.path().display()
        );
        let wf = workflow(&yaml);
        let mut e = Executor::new(ws.path());
        e.env_passthrough = true; // need git on PATH
        let run = execute_workflow(&wf, &e).expect("foreign-repo checkout clones natively");
        let step = &run.instances[0].steps[0];
        assert_eq!(
            step.conclusion,
            StepConclusion::Success,
            "stderr: {}",
            step.stderr
        );
        // `path:` placed the clone under the workspace; `ref:` checked it out.
        let cloned = ws.path().join("vendored/dep/hello.txt");
        assert!(cloned.exists(), "clone landed at the requested path");
        assert_eq!(
            std::fs::read_to_string(&cloned).unwrap(),
            "from-foreign-repo"
        );
    }

    #[test]
    fn foreign_checkout_default_path_is_the_repo_short_name() {
        // No `path:` ⇒ the clone lands in a subdir named after the repo, never
        // the workspace root (which would clobber the run's own positioned tree).
        let git = |dir: &std::path::Path, args: &[&str]| {
            assert!(Command::new("git")
                .current_dir(dir)
                .args(args)
                .output()
                .unwrap()
                .status
                .success());
        };
        let src = tempfile::tempdir().unwrap();
        git(src.path(), &["init", "-b", "main"]);
        git(src.path(), &["config", "user.email", "t@t.t"]);
        git(src.path(), &["config", "user.name", "t"]);
        std::fs::write(src.path().join("f"), "x").unwrap();
        git(src.path(), &["add", "."]);
        git(src.path(), &["commit", "-m", "i"]);
        // Rename the source dir's leaf so the default-path derivation is
        // observable: clone into `<workspace>/<leaf>`.
        let leaf = src
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();

        let ws = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          repository: {src}
"#,
            src = src.path().display()
        );
        let wf = workflow(&yaml);
        let mut e = Executor::new(ws.path());
        e.env_passthrough = true;
        let run = execute_workflow(&wf, &e).expect("clones with default path");
        assert_eq!(run.instances[0].steps[0].conclusion, StepConclusion::Success);
        assert!(
            ws.path().join(&leaf).join("f").exists(),
            "default clone path is the repo short name `{leaf}`"
        );
        // The workspace root itself was not turned into the clone.
        assert!(!ws.path().join("f").exists());
    }

    #[test]
    fn included_instance_keys_skip_non_selected_matrix_rows() {
        // R499-F3 phase 2: an operator picks one row of a matrix; the other
        // rows short-circuit to Skipped (same wire as a GHA `if: false`)
        // so downstream `needs.X.result` aggregation still sees them.
        let yaml = r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [a, b, c]
    steps:
      - run: echo "building ${{ matrix.target }}"
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        e.included_instance_keys = Some(["build#1".to_string()].into_iter().collect());
        let run = execute_workflow(&wf, &e).expect("execute");
        // Three instances scheduled; only row 1 actually executes.
        assert_eq!(run.instances.len(), 3);
        assert_eq!(run.instance_at("build", 0).unwrap().result, JobResult::Skipped);
        assert_eq!(run.instance_at("build", 1).unwrap().result, JobResult::Success);
        assert_eq!(run.instance_at("build", 2).unwrap().result, JobResult::Skipped);
        // Selected row actually ran a step; skipped rows have no steps.
        assert_eq!(run.instance_at("build", 0).unwrap().steps.len(), 0);
        assert_eq!(run.instance_at("build", 1).unwrap().steps.len(), 1);
    }

    fn run_in_fresh_workspace(wf: &Workflow) -> WorkflowRun {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = Executor::new(tmp.path());
        e.env_passthrough = true; // need PATH for bash/coreutils
        e.runner_os = "Linux".into();
        let run = execute_workflow(wf, &e).unwrap_or_else(|err| panic!("execute: {err}"));
        std::mem::forget(tmp); // keep .qed-artifacts alive through assertions
        run
    }

    #[test]
    fn needs_ordered_producer_runs_before_consumer() {
        // R516-B1 happy path (recast off tier-3 artifacts onto run: steps after
        // R533-T7 retired upload/download-artifact): a `needs`-ordered
        // producer→consumer pair both succeed, pinning that the wave scheduler
        // runs the producer before the consumer.
        let yaml = r#"
on: [push]
jobs:
  producer:
    runs-on: ubuntu-latest
    steps:
      - run: echo "payload-bytes" > artifact.txt
  consumer:
    needs: [producer]
    runs-on: ubuntu-latest
    steps:
      - run: echo "consume"
"#;
        let wf = workflow(yaml);
        let run = run_in_fresh_workspace(&wf);
        assert_eq!(run.instance("producer").unwrap().result, JobResult::Success);
        let consumer = run.instance("consumer").unwrap();
        assert_eq!(consumer.result, JobResult::Success);
        assert_eq!(consumer.steps[0].conclusion, StepConclusion::Success);
    }

    #[test]
    fn failed_producer_skips_consumer_via_needs_gate() {
        // R516-B1 regression: when the producer fails, the consumer that
        // `needs` it must be SKIPPED (GHA implicit needs-gate), and the workflow
        // must complete (return Ok), not abort. (Recast off tier-3 artifacts
        // onto run: steps after R533-T7 retired upload/download-artifact.)
        let yaml = r#"
on: [push]
jobs:
  producer:
    runs-on: ubuntu-latest
    steps:
      - run: exit 1
      - run: echo "never reached"
  consumer:
    needs: [producer]
    runs-on: ubuntu-latest
    steps:
      - run: echo "consume"
"#;
        let wf = workflow(yaml);
        let run = run_in_fresh_workspace(&wf);
        assert_eq!(run.instance("producer").unwrap().result, JobResult::Failure);
        // Second step skipped (prior step failed without continue-on-error).
        let producer = run.instance("producer").unwrap();
        assert_eq!(producer.steps[1].conclusion, StepConclusion::Skipped);
        // Consumer skipped by the needs-gate; it never reached its step.
        let consumer = run.instance("consumer").unwrap();
        assert_eq!(consumer.result, JobResult::Skipped);
        assert!(consumer.steps.is_empty(), "consumer must not run any step");
    }

    #[test]
    fn explicit_if_overrides_needs_gate() {
        // A consumer with an explicit `if: always()` opts out of the implicit
        // needs-gate (GHA semantics) and runs even though its producer failed.
        // This is the `publish-*` job shape in release.yml.
        let yaml = r#"
on: [push]
jobs:
  producer:
    runs-on: ubuntu-latest
    steps:
      - run: exit 1
  consumer:
    needs: [producer]
    if: always()
    runs-on: ubuntu-latest
    steps:
      - run: echo "ran anyway"
"#;
        let wf = workflow(yaml);
        let run = run_in_fresh_workspace(&wf);
        assert_eq!(run.instance("producer").unwrap().result, JobResult::Failure);
        assert_eq!(run.instance("consumer").unwrap().result, JobResult::Success);
    }

    #[test]
    fn skip_propagates_via_needs_result() {
        // Job B's `if:` reads `needs.A.result` — A is skipped, B should still
        // run as long as its gate accepts skipped/success.
        let yaml = r#"
on: [push]
jobs:
  a:
    runs-on: ubuntu-latest
    if: false
    steps:
      - run: echo "won't run"
  b:
    needs: [a]
    if: always() && needs.a.result != 'failure'
    runs-on: ubuntu-latest
    steps:
      - run: echo "B ran"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        assert_eq!(run.instance("a").unwrap().result, JobResult::Skipped);
        assert_eq!(run.instance("b").unwrap().result, JobResult::Success);
    }
}
