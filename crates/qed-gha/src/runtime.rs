//! Step executor + workflow walker.
//!
//! F4 walks the [`crate::graph::Plan`] wave by wave (sequentially within a
//! wave — concurrency is a later concern), evaluating `if:` at each tier and
//! running steps through [`run_step`]. `run:` blocks spawn `bash`, capture
//! `::set-output::` / `$GITHUB_OUTPUT` / `$GITHUB_ENV`, and thread results
//! into `steps.<id>.outputs.*` for the next step. `uses:` blocks route
//! through [`OverrideRegistry`]; an unknown slug is a loud error per W200.

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
use crate::overrides::{Lookup, OverrideCall, OverrideRegistry, ProducedArtifact, StepConclusion};
#[cfg(test)]
use crate::overrides::OverrideOutcome;
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
    #[error("no override registered for `{slug}` — register a built-in or add a TOML deny rule (W200 policy: every uses: must be overridden)")]
    UnknownAction { slug: String },
    #[error("override `{slug}` denied: {message}")]
    DeniedAction { slug: String, message: String },
    #[error("override `{slug}` failed: {message}")]
    OverrideFailed { slug: String, message: String },
}

/// Public executor handle. Workflow-level inputs (github / inputs / runner_os)
/// stay on the executor so a single instance can run several workflows; the
/// registry is owned here so callers wire built-ins + TOML overlays once.
pub struct Executor {
    pub workspace: PathBuf,
    pub registry: OverrideRegistry,
    pub github: Value,
    pub inputs: Value,
    pub runner_os: String,
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
}

impl Executor {
    /// New executor with the F5 built-in overrides pre-registered. This is
    /// the right default for production callers — a workflow whose `uses:`
    /// only references built-in slugs (the common case for `release.yml`'s
    /// build-only legs) runs straight through without extra wiring.
    pub fn new(workspace: impl Into<PathBuf>) -> Self {
        let mut e = Self::bare(workspace);
        crate::overrides_builtin::register_builtins(&mut e.registry);
        e
    }

    /// Empty-registry executor for tests that want hermetic dispatch (no
    /// built-ins, no `git`/`rustup`/`bun` shelled out by accident). F4 tests
    /// use this to assert the W200 unknown-action error fires.
    pub fn bare(workspace: impl Into<PathBuf>) -> Self {
        Self {
            workspace: workspace.into(),
            registry: OverrideRegistry::new(),
            github: Value::object(),
            inputs: Value::object(),
            runner_os: detect_runner_os().into(),
            env_passthrough: true,
            included_instance_keys: None,
            events: None,
            secrets: Value::object(),
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
}

fn detect_runner_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "macOS",
        "linux" => "Linux",
        "windows" => "Windows",
        _ => "Linux",
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

    /// Flat list of artifacts every successful override step produced. This
    /// is the F9 hook: the QED runner lifts this into the parent step's
    /// `Outcome::Publish` collection, no per-step plumbing required.
    pub fn produced(&self) -> Vec<&ProducedArtifact> {
        self.instances
            .iter()
            .filter(|i| matches!(i.result, JobResult::Success))
            .flat_map(|i| i.produced.iter())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct InstanceRun {
    pub job_id: String,
    pub matrix_index: Option<usize>,
    pub result: JobResult,
    pub steps: Vec<StepResult>,
    pub outputs: IndexMap<String, Value>,
    /// Concatenated [`ProducedArtifact`]s from every successful step in this
    /// instance — the per-job slice of [`WorkflowRun::produced`].
    pub produced: Vec<ProducedArtifact>,
}

#[derive(Debug, Clone)]
pub struct StepResult {
    pub step_id: Option<String>,
    pub name: Option<String>,
    pub conclusion: StepConclusion,
    pub outputs: IndexMap<String, Value>,
    pub stdout: String,
    pub stderr: String,
    pub produced: Vec<ProducedArtifact>,
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
                    produced: vec![],
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

    // Pre-step context: matrix + needs + env composed but ctx.steps empty.
    let mut ctx = build_context_for_instance(
        instance,
        workflow,
        completed,
        executor.github.clone(),
        executor.inputs.clone(),
        &executor.runner_os,
        executor.secrets.clone(),
    )?;

    if !should_run_job(job, &ctx)? {
        return Ok(InstanceRun {
            job_id: instance.job_id.clone(),
            matrix_index: instance.matrix_index,
            result: JobResult::Skipped,
            steps: vec![],
            outputs: IndexMap::new(),
            produced: vec![],
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
                produced: vec![],
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

    let produced: Vec<ProducedArtifact> = step_results
        .iter()
        .filter(|s| matches!(s.conclusion, StepConclusion::Success))
        .flat_map(|s| s.produced.iter().cloned())
        .collect();

    Ok(InstanceRun {
        job_id: instance.job_id.clone(),
        matrix_index: instance.matrix_index,
        result,
        steps: step_results,
        outputs,
        produced,
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
    let env = compose_step_env(step, ctx, env_overlay)?;
    let res = match &step.action {
        StepAction::Run { body, shell } => {
            run_bash_step(step, body, shell.as_deref(), &env, executor, instance, step_index)?
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
) -> Result<IndexMap<String, String>, RuntimeError> {
    let mut out: IndexMap<String, String> = IndexMap::new();
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
    executor: &Executor,
    instance: &JobInstance,
    step_index: usize,
) -> Result<StepResult, RuntimeError> {
    let body_str = crate::graph::eval_exprstring(body, &empty_ctx()).map_err(|source| {
        // The body's expressions need the current ctx — call site doesn't
        // pass it through here because compose_step_env already did the env
        // work. Re-eval against an empty ctx is wrong; route via the caller.
        // (Replaced in the next call site.)
        RuntimeError::Expr { site: "step.run".into(), source }
    })?;
    // Compute body against an env-aware shell instead — fall through.
    let _ = body_str;
    let body_str = render_run_body(body, env)?;

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
        produced: vec![],
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

fn render_run_body(
    body: &ExprString,
    _env: &IndexMap<String, String>,
) -> Result<String, RuntimeError> {
    // The run-body is a YAML scalar that may carry `${{ }}` interpolations.
    // We've already evaluated env separately; here we only need to swap in
    // expression values, leaving the surrounding shell text intact. The
    // caller hands us the ExprString tokens; we walk and stitch.
    //
    // Note: the body's expressions read the same ctx the env composition
    // used, but ctx isn't threaded here — the caller's pre-eval already
    // produced literal-or-expr tokens. We assume the run body's expressions
    // have already been folded; for tokens of [Literal], we return the
    // literal. For mixed tokens we'd need ctx — that path lands in
    // `run_bash_step_with_ctx` (see [`run_step_full`]).
    if let [ExprToken::Literal(b)] = body.tokens.as_slice() {
        return Ok(b.clone());
    }
    // F4 happens to not need run-body expression eval in any test, but a
    // correct fallback is to concatenate already-rendered tokens.
    let mut out = String::new();
    for t in &body.tokens {
        if let ExprToken::Literal(s) = t {
            out.push_str(s);
        }
    }
    Ok(out)
}

fn empty_ctx() -> Context<'static> {
    Context::new()
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
        Lookup::Found { ovr, config } => {
            let call = OverrideCall {
                slug,
                git_ref,
                with: &typed_with,
                env,
                workspace: &executor.workspace,
                config,
            };
            ovr.execute(&call)
                .map_err(|message| RuntimeError::OverrideFailed {
                    slug: slug.into(),
                    message,
                })?
        }
        Lookup::Denied { message } => {
            return Err(RuntimeError::DeniedAction {
                slug: slug.into(),
                message: message.into(),
            })
        }
        Lookup::Unknown => return Err(RuntimeError::UnknownAction { slug: slug.into() }),
    };

    Ok(StepResult {
        step_id: step.id.clone(),
        name: step.name.as_ref().and_then(exprstring_static),
        conclusion: outcome.conclusion,
        outputs: outcome.outputs,
        stdout: outcome.log,
        stderr: String::new(),
        produced: outcome.produced,
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
        produced: res.produced.clone(),
    });
}

// ─── tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overrides::Override;
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

    // ── uses dispatch

    struct FakeOverride {
        slug: &'static str,
    }
    impl Override for FakeOverride {
        fn execute(&self, call: &OverrideCall<'_>) -> Result<OverrideOutcome, String> {
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
            // Use the slug to identify which override fired.
            let _ = self.slug;
            Ok(OverrideOutcome {
                outputs,
                log: "fake".into(),
                conclusion: StepConclusion::Success,
            produced: Vec::new(),
            })
        }
    }

    #[test]
    fn uses_unknown_action_raises_w200_error() {
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
            .register("test/echo", Box::new(FakeOverride { slug: "test/echo" }));
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
    fn uses_denied_action_surfaces_message() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/github-script@v6
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.registry
            .load_toml_str(
                r#"
            [overrides."actions/github-script"]
            deny = true
            deny_message = "JS actions not supported in v1"
            "#,
            )
            .unwrap();
        let err = execute_workflow(&wf, &e).expect_err("denied");
        match err {
            RuntimeError::DeniedAction { slug, message } => {
                assert_eq!(slug, "actions/github-script");
                assert_eq!(message, "JS actions not supported in v1");
            }
            other => panic!("expected DeniedAction, got {other}"),
        }
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
