//! Job graph: topological wave scheduling + matrix expansion + the helpers
//! `if:` evaluation and outputs propagation hang off.
//!
//! No step execution lives here — F3 proves order, matrix fan-out, and the
//! `needs.<job>.outputs.<key>` / `needs.<job>.result` propagation against a
//! synthetic completion driver. F4 wires the step executor on top.
//!
//! @yah:ticket(R654-T1, "Populate runner.environment (github-hosted | self-hosted) in the GHA expression context")
//! @yah:status(review)
//! @yah:at(2026-08-01T23:44:06Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R654)
//! @yah:next("GHA's runner context carries `environment` (github-hosted | self-hosted) alongside os/arch. qed-gha's build_context_for_instance sets only {os, arch}, so a workflow cannot gate hosted-runner-shape steps to no-op under `yah qed run`. Add the field, populate it from RUNNER_ENVIRONMENT when present else self-hosted, and export RUNNER_ENVIRONMENT to step subprocesses next to RUNNER_OS/RUNNER_ARCH.")
//! @yah:next("DO NOT use `if: env.GITHUB_ACTIONS == 'true'` as the workaround — qed-gha builds the if= env context from workflow.env + job.env only, and GitHub's own env context likewise excludes default variables. A real context field is the fix.")
//! @yah:verify("cargo test -p yah-qed-gha — a step gated `if: runner.environment == 'github-hosted'` is skipped under the executor, and its `!=` sibling runs.")
//! @yah:assumes("Tier: Cleric — one context field plus a populate site.")
//! @yah:handoff("LANDED. `runner.environment` is now a real context field. graph.rs: new `RunnerInfo { os, arch, environment }` replaces the two adjacent `&str` params on build_context_for_instance (three transposable strings in a row was the shape to avoid), and ctx.runner is built as {os, arch, environment}. runtime.rs: `Executor.runner_environment`, defaulted by `detect_runner_environment()`, plus `RUNNER_ENVIRONMENT` exported to every step subprocess next to RUNNER_OS/RUNNER_ARCH so a `run:` body can branch on it too. lib.rs re-exports RunnerInfo.")
//! @yah:handoff("Detection is keyed off the `RUNNER_ENVIRONMENT` env var GitHub's own runner exports, NOT off GITHUB_ACTIONS: that variable only says 'some GHA runner is involved' and a self-hosted GHA runner sets it too. So QED inside a hosted job honestly reports `github-hosted`; everywhere else (dev box, fleet slot) it reports `self-hosted`.")
//! @yah:handoff("No qed-runner change needed: Executor::bare detects at construction, and a remote fleet runner constructs its own Executor on the remote host, so the detection is correct on both sides of a dispatch.")
//! @yah:handoff("VERIFIED: cargo test -p yah-qed-gha = 112/112, zero warnings; cargo build --workspace in oss/qed clean. Three new tests: self_hosted_runner_environment_skips_the_hosted_only_step, github_hosted_runner_environment_runs_the_hosted_only_step (both directions of the relay's verify line, asserting StepConclusion::Skipped/Success on the same workflow), runner_environment_defaults_to_self_hosted_off_a_github_runner.")
//! @yah:handoff("DISCOVERED + FIXED IN PASS: removed a dead `fn executor()` test helper in runtime.rs (pre-existing dead_code warning, unused since before HEAD a9bf307d, and it did nothing coherent with the PATH it read).")
//! @yah:verify("cargo test -p yah-qed-gha runner_environment — 3/3 pass (VERIFIED)")
//!
//! @yah:ticket(R654-F2, "Evaluate ${{ }} expressions in strategy.matrix dimension position (dynamic matrix)")
//! @yah:status(review)
//! @yah:at(2026-08-01T23:57:08Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R654)
//! @yah:next("workflow::Matrix.dimensions is IndexMap<String, Vec<Value>> with no expression pass, so the standard GHA idiom for a caller-narrowed matrix (`board: ${{ fromJSON(inputs.board && format(...) || '[...]') }}`) cannot work — the dimension is not even a list before evaluation. fromJSON itself IS implemented in expr.rs, so this is specifically about evaluating expressions in the matrix position.")
//! @yah:next("Evaluation ordering is the real design work: matrix dimensions are evaluated at expansion time (plan()), BEFORE any per-instance context exists, so the expression pass can only see github/inputs/vars/needs — not matrix/steps/runner/env. Decide and document that restricted context rather than reusing build_context_for_instance.")
//! @yah:next("Worked around for noisetable by the value-based Executor.matrix_filter (see R653-T3), which solves SELECTION but not a genuinely dynamic matrix. That workaround stays; this ticket is about the dynamic case.")
//! @yah:verify("A workflow whose `strategy.matrix.board` is `${{ fromJSON(inputs.board && format('[\"{0}\"]', inputs.board) || '[\"a\",\"b\"]') }}` expands to one instance when inputs.board is set and two when it is not.")
//! @yah:assumes("Tier: Warrior — an expression pass over matrix dimensions, with expansion-time evaluation ordering to settle.")
//! @yah:notify_on(R653, "R653-F1 added `Context::extra` + `Context::with_namespace` (host-defined namespace roots, used for qed's `params.*`). R654-F2's `PlanContext` deliberately carries only {github, inputs, vars} and builds a fresh Context, so `params.*` is NOT visible in a matrix dimension expression. Once R653's params plumbing is settled, decide whether PlanContext grows a passthrough for host namespaces — a qed pipeline narrowing a wrapped workflow's matrix from a run param is the obvious next ask, and today it has to route through the gha-workflow step's `inputs:` map instead.")
//! @yah:handoff("LANDED. Matrix dimensions, include rows and exclude rows all go through an expression pass before expansion. `board: ${{ fromJSON(inputs.board && format('[\"{0}\"]', inputs.board) || '[\"a\",\"b\"]') }}` now expands to 1 instance when inputs.board is set and 2 when it is not — the relay's verify line, asserted at both the expand_matrix level and end-to-end through execute_workflow.")
//! @yah:handoff("EVALUATION ORDERING RESOLVED with a new narrow type, `graph::PlanContext { github, inputs, vars }`, rather than reusing Context. Expansion happens in plan(), up front, so matrix/steps/env/runner/needs are STRUCTURALLY unavailable, not merely unimplemented — PlanContext says that at the call site instead of handing the pass a Context whose other nine fields are silently empty. `secrets` is excluded on purpose, matching GHA's own context-availability table for `strategy`. (Real GHA evaluates a job's matrix at job start and so does admit needs.*; QED plans the whole graph up front, which is what makes the wave scheduler and the dashboard row picker possible. needs.* in a matrix would be a re-plan-per-wave change, not a context-widening one — documented on PlanContext.)")
//! @yah:handoff("SPLICE RULE: an entry that is an EXPRESSION and evaluates to an array becomes that dimension's list of values; anything else contributes one value. Gating on expression-ness is what keeps a literal array-valued dimension (`pair: [[1,2],[3,4]]`, legal GHA) from flattening — covered by literal_array_dimension_values_are_not_spliced.")
//! @yah:handoff("SIGNATURE CHANGES (all call sites in-tree, no yah-side consumers): `plan(&Workflow, &PlanContext)`, `expand_matrix(&Matrix, &PlanContext) -> Result<Vec<Value>, GraphError>`. Expression failures abort the plan before any job runs and name their site (`strategy.matrix.board[0]`), since that message is all the operator gets.")
//! @yah:handoff("The R653-T3 value-based Executor.matrix_filter is untouched and still composes — it narrows by VALUE after expansion; this narrows the fan-out at expansion.")
//! @yah:handoff("VERIFIED: cargo test -p yah-qed-gha 125/125 zero warnings; cargo build+test --workspace in oss/qed green (one flake, yah-qed waitfor::tcp_probe_fails_against_a_dead_port, failed once under full-suite parallelism and passes in isolation and on rerun — a port race, unrelated); cargo check -p yah at repo root clean, confirming the signature change reaches no yah-side caller.")
//! @yah:handoff("DIVERGENCE FOUND, LEFT AS-IS AND DOCUMENTED: an EMPTY dimension (literal `board: []`, or an expression evaluating to `[]`) contributes no key rather than zeroing the product, so the job still gets one row with that dimension absent. That is cartesian()'s pre-existing empty-`vs` arm; GHA instead hard-errors 'matrix vector does not contain any values'. It mattered less when only a typo could reach it — a dynamic matrix reaches it at runtime. Documented on expand_matrix and pinned by matrix_expressions_cannot_see_the_per_instance_context. Changing it is a semantics call, not a bug fix, so it is flagged not taken.")
//! @yah:verify("cargo test -p yah-qed-gha (from oss/qed) — 125/125 including dynamic_matrix_dimension_narrows_from_inputs, dynamic_matrix_dimension_falls_back_to_the_full_list, dynamic_matrix_narrows_the_whole_plan_not_just_the_rows, dynamic_matrix_narrows_the_executed_fan_out_from_inputs, include_and_exclude_rows_evaluate_expressions_too, literal_array_dimension_values_are_not_spliced, matrix_expression_error_names_its_site — VERIFIED")
//! @yah:cleanup("Decide the empty-matrix-dimension semantics: today one dimensionless row, GHA hard-errors. Reachable at runtime now that dimensions can be expressions.")

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use thiserror::Error;

use crate::expr::{self, obj, Context, ExprError, Value};
use crate::expr_str::{ExprString, ExprToken};
use crate::workflow::{Job, Matrix, Workflow};

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("unknown job referenced in needs: `{0}`")]
    UnknownNeeds(String),
    #[error("cycle detected; remaining jobs: {0:?}")]
    Cycle(Vec<String>),
    #[error("expression error in {site}: {source}")]
    Expr {
        site: String,
        #[source]
        source: ExprError,
    },
}

// ─── plan ──────────────────────────────────────────────────────────────────

/// Concrete schedulable unit: one job, optionally one row of its matrix.
#[derive(Debug, Clone)]
pub struct JobInstance {
    pub job_id: String,
    /// `None` for non-matrix jobs. For matrix jobs this is a [`Value::Object`]
    /// holding the per-row variables (so the evaluator can resolve
    /// `matrix.target` etc.).
    pub matrix: Option<Value>,
    /// Stable index into the expanded matrix, used by [`JobInstance::key`] to
    /// disambiguate parallel rows.
    pub matrix_index: Option<usize>,
}

impl JobInstance {
    /// Stable id for `needs.*` lookup. For matrix jobs the rows aggregate
    /// upward — `needs.X.result` is `failure` if *any* row failed — so the
    /// per-row key here is internal, not exposed to expressions.
    pub fn key(&self) -> String {
        match self.matrix_index {
            Some(i) => format!("{}#{}", self.job_id, i),
            None => self.job_id.clone(),
        }
    }
}

/// Topologically-sorted, matrix-expanded execution plan.
#[derive(Debug, Clone, Default)]
pub struct Plan {
    /// Jobs grouped by wave; within a wave every job's needs are already met.
    pub waves: Vec<Vec<JobInstance>>,
}

impl Plan {
    pub fn iter_instances(&self) -> impl Iterator<Item = &JobInstance> {
        self.waves.iter().flat_map(|w| w.iter())
    }
}

/// Everything a `strategy.matrix` expression is allowed to see.
///
/// Matrix expansion happens in [`plan`] — up front, before any job has run and
/// before any per-instance context exists. So `matrix.*`, `steps.*`, `env.*`,
/// `runner.*` and `needs.*` are *structurally* unavailable at this point, not
/// merely unimplemented, and this narrow type says so at the call site instead
/// of handing the expansion pass a full [`Context`] whose other nine fields
/// would be silently empty.
///
/// (GHA evaluates a job's matrix when the job starts, so real GHA *does* admit
/// `needs.*` there. QED plans the whole graph up front — that's what makes the
/// wave scheduler and the dashboard's row picker possible. Supporting
/// `needs.*` in a matrix would be a re-plan-per-wave change, not a
/// context-widening one; `secrets.*` is excluded on purpose, matching GHA's
/// own context-availability table for `strategy`.)
#[derive(Debug, Clone, Default)]
pub struct PlanContext {
    pub github: Value,
    pub inputs: Value,
    pub vars: Value,
}

impl PlanContext {
    fn to_context<'h>(&self) -> Context<'h> {
        let mut ctx = Context::new();
        ctx.github = self.github.clone();
        ctx.inputs = self.inputs.clone();
        ctx.vars = self.vars.clone();
        ctx
    }
}

pub fn plan(workflow: &Workflow, plan_ctx: &PlanContext) -> Result<Plan, GraphError> {
    let waves = topo_sort(workflow)?;
    let mut out = Plan::default();
    for wave in waves {
        let mut row = Vec::new();
        for job_id in wave {
            let job = workflow
                .jobs
                .get(&job_id)
                .expect("topo_sort never returns unknown job ids");
            let instances = match job.strategy.as_ref().and_then(|s| s.matrix.as_ref()) {
                Some(m) => {
                    let rows = expand_matrix(m, plan_ctx)?;
                    if rows.is_empty() {
                        // Matrix block present but resolved to zero rows (all
                        // dimensions empty + no include). Still schedule one
                        // instance with no matrix so the consumer sees the
                        // job; this matches GHA's "skip with note" behavior.
                        vec![JobInstance {
                            job_id: job_id.clone(),
                            matrix: None,
                            matrix_index: None,
                        }]
                    } else {
                        rows.into_iter()
                            .enumerate()
                            .map(|(i, m)| JobInstance {
                                job_id: job_id.clone(),
                                matrix: Some(m),
                                matrix_index: Some(i),
                            })
                            .collect()
                    }
                }
                None => vec![JobInstance {
                    job_id: job_id.clone(),
                    matrix: None,
                    matrix_index: None,
                }],
            };
            row.extend(instances);
        }
        out.waves.push(row);
    }
    Ok(out)
}

// ─── topo sort ─────────────────────────────────────────────────────────────

/// Kahn's algorithm — emits jobs in waves so the caller can run a wave in
/// parallel. Cycles produce [`GraphError::Cycle`] with the unresolved set.
pub fn topo_sort(workflow: &Workflow) -> Result<Vec<Vec<String>>, GraphError> {
    let mut indeg: HashMap<String, usize> = workflow
        .jobs
        .keys()
        .map(|k| (k.clone(), 0))
        .collect();
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();

    for (id, job) in &workflow.jobs {
        for dep in &job.needs {
            if !workflow.jobs.contains_key(dep) {
                return Err(GraphError::UnknownNeeds(dep.clone()));
            }
            adj.entry(dep.clone()).or_default().push(id.clone());
            *indeg.get_mut(id).unwrap() += 1;
        }
    }

    let mut waves: Vec<Vec<String>> = Vec::new();
    let mut ready: Vec<String> = workflow
        .jobs
        .keys()
        .filter(|k| indeg[*k] == 0)
        .cloned()
        .collect();

    while !ready.is_empty() {
        // Preserve declaration order within a wave for diff-stable output.
        ready.sort_by_key(|id| workflow.jobs.get_index_of(id).unwrap_or(usize::MAX));
        let next: Vec<String> = ready.drain(..).collect();
        let mut new_ready = Vec::new();
        for id in &next {
            if let Some(children) = adj.get(id) {
                for c in children {
                    let d = indeg.get_mut(c).unwrap();
                    *d -= 1;
                    if *d == 0 {
                        new_ready.push(c.clone());
                    }
                }
            }
        }
        waves.push(next);
        ready = new_ready;
    }

    let remaining: Vec<String> = indeg
        .into_iter()
        .filter_map(|(k, v)| if v > 0 { Some(k) } else { None })
        .collect();
    if !remaining.is_empty() {
        return Err(GraphError::Cycle(remaining));
    }
    Ok(waves)
}

// ─── matrix expansion ──────────────────────────────────────────────────────

/// Expand `strategy.matrix` into a flat list of matrix rows. Each row is a
/// `Value::Object` so it can drop straight into [`Context::matrix`].
///
/// GHA semantics:
///   1. Take the cartesian product of dimensions (in declaration order so the
///      output is stable).
///   2. Apply `include:` rows. Each include row either (a) merges into an
///      existing combination — extending it with non-conflicting new keys
///      when all of its original-dimension keys match — or (b) appends as a
///      standalone row when no merge target exists or the include defines no
///      original-dimension keys.
///   3. Drop any row matching an `exclude:` entry (all listed keys equal).
///
/// Every scalar in the matrix — dimension value, `include:` value, `exclude:`
/// value — is first run through the expression pass described on
/// [`eval_matrix_scalar`], so `board: ${{ fromJSON(...) }}` resolves to a real
/// list of values before the cartesian product sees it.
///
/// A dimension that ends up EMPTY (a literal `board: []`, or an expression
/// that evaluates to `[]`) contributes no key to the product rather than
/// zeroing it — the job still gets one row, with that dimension absent. That
/// predates the expression pass; it's [`cartesian`]'s empty-`vs` arm. GHA
/// instead hard-errors ("matrix vector does not contain any values"), so this
/// is a divergence worth revisiting now that an empty dimension is reachable
/// at runtime and not just from a typo.
pub fn expand_matrix(matrix: &Matrix, plan_ctx: &PlanContext) -> Result<Vec<Value>, GraphError> {
    let ctx = plan_ctx.to_context();

    // Step 1 — cartesian product over dimensions.
    let dim_keys: Vec<String> = matrix.dimensions.keys().cloned().collect();
    let mut dim_vals: Vec<Vec<Value>> = Vec::with_capacity(dim_keys.len());
    for (key, seq) in &matrix.dimensions {
        let mut vals = Vec::with_capacity(seq.len());
        for (i, raw) in seq.iter().enumerate() {
            let site = format!("strategy.matrix.{key}[{i}]");
            match eval_matrix_scalar(raw, &ctx, &site)? {
                // An *expression* that produced a list IS this dimension's
                // list of values — that's the whole `fromJSON(...)` idiom, and
                // the parser has already flattened `board: <scalar>` into a
                // one-element Vec, so the splice happens here. A LITERAL YAML
                // list stays nested (`pair: [[1,2],[3,4]]` is a legal GHA
                // matrix of two array-valued rows), which is why this arm is
                // gated on the source having been an expression.
                Value::Array(items) if expr_scalar(raw).is_some() => vals.extend(items),
                other => vals.push(other),
            }
        }
        dim_vals.push(vals);
    }

    let mut rows: Vec<IndexMap<String, Value>> = if dim_keys.is_empty() {
        vec![]
    } else {
        cartesian(&dim_keys, &dim_vals)
    };

    // Step 2 — apply include rows.
    let original_keys: HashSet<&str> = dim_keys.iter().map(|s| s.as_str()).collect();
    for (i, inc) in matrix.include.iter().enumerate() {
        let mut inc_obj: IndexMap<String, Value> = IndexMap::new();
        for (k, v) in inc {
            let site = format!("strategy.matrix.include[{i}].{k}");
            inc_obj.insert(k.clone(), eval_matrix_scalar(v, &ctx, &site)?);
        }

        if dim_keys.is_empty() {
            // No dimensions to merge against — include becomes a standalone row.
            rows.push(inc_obj);
            continue;
        }

        // Split include keys into original-dimension keys vs new keys.
        let (orig_part, new_part): (
            IndexMap<String, Value>,
            IndexMap<String, Value>,
        ) = inc_obj
            .into_iter()
            .partition(|(k, _)| original_keys.contains(k.as_str()));

        if orig_part.is_empty() {
            // No anchor — GHA appends this as a separate combination.
            rows.push(new_part);
            continue;
        }

        let mut matched_any = false;
        for row in rows.iter_mut() {
            if orig_part.iter().all(|(k, v)| row.get(k) == Some(v)) {
                matched_any = true;
                for (k, v) in &new_part {
                    // Don't overwrite an existing key already produced by the
                    // cartesian product; GHA's docs phrase this as
                    // "without overwriting any of the original matrix values".
                    if !row.contains_key(k) {
                        row.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        if !matched_any {
            // No anchor matched — append as a standalone row carrying both
            // the original-part and new-part keys.
            let mut row = IndexMap::new();
            row.extend(orig_part);
            row.extend(new_part);
            rows.push(row);
        }
    }

    // Step 3 — drop excluded rows.
    if !matrix.exclude.is_empty() {
        let mut excludes: Vec<IndexMap<String, Value>> = Vec::with_capacity(matrix.exclude.len());
        for (i, ex) in matrix.exclude.iter().enumerate() {
            let mut ex_obj = IndexMap::new();
            for (k, v) in ex {
                let site = format!("strategy.matrix.exclude[{i}].{k}");
                ex_obj.insert(k.clone(), eval_matrix_scalar(v, &ctx, &site)?);
            }
            excludes.push(ex_obj);
        }
        rows.retain(|row| {
            !excludes
                .iter()
                .any(|ex| ex.iter().all(|(k, ev)| row.get(k.as_str()) == Some(ev)))
        });
    }

    Ok(rows.into_iter().map(Value::Object).collect())
}

/// The `${{ … }}` source of a YAML scalar, when it has one. A matrix entry
/// that isn't a string, or a string with no expression block, returns `None`
/// and is taken literally.
fn expr_scalar(v: &serde_yaml::Value) -> Option<&str> {
    match v {
        serde_yaml::Value::String(s) if s.contains("${{") => Some(s),
        _ => None,
    }
}

/// Lower one matrix entry to a [`Value`], evaluating any `${{ … }}` it
/// carries against the expansion-time context.
///
/// A single-expression scalar keeps its evaluated *type* — this is what makes
/// `board: ${{ fromJSON('["a","b"]') }}` a list rather than the string
/// `"[a, b]"` — while a scalar that mixes literal text with expressions
/// concatenates, per the usual [`eval_exprstring`] rule. Nested sequences and
/// mappings recurse, so an `include:` row can carry an expression inside a
/// nested value.
fn eval_matrix_scalar(
    v: &serde_yaml::Value,
    ctx: &Context,
    site: &str,
) -> Result<Value, GraphError> {
    match v {
        serde_yaml::Value::String(s) if s.contains("${{") => {
            eval_exprstring(&ExprString::parse(s), ctx).map_err(|source| GraphError::Expr {
                site: site.to_string(),
                source,
            })
        }
        serde_yaml::Value::Sequence(seq) => {
            let mut out = Vec::with_capacity(seq.len());
            for (i, item) in seq.iter().enumerate() {
                out.push(eval_matrix_scalar(item, ctx, &format!("{site}[{i}]"))?);
            }
            Ok(Value::Array(out))
        }
        serde_yaml::Value::Mapping(map) => {
            let mut out = IndexMap::new();
            for (k, item) in map {
                let Some(k) = k.as_str() else { continue };
                out.insert(
                    k.to_string(),
                    eval_matrix_scalar(item, ctx, &format!("{site}.{k}"))?,
                );
            }
            Ok(Value::Object(out))
        }
        other => Ok(yaml_to_value(other)),
    }
}

fn cartesian(keys: &[String], vals: &[Vec<Value>]) -> Vec<IndexMap<String, Value>> {
    let mut out = vec![IndexMap::<String, Value>::new()];
    for (k, vs) in keys.iter().zip(vals.iter()) {
        let mut next = Vec::with_capacity(out.len() * vs.len().max(1));
        for row in &out {
            if vs.is_empty() {
                next.push(row.clone());
                continue;
            }
            for v in vs {
                let mut nr = row.clone();
                nr.insert(k.clone(), v.clone());
                next.push(nr);
            }
        }
        out = next;
    }
    out
}

/// Lossy yaml→value lowering for matrix entries. Scalars round-trip
/// faithfully; sequences/mappings drop through since the matrix audit doesn't
/// use nested matrix values today.
fn yaml_to_value(v: &serde_yaml::Value) -> Value {
    match v {
        serde_yaml::Value::Null => Value::Null,
        serde_yaml::Value::Bool(b) => Value::Bool(*b),
        serde_yaml::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Number(i as f64)
            } else if let Some(f) = n.as_f64() {
                Value::Number(f)
            } else {
                Value::Null
            }
        }
        serde_yaml::Value::String(s) => Value::String(s.clone()),
        serde_yaml::Value::Sequence(seq) => Value::Array(seq.iter().map(yaml_to_value).collect()),
        serde_yaml::Value::Mapping(map) => {
            let mut out = IndexMap::new();
            for (k, v) in map {
                if let Some(k) = k.as_str() {
                    out.insert(k.to_string(), yaml_to_value(v));
                }
            }
            Value::Object(out)
        }
        serde_yaml::Value::Tagged(t) => yaml_to_value(&t.value),
    }
}

// ─── completion + needs propagation ────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobResult {
    Success,
    Failure,
    Cancelled,
    Skipped,
}

impl JobResult {
    pub fn as_str(self) -> &'static str {
        match self {
            JobResult::Success => "success",
            JobResult::Failure => "failure",
            JobResult::Cancelled => "cancelled",
            JobResult::Skipped => "skipped",
        }
    }

    /// Aggregation rule for matrix rows of the same job (`needs.X.result`):
    /// any failure wins, then cancelled, then skipped, else success.
    pub fn aggregate<I: IntoIterator<Item = JobResult>>(rows: I) -> Self {
        let mut seen_success = false;
        let mut seen_failure = false;
        let mut seen_cancelled = false;
        let mut seen_any = false;
        for r in rows {
            seen_any = true;
            match r {
                JobResult::Failure => seen_failure = true,
                JobResult::Cancelled => seen_cancelled = true,
                JobResult::Success => seen_success = true,
                JobResult::Skipped => {}
            }
        }
        if !seen_any {
            JobResult::Skipped
        } else if seen_failure {
            JobResult::Failure
        } else if seen_cancelled {
            JobResult::Cancelled
        } else if seen_success {
            JobResult::Success
        } else {
            // All rows were skipped.
            JobResult::Skipped
        }
    }
}

/// A completed (or skipped) job instance feeding `needs.*` for downstream
/// jobs. F3 doesn't decide what's in here — F4+ derives it from real step
/// execution. F3 lets callers stitch it together for tests.
#[derive(Debug, Clone)]
pub struct CompletedInstance {
    pub job_id: String,
    pub matrix_index: Option<usize>,
    pub result: JobResult,
    /// Job-level outputs after their `ExprString` templates have been
    /// evaluated against the job's own steps-context. Aggregated to a single
    /// map per job_id when feeding `needs.<job>.outputs`.
    pub outputs: IndexMap<String, Value>,
}

/// Compose the `needs:` namespace as a `Value::Object`. Rows of the same job
/// merge: outputs are unioned (later instances clobber earlier on key
/// collision; GHA's behavior is implementation-defined here, last-write
/// keeps the rule simple); `result` is aggregated per [`JobResult::aggregate`].
pub fn build_needs_value(completed: &[CompletedInstance]) -> Value {
    let mut grouped: IndexMap<String, (Vec<JobResult>, IndexMap<String, Value>)> =
        IndexMap::new();
    for c in completed {
        let entry = grouped
            .entry(c.job_id.clone())
            .or_insert_with(|| (Vec::new(), IndexMap::new()));
        entry.0.push(c.result);
        for (k, v) in &c.outputs {
            entry.1.insert(k.clone(), v.clone());
        }
    }
    let mut out = IndexMap::new();
    for (job_id, (results, outputs)) in grouped {
        let agg = JobResult::aggregate(results.iter().copied());
        let mut entry = IndexMap::new();
        entry.insert("result".to_string(), Value::String(agg.as_str().into()));
        entry.insert("outputs".to_string(), Value::Object(outputs));
        out.insert(job_id, Value::Object(entry));
    }
    Value::Object(out)
}

// ─── per-instance context builder + if/output evaluation ───────────────────

/// The `runner.*` context surface, as one value. GHA exposes `runner.os`,
/// `runner.arch`, and `runner.environment`; they always travel together into
/// the evaluator, and three adjacent `&str` parameters is a shape you can
/// transpose without the compiler noticing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunnerInfo<'a> {
    /// `Linux` / `macOS` / `Windows`.
    pub os: &'a str,
    /// `X64` / `ARM64` / `X86` / `ARM`.
    pub arch: &'a str,
    /// `github-hosted` when a GitHub-provided runner is executing the job,
    /// `self-hosted` otherwise — which is what QED reports, since a QED run
    /// is by definition not on a GitHub-hosted runner. This is the field a
    /// workflow gates its runner-shape steps on (relocating Docker's storage
    /// onto the hosted scratch volume, `sudo apt-get install` of a hosted
    /// image's missing packages) so they no-op off GitHub.
    pub environment: &'a str,
}

/// Build the evaluator context for one [`JobInstance`] at scheduling time.
/// Caller supplies the workflow's `github` / `inputs` snapshot; this helper
/// stitches in `matrix`, `needs`, `env`, and `runner.{os,arch,environment}`.
pub fn build_context_for_instance<'h>(
    instance: &JobInstance,
    workflow: &Workflow,
    completed: &[CompletedInstance],
    github: Value,
    inputs: Value,
    runner: RunnerInfo<'_>,
    secrets: Value,
) -> Result<Context<'h>, GraphError> {
    let mut ctx = Context::new();
    ctx.github = github;
    ctx.inputs = inputs;
    ctx.runner = obj([
        ("os", runner.os),
        ("arch", runner.arch),
        ("environment", runner.environment),
    ]);
    ctx.matrix = instance.matrix.clone();
    ctx.needs = build_needs_value(completed);
    ctx.secrets = secrets;

    // Compose env = workflow.env + job.env, with job.env shadowing. The
    // ExprString tokens are evaluated against everything *outside* `env.*`
    // (env-on-env self-reference isn't in F3 scope; it's a job-step concern).
    let mut env_obj = IndexMap::new();
    let env_ctx_for_eval = {
        let mut c = Context::new();
        c.github = ctx.github.clone();
        c.inputs = ctx.inputs.clone();
        c.runner = ctx.runner.clone();
        c.matrix = ctx.matrix.clone();
        c.needs = ctx.needs.clone();
        c
    };
    for (k, v) in &workflow.env {
        let value = eval_exprstring(v, &env_ctx_for_eval)
            .map_err(|source| GraphError::Expr { site: format!("workflow.env.{k}"), source })?;
        env_obj.insert(k.clone(), value);
    }
    if let Some(job) = workflow.jobs.get(&instance.job_id) {
        for (k, v) in &job.env {
            let value = eval_exprstring(v, &env_ctx_for_eval).map_err(|source| GraphError::Expr {
                site: format!("jobs.{}.env.{k}", instance.job_id),
                source,
            })?;
            env_obj.insert(k.clone(), value);
        }
    }
    ctx.env = Value::Object(env_obj);
    Ok(ctx)
}

/// Evaluate a job's `if:` condition. Missing condition means "run". A
/// non-boolean truthy/falsy result is coerced per [`Value::is_truthy`].
///
/// GHA `if:` is an *implicit* expression — the body is parsed as an
/// expression regardless of `${{ }}` delimiters, unlike a string scalar in
/// `with:` / `env:` where bare text stays text. We extract the raw body
/// (single-token shapes) and route through [`expr::evaluate`].
pub fn should_run_job(job: &Job, ctx: &Context) -> Result<bool, GraphError> {
    let Some(expr_str) = &job.if_cond else { return Ok(true) };
    let body = match expr_str.tokens.as_slice() {
        [ExprToken::Literal(b)] | [ExprToken::Expr(b)] => b.clone(),
        // Mixed tokens in `if:` aren't a shape GHA supports cleanly; fall
        // back to the strict ExprString eval so the failure mode at least
        // surfaces the malformed condition rather than silently passing.
        _ => {
            return eval_exprstring(expr_str, ctx)
                .map(|v| v.is_truthy())
                .map_err(|source| GraphError::Expr { site: "job.if".into(), source });
        }
    };
    let v = expr::evaluate(&body, ctx).map_err(|source| GraphError::Expr {
        site: "job.if".into(),
        source,
    })?;
    Ok(v.is_truthy())
}

/// Evaluate a job's `outputs:` map after its steps have run. Each value is an
/// `ExprString` template (typically a single `${{ steps.X.outputs.Y }}`);
/// caller is responsible for populating `ctx.steps` before this is called.
pub fn evaluate_outputs(
    outputs: &IndexMap<String, ExprString>,
    ctx: &Context,
) -> Result<IndexMap<String, Value>, GraphError> {
    let mut out = IndexMap::new();
    for (k, v) in outputs {
        let value = eval_exprstring(v, ctx).map_err(|source| GraphError::Expr {
            site: format!("job.outputs.{k}"),
            source,
        })?;
        out.insert(k.clone(), value);
    }
    Ok(out)
}

/// Evaluate an [`ExprString`]. A single-`Expr` token returns the typed value
/// directly (so `${{ true }}` is a `Bool`); mixed tokens concatenate via the
/// string coercion rules in [`Value::as_str_lossy`].
pub fn eval_exprstring(s: &ExprString, ctx: &Context) -> Result<Value, ExprError> {
    if let [ExprToken::Expr(body)] = s.tokens.as_slice() {
        let e = expr::parse(body)?;
        return expr::eval(&e, ctx);
    }
    let mut out = String::new();
    for t in &s.tokens {
        match t {
            ExprToken::Literal(lit) => out.push_str(lit),
            ExprToken::Expr(body) => {
                let e = expr::parse(body)?;
                let v = expr::eval(&e, ctx)?;
                out.push_str(&v.as_str_lossy());
            }
        }
    }
    Ok(Value::String(out))
}

// ─── tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_workflow;

    fn make_workflow(yaml: &str) -> Workflow {
        parse_workflow(yaml).unwrap_or_else(|e| panic!("parse: {e}"))
    }

    fn get(v: &Value, k: &str) -> Value {
        match v {
            Value::Object(m) => m.get(k).cloned().unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }

    /// Expand with no expansion-time context — the literal-matrix case every
    /// pre-R654-F2 test asserts.
    fn expand(m: &Matrix) -> Vec<Value> {
        expand_matrix(m, &PlanContext::default()).unwrap_or_else(|e| panic!("expand: {e}"))
    }

    fn test_runner() -> RunnerInfo<'static> {
        RunnerInfo {
            os: "Linux",
            arch: "X64",
            environment: "self-hosted",
        }
    }

    // ── topo sort

    #[test]
    fn topo_groups_independent_jobs_into_one_wave() {
        let yaml = r#"
on: [push]
jobs:
  a:
    runs-on: ubuntu-latest
    steps: [{ run: "true" }]
  b:
    runs-on: ubuntu-latest
    steps: [{ run: "true" }]
  c:
    runs-on: ubuntu-latest
    needs: [a, b]
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        let waves = topo_sort(&wf).unwrap();
        assert_eq!(waves.len(), 2);
        let w0: HashSet<_> = waves[0].iter().cloned().collect();
        assert_eq!(w0, HashSet::from(["a".to_string(), "b".to_string()]));
        assert_eq!(waves[1], vec!["c".to_string()]);
    }

    #[test]
    fn topo_detects_cycle() {
        let yaml = r#"
on: [push]
jobs:
  a:
    runs-on: ubuntu-latest
    needs: [b]
    steps: [{ run: "true" }]
  b:
    runs-on: ubuntu-latest
    needs: [a]
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        match topo_sort(&wf) {
            Err(GraphError::Cycle(set)) => {
                let s: HashSet<_> = set.into_iter().collect();
                assert_eq!(s, HashSet::from(["a".to_string(), "b".to_string()]));
            }
            other => panic!("expected Cycle, got {other:?}"),
        }
    }

    #[test]
    fn topo_rejects_unknown_needs() {
        let yaml = r#"
on: [push]
jobs:
  a:
    runs-on: ubuntu-latest
    needs: [b]
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        assert!(matches!(topo_sort(&wf), Err(GraphError::UnknownNeeds(_))));
    }

    // ── matrix expansion

    #[test]
    fn cartesian_two_dimensions() {
        let yaml = r#"
on: [push]
jobs:
  m:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
        rust: [stable, beta]
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        let matrix = wf.jobs["m"].strategy.as_ref().unwrap().matrix.as_ref().unwrap();
        let rows = expand(matrix);
        assert_eq!(rows.len(), 4, "2 * 2 = 4 combinations");
        // First two share the same os, varying rust — confirms iteration order
        // is dimensions-in-declaration-order, inner-loop last.
        assert_eq!(get(&rows[0], "os"), Value::String("ubuntu-latest".into()));
        assert_eq!(get(&rows[0], "rust"), Value::String("stable".into()));
        assert_eq!(get(&rows[1], "os"), Value::String("ubuntu-latest".into()));
        assert_eq!(get(&rows[1], "rust"), Value::String("beta".into()));
    }

    #[test]
    fn include_only_matrix_mirrors_release_yml() {
        // The cli-release matrix in release.yml is include-only.
        let yaml = r#"
on: [push]
jobs:
  cli-release:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        include:
          - os: ubuntu-latest
            target: x86_64-unknown-linux-gnu
            use_cross: false
          - os: ubuntu-latest
            target: x86_64-unknown-linux-musl
            use_cross: true
          - os: ubuntu-latest
            target: aarch64-unknown-linux-musl
            use_cross: true
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        let matrix = wf.jobs["cli-release"].strategy.as_ref().unwrap().matrix.as_ref().unwrap();
        let rows = expand(matrix);
        assert_eq!(rows.len(), 3);
        let targets: Vec<_> = rows.iter().map(|r| get(r, "target")).collect();
        assert_eq!(
            targets,
            vec![
                Value::String("x86_64-unknown-linux-gnu".into()),
                Value::String("x86_64-unknown-linux-musl".into()),
                Value::String("aarch64-unknown-linux-musl".into()),
            ]
        );
        // use_cross deserializes as bool.
        assert_eq!(get(&rows[0], "use_cross"), Value::Bool(false));
        assert_eq!(get(&rows[1], "use_cross"), Value::Bool(true));
    }

    #[test]
    fn include_extends_matching_combination() {
        // Cartesian over (os, rust); include adds a new key only when (os, rust)
        // matches an existing combo.
        let yaml = r#"
on: [push]
jobs:
  m:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
        rust: [stable, beta]
        include:
          - os: ubuntu-latest
            rust: stable
            extra: special
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        let m = wf.jobs["m"].strategy.as_ref().unwrap().matrix.as_ref().unwrap();
        let rows = expand(m);
        assert_eq!(rows.len(), 4);
        // Only the matching combo got `extra`.
        for row in &rows {
            let has_extra = matches!(get(row, "extra"), Value::String(_));
            let is_match = get(row, "os") == Value::String("ubuntu-latest".into())
                && get(row, "rust") == Value::String("stable".into());
            assert_eq!(has_extra, is_match, "extra only on matching row: {row:?}");
        }
    }

    #[test]
    fn exclude_drops_matching_row() {
        let yaml = r#"
on: [push]
jobs:
  m:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
        rust: [stable, beta]
        exclude:
          - os: macos-latest
            rust: beta
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        let m = wf.jobs["m"].strategy.as_ref().unwrap().matrix.as_ref().unwrap();
        let rows = expand(m);
        assert_eq!(rows.len(), 3);
        let any_excluded = rows.iter().any(|r| {
            get(r, "os") == Value::String("macos-latest".into())
                && get(r, "rust") == Value::String("beta".into())
        });
        assert!(!any_excluded);
    }

    // ── plan (topo + matrix together)

    #[test]
    fn plan_expands_matrix_inside_topo_wave() {
        let yaml = r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
    steps: [{ run: "true" }]
  publish:
    needs: [build]
    runs-on: ubuntu-latest
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        let plan = plan(&wf, &PlanContext::default()).unwrap();
        assert_eq!(plan.waves.len(), 2);
        assert_eq!(plan.waves[0].len(), 2, "build expands to 2 matrix rows");
        let keys: Vec<_> = plan.waves[0].iter().map(|i| i.key()).collect();
        assert_eq!(keys, vec!["build#0", "build#1"]);
        assert_eq!(plan.waves[1].len(), 1);
        assert_eq!(plan.waves[1][0].key(), "publish");
    }

    // ── result aggregation

    #[test]
    fn job_result_aggregate_failure_wins() {
        assert_eq!(
            JobResult::aggregate([JobResult::Success, JobResult::Failure, JobResult::Success]),
            JobResult::Failure,
        );
        assert_eq!(
            JobResult::aggregate([JobResult::Success, JobResult::Cancelled]),
            JobResult::Cancelled,
        );
        assert_eq!(JobResult::aggregate([JobResult::Success]), JobResult::Success);
    }

    // ── needs propagation

    #[test]
    fn build_needs_value_exposes_result_and_outputs() {
        let completed = vec![
            CompletedInstance {
                job_id: "image-yah-base".into(),
                matrix_index: None,
                result: JobResult::Success,
                outputs: IndexMap::from([(
                    "digest".to_string(),
                    Value::String("sha256:abc".into()),
                )]),
            },
            CompletedInstance {
                job_id: "smoke".into(),
                matrix_index: None,
                result: JobResult::Skipped,
                outputs: IndexMap::new(),
            },
        ];
        let needs = build_needs_value(&completed);
        let base = get(&needs, "image-yah-base");
        assert_eq!(get(&base, "result"), Value::String("success".into()));
        let outs = get(&base, "outputs");
        assert_eq!(get(&outs, "digest"), Value::String("sha256:abc".into()));
        let smoke = get(&needs, "smoke");
        assert_eq!(get(&smoke, "result"), Value::String("skipped".into()));
    }

    #[test]
    fn matrix_failure_aggregates_to_job_failure() {
        let completed = vec![
            CompletedInstance {
                job_id: "build".into(),
                matrix_index: Some(0),
                result: JobResult::Success,
                outputs: IndexMap::new(),
            },
            CompletedInstance {
                job_id: "build".into(),
                matrix_index: Some(1),
                result: JobResult::Failure,
                outputs: IndexMap::new(),
            },
        ];
        let needs = build_needs_value(&completed);
        assert_eq!(
            get(&get(&needs, "build"), "result"),
            Value::String("failure".into())
        );
    }

    // ── end-to-end: simulate release.yml smoke gate

    #[test]
    fn release_yml_image_gate_evaluates_against_needs() {
        // Synthetic workflow mirroring release.yml's image-yah-base if/needs shape.
        let yaml = r#"
on: [push]
jobs:
  smoke:
    runs-on: ubuntu-latest
    steps: [{ run: "true" }]
  image-yah-base:
    needs: [smoke]
    if: always() && needs.smoke.result != 'failure' && needs.smoke.result != 'cancelled'
    runs-on: ubuntu-latest
    outputs:
      digest: ${{ steps.build.outputs.digest }}
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        let plan = plan(&wf, &PlanContext::default()).unwrap();
        assert_eq!(plan.waves.len(), 2);

        // Smoke succeeded — image gate should run.
        let completed = vec![CompletedInstance {
            job_id: "smoke".into(),
            matrix_index: None,
            result: JobResult::Success,
            outputs: IndexMap::new(),
        }];
        let img_instance = &plan.waves[1][0];
        let ctx = build_context_for_instance(
            img_instance,
            &wf,
            &completed,
            Value::object(),
            Value::object(),
            test_runner(),
            Value::object(),
        )
        .unwrap();
        let job = &wf.jobs["image-yah-base"];
        assert!(should_run_job(job, &ctx).unwrap());

        // Smoke failed — gate should skip.
        let completed_fail = vec![CompletedInstance {
            job_id: "smoke".into(),
            matrix_index: None,
            result: JobResult::Failure,
            outputs: IndexMap::new(),
        }];
        let ctx_fail = build_context_for_instance(
            img_instance,
            &wf,
            &completed_fail,
            Value::object(),
            Value::object(),
            test_runner(),
            Value::object(),
        )
        .unwrap();
        assert!(!should_run_job(job, &ctx_fail).unwrap());
    }

    #[test]
    fn outputs_evaluate_against_steps_context() {
        // `outputs.digest: ${{ steps.build.outputs.digest }}` — populate
        // ctx.steps with a fake step result and confirm the output threads
        // through.
        let exprstr = ExprString::parse("${{ steps.build.outputs.digest }}");
        let mut outputs = IndexMap::new();
        outputs.insert("digest".to_string(), exprstr);

        let mut ctx = Context::new();
        ctx.steps = obj([(
            "build",
            obj([("outputs", obj([("digest", "sha256:def")]))]),
        )]);
        let evaluated = evaluate_outputs(&outputs, &ctx).unwrap();
        assert_eq!(
            evaluated.get("digest"),
            Some(&Value::String("sha256:def".into()))
        );
    }

    // ── exprstring evaluation: mixed tokens concatenate via string coercion

    #[test]
    fn exprstring_evaluate_mixed_concatenates() {
        let s = ExprString::parse("ghcr.io/yah-ai/yah-base:${{ github.ref_name }}");
        let mut ctx = Context::new();
        ctx.github = obj([("ref_name", "v1.2.3")]);
        let v = eval_exprstring(&s, &ctx).unwrap();
        assert_eq!(v, Value::String("ghcr.io/yah-ai/yah-base:v1.2.3".into()));
    }

    #[test]
    fn exprstring_single_expr_preserves_type() {
        // `outputs.x: ${{ true }}` should yield Bool, not the string "true".
        let s = ExprString::parse("${{ true }}");
        let ctx = Context::new();
        let v = eval_exprstring(&s, &ctx).unwrap();
        assert_eq!(v, Value::Bool(true));
    }

    // ── R654-F2: expressions in the matrix position

    /// The caller-narrowed-matrix idiom, verbatim from the workflow that
    /// motivated R654: one `board` when the caller names one, the full list
    /// when they don't.
    const DYNAMIC_BOARD_MATRIX: &str = r#"
on: [workflow_dispatch]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        board: ${{ fromJSON(inputs.board && format('["{0}"]', inputs.board) || '["rpi_zero2w","rpi4"]') }}
    steps: [{ run: "true" }]
"#;

    fn boards(rows: &[Value]) -> Vec<String> {
        rows.iter().map(|r| get(r, "board").as_str_lossy()).collect()
    }

    #[test]
    fn dynamic_matrix_dimension_falls_back_to_the_full_list() {
        let wf = make_workflow(DYNAMIC_BOARD_MATRIX);
        let m = wf.jobs["build"].strategy.as_ref().unwrap().matrix.as_ref().unwrap();
        let rows = expand_matrix(m, &PlanContext::default()).unwrap();
        assert_eq!(boards(&rows), vec!["rpi_zero2w", "rpi4"]);
    }

    #[test]
    fn dynamic_matrix_dimension_narrows_from_inputs() {
        let wf = make_workflow(DYNAMIC_BOARD_MATRIX);
        let m = wf.jobs["build"].strategy.as_ref().unwrap().matrix.as_ref().unwrap();
        let ctx = PlanContext {
            inputs: obj([("board", "rpi4")]),
            ..PlanContext::default()
        };
        let rows = expand_matrix(m, &ctx).unwrap();
        assert_eq!(boards(&rows), vec!["rpi4"]);
    }

    #[test]
    fn dynamic_matrix_narrows_the_whole_plan_not_just_the_rows() {
        // The observable end of the verify line: instance count out of plan().
        let wf = make_workflow(DYNAMIC_BOARD_MATRIX);
        assert_eq!(
            plan(&wf, &PlanContext::default()).unwrap().iter_instances().count(),
            2
        );
        let narrowed = PlanContext {
            inputs: obj([("board", "rpi4")]),
            ..PlanContext::default()
        };
        let p = plan(&wf, &narrowed).unwrap();
        assert_eq!(p.iter_instances().count(), 1);
        let only = p.iter_instances().next().unwrap();
        assert_eq!(only.matrix_index, Some(0), "sole row is index 0, not the source position");
    }

    #[test]
    fn matrix_expression_yielding_a_scalar_is_one_dimension_value() {
        // Not every expression returns a list. A scalar result contributes a
        // single value rather than erroring or stringifying to "[x]".
        let yaml = r#"
on: [workflow_dispatch]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: ${{ inputs.target }}
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        let m = wf.jobs["build"].strategy.as_ref().unwrap().matrix.as_ref().unwrap();
        let ctx = PlanContext {
            inputs: obj([("target", "aarch64-unknown-linux-gnu")]),
            ..PlanContext::default()
        };
        let rows = expand_matrix(m, &ctx).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            get(&rows[0], "target"),
            Value::String("aarch64-unknown-linux-gnu".into())
        );
    }

    #[test]
    fn literal_array_dimension_values_are_not_spliced() {
        // GHA allows array-valued matrix entries (`node: [[14,'lts'],…]`). The
        // splice rule is gated on the source being an EXPRESSION, so a literal
        // nested list stays one value per row instead of flattening.
        let yaml = r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        pair: [[1, 2], [3, 4]]
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        let m = wf.jobs["build"].strategy.as_ref().unwrap().matrix.as_ref().unwrap();
        let rows = expand(m);
        assert_eq!(rows.len(), 2, "two rows, not four spliced scalars");
        assert_eq!(
            get(&rows[0], "pair"),
            Value::Array(vec![Value::Number(1.0), Value::Number(2.0)])
        );
    }

    #[test]
    fn include_and_exclude_rows_evaluate_expressions_too() {
        // The expression pass covers the whole matrix block, not just
        // dimensions: an include row can name a caller-supplied value, and an
        // exclude row can drop a combination the caller asked to skip.
        let yaml = r#"
on: [workflow_dispatch]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        board: [rpi_zero2w, rpi4]
        include:
          - board: rpi4
            variant: ${{ inputs.variant }}
        exclude:
          - board: ${{ inputs.skip }}
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        let m = wf.jobs["build"].strategy.as_ref().unwrap().matrix.as_ref().unwrap();
        let ctx = PlanContext {
            inputs: obj([("variant", "lite"), ("skip", "rpi_zero2w")]),
            ..PlanContext::default()
        };
        let rows = expand_matrix(m, &ctx).unwrap();
        assert_eq!(boards(&rows), vec!["rpi4"], "excluded board is dropped");
        assert_eq!(get(&rows[0], "variant"), Value::String("lite".into()));
    }

    #[test]
    fn matrix_expression_error_names_its_site() {
        // A broken matrix expression must say WHICH dimension entry broke —
        // the plan aborts before any job runs, so the message is all the
        // operator gets.
        let yaml = r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        board: ${{ fromJSON('not json') }}
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        let err = plan(&wf, &PlanContext::default()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("strategy.matrix.board[0]"),
            "error should name the failing entry, got: {msg}"
        );
    }

    #[test]
    fn matrix_expressions_cannot_see_the_per_instance_context() {
        // The documented restriction on PlanContext: `needs.*` (and `env.*`,
        // `matrix.*`, `runner.*`) resolve to null at expansion time, so this
        // dimension evaluates to the empty list. It does NOT silently fan out
        // from stale state — it collapses to the same single dimensionless row
        // a literal `board: []` produces today (see `cartesian`), which then
        // runs once with `matrix.board` empty and fails loudly downstream.
        let yaml = r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        board: ${{ fromJSON(needs.discover.outputs.boards || '[]') }}
    steps: [{ run: "true" }]
"#;
        let wf = make_workflow(yaml);
        let m = wf.jobs["build"].strategy.as_ref().unwrap().matrix.as_ref().unwrap();
        let rows = expand_matrix(m, &PlanContext::default()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(get(&rows[0], "board"), Value::Null, "no board was selected");
    }
}
