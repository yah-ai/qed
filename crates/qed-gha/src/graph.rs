//! Job graph: topological wave scheduling + matrix expansion + the helpers
//! `if:` evaluation and outputs propagation hang off.
//!
//! No step execution lives here — F3 proves order, matrix fan-out, and the
//! `needs.<job>.outputs.<key>` / `needs.<job>.result` propagation against a
//! synthetic completion driver. F4 wires the step executor on top.

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

pub fn plan(workflow: &Workflow) -> Result<Plan, GraphError> {
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
                    let rows = expand_matrix(m);
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
pub fn expand_matrix(matrix: &Matrix) -> Vec<Value> {
    // Step 1 — cartesian product over dimensions.
    let dim_keys: Vec<String> = matrix.dimensions.keys().cloned().collect();
    let dim_vals: Vec<Vec<Value>> = matrix
        .dimensions
        .values()
        .map(|seq| seq.iter().map(yaml_to_value).collect())
        .collect();

    let mut rows: Vec<IndexMap<String, Value>> = if dim_keys.is_empty() {
        vec![]
    } else {
        cartesian(&dim_keys, &dim_vals)
    };

    // Step 2 — apply include rows.
    let original_keys: HashSet<&str> = dim_keys.iter().map(|s| s.as_str()).collect();
    for inc in &matrix.include {
        let inc_obj: IndexMap<String, Value> = inc
            .iter()
            .map(|(k, v)| (k.clone(), yaml_to_value(v)))
            .collect();

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
        rows.retain(|row| {
            !matrix.exclude.iter().any(|ex| {
                ex.iter().all(|(k, ev)| {
                    let v = yaml_to_value(ev);
                    row.get(k.as_str()) == Some(&v)
                })
            })
        });
    }

    rows.into_iter().map(Value::Object).collect()
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

/// Build the evaluator context for one [`JobInstance`] at scheduling time.
/// Caller supplies the workflow's `github` / `inputs` snapshot; this helper
/// stitches in `matrix`, `needs`, `env`, and `runner.os`.
pub fn build_context_for_instance<'h>(
    instance: &JobInstance,
    workflow: &Workflow,
    completed: &[CompletedInstance],
    github: Value,
    inputs: Value,
    runner_os: &str,
    secrets: Value,
) -> Result<Context<'h>, GraphError> {
    let mut ctx = Context::new();
    ctx.github = github;
    ctx.inputs = inputs;
    ctx.runner = obj([("os", runner_os)]);
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
        let rows = expand_matrix(matrix);
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
        let rows = expand_matrix(matrix);
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
        let rows = expand_matrix(m);
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
        let rows = expand_matrix(m);
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
        let plan = plan(&wf).unwrap();
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
        let plan = plan(&wf).unwrap();
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
            "Linux",
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
            "Linux",
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
}
