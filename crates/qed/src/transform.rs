//! Assisted one-way GHA→QED transformer (R533-F4, W224).
//!
//! W224 ("import, don't emulate") makes a GitHub Actions workflow an *import
//! source*, not a foreign runtime QED reproduces forever. The onboarding path
//! is a **one-way, assisted, lossy-with-warnings** transform: map the tier-1/2
//! ~80% mechanically, and **flag** the tier-3 steps it deliberately declines to
//! imitate — proposing the native QED replacement for each rather than guessing.
//!
//! This module is that transform. It sits on top of the [`yah_qed_gha`] parser +
//! the R533-F2 [tier classifier](yah_qed_gha::classify_step) and emits native
//! [`QedStep`]s for the runnable compute, paired with a list of human-facing
//! flags for everything that needs a decision. It is **pure** — it operates on
//! an already-parsed [`Workflow`] and performs no I/O — so the runner / the
//! `eject` materializer (R533-F6) own the file read and the TOML write.
//!
//! ## What maps mechanically, what gets flagged
//!
//! | Parsed step | Tier (F2) | Result |
//! |---|---|---|
//! | `run:` bash | 1/2 compute | **native** [`StepKind::Subprocess`] step |
//! | `run:` bash reaching `gh`/`api.github.com`/`GITHUB_TOKEN` | 1/2 + service touch | native step **and** an [`FlagKind::EmbeddedServiceTouch`] flag |
//! | `run:` carrying `${{ … }}` | 1/2 compute | native step **and** an [`FlagKind::UnresolvedExpression`] flag |
//! | `uses: org/setup-*`, `dtolnay/rust-toolchain`, … | 1/2 toolkit | [`FlagKind::ToolkitAction`] — runs via the R533-T7 toolkit executor, no subprocess emitted yet |
//! | `uses: actions/checkout`, `cache`, `upload-artifact`, `gh-release`, `build-push`, … | 3 service | [`FlagKind::ReplaceWithNative`] carrying the native stanza |
//! | `uses:` unrecognized | unknown | [`FlagKind::Unknown`] — surfaced for review |
//!
//! ## Job DAG → flat pipeline
//!
//! A native [`Pipeline`](crate::types::Pipeline) is a flat `Vec<QedStep>` run in
//! declaration order; GHA workflows are a job DAG. The transform flattens jobs
//! into a **topological linearization** ([`yah_qed_gha::topo_sort`]) — every job's
//! `needs:` predecessors emit before it — so execution order is honest even
//! though the job grouping is lost.
//!
//! Read that as "the DAG is not *represented*", NOT as "the port gives up
//! parallelism you had". QED has no inter-job parallelism to give up at either
//! end: `QedStep` carries no dependency edges and the runner walks
//! `pipeline.steps` in order, and the qed-gha emulator likewise computes waves
//! and then executes every instance one at a time (`runtime.rs`, "Sequential
//! within wave"). A flattened port therefore runs in the same wall-clock as the
//! same workflow under the emulator; both are slower than GitHub, which schedules
//! jobs across runners. Making QED honour the DAG is a *runner* feature (a
//! dependency model on the step/job layer plus a concurrent scheduler), not
//! something this transform can recover on its own — so do not "fix" it here by
//! synthesising ordering the executor cannot act on.
//!
//! Matrix expansion and the `workflow_call` port contract are *not* handled
//! here: target lifting out of `strategy.matrix` is R533-F9 (it layers onto the
//! steps emitted here) and the down/up-port mapping is R533-F5.
//!
//! @yah:ticket(R605-F3, "QED executes a job DAG: dependency edges on the step/job layer + a concurrent scheduler")
//! @yah:at(2026-08-03T06:12:20Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R605)
//! @yah:next("Half (a) - qed-gha emulator: runtime.rs `for wave in &plan.waves { for instance in wave {` runs every instance serially ('Sequential within wave - F4 simplification'). The waves are ALREADY computed correctly by graph::plan; only the executor is serial. Needs a concurrency cap + shared-resource keys before turning on: unlike GHA these instances share one host, one cargo target dir and one docker daemon, so naive fan-out contends where GHA does not.")
//! @yah:next("Half (b) - native pipelines: QedStep carries no dependency edges at all and PipelineRunner walks `for step in &self.pipeline.steps` in order, so an ejected pipeline cannot REPRESENT the DAG regardless of scheduling. This is the half that gates porting release.yml as anything but a flat list. Needs a dependency model (a `needs` on QedStep, or a job-grouping layer above steps) before the transform has anywhere to put the edges.")
//! @yah:next("Only after (b): change transform.rs's topo_sort(...).map(|waves| waves.into_iter().flatten().collect()) to emit the wave structure instead of flattening. Do NOT do this first - synthesising ordering the executor cannot act on is worse than the honest flat list.")
//! @yah:next("Blast radius on (b), scope before starting: PipelineRunner's sequential loop; remote-resume, which indexes by pipeline-local step index (runner.rs ~2659); background_until, whose contract is 'the named step must appear AFTER this one' - a partial order breaks 'after'; concurrency_key lock acquisition; and the per-step event/index stream the desktop QED tab renders.")
//! @yah:verify("A pipeline with two independent branches and one join records overlapping start/end timestamps for the branches, and the join starts only after both finish")
//! @yah:verify("yah qed eject on a multi-job workflow emits the job structure rather than a flat list, and `yah qed validate` still round-trips it")
//! @yah:gotcha("The reporting layer already models this and is NOT the gap: the run-status wire struct carries `needs: Vec<String>` per job (W223 R532-F2) so the graph viewer can draw dependency edges for a WRAPPED workflow. That is display-only, fed from yah_qed_gha::plan - it does not mean native pipelines have dependencies.")
//! @yah:gotcha("Do not file this as a release.yml blocker on parallelism grounds. Porting release.yml flat costs NOTHING against QED-today, because the emulator is serial too. The parallelism gap is against GitHub, and you already pay it the moment you run release.yml through QED at all. The real reasons to hold release.yml are its 52 tier-3 flags (several with no native replacement built yet) and it being a one-way move off the thing that currently ships releases.")
//! @arch:see(oss/qed/crates/qed-gha/src/runtime.rs)
//! @arch:see(oss/qed/crates/qed/src/transform.rs)

use crate::matrix::MatrixSpec;
use crate::platform::PlatformSpec;
use crate::types::{OnFail, QedStep};
use indexmap::IndexMap;
use yah_qed_gha::{
    classify_step, topo_sort, Disposition, ExprString, ExprToken, Job, NativeReplacement,
    ServiceTouch, Step, StepAction, Workflow,
};

/// The result of transforming one workflow — the native steps that mapped
/// mechanically, interleaved (in execution order) with the flags the human must
/// resolve. "Assisted / lossy-with-warnings" made concrete: nothing tier-3 is
/// silently run, and nothing un-mappable is silently dropped.
#[derive(Debug, Clone)]
pub struct TransformReport {
    /// Native pipeline name — the workflow `name:` sanitized to a slug, or
    /// `"imported-workflow"` when the source declares none.
    pub name: String,
    /// Human-readable label — the workflow `name:` verbatim, else the slug.
    pub label: String,
    /// One entry per parsed step, in flattened (topo-job then step) order. Each
    /// carries the mechanically-mapped native step (when one could be emitted)
    /// and/or the flags raised for it.
    pub steps: Vec<TransformedStep>,
}

impl TransformReport {
    /// The mechanically-mapped native steps, in order — the runnable spine F6
    /// materializes and F9 lifts platform targets into.
    pub fn native_steps(&self) -> impl Iterator<Item = &QedStep> {
        self.steps.iter().filter_map(|s| s.native.as_ref())
    }

    /// Owned copy of the native steps, ready to drop into a
    /// [`Pipeline::steps`](crate::types::Pipeline::steps).
    pub fn collect_native(&self) -> Vec<QedStep> {
        self.native_steps().cloned().collect()
    }

    /// Every flag raised across all steps, paired with the step that raised it.
    pub fn flags(&self) -> impl Iterator<Item = (&TransformedStep, &FlagKind)> {
        self.steps.iter().flat_map(|s| s.flags.iter().map(move |f| (s, f)))
    }

    /// `true` when every step mapped to clean native compute with no flag — the
    /// workflow imported losslessly (rare; most real workflows touch tier 3).
    pub fn is_clean(&self) -> bool {
        self.steps.iter().all(|s| s.flags.is_empty() && s.native.is_some())
    }
}

/// One parsed workflow step after transformation. A clean tier-1/2 `run:` step
/// has `native = Some(..)` and `flags = []`; a tier-3 step has `native = None`
/// and a [`FlagKind::ReplaceWithNative`]; a `run:` step reaching the service has
/// **both** a native step (it runs) and a flag (the reach won't resolve on QED).
#[derive(Debug, Clone)]
pub struct TransformedStep {
    /// Owning GHA job id.
    pub job: String,
    /// 0-based index within the job's `steps:` list.
    pub step_index: usize,
    /// The step's `name:` rendered to text, or `None` when unnamed.
    pub step_name: Option<String>,
    /// The mechanically-mapped native step, when one could be emitted. `None`
    /// for purely-flagged steps (tier-3, toolkit `uses:`, unknown) — there is
    /// nothing to run natively yet.
    pub native: Option<QedStep>,
    /// Why this step needs human attention. Empty for clean compute.
    pub flags: Vec<FlagKind>,
}

/// Why a parsed step couldn't be imported as clean native compute — the
/// assisted half of the transform. Each variant proposes what to do instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlagKind {
    /// Tier-3 GitHub-the-service action. Replace with the named native QED
    /// facility; [`stanza_hint`](FlagKind::stanza_hint) carries the guidance.
    ReplaceWithNative(NativeReplacement),
    /// A clean-compute `run:` step that reaches GitHub-the-service from inside
    /// its bash. It still runs on the executor, but the service call won't
    /// resolve on QED — replace the reach with a native facility.
    EmbeddedServiceTouch(Vec<ServiceTouch>),
    /// A tier-1/2 `uses:` toolkit action. Runs via the toolkit-contract
    /// executor (R533-T7); no native subprocess is emitted until that executor's
    /// step surface lands.
    ToolkitAction { slug: String, git_ref: Option<String> },
    /// An unrecognized `uses:` slug — surfaced for review rather than guessed.
    Unknown { slug: String },
    /// A mechanically-mapped `run:` step whose script still carries GHA
    /// `${{ … }}` expressions, which QED's `{{key}}` subprocess substitution
    /// won't expand. Convert to QED params / native outputs, or lift via
    /// import-time target lifting (R533-F9).
    UnresolvedExpression,
    /// A surviving `${{ secrets.NAME }}` reference — split out of
    /// [`UnresolvedExpression`] because its failure mode is categorically
    /// worse than the rest of that bucket.
    ///
    /// Every other unresolved expression degrades to a visibly wrong literal:
    /// a path that doesn't exist, a tag that doesn't match. A secret degrades
    /// to a *non-empty string that looks like a credential*, so the usual
    /// `if [ -z "$TOKEN" ]; then skip; fi` guard passes and the step proceeds
    /// to make authenticated-looking calls with the literal text
    /// `${{ secrets.NAME }}` as its bearer token. A step that would have
    /// safely no-op'd instead fails deep inside a third-party API — which is
    /// exactly the lossy-in-silence outcome this transform exists to prevent.
    ///
    /// The native replacement is real and nameable, which is why this is
    /// `Review` and not `Info`: bridge the name through
    /// `~/.yah/qed/secrets.toml`.
    UnbridgedSecret { names: Vec<String> },
}

/// How loud a [`FlagKind`] is, for preflight summaries and reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlagSeverity {
    /// Tier-3 surface QED declines to imitate — the import *cannot* run this as
    /// authored; a native replacement is required.
    Replace,
    /// A human decision is needed (unrecognized action, embedded service reach).
    Review,
    /// Informational — handled by other relay tickets (T7 executor, F9 lifting).
    Info,
}

impl FlagSeverity {
    pub fn label(self) -> &'static str {
        match self {
            FlagSeverity::Replace => "replace",
            FlagSeverity::Review => "review",
            FlagSeverity::Info => "info",
        }
    }
}

impl FlagKind {
    /// Severity bucket for this flag.
    pub fn severity(&self) -> FlagSeverity {
        match self {
            FlagKind::ReplaceWithNative(_) => FlagSeverity::Replace,
            FlagKind::EmbeddedServiceTouch(_)
            | FlagKind::Unknown { .. }
            | FlagKind::UnbridgedSecret { .. } => FlagSeverity::Review,
            FlagKind::ToolkitAction { .. } | FlagKind::UnresolvedExpression => FlagSeverity::Info,
        }
    }

    /// The "here's the native stanza" guidance surfaced alongside the flag — the
    /// lossy-with-warnings payload W224 calls for.
    pub fn stanza_hint(&self) -> String {
        match self {
            FlagKind::ReplaceWithNative(nr) => nr.stanza_hint().to_string(),
            FlagKind::EmbeddedServiceTouch(touches) => {
                let names: Vec<&str> = touches.iter().map(|t| t.label()).collect();
                format!(
                    "Run step reaches GitHub-the-service ({}); it runs on the executor but the \
                     call won't resolve on QED — replace with a native facility \
                     (content-addressed artifacts / a W208 publisher).",
                    names.join(", ")
                )
            }
            FlagKind::ToolkitAction { slug, .. } => format!(
                "Tier-1/2 toolkit action `{slug}` — runs via the toolkit-contract executor \
                 (R533-T7); no native subprocess emitted yet."
            ),
            FlagKind::Unknown { slug } => format!(
                "Unrecognized action `{slug}` — map it by hand or extend the tier catalog \
                 (qed-gha `classify_uses`); not run silently."
            ),
            FlagKind::UnresolvedExpression => {
                "Step carries GHA `${{ … }}` expressions (in the script or its `env:`) that \
                 QED's subprocess substitution won't expand; convert to QED params \
                 (`{{key}}`) / native outputs, or lift the build target at import time \
                 (R533-F9)."
                    .to_string()
            }
            FlagKind::UnbridgedSecret { names } => format!(
                "References secret(s) {} which lower to the LITERAL text `${{{{ secrets.NAME }}}}`, \
                 not to a value — a non-empty string that defeats an `if [ -z \"$TOKEN\" ]` guard \
                 and is then sent as a live-looking credential. Bridge each name in \
                 `~/.yah/qed/secrets.toml` (`NAME = \"vault:<slot>\"`) before running this step.",
                names.join(", ")
            ),
        }
    }
}

/// Transform a parsed workflow into native QED steps + assisted flags.
///
/// Jobs are flattened in [`topo_sort`] order so `needs:` predecessors precede
/// their dependents; an unresolvable graph (cycle / unknown `needs`) falls back
/// to declaration order rather than failing the import — the operator still gets
/// the per-step transform to work from.
pub fn transform_workflow(wf: &Workflow) -> TransformReport {
    let label = wf
        .name
        .clone()
        .unwrap_or_else(|| "imported workflow".to_string());
    let name = slugify(&label, "imported-workflow");

    // Topo-linearize the job DAG; on an unresolvable graph, keep declaration
    // order so the import still produces something to edit.
    let order: Vec<String> = topo_sort(wf)
        .map(|waves| waves.into_iter().flatten().collect())
        .unwrap_or_else(|_| wf.jobs.keys().cloned().collect());

    let mut steps = Vec::new();
    for job_id in &order {
        let Some(job) = wf.jobs.get(job_id) else { continue };
        for (step_index, step) in job.steps.iter().enumerate() {
            steps.push(transform_step(job_id, job, step_index, step));
        }
    }

    TransformReport { name, label, steps }
}

/// Convenience: parse raw workflow YAML and transform it in one call. Still
/// pure (no file I/O) — the caller supplies the bytes. Used by `eject`
/// (R533-F6) and the tests here.
pub fn transform_workflow_src(src: &str) -> Result<TransformReport, yah_qed_gha::ParseError> {
    Ok(transform_workflow(&yah_qed_gha::parse_workflow(src)?))
}

/// Transform a single classified step.
fn transform_step(job_id: &str, job: &Job, step_index: usize, step: &Step) -> TransformedStep {
    let step_name = step.name.as_ref().map(render_exprstring).map(|s| s.trim().to_string());
    let class = classify_step(step);
    let mut native = None;
    let mut flags = Vec::new();

    match (&step.action, &class.disposition) {
        // Tier-1/2 `run:` compute → mechanical native subprocess. Embedded
        // service touches / surviving expressions ride along as flags.
        (StepAction::Run { body, shell }, Disposition::Compute) => {
            let (step_native, lifted_key) =
                map_run_step(job_id, job, step_index, step, body, shell.as_deref());
            native = Some(step_native);
            if !class.service_touches.is_empty() {
                flags.push(FlagKind::EmbeddedServiceTouch(class.service_touches.clone()));
            }
            // A `${{ matrix.<key> }}` reference that R533-F9 lifted (target
            // dimension carried as a step matrix) resolves natively, so it is
            // *not* an unresolved expression; any other `${{ … }}` still is.
            let key = lifted_key.as_deref();
            if has_unresolved_expression(body, key)
                || step.env.values().any(|v| has_unresolved_expression(v, key))
            {
                flags.push(FlagKind::UnresolvedExpression);
            }
            // Secrets are a subset of the unresolved expressions above, and are
            // reported *in addition to* — not instead of — the general flag:
            // the two want different fixes (params vs. the secrets bridge) and
            // a step can need both.
            let secrets = referenced_secrets(step, body);
            if !secrets.is_empty() {
                flags.push(FlagKind::UnbridgedSecret { names: secrets });
            }
        }
        // Tier-1/2 `uses:` toolkit action → flagged for the T7 executor.
        (StepAction::Uses { slug, git_ref, .. }, Disposition::Compute) => {
            flags.push(FlagKind::ToolkitAction { slug: slug.clone(), git_ref: git_ref.clone() });
        }
        // Tier-3 → replace with the named native facility.
        (_, Disposition::ReplaceWithNative(nr)) => {
            flags.push(FlagKind::ReplaceWithNative(*nr));
        }
        // Unrecognized `uses:` → surface for review.
        (StepAction::Uses { slug, .. }, Disposition::Unknown) => {
            flags.push(FlagKind::Unknown { slug: slug.clone() });
        }
        // A `run:` step is always Compute in the classifier, so this is
        // unreachable in practice; map it natively rather than dropping it.
        (StepAction::Run { body, shell }, Disposition::Unknown) => {
            native = Some(map_run_step(job_id, job, step_index, step, body, shell.as_deref()).0);
        }
    }

    TransformedStep { job: job_id.to_string(), step_index, step_name, native, flags }
}

/// Map a tier-1/2 `run:` step to a native [`StepKind::Subprocess`] step,
/// lifting any build target (R533-F9) into the structured `platform` field.
///
/// Returns the step plus the matrix key whose target dimension was lifted (so
/// the caller can suppress the unresolved-expression flag for that resolved
/// reference). `None` when no matrix-driven target was carried.
fn map_run_step(
    job_id: &str,
    job: &Job,
    step_index: usize,
    step: &Step,
    body: &ExprString,
    shell: Option<&str>,
) -> (QedStep, Option<String>) {
    let name = match step.name.as_ref().map(render_exprstring) {
        Some(n) if !n.trim().is_empty() => format!("{job_id}: {}", n.trim()),
        _ => format!("{job_id}: step {step_index}"),
    };
    let script = render_exprstring(body);
    let argv = shell_argv(shell, &script);
    let env = step
        .env
        .iter()
        .map(|(k, v)| (k.clone(), render_exprstring(v)))
        .collect();
    let cwd = step
        .working_directory
        .as_ref()
        .map(render_exprstring)
        .filter(|s| !s.is_empty());
    let timeout = step.timeout_minutes.map(|m| u64::from(m) * 60);
    let on_fail = if step.continue_on_error == Some(true) {
        OnFail::Continue
    } else {
        OnFail::Abort
    };
    let if_cond = step.if_cond.as_ref().map(render_exprstring);

    // R533-F9: lift the build target out of `--target <triple>` into the
    // structured platform field, so F3's native resolve() reasons about it
    // instead of a runtime bash scrape.
    let (platform, matrix, lifted_key) = lift_target(job, &script);

    let step = QedStep {
        background: false,
        background_until: None,
        wait_for: None,
        manual: None,
        argv,
        cwd,
        env,
        timeout,
        on_fail,
        if_cond,
        platform,
        matrix,
        ..base_step(name)
    };
    (step, lifted_key)
}

/// Lift a `--target <triple>` token from a step's script into a [`PlatformSpec`]
/// (R533-F9). When the target is a `${{ matrix.<key> }}` reference whose job
/// matrix dimension holds concrete triples, the dimension is carried as a
/// step-level [`MatrixSpec`] so QED fans the step out, one native build per
/// target — and the `platform.target` reference concretizes per row.
///
/// Returns `(platform, step_matrix, lifted_matrix_key)`; all `None` when the
/// step declares no `--target`.
fn lift_target(job: &Job, script: &str) -> (Option<PlatformSpec>, Option<MatrixSpec>, Option<String>) {
    let Some(raw_target) = extract_target(script) else {
        return (None, None, None);
    };
    let platform = Some(PlatformSpec {
        target: Some(raw_target.clone()),
        container_platform: None,
        native: false,
    });

    // A concrete triple needs no matrix; a matrix reference whose dimension we
    // can resolve carries the target values so QED expands them natively.
    if let Some(key) = matrix_ref_key(&raw_target) {
        let values = matrix_target_values(job, &key);
        if !values.is_empty() {
            return (platform, Some(target_matrix(&key, &values)), Some(key));
        }
    }
    (platform, None, None)
}

/// Extract the value of the first `--target <X>` / `--target=<X>` flag in a
/// script. `X` is either a concrete triple or a `${{ matrix.<key> }}` reference
/// (returned with normalized spacing). Returns `None` when absent — and is
/// careful not to mistake `--target-dir` for `--target`.
fn extract_target(script: &str) -> Option<String> {
    const FLAG: &str = "--target";
    let mut from = 0;
    while let Some(rel) = script[from..].find(FLAG) {
        let pos = from + rel;
        let after = &script[pos + FLAG.len()..];
        from = pos + FLAG.len();
        let mut chars = after.chars();
        match chars.next() {
            // `--target=<value>`
            Some('=') => {
                if let Some(v) = read_target_value(&after[1..]) {
                    return Some(v);
                }
            }
            // `--target <value>`
            Some(c) if c.is_whitespace() => {
                if let Some(v) = read_target_value(after) {
                    return Some(v);
                }
            }
            // `--target-dir`, `--targets`, … — not the flag we want.
            _ => {}
        }
    }
    None
}

/// Read a target value at the start of `s` (already past `--target`/`=`): a
/// `${{ … }}` block (preserved with normalized spacing) or a non-whitespace run.
fn read_target_value(s: &str) -> Option<String> {
    let s = s.trim_start();
    if let Some(rest) = s.strip_prefix("${{") {
        let end = rest.find("}}")?;
        return Some(format!("${{{{ {} }}}}", rest[..end].trim()));
    }
    let val: String = s.chars().take_while(|c| !c.is_whitespace()).collect();
    (!val.is_empty()).then_some(val)
}

/// The matrix dimension key of a bare `${{ matrix.<key> }}` reference, or `None`
/// for a concrete value or a more complex expression.
fn matrix_ref_key(value: &str) -> Option<String> {
    let inner = value.trim().strip_prefix("${{")?.strip_suffix("}}")?.trim();
    let key = inner.strip_prefix("matrix.")?.trim();
    (!key.is_empty() && !key.contains(char::is_whitespace)).then(|| key.to_string())
}

/// Concrete triple values for a job's `strategy.matrix.<key>` dimension —
/// gathered from both the dimension list and any `include:` rows that carry the
/// key (release-shaped include-only matrices put the target on include rows).
fn matrix_target_values(job: &Job, key: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |s: String| {
        if !out.contains(&s) {
            out.push(s);
        }
    };
    if let Some(matrix) = job.strategy.as_ref().and_then(|s| s.matrix.as_ref()) {
        if let Some(values) = matrix.dimensions.get(key) {
            for v in values {
                if let Some(s) = v.as_str() {
                    push(s.to_string());
                }
            }
        }
        for inc in &matrix.include {
            if let Some(s) = inc.get(key).and_then(|v| v.as_str()) {
                push(s.to_string());
            }
        }
    }
    out
}

/// A single-dimension step matrix over the lifted target triples.
fn target_matrix(key: &str, values: &[String]) -> MatrixSpec {
    let mut dimensions: IndexMap<String, Vec<toml::Value>> = IndexMap::new();
    dimensions.insert(
        key.to_string(),
        values.iter().map(|s| toml::Value::String(s.clone())).collect(),
    );
    MatrixSpec { dimensions, include: Vec::new(), exclude: Vec::new() }
}

/// A `QedStep` with every non-`Subprocess` field at its default — the spine
/// `map_run_step` overlays argv/env/etc. onto.
///
/// This used to spell all 30-odd fields out, with a comment saying `QedStep` has
/// no `Default`. It has one (types.rs, R633) and that impl is *stricter* than a
/// hand-written literal: it round-trips serde's own defaults, so it cannot drift
/// from what a TOML file with only `name =` deserializes to. Enumerating the
/// fields here bought nothing and cost an edit on every new `QedStep` field —
/// including the two that led to this comment being disproved (R717-T1/T2).
fn base_step(name: String) -> QedStep {
    QedStep {
        name,
        ..Default::default()
    }
}

/// Wrap a rendered script body in its shell's argv. GHA's default `bash`/`sh`
/// run with fail-fast (`set -eo pipefail` / `set -e`); preserve that so an
/// imported step fails on the same line it would on GitHub rather than silently
/// swallowing a mid-script error.
fn shell_argv(shell: Option<&str>, script: &str) -> Vec<String> {
    let argv = |prog: &str, flag: &str, body: String| {
        vec![prog.to_string(), flag.to_string(), body]
    };
    match shell.unwrap_or("bash") {
        "bash" => argv("bash", "-c", format!("set -eo pipefail\n{script}")),
        "sh" => argv("sh", "-c", format!("set -e\n{script}")),
        "pwsh" | "powershell" => argv("pwsh", "-Command", script.to_string()),
        "python" | "python3" => argv("python3", "-c", script.to_string()),
        other => argv(other, "-c", script.to_string()),
    }
}

/// Render an [`ExprString`] back to text, reconstructing `${{ … }}` around each
/// expression token. Literal segments pass through verbatim.
pub(crate) fn render_exprstring(s: &ExprString) -> String {
    let mut out = String::new();
    for t in &s.tokens {
        match t {
            ExprToken::Literal(x) => out.push_str(x),
            ExprToken::Expr(x) => {
                out.push_str("${{ ");
                out.push_str(x);
                out.push_str(" }}");
            }
        }
    }
    out
}

/// True when the string carries a `${{ … }}` expression QED won't expand. A
/// `${{ matrix.<key> }}` reference to `resolved_matrix_key` (the target
/// dimension R533-F9 carried as a step matrix) *does* resolve natively, so it
/// is not counted; every other expression — `github.*`, an unlifted matrix key
/// — is unresolved.
fn has_unresolved_expression(s: &ExprString, resolved_matrix_key: Option<&str>) -> bool {
    s.tokens.iter().any(|t| match t {
        ExprToken::Literal(_) => false,
        ExprToken::Expr(raw) => match (raw.trim().strip_prefix("matrix."), resolved_matrix_key) {
            (Some(k), Some(rk)) => k.trim() != rk,
            _ => true,
        },
    })
}

/// Every distinct `secrets.NAME` referenced by a step's script or its `env:`,
/// sorted and deduped.
///
/// Scans both surfaces because the `env:` case is the common and the more
/// dangerous one — `env: { TOKEN: "${{ secrets.X }}" }` is how nearly every
/// workflow hands a credential to a `run:` block, and it is precisely the shape
/// that lowers to a plausible-looking non-empty literal.
fn referenced_secrets(step: &Step, body: &ExprString) -> Vec<String> {
    let mut names: Vec<String> = std::iter::once(body)
        .chain(step.env.values())
        .flat_map(|s| s.tokens.iter())
        .filter_map(|t| match t {
            ExprToken::Expr(raw) => raw.trim().strip_prefix("secrets."),
            ExprToken::Literal(_) => None,
        })
        // `secrets.GITHUB_TOKEN` is tier-3 by nature and already reported by the
        // service-touch classifier; listing it here as "bridge this" would point
        // the human at a fix that cannot work — QED issues no GitHub tokens.
        .map(|n| n.trim().to_string())
        .filter(|n| !n.is_empty() && n != "GITHUB_TOKEN")
        .collect();
    names.sort();
    names.dedup();
    names
}

/// Sanitize a workflow name into a pipeline-name slug: lowercase, non-alnum runs
/// collapsed to a single `-`, trimmed. Empty → `fallback`.
fn slugify(name: &str, fallback: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_dash = false;
        } else if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }
    let slug = out.trim_matches('-').to_string();
    if slug.is_empty() {
        fallback.to_string()
    } else {
        slug
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse + transform an inline workflow.
    fn xf(src: &str) -> TransformReport {
        transform_workflow_src(src).expect("parse")
    }

    /// The native step emitted for `job`'s step at `idx`.
    fn native_at<'a>(r: &'a TransformReport, job: &str, idx: usize) -> &'a QedStep {
        r.steps
            .iter()
            .find(|s| s.job == job && s.step_index == idx)
            .and_then(|s| s.native.as_ref())
            .unwrap_or_else(|| panic!("no native step at {job}[{idx}]"))
    }

    /// The flags on `job`'s step at `idx`.
    fn flags_at<'a>(r: &'a TransformReport, job: &str, idx: usize) -> &'a [FlagKind] {
        &r.steps
            .iter()
            .find(|s| s.job == job && s.step_index == idx)
            .unwrap_or_else(|| panic!("no step at {job}[{idx}]"))
            .flags
    }

    const RUN_JOB: &str = r#"
name: ci
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Compile
        run: cargo build --release
        env:
          RUSTFLAGS: "-D warnings"
        working-directory: app
        timeout-minutes: 20
        continue-on-error: true
"#;

    #[test]
    fn run_step_maps_to_native_subprocess() {
        let r = xf(RUN_JOB);
        let step = native_at(&r, "build", 0);
        assert_eq!(step.name, "build: Compile");
        assert_eq!(step.kind, crate::types::StepKind::Subprocess);
        assert_eq!(step.argv[0], "bash");
        assert_eq!(step.argv[1], "-c");
        assert!(step.argv[2].starts_with("set -eo pipefail\n"));
        assert!(step.argv[2].contains("cargo build --release"));
        assert_eq!(step.env.get("RUSTFLAGS").map(String::as_str), Some("-D warnings"));
        assert_eq!(step.cwd.as_deref(), Some("app"));
        assert_eq!(step.timeout, Some(20 * 60));
        assert!(matches!(step.on_fail, OnFail::Continue));
        assert!(flags_at(&r, "build", 0).is_empty(), "clean compute → no flags");
    }

    #[test]
    fn pipeline_name_is_slugified_from_workflow_name() {
        let r = xf("name: My Release Flow!\non: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: true\n");
        assert_eq!(r.name, "my-release-flow");
        assert_eq!(r.label, "My Release Flow!");
    }

    #[test]
    fn unnamed_workflow_falls_back() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: true\n");
        assert_eq!(r.name, "imported-workflow");
    }

    #[test]
    fn tier3_checkout_is_flagged_not_mapped() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/checkout@v4\n");
        let step = &r.steps[0];
        assert!(step.native.is_none(), "tier-3 emits no native step");
        assert_eq!(
            step.flags,
            vec![FlagKind::ReplaceWithNative(NativeReplacement::Checkout)]
        );
        assert_eq!(step.flags[0].severity(), FlagSeverity::Replace);
        assert!(step.flags[0].stanza_hint().contains("checkout is implicit"));
    }

    #[test]
    fn tier3_upload_artifact_proposes_content_addressed_output() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/upload-artifact@v4\n");
        assert_eq!(
            r.steps[0].flags,
            vec![FlagKind::ReplaceWithNative(NativeReplacement::UploadArtifact)]
        );
        assert!(r.steps[0].flags[0].stanza_hint().contains("content-addressed output"));
    }

    #[test]
    fn compute_uses_is_a_toolkit_action_flag() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/setup-node@v4\n");
        let step = &r.steps[0];
        assert!(step.native.is_none(), "no subprocess until the T7 executor");
        assert_eq!(
            step.flags,
            vec![FlagKind::ToolkitAction {
                slug: "actions/setup-node".into(),
                git_ref: Some("v4".into()),
            }]
        );
        assert_eq!(step.flags[0].severity(), FlagSeverity::Info);
    }

    #[test]
    fn unknown_uses_is_flagged_for_review() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: some-org/exotic@v1\n");
        let step = &r.steps[0];
        assert!(step.native.is_none());
        assert_eq!(step.flags, vec![FlagKind::Unknown { slug: "some-org/exotic".into() }]);
        assert_eq!(step.flags[0].severity(), FlagSeverity::Review);
    }

    #[test]
    fn run_step_with_gh_cli_runs_but_is_flagged() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: gh release create v1 ./dist/*\n");
        let step = &r.steps[0];
        assert!(step.native.is_some(), "still runs on the executor");
        assert_eq!(
            step.flags,
            vec![FlagKind::EmbeddedServiceTouch(vec![ServiceTouch::GhCli])]
        );
        assert_eq!(step.flags[0].severity(), FlagSeverity::Review);
    }

    #[test]
    fn run_step_with_expression_is_flagged_unresolved() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: echo ${{ github.sha }}\n");
        let step = &r.steps[0];
        assert!(step.native.is_some());
        // The rendered script preserves the GHA expression verbatim.
        assert!(step.native.as_ref().unwrap().argv[2].contains("${{ github.sha }}"));
        assert!(step.flags.contains(&FlagKind::UnresolvedExpression));
    }

    #[test]
    fn jobs_flatten_in_topological_order() {
        let src = r#"
on: push
jobs:
  publish:
    needs: build
    runs-on: x
    steps:
      - run: echo publish
  build:
    runs-on: x
    steps:
      - run: echo build
"#;
        let r = xf(src);
        // `build` (no needs) must precede `publish` (needs: build) even though
        // it is declared second.
        let jobs: Vec<&str> = r.steps.iter().map(|s| s.job.as_str()).collect();
        assert_eq!(jobs, vec!["build", "publish"]);
    }

    #[test]
    fn shell_variants_select_the_right_interpreter() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: print(1)\n        shell: python\n");
        let step = native_at(&r, "a", 0);
        assert_eq!(step.argv[0], "python3");
        assert_eq!(step.argv[1], "-c");
        assert_eq!(step.argv[2], "print(1)");
    }

    #[test]
    fn report_accessors_partition_native_and_flagged() {
        let src = r#"
on: push
jobs:
  a:
    runs-on: x
    steps:
      - run: cargo test
      - uses: actions/checkout@v4
"#;
        let r = xf(src);
        assert_eq!(r.collect_native().len(), 1, "only the run step is native");
        assert_eq!(r.flags().count(), 1, "only checkout flags");
        assert!(!r.is_clean(), "a tier-3 step is present");
    }

    #[test]
    fn unnamed_run_step_gets_positional_name() {
        let r = xf("on: push\njobs:\n  b:\n    runs-on: x\n    steps:\n      - run: make\n");
        assert_eq!(native_at(&r, "b", 0).name, "b: step 0");
    }

    // ── R533-F9: import-time target lifting ───────────────────────────────

    #[test]
    fn concrete_target_lifts_into_platform_no_matrix() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: cargo build --target x86_64-unknown-linux-musl --release\n");
        let step = native_at(&r, "a", 0);
        let p = step.platform.as_ref().expect("platform lifted");
        assert_eq!(p.target.as_deref(), Some("x86_64-unknown-linux-musl"));
        assert!(step.matrix.is_none(), "a concrete target needs no matrix");
    }

    #[test]
    fn target_equals_form_is_recognized() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: cross build --target=aarch64-unknown-linux-gnu\n");
        let p = native_at(&r, "a", 0).platform.as_ref().expect("platform");
        assert_eq!(p.target.as_deref(), Some("aarch64-unknown-linux-gnu"));
    }

    #[test]
    fn target_dir_is_not_mistaken_for_target() {
        let r = xf("on: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: cargo build --target-dir /tmp/out\n");
        assert!(native_at(&r, "a", 0).platform.is_none(), "--target-dir is not --target");
    }

    #[test]
    fn step_without_target_has_no_platform() {
        let r = xf(RUN_JOB);
        assert!(native_at(&r, "build", 0).platform.is_none());
    }

    #[test]
    fn matrix_target_dimension_lifts_and_carries_step_matrix() {
        let src = r#"
on: push
jobs:
  build:
    runs-on: x
    strategy:
      matrix:
        target:
          - x86_64-unknown-linux-musl
          - aarch64-unknown-linux-musl
    steps:
      - run: cross build --target ${{ matrix.target }}
"#;
        let r = xf(src);
        let step = native_at(&r, "build", 0);
        // platform.target holds the (QED-native) matrix reference …
        assert_eq!(
            step.platform.as_ref().unwrap().target.as_deref(),
            Some("${{ matrix.target }}")
        );
        // … and the target dimension rides along as a step matrix so QED fans it.
        let m = step.matrix.as_ref().expect("step matrix carried");
        let vals = m.dimensions.get("target").expect("target dimension");
        assert_eq!(vals.len(), 2);
        // The matrix.target reference is *resolved* natively → no unresolved flag.
        assert!(flags_at(&r, "build", 0).is_empty());
    }

    #[test]
    fn matrix_target_from_include_rows() {
        // Release-shaped include-only matrix: targets live on include rows.
        let src = r#"
on: push
jobs:
  cli:
    runs-on: x
    strategy:
      matrix:
        include:
          - target: x86_64-apple-darwin
          - target: aarch64-apple-darwin
    steps:
      - run: cargo build --target ${{ matrix.target }}
"#;
        let r = xf(src);
        let m = native_at(&r, "cli", 0).matrix.as_ref().expect("matrix from include rows");
        let vals = m.dimensions.get("target").expect("target dimension");
        assert_eq!(vals.len(), 2);
    }

    // (Native step-matrix expansion concretizing platform.target end-to-end is
    // covered by matrix::tests::step_matrix_substitutes_lifted_platform_target.)

    /// Locate yah's live `release.yml` by ascending to the `.github/workflows`
    /// marker. Absent in the standalone export mirror → the fixture test skips.
    ///
    /// `oss/qed` (this crate's own exportable subtree) carries a decoy
    /// `.github/workflows/release.yml` of its own — a crates.io-publish
    /// workflow for the standalone mirror, single `publish` job — which sits
    /// *closer* to `CARGO_MANIFEST_DIR` than the monorepo's real CI workflow.
    /// An ancestor-walk that stops at the first match silently transforms the
    /// wrong file. `.yah/camp.toml` only exists at the true monorepo root
    /// (oss/* subtrees are deliberately un-anchored so they stay transparent
    /// to the camp's ticket index) — require it alongside the workflow file so
    /// the walk skips the decoy and keeps ascending to the real root.
    fn release_yml() -> Option<String> {
        let mut dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        loop {
            let cand = dir.join(".github/workflows/release.yml");
            if cand.is_file() && dir.join(".yah/camp.toml").is_file() {
                return std::fs::read_to_string(cand).ok();
            }
            if !dir.pop() {
                return None;
            }
        }
    }

    #[test]
    fn env_secret_is_flagged_for_review_not_buried_in_info() {
        // The smoke-sweeper shape: a credential handed to bash via `env:`, then
        // guarded with `-z`. Lowered literally, the guard passes and the step
        // calls a live API with `${{ secrets.X }}` as its bearer token.
        let r = xf(r#"
on: [push]
jobs:
  sweep:
    runs-on: ubuntu-latest
    steps:
      - name: Sweep
        env:
          HETZNER_API_TOKEN: ${{ secrets.HETZNER_API_TOKEN }}
        run: |
          if [ -z "$HETZNER_API_TOKEN" ]; then exit 0; fi
          curl -H "Authorization: Bearer $HETZNER_API_TOKEN" https://api.example.com
"#);
        let flag = r
            .flags()
            .map(|(_, f)| f)
            .find(|f| matches!(f, FlagKind::UnbridgedSecret { .. }))
            .expect("env secret is flagged");
        assert_eq!(
            flag,
            &FlagKind::UnbridgedSecret { names: vec!["HETZNER_API_TOKEN".into()] }
        );
        assert_eq!(
            flag.severity(),
            FlagSeverity::Review,
            "a credential that lowers to a plausible non-empty literal is not informational"
        );
        assert!(flag.stanza_hint().contains("secrets.toml"));
    }

    #[test]
    fn secrets_in_the_script_body_are_collected_deduped_and_sorted() {
        let r = xf(r#"
on: [push]
jobs:
  pub:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo "${{ secrets.R2_SECRET }}" > /dev/null
          echo "${{ secrets.CF_TOKEN }}"
          echo "${{ secrets.R2_SECRET }}"
"#);
        let flag = r
            .flags()
            .map(|(_, f)| f)
            .find(|f| matches!(f, FlagKind::UnbridgedSecret { .. }))
            .expect("script secrets are flagged");
        assert_eq!(
            flag,
            &FlagKind::UnbridgedSecret {
                names: vec!["CF_TOKEN".into(), "R2_SECRET".into()]
            }
        );
    }

    #[test]
    fn github_token_is_not_reported_as_bridgeable() {
        // QED issues no GitHub tokens, so "bridge this in secrets.toml" would be
        // advice that cannot be followed. The service-touch classifier already
        // owns this case.
        let r = xf(r#"
on: [push]
jobs:
  rel:
    runs-on: ubuntu-latest
    steps:
      - env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: gh release upload v1 ./dist/x
"#);
        assert!(
            !r.flags().any(|(_, f)| matches!(f, FlagKind::UnbridgedSecret { .. })),
            "GITHUB_TOKEN must not be offered as a bridgeable secret"
        );
    }

    #[test]
    fn a_step_with_no_secrets_raises_no_secret_flag() {
        let r = xf(r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - env:
          RUSTFLAGS: -D warnings
        run: cargo build --release
"#);
        assert!(!r.flags().any(|(_, f)| matches!(f, FlagKind::UnbridgedSecret { .. })));
    }

    #[test]
    fn release_yml_transforms_end_to_end() {
        let Some(src) = release_yml() else {
            eprintln!("skip: yah workflow fixtures not present");
            return;
        };
        let r = xf(&src);
        assert!(!r.steps.is_empty(), "release.yml has steps");

        // checkout → flagged tier-3, never a native step.
        let checkout = r
            .steps
            .iter()
            .find(|s| s.job == "smoke" && s.step_index == 0)
            .expect("smoke step 0");
        assert!(checkout.native.is_none());
        assert!(checkout
            .flags
            .contains(&FlagKind::ReplaceWithNative(NativeReplacement::Checkout)));

        // The image jobs' build-push → registry-publish replacement.
        assert!(r.flags().any(|(_, f)| matches!(
            f,
            FlagKind::ReplaceWithNative(NativeReplacement::RegistryPublish)
        )));

        // The cargo build `run:` steps map to native bash subprocesses.
        assert!(
            r.native_steps().any(|s| s.argv.first().map(String::as_str) == Some("bash")
                && s.argv.last().is_some_and(|c| c.contains("cargo"))),
            "at least one native cargo build step",
        );

        // Topo linearization: cli-build precedes smoke, smoke precedes publish.
        let first_idx = |job: &str| r.steps.iter().position(|s| s.job == job);
        let (build, smoke, publish) =
            (first_idx("cli-build"), first_idx("smoke"), first_idx("publish-cli"));
        if let (Some(b), Some(s), Some(p)) = (build, smoke, publish) {
            assert!(b < s, "cli-build before smoke");
            assert!(s < p, "smoke before publish-cli");
        }
    }
}
