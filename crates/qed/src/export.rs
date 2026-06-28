//! Export-with-degradation: QED → GitHub Actions (R533-F8, W224).
//!
//! W224 settles the QED↔GHA boundary as **import, not emulate**, and is
//! explicit that the *reverse* direction is asymmetric: "QED→GHA can only emit
//! GHA that shells out to `yah` in `run:` steps (content-addressed /
//! atomic-release features don't map back). Useful for *keep GitHub as a
//! fallback CI*; build it second; **never call it lossless**."
//!
//! This module is that second direction, and it is honest about the loss:
//! [`export_pipeline`] returns an [`ExportReport`] whose `degradations` list
//! enumerates every feature that did not survive the round-trip, and the
//! emitted YAML carries the same notes as `# degraded:` comments. The caller
//! (and a future `qed export` CLI) surfaces them; nothing here pretends the
//! export is faithful.
//!
//! ## Two modes, picked automatically
//!
//! - **Portable** — every step is a plain [`StepKind::Subprocess`]. These map
//!   cleanly onto GHA `run:` steps, so the workflow runs the *real* build/test
//!   commands on a GitHub runner. This is the high-value case (a `check` /
//!   `smoke` pipeline becomes a usable GitHub workflow).
//! - **Wholesale shim** — the pipeline has any native-only step kind
//!   (`build-image`, `package-native-tarball`, `sub-pipeline`, `import`, …).
//!   GitHub has no native equivalent and the `yah qed` CLI runs *whole*
//!   pipelines (not single steps), so the only honest emission is a workflow
//!   that checks out the repo and shells `yah qed run <name>` — GitHub as a
//!   trigger that delegates to QED. Every native feature is recorded as a
//!   degradation.
//!
//! Outcomes ([`Outcome::Publish`] and friends), pipeline-chain triggers,
//! `LocalOnly` placement, `OnFail::Retry`, and step `produces` are recorded as
//! degradations in *either* mode — a portable workflow still builds and tests,
//! it just doesn't publish (content-addressed release stays native; the note
//! points the operator at `yah qed run <name>`).

use crate::types::{
    Outcome, OnFail, Pipeline, Placement, QedStep, RunStatus, StepKind, Trigger,
};

/// One feature that did not survive the QED → GHA export. The `feature` tag is
/// a stable short identifier (`outcome:publish`, `step-kind:build-image`, …);
/// `detail` is the human explanation that also rides the YAML as a comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Degradation {
    /// Where the loss occurred — a step name, or `"triggers"` / `"placement"` /
    /// `"on_success"` / `"on_fail"`.
    pub site: String,
    /// Stable short tag for the lossy feature.
    pub feature: String,
    /// Human-readable explanation of what GHA can't do and the native fallback.
    pub detail: String,
}

/// The result of exporting a [`Pipeline`] to GitHub Actions.
#[derive(Debug, Clone)]
pub struct ExportReport {
    /// The emitted `.github/workflows/<name>.yml` text. Always valid YAML; a
    /// best-effort fallback CI, **never a faithful mirror** of the pipeline.
    pub yaml: String,
    /// Every feature that did not map. Empty only for a fully-portable pipeline
    /// with no outcomes, no special triggers, and `Anywhere` placement.
    pub degradations: Vec<Degradation>,
    /// `true` when the pipeline degraded to the wholesale `yah qed run <name>`
    /// shim (it had native-only step kinds), `false` when steps rendered
    /// faithfully.
    pub shimmed: bool,
}

impl ExportReport {
    /// Whether anything was lost. The export is **never** claimed lossless when
    /// this is `true`; callers should surface [`Self::degradations`].
    pub fn is_lossy(&self) -> bool {
        !self.degradations.is_empty()
    }
}

/// Export a QED [`Pipeline`] to a GitHub Actions workflow, degrading every
/// native-only feature explicitly. See the module docs for the two modes.
pub fn export_pipeline(pipeline: &Pipeline) -> ExportReport {
    let mut degradations: Vec<Degradation> = Vec::new();

    // Triggers + placement degrade identically in both modes.
    let on_block = render_on(&pipeline.triggers, &mut degradations);
    if pipeline.placement == Placement::LocalOnly {
        degradations.push(Degradation {
            site: "placement".into(),
            feature: "placement:local-only".into(),
            detail: "pipeline is local-only (its output is meaningless on a clean CI runner); \
                     exported anyway, but a GitHub run likely produces nothing useful"
                .into(),
        });
    }

    // Outcomes never map — content-addressed publish / atomic release / vendor
    // adapters are native. Record them regardless of mode; the GHA workflow
    // builds + tests but does not publish.
    record_outcome_degradations("on_success", &pipeline.on_success, &mut degradations);
    record_outcome_degradations("on_fail", &pipeline.on_fail, &mut degradations);

    let native_steps: Vec<&QedStep> = pipeline
        .steps
        .iter()
        .filter(|s| !is_portable_kind(&s.kind))
        .collect();
    let shimmed = !native_steps.is_empty();

    let job_id = sanitize_job_id(&pipeline.name);
    let steps_yaml = if shimmed {
        // Record every native step as a degradation, then emit the single
        // delegating shim.
        for s in &native_steps {
            degradations.push(Degradation {
                site: s.name.clone(),
                feature: format!("step-kind:{}", kind_tag(&s.kind)),
                detail: format!(
                    "`{}` is a native QED step ({}) with no GitHub-native equivalent; \
                     the whole pipeline runs via the `yah qed run` shim instead",
                    s.name,
                    kind_tag(&s.kind),
                ),
            });
        }
        render_shim_steps(&pipeline.name)
    } else {
        render_faithful_steps(&pipeline.steps, &mut degradations)
    };

    let yaml = render_workflow(pipeline, &job_id, &on_block, &steps_yaml, &degradations, shimmed);
    ExportReport { yaml, degradations, shimmed }
}

/// Only a plain subprocess maps to a GHA `run:` step. Every other kind is a
/// native QED facility (image build, native packaging/signing, composition,
/// import) that GitHub can't reproduce.
fn is_portable_kind(kind: &StepKind) -> bool {
    matches!(kind, StepKind::Subprocess)
}

fn kind_tag(kind: &StepKind) -> &'static str {
    match kind {
        StepKind::Subprocess => "subprocess",
        StepKind::BuildImage => "build-image",
        StepKind::PackageNativeTarball => "package-native-tarball",
        StepKind::MuslStaticPreflight => "musl-static-preflight",
        StepKind::SignNativeTarball => "sign-native-tarball",
        StepKind::SubPipeline => "sub-pipeline",
        StepKind::GhaWorkflow => "gha-workflow",
        StepKind::Import => "import",
        StepKind::WaitFor => "wait-for",
    }
}

/// Render the `on:` trigger block (already indented two spaces under `on:`).
/// Pipeline-chain triggers have no faithful GHA mapping — best-effort to
/// `workflow_run` with a degradation; an empty trigger list defaults to
/// `workflow_dispatch` (QED's implicit `Manual`).
fn render_on(triggers: &[Trigger], degradations: &mut Vec<Degradation>) -> String {
    let mut out = String::new();
    let mut emitted_dispatch = false;
    let effective = if triggers.is_empty() {
        std::slice::from_ref(&Trigger::Manual)
    } else {
        triggers
    };
    let mut tags: Vec<&str> = Vec::new();
    let mut crons: Vec<&str> = Vec::new();
    for t in effective {
        match t {
            Trigger::Manual => {
                if !emitted_dispatch {
                    out.push_str("  workflow_dispatch:\n");
                    emitted_dispatch = true;
                }
            }
            Trigger::Tag { pattern } => tags.push(pattern),
            Trigger::Schedule { cron } => crons.push(cron),
            Trigger::Pipeline { id, status } => {
                degradations.push(Degradation {
                    site: "triggers".into(),
                    feature: "trigger:pipeline-chain".into(),
                    detail: format!(
                        "pipeline-completion trigger (`{id}` = {status:?}) has no GHA equivalent; \
                         emitted as a best-effort `workflow_run` (fires on ANY completion of \
                         `{id}` — the {status:?} status filter is not expressible in GHA)"
                    ),
                });
                out.push_str("  workflow_run:\n");
                out.push_str(&format!("    workflows: [{}]\n", yaml_flow_scalar(id)));
                out.push_str("    types: [completed]\n");
            }
        }
    }
    if !tags.is_empty() {
        out.push_str("  push:\n    tags:\n");
        for pat in tags {
            out.push_str(&format!("      - {}\n", yaml_flow_scalar(pat)));
        }
    }
    if !crons.is_empty() {
        out.push_str("  schedule:\n");
        for cron in crons {
            out.push_str(&format!("    - cron: {}\n", yaml_flow_scalar(cron)));
        }
    }
    out
}

fn record_outcome_degradations(site: &str, outcomes: &[Outcome], degradations: &mut Vec<Degradation>) {
    for o in outcomes {
        let (feature, detail) = match o {
            Outcome::Publish { .. } => (
                "outcome:publish",
                "content-addressed release publish is a native QED facility (atomic-release \
                 model); GHA does not publish — run `yah qed run` to publish",
            ),
            Outcome::WardenDeploy { .. } => (
                "outcome:yubaba-deploy",
                "yubaba deploy is native; GHA cannot perform it — delegate to `yah qed run`",
            ),
            Outcome::AlmanacRun { .. } => (
                "outcome:almanac-run",
                "almanac dispatch is native; GHA cannot perform it — delegate to `yah qed run`",
            ),
            Outcome::Provider { .. } => (
                "outcome:provider",
                "vendor release adapter (notarize/sparkle/…) is native; GHA cannot perform it — \
                 delegate to `yah qed run`",
            ),
        };
        degradations.push(Degradation {
            site: site.into(),
            feature: feature.into(),
            detail: detail.into(),
        });
    }
}

/// Faithful per-step rendering for a portable pipeline. Each subprocess step
/// becomes a `run:` step; `produces` / `OnFail::Retry` are recorded as
/// degradations (the step still runs).
fn render_faithful_steps(steps: &[QedStep], degradations: &mut Vec<Degradation>) -> String {
    let mut out = String::new();
    // GHA runners start empty; QED runs in the camp with the source already
    // present. A fallback CI needs an explicit checkout.
    out.push_str("      # QED runs in-camp with the source already present; GHA needs checkout.\n");
    out.push_str("      - name: Checkout\n");
    out.push_str("        uses: actions/checkout@v4\n");

    for step in steps {
        if !step.produces.is_empty() {
            degradations.push(Degradation {
                site: step.name.clone(),
                feature: "produces".into(),
                detail: format!(
                    "step `{}` declares release artifacts; GHA builds them but does not publish \
                     (content-addressed release is native) — run `yah qed run` to publish",
                    step.name
                ),
            });
        }

        out.push_str(&format!("      - name: {}\n", yaml_flow_scalar(&step.name)));
        out.push_str(&render_run(&step.argv));
        if let Some(cwd) = &step.cwd {
            out.push_str(&format!("        working-directory: {}\n", yaml_flow_scalar(cwd)));
        }
        if let Some(timeout) = step.timeout {
            // QED timeouts are seconds; GHA `timeout-minutes` is minutes — round up.
            let minutes = timeout.div_ceil(60).max(1);
            out.push_str(&format!("        timeout-minutes: {minutes}\n"));
        }
        match &step.on_fail {
            OnFail::Continue => out.push_str("        continue-on-error: true\n"),
            OnFail::Retry { max } => degradations.push(Degradation {
                site: step.name.clone(),
                feature: "on-fail:retry".into(),
                detail: format!(
                    "step `{}` retries up to {max}× on failure; GHA has no built-in step retry \
                     (the step runs once)",
                    step.name
                ),
            }),
            OnFail::Abort => {}
        }
        if !step.env.is_empty() {
            out.push_str("        env:\n");
            // Deterministic order — HashMap iteration isn't stable.
            let mut keys: Vec<&String> = step.env.keys().collect();
            keys.sort();
            for k in keys {
                out.push_str(&format!("          {}: {}\n", k, yaml_flow_scalar(&step.env[k])));
            }
        }
    }
    out
}

/// The wholesale shim: checkout + `yah qed run <name>`. GitHub becomes a
/// trigger that delegates the whole native pipeline to the `yah` CLI.
fn render_shim_steps(pipeline_name: &str) -> String {
    let mut out = String::new();
    out.push_str("      - name: Checkout\n");
    out.push_str("        uses: actions/checkout@v4\n");
    out.push_str("      # This pipeline has native QED steps with no GHA equivalent; GitHub\n");
    out.push_str("      # delegates the whole run to the `yah` CLI (requires `yah` on PATH).\n");
    out.push_str("      - name: Run QED pipeline\n");
    out.push_str(&render_run(&[
        "yah".to_string(),
        "qed".to_string(),
        "run".to_string(),
        pipeline_name.to_string(),
    ]));
    out
}

/// Render a `run:` field from an argv list. Single-token-per-line is not GHA's
/// model; we join the argv into one shell command, quoting tokens that need it.
fn render_run(argv: &[String]) -> String {
    if argv.is_empty() {
        // A subprocess with no argv is degenerate; emit a no-op so the YAML
        // stays valid (the degradation, if any, is recorded by the caller).
        return "        run: ':'\n".to_string();
    }
    let cmd = argv.iter().map(|a| sh_quote(a)).collect::<Vec<_>>().join(" ");
    format!("        run: {}\n", yaml_flow_scalar(&cmd))
}

/// Assemble the full workflow document: degradation header, `name:`, `on:`, and
/// the single job.
fn render_workflow(
    pipeline: &Pipeline,
    job_id: &str,
    on_block: &str,
    steps_yaml: &str,
    degradations: &[Degradation],
    shimmed: bool,
) -> String {
    let mut out = String::new();
    out.push_str("# @qed:exported (R533-F8, W224) — QED → GitHub Actions, export-with-degradation.\n");
    out.push_str("# This is a best-effort FALLBACK CI, NOT a faithful mirror of the QED pipeline.\n");
    if shimmed {
        out.push_str("# Mode: wholesale shim (native steps delegate to `yah qed run`).\n");
    } else {
        out.push_str("# Mode: faithful (portable subprocess steps mapped to run: steps).\n");
    }
    if degradations.is_empty() {
        out.push_str("# No degradations: this pipeline is fully portable.\n");
    } else {
        out.push_str(&format!("# {} degradation(s):\n", degradations.len()));
        for d in degradations {
            out.push_str(&format!("#   - [{}] {}: {}\n", d.feature, d.site, d.detail));
        }
    }
    out.push('\n');

    out.push_str(&format!("name: {}\n", yaml_flow_scalar(&pipeline.label)));
    out.push_str("on:\n");
    out.push_str(on_block);
    out.push_str("jobs:\n");
    out.push_str(&format!("  {job_id}:\n"));
    out.push_str("    runs-on: ubuntu-latest\n");
    out.push_str("    steps:\n");
    out.push_str(steps_yaml);
    out
}

/// GHA job ids must match `[A-Za-z_][A-Za-z0-9_-]*`. Slug the pipeline name and
/// prefix a letter if it would otherwise start with a digit.
fn sanitize_job_id(name: &str) -> String {
    let mut s: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect();
    if s.is_empty() {
        return "qed".to_string();
    }
    let first = s.chars().next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        s.insert(0, 'j');
        s.insert(1, '-');
    }
    s
}

/// Shell-quote one argv token for embedding in a `run:` command. Single-quote
/// (POSIX literal) when it contains anything beyond a safe shell-word charset.
fn sh_quote(token: &str) -> String {
    let safe = !token.is_empty()
        && token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/' | '=' | ':' | '@' | '+' | ','));
    if safe {
        token.to_string()
    } else {
        // POSIX single-quote: close, escaped literal quote, reopen.
        format!("'{}'", token.replace('\'', "'\\''"))
    }
}

/// Render a YAML scalar in flow position (after `key: ` or inside `[ ]`). Plain
/// when it's a safe word that can't be misread as a bool/number/null/indicator,
/// otherwise single-quoted (YAML literal: internal `'` is doubled).
fn yaml_flow_scalar(s: &str) -> String {
    if is_plain_safe(s) {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "''"))
    }
}

fn is_plain_safe(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    // Reserved scalars that YAML would interpret as non-strings.
    const RESERVED: &[&str] = &[
        "true", "false", "yes", "no", "on", "off", "null", "~", "True", "False",
        "Yes", "No", "On", "Off", "Null", "NULL", "TRUE", "FALSE",
    ];
    if RESERVED.contains(&s) {
        return false;
    }
    // Anything number-shaped quotes (so `1.0`, `0755`, `1e3` stay strings).
    if s.chars().all(|c| c.is_ascii_digit() || matches!(c, '.' | '-' | '+' | 'e' | 'E')) {
        return false;
    }
    let first = s.chars().next().unwrap();
    // YAML indicator characters that aren't allowed to start a plain scalar.
    if matches!(
        first,
        '!' | '&' | '*' | '-' | '?' | '{' | '}' | '[' | ']' | ',' | '#' | '|'
            | '>' | '@' | '`' | '"' | '\'' | '%' | ' '
    ) {
        return false;
    }
    if s.ends_with(' ') {
        return false;
    }
    // Reject anything containing structural / comment indicators that change
    // meaning mid-scalar (`: ` mapping sep, ` #` comment, flow punctuation).
    if s.contains(": ") || s.ends_with(':') || s.contains(" #") || s.contains('\n') {
        return false;
    }
    s.chars().all(|c| {
        c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/' | '=' | ':' | '@' | '+' | ' ')
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ProducedArtifact;
    use std::collections::HashMap;

    fn step(name: &str, argv: &[&str]) -> QedStep {
        QedStep {
            background: false,
            background_until: None,
            wait_for: None,
            name: name.into(),
            argv: argv.iter().map(|s| s.to_string()).collect(),
            ..base_step()
        }
    }

    fn base_step() -> QedStep {
        // Construct via TOML so we don't have to track every QedStep field.
        toml::from_str(r#"name = "x""#).expect("minimal QedStep")
    }

    fn pipeline(name: &str, steps: Vec<QedStep>) -> Pipeline {
        Pipeline {
            name: name.into(),
            label: name.into(),
            steps,
            params: HashMap::new(),
            on_success: Vec::new(),
            on_fail: Vec::new(),
            triggers: Vec::new(),
            concurrency_key: None,
            placement: Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::default(),
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        }
    }

    /// A YAML sanity check: the emitted document parses as a mapping with the
    /// expected top-level keys. (qed-gha bundles serde_yaml; reuse its parser
    /// transitively is not available here, so we assert structurally on text.)
    fn assert_well_formed(yaml: &str) {
        assert!(yaml.contains("\nname: "), "has name:");
        assert!(yaml.contains("\non:\n"), "has on:");
        assert!(yaml.contains("\njobs:\n"), "has jobs:");
        assert!(yaml.contains("runs-on: ubuntu-latest"), "has runs-on");
        // Header always present and honest about not being lossless.
        assert!(yaml.contains("NOT a faithful mirror"), "never-lossless header");
    }

    #[test]
    fn portable_pipeline_renders_faithfully() {
        let p = pipeline("check", vec![
            step("Typecheck", &["cargo", "check", "--workspace"]),
            step("Test", &["cargo", "test"]),
        ]);
        let r = export_pipeline(&p);
        assert!(!r.shimmed, "all-subprocess pipeline renders faithfully");
        assert!(r.degradations.is_empty(), "no degradations for a portable pipeline: {:?}", r.degradations);
        assert!(!r.is_lossy());
        assert_well_formed(&r.yaml);
        // Real commands present; checkout prepended; manual → workflow_dispatch.
        assert!(r.yaml.contains("run: cargo check --workspace"));
        assert!(r.yaml.contains("uses: actions/checkout@v4"));
        assert!(r.yaml.contains("workflow_dispatch:"));
        assert!(r.yaml.contains("\n  check:\n"), "job id from pipeline name");
    }

    #[test]
    fn native_kind_degrades_to_wholesale_shim() {
        let mut img = step("Build image", &[]);
        img.kind = StepKind::BuildImage;
        let p = pipeline("release-build", vec![
            step("Compile", &["cargo", "build", "--release"]),
            img,
        ]);
        let r = export_pipeline(&p);
        assert!(r.shimmed, "a build-image step forces the wholesale shim");
        assert!(r.is_lossy());
        // The shim delegates the whole pipeline; the faithful Compile step is
        // NOT rendered (the shim re-runs everything via yah).
        assert!(r.yaml.contains("run: yah qed run release-build"));
        assert!(!r.yaml.contains("cargo build --release"), "shim mode does not render subprocess steps");
        assert!(r.degradations.iter().any(|d| d.feature == "step-kind:build-image"));
    }

    #[test]
    fn outcomes_degrade_but_steps_still_render() {
        let mut p = pipeline("release", vec![step("Build", &["cargo", "build"])]);
        p.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
        }];
        let r = export_pipeline(&p);
        assert!(!r.shimmed, "subprocess-only steps still render faithfully");
        assert!(r.yaml.contains("run: cargo build"));
        assert!(r.is_lossy(), "the publish outcome is a degradation");
        assert!(r.degradations.iter().any(|d| d.feature == "outcome:publish"));
        // The loss is visible in the YAML, not just the report.
        assert!(r.yaml.to_lowercase().contains("publish"));
    }

    #[test]
    fn triggers_map_and_pipeline_chain_degrades() {
        let mut p = pipeline("nightly", vec![step("Smoke", &["./smoke.sh"])]);
        p.triggers = vec![
            Trigger::Tag { pattern: "v*.*.*".into() },
            Trigger::Schedule { cron: "0 0 * * *".into() },
            Trigger::Pipeline { id: "build".into(), status: RunStatus::Success },
        ];
        let r = export_pipeline(&p);
        assert!(r.yaml.contains("push:"));
        assert!(r.yaml.contains("tags:"));
        assert!(r.yaml.contains("- 'v*.*.*'"), "glob tag is quoted");
        assert!(r.yaml.contains("- cron: '0 0 * * *'"));
        assert!(r.yaml.contains("workflow_run:"));
        assert!(r.degradations.iter().any(|d| d.feature == "trigger:pipeline-chain"));
    }

    #[test]
    fn empty_triggers_default_to_workflow_dispatch() {
        let p = pipeline("check", vec![step("Lint", &["cargo", "clippy"])]);
        let r = export_pipeline(&p);
        assert!(r.yaml.contains("workflow_dispatch:"));
    }

    #[test]
    fn local_only_placement_is_a_degradation() {
        let mut p = pipeline("install", vec![step("Install", &["./install.sh"])]);
        p.placement = Placement::LocalOnly;
        let r = export_pipeline(&p);
        assert!(r.degradations.iter().any(|d| d.feature == "placement:local-only"));
    }

    #[test]
    fn retry_and_continue_on_fail_handled() {
        let mut retry = step("Flaky", &["./flaky.sh"]);
        retry.on_fail = OnFail::Retry { max: 3 };
        let mut cont = step("Best effort", &["./maybe.sh"]);
        cont.on_fail = OnFail::Continue;
        let p = pipeline("ci", vec![retry, cont]);
        let r = export_pipeline(&p);
        assert!(r.yaml.contains("continue-on-error: true"), "Continue maps natively");
        assert!(r.degradations.iter().any(|d| d.feature == "on-fail:retry"), "Retry degrades");
    }

    #[test]
    fn produces_degrades_but_step_renders() {
        let mut s = step("Build", &["cargo", "build", "--release"]);
        s.produces = vec![ProducedArtifact {
            binary: "yah".into(),
            path: "target/release/yah".into(),
            triple: None,
        }];
        let p = pipeline("build", vec![s]);
        let r = export_pipeline(&p);
        assert!(!r.shimmed);
        assert!(r.yaml.contains("run: cargo build --release"));
        assert!(r.degradations.iter().any(|d| d.feature == "produces"));
    }

    #[test]
    fn env_cwd_timeout_render_on_steps() {
        let mut s = step("Build", &["make"]);
        s.cwd = Some("crates/app".into());
        s.timeout = Some(90); // 90s → 2 minutes (ceil)
        s.env.insert("RUSTFLAGS".into(), "-D warnings".into());
        let p = pipeline("build", vec![s]);
        let r = export_pipeline(&p);
        assert!(r.yaml.contains("working-directory: crates/app"));
        assert!(r.yaml.contains("timeout-minutes: 2"));
        assert!(r.yaml.contains("RUSTFLAGS: '-D warnings'"), "value with space + leading - is quoted");
    }

    #[test]
    fn yaml_quoting_protects_reserved_and_special_scalars() {
        assert_eq!(yaml_flow_scalar("cargo"), "cargo");
        assert_eq!(yaml_flow_scalar("on"), "'on'");
        assert_eq!(yaml_flow_scalar("true"), "'true'");
        assert_eq!(yaml_flow_scalar("1.0"), "'1.0'");
        assert_eq!(yaml_flow_scalar("a: b"), "'a: b'");
        assert_eq!(yaml_flow_scalar("v*.*.*"), "'v*.*.*'");
        assert_eq!(yaml_flow_scalar("it's"), "'it''s'");
    }

    /// The hardest correctness check: parse the emitted YAML back through the
    /// real GHA parser (qed-gha) and confirm it's a well-formed workflow — not
    /// just text that *looks* like YAML. Covers both modes.
    #[test]
    fn emitted_yaml_parses_as_a_real_gha_workflow() {
        // Faithful mode.
        let p = pipeline("check", vec![
            step("Build", &["cargo", "build"]),
            step("Test", &["cargo", "test", "--workspace"]),
        ]);
        let r = export_pipeline(&p);
        let wf = yah_qed_gha::parse_workflow(&r.yaml).expect("faithful export is valid GHA");
        let job = wf.jobs.get("check").expect("job named after pipeline");
        assert_eq!(job.steps.len(), 3, "checkout + 2 run steps");

        // Shim mode.
        let mut img = step("Build image", &[]);
        img.kind = StepKind::BuildImage;
        let p2 = pipeline("release-build", vec![step("Compile", &["cargo", "build"]), img]);
        let r2 = export_pipeline(&p2);
        let wf2 = yah_qed_gha::parse_workflow(&r2.yaml).expect("shim export is valid GHA");
        let job2 = wf2.jobs.get("release-build").expect("job");
        assert_eq!(job2.steps.len(), 2, "checkout + the yah shim");
    }

    /// Triggers + env quoting survive the real parser too — a regression guard
    /// on the YAML quoting helper against an actual YAML reader.
    #[test]
    fn emitted_triggers_and_env_parse_back() {
        let mut s = step("Build", &["make"]);
        s.env.insert("RUSTFLAGS".into(), "-D warnings".into());
        let mut p = pipeline("nightly", vec![s]);
        p.triggers = vec![
            Trigger::Tag { pattern: "v*.*.*".into() },
            Trigger::Schedule { cron: "0 0 * * *".into() },
        ];
        let r = export_pipeline(&p);
        let wf = yah_qed_gha::parse_workflow(&r.yaml).expect("valid GHA");
        assert!(wf.triggers.push.as_ref().map(|p| !p.tags.is_empty()).unwrap_or(false));
        assert_eq!(wf.triggers.schedule.len(), 1);
    }

    #[test]
    fn job_id_is_sanitized() {
        assert_eq!(sanitize_job_id("release-build"), "release-build");
        assert_eq!(sanitize_job_id("123go"), "j-123go");
        assert_eq!(sanitize_job_id("a b/c"), "a-b-c");
    }
}
