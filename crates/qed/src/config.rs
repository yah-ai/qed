//! @yah:ticket(R299-T4, "Create .yah/qed/ pipeline directory")
//! @yah:at(2026-05-23T01:43:12Z)
//! @yah:status(review)
//! @yah:parent(R299)
//! @yah:handoff(".yah/qed/ directory created at workspace root. PipelineLoader.list_all() now dedupes built-ins + custom files. Camp TOML overrides built-ins by name.")

use crate::peers::PeerConfig;
use crate::registries::{extract_registry_host, RegistryConfig, RegistryConfigError};
use crate::types::{
    GhaWorkflowConfig, ParamDef, Pipeline, Placement, QedStep, StepKind,
    StepValidationError, SubPipelineRef, SubPipelineResolver,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Locate `{dir}/{name}.toml`. The filename IS the pipeline name — a direct
/// join, no directory scan, no alternate spelling.
///
/// This used to prefer a `P{n}-{name}.toml` form and fall back to the bare
/// name. The numbering was never enforceable: nothing assigned the next free
/// number, `yah cloud init` generated unprefixed cards, retiring a pipeline
/// orphaned its number (P003/P006/P007 all became dangling references in
/// prose and `@arch:see` annotations), and on a shared tree two agents adding
/// a pipeline would race for the same integer. Removed R707 — the name is the
/// only handle anything ever dispatched by.
fn find_pipeline_file(dir: &Path, name: &str) -> Option<PathBuf> {
    let path = dir.join(format!("{name}.toml"));
    path.is_file().then_some(path)
}

/// Lift a pipeline file's leading `#` comment block into readme prose
/// (R703-F3).
///
/// Every pipeline in a mature camp already carries a readme — authors write
/// the rationale at the top of the TOML, where an editor shows it. Before this
/// only the one-line `label` reached the wire and that block was dropped on the
/// floor, so the catalog UI could show a row title and nothing else.
///
/// What counts as the block:
/// - Only the contiguous comment run at the *top* of the file. The first
///   non-blank line that isn't a comment ends it, so a comment above a step is
///   never mistaken for a readme.
/// - `@yah:` / `@arch:` annotation lines end it too. Board annotations live in
///   these headers (see `.yah/qed/check.toml`) and are metadata, not prose —
///   they'd otherwise show up mid-readme in the UI. A prose line that merely
///   *mentions* one mid-sentence is unaffected; the match is line-initial.
/// - A `#:schema …` taplo directive is skipped, not treated as a terminator:
///   `.yah/qed/dashboard-e2e.toml` opens with one and puts its readme under it.
/// - One leading space after `#` is stripped, and no more: indented sub-lists
///   and box-drawing rules (`# ── Why this exists ───`) survive verbatim,
///   because these blocks are already written as markdown-ish prose.
/// - A bare `#` becomes an empty line, which is how the paragraph breaks in
///   these headers are spelled.
///
/// Returns `None` for a file with no leading block (or one holding only
/// annotations), so a consumer can omit the readme section rather than render
/// an empty one.
pub fn leading_comment_block(content: &str) -> Option<String> {
    let mut lines: Vec<&str> = Vec::new();
    for raw in content.lines() {
        let trimmed = raw.trim_start();
        let Some(rest) = trimmed.strip_prefix('#') else {
            // Blank lines before the block, or between two comment paragraphs
            // of it, don't end it — only real content does.
            if trimmed.is_empty() {
                if !lines.is_empty() {
                    lines.push("");
                }
                continue;
            }
            break;
        };
        // `#:schema …` is a taplo editor directive, not prose. Skipped rather
        // than treated as a terminator: `.yah/qed/dashboard-e2e.toml` opens
        // with one and puts its readme underneath.
        if rest.starts_with(':') {
            continue;
        }
        let body = rest.strip_prefix(' ').unwrap_or(rest);
        if body.starts_with("@yah:") || body.starts_with("@arch:") {
            break;
        }
        lines.push(body.trim_end());
    }
    while lines.last().is_some_and(|l| l.trim().is_empty()) {
        lines.pop();
    }
    if lines.is_empty() {
        return None;
    }
    Some(lines.join("\n"))
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TOML parse error: {0}")]
    TomlParse(#[from] toml::de::Error),
    #[error("Pipeline not found: {0}")]
    NotFound(String),
    #[error("Invalid step: {0}")]
    InvalidStep(#[from] StepValidationError),
    #[error("Registry config: {0}")]
    Registry(#[from] RegistryConfigError),
    #[error("Sub-pipeline graph: {0}")]
    SubPipelineGraph(#[from] crate::types::SubPipelineError),
    #[error("Invalid bind: {0}")]
    InvalidBind(String),
    #[error("Invalid param: {0}")]
    InvalidParam(String),
}

/// On-disk shape of a `.yah/qed/*.toml` pipeline file. This is the JSON-Schema
/// source of truth (R533-T10): `cargo run -p xtask -- emit-schemas` derives
/// `qed-pipeline.toml.schema.json` from it via `schemars`, and a drift test
/// asserts the committed schema matches. Kept `pub` solely so xtask can name it
/// in `schema_for!`.
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct PipelineToml {
    pub pipeline: PipelineConfig,
    /// W209: top-level `[[bind]]` tables — placed at file root (not inside
    /// `[pipeline]`) per the design doc's examples. The loader hoists them
    /// onto `Pipeline.binds`.
    #[serde(default, rename = "bind")]
    #[cfg_attr(feature = "json-schema", schemars(schema_with = "crate::types::permissive_schema"))]
    pub binds: Vec<manifest_bind::BindSpec>,
    /// W209/R510-F6: top-level `[[on_change]]` hash-change hooks, hoisted onto
    /// `Pipeline.on_change` (same root-level placement as `[[bind]]`).
    #[serde(default)]
    #[cfg_attr(feature = "json-schema", schemars(schema_with = "crate::types::permissive_schema"))]
    pub on_change: Vec<manifest_bind::OnChangeHook>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct PipelineConfig {
    name: String,
    label: String,
    /// Explicit long-form readme. Almost always omitted — see
    /// [`crate::types::Pipeline::description`]; the loader falls back to the
    /// file's leading `#` comment block, which is where camps already write
    /// this. Present so an ejected pipeline round-trips.
    #[serde(default)]
    description: Option<String>,
    /// Catalog classification tags — see [`crate::types::Pipeline::tags`].
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    steps: Vec<QedStep>,
    #[serde(default)]
    params: Option<HashMap<String, ParamDef>>,
    #[serde(default)]
    on_success: Vec<crate::types::Outcome>,
    #[serde(default)]
    on_fail: Vec<crate::types::Outcome>,
    #[serde(default)]
    triggers: Vec<crate::types::Trigger>,
    #[serde(default)]
    concurrency_key: Option<String>,
    #[serde(default)]
    placement: Placement,
    #[serde(default)]
    workspace: crate::types::WorkspaceMode,
    #[serde(default)]
    wraps: Option<String>,
    #[serde(default)]
    #[cfg_attr(feature = "json-schema", schemars(schema_with = "crate::types::permissive_schema"))]
    matrix: Option<crate::matrix::MatrixSpec>,
    #[serde(default)]
    #[cfg_attr(feature = "json-schema", schemars(schema_with = "crate::types::permissive_schema"))]
    toolchain: Option<crate::toolchain::ToolchainSpec>,
    /// W207 Gap #6 (R513-F4): `[[finally]]` always-run teardown steps. Authored
    /// at the `[pipeline]` level (a sibling of `[[steps]]`). Hoisted onto
    /// [`Pipeline::finally`] and validated with [`QedStep::validate_finally`].
    #[serde(default)]
    finally: Vec<QedStep>,
}

#[derive(Clone)]
pub struct PipelineLoader {
    pub(crate) qed_dir: std::path::PathBuf,
    /// Per-camp registry allowlist used by parse-time `push = true`
    /// validation (R381-T6). Auto-loaded from `<qed_dir>/registries.toml`
    /// on construction; tests can swap it with [`Self::with_registries`].
    registries: RegistryConfig,
    /// Per-camp peer registry used by [`SubPipelineRef::Peer`] resolution
    /// (R494-F2). Auto-loaded from `<qed_dir>/peers.toml`; missing file is
    /// fine — `Peer` refs will fail at the resolver with the same
    /// "unresolvable target" surface unknown peers get.
    pub(crate) peers: PeerConfig,
}

impl PipelineLoader {
    /// Construct a loader rooted at `qed_dir`. Reads
    /// `<qed_dir>/registries.toml` + `<qed_dir>/peers.toml` opportunistically
    /// — missing files are fine. A malformed file surfaces on the first
    /// `load*` call rather than at construction so callers don't have to
    /// handle the error twice.
    pub fn new(qed_dir: impl AsRef<Path>) -> Self {
        let qed_dir = qed_dir.as_ref().to_path_buf();
        let registries = RegistryConfig::load(&qed_dir).unwrap_or_default();
        let peers = PeerConfig::load(&qed_dir).unwrap_or_default();
        Self {
            qed_dir,
            registries,
            peers,
        }
    }

    /// Replace the auto-loaded registry config. Useful in tests when the
    /// fixture qed_dir doesn't carry a `registries.toml`.
    pub fn with_registries(mut self, registries: RegistryConfig) -> Self {
        self.registries = registries;
        self
    }

    /// Replace the auto-loaded peer config (R494-F2). Tests construct a
    /// fixture loader that already knows about its sibling peer camps
    /// without needing a `peers.toml` on disk.
    pub fn with_peers(mut self, peers: PeerConfig) -> Self {
        self.peers = peers;
        self
    }

    /// Load a pipeline by name. Resolution order:
    ///   1. `<qed_dir>/P{n}-<name>.toml` (or legacy `<name>.toml`)
    ///   2. `<workspace>/.github/workflows/<name>.yml` (or `.yaml`),
    ///      synthesised into a one-step `StepKind::GhaWorkflow` pipeline.
    pub fn load(&self, name: &str) -> Result<Pipeline, ConfigError> {
        if let Some(path) = find_pipeline_file(&self.qed_dir, name) {
            return self.load_from_file(&path);
        }
        if let Some(entry) = self.find_gha_workflow(name) {
            return Ok(synthesise_gha_pipeline(&entry));
        }
        Err(ConfigError::NotFound(name.to_string()))
    }

    /// Returns `true` when a camp-level TOML file exists for `name`
    /// (prefixed or legacy form).
    pub fn has_camp_file(&self, name: &str) -> bool {
        find_pipeline_file(&self.qed_dir, name).is_some()
    }

    /// Camp root, derived from `<qed_dir>/../..` (the conventional
    /// `<camp>/.yah/qed` layout). Falls back to `qed_dir` itself when the
    /// loader is rooted somewhere unusual (test fixtures, in-memory dirs).
    pub fn workspace_root(&self) -> PathBuf {
        self.qed_dir
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| self.qed_dir.clone())
    }

    /// Walk `<workspace>/.github/workflows/*.yml` and return parsed
    /// workflows. Files that fail to parse are skipped (logged at `warn`)
    /// so a single malformed workflow doesn't blank the whole catalog.
    pub fn list_gha_workflows(&self) -> Vec<GhaWorkflowEntry> {
        let workflows_dir = self.workspace_root().join(".github").join("workflows");
        if !workflows_dir.exists() {
            return Vec::new();
        }
        let mut out = Vec::new();
        let entries = match fs::read_dir(&workflows_dir) {
            Ok(it) => it,
            Err(_) => return Vec::new(),
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str());
            if ext != Some("yml") && ext != Some("yaml") {
                continue;
            }
            let name = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s.to_string(),
                None => continue,
            };
            let content = match fs::read_to_string(&path) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let workflow = match yah_qed_gha::parse_workflow(&content) {
                Ok(w) => w,
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "gha workflow parse failed; skipping",
                    );
                    continue;
                }
            };
            let rel_path = path
                .strip_prefix(self.workspace_root())
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|_| path.clone());
            out.push(GhaWorkflowEntry {
                name,
                rel_path,
                workflow,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    fn find_gha_workflow(&self, name: &str) -> Option<GhaWorkflowEntry> {
        self.list_gha_workflows()
            .into_iter()
            .find(|w| w.name == name)
    }

    /// List all pipeline names from `<qed_dir>/*.toml`, sorted by name.
    ///
    /// Name order, not creation order: the `P{n}-` prefix that used to impose
    /// the latter was removed in R707 (see [`find_pipeline_file`]). Callers
    /// that want recency have the run history, which is a truer answer than a
    /// number nobody was assigning.
    pub fn list_all(&self) -> Result<Vec<String>, ConfigError> {
        let mut names: Vec<String> = Vec::new();

        if self.qed_dir.exists() {
            for entry in fs::read_dir(&self.qed_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().map_or(false, |e| e == "toml") {
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        if !names.iter().any(|n| n == stem) {
                            names.push(stem.to_string());
                        }
                    }
                }
            }
            names.sort();
        }

        Ok(names)
    }

    /// Load a pipeline by name AND walk its sub-pipeline graph for cycles
    /// and excessive nesting (R488-F1/F2). Use this before handing a
    /// pipeline to [`PipelineRunner`](crate::runner::PipelineRunner) when
    /// you want parse-time confirmation that the SubPipeline graph is
    /// well-formed; plain [`Self::load`] skips the walk so loading
    /// individual children doesn't re-validate the whole graph repeatedly.
    pub fn load_and_validate_graph(&self, name: &str) -> Result<Pipeline, ConfigError> {
        let pipeline = self.load(name)?;
        let resolver = LoaderSubPipelineResolver::new(self.clone());
        crate::types::validate_sub_pipeline_graph(&pipeline, &resolver)?;
        Ok(pipeline)
    }

    /// List only custom pipeline files from `.yah/qed/` (excludes built-ins),
    /// returning the pipeline name — which is the filename stem.
    pub fn list_files(&self) -> Result<Vec<String>, ConfigError> {
        let mut pipelines = Vec::new();
        if self.qed_dir.exists() {
            for entry in fs::read_dir(&self.qed_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "toml") {
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        pipelines.push(stem.to_string());
                    }
                }
            }
        }
        Ok(pipelines)
    }

    /// Parse + validate one pipeline TOML document. The single place a
    /// [`PipelineToml`] becomes a [`Pipeline`] — `load_from_file` and the
    /// test-only `load_from_str` both route through here so a new field can't
    /// be wired into one path and forgotten in the other (R703-F3: which is
    /// exactly what two copies of this hoist invited).
    fn pipeline_from_str(&self, content: &str) -> Result<Pipeline, ConfigError> {
        let parsed: PipelineToml = toml::from_str(content)?;
        let pipeline = Pipeline {
            name: parsed.pipeline.name,
            label: parsed.pipeline.label,
            // Explicit key wins; otherwise the file's own header block is the
            // readme (R703-F3).
            description: parsed
                .pipeline
                .description
                .or_else(|| leading_comment_block(content)),
            tags: parsed.pipeline.tags,
            steps: parsed.pipeline.steps,
            params: parsed.pipeline.params.unwrap_or_default(),
            on_success: parsed.pipeline.on_success,
            on_fail: parsed.pipeline.on_fail,
            triggers: parsed.pipeline.triggers,
            concurrency_key: parsed.pipeline.concurrency_key,
            placement: parsed.pipeline.placement,
            workspace: parsed.pipeline.workspace,
            wraps: parsed.pipeline.wraps,
            matrix: parsed.pipeline.matrix,
            toolchain: parsed.pipeline.toolchain,
            binds: parsed.binds,
            on_change: parsed.on_change,
            finally: parsed.pipeline.finally,
        };
        self.validate_steps(&pipeline)?;
        self.validate_binds(&pipeline)?;
        self.validate_params(&pipeline)?;
        Ok(pipeline)
    }

    #[cfg(test)]
    fn load_from_str(&self, content: &str) -> Result<Pipeline, ConfigError> {
        self.pipeline_from_str(content)
    }

    /// Public helper: parse a pipeline directly from a file path, bypassing
    /// the `<qed_dir>/P{n}-<name>.toml` lookup. Used by `qed plan
    /// <path>.toml` to preview drafts that haven't been moved into the camp
    /// pipeline directory yet.
    pub fn parse_from_path(&self, path: &Path) -> Result<Pipeline, ConfigError> {
        self.load_from_file(path)
    }

    pub(crate) fn load_from_file(&self, path: &Path) -> Result<Pipeline, ConfigError> {
        let content = fs::read_to_string(path)?;
        self.pipeline_from_str(&content)
    }

    /// Run [`QedStep::validate`] across every step, then enforce the
    /// per-camp registry allowlist on any `build-image` step with
    /// `push = true` (R381-T6). Surfaces the *first* failure — pipeline TOML
    /// authors get one error at a time, which is friendlier than a wall of
    /// validation failures.
    fn validate_steps(&self, pipeline: &Pipeline) -> Result<(), ConfigError> {
        for step in &pipeline.steps {
            step.validate()?;
            if matches!(step.kind, StepKind::BuildImage) && step.push {
                let tag_for_host = step.tag.as_deref().or(step.image.as_deref()).unwrap_or("");
                let host = extract_registry_host(tag_for_host);
                if !self.registries.is_writable(host) {
                    return Err(ConfigError::InvalidStep(
                        StepValidationError::PushRequiresWritableRegistry {
                            step: step.name.clone(),
                            host: host.to_string(),
                        },
                    ));
                }
            }
        }
        // R513-F4: `[[finally]]` teardown steps validate with the stricter
        // finally rule (subprocess-only, never background) on top of the normal
        // kind-specific checks.
        for step in &pipeline.finally {
            step.validate_finally()
                .map_err(ConfigError::InvalidStep)?;
        }
        Ok(())
    }

    /// W209 parse-time bind validation: every `[[bind]].from` that names a
    /// step output must reference (a) a step that exists in this pipeline,
    /// and (b) an output key declared on that step. URI-shaped `from`
    /// (`registry://...`) is the escape hatch and skips this check. Surfaces
    /// the first offender — authors get one error at a time, same as
    /// `validate_steps`.
    fn validate_binds(&self, pipeline: &Pipeline) -> Result<(), ConfigError> {
        for bind in &pipeline.binds {
            match &bind.from {
                manifest_bind::OutputRef::Uri(_) => continue,
                manifest_bind::OutputRef::StepOutput { step, key } => {
                    let Some(producer) = pipeline.steps.iter().find(|s| &s.name == step) else {
                        return Err(ConfigError::InvalidBind(format!(
                            "[[bind]] file = {:?}: from references unknown step {step:?}",
                            bind.file
                        )));
                    };
                    if !producer.outputs.iter().any(|o| &o.name == key) {
                        return Err(ConfigError::InvalidBind(format!(
                            "[[bind]] file = {:?}: step {step:?} does not declare output {key:?} \
                             (declare it under [[pipeline.steps]].outputs)",
                            bind.file
                        )));
                    }
                }
            }
        }
        // W209/R510-F6: every `[[on_change]].bind` selector must reference a
        // declared `[[bind]].path` — a hook keyed off a slot nothing binds is
        // dead config (a typo'd selector). Same first-offender surface as the
        // bind checks above.
        for hook in &pipeline.on_change {
            if !pipeline.binds.iter().any(|b| b.path == hook.bind) {
                return Err(ConfigError::InvalidBind(format!(
                    "[[on_change]] bind = {:?}: no [[bind]] declares path {:?} \
                     (the selector must match a bound slot's `path`)",
                    hook.bind, hook.bind
                )));
            }
        }
        Ok(())
    }

    /// A param declaring `options` declares a closed set, so its `default` has
    /// to be a member of it. Catching this at load time matters more than it
    /// looks: [`Pipeline::resolve_params`](crate::types::Pipeline::resolve_params)
    /// would otherwise only reject it on the runs where the operator *omitted*
    /// the param, so a mistyped default hides until someone takes the default
    /// path. Same first-offender surface as the step/bind checks.
    fn validate_params(&self, pipeline: &Pipeline) -> Result<(), ConfigError> {
        let mut names: Vec<&String> = pipeline.params.keys().collect();
        names.sort();
        for name in names {
            let def = &pipeline.params[name];
            if def.options.is_empty() {
                continue;
            }
            if let Some(default) = &def.default {
                if !def.options.iter().any(|o| o == default) {
                    return Err(ConfigError::InvalidParam(format!(
                        "[pipeline.params.{name}]: default = {default:?} is not one of its \
                         options ({}) — a default has to be a value the param accepts",
                        def.options.join(", ")
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Bridge a [`PipelineLoader`] into the [`SubPipelineResolver`] trait so
/// [`PipelineRunner`](crate::runner::PipelineRunner) can recurse into
/// SubPipeline children without `runner.rs` taking a direct dependency on
/// the loader (and so callers don't need to write their own resolver).
///
/// Resolution:
/// - `Builtin(name)` → `loader.load(name)` (which itself prefers camp-level
///   `.yah/qed/<name>.toml` over the bundled builtin — same precedence as
///   any other pipeline lookup).
/// - `Path(p)` → `loader.load_from_file(&p)` against `p` as-is when
///   absolute, otherwise resolved against the loader's `qed_dir` parent
///   (the camp root).
/// - `GhaWorkflow { .. }` → returns `None` until W200-F9 (StepKind::GhaWorkflow
///   + native runtime dispatch) lands; the runner surfaces this as a clear
///   "unresolvable" `StepFailed` at execution time.
///
/// Errors from the underlying `load` are swallowed into `None` so the
/// walker / runner can give consistent "unresolvable" error messages
/// (rather than threading a richer error through `SubPipelineResolver`).
/// Operators see the missing-pipeline path/name in the runner's
/// `StepFailed.msg`; if `load` returned an error mid-recursion, the
/// equivalent surface is "target not found".
/// One parsed `.github/workflows/<name>.yml` surfaced by
/// [`PipelineLoader::list_gha_workflows`]. The daemon's `qed.pipelines`
/// handler uses this to flatten jobs/steps into a `QedPipelineWire`, and
/// [`PipelineLoader::load`] uses it to synthesise a one-step
/// `StepKind::GhaWorkflow` pipeline when an operator runs the workflow by
/// name (so `yah qed run release` works for `.github/workflows/release.yml`).
pub struct GhaWorkflowEntry {
    /// Filename stem (`release.yml` → `release`). The catalog key.
    pub name: String,
    /// Path relative to the workspace root (`.github/workflows/release.yml`).
    pub rel_path: PathBuf,
    /// Fully parsed workflow as returned by `yah_qed_gha::parse_workflow`. The
    /// daemon walks `workflow.jobs[].steps[]` to build the wire's `steps[]`,
    /// keeping a single source of truth between visualisation and execution.
    pub workflow: yah_qed_gha::Workflow,
}

/// Synthesise a one-step `StepKind::GhaWorkflow` pipeline that wraps the
/// given workflow entry. Mirrors [`SubPipelineRef::GhaWorkflow`] resolution
/// so `yah qed run <workflow>` and `target = { gha-workflow = ... }` end up
/// at the same runner arm.
fn synthesise_gha_pipeline(entry: &GhaWorkflowEntry) -> Pipeline {
    // Spelled as an overlay on `QedStep::default()` (which round-trips serde's
    // own defaults, so it cannot drift from what a minimal TOML deserializes to)
    // rather than as a 30-field literal. Only the two fields that make this a
    // gha-workflow step differ from the default.
    let step = QedStep {
        name: "gha-workflow".to_string(),
        kind: StepKind::GhaWorkflow,
        gha_workflow: Some(GhaWorkflowConfig {
            path: entry.rel_path.clone(),
            event: None,
            inputs: HashMap::new(),
            // Auto-ingest represents a workflow AS GITHUB WOULD RUN IT, so it
            // narrows nothing: the whole matrix.
            matrix: HashMap::new(),
        }),
        ..Default::default()
    };
    Pipeline {
        description: None,
        name: entry.name.clone(),
        label: entry
            .workflow
            .name
            .clone()
            .unwrap_or_else(|| entry.name.clone()),
        // Auto-ingested workflows already carry `scope: "gha"` on the wire;
        // tags are the *author's* classification and a synthesised pipeline
        // has no author.
        tags: Vec::new(),
        steps: vec![step],
        params: HashMap::new(),
        on_success: Vec::new(),
        on_fail: Vec::new(),
        triggers: Vec::new(),
        concurrency_key: None,
        placement: Placement::default(),
        workspace: crate::types::WorkspaceMode::default(),
        wraps: None,
        matrix: None,
        toolchain: None,
        binds: Vec::new(),
        on_change: Vec::new(),
        finally: Vec::new(),
    }
}

pub struct LoaderSubPipelineResolver {
    loader: PipelineLoader,
}

impl LoaderSubPipelineResolver {
    pub fn new(loader: PipelineLoader) -> Self {
        Self { loader }
    }

    /// Resolve a local peer camp's root from `peers.toml`, relative to this
    /// camp. Returns `None` for unknown camps and for remote peers (`rig`
    /// set) — those don't resolve to a local path. Shared by [`resolve`]
    /// (to load the peer's pipeline) and [`resolved_camp_root`] (to run that
    /// pipeline's steps in the peer's workspace).
    fn local_peer_camp_root(&self, camp: &str) -> Option<std::path::PathBuf> {
        let entry = self.loader.peers.get(camp)?;
        if entry.rig.is_some() {
            return None;
        }
        if entry.path.is_absolute() {
            return Some(entry.path.clone());
        }
        // self.loader.qed_dir is `<this camp root>/.yah/qed`; pop twice to
        // reach `<this camp root>`, then join the peer's relative path.
        self.loader
            .qed_dir
            .parent()
            .and_then(|p| p.parent())
            .map(|root| root.join(&entry.path))
    }
}

impl SubPipelineResolver for LoaderSubPipelineResolver {
    fn resolve(&self, target: &SubPipelineRef) -> Option<Pipeline> {
        match target {
            SubPipelineRef::Builtin(name) => self.loader.load(name).ok(),
            SubPipelineRef::Path(p) => {
                let resolved: std::path::PathBuf = if p.is_absolute() {
                    p.clone()
                } else {
                    // qed_dir is conventionally `<camp>/.yah/qed`; its parent
                    // is `<camp>/.yah` — pop once more to reach the camp root
                    // so a SubPipeline path of `.yah/qed/foo.toml` resolves
                    // correctly against the camp.
                    self.loader
                        .qed_dir
                        .parent()
                        .and_then(|p| p.parent())
                        .map(|root| root.join(p))
                        .unwrap_or_else(|| p.clone())
                };
                self.loader.load_from_file(&resolved).ok()
            }
            // GhaWorkflow children synthesize a one-step Pipeline whose
            // single step is `StepKind::GhaWorkflow` (W200-F9). The runner's
            // own arm then dispatches to yah_qed_gha::execute_workflow and lifts
            // ProducedArtifacts the same way Subprocess `produces` does.
            // Going through SubPipeline preserves the propagate.produces /
            // suppress_publish_outcomes plumbing so a child workflow's R2
            // staging fires from the parent's terminal publish, not the
            // child's.
            SubPipelineRef::GhaWorkflow {
                path,
                event,
                inputs,
            } => {
                // Overlay on `QedStep::default()` — see `synthesise_gha_pipeline`.
                let step = crate::types::QedStep {
                    name: "gha-workflow".into(),
                    kind: crate::types::StepKind::GhaWorkflow,
                    gha_workflow: Some(crate::types::GhaWorkflowConfig {
                        path: path.clone(),
                        event: event.clone(),
                        inputs: inputs.clone(),
                        // `SubPipelineRef::GhaWorkflow` carries no row selector,
                        // so there is nothing to forward. Pin a row with a direct
                        // `kind = "gha-workflow"` step instead; widening the
                        // SubPipelineRef variant is a change worth making when a
                        // composite pipeline actually needs it, not before.
                        matrix: HashMap::new(),
                    }),
                    ..Default::default()
                };
                Some(crate::types::Pipeline {
                    description: None,
                    name: format!("gha-workflow:{}", path.display()),
                    label: String::new(),
                    tags: Vec::new(),
                    concurrency_key: None,
                    steps: vec![step],
                    triggers: Vec::new(),
                    on_success: Vec::new(),
                    on_fail: Vec::new(),
                    placement: crate::types::Placement::default(),
                    workspace: crate::types::WorkspaceMode::default(),
                    wraps: None,
                    matrix: None,
                    params: std::collections::HashMap::new(),
                    toolchain: None,
                    binds: Vec::new(),
                    on_change: Vec::new(),
                    finally: Vec::new(),
                })
            }
            // Peer resolution (R494-F2). Look the peer up in this camp's
            // `peers.toml`; resolve its camp root relative to ours (or use
            // the absolute path for remote peers, which T5 will refine into
            // a typed unsupported-error path — for now they swallow to
            // None like any unresolvable target). Stamp `concurrency_key`
            // to `peer:<camp>` when the loaded pipeline didn't set one
            // itself, so two yah runs invoking different pipelines in the
            // same peer camp (e.g. cheers/build + cheers/test) still
            // serialize on cheers' shared `target/`.
            SubPipelineRef::Peer { camp, pipeline } => {
                // Remote peers (`rig` set) go through kamaji, which isn't
                // wired yet. `local_peer_camp_root` returns None for them and
                // for unknown camps; the runner consults `unresolved_reason`
                // below to surface a typed message in StepFailed.msg rather
                // than the generic "target unresolvable" tail.
                let peer_camp_root = self.local_peer_camp_root(camp)?;
                let peer_qed_dir = peer_camp_root.join(".yah").join("qed");
                let peer_loader = PipelineLoader::new(&peer_qed_dir);
                let mut child = peer_loader.load(pipeline).ok()?;
                if child.concurrency_key.is_none() {
                    child.concurrency_key = Some(format!("peer:{camp}"));
                }
                Some(child)
            }
        }
    }

    fn unresolved_reason(&self, target: &SubPipelineRef) -> Option<String> {
        match target {
            SubPipelineRef::Peer { camp, pipeline } => match self.loader.peers.get(camp) {
                None => Some(format!(
                    "peer camp `{camp}` is not declared in `{}/peers.toml` \
                     (add `[peer.{camp}]` with `path = \"...\"`)",
                    self.loader.qed_dir.display()
                )),
                Some(entry) => entry
                    .rig
                    .as_ref()
                    .map(|rig| {
                        format!(
                            "remote peer `{camp}` lives on rig `{rig}` — \
                         cross-rig peer execution is not yet supported \
                         (R494-T5: kamaji hop pending). Drop the `rig = ...` \
                         field on `[peer.{camp}]` in peers.toml to run the \
                         peer camp locally, or wait for R494-F10.",
                        )
                    })
                    .or_else(|| {
                        Some(format!(
                            "peer camp `{camp}` is declared but pipeline `{pipeline}` \
                     was not found in `{}/.yah/qed/` \
                     (check the peer's pipeline name)",
                            entry.path.display()
                        ))
                    }),
            },
            _ => None,
        }
    }

    fn resolved_camp_root(&self, target: &SubPipelineRef) -> Option<std::path::PathBuf> {
        // Only Peer children switch camps; Builtin/Path/GhaWorkflow run in
        // the parent's camp (return None → runner inherits parent camp_root).
        match target {
            SubPipelineRef::Peer { camp, .. } => self.local_peer_camp_root(camp),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// R707: the filename stem IS the pipeline name. Guards against anyone
    /// reintroducing a decorated filename form — a `P013-release.toml` no
    /// longer resolves under the name `release`, it resolves (only) under the
    /// name `P013-release`, which is what makes the coupling self-enforcing
    /// rather than something a convention has to police.
    #[test]
    fn pipeline_resolves_by_filename_stem_only() {
        let dir = tempfile::tempdir().unwrap();
        let body = |name: &str| {
            format!("[pipeline]\nname = \"{name}\"\nlabel = \"l\"\nworkspace = \"live\"\n")
        };
        std::fs::write(dir.path().join("release.toml"), body("release")).unwrap();
        std::fs::write(dir.path().join("P013-legacy.toml"), body("legacy")).unwrap();
        let loader = PipelineLoader::new(dir.path());

        assert_eq!(loader.load("release").unwrap().name, "release");
        // The decorated file is reachable only by its literal stem...
        assert!(loader.load("P013-legacy").is_ok());
        // ...never by the name inside it.
        assert!(matches!(
            loader.load("legacy"),
            Err(ConfigError::NotFound(_))
        ));
        assert_eq!(
            loader.list_all().unwrap(),
            vec!["P013-legacy".to_string(), "release".to_string()],
            "list_all is name-sorted, no numeric axis",
        );
    }
    /// R703-F3: the header block is the readme. Paragraph breaks (`#` alone)
    /// and the box-drawing section rules these files use must survive intact —
    /// the block is rendered as markdown-ish prose, not reflowed.
    #[test]
    fn leading_comment_block_becomes_the_description() {
        let toml = "\
# release — cut a tag and ship it.
#
# ── Why this exists ─────────────
# Because the label is one line and this is not.
#   - indented detail keeps its indent

[pipeline]
name = \"release\"
label = \"Release\"
workspace = \"live\"
";
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let p = loader.load_from_str(toml).unwrap();
        assert_eq!(
            p.description.as_deref(),
            Some(
                "release — cut a tag and ship it.\n\
                 \n\
                 ── Why this exists ─────────────\n\
                 Because the label is one line and this is not.\n\
                 \x20 - indented detail keeps its indent"
            ),
        );
    }

    /// The board annotations that live in these headers are metadata, not
    /// prose — they end the readme rather than appearing inside it. See
    /// `.yah/qed/check.toml`, where twenty lines of `@yah:` follow the prose.
    #[test]
    fn description_stops_at_board_annotations() {
        let toml = "\
# check — the correctness bar.
#
# @yah:ticket(R475-T7, \"something\")
# @yah:status(review)

[pipeline]
name = \"check\"
label = \"Check\"
workspace = \"live\"
";
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let p = loader.load_from_str(toml).unwrap();
        assert_eq!(p.description.as_deref(), Some("check — the correctness bar."));
    }

    /// A `#:schema` line is a taplo editor directive, not prose. Several of
    /// this camp's pipelines open with one and put the readme underneath, so it
    /// has to be skipped rather than end the block — and it must not become the
    /// readme's first line, which is what the collapsed section summarises.
    #[test]
    fn schema_directive_is_not_part_of_the_readme() {
        let toml = "\
#:schema ../schema/qed-pipeline.toml.schema.json

# dashboard-e2e — the real readme.
# Second line.

[pipeline]
name = \"dashboard-e2e\"
label = \"l\"
workspace = \"live\"
";
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        assert_eq!(
            loader.load_from_str(toml).unwrap().description.as_deref(),
            Some("dashboard-e2e — the real readme.\nSecond line."),
        );
    }

    /// Prose that *mentions* `@yah:` mid-sentence is prose. Four of this camp's
    /// headers do exactly that ("the canonical `@yah:ticket` block lives in
    /// …"), and truncating there would drop the rest of the readme.
    #[test]
    fn annotation_mentioned_mid_sentence_does_not_end_the_readme() {
        let toml = "\
# release-build — cross-compile one target.
# The load-bearing check is the one the ticket's @yah:verify asks for.
# Still readme.

[pipeline]
name = \"release-build\"
label = \"l\"
workspace = \"live\"
";
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let d = loader.load_from_str(toml).unwrap().description.unwrap();
        assert!(d.ends_with("Still readme."), "got: {d:?}");
    }

    /// No header ⇒ `None`, not `Some("")`. The UI omits the readme section
    /// entirely on `None`; an empty string would render an empty one.
    #[test]
    fn no_header_block_yields_no_description() {
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let p = loader
            .load_from_str("[pipeline]\nname = \"n\"\nlabel = \"l\"\nworkspace = \"live\"\n")
            .unwrap();
        assert_eq!(p.description, None);
        // Annotations-only header is also nothing to show.
        let only_annotations = loader
            .load_from_str(
                "# @yah:ticket(R1, \"x\")\n\n[pipeline]\nname = \"n\"\nlabel = \"l\"\nworkspace = \"live\"\n",
            )
            .unwrap();
        assert_eq!(only_annotations.description, None);
    }

    /// A comment attached to a step is not a readme — only the contiguous run
    /// at the very top of the file counts.
    #[test]
    fn comments_below_the_header_are_not_the_description() {
        let toml = "\
[pipeline]
name = \"n\"
label = \"l\"
workspace = \"live\"

# this explains the step, not the pipeline
[[pipeline.steps]]
name = \"s\"
argv = [\"true\"]
";
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        assert_eq!(loader.load_from_str(toml).unwrap().description, None);
    }

    /// An explicit `description =` key beats the header block, so a pipeline
    /// that came back through `qed eject` keeps the prose it was ejected with.
    #[test]
    fn explicit_description_key_wins_over_header() {
        let toml = "\
# header prose

[pipeline]
name = \"n\"
label = \"l\"
description = \"explicit\"
workspace = \"live\"
";
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        assert_eq!(
            loader.load_from_str(toml).unwrap().description.as_deref(),
            Some("explicit"),
        );
    }

    use crate::registries::RegistryEntry;
    use crate::types::Outcome;

    /// W209: round-trip a pipeline TOML that declares a typed step output
    /// and a `[[bind]]` referencing it. Loader must parse, type-validate,
    /// and surface the BindSpec on the loaded Pipeline.
    #[test]
    fn loads_pipeline_with_typed_output_and_bind() {
        let toml = r#"
[pipeline]
name = "publish-assets"
label = "Publish whisper assets"

[[pipeline.steps]]
name = "apply"
kind = "subprocess"
argv = ["yah", "cloud", "apply"]

[[pipeline.steps.outputs]]
name = "discovered_asset_blake3"
type = "blake3-hex"

[[pipeline.steps.outputs]]
name = "discovered_fetch_blake3"
type = "blake3-hex"

[[bind]]
file   = "app/yah/desktop/assets/whisper/workload.toml"
path   = "asset[filename='whisper.tar.gz'].blake3"
from   = "apply.outputs.discovered_asset_blake3"
intent = "latest"

[[bind]]
file   = "app/yah/desktop/assets/whisper/workload.toml"
path   = "asset[filename='whisper.tar.gz'].derive.fetch.blake3"
from   = "apply.outputs.discovered_fetch_blake3"
"#;
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let pipeline = loader.load_from_str(toml).expect("loads cleanly");
        assert_eq!(pipeline.binds.len(), 2);
        assert_eq!(pipeline.steps[0].outputs.len(), 2);
        assert_eq!(
            pipeline.steps[0].outputs[0].kind,
            manifest_bind::ValueType::Blake3Hex,
        );
        // First bind = explicit latest, second omits intent and defaults to pin.
        assert!(matches!(
            pipeline.binds[0].intent,
            manifest_bind::Intent::Keyword(manifest_bind::IntentKeyword::Latest)
        ));
        assert!(matches!(
            pipeline.binds[1].intent,
            manifest_bind::Intent::Keyword(manifest_bind::IntentKeyword::Pin)
        ));
    }

    /// W209: a bind whose `from` references an undeclared step output is
    /// rejected at parse time.
    #[test]
    fn rejects_bind_referencing_undeclared_output() {
        let toml = r#"
[pipeline]
name = "publish-assets"
label = "Publish whisper assets"

[[pipeline.steps]]
name = "apply"
kind = "subprocess"
argv = ["yah", "cloud", "apply"]

[[bind]]
file   = "workload.toml"
path   = "image"
from   = "apply.outputs.missing_key"
intent = "latest"
"#;
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let err = loader.load_from_str(toml).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidBind(_)), "got {err:?}");
    }

    /// R653-F2: an enumerated param round-trips from TOML — `options` is what
    /// the operator surface renders a dropdown from, so it has to survive the
    /// loader, and `description` has to come with it (the QED tab had no way to
    /// show a description it already had in the file).
    #[test]
    fn parses_enumerated_params() {
        let toml = r#"
[pipeline]
name = "appliance-image"
label = "Build an appliance image"

[pipeline.params.board]
description = "Which board to build for"
default = "orangepi_zero2w"
options = ["orangepi_zero2w", "rpi_zero2w"]

[[pipeline.steps]]
name = "build"
kind = "subprocess"
argv = ["make", "image", "BOARD={{board}}"]
"#;
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let pipeline = loader.load_from_str(toml).expect("loads cleanly");
        let board = &pipeline.params["board"];
        assert_eq!(board.options, vec!["orangepi_zero2w", "rpi_zero2w"]);
        assert_eq!(board.default.as_deref(), Some("orangepi_zero2w"));
        assert_eq!(board.description.as_deref(), Some("Which board to build for"));
        assert!(!board.required, "a param with a default needn't claim required");
    }

    /// A default outside the option set is an authoring error, and it has to
    /// fail at load rather than only on the runs that take the default.
    #[test]
    fn rejects_param_default_outside_its_options() {
        let toml = r#"
[pipeline]
name = "appliance-image"
label = "Build an appliance image"

[pipeline.params.board]
default = "rpi_zero2"
options = ["orangepi_zero2w", "rpi_zero2w"]

[[pipeline.steps]]
name = "build"
kind = "subprocess"
argv = ["make", "image"]
"#;
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let err = loader.load_from_str(toml).unwrap_err();
        match &err {
            ConfigError::InvalidParam(msg) => {
                assert!(msg.contains("board") && msg.contains("rpi_zero2w"), "got: {msg}");
            }
            other => panic!("expected InvalidParam, got {other:?}"),
        }
    }

    /// R513-F4: a `[[pipeline.finally]]` subprocess teardown step parses and is
    /// hoisted onto `Pipeline::finally`.
    #[test]
    fn parses_finally_teardown_steps() {
        let toml = r#"
[pipeline]
name = "e2e"
label = "Dashboard E2E"

[[pipeline.steps]]
name = "test"
kind = "subprocess"
argv = ["playwright", "test"]

[[pipeline.finally]]
name = "upload-traces"
kind = "subprocess"
argv = ["aws", "s3", "cp", "traces/", "s3://ci/traces/", "--recursive"]
"#;
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let pipeline = loader.load_from_str(toml).expect("loads cleanly");
        assert_eq!(pipeline.finally.len(), 1);
        assert_eq!(pipeline.finally[0].name, "upload-traces");
        assert_eq!(pipeline.finally[0].kind, StepKind::Subprocess);
    }

    /// R513-F4: a non-subprocess `[[pipeline.finally]]` step is rejected at
    /// parse time (v1 teardown is subprocess-only).
    #[test]
    fn rejects_non_subprocess_finally_step() {
        let toml = r#"
[pipeline]
name = "e2e"
label = "Dashboard E2E"

[[pipeline.steps]]
name = "test"
kind = "subprocess"
argv = ["true"]

[[pipeline.finally]]
name = "gate"
kind = "wait-for"
[pipeline.finally.wait_for]
http = "http://localhost:3000/health"
"#;
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let err = loader.load_from_str(toml).unwrap_err();
        assert!(
            matches!(
                err,
                ConfigError::InvalidStep(StepValidationError::FinallyRequiresSubprocess(_))
            ),
            "got {err:?}"
        );
    }

    /// W209: a bind whose `from` names a step that doesn't exist in this
    /// pipeline is rejected at parse time.
    #[test]
    fn rejects_bind_referencing_unknown_step() {
        let toml = r#"
[pipeline]
name = "publish-assets"
label = "Publish whisper assets"

[[pipeline.steps]]
name = "apply"
kind = "subprocess"
argv = ["yah", "cloud", "apply"]

[[bind]]
file   = "workload.toml"
path   = "image"
from   = "doesnt_exist.outputs.x"
intent = "latest"
"#;
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let err = loader.load_from_str(toml).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidBind(_)), "got {err:?}");
    }

    /// W209/R510-F6: a pipeline with `[[bind]]` + `[[on_change]]` round-trips
    /// through the loader, with the hooks hoisted onto `Pipeline.on_change`
    /// and the action variants parsed.
    #[test]
    fn loads_pipeline_with_on_change_hooks() {
        let toml = r#"
[pipeline]
name = "publish-assets"
label = "Publish whisper assets"

[[pipeline.steps]]
name = "apply"
kind = "subprocess"
argv = ["yah", "cloud", "apply"]

[[pipeline.steps.outputs]]
name = "discovered_asset_blake3"
type = "blake3-hex"

[[bind]]
file   = "app/yah/desktop/assets/whisper/workload.toml"
path   = "asset[filename='whisper.tar.gz'].blake3"
from   = "apply.outputs.discovered_asset_blake3"
intent = "latest"

[[on_change]]
bind   = "asset[filename='whisper.tar.gz'].blake3"
action = { pipeline = "release.bump-manifest", params = { component = "whisper-coreml" } }

[[on_change]]
bind   = "asset[filename='whisper.tar.gz'].blake3"
action = { journal = ".yah/qed/whisper.journal" }
"#;
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let pipeline = loader.load_from_str(toml).expect("loads cleanly");
        assert_eq!(pipeline.on_change.len(), 2);
        assert!(matches!(
            pipeline.on_change[0].action,
            manifest_bind::OnChangeAction::Pipeline { .. }
        ));
        assert!(matches!(
            pipeline.on_change[1].action,
            manifest_bind::OnChangeAction::Journal { .. }
        ));
    }

    /// W209/R510-F6: an `[[on_change]]` whose `bind` selector matches no
    /// declared `[[bind]].path` is dead config and rejected at parse time.
    #[test]
    fn rejects_on_change_referencing_undeclared_bind() {
        let toml = r#"
[pipeline]
name = "publish-assets"
label = "Publish whisper assets"

[[pipeline.steps]]
name = "apply"
kind = "subprocess"
argv = ["yah", "cloud", "apply"]

[[pipeline.steps.outputs]]
name = "discovered_asset_blake3"
type = "blake3-hex"

[[bind]]
file   = "workload.toml"
path   = "blake3"
from   = "apply.outputs.discovered_asset_blake3"
intent = "latest"

[[on_change]]
bind   = "image"
action = { journal = ".yah/qed/x.journal" }
"#;
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let err = loader.load_from_str(toml).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidBind(_)), "got {err:?}");
    }

    /// W209: URI-shaped `from` (escape hatch) bypasses the
    /// step/output-existence check — the producer is external.
    #[test]
    fn uri_from_bypasses_step_existence_check() {
        let toml = r#"
[pipeline]
name = "pin-image"
label = "Pin python image"

[[pipeline.steps]]
name = "noop"
kind = "subprocess"
argv = ["true"]

[[bind]]
file   = ".yah/qed/transforms/whisper-bundle-tar.toml"
path   = "image"
from   = "registry://python:3.12-slim"
intent = { semver = "^3.12" }
"#;
        let dir = tempfile::tempdir().unwrap();
        let loader = PipelineLoader::new(dir.path());
        let pipeline = loader.load_from_str(toml).expect("URI from loads cleanly");
        assert_eq!(pipeline.binds.len(), 1);
        assert!(matches!(
            pipeline.binds[0].from,
            manifest_bind::OutputRef::Uri(_)
        ));
    }

    #[test]
    fn parses_on_success_outcomes_from_toml() {
        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name    = "release"
label   = "Release pipeline"

[[pipeline.steps]]
name = "build"
argv = ["cargo", "build", "--release", "-p", "yah"]

[[pipeline.on_success]]
kind    = "yubaba-deploy"
service = "yah"
env     = "production"

[[pipeline.on_success]]
kind     = "almanac-run"
pipeline = "update-release-index"

[[pipeline.on_fail]]
kind     = "almanac-run"
pipeline = "notify-failure"
"#;
        let pipeline = loader.load_from_str(toml).expect("should parse");
        assert_eq!(pipeline.on_success.len(), 2);
        assert_eq!(pipeline.on_fail.len(), 1);

        assert!(matches!(
            &pipeline.on_success[0],
            Outcome::YubabaDeploy { service, env }
            if service == "yah" && env == "production"
        ));
        assert!(matches!(
            &pipeline.on_success[1],
            Outcome::AlmanacRun { pipeline } if pipeline == "update-release-index"
        ));
        assert!(matches!(
            &pipeline.on_fail[0],
            Outcome::AlmanacRun { pipeline } if pipeline == "notify-failure"
        ));
    }

    #[test]
    fn parses_provider_outcome_with_config_table() {
        // R509: a vendor `provider` outcome (notarize) with a `with` config
        // table + base_url round-trips through the real loader onto
        // `Outcome::Provider`. This is the schema noisetable's release.apple.toml
        // drafts against for the mac slice (notarize → sparkle).
        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "release.apple"
label = "Apple release"

[[pipeline.steps]]
name = "build"
argv = ["cargo", "build", "--release"]

[[pipeline.on_success]]
kind     = "provider"
provider = "notarize"
base_url = "https://releases.yah.dev"
with     = { artifacts = ["desktop"] }
"#;
        let pipeline = loader.load_from_str(toml).expect("should parse");
        assert_eq!(pipeline.on_success.len(), 1);
        match &pipeline.on_success[0] {
            Outcome::Provider {
                provider,
                with,
                base_url,
            } => {
                assert_eq!(provider, "notarize");
                assert_eq!(base_url.as_deref(), Some("https://releases.yah.dev"));
                assert_eq!(with["artifacts"][0], "desktop");
            }
            other => panic!("expected Outcome::Provider, got {other:?}"),
        }
    }

    #[test]
    fn pipeline_without_outcomes_defaults_to_empty() {
        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "check"
label = "Quick check"

[[pipeline.steps]]
name = "cargo-check"
argv = ["cargo", "check"]
"#;
        let pipeline = loader.load_from_str(toml).expect("should parse");
        assert!(pipeline.on_success.is_empty());
        assert!(pipeline.on_fail.is_empty());
    }

    #[test]
    fn parses_toolchain_pins_pipeline_and_step_scope() {
        // R507/W208: `[pipeline.toolchain]` + per-step `toolchain.<tool>`
        // override survive the real loader onto Pipeline/QedStep.
        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "release.apple"
label = "Apple release"

[pipeline.toolchain]
rust  = "1.84.0"
xcode = "15.4"
ndk   = "r27"

[[pipeline.steps]]
name = "build"
argv = ["cargo", "build", "--release"]

[[pipeline.steps]]
name = "build-android"
argv = ["cargo", "ndk", "build"]
toolchain.ndk = "r26d"
"#;
        let pipeline = loader.load_from_str(toml).expect("should parse");
        let tc = pipeline
            .toolchain
            .as_ref()
            .expect("pipeline toolchain present");
        assert_eq!(tc.pins.get("xcode").map(String::as_str), Some("15.4"));
        assert_eq!(tc.pins.get("rust").map(String::as_str), Some("1.84.0"));
        // The build step inherits the pipeline pins (no override block).
        assert!(pipeline.steps[0].toolchain.is_none());
        // The android step carries its own ndk override.
        let step_tc = pipeline.steps[1]
            .toolchain
            .as_ref()
            .expect("step override present");
        assert_eq!(step_tc.pins.get("ndk").map(String::as_str), Some("r26d"));
        // Effective pins for the android step: pipeline rust/xcode + overridden ndk.
        let eff = crate::toolchain::effective_pins(
            pipeline.toolchain.as_ref(),
            pipeline.steps[1].toolchain.as_ref(),
        );
        assert_eq!(eff.get("ndk").map(String::as_str), Some("r26d"));
        assert_eq!(eff.get("rust").map(String::as_str), Some("1.84.0"));
    }

    #[test]
    fn parses_schedule_trigger_from_toml() {
        use crate::types::Trigger;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "nightly"
label = "Nightly CI run"

[[pipeline.steps]]
name = "cargo-check"
argv = ["cargo", "check", "--workspace"]

[[pipeline.triggers]]
kind = "schedule"
cron = "0 2 * * *"

[[pipeline.triggers]]
kind = "manual"
"#;
        let pipeline = loader.load_from_str(toml).expect("should parse");
        assert_eq!(pipeline.triggers.len(), 2);
        assert!(matches!(
            &pipeline.triggers[0],
            Trigger::Schedule { cron } if cron == "0 2 * * *"
        ));
        assert!(matches!(&pipeline.triggers[1], Trigger::Manual));
    }

    // R467-cleanup: the three serialize_builtin round-trip tests were deleted
    // alongside `builtins.rs` and `serialize_builtin_to_toml`. The pipelines
    // they exercised now live as ordinary `.yah/qed/P00*-<name>.toml` files
    // and are covered by the loader's general parse path below.

    #[test]
    fn pipeline_without_triggers_defaults_to_empty_vec() {
        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "check"
label = "Quick check"

[[pipeline.steps]]
name = "cargo-check"
argv = ["cargo", "check"]
"#;
        let pipeline = loader.load_from_str(toml).expect("should parse");
        assert!(pipeline.triggers.is_empty());
    }

    #[test]
    fn parses_optional_runtime_per_step() {
        use velveteen::TaskRuntime;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "mixed"
label = "Mixed runtime pipeline"

[[pipeline.steps]]
name = "native-step"
argv = ["echo", "hi"]

[[pipeline.steps]]
name = "container-step"
argv = ["echo", "hi"]
runtime = "container"
"#;
        let pipeline = loader.load_from_str(toml).expect("should parse");
        assert_eq!(pipeline.steps.len(), 2);
        assert!(
            pipeline.steps[0].runtime.is_none(),
            "no runtime ⇒ pipeline default"
        );
        assert_eq!(pipeline.steps[1].runtime, Some(TaskRuntime::Container));
    }

    #[test]
    fn parses_build_image_step_from_toml() {
        use crate::types::StepKind;

        // push=true requires a registries.toml entry — supply one inline.
        let registries = RegistryConfig {
            registries: vec![RegistryEntry {
                name: "ghcr".into(),
                host: "ghcr.io".into(),
                writable: true,
            }],
        };
        let loader = PipelineLoader::new(".yah/qed").with_registries(registries);
        let toml = r#"
[pipeline]
name  = "image"
label = "Bake an image"

[[pipeline.steps]]
name  = "bake"
kind  = "build-image"
image = "yah-rust"
tag   = "ghcr.io/yah-ai/yah-rust:dev"
push  = true
runtime = "container"
"#;
        let pipeline = loader.load_from_str(toml).expect("valid build-image step");
        assert_eq!(pipeline.steps.len(), 1);
        let step = &pipeline.steps[0];
        assert_eq!(step.kind, StepKind::BuildImage);
        assert_eq!(step.image.as_deref(), Some("yah-rust"));
        assert_eq!(step.tag.as_deref(), Some("ghcr.io/yah-ai/yah-rust:dev"));
        assert!(step.push);
    }

    // ── R381-T6 push validation ────────────────────────────────────────────

    #[test]
    fn build_image_push_without_registry_rejected() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed"); // no registries.toml
        let toml = r#"
[pipeline]
name  = "image"
label = "Bake an image"

[[pipeline.steps]]
name  = "bake"
kind  = "build-image"
image = "yah-rust"
tag   = "ghcr.io/yah-ai/yah-rust:dev"
push  = true
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        match err {
            ConfigError::InvalidStep(StepValidationError::PushRequiresWritableRegistry {
                step,
                host,
            }) => {
                assert_eq!(step, "bake");
                assert_eq!(host, "ghcr.io");
            }
            other => panic!("expected PushRequiresWritableRegistry, got {other:?}"),
        }
    }

    #[test]
    fn build_image_push_with_writable_registry_accepted() {
        let registries = RegistryConfig {
            registries: vec![RegistryEntry {
                name: "ghcr".into(),
                host: "ghcr.io".into(),
                writable: true,
            }],
        };
        let loader = PipelineLoader::new(".yah/qed").with_registries(registries);
        let toml = r#"
[pipeline]
name  = "image"
label = "Bake an image"

[[pipeline.steps]]
name  = "bake"
kind  = "build-image"
image = "yah-rust"
tag   = "ghcr.io/yah-ai/yah-rust:dev"
push  = true
"#;
        loader
            .load_from_str(toml)
            .expect("writable registry should allow push");
    }

    #[test]
    fn build_image_push_with_readonly_registry_rejected() {
        // Entry exists but writable=false → still rejected.
        let registries = RegistryConfig {
            registries: vec![RegistryEntry {
                name: "ghcr".into(),
                host: "ghcr.io".into(),
                writable: false,
            }],
        };
        let loader = PipelineLoader::new(".yah/qed").with_registries(registries);
        let toml = r#"
[pipeline]
name  = "image"
label = "Bake an image"

[[pipeline.steps]]
name  = "bake"
kind  = "build-image"
image = "yah-rust"
tag   = "ghcr.io/yah-ai/yah-rust:dev"
push  = true
"#;
        loader
            .load_from_str(toml)
            .expect_err("readonly registry must reject push");
    }

    #[test]
    fn build_image_push_false_ignores_registry_config() {
        // No registries.toml; push=false → no validation needed.
        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "image"
label = "Bake an image"

[[pipeline.steps]]
name  = "bake"
kind  = "build-image"
image = "yah-rust"
tag   = "ghcr.io/yah-ai/yah-rust:dev"
# push omitted → default false → OCI archive fallback (R381-T4)
"#;
        loader
            .load_from_str(toml)
            .expect("push=false bypasses registry check");
    }

    #[test]
    fn build_image_push_falls_back_to_image_when_tag_absent() {
        // A bare `image = "yah-rust"` with no tag and push=true: the host
        // derived from "yah-rust" is docker.io. No registry → rejected.
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "image"
label = "Bake an image"

[[pipeline.steps]]
name  = "bake"
kind  = "build-image"
image = "yah-rust"
push  = true
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        match err {
            ConfigError::InvalidStep(StepValidationError::PushRequiresWritableRegistry {
                step,
                host,
            }) => {
                assert_eq!(step, "bake");
                assert_eq!(host, "docker.io", "no tag → docker.io fallback");
            }
            other => panic!("expected PushRequiresWritableRegistry, got {other:?}"),
        }
    }

    #[test]
    fn build_image_step_without_image_field_rejected() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "image"
label = "Bake an image"

[[pipeline.steps]]
name = "bake"
kind = "build-image"
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        match err {
            ConfigError::InvalidStep(StepValidationError::BuildImageMissingImage(name)) => {
                assert_eq!(name, "bake");
            }
            other => panic!("expected BuildImageMissingImage, got {other:?}"),
        }
    }

    #[test]
    fn build_image_step_with_native_runtime_rejected() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "image"
label = "Bake an image"

[[pipeline.steps]]
name    = "bake"
kind    = "build-image"
image   = "yah-rust"
runtime = "native"
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        match err {
            ConfigError::InvalidStep(StepValidationError::BuildImageNativeRuntime(name)) => {
                assert_eq!(name, "bake");
            }
            other => panic!("expected BuildImageNativeRuntime, got {other:?}"),
        }
    }

    // ── R407-T2 package-native-tarball parse-time validation ───────────────

    #[test]
    fn parses_package_native_tarball_step_from_toml() {
        use crate::types::StepKind;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "pack-yubaba"
label = "Package native yubaba"

[[pipeline.steps]]
name        = "pack"
kind        = "package-native-tarball"
image       = "yah-yubaba"
binary_path = "target/x86_64-unknown-linux-musl/release/yubaba"
triple      = "x86_64-unknown-linux-musl"
"#;
        let pipeline = loader.load_from_str(toml).expect("valid package step");
        assert_eq!(pipeline.steps.len(), 1);
        let step = &pipeline.steps[0];
        assert_eq!(step.kind, StepKind::PackageNativeTarball);
        assert_eq!(step.image.as_deref(), Some("yah-yubaba"));
        assert_eq!(
            step.binary_path.as_deref(),
            Some("target/x86_64-unknown-linux-musl/release/yubaba"),
        );
        assert_eq!(step.triple.as_deref(), Some("x86_64-unknown-linux-musl"));
    }

    #[test]
    fn package_native_tarball_without_image_rejected_at_parse_time() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "pack"
label = "pack"

[[pipeline.steps]]
name        = "p"
kind        = "package-native-tarball"
binary_path = "target/release/yubaba"
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        assert!(matches!(
            err,
            ConfigError::InvalidStep(StepValidationError::PackageNativeTarballMissingImage(ref n))
            if n == "p"
        ));
    }

    #[test]
    fn package_native_tarball_without_binary_path_rejected_at_parse_time() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "pack"
label = "pack"

[[pipeline.steps]]
name  = "p"
kind  = "package-native-tarball"
image = "yah-yubaba"
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        assert!(matches!(
            err,
            ConfigError::InvalidStep(StepValidationError::PackageNativeTarballMissingBinaryPath(ref n))
            if n == "p"
        ));
    }

    #[test]
    fn package_native_tarball_with_container_runtime_rejected_at_parse_time() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "pack"
label = "pack"

[[pipeline.steps]]
name        = "p"
kind        = "package-native-tarball"
image       = "yah-yubaba"
binary_path = "target/release/yubaba"
runtime     = "container"
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        assert!(matches!(
            err,
            ConfigError::InvalidStep(StepValidationError::PackageNativeTarballContainerRuntime(ref n))
            if n == "p"
        ));
    }

    // ── R407-T3 musl-static-preflight parse-time validation ───────────────

    #[test]
    fn parses_musl_static_preflight_step_from_toml() {
        use crate::types::StepKind;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "yubaba-preflight"
label = "Gate yubaba against musl-static deps"

[[pipeline.steps]]
name    = "musl-gate"
kind    = "musl-static-preflight"
package = "yubaba"
"#;
        let pipeline = loader.load_from_str(toml).expect("valid preflight step");
        assert_eq!(pipeline.steps.len(), 1);
        let step = &pipeline.steps[0];
        assert_eq!(step.kind, StepKind::MuslStaticPreflight);
        assert_eq!(step.package.as_deref(), Some("yubaba"));
    }

    #[test]
    fn musl_static_preflight_without_package_rejected_at_parse_time() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "preflight"
label = "preflight"

[[pipeline.steps]]
name = "p"
kind = "musl-static-preflight"
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        assert!(matches!(
            err,
            ConfigError::InvalidStep(StepValidationError::MuslStaticPreflightMissingPackage(ref n))
            if n == "p"
        ));
    }

    #[test]
    fn musl_static_preflight_with_container_runtime_rejected_at_parse_time() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "preflight"
label = "preflight"

[[pipeline.steps]]
name    = "p"
kind    = "musl-static-preflight"
package = "yubaba"
runtime = "container"
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        assert!(matches!(
            err,
            ConfigError::InvalidStep(StepValidationError::MuslStaticPreflightContainerRuntime(ref n))
            if n == "p"
        ));
    }

    #[test]
    fn musl_static_preflight_with_argv_rejected_at_parse_time() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "preflight"
label = "preflight"

[[pipeline.steps]]
name    = "p"
kind    = "musl-static-preflight"
package = "yubaba"
argv    = ["cargo", "metadata"]
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        assert!(matches!(
            err,
            ConfigError::InvalidStep(StepValidationError::MuslStaticPreflightHasArgv(ref n))
            if n == "p"
        ));
    }

    // ── R407-T5 sign-native-tarball parse-time validation ──────────────────

    #[test]
    fn parses_sign_native_tarball_step_from_toml() {
        use crate::types::StepKind;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "sign-yubaba"
label = "Sign native yubaba tarball"

[[pipeline.steps]]
name   = "sign"
kind   = "sign-native-tarball"
image  = "yah-yubaba"
triple = "x86_64-unknown-linux-musl"
"#;
        let pipeline = loader.load_from_str(toml).expect("valid sign step");
        assert_eq!(pipeline.steps.len(), 1);
        let step = &pipeline.steps[0];
        assert_eq!(step.kind, StepKind::SignNativeTarball);
        assert_eq!(step.image.as_deref(), Some("yah-yubaba"));
        assert_eq!(step.triple.as_deref(), Some("x86_64-unknown-linux-musl"));
    }

    #[test]
    fn sign_native_tarball_without_image_rejected_at_parse_time() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "sign"
label = "sign"

[[pipeline.steps]]
name = "s"
kind = "sign-native-tarball"
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        assert!(matches!(
            err,
            ConfigError::InvalidStep(StepValidationError::SignNativeTarballMissingImage(ref n))
            if n == "s"
        ));
    }

    #[test]
    fn sign_native_tarball_with_argv_rejected_at_parse_time() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "sign"
label = "sign"

[[pipeline.steps]]
name  = "s"
kind  = "sign-native-tarball"
image = "yah-yubaba"
argv  = ["cosign", "sign-blob"]
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        assert!(matches!(
            err,
            ConfigError::InvalidStep(StepValidationError::SignNativeTarballHasArgv(ref n))
            if n == "s"
        ));
    }

    #[test]
    fn sign_native_tarball_with_container_runtime_rejected_at_parse_time() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "sign"
label = "sign"

[[pipeline.steps]]
name    = "s"
kind    = "sign-native-tarball"
image   = "yah-yubaba"
runtime = "container"
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        assert!(matches!(
            err,
            ConfigError::InvalidStep(StepValidationError::SignNativeTarballContainerRuntime(ref n))
            if n == "s"
        ));
    }

    #[test]
    fn build_image_step_with_argv_rejected() {
        use crate::types::StepValidationError;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "image"
label = "Bake an image"

[[pipeline.steps]]
name  = "bake"
kind  = "build-image"
image = "yah-rust"
argv  = ["docker", "build", "."]
"#;
        let err = loader.load_from_str(toml).expect_err("must reject");
        match err {
            ConfigError::InvalidStep(StepValidationError::BuildImageHasArgv(name)) => {
                assert_eq!(name, "bake");
            }
            other => panic!("expected BuildImageHasArgv, got {other:?}"),
        }
    }

    #[test]
    fn build_image_step_parses_context_and_load_fields() {
        use crate::types::StepKind;
        use std::path::PathBuf;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "build-yubaba"
label = "Build yah-yubaba locally"

[[pipeline.steps]]
name    = "image"
kind    = "build-image"
image   = "yah-yubaba"
tag     = "ghcr.io/yah-ai/yah-yubaba:latest"
context = "target/yah-yubaba-ctx"
load    = true
push    = false
"#;
        let pipeline = loader.load_from_str(toml).expect("valid build-image step");
        assert_eq!(pipeline.steps.len(), 1);
        let step = &pipeline.steps[0];
        assert_eq!(step.kind, StepKind::BuildImage);
        assert_eq!(step.image.as_deref(), Some("yah-yubaba"));
        assert_eq!(
            step.tag.as_deref(),
            Some("ghcr.io/yah-ai/yah-yubaba:latest")
        );
        assert_eq!(step.context, Some(PathBuf::from("target/yah-yubaba-ctx")));
        assert!(step.load);
        assert!(!step.push);
    }

    /// R590-F4: the `rusty-v8-musl` pipeline shape parses — a subprocess step
    /// carrying a per-step `image`, `runtime = "container"`, and a
    /// `platform = { target = "…", native = true }` inline table — and that
    /// declaration resolves to Offload on an arm64 host (so `pipeline_needs_offload`
    /// tells the CLI to stand up the fleet path). Mirrors
    /// `.yah/qed/rusty-v8-musl.toml`.
    #[test]
    fn native_container_run_step_parses_and_offloads() {
        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "rusty-v8-musl"
label = "Build rusty_v8 static lib for x86_64-unknown-linux-musl"
placement = "anywhere"

[[pipeline.steps]]
name     = "build-v8-musl"
image    = "cr.yah.dev/rusty-v8-musl-builder:v149.4.0-amd64@sha256:a1fb9d9cc631dcb844fbbb949dc65a80be1d532fa80868c4df5ed4b21939f9a4"
runtime  = "container"
platform = { target = "x86_64-unknown-linux-musl", native = true }
argv     = ["build-v8.sh 'x86_64-unknown-linux-musl' '/tmp/out.tar.gz'"]
timeout  = 9000
"#;
        let pipeline = loader
            .load_from_str(toml)
            .expect("rusty-v8-musl pipeline shape must parse");
        assert_eq!(pipeline.steps.len(), 1);
        let step = &pipeline.steps[0];
        assert_eq!(
            step.image.as_deref(),
            Some(
                "cr.yah.dev/rusty-v8-musl-builder:v149.4.0-amd64\
                 @sha256:a1fb9d9cc631dcb844fbbb949dc65a80be1d532fa80868c4df5ed4b21939f9a4"
            ),
            "the full digest-pinned ref survives the loader verbatim (R590-B5)",
        );
        let plat = step.platform.as_ref().expect("platform declared");
        assert_eq!(plat.target.as_deref(), Some("x86_64-unknown-linux-musl"));
        assert!(plat.native, "native flag must round-trip from the inline table");

        // On an arm64 host the native x86 step offloads → the CLI needs the fleet.
        assert!(crate::runner::pipeline_needs_offload(
            &pipeline,
            "aarch64-apple-darwin"
        ));
        // On the x86 build-worker it's host-arch → no offload (runs there).
        assert!(!crate::runner::pipeline_needs_offload(
            &pipeline,
            "x86_64-unknown-linux-gnu"
        ));
    }

    #[test]
    fn build_image_step_context_defaults_to_none_when_absent() {
        use crate::types::StepKind;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "build-yubaba"
label = "Build image"

[[pipeline.steps]]
name  = "image"
kind  = "build-image"
image = "yah-yubaba"
"#;
        let pipeline = loader.load_from_str(toml).expect("valid");
        let step = &pipeline.steps[0];
        assert_eq!(step.kind, StepKind::BuildImage);
        assert!(step.context.is_none(), "context should default to None");
        assert!(!step.load, "load should default to false");
    }

    #[test]
    fn loader_resolver_synthesizes_pipeline_for_gha_workflow_target() {
        // W200-F9: a SubPipelineRef::GhaWorkflow target no longer returns
        // None — it resolves to a one-step Pipeline whose step kind is
        // GhaWorkflow + carries the path/event/inputs the parent declared.
        use crate::types::SubPipelineRef;
        let loader = PipelineLoader::new(".yah/qed");
        let resolver = LoaderSubPipelineResolver::new(loader);
        let mut inputs = std::collections::HashMap::new();
        inputs.insert("tag".into(), "v1.0.0".into());
        let target = SubPipelineRef::GhaWorkflow {
            path: std::path::PathBuf::from(".github/workflows/release.yml"),
            event: Some("workflow_dispatch".into()),
            inputs,
        };
        let pipeline = resolver.resolve(&target).expect("must resolve");
        assert_eq!(pipeline.steps.len(), 1);
        let step = &pipeline.steps[0];
        assert_eq!(step.kind, crate::types::StepKind::GhaWorkflow);
        let cfg = step.gha_workflow.as_ref().expect("gha_workflow block");
        assert_eq!(
            cfg.path,
            std::path::PathBuf::from(".github/workflows/release.yml")
        );
        assert_eq!(cfg.event.as_deref(), Some("workflow_dispatch"));
        assert_eq!(cfg.inputs.get("tag").map(|s| s.as_str()), Some("v1.0.0"));
    }

    #[test]
    fn parses_tag_trigger_from_toml() {
        use crate::types::Trigger;

        let loader = PipelineLoader::new(".yah/qed");
        let toml = r#"
[pipeline]
name  = "release"
label = "Release on tag"

[[pipeline.steps]]
name = "build"
argv = ["cargo", "build", "--release"]

[[pipeline.triggers]]
kind    = "tag"
pattern = "v*.*.*"
"#;
        let pipeline = loader.load_from_str(toml).expect("should parse");
        assert_eq!(pipeline.triggers.len(), 1);
        assert!(matches!(
            &pipeline.triggers[0],
            Trigger::Tag { pattern } if pattern == "v*.*.*"
        ));
    }

    // ---- R494-F2: cross-camp Peer resolution ---------------------------

    /// Build a fixture parent-camp + peer-camp pair under a tempdir.
    /// Layout:
    ///   <tmp>/parent/.yah/qed/peers.toml   (parent's peers registry)
    ///   <tmp>/peers/cheers/.yah/qed/publish.toml  (peer pipeline)
    /// Returns the parent's qed_dir for `PipelineLoader::new(...)`.
    fn fixture_peer_camp(
        tmp: &Path,
        peer_pipeline_toml: &str,
        peers_toml: &str,
    ) -> std::path::PathBuf {
        let parent_qed = tmp.join("parent/.yah/qed");
        fs::create_dir_all(&parent_qed).unwrap();
        fs::write(parent_qed.join("peers.toml"), peers_toml).unwrap();

        let peer_qed = tmp.join("peers/cheers/.yah/qed");
        fs::create_dir_all(&peer_qed).unwrap();
        fs::write(peer_qed.join("publish.toml"), peer_pipeline_toml).unwrap();

        parent_qed
    }

    const PEER_PUBLISH_TOML: &str = r#"
[pipeline]
name  = "publish"
label = "Publish cheers"

[[pipeline.steps]]
name = "build"
argv = ["cargo", "build", "--release"]
"#;

    #[test]
    fn peer_resolver_loads_pipeline_from_sibling_camp() {
        let tmp = tempfile::tempdir().unwrap();
        let parent_qed = fixture_peer_camp(
            tmp.path(),
            PEER_PUBLISH_TOML,
            r#"
            [peer.cheers]
            path = "../peers/cheers"
            "#,
        );
        let loader = PipelineLoader::new(&parent_qed);
        let resolver = LoaderSubPipelineResolver::new(loader);
        let resolved = resolver
            .resolve(&SubPipelineRef::Peer {
                camp: "cheers".into(),
                pipeline: "publish".into(),
            })
            .expect("peer pipeline should resolve");
        assert_eq!(resolved.name, "publish");
        assert_eq!(resolved.steps.len(), 1);
        // No explicit concurrency_key on the peer pipeline → stamped to peer:<camp>
        // so two parent runs invoking different pipelines in the same peer camp
        // still serialize on that camp's shared `target/`.
        assert_eq!(resolved.concurrency_key.as_deref(), Some("peer:cheers"));
    }

    #[test]
    fn peer_resolver_reports_peer_camp_root_for_subprocess_cwd() {
        // Regression: peer children must execute in the *peer* camp's
        // workspace, not the parent's. Without this, `peer-binaries` runs
        // yubaba's `cargo publish -p workload-spec` from yah's root and the
        // package isn't found. resolved_camp_root feeds the child runner's
        // camp_root, which is the cwd for subprocess steps.
        let tmp = tempfile::tempdir().unwrap();
        let parent_qed = fixture_peer_camp(
            tmp.path(),
            PEER_PUBLISH_TOML,
            r#"
            [peer.cheers]
            path = "../peers/cheers"
            "#,
        );
        let loader = PipelineLoader::new(&parent_qed);
        let resolver = LoaderSubPipelineResolver::new(loader);
        let root = resolver
            .resolved_camp_root(&SubPipelineRef::Peer {
                camp: "cheers".into(),
                pipeline: "publish".into(),
            })
            .expect("peer camp root should resolve");
        // qed_dir is `<tmp>/parent/.yah/qed`; pop twice → `<tmp>/parent`,
        // join the peer's `../peers/cheers`.
        assert_eq!(root, tmp.path().join("parent").join("../peers/cheers"));
        // Non-peer targets share the parent camp → inherit (None).
        assert!(resolver
            .resolved_camp_root(&SubPipelineRef::Builtin("check".into()))
            .is_none());
        // Unknown peer → no local root.
        assert!(resolver
            .resolved_camp_root(&SubPipelineRef::Peer {
                camp: "ghost".into(),
                pipeline: "publish".into(),
            })
            .is_none());
    }

    #[test]
    fn peer_resolver_preserves_explicit_concurrency_key() {
        let tmp = tempfile::tempdir().unwrap();
        let parent_qed = fixture_peer_camp(
            tmp.path(),
            r#"
[pipeline]
name             = "publish"
label            = "Publish cheers"
concurrency_key  = "@parallel"

[[pipeline.steps]]
name = "build"
argv = ["cargo", "build", "--release"]
"#,
            r#"
            [peer.cheers]
            path = "../peers/cheers"
            "#,
        );
        let loader = PipelineLoader::new(&parent_qed);
        let resolver = LoaderSubPipelineResolver::new(loader);
        let resolved = resolver
            .resolve(&SubPipelineRef::Peer {
                camp: "cheers".into(),
                pipeline: "publish".into(),
            })
            .expect("peer pipeline should resolve");
        // Explicit key wins — peer opts out of the camp-wide serialization.
        assert_eq!(resolved.concurrency_key.as_deref(), Some("@parallel"));
    }

    #[test]
    fn peer_resolver_returns_none_for_unknown_camp() {
        let tmp = tempfile::tempdir().unwrap();
        let parent_qed = fixture_peer_camp(
            tmp.path(),
            PEER_PUBLISH_TOML,
            r#"
            [peer.cheers]
            path = "../peers/cheers"
            "#,
        );
        let loader = PipelineLoader::new(&parent_qed);
        let resolver = LoaderSubPipelineResolver::new(loader);
        let resolved = resolver.resolve(&SubPipelineRef::Peer {
            camp: "ghost".into(),
            pipeline: "publish".into(),
        });
        assert!(resolved.is_none());
    }

    #[test]
    fn peer_resolver_returns_none_for_unknown_pipeline_in_known_camp() {
        let tmp = tempfile::tempdir().unwrap();
        let parent_qed = fixture_peer_camp(
            tmp.path(),
            PEER_PUBLISH_TOML,
            r#"
            [peer.cheers]
            path = "../peers/cheers"
            "#,
        );
        let loader = PipelineLoader::new(&parent_qed);
        let resolver = LoaderSubPipelineResolver::new(loader);
        let resolved = resolver.resolve(&SubPipelineRef::Peer {
            camp: "cheers".into(),
            pipeline: "no-such-pipeline".into(),
        });
        assert!(resolved.is_none());
    }

    #[test]
    fn peer_resolver_remote_peer_surfaces_typed_unsupported_reason() {
        // R494-T5: when peers.toml carries a `rig = ...` field, the
        // resolver returns None *and* publishes a typed reason naming the
        // camp + rig so the runner's StepFailed.msg routes the operator
        // to either drop the rig field or wait for the kamaji hop.
        let tmp = tempfile::tempdir().unwrap();
        let parent_qed = fixture_peer_camp(
            tmp.path(),
            PEER_PUBLISH_TOML,
            r#"
            [peer.cheers]
            rig  = "rig-tokyo-1"
            path = "/srv/camps/cheers"
            "#,
        );
        let loader = PipelineLoader::new(&parent_qed);
        let resolver = LoaderSubPipelineResolver::new(loader);
        let target = SubPipelineRef::Peer {
            camp: "cheers".into(),
            pipeline: "publish".into(),
        };
        assert!(
            resolver.resolve(&target).is_none(),
            "remote peer should not resolve in v1"
        );
        let reason = resolver
            .unresolved_reason(&target)
            .expect("remote-peer miss should publish a typed reason");
        assert!(
            reason.contains("rig-tokyo-1"),
            "reason names the rig: {reason}"
        );
        assert!(reason.contains("cheers"), "reason names the camp: {reason}");
        assert!(
            reason.contains("R494-T5"),
            "reason cites the ticket: {reason}"
        );
    }

    #[test]
    fn peer_resolver_unknown_camp_publishes_actionable_reason() {
        // Unknown camp: reason should mention peers.toml so operators
        // know where to declare the entry.
        let tmp = tempfile::tempdir().unwrap();
        let parent_qed = fixture_peer_camp(
            tmp.path(),
            PEER_PUBLISH_TOML,
            r#"
            [peer.cheers]
            path = "../peers/cheers"
            "#,
        );
        let loader = PipelineLoader::new(&parent_qed);
        let resolver = LoaderSubPipelineResolver::new(loader);
        let target = SubPipelineRef::Peer {
            camp: "ghost".into(),
            pipeline: "publish".into(),
        };
        assert!(resolver.resolve(&target).is_none());
        let reason = resolver
            .unresolved_reason(&target)
            .expect("reason for unknown camp");
        assert!(reason.contains("ghost"), "reason names the camp: {reason}");
        assert!(
            reason.contains("peers.toml"),
            "reason routes to peers.toml: {reason}"
        );
    }

    #[test]
    fn peer_resolver_unknown_pipeline_in_known_camp_publishes_reason() {
        // Known camp, missing pipeline: reason names the pipeline and the
        // resolved peer-camp path so the operator can grep that directory.
        let tmp = tempfile::tempdir().unwrap();
        let parent_qed = fixture_peer_camp(
            tmp.path(),
            PEER_PUBLISH_TOML,
            r#"
            [peer.cheers]
            path = "../peers/cheers"
            "#,
        );
        let loader = PipelineLoader::new(&parent_qed);
        let resolver = LoaderSubPipelineResolver::new(loader);
        let target = SubPipelineRef::Peer {
            camp: "cheers".into(),
            pipeline: "no-such".into(),
        };
        assert!(resolver.resolve(&target).is_none());
        let reason = resolver
            .unresolved_reason(&target)
            .expect("reason for missing pipeline");
        assert!(
            reason.contains("no-such"),
            "reason names the pipeline: {reason}"
        );
        assert!(reason.contains("cheers"), "reason names the camp: {reason}");
    }

    #[test]
    fn peer_resolver_unresolved_reason_is_none_for_non_peer_targets() {
        // Other SubPipelineRef shapes go through their own resolvers
        // (Builtin/Path/GhaWorkflow); LoaderSubPipelineResolver only
        // diagnoses peer misses.
        let tmp = tempfile::tempdir().unwrap();
        let parent_qed = fixture_peer_camp(
            tmp.path(),
            PEER_PUBLISH_TOML,
            r#"
            [peer.cheers]
            path = "../peers/cheers"
            "#,
        );
        let loader = PipelineLoader::new(&parent_qed);
        let resolver = LoaderSubPipelineResolver::new(loader);
        assert!(resolver
            .unresolved_reason(&SubPipelineRef::Builtin("missing".into()))
            .is_none());
        assert!(resolver
            .unresolved_reason(&SubPipelineRef::Path(".yah/qed/missing.toml".into()))
            .is_none());
    }
}
