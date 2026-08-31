//! @yah:ticket(R299-T4, "Create .yah/qed/ pipeline directory")
//! @yah:at(2026-05-23T01:43:12Z)
//! @yah:status(review)
//! @yah:parent(R299)
//! @yah:handoff(".yah/qed/ directory created at workspace root. PipelineLoader.list_all() now dedupes built-ins + custom files. Camp TOML overrides built-ins by name.")
//!
//! @yah:relay(R751, "QED templates + specializations: alias_of/pin, template classification, smell-your-way-to-it discovery")
//! @yah:at(2026-08-12T02:22:52Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:handoff("GOAL (operator, 2026-08-11): every build job should be a QED run or a VARIANT of one. The vocabulary the operator asked for is C++/Rust templates: a pipeline with unbound required params is a TEMPLATE - it cannot run without specialization - and a semantic name bound to a template plus pinned params is a SPECIALIZATION. Specializations carry their own title and tags so an operator can smell their way to them in the roster instead of knowing the base pipeline's name.")
//! @yah:handoff("WHAT ALREADY EXISTS, verified in source, so nobody rebuilds it: ParamDef with required/default/options/options_from (oss/qed/crates/qed/src/types.rs:2174); {{key}} substitution into argv, env and a wrapped GHA workflow's inputs+matrix (types.rs:677 apply_params); params-as-VARIANTS via step gating - R653-F1 put resolved params into the if-context, so if = \"params.variant == 'full'\" is live today (types.rs:929, wired at app/yah/cli/src/qed.rs:757); SubPipelineConfig{target, params, propagate, opaque} forwards pinned params to a child (types.rs:1158); tags: Vec<String> and label already on PipelineConfig (config.rs:141-152) and already on the FE PipelineDef. The template CONCEPT also already exists in the UI as a derived runnable-vs-template cut (QedPanel.tsx:1702-1708). None of that needs inventing.")
//! @yah:next("THE SEAM IS ONE FUNCTION: PipelineLoader::load (oss/qed/crates/qed/src/config.rs:234). Every consumer - CLI, QED_PIPELINES RPC, desktop Run tab, LoaderSubPipelineResolver - goes through it, so resolving a specialization there makes it indistinguishable from a hand-written pipeline everywhere with no downstream change: base steps, alias's own name/label/tags/description, base param schema MINUS the pinned keys.")
//! @yah:handoff("THE GAP IS THE NAME, NOT THE CONFIGURATION. rg alias over config.rs/types.rs/runner.rs returns nothing, so a semantic name costs a whole pipeline file whose only content is a one-step SubPipeline wrapper - and that file then LIES in the catalog, because qed_pipelines_handler (app/yah/cli/src/camp.rs:8971) reads steps and params off whatever loader.load(name) returned, so the wrapper reports 1 step and no params. .yah/qed/desktop-local.toml is exactly that shape today.")
//! @yah:assumes("TWO DESIGN DEFAULTS the operator accepted by saying 'sure file it' rather than by picking. (1) A specialization lives in ITS OWN FILE (alias_of in [pipeline]), not as [[alias]] blocks inside the base - so a camp can specialize a builtin or an oss/-shipped pipeline without editing it. (2) A PINNED param is NOT overridable from --param: a pin a flag can undo is not a variant, it is a default, and ParamDef.default already covers that. Reverse either only on purpose.")
//! @yah:gotcha("oss/qed is an independent Cargo workspace exported to a public mirror (github.com/yah-ai/qed) - edit in place, never the mirror, and do not use workspace=true inheritance from the yah root. PipelineToml/PipelineConfig are ALSO the schema source of truth: cargo run -p xtask -- emit-schemas derives .yah/schema/qed-pipeline.toml.schema.json from them and xtask/tests/schema_drift.rs asserts it matches. Any new [pipeline] key here means regenerating that schema in the same commit - the pre-commit hook does it if core.hooksPath is set to scripts/git-hooks.")
//!
//! @yah:ticket(R751-F2, "alias_of + pin: specializations resolved in PipelineLoader::load, with their own label/tags")
//! @yah:status(review)
//! @yah:at(2026-08-12T22:09:21Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R751)
//! @yah:next("Resolve in PipelineLoader::load (config.rs:234), NOT in each caller. Everything downstream - the CLI, QED_PIPELINES, the desktop Run tab, LoaderSubPipelineResolver - already funnels through it, so a specialization then needs zero downstream code to appear as a first-class entry.")
//! @yah:next("Reject at load time, loudly, with a typed ConfigError: alias_of naming an unknown base; a pin key the base does not declare (the whole point is that a pin is checked against a real param); an alias that also declares steps; and an alias chain that cycles or exceeds a small depth cap. MAX_SUB_PIPELINE_DEPTH (types.rs:1148) is the precedent for the cap, and validate_sub_pipeline_graph is the precedent for the cycle walk.")
//! @yah:next("Do NOT build this as a SubPipeline wrapper under the hood. That is exactly the shape being replaced, it doubles the run tree for no reason, and it would put the alias's steps behind an extra node in the graph tab.")
//! @yah:handoff("THE PRIMITIVE. A specialization is a steps-less pipeline file: [pipeline] name/label/tags/description of its own, alias_of = \"<base>\", and a [pipeline.pin] table fixing some of the base's params. The loader resolves it into a real Pipeline - base steps, alias's identity, base params minus the pinned keys - so desktop-local / all-local / another-local-variant become ~6-line files over one real pipeline.")
//! @yah:verify("A specialization appears in qed.pipelines with its OWN name/label/tags and the base's step list - not as a 1-step wrapper. Compare against .yah/qed/desktop-local.toml's current wire shape (1 step, no params) to see the difference the ticket is buying.")
//! @yah:verify("A pinned param is absent from the alias's advertised params and cannot be overridden by --param (see the relay's assumes - reverse only on purpose).")
//! @yah:verify("Round-trip: a specialization TOML survives load -> export -> load unchanged. oss/qed/crates/qed/src/export.rs is the serializer that has to learn the new keys.")
//! @yah:gotcha("PipelineConfig is the schemars source for .yah/schema/qed-pipeline.toml.schema.json - adding alias_of/pin means running cargo run -p xtask -- emit-schemas and committing the regenerated schema, or xtask/tests/schema_drift.rs goes red for everyone on the shared tree. It is a generated artifact, so regenerating it is nobody's permission to ask for.")
//! @yah:handoff("SHIPPED. A specialization is a steps-less TOML: [pipeline] name/label/tags/description of its own, alias_of = <base>, and a [pipeline.pin] table. PipelineLoader::load (oss/qed/crates/qed/src/config.rs:361) resolves it, so the CLI, qed.pipelines, the desktop Run tab and LoaderSubPipelineResolver all see a first-class pipeline with ZERO downstream change: base steps, alias identity, base params MINUS the pinned keys.")
//! @yah:handoff("Pipeline grew two provenance fields (types.rs:449 alias_of, types.rs:472 pins); PipelineConfig grew alias_of + pin (config.rs). Pins are moved OUT of params at load and re-inserted by resolve_params (types.rs:657), so {{key}} substitution and R653-F1 if= step gating see a pinned value exactly like any other resolved param.")
//! @yah:handoff("Five typed load-time rejections, all with repair hints: AliasBaseNotFound, AliasHasBody (every offending body key at once), AliasPinUnknownParam (lists what the base does declare), AliasPinNotInOptions, AliasChain (cycle + MAX_ALIAS_DEPTH=4, mirroring MAX_SUB_PIPELINE_DEPTH). A pinned param is not overridable: ParamError::PinnedParamOverride fires on a CONFLICTING --param, while the same value is a no-op so re-running a recorded param set still works.")
//! @yah:verify("12 new tests in config::alias_tests (cargo test -p yah-qed --lib alias, run from oss/qed): 12 passed. Cover identity/body split, pin reaching the run + refusing override, all five rejections, alias-of-an-alias pin accumulation, tag + description fallback, and serde round-trip (an ordinary pipeline serializes with NO alias_of/pins keys, so qed eject output is unchanged).")
//! @yah:verify("E2E through the real binary, not only unit tests: a 7-line desktop-local.toml over a 2-step local-install planned as 'desktop-local - 1 expanded job(s)' with steps Build + Install (oss/qed/target/debug/yah-qed plan). The 1-step SubPipeline wrapper shape this replaces reports 1 step and no params. A bad pin failed with: base pipeline local-install declares no such param; it declares: profile, target.")
//! @yah:verify("Regression sweep: every .yah/qed/*.toml in this camp still loads through the changed loader (only baseline/gha-actions/peers fail, and those are not pipeline files). cargo check --workspace --all-targets clean. cargo test -p yah --test camp_qed_image_pins 3 passed.")
//! @yah:verify("Generated artifacts regenerated and green: cargo run -p xtask -- emit-schemas added alias_of + pin to .yah/schema/qed-pipeline.toml.schema.json; cargo test -p xtask --test schema_drift 3 passed; scripts/check-workload-spec-ts.sh in sync.")
//! @yah:gotcha("Pre-existing, NOT from this ticket: cargo test -p yah-qed --lib has one failure, tests::desktop_release_matrix_routes_each_row_to_its_own_platform, which asserts 3 matrix rows while .yah/qed/desktop-release.toml declares 1. The narrowing landed in commit 497a8a6b (a peer sync). Already filed as R577-B5 and explicitly deferred there as a product call. 815 of 816 pass.")

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
    /// R605-F3: the pipeline's `needs` edges don't form a runnable DAG — an
    /// unknown/self/ambiguous reference, or a cycle. Rejected at load time
    /// because the alternative is a run that reports Success having silently
    /// dropped an edge, or one that hangs with nothing ready.
    #[error("Invalid step graph: {0}")]
    InvalidDag(#[from] crate::dag::DagError),
    /// R751-F2: a specialization whose `alias_of` names a pipeline that doesn't
    /// exist. Separate from [`Self::NotFound`] because the name the operator
    /// asked for *did* resolve — it's the file's own reference that dangles,
    /// and saying "pipeline not found: local-instal" when they ran
    /// `desktop-local` sends them looking in the wrong place.
    #[error(
        "pipeline '{alias}': alias_of = {base:?} names a pipeline that does not exist ({source})"
    )]
    AliasBaseNotFound {
        alias: String,
        base: String,
        source: Box<ConfigError>,
    },
    /// R751-F2: `alias_of` alongside keys that define a pipeline BODY. A
    /// specialization binds a base; it does not get to also be one, because
    /// there would be no answer to which set of steps runs.
    #[error(
        "pipeline '{alias}': alias_of = {base:?} cannot be combined with {keys} — a specialization \
         binds the base's body, it does not declare one of its own"
    )]
    AliasHasBody {
        alias: String,
        base: String,
        keys: String,
    },
    /// R751-F2: a `[pipeline.pin]` key the base doesn't declare as a param.
    /// The whole value of a pin over a hand-copied pipeline is that it is
    /// checked against a real declaration, so a stale pin (base renamed the
    /// param) has to fail loudly rather than sit inert in the file.
    #[error(
        "pipeline '{alias}': [pipeline.pin] {name} = {value:?} — base pipeline '{base}' declares no \
         such param{}",
        if known.is_empty() {
            " (it declares none at all)".to_string()
        } else {
            format!("; it declares: {}", known.join(", "))
        }
    )]
    AliasPinUnknownParam {
        alias: String,
        base: String,
        name: String,
        value: String,
        known: Vec<String>,
    },
    /// R751-F2: a pin whose value is outside the base param's declared
    /// `options`. Same reasoning as [`Self::InvalidParam`] for a bad default —
    /// caught at load time, because a specialization pinning an illegal value
    /// would otherwise only fail on the runs that actually reached it.
    #[error(
        "pipeline '{alias}': [pipeline.pin] {name} = {value:?} is not one of base pipeline \
         '{base}' param's options ({})",
        options.join(", ")
    )]
    AliasPinNotInOptions {
        alias: String,
        base: String,
        name: String,
        value: String,
        options: Vec<String>,
    },
    /// R751-F2: `alias_of` chains that cycle, or nest deeper than
    /// [`MAX_ALIAS_DEPTH`].
    #[error("pipeline alias chain {}: {reason}", chain.join(" -> "))]
    AliasChain { chain: Vec<String>, reason: String },
}

/// How deep an `alias_of` chain may nest before the loader refuses it
/// (R751-F2). A specialization of a specialization is legitimate —
/// `all-local` → `desktop-local` → `local-install` pins one more key at each
/// hop — but a chain long enough to hit this is a modelling mistake, not a
/// recipe. Same posture (and same number) as
/// [`MAX_SUB_PIPELINE_DEPTH`](crate::types::MAX_SUB_PIPELINE_DEPTH): a small
/// cap that turns runaway indirection into an error at load rather than a
/// stack overflow at run.
pub const MAX_ALIAS_DEPTH: usize = 4;

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
    /// R751-F2 — declare this file a SPECIALIZATION of the named pipeline.
    ///
    /// ```toml
    /// [pipeline]
    /// name = "desktop-local"
    /// label = "Install the desktop app locally"
    /// tags = ["build", "desktop", "local"]
    /// alias_of = "local-install"
    ///
    /// [pipeline.pin]
    /// target = "desktop"
    /// ```
    ///
    /// The alias lives in its OWN file rather than as `[[alias]]` blocks inside
    /// the base, so a camp can specialize a pipeline shipped by `oss/` (or by
    /// another camp) without editing it — the same reason a C++ template
    /// specialization doesn't live inside the primary template's header.
    ///
    /// Resolution happens in [`PipelineLoader::load`], so no consumer needs to
    /// know: see [`crate::types::Pipeline::alias_of`].
    #[serde(default)]
    alias_of: Option<String>,
    /// R751-F2 — `[pipeline.pin]`: param values this specialization FIXES.
    /// Only meaningful with [`Self::alias_of`]; every key must name a param the
    /// base declares. See [`crate::types::Pipeline::pins`] for why a pin is not
    /// spelled as a `default`.
    #[serde(default)]
    pin: HashMap<String, String>,
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
    /// R605-F3 — ceiling on concurrent steps within one run. See
    /// [`crate::types::Pipeline::max_parallel`].
    #[serde(default)]
    max_parallel: Option<usize>,
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
    ///
    /// R751-F2: a file declaring `alias_of` is a SPECIALIZATION and is resolved
    /// here — the returned `Pipeline` carries the base's body under the alias's
    /// own identity, with `[pipeline.pin]` moved from `params` to `pins`. This
    /// is deliberately the *only* place that happens: the CLI, the daemon's
    /// `qed.pipelines` / `qed.run`, the desktop Run tab and
    /// [`LoaderSubPipelineResolver`] all funnel through `load`, so a
    /// specialization is indistinguishable from a hand-written pipeline
    /// everywhere without a line of downstream change.
    pub fn load(&self, name: &str) -> Result<Pipeline, ConfigError> {
        self.load_chain(name, &mut Vec::new())
    }

    /// [`Self::load`] carrying the `alias_of` chain walked so far, for cycle
    /// and depth detection. `chain` holds pipeline *names* in resolution order.
    fn load_chain(&self, name: &str, chain: &mut Vec<String>) -> Result<Pipeline, ConfigError> {
        if let Some(path) = find_pipeline_file(&self.qed_dir, name) {
            let content = fs::read_to_string(&path)?;
            return self.pipeline_from_str(&content, chain);
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
    fn pipeline_from_str(
        &self,
        content: &str,
        chain: &mut Vec<String>,
    ) -> Result<Pipeline, ConfigError> {
        let parsed: PipelineToml = toml::from_str(content)?;
        // R751-F2: a specialization has no body of its own — it takes the
        // base's and rebrands it. Everything below this point would be
        // operating on an empty `steps`, so branch before building it.
        if parsed.pipeline.alias_of.is_some() {
            return self.resolve_specialization(parsed, content, chain);
        }
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
            alias_of: None,
            pins: HashMap::new(),
            steps: parsed.pipeline.steps,
            params: parsed.pipeline.params.unwrap_or_default(),
            on_success: parsed.pipeline.on_success,
            on_fail: parsed.pipeline.on_fail,
            triggers: parsed.pipeline.triggers,
            concurrency_key: parsed.pipeline.concurrency_key,
            max_parallel: parsed.pipeline.max_parallel,
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
        self.validate_dag(&pipeline)?;
        self.validate_binds(&pipeline)?;
        self.validate_params(&pipeline)?;
        Ok(pipeline)
    }

    /// R751-F2 — turn a parsed specialization file into a real [`Pipeline`].
    ///
    /// What comes from where:
    /// - **body** (steps, params, finally, matrix, toolchain, triggers,
    ///   placement, workspace, outcomes, binds, …) — entirely the base's. The
    ///   alias is forbidden from declaring any of it, so a key that would
    ///   silently do nothing is an error instead.
    /// - **identity** (name, label, description, tags) — the alias's own. This
    ///   is the point of the feature: `desktop-local` is a row in the catalog
    ///   an operator can smell their way to, not a footnote on `local-install`.
    ///   `tags` fall back to the base's when the alias declares none, because a
    ///   specialization of a `build` pipeline is still a build.
    /// - **params** — the base's, MINUS every pinned key, which moves to
    ///   [`Pipeline::pins`]. A pinned param is no longer a question to ask, so
    ///   it must not appear in the run form.
    fn resolve_specialization(
        &self,
        parsed: PipelineToml,
        content: &str,
        chain: &mut Vec<String>,
    ) -> Result<Pipeline, ConfigError> {
        let cfg = parsed.pipeline;
        let alias = cfg.name;
        let base_name = cfg.alias_of.expect("caller checked alias_of is Some");

        // Body keys are the base's, so declaring one here is an authoring
        // error, not an override. Reported together: an author who wrote a
        // whole pipeline body under an `alias_of` should see that in one pass.
        let mut body_keys: Vec<&str> = Vec::new();
        if !cfg.steps.is_empty() {
            body_keys.push("steps");
        }
        if !cfg.finally.is_empty() {
            body_keys.push("finally");
        }
        if cfg.params.as_ref().is_some_and(|p| !p.is_empty()) {
            body_keys.push("params (pin the base's instead)");
        }
        if !cfg.on_success.is_empty() {
            body_keys.push("on_success");
        }
        if !cfg.on_fail.is_empty() {
            body_keys.push("on_fail");
        }
        if !cfg.triggers.is_empty() {
            body_keys.push("triggers");
        }
        if cfg.concurrency_key.is_some() {
            body_keys.push("concurrency_key");
        }
        if cfg.max_parallel.is_some() {
            body_keys.push("max_parallel");
        }
        if cfg.placement != Placement::default() {
            body_keys.push("placement");
        }
        if cfg.workspace != crate::types::WorkspaceMode::default() {
            body_keys.push("workspace");
        }
        if cfg.wraps.is_some() {
            body_keys.push("wraps");
        }
        if cfg.matrix.is_some() {
            body_keys.push("matrix");
        }
        if cfg.toolchain.is_some() {
            body_keys.push("toolchain");
        }
        if !parsed.binds.is_empty() {
            body_keys.push("[[bind]]");
        }
        if !parsed.on_change.is_empty() {
            body_keys.push("[[on_change]]");
        }
        if !body_keys.is_empty() {
            return Err(ConfigError::AliasHasBody {
                alias,
                base: base_name,
                keys: body_keys.join(", "),
            });
        }

        // Cycle + depth, walked over the chain of names rather than of files:
        // `alias_of` is a name reference, so a cycle can only close on one.
        if chain.iter().any(|n| n == &base_name) {
            let mut cycled = chain.clone();
            cycled.push(alias.clone());
            cycled.push(base_name.clone());
            return Err(ConfigError::AliasChain {
                chain: cycled,
                reason: format!("alias_of cycles back to '{base_name}'"),
            });
        }
        chain.push(alias.clone());
        if chain.len() > MAX_ALIAS_DEPTH {
            let mut too_deep = chain.clone();
            too_deep.push(base_name.clone());
            return Err(ConfigError::AliasChain {
                chain: too_deep,
                reason: format!(
                    "alias_of nests deeper than MAX_ALIAS_DEPTH ({MAX_ALIAS_DEPTH})"
                ),
            });
        }

        let mut base =
            self.load_chain(&base_name, chain)
                .map_err(|source| match source {
                    // A chain error from further down is already the more
                    // specific diagnosis; wrapping it in "base not found"
                    // would bury it.
                    e @ (ConfigError::AliasChain { .. }
                    | ConfigError::AliasHasBody { .. }
                    | ConfigError::AliasPinUnknownParam { .. }
                    | ConfigError::AliasPinNotInOptions { .. }
                    | ConfigError::AliasBaseNotFound { .. }) => e,
                    source => ConfigError::AliasBaseNotFound {
                        alias: alias.clone(),
                        base: base_name.clone(),
                        source: Box::new(source),
                    },
                })?;

        // Every pin must name a param the base actually declares. Sorted so a
        // file with two bad pins names the same one on every run.
        let mut pin_names: Vec<&String> = cfg.pin.keys().collect();
        pin_names.sort();
        for name in pin_names {
            let value = &cfg.pin[name];
            let Some(def) = base.params.get(name) else {
                let mut known: Vec<String> = base.params.keys().cloned().collect();
                known.sort();
                return Err(ConfigError::AliasPinUnknownParam {
                    alias: alias.clone(),
                    base: base.name.clone(),
                    name: name.clone(),
                    value: value.clone(),
                    known,
                });
            };
            // `options_from` resolves at READ time (see `ParamDef::options_from`),
            // so a pin against one of those can only be checked when the run
            // resolves it — the same limit the `default` check has.
            if !def.options.is_empty() && !def.options.iter().any(|o| o == value) {
                return Err(ConfigError::AliasPinNotInOptions {
                    alias: alias.clone(),
                    base: base.name.clone(),
                    name: name.clone(),
                    value: value.clone(),
                    options: def.options.clone(),
                });
            }
        }

        // A pinned param stops being a question. Removing it from `params` is
        // what makes the run form, `yah qed pipelines`, and the required-param
        // check all agree that the specialization takes fewer inputs than its
        // base — the visible difference between this and a wrapper pipeline.
        for name in cfg.pin.keys() {
            base.params.remove(name);
        }

        // A specialization OF a specialization inherits the inner pins: the
        // base already moved them out of `params`, so the unknown-param check
        // above guarantees the two sets are disjoint and neither can clobber
        // the other.
        base.pins.extend(cfg.pin);

        // Rebrand in place rather than rebuilding the struct: the whole
        // contract is "the base's body, unchanged", and a field-by-field copy
        // is exactly the thing that silently drops a field somebody adds later.
        base.alias_of = Some(std::mem::replace(&mut base.name, alias));
        base.label = cfg.label;
        // Same precedence as a normal file (explicit key, then the file's own
        // `#` header) with one extra rung: a specialization that says nothing
        // about itself describes the thing it specializes.
        base.description = cfg
            .description
            .or_else(|| leading_comment_block(content))
            .or(base.description);
        if !cfg.tags.is_empty() {
            base.tags = cfg.tags;
        }
        Ok(base)
    }

    #[cfg(test)]
    fn load_from_str(&self, content: &str) -> Result<Pipeline, ConfigError> {
        self.pipeline_from_str(content, &mut Vec::new())
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
        self.pipeline_from_str(&content, &mut Vec::new())
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

    /// R605-F3 parse-time step-graph validation: resolve every `needs` edge
    /// against the whole pipeline and group the result into waves, so an
    /// unknown reference, a self-edge, an ambiguous duplicate name or a cycle
    /// fails the *load* rather than the run.
    ///
    /// Strict about unknown names on purpose — the runner resolves the same
    /// graph with [`crate::dag::Missing::Satisfied`] because a
    /// resume-from-step run hands it a drained prefix, and that leniency is
    /// only safe because this check already proved the full pipeline resolves.
    ///
    /// `[[finally]]` teardown is always-run and unconditional, so a `needs` on
    /// one would be inert config; [`crate::types::QedStep::validate_finally`]
    /// rejects it.
    fn validate_dag(&self, pipeline: &Pipeline) -> Result<(), ConfigError> {
        crate::dag::waves(&pipeline.steps, crate::dag::Missing::Reject)?;
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
        max_parallel: None,
        placement: Placement::default(),
        workspace: crate::types::WorkspaceMode::default(),
        wraps: None,
        matrix: None,
        toolchain: None,
        binds: Vec::new(),
        on_change: Vec::new(),
        alias_of: None,
        pins: Default::default(),
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
                    max_parallel: None,
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
                    alias_of: None,
                    pins: Default::default(),
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
mod alias_tests {
    use super::*;
    use crate::types::ParamError;

    /// The base every specialization test below binds: two steps, three params,
    /// one of them enumerated. Written as real TOML rather than a `Pipeline`
    /// literal because the thing under test is what the *loader* does with a
    /// file, and a literal would skip the parse half of it.
    const LOCAL_INSTALL: &str = r#"
# local-install — build a yah surface and install it into this machine.

[pipeline]
name = "local-install"
label = "Local install"
tags = ["build", "local"]

[pipeline.params]
target = { required = true, options = ["desktop", "cli", "all"] }
profile = { required = false, default = "release" }
notify = { required = false, default = "" }

[[pipeline.steps]]
name = "Build"
argv = ["cargo", "build", "--profile", "{{profile}}", "-p", "yah-{{target}}"]

[[pipeline.steps]]
name = "Install"
argv = ["./scripts/install.sh", "{{target}}"]
"#;

    /// A loader over a temp `.yah/qed` holding `local-install` plus whatever
    /// alias files the test writes.
    fn camp(files: &[(&str, &str)]) -> (tempfile::TempDir, PipelineLoader) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("local-install.toml"), LOCAL_INSTALL).unwrap();
        for (name, body) in files {
            std::fs::write(dir.path().join(format!("{name}.toml")), body).unwrap();
        }
        let loader = PipelineLoader::new(dir.path());
        (dir, loader)
    }

    // ── R605-F3: step-graph validation ────────────────────────────────────

    /// A `needs` typo is caught at LOAD, not at run. The runner resolves the
    /// same graph leniently (a resume-from-step run is handed a drained
    /// prefix), so this is the only place a dangling name is an error — and it
    /// has to be, or a mistyped edge silently becomes no edge.
    #[test]
    fn a_needs_naming_no_step_fails_the_load() {
        let (_d, loader) = camp(&[(
            "typo",
            r#"
[pipeline]
name = "typo"
label = "l"

[[pipeline.steps]]
name = "build"
argv = ["true"]

[[pipeline.steps]]
name = "ship"
argv = ["true"]
needs = ["biuld"]
"#,
        )]);
        let err = loader.load("typo").expect_err("dangling needs");
        assert!(
            matches!(&err, ConfigError::InvalidDag(crate::dag::DagError::UnknownNeed { missing, .. })
                     if missing == "biuld"),
            "got {err:?}",
        );
    }

    /// A cycle can never drain, so it is a load error rather than a run that
    /// finishes instantly having executed nothing.
    #[test]
    fn a_cyclic_needs_graph_fails_the_load() {
        let (_d, loader) = camp(&[(
            "cyc",
            r#"
[pipeline]
name = "cyc"
label = "l"

[[pipeline.steps]]
name = "a"
argv = ["true"]
needs = ["b"]

[[pipeline.steps]]
name = "b"
argv = ["true"]
needs = ["a"]
"#,
        )]);
        assert!(
            matches!(
                loader.load("cyc").expect_err("cycle"),
                ConfigError::InvalidDag(crate::dag::DagError::Cycle(_))
            ),
        );
    }

    /// `[[finally]]` is unconditional always-run teardown; a `needs` there
    /// would be inert config that reads as if it scheduled something.
    #[test]
    fn a_needs_on_a_finally_step_is_rejected() {
        let (_d, loader) = camp(&[(
            "teardown",
            r#"
[pipeline]
name = "teardown"
label = "l"

[[pipeline.steps]]
name = "test"
argv = ["true"]

[[pipeline.finally]]
name = "cleanup"
argv = ["true"]
needs = ["test"]
"#,
        )]);
        let err = loader.load("teardown").expect_err("needs on finally");
        assert!(
            matches!(
                &err,
                ConfigError::InvalidStep(StepValidationError::FinallyCannotDeclareNeeds(n))
                    if n == "cleanup"
            ),
            "got {err:?}",
        );
    }

    /// `max_parallel` is a pipeline body key and parses onto the type — pinned
    /// because the loader has a separate hand-written struct that a new field
    /// is easy to add to `Pipeline` without.
    #[test]
    fn max_parallel_parses_off_the_pipeline_table() {
        let (_d, loader) = camp(&[(
            "capped",
            r#"
[pipeline]
name = "capped"
label = "l"
max_parallel = 2

[[pipeline.steps]]
name = "a"
argv = ["true"]
needs = []

[[pipeline.steps]]
name = "b"
argv = ["true"]
needs = []
"#,
        )]);
        let p = loader.load("capped").expect("loads");
        assert_eq!(p.max_parallel, Some(2));
        assert_eq!(
            crate::dag::waves(&p.steps, crate::dag::Missing::Reject).unwrap(),
            vec![vec![0, 1]],
        );
    }

    /// The whole ticket in one assertion set: a six-line file becomes a
    /// first-class catalog entry carrying the BASE's steps under its OWN
    /// identity. Contrast the shape this replaces — a one-step SubPipeline
    /// wrapper, which reports 1 step and no params to every consumer that
    /// reads what `load` returned.
    #[test]
    fn specialization_carries_the_base_body_under_its_own_identity() {
        let (_d, loader) = camp(&[(
            "desktop-local",
            r#"
[pipeline]
name = "desktop-local"
label = "Install the desktop app locally"
tags = ["build", "desktop", "local"]
alias_of = "local-install"

[pipeline.pin]
target = "desktop"
"#,
        )]);

        let p = loader.load("desktop-local").unwrap();
        assert_eq!(p.name, "desktop-local");
        assert_eq!(p.label, "Install the desktop app locally");
        assert_eq!(p.tags, vec!["build", "desktop", "local"]);
        assert_eq!(p.alias_of.as_deref(), Some("local-install"));

        // The base's body, verbatim — not a wrapper around it.
        let steps: Vec<&str> = p.steps.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(steps, vec!["Build", "Install"]);

        // A pinned param is no longer a question to ask the operator...
        let mut advertised: Vec<&str> = p.params.keys().map(String::as_str).collect();
        advertised.sort();
        assert_eq!(advertised, vec!["notify", "profile"]);
        // ...but it is still a param at run time.
        assert_eq!(p.pins.get("target").map(String::as_str), Some("desktop"));
    }

    /// A pin is the specialization's identity, not a default: substitution and
    /// `if =` gating both see it, and a run cannot talk it out of it.
    #[test]
    fn a_pin_reaches_the_run_and_refuses_to_be_overridden() {
        let (_d, loader) = camp(&[(
            "desktop-local",
            "[pipeline]\nname = \"desktop-local\"\nlabel = \"l\"\n\
             alias_of = \"local-install\"\n\n[pipeline.pin]\ntarget = \"desktop\"\n",
        )]);
        let mut p = loader.load("desktop-local").unwrap();

        // Nothing supplied: the pin lands alongside the base's declared
        // defaults, and `{{target}}` substitutes.
        let resolved = p.resolve_params(&HashMap::new()).unwrap();
        assert_eq!(resolved.get("target").map(String::as_str), Some("desktop"));
        assert_eq!(resolved.get("profile").map(String::as_str), Some("release"));
        p.apply_params(&resolved);
        assert_eq!(
            p.steps[1].argv,
            vec!["./scripts/install.sh".to_string(), "desktop".to_string()]
        );

        // The same value is a no-op — which is what re-running a recorded
        // param set does, and it must not become an error.
        let same: HashMap<String, String> =
            [("target".to_string(), "desktop".to_string())].into_iter().collect();
        assert!(loader.load("desktop-local").unwrap().resolve_params(&same).is_ok());

        // A different one is refused, loudly. Silently ignoring it would hand
        // back a run that looks like it honoured the flag and didn't.
        let other: HashMap<String, String> =
            [("target".to_string(), "cli".to_string())].into_iter().collect();
        let err = loader
            .load("desktop-local")
            .unwrap()
            .resolve_params(&other)
            .expect_err("pinned params are not overridable");
        match &err {
            ParamError::PinnedParamOverride { name, pinned, value, base, .. } => {
                assert_eq!((name.as_str(), pinned.as_str(), value.as_str()), ("target", "desktop", "cli"));
                assert_eq!(base, "local-install");
            }
            other => panic!("expected PinnedParamOverride, got {other:?}"),
        }
        // The message has to point at the escape hatch: run the base.
        assert!(err.to_string().contains("local-install"), "got: {err}");
    }

    /// The pin is checked against a REAL declaration — that is the whole
    /// advantage over hand-copying a pipeline. A base that renames a param
    /// must break its specializations at load, not leave a dead pin in a file.
    #[test]
    fn a_pin_naming_no_declared_param_is_rejected() {
        let (_d, loader) = camp(&[(
            "typo-local",
            "[pipeline]\nname = \"typo-local\"\nlabel = \"l\"\n\
             alias_of = \"local-install\"\n\n[pipeline.pin]\ntarrget = \"desktop\"\n",
        )]);
        let err = loader.load("typo-local").expect_err("tarrget is not a param");
        match &err {
            ConfigError::AliasPinUnknownParam { name, known, .. } => {
                assert_eq!(name, "tarrget");
                assert_eq!(known, &["notify", "profile", "target"]);
            }
            other => panic!("expected AliasPinUnknownParam, got {other:?}"),
        }
        // Naming the legal set is the repair hint.
        assert!(err.to_string().contains("target"), "got: {err}");
    }

    /// Same reasoning as a `default` outside `options` (see `validate_params`):
    /// caught at load, because otherwise it only fails on the runs that reach it.
    #[test]
    fn a_pin_outside_the_base_options_is_rejected() {
        let (_d, loader) = camp(&[(
            "web-local",
            "[pipeline]\nname = \"web-local\"\nlabel = \"l\"\n\
             alias_of = \"local-install\"\n\n[pipeline.pin]\ntarget = \"web\"\n",
        )]);
        let err = loader.load("web-local").expect_err("web is not a declared target");
        assert!(
            matches!(&err, ConfigError::AliasPinNotInOptions { value, .. } if value == "web"),
            "got {err:?}",
        );
        assert!(err.to_string().contains("desktop, cli, all"), "got: {err}");
    }

    /// A specialization binds a body; it does not get to also declare one,
    /// because nothing could say which set of steps runs. Every body key is
    /// reported at once — an author who wrote a whole pipeline under an
    /// `alias_of` should learn that in one pass.
    #[test]
    fn an_alias_that_also_declares_a_body_is_rejected() {
        let (_d, loader) = camp(&[(
            "hybrid",
            r#"
[pipeline]
name = "hybrid"
label = "l"
alias_of = "local-install"
concurrency_key = "cargo-target"

[[pipeline.steps]]
name = "Extra"
argv = ["true"]
"#,
        )]);
        let err = loader.load("hybrid").expect_err("alias + body");
        match &err {
            ConfigError::AliasHasBody { keys, .. } => {
                assert!(keys.contains("steps"), "got: {keys}");
                assert!(keys.contains("concurrency_key"), "got: {keys}");
            }
            other => panic!("expected AliasHasBody, got {other:?}"),
        }
    }

    /// The base name is a reference like any other and can dangle. Reported as
    /// its own error rather than a bare `NotFound`: the name the operator asked
    /// for did resolve, so "pipeline not found: local-instal" would send them
    /// looking in entirely the wrong place.
    #[test]
    fn an_alias_of_an_unknown_base_names_both() {
        let (_d, loader) = camp(&[(
            "orphan",
            "[pipeline]\nname = \"orphan\"\nlabel = \"l\"\nalias_of = \"local-instal\"\n",
        )]);
        let err = loader.load("orphan").expect_err("no such base");
        assert!(
            matches!(&err, ConfigError::AliasBaseNotFound { alias, base, .. }
                     if alias == "orphan" && base == "local-instal"),
            "got {err:?}",
        );
        let msg = err.to_string();
        assert!(msg.contains("orphan") && msg.contains("local-instal"), "got: {msg}");
    }

    /// Specializing a specialization is legitimate — each hop pins one more
    /// key — and the pins accumulate rather than shadow. The inner pin is
    /// already out of `params`, so the unknown-param check makes re-pinning it
    /// impossible by construction.
    #[test]
    fn a_specialization_of_a_specialization_accumulates_pins() {
        let (_d, loader) = camp(&[
            (
                "desktop-local",
                "[pipeline]\nname = \"desktop-local\"\nlabel = \"desktop\"\n\
                 tags = [\"build\", \"desktop\"]\nalias_of = \"local-install\"\n\n\
                 [pipeline.pin]\ntarget = \"desktop\"\n",
            ),
            (
                "desktop-local-debug",
                "[pipeline]\nname = \"desktop-local-debug\"\nlabel = \"desktop (debug)\"\n\
                 alias_of = \"desktop-local\"\n\n[pipeline.pin]\nprofile = \"dev\"\n",
            ),
        ]);

        let p = loader.load("desktop-local-debug").unwrap();
        assert_eq!(p.name, "desktop-local-debug");
        assert_eq!(p.alias_of.as_deref(), Some("desktop-local"));
        assert_eq!(p.pins.get("target").map(String::as_str), Some("desktop"));
        assert_eq!(p.pins.get("profile").map(String::as_str), Some("dev"));
        assert_eq!(
            p.params.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["notify"],
            "both pinned keys are out of the advertised schema",
        );
        // Tags fall back through the chain: a specialization of a `build`
        // pipeline is still a build, and re-declaring that in every file is
        // how tag vocabularies rot.
        assert_eq!(p.tags, vec!["build", "desktop"]);
        // Re-pinning an already-pinned key can't even be spelled.
        let resolved = p.resolve_params(&HashMap::new()).unwrap();
        assert_eq!(resolved.get("profile").map(String::as_str), Some("dev"));
    }

    /// `alias_of` is a name reference, so a chain can close on itself. Caught
    /// at load with the chain in the message rather than by blowing the stack.
    #[test]
    fn an_alias_cycle_is_rejected_with_its_chain() {
        let (_d, loader) = camp(&[
            (
                "ping",
                "[pipeline]\nname = \"ping\"\nlabel = \"l\"\nalias_of = \"pong\"\n",
            ),
            (
                "pong",
                "[pipeline]\nname = \"pong\"\nlabel = \"l\"\nalias_of = \"ping\"\n",
            ),
        ]);
        let err = loader.load("ping").expect_err("ping -> pong -> ping");
        match &err {
            ConfigError::AliasChain { chain, .. } => {
                assert_eq!(chain, &["ping", "pong", "ping"]);
            }
            other => panic!("expected AliasChain, got {other:?}"),
        }
        assert!(err.to_string().contains("ping -> pong -> ping"), "got: {err}");
    }

    /// A chain longer than `MAX_ALIAS_DEPTH` is a modelling mistake, not a
    /// recipe. Same posture as `MAX_SUB_PIPELINE_DEPTH`.
    #[test]
    fn an_alias_chain_deeper_than_the_cap_is_rejected() {
        let mut files: Vec<(String, String)> = Vec::new();
        // a0 -> a1 -> … -> a{N} -> local-install; N chosen to exceed the cap.
        let depth = MAX_ALIAS_DEPTH + 1;
        for i in 0..depth {
            let base = if i + 1 == depth {
                "local-install".to_string()
            } else {
                format!("a{}", i + 1)
            };
            files.push((
                format!("a{i}"),
                format!("[pipeline]\nname = \"a{i}\"\nlabel = \"l\"\nalias_of = \"{base}\"\n"),
            ));
        }
        let refs: Vec<(&str, &str)> = files
            .iter()
            .map(|(n, b)| (n.as_str(), b.as_str()))
            .collect();
        let (_d, loader) = camp(&refs);

        let err = loader.load("a0").expect_err("chain exceeds the cap");
        assert!(
            matches!(&err, ConfigError::AliasChain { reason, .. } if reason.contains("MAX_ALIAS_DEPTH")),
            "got {err:?}",
        );
        // Entering the same chain ONE hop later resolves: the cap is a cap on
        // nesting, not a ban on it, and this is the boundary it sits at.
        assert!(loader.load("a1").is_ok(), "a1 -> … -> local-install is exactly at the cap");
    }

    /// A specialization with nothing to say about itself describes the thing it
    /// specializes, rather than showing an empty readme in the catalog. Its own
    /// header block still wins when it has one.
    #[test]
    fn description_falls_back_through_the_alias_to_the_base() {
        let (_d, loader) = camp(&[
            (
                "quiet",
                "[pipeline]\nname = \"quiet\"\nlabel = \"l\"\nalias_of = \"local-install\"\n\n\
                 [pipeline.pin]\ntarget = \"cli\"\n",
            ),
            (
                "loud",
                "# loud — the cli, installed, with feeling.\n\n\
                 [pipeline]\nname = \"loud\"\nlabel = \"l\"\nalias_of = \"local-install\"\n\n\
                 [pipeline.pin]\ntarget = \"cli\"\n",
            ),
        ]);
        assert_eq!(
            loader.load("quiet").unwrap().description.as_deref(),
            Some("local-install — build a yah surface and install it into this machine."),
        );
        assert_eq!(
            loader.load("loud").unwrap().description.as_deref(),
            Some("loud — the cli, installed, with feeling."),
        );
    }

    /// R751-F3: the template ↔ concrete cut, over real files rather than over
    /// hand-built `Pipeline` literals — the FE's own copy of this derivation
    /// passed its unit tests and was still wrong on the wire path (R751-B1),
    /// so the one that replaces it is tested through the loader.
    #[test]
    fn classification_follows_what_a_run_still_has_to_be_told() {
        let (_d, loader) = camp(&[
            (
                // Fully binds the base's only unbound required param.
                "desktop-local",
                "[pipeline]\nname = \"desktop-local\"\nlabel = \"l\"\n\
                 alias_of = \"local-install\"\n\n[pipeline.pin]\ntarget = \"desktop\"\n",
            ),
            (
                // Pins something, but leaves `target` open. A PARTIAL
                // specialization is still a template — you cannot instantiate
                // it — which is the whole reason template wins the precedence.
                "debug-local",
                "[pipeline]\nname = \"debug-local\"\nlabel = \"l\"\n\
                 alias_of = \"local-install\"\n\n[pipeline.pin]\nprofile = \"dev\"\n",
            ),
        ]);

        let base = loader.load("local-install").unwrap();
        assert_eq!(base.classification(), crate::types::PipelineClass::Template);
        assert_eq!(base.unbound_params(), vec!["target"]);

        let full = loader.load("desktop-local").unwrap();
        assert_eq!(full.classification(), crate::types::PipelineClass::Specialization);
        assert!(full.unbound_params().is_empty(), "the pin bound the last one");

        let partial = loader.load("debug-local").unwrap();
        assert_eq!(
            partial.classification(),
            crate::types::PipelineClass::Template,
            "a partial specialization still cannot be run with no input",
        );
        assert_eq!(partial.unbound_params(), vec!["target"]);
        assert_eq!(
            partial.alias_of.as_deref(),
            Some("local-install"),
            "and it still says where it came from, so nothing is lost by the precedence",
        );
    }

    /// A `required` param carrying a `default` is BOUND — that is the whole
    /// point of `default = ""` (see `ParamDef::default`), and reading `required`
    /// alone would file half the catalog under templates.
    #[test]
    fn a_required_param_with_a_default_is_not_unbound() {
        let (_d, loader) = camp(&[]);
        let p = loader.load("local-install").unwrap();
        // `profile` is required=false with a default; `notify` defaults to "".
        assert_eq!(p.unbound_params(), vec!["target"]);
        assert_eq!(
            loader
                .load_from_str(
                    "[pipeline]\nname = \"n\"\nlabel = \"l\"\n\n\
                     [pipeline.params]\nfeatures = { required = true, default = \"\" }\n"
                )
                .unwrap()
                .classification(),
            crate::types::PipelineClass::Concrete,
        );
    }

    /// R751-F2 verify: a specialization survives load → serialize → load. The
    /// serialized form is what `qed eject` writes and what the wire carries, so
    /// `alias_of` / `pins` dropping out of it would silently un-specialize a
    /// pipeline somewhere downstream.
    #[test]
    fn a_specialization_round_trips_through_serde() {
        let (_d, loader) = camp(&[(
            "desktop-local",
            "[pipeline]\nname = \"desktop-local\"\nlabel = \"l\"\ntags = [\"desktop\"]\n\
             alias_of = \"local-install\"\n\n[pipeline.pin]\ntarget = \"desktop\"\n",
        )]);
        let p = loader.load("desktop-local").unwrap();
        let json = serde_json::to_string(&p).unwrap();
        let back: Pipeline = serde_json::from_str(&json).unwrap();
        assert_eq!(back.alias_of, p.alias_of);
        assert_eq!(back.pins, p.pins);
        assert_eq!(back.params.len(), p.params.len());
        assert_eq!(back.steps.len(), p.steps.len());

        // An ordinary pipeline must not grow the keys: `skip_serializing_if`
        // keeps `qed eject`'s output free of `alias_of = null` / `pins = {}`.
        let plain = serde_json::to_string(&loader.load("local-install").unwrap()).unwrap();
        assert!(!plain.contains("alias_of"), "got: {plain}");
        assert!(!plain.contains("pins"), "got: {plain}");
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
