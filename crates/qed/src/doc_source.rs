//! Markdown as a QED source (R717-F4, W296) — a `.md` whose fenced code blocks
//! are runnable, subject-keyed cells.
//!
//! This mirrors [`crate::import`] rather than inventing anything: an executable
//! doc is an **import source** QED expands into its own native subgraph, exactly
//! as W224 settled for a GitHub Actions workflow. The doctrine carries over
//! verbatim — *virtual expansion, recomputed at plan time, never persisted, zero
//! drift by construction*. The markdown is canonical; nothing about the derived
//! pipeline is ever written back into the `.md`.
//!
//! That last clause is the decision that separates this from Jupyter, and it is
//! not close. `.ipynb` stores outputs in the file, which is why every notebook
//! repo is a merge-conflict farm. yah runs many sessions against one working
//! tree; a doc that dirtied itself on every run would put a git diff between an
//! operator and a status badge. Run state goes to `.yah/jit/qed/`, keyed by
//! [`CellRef`](crate::types::CellRef).
//!
//! ## Cells live in the fence info string
//!
//! ````text
//! ```bash cell=probe-identity assert host={{node}}
//! test "$(whoami)@$(hostname)" = "yah@{{node}}"
//! sudo -n true
//! ```
//! ````
//!
//! **`cell=<id>` is what makes a fence a cell at all.** A fence without it is an
//! ordinary code block and renders exactly as it does today — which is most of
//! them, in every doc in the tree.
//!
//! Why the info string and not a custom container or an HTML comment block:
//! these files are markdown in a git repo, read on GitHub, in editors, and by
//! agents with `Read`. An info-string attribute **degrades to a perfectly
//! ordinary fenced code block everywhere that does not know about it**. A custom
//! syntax does not. (Settled in W296; not open.)
//!
//! | Attribute | Meaning |
//! |---|---|
//! | `cell=<id>` | Stable, author-assigned identity. **Never positional** — these docs get reordered constantly. |
//! | `assert` | Exit code is the verdict. Without it a cell is `show`: it runs, its output is for a human, and it carries no pass/fail. |
//! | `capture=<k,…>` | Lowers to the existing [`OutputDecl`] + `$YAH_OUTPUTS` path. |
//! | `inputs=<path,…>` | R717-T1 freshness pin. |
//! | `needs=<cell,…>` | A *checked* dependency — see below. |
//! | `if=<expr>` | The existing [`QedStep::if_cond`]. |
//! | `secret` | R717-T2 capture opt-out. |
//! | `host=<param\|name>` | R717-T5 — resolve `[connect].ssh` from `.yah/infra/machines/<name>.toml` and wrap the body in `ssh`. |
//! | `manual` | R717-T11 — lowers to [`StepKind::Manual`] (R622/W282). The cell is a **human's** gate; see below. |
//!
//! ## A manual cell is a human's gate
//!
//! `manual` turns the cell into W282's [`StepKind::Manual`]: the run parks, a
//! form lands in the AnswerQueue, and the step advances only when a person says
//! so (or when its `advance` condition starts passing on its own). The cell body
//! carries the human's half in a **leading comment block**:
//!
//! ````text
//! ```bash cell=kek-mint manual needs=kek-camp-exists if=!cells.kek-camp-exists.ok
//! # Mint the camp KEK. Once, ever — a second mint orphans every secret
//! # sealed under the first.
//! #
//! # advance: yah cloud secret kek fingerprint
//! # checklist: Confirmed no camp KEK exists
//! yah cloud secret kek init
//! ```
//! ````
//!
//! Everything above the first non-comment line is the human's brief: bare `#`
//! prose becomes [`ManualConfig::prompt`], `# advance:` becomes
//! [`ManualConfig::advance`], and each `# checklist:` an advisory checkbox.
//! What is left below is [`ManualConfig::terminal`] — commands the form
//! **prefills but never runs**. A cell that is all comment (W257's BIOS block:
//! Secure Boot, USB-first boot order, restore-on-AC-loss) is the honest case
//! where there is nothing to prefill and nothing to verify; `advance` is
//! optional precisely for it, and a checklist that remembers whether you did it
//! is the whole contribution there.
//!
//! **The rule that makes this worth having:** a manual cell declares a *human*
//! actor, and an agent-initiated run parks on it rather than answering it.
//! Agents can otherwise resolve forms — that is the standing approval path
//! (`subagent.answer_ask`) — so without this an agent driving W257 sails
//! straight through "go set restore-on-AC-power-loss in the BIOS". The
//! enforcement is structural, not conventional: choosing [`StepKind::Manual`]
//! here means the daemon's gate mints the form with `prefer = "human"`, and the
//! daemon refuses an agent's submission of such a form outright.
//!
//! `assert`, `capture=` and `host=` are rejected on a manual cell — a step with
//! no argv has no exit code to make a verdict of, no `$YAH_OUTPUTS` to read, and
//! nothing to wrap in `ssh` (put the ssh in `advance:`, which is where the
//! router-reservation cell wants it anyway).
//!
//! ## `needs=` is a constraint, not a scheduler
//!
//! Document order **is** execution order, and `needs=` is checked against it: a
//! cell that names a later cell is a parse error. It does not reorder anything.
//!
//! This is deliberate and worth not undoing. A QED pipeline is a sequence, so a
//! topological reorder inside the parser would make the doc's visible cell order
//! differ from what actually runs — and for a runbook a human reads top to
//! bottom, that is precisely the wrong outcome. What `needs=` earns instead is
//! (a) an enforced dependency the author can state, and (b) the prerequisite set
//! for a single-cell run (`--cell probe-identity`, R717-T7), which is the case
//! where the edges genuinely have to be traversed.
//!
//! ## Doc-level config
//!
//! One leading TOML fence tagged `notebook=<name>` carries params, binds, and
//! the pipeline knobs. It must come before any cell.
//!
//! ````text
//! ```toml notebook=node-onboard
//! [params]
//! node = { required = true, description = "machine name, e.g. us-west-003" }
//!
//! [[bind]]
//! file = ".yah/infra/machines/{{node}}.toml"
//! path = "registration.hostkey_fingerprint"
//! from = "identity-probe.outputs.fingerprint"
//! ```
//! ````
//!
//! Two defaults differ from a hand-written `.yah/qed/*.toml`, both settled in
//! R717-S13 and both overridable from this fence:
//!
//! - **`workspace = "live"`.** The [`WorkspaceMode`] default is `Checkout`,
//!   which switches to the run's ref and bails if the tree is dirty. On a shared
//!   tree that makes a runbook unrunnable most of the time, and it is the wrong
//!   semantics anyway: a runbook asserts things about the operator's *actual*
//!   tree and the live world (a box, a KEK, a docker daemon), not about bytes at
//!   some other ref.
//! - **`concurrency_key = "@doc:<doc>#<param_fingerprint>"`.** Two operators
//!   bringing up two boxes at once is the *expected* case; the camp-global
//!   `@camp` default (R719-F1) would serialize them, and each other's unrelated
//!   recipes too. This is not a carve-out from that rule but an application of
//!   it: an unkeyed *build* recipe's contended resource is the camp tree, while a
//!   doc cell's is **the remote subject** — which the fingerprint names exactly.
//!   So the doc source is supplying the correct key, not forgetting one.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::types::{
    ManualConfig, OnFail, OutputDecl, ParamDef, Pipeline, QedStep, StepKind, WorkspaceMode,
};

/// Why a `.md` could not be read as a QED source. Every variant names the cell
/// (or the attribute) at fault — an author fixing a runbook should not have to
/// bisect it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DocSourceError {
    #[error("doc has no `notebook=<name>` TOML fence — a runnable doc needs one to name the pipeline and declare its params")]
    NoNotebookFence,
    #[error(
        "the `notebook={0}` fence appears after cell `{1}` — doc-level config declares the params \
         that cells substitute, so it has to come first"
    )]
    NotebookFenceNotFirst(String, String),
    #[error("doc declares more than one `notebook=` fence (`{0}` and `{1}`)")]
    DuplicateNotebookFence(String, String),
    #[error("`notebook={0}` fence is not valid TOML: {1}")]
    NotebookFenceParse(String, String),
    #[error("duplicate cell id `{0}` — cell ids are the stable key run state is filed under, so two cells cannot share one")]
    DuplicateCellId(String),
    #[error("cell `{0}`: `needs={1}` names no cell in this doc")]
    NeedsUnknownCell(String, String),
    #[error(
        "cell `{0}`: `needs={1}` names a cell that appears LATER in the document. Document order \
         is execution order — `needs=` is checked against it, it does not reorder"
    )]
    NeedsForwardReference(String, String),
    #[error(
        "cell `{0}`: declares `capture={1}` but its body never writes to $YAH_OUTPUTS, so the \
         output would always be empty. Append `echo {1}=... >> \"$YAH_OUTPUTS\"`, or drop capture="
    )]
    CaptureWithoutOutputsWrite(String, String),
    #[error(
        "cell `{0}`: `secret` and `capture={1}` contradict each other — a secret cell's \
         $YAH_OUTPUTS is dropped unread, so the captured value would always be empty. A value \
         you need OUT of a cell is not secret: split it into its own cell"
    )]
    SecretWithCapture(String, String),
    #[error("cell `{0}`: unknown attribute `{1}`")]
    UnknownAttribute(String, String),
    #[error("cell `{0}`: attribute `{1}` needs a value (`{1}=...`)")]
    AttributeNeedsValue(String, String),
    #[error("cell `{0}`: attribute `{1}` is a flag and takes no value")]
    AttributeTakesNoValue(String, String),
    #[error(
        "cell `{0}`: a `manual` cell needs a prompt — the leading `# ` comment block of the body \
         says what the human must accomplish. Without it the form is a bare Continue button, \
         which asserts nothing"
    )]
    ManualNeedsPrompt(String),
    #[error("cell `{0}`: `# advance:` is empty — give it a command that proves the work was done, or drop the line (a manual cell may have no advance at all)")]
    ManualBlankAdvance(String),
    #[error("cell `{0}`: more than one `# advance:` line — a manual cell has exactly one condition (`{1}` then `{2}`)")]
    ManualDuplicateAdvance(String, String, String),
    #[error(
        "cell `{0}`: `manual` and `assert` contradict each other — a manual step runs no argv, so \
         there is no exit code to make a verdict of. What proves a manual cell was done is its \
         `# advance:` line"
    )]
    ManualWithAssert(String),
    #[error(
        "cell `{0}`: `manual` and `capture={1}` contradict each other — a manual step runs no \
         argv, so nothing ever writes $YAH_OUTPUTS. Capture the value in a following non-manual \
         cell instead"
    )]
    ManualWithCapture(String, String),
    #[error(
        "cell `{0}`: `manual` and `host={1}` contradict each other — a manual step is a person at \
         a keyboard on the qed host, not a remote command. Put the ssh in the cell's `# advance:` \
         line, which is where a remote proof belongs"
    )]
    ManualWithHost(String, String),
    #[error("cell `{0}`: `host={1}` resolved to `{2}`, but there is no machine file at `{3}`")]
    UnknownHost(String, String, String, String),
    #[error("cell `{0}`: machine file `{1}` is not valid TOML: {2}")]
    MachineFileParse(String, String, String),
    #[error(
        "cell `{0}`: machine file `{1}` declares no `[connect].ssh`, so there is no address to \
         reach it at"
    )]
    MachineFileNoSsh(String, String),
    #[error("cell `{0}`: `host={1}` still contains an unsubstituted `{{{{...}}}}` placeholder after params were applied — declare it in the notebook fence's [params]")]
    HostUnresolvedParam(String, String),
    #[error(
        "param `{0}`: `options_from={1}` matched no files under the camp root. A glob that names \
         nothing is an authoring error, not an empty dropdown — a typo'd glob must not be \
         indistinguishable from a correct one"
    )]
    OptionsFromMatchedNothing(String, String),
    #[error("param `{0}`: `options_from={1}` is not a `<dir>/<pattern>` glob — it needs a directory to enumerate")]
    OptionsFromNotAGlob(String, String),
    #[error("{0}")]
    OptionsCmd(String),
}

/// One runnable cell, parsed but not yet lowered.
///
/// Split from the lowered [`QedStep`] on purpose: `host=` resolves through the
/// run's params, so lowering cannot happen at parse time (R717-T5). It also
/// keeps the doc-level facts a `Pipeline` has nowhere to put — the cell id, the
/// `needs` edges, `assert`-vs-`show` — addressable by R717-F6/T7 without
/// re-parsing the markdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocCell {
    /// `cell=<id>`, author-assigned. The key run state is filed under.
    pub id: String,
    /// The fence's info-string language token (`bash`, `sh`, …).
    pub lang: String,
    /// The fence body, verbatim.
    pub body: String,
    /// `assert` present ⇒ the exit code is the verdict. Absent ⇒ `show`: the
    /// cell runs and its output is for a human, but it renders no pass/fail.
    pub assert: bool,
    pub capture: Vec<String>,
    pub inputs: Vec<PathBuf>,
    pub needs: Vec<String>,
    pub if_cond: Option<String>,
    pub secret: bool,
    /// `host=<param|name>`, **unsubstituted** — it may be `{{node}}`, which only
    /// the run's params can resolve.
    pub host: Option<String>,
    /// `manual` — the human's half, lifted out of the body's leading comment
    /// block (R717-T11). `Some` ⇒ this cell lowers to [`StepKind::Manual`] and
    /// **no agent may answer it**; see the module docs.
    pub manual: Option<ManualCell>,
}

/// A `manual` cell's brief, parsed out of the body's leading comment block and
/// still **unsubstituted** (params are applied at lowering, in one pass with
/// the rest of the cell).
///
/// Field-for-field a [`ManualConfig`] minus `advance_poll_secs`, which a doc
/// has no reason to override — a park spans human time, so the 5s default is
/// as good as any number an author would pick.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ManualCell {
    /// The bare `# ` prose: what the human must accomplish.
    pub prompt: String,
    /// Everything below the comment block — commands the form prefills into
    /// terminal tiles and **never runs**.
    pub terminal: Vec<String>,
    /// `# advance: <cmd>`, the condition that proves they did it. `None` for
    /// the irreducible cases (a person at a box with no out-of-band access);
    /// then the human's word is the only door.
    pub advance: Option<String>,
    /// `# checklist: <item>` lines. Advisory — they gate nothing.
    pub checklist: Vec<String>,
}

/// A markdown document read as a QED source: doc-level config plus its cells,
/// before params are known.
///
/// No `PartialEq`: [`NotebookConfig`] carries `ParamDef` / `BindSpec`, neither of
/// which derives it upstream, and forcing those derives onto other crates to
/// make a test assertion convenient is the wrong trade.
#[derive(Debug, Clone)]
pub struct DocSource {
    /// Camp-root-relative path of the source document — the `doc` half of a
    /// [`CellRef`](crate::types::CellRef).
    pub doc: String,
    /// `notebook=<name>`; becomes the pipeline name.
    pub name: String,
    pub config: NotebookConfig,
    pub cells: Vec<DocCell>,
}

/// The `notebook=` fence's body. Field-for-field a subset of the `[pipeline]`
/// table, using the **same types**, so a doc's params and binds validate through
/// exactly the paths a `.yah/qed/*.toml` does rather than a parallel copy.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotebookConfig {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub params: HashMap<String, ParamDef>,
    /// Overrides the doc default (`@doc:<doc>#<fingerprint>`). See the module
    /// docs for why that default is not `@camp`.
    #[serde(default)]
    pub concurrency_key: Option<String>,
    /// Overrides the doc default (`live`). See the module docs.
    #[serde(default)]
    pub workspace: Option<WorkspaceMode>,
    /// Root-level `[[bind]]` tables, same placement and same type as a pipeline
    /// TOML's.
    #[serde(default, rename = "bind")]
    pub binds: Vec<manifest_bind::BindSpec>,
    #[serde(default)]
    pub on_change: Vec<manifest_bind::OnChangeHook>,
}

/// Parse a markdown document into a [`DocSource`].
///
/// `doc_rel` is the camp-root-relative path, carried through to
/// [`CellRef::doc`](crate::types::CellRef::doc); it is not read from disk here
/// (the caller owns the bytes, same shape as [`crate::import::content_hash`]).
pub fn parse_doc(doc_rel: &str, markdown: &str) -> Result<DocSource, DocSourceError> {
    let fences = scan_fences(markdown);

    let mut notebook: Option<(String, NotebookConfig)> = None;
    let mut cells: Vec<DocCell> = Vec::new();

    for fence in &fences {
        let (lang, attrs) = split_info_string(&fence.info);

        if let Some(name) = attrs.iter().find(|a| a.key == "notebook") {
            let name = name.value.clone().unwrap_or_default();
            if let Some((first, _)) = &notebook {
                return Err(DocSourceError::DuplicateNotebookFence(
                    first.clone(),
                    name,
                ));
            }
            if let Some(cell) = cells.first() {
                return Err(DocSourceError::NotebookFenceNotFirst(name, cell.id.clone()));
            }
            let cfg: NotebookConfig = toml::from_str(&fence.body)
                .map_err(|e| DocSourceError::NotebookFenceParse(name.clone(), e.to_string()))?;
            notebook = Some((name, cfg));
            continue;
        }

        let Some(id_attr) = attrs.iter().find(|a| a.key == "cell") else {
            // No `cell=` ⇒ an ordinary fenced code block. Today's rendering,
            // unchanged. This is the common case in every doc in the tree.
            continue;
        };
        let id = id_attr.value.clone().unwrap_or_default();
        if id.is_empty() {
            return Err(DocSourceError::AttributeNeedsValue(
                "<unnamed>".to_string(),
                "cell".to_string(),
            ));
        }
        if cells.iter().any(|c| c.id == id) {
            return Err(DocSourceError::DuplicateCellId(id));
        }
        cells.push(build_cell(id, lang.to_string(), fence.body.clone(), &attrs)?);
    }

    let Some((name, config)) = notebook else {
        return Err(DocSourceError::NoNotebookFence);
    };

    validate_cells(&cells)?;

    Ok(DocSource {
        doc: doc_rel.to_string(),
        name,
        config,
        cells,
    })
}

impl DocSource {
    /// Find a cell by its `cell=` id.
    pub fn cell(&self, id: &str) -> Option<&DocCell> {
        self.cells.iter().find(|c| c.id == id)
    }

    /// This doc's params with every [`ParamDef::options_from`] glob resolved
    /// into [`ParamDef::options`] — R717-T10, W296 §Q3.
    ///
    /// The subject selector calls this rather than reading `config.params`
    /// directly, so an enumerable param arrives as the same closed set a
    /// hand-written `options = [...]` produces and nothing downstream has to
    /// know which spelling the author used.
    pub fn resolved_params(
        &self,
        camp_root: &Path,
    ) -> Result<HashMap<String, ParamDef>, DocSourceError> {
        let mut out = self.config.params.clone();
        for (name, def) in out.iter_mut() {
            resolve_options_from(camp_root, name, def)?;
            crate::config::resolve_options_cmd(camp_root, name, def)
                .map_err(DocSourceError::OptionsCmd)?;
        }
        Ok(out)
    }

    /// The transitive `needs=` closure of `id`, in document order, including
    /// `id` itself. This is what a single-cell run has to execute — the case
    /// `needs=` exists for (R717-T7's `--cell`).
    ///
    /// Total by construction: [`parse_doc`] has already rejected unknown and
    /// forward references, so the edges only ever point backwards and the walk
    /// cannot cycle or dangle.
    pub fn closure(&self, id: &str) -> Vec<&DocCell> {
        let mut wanted: Vec<&str> = vec![id];
        let mut i = 0;
        while i < wanted.len() {
            if let Some(cell) = self.cell(wanted[i]) {
                for need in &cell.needs {
                    if !wanted.contains(&need.as_str()) {
                        wanted.push(need);
                    }
                }
            }
            i += 1;
        }
        self.cells
            .iter()
            .filter(|c| wanted.contains(&c.id.as_str()))
            .collect()
    }

    /// Lower this doc into a runnable [`Pipeline`].
    ///
    /// `params` must be the **resolved** map ([`Pipeline::resolve_params`]
    /// output, defaults filled in): `host={{node}}` is substituted here, and the
    /// default `concurrency_key` is keyed off the same fingerprint the run's
    /// [`CellRef`](crate::types::CellRef) will carry. Lowering after resolution
    /// rather than before is what makes those two agree.
    ///
    /// `camp_root` is read for `host=` cells only — to resolve
    /// `.yah/infra/machines/<name>.toml`.
    ///
    /// `only` restricts the lowering to a cell id and its `needs=` closure;
    /// `None` lowers the whole doc in document order.
    pub fn lower(
        &self,
        camp_root: &Path,
        params: &HashMap<String, String>,
        only: Option<&str>,
    ) -> Result<Pipeline, DocSourceError> {
        let selected: Vec<&DocCell> = match only {
            Some(id) => self.closure(id),
            None => self.cells.iter().collect(),
        };
        let steps = selected
            .iter()
            .map(|cell| self.lower_cell(cell, camp_root, params))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Pipeline {
            allow_late_operator_block: false,
            name: self.name.clone(),
            label: self
                .config
                .label
                .clone()
                .unwrap_or_else(|| self.name.clone()),
            description: self.config.description.clone(),
            tags: self.config.tags.clone(),
            steps,
            params: self.config.params.clone(),
            on_success: Vec::new(),
            on_fail: Vec::new(),
            triggers: Vec::new(),
            // R717-S13 Q4 — per-SUBJECT, not per-camp. See the module docs.
            concurrency_key: Some(
                self.config
                    .concurrency_key
                    .clone()
                    .unwrap_or_else(|| self.default_concurrency_key(params)),
            ),
            max_parallel: None,
            environment: Default::default(),
            // R717-S13 Q1 — `live`, not the `Checkout` default. See the module docs.
            workspace: self.config.workspace.unwrap_or(WorkspaceMode::Live),
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: self.config.binds.clone(),
            on_change: self.config.on_change.clone(),
            alias_of: None,
            pins: Default::default(),
            finally: Vec::new(),
            participants: None,
        })
    }

    /// `@doc:<doc>#<param_fingerprint>` — the per-subject lane a doc run holds.
    pub fn default_concurrency_key(&self, params: &HashMap<String, String>) -> String {
        format!(
            "@doc:{}#{}",
            self.doc,
            crate::types::param_fingerprint(params)
        )
    }

    fn lower_cell(
        &self,
        cell: &DocCell,
        camp_root: &Path,
        params: &HashMap<String, String>,
    ) -> Result<QedStep, DocSourceError> {
        // R717-T11: a `manual` cell is not a command at all — it lowers to
        // W282's human gate, whose whole contract is that the runner parks
        // instead of executing. `argv` MUST stay empty (`validate()` rejects a
        // manual step that carries one), so this returns before the `bash -c`
        // wrapping below rather than sharing it.
        if let Some(m) = &cell.manual {
            return Ok(QedStep {
                name: cell.id.clone(),
                argv: Vec::new(),
                kind: StepKind::Manual,
                manual: Some(ManualConfig {
                    prompt: substitute_params(&m.prompt, params),
                    terminal: m
                        .terminal
                        .iter()
                        .map(|c| substitute_params(c, params))
                        .collect(),
                    advance: m.advance.as_ref().map(|a| substitute_params(a, params)),
                    checklist: m
                        .checklist
                        .iter()
                        .map(|c| substitute_params(c, params))
                        .collect(),
                    advance_poll_secs: crate::types::default_manual_advance_poll_secs(),
                    // A runbook cell has no `audience` attribute to lower, so
                    // it takes the type's default (agent) like any TOML step
                    // that omits the key.
                    audience: Default::default(),
                }),
                // Always `Abort`, and deliberately not the `assert`/`show`
                // choice the other cells make: a manual step only "fails" when
                // the human declined it or its `advance` could not be
                // satisfied, and continuing past a gate a person just refused
                // is not a thing a runbook should be able to express. (`assert`
                // is rejected on a manual cell for the same reason.)
                on_fail: OnFail::Abort,
                inputs: cell.inputs.clone(),
                secret: cell.secret,
                if_cond: cell.if_cond.clone(),
                ..Default::default()
            });
        }

        // `set -eo pipefail` so a multi-line assert cell fails on the line that
        // actually failed rather than on whatever the last line happened to
        // return — the same prologue `crate::transform` gives an imported GHA
        // `run:` block, and for the same reason.
        //
        // Params are substituted HERE rather than left to `Pipeline::apply_params`
        // so the body and the `host=` target resolve in one pass against one map.
        // Splitting them would let a `host=` cell address `us-west-003` while its
        // body still said `{{node}}` — the exact class of mismatch this whole
        // ticket is about.
        let script = format!("set -eo pipefail\n{}", substitute_params(&cell.body, params));
        let argv = match &cell.host {
            None => vec!["bash".to_string(), "-c".to_string(), script],
            Some(host) => ssh_argv(cell, host, &script, camp_root, params)?,
        };

        Ok(QedStep {
            name: cell.id.clone(),
            argv,
            kind: StepKind::Subprocess,
            // `assert` ⇒ the exit code is the verdict, i.e. the default abort.
            // `show` ⇒ the cell runs for its output and carries no verdict, so a
            // non-zero exit must not stop the doc.
            on_fail: if cell.assert {
                OnFail::Abort
            } else {
                OnFail::Continue
            },
            outputs: cell
                .capture
                .iter()
                .map(|name| OutputDecl {
                    name: name.clone(),
                    description: None,
                    kind: manifest_bind::ValueType::String,
                    validate: None,
                })
                .collect(),
            inputs: cell.inputs.clone(),
            secret: cell.secret,
            if_cond: cell.if_cond.clone(),
            ..Default::default()
        })
    }
}

/// R717-T5 — resolve `host=` and wrap the cell body in `ssh`.
///
/// **This lowers to a plain [`StepKind::Subprocess`] step.** No new step kind, no
/// runner mechanism, no remote-execution path: the resulting argv is the `ssh`
/// line an operator would type, and everything downstream treats it as the
/// ordinary local subprocess it is.
///
/// ## The two cwds, and which one QED owns (R717-S13 Q1)
///
/// The **local** cwd is the run's positioned workspace — that is where the `ssh`
/// binary is invoked and where any local path in the argv resolves. The **remote**
/// cwd is whatever the SSH login lands in (the remote user's home), because
/// `.yah/infra/machines/<name>.toml` records a *connection*, not a remote
/// workspace. A cell that needs a particular remote directory writes an explicit
/// `cd` in its body.
///
/// Do **not** add a `remote_cwd=` attribute for this. It would be a second
/// positioning mechanism against a tree QED neither owns nor versions, and it
/// would drift from the local one silently.
///
/// ## Two deliberate additions over the hand-typed line
///
/// - `-o BatchMode=yes`, so a cell whose key is not loaded **fails** instead of
///   parking a whole pipeline on an invisible password prompt.
/// - `-i <identity>` when the machine file declares `[connect].identity`
///   (`~` expanded here, since `ssh` does not expand it in an argv element).
///   Absent, the user's `~/.ssh/config` decides — the same shape
///   `crates/yah/rpc-ssh`'s optional `key_path` already uses.
fn ssh_argv(
    cell: &DocCell,
    host: &str,
    script: &str,
    camp_root: &Path,
    params: &HashMap<String, String>,
) -> Result<Vec<String>, DocSourceError> {
    let resolved = substitute_params(host, params);
    if resolved.contains("{{") {
        return Err(DocSourceError::HostUnresolvedParam(
            cell.id.clone(),
            host.to_string(),
        ));
    }

    let rel = format!(".yah/infra/machines/{resolved}.toml");
    let path = camp_root.join(&rel);
    let text = std::fs::read_to_string(&path).map_err(|_| {
        DocSourceError::UnknownHost(cell.id.clone(), host.to_string(), resolved.clone(), rel.clone())
    })?;
    let machine: MachineFile = toml::from_str(&text)
        .map_err(|e| DocSourceError::MachineFileParse(cell.id.clone(), rel.clone(), e.to_string()))?;
    let connect = machine
        .connect
        .filter(|c| !c.ssh.trim().is_empty())
        .ok_or_else(|| DocSourceError::MachineFileNoSsh(cell.id.clone(), rel.clone()))?;

    let mut argv = vec![
        "ssh".to_string(),
        "-o".to_string(),
        "BatchMode=yes".to_string(),
    ];
    if let Some(identity) = connect.identity.as_deref() {
        argv.push("-i".to_string());
        argv.push(expand_tilde(identity));
    }
    argv.push(connect.ssh.clone());
    // The body goes across as ONE argument, so the remote shell sees the script
    // verbatim; splitting it would let the local shell re-tokenize the operator's
    // quoting.
    argv.push(script.to_string());
    Ok(argv)
}

/// The slice of `.yah/infra/machines/<name>.toml` a `host=` cell reads. Only
/// `[connect]` — everything else in those files (mesh tags, taints, allocatable)
/// is fleet-placement config a runbook cell has no business interpreting.
#[derive(Debug, serde::Deserialize)]
struct MachineFile {
    connect: Option<MachineConnect>,
}

#[derive(Debug, serde::Deserialize)]
struct MachineConnect {
    /// `user@host`, as every machine file in this camp spells it.
    ssh: String,
    /// Optional identity file. Not declared by any machine file today; present
    /// so a camp whose fleet key is not in `~/.ssh/config` can name it without
    /// hardcoding a yah-specific path into this crate (which ships standalone).
    #[serde(default)]
    identity: Option<String>,
}

fn expand_tilde(path: &str) -> String {
    match path.strip_prefix("~/") {
        Some(rest) => match std::env::var("HOME") {
            Ok(home) => format!("{home}/{rest}"),
            Err(_) => path.to_string(),
        },
        None => path.to_string(),
    }
}

/// `{{key}}` substitution, matching `Pipeline::apply_params` semantics: an
/// unknown placeholder is left alone (and then rejected by the caller, since a
/// literal `{{node}}` in an ssh target could only ever fail).
fn substitute_params(input: &str, params: &HashMap<String, String>) -> String {
    let mut out = input.to_string();
    for (key, value) in params {
        out = out.replace(&format!("{{{{{key}}}}}"), value);
    }
    out
}

// ---------------------------------------------------------------------------
// Fence scanning + info-string parsing
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Fence {
    info: String,
    body: String,
}

/// Scan CommonMark backtick fences. A fence opens on a run of three or more
/// backticks and closes on a run of **at least as many** — which is not
/// pedantry here: W296 itself wraps its cell examples in four-backtick fences,
/// so a scanner that closed on any ``` would read those examples as live cells.
fn scan_fences(markdown: &str) -> Vec<Fence> {
    let mut out = Vec::new();
    let mut lines = markdown.lines().peekable();
    while let Some(line) = lines.next() {
        let ticks = line.chars().take_while(|c| *c == '`').count();
        if ticks < 3 {
            continue;
        }
        let info = line[ticks..].trim().to_string();
        let mut body = String::new();
        for inner in lines.by_ref() {
            let closing = inner.chars().take_while(|c| *c == '`').count();
            if closing >= ticks && inner[closing..].trim().is_empty() {
                break;
            }
            body.push_str(inner);
            body.push('\n');
        }
        out.push(Fence { info, body });
    }
    out
}

#[derive(Debug, PartialEq, Eq)]
struct Attr {
    key: String,
    value: Option<String>,
}

/// Split an info string into its language token and attributes.
///
/// Values may be double-quoted, which `if=` needs: `if="params.variant == 'full'"`
/// has spaces in it and a naive whitespace split would shred it.
fn split_info_string(info: &str) -> (&str, Vec<Attr>) {
    let mut tokens = Vec::new();
    let bytes: Vec<char> = info.chars().collect();
    let mut i = 0;
    let mut current = String::new();
    let mut in_quotes = false;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            '"' => in_quotes = !in_quotes,
            c if c.is_whitespace() && !in_quotes => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
        i += 1;
    }
    if !current.is_empty() {
        tokens.push(current);
    }

    let lang_len = info.find(char::is_whitespace).unwrap_or(info.len());
    let lang = &info[..lang_len];

    let attrs = tokens
        .into_iter()
        .skip(1)
        .map(|t| match t.split_once('=') {
            Some((k, v)) => Attr {
                key: k.to_string(),
                value: Some(v.to_string()),
            },
            None => Attr {
                key: t,
                value: None,
            },
        })
        .collect();
    (lang, attrs)
}

fn build_cell(
    id: String,
    lang: String,
    body: String,
    attrs: &[Attr],
) -> Result<DocCell, DocSourceError> {
    let mut cell = DocCell {
        id: id.clone(),
        lang,
        body,
        assert: false,
        capture: Vec::new(),
        inputs: Vec::new(),
        needs: Vec::new(),
        if_cond: None,
        secret: false,
        host: None,
        manual: None,
    };
    let mut is_manual = false;

    let need_value = |a: &Attr| -> Result<String, DocSourceError> {
        a.value
            .clone()
            .ok_or_else(|| DocSourceError::AttributeNeedsValue(id.clone(), a.key.clone()))
    };
    let no_value = |a: &Attr| -> Result<(), DocSourceError> {
        match a.value {
            None => Ok(()),
            Some(_) => Err(DocSourceError::AttributeTakesNoValue(
                id.clone(),
                a.key.clone(),
            )),
        }
    };

    for attr in attrs {
        match attr.key.as_str() {
            "cell" => {}
            "assert" => {
                no_value(attr)?;
                cell.assert = true;
            }
            "secret" => {
                no_value(attr)?;
                cell.secret = true;
            }
            "capture" => cell.capture = split_list(&need_value(attr)?),
            "inputs" => cell.inputs = split_list(&need_value(attr)?).into_iter().map(PathBuf::from).collect(),
            "needs" => cell.needs = split_list(&need_value(attr)?),
            "if" => cell.if_cond = Some(need_value(attr)?),
            "host" => cell.host = Some(need_value(attr)?),
            "manual" => {
                no_value(attr)?;
                is_manual = true;
            }
            other => {
                return Err(DocSourceError::UnknownAttribute(
                    id,
                    other.to_string(),
                ))
            }
        }
    }

    if is_manual {
        // Checked here rather than in `validate_cells` so the contradiction is
        // reported before the body parse, which would otherwise complain about
        // whatever the author wrote under a `host=` they thought was running.
        if cell.assert {
            return Err(DocSourceError::ManualWithAssert(id));
        }
        if !cell.capture.is_empty() {
            return Err(DocSourceError::ManualWithCapture(id, cell.capture.join(",")));
        }
        if let Some(host) = &cell.host {
            return Err(DocSourceError::ManualWithHost(id.clone(), host.clone()));
        }
        cell.manual = Some(parse_manual_body(&id, &cell.body)?);
    }
    Ok(cell)
}

/// Lift a `manual` cell's brief out of its body (R717-T11).
///
/// The **leading** run of blank and `#` lines is the human's half; the first
/// line that is neither ends it, and everything from there down is
/// [`ManualCell::terminal`]. Anchoring on the leading block rather than
/// scanning the whole body is what lets an ordinary shell comment stay an
/// ordinary shell comment once the commands start.
fn parse_manual_body(id: &str, body: &str) -> Result<ManualCell, DocSourceError> {
    let mut out = ManualCell::default();
    let mut prompt_lines: Vec<&str> = Vec::new();
    let mut in_header = true;

    for line in body.lines() {
        let trimmed = line.trim();
        if in_header {
            if trimmed.is_empty() {
                // A blank line inside the block is a paragraph break — but a
                // leading one is just the fence's own newline.
                if !prompt_lines.is_empty() {
                    prompt_lines.push("");
                }
                continue;
            }
            if let Some(rest) = trimmed.strip_prefix('#') {
                let rest = rest.trim();
                if let Some(cond) = rest.strip_prefix("advance:") {
                    let cond = cond.trim();
                    if cond.is_empty() {
                        return Err(DocSourceError::ManualBlankAdvance(id.to_string()));
                    }
                    if let Some(first) = &out.advance {
                        return Err(DocSourceError::ManualDuplicateAdvance(
                            id.to_string(),
                            first.clone(),
                            cond.to_string(),
                        ));
                    }
                    out.advance = Some(cond.to_string());
                } else if let Some(item) = rest.strip_prefix("checklist:") {
                    let item = item.trim();
                    if !item.is_empty() {
                        out.checklist.push(item.to_string());
                    }
                } else {
                    prompt_lines.push(rest);
                }
                continue;
            }
            in_header = false;
        }
        if !trimmed.is_empty() {
            out.terminal.push(trimmed.to_string());
        }
    }

    out.prompt = prompt_lines.join("\n").trim().to_string();
    if out.prompt.is_empty() {
        return Err(DocSourceError::ManualNeedsPrompt(id.to_string()));
    }
    Ok(out)
}

fn split_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn validate_cells(cells: &[DocCell]) -> Result<(), DocSourceError> {
    for (i, cell) in cells.iter().enumerate() {
        for need in &cell.needs {
            match cells.iter().position(|c| &c.id == need) {
                None => {
                    return Err(DocSourceError::NeedsUnknownCell(
                        cell.id.clone(),
                        need.clone(),
                    ))
                }
                Some(j) if j >= i => {
                    return Err(DocSourceError::NeedsForwardReference(
                        cell.id.clone(),
                        need.clone(),
                    ))
                }
                Some(_) => {}
            }
        }
        // A `capture=` whose body never writes the file would render an
        // always-empty output and look like a runner bug. Catch it where the
        // author can still see the cell.
        if !cell.capture.is_empty() && !cell.body.contains("YAH_OUTPUTS") {
            return Err(DocSourceError::CaptureWithoutOutputsWrite(
                cell.id.clone(),
                cell.capture.join(","),
            ));
        }
        // Caught at the step level too (`SecretCannotDeclareOutputs`), but the
        // author is looking at a fence info string, not a TOML table — so say it
        // in the vocabulary they wrote.
        if cell.secret && !cell.capture.is_empty() {
            return Err(DocSourceError::SecretWithCapture(
                cell.id.clone(),
                cell.capture.join(","),
            ));
        }
    }
    Ok(())
}

/// Resolve one param's [`ParamDef::options_from`] glob into its
/// [`ParamDef::options`] — R717-T10, W296 §Q3. A no-op for a param that does
/// not declare one.
///
/// The glob is camp-relative and shaped `<dir>/<pattern>`, where `<pattern>`
/// may carry `*` wildcards; the **file stems** of the matches become the
/// options, sorted, deduplicated, and merged with any hand-written `options`
/// (a doc may pin one extra value the directory does not carry). Only the
/// final segment is a pattern — a `*` in the directory half would need a
/// recursive walk to answer, and nothing asks for one.
///
/// Matching nothing is an error rather than an empty dropdown: the whole
/// reason to enumerate is that the author does not want to hand-maintain the
/// list, so a typo'd glob has to be distinguishable from a correct one.
pub fn resolve_options_from(
    camp_root: &Path,
    name: &str,
    def: &mut ParamDef,
) -> Result<(), DocSourceError> {
    let Some(glob) = def.options_from.clone() else {
        return Ok(());
    };
    let (dir, pattern) = glob
        .rsplit_once('/')
        .ok_or_else(|| DocSourceError::OptionsFromNotAGlob(name.to_string(), glob.clone()))?;

    let mut found: Vec<String> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(camp_root.join(dir)) {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if !wildcard_match(pattern, &file_name) {
                continue;
            }
            let stem = std::path::Path::new(file_name.as_ref())
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| file_name.to_string());
            found.push(stem);
        }
    }
    if found.is_empty() {
        return Err(DocSourceError::OptionsFromMatchedNothing(
            name.to_string(),
            glob,
        ));
    }
    // Read-time resolution FILLS `options`, it does not replace the field's
    // meaning: everything downstream (`resolve_params`, `NotInOptions`, the
    // desktop dropdown) keeps reading one list.
    found.extend(def.options.iter().cloned());
    found.sort();
    found.dedup();
    def.options = found;
    Ok(())
}

/// `*`-only glob match over a single path segment. Enough for the
/// `<name>.toml` / `W*.md` shapes [`resolve_options_from`] enumerates, and
/// deliberately not a dependency: this crate publishes to crates.io, so a
/// twenty-line matcher beats a dep bump on every consumer.
fn wildcard_match(pattern: &str, candidate: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == candidate;
    }
    let mut rest = candidate;
    // A pattern not starting with `*` must match at position 0, and likewise
    // the tail must land at the very end — otherwise `*.toml` would accept
    // `notes.toml.bak`.
    if let Some(first) = parts.first() {
        match rest.strip_prefix(first) {
            Some(r) => rest = r,
            None => return false,
        }
    }
    if let Some(last) = parts.last() {
        if parts.len() > 1 {
            match rest.strip_suffix(last) {
                Some(r) => rest = r,
                None => return false,
            }
        }
    }
    for middle in parts.iter().take(parts.len() - 1).skip(1) {
        match rest.find(*middle) {
            Some(at) => rest = &rest[at + middle.len()..],
            None => return false,
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const W257_EXCERPT: &str = r#"
# W257 — static node fleet onboarding

```toml notebook=node-onboard
[params]
node = { required = true, description = "machine name, e.g. us-west-003" }
```

Some prose about flashing the stick.

```bash
# not a cell — no cell= attribute. Renders exactly as it does today.
echo hello
```

### 0x — build the ISO

```bash cell=build-iso inputs=.yah/infra/preseed/build-iso.sh,.yah/infra/preseed/yah-x86-worker.cfg capture=iso_sha
.yah/infra/preseed/build-iso.sh
sha256sum out.iso | cut -d' ' -f1 > /dev/null
echo iso_sha=abc >> "$YAH_OUTPUTS"
```

### 4 — prove the right image

```bash cell=probe-identity assert host={{node}} needs=build-iso
test "$(whoami)@$(hostname)" = "yah@{{node}}"
sudo -n true
```

### 8 — push the KEK

```bash cell=kek-push secret needs=probe-identity
yah cloud secret kek export --out "$RUNDIR/cluster.kek"
```
"#;

    fn machine_camp() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let machines = dir.path().join(".yah/infra/machines");
        std::fs::create_dir_all(&machines).unwrap();
        std::fs::write(
            machines.join("us-west-003.toml"),
            "name = \"us-west-003\"\n\n[connect]\naddress = \"192.168.10.32\"\nssh = \"yah@192.168.10.32\"\n",
        )
        .unwrap();
        // The second box W296 keeps naming — the whole point of the subject key
        // is that these two render independently.
        std::fs::write(
            machines.join("us-west-013.toml"),
            "name = \"us-west-013\"\n\n[connect]\nssh = \"yah@192.168.10.13\"\n",
        )
        .unwrap();
        dir
    }

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn a_fence_without_cell_is_not_a_cell() {
        let doc = parse_doc("W257.md", W257_EXCERPT).unwrap();
        assert_eq!(
            doc.cells.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            vec!["build-iso", "probe-identity", "kek-push"],
            "the plain ```bash fence is untouched — that is the whole degradation story"
        );
    }

    #[test]
    fn attributes_parse_off_the_info_string() {
        let doc = parse_doc("W257.md", W257_EXCERPT).unwrap();

        let build = doc.cell("build-iso").unwrap();
        assert!(!build.assert, "no `assert` ⇒ show");
        assert_eq!(build.capture, vec!["iso_sha"]);
        assert_eq!(build.inputs.len(), 2);
        assert_eq!(
            build.inputs[1],
            PathBuf::from(".yah/infra/preseed/yah-x86-worker.cfg")
        );

        let probe = doc.cell("probe-identity").unwrap();
        assert!(probe.assert);
        assert_eq!(probe.host.as_deref(), Some("{{node}}"));
        assert_eq!(probe.needs, vec!["build-iso"]);

        assert!(doc.cell("kek-push").unwrap().secret);
    }

    #[test]
    fn notebook_fence_supplies_params() {
        let doc = parse_doc("W257.md", W257_EXCERPT).unwrap();
        assert_eq!(doc.name, "node-onboard");
        assert!(doc.config.params["node"].required);
    }

    /// The F4 verify: W257's cells lower into the shape a hand-written
    /// `.yah/qed/node-onboard.toml` would have.
    #[test]
    fn the_doc_lowers_into_a_pipeline_shaped_like_a_hand_written_one() {
        let camp = machine_camp();
        let doc = parse_doc("W257.md", W257_EXCERPT).unwrap();
        let p = doc
            .lower(camp.path(), &params(&[("node", "us-west-003")]), None)
            .unwrap();

        assert_eq!(p.name, "node-onboard");
        assert_eq!(p.steps.len(), 3);
        assert_eq!(
            p.steps.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
            vec!["build-iso", "probe-identity", "kek-push"],
            "document order IS execution order"
        );
        // show ⇒ continue, assert ⇒ abort.
        assert!(matches!(p.steps[0].on_fail, OnFail::Continue));
        assert!(matches!(p.steps[1].on_fail, OnFail::Abort));
        // The R717-T1/T2 attributes reach their fields.
        assert_eq!(p.steps[0].inputs.len(), 2);
        assert_eq!(p.steps[0].outputs[0].name, "iso_sha");
        assert!(p.steps[2].secret);
        // Every lowered step is a plain subprocess — no new kind anywhere.
        assert!(p.steps.iter().all(|s| s.kind == StepKind::Subprocess));
        // And each one validates through the normal step rules.
        assert!(p.steps.iter().all(|s| s.validate().is_ok()));
    }

    /// R717-S13 Q1/Q4: the two doc defaults that differ from a pipeline TOML.
    #[test]
    fn doc_defaults_are_live_workspace_and_a_per_subject_lane() {
        let camp = machine_camp();
        let doc = parse_doc("W257.md", W257_EXCERPT).unwrap();

        let three = doc
            .lower(camp.path(), &params(&[("node", "us-west-003")]), None)
            .unwrap();
        let thirteen = doc
            .lower(camp.path(), &params(&[("node", "us-west-013")]), None)
            .unwrap();

        assert_eq!(three.workspace, WorkspaceMode::Live);
        assert_ne!(
            three.effective_concurrency_key(),
            thirteen.effective_concurrency_key(),
            "two operators bringing up two boxes is the EXPECTED case; they must not serialize"
        );
        assert_ne!(
            three.effective_concurrency_key(),
            crate::types::DEFAULT_CONCURRENCY_KEY,
            "and neither of them lands in the camp-global lane"
        );
        assert!(three.effective_concurrency_key().starts_with("@doc:W257.md#"));
    }

    #[test]
    fn an_explicit_notebook_key_wins_over_the_doc_default() {
        let camp = machine_camp();
        let md = "```toml notebook=n\nconcurrency_key = \"pi-image\"\n```\n\n\
                  ```bash cell=a\ntrue\n```\n";
        let p = parse_doc("d.md", md)
            .unwrap()
            .lower(camp.path(), &HashMap::new(), None)
            .unwrap();
        assert_eq!(p.effective_concurrency_key(), "pi-image");
    }

    /// R717-T5: the argv is the ssh line an operator would type, against the
    /// address `.yah/infra/machines/us-west-003.toml` actually declares.
    #[test]
    fn host_lowers_to_a_plain_ssh_subprocess_step() {
        let camp = machine_camp();
        let doc = parse_doc("W257.md", W257_EXCERPT).unwrap();
        let p = doc
            .lower(camp.path(), &params(&[("node", "us-west-003")]), None)
            .unwrap();

        let probe = &p.steps[1];
        assert_eq!(probe.kind, StepKind::Subprocess, "no new step kind");
        assert_eq!(probe.argv[0], "ssh");
        assert_eq!(&probe.argv[1..3], &["-o".to_string(), "BatchMode=yes".to_string()]);
        assert_eq!(probe.argv[3], "yah@192.168.10.32");
        assert_eq!(probe.argv.len(), 5, "the body crosses as ONE argument");
        assert!(probe.argv[4].contains("test \"$(whoami)@$(hostname)\" = \"yah@us-west-003\""));
        assert!(
            !probe.argv[4].contains("{{node}}"),
            "params are substituted into the body too"
        );
    }

    #[test]
    fn host_declaring_an_identity_gets_dash_i() {
        let camp = machine_camp();
        std::fs::write(
            camp.path().join(".yah/infra/machines/us-west-003.toml"),
            "[connect]\nssh = \"yah@192.168.10.32\"\nidentity = \"/keys/yah\"\n",
        )
        .unwrap();
        let md = "```toml notebook=n\n```\n\n```bash cell=probe host=us-west-003\ntrue\n```\n";
        let p = parse_doc("d.md", md)
            .unwrap()
            .lower(camp.path(), &HashMap::new(), None)
            .unwrap();
        assert_eq!(&p.steps[0].argv[3..5], &["-i".to_string(), "/keys/yah".to_string()]);
        assert_eq!(p.steps[0].argv[5], "yah@192.168.10.32");
    }

    #[test]
    fn host_naming_no_machine_file_is_an_error_that_names_the_path() {
        let camp = machine_camp();
        let md = "```toml notebook=n\n```\n\n```bash cell=probe host=us-west-999\ntrue\n```\n";
        let err = parse_doc("d.md", md)
            .unwrap()
            .lower(camp.path(), &HashMap::new(), None)
            .unwrap_err();
        assert!(
            err.to_string().contains(".yah/infra/machines/us-west-999.toml"),
            "{err}"
        );
    }

    #[test]
    fn host_with_an_undeclared_param_fails_rather_than_ssh_ing_to_a_literal_brace() {
        let camp = machine_camp();
        let md = "```toml notebook=n\n```\n\n```bash cell=probe host={{node}}\ntrue\n```\n";
        assert!(matches!(
            parse_doc("d.md", md)
                .unwrap()
                .lower(camp.path(), &HashMap::new(), None),
            Err(DocSourceError::HostUnresolvedParam(_, _))
        ));
    }

    // ----- reject-at-parse cases ---------------------------------------------

    #[test]
    fn duplicate_cell_ids_are_rejected() {
        let md = "```toml notebook=n\n```\n\n```bash cell=a\ntrue\n```\n\n```bash cell=a\ntrue\n```\n";
        assert_eq!(
            parse_doc("d.md", md).unwrap_err(),
            DocSourceError::DuplicateCellId("a".into())
        );
    }

    #[test]
    fn needs_naming_an_unknown_cell_is_rejected() {
        let md = "```toml notebook=n\n```\n\n```bash cell=a needs=ghost\ntrue\n```\n";
        assert!(matches!(
            parse_doc("d.md", md),
            Err(DocSourceError::NeedsUnknownCell(_, _))
        ));
    }

    #[test]
    fn needs_naming_a_later_cell_is_rejected() {
        // Document order is execution order, so a forward `needs` is a claim the
        // parser cannot honour — better to say so than to silently reorder the
        // doc out from under the reader.
        let md = "```toml notebook=n\n```\n\n```bash cell=a needs=b\ntrue\n```\n\n\
                  ```bash cell=b\ntrue\n```\n";
        assert!(matches!(
            parse_doc("d.md", md),
            Err(DocSourceError::NeedsForwardReference(_, _))
        ));
    }

    #[test]
    fn a_notebook_fence_after_a_cell_is_rejected() {
        let md = "```bash cell=a\ntrue\n```\n\n```toml notebook=n\n```\n";
        assert!(matches!(
            parse_doc("d.md", md),
            Err(DocSourceError::NotebookFenceNotFirst(_, _))
        ));
    }

    #[test]
    fn capture_without_a_yah_outputs_write_is_rejected() {
        let md = "```toml notebook=n\n```\n\n```bash cell=a capture=sha\necho hi\n```\n";
        assert!(matches!(
            parse_doc("d.md", md),
            Err(DocSourceError::CaptureWithoutOutputsWrite(_, _))
        ));
    }

    #[test]
    fn secret_and_capture_together_are_rejected() {
        let md = "```toml notebook=n\n```\n\n\
                  ```bash cell=a secret capture=fp\necho fp=x >> \"$YAH_OUTPUTS\"\n```\n";
        assert!(matches!(
            parse_doc("d.md", md),
            Err(DocSourceError::SecretWithCapture(_, _))
        ));
    }

    #[test]
    fn a_doc_with_no_notebook_fence_is_not_a_qed_source() {
        assert_eq!(
            parse_doc("d.md", "```bash cell=a\ntrue\n```\n").unwrap_err(),
            DocSourceError::NoNotebookFence
        );
    }

    // ── R717-T11 (W296): manual cells ────────────────────────────────────────

    /// W296's kek-mint cell, verbatim in shape: prose, an `advance:`, a
    /// checklist, and one command that is *prefill*, never run.
    const MANUAL_DOC: &str = r#"
```toml notebook=node-onboard
[params]
node = { required = true }
```

```bash cell=kek-mint manual if=!cells.kek-camp-exists.ok
# Mint the camp KEK for {{node}}. Once, ever — a second mint orphans
# every secret sealed under the first.
#
# advance: yah cloud secret kek fingerprint
# checklist: Confirmed no camp KEK exists
# checklist: Backup location decided
yah cloud secret kek init
```

### 0x·1–4 — BIOS

```bash cell=bios manual
# At the box, with a keyboard and a monitor, in BIOS setup:
# checklist: Secure Boot disabled
# checklist: USB first in the boot order
# checklist: Restore-on-AC-power-loss = On
```
"#;

    #[test]
    fn a_manual_cell_lifts_its_brief_out_of_the_leading_comment_block() {
        let doc = parse_doc("W257.md", MANUAL_DOC).unwrap();
        let m = doc.cell("kek-mint").unwrap().manual.as_ref().unwrap();

        assert_eq!(
            m.prompt,
            "Mint the camp KEK for {{node}}. Once, ever — a second mint orphans\n\
             every secret sealed under the first.",
            "bare `# ` prose is the prompt, and it is NOT substituted at parse time"
        );
        assert_eq!(m.advance.as_deref(), Some("yah cloud secret kek fingerprint"));
        assert_eq!(
            m.checklist,
            vec!["Confirmed no camp KEK exists", "Backup location decided"]
        );
        assert_eq!(
            m.terminal,
            vec!["yah cloud secret kek init"],
            "what is left below the comment block is PREFILL, not something the runner executes"
        );
    }

    /// W257's BIOS block is the case `advance` exists to be optional for: a
    /// person at the box, no out-of-band access, nothing to verify.
    #[test]
    fn a_manual_cell_may_have_no_advance_and_no_commands() {
        let doc = parse_doc("W257.md", MANUAL_DOC).unwrap();
        let m = doc.cell("bios").unwrap().manual.as_ref().unwrap();
        assert!(m.advance.is_none(), "advance must be optional");
        assert!(m.terminal.is_empty());
        assert_eq!(m.checklist.len(), 3, "the checklist is the whole contribution here");
        assert!(m.prompt.starts_with("At the box"));
    }

    /// The T11 verify, first half: a manual cell lowers to W282's human gate —
    /// no argv, `kind = manual`, and it validates through the normal step rules.
    #[test]
    fn a_manual_cell_lowers_to_a_step_kind_manual_gate() {
        let camp = machine_camp();
        let doc = parse_doc("W257.md", MANUAL_DOC).unwrap();
        let p = doc
            .lower(camp.path(), &params(&[("node", "us-west-003")]), None)
            .unwrap();

        let kek = &p.steps[0];
        assert_eq!(kek.kind, StepKind::Manual);
        assert!(
            kek.argv.is_empty(),
            "a manual step MUST carry no argv — the runner parks, it does not execute"
        );
        assert!(matches!(kek.on_fail, OnFail::Abort), "you cannot continue past a refused gate");
        assert_eq!(kek.if_cond.as_deref(), Some("!cells.kek-camp-exists.ok"));

        let cfg = kek.manual.as_ref().unwrap();
        assert!(
            cfg.prompt.starts_with("Mint the camp KEK for us-west-003."),
            "params reach the prompt at lowering, one pass with the rest of the cell: {}",
            cfg.prompt
        );
        assert_eq!(cfg.terminal, vec!["yah cloud secret kek init"]);
        assert_eq!(cfg.advance.as_deref(), Some("yah cloud secret kek fingerprint"));
        assert_eq!(cfg.advance_poll_secs, 5);

        assert!(p.steps[1].manual.as_ref().unwrap().advance.is_none());
        assert!(
            p.steps.iter().all(|s| s.validate().is_ok()),
            "and both validate through R622's own step rules"
        );
    }

    #[test]
    fn a_manual_cell_with_no_prompt_is_rejected() {
        let md = "```toml notebook=n\n```\n\n```bash cell=bios manual\n# advance: true\n```\n";
        assert!(matches!(
            parse_doc("d.md", md),
            Err(DocSourceError::ManualNeedsPrompt(_))
        ));
    }

    #[test]
    fn manual_contradictions_are_rejected_at_parse() {
        let cases: Vec<(&str, fn(&DocSourceError) -> bool)> = vec![
            (
                "```bash cell=a manual assert\n# do it\n```",
                |e| matches!(e, DocSourceError::ManualWithAssert(_)),
            ),
            (
                "```bash cell=a manual capture=fp\n# do it\n```",
                |e| matches!(e, DocSourceError::ManualWithCapture(_, _)),
            ),
            (
                "```bash cell=a manual host=us-west-003\n# do it\n```",
                |e| matches!(e, DocSourceError::ManualWithHost(_, _)),
            ),
            (
                "```bash cell=a manual\n# do it\n# advance:\n```",
                |e| matches!(e, DocSourceError::ManualBlankAdvance(_)),
            ),
            (
                "```bash cell=a manual\n# do it\n# advance: x\n# advance: y\n```",
                |e| matches!(e, DocSourceError::ManualDuplicateAdvance(_, _, _)),
            ),
        ];
        for (fence, want) in cases {
            let md = format!("```toml notebook=n\n```\n\n{fence}\n");
            let err = parse_doc("d.md", &md).unwrap_err();
            assert!(want(&err), "wrong error for `{fence}`: {err}");
        }
    }

    /// A `#` line *below* the commands is an ordinary shell comment and stays
    /// one — the brief is the LEADING block, not every comment in the body.
    #[test]
    fn only_the_leading_comment_block_is_the_brief() {
        let md = "```toml notebook=n\n```\n\n\
                  ```bash cell=a manual\n# Do the thing.\nfirst --cmd\n\
                  # advance: not-a-directive\nsecond --cmd\n```\n";
        let doc = parse_doc("d.md", md).unwrap();
        let m = doc.cell("a").unwrap().manual.as_ref().unwrap();
        assert_eq!(m.prompt, "Do the thing.");
        assert!(m.advance.is_none(), "a directive below the block is just shell prose");
        assert_eq!(
            m.terminal,
            vec!["first --cmd", "# advance: not-a-directive", "second --cmd"]
        );
    }

    #[test]
    fn an_unknown_attribute_is_rejected_rather_than_ignored() {
        let md = "```toml notebook=n\n```\n\n```bash cell=a ttl=6w\ntrue\n```\n";
        assert!(matches!(
            parse_doc("d.md", md),
            Err(DocSourceError::UnknownAttribute(_, _))
        ));
    }

    // ----- scanner + info string ---------------------------------------------

    /// W296 wraps its own cell examples in FOUR-backtick fences. A scanner that
    /// closed on any ``` would read those examples as live cells.
    #[test]
    fn a_four_backtick_fence_does_not_close_on_an_inner_three() {
        let md = "````text\n```bash cell=not-real\ntrue\n```\n````\n\n\
                  ```toml notebook=n\n```\n\n```bash cell=real\ntrue\n```\n";
        let doc = parse_doc("d.md", md).unwrap();
        assert_eq!(
            doc.cells.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            vec!["real"]
        );
    }

    #[test]
    fn a_quoted_attribute_value_may_contain_spaces() {
        let (lang, attrs) = split_info_string(r#"bash cell=a if="params.variant == 'full'" assert"#);
        assert_eq!(lang, "bash");
        assert_eq!(
            attrs,
            vec![
                Attr { key: "cell".into(), value: Some("a".into()) },
                Attr {
                    key: "if".into(),
                    value: Some("params.variant == 'full'".into()),
                },
                Attr { key: "assert".into(), value: None },
            ]
        );
    }

    #[test]
    fn if_reaches_the_lowered_step() {
        let camp = machine_camp();
        let md = "```toml notebook=n\n```\n\n\
                  ```bash cell=a if=\"!cells.probe.ok\"\ntrue\n```\n";
        let p = parse_doc("d.md", md)
            .unwrap()
            .lower(camp.path(), &HashMap::new(), None)
            .unwrap();
        assert_eq!(p.steps[0].if_cond.as_deref(), Some("!cells.probe.ok"));
    }

    // ----- single-cell selection ---------------------------------------------

    #[test]
    fn closure_pulls_in_prerequisites_in_document_order() {
        let doc = parse_doc("W257.md", W257_EXCERPT).unwrap();
        assert_eq!(
            doc.closure("kek-push")
                .iter()
                .map(|c| c.id.as_str())
                .collect::<Vec<_>>(),
            vec!["build-iso", "probe-identity", "kek-push"],
            "running one cell runs what it needs, in the order the doc reads"
        );
        assert_eq!(
            doc.closure("build-iso")
                .iter()
                .map(|c| c.id.as_str())
                .collect::<Vec<_>>(),
            vec!["build-iso"]
        );
    }

    #[test]
    fn lowering_one_cell_lowers_only_its_closure() {
        let camp = machine_camp();
        let doc = parse_doc("W257.md", W257_EXCERPT).unwrap();
        let p = doc
            .lower(
                camp.path(),
                &params(&[("node", "us-west-003")]),
                Some("probe-identity"),
            )
            .unwrap();
        assert_eq!(
            p.steps.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
            vec!["build-iso", "probe-identity"]
        );
    }

    // ── R717-T10 (W296 §Q3): `options_from` enumerates by path glob ──────────

    const OPTIONS_FROM_DOC: &str = r#"
```toml notebook=node-onboard
[params]
node = { required = true, options_from = ".yah/infra/machines/*.toml" }
```

```bash cell=probe assert host={{node}}
sudo -n true
```
"#;

    #[test]
    fn options_from_fills_options_with_the_matched_file_stems() {
        let camp = machine_camp();
        let doc = parse_doc("W257.md", OPTIONS_FROM_DOC).unwrap();
        let resolved = doc.resolved_params(camp.path()).unwrap();
        assert_eq!(
            resolved["node"].options,
            vec!["us-west-003", "us-west-013"],
            "the selector's two subjects come from the directory, not from the doc"
        );
        assert_eq!(
            doc.config.params["node"].options,
            Vec::<String>::new(),
            "resolution is READ time — the parsed doc keeps the glob, so a machine \
             added later shows up without editing the doc"
        );
    }

    #[test]
    fn a_glob_matching_nothing_is_an_authoring_error() {
        let camp = tempfile::tempdir().unwrap();
        let doc = parse_doc("W257.md", OPTIONS_FROM_DOC).unwrap();
        assert!(matches!(
            doc.resolved_params(camp.path()),
            Err(DocSourceError::OptionsFromMatchedNothing(name, _)) if name == "node"
        ));
    }

    #[test]
    fn a_param_with_no_options_from_resolves_to_itself() {
        let camp = machine_camp();
        let doc = parse_doc("W257.md", W257_EXCERPT).unwrap();
        let resolved = doc.resolved_params(camp.path()).unwrap();
        assert!(resolved["node"].required);
        assert!(resolved["node"].options.is_empty());
    }

    #[test]
    fn wildcard_match_anchors_both_ends() {
        assert!(wildcard_match("*.toml", "us-west-003.toml"));
        assert!(!wildcard_match("*.toml", "us-west-003.toml.bak"));
        assert!(wildcard_match("W*.md", "W257-static-node-fleet-onboarding.md"));
        assert!(!wildcard_match("W*.md", "A046-yah-run-tab.md"));
        assert!(wildcard_match("exact.toml", "exact.toml"));
        assert!(!wildcard_match("exact.toml", "other.toml"));
    }
}
