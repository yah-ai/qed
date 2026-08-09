//! The W224 import primitive's pure core (R533-F1).
//!
//! W224 settles "what is a GitHub Actions workflow to QED?" as **import, not
//! emulate**: a `workflow.yml` is an *import source* QED expands into its own
//! native subgraph, not a foreign runtime QED faithfully reproduces forever.
//! This module holds the side-effect-free heart of that primitive:
//!
//! 1. [`content_hash`] — the blake3 pin of a source yml's raw bytes. The pin
//!    lives in [`ImportConfig::hash`](crate::types::ImportConfig::hash); on
//!    every run the runner recomputes the source's hash and compares.
//! 2. [`ImportFreshness`] + [`ImportConfig::freshness`] — the staleness
//!    decision the pin enables. There are never two editable canonical copies
//!    at once (W224): while the yml is canonical the expansion is *virtual*
//!    (recomputed at plan time, never stored — zero drift by construction), so
//!    a drifted source is benign (re-expand + re-pin). Once a generated TOML is
//!    materialized (`eject`, R533-F6) the pin instead marks that on-disk
//!    derivative stale.
//! 3. [`expand_import`] — the plan-time expansion seam: parsed workflow →
//!    native QED subgraph.
//!
//! ## F1 scope of the expansion
//!
//! The mechanical tier-1/2 → native step mapping is **R533-F4** (the assisted
//! transformer). Until it lands, [`expand_import`] produces the single-node
//! [`ImportExpansion::Delegated`] form: route the whole workflow through the
//! recast W200 GHA front-end (the `qed-gha` parser + tier-1/2 executor, which
//! W224 keeps and re-points). This is the migration ramp — while GHA is
//! canonical the import step still *runs* — and it keeps the runner seam,
//! freshness check, and re-pin loop settled here so F4 swaps only the
//! expansion body, not the surrounding machinery.
//!
//! @yah:ticket(R717-F4, "doc_source.rs: parse a .md into a Pipeline from fence info-strings + one notebook= TOML fence")
//! @yah:status(review)
//! @yah:assignee(agent:bundle-anthropic-glimmerstone)
//! @yah:at(2026-08-08T21:19:42Z)
//! @yah:phase(P2)
//! @yah:parent(R717)
//! @arch:see(.yah/docs/working/W296-executable-docs-notebook-cells.md)
//! @yah:next("NEW FILE oss/qed/crates/qed/src/doc_source.rs — annotation lives here on import.rs because that is the mechanism being mirrored; the edit target is the new module.")
//! @yah:next("Fence info-string attributes, all optional, cell=<id> being what makes a fence a cell at all — ABSENT MEANS TODAY'S RENDERING, UNCHANGED. Vocabulary: cell=<id> (stable, author-assigned, never positional), assert (exit code is the verdict; default is show), capture=<k,..> (lowers to the existing OutputDecl + $YAH_OUTPUTS path), inputs=<path,..> (R717-T1), needs=<cell> (default is document order), if=<expr> (existing QedStep::if_cond), secret (R717-T2), host=<param|name> (R717-F5), manual (R717-T11, blocked on R622).")
//! @yah:next("Doc-level config — params, concurrency_key, binds — comes from ONE leading TOML fence tagged notebook=<name>, deserialized through the EXISTING PipelineFile syntax so validate() is reused rather than reimplemented.")
//! @yah:next("Follow StepKind::Import's doctrine verbatim: virtual expansion recomputed at plan time, never persisted, zero drift by construction. The markdown is the source; nothing about the pipeline is written back into the .md.")
//! @yah:next("Reject-at-parse cases worth tests: duplicate cell= ids in one doc, needs= naming an unknown cell, a notebook= fence that is not first, capture= on a cell whose body writes no $YAH_OUTPUTS.")
//! @yah:next("Tier: Wizard — this is the core of the spike; the vocabulary cut (assert/show/capture/host) has met exactly one runbook and the parser is where the cut gets tested.")
//! @yah:verify("steps 4-8 of W257 parse into a Pipeline whose shape matches a hand-written .yah/qed/node-onboard.toml")
//! @yah:gotcha("Why the info string and not a custom container or HTML comment: these files are markdown in a git repo, read on GitHub, in editors, and by agents with Read. An info-string attribute degrades to an ordinary fenced code block everywhere that does not know about it; a custom syntax does not. Do not relitigate this.")
//! @yah:handoff("SHIPPED (uncommitted). NEW FILE oss/qed/crates/qed/src/doc_source.rs, registered in lib.rs and re-exported. Two-phase by design: parse_doc(doc_rel, markdown) -> DocSource is params-free, and DocSource::lower(camp_root, resolved_params, only) -> Pipeline resolves host= and substitutes {{param}} into cell bodies in ONE pass against ONE map. Splitting those would let a host= cell address us-west-003 while its body still said {{node}} — exactly the class of mismatch this relay exists to kill. The lowering follows StepKind::Import's doctrine verbatim: virtual, recomputed at plan time, nothing written back into the .md.")
//! @yah:handoff("DocSource keeps the doc-level facts a Pipeline has nowhere to put — cell id, needs edges, assert-vs-show, host — so R717-F6/T7 can address them without re-parsing markdown. Vocabulary lowering: cell= becomes the step NAME; assert -> OnFail::Abort (the exit code is the verdict) and its absence -> OnFail::Continue (a show cell runs for its output and must not stop the doc); capture= -> Vec<OutputDecl> on the existing $YAH_OUTPUTS path; inputs=/secret/if= -> the R717-T1/T2 fields and the existing if_cond. Bodies get a `set -eo pipefail` prologue, the same one crate::transform gives an imported GHA run: block and for the same reason — a multi-line assert cell must fail on the line that failed, not on whatever the last line returned.")
//! @yah:handoff("DESIGN CALL: `needs=` is a CHECKED CONSTRAINT, not a scheduler. Document order is execution order; a needs= naming a later cell is a parse error (NeedsForwardReference) rather than a reorder. Reason: a QED pipeline is a sequence, so a topological reorder inside the parser would make the doc's visible cell order differ from what actually runs — and for a runbook a human reads top to bottom that is precisely the wrong outcome. What needs= earns instead is (a) an enforced dependency the author can state and (b) DocSource::closure(id), the prerequisite set for a single-cell run, which is the case where the edges genuinely have to be traversed. lower(.., only: Some(id)) already lowers exactly that closure — R717-T7's --cell should call it rather than re-deriving.")
//! @yah:handoff("Doc-level config: NotebookConfig deserializes the one leading `toml notebook=<name>` fence, reusing the SAME field types as PipelineConfig (HashMap<String,ParamDef>, Vec<BindSpec>, Vec<OnChangeHook>, WorkspaceMode) so params and binds validate through the paths a .yah/qed/*.toml uses rather than a parallel copy. serde(deny_unknown_fields) so a typo'd key fails loudly. Two defaults differ from a hand-written pipeline, both from R717-S13 and both overridable from the fence: workspace = live (the Checkout default bails on a dirty tree, which makes a runbook unrunnable on this shared tree and is wrong semantics anyway) and concurrency_key = @doc:<doc>#<param_fingerprint> (per-SUBJECT; the @camp default from R719-F1 would serialize two operators bringing up two boxes, which is the expected case).")
//! @yah:gotcha("The fence scanner implements CommonMark's close-on-at-least-as-many-backticks rule, and that is load-bearing rather than pedantry: W296 wraps its OWN cell examples in four-backtick fences, so a scanner that closed on any ``` would read the design doc's examples as live cells. Pinned by doc_source::tests::a_four_backtick_fence_does_not_close_on_an_inner_three. Info-string values may be double-quoted, which if= needs (if=\"params.variant == 'full'\" has spaces a naive whitespace split would shred).")
//! @yah:gotcha("Two attributes are REJECTED rather than ignored, deliberately. `manual` -> DocSourceError::ManualNotYetSupported (R717-T11, blocked on R622): silently RUNNING a cell whose entire purpose is to stop for a human is the one failure mode this feature must not have, so it fails at parse time until the lowering exists. Any unrecognised attribute -> UnknownAttribute, which among other things means a hopeful `ttl=` fails loudly instead of being quietly dropped — R717-S13 Q2 decided against a ttl axis and this is what keeps that decision visible to an author who tries.")
//! @yah:handoff("Tree anchor at handoff: 85801e7f6b76b369c0c8ecd2e5c7874990cd9286 — the shared tree as I left it. Diff against it (`git diff 85801e7f6b76b369c0c8ecd2e5c7874990cd9286..HEAD`) to see what landed under you, and quote this SHA rather than 'HEAD' in any revert/restore instruction.")
//! @yah:next("NOT WIRED TO ANY CALLER — doc_source is a library seam only. Nothing loads a .md as a pipeline yet; that is R717-T7 (yah qed doc / --doc/--cell/--param) and R717-T8 (qed.run schema + qed.cells), both gated on @Ashguard:polaris draining the cli.rs/mcp/tools.rs lane. Both should go through parse_doc + DocSource::lower(camp_root, resolved_params, only) and pass PipelineRunner::with_cell(CellRef{doc, cell_id, param_fingerprint(resolved)}) — the fingerprint MUST come from the resolved map or R717-T3's subject equivalence breaks.")
//! @yah:verify("The F4 verify, as doc_source::tests::the_doc_lowers_into_a_pipeline_shaped_like_a_hand_written_one: a W257 excerpt (notebook fence + a plain non-cell bash fence + build-iso/probe-identity/kek-push) lowers to a 3-step Pipeline in document order; assert vs show map to Abort vs Continue; inputs=/capture=/secret reach their fields; EVERY lowered step is StepKind::Subprocess and every one passes QedStep::validate().")
//! @yah:verify("a_fence_without_cell_is_not_a_cell — the plain ```bash fence is untouched, which is the whole degradation story.")
//! @yah:verify("All five reject-at-parse cases the ticket asked for, plus three more: DuplicateCellId, NeedsUnknownCell, NeedsForwardReference, NotebookFenceNotFirst, CaptureWithoutOutputsWrite, SecretWithCapture, ManualNotYetSupported, UnknownAttribute.")
//! @yah:verify("cargo test -p yah-qed --lib -- doc_source:: staleness:: : 31 passed / 0 failed. Full suite (skipping the docker test that hangs on this host): 778 passed / 0 failed / 1 ignored.")
//! @yah:handoff("Reconciliation audit: baton's 'NOT WIRED TO ANY CALLER' is by design (library seam; wiring is T7/T8's own scope, handled separately) — not residual for F4. doc_source.rs (parse_doc/DocSource::lower) confirmed landed in 871fde1c by content. cargo test -p yah-qed --lib -- --skip local_container_step_routes_through_docker_path: 782 passed / 0 failed / 1 ignored, incl. doc_source:: tests.")
//!
//! @yah:ticket(R717-T5, "host= lowering: resolve [connect].ssh from .yah/infra/machines/<name>.toml and wrap argv in ssh")
//! @yah:status(review)
//! @yah:assignee(agent:bundle-anthropic-glimmerstone)
//! @yah:at(2026-08-08T21:19:46Z)
//! @yah:phase(P2)
//! @yah:parent(R717)
//! @arch:see(.yah/docs/working/W296-executable-docs-notebook-cells.md)
//! @yah:next("host=<param|name> resolves [connect].ssh from .yah/infra/machines/<name>.toml and wraps the cell body's argv in ssh. It LOWERS TO A PLAIN Subprocess STEP — no new runner mechanism, no new StepKind. If this ticket grows a runner change, the design has been misread.")
//! @yah:next("host={{node}} must resolve through the notebook's params, so this runs after param substitution, not before.")
//! @yah:next("Interacts with W296 open question 1 (cwd): a pipeline positions a workspace once and runbook cells mostly want the camp root, but a host= cell runs somewhere else entirely. State the cwd rule for host= cells in a doc comment rather than leaving it to be discovered. R717-S13 owns settling it if the answer is not obvious in the writing.")
//! @yah:next("Tier: Cleric — mechanical lowering against an existing TOML shape.")
//! @yah:verify("a host= cell against us-west-003 produces the same argv as the ssh line an operator would type by hand")
//! @yah:handoff("SHIPPED (uncommitted) inside R717-F4's new doc_source.rs, as fn ssh_argv. It LOWERS TO A PLAIN Subprocess STEP: no new StepKind, no runner change, nothing downstream knows it is remote. Resolution runs during lower(), i.e. AFTER params, so host={{node}} substitutes first; an unsubstituted {{...}} surviving that is a hard error (HostUnresolvedParam) rather than an ssh to a literal brace. The resolved name must name a file at .yah/infra/machines/<name>.toml — a missing one errors with the full path (UnknownHost). Reads only [connect] from that file (ssh, plus an optional identity); everything else there is fleet-placement config a runbook cell has no business interpreting. Confirmed against the tree: all 9 machine TOMLs spell [connect].ssh as user@host.")
//! @yah:handoff("THE Q1 CWD RULE IS WRITTEN AS A DOC COMMENT ON ssh_argv, as the ticket asked, not left to be discovered. A host= cell has TWO cwds and QED owns one. LOCAL cwd is the run's positioned workspace — where the ssh binary is invoked and where any local path in the argv resolves. REMOTE cwd is the SSH login default (the remote user's home), because .yah/infra/machines/<name>.toml records a CONNECTION, not a remote workspace. A cell needing a particular remote directory writes an explicit cd in its body. Do NOT add a remote_cwd= attribute: it would be a second positioning mechanism against a tree QED neither owns nor versions, and it would drift from the local one silently. Same text is in W296's settled-questions section under Q1.")
//! @yah:handoff("Emitted argv is [ssh, -o, BatchMode=yes, (-i <identity>)?, <user@host>, <one-arg script>]. Two deliberate departures from the line W257 has an operator type by hand, both documented at the call site. (1) -o BatchMode=yes so a cell whose key is not loaded FAILS instead of parking a whole pipeline on an invisible password prompt. (2) The body crosses as ONE argument so the remote shell sees the script verbatim — splitting it would let the local shell re-tokenize the operator's quoting.")
//! @yah:gotcha("IDENTITY FILE, and the one config addition W257's cells would need. Every ssh line in W257 is `ssh -i ~/.ssh/yah yah@...`, but NO machine TOML declares a key path today — verified across all 9. Hardcoding ~/.ssh/yah into oss/qed is wrong (the crate ships standalone), so the lowering reads an OPTIONAL [connect].identity, expands a leading ~/ against $HOME (ssh does NOT expand ~ in an argv element), and emits -i only when it is present. Absent, the user's ~/.ssh/config decides — the same shape crates/yah/rpc-ssh's optional key_path already uses. To make a W257 host= cell byte-match the hand-typed line, add `identity = \"~/.ssh/yah\"` under [connect] in .yah/infra/machines/*.toml. NOT done here: those are fleet-config files outside this ticket's blast radius and no test needs them.")
//! @yah:handoff("Tree anchor at handoff: 85801e7f6b76b369c0c8ecd2e5c7874990cd9286 — the shared tree as I left it. Diff against it (`git diff 85801e7f6b76b369c0c8ecd2e5c7874990cd9286..HEAD`) to see what landed under you, and quote this SHA rather than 'HEAD' in any revert/restore instruction.")
//! @yah:verify("The T5 verify, as doc_source::tests::host_lowers_to_a_plain_ssh_subprocess_step: a host={{node}} cell against a fixture us-west-003.toml carrying the real [connect].ssh = yah@192.168.10.32 lowers to StepKind::Subprocess with argv[0]=ssh, the address the file declares, the body as a single trailing argument, and {{node}} substituted inside the body too.")
//! @yah:verify("host_declaring_an_identity_gets_dash_i; host_naming_no_machine_file_is_an_error_that_names_the_path; host_with_an_undeclared_param_fails_rather_than_ssh_ing_to_a_literal_brace.")
//! @yah:verify("cargo test -p yah-qed --lib -- doc_source:: staleness:: : 31 passed / 0 failed. Full suite (skipping the docker test that hangs here): 778 passed / 0 failed / 1 ignored.")
//! @yah:handoff("Reconciliation audit: baton was verify+commit only, no residual noted. ssh_argv confirmed landed in 871fde1c by content (inside doc_source.rs). cargo test -p yah-qed --lib -- --skip local_container_step_routes_through_docker_path: 782 passed / 0 failed / 1 ignored, incl. host_lowers_to_a_plain_ssh_subprocess_step.")
//!
//! @yah:ticket(R717-T11, "manual cells: lower to StepKind::Manual, and an agent-initiated doc run PARKS instead of self-answering")
//! @yah:at(2026-08-05T01:57:17Z)
//! @yah:status(open)
//! @yah:phase(P5)
//! @yah:parent(R717)
//! @arch:see(.yah/docs/working/W296-executable-docs-notebook-cells.md)
//! @yah:depends_on(R622)
//! @yah:next("BLOCKED ON R622 (@Ashguard, active) — StepKind::Manual, RunStatus::AwaitingHuman, park/resume with lock release, and form-as-surface all land there. This ticket ADDS NOTHING to that design; it only lowers the manual cell attribute onto it.")
//! @yah:next("prompt / advance / checklist ride in the cell body's leading comment block and lower to R622's [manual] config block.")
//! @yah:next("THE LOAD-BEARING RULE: a manual cell declares a human actor, and an agent-initiated run parks on it and reports AwaitingHuman. Agents CAN answer forms — that is the standing approval path — so without this rule an agent driving W257 sails straight through 'go set Restore-on-AC-Power-Loss in the BIOS'. Encode it as a rule in the lowering, not as a convention.")
//! @yah:next("Some manual cells have NO advance and never will: W257's BIOS block (Secure Boot, USB-first boot order, Restore-on-AC-loss) is a person at the box with no out-of-band access. advance must be optional; a checklist that remembers whether you did it is the whole contribution there.")
//! @yah:next("Tier: Warrior — small lowering, but the do-not-self-answer rule is a safety property and the parked-run semantics come from another agent's in-flight work.")
//! @yah:verify("an agent-initiated run of a doc containing a manual cell reports AwaitingHuman and stops; it does not answer the form")
//! @yah:gotcha("A manual step is a COORDINATION point, not an authorization gate — W282 non-goal, inherited verbatim by W296. Do not let it grow into a permissions system.")

use crate::types::{GhaWorkflowConfig, ImportConfig};

/// blake3 content hash of a source workflow's raw bytes, hex-encoded. The pin
/// stored in [`ImportConfig::hash`](crate::types::ImportConfig::hash) is
/// exactly this string.
///
/// Hashing the raw bytes (not the parsed AST) is deliberate: it catches every
/// edit — including comment / whitespace churn that a re-serialized AST would
/// erase — so "is this byte-for-byte the yml I pinned?" is answered without
/// re-parsing, and a hand-edit can never be silently honored.
pub fn content_hash(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// Freshness of an imported source relative to its pinned hash.
///
/// The disposition of [`Stale`](ImportFreshness::Stale) depends on the import's
/// `materialize` toggle, not on this enum: virtual expansion re-expands and
/// re-pins (benign); a materialized eject treats it as a stale derivative
/// (R533-F6). This type only reports the comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportFreshness {
    /// No hash pinned yet — first import, or a hand-authored `[import]` block.
    /// The caller should expand and adopt the freshly-computed hash as the pin.
    Unpinned,
    /// The source's current hash matches the pin. Safe to expand.
    Fresh,
    /// The source drifted since it was pinned. Carries both hashes so a
    /// caller (or `qed validate`, R533-F6) can report the divergence.
    Stale { pinned: String, actual: String },
}

impl ImportFreshness {
    /// Whether the on-disk source still matches its pin (or was never pinned).
    /// `false` only for [`Stale`](ImportFreshness::Stale).
    pub fn is_current(&self) -> bool {
        !matches!(self, ImportFreshness::Stale { .. })
    }
}

impl ImportConfig {
    /// Compare a freshly-computed source hash against the pinned one.
    ///
    /// `actual` is the [`content_hash`] of the bytes currently on disk; the
    /// caller computes it (the runner has just read the file, so it owns the
    /// bytes). Pure — no I/O here.
    pub fn freshness(&self, actual: &str) -> ImportFreshness {
        match self.hash.as_deref() {
            None => ImportFreshness::Unpinned,
            Some(pinned) if pinned == actual => ImportFreshness::Fresh,
            Some(pinned) => ImportFreshness::Stale {
                pinned: pinned.to_string(),
                actual: actual.to_string(),
            },
        }
    }
}

/// The result of expanding an imported workflow at plan time.
///
/// Modeled as an enum from the start so the runner seam stays stable across the
/// F1 → F4 transition: F1 only ever yields [`Delegated`](ImportExpansion::Delegated);
/// R533-F4 adds a native-steps variant carrying the mechanical tier-1/2 map,
/// and the runner's `match` grows one arm rather than changing the call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportExpansion {
    /// Single-node delegation: run the whole workflow through the recast W200
    /// GHA front-end. The F1 default and the migration ramp while GHA is
    /// canonical. R533-F4 introduces the native-steps form alongside this.
    Delegated(GhaWorkflowConfig),
}

/// Expand an imported workflow into a QED subgraph at plan time (W224 "import,
/// don't emulate").
///
/// F1 SCOPE: returns [`ImportExpansion::Delegated`] — the single-node form that
/// routes through the W200 GHA front-end. The `event` / `inputs` carried on the
/// [`ImportConfig`] are forwarded into the synthesized [`GhaWorkflowConfig`] so
/// the expansion impersonates the same trigger the source declares. R533-F4
/// replaces this body with the mechanical tier-1/2 native map (and, for tier-3
/// steps, native-replacement stanzas); the pin + virtual/eject toggle around it
/// are already owned by the caller, so nothing else moves.
pub fn expand_import(cfg: &ImportConfig) -> ImportExpansion {
    ImportExpansion::Delegated(GhaWorkflowConfig {
        path: cfg.source.clone(),
        event: cfg.event.clone(),
        inputs: cfg.inputs.clone(),
        // An import is a faithful expansion of the source workflow, not a
        // narrowing of it: `ImportConfig` declares no row selector and adding one
        // would change what "import this workflow" means.
        matrix: std::collections::HashMap::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn cfg(hash: Option<&str>) -> ImportConfig {
        ImportConfig {
            source: PathBuf::from(".github/workflows/release.yml"),
            hash: hash.map(str::to_string),
            materialize: false,
            event: None,
            inputs: Default::default(),
        }
    }

    #[test]
    fn content_hash_is_stable_and_byte_sensitive() {
        let a = content_hash(b"name: release\n");
        let b = content_hash(b"name: release\n");
        let c = content_hash(b"name: release \n"); // one extra space
        assert_eq!(a, b, "same bytes hash identically");
        assert_ne!(a, c, "a one-byte edit changes the pin");
        // blake3 hex is 64 chars.
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn freshness_unpinned_when_no_hash() {
        assert_eq!(cfg(None).freshness("deadbeef"), ImportFreshness::Unpinned);
    }

    #[test]
    fn freshness_fresh_on_match() {
        let h = content_hash(b"on: push\n");
        assert_eq!(cfg(Some(&h)).freshness(&h), ImportFreshness::Fresh);
    }

    #[test]
    fn freshness_stale_on_drift_carries_both_hashes() {
        let pinned = content_hash(b"on: push\n");
        let actual = content_hash(b"on: workflow_dispatch\n");
        let f = cfg(Some(&pinned)).freshness(&actual);
        assert_eq!(
            f,
            ImportFreshness::Stale {
                pinned: pinned.clone(),
                actual: actual.clone(),
            }
        );
        assert!(!f.is_current(), "stale is not current");
        assert!(ImportFreshness::Fresh.is_current());
        assert!(ImportFreshness::Unpinned.is_current());
    }

    #[test]
    fn expand_forwards_source_event_and_inputs() {
        let mut c = cfg(None);
        c.event = Some("workflow_dispatch".into());
        c.inputs.insert("tag".into(), "v1.2.3".into());
        let ImportExpansion::Delegated(gha) = expand_import(&c);
        assert_eq!(gha.path, c.source);
        assert_eq!(gha.event.as_deref(), Some("workflow_dispatch"));
        assert_eq!(gha.inputs.get("tag").map(String::as_str), Some("v1.2.3"));
    }
}
