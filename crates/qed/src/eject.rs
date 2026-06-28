//! Eject / materialize an imported workflow to hash-stamped TOML (R533-F6, W224).
//!
//! W224's import primitive has one toggle with two states, and that toggle *is*
//! the migration ramp:
//!
//! - **Virtual** (default, R533-F1): expand the `workflow.yml` into the in-memory
//!   subgraph at plan time, persist nothing. Zero drift by construction — there
//!   is no stored derivative to diverge.
//! - **Eject / materialize** (this module): write the F4 transform's native
//!   steps as a generated, **hash-stamped** TOML pipeline. A one-time directional
//!   move — after ejecting, the TOML is canonical and hand-editable and the
//!   source yml can be deleted. The "sync button" is an *eject* button.
//!
//! The hard rule W224 sets is **never two editable canonical copies at once**.
//! While the yml is canonical the TOML is virtual; once ejected the yml is gone.
//! If a materialized TOML must coexist with its yml during an overlap window, the
//! pinned source hash is the guardrail:
//!
//! - [`freshness`] recomputes the source hash on demand; a mismatch means the
//!   source drifted since the eject ([`EjectFreshness::StaleSource`]).
//! - [`validate_ejected`] **re-expands** the source and compares it byte-for-byte
//!   against the on-disk generated body, so a hand-edit of a generated file is
//!   caught and never silently honored — and a drifted source is reported
//!   distinctly from a hand-edit.
//!
//! ## Provenance lives in a comment header, not the pipeline body
//!
//! The generated body is a **100%-valid normal [`Pipeline`] TOML** — the existing
//! loader runs an ejected pipeline with no special-casing. Provenance (source
//! path + pinned hash) and the F4 flags ride in a leading `# @qed:generated …`
//! comment header that the loader ignores and this module parses. That keeps the
//! eject reversible-by-inspection and avoids both a 46-site `Pipeline` field add
//! and TOML's table-after-array ordering trap.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::import::content_hash;
use crate::transform::{transform_workflow, FlagKind, TransformReport};
use crate::types::{Pipeline, Placement};
use yah_qed_gha::Workflow;

/// Marker beginning the provenance comment line. The whole header is a run of
/// leading `#` comments; only the `@qed:generated` line carries the pin.
const HEADER_TAG: &str = "# @qed:generated";

/// Parsed provenance of an ejected pipeline — what it was generated from and the
/// source hash pinned at eject time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedHeader {
    /// The `workflow.yml` this TOML was ejected from (camp-relative).
    pub source: PathBuf,
    /// blake3 [`content_hash`] of the source bytes at eject time — the pin the
    /// freshness / validate guards compare against.
    pub source_hash: String,
}

/// Freshness of an on-disk ejected pipeline relative to its source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EjectFreshness {
    /// The source's current hash matches the pin — the eject is up to date.
    Fresh,
    /// The source drifted since the eject. Under materialization this marks the
    /// eject "dirty" (re-eject needed); carries both hashes for reporting.
    StaleSource { pinned: String, actual: String },
}

impl EjectFreshness {
    pub fn is_fresh(&self) -> bool {
        matches!(self, EjectFreshness::Fresh)
    }
}

/// Why an on-disk ejected pipeline failed [`validate_ejected`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidateError {
    /// No `# @qed:generated` header — the file isn't an ejected pipeline (or the
    /// header was stripped), so there's nothing to re-expand against.
    NotGenerated,
    /// The source drifted since the eject (pin mismatch). Re-eject to refresh.
    SourceDrifted { pinned: String, actual: String },
    /// The on-disk generated body no longer matches what re-expanding the source
    /// produces — a hand-edit of a generated file. W224: caught, never honored.
    HandEdited,
}

/// Eject an imported workflow to a hash-stamped, generated TOML string.
///
/// `source` is the camp-relative path recorded in the header; `source_bytes` are
/// the exact bytes hashed for the pin (the caller owns the file read — this stays
/// pure). The body is the F4 [`transform`](crate::transform) of `workflow`
/// rendered as a [`Pipeline`]; tier-3 / unknown flags are surfaced as header
/// comments so the human sees what still needs a native replacement.
pub fn eject(source: &Path, source_bytes: &[u8], workflow: &Workflow) -> String {
    let report = transform_workflow(workflow);
    let header = GeneratedHeader { source: source.to_path_buf(), source_hash: content_hash(source_bytes) };
    render_document(&header, &report)
}

/// Recompute the source hash and compare against an ejected pipeline's pin.
/// `current_source_bytes` are the bytes on disk now; returns [`EjectFreshness`].
/// `None` when `generated_toml` carries no `# @qed:generated` header.
pub fn freshness(generated_toml: &str, current_source_bytes: &[u8]) -> Option<EjectFreshness> {
    let header = parse_header(generated_toml)?;
    let actual = content_hash(current_source_bytes);
    Some(if actual == header.source_hash {
        EjectFreshness::Fresh
    } else {
        EjectFreshness::StaleSource { pinned: header.source_hash, actual }
    })
}

/// The `qed validate` re-expansion guard. Given the on-disk generated TOML, the
/// current source bytes, and the freshly-parsed source workflow:
///
/// 1. require a provenance header ([`ValidateError::NotGenerated`] otherwise);
/// 2. fail if the source drifted from the pin ([`ValidateError::SourceDrifted`]);
/// 3. re-eject the source and fail if the generated *body* differs from disk
///    ([`ValidateError::HandEdited`]) — a hand-edit of a generated file.
///
/// On success the on-disk file faithfully reflects its source.
pub fn validate_ejected(
    generated_toml: &str,
    current_source_bytes: &[u8],
    workflow: &Workflow,
) -> Result<(), ValidateError> {
    let header = parse_header(generated_toml).ok_or(ValidateError::NotGenerated)?;

    let actual = content_hash(current_source_bytes);
    if actual != header.source_hash {
        return Err(ValidateError::SourceDrifted { pinned: header.source_hash, actual });
    }

    // Re-expand and compare bodies (header stripped — comments aren't canonical).
    let expected = eject(&header.source, current_source_bytes, workflow);
    if strip_header(&expected) != strip_header(generated_toml) {
        return Err(ValidateError::HandEdited);
    }
    Ok(())
}

/// Render the full ejected document: provenance + flag comment header, then the
/// native pipeline body.
fn render_document(header: &GeneratedHeader, report: &TransformReport) -> String {
    let pipeline = report_to_pipeline(report);
    let body = toml::to_string_pretty(&pipeline)
        .unwrap_or_else(|e| panic!("serialize ejected pipeline: {e}"));
    format!("{}\n{body}", render_header(header, report))
}

/// The leading comment block: the machine-readable pin line, a provenance note,
/// and one `# @qed:flag …` line per F4 flag (so tier-3 replacements travel with
/// the generated file).
fn render_header(header: &GeneratedHeader, report: &TransformReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{HEADER_TAG} source=\"{}\" hash=\"{}\"\n",
        header.source.display(),
        header.source_hash
    ));
    out.push_str("# Generated by `qed eject` (R533-F6, W224). Do not hand-edit: re-eject the\n");
    out.push_str("# source, or delete the source and own this file. `qed validate` re-expands\n");
    out.push_str("# and fails if this body drifts from its source.\n");
    for step in &report.steps {
        for flag in &step.flags {
            out.push_str(&format!(
                "# @qed:flag job={} step={} severity={} -- {}\n",
                step.job,
                step.step_index,
                flag.severity().label(),
                flag_summary(flag),
            ));
        }
    }
    out
}

/// One-line summary of a flag for the header: what it is + the native stanza.
fn flag_summary(flag: &FlagKind) -> String {
    let what = match flag {
        FlagKind::ReplaceWithNative(nr) => format!("tier-3 {}", nr.label()),
        FlagKind::EmbeddedServiceTouch(_) => "embedded service touch".to_string(),
        FlagKind::ToolkitAction { slug, .. } => format!("toolkit action {slug}"),
        FlagKind::Unknown { slug } => format!("unknown action {slug}"),
        FlagKind::UnresolvedExpression => "unresolved expression".to_string(),
    };
    format!("{what}: {}", flag.stanza_hint())
}

/// Build a native [`Pipeline`] from a transform report — the ejected body.
fn report_to_pipeline(report: &TransformReport) -> Pipeline {
    Pipeline {
        name: report.name.clone(),
        label: report.label.clone(),
        steps: report.collect_native(),
        params: HashMap::new(),
        on_success: Vec::new(),
        on_fail: Vec::new(),
        triggers: Vec::new(),
        concurrency_key: None,
        placement: Placement::default(),
        workspace: crate::types::WorkspaceMode::default(),
        // Record that this pipeline exists *because* it composes a workflow, so
        // the daemon suppresses the source's auto-ingest (no double catalog
        // entry). Advisory only.
        wraps: Some(format!("gha:{}", report.name)),
        matrix: None,
        toolchain: None,
        binds: Vec::new(),
        on_change: Vec::new(),
        finally: Vec::new(),
    }
}

/// Parse the `# @qed:generated source="…" hash="…"` provenance line out of a
/// document's leading comment header. `None` when absent.
fn parse_header(toml_text: &str) -> Option<GeneratedHeader> {
    let line = toml_text.lines().find(|l| l.trim_start().starts_with(HEADER_TAG))?;
    let source = scan_quoted_field(line, "source=")?;
    let source_hash = scan_quoted_field(line, "hash=")?;
    Some(GeneratedHeader { source: PathBuf::from(source), source_hash })
}

/// Extract a `key="value"` field's value from a header line.
fn scan_quoted_field(line: &str, key: &str) -> Option<String> {
    let after = &line[line.find(key)? + key.len()..];
    let rest = after.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Drop the leading run of comment / blank lines — the non-canonical header —
/// leaving the pipeline body for byte-comparison.
fn strip_header(text: &str) -> &str {
    let mut idx = 0;
    for line in text.lines() {
        let t = line.trim_start();
        if t.starts_with('#') || t.is_empty() {
            idx += line.len() + 1; // +1 for the '\n'
        } else {
            break;
        }
    }
    text[idx.min(text.len())..].trim_start_matches('\n')
}

#[cfg(test)]
mod tests {
    use super::*;

    const WF: &str = r#"
name: Release Flow
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Build
        run: cargo build --release
"#;

    fn wf(src: &str) -> Workflow {
        yah_qed_gha::parse_workflow(src).expect("parse")
    }

    #[test]
    fn ejected_body_is_loadable_pipeline_toml() {
        let doc = eject(Path::new(".github/workflows/release.yml"), WF.as_bytes(), &wf(WF));
        // Header present and machine-readable.
        assert!(doc.contains("# @qed:generated source="));
        // The tier-3 checkout flag travels in the header.
        assert!(doc.contains("@qed:flag"));
        assert!(doc.to_lowercase().contains("checkout"));
        // The body (header stripped) parses as a normal Pipeline.
        let body = strip_header(&doc);
        let pipeline: Pipeline = toml::from_str(body).expect("ejected body is valid Pipeline TOML");
        assert_eq!(pipeline.name, "release-flow");
        assert_eq!(pipeline.steps.len(), 1, "only the run step is native; checkout is flagged");
        assert_eq!(pipeline.steps[0].name, "build: Build");
    }

    #[test]
    fn header_round_trips_through_parse() {
        let doc = eject(Path::new("wf.yml"), WF.as_bytes(), &wf(WF));
        let h = parse_header(&doc).expect("header parses");
        assert_eq!(h.source, PathBuf::from("wf.yml"));
        assert_eq!(h.source_hash, content_hash(WF.as_bytes()));
        assert_eq!(h.source_hash.len(), 64);
    }

    #[test]
    fn freshness_is_fresh_for_unchanged_source() {
        let doc = eject(Path::new("wf.yml"), WF.as_bytes(), &wf(WF));
        assert_eq!(freshness(&doc, WF.as_bytes()), Some(EjectFreshness::Fresh));
    }

    #[test]
    fn freshness_is_stale_when_source_drifts() {
        let doc = eject(Path::new("wf.yml"), WF.as_bytes(), &wf(WF));
        let drifted = format!("{WF}\n# a comment that changes the bytes\n");
        match freshness(&doc, drifted.as_bytes()) {
            Some(EjectFreshness::StaleSource { pinned, actual }) => {
                assert_eq!(pinned, content_hash(WF.as_bytes()));
                assert_eq!(actual, content_hash(drifted.as_bytes()));
                assert_ne!(pinned, actual);
            }
            other => panic!("expected StaleSource, got {other:?}"),
        }
    }

    #[test]
    fn freshness_none_without_header() {
        assert_eq!(freshness("name = \"x\"\nlabel = \"x\"\n", WF.as_bytes()), None);
    }

    #[test]
    fn validate_passes_for_a_fresh_unedited_eject() {
        let doc = eject(Path::new("wf.yml"), WF.as_bytes(), &wf(WF));
        assert_eq!(validate_ejected(&doc, WF.as_bytes(), &wf(WF)), Ok(()));
    }

    #[test]
    fn validate_flags_a_hand_edited_body() {
        let doc = eject(Path::new("wf.yml"), WF.as_bytes(), &wf(WF));
        // Tamper with the generated body (not the header).
        let tampered = doc.replace("cargo build --release", "cargo build --release --tampered");
        assert_ne!(tampered, doc);
        assert_eq!(
            validate_ejected(&tampered, WF.as_bytes(), &wf(WF)),
            Err(ValidateError::HandEdited),
        );
    }

    #[test]
    fn validate_reports_source_drift_distinctly_from_hand_edit() {
        let doc = eject(Path::new("wf.yml"), WF.as_bytes(), &wf(WF));
        let drifted = format!("{WF}\n# drift\n");
        match validate_ejected(&doc, drifted.as_bytes(), &wf(&drifted)) {
            Err(ValidateError::SourceDrifted { pinned, actual }) => {
                assert_eq!(pinned, content_hash(WF.as_bytes()));
                assert_eq!(actual, content_hash(drifted.as_bytes()));
            }
            other => panic!("expected SourceDrifted, got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_a_non_generated_file() {
        assert_eq!(
            validate_ejected("name = \"hand\"\nlabel = \"hand\"\n", WF.as_bytes(), &wf(WF)),
            Err(ValidateError::NotGenerated),
        );
    }

    #[test]
    fn eject_is_deterministic() {
        let a = eject(Path::new("wf.yml"), WF.as_bytes(), &wf(WF));
        let b = eject(Path::new("wf.yml"), WF.as_bytes(), &wf(WF));
        assert_eq!(a, b, "same source → byte-identical eject (validate relies on this)");
    }
}
