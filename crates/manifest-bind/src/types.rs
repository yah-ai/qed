use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::intent::Intent;
use crate::value_type::ValueType;

/// A typed value produced by a pipeline step. The string carries the bytes
/// the producer wrote to `$YAH_OUTPUTS`; `kind` carries the declared shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputValue {
    pub kind: ValueType,
    pub raw: String,
    /// Optional metadata the predicate can use (e.g. semver tag,
    /// human-readable version string from `cargo pkgid`).
    #[serde(default)]
    pub tag: Option<String>,
}

impl OutputValue {
    pub fn new(kind: ValueType, raw: impl Into<String>) -> Self {
        Self {
            kind,
            raw: raw.into(),
            tag: None,
        }
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Run the value's declared type validator. Cheap regex check; catches
    /// "this step claims blake3-hex and produced something else."
    pub(crate) fn validate_type(&self) -> Result<(), String> {
        self.kind.validate(&self.raw)
    }
}

/// `step.outputs.name` — a reference from a `[[bind]].from` field to a
/// specific step output. The URI-shaped escape hatch (`registry://...`)
/// parses as `OutputRef::Uri(String)`; it isn't resolved by the applier in
/// v1 (off the happy path — see W209 § Escape hatches).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum OutputRef {
    /// `<step_id>.outputs.<key>`
    StepOutput { step: String, key: String },
    /// Off the v1 happy path; reserved.
    Uri(String),
}

impl OutputRef {
    /// Parse the `from` field of a bind spec.
    pub fn parse(s: &str) -> Result<Self, String> {
        if let Some(idx) = s.find("://") {
            // crude scheme detection — registry://, oci://, https://, etc.
            if idx > 0 && idx < 16 {
                return Ok(Self::Uri(s.to_owned()));
            }
        }
        // Canonical: <step>.outputs.<key>
        let parts: Vec<&str> = s.splitn(3, '.').collect();
        match parts.as_slice() {
            [step, "outputs", key] if !step.is_empty() && !key.is_empty() => {
                Ok(Self::StepOutput {
                    step: (*step).to_owned(),
                    key: (*key).to_owned(),
                })
            }
            _ => Err(format!(
                "expected '<step>.outputs.<key>' or '<scheme>://...', got {s:?}"
            )),
        }
    }
}

impl TryFrom<String> for OutputRef {
    type Error = String;
    fn try_from(s: String) -> Result<Self, String> {
        Self::parse(&s)
    }
}

impl From<OutputRef> for String {
    fn from(r: OutputRef) -> String {
        r.to_string()
    }
}

impl std::fmt::Display for OutputRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StepOutput { step, key } => write!(f, "{step}.outputs.{key}"),
            Self::Uri(s) => f.write_str(s),
        }
    }
}

/// Outputs collected across one pipeline run, keyed by `(step, key)`.
#[derive(Debug, Clone, Default)]
pub struct OutputMap {
    by_step: HashMap<String, HashMap<String, OutputValue>>,
}

impl OutputMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one output emitted by a step.
    pub fn insert(&mut self, step: impl Into<String>, key: impl Into<String>, value: OutputValue) {
        self.by_step
            .entry(step.into())
            .or_default()
            .insert(key.into(), value);
    }

    /// Look up the value for a bind's `from` reference.
    pub fn lookup(&self, from: &OutputRef) -> Option<&OutputValue> {
        match from {
            OutputRef::StepOutput { step, key } => {
                self.by_step.get(step).and_then(|m| m.get(key))
            }
            // URI-shaped from is an escape hatch — applier doesn't resolve it
            // in v1; treat as "no value present yet".
            OutputRef::Uri(_) => None,
        }
    }

    /// True iff this step has at least one recorded output.
    pub fn has_step(&self, step: &str) -> bool {
        self.by_step.contains_key(step)
    }
}

/// One `[[bind]]` table from a pipeline TOML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindSpec {
    /// Path to the manifest file, relative to `workspace_root`.
    pub file: PathBuf,
    /// Format-aware path within the file (TOML dotted/indexed).
    pub path: String,
    /// Producer reference (`<step>.outputs.<key>` or `<scheme>://...`).
    pub from: OutputRef,
    /// Predicate. Defaults to `pin` if omitted at the TOML layer.
    #[serde(default)]
    pub intent: Intent,
    /// Opt-in for cross-workspace writes (non-interactive runs require this
    /// to be true if `file` resolves outside the publishing workspace).
    /// Enforcement of the workspace check itself lives in the qed-run tile;
    /// the applier just plumbs the flag.
    #[serde(default)]
    pub cross_workspace: bool,
    /// Optional schema tag (`"workload.toml/v1"`); when present the applier
    /// validates the file against that schema before writing. Bootstrap form
    /// is raw-path; schema-aware binds layer on top (W209 § Decided).
    #[serde(default)]
    pub schema: Option<String>,
}

/// One bind result, surfaced to the runner so callers (hash-change hooks,
/// qed-run tile) can react.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedBind {
    pub file: PathBuf,
    pub path: String,
    pub from: String,
    /// Previous value at `path`, if any.
    pub old: Option<String>,
    /// Value after the bind.
    pub new: String,
    /// True iff the write actually changed bytes on disk.
    pub changed: bool,
    /// True when this bind's target escapes the publishing workspace root
    /// (mirrors [`BindSpec::cross_workspace`]). Surfaced to the qed-run tile
    /// (R510-F7) so cross-workspace writes get a distinct confirmation
    /// affordance. `#[serde(default)]` keeps pre-R510-F7 run journals
    /// deserializable.
    #[serde(default)]
    pub cross_workspace: bool,
}

#[derive(Debug, Error)]
pub enum BindError {
    #[error("io error on {file}: {source}")]
    Io {
        file: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse error on {file}: {detail}")]
    Parse { file: PathBuf, detail: String },
    #[error("cannot resolve path {path:?} in {file}: {detail}")]
    PathResolve {
        file: PathBuf,
        path: String,
        detail: String,
    },
    #[error(
        "output {from} failed type validation before binding to {file}:{path} — {detail}"
    )]
    OutputTypeMismatch {
        from: String,
        file: PathBuf,
        path: String,
        detail: String,
    },
    #[error("unknown manifest kind for {file} (extension {ext:?})")]
    UnknownManifestKind { file: PathBuf, ext: String },
    #[error("manifest kind {kind} not yet implemented for {file}")]
    Unimplemented { file: PathBuf, kind: String },
}
