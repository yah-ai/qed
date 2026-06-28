//! `workflow_call` port contract (R533-F5, W224).
//!
//! W224 settles the nesting boundary by *not inventing one*: a reusable GitHub
//! Actions workflow declares `on: workflow_call: { inputs, outputs, secrets }`,
//! and that **is** a typed module boundary. An imported workflow is therefore a
//! **black-box subgraph with ports**, not a flattened step list — internally it
//! keeps its own jobs/matrix/DAG, but QED sees one node with typed inputs and
//! outputs, indistinguishable at the boundary from a native step.
//!
//! This module extracts that contract from a parsed [`Workflow`]:
//!
//! - **down-port (QED → workflow):** the caller's prior content-addressed
//!   artifacts + env + secrets feed the workflow's `workflow_call` inputs and
//!   secrets. Secrets land as **env injection** — there is no keystore-of-GitHub
//!   to reproduce. [`WorkflowPorts::resolve_inputs`] validates required inputs
//!   and applies declared defaults.
//! - **up-port (workflow → QED):** the workflow's declared `outputs:` surface as
//!   **content-addressed QED artifacts** that downstream native steps consume via
//!   a normal `needs:` edge ([`WorkflowPorts::output_artifacts`]).
//! - **explicit tier-3 boundary declaration:** the one thing the boundary must
//!   *additionally* state is which tier-3 facilities the nested box assumes (an
//!   inner `checkout` / `upload-artifact` needs the native substitute when run on
//!   QED). [`WorkflowPorts::tier3_assumptions`] is that declaration, computed from
//!   the R533-F2 classifier scoped to the box — so "runs on GHA today, runs on QED
//!   tomorrow" stays honest instead of failing mysteriously inside the black box.
//!
//! This is W201's `SubPipelineRef::GhaWorkflow` with the port contract made
//! explicit; the module is pure (no I/O) and operates on the parsed workflow.

use std::collections::HashMap;

use crate::transform::render_exprstring;
use yah_qed_gha::{classify_workflow, Disposition, NativeReplacement, Workflow};

/// The typed boundary of an imported workflow — its declared ports plus the
/// tier-3 facilities its body assumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowPorts {
    /// Whether the workflow declares `on: workflow_call` — i.e. is a *reusable*
    /// module with an explicit boundary. A top-level workflow imported directly
    /// has no declared ports (`false`); only its tier-3 assumptions are
    /// meaningful.
    pub reusable: bool,
    /// down-port inputs (`workflow_call.inputs`), in declaration order.
    pub inputs: Vec<PortInput>,
    /// down-port secrets (`workflow_call.secrets`) — injected as env at the box.
    pub secrets: Vec<PortSecret>,
    /// up-port outputs (`workflow_call.outputs`) — each a content-addressed
    /// artifact downstream native steps consume via `needs:`.
    pub outputs: Vec<PortOutput>,
    /// The distinct tier-3 facilities the nested box assumes, in first-seen
    /// order — the explicit boundary declaration W224 requires.
    pub tier3_assumptions: Vec<NativeReplacement>,
}

/// A `workflow_call` input — a typed down-port the caller must satisfy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortInput {
    pub name: String,
    pub required: bool,
    /// Declared `type:` (`string` / `boolean` / `number`), if any.
    pub ty: Option<String>,
    /// Rendered default value (expressions preserved), if any.
    pub default: Option<String>,
    pub description: Option<String>,
}

/// A `workflow_call` secret — injected into the box as an environment variable
/// (the W224 "secrets are ENV-injected" convention; the env var is the secret's
/// own name, the form `${{ secrets.NAME }}` expands to).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortSecret {
    pub name: String,
    pub required: bool,
    /// The environment variable the secret is injected as at the boundary.
    pub env_var: String,
    pub description: Option<String>,
}

/// A `workflow_call` output — an up-port surfaced as a content-addressed QED
/// artifact downstream native steps reference via `needs:`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortOutput {
    pub name: String,
    /// The content-addressed artifact id this output surfaces as (the output's
    /// own name — downstream `needs:` names this).
    pub artifact: String,
    /// The rendered value expression backing the output
    /// (`${{ jobs.build.outputs.digest }}`), if declared.
    pub value: Option<String>,
    pub description: Option<String>,
}

/// Inputs that could not be resolved against the boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortError {
    /// Required inputs with no supplied value and no default.
    MissingRequired(Vec<String>),
}

impl WorkflowPorts {
    /// Resolve the down-port: validate that every required input is supplied (or
    /// has a default), and return the effective input map (supplied wins over
    /// default). Unknown supplied keys are ignored — extra context is harmless.
    pub fn resolve_inputs(
        &self,
        supplied: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>, PortError> {
        let mut resolved = HashMap::new();
        let mut missing = Vec::new();
        for input in &self.inputs {
            if let Some(v) = supplied.get(&input.name) {
                resolved.insert(input.name.clone(), v.clone());
            } else if let Some(d) = &input.default {
                resolved.insert(input.name.clone(), d.clone());
            } else if input.required {
                missing.push(input.name.clone());
            }
        }
        if missing.is_empty() {
            Ok(resolved)
        } else {
            Err(PortError::MissingRequired(missing))
        }
    }

    /// The up-port: outputs as content-addressed artifacts downstream consumes.
    pub fn output_artifacts(&self) -> impl Iterator<Item = &PortOutput> {
        self.outputs.iter()
    }

    /// The env-injection map for the down-port secrets (`env_var → secret name`).
    pub fn secret_env(&self) -> HashMap<String, String> {
        self.secrets.iter().map(|s| (s.env_var.clone(), s.name.clone())).collect()
    }

    /// Whether the box assumes a given tier-3 facility — the boundary needs its
    /// native substitute before the import can run on QED.
    pub fn assumes(&self, facility: NativeReplacement) -> bool {
        self.tier3_assumptions.contains(&facility)
    }
}

/// Extract the `workflow_call` port contract from a parsed workflow.
///
/// Always returns a [`WorkflowPorts`]: when the workflow declares no
/// `on: workflow_call`, the declared-port lists are empty (`reusable = false`)
/// but [`tier3_assumptions`](WorkflowPorts::tier3_assumptions) is still computed
/// from the body, since every imported workflow has a tier-3 boundary.
pub fn workflow_ports(wf: &Workflow) -> WorkflowPorts {
    let call = wf.triggers.workflow_call.as_ref();
    let reusable = call.is_some();

    let inputs = call
        .map(|c| {
            c.inputs
                .iter()
                .map(|(name, i)| PortInput {
                    name: name.clone(),
                    required: i.required.unwrap_or(false),
                    ty: i.r#type.clone(),
                    default: i.default.as_ref().map(render_exprstring),
                    description: i.description.clone(),
                })
                .collect()
        })
        .unwrap_or_default();

    let secrets = call
        .map(|c| {
            c.secrets
                .iter()
                .map(|(name, s)| PortSecret {
                    name: name.clone(),
                    required: s.required.unwrap_or(false),
                    env_var: name.clone(),
                    description: s.description.clone(),
                })
                .collect()
        })
        .unwrap_or_default();

    let outputs = call
        .map(|c| {
            c.outputs
                .iter()
                .map(|(name, o)| PortOutput {
                    name: name.clone(),
                    artifact: name.clone(),
                    value: o.value.as_ref().map(render_exprstring),
                    description: o.description.clone(),
                })
                .collect()
        })
        .unwrap_or_default();

    WorkflowPorts { reusable, inputs, secrets, outputs, tier3_assumptions: tier3_assumptions(wf) }
}

/// The distinct tier-3 facilities a workflow's steps assume, in first-seen
/// order — the explicit boundary declaration. Computed from the R533-F2
/// classifier over every step in the box.
fn tier3_assumptions(wf: &Workflow) -> Vec<NativeReplacement> {
    let mut out: Vec<NativeReplacement> = Vec::new();
    for classified in classify_workflow(wf) {
        if let Disposition::ReplaceWithNative(nr) = classified.class.disposition {
            if !out.contains(&nr) {
                out.push(nr);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wf(src: &str) -> Workflow {
        yah_qed_gha::parse_workflow(src).expect("parse")
    }

    const REUSABLE: &str = r#"
name: build-and-publish
on:
  workflow_call:
    inputs:
      tag:
        required: true
        type: string
      channel:
        required: false
        type: string
        default: stable
    secrets:
      CARGO_TOKEN:
        required: true
    outputs:
      digest:
        description: the built image digest
        value: ${{ jobs.build.outputs.digest }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/upload-artifact@v4
      - run: cargo build --release
"#;

    #[test]
    fn reusable_workflow_declares_typed_ports() {
        let p = workflow_ports(&wf(REUSABLE));
        assert!(p.reusable);

        // down-port inputs
        assert_eq!(p.inputs.len(), 2);
        let tag = &p.inputs[0];
        assert_eq!(tag.name, "tag");
        assert!(tag.required);
        assert_eq!(tag.ty.as_deref(), Some("string"));
        let channel = &p.inputs[1];
        assert!(!channel.required);
        assert_eq!(channel.default.as_deref(), Some("stable"));

        // down-port secrets → env injection
        assert_eq!(p.secrets.len(), 1);
        assert_eq!(p.secrets[0].name, "CARGO_TOKEN");
        assert_eq!(p.secrets[0].env_var, "CARGO_TOKEN");
        assert!(p.secrets[0].required);

        // up-port outputs → content-addressed artifacts
        assert_eq!(p.outputs.len(), 1);
        assert_eq!(p.outputs[0].name, "digest");
        assert_eq!(p.outputs[0].artifact, "digest");
        assert_eq!(p.outputs[0].value.as_deref(), Some("${{ jobs.build.outputs.digest }}"));
    }

    #[test]
    fn down_port_resolves_inputs_with_defaults_and_required() {
        let p = workflow_ports(&wf(REUSABLE));

        // Missing the required `tag` → error naming it.
        let err = p.resolve_inputs(&HashMap::new()).unwrap_err();
        assert_eq!(err, PortError::MissingRequired(vec!["tag".into()]));

        // Supplying `tag` → `channel` falls back to its default.
        let supplied = HashMap::from([("tag".to_string(), "v1.2.3".to_string())]);
        let resolved = p.resolve_inputs(&supplied).expect("resolves");
        assert_eq!(resolved.get("tag").map(String::as_str), Some("v1.2.3"));
        assert_eq!(resolved.get("channel").map(String::as_str), Some("stable"));
    }

    #[test]
    fn secret_env_injection_map() {
        let p = workflow_ports(&wf(REUSABLE));
        let env = p.secret_env();
        assert_eq!(env.get("CARGO_TOKEN").map(String::as_str), Some("CARGO_TOKEN"));
    }

    #[test]
    fn explicit_tier3_boundary_declaration() {
        let p = workflow_ports(&wf(REUSABLE));
        // checkout + upload-artifact are the tier-3 facilities the box assumes.
        assert!(p.assumes(NativeReplacement::Checkout));
        assert!(p.assumes(NativeReplacement::UploadArtifact));
        assert!(!p.assumes(NativeReplacement::ReleasePublisher));
        // Distinct + first-seen order, no duplicates.
        assert_eq!(
            p.tier3_assumptions,
            vec![NativeReplacement::Checkout, NativeReplacement::UploadArtifact]
        );
    }

    #[test]
    fn non_reusable_workflow_has_no_ports_but_keeps_tier3_declaration() {
        let src = r#"
on: push
jobs:
  a:
    runs-on: x
    steps:
      - uses: actions/checkout@v4
      - run: make
"#;
        let p = workflow_ports(&wf(src));
        assert!(!p.reusable);
        assert!(p.inputs.is_empty());
        assert!(p.secrets.is_empty());
        assert!(p.outputs.is_empty());
        // A top-level import still declares its tier-3 boundary.
        assert_eq!(p.tier3_assumptions, vec![NativeReplacement::Checkout]);
    }

    #[test]
    fn tier3_assumptions_dedupe_across_jobs() {
        // checkout in two jobs → declared once.
        let src = r#"
on: workflow_call
jobs:
  a:
    runs-on: x
    steps:
      - uses: actions/checkout@v4
  b:
    runs-on: x
    steps:
      - uses: actions/checkout@v4
      - uses: actions/cache@v4
"#;
        let p = workflow_ports(&wf(src));
        assert_eq!(
            p.tier3_assumptions,
            vec![NativeReplacement::Checkout, NativeReplacement::ContentAddressedCache]
        );
    }

    #[test]
    fn output_artifacts_iterator_exposes_up_port() {
        let p = workflow_ports(&wf(REUSABLE));
        let arts: Vec<&str> = p.output_artifacts().map(|o| o.artifact.as_str()).collect();
        assert_eq!(arts, vec!["digest"]);
    }
}
