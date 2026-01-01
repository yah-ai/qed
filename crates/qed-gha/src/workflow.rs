//! Post-parse workflow types — the surface F2 (expr) and F3 (graph) build on.
//!
//! These are intentionally not raw `serde_yaml::Value` blobs: every field the
//! W200 scope (`release.yml` audit) needs is named here. Fields outside that
//! scope are tolerated by the parser but dropped; we'd rather grow the type
//! when a real workflow stretches the surface than silently lose data.

use indexmap::IndexMap;
use serde::Serialize;
use serde_yaml::Value;

use crate::expr_str::ExprString;

/// A parsed GitHub Actions workflow.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Workflow {
    pub name: Option<String>,
    pub triggers: Triggers,
    pub permissions: Option<Permissions>,
    pub env: IndexMap<String, ExprString>,
    pub concurrency: Option<Concurrency>,
    pub defaults: Option<Defaults>,
    /// Insertion-ordered: GHA's `needs:` resolution doesn't care about
    /// declaration order, but error messages and diff-friendliness do.
    pub jobs: IndexMap<String, Job>,
}

/// Workflow-level `on:` triggers. The YAML shape varies wildly (string, list,
/// or map); the parser normalizes to this struct.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Triggers {
    pub push: Option<PushTrigger>,
    pub pull_request: Option<PullRequestTrigger>,
    pub workflow_dispatch: Option<WorkflowDispatch>,
    pub workflow_call: Option<WorkflowCall>,
    pub schedule: Vec<ScheduleEntry>,
    /// Anything outside the W200 audit (e.g. `repository_dispatch`, `release`).
    /// Preserved as raw YAML so a future phase can wire it through without a
    /// data loss step.
    pub other: IndexMap<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct PushTrigger {
    pub branches: Vec<String>,
    pub branches_ignore: Vec<String>,
    pub tags: Vec<String>,
    pub tags_ignore: Vec<String>,
    pub paths: Vec<String>,
    pub paths_ignore: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct PullRequestTrigger {
    pub branches: Vec<String>,
    pub paths: Vec<String>,
    pub types: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct WorkflowDispatch {
    pub inputs: IndexMap<String, WorkflowInput>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct WorkflowCall {
    pub inputs: IndexMap<String, WorkflowInput>,
    pub outputs: IndexMap<String, WorkflowOutput>,
    pub secrets: IndexMap<String, WorkflowSecret>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct WorkflowInput {
    pub description: Option<String>,
    pub required: Option<bool>,
    pub default: Option<ExprString>,
    pub r#type: Option<String>,
    pub options: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct WorkflowOutput {
    pub description: Option<String>,
    pub value: Option<ExprString>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct WorkflowSecret {
    pub description: Option<String>,
    pub required: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScheduleEntry {
    pub cron: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Permissions {
    /// `permissions: read-all` / `permissions: write-all` / `permissions: {}`
    All(String),
    /// Per-scope map (`contents: read`, `id-token: write`, ...).
    Scopes(IndexMap<String, String>),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Concurrency {
    pub group: ExprString,
    pub cancel_in_progress: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Defaults {
    pub run: Option<RunDefaults>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunDefaults {
    pub shell: Option<String>,
    pub working_directory: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Job {
    pub name: Option<ExprString>,
    pub runs_on: RunsOn,
    pub needs: Vec<String>,
    pub if_cond: Option<ExprString>,
    pub permissions: Option<Permissions>,
    pub outputs: IndexMap<String, ExprString>,
    pub env: IndexMap<String, ExprString>,
    pub strategy: Option<Strategy>,
    pub timeout_minutes: Option<u32>,
    pub continue_on_error: Option<bool>,
    pub defaults: Option<Defaults>,
    pub steps: Vec<Step>,
}

/// `runs-on:` value. A literal string (`ubuntu-latest`), an expression
/// (`${{ matrix.os }}`), or a list (group-runner shape: `runs-on: [self-hosted, linux]`).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum RunsOn {
    Label(ExprString),
    Group(Vec<ExprString>),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Strategy {
    pub fail_fast: Option<bool>,
    pub max_parallel: Option<u32>,
    pub matrix: Option<Matrix>,
}

/// `strategy.matrix:` — F1 keeps raw `Value`s for dimension entries and
/// include/exclude rows. F3 will normalize this into actual matrix rows.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Matrix {
    pub dimensions: IndexMap<String, Vec<Value>>,
    pub include: Vec<IndexMap<String, Value>>,
    pub exclude: Vec<IndexMap<String, Value>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Step {
    pub id: Option<String>,
    pub name: Option<ExprString>,
    pub if_cond: Option<ExprString>,
    pub env: IndexMap<String, ExprString>,
    pub continue_on_error: Option<bool>,
    pub timeout_minutes: Option<u32>,
    pub working_directory: Option<ExprString>,
    pub action: StepAction,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum StepAction {
    /// `uses: org/repo/path@ref` plus a `with:` input map.
    Uses {
        /// `org/repo` or `org/repo/sub/path`. The `@ref` suffix is split off.
        slug: String,
        /// Whatever followed `@` — a tag, branch, sha, or `main`.
        git_ref: Option<String>,
        with: IndexMap<String, ExprString>,
    },
    /// `run:` (a literal shell script — usually bash).
    Run {
        body: ExprString,
        shell: Option<String>,
    },
}
