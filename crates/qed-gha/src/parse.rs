//! YAML → [`Workflow`] parsing.
//!
//! F1 does the structural conversion. Each named field on the raw shape lands
//! in a strongly typed slot on `Workflow`/`Job`/`Step`; the few keys that have
//! a "string or list or map" YAML idiom (notably `on:`, `runs-on:`, `needs:`,
//! `permissions:`) get a per-key normalizer. Unknown keys at the same level
//! are dropped — F1 is a scaffold, not a strict schema validator.

use indexmap::IndexMap;
use serde_yaml::{Mapping, Value};
use thiserror::Error;

use crate::expr_str::ExprString;
use crate::workflow::*;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("workflow root must be a mapping")]
    NotMapping,
    #[error("missing required key `{0}`")]
    MissingKey(&'static str),
    #[error("expected {expected} for key `{key}`, got {got}")]
    TypeMismatch {
        key: &'static str,
        expected: &'static str,
        got: &'static str,
    },
}

pub fn parse_workflow(yaml: &str) -> Result<Workflow, ParseError> {
    let root: Value = serde_yaml::from_str(yaml)?;
    let map = root.as_mapping().ok_or(ParseError::NotMapping)?;

    let name = take_str(map, "name");
    let triggers = match take_value(map, "on") {
        Some(v) => parse_triggers(&v)?,
        None => Triggers::default(),
    };
    let permissions = take_value(map, "permissions").map(|v| parse_permissions(&v)).transpose()?;
    let env = take_value(map, "env").map(|v| parse_expr_map("env", &v)).transpose()?.unwrap_or_default();
    let concurrency = take_value(map, "concurrency").map(|v| parse_concurrency(&v)).transpose()?;
    let defaults = take_value(map, "defaults").map(|v| parse_defaults(&v)).transpose()?;

    let jobs_val = take_value(map, "jobs").ok_or(ParseError::MissingKey("jobs"))?;
    let jobs = parse_jobs(&jobs_val)?;

    Ok(Workflow {
        name,
        triggers,
        permissions,
        env,
        concurrency,
        defaults,
        jobs,
    })
}

// ─── primitives ────────────────────────────────────────────────────────────

fn lookup<'a>(map: &'a Mapping, key: &str) -> Option<&'a Value> {
    map.get(Value::String(key.to_string()))
}

fn take_value(map: &Mapping, key: &str) -> Option<Value> {
    lookup(map, key).cloned()
}

fn take_str(map: &Mapping, key: &str) -> Option<String> {
    lookup(map, key).and_then(|v| v.as_str().map(|s| s.to_string()))
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Sequence(_) => "sequence",
        Value::Mapping(_) => "mapping",
        Value::Tagged(_) => "tagged",
    }
}

fn expr_from_scalar(v: &Value) -> ExprString {
    match v {
        Value::Null => ExprString::default(),
        Value::Bool(b) => ExprString::literal(b.to_string()),
        Value::Number(n) => ExprString::literal(n.to_string()),
        Value::String(s) => ExprString::parse(s),
        // Multi-line block scalars surface as plain strings — handled above.
        // Sequences and mappings shouldn't appear where an expression scalar is
        // expected; fall back to a YAML-rendered literal so we don't panic.
        other => ExprString::literal(serde_yaml::to_string(other).unwrap_or_default()),
    }
}

fn parse_string_list(key: &'static str, v: &Value) -> Result<Vec<String>, ParseError> {
    match v {
        Value::Null => Ok(vec![]),
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Sequence(seq) => seq
            .iter()
            .map(|e| match e {
                Value::String(s) => Ok(s.clone()),
                Value::Bool(b) => Ok(b.to_string()),
                Value::Number(n) => Ok(n.to_string()),
                _ => Err(ParseError::TypeMismatch {
                    key,
                    expected: "string",
                    got: type_name(e),
                }),
            })
            .collect(),
        _ => Err(ParseError::TypeMismatch {
            key,
            expected: "string or list of strings",
            got: type_name(v),
        }),
    }
}

fn parse_expr_map(key: &'static str, v: &Value) -> Result<IndexMap<String, ExprString>, ParseError> {
    let map = v.as_mapping().ok_or(ParseError::TypeMismatch {
        key,
        expected: "mapping",
        got: type_name(v),
    })?;
    let mut out = IndexMap::with_capacity(map.len());
    for (k, val) in map {
        let Some(k) = k.as_str() else { continue };
        out.insert(k.to_string(), expr_from_scalar(val));
    }
    Ok(out)
}

fn parse_string_map(key: &'static str, v: &Value) -> Result<IndexMap<String, String>, ParseError> {
    let map = v.as_mapping().ok_or(ParseError::TypeMismatch {
        key,
        expected: "mapping",
        got: type_name(v),
    })?;
    let mut out = IndexMap::with_capacity(map.len());
    for (k, val) in map {
        let Some(k) = k.as_str() else { continue };
        let s = match val {
            Value::String(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            Value::Null => String::new(),
            _ => continue,
        };
        out.insert(k.to_string(), s);
    }
    Ok(out)
}

// ─── triggers ──────────────────────────────────────────────────────────────

fn parse_triggers(v: &Value) -> Result<Triggers, ParseError> {
    let mut out = Triggers::default();
    match v {
        Value::String(s) => {
            apply_simple_trigger(&mut out, s);
            return Ok(out);
        }
        Value::Sequence(seq) => {
            for entry in seq {
                if let Some(s) = entry.as_str() {
                    apply_simple_trigger(&mut out, s);
                }
            }
            return Ok(out);
        }
        Value::Mapping(map) => {
            for (k, val) in map {
                let Some(k) = k.as_str() else { continue };
                match k {
                    "push" => out.push = Some(parse_push(val)?),
                    "pull_request" => out.pull_request = Some(parse_pull_request(val)?),
                    "workflow_dispatch" => out.workflow_dispatch = Some(parse_workflow_dispatch(val)?),
                    "workflow_call" => out.workflow_call = Some(parse_workflow_call(val)?),
                    "schedule" => out.schedule = parse_schedule(val)?,
                    _ => {
                        out.other.insert(k.to_string(), val.clone());
                    }
                }
            }
        }
        Value::Null => {}
        _ => {
            return Err(ParseError::TypeMismatch {
                key: "on",
                expected: "string, list, or mapping",
                got: type_name(v),
            });
        }
    }
    Ok(out)
}

fn apply_simple_trigger(out: &mut Triggers, name: &str) {
    match name {
        "push" => out.push = Some(PushTrigger::default()),
        "pull_request" => out.pull_request = Some(PullRequestTrigger::default()),
        "workflow_dispatch" => out.workflow_dispatch = Some(WorkflowDispatch::default()),
        "workflow_call" => out.workflow_call = Some(WorkflowCall::default()),
        other => {
            out.other.insert(other.to_string(), Value::Null);
        }
    }
}

fn parse_push(v: &Value) -> Result<PushTrigger, ParseError> {
    let mut out = PushTrigger::default();
    let Some(map) = as_mapping_or_null(v) else { return Ok(out) };
    if let Some(b) = lookup(map, "branches") { out.branches = parse_string_list("push.branches", b)?; }
    if let Some(b) = lookup(map, "branches-ignore") { out.branches_ignore = parse_string_list("push.branches-ignore", b)?; }
    if let Some(t) = lookup(map, "tags") { out.tags = parse_string_list("push.tags", t)?; }
    if let Some(t) = lookup(map, "tags-ignore") { out.tags_ignore = parse_string_list("push.tags-ignore", t)?; }
    if let Some(p) = lookup(map, "paths") { out.paths = parse_string_list("push.paths", p)?; }
    if let Some(p) = lookup(map, "paths-ignore") { out.paths_ignore = parse_string_list("push.paths-ignore", p)?; }
    Ok(out)
}

fn parse_pull_request(v: &Value) -> Result<PullRequestTrigger, ParseError> {
    let mut out = PullRequestTrigger::default();
    let Some(map) = as_mapping_or_null(v) else { return Ok(out) };
    if let Some(b) = lookup(map, "branches") { out.branches = parse_string_list("pull_request.branches", b)?; }
    if let Some(p) = lookup(map, "paths") { out.paths = parse_string_list("pull_request.paths", p)?; }
    if let Some(t) = lookup(map, "types") { out.types = parse_string_list("pull_request.types", t)?; }
    Ok(out)
}

fn parse_workflow_dispatch(v: &Value) -> Result<WorkflowDispatch, ParseError> {
    let mut out = WorkflowDispatch::default();
    let Some(map) = as_mapping_or_null(v) else { return Ok(out) };
    if let Some(inputs) = lookup(map, "inputs") {
        let inputs_map = inputs.as_mapping().ok_or(ParseError::TypeMismatch {
            key: "workflow_dispatch.inputs",
            expected: "mapping",
            got: type_name(inputs),
        })?;
        for (k, val) in inputs_map {
            let Some(k) = k.as_str() else { continue };
            out.inputs.insert(k.to_string(), parse_workflow_input(val)?);
        }
    }
    Ok(out)
}

fn parse_workflow_input(v: &Value) -> Result<WorkflowInput, ParseError> {
    let mut out = WorkflowInput::default();
    let Some(map) = as_mapping_or_null(v) else { return Ok(out) };
    out.description = take_str(map, "description");
    out.required = lookup(map, "required").and_then(|v| v.as_bool());
    out.default = lookup(map, "default").map(expr_from_scalar);
    out.r#type = take_str(map, "type");
    if let Some(opts) = lookup(map, "options") {
        out.options = parse_string_list("input.options", opts)?;
    }
    Ok(out)
}

fn parse_workflow_call(v: &Value) -> Result<WorkflowCall, ParseError> {
    let mut out = WorkflowCall::default();
    let Some(map) = as_mapping_or_null(v) else { return Ok(out) };
    if let Some(inputs) = lookup(map, "inputs") {
        if let Some(inputs_map) = inputs.as_mapping() {
            for (k, val) in inputs_map {
                let Some(k) = k.as_str() else { continue };
                out.inputs.insert(k.to_string(), parse_workflow_input(val)?);
            }
        }
    }
    if let Some(outputs) = lookup(map, "outputs") {
        if let Some(outputs_map) = outputs.as_mapping() {
            for (k, val) in outputs_map {
                let Some(k) = k.as_str() else { continue };
                let mut item = WorkflowOutput::default();
                if let Some(m) = as_mapping_or_null(val) {
                    item.description = take_str(m, "description");
                    item.value = lookup(m, "value").map(expr_from_scalar);
                }
                out.outputs.insert(k.to_string(), item);
            }
        }
    }
    if let Some(secrets) = lookup(map, "secrets") {
        if let Some(secrets_map) = secrets.as_mapping() {
            for (k, val) in secrets_map {
                let Some(k) = k.as_str() else { continue };
                let mut item = WorkflowSecret::default();
                if let Some(m) = as_mapping_or_null(val) {
                    item.description = take_str(m, "description");
                    item.required = lookup(m, "required").and_then(|v| v.as_bool());
                }
                out.secrets.insert(k.to_string(), item);
            }
        }
    }
    Ok(out)
}

fn parse_schedule(v: &Value) -> Result<Vec<ScheduleEntry>, ParseError> {
    let seq = v.as_sequence().ok_or(ParseError::TypeMismatch {
        key: "schedule",
        expected: "sequence",
        got: type_name(v),
    })?;
    let mut out = Vec::with_capacity(seq.len());
    for entry in seq {
        let Some(map) = entry.as_mapping() else { continue };
        if let Some(cron) = lookup(map, "cron").and_then(|v| v.as_str()) {
            out.push(ScheduleEntry { cron: cron.to_string() });
        }
    }
    Ok(out)
}

fn as_mapping_or_null(v: &Value) -> Option<&Mapping> {
    match v {
        Value::Mapping(m) => Some(m),
        // `workflow_dispatch:` with no body is a YAML null, which means "use
        // defaults for this trigger" — callers treat None as default.
        _ => None,
    }
}

// ─── permissions / concurrency / defaults ──────────────────────────────────

fn parse_permissions(v: &Value) -> Result<Permissions, ParseError> {
    match v {
        Value::String(s) => Ok(Permissions::All(s.clone())),
        Value::Mapping(_) => Ok(Permissions::Scopes(parse_string_map("permissions", v)?)),
        Value::Null => Ok(Permissions::Scopes(IndexMap::new())),
        _ => Err(ParseError::TypeMismatch {
            key: "permissions",
            expected: "string or mapping",
            got: type_name(v),
        }),
    }
}

fn parse_concurrency(v: &Value) -> Result<Concurrency, ParseError> {
    match v {
        Value::String(s) => Ok(Concurrency {
            group: ExprString::parse(s),
            cancel_in_progress: None,
        }),
        Value::Mapping(_) => {
            let map = v.as_mapping().unwrap();
            let group = lookup(map, "group")
                .map(expr_from_scalar)
                .ok_or(ParseError::MissingKey("concurrency.group"))?;
            let cancel_in_progress = lookup(map, "cancel-in-progress").and_then(|v| v.as_bool());
            Ok(Concurrency { group, cancel_in_progress })
        }
        _ => Err(ParseError::TypeMismatch {
            key: "concurrency",
            expected: "string or mapping",
            got: type_name(v),
        }),
    }
}

fn parse_defaults(v: &Value) -> Result<Defaults, ParseError> {
    let Some(map) = v.as_mapping() else {
        return Err(ParseError::TypeMismatch {
            key: "defaults",
            expected: "mapping",
            got: type_name(v),
        });
    };
    let run = lookup(map, "run").map(|v| {
        let m = v.as_mapping();
        RunDefaults {
            shell: m.and_then(|m| lookup(m, "shell")).and_then(|v| v.as_str().map(|s| s.to_string())),
            working_directory: m
                .and_then(|m| lookup(m, "working-directory"))
                .and_then(|v| v.as_str().map(|s| s.to_string())),
        }
    });
    Ok(Defaults { run })
}

// ─── jobs ──────────────────────────────────────────────────────────────────

fn parse_jobs(v: &Value) -> Result<IndexMap<String, Job>, ParseError> {
    let map = v.as_mapping().ok_or(ParseError::TypeMismatch {
        key: "jobs",
        expected: "mapping",
        got: type_name(v),
    })?;
    let mut out = IndexMap::with_capacity(map.len());
    for (k, val) in map {
        let Some(k) = k.as_str() else { continue };
        out.insert(k.to_string(), parse_job(k, val)?);
    }
    Ok(out)
}

fn parse_job(_id: &str, v: &Value) -> Result<Job, ParseError> {
    let map = v.as_mapping().ok_or(ParseError::TypeMismatch {
        key: "job",
        expected: "mapping",
        got: type_name(v),
    })?;

    let name = lookup(map, "name").map(expr_from_scalar);
    let runs_on = match lookup(map, "runs-on") {
        Some(Value::Sequence(seq)) => RunsOn::Group(seq.iter().map(expr_from_scalar).collect()),
        Some(other) => RunsOn::Label(expr_from_scalar(other)),
        None => RunsOn::Label(ExprString::default()),
    };
    let needs = lookup(map, "needs")
        .map(|v| parse_string_list("needs", v))
        .transpose()?
        .unwrap_or_default();
    let if_cond = lookup(map, "if").map(expr_from_scalar);
    let permissions = lookup(map, "permissions").map(parse_permissions).transpose()?;
    let outputs = lookup(map, "outputs").map(|v| parse_expr_map("outputs", v)).transpose()?.unwrap_or_default();
    let env = lookup(map, "env").map(|v| parse_expr_map("env", v)).transpose()?.unwrap_or_default();
    let strategy = lookup(map, "strategy").map(parse_strategy).transpose()?;
    let timeout_minutes = lookup(map, "timeout-minutes").and_then(|v| v.as_u64()).map(|n| n as u32);
    let continue_on_error = lookup(map, "continue-on-error").and_then(|v| v.as_bool());
    let defaults = lookup(map, "defaults").map(parse_defaults).transpose()?;

    let steps = match lookup(map, "steps") {
        Some(Value::Sequence(seq)) => seq.iter().map(parse_step).collect::<Result<Vec<_>, _>>()?,
        Some(Value::Null) | None => vec![],
        Some(other) => {
            return Err(ParseError::TypeMismatch {
                key: "steps",
                expected: "sequence",
                got: type_name(other),
            });
        }
    };

    Ok(Job {
        name,
        runs_on,
        needs,
        if_cond,
        permissions,
        outputs,
        env,
        strategy,
        timeout_minutes,
        continue_on_error,
        defaults,
        steps,
    })
}

fn parse_strategy(v: &Value) -> Result<Strategy, ParseError> {
    let Some(map) = v.as_mapping() else {
        return Err(ParseError::TypeMismatch {
            key: "strategy",
            expected: "mapping",
            got: type_name(v),
        });
    };
    let fail_fast = lookup(map, "fail-fast").and_then(|v| v.as_bool());
    let max_parallel = lookup(map, "max-parallel").and_then(|v| v.as_u64()).map(|n| n as u32);
    let matrix = lookup(map, "matrix").map(parse_matrix).transpose()?;
    Ok(Strategy { fail_fast, max_parallel, matrix })
}

fn parse_matrix(v: &Value) -> Result<Matrix, ParseError> {
    let map = v.as_mapping().ok_or(ParseError::TypeMismatch {
        key: "matrix",
        expected: "mapping",
        got: type_name(v),
    })?;
    let mut out = Matrix::default();
    for (k, val) in map {
        let Some(k) = k.as_str() else { continue };
        match k {
            "include" => {
                if let Some(seq) = val.as_sequence() {
                    out.include = seq.iter().map(value_to_index_map).collect();
                }
            }
            "exclude" => {
                if let Some(seq) = val.as_sequence() {
                    out.exclude = seq.iter().map(value_to_index_map).collect();
                }
            }
            // Anything else is a dimension: key → list of values.
            _ => {
                let values = match val {
                    Value::Sequence(seq) => seq.clone(),
                    other => vec![other.clone()],
                };
                out.dimensions.insert(k.to_string(), values);
            }
        }
    }
    Ok(out)
}

fn value_to_index_map(v: &Value) -> IndexMap<String, Value> {
    let mut out = IndexMap::new();
    let Some(map) = v.as_mapping() else { return out };
    for (k, val) in map {
        let Some(k) = k.as_str() else { continue };
        out.insert(k.to_string(), val.clone());
    }
    out
}

// ─── steps ─────────────────────────────────────────────────────────────────

fn parse_step(v: &Value) -> Result<Step, ParseError> {
    let map = v.as_mapping().ok_or(ParseError::TypeMismatch {
        key: "step",
        expected: "mapping",
        got: type_name(v),
    })?;

    let id = take_str(map, "id");
    let name = lookup(map, "name").map(expr_from_scalar);
    let if_cond = lookup(map, "if").map(expr_from_scalar);
    let env = lookup(map, "env").map(|v| parse_expr_map("step.env", v)).transpose()?.unwrap_or_default();
    let continue_on_error = lookup(map, "continue-on-error").and_then(|v| v.as_bool());
    let timeout_minutes = lookup(map, "timeout-minutes").and_then(|v| v.as_u64()).map(|n| n as u32);
    let working_directory = lookup(map, "working-directory").map(expr_from_scalar);

    let uses = lookup(map, "uses").and_then(|v| v.as_str().map(|s| s.to_string()));
    let run = lookup(map, "run").map(expr_from_scalar);
    let shell = take_str(map, "shell");

    let action = match (uses, run) {
        (Some(uses), _) => {
            let (slug, git_ref) = split_uses(&uses);
            let with = lookup(map, "with")
                .map(|v| parse_expr_map("with", v))
                .transpose()?
                .unwrap_or_default();
            StepAction::Uses { slug, git_ref, with }
        }
        (None, Some(body)) => StepAction::Run { body, shell },
        (None, None) => {
            return Err(ParseError::MissingKey("step.uses-or-run"));
        }
    };

    Ok(Step {
        id,
        name,
        if_cond,
        env,
        continue_on_error,
        timeout_minutes,
        working_directory,
        action,
    })
}

/// Split `org/repo/path@v3` into (`org/repo/path`, Some("v3")). A docker-image
/// uses (`docker://image:tag`) keeps the whole string as the slug, no ref.
fn split_uses(raw: &str) -> (String, Option<String>) {
    if raw.starts_with("docker://") {
        return (raw.to_string(), None);
    }
    match raw.rsplit_once('@') {
        Some((slug, gref)) => (slug.to_string(), Some(gref.to_string())),
        None => (raw.to_string(), None),
    }
}

