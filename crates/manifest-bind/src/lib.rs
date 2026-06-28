//! Pipeline outputs → manifest bind (W209).
//!
//! The applier evaluates a predicate against each producer's current output,
//! picks a winner (or not), and binds the hash into a checked-in manifest
//! file. Per-file transactional writes, idempotent on no-op.
//!
//! @arch:see(.yah/docs/working/W209-pipeline-output-manifest-bind.md)

mod intent;
mod on_change;
mod path_resolver;
mod types;
mod value_type;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub use intent::{Intent, IntentKeyword, IntentTabular};
pub use on_change::{
    dispatch_hook, fired_hooks, FiredHook, HookOutcome, OnChangeAction, OnChangeHook,
    HOOK_EVENTS_JOURNAL,
};
pub use types::{AppliedBind, BindError, BindSpec, OutputMap, OutputValue, OutputRef};
pub use value_type::ValueType;

/// Evaluate every bind whose `from` resolves in `outputs`, write results to
/// disk as one transaction per file, and report what changed.
///
/// Binds whose producer is not yet present in `outputs` are skipped (their
/// step has not run / not succeeded yet); they are not an error.
pub fn apply_binds(
    outputs: &OutputMap,
    binds: &[BindSpec],
    workspace_root: &Path,
) -> Result<Vec<AppliedBind>, BindError> {
    // Group binds by file so each file is rewritten in one transaction.
    let mut by_file: HashMap<PathBuf, Vec<&BindSpec>> = HashMap::new();
    for bind in binds {
        let Some(value) = outputs.lookup(&bind.from) else {
            continue;
        };
        // Type-check at the boundary — a producer that emits the wrong
        // shape never reaches a bind target.
        if let Err(e) = value.validate_type() {
            return Err(BindError::OutputTypeMismatch {
                from: bind.from.to_string(),
                file: bind.file.clone(),
                path: bind.path.clone(),
                detail: e,
            });
        }
        let abs = workspace_root.join(&bind.file);
        by_file.entry(abs).or_default().push(bind);
    }

    let mut applied = Vec::new();
    for (abs_file, file_binds) in by_file {
        apply_file_transaction(&abs_file, &file_binds, outputs, &mut applied)?;
    }
    Ok(applied)
}

fn apply_file_transaction(
    abs_file: &Path,
    binds: &[&BindSpec],
    outputs: &OutputMap,
    applied: &mut Vec<AppliedBind>,
) -> Result<(), BindError> {
    // v1 ships TOML; JSON/YAML are scaffolded as TODO until a real case demands them.
    let kind = ManifestKind::detect(abs_file)?;
    let text = fs::read_to_string(abs_file).map_err(|e| BindError::Io {
        file: abs_file.to_path_buf(),
        source: e,
    })?;

    let mut staged: Vec<AppliedBind> = Vec::with_capacity(binds.len());
    let new_text = match kind {
        ManifestKind::Toml => {
            let mut doc: toml_edit::DocumentMut =
                text.parse().map_err(|e: toml_edit::TomlError| BindError::Parse {
                    file: abs_file.to_path_buf(),
                    detail: e.to_string(),
                })?;
            for bind in binds {
                let value = outputs
                    .lookup(&bind.from)
                    .expect("filtered in apply_binds above");
                if !bind.intent.accepts(value) {
                    // Predicate rejected; no write, no AppliedBind entry.
                    continue;
                }
                let new_value = value.as_str().to_owned();
                let old = path_resolver::toml_get(&doc, &bind.path).map_err(|e| {
                    BindError::PathResolve {
                        file: abs_file.to_path_buf(),
                        path: bind.path.clone(),
                        detail: e,
                    }
                })?;
                let changed = old.as_deref() != Some(new_value.as_str());
                if changed {
                    path_resolver::toml_set(&mut doc, &bind.path, &new_value).map_err(|e| {
                        BindError::PathResolve {
                            file: abs_file.to_path_buf(),
                            path: bind.path.clone(),
                            detail: e,
                        }
                    })?;
                }
                staged.push(AppliedBind {
                    file: bind.file.clone(),
                    path: bind.path.clone(),
                    from: bind.from.to_string(),
                    old,
                    new: new_value,
                    changed,
                    cross_workspace: bind.cross_workspace,
                });
            }
            doc.to_string()
        }
        ManifestKind::Json | ManifestKind::Yaml => {
            return Err(BindError::Unimplemented {
                file: abs_file.to_path_buf(),
                kind: format!("{kind:?}"),
            });
        }
    };

    // Skip the disk write if nothing actually changed across all binds in this file.
    if staged.iter().any(|a| a.changed) {
        write_atomic(abs_file, &new_text).map_err(|e| BindError::Io {
            file: abs_file.to_path_buf(),
            source: e,
        })?;
    }
    applied.extend(staged);
    Ok(())
}

fn write_atomic(abs_file: &Path, contents: &str) -> std::io::Result<()> {
    let tmp = abs_file.with_extension(format!(
        "{}.bind-tmp",
        abs_file
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("tmp")
    ));
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(contents.as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, abs_file)?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ManifestKind {
    Toml,
    Json,
    Yaml,
}

impl ManifestKind {
    fn detect(path: &Path) -> Result<Self, BindError> {
        match path.extension().and_then(|s| s.to_str()) {
            Some("toml") => Ok(Self::Toml),
            Some("json") => Ok(Self::Json),
            Some("yaml") | Some("yml") => Ok(Self::Yaml),
            other => Err(BindError::UnknownManifestKind {
                file: path.to_path_buf(),
                ext: other.unwrap_or("").to_owned(),
            }),
        }
    }
}
