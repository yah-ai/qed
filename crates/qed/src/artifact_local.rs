//! Single-host artifact store for the qed-gha runtime (R594).
//!
//! Implements [`yah_qed_gha::ArtifactStore`] for `actions/upload-artifact` /
//! `actions/download-artifact` by copying into / out of a content-addressed
//! directory under the workspace (`.qed-artifacts/<name>/`). Restores the
//! R487-F6 behavior W224 retired, as an injected handler owned by the runner.
//!
//! This is the single-host case: uploads and downloads share one workspace, so
//! a job that uploads binaries and a later job that downloads them just move
//! files through the on-disk store. The fleet phase swaps in a transport-backed
//! store so a build-worker on another node can fetch an artifact produced here.

use std::ffi::OsStr;
use std::path::Path;

use indexmap::IndexMap;
use yah_qed_gha::{ArtifactCall, ArtifactStore, StepConclusion, ToolkitOutcome, Value};

/// Directory (workspace-relative) the artifacts live under.
const STORE_DIR: &str = ".qed-artifacts";

/// [`ArtifactStore`] backed by a workspace-local directory. Cheap to clone; no
/// state beyond the store-dir convention.
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalArtifactStore;

impl LocalArtifactStore {
    pub fn new() -> Self {
        Self
    }
}

impl ArtifactStore for LocalArtifactStore {
    /// `actions/upload-artifact` — copy each `with.path` line into
    /// `.qed-artifacts/<name>/`. A missing source path is a step failure (not a
    /// runtime error) so the producing job fails alone and consumers skip via
    /// the needs-gate.
    fn upload(&self, call: &ArtifactCall<'_>) -> Result<ToolkitOutcome, String> {
        let with = call.with;
        let name = with_string(with, "name")
            .ok_or_else(|| "actions/upload-artifact: missing `name`".to_string())?;
        let path_input = with_string(with, "path")
            .ok_or_else(|| "actions/upload-artifact: missing `path`".to_string())?;

        let dest_root = call.workspace.join(STORE_DIR).join(&name);
        if dest_root.exists() {
            std::fs::remove_dir_all(&dest_root)
                .map_err(|e| format!("clean {}: {e}", dest_root.display()))?;
        }
        std::fs::create_dir_all(&dest_root)
            .map_err(|e| format!("mkdir {}: {e}", dest_root.display()))?;

        let mut count = 0usize;
        for raw in path_input.lines() {
            let p = raw.trim();
            if p.is_empty() {
                continue;
            }
            let src = call.workspace.join(p);
            if !src.exists() {
                return Ok(failure(format!(
                    "actions/upload-artifact: path `{p}` does not exist (artifact `{name}`); \
                     the step that was supposed to produce it did not run or produced nothing.",
                )));
            }
            let dst = dest_root.join(src.file_name().unwrap_or_else(|| OsStr::new("artifact")));
            copy_tree(&src, &dst).map_err(|e| format!("copy {p}: {e}"))?;
            count += 1;
        }

        let mut outputs = IndexMap::new();
        outputs.insert("artifact-id".into(), Value::String(name.clone()));
        outputs.insert(
            "artifact-url".into(),
            Value::String(format!("qed-artifact://{name}")),
        );
        Ok(ToolkitOutcome {
            outputs,
            log: format!("actions/upload-artifact: stored {count} path(s) under `{name}`"),
            conclusion: StepConclusion::Success,
        })
    }

    /// `actions/download-artifact` — copy from `.qed-artifacts/<name>/` to
    /// `with.path` (default the workspace). With no `name`, restores every
    /// stored artifact into a subdir by name. A missing artifact is a step
    /// failure carrying a diagnostic that lists what *was* produced.
    fn download(&self, call: &ArtifactCall<'_>) -> Result<ToolkitOutcome, String> {
        let with = call.with;
        let name = with_string(with, "name");
        let dest = with_string(with, "path").unwrap_or_else(|| ".".into());
        let dest_dir = call.workspace.join(&dest);
        std::fs::create_dir_all(&dest_dir)
            .map_err(|e| format!("mkdir {}: {e}", dest_dir.display()))?;

        let root = call.workspace.join(STORE_DIR);
        if !root.exists() {
            let want = name.as_deref().unwrap_or("<all>");
            return Ok(failure(format!(
                "actions/download-artifact: cannot fetch artifact `{want}` — the artifact store \
                 (`{STORE_DIR}/`) is empty; no `actions/upload-artifact` step has run \
                 successfully yet in this workflow run.",
            )));
        }

        let mut count = 0usize;
        if let Some(name) = name {
            let src = root.join(&name);
            if !src.exists() {
                return Ok(failure(format!(
                    "actions/download-artifact: artifact `{name}` not found. Artifacts present: \
                     [{}]. The job whose `actions/upload-artifact` step produces `{name}` did \
                     not complete successfully.",
                    list_stored_artifacts(&root).join(", "),
                )));
            }
            copy_tree_contents(&src, &dest_dir).map_err(|e| format!("restore {name}: {e}"))?;
            count = 1;
        } else {
            for entry in std::fs::read_dir(&root).map_err(|e| format!("scan artifacts: {e}"))? {
                let entry = entry.map_err(|e| format!("scan entry: {e}"))?;
                let entry_name = entry.file_name();
                let dst = dest_dir.join(&entry_name);
                std::fs::create_dir_all(&dst).map_err(|e| format!("mkdir {}: {e}", dst.display()))?;
                copy_tree_contents(&entry.path(), &dst)
                    .map_err(|e| format!("restore {}: {e}", entry_name.to_string_lossy()))?;
                count += 1;
            }
        }

        Ok(ToolkitOutcome {
            outputs: IndexMap::new(),
            log: format!(
                "actions/download-artifact: restored {count} artifact(s) into {}",
                dest_dir.display()
            ),
            conclusion: StepConclusion::Success,
        })
    }
}

fn failure(log: String) -> ToolkitOutcome {
    ToolkitOutcome {
        outputs: IndexMap::new(),
        log,
        conclusion: StepConclusion::Failure,
    }
}

fn list_stored_artifacts(root: &Path) -> Vec<String> {
    let Ok(rd) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut names: Vec<String> = rd
        .flatten()
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn with_string(with: &IndexMap<String, Value>, key: &str) -> Option<String> {
    with.get(key).map(|v| v.as_str_lossy())
}

fn copy_tree(src: &Path, dst: &Path) -> std::io::Result<()> {
    let meta = std::fs::symlink_metadata(src)?;
    if meta.file_type().is_dir() {
        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            copy_tree(&entry.path(), &dst.join(entry.file_name()))?;
        }
        Ok(())
    } else if meta.file_type().is_symlink() {
        let target = std::fs::read_link(src)?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, dst)?;
        #[cfg(not(unix))]
        std::fs::copy(src, dst).map(|_| ())?;
        Ok(())
    } else {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(src, dst).map(|_| ())
    }
}

fn copy_tree_contents(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        copy_tree(&entry.path(), &dst.join(entry.file_name()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn with(pairs: &[(&str, &str)]) -> IndexMap<String, Value> {
        let mut m = IndexMap::new();
        for (k, v) in pairs {
            m.insert((*k).into(), Value::String((*v).into()));
        }
        m
    }

    #[test]
    fn upload_then_download_roundtrips_a_file() {
        let ws = TempDir::new().unwrap();
        std::fs::write(ws.path().join("bin"), b"hello").unwrap();
        let store = LocalArtifactStore::new();

        let up = with(&[("name", "mybin"), ("path", "bin")]);
        let out = store
            .upload(&ArtifactCall { with: &up, workspace: ws.path() })
            .unwrap();
        assert_eq!(out.conclusion, StepConclusion::Success);

        let dl = with(&[("name", "mybin"), ("path", "out")]);
        let out = store
            .download(&ArtifactCall { with: &dl, workspace: ws.path() })
            .unwrap();
        assert_eq!(out.conclusion, StepConclusion::Success);
        let restored = std::fs::read(ws.path().join("out").join("bin")).unwrap();
        assert_eq!(restored, b"hello");
    }

    #[test]
    fn upload_missing_path_is_step_failure_not_error() {
        let ws = TempDir::new().unwrap();
        let store = LocalArtifactStore::new();
        let up = with(&[("name", "x"), ("path", "does-not-exist")]);
        let out = store
            .upload(&ArtifactCall { with: &up, workspace: ws.path() })
            .expect("step failure, not Err");
        assert_eq!(out.conclusion, StepConclusion::Failure);
    }

    #[test]
    fn download_missing_artifact_is_step_failure() {
        let ws = TempDir::new().unwrap();
        std::fs::create_dir_all(ws.path().join(STORE_DIR).join("other")).unwrap();
        let store = LocalArtifactStore::new();
        let dl = with(&[("name", "absent"), ("path", "out")]);
        let out = store
            .download(&ArtifactCall { with: &dl, workspace: ws.path() })
            .unwrap();
        assert_eq!(out.conclusion, StepConclusion::Failure);
        assert!(out.log.contains("other"), "diagnostic lists present artifacts");
    }
}
