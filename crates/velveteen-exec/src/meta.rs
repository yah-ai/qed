//! `ForgeMeta` — the unified run-metadata type for all three forge species.
//!
//! Mirrors the fields of `TaskRunMeta` with three additions:
//!  - `id` is a `ForgeId` instead of `TaskRunId` (same underlying UUID for
//!    local-forge runs).
//!  - `where_` identifies the placement of the run (location × runtime).
//!  - `species` identifies which driver produced the run — orthogonal to
//!    placement (a `Remote` species always runs on yubaba, but a `Local`
//!    species can be `Native` or `Container` per `TaskPlacement.runtime`).
//!
//! `From<TaskRunMeta>` converts a local-forge record into `ForgeMeta` with
//! `where_ = TaskPlacement{Local, Native}`, `species = ForgeSpecies::Local`,
//! and a zero-cost UUID conversion.

use std::path::PathBuf;

use observation::ForgeId;
use serde::{Deserialize, Serialize};
use task_runs::{BeholderStatus, Initiator, TaskRunMeta};

use velveteen::{ForgeSpecies, ForgeStatus, TaskLocation, TaskPlacement, TaskRuntime};

/// Metadata for a forge run, regardless of species.
///
/// `Display` follows `TaskRunMeta`'s convention: `<id>  <command>  [label]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForgeMeta {
    pub id: ForgeId,
    pub command: String,
    pub cwd: PathBuf,
    pub env: Vec<(String, String)>,
    pub started_at: u64,
    pub status: ForgeStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub initiator: Initiator,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub beholder_status: Option<BeholderStatus>,
    #[serde(default)]
    pub pinned: bool,
    /// Where the run was placed (location × runtime).
    pub where_: TaskPlacement,
    /// Which driver produced the run.
    pub species: ForgeSpecies,
}

impl std::fmt::Display for ForgeMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}  {}", self.id, self.command)?;
        if let Some(label) = &self.label {
            write!(f, "  [{label}]")?;
        }
        Ok(())
    }
}

impl From<TaskRunMeta> for ForgeMeta {
    /// Convert a local-forge `TaskRunMeta` into `ForgeMeta`.
    ///
    /// `where_` defaults to `{Local, Native}` and `species` to `Local` — a
    /// `TaskRunMeta` records a subprocess on the host.  The UUID is preserved
    /// unchanged via the identity `From<TaskRunId> for ForgeId` conversion.
    fn from(meta: TaskRunMeta) -> Self {
        Self {
            id: meta.id.into(),
            command: meta.command,
            cwd: meta.cwd,
            env: meta.env,
            started_at: meta.started_at,
            status: meta.status.into(),
            label: meta.label,
            initiator: meta.initiator,
            beholder_status: meta.beholder_status,
            pinned: meta.pinned,
            where_: TaskPlacement::new(TaskLocation::Local, TaskRuntime::Native),
            species: ForgeSpecies::Local,
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod meta {
    use super::*;
    use observation::TaskRunId;
    use task_runs::{Initiator, RunStatus, TaskRunMeta};

    fn sample_task_run_meta() -> TaskRunMeta {
        let id = TaskRunId::new();
        TaskRunMeta {
            id,
            command: "cargo check".into(),
            cwd: "/home/user/project".into(),
            env: vec![("RUST_LOG".into(), "debug".into())],
            started_at: 1_000_000,
            status: RunStatus::Done { exit_code: 0, ended_at: 1_000_500 },
            label: Some("ci".into()),
            initiator: Initiator::Human { camp: "my-camp".into() },
            beholder_status: None,
            pinned: false,
            origin: None,
        }
    }

    #[test]
    fn from_taskrunmeta_preserves_all_fields() {
        let meta = sample_task_run_meta();
        let original_uuid = meta.id.0;
        let forge: ForgeMeta = meta.clone().into();

        // UUID is preserved (identity conversion)
        assert_eq!(forge.id.0, original_uuid);
        assert_eq!(forge.command, meta.command);
        assert_eq!(forge.cwd, meta.cwd);
        assert_eq!(forge.env, meta.env);
        assert_eq!(forge.started_at, meta.started_at);
        assert_eq!(forge.label, meta.label);
        assert!(forge.beholder_status.is_none());
        assert!(!forge.pinned);
    }

    #[test]
    fn from_taskrunmeta_where_defaults_to_local() {
        let meta = sample_task_run_meta();
        let forge: ForgeMeta = meta.into();
        assert_eq!(
            forge.where_,
            TaskPlacement::new(TaskLocation::Local, TaskRuntime::Native),
        );
    }

    #[test]
    fn from_taskrunmeta_species_defaults_to_local() {
        let meta = sample_task_run_meta();
        let forge: ForgeMeta = meta.into();
        assert_eq!(forge.species, ForgeSpecies::Local);
    }

    #[test]
    fn from_taskrunmeta_status_converted() {
        let mut meta = sample_task_run_meta();
        meta.status = RunStatus::Done { exit_code: 1, ended_at: 999 };
        let forge: ForgeMeta = meta.into();
        assert!(matches!(forge.status, ForgeStatus::Done { exit_code: 1, ended_at: 999 }));
    }

    #[test]
    fn forge_meta_round_trip_serde() {
        let meta = sample_task_run_meta();
        let forge: ForgeMeta = meta.into();
        let json = serde_json::to_string(&forge).unwrap();
        let back: ForgeMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(forge.id, back.id);
        assert_eq!(forge.command, back.command);
        assert_eq!(forge.where_, back.where_);
    }

    #[test]
    fn forge_meta_display() {
        let meta = sample_task_run_meta();
        let id_str = meta.id.to_string();
        let forge: ForgeMeta = meta.into();
        let display = forge.to_string();
        assert!(display.contains(&id_str));
        assert!(display.contains("cargo check"));
        assert!(display.contains("[ci]"));
    }
}
