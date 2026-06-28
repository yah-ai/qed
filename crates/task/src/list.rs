//! `forge_list` — cross-species forge run listing with species + status filters.
//!
//! Local-forge entries come from the `TaskStore` (same store as `task.list`).
//! Remote and integration entries are passed in as pre-fetched slices; the
//! caller is responsible for querying them (via yubaba RPC / R093-F4 federation
//! for remote, via integration driver registry for integration runs).
//!
//! Sorting: running first, then by `started_at` descending.

use crate::{ForgeMeta, ForgeSpecies, ForgeStatus};
use task_runs::{RunFilter, StoreError, TaskStore};

// ─── ForgeListFilter ─────────────────────────────────────────────────────────

/// Filter for `forge_list`.  All fields are optional; absent fields apply no
/// constraint.
#[derive(Debug, Default, Clone)]
pub struct ForgeListFilter {
    /// Only include runs with `started_at >= since` (Unix ms).
    pub since: Option<u64>,
    /// Only include runs whose `label` matches exactly.
    pub label: Option<String>,
    /// Filter by status discriminant: `"pending"` | `"running"` | `"done"` |
    /// `"killed"` | `"timed_out"` | `"lost"`.
    pub status: Option<String>,
    /// Filter by species (Local / Remote / Integration).  `None` includes
    /// every species.
    pub species: Option<ForgeSpecies>,
    /// Maximum number of entries to return (across all species, after merge +
    /// sort).
    pub limit: Option<usize>,
}

// ─── forge_list ───────────────────────────────────────────────────────────────

/// Return forge run metadata across all three species, filtered and sorted.
///
/// - `local_store`: the `TaskStore` for local-forge runs.
/// - `remote_metas`: pre-fetched remote-forge `ForgeMeta` entries (e.g. from
///   yubaba RPC).  Pass `&[]` when no remote query was performed.
/// - `integration_metas`: pre-fetched integration-forge entries.  Pass `&[]`
///   when no integration query was performed.
/// - `filter`: species, status, label, since, and limit constraints.
pub async fn forge_list(
    local_store: &TaskStore,
    remote_metas: &[ForgeMeta],
    integration_metas: &[ForgeMeta],
    filter: &ForgeListFilter,
) -> Result<Vec<ForgeMeta>, StoreError> {
    let mut results: Vec<ForgeMeta> = Vec::new();

    if wants_species(&filter.species, ForgeSpecies::Local) {
        let run_filter = to_run_filter(filter);
        let local_runs = local_store.list_runs(&run_filter).await?;
        results.extend(local_runs.into_iter().map(ForgeMeta::from));
    }

    if wants_species(&filter.species, ForgeSpecies::Remote) {
        results.extend(
            remote_metas
                .iter()
                .filter(|m| meta_matches(m, filter))
                .cloned(),
        );
    }

    if wants_species(&filter.species, ForgeSpecies::Integration) {
        results.extend(
            integration_metas
                .iter()
                .filter(|m| meta_matches(m, filter))
                .cloned(),
        );
    }

    // Running first, then by started_at descending.
    results.sort_by(|a, b| {
        let a_run = matches!(a.status, ForgeStatus::Running);
        let b_run = matches!(b.status, ForgeStatus::Running);
        if a_run != b_run {
            return b_run.cmp(&a_run);
        }
        b.started_at.cmp(&a.started_at)
    });

    if let Some(limit) = filter.limit {
        results.truncate(limit);
    }

    Ok(results)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

fn wants_species(filter: &Option<ForgeSpecies>, candidate: ForgeSpecies) -> bool {
    match filter {
        None => true,
        Some(s) => *s == candidate,
    }
}

/// Convert `ForgeListFilter` into a `RunFilter` for the local `TaskStore`.
///
/// `where_` and `limit` are handled at the forge level, not passed to the
/// store — `limit` is applied after merging all species so the cap applies
/// globally, and `where_` is species-routing logic above this call.
fn to_run_filter(f: &ForgeListFilter) -> RunFilter {
    RunFilter {
        since: f.since,
        label: f.label.clone(),
        status: f.status.clone(),
        limit: None,
        archived: None,
        origin: None,
    }
}

/// Returns `true` when `meta` satisfies the non-species fields of `filter`.
///
/// Used for the remote and integration slices (local-forge filtering is
/// delegated to `TaskStore::list_runs`).
fn meta_matches(meta: &ForgeMeta, filter: &ForgeListFilter) -> bool {
    if let Some(since) = filter.since {
        if meta.started_at < since {
            return false;
        }
    }
    if let Some(label) = &filter.label {
        if meta.label.as_deref() != Some(label.as_str()) {
            return false;
        }
    }
    if let Some(status) = &filter.status {
        if meta.status.discriminant() != status.as_str() {
            return false;
        }
    }
    true
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod list {
    use super::*;
    use crate::{ForgeSpecies, ForgeStatus, TaskLocation, TaskPlacement, TaskRuntime};
    use observation::ForgeId;
    use std::path::PathBuf;
    use task_runs::{Initiator, RunStatus, TaskRunId, TaskRunMeta, TaskStore};
    use tempfile::tempdir;
    use workload_spec::MeshIdent;

    fn remote_warden() -> TaskPlacement {
        TaskPlacement::new(
            TaskLocation::Remote { node: MeshIdent("builder.pdx".into()) },
            TaskRuntime::Container,
        )
    }

    fn open_store(dir: &std::path::Path) -> TaskStore {
        TaskStore::open(&dir.join("task-runs.db")).expect("open store")
    }

    fn insert_local(store: &TaskStore, label: &str, status: RunStatus, started_at: u64) -> TaskRunId {
        let id = TaskRunId::new();
        let meta = TaskRunMeta {
            id: id.clone(),
            command: format!("cargo check --{label}"),
            cwd: PathBuf::from("/tmp"),
            env: vec![],
            started_at,
            status,
            label: Some(label.into()),
            initiator: Initiator::Human { camp: "test".into() },
            beholder_status: None,
            pinned: false,
            origin: None,
        };
        store.insert_run(&meta).expect("insert_run");
        id
    }

    fn make_remote_meta(label: &str, status: ForgeStatus, started_at: u64) -> ForgeMeta {
        ForgeMeta {
            id: ForgeId::new(),
            command: format!("cargo build --{label}"),
            cwd: PathBuf::from("/remote"),
            env: vec![],
            started_at,
            status,
            label: Some(label.into()),
            initiator: Initiator::Human { camp: "test".into() },
            beholder_status: None,
            pinned: false,
            where_: remote_warden(),
            species: ForgeSpecies::Remote,
        }
    }

    fn make_integration_meta(label: &str, status: ForgeStatus, started_at: u64) -> ForgeMeta {
        ForgeMeta {
            id: ForgeId::new(),
            command: "integration-harness".into(),
            cwd: PathBuf::from("/integration"),
            env: vec![],
            started_at,
            status,
            label: Some(label.into()),
            initiator: Initiator::Human { camp: "test".into() },
            beholder_status: None,
            pinned: false,
            where_: TaskPlacement::new(TaskLocation::Local, TaskRuntime::Container),
            species: ForgeSpecies::Integration,
        }
    }

    #[test]
    fn all_species_returned_when_no_filter() {
        let dir = tempdir().unwrap();
        let store = open_store(dir.path());
        insert_local(&store, "local-run", RunStatus::Running, 1000);

        let remote = [make_remote_meta("remote-run", ForgeStatus::Running, 2000)];
        let integration = [make_integration_meta("int-run", ForgeStatus::Running, 3000)];

        let result = forge_list(&store, &remote, &integration, &ForgeListFilter::default()).unwrap();
        assert_eq!(result.len(), 3);

        let species: Vec<_> = result.iter().map(|m| m.species).collect();
        assert!(species.contains(&ForgeSpecies::Local));
        assert!(species.contains(&ForgeSpecies::Remote));
        assert!(species.contains(&ForgeSpecies::Integration));
    }

    #[test]
    fn filter_by_species_local() {
        let dir = tempdir().unwrap();
        let store = open_store(dir.path());
        insert_local(&store, "local-run", RunStatus::Running, 1000);

        let remote = [make_remote_meta("remote-run", ForgeStatus::Running, 2000)];
        let integration = [make_integration_meta("int-run", ForgeStatus::Running, 3000)];

        let filter = ForgeListFilter {
            species: Some(ForgeSpecies::Local),
            ..Default::default()
        };
        let result = forge_list(&store, &remote, &integration, &filter).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].species, ForgeSpecies::Local);
    }

    #[test]
    fn filter_by_species_remote() {
        let dir = tempdir().unwrap();
        let store = open_store(dir.path());
        insert_local(&store, "local-run", RunStatus::Running, 1000);

        let remote = [make_remote_meta(
            "remote-run",
            ForgeStatus::Done { exit_code: 0, ended_at: 9000 },
            2000,
        )];
        let integration = [make_integration_meta("int-run", ForgeStatus::Running, 3000)];

        let filter = ForgeListFilter {
            species: Some(ForgeSpecies::Remote),
            ..Default::default()
        };
        let result = forge_list(&store, &remote, &integration, &filter).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].species, ForgeSpecies::Remote);
    }

    #[test]
    fn filter_by_species_integration() {
        let dir = tempdir().unwrap();
        let store = open_store(dir.path());
        insert_local(&store, "local-run", RunStatus::Running, 1000);

        let remote = [make_remote_meta("remote-run", ForgeStatus::Running, 2000)];
        let integration =
            [make_integration_meta("int-run", ForgeStatus::Done { exit_code: 0, ended_at: 9000 }, 3000)];

        let filter = ForgeListFilter {
            species: Some(ForgeSpecies::Integration),
            ..Default::default()
        };
        let result = forge_list(&store, &remote, &integration, &filter).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].species, ForgeSpecies::Integration);
    }

    #[test]
    fn filter_by_status() {
        let dir = tempdir().unwrap();
        let store = open_store(dir.path());
        insert_local(&store, "running-run", RunStatus::Running, 1000);
        insert_local(&store, "done-run", RunStatus::Done { exit_code: 0, ended_at: 5000 }, 900);

        let remote = [make_remote_meta("remote-running", ForgeStatus::Running, 2000)];
        let integration = [make_integration_meta("int-done", ForgeStatus::Done { exit_code: 1, ended_at: 8000 }, 800)];

        let filter = ForgeListFilter { status: Some("running".into()), ..Default::default() };
        let result = forge_list(&store, &remote, &integration, &filter).unwrap();
        assert_eq!(result.len(), 2, "should include local running + remote running");
        assert!(result.iter().all(|m| m.status.discriminant() == "running"));
    }

    #[test]
    fn filter_by_label() {
        let dir = tempdir().unwrap();
        let store = open_store(dir.path());
        insert_local(&store, "target", RunStatus::Running, 1000);
        insert_local(&store, "other", RunStatus::Running, 900);

        let remote = [make_remote_meta("target", ForgeStatus::Running, 2000)];
        let integration = [];

        let filter = ForgeListFilter { label: Some("target".into()), ..Default::default() };
        let result = forge_list(&store, &remote, &integration, &filter).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|m| m.label.as_deref() == Some("target")));
    }

    #[test]
    fn filter_by_since() {
        let dir = tempdir().unwrap();
        let store = open_store(dir.path());
        insert_local(&store, "old", RunStatus::Done { exit_code: 0, ended_at: 500 }, 100);
        insert_local(&store, "new", RunStatus::Running, 2000);

        let remote = [
            make_remote_meta("old-remote", ForgeStatus::Done { exit_code: 0, ended_at: 600 }, 200),
            make_remote_meta("new-remote", ForgeStatus::Running, 3000),
        ];
        let integration = [];

        let filter = ForgeListFilter { since: Some(1500), ..Default::default() };
        let result = forge_list(&store, &remote, &integration, &filter).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|m| m.started_at >= 1500));
    }

    #[test]
    fn limit_applied_after_merge() {
        let dir = tempdir().unwrap();
        let store = open_store(dir.path());
        insert_local(&store, "a", RunStatus::Running, 1000);
        insert_local(&store, "b", RunStatus::Running, 900);

        let remote = [make_remote_meta("c", ForgeStatus::Running, 2000)];
        let integration = [make_integration_meta("d", ForgeStatus::Running, 3000)];

        let filter = ForgeListFilter { limit: Some(2), ..Default::default() };
        let result = forge_list(&store, &remote, &integration, &filter).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn running_sorted_first() {
        let dir = tempdir().unwrap();
        let store = open_store(dir.path());
        insert_local(&store, "done-local", RunStatus::Done { exit_code: 0, ended_at: 5000 }, 500);
        insert_local(&store, "running-local", RunStatus::Running, 1000);

        let remote = [];
        let integration = [];

        let result = forge_list(&store, &remote, &integration, &ForgeListFilter::default()).unwrap();
        assert_eq!(result.len(), 2);
        assert!(matches!(result[0].status, ForgeStatus::Running));
    }

    #[test]
    fn timed_out_status_filter() {
        let dir = tempdir().unwrap();
        let store = open_store(dir.path());
        // TaskStore has no timed_out — only remote/integration can produce it.
        let remote = [
            make_remote_meta("timed", ForgeStatus::TimedOut { ended_at: 9000 }, 1000),
            make_remote_meta("done", ForgeStatus::Done { exit_code: 0, ended_at: 8000 }, 2000),
        ];
        let integration = [];

        let filter = ForgeListFilter { status: Some("timed_out".into()), ..Default::default() };
        let result = forge_list(&store, &remote, &integration, &filter).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0].status, ForgeStatus::TimedOut { .. }));
    }
}
