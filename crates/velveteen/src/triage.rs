//! `forge.triage` + `forge.diagnostics` — species-agnostic triage routing.
//!
//! Both functions route through the appropriate event source based on which
//! storage has data for the given `ForgeId`:
//!
//! - **Local-forge** (`ForgeSpecies::Local`): events live in the `TaskStore`
//!   under `EventScope::TaskRun(forge_id.into())`.  `forge_diagnostics`
//!   delegates to `TaskStore::query_diagnostics`; `forge_triage` returns the
//!   cached LLM triage if present, then falls back to a structural triage
//!   derived from the same events.
//!
//! - **Remote / Integration**: events live in scryer's `events.db` under
//!   `EventScope::Forge(forge_id)`.  Both functions query scryer with the
//!   Forge scope and apply the same mapping logic.
//!
//! The output shapes (`Diagnostic`, `Triage`) are identical regardless of
//! species — arch doc §forge-6 guarantee.
//!
//! @yah:relay(R598, "velveteen lib-test binary repair: finish the async migration at test call-sites")
//! @yah:at(2026-07-06T07:54:15Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//!
//! @yah:ticket(R598-B1, "~31 velveteen test call-sites call async fns (scryer.events/TaskStore/forge_triage/forge_list) without .await")
//! @yah:status(review)
//! @yah:at(2026-07-06T11:41:18Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R598)
//! @yah:severity(medium)
//! @yah:verify("cargo test -p velveteen --lib compiles and runs green (incl R590-F2's buildctl_argv/build_image_command/round-trip tests)")
//! @yah:gotcha("Discovered during R590-F2. On committed HEAD `cargo test -p velveteen --lib` fails to compile (~31 errors), blocking ALL velveteen lib tests. Root cause: scryer.events, TaskStore::open/insert_run, forge_triage, forge_list became `async fn` but their test call-sites still call .unwrap()/.expect() synchronously. Sites: triage.rs(15) list.rs(12) remote.rs(3) integration.rs(1). Fix = add .await + convert enclosing #[test]->#[tokio::test] async fn where needed. Tier: Thief (mechanical).")
//! @yah:handoff("FIXED. velveteen `cargo test -p velveteen --lib` = 84 pass / 0 fail / 2 ignored (was ~31 compile errors). Added .await to async call-sites: integration.rs(scryer.events), remote.rs(3x scryer.events), list.rs(open_store+insert_local helpers made async fn, all 10 tests -> #[tokio::test], forge_list awaited). triage.rs converged via peer/linter edits on the shared tree. Unblocks R590-F2's velveteen-side unit tests (buildctl_argv_*, build_image_workload_spec_*, build_image_emits_platform_and_build_args all green now).")

use observation::{Diagnostic, Event, EventScope, ForgeId, Level, TaskRunId};
use yah_scryer::{EventFilter as ScryerEventFilter, Scryer, ScryerError};
use task_runs::{
    EventFilter as TaskEventFilter, KeepRange, SeqRange, StoreError, TaskStore, Triage,
};
use thiserror::Error;

// ─── Error ───────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ForgeTriageError {
    #[error("scryer: {0}")]
    Scryer(#[from] ScryerError),
    #[error("task-runs store: {0}")]
    TaskStore(#[from] StoreError),
}

// ─── forge_diagnostics ───────────────────────────────────────────────────────

/// Return normalized diagnostics (warn/error/fatal events) for a forge run,
/// regardless of species.
///
/// **Routing:**
/// - If `task_store` is provided and the run exists locally (local-forge):
///   delegates to `TaskStore::query_diagnostics` for authoritative results.
/// - Otherwise (remote/integration): queries scryer with
///   `EventScope::Forge(forge_id)` filtered to `min_level = Warn`.
///
/// The returned `Diagnostic.run_id` is always the `TaskRunId` equivalent of
/// `forge_id` (identity UUID conversion) so callers can round-trip back to
/// either type.
pub async fn forge_diagnostics(
    scryer: &Scryer,
    task_store: Option<&TaskStore>,
    forge_id: &ForgeId,
) -> Result<Vec<Diagnostic>, ForgeTriageError> {
    let run_id: TaskRunId = forge_id.clone().into();

    // Local-forge path: task-runs store is authoritative.
    if let Some(ts) = task_store {
        if ts.get_run(&run_id).await?.is_some() {
            return Ok(ts.query_diagnostics(&run_id).await?);
        }
    }

    // Non-local path: scryer events.db with Forge(id) scope.
    let scope = EventScope::Forge(forge_id.clone());
    let filter = ScryerEventFilter { min_level: Some(Level::Warn), ..Default::default() };
    let events = scryer.events(&scope, &filter).await?;
    Ok(events.into_iter().map(event_to_diagnostic).collect())
}

// ─── forge_triage ────────────────────────────────────────────────────────────

/// Return a triage summary for a forge run, regardless of species.
///
/// **Local-forge:**
/// 1. If `task_store` is provided and a cached (LLM-produced) triage exists
///    for this run and `force = false`: returns it directly.
/// 2. Otherwise falls back to a structural triage derived from warn/error
///    events in the task-runs store.
///
/// **Remote / Integration:**
/// Always derives a structural triage from scryer events
/// (`EventScope::Forge(forge_id)`).  No LLM triage cache exists for non-local
/// species yet (R094-F7 will add the forge_tools layer that can schedule triage
/// jobs for any species; this function is the read side).
///
/// Returns `None` when no warn/error events are found (clean run, nothing to
/// triage).  The `Triage.run_id` is set to `forge_id.into()` so callers can
/// round-trip to either `ForgeId` or `TaskRunId`.
pub async fn forge_triage(
    scryer: &Scryer,
    task_store: Option<&TaskStore>,
    forge_id: &ForgeId,
    force: bool,
) -> Result<Option<Triage>, ForgeTriageError> {
    let run_id: TaskRunId = forge_id.clone().into();

    // Local-forge: return cached LLM triage if present (and not forced refresh).
    if !force {
        if let Some(ts) = task_store {
            if let Some(t) = ts.get_triage(&run_id).await? {
                return Ok(Some(t));
            }
        }
    }

    // Determine the event source: local (task-runs store) or non-local (scryer).
    let events: Vec<Event> = if let Some(ts) = task_store {
        if ts.get_run(&run_id).await?.is_some() {
            // Local-forge: derive from task-runs store events.
            let filter = TaskEventFilter {
                min_level: Some(Level::Warn),
                ..Default::default()
            };
            ts.query_events(&run_id, &filter).await?
        } else {
            // task_store provided but run not found locally — treat as non-local.
            scryer_warn_events(scryer, forge_id).await?
        }
    } else {
        // No task_store: remote or integration species.
        scryer_warn_events(scryer, forge_id).await?
    };

    if events.is_empty() {
        return Ok(None);
    }

    Ok(Some(triage_from_events(&events, &run_id)))
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

async fn scryer_warn_events(scryer: &Scryer, forge_id: &ForgeId) -> Result<Vec<Event>, ScryerError> {
    let scope = EventScope::Forge(forge_id.clone());
    let filter = ScryerEventFilter { min_level: Some(Level::Warn), ..Default::default() };
    scryer.events(&scope, &filter).await
}

/// Build a structural `Triage` from warn/error events (no LLM).
///
/// Uses the first and last warn/error event `seq` values as the `primary`
/// range.  Produces a synopsis from the first three error messages.
/// `model` is `"structural"` and `partial` is `true` to signal this is not
/// an LLM-produced triage.
fn triage_from_events(events: &[Event], run_id: &TaskRunId) -> Triage {
    debug_assert!(!events.is_empty());

    let lo = events.first().map_or(0, |e| e.seq);
    let hi = events.last().map_or(0, |e| e.seq);

    let synopsis = events
        .iter()
        .take(3)
        .map(|e| e.msg.as_str())
        .collect::<Vec<_>>()
        .join("; ");

    let keep = vec![KeepRange {
        range: SeqRange { lo, hi },
        reason: "warn/error events".into(),
    }];

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    Triage {
        run_id: run_id.clone(),
        synopsis,
        keep,
        primary: SeqRange { lo, hi },
        model: "structural".into(),
        prompt_version: 0,
        cached_at: now_ms,
        partial: true,
    }
}

/// Map a scryer `Event` to the normalized `Diagnostic` shape.
///
/// Mirrors `task_runs::store::event_to_diagnostic` — same reserved field
/// conventions (`$.file.*`, `$.error.code`).  Kept local so scryer's internal
/// helper doesn't need to be pub.
pub fn event_to_diagnostic(e: Event) -> Diagnostic {
    let file = e
        .fields
        .get("file")
        .and_then(|f| f.get("path"))
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let line = e
        .fields
        .get("file")
        .and_then(|f| f.get("line"))
        .and_then(|v| v.as_u64())
        .map(|n| n as u32);
    let col = e
        .fields
        .get("file")
        .and_then(|f| f.get("col"))
        .and_then(|v| v.as_u64())
        .map(|n| n as u32);
    let code = e
        .fields
        .get("error")
        .and_then(|f| f.get("code"))
        .and_then(|v| v.as_str())
        .map(str::to_owned);

    Diagnostic {
        severity: e.level,
        file,
        line,
        col,
        code,
        message: e.msg,
        source: format!("{}/{}", e.source.kind_str(), e.source.name_str()),
        run_id: e.run_id,
        event_seq: e.seq,
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod diagnostics {
    use super::*;
    use observation::{EventSource, ForgeId, Level, TaskRunId};
    use yah_scryer::{Scryer, ScryerConfig};
    use serde_json::json;
    use std::{path::PathBuf, sync::Arc};
    use task_runs::{Initiator, RunStatus, TaskRunMeta, TaskStore};
    use tempfile::TempDir;

    /// Fixture: two rustc errors that any species beholder would emit.
    fn fixture_events() -> Vec<(Level, &'static str, &'static str)> {
        vec![
            (Level::Error, "cargo::rustc", "cannot find value `foo` in this scope"),
            (Level::Error, "cargo::rustc", "aborting due to 1 previous error"),
        ]
    }

    /// Push fixture events into scryer with `Forge(id)` scope (remote / integration path).
    fn push_forge_events(scryer: &Scryer, forge_id: &ForgeId) {
        let scope = EventScope::Forge(forge_id.clone());
        let run_id: TaskRunId = forge_id.clone().into();
        for (i, (level, target, msg)) in fixture_events().iter().enumerate() {
            scryer
                .push(scope.clone(), Event {
                    run_id: run_id.clone(),
                    seq: i as u32,
                    offset_ms: i as u32 * 100,
                    level: *level,
                    target: target.to_string(),
                    msg: msg.to_string(),
                    fields: json!({"error": {"code": "E0425"}, "file": {"path": "src/main.rs", "line": 5, "col": 4}}),
                    anchor: None,
                    source: EventSource::Synth,
                })
                .unwrap();
        }
        scryer.flush_ring().unwrap();
    }

    /// Insert run + fixture error events into a `TaskStore` (local-forge path).
    async fn insert_local_failure(task_store: &TaskStore, forge_id: &ForgeId) {
        let run_id: TaskRunId = forge_id.clone().into();
        task_store
            .insert_run(&TaskRunMeta {
                id: run_id.clone(),
                command: "cargo check".into(),
                cwd: PathBuf::from("/tmp"),
                env: vec![],
                started_at: 0,
                status: RunStatus::Done { exit_code: 1, ended_at: 1000 },
                label: None,
                initiator: Initiator::Human { camp: "test".into() },
                beholder_status: None,
                pinned: false,
                origin: None,
            })
            .await
            .unwrap();
        for (i, (level, target, msg)) in fixture_events().iter().enumerate() {
            task_store
                .append_event(
                    &run_id,
                    i as u32 * 100,
                    *level,
                    target,
                    msg,
                    &json!({"error": {"code": "E0425"}, "file": {"path": "src/main.rs", "line": 5, "col": 4}}),
                    None,
                    &EventSource::Synth,
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn cross_species() {
        let fixture = fixture_events();

        let local_diags = {
            let dir = TempDir::new().unwrap();
            let forge_id = ForgeId::new();
            let arc_ts = Arc::new(TaskStore::open(&dir.path().join("tr.db")).await.unwrap());
            insert_local_failure(&arc_ts, &forge_id).await;
            let scryer =
                Scryer::new(ScryerConfig::new(dir.path().join("scryer.db")), Some(Arc::clone(&arc_ts)))
                    .unwrap();
            forge_diagnostics(&scryer, Some(&arc_ts), &forge_id).await.unwrap()
        };

        let remote_diags = {
            let dir = TempDir::new().unwrap();
            let forge_id = ForgeId::new();
            let scryer = Scryer::new(ScryerConfig::new(dir.path().join("scryer.db")), None).unwrap();
            push_forge_events(&scryer, &forge_id);
            forge_diagnostics(&scryer, None, &forge_id).await.unwrap()
        };

        let integration_diags = {
            let dir = TempDir::new().unwrap();
            let forge_id = ForgeId::new();
            let scryer = Scryer::new(ScryerConfig::new(dir.path().join("scryer.db")), None).unwrap();
            push_forge_events(&scryer, &forge_id);
            forge_diagnostics(&scryer, None, &forge_id).await.unwrap()
        };

        // All three species must produce the same number of diagnostics.
        assert_eq!(local_diags.len(), fixture.len(), "local species diagnostic count");
        assert_eq!(remote_diags.len(), fixture.len(), "remote species diagnostic count");
        assert_eq!(integration_diags.len(), fixture.len(), "integration species diagnostic count");

        // Messages must match the fixture for all species.
        for (i, (_, _, expected_msg)) in fixture.iter().enumerate() {
            assert_eq!(
                local_diags[i].message, *expected_msg,
                "local diag[{i}] message mismatch"
            );
            assert_eq!(
                remote_diags[i].message, *expected_msg,
                "remote diag[{i}] message mismatch"
            );
            assert_eq!(
                integration_diags[i].message, *expected_msg,
                "integration diag[{i}] message mismatch"
            );
        }

        // Error codes must match (lifted from fields.error.code).
        for diags in [&local_diags, &remote_diags, &integration_diags] {
            for d in diags {
                assert_eq!(d.code.as_deref(), Some("E0425"), "error code");
                assert_eq!(d.file.as_deref(), Some("src/main.rs"), "file path");
                assert_eq!(d.line, Some(5), "line number");
            }
        }
    }
}

#[cfg(test)]
mod triage {
    use super::*;
    use observation::{EventSource, ForgeId, Level, TaskRunId};
    use yah_scryer::{Scryer, ScryerConfig};
    use serde_json::json;
    use std::{path::PathBuf, sync::Arc};
    use task_runs::{Initiator, RunStatus, TaskRunMeta, TaskStore};
    use tempfile::TempDir;

    fn fixture_events() -> Vec<(Level, &'static str, &'static str)> {
        vec![
            (Level::Error, "cargo::rustc", "cannot find value `foo` in this scope"),
            (Level::Error, "cargo::rustc", "aborting due to 1 previous error"),
        ]
    }

    fn push_forge_events(scryer: &Scryer, forge_id: &ForgeId) {
        let scope = EventScope::Forge(forge_id.clone());
        let run_id: TaskRunId = forge_id.clone().into();
        for (i, (level, target, msg)) in fixture_events().iter().enumerate() {
            scryer
                .push(scope.clone(), Event {
                    run_id: run_id.clone(),
                    seq: i as u32,
                    offset_ms: i as u32 * 100,
                    level: *level,
                    target: target.to_string(),
                    msg: msg.to_string(),
                    fields: json!({}),
                    anchor: None,
                    source: EventSource::Synth,
                })
                .unwrap();
        }
        scryer.flush_ring().unwrap();
    }

    async fn insert_local_failure(task_store: &TaskStore, forge_id: &ForgeId) {
        let run_id: TaskRunId = forge_id.clone().into();
        task_store
            .insert_run(&TaskRunMeta {
                id: run_id.clone(),
                command: "cargo check".into(),
                cwd: PathBuf::from("/tmp"),
                env: vec![],
                started_at: 0,
                status: RunStatus::Done { exit_code: 1, ended_at: 1000 },
                label: None,
                initiator: Initiator::Human { camp: "test".into() },
                beholder_status: None,
                pinned: false,
                origin: None,
            })
            .await
            .unwrap();
        for (i, (level, target, msg)) in fixture_events().iter().enumerate() {
            task_store
                .append_event(
                    &run_id,
                    i as u32 * 100,
                    *level,
                    target,
                    msg,
                    &json!({}),
                    None,
                    &EventSource::Synth,
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn cross_species() {
        let fixture = fixture_events();
        let first_msg = fixture[0].2;

        let local_triage = {
            let dir = TempDir::new().unwrap();
            let forge_id = ForgeId::new();
            let arc_ts = Arc::new(TaskStore::open(&dir.path().join("tr.db")).await.unwrap());
            insert_local_failure(&arc_ts, &forge_id).await;
            let scryer =
                Scryer::new(ScryerConfig::new(dir.path().join("scryer.db")), Some(Arc::clone(&arc_ts)))
                    .unwrap();
            forge_triage(&scryer, Some(&arc_ts), &forge_id, false)
                .await
                .unwrap()
                .expect("local triage should be Some")
        };

        let remote_triage = {
            let dir = TempDir::new().unwrap();
            let forge_id = ForgeId::new();
            let scryer = Scryer::new(ScryerConfig::new(dir.path().join("scryer.db")), None).unwrap();
            push_forge_events(&scryer, &forge_id);
            forge_triage(&scryer, None, &forge_id, false)
                .await
                .unwrap()
                .expect("remote triage should be Some")
        };

        let integration_triage = {
            let dir = TempDir::new().unwrap();
            let forge_id = ForgeId::new();
            let scryer = Scryer::new(ScryerConfig::new(dir.path().join("scryer.db")), None).unwrap();
            push_forge_events(&scryer, &forge_id);
            forge_triage(&scryer, None, &forge_id, false)
                .await
                .unwrap()
                .expect("integration triage should be Some")
        };

        // All three species must produce structural triage (partial=true, model="structural").
        for (label, t) in [
            ("local", &local_triage),
            ("remote", &remote_triage),
            ("integration", &integration_triage),
        ] {
            assert!(t.partial, "{label} triage must be partial=true (structural)");
            assert_eq!(t.model, "structural", "{label} triage model");
        }

        // Primary range must cover the fixture events (seq 0..1).
        for (label, t) in [
            ("local", &local_triage),
            ("remote", &remote_triage),
            ("integration", &integration_triage),
        ] {
            assert_eq!(t.primary.lo, 0, "{label} primary.lo");
            assert_eq!(t.primary.hi, 1, "{label} primary.hi");
        }

        // Synopsis must include the first error message for all species.
        for (label, t) in [
            ("local", &local_triage),
            ("remote", &remote_triage),
            ("integration", &integration_triage),
        ] {
            assert!(
                t.synopsis.contains(first_msg),
                "{label} synopsis must contain first error: got {:?}",
                t.synopsis
            );
        }
    }

    #[tokio::test]
    async fn local_returns_cached_triage() {
        use task_runs::{KeepRange, SeqRange, Triage};

        let dir = TempDir::new().unwrap();
        let forge_id = ForgeId::new();
        let run_id: TaskRunId = forge_id.clone().into();

        let arc_ts = Arc::new(TaskStore::open(&dir.path().join("tr.db")).await.unwrap());
        insert_local_failure(&arc_ts, &forge_id).await;

        // Write a cached (LLM) triage for this run.
        let cached = Triage {
            run_id: run_id.clone(),
            synopsis: "LLM-produced summary".into(),
            keep: vec![KeepRange { range: SeqRange { lo: 0, hi: 1 }, reason: "all".into() }],
            primary: SeqRange { lo: 0, hi: 1 },
            model: "claude-3-5-sonnet".into(),
            prompt_version: 1,
            cached_at: 9_000_000,
            partial: false,
        };
        arc_ts.upsert_triage(&cached).await.unwrap();

        let scryer =
            Scryer::new(ScryerConfig::new(dir.path().join("scryer.db")), Some(Arc::clone(&arc_ts)))
                .unwrap();
        let result = forge_triage(&scryer, Some(&arc_ts), &forge_id, false)
            .await
            .unwrap()
            .expect("should return cached triage");

        // Must return the LLM triage, not the structural fallback.
        assert_eq!(result.model, "claude-3-5-sonnet");
        assert!(!result.partial);
        assert_eq!(result.synopsis, "LLM-produced summary");
    }

    #[tokio::test]
    async fn clean_run_returns_none() {
        let dir = TempDir::new().unwrap();
        let forge_id = ForgeId::new();
        // No events written → forge_triage should return None.
        let scryer = Scryer::new(ScryerConfig::new(dir.path().join("scryer.db")), None).unwrap();
        let result = forge_triage(&scryer, None, &forge_id, false).await.unwrap();
        assert!(result.is_none(), "clean run with no warn/error events should return None");
    }
}
