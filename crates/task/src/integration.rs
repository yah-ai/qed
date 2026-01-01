// @yah:ticket(R094-F4, "Integration-forge driver: lift R091 test composition logic into forge::integration; #[test_with_provider] becomes a thin wrapper")
// @yah:assignee(agent:claude)
// @yah:status(review)
// @yah:phase(P2)
// @yah:parent(R094)
// @yah:handoff("crates/yah/task/src/integration.rs — ClusterClient trait (deploy/connect_logs/teardown) + IntegrationForgeDriver::start (allocates ForgeId, deploys all workloads, spawns per-workload log-ingestion tasks sharing a global AtomicU32 seq counter to avoid PRIMARY KEY collisions in scryer, returns IntegrationRunHandle) + IntegrationRunHandle::complete (applies TeardownPolicy: Always/OnSuccess/Manual) + IntegrationRunHandle::teardown (explicit reap for Manual). test_support::ScriptedClusterClient tracks torn_down per-ident. All 25 task tests pass (6 teardown_modes variants + scryer scope test + 16 prior tests). cargo check --workspace clean.")
// @yah:next("Human review: (a) ClusterClient trait shape — does it match what R091-F1 ContainerRuntime will need to implement? (b) shared AtomicU32 seq counter rationale — scryer PRIMARY KEY is (scope_kind, scope_id, seq) so per-workload seq=0 would collide without it; (c) TeardownPolicy semantics in complete() vs teardown(); (d) confirm EventScope::Forge scope is correct for all events from all workloads in the stand-up.")
// @yah:next("R091 integration tests (cargo test --test integration) will route through forge.integration once R091-F4 proc macro lands — deferred to that relay.")
// @arch:see(.yah/docs/architecture/A035-yah-forge.md)
// @arch:see(.yah/docs/architecture/A053-yah-warden-integration-testing.md)
//!
//! Integration-forge driver.
//!
//! Deploys an [`IntegrationForgeSpec`] (N workloads) via a [`ClusterClient`]
//! seam, attaches a per-workload log-ingestion task, and applies
//! [`TeardownPolicy`] when the test body finishes.
//!
//! # Seam
//!
//! [`ClusterClient`] abstracts the underlying cluster. Production warden wires
//! in `ContainerRuntime` (R091-F1). Tests use
//! [`test_support::ScriptedClusterClient`].
//!
//! # Teardown modes
//!
//! | Policy | `complete(false)` | `complete(true)` | explicit `teardown()` |
//! |---|---|---|---|
//! | `Always` | reaps | reaps | reaps |
//! | `OnSuccess` | **keeps alive** for post-mortem | reaps | reaps |
//! | `Manual` | keeps alive | keeps alive | reaps |

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use observation::{Event, EventScope, EventSource, ForgeId, Level, TaskRunId};
use scryer::service::Scryer;
use serde_json::json;
use thiserror::Error;
use tokio::sync::mpsc;
use workload_spec::{MeshIdent, WorkloadSpec};

use crate::{ForgeStatus, IntegrationForgeSpec, TeardownPolicy};

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum IntegrationForgeError {
    #[error("deploy workload '{ident}': {cause}")]
    Deploy { ident: String, cause: String },
    #[error("log stream for '{ident}': {cause}")]
    LogStream { ident: String, cause: String },
    #[error("teardown '{ident}': {cause}")]
    Teardown { ident: String, cause: String },
}

// ─── ClusterClient ────────────────────────────────────────────────────────────

/// Seam between the integration-forge driver and the underlying cluster.
///
/// Production: warden's `ContainerRuntime` impl (R091-F1). Tests:
/// [`test_support::ScriptedClusterClient`].
#[async_trait]
pub trait ClusterClient: Send + Sync {
    /// Deploy a workload. Returns once the RPC completes; does not wait for the
    /// container to reach Ready.
    async fn deploy(&self, spec: &WorkloadSpec) -> Result<(), IntegrationForgeError>;

    /// Open a line-oriented log stream for the named workload. The returned
    /// `Receiver` yields one line per item; sender drop signals clean close.
    async fn connect_logs(
        &self,
        ident: &MeshIdent,
    ) -> Result<mpsc::Receiver<String>, IntegrationForgeError>;

    /// Tear down a workload. Returns `Ok` even if already gone.
    async fn teardown(&self, ident: &MeshIdent) -> Result<(), IntegrationForgeError>;
}

// ─── IntegrationForgeDriver ───────────────────────────────────────────────────

/// Drives N-workload integration-forge stand-ups.
///
/// [`start`](Self::start) deploys every workload, spawns per-workload
/// log-ingestion tasks, and returns an [`IntegrationRunHandle`] immediately.
/// The caller drives the test body, then calls
/// [`IntegrationRunHandle::complete`] (or [`IntegrationRunHandle::teardown`]
/// for [`TeardownPolicy::Manual`]).
pub struct IntegrationForgeDriver {
    scryer: Arc<Scryer>,
    client: Arc<dyn ClusterClient>,
}

impl IntegrationForgeDriver {
    pub fn new(scryer: Arc<Scryer>, client: Arc<dyn ClusterClient>) -> Self {
        Self { scryer, client }
    }

    /// Start an integration-forge stand-up.
    ///
    /// Deploys every workload in `spec`, attaches a scryer ingestion task per
    /// workload (all events land under `EventScope::Forge(id)`), and returns a
    /// handle before any workload finishes. Returns an error if any deployment
    /// or log-stream connection fails.
    pub async fn start(
        &self,
        spec: IntegrationForgeSpec,
    ) -> Result<IntegrationRunHandle, IntegrationForgeError> {
        let forge_id = ForgeId::new();
        let run_id: TaskRunId = forge_id.clone().into();
        let scope = EventScope::Forge(forge_id.clone());
        // Shared monotonic seq counter: events from all workloads share one
        // scope, and the store PRIMARY KEY is (scope_kind, scope_id, seq).
        // Each ingestion task must draw from this counter to avoid collisions.
        let seq_counter = Arc::new(AtomicU32::new(0));
        let mut idents = Vec::with_capacity(spec.workloads.len());

        for workload in &spec.workloads {
            let ident = workload.expose.mesh.identity.clone();

            self.client.deploy(workload).await.map_err(|e| IntegrationForgeError::Deploy {
                ident: ident.0.clone(),
                cause: e.to_string(),
            })?;

            let rx = self.client.connect_logs(&ident).await.map_err(|e| {
                IntegrationForgeError::LogStream {
                    ident: ident.0.clone(),
                    cause: e.to_string(),
                }
            })?;

            let scryer = self.scryer.clone();
            let s = scope.clone();
            let rid = run_id.clone();
            let counter = seq_counter.clone();
            tokio::spawn(async move {
                ingest_logs(s, rid, rx, scryer, counter).await;
            });

            idents.push(ident);
        }

        Ok(IntegrationRunHandle {
            id: forge_id,
            idents,
            policy: spec.teardown,
            client: self.client.clone(),
        })
    }
}

// ─── IntegrationRunHandle ─────────────────────────────────────────────────────

/// Handle to a live integration-forge stand-up.
///
/// Returned by [`IntegrationForgeDriver::start`]. Call
/// [`complete`](Self::complete) when the test body finishes; the
/// [`TeardownPolicy`] governs whether workloads are reaped automatically.
/// For [`TeardownPolicy::Manual`], call [`teardown`](Self::teardown) explicitly.
pub struct IntegrationRunHandle {
    /// Stable identifier for this stand-up, shared across all workloads.
    pub id: ForgeId,
    idents: Vec<MeshIdent>,
    policy: TeardownPolicy,
    client: Arc<dyn ClusterClient>,
}

impl IntegrationRunHandle {
    /// Signal that the test body has finished.
    ///
    /// Applies [`TeardownPolicy`] — see the table in the module doc — and
    /// returns the [`ForgeStatus`] for the stand-up:
    /// - `Done { exit_code: 0 }` when `success = true`
    /// - `Lost { reason: "test failure" }` when `success = false`
    pub async fn complete(self, success: bool) -> ForgeStatus {
        let IntegrationRunHandle { idents, policy, client, .. } = self;
        let should_teardown = match policy {
            TeardownPolicy::Always => true,
            TeardownPolicy::OnSuccess => success,
            TeardownPolicy::Manual => false,
        };
        if should_teardown {
            for ident in &idents {
                let _ = client.teardown(ident).await;
            }
        }
        if success {
            ForgeStatus::Done { exit_code: 0, ended_at: now_ms() }
        } else {
            ForgeStatus::Lost { reason: "test failure".into() }
        }
    }

    /// Explicitly reap all workloads in the stand-up.
    ///
    /// Required when [`TeardownPolicy::Manual`] — [`complete`](Self::complete)
    /// will not tear down in that case. Safe to call even after
    /// `Always`/`OnSuccess` already reaped.
    pub async fn teardown(self) {
        let IntegrationRunHandle { idents, client, .. } = self;
        for ident in &idents {
            let _ = client.teardown(ident).await;
        }
    }
}

// ─── Internals ────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

async fn ingest_logs(
    scope: EventScope,
    run_id: TaskRunId,
    mut rx: mpsc::Receiver<String>,
    scryer: Arc<Scryer>,
    seq_counter: Arc<AtomicU32>,
) {
    while let Some(line) = rx.recv().await {
        let seq = seq_counter.fetch_add(1, Ordering::Relaxed);
        let ev = Event {
            run_id: run_id.clone(),
            seq,
            offset_ms: 0,
            level: Level::Info,
            target: "forge.integration".into(),
            msg: line,
            fields: json!({}),
            anchor: None,
            source: EventSource::Synth,
        };
        let _ = scryer.push(scope.clone(), ev);
    }
}

// ─── Test support ─────────────────────────────────────────────────────────────

/// Test-only cluster client implementations.
#[cfg(test)]
pub mod test_support {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A cluster client that emits scripted log lines per workload and records
    /// teardown calls for assertion.
    pub struct ScriptedClusterClient {
        pub lines_by_ident: HashMap<String, Vec<String>>,
        /// Ordered list of `MeshIdent`s that `teardown` was called with.
        pub torn_down: Arc<Mutex<Vec<MeshIdent>>>,
    }

    impl ScriptedClusterClient {
        /// Create a scripted client.
        ///
        /// `lines` maps mesh-ident strings to the lines that workload's log
        /// stream will emit. An ident with no entry produces an empty stream.
        pub fn new(lines: Vec<(&str, Vec<String>)>) -> Arc<Self> {
            Arc::new(Self {
                lines_by_ident: lines.into_iter().map(|(k, v)| (k.into(), v)).collect(),
                torn_down: Default::default(),
            })
        }
    }

    #[async_trait]
    impl ClusterClient for ScriptedClusterClient {
        async fn deploy(&self, _spec: &WorkloadSpec) -> Result<(), IntegrationForgeError> {
            Ok(())
        }

        async fn connect_logs(
            &self,
            ident: &MeshIdent,
        ) -> Result<mpsc::Receiver<String>, IntegrationForgeError> {
            let (tx, rx) = mpsc::channel(16);
            let lines = self.lines_by_ident.get(&ident.0).cloned().unwrap_or_default();
            tokio::spawn(async move {
                for line in lines {
                    if tx.send(line).await.is_err() {
                        break;
                    }
                }
            });
            Ok(rx)
        }

        async fn teardown(&self, ident: &MeshIdent) -> Result<(), IntegrationForgeError> {
            self.torn_down.lock().unwrap().push(ident.clone());
            Ok(())
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod integration {
    use super::test_support::*;
    use super::*;
    use observation::EventScope;
    use scryer::service::{EventFilter, Scryer, ScryerConfig};
    use tempfile::TempDir;
    use workload_spec::{ImageRef, Millis, TierTag, WorkloadSpec};

    fn make_scryer(dir: &TempDir) -> Arc<Scryer> {
        let cfg = ScryerConfig::new(dir.path().join("events.db"));
        Arc::new(Scryer::new(cfg, None).unwrap())
    }

    fn alpine_image() -> ImageRef {
        ImageRef {
            registry: "docker.io".into(),
            repository: "library/alpine".into(),
            tag: "3.19".into(),
            digest: workload_spec::testing::test_digest(),
        }
    }

    /// Helper: build a test workload whose mesh ident is `"forge.<name>"`.
    fn test_workload(name: &str) -> WorkloadSpec {
        WorkloadSpec::for_forge(name, alpine_image(), TierTag("infra".into()), vec![])
    }

    fn test_spec(workloads: Vec<WorkloadSpec>, policy: TeardownPolicy) -> IntegrationForgeSpec {
        IntegrationForgeSpec {
            workloads,
            topology: Default::default(),
            fixtures: vec![],
            timeout: Millis::from_secs(30),
            teardown: policy,
            label: None,
        }
    }

    // ── R094-F4 accept: teardown_modes ────────────────────────────────────────

    mod teardown_modes {
        use super::*;

        /// `Always` reaps all workloads even when the test fails.
        #[tokio::test]
        async fn always_reaps_on_failure() {
            let dir = TempDir::new().unwrap();
            let client = ScriptedClusterClient::new(vec![]);
            let torn_down = client.torn_down.clone();
            let driver = IntegrationForgeDriver::new(make_scryer(&dir), client);

            let spec = test_spec(
                vec![test_workload("w1"), test_workload("w2")],
                TeardownPolicy::Always,
            );
            let handle = driver.start(spec).await.unwrap();
            let status = handle.complete(false).await;

            let td = torn_down.lock().unwrap();
            assert_eq!(td.len(), 2, "Always: both workloads must be torn down on failure");
            assert!(matches!(status, ForgeStatus::Lost { .. }));
        }

        /// `Always` reaps on success too.
        #[tokio::test]
        async fn always_reaps_on_success() {
            let dir = TempDir::new().unwrap();
            let client = ScriptedClusterClient::new(vec![]);
            let torn_down = client.torn_down.clone();
            let driver = IntegrationForgeDriver::new(make_scryer(&dir), client);

            let spec = test_spec(vec![test_workload("w1")], TeardownPolicy::Always);
            let handle = driver.start(spec).await.unwrap();
            let status = handle.complete(true).await;

            assert_eq!(torn_down.lock().unwrap().len(), 1, "Always: must reap on success");
            assert!(matches!(status, ForgeStatus::Done { exit_code: 0, .. }));
        }

        /// `OnSuccess` keeps stand-up alive on failure for post-mortem.
        #[tokio::test]
        async fn on_success_preserves_on_failure() {
            let dir = TempDir::new().unwrap();
            let client = ScriptedClusterClient::new(vec![]);
            let torn_down = client.torn_down.clone();
            let driver = IntegrationForgeDriver::new(make_scryer(&dir), client);

            let spec = test_spec(vec![test_workload("w1")], TeardownPolicy::OnSuccess);
            let handle = driver.start(spec).await.unwrap();
            let status = handle.complete(false).await;

            assert!(
                torn_down.lock().unwrap().is_empty(),
                "OnSuccess: must NOT tear down on failure"
            );
            assert!(matches!(status, ForgeStatus::Lost { .. }));
        }

        /// `OnSuccess` reaps when the test passes.
        #[tokio::test]
        async fn on_success_reaps_on_success() {
            let dir = TempDir::new().unwrap();
            let client = ScriptedClusterClient::new(vec![]);
            let torn_down = client.torn_down.clone();
            let driver = IntegrationForgeDriver::new(make_scryer(&dir), client);

            let spec = test_spec(vec![test_workload("w1")], TeardownPolicy::OnSuccess);
            let handle = driver.start(spec).await.unwrap();
            let status = handle.complete(true).await;

            assert_eq!(torn_down.lock().unwrap().len(), 1, "OnSuccess: must reap on success");
            assert!(matches!(status, ForgeStatus::Done { exit_code: 0, .. }));
        }

        /// `Manual` never tears down on `complete` — neither success nor failure.
        #[tokio::test]
        async fn manual_complete_does_not_teardown() {
            let dir = TempDir::new().unwrap();
            let client = ScriptedClusterClient::new(vec![]);
            let torn_down = client.torn_down.clone();
            let driver = IntegrationForgeDriver::new(make_scryer(&dir), client);

            let spec = test_spec(vec![test_workload("w1")], TeardownPolicy::Manual);
            let handle = driver.start(spec).await.unwrap();
            let status = handle.complete(true).await;

            assert!(
                torn_down.lock().unwrap().is_empty(),
                "Manual: complete(true) must NOT tear down"
            );
            assert!(matches!(status, ForgeStatus::Done { .. }));
        }

        /// `Manual` requires an explicit `teardown()` call.
        #[tokio::test]
        async fn manual_requires_explicit_teardown() {
            let dir = TempDir::new().unwrap();
            let client = ScriptedClusterClient::new(vec![]);
            let torn_down = client.torn_down.clone();
            let driver = IntegrationForgeDriver::new(make_scryer(&dir), client);

            let spec = test_spec(vec![test_workload("w1")], TeardownPolicy::Manual);
            let handle = driver.start(spec).await.unwrap();
            handle.teardown().await;

            assert_eq!(
                torn_down.lock().unwrap().len(),
                1,
                "Manual: explicit teardown() must reap"
            );
        }
    }

    // ── R094-F4 accept: events land in scryer with Forge scope ────────────────

    /// Logs from every workload land in scryer under the same `Forge(id)` scope.
    #[tokio::test]
    async fn events_land_in_scryer_with_forge_scope() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        // WorkloadSpec::for_forge("w1") produces mesh ident "forge.w1".
        let client = ScriptedClusterClient::new(vec![
            ("forge.w1", vec!["alpha".into(), "beta".into()]),
            ("forge.w2", vec!["gamma".into()]),
        ]);
        let driver = IntegrationForgeDriver::new(scryer.clone(), client);

        let spec = test_spec(
            vec![test_workload("w1"), test_workload("w2")],
            TeardownPolicy::Always,
        );
        let handle = driver.start(spec).await.unwrap();
        let id = handle.id.clone();
        let _ = handle.complete(true).await;

        // Poll until background ingestion tasks drain (max 2s).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let events = loop {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            scryer.flush_ring().unwrap();
            let evs = scryer.events(&EventScope::Forge(id.clone()), &EventFilter::default()).unwrap();
            if evs.len() >= 3 || std::time::Instant::now() > deadline {
                break evs;
            }
        };
        assert_eq!(events.len(), 3, "expected 3 events across 2 workloads");
        assert_eq!(events[0].target, "forge.integration");
    }
}
