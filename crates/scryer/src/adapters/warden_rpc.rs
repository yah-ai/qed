//! `adapter::warden_rpc` — receives structured events emitted by yubaba.
//!
//! Yubaba has the structured information for its own actions
//! (`workload.deploy`, `mesh.peer_join`, `cloudflared.register`,
//! `raft.term_change`, …) — re-parsing its logs would lose that. The arch doc
//! commits scryer to plumb these directly via an in-process channel (when
//! colocated) or a Unix socket (when split). F2 ships the in-process variant.
//!
//! All events from this adapter are scoped to `MeshIdent("yubaba.local")`.

use crate::adapters::{synth_event, Adapter, AdapterError};
use crate::service::Scryer;
use async_trait::async_trait;
use observation::{EventScope, Level};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use workload_spec::MeshIdent;

// ─── WardenEvent ──────────────────────────────────────────────────────────────

/// Discriminated union of yubaba's first-party emissions.
///
/// Held intentionally narrow at F2 — covers the four listed in the arch doc.
/// Adding a variant here is the supported way to extend yubaba's coverage in
/// scryer rather than emitting unstructured logs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WardenEvent {
    WorkloadDeploy {
        workload: String,
        ident: String,
        replicas: u32,
    },
    MeshPeerJoin {
        peer: String,
        endpoint: String,
    },
    CloudflaredRegister {
        hostname: String,
        ident: String,
    },
    RaftTermChange {
        term: u64,
        leader: Option<String>,
    },
}

impl WardenEvent {
    pub fn target(&self) -> &'static str {
        match self {
            WardenEvent::WorkloadDeploy { .. } => "yubaba.workload.deploy",
            WardenEvent::MeshPeerJoin { .. } => "yubaba.mesh.peer_join",
            WardenEvent::CloudflaredRegister { .. } => "yubaba.cloudflared.register",
            WardenEvent::RaftTermChange { .. } => "yubaba.raft.term_change",
        }
    }

    pub fn level(&self) -> Level {
        Level::Info
    }

    pub fn msg(&self) -> String {
        match self {
            WardenEvent::WorkloadDeploy { workload, .. } => format!("deploy {}", workload),
            WardenEvent::MeshPeerJoin { peer, .. } => format!("peer joined {}", peer),
            WardenEvent::CloudflaredRegister { hostname, .. } => {
                format!("cf route registered {}", hostname)
            }
            WardenEvent::RaftTermChange { term, .. } => format!("raft term -> {}", term),
        }
    }

    pub fn fields(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

// ─── WardenRpcAdapter ─────────────────────────────────────────────────────────

/// Adapter that drains a `mpsc::Receiver<WardenEvent>` into scryer.
///
/// Yubaba constructs the channel and hands the receiver to scryer at startup;
/// the sender side stays inside yubaba's RPC dispatch. When the channel
/// closes (yubaba shutting down), the adapter exits with `Ok(())` so the
/// supervisor stops cleanly.
pub struct WardenRpcAdapter {
    scryer: Arc<Scryer>,
    rx: Option<mpsc::Receiver<WardenEvent>>,
    started_at: Instant,
    seq: u32,
    /// Override for the scope identity. Defaults to `yubaba.local`.
    ident: MeshIdent,
}

impl WardenRpcAdapter {
    pub fn new(scryer: Arc<Scryer>, rx: mpsc::Receiver<WardenEvent>) -> Self {
        Self {
            scryer,
            rx: Some(rx),
            started_at: Instant::now(),
            seq: 0,
            ident: MeshIdent("yubaba.local".to_string()),
        }
    }

    fn offset_ms(&self) -> u32 {
        let elapsed = self.started_at.elapsed().as_millis();
        elapsed.min(u32::MAX as u128) as u32
    }
}

#[async_trait]
impl Adapter for WardenRpcAdapter {
    fn name(&self) -> &str {
        "warden_rpc"
    }

    fn scope(&self) -> EventScope {
        EventScope::Service(self.ident.clone())
    }

    async fn run(&mut self) -> Result<(), AdapterError> {
        let mut rx = self
            .rx
            .take()
            .ok_or_else(|| AdapterError::Permanent("warden_rpc adapter already drained".into()))?;
        let scope = EventScope::Service(self.ident.clone());

        while let Some(we) = rx.recv().await {
            let event = synth_event(
                we.target(),
                we.level(),
                we.msg(),
                we.fields(),
                self.offset_ms(),
                self.seq,
            );
            self.seq += 1;
            self.scryer.push(scope.clone(), event)?;
        }

        // Sender dropped — yubaba is going away. Clean exit.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::{EventFilter, Scryer, ScryerConfig};
    use tempfile::TempDir;

    fn make_scryer(dir: &TempDir) -> Arc<Scryer> {
        let cfg = ScryerConfig::new(dir.path().join("events.db"));
        Arc::new(Scryer::new(cfg, None).unwrap())
    }

    #[tokio::test]
    async fn warden_emissions_appear_under_warden_local() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let (tx, rx) = mpsc::channel::<WardenEvent>(16);
        let mut adapter = WardenRpcAdapter::new(scryer.clone(), rx);

        // Push a few events and close the sender.
        tx.send(WardenEvent::WorkloadDeploy {
            workload: "api".into(),
            ident: "api.pdx".into(),
            replicas: 1,
        })
        .await
        .unwrap();
        tx.send(WardenEvent::MeshPeerJoin {
            peer: "node-2".into(),
            endpoint: "10.0.0.2:51820".into(),
        })
        .await
        .unwrap();
        tx.send(WardenEvent::RaftTermChange { term: 7, leader: Some("node-1".into()) })
            .await
            .unwrap();
        drop(tx);

        let result = adapter.run().await;
        assert!(result.is_ok(), "clean shutdown expected, got {:?}", result);

        scryer.flush_ring().unwrap();
        let scope = EventScope::Service(MeshIdent("yubaba.local".into()));
        let events = scryer.events(&scope, &EventFilter::default()).await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].target, "yubaba.workload.deploy");
        assert_eq!(events[1].target, "yubaba.mesh.peer_join");
        assert_eq!(events[2].target, "yubaba.raft.term_change");
        // fields round-trip the original WardenEvent shape.
        assert_eq!(events[0].fields["workload"], "api");
        assert_eq!(events[2].fields["term"], 7);
    }

    #[tokio::test]
    async fn warden_rpc_double_run_is_permanent_error() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let (tx, rx) = mpsc::channel::<WardenEvent>(1);
        drop(tx);
        let mut adapter = WardenRpcAdapter::new(scryer, rx);
        adapter.run().await.unwrap();
        let err = adapter.run().await.unwrap_err();
        assert!(matches!(err, AdapterError::Permanent(_)));
    }
}
