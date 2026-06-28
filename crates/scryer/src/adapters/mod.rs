//! Ingestion adapters — long-running subscribers that turn external log
//! sources into `Event` rows in scryer's store.
//!
//! Each adapter implements [`Adapter::run`], which drives the underlying source
//! to completion (clean shutdown → `Ok`) or to a stream break (`Err`). The
//! [`Supervisor`] wraps an adapter with exponential-backoff restart capped at
//! 30s and emits a synthetic `service.restart` event on each restart so
//! consumers can see the discontinuity.
//!
//! F2 ships two adapters:
//!
//! - [`containerd_logs`] — pulls stdout/stderr from any container yubaba
//!   deployed; the `LogSource` trait is the seam so tests don't need a real
//!   containerd.
//! - [`warden_rpc`] — receives structured events emitted by yubaba itself
//!   (workload.deploy, mesh.peer_join, raft.term_change, …).

use crate::service::{Scryer, ScryerError};
use async_trait::async_trait;
use observation::{Event, EventScope, EventSource, Level, TaskRunId};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;
use workload_spec::MeshIdent;

pub mod containerd_logs;
pub mod journald;
pub mod warden_rpc;

pub use containerd_logs::{ContainerLogSource, ContainerdLogsAdapter, DockerLogSource};
pub use journald::{JournaldAdapter, JournaldEntry, JournaldSource};
pub use warden_rpc::{WardenEvent, WardenRpcAdapter};

// ─── AdapterError ─────────────────────────────────────────────────────────────

/// Failure mode of an adapter run.
///
/// `StreamBroken` triggers supervised restart; `Permanent` exits the
/// supervisor (the operator made an unrecoverable decision, e.g. the
/// containerd socket is misconfigured).
#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("stream broken: {0}")]
    StreamBroken(String),
    #[error("permanent: {0}")]
    Permanent(String),
    #[error("scryer push: {0}")]
    Push(#[from] ScryerError),
}

impl AdapterError {
    pub fn is_recoverable(&self) -> bool {
        matches!(self, AdapterError::StreamBroken(_) | AdapterError::Push(_))
    }
}

// ─── Adapter trait ────────────────────────────────────────────────────────────

/// Long-running adapter contract.
///
/// `run` returns when either:
/// - the underlying source closes cleanly (`Ok(())` — supervisor exits), or
/// - the source breaks (`Err` — supervisor backs off and restarts).
///
/// Adapters write events into scryer themselves (held internally as an
/// `Arc<Scryer>`); they don't return a stream because the per-event
/// dispatch is more straightforward when scoped to a Service identity.
#[async_trait]
pub trait Adapter: Send {
    /// Stable identifier — used for diagnostics and the synthetic
    /// `service.restart` target.
    fn name(&self) -> &str;
    /// Mesh identity events from this adapter are scoped to.
    fn scope(&self) -> EventScope;
    /// Drive the source. Idempotent across restarts — the supervisor calls
    /// this in a loop, with backoff between failures.
    async fn run(&mut self) -> Result<(), AdapterError>;
}

// ─── BackoffConfig ────────────────────────────────────────────────────────────

/// Exponential-backoff knobs for the supervisor restart loop.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    pub initial: Duration,
    pub max: Duration,
    pub multiplier: f64,
    /// Hard cap on retry count; `None` = unlimited.
    pub max_attempts: Option<u32>,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(30),
            multiplier: 2.0,
            max_attempts: None,
        }
    }
}

impl BackoffConfig {
    /// Test-only helper: zero-delay backoff with a small attempt cap so
    /// supervised tests run fast and terminate.
    pub fn test() -> Self {
        Self {
            initial: Duration::from_millis(0),
            max: Duration::from_millis(0),
            multiplier: 1.0,
            max_attempts: Some(3),
        }
    }
}

// ─── Supervisor ───────────────────────────────────────────────────────────────

/// Drives an adapter with exponential-backoff restart and emits the
/// synthetic `service.restart` event on each restart cycle.
///
/// A clean `Ok(())` exits the supervisor. A `Permanent` error exits with the
/// error. A `StreamBroken` (or push error) loops with backoff, capped by
/// `BackoffConfig.max_attempts`.
pub struct Supervisor {
    name: String,
    scope: EventScope,
    scryer: Arc<Scryer>,
    cfg: BackoffConfig,
    /// Running counter for the synthetic `service.restart` events. Used as a
    /// monotonic offset_ms tag so consumers can order restart events.
    restart_count: u32,
    /// Seq value for the next synth `service.restart` event. The store's
    /// primary key is `(scope_kind, scope_id, seq)` with `INSERT OR IGNORE`
    /// semantics, so synth events must not collide with the beholder's per-
    /// scope seq stream. The supervisor allocates from the top of the u32
    /// space and decrements; beholders allocate from 0 upward, leaving
    /// roughly two billion of headroom in practice.
    synth_seq: u32,
}

impl Supervisor {
    pub fn new(scryer: Arc<Scryer>, scope: EventScope, name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            scope,
            scryer,
            cfg: BackoffConfig::default(),
            restart_count: 0,
            synth_seq: u32::MAX,
        }
    }

    pub fn with_backoff(mut self, cfg: BackoffConfig) -> Self {
        self.cfg = cfg;
        self
    }

    /// Run the adapter to completion under supervision.
    pub async fn run(&mut self, adapter: &mut dyn Adapter) -> Result<(), AdapterError> {
        let mut delay = self.cfg.initial;
        let mut attempt: u32 = 0;
        loop {
            let result = adapter.run().await;
            match result {
                Ok(()) => return Ok(()),
                Err(e) if matches!(e, AdapterError::Permanent(_)) => return Err(e),
                Err(_) => {
                    attempt += 1;
                    if let Some(cap) = self.cfg.max_attempts {
                        if attempt > cap {
                            return Err(AdapterError::StreamBroken(format!(
                                "supervisor exhausted {cap} attempts for {}",
                                self.name
                            )));
                        }
                    }
                    self.emit_restart_event()?;
                    if delay > Duration::ZERO {
                        sleep(delay).await;
                    }
                    delay = (delay.mul_f64(self.cfg.multiplier)).min(self.cfg.max);
                    if delay == Duration::ZERO {
                        delay = self.cfg.initial;
                    }
                }
            }
        }
    }

    fn emit_restart_event(&mut self) -> Result<(), AdapterError> {
        self.restart_count += 1;
        let seq = self.synth_seq;
        self.synth_seq = self.synth_seq.saturating_sub(1);
        let event = Event {
            run_id: TaskRunId::new(),
            seq,
            offset_ms: 0,
            level: Level::Info,
            target: format!("scryer.{}", self.name),
            msg: "service.restart".to_string(),
            fields: serde_json::json!({ "attempt": self.restart_count, "adapter": self.name }),
            anchor: None,
            source: EventSource::Synth,
        };
        self.scryer.push(self.scope.clone(), event)?;
        Ok(())
    }
}

// ─── Helpers shared with adapter impls ────────────────────────────────────────

/// Build a low-level `Event` from a beholder-emitted line + scope context.
///
/// Adapters call this when they want to push a one-off `Event` (e.g. warden_rpc
/// translating a `WardenEvent` into the wire shape) without round-tripping
/// through the beholder framework.
pub(crate) fn synth_event(
    target: impl Into<String>,
    level: Level,
    msg: impl Into<String>,
    fields: Value,
    offset_ms: u32,
    seq: u32,
) -> Event {
    Event {
        run_id: TaskRunId::new(),
        seq,
        offset_ms,
        level,
        target: target.into(),
        msg: msg.into(),
        fields,
        anchor: None,
        source: EventSource::Synth,
    }
}

#[allow(dead_code)]
pub(crate) fn warden_local_scope() -> EventScope {
    EventScope::Service(MeshIdent("yubaba.local".to_string()))
}
