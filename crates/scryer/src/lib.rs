//! @arch:layer(kg_store)
//! @arch:role(substrate)
//! @arch:see(.yah/docs/architecture/A049-yah-scryer.md)
//!
//! `scryer` — per-machine event store with tiered retention.
//!
//! Tier model (this crate — F1 / scryer-1):
//!   - Recent ring: in-memory ring of last 10k events / 64 MB.
//!   - Short disk: per-machine SQLite at `/var/lib/yah/scryer/events.db`.
//!   - (Long tier / Parquet is R093-F5, off by default, not in this crate.)
//!
//! Query surface (F1, TaskRun scope only):
//!   - `Scryer::events(scope, filter)` — query events; delegates to task-runs store for TaskRun scope.
//!   - `Scryer::tail(scope, cursor)` — cursor-based live follow.
//!   - `Scryer::subscribe(scope)` — push stream for tile viewers and gnomes.
//!
//! F2 adds:
//!   - `EventScope::Service(MeshIdent)` ingestion via [`adapters::ContainerdLogsAdapter`]
//!     (containerd gRPC log API) and [`adapters::WardenRpcAdapter`] (yubaba's
//!     own structured emissions).
//!   - [`beholders::ServiceBeholder`] trait + bundled `vanilla` (rfc5424-ish)
//!     and `unstructured` (passthrough) parsers.
//!
//! F3 adds:
//!   - [`beholders::PinoBeholder`] — Node.js pino NDJSON structured parser.
//!   - [`beholders::TracingJsonBeholder`] — Rust tracing-subscriber JSON parser.
//!   - Image-label opt-in (`yah.beholder=<name>`) forcing beholder selection.
//!   - `ServiceBeholder::unknown_format_reason()` for schema versioning + decline.
//!   - [`quota::ServiceQuotaManager`] — per-`MeshIdent` rate limiting (1000 ev/s
//!     default) with Synth drop-count events on window rollover.
//!
//! F7 adds (P2 narrow):
//!   - [`adapters::JournaldAdapter`] — host-level systemd units (sshd, kernel,
//!     yubaba's own unit). Entries scoped to `Service(MeshIdent("<unit>.host"))`.
//!     Explicit allow-list; yah-managed services stay on `containerd_logs`.
//!
//! F8 adds (P2 tier-2):
//!   - [`ingestion::IngestionServer`] — Unix socket ingestion server for
//!     `yah-log` service-scope events. Yubaba injects `YAH_SERVICE_IDENT` +
//!     `YAH_SCRYER_SOCKET` into workload env; the shim connects and writes
//!     JSON-lines with `scope_kind = "service"` scope envelope.
//!
//! F5 adds (P3 opt-in):
//!   - [`long_tier::LongTierStore`] — per-day Parquet shard rollover from
//!     short-disk to R2/MinIO, keyed by `(machine_id, day)`.
//!   - [`long_tier::ObjectStore`] trait — production wires an S3-compat impl;
//!     tests use [`long_tier::InMemoryObjectStore`].
//!   - [`service::Scryer::aggregate`] — cross-boundary rollup: routes events
//!     older than `short_disk_retention_ms` to the Parquet tier.

pub mod adapters;
pub mod beholders;
pub mod federation;
#[cfg(unix)]
pub mod ingestion;
pub mod long_tier;
pub mod quota;
pub mod ring;
pub mod service;
pub mod store;

#[cfg(unix)]
pub use ingestion::{IngestionError, IngestionServer};
pub use long_tier::{
    InMemoryObjectStore, LongTierConfig, LongTierError, LongTierStore, MS_PER_DAY, ObjectStore,
};
pub use adapters::{
    Adapter, AdapterError, BackoffConfig, ContainerLogSource, ContainerdLogsAdapter,
    JournaldAdapter, JournaldEntry, JournaldSource, Supervisor, WardenEvent, WardenRpcAdapter,
};
pub use beholders::{
    BeholderCtx, LogLine, PinoBeholder, PinoFactory, ServiceBeholder, ServiceBeholderFactory,
    ServiceBeholderRegistry, ServiceHints, TracingJsonBeholder, TracingJsonFactory,
    UnstructuredBeholder, VanillaBeholder,
};
pub use federation::{
    DenyAllAcl, FederationAcl, FederationError, FederationPeer, FederationRule, OperatorTagAcl,
    PeerIdentity, federated_events, merge_events,
};
pub use quota::{QuotaDecision, ServiceQuotaManager, DEFAULT_QUOTA_PER_SECOND};
pub use ring::{EventRing, RingConfig};
pub use service::{
    AggregateBucket, EventFilter, QueryCursor, Scryer, ScryerConfig, ScryerError, TailResult,
};
pub use store::{EventStore, ScopeFilter, ScopeInfo, ScryerStoreError};
