//! @arch:layer(kg_store)
//! @arch:role(substrate)
//! @arch:see(.yah/docs/working/yah-task-runs.md)
//!
//! `task-runs` — lossless command-output capture with a tiered observation
//! model (bytes → beholders → triage → shims).
//!
//! **Tier 1 (this crate):** core types + per-camp SQLite store.
//! Higher tiers (beholders, pruner, shims) build on top.

pub mod beholders;
pub mod driver;
pub mod rule_filter;
pub mod store;
pub mod types;
pub mod user_beholders;

pub use driver::{DriverError, SpawnOpts, TaskDriver};
pub use beholders::{
    AttachResult, Beholder, BeholderFactory, BeholderMode, BeholderRegistry, BeholderSelect,
    CargoBeholder, CargoBeholderFactory, ToolVersionRange, default_registry,
    registry_with_user_beholders, resolve_argv,
};
pub use user_beholders::load_user_beholders;
pub use store::{
    AggregateBucket, AggregateFilter, AggregateGroupBy, ChunkFilter, EventFilter, FieldFilter,
    FieldIndexInfo, GcConfig, GcResult, RunFilter, StoreError, TaskStore, TimelineTick,
    validate_field_path,
};
pub use rule_filter::{EventsRuleFilter, FieldPredicate, ParseError, ScopeSpec, TaskEventsRuleFilter};
pub use types::{
    BeholderStatus, ChunkRef, Diagnostic, Event, EventScope, EventSource, Initiator, KeepRange,
    Level, OutputChunk, RunStatus, SeqRange, Stream, TaskRunId, TaskRunMeta, Triage,
    RESERVED_FIELD_PATHS,
};
