//! @arch:layer(kg_store)
//! @arch:role(substrate)
//! @arch:see(.yah/docs/architecture/A049-yah-scryer.md)
//! @arch:see(.yah/docs/working/yah-task-runs.md)
//!
//! `observation` — shared event types for the scryer observation substrate.
//!
//! Holds the types that both `task-runs` and `scryer` depend on.
//! `task-runs` re-exports everything from here for backward compat.

pub mod types;

pub use types::{
    ChunkRef, Diagnostic, Event, EventScope, EventSource, ForgeId, Initiator, Level, RunStatus,
    TaskRunId, RESERVED_FIELD_PATHS,
};
