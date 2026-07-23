//! @arch:layer(kg_store)
//! @arch:role(runtime)
//! @arch:see(.yah/docs/architecture/A035-yah-forge.md)
//!
//! `velveteen-exec` — the execution drivers for the [`velveteen`] task
//! vocabulary.
//!
//! `velveteen` describes work: `ForgeSpec`, `ForgeCommand`, `TaskPlacement`.
//! This crate *runs* it. The split (R619) exists so that consumers which only
//! need to describe or route a task — a queue, a scheduler, a dashboard, a
//! wire client — don't drag in tokio/process, the docker shim, `task-runs`'
//! SQLite store, or `yah-scryer`.
//!
//! Three species share `ForgeId`, `EventScope::Forge`, and the agent-facing
//! query surface (`forge.run/status/events/diagnostics/triage/kill/list`)
//! (tool names remain "forge" for now):
//!
//! - **local-forge**: subprocess on the dev box; backed by `task-runs`.
//! - **remote-forge**: one-shot workload on a yubaba machine (R094-F3).
//! - **integration-forge**: N-workload stand-up scoped to a test or flow
//!   (R094-F4).

pub mod default_image;
pub mod executor;
pub mod integration;
pub mod list;
pub mod local;
pub mod meta;
pub mod remote;
pub mod transforms;
pub mod triage;

pub use executor::{
    ExecContext, ExecEvent, ExecOutcome, ForgeExecutor, ForgeExecutorError, OutputStream,
};
pub use integration::{
    ClusterClient, IntegrationForgeDriver, IntegrationForgeError, IntegrationRunHandle,
};
pub use list::{ForgeListFilter, forge_list};
pub use local::LocalForgeDriver;
pub use meta::ForgeMeta;
pub use remote::{ForgeRunHandle, RemoteForgeDriver, RemoteForgeError, WardenClient};
pub use transforms::{
    substitute_argv, RecipeError, RecipeLocation, RecipePlacement, RecipeStep, TransformRecipe,
    TransformRecipeLoader, ENV_TRANSFORM_IN_0, ENV_TRANSFORM_OUT,
};
pub use triage::{ForgeTriageError, event_to_diagnostic, forge_diagnostics, forge_triage};
