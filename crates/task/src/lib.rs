//! @arch:layer(kg_store)
//! @arch:role(runtime)
//! @arch:see(.yah/docs/architecture/A035-yah-forge.md)
//!
//! `task` — transient execution umbrella for all three task species.
//!
//! Three species share `ForgeId`, `EventScope::Forge`, and the agent-facing
//! query surface (`forge.run/status/events/diagnostics/triage/kill/list`) (tool names remain "forge" for now):
//!
//! - **local-forge**: subprocess on the dev box; backed by `crates/yah/task-runs/`.
//! - **remote-forge**: one-shot workload on a warden machine (R094-F3).
//! - **integration-forge**: N-workload stand-up scoped to a test or flow (R094-F4).
//!
//! `ForgeId` lives in `crates/yah/observation/` so scryer can reference it in
//! `EventScope::Forge` without depending on this crate.
//!
//! @yah:ticket(R299-T1, "Rename crates/yah/forge/ → crates/yah/task/")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-23T01:39:31Z)
//! @yah:status(review)
//! @yah:parent(R299)
//! @yah:next("DONE: Renamed directory crates/yah/forge → crates/yah/task")
//! @yah:next("DONE: Updated Cargo.toml package name forge → task")
//! @yah:next("DONE: No use statements to update (qed depends on it but doesn't import it yet)")
//! @yah:next("DONE: Updated qed/Cargo.toml dependency forge → task")
//! @yah:next("DONE: cargo check --workspace passes")
//! @yah:handoff("Completed: crates/yah/forge/ renamed to crates/yah/task/. All references updated in workspace Cargo.toml (members + default-members), qed/Cargo.toml dependency updated (forge → task), package name changed from 'forge' to 'task', lib name updated, description reflects new TaskId vocabulary. cargo check -p task clean, cargo check -p qed clean, zero stray 'forge' references in crate Cargo.toml files.")
//!
//! @yah:ticket(R380-T1, "Add TaskPlacement + TaskLocation + TaskRuntime types in task crate (additive, with From<ForgeWhere> bidirectional conversion)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-01T21:06:00Z)
//! @yah:status(review)
//! @yah:parent(R380)
//! @yah:next("Add the new types alongside (not replacing) ForgeWhere; both compile and coexist after this ticket lands.")
//! @yah:next("From<ForgeWhere> for TaskPlacement maps Local → {Local, Native}, Remote(id) → {Remote(id), Container}, RemoteAny{tier} → {RemoteAny{tier}, Container}. Integration is the meta-only species; do NOT add it to TaskLocation — it gets a separate path later (T8).")
//! @yah:next("Reverse conversion TryFrom<TaskPlacement> for ForgeWhere returns Err for any quadrant the old enum can't express (local+container, remote+native) so callers fail loudly when bridging old code.")
//! @yah:next("Add unit tests for the round-trip (each old ForgeWhere variant → TaskPlacement → back must equal the start).")
//! @yah:handoff("T1 complete: TaskPlacement, TaskLocation, TaskRuntime added to crates/yah/task/src/lib.rs alongside ForgeWhere. From<ForgeWhere> for TaskPlacement maps Local→{Local,Native}, Remote(id)→{Remote{node:id},Container}, RemoteAny{tier}→{RemoteAny{tier},Container}; Integration panics (it's a species, not a placement — must be branched on upstream). TryFrom<TaskPlacement> for ForgeWhere returns TaskPlacementToForgeWhereError::{LocalContainer, RemoteNative} for quadrants the old enum can't express. 7 new tests cover round-trip per ForgeWhere variant, both error quadrants, the Integration panic, and JSON round-trip of the new struct. All 47 task crate tests pass; cargo check --workspace clean.")
//! @yah:verify("cargo test -p task --lib")
//! @yah:gotcha("TaskLocation::Remote is a struct variant (Remote { node: MeshIdent }) rather than a tuple variant — serde internal tagging can't represent a tuple variant whose payload serializes to a primitive (MeshIdent is a transparent String newtype). Pre-existing ForgeWhere::Remote(MeshIdent) has the same latent bug but is never exercised by JSON round-trip tests. T8 sweep should consider whether to fix that or just delete ForgeWhere.")
//!
//! @yah:ticket(R380-T6, "Wire local + container execution path in task::local (docker run shim)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-01T21:06:23Z)
//! @yah:status(review)
//! @yah:parent(R380)
//! @yah:next("task::local execution branch today is always native subprocess. Split into a local_native and local_container path based on placement.runtime.")
//! @yah:next("local_container shells to `docker run --rm` (or the configured engine — pond uses OrbStack on macOS; Linux dev boxes can use rootless podman / buildkitd). Pull through the standard image policy (digest pinning when set, :latest otherwise) — same shape as remote pulls.")
//! @yah:next("Stdout/stderr stream into scryer the same way local_native does. Exit code maps the same.")
//! @yah:next("Integration test: a ForgeSpec with local+container + an `image: yah-rust-bun` runs `cargo --version` inside the container and reports Done with the right output.")
//! @yah:handoff("Wired local+container execution. New module crates/yah/task/src/local.rs provides local_container_command(image, argv, cwd, env) -> tokio::process::Command that assembles `docker run --rm -v <cwd>:<cwd> -w <cwd> -e KEY=VAL <image> <argv>`. Image arg honours digest pinning (image_ref_arg prefers <reg>/<repo>@<digest>, falls back to <reg>/<repo>:<tag>) — same shape as remote pulls. cwd is bind-mounted at the same path inside the container so relative paths in stdout match the native run.")
//! @yah:handoff("qed::runner::execute_step_local_container replaces the InvalidConfig stub. Mirrors execute_step_local: spawns the command, drains stdout/stderr concurrently into the QedEvent sink (StepOutput { stream: Stdout|Stderr, line }), captures stderr for the StepFailed msg. Image defaulted to task::default_image::default_forge_image() — a per-step image catalog (yah-rust-bun / yah-python / yah-cuda) is R381 territory. The local_container_errors_until_t6 test became local_container_step_routes_through_docker_path, which exercises the new branch without requiring docker (uses a bogus argv so spawn or pull failure surfaces as StepFailed).")
//! @yah:handoff("Tests: task crate 53/53 lib pass (6 new in local::tests for command shape — digest vs tag, mount+chdir, env, program name, argv, plus a #[ignore] docker smoke that pulls alpine:3 and asserts exit code 7 + 'hello-from-container' on stdout). qed: 33/34 pass — the lone failure is the pre-existing tests::test_builtin_release_build_pipeline (4-vs-6 step count) flagged as a gotcha in T2. yah cli builds clean. Docker smoke verified against the local OrbStack-backed docker (`cargo test -p task local_container_run_exits_with_code -- --include-ignored` passes end-to-end).")
//! @yah:next("R380-T7 is independent and ready: decide remote+native policy. task::remote::build_workload_spec already returns InvalidSpec for runtime != Container — T7 makes that the explicit refusal seam with a clear error message + a v2 hook (WardenClient::exec_native parallel method, type-level only).")
//! @yah:next("Image catalog (R381 follow-up): QedStep gains an `image: Option<String>` field that resolves through a catalog loader to an ImageRef. yah-rust-bun / yah-python / yah-cuda become first-class image names. Then the integration test the T6 ticket envisioned (a ForgeSpec with image: yah-rust-bun running `cargo --version`) becomes runnable as a real qed pipeline.")
//! @yah:next("Optional: lift the docker discovery into local-driver's LocalRuntime so the qed step path probes OrbStack / Docker Desktop / Colima / Podman sockets instead of relying on bare `docker` on PATH. Today's path works on any dev box where one of those installs the docker shim; the discovery wiring is a quality-of-life upgrade rather than a correctness fix.")
//! @yah:verify("cargo test -p task --lib  # 53 pass, 2 ignored")
//! @yah:verify("cargo test -p task --lib local_container_run_exits_with_code -- --include-ignored  # docker smoke, real container exit 7")
//! @yah:verify("cargo build -p yah --bin yah  # clean")
//!
//! @yah:ticket(R380-T8, "Drop ForgeWhere + move Integration off the placement enum + rename TS bindings")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-01T21:06:33Z)
//! @yah:status(review)
//! @yah:parent(R380)
//! @yah:next("Cleanup phase — gated on T2 through T7 landing.")
//! @yah:next("Remove ForgeWhere type from task crate + tower-rules crate.")
//! @yah:next("Integration species moves to ForgeMeta.species: ForgeSpecies (Local | Remote | Integration) — sibling field to placement, not embedded in it.")
//! @yah:next("Delete bindings/ForgeWhere.ts (already replaced by TaskPlacement.ts in T4).")
//! @yah:next("Sweep .yah/docs/architecture/A035-yah-forge.md + arch refs for stale ForgeWhere mentions; replace with TaskPlacement.")
//! @yah:handoff("R380-T8 complete: ForgeWhere dropped from task + tower-rules; ForgeSpecies (Local|Remote|Integration) added as a sibling field on ForgeMeta; ForgeListFilter swapped its placement filter for a species filter; bindings/ForgeWhere.ts + gen/ForgeWhere.ts deleted (regenerated bindings confirm zero ForgeWhere mentions); arch docs A035-yah-forge.md + A052-yah-tower.md swept (forge_where:ForgeWhere → placement:TaskPlacement, ForgeSpec/Trigger code blocks updated, Where-things-live table now points at crates/yah/task/src/lib.rs). Tests: task 48/50 lib pass (2 ignored docker smoke), tower-rules 25+24 pass, tower 30+23+6+8+18+18+18+23 pass, yah cli builds clean, cargo check --workspace clean. The pre-existing qed test_builtin_release_build_pipeline failure (4-vs-6 step count) flagged in T2's gotcha is still present — unrelated to T8.")
//! @yah:verify("cargo test -p task --lib  # 48 pass, 2 ignored")
//! @yah:verify("cargo test -p tower-rules  # 49 pass total")
//! @yah:verify("cargo test -p tower  # 144 pass total")
//! @yah:verify("cargo check --workspace  # clean")
//! @yah:verify("cargo run -q -p tower-rules --bin tower-rules-export-ts && ! ls packages/yah/ui/src/gen/ForgeWhere.ts crates/yah/tower-rules/bindings/ForgeWhere.ts 2>/dev/null")
//! @yah:gotcha("WireForgeWhere in packages/yah/ui/src/env/types.ts is intentionally left as-is: it's a hand-rolled UI stand-in (ForgePanel.tsx builds WireForgeMeta client-side from task.list results, never reads forge_list from the backend). When R094-F7 lands the forge_list Tauri command, that wire type must be re-aligned with the new ForgeMeta shape (where_: TaskPlacement + species: ForgeSpecies). Comment in env/types.ts spells this out.")
//!
//! @yah:ticket(R406-T3, "TaskRuntime peer parity: confirm Native + Container symmetry after R380 lands")
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:at(2026-06-02T03:26:26Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R406)
//! @arch:see(.yah/docs/working/W154-warden-dual-runtime.md)
//! @yah:next("Sign off → archive R406-T3. R406-T1/T2/T3 close out P1 of R406. P2 begins with the Linux-specific drivers: T4 (cgroup), T5 (fork+exec+sandbox), T6 (pidfd loop).")
//! @yah:handoff("TaskRuntime peer parity verified post-R380. Findings: (1) task::TaskRuntime at crates/yah/task/src/lib.rs:207 declares Native + Container as peer variants with snake_case serde — used as a sibling field in TaskPlacement {location, runtime}. (2) tower_rules::TaskRuntime at crates/yah/tower-rules/src/lib.rs:293 mirrors the same Native|Container shape with TS-export. (3) TS binding crates/yah/tower-rules/bindings/TaskRuntime.ts emits exactly \"native\" | \"container\". (4) Four-quadrant TaskPlacement doc-table (task/lib.rs:219-223) explicitly covers all (location × runtime) pairs incl. Local+Native / Local+Container / Remote+Native / Remote+Container. (5) Runner dispatch at qed/runner.rs:296-307 routes (Local, Native)→execute_step_local, (Local, Container)→execute_step_local_container, and (Remote, _)→execute_step_remote(step, runtime) — runtime threaded into remote dispatch. (6) task::local module doc explicitly enumerates Native (tokio::process::Command) and Container (docker run shim) as siblings under the local location; image_ref_arg supports digest pinning identical to the warden remote path. (7) Round-trip tests at tower-rules/tests/round_trip.rs prove both variants round-trip wire-compatibly: task_runtime_round_trip + task_placement_four_quadrants_round_trip + task_placement_remote_any_native_wire_format (24/24 pass on the suite). Conclusion: Native + Container are first-class peers across the Rust types, TS bindings, doc model, dispatch, and tests — no asymmetry to fix. T3 is verification-only; no code change required.")
//! @yah:verify("cargo check -p task -p tower-rules -p qed")
//! @yah:verify("cargo test -p tower-rules --test round_trip")
//!
//! @yah:relay(R438, "ForgeCommand lowering: derived static-assets (W164) + mesofact build_mode (W165)")
//! @yah:at(2026-06-04T21:06:20Z)
//! @yah:status(open)
//! @arch:see(.yah/docs/working/W164-derived-static-assets.md)
//! @arch:see(.yah/docs/working/W165-mesofact-build-mode-lowering.md)

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
pub use integration::{ClusterClient, IntegrationForgeDriver, IntegrationForgeError, IntegrationRunHandle};
pub use list::{ForgeListFilter, forge_list};
pub use local::LocalForgeDriver;
pub use meta::ForgeMeta;
pub use observation::ForgeId;
pub use task_runs::Initiator;
pub use remote::{ForgeRunHandle, RemoteForgeDriver, RemoteForgeError, WardenClient};
pub use transforms::{
    substitute_argv, RecipeError, RecipeLocation, RecipePlacement, RecipeStep, TransformRecipe,
    TransformRecipeLoader, ENV_TRANSFORM_IN_0, ENV_TRANSFORM_OUT,
};
pub use triage::{ForgeTriageError, event_to_diagnostic, forge_diagnostics, forge_triage};

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use workload_spec::{ImageRef, Millis, MeshIdent, TierTag, WorkloadSpec};

// ─── ForgeStatus ──────────────────────────────────────────────────────────────

/// Terminal + in-flight status for a forge run.
///
/// Extends `task_runs::RunStatus` with `TimedOut` — a forge run that exceeds
/// its `timeout` field produces this status rather than `Lost`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ForgeStatus {
    Pending,
    Running,
    Done { exit_code: i32, ended_at: u64 },
    Killed { signal: i32, ended_at: u64 },
    TimedOut { ended_at: u64 },
    Lost { reason: String },
}

impl ForgeStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ForgeStatus::Done { .. }
                | ForgeStatus::Killed { .. }
                | ForgeStatus::TimedOut { .. }
                | ForgeStatus::Lost { .. }
        )
    }

    /// String discriminant matching the on-wire `status` tag.
    pub fn discriminant(&self) -> &'static str {
        match self {
            ForgeStatus::Pending => "pending",
            ForgeStatus::Running => "running",
            ForgeStatus::Done { .. } => "done",
            ForgeStatus::Killed { .. } => "killed",
            ForgeStatus::TimedOut { .. } => "timed_out",
            ForgeStatus::Lost { .. } => "lost",
        }
    }
}

impl From<task_runs::RunStatus> for ForgeStatus {
    fn from(s: task_runs::RunStatus) -> Self {
        match s {
            task_runs::RunStatus::Pending => ForgeStatus::Pending,
            task_runs::RunStatus::Running => ForgeStatus::Running,
            task_runs::RunStatus::Done { exit_code, ended_at } => {
                ForgeStatus::Done { exit_code, ended_at }
            }
            task_runs::RunStatus::Killed { signal, ended_at } => {
                ForgeStatus::Killed { signal, ended_at }
            }
            task_runs::RunStatus::Lost { reason } => ForgeStatus::Lost { reason },
        }
    }
}

// ─── ForgeSpecies ─────────────────────────────────────────────────────────────

/// Which forge species a run belongs to.
///
/// Sibling to [`TaskPlacement`] on [`ForgeMeta`]: the placement says *where*
/// and *how* the run executes; the species says *which driver* produced it.
/// The two are orthogonal — a `Remote` species always runs as a containerd
/// workload on warden, but a `Local` species can run native or container per
/// `TaskPlacement.runtime`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForgeSpecies {
    /// Subprocess on the dev box, driven by `task-runs`.
    Local,
    /// One-shot workload on a warden node, driven by [`RemoteForgeDriver`].
    Remote,
    /// N-workload stand-up driven by [`IntegrationForgeDriver`].
    Integration,
}

// ─── TaskPlacement ────────────────────────────────────────────────────────────

/// Where a task runs.  Independent of how it's sandboxed — combine with
/// [`TaskRuntime`] inside [`TaskPlacement`].
///
/// Integration is *not* a location — it's a species and lives on `ForgeMeta`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskLocation {
    /// Run on the dev box that submitted the task.
    Local,

    /// Run on the named warden node.
    Remote { node: MeshIdent },

    /// Run on any warden node in the requested tier; warden picks based on
    /// capacity admission control (R090-F3).
    RemoteAny { tier: TierTag },
}

/// How a task is sandboxed.  Independent of where it runs — combine with
/// [`TaskLocation`] inside [`TaskPlacement`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRuntime {
    /// Subprocess on the host (no container).
    Native,
    /// Image-backed container (docker/podman locally; containerd on warden).
    Container,
}

/// Orthogonal placement of a task: *where* it runs (`location`) × *how* it's
/// sandboxed (`runtime`).
///
/// The four quadrants:
///
/// | location → runtime | `Native` | `Container` |
/// |---|---|---|
/// | `Local`        | subprocess on dev box       | docker run on dev box |
/// | `Remote(_)`    | warden agent exec on node   | containerd workload on node |
/// | `RemoteAny{_}` | warden agent exec (any node)| containerd workload (any node) |
///
/// See [W149](.yah/docs/working/W149-task-placement-axis.md) for the model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskPlacement {
    pub location: TaskLocation,
    pub runtime: TaskRuntime,
}

impl TaskPlacement {
    pub fn new(location: TaskLocation, runtime: TaskRuntime) -> Self {
        Self { location, runtime }
    }
}

// ─── MeshAccess ───────────────────────────────────────────────────────────────

/// How a remote or integration forge run may reach mirror services over the
/// cluster mesh.  `None` is the default — most forge runs don't need cluster
/// access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MeshAccess {
    /// No cluster mesh access.  The forge run is network-isolated.
    None,

    /// Read-only mesh access.  The run can reach services matching
    /// `tier_filter`; it cannot write or bind ports visible to other workloads.
    ReadOnly,

    /// Read-write mesh access filtered to the listed tiers.  Allows a forge
    /// run to reach e.g. `database.tenant` without becoming a backdoor into
    /// every tier.
    ReadWrite { tier_filter: Vec<TierTag> },
}

impl Default for MeshAccess {
    fn default() -> Self {
        MeshAccess::None
    }
}

// ─── ForgeCommand ─────────────────────────────────────────────────────────────

/// What the forge run executes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ForgeCommand {
    /// Run `argv` inside a container.  `image` defaults to the yah-provided
    /// minimal image (R094-F8) when `None`.
    Subprocess {
        argv: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        image: Option<ImageRef>,
    },

    /// Deploy the spec verbatim via warden RPC.  Warden sets
    /// `restart_policy=Never` and the forge mesh-ident convention if not
    /// already present.
    Workload { spec: WorkloadSpec },

    /// Build a container image from a Dockerfile + build context, producing an
    /// `ImageRef`.  Local builds shell to `docker buildx` (R381-T4); remote
    /// builds submit a BuildKit-in-containerd workload to warden (R381-T5).
    ///
    /// `dockerfile` and `context` are paths resolved by the caller — the qed
    /// runner uses the catalog (R381-T1) to translate a catalog name into
    /// these paths before constructing the spec.
    BuildImage {
        dockerfile: PathBuf,
        context: PathBuf,
        tag: String,
        #[serde(default)]
        push: bool,
    },
}

// ─── ForgeSpec ────────────────────────────────────────────────────────────────

/// Input description of a forge run.  Handed to the appropriate species driver
/// which synthesises the underlying execution primitive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForgeSpec {
    /// What to run.
    pub command: ForgeCommand,

    /// Where to run it (location × runtime).  Integration runs are described
    /// by `IntegrationForgeSpec` instead; this field is placement-only.
    pub where_: TaskPlacement,

    /// Wall-clock timeout.  `None` means no limit (use with caution).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<Millis>,

    /// Human-readable tag surfaced in `forge.list` and desktop tiles.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,

    /// Who or what initiated this run.
    pub initiator: Initiator,

    /// How the run may reach mirror services over the cluster mesh.
    #[serde(default)]
    pub mesh_access: MeshAccess,
}

// ─── IntegrationForgeSpec ─────────────────────────────────────────────────────

/// Input description of an integration-forge run: N workloads stood up for a
/// single test or operator-driven flow, torn down on completion.
///
/// The R091 `#[test_with_provider]` macro constructs this and calls
/// `forge.run`; most callers never build it directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationForgeSpec {
    /// All workloads deployed under `restart_policy=Never`.
    pub workloads: Vec<WorkloadSpec>,

    /// Topology hints for the stand-up (node count, mesh shape, etc.).
    /// Stub until R094-F4; defaults to a single-node local stand-up.
    #[serde(default)]
    pub topology: TopologyHints,

    /// Seed data, image preloads, etc.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fixtures: Vec<FixtureRef>,

    /// Wall-clock timeout for the entire stand-up.
    pub timeout: Millis,

    /// When to reap the stand-up.
    pub teardown: TeardownPolicy,

    /// Human-readable tag.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// Topology placement hints for an integration-forge stand-up.
///
/// Stub shape — R094-F4 fills in network degradation knobs, multi-node mesh
/// options, etc.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopologyHints {
    /// Minimum node count.  `1` targets a single local node; higher values
    /// require a live warden cluster.
    #[serde(default = "default_node_count")]
    pub node_count: u32,
}

fn default_node_count() -> u32 {
    1
}

/// Reference to a fixture loaded before integration-forge workloads start.
///
/// Stub shape — R094-F4 defines seed-data and image-preload variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureRef {
    pub name: String,
}

/// When to reap an integration-forge stand-up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeardownPolicy {
    /// Always reap, even on test failure.
    Always,
    /// Reap only when the run succeeds; keep alive on failure for post-mortem.
    OnSuccess,
    /// Require an explicit `forge.teardown` call.
    Manual,
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod types {
    use super::*;
    use observation::ForgeId;
    use workload_spec::{ImageRef, Millis, TierTag};

    fn sample_forge_spec() -> ForgeSpec {
        ForgeSpec {
            command: ForgeCommand::Subprocess {
                argv: vec!["cargo".into(), "check".into()],
                image: None,
            },
            where_: TaskPlacement::new(
                TaskLocation::RemoteAny { tier: TierTag("infra".into()) },
                TaskRuntime::Container,
            ),
            timeout: Some(Millis::from_secs(300)),
            label: Some("ci-check".into()),
            initiator: task_runs::Initiator::Human { camp: "my-camp".into() },
            mesh_access: MeshAccess::None,
        }
    }

    #[test]
    fn forge_id_round_trip() {
        let id = ForgeId::new();
        let json = serde_json::to_string(&id).unwrap();
        let back: ForgeId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn mesh_access_variants_round_trip() {
        for access in [
            MeshAccess::None,
            MeshAccess::ReadOnly,
            MeshAccess::ReadWrite { tier_filter: vec![TierTag("tenant".into())] },
        ] {
            let json = serde_json::to_string(&access).unwrap();
            let back: MeshAccess = serde_json::from_str(&json).unwrap();
            assert_eq!(access, back);
        }
    }

    #[test]
    fn forge_status_variants_round_trip() {
        let statuses = vec![
            ForgeStatus::Pending,
            ForgeStatus::Running,
            ForgeStatus::Done { exit_code: 0, ended_at: 1000 },
            ForgeStatus::Killed { signal: 9, ended_at: 2000 },
            ForgeStatus::TimedOut { ended_at: 3000 },
            ForgeStatus::Lost { reason: "connection reset".into() },
        ];
        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let back: ForgeStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, back);
        }
    }

    #[test]
    fn forge_status_terminal_predicate() {
        assert!(!ForgeStatus::Pending.is_terminal());
        assert!(!ForgeStatus::Running.is_terminal());
        assert!(ForgeStatus::Done { exit_code: 0, ended_at: 0 }.is_terminal());
        assert!(ForgeStatus::Killed { signal: 9, ended_at: 0 }.is_terminal());
        assert!(ForgeStatus::TimedOut { ended_at: 0 }.is_terminal());
        assert!(ForgeStatus::Lost { reason: String::new() }.is_terminal());
    }

    #[test]
    fn forge_command_build_image_round_trip() {
        let cmd = ForgeCommand::BuildImage {
            dockerfile: PathBuf::from("crates/yah/qed/images/yah-rust/Dockerfile"),
            context: PathBuf::from("."),
            tag: "ghcr.io/yah-ai/yah-rust:dev".into(),
            push: true,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let back: ForgeCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(cmd, back);
        // Confirm the snake_case tag is what warden reads on the wire.
        assert!(json.contains(r#""kind":"build_image""#));
    }

    #[test]
    fn forge_command_subprocess_round_trip() {
        let cmd = ForgeCommand::Subprocess {
            argv: vec!["bash".into(), "-c".into(), "echo hi".into()],
            image: Some(ImageRef {
                registry: "ghcr.io".into(),
                repository: "yah/forge-minimal".into(),
                tag: "latest".into(),
                digest: workload_spec::testing::test_digest(),
            }),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let back: ForgeCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(cmd, back);
    }

    #[test]
    fn forge_spec_round_trip() {
        let spec = sample_forge_spec();
        let json = serde_json::to_string(&spec).unwrap();
        let back: ForgeSpec = serde_json::from_str(&json).unwrap();
        // Spot-check key fields
        assert_eq!(back.label, spec.label);
        assert_eq!(back.timeout, spec.timeout);
        assert_eq!(back.mesh_access, spec.mesh_access);
    }

    #[test]
    fn teardown_policy_round_trip() {
        for policy in [TeardownPolicy::Always, TeardownPolicy::OnSuccess, TeardownPolicy::Manual] {
            let json = serde_json::to_string(&policy).unwrap();
            let back: TeardownPolicy = serde_json::from_str(&json).unwrap();
            assert_eq!(policy, back);
        }
    }

    #[test]
    fn forge_id_from_task_run_id_identity() {
        use observation::TaskRunId;
        let id = ForgeId::new();
        let task_id: TaskRunId = id.clone().into();
        let back: ForgeId = task_id.into();
        assert_eq!(id, back);
    }

    #[test]
    fn event_scope_forge_round_trip() {
        use observation::EventScope;
        let scope = EventScope::Forge(ForgeId::new());
        let json = serde_json::to_string(&scope).unwrap();
        let back: EventScope = serde_json::from_str(&json).unwrap();
        assert_eq!(scope, back);
    }

    // ─── TaskPlacement serde ──────────────────────────────────────────────────

    fn ident(s: &str) -> MeshIdent {
        MeshIdent(s.to_string())
    }

    #[test]
    fn task_placement_round_trip_serde() {
        let placement = TaskPlacement::new(
            TaskLocation::Remote { node: ident("warden-01") },
            TaskRuntime::Container,
        );
        let json = serde_json::to_string(&placement).unwrap();
        let back: TaskPlacement = serde_json::from_str(&json).unwrap();
        assert_eq!(placement, back);
    }

    #[test]
    fn forge_species_variants_round_trip() {
        for species in [ForgeSpecies::Local, ForgeSpecies::Remote, ForgeSpecies::Integration] {
            let json = serde_json::to_string(&species).unwrap();
            let back: ForgeSpecies = serde_json::from_str(&json).unwrap();
            assert_eq!(species, back);
        }
    }
}
