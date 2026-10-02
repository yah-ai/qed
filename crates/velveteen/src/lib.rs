//! @arch:layer(kg_store)
//! @arch:role(runtime)
//! @arch:see(.yah/docs/architecture/A035-yah-forge.md)
//!
//! `velveteen` — the task vocabulary: how work is *described*.
//!
//! A task is a description of work, not a process supervisor. This crate holds
//! the nouns — [`ForgeSpec`], [`ForgeCommand`], [`TaskPlacement`]
//! ([`TaskLocation`] × [`TaskRuntime`]), [`ForgeStatus`], [`ForgeSpecies`],
//! [`MeshAccess`], [`IntegrationForgeSpec`] — and nothing that runs them. Its
//! dependency set is deliberately serde + `workload-spec` + `observation`, so a
//! queue, scheduler, dashboard, or wire client can speak the vocabulary without
//! dragging in tokio/process, the docker shim, `task-runs`' SQLite store, or
//! `yah-scryer` (R619).
//!
//! The drivers that execute these specs live in the `velveteen-exec` sibling
//! crate, one module per species:
//!
//! - **local-forge**: subprocess on the dev box; backed by `task-runs`.
//! - **remote-forge**: one-shot workload on a yubaba machine (R094-F3).
//! - **integration-forge**: N-workload stand-up scoped to a test or flow (R094-F4).
//!
//! [`ForgeId`] and [`Initiator`] live in `observation` so scryer can reference
//! them in `EventScope::Forge` without depending on this crate; both are
//! re-exported here.
//!
//! @arch:see(.yah/docs/working/W154-yubaba-dual-runtime.md)
//! @arch:see(.yah/docs/working/W164-derived-static-assets.md)
//! @arch:see(.yah/docs/working/W165-mesofact-build-mode-lowering.md)
//! @arch:see(.yah/docs/working/W276-susuwatari-task-queue.md)

pub use observation::{ForgeId, Initiator};

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use workload_spec::{ImageRef, Millis, MeshIdent, TierTag, WorkloadSpec};

// ─── ForgeStatus ──────────────────────────────────────────────────────────────

/// Terminal + in-flight status for a forge run.
///
/// Extends [`observation::RunStatus`] with `TimedOut` — a forge run that
/// exceeds its `timeout` field produces this status rather than `Lost`.
/// (`task_runs::RunStatus` is a re-export of the same type.)
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

impl From<observation::RunStatus> for ForgeStatus {
    fn from(s: observation::RunStatus) -> Self {
        match s {
            observation::RunStatus::Pending => ForgeStatus::Pending,
            observation::RunStatus::Running => ForgeStatus::Running,
            observation::RunStatus::Done { exit_code, ended_at } => {
                ForgeStatus::Done { exit_code, ended_at }
            }
            observation::RunStatus::Killed { signal, ended_at } => {
                ForgeStatus::Killed { signal, ended_at }
            }
            observation::RunStatus::Lost { reason } => ForgeStatus::Lost { reason },
        }
    }
}

// ─── ForgeSpecies ─────────────────────────────────────────────────────────────

/// Which forge species a run belongs to.
///
/// Sibling to [`TaskPlacement`] on `ForgeMeta` (in `velveteen-exec`): the placement says *where*
/// and *how* the run executes; the species says *which driver* produced it.
/// The two are orthogonal — a `Remote` species always runs as a containerd
/// workload on yubaba, but a `Local` species can run native or container per
/// `TaskPlacement.runtime`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForgeSpecies {
    /// Subprocess on the dev box, driven by `task-runs`.
    Local,
    /// One-shot workload on a yubaba node, driven by
    /// `velveteen_exec::RemoteForgeDriver`.
    Remote,
    /// N-workload stand-up driven by `velveteen_exec::IntegrationForgeDriver`.
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

    /// Run on the named yubaba node.
    Remote { node: MeshIdent },

    /// Run on any yubaba node in the requested tier; yubaba picks based on
    /// capacity admission control (R090-F3).
    ///
    /// `mesh_tags` narrows the candidate set to nodes whose mesh tags are a
    /// **superset** of the requested ones (R594) — e.g.
    /// `["tag:build-worker", "arch:x86"]` routes an amd64 image build to the
    /// x86 build-worker fleet. Empty (the default) means "any node in `tier`",
    /// preserving the pre-R594 behavior. Arch is expressed as a mesh tag
    /// (`arch:x86` / `arch:arm`), matching how the fleet nodes are tagged in
    /// `.yah/infra/machines/*.toml`.
    RemoteAny {
        tier: TierTag,
        #[serde(default)]
        mesh_tags: Vec<String>,
    },
}

/// How a task is sandboxed.  Independent of where it runs — combine with
/// [`TaskLocation`] inside [`TaskPlacement`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRuntime {
    /// Subprocess on the host (no container).
    Native,
    /// Image-backed container (docker/podman locally; containerd on yubaba).
    Container,
    /// KVM microVM — the task boots its own kernel in its own guest
    /// (R605-F8 / W325 §5).
    ///
    /// Remote-only, and refused locally: there is no dev-box story here the way
    /// there is for `Container` (docker on the laptop). This exists so a build
    /// can be placed on a node that is *also* running something that matters —
    /// a raft voter, the public site — without the build sharing that node's
    /// kernel. On a dev box there is nothing to be isolated from.
    ///
    /// Like `Native`, it is carried to kamaji as an annotation on an ordinary
    /// `Workload::Container` rather than as a new wire shape; see
    /// [`workload_spec::WorkloadSpec::wants_microvm`].
    ///
    /// # Why this one variant overrides `rename_all`
    ///
    /// R605-T24. `rename_all = "snake_case"` spells this `micro_vm`, and not one
    /// thing in the tree ever called it that: the qed runner's own refusal says
    /// "step `x` declares runtime = microvm, which is remote-only"
    /// (`qed::runner::local_microvm_is_refused`), `velveteen_exec::local` says
    /// "runtime = microvm is remote-only", `admission` renders the substrate as
    /// `"microvm"`, and the annotation kamaji routes on is `yah.exec = microvm`.
    /// So an operator who wrote the value every error message in the system told
    /// them to write got `unknown variant 'microvm', expected one of 'native',
    /// 'container', 'micro_vm'` — from the TOML parser, before any of those
    /// messages could ever be reached. That was the first thing R605-T24 hit
    /// trying to author the first microVM pipeline in the tree, which is to say
    /// the first time anyone tried this token at all.
    ///
    /// Renamed rather than changing the messages, because the messages agree
    /// with the annotation and the annotation is the name of the thing. Free to
    /// do: `micro_vm` appeared in no pipeline, no persisted run record and no
    /// schema — nothing had ever successfully written it.
    #[serde(rename = "microvm")]
    MicroVm,
}

/// Orthogonal placement of a task: *where* it runs (`location`) × *how* it's
/// sandboxed (`runtime`).
///
/// The four quadrants:
///
/// | location → runtime | `Native` | `Container` |
/// |---|---|---|
/// | `Local`        | subprocess on dev box       | docker run on dev box |
/// | `Remote(_)`    | yubaba agent exec on node   | containerd workload on node |
/// | `RemoteAny{_}` | yubaba agent exec (any node)| containerd workload (any node) |
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

    /// Deploy the spec verbatim via yubaba RPC.  Yubaba sets
    /// `restart_policy=Never` and the forge mesh-ident convention if not
    /// already present.
    Workload { spec: WorkloadSpec },

    /// Build a container image from a Dockerfile + build context, producing an
    /// `ImageRef`.  Local builds shell to `docker buildx` (R381-T4); remote
    /// builds submit a BuildKit-in-containerd workload to yubaba (R381-T5).
    ///
    /// `dockerfile` and `context` are paths resolved by the caller — the qed
    /// runner uses the catalog (R381-T1) to translate a catalog name into
    /// these paths before constructing the spec.
    ///
    /// R590-F2 widened this from a single `tag` to the full buildx surface the
    /// arch-matched-fleet path needs: multiple `tags` on one build, explicit
    /// target `platforms`, and `build_args`. `platforms` empty ⇒ build for the
    /// worker's native arch (the per-arch-native case — a multi-arch image is
    /// produced by stitching N native single-platform builds with `buildx
    /// imagetools`, not by one cross-arch build).
    BuildImage {
        dockerfile: PathBuf,
        context: PathBuf,
        /// When set, the build context is loaded by BuildKit from this URL — a
        /// (optionally gzipped) tar whose root holds the context *and* the
        /// Dockerfile named by `dockerfile`'s basename — instead of from the
        /// local `context` / `dockerfile` paths (R636-B1).
        ///
        /// The local paths are host paths on whoever *composed* the spec. That
        /// is fine when the builder shares a filesystem with the composer, and
        /// wrong the moment it doesn't: a remote build bind-mounted the qed
        /// host's camp root onto a worker that has no such directory, and runc
        /// failed with `open /Users/…: no such file or directory`. A URL is the
        /// one context reference that means the same thing on both machines.
        ///
        /// `None` keeps the bind-mount shape, which remains correct for the
        /// same-host case (local docker, or a yubaba on the qed host).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_url: Option<String>,
        /// One or more image tags to apply. All tags reference the same built
        /// image; a single build emits every tag. Non-empty by convention —
        /// the qed runner always supplies at least one.
        tags: Vec<String>,
        /// Target platforms in buildkit form (`linux/amd64`, `linux/arm64`).
        /// Empty ⇒ the worker's native platform. A non-native single entry
        /// requires the worker to have QEMU/binfmt; the fleet path prefers
        /// native-per-arch placement and leaves this empty.
        #[serde(default)]
        platforms: Vec<String>,
        /// `--build-arg KEY=VALUE` pairs, order-preserving. Kept as a `Vec` of
        /// pairs (not a map) so the postcard wire encoding to kamaji stays
        /// positional and deterministic (see R590-B3).
        #[serde(default)]
        build_args: Vec<(String, String)>,
        #[serde(default)]
        push: bool,
        /// Load the finished image into the worker's local docker/containerd
        /// image store (`--load`). Mutually exclusive with multi-platform and
        /// with `push`.
        #[serde(default)]
        load: bool,
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

    /// R876-F4 — key of the host-persistent build cache this run may reuse, or
    /// `None` for the default cold-every-time behaviour.
    ///
    /// The driver lowers it to a bind of `workload_spec::forge_cache::HOST_ROOT
    /// /<key>` onto `forge_cache::CONTAINER_DIR`, so a step's build scratch
    /// outlives the container kamaji reaps. Only a
    /// [`ForgeCommand::Subprocess`] accepts one: a `Workload` forge carries its
    /// own volumes and a `BuildImage` forge's caching belongs to BuildKit.
    ///
    /// The key is DERIVED by the dispatcher (`forge_cache::key_from_parts`),
    /// never written by hand — two runs sharing a cargo target dir that should
    /// not is a correctness bug, and a derived key makes the collision
    /// impossible rather than merely unlikely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<String>,
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
    /// require a live yubaba cluster.
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
                TaskLocation::RemoteAny { tier: TierTag("infra".into()), mesh_tags: vec![] },
                TaskRuntime::Container,
            ),
            timeout: Some(Millis::from_secs(300)),
            label: Some("ci-check".into()),
            initiator: Initiator::Human { camp: "my-camp".into() },
            mesh_access: MeshAccess::None,
            cache_key: None,
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
            context_url: None,
            tags: vec![
                "ghcr.io/yah-ai/yah-rust:dev".into(),
                "ghcr.io/yah-ai/yah-rust:latest".into(),
            ],
            platforms: vec!["linux/amd64".into()],
            build_args: vec![("RUST_VERSION".into(), "1.85".into())],
            push: true,
            load: false,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let back: ForgeCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(cmd, back);
        // Confirm the snake_case tag is what yubaba reads on the wire.
        assert!(json.contains(r#""kind":"build_image""#));
        // `None` must not appear on the wire at all: a kamaji that predates
        // this field has to keep deserializing specs from a newer qed.
        assert!(!json.contains("context_url"));
    }

    /// A remote-context spec round-trips and carries the URL on the wire.
    #[test]
    fn forge_command_build_image_carries_context_url() {
        let cmd = ForgeCommand::BuildImage {
            dockerfile: PathBuf::from("/camp/.yah/cache/buildkit/yah-rust.Dockerfile"),
            context: PathBuf::from("/camp/oss/qed/crates/qed/images/yah-rust"),
            context_url: Some("https://cdn.yah.dev/yah-cloud/qed-context/abc.tar.gz".into()),
            tags: vec!["cr.yah.dev/yah-rust:dev".into()],
            platforms: vec![],
            build_args: vec![],
            push: false,
            load: false,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("qed-context/abc.tar.gz"));
        let back: ForgeCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(cmd, back);
    }

    /// A spec written before `context_url` existed still deserializes — the
    /// field defaults to `None` (bind-mount shape), not a parse error.
    #[test]
    fn forge_command_build_image_accepts_legacy_json_without_context_url() {
        let legacy = r#"{"kind":"build_image","dockerfile":"D","context":".","tags":["t"]}"#;
        let back: ForgeCommand = serde_json::from_str(legacy).unwrap();
        match back {
            ForgeCommand::BuildImage { context_url, .. } => assert!(context_url.is_none()),
            other => panic!("expected BuildImage, got {other:?}"),
        }
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
            TaskLocation::Remote { node: ident("yubaba-01") },
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
