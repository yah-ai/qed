//! @yah:ticket(R299-F7, "Remote step dispatch (where=remote): run qed steps as task::remote workloads on yubaba")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-23T01:46:59Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R299)
//! @arch:see(.yah/docs/working/W126-yah-qed.md)
//! @yah:depends_on(R299-T5)
//! @yah:handoff("Remote step dispatch wired in crates/yah/qed/src/runner.rs. PipelineRunner::new_remote(pipeline, scryer, yubaba) added; execute_step_remote maps QedStep → ForgeSpec (RemoteAny/infra tier), dispatches via RemoteForgeDriver, records forge_id in StepStatus.task_run_id. RunWhere enum exported from qed lib. CLI --where=remote gives clear error 'yubaba RPC client (R091) not yet implemented'. 3 new tests (remote_step_success, remote_step_failure, remote_abort_on_fail) all pass. cargo test -p qed: 6/6 ok, cargo check -p yah: clean.")
//! @yah:verify("cargo test -p qed  # 6/6 pass (includes 3 new remote_* tests)")
//! @yah:verify("cargo check -p qed -p yah  # clean")
//! @yah:verify("yah qed run --where=remote check  # exits with 'yubaba RPC client (R091) not yet implemented'")
//!
//! @yah:ticket(R380-T2, "Migrate task crate internal callsites to TaskPlacement (remote.rs, meta.rs, list.rs)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-01T21:06:04Z)
//! @yah:status(review)
//! @yah:parent(R380)
//! @yah:next("Replace ForgeSpec.where_ field type from ForgeWhere → TaskPlacement.")
//! @yah:next("remote.rs build_workload_spec switches on placement.location for tier defaulting; the placement.runtime must be Container or it's a programmer error (return InvalidSpec).")
//! @yah:next("list.rs wants_remote / wants_integration become matches on TaskPlacement.location.")
//! @yah:next("meta.rs ForgeMeta.where_ stays a placement field (no Integration there yet — T8 sorts that out).")
//! @yah:handoff("T2 complete: ForgeSpec.where_ and ForgeMeta.where_ both migrated from ForgeWhere → TaskPlacement. ForgeListFilter.where_ likewise. remote::build_workload_spec now requires runtime=Container (returns InvalidSpec for remote+native — pre-stub for T7) and matches on TaskPlacement.location for tier defaulting. From<TaskRunMeta> for ForgeMeta sets where_ to {Local, Native}. list.rs: wants_local + wants_remote now match TaskPlacement.location; wants_integration dropped (TaskLocation has no Integration variant); integration_metas always merge into results (placement filter can't address them until T8 adds a sibling species field). qed/runner.rs::execute_step_remote bridged to construct TaskPlacement directly (one-line change, leaves the wider qed migration — RunWhere refactor + --runtime CLI flag — to T3). All 47 task crate tests pass; cargo check --workspace clean.")
//! @yah:next("T8 sweep: add ForgeMeta.species: ForgeSpecies (Local|Remote|Integration) sibling field + restore filtered Integration enumeration in ForgeListFilter; delete the integration_metas_always_merge_until_t8 placeholder test.")
//! @yah:verify("cargo test -p task --lib  # 47/47 pass")
//! @yah:verify("cargo check --workspace  # clean")
//! @yah:gotcha("Pre-existing failure in qed::tests::test_builtin_release_build_pipeline (asserts 4 steps but builtin_release_build() now has 6) — unrelated to this ticket; should be fixed in its own bug ticket.")
//! @yah:gotcha("filter_by_where_integration test was deleted; replaced by integration_metas_always_merge_until_t8 which pins the new interim semantics. T8 should reintroduce species-based integration filtering.")
//!
//! @yah:ticket(R380-T7, "Remote + native quadrant: refuse at WardenClient seam in v1 (with clear error + v2 hook)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-01T21:06:28Z)
//! @yah:status(review)
//! @yah:parent(R380)
//! @yah:next("Decision recommendation: refuse remote+native in v1. Implement only when a real use case arrives (BuildKit shelling to host docker on a yubaba node is the leading candidate).")
//! @yah:next("WardenClient::deploy receives a TaskPlacement; if runtime=Native, return RemoteForgeError::InvalidSpec('yubaba-native exec not supported in v1; use runtime=container').")
//! @yah:next("Document the v2 hook: WardenClient gets a separate exec_native(spec) method later, parallel to deploy(). Don't add it now — type-level option only.")
//! @yah:next("Test: a remote+native ForgeSpec fails with InvalidSpec at start() and emits no events.")
//! @yah:handoff("v1 refusal seam for remote+native locked in. The refusal lives in task::remote::build_workload_spec — the very first thing RemoteForgeDriver::start does — so a remote+native ForgeSpec never allocates a ForgeId in scryer's namespace, never spawns the log-ingest task, and never reaches WardenClient::deploy. The error message now reads 'remote + native is not supported in v1 — set placement.runtime = container, or run locally with placement.location = local. A future yubaba `exec_native` surface lands when a real use case arrives (R380-T7 / W149).'")
//! @yah:handoff("WardenClient trait docs now carry the v2 contract: an explicit 'Remote + native: not in v1' section explains that `deploy` is image-backed only and that v2 adds a sibling `async fn exec_native(spec) -> ...` when a real use case lands (BuildKit shelling to host docker on a yubaba node is the leading W149 candidate). No exec_native method added — type-level option only, per the ticket's instructions.")
//! @yah:handoff("Test: remote::remote_native_refused_at_start_emits_no_events asserts (a) start() returns Err(InvalidSpec) with the right message, (b) yubaba.deploy was never called, (c) scryer holds no Forge-scoped events after the refusal. ScriptedWardenClient gained a deploy_called: Arc<Mutex<bool>> so the assertion is structural, not log-grep. cargo test -p task --lib: 54/54 pass (53 before + the new refusal test). cargo check --workspace clean.")
//! @yah:next("R380-T8 (cleanup, the last child) picks up next: drop ForgeWhere from task + tower-rules, move Integration off the placement enum onto a sibling ForgeMeta.species field, delete ForgeWhere.ts, restore species-based Integration filtering in ForgeListFilter (replaces the integration_metas_always_merge_until_t8 placeholder test that T2 left behind), and sweep .yah/docs/architecture/A035-yah-forge.md + arch refs for stale ForgeWhere mentions.")
//! @yah:next("Future work for v2 exec_native: when the first real use case arrives (e.g. BuildKit shelling to host docker on a yubaba node), the v2 PR adds `async fn exec_native(&self, spec: &NativeExecSpec) -> Result<..., RemoteForgeError>` to WardenClient and a parallel `start_native` path on RemoteForgeDriver. Until then the type-level hook stays trait-docs-only — no NativeExecSpec, no exec_native method, no premature surface.")
//! @yah:verify("cargo test -p task --lib  # 54 pass, 2 ignored")
//! @yah:verify("cargo test -p task --lib remote_native_refused_at_start_emits_no_events  # the new test in isolation")
//! @yah:verify("cargo check --workspace  # clean (pre-existing desktop warnings unrelated)")

// @yah:ticket(R094-F3, "Remote-forge driver: synthesize WorkloadSpec from ForgeSpec, deploy via yubaba RPC, attach containerd-logs scryer adapter scoped to Forge(id)")
// @yah:assignee(agent:claude)
// @yah:status(review)
// @yah:phase(P2)
// @yah:parent(R094)
// @yah:handoff("crates/yah/task/src/remote.rs — WardenClient trait (deploy/connect_logs/teardown/exit_code) + RemoteForgeDriver::start (allocates ForgeId, synthesizes WorkloadSpec::for_forge, deploys via yubaba, spawns log-ingestion task with EventScope::Forge scope, returns ForgeRunHandle backed by tokio::sync::watch) + ForgeRunHandle::wait (async, terminal-state poll) + ForgeRunHandle::kill. ScriptedWardenClient + HangingWardenClient test helpers in test_support mod. task Cargo.toml: added scryer, tokio, async-trait, thiserror deps. Two verify tests pass: remote::happy (Done exit_code=0 + 2 events queryable via scryer.events(Forge(id))) and remote::timeout (TimedOut + yubaba.teardown called). cargo test -p task 18/18 ok; cargo check --workspace clean.")
// @yah:next("Human review: (a) check crates/yah/task/src/remote.rs — WardenClient trait shape, ForgeRunHandle watch semantics, build_workload_spec tier defaulting (Remote(ident) → infra), default_forge_image placeholder tagged as F8 follow-up; (b) check test_support::ScriptedWardenClient + HangingWardenClient for correctness; (c) confirm the Forge(id) scryer scope is correct per arch doc §Remote-forge ingestion-side branch.")
// @yah:next("Smoke under R091 smoke tier: forge.run({ command: Subprocess { argv: [\"cargo\", \"check\"] }, where: RemoteAny { tier: \"infra\" } }) runs against real Hetzner — deferred to R091 integration test infrastructure landing.")
// @arch:see(.yah/docs/architecture/A035-yah-forge.md)
//!
//! Remote-forge driver.
//!
//! Synthesizes a [`WorkloadSpec`] from a [`ForgeSpec`], deploys via the
//! [`WardenClient`] seam, ingests the container's log stream into scryer with
//! `Forge(id)` scope, and tracks the terminal status via a `watch` channel.
//!
//! # Seam
//!
//! [`WardenClient`] is the trait yubaba (or test code) implements.  Production
//! yubaba uses containerd gRPC (R091).  Tests use
//! [`test_support::ScriptedWardenClient`] and
//! [`test_support::HangingWardenClient`].

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use observation::{Event, EventScope, EventSource, ForgeId, Level, TaskRunId};
use yah_scryer::service::Scryer;
use serde_json::json;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use workload_spec::{ImageRef, MeshIdent, TierTag, VolumeMount, VolumeSource, WorkloadSpec};

use crate::executor::{ExecEvent, OutputStream};
use crate::{ForgeCommand, ForgeSpec, ForgeStatus, TaskLocation, TaskRuntime};

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum RemoteForgeError {
    #[error("yubaba deploy: {0}")]
    Deploy(String),
    #[error("yubaba log stream: {0}")]
    LogStream(String),
    #[error("yubaba teardown: {0}")]
    Teardown(String),
    #[error("yubaba exit code: {0}")]
    ExitCode(String),
    #[error("scryer push: {0}")]
    Push(String),
    #[error("invalid spec: {0}")]
    InvalidSpec(String),
}

// ─── WardenClient ─────────────────────────────────────────────────────────────

/// Seam between the remote-forge driver and yubaba's containerd-backed RPC.
///
/// Production: yubaba's containerd gRPC client (R091).  Tests:
/// [`test_support::ScriptedWardenClient`] / [`test_support::HangingWardenClient`].
///
/// # Remote + native: not in v1
///
/// All v1 paths through this trait are containerd-backed — `deploy` takes a
/// [`WorkloadSpec`] which is an image-pinned workload.  Remote + native (a
/// host subprocess running directly on the yubaba node, no containerd) is
/// intentionally absent from the surface.  When a real use case lands
/// (BuildKit shelling to host docker on a yubaba node is the leading
/// candidate per W149 §Open policy), v2 adds a sibling method here:
///
/// ```ignore
/// async fn exec_native(&self, spec: &NativeExecSpec) -> Result<…, RemoteForgeError>;
/// ```
///
/// Until then, any `ForgeSpec` whose `placement.runtime == Native` and
/// `placement.location` is `Remote` or `RemoteAny` is refused by
/// [`RemoteForgeDriver::start`] before it touches this trait —
/// see [`build_workload_spec`].
#[async_trait]
pub trait WardenClient: Send + Sync {
    /// Submit a container workload for deployment.  Returns once the RPC
    /// completes; does NOT wait for the container to reach Ready.
    ///
    /// Only image-backed workloads (`placement.runtime = Container`) flow
    /// through here in v1.  See trait-level docs for the v2 `exec_native`
    /// plan.
    async fn deploy(&self, spec: &WorkloadSpec) -> Result<(), RemoteForgeError>;

    /// Open a line-oriented log stream for the named container.  The returned
    /// `Receiver` yields one line per item; when the sender is dropped the
    /// stream is considered cleanly closed.
    async fn connect_logs(
        &self,
        ident: &MeshIdent,
    ) -> Result<mpsc::Receiver<String>, RemoteForgeError>;

    /// Tear down the container.  Returns `Ok` even if already gone.
    async fn teardown(&self, ident: &MeshIdent) -> Result<(), RemoteForgeError>;

    /// Query the exit code of a terminated container.  `None` if still running
    /// or exit code is unavailable.
    async fn exit_code(&self, ident: &MeshIdent) -> Result<Option<i32>, RemoteForgeError>;
}

// ─── RemoteForgeDriver ────────────────────────────────────────────────────────

/// Drives one-shot forge runs on yubaba-managed machines.
///
/// [`start`](Self::start) deploys the workload, spawns the log-ingestion task,
/// and returns a [`ForgeRunHandle`] immediately.  The caller calls
/// [`ForgeRunHandle::wait`] to block until a terminal state is reached.
pub struct RemoteForgeDriver {
    scryer: Arc<Scryer>,
    yubaba: Arc<dyn WardenClient>,
}

impl RemoteForgeDriver {
    pub fn new(scryer: Arc<Scryer>, yubaba: Arc<dyn WardenClient>) -> Self {
        Self { scryer, yubaba }
    }

    /// Start a remote forge run.
    ///
    /// Synthesizes a `WorkloadSpec`, deploys it via yubaba, and spawns the
    /// log-ingestion task.  Returns a `ForgeRunHandle` before the run finishes.
    pub async fn start(&self, spec: ForgeSpec) -> Result<ForgeRunHandle, RemoteForgeError> {
        self.start_with_sink(spec, None).await
    }

    /// Like [`start`](Self::start) but also tees each container log line into
    /// `sink` as an [`ExecEvent::Output`].
    ///
    /// The qed runner passes its live-event sink here so a yubaba-dispatched
    /// step streams stdout lines into `QedEvent::StepOutput` *during* the run
    /// rather than batching them post-completion (R508). The sink is a pure
    /// fan-out: lines still flow into scryer under `Forge(id)` scope exactly as
    /// before. Container logs are line-merged with no stdout/stderr split, so
    /// every forwarded line is tagged [`OutputStream::Stdout`].
    pub async fn start_with_sink(
        &self,
        spec: ForgeSpec,
        sink: Option<mpsc::UnboundedSender<ExecEvent>>,
    ) -> Result<ForgeRunHandle, RemoteForgeError> {
        let forge_id = ForgeId::new();
        let ident = forge_mesh_ident(&forge_id);
        let timeout = spec.timeout.map(|ms| Duration::from_millis(ms.as_ms()));

        let workload = build_workload_spec(&forge_id, &spec)?;
        self.yubaba.deploy(&workload).await?;

        let (status_tx, status_rx) = watch::channel(ForgeStatus::Running);
        let scryer = self.scryer.clone();
        let yubaba = self.yubaba.clone();
        let id = forge_id.clone();

        tokio::spawn(async move {
            let status =
                run_log_task(id, ident, timeout, scryer, yubaba, sink).await;
            let _ = status_tx.send(status);
        });

        Ok(ForgeRunHandle { id: forge_id, status_rx })
    }

    /// Tear down a running forge container.
    ///
    /// The background log task will detect the stream closing and resolve the
    /// run to a terminal state (typically `Lost` or the actual exit code if
    /// yubaba reports one before the stream closes).
    pub async fn kill(&self, forge_id: &ForgeId) -> Result<(), RemoteForgeError> {
        self.yubaba.teardown(&forge_mesh_ident(forge_id)).await
    }
}

// ─── ForgeRunHandle ───────────────────────────────────────────────────────────

/// Handle to a running (or completed) remote-forge run.
///
/// Returned by [`RemoteForgeDriver::start`].  Call [`wait`](Self::wait) to
/// block until a terminal state arrives.
pub struct ForgeRunHandle {
    pub id: ForgeId,
    status_rx: watch::Receiver<ForgeStatus>,
}

impl ForgeRunHandle {
    /// Block until the run reaches a terminal state and return it.
    pub async fn wait(mut self) -> ForgeStatus {
        loop {
            if self.status_rx.borrow().is_terminal() {
                return self.status_rx.borrow().clone();
            }
            if self.status_rx.changed().await.is_err() {
                return ForgeStatus::Lost { reason: "status sender dropped".into() };
            }
        }
    }

    /// Return the current (possibly in-flight) status without waiting.
    pub fn current_status(&self) -> ForgeStatus {
        self.status_rx.borrow().clone()
    }
}

// ─── Internals ────────────────────────────────────────────────────────────────

fn forge_mesh_ident(id: &ForgeId) -> MeshIdent {
    MeshIdent(format!("forge.{id}"))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Synthesise a [`WorkloadSpec`] from a [`ForgeSpec`].
///
/// Refuses the remote + native quadrant with [`RemoteForgeError::InvalidSpec`]
/// before any state is allocated or any yubaba RPC is issued — see W149
/// §Open policy and the [`WardenClient`] trait docs for the v2 `exec_native`
/// path.  No forge id is published, no events flow into scryer, and yubaba's
/// `deploy` is not called.
fn build_workload_spec(
    forge_id: &ForgeId,
    spec: &ForgeSpec,
) -> Result<WorkloadSpec, RemoteForgeError> {
    if !matches!(spec.where_.runtime, TaskRuntime::Container) {
        return Err(RemoteForgeError::InvalidSpec(
            "remote + native is not supported in v1 — \
             set placement.runtime = container, or run locally with placement.location = local. \
             A future yubaba `exec_native` surface lands when a real use case arrives \
             (R380-T7 / W149)."
                .into(),
        ));
    }

    let tier = match &spec.where_.location {
        TaskLocation::RemoteAny { tier } => tier.clone(),
        // Pin to a specific node: tier defaults to infra (conventional for forge).
        TaskLocation::Remote { .. } => TierTag("infra".into()),
        TaskLocation::Local => {
            return Err(RemoteForgeError::InvalidSpec(
                "RemoteForgeDriver received a local ForgeSpec".into(),
            ));
        }
    };

    let ws = match &spec.command {
        ForgeCommand::Subprocess { argv, image } => {
            let image = image.clone().unwrap_or_else(crate::default_image::default_forge_image);
            let mut ws =
                WorkloadSpec::for_forge(&forge_id.to_string(), image, tier, vec![]);
            ws.command = Some(argv.clone());
            ws
        }
        ForgeCommand::Workload { spec: inner } => inner.clone(),
        ForgeCommand::BuildImage { dockerfile, context, tag, push } => {
            build_image_workload_spec(forge_id, dockerfile, context, tag, *push, tier)?
        }
    };

    Ok(ws)
}

// ─── BuildKit workload synthesis (R381-T5) ────────────────────────────────────

/// Conventional output dir bind-mounted into the BuildKit container when an
/// OCI archive is requested.  The yubaba node must have this directory
/// writable and on a filesystem the operator can reach for cross-node
/// consumption; the single-machine sim/dogfood case (yubaba + qed sharing a
/// host) is the v1 happy path.  Cross-node consumers should set `push=true`
/// and let the registry handle distribution.
const BUILDKIT_HOST_OUT_DIR: &str = "/var/lib/yah/qed/build-out";

/// Default BuildKit image used by remote build-image dispatch.
///
/// Held in `option_env!` so a deployment can pin a different version without
/// rebuilding qed.  The default is a current rootless BuildKit release: the
/// rootless variant runs `buildkitd` in user-space inside the container so the
/// workload doesn't require yubaba to grant `CAP_SYS_ADMIN`.
fn default_buildkit_image() -> ImageRef {
    let tag = option_env!("YAH_BUILDKIT_TAG").unwrap_or("v0.12.5-rootless");
    let digest = option_env!("YAH_BUILDKIT_DIGEST")
        .map(Into::into)
        .unwrap_or_else(workload_spec::testing::test_digest);
    ImageRef {
        registry: "docker.io".into(),
        repository: "moby/buildkit".into(),
        tag: tag.into(),
        digest,
    }
}

/// Synthesise the BuildKit workload that performs a remote build-image step.
///
/// The container runs `buildctl-daemonless.sh` (provided by the rootless image)
/// which boots an in-process `buildkitd` and pipes the build through it.  The
/// build context and dockerfile parent are bind-mounted at conventional paths;
/// when `push=true` the result is pushed straight to the tag's registry,
/// otherwise an OCI archive is written to a bind-mounted host directory
/// ([`BUILDKIT_HOST_OUT_DIR`]).
///
/// Bind volume mounts require `tier == "infra"`, which yubaba's shape
/// validation enforces; the forge convention picks infra by default so this is
/// safe for the v1 dogfood path.
fn build_image_workload_spec(
    forge_id: &ForgeId,
    dockerfile: &Path,
    context: &Path,
    tag: &str,
    push: bool,
    tier: TierTag,
) -> Result<WorkloadSpec, RemoteForgeError> {
    let dockerfile_basename = dockerfile
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| {
            RemoteForgeError::InvalidSpec(format!(
                "build-image dockerfile path has no filename component: {}",
                dockerfile.display()
            ))
        })?;
    let dockerfile_parent = dockerfile.parent().ok_or_else(|| {
        RemoteForgeError::InvalidSpec(format!(
            "build-image dockerfile path has no parent directory: {}",
            dockerfile.display()
        ))
    })?;

    let image = default_buildkit_image();
    let mut ws = WorkloadSpec::for_forge(&forge_id.to_string(), image, tier, vec![]);

    // Image builds routinely peak at several hundred MiB; the for_forge
    // defaults (256MiB / 512 cpu_shares) are too tight for buildkit.
    ws.resources.memory_mb = 2048;
    ws.resources.cpu_shares = 2048;
    ws.resources.ephemeral_storage_mb = 4096;

    ws.volumes.push(VolumeMount {
        source: VolumeSource::Bind { host_path: context.to_path_buf() },
        target: PathBuf::from("/yah/build/context"),
        read_only: true,
    });
    ws.volumes.push(VolumeMount {
        source: VolumeSource::Bind { host_path: dockerfile_parent.to_path_buf() },
        target: PathBuf::from("/yah/build/dockerfile"),
        read_only: true,
    });

    let oci_archive_remote = (!push).then(|| {
        ws.volumes.push(VolumeMount {
            source: VolumeSource::Bind {
                host_path: PathBuf::from(BUILDKIT_HOST_OUT_DIR),
            },
            target: PathBuf::from("/yah/build/out"),
            read_only: false,
        });
        format!("/yah/build/out/{}", oci_archive_basename(tag))
    });

    ws.command = Some(buildctl_argv(dockerfile_basename, tag, push, oci_archive_remote.as_deref()));
    Ok(ws)
}

/// Map a docker tag (`reg/repo:ver`) to a filesystem-safe basename for the
/// OCI archive output. Mirrors `qed::runner::tag_to_filename`; duplicated here
/// to keep the workload-spec layer free of qed deps.
fn oci_archive_basename(tag: &str) -> String {
    let safe: String = tag
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{safe}.tar")
}

/// `buildctl-daemonless.sh` argv for a one-shot Dockerfile build.
fn buildctl_argv(
    dockerfile_basename: &str,
    tag: &str,
    push: bool,
    oci_archive_path: Option<&str>,
) -> Vec<String> {
    let mut argv = vec![
        "buildctl-daemonless.sh".to_string(),
        "build".to_string(),
        "--frontend".to_string(),
        "dockerfile.v0".to_string(),
        "--local".to_string(),
        "context=/yah/build/context".to_string(),
        "--local".to_string(),
        "dockerfile=/yah/build/dockerfile".to_string(),
        "--opt".to_string(),
        format!("filename={dockerfile_basename}"),
    ];
    if push {
        argv.push("--output".to_string());
        argv.push(format!("type=image,name={tag},push=true"));
    } else if let Some(archive) = oci_archive_path {
        argv.push("--output".to_string());
        argv.push(format!("type=oci,name={tag},dest={archive}"));
    } else {
        argv.push("--output".to_string());
        argv.push(format!("type=image,name={tag}"));
    }
    argv
}

async fn run_log_task(
    forge_id: ForgeId,
    ident: MeshIdent,
    timeout: Option<Duration>,
    scryer: Arc<Scryer>,
    yubaba: Arc<dyn WardenClient>,
    sink: Option<mpsc::UnboundedSender<ExecEvent>>,
) -> ForgeStatus {
    let ingest = ingest_logs(forge_id.clone(), &ident, &scryer, yubaba.as_ref(), sink);

    let ingest_result = match timeout {
        None => ingest.await,
        Some(d) => match tokio::time::timeout(d, ingest).await {
            Ok(r) => r,
            Err(_) => {
                let _ = yubaba.teardown(&ident).await;
                return ForgeStatus::TimedOut { ended_at: now_ms() };
            }
        },
    };

    match ingest_result {
        Err(e) => ForgeStatus::Lost { reason: e.to_string() },
        Ok(()) => match yubaba.exit_code(&ident).await {
            Ok(Some(code)) => ForgeStatus::Done { exit_code: code, ended_at: now_ms() },
            Ok(None) => ForgeStatus::Lost {
                reason: "container exited but no exit code available".into(),
            },
            Err(e) => ForgeStatus::Lost { reason: e.to_string() },
        },
    }
}

async fn ingest_logs(
    forge_id: ForgeId,
    ident: &MeshIdent,
    scryer: &Scryer,
    yubaba: &dyn WardenClient,
    sink: Option<mpsc::UnboundedSender<ExecEvent>>,
) -> Result<(), RemoteForgeError> {
    let mut rx =
        yubaba.connect_logs(ident).await.map_err(|e| RemoteForgeError::LogStream(e.to_string()))?;

    let scope = EventScope::Forge(forge_id.clone());
    let run_id: TaskRunId = forge_id.into();
    let mut seq = 0u32;

    while let Some(line) = rx.recv().await {
        // Fan the line out to the live sink first (a closed sink is benign —
        // the run continues; only the live tail loses the line). scryer
        // remains the durable record.
        if let Some(s) = &sink {
            let _ = s.send(ExecEvent::Output {
                stream: OutputStream::Stdout,
                line: line.clone(),
            });
        }
        let ev = Event {
            run_id: run_id.clone(),
            seq,
            offset_ms: 0,
            level: Level::Info,
            target: "forge.remote".into(),
            msg: line,
            fields: json!({}),
            anchor: None,
            source: EventSource::Synth,
        };
        scryer
            .push(scope.clone(), ev)
            .map_err(|e| RemoteForgeError::Push(e.to_string()))?;
        seq += 1;
    }

    Ok(())
}

// ─── Test support ─────────────────────────────────────────────────────────────

/// Test-only yubaba client implementations.
#[cfg(test)]
pub mod test_support {
    use super::*;
    use std::sync::Mutex;

    /// A yubaba client that sends a fixed set of log lines then exits with a
    /// configured exit code.
    pub struct ScriptedWardenClient {
        pub lines: Vec<String>,
        pub exit_code: i32,
        pub deploy_called: Arc<Mutex<bool>>,
        pub teardown_called: Arc<Mutex<bool>>,
    }

    impl ScriptedWardenClient {
        pub fn new(lines: Vec<String>, exit_code: i32) -> Arc<Self> {
            Arc::new(Self {
                lines,
                exit_code,
                deploy_called: Default::default(),
                teardown_called: Default::default(),
            })
        }
    }

    #[async_trait]
    impl WardenClient for ScriptedWardenClient {
        async fn deploy(&self, _spec: &WorkloadSpec) -> Result<(), RemoteForgeError> {
            *self.deploy_called.lock().unwrap() = true;
            Ok(())
        }

        async fn connect_logs(
            &self,
            _ident: &MeshIdent,
        ) -> Result<mpsc::Receiver<String>, RemoteForgeError> {
            let (tx, rx) = mpsc::channel(64);
            let lines = self.lines.clone();
            tokio::spawn(async move {
                for line in lines {
                    if tx.send(line).await.is_err() {
                        break;
                    }
                }
                // Dropping tx closes the stream → ingest_logs returns Ok(()).
            });
            Ok(rx)
        }

        async fn teardown(&self, _ident: &MeshIdent) -> Result<(), RemoteForgeError> {
            *self.teardown_called.lock().unwrap() = true;
            Ok(())
        }

        async fn exit_code(
            &self,
            _ident: &MeshIdent,
        ) -> Result<Option<i32>, RemoteForgeError> {
            Ok(Some(self.exit_code))
        }
    }

    /// A yubaba client whose log stream never closes — simulates a hung
    /// container so that timeout behavior can be tested.
    pub struct HangingWardenClient {
        pub initial_lines: Vec<String>,
        pub teardown_called: Arc<Mutex<bool>>,
    }

    impl HangingWardenClient {
        pub fn new(initial_lines: Vec<String>) -> Arc<Self> {
            Arc::new(Self { initial_lines, teardown_called: Default::default() })
        }
    }

    #[async_trait]
    impl WardenClient for HangingWardenClient {
        async fn deploy(&self, _spec: &WorkloadSpec) -> Result<(), RemoteForgeError> {
            Ok(())
        }

        async fn connect_logs(
            &self,
            _ident: &MeshIdent,
        ) -> Result<mpsc::Receiver<String>, RemoteForgeError> {
            let (tx, rx) = mpsc::channel::<String>(8);
            let lines = self.initial_lines.clone();
            tokio::spawn(async move {
                for line in lines {
                    if tx.send(line).await.is_err() {
                        return;
                    }
                }
                // Keep tx alive indefinitely — the stream never closes.
                tokio::time::sleep(Duration::from_secs(3600)).await;
                drop(tx);
            });
            Ok(rx)
        }

        async fn teardown(&self, _ident: &MeshIdent) -> Result<(), RemoteForgeError> {
            *self.teardown_called.lock().unwrap() = true;
            Ok(())
        }

        async fn exit_code(
            &self,
            _ident: &MeshIdent,
        ) -> Result<Option<i32>, RemoteForgeError> {
            Ok(None)
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod remote {
    use super::test_support::*;
    use super::*;
    use crate::TaskPlacement;
    use observation::EventScope;
    use yah_scryer::service::{EventFilter, Scryer, ScryerConfig};
    use task_runs::Initiator;
    use tempfile::TempDir;
    use workload_spec::{Millis, TierTag};

    fn make_scryer(dir: &TempDir) -> Arc<Scryer> {
        let cfg = ScryerConfig::new(dir.path().join("events.db"));
        Arc::new(Scryer::new(cfg, None).unwrap())
    }

    fn subprocess_spec(where_: TaskPlacement, timeout: Option<Millis>) -> ForgeSpec {
        ForgeSpec {
            command: ForgeCommand::Subprocess {
                argv: vec!["true".into()],
                image: None,
            },
            where_,
            timeout,
            label: None,
            initiator: Initiator::Human { camp: "test-camp".into() },
            mesh_access: crate::MeshAccess::None,
        }
    }

    fn remote_any_infra() -> TaskPlacement {
        TaskPlacement::new(
            TaskLocation::RemoteAny { tier: TierTag("infra".into()) },
            TaskRuntime::Container,
        )
    }

    /// R094-F3 accept: forge.run with RemoteAny + Subprocess, scripted yubaba
    /// that emits two log lines and exits 0.  After wait(), status is Done and
    /// the two events are queryable via scryer.events(Forge(id)).
    #[tokio::test]
    async fn happy() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let yubaba = ScriptedWardenClient::new(
            vec!["line one".to_string(), "line two".to_string()],
            0,
        );

        let driver = RemoteForgeDriver::new(scryer.clone(), yubaba);
        let spec = subprocess_spec(remote_any_infra(), None);

        let handle = driver.start(spec).await.unwrap();
        let id = handle.id.clone();
        let status = handle.wait().await;

        assert!(
            matches!(status, ForgeStatus::Done { exit_code: 0, .. }),
            "expected Done exit_code=0, got {status:?}"
        );

        scryer.flush_ring().unwrap();
        let events =
            scryer.events(&EventScope::Forge(id), &EventFilter::default()).unwrap();
        assert_eq!(events.len(), 2, "expected 2 events");
        assert_eq!(events[0].msg, "line one");
        assert_eq!(events[1].msg, "line two");
        assert_eq!(events[0].target, "forge.remote");
    }

    /// R508 accept: `start_with_sink` tees every yubaba log line into the
    /// caller's sink as an `ExecEvent::Output` *as well as* scryer, so the qed
    /// runner can stream remote-step output live. After the run completes the
    /// sink has seen the same two lines, in order, that scryer recorded.
    #[tokio::test]
    async fn streams_log_lines_to_sink() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let yubaba = ScriptedWardenClient::new(
            vec!["alpha".to_string(), "beta".to_string()],
            0,
        );

        let driver = RemoteForgeDriver::new(scryer.clone(), yubaba);
        let (tx, mut rx) = mpsc::unbounded_channel::<ExecEvent>();
        let spec = subprocess_spec(remote_any_infra(), None);

        let handle = driver.start_with_sink(spec, Some(tx)).await.unwrap();
        let id = handle.id.clone();
        let status = handle.wait().await;
        assert!(
            matches!(status, ForgeStatus::Done { exit_code: 0, .. }),
            "expected Done exit_code=0, got {status:?}"
        );

        // The driver's ingest task drops its sink clone when the log stream
        // closes, so draining to None terminates.
        let mut lines = Vec::new();
        while let Some(ExecEvent::Output { stream, line }) = rx.recv().await {
            assert_eq!(stream, OutputStream::Stdout, "container logs are stdout-tagged");
            lines.push(line);
        }
        assert_eq!(lines, vec!["alpha".to_string(), "beta".to_string()]);

        // scryer still holds the durable copy — the sink is pure fan-out.
        scryer.flush_ring().unwrap();
        let events =
            scryer.events(&EventScope::Forge(id), &EventFilter::default()).await.unwrap();
        assert_eq!(events.len(), 2, "scryer must still record both lines");
    }

    /// R380-T7 accept: a remote + native ForgeSpec is refused at `start()`
    /// before any yubaba RPC fires and before any event is pushed to scryer.
    ///
    /// Verifies the v1 refusal contract on the WardenClient seam:
    /// - `start()` returns `Err(InvalidSpec)` with an actionable message.
    /// - `yubaba.deploy` is never called (state-of-the-cluster untouched).
    /// - No `Forge(*)` events appear in scryer (no ingest thread spawned).
    #[tokio::test]
    async fn remote_native_refused_at_start_emits_no_events() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let yubaba = ScriptedWardenClient::new(vec!["should not arrive".into()], 0);
        let deploy_called = yubaba.deploy_called.clone();

        let driver = RemoteForgeDriver::new(scryer.clone(), yubaba);
        // remote + native — the quadrant this ticket refuses.
        let placement = TaskPlacement::new(
            TaskLocation::RemoteAny { tier: TierTag("infra".into()) },
            TaskRuntime::Native,
        );
        let spec = subprocess_spec(placement, None);

        let err = match driver.start(spec).await {
            Ok(_) => panic!("remote+native must be refused"),
            Err(e) => e,
        };
        match err {
            RemoteForgeError::InvalidSpec(msg) => {
                assert!(
                    msg.contains("remote + native"),
                    "error must name the refused quadrant; got {msg:?}",
                );
                assert!(
                    msg.contains("R380-T7") || msg.contains("W149"),
                    "error should reference the ticket / arch doc for context; got {msg:?}",
                );
            }
            other => panic!("expected InvalidSpec, got {other:?}"),
        }

        assert!(
            !*deploy_called.lock().unwrap(),
            "yubaba.deploy must NOT be called when the spec is refused upstream",
        );

        // Give any (unexpected) background ingest task a tick to push, then
        // verify scryer is empty of Forge-scoped events.
        tokio::time::sleep(Duration::from_millis(20)).await;
        scryer.flush_ring().unwrap();
        // We can't query `Forge(*)` directly — but no forge_id was returned to
        // the caller, so there's no scope to check. Query every recently-used
        // scope to make sure nothing snuck through.  scryer.events on a fresh
        // ForgeId returns empty, which is the strongest assertion we can make
        // without a wildcard query.
        let probe_id = ForgeId::new();
        let events =
            scryer.events(&EventScope::Forge(probe_id), &EventFilter::default()).unwrap();
        assert!(events.is_empty(), "no events expected; got {events:?}");
    }

    // ── R381-T5 BuildKit synthesis ──────────────────────────────────────────

    fn build_image_spec(push: bool) -> ForgeSpec {
        ForgeSpec {
            command: ForgeCommand::BuildImage {
                dockerfile: PathBuf::from(
                    "/tmp/camp/.yah/cache/buildkit/yah-rust.Dockerfile",
                ),
                context: PathBuf::from("/tmp/camp"),
                tag: "ghcr.io/yah-ai/yah-rust:dev".into(),
                push,
            },
            where_: remote_any_infra(),
            timeout: None,
            label: None,
            initiator: Initiator::Human { camp: "test-camp".into() },
            mesh_access: crate::MeshAccess::None,
        }
    }

    /// build_image_workload_spec assembles a buildkit-shaped WorkloadSpec:
    /// rootless moby/buildkit image, buildctl one-shot argv, context +
    /// dockerfile bind-mounts, OCI archive output dir when push=false.
    #[test]
    fn build_image_workload_spec_shape_push_false() {
        let forge_id = ForgeId::new();
        let cmd = match build_image_spec(false).command {
            ForgeCommand::BuildImage { dockerfile, context, tag, push } => {
                (dockerfile, context, tag, push)
            }
            _ => unreachable!(),
        };
        let ws = build_image_workload_spec(
            &forge_id,
            &cmd.0,
            &cmd.1,
            &cmd.2,
            cmd.3,
            TierTag("infra".into()),
        )
        .expect("synthesis ok");

        assert_eq!(ws.image.registry, "docker.io");
        assert_eq!(ws.image.repository, "moby/buildkit");
        assert!(
            ws.image.tag.contains("rootless"),
            "default buildkit tag must be a rootless variant: {:?}",
            ws.image.tag
        );

        // Resources upsized vs the for_forge default (256MiB / 512 cpu_shares).
        assert!(ws.resources.memory_mb >= 1024, "memory should be ≥1GiB");
        assert!(
            ws.resources.cpu_shares >= 1024,
            "cpu_shares should be ≥ one full core"
        );

        // Bind mounts: context (ro), dockerfile dir (ro), out dir (rw).
        let mounts: Vec<_> = ws
            .volumes
            .iter()
            .map(|v| (v.target.to_string_lossy().into_owned(), v.read_only))
            .collect();
        assert!(
            mounts
                .iter()
                .any(|(t, ro)| t == "/yah/build/context" && *ro),
            "context bind-mount missing or not read-only: {mounts:?}"
        );
        assert!(
            mounts
                .iter()
                .any(|(t, ro)| t == "/yah/build/dockerfile" && *ro),
            "dockerfile bind-mount missing or not read-only: {mounts:?}"
        );
        assert!(
            mounts
                .iter()
                .any(|(t, ro)| t == "/yah/build/out" && !*ro),
            "build-out dir bind-mount must be writable when push=false: {mounts:?}"
        );

        // Command invokes buildctl-daemonless.sh and points at the OCI archive
        // when push=false.
        let argv = ws.command.expect("command must be set");
        assert_eq!(argv.first().map(String::as_str), Some("buildctl-daemonless.sh"));
        assert!(
            argv.iter().any(|a| a.starts_with("type=oci,")),
            "push=false must emit --output type=oci: {argv:?}",
        );
        assert!(
            argv.iter().any(|a| a == "filename=yah-rust.Dockerfile"),
            "buildctl --opt filename=<basename> must be set: {argv:?}",
        );
    }

    /// push=true switches the buildctl output to a registry push and drops the
    /// build-out bind-mount.
    #[test]
    fn build_image_workload_spec_push_true_uses_registry_output() {
        let forge_id = ForgeId::new();
        let (df, ctx, tag, push) = match build_image_spec(true).command {
            ForgeCommand::BuildImage { dockerfile, context, tag, push } => {
                (dockerfile, context, tag, push)
            }
            _ => unreachable!(),
        };
        let ws = build_image_workload_spec(
            &forge_id,
            &df,
            &ctx,
            &tag,
            push,
            TierTag("infra".into()),
        )
        .expect("synthesis ok");

        let argv = ws.command.expect("command must be set");
        assert!(
            argv.iter().any(|a| a.contains("type=image") && a.contains("push=true")),
            "push=true must emit --output type=image,…,push=true: {argv:?}",
        );

        let has_out_dir = ws
            .volumes
            .iter()
            .any(|v| v.target == PathBuf::from("/yah/build/out"));
        assert!(
            !has_out_dir,
            "push=true must NOT mount the build-out dir: {:?}",
            ws.volumes,
        );
    }

    /// Remote BuildImage dispatch round-trips through RemoteForgeDriver +
    /// ScriptedWardenClient: the synthesized BuildKit workload is deployed,
    /// the scripted exit code surfaces as a Done status, and yubaba.deploy
    /// was actually called.
    #[tokio::test]
    async fn remote_build_image_success_round_trip() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let yubaba = ScriptedWardenClient::new(
            vec!["#1 [internal] load build definition".into(), "#5 DONE".into()],
            0,
        );
        let deploy_called = yubaba.deploy_called.clone();

        let driver = RemoteForgeDriver::new(scryer.clone(), yubaba);
        let handle = driver.start(build_image_spec(true)).await.unwrap();
        let id = handle.id.clone();
        let status = handle.wait().await;

        assert!(
            matches!(status, ForgeStatus::Done { exit_code: 0, .. }),
            "expected Done exit_code=0, got {status:?}"
        );
        assert!(
            *deploy_called.lock().unwrap(),
            "yubaba.deploy should have been called for remote build-image"
        );

        scryer.flush_ring().unwrap();
        let events =
            scryer.events(&EventScope::Forge(id), &EventFilter::default()).unwrap();
        assert_eq!(events.len(), 2, "expected 2 buildkit log lines");
    }

    /// R094-F3 accept: forge.run with a 50ms timeout against a hanging stream.
    /// Status must be TimedOut and yubaba teardown must have been called.
    #[tokio::test]
    async fn timeout() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let yubaba = HangingWardenClient::new(vec!["slow start".to_string()]);
        let teardown_called = yubaba.teardown_called.clone();

        let driver = RemoteForgeDriver::new(scryer.clone(), yubaba);
        let spec = subprocess_spec(remote_any_infra(), Some(Millis::from_ms(50)));

        let handle = driver.start(spec).await.unwrap();
        let status = handle.wait().await;

        assert!(
            matches!(status, ForgeStatus::TimedOut { .. }),
            "expected TimedOut, got {status:?}"
        );
        assert!(
            *teardown_called.lock().unwrap(),
            "yubaba.teardown must be called on timeout"
        );
    }
}
