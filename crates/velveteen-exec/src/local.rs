//! task::local — local execution helpers for the local quadrant of
//! [`TaskPlacement`].
//!
//! Two runtimes share the `local` location:
//!
//! - **Native** — `tokio::process::Command` directly. Callers stream stdout
//!   and stderr themselves; no helper is needed here.
//! - **Container** — `docker run --rm <image> <argv>` against whatever
//!   Docker-compatible CLI is on PATH. This module assembles the command;
//!   the caller spawns and drains it (so the call site can hook its own
//!   stdout/stderr event sink, matching whatever it does in the native
//!   path).
//!
//! Runtime discovery (OrbStack / Docker Desktop / Colima / Podman socket
//! probing) lives in `local-driver`; this module deliberately stays out of
//! that dep — `local-driver` is built around long-lived appliance
//! containers (pond MinIO, miniflare), not one-shot exec. A follow-up can
//! route the docker CLI through `LocalRuntime::cmd()` once a real consumer
//! needs the socket-probe story (R380-T6's qed step path uses the default
//! `docker` shim that OrbStack / Docker Desktop / Colima all install on
//! PATH).
//!
//! # Image policy
//!
//! `ImageRef` carries `{ registry, repository, tag, digest }`. When `digest`
//! is `Some`, the docker arg is `<registry>/<repository>@<digest>` —
//! identical to the yubaba remote path. When `digest` is `None`, falls back
//! to `<registry>/<repository>:<tag>`. Callers wanting digest pinning for
//! local runs construct an `ImageRef` with `Some(digest)` themselves; the
//! default builder ([`crate::default_image::default_forge_image`]) honours
//! the per-image `YAH_<NAME>_DIGEST` env vars (e.g. `YAH_RUST_BUN_DIGEST`)
//! at compile time — see [`crate::default_image`] for the full list.
//!
//! @yah:ticket(R438-T13, "ForgeExecutor trait + LocalForgeDriver in task::local")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-05T00:03:22Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R438)
//! @yah:next("For ForgeCommand::BuildImage / Workload — defer (return Unsupported). Cloud reconciler only needs Subprocess; qed's BuildImage path stays in qed::runner for now and lands later.")
//! @yah:verify("cargo check --workspace --locked  # nothing else depends on the new trait yet so this should stay clean")
//! @yah:assumes("The minimal task::ExecEvent shape (Started/Output/Finished) is sufficient for both qed (which adapts to QedEvent::StepOutput) and cloud (which ignores). If qed needs richer events the adapter can fill them; if cloud needs progress for HTTP-tied transforms it's already covered via Output.")
//! @arch:see(.yah/docs/working/W164-derived-static-assets.md)
//! @yah:depends_on(R438-T1)
//! @yah:depends_on(R438-T2)
//! @yah:depends_on(R438-T3)
//! @yah:depends_on(R438-T4)
//! @yah:handoff("T13 landed. New module crates/yah/task/src/executor.rs defines: ForgeExecutor trait (async fn execute(spec, ctx, sink)), ExecContext {cwd, env}, ExecEvent (Started/Output{stream,line}/Finished{status}), OutputStream (Stdout/Stderr), ExecOutcome {status: ForgeStatus, stderr_tail: String}, ForgeExecutorError (Unsupported/Spawn/Io). Re-exported from task::lib. LocalForgeDriver added to task::local — implements ForgeExecutor for ForgeCommand::Subprocess (Native via tokio::process::Command, Container via existing local_container_command); rejects BuildImage/Workload with Unsupported. Drains stdout/stderr line-by-line in concurrent tasks; stderr tail captured into ExecOutcome.stderr_tail; sink optional. 8 new tests: native happy + non-zero + cwd/env passthrough + empty argv rejection + container-without-image rejection + container-routes-through-docker (mirror of qed's local_container_step_routes_through_docker_path) + BuildImage Unsupported + Workload Unsupported. cargo test -p task --lib: 67 pass (was 49). cargo check --workspace --locked clean.")
//! @yah:next("R438-T15 (cloud reconciler materialize step) is also unblocked and can proceed in parallel with T14. Cloud reconciler adds `executor: Arc<dyn ForgeExecutor>` field (Arc::new(LocalForgeDriver::new()) default), no qed dep needed — only a new `task = { path = \"../task\" }` edge on cloud's Cargo.toml. Lowering: TransformRecipe -> ForgeSpec{ command: Subprocess{argv, image}, where_: TaskPlacement{Local, recipe.placement.runtime} }; ExecContext{cwd: workload_dir, env: vec![]} bind the YAH_TRANSFORM_IN_0 / OUT via substituted argv (recipe loader already handles that).")
//! @yah:verify("cargo test -p task --lib  # 67 pass (was 49 — 8 new LocalForgeDriver tests under local::tests)")
//! @yah:verify("cargo check --workspace --locked  # clean")

use std::path::Path;
use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc::UnboundedSender;
use workload_spec::ImageRef;

use crate::executor::{
    ExecContext, ExecEvent, ExecOutcome, ForgeExecutor, ForgeExecutorError, OutputStream,
};
use velveteen::{ForgeCommand, ForgeSpec, ForgeStatus, TaskLocation, TaskRuntime};

/// Format an [`ImageRef`] as the single positional argument passed to
/// `docker run`. Pinned images resolve content-addressed (`repo:tag@digest`);
/// unpinned images (the all-zeros [`ImageRef::UNPINNED_DIGEST`] sentinel — a
/// dev build or a not-yet-published catalog image) fall back to tag-only, since
/// docker holds a locally-built or tag-pulled image under `repo:tag`, never
/// under the sentinel digest (R590-B5). Delegates to [`ImageRef::pull_ref`] so
/// the tag-fallback rule lives in one place.
pub fn image_ref_arg(image: &ImageRef) -> String {
    image.pull_ref()
}

/// Build a `tokio::process::Command` for `docker run --rm` that, when
/// spawned, executes `argv` inside `image`. `cwd`, when set, is mounted
/// read-write at the same path inside the container and used as the
/// container's working directory — this is the standard dev pattern that
/// keeps relative paths in stdout matching the host's view.
///
/// `env` is passed through with `-e KEY=VALUE`. The caller spawns the
/// returned `Command` and drains stdout / stderr the same way the native
/// path does — see `qed::runner::execute_step_local` for the canonical
/// shape.
///
/// The returned command sets `kill_on_drop(true)` so a cancelled run
/// terminates the container instead of orphaning it.
pub fn local_container_command(
    image: &ImageRef,
    argv: &[String],
    cwd: Option<&Path>,
    env: &[(String, String)],
    platform: Option<&str>,
    produced_dir: Option<&Path>,
    cache_dir: Option<&Path>,
) -> Command {
    let mut cmd = Command::new("docker");
    cmd.arg("run").arg("--rm");

    if let Some(platform) = platform {
        cmd.arg("--platform").arg(platform);
    }

    if let Some(cwd) = cwd {
        cmd.arg("-v").arg(format!("{0}:{0}", cwd.display()));
        cmd.arg("-w").arg(cwd);
    }

    // R560-B12: bind the caller's produced dir at the conventional container
    // path, so a step's declared `produces` survive `--rm`. Mounted at
    // `forge_produced::CONTAINER_DIR` — the SAME path kamaji binds on a remote
    // worker — so one argv writes to one path and works in both placements.
    // That sameness is the point: `mesofact-musl`'s two legs differ only in
    // triple and image, and a local-only output convention would have made them
    // differ in the one place the file insists they must not.
    if let Some(produced_dir) = produced_dir {
        cmd.arg("-v").arg(format!(
            "{}:{}",
            produced_dir.display(),
            workload_spec::forge_produced::CONTAINER_DIR
        ));
    }

    // R876-F4: the build-cache bind, at the SAME container path a remote
    // worker's kamaji binds `forge_cache::HOST_ROOT/<key>` to. Only the host
    // side differs between placements — a camp-cache dir here, a worker state
    // dir there — so `mesofact-musl`'s two legs can export one
    // `CARGO_TARGET_DIR` and mean it in both.
    if let Some(cache_dir) = cache_dir {
        cmd.arg("-v").arg(format!(
            "{}:{}",
            cache_dir.display(),
            workload_spec::forge_cache::CONTAINER_DIR
        ));
    }

    for (k, v) in env {
        cmd.arg("-e").arg(format!("{k}={v}"));
    }

    cmd.arg(image_ref_arg(image));
    for a in argv {
        cmd.arg(a);
    }

    cmd.kill_on_drop(true);
    cmd
}

// ─── docker buildx (R381-T4) ──────────────────────────────────────────────────

/// Options for [`build_image_command`].
///
/// Path types are borrowed because the builder is sync and the caller owns
/// the buffers (typically the runner allocates the Dockerfile path under
/// `.yah/cache/buildkit/`).
#[derive(Debug, Clone)]
pub struct BuildImageOptions<'a> {
    /// Absolute or workspace-relative path to the Dockerfile.
    pub dockerfile: &'a Path,
    /// Build context (typically `.`).
    pub context: &'a Path,
    /// Image tag (`<repo>:<version>` or `<registry>/<repo>:<version>`).
    pub tag: &'a str,
    /// Push to the tag's registry on success (`--push`). Mutually exclusive
    /// with [`Self::oci_archive`] — when both are set the buildx command
    /// emits both outputs (the OCI archive serializes locally, the registry
    /// push uploads).
    pub push: bool,
    /// BuildKit local cache directory (`--cache-to type=local,dest=…` +
    /// `--cache-from type=local,src=…`). `None` skips cache flags.
    pub cache_dir: Option<&'a Path>,
    /// OCI image archive output (`--output type=oci,dest=…`). Required when
    /// no registry is configured — pipeline downstream consumers reference
    /// the resulting tarball by file path.
    pub oci_archive: Option<&'a Path>,
    /// Load the built image directly into the local docker daemon (`--load`).
    /// Mutually exclusive with multi-platform builds; for local dev pipelines
    /// where the image must be immediately runnable by `docker run`. When
    /// `true`, `oci_archive` is typically `None` (caller's responsibility).
    pub load: bool,
    /// Target platforms (`--platform linux/amd64,linux/arm64`), R590-F2. Empty
    /// slice ⇒ no `--platform` flag (host-native build). Multi-platform buildx
    /// builds cannot be `--load`ed into the local daemon.
    pub platforms: &'a [String],
    /// `--build-arg KEY=VALUE` pairs, order-preserving (R590-F2).
    pub build_args: &'a [(String, String)],
}

/// Build a `docker buildx build …` command for a build-image step.
///
/// The shape is:
///
/// ```text
/// docker buildx build \
///   --file <dockerfile> \
///   --tag <tag> \
///   [--platform <csv>] \
///   [--build-arg KEY=VALUE]... \
///   [--push] \
///   [--cache-to type=local,dest=<cache>] \
///   [--cache-from type=local,src=<cache>] \
///   [--output type=oci,dest=<archive>] \
///   <context>
/// ```
///
/// `kill_on_drop(true)` is set so cancelling the run terminates buildx
/// instead of orphaning the in-progress build.
pub fn build_image_command(opts: &BuildImageOptions<'_>) -> Command {
    let mut cmd = Command::new("docker");
    cmd.arg("buildx").arg("build");

    cmd.arg("--file").arg(opts.dockerfile);
    cmd.arg("--tag").arg(opts.tag);

    if !opts.platforms.is_empty() {
        cmd.arg("--platform").arg(opts.platforms.join(","));
    }
    for (k, v) in opts.build_args {
        cmd.arg("--build-arg").arg(format!("{k}={v}"));
    }

    if opts.push {
        cmd.arg("--push");
    }

    if let Some(cache) = opts.cache_dir {
        cmd.arg("--cache-to")
            .arg(format!("type=local,dest={}", cache.display()));
        cmd.arg("--cache-from")
            .arg(format!("type=local,src={}", cache.display()));
    }

    if let Some(archive) = opts.oci_archive {
        cmd.arg("--output")
            .arg(format!("type=oci,dest={}", archive.display()));
    }

    if opts.load {
        cmd.arg("--load");
    }

    cmd.arg(opts.context);
    cmd.kill_on_drop(true);
    cmd
}

// ─── docker buildx imagetools (R590-F2 multi-arch stitch) ─────────────────────

/// Build a `docker buildx imagetools create` command that stitches N per-arch
/// source images (already pushed to a registry) into one multi-arch manifest
/// list published under `target`.
///
/// This is a registry-only operation — it reads the source images' manifests
/// and writes a new manifest-list tag; it does NOT need a build worker or the
/// build context, only registry access + a local `docker buildx`. That is why
/// the qed runner runs it host-native even for a `--where=remote` pipeline: the
/// per-arch builds fan out to the arch-matched fleet, but the manifest stitch
/// runs where qed runs.
///
/// The shape is:
///
/// ```text
/// docker buildx imagetools create \
///   --tag <target> \
///   <source0> <source1> ...
/// ```
///
/// Sources are the arch-specific tags each per-arch build pushed (e.g.
/// `ghcr.io/yah-ai/img:v1-amd64`, `…:v1-arm64`); `target` is the arch-agnostic
/// tag (`…:v1`) consumers pull. Caller guarantees ≥1 source.
pub fn imagetools_create_command(target: &str, sources: &[String]) -> Command {
    let mut cmd = Command::new("docker");
    cmd.arg("buildx").arg("imagetools").arg("create");
    cmd.arg("--tag").arg(target);
    for src in sources {
        cmd.arg(src);
    }
    cmd.kill_on_drop(true);
    cmd
}

// ─── LocalForgeDriver ────────────────────────────────────────────────────────

/// [`ForgeExecutor`] backed by host-side subprocesses. Dispatches by
/// `spec.where_.runtime`:
///
/// - `Native` — `tokio::process::Command::new(argv[0])` with `ctx.cwd` /
///   `ctx.env` applied.
/// - `Container` — routes through [`local_container_command`]
///   (`docker run --rm -v <cwd>:<cwd> -w <cwd> -e KEY=VAL <image> <argv>`).
///
/// Stdout and stderr are drained line-by-line in concurrent tasks. When a
/// sink is attached, each line is forwarded as an [`ExecEvent::Output`];
/// regardless, the trailing stderr (joined by `\n`, trimmed) is captured
/// into [`ExecOutcome::stderr_tail`] so callers can surface a failure
/// message without re-aggregating events.
///
/// Refuses [`ForgeCommand::BuildImage`] and [`ForgeCommand::Workload`] with
/// [`ForgeExecutorError::Unsupported`] — those have dedicated paths
/// (qed's build-image dispatch; yubaba RPC).
///
/// Also refuses any spec whose `where_.location` is not
/// [`TaskLocation::Local`]. That check is load-bearing, not defensive: W235
/// opened recipe placement to `remote` / `remote_any`, and this driver has
/// always dispatched on `runtime` alone. Without the guard a recipe that says
/// `location = { kind = "remote_any", … }` would run happily *on the dev box*
/// and report success — the failure mode being an arch-mismatched artifact
/// produced under emulation, discovered hours later at link time. Fail at
/// dispatch instead; the caller's job is to route remote specs to
/// [`RemoteForgeDriver`](crate::remote::RemoteForgeDriver).
#[derive(Debug, Default, Clone, Copy)]
pub struct LocalForgeDriver;

impl LocalForgeDriver {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ForgeExecutor for LocalForgeDriver {
    async fn execute(
        &self,
        spec: ForgeSpec,
        ctx: ExecContext,
        sink: Option<UnboundedSender<ExecEvent>>,
    ) -> Result<ExecOutcome, ForgeExecutorError> {
        if !matches!(spec.where_.location, TaskLocation::Local) {
            return Err(ForgeExecutorError::Unsupported(
                "LocalForgeDriver received a spec with a remote placement.location — \
                 route it to RemoteForgeDriver, or set placement.location = \"local\". \
                 Running it here would silently produce a host-arch artifact (W235)",
            ));
        }
        if ctx.produced.is_some() {
            return Err(ForgeExecutorError::Unsupported(
                "LocalForgeDriver received ExecContext::produced, which is a remote-only \
                 retrieval contract — a local run writes its artifact straight onto this \
                 filesystem, so there is nothing to fetch. Bind the output path directly \
                 instead of asking for retrieval",
            ));
        }
        if !ctx.secrets.is_empty() {
            return Err(ForgeExecutorError::Unsupported(
                "LocalForgeDriver received ExecContext::secrets. Vault credentials are \
                 resolved by yubaba on the node that runs the workload; this box has no \
                 cluster secret store, so the mount cannot be honored — and running the \
                 step without it would fail later, inside the build, as a missing-auth \
                 error nobody would trace back to placement (R555-F5)",
            ));
        }
        let runtime = spec.where_.runtime;
        match spec.command {
            ForgeCommand::Subprocess { argv, image } => match runtime {
                TaskRuntime::Native => {
                    // R560-B12: a produced-dir bind is a CONTAINER affordance.
                    // A native local run has no filesystem boundary to bridge,
                    // so honoring this would mean silently mounting nothing and
                    // letting the step write wherever it liked — the caller then
                    // reads an empty dir and blames its own path. Refuse, in the
                    // same spirit as the `produced` refusal above.
                    if ctx.produced_dir.is_some() {
                        return Err(ForgeExecutorError::Unsupported(
                            "LocalForgeDriver received ExecContext::produced_dir on a NATIVE \
                             step. That field binds a host dir into a container; a native run \
                             already writes straight onto this filesystem, so there is no \
                             boundary to bridge. Point the step's `produces` at the path it \
                             actually writes, or set runtime = container",
                        ));
                    }
                    // R876-F4: same shape, same reason. A native run's build
                    // scratch already lives on this filesystem and persists on
                    // its own; binding a cache dir into a process that has no
                    // mount namespace would mount nothing and report a cache
                    // the step never saw.
                    if ctx.cache_dir.is_some() {
                        return Err(ForgeExecutorError::Unsupported(
                            "LocalForgeDriver received ExecContext::cache_dir on a NATIVE \
                             step. That field binds a host dir into a container; a native run \
                             already keeps its build scratch on this filesystem. Drop `cache` \
                             from the step, or set runtime = container (R876-F4)",
                        ));
                    }
                    run_subprocess(build_native_command(&argv, &ctx)?, sink).await
                }
                TaskRuntime::Container => {
                    let image = image.ok_or(ForgeExecutorError::Unsupported(
                        "container runtime requires a Subprocess image",
                    ))?;
                    run_subprocess(build_container_command(&image, &argv, &ctx), sink).await
                }
                // R605-F8: microVM is remote-only by construction. The point of
                // booting a guest is to protect *co-tenants* — a raft voter, a
                // live site — and a dev box has none, so a local microVM would
                // cost a kernel boot to isolate a build from its own author.
                TaskRuntime::MicroVm => Err(ForgeExecutorError::Unsupported(
                    "runtime = microvm is remote-only: it isolates a build from what else \
                     is running on the node, and locally that is you. Use runtime = \
                     container (or native) for a local run (R605-F8)",
                )),
            },
            ForgeCommand::BuildImage { .. } => Err(ForgeExecutorError::Unsupported("BuildImage")),
            ForgeCommand::Workload { .. } => Err(ForgeExecutorError::Unsupported("Workload")),
        }
    }
}

fn build_native_command(argv: &[String], ctx: &ExecContext) -> Result<Command, ForgeExecutorError> {
    let program = argv.first().ok_or(ForgeExecutorError::Unsupported(
        "Subprocess argv is empty",
    ))?;
    let mut cmd = Command::new(program);
    if argv.len() > 1 {
        cmd.args(&argv[1..]);
    }
    if let Some(cwd) = &ctx.cwd {
        cmd.current_dir(cwd);
    }
    for (k, v) in &ctx.env {
        cmd.env(k, v);
    }
    cmd.kill_on_drop(true);
    Ok(cmd)
}

fn build_container_command(image: &ImageRef, argv: &[String], ctx: &ExecContext) -> Command {
    local_container_command(
        image,
        argv,
        ctx.cwd.as_deref(),
        &ctx.env,
        ctx.platform.as_deref(),
        ctx.produced_dir.as_deref(),
        ctx.cache_dir.as_deref(),
    )
}

async fn run_subprocess(
    mut cmd: Command,
    sink: Option<UnboundedSender<ExecEvent>>,
) -> Result<ExecOutcome, ForgeExecutorError> {
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    if let Some(tx) = &sink {
        let _ = tx.send(ExecEvent::Started);
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| ForgeExecutorError::Spawn(e.to_string()))?;
    let stdout = child.stdout.take().expect("stdout piped above");
    let stderr = child.stderr.take().expect("stderr piped above");

    let stdout_task = {
        let sink = sink.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(tx) = &sink {
                    let _ = tx.send(ExecEvent::Output {
                        stream: OutputStream::Stdout,
                        line,
                    });
                }
            }
        })
    };

    let stderr_task = {
        let sink = sink.clone();
        tokio::spawn(async move {
            let mut captured: Vec<String> = Vec::new();
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(tx) = &sink {
                    let _ = tx.send(ExecEvent::Output {
                        stream: OutputStream::Stderr,
                        line: line.clone(),
                    });
                }
                captured.push(line);
            }
            captured
        })
    };

    let status = child.wait().await?;
    let _ = stdout_task.await;
    let stderr_lines = stderr_task.await.unwrap_or_default();
    let stderr_tail = stderr_lines.join("\n").trim().to_string();

    let ended_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let forge_status = match status.code() {
        Some(code) => ForgeStatus::Done {
            exit_code: code,
            ended_at,
        },
        None => {
            // Process terminated by signal; treat as Killed with signal=-1
            // when we can't read the signal (Windows / unusual termination).
            #[cfg(unix)]
            let signal = {
                use std::os::unix::process::ExitStatusExt;
                status.signal().unwrap_or(-1)
            };
            #[cfg(not(unix))]
            let signal = -1;
            ForgeStatus::Killed { signal, ended_at }
        }
    };

    if let Some(tx) = &sink {
        let _ = tx.send(ExecEvent::Finished {
            status: forge_status.clone(),
        });
    }

    Ok(ExecOutcome {
        status: forge_status,
        stderr_tail,
    })
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::path::PathBuf;

    fn args_of(cmd: &Command) -> Vec<String> {
        cmd.as_std()
            .get_args()
            .map(|a: &OsStr| a.to_string_lossy().into_owned())
            .collect()
    }

    fn img(registry: &str, repository: &str, tag: &str, digest: &str) -> ImageRef {
        ImageRef {
            registry: registry.into(),
            repository: repository.into(),
            tag: tag.into(),
            digest: digest.into(),
        }
    }

    #[test]
    fn image_ref_arg_formats_with_tag_and_digest() {
        let img = img(
            "ghcr.io",
            "yah-ai/forge-minimal",
            "latest",
            "sha256:abc123",
        );
        assert_eq!(
            image_ref_arg(&img),
            "ghcr.io/yah-ai/forge-minimal:latest@sha256:abc123",
        );
    }

    #[test]
    fn image_ref_arg_unpinned_falls_back_to_tag_only() {
        // A not-yet-published catalog image (all-zeros sentinel) pulls by tag;
        // docker holds it under `repo:tag`, not `repo@sha256:0000…` (R590-B5).
        let img = img(
            "ghcr.io",
            "yah-ai/rusty-v8-musl-builder",
            "latest",
            workload_spec::testing::TEST_DIGEST,
        );
        assert_eq!(
            image_ref_arg(&img),
            "ghcr.io/yah-ai/rusty-v8-musl-builder:latest",
        );
    }

    #[test]
    fn command_program_is_docker() {
        let image = img(
            "ghcr.io",
            "yah-ai/forge-minimal",
            "latest",
            workload_spec::testing::TEST_DIGEST,
        );
        let cmd = local_container_command(&image, &["true".into()], None, &[], None, None, None);
        assert_eq!(cmd.as_std().get_program(), "docker");
    }

    const TEST_PIN: &str = "sha256:deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

    #[test]
    fn command_argv_no_cwd_no_env() {
        let image = img("ghcr.io", "yah-ai/forge-minimal", "latest", TEST_PIN);
        let cmd = local_container_command(
            &image,
            &["echo".into(), "hi".into()],
            None,
            &[],
            None,
            None,
            None,
        );
        assert_eq!(
            args_of(&cmd),
            vec![
                "run",
                "--rm",
                &format!("ghcr.io/yah-ai/forge-minimal:latest@{TEST_PIN}"),
                "echo",
                "hi",
            ],
        );
    }

    #[test]
    fn command_mounts_and_chdirs_into_cwd() {
        let image = img("ghcr.io", "yah-ai/forge-minimal", "latest", TEST_PIN);
        let cwd = PathBuf::from("/work/repo");
        let cmd = local_container_command(&image, &["pwd".into()], Some(&cwd), &[], None, None, None);
        assert_eq!(
            args_of(&cmd),
            vec![
                "run",
                "--rm",
                "-v",
                "/work/repo:/work/repo",
                "-w",
                "/work/repo",
                &format!("ghcr.io/yah-ai/forge-minimal:latest@{TEST_PIN}"),
                "pwd",
            ],
        );
    }

    /// R560-B12: the produced dir is bound at the CONVENTIONAL container path,
    /// not at its host path. Asserting the exact `-v` string is the point — the
    /// remote leg's argv writes to `/yah/produced/…` because kamaji binds it
    /// there, and a local leg that bound it anywhere else would force the two
    /// legs of a pipeline to carry different argvs for the same build.
    #[test]
    fn command_binds_the_produced_dir_at_the_conventional_container_path() {
        let image = img("ghcr.io", "yah-ai/forge-minimal", "latest", TEST_PIN);
        let produced = PathBuf::from("/camp/.yah/cache/qed/produced/run1/build");
        let cmd = local_container_command(
            &image,
            &["true".into()],
            None,
            &[],
            None,
            Some(&produced),
            None,
        );
        assert_eq!(
            args_of(&cmd),
            vec![
                "run",
                "--rm",
                "-v",
                "/camp/.yah/cache/qed/produced/run1/build:/yah/produced",
                &format!("ghcr.io/yah-ai/forge-minimal:latest@{TEST_PIN}"),
                "true",
            ],
        );
    }

    /// Absent by default, so every pre-R560-B12 call site keeps emitting the
    /// exact same docker argv it always did.
    #[test]
    fn no_produced_dir_means_no_extra_mount() {
        let image = img("ghcr.io", "yah-ai/forge-minimal", "latest", TEST_PIN);
        let cmd = local_container_command(&image, &["true".into()], None, &[], None, None, None);
        assert!(
            !args_of(&cmd).iter().any(|a| a.contains("/yah/produced")),
            "a step declaring no produces must get no produced mount",
        );
    }

    /// R876-F4: the local placement binds its cache at the SAME container path
    /// a remote worker does, which is what lets `mesofact-musl`'s two legs run
    /// one argv. Only the host side differs.
    #[test]
    fn command_binds_the_cache_dir_at_the_conventional_container_path() {
        let image = img("ghcr.io", "yah-ai/forge-minimal", "latest", TEST_PIN);
        let cache = PathBuf::from("/camp/.yah/cache/qed/build/p.step.aarch64-unknown-linux-musl");
        let cmd = local_container_command(
            &image,
            &["true".into()],
            None,
            &[],
            None,
            None,
            Some(&cache),
        );
        assert_eq!(
            args_of(&cmd),
            vec![
                "run",
                "--rm",
                "-v",
                "/camp/.yah/cache/qed/build/p.step.aarch64-unknown-linux-musl:/yah/cache",
                &format!("ghcr.io/yah-ai/forge-minimal:latest@{TEST_PIN}"),
                "true",
            ],
        );
    }

    #[test]
    fn no_cache_dir_means_no_cache_mount() {
        let image = img("ghcr.io", "yah-ai/forge-minimal", "latest", TEST_PIN);
        let cmd = local_container_command(&image, &["true".into()], None, &[], None, None, None);
        assert!(
            !args_of(&cmd).iter().any(|a| a.contains("/yah/cache")),
            "a step declaring no cache must get no cache mount",
        );
    }

    /// The refusal that keeps `produced_dir` from being read as "an output
    /// directory" in general. On a native local step there is no boundary to
    /// bridge, so honoring it would mount nothing and silently succeed.
    #[tokio::test]
    async fn a_produced_dir_on_a_native_step_is_refused_rather_than_ignored() {
        let err = LocalForgeDriver::new()
            .execute(
                ForgeSpec {
                    command: ForgeCommand::Subprocess {
                        argv: vec!["true".into()],
                        image: None,
                    },
                    where_: velveteen::TaskPlacement::new(TaskLocation::Local, TaskRuntime::Native),
                    timeout: None,
                    label: None,
                    initiator: velveteen::Initiator::Human { camp: "t".into() },
                    mesh_access: velveteen::MeshAccess::None,
                    cache_key: None,
                },
                ExecContext::default().with_produced_dir(PathBuf::from("/tmp/out")),
                None,
            )
            .await
            .expect_err("produced_dir is a container affordance");
        assert!(
            matches!(err, ForgeExecutorError::Unsupported(m) if m.contains("NATIVE")),
            "the refusal must name the placement that caused it",
        );
    }

    #[test]
    fn command_passes_env_vars_with_dash_e() {
        let image = img("ghcr.io", "yah-ai/forge-minimal", "latest", TEST_PIN);
        let env = vec![("FOO".into(), "bar".into()), ("BAZ".into(), "qux".into())];
        let cmd = local_container_command(&image, &["env".into()], None, &env, None, None, None);
        let args = args_of(&cmd);
        // -e arrives in declared order
        let foo_idx = args.iter().position(|a| a == "FOO=bar").unwrap();
        let baz_idx = args.iter().position(|a| a == "BAZ=qux").unwrap();
        assert!(foo_idx < baz_idx, "env vars preserve declaration order");
        // Each -e precedes its KEY=VALUE
        assert_eq!(args[foo_idx - 1], "-e");
        assert_eq!(args[baz_idx - 1], "-e");
    }

    #[test]
    fn command_uses_digest_pinned_image() {
        let image = img(
            "ghcr.io",
            "yah-ai/forge-minimal",
            "latest",
            TEST_PIN,
        );
        let cmd = local_container_command(&image, &["true".into()], None, &[], None, None, None);
        let args = args_of(&cmd);
        let expected = format!("ghcr.io/yah-ai/forge-minimal:latest@{TEST_PIN}");
        assert!(
            args.iter().any(|a| a == &expected),
            "digest-pinned image must appear as the docker image arg; got {args:?}",
        );
    }

    // ── docker buildx (R381-T4) ─────────────────────────────────────────────

    fn opts<'a>(
        dockerfile: &'a Path,
        context: &'a Path,
        tag: &'a str,
    ) -> BuildImageOptions<'a> {
        BuildImageOptions {
            dockerfile,
            context,
            tag,
            push: false,
            load: false,
            cache_dir: None,
            oci_archive: None,
            platforms: &[],
            build_args: &[],
        }
    }

    #[test]
    fn build_image_minimum_command_shape() {
        let dockerfile = PathBuf::from("/work/Dockerfile");
        let context = PathBuf::from(".");
        let cmd = build_image_command(&opts(&dockerfile, &context, "yah-rust:dev"));
        assert_eq!(cmd.as_std().get_program(), "docker");
        assert_eq!(
            args_of(&cmd),
            vec![
                "buildx",
                "build",
                "--file",
                "/work/Dockerfile",
                "--tag",
                "yah-rust:dev",
                ".",
            ],
        );
    }

    #[test]
    fn build_image_push_flag_is_passed_through() {
        let dockerfile = PathBuf::from("/work/Dockerfile");
        let context = PathBuf::from(".");
        let mut o = opts(&dockerfile, &context, "yah-rust:dev");
        o.push = true;
        let cmd = build_image_command(&o);
        let args = args_of(&cmd);
        assert!(args.iter().any(|a| a == "--push"));
    }

    #[test]
    fn build_image_emits_cache_to_and_cache_from_when_dir_set() {
        let dockerfile = PathBuf::from("/work/Dockerfile");
        let context = PathBuf::from(".");
        let cache = PathBuf::from(".yah/cache/buildkit");
        let mut o = opts(&dockerfile, &context, "yah-rust:dev");
        o.cache_dir = Some(&cache);
        let cmd = build_image_command(&o);
        let args = args_of(&cmd);
        assert!(
            args.iter().any(|a| a == "type=local,dest=.yah/cache/buildkit"),
            "missing --cache-to: {args:?}",
        );
        assert!(
            args.iter().any(|a| a == "type=local,src=.yah/cache/buildkit"),
            "missing --cache-from: {args:?}",
        );
    }

    #[test]
    fn build_image_emits_oci_archive_output_when_set() {
        let dockerfile = PathBuf::from("/work/Dockerfile");
        let context = PathBuf::from(".");
        let archive = PathBuf::from(".yah/cache/images/yah-rust-dev.tar");
        let mut o = opts(&dockerfile, &context, "yah-rust:dev");
        o.oci_archive = Some(&archive);
        let cmd = build_image_command(&o);
        let args = args_of(&cmd);
        assert!(
            args.iter()
                .any(|a| a == "type=oci,dest=.yah/cache/images/yah-rust-dev.tar"),
            "missing --output: {args:?}",
        );
    }

    #[test]
    fn build_image_load_flag_is_passed_through() {
        let dockerfile = PathBuf::from("/work/Dockerfile");
        let context = PathBuf::from(".");
        let mut o = opts(&dockerfile, &context, "yah-yubaba:latest");
        o.load = true;
        let cmd = build_image_command(&o);
        let args = args_of(&cmd);
        assert!(args.iter().any(|a| a == "--load"), "missing --load in {args:?}");
    }

    #[test]
    fn build_image_emits_platform_and_build_args() {
        let dockerfile = PathBuf::from("/work/Dockerfile");
        let context = PathBuf::from(".");
        let platforms = vec!["linux/amd64".to_string(), "linux/arm64".to_string()];
        let build_args = vec![("RUST_VERSION".to_string(), "1.85".to_string())];
        let mut o = opts(&dockerfile, &context, "yah-rust:dev");
        o.platforms = &platforms;
        o.build_args = &build_args;
        let cmd = build_image_command(&o);
        let args = args_of(&cmd);
        assert!(
            args.windows(2)
                .any(|w| w[0] == "--platform" && w[1] == "linux/amd64,linux/arm64"),
            "missing --platform <csv>: {args:?}",
        );
        assert!(
            args.windows(2)
                .any(|w| w[0] == "--build-arg" && w[1] == "RUST_VERSION=1.85"),
            "missing --build-arg K=V: {args:?}",
        );
    }

    #[test]
    fn imagetools_create_stitches_target_and_sources() {
        let sources = vec![
            "ghcr.io/yah-ai/img:v1-amd64".to_string(),
            "ghcr.io/yah-ai/img:v1-arm64".to_string(),
        ];
        let cmd = imagetools_create_command("ghcr.io/yah-ai/img:v1", &sources);
        assert_eq!(cmd.as_std().get_program(), "docker");
        assert_eq!(
            args_of(&cmd),
            vec![
                "buildx",
                "imagetools",
                "create",
                "--tag",
                "ghcr.io/yah-ai/img:v1",
                "ghcr.io/yah-ai/img:v1-amd64",
                "ghcr.io/yah-ai/img:v1-arm64",
            ],
        );
    }

    #[test]
    fn build_image_context_is_last_positional_arg() {
        let dockerfile = PathBuf::from("/work/Dockerfile");
        let context = PathBuf::from("./monorepo");
        let cmd = build_image_command(&opts(&dockerfile, &context, "tag"));
        let args = args_of(&cmd);
        assert_eq!(args.last(), Some(&"./monorepo".to_string()));
    }

    // ── Integration test (docker required) ────────────────────────────────────

    /// End-to-end smoke: spawn a small public container that exits with a
    /// fixed code and assert the exit status surfaces.
    ///
    /// Marked `#[ignore]` so CI without docker doesn't fail; run locally with:
    ///
    /// ```sh
    /// cargo test -p task local::tests::local_container_run_exits_with_code -- --include-ignored
    /// ```
    #[tokio::test]
    #[ignore]
    async fn local_container_run_exits_with_code() {
        // alpine:3 is small, widely cached, and has /bin/sh. Test isn't
        // pinned to a digest — this is local-dev smoke, not supply-chain.
        let image = img("docker.io/library", "alpine", "3", TEST_PIN);
        let mut cmd = local_container_command(
            &image,
            &["sh".into(), "-c".into(), "echo hello-from-container; exit 7".into()],
            None,
            &[],
            None,
            None,
            None,
        );
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let out = cmd.output().await.expect("docker spawn failed (is docker installed and running?)");
        assert_eq!(out.status.code(), Some(7), "exit code should round-trip");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            stdout.contains("hello-from-container"),
            "stdout should reach the host; got {stdout:?}"
        );
    }

    // ── LocalForgeDriver ──────────────────────────────────────────────────

    use crate::executor::{ExecContext, ExecEvent, ForgeExecutor, ForgeExecutorError};
    use velveteen::{ForgeCommand, ForgeSpec, ForgeStatus, MeshAccess, TaskLocation, TaskPlacement};
    use task_runs::Initiator;
    use tokio::sync::mpsc;

    fn subprocess_spec(argv: Vec<String>, runtime: velveteen::TaskRuntime) -> ForgeSpec {
        ForgeSpec {
            command: ForgeCommand::Subprocess { argv, image: None },
            where_: TaskPlacement::new(TaskLocation::Local, runtime),
            timeout: None,
            label: None,
            initiator: Initiator::Human { camp: "test".into() },
            mesh_access: MeshAccess::None,
            cache_key: None,
        }
    }

    #[tokio::test]
    async fn native_subprocess_succeeds_and_streams_stdout() {
        let driver = LocalForgeDriver::new();
        let spec = subprocess_spec(
            vec!["sh".into(), "-c".into(), "echo hello-native".into()],
            velveteen::TaskRuntime::Native,
        );
        let (tx, mut rx) = mpsc::unbounded_channel();
        let outcome = driver
            .execute(spec, ExecContext::default(), Some(tx))
            .await
            .expect("execute should succeed");
        assert!(outcome.succeeded(), "exit 0 expected, got {:?}", outcome.status);
        assert_eq!(outcome.stderr_tail, "");

        let mut events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }
        assert!(
            matches!(events.first(), Some(ExecEvent::Started)),
            "first event Started, got {:?}",
            events.first()
        );
        assert!(
            matches!(events.last(), Some(ExecEvent::Finished { status }) if matches!(status, ForgeStatus::Done { exit_code: 0, .. })),
            "last event Finished/Done(0), got {:?}",
            events.last()
        );
        assert!(
            events.iter().any(|e| matches!(
                e,
                ExecEvent::Output { stream: OutputStream::Stdout, line } if line == "hello-native"
            )),
            "captured stdout line, got {events:?}"
        );
    }

    #[tokio::test]
    async fn native_non_zero_exit_returns_done_with_code_and_stderr_tail() {
        let driver = LocalForgeDriver::new();
        let spec = subprocess_spec(
            vec![
                "sh".into(),
                "-c".into(),
                "echo boom-native >&2; exit 3".into(),
            ],
            velveteen::TaskRuntime::Native,
        );
        let outcome = driver
            .execute(spec, ExecContext::default(), None)
            .await
            .expect("execute should return Outcome even on failure");
        assert!(!outcome.succeeded(), "exit 3 must not succeed");
        match outcome.status {
            ForgeStatus::Done { exit_code, .. } => assert_eq!(exit_code, 3),
            other => panic!("expected Done(3), got {other:?}"),
        }
        assert_eq!(
            outcome.stderr_tail, "boom-native",
            "stderr tail captured for failure-message synthesis"
        );
    }

    #[tokio::test]
    async fn native_respects_cwd_and_env() {
        let driver = LocalForgeDriver::new();
        let tmp = tempfile::tempdir().unwrap();
        let spec = subprocess_spec(
            vec![
                "sh".into(),
                "-c".into(),
                r#"echo cwd=$(pwd); echo env=$YAH_TEST_VAR"#.into(),
            ],
            velveteen::TaskRuntime::Native,
        );
        let ctx = ExecContext::default()
            .with_cwd(tmp.path().to_path_buf())
            .with_env(vec![("YAH_TEST_VAR".into(), "hello-from-env".into())]);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let outcome = driver.execute(spec, ctx, Some(tx)).await.unwrap();
        assert!(outcome.succeeded());

        let mut stdout_lines = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let ExecEvent::Output {
                stream: OutputStream::Stdout,
                line,
            } = ev
            {
                stdout_lines.push(line);
            }
        }
        // macOS resolves /var to /private/var; canonicalize both sides for comparison
        let actual_cwd_line = stdout_lines
            .iter()
            .find(|l| l.starts_with("cwd="))
            .expect("cwd line should be present");
        let actual_cwd = actual_cwd_line.strip_prefix("cwd=").unwrap();
        let actual = std::fs::canonicalize(actual_cwd).unwrap();
        let expected = std::fs::canonicalize(tmp.path()).unwrap();
        assert_eq!(actual, expected, "subprocess cwd reflects ExecContext.cwd");
        assert!(
            stdout_lines.iter().any(|l| l == "env=hello-from-env"),
            "env var passed through ExecContext, got {stdout_lines:?}"
        );
    }

    #[tokio::test]
    async fn empty_argv_rejected() {
        let driver = LocalForgeDriver::new();
        let spec = subprocess_spec(vec![], velveteen::TaskRuntime::Native);
        let err = driver
            .execute(spec, ExecContext::default(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, ForgeExecutorError::Unsupported(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn container_subprocess_without_image_rejected() {
        let driver = LocalForgeDriver::new();
        let spec = subprocess_spec(vec!["true".into()], velveteen::TaskRuntime::Container);
        let err = driver
            .execute(spec, ExecContext::default(), None)
            .await
            .unwrap_err();
        match err {
            ForgeExecutorError::Unsupported(msg) => {
                assert!(msg.contains("container"), "msg should mention container, got {msg:?}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn container_subprocess_routes_through_docker_when_image_set() {
        // Mirrors qed::runner::tests::local_container_step_routes_through_docker_path:
        // an unreachable docker binary or bogus argv must surface as a clean
        // Spawn error, not panic, and not silently fall back to native.
        let driver = LocalForgeDriver::new();
        let image = ImageRef {
            registry: "ghcr.io".into(),
            repository: "yah-ai/forge-minimal".into(),
            tag: "latest".into(),
            digest: TEST_PIN.into(),
        };
        let spec = ForgeSpec {
            command: ForgeCommand::Subprocess {
                argv: vec!["__nonexistent_binary_for_docker_test__".into()],
                image: Some(image),
            },
            where_: TaskPlacement::new(TaskLocation::Local, velveteen::TaskRuntime::Container),
            timeout: None,
            label: None,
            initiator: Initiator::Human { camp: "test".into() },
            mesh_access: MeshAccess::None,
            cache_key: None,
        };
        let result = driver.execute(spec, ExecContext::default(), None).await;
        // Either spawn fails (no docker) or docker exits non-zero (image
        // pull / argv miss). Both are acceptable — the contract is that we
        // do NOT silently fall back to native subprocess.
        match result {
            Ok(outcome) => {
                assert!(!outcome.succeeded(), "bogus argv must not succeed; got {:?}", outcome.status);
            }
            Err(ForgeExecutorError::Spawn(_)) => { /* docker not installed — fine */ }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn build_image_command_returns_unsupported() {
        let driver = LocalForgeDriver::new();
        let spec = ForgeSpec {
            command: ForgeCommand::BuildImage {
                dockerfile: PathBuf::from("/tmp/Dockerfile"),
                context: PathBuf::from("."),
                context_url: None,
                tags: vec!["x:y".into()],
                platforms: vec![],
                build_args: vec![],
                push: false,
                load: false,
            },
            where_: TaskPlacement::new(TaskLocation::Local, velveteen::TaskRuntime::Container),
            timeout: None,
            label: None,
            initiator: Initiator::Human { camp: "test".into() },
            mesh_access: MeshAccess::None,
            cache_key: None,
        };
        let err = driver
            .execute(spec, ExecContext::default(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, ForgeExecutorError::Unsupported("BuildImage")), "got {err:?}");
    }

    /// R555-T2: W235 opened `RecipeLocation` to `remote` / `remote_any`, and
    /// this driver dispatches on `runtime` alone. Without an explicit refusal a
    /// remote-placed spec runs right here on the dev box and reports success —
    /// the artifact is built for the host architecture and nothing notices
    /// until it fails to link. Fail at dispatch instead.
    #[tokio::test]
    async fn remote_placement_is_refused_not_silently_run_locally() {
        let driver = LocalForgeDriver::new();
        for location in [
            TaskLocation::Remote {
                node: workload_spec::MeshIdent("us-west-002".into()),
            },
            TaskLocation::RemoteAny {
                tier: workload_spec::TierTag("infra".into()),
                mesh_tags: vec!["arch:x86".into()],
            },
        ] {
            let mut spec = subprocess_spec(
                vec!["/bin/sh".into(), "-c".into(), "exit 0".into()],
                velveteen::TaskRuntime::Native,
            );
            spec.where_ = TaskPlacement::new(location.clone(), velveteen::TaskRuntime::Native);

            let err = driver
                .execute(spec, ExecContext::default(), None)
                .await
                .expect_err("remote placement must not run locally");
            assert!(
                matches!(err, ForgeExecutorError::Unsupported(_)),
                "location {location:?} → {err:?}"
            );
            assert!(
                err.to_string().contains("RemoteForgeDriver"),
                "the refusal must name where the spec should go, got: {err}"
            );
        }
    }

    /// R555-F3: `ExecContext::produced` asks the driver to pull an artifact off
    /// a worker. There is no worker here, and the local run wrote its output
    /// straight to the caller's disk — so honoring it is impossible and
    /// ignoring it would hand back a run that looks retrieved and isn't.
    #[tokio::test]
    async fn a_produced_retrieval_request_is_refused_rather_than_ignored() {
        let driver = LocalForgeDriver::new();
        let spec = subprocess_spec(
            vec!["/bin/sh".into(), "-c".into(), "exit 0".into()],
            velveteen::TaskRuntime::Native,
        );
        let err = driver
            .execute(
                spec,
                ExecContext::default()
                    .with_produced(PathBuf::from("/yah/produced/out.bin"), PathBuf::from("/tmp/out.bin")),
                None,
            )
            .await
            .expect_err("produced retrieval is remote-only");
        assert!(
            err.to_string().contains("remote-only"),
            "the refusal must say why, got: {err}"
        );
    }

    /// R555-F5: vault credentials are resolved by yubaba on the node that runs
    /// the workload. Dropping the mount would run the build without the
    /// credential it declared — a missing-auth failure deep inside a two-hour
    /// build, traced back to placement by nobody.
    #[tokio::test]
    async fn a_secret_mount_is_refused_rather_than_ignored() {
        let driver = LocalForgeDriver::new();
        let spec = subprocess_spec(
            vec!["/bin/sh".into(), "-c".into(), "exit 0".into()],
            velveteen::TaskRuntime::Native,
        );
        let err = driver
            .execute(
                spec,
                ExecContext::default().with_secrets(vec![workload_spec::SecretMount {
                    source: workload_spec::SecretRef::Cluster {
                        name: "r2-write".into(),
                    },
                    target: workload_spec::SecretTarget::File {
                        path: PathBuf::from("/run/yah/r2.json"),
                        mode: 0o400,
                    },
                }]),
                None,
            )
            .await
            .expect_err("secret mounts are remote-only");
        assert!(
            matches!(err, ForgeExecutorError::Unsupported(_)),
            "{err:?}"
        );
        assert!(err.to_string().contains("cluster secret store"), "{err}");
    }

    #[tokio::test]
    async fn workload_command_returns_unsupported() {
        use workload_spec::{TierTag, WorkloadSpec};
        let driver = LocalForgeDriver::new();
        let image = ImageRef {
            registry: "ghcr.io".into(),
            repository: "x/y".into(),
            tag: "v1".into(),
            digest: TEST_PIN.into(),
        };
        let workload = WorkloadSpec::for_forge("test-workload", image, TierTag("infra".into()), vec![]);
        let spec = ForgeSpec {
            command: ForgeCommand::Workload { spec: workload },
            where_: TaskPlacement::new(TaskLocation::Local, velveteen::TaskRuntime::Native),
            timeout: None,
            label: None,
            initiator: Initiator::Human { camp: "test".into() },
            mesh_access: MeshAccess::None,
            cache_key: None,
        };
        let err = driver
            .execute(spec, ExecContext::default(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, ForgeExecutorError::Unsupported("Workload")), "got {err:?}");
    }
}
