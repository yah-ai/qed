//! Docker push-family image builder for the qed-gha runtime (R594).
//!
//! When `yah qed run <pipeline>` executes a real `.github/workflows/*.yml` whose
//! image jobs use `docker/login-action` + `docker/build-push-action`, the
//! qed-gha runtime routes those two slugs to an injected
//! [`yah_qed_gha::ImageBuilder`]. [`QedImageBuilder`] is that implementation.
//!
//! It restores the R487-F6 behavior that W224 retired from the runtime, but as
//! an *injected* handler owned by the qed runner rather than a toolkit action —
//! so qed-gha on its own still never shells docker (see
//! `qed-gha/src/image_builder.rs`).
//!
//! Config comes from the W200 overlay files (`.yah/qed/gha-actions.toml` +
//! `~/.yah/qed/gha-actions.toml`, machine wins) — the same
//! `[overrides."<slug>"] config.registry_route / config.registry_auth` schema
//! F6 used. The overlay lets a dev retarget the workflow's hard-coded
//! `ghcr.io/yah-ai/<img>` push to a registry their local token can write
//! (e.g. `docker.io/yahdev`); the Dockerfiles and `release.yml` stay 100%
//! ghcr.io and the rewrite happens here at runtime. Image digests are
//! content-addressed, so the bytes verified locally are identical to CI's.
//!
//! @yah:relay(R590, "R594 fleet followups: run gha image jobs on the arch-matched build-worker fleet")
//! @yah:at(2026-07-01T08:04:26Z)
//! @yah:assignee(agent:claude)
//! @arch:see(.yah/docs/working/W235-remote-qed.md)
//! @yah:depends_on(R555)
//! @yah:depends_on(R572)
//!
//! R605-F2: [`QedImageBuilder::with_remote`] adds the fleet-dispatch half this
//! module's own doc used to call "phase C" — a slug opts in per-camp via the
//! W200 overlay (`config.remote = true` under `.yah/qed/gha-actions.toml` or
//! the per-machine override), and `do_build_push` then routes through the
//! SAME [`velveteen::ForgeCommand::BuildImage`] + [`RemoteForgeDriver`] +
//! [`BuildContextPublisher`] substrate the native `build-image` step kind
//! (`runner::execute_step_build_image_remote`) already uses successfully —
//! not a second builder. Left unset, behavior is byte-identical to before:
//! local `docker buildx build` on the qed host.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use indexmap::IndexMap;
use task_runs::Initiator;
use velveteen::{ForgeCommand, ForgeSpec, ForgeStatus, MeshAccess, TaskLocation, TaskPlacement, TaskRuntime};
use velveteen_exec::RemoteForgeDriver;
use workload_spec::TierTag;
use yah_qed_gha::{ImageBuildCall, ImageBuilder, StepConclusion, ToolkitOutcome, Value};

use crate::build_context::BuildContextPublisher;

/// Injected image builder for the docker push family. Holds the per-slug
/// overlay config (registry route + auth) and the resolved secrets context.
///
/// Local builds shell `docker buildx` synchronously (this runs inside the
/// runner's `spawn_blocking` gha-workflow task, so a sync child process is the
/// natural fit and mirrors the toolkit-action contract, which is buffered).
pub struct QedImageBuilder {
    /// Per-slug `config` blob from the overlay, keyed by `uses:` slug
    /// (`docker/login-action`, `docker/build-push-action`). Empty when no
    /// overlay is present — then tags push to the registry the workflow names.
    configs: IndexMap<String, Value>,
    /// Pre-resolved `secrets.*` context (same `Value` the executor evaluates
    /// `${{ secrets.X }}` against) — used to resolve `registry_auth`'s
    /// `password_secret` for a redirected push target.
    secrets: Value,
    /// R605-F2 fleet-dispatch handles, wired via [`Self::with_remote`]. All
    /// three are `None` (the default from [`Self::new`]) until a caller opts
    /// in; `do_build_push` falls back to local `docker buildx` whenever any
    /// is missing, even if a slug's overlay `config.remote = true` — a
    /// dangling opt-in with nothing wired to serve it fails loudly instead of
    /// silently building local, matching [`crate::build_context::NoBuildContextPublisher`]'s
    /// philosophy.
    tokio_handle: Option<tokio::runtime::Handle>,
    remote_driver: Option<Arc<RemoteForgeDriver>>,
    build_context_publisher: Option<Arc<dyn BuildContextPublisher>>,
}

impl QedImageBuilder {
    /// Build from the camp workspace: loads the W200 overlay(s) and captures the
    /// resolved secrets context. Local-only until [`Self::with_remote`] is
    /// also applied.
    pub fn new(workspace: &Path, secrets: Value) -> Self {
        let configs = load_overlay_configs(workspace);
        Self {
            configs,
            secrets,
            tokio_handle: None,
            remote_driver: None,
            build_context_publisher: None,
        }
    }

    /// Wire the fleet-dispatch substrate (R605-F2). `tokio_handle` must be
    /// captured with [`tokio::runtime::Handle::current`] *before* crossing
    /// into the `spawn_blocking` closure the qed-gha runtime executes in —
    /// `do_build_push` is a synchronous [`ImageBuilder::handle`] call, so it
    /// bridges into the async `driver.start(..).await` / `publisher.publish(..).await`
    /// calls via `tokio_handle.block_on(..)`, the standard sync-from-blocking-pool
    /// pattern. `remote_driver` / `build_context_publisher` are typically the
    /// same `Option<Arc<_>>` fields a [`crate::runner::PipelineRunner`] already
    /// carries for the native build-image path — pass them through unchanged
    /// rather than standing up a second copy.
    pub fn with_remote(
        mut self,
        tokio_handle: tokio::runtime::Handle,
        remote_driver: Option<Arc<RemoteForgeDriver>>,
        build_context_publisher: Option<Arc<dyn BuildContextPublisher>>,
    ) -> Self {
        self.tokio_handle = Some(tokio_handle);
        self.remote_driver = remote_driver;
        self.build_context_publisher = build_context_publisher;
        self
    }

    fn config_for(&self, slug: &str) -> &Value {
        self.configs.get(slug).unwrap_or(&NULL_CONFIG_SENTINEL)
    }
}

/// Which fleet tier/arch a `config.remote = true` slug wants (R605-F2). Parsed
/// out of the same per-slug overlay `config` blob `apply_registry_route` and
/// friends already read — `registry_route` says where the bytes land,
/// `remote` + `tier` + `arch` say where the build itself runs.
struct RemoteBuildTarget {
    /// `TaskLocation::RemoteAny.tier` — defaults to `"infra"`, the tier
    /// `execute_step_build_image_remote` already routes catalog builds to.
    tier: String,
    /// Feeds [`crate::platform::build_worker_mesh_tags`] — defaults to
    /// `"x86_64"`, the only arch the current build-worker pool
    /// (us-west-002/003, `.yah/infra/machines/`) actually carries.
    arch: String,
}

/// `config.remote = true` under a slug's overlay entry opts that slug into
/// fleet dispatch. `false`/absent (the default) keeps today's local-only
/// behavior — a build-worker's presence and, more to the point, its registry
/// push credentials are not guaranteed on every host running `yah qed run`,
/// so this is an explicit per-camp/per-machine choice, not an autodetect.
fn remote_build_requested(config: &Value) -> Option<RemoteBuildTarget> {
    let Value::Object(cfg) = config else {
        return None;
    };
    if !cfg.get("remote").map(|v| v.is_truthy()).unwrap_or(false) {
        return None;
    }
    let tier = cfg
        .get("tier")
        .map(|v| v.as_str_lossy())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "infra".to_string());
    let arch = cfg
        .get("arch")
        .map(|v| v.as_str_lossy())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "x86_64".to_string());
    Some(RemoteBuildTarget { tier, arch })
}

// A shared empty config for slugs with no overlay entry. `Value` isn't `const`-
// constructible (it owns an IndexMap), so use a thread-safe lazy.
use std::sync::LazyLock;
static NULL_CONFIG_SENTINEL: LazyLock<Value> = LazyLock::new(Value::object);

impl ImageBuilder for QedImageBuilder {
    fn handle(&self, call: &ImageBuildCall<'_>) -> Result<ToolkitOutcome, String> {
        match call.slug {
            "docker/login-action" => self.do_login(call),
            "docker/build-push-action" => self.do_build_push(call),
            other => Err(format!("QedImageBuilder: unhandled slug `{other}`")),
        }
    }
}

impl QedImageBuilder {
    /// `docker/login-action` — apply the registry redirect, then shell
    /// `docker login`. An empty password (the `${{ secrets.GITHUB_TOKEN }}`
    /// case QED can't resolve) is a skip-with-success so the host's existing
    /// docker creds carry the later push and any real auth failure surfaces at
    /// the push site with the registry's own message.
    fn do_login(&self, call: &ImageBuildCall<'_>) -> Result<ToolkitOutcome, String> {
        let config = self.config_for(call.slug);
        let raw_registry = with_string(call.with, "registry").unwrap_or_default();
        let registry = apply_registry_route(&raw_registry, config);
        let (username, password) = resolve_login_auth(call.with, config, &self.secrets, &registry);

        if password.is_empty() {
            return Ok(success(format!(
                "docker/login-action: skipped (empty password — host's existing docker creds for `{registry}` will be used)"
            )));
        }

        let mut cmd = Command::new("docker");
        cmd.arg("login").arg(&registry);
        if !username.is_empty() {
            cmd.arg("-u").arg(&username);
        }
        cmd.arg("--password-stdin");
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("docker/login-action: spawn: {e}"))?;
        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write;
            stdin
                .write_all(password.as_bytes())
                .map_err(|e| format!("docker/login-action: write stdin: {e}"))?;
        }
        let out = child
            .wait_with_output()
            .map_err(|e| format!("docker/login-action: wait: {e}"))?;
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        let conclusion = if out.status.success() {
            StepConclusion::Success
        } else {
            StepConclusion::Failure
        };
        Ok(ToolkitOutcome {
            outputs: IndexMap::new(),
            log: format!("docker/login-action: target=`{registry}` user=`{username}`\n{stdout}\n{stderr}"),
            conclusion,
        })
    }

    /// `docker/build-push-action` — apply the registry redirect to each tag,
    /// then shell `docker buildx build` honoring `with.{push,load,platforms,
    /// file,provenance,sbom,build-args,context}`. Captures `digest` + `imageid`
    /// via `--metadata-file` so downstream `steps.<id>.outputs.digest` (cosign
    /// sign + the per-binary `DIGEST` env blocks in `release.yml`) resolve.
    fn do_build_push(&self, call: &ImageBuildCall<'_>) -> Result<ToolkitOutcome, String> {
        let config = self.config_for(call.slug);
        let with = call.with;
        let context = with_string(with, "context").unwrap_or_else(|| ".".into());
        let file = with_string(with, "file");
        let push = with_bool(with, "push");
        let load = with_bool(with, "load");
        let provenance = with_string(with, "provenance");
        let sbom = with_string(with, "sbom");
        let platforms = with_string(with, "platforms");
        let tags: Vec<String> = with_string(with, "tags")
            .unwrap_or_default()
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|t| apply_registry_route(t, config))
            .collect();
        if tags.is_empty() && push {
            return Err("docker/build-push-action: push=true but no `tags` provided".into());
        }
        let build_args = collect_build_args(with, config);

        if let Some(target) = remote_build_requested(config) {
            return self.do_build_push_remote(
                call, &context, file.as_deref(), push, load, &platforms, &tags, &build_args, target,
            );
        }

        let metadata_dir =
            tempfile::tempdir().map_err(|e| format!("docker/build-push-action: tempdir: {e}"))?;
        let metadata_path = metadata_dir.path().join("metadata.json");

        let mut cmd = Command::new("docker");
        cmd.arg("buildx").arg("build");
        if push {
            cmd.arg("--push");
        }
        if load {
            cmd.arg("--load");
        }
        if let Some(p) = &platforms {
            cmd.arg("--platform").arg(p);
        }
        if let Some(f) = &file {
            cmd.arg("-f").arg(f);
        }
        if let Some(p) = &provenance {
            cmd.arg("--provenance").arg(p);
        }
        if let Some(s) = &sbom {
            cmd.arg("--sbom").arg(s);
        }
        for (k, v) in &build_args {
            cmd.arg("--build-arg").arg(format!("{k}={v}"));
        }
        for t in &tags {
            cmd.arg("-t").arg(t);
        }
        cmd.arg("--metadata-file").arg(&metadata_path);
        cmd.arg(call.workspace.join(&context));
        // Resolve relative paths (notably `-f <file>`) like a real runner does:
        // cwd == the checkout root. Without this a relative `file:` such as
        // `oss/qed/.../Dockerfile` fails with `lstat oss: no such file`.
        cmd.current_dir(call.workspace);
        for (k, v) in call.env {
            cmd.env(k, v);
        }

        let out = cmd
            .output()
            .map_err(|e| format!("docker/build-push-action: spawn: {e}"))?;
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        let conclusion = if out.status.success() {
            StepConclusion::Success
        } else {
            StepConclusion::Failure
        };

        let mut outputs = IndexMap::new();
        if conclusion == StepConclusion::Success {
            if let Ok(meta_json) = std::fs::read_to_string(&metadata_path) {
                if let Some((digest, imageid)) = parse_buildx_metadata(&meta_json) {
                    if let Some(d) = digest {
                        outputs.insert("digest".into(), Value::String(d));
                    }
                    if let Some(i) = imageid {
                        outputs.insert("imageid".into(), Value::String(i));
                    }
                }
                outputs.insert("metadata".into(), Value::String(meta_json));
            }
        }

        Ok(ToolkitOutcome {
            outputs,
            log: format!(
                "docker/build-push-action: tags=[{}] push={push} platforms={}\n{stdout}\n{stderr}",
                tags.join(", "),
                platforms.unwrap_or_else(|| "(host)".into()),
            ),
            conclusion,
        })
    }

    /// `docker/build-push-action`, fleet leg (R605-F2): pack the context,
    /// publish it, and dispatch a `ForgeCommand::BuildImage` to an
    /// arch-matched build-worker instead of shelling `docker buildx` on this
    /// host — the same request shape `runner::execute_step_build_image_remote`
    /// sends for a native `build-image` pipeline step.
    ///
    /// `push`/`load` and `platforms` (buildkit's `--opt platform=<csv>`, which
    /// — like a real GHA `ubuntu-latest` runner's qemu-backed buildx — builds
    /// every requested arch in one dispatch to one worker) pass straight
    /// through, so a multi-arch `linux/amd64,linux/arm64` build-push-action
    /// step needs no change on the workflow side to go remote.
    ///
    /// Does NOT resolve `outputs.digest`/`outputs.imageid`: unlike local
    /// `docker buildx --metadata-file`, nothing today streams a remote
    /// workload's files back to the qed daemon (R555-F6's "logs stream to
    /// QED/task pane" is still open, gated on R729's server-side log
    /// streaming) — so those two evaluate empty for a remote build, same as
    /// any other unset GHA step output. A cosign-sign step keyed on the
    /// digest needs that wired first; this ticket's own verify criterion
    /// (build + push lands in the registry) does not depend on it.
    #[allow(clippy::too_many_arguments)]
    fn do_build_push_remote(
        &self,
        call: &ImageBuildCall<'_>,
        context_rel: &str,
        file: Option<&str>,
        push: bool,
        load: bool,
        platforms_raw: &Option<String>,
        tags: &[String],
        build_args: &IndexMap<String, String>,
        target: RemoteBuildTarget,
    ) -> Result<ToolkitOutcome, String> {
        let (Some(tokio_handle), Some(driver), Some(publisher)) = (
            self.tokio_handle.clone(),
            self.remote_driver.clone(),
            self.build_context_publisher.clone(),
        ) else {
            return Err(format!(
                "docker/build-push-action: overlay sets config.remote = true for `{}` but this \
                 runner has no fleet dispatch wired (RemoteForgeDriver / BuildContextPublisher) — \
                 the `yah` CLI wires both when the camp has fleet config; a bare qed-runner \
                 embedding needs QedImageBuilder::with_remote",
                call.slug
            ));
        };

        let context_dir = call.workspace.join(context_rel);
        let dockerfile_path = match file {
            Some(f) => call.workspace.join(f),
            None => context_dir.join("Dockerfile"),
        };
        let (dockerfile_basename, extra) = match dockerfile_path.strip_prefix(&context_dir) {
            // The common case (release.yml's three image jobs): the Dockerfile
            // already lives inside the context, so it travels in the packed
            // tar for free — no extra entry needed.
            Ok(rel) => (rel.to_string_lossy().to_string(), Vec::new()),
            // A Dockerfile outside the context (the catalog build-image path's
            // shape, `.yah/cache/buildkit/<name>.Dockerfile`) has to be
            // injected as an extra tar entry at the root, same as
            // `runner::publish_build_context`.
            Err(_) => {
                let name = dockerfile_path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "Dockerfile".to_string());
                let bytes = std::fs::read(&dockerfile_path).map_err(|e| {
                    format!(
                        "docker/build-push-action: reading {}: {e}",
                        dockerfile_path.display()
                    )
                })?;
                (name.clone(), vec![(name, bytes)])
            }
        };

        let tarball = crate::build_context::pack_context(&context_dir, &extra).map_err(|e| e.to_string())?;
        let context_kib = tarball.len() / 1024;

        let platforms: Vec<String> = platforms_raw
            .as_deref()
            .map(|p| p.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
            .unwrap_or_default();
        let mesh_tags = crate::platform::build_worker_mesh_tags(&target.arch, "linux");
        let tier_for_log = target.tier.clone();
        let arch_for_log = target.arch.clone();
        let forge_key = crate::runner::tag_to_filename(&format!(
            "gha-{}-{}",
            tags.first().map(String::as_str).unwrap_or(call.slug),
            uuid::Uuid::new_v4(),
        ));
        let spec_build_args: Vec<(String, String)> =
            build_args.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let tags_owned = tags.to_vec();
        let label = format!("gha:{}", call.slug);
        let forge_key_for_dispatch = forge_key.clone();

        let outcome: Result<(), String> = tokio_handle.block_on(async move {
            let context_url = publisher
                .publish(&forge_key_for_dispatch, tarball)
                .await
                .map_err(|e| e.to_string())?;
            let spec = ForgeSpec {
                command: ForgeCommand::BuildImage {
                    dockerfile: PathBuf::from(&dockerfile_basename),
                    context: context_dir,
                    context_url: Some(context_url),
                    tags: tags_owned,
                    platforms,
                    build_args: spec_build_args,
                    push,
                    load,
                },
                where_: TaskPlacement::new(
                    TaskLocation::RemoteAny {
                        tier: TierTag(target.tier),
                        mesh_tags,
                    },
                    TaskRuntime::Container,
                ),
                timeout: None,
                label: Some(label),
                initiator: Initiator::Human { camp: "qed".into() },
                mesh_access: MeshAccess::None,
                cache_key: None,
            };

            let result = match driver.start(spec).await {
                Err(e) => Err(e.to_string()),
                Ok(handle) => match handle.wait().await {
                    ForgeStatus::Done { exit_code: 0, .. } => Ok(()),
                    ForgeStatus::Done { exit_code, .. } => {
                        Err(format!("buildkit exited with code {exit_code}"))
                    }
                    ForgeStatus::TimedOut { .. } => Err("remote build-image timed out".to_string()),
                    ForgeStatus::Killed { signal, .. } => {
                        Err(format!("buildkit killed by signal {signal}"))
                    }
                    ForgeStatus::Lost { reason } => Err(format!("buildkit lost: {reason}")),
                    ForgeStatus::Pending | ForgeStatus::Running => {
                        unreachable!("ForgeRunHandle::wait returns a terminal status")
                    }
                },
            };
            // Drop the uploaded context on BOTH legs, same reasoning as
            // runner::execute_step_build_image_remote: a failed build is
            // exactly when an operator re-runs, and every re-run uploads a
            // fresh key, so skipping cleanup on failure just accumulates
            // copies nobody will ever look at again.
            publisher.discard(&forge_key_for_dispatch).await;
            result
        });

        let conclusion = if outcome.is_ok() {
            StepConclusion::Success
        } else {
            StepConclusion::Failure
        };
        let detail = outcome.err().unwrap_or_else(|| "ok".to_string());
        Ok(ToolkitOutcome {
            outputs: IndexMap::new(),
            log: format!(
                "docker/build-push-action (remote build-worker tier={tier_for_log} arch={arch_for_log}): \
                 tags=[{}] push={push} platforms={} context={context_kib}KiB forge_key={forge_key}\n{detail}",
                tags.join(", "),
                platforms_raw.as_deref().unwrap_or("(worker default)"),
            ),
            conclusion,
        })
    }
}

// ─── overlay loading ────────────────────────────────────────────────────────

/// Load the W200 overlay files and return the per-slug `config` blobs (route +
/// auth). The per-machine overlay (`~/.yah/qed/gha-actions.toml`) is merged on
/// top of the per-camp one, so a dev's private push target wins. Missing files
/// are silently skipped.
pub fn load_overlay_configs(workspace: &Path) -> IndexMap<String, Value> {
    let mut configs: IndexMap<String, Value> = IndexMap::new();
    for path in default_overlay_paths(workspace) {
        let Ok(src) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(parsed) = toml::from_str::<RawOverlay>(&src) else {
            continue;
        };
        for (slug, entry) in parsed.overrides.unwrap_or_default() {
            if let Some(cfg) = entry.config {
                configs.insert(slug, toml_to_value(&cfg));
            }
        }
    }
    configs
}

fn default_overlay_paths(workspace: &Path) -> Vec<PathBuf> {
    let mut out = vec![workspace.join(".yah/qed/gha-actions.toml")];
    if let Some(home) = std::env::var_os("HOME") {
        out.push(PathBuf::from(home).join(".yah/qed/gha-actions.toml"));
    }
    out
}

#[derive(serde::Deserialize)]
struct RawOverlay {
    #[serde(default)]
    overrides: Option<IndexMap<String, RawOverride>>,
}

#[derive(serde::Deserialize)]
struct RawOverride {
    #[serde(default)]
    config: Option<toml::Value>,
}

fn toml_to_value(v: &toml::Value) -> Value {
    match v {
        toml::Value::String(s) => Value::String(s.clone()),
        toml::Value::Integer(n) => Value::Number(*n as f64),
        toml::Value::Float(f) => Value::Number(*f),
        toml::Value::Boolean(b) => Value::Bool(*b),
        toml::Value::Datetime(d) => Value::String(d.to_string()),
        toml::Value::Array(a) => Value::Array(a.iter().map(toml_to_value).collect()),
        toml::Value::Table(t) => {
            let mut out = IndexMap::new();
            for (k, v) in t {
                out.insert(k.clone(), toml_to_value(v));
            }
            Value::Object(out)
        }
    }
}

// ─── pure helpers (ported from R487-F6) ─────────────────────────────────────

/// Core `config.registry_route` rewrite. Route keys may be a bare host
/// (`ghcr.io`) or a host+namespace prefix (`ghcr.io/yah-ai`). The longest key
/// that equals `raw`, or is a prefix of `raw` ending on a `/` boundary, wins;
/// its value replaces exactly that prefix, the remainder (repo path + tag /
/// digest) preserved. Unmatched input falls through unchanged.
fn apply_registry_route(raw: &str, config: &Value) -> String {
    let Value::Object(cfg) = config else {
        return raw.to_string();
    };
    let Some(Value::Object(routes)) = cfg.get("registry_route") else {
        return raw.to_string();
    };
    let mut best: Option<(&str, &str)> = None;
    for (key, val) in routes {
        let Value::String(target) = val else { continue };
        let matches = raw == key
            || raw
                .strip_prefix(key.as_str())
                .is_some_and(|rest| rest.starts_with('/'));
        if matches && best.is_none_or(|(k, _)| key.len() > k.len()) {
            best = Some((key.as_str(), target.as_str()));
        }
    }
    match best {
        Some((key, target)) => format!("{target}{}", &raw[key.len()..]),
        None => raw.to_string(),
    }
}

/// Resolve `(username, password)` for `docker login` against the (redirected)
/// `registry`. When `config.registry_auth.<registry>` exists, the redirected
/// target authenticates with its own PAT (`username` verbatim, `password` from
/// the named `secrets.*` entry) instead of the GHCR-oriented
/// `${{ github.actor }}` / `${{ secrets.GITHUB_TOKEN }}` the workflow hard-codes.
/// Falls back to the workflow `with:` values when no entry matches.
fn resolve_login_auth(
    with: &IndexMap<String, Value>,
    config: &Value,
    secrets: &Value,
    registry: &str,
) -> (String, String) {
    if let Value::Object(cfg) = config {
        if let Some(Value::Object(auth)) = cfg.get("registry_auth") {
            if let Some(Value::Object(entry)) = auth.get(registry) {
                let username = entry
                    .get("username")
                    .map(|v| v.as_str_lossy())
                    .unwrap_or_default();
                let password = entry
                    .get("password_secret")
                    .map(|v| v.as_str_lossy())
                    .and_then(|name| secret_value(secrets, &name))
                    .unwrap_or_default();
                return (username, password);
            }
        }
    }
    (
        with_string(with, "username").unwrap_or_default(),
        with_string(with, "password").unwrap_or_default(),
    )
}

fn secret_value(secrets: &Value, name: &str) -> Option<String> {
    match secrets {
        Value::Object(m) => m.get(name).map(|v| v.as_str_lossy()),
        _ => None,
    }
}

/// Pluck `containerimage.digest` + `containerimage.config.digest` out of
/// `docker buildx --metadata-file` output. Shape varies between single- and
/// multi-platform builds; both are tolerated.
fn parse_buildx_metadata(json: &str) -> Option<(Option<String>, Option<String>)> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let digest = v
        .get("containerimage.digest")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());
    let imageid = v
        .get("containerimage.config.digest")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());
    Some((digest, imageid))
}

/// Parse `with.build-args` (one `KEY=VALUE` per line), routing each VALUE
/// through [`apply_registry_route`].
///
/// Routing the values is not cosmetic — it is what makes a *chained* image
/// build work off ghcr. `release.yml` builds `yah-rust-bun` `FROM ${RUST_BASE}`
/// with `RUST_BASE=ghcr.io/yah-ai/yah-rust:smoke-<sha>`, the tag the previous
/// job just pushed. Under a redirect those bytes went to the routed registry,
/// so an unrouted build-arg sends the `FROM` back to ghcr and the build dies on
/// a tag that was never pushed there. Rewriting only the `tags` gets you a
/// registry that receives images but can never be built *from*.
///
/// Non-image build args are untouched by construction: [`apply_registry_route`]
/// only substitutes on a registry-host key match at a `/` boundary, so
/// `FEATURES=deploy` and friends fall through unchanged.
fn collect_build_args(with: &IndexMap<String, Value>, config: &Value) -> IndexMap<String, String> {
    let mut out = IndexMap::new();
    let raw = with_string(with, "build-args").unwrap_or_default();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            out.insert(k.trim().to_string(), apply_registry_route(v, config));
        }
    }
    out
}

fn with_string(with: &IndexMap<String, Value>, key: &str) -> Option<String> {
    with.get(key).map(|v| v.as_str_lossy())
}

fn with_bool(with: &IndexMap<String, Value>, key: &str) -> bool {
    with.get(key).map(|v| v.is_truthy()).unwrap_or(false)
}

fn success(log: String) -> ToolkitOutcome {
    ToolkitOutcome {
        outputs: IndexMap::new(),
        log,
        conclusion: StepConclusion::Success,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(pairs: &[(&str, Value)]) -> Value {
        let mut m = IndexMap::new();
        for (k, v) in pairs {
            m.insert((*k).into(), v.clone());
        }
        Value::Object(m)
    }

    #[test]
    fn route_rewrites_host_plus_namespace_longest_prefix() {
        let config = obj(&[(
            "registry_route",
            obj(&[
                ("ghcr.io/yah-ai", Value::String("docker.io/yahdev".into())),
                ("ghcr.io", Value::String("docker.io".into())),
            ]),
        )]);
        // Full image ref → host+namespace key wins.
        assert_eq!(
            apply_registry_route("ghcr.io/yah-ai/yah-base:latest", &config),
            "docker.io/yahdev/yah-base:latest"
        );
        // Bare host (login input) → host-only key.
        assert_eq!(apply_registry_route("ghcr.io", &config), "docker.io");
        // Unmatched → unchanged.
        assert_eq!(
            apply_registry_route("quay.io/x/y:z", &config),
            "quay.io/x/y:z"
        );
    }

    #[test]
    fn route_falls_through_without_config() {
        assert_eq!(
            apply_registry_route("ghcr.io/yah-ai/x:1", &Value::object()),
            "ghcr.io/yah-ai/x:1"
        );
    }

    #[test]
    fn login_auth_prefers_registry_auth_entry_over_with() {
        let config = obj(&[(
            "registry_auth",
            obj(&[(
                "docker.io",
                obj(&[
                    ("username", Value::String("yahdev".into())),
                    ("password_secret", Value::String("DOCKERHUB_TOKEN".into())),
                ]),
            )]),
        )]);
        let secrets = obj(&[("DOCKERHUB_TOKEN", Value::String("pat-xyz".into()))]);
        let with = obj(&[
            ("username", Value::String("github-actor".into())),
            ("password", Value::String("".into())),
        ]);
        let Value::Object(with) = with else {
            unreachable!()
        };
        let (u, p) = resolve_login_auth(&with, &config, &secrets, "docker.io");
        assert_eq!(u, "yahdev");
        assert_eq!(p, "pat-xyz");
    }

    #[test]
    fn build_arg_base_image_follows_the_route() {
        // The v0.8.20 `yah-rust-bun` failure: tags were routed to docker.io but
        // `RUST_BASE` still named ghcr, so `FROM ${RUST_BASE}` 404'd on a tag
        // that had only ever been pushed to the routed registry.
        let config = obj(&[(
            "registry_route",
            obj(&[("ghcr.io/yah-ai", Value::String("docker.io/yahdev".into()))]),
        )]);
        let with = obj(&[(
            "build-args",
            Value::String("RUST_BASE=ghcr.io/yah-ai/yah-rust:smoke-abc\nFEATURES=deploy\n".into()),
        )]);
        let Value::Object(with) = with else {
            unreachable!()
        };
        let args = collect_build_args(&with, &config);
        assert_eq!(
            args.get("RUST_BASE").map(String::as_str),
            Some("docker.io/yahdev/yah-rust:smoke-abc")
        );
        // A build arg that is not an image ref cannot match a registry key.
        assert_eq!(args.get("FEATURES").map(String::as_str), Some("deploy"));
    }

    #[test]
    fn build_args_are_untouched_without_a_route() {
        let with = obj(&[(
            "build-args",
            Value::String("RUST_BASE=ghcr.io/yah-ai/yah-rust:smoke-abc".into()),
        )]);
        let Value::Object(with) = with else {
            unreachable!()
        };
        let args = collect_build_args(&with, &Value::object());
        assert_eq!(
            args.get("RUST_BASE").map(String::as_str),
            Some("ghcr.io/yah-ai/yah-rust:smoke-abc")
        );
    }

    #[test]
    fn remote_not_requested_by_default() {
        assert!(remote_build_requested(&Value::object()).is_none());
        let cfg = obj(&[("remote", Value::Bool(false))]);
        assert!(remote_build_requested(&cfg).is_none());
    }

    #[test]
    fn remote_config_parses_defaults_and_overrides() {
        let cfg = obj(&[("remote", Value::Bool(true))]);
        let t = remote_build_requested(&cfg).expect("remote requested");
        assert_eq!(t.tier, "infra");
        assert_eq!(t.arch, "x86_64");

        let cfg2 = obj(&[
            ("remote", Value::Bool(true)),
            ("tier", Value::String("build".into())),
            ("arch", Value::String("aarch64".into())),
        ]);
        let t2 = remote_build_requested(&cfg2).expect("remote requested");
        assert_eq!(t2.tier, "build");
        assert_eq!(t2.arch, "aarch64");
    }

    // R605-F2: a slug opted into `config.remote = true` with nothing wired to
    // serve it must fail loudly, not silently fall back to local docker
    // buildx — a dev who set the overlay flag and forgot `with_remote` should
    // see why nothing built remotely, not a build that quietly ran local.
    #[test]
    fn remote_opt_in_without_wiring_fails_loudly_not_silently_local() {
        let mut configs = IndexMap::new();
        configs.insert(
            "docker/build-push-action".to_string(),
            obj(&[("remote", Value::Bool(true))]),
        );
        let builder = QedImageBuilder {
            configs,
            secrets: Value::object(),
            tokio_handle: None,
            remote_driver: None,
            build_context_publisher: None,
        };
        let mut with = IndexMap::new();
        with.insert("tags".to_string(), Value::String("ghcr.io/yah-ai/x:1".into()));
        with.insert("context".to_string(), Value::String(".".into()));
        with.insert("push".to_string(), Value::Bool(false));
        let env = IndexMap::new();
        let call = ImageBuildCall {
            slug: "docker/build-push-action",
            with: &with,
            env: &env,
            workspace: Path::new("."),
        };
        let err = builder.handle(&call).expect_err("no fleet dispatch wired");
        assert!(
            err.contains("with_remote"),
            "expected the no-wiring guard message, got: {err}"
        );
    }

    #[test]
    fn parse_metadata_extracts_digest() {
        let json = r#"{"containerimage.digest":"sha256:abc","containerimage.config.digest":"sha256:cfg"}"#;
        assert_eq!(
            parse_buildx_metadata(json),
            Some((Some("sha256:abc".into()), Some("sha256:cfg".into())))
        );
    }
}
