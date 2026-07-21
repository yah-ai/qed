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

use std::path::{Path, PathBuf};
use std::process::Command;

use indexmap::IndexMap;
use yah_qed_gha::{ImageBuildCall, ImageBuilder, StepConclusion, ToolkitOutcome, Value};

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
}

impl QedImageBuilder {
    /// Build from the camp workspace: loads the W200 overlay(s) and captures the
    /// resolved secrets context.
    pub fn new(workspace: &Path, secrets: Value) -> Self {
        let configs = load_overlay_configs(workspace);
        Self { configs, secrets }
    }

    fn config_for(&self, slug: &str) -> &Value {
        self.configs.get(slug).unwrap_or(&NULL_CONFIG_SENTINEL)
    }
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
        let build_args = collect_build_args(with);

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

fn collect_build_args(with: &IndexMap<String, Value>) -> IndexMap<String, String> {
    let mut out = IndexMap::new();
    let raw = with_string(with, "build-args").unwrap_or_default();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            out.insert(k.trim().to_string(), v.to_string());
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
    fn parse_metadata_extracts_digest() {
        let json = r#"{"containerimage.digest":"sha256:abc","containerimage.config.digest":"sha256:cfg"}"#;
        assert_eq!(
            parse_buildx_metadata(json),
            Some((Some("sha256:abc".into()), Some("sha256:cfg".into())))
        );
    }
}
