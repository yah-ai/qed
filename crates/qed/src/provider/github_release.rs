//! GitHub Release publisher adapter (R509-F7).
//!
//! Creates (or updates) a GitHub Release for a tag and uploads the run's
//! produced artifacts as release assets. This is the **native** path to GitHub
//! Releases — now the only one, since W224/R533-T7 retired the GHA `gh-release`
//! workflow override — so a non-GHA pipeline can tag-and-upload directly. It's
//! also how the `oss/` crates.io mirror releases publish their binaries.
//!
//! Driven through the `gh` CLI rather than a hand-rolled REST client: `gh` is
//! ubiquitous in CI, already this workspace's GitHub surface, and handles auth,
//! retries, and the two-step asset upload (`POST /releases` then the
//! `uploads.github.com` multipart) — so the adapter adds **no new dependency**
//! (same subprocess pattern as `notarize`/`testflight`). The `gh` binary is
//! overridable; `GITHUB_TOKEN` is passed via the `GH_TOKEN` env var.
//!
//! A GitHub Release is a *ship*: [`ProviderReport::produced`] stays empty and
//! the release `html_url` is returned in [`ProviderReport::published`].
//!
//! ## Idempotency
//!
//! `dispatch` is find-or-create by tag: if a release already exists for the tag
//! it uploads the assets with `--clobber` (re-runs replace same-named assets);
//! otherwise it creates the release. Re-running a release is safe.
//!
//! ## Credentials
//!
//! - `GITHUB_TOKEN` — a repo-scoped PAT or installation token, passed to `gh`
//!   as `GH_TOKEN`. Never logged.
//!
//! ## Dry run
//!
//! `ctx.dry_run` confirms `GITHUB_TOKEN` resolves and the selected asset files
//! exist, then reports `would create release <owner/repo>@<tag> with N assets`
//! — spawning no `gh` and mutating nothing.

use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use crate::provider::{ProviderContext, ProviderReport, ReleaseProvider};
use crate::runner::RunnerError;
use crate::types::ProducedArtifact;

/// Credential slot: the GitHub token (passed to `gh` as `GH_TOKEN`).
const SLOT_TOKEN: &str = "GITHUB_TOKEN";

/// The `with = { … }` config block for a `github-release` outcome.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct GithubReleaseConfig {
    /// `owner/repo` slug the release belongs to. Required for a live publish.
    repo: String,
    /// Git tag to release. Defaults to `v<version>` when unset.
    tag: Option<String>,
    /// Target commitish (branch / SHA) for the tag if it doesn't exist yet.
    target_commitish: Option<String>,
    /// Create the release as a draft.
    draft: bool,
    /// Mark the release as a prerelease.
    prerelease: bool,
    /// Release title. Defaults to the tag.
    title: Option<String>,
    /// Inline release-notes body. Ignored when `notes_file` is set.
    notes: Option<String>,
    /// Path to a release-notes file (`--notes-file`). Wins over `notes`.
    notes_file: Option<String>,
    /// Binary-name / basename globs selecting which produced artifacts to
    /// attach. Empty → attach every produced artifact (a notes-only release is
    /// fine — zero assets is allowed).
    artifacts: Vec<String>,
}

/// `github-release` — native GitHub Release create + asset upload. See module
/// docs.
#[derive(Debug, Default)]
pub struct GithubReleaseProvider {
    /// `gh` binary; overridable for tests / non-default installs.
    gh_bin: Option<String>,
}

impl GithubReleaseProvider {
    /// Path to the `gh` binary (defaults to `gh` on `PATH`).
    fn gh(&self) -> &str {
        self.gh_bin.as_deref().unwrap_or("gh")
    }
}

#[async_trait]
impl ReleaseProvider for GithubReleaseProvider {
    fn name(&self) -> &str {
        "github-release"
    }

    fn required_slots(&self) -> Vec<&str> {
        vec![SLOT_TOKEN]
    }

    async fn dispatch(&self, ctx: &ProviderContext<'_>) -> Result<ProviderReport, RunnerError> {
        let cfg: GithubReleaseConfig = parse_config(ctx.config)?;
        let token = ctx.require_secret(SLOT_TOKEN)?;
        let tag = cfg
            .tag
            .clone()
            .unwrap_or_else(|| format!("v{}", ctx.version));

        let assets = select_assets(ctx.artifacts, &cfg.artifacts)?;
        // Every selected asset must exist on disk before we try to attach it.
        for a in &assets {
            if !Path::new(&a.path).exists() {
                return Err(RunnerError::Outcome(format!(
                    "github-release: asset {} does not exist",
                    a.path
                )));
            }
        }

        let repo = repo_label(&cfg);

        if ctx.dry_run {
            return Ok(ProviderReport::action(format!(
                "would create release {repo}@{tag} with {} asset(s)",
                assets.len(),
            )));
        }

        if cfg.repo.is_empty() {
            return Err(RunnerError::Outcome(
                "github-release: `with.repo` (owner/repo) is required for a live publish".into(),
            ));
        }

        let asset_paths: Vec<&str> = assets.iter().map(|a| a.path.as_str()).collect();
        let gh = self.gh();

        if release_exists(gh, &token, &cfg.repo, &tag).await? {
            if !asset_paths.is_empty() {
                upload_assets(gh, &token, &cfg.repo, &tag, &asset_paths).await?;
            }
        } else {
            create_release(gh, &token, &cfg, &tag, &asset_paths).await?;
        }

        let url = release_url(gh, &token, &cfg.repo, &tag).await?;
        Ok(ProviderReport {
            actions: vec![format!("published release {repo}@{tag} ({} assets)", assets.len())],
            produced: Vec::new(),
            published: vec![url],
        })
    }
}

/// Deserialize the opaque `with` blob into [`GithubReleaseConfig`].
fn parse_config(value: &serde_json::Value) -> Result<GithubReleaseConfig, RunnerError> {
    if value.is_null() {
        return Ok(GithubReleaseConfig::default());
    }
    serde_json::from_value(value.clone())
        .map_err(|e| RunnerError::Outcome(format!("github-release: invalid `with` config: {e}")))
}

/// Human label for the target repo, or a placeholder when unset (dry-run only).
fn repo_label(cfg: &GithubReleaseConfig) -> String {
    if cfg.repo.is_empty() {
        "<owner/repo>".to_string()
    } else {
        cfg.repo.clone()
    }
}

/// Select the assets to attach: the config globs over the produced artifacts,
/// or — when no globs are given — every produced artifact. Unlike the other
/// adapters, zero assets is allowed (a notes-only release), but a glob that
/// matches nothing is a config error.
fn select_assets(
    artifacts: &[ProducedArtifact],
    globs: &[String],
) -> Result<Vec<ProducedArtifact>, RunnerError> {
    if globs.is_empty() {
        return Ok(artifacts.to_vec());
    }
    let selected: Vec<ProducedArtifact> = artifacts
        .iter()
        .filter(|a| globs.iter().any(|g| artifact_matches(a, g)))
        .cloned()
        .collect();
    if selected.is_empty() {
        return Err(RunnerError::Outcome(format!(
            "github-release: no artifact matched config globs {globs:?} (of {} produced)",
            artifacts.len()
        )));
    }
    Ok(selected)
}

/// Whether a release already exists for `tag` (`gh release view` exits non-zero
/// when it doesn't).
async fn release_exists(
    gh: &str,
    token: &str,
    repo: &str,
    tag: &str,
) -> Result<bool, RunnerError> {
    let out = tokio::process::Command::new(gh)
        .args(["release", "view", tag, "--repo", repo])
        .env("GH_TOKEN", token)
        .output()
        .await
        .map_err(|e| RunnerError::Outcome(format!("github-release: spawning `{gh} release view`: {e}")))?;
    Ok(out.status.success())
}

/// Create the release with `gh release create`, attaching any assets inline.
async fn create_release(
    gh: &str,
    token: &str,
    cfg: &GithubReleaseConfig,
    tag: &str,
    assets: &[&str],
) -> Result<(), RunnerError> {
    let mut cmd = tokio::process::Command::new(gh);
    cmd.args(["release", "create", tag, "--repo", &cfg.repo]);
    cmd.arg("--title").arg(cfg.title.as_deref().unwrap_or(tag));
    if let Some(file) = &cfg.notes_file {
        cmd.arg("--notes-file").arg(file);
    } else {
        cmd.arg("--notes").arg(cfg.notes.as_deref().unwrap_or(""));
    }
    if let Some(target) = &cfg.target_commitish {
        cmd.arg("--target").arg(target);
    }
    if cfg.draft {
        cmd.arg("--draft");
    }
    if cfg.prerelease {
        cmd.arg("--prerelease");
    }
    for a in assets {
        cmd.arg(a);
    }
    let out = cmd
        .env("GH_TOKEN", token)
        .output()
        .await
        .map_err(|e| RunnerError::Outcome(format!("github-release: spawning `{gh} release create`: {e}")))?;
    if !out.status.success() {
        return Err(RunnerError::Outcome(format!(
            "github-release: `gh release create {tag}` failed (status {}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim(),
        )));
    }
    Ok(())
}

/// Upload assets to an existing release with `--clobber` (idempotent re-run).
async fn upload_assets(
    gh: &str,
    token: &str,
    repo: &str,
    tag: &str,
    assets: &[&str],
) -> Result<(), RunnerError> {
    let mut cmd = tokio::process::Command::new(gh);
    cmd.args(["release", "upload", tag, "--repo", repo, "--clobber"]);
    for a in assets {
        cmd.arg(a);
    }
    let out = cmd
        .env("GH_TOKEN", token)
        .output()
        .await
        .map_err(|e| RunnerError::Outcome(format!("github-release: spawning `{gh} release upload`: {e}")))?;
    if !out.status.success() {
        return Err(RunnerError::Outcome(format!(
            "github-release: `gh release upload {tag}` failed (status {}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim(),
        )));
    }
    Ok(())
}

/// Resolve the release `html_url` via `gh release view --json url`.
async fn release_url(gh: &str, token: &str, repo: &str, tag: &str) -> Result<String, RunnerError> {
    let out = tokio::process::Command::new(gh)
        .args(["release", "view", tag, "--repo", repo, "--json", "url", "-q", ".url"])
        .env("GH_TOKEN", token)
        .output()
        .await
        .map_err(|e| RunnerError::Outcome(format!("github-release: spawning `{gh} release view`: {e}")))?;
    if !out.status.success() {
        return Err(RunnerError::Outcome(format!(
            "github-release: resolving release URL for {tag} failed (status {}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim(),
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// A config glob matches an artifact when it globs the `binary` field or the
/// file basename.
fn artifact_matches(art: &ProducedArtifact, glob: &str) -> bool {
    glob_match(glob, &art.binary) || glob_match(glob, basename(&art.path))
}

/// File basename of a path (the part after the last `/`).
fn basename(path: &str) -> &str {
    Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(path)
}

/// Minimal glob matcher — `*` matches any run (including empty); every other
/// char is literal.
fn glob_match(pattern: &str, text: &str) -> bool {
    let (p, t): (Vec<char>, Vec<char>) = (pattern.chars().collect(), text.chars().collect());
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut star_t): (Option<usize>, usize) = (None, 0);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            star_t = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            star_t += 1;
            ti = star_t;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::MapSecrets;
    use std::collections::BTreeMap;

    fn fixture_asset(dir: &Path, name: &str) -> ProducedArtifact {
        let p = dir.join(name);
        std::fs::write(&p, b"binary").unwrap();
        ProducedArtifact {
            binary: name.split('.').next().unwrap_or(name).into(),
            path: p.to_str().unwrap().into(),
            triple: Some("linux-x86_64".into()),
        }
    }

    fn full_secrets() -> MapSecrets {
        let mut m = BTreeMap::new();
        m.insert(SLOT_TOKEN.into(), "ghp_xxx".into());
        MapSecrets(m)
    }

    fn ctx<'a>(
        secrets: &'a dyn crate::provider::SecretSource,
        work: &'a Path,
        cfg: &'a serde_json::Value,
        artifacts: &'a [ProducedArtifact],
        dry_run: bool,
    ) -> ProviderContext<'a> {
        ProviderContext {
            version: "1.2.3",
            artifacts,
            base_url: None,
            config: cfg,
            work_dir: work,
            secrets,
            dry_run,
        }
    }

    #[test]
    fn declares_github_token_slot() {
        let p = GithubReleaseProvider::default();
        assert_eq!(p.name(), "github-release");
        assert_eq!(p.required_slots(), vec![SLOT_TOKEN]);
    }

    #[tokio::test]
    async fn dry_run_reports_release_without_spawning_gh() {
        let work = tempfile::tempdir().unwrap();
        let a = fixture_asset(work.path(), "yah-linux.tar.gz");
        let b = fixture_asset(work.path(), "yah-macos.tar.gz");
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "repo": "yah-ai/yah" });
        let report = GithubReleaseProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &[a, b], true))
            .await
            .unwrap();
        assert_eq!(report.actions.len(), 1);
        assert!(report.actions[0].contains("would create release yah-ai/yah@v1.2.3"));
        assert!(report.actions[0].contains("with 2 asset(s)"));
        assert!(report.published.is_empty());
    }

    #[tokio::test]
    async fn tag_defaults_to_v_version_and_config_overrides() {
        let work = tempfile::tempdir().unwrap();
        let a = fixture_asset(work.path(), "x.tar.gz");
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "repo": "o/r", "tag": "release-7" });
        let report = GithubReleaseProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&a), true))
            .await
            .unwrap();
        assert!(report.actions[0].contains("o/r@release-7"));
    }

    #[tokio::test]
    async fn dry_run_allows_zero_assets_notes_only_release() {
        let work = tempfile::tempdir().unwrap();
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "repo": "o/r" });
        let report = GithubReleaseProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &[], true))
            .await
            .unwrap();
        assert!(report.actions[0].contains("with 0 asset(s)"));
    }

    #[tokio::test]
    async fn config_glob_selects_subset_of_assets() {
        let work = tempfile::tempdir().unwrap();
        let a = fixture_asset(work.path(), "yah-linux.tar.gz");
        let b = fixture_asset(work.path(), "desktop.dmg");
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "repo": "o/r", "artifacts": ["*.tar.gz"] });
        let report = GithubReleaseProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, &[a, b], true))
            .await
            .unwrap();
        assert!(report.actions[0].contains("with 1 asset(s)"));
    }

    #[tokio::test]
    async fn glob_with_no_match_is_an_error() {
        let work = tempfile::tempdir().unwrap();
        let a = fixture_asset(work.path(), "x.dmg");
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "repo": "o/r", "artifacts": ["*.exe"] });
        let err = GithubReleaseProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&a), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no artifact matched"));
    }

    #[tokio::test]
    async fn missing_token_is_typed_error() {
        let work = tempfile::tempdir().unwrap();
        let a = fixture_asset(work.path(), "x.tar.gz");
        let secrets = MapSecrets::default();
        let cfg = serde_json::json!({ "repo": "o/r" });
        let err = GithubReleaseProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&a), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains(SLOT_TOKEN));
    }

    #[tokio::test]
    async fn missing_asset_file_is_an_error() {
        let work = tempfile::tempdir().unwrap();
        // Artifact path points at a file that was never written.
        let ghost = ProducedArtifact {
            binary: "x".into(),
            path: work.path().join("missing.tar.gz").to_str().unwrap().into(),
            triple: None,
        };
        let secrets = full_secrets();
        let cfg = serde_json::json!({ "repo": "o/r" });
        let err = GithubReleaseProvider::default()
            .dispatch(&ctx(&secrets, work.path(), &cfg, std::slice::from_ref(&ghost), true))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("does not exist"));
    }

    #[test]
    fn glob_match_supports_star() {
        assert!(glob_match("*.tar.gz", "yah-linux.tar.gz"));
        assert!(glob_match("yah-*", "yah-linux.tar.gz"));
        assert!(!glob_match("*.exe", "x.dmg"));
    }
}
