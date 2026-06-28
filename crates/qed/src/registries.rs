//! Per-camp registry config (R381-T6).
//!
//! When a `build-image` step has `push = true`, qed needs to know whether the
//! tag's registry is writable from this camp. The config lives at
//! `<qed_dir>/registries.toml` (typically `.yah/qed/registries.toml`) and
//! lists named entries with a per-host writable flag — opt-in by default.
//!
//! Parse-time validation rejects a `push = true` step whose tag points at a
//! registry that isn't in the writable allowlist, so misconfigurations surface
//! when the pipeline loads rather than after the build has already burned
//! minutes pulling the BuildKit image.
//!
//! Shape:
//!
//! ```toml
//! # .yah/qed/registries.toml
//! [[registries]]
//! name     = "ghcr"
//! host     = "ghcr.io"
//! writable = true
//!
//! [[registries]]
//! name     = "local"
//! host     = "localhost:5000"
//! writable = true
//! ```
//!
//! Credentials are not stored here — BuildKit (local: `docker buildx`; remote:
//! buildctl in a BuildKit workload) resolves them via the engine's standard
//! credentials store (`~/.docker/config.json` or equivalent). Sigstore signing
//! for the bundled catalog images stays in the release pipeline (R381-T9).

use serde::Deserialize;
use std::fs;
use std::path::Path;
use thiserror::Error;

/// Per-camp registry allowlist controlling which hosts a `push = true`
/// build-image step is allowed to write to.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RegistryConfig {
    #[serde(default)]
    pub registries: Vec<RegistryEntry>,
}

/// One named registry entry from `registries.toml`.
///
/// `writable` is the opt-in: by default a host is read-only (catalog images
/// are *pulled* without any per-camp config). Setting `writable = true`
/// declares "this camp may push images to this host."
#[derive(Debug, Clone, Deserialize)]
pub struct RegistryEntry {
    pub name: String,
    pub host: String,
    #[serde(default)]
    pub writable: bool,
}

/// Errors surfaced while loading `registries.toml`. Missing file is **not**
/// an error — it yields a [`RegistryConfig::default()`].
#[derive(Debug, Error)]
pub enum RegistryConfigError {
    #[error("IO error reading {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("TOML parse error in {path}: {source}")]
    Parse {
        path: String,
        source: toml::de::Error,
    },
}

impl RegistryConfig {
    /// Load `<qed_dir>/registries.toml` if present; return an empty config
    /// otherwise. The empty config rejects every `push = true` build-image
    /// step at parse time, which is the right v1 default — operators opt in
    /// by writing the file.
    pub fn load(qed_dir: &Path) -> Result<Self, RegistryConfigError> {
        let path = qed_dir.join("registries.toml");
        if !path.exists() {
            return Ok(Self::default());
        }
        let src = fs::read_to_string(&path).map_err(|e| RegistryConfigError::Io {
            path: path.display().to_string(),
            source: e,
        })?;
        toml::from_str(&src).map_err(|e| RegistryConfigError::Parse {
            path: path.display().to_string(),
            source: e,
        })
    }

    /// Returns true when `host` is declared writable in this config.
    pub fn is_writable(&self, host: &str) -> bool {
        self.registries.iter().any(|r| r.writable && r.host == host)
    }
}

/// Extract the registry hostname from a docker tag.
///
/// Docker's tag-parsing rule (paraphrased): the first segment is a registry
/// host iff it contains `.` or `:`, or is exactly `localhost`. Otherwise it's
/// a Docker Hub repository segment and the implicit host is `docker.io`.
///
/// Examples:
/// - `ghcr.io/yah-ai/yah-rust:dev`     → `ghcr.io`
/// - `localhost:5000/yah-rust:dev`   → `localhost:5000`
/// - `localhost/yah-rust:dev`        → `localhost`
/// - `nginx`                         → `docker.io`
/// - `library/nginx:latest`          → `docker.io`
/// - `yah-rust:dev`                  → `docker.io`
pub fn extract_registry_host(tag: &str) -> &str {
    let first_segment = tag.split('/').next().unwrap_or(tag);
    // Strip any trailing :port-or-tag from the first segment when checking
    // for a host marker (a tag like `nginx:latest` has `:` in its first
    // segment but the segment is the repo, not a host).
    let has_dot = first_segment.contains('.');
    let is_localhost = first_segment == "localhost" || first_segment.starts_with("localhost:");
    let has_port_separator = first_segment.contains(':') && tag.contains('/');

    if has_dot || is_localhost || has_port_separator {
        first_segment
    } else {
        "docker.io"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn extract_registry_host_recognises_ghcr() {
        assert_eq!(
            extract_registry_host("ghcr.io/yah-ai/yah-rust:dev"),
            "ghcr.io"
        );
    }

    #[test]
    fn extract_registry_host_recognises_localhost_with_port() {
        assert_eq!(
            extract_registry_host("localhost:5000/yah-rust:dev"),
            "localhost:5000",
        );
    }

    #[test]
    fn extract_registry_host_recognises_bare_localhost() {
        assert_eq!(extract_registry_host("localhost/yah-rust:dev"), "localhost");
    }

    #[test]
    fn extract_registry_host_falls_back_to_docker_io_for_bare_name() {
        assert_eq!(extract_registry_host("nginx"), "docker.io");
        assert_eq!(extract_registry_host("nginx:latest"), "docker.io");
        assert_eq!(extract_registry_host("library/nginx:latest"), "docker.io");
        assert_eq!(extract_registry_host("yah-rust:dev"), "docker.io");
    }

    #[test]
    fn registry_config_load_missing_file_is_empty() {
        let dir = TempDir::new().unwrap();
        let cfg = RegistryConfig::load(dir.path()).unwrap();
        assert!(cfg.registries.is_empty());
        assert!(!cfg.is_writable("ghcr.io"));
    }

    #[test]
    fn registry_config_load_parses_writable_entries() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("registries.toml"),
            r#"
[[registries]]
name     = "ghcr"
host     = "ghcr.io"
writable = true

[[registries]]
name = "docker-hub"
host = "docker.io"
# writable omitted → defaults to false
"#,
        )
        .unwrap();
        let cfg = RegistryConfig::load(dir.path()).unwrap();
        assert_eq!(cfg.registries.len(), 2);
        assert!(cfg.is_writable("ghcr.io"));
        assert!(
            !cfg.is_writable("docker.io"),
            "writable defaults to false; docker.io entry must not be considered writable"
        );
        assert!(!cfg.is_writable("nowhere.example"));
    }

    #[test]
    fn registry_config_load_parses_bad_toml_as_error() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("registries.toml"), "not = valid toml [[").unwrap();
        let err = RegistryConfig::load(dir.path()).unwrap_err();
        match err {
            RegistryConfigError::Parse { .. } => {}
            other => panic!("expected Parse, got {other:?}"),
        }
    }
}
