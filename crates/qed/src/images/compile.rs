//! TOML → Dockerfile compiler.
//!
//! Takes a [`CatalogEntry`](super::CatalogEntry) (plus the surrounding
//! [`CatalogManifest`](super::CatalogManifest) for `extends` validation) and
//! emits the Dockerfile text that the build-image step kind (R381-T2) will
//! hand to `docker buildx` (T4) or BuildKit-in-containerd (T5).
//!
//! Two paths:
//!
//! - [`compile_entry`] — pure TOML layering. Emits a Dockerfile string from
//!   `base` / `extends` / `apt` / `pip` / `env`. The escape hatch is a
//!   sibling Dockerfile loaded by [`compile_with_dockerfile_dir`].
//!
//! - [`compile_with_dockerfile_dir`] — if `<dir>/Dockerfile` exists, return
//!   its contents verbatim (with a `FROM` line prepended when the user's
//!   Dockerfile has none and the TOML sets `extends`). Otherwise falls back
//!   to [`compile_entry`].

use std::collections::HashSet;
use std::path::Path;
use thiserror::Error;

use super::catalog::{CatalogEntry, CatalogManifest};

/// Max length of an `extends` chain before the compiler aborts.
/// W148 lists 5 as the default ceiling.
pub const MAX_EXTENDS_DEPTH: usize = 5;

#[derive(Error, Debug)]
pub enum CompileError {
    #[error("catalog entry `{name}` extends unknown image `{target}`")]
    ExtendsNotFound { name: String, target: String },
    #[error("catalog entry `{name}` has a cyclic extends chain: {}", chain.join(" → "))]
    ExtendsCycle { name: String, chain: Vec<String> },
    #[error("catalog entry `{name}` extends chain is deeper than the {max}-step limit")]
    ExtendsTooDeep { name: String, max: usize },
    #[error("catalog entry `{0}` has neither `base` nor `extends`")]
    NoBase(String),
    #[error("io error reading sibling Dockerfile at {path}: {source}")]
    DockerfileIo {
        path: String,
        source: std::io::Error,
    },
}

/// The published image reference for a catalog entry — what other Dockerfiles
/// `FROM` when they extend it. Defaults to `ghcr.io/yah-ai/yah-<name>:latest`;
/// release-time digest injection replaces `:latest` with `@sha256:...` (T8).
pub fn catalog_image_ref(entry_name: &str) -> String {
    format!("ghcr.io/yah-ai/{entry_name}:latest")
}

/// Generate a Dockerfile from a [`CatalogEntry`]'s layering shorthand.
///
/// - Entries with `base` start `FROM <base>` (the upstream image).
/// - Entries with `extends` start `FROM ghcr.io/yah-ai/yah-<target>:latest`
///   (the already-built catalog image).
/// - `apt` becomes a single `RUN apt-get install --no-install-recommends …`
///   layer (one cache layer, no per-package fragmentation).
/// - `pip` becomes a single `RUN pip install --no-cache-dir …` layer.
/// - `env` becomes one `ENV` line per pair (sorted for reproducibility).
///
/// Returns an error if the entry is orphan (no base, no extends), or if
/// the extends chain has a cycle / exceeds [`MAX_EXTENDS_DEPTH`].
pub fn compile_entry(
    entry: &CatalogEntry,
    catalog: &CatalogManifest,
) -> Result<String, CompileError> {
    validate_extends_chain(entry, catalog)?;
    Ok(emit_dockerfile(entry))
}

/// Compile in a per-camp directory context: if `<dir>/Dockerfile` exists,
/// return it verbatim. When the user's Dockerfile has no `FROM` line and the
/// catalog entry sets `extends`, the compiler prepends one — this lets a
/// camp drop a partial Dockerfile next to `image.toml` and still inherit
/// from the catalog. TOML `apt` / `pip` / `env` are ignored when a Dockerfile
/// is present (the user is overriding the shorthand).
///
/// Falls back to [`compile_entry`] when no sibling Dockerfile exists.
pub fn compile_with_dockerfile_dir(
    entry: &CatalogEntry,
    catalog: &CatalogManifest,
    dir: &Path,
) -> Result<String, CompileError> {
    let dockerfile_path = dir.join("Dockerfile");
    if !dockerfile_path.is_file() {
        return compile_entry(entry, catalog);
    }

    // Extends still must validate even when the user supplies a Dockerfile —
    // if they reference an unknown parent in TOML we want to fail fast.
    validate_extends_chain(entry, catalog)?;

    let contents =
        std::fs::read_to_string(&dockerfile_path).map_err(|e| CompileError::DockerfileIo {
            path: dockerfile_path.display().to_string(),
            source: e,
        })?;

    if has_from_line(&contents) {
        return Ok(contents);
    }

    // No FROM line in the user's Dockerfile. If the TOML sets extends or
    // base, prepend our resolved FROM line. Otherwise fall through to the
    // raw contents (the user is on their own — docker will fail at build
    // time, which is the same failure mode they'd see writing this by hand).
    if let Some(from_line) = from_line_for(entry) {
        Ok(format!("{from_line}\n\n{contents}"))
    } else {
        Ok(contents)
    }
}

// ─── Internals ────────────────────────────────────────────────────────────────

/// Walk the `extends` chain looking for cycles, depth violations, or dangling
/// references. Returns `Ok(())` if the chain is well-formed (including the
/// degenerate case where `entry.extends` is `None`).
fn validate_extends_chain(
    entry: &CatalogEntry,
    catalog: &CatalogManifest,
) -> Result<(), CompileError> {
    let mut chain = vec![entry.name.clone()];
    let mut seen: HashSet<String> = HashSet::new();
    seen.insert(entry.name.clone());
    let mut cursor: &CatalogEntry = entry;

    while let Some(parent_name) = cursor.extends.as_deref() {
        if !seen.insert(parent_name.to_string()) {
            chain.push(parent_name.to_string());
            return Err(CompileError::ExtendsCycle {
                name: entry.name.clone(),
                chain,
            });
        }
        chain.push(parent_name.to_string());
        if chain.len() > MAX_EXTENDS_DEPTH {
            return Err(CompileError::ExtendsTooDeep {
                name: entry.name.clone(),
                max: MAX_EXTENDS_DEPTH,
            });
        }
        cursor = catalog
            .get(parent_name)
            .ok_or_else(|| CompileError::ExtendsNotFound {
                name: entry.name.clone(),
                target: parent_name.to_string(),
            })?;
    }

    // Reached an entry with no `extends`. It must have a `base` — otherwise
    // the catalog has an orphan that the loader's validate() should have
    // rejected, but be defensive here too.
    if cursor.base.is_none() {
        return Err(CompileError::NoBase(cursor.name.clone()));
    }
    Ok(())
}

/// Resolve the `FROM` line for an entry's *own* Dockerfile (not the chain).
fn from_line_for(entry: &CatalogEntry) -> Option<String> {
    if let Some(parent) = &entry.extends {
        return Some(format!("FROM {}", catalog_image_ref(parent)));
    }
    entry.base.as_ref().map(|b| format!("FROM {b}"))
}

fn has_from_line(dockerfile: &str) -> bool {
    dockerfile.lines().any(|l| {
        let trimmed = l.trim_start();
        // Skip comments and the optional `# syntax=` directive.
        if trimmed.starts_with('#') || trimmed.is_empty() {
            return false;
        }
        trimmed
            .split_whitespace()
            .next()
            .map(|w| w.eq_ignore_ascii_case("FROM"))
            .unwrap_or(false)
    })
}

fn emit_dockerfile(entry: &CatalogEntry) -> String {
    let mut lines: Vec<String> = Vec::new();
    lines.push("# syntax=docker/dockerfile:1".to_string());
    lines.push(format!(
        "# Generated by qed::images::compile for catalog entry `{}`.",
        entry.name
    ));
    if let Some(from) = from_line_for(entry) {
        lines.push(from);
    }

    if !entry.apt.is_empty() {
        let mut pkgs = entry.apt.clone();
        pkgs.sort();
        lines.push(format!(
            "RUN apt-get update \\\n    && apt-get install -y --no-install-recommends \\\n        {} \\\n    && rm -rf /var/lib/apt/lists/*",
            pkgs.join(" \\\n        ")
        ));
    }

    if !entry.pip.is_empty() {
        let mut pkgs = entry.pip.clone();
        pkgs.sort();
        lines.push(format!(
            "RUN pip install --no-cache-dir \\\n        {}",
            pkgs.join(" \\\n        ")
        ));
    }

    if !entry.env.is_empty() {
        let mut pairs: Vec<(&String, &String)> = entry.env.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for (k, v) in pairs {
            lines.push(format!("ENV {k}={v}"));
        }
    }

    // Trailing newline matches what docker buildx expects from a Dockerfile.
    let mut out = lines.join("\n");
    out.push('\n');
    out
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use tempfile::tempdir;

    fn entry(name: &str, base: Option<&str>, extends: Option<&str>) -> CatalogEntry {
        CatalogEntry {
            name: name.into(),
            base: base.map(Into::into),
            extends: extends.map(Into::into),
            description: format!("{name} test fixture"),
            tools: Vec::new(),
            digests: HashMap::new(),
            apt: Vec::new(),
            pip: Vec::new(),
            env: HashMap::new(),
            produces: vec![crate::images::ProduceTarget::OciImage],
        }
    }

    fn bundled() -> CatalogManifest {
        CatalogManifest::bundled().unwrap()
    }

    #[test]
    fn pure_toml_layering_yah_rust_pg() {
        // The W148 worked example.
        let mut e = entry("yah-rust-pg", None, Some("yah-rust"));
        e.apt = vec!["postgresql-client".into(), "libpq-dev".into()];
        e.env = HashMap::from([("PGUSER".into(), "yah".into())]);
        let dockerfile = compile_entry(&e, &bundled()).unwrap();
        assert!(
            dockerfile.contains("FROM ghcr.io/yah-ai/yah-rust:latest"),
            "missing FROM: {dockerfile}"
        );
        assert!(
            dockerfile.contains("apt-get install"),
            "missing apt: {dockerfile}"
        );
        assert!(
            dockerfile.contains("libpq-dev"),
            "missing package: {dockerfile}"
        );
        assert!(
            dockerfile.contains("postgresql-client"),
            "missing package: {dockerfile}"
        );
        assert!(
            dockerfile.contains("ENV PGUSER=yah"),
            "missing env: {dockerfile}"
        );
    }

    #[test]
    fn pure_toml_pip_layering() {
        let mut e = entry("ml-runner", None, Some("yah-python"));
        e.pip = vec!["numpy".into(), "scipy".into(), "pandas".into()];
        let dockerfile = compile_entry(&e, &bundled()).unwrap();
        assert!(dockerfile.contains("FROM ghcr.io/yah-ai/yah-python:latest"));
        assert!(dockerfile.contains("pip install --no-cache-dir"));
        assert!(dockerfile.contains("numpy"));
        assert!(dockerfile.contains("pandas"));
    }

    #[test]
    fn base_only_entry_emits_from_base() {
        let e = entry("custom-base", Some("alpine:3.20"), None);
        let dockerfile = compile_entry(&e, &bundled()).unwrap();
        assert!(dockerfile.contains("FROM alpine:3.20"));
    }

    #[test]
    fn extends_chain_validates_root_has_base() {
        // yah-rust-bun → yah-rust → (base: rust:1-slim-bookworm). Should be fine.
        let e = bundled().get("yah-rust-bun").unwrap().clone();
        compile_entry(&e, &bundled()).unwrap();
    }

    #[test]
    fn extends_unknown_target_rejected() {
        let e = entry("orphan", None, Some("does-not-exist"));
        let err = compile_entry(&e, &bundled()).unwrap_err();
        assert!(
            matches!(err, CompileError::ExtendsNotFound { ref target, .. } if target == "does-not-exist")
        );
    }

    #[test]
    fn extends_cycle_detected() {
        // Build a synthetic catalog with a→b→a.
        let a = {
            let mut e = entry("a", None, Some("b"));
            e.description = "cycle a".into();
            e
        };
        let b = entry("b", None, Some("a"));
        // Build a CatalogManifest from synthetic TOML so we exercise the
        // public loader rather than poking internals.
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("a.toml"),
            r#"
[image]
name        = "a"
extends     = "b"
description = "cycle a"
"#,
        )
        .unwrap();
        fs::write(
            dir.path().join("b.toml"),
            r#"
[image]
name        = "b"
extends     = "a"
description = "cycle b"
"#,
        )
        .unwrap();
        let manifest = CatalogManifest::load(dir.path()).unwrap();
        let err = compile_entry(&a, &manifest).unwrap_err();
        match err {
            CompileError::ExtendsCycle { name, chain } => {
                assert_eq!(name, "a");
                assert_eq!(chain.first().map(String::as_str), Some("a"));
                assert!(
                    chain.iter().filter(|n| *n == "a").count() >= 2,
                    "cycle chain shows return: {chain:?}"
                );
            }
            other => panic!("expected ExtendsCycle, got {other:?}"),
        }
        // Touch b so it's not unused.
        assert_eq!(b.name, "b");
    }

    #[test]
    fn extends_depth_limit_enforced() {
        // Build a→b→c→d→e→f (6 deep — exceeds MAX_EXTENDS_DEPTH = 5).
        let dir = tempdir().unwrap();
        for (name, parent) in [
            ("a", "b"),
            ("b", "c"),
            ("c", "d"),
            ("d", "e"),
            ("e", "f"),
            ("f", "g"),
        ] {
            fs::write(
                dir.path().join(format!("{name}.toml")),
                format!(
                    r#"
[image]
name        = "{name}"
extends     = "{parent}"
description = "depth fixture"
"#
                ),
            )
            .unwrap();
        }
        fs::write(
            dir.path().join("g.toml"),
            r#"
[image]
name        = "g"
base        = "alpine:3.20"
description = "root"
"#,
        )
        .unwrap();
        let manifest = CatalogManifest::load(dir.path()).unwrap();
        let a = manifest.get("a").unwrap().clone();
        let err = compile_entry(&a, &manifest).unwrap_err();
        assert!(
            matches!(err, CompileError::ExtendsTooDeep { ref name, max } if name == "a" && max == MAX_EXTENDS_DEPTH),
            "got: {err:?}"
        );
    }

    #[test]
    fn sibling_dockerfile_returned_verbatim_when_from_present() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("Dockerfile"),
            "FROM debian:bookworm-slim\nRUN echo hi\n",
        )
        .unwrap();
        let e = entry("custom", None, Some("yah-base"));
        let out = compile_with_dockerfile_dir(&e, &bundled(), dir.path()).unwrap();
        assert_eq!(out, "FROM debian:bookworm-slim\nRUN echo hi\n");
    }

    #[test]
    fn sibling_dockerfile_gets_from_prefix_when_missing() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("Dockerfile"), "RUN echo hi\n").unwrap();
        let e = entry("custom", None, Some("yah-base"));
        let out = compile_with_dockerfile_dir(&e, &bundled(), dir.path()).unwrap();
        assert!(
            out.starts_with("FROM ghcr.io/yah-ai/yah-base:latest"),
            "missing prepended FROM: {out}"
        );
        assert!(out.contains("RUN echo hi"));
    }

    #[test]
    fn sibling_dockerfile_with_comment_only_still_gets_prefix() {
        // A `# syntax=` directive should not be mistaken for a FROM line.
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("Dockerfile"),
            "# syntax=docker/dockerfile:1\nRUN echo hi\n",
        )
        .unwrap();
        let e = entry("custom", None, Some("yah-base"));
        let out = compile_with_dockerfile_dir(&e, &bundled(), dir.path()).unwrap();
        assert!(
            out.starts_with("FROM ghcr.io/yah-ai/yah-base:latest"),
            "syntax directive shouldn't count as FROM: {out}"
        );
    }

    #[test]
    fn dir_without_dockerfile_falls_back_to_toml_layering() {
        let dir = tempdir().unwrap();
        let mut e = entry("layered", None, Some("yah-base"));
        e.apt = vec!["jq".into()];
        let out = compile_with_dockerfile_dir(&e, &bundled(), dir.path()).unwrap();
        assert!(out.contains("FROM ghcr.io/yah-ai/yah-base:latest"));
        assert!(out.contains("jq"));
    }

    #[test]
    fn sibling_dockerfile_with_invalid_extends_still_rejected() {
        // The Dockerfile escape hatch doesn't excuse a bogus extends — the
        // user has explicitly referenced a parent and we should catch typos.
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("Dockerfile"), "FROM scratch\n").unwrap();
        let e = entry("typo", None, Some("yah-rsut")); // 'rsut' typo
        let err = compile_with_dockerfile_dir(&e, &bundled(), dir.path()).unwrap_err();
        assert!(
            matches!(err, CompileError::ExtendsNotFound { ref target, .. } if target == "yah-rsut")
        );
    }
}
