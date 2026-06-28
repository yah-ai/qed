//! Catalog data model: yah-managed container images, addressable by name.
//!
//! The bundled manifest at `crates/yah/qed/images/catalog.toml` ships
//! `yah-base`, `yah-rust`, `yah-bun`, `yah-rust-bun`, `yah-python`,
//! `yah-cuda`, `yah-node`, `yah-yubaba`, `yah-miniflare`. Per-camp entries
//! at `.yah/qed/images/<name>.toml`
//! (or `.yah/qed/images/<name>/image.toml` when a Dockerfile lives next to
//! the TOML) override or extend the bundled set.
//!
//! No build logic here — just data. Compile (TOML layering → Dockerfile)
//! and dispatch (`qed::build-image` step) ship in R381-T3 and R381-T2/T4/T5.

use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use thiserror::Error;

/// Output artifact kind a catalog entry yields.
///
/// W148 ships only [`ProduceTarget::OciImage`] — entries are built into
/// container images. W154 (R407) adds [`ProduceTarget::NativeTarball`]:
/// a static musl Rust binary packaged as a plain tarball for the native
/// runtime path under Kamaji. The two are not mutually exclusive — an
/// entry may declare both to cover container and native peers from one
/// catalog row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProduceTarget {
    OciImage,
    NativeTarball,
}

/// One container image in the catalog.
///
/// Exactly one of `base` (upstream image) or `extends` (another catalog
/// entry by name) must be set. `digests` is keyed by Docker architecture
/// (e.g. `amd64`, `arm64`) and populated by the release pipeline at build
/// time — empty in source.
///
/// `apt`, `pip`, `env` carry the W148 layering shorthand for per-camp
/// customs.  Bundled entries leave these empty; the [`compile`](super::compile)
/// module turns a non-empty layering into a generated Dockerfile.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CatalogEntry {
    pub name: String,
    #[serde(default)]
    pub base: Option<String>,
    #[serde(default)]
    pub extends: Option<String>,
    pub description: String,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub digests: HashMap<String, String>,
    /// Debian/Ubuntu apt packages installed via `apt-get install` on top of
    /// the resolved base.  Layering shorthand — ignored when a sibling
    /// Dockerfile is present.
    #[serde(default)]
    pub apt: Vec<String>,
    /// Python pip packages installed via `pip install`. Same caveats as `apt`.
    #[serde(default)]
    pub pip: Vec<String>,
    /// Environment variables baked into the image via `ENV K=V`.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Output artifact kinds this entry produces (W154 R407). Defaults to
    /// `[oci-image]` — container-first for safety. An entry adopting the
    /// native runtime path declares `native-tarball` (alone or alongside
    /// `oci-image`); the packaging step (R407-T2) emits a static musl
    /// binary tarball when this is present and the cross-compile preflight
    /// (R407-T3) clears the musl gate.
    #[serde(default = "default_produces")]
    pub produces: Vec<ProduceTarget>,
}

fn default_produces() -> Vec<ProduceTarget> {
    vec![ProduceTarget::OciImage]
}

impl CatalogEntry {
    /// Reject orphan entries (neither `base` nor `extends`), entries that
    /// set both (ambiguous — pick one), and entries that explicitly declare
    /// an empty `produces` list (an entry that produces nothing is
    /// unbuildable).
    fn validate(&self) -> Result<(), CatalogError> {
        match (self.base.as_deref(), self.extends.as_deref()) {
            (None, None) => return Err(CatalogError::OrphanEntry(self.name.clone())),
            (Some(_), Some(_)) => return Err(CatalogError::AmbiguousBase(self.name.clone())),
            _ => {}
        }
        if self.produces.is_empty() {
            return Err(CatalogError::EmptyProduces(self.name.clone()));
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("IO error reading catalog: {0}")]
    Io(#[from] std::io::Error),
    #[error("TOML parse error in {path}: {source}")]
    TomlParse {
        path: String,
        source: toml::de::Error,
    },
    #[error("catalog entry `{0}` has neither `base` nor `extends`")]
    OrphanEntry(String),
    #[error("catalog entry `{0}` sets both `base` and `extends` (pick one)")]
    AmbiguousBase(String),
    #[error("catalog entry `{0}` declares an empty `produces` list (must yield at least one of `oci-image`, `native-tarball`)")]
    EmptyProduces(String),
}

#[derive(Debug, Deserialize)]
struct BundledToml {
    #[serde(default)]
    image: Vec<CatalogEntry>,
}

#[derive(Debug, Deserialize)]
struct PerCampToml {
    image: CatalogEntry,
}

/// Bundled catalog source, embedded at compile time.
const BUNDLED_CATALOG_TOML: &str = include_str!("../../images/catalog.toml");

/// Merged catalog: bundled defaults + per-camp overrides.
///
/// Entry order is preserved from the bundled manifest, then per-camp entries
/// (any name not in the bundled set) are appended in directory-scan order.
/// Lookups are by name; `entries` is the canonical list.
#[derive(Debug, Clone)]
pub struct CatalogManifest {
    entries: Vec<CatalogEntry>,
}

impl CatalogManifest {
    /// Parse the bundled catalog only. Useful in tests and for callers that
    /// don't have a camp directory (e.g. inspecting defaults).
    pub fn bundled() -> Result<Self, CatalogError> {
        Self::from_bundled_str(BUNDLED_CATALOG_TOML)
    }

    /// Bundled catalog + per-camp overrides from `qed_images_dir`
    /// (typically `.yah/qed/images/`).
    ///
    /// Per-camp resolution order, per name:
    /// 1. `<qed_images_dir>/<name>.toml` (flat form, layering-only)
    /// 2. `<qed_images_dir>/<name>/image.toml` (directory form, may have a sibling Dockerfile)
    ///
    /// A missing or non-directory `qed_images_dir` is not an error — the
    /// loader simply returns the bundled set.
    pub fn load(qed_images_dir: impl AsRef<Path>) -> Result<Self, CatalogError> {
        let mut manifest = Self::bundled()?;
        let dir = qed_images_dir.as_ref();
        if !dir.is_dir() {
            return Ok(manifest);
        }
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            let parsed = if file_type.is_file() && path.extension().map_or(false, |e| e == "toml") {
                Some(parse_per_camp(&path)?)
            } else if file_type.is_dir() {
                let nested = path.join("image.toml");
                if nested.is_file() {
                    Some(parse_per_camp(&nested)?)
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(entry) = parsed {
                manifest.upsert(entry);
            }
        }
        Ok(manifest)
    }

    fn from_bundled_str(src: &str) -> Result<Self, CatalogError> {
        let parsed: BundledToml = toml::from_str(src).map_err(|e| CatalogError::TomlParse {
            path: "<bundled catalog.toml>".to_string(),
            source: e,
        })?;
        for entry in &parsed.image {
            entry.validate()?;
        }
        Ok(Self {
            entries: parsed.image,
        })
    }

    fn upsert(&mut self, entry: CatalogEntry) {
        if let Some(slot) = self.entries.iter_mut().find(|e| e.name == entry.name) {
            *slot = entry;
        } else {
            self.entries.push(entry);
        }
    }

    /// All entries in canonical order (bundled first, then per-camp additions).
    pub fn entries(&self) -> &[CatalogEntry] {
        &self.entries
    }

    /// Lookup by catalog name.
    pub fn get(&self, name: &str) -> Option<&CatalogEntry> {
        self.entries.iter().find(|e| e.name == name)
    }

    /// All entry names in canonical order.
    pub fn names(&self) -> Vec<&str> {
        self.entries.iter().map(|e| e.name.as_str()).collect()
    }
}

fn parse_per_camp(path: &Path) -> Result<CatalogEntry, CatalogError> {
    let content = fs::read_to_string(path)?;
    let parsed: PerCampToml = toml::from_str(&content).map_err(|e| CatalogError::TomlParse {
        path: path.display().to_string(),
        source: e,
    })?;
    parsed.image.validate()?;
    Ok(parsed.image)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    const EXPECTED_BUNDLED: &[&str] = &[
        "yah-base",
        "yah-rust",
        "yah-bun",
        "yah-rust-bun",
        "yah-python",
        "yah-cuda",
        "yah-node",
        "yah-yubaba",
        "yah-miniflare",
        "yah-mesofact-dev",
        "yah-cloud-runner",
        "rusty-v8-musl-builder",
    ];

    #[test]
    fn bundled_catalog_loads_with_all_entries() {
        let manifest = CatalogManifest::bundled().expect("bundled catalog parses");
        let names = manifest.names();
        for expected in EXPECTED_BUNDLED {
            assert!(
                names.contains(expected),
                "bundled catalog missing `{expected}` (got {names:?})"
            );
        }
    }

    #[test]
    fn yah_rust_bun_extends_yah_rust() {
        let manifest = CatalogManifest::bundled().unwrap();
        let entry = manifest.get("yah-rust-bun").expect("yah-rust-bun present");
        assert_eq!(entry.extends.as_deref(), Some("yah-rust"));
        assert!(entry.base.is_none());
    }

    #[test]
    fn primitive_entry_has_base_not_extends() {
        let manifest = CatalogManifest::bundled().unwrap();
        let entry = manifest.get("yah-rust").expect("yah-rust present");
        assert!(entry.extends.is_none());
        assert_eq!(entry.base.as_deref(), Some("rust:1-slim-bookworm"));
    }

    #[test]
    fn bundled_entries_have_empty_digests() {
        let manifest = CatalogManifest::bundled().unwrap();
        for entry in manifest.entries() {
            assert!(
                entry.digests.is_empty(),
                "bundled entry `{}` carries digests in source (should be release-time inject only)",
                entry.name,
            );
        }
    }

    #[test]
    fn missing_camp_dir_returns_bundled() {
        let manifest = CatalogManifest::load("/nonexistent/path/does/not/exist").unwrap();
        assert_eq!(manifest.names().len(), EXPECTED_BUNDLED.len());
    }

    #[test]
    fn per_camp_flat_toml_overrides_by_name() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("yah-rust.toml"),
            r#"
[image]
name        = "yah-rust"
base        = "rust:1.80-slim-bookworm"
description = "Pinned to 1.80 for this camp"
tools       = ["cargo", "rustc"]
"#,
        )
        .unwrap();
        let manifest = CatalogManifest::load(dir.path()).unwrap();
        let entry = manifest.get("yah-rust").unwrap();
        assert_eq!(entry.base.as_deref(), Some("rust:1.80-slim-bookworm"));
        assert_eq!(entry.description, "Pinned to 1.80 for this camp");
    }

    #[test]
    fn per_camp_directory_form_overrides_by_name() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("yah-python");
        fs::create_dir(&sub).unwrap();
        fs::write(
            sub.join("image.toml"),
            r#"
[image]
name        = "yah-python"
base        = "python:3.12-slim-bookworm"
description = "Pinned to 3.12"
tools       = ["pip"]
"#,
        )
        .unwrap();
        let manifest = CatalogManifest::load(dir.path()).unwrap();
        let entry = manifest.get("yah-python").unwrap();
        assert_eq!(entry.base.as_deref(), Some("python:3.12-slim-bookworm"));
    }

    #[test]
    fn per_camp_new_entry_appended() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("yah-rust-pg.toml"),
            r#"
[image]
name        = "yah-rust-pg"
extends     = "yah-rust"
description = "Rust + postgres client"
tools       = ["psql"]
"#,
        )
        .unwrap();
        let manifest = CatalogManifest::load(dir.path()).unwrap();
        assert!(manifest.get("yah-rust-pg").is_some());
        assert_eq!(manifest.names().len(), EXPECTED_BUNDLED.len() + 1);
    }

    #[test]
    fn orphan_entry_rejected() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("broken.toml"),
            r#"
[image]
name        = "broken"
description = "No base or extends"
"#,
        )
        .unwrap();
        let err = CatalogManifest::load(dir.path()).unwrap_err();
        assert!(matches!(err, CatalogError::OrphanEntry(ref n) if n == "broken"));
    }

    #[test]
    fn ambiguous_base_rejected() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("both.toml"),
            r#"
[image]
name        = "both"
base        = "debian:bookworm-slim"
extends     = "yah-base"
description = "Sets both — ambiguous"
"#,
        )
        .unwrap();
        let err = CatalogManifest::load(dir.path()).unwrap_err();
        assert!(matches!(err, CatalogError::AmbiguousBase(ref n) if n == "both"));
    }

    #[test]
    fn bundled_entries_default_to_oci_image_produces() {
        let manifest = CatalogManifest::bundled().unwrap();
        for entry in manifest.entries() {
            assert_eq!(
                entry.produces,
                vec![ProduceTarget::OciImage],
                "bundled entry `{}` should default to [oci-image] (no explicit produces in source)",
                entry.name,
            );
        }
    }

    #[test]
    fn per_camp_produces_native_tarball_parses() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("yah-yubaba.toml"),
            r#"
[image]
name        = "yah-yubaba"
base        = "scratch"
description = "Native musl-static yubaba binary"
produces    = ["native-tarball"]
"#,
        )
        .unwrap();
        let manifest = CatalogManifest::load(dir.path()).unwrap();
        let entry = manifest.get("yah-yubaba").unwrap();
        assert_eq!(entry.produces, vec![ProduceTarget::NativeTarball]);
    }

    #[test]
    fn per_camp_produces_both_targets_parses() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("yah-yubaba.toml"),
            r#"
[image]
name        = "yah-yubaba"
base        = "debian:bookworm-slim"
description = "Container + native peer"
produces    = ["oci-image", "native-tarball"]
"#,
        )
        .unwrap();
        let manifest = CatalogManifest::load(dir.path()).unwrap();
        let entry = manifest.get("yah-yubaba").unwrap();
        assert_eq!(
            entry.produces,
            vec![ProduceTarget::OciImage, ProduceTarget::NativeTarball],
        );
    }

    #[test]
    fn empty_produces_rejected() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("ghost.toml"),
            r#"
[image]
name        = "ghost"
base        = "scratch"
description = "Produces nothing — must reject"
produces    = []
"#,
        )
        .unwrap();
        let err = CatalogManifest::load(dir.path()).unwrap_err();
        assert!(matches!(err, CatalogError::EmptyProduces(ref n) if n == "ghost"));
    }

    #[test]
    fn unknown_produces_variant_rejected() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("bad.toml"),
            r#"
[image]
name        = "bad"
base        = "scratch"
description = "Bogus produces variant"
produces    = ["systemd-portable"]
"#,
        )
        .unwrap();
        let err = CatalogManifest::load(dir.path()).unwrap_err();
        assert!(matches!(err, CatalogError::TomlParse { .. }));
    }

    #[test]
    fn unrelated_files_ignored() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("README.md"), "ignore me").unwrap();
        // Directory without image.toml is silently skipped.
        fs::create_dir(dir.path().join("empty-dir")).unwrap();
        let manifest = CatalogManifest::load(dir.path()).unwrap();
        assert_eq!(manifest.names().len(), EXPECTED_BUNDLED.len());
    }
}
