//! Transform recipe loader — separate from the pipeline loader.
//!
//! W164 transforms are tiny TOML files under `.yah/qed/transforms/<name>.toml`
//! that describe a deterministic, digest-pinned, container-by-default tool
//! invocation (e.g. `whisper-cpp quantize`). They are NOT pipelines — they
//! have a fixed IO contract:
//!
//! - `YAH_TRANSFORM_IN_0` — path to the resolved fetched input bytes
//! - `YAH_TRANSFORM_OUT`  — path the recipe writes the transformed bytes to
//!
//! Both are bound as `{{key}}` substitutions at argv-element granularity
//! (no shell, no string concat). Caller-supplied `params` from
//! `[[asset.derive.transform]].params` substitute the same way.
//!
//! The recipe lowers to a `task::ForgeSpec` at materialize time (R438-T5).
//! This module is parse-only — no execution, no I/O beyond reading the TOML.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;
use workload_spec::ImageRef;

use velveteen::TaskRuntime;

/// Substitution key bound to the resolved fetched input (always present).
pub const ENV_TRANSFORM_IN_0: &str = "YAH_TRANSFORM_IN_0";

/// Substitution key bound to the recipe's output path (always present).
pub const ENV_TRANSFORM_OUT: &str = "YAH_TRANSFORM_OUT";

/// On-disk recipe shape. Top-level TOML keys map 1:1 to fields.
///
/// The image is digest-enforced at two layers:
/// 1. String-form `image = "ghcr.io/.../foo:v1@sha256:<hex>"` — rejected at
///    serde-deserialize by [`workload_spec::ImageRef`]'s custom Deserialize
///    (R438-T3).
/// 2. Struct-form `[image] registry = ... tag = ...` — backwards-compat with
///    the legacy [`WorkloadSpec`](workload_spec::WorkloadSpec) shape, but the
///    recipe loader enforces `digest.is_some()` post-parse so the recipe path
///    can't slip a bare-tag image through.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TransformRecipe {
    pub name: String,
    pub label: String,
    pub placement: RecipePlacement,
    pub image: ImageRef,
    #[serde(default)]
    pub steps: Vec<RecipeStep>,
}

/// Where + how a recipe step runs.
///
/// W164 transforms are local-only by design (the materialize step runs in the
/// reconciler that owns the cache). Remote placement is explicitly out of
/// scope for W164 — the location enum reflects that today and stays open for
/// additive growth if a later doc opens that surface.
///
/// `platform`, when set, forces `docker run --platform <value>` so a recipe
/// pinned to a single-arch upstream image (e.g. ggerganov/whisper.cpp ships
/// `linux/amd64` only) still runs on cross-arch hosts via emulation (Rosetta
/// on Apple Silicon, qemu on Linux/arm64). Omit it for multi-arch images and
/// docker picks the host-matching manifest automatically.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RecipePlacement {
    pub location: RecipeLocation,
    pub runtime: TaskRuntime,
    #[serde(default)]
    pub platform: Option<String>,
}

/// Recipe-side location vocabulary (Local-only for W164).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecipeLocation {
    Local,
}

/// One executable step in a recipe. `argv[0]` is the executable; the
/// `{{key}}` placeholders are substituted at element granularity by
/// [`substitute_argv`]. There's no shell — element boundaries are preserved.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RecipeStep {
    pub name: String,
    pub argv: Vec<String>,
    /// Step timeout in seconds. `0` means no timeout.
    #[serde(default)]
    pub timeout: u64,
}

#[derive(Error, Debug)]
pub enum RecipeError {
    #[error("recipe {name:?} not found at {path}")]
    NotFound { name: String, path: PathBuf },
    #[error("reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parsing recipe {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error(
        "recipe {name:?} at {path} uses a bare-tag image; recipe images must \
         be digest-pinned (e.g. `image = \"...:v1@sha256:<hex>\"`) for \
         reproducibility (W164)"
    )]
    ImageNotPinned { name: String, path: PathBuf },
}

/// Loader for `.yah/qed/transforms/*.toml` files.
///
/// Kept *separate* from `PipelineLoader` (in `yah-qed`) per W164
/// OQ#1: pipelines and transforms have different IO contracts and conflating
/// them in one loader forces every pipeline to branch on "is this actually a
/// transform?".
pub struct TransformRecipeLoader {
    transforms_dir: PathBuf,
}

impl TransformRecipeLoader {
    /// Construct a loader rooted at the transforms directory
    /// (conventionally `<workspace>/.yah/qed/transforms`).
    pub fn new(transforms_dir: impl AsRef<Path>) -> Self {
        Self {
            transforms_dir: transforms_dir.as_ref().to_path_buf(),
        }
    }

    /// Path the recipe with the given name would live at.
    pub fn recipe_path(&self, name: &str) -> PathBuf {
        self.transforms_dir.join(format!("{name}.toml"))
    }

    /// List every recipe name (the `*.toml` file stem) in the transforms dir,
    /// sorted. Mirrors [`PipelineLoader::list_all`](crate) — scans the
    /// directory on each call so a freshly-dropped recipe surfaces without a
    /// daemon restart. A missing transforms dir is not an error: it yields an
    /// empty list (a camp may legitimately define no transforms).
    pub fn list_all(&self) -> Result<Vec<String>, RecipeError> {
        let entries = match fs::read_dir(&self.transforms_dir) {
            Ok(e) => e,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(RecipeError::Io {
                    path: self.transforms_dir.clone(),
                    source,
                })
            }
        };
        let mut names: Vec<String> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "toml"))
            .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
            .collect();
        names.sort();
        Ok(names)
    }

    /// Load and validate a recipe by name. The two post-parse rules:
    /// - `recipe.name` must match the requested name (catches typos / rename
    ///   accidents).
    /// - `recipe.image.digest` must be `Some` (digest-pin enforcement; the
    ///   string-form deserializer enforces this for `image = "..."`, this
    ///   catches the legacy struct form too).
    pub fn load(&self, name: &str) -> Result<TransformRecipe, RecipeError> {
        let path = self.recipe_path(name);
        if !path.exists() {
            return Err(RecipeError::NotFound { name: name.to_string(), path });
        }
        self.load_from_path(&path)
    }

    /// Read + parse + validate a recipe at an explicit path. Lower-level than
    /// [`Self::load`]; useful when callers already know the file location
    /// (tests, single-recipe materialize paths).
    pub fn load_from_path(&self, path: &Path) -> Result<TransformRecipe, RecipeError> {
        let content = fs::read_to_string(path).map_err(|source| RecipeError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let recipe: TransformRecipe = toml::from_str(&content).map_err(|source| RecipeError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        // R411 schema: ImageRef.digest is now a non-optional String — serde
        // already rejects struct-form TOML missing the field at parse time.
        // Defense-in-depth here against an empty value sneaking past.
        if recipe.image.digest.is_empty() {
            return Err(RecipeError::ImageNotPinned {
                name: recipe.name.clone(),
                path: path.to_path_buf(),
            });
        }
        Ok(recipe)
    }
}

/// Substitute `{{key}}` placeholders in every argv element.
///
/// Substitution rules:
/// - Match `{{key}}` literally; whitespace inside (`{{ key }}`) is trimmed.
/// - Replace with `params[key]` if present; **leave the placeholder verbatim**
///   if the key is unknown (callers must inspect the result and decide how to
///   surface missing bindings — typically by erroring).
/// - Unterminated `{{` (no closing `}}`) is preserved as literal text.
/// - Substitution is per-element: a `{{key}}` that resolves to a value
///   containing spaces does NOT split into multiple argv elements. This is
///   the "no shell, no string concat" rule.
pub fn substitute_argv(template: &[String], params: &BTreeMap<String, String>) -> Vec<String> {
    template.iter().map(|elem| substitute_one(elem, params)).collect()
}

fn substitute_one(elem: &str, params: &BTreeMap<String, String>) -> String {
    let mut out = String::with_capacity(elem.len());
    let mut rest = elem;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            // Unterminated — preserve the rest verbatim and stop scanning.
            out.push_str("{{");
            out.push_str(after);
            return out;
        };
        let key = after[..end].trim();
        if let Some(val) = params.get(key) {
            out.push_str(val);
        } else {
            // Unknown key: keep the placeholder so the caller can detect it.
            out.push_str("{{");
            out.push_str(&after[..end]);
            out.push_str("}}");
        }
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const HASH_64: &str = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";

    fn whisper_quantize_toml() -> String {
        // NB: `image` must come BEFORE the `[placement]` table — otherwise TOML
        // scopes it into placement. Top-level scalar fields (name / label /
        // image) all live in the implicit top-level table.
        format!(
            r#"
name  = "whisper-quantize"
label = "Quantize a whisper GGML model"
image = "ghcr.io/ggerganov/whisper.cpp:v1.7.4@sha256:{HASH_64}"

[placement]
location = "local"
runtime  = "container"

[[steps]]
name    = "quantize"
argv    = ["./quantize", "{{{{YAH_TRANSFORM_IN_0}}}}", "{{{{YAH_TRANSFORM_OUT}}}}", "{{{{quant}}}}"]
timeout = 600
"#
        )
    }

    #[test]
    fn loads_sample_recipe_round_trip() {
        let dir = tempdir().unwrap();
        let transforms = dir.path().join("transforms");
        fs::create_dir_all(&transforms).unwrap();
        fs::write(
            transforms.join("whisper-quantize.toml"),
            whisper_quantize_toml(),
        )
        .unwrap();

        let loader = TransformRecipeLoader::new(&transforms);
        let recipe = loader.load("whisper-quantize").expect("load recipe");
        assert_eq!(recipe.name, "whisper-quantize");
        assert_eq!(recipe.placement.location, RecipeLocation::Local);
        assert_eq!(recipe.placement.runtime, TaskRuntime::Container);
        assert_eq!(recipe.image.registry, "ghcr.io");
        assert_eq!(recipe.image.repository, "ggerganov/whisper.cpp");
        assert_eq!(recipe.image.tag, "v1.7.4");
        assert_eq!(recipe.image.digest, format!("sha256:{HASH_64}"));
        assert_eq!(recipe.steps.len(), 1);
        assert_eq!(recipe.steps[0].name, "quantize");
        assert_eq!(recipe.steps[0].timeout, 600);
        assert_eq!(
            recipe.steps[0].argv,
            vec![
                "./quantize",
                "{{YAH_TRANSFORM_IN_0}}",
                "{{YAH_TRANSFORM_OUT}}",
                "{{quant}}",
            ]
        );
    }

    #[test]
    fn list_all_returns_sorted_stems_and_ignores_non_toml() {
        let dir = tempdir().unwrap();
        let transforms = dir.path().join("transforms");
        fs::create_dir_all(&transforms).unwrap();
        fs::write(transforms.join("zeta.toml"), "").unwrap();
        fs::write(transforms.join("alpha.toml"), "").unwrap();
        fs::write(transforms.join("README.md"), "ignore me").unwrap();

        let loader = TransformRecipeLoader::new(&transforms);
        let names = loader.list_all().expect("list_all ok");
        assert_eq!(names, vec!["alpha".to_string(), "zeta".to_string()]);
    }

    #[test]
    fn list_all_missing_dir_is_empty_not_error() {
        let dir = tempdir().unwrap();
        let loader = TransformRecipeLoader::new(dir.path().join("does-not-exist"));
        assert_eq!(loader.list_all().expect("ok"), Vec::<String>::new());
    }

    #[test]
    fn rejects_recipe_with_bare_tag_string_image() {
        let dir = tempdir().unwrap();
        let transforms = dir.path().join("transforms");
        fs::create_dir_all(&transforms).unwrap();
        let bad = r#"
name  = "bare-tag"
label = "no pin"
image = "node:20"

[placement]
location = "local"
runtime  = "container"

[[steps]]
name = "noop"
argv = ["true"]
"#;
        fs::write(transforms.join("bare-tag.toml"), bad).unwrap();
        let loader = TransformRecipeLoader::new(&transforms);
        let err = loader.load("bare-tag").expect_err("bare-tag must reject");
        // ImageRef's string-form Deserialize (R438-T3) catches it first → Parse.
        assert!(matches!(err, RecipeError::Parse { .. }), "got {err:?}");
    }

    #[test]
    fn rejects_recipe_with_struct_image_missing_digest() {
        let dir = tempdir().unwrap();
        let transforms = dir.path().join("transforms");
        fs::create_dir_all(&transforms).unwrap();
        let bad = r#"
name  = "struct-bare"
label = "struct-form bare tag"

[placement]
location = "local"
runtime  = "container"

[image]
registry = "ghcr.io"
repository = "foo/bar"
tag = "v1"

[[steps]]
name = "noop"
argv = ["true"]
"#;
        fs::write(transforms.join("struct-bare.toml"), bad).unwrap();
        let loader = TransformRecipeLoader::new(&transforms);
        let err = loader
            .load("struct-bare")
            .expect_err("struct-form bare tag must reject");
        // ImageRef.digest is required at deserialize (R438-T3 workspace-wide
        // tightening) — struct-form without digest fails at parse time, not at
        // the post-parse ImageNotPinned check. The post-parse check is now
        // belt-and-braces against an empty-string digest sneaking through.
        assert!(matches!(err, RecipeError::Parse { .. }), "got {err:?}");
    }

    #[test]
    fn missing_recipe_reports_path() {
        let dir = tempdir().unwrap();
        let loader = TransformRecipeLoader::new(dir.path().join("transforms"));
        let err = loader.load("nope").expect_err("missing recipe");
        match err {
            RecipeError::NotFound { name, .. } => assert_eq!(name, "nope"),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn substitute_argv_replaces_known_placeholders() {
        let template = vec![
            "./tool".to_string(),
            "{{YAH_TRANSFORM_IN_0}}".to_string(),
            "{{YAH_TRANSFORM_OUT}}".to_string(),
            "--mode={{quant}}".to_string(),
        ];
        let mut params = BTreeMap::new();
        params.insert(ENV_TRANSFORM_IN_0.into(), "/cache/fetch/abc.bin".into());
        params.insert(ENV_TRANSFORM_OUT.into(), "/tmp/out.bin".into());
        params.insert("quant".into(), "q5_1".into());
        let resolved = substitute_argv(&template, &params);
        assert_eq!(
            resolved,
            vec![
                "./tool",
                "/cache/fetch/abc.bin",
                "/tmp/out.bin",
                "--mode=q5_1",
            ]
        );
    }

    #[test]
    fn substitute_argv_preserves_unknown_placeholders() {
        let template = vec!["{{unknown}}".to_string()];
        let resolved = substitute_argv(&template, &BTreeMap::new());
        assert_eq!(resolved, vec!["{{unknown}}".to_string()]);
    }

    #[test]
    fn substitute_argv_does_not_split_values_with_spaces() {
        // Per the no-shell rule: a substituted value containing spaces stays
        // as ONE argv element. The element boundary is enforced.
        let template = vec!["{{flag}}".to_string()];
        let mut params = BTreeMap::new();
        params.insert("flag".into(), "--a --b --c".into());
        let resolved = substitute_argv(&template, &params);
        assert_eq!(resolved, vec!["--a --b --c"]);
        assert_eq!(resolved.len(), 1, "must not split on spaces");
    }

    #[test]
    fn substitute_argv_handles_unterminated_braces() {
        let template = vec!["{{never_closed".to_string()];
        let resolved = substitute_argv(&template, &BTreeMap::new());
        assert_eq!(resolved, vec!["{{never_closed".to_string()]);
    }

    #[test]
    fn substitute_argv_trims_whitespace_in_key() {
        let template = vec!["{{  key  }}".to_string()];
        let mut params = BTreeMap::new();
        params.insert("key".into(), "value".into());
        let resolved = substitute_argv(&template, &params);
        assert_eq!(resolved, vec!["value".to_string()]);
    }
}
