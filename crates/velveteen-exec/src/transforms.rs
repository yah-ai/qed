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

use velveteen::{TaskLocation, TaskRuntime};

/// Substitution key bound to the resolved fetched input (always present).
///
/// **Bind an ABSOLUTE path.** See [`ENV_TRANSFORM_OUT`].
pub const ENV_TRANSFORM_IN_0: &str = "YAH_TRANSFORM_IN_0";

/// Substitution key bound to the recipe's output path (always present).
///
/// **Bind an ABSOLUTE path.** A recipe is free to `cd` — rusty-v8's build-v8.sh
/// chdirs into a scratch dir to build V8 — so a relative binding resolves
/// against whatever cwd the recipe happens to be in when it writes. The step
/// then exits 0 having written the artifact somewhere the caller never looks;
/// inside a container those bytes die with it. This cost a ~2h arm64 V8 build
/// that had actually succeeded (R546-B8). Callers must canonicalize before
/// binding: `cache_dir` and `workspace_root` are commonly relative (`yah cloud
/// apply` defaults `--path` to `"."`).
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
    /// `[admission]` — the signature a signed recipe carries to kamaji's
    /// admission gate (R555-F4 / W235 §(c)). Absent on an unsigned recipe,
    /// which stays legal: a node running the default `permissive` policy admits
    /// it, and one running `required` refuses it. See
    /// [`crate::admission`] for what the signature covers and why the grant
    /// document is derived rather than stored here.
    #[serde(default)]
    pub admission: Option<crate::admission::RecipeAdmission>,
    /// `[[secrets]]` — the vault credentials this recipe needs on the worker
    /// (R555-F5 / W235 §(c)). Empty on every recipe in the tree today, and
    /// empty means the run can read nothing: the derived admission grant
    /// enumerates exactly these, and a node refuses a dispatched spec that
    /// mounts anything else.
    #[serde(default)]
    pub secrets: Vec<RecipeSecret>,
}

/// One vault credential a recipe declares.
///
/// ```toml
/// [[secrets]]
/// cluster = "r2-write"          # cluster secret name
/// path    = "/run/yah/r2.json"  # absolute path inside the container
/// mode    = "0400"              # optional, octal, defaults to 0400
/// ```
///
/// # Why cluster-only, and why file-only
///
/// [`SecretRef::LocalFile`](workload_spec::SecretRef::LocalFile) names a file in
/// the per-machine store, which carries no access rule at all — naming it is the
/// whole authorization. That is the ambient grant W235 §(c) exists to replace,
/// so a recipe cannot ask for one. A cluster secret carries a
/// [`SecretAccess`](workload_spec::secrets::SecretAccess) rule evaluated on the
/// node at mount time, which is what makes the grant *two-sided*: the recipe
/// author signs for what the build may read, and the secret's owner
/// independently names which recipe may read it.
///
/// File-only for the reason
/// [`SecretTarget`](workload_spec::SecretTarget) already gives: an env var leaks
/// through the subprocess environment and every log dump of it. Admission
/// refuses an env-target secret outright.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecipeSecret {
    /// Cluster secret name, as `yah cloud secret put` stored it.
    pub cluster: String,
    /// Absolute path the value is mounted at inside the container.
    pub path: PathBuf,
    /// Octal file mode, as a string (TOML has no octal literal, and `0400`
    /// written bare is not a valid TOML integer).
    #[serde(default = "default_secret_mode", deserialize_with = "deserialize_octal_mode")]
    pub mode: u32,
}

/// Owner-read-only. A build that needs a credential does not need to hand it to
/// every uid in the container.
fn default_secret_mode() -> u32 {
    0o400
}

fn deserialize_octal_mode<'de, D>(de: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(de)?;
    u32::from_str_radix(raw.trim_start_matches("0o"), 8).map_err(|_| {
        serde::de::Error::custom(format!(
            "mode {raw:?} is not an octal file mode — write it as a string, e.g. \"0400\""
        ))
    })
}

impl RecipeSecret {
    /// The admission-grant entry this declaration authorizes.
    pub fn to_grant(&self) -> workload_spec::admission::GrantSecret {
        workload_spec::admission::GrantSecret {
            source: workload_spec::SecretRef::Cluster {
                name: self.cluster.clone(),
            },
            path: self.path.clone(),
            mode: self.mode,
        }
    }

    /// The mount the dispatcher puts on the workload spec. Exactly the thing
    /// [`RecipeSecret::to_grant`] describes — one constructor each way, so the
    /// pair cannot drift into a grant that does not cover its own mount.
    pub fn to_mount(&self) -> workload_spec::SecretMount {
        workload_spec::SecretMount {
            source: workload_spec::SecretRef::Cluster {
                name: self.cluster.clone(),
            },
            target: workload_spec::SecretTarget::File {
                path: self.path.clone(),
                mode: self.mode,
            },
        }
    }
}

/// Where + how a recipe step runs.
///
/// W164 scoped transforms to local-only; W235 (Remote QED) opened the surface
/// the W164 doc-comment left open. A recipe may now declare a remote node or a
/// remote tier, and the materialize path lowers `location` straight through to
/// [`velveteen::TaskPlacement`] — see [`RecipeLocation`] for the TOML forms.
///
/// `platform`, when set, forces `docker run --platform <value>` so a recipe
/// pinned to a single-arch upstream image (e.g. ggerganov/whisper.cpp ships
/// `linux/amd64` only) still runs on cross-arch hosts via emulation (Rosetta
/// on Apple Silicon, qemu on Linux/arm64). Omit it for multi-arch images and
/// docker picks the host-matching manifest automatically.
///
/// **`platform` is a LOCAL-only knob.** It asks the host's container runtime to
/// emulate a foreign architecture; a remote run has no such host and selects
/// architecture by scheduling instead — `location = { kind = "remote_any", …,
/// mesh_tags = ["arch:x86"] }`. `RemoteForgeDriver` refuses a spec that carries
/// a platform request rather than dropping it, because a silently-ignored
/// `--platform` yields a wrong-arch artifact that only fails at link time.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RecipePlacement {
    #[serde(deserialize_with = "deserialize_recipe_location")]
    pub location: RecipeLocation,
    pub runtime: TaskRuntime,
    #[serde(default)]
    pub platform: Option<String>,
}

/// Recipe-side location vocabulary — **this is [`velveteen::TaskLocation`]**.
///
/// The recipe layer reuses the task layer's placement vocabulary rather than
/// mirroring it, exactly as [`RecipePlacement::runtime`] already reuses
/// [`TaskRuntime`]. A mirrored enum has to re-grow every time the task layer
/// does (R594 added `mesh_tags` to `TaskLocation`; a mirror would have missed
/// it), and "map `RecipePlacement` → `TaskPlacement` straight through" is only
/// honest if the mapping is the identity.
///
/// TOML forms accepted by [`RecipePlacement`]:
///
/// ```toml
/// location = "local"
/// location = { kind = "remote", node = "us-west-002" }
/// location = { kind = "remote_any", tier = "infra", mesh_tags = ["arch:x86"] }
/// ```
///
/// The bare-string form is the pre-W235 spelling and stays valid for `local`
/// only — the remote variants carry a payload, so they need the table form.
pub type RecipeLocation = TaskLocation;

/// Accept both the legacy bare-string `location = "local"` and the tagged
/// table form that [`TaskLocation`]'s own `Deserialize` understands.
///
/// `TaskLocation` is internally tagged (`#[serde(tag = "kind")]`), so it can
/// only be deserialized from a map. Every recipe in the tree predates W235 and
/// says `location = "local"`; rejecting that would be a gratuitous break for
/// zero type-safety gain. Dispatching on the input shape keeps both readable.
fn deserialize_recipe_location<'de, D>(de: D) -> Result<RecipeLocation, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct LocationVisitor;

    impl<'de> serde::de::Visitor<'de> for LocationVisitor {
        type Value = RecipeLocation;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str(
                r#""local", or a table like { kind = "remote", node = "…" } / \
                { kind = "remote_any", tier = "…", mesh_tags = [ … ] }"#,
            )
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            match v {
                "local" => Ok(TaskLocation::Local),
                other => Err(E::custom(format!(
                    "unknown recipe placement location {other:?}. Valid forms: \
                     `location = \"local\"`, \
                     `location = {{ kind = \"remote\", node = \"<mesh-ident>\" }}`, \
                     `location = {{ kind = \"remote_any\", tier = \"<tier>\", \
                     mesh_tags = [\"arch:x86\"] }}`. The remote variants carry a \
                     payload, so the bare-string spelling can't express them."
                ))),
            }
        }

        fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            // Delegate to TaskLocation so its own field-level errors (missing
            // `tier`, unknown `kind`) survive verbatim.
            TaskLocation::deserialize(serde::de::value::MapAccessDeserializer::new(map))
        }
    }

    de.deserialize_any(LocationVisitor)
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

    /// A recipe TOML with `[placement]` spelled however the caller wants.
    fn recipe_with_placement(placement: &str) -> String {
        format!(
            r#"
name  = "placed"
label = "Placement fixture"
image = "ghcr.io/yah-ai/tool:v1@sha256:{HASH_64}"

[placement]
{placement}

[[steps]]
name = "noop"
argv = ["true"]
"#
        )
    }

    fn load_placement(placement: &str) -> Result<RecipePlacement, RecipeError> {
        let dir = tempdir().unwrap();
        let transforms = dir.path().join("transforms");
        fs::create_dir_all(&transforms).unwrap();
        fs::write(
            transforms.join("placed.toml"),
            recipe_with_placement(placement),
        )
        .unwrap();
        TransformRecipeLoader::new(&transforms)
            .load("placed")
            .map(|r| r.placement)
    }

    /// Every recipe in the tree predates W235 and says `location = "local"`.
    /// Growing the enum must not break a single one of them.
    #[test]
    fn bare_string_local_still_parses_after_w235() {
        let placement = load_placement("location = \"local\"\nruntime = \"container\"").unwrap();
        assert_eq!(placement.location, RecipeLocation::Local);
        assert_eq!(placement.runtime, TaskRuntime::Container);
    }

    #[test]
    fn tagged_table_remote_pins_a_named_node() {
        let placement = load_placement(
            "location = { kind = \"remote\", node = \"us-west-002\" }\nruntime = \"container\"",
        )
        .unwrap();
        assert_eq!(
            placement.location,
            RecipeLocation::Remote {
                node: workload_spec::MeshIdent("us-west-002".into()),
            }
        );
    }

    /// The R555-T7 / R546 shape: schedule on an arch-matched node instead of
    /// forking the recipe per architecture and emulating locally.
    #[test]
    fn tagged_table_remote_any_carries_tier_and_mesh_tags() {
        let placement = load_placement(
            "location = { kind = \"remote_any\", tier = \"infra\", mesh_tags = [\"arch:x86\"] }\n\
             runtime = \"container\"",
        )
        .unwrap();
        assert_eq!(
            placement.location,
            RecipeLocation::RemoteAny {
                tier: workload_spec::TierTag("infra".into()),
                mesh_tags: vec!["arch:x86".to_string()],
            }
        );
    }

    #[test]
    fn remote_any_mesh_tags_default_to_empty() {
        let placement = load_placement(
            "location = { kind = \"remote_any\", tier = \"infra\" }\nruntime = \"container\"",
        )
        .unwrap();
        assert_eq!(
            placement.location,
            RecipeLocation::RemoteAny {
                tier: workload_spec::TierTag("infra".into()),
                mesh_tags: vec![],
            }
        );
    }

    /// The remote variants carry a payload, so the bare-string spelling can't
    /// express them — say so instead of a bare "unknown variant".
    #[test]
    fn bare_string_remote_names_the_table_form() {
        let err = load_placement("location = \"remote\"\nruntime = \"container\"")
            .expect_err("bare `remote` must reject");
        let msg = err.to_string();
        assert!(matches!(err, RecipeError::Parse { .. }), "got {err:?}");
        assert!(
            msg.contains("kind = \\\"remote\\\"") || msg.contains("kind = \"remote\""),
            "error must show the table spelling, got: {msg}"
        );
    }

    #[test]
    fn remote_any_without_tier_is_rejected() {
        let err = load_placement(
            "location = { kind = \"remote_any\" }\nruntime = \"container\"",
        )
        .expect_err("remote_any without a tier must reject");
        assert!(matches!(err, RecipeError::Parse { .. }), "got {err:?}");
        assert!(
            err.to_string().contains("tier"),
            "error must name the missing field, got: {err}"
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

    // ── [[secrets]] (R555-F5) ────────────────────────────────────────────────

    fn recipe_with_secrets(block: &str) -> Result<TransformRecipe, toml::de::Error> {
        toml::from_str(&format!(
            r#"
name  = "rusty-v8-musl"
label = "Build rusty_v8 from source"
image = "ghcr.io/yah-ai/b:v1@sha256:{HASH_64}"

[placement]
location = {{ kind = "remote_any", tier = "infra", mesh_tags = ["tier:x86"] }}
runtime  = "container"

[[steps]]
name    = "build"
argv    = ["build-v8.sh"]
timeout = 9000
{block}
"#
        ))
    }

    #[test]
    fn a_recipe_declares_cluster_secrets_with_a_default_owner_only_mode() {
        let r = recipe_with_secrets(
            r#"
[[secrets]]
cluster = "r2-write"
path    = "/run/yah/r2.json"
"#,
        )
        .expect("secrets block parses");
        assert_eq!(
            r.secrets,
            vec![RecipeSecret {
                cluster: "r2-write".into(),
                path: PathBuf::from("/run/yah/r2.json"),
                mode: 0o400,
            }]
        );
    }

    #[test]
    fn an_explicit_mode_is_read_as_octal() {
        let r = recipe_with_secrets(
            r#"
[[secrets]]
cluster = "r2-write"
path    = "/run/yah/r2.json"
mode    = "0440"
"#,
        )
        .unwrap();
        // The whole reason mode is a string: `0440` written bare is not a valid
        // TOML integer, and `440` read as decimal would be mode 0o670.
        assert_eq!(r.secrets[0].mode, 0o440);
    }

    #[test]
    fn a_decimal_looking_mode_is_still_octal_and_a_bad_one_is_an_error() {
        let err = recipe_with_secrets(
            r#"
[[secrets]]
cluster = "r2-write"
path    = "/run/yah/r2.json"
mode    = "0480"
"#,
        )
        .expect_err("8 is not an octal digit");
        assert!(err.to_string().contains("octal"), "{err}");
    }

    /// A per-machine `local_file` secret carries no access rule — naming it is
    /// the whole authorization — so the recipe surface does not offer one, and
    /// a typo reaching for it must not be silently dropped.
    #[test]
    fn a_recipe_cannot_reach_for_a_per_machine_file_secret() {
        let err = recipe_with_secrets(
            r#"
[[secrets]]
local_file = "/var/lib/yah/yubaba/secrets/r2"
path       = "/run/yah/r2.json"
"#,
        )
        .expect_err("local_file is not a recipe-declarable source");
        assert!(err.to_string().contains("local_file"), "{err}");
    }

    #[test]
    fn a_recipe_with_no_secrets_block_declares_none() {
        let r: TransformRecipe = toml::from_str(&whisper_quantize_toml()).unwrap();
        assert!(r.secrets.is_empty());
    }

    /// The paired constructors that keep the grant and the mount from drifting.
    #[test]
    fn the_grant_entry_and_the_mount_describe_the_same_credential() {
        let s = RecipeSecret {
            cluster: "r2-write".into(),
            path: PathBuf::from("/run/yah/r2.json"),
            mode: 0o400,
        };
        let grant = s.to_grant();
        let mount = s.to_mount();
        assert_eq!(
            workload_spec::admission::GrantSecret::of_mount(&mount),
            Some(grant)
        );
    }
}
