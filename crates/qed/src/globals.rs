//! Camp-wide QED globals — checked-in policy defaults, editable from the
//! Run tab's QED subtab. `.yah/qed/globals.toml` at the camp root.
//!
//! Distinct from [`crate::secrets_bridge`]'s `~/.yah/qed/secrets.toml`: that
//! file is per-user, HOME-relative, and never checked in — it names a vault
//! slot, which is exactly what must NOT travel in git. Globals are the
//! opposite: a camp-level policy (which release checks block vs. warn, and
//! whatever else grows here), so an edit shows up in `git status` and
//! `git blame` like any other config change, and every peer on the camp sees
//! the same value.
//!
//! ## Why this file, not one camp-wide blob
//!
//! `.yah/docs/architecture/A031-yah-cloud-config-shape.md:364` records that a
//! generic `[mesofact_dev]` section in `camp.toml` was PROPOSED and
//! REJECTED — "there is no `[mesofact_dev]` block in `camp.toml`... the
//! manifest files are the authority." Each domain owns its own schema in its
//! own file; there is no shared grab-bag. This module is QED's file, not a
//! precedent for a universal one — a future `mesofact_dev_hot_reload` global
//! (a live example from that same rejected proposal) belongs in a
//! `.yah/mesofact/globals.toml` of the same shape, not in this one.
//!
//! ## Tree in the file, flat at the call site (R741-B2)
//!
//! The checked-in file is an ordinary nested TOML table (`[release]
//! tag_hygiene = "warn"`) — nesting is free, it's just TOML, and it's what
//! keeps a growing schema legible instead of one flat wall of prefixed keys.
//! [`CampGlobals::as_param_defaults`] is the one-directional projection down
//! to the flat `name -> value` shape QED's `--param` world already speaks:
//! join every leaf's path with `_` and drop the leading domain segment (this
//! file's is [`DOMAIN_PREFIX`], `"qed"`) — `release.tag_hygiene` becomes the
//! pipeline param `release_tag_hygiene`. [`CampGlobals::as_flat_vars`] keeps
//! the domain segment on, for a future cross-domain export (env vars handed
//! to an arbitrary subprocess, say) where nothing else disambiguates which
//! file a flat name came from.
//!
//! There is deliberately no reverse (flat string -> tree) direction: nothing
//! writes this file FROM a flat map. The Run tab's editor and
//! `qed.globals.set` both round-trip the real nested shape as JSON (JSON
//! nests natively, so no flattening is needed on that leg at all); a flat
//! key split back into a tree is ambiguous in general (an underscore in a
//! field name is indistinguishable from a path join) and nothing here needs
//! to solve that ambiguity, so it isn't attempted.
//!
//! Typed rather than a free-form map on purpose: a rigid schema is what lets
//! the Run tab render a real form (via the generated JSON schema, same
//! pipeline as `qed-pipeline.toml.schema.json`) instead of a raw text editor,
//! and lets a typo in a value fail to parse instead of silently resolving to
//! nothing. Grow this struct field-by-field (a new leaf, or a new nested
//! group alongside `release`) as a real pipeline needs a new camp-wide
//! default — don't pre-populate slots nothing reads yet.
//!
//! Consumption: [`crate::types::Pipeline::resolve_params`] never sees this
//! type directly — it stays a pure function of its `supplied` map. The one
//! call site that starts a run (the daemon's `qed.run` handler and the CLI's
//! in-process fallback) calls [`CampGlobals::resolve_params`], which merges
//! [`CampGlobals::as_param_defaults`] into `supplied` as fallbacks — a
//! React-props-style `{ ...camp_globals, ...operator_supplied }` spread, so
//! an explicit `--param` always shadows the camp default — *before* calling
//! [`crate::types::Pipeline::resolve_params`]. Because that merge happens
//! before [`crate::types::Pipeline::apply_params`] bakes `{{key}}` into each
//! step's `argv`, a global's value reaches a fleet-offloaded step
//! (`execute_step_remote`) through the exact same argv substitution a
//! `--param` override already uses — no separate secrets-style transport
//! needed for propagation to a build-worker.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

/// Camp-relative path to the checked-in globals file.
pub const RELATIVE_PATH: &str = ".yah/qed/globals.toml";

/// This file's domain segment in the flat cross-domain namespace — mirrors
/// the directory it lives under (`.yah/qed/`). A sibling
/// `.yah/mesofact/globals.toml` would use `"mesofact"`.
pub const DOMAIN_PREFIX: &str = "qed";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct CampGlobals {
    #[serde(default)]
    pub release: ReleaseGlobals,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct ReleaseGlobals {
    /// How `check-tag-hygiene` (version-bump / release-patch / release-wizard)
    /// treats a local release tag with no matching ref on `origin`. Defaults
    /// to blocking a new bump — see `.yah/qed/version-bump.toml`.
    #[serde(default)]
    pub tag_hygiene: TagHygiene,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum TagHygiene {
    /// Fail the run — an unpushed local tag must be pushed or deleted first.
    #[default]
    Block,
    /// Print the unpushed tag(s) and continue.
    Warn,
}

impl TagHygiene {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Warn => "warn",
        }
    }
}

impl CampGlobals {
    /// Path: `<camp_root>/.yah/qed/globals.toml`.
    pub fn path_for(camp_root: &Path) -> PathBuf {
        camp_root.join(RELATIVE_PATH)
    }

    /// Missing file (fresh checkout, no camp-level overrides yet) is not an
    /// error — every field's `#[serde(default)]` already expresses the
    /// built-in default, so an absent file and an empty file behave
    /// identically.
    pub fn load_default(camp_root: &Path) -> Self {
        Self::load_from(&Self::path_for(camp_root))
    }

    pub fn load_from(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text).unwrap_or_else(|e| {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "qed globals: parse failed; using built-in defaults",
                );
                Self::default()
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "qed globals: read failed; using built-in defaults",
                );
                Self::default()
            }
        }
    }

    /// Atomic write — tempfile + rename in the same directory, so a reader
    /// never sees a half-written file. Creates `.yah/qed/` if missing.
    pub fn save_to(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let text = toml::to_string_pretty(self)
            .expect("CampGlobals has no non-serializable field types");
        let dir = path.parent().unwrap_or_else(|| Path::new("."));
        let tmp_name = format!(
            ".{}.tmp",
            path.file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("globals.toml")
        );
        let tmp = dir.join(tmp_name);
        std::fs::write(&tmp, text.as_bytes())?;
        std::fs::rename(&tmp, path)
    }

    /// Every leaf, flattened to `<domain>_<path>` -> value — e.g.
    /// `release.tag_hygiene` becomes `qed_release_tag_hygiene`. Generic over
    /// the struct's shape (walks it as JSON), so a new nested group or leaf
    /// needs no change here — only a new field on the struct above.
    pub fn as_flat_vars(&self) -> HashMap<String, String> {
        let value = serde_json::to_value(self).expect("CampGlobals is plain data");
        let mut out = HashMap::new();
        flatten_into(&mut out, DOMAIN_PREFIX, &value);
        out
    }

    /// [`Self::as_flat_vars`] with the leading `qed_` domain segment
    /// stripped, for merging into a QED run's params — the domain is already
    /// implied by being inside `yah qed run` / the daemon's `qed.run`
    /// handler, so the operator-facing param name is just `release_tag_hygiene`.
    pub fn as_param_defaults(&self) -> HashMap<String, String> {
        let prefix = format!("{DOMAIN_PREFIX}_");
        self.as_flat_vars()
            .into_iter()
            .filter_map(|(k, v)| k.strip_prefix(prefix.as_str()).map(|k| (k.to_string(), v)))
            .collect()
    }

    /// Merge [`Self::as_param_defaults`] into `supplied` as fallbacks — a
    /// `{ ...camp_globals, ...supplied }` spread, operator-supplied always
    /// wins (`entry().or_insert()`, not overwrite) — then resolve. The one
    /// path both the daemon's `qed.run` handler and the CLI's in-process
    /// fallback call, so they cannot disagree about which values a camp
    /// global actually fills — the same reason `Pipeline::resolve_params`
    /// itself became a single shared implementation (R751-F3).
    pub fn resolve_params(
        &self,
        pipeline: &crate::types::Pipeline,
        supplied: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>, crate::types::ParamError> {
        let mut merged = supplied.clone();
        for (k, v) in self.as_param_defaults() {
            merged.entry(k).or_insert(v);
        }
        pipeline.resolve_params(&merged)
    }
}

/// Depth-first walk of a JSON tree, joining object keys onto `prefix` with
/// `_` and recording each scalar leaf. Arrays have no unambiguous flat-name
/// convention (no field here is an array today) and are skipped rather than
/// guessed at; `null` (an absent `Option` field, if one is ever added) is
/// skipped the same way `#[serde(default)]` skips it on load.
fn flatten_into(out: &mut HashMap<String, String>, prefix: &str, value: &Json) {
    match value {
        Json::Object(map) => {
            for (k, v) in map {
                flatten_into(out, &format!("{prefix}_{k}"), v);
            }
        }
        Json::String(s) => {
            out.insert(prefix.to_string(), s.clone());
        }
        Json::Bool(b) => {
            out.insert(prefix.to_string(), b.to_string());
        }
        Json::Number(n) => {
            out.insert(prefix.to_string(), n.to_string());
        }
        Json::Null | Json::Array(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_loads_built_in_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        let g = CampGlobals::load_from(&tmp.path().join("globals.toml"));
        assert_eq!(g, CampGlobals::default());
        assert_eq!(g.release.tag_hygiene, TagHygiene::Block);
    }

    #[test]
    fn save_to_roundtrips_through_load_from() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(".yah/qed/globals.toml");
        let g = CampGlobals {
            release: ReleaseGlobals {
                tag_hygiene: TagHygiene::Warn,
            },
        };
        g.save_to(&path).unwrap();
        let loaded = CampGlobals::load_from(&path);
        assert_eq!(loaded, g);
    }

    #[test]
    fn save_to_writes_real_nested_toml_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("globals.toml");
        CampGlobals {
            release: ReleaseGlobals {
                tag_hygiene: TagHygiene::Warn,
            },
        }
        .save_to(&path)
        .unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("[release]"), "expected a [release] table:\n{text}");
        assert!(text.contains("tag_hygiene"));
    }

    #[test]
    fn corrupt_file_falls_back_to_defaults_rather_than_erroring() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("globals.toml");
        std::fs::write(&path, "not = [valid toml").unwrap();
        assert_eq!(CampGlobals::load_from(&path), CampGlobals::default());
    }

    #[test]
    fn as_flat_vars_joins_domain_and_path_with_underscores() {
        let g = CampGlobals {
            release: ReleaseGlobals {
                tag_hygiene: TagHygiene::Warn,
            },
        };
        assert_eq!(
            g.as_flat_vars().get("qed_release_tag_hygiene").map(String::as_str),
            Some("warn")
        );
    }

    #[test]
    fn as_param_defaults_drops_the_domain_prefix() {
        let g = CampGlobals {
            release: ReleaseGlobals {
                tag_hygiene: TagHygiene::Warn,
            },
        };
        let defaults = g.as_param_defaults();
        assert_eq!(
            defaults.get("release_tag_hygiene").map(String::as_str),
            Some("warn")
        );
        assert!(
            !defaults.contains_key("qed_release_tag_hygiene"),
            "the qed_ domain segment is implicit inside `yah qed run`, not a param name"
        );
    }

    fn release_tag_hygiene_pipeline() -> crate::types::Pipeline {
        let mut params = HashMap::new();
        params.insert(
            "release_tag_hygiene".to_string(),
            crate::types::ParamDef {
                required: false,
                description: None,
                default: Some("block".to_string()),
                options: vec!["block".to_string(), "warn".to_string()],
                options_from: None,
            },
        );
        crate::types::Pipeline {
            name: "version-bump".to_string(),
            params,
            ..Default::default()
        }
    }

    #[test]
    fn resolve_params_fills_from_global_when_operator_supplies_nothing() {
        let g = CampGlobals {
            release: ReleaseGlobals {
                tag_hygiene: TagHygiene::Warn,
            },
        };
        let resolved = g
            .resolve_params(&release_tag_hygiene_pipeline(), &HashMap::new())
            .unwrap();
        assert_eq!(
            resolved.get("release_tag_hygiene").map(String::as_str),
            Some("warn")
        );
    }

    #[test]
    fn resolve_params_lets_an_explicit_param_override_the_global() {
        let g = CampGlobals {
            release: ReleaseGlobals {
                tag_hygiene: TagHygiene::Warn,
            },
        };
        let mut supplied = HashMap::new();
        supplied.insert("release_tag_hygiene".to_string(), "block".to_string());
        let resolved = g
            .resolve_params(&release_tag_hygiene_pipeline(), &supplied)
            .unwrap();
        assert_eq!(
            resolved.get("release_tag_hygiene").map(String::as_str),
            Some("block")
        );
    }
}
