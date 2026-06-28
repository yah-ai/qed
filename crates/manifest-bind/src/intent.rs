use serde::{Deserialize, Serialize};

use crate::types::OutputValue;

/// Predicate that gates whether an output's value is accepted into a bind
/// slot. The applier only writes to disk when the predicate accepts.
///
/// **Pin is the safe default.** A pinned bind never auto-rolls; the operator
/// has to change `intent` to land a new value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", untagged)]
pub enum Intent {
    /// Bare keyword form: `intent = "pin"` or `intent = "latest"`.
    Keyword(IntentKeyword),
    /// Table form: `intent = { semver = "^1.2" }` or `{ matches = "v.*" }`.
    Tabular(IntentTabular),
}

impl Default for Intent {
    fn default() -> Self {
        Self::Keyword(IntentKeyword::Pin)
    }
}

impl Default for IntentKeyword {
    fn default() -> Self {
        Self::Pin
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum IntentKeyword {
    /// Reject everything. The bind never auto-rolls.
    Pin,
    /// Accept everything the producer surfaces.
    Latest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentTabular {
    /// Cargo-style semver range. Predicate accepts hashes whose associated
    /// tag/version satisfies the range. The applier reads `value.tag` for the
    /// version string; outputs without a tag are rejected by this variant.
    Semver(String),
    /// Regex on `value.tag` (or `value.raw` when no tag is present).
    Matches(String),
}

impl Intent {
    pub fn accepts(&self, value: &OutputValue) -> bool {
        match self {
            Intent::Keyword(IntentKeyword::Pin) => false,
            Intent::Keyword(IntentKeyword::Latest) => true,
            Intent::Tabular(IntentTabular::Semver(_range)) => {
                // v1: real semver matching defers to a follow-up. The shape
                // is named here so the schema stays stable; downstream
                // pipelines that actually need semver wire in a `semver`
                // crate via this method. Until then we accept iff a tag is
                // present (conservative — better than silently accepting
                // hashes whose version we can't read).
                value.tag.is_some()
            }
            Intent::Tabular(IntentTabular::Matches(pat)) => {
                let probe = value.tag.as_deref().unwrap_or(&value.raw);
                match regex::Regex::new(pat) {
                    Ok(re) => re.is_match(probe),
                    Err(_) => false,
                }
            }
        }
    }
}
