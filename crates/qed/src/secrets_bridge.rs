//! Loads `~/.yah/qed/secrets.toml` — a per-user name-bridge from GHA secret
//! names to a yah-vault slot (or, as a CI/headless fallback, an env var).
//! R487 follow-up.
//!
//! The file is a flat mapping under `[secrets]`. Values are source URIs:
//!
//! ```toml
//! [secrets]
//! # GHA name              # local source
//! GITHUB_TOKEN            = "vault:github-pat"
//! CF_R2_ACCESS_KEY_ID     = "vault:r2-access-key"
//! CF_R2_SECRET_ACCESS_KEY = "vault:r2-secret-key"
//! DEEPSEEK_API_KEY        = "vault:deepseek-api-key"
//!
//! # Env fallback — for CI/headless hosts that don't carry a vault.
//! NPM_TOKEN               = "env:NPM_TOKEN"
//!
//! # Or-fallback: try vault first, then env on miss.
//! GH_PAT                  = "vault:github-pat|env:GH_PAT_LOCAL"
//! ```
//!
//! Sources today:
//! - `vault:<slot>` — read from the yah credentials vault
//!   ([`keys::KeysStore::get`]). Vault-open failures (no machine key, CI
//!   runner, fresh install) are tolerated — they just count as "not found"
//!   so the env fallback can still resolve.
//! - `env:<VAR>`    — read from `std::env::var(VAR)`.
//! - `<vault:…>|<env:…>` — pipe-joined fallback chain; first source that
//!   yields a value wins. Mixed schemes are fine.
//! - bare literal   — return verbatim. Escape hatch for fixtures /
//!                    non-secret values; DO NOT use for real secrets.
//!
//! Missing file → empty mapping (every `${{ secrets.X }}` evaluates to `""`,
//! matching GHA's behavior for unset secrets — workflows that depend on a
//! secret will fail at their own check or at the consuming step).
//!
//! @yah:relay(R500, "Vault UI: GHA secrets bridge editor + raw slot list")
//! @yah:at(2026-06-10T01:38:49Z)
//! @yah:status(open)
//! @yah:next("R027 covers the curated Settings→API Keys panel for known model/cloud providers; this relay adds the two un-curated surfaces: (a) the raw vault slot dictionary, (b) the GHA-name↔vault-slot bridge that `secrets_bridge.rs` consumes. Same KeysStore underneath; no RBAC, no leases.")
//! @yah:next("Three children: F1 = daemon RPC for slot list/set/delete + secrets.toml read/write, F2 = Settings→Vault pane (raw slot CRUD), F3 = QED→Secrets tab (GHA-name bridge editor + live resolution status, autocompletes slot names from F1).")
//! @yah:next("F2 and F3 both consume F1; F3 also depends on F2's UI patterns. F2 stands alone (useful for any vault user, not just qed-gha).")
//! @yah:gotcha("KeysStore::get returns the plaintext secret — the daemon RPC must NEVER ship values to the renderer, only slot NAMES + presence. The F2 'set' RPC takes a value but the response only echoes the name. R027-T7's single-blob storage already exists; this relay just exposes list/set/delete + a small TOML read/write for secrets.toml.")
//! @arch:see(.yah/docs/architecture/A019-settings-api-keys.md)

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

/// Parsed `~/.yah/qed/secrets.toml`. Missing file is not an error.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SecretsConfig {
    #[serde(default)]
    pub secrets: HashMap<String, String>,
}

impl SecretsConfig {
    /// Path: `<home>/.yah/qed/secrets.toml`. Returns `Default::default()`
    /// when the file or HOME is unavailable.
    pub fn load_default() -> Self {
        let Some(path) = default_path() else {
            return Self::default();
        };
        Self::load_from(&path)
    }

    pub fn load_from(path: &std::path::Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(text) => match toml::from_str::<SecretsConfig>(&text) {
                Ok(cfg) => cfg,
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "qed-gha secrets: parse failed; ignoring file",
                    );
                    Self::default()
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "qed-gha secrets: read failed; ignoring file",
                );
                Self::default()
            }
        }
    }

    /// Build the `secrets.*` `Value::Object` the qed-gha executor expects.
    /// Each declared GHA secret name is resolved through its source URI
    /// using the canonical yah vault; unresolved sources surface as `""`
    /// (matches GHA's unset behavior).
    pub fn resolve_all(&self) -> yah_qed_gha::Value {
        // Open the vault once per resolve — `KeysStore::get` is a small
        // file read + AES-GCM decrypt of a usually-tiny credentials.enc,
        // so the open cost amortizes across N lookups.
        let vault = fob::KeysStore::open().ok();
        let mut out: indexmap::IndexMap<String, yah_qed_gha::Value> = indexmap::IndexMap::new();
        for (gha_name, source) in &self.secrets {
            let value = resolve_source(source, vault.as_ref()).unwrap_or_default();
            out.insert(gha_name.clone(), yah_qed_gha::Value::String(value));
        }
        yah_qed_gha::Value::Object(out)
    }

    /// Resolve a single declared secret by its bridged name, running the same
    /// `vault:` / `env:` / pipe-fallback chain as [`resolve_all`]. Returns
    /// `None` when the name isn't declared in the bridge, or when its source
    /// chain yields nothing. Opens the vault once per call — fine for the
    /// handful of credential slots a release-provider adapter reads at publish
    /// time (R509). For resolving the whole bridge at once, prefer
    /// [`resolve_all`], which amortizes the vault open across all entries.
    pub fn resolve_one(&self, name: &str) -> Option<String> {
        let source = self.secrets.get(name)?;
        let vault = fob::KeysStore::open().ok();
        resolve_source(source, vault.as_ref())
    }

    /// Names of declared GHA secrets (sorted). Used by the desktop
    /// vault-explorer to show which workflow names are bridged + which
    /// vault slots they map to.
    pub fn names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.secrets.keys().cloned().collect();
        v.sort();
        v
    }

    /// Per-entry presence report — runs the same fallback chain as
    /// [`resolve_all`] but reports only whether each entry resolves to a
    /// non-empty value, never the value itself. Sorted by GHA name so
    /// the editor UI gets a stable row order. (R500-F1)
    pub fn resolve_status(&self) -> Vec<EntryStatus> {
        let vault = fob::KeysStore::open().ok();
        let mut out: Vec<EntryStatus> = self
            .secrets
            .iter()
            .map(|(name, source)| {
                let resolved = resolve_source(source, vault.as_ref())
                    .map(|v| !v.is_empty())
                    .unwrap_or(false);
                EntryStatus {
                    name: name.clone(),
                    source: source.clone(),
                    resolved,
                }
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }
}

/// Per-entry resolution status from [`SecretsConfig::resolve_status`].
/// `resolved` carries presence only — the actual value never leaves the
/// daemon process.
#[derive(Debug, Clone)]
pub struct EntryStatus {
    pub name: String,
    pub source: String,
    pub resolved: bool,
}

/// Atomically write `entries` as the `[secrets]` table of `path`. Tempfile
/// + rename in the same directory so partially-written files are never
/// visible to readers. Creates the parent directory if missing. Empty
/// `entries` writes an empty `[secrets]` table (well-formed; loads back
/// as an empty mapping). (R500-F1)
pub fn save_to(
    path: &std::path::Path,
    entries: &std::collections::BTreeMap<String, String>,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut buf = String::with_capacity(64 + entries.len() * 64);
    buf.push_str("[secrets]\n");
    for (name, source) in entries {
        // Authored TOML keys can already include `[a-zA-Z0-9_-]+`
        // bare; anything else (a name with a dot, say) needs quoting.
        // GHA secret names in practice are uppercase + underscores;
        // err on the side of always-quoting to keep the writer total.
        buf.push_str(&format!(
            "{} = {}\n",
            quote_toml_key(name),
            quote_toml_str(source)
        ));
    }
    let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let tmp_name = format!(
        ".{}.tmp",
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("secrets.toml")
    );
    let tmp = dir.join(tmp_name);
    std::fs::write(&tmp, buf.as_bytes())?;
    std::fs::rename(&tmp, path)
}

fn quote_toml_key(k: &str) -> String {
    let bare_ok = !k.is_empty()
        && k.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if bare_ok {
        k.to_string()
    } else {
        quote_toml_str(k)
    }
}

fn quote_toml_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Resolve a single source URI to its current string value, using `vault`
/// for `vault:<slot>` lookups. Returns `None` when nothing resolved (the
/// caller folds that to `""` so undefined secrets read empty, matching GHA).
fn resolve_source(source: &str, vault: Option<&fob::KeysStore>) -> Option<String> {
    // Pipe-joined fallback chain: try each alternative in order until one
    // yields a Some. Same precedence as shell `${VAR:-${OTHER:-…}}` — the
    // first non-empty wins.
    for alt in source.split('|') {
        let alt = alt.trim();
        if alt.is_empty() {
            continue;
        }
        if let Some(slot) = alt.strip_prefix("vault:") {
            if let Some(v) = vault {
                match v.get(slot) {
                    Ok(Some(value)) if !value.is_empty() => return Some(value),
                    Ok(_) => continue,
                    Err(e) => {
                        // Lenient: a corrupt or partially-set vault on
                        // this host shouldn't blanket-fail all secrets
                        // (env fallbacks should still work). Log + drop.
                        tracing::warn!(
                            slot = %slot,
                            error = %e,
                            "qed-gha secrets: vault read failed; trying next fallback",
                        );
                        continue;
                    }
                }
            }
            // No vault on this host (no machine.key — common on CI /
            // fresh installs). Fall through to next alt.
            continue;
        }
        if let Some(var) = alt.strip_prefix("env:") {
            if let Ok(value) = std::env::var(var) {
                if !value.is_empty() {
                    return Some(value);
                }
            }
            continue;
        }
        if alt.starts_with("keystore://") {
            // Reserved for future OS keystore (the URI scheme `cloud::config`
            // uses for `credentials = "keystore://cloudflare/yah"`). No
            // resolver yet — warn + treat as unset.
            tracing::warn!(
                source = %alt,
                "qed-gha secrets: keystore:// scheme is reserved; no resolver yet — trying next fallback",
            );
            continue;
        }
        // Bare literal escape hatch (fixtures / known-non-secret values).
        return Some(alt.to_string());
    }
    None
}

/// Canonical path for the bridge file: `~/.yah/qed/secrets.toml`. `None`
/// when HOME is unset (CI containers without a $HOME). Exposed so callers
/// that need to read **and** write the file share the same path.
pub fn default_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    Some(
        PathBuf::from(home)
            .join(".yah")
            .join("qed")
            .join("secrets.toml"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_yields_empty_mapping() {
        let cfg = SecretsConfig::load_from(std::path::Path::new("/nonexistent/secrets.toml"));
        assert!(cfg.secrets.is_empty());
    }

    #[test]
    fn env_scheme_resolves_via_env_var() {
        let key = "QED_SECRETS_TEST_VAR_4F8A";
        std::env::set_var(key, "hunter2");
        let mut cfg = SecretsConfig::default();
        cfg.secrets
            .insert("GITHUB_TOKEN".into(), format!("env:{key}"));
        let v = cfg.resolve_all();
        assert_eq!(string_at(&v, "GITHUB_TOKEN").as_deref(), Some("hunter2"));
        std::env::remove_var(key);
    }

    #[test]
    fn unresolved_env_var_yields_empty_string() {
        let mut cfg = SecretsConfig::default();
        cfg.secrets.insert(
            "GITHUB_TOKEN".into(),
            "env:DEFINITELY_NOT_SET_QED_X92".into(),
        );
        let v = cfg.resolve_all();
        // Unresolved → empty string (matches GHA's unset-secret behavior).
        assert_eq!(string_at(&v, "GITHUB_TOKEN").as_deref(), Some(""));
    }

    #[test]
    fn pipe_chain_falls_back_to_env_when_vault_misses() {
        let key = "QED_SECRETS_FALLBACK_VAR_AAAA";
        std::env::set_var(key, "from-env");
        let mut cfg = SecretsConfig::default();
        // Vault may or may not exist on this host; either way the slot
        // `nonexistent-slot-1f2e` won't resolve, so the pipe chain falls
        // through to the env source.
        cfg.secrets.insert(
            "GH_PAT".into(),
            format!("vault:nonexistent-slot-1f2e|env:{key}"),
        );
        let v = cfg.resolve_all();
        assert_eq!(string_at(&v, "GH_PAT").as_deref(), Some("from-env"));
        std::env::remove_var(key);
    }

    #[test]
    fn parses_a_secrets_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.toml");
        std::fs::write(
            &path,
            r#"
[secrets]
GITHUB_TOKEN = "vault:github-pat"
CF_R2_ACCESS_KEY = "vault:r2-access-key|env:CF_R2_ACCESS_KEY"
"#,
        )
        .unwrap();
        let cfg = SecretsConfig::load_from(&path);
        assert_eq!(cfg.secrets.len(), 2);
        assert_eq!(
            cfg.secrets.get("GITHUB_TOKEN").map(|s| s.as_str()),
            Some("vault:github-pat"),
        );
        assert_eq!(cfg.names(), vec!["CF_R2_ACCESS_KEY", "GITHUB_TOKEN"]);
    }

    #[test]
    fn save_to_roundtrips_through_load_from() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.toml");
        let mut entries = std::collections::BTreeMap::new();
        entries.insert("GITHUB_TOKEN".into(), "vault:github-pat".into());
        entries.insert("GH_PAT".into(), "vault:github-pat|env:GH_PAT_LOCAL".into());
        save_to(&path, &entries).unwrap();
        let cfg = SecretsConfig::load_from(&path);
        assert_eq!(cfg.secrets.len(), 2);
        assert_eq!(
            cfg.secrets.get("GITHUB_TOKEN").map(|s| s.as_str()),
            Some("vault:github-pat"),
        );
        assert_eq!(
            cfg.secrets.get("GH_PAT").map(|s| s.as_str()),
            Some("vault:github-pat|env:GH_PAT_LOCAL"),
        );
    }

    #[test]
    fn save_to_quotes_special_chars_in_source() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.toml");
        let mut entries = std::collections::BTreeMap::new();
        entries.insert("TRICKY".into(), "bare \"quoted\" \\and\\ slashed".into());
        save_to(&path, &entries).unwrap();
        let cfg = SecretsConfig::load_from(&path);
        assert_eq!(
            cfg.secrets.get("TRICKY").map(|s| s.as_str()),
            Some("bare \"quoted\" \\and\\ slashed"),
        );
    }

    #[test]
    fn resolve_status_reports_presence_not_values() {
        let key = "QED_SECRETS_STATUS_VAR_BBBB";
        std::env::set_var(key, "present");
        let mut cfg = SecretsConfig::default();
        cfg.secrets.insert("PRESENT".into(), format!("env:{key}"));
        cfg.secrets
            .insert("ABSENT".into(), "env:DEFINITELY_NOT_SET_QED_X93".into());
        let report = cfg.resolve_status();
        assert_eq!(report.len(), 2);
        // Sorted by name: ABSENT, PRESENT.
        assert_eq!(report[0].name, "ABSENT");
        assert!(!report[0].resolved);
        assert_eq!(report[1].name, "PRESENT");
        assert!(report[1].resolved);
        // The status report never carries the actual value — only
        // (name, source, bool). Compile-time guaranteed by the struct
        // shape; this assertion just documents the invariant.
        assert_eq!(report[1].source, format!("env:{key}"));
        std::env::remove_var(key);
    }

    fn string_at(v: &yah_qed_gha::Value, key: &str) -> Option<String> {
        match v {
            yah_qed_gha::Value::Object(m) => m.get(key).and_then(|x| match x {
                yah_qed_gha::Value::String(s) => Some(s.clone()),
                _ => None,
            }),
            _ => None,
        }
    }
}
