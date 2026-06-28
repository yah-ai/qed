//! Shared App Store Connect API-key plumbing for the Apple adapters
//! (R509-F1 notarize, R509-F5 testflight).
//!
//! Both authenticate to Apple with the **same App Store Connect API key
//! family** — a key id, an issuer UUID, and a `.p8` EC private key — and both
//! materialize the `.p8` to a `0600` scratch file in the form `xcrun`
//! (`notarytool` / `altool`) expects. The slot names and the materialization
//! live here so the two adapters can't drift.

use std::path::{Path, PathBuf};

use crate::runner::RunnerError;

/// App Store Connect API key id (`--key-id` / `--apiKey`).
pub const SLOT_KEY_ID: &str = "APPLE_API_KEY_ID";
/// App Store Connect issuer UUID (`--issuer` / `--apiIssuer`).
pub const SLOT_ISSUER: &str = "APPLE_API_ISSUER";
/// The `.p8` private-key *contents* (materialized to a temp file, never logged).
pub const SLOT_KEY_P8: &str = "APPLE_API_KEY_P8";

/// Materialize the `.p8` private-key contents to a `0600` file under the
/// scratch dir, named `AuthKey_<key_id>.p8` — the filename `notarytool`'s
/// `--key` points at directly, and the name `altool --apiKey <key_id>` resolves
/// inside a private-keys dir. Never logs the contents.
pub fn write_p8_key(work_dir: &Path, key_id: &str, contents: &str) -> Result<PathBuf, RunnerError> {
    let path = work_dir.join(format!("AuthKey_{key_id}.p8"));
    std::fs::write(&path, contents)
        .map_err(|e| RunnerError::Outcome(format!("apple: writing API key file: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| RunnerError::Outcome(format!("apple: chmod 600 API key file: {e}")))?;
    }
    Ok(path)
}
