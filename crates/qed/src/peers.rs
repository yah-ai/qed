//! Per-camp peer registry (R494-F1).
//!
//! When a pipeline carries a [`SubPipelineRef::Peer { camp, pipeline }`] step,
//! qed needs to know where that camp lives. The registry lives at
//! `<qed_dir>/peers.toml` (typically `.yah/qed/peers.toml`) and maps
//! registry keys to camp folders on this rig — or, when `rig` is set, to a
//! camp on another rig that kamaji will broker the run to.
//!
//! Shape:
//!
//! ```toml
//! # .yah/qed/peers.toml
//! [peer.mesofact]
//! path = "external/mesofact"          # relative to this camp's root
//!
//! [peer.cheers]
//! path = "external/cheers"
//!
//! [peer.bigbuild]
//! rig  = "rig-tokyo-1"                # remote — kamaji brokers (R494-T5)
//! path = "/srv/camps/bigbuild"
//! ```
//!
//! Resolution rules (R494-F2 wires the runner side):
//!
//! - **Local peer (`rig` unset).** The rig-local camp daemon loads the
//!   peer camp's `.yah/qed/` and runs the named pipeline as a nested
//!   [`QedRun`](crate::types::QedRunId). Same process, same runner, same
//!   DB. There is **one camp-daemon per rig**; "other camps" are just
//!   different folders to that daemon. No IPC.
//! - **Remote peer (`rig` set).** Daemon asks kamaji to broker the
//!   run on the named rig's daemon (R494-T5 stubs this — v1 surfaces an
//!   explicit unsupported error).
//!
//! `yubaba` does **not** enter the resolution path. It only appears if a
//! peer's own pipeline carries an `[[pipeline.on_success]] kind =
//! "yubaba-deploy"` outcome — same as a same-camp pipeline.

use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Per-camp peer registry. Empty by default — the registry exists only
/// when this camp wants to compose pipelines from other camp folders.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PeerConfig {
    #[serde(default)]
    pub peer: HashMap<String, PeerEntry>,
}

/// One peer entry from `peers.toml`. Either local (path only) or remote
/// (`rig` + path on that rig). `path` is mandatory in both cases — for
/// local peers it is relative to *this* camp's root; for remote peers it
/// is an absolute path on the rig.
#[derive(Debug, Clone, Deserialize)]
pub struct PeerEntry {
    pub path: PathBuf,
    /// When set, the peer lives on another rig and resolution goes
    /// through kamaji. v1 surfaces this as an unsupported-error at
    /// step-execution time (R494-T5).
    #[serde(default)]
    pub rig: Option<String>,
}

/// Errors surfaced while loading `peers.toml`. Missing file is **not**
/// an error — it yields a [`PeerConfig::default()`].
#[derive(Debug, Error)]
pub enum PeerConfigError {
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

impl PeerConfig {
    /// Load `<qed_dir>/peers.toml` if present; return an empty config
    /// otherwise. The empty config rejects every [`SubPipelineRef::Peer`]
    /// step at resolution time, which is the right v1 default — operators
    /// opt in by writing the file.
    pub fn load(qed_dir: &Path) -> Result<Self, PeerConfigError> {
        let path = qed_dir.join("peers.toml");
        if !path.exists() {
            return Ok(Self::default());
        }
        let src = fs::read_to_string(&path).map_err(|e| PeerConfigError::Io {
            path: path.display().to_string(),
            source: e,
        })?;
        toml::from_str(&src).map_err(|e| PeerConfigError::Parse {
            path: path.display().to_string(),
            source: e,
        })
    }

    /// Look up a peer by registry key. Returns `None` for unknown peers
    /// — the runner surfaces that as a resolution error at step time.
    pub fn get(&self, camp: &str) -> Option<&PeerEntry> {
        self.peer.get(camp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_yields_empty_config() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = PeerConfig::load(tmp.path()).unwrap();
        assert!(cfg.peer.is_empty());
        assert!(cfg.get("anyone").is_none());
    }

    #[test]
    fn parses_local_and_remote_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let src = r#"
            [peer.mesofact]
            path = "external/mesofact"

            [peer.cheers]
            path = "external/cheers"

            [peer.bigbuild]
            rig  = "rig-tokyo-1"
            path = "/srv/camps/bigbuild"
        "#;
        fs::write(tmp.path().join("peers.toml"), src).unwrap();
        let cfg = PeerConfig::load(tmp.path()).unwrap();
        assert_eq!(cfg.peer.len(), 3);

        let meso = cfg.get("mesofact").unwrap();
        assert_eq!(meso.path, PathBuf::from("external/mesofact"));
        assert!(meso.rig.is_none());

        let big = cfg.get("bigbuild").unwrap();
        assert_eq!(big.rig.as_deref(), Some("rig-tokyo-1"));
        assert_eq!(big.path, PathBuf::from("/srv/camps/bigbuild"));
    }

    #[test]
    fn malformed_toml_surfaces_parse_error() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("peers.toml"), "not = valid = toml").unwrap();
        let err = PeerConfig::load(tmp.path()).unwrap_err();
        assert!(matches!(err, PeerConfigError::Parse { .. }), "got: {err:?}");
    }

    #[test]
    fn entry_without_path_is_a_parse_error() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(
            tmp.path().join("peers.toml"),
            "[peer.broken]\nrig = \"some-rig\"\n",
        )
        .unwrap();
        let err = PeerConfig::load(tmp.path()).unwrap_err();
        assert!(matches!(err, PeerConfigError::Parse { .. }), "got: {err:?}");
    }
}
