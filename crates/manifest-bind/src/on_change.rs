//! Hash-change hooks (W209 § Hash-change hooks, R510-F6).
//!
//! Some outputs trigger downstream work when they *change* — a whisper model
//! BLAKE3 bump should bump the desktop release-manifest version; a container
//! digest change should re-run dependent transforms. The applier already
//! knows which binds changed ([`AppliedBind::changed`]); a pipeline declares
//! `[[on_change]]` hooks that fire **after the bind transaction commits**, in
//! declared order, and **only for binds whose write actually changed bytes**.
//!
//! This module is the firing gate + side-effect dispatch. The gate
//! ([`fired_hooks`]) is a pure function — it is the W209 verification
//! criterion ("fires once on real change, zero times on no-op rewrite") and
//! is unit-tested as such. Dispatch is deliberately leaf-only: `journal` and
//! `event` are file appends this crate owns end-to-end; `pipeline` returns a
//! [`HookOutcome::PipelineRequested`] for the *caller* (the qed runner) to
//! enqueue — manifest-bind has no pipeline runtime and must not grow one
//! (and not auto-cascading pipelines in v1 is the conservative default the
//! design's reserved `rebind_stop` guard is meant to bound).

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::types::{AppliedBind, BindError};

/// Default sink for `event` actions, relative to the workspace root. One
/// JSON object per line so journals/subscribers can tail it.
pub const HOOK_EVENTS_JOURNAL: &str = ".yah/qed/hook-events.jsonl";

/// One `[[on_change]]` table from a pipeline TOML.
///
/// ```toml
/// [[on_change]]
/// bind   = "asset[filename='whisper.tar.gz'].blake3"
/// action = { pipeline = "release.bump-manifest", params = { component = "whisper-coreml" } }
/// ```
///
/// `bind` is matched against an [`AppliedBind::path`] — the same path string
/// a `[[bind]]` declared. A hook with no matching changed bind never fires.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OnChangeHook {
    /// Selector matched against a changed bind's `path`.
    pub bind: String,
    /// What to do when a matching bind changes.
    pub action: OnChangeAction,
}

/// Action variants (W209). Disambiguated by the required key (`pipeline` /
/// `event` / `journal`) so the TOML table form parses without a tag field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum OnChangeAction {
    /// Fire a downstream QED pipeline. The applier records the request; the
    /// runner decides whether/when to enqueue it (no auto-cascade in v1).
    Pipeline {
        pipeline: String,
        #[serde(default)]
        params: BTreeMap<String, String>,
    },
    /// Emit a structured event line for journals/subscribers.
    Event {
        event: String,
        #[serde(default)]
        payload: BTreeMap<String, String>,
    },
    /// Append a human-readable line to a journal file (relative to the
    /// workspace root).
    Journal { journal: String },
}

/// One hook that matched a changed bind, ready to dispatch. Carries the
/// triggering value so `journal` / `event` payloads can describe the change
/// without re-resolving the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiredHook {
    /// The matched bind's path (== `OnChangeHook::bind` == `AppliedBind::path`).
    pub bind: String,
    /// Manifest file whose bind changed.
    pub file: PathBuf,
    /// Value after the bind (the new receipt).
    pub new: String,
    /// Value before the bind, if any.
    pub old: Option<String>,
    /// The action to perform.
    pub action: OnChangeAction,
}

/// What a dispatched hook did. `journal` / `event` are committed to disk by
/// this crate; `pipeline` is handed back for the caller to enqueue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookOutcome {
    Journaled { file: PathBuf },
    EventEmitted { file: PathBuf, kind: String },
    PipelineRequested {
        pipeline: String,
        params: BTreeMap<String, String>,
    },
}

/// The firing gate. For each hook in **declared order**, emit a [`FiredHook`]
/// for every applied bind that (a) actually changed bytes on disk and (b)
/// whose path equals the hook's `bind` selector. No-op rewrites
/// (`changed == false`) never fire — this is the W209 idempotency guarantee.
pub fn fired_hooks(hooks: &[OnChangeHook], applied: &[AppliedBind]) -> Vec<FiredHook> {
    let mut fired = Vec::new();
    for hook in hooks {
        for a in applied {
            if a.changed && a.path == hook.bind {
                fired.push(FiredHook {
                    bind: a.path.clone(),
                    file: a.file.clone(),
                    new: a.new.clone(),
                    old: a.old.clone(),
                    action: hook.action.clone(),
                });
            }
        }
    }
    fired
}

/// Perform a fired hook's side effect. `journal`/`event` append to disk under
/// `workspace_root`; `pipeline` is returned for the caller to enqueue.
pub fn dispatch_hook(
    fired: &FiredHook,
    workspace_root: &Path,
) -> Result<HookOutcome, BindError> {
    match &fired.action {
        OnChangeAction::Journal { journal } => {
            let abs = workspace_root.join(journal);
            let old = fired.old.as_deref().unwrap_or("<unset>");
            let line = format!("bound {} = {} (was {})\n", fired.bind, fired.new, old);
            append_line(&abs, &line)?;
            Ok(HookOutcome::Journaled { file: abs })
        }
        OnChangeAction::Event { event, payload } => {
            let abs = workspace_root.join(HOOK_EVENTS_JOURNAL);
            // Hand-rolled JSON keeps payload key order deterministic (BTreeMap)
            // and avoids pulling serde_json's Value into the hot path.
            let mut obj = String::from("{");
            obj.push_str(&format!("\"event\":{}", json_str(event)));
            obj.push_str(&format!(",\"bind\":{}", json_str(&fired.bind)));
            obj.push_str(&format!(",\"file\":{}", json_str(&fired.file.to_string_lossy())));
            obj.push_str(&format!(",\"new\":{}", json_str(&fired.new)));
            if let Some(old) = &fired.old {
                obj.push_str(&format!(",\"old\":{}", json_str(old)));
            }
            for (k, v) in payload {
                obj.push_str(&format!(",{}:{}", json_str(k), json_str(v)));
            }
            obj.push_str("}\n");
            append_line(&abs, &obj)?;
            Ok(HookOutcome::EventEmitted {
                file: abs,
                kind: event.clone(),
            })
        }
        OnChangeAction::Pipeline { pipeline, params } => Ok(HookOutcome::PipelineRequested {
            pipeline: pipeline.clone(),
            params: params.clone(),
        }),
    }
}

fn append_line(abs: &Path, line: &str) -> Result<(), BindError> {
    if let Some(parent) = abs.parent() {
        fs::create_dir_all(parent).map_err(|e| BindError::Io {
            file: abs.to_path_buf(),
            source: e,
        })?;
    }
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(abs)
        .map_err(|e| BindError::Io {
            file: abs.to_path_buf(),
            source: e,
        })?;
    f.write_all(line.as_bytes()).map_err(|e| BindError::Io {
        file: abs.to_path_buf(),
        source: e,
    })
}

/// Minimal JSON string escaper for the event journal — handles the cases that
/// actually occur in hashes, paths, and short payload values.
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn applied(path: &str, new: &str, changed: bool) -> AppliedBind {
        AppliedBind {
            file: PathBuf::from("app/yah/desktop/assets/whisper/workload.toml"),
            path: path.to_owned(),
            from: "publish.outputs.discovered_asset_blake3".to_owned(),
            old: Some("0".repeat(64)),
            new: new.to_owned(),
            changed,
            cross_workspace: false,
        }
    }

    fn journal_hook(bind: &str, file: &str) -> OnChangeHook {
        OnChangeHook {
            bind: bind.to_owned(),
            action: OnChangeAction::Journal {
                journal: file.to_owned(),
            },
        }
    }

    #[test]
    fn fires_once_on_real_change() {
        let hooks = vec![journal_hook("asset[x].blake3", ".yah/qed/whisper.journal")];
        let applied = vec![applied("asset[x].blake3", &"a".repeat(64), true)];
        let fired = fired_hooks(&hooks, &applied);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].bind, "asset[x].blake3");
        assert_eq!(fired[0].new, "a".repeat(64));
    }

    #[test]
    fn zero_fires_on_noop_rewrite() {
        // changed == false → predicate accepted but the bytes already matched.
        let hooks = vec![journal_hook("asset[x].blake3", ".yah/qed/whisper.journal")];
        let applied = vec![applied("asset[x].blake3", &"a".repeat(64), false)];
        assert!(fired_hooks(&hooks, &applied).is_empty());
    }

    #[test]
    fn unmatched_selector_does_not_fire() {
        let hooks = vec![journal_hook("image", ".yah/qed/x.journal")];
        let applied = vec![applied("asset[x].blake3", &"a".repeat(64), true)];
        assert!(fired_hooks(&hooks, &applied).is_empty());
    }

    #[test]
    fn hooks_fire_in_declared_order() {
        let hooks = vec![
            journal_hook("b", "j1"),
            journal_hook("a", "j2"),
        ];
        let applied = vec![
            applied("a", &"1".repeat(64), true),
            applied("b", &"2".repeat(64), true),
        ];
        let fired = fired_hooks(&hooks, &applied);
        // Declared order is b-then-a, regardless of applied order.
        assert_eq!(fired.iter().map(|f| f.bind.as_str()).collect::<Vec<_>>(), vec!["b", "a"]);
    }

    #[test]
    fn journal_action_appends_line() {
        let dir = tempfile::tempdir().unwrap();
        let fired = FiredHook {
            bind: "asset[x].blake3".to_owned(),
            file: PathBuf::from("workload.toml"),
            new: "a".repeat(64),
            old: Some("0".repeat(64)),
            action: OnChangeAction::Journal {
                journal: ".yah/qed/whisper.journal".to_owned(),
            },
        };
        let out = dispatch_hook(&fired, dir.path()).unwrap();
        let HookOutcome::Journaled { file } = out else {
            panic!("expected Journaled, got {out:?}");
        };
        let body = fs::read_to_string(&file).unwrap();
        assert_eq!(body, format!("bound asset[x].blake3 = {} (was {})\n", "a".repeat(64), "0".repeat(64)));
        // Re-dispatch appends, never truncates.
        dispatch_hook(&fired, dir.path()).unwrap();
        assert_eq!(fs::read_to_string(&file).unwrap().lines().count(), 2);
    }

    #[test]
    fn event_action_appends_json_to_shared_journal() {
        let dir = tempfile::tempdir().unwrap();
        let mut payload = BTreeMap::new();
        payload.insert("component".to_owned(), "whisper-coreml".to_owned());
        let fired = FiredHook {
            bind: "asset[x].blake3".to_owned(),
            file: PathBuf::from("workload.toml"),
            new: "a".repeat(64),
            old: None,
            action: OnChangeAction::Event {
                event: "whisper-changed".to_owned(),
                payload,
            },
        };
        let out = dispatch_hook(&fired, dir.path()).unwrap();
        let HookOutcome::EventEmitted { file, kind } = out else {
            panic!("expected EventEmitted, got {out:?}");
        };
        assert_eq!(kind, "whisper-changed");
        let body = fs::read_to_string(&file).unwrap();
        assert!(body.contains("\"event\":\"whisper-changed\""), "body: {body}");
        assert!(body.contains("\"component\":\"whisper-coreml\""), "body: {body}");
        assert!(body.ends_with('\n'));
    }

    #[test]
    fn pipeline_action_returns_request_without_side_effect() {
        let dir = tempfile::tempdir().unwrap();
        let mut params = BTreeMap::new();
        params.insert("component".to_owned(), "whisper-coreml".to_owned());
        let fired = FiredHook {
            bind: "asset[x].blake3".to_owned(),
            file: PathBuf::from("workload.toml"),
            new: "a".repeat(64),
            old: None,
            action: OnChangeAction::Pipeline {
                pipeline: "release.bump-manifest".to_owned(),
                params: params.clone(),
            },
        };
        let out = dispatch_hook(&fired, dir.path()).unwrap();
        assert_eq!(
            out,
            HookOutcome::PipelineRequested {
                pipeline: "release.bump-manifest".to_owned(),
                params,
            }
        );
        // No file should have been written for a pipeline request.
        assert!(!dir.path().join(HOOK_EVENTS_JOURNAL).exists());
    }

    #[test]
    fn action_variants_round_trip_through_toml() {
        let toml = r#"
[[on_change]]
bind = "asset[x].blake3"
action = { pipeline = "release.bump-manifest", params = { component = "whisper-coreml" } }

[[on_change]]
bind = "image"
action = { event = "digest-changed", payload = { transform = "whisper-bundle-tar" } }

[[on_change]]
bind = "asset[y].blake3"
action = { journal = ".yah/qed/whisper.journal" }
"#;
        #[derive(Deserialize)]
        struct Doc {
            on_change: Vec<OnChangeHook>,
        }
        let doc: Doc = toml::from_str(toml).unwrap();
        assert_eq!(doc.on_change.len(), 3);
        assert!(matches!(doc.on_change[0].action, OnChangeAction::Pipeline { .. }));
        assert!(matches!(doc.on_change[1].action, OnChangeAction::Event { .. }));
        assert!(matches!(doc.on_change[2].action, OnChangeAction::Journal { .. }));
    }
}
