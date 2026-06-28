//! Core types for the TaskRun system.
//!
//! These mirror the data model in `.yah/docs/working/yah-task-runs.md`.
//! Intentionally kept free of I/O — the store layer owns persistence.
//!
//! The types `TaskRunId`, `Level`, `EventSource`, `ChunkRef`, `Event`, and
//! `Diagnostic` live in `crates/yah/observation/` and are re-exported here
//! for backward compatibility.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// Re-export the hoisted observation types so all existing callers continue to
// work via `use task_runs::{TaskRunId, Event, ...}`.
pub use observation::{
    ChunkRef, Diagnostic, Event, EventScope, EventSource, ForgeId, Level, TaskRunId,
    RESERVED_FIELD_PATHS,
};

// ─── RunStatus ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Done { exit_code: i32, ended_at: u64 },
    Killed { signal: i32, ended_at: u64 },
    Lost { reason: String },
}

// ─── Initiator ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Initiator {
    Human { camp: String },
    Agent { camp: String, agent: String, session: String },
    Gnome { camp: String, shift: String },
    Cron { camp: String, schedule: String },
}

// ─── BeholderStatus ───────────────────────────────────────────────────────────

/// Opaque string surfaced on `TaskRunMeta.beholder_status`.
///
/// Examples: `"attached:cargo@1.78"`, `"none:auto"`, `"declined:cargo
/// reason=\"explicit --message-format=human\""`, `"unknown_format"`,
/// `"attached:cargo@1.78 rewrite=\"--message-format=json-render-diagnostics\""`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeholderStatus {
    pub text: String,
    /// Args added to argv by a `Rewriter` beholder. `None` for `Parser` mode or
    /// when no beholder attached.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rewrite_added: Option<Vec<String>>,
}

impl BeholderStatus {
    fn make(text: String) -> Self {
        Self { text, rewrite_added: None }
    }

    pub fn none_auto() -> Self {
        Self::make("none:auto".to_string())
    }
    /// Bytes-only because the caller explicitly set `BeholderSelect::None`.
    pub fn none_explicit() -> Self {
        Self::make("none:explicit".to_string())
    }
    pub fn attached(name: &str, version: &str) -> Self {
        Self::make(format!("attached:{name}@{version}"))
    }
    pub fn declined(name: &str, reason: &str) -> Self {
        Self::make(format!("declined:{name} reason=\"{reason}\""))
    }
    /// `BeholderSelect::Force` matched; `matches` predicate agreed.
    pub fn forced(name: &str, version: &str) -> Self {
        Self::make(format!("forced:{name}@{version}"))
    }
    /// `BeholderSelect::Force` matched; beholder's `matches` would have declined.
    pub fn forced_against_flags(name: &str, version: &str) -> Self {
        Self::make(format!("forced-against-flags:{name}@{version}"))
    }
    /// `BeholderSelect::Force` matched on a TTY-attached run where a Rewriter
    /// beholder would normally decline to preserve human output.
    pub fn forced_against_tty(name: &str, version: &str) -> Self {
        Self::make(format!("forced-against-tty:{name}@{version}"))
    }
    pub fn unknown_format() -> Self {
        Self::make("unknown_format".to_string())
    }
    /// Beholder detected that the tool's output format is unrecognized (schema
    /// drift). Records which beholder made the call and why.
    pub fn unknown_format_with_reason(name: &str, reason: &str) -> Self {
        Self::make(format!("unknown_format:{name} reason=\"{reason}\""))
    }

    /// Append rewrite info to this status if `added` is non-empty.
    ///
    /// Called after a `Rewriter` beholder adjusts argv so agents can see exactly
    /// what was injected into the command line.
    pub fn with_rewrite(mut self, added: Vec<String>) -> Self {
        if !added.is_empty() {
            let repr = added.join(" ");
            self.text = format!("{} rewrite=\"{repr}\"", self.text);
            self.rewrite_added = Some(added);
        }
        self
    }
}

// ─── TaskRunMeta ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRunMeta {
    pub id: TaskRunId,
    pub command: String,
    pub cwd: PathBuf,
    pub env: Vec<(String, String)>,
    pub started_at: u64,
    pub status: RunStatus,
    pub label: Option<String>,
    pub initiator: Initiator,
    pub beholder_status: Option<BeholderStatus>,
    /// If true, the GC sweep will not drop this run's output during warm rolloff.
    /// Pinned runs are exempt until explicitly unpinned or archived.
    #[serde(default)]
    pub pinned: bool,
    /// What surface spawned this run. `None` (the default) is an ordinary
    /// `task.run` job; `Some("terminal")` marks an interactive terminal
    /// session (SSH / local PTY / camp shell). A generic provenance tag, not
    /// a UI concept — it lets a consumer (e.g. the desktop terminal rail) list
    /// just its own runs from `task.list` without scooping up unrelated jobs.
    #[serde(default)]
    pub origin: Option<String>,
}

// ─── Stream ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stream {
    Stdout,
    Stderr,
    Synth,
}

impl Stream {
    pub fn as_str(self) -> &'static str {
        match self {
            Stream::Stdout => "stdout",
            Stream::Stderr => "stderr",
            Stream::Synth => "synth",
        }
    }
}

impl std::str::FromStr for Stream {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "stdout" => Ok(Stream::Stdout),
            "stderr" => Ok(Stream::Stderr),
            "synth" => Ok(Stream::Synth),
            other => Err(format!("unknown stream: {other}")),
        }
    }
}

// ─── OutputChunk ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputChunk {
    pub run_id: TaskRunId,
    pub seq: u32,
    pub offset_ms: u32,
    pub stream: Stream,
    pub bytes: Vec<u8>,
}

// ─── SeqRange ─────────────────────────────────────────────────────────────────

/// Inclusive range over chunk `seq` numbers within a single run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeqRange {
    pub lo: u32,
    pub hi: u32,
}

// ─── Triage (Tier 1.75 — pruner output) ───────────────────────────────────────

/// Pruner output for a run: a list of verbatim chunk ranges + a human-facing
/// synopsis.
///
/// **Agents must read `keep`/`primary` ranges and resolve them to bytes via
/// `task.lines`. The `synopsis` is for human display only — it can paraphrase
/// or hallucinate. Never parse it programmatically.**
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Triage {
    pub run_id: TaskRunId,
    pub synopsis: String,
    pub keep: Vec<KeepRange>,
    pub primary: SeqRange,
    pub model: String,
    pub prompt_version: u32,
    pub cached_at: u64,
    pub partial: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeepRange {
    pub range: SeqRange,
    pub reason: String,
}
