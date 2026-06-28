//! Core types for the TaskSession system.
//!
//! Mirrors the data model in `.yah/docs/working/W136-yah-task-sessions.md`.
//! Intentionally kept free of I/O — the store layer owns persistence.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

// ─── TaskSessionId ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskSessionId(pub Uuid);

impl TaskSessionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TaskSessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TaskSessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for TaskSessionId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

// ─── TaskSessionKind ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskSessionKind {
    Ticket { ticket_id: String },
    Relay { relay_id: String },
    BugSprint { sprint_id: String },
    GnomeShift { shift_id: String, run_n: u32 },
    Adhoc,
}

impl TaskSessionKind {
    pub fn tag(&self) -> &'static str {
        match self {
            TaskSessionKind::Ticket { .. } => "ticket",
            TaskSessionKind::Relay { .. } => "relay",
            TaskSessionKind::BugSprint { .. } => "bug_sprint",
            TaskSessionKind::GnomeShift { .. } => "gnome_shift",
            TaskSessionKind::Adhoc => "adhoc",
        }
    }
}

// ─── SessionStatus ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Active,
    Paused,
    Closed,
    Escalated,
    Abandoned,
}

impl SessionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SessionStatus::Active => "active",
            SessionStatus::Paused => "paused",
            SessionStatus::Closed => "closed",
            SessionStatus::Escalated => "escalated",
            SessionStatus::Abandoned => "abandoned",
        }
    }
}

impl std::str::FromStr for SessionStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(SessionStatus::Active),
            "paused" => Ok(SessionStatus::Paused),
            "closed" => Ok(SessionStatus::Closed),
            "escalated" => Ok(SessionStatus::Escalated),
            "abandoned" => Ok(SessionStatus::Abandoned),
            other => Err(format!("unknown session status: {other}")),
        }
    }
}

// ─── Binding ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Binding {
    Chat { session: String, role: ChatBindingRole },
    Pr { url: String },
    Ticket { id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatBindingRole {
    Driver,
    Witness,
}

impl ChatBindingRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChatBindingRole::Driver => "driver",
            ChatBindingRole::Witness => "witness",
        }
    }
}

// ─── Verdict ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Pass,
    Fail,
    Inconclusive,
}

impl Verdict {
    pub fn as_str(&self) -> &'static str {
        match self {
            Verdict::Pass => "pass",
            Verdict::Fail => "fail",
            Verdict::Inconclusive => "inconclusive",
        }
    }
}

impl std::str::FromStr for Verdict {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pass" => Ok(Verdict::Pass),
            "fail" => Ok(Verdict::Fail),
            "inconclusive" => Ok(Verdict::Inconclusive),
            other => Err(format!("unknown verdict: {other}")),
        }
    }
}

// ─── VerificationRef ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationRef {
    pub task_run: String,
    pub verdict: Verdict,
    pub at: u64,
}

// ─── DiffSummary ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DiffSummary {
    pub files_changed: usize,
    pub insertions: usize,
    pub deletions: usize,
    /// SHA-256 of the accumulated diff content for identity checks.
    pub hash: Option<String>,
}

// ─── EscalationTarget ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EscalationTarget {
    Ticket { id: String },
    Relay { id: String },
    Pr { url: String },
}

// ─── Escalation ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Escalation {
    pub to: EscalationTarget,
    pub successor_session: String,
    pub at: u64,
    pub reason: String,
}

// ─── SessionResult ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionResult {
    pub diff_summary: DiffSummary,
    pub final_verdict: Verdict,
    pub escalation: Option<Escalation>,
}

// ─── TaskSession ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSession {
    pub id: TaskSessionId,
    pub kind: TaskSessionKind,
    pub created_at: u64,
    pub closed_at: Option<u64>,
    pub status: SessionStatus,
    pub label: Option<String>,
    pub working_set: Vec<PathBuf>,
    pub bindings: Vec<Binding>,
    pub escalated_from: Option<TaskSessionId>,
    pub result: Option<SessionResult>,
}

// ─── TicketClaim ──────────────────────────────────────────────────────────────

/// Outcome of an atomic ticket claim (W210 board.claim).
///
/// The claim is conflict-rejecting: at most one live (active) TaskSession may
/// own a ticket. Because the daemon is the single writer, the find-then-create
/// inside [`SessionStore::claim_ticket_session`] is effectively atomic — two
/// pickers racing through the daemon serialize, so exactly one gets `Claimed`
/// and the other gets `Conflict`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TicketClaim {
    /// No live session existed; a fresh one was created and bound to the ticket.
    Claimed(TaskSessionId),
    /// The caller already owns the live session (idempotent re-claim).
    AlreadyOwned(TaskSessionId),
    /// A different live session already owns the ticket — claim rejected.
    Conflict {
        session: TaskSessionId,
        /// The holder's claimant token (`label`), if it recorded one.
        holder: Option<String>,
    },
}

// ─── SessionFilter ────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct SessionFilter {
    pub kind: Option<String>,
    pub status: Option<String>,
    pub since: Option<u64>,
    pub limit: Option<usize>,
}

// ─── ToolCallRef ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRef {
    pub chat_session: String,
    pub turn_seq: u64,
    pub call_seq: u64,
    pub tool_name: String,
}

// ─── CardOutcome ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CardOutcome {
    Accepted,
    Declined,
    EditedThenAccepted,
    Expired,
    Reverted,
}

impl CardOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            CardOutcome::Accepted => "accepted",
            CardOutcome::Declined => "declined",
            CardOutcome::EditedThenAccepted => "edited_then_accepted",
            CardOutcome::Expired => "expired",
            CardOutcome::Reverted => "reverted",
        }
    }
}

/// One row from `session_card_outcomes` returned by `SessionStore::list_card_outcomes`.
#[derive(Debug, Clone)]
pub struct CardOutcomeRow {
    pub card_id: String,
    pub rule_id: Option<String>,
    pub shift_id: Option<String>,
    pub outcome: String,
    pub decline_reason: Option<String>,
    pub decline_note: Option<String>,
    pub decided_at: u64,
    pub decided_by: String,
    pub path: Option<String>,
    pub before_blob: Option<String>,
    pub reverts_card: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeclineReason {
    WrongRemediation,
    RuleNotApplicable,
    Stylistic,
    Unsafe,
    OutOfScope,
}

impl DeclineReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            DeclineReason::WrongRemediation => "wrong_remediation",
            DeclineReason::RuleNotApplicable => "rule_not_applicable",
            DeclineReason::Stylistic => "stylistic",
            DeclineReason::Unsafe => "unsafe",
            DeclineReason::OutOfScope => "out_of_scope",
        }
    }
}

impl std::str::FromStr for DeclineReason {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "wrong_remediation" => Ok(DeclineReason::WrongRemediation),
            "rule_not_applicable" => Ok(DeclineReason::RuleNotApplicable),
            "stylistic" => Ok(DeclineReason::Stylistic),
            "unsafe" => Ok(DeclineReason::Unsafe),
            "out_of_scope" => Ok(DeclineReason::OutOfScope),
            other => Err(format!("unknown decline reason: {other}")),
        }
    }
}
