//! Durable record of bounded work units.
//!
//! Task sessions sit alongside chat sessions: a chat session is *what was said*,
//! a task session is *what was done*. They bind together — a chat can drive a task,
//! a task can spawn a chat.
//!
//! See `.yah/docs/working/W136-yah-task-sessions.md` for the full design.

pub mod store;
pub mod types;

pub use store::{SessionStore, SessionStoreError};
pub use types::{
    Binding, CardOutcome, CardOutcomeRow, ChatBindingRole, DeclineReason, DiffSummary, Escalation,
    EscalationTarget, SessionFilter, SessionResult, SessionStatus, TaskSession, TaskSessionId,
    TaskSessionKind, TicketClaim, ToolCallRef, Verdict, VerificationRef,
};
