//! Per-camp Turso store for TaskSession records.
//!
//! One `SessionStore` per daemon instance; callers share it via `Arc<SessionStore>`.
//! Backed by `turso` (in-process, async) per W195 §Engine. Writes serialize through
//! `turso::Connection`'s internal synchronization — the explicit `Mutex` that wrapped
//! the old rusqlite `Connection` is gone.
//!
//! Storage contract (W195 §3 / Shape 1): this store owns
//! `.yah/db/task-sessions.turso` under the camp daemon.

use crate::types::{
    Binding, CardOutcome, CardOutcomeRow, ChatBindingRole, DeclineReason, EscalationTarget,
    SessionFilter, SessionResult, SessionStatus, TaskSession, TaskSessionId, TaskSessionKind,
    TicketClaim, ToolCallRef, Verdict,
};
use std::path::Path;
use thiserror::Error;
use turso::{params, params_from_iter, Builder, Connection, Value};

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum SessionStoreError {
    #[error("turso: {0}")]
    Sql(#[from] turso::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse: {0}")]
    Parse(String),
    #[error("escalation cycle detected")]
    EscalationCycle,
}

// ─── Schema ───────────────────────────────────────────────────────────────────

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS task_sessions (
  id              TEXT PRIMARY KEY,
  kind_tag        TEXT NOT NULL CHECK (kind_tag IN
                    ('ticket', 'relay', 'bug_sprint', 'gnome_shift', 'adhoc')),
  kind_payload    TEXT,
  created_at      INTEGER NOT NULL,
  closed_at       INTEGER,
  status          TEXT NOT NULL CHECK (status IN
                    ('active', 'paused', 'closed', 'escalated', 'abandoned')),
  label           TEXT,
  working_set     TEXT NOT NULL,
  escalated_from  TEXT,
  result          TEXT
);
CREATE INDEX IF NOT EXISTS task_sessions_by_status ON task_sessions(status);
CREATE INDEX IF NOT EXISTS task_sessions_by_kind ON task_sessions(kind_tag);
CREATE INDEX IF NOT EXISTS task_sessions_by_predecessor ON task_sessions(escalated_from);

CREATE TABLE IF NOT EXISTS session_bindings (
  session_id   TEXT NOT NULL,
  binding_kind TEXT NOT NULL CHECK (binding_kind IN ('chat', 'pr', 'ticket')),
  binding_role TEXT CHECK (binding_role IN ('driver', 'witness') OR binding_role IS NULL),
  payload      TEXT NOT NULL,
  PRIMARY KEY (session_id, binding_kind, payload)
);

CREATE TABLE IF NOT EXISTS session_runs (
  session_id   TEXT NOT NULL,
  run_id       TEXT NOT NULL,
  is_verify    INTEGER NOT NULL CHECK (is_verify IN (0, 1)),
  verdict      TEXT CHECK (verdict IN ('pass', 'fail', 'inconclusive') OR verdict IS NULL),
  noted_at     INTEGER NOT NULL,
  PRIMARY KEY (session_id, run_id)
);

CREATE TABLE IF NOT EXISTS session_tool_calls (
  session_id   TEXT NOT NULL,
  chat_session TEXT NOT NULL,
  turn_seq     INTEGER NOT NULL,
  call_seq     INTEGER NOT NULL,
  tool_name    TEXT NOT NULL,
  noted_at     INTEGER NOT NULL,
  PRIMARY KEY (session_id, chat_session, turn_seq, call_seq)
);

CREATE TABLE IF NOT EXISTS session_card_outcomes (
  session_id     TEXT NOT NULL,
  card_id        TEXT NOT NULL,
  rule_id        TEXT,
  shift_id       TEXT,
  outcome        TEXT NOT NULL CHECK (outcome IN
                   ('accepted', 'declined', 'edited_then_accepted', 'expired', 'reverted')),
  decline_reason TEXT CHECK (decline_reason IN
                   ('wrong_remediation', 'rule_not_applicable',
                    'stylistic', 'unsafe', 'out_of_scope') OR decline_reason IS NULL),
  decline_note   TEXT,
  decided_at     INTEGER NOT NULL,
  decided_by     TEXT NOT NULL,
  path           TEXT,
  before_blob    TEXT,
  reverts_card   TEXT,
  PRIMARY KEY (session_id, card_id)
);
CREATE INDEX IF NOT EXISTS session_card_outcomes_by_rule
  ON session_card_outcomes(rule_id, decided_at);
CREATE INDEX IF NOT EXISTS session_card_outcomes_by_shift
  ON session_card_outcomes(shift_id, decided_at);
CREATE INDEX IF NOT EXISTS session_card_outcomes_by_session
  ON session_card_outcomes(session_id, decided_at);
"#;

// ─── SessionStore ─────────────────────────────────────────────────────────────

pub struct SessionStore {
    conn: Connection,
}

impl SessionStore {
    pub async fn open(path: &Path) -> Result<Self, SessionStoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Builder::new_local(path.to_string_lossy().as_ref())
            .build()
            .await?;
        let conn = db.connect()?;
        conn.execute_batch(SCHEMA).await?;
        Ok(Self { conn })
    }

    /// Open an in-memory store for testing.
    #[cfg(test)]
    pub async fn open_in_memory() -> Result<Self, SessionStoreError> {
        let db = Builder::new_local(":memory:").build().await?;
        let conn = db.connect()?;
        conn.execute_batch(SCHEMA).await?;
        Ok(Self { conn })
    }

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    // ── Create / lifecycle ────────────────────────────────────────────────────

    pub async fn create_session(
        &self,
        kind: TaskSessionKind,
        label: Option<String>,
        escalated_from: Option<&TaskSessionId>,
    ) -> Result<TaskSessionId, SessionStoreError> {
        let id = TaskSessionId::new();
        let kind_tag = kind.tag();
        let kind_payload = match &kind {
            TaskSessionKind::Adhoc => None,
            _ => Some(serde_json::to_string(&kind)?),
        };
        let now = Self::now_ms();
        let working_set = "[]";
        self.conn
            .execute(
                "INSERT INTO task_sessions (id, kind_tag, kind_payload, created_at, status,
                 label, working_set, escalated_from)
                 VALUES (?1, ?2, ?3, ?4, 'active', ?5, ?6, ?7)",
                params![
                    id.to_string(),
                    kind_tag.to_string(),
                    kind_payload,
                    now as i64,
                    label,
                    working_set.to_string(),
                    escalated_from.map(|x| x.to_string()),
                ],
            )
            .await?;
        Ok(id)
    }

    pub async fn pause_session(&self, id: &TaskSessionId) -> Result<(), SessionStoreError> {
        let n = self
            .conn
            .execute(
                "UPDATE task_sessions SET status='paused' WHERE id=?1 AND status='active'",
                params![id.to_string()],
            )
            .await?;
        if n == 0 {
            return Err(SessionStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn resume_session(&self, id: &TaskSessionId) -> Result<(), SessionStoreError> {
        let n = self
            .conn
            .execute(
                "UPDATE task_sessions SET status='active' WHERE id=?1 AND status='paused'",
                params![id.to_string()],
            )
            .await?;
        if n == 0 {
            return Err(SessionStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn close_session(
        &self,
        id: &TaskSessionId,
        result: Option<SessionResult>,
    ) -> Result<(), SessionStoreError> {
        let now = Self::now_ms();
        let result_json = result.map(|r| serde_json::to_string(&r)).transpose()?;
        let n = self
            .conn
            .execute(
                "UPDATE task_sessions SET status='closed', closed_at=?2, result=?3
                 WHERE id=?1 AND status IN ('active','paused')",
                params![id.to_string(), now as i64, result_json],
            )
            .await?;
        if n == 0 {
            return Err(SessionStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn abandon_session(&self, id: &TaskSessionId) -> Result<(), SessionStoreError> {
        let now = Self::now_ms();
        let n = self
            .conn
            .execute(
                "UPDATE task_sessions SET status='abandoned', closed_at=?2
                 WHERE id=?1 AND status IN ('active','paused')",
                params![id.to_string(), now as i64],
            )
            .await?;
        if n == 0 {
            return Err(SessionStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    /// Mark a session as escalated and link the successor.
    ///
    /// Checks that `id` does not appear in `successor`'s ancestor chain
    /// to prevent cycles (rule: no cycles in escalation chain).
    pub async fn escalate_session(
        &self,
        id: &TaskSessionId,
        target: EscalationTarget,
        successor_session: &TaskSessionId,
        reason: String,
    ) -> Result<(), SessionStoreError> {
        // Cycle check: successor must not have id in its ancestor chain.
        self.check_no_escalation_cycle(successor_session, id).await?;

        let now = Self::now_ms();
        let escalation = crate::types::Escalation {
            to: target,
            successor_session: successor_session.to_string(),
            at: now,
            reason,
        };
        let result = SessionResult {
            diff_summary: Default::default(),
            final_verdict: Verdict::Inconclusive,
            escalation: Some(escalation),
        };
        let result_json = serde_json::to_string(&result)?;
        let n = self
            .conn
            .execute(
                "UPDATE task_sessions SET status='escalated', closed_at=?2, result=?3
                 WHERE id=?1 AND status IN ('active','paused')",
                params![id.to_string(), now as i64, result_json],
            )
            .await?;
        if n == 0 {
            return Err(SessionStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    /// Walk `escalated_from` chain from `start`; return Err if `forbidden` appears.
    async fn check_no_escalation_cycle(
        &self,
        start: &TaskSessionId,
        forbidden: &TaskSessionId,
    ) -> Result<(), SessionStoreError> {
        let mut current = start.to_string();
        let forbidden_str = forbidden.to_string();
        for _ in 0..1000 {
            if current == forbidden_str {
                return Err(SessionStoreError::EscalationCycle);
            }
            let mut rows = self
                .conn
                .query(
                    "SELECT escalated_from FROM task_sessions WHERE id=?1",
                    params![current.clone()],
                )
                .await?;
            let parent: Option<String> = match rows.next().await? {
                Some(row) => row.get(0)?,
                None => return Ok(()),
            };
            match parent {
                None => return Ok(()),
                Some(p) => current = p,
            }
        }
        Ok(())
    }

    // ── Bind ──────────────────────────────────────────────────────────────────

    pub async fn bind_session(
        &self,
        id: &TaskSessionId,
        binding: &Binding,
    ) -> Result<(), SessionStoreError> {
        let (kind, role, payload) = match binding {
            Binding::Chat { session, role } => (
                "chat",
                Some(role.as_str().to_string()),
                session.clone(),
            ),
            Binding::Pr { url } => ("pr", None, url.clone()),
            Binding::Ticket { id } => ("ticket", None, id.clone()),
        };
        self.conn
            .execute(
                "INSERT OR REPLACE INTO session_bindings (session_id, binding_kind, binding_role, payload)
                 VALUES (?1, ?2, ?3, ?4)",
                params![id.to_string(), kind.to_string(), role, payload],
            )
            .await?;
        Ok(())
    }

    // ── Note tool call ────────────────────────────────────────────────────────

    pub async fn note_tool_call(
        &self,
        id: &TaskSessionId,
        tool_call: &ToolCallRef,
    ) -> Result<(), SessionStoreError> {
        let now = Self::now_ms();
        self.conn
            .execute(
                "INSERT OR IGNORE INTO session_tool_calls
                 (session_id, chat_session, turn_seq, call_seq, tool_name, noted_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    id.to_string(),
                    tool_call.chat_session.clone(),
                    tool_call.turn_seq as i64,
                    tool_call.call_seq as i64,
                    tool_call.tool_name.clone(),
                    now as i64,
                ],
            )
            .await?;
        Ok(())
    }

    // ── Note run ──────────────────────────────────────────────────────────────

    pub async fn note_run(
        &self,
        id: &TaskSessionId,
        run_id: &str,
        is_verify: bool,
    ) -> Result<(), SessionStoreError> {
        let now = Self::now_ms();
        self.conn
            .execute(
                "INSERT OR IGNORE INTO session_runs
                 (session_id, run_id, is_verify, noted_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    id.to_string(),
                    run_id.to_string(),
                    is_verify as i64,
                    now as i64,
                ],
            )
            .await?;
        Ok(())
    }

    // ── Note verify ───────────────────────────────────────────────────────────

    pub async fn note_verify(
        &self,
        id: &TaskSessionId,
        run_id: &str,
        verdict: Verdict,
    ) -> Result<(), SessionStoreError> {
        // Upsert: mark verify=1 and set verdict.
        self.conn
            .execute(
                "INSERT INTO session_runs (session_id, run_id, is_verify, verdict, noted_at)
                 VALUES (?1, ?2, 1, ?3, ?4)
                 ON CONFLICT(session_id, run_id) DO UPDATE SET
                   is_verify=1, verdict=excluded.verdict",
                params![
                    id.to_string(),
                    run_id.to_string(),
                    verdict.as_str().to_string(),
                    Self::now_ms() as i64,
                ],
            )
            .await?;
        Ok(())
    }

    // ── Read ──────────────────────────────────────────────────────────────────

    pub async fn get_session(&self, id: &TaskSessionId) -> Result<TaskSession, SessionStoreError> {
        let mut rows = self
            .conn
            .query(
                "SELECT id, kind_tag, kind_payload, created_at, closed_at, status,
                        label, working_set, escalated_from, result
                 FROM task_sessions WHERE id=?1",
                params![id.to_string()],
            )
            .await?;
        let row = rows
            .next()
            .await?
            .ok_or_else(|| SessionStoreError::NotFound(id.to_string()))?;

        let sid: String = row.get(0)?;
        let kind_tag: String = row.get(1)?;
        let kind_payload: Option<String> = row.get(2)?;
        let created_at: i64 = row.get(3)?;
        let closed_at: Option<i64> = row.get(4)?;
        let status_str: String = row.get(5)?;
        let label: Option<String> = row.get(6)?;
        let working_set_json: String = row.get(7)?;
        let escalated_from: Option<String> = row.get(8)?;
        let result_json: Option<String> = row.get(9)?;

        let bindings = self.load_bindings_inner(&sid).await?;

        let kind = parse_kind(&kind_tag, kind_payload.as_deref())?;
        let status = status_str
            .parse::<SessionStatus>()
            .map_err(SessionStoreError::Parse)?;
        let working_set = serde_json::from_str(&working_set_json).unwrap_or_default();
        let result = result_json
            .map(|r| serde_json::from_str::<crate::types::SessionResult>(&r))
            .transpose()?;
        let escalated_from = escalated_from
            .map(|s| s.parse::<TaskSessionId>())
            .transpose()
            .map_err(|e| SessionStoreError::Parse(e.to_string()))?;

        Ok(TaskSession {
            id: id.clone(),
            kind,
            created_at: created_at as u64,
            closed_at: closed_at.map(|x| x as u64),
            status,
            label,
            working_set,
            bindings,
            escalated_from,
            result,
        })
    }

    pub async fn list_sessions(
        &self,
        filter: &SessionFilter,
    ) -> Result<Vec<TaskSession>, SessionStoreError> {
        // Use NULL-guard pattern so we always pass exactly 3 params.
        let mut sql = String::from(
            "SELECT id, kind_tag, kind_payload, created_at, closed_at, status,
                    label, working_set, escalated_from, result
             FROM task_sessions
             WHERE (?1 IS NULL OR kind_tag = ?1)
               AND (?2 IS NULL OR status = ?2)
               AND (?3 IS NULL OR created_at >= ?3)
             ORDER BY created_at DESC",
        );
        if let Some(limit) = filter.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        let p: Vec<Value> = vec![
            filter
                .kind
                .as_deref()
                .map(|s| Value::Text(s.to_string()))
                .unwrap_or(Value::Null),
            filter
                .status
                .as_deref()
                .map(|s| Value::Text(s.to_string()))
                .unwrap_or(Value::Null),
            filter
                .since
                .map(|s| Value::Integer(s as i64))
                .unwrap_or(Value::Null),
        ];

        let mut rows = self.conn.query(&sql, params_from_iter(p)).await?;
        let mut session_rows: Vec<(
            String,
            String,
            Option<String>,
            i64,
            Option<i64>,
            String,
            Option<String>,
            String,
            Option<String>,
            Option<String>,
        )> = Vec::new();
        while let Some(row) = rows.next().await? {
            session_rows.push((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
            ));
        }

        let mut sessions = Vec::with_capacity(session_rows.len());
        for (sid, kind_tag, kind_payload, created_at, closed_at, status_str, label,
             working_set_json, escalated_from, result_json) in session_rows
        {
            let bindings = self.load_bindings_inner(&sid).await?;
            let id = sid
                .parse::<TaskSessionId>()
                .map_err(|e| SessionStoreError::Parse(e.to_string()))?;
            let kind = parse_kind(&kind_tag, kind_payload.as_deref())?;
            let status = status_str
                .parse::<SessionStatus>()
                .map_err(SessionStoreError::Parse)?;
            let working_set = serde_json::from_str(&working_set_json).unwrap_or_default();
            let result = result_json
                .map(|r| serde_json::from_str::<crate::types::SessionResult>(&r))
                .transpose()?;
            let escalated_from = escalated_from
                .map(|s| s.parse::<TaskSessionId>())
                .transpose()
                .map_err(|e| SessionStoreError::Parse(e.to_string()))?;
            sessions.push(TaskSession {
                id,
                kind,
                created_at: created_at as u64,
                closed_at: closed_at.map(|x| x as u64),
                status,
                label,
                working_set,
                bindings,
                escalated_from,
                result,
            });
        }
        Ok(sessions)
    }

    async fn load_bindings_inner(
        &self,
        session_id: &str,
    ) -> Result<Vec<Binding>, SessionStoreError> {
        let mut rows = self
            .conn
            .query(
                "SELECT binding_kind, binding_role, payload FROM session_bindings
                 WHERE session_id=?1",
                params![session_id.to_string()],
            )
            .await?;
        let mut bindings = Vec::new();
        while let Some(row) = rows.next().await? {
            let kind: String = row.get(0)?;
            let role: Option<String> = row.get(1)?;
            let payload: String = row.get(2)?;
            let binding = match kind.as_str() {
                "chat" => {
                    let role = match role.as_deref() {
                        Some("driver") => ChatBindingRole::Driver,
                        _ => ChatBindingRole::Witness,
                    };
                    Binding::Chat { session: payload, role }
                }
                "pr" => Binding::Pr { url: payload },
                _ => Binding::Ticket { id: payload },
            };
            bindings.push(binding);
        }
        Ok(bindings)
    }

    /// Find the active TaskSession bound to `ticket_id`.
    /// Returns the session id, or `None` if no such active session exists.
    pub async fn find_active_for_ticket(
        &self,
        ticket_id: &str,
    ) -> Result<Option<TaskSessionId>, SessionStoreError> {
        let mut rows = self
            .conn
            .query(
                "SELECT ts.id FROM task_sessions ts
                 JOIN session_bindings sb ON sb.session_id = ts.id
                 WHERE ts.kind_tag = 'ticket'
                   AND ts.status = 'active'
                   AND sb.binding_kind = 'ticket'
                   AND sb.payload = ?1
                 LIMIT 1",
                params![ticket_id.to_string()],
            )
            .await?;
        let id = match rows.next().await? {
            Some(row) => {
                let s: String = row.get(0)?;
                Some(s)
            }
            None => None,
        };
        id.map(|s| {
            s.parse::<TaskSessionId>()
                .map_err(|e| SessionStoreError::Parse(e.to_string()))
        })
        .transpose()
    }

    /// List every ticket ID that currently has an Active Ticket session.
    ///
    /// The whole-board analogue of [`find_active_for_ticket`]: one query that
    /// folds all live ticket ownership into a set the board layer derives
    /// columns from (W210). A ticket in this set is `active` regardless of its
    /// source `@yah:status`, so it can't appear in the picker's `handoff`/`open`
    /// view (collision-free dispatch).
    pub async fn list_active_ticket_ids(&self) -> Result<Vec<String>, SessionStoreError> {
        let mut rows = self
            .conn
            .query(
                "SELECT DISTINCT sb.payload FROM task_sessions ts
                 JOIN session_bindings sb ON sb.session_id = ts.id
                 WHERE ts.kind_tag = 'ticket'
                   AND ts.status = 'active'
                   AND sb.binding_kind = 'ticket'",
                (),
            )
            .await?;
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await? {
            ids.push(row.get::<String>(0)?);
        }
        Ok(ids)
    }

    /// Find the ticket binding for a session — the inverse of `find_active_for_ticket`.
    /// Returns the ticket ID string bound to this session, or `None` if the session
    /// has no ticket binding.
    pub async fn find_ticket_for_session(
        &self,
        session_id: &TaskSessionId,
    ) -> Result<Option<String>, SessionStoreError> {
        let mut rows = self
            .conn
            .query(
                "SELECT sb.payload FROM session_bindings sb
                 WHERE sb.session_id = ?1
                   AND sb.binding_kind = 'ticket'
                 LIMIT 1",
                params![session_id.to_string()],
            )
            .await?;
        let payload: Option<String> = match rows.next().await? {
            Some(row) => Some(row.get(0)?),
            None => None,
        };
        Ok(payload)
    }

    /// Relabel the active Ticket session for `ticket_id` (W210-F2).
    ///
    /// The picker claims a ticket *before* it knows the engine session id (the
    /// claim must reject a racing picker before any session is spawned), so the
    /// claim labels the session with the agent id. Once the engine session is
    /// forked, the desktop relabels the session with the concrete engine
    /// `session_id` so [`close_ticket_sessions_with_label`] can release exactly
    /// this session's claim when it ends — never a different worker's. Returns
    /// `true` when an active session was found and relabelled.
    pub async fn set_ticket_session_label(
        &self,
        ticket_id: &str,
        label: &str,
    ) -> Result<bool, SessionStoreError> {
        let Some(id) = self.find_active_for_ticket(ticket_id).await? else {
            return Ok(false);
        };
        let n = self
            .conn
            .execute(
                "UPDATE task_sessions SET label=?2 WHERE id=?1",
                params![id.to_string(), label.to_string()],
            )
            .await?;
        Ok(n > 0)
    }

    /// Close every active Ticket session whose `label` equals `label` (W210-F2).
    ///
    /// The release half of the picker-claim lifecycle: when an engine session
    /// ends — cleanly, by crash, or by app-quit — the desktop calls this with
    /// the ended session id so the ticket it owned re-derives to `handoff`/`open`
    /// for the next picker. Keyed on `label` (the engine session id, set by
    /// [`set_ticket_session_label`]) so it closes only the ended session's claim,
    /// not a claim some other live worker holds on the same ticket. Idempotent:
    /// returns `0` when the session was already released by `review`/`handoff`.
    pub async fn close_ticket_sessions_with_label(
        &self,
        label: &str,
    ) -> Result<usize, SessionStoreError> {
        let now = Self::now_ms();
        let n = self
            .conn
            .execute(
                "UPDATE task_sessions SET status='closed', closed_at=?2
                 WHERE kind_tag='ticket' AND status='active' AND label=?1",
                params![label.to_string(), now as i64],
            )
            .await?;
        Ok(n as usize)
    }

    /// List every active Ticket session as `(session id, owner label)` (W210-F2).
    ///
    /// The boot-reconcile read side: the desktop closes any claim whose owner
    /// label (the engine session id stamped by [`set_ticket_session_label`]) is
    /// no longer live or cleanly paused, freeing tickets orphaned by a hard
    /// app-quit. A `None` label is a claim that crashed between claim and
    /// relabel — it has no live owner and is reconciled the same way.
    pub async fn list_active_ticket_sessions(
        &self,
    ) -> Result<Vec<(TaskSessionId, Option<String>)>, SessionStoreError> {
        let mut rows = self
            .conn
            .query(
                "SELECT id, label FROM task_sessions
                 WHERE kind_tag='ticket' AND status='active'",
                (),
            )
            .await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            let id_s: String = row.get(0)?;
            let label: Option<String> = row.get(1)?;
            let id = id_s
                .parse::<TaskSessionId>()
                .map_err(|e| SessionStoreError::Parse(e.to_string()))?;
            out.push((id, label));
        }
        Ok(out)
    }

    /// Idempotent: finds an existing active Ticket session or creates a new one.
    pub async fn ensure_ticket_session(
        &self,
        ticket_id: &str,
        label: Option<String>,
    ) -> Result<TaskSessionId, SessionStoreError> {
        if let Some(id) = self.find_active_for_ticket(ticket_id).await? {
            return Ok(id);
        }
        let id = self
            .create_session(
                TaskSessionKind::Ticket {
                    ticket_id: ticket_id.to_string(),
                },
                label,
                None,
            )
            .await?;
        self.bind_session(
            &id,
            &Binding::Ticket {
                id: ticket_id.to_string(),
            },
        )
        .await?;
        Ok(id)
    }

    /// Atomically claim a ticket for a live session (W210 `board.claim`).
    ///
    /// Conflict-rejecting: at most one active TaskSession may own a ticket.
    /// - No live session → create one (labelled with `claimant`), bind it,
    ///   return [`TicketClaim::Claimed`].
    /// - A live session whose `label` equals `claimant` → idempotent
    ///   [`TicketClaim::AlreadyOwned`] (a picker re-claiming its own ticket).
    /// - A live session owned by anyone else → [`TicketClaim::Conflict`]; no
    ///   write happens.
    ///
    /// A `None` claimant can never match an existing owner — an anonymous
    /// caller can't prove ownership, so any live session is a conflict. The
    /// picker always passes its session/agent token, so idempotent re-claim
    /// works for the real call site.
    ///
    /// Atomicity rests on the daemon being the single writer: the find +
    /// create run without another writer interleaving, so two pickers racing
    /// through the daemon serialize — one `Claimed`, the other `Conflict`.
    pub async fn claim_ticket_session(
        &self,
        ticket_id: &str,
        claimant: Option<&str>,
    ) -> Result<TicketClaim, SessionStoreError> {
        if let Some(existing) = self.find_active_for_ticket(ticket_id).await? {
            let holder = self.get_session(&existing).await?.label;
            // Idempotent only when both sides carry the same non-empty token.
            let mine = match (claimant, holder.as_deref()) {
                (Some(c), Some(h)) => c == h,
                _ => false,
            };
            return Ok(if mine {
                TicketClaim::AlreadyOwned(existing)
            } else {
                TicketClaim::Conflict {
                    session: existing,
                    holder,
                }
            });
        }

        let id = self
            .create_session(
                TaskSessionKind::Ticket {
                    ticket_id: ticket_id.to_string(),
                },
                claimant.map(|c| c.to_string()),
                None,
            )
            .await?;
        self.bind_session(
            &id,
            &Binding::Ticket {
                id: ticket_id.to_string(),
            },
        )
        .await?;
        Ok(TicketClaim::Claimed(id))
    }

    /// Count how many GnomeShift sessions exist for `shift_id` (any status).
    /// Used to derive the next `run_n`.
    pub async fn count_gnome_shift_runs(&self, shift_id: &str) -> Result<u32, SessionStoreError> {
        // kind_payload is a JSON object; shift_id appears as `"shift_id":"<id>"`.
        // We use a LIKE match on the stored JSON rather than parsing each row.
        let mut rows = self
            .conn
            .query(
                "SELECT COUNT(*) FROM task_sessions
                 WHERE kind_tag = 'gnome_shift'
                   AND kind_payload LIKE '%' || ?1 || '%'",
                params![shift_id.to_string()],
            )
            .await?;
        let row = rows.next().await?.expect("COUNT(*) always returns one row");
        let n: i64 = row.get(0)?;
        Ok(n as u32)
    }

    /// Find the active TaskSession for `(shift_id, run_n)`.
    pub async fn find_gnome_shift_session(
        &self,
        shift_id: &str,
        run_n: u32,
    ) -> Result<Option<TaskSessionId>, SessionStoreError> {
        let mut rows = self
            .conn
            .query(
                "SELECT id FROM task_sessions
                 WHERE kind_tag = 'gnome_shift'
                   AND status = 'active'
                   AND kind_payload LIKE '%' || ?1 || '%'
                   AND json_extract(kind_payload, '$.run_n') = ?2
                 LIMIT 1",
                params![shift_id.to_string(), run_n as i64],
            )
            .await?;
        let id = match rows.next().await? {
            Some(row) => {
                let s: String = row.get(0)?;
                Some(s)
            }
            None => None,
        };
        id.map(|s| {
            s.parse::<TaskSessionId>()
                .map_err(|e| SessionStoreError::Parse(e.to_string()))
        })
        .transpose()
    }

    /// Idempotent: find an active GnomeShift session for `(shift_id, run_n)` or create one.
    ///
    /// The `run_n` is the caller's responsibility — use `count_gnome_shift_runs` to derive it
    /// before calling if you don't already have it.
    pub async fn ensure_gnome_shift_session(
        &self,
        shift_id: &str,
        run_n: u32,
    ) -> Result<TaskSessionId, SessionStoreError> {
        if let Some(id) = self.find_gnome_shift_session(shift_id, run_n).await? {
            return Ok(id);
        }
        let id = self
            .create_session(
                TaskSessionKind::GnomeShift {
                    shift_id: shift_id.to_string(),
                    run_n,
                },
                Some(format!("{shift_id} run #{run_n}")),
                None,
            )
            .await?;
        Ok(id)
    }

    pub async fn count_tool_calls(&self, id: &TaskSessionId) -> Result<u64, SessionStoreError> {
        let mut rows = self
            .conn
            .query(
                "SELECT COUNT(*) FROM session_tool_calls WHERE session_id = ?1",
                params![id.to_string()],
            )
            .await?;
        let row = rows.next().await?.expect("COUNT(*) returns one row");
        let n: i64 = row.get(0)?;
        Ok(n as u64)
    }

    /// Find the active TaskSession where `chat_session_id` is bound as Driver.
    /// Returns the session id string, or `None` if no such session exists.
    pub async fn find_active_for_chat(
        &self,
        chat_session_id: &str,
    ) -> Result<Option<TaskSessionId>, SessionStoreError> {
        let mut rows = self
            .conn
            .query(
                "SELECT ts.id FROM task_sessions ts
                 JOIN session_bindings sb ON sb.session_id = ts.id
                 WHERE sb.binding_kind = 'chat'
                   AND sb.binding_role = 'driver'
                   AND sb.payload = ?1
                   AND ts.status = 'active'
                 LIMIT 1",
                params![chat_session_id.to_string()],
            )
            .await?;
        let id: Option<String> = match rows.next().await? {
            Some(row) => Some(row.get(0)?),
            None => None,
        };
        id.map(|s| {
            s.parse::<TaskSessionId>()
                .map_err(|e| SessionStoreError::Parse(e.to_string()))
        })
        .transpose()
    }

    // ── Card outcomes ─────────────────────────────────────────────────────────

    /// Query the decline/accept counts and most-recent decline for `rule_id`
    /// over the window `[since_ms, ∞)`.
    ///
    /// Returns `(recent_declines, recent_accepts, last_decline_reason, last_decline_at_ms)`.
    /// All values are zero / `None` when no rows match (the rule has never been
    /// declined or accepted in the window).
    pub async fn decline_signal_for_rule(
        &self,
        rule_id: &str,
        since_ms: u64,
    ) -> Result<(u16, u16, Option<DeclineReason>, Option<u64>), SessionStoreError> {
        // Aggregate counts in one pass.
        let mut rows = self
            .conn
            .query(
                "SELECT
                   SUM(CASE WHEN outcome = 'declined' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN outcome IN ('accepted', 'edited_then_accepted') THEN 1 ELSE 0 END)
                 FROM session_card_outcomes
                 WHERE rule_id = ?1 AND decided_at >= ?2",
                params![rule_id.to_string(), since_ms as i64],
            )
            .await?;
        let row = rows.next().await?.expect("aggregate returns one row");
        let declines: i64 = row.get::<i64>(0).unwrap_or(0);
        let accepts: i64 = row.get::<i64>(1).unwrap_or(0);

        // Most-recent decline (separate query keeps the aggregate query simple).
        let mut rows = self
            .conn
            .query(
                "SELECT decline_reason, decided_at
                 FROM session_card_outcomes
                 WHERE rule_id = ?1 AND outcome = 'declined' AND decided_at >= ?2
                 ORDER BY decided_at DESC
                 LIMIT 1",
                params![rule_id.to_string(), since_ms as i64],
            )
            .await?;
        let (last_reason, last_at) = match rows.next().await? {
            None => (None, None),
            Some(row) => {
                let reason_str: Option<String> = row.get(0)?;
                let at: i64 = row.get(1)?;
                let reason = reason_str
                    .as_deref()
                    .and_then(|s| s.parse::<DeclineReason>().ok());
                (reason, Some(at as u64))
            }
        };

        Ok((
            declines.min(u16::MAX as i64) as u16,
            accepts.min(u16::MAX as i64) as u16,
            last_reason,
            last_at,
        ))
    }

    pub async fn record_card_outcome(
        &self,
        session_id: &TaskSessionId,
        card_id: &str,
        rule_id: Option<&str>,
        shift_id: Option<&str>,
        outcome: CardOutcome,
        decline_reason: Option<DeclineReason>,
        decline_note: Option<&str>,
        decided_by: &str,
        path: Option<&str>,
        before_blob: Option<&str>,
        reverts_card: Option<&str>,
    ) -> Result<(), SessionStoreError> {
        let now = Self::now_ms();
        self.conn
            .execute(
                "INSERT OR REPLACE INTO session_card_outcomes
                 (session_id, card_id, rule_id, shift_id, outcome, decline_reason,
                  decline_note, decided_at, decided_by, path, before_blob, reverts_card)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    session_id.to_string(),
                    card_id.to_string(),
                    rule_id.map(|s| s.to_string()),
                    shift_id.map(|s| s.to_string()),
                    outcome.as_str().to_string(),
                    decline_reason.as_ref().map(|r| r.as_str().to_string()),
                    decline_note.map(|s| s.to_string()),
                    now as i64,
                    decided_by.to_string(),
                    path.map(|s| s.to_string()),
                    before_blob.map(|s| s.to_string()),
                    reverts_card.map(|s| s.to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    /// List all card outcomes for a session, ordered by decided_at asc.
    pub async fn list_card_outcomes(
        &self,
        session_id: &TaskSessionId,
    ) -> Result<Vec<CardOutcomeRow>, SessionStoreError> {
        let mut rows = self
            .conn
            .query(
                "SELECT card_id, rule_id, shift_id, outcome, decline_reason,
                        decline_note, decided_at, decided_by, path, before_blob, reverts_card
                 FROM session_card_outcomes
                 WHERE session_id = ?1
                 ORDER BY decided_at ASC",
                params![session_id.to_string()],
            )
            .await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            let decided_at: i64 = row.get(6)?;
            out.push(CardOutcomeRow {
                card_id: row.get(0)?,
                rule_id: row.get(1)?,
                shift_id: row.get(2)?,
                outcome: row.get::<String>(3)?,
                decline_reason: row.get(4)?,
                decline_note: row.get(5)?,
                decided_at: decided_at as u64,
                decided_by: row.get(7)?,
                path: row.get(8)?,
                before_blob: row.get(9)?,
                reverts_card: row.get(10)?,
            });
        }
        Ok(out)
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn parse_kind(
    tag: &str,
    payload: Option<&str>,
) -> Result<TaskSessionKind, SessionStoreError> {
    Ok(match tag {
        "adhoc" => TaskSessionKind::Adhoc,
        _ => {
            let p = payload.unwrap_or("{}");
            serde_json::from_str(p).map_err(SessionStoreError::Json)?
        }
    })
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> SessionStore {
        SessionStore::open_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn round_trip_adhoc() {
        let s = store().await;
        let id = s
            .create_session(TaskSessionKind::Adhoc, Some("my adhoc".into()), None)
            .await
            .unwrap();
        let session = s.get_session(&id).await.unwrap();
        assert_eq!(session.status, SessionStatus::Active);
        assert_eq!(session.label.as_deref(), Some("my adhoc"));
        assert!(matches!(session.kind, TaskSessionKind::Adhoc));
    }

    #[tokio::test]
    async fn round_trip_ticket_kind() {
        let s = store().await;
        let id = s
            .create_session(
                TaskSessionKind::Ticket { ticket_id: "R076-F1".into() },
                None,
                None,
            )
            .await
            .unwrap();
        let session = s.get_session(&id).await.unwrap();
        assert!(matches!(session.kind, TaskSessionKind::Ticket { ref ticket_id } if ticket_id == "R076-F1"));
    }

    #[tokio::test]
    async fn round_trip_gnome_shift_kind() {
        let s = store().await;
        let id = s
            .create_session(
                TaskSessionKind::GnomeShift { shift_id: "shift-1".into(), run_n: 3 },
                None,
                None,
            )
            .await
            .unwrap();
        let session = s.get_session(&id).await.unwrap();
        assert!(
            matches!(session.kind, TaskSessionKind::GnomeShift { ref shift_id, run_n } if shift_id == "shift-1" && run_n == 3)
        );
    }

    #[tokio::test]
    async fn check_constraint_bad_status_rejected() {
        let s = store().await;
        let err = s
            .conn
            .execute(
                "INSERT INTO task_sessions
                 (id, kind_tag, created_at, status, working_set)
                 VALUES ('x', 'adhoc', 1, 'bogus', '[]')",
                (),
            )
            .await;
        assert!(err.is_err(), "bad status should be rejected by CHECK");
    }

    #[tokio::test]
    async fn check_constraint_bad_kind_rejected() {
        let s = store().await;
        let err = s
            .conn
            .execute(
                "INSERT INTO task_sessions
                 (id, kind_tag, created_at, status, working_set)
                 VALUES ('y', 'not_a_kind', 1, 'active', '[]')",
                (),
            )
            .await;
        assert!(err.is_err(), "bad kind_tag should be rejected by CHECK");
    }

    #[tokio::test]
    async fn pause_resume_lifecycle() {
        let s = store().await;
        let id = s.create_session(TaskSessionKind::Adhoc, None, None).await.unwrap();
        s.pause_session(&id).await.unwrap();
        assert_eq!(s.get_session(&id).await.unwrap().status, SessionStatus::Paused);
        s.resume_session(&id).await.unwrap();
        assert_eq!(s.get_session(&id).await.unwrap().status, SessionStatus::Active);
    }

    #[tokio::test]
    async fn close_session_records_result() {
        let s = store().await;
        let id = s.create_session(TaskSessionKind::Adhoc, None, None).await.unwrap();
        let result = crate::types::SessionResult {
            diff_summary: crate::types::DiffSummary {
                files_changed: 2,
                insertions: 10,
                deletions: 3,
                hash: None,
            },
            final_verdict: Verdict::Pass,
            escalation: None,
        };
        s.close_session(&id, Some(result)).await.unwrap();
        let session = s.get_session(&id).await.unwrap();
        assert_eq!(session.status, SessionStatus::Closed);
        assert!(session.result.is_some());
    }

    #[tokio::test]
    async fn binding_round_trip() {
        let s = store().await;
        let id = s.create_session(TaskSessionKind::Adhoc, None, None).await.unwrap();
        s.bind_session(
            &id,
            &Binding::Chat {
                session: "chat-abc".into(),
                role: ChatBindingRole::Driver,
            },
        )
        .await
        .unwrap();
        let session = s.get_session(&id).await.unwrap();
        assert_eq!(session.bindings.len(), 1);
        assert!(
            matches!(&session.bindings[0], Binding::Chat { session, role }
                if session == "chat-abc" && *role == ChatBindingRole::Driver)
        );
    }

    #[tokio::test]
    async fn note_run_and_verify() {
        let s = store().await;
        let id = s.create_session(TaskSessionKind::Adhoc, None, None).await.unwrap();
        s.note_run(&id, "run-1", false).await.unwrap();
        s.note_verify(&id, "run-1", Verdict::Pass).await.unwrap();
        // No error = success; note_verify upserts.
        s.note_verify(&id, "run-2", Verdict::Fail).await.unwrap();
    }

    #[tokio::test]
    async fn note_tool_call_idempotent() {
        let s = store().await;
        let id = s.create_session(TaskSessionKind::Adhoc, None, None).await.unwrap();
        s.note_tool_call(
            &id,
            &ToolCallRef {
                chat_session: "chat-1".into(),
                turn_seq: 1,
                call_seq: 0,
                tool_name: "Edit".into(),
            },
        )
        .await
        .unwrap();
        // Idempotent: same PK ignored.
        s.note_tool_call(
            &id,
            &ToolCallRef {
                chat_session: "chat-1".into(),
                turn_seq: 1,
                call_seq: 0,
                tool_name: "Edit".into(),
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn list_sessions_filter_by_status() {
        let s = store().await;
        let id1 = s.create_session(TaskSessionKind::Adhoc, None, None).await.unwrap();
        let _id2 = s.create_session(TaskSessionKind::Adhoc, None, None).await.unwrap();
        s.close_session(&id1, None).await.unwrap();
        let active = s
            .list_sessions(&SessionFilter {
                status: Some("active".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(active.len(), 1);
        let closed = s
            .list_sessions(&SessionFilter {
                status: Some("closed".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(closed.len(), 1);
    }

    #[tokio::test]
    async fn escalation_cycle_rejected() {
        let s = store().await;
        let a = s.create_session(TaskSessionKind::Adhoc, None, None).await.unwrap();
        let b = s.create_session(TaskSessionKind::Adhoc, None, Some(&a)).await.unwrap();
        let err = s
            .escalate_session(
                &a,
                EscalationTarget::Ticket { id: "T1".into() },
                &b,
                "test".into(),
            )
            .await;
        assert!(matches!(err, Err(SessionStoreError::EscalationCycle)), "expected cycle error, got {err:?}");
    }

    #[tokio::test]
    async fn escalation_linear_ok() {
        let s = store().await;
        let a = s.create_session(TaskSessionKind::Adhoc, None, None).await.unwrap();
        let b = s
            .create_session(
                TaskSessionKind::Ticket { ticket_id: "R076-F1".into() },
                None,
                None,
            )
            .await
            .unwrap();
        s.escalate_session(
            &a,
            EscalationTarget::Ticket { id: "R076-F1".into() },
            &b,
            "testing".into(),
        )
        .await
        .unwrap();
        let session = s.get_session(&a).await.unwrap();
        assert_eq!(session.status, SessionStatus::Escalated);
        assert!(session.result.unwrap().escalation.is_some());
    }

    #[tokio::test]
    async fn find_active_for_chat_returns_driver_session() {
        let s = store().await;
        let id = s.create_session(TaskSessionKind::Adhoc, None, None).await.unwrap();
        // Not yet bound → should return None.
        assert!(s.find_active_for_chat("chat:abc").await.unwrap().is_none());
        // Bind as Witness → still None (driver only).
        s.bind_session(&id, &Binding::Chat {
            session: "chat:abc".into(),
            role: ChatBindingRole::Witness,
        })
        .await
        .unwrap();
        assert!(s.find_active_for_chat("chat:abc").await.unwrap().is_none());
        // Bind as Driver → should now find it.
        s.bind_session(&id, &Binding::Chat {
            session: "chat:abc".into(),
            role: ChatBindingRole::Driver,
        })
        .await
        .unwrap();
        let found = s.find_active_for_chat("chat:abc").await.unwrap();
        assert_eq!(found.as_ref().map(|x| x.to_string()), Some(id.to_string()));
        // Close the session → no longer active → returns None.
        s.close_session(&id, None).await.unwrap();
        assert!(s.find_active_for_chat("chat:abc").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ensure_ticket_session_is_idempotent() {
        let s = SessionStore::open_in_memory().await.unwrap();
        let id1 = s.ensure_ticket_session("R001-T1", None).await.unwrap();
        let id2 = s.ensure_ticket_session("R001-T1", None).await.unwrap();
        assert_eq!(id1, id2, "same ticket → same session id");
        // Different ticket gets a new session.
        let id3 = s.ensure_ticket_session("R001-T2", None).await.unwrap();
        assert_ne!(id1, id3);
    }

    #[tokio::test]
    async fn list_active_ticket_ids_folds_live_ownership() {
        let s = SessionStore::open_in_memory().await.unwrap();
        // Two active ticket sessions.
        s.ensure_ticket_session("R001-T1", None).await.unwrap();
        s.ensure_ticket_session("R001-T2", None).await.unwrap();
        // A closed one must not appear.
        let id3 = s.ensure_ticket_session("R001-T3", None).await.unwrap();
        s.close_session(&id3, None).await.unwrap();

        let mut ids = s.list_active_ticket_ids().await.unwrap();
        ids.sort();
        assert_eq!(
            ids,
            vec!["R001-T1".to_string(), "R001-T2".to_string()],
            "only live (active) ticket sessions are folded; closed ones excluded"
        );
    }

    #[tokio::test]
    async fn relabel_then_close_by_label_releases_only_that_owner() {
        let s = SessionStore::open_in_memory().await.unwrap();

        // Two ticket claims, agent-labelled by the picker, then relabelled to
        // their concrete engine session ids (W210-F2).
        s.claim_ticket_session("R001-T1", Some("agent:A")).await.unwrap();
        s.claim_ticket_session("R001-T2", Some("agent:B")).await.unwrap();
        assert!(s.set_ticket_session_label("R001-T1", "session:S1").await.unwrap());
        assert!(s.set_ticket_session_label("R001-T2", "session:S2").await.unwrap());

        // Relabelling a ticket with no active claim is a no-op (false).
        assert!(!s.set_ticket_session_label("R001-T9", "session:X").await.unwrap());

        // Closing by S1's owner token releases exactly T1's claim.
        assert_eq!(s.close_ticket_sessions_with_label("session:S1").await.unwrap(), 1);
        assert!(s.find_active_for_ticket("R001-T1").await.unwrap().is_none());
        assert!(s.find_active_for_ticket("R001-T2").await.unwrap().is_some());

        // Idempotent: closing the same owner again closes nothing.
        assert_eq!(s.close_ticket_sessions_with_label("session:S1").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn list_active_ticket_sessions_reports_id_and_label() {
        let s = SessionStore::open_in_memory().await.unwrap();
        s.claim_ticket_session("R001-T1", Some("agent:A")).await.unwrap();
        s.set_ticket_session_label("R001-T1", "session:S1").await.unwrap();
        // A claim that never got relabelled keeps its agent label.
        s.claim_ticket_session("R001-T2", Some("agent:B")).await.unwrap();
        // A closed one must not appear.
        let id3 = s.claim_ticket_session("R001-T3", Some("agent:C")).await.unwrap();
        if let TicketClaim::Claimed(id) = id3 {
            s.close_session(&id, None).await.unwrap();
        }

        let mut labels: Vec<Option<String>> = s
            .list_active_ticket_sessions()
            .await
            .unwrap()
            .into_iter()
            .map(|(_, label)| label)
            .collect();
        labels.sort();
        assert_eq!(
            labels,
            vec![Some("agent:B".to_string()), Some("session:S1".to_string())],
            "only live claims, carrying their current owner label"
        );
    }

    #[tokio::test]
    async fn claim_ticket_session_fresh_then_idempotent_then_conflict() {
        let s = SessionStore::open_in_memory().await.unwrap();

        // First claim by picker-A → fresh.
        let first = s
            .claim_ticket_session("R001-T1", Some("agent:A"))
            .await
            .unwrap();
        let claimed_id = match first {
            TicketClaim::Claimed(id) => id,
            other => panic!("expected Claimed, got {other:?}"),
        };

        // A re-claims its own ticket → idempotent, same session.
        let again = s
            .claim_ticket_session("R001-T1", Some("agent:A"))
            .await
            .unwrap();
        assert_eq!(again, TicketClaim::AlreadyOwned(claimed_id.clone()));

        // Picker-B races on the same ticket → conflict, no new session.
        let conflict = s
            .claim_ticket_session("R001-T1", Some("agent:B"))
            .await
            .unwrap();
        assert_eq!(
            conflict,
            TicketClaim::Conflict {
                session: claimed_id.clone(),
                holder: Some("agent:A".to_string()),
            }
        );

        // Only one active session exists despite three claim calls.
        assert_eq!(
            s.list_active_ticket_ids().await.unwrap(),
            vec!["R001-T1".to_string()]
        );
    }

    #[tokio::test]
    async fn claim_ticket_session_anonymous_caller_never_owns() {
        let s = SessionStore::open_in_memory().await.unwrap();
        // Anonymous claim creates the session...
        let first = s.claim_ticket_session("R001-T1", None).await.unwrap();
        assert!(matches!(first, TicketClaim::Claimed(_)));
        // ...but a second anonymous claim can't prove ownership → conflict.
        let second = s.claim_ticket_session("R001-T1", None).await.unwrap();
        assert!(matches!(second, TicketClaim::Conflict { .. }));
    }

    #[tokio::test]
    async fn claim_ticket_session_reclaimable_after_close() {
        let s = SessionStore::open_in_memory().await.unwrap();
        let id = match s.claim_ticket_session("R001-T1", Some("agent:A")).await.unwrap() {
            TicketClaim::Claimed(id) => id,
            other => panic!("expected Claimed, got {other:?}"),
        };
        // Owner closes the session (handoff / crash recovery).
        s.close_session(&id, None).await.unwrap();
        // A different agent can now claim cleanly — column re-derived to open.
        let next = s.claim_ticket_session("R001-T1", Some("agent:B")).await.unwrap();
        assert!(matches!(next, TicketClaim::Claimed(_)));
    }

    #[tokio::test]
    async fn card_outcome_round_trip() {
        let s = store().await;
        let id = s.create_session(TaskSessionKind::Adhoc, None, None).await.unwrap();
        s.record_card_outcome(
            &id,
            "card-1",
            Some("rule-x"),
            None,
            CardOutcome::Declined,
            Some(DeclineReason::Stylistic),
            Some("looked fine to me"),
            "operator:user",
            Some("src/lib.rs"),
            Some(r#"{"kind":"text","bytes":"old"}"#),
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn card_outcome_bad_outcome_rejected() {
        let s = store().await;
        let id = s.create_session(TaskSessionKind::Adhoc, None, None).await.unwrap();
        let err = s
            .conn
            .execute(
                "INSERT INTO session_card_outcomes
                 (session_id, card_id, outcome, decided_at, decided_by)
                 VALUES (?1, 'c1', 'bogus_outcome', 1, 'op')",
                params![id.to_string()],
            )
            .await;
        assert!(err.is_err(), "bad outcome should be rejected by CHECK");
    }

    // ── GnomeShift session ────────────────────────────────────────────────────

    #[tokio::test]
    async fn ensure_gnome_shift_session_is_idempotent() {
        let s = store().await;
        let id1 = s.ensure_gnome_shift_session("lint-fix", 0).await.unwrap();
        let id2 = s.ensure_gnome_shift_session("lint-fix", 0).await.unwrap();
        assert_eq!(id1, id2, "same (shift_id, run_n) → same session id");
        // Different run_n gets a new session.
        let id3 = s.ensure_gnome_shift_session("lint-fix", 1).await.unwrap();
        assert_ne!(id1, id3, "different run_n → new session");
        // Different shift gets a new session.
        let id4 = s.ensure_gnome_shift_session("format", 0).await.unwrap();
        assert_ne!(id1, id4, "different shift_id → new session");
    }

    #[tokio::test]
    async fn count_gnome_shift_runs_increments() {
        let s = store().await;
        assert_eq!(s.count_gnome_shift_runs("chat-index").await.unwrap(), 0);
        s.ensure_gnome_shift_session("chat-index", 0).await.unwrap();
        assert_eq!(s.count_gnome_shift_runs("chat-index").await.unwrap(), 1);
        s.ensure_gnome_shift_session("chat-index", 1).await.unwrap();
        assert_eq!(s.count_gnome_shift_runs("chat-index").await.unwrap(), 2);
        // Other shifts don't affect the count.
        s.ensure_gnome_shift_session("other-shift", 0).await.unwrap();
        assert_eq!(s.count_gnome_shift_runs("chat-index").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn ensure_gnome_shift_session_creates_correct_kind() {
        let s = store().await;
        let id = s.ensure_gnome_shift_session("chat-index", 5).await.unwrap();
        let session = s.get_session(&id).await.unwrap();
        assert_eq!(session.status, SessionStatus::Active);
        assert!(
            matches!(&session.kind, TaskSessionKind::GnomeShift { shift_id, run_n }
                if shift_id == "chat-index" && *run_n == 5),
            "unexpected kind: {:?}", session.kind
        );
        assert_eq!(session.label.as_deref(), Some("chat-index run #5"));
    }

    #[tokio::test]
    async fn find_gnome_shift_session_returns_none_after_close() {
        let s = store().await;
        let id = s.ensure_gnome_shift_session("lint-fix", 0).await.unwrap();
        assert!(s.find_gnome_shift_session("lint-fix", 0).await.unwrap().is_some());
        s.close_session(&id, None).await.unwrap();
        assert!(
            s.find_gnome_shift_session("lint-fix", 0).await.unwrap().is_none(),
            "closed session should not be returned"
        );
        // ensure after close creates a new session (run_n 1 would be a new run)
        let id2 = s.ensure_gnome_shift_session("lint-fix", 1).await.unwrap();
        assert_ne!(id, id2);
    }
}
