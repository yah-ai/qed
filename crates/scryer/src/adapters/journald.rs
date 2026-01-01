//! `adapter::journald` — narrow P2 adapter for host-level systemd units.
//!
//! Scope per arch doc §"`adapter::journald`" (narrow, P2):
//! - **In scope**: warden's own systemd unit, sshd, host kernel events (opt-in),
//!   mode-A yah-camp daemons colocated with warden (rare).
//! - **Out of scope**: yah-managed services (covered by `containerd_logs`),
//!   cloudflared, tailscaled, every workload running under containerd.
//!
//! Each entry is scoped to `Service(MeshIdent("<unit>.host"))` where `<unit>`
//! is the systemd unit name with any `.service` / `.socket` suffix stripped.
//! sshd's entries → `Service(MeshIdent("sshd.host"))`, kernel → `kernel.host`, etc.
//!
//! Production deployment: warden subscribes to journald via the
//! `systemd-journal-gateway` HTTP SSE stream or `sd-journal` bindings and
//! hands the adapter a [`JournaldSource`] impl. The trait seam lets tests inject
//! a scripted fixture without a real journald.

use crate::adapters::{Adapter, AdapterError};
use crate::service::Scryer;
use async_trait::async_trait;
use observation::{Event, EventScope, EventSource, Level, TaskRunId};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use workload_spec::MeshIdent;

// ─── JournaldEntry ────────────────────────────────────────────────────────────

/// A single entry from the systemd journal.
///
/// Field names follow the journal's own naming convention; only the fields
/// scryer actually uses are required — everything else can land in `extra`.
#[derive(Debug, Clone)]
pub struct JournaldEntry {
    /// Systemd unit name, e.g. `"sshd.service"` or `"kernel"`.
    pub unit: String,
    /// Syslog priority 0–7 (0 = EMERG, 7 = DEBUG).
    pub priority: u8,
    /// Journal MESSAGE field.
    pub message: String,
    /// `__REALTIME_TIMESTAMP` in microseconds (0 in tests / when unavailable).
    pub timestamp_us: u64,
    /// Any extra SD fields (`SYSLOG_IDENTIFIER`, `_PID`, etc.) available for
    /// the agent to query from `event.fields`.
    pub extra: HashMap<String, String>,
}

// ─── JournaldSource ───────────────────────────────────────────────────────────

/// Trait implemented by production (journal-gateway / sd-journal) and tests
/// (scripted fixture).
///
/// `subscribe` opens a stream filtered to the given unit names and returns a
/// channel receiver. Entries arrive in journal order; when the sender drops the
/// adapter exits with `StreamBroken` so the supervisor can restart.
#[async_trait]
pub trait JournaldSource: Send + Sync {
    async fn subscribe(
        &self,
        units: &[String],
    ) -> Result<mpsc::Receiver<JournaldEntry>, AdapterError>;
}

// ─── helpers ──────────────────────────────────────────────────────────────────

/// Strip `.service`, `.socket`, `.target`, `.timer` suffix from a unit name.
///
/// `"sshd.service"` → `"sshd"`, `"sshd"` → `"sshd"`, `"kernel"` → `"kernel"`.
fn unit_base(unit: &str) -> &str {
    for suffix in &[".service", ".socket", ".target", ".timer", ".scope"] {
        if let Some(base) = unit.strip_suffix(suffix) {
            return base;
        }
    }
    unit
}

/// Map a syslog priority (0–7) to a scryer [`Level`].
///
/// | Priority | Syslog name | Level  |
/// |----------|-------------|--------|
/// | 0        | EMERG       | Fatal  |
/// | 1        | ALERT       | Fatal  |
/// | 2        | CRIT        | Error  |
/// | 3        | ERR         | Error  |
/// | 4        | WARNING     | Warn   |
/// | 5        | NOTICE      | Info   |
/// | 6        | INFO        | Info   |
/// | 7        | DEBUG       | Debug  |
fn priority_level(priority: u8) -> Level {
    match priority {
        0 | 1 => Level::Fatal,
        2 | 3 => Level::Error,
        4 => Level::Warn,
        5 | 6 => Level::Info,
        7 => Level::Debug,
        _ => Level::Info,
    }
}

// ─── JournaldAdapter ─────────────────────────────────────────────────────────

/// Ingestion adapter for host-level journald entries.
///
/// Runs in a loop: `subscribe` → drain entries → on stream close, return
/// `Err(StreamBroken)` so the supervisor backs off and retries.
pub struct JournaldAdapter {
    scryer: Arc<Scryer>,
    source: Arc<dyn JournaldSource>,
    /// Normalised unit names to subscribe to (e.g. `["sshd", "ssh"]`).  The
    /// source filters by these; any entry from an unexpected unit is dropped.
    allowed_units: Vec<String>,
    started_at: Instant,
    /// Per-scope (`<unit>.host`) monotonic seq counter.
    seqs: HashMap<String, u32>,
    /// Stable `run_id` anchor per scope; backward-compat field in the `Event`
    /// row shape.  One anchor per unique `(scope_kind, scope_id)`.
    run_ids: HashMap<String, TaskRunId>,
}

impl JournaldAdapter {
    /// Create a new adapter that subscribes to the given `units`.
    ///
    /// `units` should be base names without suffix
    /// (e.g. `["sshd", "kernel"]`) — the source impl may accept either form.
    pub fn new(
        scryer: Arc<Scryer>,
        source: Arc<dyn JournaldSource>,
        units: Vec<String>,
    ) -> Self {
        Self {
            scryer,
            source,
            allowed_units: units,
            started_at: Instant::now(),
            seqs: HashMap::new(),
            run_ids: HashMap::new(),
        }
    }

    fn offset_ms(&self) -> u32 {
        let elapsed = self.started_at.elapsed().as_millis();
        elapsed.min(u32::MAX as u128) as u32
    }

    fn next_seq(&mut self, scope_id: &str) -> u32 {
        let s = self.seqs.entry(scope_id.to_string()).or_insert(0);
        let seq = *s;
        *s += 1;
        seq
    }

    fn run_id_for(&mut self, scope_id: &str) -> TaskRunId {
        self.run_ids
            .entry(scope_id.to_string())
            .or_insert_with(TaskRunId::new)
            .clone()
    }

    fn entry_to_event(&mut self, entry: &JournaldEntry) -> (EventScope, Event) {
        let base = unit_base(&entry.unit);
        let scope_id = format!("{}.host", base);
        let scope = EventScope::Service(MeshIdent(scope_id.clone()));

        let run_id = self.run_id_for(&scope_id);
        let seq = self.next_seq(&scope_id);
        let level = priority_level(entry.priority);

        let mut fields = json!({
            "unit": entry.unit,
            "priority": entry.priority,
        });
        if entry.timestamp_us > 0 {
            fields["timestamp_us"] = entry.timestamp_us.into();
        }
        for (k, v) in &entry.extra {
            fields[k] = v.as_str().into();
        }

        let event = Event {
            run_id,
            seq,
            offset_ms: self.offset_ms(),
            level,
            target: format!("journald.{}", base),
            msg: entry.message.clone(),
            fields,
            anchor: None,
            source: EventSource::Synth,
        };
        (scope, event)
    }
}

#[async_trait]
impl Adapter for JournaldAdapter {
    fn name(&self) -> &str {
        "journald"
    }

    /// Returns a sentinel scope used by the supervisor for synthetic
    /// `service.restart` events.  Individual journal entries are scoped to
    /// `MeshIdent("<unit>.host")` for each host unit.
    fn scope(&self) -> EventScope {
        EventScope::Service(MeshIdent("journald.host".to_string()))
    }

    async fn run(&mut self) -> Result<(), AdapterError> {
        let mut rx = self.source.subscribe(&self.allowed_units).await?;

        while let Some(entry) = rx.recv().await {
            // Drop entries for units that weren't requested (source may not
            // filter strictly, or extra units slip through).
            let base = unit_base(&entry.unit);
            let is_allowed = self.allowed_units.iter().any(|u| {
                let allowed_base = unit_base(u);
                allowed_base == base || u == &entry.unit
            });
            if !is_allowed {
                continue;
            }

            let (scope, event) = self.entry_to_event(&entry);
            self.scryer.push(scope, event)?;
        }

        // Sender dropped — journal stream closed.
        Err(AdapterError::StreamBroken("journald stream closed".into()))
    }
}

// ─── Test helpers ─────────────────────────────────────────────────────────────

#[cfg(test)]
pub mod test_source {
    use super::*;
    use std::sync::Mutex;

    /// Test-only source that hands out pre-canned entry batches.
    ///
    /// Each `subscribe` call consumes the next batch. When exhausted the
    /// returned receiver closes immediately (StreamBroken → supervisor retries
    /// or gives up per max_attempts).
    pub struct ScriptedJournaldSource {
        batches: Mutex<Vec<Vec<JournaldEntry>>>,
    }

    impl ScriptedJournaldSource {
        pub fn new(batches: Vec<Vec<JournaldEntry>>) -> Arc<Self> {
            Arc::new(Self { batches: Mutex::new(batches) })
        }
    }

    #[async_trait]
    impl JournaldSource for ScriptedJournaldSource {
        async fn subscribe(
            &self,
            _units: &[String],
        ) -> Result<mpsc::Receiver<JournaldEntry>, AdapterError> {
            let mut g = self.batches.lock().unwrap();
            if g.is_empty() {
                return Err(AdapterError::StreamBroken("scripted source exhausted".into()));
            }
            let entries = g.remove(0);
            drop(g);

            let (tx, rx) = mpsc::channel::<JournaldEntry>(64);
            tokio::spawn(async move {
                for entry in entries {
                    if tx.send(entry).await.is_err() {
                        break;
                    }
                }
            });
            Ok(rx)
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::test_source::ScriptedJournaldSource;
    use super::*;
    use crate::service::{EventFilter, Scryer, ScryerConfig};
    use observation::Level;
    use tempfile::TempDir;

    fn make_scryer(dir: &TempDir) -> Arc<Scryer> {
        Arc::new(Scryer::new(ScryerConfig::new(dir.path().join("events.db")), None).unwrap())
    }

    fn sshd_entry(msg: &str, priority: u8) -> JournaldEntry {
        JournaldEntry {
            unit: "sshd.service".to_string(),
            priority,
            message: msg.to_string(),
            timestamp_us: 0,
            extra: HashMap::new(),
        }
    }

    fn kernel_entry(msg: &str) -> JournaldEntry {
        JournaldEntry {
            unit: "kernel".to_string(),
            priority: 6,
            message: msg.to_string(),
            timestamp_us: 0,
            extra: HashMap::new(),
        }
    }

    fn cloudflared_entry(msg: &str) -> JournaldEntry {
        JournaldEntry {
            unit: "cloudflared.service".to_string(),
            priority: 6,
            message: msg.to_string(),
            timestamp_us: 0,
            extra: HashMap::new(),
        }
    }

    // ─── host_units ──────────────────────────────────────────────────────────

    /// Verify condition: fixture journald entries for sshd land as
    /// `Service(MeshIdent("sshd.host"))` events.
    #[tokio::test]
    async fn host_units() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let source = ScriptedJournaldSource::new(vec![vec![
            sshd_entry("Accepted publickey for operator", 6),
            sshd_entry("session opened for user operator", 6),
            sshd_entry("POSSIBLE BREAK-IN ATTEMPT!", 4),
        ]]);

        let mut adapter = JournaldAdapter::new(
            scryer.clone(),
            source,
            vec!["sshd".to_string()],
        );

        let result = adapter.run().await;
        assert!(
            matches!(result, Err(AdapterError::StreamBroken(_))),
            "expected StreamBroken on stream close, got {:?}",
            result
        );

        scryer.flush_ring().unwrap();
        let scope = EventScope::Service(MeshIdent("sshd.host".to_string()));
        let events = scryer.events(&scope, &EventFilter::default()).unwrap();

        assert_eq!(events.len(), 3, "all three sshd entries should land");
        assert_eq!(events[0].msg, "Accepted publickey for operator");
        assert_eq!(events[1].msg, "session opened for user operator");
        assert_eq!(events[2].msg, "POSSIBLE BREAK-IN ATTEMPT!");

        // Priority 6 (INFO) → Level::Info; priority 4 (WARNING) → Level::Warn.
        assert_eq!(events[0].level, Level::Info);
        assert_eq!(events[1].level, Level::Info);
        assert_eq!(events[2].level, Level::Warn);

        // Target follows "journald.<unit_base>" convention.
        assert!(events.iter().all(|e| e.target == "journald.sshd"));

        // Fields carry the unit name for agent inspection.
        assert!(events.iter().all(|e| e.fields["unit"] == "sshd.service"));
    }

    // ─── unit suffix stripping ────────────────────────────────────────────────

    /// `.service` suffix is stripped; MeshIdent uses the base name.
    #[tokio::test]
    async fn service_suffix_stripped() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let source = ScriptedJournaldSource::new(vec![vec![
            sshd_entry("login", 6),
        ]]);
        let mut adapter = JournaldAdapter::new(
            scryer.clone(),
            source,
            vec!["sshd.service".to_string()],
        );
        let _ = adapter.run().await;
        scryer.flush_ring().unwrap();

        let scope = EventScope::Service(MeshIdent("sshd.host".to_string()));
        let events = scryer.events(&scope, &EventFilter::default()).unwrap();
        assert_eq!(events.len(), 1);
    }

    // ─── out-of-scope filtering ───────────────────────────────────────────────

    /// Entries for units not in the allow-list are silently dropped — the
    /// journald adapter does NOT ingest yah-managed services (those go through
    /// containerd_logs), cloudflared, or tailscaled.
    #[tokio::test]
    async fn out_of_scope_units_not_ingested() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let source = ScriptedJournaldSource::new(vec![vec![
            sshd_entry("login from 1.2.3.4", 6),
            cloudflared_entry("starting tunnel"),        // NOT in allow-list
            kernel_entry("some kernel message"),         // NOT in allow-list
        ]]);

        let mut adapter = JournaldAdapter::new(
            scryer.clone(),
            source,
            vec!["sshd".to_string()], // only sshd allowed
        );
        let _ = adapter.run().await;
        scryer.flush_ring().unwrap();

        // Only sshd events land.
        let sshd_scope = EventScope::Service(MeshIdent("sshd.host".to_string()));
        let sshd_events = scryer.events(&sshd_scope, &EventFilter::default()).unwrap();
        assert_eq!(sshd_events.len(), 1);

        let cf_scope = EventScope::Service(MeshIdent("cloudflared.host".to_string()));
        let cf_events = scryer.events(&cf_scope, &EventFilter::default()).unwrap();
        assert_eq!(cf_events.len(), 0, "cloudflared must not be ingested via journald");

        let kern_scope = EventScope::Service(MeshIdent("kernel.host".to_string()));
        let kern_events = scryer.events(&kern_scope, &EventFilter::default()).unwrap();
        assert_eq!(kern_events.len(), 0, "kernel not in allow-list");
    }

    // ─── multiple units ───────────────────────────────────────────────────────

    /// When multiple units are in the allow-list, each lands under its own
    /// `<unit>.host` scope with independent seq counters.
    #[tokio::test]
    async fn multiple_units_independent_scopes() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let source = ScriptedJournaldSource::new(vec![vec![
            sshd_entry("sshd msg 1", 6),
            kernel_entry("kernel msg 1"),
            sshd_entry("sshd msg 2", 6),
            kernel_entry("kernel msg 2"),
        ]]);

        let mut adapter = JournaldAdapter::new(
            scryer.clone(),
            source,
            vec!["sshd".to_string(), "kernel".to_string()],
        );
        let _ = adapter.run().await;
        scryer.flush_ring().unwrap();

        let sshd_scope = EventScope::Service(MeshIdent("sshd.host".to_string()));
        let sshd_events = scryer.events(&sshd_scope, &EventFilter::default()).unwrap();
        assert_eq!(sshd_events.len(), 2);
        assert_eq!(sshd_events[0].seq, 0);
        assert_eq!(sshd_events[1].seq, 1);

        let kern_scope = EventScope::Service(MeshIdent("kernel.host".to_string()));
        let kern_events = scryer.events(&kern_scope, &EventFilter::default()).unwrap();
        assert_eq!(kern_events.len(), 2);
        assert_eq!(kern_events[0].seq, 0);
        assert_eq!(kern_events[1].seq, 1);
    }

    // ─── priority mapping ─────────────────────────────────────────────────────

    #[test]
    fn priority_level_map() {
        assert_eq!(priority_level(0), Level::Fatal);  // EMERG
        assert_eq!(priority_level(1), Level::Fatal);  // ALERT
        assert_eq!(priority_level(2), Level::Error);  // CRIT
        assert_eq!(priority_level(3), Level::Error);  // ERR
        assert_eq!(priority_level(4), Level::Warn);   // WARNING
        assert_eq!(priority_level(5), Level::Info);   // NOTICE
        assert_eq!(priority_level(6), Level::Info);   // INFO
        assert_eq!(priority_level(7), Level::Debug);  // DEBUG
    }

    // ─── unit_base helper ─────────────────────────────────────────────────────

    #[test]
    fn unit_base_strips_suffixes() {
        assert_eq!(unit_base("sshd.service"), "sshd");
        assert_eq!(unit_base("sshd"), "sshd");
        assert_eq!(unit_base("kernel"), "kernel");
        assert_eq!(unit_base("network.target"), "network");
        assert_eq!(unit_base("cron.timer"), "cron");
    }

    // ─── extra fields ─────────────────────────────────────────────────────────

    /// Extra SD fields (e.g. `_PID`, `SYSLOG_IDENTIFIER`) land in event.fields.
    #[tokio::test]
    async fn extra_fields_in_event_fields() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let mut extra = HashMap::new();
        extra.insert("_PID".to_string(), "1234".to_string());
        extra.insert("SYSLOG_IDENTIFIER".to_string(), "sshd".to_string());

        let source = ScriptedJournaldSource::new(vec![vec![JournaldEntry {
            unit: "sshd.service".to_string(),
            priority: 6,
            message: "session opened".to_string(),
            timestamp_us: 1_715_000_000_000_000,
            extra,
        }]]);

        let mut adapter =
            JournaldAdapter::new(scryer.clone(), source, vec!["sshd".to_string()]);
        let _ = adapter.run().await;
        scryer.flush_ring().unwrap();

        let scope = EventScope::Service(MeshIdent("sshd.host".to_string()));
        let events = scryer.events(&scope, &EventFilter::default()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].fields["_PID"], "1234");
        assert_eq!(events[0].fields["SYSLOG_IDENTIFIER"], "sshd");
        assert_eq!(events[0].fields["timestamp_us"], 1_715_000_000_000_000u64);
    }
}
