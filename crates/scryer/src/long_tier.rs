//! Long-tier Parquet storage — R093-F5.
//!
//! The long tier is **optional and off by default** (§Storage tiers).  It earns
//! its keep when the operator wants cross-week aggregates and is willing to pay
//! R2 / MinIO + Parquet shard cost.  Most camps never enable it.
//!
//! # Shard layout
//!
//! ```text
//! events/{machine_id}/{day_number}.parquet
//! ```
//!
//! `day_number = event.offset_ms / MS_PER_DAY`.  Day-aligned shards are the
//! simplest choice (arch doc Open questions: size-aligned would compress better
//! for chatty services; revisit if storage cost becomes load-bearing).
//!
//! # ObjectStore abstraction
//!
//! Production wires `S3ObjectStore` (R2 / MinIO via AWS Sig V4).  Tests use
//! [`InMemoryObjectStore`] — same code path, zero network.
//!
//! @yah:relay(R498, "object-store crate: lift ObjectStore trait + R2 impl + bucket CLI + data-tab viewer")
//! @yah:at(2026-06-09T03:28:56Z)
//! @yah:status(open)
//! @yah:next("Lift ObjectStore trait + InMemoryObjectStore from yah_scryer::long_tier into new crates/yah/object-store/ crate; add head/delete; generic Error; re-export from scryer for back-compat")
//! @yah:next("R2ObjectStore impl in object-store crate wrapping local_driver::s3_sign; constructor reads cloudflare-r2-{access-key-id,secret-key} vault slots with env fallback; tests")
//! @yah:next("yah cloud bucket put|get|ls|head <key> [--bucket B] [--file F] CLI on top of R2ObjectStore")
//! @yah:next("First user: push yubaba-v0.8.9-{x86_64,aarch64}-unknown-linux-musl.tar.gz from GH Release v0.8.9 to yah-dev/yubaba/0.8.9/{triple}/ + write yubaba/release-manifest.json")
//! @yah:next("Refactor cloud::reconciler::r2_publish::publish_to_r2 inline SigV4 PUT loop onto R2ObjectStore")
//! @yah:next("Data-tab bucket viewer: tauri command exposing list_prefix+head+get; React panel with bucket picker + prefix nav + object list + byte preview")
//! @yah:gotcha("yah_scryer::long_tier currently owns ObjectStore + InMemoryObjectStore; F1 must re-export from scryer to keep LongTierStore callers compiling")
//! @yah:gotcha("GHA tarball is named yubaba-v0.8.9-{triple}.tar.gz but P007 documents the published key as yah-yubaba-{triple}.tar.gz — pick one convention in T4 and stick with it (lean toward dropping the v prefix to match the documented layout)")
//! @yah:assumes("ObjectStore trait shape (put/get/list_prefix) is the right starting point; head+delete are additions, not redesigns")
//! @arch:see(.yah/qed/P007-yubaba-release.toml)
//! @arch:see(crates/yah/cloud/src/reconciler/r2_publish.rs)
//!
//! @yah:ticket(R498-F2, "R2ObjectStore impl over local_driver::s3_sign")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-09T03:29:17Z)
//! @yah:status(review)
//! @yah:parent(R498)
//! @yah:next("Add R2ObjectStore struct to crates/yah/object-store/ with fields: account_id, bucket, access_key, secret_key, http client")
//! @yah:next("Constructor R2ObjectStore::from_vault(bucket) reads cloudflare-r2-access-key-id + cloudflare-r2-secret-key via keys::get_or_env; env fallback CF_R2_ACCESS_KEY_ID / CF_R2_SECRET_KEY")
//! @yah:next("Implement put/get/head/delete/list_prefix via local_driver::s3_sign (sign_s3_put_object, sign_s3_empty_body for HEAD/DELETE/GET, ?list-type=2 for list_prefix)")
//! @yah:next("Offline unit tests on canonical request strings (mirror local_driver::s3_sign style); one online integration test gated on env var YAH_R2_LIVE_TEST=1")
//! @yah:verify("cargo test -p object-store --lib ok")
//! @yah:gotcha("list_prefix needs SigV4 over the ?list-type=2&prefix=... query — query-string signing path. Existing s3_sign helpers may need a new sign_s3_get_with_query variant")
//! @yah:depends_on(R498-F1)
//! @yah:tier(Warrior)
//! @yah:handoff("R2ObjectStore landed in crates/yah/object-store/src/r2.rs. Public API: R2ObjectStore::new(account_id, bucket, access_key, secret_key) for explicit keys, R2ObjectStore::from_vault(account_id, bucket) for the keystore path (slots cloudflare-r2-access-key-id + cloudflare-r2-secret-key, env fallback CF_R2_ACCESS_KEY_ID / CF_R2_SECRET_KEY). Region pinned to 'auto' (R2 convention). Re-exported at yah_object_store::R2ObjectStore. Implements all five ObjectStore methods: put via sign_s3_put_object + body-sha256, get/head/delete via sign_s3_empty_body (HEAD returns true/false on 200/404), delete is idempotent (200/204/404 all succeed), list_prefix issues GET ?list-type=2&prefix=... via sign_s3_get_with_query and pages with continuation-token until IsTruncated=false. Status mapping: 401/403 → Error::Auth, 404 → Error::NotFound, other non-2xx → Error::Backend with first 200 chars of response body. Tiny inline XML extractor (extract_all_tags / extract_first_tag) — no quick-xml dep. HTTP via reqwest::blocking with rustls-tls (avoids OpenSSL on musl). Deps added to object-store/Cargo.toml: reqwest 0.12 (blocking + rustls-tls), sha2, hex, percent-encoding, local-driver (s3_sign), keys (vault). 10/10 tests pass (5 base + parse_list_v2 × 3 + construction × 2). cargo check --workspace exit 0. No online integration test yet — first real exercise is T4 (yubaba push).")
//! @yah:verify("cargo test -p yah-object-store --lib  # 10/10 ok")
//! @yah:verify("cargo check --workspace  # exit 0")

use crate::store::{EventStore, ScryerStoreError};
use arrow_array::{
    BooleanArray, Int64Array, StringArray, UInt32Array,
    cast::AsArray,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use observation::{ChunkRef, Event, EventScope, EventSource, ForgeId, Level, TaskRunId};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use workload_spec::MeshIdent;

// Re-export from the new yah-object-store crate. R498-F1 lifted the trait
// + InMemoryObjectStore out of this file; the re-exports keep existing
// `use yah_scryer::long_tier::{ObjectStore, InMemoryObjectStore}` call sites
// compiling unchanged.
pub use yah_object_store::{Error as ObjectStoreError, InMemoryObjectStore, ObjectStore};

// ─── Constants ────────────────────────────────────────────────────────────────

pub const MS_PER_DAY: u64 = 24 * 3_600 * 1_000;

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum LongTierError {
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("store: {0}")]
    Store(#[from] ScryerStoreError),
    #[error("object store: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("decode: {0}")]
    Decode(String),
}

// ─── Config ───────────────────────────────────────────────────────────────────

/// Configuration for the long-tier Parquet store.
///
/// Enabled is opt-in per arch doc §Storage tiers.  `retention_ms` mirrors the
/// short-disk retention so rollover knows which events to promote.
#[derive(Debug, Clone)]
pub struct LongTierConfig {
    /// Stable machine identifier — part of every shard key.
    pub machine_id: String,
    /// Short-disk retention window in ms (default 7d).  Events older than this
    /// are eligible for Parquet rollover.
    pub retention_ms: u64,
}

impl LongTierConfig {
    pub fn new(machine_id: impl Into<String>) -> Self {
        Self { machine_id: machine_id.into(), retention_ms: 7 * MS_PER_DAY }
    }
}

// ─── LongTierStore ────────────────────────────────────────────────────────────

/// Long-tier Parquet store.
///
/// Wraps an [`ObjectStore`] backend and owns the shard key scheme.  A single
/// `LongTierStore` instance lives inside [`crate::service::Scryer`] when the
/// operator enables the long tier.
pub struct LongTierStore {
    cfg: LongTierConfig,
    object_store: Arc<dyn ObjectStore>,
}

impl LongTierStore {
    pub fn new(cfg: LongTierConfig, object_store: Arc<dyn ObjectStore>) -> Self {
        Self { cfg, object_store }
    }

    fn shard_key(&self, day: u64) -> String {
        format!("events/{}/{}.parquet", self.cfg.machine_id, day)
    }

    /// Promote events older than `cutoff_ms` from short-disk into Parquet shards.
    ///
    /// Steps:
    ///  1. Read all events with `offset_ms < cutoff_ms` from short-disk.
    ///  2. Group by day (`offset_ms / MS_PER_DAY`).
    ///  3. Merge each day's new events with any existing shard and write back.
    ///  4. Delete the promoted events from short-disk.
    ///
    /// Returns the number of events deleted from short-disk.
    pub fn rollover(
        &self,
        event_store: &EventStore,
        cutoff_ms: u64,
    ) -> Result<usize, LongTierError> {
        let old_items = event_store.query_events_older_than(cutoff_ms)?;
        if old_items.is_empty() {
            return Ok(0);
        }

        // Group by day.
        let mut by_day: BTreeMap<u64, Vec<(EventScope, Event)>> = BTreeMap::new();
        for item in &old_items {
            let day = item.1.offset_ms as u64 / MS_PER_DAY;
            by_day.entry(day).or_default().push(item.clone());
        }

        // Write (or merge with existing) shard for each day.
        for (day, new_items) in &by_day {
            let key = self.shard_key(*day);
            let to_write = if let Some(existing_bytes) =
                self.object_store.get(&key)?
            {
                // Merge: existing shard rows + new rows, deduplicated by primary key.
                let mut merged = read_shard_bytes(existing_bytes)?;
                merged.extend_from_slice(new_items);
                merged.sort_by(|(sa, ea), (sb, eb)| {
                    sa.kind_str()
                        .cmp(sb.kind_str())
                        .then(sa.id_str().cmp(&sb.id_str()))
                        .then(ea.seq.cmp(&eb.seq))
                });
                merged.dedup_by(|(sa, ea), (sb, eb)| {
                    sa.kind_str() == sb.kind_str()
                        && sa.id_str() == sb.id_str()
                        && ea.seq == eb.seq
                });
                merged
            } else {
                new_items.clone()
            };

            let shard_bytes = write_shard_bytes(&to_write)?;
            self.object_store.put(&key, shard_bytes)?;
        }

        // Prune promoted events from short-disk.
        let pruned = event_store.prune_older_than(cutoff_ms)?;
        Ok(pruned)
    }

    /// Query events from long-tier Parquet shards covering `[since_ms, until_ms)`.
    ///
    /// If `scope` is given, only events matching that scope are returned.
    /// Returns events in order `(scope_kind, scope_id, seq)`.
    pub fn query_range(
        &self,
        scope: Option<&EventScope>,
        since_ms: u64,
        until_ms: u64,
    ) -> Result<Vec<(EventScope, Event)>, LongTierError> {
        let since_day = since_ms / MS_PER_DAY;
        let until_day = until_ms.saturating_sub(1) / MS_PER_DAY; // inclusive end

        let mut results = Vec::new();
        for day in since_day..=until_day {
            let key = self.shard_key(day);
            let data = match self.object_store.get(&key)? {
                Some(d) => d,
                None => continue,
            };
            for (ev_scope, ev) in read_shard_bytes(data)? {
                let off = ev.offset_ms as u64;
                let in_range = off >= since_ms && off < until_ms;
                let scope_match = scope.map_or(true, |s| &ev_scope == s);
                if in_range && scope_match {
                    results.push((ev_scope, ev));
                }
            }
        }
        Ok(results)
    }
}

// ─── Parquet schema ───────────────────────────────────────────────────────────

fn events_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("scope_kind", DataType::Utf8, false),
        Field::new("scope_id", DataType::Utf8, false),
        Field::new("seq", DataType::UInt32, false),
        Field::new("offset_ms", DataType::UInt32, false),
        Field::new("level", DataType::Utf8, false),
        Field::new("target", DataType::Utf8, false),
        Field::new("msg", DataType::Utf8, false),
        Field::new("fields_json", DataType::Utf8, false),
        // nullable: None when the event has no anchor
        Field::new("has_anchor", DataType::Boolean, false),
        Field::new("anchor_seq", DataType::Int64, false), // 0 when no anchor
        Field::new("source_kind", DataType::Utf8, false),
        Field::new("source_name", DataType::Utf8, false),
    ]))
}

// ─── Parquet write ────────────────────────────────────────────────────────────

/// Serialize `items` to a Parquet byte buffer.
fn write_shard_bytes(items: &[(EventScope, Event)]) -> Result<Vec<u8>, LongTierError> {
    if items.is_empty() {
        // Write a valid empty Parquet file.
        let schema = events_schema();
        let mut buf = Vec::new();
        let writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)?;
        writer.close()?;
        return Ok(buf);
    }

    let schema = events_schema();

    // Collect columns.
    let scope_kind_col: StringArray =
        items.iter().map(|(s, _)| s.kind_str()).collect::<Vec<&str>>().into();
    let scope_id_vals: Vec<String> = items.iter().map(|(s, _)| s.id_str()).collect();
    let scope_id_col: StringArray =
        scope_id_vals.iter().map(|s| s.as_str()).collect::<Vec<&str>>().into();
    let seq_col: UInt32Array = items.iter().map(|(_, e)| e.seq).collect();
    let offset_ms_col: UInt32Array = items.iter().map(|(_, e)| e.offset_ms).collect();
    let level_col: StringArray =
        items.iter().map(|(_, e)| e.level.as_str()).collect::<Vec<&str>>().into();
    let target_vals: Vec<&str> = items.iter().map(|(_, e)| e.target.as_str()).collect();
    let target_col: StringArray = target_vals.into();
    let msg_vals: Vec<&str> = items.iter().map(|(_, e)| e.msg.as_str()).collect();
    let msg_col: StringArray = msg_vals.into();
    let fields_vals: Vec<String> =
        items.iter().map(|(_, e)| e.fields.to_string()).collect();
    let fields_col: StringArray =
        fields_vals.iter().map(|s| s.as_str()).collect::<Vec<&str>>().into();
    let has_anchor_col: BooleanArray = items.iter().map(|(_, e)| e.anchor.is_some()).collect();
    let anchor_seq_col: Int64Array = items
        .iter()
        .map(|(_, e)| e.anchor.as_ref().map(|a| a.seq as i64).unwrap_or(0))
        .collect();
    let source_kind_col: StringArray =
        items.iter().map(|(_, e)| e.source.kind_str()).collect::<Vec<&str>>().into();
    let source_name_vals: Vec<&str> =
        items.iter().map(|(_, e)| e.source.name_str()).collect();
    let source_name_col: StringArray = source_name_vals.into();

    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(scope_kind_col),
            Arc::new(scope_id_col),
            Arc::new(seq_col),
            Arc::new(offset_ms_col),
            Arc::new(level_col),
            Arc::new(target_col),
            Arc::new(msg_col),
            Arc::new(fields_col),
            Arc::new(has_anchor_col),
            Arc::new(anchor_seq_col),
            Arc::new(source_kind_col),
            Arc::new(source_name_col),
        ],
    )?;

    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buf)
}

// ─── Parquet read ─────────────────────────────────────────────────────────────

/// Deserialize a Parquet byte buffer back into `(EventScope, Event)` pairs.
fn read_shard_bytes(data: Vec<u8>) -> Result<Vec<(EventScope, Event)>, LongTierError> {
    let bytes = Bytes::from(data);
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(LongTierError::Parquet)?
        .with_batch_size(4096)
        .build()
        .map_err(LongTierError::Parquet)?;

    let mut results = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(LongTierError::Arrow)?;
        let n = batch.num_rows();
        if n == 0 {
            continue;
        }

        let scope_kind_col = batch.column(0).as_string::<i32>();
        let scope_id_col = batch.column(1).as_string::<i32>();
        let seq_col = batch.column(2).as_primitive::<arrow_array::types::UInt32Type>();
        let offset_ms_col = batch.column(3).as_primitive::<arrow_array::types::UInt32Type>();
        let level_col = batch.column(4).as_string::<i32>();
        let target_col = batch.column(5).as_string::<i32>();
        let msg_col = batch.column(6).as_string::<i32>();
        let fields_col = batch.column(7).as_string::<i32>();
        let has_anchor_col = batch.column(8).as_boolean();
        let anchor_seq_col = batch.column(9).as_primitive::<arrow_array::types::Int64Type>();
        let source_kind_col = batch.column(10).as_string::<i32>();
        let source_name_col = batch.column(11).as_string::<i32>();

        for i in 0..n {
            let scope_kind = scope_kind_col.value(i);
            let scope_id = scope_id_col.value(i);
            let scope = decode_scope(scope_kind, scope_id)?;

            let seq = seq_col.value(i);
            let offset_ms = offset_ms_col.value(i);
            let level_str = level_col.value(i);
            let level =
                Level::from_str(level_str).map_err(|e| LongTierError::Decode(e))?;
            let target = target_col.value(i).to_string();
            let msg = msg_col.value(i).to_string();
            let fields_str = fields_col.value(i);
            let fields: Value = serde_json::from_str(fields_str)
                .unwrap_or(serde_json::json!({}));
            let has_anchor = has_anchor_col.value(i);
            let anchor = if has_anchor {
                Some(ChunkRef { seq: anchor_seq_col.value(i) as u32 })
            } else {
                None
            };
            let source_kind_str = source_kind_col.value(i);
            let source_name_str = source_name_col.value(i).to_string();
            let source = decode_source(source_kind_str, source_name_str);

            // run_id: for non-TaskRun scopes, use a synthetic UUID derived from scope_id.
            let run_id = match &scope {
                EventScope::TaskRun(id) => id.clone(),
                EventScope::Forge(id) => TaskRunId(id.0),
                EventScope::Service(_) => scope_id
                    .parse::<TaskRunId>()
                    .unwrap_or_else(|_| TaskRunId::new()),
            };

            results.push((
                scope,
                Event { run_id, seq, offset_ms, level, target, msg, fields, anchor, source },
            ));
        }
    }
    Ok(results)
}

fn decode_scope(kind: &str, id: &str) -> Result<EventScope, LongTierError> {
    match kind {
        "task_run" => id
            .parse::<TaskRunId>()
            .map(EventScope::TaskRun)
            .map_err(|e| LongTierError::Decode(e.to_string())),
        "service" => Ok(EventScope::Service(MeshIdent(id.to_string()))),
        "forge" => id
            .parse::<ForgeId>()
            .map(EventScope::Forge)
            .map_err(|e| LongTierError::Decode(e.to_string())),
        other => Err(LongTierError::Decode(format!("unknown scope kind: {other}"))),
    }
}

fn decode_source(kind: &str, name: String) -> EventSource {
    match kind {
        "beholder" => EventSource::Beholder { name, version: String::new() },
        "shim" => EventSource::Shim { lib: name, version: String::new() },
        _ => EventSource::Synth,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use observation::{EventSource, Level, TaskRunId};
    use serde_json::json;
    use tempfile::TempDir;

    fn make_event(run_id: &TaskRunId, seq: u32, offset_ms: u32, level: Level) -> Event {
        Event {
            run_id: run_id.clone(),
            seq,
            offset_ms,
            level,
            target: format!("test::{seq}"),
            msg: format!("msg {seq}"),
            fields: json!({"seq": seq}),
            anchor: None,
            source: EventSource::Synth,
        }
    }

    fn make_store(dir: &TempDir) -> EventStore {
        EventStore::open(&dir.path().join("events.db")).unwrap()
    }

    fn make_lt(machine_id: &str) -> (LongTierStore, Arc<InMemoryObjectStore>) {
        let obj_store = Arc::new(InMemoryObjectStore::new());
        let cfg =
            LongTierConfig { machine_id: machine_id.to_string(), retention_ms: 7 * MS_PER_DAY };
        let lt = LongTierStore::new(cfg, Arc::clone(&obj_store) as Arc<dyn ObjectStore>);
        (lt, obj_store)
    }

    /// Verify condition 1 (R093-F5):
    /// Events older than retention land in Parquet shards keyed by (machine, day);
    /// short-disk reflects deletion.
    #[test]
    fn rollover() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let (lt, obj_store) = make_lt("m1");

        let scope_a = EventScope::Service(MeshIdent("svc-a.prod".to_string()));
        let run_id_a = TaskRunId::new();

        // Old events: offset_ms = 1 day (86_400_000 ms) — below 7-day cutoff.
        let old_cutoff_ms = 7 * MS_PER_DAY; // 604_800_000
        let day0_offset = MS_PER_DAY as u32; // 86_400_000

        let old_events: Vec<(EventScope, Event)> = (0u32..5)
            .map(|i| (scope_a.clone(), make_event(&run_id_a, i, day0_offset, Level::Warn)))
            .collect();
        store.insert_events(&old_events).unwrap();
        assert_eq!(store.count().unwrap(), 5, "5 events in short-disk before rollover");

        // Also insert a recent event (offset_ms = 10 days) — should NOT be rolled.
        let recent_events = vec![(
            scope_a.clone(),
            make_event(&run_id_a, 99, (10 * MS_PER_DAY) as u32, Level::Info),
        )];
        store.insert_events(&recent_events).unwrap();
        assert_eq!(store.count().unwrap(), 6);

        // Rollover at 7-day boundary.
        let promoted = lt.rollover(&store, old_cutoff_ms).unwrap();
        assert_eq!(promoted, 5, "5 old events should have been promoted");

        // Short-disk should only hold the 1 recent event.
        assert_eq!(store.count().unwrap(), 1, "1 recent event remains in short-disk");

        // Parquet shard for day 1 (offset_ms / MS_PER_DAY = 1) must exist.
        let shard_key = format!("events/m1/1.parquet");
        assert!(
            obj_store.contains_key(&shard_key),
            "Parquet shard events/m1/1.parquet should exist"
        );

        // Read the shard back and verify all 5 events are there.
        let shard_data = obj_store.get(&shard_key).unwrap().unwrap();
        let shard_events = read_shard_bytes(shard_data).unwrap();
        assert_eq!(shard_events.len(), 5, "shard should hold all 5 old events");
        assert!(
            shard_events.iter().all(|(_, e)| e.level == Level::Warn),
            "all shard events should be Warn"
        );

        // Shard for day 10 (recent event) must NOT exist yet.
        let recent_shard = format!("events/m1/10.parquet");
        assert!(
            !obj_store.contains_key(&recent_shard),
            "recent event shard should not exist"
        );
    }

    /// Rollover is idempotent: re-rolling the same events merges without duplicates.
    #[test]
    fn rollover_idempotent() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let (lt, obj_store) = make_lt("m2");

        let scope = EventScope::Service(MeshIdent("svc.local".to_string()));
        let run_id = TaskRunId::new();
        let items: Vec<(EventScope, Event)> =
            (0u32..3).map(|i| (scope.clone(), make_event(&run_id, i, 1000, Level::Info))).collect();
        store.insert_events(&items).unwrap();

        lt.rollover(&store, 7 * MS_PER_DAY).unwrap();
        // short-disk is empty; re-insert same events and roll again.
        store.insert_events(&items).unwrap();
        lt.rollover(&store, 7 * MS_PER_DAY).unwrap();

        let key = "events/m2/0.parquet".to_string();
        let data = obj_store.get(&key).unwrap().unwrap();
        let shard = read_shard_bytes(data).unwrap();
        assert_eq!(shard.len(), 3, "merge must deduplicate — still 3 events");
    }

    /// Parquet round-trip: write then read yields identical events.
    #[test]
    fn parquet_round_trip() {
        let run_id = TaskRunId::new();
        let scope = EventScope::Service(MeshIdent("svc.rt".to_string()));
        let events: Vec<(EventScope, Event)> = vec![
            (scope.clone(), make_event(&run_id, 0, 100, Level::Info)),
            (
                scope.clone(),
                Event {
                    run_id: run_id.clone(),
                    seq: 1,
                    offset_ms: 200,
                    level: Level::Error,
                    target: "app::db".to_string(),
                    msg: "connection refused".to_string(),
                    fields: json!({"error": {"code": "ECONNREFUSED"}}),
                    anchor: Some(ChunkRef { seq: 42 }),
                    source: EventSource::Beholder {
                        name: "pino".to_string(),
                        version: "1.0".to_string(),
                    },
                },
            ),
        ];

        let bytes = write_shard_bytes(&events).unwrap();
        let recovered = read_shard_bytes(bytes).unwrap();

        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].1.seq, 0);
        assert_eq!(recovered[0].1.level, Level::Info);
        assert_eq!(recovered[1].1.seq, 1);
        assert_eq!(recovered[1].1.level, Level::Error);
        assert_eq!(recovered[1].1.target, "app::db");
        assert_eq!(recovered[1].1.anchor.as_ref().unwrap().seq, 42);
        // Beholder source kind preserved.
        assert_eq!(recovered[1].1.source.kind_str(), "beholder");
    }

    /// `query_range` returns only events within the ms range.
    #[test]
    fn query_range_filters_by_range() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let (lt, _) = make_lt("m3");

        let scope = EventScope::Service(MeshIdent("svc.range".to_string()));
        let run_id = TaskRunId::new();

        // Day 0 events (offset_ms = 100).
        let day0: Vec<(EventScope, Event)> =
            (0u32..3).map(|i| (scope.clone(), make_event(&run_id, i, 100, Level::Info))).collect();
        // Day 2 events (offset_ms = 2 days + 1ms).
        let day2_ms = (2 * MS_PER_DAY + 1) as u32;
        let day2: Vec<(EventScope, Event)> = (3u32..5)
            .map(|i| (scope.clone(), make_event(&run_id, i, day2_ms, Level::Warn)))
            .collect();

        store.insert_events(&day0).unwrap();
        store.insert_events(&day2).unwrap();
        lt.rollover(&store, 7 * MS_PER_DAY).unwrap();

        // Query only day 0 (since_ms=0, until_ms=MS_PER_DAY).
        let results = lt.query_range(Some(&scope), 0, MS_PER_DAY).unwrap();
        assert_eq!(results.len(), 3, "only day-0 events in range");
        assert!(results.iter().all(|(_, e)| e.level == Level::Info));

        // Query day 2 only.
        let results = lt.query_range(Some(&scope), 2 * MS_PER_DAY, 3 * MS_PER_DAY).unwrap();
        assert_eq!(results.len(), 2, "only day-2 events in range");
        assert!(results.iter().all(|(_, e)| e.level == Level::Warn));
    }
}
