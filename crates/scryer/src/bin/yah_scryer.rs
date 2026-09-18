//! `yah-scryer` — per-node scryer daemon.
//!
//! Started by `yah-scryer.service` (kamaji-managed, see
//! `app/yah/cli/resources/yah-scryer.service` and W264). Owns the local
//! short-disk events.db and serves the federation HTTP surface (R556-F7-T2)
//! on the configured listener.
//!
//! Flags are intentionally minimal — the operator-facing knobs are the
//! systemd ExecStart line, not a sprawling subcommand tree.
//!
//! Args:
//!   --listen <addr>     default `127.0.0.1:6543`. In production this is the
//!                       node's tailnet IP (e.g. `100.64.0.7:6543`).
//!   --data <dir>        default `/var/lib/yah/scryer/`. The short-disk SQLite
//!                       file `events.db` is created under this directory.
//!   --ingest-socket <path>
//!                       bind the local-agent Unix ingestion socket here
//!                       (typically `/run/yah/scryer.sock`). This is what
//!                       `yah-log`'s service layer and passway's span exporter
//!                       write to. OFF by default, and it is half a contract:
//!                       the node's kamaji must be started with
//!                       `--scryer-socket <the same path>` or no workload will
//!                       ever be told where this socket is (R893-B17).
//!
//! `--listen` and `--ingest-socket` are different surfaces, not alternatives.
//! `--listen` is the FEDERATION (cross-machine read) HTTP API; `--ingest-socket`
//! is the local write path. A per-request span emitter paying HTTP framing to
//! reach a collector in its own mount namespace would be strictly worse, which
//! is why R893-F16 chose the socket.
//!
//! Long-tier promotion (R556-F5) — off unless a bucket is configured. When
//! enabled, a background consumer rolls short-disk events older than the
//! retention window into per-day Parquet shards in R2 (the at-rest snapshot
//! source Mode-2 / R556-F6 reads from). Aggregate queries route across the
//! boundary via [`Scryer::with_long_tier`].
//!
//!   --long-tier-bucket <name>     R2 bucket for Parquet shards. Presence of
//!                                 this flag (with --r2-account) enables the
//!                                 long tier + promotion loop.
//!   --r2-account <id>             Cloudflare account id (the subdomain in
//!                                 `<id>.r2.cloudflarestorage.com`). R2
//!                                 credentials come from the vault slots
//!                                 `cloudflare-r2-access-key-id` /
//!                                 `cloudflare-r2-secret-key` (env fallback
//!                                 `CF_R2_ACCESS_KEY_ID` / `CF_R2_SECRET_KEY`).
//!   --machine-id <id>             Stable node id — the shard key prefix
//!                                 (`events/<machine-id>/<day>.parquet`).
//!                                 Defaults to $YAH_MACHINE_ID, then $HOSTNAME.
//!   --retention-days <n>          Short-disk retention / tier boundary in days
//!                                 (default 7). Events older are promoted.
//!   --promote-interval-secs <n>   Promotion cadence (default 3600).
//!
//! Mode-2 analytics snapshots (R556-F6) — off unless `--snapshot-interval-secs`
//! is given (and the long tier is enabled). When on, a background producer
//! aggregates the FULL Parquet corpus — every machine's shards discovered
//! under the bucket's `events/` prefix, not just this node's — into an
//! at-rest JSON snapshot published to R2 (`analytics/current.json` →
//! content-addressed blob), which the managed mesofact analytics server
//! renders behind cheers auth (W234 §Mode-2). Node-agnostic by design:
//! any long-tier node may run the producer and several running at once are
//! redundant but harmless (identical corpora hash to identical blobs).
//!
//!   --snapshot-interval-secs <n>  Snapshot cadence. Enables the producer.
//!
//! Exit codes:
//!   0  clean shutdown (SIGINT/SIGTERM)
//!   1  unrecoverable startup error (bind failure, db open failure, bad
//!      long-tier config, etc.)

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use yah_object_store::R2ObjectStore;
use yah_scryer::{
    FederationState, IngestionServer, LongTierConfig, LongTierStore, MS_PER_DAY, ObjectStore,
    OperatorTagAcl,
    PromotionConfig, PromotionConsumer, Scryer, ScryerConfig, SnapshotConfig, SnapshotProducer,
    serve_federation,
};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let listen = parse_arg(&args, "--listen").unwrap_or_else(|| "127.0.0.1:6543".to_string());
    let data_dir =
        parse_arg(&args, "--data").unwrap_or_else(|| "/var/lib/yah/scryer/".to_string());
    // R893-B17. Opt-in rather than defaulted: the default would be a path under
    // /run that does not exist on a dev box, and a collector that fails to
    // start is worse than one that was never asked to.
    let ingest_socket = parse_arg(&args, "--ingest-socket").map(PathBuf::from);

    let addr: SocketAddr = match listen.parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("yah-scryer: invalid --listen {listen:?}: {e}");
            return ExitCode::from(1);
        }
    };

    let data_path = PathBuf::from(&data_dir);
    if let Err(e) = std::fs::create_dir_all(&data_path) {
        eprintln!("yah-scryer: cannot create data dir {data_path:?}: {e}");
        return ExitCode::from(1);
    }

    let cfg = ScryerConfig::new(data_path.join("events.db"));
    let mut scryer = match Scryer::new(cfg, None) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("yah-scryer: cannot open events.db: {e}");
            return ExitCode::from(1);
        }
    };

    // Optional long tier: enabled when both --long-tier-bucket and --r2-account
    // are supplied. Builds the R2-backed store, wires it for aggregate routing,
    // and hands a clone to the promotion consumer spawned below.
    let retention_days: u64 = parse_arg(&args, "--retention-days")
        .and_then(|s| s.parse().ok())
        .unwrap_or(7);
    let retention_ms = retention_days.saturating_mul(MS_PER_DAY);
    let promote_interval = std::time::Duration::from_secs(
        parse_arg(&args, "--promote-interval-secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(3600),
    );
    // R556-F6: opt-in Mode-2 analytics snapshot producer. Presence of the flag
    // (with the long tier enabled) spawns a loop that aggregates this node's
    // Parquet corpus into an at-rest snapshot published to R2 for the managed
    // mesofact analytics server to render (W234 §Mode-2).
    let snapshot_interval: Option<std::time::Duration> = parse_arg(&args, "--snapshot-interval-secs")
        .and_then(|s| s.parse::<u64>().ok())
        .map(std::time::Duration::from_secs);

    let (long_tier, snapshot): (Option<Arc<LongTierStore>>, Option<SnapshotProducer>) = match (
        parse_arg(&args, "--long-tier-bucket"),
        parse_arg(&args, "--r2-account"),
    ) {
        (Some(bucket), Some(account)) => {
            let machine_id = parse_arg(&args, "--machine-id")
                .or_else(|| std::env::var("YAH_MACHINE_ID").ok())
                .or_else(|| std::env::var("HOSTNAME").ok())
                .or_else(os_hostname)
                .unwrap_or_default();
            if machine_id.is_empty() {
                eprintln!(
                    "yah-scryer: long tier requires a machine id — pass --machine-id \
                     (a systemd drop-in should use `--machine-id %H`), or set \
                     $YAH_MACHINE_ID, or give this host a readable hostname"
                );
                return ExitCode::from(1);
            }
            let obj_store = match R2ObjectStore::from_vault(account, bucket) {
                Ok(s) => Arc::new(s) as Arc<dyn ObjectStore>,
                Err(e) => {
                    eprintln!("yah-scryer: cannot build R2 object store: {e}");
                    return ExitCode::from(1);
                }
            };
            // Empty machine list = DISCOVER: the producer aggregates every
            // machine with shards under the bucket's `events/` prefix, not
            // just this node's, so any long-tier node runs it and publishes
            // the same full-corpus snapshot (operator decision 2026-09-04,
            // R556-F6: "there should be nothing in our system that requires
            // a specific node"; see SnapshotConfig::machines).
            let snapshot = snapshot_interval.map(|iv| {
                SnapshotProducer::new(
                    Arc::clone(&obj_store),
                    SnapshotConfig::new(Vec::new(), retention_ms).with_interval(iv),
                )
            });
            let lt = Arc::new(LongTierStore::new(
                LongTierConfig { machine_id, retention_ms },
                obj_store,
            ));
            scryer = scryer.with_long_tier(Arc::clone(&lt), retention_ms);
            (Some(lt), snapshot)
        }
        (Some(_), None) | (None, Some(_)) => {
            eprintln!(
                "yah-scryer: long tier needs both --long-tier-bucket and --r2-account; \
                 ignoring partial config"
            );
            (None, None)
        }
        (None, None) => (None, None),
    };

    let scryer = Arc::new(scryer);
    let promo_scryer = Arc::clone(&scryer);
    let ingest_scryer = Arc::clone(&scryer);
    let state = FederationState::new(scryer, Arc::new(OperatorTagAcl));
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("yah-scryer: cannot build tokio runtime: {e}");
            return ExitCode::from(1);
        }
    };

    runtime.block_on(async move {
        let (local, handle) = match serve_federation(state, addr).await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("yah-scryer: bind {addr} failed: {e}");
                return ExitCode::from(1);
            }
        };
        eprintln!("yah-scryer listening on {local}");

        // R893-B17: the local write path. Bound before anything else is
        // spawned and FAILED LOUDLY, for the same reason the federation bind
        // above is: an operator who passed --ingest-socket asked for workload
        // telemetry, and a daemon that came up serving reads while silently
        // ingesting nothing is the exact silent-nowhere failure this ticket
        // removes. Pair it with `kamaji --scryer-socket <the same path>`.
        if let Some(path) = &ingest_socket {
            let server = IngestionServer::new(ingest_scryer, path);
            // Bind synchronously so a failure is a startup error rather than a
            // background task nobody reads the result of.
            match server.bind().await {
                Ok(bound) => {
                    eprintln!("yah-scryer ingesting on {}", path.display());
                    tokio::spawn(async move {
                        if let Err(e) = bound.serve().await {
                            eprintln!("yah-scryer: ingestion socket stopped: {e}");
                        }
                    });
                }
                Err(e) => {
                    eprintln!(
                        "yah-scryer: cannot bind ingestion socket {}: {e}",
                        path.display()
                    );
                    return ExitCode::from(1);
                }
            }
        }

        if let Some(lt) = long_tier {
            eprintln!(
                "yah-scryer: long-tier promotion enabled (retention {retention_days}d, \
                 interval {}s)",
                promote_interval.as_secs()
            );
            let cfg = PromotionConfig::new(retention_ms).with_interval(promote_interval);
            PromotionConsumer::new(promo_scryer, lt, cfg).spawn();
        }

        if let Some(producer) = snapshot {
            eprintln!(
                "yah-scryer: Mode-2 analytics snapshot producer enabled (interval {}s)",
                snapshot_interval.map(|d| d.as_secs()).unwrap_or(0)
            );
            producer.spawn();
        }

        // Wait for either the server to exit on its own or a shutdown signal.
        tokio::select! {
            _ = handle => {}
            _ = shutdown_signal() => {
                eprintln!("yah-scryer: shutdown signal received");
            }
        }
        ExitCode::SUCCESS
    })
}

/// Last-resort machine id: this host's kernel hostname, read from the
/// filesystem so the binary keeps its zero-C-dependency posture.
///
/// WHY THIS EXISTS — the `$HOSTNAME` link above is a lie in the one place that
/// matters. `HOSTNAME` is a variable an interactive **shell** exports; systemd
/// does not put it in a unit's environment. So `yah-scryer.service`'s own
/// comment, and R556-F6's mirror prose, both promised "machine-id defaults from
/// $HOSTNAME" for a daemon that could never see it, and the first drop-in
/// written against that promise crash-looped on us-east-001 (2026-09-10) with
/// the error two lines up. A drop-in should still pass `--machine-id %H`
/// explicitly — systemd's own specifier is clearer than an implicit fallback,
/// and it works on the releases already on the fleet — but a daemon whose doc
/// comment claims a default should actually have one.
///
/// Linux-only by construction, which is the whole deployment surface: these
/// units run on musl fleet nodes. A dev box that somehow reaches this arm gets
/// the explicit error rather than a wrong id.
fn os_hostname() -> Option<String> {
    for path in ["/proc/sys/kernel/hostname", "/etc/hostname"] {
        if let Ok(s) = std::fs::read_to_string(path) {
            let s = s.trim();
            if !s.is_empty() {
                return Some(s.to_string());
            }
        }
    }
    None
}

fn parse_arg(args: &[String], name: &str) -> Option<String> {
    let mut iter = args.iter().peekable();
    while let Some(arg) = iter.next() {
        if arg == name {
            return iter.next().cloned();
        }
        if let Some(rest) = arg.strip_prefix(&format!("{name}=")) {
            return Some(rest.to_string());
        }
    }
    None
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
