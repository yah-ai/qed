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
    FederationState, LongTierConfig, LongTierStore, MS_PER_DAY, ObjectStore, OperatorTagAcl,
    PromotionConfig, PromotionConsumer, Scryer, ScryerConfig, serve_federation,
};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let listen = parse_arg(&args, "--listen").unwrap_or_else(|| "127.0.0.1:6543".to_string());
    let data_dir =
        parse_arg(&args, "--data").unwrap_or_else(|| "/var/lib/yah/scryer/".to_string());

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

    let long_tier: Option<Arc<LongTierStore>> = match (
        parse_arg(&args, "--long-tier-bucket"),
        parse_arg(&args, "--r2-account"),
    ) {
        (Some(bucket), Some(account)) => {
            let machine_id = parse_arg(&args, "--machine-id")
                .or_else(|| std::env::var("YAH_MACHINE_ID").ok())
                .or_else(|| std::env::var("HOSTNAME").ok())
                .unwrap_or_default();
            if machine_id.is_empty() {
                eprintln!(
                    "yah-scryer: long tier requires a machine id — pass --machine-id \
                     or set $YAH_MACHINE_ID / $HOSTNAME"
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
            let lt = Arc::new(LongTierStore::new(
                LongTierConfig { machine_id, retention_ms },
                obj_store,
            ));
            scryer = scryer.with_long_tier(Arc::clone(&lt), retention_ms);
            Some(lt)
        }
        (Some(_), None) | (None, Some(_)) => {
            eprintln!(
                "yah-scryer: long tier needs both --long-tier-bucket and --r2-account; \
                 ignoring partial config"
            );
            None
        }
        (None, None) => None,
    };

    let scryer = Arc::new(scryer);
    let promo_scryer = Arc::clone(&scryer);
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

        if let Some(lt) = long_tier {
            eprintln!(
                "yah-scryer: long-tier promotion enabled (retention {retention_days}d, \
                 interval {}s)",
                promote_interval.as_secs()
            );
            let cfg = PromotionConfig::new(retention_ms).with_interval(promote_interval);
            PromotionConsumer::new(promo_scryer, lt, cfg).spawn();
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
