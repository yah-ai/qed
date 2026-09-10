//! Protocol-pluggable "poll until healthy" primitive, with backoff.
//!
//! Split out of `yah-qed` (where it backed `StepKind::WaitFor`, R513-F3 /
//! W207 Gap #5) so it can be depended on without pulling in qed's scheduler
//! stack (`task-runs`, `velveteen`, …) — a caller that only wants "wait for
//! this thing to materialize" (a network endpoint, an npm publish landing, a
//! DB row appearing) should not need a CI-pipeline engine to get it.
//!
//! Three dependency-free probes (no HTTP client / TLS stack — that matters
//! for qed's musl-static build):
//!
//! - **http** — plaintext HTTP/1.1 `GET` over a raw [`tokio::net::TcpStream`].
//!   Healthy on 2xx/3xx, or an exact match to `expect_status`. No TLS in v1;
//!   an `https://` target should use `Probe::Shell` (e.g. `curl -fsS ...`)
//!   instead of pulling a TLS stack into every caller.
//! - **tcp** — bare connect to `host:port`. Healthy the moment it accepts.
//! - **shell** — `sh -c <command>`. Healthy on exit 0. The escape hatch for
//!   anything the other two can't express (HTTPS, a CLI query, a DB check).
//!
//! [`poll_until`] owns the deadline/backoff scheduling and attempt callback;
//! callers only supply a [`Probe`] and a [`BackoffConfig`].

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Instant};

/// A parsed plaintext-HTTP target. v1 supports `http://` only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpTarget {
    pub host: String,
    pub port: u16,
    /// Request target, always beginning with `/` (defaults to `/`).
    pub path: String,
}

/// Split a plaintext-HTTP URL into `host` / `port` / `path`. Deliberately
/// minimal — no query/fragment/userinfo handling beyond what a health-gate
/// URL needs. Rejects an `https://` scheme and an empty host.
pub fn parse_http_url(url: &str) -> Result<HttpTarget, String> {
    let url = url.trim();
    let rest = url.strip_prefix("http://").ok_or_else(|| {
        if url.starts_with("https://") {
            format!(
                "`{url}`: https targets are not supported directly (R513-F3) — use a \
                 `shell` probe (e.g. `curl -fsS {url}`) or a `tcp` target"
            )
        } else {
            format!("`{url}`: wait-for `http` target must be an http:// URL")
        }
    })?;

    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    if authority.is_empty() {
        return Err(format!("`{url}`: wait-for `http` target has an empty host"));
    }

    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => {
            let port: u16 = p
                .parse()
                .map_err(|_| format!("`{url}`: invalid port `{p}`"))?;
            (h.to_string(), port)
        }
        None => (authority.to_string(), 80),
    };
    if host.is_empty() {
        return Err(format!("`{url}`: wait-for `http` target has an empty host"));
    }

    Ok(HttpTarget {
        host,
        port,
        path: if path.is_empty() { "/".to_string() } else { path },
    })
}

/// Is `status` acceptable? With `expect = Some(n)` only an exact `n` passes;
/// otherwise any 2xx/3xx.
pub fn http_status_ok(status: u16, expect: Option<u16>) -> bool {
    match expect {
        Some(want) => status == want,
        None => (200..400).contains(&status),
    }
}

/// One HTTP `GET` attempt against `target`. `Ok(status)` on a complete
/// response line; `Err(reason)` on connect/write/read failure or a malformed
/// status line. `attempt_timeout` bounds the whole connect+request+response.
pub async fn probe_http_once(target: &HttpTarget, attempt_timeout: Duration) -> Result<u16, String> {
    let fut = async {
        let mut stream = TcpStream::connect((target.host.as_str(), target.port))
            .await
            .map_err(|e| format!("connect {}:{}: {e}", target.host, target.port))?;
        let req = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nUser-Agent: pleasehold\r\nAccept: */*\r\n\r\n",
            target.path, target.host,
        );
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| format!("write request: {e}"))?;

        let mut buf = Vec::with_capacity(256);
        let mut chunk = [0u8; 256];
        loop {
            let n = stream
                .read(&mut chunk)
                .await
                .map_err(|e| format!("read response: {e}"))?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(2).any(|w| w == b"\r\n") || buf.len() > 8192 {
                break;
            }
        }
        parse_status_line(&buf)
    };

    timeout(attempt_timeout, fut)
        .await
        .map_err(|_| format!("no response within {}ms", attempt_timeout.as_millis()))?
}

fn parse_status_line(buf: &[u8]) -> Result<u16, String> {
    let head = String::from_utf8_lossy(buf);
    let first = head.lines().next().unwrap_or("").trim();
    let mut parts = first.split_whitespace();
    let version = parts.next().unwrap_or("");
    if !version.starts_with("HTTP/") {
        return Err(format!("not an HTTP response (got `{first}`)"));
    }
    let code = parts
        .next()
        .ok_or_else(|| format!("malformed status line `{first}`"))?;
    code.parse::<u16>()
        .map_err(|_| format!("malformed status code in `{first}`"))
}

/// One TCP connect attempt against `addr` (`host:port`). `Ok(())` the moment
/// the port accepts; `Err(reason)` on connect failure or timeout. No bytes
/// are exchanged — a successful connect is the whole signal.
pub async fn probe_tcp_once(addr: &str, attempt_timeout: Duration) -> Result<(), String> {
    let addr = addr.trim();
    if addr.rsplit_once(':').is_none_or(|(_, p)| p.is_empty()) {
        return Err(format!("`{addr}`: wait-for `tcp` target must be `host:port`"));
    }
    let fut = TcpStream::connect(addr);
    match timeout(attempt_timeout, fut).await {
        Ok(Ok(_stream)) => Ok(()),
        Ok(Err(e)) => Err(format!("connect {addr}: {e}")),
        Err(_) => Err(format!(
            "connect {addr}: no response within {}ms",
            attempt_timeout.as_millis()
        )),
    }
}

/// One shell-condition attempt: `sh -c <command>`, healthy on exit 0.
/// `Err` carries the trimmed tail of combined stdout+stderr so a caller can
/// show *why* the condition still doesn't hold.
pub async fn probe_shell_once(command: &str, attempt_timeout: Duration) -> Result<(), String> {
    let fut = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .output();
    let out = timeout(attempt_timeout, fut)
        .await
        .map_err(|_| format!("no result within {}ms", attempt_timeout.as_millis()))?
        .map_err(|e| format!("could not run `{command}`: {e}"))?;
    if out.status.success() {
        return Ok(());
    }
    let mut tail = String::new();
    for stream in [&out.stdout, &out.stderr] {
        let s = String::from_utf8_lossy(stream);
        let s = s.trim();
        if !s.is_empty() {
            if !tail.is_empty() {
                tail.push('\n');
            }
            tail.push_str(s);
        }
    }
    Err(if tail.is_empty() {
        format!("exit {}", out.status)
    } else {
        format!("exit {}: {tail}", out.status)
    })
}

/// A single thing to poll. Owns enough state to both label itself (for
/// progress/failure messages) and run one attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Probe {
    Http {
        target: HttpTarget,
        raw_url: String,
        expect_status: Option<u16>,
    },
    Tcp(String),
    Shell(String),
}

impl Probe {
    /// Build an HTTP probe from a raw URL, up front — so a malformed URL
    /// fails immediately instead of burning the whole timeout budget
    /// retrying an unparseable target.
    pub fn http(url: &str, expect_status: Option<u16>) -> Result<Self, String> {
        let target = parse_http_url(url)?;
        Ok(Probe::Http {
            target,
            raw_url: url.to_string(),
            expect_status,
        })
    }

    pub fn tcp(addr: &str) -> Self {
        Probe::Tcp(addr.to_string())
    }

    pub fn shell(command: &str) -> Self {
        Probe::Shell(command.to_string())
    }

    /// Human-readable label for progress/failure messages.
    pub fn label(&self) -> String {
        match self {
            Probe::Http { raw_url, .. } => raw_url.clone(),
            Probe::Tcp(addr) => addr.clone(),
            Probe::Shell(command) => format!("shell: {command}"),
        }
    }

    /// One attempt. `attempt_timeout` bounds it regardless of probe kind.
    pub async fn probe_once(&self, attempt_timeout: Duration) -> Result<(), String> {
        match self {
            Probe::Http {
                target,
                expect_status,
                ..
            } => match probe_http_once(target, attempt_timeout).await {
                Ok(status) if http_status_ok(status, *expect_status) => Ok(()),
                Ok(status) => Err(match expect_status {
                    Some(want) => format!("HTTP {status} (want {want})"),
                    None => format!("HTTP {status} (want 2xx/3xx)"),
                }),
                Err(e) => Err(e),
            },
            Probe::Tcp(addr) => probe_tcp_once(addr, attempt_timeout).await,
            Probe::Shell(command) => probe_shell_once(command, attempt_timeout).await,
        }
    }
}

/// Poll timing: how long to keep trying, and how the delay between attempts
/// grows. `multiplier = 1.0` (the default) is a plain fixed interval — the
/// behavior every existing `wait-for` gate had before backoff existed.
#[derive(Debug, Clone, PartialEq)]
pub struct BackoffConfig {
    /// Total budget for the probe to become healthy.
    pub timeout: Duration,
    /// Delay before the second attempt (the first attempt is immediate).
    pub initial_interval: Duration,
    /// Delay never grows past this, however large `multiplier` is.
    pub max_interval: Duration,
    /// Each failed attempt's delay is multiplied by this for the next one.
    /// `1.0` = fixed interval; `> 1.0` = exponential backoff.
    pub multiplier: f64,
}

impl BackoffConfig {
    /// A fixed-interval policy — no backoff growth. Matches the pre-backoff
    /// `WaitForConfig` default (500ms / 30s).
    pub fn fixed(interval: Duration, timeout: Duration) -> Self {
        BackoffConfig {
            timeout,
            initial_interval: interval,
            max_interval: interval,
            multiplier: 1.0,
        }
    }
}

/// One attempt's outcome, handed to the caller's progress callback so it can
/// stream "waiting… (attempt N)" without owning any of the timing logic.
pub struct Attempt<'a> {
    pub number: u32,
    pub result: &'a Result<(), String>,
}

#[derive(Debug)]
pub struct PollSuccess {
    pub attempts: u32,
    pub elapsed: Duration,
}

#[derive(Debug)]
pub struct PollFailure {
    pub attempts: u32,
    pub elapsed: Duration,
    pub last_error: String,
}

/// Poll `probe` under `cfg` until it's healthy or the timeout elapses.
///
/// `on_attempt` fires after every attempt (success or failure) so a caller
/// can emit its own progress events; it does not affect timing. Cancellation
/// is structural — dropping this future (e.g. the caller's task) mid-`sleep`
/// or mid-probe stops the loop with no lingering poller.
pub async fn poll_until(
    probe: &Probe,
    cfg: &BackoffConfig,
    mut on_attempt: impl FnMut(Attempt<'_>),
) -> Result<PollSuccess, PollFailure> {
    // Never let a single probe attempt outlast the whole budget, and cap it
    // at 5s so a hung probe can't stall a fast-timeout gate either.
    let attempt_timeout = cfg.timeout.min(Duration::from_secs(5));
    let deadline = Instant::now() + cfg.timeout;
    let started = Instant::now();
    let mut interval = cfg.initial_interval;
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;
        let result = probe.probe_once(attempt_timeout).await;
        on_attempt(Attempt {
            number: attempt,
            result: &result,
        });
        match result {
            Ok(()) => {
                return Ok(PollSuccess {
                    attempts: attempt,
                    elapsed: started.elapsed(),
                })
            }
            Err(reason) => {
                // Stop if the next sleep would push us past the deadline —
                // no point sleeping only to give up.
                if Instant::now() + interval >= deadline {
                    return Err(PollFailure {
                        attempts: attempt,
                        elapsed: started.elapsed(),
                        last_error: reason,
                    });
                }
                tokio::time::sleep(interval).await;
                let grown = interval.as_secs_f64() * cfg.multiplier;
                interval = Duration::from_secs_f64(grown).min(cfg.max_interval);
            }
        }
    }
}

/// Type alias for a boxed async probe closure, kept available for callers
/// that want to poll something `Probe` can't express (e.g. a DB query)
/// without duplicating the backoff loop. Not used by [`poll_until`] itself —
/// see [`poll_until_with`].
pub type BoxedProbeFn<'a> = Box<dyn FnMut() -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> + Send + 'a>;

/// Same loop as [`poll_until`], but over an arbitrary probe closure instead
/// of the built-in [`Probe`] enum — the escape hatch for a caller whose
/// condition is neither http/tcp/shell (e.g. an in-process DB query) and who
/// doesn't want to shell out just to reuse the timing logic.
pub async fn poll_until_with(
    mut probe: BoxedProbeFn<'_>,
    cfg: &BackoffConfig,
    mut on_attempt: impl FnMut(Attempt<'_>),
) -> Result<PollSuccess, PollFailure> {
    let deadline = Instant::now() + cfg.timeout;
    let started = Instant::now();
    let mut interval = cfg.initial_interval;
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;
        let result = probe().await;
        on_attempt(Attempt {
            number: attempt,
            result: &result,
        });
        match result {
            Ok(()) => {
                return Ok(PollSuccess {
                    attempts: attempt,
                    elapsed: started.elapsed(),
                })
            }
            Err(reason) => {
                if Instant::now() + interval >= deadline {
                    return Err(PollFailure {
                        attempts: attempt,
                        elapsed: started.elapsed(),
                        last_error: reason,
                    });
                }
                tokio::time::sleep(interval).await;
                let grown = interval.as_secs_f64() * cfg.multiplier;
                interval = Duration::from_secs_f64(grown).min(cfg.max_interval);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_http_url_with_port_and_path() {
        let t = parse_http_url("http://localhost:3000/health").unwrap();
        assert_eq!(t.host, "localhost");
        assert_eq!(t.port, 3000);
        assert_eq!(t.path, "/health");
    }

    #[test]
    fn parses_http_url_defaults_port_and_path() {
        let t = parse_http_url("http://example.com").unwrap();
        assert_eq!(t.host, "example.com");
        assert_eq!(t.port, 80);
        assert_eq!(t.path, "/");
    }

    #[test]
    fn rejects_https_with_pointed_message() {
        let err = parse_http_url("https://localhost/health").unwrap_err();
        assert!(err.contains("shell"), "got: {err}");
        assert!(err.contains("R513-F3"), "got: {err}");
    }

    #[test]
    fn rejects_non_http_scheme() {
        assert!(parse_http_url("localhost:3000/health").is_err());
        assert!(parse_http_url("http:///health").is_err());
    }

    #[test]
    fn rejects_bad_port() {
        assert!(parse_http_url("http://localhost:notaport/").is_err());
    }

    #[test]
    fn status_ok_band_and_exact() {
        assert!(http_status_ok(200, None));
        assert!(http_status_ok(204, None));
        assert!(http_status_ok(302, None));
        assert!(!http_status_ok(404, None));
        assert!(!http_status_ok(500, None));
        // exact
        assert!(http_status_ok(204, Some(204)));
        assert!(!http_status_ok(200, Some(204)));
    }

    #[test]
    fn parses_status_line() {
        assert_eq!(parse_status_line(b"HTTP/1.1 200 OK\r\n").unwrap(), 200);
        assert_eq!(parse_status_line(b"HTTP/1.0 503 Service Unavailable\r\n").unwrap(), 503);
        assert!(parse_status_line(b"garbage\r\n").is_err());
        assert!(parse_status_line(b"\x16\x03\x01garbage").is_err());
    }

    #[tokio::test]
    async fn tcp_probe_succeeds_against_a_live_listener() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let res = probe_tcp_once(&addr, Duration::from_secs(1)).await;
        assert!(res.is_ok(), "got: {res:?}");
    }

    /// R823-T3: bind-then-drop does not *reserve* the port it frees — on a busy
    /// machine the kernel can hand that ephemeral number to an unrelated
    /// process between the drop and the probe, and then a correct
    /// `probe_tcp_once` connects and this test fails. Observed once during a
    /// full `cargo test -p yah-qed --lib` in this camp (which runs many
    /// concurrent sessions), passing on the very next run — the shape of a
    /// flake that costs someone a bisect.
    ///
    /// Retrying with a *fresh* port makes the outcome deterministic without
    /// pretending the race is not there: losing it twice in a row on
    /// independently-allocated ports is not something a passing run should be
    /// held to.
    #[tokio::test]
    async fn tcp_probe_fails_against_a_dead_port() {
        for attempt in 1..=5 {
            // Bind then drop to free a port nothing is listening on.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            drop(listener);
            if probe_tcp_once(&addr, Duration::from_millis(200)).await.is_err() {
                return;
            }
            assert!(
                attempt < 5,
                "five freed ephemeral ports in a row were re-taken before the probe — \
                 that is no longer a race, look at probe_tcp_once"
            );
        }
    }

    #[tokio::test]
    async fn tcp_probe_rejects_missing_port() {
        assert!(probe_tcp_once("localhost", Duration::from_millis(100))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn http_probe_reads_status_from_a_canned_server() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut scratch = [0u8; 1024];
                let _ = sock.read(&mut scratch).await;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .await;
            }
        });
        let target = HttpTarget {
            host: addr.ip().to_string(),
            port: addr.port(),
            path: "/health".to_string(),
        };
        let status = probe_http_once(&target, Duration::from_secs(2)).await.unwrap();
        assert_eq!(status, 200);
    }

    #[tokio::test]
    async fn http_probe_surfaces_a_503() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut scratch = [0u8; 1024];
                let _ = sock.read(&mut scratch).await;
                let _ = sock
                    .write_all(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")
                    .await;
            }
        });
        let target = HttpTarget {
            host: addr.ip().to_string(),
            port: addr.port(),
            path: "/".to_string(),
        };
        let status = probe_http_once(&target, Duration::from_secs(2)).await.unwrap();
        assert_eq!(status, 503);
        assert!(!http_status_ok(status, None));
    }

    #[tokio::test]
    async fn shell_probe_passes_on_exit_zero() {
        assert!(probe_shell_once("true", Duration::from_secs(2)).await.is_ok());
    }

    #[tokio::test]
    async fn shell_probe_fails_on_nonzero_exit_with_output() {
        let err = probe_shell_once("echo nope >&2; exit 1", Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(err.contains("nope"), "got: {err}");
    }

    #[tokio::test]
    async fn poll_until_succeeds_immediately_when_probe_is_healthy() {
        let probe = Probe::shell("true");
        let cfg = BackoffConfig::fixed(Duration::from_millis(10), Duration::from_secs(1));
        let mut attempts = 0;
        let outcome = poll_until(&probe, &cfg, |a| attempts = a.number).await;
        assert!(outcome.is_ok());
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn poll_until_times_out_when_probe_never_passes() {
        let probe = Probe::shell("exit 1");
        let cfg = BackoffConfig::fixed(Duration::from_millis(20), Duration::from_millis(80));
        let err = poll_until(&probe, &cfg, |_| {}).await.unwrap_err();
        assert!(err.attempts >= 2, "expected multiple attempts, got {}", err.attempts);
    }

    #[tokio::test]
    async fn backoff_grows_interval_and_caps_at_max() {
        // 3 failures then success: interval should have grown 10ms -> 20ms ->
        // 40ms (capped at 45ms) rather than staying fixed at 10ms — assert
        // indirectly via elapsed time being closer to the grown schedule than
        // to 3 * 10ms.
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let calls_for_probe = calls.clone();
        let probe_fn: BoxedProbeFn = Box::new(move || {
            let calls = calls_for_probe.clone();
            Box::pin(async move {
                let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n >= 3 {
                    Ok(())
                } else {
                    Err("not yet".to_string())
                }
            })
        });
        let cfg = BackoffConfig {
            timeout: Duration::from_secs(2),
            initial_interval: Duration::from_millis(10),
            max_interval: Duration::from_millis(45),
            multiplier: 2.0,
        };
        let started = Instant::now();
        let outcome = poll_until_with(probe_fn, &cfg, |_| {}).await;
        assert!(outcome.is_ok());
        // 10 + 20 + 40(capped to 45) = ~70ms minimum sleep before success.
        assert!(
            started.elapsed() >= Duration::from_millis(65),
            "elapsed {:?} looks like it didn't back off",
            started.elapsed()
        );
    }
}
