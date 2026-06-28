//! @yah:ticket(R513-F3, "QED wait_for step kind / modifier (http health-gate, timeout)")
//! @yah:at(2026-06-20T00:00:00Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R513)
//! @arch:see(.yah/docs/working/W207-dashboard-e2e-in-qed.md)
//!
//! The `wait_for` health-gate primitive (W207 Gap #5). A [`StepKind::WaitFor`]
//! step polls a single network endpoint until it answers, then advances — the
//! gate that sits between a `background` sidecar (`yah-camp`, `vite preview`)
//! and the step that talks to it, so the consumer never races a not-yet-bound
//! port. Before this, the recipe hand-rolled
//! `bash -c "until curl …/health; do sleep 1; done"`.
//!
//! [`StepKind::WaitFor`]: crate::types::StepKind::WaitFor
//!
//! Two probe shapes, both dependency-free (no HTTP client / TLS stack is pulled
//! into qed — that matters for the musl-static build):
//!
//! - **http** — a plaintext HTTP/1.1 `GET` over a raw [`TcpStream`]. Healthy on
//!   a 2xx/3xx status, or an exact match to `expect_status` when set. TLS is out
//!   of scope for v1; an `https://` URL is rejected by the runner before we get
//!   here.
//! - **tcp** — a bare connect to `host:port`. Healthy the moment the port
//!   accepts; no bytes are exchanged.
//!
//! The poll loop itself (deadline + interval + live `StepOutput` progress)
//! lives in the runner, which holds the event sink; this module owns the pure
//! URL parsing and the single-attempt probes so they unit-test without one.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// A parsed plaintext-HTTP target. v1 supports `http://` only — `https://` is
/// rejected upstream (see [`crate::types::WaitForConfig::http_is_tls`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpTarget {
    pub host: String,
    pub port: u16,
    /// Request target, always beginning with `/` (defaults to `/` when the URL
    /// has no path).
    pub path: String,
}

/// Split a plaintext-HTTP URL into `host` / `port` / `path`. Deliberately
/// minimal — no query/fragment/userinfo handling beyond what a health-gate URL
/// needs. Rejects an `https://` scheme (TLS is a v1 non-goal) and an empty host.
pub fn parse_http_url(url: &str) -> Result<HttpTarget, String> {
    let url = url.trim();
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| {
            if url.starts_with("https://") {
                format!("`{url}`: https health-gates are not supported in v1 (R513-F3) — use an http:// URL or a `tcp` target")
            } else {
                format!("`{url}`: wait-for `http` target must be an http:// URL")
            }
        })?;

    // Split authority from path at the first '/'. The path keeps its leading
    // slash; an absent path becomes "/".
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    if authority.is_empty() {
        return Err(format!("`{url}`: wait-for `http` target has an empty host"));
    }

    // Authority is host[:port]. Default port 80 for http.
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
/// otherwise any 2xx/3xx (the conventional "up and serving" band — a redirect
/// from a health endpoint still means the server is listening).
pub fn http_status_ok(status: u16, expect: Option<u16>) -> bool {
    match expect {
        Some(want) => status == want,
        None => (200..400).contains(&status),
    }
}

/// One HTTP `GET` attempt against `target`. `Ok(status)` on a complete response
/// line; `Err(reason)` on connect / write / read failure or a malformed status
/// line. `attempt_timeout` bounds the whole connect+request+response.
pub async fn probe_http_once(
    target: &HttpTarget,
    attempt_timeout: Duration,
) -> Result<u16, String> {
    let fut = async {
        let mut stream = TcpStream::connect((target.host.as_str(), target.port))
            .await
            .map_err(|e| format!("connect {}:{}: {e}", target.host, target.port))?;
        let req = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nUser-Agent: qed-waitfor\r\nAccept: */*\r\n\r\n",
            target.path, target.host,
        );
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| format!("write request: {e}"))?;

        // We only need the status line. Read until we have the first CRLF (or
        // the connection closes / a small cap is hit), then parse it.
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

/// Parse `HTTP/1.1 200 OK\r\n…` → `200`. Errors when the bytes don't look like
/// an HTTP status line (e.g. a TLS handshake on a plaintext probe).
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

/// One TCP connect attempt against `addr` (`host:port`). `Ok(())` the moment the
/// port accepts; `Err(reason)` on connect failure or timeout. No bytes are
/// exchanged — a successful connect is the whole signal.
pub async fn probe_tcp_once(addr: &str, attempt_timeout: Duration) -> Result<(), String> {
    let addr = addr.trim();
    // Validate the shape up front so a `host` with no port fails clearly rather
    // than as an opaque resolver error.
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
        assert!(err.contains("https"), "got: {err}");
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

    #[tokio::test]
    async fn tcp_probe_fails_against_a_dead_port() {
        // Bind then drop to free a port nothing is listening on.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let res = probe_tcp_once(&addr, Duration::from_millis(200)).await;
        assert!(res.is_err(), "expected connect failure on a dead port");
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
}
