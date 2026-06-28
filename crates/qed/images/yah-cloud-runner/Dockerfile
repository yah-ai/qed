# syntax=docker/dockerfile:1
# yah-cloud-runner — cloud-tier almanac runner container (R330-F11, W059).
#
# The single long-lived workload behind runner.yah.dev. One container, one
# HTTP listener: almanac-serve on ALMANAC_PORT (default 4323) exposing
#   POST /revalidate   — push-rebuild receiver (per-tenant feed bust)
#   GET  /healthz       — liveness
#   GET  /readyz        — readiness
#
# This is the cloud-tier sibling of yah-mesofact-dev (the pond-tier image),
# trimmed to almanac-serve alone: the issues sidecar is a *separate tenant*
# (R330-F13 / R523), not baked in here. Multi-tenant feed configs mount
# read-only at /etc/almanac; per-tenant artifacts + SQLite land under /data
# (tenant config shape settled by R330-F12).
#
# Build model: COPY a pre-built static musl binary from the build context
# (matches yah-yubaba / yah-mesofact-dev). almanac-serve is pure Rust
# (axum + reqwest/rustls — no C deps), so `cargo build --target
# x86_64-unknown-linux-musl -p almanac --bin almanac-serve` produces a
# self-contained binary that runs on any glibc/musl base.
#
# Build context layout (one arch per dir, mirrors yah-mesofact-dev):
#   .
#   ├── Dockerfile
#   └── amd64/almanac-serve        # (+ arm64/almanac-serve when multi-arch)
#
# Runtime env knobs (yubaba's MesofactRunnerSpec → WorkloadSpec.env overrides):
#   ALMANAC_PORT             default 4323 (CF tunnel routes runner.yah.dev here)
#   ALMANAC_DIR              feed TOML dir (bind-mount /etc/almanac read-only)
#   ALMANAC_PROJECT_ROOT     artifact output root (bind-mount /data)
#   ALMANAC_ENV              mirror environment label (default cloud)
#   ALMANAC_MIRROR_KEY       optional bearer secret for /revalidate
#
# Arch doc: .yah/docs/working/W059-almanac-release-feed.md

FROM debian:bookworm-slim

ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

# tini = PID 1 (zombie reaping + signal forwarding so SIGTERM drains cleanly).
# ca-certificates for outbound TLS (GitHub + R2 feed fetches via rustls).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tini \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY ${TARGETARCH}/almanac-serve /usr/local/bin/almanac-serve
RUN chmod +x /usr/local/bin/almanac-serve

# Feed TOMLs mount read-only at /etc/almanac; runtime writes land in /data.
RUN mkdir -p /data /etc/almanac

ENV ALMANAC_PORT=4323
ENV ALMANAC_DIR=/etc/almanac
ENV ALMANAC_PROJECT_ROOT=/data
ENV ALMANAC_ENV=cloud

# almanac receiver (POST /revalidate) + health probes
EXPOSE 4323

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/almanac-serve"]
