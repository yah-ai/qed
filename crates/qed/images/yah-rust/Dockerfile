# syntax=docker/dockerfile:1
# yah-rust — Rust toolchain image (heir of forge-minimal's Rust layer).
#
# Tools included:
#   bash   git   curl   cargo / rustc (stable from rust:1-slim-bookworm)
#   build-essential   pkg-config
#
# The catalog manifest lists `base = "rust:1-slim-bookworm"` rather than
# `extends = "yah-base"` because the upstream rust image is more cleanly
# maintained than a hand-rolled apt-install on yah-base — see W148. We
# still add the shell tooling (git, curl) that yah-base advertises so
# downstream consumers don't have to reinstall.
#
# Built + signed + pushed to ghcr.io/yah-ai/yah-rust by the release pipeline
# (R381-T7). Per-arch digests are injected into the yah binary at compile
# time via YAH_RUST_DIGEST.
#
# Catalog spec: crates/yah/qed/images/catalog.toml + W148.

FROM rust:1-slim-bookworm

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        git \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Smoke-test the catalog-advertised tools.
RUN bash --version \
    && git --version \
    && curl --version \
    && cargo --version \
    && rustc --version

WORKDIR /workspace

CMD ["bash"]
