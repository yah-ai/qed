# syntax=docker/dockerfile:1
# yah-base — minimal Debian + shell-script tooling.
#
# Tools included:
#   bash   git   curl   ca-certificates
#
# Used as the lowest tier of the yah image catalog. Heavier images extend
# this directly (yah-python) or pull from upstream language bases instead
# (yah-rust uses rust:1-slim-bookworm).
#
# Built + signed + pushed to ghcr.io/yah-ai/yah-base by the release pipeline
# (R381-T7). Per-arch digests are injected into the yah binary at compile
# time via YAH_BASE_DIGEST.
#
# Catalog spec: crates/yah/qed/images/catalog.toml + W148.

FROM debian:bookworm-slim

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

# Smoke-test that the catalog-advertised tools are on PATH.
RUN bash --version \
    && git --version \
    && curl --version

WORKDIR /workspace

CMD ["bash"]
