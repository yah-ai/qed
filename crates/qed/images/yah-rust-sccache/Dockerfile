# syntax=docker/dockerfile:1
# yah-rust-sccache — yah-rust + sccache.
#
# W219's *lower layer*: the read-only, content-addressed half of the camp
# scratch overlay. Everything here is generic to any Rust workspace — a
# toolchain and a compiler cache. Nothing in this image knows about the yah
# workspace, which is the point: W219 keeps the opinion in the base image and
# the contract general, so a camp's own dependency warming belongs one layer
# up in a per-camp entry (see .yah/qed/images/yah-rust-warm/), not here.
#
# Tools (inherited from yah-rust + added here):
#   bash   git   curl                       (yah-rust)
#   cargo / rustc / build-essential         (yah-rust)
#   pkg-config                              (yah-rust)
#   sccache                                 (this layer)
#
# WHY A RELEASE TARBALL AND NOT `cargo install sccache`: building sccache from
# source costs ~10 minutes of the very compile time this image exists to avoid,
# and it would be redone on every cache miss of this layer. The pinned tarball
# is a single ~9 MB download.
#
# The pin is exact and checksummed. Both SHA-256s below were verified twice:
# against mozilla/sccache's published `<asset>.tar.gz.sha256` sidecars, and by
# downloading both assets and recomputing locally (2026-08-04). `sha256sum -c`
# in the RUN below re-checks at build time, so a swapped asset fails the build
# rather than baking a surprise binary.
#
# Bumping the pin: set SCCACHE_VERSION, then take the two hashes from
#   https://github.com/mozilla/sccache/releases/download/v<ver>/sccache-v<ver>-<triple>-unknown-linux-musl.tar.gz.sha256
# for triples x86_64 and aarch64. Do not guess a hash — an unverifiable arch
# should be dropped from the case arm instead.
#
# The musl builds are used on a glibc Debian base on purpose: sccache is a
# standalone process, not linked into anything we compile, and the static musl
# artifact has no runtime dependency on the base image at all.
#
# Catalog spec: crates/yah/qed/images/catalog.toml + W148. Consumed by the
# camp-side sccache service (crates/yah/camp-service/src/sccache.rs), whose
# `find_binary()` is a plain `which("sccache")` — hence /usr/local/bin.

# Base defaults to :latest for standalone/local builds; the release pipeline
# overrides RUST_BASE to the just-built :smoke-<sha> tag so this builds against
# the exact rust image from the same run. Same convention as yah-rust-bun.
ARG RUST_BASE=ghcr.io/yah-ai/yah-rust:latest
FROM ${RUST_BASE}

ARG DEBIAN_FRONTEND=noninteractive

# Supplied by buildx for every build; declaring it here pulls it into scope.
ARG TARGETARCH

ARG SCCACHE_VERSION=0.17.0
ARG SCCACHE_SHA256_AMD64=67c4a96dd237c1f518f6b36083f270f9976d516f1e57fce891755ea782e50006
ARG SCCACHE_SHA256_ARM64=821a86343191aa1cbab74bd42f9e93c9a63bf85e4742945f40d3ae84193c1c77

RUN set -eu; \
    case "${TARGETARCH}" in \
      amd64) triple=x86_64-unknown-linux-musl;  sha="${SCCACHE_SHA256_AMD64}" ;; \
      arm64) triple=aarch64-unknown-linux-musl; sha="${SCCACHE_SHA256_ARM64}" ;; \
      *) echo "yah-rust-sccache: no pinned sccache checksum for TARGETARCH=${TARGETARCH} (have amd64, arm64)" >&2; exit 1 ;; \
    esac; \
    stem="sccache-v${SCCACHE_VERSION}-${triple}"; \
    curl -fsSL -o "/tmp/${stem}.tar.gz" \
        "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/${stem}.tar.gz"; \
    echo "${sha}  /tmp/${stem}.tar.gz" | sha256sum -c -; \
    tar -xzf "/tmp/${stem}.tar.gz" -C /tmp; \
    install -m 0755 "/tmp/${stem}/sccache" /usr/local/bin/sccache; \
    rm -rf "/tmp/${stem}.tar.gz" "/tmp/${stem}"

# 1777 rather than 0755: a container run as a non-root uid still has to write
# cache entries, and a read-only SCCACHE_DIR makes every rustc invocation fail
# once RUSTC_WRAPPER is set — turning the cache from an optimization into a
# hard dependency. Sticky bit keeps one uid from deleting another's entries.
RUN mkdir -p /var/cache/sccache && chmod 1777 /var/cache/sccache

ENV RUSTC_WRAPPER=/usr/local/bin/sccache
ENV SCCACHE_DIR=/var/cache/sccache

# Smoke-test the catalog-advertised tools.
RUN bash --version \
    && git --version \
    && curl --version \
    && cargo --version \
    && rustc --version \
    && sccache --version

WORKDIR /workspace

CMD ["bash"]
