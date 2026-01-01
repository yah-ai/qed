# syntax=docker/dockerfile:1
# yah-rust-bun — direct heir of forge-minimal.
#
# Default container image for `ForgeCommand::Subprocess { image: None }` —
# what `task::default_image::default_forge_image()` returns. The yah
# workspace's own pipelines (cargo + bun frontend) all run inside this image.
#
# Tools (inherited from yah-rust + added here):
#   bash   git   curl                       (yah-rust)
#   cargo / rustc / build-essential         (yah-rust)
#   bun                                     (this layer)
#   node (LTS)                              (this layer)
#
# Built + signed + pushed to ghcr.io/yah-ai/yah-rust-bun by the release
# pipeline (R381-T7). Per-arch digests are injected into the yah binary at
# compile time via YAH_RUST_BUN_DIGEST.
#
# Supply-chain: Sigstore keyless OIDC (cosign). Warden's R090-F4
# environment validator verifies the signature before pulling. Inherits
# the same trust chain forge-minimal had.
#
# Catalog spec: crates/yah/qed/images/catalog.toml + W148.

FROM ghcr.io/yah-ai/yah-rust:latest

ARG DEBIAN_FRONTEND=noninteractive

# Node LTS from the upstream NodeSource feed — Debian's `nodejs` lags far
# behind the LTS release pace and trips on modern frontend tooling.
RUN curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - \
    && apt-get install -y --no-install-recommends nodejs unzip \
    && rm -rf /var/lib/apt/lists/*

# Bun installs into /root/.bun by default; symlink onto PATH so it's
# usable from any working directory inside the container.
RUN curl -fsSL https://bun.sh/install | bash \
    && ln -s /root/.bun/bin/bun /usr/local/bin/bun

# Smoke-test the catalog-advertised tools.
RUN bash --version \
    && git --version \
    && curl --version \
    && cargo --version \
    && node --version \
    && bun --version

WORKDIR /workspace

CMD ["bash"]
