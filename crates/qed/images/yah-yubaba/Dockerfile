# syntax=docker/dockerfile:1
# yah-yubaba — pond-tier yubaba+kamaji container image (R408-T1).
#
# Ships the Yubaba-proper + Kamaji sibling pair from W154 inside a single
# OCI image. Pond runs this container on OrbStack; production deploys use the
# systemd sibling units (R406-T13). PID 1 is tini so zombie reaping and signal
# forwarding stay correct under both setups.
#
# Build model: COPY pre-built musl-static binaries from the build context.
# The release pipeline cross-compiles yubaba + kamaji in `yubaba-release`
# for both linux/amd64 and linux/arm64, then layers them into this image via
# `docker buildx build --platform linux/amd64,linux/arm64`. buildx sets
# TARGETARCH to `amd64` / `arm64` per platform; the COPY picks the matching
# binary out of the context.
#
# Build context layout expected (assembled by the GHA image-yah-yubaba job):
#   .
#   ├── Dockerfile
#   ├── pond-supervise.sh
#   ├── amd64/{yubaba, kamaji}
#   └── arm64/{yubaba, kamaji}
#
# Layout inside the image:
#   /usr/bin/tini                          # PID 1
#   /usr/local/bin/yubaba                  # Yubaba-proper (mesh/raft/admission)
#   /usr/local/bin/kamaji               # Kamaji (process supervisor + UDS)
#   /usr/local/bin/yubaba-pond-supervise
#                                          # bash launcher: starts both as siblings
#   /run/kamaji/                        # UDS rendezvous (kamaji.sock here)
#
# Required `docker run` flags (T2 documents the wiring; surfaced here as the
# acceptance contract this image expects):
#   --cgroupns=private        # isolate the cgroup namespace so Kamaji can
#                             # create child cgroups under /sys/fs/cgroup.
#   --cap-add=SYS_ADMIN       # cgroup ops, mount, unshare in Kamaji.
#   --pid=host (optional)     # only if cross-container supervision is needed;
#                             # the pond shape uses sibling containers via the
#                             # docker socket instead, so pond stays without
#                             # --pid=host.
#   -v /var/run/docker.sock:/var/run/docker.sock
#                             # T2 — container-backend workloads spawn as
#                             # sibling containers; Kamaji drives the host
#                             # docker socket.
#   -v <state>:/var/lib/yah-yubaba
#                             # raft state + identity (W105 — orthogonal to
#                             # the split; lands in Yubaba's intent layer).
#
# Catalog spec: crates/yah/qed/images/catalog.toml + W148.
# Arch doc: .yah/docs/working/W154-yubaba-dual-runtime.md

# Fetch stage: static docker CLI. The pond yubaba drives sibling MinIO/
# miniflare containers through the host docker socket (R408-T2 contract),
# and local-driver's LocalRuntime shells out to `docker` with DOCKER_HOST
# set — so the CLI binary must exist in the image. Static build from
# download.docker.com keeps the runtime image slim (no containerd/runc).
FROM debian:bookworm-slim AS docker-cli
ARG TARGETARCH
ARG DOCKER_CLI_VERSION=27.5.1
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && case "${TARGETARCH}" in \
         amd64) DOCKER_ARCH=x86_64 ;; \
         arm64) DOCKER_ARCH=aarch64 ;; \
         *) echo "unsupported TARGETARCH ${TARGETARCH}" >&2; exit 1 ;; \
       esac \
    && curl -fsSL "https://download.docker.com/linux/static/stable/${DOCKER_ARCH}/docker-${DOCKER_CLI_VERSION}.tgz" \
       | tar -xz --strip-components=1 -C /usr/local/bin docker/docker

FROM debian:bookworm-slim

ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

# tini = PID 1 (zombie reaping + signal forwarding). ca-certificates lets
# yubaba TLS to peers / control planes; iproute2 + procps make the container
# debuggable (`ss`, `ip`, `ps`) without being a kitchen sink.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tini \
        ca-certificates \
        iproute2 \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Docker CLI only (client, no daemon) — pond sibling-container contract.
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker

COPY ${TARGETARCH}/yubaba /usr/local/bin/yubaba
COPY ${TARGETARCH}/kamaji  /usr/local/bin/kamaji
COPY pond-supervise.sh        /usr/local/bin/yubaba-pond-supervise
RUN chmod +x /usr/local/bin/yubaba \
             /usr/local/bin/kamaji \
             /usr/local/bin/yubaba-pond-supervise

# UDS rendezvous dir. Kamaji creates the socket; yubaba dials in. Kept on
# a tmpfs-friendly path so a container restart leaves no stale socket file.
RUN mkdir -p /run/kamaji /var/lib/yah-yubaba
ENV KAMAJI_SOCK=/run/kamaji/kamaji.sock

# Smoke-test that the binaries link cleanly and respond to --version. This
# fails the build if either binary is missing a runtime dep. (Cross-arch
# binaries still execute under buildx's QEMU layer during `RUN`.)
RUN /usr/local/bin/yubaba --version \
    && /usr/local/bin/kamaji --version \
    && /usr/local/bin/docker --version

# Yubaba's HTTP surface (pond/state, raft RPC if exposed). Operators map this
# to a host port; the catalog/pond reconciler picks 0 and reads back.
EXPOSE 8800

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/yubaba-pond-supervise"]
