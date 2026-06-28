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
#   ├── amd64/{yah-yubaba, kamaji}
#   └── arm64/{yah-yubaba, kamaji}
#
# Layout inside the image:
#   /usr/bin/tini                          # PID 1
#   /usr/local/bin/yah-yubaba              # Yubaba-proper (mesh/raft/admission)
#   /usr/local/bin/kamaji               # Kamaji (process supervisor + UDS)
#   /usr/local/bin/yah-yubaba-pond-supervise
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

COPY ${TARGETARCH}/yah-yubaba /usr/local/bin/yah-yubaba
COPY ${TARGETARCH}/kamaji  /usr/local/bin/kamaji
COPY pond-supervise.sh        /usr/local/bin/yah-yubaba-pond-supervise
RUN chmod +x /usr/local/bin/yah-yubaba \
             /usr/local/bin/kamaji \
             /usr/local/bin/yah-yubaba-pond-supervise

# UDS rendezvous dir. Kamaji creates the socket; yubaba dials in. Kept on
# a tmpfs-friendly path so a container restart leaves no stale socket file.
RUN mkdir -p /run/kamaji /var/lib/yah-yubaba
ENV CONSTABLE_SOCK=/run/kamaji/kamaji.sock

# Smoke-test that the binaries link cleanly and respond to --version. This
# fails the build if either binary is missing a runtime dep. (Cross-arch
# binaries still execute under buildx's QEMU layer during `RUN`.)
RUN /usr/local/bin/yah-yubaba --version \
    && /usr/local/bin/kamaji --version

# Yubaba's HTTP surface (pond/state, raft RPC if exposed). Operators map this
# to a host port; the catalog/pond reconciler picks 0 and reads back.
EXPOSE 8800

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/yah-yubaba-pond-supervise"]
