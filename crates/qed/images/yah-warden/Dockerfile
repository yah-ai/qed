# syntax=docker/dockerfile:1
# yah-warden — pond-tier warden+constable container image (R408-T1).
#
# Ships the Warden-proper + Constable sibling pair from W154 inside a single
# OCI image. Pond runs this container on OrbStack; production deploys use the
# systemd sibling units (R406-T13). PID 1 is tini so zombie reaping and signal
# forwarding stay correct under both setups.
#
# Build model: COPY pre-built musl-static binaries from the build context.
# The release pipeline cross-compiles warden + constable in `warden-release`
# for both linux/amd64 and linux/arm64, then layers them into this image via
# `docker buildx build --platform linux/amd64,linux/arm64`. buildx sets
# TARGETARCH to `amd64` / `arm64` per platform; the COPY picks the matching
# binary out of the context.
#
# Build context layout expected (assembled by the GHA image-yah-warden job):
#   .
#   ├── Dockerfile
#   ├── pond-supervise.sh
#   ├── amd64/{yah-warden, constable}
#   └── arm64/{yah-warden, constable}
#
# Layout inside the image:
#   /usr/bin/tini                          # PID 1
#   /usr/local/bin/yah-warden              # Warden-proper (mesh/raft/admission)
#   /usr/local/bin/constable               # Constable (process supervisor + UDS)
#   /usr/local/bin/yah-warden-pond-supervise
#                                          # bash launcher: starts both as siblings
#   /run/constable/                        # UDS rendezvous (constable.sock here)
#
# Required `docker run` flags (T2 documents the wiring; surfaced here as the
# acceptance contract this image expects):
#   --cgroupns=private        # isolate the cgroup namespace so Constable can
#                             # create child cgroups under /sys/fs/cgroup.
#   --cap-add=SYS_ADMIN       # cgroup ops, mount, unshare in Constable.
#   --pid=host (optional)     # only if cross-container supervision is needed;
#                             # the pond shape uses sibling containers via the
#                             # docker socket instead, so pond stays without
#                             # --pid=host.
#   -v /var/run/docker.sock:/var/run/docker.sock
#                             # T2 — container-backend workloads spawn as
#                             # sibling containers; Constable drives the host
#                             # docker socket.
#   -v <state>:/var/lib/yah-warden
#                             # raft state + identity (W105 — orthogonal to
#                             # the split; lands in Warden's intent layer).
#
# Catalog spec: crates/yah/qed/images/catalog.toml + W148.
# Arch doc: .yah/docs/working/W154-warden-dual-runtime.md

FROM debian:bookworm-slim

ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

# tini = PID 1 (zombie reaping + signal forwarding). ca-certificates lets
# warden TLS to peers / control planes; iproute2 + procps make the container
# debuggable (`ss`, `ip`, `ps`) without being a kitchen sink.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tini \
        ca-certificates \
        iproute2 \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY ${TARGETARCH}/yah-warden /usr/local/bin/yah-warden
COPY ${TARGETARCH}/constable  /usr/local/bin/constable
COPY pond-supervise.sh        /usr/local/bin/yah-warden-pond-supervise
RUN chmod +x /usr/local/bin/yah-warden \
             /usr/local/bin/constable \
             /usr/local/bin/yah-warden-pond-supervise

# UDS rendezvous dir. Constable creates the socket; warden dials in. Kept on
# a tmpfs-friendly path so a container restart leaves no stale socket file.
RUN mkdir -p /run/constable /var/lib/yah-warden
ENV CONSTABLE_SOCK=/run/constable/constable.sock

# Smoke-test that the binaries link cleanly and respond to --version. This
# fails the build if either binary is missing a runtime dep. (Cross-arch
# binaries still execute under buildx's QEMU layer during `RUN`.)
RUN /usr/local/bin/yah-warden --version \
    && /usr/local/bin/constable --version

# Warden's HTTP surface (pond/state, raft RPC if exposed). Operators map this
# to a host port; the catalog/pond reconciler picks 0 and reads back.
EXPOSE 8800

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/yah-warden-pond-supervise"]
