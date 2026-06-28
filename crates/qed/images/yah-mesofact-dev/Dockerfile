# syntax=docker/dockerfile:1
# yah-mesofact-dev — pond-tier almanac+issue-tracker backend container (R455-F2).
#
# Ships almanac-serve + issue-tracker as siblings under tini (W180 Phase D).
# One container, two HTTP listeners:
#   - almanac-serve on ALMANAC_PORT (default 4323): POST /revalidate + /healthz + /readyz
#   - issue-tracker on ISSUE_TRACKER_PORT (default 8731): POST /issues + /healthz + /readyz
#
# Build model: COPY pre-built musl-static binaries from the build context.
# The release pipeline cross-compiles both binaries in `mesofact-dev-release`
# for linux/amd64 and linux/arm64, then layers them via buildx.
#
# Build context layout (assembled by GHA image-yah-mesofact-dev job):
#   .
#   ├── Dockerfile
#   ├── mesofact-supervise.sh
#   ├── amd64/{almanac-serve, issue-tracker}
#   └── arm64/{almanac-serve, issue-tracker}
#
# Runtime env knobs (set defaults here; yubaba's MesofactDevSpec overrides at run time):
#   ALMANAC_PORT             default 4323
#   ALMANAC_DIR              feed TOML dir (bind-mount /etc/almanac read-only)
#   ALMANAC_PROJECT_ROOT     artifact output root (bind-mount /data)
#   ALMANAC_SERVICE_ID       mirror binding service id
#   ALMANAC_ENV              mirror environment label (default pond)
#   ALMANAC_MIRROR_KEY       optional bearer secret
#   ISSUE_TRACKER_PORT       default 8731
#   ISSUE_TRACKER_DB_PATH    SQLite path (default /data/issues.db)
#   ISSUES_JSON_PATH         issues.json output (default /data/issues.json)
#
# Required bind mounts (yubaba's MesofactDevSpec sets these):
#   -v <data_dir>:/data             issues SQLite + almanac feed artifacts
#   -v <almanac_config>:/etc/almanac:ro  feed TOML files
#
# Arch doc: .yah/docs/working/W180-pond-richer-topology.md

FROM debian:bookworm-slim

ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

# tini = PID 1 (zombie reaping + signal forwarding). ca-certificates for any
# outbound TLS (feed fetches). iproute2 + procps for operator debugging.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tini \
        ca-certificates \
        iproute2 \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY ${TARGETARCH}/almanac-serve  /usr/local/bin/almanac-serve
COPY ${TARGETARCH}/issue-tracker  /usr/local/bin/issue-tracker
COPY mesofact-supervise.sh        /usr/local/bin/mesofact-supervise
RUN chmod +x /usr/local/bin/almanac-serve \
             /usr/local/bin/issue-tracker \
             /usr/local/bin/mesofact-supervise

# Data root: issues SQLite + almanac artifacts. Feed TOMLs mount read-only
# at /etc/almanac; runtime writes land in /data.
RUN mkdir -p /data /etc/almanac

ENV ALMANAC_PORT=4323
ENV ALMANAC_DIR=/etc/almanac
ENV ALMANAC_PROJECT_ROOT=/data
ENV ALMANAC_ENV=pond
ENV ISSUE_TRACKER_PORT=8731
ENV ISSUE_TRACKER_DB_PATH=/data/issues.db
ENV ISSUES_JSON_PATH=/data/issues.json

# almanac receiver (POST /revalidate) + health probes
EXPOSE 4323
# issue-tracker (POST /issues) — Worker reaches it over the bridge network;
# not published to host by default (W180 §Bridge network).
EXPOSE 8731

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/mesofact-supervise"]
