# syntax=docker/dockerfile:1
# yah-miniflare — pond-tier miniflare container image (R455-F1).
#
# Replaces the host-side `bun miniflare-sim.mjs` process with a container that
# joins the per-cell `yah-pond-<svc>-<env>` bridge network so the Worker can
# DNS-resolve sibling containers (MinIO, mesofact-dev). Miniflare's port still
# publishes to the host — operators hit `http://localhost:4322` exactly as they
# do today.
#
# Build model: bun base + tini PID 1 + a baked miniflare install. The Worker
# bundle (`worker.js`) is workload-specific and gets bind-mounted at runtime
# instead of baked, so a single image serves every (service, env) pond.
#
# Build context: the workspace root (matches yah-warden's pattern). The
# Dockerfile COPYs the shared `miniflare-sim.mjs` from `crates/yah/cloud/worker/`
# so the container shim and the embedded host-side shim stay byte-identical.
# The bun install at image-build time pulls miniflare + workerd from npm.
#
# Layout inside the image:
#   /usr/bin/tini                        # PID 1
#   /app/miniflare-sim.mjs               # ESM shim (R327-F2; reads MF_* env)
#   /app/package.json                    # carries the miniflare devDep
#   /app/node_modules/                   # populated by `bun install` at build
#   /work/worker.js                      # workload's Worker bundle (bind mount)
#
# Required `docker run` flags (the warden + cloud reconciler wire this up):
#   --network yah-pond-<svc>-<env>        # per-cell bridge (R455-F1)
#   --network-alias miniflare             # sibling DNS
#   -p <host>:4322                        # operator-visible port
#   -v <state_dir>/worker.js:/work/worker.js
#                                        # workload's bundle, bind-mounted RO
#   -e MF_PORT=4322
#   -e MF_SCRIPT=/work/worker.js
#   -e ASSET_ORIGIN=http://minio:9000/<bucket>
#   -e WORKER_MODE=static|spa|ssr
#   -e SSR_ORIGIN=http://ssr:3000        # ssr mode only
#   -e SSR_PREFIXES='["/foo","/bar"]'
#
# Catalog spec: crates/yah/qed/images/catalog.toml + W148.
# Arch doc: .yah/docs/working/W180-pond-richer-topology.md

FROM oven/bun:1-debian

ARG DEBIAN_FRONTEND=noninteractive

# tini = PID 1 (zombie reaping + signal forwarding). ca-certificates lets the
# miniflare shim resolve HTTPS during workerd's first-run download (if it ever
# needs to fall back to one). curl is included so the image-internal health
# probe / debug `docker exec curl http://localhost:4322` works without piling
# on a separate debug image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tini \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Minimal package.json keyed only to miniflare so `bun install` produces a
# deterministic node_modules layer (image-cache key = the version below).
# Bumping the miniflare version is a one-line change here + rebuild.
RUN cat > package.json <<'EOF'
{
  "name": "yah-miniflare-container",
  "private": true,
  "type": "module",
  "dependencies": {
    "miniflare": "^3.20240610.1"
  }
}
EOF

RUN bun install --no-save \
    && rm -rf /root/.bun/install/cache

COPY crates/yah/cloud/worker/miniflare-sim.mjs /app/miniflare-sim.mjs

# Worker bundle gets bind-mounted at runtime. The default value of MF_SCRIPT
# points at this path; callers can override via `-e MF_SCRIPT=…` if they bind
# the bundle elsewhere.
RUN mkdir -p /work
ENV MF_SCRIPT=/work/worker.js
ENV MF_PORT=4322

EXPOSE 4322

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bun", "/app/miniflare-sim.mjs"]
