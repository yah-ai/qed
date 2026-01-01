#!/usr/bin/env bash
# Sibling-process supervisor for the yah-warden image (R408-T1).
#
# tini is PID 1 and reaps; this script's job is to start constable + warden
# as siblings and exit when either dies so tini propagates the failure to the
# container runtime. Pond's outer policy is "if a half goes down, the whole
# container restarts" — fine for dev tier; cloud uses systemd siblings with
# independent restart.
#
# Environment knobs:
#   CONSTABLE_SOCK   UDS path; default /run/constable/constable.sock
#   YAH_WARDEN_ARGS  extra args appended to yah-warden invocation
#   YAH_LOG          RUST_LOG passthrough (default info)

set -euo pipefail

CONSTABLE_SOCK="${CONSTABLE_SOCK:-/run/constable/constable.sock}"
export RUST_LOG="${YAH_LOG:-${RUST_LOG:-info}}"

mkdir -p "$(dirname "$CONSTABLE_SOCK")"
# A stale socket from a prior container life would block constable's bind.
rm -f "$CONSTABLE_SOCK"

echo "[yah-warden] starting constable on $CONSTABLE_SOCK" >&2
/usr/local/bin/constable --socket "$CONSTABLE_SOCK" &
constable_pid=$!

# Wait for constable to bind the UDS before warden tries to dial in. Bounded
# so we fail loudly if constable can't start.
for _ in $(seq 1 50); do
    if [[ -S "$CONSTABLE_SOCK" ]]; then
        break
    fi
    sleep 0.1
done
if [[ ! -S "$CONSTABLE_SOCK" ]]; then
    echo "[yah-warden] constable did not bind $CONSTABLE_SOCK within 5s; aborting" >&2
    kill "$constable_pid" 2>/dev/null || true
    exit 1
fi

echo "[yah-warden] starting yah-warden serve (CONSTABLE_SOCK=$CONSTABLE_SOCK)" >&2
# Explicit `serve` subcommand. The binary also treats a missing subcommand as
# serve-with-defaults (R471-B7) but spelling it here means YAH_WARDEN_ARGS
# extras still attach to `serve` rather than ambiguously to the root parser.
# shellcheck disable=SC2086
CONSTABLE_SOCK="$CONSTABLE_SOCK" /usr/local/bin/yah-warden serve ${YAH_WARDEN_ARGS:-} &
warden_pid=$!

# Exit with the status of whichever sibling dies first; tini forwards SIGTERM
# from `docker stop` to this script and we propagate it to both children.
trap 'kill -TERM "$constable_pid" "$warden_pid" 2>/dev/null || true' TERM INT

wait -n "$constable_pid" "$warden_pid"
exit_status=$?
echo "[yah-warden] sibling exited with $exit_status; tearing down peer" >&2
kill -TERM "$constable_pid" "$warden_pid" 2>/dev/null || true
wait || true
exit "$exit_status"
