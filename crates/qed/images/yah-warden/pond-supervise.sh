#!/usr/bin/env bash
# Sibling-process supervisor for the yah-yubaba image (R408-T1).
#
# tini is PID 1 and reaps; this script's job is to start kamaji + yubaba
# as siblings and exit when either dies so tini propagates the failure to the
# container runtime. Pond's outer policy is "if a half goes down, the whole
# container restarts" — fine for dev tier; cloud uses systemd siblings with
# independent restart.
#
# Environment knobs:
#   CONSTABLE_SOCK   UDS path; default /run/kamaji/kamaji.sock
#   YAH_WARDEN_ARGS  flags appended AFTER the `serve` subcommand (this script
#                    owns the `serve` token — the value must NOT repeat it, or
#                    clap sees a doubled subcommand and crash-loops; see R471)
#   YAH_LOG          RUST_LOG passthrough (default info)

set -euo pipefail

CONSTABLE_SOCK="${CONSTABLE_SOCK:-/run/kamaji/kamaji.sock}"
export RUST_LOG="${YAH_LOG:-${RUST_LOG:-info}}"

mkdir -p "$(dirname "$CONSTABLE_SOCK")"
# A stale socket from a prior container life would block kamaji's bind.
rm -f "$CONSTABLE_SOCK"

echo "[yah-yubaba] starting kamaji on $CONSTABLE_SOCK" >&2
/usr/local/bin/kamaji --socket "$CONSTABLE_SOCK" &
constable_pid=$!

# Wait for kamaji to bind the UDS before yubaba tries to dial in. Bounded
# so we fail loudly if kamaji can't start.
for _ in $(seq 1 50); do
    if [[ -S "$CONSTABLE_SOCK" ]]; then
        break
    fi
    sleep 0.1
done
if [[ ! -S "$CONSTABLE_SOCK" ]]; then
    echo "[yah-yubaba] kamaji did not bind $CONSTABLE_SOCK within 5s; aborting" >&2
    kill "$constable_pid" 2>/dev/null || true
    exit 1
fi

echo "[yah-yubaba] starting yah-yubaba serve (CONSTABLE_SOCK=$CONSTABLE_SOCK)" >&2
# This script is the sole owner of the `serve` subcommand token. The binary
# also treats a missing subcommand as serve-with-defaults (R471-B7), but
# spelling it here means YAH_WARDEN_ARGS flags attach to `serve` unambiguously.
# YAH_WARDEN_ARGS carries flags ONLY — if it also began with `serve` the line
# below would expand to `yah-yubaba serve serve …` and crash-loop (R471 redux);
# DEFAULT_WARDEN_ARGS in local-driver/src/pond_warden.rs enforces flags-only.
# shellcheck disable=SC2086
CONSTABLE_SOCK="$CONSTABLE_SOCK" /usr/local/bin/yah-yubaba serve ${YAH_WARDEN_ARGS:-} &
warden_pid=$!

# Exit with the status of whichever sibling dies first; tini forwards SIGTERM
# from `docker stop` to this script and we propagate it to both children.
trap 'kill -TERM "$constable_pid" "$warden_pid" 2>/dev/null || true' TERM INT

wait -n "$constable_pid" "$warden_pid"
exit_status=$?
echo "[yah-yubaba] sibling exited with $exit_status; tearing down peer" >&2
kill -TERM "$constable_pid" "$warden_pid" 2>/dev/null || true
wait || true
exit "$exit_status"
