#!/usr/bin/env bash
# Sibling-process supervisor for the yah-mesofact-dev image (R455-F2).
#
# tini is PID 1 and reaps; this script starts almanac-serve + issue-tracker
# as siblings and exits when either dies so tini propagates the failure to the
# container runtime. Pond's outer policy is "if a half goes down, restart the
# whole container" — fine for dev tier; MesofactDevReconciler handles restart
# counting and failure promotion.
#
# Environment knobs:
#   ALMANAC_PORT             almanac-serve HTTP port (default 4323)
#   ALMANAC_DIR              feed TOML directory (default /etc/almanac)
#   ALMANAC_PROJECT_ROOT     artifact output dir (default /data)
#   ALMANAC_SERVICE_ID       mirror binding service id
#   ALMANAC_ENV              mirror environment (default pond)
#   ALMANAC_MIRROR_KEY       optional bearer secret
#   ISSUE_TRACKER_PORT       issue-tracker HTTP port (default 8731)
#   ISSUE_TRACKER_DB_PATH    SQLite db path (default /data/issues.db)
#   ISSUES_JSON_PATH         issues.json output path (default /data/issues.json)
#   YAH_LOG                  RUST_LOG passthrough (default info)

set -euo pipefail

export RUST_LOG="${YAH_LOG:-${RUST_LOG:-info}}"

echo "[mesofact-dev] starting almanac-serve on port ${ALMANAC_PORT:-4323}" >&2
/usr/local/bin/almanac-serve &
almanac_pid=$!

echo "[mesofact-dev] starting issue-tracker on port ${ISSUE_TRACKER_PORT:-8731}" >&2
/usr/local/bin/issue-tracker &
tracker_pid=$!

# Propagate SIGTERM/SIGINT from tini (docker stop) to both children.
trap 'kill -TERM "$almanac_pid" "$tracker_pid" 2>/dev/null || true' TERM INT

# Exit with the status of whichever sibling dies first; tini forwards the
# exit code to the container runtime as the container exit status.
wait -n "$almanac_pid" "$tracker_pid"
exit_status=$?
echo "[mesofact-dev] sibling exited with $exit_status; tearing down peer" >&2
kill -TERM "$almanac_pid" "$tracker_pid" 2>/dev/null || true
wait || true
exit "$exit_status"
