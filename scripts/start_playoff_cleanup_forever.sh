#!/usr/bin/env bash
set -euo pipefail

ROOT="/mnt/c/users/dave/Downloads/nba-onoff-publish"
LOG_DIR="$ROOT/logs"
PID_FILE="$LOG_DIR/playoff_cleanup_forever.pid"
STDOUT_LOG="$LOG_DIR/playoff_cleanup_forever.stdout.log"
PYTHON="/tmp/nba-onoff-publish-linux/.venv/bin/python"

mkdir -p "$LOG_DIR"

cd "$ROOT"
nohup "$PYTHON" scripts/run_playoff_cleanup_forever.py \
  --block-batches 10 \
  --batch-size 10 \
  --regenerate-pages-every 5 \
  > "$STDOUT_LOG" 2>&1 < /dev/null &

echo $! > "$PID_FILE"
echo $!
