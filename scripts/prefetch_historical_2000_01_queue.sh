#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/mnt/c/users/dave/Downloads/nba-onoff-publish"
PYTHON_BIN="/mnt/c/users/dave/Downloads/nba-3pt-adjust/nba-3pt-adjust/.venv/bin/python"
LOG_DIR="$REPO_DIR/logs"
WORKERS="${WORKERS:-4}"

mkdir -p "$LOG_DIR"
cd "$REPO_DIR"

run_chunk() {
  local start="$1"
  local end="$2"
  echo "== $(date '+%Y-%m-%d %H:%M:%S') chunk $start -> $end =="
  PYTHONPATH="$REPO_DIR" \
    "$PYTHON_BIN" prefetch_historical_stats_cache.py \
      --start "$start" \
      --end "$end" \
      --starter-overrides-path data/stints_historical.csv \
      --workers "$WORKERS"
}

run_chunk "2000-11-16" "2000-11-30"
run_chunk "2000-12-01" "2000-12-15"
run_chunk "2000-12-16" "2000-12-31"
run_chunk "2001-01-01" "2001-01-15"
run_chunk "2001-01-16" "2001-01-31"
run_chunk "2001-02-01" "2001-02-15"
run_chunk "2001-02-16" "2001-02-28"
run_chunk "2001-03-01" "2001-03-15"
run_chunk "2001-03-16" "2001-03-31"
run_chunk "2001-04-01" "2001-04-15"
run_chunk "2001-04-16" "2001-04-30"
run_chunk "2001-05-01" "2001-05-15"
run_chunk "2001-05-16" "2001-05-31"
run_chunk "2001-06-01" "2001-06-30"

echo "== $(date '+%Y-%m-%d %H:%M:%S') completed historical 2000-01 prefetch queue =="
