#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "Usage: $0 <plain|vwd|dual> <tag> <pbp_dir> <checkpoint_file>"
  exit 1
fi

MODE="$1"
TAG="$2"
PBP_DIR="$3"
CHECKPOINT_FILE="$4"

ROOT="/mnt/c/users/dave/Downloads/nba-onoff-publish"
BATCH_RUNNER="$ROOT/scripts/run_historical_batch.sh"

cd "$ROOT"

STATE_PATH="data/player_state_historical_pbp_${TAG}.csv"
touch "$CHECKPOINT_FILE"

season_range() {
  local start_year="$1"
  case "$start_year" in
    1996) echo "1996-11-01 1997-04-20" ;;
    1998) echo "1998-10-01 1999-05-05" ;;
    2011) echo "2011-10-01 2012-04-26" ;;
    2019) echo "2019-10-01 2020-08-14" ;;
    2020) echo "2020-12-22 2021-05-16" ;;
    2025) echo "2025-10-01 2026-03-13" ;;
    *) echo "${start_year}-10-01 $((start_year+1))-06-30" ;;
  esac
}

done_key() {
  local mode="$1"
  local start="$2"
  local end="$3"
  python3 - <<PY
import csv
from pathlib import Path
p = Path(r"$CHECKPOINT_FILE")
mode = "$mode"
start = "$start"
end = "$end"
found = False
if p.exists():
    with p.open(newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if len(row) >= 4 and row[1] == mode and row[2] == start and row[3] == end:
                found = True
                break
print("yes" if found else "no")
PY
}

for start_year in $(seq 1996 2025); do
  read -r START END < <(season_range "$start_year")
  if [[ "$(done_key "$MODE" "$START" "$END")" == "yes" ]]; then
    echo "[$MODE] skip completed $START -> $END"
    continue
  fi
  "$BATCH_RUNNER" "$MODE" "$START" "$END" "$STATE_PATH" "$TAG" "$PBP_DIR" "$CHECKPOINT_FILE"
done

echo "[$MODE] queue complete"
