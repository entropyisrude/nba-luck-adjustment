#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 7 ]]; then
  echo "Usage: $0 <plain|vwd|dual> <start> <end> <state_in> <tag> <pbp_dir> <checkpoint_file>"
  exit 1
fi

MODE="$1"
START="$2"
END="$3"
STATE_IN="$4"
TAG="$5"
PBP_DIR="$6"
CHECKPOINT_FILE="$7"

ROOT="/mnt/c/users/dave/Downloads/nba-onoff-publish"
PY="/mnt/c/users/dave/Downloads/nba-3pt-adjust/nba-3pt-adjust/.venv/bin/python"

cd "$ROOT"

if [[ "$MODE" == "dual" ]]; then
  ONOFF_OUT="data/adjusted_onoff_historical_pbp_${TAG}_plain.csv"
  SECONDARY_ONOFF_OUT="data/adjusted_onoff_historical_pbp_${TAG}_vwd.csv"
else
  ONOFF_OUT="data/adjusted_onoff_historical_pbp_${TAG}.csv"
  SECONDARY_ONOFF_OUT=""
fi
STINTS_OUT="data/stints_historical_pbp_${TAG}.csv"
POSS_OUT="data/possessions_historical_pbp_${TAG}.csv"
STATE_OUT="data/player_state_historical_pbp_${TAG}.csv"

CMD=(
  "$PY" rebuild_historical_regular_season.py
  --start "$START"
  --end "$END"
  --pbp-dir "$PBP_DIR"
  --state-out "$STATE_OUT"
  --onoff-out "$ONOFF_OUT"
  --stints-out "$STINTS_OUT"
  --possessions-out "$POSS_OUT"
  --game-dates-path data/stints_historical_pbp_v2.csv
  --disable-lineup-overrides
  --stats-cache-only
)

if [[ -s "$STATE_IN" ]]; then
  CMD+=(--state-in "$STATE_IN")
fi

if [[ "$MODE" == "vwd" ]]; then
  CMD+=(--shot-priors-dir data/vwd_priors_by_season)
fi

if [[ "$MODE" == "dual" ]]; then
  CMD+=(--secondary-onoff-out "$SECONDARY_ONOFF_OUT" --secondary-shot-priors-dir data/vwd_priors_by_season)
fi

echo "[$MODE] batch start $START -> $END"
"${CMD[@]}"
printf '%s,%s,%s,%s\n' "$(date '+%F %T')" "$MODE" "$START" "$END" >> "$CHECKPOINT_FILE"
echo "[$MODE] batch complete $START -> $END"
