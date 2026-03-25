#!/usr/bin/env bash
set -euo pipefail

cd /mnt/c/users/dave/Downloads/nba-onoff-publish

PY="/mnt/c/users/dave/Downloads/nba-3pt-adjust/nba-3pt-adjust/.venv/bin/python"
PBP_DIR="/mnt/c/users/dave/Downloads/nba-3pt-adjust-local-backups/untracked_20260312_081227/data/pbp"

PLAIN_TAG="20260317_fixes_plain"
VWD_TAG="20260317_fixes_vwd"

rm -f \
  "/tmp/${PLAIN_TAG}_onoff.csv" \
  "/tmp/${PLAIN_TAG}_stints.csv" \
  "/tmp/${PLAIN_TAG}_poss.csv" \
  "/tmp/${PLAIN_TAG}_state.csv" \
  "/tmp/${VWD_TAG}_onoff.csv" \
  "/tmp/${VWD_TAG}_stints.csv" \
  "/tmp/${VWD_TAG}_poss.csv" \
  "/tmp/${VWD_TAG}_state.csv"

echo "[plain] starting $(date)"
"$PY" rebuild_historical_regular_season.py \
  --start 1996-11-01 \
  --end 2026-03-13 \
  --pbp-dir "$PBP_DIR" \
  --state-in "/tmp/${PLAIN_TAG}_seed_state.csv" \
  --state-out "/tmp/${PLAIN_TAG}_state.csv" \
  --onoff-out "/tmp/${PLAIN_TAG}_onoff.csv" \
  --stints-out "/tmp/${PLAIN_TAG}_stints.csv" \
  --possessions-out "/tmp/${PLAIN_TAG}_poss.csv" \
  --game-dates-path data/stints_historical_pbp_v2.csv \
  --disable-lineup-overrides \
  --recompute-existing \
  --stats-cache-only

cp "/tmp/${PLAIN_TAG}_onoff.csv" "data/adjusted_onoff_historical_pbp_${PLAIN_TAG}.csv"
cp "/tmp/${PLAIN_TAG}_stints.csv" "data/stints_historical_pbp_${PLAIN_TAG}.csv"
cp "/tmp/${PLAIN_TAG}_poss.csv" "data/possessions_historical_pbp_${PLAIN_TAG}.csv"
cp "/tmp/${PLAIN_TAG}_state.csv" "data/player_state_historical_pbp_${PLAIN_TAG}.csv"
echo "[plain] finished $(date)"

echo "[vwd] starting $(date)"
"$PY" rebuild_historical_regular_season.py \
  --start 1996-11-01 \
  --end 2026-03-13 \
  --pbp-dir "$PBP_DIR" \
  --state-in "/tmp/${VWD_TAG}_seed_state.csv" \
  --state-out "/tmp/${VWD_TAG}_state.csv" \
  --onoff-out "/tmp/${VWD_TAG}_onoff.csv" \
  --stints-out "/tmp/${VWD_TAG}_stints.csv" \
  --possessions-out "/tmp/${VWD_TAG}_poss.csv" \
  --game-dates-path data/stints_historical_pbp_v2.csv \
  --disable-lineup-overrides \
  --recompute-existing \
  --stats-cache-only \
  --shot-priors-dir data/vwd_priors_by_season

cp "/tmp/${VWD_TAG}_onoff.csv" "data/adjusted_onoff_historical_pbp_${VWD_TAG}.csv"
cp "/tmp/${VWD_TAG}_stints.csv" "data/stints_historical_pbp_${VWD_TAG}.csv"
cp "/tmp/${VWD_TAG}_poss.csv" "data/possessions_historical_pbp_${VWD_TAG}.csv"
cp "/tmp/${VWD_TAG}_state.csv" "data/player_state_historical_pbp_${VWD_TAG}.csv"
echo "[vwd] finished $(date)"
