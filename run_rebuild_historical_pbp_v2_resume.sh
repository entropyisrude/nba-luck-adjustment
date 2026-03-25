#!/usr/bin/env bash
set -u

cd /mnt/c/users/dave/Downloads/nba-onoff-publish || exit 1

LOG="logs/rebuild_historical_pbp_v2_resume.log"
PY="/mnt/c/users/dave/Downloads/nba-3pt-adjust/nba-3pt-adjust/.venv/bin/python"

{
  echo "START $(date -Is)"
  echo "PWD $(pwd)"
  echo "CMD rebuild_historical_regular_season.py --start 2011-12-25 --end 2026-03-07 --disable-lineup-overrides"
  "$PY" -u rebuild_historical_regular_season.py \
    --start 2011-12-25 \
    --end 2026-03-07 \
    --pbp-dir /mnt/c/users/dave/Downloads/nba-3pt-adjust-local-backups/untracked_20260312_081227/data/pbp \
    --state-in data/player_state_historical_pbp_v2.csv \
    --state-out data/player_state_historical_pbp_v2.csv \
    --onoff-out data/adjusted_onoff_historical_pbp_v2.csv \
    --stints-out data/stints_historical_pbp_v2.csv \
    --possessions-out data/possessions_historical_pbp_v2.csv \
    --starter-overrides-path data/stints_historical.csv \
    --game-dates-path data/stints_historical.csv \
    --disable-lineup-overrides
  rc=$?
  echo "EXIT $rc $(date -Is)"
  exit "$rc"
} >> "$LOG" 2>&1
