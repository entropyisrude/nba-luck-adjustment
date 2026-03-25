#!/usr/bin/env bash
set -euo pipefail

cd /mnt/c/users/dave/Downloads/nba-onoff-publish

exec /mnt/c/users/dave/Downloads/nba-3pt-adjust/nba-3pt-adjust/.venv/bin/python \
  rebuild_historical_regular_season.py \
  --start 2012-04-27 \
  --end 2026-03-13 \
  --pbp-dir /mnt/c/users/dave/Downloads/nba-3pt-adjust-local-backups/untracked_20260312_081227/data/pbp \
  --state-in data/player_state_historical_pbp_20260317_vwd.csv \
  --state-out /tmp/vwd_tail_20260317_state.csv \
  --onoff-out /tmp/vwd_tail_20260317_onoff.csv \
  --stints-out /tmp/vwd_tail_20260317_stints.csv \
  --possessions-out /tmp/vwd_tail_20260317_poss.csv \
  --game-dates-path data/stints_historical_pbp_v2.csv \
  --disable-lineup-overrides \
  --recompute-existing \
  --stats-cache-only \
  --shot-priors-dir data/vwd_priors_by_season
