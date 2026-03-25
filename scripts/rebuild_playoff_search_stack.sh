#!/usr/bin/env bash
set -euo pipefail

ROOT="/mnt/c/users/dave/Downloads/nba-onoff-publish"
PY="/tmp/nba-onoff-publish-linux/.venv/bin/python"

cd "$ROOT"

echo "[1/4] Building possessions_playoffs.csv"
"$PY" -u "$ROOT/scripts/build_playoff_possessions.py" --workers 4

echo "[2/4] Building nba_analytics_playoffs.duckdb"
"$PY" "$ROOT/scripts/build_playoff_analytics_duckdb.py"

echo "[3/4] Generating game-search-playoffs.html"
"$PY" "$ROOT/generate_player_game_search_playoffs.py"

echo "[4/4] Generating player-span-search-playoffs.html"
"$PY" "$ROOT/generate_player_span_search_playoffs.py"

echo "Playoff search stack rebuild complete"
