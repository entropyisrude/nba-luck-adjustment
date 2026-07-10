"""Build data/nerd_seasons.js for the NERD leaderboard page.

NERD (Net Estimated Rating, Denoised) = the all-in-one metric's joint-solve
values (metric_v0: prior-informed multi-year luck-adjusted RAPM), one row
per player-season 1996-97..2025-26. The Kalman filtered state supplies the
uncertainty band (sd of O+D state). Team = most-minutes RS team that season.

Inputs (desktop-only): nba-metric-data/metric/metric_v0.parquet,
nba-metric-data/kalman/kalman_states.parquet, data/nba_analytics.duckdb.
Output: data/nerd_seasons.js  ->  window.NERD_DATA = {cols, rows}

Usage: python scripts/build_nerd_data.py
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
OUT = ROOT / "data" / "nerd_seasons.js"

COLS = ["season", "pid", "name", "team", "poss", "o", "d", "nerd", "sd"]


def main() -> None:
    m = pd.read_parquet(METRIC_DATA / "metric" / "metric_v0.parquet")
    m = m[["season_year", "player_id", "player_name", "poss_season",
           "metric_o", "metric_d", "metric"]]

    k = pd.read_parquet(METRIC_DATA / "kalman" / "kalman_states.parquet")
    k["sd"] = np.sqrt(k["filt_var_o"] + k["filt_var_d"])
    m = m.merge(k[["player_id", "season_year", "sd"]],
                on=["player_id", "season_year"], how="left")

    con = duckdb.connect(str(ROOT / "data" / "nba_analytics.duckdb"), read_only=True)
    team = con.execute("""
        SELECT pid, season_year, team_abbr FROM (
            SELECT pid, season_year, team_abbr,
                   row_number() OVER (
                       PARTITION BY pid, season_year
                       ORDER BY mins DESC) rn
            FROM (
                SELECT CAST(player_id AS BIGINT) pid,
                       CAST(substr(season,1,4) AS INTEGER) season_year,
                       team_abbr, sum(minutes) mins
                FROM player_game_facts
                WHERE CAST(game_id AS VARCHAR) LIKE '2%'
                GROUP BY 1, 2, 3))
        WHERE rn = 1""").df()
    con.close()
    m = m.merge(team.rename(columns={"pid": "player_id"}),
                on=["player_id", "season_year"], how="left")
    m["team_abbr"] = m["team_abbr"].fillna("")

    rows = [[int(r.season_year), int(r.player_id), r.player_name, r.team_abbr,
             int(round(r.poss_season)),
             round(float(r.metric_o), 2), round(float(r.metric_d), 2),
             round(float(r.metric), 2),
             None if pd.isna(r.sd) else round(float(r.sd), 2)]
            for r in m.itertuples(index=False)]
    payload = json.dumps({"cols": COLS, "rows": rows}, separators=(",", ":"),
                         ensure_ascii=False)
    OUT.write_text("window.NERD_DATA = " + payload + ";\n", encoding="utf-8")
    print(f"wrote {OUT} ({len(rows)} rows, {OUT.stat().st_size/1e6:.2f} MB)")


if __name__ == "__main__":
    main()
