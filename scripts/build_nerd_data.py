"""Build data/nerd_seasons.js for the NERD leaderboard page.

NERD (Net Estimated Rating, Denoised) = the all-in-one metric's joint-solve
values (metric_v0: prior-informed multi-year luck-adjusted RAPM), one row
per player-season 1996-97..2025-26. The Kalman filtered state supplies the
uncertainty band (sd of O+D state). Team = most-minutes RS team that season.

Also emits PROJECTED rows for the upcoming season (season = PROJECT_TO):
each recently-active player's Kalman filtered state drifted along our aging
curves with process noise q per season — the honest one-step-ahead forecast
(same construction the backtest validated at 0.547). Projection rows carry
last season's possessions (for the min-poss filter) and the wider pred sd.

Inputs (desktop-only): nba-metric-data/metric/metric_v0.parquet,
nba-metric-data/kalman/kalman_states.parquet, nba-metric-data/aging/
aging_curves.csv, features_box_season.parquet, data/nba_analytics.duckdb.
Output: data/nerd_seasons.js  ->  window.NERD_DATA = {cols, rows}

Usage: python scripts/build_nerd_data.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
OUT = ROOT / "data" / "nerd_seasons.js"

COLS = ["season", "pid", "name", "team", "poss", "o", "d", "nerd", "sd"]

PROJECT_TO = 2026            # season_year of the upcoming season (2026-27)
PROJ_LAST_SEEN = 2024        # project players last seen this season or later
KALMAN_Q = 1.0               # process variance per season per side (Phase 4a)
MAX_DRIFT_YEARS = 4


def build_projections(k: pd.DataFrame, team: pd.DataFrame) -> list[list]:
    curves = pd.read_csv(METRIC_DATA / "aging" / "aging_curves.csv").dropna()
    drift_o = lambda a: float(np.interp(a, curves["age"], curves["d_orapm"]))
    drift_d = lambda a: float(np.interp(a, curves["age"], curves["d_drapm"]))
    # survivorship correction past 29 (see build_kalman_v0.old_drift_extra)
    sys.path.insert(0, str(ROOT / "metric"))
    from build_kalman_v0 import old_drift_extra
    feats = pd.read_parquet(METRIC_DATA / "features_box_season.parquet",
                            columns=["pid", "season_year", "age", "poss"])

    last = (k.sort_values("season_year").groupby("player_id").last()
            .reset_index())
    last = last[last["season_year"] >= PROJ_LAST_SEEN]
    last = last.merge(feats.rename(columns={"pid": "player_id"}),
                      on=["player_id", "season_year"], how="left")
    last = last.merge(team.rename(columns={"pid": "player_id"}),
                      on=["player_id", "season_year"], how="left")

    rows = []
    for r in last.itertuples(index=False):
        m_o, m_d = float(r.filt_o), float(r.filt_d)
        v_o, v_d = float(r.filt_var_o), float(r.filt_var_d)
        age = float(r.age) if pd.notna(r.age) else 27.0
        gap = PROJECT_TO - int(r.season_year)
        for step in range(min(gap, MAX_DRIFT_YEARS)):
            extra = old_drift_extra(age + step + 1)
            m_o += drift_o(age + step) + extra / 2
            m_d += drift_d(age + step) + extra / 2
        v_o += KALMAN_Q * gap
        v_d += KALMAN_Q * gap
        rows.append([PROJECT_TO, int(r.player_id), r.player_name,
                     r.team_abbr if pd.notna(r.team_abbr) else "",
                     int(round(r.poss)) if pd.notna(r.poss) else 0,
                     round(m_o, 2), round(m_d, 2), round(m_o + m_d, 2),
                     round(float(np.sqrt(v_o + v_d)), 2)])
    return rows


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
    proj = build_projections(k, team)
    rows += proj
    print(f"projection rows for {PROJECT_TO}-{str(PROJECT_TO+1)[-2:]}: {len(proj)}")
    payload = json.dumps({"model": "atomic_denominator",
                          "evidence": "canonical_counted_possessions_v1",
                          "cols": COLS,
                          "rows": rows}, separators=(",", ":"),
                         ensure_ascii=False)
    OUT.write_text("window.NERD_DATA = " + payload + ";\n", encoding="utf-8")
    print(f"wrote {OUT} ({len(rows)} rows, {OUT.stat().st_size/1e6:.2f} MB)")


if __name__ == "__main__":
    main()
