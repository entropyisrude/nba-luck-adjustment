"""Modern chronological margin test for production vs multivariate states.

Uses regular-season player-game facts from the local analytics database.  Each
season's ratings are strictly the filters' pre-observation ``pred_total``.
Actual-minutes and trailing pregame projected-minutes variants are calibrated
separately with an affine margin model on the previous four seasons.

This is a validation script; outputs stay beside the
multivariate prototype and no production artifact is modified.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "nba_analytics.duckdb"
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
MV_ROOT = ROOT / "outputs" / "contextual_causal" / "multivariate_kalman"
MV_PATH = MV_ROOT / "c20000_sb8" / "multivariate_kalman_states.parquet"
OUT = MV_ROOT / "c20000_sb8" / "modern_margin_backtest.parquet"
REPLACEMENT = -2.0
CALIB_YEARS = 4
EVAL_START = 2019


def load_player_games() -> pd.DataFrame:
    con = duckdb.connect(str(DB), read_only=True)
    pg = con.execute("""
        SELECT CAST(game_id AS VARCHAR) game_id, date,
               CAST(substr(season,1,4) AS INTEGER) season_year,
               CAST(player_id AS BIGINT) pid, CAST(team_id AS BIGINT) team_id,
               lower(home_away) home_away, minutes,
               team_pts_actual, opp_pts_actual
        FROM player_game_facts
        WHERE CAST(game_id AS VARCHAR) LIKE '2%'
          AND CAST(substr(season,1,4) AS INTEGER) BETWEEN 2015 AND 2025
          AND minutes > 0
    """).df()
    con.close()
    pg = pg.drop_duplicates(["game_id", "pid", "team_id"])
    return pg


def add_projected_minutes(pg: pd.DataFrame) -> pd.DataFrame:
    pg = pg.sort_values(["pid", "date", "game_id"]).copy()
    prior = (pg.groupby(["pid", "season_year"]).minutes
             .agg(prev_mpg="mean").reset_index())
    prior["season_year"] += 1
    pg = pg.merge(prior, on=["pid", "season_year"], how="left")
    g = pg.groupby(["pid", "season_year"]).minutes
    pg["min_proj"] = ((g.cumsum() - pg.minutes)
                      / g.cumcount().replace(0, np.nan))
    pg["min_proj"] = pg.min_proj.fillna(pg.prev_mpg).fillna(15.0)
    scale = pg.groupby(["game_id", "team_id"]).min_proj.transform("sum")
    pg["min_proj"] *= 240.0 / scale
    return pg


def strengths(pg: pd.DataFrame) -> pd.DataFrame:
    prod = pd.read_parquet(METRIC_DATA / "kalman" / "kalman_states.parquet")
    prod = prod[["player_id", "season_year", "pred_total"]].rename(
        columns={"player_id": "pid", "pred_total": "production"})
    mv = pd.read_parquet(MV_PATH)[
        ["player_id", "season_year", "pred_total"]].rename(
            columns={"player_id": "pid", "pred_total": "multivariate"})
    pg = pg.merge(prod, on=["pid", "season_year"], how="left")
    pg = pg.merge(mv, on=["pid", "season_year"], how="left")
    joint_path = METRIC_DATA / "metric" / "metric_v1_kcenter.parquet"
    joint = pd.read_parquet(joint_path)[
        ["player_id", "season_year", "metric"]].rename(
            columns={"player_id": "pid", "metric": "jointk"})
    joint["season_year"] += 1
    pg = pg.merge(joint, on=["pid", "season_year"], how="left")
    print("rating coverage", {
        c: f"{pg[c].notna().mean():.1%}"
        for c in ("production", "multivariate", "jointk")})
    for c in ("production", "multivariate", "jointk"):
        pg[c] = pg[c].fillna(REPLACEMENT)

    blocks = []
    for minutes, suffix in (("minutes", "actual"), ("min_proj", "proj")):
        q = pg[["game_id", "team_id"]].copy()
        for model in ("production", "multivariate", "jointk"):
            q[f"{model}_{suffix}"] = pg[minutes] / 48.0 * pg[model]
        blocks.append(q.groupby(["game_id", "team_id"], as_index=False).sum())
    return blocks[0].merge(blocks[1], on=["game_id", "team_id"])


def games(pg: pd.DataFrame, strength: pd.DataFrame) -> pd.DataFrame:
    team = (pg.groupby(["game_id", "team_id", "season_year", "home_away"],
                       as_index=False)
            .agg(points=("team_pts_actual", "max")))
    team = team.merge(strength, on=["game_id", "team_id"])
    home = team[team.home_away.eq("home")].drop(columns="home_away")
    away = team[team.home_away.eq("away")].drop(columns="home_away")
    g = home.merge(away, on=["game_id", "season_year"],
                   suffixes=("_home", "_away"), validate="one_to_one")
    g["margin"] = g.points_home - g.points_away
    for model in ("production", "multivariate", "jointk"):
        for mv in ("actual", "proj"):
            g[f"diff_{model}_{mv}"] = (
                g[f"{model}_{mv}_home"] - g[f"{model}_{mv}_away"])
    return g


def main() -> None:
    pg = add_projected_minutes(load_player_games())
    g = games(pg, strengths(pg))
    rows = []
    for sy in range(EVAL_START, int(g.season_year.max()) + 1):
        train = g[(g.season_year >= sy - CALIB_YEARS)
                  & (g.season_year < sy)]
        test = g[g.season_year == sy].copy()
        if train.empty or test.empty:
            continue
        for model in ("production", "multivariate", "jointk"):
            for mv in ("actual", "proj"):
                col = f"diff_{model}_{mv}"
                X = np.c_[np.ones(len(train)), train[col].to_numpy(float)]
                beta = np.linalg.lstsq(X, train.margin.to_numpy(float),
                                       rcond=None)[0]
                test[f"pred_{model}_{mv}"] = beta[0] + beta[1] * test[col]
        rows.append(test)
    out = pd.concat(rows, ignore_index=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)
    for sy, q in [("ALL", out)] + list(out.groupby("season_year")):
        print(f"\n{sy} n={len(q)}")
        for model in ("production", "multivariate", "jointk"):
            for mv in ("actual", "proj"):
                pred = q[f"pred_{model}_{mv}"]
                mae = np.mean(np.abs(q.margin - pred))
                corr = np.corrcoef(q.margin, pred)[0, 1]
                print(f"  {model:12} {mv:6} MAE {mae:.4f} corr {corr:.4f}")
    print(f"wrote {OUT} ({len(out)} games)")


if __name__ == "__main__":
    main()
