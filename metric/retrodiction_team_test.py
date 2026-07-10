"""Replicate the dunksandthrees metric-comparison retrodiction protocol.

https://dunksandthrees.com/blog/metric-comparison ranks metrics by how well
prior-season player values, weighted by ACTUAL minutes played in the target
season, predict the target season's team adjusted net rating. Their table
(RMSE, 180 team-seasons, targets 2014-15..2019-20): EPM 2.48, RPM 2.60,
RAPTOR 2.63, BPM 2.71, PIPM 2.78, RAPM 2.80.

We run the same protocol on our metrics plus RAPTOR and BPM. Absolute RMSEs
depend on target-construction details (we use SOS-adjusted point margin per
game from official per-player +/-), so the anchor is RELATIVE position:
where do we land against RAPTOR/BPM measured identically, and does the gap
pattern match theirs?

Conventions (theirs): players under 250 target-season minutes and rookies /
missing values -> replacement level (-2.0).

Usage: python metric/retrodiction_team_test.py
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT",
                           r"C:\Users\Dave\Downloads\nba-onoff-publish"))
RS_DB = ROOT / "data" / "nba_analytics.duckdb"
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
KALMAN_PATH = METRIC_DATA / "kalman" / "kalman_states.parquet"
METRIC_PATH = METRIC_DATA / "metric" / "metric_v0.parquet"
BBREF_DIR = METRIC_DATA / "benchmarks" / "bbref_advanced"
RAPTOR_PATH = METRIC_DATA / "benchmarks" / "historical_RAPTOR_by_player.csv"

TARGET_SEASONS = range(2014, 2020)   # season_year of the PREDICTED season
MIN_MINUTES = 250.0
REPLACEMENT = -2.0


def load_db():
    con = duckdb.connect(str(RS_DB), read_only=True)
    pm = con.execute("""
        SELECT CAST(substr(season,1,4) AS INTEGER) season_year,
               CAST(game_id AS VARCHAR) game_id, team_abbr,
               CAST(player_id AS BIGINT) pid, minutes,
               plus_minus_actual pm
        FROM player_game_facts
        WHERE CAST(game_id AS VARCHAR) LIKE '2%'
          AND CAST(substr(season,1,4) AS INTEGER) BETWEEN 2013 AND 2019
    """).df()
    con.close()
    return pm


def team_adjusted_net(pm: pd.DataFrame) -> pd.DataFrame:
    """SOS-adjusted point margin per game per team-season, from official
    per-player +/- (team margin = sum(pm)/5)."""
    tg = (pm.groupby(["season_year", "game_id", "team_abbr"])["pm"].sum() / 5.0) \
        .rename("margin").reset_index()
    rows = []
    for sy, g in tg.groupby("season_year"):
        # two rows per game; pair them
        pairs = g.merge(g, on="game_id", suffixes=("", "_opp"))
        pairs = pairs[pairs["team_abbr"] != pairs["team_abbr_opp"]]
        teams = np.sort(g["team_abbr"].unique())
        ti = {t: i for i, t in enumerate(teams)}
        X = np.zeros((len(pairs), len(teams)))
        X[np.arange(len(pairs)), pairs["team_abbr"].map(ti)] = 1.0
        X[np.arange(len(pairs)), pairs["team_abbr_opp"].map(ti)] = -1.0
        y = pairs["margin"].to_numpy()
        beta = np.linalg.solve(X.T @ X + 1.0 * np.eye(len(teams)), X.T @ y)
        beta -= beta.mean()
        for t, i in ti.items():
            rows.append({"season_year": sy, "team_abbr": t,
                         "adj_net": beta[i]})
    return pd.DataFrame(rows)


def load_ratings() -> dict[str, pd.DataFrame]:
    """(pid, season_year of the TARGET season) -> rating from t-1 info."""
    out = {}
    k = pd.read_parquet(KALMAN_PATH).rename(columns={"player_id": "pid"})
    out["ours_kalman"] = k[["pid", "season_year", "pred_total"]].rename(
        columns={"pred_total": "rating"})
    m = pd.read_parquet(METRIC_PATH).rename(columns={"player_id": "pid"})
    m = m[["pid", "season_year", "metric"]].copy()
    m["season_year"] += 1
    out["ours_static"] = m.rename(columns={"metric": "rating"})

    names = k.groupby("pid")["player_name"].last().reset_index()
    bb = []
    for p in sorted(BBREF_DIR.glob("advanced_*.csv")):
        yr = int(p.stem.split("_")[1])
        d = pd.read_csv(p)
        if "BPM" not in d.columns or "Player" not in d.columns:
            continue
        d = d[["Player", "BPM", "MP"]].copy()
        d["MP"] = pd.to_numeric(d["MP"], errors="coerce")
        d = d.sort_values("MP", ascending=False).drop_duplicates("Player")
        d["season_year"] = yr   # measured yr-1 -> applies to target yr
        bb.append(d)
    bb = pd.concat(bb, ignore_index=True)
    bb["BPM"] = pd.to_numeric(bb["BPM"], errors="coerce")
    bb = bb.merge(names, left_on="Player", right_on="player_name")
    out["bpm"] = bb.dropna(subset=["BPM"])[["pid", "season_year", "BPM"]] \
        .rename(columns={"BPM": "rating"})

    rap = pd.read_csv(RAPTOR_PATH).merge(names, on="player_name")
    rap = rap.rename(columns={"season": "season_year"})
    out["raptor"] = (rap.dropna(subset=["raptor_total"])
                     .groupby(["pid", "season_year"])["raptor_total"].mean()
                     .rename("rating").reset_index())
    return {n: r.drop_duplicates(["pid", "season_year"]) for n, r in out.items()}


def main() -> None:
    pm = load_db()
    actual = team_adjusted_net(pm)
    print(f"actual adjusted nets: {len(actual)} team-seasons")

    mins = (pm.groupby(["season_year", "team_abbr", "pid"])["minutes"].sum()
            .rename("mins").reset_index())
    mins = mins[mins["season_year"].isin(TARGET_SEASONS)]
    ratings = load_ratings()

    res = {}
    for name, r in ratings.items():
        df = mins.merge(r, on=["pid", "season_year"], how="left")
        low = (df["mins"] < MIN_MINUTES) | df["rating"].isna()
        df.loc[low, "rating"] = REPLACEMENT
        # minutes-share-weighted lineup value: sum_i (min_i / (team_min/5)) * r_i / 5
        team = df.groupby(["season_year", "team_abbr"]).apply(
            lambda g: np.sum(g["mins"] * g["rating"]) / g["mins"].sum(),
            include_groups=False).rename("pred_avg").reset_index()
        team["pred"] = team["pred_avg"] * 5.0
        j = team.merge(actual, on=["season_year", "team_abbr"])
        # one free scale+intercept per metric (units differ per-100 vs
        # per-game etc.); fit on all pairs, same for every metric
        b = np.polyfit(j["pred"], j["adj_net"], 1)
        j["pred_cal"] = b[0] * j["pred"] + b[1]
        rmse = float(np.sqrt(np.mean((j["pred_cal"] - j["adj_net"]) ** 2)))
        rmse_raw = float(np.sqrt(np.mean((j["pred"] - j["adj_net"]) ** 2)))
        corr = float(np.corrcoef(j["pred"], j["adj_net"])[0, 1])
        res[name] = (rmse, rmse_raw, corr, len(j))

    print(f"\nRetrodiction (targets {min(TARGET_SEASONS)}-{max(TARGET_SEASONS)}, "
          f"minutes-weighted prior-season ratings -> team adj net, "
          f"<{MIN_MINUTES:.0f} min & missing = {REPLACEMENT}):")
    print(f"{'metric':>12}  {'RMSE(cal)':>9}  {'RMSE(raw)':>9}  {'corr':>6}  n")
    for name, (rmse, rr, corr, n) in sorted(res.items(), key=lambda kv: kv[1][0]):
        print(f"{name:>12}  {rmse:9.3f}  {rr:9.3f}  {corr:6.3f}  {n}")
    print("\ntheir table (same protocol, their target): EPM 2.48  RPM 2.60  "
          "RAPTOR 2.63  BPM 2.71  PIPM 2.78  RAPM 2.80")


if __name__ == "__main__":
    main()
