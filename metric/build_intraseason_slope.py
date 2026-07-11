"""Intra-season trajectory test: does within-season skill movement predict
next season beyond the season-average Kalman forecast?

Pipeline: per-game filtered skill states (build_game_kalman --trajectories)
-> each player-season's state at his possession midpoint vs season end ->
the change mapped through an impact-direction vector (ridge of season
features onto the Phase-1 target, fit on train years only) -> a scalar
"slope" = implied within-season impact change.

Tests (temporal split, train season_year <= 2018):
  1. Does slope carry incremental weight predicting next-season evidence
     ON TOP of the Kalman one-step prediction (the production bar)?
  2. Age-group breakdown (young improvers vs old decliners).

Interpretation caveat printed with results: a positive slope effect can be
real momentum OR the averaging artifact (season means undersell a mid-year
improver's end level). Both improve projections; they differ in story.

Usage: python metric/build_intraseason_slope.py
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
TRAJ = METRIC_DATA / "game_kalman" / "state_trajectories.parquet"
FEATS = METRIC_DATA / "features_box_season.parquet"
TARGET = METRIC_DATA / "targets" / "rapm_target_hl550.parquet"
KALMAN = METRIC_DATA / "kalman" / "kalman_states.parquet"
EVID = METRIC_DATA / "evidence_season.parquet"

TRAIN_MAX = 2018
MIN_POSS = 2500          # player-season floor for a meaningful slope
RIDGE = 50.0

# game-feature -> season-feature name (identical except mins -> mpg)
GAME_FEATS = ["pts_75", "ast_75", "oreb_75", "dreb_75", "stl_75", "blk_75",
              "tov_75", "pf_75", "fouls_drawn_75", "blocked_75", "fta_75",
              "fg3a_75", "fg3_rate", "ft_pct", "usg", "ast_pct", "oreb_pct",
              "dreb_pct", "ts", "efg", "pct_unassisted", "pct_pts3",
              "pct_paint", "pct_fb", "pct_ft", "pct_fga3", "mins"]
SEASON_NAME = {c: ("mpg" if c == "mins" else c) for c in GAME_FEATS}


def wcorr(a, b, w):
    a, b, w = map(np.asarray, (a, b, w))
    am, bm = np.average(a, weights=w), np.average(b, weights=w)
    cov = np.average((a - am) * (b - bm), weights=w)
    return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                         * np.average((b - bm) ** 2, weights=w))


def main() -> None:
    traj = pd.read_parquet(TRAJ)
    fcols = [c + "_filt" for c in GAME_FEATS]
    traj = traj.sort_values(["pid", "season_year", "date"]).reset_index(drop=True)
    g = traj.groupby(["pid", "season_year"])
    traj["cum"] = g["poss"].cumsum()
    tot = g["poss"].transform("sum")
    traj["half"] = traj["cum"] <= tot / 2
    # state at midpoint = last row of first half; state at end = last row
    mid = traj[traj["half"]].groupby(["pid", "season_year"], as_index=False).tail(1)
    end = traj.groupby(["pid", "season_year"], as_index=False).tail(1)
    key = ["pid", "season_year"]
    m = mid[key + fcols].merge(end[key + fcols], on=key, suffixes=("_mid", "_end"))
    m = m.merge(tot.rename("tot_poss").to_frame().join(traj[key]).drop_duplicates(key),
                on=key)
    m = m[m["tot_poss"] >= MIN_POSS].reset_index(drop=True)
    print(f"{len(m)} player-seasons with >= {MIN_POSS} poss")

    # impact-direction vector: ridge of SEASON features onto the Phase-1
    # target, standardized, fit on train years only
    feats = pd.read_parquet(FEATS)
    tgt = pd.read_parquet(TARGET)
    tgt = tgt[tgt["alpha"] == 500].copy()
    tgt["season_year"] = tgt["target_season"].str[:4].astype(int)
    scols = [SEASON_NAME[c] for c in GAME_FEATS]
    tr = feats.merge(tgt.rename(columns={"player_id": "pid"}),
                     on=["pid", "season_year"])
    tr = tr[(tr["poss_season"] >= 1000) & (tr["season_year"] <= TRAIN_MAX)]
    tr = tr.dropna(subset=scols)
    X = tr[scols].to_numpy(float)
    w = tr["poss_season"].to_numpy()
    mu = np.average(X, axis=0, weights=w)
    sd = np.sqrt(np.average((X - mu) ** 2, axis=0, weights=w)) + 1e-9
    Xz = (X - mu) / sd
    beta = np.linalg.solve((Xz * w[:, None]).T @ Xz + RIDGE * np.eye(len(scols)),
                           (Xz * w[:, None]).T @ tr["rapm"].to_numpy())

    # slope = impact-weighted, standardized within-season state change
    d = (m[[c + "_end" for c in fcols]].to_numpy(float)
         - m[[c + "_mid" for c in fcols]].to_numpy(float))
    m["slope"] = (d / sd) @ beta
    print(f"slope sd {m['slope'].std():.3f}; "
          f"top movers: {m.nlargest(3, 'slope')[key + ['slope']].values.tolist()}")

    # tests: next-season evidence vs kalman pred (+ slope)
    k = pd.read_parquet(KALMAN).rename(columns={"player_id": "pid"})
    ev = pd.read_parquet(EVID).rename(columns={"player_id": "pid"})
    ev["evid"] = ev["ev_o"] + ev["ev_d"]
    j = m[key + ["slope"]].copy()
    j["next"] = j["season_year"] + 1
    j = j.merge(k[["pid", "season_year", "pred_total"]],
                left_on=["pid", "next"], right_on=["pid", "season_year"],
                suffixes=("", "_k"))
    j = j.merge(ev[["pid", "season_year", "evid", "ev_poss"]],
                left_on=["pid", "next"], right_on=["pid", "season_year"],
                suffixes=("", "_e"))
    j = j[j["ev_poss"] >= 1000]
    trn = j[j["season_year"] <= TRAIN_MAX]
    tst = j[j["season_year"] > TRAIN_MAX]
    print(f"pairs: train {len(trn)}, test {len(tst)}")

    def fit_eval(cols):
        Xt = np.column_stack([np.ones(len(trn))] + [trn[c] for c in cols])
        wt = trn["ev_poss"].to_numpy()
        b, *_ = np.linalg.lstsq(Xt * np.sqrt(wt)[:, None],
                                trn["evid"].to_numpy() * np.sqrt(wt), rcond=None)
        Xs = np.column_stack([np.ones(len(tst))] + [tst[c] for c in cols])
        pred = Xs @ b
        return b, wcorr(pred, tst["evid"], tst["ev_poss"])

    b0, r0 = fit_eval(["pred_total"])
    b1, r1 = fit_eval(["pred_total", "slope"])
    print(f"\nOOS (test {TRAIN_MAX+1}+) predicting next-season evidence:")
    print(f"  kalman pred only        : wcorr {r0:.4f}")
    print(f"  + within-season slope   : wcorr {r1:.4f}   "
          f"(slope coef {b1[2]:+.3f})")

    # age breakdown
    ages = pd.read_parquet(FEATS, columns=["pid", "season_year", "age"])
    ja = j.merge(ages, on=["pid", "season_year"], how="left").dropna(subset=["age"])
    print("\nslope coef by age group (train fit; resid of evid on pred_total):")
    for lo, hi, lbl in [(18, 24, "<=23"), (24, 30, "24-29"), (30, 45, "30+")]:
        gg = ja[(ja["age"] >= lo) & (ja["age"] < hi) & (ja["season_year"] <= TRAIN_MAX)]
        if len(gg) < 100:
            continue
        wt = gg["ev_poss"].to_numpy()
        Xg = np.column_stack([np.ones(len(gg)), gg["pred_total"], gg["slope"]])
        bg, *_ = np.linalg.lstsq(Xg * np.sqrt(wt)[:, None],
                                 gg["evid"].to_numpy() * np.sqrt(wt), rcond=None)
        print(f"  age {lbl:>6}: slope coef {bg[2]:+.3f} (n={len(gg)})")

    out = m[key + ["tot_poss", "slope"]]
    out.to_parquet(METRIC_DATA / "game_kalman" / "intraseason_slope.parquet",
                   index=False)
    print(f"\nwrote {METRIC_DATA / 'game_kalman' / 'intraseason_slope.parquet'}")


if __name__ == "__main__":
    main()
