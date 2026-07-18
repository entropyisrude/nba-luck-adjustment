"""Exploratory: does the box-stat -> impact relationship look different in the
playoffs than in the regular season?

The production box prior (build_box_prior.py) fits standardized box features
(computed from REGULAR-SEASON games only) onto rapm_target_hl550.parquet,
a multi-year decayed target that blends RS+playoffs. That target isn't
meaningful restricted to a single playoff run (a few hundred to ~2000
possessions per rotation player, vs 4000-8000 for a full RS) so this script
builds its own CAREER-POOLED (all playoff games/seasons a player ever
played, no time decay) joint RAPM target from playoff stints only, pairs it
with CAREER-POOLED playoff box features, and fits the same
Ridge(alpha=RIDGE_ALPHA) on standardized features to see whether the
coefficients diverge from the production (regular-season-fit) prior.

This is necessarily a small-sample, in-sample fit -- not enough playoff data
per player for honest leave-one-out validation. Treat the comparison as
descriptive, not a validated model.

Choices made for tractability (documented, not silently assumed):
  * age/height dropped from the feature set: our DB's player_game_facts
    table is RS-centric (only ~3.7k playoff rows out of 740k), too sparse
    to get a reliable career-average age/height specifically for playoff
    appearances. All other FEATURES_V1 inputs are reproduced.
  * gameType == "Playoffs" only (Play-in Tournament, Preseason, All-Star,
    and NBA Cup rows excluded -- confirmed via value_counts() below).

Usage: python metric/test_playoff_box_prior.py
"""
from __future__ import annotations

import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.linear_model import Ridge

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_rapm_target import prepare, HCOLS, ACOLS, MIN_SECONDS, load_player_names

ROOT = Path(__file__).resolve().parents[1]
EOIN_ZIP = ROOT / "historical-nba-data-and-player-box-scores.zip"
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PROD_COEF_PATH = METRIC_DATA / "priors" / "box_prior_coefficients.csv"

TARGET_ALPHA = 500     # matches build_rapm_target.py / build_box_prior.py TARGET_ALPHA
RIDGE_ALPHA = 50.0     # matches build_box_prior.py RIDGE_ALPHA, on standardized features
PROD_ERA = "2021-2025"

# same dicts as build_box_prior.py's build_features() -- copied verbatim
COUNT_COLS = {
    "points": "pts", "assists": "ast", "reboundsOffensive": "oreb",
    "reboundsDefensive": "dreb", "steals": "stl", "blocks": "blk",
    "turnovers": "tov", "foulsPersonal": "pf", "foulsAgainst": "fouls_drawn",
    "blocksAgainst": "blocked", "freeThrowsAttempted": "fta",
    "freeThrowsMade": "ftm", "fieldGoalsAttempted": "fga",
    "fieldGoalsMade": "fgm", "threePointersAttempted": "fg3a",
    "threePointersMade": "fg3m",
}
PCT_COLS = {
    "usagePercentage": "usg", "assistPercentage": "ast_pct",
    "offensiveReboundPercentage": "oreb_pct", "defensiveReboundPercentage": "dreb_pct",
    "trueShootingPercentage": "ts", "effectiveFieldGoalPercentage": "efg",
    "percentUnassistedFieldGoalsMade": "pct_unassisted",
    "percentPoints3Point": "pct_pts3", "percentPointsInPaint": "pct_paint",
    "percentPointsFastBreak": "pct_fb", "percentPointsFreeThrow": "pct_ft",
    "percentFieldGoalAttempts3Point": "pct_fga3",
}

# FEATURES_V1 from build_box_prior.py, minus age/height (see docstring)
FEATURES = ["pts_75", "ast_75", "oreb_75", "dreb_75", "stl_75", "blk_75",
            "tov_75", "pf_75", "fouls_drawn_75", "blocked_75", "fta_75",
            "fg3a_75", "fg3_rate", "ft_pct", "ts", "efg", "usg", "ast_pct",
            "oreb_pct", "dreb_pct", "pct_unassisted", "pct_pts3", "pct_paint",
            "pct_fb", "pct_ft", "pct_fga3", "mpg"]


# --------------------------------------------------------------------------
# Step 1: career-pooled, no-decay playoff RAPM target
# --------------------------------------------------------------------------

def build_playoff_target(st: pd.DataFrame) -> pd.DataFrame:
    st = st.dropna(subset=HCOLS + ACOLS).copy()
    st = st[(st["is_playoff"] == 1) & (st["seconds"] >= MIN_SECONDS)].reset_index(drop=True)
    st["poss"] = np.maximum(st["seconds"].to_numpy() / 24.0, 0.1)
    print(f"Playoff stints (career-pooled, all seasons): {len(st)}")

    players = np.unique(st[HCOLS + ACOLS].to_numpy().astype(int).ravel())
    pidx = {p: i for i, p in enumerate(players)}
    P = len(players)
    n = len(st)
    print(f"Players who logged playoff possessions: {P}")

    lookup = np.vectorize(pidx.get)
    hidx = lookup(st[HCOLS].to_numpy().astype(int))
    aidx = lookup(st[ACOLS].to_numpy().astype(int))

    # same sparse joint design as build_rapm_target.build_targets(): two rows
    # per stint, home-offense row credits home O + away D, away-offense row
    # credits away O + home D
    rows, cols, vals = [], [], []
    r = np.arange(n)
    for k in range(5):
        rows += [2 * r, 2 * r]
        cols += [hidx[:, k], P + aidx[:, k]]
        vals += [np.ones(n), np.ones(n)]
        rows += [2 * r + 1, 2 * r + 1]
        cols += [aidx[:, k], P + hidx[:, k]]
        vals += [np.ones(n), np.ones(n)]
    X = sparse.csr_matrix((np.concatenate(vals),
                           (np.concatenate(rows), np.concatenate(cols))),
                          shape=(2 * n, 2 * P))

    poss = st["poss"].to_numpy()
    y = np.empty(2 * n)
    y[0::2] = st["home_pts_adj"].to_numpy() / poss * 100.0
    y[1::2] = st["away_pts_adj"].to_numpy() / poss * 100.0

    # pure possession weighting, no time decay -- one career solve
    w = np.repeat(poss, 2)
    ybar = np.average(y, weights=w)
    Xw = X.multiply(np.sqrt(w)[:, None]).tocsr()
    yw = (y - ybar) * np.sqrt(w)
    XtX = (Xw.T @ Xw).toarray()
    Xty = Xw.T @ yw

    beta = np.linalg.solve(XtX + TARGET_ALPHA * np.eye(2 * P), Xty)
    O = beta[:P]
    D = beta[P:]
    om, dm = O.mean(), D.mean()

    w_on = np.zeros(P)
    for k in range(5):
        for idxs in (hidx[:, k], aidx[:, k]):
            np.add.at(w_on, idxs, poss)

    name_map = load_player_names()
    out = pd.DataFrame({
        "pid": players,
        "orapm": O - om,
        "drapm": -(D - dm),
    })
    out["rapm"] = out["orapm"] + out["drapm"]
    out["poss"] = w_on
    out["player_name"] = out["pid"].map(name_map)
    return out


# --------------------------------------------------------------------------
# Step 2: career-pooled playoff box features
# --------------------------------------------------------------------------

def build_playoff_features() -> pd.DataFrame:
    usecols = sorted(set(
        ["personId", "gameDateTimeEst", "gameType", "numMinutes", "possessions"]
        + list(COUNT_COLS) + list(PCT_COLS)))
    print("Loading extended box for playoff feature build...")
    with zipfile.ZipFile(EOIN_ZIP) as z:
        with z.open("PlayerStatisticsExtended.csv") as f:
            eo = pd.read_csv(f, usecols=usecols, low_memory=False)

    print("\ngameType value_counts (confirming the playoff-only filter):")
    print(eo["gameType"].value_counts())
    eo = eo[eo["gameType"] == "Playoffs"].copy()

    d = pd.to_datetime(eo["gameDateTimeEst"], errors="coerce")
    eo["season_year"] = (d.dt.year - (d.dt.month < 10)).astype(int)
    eo = eo[eo["season_year"] >= 1996]
    eo["pid"] = pd.to_numeric(eo["personId"], errors="coerce")
    eo = eo.dropna(subset=["pid"])
    eo["pid"] = eo["pid"].astype(int)
    eo["mins"] = pd.to_numeric(eo["numMinutes"], errors="coerce").fillna(0.0)
    eo["poss"] = pd.to_numeric(eo["possessions"], errors="coerce")
    eo["poss"] = eo["poss"].fillna(eo["mins"] * 2.08)
    eo = eo[eo["mins"] > 0]
    for c in list(COUNT_COLS) + list(PCT_COLS):
        eo[c] = pd.to_numeric(eo[c], errors="coerce")

    print(f"\n{len(eo)} playoff player-games ({eo['pid'].nunique()} players); "
          f"aggregating to career totals...")
    grp = eo.groupby("pid")
    agg = grp.agg(games=("mins", "size"), mins=("mins", "sum"), poss=("poss", "sum"),
                  **{v: (k, "sum") for k, v in COUNT_COLS.items()})
    # minutes-weighted means for pct features, career-pooled
    for k, v in PCT_COLS.items():
        eo["_w"] = eo[k] * eo["mins"]
        eo["_wd"] = eo["mins"].where(eo[k].notna())
        agg[v] = grp["_w"].sum() / grp["_wd"].sum()
    agg = agg.reset_index()

    per75 = ["pts", "ast", "oreb", "dreb", "stl", "blk", "tov", "pf",
             "fouls_drawn", "blocked", "fta", "fg3a"]
    for c in per75:
        agg[c + "_75"] = agg[c] / agg["poss"].clip(lower=1) * 75.0
    agg["fg3_rate"] = agg["fg3a"] / agg["fga"].clip(lower=1)
    agg["ft_pct"] = agg["ftm"] / agg["fta"].clip(lower=1)
    agg["mpg"] = agg["mins"] / agg["games"]
    # rename box-side possession total to avoid colliding with the RAPM
    # target's own poss column on merge -- step 3 weights/thresholds off the
    # target's poss (the RAPM design's possession weight), not this one
    return agg.rename(columns={"poss": "box_poss"})


# --------------------------------------------------------------------------
# Step 3: fit + compare
# --------------------------------------------------------------------------

def wcorr(a, b, w) -> float:
    a, b, w = np.asarray(a, float), np.asarray(b, float), np.asarray(w, float)
    am, bm = np.average(a, weights=w), np.average(b, weights=w)
    cov = np.average((a - am) * (b - bm), weights=w)
    return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                         * np.average((b - bm) ** 2, weights=w))


def fit_ridge(df: pd.DataFrame, target_col: str) -> tuple[np.ndarray, float]:
    X = df[FEATURES].to_numpy(dtype=float)
    w = df["poss"].to_numpy(dtype=float)
    mu = np.average(X, axis=0, weights=w)
    sd = np.sqrt(np.average((X - mu) ** 2, axis=0, weights=w)) + 1e-9
    Xs = (X - mu) / sd
    y = df[target_col].to_numpy(dtype=float)
    m = Ridge(alpha=RIDGE_ALPHA)
    m.fit(Xs, y, sample_weight=w)
    pred = m.predict(Xs)
    r = wcorr(pred, y, w)
    return m.coef_, r


def main() -> None:
    enc = sys.stdout.encoding or "utf-8"
    def p(s: str) -> None:
        print(s.encode(enc, errors="replace").decode(enc))

    print("=== Step 1: career-pooled playoff RAPM target ===")
    st = prepare()
    tgt = build_playoff_target(st)
    for thresh in (500, 1500):
        n = int((tgt["poss"] >= thresh).sum())
        print(f"Players with >= {thresh} career playoff possessions: {n}")

    n500 = int((tgt["poss"] >= 500).sum())
    n1500 = int((tgt["poss"] >= 1500).sum())
    # prefer the stricter (1500) cut -- more reliable per-player playoff RAPM
    # estimates -- as long as it still leaves a workable regression sample;
    # only fall back to the looser 500 cut if 1500 is too thin
    FIT_MIN_POSS = 1500 if n1500 >= 100 else 500
    print(f"Using FIT_MIN_POSS = {FIT_MIN_POSS} for the regression "
          f"(n500={n500}, n1500={n1500})")

    print("\n=== Step 2: career-pooled playoff box features ===")
    feats = build_playoff_features()

    print("\n=== Step 3: fit + compare ===")
    df = tgt.merge(feats, on="pid", how="inner")
    df = df[df["poss"] >= FIT_MIN_POSS].dropna(subset=FEATURES).reset_index(drop=True)
    print(f"Fit sample: {len(df)} players (poss >= {FIT_MIN_POSS}, all FEATURES non-null)")
    print("NOTE: small-sample, in-sample fit -- no leave-one-out validation is "
          "possible at this sample size. Treat as descriptive, not validated.")

    coef_o, r_o = fit_ridge(df, "orapm")
    coef_d, r_d = fit_ridge(df, "drapm")
    print(f"\nIn-sample weighted corr(prediction, target): O={r_o:.3f}  D={r_d:.3f}  "
          f"(n={len(df)})")

    play = pd.DataFrame({"feature": FEATURES, "playoff_o": coef_o, "playoff_d": coef_d})

    prod = pd.read_csv(PROD_COEF_PATH)
    prod = prod[prod["era"] == PROD_ERA]
    prod_o = prod[prod["target"] == "orapm"].set_index("feature")["coef"]
    prod_d = prod[prod["target"] == "drapm"].set_index("feature")["coef"]
    play["prod_o"] = play["feature"].map(prod_o)
    play["prod_d"] = play["feature"].map(prod_d)
    play["diff_o"] = play["playoff_o"] - play["prod_o"]
    play["diff_d"] = play["playoff_d"] - play["prod_d"]
    play["abs_diff"] = play["diff_o"].abs() + play["diff_d"].abs()

    tbl = play.sort_values("abs_diff", ascending=False).reset_index(drop=True)
    p("\nFull comparison (standardized coefficients), sorted by |diff_O|+|diff_D|:")
    p(tbl[["feature", "playoff_o", "prod_o", "playoff_d", "prod_d"]]
      .round(3).to_string(index=False))

    p(f"\nTop divergences, OFFENSE (playoff-only vs production, era {PROD_ERA}):")
    p(tbl.reindex(tbl["diff_o"].abs().sort_values(ascending=False).index)
      .head(8)[["feature", "playoff_o", "prod_o", "diff_o"]].round(3).to_string(index=False))

    p(f"\nTop divergences, DEFENSE (playoff-only vs production, era {PROD_ERA}):")
    p(tbl.reindex(tbl["diff_d"].abs().sort_values(ascending=False).index)
      .head(8)[["feature", "playoff_d", "prod_d", "diff_d"]].round(3).to_string(index=False))


if __name__ == "__main__":
    main()
