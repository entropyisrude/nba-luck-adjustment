"""Phase 2 of the all-in-one metric: the box-score prior.

Regularized regression of season box-profile features onto the Phase 1
multi-year luck-adjusted RAPM target (alpha=500), offense and defense fit
separately, in era buckets, observations weighted by current-season
possessions. This is the "what should we expect this player's impact to be,
given how he plays" component that the Bayesian combination (Phase 3) shrinks
thin RAPM samples toward.

Features come from the Eoin extended per-game box (usage, assisted/unassisted
splits, scoring mix, fouls drawn, ...) aggregated to player-season rates
(regular season only), plus age and height from our DB. Feature aggregation
is cached at FEATURES_CACHE (delete to rebuild, ~5 min).

Validation:
  * leave-one-season-out within each era (the honest fit metric),
  * strict-temporal (train strictly earlier seasons only),
  * baseline comparison: how well does BBRef BPM explain the same target on
    the same held-out player-seasons?

Outputs (OUT_DIR): box_prior.parquet/csv (player-season O/D/total prior,
in-sample and LOSO variants) and box_prior_coefficients.csv.

Usage: python metric/build_box_prior.py [--features-only]
"""
from __future__ import annotations

import argparse
import os
import zipfile
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
EOIN_ZIP = ROOT / "historical-nba-data-and-player-box-scores.zip"
RS_DB = ROOT / "data" / "nba_analytics.duckdb"

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
TARGET_PATH = METRIC_DATA / "targets" / "rapm_target_hl550.parquet"
FEATURES_CACHE = METRIC_DATA / "features_box_season.parquet"
BBREF_DIR = METRIC_DATA / "benchmarks" / "bbref_advanced"
OUT_DIR = METRIC_DATA / "priors"

TARGET_ALPHA = 500
MIN_FIT_POSS = 1000      # player-seasons below this inform nothing; still predicted
RIDGE_ALPHA = 50.0       # on standardized features
ERAS = [(1996, 2003), (2004, 2010), (2011, 2016), (2017, 2020), (2021, 2025)]

# per-game columns summed then converted to per-75-possession rates
COUNT_COLS = {
    "points": "pts", "assists": "ast", "reboundsOffensive": "oreb",
    "reboundsDefensive": "dreb", "steals": "stl", "blocks": "blk",
    "turnovers": "tov", "foulsPersonal": "pf", "foulsAgainst": "fouls_drawn",
    "blocksAgainst": "blocked", "freeThrowsAttempted": "fta",
    "freeThrowsMade": "ftm", "fieldGoalsAttempted": "fga",
    "fieldGoalsMade": "fgm", "threePointersAttempted": "fg3a",
    "threePointersMade": "fg3m",
}
# per-game percentage/ratio columns averaged weighted by minutes
PCT_COLS = {
    "usagePercentage": "usg", "assistPercentage": "ast_pct",
    "offensiveReboundPercentage": "oreb_pct", "defensiveReboundPercentage": "dreb_pct",
    "trueShootingPercentage": "ts", "effectiveFieldGoalPercentage": "efg",
    "percentUnassistedFieldGoalsMade": "pct_unassisted",
    "percentPoints3Point": "pct_pts3", "percentPointsInPaint": "pct_paint",
    "percentPointsFastBreak": "pct_fb", "percentPointsFreeThrow": "pct_ft",
    "percentFieldGoalAttempts3Point": "pct_fga3",
}


def season_label(y: int) -> str:
    return f"{y}-{str(y + 1)[-2:]}"


# --------------------------------------------------------------------------
# feature construction (cached)
# --------------------------------------------------------------------------

def build_features() -> pd.DataFrame:
    if FEATURES_CACHE.exists():
        print(f"Using cached {FEATURES_CACHE}")
        return pd.read_parquet(FEATURES_CACHE)

    usecols = (["personId", "gameDateTimeEst", "gameType", "numMinutes", "possessions"]
               + list(COUNT_COLS) + list(PCT_COLS))
    print("Loading extended box (one-time, cached afterwards)...")
    with zipfile.ZipFile(EOIN_ZIP) as z:
        with z.open("PlayerStatisticsExtended.csv") as f:
            eo = pd.read_csv(f, usecols=usecols, low_memory=False)
    eo = eo[eo["gameType"] == "Regular Season"].copy()
    d = pd.to_datetime(eo["gameDateTimeEst"], errors="coerce")
    eo["season_year"] = (d.dt.year - (d.dt.month < 10)).astype(int)
    eo = eo[eo["season_year"] >= 1996]
    eo["pid"] = pd.to_numeric(eo["personId"], errors="coerce")
    eo = eo.dropna(subset=["pid"])
    eo["pid"] = eo["pid"].astype(int)
    eo["mins"] = pd.to_numeric(eo["numMinutes"], errors="coerce").fillna(0.0)
    eo["poss"] = pd.to_numeric(eo["possessions"], errors="coerce")
    # possessions occasionally null -> minutes-based estimate at league pace
    eo["poss"] = eo["poss"].fillna(eo["mins"] * 2.08)
    eo = eo[eo["mins"] > 0]
    for c in list(COUNT_COLS) + list(PCT_COLS):
        eo[c] = pd.to_numeric(eo[c], errors="coerce")

    print(f"  {len(eo)} player-games; aggregating to player-seasons...")
    grp = eo.groupby(["pid", "season_year"])
    agg = grp.agg(games=("mins", "size"), mins=("mins", "sum"), poss=("poss", "sum"),
                  **{v: (k, "sum") for k, v in COUNT_COLS.items()})
    # minutes-weighted means for pct features
    for k, v in PCT_COLS.items():
        eo["_w"] = eo[k] * eo["mins"]
        agg[v] = grp["_w"].sum() / grp.apply(
            lambda g: g.loc[g[k].notna(), "mins"].sum(), include_groups=False)
    agg = agg.reset_index()

    per75 = ["pts", "ast", "oreb", "dreb", "stl", "blk", "tov", "pf",
             "fouls_drawn", "blocked", "fta", "fg3a"]
    for c in per75:
        agg[c + "_75"] = agg[c] / agg["poss"].clip(lower=1) * 75.0
    agg["fg3_rate"] = agg["fg3a"] / agg["fga"].clip(lower=1)
    agg["ft_pct"] = agg["ftm"] / agg["fta"].clip(lower=1)
    agg["mpg"] = agg["mins"] / agg["games"]

    # age + height from our DB
    con = duckdb.connect(str(RS_DB), read_only=True)
    bio = con.execute("""
        SELECT CAST(player_id AS BIGINT) pid,
               CAST(substr(season, 1, 4) AS INTEGER) season_year,
               avg(age) age, avg(height_inches) height
        FROM player_game_facts
        WHERE CAST(game_id AS VARCHAR) LIKE '2%'
        GROUP BY 1, 2""").df()
    con.close()
    agg = agg.merge(bio, on=["pid", "season_year"], how="left")
    agg["age"] = agg["age"].fillna(agg["age"].median())
    agg["height"] = agg["height"].fillna(agg["height"].median())

    METRIC_DATA.mkdir(parents=True, exist_ok=True)
    agg.to_parquet(FEATURES_CACHE, index=False)
    print(f"Cached {len(agg)} player-season feature rows to {FEATURES_CACHE}")
    return agg


FEATURES = ["pts_75", "ast_75", "oreb_75", "dreb_75", "stl_75", "blk_75",
            "tov_75", "pf_75", "fouls_drawn_75", "blocked_75", "fta_75",
            "fg3a_75", "fg3_rate", "ft_pct", "ts", "efg", "usg", "ast_pct",
            "oreb_pct", "dreb_pct", "pct_unassisted", "pct_pts3", "pct_paint",
            "pct_fb", "pct_ft", "pct_fga3", "mpg", "age", "height"]


def era_of(y: int) -> int:
    for i, (a, b) in enumerate(ERAS):
        if a <= y <= b:
            return i
    return len(ERAS) - 1


def fit_predict(df: pd.DataFrame) -> pd.DataFrame:
    """Era-bucketed weighted ridge, O and D separately.
    Adds columns: prior_o/prior_d/prior (in-sample-era fit, used for output)
    and loso_o/loso_d/loso (leave-one-season-out honest predictions)."""
    df = df.copy()
    Xall = df[FEATURES].to_numpy(dtype=float)
    # standardize globally (weighted)
    wq = df["fit_w"].to_numpy()
    mu = np.average(Xall, axis=0, weights=wq)
    sd = np.sqrt(np.average((Xall - mu) ** 2, axis=0, weights=wq)) + 1e-9
    Xall = (Xall - mu) / sd

    coefs = []
    for col_out, col_loso, target in [("prior_o", "loso_o", "orapm"),
                                      ("prior_d", "loso_d", "drapm")]:
        df[col_out] = np.nan
        df[col_loso] = np.nan
        for e in range(len(ERAS)):
            era_mask = df["era"].to_numpy() == e
            fit_mask = era_mask & df["fit_ok"].to_numpy()
            if fit_mask.sum() < 200:
                continue
            X = Xall[fit_mask]
            yv = df.loc[fit_mask, target].to_numpy()
            w = df.loc[fit_mask, "fit_w"].to_numpy()
            m = Ridge(alpha=RIDGE_ALPHA)
            m.fit(X, yv, sample_weight=w)
            df.loc[era_mask, col_out] = m.predict(Xall[era_mask])
            coefs.append(pd.DataFrame({
                "target": target, "era": f"{ERAS[e][0]}-{ERAS[e][1]}",
                "feature": FEATURES, "coef": m.coef_}))
            # leave-one-season-out
            for sy in sorted(df.loc[era_mask, "season_year"].unique()):
                tr = fit_mask & (df["season_year"].to_numpy() != sy)
                te = era_mask & (df["season_year"].to_numpy() == sy)
                if tr.sum() < 200 or not te.any():
                    continue
                m2 = Ridge(alpha=RIDGE_ALPHA)
                m2.fit(Xall[tr], df.loc[tr, target].to_numpy(),
                       sample_weight=df.loc[tr, "fit_w"].to_numpy())
                df.loc[te, col_loso] = m2.predict(Xall[te])
    df["prior"] = df["prior_o"] + df["prior_d"]
    df["loso"] = df["loso_o"] + df["loso_d"]
    return df, pd.concat(coefs, ignore_index=True)


def wcorr(a, b, w) -> float:
    a, b, w = np.asarray(a, float), np.asarray(b, float), np.asarray(w, float)
    am, bm = np.average(a, weights=w), np.average(b, weights=w)
    cov = np.average((a - am) * (b - bm), weights=w)
    return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                         * np.average((b - bm) ** 2, weights=w))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--features-only", action="store_true")
    args = ap.parse_args()

    feats = build_features()
    if args.features_only:
        return

    tgt = pd.read_parquet(TARGET_PATH)
    tgt = tgt[tgt["alpha"] == TARGET_ALPHA].copy()
    tgt["season_year"] = tgt["target_season"].str[:4].astype(int)
    df = feats.merge(tgt.rename(columns={"player_id": "pid"}),
                     on=["pid", "season_year"], how="inner")
    df["era"] = df["season_year"].map(era_of)
    df["fit_w"] = df["poss_season"].clip(lower=0)
    df["fit_ok"] = df["poss_season"] >= MIN_FIT_POSS
    print(f"Joined {len(df)} player-season rows ({int(df['fit_ok'].sum())} fit-eligible)")

    df, coefs = fit_predict(df)

    # ---- validation ------------------------------------------------------
    v = df[df["fit_ok"] & df["loso"].notna()]
    print("\nLeave-one-season-out (weighted corr with target):")
    print(f"  total: {wcorr(v['loso'], v['rapm'], v['fit_w']):.3f}   "
          f"O: {wcorr(v['loso_o'], v['orapm'], v['fit_w']):.3f}   "
          f"D: {wcorr(v['loso_d'], v['drapm'], v['fit_w']):.3f}   (n={len(v)})")
    for e, (a, b) in enumerate(ERAS):
        ve = v[v["era"] == e]
        if len(ve):
            print(f"  era {a}-{b}: total {wcorr(ve['loso'], ve['rapm'], ve['fit_w']):.3f} "
                  f"(n={len(ve)})")

    # strict temporal: train < 2019, predict 2019+ with one global-era model each era
    tr = df[df["fit_ok"] & (df["season_year"] < 2019)]
    te = df[df["fit_ok"] & (df["season_year"] >= 2019)]
    Xf = lambda d: d[FEATURES].to_numpy(dtype=float)
    mu = Xf(tr).mean(axis=0); sd = Xf(tr).std(axis=0) + 1e-9
    strict = {}
    for tcol in ("orapm", "drapm"):
        m = Ridge(alpha=RIDGE_ALPHA)
        m.fit((Xf(tr) - mu) / sd, tr[tcol], sample_weight=tr["fit_w"])
        strict[tcol] = m.predict((Xf(te) - mu) / sd)
    pred = strict["orapm"] + strict["drapm"]
    print(f"\nStrict temporal (train <2019, test 2019+): "
          f"total {wcorr(pred, te['rapm'], te['fit_w']):.3f} (n={len(te)})")

    # BPM baseline on the same held-out rows
    bb = []
    for p in sorted(BBREF_DIR.glob("advanced_*.csv")):
        yr = int(p.stem.split("_")[1])
        d = pd.read_csv(p)
        if "BPM" not in d.columns or "Player" not in d.columns:
            continue
        d = d[["Player", "BPM", "MP"]].copy()
        d["MP"] = pd.to_numeric(d["MP"], errors="coerce")
        d = d.sort_values("MP", ascending=False).drop_duplicates("Player")
        d["season_year"] = yr - 1
        bb.append(d)
    bb = pd.concat(bb, ignore_index=True)
    bb["BPM"] = pd.to_numeric(bb["BPM"], errors="coerce")
    vb = v.merge(bb, left_on=["player_name", "season_year"],
                 right_on=["Player", "season_year"]).dropna(subset=["BPM"])
    print(f"\nBaseline on matched LOSO rows (n={len(vb)}):")
    print(f"  our box prior vs target: {wcorr(vb['loso'], vb['rapm'], vb['fit_w']):.3f}")
    print(f"  BBRef BPM   vs target: {wcorr(vb['BPM'], vb['rapm'], vb['fit_w']):.3f}")

    # ---- outputs ---------------------------------------------------------
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_cols = ["pid", "player_name", "season_year", "target_season", "era",
                "games", "mins", "poss", "poss_season", "w_poss",
                "prior_o", "prior_d", "prior", "loso_o", "loso_d", "loso",
                "orapm", "drapm", "rapm"]
    df[out_cols].to_parquet(OUT_DIR / "box_prior.parquet", index=False)
    df[out_cols].to_csv(OUT_DIR / "box_prior.csv", index=False)
    coefs.to_csv(OUT_DIR / "box_prior_coefficients.csv", index=False)
    print(f"\nWrote {len(df)} rows to {OUT_DIR / 'box_prior.parquet'}")

    top = df[(df["season_year"] == 2025) & df["fit_ok"]].nlargest(12, "prior")
    print("\nTop 12 by box prior, 2025-26:")
    print(top[["player_name", "prior_o", "prior_d", "prior", "rapm"]]
          .round(2).to_string(index=False))


if __name__ == "__main__":
    main()
