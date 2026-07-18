"""
Stage 2: regress each bucket's Stage-1 OFFENSIVE RAPM (ORAPM -- points per
100 possessions on offense, adjusted) on career-aggregated OFFENSIVE box
rate stats, separately for playoff and regular, and compare coefficients.

Offense-only target and predictors: without reliable steals/blocks, DRB was
the only defensive signal left, and it's not enough to say anything
meaningful about defense -- so this drops the defensive side of RAPM
entirely and uses ORAPM as the target, and swaps REB/36 for ORB/36 (offense
only) in the predictor set.

Predictor design note: PTS/36 is mechanically ~2 x true-shot-attempts x TS%,
so including raw PTS/36 alongside TS% and 3PA/36 double-counts the same
scoring signal through overlapping predictors (multicollinearity). Instead:
  - TSA/36 (true shot attempts = FGA + 0.44*FTA, per 36) captures scoring
    VOLUME, decoupled from...
  - TS%, which captures scoring EFFICIENCY on those attempts.
  - 3PA_share (3PA / FGA) captures shot-mix (how many of a player's shots
    are threes), decoupled from how many shots they take overall (which
    3PA/36 would conflate with usage/minutes already captured by TSA/36).
AST/36, ORB/36 and unassisted FG% are kept as rates -- they're distinct
skill dimensions, not restatements of the scoring block.

Weighted by bucket minutes (more of a player's bucket-minutes = more
reliable box rates), with a minimum-minutes floor so tiny samples don't
inject noise as if it were signal.

Output: data/leverage_rapm/stage2_comparison.csv
"""

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "leverage_rapm"

MIN_BUCKET_MINUTES = 100.0

PREDICTORS = ["tsa_per36", "ast_per36", "ts_pct", "fg3a_share", "unassisted_fg_pct", "orb_per36"]


def load_bucket(label: str) -> pd.DataFrame:
    rapm = pd.read_csv(DATA_DIR / f"rapm_{label}.csv")
    box = pd.read_csv(DATA_DIR / "box_lines.csv")
    box = box[box["bucket"] == label]

    df = rapm.merge(box, on="player_id", how="inner")
    df = df[df["bucket_minutes"] >= MIN_BUCKET_MINUTES].copy()

    df["ts_pct"] = df["ts_pct"].fillna(0)
    df["unassisted_fg_pct"] = df["unassisted_fg_pct"].fillna(0)
    df["fg3a_share"] = (df["fg3a"] / df["fga"].replace(0, pd.NA)).fillna(0)

    df["ast_per36"] = df["ast"] / df["bucket_minutes"] * 36
    df["orb_per36"] = df["orb"] / df["bucket_minutes"] * 36
    df["tsa_per36"] = (df["fga"] + 0.44 * df["fta"]) / df["bucket_minutes"] * 36
    return df


def weighted_vif(df: pd.DataFrame, predictors: list, weights: np.ndarray) -> pd.Series:
    """VIF_i = 1 / (1 - R_i^2), where R_i^2 comes from regressing predictor i
    on all other predictors (weighted). VIF > ~5-10 flags real multicollinearity."""
    vifs = {}
    for i, name in enumerate(predictors):
        others = [p for p in predictors if p != name]
        X = df[others].values
        y = df[name].values
        model = LinearRegression().fit(X, y, sample_weight=weights)
        r2 = model.score(X, y, sample_weight=weights)
        vifs[name] = 1.0 / (1.0 - r2) if r2 < 0.999 else np.inf
    return pd.Series(vifs)


def fit(df: pd.DataFrame, label: str) -> dict:
    X = df[PREDICTORS].values
    y = df["orapm"].values
    w = df["bucket_minutes"].values

    model = LinearRegression()
    model.fit(X, y, sample_weight=w)
    r2 = model.score(X, y, sample_weight=w)
    vif = weighted_vif(df, PREDICTORS, w)

    print(f"\n=== {label}: n={len(df)} players (>= {MIN_BUCKET_MINUTES:.0f} bucket minutes), weighted R^2={r2:.3f} ===")
    print(f"  intercept: {model.intercept_:+.3f}")
    for name, coef in zip(PREDICTORS, model.coef_):
        print(f"  {name:20s} coef={coef:+.4f}   VIF={vif[name]:6.2f}")

    return {"label": label, "n_players": len(df), "r2": r2, "intercept": model.intercept_,
            **{f"{p}_coef": c for p, c in zip(PREDICTORS, model.coef_)},
            **{f"{p}_vif": vif[p] for p in PREDICTORS}}


def main():
    results = [fit(load_bucket(label), label) for label in ["playoff", "regular"]]

    comparison = pd.DataFrame(results)
    out_path = DATA_DIR / "stage2_comparison.csv"
    comparison.to_csv(out_path, index=False)
    print(f"\nWrote {out_path}")
    print(comparison.to_string(index=False))


if __name__ == "__main__":
    main()
