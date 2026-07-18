"""
Within-player paired test: does the SAME player's OFFENSIVE value (ORAPM)
shift toward a 3PA-share / unassisted-FG% profile more in the playoffs than
in the regular season, or was the cross-sectional Stage-2 finding just
picking up which players get more playoff/regular minutes in the first
place?

Restrict to players with enough minutes in BOTH playoff and regular season,
take delta = orapm_playoff - orapm_regular for each such player (this
differences out anything about the player that's constant across situations
-- e.g. their overall talent level -- leaving only how their situational
value moved). Then regress delta on each player's career OFFENSIVE box-rate
profile: if 3PA-share/unassisted-FG% still predict a positive delta here,
that's evidence of a genuine situational value shift, not just selection
into who plays more playoff minutes.

Output: prints coefficients; writes data/leverage_rapm/within_player_paired.csv
"""

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "leverage_rapm"

MIN_PAIR_MINUTES = 100.0
PREDICTORS = ["tsa_per36", "ast_per36", "ts_pct", "fg3a_share", "unassisted_fg_pct", "orb_per36"]


def build_career_profile() -> pd.DataFrame:
    box = pd.read_csv(DATA_DIR / "box_lines.csv")
    stat_cols = ["fga", "fgm", "fg3a", "fg3m", "fta", "ftm", "ast", "orb", "unassisted_fgm"]
    career = box.groupby("player_id")[stat_cols].sum().reset_index()

    minutes = None
    for label in ["playoff", "regular"]:
        m = pd.read_csv(DATA_DIR / f"rapm_{label}.csv")[["player_id", "bucket_minutes"]]
        m = m.rename(columns={"bucket_minutes": f"min_{label}"})
        minutes = m if minutes is None else minutes.merge(m, on="player_id", how="outer")
    minutes = minutes.fillna(0)
    minutes["total_minutes"] = minutes["min_playoff"] + minutes["min_regular"]
    career = career.merge(minutes[["player_id", "total_minutes"]], on="player_id", how="inner")
    career = career[career["total_minutes"] > 0]

    career["pts"] = (career["fgm"] - career["fg3m"]) * 2 + career["fg3m"] * 3 + career["ftm"]
    career["ts_pct"] = career["pts"] / (2 * (career["fga"] + 0.44 * career["fta"])).replace(0, pd.NA)
    career["ts_pct"] = career["ts_pct"].fillna(0)
    career["unassisted_fg_pct"] = (career["unassisted_fgm"] / career["fgm"].replace(0, pd.NA)).fillna(0)
    career["fg3a_share"] = (career["fg3a"] / career["fga"].replace(0, pd.NA)).fillna(0)

    career["tsa_per36"] = (career["fga"] + 0.44 * career["fta"]) / career["total_minutes"] * 36
    career["ast_per36"] = career["ast"] / career["total_minutes"] * 36
    career["orb_per36"] = career["orb"] / career["total_minutes"] * 36
    return career


def paired_test(career: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    po = pd.read_csv(DATA_DIR / "rapm_playoff.csv")[["player_id", "player_name", "bucket_minutes", "orapm"]]
    rs = pd.read_csv(DATA_DIR / "rapm_regular.csv")[["player_id", "bucket_minutes", "orapm"]]

    paired = po.merge(rs, on="player_id", suffixes=("_playoff", "_regular"))
    paired = paired[
        (paired["bucket_minutes_playoff"] >= MIN_PAIR_MINUTES)
        & (paired["bucket_minutes_regular"] >= MIN_PAIR_MINUTES)
    ].copy()

    paired["delta_orapm"] = paired["orapm_playoff"] - paired["orapm_regular"]
    # weight by the harmonic mean of the two minute totals -- a pair is only as
    # well-measured as its SMALLER side, so this penalizes lopsided pairs more
    # than a simple average would.
    m1, m2 = paired["bucket_minutes_playoff"], paired["bucket_minutes_regular"]
    paired["pair_weight"] = 2 * m1 * m2 / (m1 + m2)

    df = paired.merge(career[["player_id"] + PREDICTORS], on="player_id", how="inner")

    X = df[PREDICTORS].values
    y = df["delta_orapm"].values
    w = df["pair_weight"].values

    model = LinearRegression()
    model.fit(X, y, sample_weight=w)
    r2 = model.score(X, y, sample_weight=w)

    print(f"\n=== within-player: playoff minus regular ORAPM, n={len(df)} paired players "
          f"(>= {MIN_PAIR_MINUTES:.0f} min in both), weighted R^2={r2:.3f} ===")
    print(f"  intercept (avg unexplained shift): {model.intercept_:+.3f}")
    for name, coef in zip(PREDICTORS, model.coef_):
        print(f"  {name:20s} {coef:+.4f}")

    return {"n_pairs": len(df), "r2": r2, "intercept": model.intercept_,
            **dict(zip(PREDICTORS, model.coef_))}, df


def main():
    career = build_career_profile()
    result, df = paired_test(career)

    comparison = pd.DataFrame([result])
    print("\n--- Within-player result ---")
    print(comparison.to_string(index=False))

    df.to_csv(DATA_DIR / "within_player_paired.csv", index=False)
    comparison.to_csv(DATA_DIR / "within_player_comparison.csv", index=False)
    print(f"\nWrote {DATA_DIR / 'within_player_paired.csv'} and within_player_comparison.csv")


if __name__ == "__main__":
    main()
