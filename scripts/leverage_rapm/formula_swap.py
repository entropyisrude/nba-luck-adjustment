"""
The "player-BPM" counterfactual: hold each player's own career OFFENSIVE
box-rate profile fixed, and score it TWICE -- once with the coefficients
fit on regular-season data, once with the coefficients fit on playoff data
(both from run_stage2.py's ORAPM regression). The difference is what a
BPM-style formula built from playoff data would say about this exact player
compared to what a formula built from regular-season data would say -- not
his actual observed on/off value in either bucket.

Also decomposes each player's total delta into the piece attributable to
each individual predictor's coefficient difference, to size up specifically
how much of the swing is coming from 3PA-share vs. unassisted-FG% vs. the
others.

Output: prints distribution + top movers; writes data/leverage_rapm/formula_swap.csv
"""

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "leverage_rapm"

MIN_CAREER_MINUTES = 500.0
PREDICTORS = ["tsa_per36", "ast_per36", "ts_pct", "fg3a_share", "unassisted_fg_pct", "orb_per36"]


def build_career_profile() -> pd.DataFrame:
    box = pd.read_csv(DATA_DIR / "box_lines.csv")
    stat_cols = ["fga", "fgm", "fg3a", "fg3m", "fta", "ftm", "ast", "orb", "unassisted_fgm"]
    career = box.groupby("player_id")[stat_cols].sum().reset_index()

    rapm_po = pd.read_csv(DATA_DIR / "rapm_playoff.csv")[["player_id", "player_name", "team_abbr", "bucket_minutes"]]
    rapm_rs = pd.read_csv(DATA_DIR / "rapm_regular.csv")[["player_id", "player_name", "bucket_minutes"]]
    minutes = rapm_po.merge(rapm_rs, on="player_id", how="outer", suffixes=("_po", "_rs"))
    minutes["bucket_minutes_po"] = minutes["bucket_minutes_po"].fillna(0)
    minutes["bucket_minutes_rs"] = minutes["bucket_minutes_rs"].fillna(0)
    minutes["total_minutes"] = minutes["bucket_minutes_po"] + minutes["bucket_minutes_rs"]
    minutes["player_name"] = minutes["player_name_po"].fillna(minutes["player_name_rs"])

    df = career.merge(
        minutes[["player_id", "player_name", "team_abbr", "total_minutes", "bucket_minutes_po"]],
        on="player_id", how="inner",
    )
    df = df.rename(columns={"bucket_minutes_po": "playoff_minutes"})
    df = df[df["total_minutes"] >= MIN_CAREER_MINUTES].copy()

    df["pts"] = (df["fgm"] - df["fg3m"]) * 2 + df["fg3m"] * 3 + df["ftm"]
    df["ts_pct"] = df["pts"] / (2 * (df["fga"] + 0.44 * df["fta"])).replace(0, pd.NA)
    df["ts_pct"] = df["ts_pct"].fillna(0)
    df["unassisted_fg_pct"] = (df["unassisted_fgm"] / df["fgm"].replace(0, pd.NA)).fillna(0)
    df["fg3a_share"] = (df["fg3a"] / df["fga"].replace(0, pd.NA)).fillna(0)

    df["tsa_per36"] = (df["fga"] + 0.44 * df["fta"]) / df["total_minutes"] * 36
    df["ast_per36"] = df["ast"] / df["total_minutes"] * 36
    df["orb_per36"] = df["orb"] / df["total_minutes"] * 36
    return df


def load_formula(label: str) -> dict:
    comp = pd.read_csv(DATA_DIR / "stage2_comparison.csv")
    row = comp[comp["label"] == label].iloc[0]
    return {"intercept": row["intercept"], **{p: row[f"{p}_coef"] for p in PREDICTORS}}


def apply_formula(df: pd.DataFrame, formula: dict) -> pd.Series:
    pred = pd.Series(formula["intercept"], index=df.index)
    for p in PREDICTORS:
        pred = pred + df[p] * formula[p]
    return pred


def main():
    df = build_career_profile()
    po_formula = load_formula("playoff")
    rs_formula = load_formula("regular")

    df["predicted_playoff_formula"] = apply_formula(df, po_formula)
    df["predicted_regular_formula"] = apply_formula(df, rs_formula)
    df["delta"] = df["predicted_playoff_formula"] - df["predicted_regular_formula"]

    print(f"n players (>= {MIN_CAREER_MINUTES:.0f} career minutes): {len(df)}")
    print("\nDistribution of delta (predicted value if scored by the PLAYOFF formula minus the")
    print("REGULAR-SEASON formula, holding each player's own career stat line fixed -- units are")
    print("offensive RAPM points per 100 possessions:")
    print(df["delta"].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9]))

    print("\n--- Per-predictor contribution to delta (player_stat x coefficient_difference) ---")
    coef_diff = {p: po_formula[p] - rs_formula[p] for p in PREDICTORS}
    for p in PREDICTORS:
        contrib = df[p] * coef_diff[p]
        print(f"  {p:20s} coef_diff={coef_diff[p]:+.4f}  contribution mean={contrib.mean():+.3f}  "
              f"std={contrib.std():.3f}  range=[{contrib.min():+.2f}, {contrib.max():+.2f}]")
    intercept_diff = po_formula["intercept"] - rs_formula["intercept"]
    print(f"  {'(intercept)':20s} coef_diff={intercept_diff:+.4f}  (flat shift applied to every player)")

    print("\n--- Top 10 players whose OWN profile the PLAYOFF formula values most, relative to the regular-season formula ---")
    cols = ["player_name", "total_minutes", "fg3a_share", "unassisted_fg_pct", "delta"]
    print(df.sort_values("delta", ascending=False)[cols].head(10).to_string(index=False).encode("ascii", "replace").decode("ascii"))

    print("\n--- Top 10 whose OWN profile the PLAYOFF formula values LEAST, relative to the regular-season formula ---")
    print(df.sort_values("delta", ascending=True)[cols].head(10).to_string(index=False).encode("ascii", "replace").decode("ascii"))

    out_cols = ["player_id", "player_name", "team_abbr", "total_minutes", "playoff_minutes"] + PREDICTORS + [
        "predicted_playoff_formula", "predicted_regular_formula", "delta"
    ]
    df[out_cols].sort_values("delta", ascending=False).to_csv(DATA_DIR / "formula_swap.csv", index=False)
    print(f"\nWrote {DATA_DIR / 'formula_swap.csv'}")


if __name__ == "__main__":
    main()
