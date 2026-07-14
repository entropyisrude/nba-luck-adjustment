"""
Tests a specific conditional-discount interaction (user hypothesis,
2026-07-14): is dreb_75 worth LESS to defensive value when a player also
has low blocks AND low assists -- i.e. does dreb_75's payoff depend on
having some complementary "defensive versatility/IQ" signal, or is a raw
rebounding number worth the same regardless of the rest of the profile?

Different from test_rim_archetype_interaction.py (2026-07-12), which tested
whether a NEW feature (rim_fg_allow) becomes more informative for a
high-dreb/low-blk archetype. This tests whether dreb_75's OWN coefficient
should be discounted for players low in blk_75 AND ast_75 -- a direct
interaction on the existing feature, not a new feature's conditional value.

low_skill = -(z(blk_75) + z(ast_75))/2, era-standardized -- HIGH values mean
LOW blocks AND LOW assists (the "pure compiler" archetype: rebounds without
complementary defensive-IQ signals). interaction = z(dreb_75) * low_skill.
A negative interaction coefficient would support the hypothesis (dreb_75
predicts less D value when blk+ast are both low).

Usage: python metric/test_dreb_skill_interaction.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_box_prior import (FEATURES_V1, TARGET_PATH, TARGET_ALPHA, MIN_FIT_POSS,
                             build_features, era_of, fit_predict, wcorr)

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
EVID_PATH = METRIC_DATA / "evidence_season.parquet"


def main() -> None:
    feats = build_features()
    tgt = pd.read_parquet(TARGET_PATH)
    tgt = tgt[tgt["alpha"] == TARGET_ALPHA].copy()
    tgt["season_year"] = tgt["target_season"].str[:4].astype(int)
    df = feats.merge(tgt.rename(columns={"player_id": "pid"}), on=["pid", "season_year"], how="inner")
    df["era"] = df["season_year"].map(era_of)
    df["fit_w"] = df["poss_season"].clip(lower=0)
    df["fit_ok"] = df["poss_season"] >= MIN_FIT_POSS

    # era-standardized z-scores (within fit-eligible population), same
    # convention as the archetype-interaction test
    df["z_dreb"] = np.nan
    df["z_blk"] = np.nan
    df["z_ast"] = np.nan
    for e in df["era"].unique():
        m = (df["era"] == e) & df["fit_ok"]
        w = df.loc[m, "fit_w"]
        for col, zcol in [("dreb_75", "z_dreb"), ("blk_75", "z_blk"), ("ast_75", "z_ast")]:
            mu = np.average(df.loc[m, col], weights=w)
            sd = np.sqrt(np.average((df.loc[m, col] - mu) ** 2, weights=w))
            all_e = df["era"] == e
            df.loc[all_e, zcol] = (df.loc[all_e, col] - mu) / sd

    df["low_skill"] = -(df["z_blk"] + df["z_ast"]) / 2
    df["dreb_x_lowskill"] = df["z_dreb"] * df["low_skill"]

    ev = pd.read_parquet(EVID_PATH)
    ev["prev_year"] = ev["season_year"] - 1

    VARIANTS = {
        "v1 baseline": FEATURES_V1,
        "v1 + interaction": FEATURES_V1 + ["dreb_x_lowskill"],
    }

    low_skill_thresh = df.loc[df["fit_ok"], "low_skill"].quantile(0.75)
    print(f"high-'low_skill' threshold (top quartile: low blk AND low ast): {low_skill_thresh:.2f}\n")

    print(f"{'variant':>20} {'LOSO-D':>7} {'nextD-all':>10} {'nextD-hiArch':>13}")
    for name, cols in VARIANTS.items():
        fit, coefs = fit_predict(df, features=cols)
        v = fit[fit["fit_ok"] & fit["loso"].notna()]
        loso_d = wcorr(v["loso_d"], v["drapm"], v["fit_w"])
        j = fit.merge(ev.rename(columns={"player_id": "pid"}), left_on=["pid", "season_year"],
                      right_on=["pid", "prev_year"], suffixes=("", "_ev"))
        j = j[(j["ev_poss"] >= 1000) & j["loso"].notna()]
        nxt_d = wcorr(j["loso_d"], j["ev_d"], j["ev_poss"])
        jh = j[j["low_skill"] >= low_skill_thresh]
        nxt_d_hi = wcorr(jh["loso_d"], jh["ev_d"], jh["ev_poss"]) if len(jh) > 50 else float("nan")
        print(f"{name:>20} {loso_d:7.4f} {nxt_d:10.4f} {nxt_d_hi:13.4f}   (n_hi={len(jh)})")

        if "dreb_x_lowskill" in cols:
            era_coefs = coefs[(coefs.target == "drapm") & (coefs.era == "2021-2025")]
            ic = era_coefs[era_coefs.feature == "dreb_x_lowskill"]["coef"]
            print(f"  interaction coefficient (2021-2025 era): {ic.iloc[0]:+.3f}  "
                  f"(negative = supports the hypothesis: dreb worth less when blk+ast both low)")

    ids = {"Sengun": 1630578, "Ayton": 1629028, "Vucevic": 202696}
    fit_base, _ = fit_predict(df, features=FEATURES_V1)
    fit_int, _ = fit_predict(df, features=FEATURES_V1 + ["dreb_x_lowskill"])
    print("\nprior_d under interaction model vs baseline, 2024-25:")
    for name, pid in ids.items():
        rb = fit_base[(fit_base.season_year == 2024) & (fit_base.pid == pid)]
        ri = fit_int[(fit_int.season_year == 2024) & (fit_int.pid == pid)]
        if len(rb) and len(ri):
            ls = df[(df.season_year == 2024) & (df.pid == pid)]["low_skill"].iloc[0]
            print(f"  {name}: baseline={rb['prior_d'].iloc[0]:.2f}  "
                  f"-> with interaction={ri['prior_d'].iloc[0]:.2f}  (low_skill z={ls:+.2f})")


if __name__ == "__main__":
    main()
