"""
Extends test_dreb_skill_interaction.py (2026-07-14): broadens the
"complementary defensive skill" signal from blocks+assists to
blocks+assists+deflections (a hustle stat already in the features cache,
tested as a direct feature in ablate_box_prior_features.py and found not
to help there -- this tests it as an INTERACTION conditioner instead of a
main effect, a different role for the same data).

Tests two functional forms per the user's own phrasing ("dreb are worth X
unless [...] below Y and then even dreb are worth X-1" -- a KINKED/
threshold shape, not necessarily smooth):
  1. smooth linear interaction: z(dreb_75) * low_activity
     (low_activity = -(z(blk_75)+z(ast_75)+z(defl_75))/3)
  2. threshold interaction: z(dreb_75) * I(low_activity >= top quartile)
     -- a hard discount only below some activity floor, not a graded one

Hustle stats (deflections) only exist 2016+; era 2021-2025 has ~98-99%
coverage, checked directly, not assumed.

Usage: python metric/test_dreb_activity_interaction.py
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


def zscore_by_era(df: pd.DataFrame, col: str, era_col: str, w_col: str, fit_col: str) -> pd.Series:
    out = pd.Series(np.nan, index=df.index)
    for e in df[era_col].unique():
        m = (df[era_col] == e) & df[fit_col]
        w = df.loc[m, w_col]
        mu = np.average(df.loc[m, col], weights=w)
        sd = np.sqrt(np.average((df.loc[m, col] - mu) ** 2, weights=w))
        all_e = df[era_col] == e
        out.loc[all_e] = (df.loc[all_e, col] - mu) / sd
    return out


def main() -> None:
    feats = build_features()
    tgt = pd.read_parquet(TARGET_PATH)
    tgt = tgt[tgt["alpha"] == TARGET_ALPHA].copy()
    tgt["season_year"] = tgt["target_season"].str[:4].astype(int)
    df = feats.merge(tgt.rename(columns={"player_id": "pid"}), on=["pid", "season_year"], how="inner")
    df["era"] = df["season_year"].map(era_of)
    df["fit_w"] = df["poss_season"].clip(lower=0)
    df["fit_ok"] = df["poss_season"] >= MIN_FIT_POSS
    df = df[df["defl_75"].notna()].copy()   # hustle-stat-covered rows only

    for col, zcol in [("dreb_75", "z_dreb"), ("blk_75", "z_blk"),
                      ("ast_75", "z_ast"), ("defl_75", "z_defl")]:
        df[zcol] = zscore_by_era(df, col, "era", "fit_w", "fit_ok")

    df["low_activity"] = -(df["z_blk"] + df["z_ast"] + df["z_defl"]) / 3
    df["dreb_x_lowactivity"] = df["z_dreb"] * df["low_activity"]

    thresh = df.loc[df["fit_ok"], "low_activity"].quantile(0.75)
    df["low_activity_hi"] = (df["low_activity"] >= thresh).astype(float)
    df["dreb_x_lowactivity_thresh"] = df["z_dreb"] * df["low_activity_hi"]

    ev = pd.read_parquet(EVID_PATH)
    ev["prev_year"] = ev["season_year"] - 1

    VARIANTS = {
        "v1 baseline": FEATURES_V1,
        "v1 + smooth interaction": FEATURES_V1 + ["dreb_x_lowactivity"],
        "v1 + threshold interaction": FEATURES_V1 + ["dreb_x_lowactivity_thresh"],
    }

    print(f"low_activity top-quartile threshold: {thresh:.2f} "
          f"(low blk+ast+defl combined)\n")
    print(f"{'variant':>28} {'LOSO-D':>7} {'nextD-all':>10} {'nextD-hiArch':>13}")
    for name, cols in VARIANTS.items():
        fit, coefs = fit_predict(df, features=cols)
        v = fit[fit["fit_ok"] & fit["loso"].notna()]
        loso_d = wcorr(v["loso_d"], v["drapm"], v["fit_w"])
        j = fit.merge(ev.rename(columns={"player_id": "pid"}), left_on=["pid", "season_year"],
                      right_on=["pid", "prev_year"], suffixes=("", "_ev"))
        j = j[(j["ev_poss"] >= 1000) & j["loso"].notna()]
        nxt_d = wcorr(j["loso_d"], j["ev_d"], j["ev_poss"])
        jh = j[j["low_activity"] >= thresh]
        nxt_d_hi = wcorr(jh["loso_d"], jh["ev_d"], jh["ev_poss"]) if len(jh) > 50 else float("nan")
        print(f"{name:>28} {loso_d:7.4f} {nxt_d:10.4f} {nxt_d_hi:13.4f}   (n_hi={len(jh)})")
        for feat_name in ("dreb_x_lowactivity", "dreb_x_lowactivity_thresh"):
            if feat_name in cols:
                era_coefs = coefs[(coefs.target == "drapm") & (coefs.era == "2021-2025")]
                ic = era_coefs[era_coefs.feature == feat_name]["coef"]
                if len(ic):
                    print(f"  {feat_name} coefficient: {ic.iloc[0]:+.3f}")

    ids = {"Sengun": 1630578, "Ayton": 1629028, "Vucevic": 202696,
           "Zubac": 1627826, "Turner": 1626167, "Capela": 203991}
    fit_base, _ = fit_predict(df, features=FEATURES_V1)
    fit_sm, _ = fit_predict(df, features=FEATURES_V1 + ["dreb_x_lowactivity"])
    print("\nprior_d under smooth-interaction model vs baseline, 2024-25:")
    for name, pid in ids.items():
        rb = fit_base[(fit_base.season_year == 2024) & (fit_base.pid == pid)]
        rs = fit_sm[(fit_sm.season_year == 2024) & (fit_sm.pid == pid)]
        la = df[(df.season_year == 2024) & (df.pid == pid)]["low_activity"]
        if len(rb) and len(rs) and len(la):
            print(f"  {name}: baseline={rb['prior_d'].iloc[0]:.2f}  "
                  f"-> smooth interaction={rs['prior_d'].iloc[0]:.2f}  (low_activity z={la.iloc[0]:+.2f})")


if __name__ == "__main__":
    main()
