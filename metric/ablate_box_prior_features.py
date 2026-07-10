"""Ablation: which v2 box-prior feature groups actually TRANSFER?

The v2 feature expansion lifted same-season LOSO sharply (0.708 -> 0.743)
but left the honest next-season scoreboards flat-to-down (metric v0
posterior 0.526 -> 0.522; Kalman one-step 0.547 -> 0.5445). This script
scores each feature group directly at the prior level on both boards:

  * LOSO weighted corr with the (same-season) multi-year target,
  * LOSO prior for season t vs season t+1 single-season evidence
    (the transfer test — cached evidence, no RAPM solves needed).

Usage: python metric/ablate_box_prior_features.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_box_prior import (FEATURES_V1, FEATURES_V2_EXTRA, TARGET_PATH,
                             TARGET_ALPHA, MIN_FIT_POSS,
                             build_features, era_of, fit_predict, wcorr)

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
EVID_PATH = METRIC_DATA / "evidence_season.parquet"

V1 = FEATURES_V1
MIX = ["pct_ast2", "pct_ast3", "pct_mid", "stl_share", "blk_share",
       "fd_share", "tov_pct", "pts_offtov_75", "pts_2nd_75"]
CONTEXT = ["pace", "opp_paint_75", "opp_fb_75", "opp_offtov_75", "opp_2nd_75"]
SKILL = ["defl_75", "contest_75", "chg_75", "screen_75", "loose_75",
         "boxout_75", "wing_rel"]

VARIANTS = {
    "v1 baseline": V1,
    "v1 + hustle/wingspan": V1 + SKILL,
    "v1 + mix/shares": V1 + MIX,
    "v1 + on-court context": V1 + CONTEXT,
    "v1 + skill + mix": V1 + SKILL + MIX,
    "full v2": V1 + FEATURES_V2_EXTRA,
}


def main() -> None:
    feats = build_features()
    tgt = pd.read_parquet(TARGET_PATH)
    tgt = tgt[tgt["alpha"] == TARGET_ALPHA].copy()
    tgt["season_year"] = tgt["target_season"].str[:4].astype(int)
    df = feats.merge(tgt.rename(columns={"player_id": "pid"}),
                     on=["pid", "season_year"], how="inner")
    df["era"] = df["season_year"].map(era_of)
    df["fit_w"] = df["poss_season"].clip(lower=0)
    df["fit_ok"] = df["poss_season"] >= MIN_FIT_POSS

    ev = pd.read_parquet(EVID_PATH)
    ev["prev_year"] = ev["season_year"] - 1

    print(f"{'variant':>22}  {'LOSO':>6} {'O':>6} {'D':>6}   "
          f"{'next-ssn':>8} {'O':>6} {'D':>6}")
    for name, cols in VARIANTS.items():
        fit, _ = fit_predict(df, features=cols)
        v = fit[fit["fit_ok"] & fit["loso"].notna()]
        loso = wcorr(v["loso"], v["rapm"], v["fit_w"])
        loso_o = wcorr(v["loso_o"], v["orapm"], v["fit_w"])
        loso_d = wcorr(v["loso_d"], v["drapm"], v["fit_w"])
        j = fit.merge(ev.rename(columns={"player_id": "pid"}),
                      left_on=["pid", "season_year"],
                      right_on=["pid", "prev_year"], suffixes=("", "_ev"))
        j = j[(j["ev_poss"] >= 1000) & j["loso"].notna()]
        nxt = wcorr(j["loso"], j["ev_o"] + j["ev_d"], j["ev_poss"])
        nxt_o = wcorr(j["loso_o"], j["ev_o"], j["ev_poss"])
        nxt_d = wcorr(j["loso_d"], j["ev_d"], j["ev_poss"])
        print(f"{name:>22}  {loso:6.4f} {loso_o:6.4f} {loso_d:6.4f}   "
              f"{nxt:8.4f} {nxt_o:6.4f} {nxt_d:6.4f}  (n={len(v)}/{len(j)})")


if __name__ == "__main__":
    main()
