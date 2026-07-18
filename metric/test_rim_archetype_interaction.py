"""Does rim defense matter MORE for a specific archetype (high defensive
rebounding, low shot-blocking -- the "compiler big" profile) even though it
showed no incremental value on AVERAGE across all players
(metric/build_rim_defense.py + the 2026-07-12 box-prior ablation)?

Archetype score = z(dreb_75) - z(blk_75), standardized within era (same
era buckets as build_box_prior). High score = lots of boards, few blocks --
exactly the Vucevic/Valanciunas/Kanter-style profile the rim_fg_allow
correlation analysis showed is NOT reliably predicted by rebounding alone
(raw corr dreb_75 x rim_fg_allow = -0.065, essentially zero).

rim_fg_allow is centered on its era mean before interacting, so the
interaction term isn't just re-expressing archetype's own main effect
(dreb_75/blk_75 already have their own coefficients in FEATURES_V1).

Tests, via the same fixed-target protocol used throughout:
  1. same-season LOSO-D fit
  2. honest next-season D transfer, WHOLE population
  3. honest next-season D transfer, HIGH-ARCHETYPE SUBGROUP ONLY (top
     quartile of archetype score) -- the subgroup the hypothesis is
     actually about; an aggregate null doesn't rule out a concentrated
     effect here.

Usage: python metric/test_rim_archetype_interaction.py
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
    rim = pd.read_parquet(METRIC_DATA / "rim_defense_season.parquet")[
        ["pid", "season_year", "rim_att_75", "rim_fg_allow"]]
    feats = feats.merge(rim, on=["pid", "season_year"], how="left")
    feats["era_tmp"] = feats.season_year.map(era_of)
    for c in ["rim_att_75", "rim_fg_allow"]:
        feats[c] = feats[c].fillna(feats.groupby("era_tmp")[c].transform("mean"))
        feats[c] = feats[c].fillna(feats[c].mean())

    tgt = pd.read_parquet(TARGET_PATH)
    tgt = tgt[tgt["alpha"] == TARGET_ALPHA].copy()
    tgt["season_year"] = tgt["target_season"].str[:4].astype(int)
    df = feats.merge(tgt.rename(columns={"player_id": "pid"}), on=["pid", "season_year"], how="inner")
    df["era"] = df["season_year"].map(era_of)
    df["fit_w"] = df["poss_season"].clip(lower=0)
    df["fit_ok"] = df["poss_season"] >= MIN_FIT_POSS

    # archetype score + centered rim_fg_allow, computed within fit-eligible pop per era
    df["archetype"] = np.nan
    df["rim_fg_c"] = np.nan
    for e in df["era"].unique():
        m = (df["era"] == e) & df["fit_ok"]
        w = df.loc[m, "fit_w"]
        for c, out in [("dreb_75", None), ("blk_75", None)]:
            pass
        mu_dreb = np.average(df.loc[m, "dreb_75"], weights=w)
        sd_dreb = np.sqrt(np.average((df.loc[m, "dreb_75"] - mu_dreb) ** 2, weights=w))
        mu_blk = np.average(df.loc[m, "blk_75"], weights=w)
        sd_blk = np.sqrt(np.average((df.loc[m, "blk_75"] - mu_blk) ** 2, weights=w))
        mu_rim = np.average(df.loc[m, "rim_fg_allow"], weights=w)
        all_e = df["era"] == e
        df.loc[all_e, "archetype"] = ((df.loc[all_e, "dreb_75"] - mu_dreb) / sd_dreb
                                      - (df.loc[all_e, "blk_75"] - mu_blk) / sd_blk)
        df.loc[all_e, "rim_fg_c"] = df.loc[all_e, "rim_fg_allow"] - mu_rim
    df["rim_interact"] = df["archetype"] * df["rim_fg_c"]

    ev = pd.read_parquet(EVID_PATH)
    ev["prev_year"] = ev["season_year"] - 1

    VARIANTS = {
        "v1 baseline": FEATURES_V1,
        "v1 + rim_fg_c (main)": FEATURES_V1 + ["rim_fg_c"],
        "v1 + interaction only": FEATURES_V1 + ["rim_interact"],
        "v1 + main + interaction": FEATURES_V1 + ["rim_fg_c", "rim_interact"],
    }

    arch_thresh = df.loc[df["fit_ok"], "archetype"].quantile(0.75)
    print(f"high-archetype threshold (top quartile): {arch_thresh:.2f}\n")

    print(f"{'variant':>24} {'LOSO-D':>7} {'nextD-all':>10} {'nextD-hiArch':>13}")
    for name, cols in VARIANTS.items():
        fit, _ = fit_predict(df, features=cols)
        v = fit[fit["fit_ok"] & fit["loso"].notna()]
        loso_d = wcorr(v["loso_d"], v["drapm"], v["fit_w"])
        j = fit.merge(ev.rename(columns={"player_id": "pid"}), left_on=["pid", "season_year"],
                      right_on=["pid", "prev_year"], suffixes=("", "_ev"))
        j = j[(j["ev_poss"] >= 1000) & j["loso"].notna()]
        nxt_d = wcorr(j["loso_d"], j["ev_d"], j["ev_poss"])
        jh = j[j["archetype"] >= arch_thresh]
        nxt_d_hi = wcorr(jh["loso_d"], jh["ev_d"], jh["ev_poss"]) if len(jh) > 50 else float("nan")
        print(f"{name:>24} {loso_d:7.4f} {nxt_d:10.4f} {nxt_d_hi:13.4f}   (n_hi={len(jh)})")

    print("\nWhere do Sengun/Ayton/Vucevic sit on the archetype axis (2024-25)?")
    ids = {"Sengun": 1630578, "Ayton": 1629028, "Vucevic": 202696}
    row2024 = df[df["season_year"] == 2024]
    for name, pid in ids.items():
        r = row2024[row2024["pid"] == pid]
        if len(r):
            pctile = (row2024.loc[row2024["fit_ok"], "archetype"] < r["archetype"].iloc[0]).mean() * 100
            print(f"  {name}: archetype={r['archetype'].iloc[0]:+.2f} (pctile {pctile:.0f})  "
                  f"rim_fg_c={r['rim_fg_c'].iloc[0]:+.3f}")

    fit_final, _ = fit_predict(df, features=VARIANTS["v1 + main + interaction"])
    print("\nprior_d under 'main + interaction' model vs baseline, 2024-25:")
    fit_base, _ = fit_predict(df, features=VARIANTS["v1 baseline"])
    for name, pid in ids.items():
        rb = fit_base[(fit_base.season_year == 2024) & (fit_base.pid == pid)]
        rf = fit_final[(fit_final.season_year == 2024) & (fit_final.pid == pid)]
        if len(rb) and len(rf):
            print(f"  {name}: baseline prior_d={rb['prior_d'].iloc[0]:.2f}  "
                  f"-> with interaction={rf['prior_d'].iloc[0]:.2f}")


if __name__ == "__main__":
    main()
