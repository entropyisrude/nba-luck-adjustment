"""
Fit the box prior separately against first-chance-only and second-chance-
only defensive RAPM targets (2026-07-14 fix for the dreb_75/center-defense
bias found via metric/test_firstchance_rapm.py and generalized in
metric/build_rapm_target_fcsc.py).

Only era 2021-2025 is fit (the only era the FC/SC targets cover -- see
build_rapm_target_fcsc.py's MIN_TARGET_SEASON). Reuses build_box_prior.py's
FEATURES_V1 and fit_predict() machinery unchanged -- fit_predict() reads
whatever "orapm"/"drapm" columns are on the joined dataframe, so swapping in
the FC or SC target's own orapm/drapm (same column names, same schema as
the standard target) is enough; no changes needed there.

Validation (the actual point of this script): does prior_fc_d + prior_sc_d
predict NEXT-SEASON defensive evidence better than the single, currently-
shipped prior_d? Same honest fixed-target protocol used throughout this
project. Only worth wiring into Phase 3 if this wins or ties.

RESULT (2026-07-14): NEGATIVE -- NOT SHIPPED. prior_fc_d + prior_sc_d scores
r=0.5425 on next-season D transfer vs the shipped prior_d's r=0.5426 --
statistically a wash, and it stays a wash among high-dreb_75 bigs
specifically (0.5356 vs 0.5371, if anything slightly worse). A full weighted
blend grid (w*fc + (1-w)*sc, and separately standard vs fc+sc at every
mixing weight) never beats the standard target at any weight -- see
test_fcsc_blend results in git history / project memory. The diagnosis is
still correct (dreb_75's correlation with the target runs almost entirely
through second-chance points, confirmed via test_firstchance_rapm.py and
replicated here in the multi-year target: r=0.438 standard -> r=0.163
first-chance-only, r=0.376 second-chance-only) -- what doesn't work is THIS
fix (decompose the target, refit two separate box priors, sum). Most likely
explanation: each sub-target is individually much noisier in-sample
(LOSO-D 0.67 standard -> 0.40 fc-only / 0.37 sc-only) and summing two
noisier estimates recovers the same net predictive power as one estimate
that (correctly, per the earlier finding) over-generalizes a narrow-but-
real effect -- the "wrong" generalization and the added estimation noise
roughly cancel out. A more surgical fix (e.g. orthogonalizing dreb_75
against the SC target before it's allowed into the FC model, rather than
two fully independent regressions) might do better but hasn't been tried.

Usage: python metric/build_box_prior_fcsc.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_box_prior import FEATURES_V1, MIN_FIT_POSS, build_features, fit_predict, wcorr

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
TARGETS_DIR = METRIC_DATA / "targets"
EVID_PATH = METRIC_DATA / "evidence_season.parquet"
STD_TARGET = TARGETS_DIR / "rapm_target_hl550.parquet"
FC_TARGET = TARGETS_DIR / "rapm_target_firstchance.parquet"
SC_TARGET = TARGETS_DIR / "rapm_target_secondchance.parquet"
OUT = METRIC_DATA / "priors" / "box_prior_fcsc.parquet"

ERA_ONLY = 4   # 2021-2025, the only era the FC/SC targets cover
MIN_SEASON = 2021


def load_target(path: Path, alpha: int | None = None) -> pd.DataFrame:
    t = pd.read_parquet(path)
    if alpha is not None:
        t = t[t["alpha"] == alpha].copy()
    t["season_year"] = t["target_season"].str[:4].astype(int)
    return t


def fit_one(feats: pd.DataFrame, tgt: pd.DataFrame, label: str) -> pd.DataFrame:
    df = feats.merge(tgt.rename(columns={"player_id": "pid"}), on=["pid", "season_year"], how="inner")
    df["era"] = ERA_ONLY   # force single-era fit regardless of era_of()'s own bucketing
    df["fit_w"] = df["poss_season"].clip(lower=0)
    df["fit_ok"] = df["poss_season"] >= MIN_FIT_POSS
    fit, _ = fit_predict(df, features=FEATURES_V1)
    v = fit[fit["fit_ok"] & fit["loso"].notna()]
    print(f"  {label}: LOSO total={wcorr(v['loso'], v['rapm'], v['fit_w']):.4f} "
          f"O={wcorr(v['loso_o'], v['orapm'], v['fit_w']):.4f} "
          f"D={wcorr(v['loso_d'], v['drapm'], v['fit_w']):.4f}  (n={len(v)})")
    return fit


def main() -> None:
    feats = build_features()
    feats = feats[feats["season_year"] >= MIN_SEASON]

    print("Fitting box priors (era 2021-2025 only):")
    std = load_target(STD_TARGET, alpha=500)
    fit_std = fit_one(feats, std, "standard (shipped) target")
    fc = load_target(FC_TARGET)
    fit_fc = fit_one(feats, fc, "first-chance-only target")
    sc = load_target(SC_TARGET)
    fit_sc = fit_one(feats, sc, "second-chance-only target")

    # honest next-season transfer test: does prior_fc_d + prior_sc_d predict
    # NEXT season's defensive evidence better than the single prior_d?
    ev = pd.read_parquet(EVID_PATH)
    ev["prev_year"] = ev["season_year"] - 1

    combo = fit_fc[["pid", "season_year", "loso_d"]].rename(columns={"loso_d": "loso_fc_d"}).merge(
        fit_sc[["pid", "season_year", "loso_d"]].rename(columns={"loso_d": "loso_sc_d"}),
        on=["pid", "season_year"])
    combo["loso_combo_d"] = combo["loso_fc_d"] + combo["loso_sc_d"]
    combo = combo.merge(fit_std[["pid", "season_year", "loso_d", "fit_w", "fit_ok"]],
                        on=["pid", "season_year"])
    combo = combo[combo["fit_ok"]]

    j = combo.merge(ev.rename(columns={"player_id": "pid"}), left_on=["pid", "season_year"],
                    right_on=["pid", "prev_year"], suffixes=("", "_ev"))
    j = j[j["ev_poss"] >= 1000].dropna(subset=["loso_d", "loso_combo_d", "ev_d"])

    print(f"\nHonest next-season D transfer test (n={len(j)}):")
    print(f"  current shipped prior_d alone:  r={wcorr(j['loso_d'], j['ev_d'], j['ev_poss']):.4f}")
    print(f"  prior_fc_d + prior_sc_d:        r={wcorr(j['loso_combo_d'], j['ev_d'], j['ev_poss']):.4f}")

    # also check specifically among BIGS (high dreb_75/height) -- this is
    # exactly the subgroup the whole investigation is about
    feats_cur = feats[["pid", "season_year", "dreb_75", "height"]]
    jb = j.merge(feats_cur, on=["pid", "season_year"])
    arch = jb["dreb_75"] >= jb["dreb_75"].quantile(0.75)
    jbig = jb[arch]
    print(f"\n  among high-dreb_75 bigs only (n={len(jbig)}):")
    print(f"    current shipped prior_d alone:  r={wcorr(jbig['loso_d'], jbig['ev_d'], jbig['ev_poss']):.4f}")
    print(f"    prior_fc_d + prior_sc_d:        r={wcorr(jbig['loso_combo_d'], jbig['ev_d'], jbig['ev_poss']):.4f}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out = fit_fc[["pid", "player_name", "season_year", "prior_d", "prior_o"]].rename(
        columns={"prior_d": "prior_fc_d", "prior_o": "prior_fc_o"})
    out = out.merge(fit_sc[["pid", "season_year", "prior_d", "prior_o"]].rename(
        columns={"prior_d": "prior_sc_d", "prior_o": "prior_sc_o"}), on=["pid", "season_year"])
    out.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
