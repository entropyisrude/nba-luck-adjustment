"""Box prior v3: atomic features + SE-based observation weights.

Fits the locked 35-atom feature set (metric/build_atomic_features.py)
against the multiyear luck-adjusted RAPM target, with observation weights
1/se_o^2 (offense fit) and 1/se_d^2 (defense fit) from the target's
bootstrap-calibrated standard errors, and NO minimum-possession cutoff
(low-information rows are downweighted by their SEs instead of dropped —
removes the documented fringe extrapolation bias of the 1000-poss knife).

Produces a 2x2 scoreboard decomposing feature-set effect vs weighting
effect: {FEATURES_V1, atomic v3} x {old weighting (poss_season, >=1000
cutoff), new weighting (1/se^2, no cutoff)}. The SCORING protocol is held
fixed across cells (LOSO wcorr on poss>=1000 rows weighted by poss;
next-season transfer vs single-season evidence, ev_poss>=1000) so cells
are comparable; only the FIT changes. Baselines to reproduce in the
v1+old cell: LOSO 0.7054, transfer 0.4856.

Deliberately does NOT touch build_box_prior.py or anything site-facing.

Outputs: nba-metric-data/priors/box_prior_v3_coefficients.csv
Usage:   set PYTHONIOENCODING=utf-8 & python metric/build_box_prior_v3.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_box_prior import FEATURES_V1, era_of, ERAS, wcorr
from build_atomic_features import ATOMS

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
TARGET = METRIC_DATA / "targets" / "rapm_target_hl550.parquet"
V1_CACHE = METRIC_DATA / "features_box_season.parquet"
V3_CACHE = METRIC_DATA / "features_atomic_season.parquet"
EVID = METRIC_DATA / "evidence_season.parquet"
OUT_COEF = METRIC_DATA / "priors" / "box_prior_v3_coefficients.csv"

TARGET_ALPHA = 500
RIDGE_ALPHA = 50.0
MIN_FIT_POSS = 1000     # old-scheme cutoff + fixed scoring population


def fit_predict_w(df: pd.DataFrame, features: list[str],
                  w_o: np.ndarray, w_d: np.ndarray,
                  fit_mask: np.ndarray):
    """Era-bucketed weighted ridge with SEPARATE O/D observation weights.
    Returns df with prior_o/d, loso_o/d columns + coefficient frame."""
    df = df.copy()
    for c in features:
        if df[c].isna().any():
            df[c] = df[c].fillna(df.groupby("era")[c].transform("mean"))
            df[c] = df[c].fillna(df[c].mean())
    Xall = df[features].to_numpy(dtype=float)
    wq = np.where(fit_mask, w_o + w_d, 0.0)
    mu = np.average(Xall, axis=0, weights=np.maximum(wq, 1e-12))
    sd = np.sqrt(np.average((Xall - mu) ** 2, axis=0,
                            weights=np.maximum(wq, 1e-12))) + 1e-9
    Xall = (Xall - mu) / sd

    coefs = []
    era_arr = df["era"].to_numpy()
    sy_arr = df["season_year"].to_numpy()
    for col_out, col_loso, target, w in [
            ("prior_o", "loso_o", "orapm", w_o),
            ("prior_d", "loso_d", "drapm", w_d)]:
        df[col_out] = np.nan
        df[col_loso] = np.nan
        yv_all = df[target].to_numpy()
        for e in range(len(ERAS)):
            era_mask = era_arr == e
            fm = era_mask & fit_mask & (w > 0)
            if fm.sum() < 200:
                continue
            m = Ridge(alpha=RIDGE_ALPHA)
            m.fit(Xall[fm], yv_all[fm], sample_weight=w[fm])
            df.loc[era_mask, col_out] = m.predict(Xall[era_mask])
            coefs.append(pd.DataFrame({
                "target": target, "era": f"{ERAS[e][0]}-{ERAS[e][1]}",
                "feature": features, "coef": m.coef_}))
            for sy in np.unique(sy_arr[era_mask]):
                tr = fm & (sy_arr != sy)
                te = era_mask & (sy_arr == sy)
                if tr.sum() < 200 or not te.any():
                    continue
                m2 = Ridge(alpha=RIDGE_ALPHA)
                m2.fit(Xall[tr], yv_all[tr], sample_weight=w[tr])
                df.loc[te, col_loso] = m2.predict(Xall[te])
    df["loso"] = df["loso_o"] + df["loso_d"]
    return df, (pd.concat(coefs, ignore_index=True) if coefs else None)


def _wsd(a, w) -> float:
    a, w = np.asarray(a, float), np.asarray(w, float)
    m = np.average(a, weights=w)
    return float(np.sqrt(np.average((a - m) ** 2, weights=w)))


def score(df: pd.DataFrame, ev: pd.DataFrame, protocol: str = "old") -> dict:
    """Scoring protocols:
    'old' — poss>=1000 rows, poss-weighted (the v1 fit's home turf);
    'se'  — all SE rows, 1/(se_o^2+se_d^2)-weighted (the se fit's turf).
    Each also reports a CALIBRATED-SUM total: prediction components
    rescaled to the target components' spreads before summing, so the
    total can't be dragged by a components-scale artifact."""
    if protocol == "old":
        v = df[(df["poss_season"] >= MIN_FIT_POSS) & df["loso"].notna()]
        wv = v["poss_season"]
    else:
        v = df[df["se_o"].notna() & df["se_d"].notna() & df["loso"].notna()]
        wv = 1.0 / (v["se_o"] ** 2 + v["se_d"] ** 2)
    cal = (v["loso_o"] / _wsd(v["loso_o"], wv) * _wsd(v["orapm"], wv)
           + v["loso_d"] / _wsd(v["loso_d"], wv) * _wsd(v["drapm"], wv))
    out = {
        "loso": wcorr(v["loso"], v["rapm"], wv),
        "loso_cal": wcorr(cal, v["rapm"], wv),
        "loso_o": wcorr(v["loso_o"], v["orapm"], wv),
        "loso_d": wcorr(v["loso_d"], v["drapm"], wv),
        "n_loso": len(v),
    }
    j = df.merge(ev.rename(columns={"player_id": "pid"}),
                 left_on=["pid", "season_year"],
                 right_on=["pid", "prev_year"], suffixes=("", "_ev"))
    j = j[(j["ev_poss"] >= 1000) & j["loso"].notna()]
    if protocol == "se":
        j = j[j["se_o"].notna() & j["se_d"].notna()]
        wj = 1.0 / (j["se_o"] ** 2 + j["se_d"] ** 2)
    else:
        wj = j["ev_poss"]
    calj = (j["loso_o"] / _wsd(j["loso_o"], wj) * _wsd(j["ev_o"], wj)
            + j["loso_d"] / _wsd(j["loso_d"], wj) * _wsd(j["ev_d"], wj))
    out["nxt"] = wcorr(j["loso"], j["ev_o"] + j["ev_d"], wj)
    out["nxt_cal"] = wcorr(calj, j["ev_o"] + j["ev_d"], wj)
    out["nxt_o"] = wcorr(j["loso_o"], j["ev_o"], wj)
    out["nxt_d"] = wcorr(j["loso_d"], j["ev_d"], wj)
    out["n_nxt"] = len(j)
    return out


def main() -> None:
    global MIN_FIT_POSS
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default=str(TARGET),
                    help="alternate RAPM target parquet")
    ap.add_argument("--min-poss", type=float, default=MIN_FIT_POSS,
                    help="fit/scoring possession floor (new possession "
                         "targets count both sides: use ~1700 to match "
                         "the old 1000)")
    args = ap.parse_args()
    MIN_FIT_POSS = args.min_poss

    enc = sys.stdout.encoding or "utf-8"
    def p(s: str) -> None:
        print(s.encode(enc, errors="replace").decode(enc), flush=True)

    p(f"target: {args.target}  (min_poss {MIN_FIT_POSS})")
    tgt = pd.read_parquet(args.target)
    tgt = tgt[tgt["alpha"] == TARGET_ALPHA].copy()
    tgt["season_year"] = tgt["target_season"].str[:4].astype(int)
    tgt = tgt.rename(columns={"player_id": "pid"})

    v1 = pd.read_parquet(V1_CACHE)
    v3 = pd.read_parquet(V3_CACHE)
    v3 = v3.drop(columns=[c for c in ("games", "mins", "poss")
                          if c in v3.columns])
    both = v1.merge(v3, on=["pid", "season_year"], how="inner",
                    suffixes=("", "_v3"))
    # bio atoms exist in both caches -> the v3 copy is suffixed; alias back
    for c in ("height", "age", "wing_rel"):
        if c + "_v3" in both.columns:
            both = both.drop(columns=[c + "_v3"])
    df = both.merge(tgt, on=["pid", "season_year"], how="inner")
    df["era"] = df["season_year"].map(era_of)
    df = df[df["poss_season"] > 0].reset_index(drop=True)
    p(f"joined {len(df)} player-season rows")

    ev = pd.read_parquet(EVID)
    ev["prev_year"] = ev["season_year"] - 1

    poss_w = df["poss_season"].clip(lower=0).to_numpy()
    cut_ok = (df["poss_season"] >= MIN_FIT_POSS).to_numpy()
    se_ok = df["se_o"].notna().to_numpy() & df["se_d"].notna().to_numpy()
    w_se_o = np.where(se_ok, 1.0 / df["se_o"].fillna(np.inf) ** 2, 0.0)
    w_se_d = np.where(se_ok, 1.0 / df["se_d"].fillna(np.inf) ** 2, 0.0)

    cells = {
        "v1 + old-w": (FEATURES_V1, poss_w, poss_w, cut_ok),
        "v1 + se-w": (FEATURES_V1, w_se_o, w_se_d, se_ok),
        "atomic + old-w": (ATOMS, poss_w, poss_w, cut_ok),
        "atomic + se-w": (ATOMS, w_se_o, w_se_d, se_ok),
    }
    coef_keep = None
    fits = {}
    for name, (feats, wo, wd, fm) in cells.items():
        fits[name], coefs = fit_predict_w(df, feats, wo, wd, fm)
        if name == "atomic + se-w":
            coef_keep = coefs
    for proto in ("old", "se"):
        p(f"\nprotocol: {proto}"
          f" ({'poss-weighted, >=1000' if proto == 'old' else '1/se^2-weighted, no cutoff'})")
        p(f"{'cell':>16}  {'LOSO':>6} {'cal':>6} {'O':>6} {'D':>6}   "
          f"{'next':>6} {'cal':>6} {'O':>6} {'D':>6}")
        for name in cells:
            s = score(fits[name], ev, protocol=proto)
            p(f"{name:>16}  {s['loso']:6.4f} {s['loso_cal']:6.4f} "
              f"{s['loso_o']:6.4f} {s['loso_d']:6.4f}   {s['nxt']:6.4f} "
              f"{s['nxt_cal']:6.4f} {s['nxt_o']:6.4f} {s['nxt_d']:6.4f}"
              f"  (n={s['n_loso']}/{s['n_nxt']})")

    OUT_COEF.parent.mkdir(parents=True, exist_ok=True)
    coef_keep.to_csv(OUT_COEF, index=False)
    p(f"\nwrote {OUT_COEF}")

    era = coef_keep[coef_keep["era"] == "2021-2025"]
    for t in ("orapm", "drapm"):
        sub = era[era["target"] == t].copy()
        sub["a"] = sub["coef"].abs()
        p(f"\natomic + se-w coefficients, era 2021-2025, {t} "
          f"(sorted by |coef|):")
        p(sub.sort_values("a", ascending=False)[["feature", "coef"]]
          .round(3).to_string(index=False))

    # remaining collinearity check in the atomic set
    fitrows = df[cut_ok]
    X = fitrows[ATOMS].copy()
    for c in ATOMS:
        X[c] = X[c].fillna(X[c].mean())
    corr = X.corr()
    hits = []
    for i, a in enumerate(ATOMS):
        for b in ATOMS[i + 1:]:
            r = corr.loc[a, b]
            if abs(r) > 0.85:
                hits.append((a, b, round(float(r), 3)))
    p(f"\natomic pairs with |r|>0.85: {hits if hits else 'none'}")


if __name__ == "__main__":
    main()
