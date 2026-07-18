"""Chronological evaluation of the v1 and atomic box priors.

For test season t, coefficients are fit only on RAPM targets ending before t.
Hyperparameters are selected on 2004-18 predictions and locked for the
2019-25 report.  Both same-season (retrospective prior) and next-season
transfer are scored against independent counted-possession evidence.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_box_prior import FEATURES_V1, wcorr
from build_atomic_features import ATOMS

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
TARGET = DATA / "targets" / "rapm_target_poss_hl550.parquet"
V1 = DATA / "features_box_season.parquet"
V3 = DATA / "features_atomic_season.parquet"
EVID = DATA / "evidence_poss_season.parquet"
OUT = ROOT / "outputs" / "contextual_causal"

ALPHAS = [0.1, 1.0, 10.0, 50.0, 200.0]
MIN_POSS = 1700.0
TRAIN_YEARS = 10
DEV_END = 2018
TEST_START = 2019


def prep() -> tuple[pd.DataFrame, pd.DataFrame]:
    v1 = pd.read_parquet(V1)
    v3 = pd.read_parquet(V3).drop(columns=["games", "mins", "poss"],
                                         errors="ignore")
    feat = v1.merge(v3, on=["pid", "season_year"], suffixes=("", "_v3"))
    for c in ("height", "age", "wing_rel"):
        feat = feat.drop(columns=[c + "_v3"], errors="ignore")
    tgt = pd.read_parquet(TARGET)
    tgt = tgt[tgt["alpha"] == 500].copy()
    tgt["season_year"] = tgt["target_season"].str[:4].astype(int)
    tgt = tgt.rename(columns={"player_id": "pid"})
    panel = feat.merge(tgt, on=["pid", "season_year"], how="inner")
    ev = pd.read_parquet(EVID).rename(columns={"player_id": "pid"})
    return panel, ev


def design(train: pd.DataFrame, test: pd.DataFrame, features: list[str],
           weight: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    tr = train[features].replace([np.inf, -np.inf], np.nan).astype(float)
    te = test[features].replace([np.inf, -np.inf], np.nan).astype(float)
    mu_fill = tr.mean().fillna(0.0)
    tr = tr.fillna(mu_fill); te = te.fillna(mu_fill)
    if weight == "poss":
        w = train["poss_season"].to_numpy(float)
    else:
        se = train["_fit_se"].to_numpy(float)
        w = np.divide(1.0, se ** 2, out=np.zeros(len(se)),
                      where=np.isfinite(se) & (se > 0))
    w = w / np.mean(w[w > 0])
    x = tr.to_numpy(); z = te.to_numpy()
    mu = np.average(x, axis=0, weights=w)
    sd = np.sqrt(np.average((x - mu) ** 2, axis=0, weights=w))
    sd = np.where(np.isfinite(sd) & (sd > 1e-8), sd, 1.0)
    return (x - mu) / sd, (z - mu) / sd, w


def predictions(panel: pd.DataFrame, features: list[str], weight: str,
                target: str, alpha: float) -> pd.DataFrame:
    se_col = "se_o" if target == "orapm" else "se_d"
    all_rows = []
    for sy in range(2004, 2026):
        tr = panel[(panel.season_year < sy)
                   & (panel.season_year >= sy - TRAIN_YEARS)].copy()
        te = panel[panel.season_year == sy].copy()
        if weight == "poss":
            tr = tr[tr.poss_season >= MIN_POSS]
        else:
            tr = tr[np.isfinite(tr[se_col]) & (tr[se_col] > 0)]
        if len(tr) < 500 or te.empty:
            continue
        tr["_fit_se"] = tr[se_col]
        X, Z, w = design(tr, te, features, weight)
        m = Ridge(alpha=alpha).fit(X, tr[target], sample_weight=w)
        all_rows.append(pd.DataFrame({"pid": te.pid.to_numpy(),
                                      "season_year": sy,
                                      "pred": m.predict(Z)}))
    return pd.concat(all_rows, ignore_index=True)


def score(pred: pd.DataFrame, ev: pd.DataFrame, years: tuple[int, int],
          lead: int = 0) -> tuple[float, float, int]:
    p = pred.copy(); p["evidence_year"] = p.season_year + lead
    j = p.merge(ev, left_on=["pid", "evidence_year"],
                right_on=["pid", "season_year"], suffixes=("", "_ev"))
    j = j[(j.evidence_year >= years[0]) & (j.evidence_year <= years[1])
          & (j.ev_poss >= MIN_POSS)]
    actual = j.ev_o if pred.attrs["target"] == "orapm" else j.ev_d
    r = wcorr(j.pred, actual, j.ev_poss)
    # Weighted slope is useful: correlation alone can hide a badly scaled prior.
    pm = np.average(j.pred, weights=j.ev_poss)
    am = np.average(actual, weights=j.ev_poss)
    slope = np.sum(j.ev_poss * (j.pred - pm) * (actual - am)) / np.sum(
        j.ev_poss * (j.pred - pm) ** 2)
    return float(r), float(slope), len(j)


def main() -> None:
    panel, ev = prep()
    OUT.mkdir(parents=True, exist_ok=True)
    cache = {}
    summaries = []
    for fs_name, features in (("v1", FEATURES_V1), ("atomic", ATOMS)):
        for weight in ("poss", "se"):
            for target in ("orapm", "drapm"):
                candidates = []
                for alpha in ALPHAS:
                    pr = predictions(panel, features, weight, target, alpha)
                    pr.attrs["target"] = target
                    r, slope, n = score(pr, ev, (2004, DEV_END), lead=0)
                    candidates.append((r, alpha, slope, n, pr))
                best = max(candidates, key=lambda z: z[0])
                _, alpha, dev_slope, dev_n, pr = best
                pr.attrs["target"] = target
                test_r, test_slope, test_n = score(pr, ev, (TEST_START, 2025), 0)
                next_r, next_slope, next_n = score(pr, ev, (TEST_START, 2025), 1)
                summaries.append({"features": fs_name, "weight": weight,
                                  "target": target, "alpha": alpha,
                                  "dev_r": best[0], "dev_slope": dev_slope,
                                  "test_r": test_r, "test_slope": test_slope,
                                  "next_r": next_r, "next_slope": next_slope,
                                  "n_test": test_n, "n_next": next_n})
                cache[(fs_name, weight, target)] = pr
                print(f"{fs_name:6} {weight:4} {target}: alpha={alpha:5g} "
                      f"dev={best[0]:.4f}  2019+={test_r:.4f} "
                      f"(slope {test_slope:.2f})  next={next_r:.4f}")

    # Total score uses independently selected O and D models; no test tuning.
    for fs_name in ("v1", "atomic"):
        for weight in ("poss", "se"):
            o = cache[(fs_name, weight, "orapm")].rename(columns={"pred": "po"})
            d = cache[(fs_name, weight, "drapm")].rename(columns={"pred": "pd"})
            pr = o.merge(d, on=["pid", "season_year"])
            pr["pred"] = pr.po + pr.pd
            pr.attrs["target"] = "total"
            for lead, label in ((0, "same"), (1, "next")):
                p = pr.copy(); p["evidence_year"] = p.season_year + lead
                j = p.merge(ev, left_on=["pid", "evidence_year"],
                            right_on=["pid", "season_year"], suffixes=("", "_ev"))
                j = j[(j.evidence_year >= TEST_START) & (j.evidence_year <= 2025)
                      & (j.ev_poss >= MIN_POSS)]
                r = wcorr(j.pred, j.ev_o + j.ev_d, j.ev_poss)
                print(f"TOTAL {fs_name:6} {weight:4} {label}: {r:.4f} (n={len(j)})")
                summaries.append({"features": fs_name, "weight": weight,
                                  "target": "total_" + label, "test_r": r,
                                  "n_test": len(j)})
            pr.to_parquet(OUT / f"rolling_prior_{fs_name}_{weight}.parquet",
                          index=False)
    pd.DataFrame(summaries).to_csv(OUT / "rolling_atomic_prior_scoreboard.csv",
                                   index=False)
    print(f"Wrote scoreboard to {OUT / 'rolling_atomic_prior_scoreboard.csv'}")


if __name__ == "__main__":
    main()
