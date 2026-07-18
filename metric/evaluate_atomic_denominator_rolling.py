"""Chronological evaluation of denominator-aware atomic box priors.

The shrinkage rule and grids are preregistered in docs/research_log.md.
Hyperparameters are selected only on evidence seasons through 2018 and then
reported unchanged on 2019-25.  This script does not touch production output.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_box_prior import wcorr
from build_atomic_features import ATOMS

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
FEATURES = DATA / "features_atomic_denominator_season.parquet"
TARGET = DATA / "targets" / "rapm_target_poss_hl550.parquet"
EVIDENCE = DATA / "evidence_poss_season.parquet"
OUT = ROOT / "outputs" / "contextual_causal"

BIOS = {"height", "age", "wing_rel"}
ALPHAS = [0.1, 1.0, 10.0, 50.0, 200.0]
LAMBDAS = [0.0, 0.1, 0.25, 0.5, 1.0, 2.0, 4.0]
MIN_POSS = 1700.0
TRAIN_YEARS = 10
DEV_END = 2018
TEST_START = 2019


def prep() -> tuple[pd.DataFrame, pd.DataFrame]:
    feat = pd.read_parquet(FEATURES)
    tgt = pd.read_parquet(TARGET)
    tgt = tgt[tgt["alpha"] == 500].copy()
    tgt["season_year"] = tgt["target_season"].str[:4].astype(int)
    tgt = tgt.rename(columns={"player_id": "pid"})
    panel = feat.merge(tgt, on=["pid", "season_year"], how="inner")
    ev = pd.read_parquet(EVIDENCE).rename(columns={"player_id": "pid"})
    return panel, ev


def shrink_design(train: pd.DataFrame, test: pd.DataFrame, lam: float
                  ) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict]:
    """Return standardized EB-shrunk designs using training information only."""
    tr_out, te_out, meta = {}, {}, {}
    for atom in ATOMS:
        tr = pd.to_numeric(train[atom], errors="coerce").to_numpy(float)
        te = pd.to_numeric(test[atom], errors="coerce").to_numpy(float)
        if atom in BIOS:
            finite = np.isfinite(tr)
            mu = float(np.mean(tr[finite])) if finite.any() else 0.0
            tr_out[atom] = np.where(np.isfinite(tr), tr, mu)
            te_out[atom] = np.where(np.isfinite(te), te, mu)
            meta[atom] = {"rate_mean": mu, "median_denom": np.inf}
            continue

        dc = atom + "__denom"
        dtr = pd.to_numeric(train[dc], errors="coerce").to_numpy(float)
        dte = pd.to_numeric(test[dc], errors="coerce").to_numpy(float)
        valid = np.isfinite(tr) & np.isfinite(dtr) & (dtr > 0)
        if valid.any():
            mu = float(np.average(tr[valid], weights=dtr[valid]))
            med = float(np.median(dtr[valid]))
        else:
            mu, med = 0.0, 1.0
        med = max(med, 1e-8)
        rtr = np.divide(dtr, dtr + lam * med,
                        out=np.zeros_like(dtr),
                        where=np.isfinite(dtr) & (dtr > 0))
        rte = np.divide(dte, dte + lam * med,
                        out=np.zeros_like(dte),
                        where=np.isfinite(dte) & (dte > 0))
        tr_clean = np.where(np.isfinite(tr), tr, mu)
        te_clean = np.where(np.isfinite(te), te, mu)
        tr_out[atom] = rtr * tr_clean + (1.0 - rtr) * mu
        te_out[atom] = rte * te_clean + (1.0 - rte) * mu
        meta[atom] = {"rate_mean": mu, "median_denom": med}

    x = pd.DataFrame(tr_out).to_numpy(float)
    z = pd.DataFrame(te_out).to_numpy(float)
    w = train["poss_season"].to_numpy(float)
    w = w / np.mean(w[w > 0])
    mu_x = np.average(x, axis=0, weights=w)
    sd_x = np.sqrt(np.average((x - mu_x) ** 2, axis=0, weights=w))
    sd_x = np.where(np.isfinite(sd_x) & (sd_x > 1e-8), sd_x, 1.0)
    meta["standardization"] = {"mean": mu_x, "sd": sd_x}
    return (x - mu_x) / sd_x, (z - mu_x) / sd_x, w, meta


def predictions(panel: pd.DataFrame, target: str, alpha: float, lam: float,
                keep_coefficients: bool = False
                ) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows, coefs = [], []
    for sy in range(2004, 2026):
        tr = panel[(panel.season_year < sy)
                   & (panel.season_year >= sy - TRAIN_YEARS)
                   & (panel.poss_season >= MIN_POSS)].copy()
        te = panel[panel.season_year == sy].copy()
        if len(tr) < 500 or te.empty:
            continue
        X, Z, w, meta = shrink_design(tr, te, lam)
        model = Ridge(alpha=alpha).fit(X, tr[target], sample_weight=w)
        rows.append(pd.DataFrame({"pid": te.pid.to_numpy(),
                                  "season_year": sy,
                                  "pred": model.predict(Z)}))
        if keep_coefficients:
            std_meta = meta["standardization"]
            for idx, (atom, coef) in enumerate(zip(ATOMS, model.coef_)):
                coefs.append({"prediction_season": sy, "target": target,
                              "feature": atom, "coef_per_training_sd": coef,
                              "rate_mean": meta[atom]["rate_mean"],
                              "median_denom": meta[atom]["median_denom"],
                              "training_mean_after_shrink":
                                  std_meta["mean"][idx],
                              "training_sd_after_shrink":
                                  std_meta["sd"][idx],
                              "model_intercept": model.intercept_,
                              "alpha": alpha, "lambda": lam})
    return pd.concat(rows, ignore_index=True), pd.DataFrame(coefs)


def score(pred: pd.DataFrame, ev: pd.DataFrame, target: str,
          years: tuple[int, int], lead: int = 0) -> tuple[float, float, int]:
    p = pred.copy()
    p["evidence_year"] = p.season_year + lead
    j = p.merge(ev, left_on=["pid", "evidence_year"],
                right_on=["pid", "season_year"], suffixes=("", "_ev"))
    j = j[(j.evidence_year >= years[0]) & (j.evidence_year <= years[1])
          & (j.ev_poss >= MIN_POSS)]
    actual = j.ev_o if target == "orapm" else j.ev_d
    r = wcorr(j.pred, actual, j.ev_poss)
    pm = np.average(j.pred, weights=j.ev_poss)
    am = np.average(actual, weights=j.ev_poss)
    slope = np.sum(j.ev_poss * (j.pred - pm) * (actual - am)) / np.sum(
        j.ev_poss * (j.pred - pm) ** 2)
    return float(r), float(slope), len(j)


def total_score(pr: pd.DataFrame, ev: pd.DataFrame, lead: int
                ) -> tuple[float, int]:
    p = pr.copy()
    p["evidence_year"] = p.season_year + lead
    j = p.merge(ev, left_on=["pid", "evidence_year"],
                right_on=["pid", "season_year"], suffixes=("", "_ev"))
    j = j[(j.evidence_year >= TEST_START) & (j.evidence_year <= 2025)
          & (j.ev_poss >= MIN_POSS)]
    return float(wcorr(j.pred, j.ev_o + j.ev_d, j.ev_poss)), len(j)


def main() -> None:
    panel, ev = prep()
    OUT.mkdir(parents=True, exist_ok=True)
    chosen, summary, coef_frames, grid_rows = {}, [], [], []
    for target in ("orapm", "drapm"):
        candidates = []
        for lam in LAMBDAS:
            for alpha in ALPHAS:
                pr, _ = predictions(panel, target, alpha, lam)
                dev_r, dev_slope, dev_n = score(
                    pr, ev, target, (2004, DEV_END), 0)
                candidates.append((dev_r, lam, alpha, dev_slope, dev_n))
                grid_rows.append({"target": target, "lambda": lam,
                                  "alpha": alpha, "dev_r": dev_r,
                                  "dev_slope": dev_slope, "dev_n": dev_n})
        dev_r, lam, alpha, dev_slope, dev_n = max(candidates,
                                                   key=lambda x: x[0])
        pr, cf = predictions(panel, target, alpha, lam, True)
        test_r, test_slope, test_n = score(
            pr, ev, target, (TEST_START, 2025), 0)
        next_r, next_slope, next_n = score(
            pr, ev, target, (TEST_START, 2025), 1)
        chosen[target] = pr
        coef_frames.append(cf)
        summary.append({"features": "atomic_denominator", "weight": "poss",
                        "target": target, "alpha": alpha, "lambda": lam,
                        "dev_r": dev_r, "dev_slope": dev_slope,
                        "dev_n": dev_n, "test_r": test_r,
                        "test_slope": test_slope, "n_test": test_n,
                        "next_r": next_r, "next_slope": next_slope,
                        "n_next": next_n})
        print(f"{target}: lambda={lam:g} alpha={alpha:g} dev={dev_r:.4f} "
              f"2019+={test_r:.4f} (slope {test_slope:.2f}) "
              f"next={next_r:.4f}")

    o = chosen["orapm"].rename(columns={"pred": "po"})
    d = chosen["drapm"].rename(columns={"pred": "pd"})
    prior = o.merge(d, on=["pid", "season_year"])
    prior["pred"] = prior.po + prior.pd
    for lead, label in ((0, "same"), (1, "next")):
        r, n = total_score(prior, ev, lead)
        summary.append({"features": "atomic_denominator", "weight": "poss",
                        "target": "total_" + label, "test_r": r,
                        "n_test": n})
        print(f"TOTAL {label}: {r:.4f} (n={n})")

    prior.to_parquet(OUT / "rolling_prior_atomic_denominator_poss.parquet",
                     index=False)
    pd.concat(coef_frames, ignore_index=True).to_csv(
        OUT / "atomic_denominator_rolling_coefficients.csv", index=False)
    pd.DataFrame(summary).to_csv(
        OUT / "rolling_atomic_denominator_scoreboard.csv", index=False)
    pd.DataFrame(grid_rows).to_csv(
        OUT / "rolling_atomic_denominator_dev_grid.csv", index=False)


if __name__ == "__main__":
    main()
