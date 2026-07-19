"""Covariance-aware multivariate season-state filter for counted stints.

Production ``build_kalman_v0.py`` first creates a ridge RAPM point estimate,
then updates every player independently with variance ``c / possessions``.
That is a diagonal approximation to the stint likelihood: it discards the
off-diagonal information saying that two tethered teammates' coefficient
errors move together.

This builder performs the Gaussian update jointly instead:

    posterior precision = predicted_precision + box_precision + X'WX / c
    posterior rhs       = predicted_precision * predicted_mean
                          + box_precision * box_mean + X'Wy / c

The resulting covariance is carried for players who appear in consecutive
seasons. New and gap-returning players retain the independent initialization
fallback. Parameters are locked by environment variables for reproducible
candidate builds; artifacts are staged under outputs before promotion.

Outputs only under outputs/contextual_causal/multivariate_kalman/.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import linalg, sparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "metric"))

from build_aging_curves import load_ages
from build_kalman_v0 import MAX_DRIFT_YEARS, old_drift_extra
from build_rapm_target import load_player_names
from counted_production_design import load_design

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PRIOR_PATH = METRIC_DATA / "priors" / "box_prior_atomic_denominator.parquet"
EVIDENCE_PATH = METRIC_DATA / "evidence_season_canonical_counted.parquet"
CURVES_PATH = METRIC_DATA / "aging" / "aging_curves.csv"
PRODUCTION_PATH = METRIC_DATA / "kalman" / "kalman_states.parquet"
OUT = ROOT / "outputs" / "contextual_causal" / "multivariate_kalman"

# Freeze production parameters for the first comparison.  No Castle-specific
# or multivariate-specific tuning is permitted in this script.
Q = float(os.environ.get("MVK_Q", "1.0"))
C = float(os.environ.get("MVK_C", "20000"))
BOX_VAR = float(os.environ.get("MVK_BOX_VAR", "8.0"))
INIT_VAR = 9.0
SHIFT_TOTAL = 2.0
FULL_POSS = 2_000.0
MIN_PANEL_POSS = 50.0
MIN_SCORE_POSS = 1_000.0
DEV_END = 2018


def cho_inverse(a: np.ndarray) -> tuple[np.ndarray, tuple]:
    """Return an SPD inverse and its Cholesky factor, with a tiny safety
    jitter only if roundoff prevents factorization."""
    eye = np.eye(len(a))
    for jitter in (0.0, 1e-10, 1e-8, 1e-6):
        try:
            cf = linalg.cho_factor(a + jitter * eye, lower=True,
                                   check_finite=False)
            return linalg.cho_solve(cf, eye, check_finite=False), cf
        except linalg.LinAlgError:
            continue
    raise linalg.LinAlgError("matrix remained non-SPD after safety jitter")


def wcorr(a: np.ndarray, b: np.ndarray, w: np.ndarray) -> float:
    am, bm = np.average(a, weights=w), np.average(b, weights=w)
    cov = np.average((a - am) * (b - bm), weights=w)
    den = np.sqrt(np.average((a - am) ** 2, weights=w)
                  * np.average((b - bm) ** 2, weights=w))
    return float(cov / den)


def fit_affine(train: pd.DataFrame, test: pd.DataFrame,
               column: str) -> tuple[float, float, float]:
    """Possession-weighted dev calibration, then test RMSE."""
    x = train[column].to_numpy(float)
    y = train.evid.to_numpy(float)
    w = train.ev_poss.to_numpy(float)
    xbar, ybar = np.average(x, weights=w), np.average(y, weights=w)
    slope = np.average((x - xbar) * (y - ybar), weights=w) / np.average(
        (x - xbar) ** 2, weights=w)
    intercept = ybar - slope * xbar
    pred = intercept + slope * test[column].to_numpy(float)
    rmse = np.sqrt(np.average((test.evid.to_numpy(float) - pred) ** 2,
                              weights=test.ev_poss.to_numpy(float)))
    return float(intercept), float(slope), float(rmse)


def main() -> None:
    run_name = os.environ.get("MVK_RUN_NAME", "production_params")
    run_out = OUT / run_name
    run_out.mkdir(parents=True, exist_ok=True)
    design = load_design()
    X, y, row_poss = design["X"], design["y"], design["poss"]
    players, pidx, P = design["players"], design["pidx"], design["P"]
    seasons = design["seasons"]
    names = load_player_names()

    evidence = pd.read_parquet(EVIDENCE_PATH)
    ev_lookup = evidence.set_index(["season_year", "player_id"])
    pri = pd.read_parquet(PRIOR_PATH).rename(columns={"pid": "player_id"})
    pri = pri.set_index(["season_year", "player_id"])
    ages = load_ages().set_index(["season_year", "player_id"])
    curves = pd.read_csv(CURVES_PATH).dropna()
    drift_o = lambda a: float(np.interp(a, curves.age, curves.d_orapm))
    drift_d = lambda a: float(np.interp(a, curves.age, curves.d_drapm))

    # Last marginal state survives gaps.  Full covariance is retained only
    # from the immediately previous season, which is the auditable prototype's
    # explicit approximation for trades, retirements and gap-returners.
    last: dict[int, dict[str, float]] = {}
    prev_year: int | None = None
    prev_keys: list[tuple[int, str]] = []
    prev_cov: np.ndarray | None = None
    rows: list[dict] = []
    latest_diag: dict[str, float] = {}

    for sy in sorted(np.unique(seasons)):
        row_mask = (seasons == sy) & (row_poss > 0)
        Xy = X[row_mask]
        wy = row_poss[row_mask].astype(float)
        yy = y[row_mask].astype(float)
        yy -= np.average(yy, weights=wy)

        # Include every player represented in that season's likelihood.  The
        # public panel threshold controls output/scoring, not nuisance columns.
        present_cols = np.asarray(Xy.getnnz(axis=0)).ravel() > 0
        active_global = np.flatnonzero(
            present_cols[:P] | present_cols[P:])
        active_ids = players[active_global].astype(int)
        cols = np.r_[active_global, P + active_global]
        Xa = Xy[:, cols].tocsr()
        n = len(active_ids)
        keys = ([(int(pid), "o") for pid in active_ids]
                + [(int(pid), "dcoef") for pid in active_ids])
        key_index = {k: i for i, k in enumerate(keys)}

        sqrtw = np.sqrt(wy)
        Xw = Xa.multiply(sqrtw[:, None]).tocsr()
        yw = yy * sqrtw
        gram = (Xw.T @ Xw).toarray()
        info_rhs = np.asarray(Xw.T @ yw).ravel()

        pred_mean = np.zeros(2 * n)
        pred_cov = np.zeros((2 * n, 2 * n))
        box_mean = np.zeros(2 * n)
        ev_poss = np.zeros(n)
        ages_now = np.full(n, np.nan)
        first = np.zeros(n, dtype=bool)

        # Build predicted state and independent marginal variances.
        for j, pid in enumerate(active_ids):
            ev_key = (int(sy), int(pid))
            if ev_key in ev_lookup.index:
                evr = ev_lookup.loc[ev_key]
                ev_poss[j] = float(evr.ev_poss)
            age_now = (float(ages.loc[ev_key, "age"])
                       if ev_key in ages.index else np.nan)
            ages_now[j] = age_now

            pr_o, pr_d = -0.5, -0.2
            if ev_key in pri.index:
                prr = pri.loc[ev_key]
                if pd.notna(prr.loso_o):
                    pr_o = float(prr.loso_o)
                if pd.notna(prr.loso_d):
                    pr_d = float(prr.loso_d)
            exposure_weight = min(ev_poss[j] / FULL_POSS, 1.0)
            pr_o -= (1.0 - exposure_weight) * SHIFT_TOTAL / 2.0
            pr_d -= (1.0 - exposure_weight) * SHIFT_TOTAL / 2.0
            box_mean[j], box_mean[n + j] = pr_o, -pr_d

            if int(pid) not in last:
                first[j] = True
                pred_mean[j], pred_mean[n + j] = pr_o, -pr_d
                pred_cov[j, j] = pred_cov[n + j, n + j] = INIT_VAR
                continue

            state = last[int(pid)]
            gap = int(sy - state["year"])
            mo, md = state["o"], state["dcoef"]
            last_age = state["age"]
            if not np.isfinite(last_age):
                last_age = (age_now - gap if np.isfinite(age_now) else 25.0)
            for step in range(min(gap, MAX_DRIFT_YEARS)):
                age_then = last_age + step
                extra = old_drift_extra(age_then + 1)
                mo += drift_o(age_then) + extra / 2
                md += -drift_d(age_then) - extra / 2
            pred_mean[j], pred_mean[n + j] = mo, md
            pred_cov[j, j] = state["var_o"] + Q * gap
            pred_cov[n + j, n + j] = state["var_dcoef"] + Q * gap

        # Carry the complete covariance submatrix for consecutive returners.
        if prev_cov is not None and prev_year == sy - 1:
            prev_index = {k: i for i, k in enumerate(prev_keys)}
            shared = [k for k in keys if k in prev_index]
            if shared:
                ci = np.array([key_index[k] for k in shared])
                pi = np.array([prev_index[k] for k in shared])
                block = prev_cov[np.ix_(pi, pi)].copy()
                block[np.diag_indices_from(block)] += Q
                pred_cov[np.ix_(ci, ci)] = block

        pred_precision, _ = cho_inverse(pred_cov)
        joint_precision = pred_precision + np.eye(2 * n) / BOX_VAR + gram / C
        rhs = (pred_precision @ pred_mean + box_mean / BOX_VAR
               + info_rhs / C)
        post_cov, post_cf = cho_inverse(joint_precision)
        post_mean = linalg.cho_solve(post_cf, rhs, check_finite=False)

        for j, pid in enumerate(active_ids):
            # Production suppresses trivial/ghost appearances from its panel.
            if ev_poss[j] >= MIN_PANEL_POSS:
                rows.append({
                    "player_id": int(pid), "season_year": int(sy),
                    "player_name": names.get(int(pid), str(pid)),
                    "pred_o": float(pred_mean[j]),
                    "pred_d": float(-pred_mean[n + j]),
                    "pred_total": float(pred_mean[j] - pred_mean[n + j]),
                    "pred_var_o": float(pred_cov[j, j]),
                    "pred_var_d": float(pred_cov[n + j, n + j]),
                    "pred_cov_od": float(-pred_cov[j, n + j]),
                    "filt_o": float(post_mean[j]),
                    "filt_d": float(-post_mean[n + j]),
                    "filt_total": float(post_mean[j] - post_mean[n + j]),
                    "filt_var_o": float(post_cov[j, j]),
                    "filt_var_d": float(post_cov[n + j, n + j]),
                    "filt_cov_od": float(-post_cov[j, n + j]),
                    "ev_poss": float(ev_poss[j]),
                    "first_season": bool(first[j]),
                    "prior_model": "atomic_denominator",
                    "evidence_model": "canonical_counted_possessions_v1",
                    "filter_model": "multivariate_stint_gaussian_v1",
                    "filter_q": Q, "filter_c": C,
                    "filter_box_var": BOX_VAR,
                })
            age_store = ages_now[j]
            if not np.isfinite(age_store):
                age_store = (last[int(pid)]["age"] + sy - last[int(pid)]["year"]
                             if int(pid) in last else 25.0)
            last[int(pid)] = {
                "year": int(sy), "age": float(age_store),
                "o": float(post_mean[j]), "dcoef": float(post_mean[n + j]),
                "var_o": float(post_cov[j, j]),
                "var_dcoef": float(post_cov[n + j, n + j]),
            }

        # Diagnostics for the current Castle/Wembanyama allocation.
        if (sy == 2025 and (1642264, "o") in key_index
                and (1641705, "o") in key_index):
            for side in ("o", "dcoef"):
                a = key_index[(1642264, side)]
                b = key_index[(1641705, side)]
                latest_diag[f"post_corr_{side}"] = float(
                    post_cov[a, b] / np.sqrt(post_cov[a, a] * post_cov[b, b]))
                latest_diag[f"pred_corr_{side}"] = float(
                    pred_cov[a, b] / np.sqrt(pred_cov[a, a] * pred_cov[b, b]))

        prev_year, prev_keys, prev_cov = int(sy), keys, post_cov
        print(f"{sy}: {n} likelihood players, "
              f"{int((ev_poss >= MIN_PANEL_POSS).sum())} panel players",
              flush=True)

    out = pd.DataFrame(rows)
    out.to_parquet(run_out / "multivariate_kalman_states.parquet", index=False)
    out.to_csv(run_out / "multivariate_kalman_states.csv", index=False)

    score = out.merge(
        evidence.assign(evid=evidence.ev_o + evidence.ev_d),
        on=["player_id", "season_year"], suffixes=("", "_target"))
    first_year = score.groupby("player_id").season_year.transform("min")
    score = score[(score.season_year > first_year)
                  & (score.ev_poss_target >= MIN_SCORE_POSS)].copy()

    prod = pd.read_parquet(PRODUCTION_PATH)[
        ["player_id", "season_year", "pred_total", "filt_total"]].rename(
            columns={"pred_total": "prod_pred", "filt_total": "prod_filt"})
    score = score.merge(prod, on=["player_id", "season_year"], how="left")
    dev = score[score.season_year <= DEV_END]
    test = score[score.season_year > DEV_END]

    summary: dict[str, object] = {
        "parameters": {"q": Q, "c": C, "box_var": BOX_VAR,
                       "shift_total": SHIFT_TOTAL, "full_poss": FULL_POSS},
        "rows": len(out), "score_rows": len(score),
        "castle_wembanyama": latest_diag,
    }
    for label, frame in (("development_through_2018", dev),
                         ("confirmation_2019_plus", test)):
        vals = {}
        for col in ("pred_total", "prod_pred"):
            qf = frame.dropna(subset=[col, "evid", "ev_poss_target"])
            vals[f"wcorr_{col}"] = wcorr(
                qf[col].to_numpy(float), qf.evid.to_numpy(float),
                qf.ev_poss_target.to_numpy(float))
        summary[label] = vals

    a, b, rmse = fit_affine(dev, test, "pred_total")
    pa, pb, prmse = fit_affine(dev.dropna(subset=["prod_pred"]),
                               test.dropna(subset=["prod_pred"]), "prod_pred")
    summary["confirmation_affine"] = {
        "multivariate_intercept": a, "multivariate_slope": b,
        "multivariate_rmse": rmse, "production_intercept": pa,
        "production_slope": pb, "production_rmse": prmse,
    }

    castle = out[out.player_id == 1642264].sort_values("season_year")
    summary["castle_states"] = castle[
        ["season_year", "pred_total", "filt_total", "filt_var_o",
         "filt_var_d", "filt_cov_od"]].to_dict("records")
    (run_out / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
