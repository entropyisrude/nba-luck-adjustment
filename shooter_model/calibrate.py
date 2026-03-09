"""
Calibrate population-level priors for the Bayesian 3PT expectation model.

Fits:
  1. FT% -> 3PT% regression (overall, C&S, pull-up)
  2. Prior concentration κ for each context via Beta-Binomial MLE

Reads:  shooter_model/data/calibration.csv
Writes: shooter_model/params.json
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import optimize, special, stats

DATA_DIR = Path(__file__).parent / "data"
PARAMS_PATH = Path(__file__).parent / "params.json"

MIN_3PA = 20    # minimum total 3PA for calibration
MIN_FTA = 30    # minimum FTA for FT% to be reliable
MIN_CS_3PA = 15 # minimum C&S attempts for C&S-specific fit
MIN_PU_3PA = 15 # minimum pull-up attempts for pull-up fit


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_calibration_data() -> pd.DataFrame:
    path = DATA_DIR / "calibration.csv"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found — run fetch_data.py first.")
    raw = pd.read_csv(path)
    df = raw[(raw["FG3A"] >= MIN_3PA) & (raw["FTA"] >= MIN_FTA)].copy()
    print(f"Usable player-seasons: {len(df)} of {len(raw)} total rows")
    print(f"Seasons: {sorted(df['SEASON'].unique().tolist())}")
    return df


# ---------------------------------------------------------------------------
# Linear regression helpers
# ---------------------------------------------------------------------------

def _wls(x: np.ndarray, y: np.ndarray, w: np.ndarray) -> tuple[float, float, float]:
    """Weighted least squares: y = intercept + slope * x. Returns (intercept, slope, r2)."""
    coeffs = np.polyfit(x, y, 1, w=w)
    slope, intercept = float(coeffs[0]), float(coeffs[1])
    y_hat = intercept + slope * x
    y_bar = np.average(y, weights=w)
    ss_res = np.sum(w * (y - y_hat) ** 2)
    ss_tot = np.sum(w * (y - y_bar) ** 2)
    r2 = float(1.0 - ss_res / ss_tot)
    return intercept, slope, r2


def fit_overall_regression(df: pd.DataFrame) -> dict:
    sub = df.dropna(subset=["FT_PCT", "FG3_PCT"])
    w = np.sqrt(sub["FG3A"].values)
    intercept, slope, r2 = _wls(sub["FT_PCT"].values, sub["FG3_PCT"].values, w)
    print(f"\nOverall  3PT% = {intercept:.4f} + {slope:.4f} * FT%   (wR²={r2:.3f}, n={len(sub)})")
    for ft in (0.60, 0.75, 0.85, 0.95):
        print(f"  FT%={ft:.2f} -> expected 3PT% = {intercept + slope * ft:.3f}")
    return {"intercept": intercept, "slope": slope, "r_squared": r2}


def fit_cs_regression(df: pd.DataFrame) -> dict:
    sub = df[(df["CATCH_SHOOT_FG3A"] >= MIN_CS_3PA)].dropna(
        subset=["FT_PCT", "CATCH_SHOOT_FG3_PCT"]
    )
    w = np.sqrt(sub["CATCH_SHOOT_FG3A"].values)
    intercept, slope, r2 = _wls(sub["FT_PCT"].values, sub["CATCH_SHOOT_FG3_PCT"].values, w)
    print(f"\nC&S      3PT% = {intercept:.4f} + {slope:.4f} * FT%   (wR²={r2:.3f}, n={len(sub)})")
    return {"intercept": intercept, "slope": slope, "r_squared": r2}


def fit_pullup_regression(df: pd.DataFrame) -> dict:
    sub = df[(df["PULLUP_FG3A"] >= MIN_PU_3PA)].dropna(
        subset=["FT_PCT", "PULLUP_FG3_PCT"]
    )
    w = np.sqrt(sub["PULLUP_FG3A"].values)
    intercept, slope, r2 = _wls(sub["FT_PCT"].values, sub["PULLUP_FG3_PCT"].values, w)
    print(f"\nPull-up  3PT% = {intercept:.4f} + {slope:.4f} * FT%   (wR²={r2:.3f}, n={len(sub)})")
    return {"intercept": intercept, "slope": slope, "r_squared": r2}


# ---------------------------------------------------------------------------
# Kappa estimation via Beta-Binomial MLE
# ---------------------------------------------------------------------------

def _bb_loglik(log_kappa: float, mu: np.ndarray, k: np.ndarray, n: np.ndarray) -> float:
    """
    Negative Beta-Binomial log-likelihood for a given log(kappa).

    For each player-season:
      p_i ~ Beta(mu_i * kappa, (1 - mu_i) * kappa)
      k_i | n_i, p_i ~ Binomial(n_i, p_i)

    Marginal: log P(k_i | n_i) = log B(a+k, b+n-k) - log B(a, b)  (binomial coeff omitted)
    where a = mu * kappa, b = (1-mu) * kappa.
    """
    kappa = np.exp(log_kappa)
    a = mu * kappa
    b = (1.0 - mu) * kappa
    ll = (
        special.betaln(a + k, b + n - k)
        - special.betaln(a, b)
    )
    return -float(np.sum(ll))


def estimate_kappa(
    df: pd.DataFrame,
    reg: dict,
    k_col: str,
    n_col: str,
    ft_col: str = "FT_PCT",
    label: str = "",
    min_n: int = 1,
) -> float:
    """Find kappa that maximises the Beta-Binomial likelihood."""
    sub = df[(df[n_col] >= min_n)].dropna(subset=[ft_col, k_col, n_col]).copy()
    mu = np.clip(
        reg["intercept"] + reg["slope"] * sub[ft_col].values,
        0.05, 0.65,
    )
    k = sub[k_col].values.astype(float)
    n = sub[n_col].values.astype(float)

    result = optimize.minimize_scalar(
        lambda lk: _bb_loglik(lk, mu, k, n),
        bounds=(np.log(5), np.log(2000)),
        method="bounded",
    )
    kappa = float(np.exp(result.x))
    print(f"  Optimal kappa ({label}): {kappa:.1f}  (neg-LL={result.fun:.1f})")
    return kappa


# ---------------------------------------------------------------------------
# Population stats
# ---------------------------------------------------------------------------

def population_stats(df: pd.DataFrame) -> dict:
    return {
        "n_player_seasons": int(len(df)),
        "seasons": sorted(df["SEASON"].unique().tolist()),
        "mean_ft_pct": float(df["FT_PCT"].mean()),
        "mean_3pt_pct": float(df["FG3_PCT"].mean()),
        "mean_cs_3pt_pct": float(df["CATCH_SHOOT_FG3_PCT"].dropna().mean()),
        "mean_pullup_3pt_pct": float(df["PULLUP_FG3_PCT"].dropna().mean()),
        "mean_cs_rate": float(df["CS_RATE"].dropna().mean()),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("3PT Expectation Model — Calibration")
    print("=" * 60)

    df = load_calibration_data()

    # Regressions
    overall = fit_overall_regression(df)
    cs = fit_cs_regression(df)
    pu = fit_pullup_regression(df)

    # Kappa estimation
    print("\nEstimating prior concentration (kappa) via Beta-Binomial MLE:")
    kappa_overall = estimate_kappa(
        df, overall, k_col="FG3M", n_col="FG3A", label="overall"
    )
    kappa_cs = estimate_kappa(
        df[df["CATCH_SHOOT_FG3A"] >= MIN_CS_3PA],
        cs, k_col="CATCH_SHOOT_FG3M", n_col="CATCH_SHOOT_FG3A", label="C&S",
        min_n=MIN_CS_3PA,
    )
    kappa_pu = estimate_kappa(
        df[df["PULLUP_FG3A"] >= MIN_PU_3PA],
        pu, k_col="PULLUP_FG3M", n_col="PULLUP_FG3A", label="pull-up",
        min_n=MIN_PU_3PA,
    )

    # Population stats
    pop = population_stats(df)
    print(f"\nPopulation stats: {pop}")

    params = {
        "overall": {**overall, "kappa": kappa_overall},
        "catch_shoot": {**cs, "kappa": kappa_cs},
        "pullup": {**pu, "kappa": kappa_pu},
        "population": pop,
        "filters": {"min_3pa": MIN_3PA, "min_fta": MIN_FTA,
                    "min_cs_3pa": MIN_CS_3PA, "min_pu_3pa": MIN_PU_3PA},
    }

    with open(PARAMS_PATH, "w") as f:
        json.dump(params, f, indent=2)
    print(f"\nWrote {PARAMS_PATH}")


if __name__ == "__main__":
    main()
