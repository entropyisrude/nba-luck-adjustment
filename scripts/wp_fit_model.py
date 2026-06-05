"""
Step 3: Fit Gaussian win-probability model with time-varying sigma.

Model:
    P(home wins | diff D, time T) = Phi( (D + mu_era * T) / (sigma_era(T) * sqrt(T)) )

where:
    D            = score_diff = home_score - away_score
    T            = time_remaining in seconds
    mu_era       = home court advantage drift (pts/sec), fit per era
    sigma_era(T) = scoring volatility at time T remaining, fit per (era, 5-min window)

Each window's sigma is fit independently from the local observations in that window.
This is the "conditional sigma" interpretation: at time T, what volatility best explains
outcomes from that game state? It captures real regime changes (intentional fouling,
3pt hunting late) that a single constant sigma misses.

Observations:
    - Stint transitions (RS, ~1.8M obs)
    - Playoff PBP events (~264K obs)

Output: data/wp_model.json
"""
from __future__ import annotations
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize, minimize_scalar
from scipy.special import ndtr

STINTS_PARQUET = Path(__file__).parent.parent / "data" / "wp_obs_stints.parquet"
PBP_PARQUET    = Path(__file__).parent.parent / "data" / "wp_obs_pbp.parquet"
OUT_JSON       = Path(__file__).parent.parent / "data" / "wp_model.json"

MIN_TIME     = 10    # ignore last 10 seconds
WINDOW_SECS  = 300   # 5-minute windows
ERA_ORDER    = ["1997-2003", "2004-2010", "2011-2017", "2018-2025"]

# ── Load & merge ──────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    stints = pd.read_parquet(STINTS_PARQUET)
    pbp    = pd.read_parquet(PBP_PARQUET)
    df = pd.concat([stints, pbp], ignore_index=True)
    df = df[df["time_remaining"] > MIN_TIME].copy()
    df = df[df["era"].notna()].copy()
    df["T"] = df["time_remaining"].astype(float)
    df["D"] = df["score_diff"].astype(float)
    df["y"] = df["home_won"].astype(float)
    print(f"Total observations  : {len(df):,}")
    print(f"  From stints       : {(df['source']=='stints').sum():,}")
    print(f"  From playoff PBP  : {(df['source']=='pbp_playoff').sum():,}")
    print(f"Era breakdown:\n{df['era'].value_counts().sort_index()}")
    return df

# ── Model functions ───────────────────────────────────────────────────────────

def wp(D, T, sigma, mu):
    """Gaussian WP with given sigma (used for era-level fitting and per-window fitting)."""
    return ndtr((D + mu * T) / (sigma * np.sqrt(np.maximum(T, 1e-6))))

def neg_ll(params, D, T, y):
    sigma, mu = params
    if sigma <= 0:
        return 1e12
    p = np.clip(wp(D, T, sigma, mu), 1e-10, 1 - 1e-10)
    return -np.sum(y * np.log(p) + (1 - y) * np.log(1 - p))

def fit_era(df_era, init_sigma=1.0, init_mu=0.002):
    D, T, y = df_era["D"].values, df_era["T"].values, df_era["y"].values
    res = minimize(neg_ll, [init_sigma, init_mu], args=(D, T, y),
                   method="L-BFGS-B", bounds=[(0.1, 10), (-0.05, 0.05)])
    return float(res.x[0]), float(res.x[1]), res.fun / len(y)

# ── Time-varying sigma: fit per (era, window) independently ──────────────────

def fit_tv_sigmas(df: pd.DataFrame, era_params: dict) -> dict:
    """
    For each era, fit sigma independently in each 5-minute time window.
    Uses the conditional sigma interpretation: what sigma best explains outcomes
    from game states in this window?  Each window is fit using the standard
    wp formula with the era's mu fixed.

    Returns: era -> list of window dicts {lo, hi, minutes_remaining, sigma, n}
    """
    win_los  = list(range(0, 2880, WINDOW_SECS))
    era_tv   = {}

    for era in ERA_ORDER:
        mu_era   = era_params[era]["mu"]
        sig_init = era_params[era]["sigma"]
        sub_era  = df[df["era"] == era]
        windows  = []

        for lo in win_los:
            hi  = min(lo + WINDOW_SECS, 2880)
            sub = sub_era[(sub_era["T"] >= lo) & (sub_era["T"] < hi)]
            n   = len(sub)
            mid_min = round((2880 - (lo + hi) / 2) / 60, 1)  # minutes remaining at midpoint

            if n < 300:
                sigma_w = sig_init  # fallback for thin windows
            else:
                D_w, T_w, y_w = sub["D"].values, sub["T"].values, sub["y"].values
                res = minimize_scalar(
                    lambda s: neg_ll([s, mu_era], D_w, T_w, y_w),
                    bounds=(0.1, 10), method="bounded"
                )
                sigma_w = float(res.x)

            windows.append({
                "lo":                int(lo),
                "hi":                int(hi),
                "minutes_remaining": mid_min,
                "sigma":             round(sigma_w, 6),
                "n":                 int(n),
            })

        era_tv[era] = windows

    return era_tv

def sigma_at_T(windows: list[dict], T_sec: float) -> float:
    """Return the fitted sigma for the window containing T_sec."""
    for w in windows:
        if w["lo"] <= T_sec < w["hi"]:
            return w["sigma"]
    return windows[-1]["sigma"]  # extrapolate with last window if needed

# ── Calibration ───────────────────────────────────────────────────────────────

def build_calibration_tv(df: pd.DataFrame, era_params: dict,
                          era_tv: dict) -> list[dict]:
    """Bin by TV-model predicted probability → compare to actual win rate."""
    df    = df.copy()
    preds = np.full(len(df), np.nan)

    for era in ERA_ORDER:
        mask = (df["era"] == era).values
        sub  = df[mask]
        mu   = era_params[era]["mu"]
        T    = sub["T"].values
        D    = sub["D"].values

        # Vectorise sigma lookup: one sigma per window, apply per observation
        windows = era_tv[era]
        sigma_v = np.array([sigma_at_T(windows, t) for t in T])
        pred    = ndtr((D + mu * T) / (sigma_v * np.sqrt(np.maximum(T, 1e-6))))
        preds[mask] = pred

    df["pred"] = preds
    bins = np.linspace(0, 1, 21)
    df["bin"] = pd.cut(df["pred"], bins=bins)
    rows = []
    for b, grp in df.groupby("bin", observed=False):
        if len(grp) < 20:
            continue
        rows.append({
            "pred_mid": float((b.left + b.right) / 2),
            "actual":   float(grp["y"].mean()),
            "n":        int(len(grp)),
        })
    return rows

# ── WP surface ────────────────────────────────────────────────────────────────

def precompute_surface_tv(windows: list[dict], mu: float,
                           diffs: list[int], times_sec: list[int]) -> dict:
    """WP[diff][time_sec] lookup using time-varying sigma.
    Keys are integer seconds so the JS can do an exact lookup with no float formatting.
    """
    out = {}
    for d in diffs:
        out[str(d)] = {}
        for t_sec in times_sec:
            if t_sec <= 0:
                p = 1.0 if d > 0 else (0.0 if d < 0 else 0.5)
            else:
                sigma = sigma_at_T(windows, t_sec)
                p = float(ndtr((d + mu * t_sec) / (sigma * np.sqrt(t_sec))))
            out[str(d)][str(t_sec)] = round(p, 4)
    return out

# ── Year-by-year sigma (pace trend chart) ────────────────────────────────────

def sigma_by_year(df: pd.DataFrame, mu_global: float) -> list[dict]:
    rows = []
    for yr in sorted(df["season_yr"].unique()):
        sub = df[df["season_yr"] == yr]
        if len(sub) < 5000:
            continue
        D, T, y = sub["D"].values, sub["T"].values, sub["y"].values
        res = minimize_scalar(lambda s: neg_ll([s, mu_global], D, T, y),
                              bounds=(0.1, 10), method="bounded")
        rows.append({
            "season_yr": int(yr),
            "season":    f"{yr}-{str(yr+1)[2:]}",
            "sigma":     float(res.x),
            "n":         int(len(sub)),
        })
    return rows

# ── Combined 1997-2025 era ────────────────────────────────────────────────────

def build_combined_era(era_tv: dict, era_params: dict,
                       mu_global: float) -> tuple[list[dict], dict]:
    """
    Build '1997-2025' TV windows as the observation-weighted RMS of era windows.
    """
    n_windows = len(next(iter(era_tv.values())))
    combined  = []

    for i in range(n_windows):
        w0       = era_tv[ERA_ORDER[0]][i]
        total_n  = sum(era_tv[e][i]["n"] for e in ERA_ORDER)
        if total_n == 0:
            sigma_w = float(np.mean([era_tv[e][i]["sigma"] for e in ERA_ORDER]))
        else:
            sigma_w = float(np.sqrt(
                sum(era_tv[e][i]["sigma"] ** 2 * era_tv[e][i]["n"] for e in ERA_ORDER)
                / total_n
            ))
        combined.append({
            "lo":                w0["lo"],
            "hi":                w0["hi"],
            "minutes_remaining": w0["minutes_remaining"],
            "sigma":             round(sigma_w, 6),
            "n":                 int(total_n),
        })

    sig_display = float(np.sqrt(
        sum(p["sigma"] ** 2 * p["n"] for p in era_params.values())
        / sum(p["n"] for p in era_params.values())
    ))
    params = {
        "sigma":   round(sig_display, 6),
        "mu":      mu_global,
        "hca_pts": round(mu_global * 2880, 3),
        "n":       sum(p["n"] for p in era_params.values()),
    }
    return combined, params

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    df = load_data()

    # ── Step 1: Fit constant sigma + mu per era (summary display) ─────────────
    print("\n--- Fitting per-era constant-sigma models ---")
    era_params = {}
    sig0, mu0, _ = fit_era(df, init_sigma=1.0, init_mu=0.002)
    mu_global    = float(mu0)
    print(f"Global fit:  sigma={sig0:.4f}  mu={mu_global:.6f}")

    for era in ERA_ORDER:
        sub                 = df[df["era"] == era]
        sigma_era, mu_era, nll = fit_era(sub, init_sigma=sig0, init_mu=mu_global)
        hca                 = mu_era * 2880
        era_params[era]     = {
            "sigma":   round(float(sigma_era), 6),
            "mu":      round(float(mu_era), 8),
            "hca_pts": round(hca, 3),
            "n":       int(len(sub)),
        }
        print(f"  {era}: sigma={sigma_era:.4f}  HCA={hca:.2f}pts")

    # ── Step 2: Fit time-varying sigma per (era, window) ─────────────────────
    print("\n--- Fitting time-varying sigma per era ---")
    era_tv = fit_tv_sigmas(df, era_params)
    for era, windows in era_tv.items():
        sigs = "  ".join(f"{w['sigma']:.3f}@{w['minutes_remaining']:.0f}m"
                         for w in windows)
        print(f"  {era}: {sigs}")

    # ── Step 3: Build combined 1997-2025 era ──────────────────────────────────
    combined_windows, combined_params = build_combined_era(era_tv, era_params, mu_global)
    era_tv["1997-2025"]     = combined_windows
    era_params["1997-2025"] = combined_params

    ERA_ORDER_ALL = ERA_ORDER + ["1997-2025"]

    # ── Step 4: Precompute WP surfaces ────────────────────────────────────────
    print("\n--- Precomputing WP surfaces (time-varying sigma, 10-sec resolution) ---")
    DIFFS     = list(range(-30, 31))
    TIMES_SEC = list(range(0, 2881, 10))   # 0 to 2880 seconds in 10-sec steps (289 values)
    TIMES_MIN = [round(t / 60, 6) for t in TIMES_SEC]  # for JS curve drawing

    surfaces = {}
    for era in ERA_ORDER_ALL:
        surfaces[era] = precompute_surface_tv(
            era_tv[era], era_params[era]["mu"], DIFFS, TIMES_SEC
        )
        print(f"  {era}: done ({len(TIMES_SEC)} time points)")

    # ── Step 5: Year-by-year sigma (pace trend) ───────────────────────────────
    print("\n--- Year-by-year sigma ---")
    yearly = sigma_by_year(df, mu_global)
    for row in yearly[::3]:
        print(f"  {row['season']}: sigma={row['sigma']:.4f}  n={row['n']:,}")

    # ── Step 6: Calibration ───────────────────────────────────────────────────
    print("\n--- Building calibration ---")
    calib = build_calibration_tv(df, era_params, era_tv)

    # ── Step 7: Write JSON ────────────────────────────────────────────────────
    output = {
        "mu_global":       mu_global,
        "regulation_secs": 2880,
        "era_order":       ERA_ORDER_ALL,
        "era_params":      era_params,
        "era_tv":          era_tv,
        "yearly_sigma":    yearly,
        "calibration":     calib,
        "surfaces":        surfaces,
        "diffs":           DIFFS,
        "times_sec":       TIMES_SEC,   # integer seconds, matches surface keys
        "times_min":       TIMES_MIN,   # float minutes, for JS curve drawing
    }

    with open(OUT_JSON, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    kb = OUT_JSON.stat().st_size / 1024
    print(f"\nModel saved to {OUT_JSON}  ({kb:.0f} KB)")

    print("\n=== SUMMARY ===")
    print(f"{'Era':<12} {'sigma':>8} {'HCA':>8}   sigma by window (game-end -> tipoff)")
    for era in ERA_ORDER_ALL:
        p   = era_params[era]
        tvs = "  ".join(f"{w['sigma']:.3f}" for w in era_tv[era])
        print(f"  {era:<12} {p['sigma']:>8.4f} {p['hca_pts']:>7.2f}pts  [{tvs}]")

if __name__ == "__main__":
    run()
