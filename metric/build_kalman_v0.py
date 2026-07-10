"""Phase 4a: season-level Kalman filter over player skill states.

Each player carries a hidden (O, D) impact state. Between seasons the state
drifts by our own aging curves (metric/build_aging_curves.py) and gains
process noise Q per elapsed season (missed years still age and diffuse).
Each played season provides two observations:

  * single-season luck-adjusted RAPM evidence (variance ~ c / possessions),
  * the LOSO box prior for that season (variance sigma_b^2).

The one-step-ahead prediction (state after drift, BEFORE seeing that
season's observations) is the honest deliverable: it predicts season t using
information through t-1 only, directly comparable to Phase 3's metric_v0
(which predicts next-season evidence from data through t-1 as well).

(q_o, q_d, c, sigma_b) are selected on that predictive scoreboard.

Replaces the fixed 550-day decay with learned dynamics: recency weighting
now comes from Q (how fast skill really moves) instead of a hand-picked
half-life, and aging enters explicitly.

Outputs: nba-metric-data/kalman/kalman_states.parquet|csv
(filtered + predicted O/D/total + variances per player-season, best config)
and evidence cache nba-metric-data/evidence_season.parquet (O/D split).

Usage: python metric/build_kalman_v0.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_metric_v0 import build_design, normal_eqs
from build_rapm_target import prepare, load_player_names
from build_aging_curves import load_ages

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PRIOR_PATH = METRIC_DATA / "priors" / "box_prior.parquet"
CURVES_PATH = METRIC_DATA / "aging" / "aging_curves.csv"
EVID_CACHE = METRIC_DATA / "evidence_season.parquet"
OUT_DIR = METRIC_DATA / "kalman"

EVID_ALPHA = 150
MIN_EVID_POSS = 1000          # scoreboard filter (same as Phase 3)
INIT_VAR = 9.0                # first-appearance state variance (sd 3)

Q_GRID = [0.25, 0.5, 1.0]     # process variance per season, per side
C_GRID = [2e4, 5e4, 1e5]      # evidence variance = c / poss
SB_GRID = [4.0, 8.0]          # box-prior observation variance


def build_evidence() -> pd.DataFrame:
    if EVID_CACHE.exists():
        print(f"Using cached {EVID_CACHE}")
        return pd.read_parquet(EVID_CACHE)
    st = prepare()
    st["date"] = pd.to_datetime(st["date"])
    st, X, y, poss, players, pidx, hidx, aidx = build_design(st)
    P = len(players)
    syears = st["season_year"].to_numpy()
    rows = []
    for sy in range(1996, 2026):
        sel = syears == sy
        if not sel.any():
            continue
        w_ev = np.where(sel, poss, 0.0)
        XtX, Xty, used = normal_eqs(X, y, w_ev)
        be = np.linalg.solve(XtX + EVID_ALPHA * np.eye(2 * P), Xty)
        O, D = be[:P], be[P:]
        om, dm = O.mean(), D.mean()
        raw = np.zeros(P)
        psel = poss[used]
        insel = sel[used]
        for k in range(5):
            for idxs in (hidx[used, k], aidx[used, k]):
                np.add.at(raw, idxs, np.where(insel, psel, 0.0))
        for i in np.nonzero(raw > 0)[0]:
            rows.append({"player_id": int(players[i]), "season_year": sy,
                         "ev_o": float(O[i] - om), "ev_d": float(-(D[i] - dm)),
                         "ev_poss": float(raw[i])})
        print(f"  evidence {sy}: {int((raw > 0).sum())} players", flush=True)
    ev = pd.DataFrame(rows)
    ev.to_parquet(EVID_CACHE, index=False)
    print(f"Cached {len(ev)} evidence rows to {EVID_CACHE}")
    return ev


def run_filter(panel: pd.DataFrame, drift_o, drift_d,
               q: float, c: float, sb: float) -> pd.DataFrame:
    """Sequential filter per player. panel: one row per (player, season) sorted."""
    out = []
    for pid, g in panel.groupby("player_id", sort=False):
        m_o = m_d = None
        v_o = v_d = None
        last_year = None
        for r in g.itertuples(index=False):
            if m_o is None:
                # initialize at box prior (or 0) with wide variance
                m_o = r.prior_o if not np.isnan(r.prior_o) else -0.5
                m_d = r.prior_d if not np.isnan(r.prior_d) else -0.2
                v_o = v_d = INIT_VAR
            else:
                gap = r.season_year - last_year
                for yy in range(last_year, r.season_year):
                    age_then = r.age - (r.season_year - yy)
                    m_o += drift_o(age_then)
                    m_d += drift_d(age_then)
                v_o += q * gap
                v_d += q * gap
            pred_o, pred_d, pv_o, pv_d = m_o, m_d, v_o, v_d

            # update with box prior observation
            if not np.isnan(r.prior_o):
                k = v_o / (v_o + sb)
                m_o += k * (r.prior_o - m_o); v_o *= (1 - k)
                k = v_d / (v_d + sb)
                m_d += k * (r.prior_d - m_d); v_d *= (1 - k)
            # update with season RAPM evidence
            if not np.isnan(r.ev_o) and r.ev_poss > 0:
                var_e = c / r.ev_poss
                k = v_o / (v_o + var_e)
                m_o += k * (r.ev_o - m_o); v_o *= (1 - k)
                k = v_d / (v_d + var_e)
                m_d += k * (r.ev_d - m_d); v_d *= (1 - k)

            out.append({"player_id": pid, "season_year": r.season_year,
                        "pred_o": pred_o, "pred_d": pred_d,
                        "pred_var_o": pv_o, "pred_var_d": pv_d,
                        "filt_o": m_o, "filt_d": m_d,
                        "filt_var_o": v_o, "filt_var_d": v_d})
            last_year = r.season_year
    return pd.DataFrame(out)


def wcorr(a, b, w):
    am, bm = np.average(a, weights=w), np.average(b, weights=w)
    cov = np.average((a - am) * (b - bm), weights=w)
    return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                         * np.average((b - bm) ** 2, weights=w))


def main() -> None:
    ev = build_evidence()
    prior = pd.read_parquet(PRIOR_PATH)
    prior = prior.rename(columns={"pid": "player_id"})[
        ["player_id", "season_year", "loso_o", "loso_d"]].rename(
        columns={"loso_o": "prior_o", "loso_d": "prior_d"})
    ages = load_ages()

    panel = ev.merge(prior, on=["player_id", "season_year"], how="left") \
              .merge(ages, on=["player_id", "season_year"], how="left")
    panel["age"] = panel["age"].fillna(27.0)
    panel = panel.sort_values(["player_id", "season_year"]).reset_index(drop=True)
    print(f"Panel: {len(panel)} player-seasons")

    curves = pd.read_csv(CURVES_PATH).dropna()
    drift_o = lambda a: float(np.interp(a, curves["age"], curves["d_orapm"]))
    drift_d = lambda a: float(np.interp(a, curves["age"], curves["d_drapm"]))

    # scoreboard: predicted (pre-observation) state vs that season's evidence
    score_mask_cols = ["ev_o", "ev_d"]
    best = None
    for q in Q_GRID:
        for c in C_GRID:
            for sb in SB_GRID:
                f = run_filter(panel, drift_o, drift_d, q, c, sb)
                j = f.merge(panel[["player_id", "season_year", "ev_o", "ev_d",
                                   "ev_poss"]], on=["player_id", "season_year"])
                # only seasons after the first (a prediction exists) and real minutes
                j = j[(j["ev_poss"] >= MIN_EVID_POSS)]
                # exclude each player's first season (prediction == prior init)
                firsts = panel.groupby("player_id")["season_year"].min().rename("fy")
                j = j.merge(firsts, on="player_id")
                j = j[j["season_year"] > j["fy"]]
                s = wcorr(j["pred_o"] + j["pred_d"], j["ev_o"] + j["ev_d"],
                          j["ev_poss"])
                print(f"  q={q:<5} c={c:<7.0f} sb={sb:<3}: {s:.4f} (n={len(j)})",
                      flush=True)
                if best is None or s > best[0]:
                    best = (s, q, c, sb, f)
    s, q, c, sb, f = best
    print(f"\nBest: q={q} c={c} sb={sb} -> predictive wcorr {s:.4f}")

    names = load_player_names()
    f["player_name"] = f["player_id"].map(names)
    f["pred_total"] = f["pred_o"] + f["pred_d"]
    f["filt_total"] = f["filt_o"] + f["filt_d"]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    f.to_parquet(OUT_DIR / "kalman_states.parquet", index=False)
    f.to_csv(OUT_DIR / "kalman_states.csv", index=False)
    print(f"Wrote {len(f)} rows to {OUT_DIR / 'kalman_states.parquet'}")

    top = f[f["season_year"] == 2025].nlargest(12, "filt_total")
    print("\nTop 12 filtered states, 2025-26:")
    print(top[["player_name", "filt_o", "filt_d", "filt_total",
               "filt_var_o"]].round(2).to_string(index=False))


if __name__ == "__main__":
    main()
