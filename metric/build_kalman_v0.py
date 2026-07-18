"""Phase 4a: season-level Kalman filter over player skill states.

Each player carries a hidden (O, D) impact state. Between seasons the state
drifts by our own aging curves (metric/build_aging_curves.py) and gains
process noise Q per elapsed season (missed years still age and diffuse).
Each played season provides two observations:

  * single-season luck-adjusted RAPM evidence (variance ~ c / possessions),
  * the denominator-aware atomic prior for that season (variance sigma_b^2).

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
from build_rapm_target import (DECAY_HALFLIFE_DAYS, WINDOW_DAYS,
                               load_player_names)
from counted_production_design import (exposure_arrays, load_design,
                                       normal_eqs)
from build_aging_curves import load_ages

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
# --filtered-prior swaps in the Phase 4b game-filtered box prior
PRIOR_PATH = METRIC_DATA / "priors" / "box_prior_atomic_denominator.parquet"
PRIOR_MODEL = "atomic_denominator"
EVIDENCE_MODEL = "canonical_counted_possessions_v1"
FILTERED_PRIOR_PATH = METRIC_DATA / "game_kalman" / "filtered_prior.parquet"
CURVES_PATH = METRIC_DATA / "aging" / "aging_curves.csv"
EVID_CACHE = METRIC_DATA / "evidence_season_canonical_counted.parquet"
OUT_DIR = METRIC_DATA / "kalman"

EVID_ALPHA = 150
MIN_EVID_POSS = 1000          # scoreboard filter (same as Phase 3)
MIN_PANEL_POSS = 50           # below this an "appearance" is likely a lineup-id
                              # parsing ghost (retired coaches show up with ~15
                              # possessions) and carries no information anyway
INIT_VAR = 9.0                # first-appearance state variance (sd 3)
MAX_DRIFT_YEARS = 4           # cap accumulated aging drift across long gaps;
                              # beyond this the exploding variance does the work

# Survivorship correction to the old-age drift (added 2026-07-11): the
# delta-method aging curves only see players who kept playing, so they
# understate decline past ~30. Fit on train-year (<=2018) one-step forecast
# residuals, validated OOS 2019+: 30+ residuals -0.47/-0.59 -> -0.18/-0.19,
# younger ages untouched, wcorr 0.5550 -> 0.5590. Split evenly O/D.
OLD_DRIFT_AGE = 29.0
OLD_DRIFT_A = -0.230          # extra total drift/season at age 30
OLD_DRIFT_B = -0.029          # additional per year beyond 30


def old_drift_extra(age: float) -> float:
    """Extra total drift for the season played at `age` (>29): the fit is
    corr(age) = -0.230 - 0.029*(age - 29), i.e. -0.259 at 30, -0.46 at 37."""
    if age <= OLD_DRIFT_AGE:
        return 0.0
    return OLD_DRIFT_A + OLD_DRIFT_B * (age - OLD_DRIFT_AGE)

Q_GRID = [0.25, 0.5, 1.0]     # process variance per season, per side
C_GRID = [2e4, 5e4, 1e5]      # evidence variance = c / poss
SB_GRID = [4.0, 8.0]          # box-prior observation variance

# Fringe-player fix (2026-07-10, motivated by the margin backtest): the box
# prior is fit on >=1000-poss players and extrapolates too generously below
# that range (200-1k-poss players were predicted -0.96 vs evidence -1.61).
# Exposure-dependent LEVEL shift of the prior observation (and init):
# p = prior - (1 - w) * shift, w = min(poss / full_poss, 1). A shift (not a
# blend to a constant) fixes the level the margin backtest cares about while
# preserving ordering among fringe players — blending to a constant was
# tried first and destroyed fringe correlation (0.143 -> 0.107).
SHIFT_GRID = [0.0, 1.0, 2.0, 3.0]        # total shift at zero exposure (split O/D)
FULL_POSS_GRID = [1000.0, 2000.0]        # poss at which the prior is fully trusted
FRINGE_LO, FRINGE_HI = 200.0, 1000.0     # fringe scoreboard bucket


def build_evidence() -> pd.DataFrame:
    if EVID_CACHE.exists():
        print(f"Using cached {EVID_CACHE}")
        return pd.read_parquet(EVID_CACHE)
    design = load_design()
    X, y, poss = design["X"], design["y"], design["poss"]
    players, P = design["players"], design["P"]
    dates, syears = design["dates"], design["seasons"]
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
        end = dates[sel].max()
        raw, _ = exposure_arrays(
            design, sy, end, DECAY_HALFLIFE_DAYS, WINDOW_DAYS)
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
               q: float, c: float, sb: float,
               shift_total: float = 0.0, full_poss: float = 0.0) -> pd.DataFrame:
    """Sequential filter per player. panel: one row per (player, season) sorted.
    shift_total/full_poss > 0 level-shifts the prior observation down for
    players below the prior's fit range: p = prior - (1-w)*shift,
    w = min(poss/full_poss, 1)."""
    shift_o = shift_d = shift_total / 2.0
    out = []
    for pid, g in panel.groupby("player_id", sort=False):
        m_o = m_d = None
        v_o = v_d = None
        last_year = None
        last_age = None
        for r in g.itertuples(index=False):
            # age for this row: observed, else carried forward from last seen
            age_now = r.age
            if np.isnan(age_now):
                age_now = (last_age + (r.season_year - last_year)
                           if last_age is not None else 25.0)
            # exposure-shifted prior observation: below the prior's fit
            # range, shift the level down (being barely played is itself
            # evidence — coaches' revealed preference)
            if full_poss > 0:
                w = min(r.ev_poss / full_poss, 1.0)
                base_o = r.prior_o if not np.isnan(r.prior_o) else -0.5
                base_d = r.prior_d if not np.isnan(r.prior_d) else -0.2
                p_o = base_o - (1 - w) * shift_o
                p_d = base_d - (1 - w) * shift_d
            else:
                p_o, p_d = r.prior_o, r.prior_d
            if m_o is None:
                # initialize at (blended) box prior with wide variance
                m_o = p_o if not np.isnan(p_o) else -0.5
                m_d = p_d if not np.isnan(p_d) else -0.2
                v_o = v_d = INIT_VAR
            else:
                gap = r.season_year - last_year
                for k in range(min(gap, MAX_DRIFT_YEARS)):
                    age_then = last_age + k
                    extra = old_drift_extra(age_then + 1)
                    m_o += drift_o(age_then) + extra / 2
                    m_d += drift_d(age_then) + extra / 2
                v_o += q * gap
                v_d += q * gap
            pred_o, pred_d, pv_o, pv_d = m_o, m_d, v_o, v_d

            # update with box prior observation
            if not np.isnan(p_o):
                k = v_o / (v_o + sb)
                m_o += k * (p_o - m_o); v_o *= (1 - k)
                k = v_d / (v_d + sb)
                m_d += k * (p_d - m_d); v_d *= (1 - k)
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
            last_age = age_now
    return pd.DataFrame(out)


def wcorr(a, b, w):
    am, bm = np.average(a, weights=w), np.average(b, weights=w)
    cov = np.average((a - am) * (b - bm), weights=w)
    return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                         * np.average((b - bm) ** 2, weights=w))


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--filtered-prior", action="store_true",
                    help="use the Phase 4b game-filtered box prior")
    args = ap.parse_args()

    ev = build_evidence()
    path = FILTERED_PRIOR_PATH if args.filtered_prior else PRIOR_PATH
    print(f"Prior source: {path.name}")
    prior = pd.read_parquet(path)
    prior = prior.rename(columns={"pid": "player_id"})[
        ["player_id", "season_year", "loso_o", "loso_d"]].rename(
        columns={"loso_o": "prior_o", "loso_d": "prior_d"})
    ages = load_ages()

    panel = ev.merge(prior, on=["player_id", "season_year"], how="left") \
              .merge(ages, on=["player_id", "season_year"], how="left")
    n0 = len(panel)
    panel = panel[panel["ev_poss"] >= MIN_PANEL_POSS]
    panel = panel.sort_values(["player_id", "season_year"]).reset_index(drop=True)
    print(f"Panel: {len(panel)} player-seasons "
          f"({n0 - len(panel)} ghost/trivial appearances dropped)")

    curves = pd.read_csv(CURVES_PATH).dropna()
    drift_o = lambda a: float(np.interp(a, curves["age"], curves["d_orapm"]))
    drift_d = lambda a: float(np.interp(a, curves["age"], curves["d_drapm"]))

    # scoreboard: predicted (pre-observation) state vs that season's evidence
    firsts = panel.groupby("player_id")["season_year"].min().rename("fy")

    def score(f, min_year=None, max_year=None):
        """primary (>=1000 poss, star-weighted) + fringe (200-1000) boards."""
        j = f.merge(panel[["player_id", "season_year", "ev_o", "ev_d",
                           "ev_poss"]], on=["player_id", "season_year"])
        # exclude each player's first season (prediction == prior init)
        j = j.merge(firsts, on="player_id")
        j = j[j["season_year"] > j["fy"]]
        if min_year is not None:
            j = j[j["season_year"] >= min_year]
        if max_year is not None:
            j = j[j["season_year"] <= max_year]
        j["pred"] = j["pred_o"] + j["pred_d"]
        j["evid"] = j["ev_o"] + j["ev_d"]
        main_ = j[j["ev_poss"] >= MIN_EVID_POSS]
        fr = j[(j["ev_poss"] >= FRINGE_LO) & (j["ev_poss"] < FRINGE_HI)]
        s = wcorr(main_["pred"], main_["evid"], main_["ev_poss"])
        sf = wcorr(fr["pred"], fr["evid"], fr["ev_poss"])
        bias = (fr["pred"] - fr["evid"]).mean()
        return s, sf, bias, len(main_), len(fr)

    best = None
    for q in Q_GRID:
        for c in C_GRID:
            for sb in SB_GRID:
                f = run_filter(panel, drift_o, drift_d, q, c, sb)
                s, sf, bias, n, nf = score(f, max_year=2018)
                st, sft, biast, nt, nft = score(f, min_year=2019)
                print(f"  q={q:<5} c={c:<7.0f} sb={sb:<3}: "
                      f"dev {s:.4f} (n={n}); 2019+ {st:.4f} (n={nt})",
                      flush=True)
                if best is None or s > best[0]:
                    best = (s, q, c, sb, f)
    s, q, c, sb, f = best
    print(f"\nBest: q={q} c={c} sb={sb} -> predictive wcorr {s:.4f}")

    # stage 2: exposure-dependent level shift below the prior's fit range.
    # Guard: primary board must hold (>= legacy - 0.002); among survivors
    # minimize |fringe bias| (the margin backtest cares about levels).
    _, sf0, bias0, _, nf = score(f, max_year=2018)
    print(f"\nExposure shift (legacy fringe wcorr {sf0:.4f}, "
          f"bias {bias0:+.2f}, n={nf}):")
    best2 = (s, sf0, abs(bias0), 0.0, 0.0, f)
    for shift in SHIFT_GRID:
        for fp in FULL_POSS_GRID:
            if shift == 0.0:
                continue
            f2 = run_filter(panel, drift_o, drift_d, q, c, sb,
                            shift_total=shift, full_poss=fp)
            s2, sf2, bias2, n2, _ = score(f2, max_year=2018)
            tag = ""
            if s2 >= s - 0.002 and abs(bias2) < best2[2]:
                best2 = (s2, sf2, abs(bias2), shift, fp, f2)
                tag = "  <-"
            print(f"  shift={shift:<4} full={fp:<6.0f}: primary {s2:.4f}  "
                  f"fringe {sf2:.4f}  bias {bias2:+.2f}{tag}", flush=True)
    s, sf, _, shift, fp, f = best2
    print(f"\nSelected: shift={shift} full_poss={fp} -> "
          f"primary {s:.4f}, fringe {sf:.4f}")
    st, sft, biast, nt, nft = score(f, min_year=2019)
    print(f"2019+ confirmation: primary {st:.4f}, fringe {sft:.4f}, "
          f"fringe bias {biast:+.2f} (n={nt}/{nft})")

    names = load_player_names()
    f["player_name"] = f["player_id"].map(names)
    f["prior_model"] = PRIOR_MODEL
    f["evidence_model"] = EVIDENCE_MODEL
    f["pred_total"] = f["pred_o"] + f["pred_d"]
    f["filt_total"] = f["filt_o"] + f["filt_d"]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    f.to_parquet(OUT_DIR / "kalman_states.parquet", index=False)
    f.to_csv(OUT_DIR / "kalman_states.csv", index=False)
    print(f"Wrote {len(f)} rows to {OUT_DIR / 'kalman_states.parquet'}")

    f = f.merge(panel[["player_id", "season_year", "ev_poss"]],
                on=["player_id", "season_year"], how="left")
    top = f[(f["season_year"] == 2025) & (f["ev_poss"] >= 3000)] \
        .nlargest(12, "filt_total")
    print("\nTop 12 filtered states, 2025-26 (>=3000 poss):")
    txt = top[["player_name", "filt_o", "filt_d", "filt_total",
               "filt_var_o"]].round(2).to_string(index=False)
    enc = sys.stdout.encoding or "utf-8"
    print(txt.encode(enc, errors="replace").decode(enc))


if __name__ == "__main__":
    main()
