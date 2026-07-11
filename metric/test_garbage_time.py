"""Garbage-time denoising experiment (protocol fixed before results).

Hypothesis: possessions played with the outcome decided (big margin, little
time) carry less information about true ability — effort collapses, benches
stat-pad — so down-weighting them should make single-season evidence MORE
repeatable and MORE predictive.

Leverage rules (margin m at stint start vs time remaining, effort-based
tiers — not win-probability, which misfires on close-late situations):
  A (moderate): garbage if rem<12min & m>=25 | rem<9min & m>=20
                | rem<6min & m>=15 | rem<3min & m>=10
  B (strict):   garbage if rem<6min & m>=20  | rem<3min & m>=13

Variants: baseline w=1; A with w_g in {0.25, 0}; B with w_g 0.25.

Scoreboard (fixed target so variants compete fairly): each variant's
season-t evidence predicting the BASELINE evidence at t+1, possession-
weighted, pooled across seasons; plus within-variant year-over-year
autocorrelation. A denoiser must beat baseline on the fixed target.

RESULT (2026-07-11): NEGATIVE — do not ship. Garbage share: A 6.0% of
possessions, B 3.2%. Fixed-target board: baseline 0.4271 vs A_w25 0.4266 /
A_w0 0.4241 / B_w25 0.4259 — every variant equal-or-worse; full deletion
worst (sample loss beats noise gain). Interpretation: the joint solve is
already structurally robust to garbage time (the lineups on the floor are
controlled for — the classic complaints target box stats and raw plus-
minus); the residual effort distortion on ~6% of possessions is too small
to beat the cost of down-weighting real sample.

Usage: python metric/test_garbage_time.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_metric_v0 import build_design, normal_eqs
from build_rapm_target import prepare

EVID_ALPHA = 150
MIN_POSS = 1000
REG_SECONDS = 48 * 60.0


def leverage_flags(st: pd.DataFrame) -> pd.DataFrame:
    m = (st["start_home_score"] - st["start_away_score"]).abs()
    mid = st["start_elapsed"] + st["seconds"] / 2.0
    rem = np.maximum(REG_SECONDS - mid, 30.0) / 60.0   # minutes; OT clamps small
    st = st.copy()
    st["garbA"] = (((rem < 12) & (m >= 25)) | ((rem < 9) & (m >= 20))
                   | ((rem < 6) & (m >= 15)) | ((rem < 3) & (m >= 10)))
    st["garbB"] = (((rem < 6) & (m >= 20)) | ((rem < 3) & (m >= 13)))
    return st


def main() -> None:
    st = prepare(adjustments=())   # historical experiment ran pre-FT-adjustment
    st["date"] = pd.to_datetime(st["date"])
    st = leverage_flags(st)
    st, X, y, poss, players, pidx, hidx, aidx = build_design(st)
    P = len(players)
    syears = st["season_year"].to_numpy()
    gA = st["garbA"].to_numpy()
    gB = st["garbB"].to_numpy()
    tot = poss.sum()
    print(f"{len(st)} stints; garbage poss share: A {poss[gA].sum()/tot:.2%}, "
          f"B {poss[gB].sum()/tot:.2%}")

    VARIANTS = {
        "baseline": np.ones(len(st)),
        "A_w25": np.where(gA, 0.25, 1.0),
        "A_w0":  np.where(gA, 0.0, 1.0),
        "B_w25": np.where(gB, 0.25, 1.0),
    }

    results = {}
    for name, lw in VARIANTS.items():
        rows = []
        for sy in range(1996, 2026):
            sel = syears == sy
            if not sel.any():
                continue
            w_ev = np.where(sel, poss * lw, 0.0)
            XtX, Xty, used = normal_eqs(X, y, w_ev)
            be = np.linalg.solve(XtX + EVID_ALPHA * np.eye(2 * P), Xty)
            O, D = be[:P], be[P:]
            om, dm = O.mean(), D.mean()
            raw = np.zeros(P)
            psel = poss[used]; insel = sel[used]
            for k in range(5):
                for idxs in (hidx[used, k], aidx[used, k]):
                    np.add.at(raw, idxs, np.where(insel, psel, 0.0))
            act = raw > 0
            rows.append(pd.DataFrame({
                "player_id": players[act].astype(int), "season_year": sy,
                "evid": (O[act] - om) - (D[act] - dm), "poss": raw[act]}))
            print(f"  {name} {sy}", flush=True)
        results[name] = pd.concat(rows, ignore_index=True)
        results[name].to_parquet(
            Path(r"C:\Users\Dave\Downloads\nba-metric-data") /
            f"gt_evidence_{name}.parquet", index=False)

    def wcorr(a, b, w):
        am, bm = np.average(a, weights=w), np.average(b, weights=w)
        cov = np.average((a - am) * (b - bm), weights=w)
        return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                             * np.average((b - bm) ** 2, weights=w))

    base = results["baseline"].copy()
    base["prev"] = base["season_year"] - 1
    print(f"\n{'variant':>10}  {'fixed-target next-season':>25}  {'self-autocorr':>14}")
    for name, df in results.items():
        j = df.merge(base.rename(columns={"evid": "target", "poss": "tposs"}),
                     left_on=["player_id", "season_year"],
                     right_on=["player_id", "prev"], suffixes=("", "_t"))
        j = j[(j["poss"] >= MIN_POSS) & (j["tposs"] >= MIN_POSS)]
        r_fixed = wcorr(j["evid"], j["target"], j["tposs"])
        s = df.copy(); s["prev"] = s["season_year"] - 1
        js = df.merge(s.rename(columns={"evid": "nxt", "poss": "nposs"}),
                      left_on=["player_id", "season_year"],
                      right_on=["player_id", "prev"], suffixes=("", "_s"))
        js = js[(js["poss"] >= MIN_POSS) & (js["nposs"] >= MIN_POSS)]
        r_self = wcorr(js["evid"], js["nxt"], np.minimum(js["poss"], js["nposs"]))
        print(f"{name:>10}  {r_fixed:>25.4f}  {r_self:>14.4f}  (n={len(j)})")


if __name__ == "__main__":
    main()
