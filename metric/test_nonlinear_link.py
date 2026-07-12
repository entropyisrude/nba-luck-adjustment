"""Out-of-sample validation of lineup-sum nonlinearity (protocol first).

Finding to validate (2026-07-12, in-sample): actual scoring vs the additive
m4000 prediction bends — offense is super-additive in the good range and
collapses at both extremes (S-shape), defense is compressed at both ends
(diminishing returns). If real, a low-dimensional link function should fit
on pre-2019 data and hold on 2019+.

Model (weighted LS, offense rows):
  linear:  y ~ a + b(S_off - S_def) + h*home
  link:    y ~ a + b1*S_off + b2*S_off^2 + b3*S_off^3
               + c1*S_def + c2*S_def^2 + h*home

Checks:
  1. Train-fitted LINEAR residuals on TEST, bucketed by off/def-sum deciles
     (train-defined edges): do the curve shapes persist out-of-sample?
  2. Train-fitted LINK residuals on TEST: do the curves flatten?
  3. Everything split REGULAR SEASON vs PLAYOFFS (concentration hypothesis:
     saturation stronger in playoffs).
Writes link coefficients to nba-metric-data/nonlinear_link.json if the
curves validate (decided by reader, not automatically).

RESULT (2026-07-12): the original curves were an ERA ARTIFACT — a single
global intercept let the secular scoring rise (~+11/100 from train era to
2019+) masquerade as lineup-sum curvature (user caught it via the absurd
test offsets). Properly centered (season x RS/PO cells):
  * curve SHAPES do not transfer OOS (defensive "compression" shrinks to
    ~+/-0.25 whisper; offensive S-curve reverses sign between eras);
  * star-on-DEFENSE sub-additivity ~vanishes (-0.09/100) — the "Wemby
    saturates so Castle gets docked" mechanism is NOT supported;
  * star-on-OFFENSE sub-additivity survives, modest (-0.43/100 on 467k
    poss) — one-ball usage redundancy;
  * the robust keeper: PLAYOFF SLOPE COMPRESSION — talent gaps convert
    to margin at ~7.4% lower rate in playoffs (slope ratio PO/RS 0.926,
    replicated in both eras: 1.162/1.249 train, 1.201/1.277 test).
nonlinear_link.json deleted (fit under the bad spec). GAM-RAPM
re-attribution deprioritized: surviving nonlinearity too small.

Usage: python metric/test_nonlinear_link.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

MD = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
TRAIN_MAX = 2018


def main() -> None:
    st = pd.read_parquet(MD / "prepared_stints.parquet")
    st["sy"] = pd.to_datetime(st.date).dt.year - (pd.to_datetime(st.date).dt.month < 10)
    st["poss"] = np.maximum(st.seconds / 24, 0.1)
    H = st[[f"home_p{i}" for i in range(1, 6)]].to_numpy().astype(np.int64)
    A = st[[f"away_p{i}" for i in range(1, 6)]].to_numpy().astype(np.int64)
    sy = st.sy.to_numpy()
    poss = st.poss.to_numpy()
    po = st.is_playoff.to_numpy() == 1

    m = pd.read_parquet(MD / "metric/metric_v0.parquet")
    mo = {(int(r.player_id), int(r.season_year)): r.m4000_o for r in m.itertuples()}
    md = {(int(r.player_id), int(r.season_year)): r.m4000_d for r in m.itertuples()}
    lko = np.vectorize(lambda p, s: mo.get((p, s), -0.5))
    lkd = np.vectorize(lambda p, s: md.get((p, s), -0.5))
    ho = sum(lko(H[:, k], sy) for k in range(5)); hd = sum(lkd(H[:, k], sy) for k in range(5))
    ao = sum(lko(A[:, k], sy) for k in range(5)); ad = sum(lkd(A[:, k], sy) for k in range(5))

    y = np.concatenate([st.home_pts_adj.to_numpy() / poss * 100,
                        st.away_pts_adj.to_numpy() / poss * 100])
    OFF = np.concatenate([ho, ao]); DEF = np.concatenate([ad, hd])
    w = np.concatenate([poss, poss])
    home = np.concatenate([np.ones(len(st)), np.zeros(len(st))])
    seas = np.concatenate([sy, sy])
    is_po = np.concatenate([po, po])
    tr = seas <= TRAIN_MAX
    te = ~tr

    def wls(Xm, ym, wm):
        b, *_ = np.linalg.lstsq(Xm * np.sqrt(wm)[:, None], ym * np.sqrt(wm), rcond=None)
        return b

    X_lin = np.column_stack([np.ones(len(y)), OFF - DEF, home])
    X_lnk = np.column_stack([np.ones(len(y)), OFF, OFF**2, OFF**3,
                             DEF, DEF**2, home])
    b_lin = wls(X_lin[tr], y[tr], w[tr])
    b_lnk = wls(X_lnk[tr], y[tr], w[tr])
    r_lin = y - X_lin @ b_lin
    r_lnk = y - X_lnk @ b_lnk

    # decile edges from TRAIN
    def curve(vals, resid, mask, edges):
        q = np.clip(np.searchsorted(edges, vals[mask]), 0, 9)
        out = []
        for d in range(10):
            sel = q == d
            if w[mask][sel].sum() < 5000:
                out.append(None); continue
            out.append(np.average(resid[mask][sel], weights=w[mask][sel]))
        return out

    for varname, vals in [("OFF-sum", OFF), ("DEF-sum", DEF)]:
        edges = np.quantile(vals[tr], np.arange(1, 10) / 10)
        print(f"\n=== {varname} decile residual curves (per 100) ===")
        for lbl, mask in [("train RS", tr & ~is_po), ("train PO", tr & is_po),
                          ("test RS ", te & ~is_po), ("test PO ", te & is_po)]:
            c_lin = curve(vals, r_lin, mask, edges)
            fmt = lambda c: " ".join("  -- " if v is None else f"{v:+.2f}" for v in c)
            print(f"  {lbl} linear: {fmt(c_lin)}")
        for lbl, mask in [("test RS ", te & ~is_po), ("test PO ", te & is_po)]:
            c_l = curve(vals, r_lnk, mask, edges)
            print(f"  {lbl} LINK  : "
                  + " ".join("  -- " if v is None else f"{v:+.2f}" for v in c_l))

    # MSE reduction on test
    for lbl, mask in [("test RS", te & ~is_po), ("test PO", te & is_po)]:
        m0 = np.average(r_lin[mask]**2, weights=w[mask])
        m1 = np.average(r_lnk[mask]**2, weights=w[mask])
        print(f"{lbl}: weighted MSE linear {m0:.2f} -> link {m1:.2f} "
              f"({(m0-m1)/m0*1e4:.1f} bp)")

    out = {"train_max": TRAIN_MAX,
           "link_coefs": {"const": b_lnk[0], "off": b_lnk[1], "off2": b_lnk[2],
                          "off3": b_lnk[3], "def": b_lnk[4], "def2": b_lnk[5],
                          "home": b_lnk[6]}}
    (MD / "nonlinear_link.json").write_text(json.dumps(out, indent=2))
    print(f"\nlink coefficients -> {MD / 'nonlinear_link.json'}")


if __name__ == "__main__":
    main()
