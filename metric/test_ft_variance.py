"""FT-variance denoising experiment (same protocol as garbage time).

Variant: stint points additionally adjusted for free-throw luck
(build_ft_adjust.py) on top of the existing 3PT adjustment. Judged on the
fixed-target board: variant season-t evidence predicting BASELINE t+1
evidence (possession-weighted, both >=1000 poss), plus self-autocorr.

RESULT (2026-07-11): POSITIVE — SHIPPED. Fixed-target board 0.4271 ->
0.4311, self-autocorr 0.4394 -> 0.4454 (59.6% of stints carry an
adjustment, mean |luck| 0.37 pts/stint). FT luck was real unremoved
noise; production prepare() now applies ft_stint_adjust.parquet.

Usage: python metric/test_ft_variance.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_metric_v0 import build_design, normal_eqs
from build_rapm_target import prepare

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
FT_ADJ = METRIC_DATA / "ft_stint_adjust.parquet"
EVID_ALPHA = 150
MIN_POSS = 1000


def evidence(st: pd.DataFrame) -> pd.DataFrame:
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
        psel = poss[used]; insel = sel[used]
        for k in range(5):
            for idxs in (hidx[used, k], aidx[used, k]):
                np.add.at(raw, idxs, np.where(insel, psel, 0.0))
        act = raw > 0
        rows.append(pd.DataFrame({
            "player_id": players[act].astype(int), "season_year": sy,
            "evid": (O[act] - om) - (D[act] - dm), "poss": raw[act]}))
        print(f"  {sy}", flush=True)
    return pd.concat(rows, ignore_index=True)


def wcorr(a, b, w):
    am, bm = np.average(a, weights=w), np.average(b, weights=w)
    cov = np.average((a - am) * (b - bm), weights=w)
    return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                         * np.average((b - bm) ** 2, weights=w))


def main() -> None:
    st = prepare(apply_ft=False)   # this script applies its own FT variant
    st["date"] = pd.to_datetime(st["date"])
    adj = pd.read_parquet(FT_ADJ)
    st = st.merge(adj, on=["game_id", "stint_index"], how="left")
    st[["ft_luck_home", "ft_luck_away"]] = st[["ft_luck_home", "ft_luck_away"]].fillna(0.0)
    cov = (st["ft_luck_home"].ne(0) | st["ft_luck_away"].ne(0)).mean()
    print(f"stints with FT adjustment: {cov:.1%}; "
          f"|luck| per stint mean {np.abs(st.ft_luck_home).mean()+np.abs(st.ft_luck_away).mean():.3f}")

    print("baseline evidence...")
    base = evidence(st)
    ftv = st.copy()
    ftv["home_pts_adj"] = ftv["home_pts_adj"] - ftv["ft_luck_home"]
    ftv["away_pts_adj"] = ftv["away_pts_adj"] - ftv["ft_luck_away"]
    print("FT-adjusted evidence...")
    ftev = evidence(ftv)

    tgt = base.copy()
    tgt["prev"] = tgt["season_year"] - 1
    print(f"\n{'variant':>12}  {'fixed-target next-season':>25}  {'self-autocorr':>14}")
    for name, df in [("baseline", base), ("ft_adjust", ftev)]:
        j = df.merge(tgt.rename(columns={"evid": "target", "poss": "tposs"}),
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
        print(f"{name:>12}  {r_fixed:>25.4f}  {r_self:>14.4f}  (n={len(j)})")


if __name__ == "__main__":
    main()
