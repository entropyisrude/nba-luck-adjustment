"""Mid-range variance denoising experiment, with O/D-split boards.

Variant: stint points additionally adjusted for mid-range shooting luck
(build_midrange_adjust.py) ON TOP of the shipped FT adjustment. Unlike
free throws, mid-range outcomes are partly defense-owned (contests), so
the fixed-target board is scored separately for offensive and defensive
evidence: the adjustment must not buy offensive precision by deleting
defensive signal.

Fixed target = the production (FT-adjusted) evidence at t+1.

RESULT (2026-07-11): POSITIVE on ALL boards — SHIPPED. Total 0.4339 ->
0.4367, O 0.6522 -> 0.6585, D 0.5508 -> 0.5649 (the D board improved
MOST — defenders were being credited/blamed for opponents' mid-range
luck, and that noise outweighed the contest-quality signal in the
residuals), self-autocorr 0.4454 -> 0.4573. 65% of stints adjusted.
prepare() default now applies both "ft" and "mr" adjustments.

Usage: python metric/test_midrange_variance.py
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
MR_ADJ = METRIC_DATA / "midrange_stint_adjust.parquet"
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
            "ev_o": O[act] - om, "ev_d": -(D[act] - dm), "poss": raw[act]}))
        print(f"  {sy}", flush=True)
    df = pd.concat(rows, ignore_index=True)
    df["evid"] = df["ev_o"] + df["ev_d"]
    return df


def wcorr(a, b, w):
    am, bm = np.average(a, weights=w), np.average(b, weights=w)
    cov = np.average((a - am) * (b - bm), weights=w)
    return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                         * np.average((b - bm) ** 2, weights=w))


def main() -> None:
    st = prepare(adjustments=("ft",))   # baseline = FT only (this script adds MR)
    st["date"] = pd.to_datetime(st["date"])
    adj = pd.read_parquet(MR_ADJ)
    st = st.merge(adj, on=["game_id", "stint_index"], how="left")
    st[["mr_luck_home", "mr_luck_away"]] = \
        st[["mr_luck_home", "mr_luck_away"]].fillna(0.0)
    print(f"stints with MR adjustment: "
          f"{(st.mr_luck_home.ne(0) | st.mr_luck_away.ne(0)).mean():.1%}")

    print("baseline (FT-adjusted) evidence...")
    base = evidence(st)
    mrv = st.copy()
    mrv["home_pts_adj"] = mrv["home_pts_adj"] - mrv["mr_luck_home"]
    mrv["away_pts_adj"] = mrv["away_pts_adj"] - mrv["mr_luck_away"]
    print("mid-range-adjusted evidence...")
    mrev = evidence(mrv)

    tgt = base.copy()
    tgt["prev"] = tgt["season_year"] - 1
    print(f"\n{'variant':>12}  {'total':>8}  {'O':>8}  {'D':>8}  {'self-ac':>8}")
    for name, df in [("baseline", base), ("mr_adjust", mrev)]:
        j = df.merge(tgt.rename(columns={"evid": "t", "ev_o": "t_o",
                                         "ev_d": "t_d", "poss": "tposs"}),
                     left_on=["player_id", "season_year"],
                     right_on=["player_id", "prev"], suffixes=("", "_x"))
        j = j[(j["poss"] >= MIN_POSS) & (j["tposs"] >= MIN_POSS)]
        rt = wcorr(j["evid"], j["t"], j["tposs"])
        ro = wcorr(j["ev_o"], j["t_o"], j["tposs"])
        rd = wcorr(j["ev_d"], j["t_d"], j["tposs"])
        s = df.copy(); s["prev"] = s["season_year"] - 1
        js = df.merge(s.rename(columns={"evid": "nxt", "poss": "nposs"}),
                      left_on=["player_id", "season_year"],
                      right_on=["player_id", "prev"], suffixes=("", "_s"))
        js = js[(js["poss"] >= MIN_POSS) & (js["nposs"] >= MIN_POSS)]
        ra = wcorr(js["evid"], js["nxt"], np.minimum(js["poss"], js["nposs"]))
        print(f"{name:>12}  {rt:8.4f}  {ro:8.4f}  {rd:8.4f}  {ra:8.4f}  (n={len(j)})")


if __name__ == "__main__":
    main()
