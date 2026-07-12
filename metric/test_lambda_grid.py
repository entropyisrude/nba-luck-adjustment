"""Lambda grid for the FT+mid-range luck removal (protocol fixed first).

How MUCH of the shooting-luck deviation should be removed? lambda scales
the per-stint luck subtraction: y = pts_adj_3pt - lambda x (ft+mr luck).

  * uniform lambda in {0, .25, .5, .75, 1.0}  (0 = 3PT-only; 1 = shipped)
  * asymmetric: offense coefficients harvested from a muted solve
    (lambda_off in {0, .25, .5}) + defense coefficients from the
    lambda=1 solve — the "defense fully denoised, offense partially"
    design. (One solve cannot do both: each possession's points enter
    the offense and defense equations identically.)

Boards: fixed target = lambda=0 evidence at t+1 (neutral), scored as
total / O-only / D-only, possession-weighted, both sides >=1000 poss.

RESULT (2026-07-12): uniform lambda=0.75 ADOPTED. Boards (total/O/D):
lam 0: .4271/.6451/.5452; .25: .4342/.6518/.5547; .5: .4380/.6557/.5611;
.75: .4379/.6564/.5642; 1.0: .4337/.6540/.5639. Full removal over-
corrects (real in-season form exists); D board flat .75-1.0 (opponent
variance = purest luck) which is why asymmetric O-muted/D-full combos
LOSE to uniform (O.5/D1 = .4366 < .4380): cross-solve incoherence costs
more than D=1 gains. LUCK_LAMBDA=0.75 in metric prepare() and site
rapm_luck.py.

Usage: python metric/test_lambda_grid.py
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
EVID_ALPHA = 150
MIN_POSS = 1000
LAMBDAS = [0.0, 0.25, 0.5, 0.75, 1.0]
ASYM = [(0.0, 1.0), (0.25, 1.0), (0.5, 1.0)]


def main() -> None:
    st = prepare(adjustments=())
    st["date"] = pd.to_datetime(st["date"])
    for name in ("ft_stint_adjust", "midrange_stint_adjust"):
        adj = pd.read_parquet(METRIC_DATA / f"{name}.parquet")
        st = st.merge(adj, on=["game_id", "stint_index"], how="left")
    st = st.fillna({c: 0.0 for c in ["ft_luck_home", "ft_luck_away",
                                     "mr_luck_home", "mr_luck_away"]})
    st["luck_h"] = st["ft_luck_home"] + st["mr_luck_home"]
    st["luck_a"] = st["ft_luck_away"] + st["mr_luck_away"]

    # design is lambda-invariant; build once, recompute y per lambda
    stf, X, y0, poss, players, pidx, hidx, aidx = build_design(st)
    P = len(players)
    syears = stf["season_year"].to_numpy()
    lh = stf["luck_h"].to_numpy() / poss * 100.0
    la = stf["luck_a"].to_numpy() / poss * 100.0

    def evidence(lam: float) -> pd.DataFrame:
        y = y0.copy()
        y[0::2] -= lam * lh
        y[1::2] -= lam * la
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
        df = pd.concat(rows, ignore_index=True)
        df["evid"] = df["ev_o"] + df["ev_d"]
        print(f"  lambda={lam} done", flush=True)
        return df

    results = {lam: evidence(lam) for lam in LAMBDAS}

    def wcorr(a, b, w):
        am, bm = np.average(a, weights=w), np.average(b, weights=w)
        cov = np.average((a - am) * (b - bm), weights=w)
        return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                             * np.average((b - bm) ** 2, weights=w))

    tgt = results[0.0].copy()
    tgt["prev"] = tgt["season_year"] - 1

    def boards(df):
        j = df.merge(tgt.rename(columns={"evid": "t", "ev_o": "t_o",
                                         "ev_d": "t_d", "poss": "tposs"}),
                     left_on=["player_id", "season_year"],
                     right_on=["player_id", "prev"], suffixes=("", "_x"))
        j = j[(j["poss"] >= MIN_POSS) & (j["tposs"] >= MIN_POSS)]
        return (wcorr(j["evid"], j["t"], j["tposs"]),
                wcorr(j["ev_o"], j["t_o"], j["tposs"]),
                wcorr(j["ev_d"], j["t_d"], j["tposs"]), len(j))

    print(f"\n{'variant':>16}  {'total':>8}  {'O':>8}  {'D':>8}")
    for lam in LAMBDAS:
        t, o, d, n = boards(results[lam])
        print(f"{'uniform '+str(lam):>16}  {t:8.4f}  {o:8.4f}  {d:8.4f}")
    for lo, ld in ASYM:
        a = results[lo][["player_id", "season_year", "ev_o"]].merge(
            results[ld][["player_id", "season_year", "ev_d", "poss"]],
            on=["player_id", "season_year"])
        a["evid"] = a["ev_o"] + a["ev_d"]
        t, o, d, n = boards(a)
        print(f"{'O'+str(lo)+'/D'+str(ld):>16}  {t:8.4f}  {o:8.4f}  {d:8.4f}")


if __name__ == "__main__":
    main()
