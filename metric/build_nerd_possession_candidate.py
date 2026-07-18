"""Build and chronologically validate the counted-possession NERD candidate.

The ridge is centered on the denominator-aware rolling atomic box priors whose coefficients
were learned only from earlier target seasons.  Alpha is selected on future
single-season evidence through 2018, then reported unchanged for 2019+.
Nothing in this script writes the production metric or site artifacts.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_possession_evidence import load_counted_design
from build_rapm_target import (DECAY_HALFLIFE_DAYS, WINDOW_DAYS, season_label,
                               load_player_names)
from build_box_prior import wcorr

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PRIOR = (ROOT / "outputs" / "contextual_causal"
         / "rolling_prior_atomic_denominator_poss.parquet")
PRIOR_MODEL = "atomic_denominator"
EVID = DATA / "evidence_poss_season.parquet"
OUT = ROOT / "outputs" / "contextual_causal"
ALPHAS = [150, 500, 1000, 2000, 4000, 8000]
MIN_EVID = 1700.0


def main() -> None:
    st, X, y, n_rows, players, hidx, aidx = load_counted_design()
    P = len(players); names = load_player_names()
    pri = pd.read_parquet(PRIOR)
    po = {(int(r.pid), int(r.season_year)): float(r.po)
          for r in pri.itertuples() if pd.notna(r.po)}
    pd_ = {(int(r.pid), int(r.season_year)): float(r.pd)
           for r in pri.itertuples() if pd.notna(r.pd)}
    dates = st.date.to_numpy(); sya = st.season_year.to_numpy()
    results = []
    for sy in range(2004, 2026):
        current = sya == sy
        if not current.any():
            continue
        end = dates[current].max()
        age = (end - dates).astype("timedelta64[D]").astype(float)
        decay = np.exp(-np.log(2) * age / DECAY_HALFLIFE_DAYS)
        decay[(age < 0) | (age > WINDOW_DAYS)] = 0
        w = n_rows * np.repeat(decay, 2); use = w > 0
        used_st = use.reshape(-1, 2)
        active = np.unique(np.concatenate([
            hidx[used_st[:, 0]].ravel(), aidx[used_st[:, 0]].ravel(),
            hidx[used_st[:, 1]].ravel(), aidx[used_st[:, 1]].ravel()]))
        cols = np.r_[active, P + active, 2 * P]
        Xs = X[use][:, cols].tocsr(); ys = y[use]; ws = w[use]
        ybar = np.average(ys, weights=ws)
        Xw = Xs.multiply(np.sqrt(ws)[:, None]).tocsr()
        XtX = (Xw.T @ Xw).toarray()
        Xty = Xw.T @ ((ys - ybar) * np.sqrt(ws))
        A = len(active); pen = np.ones(2*A + 1); pen[-1] = 0
        beta0 = np.zeros(2*A + 1); n_prior = 0
        for j, gi in enumerate(active):
            pid = int(players[gi])
            if (pid, sy) in po:
                beta0[j] = po[(pid, sy)]; n_prior += 1
            if (pid, sy) in pd_:
                beta0[A+j] = -pd_[(pid, sy)]

        # Current-season counted exposure, both sides of the floor.
        raw = np.zeros(P)
        cur_rows = (np.repeat(current, 2) & (n_rows > 0)).reshape(-1, 2)
        for parity, nn in enumerate((st.n_home.to_numpy(float),
                                     st.n_away.to_numpy(float))):
            use_st = cur_rows[:, parity]
            for k in range(5):
                for idx in (hidx[use_st, k], aidx[use_st, k]):
                    np.add.at(raw, idx, nn[use_st])
        current_global = np.flatnonzero(raw > 0)
        local = {gi: j for j, gi in enumerate(active)}
        solutions = {}
        for alpha in ALPHAS:
            mat = XtX + alpha * np.diag(pen)
            b = np.linalg.solve(mat, Xty + alpha * pen * beta0)
            O, D = b[:A], b[A:2*A]
            O -= O.mean(); D -= D.mean()
            solutions[alpha] = (O, -D)
        for gi in current_global:
            j = local[gi]; pid = int(players[gi])
            row = {"season_year": sy, "target_season": season_label(sy),
                   "player_id": pid, "player_name": names.get(pid, str(pid)),
                   "poss_season": raw[gi], "prior_o": beta0[j],
                   "prior_d": -beta0[A+j]}
            for alpha, (O, D) in solutions.items():
                row[f"m{alpha}_o"] = O[j]; row[f"m{alpha}_d"] = D[j]
                row[f"m{alpha}"] = O[j] + D[j]
            results.append(row)
        print(f"  {season_label(sy)}: {len(current_global)} active, "
              f"{n_prior} centered", flush=True)

    out = pd.DataFrame(results)
    ev = pd.read_parquet(EVID)
    ev["metric_year"] = ev.season_year - 1
    j = out.merge(ev, left_on=["player_id", "season_year"],
                  right_on=["player_id", "metric_year"], suffixes=("", "_ev"))
    j = j[j.ev_poss >= MIN_EVID].copy()
    j["actual"] = j.ev_o + j.ev_d
    j["prior"] = j.prior_o + j.prior_d
    dev = j[j.season_year_ev <= 2018]
    test = j[j.season_year_ev >= 2019]
    print("\nNext-season counted-possession evidence")
    print(f"prior only: dev {wcorr(dev.prior, dev.actual, dev.ev_poss):.4f}; "
          f"2019+ {wcorr(test.prior, test.actual, test.ev_poss):.4f}")
    scoreboard = []
    for alpha in ALPHAS:
        dr = wcorr(dev[f"m{alpha}"], dev.actual, dev.ev_poss)
        tr = wcorr(test[f"m{alpha}"], test.actual, test.ev_poss)
        to = wcorr(test[f"m{alpha}_o"], test.ev_o, test.ev_poss)
        td = wcorr(test[f"m{alpha}_d"], test.ev_d, test.ev_poss)
        scoreboard.append({"alpha": alpha, "dev_total": dr,
                           "test_total": tr, "test_o": to, "test_d": td,
                           "n_dev": len(dev), "n_test": len(test)})
        print(f"alpha {alpha:4}: dev {dr:.4f}; 2019+ total {tr:.4f} "
              f"O {to:.4f} D {td:.4f}")
    best = max(scoreboard, key=lambda r: r["dev_total"])["alpha"]
    print(f"LOCKED ALPHA (selected pre-2019): {best}")
    out["nerd_o"] = out[f"m{best}_o"]
    out["nerd_d"] = out[f"m{best}_d"]
    out["nerd"] = out[f"m{best}"]
    out["prior_model"] = PRIOR_MODEL
    OUT.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT / "nerd_possession_candidate.parquet", index=False)
    out.to_csv(OUT / "nerd_possession_candidate.csv", index=False)
    pd.DataFrame(scoreboard).to_csv(OUT / "nerd_possession_scoreboard.csv",
                                    index=False)


if __name__ == "__main__":
    main()
