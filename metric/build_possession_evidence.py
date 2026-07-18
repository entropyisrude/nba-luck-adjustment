"""Build independent single-season RAPM evidence from counted possessions.

This is the chronologically honest validation outcome for the experimental
possession target and box prior.  Each season is solved from that season's
trusted games only; no observation from the season being evaluated appears
in a prior-season training target.

Output: nba-metric-data/evidence_poss_season.parquet
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_rapm_target import prepare, assemble_design

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
COUNTS = METRIC_DATA / "stint_possession_counts.parquet"
OUT = METRIC_DATA / "evidence_poss_season.parquet"
EVID_ALPHA = 150.0


def load_counted_design():
    st, X, _y, players, hidx, aidx = assemble_design(prepare())
    counts = pd.read_parquet(COUNTS)
    st["gid_n"] = st["game_id"].astype(str).str.lstrip("0")
    st = st.merge(counts[["gid_n", "stint_index", "n_home", "n_away",
                          "pts_home", "pts_away"]],
                  on=["gid_n", "stint_index"], how="left")
    for c in ("n_home", "n_away", "pts_home", "pts_away"):
        st[c] = st[c].fillna(0.0)

    # The luck adjustment is constant for identical lineup pairs, so moving
    # its game/lineup-pair total onto the counted rows is solve-exact.
    hk = pd.Series([",".join(map(str, r)) for r in
                    np.sort(st[[f"home_p{i}" for i in range(1, 6)]].to_numpy(), 1)],
                   index=st.index)
    ak = pd.Series([",".join(map(str, r)) for r in
                    np.sort(st[[f"away_p{i}" for i in range(1, 6)]].to_numpy(), 1)],
                   index=st.index)
    st["_luck_h"] = st["home_pts_adj"] - st["home_pts"]
    st["_luck_a"] = st["away_pts_adj"] - st["away_pts"]
    grp = st.groupby(["gid_n", hk, ak])
    for short, long in (("h", "home"), ("a", "away")):
        luck = grp[f"_luck_{short}"].transform("sum")
        ng = grp[f"n_{long}"].transform("sum")
        nn = st[f"n_{long}"]
        share = np.where(ng > 0, luck * nn / ng.replace(0, np.nan), 0.0)
        st[f"pts_adj_{short}"] = st[f"pts_{long}"] + np.nan_to_num(share)

    n = len(st)
    n_rows = np.empty(2 * n)
    n_rows[0::2] = st["n_home"].to_numpy(float)
    n_rows[1::2] = st["n_away"].to_numpy(float)
    y = np.zeros(2 * n)
    for parity, (pc, nc) in enumerate((("pts_adj_h", "n_home"),
                                       ("pts_adj_a", "n_away"))):
        pts = st[pc].to_numpy(float)
        nn = st[nc].to_numpy(float)
        y[parity::2] = np.divide(pts, nn, out=np.zeros(n), where=nn > 0) * 100
    return st, X, y, n_rows, players, hidx, aidx


def main() -> None:
    st, X, y, n_rows, players, hidx, aidx = load_counted_design()
    P = len(players)
    sy_arr = st["season_year"].to_numpy()
    rows = []
    for sy in sorted(np.unique(sy_arr)):
        stint_sel = sy_arr == sy
        row_sel = np.repeat(stint_sel, 2) & (n_rows > 0)
        if not row_sel.any():
            continue

        # Restrict the solve to players who actually appear in trusted rows
        # this season.  This is algebraically identical to carrying thousands
        # of all-zero historical columns and is much faster and safer.
        used_st = row_sel.reshape(-1, 2)
        active_global = np.unique(np.concatenate([
            hidx[used_st[:, 0]].ravel(), aidx[used_st[:, 0]].ravel(),
            hidx[used_st[:, 1]].ravel(), aidx[used_st[:, 1]].ravel(),
        ]))
        cols = np.r_[active_global, P + active_global, 2 * P]
        Xs = X[row_sel][:, cols].tocsr()
        ys = y[row_sel]
        ws = n_rows[row_sel]
        ybar = np.average(ys, weights=ws)
        Xw = Xs.multiply(np.sqrt(ws)[:, None]).tocsr()
        yw = (ys - ybar) * np.sqrt(ws)
        XtX = (Xw.T @ Xw).toarray()
        Xty = Xw.T @ yw
        pen = np.ones(len(cols)); pen[-1] = 0.0
        beta = np.linalg.solve(XtX + EVID_ALPHA * np.diag(pen), Xty)
        A = len(active_global)
        O, D = beta[:A], beta[A:2 * A]
        O -= O.mean(); D -= D.mean()

        raw = np.zeros(P)
        for parity, nn in enumerate((st["n_home"].to_numpy(float),
                                     st["n_away"].to_numpy(float))):
            use = used_st[:, parity]
            for k in range(5):
                for idx in (hidx[use, k], aidx[use, k]):
                    np.add.at(raw, idx, nn[use])
        for j, gi in enumerate(active_global):
            rows.append({"player_id": int(players[gi]), "season_year": int(sy),
                         "ev_o": float(O[j]), "ev_d": float(-D[j]),
                         "ev_poss": float(raw[gi])})
        print(f"  evidence {sy}: {len(active_global)} players, "
              f"{int(row_sel.sum())} side-stints", flush=True)

    out = pd.DataFrame(rows)
    out.to_parquet(OUT, index=False)
    out.to_csv(OUT.with_suffix(".csv"), index=False)
    print(f"Wrote {len(out)} rows to {OUT}")


if __name__ == "__main__":
    main()
