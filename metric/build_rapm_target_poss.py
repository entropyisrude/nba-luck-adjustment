"""Possession-denominated RAPM target — replaces the seconds/24 stint
approximation with COUNTED possessions per stint per side.

Architecture note: possessions within a stint share the same ten players,
so per-possession design rows are identical within a (stint, side); their
sufficient statistics are the side's summed points and possession COUNT.
This build therefore keeps the entire validated stint pipeline from
build_rapm_target.prepare() — chaining, official-box calibration, 3PT
(_adj) + FT/mid-range luck — and changes only the denominator/weights:

    y_side  = side_pts_adj / n_side x 100      (was: / (seconds/24))
    w_side  = n_side x decay                   (was: seconds/24 x decay)

with n_side from metric/count_stint_possessions.py (PBP possession field
2019+; validated event state machine pre-2019). Rows whose side has no
counted possessions get weight 0 (reported, never silent).

Everything else matches build_rapm_target.py: 550-day half-life, 6-yr
window, alphas {150,500,2000}, unpenalized home-court column, sandwich
SEs at alpha=500. SEs are emitted RAW (calibration = 1.0): the stint-era
SE_CALIBRATION factors do not transfer — run the game-block bootstrap
against THIS target before trusting the SE scale.

NOTE ON SCALE: poss_season / w_poss now count possessions on floor for
BOTH sides (n_home + n_away while on court) — about 1.68x the old
seconds/24 numbers (2x pairs, minus the ~19% overbooking). Downstream
possession thresholds must be reinterpreted accordingly.

Output: nba-metric-data/targets/rapm_target_poss_hl550.parquet (+csv).
The production stint target is untouched; swap only after joint review.

Usage: python metric/build_rapm_target_poss.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_rapm_target import (prepare, assemble_design, load_player_names,
                               season_label, DECAY_HALFLIFE_DAYS,
                               WINDOW_DAYS, ALPHAS, SE_ALPHA)

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
COUNTS = METRIC_DATA / "stint_possession_counts.parquet"
OUT_DIR = METRIC_DATA / "targets"


def main() -> None:
    st_raw = prepare()
    st, X, _y_old, players, hidx, aidx = assemble_design(st_raw)
    P = len(players)
    n = len(st)

    counts = pd.read_parquet(COUNTS)
    st["gid_n"] = st["game_id"].astype(str).str.lstrip("0")
    st = st.merge(counts[["gid_n", "stint_index", "n_home", "n_away",
                          "pts_home", "pts_away"]],
                  on=["gid_n", "stint_index"], how="left")
    miss = st["n_home"].isna()
    print(f"stints without walker rows: {int(miss.sum())} "
          f"of {n} ({miss.mean() * 100:.2f}%) -> weight 0")
    for c in ("n_home", "n_away", "pts_home", "pts_away"):
        st[c] = st[c].fillna(0.0)

    # ATOMICITY CHECK: with points travelling with their possession, a
    # zero-count side must be zero-point by construction
    zh = (st["n_home"] == 0) & (st["pts_home"] > 0)
    za = (st["n_away"] == 0) & (st["pts_away"] > 0)
    print(f"atomicity violations (n=0, pts>0): home {int(zh.sum())}, "
          f"away {int(za.sum())} (should be 0)")

    # LUCK: the prepared stints' (_adj - raw) delta is pure luck
    # (3PT+FT+MR; chaining and calibration shift raw and adj equally and
    # cancel). Spread it over the walker's possessions at lineup-pair
    # level within each game — solve-exact (identical design rows).
    hk = pd.Series(
        [",".join(map(str, r)) for r in
         np.sort(st[[f"home_p{i}" for i in range(1, 6)]].to_numpy(), 1)],
        index=st.index)
    ak = pd.Series(
        [",".join(map(str, r)) for r in
         np.sort(st[[f"away_p{i}" for i in range(1, 6)]].to_numpy(), 1)],
        index=st.index)
    st["_luck_h"] = st["home_pts_adj"] - st["home_pts"]
    st["_luck_a"] = st["away_pts_adj"] - st["away_pts"]
    grp = st.groupby(["gid_n", hk, ak])
    for side in ("h", "a"):
        lg = grp[f"_luck_{side}"].transform("sum")
        ng = grp[f"n_{'home' if side == 'h' else 'away'}"].transform("sum")
        nn = st[f"n_{'home' if side == 'h' else 'away'}"]
        share = np.where(ng > 0, lg * nn / ng.replace(0, np.nan), 0.0)
        st[f"pts_adj_{side}"] = (st[f"pts_{'home' if side == 'h' else 'away'}"]
                                 + np.nan_to_num(share))
    lost_luck = float(st.loc[grp["n_home"].transform("sum") == 0,
                             "_luck_h"].abs().sum()
                      + st.loc[grp["n_away"].transform("sum") == 0,
                               "_luck_a"].abs().sum())
    print(f"luck dropped on zero-possession lineup-pairs: "
          f"{lost_luck:.0f} abs pts")

    # per-side rows with the walker's own points: pts and n are the SAME
    # possessions, so per-side denominators are safe (bounded y, no seam)
    n_home = st["n_home"].to_numpy(dtype=float)
    n_away = st["n_away"].to_numpy(dtype=float)
    n_rows = np.empty(2 * n)
    n_rows[0::2] = n_home
    n_rows[1::2] = n_away
    y = np.zeros(2 * n)
    hp = st["pts_adj_h"].to_numpy(dtype=float)
    ap = st["pts_adj_a"].to_numpy(dtype=float)
    y[0::2] = np.divide(hp, n_home, out=np.zeros(n),
                        where=n_home > 0) * 100.0
    y[1::2] = np.divide(ap, n_away, out=np.zeros(n),
                        where=n_away > 0) * 100.0

    dates = st["date"].to_numpy()
    sy_arr = (st["season_year"].to_numpy() if "season_year" in st.columns
              else (pd.to_datetime(st["date"]).dt.year
                    - (pd.to_datetime(st["date"]).dt.month < 10)).to_numpy())
    is_po = st["is_playoff"].to_numpy() == 1
    name_map = load_player_names()

    pen = np.ones(2 * P + 1)
    pen[2 * P] = 0.0

    results = []
    for sy in range(1996, 2026):
        label = season_label(sy)
        sel_season = sy_arr == sy
        if not sel_season.any():
            continue
        end = dates[sel_season].max()
        age_days = (end - dates).astype("timedelta64[D]").astype(float)
        decay = np.exp(-np.log(2) * age_days / DECAY_HALFLIFE_DAYS)
        decay[(age_days < 0) | (age_days > WINDOW_DAYS)] = 0.0
        decay_rows = np.repeat(decay, 2)
        w_rows = n_rows * decay_rows
        used = w_rows > 0

        Xs = X[used]
        ys = y[used]
        ws = w_rows[used]
        ybar = np.average(ys, weights=ws)
        Xw = Xs.multiply(np.sqrt(ws)[:, None]).tocsr()
        yw = (ys - ybar) * np.sqrt(ws)
        XtX = (Xw.T @ Xw).toarray()
        Xty = Xw.T @ yw

        # sandwich SEs at SE_ALPHA (RAW: recalibrate via bootstrap)
        A = XtX + SE_ALPHA * np.diag(pen)
        beta0 = np.linalg.solve(A, Xty)
        hca = float(beta0[2 * P])
        resid = (ys - ybar) - Xs @ beta0
        dsel = decay_rows[used]
        nsel = n_rows[used]
        c_hat = float((dsel * resid ** 2 * nsel).sum() / dsel.sum())
        Xm = Xs.multiply(np.sqrt(ws ** 2 / nsel)[:, None]).tocsr()
        M = (Xm.T @ Xm).toarray()
        A_inv = np.linalg.inv(A)
        Cov = A_inv @ M @ A_inv
        Cov *= c_hat
        dg = np.diag(Cov)
        cov_od = Cov[np.arange(P), P + np.arange(P)]
        se_o_arr = np.sqrt(np.maximum(dg[:P], 0.0))
        se_d_arr = np.sqrt(np.maximum(dg[P:2 * P], 0.0))
        se_t_arr = np.sqrt(np.maximum(dg[:P] + dg[P:2 * P]
                                      - 2.0 * cov_od, 0.0))
        del A, A_inv, M, Xm, Cov

        # per-player bookkeeping at ROW level (both sides of the floor)
        w_on = np.zeros(P)
        raw_season = np.zeros(P)
        po_poss = np.zeros(P)
        used_st = used.reshape(-1, 2)
        for parity, (o_of_row, d_of_row, nn) in enumerate(
                [(hidx, aidx, n_home), (aidx, hidx, n_away)]):
            row_used = used_st[:, parity]
            wsel = (nn * decay)[row_used]
            nposs = nn[row_used]
            in_s = sel_season[row_used]
            po_s = is_po[row_used]
            for k in range(5):
                for idxs in (o_of_row[row_used, k],
                             d_of_row[row_used, k]):
                    np.add.at(w_on, idxs, wsel)
                    np.add.at(raw_season, idxs,
                              np.where(in_s, nposs, 0.0))
                    np.add.at(po_poss, idxs, np.where(po_s, wsel, 0.0))

        for alpha in ALPHAS:
            beta = np.linalg.solve(XtX + alpha * np.diag(pen), Xty)
            O = beta[:P]
            D = beta[P:2 * P]
            om, dm = O.mean(), D.mean()
            active = raw_season > 0
            for i in np.nonzero(active)[0]:
                p = int(players[i])
                results.append({
                    "target_season": label, "player_id": p,
                    "player_name": name_map.get(p, str(p)), "alpha": alpha,
                    "orapm": round(O[i] - om, 3),
                    "drapm": round(-(D[i] - dm), 3),
                    "rapm": round((O[i] - om) - (D[i] - dm), 3),
                    "w_poss": round(float(w_on[i]), 1),
                    "poss_season": round(float(raw_season[i]), 1),
                    "po_share": round(float(po_poss[i] / w_on[i])
                                      if w_on[i] > 0 else 0.0, 4),
                    "se_o": round(float(se_o_arr[i]), 3)
                            if alpha == SE_ALPHA else None,
                    "se_d": round(float(se_d_arr[i]), 3)
                            if alpha == SE_ALPHA else None,
                    "se_rapm": round(float(se_t_arr[i]), 3)
                               if alpha == SE_ALPHA else None,
                })
        n_season = n_rows[np.repeat(sel_season, 2) & used].sum()
        g_season = st.loc[sel_season, "gid_n"].nunique()
        ppg = n_season / (2 * g_season) if g_season else 0.0
        print(f"  {label}: {int(used.sum())} rows in window, "
              f"{int((raw_season > 0).sum())} active, c_hat={c_hat:.0f}, "
              f"hca={hca:+.2f}, poss/side/game={ppg:.1f}")

    out = pd.DataFrame(results)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pq = OUT_DIR / f"rapm_target_poss_hl{DECAY_HALFLIFE_DAYS}.parquet"
    out.to_parquet(pq, index=False)
    out.to_csv(pq.with_suffix(".csv"), index=False)
    print(f"Wrote {len(out)} rows to {pq}")

    top = out[(out["target_season"] == "2025-26") & (out["alpha"] == 500)
              & (out["w_poss"] > 6000)].nlargest(15, "rapm")
    enc = sys.stdout.encoding or "utf-8"
    txt = top[["player_name", "orapm", "drapm", "rapm",
               "w_poss"]].to_string(index=False)
    print("\nTop 15, 2025-26, alpha=500, min 6000 weighted poss "
          "(new both-sides scale):")
    print(txt.encode(enc, errors="replace").decode(enc))


if __name__ == "__main__":
    main()
