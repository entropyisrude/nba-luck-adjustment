"""
Multi-year decayed RAPM targets split by first-chance vs. second-chance
points -- the full-history version of test_firstchance_rapm.py's diagnostic
(2026-07-14), which found dreb_75's entire correlation with defensive RAPM
runs through preventing second-chance points (r=0.321 full target -> r=0.018
first-chance-only), not general defensive quality.

Reuses the exact same possession-weighted, decayed sparse design as
build_rapm_target.py's build_targets() -- same players, same lineups, same
recency weighting -- but solves it against TWO different y vectors (first-
chance points, second-chance points) instead of the shipped one (total
points). The design matrix and its XtX are identical regardless of which y
you regress on, so both targets are solved off ONE XtX factorization per
season rather than rebuilding it twice -- the expensive part of this
otherwise mirrors the shipped pipeline's cost.

Only alpha=500 is solved (that's TARGET_ALPHA in build_box_prior.py -- the
one that actually feeds Phase 2's box-prior fit; the shipped target's
150/2000 variants aren't needed for this).

COVERAGE: build_secondchance_adjust.py only covers 2019-20+ (see its
docstring for why). Stints from games without second-chance coverage are
EXCLUDED from this solve entirely (zero-weighted), not treated as having
zero second-chance points -- and target seasons whose 6-year window is
mostly uncovered are skipped rather than solved on a thin, biased sample.
MIN_TARGET_SEASON below is the earliest target season with enough covered
weight to trust.

Outputs (OUT_DIR):
  rapm_target_firstchance.parquet
  rapm_target_secondchance.parquet
  both with the same schema as targets/rapm_target_hl550.parquet.

Usage: python metric/build_rapm_target_fcsc.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse
from scipy.linalg import lu_factor, lu_solve

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_rapm_target import (prepare, load_player_names, season_label,
                               HCOLS, ACOLS, DECAY_HALFLIFE_DAYS, WINDOW_DAYS,
                               MIN_SECONDS)

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
SC_ADJUST = METRIC_DATA / "secondchance_stint_adjust.parquet"
OUT_DIR = METRIC_DATA / "targets"
ALPHA = 500
SC_COVERAGE_START = pd.Timestamp("2019-10-01")   # start of 2019-20 season; see build_secondchance_adjust.py
MIN_TARGET_SEASON = 2021   # first target season whose 6yr window is mostly covered


def build_fcsc_targets(st: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    st = st.dropna(subset=HCOLS + ACOLS).copy()
    st = st[st["seconds"] >= MIN_SECONDS].reset_index(drop=True)
    st["poss"] = np.maximum(st["seconds"].to_numpy() / 24.0, 0.1)
    st["season_year"] = st["date"].dt.year - (st["date"].dt.month < 10)

    # second-chance coverage is a clean season-level cutoff (possession field
    # is 100% null before 2019-20, 0% null from 2019-20 on -- verified, not a
    # partial/noisy boundary), so it's safe to filter stints by date rather
    # than by presence in the adjustment file (which only has rows for
    # stints with >=1 scoring play, not all covered stints -- a stint with
    # zero scoring plays in a COVERED game is a genuine zero, not missing).
    n0 = len(st)
    st = st[st["date"] >= SC_COVERAGE_START].reset_index(drop=True)
    print(f"restricted to 2019-20+ (second-chance coverage): {n0} -> {len(st)} stints")

    sc = pd.read_parquet(SC_ADJUST)
    sc["stint_index"] = sc["stint_index"].astype(int)
    st["stint_index"] = st["stint_index"].astype(int)
    st = st.merge(sc, on=["game_id", "stint_index"], how="left")
    st[["sc_pts_home", "sc_pts_away"]] = st[["sc_pts_home", "sc_pts_away"]].fillna(0.0)
    st["fc_pts_home"] = st["home_pts_adj"] - st["sc_pts_home"]
    st["fc_pts_away"] = st["away_pts_adj"] - st["sc_pts_away"]
    print(f"Usable stints: {len(st)} ({st['sc_pts_home'].gt(0).sum() + st['sc_pts_away'].gt(0).sum():,} "
          f"with second-chance points)")

    players = np.unique(st[HCOLS + ACOLS].to_numpy().astype(int).ravel())
    pidx = {p: i for i, p in enumerate(players)}
    P = len(players)
    print(f"Players: {P}")

    n = len(st)
    lookup = np.vectorize(pidx.get)
    hidx = lookup(st[HCOLS].to_numpy().astype(int))
    aidx = lookup(st[ACOLS].to_numpy().astype(int))

    rows, cols, vals = [], [], []
    r = np.arange(n)
    for k in range(5):
        rows += [2 * r, 2 * r]
        cols += [hidx[:, k], P + aidx[:, k]]
        vals += [np.ones(n), np.ones(n)]
        rows += [2 * r + 1, 2 * r + 1]
        cols += [aidx[:, k], P + hidx[:, k]]
        vals += [np.ones(n), np.ones(n)]
    X = sparse.csr_matrix((np.concatenate(vals),
                           (np.concatenate(rows), np.concatenate(cols))),
                          shape=(2 * n, 2 * P))

    poss = st["poss"].to_numpy()
    y_fc = np.empty(2 * n)
    y_fc[0::2] = st["fc_pts_home"].to_numpy() / poss * 100.0
    y_fc[1::2] = st["fc_pts_away"].to_numpy() / poss * 100.0
    y_sc = np.empty(2 * n)
    y_sc[0::2] = st["sc_pts_home"].to_numpy() / poss * 100.0
    y_sc[1::2] = st["sc_pts_away"].to_numpy() / poss * 100.0

    dates = st["date"].to_numpy()
    name_map = load_player_names()

    results = {"fc": [], "sc": []}
    for sy in range(MIN_TARGET_SEASON, 2026):
        label = season_label(sy)
        sel_season = (st["season_year"].to_numpy() == sy)
        if not sel_season.any():
            continue
        end = dates[sel_season].max()
        age_days = (end - dates).astype("timedelta64[D]").astype(float)
        w_st = poss * np.exp(-np.log(2) * age_days / DECAY_HALFLIFE_DAYS)
        w_st[(age_days < 0) | (age_days > WINDOW_DAYS)] = 0.0
        used = w_st > 0
        row_mask = np.repeat(used, 2)

        Xs = X[row_mask]
        ws = np.repeat(w_st[used], 2)
        Xw = Xs.multiply(np.sqrt(ws)[:, None]).tocsr()
        XtX = (Xw.T @ Xw).toarray()
        A = XtX + ALPHA * np.eye(2 * P)
        # factor once, solve twice (fc, sc) -- the expensive step is shared
        lu, piv = lu_factor(A)

        w_on = np.zeros(P)
        raw_season = np.zeros(P)
        po_poss = np.zeros(P)
        wsel = w_st[used]
        psel = poss[used]
        in_season = sel_season[used]
        is_po = st["is_playoff"].to_numpy()[used] == 1
        for k in range(5):
            for idxs in (hidx[used, k], aidx[used, k]):
                np.add.at(w_on, idxs, wsel)
                np.add.at(raw_season, idxs, np.where(in_season, psel, 0.0))
                np.add.at(po_poss, idxs, np.where(is_po, wsel, 0.0))
        active = raw_season > 0

        for key, yv in (("fc", y_fc), ("sc", y_sc)):
            ys = yv[row_mask]
            ybar = np.average(ys, weights=ws)
            yw = (ys - ybar) * np.sqrt(ws)
            Xty = Xw.T @ yw
            beta = lu_solve((lu, piv), Xty)
            O = beta[:P]
            D = beta[P:]
            om, dm = O.mean(), D.mean()
            for i in np.nonzero(active)[0]:
                p = int(players[i])
                results[key].append({
                    "target_season": label, "player_id": p,
                    "player_name": name_map.get(p, str(p)), "alpha": ALPHA,
                    "orapm": round(O[i] - om, 3),
                    "drapm": round(-(D[i] - dm), 3),
                    "rapm": round((O[i] - om) - (D[i] - dm), 3),
                    "w_poss": round(float(w_on[i]), 1),
                    "poss_season": round(float(raw_season[i]), 1),
                    "po_share": round(float(po_poss[i] / w_on[i]) if w_on[i] > 0 else 0.0, 4),
                })
        print(f"  {label}: {int(used.sum())} stints in window, {int(active.sum())} active players")

    return pd.DataFrame(results["fc"]), pd.DataFrame(results["sc"])


def main() -> None:
    st = prepare(adjustments=("ft", "mr"))
    fc, sc = build_fcsc_targets(st)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fc.to_parquet(OUT_DIR / "rapm_target_firstchance.parquet", index=False)
    sc.to_parquet(OUT_DIR / "rapm_target_secondchance.parquet", index=False)
    print(f"\nWrote {len(fc)} first-chance rows, {len(sc)} second-chance rows to {OUT_DIR}")


if __name__ == "__main__":
    main()
