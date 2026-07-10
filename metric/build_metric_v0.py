"""Phase 3 of the all-in-one metric: the Bayesian combination (metric v0).

Prior-informed RAPM: the Phase 1 multi-year decayed ridge is re-solved with
each player's shrinkage center set to his Phase 2 box prior instead of zero,

    (X'WX + aI) beta = X'Wy + a * beta0

so thin samples land near the box prior and large samples earn their own
number -- the shrinkage-in-proportion-to-sample-size arithmetic falls out of
the normal equations. beta0 uses the LEAVE-ONE-SEASON-OUT prior variant to
avoid in-sample leakage; players without a prior center at 0 (league
average, since rows are centered).

Alpha (the prior precision) is chosen on an honest predictive scoreboard:
for each season S, how well does the season-S metric predict season-S+1
SINGLE-SEASON RAPM evidence (future-only, no window overlap), weighted by
S+1 possessions? The same scoreboard scores prior-only, observation-only,
BBRef BPM and 538 RAPTOR for comparison.

Outputs (OUT_DIR): metric_v0.parquet/csv -- per player-season O/D/total for
the selected alpha plus components (obs-only alpha150/500, box prior).

Usage: python metric/build_metric_v0.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_rapm_target import (prepare, load_player_names, season_label,
                               HCOLS, ACOLS, DECAY_HALFLIFE_DAYS, WINDOW_DAYS,
                               MIN_SECONDS)

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PRIOR_PATH = METRIC_DATA / "priors" / "box_prior.parquet"
TARGET_PATH = METRIC_DATA / "targets" / "rapm_target_hl550.parquet"
BBREF_DIR = METRIC_DATA / "benchmarks" / "bbref_advanced"
RAPTOR_PATH = METRIC_DATA / "benchmarks" / "historical_RAPTOR_by_player.csv"
OUT_DIR = METRIC_DATA / "metric"

ALPHA_GRID = [150, 500, 1000, 2000]
EVID_ALPHA = 150          # single-season evidence solves (validation only)
MIN_EVID_POSS = 1000


def build_design(st: pd.DataFrame):
    st = st.dropna(subset=HCOLS + ACOLS).copy()
    st = st[st["seconds"] >= MIN_SECONDS].reset_index(drop=True)
    st["poss"] = np.maximum(st["seconds"].to_numpy() / 24.0, 0.1)
    st["season_year"] = st["date"].dt.year - (st["date"].dt.month < 10)

    players = np.unique(st[HCOLS + ACOLS].to_numpy().astype(int).ravel())
    pidx = {p: i for i, p in enumerate(players)}
    P = len(players)
    lookup = np.vectorize(pidx.get)
    hidx = lookup(st[HCOLS].to_numpy().astype(int))
    aidx = lookup(st[ACOLS].to_numpy().astype(int))

    n = len(st)
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
    y = np.empty(2 * n)
    y[0::2] = st["home_pts_adj"].to_numpy() / poss * 100.0
    y[1::2] = st["away_pts_adj"].to_numpy() / poss * 100.0
    return st, X, y, poss, players, pidx, hidx, aidx


def normal_eqs(X, y, w_st):
    used = w_st > 0
    row_mask = np.repeat(used, 2)
    Xs = X[row_mask]
    ys = y[row_mask]
    ws = np.repeat(w_st[used], 2)
    ybar = np.average(ys, weights=ws)
    Xw = Xs.multiply(np.sqrt(ws)[:, None]).tocsr()
    yw = (ys - ybar) * np.sqrt(ws)
    return (Xw.T @ Xw).toarray(), Xw.T @ yw, used


def main() -> None:
    st = prepare()
    st["date"] = pd.to_datetime(st["date"])
    st, X, y, poss, players, pidx, hidx, aidx = build_design(st)
    P = len(players)
    n = len(st)
    print(f"Design: {n} stints, {P} players")

    dates = st["date"].to_numpy()
    syears = st["season_year"].to_numpy()
    is_po = st["is_playoff"].to_numpy() == 1
    names = load_player_names()

    prior = pd.read_parquet(PRIOR_PATH)
    prior_o = {(int(r.pid), int(r.season_year)): r.loso_o
               for r in prior.itertuples(index=False) if pd.notna(r.loso_o)}
    prior_d = {(int(r.pid), int(r.season_year)): r.loso_d
               for r in prior.itertuples(index=False) if pd.notna(r.loso_d)}
    print(f"Priors loaded for {len(prior_o)} player-seasons")

    results = []
    evid_rows = []
    for sy in range(1996, 2026):
        sel = syears == sy
        if not sel.any():
            continue
        end = dates[sel].max()
        age_days = (end - dates).astype("timedelta64[D]").astype(float)
        w_st = poss * np.exp(-np.log(2) * age_days / DECAY_HALFLIFE_DAYS)
        w_st[(age_days < 0) | (age_days > WINDOW_DAYS)] = 0.0
        XtX, Xty, used = normal_eqs(X, y, w_st)

        beta0 = np.zeros(2 * P)
        n_prior = 0
        for i, p in enumerate(players):
            po = prior_o.get((int(p), sy))
            pdv = prior_d.get((int(p), sy))
            if po is not None:
                beta0[i] = po
                n_prior += 1
            if pdv is not None:
                beta0[P + i] = -pdv   # drapm is sign-flipped vs the D coefficient

        # season possession bookkeeping
        raw_season = np.zeros(P)
        w_on = np.zeros(P)
        psel = poss[used]
        insel = sel[used]
        wsel = w_st[used]
        for k in range(5):
            for idxs in (hidx[used, k], aidx[used, k]):
                np.add.at(raw_season, idxs, np.where(insel, psel, 0.0))
                np.add.at(w_on, idxs, wsel)
        active = raw_season > 0

        sol = {}
        for alpha in ALPHA_GRID:
            beta = np.linalg.solve(XtX + alpha * np.eye(2 * P), Xty + alpha * beta0)
            O, D = beta[:P], beta[P:]
            om, dm = O.mean(), D.mean()
            sol[alpha] = (O - om, -(D - dm))

        label = season_label(sy)
        for i in np.nonzero(active)[0]:
            p = int(players[i])
            row = {"season_year": sy, "target_season": label, "player_id": p,
                   "player_name": names.get(p, str(p)),
                   "poss_season": round(float(raw_season[i]), 1),
                   "w_poss": round(float(w_on[i]), 1),
                   "prior_o": round(float(beta0[i]), 3),
                   "prior_d": round(float(-beta0[P + i]), 3)}
            for alpha in ALPHA_GRID:
                o, d = sol[alpha]
                row[f"m{alpha}_o"] = round(float(o[i]), 3)
                row[f"m{alpha}_d"] = round(float(d[i]), 3)
                row[f"m{alpha}"] = round(float(o[i] + d[i]), 3)
            results.append(row)

        # single-season evidence (validation): this season only, no decay, no prior
        w_ev = np.where(sel, poss, 0.0)
        XtXe, Xtye, used_e = normal_eqs(X, y, w_ev)
        be = np.linalg.solve(XtXe + EVID_ALPHA * np.eye(2 * P), Xtye)
        Oe, De = be[:P], be[P:]
        oem, dem = Oe.mean(), De.mean()
        for i in np.nonzero(active)[0]:
            evid_rows.append({"season_year": sy, "player_id": int(players[i]),
                              "evid": float((Oe[i] - oem) - (De[i] - dem)),
                              "evid_poss": float(raw_season[i])})
        print(f"  {label}: {int(active.sum())} players "
              f"({n_prior} with priors)", flush=True)

    out = pd.DataFrame(results)
    evid = pd.DataFrame(evid_rows)

    # ---------------- predictive scoreboard --------------------------------
    ev_next = evid.rename(columns={"season_year": "next_year"})
    out["next_year"] = out["season_year"] + 1
    j = out.merge(ev_next, on=["player_id", "next_year"])
    j = j[j["evid_poss"] >= MIN_EVID_POSS]

    def wc(col):
        w = j["evid_poss"]
        a, b = j[col], j["evid"]
        am, bm = np.average(a, weights=w), np.average(b, weights=w)
        cov = np.average((a - am) * (b - bm), weights=w)
        return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                             * np.average((b - bm) ** 2, weights=w))

    j["prior_total"] = j["prior_o"] + j["prior_d"]
    print(f"\nPredicting next-season single-season RAPM evidence "
          f"(n={len(j)}, >= {MIN_EVID_POSS} poss):")
    print(f"  box prior only:      {wc('prior_total'):.3f}")
    for alpha in ALPHA_GRID:
        print(f"  posterior alpha={alpha:<5}: {wc(f'm{alpha}'):.3f}")

    # obs-only baselines from Phase 1 outputs
    tgt = pd.read_parquet(TARGET_PATH)
    for alpha in (150, 500):
        t = tgt[tgt["alpha"] == alpha][["player_id", "target_season", "rapm"]]
        t = t.rename(columns={"rapm": f"obs{alpha}"})
        j = j.merge(t, on=["player_id", "target_season"], how="left")
        jj = j.dropna(subset=[f"obs{alpha}"])
        w = jj["evid_poss"]
        a, b = jj[f"obs{alpha}"], jj["evid"]
        am, bm = np.average(a, weights=w), np.average(b, weights=w)
        cov = np.average((a - am) * (b - bm), weights=w)
        r = cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                          * np.average((b - bm) ** 2, weights=w))
        print(f"  obs-only alpha={alpha:<4}:  {r:.3f}")

    # external benchmarks on the same scoreboard
    bb = []
    for pth in sorted(BBREF_DIR.glob("advanced_*.csv")):
        yr = int(pth.stem.split("_")[1])
        d = pd.read_csv(pth)
        if "BPM" not in d.columns:
            continue
        d = d[["Player", "BPM", "MP"]].copy()
        d["MP"] = pd.to_numeric(d["MP"], errors="coerce")
        d = d.sort_values("MP", ascending=False).drop_duplicates("Player")
        d["season_year"] = yr - 1
        bb.append(d)
    bb = pd.concat(bb, ignore_index=True)
    bb["BPM"] = pd.to_numeric(bb["BPM"], errors="coerce")
    jb = j.merge(bb, left_on=["player_name", "season_year"],
                 right_on=["Player", "season_year"]).dropna(subset=["BPM"])
    rap = pd.read_csv(RAPTOR_PATH)
    rap["season_year"] = rap["season"] - 1
    jr = j.merge(rap[["player_name", "season_year", "raptor_total", "poss"]],
                 on=["player_name", "season_year"]).dropna(subset=["raptor_total"])

    def wc2(frame, col):
        w = frame["evid_poss"]
        a, b = frame[col], frame["evid"]
        am, bm = np.average(a, weights=w), np.average(b, weights=w)
        cov = np.average((a - am) * (b - bm), weights=w)
        return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                             * np.average((b - bm) ** 2, weights=w))

    best = max(ALPHA_GRID, key=lambda a: wc(f"m{a}"))
    print(f"\nBenchmarks on matched rows:")
    print(f"  vs BPM   (n={len(jb)}): ours {wc2(jb, f'm{best}'):.3f}  BPM {wc2(jb, 'BPM'):.3f}")
    print(f"  vs RAPTOR(n={len(jr)}): ours {wc2(jr, f'm{best}'):.3f}  RAPTOR {wc2(jr, 'raptor_total'):.3f}")
    print(f"\nSelected alpha: {best}")

    # era stability of predictive power
    j["era5"] = (j["season_year"] // 5) * 5
    for e in sorted(j["era5"].unique()):
        je = j[j["era5"] == e]
        if len(je) > 300:
            print(f"  {e}s: posterior {wc2(je, f'm{best}'):.3f} (n={len(je)})")

    out["metric_o"] = out[f"m{best}_o"]
    out["metric_d"] = out[f"m{best}_d"]
    out["metric"] = out[f"m{best}"]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT_DIR / "metric_v0.parquet", index=False)
    out.to_csv(OUT_DIR / "metric_v0.csv", index=False)
    print(f"\nWrote {len(out)} rows to {OUT_DIR / 'metric_v0.parquet'}")

    for season in ("2025-26", "2015-16"):
        top = out[(out["target_season"] == season) & (out["poss_season"] > 3000)]
        top = top.nlargest(12, "metric")
        print(f"\nTop 12 metric v0, {season}:")
        print(top[["player_name", "metric_o", "metric_d", "metric", "poss_season"]]
              .round(2).to_string(index=False))


if __name__ == "__main__":
    main()
