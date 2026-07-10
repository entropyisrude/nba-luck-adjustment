"""Separate O/D ridge penalties for the prior-informed joint solve.

Engelmann (nba-adjusted-plus-minus-how-to-build) reports that players have
more influence on offense than defense, so offensive and defensive
coefficients deserve different penalization: defense (noisier attribution)
should shrink harder toward its prior. This re-runs the Phase 3 solve with
a penalty pair (alpha_o, alpha_d) instead of one alpha, selected on the
same honest scoreboard (predict next season's single-season evidence).

Baseline to beat: symmetric alpha=4000 -> 0.5257 (metric_v0).

Output: nba-metric-data/metric/metric_v1_od.parquet|csv (metric_* = best
pair) if the best pair beats the symmetric baseline; always prints the grid.

Usage: python metric/build_metric_od_alphas.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_metric_v0 import (build_design, normal_eqs, PRIOR_PATH,
                             EVID_ALPHA, MIN_EVID_POSS)
from build_rapm_target import (prepare, load_player_names, season_label,
                               DECAY_HALFLIFE_DAYS, WINDOW_DAYS)

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
OUT_DIR = METRIC_DATA / "metric"

# (alpha_o, alpha_d): symmetric baselines + defense-shrinks-harder pairs
# + one offense-shrinks-harder control
PAIRS = [(4000, 4000), (2000, 2000), (8000, 8000),
         (2000, 4000), (2000, 8000), (4000, 8000), (4000, 16000),
         (8000, 4000)]


def main() -> None:
    st = prepare()
    st["date"] = pd.to_datetime(st["date"])
    st, X, y, poss, players, pidx, hidx, aidx = build_design(st)
    P = len(players)
    print(f"Design: {len(st)} stints, {P} players")

    dates = st["date"].to_numpy()
    syears = st["season_year"].to_numpy()
    names = load_player_names()

    prior = pd.read_parquet(PRIOR_PATH)
    prior_o = {(int(r.pid), int(r.season_year)): r.loso_o
               for r in prior.itertuples(index=False) if pd.notna(r.loso_o)}
    prior_d = {(int(r.pid), int(r.season_year)): r.loso_d
               for r in prior.itertuples(index=False) if pd.notna(r.loso_d)}

    results, evid_rows = [], []
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
        for i, p in enumerate(players):
            po = prior_o.get((int(p), sy))
            pdv = prior_d.get((int(p), sy))
            if po is not None:
                beta0[i] = po
            if pdv is not None:
                beta0[P + i] = -pdv

        raw_season = np.zeros(P)
        psel = poss[used]
        insel = sel[used]
        for k in range(5):
            for idxs in (hidx[used, k], aidx[used, k]):
                np.add.at(raw_season, idxs, np.where(insel, psel, 0.0))
        active = raw_season > 0

        sol = {}
        for ao, ad in PAIRS:
            pen = np.concatenate([np.full(P, float(ao)), np.full(P, float(ad))])
            beta = np.linalg.solve(XtX + np.diag(pen), Xty + pen * beta0)
            O, D = beta[:P], beta[P:]
            sol[(ao, ad)] = (O - O.mean(), -(D - D.mean()))

        label = season_label(sy)
        for i in np.nonzero(active)[0]:
            row = {"season_year": sy, "target_season": label,
                   "player_id": int(players[i]),
                   "player_name": names.get(int(players[i]), str(players[i])),
                   "poss_season": round(float(raw_season[i]), 1)}
            for (ao, ad), (o, d) in sol.items():
                row[f"m{ao}_{ad}_o"] = round(float(o[i]), 3)
                row[f"m{ao}_{ad}_d"] = round(float(d[i]), 3)
                row[f"m{ao}_{ad}"] = round(float(o[i] + d[i]), 3)
            results.append(row)

        w_ev = np.where(sel, poss, 0.0)
        XtXe, Xtye, _ = normal_eqs(X, y, w_ev)
        be = np.linalg.solve(XtXe + EVID_ALPHA * np.eye(2 * P), Xtye)
        Oe, De = be[:P], be[P:]
        for i in np.nonzero(active)[0]:
            evid_rows.append({
                "season_year": sy, "player_id": int(players[i]),
                "ev_o": float(Oe[i] - Oe.mean()),
                "ev_d": float(-(De[i] - De.mean())),
                "evid_poss": float(raw_season[i])})
        print(f"  {label}: {int(active.sum())} players", flush=True)

    out = pd.DataFrame(results)
    evid = pd.DataFrame(evid_rows)
    evid["evid"] = evid["ev_o"] + evid["ev_d"]
    ev_next = evid.rename(columns={"season_year": "next_year"})
    out["next_year"] = out["season_year"] + 1
    j = out.merge(ev_next, on=["player_id", "next_year"])
    j = j[j["evid_poss"] >= MIN_EVID_POSS]

    def wc(col, tgt="evid"):
        w = j["evid_poss"]
        a, b = j[col], j[tgt]
        am, bm = np.average(a, weights=w), np.average(b, weights=w)
        cov = np.average((a - am) * (b - bm), weights=w)
        return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                             * np.average((b - bm) ** 2, weights=w))

    print(f"\nNext-season scoreboard (n={len(j)}):")
    best = None
    for ao, ad in PAIRS:
        s = wc(f"m{ao}_{ad}")
        so = wc(f"m{ao}_{ad}_o", "ev_o")
        sd = wc(f"m{ao}_{ad}_d", "ev_d")
        print(f"  ao={ao:<6} ad={ad:<6}: total {s:.4f}  O {so:.4f}  D {sd:.4f}")
        if best is None or s > best[0]:
            best = (s, ao, ad)
    s, ao, ad = best
    print(f"\nBest: ao={ao} ad={ad} -> {s:.4f} (symmetric-4000 baseline 0.5257)")

    out["metric_o"] = out[f"m{ao}_{ad}_o"]
    out["metric_d"] = out[f"m{ao}_{ad}_d"]
    out["metric"] = out[f"m{ao}_{ad}"]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    keep = [c for c in out.columns if not c.startswith("m") or c.startswith("metric")]
    out[keep].to_parquet(OUT_DIR / "metric_v1_od.parquet", index=False)
    out[keep].to_csv(OUT_DIR / "metric_v1_od.csv", index=False)
    print(f"Wrote {OUT_DIR / 'metric_v1_od.parquet'}")


if __name__ == "__main__":
    main()
