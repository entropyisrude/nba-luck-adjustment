"""Phase 3 of the all-in-one metric: the Bayesian combination (metric v0).

Prior-informed RAPM: the Phase 1 multi-year decayed ridge is re-solved with
each player's shrinkage center set to the denominator-aware atomic box prior,

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
from build_rapm_target import (load_player_names, season_label,
                               DECAY_HALFLIFE_DAYS, WINDOW_DAYS)
from counted_production_design import (exposure_arrays, load_design,
                                       normal_eqs)

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PRIOR_PATH = METRIC_DATA / "priors" / "box_prior_atomic_denominator.parquet"
PRIOR_MODEL = "atomic_denominator"
EVIDENCE_MODEL = "canonical_counted_possessions_v1"
KALMAN_PATH = METRIC_DATA / "kalman" / "kalman_states.parquet"
TARGET_PATH = METRIC_DATA / "targets" / "rapm_target_hl550.parquet"
BBREF_DIR = METRIC_DATA / "benchmarks" / "bbref_advanced"
RAPTOR_PATH = METRIC_DATA / "benchmarks" / "historical_RAPTOR_by_player.csv"
OUT_DIR = METRIC_DATA / "metric"

ALPHA_GRID = [500, 2000, 4000, 8000]
EVID_ALPHA = 150          # single-season evidence solves (validation only)
MIN_EVID_POSS = 1000
DEV_EVID_END = 2018       # posterior strength selection; 2019+ untouched


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--centers", choices=["box", "kalman"], default="box",
                    help="ridge centers: LOSO box prior (v0) or Kalman "
                         "one-step states (v1 candidate: dynamics-aware "
                         "centers + joint solve's team-sum consistency), "
                         "with box-prior fallback")
    args = ap.parse_args()

    design = load_design()
    X, y, poss = design["X"], design["y"], design["poss"]
    players, pidx, P = design["players"], design["pidx"], design["P"]
    print(f"Design: {X.shape[0]:,} directed observations, {P} players "
          f"(centers: {args.centers}; canonical counted evidence)")

    dates = design["dates"]
    syears = design["seasons"]
    names = load_player_names()

    prior = pd.read_parquet(PRIOR_PATH)
    prior_o = {(int(r.pid), int(r.season_year)): r.loso_o
               for r in prior.itertuples(index=False) if pd.notna(r.loso_o)}
    prior_d = {(int(r.pid), int(r.season_year)): r.loso_d
               for r in prior.itertuples(index=False) if pd.notna(r.loso_d)}
    print(f"Priors loaded for {len(prior_o)} player-seasons")
    if args.centers == "kalman":
        ks = pd.read_parquet(KALMAN_PATH)
        # one-step-ahead state: honest for season t (info through t-1)
        kal_o = {(int(r.player_id), int(r.season_year)): r.pred_o
                 for r in ks.itertuples(index=False) if pd.notna(r.pred_o)}
        kal_d = {(int(r.player_id), int(r.season_year)): r.pred_d
                 for r in ks.itertuples(index=False) if pd.notna(r.pred_d)}
        print(f"Kalman centers loaded for {len(kal_o)} player-seasons")
        prior_o = {**prior_o, **kal_o}
        prior_d = {**prior_d, **kal_d}

    results = []
    evid_rows = []
    for sy in range(1996, 2026):
        sel = syears == sy
        if not sel.any():
            continue
        end = dates[sel].max()
        age_days = (end - dates).astype("timedelta64[D]").astype(float)
        row_weight = poss * np.exp(-np.log(2) * age_days / DECAY_HALFLIFE_DAYS)
        row_weight[(age_days < 0) | (age_days > WINDOW_DAYS)] = 0.0
        XtX, Xty, used = normal_eqs(X, y, row_weight)

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

        raw_season, w_on = exposure_arrays(
            design, sy, end, DECAY_HALFLIFE_DAYS, WINDOW_DAYS)
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
                   "prior_model": PRIOR_MODEL,
                   "evidence_model": EVIDENCE_MODEL,
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
    evid_path = METRIC_DATA / "evidence_season_canonical_counted.parquet"
    evid.to_parquet(evid_path, index=False)
    print(f"Wrote {len(evid)} single-season evidence rows to {evid_path}")

    # ---------------- predictive scoreboard --------------------------------
    ev_next = evid.rename(columns={"season_year": "next_year"})
    out["next_year"] = out["season_year"] + 1
    j = out.merge(ev_next, on=["player_id", "next_year"])
    j = j[j["evid_poss"] >= MIN_EVID_POSS]

    def wc(frame, col):
        w = frame["evid_poss"]
        a, b = frame[col], frame["evid"]
        am, bm = np.average(a, weights=w), np.average(b, weights=w)
        cov = np.average((a - am) * (b - bm), weights=w)
        return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                             * np.average((b - bm) ** 2, weights=w))

    j["prior_total"] = j["prior_o"] + j["prior_d"]
    dev = j[j.next_year <= DEV_EVID_END]
    test = j[j.next_year > DEV_EVID_END]
    print(f"\nPredicting next-season single-season RAPM evidence "
          f"(n={len(j)}, >= {MIN_EVID_POSS} poss):")
    print(f"  atomic prior only: dev {wc(dev, 'prior_total'):.3f}; "
          f"2019+ {wc(test, 'prior_total'):.3f}")
    for alpha in ALPHA_GRID:
        print(f"  posterior alpha={alpha:<5}: dev {wc(dev, f'm{alpha}'):.3f}; "
              f"2019+ {wc(test, f'm{alpha}'):.3f}")

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

    best = max(ALPHA_GRID, key=lambda a: wc(dev, f"m{a}"))
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
    stem = "metric_v0" if args.centers == "box" else "metric_v1_kcenter"
    out.to_parquet(OUT_DIR / f"{stem}.parquet", index=False)
    out.to_csv(OUT_DIR / f"{stem}.csv", index=False)
    print(f"\nWrote {len(out)} rows to {OUT_DIR / f'{stem}.parquet'}")

    for season in ("2025-26", "2015-16"):
        top = out[(out["target_season"] == season) & (out["poss_season"] > 3000)]
        top = top.nlargest(12, "metric")
        print(f"\nTop 12 metric v0, {season}:")
        txt = top[["player_name", "metric_o", "metric_d", "metric", "poss_season"]] \
            .round(2).to_string(index=False)
        enc = sys.stdout.encoding or "utf-8"
        print(txt.encode(enc, errors="replace").decode(enc))


if __name__ == "__main__":
    main()
