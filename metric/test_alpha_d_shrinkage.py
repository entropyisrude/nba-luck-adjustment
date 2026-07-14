"""
Test an ASYMMETRIC shrinkage alpha for the Phase 3 combination: hold
offense's shrinkage-to-box-prior strength fixed at the shipped value
(alpha_o=4000) and grid-search a separate, potentially lighter,
defense-specific alpha_d -- trusting the raw RAPM evidence more and the
(known-biased-toward-height/rebounding) box prior less for defense
specifically.

Motivated by two negative results this session: (1) decomposing the target
into first-chance/second-chance components and refitting two box priors
didn't help (metric/build_box_prior_fcsc.py), and (2) adding genuine
NBA-tracking closest-defender data as a box-prior feature didn't help
either (metric/build_closedef_stats.py test). Both suggest the problem
isn't which features feed the box prior -- it's how hard Phase 3 shrinks
toward it. This tests that directly: same box prior, same RAPM evidence,
just less pull toward the prior on the D side.

Mechanics: build_metric_v0.py's ridge-toward-prior solve currently uses one
scalar alpha for the whole 2P parameter vector,
    beta = solve(XtX + alpha*I, Xty + alpha*beta0)
This instead uses a diagonal precision matrix with alpha_o on the offense
block and alpha_d (gridded) on the defense block, so only D's pull toward
the prior changes. XtX/Xty are computed ONCE per target season (they don't
depend on alpha) and reused across the whole alpha_d grid -- same sharing
trick used in build_rapm_target_fcsc.py.

Scoped to target seasons 2020-2024 (evaluated against 2021-2025 evidence)
for tractability -- this is a diagnostic, not the production rebuild.

Checks, for each alpha_d:
  1. honest next-season D-evidence transfer (the metric that has to not get
     worse for this to be worth shipping)
  2. height / dreb_75 correlation of the resulting D value (the bias this
     is trying to reduce)

Usage: python metric/test_alpha_d_shrinkage.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_rapm_target import prepare, load_player_names, DECAY_HALFLIFE_DAYS, WINDOW_DAYS
from build_metric_v0 import build_design, normal_eqs, PRIOR_PATH, ALPHA_GRID

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
EVID_PATH = METRIC_DATA / "evidence_season.parquet"

ALPHA_O = 4000          # fixed, matches the shipped metric
ALPHA_D_GRID = [250, 500, 1000, 2000, 4000, 8000]
TARGET_SEASONS = range(2020, 2025)   # scored against 2021-2025 evidence


def wcorr(a, b, w) -> float:
    a, b, w = np.asarray(a, float), np.asarray(b, float), np.asarray(w, float)
    am, bm = np.average(a, weights=w), np.average(b, weights=w)
    cov = np.average((a - am) * (b - bm), weights=w)
    return cov / np.sqrt(np.average((a - am) ** 2, weights=w) * np.average((b - bm) ** 2, weights=w))


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

    all_results = []   # (alpha_d, season_year, player_id, d_value, poss_season)
    for sy in TARGET_SEASONS:
        sel = syears == sy
        if not sel.any():
            continue
        end = dates[sel].max()
        age_days = (end - dates).astype("timedelta64[D]").astype(float)
        w_st = poss * np.exp(-np.log(2) * age_days / DECAY_HALFLIFE_DAYS)
        w_st[(age_days < 0) | (age_days > WINDOW_DAYS)] = 0.0
        XtX, Xty, used = normal_eqs(X, y, w_st)   # shared across the whole alpha_d grid

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

        for alpha_d in ALPHA_D_GRID:
            diag = np.concatenate([np.full(P, ALPHA_O), np.full(P, alpha_d)])
            beta = np.linalg.solve(XtX + np.diag(diag), Xty + diag * beta0)
            D = beta[P:]
            dm = D.mean()
            Dc = -(D - dm)
            for i in np.nonzero(active)[0]:
                all_results.append({"alpha_d": alpha_d, "season_year": sy,
                                    "player_id": int(players[i]), "d_value": float(Dc[i]),
                                    "poss_season": float(raw_season[i])})
        print(f"  {sy}-{str(sy+1)[-2:]}: {int(active.sum())} players, solved for {len(ALPHA_D_GRID)} alpha_d values")

    res = pd.DataFrame(all_results)

    ev = pd.read_parquet(EVID_PATH)
    ev_next = ev.rename(columns={"season_year": "next_year", "ev_d": "evid_d", "ev_poss": "evid_poss"})
    res["next_year"] = res["season_year"] + 1
    j = res.merge(ev_next[["player_id", "next_year", "evid_d", "evid_poss"]],
                 on=["player_id", "next_year"])
    j = j[j["evid_poss"] >= 1000]

    feats = pd.read_parquet(METRIC_DATA / "features_box_season.parquet").rename(columns={"pid": "player_id"})
    jf = res.merge(feats[["player_id", "season_year", "dreb_75", "height"]], on=["player_id", "season_year"])

    print(f"\n{'alpha_d':>8} {'next-season D transfer':>24} {'r(height)':>11} {'r(dreb_75)':>12}")
    for a in ALPHA_D_GRID:
        ja = j[j["alpha_d"] == a]
        jfa = jf[jf["alpha_d"] == a]
        nxt = wcorr(ja["d_value"], ja["evid_d"], ja["evid_poss"])
        rh = wcorr(jfa["d_value"], jfa["height"], jfa["poss_season"])
        rd = wcorr(jfa["d_value"], jfa["dreb_75"], jfa["poss_season"])
        tag = "  <- current shipped" if a == 4000 else ""
        print(f"{a:>8} {nxt:>24.4f} {rh:>11.3f} {rd:>12.3f}{tag}")


if __name__ == "__main__":
    main()
