"""Contingent rim-defense swaps with at most one simultaneous offensive change.

Comparisons stay within the same game and period. Defensive lineups share four
players. Offensive lineups are either identical or share four players; when an
offensive player also changes, that contrast receives its own estimated term.
"""

from __future__ import annotations

import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse
from scipy.sparse.linalg import lsqr
from scipy.stats import spearmanr

from analyze_exact_lineup_swaps import OUTCOMES


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "derived" / "defense_causal" / "halfcourt_rim_lineup_possessions_2022_23_to_2025_26.parquet"
EVENTS_OUT = ROOT / "outputs" / "defense_causal" / "contingent_lineup_swap_events.parquet"
REPORT_OUT = ROOT / "outputs" / "defense_causal" / "contingent_lineup_swap_results.json"


def players(lineup: str) -> tuple[int, ...]:
    return tuple(sorted(map(int, lineup.split("-"))))


def build_events(df: pd.DataFrame) -> pd.DataFrame:
    keys = ["game_id", "season_year", "period", "offense_lineup_id", "defense_lineup_id"]
    spec = {"possessions": ("game_id", "size")}
    for numerator, denominator in OUTCOMES.values():
        spec[numerator] = (numerator, "sum")
        if denominator != "possessions":
            spec[denominator] = (denominator, "sum")
    contexts = df.groupby(keys, as_index=False).agg(**spec)
    events = []
    for (game_id, season, period), group in contexts.groupby(["game_id", "season_year", "period"], sort=False):
        records = list(group.itertuples(index=False))
        sets = [(set(players(r.offense_lineup_id)), set(players(r.defense_lineup_id))) for r in records]
        for i, j in itertools.combinations(range(len(records)), 2):
            oi, di = sets[i]; oj, dj = sets[j]
            if any(len(s) != 5 for s in (oi, oj, di, dj)):
                continue
            if len(di & dj) != 4 or len(oi & oj) < 4:
                continue
            a, b = records[i], records[j]
            da, db = next(iter(di-dj)), next(iter(dj-di))
            if da > db:
                a, b = b, a
                oi, oj, di, dj = oj, oi, dj, di
                da, db = db, da
            oa = next(iter(oi-oj)) if len(oi-oj) else 0
            ob = next(iter(oj-oi)) if len(oj-oi) else 0
            rec = {
                "game_id": str(game_id), "season_year": int(season), "period": int(period),
                "def_player_a": int(da), "def_player_b": int(db),
                "off_player_a": int(oa), "off_player_b": int(ob),
                "offense_changed": int(oa != 0),
            }
            for name, (num, den) in OUTCOMES.items():
                rec[f"{name}_y_a"] = float(getattr(a, num)); rec[f"{name}_n_a"] = float(getattr(a, den))
                rec[f"{name}_y_b"] = float(getattr(b, num)); rec[f"{name}_n_b"] = float(getattr(b, den))
            events.append(rec)
    return pd.DataFrame(events).drop_duplicates()


def fit(events: pd.DataFrame, outcome: str, ridge: float) -> tuple[dict, dict]:
    defenders = sorted(set(events.def_player_a).union(events.def_player_b))
    offense = sorted((set(events.off_player_a).union(events.off_player_b)) - {0})
    di = {p: i for i, p in enumerate(defenders)}
    oi = {p: len(defenders)+i for i, p in enumerate(offense)}
    rr, cc, vv = [], [], []
    for rownum, r in enumerate(events.itertuples(index=False)):
        rr += [rownum, rownum]; cc += [di[r.def_player_a], di[r.def_player_b]]; vv += [1.0, -1.0]
        if r.off_player_a:
            rr += [rownum, rownum]; cc += [oi[r.off_player_a], oi[r.off_player_b]]; vv += [1.0, -1.0]
    X = sparse.csr_matrix((vv, (rr, cc)), shape=(len(events), len(defenders)+len(offense)))
    na = events[f"{outcome}_n_a"].to_numpy(float); nb = events[f"{outcome}_n_b"].to_numpy(float)
    y = events[f"{outcome}_y_a"].to_numpy(float)/na - events[f"{outcome}_y_b"].to_numpy(float)/nb
    w = na*nb/(na+nb); sw = np.sqrt(w)
    penalty = sparse.eye(X.shape[1], format="csr")*np.sqrt(ridge)
    beta = lsqr(sparse.vstack([X.multiply(sw[:, None]), penalty]),
                np.r_[y*sw, np.zeros(X.shape[1])], atol=1e-9, btol=1e-9)[0]
    return ({p: float(beta[i]) for p, i in di.items()},
            {p: float(beta[i]) for p, i in oi.items()})


def evaluate(events: pd.DataFrame, outcome: str, ridge: float = 100.0) -> dict:
    threshold = 2 if outcome == "rim_fg_pct" else 3
    e = events[(events[f"{outcome}_n_a"] >= threshold) & (events[f"{outcome}_n_b"] >= threshold)].copy()
    e["fold"] = e.game_id.map(lambda x: int(x)%5)
    held = []
    for fold in range(5):
        train, test = e[e.fold != fold], e[e.fold == fold].copy()
        ds, os = fit(train, outcome, ridge)
        keep = test.def_player_a.isin(ds) & test.def_player_b.isin(ds)
        keep &= ((test.off_player_a == 0) | (test.off_player_a.isin(os) & test.off_player_b.isin(os)))
        test = test[keep].copy()
        pred = test.def_player_a.map(ds)-test.def_player_b.map(ds)
        changed = test.off_player_a != 0
        pred.loc[changed] += test.loc[changed, "off_player_a"].map(os)-test.loc[changed, "off_player_b"].map(os)
        test["prediction"] = pred
        held.append(test)
    h = pd.concat(held, ignore_index=True)
    na=h[f"{outcome}_n_a"].to_numpy(float); nb=h[f"{outcome}_n_b"].to_numpy(float)
    actual=h[f"{outcome}_y_a"].to_numpy(float)/na-h[f"{outcome}_y_b"].to_numpy(float)/nb
    pred=h.prediction.to_numpy(float); w=na*nb/(na+nb)
    rm=np.sqrt(np.average((actual-pred)**2,weights=w)); rz=np.sqrt(np.average(actual**2,weights=w))

    # Independent even/odd-game replication of defender scores.
    halves=[]
    counts=[]
    for half in (0,1):
        part=e[e.game_id.map(lambda x:int(x)%2)==half]
        ds,_=fit(part,outcome,ridge); halves.append(ds)
        counts.append(pd.concat([part.def_player_a,part.def_player_b]).value_counts())
    common=sorted(set(halves[0]).intersection(halves[1]))
    common=[p for p in common if counts[0].get(p,0)>=20 and counts[1].get(p,0)>=20]
    if len(common)>=3:
        x=np.array([halves[0][p] for p in common]); y=np.array([halves[1][p] for p in common])
        replication={"players":len(common),"pearson":float(np.corrcoef(x,y)[0,1]),
                     "spearman":float(spearmanr(x,y).statistic)}
    else: replication={"players":len(common)}
    return {
        "eligible_events":len(e), "heldout_events":len(h),
        "simultaneous_offensive_change_share":float(e.offense_changed.mean()),
        "heldout_pearson":float(np.corrcoef(actual,pred)[0,1]),
        "heldout_rmse_model":float(rm),"heldout_rmse_zero":float(rz),
        "rmse_improvement_pct":float(100*(1-rm/rz)),
        "disjoint_game_defender_score_replication":replication,"ridge":ridge,
    }


def main() -> None:
    EVENTS_OUT.parent.mkdir(parents=True, exist_ok=True)
    if EVENTS_OUT.exists() and EVENTS_OUT.stat().st_mtime >= SOURCE.stat().st_mtime:
        events=pd.read_parquet(EVENTS_OUT)
    else:
        cols=["game_id","season_year","period","offense_lineup_id","defense_lineup_id"]
        for num,den in OUTCOMES.values():
            cols += [num] + ([] if den=="possessions" else [den])
        df=pd.read_parquet(SOURCE,columns=list(dict.fromkeys(cols)))
        events=build_events(df); events.to_parquet(EVENTS_OUT,index=False)
    report={"design":"same game and period; exact defensive four-man core; offense exact or four-man core",
            "raw_events":len(events),"games":int(events.game_id.nunique()),
            "outcomes":{name:evaluate(events,name) for name in OUTCOMES}}
    REPORT_OUT.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(report,indent=2))


if __name__ == "__main__":
    main()
