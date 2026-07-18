"""Counted-possession NERD preview on canonical + deterministic salvage lineups.

This current-season diagnostic uses the exact possession walker and trust gates
from count_stint_possessions.py, but writes only versioned contextual outputs.
"""
from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd
from scipy import sparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from test_probabilistic_salvage_rapm_sensitivity import aggregate_design, norm
from count_stint_possessions import (MAX_POSS_GAP, MIN_ATTACH_RATE, POINT_TOL,
                                     load_events, walk_game)
from build_nerd_canonical_salvage_preview import (ALPHA, ATOMIC, CURRENT,
    TARGET_YEAR, V1, fit_centered, prior_vector, season_year)


ROOT = Path(__file__).resolve().parents[1]
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
SALVAGE = ROOT / "derived" / "contextual_causal" / "probabilistic_lineup_salvage"
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
OUT = ROOT / "outputs" / "contextual_causal"
HCOLS = [f"home_p{i}" for i in range(1, 6)]
ACOLS = [f"away_p{i}" for i in range(1, 6)]
WINDOW_DAYS = 6 * 365
HALFLIFE = 550.0


def build_counted_part(st: pd.DataFrame, counts: pd.DataFrame,
                       pidx: dict[int, int], end: pd.Timestamp):
    st = st.merge(counts, on=["game_id", "stint_index"], how="inner")
    st = st.sort_values(["game_id", "stint_index"]).reset_index(drop=True)
    hk = pd.Series([",".join(map(str, r)) for r in
                    np.sort(st[HCOLS].to_numpy(int), axis=1)], index=st.index)
    ak = pd.Series([",".join(map(str, r)) for r in
                    np.sort(st[ACOLS].to_numpy(int), axis=1)], index=st.index)
    st["_luck_h"] = st.home_pts_adj - st.home_pts
    st["_luck_a"] = st.away_pts_adj - st.away_pts
    grp = st.groupby(["game_id", hk, ak])
    for short, side in (("h", "home"), ("a", "away")):
        luck = grp[f"_luck_{short}"].transform("sum")
        ng = grp[f"n_{side}"].transform("sum")
        nn = st[f"n_{side}"]
        st[f"pts_adj_{short}"] = st[f"pts_{side}"] + np.where(
            ng > 0, luck * nn / ng.replace(0, np.nan), 0.0)
    n = len(st); P = len(pidx)
    lookup = np.vectorize(pidx.get)
    hi = lookup(st[HCOLS].to_numpy(int)); ai = lookup(st[ACOLS].to_numpy(int))
    rows=[]; cols=[]; vals=[]; r=np.arange(n)
    for k in range(5):
        rows.extend([2*r, 2*r, 2*r+1, 2*r+1])
        cols.extend([hi[:,k], P+ai[:,k], ai[:,k], P+hi[:,k]])
        vals.extend([np.ones(n),np.ones(n),np.ones(n),np.ones(n)])
    X = sparse.csr_matrix((np.concatenate(vals),
                           (np.concatenate(rows),np.concatenate(cols))),
                          shape=(2*n,2*P))
    nh=st.n_home.to_numpy(float); na=st.n_away.to_numpy(float)
    y=np.zeros(2*n); y[0::2]=np.divide(st.pts_adj_h,nh,out=np.zeros(n),where=nh>0)*100
    y[1::2]=np.divide(st.pts_adj_a,na,out=np.zeros(n),where=na>0)*100
    nr=np.empty(2*n); nr[0::2]=nh; nr[1::2]=na
    age=(end-pd.to_datetime(st.date)).dt.days.to_numpy(float)
    decay=np.exp(-np.log(2)*age/HALFLIFE); decay[(age<0)|(age>WINDOW_DAYS)]=0
    weight=nr*np.repeat(decay,2)
    use=weight>0
    return (X[use],y[use],weight[use]),st


def main() -> None:
    canonical=pd.read_parquet(REBUILD/"canonical_stints_candidate.parquet")
    canonical["game_id"]=norm(canonical.game_id); canonical["date"]=pd.to_datetime(canonical.date)
    probs=pd.read_csv(SALVAGE/"rapm_candidate_probabilities.csv",dtype={"game_id":str,"candidate_id":str})
    probs["game_id"]=norm(probs.game_id)
    best=(probs[probs.rapm_candidate_probability>0]
          .sort_values("rapm_candidate_probability",ascending=False)
          .drop_duplicates("game_id")[["game_id","candidate_id"]])
    bank=pd.read_parquet(SALVAGE/"rapm_score_consistent_candidate_bank.parquet")
    bank["game_id"]=norm(bank.game_id); bank["candidate_id"]=bank.candidate_id.astype(str)
    salvage=bank.merge(best,on=["game_id","candidate_id"],how="inner")
    salvage["date"]=pd.to_datetime(salvage.date)
    prepared=pd.read_parquet(METRIC_DATA/"prepared_stints.parquet")
    prepared["game_id"]=norm(prepared.game_id); prepared["date"]=pd.to_datetime(prepared.date)
    playoffs=prepared[prepared.game_id.str.startswith("4")].copy()
    end=canonical.loc[season_year(canonical.date)==TARGET_YEAR,"date"].max()
    start=end-pd.Timedelta(days=WINDOW_DAYS)
    st=pd.concat([canonical,salvage,playoffs],ignore_index=True,sort=False)
    st=st[(st.date>=start)&(st.date<=end)].copy()
    st=st.sort_values(["game_id","start_elapsed"])
    st["stint_index"]=st.groupby("game_id").cumcount()
    ids=set(st.game_id)

    ev=load_events(); ev=ev[ev.gid_n.isin(ids)].copy()
    field=(ev.assign(pnum=pd.to_numeric(ev.possession,errors="coerce"))
           .groupby("gid_n").pnum.apply(lambda s:s.notna().any()))
    field_games=set(field[field].index)
    seg_rows=[]
    for gid,g in ev.groupby("gid_n",sort=False):
        seg_rows.extend((gid,s,t,p) for s,t,p in walk_game(g,gid in field_games))
    seg=pd.DataFrame(seg_rows,columns=["game_id","elapsed","team","pts"])
    seg=seg.dropna(subset=["elapsed"]).sort_values("elapsed",kind="stable")
    attach_st=st.sort_values("start_elapsed",kind="stable")
    merged=pd.merge_asof(seg,attach_st[["game_id","stint_index","start_elapsed",
        "end_elapsed","home_id","away_id"]],left_on="elapsed",right_on="start_elapsed",
        by="game_id",direction="backward")
    merged["in_win"]=merged.stint_index.notna()&(merged.elapsed<=merged.end_elapsed+1)
    merged["side"]=np.where(merged.team==merged.home_id,"home",
                            np.where(merged.team==merged.away_id,"away","?"))
    valid=merged[merged.in_win&(merged.side!="?")].copy()
    expected=st.groupby("game_id").agg(expected_home=("home_pts","sum"),
                                        expected_away=("away_pts","sum"))
    seen=merged.groupby("game_id").agg(possessions_seen=("pts","size"),
                                        possessions_attached=("in_win","sum"))
    got=(valid.groupby(["game_id","side"]).agg(n=("pts","size"),pts=("pts","sum"))
         .unstack(fill_value=0))
    got.columns=[f"{a}_{b}" for a,b in got.columns]
    audit=seen.join(got,how="left").join(expected,how="left").fillna(0).reset_index()
    for c in ("n_home","n_away","pts_home","pts_away"):
        if c not in audit: audit[c]=0.0
    audit["attach_rate"]=audit.possessions_attached/audit.possessions_seen.clip(lower=1)
    audit["home_point_error"]=audit.pts_home-audit.expected_home
    audit["away_point_error"]=audit.pts_away-audit.expected_away
    audit["possession_gap"]=(audit.n_home-audit.n_away).abs()
    audit["trusted"]=(audit.attach_rate.ge(MIN_ATTACH_RATE)
        &audit.home_point_error.abs().le(POINT_TOL)
        &audit.away_point_error.abs().le(POINT_TOL)
        &audit.possession_gap.le(MAX_POSS_GAP))
    trusted=set(audit.loc[audit.trusted,"game_id"])
    valid=valid[valid.game_id.isin(trusted)]
    counts=(valid.groupby(["game_id","stint_index","side"])
            .agg(n=("pts","size"),pts=("pts","sum")).unstack(fill_value=0))
    counts.columns=[f"{a}_{b}" for a,b in counts.columns]
    counts=counts.reset_index()
    for c in ("n_home","n_away","pts_home","pts_away"):
        if c not in counts: counts[c]=0.0
    counts["stint_index"]=counts.stint_index.astype(int)
    audit.to_csv(OUT/"nerd_canonical_counted_game_audit.csv",index=False)
    counts.to_parquet(SALVAGE/"nerd_canonical_counted_stints.parquet",index=False)

    aggregate=pd.read_parquet(SALVAGE/"rapm_aggregate_fallback_design.parquet")
    aggregate["game_id"]=norm(aggregate.game_id); aggregate["date"]=pd.to_datetime(aggregate.date)
    # A score-consistent lineup candidate can still fail the independent
    # possession parser (one current-window game does). Retain it at aggregate
    # grain rather than silently deleting it from the preview.
    salvage_ids=set(salvage.game_id)
    untrusted_salvage=salvage_ids-trusted
    if untrusted_salvage:
        all_aggregate=pd.read_parquet(SALVAGE/"rapm_all_quarantined_aggregate_design.parquet")
        all_aggregate["game_id"]=norm(all_aggregate.game_id)
        all_aggregate["date"]=pd.to_datetime(all_aggregate.date)
        extra=all_aggregate[all_aggregate.game_id.isin(untrusted_salvage)
                            & ~all_aggregate.game_id.isin(set(aggregate.game_id))]
        aggregate=pd.concat([aggregate,extra],ignore_index=True)
    players=set(st[HCOLS+ACOLS].dropna().to_numpy(int).ravel())|set(aggregate.player_id.astype(int))
    players=np.array(sorted(players),int); pidx={p:i for i,p in enumerate(players)}; P=len(players)
    counted_part, used_st=build_counted_part(st[st.game_id.isin(trusted)],counts,pidx,end)
    aggregate_part=aggregate_design(aggregate,pidx,end)
    # ATOMIC is denominator-aware by default; raw is retained only as a named
    # diagnostic so downstream readers cannot mistake it for the promoted model.
    raw_atomic=OUT/"rolling_prior_atomic_poss.parquet"
    priors={"v1":prior_vector(V1,pidx),
            "atomic":prior_vector(ATOMIC,pidx),
            "atomic_raw":prior_vector(raw_atomic,pidx)}
    result=pd.DataFrame({"player_id":players})
    for name,(b0,_) in priors.items():
        beta=fit_centered([counted_part,aggregate_part],P,b0)
        result[f"nerd_{name}_o"]=beta[:P]; result[f"nerd_{name}_d"]=-beta[P:]
        result[f"nerd_{name}"]=beta[:P]-beta[P:]
        result[f"prior_{name}_o"]=b0[:P]; result[f"prior_{name}_d"]=-b0[P:]
    cur=used_st[season_year(used_st.date)==TARGET_YEAR]
    exposure=np.zeros(P)
    for side,ncol,cols in (("home","n_home",HCOLS),("away","n_away",ACOLS)):
        nn=cur[ncol].to_numpy(float); idx=np.vectorize(pidx.get)(cur[cols].to_numpy(int))
        for k in range(5): np.add.at(exposure,idx[:,k],nn)
    result["poss_season"]=exposure
    con=duckdb.connect(str(ROOT/"data"/"nba_analytics.duckdb"),read_only=True)
    names=con.execute("SELECT CAST(player_id AS BIGINT) player_id,max_by(player_name,date) player_name,max_by(team_abbr,date) team_abbr FROM player_game_facts GROUP BY 1").df();con.close()
    result=result.merge(names,on="player_id",how="left")
    if CURRENT.exists():
        old=pd.read_parquet(CURRENT);old=old[old.season_year==TARGET_YEAR][["player_id","nerd"]].rename(columns={"nerd":"previous_candidate"})
        result=result.merge(old,on="player_id",how="left")
    result["atomic_minus_v1"]=result.nerd_atomic-result.nerd_v1
    result["atomic_minus_raw"]=result.nerd_atomic-result.nerd_atomic_raw
    result["canonical_minus_previous"]=result.nerd_atomic-result.previous_candidate
    result=result[result.poss_season>0].sort_values("nerd_atomic",ascending=False)
    result.to_parquet(OUT/"nerd_canonical_counted_preview.parquet",index=False)
    result.to_csv(OUT/"nerd_canonical_counted_preview.csv",index=False)
    summary=pd.DataFrame([{"games_in_window":len(ids),"trusted_games":len(trusted),
        "trusted_rate":len(trusted)/len(ids),"current_players":len(result),
        "alpha":ALPHA,"target_year":TARGET_YEAR}])
    summary.to_csv(OUT/"nerd_canonical_counted_preview_summary.csv",index=False)
    print(summary.to_string(index=False))
    print(result[["player_name","team_abbr","poss_season","nerd_atomic_o",
                  "nerd_atomic_d","nerd_atomic","nerd_v1",
                  "nerd_atomic_raw"]].head(30).to_string(index=False))


if __name__=="__main__": main()
