"""Targeted adjacent-substitution tests for pre-identified rim-protector archetypes."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from analyze_exact_lineup_swaps import OUTCOMES

ROOT=Path(__file__).resolve().parents[1]
EVENTS=ROOT/"outputs"/"defense_causal"/"adjacent_substitution_rim_events.parquet"
FEATURES=Path(r"C:\Users\Dave\Downloads\nba-metric-data\features_box_season.parquet")
DB=ROOT/"data"/"nba_analytics.duckdb"
OUT=ROOT/"outputs"/"defense_causal"/"rim_protector_subgroup_results.json"


def labels()->pd.DataFrame:
    box=pd.read_parquet(FEATURES,columns=["pid","season_year","mins","height","blk_75"])
    box=box[(box.mins>=500)&box.height.notna()&box.blk_75.notna()].copy()
    box["event_season"]=box.season_year+1
    box["high_block_big"]=(box.height>=80)&(box.blk_75>=1.5)
    con=duckdb.connect(str(DB),read_only=True)
    rim=con.execute("""select player_id pid, cast(substr(season,1,4) as integer)+1 event_season,
        games, rim_dfga::double/games rim_dfga_pg
        from raw_player_rim_defense_by_season where games>=40""").df()
    rim["high_prior_rim_role"]=rim.rim_dfga_pg>=5.0
    lab=box[["pid","event_season","height","blk_75","high_block_big"]].merge(
        rim[["pid","event_season","rim_dfga_pg","high_prior_rim_role"]],
        on=["pid","event_season"],how="outer")
    lab["high_block_big"]=lab.high_block_big.astype("boolean").fillna(False).astype(bool)
    lab["high_prior_rim_role"]=lab.high_prior_rim_role.astype("boolean").fillna(False).astype(bool)
    lab["rim_protector_candidate"]=lab.high_block_big|lab.high_prior_rim_role
    return lab


def clustered_result(frame:pd.DataFrame,outcome:str)->dict:
    ya=f"{outcome}_y_a"; na=f"{outcome}_n_a"; yb=f"{outcome}_y_b"; nb=f"{outcome}_n_b"
    g=frame[(frame[na]>0)&(frame[nb]>0)].copy()
    # Orient each event candidate minus comparison player. Negative is better defense.
    sign=np.where(g.candidate_is_a,1.0,-1.0)
    g["difference"]=(g[ya]/g[na]-g[yb]/g[nb])*sign
    g["weight"]=g[na]*g[nb]/(g[na]+g[nb])
    if g.empty:return {"events":0}
    mean=float(np.average(g.difference,weights=g.weight))
    by_game=g.groupby("game_id").apply(lambda x:pd.Series({"num":(x.difference*x.weight).sum(),"den":x.weight.sum()}),include_groups=False)
    rng=np.random.default_rng(20260713); arr=by_game[["num","den"]].to_numpy(); n=len(arr)
    draws=np.empty(2000)
    for i in range(len(draws)):
        z=arr[rng.integers(0,n,n)]; draws[i]=z[:,0].sum()/z[:,1].sum()
    halves={}
    for half in (0,1):
        q=g[g.game_id.astype(str).map(lambda x:int(x)%2)==half]
        halves[str(half)]=float(np.average(q.difference,weights=q.weight)) if len(q) else None
    seasons={}
    for season,q in g.groupby("season_year"):
        seasons[str(int(season))]=float(np.average(q.difference,weights=q.weight))
    return {"events":int(len(g)),"games":int(g.game_id.nunique()),"weighted_rate_difference":mean,
            "cluster_bootstrap_95pct":[float(np.quantile(draws,.025)),float(np.quantile(draws,.975))],
            "disjoint_game_half_estimates":halves,"season_estimates":seasons}


def did_result(target:pd.DataFrame,control:pd.DataFrame,outcome:str)->dict:
    ya=f"{outcome}_y_a";na=f"{outcome}_n_a";yb=f"{outcome}_y_b";nb=f"{outcome}_n_b"
    t=target[(target[na]>0)&(target[nb]>0)].copy();c=control[(control[na]>0)&(control[nb]>0)].copy()
    def components(d:pd.DataFrame):
        ra=d[ya]/d[na];rb=d[yb]/d[nb];w=d[na]*d[nb]/(d[na]+d[nb])
        post_minus_pre=np.where(d.side_a_before==1,rb-ra,ra-rb)
        return ra,rb,w,post_minus_pre
    _,_,cw,ctrend=components(c);c["trend_num"]=cw*ctrend;c["trend_den"]=cw
    trends=c.groupby(["season_year","period"])[["trend_num","trend_den"]].sum()
    trends["trend"]=trends.trend_num/trends.trend_den
    ra,rb,w,_=components(t);raw=(ra-rb)*np.where(t.candidate_is_a,1.0,-1.0)
    keys=pd.MultiIndex.from_frame(t[["season_year","period"]]);trend=trends.trend.reindex(keys).to_numpy()
    corrected=raw+np.where(t.candidate_before,trend,-trend)
    t["difference"]=corrected;t["weight"]=w
    def summarize(q:pd.DataFrame)->dict:
        if q.empty:return {"events":0}
        mean=float(np.average(q.difference,weights=q.weight))
        bg=q.groupby("game_id").apply(lambda x:pd.Series({"num":(x.difference*x.weight).sum(),"den":x.weight.sum()}),include_groups=False)
        arr=bg[["num","den"]].to_numpy();rng=np.random.default_rng(20260714);draw=[]
        for _ in range(2000):
            z=arr[rng.integers(0,len(arr),len(arr))];draw.append(z[:,0].sum()/z[:,1].sum())
        return {"events":int(len(q)),"games":int(q.game_id.nunique()),"rate_difference":mean,
                "cluster_bootstrap_95pct_fixed_control_trend":[float(np.quantile(draw,.025)),float(np.quantile(draw,.975))]}
    return {"overall":summarize(t),"candidate_before_then_exits":summarize(t[t.candidate_before]),
            "candidate_after_then_enters":summarize(t[~t.candidate_before]),
            "control_post_minus_pre_by_season_period":{f"{s}-P{p}":float(v) for (s,p),v in trends.trend.items()}}


def main()->None:
    e=pd.read_parquet(EVENTS); e=e[e.offense_changed==0].copy()
    lab=labels()
    # Explicit merges avoid renaming the season key.
    a=lab.rename(columns={"pid":"def_player_a","event_season":"season_year",**{c:f"{c}_a" for c in lab.columns if c not in ("pid","event_season")}})
    b=lab.rename(columns={"pid":"def_player_b","event_season":"season_year",**{c:f"{c}_b" for c in lab.columns if c not in ("pid","event_season")}})
    e=e.merge(a,on=["def_player_a","season_year"],how="left").merge(b,on=["def_player_b","season_year"],how="left")
    report={"design":"adjacent stints, exact offensive five and four shared defenders; archetype defined from prior season only",
            "definitions":{"high_prior_rim_role":"prior-season rim DFGA/game >= 5 with >=40 games",
                           "high_block_big":"prior-season height >= 6-8 and blocks/75 >= 1.5 with >=500 minutes"},
            "groups":{}}
    for group in ("high_prior_rim_role","high_block_big","rim_protector_candidate"):
        ca=e[f"{group}_a"].astype("boolean").fillna(False).astype(bool); cb=e[f"{group}_b"].astype("boolean").fillna(False).astype(bool)
        q=e[ca^cb].copy(); q["candidate_is_a"]=ca[ca^cb].to_numpy()
        q["candidate_before"]=(q.candidate_is_a & (q.side_a_before==1)) | ((~q.candidate_is_a) & (q.side_a_before==0))
        candidate_ids=set(q.loc[q.candidate_is_a,"def_player_a"]).union(q.loc[~q.candidate_is_a,"def_player_b"])
        control=e[(~ca)&(~cb)].copy()
        report["groups"][group]={"candidate_players":len(candidate_ids),"matched_events_all_outcomes":len(q),
                                  "outcomes":{name:clustered_result(q,name) for name in OUTCOMES},
                                  "difference_in_differences":{name:did_result(q,control,name) for name in OUTCOMES},
                                  "strict_rim_access_by_timing":{
                                      "candidate_before_then_exits":clustered_result(q[q.candidate_before],"strict_rim_access"),
                                      "candidate_after_then_enters":clustered_result(q[~q.candidate_before],"strict_rim_access")}}
    OUT.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8"); print(json.dumps(report,indent=2))

if __name__=="__main__":main()
