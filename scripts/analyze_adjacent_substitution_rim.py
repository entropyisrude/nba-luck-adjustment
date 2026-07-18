"""Local rim-defense event study around adjacent substitution stints."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from analyze_contingent_lineup_swaps import fit, players
from analyze_exact_lineup_swaps import OUTCOMES

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "derived" / "defense_causal" / "halfcourt_rim_lineup_possessions_2022_23_to_2025_26.parquet"
EVENTS_OUT = ROOT / "outputs" / "defense_causal" / "adjacent_substitution_rim_events.parquet"
REPORT_OUT = ROOT / "outputs" / "defense_causal" / "adjacent_substitution_rim_results.json"


def build_events(df: pd.DataFrame) -> pd.DataFrame:
    keys=["game_id","season_year","period","offense_team_id","stint_index","offense_lineup_id","defense_lineup_id"]
    spec={"possessions":("game_id","size")}
    for num,den in OUTCOMES.values():
        spec[num]=(num,"sum")
        if den!="possessions": spec[den]=(den,"sum")
    stints=df.groupby(keys,as_index=False).agg(**spec)
    out=[]
    for (_, _),g in stints.groupby(["game_id","offense_team_id"],sort=False):
        g=g.sort_values("stint_index")
        rows=list(g.itertuples(index=False))
        for a,b in zip(rows,rows[1:]):
            if b.stint_index-a.stint_index!=1 or a.period!=b.period: continue
            oa,ob=set(players(a.offense_lineup_id)),set(players(b.offense_lineup_id))
            da,db=set(players(a.defense_lineup_id)),set(players(b.defense_lineup_id))
            if any(0 in s or len(s)!=5 for s in (oa,ob,da,db)): continue
            if len(da&db)!=4 or len(oa&ob)<4: continue
            dpa,dpb=next(iter(da-db)),next(iter(db-da))
            side_a_before=1
            if dpa>dpb:
                a,b=b,a; oa,ob,da,db=ob,oa,db,da; dpa,dpb=dpb,dpa
                side_a_before=0
            opa=next(iter(oa-ob)) if oa-ob else 0
            opb=next(iter(ob-oa)) if ob-oa else 0
            rec={"game_id":str(a.game_id),"season_year":int(a.season_year),"period":int(a.period),
                 "def_player_a":int(dpa),"def_player_b":int(dpb),
                 "off_player_a":int(opa),"off_player_b":int(opb),"offense_changed":int(opa!=0),
                 "side_a_before":side_a_before}
            for name,(num,den) in OUTCOMES.items():
                rec[f"{name}_y_a"]=float(getattr(a,num)); rec[f"{name}_n_a"]=float(getattr(a,den))
                rec[f"{name}_y_b"]=float(getattr(b,num)); rec[f"{name}_n_b"]=float(getattr(b,den))
            out.append(rec)
    return pd.DataFrame(out)


def evaluate(events:pd.DataFrame,outcome:str,ridge:float=100.0)->dict:
    e=events[(events[f"{outcome}_n_a"]>0)&(events[f"{outcome}_n_b"]>0)].copy()
    e["fold"]=e.game_id.map(lambda x:int(x)%5); held=[]
    for fold in range(5):
        train,test=e[e.fold!=fold],e[e.fold==fold].copy(); ds,os=fit(train,outcome,ridge)
        keep=test.def_player_a.isin(ds)&test.def_player_b.isin(ds)
        keep&=((test.off_player_a==0)|(test.off_player_a.isin(os)&test.off_player_b.isin(os)))
        test=test[keep].copy(); pred=test.def_player_a.map(ds)-test.def_player_b.map(ds)
        ch=test.off_player_a!=0
        pred.loc[ch]+=test.loc[ch,"off_player_a"].map(os)-test.loc[ch,"off_player_b"].map(os)
        test["prediction"]=pred; held.append(test)
    h=pd.concat(held,ignore_index=True)
    na=h[f"{outcome}_n_a"].to_numpy(float); nb=h[f"{outcome}_n_b"].to_numpy(float)
    y=h[f"{outcome}_y_a"].to_numpy(float)/na-h[f"{outcome}_y_b"].to_numpy(float)/nb
    p=h.prediction.to_numpy(float); w=na*nb/(na+nb)
    rm=np.sqrt(np.average((y-p)**2,weights=w)); rz=np.sqrt(np.average(y*y,weights=w))
    scores=[]; counts=[]
    for half in (0,1):
        q=e[e.game_id.map(lambda x:int(x)%2)==half]; ds,_=fit(q,outcome,ridge); scores.append(ds)
        counts.append(pd.concat([q.def_player_a,q.def_player_b]).value_counts())
    common=sorted(set(scores[0])&set(scores[1])); common=[x for x in common if counts[0].get(x,0)>=10 and counts[1].get(x,0)>=10]
    rep={"players":len(common)}
    if len(common)>=3:
        x=np.array([scores[0][i] for i in common]); z=np.array([scores[1][i] for i in common])
        rep|={"pearson":float(np.corrcoef(x,z)[0,1]),"spearman":float(spearmanr(x,z).statistic)}
    return {"eligible_events":len(e),"heldout_events":len(h),"offense_change_share":float(e.offense_changed.mean()),
            "heldout_pearson":float(np.corrcoef(y,p)[0,1]),"heldout_rmse_model":float(rm),
            "heldout_rmse_zero":float(rz),"rmse_improvement_pct":float(100*(1-rm/rz)),
            "disjoint_game_replication":rep,"ridge":ridge}


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument("--source",default=str(SOURCE));parser.add_argument("--events-out",default=str(EVENTS_OUT));parser.add_argument("--report-out",default=str(REPORT_OUT));args=parser.parse_args()
    source=Path(args.source);events_out=Path(args.events_out);report_out=Path(args.report_out)
    events_out.parent.mkdir(parents=True,exist_ok=True)
    if events_out.exists() and events_out.stat().st_mtime>=source.stat().st_mtime:
        events=pd.read_parquet(events_out)
    else:
        cols=["game_id","season_year","period","offense_team_id","stint_index","offense_lineup_id","defense_lineup_id"]
        for num,den in OUTCOMES.values(): cols += [num]+([] if den=="possessions" else [den])
        events=build_events(pd.read_parquet(source,columns=list(dict.fromkeys(cols))))
        events.to_parquet(events_out,index=False)
    report={"design":"immediately adjacent stints; same game, period, and offense team; exact defensive four-man core",
            "raw_events":len(events),"games":int(events.game_id.nunique()),
            "outcomes":{name:evaluate(events,name) for name in OUTCOMES}}
    report_out.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8"); print(json.dumps(report,indent=2))

if __name__=="__main__": main()
