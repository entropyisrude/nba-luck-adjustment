"""Held-out-season screen of prior features for matched rim-deterrence effects."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

ROOT=Path(__file__).resolve().parents[1]
EVENTS=ROOT/"outputs"/"defense_causal"/"adjacent_substitution_rim_events.parquet"
BOX=Path(r"C:\Users\Dave\Downloads\nba-metric-data\features_box_season.parquet")
DB=ROOT/"data"/"nba_analytics.duckdb"
OUT=ROOT/"outputs"/"defense_causal"/"rim_deterrence_feature_screen.json"

BASE_FEATURES=["height","blk_75","pf_75","fouls_drawn_75","dreb_75","oreb_75","stl_75",
               "contest_75","boxout_75","chg_75","defl_75","screen_75","loose_75","mpg","age",
               "rim_dfga_pg","rim_dfg_pct","rim_dfg_pct_expected","rim_dfg_pct_diff","rim_dfg_diff_shrunk"]


def feature_table()->pd.DataFrame:
    cols=["pid","season_year","mins"]+[x for x in BASE_FEATURES if not x.startswith("rim_")]
    f=pd.read_parquet(BOX,columns=cols);f=f[(f.mins>=300)&f.season_year.between(2019,2024)].copy();f["event_season"]=f.season_year+1
    c=duckdb.connect(str(DB),read_only=True)
    r=c.execute("""select player_id pid,cast(substr(season,1,4) as integer)+1 event_season,
      rim_dfga,rim_dfga::double/games rim_dfga_pg,rim_dfg_pct,rim_dfg_pct_expected,rim_dfg_pct_diff
      from raw_player_rim_defense_by_season where games>=20""").df()
    f=f.merge(r,on=["pid","event_season"],how="left")
    f["rim_dfg_diff_shrunk"]=f.rim_dfg_pct_diff*f.rim_dfga/(f.rim_dfga+200.0)
    f["block_foul_ratio"]=f.blk_75/(f.pf_75+0.5)
    f["contest_foul_ratio"]=f.contest_75/(f.pf_75+0.5)
    features=BASE_FEATURES+["block_foul_ratio","contest_foul_ratio"]
    for season,gidx in f.groupby("event_season").groups.items():
        idx=list(gidx)
        for col in features:
            med=f.loc[idx,col].median();f.loc[idx,col]=f.loc[idx,col].fillna(med)
            sd=f.loc[idx,col].std()
            f.loc[idx,"z_"+col]=(f.loc[idx,col]-f.loc[idx,col].mean())/(sd if sd and np.isfinite(sd) else 1)
    return f[["pid","event_season"]+features+["z_"+x for x in features]]


def event_panel(outcome:str)->tuple[pd.DataFrame,list[str]]:
    e=pd.read_parquet(EVENTS);e=e[e.offense_changed==0].copy()
    ya=f"{outcome}_y_a";na=f"{outcome}_n_a";yb=f"{outcome}_y_b";nb=f"{outcome}_n_b"
    e=e[(e[na]>0)&(e[nb]>0)].copy();ra=e[ya]/e[na];rb=e[yb]/e[nb]
    e["weight"]=e[na]*e[nb]/(e[na]+e[nb]);e["raw"]=ra-rb
    post=np.where(e.side_a_before==1,rb-ra,ra-rb)
    tmp=e[["season_year","period","weight"]].copy();tmp["num"]=post*tmp.weight
    tr=tmp.groupby(["season_year","period"])[["num","weight"]].sum();tr["trend"]=tr.num/tr.weight
    T=tr.trend.reindex(pd.MultiIndex.from_frame(e[["season_year","period"]])).to_numpy()
    e["target"]=e.raw+np.where(e.side_a_before==1,T,-T)
    f=feature_table();features=[x[2:] for x in f.columns if x.startswith("z_")]
    a=f.rename(columns={"pid":"def_player_a","event_season":"season_year",**{"z_"+x:"a_"+x for x in features}})
    b=f.rename(columns={"pid":"def_player_b","event_season":"season_year",**{"z_"+x:"b_"+x for x in features}})
    keepa=["def_player_a","season_year"]+["a_"+x for x in features];keepb=["def_player_b","season_year"]+["b_"+x for x in features]
    e=e.merge(a[keepa],on=["def_player_a","season_year"],how="inner").merge(b[keepb],on=["def_player_b","season_year"],how="inner")
    for x in features:e["d_"+x]=e["a_"+x]-e["b_"+x]
    return e,features


def ridge_fit(X,y,w,alpha):
    return np.linalg.solve(X.T@(w[:,None]*X)+alpha*np.eye(X.shape[1]),X.T@(w*y))


def evaluate(outcome:str)->dict:
    e,features=event_panel(outcome);pred_uni={x:np.zeros(len(e)) for x in features};pred_ridge=np.zeros(len(e));pred_tree=np.zeros(len(e))
    alphas=[1.,10.,100.,1000.]
    chosen=[]
    for season in sorted(e.season_year.unique()):
        test=e.season_year==season;train=~test
        y=e.target.to_numpy();w=e.weight.to_numpy();X=e[["d_"+x for x in features]].to_numpy()
        for j,x in enumerate(features):
            xx=X[train,j];beta=(w[train]*xx*y[train]).sum()/max((w[train]*xx*xx).sum(),1e-12);pred_uni[x][test]=X[test,j]*beta
        # Choose ridge penalty using leave-one-season-out inside the training seasons.
        scores=[]
        train_seasons=sorted(e.loc[train,"season_year"].unique())
        for alpha in alphas:
            errs=[]
            for val_season in train_seasons:
                tr=train&(e.season_year!=val_season);va=train&(e.season_year==val_season)
                beta=ridge_fit(X[tr],y[tr],w[tr],alpha);errs.append(np.average((y[va]-X[va]@beta)**2,weights=w[va]))
            scores.append(np.mean(errs))
        alpha=alphas[int(np.argmin(scores))];chosen.append(alpha);beta=ridge_fit(X[train],y[train],w[train],alpha);pred_ridge[test]=X[test]@beta
        # Shallow nonlinear screen, antisymmetry enforced by adding sign-reversed rows.
        Xt=np.vstack([X[train],-X[train]]);yt=np.r_[y[train],-y[train]];wt=np.r_[w[train],w[train]]
        tree=HistGradientBoostingRegressor(max_iter=100,max_leaf_nodes=7,min_samples_leaf=100,l2_regularization=10,random_state=7)
        tree.fit(Xt,yt,sample_weight=wt);pred_tree[test]=(tree.predict(X[test])-tree.predict(-X[test]))/2
    y=e.target.to_numpy();w=e.weight.to_numpy();zero=np.sqrt(np.average(y*y,weights=w))
    def result(p):
        rm=np.sqrt(np.average((y-p)**2,weights=w));return {"rmse":float(rm),"improvement_pct_vs_zero":float(100*(1-rm/zero)),"pearson":float(np.corrcoef(y,p)[0,1])}
    uni={x:result(pred_uni[x]) for x in features};ranked=sorted(uni.items(),key=lambda kv:kv[1]["improvement_pct_vs_zero"],reverse=True)
    return {"events":len(e),"zero_rmse":float(zero),"univariate_ranked":dict(ranked),"ridge_all":result(pred_ridge)|{"chosen_alphas":chosen},"shallow_nonlinear_all":result(pred_tree)}


def selector_validation(outcome:str)->dict:
    e,_=event_panel(outcome);f=feature_table().copy()
    selectors={
        "height":"z_height","blocks":"z_blk_75","contest_volume":"z_contest_75",
        "defensive_rebounds":"z_dreb_75","boxouts":"z_boxout_75","low_personal_fouls":"z_pf_75",
        "fouls_drawn":"z_fouls_drawn_75","block_foul_ratio":"z_block_foul_ratio",
        "contest_foul_ratio":"z_contest_foul_ratio","rim_defended_volume":"z_rim_dfga_pg",
        "prior_rim_dfg_pct":"z_rim_dfg_pct","prior_rim_dfg_pct_diff":"z_rim_dfg_pct_diff",
        "sample_shrunk_rim_dfg_diff":"z_rim_dfg_diff_shrunk",
    }
    # Convert low-is-good selectors so every score uses "higher = selected".
    f["score_low_personal_fouls"]=-f.z_pf_75;f["score_prior_rim_dfg_pct"]=-f.z_rim_dfg_pct
    f["score_size_blocks"]=(f.z_height+f.z_blk_75)/2
    f["score_anchor_composite"]=(f.z_height+f.z_blk_75+f.z_contest_75+f.z_rim_dfga_pg+
                                  f.z_dreb_75+f.z_boxout_75-f.z_pf_75)/7
    f["score_block_contest_discipline"]=(f.z_blk_75+f.z_contest_75-f.z_pf_75)/3
    selectors["size_plus_blocks"]="score_size_blocks";selectors["anchor_composite"]="score_anchor_composite"
    selectors["block_contest_discipline"]="score_block_contest_discipline"
    selectors["low_personal_fouls"]="score_low_personal_fouls";selectors["prior_rim_dfg_pct"]="score_prior_rim_dfg_pct"
    out={}
    for name,col in selectors.items():
        flags=[]
        for season,g in f.groupby("event_season"):
            threshold=g[col].quantile(.85);flags.append(pd.DataFrame({"pid":g.pid,"season_year":season,"flag":g[col]>=threshold}))
        flag=pd.concat(flags,ignore_index=True)
        a=flag.rename(columns={"pid":"def_player_a","flag":"flag_a"});b=flag.rename(columns={"pid":"def_player_b","flag":"flag_b"})
        q=e.merge(a,on=["def_player_a","season_year"],how="left").merge(b,on=["def_player_b","season_year"],how="left")
        q.flag_a=q.flag_a.fillna(False).astype(bool);q.flag_b=q.flag_b.fillna(False).astype(bool);q=q[q.flag_a^q.flag_b].copy()
        q["difference"]=q.target*np.where(q.flag_a,1.0,-1.0)
        def summary(z):
            if z.empty:return {"events":0}
            mean=float(np.average(z.difference,weights=z.weight));bg=z.groupby("game_id").apply(
                lambda x:pd.Series({"num":float((x.difference*x.weight).sum()),"den":float(x.weight.sum())}),include_groups=False)
            arr=bg[["num","den"]].to_numpy();rng=np.random.default_rng(20260715);draw=[]
            for _ in range(1000):
                zz=arr[rng.integers(0,len(arr),len(arr))];draw.append(zz[:,0].sum()/zz[:,1].sum())
            return {"events":len(z),"games":int(z.game_id.nunique()),"rate_difference":mean,
                    "cluster_95pct":[float(np.quantile(draw,.025)),float(np.quantile(draw,.975))]}
        by_season={str(int(season)):summary(q[q.season_year==season]) for season in sorted(q.season_year.unique())}
        out[name]={
            "earlier_extension_2020_21_and_2021_22":summary(q[q.season_year.isin([2020,2021])]),
            "discovery_2022_23_and_2023_24":summary(q[q.season_year.isin([2022,2023])]),
            "validation_2024_25_and_2025_26":summary(q[q.season_year.isin([2024,2025])]),
            "all_available_seasons":summary(q),
            "by_target_season_start":by_season,
        }
    return out


def main():
    global EVENTS,OUT
    parser=argparse.ArgumentParser();parser.add_argument("--events",default=str(EVENTS));parser.add_argument("--out",default=str(OUT));args=parser.parse_args();EVENTS=Path(args.events);OUT=Path(args.out)
    report={"design":"prior-season features predicting same-game adjacent-stint matched contrasts; exact offensive five; leave-one-season-out",
            "outcomes":{y:evaluate(y) for y in ["strict_rim_access","recorded_rim_attempt","restricted_area_foul","rim_fg_pct"]},
            "top_15pct_selector_validation":{y:selector_validation(y) for y in ["strict_rim_access","recorded_rim_attempt","rim_fg_pct"]}}
    OUT.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8");print(json.dumps(report,indent=2))

if __name__=="__main__":main()
