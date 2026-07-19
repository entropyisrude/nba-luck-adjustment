"""Public RAPM fits from canonical counted-possession production evidence."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.linear_model import Ridge

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "derived/contextual_causal/production_counted_evidence"
ONOFF = ROOT / "derived/contextual_causal/production_counted_onoff"
HCOLS = [f"home_p{i}" for i in range(1, 6)]
ACOLS = [f"away_p{i}" for i in range(1, 6)]

TEAM_ID_TO_ABBR = {
    1610612737:"ATL",1610612738:"BOS",1610612751:"BKN",1610612766:"CHA",
    1610612741:"CHI",1610612739:"CLE",1610612742:"DAL",1610612743:"DEN",
    1610612765:"DET",1610612744:"GSW",1610612745:"HOU",1610612754:"IND",
    1610612746:"LAC",1610612747:"LAL",1610612763:"MEM",1610612748:"MIA",
    1610612749:"MIL",1610612750:"MIN",1610612740:"NOP",1610612752:"NYK",
    1610612760:"OKC",1610612753:"ORL",1610612755:"PHI",1610612756:"PHX",
    1610612757:"POR",1610612758:"SAC",1610612759:"SAS",1610612761:"TOR",
    1610612762:"UTA",1610612764:"WAS"}


def season_year(dates: pd.Series) -> np.ndarray:
    d = pd.to_datetime(dates)
    return (d.dt.year - (d.dt.month < 10)).to_numpy(int)


class CountedPublicRapm:
    def __init__(self) -> None:
        c = pd.read_parquet(EVIDENCE / "canonical_counted_stints_production.parquet")
        a = pd.read_parquet(EVIDENCE / "canonical_counted_aggregate_production.parquet")
        c["game_id"] = c.game_id.astype(str).str.lstrip("0")
        a["game_id"] = a.game_id.astype(str).str.lstrip("0")
        c["season_year"] = season_year(c.date)
        a["season_year"] = season_year(a.date)
        players = set(c[HCOLS + ACOLS].to_numpy(int).ravel()) | set(a.player_id.astype(int))
        self.players = np.array(sorted(players), dtype=np.int64)
        self.pidx = {p: i for i, p in enumerate(self.players)}
        P = len(self.players)
        lookup = np.vectorize(self.pidx.get)
        hi, ai = lookup(c[HCOLS].to_numpy(int)), lookup(c[ACOLS].to_numpy(int))
        n = len(c)
        r = np.arange(n)
        rr, cc, vv = [], [], []
        for k in range(5):
            rr.extend([2*r, 2*r, 2*r+1, 2*r+1])
            cc.extend([hi[:,k], P+ai[:,k], ai[:,k], P+hi[:,k]])
            vv.extend([np.ones(n), -np.ones(n), np.ones(n), -np.ones(n)])
        xs = sparse.csr_matrix((np.concatenate(vv), (np.concatenate(rr), np.concatenate(cc))),
                               shape=(2*n, 2*P))
        poss = np.empty(2*n)
        poss[0::2], poss[1::2] = c.n_home.to_numpy(float), c.n_away.to_numpy(float)
        ya = np.zeros(2*n)
        np.divide(c.points_adjusted_home.to_numpy(float)*100, c.n_home.to_numpy(float),
                  out=ya[0::2], where=c.n_home.to_numpy(float)>0)
        np.divide(c.points_adjusted_away.to_numpy(float)*100, c.n_away.to_numpy(float),
                  out=ya[1::2], where=c.n_away.to_numpy(float)>0)
        ya_3pt_ft = np.zeros(2*n)
        np.divide(c.points_adjusted_home_3pt_ft.to_numpy(float)*100,
                  c.n_home.to_numpy(float), out=ya_3pt_ft[0::2],
                  where=c.n_home.to_numpy(float)>0)
        np.divide(c.points_adjusted_away_3pt_ft.to_numpy(float)*100,
                  c.n_away.to_numpy(float), out=ya_3pt_ft[1::2],
                  where=c.n_away.to_numpy(float)>0)
        # Raw targets preserve each source game's exact scoring total while
        # keeping the canonical possession placement within that game.
        tmp = c.copy()
        sums = tmp.groupby("game_id").agg(ah=("points_adjusted_home","sum"),
                    aa=("points_adjusted_away","sum"), rh=("home_pts","sum"),
                    ra=("away_pts","sum"), ph=("n_home","sum"), pa=("n_away","sum"))
        gh = tmp.game_id.map(sums.rh-sums.ah).to_numpy(float)
        ga = tmp.game_id.map(sums.ra-sums.aa).to_numpy(float)
        ph = tmp.game_id.map(sums.ph).to_numpy(float)
        pa = tmp.game_id.map(sums.pa).to_numpy(float)
        rawh = tmp.points_adjusted_home.to_numpy(float) + gh*tmp.n_home.to_numpy(float)/np.maximum(ph,1)
        rawa = tmp.points_adjusted_away.to_numpy(float) + ga*tmp.n_away.to_numpy(float)/np.maximum(pa,1)
        yr = np.zeros(2*n)
        np.divide(rawh*100, tmp.n_home.to_numpy(float), out=yr[0::2], where=tmp.n_home.to_numpy(float)>0)
        np.divide(rawa*100, tmp.n_away.to_numpy(float), out=yr[1::2], where=tmp.n_away.to_numpy(float)>0)

        obs = (a.drop_duplicates("observation_id")[["observation_id","game_id","season_year",
               "target_per_100","target_per_100_3pt_ft",
               "possessions_proxy"]].reset_index(drop=True))
        rowmap = {v:i for i,v in enumerate(obs.observation_id)}
        ar = a.observation_id.map(rowmap).to_numpy(int)
        ap = a.player_id.astype(int).map(self.pidx).to_numpy(int)
        ac = ap + np.where(a.role.eq("defense"), P, 0)
        av = a.design_value.to_numpy(float) * np.where(a.role.eq("defense"), -1.0, 1.0)
        xa = sparse.csr_matrix((av,(ar,ac)),shape=(len(obs),2*P))
        self.X = sparse.vstack([xs,xa],format="csr")
        self.y_adj = np.r_[ya,obs.target_per_100.to_numpy(float)]
        self.y_3pt_ft = np.r_[ya_3pt_ft,
                              obs.target_per_100_3pt_ft.to_numpy(float)]
        self.y_raw = np.r_[yr,obs.target_per_100.to_numpy(float)]
        self.poss = np.r_[poss,obs.possessions_proxy.to_numpy(float)]
        self.seasons = np.r_[np.repeat(c.season_year.to_numpy(int),2),obs.season_year.to_numpy(int)]
        self.game_types = np.r_[np.repeat(np.where(c.game_id.str.startswith("4"),"playoffs","regular"),2),
                                np.where(obs.game_id.str.startswith("4"),"playoffs","regular")]
        meta = []
        for f in ("adjusted_onoff_regular_canonical_counted.parquet",
                  "adjusted_onoff_playoffs_canonical_counted.parquet"):
            meta.append(pd.read_parquet(ONOFF/f,columns=["game_id","date","player_id","player_name",
                                                         "team_id","minutes_on"]))
        self.meta = pd.concat(meta,ignore_index=True)
        self.meta["game_id"] = self.meta.game_id.astype(str).str.lstrip("0")
        self.meta["season_year"] = season_year(self.meta.date)
        self.meta["game_type"] = np.where(self.meta.game_id.str.startswith("4"),"playoffs","regular")

    def fit(self, kind: str, years: list[int], alpha: float, min_minutes: int,
            adjustment: str = "default") -> list[dict]:
        mask = (self.game_types == kind) & np.isin(self.seasons, years) & (self.poss > 0)
        X = self.X[mask]
        active = np.asarray(X.getnnz(axis=0)).ravel() > 0
        X = X[:,active]
        w = self.poss[mask]
        adjusted = self.y_3pt_ft if adjustment == "3pt_ft" else self.y_adj
        Y = np.c_[adjusted[mask],self.y_raw[mask]]
        Y = Y - np.average(Y,axis=0,weights=w)
        model = Ridge(alpha=alpha,fit_intercept=False,solver="lsqr",tol=1e-6,max_iter=1000)
        model.fit(X,Y,sample_weight=w)
        full = np.zeros((2,len(self.players),2))
        coef = model.coef_.T
        active_idx = np.flatnonzero(active)
        P = len(self.players)
        for j,col in enumerate(active_idx):
            if col < P: full[0,col,:] = coef[j]
            else: full[1,col-P,:] = coef[j]
        observed = np.flatnonzero(active[:P] | active[P:])
        for side in range(2):
            full[side,observed,:] -= full[side,observed,:].mean(axis=0)

        m = self.meta[(self.meta.game_type==kind)&self.meta.season_year.isin(years)].copy()
        mins = m.groupby("player_id").minutes_on.sum()
        names = m.sort_values("date").drop_duplicates("player_id",keep="last").set_index("player_id").player_name
        tm = m.groupby(["player_id","team_id"]).minutes_on.sum().reset_index()
        teams = tm.sort_values("minutes_on").drop_duplicates("player_id",keep="last").set_index("player_id").team_id
        rows=[]
        for i,pid in enumerate(self.players):
            minutes=float(mins.get(pid,0))
            if minutes < min_minutes or i not in observed: continue
            o,d = full[0,i],full[1,i]
            tid=int(teams.get(pid,0))
            rows.append({"player_id":int(pid),"player_name":str(names.get(pid,f"Player {pid}")),
                "team_id":tid,"team_abbr":TEAM_ID_TO_ABBR.get(tid,"???"),"minutes":int(round(minutes)),
                "rapm":float(o[0]+d[0]),"orapm":float(o[0]),"drapm":float(d[0]),
                "rapm_raw":float(o[1]+d[1]),"orapm_raw":float(o[1]),"drapm_raw":float(d[1])})
        return sorted(rows,key=lambda x:x["rapm"],reverse=True)

    def regular_dict(self, min_minutes: int=200) -> dict:
        years=sorted(set(self.seasons[self.game_types=="regular"]))
        out={}
        for y in years:
            label=f"{y}-{(y+1)%100:02d}"
            for alpha in (10,500):
                print(f"regular {label} alpha={alpha}",flush=True)
                out[f"{label}_a{alpha}"]=self.fit("regular",[y],alpha,min_minutes)
            out[label]=out[f"{label}_a500"]
        windows={"Last3":years[-3:],"Last5":years[-5:],
                 "2020s":[y for y in years if y>=2020],"2010s":[y for y in years if 2010<=y<2020],
                 "2000s":[y for y in years if 2000<=y<2010],"1996-99":[y for y in years if y<2000],
                 "All":years}
        for key,ys in windows.items():
            for alpha in (10,500):
                print(f"regular {key} alpha={alpha}",flush=True)
                out[f"{key}_a{alpha}"]=self.fit("regular",ys,alpha,min_minutes)
            out[key]=out[f"{key}_a500"]
            out[f"{key}_seasons"]=[f"{y}-{(y+1)%100:02d}" for y in ys]
        return out
