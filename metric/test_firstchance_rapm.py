"""
Isolate how much of dreb_75's predictive power for defensive RAPM comes from
preventing SECOND-CHANCE points (a real but narrow, rebounding-specific
effect) versus genuine FIRST-CHANCE/point-of-attack defense (contesting the
initial shot, forcing tough looks) -- the user's proposed test, 2026-07-14.

Mechanics: using the PBP `possession` field (the team ID currently holding
the ball -- flips on a defensive rebound/turnover/make, stays fixed across
an offensive rebound, confirmed by direct inspection) and `subType` (rebounds
are tagged 'offensive'/'defensive' directly, no text parsing needed), every
possession is split into a first-chance segment (before any ORB) and a
second-chance segment (after the first ORB within that possession). Each
PBP row's point value comes from the scoreHome/scoreAway delta to the
previous row -- also avoids re-deriving shot values by hand.

Builds a stint-level "second-chance points" adjustment (same clock-window
attachment pattern as build_ft_adjust.py/build_midrange_adjust.py), SUBTRACTS
it on top of the standard luck-adjusted stints (same base as the shipped
target, alpha=500, single recent season for tractability), re-solves the
joint RAPM, and compares dreb_75's correlation with the resulting
"first-chance-only" defensive value against the standard (full) target and
against rim_fg_allow (shot-contesting quality).

Usage: python metric/test_firstchance_rapm.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_rapm_target import prepare, HCOLS, ACOLS, MIN_SECONDS

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PBP = METRIC_DATA / "PlayByPlay.parquet"
SEASON = 2024   # 2024-25, tractable single-season solve


def load_second_chance_points() -> pd.DataFrame:
    cols = ["gameId", "actionNumber", "period", "clock", "actionType", "subType",
            "possession", "scoreHome", "scoreAway", "gameDateTimeEst"]
    d = pd.read_parquet(PBP, columns=cols)
    d = d[d["gameId"].astype(str).str.startswith("2")]   # regular season only
    d["date"] = pd.to_datetime(d["gameDateTimeEst"], errors="coerce")
    d = d.dropna(subset=["date", "actionNumber"])
    d["season_year"] = d["date"].dt.year - (d["date"].dt.month < 10)
    d = d[d["season_year"] == SEASON].copy()
    d = d.sort_values(["gameId", "actionNumber"])
    print(f"{len(d):,} PBP rows for {SEASON}-{str(SEASON+1)[-2:]} regular season")

    d["poss_change"] = d.groupby("gameId")["possession"].transform(lambda s: (s != s.shift()).cumsum())
    d["is_orb"] = (d["actionType"] == "rebound") & (d["subType"] == "offensive")
    d["orb_before"] = d.groupby(["gameId", "poss_change"])["is_orb"].cumsum().shift(1).fillna(0) > 0
    # guard the shift against leaking across possession-group boundaries
    same_group = (d["poss_change"] == d.groupby("gameId")["poss_change"].shift(1))
    d["orb_before"] = d["orb_before"] & same_group.fillna(False)

    d["home_delta"] = d.groupby("gameId")["scoreHome"].diff().fillna(0)
    d["away_delta"] = d.groupby("gameId")["scoreAway"].diff().fillna(0)
    d["sc_home"] = np.where(d["orb_before"], d["home_delta"], 0.0)
    d["sc_away"] = np.where(d["orb_before"], d["away_delta"], 0.0)

    m = d["clock"].str.extract(r"PT(\d+)M([\d.]+)S")
    clock_s = pd.to_numeric(m[0], errors="coerce") * 60 + pd.to_numeric(m[1], errors="coerce")
    per = d["period"].astype(float)
    plen = np.where(per <= 4, 720.0, 300.0)
    prior = np.where(per <= 4, (per - 1) * 720.0, 2880.0 + (per - 5) * 300.0)
    d["elapsed"] = prior + (plen - clock_s)
    d["game_id"] = d["gameId"].astype(str)
    d = d.dropna(subset=["elapsed"])

    tot_sc = (d["sc_home"] + d["sc_away"]).sum()
    tot_pts = (d["home_delta"] + d["away_delta"]).sum()
    print(f"second-chance points: {tot_sc:,.0f} of {tot_pts:,.0f} total ({tot_sc/tot_pts:.1%})")
    return d[["game_id", "elapsed", "sc_home", "sc_away"]]


def attach_to_stints(sc: pd.DataFrame, st: pd.DataFrame) -> pd.DataFrame:
    stc = st[["game_id", "stint_index", "start_elapsed"]].copy()
    stc = stc.sort_values(["game_id", "start_elapsed"]).reset_index(drop=True)
    j = pd.merge_asof(sc.sort_values("elapsed").reset_index(drop=True),
                      stc.sort_values("start_elapsed"),
                      left_on="elapsed", right_on="start_elapsed",
                      by="game_id", direction="backward")
    agg = j.groupby(["game_id", "stint_index"])[["sc_home", "sc_away"]].sum().reset_index()
    return agg


def solve_one_season(st: pd.DataFrame, sy: int, alpha: int = 500) -> dict:
    st = st.dropna(subset=HCOLS + ACOLS).copy()
    st = st[st.seconds >= MIN_SECONDS].reset_index(drop=True)
    st["poss"] = np.maximum(st.seconds.to_numpy() / 24.0, 0.1)
    st["season_year"] = st.date.dt.year - (st.date.dt.month < 10)
    players = np.unique(st[HCOLS + ACOLS].to_numpy().astype(int).ravel())
    pidx = {p: i for i, p in enumerate(players)}
    P = len(players)
    n = len(st)
    lookup = np.vectorize(pidx.get)
    hidx = lookup(st[HCOLS].to_numpy().astype(int))
    aidx = lookup(st[ACOLS].to_numpy().astype(int))
    rows, cols, vals = [], [], []
    r = np.arange(n)
    for k in range(5):
        rows += [2 * r, 2 * r]; cols += [hidx[:, k], P + aidx[:, k]]; vals += [np.ones(n), np.ones(n)]
        rows += [2 * r + 1, 2 * r + 1]; cols += [aidx[:, k], P + hidx[:, k]]; vals += [np.ones(n), np.ones(n)]
    X = sparse.csr_matrix((np.concatenate(vals), (np.concatenate(rows), np.concatenate(cols))), shape=(2 * n, 2 * P))
    poss = st.poss.to_numpy()
    y = np.empty(2 * n)
    y[0::2] = st.home_pts_adj.to_numpy() / poss * 100.0
    y[1::2] = st.away_pts_adj.to_numpy() / poss * 100.0
    sel = (st.season_year.to_numpy() == sy)
    row_mask = np.repeat(sel, 2)
    Xs = X[row_mask]; ys = y[row_mask]; ws = np.repeat(poss[sel], 2)
    ybar = np.average(ys, weights=ws)
    Xw = Xs.multiply(np.sqrt(ws)[:, None]).tocsr()
    yw = (ys - ybar) * np.sqrt(ws)
    XtX = (Xw.T @ Xw).toarray(); Xty = Xw.T @ yw
    beta = np.linalg.solve(XtX + alpha * np.eye(2 * P), Xty)
    O = beta[:P]; D = beta[P:]
    om, dm = O.mean(), D.mean()
    w_on = np.zeros(P)
    for k in range(5):
        for idxs in (hidx[sel, k], aidx[sel, k]):
            np.add.at(w_on, idxs, poss[sel])
    return {int(players[i]): {"drapm": -(D[i] - dm), "poss": w_on[i]} for i in range(P) if w_on[i] > 0}


def wcorr(a, b, w) -> float:
    a, b, w = np.asarray(a, float), np.asarray(b, float), np.asarray(w, float)
    am, bm = np.average(a, weights=w), np.average(b, weights=w)
    cov = np.average((a - am) * (b - bm), weights=w)
    return cov / np.sqrt(np.average((a - am) ** 2, weights=w) * np.average((b - bm) ** 2, weights=w))


def main() -> None:
    print("=== standard (shipped) target, for reference ===")
    st_std = prepare(adjustments=("ft", "mr"))
    res_std = solve_one_season(st_std, SEASON)

    print("\n=== building second-chance-points adjustment ===")
    sc = load_second_chance_points()
    sc_stint = attach_to_stints(sc, st_std)
    st_fc = st_std.merge(sc_stint, on=["game_id", "stint_index"], how="left")
    st_fc[["sc_home", "sc_away"]] = st_fc[["sc_home", "sc_away"]].fillna(0.0)
    st_fc["home_pts_adj"] = st_fc["home_pts_adj"] - st_fc["sc_home"]
    st_fc["away_pts_adj"] = st_fc["away_pts_adj"] - st_fc["sc_away"]

    print("\n=== first-chance-only target ===")
    res_fc = solve_one_season(st_fc, SEASON)

    df = pd.DataFrame([
        {"pid": pid, "drapm_std": res_std[pid]["drapm"], "poss": res_std[pid]["poss"],
         "drapm_fc": res_fc.get(pid, {}).get("drapm")}
        for pid in res_std if pid in res_fc
    ]).dropna()

    feats = pd.read_parquet(METRIC_DATA / "features_box_season.parquet")
    feats = feats[feats.season_year == SEASON]
    rim = pd.read_parquet(METRIC_DATA / "rim_defense_season.parquet")
    rim = rim[rim.season_year == SEASON]
    df = df.merge(feats[["pid", "dreb_75", "blk_75"]], on="pid", how="left")
    df = df.merge(rim[["pid", "rim_fg_allow"]], on="pid", how="left")
    df = df[df.poss >= 1500]

    print(f"\nn={len(df)} players, poss>=1500, {SEASON}-{str(SEASON+1)[-2:]} single-season solve")
    print("\ndreb_75 correlation with:")
    for col in ["drapm_std", "drapm_fc"]:
        sub = df.dropna(subset=["dreb_75", col])
        print(f"  {col}: r={wcorr(sub.dreb_75, sub[col], sub.poss):.3f}  (n={len(sub)})")
    print("\nrim_fg_allow correlation with (sign flipped so + = better rim D = better target):")
    for col in ["drapm_std", "drapm_fc"]:
        sub = df.dropna(subset=["rim_fg_allow", col])
        print(f"  {col}: r={-wcorr(sub.rim_fg_allow, sub[col], sub.poss):.3f}  (n={len(sub)})")

    print("\nstd vs first-chance-only drapm correlation (are they even measuring similar things?):")
    print(f"  r={wcorr(df.drapm_std, df.drapm_fc, df.poss):.3f}")


if __name__ == "__main__":
    main()
