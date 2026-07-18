"""Rim-defense on/off feature: opponent rim volume + efficiency while on court.

Tests whether restricted-area shot defense (<=4ft, the NBA's own restricted-
area radius) is a useful defensive box-prior feature and whether it PERSISTS
season to season for the same player (real rim-protection skill) or is just
a finer-grained restatement of team on-court outcome -- the same failure
mode that killed the opp_paint_75 CONTEXT candidate in build_box_prior.py
(lifted same-season fit 0.708->0.743, did not improve next-season transfer).

Mechanics: every rim attempt in the game is attached to a stint (same clock-
window + fallback logic as build_midrange_adjust.py), the shooter's side
(home/away) identifies the OFFENSE, and the attempt + make are credited to
the 5 players on the opposite (DEFENSE) side. Aggregated to player-season:
  rim_att_75   = opponent rim attempts allowed per 75 possessions on court
  rim_fg_allow = opponent FG% at the rim while on court (makes/attempts)
Possession denominator is the same stint-poss convention used everywhere
else in the pipeline (each stint's poss counts once per player who was on
the floor, home or away, matching how dreb_75/blk_75 etc. are built).

Output: nba-metric-data/rim_defense_season.parquet
  (pid, season_year, rim_att, rim_make, poss, rim_att_75, rim_fg_allow)

Usage: python metric/build_rim_defense.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_rapm_target import prepare, HCOLS, ACOLS

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PBP = METRIC_DATA / "PlayByPlay.parquet"
OUT = METRIC_DATA / "rim_defense_season.parquet"

RIM_FT = 4.0    # NBA restricted-area radius


def load_rim_events() -> pd.DataFrame:
    cols = ["gameId", "personId", "period", "clock", "description",
            "actionType", "shotDistance", "gameDateTimeEst"]
    pbp = pd.read_parquet(PBP, columns=cols)
    at = pbp["actionType"].str.strip().str.lower()
    old2 = at.isin(["made shot", "missed shot"]) & \
        ~pbp["description"].str.contains("3PT", na=False)
    new2 = at == "2pt"
    sh = pbp[old2 | new2].copy()
    del pbp
    at = sh["actionType"].str.strip().str.lower()
    sh["made"] = np.where(at == "made shot", True,
                 np.where(at == "missed shot", False,
                          ~sh["description"].str.contains("MISS", case=False, na=False)))
    sh["dist"] = pd.to_numeric(sh["shotDistance"], errors="coerce")
    sh = sh[sh["dist"] <= RIM_FT]

    m = sh["clock"].str.extract(r"PT(\d+)M([\d.]+)S")
    clock_s = pd.to_numeric(m[0], errors="coerce") * 60 + pd.to_numeric(m[1], errors="coerce")
    per = sh["period"].astype(float)
    plen = np.where(per <= 4, 720.0, 300.0)
    prior = np.where(per <= 4, (per - 1) * 720.0, 2880.0 + (per - 5) * 300.0)
    sh["elapsed"] = prior + (plen - clock_s)
    sh["pid"] = pd.to_numeric(sh["personId"], errors="coerce")
    sh["date"] = pd.to_datetime(sh["gameDateTimeEst"], errors="coerce")
    n0 = len(sh)
    sh = sh.dropna(subset=["elapsed", "pid", "date"])
    sh["pid"] = sh["pid"].astype(int)
    sh["game_id"] = sh["gameId"].astype(str)
    sh["season_year"] = sh["date"].dt.year - (sh["date"].dt.month < 10)
    print(f"{len(sh)} rim (<= {RIM_FT}ft) attempts ({n0 - len(sh)} dropped unparseable); "
          f"league rim FG% {sh['made'].mean():.3f}")
    return sh


def main() -> None:
    st = prepare(adjustments=())   # attachment geometry only

    sh = load_rim_events()
    stc = st[["game_id", "date", "stint_index", "start_elapsed", "seconds"] + HCOLS + ACOLS].copy()
    stc["poss"] = np.maximum(stc["seconds"].to_numpy() / 24.0, 0.1)
    stc = stc.sort_values(["game_id", "start_elapsed"]).reset_index(drop=True)
    stc["k"] = stc.groupby("game_id").cumcount()
    sh = sh[sh["game_id"].isin(set(stc["game_id"]))]

    j = pd.merge_asof(sh.sort_values("elapsed").reset_index(drop=True),
                      stc.sort_values("start_elapsed"),
                      left_on="elapsed", right_on="start_elapsed",
                      by="game_id", direction="backward")

    def membership(frame):
        H = frame[HCOLS].to_numpy()
        A = frame[ACOLS].to_numpy()
        pid = frame["pid"].to_numpy()[:, None]
        return (H == pid).any(axis=1), (A == pid).any(axis=1)

    in_h, in_a = membership(j)
    matched = in_h | in_a
    print(f"matched to assigned stint: {matched.mean():.1%}")
    alt = j[["game_id", "k"]].copy()
    alt["k"] = alt["k"] + 1
    alt = alt.merge(stc, on=["game_id", "k"], how="left")
    alt = alt.fillna({c: -1 for c in HCOLS + ACOLS}).assign(pid=j["pid"].to_numpy())
    fb_h, fb_a = membership(alt)
    fb_ok = (~matched) & (fb_h | fb_a) & alt["stint_index"].notna().to_numpy()
    print(f"fallback rescued {fb_ok.sum()} of {(~matched).sum()} "
          f"-> coverage {(matched | fb_ok).mean():.1%}")

    # side of the SHOOTER = offense; defenders are the other 5
    j["off_side"] = np.where(matched & in_h, "h",
                    np.where(matched & in_a, "a",
                    np.where(fb_ok & fb_h, "h",
                    np.where(fb_ok & fb_a, "a", ""))))
    for c in HCOLS + ACOLS:
        j.loc[fb_ok, c] = alt.loc[fb_ok, c].to_numpy()
    j = j[j["off_side"] != ""].copy()
    j["season_year"] = j["season_year"].astype(int)

    def_cols = np.where((j["off_side"] == "h").to_numpy()[:, None],
                        j[ACOLS].to_numpy(), j[HCOLS].to_numpy())
    long_rows = []
    for k in range(5):
        long_rows.append(pd.DataFrame({
            "pid": def_cols[:, k].astype(int),
            "season_year": j["season_year"].to_numpy(),
            "made": j["made"].to_numpy().astype(int),
        }))
    long_df = pd.concat(long_rows, ignore_index=True)
    rim = long_df.groupby(["pid", "season_year"]).agg(
        rim_att=("made", "size"), rim_make=("made", "sum")).reset_index()

    # possession exposure: same stint-poss convention as the rest of the pipeline
    stc["season_year"] = pd.to_datetime(stc["date"]).dt.year - (pd.to_datetime(stc["date"]).dt.month < 10)
    exp_rows = []
    for cols in (HCOLS, ACOLS):
        for c in cols:
            exp_rows.append(stc[[c, "season_year", "poss"]].rename(columns={c: "pid"}))
    exp = pd.concat(exp_rows, ignore_index=True)
    exp["pid"] = exp["pid"].astype(int)
    exp = exp.groupby(["pid", "season_year"])["poss"].sum().reset_index()

    out = rim.merge(exp, on=["pid", "season_year"], how="left")
    out["rim_att_75"] = out["rim_att"] / out["poss"].clip(lower=1) * 75.0
    out["rim_fg_allow"] = out["rim_make"] / out["rim_att"].clip(lower=1)

    METRIC_DATA.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)
    print(f"wrote {OUT}: {len(out)} player-season rows")
    print(out[out.poss > 3000].describe()[["rim_att_75", "rim_fg_allow"]].round(3))


if __name__ == "__main__":
    main()
