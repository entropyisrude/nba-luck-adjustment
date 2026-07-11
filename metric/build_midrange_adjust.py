"""Mid-range shooting variance adjustment, built per stint from play-by-play.

For every 2-point attempt from 10-23 feet, 1996-2026: luck = (made -
shooter's expected make rate) x 1.707, where 1.707 = 2 - ORB(0.2608) x
PPP(1.124) (a missed FG can be offensive-rebounded — same convention as
the site's 3PT model). Expectation = the shooter's career-to-date make
rate WITHIN the distance band (10-15 ft / 16-23 ft), strictly prior
games, shrunk toward the league season band mean with K=100 attempts.

Shot selection is untouched (a player forced into tough mid-ranges keeps
that cost); only the make/miss residual vs his own expectation is muted.
Stint attachment mirrors build_ft_adjust (clock window + shooter-in-
lineup validation + next-stint fallback).

Output: nba-metric-data/midrange_stint_adjust.parquet
        (game_id, stint_index, mr_luck_home, mr_luck_away)

Usage: python metric/build_midrange_adjust.py
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
OUT = METRIC_DATA / "midrange_stint_adjust.parquet"

SHRINK_K = 100.0
MAKE_VALUE = 1.707      # 2 - ORB x PPP
BANDS = [(10.0, 16.0), (16.0, 23.99)]


def load_mid_events() -> pd.DataFrame:
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
    sh = sh[(sh["dist"] >= BANDS[0][0]) & (sh["dist"] <= BANDS[-1][1])]
    sh["band"] = np.where(sh["dist"] < BANDS[0][1], 0, 1)

    m = sh["clock"].str.extract(r"PT(\d+)M([\d.]+)S")
    clock_s = pd.to_numeric(m[0], errors="coerce") * 60 + pd.to_numeric(m[1], errors="coerce")
    per = sh["period"].astype(float)
    plen = np.where(per <= 4, 720.0, 300.0)
    prior = np.where(per <= 4, (per - 1) * 720.0, 2880.0 + (per - 5) * 300.0)
    sh["elapsed"] = prior + (plen - clock_s)
    sh["pid"] = pd.to_numeric(sh["personId"], errors="coerce")
    n0 = len(sh)
    sh = sh.dropna(subset=["elapsed", "pid"])
    sh["pid"] = sh["pid"].astype(int)
    sh["game_id"] = sh["gameId"].astype(str)
    sh["date"] = pd.to_datetime(sh["gameDateTimeEst"], errors="coerce")
    sh["season_year"] = sh["date"].dt.year - (sh["date"].dt.month < 10)
    print(f"{len(sh)} mid-range attempts ({n0 - len(sh)} dropped unparseable); "
          f"league make rate {sh['made'].mean():.3f}")
    return sh


def add_expectations(sh: pd.DataFrame) -> pd.DataFrame:
    lg = sh.groupby(["season_year", "band"])["made"].mean().rename("lg")
    sh = sh.sort_values(["pid", "band", "date", "elapsed"]).reset_index(drop=True)
    g = sh.groupby(["pid", "band"])["made"]
    cum_m = g.cumsum() - sh["made"]
    cum_a = g.cumcount()
    lg_m = sh.merge(lg, left_on=["season_year", "band"], right_index=True,
                    how="left")["lg"].fillna(0.41).to_numpy()
    sh["exp"] = (cum_m + SHRINK_K * lg_m) / (cum_a + SHRINK_K)
    sh["luck"] = (sh["made"].astype(float) - sh["exp"]) * MAKE_VALUE
    return sh


def main() -> None:
    st = prepare(adjustments=())   # attachment geometry only; pts unused
    sh = add_expectations(load_mid_events())

    stc = st[["game_id", "stint_index", "start_elapsed"] + HCOLS + ACOLS].copy()
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

    j["side"] = np.where(matched & in_h, "h",
                np.where(matched & in_a, "a",
                np.where(fb_ok & fb_h, "h",
                np.where(fb_ok & fb_a, "a", ""))))
    j.loc[fb_ok, "stint_index"] = alt.loc[fb_ok, "stint_index"].to_numpy()
    j = j[j["side"] != ""]

    agg = (j.groupby(["game_id", "stint_index", "side"])["luck"].sum()
           .unstack(fill_value=0.0).reset_index()
           .rename(columns={"h": "mr_luck_home", "a": "mr_luck_away"}))
    for c in ("mr_luck_home", "mr_luck_away"):
        if c not in agg.columns:
            agg[c] = 0.0
    agg.to_parquet(OUT, index=False)
    print(f"wrote {OUT}: {len(agg)} stint rows; mean luck {j['luck'].mean():+.4f}")


if __name__ == "__main__":
    main()
