"""Free-throw variance adjustment, built per stint from play-by-play.

For every free throw 1996-2026: luck = (made - shooter's expected FT%) x
point value, where the shooter expectation is his career-to-date FT%
shrunk toward the league season mean (K=40 attempts), strictly prior
games only. Point value: 1.0 for non-final attempts in a set and for
technicals (dead-ball miss costs exactly the point); final attempts get
1 - ORB_ft x PPP = 0.86 (a missed last FT can be offensive-rebounded).

Events attach to stints by elapsed-time window, validated by the shooter
appearing in the stint's ten (falling back to the adjacent stint —
substitutions happen between free throws); unmatched events are dropped
and counted.

Output: nba-metric-data/ft_stint_adjust.parquet
        (game_id, stint_index, ft_luck_home, ft_luck_away)

Usage: python metric/build_ft_adjust.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_rapm_target import prepare, HCOLS, ACOLS

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PBP = METRIC_DATA / "PlayByPlay.parquet"
OUT = METRIC_DATA / "ft_stint_adjust.parquet"

SHRINK_K = 40.0
FINAL_VALUE = 0.86      # 1 - ORB_ft(0.13) x PPP(1.1)


def load_ft_events() -> pd.DataFrame:
    cols = ["gameId", "personId", "period", "clock", "description",
            "actionType", "gameDateTimeEst"]
    pbp = pd.read_parquet(PBP, columns=cols)
    ft = pbp[pbp["actionType"].str.strip().str.lower().isin(
        ["free throw", "freethrow"])].copy()
    del pbp
    ft["made"] = ~ft["description"].str.contains("MISS", case=False, na=False)
    # set position: "1 of 2" etc.; technicals treated as non-final (dead ball)
    pos = ft["description"].str.extract(r"(\d) of (\d)")
    ft["is_final"] = (pos[0] == pos[1]) & ~ft["description"].str.contains(
        "Technical", case=False, na=False)
    m = ft["clock"].str.extract(r"PT(\d+)M([\d.]+)S")
    clock_s = pd.to_numeric(m[0], errors="coerce") * 60 + pd.to_numeric(m[1], errors="coerce")
    per = ft["period"].astype(float)
    plen = np.where(per <= 4, 720.0, 300.0)
    prior = np.where(per <= 4, (per - 1) * 720.0, 2880.0 + (per - 5) * 300.0)
    ft["elapsed"] = prior + (plen - clock_s)
    ft["pid"] = pd.to_numeric(ft["personId"], errors="coerce")
    n0 = len(ft)
    ft = ft.dropna(subset=["elapsed", "pid"])
    ft["pid"] = ft["pid"].astype(int)
    ft["game_id"] = ft["gameId"].astype(str)
    ft["date"] = pd.to_datetime(ft["gameDateTimeEst"], errors="coerce")
    ft["season_year"] = ft["date"].dt.year - (ft["date"].dt.month < 10)
    print(f"{len(ft)} FT events ({n0 - len(ft)} dropped unparseable)")
    return ft


def add_expectations(ft: pd.DataFrame) -> pd.DataFrame:
    lg = ft.groupby("season_year")["made"].mean()
    ft = ft.sort_values(["pid", "date", "elapsed"]).reset_index(drop=True)
    g = ft.groupby("pid")["made"]
    cum_m = g.cumsum() - ft["made"]
    cum_a = g.cumcount()
    lg_m = ft["season_year"].map(lg).fillna(0.757)
    ft["exp"] = (cum_m + SHRINK_K * lg_m) / (cum_a + SHRINK_K)
    ft["luck"] = (ft["made"].astype(float) - ft["exp"]) * np.where(
        ft["is_final"], FINAL_VALUE, 1.0)
    return ft


def main() -> None:
    st = prepare(apply_ft=False)   # never build the adjustment from adjusted data
    ft = add_expectations(load_ft_events())

    stc = st[["game_id", "stint_index", "start_elapsed"] + HCOLS + ACOLS].copy()
    stc = stc.sort_values(["game_id", "start_elapsed"]).reset_index(drop=True)
    stc["k"] = stc.groupby("game_id").cumcount()
    ft = ft[ft["game_id"].isin(set(stc["game_id"]))]
    j = pd.merge_asof(ft.sort_values("elapsed").reset_index(drop=True),
                      stc.sort_values("start_elapsed"),
                      left_on="elapsed", right_on="start_elapsed",
                      by="game_id", direction="backward")

    def membership(frame, suffix=""):
        H = frame[[c + suffix for c in HCOLS]].to_numpy()
        A = frame[[c + suffix for c in ACOLS]].to_numpy()
        pid = frame["pid"].to_numpy()[:, None]
        return (H == pid).any(axis=1), (A == pid).any(axis=1)

    in_h, in_a = membership(j)
    matched = in_h | in_a
    print(f"matched to assigned stint: {matched.mean():.1%}")

    # boundary fallback: substitutions happen between free throws, so try
    # the NEXT stint when the shooter isn't in the assigned one
    alt = j[["game_id", "k"]].copy()
    alt["k"] = alt["k"] + 1
    alt = alt.merge(stc, on=["game_id", "k"], how="left",
                    suffixes=("", "_n"))
    fb_h, fb_a = membership(alt.fillna({c: -1 for c in HCOLS + ACOLS})
                            .assign(pid=j["pid"].to_numpy()))
    fb_ok = (~matched) & (fb_h | fb_a) & alt["stint_index"].notna().to_numpy()
    print(f"fallback rescued {fb_ok.sum()} of {(~matched).sum()} unmatched "
          f"-> total coverage {(matched | fb_ok).mean():.1%}")

    j["side"] = np.where(matched & in_h, "h",
                np.where(matched & in_a, "a",
                np.where(fb_ok & fb_h, "h",
                np.where(fb_ok & fb_a, "a", ""))))
    j.loc[fb_ok, "stint_index"] = alt.loc[fb_ok, "stint_index"].to_numpy()
    j = j[j["side"] != ""]

    agg = (j.groupby(["game_id", "stint_index", "side"])["luck"].sum()
           .unstack(fill_value=0.0).reset_index()
           .rename(columns={"h": "ft_luck_home", "a": "ft_luck_away"}))
    for c in ("ft_luck_home", "ft_luck_away"):
        if c not in agg.columns:
            agg[c] = 0.0
    agg.to_parquet(OUT, index=False)
    print(f"wrote {OUT}: {len(agg)} stint rows; "
          f"total |luck| {j['luck'].abs().sum():,.0f} pts; "
          f"mean luck {j['luck'].mean():+.4f} (should be ~0)")


if __name__ == "__main__":
    main()
