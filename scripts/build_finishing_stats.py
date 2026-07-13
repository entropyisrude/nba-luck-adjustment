"""
Build rim-finishing stats (rim attempts, rim FG%, dunks made) per player-season
for the Player Similarity Machine's "Finishing" weight dimension.

Source: nba-metric-data/PlayByPlay.parquet (personId, shotDistance, actionType,
description). Coverage is 1996-97+ only (same as on/off) -- there's no
play-by-play with shot distance for the pre-1996 Eoin-era seasons.

Two different reliability profiles, both already learned the hard way on this
project (see the mid-range luck adjuster's PBP schema note):
  - rim attempts/makes use the numeric shotDistance field (<=4ft, the NBA's
    restricted-area radius) -- not keyword-based, no era bias, same approach
    already validated for metric/build_rim_defense.py and build_midrange_adjust.py.
  - dunks are counted from MADE shots only where the description contains
    "dunk". Missed dunks are NOT counted -- the "dunk" keyword is wildly
    under-applied to missed-shot descriptions pre-2019 (0.8% of misses vs
    10% of makes tagged "dunk" in the old "Made Shot"/"Missed Shot" schema,
    vs 2.4%/16% in the post-2019 "2pt" schema) -- the same asymmetric-
    keyword-tagging trap found during the mid-range shot-type analysis.
    Restricting to makes only sidesteps it. The made-dunk tag rate is a
    smooth ~9-11% from 1996-2018 rising organically to ~17% by 2024-25 (no
    discontinuity at the 2019 schema switch), so it's trustworthy across the
    full 1996-2025 range as "dunks made", not "dunks attempted".

Output: data/finishing_stats.json  {(pid, season_year): {rim_fga, rim_fgm, dunks}}

Usage: python scripts/build_finishing_stats.py
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PBP = METRIC_DATA / "PlayByPlay.parquet"
OUT = Path(__file__).resolve().parent.parent / "data" / "finishing_stats.json"

RIM_FT = 4.0


def main() -> None:
    cols = ["gameId", "personId", "period", "clock", "description", "actionType",
            "shotDistance", "gameDateTimeEst"]
    pbp = pd.read_parquet(PBP, columns=cols)
    at = pbp["actionType"].str.strip().str.lower()
    old2 = at.isin(["made shot", "missed shot"]) & ~pbp["description"].str.contains("3PT", na=False)
    new2 = at == "2pt"
    sh = pbp[old2 | new2].copy()
    del pbp
    at = sh["actionType"].str.strip().str.lower()
    sh["made"] = np.where(at == "made shot", True,
                 np.where(at == "missed shot", False,
                          ~sh["description"].str.contains("MISS", case=False, na=False)))
    sh["dist"] = pd.to_numeric(sh["shotDistance"], errors="coerce")
    sh["pid"] = pd.to_numeric(sh["personId"], errors="coerce")
    sh["date"] = pd.to_datetime(sh["gameDateTimeEst"], errors="coerce")
    sh = sh.dropna(subset=["pid", "date"])
    sh["pid"] = sh["pid"].astype(int)
    sh["season_year"] = sh["date"].dt.year - (sh["date"].dt.month < 10)

    # regular season only -- NBA game_id convention: leading digit 1=preseason,
    # 2=regular season, 3=all-star, 4=playoffs, 5=play-in (same '2%' filter
    # used everywhere else in this codebase, e.g. player_game_facts queries).
    # Missed pre-fix: preseason games leaked in and inflated volume stats
    # (caught via user cross-check of Mohamed Diawara's rim FGA/36 against
    # Basketball-Reference, 2026-07-14 -- see git log for before/after).
    n0 = len(sh)
    sh = sh[sh["gameId"].astype(str).str.startswith("2")]
    print(f"  regular-season filter: {n0:,} -> {len(sh):,} shot rows "
          f"({n0 - len(sh):,} preseason/playoff/other dropped)")

    sh["is_rim"] = sh["dist"] <= RIM_FT
    sh["is_dunk_make"] = sh["made"] & sh["description"].str.contains("dunk", case=False, na=False)

    sh["is_rim_make"] = sh["is_rim"] & sh["made"]
    agg = sh.groupby(["pid", "season_year"]).agg(
        rim_fga=("is_rim", "sum"),
        rim_fgm=("is_rim_make", "sum"),
        dunks=("is_dunk_make", "sum"),
    ).reset_index()

    out = {}
    for r in agg.itertuples(index=False):
        out[f"{r.pid}_{r.season_year}"] = {
            "rim_fga": int(r.rim_fga), "rim_fgm": int(r.rim_fgm), "dunks": int(r.dunks)}

    OUT.parent.mkdir(exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"))
    print(f"Wrote {len(out):,} player-season finishing rows to {OUT}")

    made_pool = sh[sh["made"]]
    tag_rate = made_pool.groupby("season_year")["is_dunk_make"].mean()
    print("\nmade-dunk tag rate by season (sanity check, should be smooth):")
    print(tag_rate.round(3).to_string())


if __name__ == "__main__":
    main()
