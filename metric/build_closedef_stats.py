"""
Pull stats.nba.com's closest-defender shot-outcome data (leaguedashptdefend)
-- the "Defense Dashboard" -- for the tracking-vs-box-score defense test
(2026-07-14): does closest-defender-attributed matchup data explain
defensive RAPM in a way our box prior can't, the way EPM's use of tracking
data plausibly does?

This is genuinely different in kind from rim_defense_season.parquet (which
is a team on/off read from raw shot events -- "was this player on the floor
while the opponent shot well at the rim") -- this is MATCHUP-attributed:
opponent FG% specifically when THIS PLAYER was the closest defender on that
possession, tracked via NBA optical tracking (SportVU/Second Spectrum) and
published by the league itself since 2013-14 (0 rows for 2012-13, confirmed
by direct query -- this start date matches what's independently reported
about EPM's own tracking-data usage starting the same season).

Publicly accessible, no API key -- just needs browser-like headers
(NBA blocks bare urllib/requests without them). CLOSE_DEF_PERSON_ID is the
league's own player ID, same scheme used everywhere else in this project,
so no name-matching is needed.

Pulls two categories per season:
  Overall        -- all shot distances
  Less Than 6Ft   -- rim defense specifically, comparable to (but matchup-
                     attributed, unlike) rim_defense_season.parquet

Output: nba-metric-data/closedef_stats.parquet
  (pid, season_year, cd_fga, cd_fg_pct, cd_pct_plusminus,
        cd_rim_fga, cd_rim_fg_pct, cd_rim_pct_plusminus)
  pct_plusminus = defended FG% minus the shooter's own normal FG% on that
  shot type -- negative means shooters shoot WORSE than their norm against
  this defender (suppression), positive means better (porous).

Usage: python metric/build_closedef_stats.py
"""
from __future__ import annotations

import json
import time
import urllib.request
from pathlib import Path

import pandas as pd

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
OUT = METRIC_DATA / "closedef_stats.parquet"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Referer": "https://www.nba.com/", "Origin": "https://www.nba.com",
    "x-nba-stats-origin": "stats", "x-nba-stats-token": "true",
    "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9",
}
FIRST_SEASON = 2013
LAST_SEASON = 2025
CATEGORIES = {"Overall": "cd", "Less Than 6Ft": "cd_rim"}


def season_label(y: int) -> str:
    return f"{y}-{str(y + 1)[-2:]}"


def fetch(season: str, category: str) -> pd.DataFrame:
    catq = category.replace(" ", "+")
    url = (f"https://stats.nba.com/stats/leaguedashptdefend?LeagueID=00&Season={season}"
           f"&SeasonType=Regular+Season&PerMode=PerGame&DefenseCategory={catq}")
    req = urllib.request.Request(url, headers=HEADERS)
    resp = urllib.request.urlopen(req, timeout=30)
    data = json.load(resp)
    rs = data["resultSets"][0]
    return pd.DataFrame(rs["rowSet"], columns=rs["headers"])


def main() -> None:
    frames = []
    for y in range(FIRST_SEASON, LAST_SEASON + 1):
        label = season_label(y)
        per_cat = {}
        for cat, prefix in CATEGORIES.items():
            try:
                df = fetch(label, cat)
            except Exception as e:
                print(f"  {label} {cat}: FAILED ({e})")
                continue
            if df.empty:
                print(f"  {label} {cat}: no data")
                continue
            # column names vary by category ("D_FGA" for Overall vs "FGA_LT_06"
            # for "Less Than 6Ft" etc.) but the last 5 columns are always in
            # the same fixed order: [made, attempted, defended%, normal%,
            # plusminus] -- use position, not name, to stay robust across cats
            made, fga, def_pct, norm_pct, plusminus = df.columns[-5:]
            df = df.rename(columns={"CLOSE_DEF_PERSON_ID": "pid", fga: f"{prefix}_fga",
                                     def_pct: f"{prefix}_fg_pct",
                                     plusminus: f"{prefix}_pct_plusminus"})
            per_cat[prefix] = df[["pid", f"{prefix}_fga", f"{prefix}_fg_pct", f"{prefix}_pct_plusminus"]]
            time.sleep(0.8)
        if not per_cat:
            continue
        merged = None
        for d in per_cat.values():
            merged = d if merged is None else merged.merge(d, on="pid", how="outer")
        merged["season_year"] = y
        frames.append(merged)
        print(f"  {label}: {len(merged)} players")

    out = pd.concat(frames, ignore_index=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT}: {len(out):,} player-season rows, {out.season_year.min()}-{out.season_year.max()}")


if __name__ == "__main__":
    main()
