"""
Pull stats.nba.com's SpeedDistance tracking stats (leaguedashptstats) --
distance covered and average speed, split by offense/defense -- to test
whether defensive-specific movement/effort data explains defensive RAPM
in a way box stats can't. Same public-API family as
build_closedef_stats.py (closest-defender shot outcomes), same 2013-14
start (SportVU/Second Spectrum tracking era), no special access needed.

Publicly accessible but this particular endpoint needs the FULL parameter
set present (even blank) or the NBA API 500s -- checked directly.

Output: nba-metric-data/speed_stats.parquet
  (pid, season_year, dist_miles, dist_miles_off, dist_miles_def,
        avg_speed, avg_speed_off, avg_speed_def)

Usage: python metric/build_speed_stats.py
"""
from __future__ import annotations

import time
import urllib.parse
import urllib.request
import json
from pathlib import Path

import pandas as pd

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
OUT = METRIC_DATA / "speed_stats.parquet"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Referer": "https://www.nba.com/", "Origin": "https://www.nba.com",
    "x-nba-stats-origin": "stats", "x-nba-stats-token": "true",
    "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9",
}
BASE_PARAMS = {
    "LeagueID": "00", "SeasonType": "Regular Season", "PerMode": "PerGame",
    "PlayerOrTeam": "Player", "PtMeasureType": "SpeedDistance",
    "College": "", "Conference": "", "Country": "", "DateFrom": "", "DateTo": "",
    "Division": "", "DraftPick": "", "DraftYear": "", "GameSegment": "", "Height": "",
    "LastNGames": "0", "Location": "", "Month": "0", "OpponentTeamID": "0", "Outcome": "",
    "PORound": "0", "Period": "0", "PlayerExperience": "", "PlayerPosition": "",
    "SeasonSegment": "", "ShotClockRange": "", "StarterBench": "", "TeamID": "0",
    "VsConference": "", "VsDivision": "", "Weight": "",
}
FIRST_SEASON = 2013
LAST_SEASON = 2025


def season_label(y: int) -> str:
    return f"{y}-{str(y + 1)[-2:]}"


def fetch(season: str) -> pd.DataFrame:
    params = dict(BASE_PARAMS, Season=season)
    url = "https://stats.nba.com/stats/leaguedashptstats?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=HEADERS)
    resp = urllib.request.urlopen(req, timeout=30)
    data = json.load(resp)
    rs = data["resultSets"][0]
    return pd.DataFrame(rs["rowSet"], columns=rs["headers"])


def main() -> None:
    frames = []
    for y in range(FIRST_SEASON, LAST_SEASON + 1):
        label = season_label(y)
        try:
            df = fetch(label)
        except Exception as e:
            print(f"  {label}: FAILED ({e})")
            continue
        if df.empty:
            print(f"  {label}: no data")
            continue
        df = df.rename(columns={
            "PLAYER_ID": "pid", "DIST_MILES": "dist_miles", "DIST_MILES_OFF": "dist_miles_off",
            "DIST_MILES_DEF": "dist_miles_def", "AVG_SPEED": "avg_speed",
            "AVG_SPEED_OFF": "avg_speed_off", "AVG_SPEED_DEF": "avg_speed_def"})
        df["season_year"] = y
        keep = ["pid", "season_year", "dist_miles", "dist_miles_off", "dist_miles_def",
                "avg_speed", "avg_speed_off", "avg_speed_def"]
        frames.append(df[keep])
        print(f"  {label}: {len(df)} players")
        time.sleep(0.8)

    out = pd.concat(frames, ignore_index=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT}: {len(out):,} player-season rows, {out.season_year.min()}-{out.season_year.max()}")


if __name__ == "__main__":
    main()
