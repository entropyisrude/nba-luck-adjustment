from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path

import pandas as pd
import requests


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
CACHE_DIR = DATA_DIR / "hustle_cache"

HEADERS = {
    "Host": "stats.nba.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

URL = "https://stats.nba.com/stats/leaguehustlestatsplayer"


def season_range(start_year: int, end_year: int) -> list[str]:
    return [f"{y}-{str(y + 1)[-2:]}" for y in range(start_year, end_year + 1)]


def fetch_season(session: requests.Session, season: str, season_type: str = "Regular Season", retries: int = 5) -> pd.DataFrame:
    params = {
        "College": "",
        "Conference": "",
        "Country": "",
        "DateFrom": "",
        "DateTo": "",
        "Division": "",
        "DraftPick": "",
        "DraftYear": "",
        "Height": "",
        "LeagueID": "00",
        "Location": "",
        "Month": "0",
        "OpponentTeamID": "0",
        "Outcome": "",
        "PORound": "0",
        "PerMode": "PerGame",
        "PlayerExperience": "",
        "PlayerPosition": "",
        "Season": season,
        "SeasonSegment": "",
        "SeasonType": season_type,
        "TeamID": "0",
        "Weight": "",
    }
    last_error: str | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(URL, params=params, headers=HEADERS, timeout=60)
            status = resp.status_code
            if status == 200:
                payload = resp.json()
                result = payload["resultSets"][0]
                df = pd.DataFrame(result["rowSet"], columns=result["headers"])
                df["SEASON"] = season
                df["SEASON_TYPE"] = season_type
                return df
            last_error = f"status_{status}"
        except Exception as exc:
            last_error = repr(exc)
        sleep_s = min(180, 20 * attempt + random.uniform(3, 12))
        print(f"{season} {season_type}: attempt {attempt} failed ({last_error}); sleeping {sleep_s:.1f}s", flush=True)
        time.sleep(sleep_s)
    raise RuntimeError(f"{season} {season_type}: failed after {retries} attempts ({last_error})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch season-level player hustle stats from stats.nba.com.")
    parser.add_argument("--start-year", type=int, default=2016)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--season-type", default="Regular Season")
    parser.add_argument("--descending", action="store_true")
    parser.add_argument("--sleep-min", type=float, default=18.0)
    parser.add_argument("--sleep-max", type=float, default=28.0)
    parser.add_argument("--output", default=str(DATA_DIR / "player_hustle_by_season.csv"))
    args = parser.parse_args()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    seasons = season_range(args.start_year, args.end_year)
    if args.descending:
        seasons = list(reversed(seasons))
    session = requests.Session()
    frames: list[pd.DataFrame] = []
    progress_path = CACHE_DIR / "player_hustle_fetch_progress.json"

    progress: dict[str, dict[str, str | int]] = {}
    if progress_path.exists():
        try:
            progress = json.loads(progress_path.read_text(encoding="utf-8"))
        except Exception:
            progress = {}

    for idx, season in enumerate(seasons, start=1):
        cache_path = CACHE_DIR / f"player_hustle_{season.replace('-', '_')}_{args.season_type.lower().replace(' ', '_')}.csv"
        if cache_path.exists():
            df = pd.read_csv(cache_path)
            frames.append(df)
            print(f"{idx}/{len(seasons)} {season}: loaded cache ({len(df)} rows)", flush=True)
        else:
            print(f"{idx}/{len(seasons)} {season}: fetching...", flush=True)
            df = fetch_season(session, season, season_type=args.season_type)
            df.to_csv(cache_path, index=False)
            frames.append(df)
            print(f"{idx}/{len(seasons)} {season}: saved {len(df)} rows", flush=True)
        progress[season] = {"rows": int(len(frames[-1])), "cache": str(cache_path)}
        progress_path.write_text(json.dumps(progress, indent=2), encoding="utf-8")
        if idx < len(seasons):
            sleep_s = random.uniform(args.sleep_min, args.sleep_max)
            print(f"cooldown {sleep_s:.1f}s", flush=True)
            time.sleep(sleep_s)

    combined = pd.concat(frames, ignore_index=True)
    combined.to_csv(args.output, index=False)
    print(f"wrote {len(combined)} rows to {args.output}", flush=True)


if __name__ == "__main__":
    main()
