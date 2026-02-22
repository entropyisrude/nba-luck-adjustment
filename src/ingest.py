from __future__ import annotations

import json
import random
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from nba_api.stats.endpoints import playercareerstats


# Cache for player career stats (in-memory)
_career_stats_cache: dict[int, dict[str, float]] = {}
_cache_loaded = False

# File-based cache path
CAREER_CACHE_PATH = Path("data/career_stats_cache.json")


def _load_career_cache() -> None:
    """Load career stats cache from file."""
    global _career_stats_cache, _cache_loaded
    if _cache_loaded:
        return
    if CAREER_CACHE_PATH.exists():
        try:
            with open(CAREER_CACHE_PATH, 'r') as f:
                data = json.load(f)
                # Convert string keys back to int
                _career_stats_cache = {int(k): v for k, v in data.items()}
        except Exception:
            _career_stats_cache = {}
    _cache_loaded = True


def _save_career_cache() -> None:
    """Save career stats cache to file."""
    try:
        CAREER_CACHE_PATH.parent.mkdir(exist_ok=True)
        with open(CAREER_CACHE_PATH, 'w') as f:
            json.dump(_career_stats_cache, f)
    except Exception:
        pass


def get_player_career_3p_stats(player_id: int) -> dict[str, float]:
    """
    Fetch career 3P stats for a player from NBA API.
    Results are cached to disk to avoid repeated API calls.

    Returns dict with 'fg3a' (attempts) and 'fg3m' (makes) career totals.
    Returns zeros if stats can't be fetched.
    """
    _load_career_cache()

    if player_id in _career_stats_cache:
        return _career_stats_cache[player_id]

    try:
        # Small delay to avoid rate limiting
        time.sleep(0.1)

        career = playercareerstats.PlayerCareerStats(player_id=str(player_id))
        totals = career.career_totals_regular_season.get_data_frame()

        if totals.empty:
            result = {'fg3a': 0.0, 'fg3m': 0.0}
        else:
            # Career totals are in a single row
            row = totals.iloc[0]
            result = {
                'fg3a': float(row.get('FG3A', 0) or 0),
                'fg3m': float(row.get('FG3M', 0) or 0),
            }
    except Exception as e:
        # If we can't fetch, default to zeros (will use league prior)
        result = {'fg3a': 0.0, 'fg3m': 0.0}

    _career_stats_cache[player_id] = result
    _save_career_cache()  # Persist to disk
    return result


# --- cdn.nba.com endpoints ---
SCHEDULE_URL = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
BOXSCORE_URL = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"

DEFAULT_TIMEOUT = 30
MAX_RETRIES = 5
BASE_SLEEP = 0.8
JITTER = 0.6

# Cache for schedule (loaded once)
_schedule_cache: dict[str, list[str]] | None = None


def _get_json(url: str) -> dict[str, Any]:
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=DEFAULT_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            sleep_s = BASE_SLEEP * (2 ** (attempt - 1)) + random.random() * JITTER
            time.sleep(sleep_s)
    raise last_err  # type: ignore[misc]


def _load_schedule() -> dict[str, list[str]]:
    """Load full season schedule and build a date -> [gameIds] mapping."""
    global _schedule_cache
    if _schedule_cache is not None:
        return _schedule_cache

    js = _get_json(SCHEDULE_URL)
    schedule = js.get("leagueSchedule", {}) or {}
    game_dates = schedule.get("gameDates", []) or []

    date_to_games: dict[str, list[str]] = {}
    for gd in game_dates:
        # gameDate format: "10/22/2025 00:00:00"
        date_str = gd.get("gameDate", "")
        if not date_str:
            continue
        # Extract MM/DD/YYYY
        date_part = date_str.split(" ")[0]  # "10/22/2025"

        games = gd.get("games", []) or []
        game_ids = []
        for g in games:
            game_id = g.get("gameId", "")
            # Only include regular season games (start with "002")
            # Preseason: 001, Regular: 002, Playoffs: 004
            if game_id and game_id.startswith("002"):
                # Only include completed games (gameStatus == 3)
                if g.get("gameStatus") == 3:
                    game_ids.append(str(game_id))
        if game_ids:
            date_to_games[date_part] = game_ids

    _schedule_cache = date_to_games
    return _schedule_cache


def _mmddyyyy_to_yyyymmdd(mmddyyyy: str) -> str:
    # mmddyyyy like "02/20/2026"
    mm, dd, yyyy = mmddyyyy.split("/")
    return f"{yyyy}{mm}{dd}"


def get_game_ids_for_date(game_date_mmddyyyy: str) -> list[str]:
    """
    Return list of NBA gameIds for a given date (MM/DD/YYYY) using schedule.
    Only returns completed regular season games.
    """
    schedule = _load_schedule()
    return schedule.get(game_date_mmddyyyy, [])


def _get_boxscore_json(game_id: str, game_date_mmddyyyy: str) -> dict[str, Any]:
    url = BOXSCORE_URL.format(game_id=game_id)
    return _get_json(url)


def get_game_home_away_team_ids(game_id: str, game_date_mmddyyyy: str) -> tuple[int, int]:
    """
    Returns (home_team_id, away_team_id) from cdn.nba.com boxscore.
    """
    js = _get_boxscore_json(game_id, game_date_mmddyyyy)
    game = js.get("game", {}) or {}
    home = game.get("homeTeam", {}) or {}
    away = game.get("awayTeam", {}) or {}
    home_id = int(home.get("teamId", 0))
    away_id = int(away.get("teamId", 0))
    return home_id, away_id


def get_boxscore_team_df(game_id: str, game_date_mmddyyyy: str) -> pd.DataFrame:
    """
    Team totals from cdn.nba.com boxscore.
    Returns columns: GAME_ID, TEAM_ID, TEAM_ABBREVIATION, PTS, FG3M, FG3A
    """
    js = _get_boxscore_json(game_id, game_date_mmddyyyy)
    game = js.get("game", {}) or {}

    home = game.get("homeTeam", {}) or {}
    away = game.get("awayTeam", {}) or {}

    def _num(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    rows = []
    for team in [home, away]:
        team_id = int(team.get("teamId", 0))
        tricode = team.get("teamTricode", "") or team.get("triCode", "") or ""
        score = _num(team.get("score", 0))

        # Team statistics contain 3PT data
        stats = team.get("statistics", {}) or {}
        fg3m = _num(stats.get("threePointersMade", 0))
        fg3a = _num(stats.get("threePointersAttempted", 0))

        # Use score from team level, or points from statistics
        pts = score if score > 0 else _num(stats.get("points", 0))

        rows.append(
            {
                "GAME_ID": str(game_id),
                "TEAM_ID": team_id,
                "TEAM_ABBREVIATION": str(tricode),
                "PTS": pts,
                "FG3M": fg3m,
                "FG3A": fg3a,
            }
        )

    return pd.DataFrame(rows)


def get_boxscore_player_df(game_id: str, game_date_mmddyyyy: str) -> pd.DataFrame:
    """
    Player rows from cdn.nba.com boxscore.
    Returns columns: GAME_ID, TEAM_ID, PLAYER_ID, PLAYER_NAME, FG3M, FG3A
    """
    js = _get_boxscore_json(game_id, game_date_mmddyyyy)
    game = js.get("game", {}) or {}

    home = game.get("homeTeam", {}) or {}
    away = game.get("awayTeam", {}) or {}

    def _num(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    rows = []
    for team in [home, away]:
        team_id = int(team.get("teamId", 0))
        players = team.get("players", []) or []

        for p in players:
            # Skip players who didn't play
            if p.get("played") != "1":
                continue

            pid = p.get("personId")
            if pid is None:
                continue

            # Build player name
            first = p.get("firstName", "") or ""
            last = p.get("familyName", "") or p.get("lastName", "") or ""
            name = (first + " " + last).strip() or p.get("name", "") or p.get("nameI", "") or ""

            # Player statistics
            stats = p.get("statistics", {}) or {}
            fg3m = _num(stats.get("threePointersMade", 0))
            fg3a = _num(stats.get("threePointersAttempted", 0))

            rows.append(
                {
                    "GAME_ID": str(game_id),
                    "TEAM_ID": team_id,
                    "PLAYER_ID": int(pid),
                    "PLAYER_NAME": str(name),
                    "FG3M": fg3m,
                    "FG3A": fg3a,
                }
            )

    return pd.DataFrame(rows)
