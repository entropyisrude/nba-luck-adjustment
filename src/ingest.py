from __future__ import annotations

import random
import time
from typing import Any

import pandas as pd
import requests


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
