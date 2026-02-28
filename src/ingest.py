from __future__ import annotations

import json
import random
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.stats.endpoints import boxscoresummaryv2
from nba_api.stats.endpoints import playbyplayv3


# Cache for player career stats (in-memory)
_career_stats_cache: dict[int, dict[str, float]] = {}
_cache_loaded = False

# File-based cache path
CAREER_CACHE_PATH = Path("data/career_stats_cache.json")
LOCAL_PBP_DIR = Path("data/pbp")


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
PLAYBYPLAY_URL = "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"

DEFAULT_TIMEOUT = 30
MAX_RETRIES = 5
BASE_SLEEP = 0.8
JITTER = 0.6
STATS_TIMEOUT = 12
STATS_MAX_RETRIES = 5

# Cache for schedule (loaded once)
_schedule_cache: dict[str, list[str]] | None = None
_local_pbp_cache: dict[int, dict[str, list[dict[str, Any]]]] = {}
_season_schedule_cache: dict[str, dict[str, list[str]]] = {}
_stats_boxscore_cache: dict[str, dict[str, pd.DataFrame]] = {}
_stats_summary_cache: dict[str, tuple[int, int]] = {}

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
}


def _get_json(url: str) -> dict[str, Any]:
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=DEFAULT_TIMEOUT, headers=DEFAULT_HEADERS)
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


def _season_start_year_from_mmddyyyy(game_date_mmddyyyy: str) -> int:
    mm, dd, yyyy = game_date_mmddyyyy.split("/")
    y = int(yyyy)
    m = int(mm)
    return y if m >= 7 else y - 1


def _season_from_mmddyyyy(game_date_mmddyyyy: str) -> str:
    start = _season_start_year_from_mmddyyyy(game_date_mmddyyyy)
    return f"{start}-{(start + 1) % 100:02d}"


def _normalize_statsv3_actions(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for a in actions:
        at = str(a.get("actionType", "") or "")
        desc = str(a.get("description", "") or "")
        desc_lower = str(desc).lower()
        if at in {"Made Shot", "Missed Shot"}:
            if "3pt" in desc_lower or "3-pt" in desc_lower or "3 pt" in desc_lower:
                a["actionType"] = "3pt"
            if not a.get("shotResult"):
                a["shotResult"] = "Made" if at == "Made Shot" else "Missed"
        if "orderNumber" not in a and a.get("actionNumber") is not None:
            a["orderNumber"] = a.get("actionNumber")
    return actions


def _normalize_name(name: str) -> str:
    return "".join(ch for ch in (name or "").lower() if ch.isalnum())


def _expand_local_substitutions(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    team_map: dict[int, dict[str, int]] = {}
    for a in actions:
        try:
            team_id = int(a.get("teamId", 0) or 0)
            pid = int(a.get("personId", 0) or 0)
        except Exception:
            continue
        if team_id <= 0 or pid <= 0:
            continue
        name = str(a.get("playerName", "") or "")
        name_i = str(a.get("playerNameI", "") or "")
        m = team_map.setdefault(team_id, {})
        key_full = _normalize_name(name)
        if key_full:
            m[key_full] = pid
            parts = [p for p in key_full.split() if p]
            if parts:
                m[parts[-1]] = pid
        raw_parts = [p for p in str(name).replace(".", " ").split(" ") if p]
        if raw_parts:
            m[_normalize_name(raw_parts[-1])] = pid
        if name_i:
            m[_normalize_name(name_i)] = pid

    expanded: list[dict[str, Any]] = []
    for a in actions:
        at = str(a.get("actionType", "")).lower()
        if at != "substitution":
            expanded.append(a)
            continue
        desc = str(a.get("description", "") or "")
        if "SUB:" not in desc or " FOR " not in desc:
            expanded.append(a)
            continue
        team_id = int(a.get("teamId", 0) or 0)
        body = desc.split("SUB:", 1)[1].strip()
        try:
            in_name, out_name = body.split(" FOR ", 1)
        except Exception:
            expanded.append(a)
            continue
        team_lookup = team_map.get(team_id, {})
        in_pid = team_lookup.get(_normalize_name(in_name.strip()), 0)
        out_pid = team_lookup.get(_normalize_name(out_name.strip()), 0)
        pid_raw = int(a.get("personId", 0) or 0)
        if pid_raw > 0:
            if in_pid == 0 and out_pid != pid_raw:
                in_pid = pid_raw
            elif out_pid == 0 and in_pid != pid_raw:
                out_pid = pid_raw
            elif in_pid == 0 and out_pid == 0:
                in_pid = pid_raw
        if out_pid > 0 and out_pid != in_pid:
            out_row = dict(a)
            out_row["personId"] = out_pid
            out_row["subType"] = "out"
            expanded.append(out_row)
        in_row = dict(a)
        in_row["personId"] = int(in_pid or 0)
        in_row["subType"] = "in"
        expanded.append(in_row)
    return expanded


def _load_local_pbp_season(season_start_year: int) -> dict[str, list[dict[str, Any]]] | None:
    path = LOCAL_PBP_DIR / f"nbastatsv3_{season_start_year}.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, dtype={"gameId": str})
    if df.empty:
        return {}
    df["gameId"] = df["gameId"].astype(str).str.lstrip("0")
    out: dict[str, list[dict[str, Any]]] = {}
    for gid, grp in df.groupby("gameId"):
        actions = grp.to_dict(orient="records")
        out[gid] = _normalize_statsv3_actions(actions)
    return out


def _get_game_ids_for_date_from_stats_api(
    game_date_mmddyyyy: str, season_type: str = "Regular Season", season_override: str | None = None
) -> list[str]:
    season = season_override if season_override else _season_from_mmddyyyy(game_date_mmddyyyy)
    cache_key = f"{season}_{season_type}"
    if cache_key not in _season_schedule_cache:
        _season_schedule_cache[cache_key] = _load_season_schedule_from_stats_api(season, season_type)
    return _season_schedule_cache.get(cache_key, {}).get(game_date_mmddyyyy, [])


def _load_season_schedule_from_stats_api(season: str, season_type: str = "Regular Season") -> dict[str, list[str]]:
    """
    Load schedule for a season from the NBA stats API.

    Args:
        season: Season string like "2024-25"
        season_type: "Regular Season" or "Playoffs"
    """
    # Game ID prefix: 002 = Regular Season, 004 = Playoffs
    game_id_prefix = "004" if season_type == "Playoffs" else "002"

    last_err: Exception | None = None
    for attempt in range(1, STATS_MAX_RETRIES + 1):
        try:
            lg = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                season_type_nullable=season_type,
                player_or_team_abbreviation="T",
                league_id_nullable="00",
                timeout=max(STATS_TIMEOUT, 45),
            )
            df = lg.get_data_frames()[0]
            if df.empty:
                return {}

            out: dict[str, list[str]] = {}
            games = df[["GAME_ID", "GAME_DATE"]].drop_duplicates()
            for _, r in games.iterrows():
                gid = str(r["GAME_ID"])
                if not gid.startswith(game_id_prefix):
                    continue
                y, m, d = str(r["GAME_DATE"]).split("-")
                mmddyyyy = f"{m}/{d}/{y}"
                out.setdefault(mmddyyyy, []).append(gid)
            return out
        except Exception as e:
            last_err = e
            sleep_s = BASE_SLEEP * (2 ** (attempt - 1)) + random.random() * JITTER
            time.sleep(sleep_s)
    if last_err:
        raise last_err
    return {}


def _load_stats_boxscore(game_id: str) -> dict[str, pd.DataFrame]:
    gid = str(game_id)
    if gid in _stats_boxscore_cache:
        return _stats_boxscore_cache[gid]

    bs = boxscoretraditionalv2.BoxScoreTraditionalV2(
        game_id=gid,
        timeout=max(STATS_TIMEOUT, 45),
    )
    dfs = bs.get_data_frames()
    players = dfs[0] if len(dfs) > 0 else pd.DataFrame()
    teams = dfs[1] if len(dfs) > 1 else pd.DataFrame()
    out = {"players": players, "teams": teams}
    _stats_boxscore_cache[gid] = out
    return out


def _load_stats_home_away(game_id: str) -> tuple[int, int]:
    gid = str(game_id)
    if gid in _stats_summary_cache:
        return _stats_summary_cache[gid]
    sm = boxscoresummaryv2.BoxScoreSummaryV2(
        game_id=gid,
        timeout=max(STATS_TIMEOUT, 45),
    )
    dfs = sm.get_data_frames()
    if not dfs or dfs[0].empty:
        raise RuntimeError(f"boxscoresummaryv2 empty for {gid}")
    row = dfs[0].iloc[0]
    home_id = int(row.get("HOME_TEAM_ID") or 0)
    away_id = int(row.get("VISITOR_TEAM_ID") or 0)
    out = (home_id, away_id)
    _stats_summary_cache[gid] = out
    return out


def get_game_ids_for_date(
    game_date_mmddyyyy: str, season_type: str = "Regular Season", season_override: str | None = None
) -> list[str]:
    """
    Return list of NBA gameIds for a given date (MM/DD/YYYY) using schedule.

    Args:
        game_date_mmddyyyy: Date in MM/DD/YYYY format
        season_type: "Regular Season" or "Playoffs"
        season_override: Optional season string (e.g., "2019-20") to override automatic detection.
                        Useful for edge cases like the COVID bubble where games are played outside
                        the normal season dates.
    """
    # Try CDN schedule first for regular season (faster and more reliable)
    if season_type == "Regular Season" and not season_override:
        schedule = _load_schedule()
        ids = schedule.get(game_date_mmddyyyy, [])
        if ids:
            return ids
    # Fall back to stats API (required for playoffs and season overrides)
    ids = _get_game_ids_for_date_from_stats_api(game_date_mmddyyyy, season_type, season_override)
    return ids if ids else []


def _get_boxscore_json(game_id: str, game_date_mmddyyyy: str) -> dict[str, Any]:
    url = BOXSCORE_URL.format(game_id=game_id)
    return _get_json(url)


def get_game_home_away_team_ids(game_id: str, game_date_mmddyyyy: str) -> tuple[int, int]:
    """
    Returns (home_team_id, away_team_id) from cdn.nba.com boxscore.
    """
    try:
        js = _get_boxscore_json(game_id, game_date_mmddyyyy)
        game = js.get("game", {}) or {}
        home = game.get("homeTeam", {}) or {}
        away = game.get("awayTeam", {}) or {}
        home_id = int(home.get("teamId", 0))
        away_id = int(away.get("teamId", 0))
        if home_id > 0 and away_id > 0:
            return home_id, away_id
    except Exception:
        pass
    return _load_stats_home_away(game_id)


def get_boxscore_team_df(game_id: str, game_date_mmddyyyy: str) -> pd.DataFrame:
    """
    Team totals from cdn.nba.com boxscore.
    Returns columns: GAME_ID, TEAM_ID, TEAM_ABBREVIATION, PTS, FG3M, FG3A
    """
    try:
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
    except Exception:
        stats = _load_stats_boxscore(game_id)["teams"]
        if stats.empty:
            return pd.DataFrame(columns=["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "PTS", "FG3M", "FG3A"])
        out = pd.DataFrame(
            {
                "GAME_ID": stats["GAME_ID"].astype(str),
                "TEAM_ID": stats["TEAM_ID"].astype(int),
                "TEAM_ABBREVIATION": stats["TEAM_ABBREVIATION"].astype(str),
                "PTS": pd.to_numeric(stats["PTS"], errors="coerce").fillna(0.0),
                "FG3M": pd.to_numeric(stats["FG3M"], errors="coerce").fillna(0.0),
                "FG3A": pd.to_numeric(stats["FG3A"], errors="coerce").fillna(0.0),
            }
        )
        return out


def get_boxscore_player_df(game_id: str, game_date_mmddyyyy: str) -> pd.DataFrame:
    """
    Player rows from cdn.nba.com boxscore.
    Returns columns: GAME_ID, TEAM_ID, PLAYER_ID, PLAYER_NAME, FG3M, FG3A
    """
    try:
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
    except Exception:
        players = _load_stats_boxscore(game_id)["players"]
        if players.empty:
            return pd.DataFrame(columns=["GAME_ID", "TEAM_ID", "PLAYER_ID", "PLAYER_NAME", "FG3M", "FG3A"])
        mins = players["MIN"].map(_parse_minutes_any)
        played = (mins > 0.0) & players["COMMENT"].fillna("").eq("")
        p = players.loc[played].copy()
        return pd.DataFrame(
            {
                "GAME_ID": p["GAME_ID"].astype(str),
                "TEAM_ID": p["TEAM_ID"].astype(int),
                "PLAYER_ID": p["PLAYER_ID"].astype(int),
                "PLAYER_NAME": p["PLAYER_NAME"].astype(str),
                "FG3M": pd.to_numeric(p["FG3M"], errors="coerce").fillna(0.0),
                "FG3A": pd.to_numeric(p["FG3A"], errors="coerce").fillna(0.0),
            }
        )


def get_boxscore_players(game_id: str, game_date_mmddyyyy: str) -> pd.DataFrame:
    """
    Player rows from cdn.nba.com boxscore with starter/oncourt flags.
    Returns columns:
        GAME_ID, TEAM_ID, PLAYER_ID, PLAYER_NAME, STARTER, ONCOURT, PLAYED,
        PLUS_MINUS, MINUTES
    """
    try:
        js = _get_boxscore_json(game_id, game_date_mmddyyyy)
        game = js.get("game", {}) or {}

        home = game.get("homeTeam", {}) or {}
        away = game.get("awayTeam", {}) or {}

        rows = []
        for team in [home, away]:
            team_id = int(team.get("teamId", 0))
            players = team.get("players", []) or []

            for p in players:
                pid = p.get("personId")
                if pid is None:
                    continue

                # Build player name
                first = p.get("firstName", "") or ""
                last = p.get("familyName", "") or p.get("lastName", "") or ""
                name = (first + " " + last).strip() or p.get("name", "") or p.get("nameI", "") or ""

                rows.append(
                    {
                        "GAME_ID": str(game_id),
                        "TEAM_ID": team_id,
                        "PLAYER_ID": int(pid),
                        "PLAYER_NAME": str(name),
                        "STARTER": str(p.get("starter", "0") or "0"),
                        "ONCOURT": str(p.get("oncourt", "0") or "0"),
                        "PLAYED": str(p.get("played", "0") or "0"),
                        "PLUS_MINUS": float((p.get("statistics", {}) or {}).get("plusMinusPoints") or 0.0),
                        "MINUTES": _iso_duration_to_minutes((p.get("statistics", {}) or {}).get("minutes")),
                    }
                )

        return pd.DataFrame(rows)
    except Exception:
        players = _load_stats_boxscore(game_id)["players"]
        if players.empty:
            return pd.DataFrame(
                columns=[
                    "GAME_ID",
                    "TEAM_ID",
                    "PLAYER_ID",
                    "PLAYER_NAME",
                    "STARTER",
                    "ONCOURT",
                    "PLAYED",
                    "PLUS_MINUS",
                    "MINUTES",
                ]
            )
        mins = players["MIN"].map(_parse_minutes_any)
        played = (mins > 0.0) & players["COMMENT"].fillna("").eq("")
        starter = players["START_POSITION"].fillna("").astype(str).str.strip().ne("")
        return pd.DataFrame(
            {
                "GAME_ID": players["GAME_ID"].astype(str),
                "TEAM_ID": players["TEAM_ID"].astype(int),
                "PLAYER_ID": players["PLAYER_ID"].astype(int),
                "PLAYER_NAME": players["PLAYER_NAME"].astype(str),
                "NAME_I": players["NICKNAME"].astype(str),
                "STARTER": starter.map(lambda x: "1" if x else "0"),
                "ONCOURT": "0",
                "PLAYED": played.map(lambda x: "1" if x else "0"),
                "PLUS_MINUS": pd.to_numeric(players["PLUS_MINUS"], errors="coerce").fillna(0.0),
                "MINUTES": mins,
            }
        )


def _iso_duration_to_minutes(value: str | None) -> float:
    """Parse ISO-ish NBA duration like PT33M37.00S to decimal minutes."""
    if not value:
        return 0.0
    s = str(value).strip()
    if not s.startswith("PT"):
        return 0.0
    try:
        body = s[2:].replace("S", "")
        if "M" in body:
            mm, ss = body.split("M", 1)
            minutes = int(mm or 0)
            seconds = float(ss or 0.0)
        else:
            minutes = 0
            seconds = float(body or 0.0)
        return round(minutes + seconds / 60.0, 4)
    except Exception:
        return 0.0


def _parse_minutes_any(value: Any) -> float:
    if value is None:
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    if s.startswith("PT"):
        return _iso_duration_to_minutes(s)
    if ":" in s:
        try:
            mm, ss = s.split(":", 1)
            return round(int(mm) + float(ss) / 60.0, 4)
        except Exception:
            return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def get_starters_by_team(game_id: str, game_date_mmddyyyy: str) -> dict[int, list[int]]:
    """
    Return starters per team_id from boxscore.
    """
    players = get_boxscore_players(game_id, game_date_mmddyyyy)
    starters: dict[int, list[int]] = {}
    if players.empty:
        return starters

    for team_id, grp in players.groupby("TEAM_ID"):
        team_id = int(team_id)
        starter_ids = [
            int(pid) for pid in grp.loc[grp["STARTER"] == "1", "PLAYER_ID"].tolist()
        ]
        starters[team_id] = starter_ids
    return starters


def _load_pbp_from_stats_api(game_id: str) -> list[dict[str, Any]]:
    """
    Load play-by-play from NBA stats API (fallback for historical games).
    Uses PlayByPlayV3 which returns CDN-like action schema.
    """
    gid = str(game_id)
    last_err: Exception | None = None
    for attempt in range(1, STATS_MAX_RETRIES + 1):
        try:
            time.sleep(0.5)  # Rate limiting
            pbp = playbyplayv3.PlayByPlayV3(game_id=gid, timeout=max(STATS_TIMEOUT, 60))
            data = pbp.get_dict() or {}
            game = data.get("game", {}) or {}
            actions = game.get("actions", []) or []
            if actions:
                return actions
            return []
        except Exception as e:
            last_err = e
            sleep_s = BASE_SLEEP * (2 ** (attempt - 1)) + random.random() * JITTER
            time.sleep(sleep_s)

    if last_err:
        raise last_err
    return []


def get_playbyplay_actions(game_id: str, game_date_mmddyyyy: str) -> list[dict[str, Any]]:
    """
    Fetch play-by-play data and return all actions.
    Tries local cache first, then CDN, then stats API as fallback.
    """
    gid = str(game_id).lstrip("0")
    season_start = _season_start_year_from_mmddyyyy(game_date_mmddyyyy)
    if season_start not in _local_pbp_cache:
        local = _load_local_pbp_season(season_start)
        if local is not None:
            _local_pbp_cache[season_start] = local
    local_season = _local_pbp_cache.get(season_start)
    if local_season and gid in local_season:
        actions = local_season[gid]
        return _expand_local_substitutions(actions)

    # Try CDN first
    try:
        url = PLAYBYPLAY_URL.format(game_id=game_id)
        pbp_data = _get_json(url)
        game = pbp_data.get("game", {}) or {}
        actions = game.get("actions", []) or []
        if actions:
            return actions
    except Exception:
        pass

    # Fallback to stats API for historical games
    try:
        return _load_pbp_from_stats_api(game_id)
    except Exception:
        return []


def get_player_3pt_df_from_pbp(game_id: str, game_date_mmddyyyy: str) -> pd.DataFrame:
    """
    Build per-player 3PT attempts/makes for a game from play-by-play.
    Returns columns: GAME_ID, TEAM_ID, PLAYER_ID, PLAYER_NAME, FG3M, FG3A
    """
    actions = get_playbyplay_actions(game_id, game_date_mmddyyyy)
    if not actions:
        return pd.DataFrame(columns=["GAME_ID", "TEAM_ID", "PLAYER_ID", "PLAYER_NAME", "FG3M", "FG3A"])

    counts: dict[int, dict[str, Any]] = {}
    for a in actions:
        at = str(a.get("actionType") or "").lower()
        if at != "3pt":
            continue
        try:
            pid = int(a.get("personId", 0) or 0)
        except Exception:
            pid = 0
        if pid <= 0:
            continue
        try:
            team_id = int(a.get("teamId", 0) or 0)
        except Exception:
            team_id = 0
        name = str(a.get("playerName") or a.get("playerNameI") or "").strip()
        made = str(a.get("shotResult") or "").lower() == "made"

        rec = counts.setdefault(pid, {"TEAM_ID": team_id, "PLAYER_NAME": name, "FG3A": 0.0, "FG3M": 0.0})
        if not rec.get("PLAYER_NAME") and name:
            rec["PLAYER_NAME"] = name
        if rec.get("TEAM_ID", 0) in (0, None) and team_id:
            rec["TEAM_ID"] = team_id
        rec["FG3A"] += 1.0
        if made:
            rec["FG3M"] += 1.0

    rows = []
    for pid, rec in counts.items():
        rows.append(
            {
                "GAME_ID": str(game_id),
                "TEAM_ID": int(rec.get("TEAM_ID") or 0),
                "PLAYER_ID": int(pid),
                "PLAYER_NAME": str(rec.get("PLAYER_NAME") or ""),
                "FG3M": float(rec.get("FG3M") or 0.0),
                "FG3A": float(rec.get("FG3A") or 0.0),
            }
        )
    return pd.DataFrame(rows)




def get_playbyplay_3pt_shots(game_id: str, game_date_mmddyyyy: str) -> pd.DataFrame:
    """
    Fetch play-by-play data and extract all 3PT shot attempts with context.

    Returns DataFrame with columns:
        GAME_ID, TEAM_ID, PLAYER_ID, PLAYER_NAME, MADE, AREA, SHOT_TYPE

    AREA: 'corner' or 'above_break'
    SHOT_TYPE: 'pullup', 'stepback', 'catch_shoot', etc.
    """
    url = PLAYBYPLAY_URL.format(game_id=game_id)
    pbp_data = _get_json(url)

    game = pbp_data.get("game", {})
    actions = game.get("actions", [])

    shots = []
    for action in actions:
        desc = action.get("description", "")
        desc_lower = str(desc).lower()

        # Look for 3PT shots by description
        is_3pt = "3pt" in desc_lower or "3-pt" in desc_lower

        if is_3pt:
            # Determine if made or missed from description
            made = 0 if desc_lower.startswith("miss") else 1

            # Get area (corner vs above break)
            area_raw = str(action.get("area", "") or "").lower()
            if "corner" in area_raw:
                area = "corner"
            else:
                area = "above_break"

            # Get shot type from description
            shot_type = _classify_shot_type(desc_lower)

            shots.append({
                "GAME_ID": str(game_id),
                "TEAM_ID": action.get("teamId"),
                "PLAYER_ID": action.get("personId"),
                "PLAYER_NAME": action.get("playerNameI", ""),
                "MADE": made,
                "AREA": area,
                "SHOT_TYPE": shot_type,
            })

    return pd.DataFrame(shots)


def _classify_shot_type(desc_lower: str) -> str:
    """Classify shot type from play-by-play description."""
    if "step back" in desc_lower or "stepback" in desc_lower:
        return "stepback"
    elif "pullup" in desc_lower or "pull-up" in desc_lower or "pull up" in desc_lower:
        return "pullup"
    elif "running" in desc_lower or "driving" in desc_lower:
        return "running"
    elif "fadeaway" in desc_lower or "fade away" in desc_lower:
        return "fadeaway"
    elif "turnaround" in desc_lower or "turn around" in desc_lower:
        return "turnaround"
    else:
        # Default to catch-and-shoot (most common for unspecified 3PT)
        return "catch_shoot"
