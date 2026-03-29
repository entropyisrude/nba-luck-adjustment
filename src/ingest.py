from __future__ import annotations

import json
import os
from datetime import datetime
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
from nba_api.stats.endpoints import gamerotation
from nba_api.stats.endpoints import playbyplayv3


# Cache for player career stats (in-memory)
_career_stats_cache: dict[int, dict[str, float]] = {}
_cache_loaded = False

# File-based cache path
CAREER_CACHE_PATH = Path("data/career_stats_cache.json")
LOCAL_PBP_DIRS = [
    Path("data/pbp"),
    Path("/mnt/c/users/dave/Downloads/nba-3pt-adjust-local-backups/untracked_20260312_081227/data/pbp"),
    Path("/mnt/c/users/dave/Downloads/nba-3pt-adjust-local-backups/untracked_20260312_081227/_local_untracked_backup_20260309_111115"),
]
HISTORICAL_PLAYOFF_PBP_DIR = Path("data/historical_pbp")
STATS_CACHE_DIR = Path("data/stats_cache")
STATS_BOXSCORE_DIR = STATS_CACHE_DIR / "boxscoretraditionalv2"
STATS_SUMMARY_DIR = STATS_CACHE_DIR / "boxscoresummaryv2"
STATS_ROTATION_DIR = STATS_CACHE_DIR / "gamerotation"

# Historical feeds occasionally swap a player's NBA person id mid-career.
# Keep this map empty until a merge is verified from full-name and date-range
# evidence; surname-only rebuilt rows are not reliable enough on their own.
CANONICAL_PLAYER_ID_MAP: dict[int, int] = {}


def canonicalize_player_id(player_id: Any) -> int:
    try:
        pid = int(player_id)
    except Exception:
        return 0
    if pid <= 0:
        return 0
    seen: set[int] = set()
    while pid in CANONICAL_PLAYER_ID_MAP and pid not in seen:
        seen.add(pid)
        pid = CANONICAL_PLAYER_ID_MAP[pid]
    return pid


def canonicalize_action_player_ids(action: dict[str, Any]) -> dict[str, Any]:
    out = dict(action)
    for key in ("personId", "playerId"):
        if key in out:
            pid = canonicalize_player_id(out.get(key))
            if pid > 0:
                out[key] = pid
    candidates = out.get("candidatePersonIds") or []
    canon_candidates: list[int] = []
    for raw in candidates:
        pid = canonicalize_player_id(raw)
        if pid > 0 and pid not in canon_candidates:
            canon_candidates.append(pid)
    if canon_candidates:
        out["candidatePersonIds"] = canon_candidates
    return out


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


def _cache_read_df(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_json(path, orient="split")
    except Exception:
        return None


def _cache_write_df(path: Path, df: pd.DataFrame) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_json(path, orient="split")
    except Exception:
        pass


def _cache_read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _cache_write_json(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception:
        pass


def _stats_cache_only() -> bool:
    return os.getenv("NBA_STATS_CACHE_ONLY", "").strip().lower() in {"1", "true", "yes"}


def _game_id_cache_keys(game_id: str) -> list[str]:
    gid = str(game_id)
    keys: list[str] = []
    if gid:
        keys.append(gid)
    if gid.isdigit():
        padded = gid.zfill(10)
        if padded not in keys:
            keys.insert(0, padded)
        stripped = gid.lstrip("0") or "0"
        if stripped not in keys:
            keys.append(stripped)
    return keys


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
_local_team_alias_cache: dict[int, dict[int, dict[str, set[int]]]] = {}
_historical_playoff_pbp_cache: dict[int, dict[str, list[dict[str, Any]]]] = {}
_historical_playoff_team_alias_cache: dict[int, dict[int, dict[str, set[int]]]] = {}
_season_schedule_cache: dict[str, dict[str, list[str]]] = {}
_stats_boxscore_cache: dict[str, dict[str, pd.DataFrame]] = {}
_stats_summary_cache: dict[str, tuple[int, int]] = {}
_stats_rotation_cache: dict[str, pd.DataFrame] = {}

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
    # NBA seasons roll over with the fall schedule, not the summer bubble.
    return y if m >= 9 else y - 1


def _calendar_year_from_mmddyyyy(game_date_mmddyyyy: str) -> int:
    return int(game_date_mmddyyyy.split("/")[2])


def _local_pbp_season_years(game_date_mmddyyyy: str) -> list[int]:
    # Local statsv3 archives are stored by calendar year, while the rest of the
    # project commonly reasons in season-start years. Try both.
    years: list[int] = []
    for year in (
        _season_start_year_from_mmddyyyy(game_date_mmddyyyy),
        _calendar_year_from_mmddyyyy(game_date_mmddyyyy),
    ):
        if year not in years:
            years.append(year)
    return years


def _local_pbp_game_ids(game_id: str) -> list[str]:
    gid = str(game_id).lstrip("0")
    return [gid] if gid else []


def _historical_playoff_file_year_from_game_id(game_id: str) -> int | None:
    gid = str(game_id).lstrip("0")
    if not gid.startswith("4") or len(gid) < 3:
        return None
    try:
        yy = int(gid[1:3])
    except Exception:
        return None
    return 1900 + yy if yy >= 90 else 2000 + yy


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
            else:
                # Non-3PT field goals are 2PT
                a["actionType"] = "2pt"
            if not a.get("shotResult"):
                a["shotResult"] = "Made" if at == "Made Shot" else "Missed"
        # Normalize free throws and jump balls for consistent lowercase matching
        elif at == "Free Throw":
            a["actionType"] = "freethrow"
        elif at == "Jump Ball":
            a["actionType"] = "jumpball"
        if "orderNumber" not in a and a.get("actionNumber") is not None:
            a["orderNumber"] = a.get("actionNumber")
    return actions


def _clock_to_iso_duration(clock_text: str | None) -> str | None:
    text = str(clock_text or "").strip()
    if not text:
        return None
    if text.startswith("PT"):
        return text
    if ":" not in text:
        return None
    try:
        mm, ss = text.split(":", 1)
        seconds = float(ss)
        return f"PT{int(mm)}M{seconds:05.2f}S"
    except Exception:
        return None


def _parse_historical_score(score_text: str | None) -> tuple[int | None, int | None]:
    text = str(score_text or "").strip()
    if " - " not in text:
        return None, None
    try:
        away_score, home_score = text.split(" - ", 1)
        return int(home_score), int(away_score)
    except Exception:
        return None, None


def _first_nonempty_text(*values: Any) -> str:
    for value in values:
        try:
            if pd.isna(value):
                continue
        except Exception:
            pass
        text = str(value or "").strip()
        if text and text.lower() != "nan":
            return text
    return ""


def _historical_event_action_type(event_type: int) -> str:
    return {
        1: "Made Shot",
        2: "Missed Shot",
        3: "Free Throw",
        4: "Rebound",
        5: "Turnover",
        6: "Foul",
        8: "Substitution",
        9: "Timeout",
        10: "Jump Ball",
        11: "Ejection",
        12: "Period",
        13: "Period",
    }.get(event_type, "")


def _load_historical_playoff_pbp_season(season_start_year: int) -> dict[str, list[dict[str, Any]]] | None:
    path = HISTORICAL_PLAYOFF_PBP_DIR / f"nbastats_po_{season_start_year}.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, dtype={"GAME_ID": str})
    if df.empty:
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    team_alias_map: dict[int, dict[str, set[int]]] = {}
    for gid, grp in df.groupby("GAME_ID"):
        actions: list[dict[str, Any]] = []
        for row in grp.to_dict(orient="records"):
            try:
                event_type = int(row.get("EVENTMSGTYPE", 0) or 0)
            except Exception:
                event_type = 0
            desc = _first_nonempty_text(
                row.get("HOMEDESCRIPTION"),
                row.get("VISITORDESCRIPTION"),
                row.get("NEUTRALDESCRIPTION"),
            )
            home_score, away_score = _parse_historical_score(row.get("SCORE"))
            try:
                person_id = canonicalize_player_id(row.get("PLAYER1_ID", 0) or 0)
            except Exception:
                person_id = 0
            try:
                team_id = int(row.get("PLAYER1_TEAM_ID", 0) or 0)
            except Exception:
                team_id = 0
            if team_id <= 0:
                try:
                    team_id = int(row.get("PLAYER2_TEAM_ID", 0) or 0)
                except Exception:
                    team_id = 0
            action = {
                "actionNumber": int(row.get("EVENTNUM", 0) or 0),
                "orderNumber": int(row.get("EVENTNUM", 0) or 0),
                "clock": _clock_to_iso_duration(row.get("PCTIMESTRING")),
                "period": int(row.get("PERIOD", 0) or 0),
                "teamId": team_id,
                "personId": person_id,
                "playerName": str(row.get("PLAYER1_NAME") or ""),
                "playerNameI": str(row.get("PLAYER1_NAME") or ""),
                "description": desc,
                "actionType": _historical_event_action_type(event_type),
                "subType": str(row.get("EVENTMSGACTIONTYPE") or ""),
            }
            if home_score is not None:
                action["scoreHome"] = home_score
            if away_score is not None:
                action["scoreAway"] = away_score
            actions.append(action)

            if team_id > 0 and person_id > 0:
                aliases = _name_aliases(
                    str(action.get("playerName") or ""),
                    str(action.get("playerNameI") or ""),
                    "" if str(action.get("actionType") or "").lower() == "substitution" else desc,
                )
                if aliases:
                    team_entry = team_alias_map.setdefault(team_id, {})
                    for alias in aliases:
                        team_entry.setdefault(alias, set()).add(person_id)
        out[str(gid).lstrip("0")] = _normalize_statsv3_actions(actions)
    _historical_playoff_team_alias_cache[season_start_year] = team_alias_map
    return out


def _normalize_name(name: str) -> str:
    return "".join(ch for ch in (name or "").lower() if ch.isalnum())


def _description_player_keys(description: str) -> set[str]:
    """
    Extract likely player-name keys from a local PBP description.

    This is intentionally broader than the structured player fields because the
    local feed often abbreviates names in text (e.g. "Ja. Green", "Jay.
    Williams", "Smith Jr.") while the machine-readable ids may be incomplete on
    substitution rows.
    """
    desc = str(description or "").strip()
    if not desc:
        return set()
    if desc.upper().startswith("MISS "):
        desc = desc[5:].strip()
    if desc.upper().startswith("SUB:"):
        desc = desc[4:].strip()
    parts = [p.strip(" ,.:;()") for p in desc.split() if p.strip(" ,.:;()")]
    if not parts:
        return set()

    suffixes = {"jr", "sr", "ii", "iii", "iv", "v"}
    ignored = {
        "and",
        "assist",
        "ball",
        "block",
        "def",
        "defensive",
        "drawn",
        "dunk",
        "fadeaway",
        "foul",
        "free",
        "ft",
        "hook",
        "jump",
        "layup",
        "loose",
        "made",
        "makes",
        "miss",
        "missed",
        "off",
        "offensive",
        "of",
        "pass",
        "personal",
        "pts",
        "pullup",
        "rebound",
        "running",
        "shot",
        "steal",
        "step",
        "sub",
        "throw",
        "tip",
        "to",
        "turnaround",
        "turnover",
        "vs",
        "for",
    }

    name_tokens: list[str] = []
    started = False
    for raw in parts:
        token = _normalize_name(raw)
        if not token:
            continue
        if not started:
            if token in ignored:
                continue
            started = True
            if token.isalpha():
                name_tokens.append(token)
            continue
        if token in ignored:
            break
        if not token.isalpha() and token not in suffixes:
            break
        name_tokens.append(token)
        if len(name_tokens) >= 2 and name_tokens[-1] not in suffixes:
            break

    if not name_tokens:
        return set()

    keys: set[str] = set()
    first = name_tokens[0]
    if first not in suffixes:
        keys.add(first)
    if len(name_tokens) >= 2:
        second = name_tokens[1]
        if second in suffixes:
            keys.add(first + second)
        else:
            keys.add(second)
            keys.add(first + second)
            if len(first) >= 1:
                keys.add(first[0] + second)
    return keys


def _description_player_key(description: str) -> str:
    keys = _description_player_keys(description)
    if not keys:
        return ""
    return max(keys, key=len)


def _name_aliases(name: str, name_i: str, desc: str) -> set[str]:
    aliases: set[str] = set()
    suffixes = {"jr", "sr", "ii", "iii", "iv"}

    def add_name(raw: str) -> None:
        text = str(raw or "").strip()
        if not text:
            return
        norm = _normalize_name(text)
        if norm:
            aliases.add(norm)
        parts = [p for p in text.replace(".", " ").split(" ") if p]
        if not parts:
            return
        norm_parts = [_normalize_name(p) for p in parts if _normalize_name(p)]
        if not norm_parts:
            return
        if len(norm_parts) >= 2 and norm_parts[-1] in suffixes:
            aliases.add("".join(norm_parts[-2:]))
        else:
            aliases.add(norm_parts[-1])
            aliases.add(norm_parts[0][0] + norm_parts[-1])

    add_name(name)
    add_name(name_i)
    aliases.update(_description_player_keys(desc))
    return {a for a in aliases if a}


def _expand_local_substitutions(
    actions: list[dict[str, Any]],
    season_team_map: dict[int, dict[str, set[int]]] | None = None,
) -> list[dict[str, Any]]:
    def _name_pair_from_raw(raw_name: str) -> tuple[str, str]:
        text = str(raw_name or "").strip()
        if not text:
            return "", ""
        parts = [p for p in text.split() if p]
        if len(parts) >= 2:
            return text, f"{parts[0][0]}. {' '.join(parts[1:])}"
        return text, text

    team_map: dict[int, dict[str, set[int]]] = {}
    player_name_map: dict[int, dict[int, tuple[str, str]]] = {}
    for a in actions:
        try:
            team_id = int(a.get("teamId", 0) or 0)
            pid = canonicalize_player_id(a.get("personId", 0) or 0)
        except Exception:
            continue
        if team_id <= 0 or pid <= 0:
            continue
        name = str(a.get("playerName", "") or "")
        name_i = str(a.get("playerNameI", "") or "")
        desc = ""
        if str(a.get("actionType", "")).lower() != "substitution":
            desc = str(a.get("description", "") or "")
        m = team_map.setdefault(team_id, {})
        names = player_name_map.setdefault(team_id, {})
        if pid not in names:
            names[pid] = (name, name_i)
        for alias in _name_aliases(name, name_i, desc):
            m.setdefault(alias, set()).add(pid)
    def _raw_name_tokens(raw_name: str) -> tuple[str, str]:
        text = str(raw_name or "").replace(".", " ").strip()
        parts = [_normalize_name(p) for p in text.split() if _normalize_name(p)]
        if not parts:
            return "", ""
        return parts[0], parts[-1]

    def _is_surname_only(raw_name: str) -> bool:
        text = str(raw_name or "").replace(".", " ").strip()
        parts = [_normalize_name(p) for p in text.split() if _normalize_name(p)]
        return len(parts) == 1

    def _core_surname(name: str) -> str:
        text = str(name or "").replace(".", " ").strip()
        parts = [_normalize_name(p) for p in text.split() if _normalize_name(p)]
        if not parts:
            return ""
        if parts[-1] in {"jr", "sr", "ii", "iii", "iv", "v"} and len(parts) >= 2:
            return parts[-2]
        return parts[-1]

    def _resolve_from_lookup(
        team_lookup: dict[str, set[int]],
        names: dict[int, tuple[str, str]],
        raw_name: str,
    ) -> list[int]:
        normalized_raw = _normalize_name(str(raw_name or "").strip())
        raw_first, raw_last = _raw_name_tokens(raw_name)

        if normalized_raw:
            exact_name_matches: list[int] = []
            for pid, (name, name_i) in names.items():
                if normalized_raw in {
                    _normalize_name(name),
                    _normalize_name(name_i),
                }:
                    canon_pid = canonicalize_player_id(pid)
                    if canon_pid > 0 and canon_pid not in exact_name_matches:
                        exact_name_matches.append(canon_pid)
            if exact_name_matches:
                return sorted(exact_name_matches)
            direct = sorted(canonicalize_player_id(pid) for pid in team_lookup.get(normalized_raw, set()) if canonicalize_player_id(pid) > 0)
            direct = list(dict.fromkeys(direct))
            if raw_last and _is_surname_only(raw_name):
                family_matches: list[int] = []
                for pid, (name, name_i) in names.items():
                    if raw_last not in {_core_surname(name), _core_surname(name_i)}:
                        continue
                    canon_pid = canonicalize_player_id(pid)
                    if canon_pid > 0 and canon_pid not in family_matches:
                        family_matches.append(canon_pid)
                if len(family_matches) > 1:
                    return sorted(family_matches)
            if direct:
                return direct

        # Handle local sub text like "Jo. Howard" by matching a unique
        # first-name-prefix + surname candidate on the active team.
        if raw_first and raw_last:
            prefixed: list[int] = []
            for pid, (name, name_i) in names.items():
                aliases = _name_aliases(name, name_i, "")
                if raw_last not in aliases:
                    continue
                if any(alias.endswith(raw_last) and alias.startswith(raw_first) for alias in aliases):
                    canon_pid = canonicalize_player_id(pid)
                    if canon_pid > 0 and canon_pid not in prefixed:
                        prefixed.append(canon_pid)
            if not prefixed:
                for alias, pids in team_lookup.items():
                    if alias == raw_last:
                        continue
                    if not (alias.endswith(raw_last) and alias.startswith(raw_first)):
                        continue
                    for pid in pids:
                        canon_pid = canonicalize_player_id(pid)
                        if canon_pid > 0 and canon_pid not in prefixed:
                            prefixed.append(canon_pid)
            if prefixed:
                return sorted(prefixed)

        if raw_last:
            surname = sorted(canonicalize_player_id(pid) for pid in team_lookup.get(raw_last, set()) if canonicalize_player_id(pid) > 0)
            surname = list(dict.fromkeys(surname))
            if surname:
                return surname

        return []

    def _resolve_team_name_candidates(team_id: int, raw_name: str) -> list[int]:
        names = player_name_map.get(team_id, {})
        local_lookup = team_map.get(team_id, {})
        local = _resolve_from_lookup(local_lookup, names, raw_name)
        if local:
            return local
        season_lookup = season_team_map.get(team_id, {}) if season_team_map else {}
        if season_lookup:
            return _resolve_from_lookup(season_lookup, names, raw_name)
        return []

    def _first_future_candidate_evidence(
        start_idx: int,
        team_id: int,
        candidate_pid: int,
    ) -> tuple[int | None, int | None, str | None]:
        for future_idx, future in enumerate(actions[start_idx + 1 :], start=start_idx + 1):
            try:
                future_team = int(future.get("teamId", 0) or 0)
                future_pid = canonicalize_player_id(future.get("personId", 0) or 0)
            except Exception:
                continue
            if future_team != team_id or future_pid != candidate_pid:
                continue
            future_type = str(future.get("actionType", "") or "").lower()
            if future_type != "substitution":
                return future_idx, 1, "action"
            desc = str(future.get("description", "") or "")
            if "SUB:" not in desc or " FOR " not in desc:
                continue
            body = desc.split("SUB:", 1)[1].strip()
            try:
                in_name, out_name = body.split(" FOR ", 1)
            except Exception:
                continue
            candidate_names = player_name_map.get(team_id, {}).get(candidate_pid, ("", ""))
            candidate_aliases = _name_aliases(candidate_names[0], candidate_names[1], "")
            in_key = _normalize_name(in_name)
            out_key = _normalize_name(out_name)
            if in_key in candidate_aliases:
                return future_idx, 2, "sub_in"
            if out_key in candidate_aliases:
                return future_idx, 0, "sub_out"
        return None, None, None

    def _resolve_ambiguous_in_candidate(
        start_idx: int,
        team_id: int,
        candidates: list[int],
    ) -> int:
        if len(candidates) <= 1:
            return int(candidates[0]) if candidates else 0
        proven_on: list[tuple[int, int]] = []
        for candidate_pid in candidates:
            evidence_idx, evidence_rank, evidence_kind = _first_future_candidate_evidence(start_idx, team_id, candidate_pid)
            if evidence_idx is None or evidence_rank is None:
                continue
            if evidence_kind in {"action", "sub_out"}:
                proven_on.append((int(evidence_idx), int(candidate_pid)))
        proven_on = sorted({(idx, pid) for idx, pid in proven_on if pid > 0})
        if len(proven_on) == 1:
            return proven_on[0][1]
        if proven_on:
            first_idx = proven_on[0][0]
            earliest = [pid for idx, pid in proven_on if idx == first_idx]
            if len(earliest) == 1:
                return earliest[0]
        return 0

    expanded: list[dict[str, Any]] = []
    for idx, a in enumerate(actions):
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
        def _resolve_pid(raw_name: str) -> tuple[int, list[int]]:
            candidates = _resolve_team_name_candidates(team_id, raw_name)
            if len(candidates) == 1:
                return candidates[0], candidates
            return 0, candidates

        in_pid, in_candidates = _resolve_pid(in_name)
        out_pid, out_candidates = _resolve_pid(out_name)
        if in_pid == 0 and len(in_candidates) > 1 and not _is_surname_only(in_name):
            disambiguated_in_pid = _resolve_ambiguous_in_candidate(idx, team_id, in_candidates)
            if disambiguated_in_pid > 0:
                in_pid = disambiguated_in_pid
                in_candidates = [disambiguated_in_pid]
        pid_raw = canonicalize_player_id(a.get("personId", 0) or 0)
        if pid_raw > 0:
            # The local statsv3 substitution row itself is keyed to the
            # outgoing player. Treat that id as authoritative for the out side
            # instead of letting surname-only text re-resolve it to a teammate.
            out_pid = pid_raw
            out_candidates = [pid_raw]
        if out_pid > 0 and out_pid != in_pid:
            out_row = dict(a)
            out_row["personId"] = canonicalize_player_id(out_pid)
            out_row["subType"] = "out"
            out_name = player_name_map.get(team_id, {}).get(out_pid)
            if out_name:
                out_row["playerName"], out_row["playerNameI"] = out_name
            if len(out_candidates) > 1:
                out_row["candidatePersonIds"] = out_candidates
            expanded.append(out_row)
        in_row = dict(a)
        in_row["personId"] = canonicalize_player_id(in_pid or 0)
        in_row["subType"] = "in"
        in_name_pair = player_name_map.get(team_id, {}).get(int(in_pid or 0))
        if in_name_pair:
            in_row["playerName"], in_row["playerNameI"] = in_name_pair
        elif int(in_pid or 0) > 0:
            in_row["playerName"], in_row["playerNameI"] = _name_pair_from_raw(in_name)
        if len(in_candidates) > 1:
            in_row["candidatePersonIds"] = in_candidates
        expanded.append(in_row)
    return [canonicalize_action_player_ids(a) for a in expanded]


def _load_local_pbp_season(season_start_year: int) -> dict[str, list[dict[str, Any]]] | None:
    path = None
    for base_dir in LOCAL_PBP_DIRS:
        candidate = base_dir / f"nbastatsv3_{season_start_year}.csv"
        if candidate.exists():
            path = candidate
            break
    if path is None:
        return None
    df = pd.read_csv(path, dtype={"gameId": str})
    if df.empty:
        return {}
    df["gameId"] = df["gameId"].astype(str).str.lstrip("0")
    team_alias_map: dict[int, dict[str, set[int]]] = {}
    for row in df.itertuples(index=False):
        try:
            team_id = int(getattr(row, "teamId", 0) or 0)
            pid = int(getattr(row, "personId", 0) or 0)
        except Exception:
            continue
        if team_id <= 0 or pid <= 0:
            continue
        aliases = _name_aliases(
            str(getattr(row, "playerName", "") or ""),
            str(getattr(row, "playerNameI", "") or ""),
            "" if str(getattr(row, "actionType", "") or "").lower() == "substitution" else str(getattr(row, "description", "") or ""),
        )
        if not aliases:
            continue
        team_entry = team_alias_map.setdefault(team_id, {})
        for alias in aliases:
            team_entry.setdefault(alias, set()).add(pid)
    _local_team_alias_cache[season_start_year] = team_alias_map
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

    for cache_gid in _game_id_cache_keys(gid):
        players_path = STATS_BOXSCORE_DIR / f"{cache_gid}_players.json"
        teams_path = STATS_BOXSCORE_DIR / f"{cache_gid}_teams.json"
        cached_players = _cache_read_df(players_path)
        cached_teams = _cache_read_df(teams_path)
        if cached_players is not None and cached_teams is not None:
            out = {"players": cached_players, "teams": cached_teams}
            _stats_boxscore_cache[gid] = out
            if cache_gid != gid:
                _stats_boxscore_cache[cache_gid] = out
            return out
    players_path = STATS_BOXSCORE_DIR / f"{gid}_players.json"
    teams_path = STATS_BOXSCORE_DIR / f"{gid}_teams.json"
    if _stats_cache_only():
        raise RuntimeError(f"stats cache miss for boxscore {gid}")

    try:
        js = _get_boxscore_json(gid, "")
        game = js.get("game", {}) or {}
        home = game.get("homeTeam", {}) or {}
        away = game.get("awayTeam", {}) or {}

        def _num(x: Any) -> float:
            try:
                return float(x)
            except Exception:
                return 0.0

        team_rows: list[dict[str, Any]] = []
        player_rows: list[dict[str, Any]] = []
        for team in [home, away]:
            team_id = int(team.get("teamId", 0) or 0)
            tricode = team.get("teamTricode", "") or team.get("triCode", "") or ""
            team_stats = team.get("statistics", {}) or {}
            team_rows.append(
                {
                    "GAME_ID": gid,
                    "TEAM_ID": team_id,
                    "TEAM_ABBREVIATION": str(tricode),
                    "PTS": _num(team.get("score", 0) or team_stats.get("points", 0)),
                    "FG3M": _num(team_stats.get("threePointersMade", 0)),
                    "FG3A": _num(team_stats.get("threePointersAttempted", 0)),
                }
            )
            for p in team.get("players", []) or []:
                pid = p.get("personId")
                if pid is None:
                    continue
                first = p.get("firstName", "") or ""
                last = p.get("familyName", "") or p.get("lastName", "") or ""
                name = (first + " " + last).strip() or p.get("name", "") or p.get("nameI", "") or ""
                stats = p.get("statistics", {}) or {}
                player_rows.append(
                    {
                        "GAME_ID": gid,
                        "TEAM_ID": team_id,
                        "PLAYER_ID": canonicalize_player_id(pid),
                        "PLAYER_NAME": str(name),
                        "NICKNAME": str(last),
                        "START_POSITION": "S" if str(p.get("starter", "0") or "0") == "1" else "",
                        "COMMENT": "",
                        "MIN": stats.get("minutes"),
                        "PLUS_MINUS": _num(stats.get("plusMinusPoints")),
                        "FG3M": _num(stats.get("threePointersMade", 0)),
                        "FG3A": _num(stats.get("threePointersAttempted", 0)),
                    }
                )
        players = pd.DataFrame(player_rows)
        if not players.empty:
            players["PLAYER_ID"] = players["PLAYER_ID"].map(canonicalize_player_id).astype(int)
        teams = pd.DataFrame(team_rows)
        if not players.empty and not teams.empty:
            out = {"players": players, "teams": teams}
            _cache_write_df(players_path, players)
            _cache_write_df(teams_path, teams)
            _stats_boxscore_cache[gid] = out
            return out
    except Exception:
        pass

    bs = boxscoretraditionalv2.BoxScoreTraditionalV2(
        game_id=gid,
        timeout=max(STATS_TIMEOUT, 45),
    )
    dfs = bs.get_data_frames()
    players = dfs[0] if len(dfs) > 0 else pd.DataFrame()
    teams = dfs[1] if len(dfs) > 1 else pd.DataFrame()
    if not players.empty and "PLAYER_ID" in players.columns:
        players["PLAYER_ID"] = players["PLAYER_ID"].map(canonicalize_player_id).astype(int)
    out = {"players": players, "teams": teams}
    _cache_write_df(players_path, players)
    _cache_write_df(teams_path, teams)
    _stats_boxscore_cache[gid] = out
    return out


def _load_stats_home_away(game_id: str) -> tuple[int, int]:
    gid = str(game_id)
    if gid in _stats_summary_cache:
        return _stats_summary_cache[gid]

    for cache_gid in _game_id_cache_keys(gid):
        summary_path = STATS_SUMMARY_DIR / f"{cache_gid}.json"
        cached = _cache_read_json(summary_path)
        if cached is not None:
            out = (int(cached.get("home_id", 0) or 0), int(cached.get("away_id", 0) or 0))
            if out[0] > 0 and out[1] > 0:
                _stats_summary_cache[gid] = out
                if cache_gid != gid:
                    _stats_summary_cache[cache_gid] = out
                return out
    summary_path = STATS_SUMMARY_DIR / f"{gid}.json"
    if _stats_cache_only():
        raise RuntimeError(f"stats cache miss for summary {gid}")

    try:
        js = _get_boxscore_json(gid, "")
        game = js.get("game", {}) or {}
        home = game.get("homeTeam", {}) or {}
        away = game.get("awayTeam", {}) or {}
        home_id = int(home.get("teamId", 0) or 0)
        away_id = int(away.get("teamId", 0) or 0)
        if home_id > 0 and away_id > 0:
            out = (home_id, away_id)
            _cache_write_json(summary_path, {"home_id": home_id, "away_id": away_id})
            _stats_summary_cache[gid] = out
            return out
    except Exception:
        pass

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
    _cache_write_json(summary_path, {"home_id": home_id, "away_id": away_id})
    _stats_summary_cache[gid] = out
    return out


def _load_stats_gamerotation(game_id: str) -> pd.DataFrame:
    gid = str(game_id)
    if gid in _stats_rotation_cache:
        return _stats_rotation_cache[gid].copy()

    for cache_gid in _game_id_cache_keys(gid):
        rotation_path = STATS_ROTATION_DIR / f"{cache_gid}.json"
        cached_rotation = _cache_read_df(rotation_path)
        if cached_rotation is not None:
            _stats_rotation_cache[gid] = cached_rotation.copy()
            if cache_gid != gid:
                _stats_rotation_cache[cache_gid] = cached_rotation.copy()
            return cached_rotation
    rotation_path = STATS_ROTATION_DIR / f"{gid}.json"
    if _stats_cache_only():
        raise RuntimeError(f"stats cache miss for gamerotation {gid}")

    gr = gamerotation.GameRotation(
        game_id=gid,
        timeout=max(STATS_TIMEOUT, 45),
    )
    dfs = gr.get_data_frames()
    if not dfs:
        raise RuntimeError(f"gamerotation empty for {gid}")
    rot = pd.concat(dfs, ignore_index=True)
    if rot.empty:
        raise RuntimeError(f"gamerotation empty for {gid}")
    rot["GAME_ID"] = rot["GAME_ID"].astype(str)
    rot["TEAM_ID"] = rot["TEAM_ID"].astype(int)
    rot["PERSON_ID"] = rot["PERSON_ID"].astype(int)
    rot["IN_TIME_REAL"] = pd.to_numeric(rot["IN_TIME_REAL"], errors="coerce").fillna(0.0)
    rot["OUT_TIME_REAL"] = pd.to_numeric(rot["OUT_TIME_REAL"], errors="coerce").fillna(0.0)
    rot["start_elapsed"] = rot["IN_TIME_REAL"] / 10.0
    rot["end_elapsed"] = rot["OUT_TIME_REAL"] / 10.0
    _cache_write_df(rotation_path, rot)
    _stats_rotation_cache[gid] = rot.copy()
    return rot


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
    # Try CDN schedule first for regular season (faster and more reliable).
    # If the league schedule is loaded and the date simply has no games,
    # return [] directly instead of falling through to the much slower stats API.
    if season_type == "Regular Season" and not season_override:
        schedule = _load_schedule()
        ids = schedule.get(game_date_mmddyyyy, [])
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
                        "PLAYER_ID": canonicalize_player_id(pid),
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
                "PLAYER_ID": p["PLAYER_ID"].map(canonicalize_player_id).astype(int),
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
                        "PLAYER_ID": canonicalize_player_id(pid),
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
        minute_col = next((c for c in ["MIN", "MIN_SEC", "MINUTES"] if c in players.columns), None)
        if minute_col is not None:
            mins = players[minute_col].map(_parse_minutes_any)
        else:
            mins = pd.Series(0.0, index=players.index)
        comment = players["COMMENT"].fillna("") if "COMMENT" in players.columns else pd.Series("", index=players.index)
        start_position = players["START_POSITION"].fillna("") if "START_POSITION" in players.columns else pd.Series("", index=players.index)
        played = (mins > 0.0) & comment.eq("")
        starter = start_position.astype(str).str.strip().ne("")
        nickname = players["NICKNAME"] if "NICKNAME" in players.columns else pd.Series("", index=players.index)
        plus_minus = pd.to_numeric(players["PLUS_MINUS"], errors="coerce").fillna(0.0) if "PLUS_MINUS" in players.columns else pd.Series(0.0, index=players.index)
        return pd.DataFrame(
            {
                "GAME_ID": players["GAME_ID"].astype(str),
                "TEAM_ID": players["TEAM_ID"].astype(int),
                "PLAYER_ID": players["PLAYER_ID"].map(canonicalize_player_id).astype(int),
                "PLAYER_NAME": players["PLAYER_NAME"].astype(str),
                "NAME_I": nickname.astype(str),
                "STARTER": starter.map(lambda x: "1" if x else "0"),
                "ONCOURT": "0",
                "PLAYED": played.map(lambda x: "1" if x else "0"),
                "PLUS_MINUS": plus_minus,
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
    local_game_ids = _local_pbp_game_ids(game_id)
    for season_year in _local_pbp_season_years(game_date_mmddyyyy):
        if season_year not in _local_pbp_cache:
            local = _load_local_pbp_season(season_year)
            if local is not None:
                _local_pbp_cache[season_year] = local
        local_season = _local_pbp_cache.get(season_year)
        if not local_season:
            continue
        for gid in local_game_ids:
            if gid in local_season:
                actions = local_season[gid]
                return [
                    canonicalize_action_player_ids(a)
                    for a in _expand_local_substitutions(actions, _local_team_alias_cache.get(season_year))
                ]

    gid = str(game_id).lstrip("0")
    if gid.startswith("4"):
        playoff_file_year = _historical_playoff_file_year_from_game_id(gid)
        if playoff_file_year is None:
            playoff_file_year = _season_start_year_from_mmddyyyy(game_date_mmddyyyy)
        if playoff_file_year not in _historical_playoff_pbp_cache:
            local_playoff = _load_historical_playoff_pbp_season(playoff_file_year)
            if local_playoff is not None:
                _historical_playoff_pbp_cache[playoff_file_year] = local_playoff
        playoff_season = _historical_playoff_pbp_cache.get(playoff_file_year)
        if playoff_season and gid in playoff_season:
            actions = playoff_season[gid]
            return [
                canonicalize_action_player_ids(a)
                for a in _expand_local_substitutions(actions, _historical_playoff_team_alias_cache.get(playoff_file_year))
            ]

    # Try CDN first
    try:
        url = PLAYBYPLAY_URL.format(game_id=game_id)
        pbp_data = _get_json(url)
        game = pbp_data.get("game", {}) or {}
        actions = game.get("actions", []) or []
        if actions:
            return [canonicalize_action_player_ids(a) for a in actions]
    except Exception:
        pass

    # Fallback to stats API for historical games
    try:
        return [canonicalize_action_player_ids(a) for a in _load_pbp_from_stats_api(game_id)]
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
                "PLAYER_ID": canonicalize_player_id(pid),
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

    season = _season_from_mmddyyyy(game_date_mmddyyyy)
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
                "PLAYER_ID": canonicalize_player_id(action.get("personId")),
                "PLAYER_NAME": action.get("playerNameI", ""),
                "MADE": made,
                "AREA": area,
                "SHOT_TYPE": shot_type,
                "SEASON": season,
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
        # Unknown shot type; caller can apply season-average mix
        return "unknown"


def _season_from_mmddyyyy(date_str: str) -> str:
    """Convert MM/DD/YYYY to season label like 2019-20."""
    d = datetime.strptime(date_str, "%m/%d/%Y")
    start = d.year if d.month >= 9 else d.year - 1
    return f"{start}-{(start + 1) % 100:02d}"
