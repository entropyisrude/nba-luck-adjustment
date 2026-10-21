"""NBA game data from nba.com's public web pages.

cdn.nba.com / stats.nba.com JSON started returning Akamai "Access Denied" in
mid-2026, but the regular nba.com pages still embed the same data in their
Next.js `__NEXT_DATA__` payload:

  /games?date=YYYY-MM-DD        -> game cards (gameId, gameStatus)
  /game/{gameId}/play-by-play   -> box score (`game`) + stats-v3 play-by-play

These helpers fetch those pages and reshape the box score into the
cdn.nba.com liveData layout the rest of the pipeline already parses.
"""
from __future__ import annotations

import json
import random
import re
import time
from typing import Any

import requests

BASE_URL = "https://www.nba.com"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "text/html"}
TIMEOUT = 30
MAX_RETRIES = 4
_NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', re.S)

_page_cache: dict[str, dict[str, Any]] = {}


def _page_props(path: str) -> dict[str, Any]:
    if path in _page_cache:
        return _page_cache[path]
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(BASE_URL + path, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            m = _NEXT_DATA_RE.search(r.text)
            if not m:
                raise ValueError(f"no __NEXT_DATA__ on {path}")
            props = json.loads(m.group(1))["props"]["pageProps"]
            _page_cache[path] = props
            return props
        except Exception as e:
            last_err = e
            time.sleep(1.0 * (2 ** (attempt - 1)) + random.random())
    raise last_err  # type: ignore[misc]


def games_for_date(date_iso: str) -> list[dict[str, Any]]:
    """Game cards for a date (YYYY-MM-DD, ET): [{gameId, gameStatus, ...}]."""
    props = _page_props(f"/games?date={date_iso}")
    out = []
    for module in (props.get("gameCardFeed") or {}).get("modules") or []:
        for card in module.get("cards") or []:
            data = card.get("cardData") or {}
            if data.get("gameId"):
                out.append(data)
    return out


def _game_props(game_id: str) -> dict[str, Any]:
    return _page_props(f"/game/{str(game_id).zfill(10)}/play-by-play")


def _mmss_to_iso(value: Any) -> str:
    """'45:15' -> 'PT45M15.00S' (cdn.nba.com liveData minutes format)."""
    text = str(value or "").strip()
    m = re.fullmatch(r"(\d+):(\d+(?:\.\d+)?)", text)
    if not m:
        return "PT00M00.00S"
    return f"PT{int(m.group(1)):02d}M{float(m.group(2)):05.2f}S"


def boxscore_json(game_id: str) -> dict[str, Any]:
    """Box score in cdn.nba.com liveData layout: {"game": {homeTeam, awayTeam, ...}}."""
    game = dict(_game_props(game_id).get("game") or {})
    if not game.get("homeTeam"):
        raise ValueError(f"no box score on nba.com page for {game_id}")
    for side in ("homeTeam", "awayTeam"):
        team = dict(game.get(side) or {})
        players = []
        for p in team.get("players") or []:
            p = dict(p)
            stats = dict(p.get("statistics") or {})
            raw_minutes = stats.get("minutes")
            stats["minutes"] = _mmss_to_iso(raw_minutes)
            p["statistics"] = stats
            p["starter"] = "1" if str(p.get("position") or "").strip() else "0"
            p["played"] = "1" if raw_minutes and stats["minutes"] != "PT00M00.00S" else "0"
            p["oncourt"] = "0"
            players.append(p)
        team["players"] = players
        game[side] = team
    return {"game": game}


def playbyplay_actions(game_id: str) -> list[dict[str, Any]]:
    """Raw stats-v3 play-by-play actions (same schema as data/pbp/nbastatsv3_*.csv)."""
    pbp = _game_props(game_id).get("playByPlay") or {}
    actions = [dict(a) for a in pbp.get("actions") or []]
    # Period start/end markers score nothing, but the feed sometimes stamps them
    # with a wrong running score (22600002: Q3 start 68-66 between 58-54 and
    # 58-56), which on/off would book as points for one lineup and take back
    # from the next. Give them the last real score instead.
    last = ("0", "0")
    for a in actions:
        if str(a.get("actionType") or "").lower() == "period":
            a["scoreHome"], a["scoreAway"] = last
        elif a.get("scoreHome") not in (None, "") and a.get("scoreAway") not in (None, ""):
            last = (a["scoreHome"], a["scoreAway"])
    return actions
