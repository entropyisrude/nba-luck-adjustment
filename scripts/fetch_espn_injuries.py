"""Estimated return dates for injured players, from ESPN's injuries feed.

The official NBA injury report says who is out for the next game but not for
how long ("Injury Management" can mean a rest day or knee surgery). ESPN's feed
carries an estimated return date per player, updated as news comes in; the
power ratings use it to size each absence.

Writes data/espn_injuries_<TAG>.json:
  {"generated", "players": [{name, team, status, return_date, type, detail, updated}]}
Only these fields are kept (no news text).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.fetch_injury_report import TEAMS  # noqa: E402
from src.season import TAG  # noqa: E402

FEED_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
OUT_PATH = Path(__file__).resolve().parents[1] / "data" / f"espn_injuries_{TAG}.json"
HEADERS = {"User-Agent": "Mozilla/5.0"}
ALIASES = {"Los Angeles Clippers": "LAC"}


def main() -> None:
    try:
        feed = requests.get(FEED_URL, headers=HEADERS, timeout=30).json()
    except (requests.RequestException, ValueError) as e:
        print(f"ESPN injuries unavailable ({e}); keeping previous file.")
        return
    players = []
    for team in feed.get("injuries", []):
        abbr = TEAMS.get(team.get("displayName")) or ALIASES.get(team.get("displayName"))
        for inj in team.get("injuries", []):
            details = inj.get("details") or {}
            players.append({
                "name": inj.get("athlete", {}).get("displayName"),
                "team": abbr,
                "status": inj.get("status"),
                "return_date": details.get("returnDate"),
                "type": details.get("type"),
                "detail": details.get("detail"),
                "updated": (inj.get("date") or "")[:10],
            })
    if not players:
        print("ESPN injuries feed was empty; keeping previous file.")
        return
    OUT_PATH.write_text(json.dumps({"generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
                                    "players": players}, indent=1), encoding="utf-8")
    print(f"Wrote {OUT_PATH}: {len(players)} players")


if __name__ == "__main__":
    main()
