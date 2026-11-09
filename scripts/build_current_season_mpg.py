"""
Builds each player's current-season minutes per game from the daily box scores
(data/player_boxscore_stats.csv), for the depth chart pages and win projections.

A player who joins a team after its depth chart was saved (trade, signing) is
appended to the rotation with a placeholder: his current-season MPG once he has
MIN_GAMES games, else last season's (data/last_season_mpg.js). Last season's MPG
alone badly overstates a player whose role has shrunk.

Keyed by the same normalized name as scripts/build_last_season_mpg.py.
Output: data/current_season_mpg.js (window.CURRENT_SEASON_MPG)
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.build_last_season_mpg import ALIASES, normalize_name  # noqa: E402
from src.season import SEASON, SEASON_START_DATE  # noqa: E402

BOX_CSV = ROOT / "data" / "player_boxscore_stats.csv"
JS_OUTPUT = ROOT / "data" / "current_season_mpg.js"
MIN_GAMES = 3


def _minutes(text: object) -> float:
    m = re.match(r"PT(\d+)M([\d.]+)S", str(text))
    return int(m[1]) + float(m[2]) / 60 if m else 0.0


def main() -> None:
    box = pd.read_csv(BOX_CSV, dtype={"game_id": str},
                      usecols=["date", "game_id", "team_abbr", "player_name", "minutes"])
    box = box[(box["date"] >= SEASON_START_DATE) & box["game_id"].str.lstrip("0").str.startswith("2")]
    box["min"] = box["minutes"].map(_minutes)
    box = box[box["min"] > 0].sort_values("date")
    lookup: dict[str, dict] = {}
    for name, g in box.groupby("player_name"):
        lookup[normalize_name(name)] = {
            "name": name, "games": int(len(g)), "mpg": round(g["min"].mean(), 1),
            "team": g["team_abbr"].iloc[-1],
        }
    for variant, canonical in ALIASES.items():
        if canonical in lookup:
            lookup.setdefault(variant, lookup[canonical])
    snapshot = {"season": SEASON, "min_games": MIN_GAMES,
                "source": "player_boxscore_stats.csv, regular-season games", "players": lookup}
    JS_OUTPUT.write_text("window.CURRENT_SEASON_MPG = " + json.dumps(snapshot, ensure_ascii=False) + ";\n",
                         encoding="utf-8")
    print(f"{len(lookup)} players -> {JS_OUTPUT}")


if __name__ == "__main__":
    main()
