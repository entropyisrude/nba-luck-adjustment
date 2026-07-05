"""
Builds a small lookup of each player's most recent completed season's minutes-per-game,
for display next to the minutes editor on the depth chart pages. Source: data/player_seasons.json
(already built by build_player_seasons.py). Keyed by a normalized name so the depth chart JS
(which reads player names from the Spotrac-derived cap data, a different source with its own
name spellings) can match against it -- see normalize_name() in depth-chart-team.html /
depth-chart-local.html, which must stay logically in sync with this function.
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "player_seasons.json"
OUTPUT = ROOT / "data" / "last_season_mpg.json"
JS_OUTPUT = ROOT / "data" / "last_season_mpg.js"
LAST_SEASON = "2025-26"

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    text = text.replace(".", "")
    text = re.sub(r"[^a-zA-Z0-9 ]", "", text)
    words = [w.lower() for w in text.split()]
    while words and words[-1] in SUFFIXES:
        words.pop()
    return " ".join(words)


def main() -> None:
    data = json.loads(SOURCE.read_text(encoding="utf-8"))
    lookup: dict[str, dict] = {}
    known: set[str] = set()
    for row in data["seasons"]:
        key = normalize_name(row["name"])
        known.add(key)
        if row["season"] != LAST_SEASON:
            continue
        games = row.get("games") or 0
        minutes = row.get("min") or 0
        if games <= 0:
            continue
        lookup[key] = {
            "name": row["name"],
            "games": games,
            "min": minutes,
            "mpg": round(minutes / games, 1),
        }

    # Players who never appear in ANY season are true rookies; a player missing
    # only LAST_SEASON but present in an earlier season is a veteran with a data
    # gap (injury, pipeline lag, etc.) -- those two cases must not be conflated,
    # since the depth chart labels the former "Rookie" and the latter "-".
    snapshot = {
        "season": LAST_SEASON,
        "generated_from": data.get("generated"),
        "players": lookup,
        "known": sorted(known),
    }
    OUTPUT.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    JS_OUTPUT.write_text(
        "window.LAST_SEASON_MPG = " + json.dumps(snapshot, ensure_ascii=False) + ";\n",
        encoding="utf-8",
    )
    print(f"{len(lookup)} players -> {OUTPUT}")


if __name__ == "__main__":
    main()
