"""
Builds a small lookup of each player's most recent completed season's minutes-per-game,
for display next to the minutes editor on the depth chart pages.

MPG/games come directly from data/nba_analytics.duckdb's player_game_facts, NOT from the
already-aggregated data/player_seasons.json. The facts table carries playoff ('4') and
play-in ('5') games alongside regular-season ones ('2') for recent seasons, so we filter
to game ids starting with '2'. player_seasons.json is still used for the "known"
set (has this player appeared in ANY season, for the rookie-vs-data-gap distinction) since
that part isn't affected by the current season's boundary problem.

Keyed by a normalized name so the depth chart JS (which reads player names from the
Spotrac-derived cap data, a different source with its own name spellings) can match
against it -- see normalize_name() in depth-chart-team.html / depth-chart-local.html,
which must stay logically in sync with this function.
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = ROOT / "data" / "nba_analytics.duckdb"
KNOWN_SOURCE = ROOT / "data" / "player_seasons.json"
OUTPUT = ROOT / "data" / "last_season_mpg.json"
JS_OUTPUT = ROOT / "data" / "last_season_mpg.js"
LAST_SEASON = "2025-26"

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

# Spotrac name variant (normalized) -> our box-data name (normalized).
# Normalization alone can't bridge these (different first names entirely).
# Found via the 2026-07-11 league-wide audit of Spotrac rosters vs box data;
# add a line whenever a veteran shows up mislabeled "Rookie" on the depth
# charts. Applied as duplicate entries so every consumer (MPG labels,
# minutes seeding, and the projections' NERD lookup via the exported
# "aliases" map) matches without JS changes.
ALIASES = {
    "herb jones": "herbert jones",
    "nicolas claxton": "nic claxton",
    "ron holland": "ronald holland",
    "cameron christie": "cam christie",
    "nahshon hyland": "bones hyland",
    "sviatoslav mykhailiuk": "svi mykhailiuk",
    "mohamed bamba": "mo bamba",
}


def normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    text = text.replace(".", "")
    text = re.sub(r"[^a-zA-Z0-9 ]", "", text)
    words = [w.lower() for w in text.split()]
    while words and words[-1] in SUFFIXES:
        words.pop()
    return " ".join(words)


def main() -> None:
    known: set[str] = set()
    for row in json.loads(KNOWN_SOURCE.read_text(encoding="utf-8"))["seasons"]:
        known.add(normalize_name(row["name"]))

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    rows = con.execute(
        """
        SELECT player_name, count(*) AS games, sum(minutes) AS total_min
        FROM player_game_facts
        WHERE season = ? AND minutes > 0
          AND CAST(game_id AS VARCHAR) LIKE '2%'  -- RS only; excludes playoffs ('4') and play-in ('5')
        GROUP BY player_name
        """,
        [LAST_SEASON],
    ).fetchall()
    con.close()

    lookup: dict[str, dict] = {}
    for name, games, total_min in rows:
        if games <= 0:
            continue
        lookup[normalize_name(name)] = {
            "name": name,
            "games": games,
            "min": round(total_min, 1),
            "mpg": round(total_min / games, 1),
        }

    # Alias entries: Spotrac variants resolve to the same record/known status.
    for variant, canonical in ALIASES.items():
        if canonical in lookup:
            lookup.setdefault(variant, lookup[canonical])
        if canonical in known:
            known.add(variant)

    # Players who never appear in ANY season are true rookies; a player missing
    # only LAST_SEASON but present in an earlier season is a veteran with a data
    # gap (injury, pipeline lag, etc.) -- those two cases must not be conflated,
    # since the depth chart labels the former "Rookie" and the latter "-".
    snapshot = {
        "season": LAST_SEASON,
        "source": "player_game_facts, regular-season games only (game-id prefix '2')",
        "players": lookup,
        "known": sorted(known),
        "aliases": ALIASES,
    }
    OUTPUT.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    JS_OUTPUT.write_text(
        "window.LAST_SEASON_MPG = " + json.dumps(snapshot, ensure_ascii=False) + ";\n",
        encoding="utf-8",
    )
    print(f"{len(lookup)} players -> {OUTPUT}")


if __name__ == "__main__":
    main()
