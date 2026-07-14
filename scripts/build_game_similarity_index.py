#!/usr/bin/env python3
"""Build the player-game similarity index from the published game chunks.

The index deliberately uses raw box-score values. Global standard deviations only
put unlike statistics on comparable scales; there is no season or era adjustment.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "data" / "game_similarity_index.json"
DEFAULT_CATALOG_OUTPUT = ROOT / "data" / "game_similarity_players.json"
DEFAULT_SHARD_DIR = ROOT / "data" / "game_similarity"
PLAYER_BUCKETS = 64
MINUTE_BANDS = [(start, start + 5) for start in range(5, 45, 5)] + [(45, 1000)]
CHUNK_DIRS = (
    ROOT / "data" / "player_game_chunks",
    ROOT / "data" / "player_game_playoff_chunks",
)

# Compact chunk columns (see generate_player_game_search_report.py).
DATE, SEASON, GAME_ID, PID, NAME, TEAM, OPP = range(7)
TEAM_PTS, OPP_PTS, SCORE_MARGIN, HOME_AWAY, WIN_LOSS, STARTER, MIN = range(7, 14)
PTS, REB, OREB, DREB, AST, STL, BLK, TOV, PF = range(14, 23)
FGM, FGA, FG2M, FG2A, FG2_PCT, FG3M, FG3A, FG3_PCT = range(23, 31)
FTM, FTA, FT_PCT = range(31, 34)
TS_GAME = 47
PLUS_MINUS_ACTUAL = 50
ON_OFF_ACTUAL = 53

SCHEMA = [
    "name", "pid", "date", "season", "opp", "min", "pts", "reb", "ast",
    "stl", "blk", "tov", "fga", "fg3a", "fg2a", "fg3_pct", "fg2_pct",
    "fta", "ft_pct", "ts_game", "plus_minus_actual", "on_off_actual",
    "team_pts_actual", "opp_pts_actual", "win_loss", "game_id",
]
SIM_STATS = [
    "min", "pts", "reb", "ast", "stl", "blk", "tov", "fga", "fg3a",
    "fg2a", "fg3_pct", "fg2_pct", "fta", "ft_pct", "ts_game",
    "plus_minus_actual", "on_off_actual",
]


def chunk_rows(path: Path) -> list[list[object]]:
    text = path.read_text(encoding="utf-8")
    match = re.search(r"=\s*(\[.*\])\s*;\s*$", text, flags=re.S)
    if not match:
        raise ValueError(f"Could not parse chunk payload: {path}")
    return json.loads(match.group(1))


def number(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def rounded(value: object) -> float:
    return round(number(value), 3)


def compact(row: list[object]) -> list[object]:
    return [
        row[NAME], row[PID], row[DATE], row[SEASON], row[OPP], rounded(row[MIN]),
        rounded(row[PTS]), rounded(row[REB]), rounded(row[AST]), rounded(row[STL]),
        rounded(row[BLK]), rounded(row[TOV]), rounded(row[FGA]), rounded(row[FG3A]),
        rounded(row[FG2A]), rounded(row[FG3_PCT]), rounded(row[FG2_PCT]),
        rounded(row[FTA]), rounded(row[FT_PCT]), rounded(row[TS_GAME]),
        rounded(row[PLUS_MINUS_ACTUAL]), rounded(row[ON_OFF_ACTUAL]),
        None if row[TEAM_PTS] is None else rounded(row[TEAM_PTS]),
        None if row[OPP_PTS] is None else rounded(row[OPP_PTS]),
        row[WIN_LOSS], str(row[GAME_ID]),
    ]


def build(min_minutes: float) -> dict[str, object]:
    games: list[list[object]] = []
    seen: set[tuple[str, object]] = set()
    for chunk_dir in CHUNK_DIRS:
        for path in sorted(chunk_dir.glob("*.js")):
            for row in chunk_rows(path):
                if len(row) < 56 or number(row[MIN]) < min_minutes:
                    continue
                key = (str(row[GAME_ID]), row[PID])
                if key in seen:
                    continue
                seen.add(key)
                games.append(compact(row))

    games.sort(key=lambda row: (str(row[2]), str(row[25]), str(row[0])))
    positions = {name: i for i, name in enumerate(SCHEMA)}
    means: list[float] = []
    stds: list[float] = []
    for stat in SIM_STATS:
        values = [number(game[positions[stat]]) for game in games]
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / len(values)
        means.append(mean)
        stds.append(math.sqrt(variance) or 1.0)

    return {
        "generated": datetime.now(timezone.utc).isoformat(),
        "schema": SCHEMA,
        "sim_stats": SIM_STATS,
        "means": means,
        "stds": stds,
        "games": games,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--catalog-output", type=Path, default=DEFAULT_CATALOG_OUTPUT)
    parser.add_argument("--shard-dir", type=Path, default=DEFAULT_SHARD_DIR)
    parser.add_argument("--monolith", action="store_true", help="Also write the legacy monolithic index")
    parser.add_argument("--min-minutes", type=float, default=5.0)
    args = parser.parse_args()
    payload = build(args.min_minutes)
    games = payload["games"]
    player_ids: dict[str, object] = {}
    for game in games:
        player_ids[str(game[0])] = game[1]
    players = sorted(
        ((name, pid, int(pid) % PLAYER_BUCKETS) for name, pid in player_ids.items()),
        key=lambda row: row[0].lower(),
    )
    catalog = {
        "generated": payload["generated"],
        "first_season": min(str(game[3]) for game in games),
        "last_season": max(str(game[3]) for game in games),
        "game_count": len(games),
        "players": players,
    }
    args.catalog_output.parent.mkdir(parents=True, exist_ok=True)
    args.catalog_output.write_text(
        json.dumps(catalog, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    args.shard_dir.mkdir(parents=True, exist_ok=True)
    search_dir = args.shard_dir / "search"
    player_dir = args.shard_dir / "players"
    search_dir.mkdir(exist_ok=True)
    player_dir.mkdir(exist_ok=True)
    for stale_path in search_dir.glob("*.json"):
        stale_path.unlink()
    search_shards: list[dict[str, object]] = []
    for low, high in MINUTE_BANDS:
        label = f"{low:02d}_{'plus' if high >= 1000 else f'{high:02d}'}"
        filename = label + ".json"
        band_games = [game for game in games if low <= float(game[5]) < high]
        (search_dir / filename).write_text(
            json.dumps(band_games, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        search_shards.append({"file": filename, "min": low, "max": high})
    for bucket in range(PLAYER_BUCKETS):
        bucket_games = [game for game in games if int(game[1]) % PLAYER_BUCKETS == bucket]
        (player_dir / f"{bucket:02x}.json").write_text(
            json.dumps(bucket_games, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
    manifest = {
        "generated": payload["generated"],
        "schema": payload["schema"],
        "sim_stats": payload["sim_stats"],
        "means": payload["means"],
        "stds": payload["stds"],
        "search_shards": search_shards,
        "player_buckets": PLAYER_BUCKETS,
        "game_count": len(games),
    }
    (args.shard_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    if args.monolith:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        print(f"Wrote {len(payload['games']):,} games to {args.output}")
    print(f"Wrote {len(search_shards)} search shards and {PLAYER_BUCKETS} player buckets to {args.shard_dir}")
    print(f"Wrote {len(players):,} players to {args.catalog_output}")


if __name__ == "__main__":
    main()
