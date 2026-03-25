from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
REG_DB = DATA_DIR / "nba_analytics.duckdb"
PLAYOFF_DB = DATA_DIR / "nba_analytics_playoffs.duckdb"
OUT_PATH = DATA_DIR / "player_rim_assists_by_season.csv"
CACHE_DIR = DATA_DIR / "rim_assist_recent_cache"

sys.path.insert(0, str(ROOT))
from src.ingest import get_playbyplay_actions  # noqa: E402


REGULAR_TARGETS = ["2023-24", "2024-25", "2025-26"]
PLAYOFF_TARGETS = ["2023-24"]
OTHER_RIM_DISTANCE_MAX = 30.0


def game_universe(db_path: Path, seasons: list[str], season_type: str) -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    games = con.execute(
        f"""
        SELECT season, CAST(date AS VARCHAR) AS date, game_id
        FROM player_game_facts
        WHERE season IN ({",".join(["?"] * len(seasons))})
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
        """,
        seasons,
    ).df()
    names = con.execute(
        f"""
        SELECT season, CAST(player_id AS BIGINT) AS player_id, MAX(player_name) AS player_name, COUNT(DISTINCT game_id) AS season_games
        FROM player_game_facts
        WHERE season IN ({",".join(["?"] * len(seasons))})
        GROUP BY 1, 2
        """,
        seasons,
    ).df()
    con.close()
    games["season_type"] = season_type
    names["season_type"] = season_type
    return games, names


def mmddyyyy(iso_date: str) -> str:
    y, m, d = iso_date.split("-")
    return f"{m}/{d}/{y}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season-type", choices=["Regular Season", "Playoffs", "all"], default="all")
    parser.add_argument("--season", action="append", default=[])
    parser.add_argument("--progress-every", type=int, default=100)
    args = parser.parse_args()

    reg_targets = REGULAR_TARGETS
    po_targets = PLAYOFF_TARGETS
    if args.season:
        if args.season_type in ("all", "Regular Season"):
            reg_targets = [s for s in REGULAR_TARGETS if s in args.season]
        else:
            reg_targets = []
        if args.season_type in ("all", "Playoffs"):
            po_targets = [s for s in PLAYOFF_TARGETS if s in args.season]
        else:
            po_targets = []
    elif args.season_type == "Regular Season":
        po_targets = []
    elif args.season_type == "Playoffs":
        reg_targets = []

    reg_games, reg_names = game_universe(REG_DB, reg_targets, "Regular Season") if reg_targets else (pd.DataFrame(), pd.DataFrame())
    po_games, po_names = game_universe(PLAYOFF_DB, po_targets, "Playoffs") if po_targets else (pd.DataFrame(), pd.DataFrame())
    games = pd.concat([reg_games, po_games], ignore_index=True)
    names = pd.concat([reg_names, po_names], ignore_index=True)

    name_map = {
        (row.season, row.season_type, int(row.player_id)): row.player_name
        for row in names.itertuples(index=False)
    }
    season_games_map = {
        (row.season, row.season_type, int(row.player_id)): int(row.season_games)
        for row in names.itertuples(index=False)
    }

    counts: dict[tuple[str, str, int], dict[str, int]] = defaultdict(lambda: {"layup": 0, "dunk": 0, "other": 0})

    total_games = len(games)
    for idx, row in enumerate(games.itertuples(index=False), start=1):
        gid = str(row.game_id).zfill(10)
        actions = get_playbyplay_actions(gid, mmddyyyy(str(row.date)))
        for a in actions:
            if str(a.get("actionType") or "") != "2pt":
                continue
            if str(a.get("shotResult") or "") != "Made":
                continue
            try:
                assist_pid = int(a.get("assistPersonId") or 0)
            except Exception:
                assist_pid = 0
            if assist_pid <= 0:
                continue
            subtype = str(a.get("subType") or "")
            key = (row.season, row.season_type, assist_pid)
            if subtype == "Layup":
                counts[key]["layup"] += 1
            elif subtype.upper() == "DUNK":
                counts[key]["dunk"] += 1
            else:
                try:
                    x = float(a.get("xLegacy")) if a.get("xLegacy") is not None else None
                    y = float(a.get("yLegacy")) if a.get("yLegacy") is not None else None
                    dist = (x * x + y * y) ** 0.5 if x is not None and y is not None else None
                except Exception:
                    dist = None
                if dist is not None and dist <= OTHER_RIM_DISTANCE_MAX:
                    counts[key]["other"] += 1
        if args.progress_every > 0 and (idx % args.progress_every == 0 or idx == total_games):
            print(f"processed {idx}/{total_games} games", flush=True)

    recent_rows = []
    for (season, season_type, player_id), c in counts.items():
        games_played = season_games_map.get((season, season_type, player_id))
        strict_total = c["layup"] + c["dunk"]
        total = strict_total + c["other"]
        if total <= 0:
            continue
        recent_rows.append(
            {
                "season": season,
                "season_type": season_type,
                "player_id": player_id,
                "player_name": name_map.get((season, season_type, player_id), ""),
                "season_games": games_played,
                "layup_assists_created": c["layup"],
                "dunk_assists_created": c["dunk"],
                "other_rim_assists_created": c["other"],
                "rim_assists_strict": strict_total,
                "rim_assists_all": total,
                "layup_assists_created_per_game": (c["layup"] / games_played) if games_played else None,
                "dunk_assists_created_per_game": (c["dunk"] / games_played) if games_played else None,
                "other_rim_assists_created_per_game": (c["other"] / games_played) if games_played else None,
                "rim_assists_strict_per_game": (strict_total / games_played) if games_played else None,
                "rim_assists_all_per_game": (total / games_played) if games_played else None,
            }
        )

    recent_df = pd.DataFrame(recent_rows)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_key = f"{args.season_type.replace(' ', '_').lower()}_{'_'.join(sorted(set(args.season or reg_targets + po_targets)))}"
    recent_df.to_csv(CACHE_DIR / f"{cache_key}.csv", index=False)
    existing = pd.read_csv(OUT_PATH)
    drop_mask = (
        ((existing["season_type"] == "Regular Season") & existing["season"].isin(reg_targets))
        | ((existing["season_type"] == "Playoffs") & existing["season"].isin(po_targets))
    )
    merged = pd.concat([existing.loc[~drop_mask].copy(), recent_df], ignore_index=True)
    merged = merged.sort_values(["season_type", "season", "rim_assists_all", "player_name"], ascending=[True, True, False, True])
    merged.to_csv(OUT_PATH, index=False)
    print(f"Backfilled recent rim assists: added/replaced {len(recent_df)} rows in {OUT_PATH}")


if __name__ == "__main__":
    main()
