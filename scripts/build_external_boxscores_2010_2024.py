from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
SRC_DIR = Path("/mnt/c/users/dave/Downloads/nba-boxscore-data/NBA-Data-2010-2024")

PARTS = [
    SRC_DIR / "regular_season_box_scores_2010_2024_part_1.csv",
    SRC_DIR / "regular_season_box_scores_2010_2024_part_2.csv",
    SRC_DIR / "regular_season_box_scores_2010_2024_part_3.csv",
]

OUT_PLAYER_CSV = DATA_DIR / "player_boxscore_stats_external_2010_2024.csv"
OUT_GAME_META_CSV = DATA_DIR / "game_metadata_external_2010_2024.csv"


def _to_int(value) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def _parse_matchup(matchup: str) -> tuple[str | None, str | None]:
    s = (matchup or "").strip()
    if " @ " in s:
        away, home = s.split(" @ ", 1)
        return home.strip(), away.strip()
    if " vs. " in s:
        home, away = s.split(" vs. ", 1)
        return home.strip(), away.strip()
    return None, None


def main() -> None:
    player_rows: list[dict] = []
    game_team_points: dict[str, dict[str, int]] = defaultdict(dict)
    game_team_ids: dict[str, dict[str, int]] = defaultdict(dict)
    game_matchups: dict[str, str] = {}
    game_dates: dict[str, str] = {}

    for path in PARTS:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                game_id = str(row["gameId"]).lstrip("0")
                team_abbr = str(row["teamTricode"] or "")
                team_id = _to_int(row["teamId"])
                pts = _to_int(row["points"])

                game_dates[game_id] = row["game_date"]
                game_matchups[game_id] = row["matchup"]
                game_team_points[game_id][team_abbr] = game_team_points[game_id].get(team_abbr, 0) + pts
                game_team_ids[game_id][team_abbr] = team_id

                player_rows.append(
                    {
                        "date": row["game_date"],
                        "game_id": game_id,
                        "team_id": team_id,
                        "team_abbr": team_abbr,
                        "player_id": _to_int(row["personId"]),
                        "player_name": row["personName"],
                        "starter": 1 if str(row.get("position") or "").strip() else 0,
                        "minutes": row["minutes"],
                        "pts": pts,
                        "reb": _to_int(row["reboundsTotal"]),
                        "oreb": _to_int(row["reboundsOffensive"]),
                        "dreb": _to_int(row["reboundsDefensive"]),
                        "ast": _to_int(row["assists"]),
                        "stl": _to_int(row["steals"]),
                        "blk": _to_int(row["blocks"]),
                        "tov": _to_int(row["turnovers"]),
                        "pf": _to_int(row["foulsPersonal"]),
                        "fgm": _to_int(row["fieldGoalsMade"]),
                        "fga": _to_int(row["fieldGoalsAttempted"]),
                        "fg3m": _to_int(row["threePointersMade"]),
                        "fg3a": _to_int(row["threePointersAttempted"]),
                        "ftm": _to_int(row["freeThrowsMade"]),
                        "fta": _to_int(row["freeThrowsAttempted"]),
                        "plus_minus_actual": _to_int(row["plusMinusPoints"]),
                    }
                )

    game_rows: list[dict] = []
    for game_id, matchup in game_matchups.items():
        home_abbr, away_abbr = _parse_matchup(matchup)
        if not home_abbr or not away_abbr:
            continue
        team_points = game_team_points.get(game_id, {})
        team_ids = game_team_ids.get(game_id, {})
        game_rows.append(
            {
                "date": game_dates.get(game_id, ""),
                "game_id": game_id,
                "home_team_id": team_ids.get(home_abbr, 0),
                "away_team_id": team_ids.get(away_abbr, 0),
                "home_team": home_abbr,
                "away_team": away_abbr,
                "home_pts_actual": team_points.get(home_abbr, 0),
                "away_pts_actual": team_points.get(away_abbr, 0),
            }
        )

    OUT_PLAYER_CSV.parent.mkdir(parents=True, exist_ok=True)

    with open(OUT_PLAYER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "date",
                "game_id",
                "team_id",
                "team_abbr",
                "player_id",
                "player_name",
                "starter",
                "minutes",
                "pts",
                "reb",
                "oreb",
                "dreb",
                "ast",
                "stl",
                "blk",
                "tov",
                "pf",
                "fgm",
                "fga",
                "fg3m",
                "fg3a",
                "ftm",
                "fta",
                "plus_minus_actual",
            ],
        )
        writer.writeheader()
        writer.writerows(player_rows)

    with open(OUT_GAME_META_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "date",
                "game_id",
                "home_team_id",
                "away_team_id",
                "home_team",
                "away_team",
                "home_pts_actual",
                "away_pts_actual",
            ],
        )
        writer.writeheader()
        writer.writerows(game_rows)

    print(f"Wrote {OUT_PLAYER_CSV} rows={len(player_rows)}")
    print(f"Wrote {OUT_GAME_META_CSV} rows={len(game_rows)}")


if __name__ == "__main__":
    main()
