from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
SRC_CSV = Path("/mnt/c/users/dave/Downloads/nba-boxscore-data/kaggle-traditional/traditional.csv")

OUT_PLAYER_CSV = DATA_DIR / "player_boxscore_stats_kaggle_traditional.csv"
OUT_GAME_META_CSV = DATA_DIR / "game_metadata_kaggle_traditional.csv"


def _to_int(value) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def _season_label_to_range(label: str) -> str:
    y = int(label)
    start = y - 1
    return f"{start}-{str(y)[2:]}"


def main() -> None:
    player_rows: list[dict] = []
    game_team_points: dict[str, dict[str, int]] = defaultdict(dict)
    game_team_ids: dict[str, dict[str, int]] = defaultdict(dict)
    game_meta_seed: dict[str, dict] = {}

    with open(SRC_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("type") != "regular":
                continue

            season_range = _season_label_to_range(row["season"])
            game_id = str(row["gameid"]).lstrip("0")
            if not game_id:
                continue

            team_abbr = str(row["team"] or "")
            home_abbr = str(row["home"] or "")
            away_abbr = str(row["away"] or "")
            pts = _to_int(row["PTS"])

            game_team_points[game_id][team_abbr] = game_team_points[game_id].get(team_abbr, 0) + pts
            game_meta_seed[game_id] = {
                "date": row["date"],
                "season": season_range,
                "home_team": home_abbr,
                "away_team": away_abbr,
            }

            player_rows.append(
                {
                    "date": row["date"],
                    "season": season_range,
                    "game_id": game_id,
                    "team_id": None,
                    "team_abbr": team_abbr,
                    "player_id": _to_int(row["playerid"]),
                    "player_name": row["player"],
                    "starter": None,
                    "minutes": row["MIN"],
                    "pts": pts,
                    "reb": _to_int(row["REB"]),
                    "oreb": _to_int(row["OREB"]),
                    "dreb": _to_int(row["DREB"]),
                    "ast": _to_int(row["AST"]),
                    "stl": _to_int(row["STL"]),
                    "blk": _to_int(row["BLK"]),
                    "tov": _to_int(row["TOV"]),
                    "pf": _to_int(row["PF"]),
                    "fgm": _to_int(row["FGM"]),
                    "fga": _to_int(row["FGA"]),
                    "fg3m": _to_int(row["3PM"]),
                    "fg3a": _to_int(row["3PA"]),
                    "ftm": _to_int(row["FTM"]),
                    "fta": _to_int(row["FTA"]),
                    "plus_minus_actual": _to_int(row["+/-"]),
                }
            )

    game_rows: list[dict] = []
    for game_id, meta in game_meta_seed.items():
        home_abbr = meta["home_team"]
        away_abbr = meta["away_team"]
        pts_by_team = game_team_points.get(game_id, {})
        game_rows.append(
            {
                "date": meta["date"],
                "season": meta["season"],
                "game_id": game_id,
                "home_team_id": None,
                "away_team_id": None,
                "home_team": home_abbr,
                "away_team": away_abbr,
                "home_pts_actual": pts_by_team.get(home_abbr, 0),
                "away_pts_actual": pts_by_team.get(away_abbr, 0),
            }
        )

    OUT_PLAYER_CSV.parent.mkdir(parents=True, exist_ok=True)

    with open(OUT_PLAYER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "date",
                "season",
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
                "season",
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
