from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
HIST_ONOFF_CSV = DATA_DIR / "adjusted_onoff_historical_pbp.csv"
STATS_CACHE_DIR = DATA_DIR / "stats_cache"
BOXSCORE_DIR = STATS_CACHE_DIR / "boxscoretraditionalv2"
SUMMARY_DIR = STATS_CACHE_DIR / "boxscoresummaryv2"

OUT_PLAYER_CSV = DATA_DIR / "player_boxscore_stats_historical_cache.csv"
OUT_GAME_META_CSV = DATA_DIR / "historical_game_metadata_cache.csv"


def _load_game_dates() -> dict[str, str]:
    out: dict[str, str] = {}
    with open(HIST_ONOFF_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = str(row["game_id"]).lstrip("0")
            if gid and gid not in out:
                out[gid] = row["date"]
    return out


def _read_json(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _row_dicts(payload: dict) -> list[dict]:
    cols = payload.get("columns") or []
    data = payload.get("data") or []
    return [dict(zip(cols, row)) for row in data]


def _to_int(value) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def main() -> None:
    game_dates = _load_game_dates()

    player_rows: list[dict] = []
    game_rows: list[dict] = []

    for players_path in sorted(BOXSCORE_DIR.glob("*_players.json")):
        gid = players_path.name.split("_")[0].lstrip("0")
        if gid not in game_dates:
            continue

        teams_path = players_path.with_name(players_path.name.replace("_players.json", "_teams.json"))
        summary_path = SUMMARY_DIR / f"{players_path.name.split('_')[0]}.json"
        if not teams_path.exists() or not summary_path.exists():
            continue

        players_payload = _read_json(players_path)
        teams_payload = _read_json(teams_path)
        summary_payload = _read_json(summary_path)

        player_dicts = _row_dicts(players_payload)
        team_dicts = _row_dicts(teams_payload)

        team_abbr_by_id = {
            _to_int(r.get("TEAM_ID")): str(r.get("TEAM_ABBREVIATION") or "")
            for r in team_dicts
        }
        team_pts_by_id = {
            _to_int(r.get("TEAM_ID")): _to_int(r.get("PTS"))
            for r in team_dicts
        }

        home_id = _to_int(summary_payload.get("home_id"))
        away_id = _to_int(summary_payload.get("away_id"))
        home_abbr = team_abbr_by_id.get(home_id, "")
        away_abbr = team_abbr_by_id.get(away_id, "")

        game_rows.append(
            {
                "date": game_dates[gid],
                "game_id": gid,
                "home_team_id": home_id,
                "away_team_id": away_id,
                "home_team": home_abbr,
                "away_team": away_abbr,
                "home_pts_actual": team_pts_by_id.get(home_id, 0),
                "away_pts_actual": team_pts_by_id.get(away_id, 0),
            }
        )

        for row in player_dicts:
            player_rows.append(
                {
                    "date": game_dates[gid],
                    "game_id": gid,
                    "team_id": _to_int(row.get("TEAM_ID")),
                    "team_abbr": str(row.get("TEAM_ABBREVIATION") or ""),
                    "player_id": _to_int(row.get("PLAYER_ID")),
                    "player_name": str(row.get("PLAYER_NAME") or ""),
                    "starter": 1 if str(row.get("START_POSITION") or "").strip() else 0,
                    "minutes": str(row.get("MIN") or ""),
                    "pts": _to_int(row.get("PTS")),
                    "reb": _to_int(row.get("REB")),
                    "oreb": _to_int(row.get("OREB")),
                    "dreb": _to_int(row.get("DREB")),
                    "ast": _to_int(row.get("AST")),
                    "stl": _to_int(row.get("STL")),
                    "blk": _to_int(row.get("BLK")),
                    "tov": _to_int(row.get("TO")),
                    "pf": _to_int(row.get("PF")),
                    "fgm": _to_int(row.get("FGM")),
                    "fga": _to_int(row.get("FGA")),
                    "fg3m": _to_int(row.get("FG3M")),
                    "fg3a": _to_int(row.get("FG3A")),
                    "ftm": _to_int(row.get("FTM")),
                    "fta": _to_int(row.get("FTA")),
                    "plus_minus_actual": row.get("PLUS_MINUS"),
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

    seen_games: set[str] = set()
    deduped_games: list[dict] = []
    for row in game_rows:
        gid = row["game_id"]
        if gid in seen_games:
            continue
        seen_games.add(gid)
        deduped_games.append(row)

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
        writer.writerows(deduped_games)

    print(f"Wrote {OUT_PLAYER_CSV} rows={len(player_rows)}")
    print(f"Wrote {OUT_GAME_META_CSV} rows={len(deduped_games)}")


if __name__ == "__main__":
    main()
