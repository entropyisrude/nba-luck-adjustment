from __future__ import annotations

import csv
from pathlib import Path

import requests


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
GAMES_CSV = DATA_DIR / "adjusted_games.csv"
OUT_CSV = DATA_DIR / "player_boxscore_stats.csv"

BOXSCORE_URL = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
}


def _num(value) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _team_rows(game_id: str, game: dict, side_key: str) -> list[dict]:
    team = (game.get(side_key) or {})
    team_id = int(team.get("teamId", 0) or 0)
    team_abbr = str(team.get("teamTricode") or "")
    out: list[dict] = []
    for p in (team.get("players") or []):
        if str(p.get("played", "0")) != "1":
            continue
        pid = p.get("personId")
        if pid is None:
            continue
        first = (p.get("firstName") or "").strip()
        last = (p.get("familyName") or p.get("lastName") or "").strip()
        name = (first + " " + last).strip() or str(p.get("name") or p.get("nameI") or "")
        stats = p.get("statistics") or {}
        out.append(
            {
                "game_id": str(game_id).lstrip("0"),
                "team_id": team_id,
                "team_abbr": team_abbr,
                "player_id": int(pid),
                "player_name": name,
                "starter": 1 if str(p.get("starter", "0")) == "1" else 0,
                "minutes": str(stats.get("minutes") or ""),
                "pts": int(_num(stats.get("points"))),
                "reb": int(_num(stats.get("reboundsTotal"))),
                "oreb": int(_num(stats.get("reboundsOffensive"))),
                "dreb": int(_num(stats.get("reboundsDefensive"))),
                "ast": int(_num(stats.get("assists"))),
                "stl": int(_num(stats.get("steals"))),
                "blk": int(_num(stats.get("blocks"))),
                "tov": int(_num(stats.get("turnovers"))),
                "pf": int(_num(stats.get("foulsPersonal"))),
                "fgm": int(_num(stats.get("fieldGoalsMade"))),
                "fga": int(_num(stats.get("fieldGoalsAttempted"))),
                "fg3m": int(_num(stats.get("threePointersMade"))),
                "fg3a": int(_num(stats.get("threePointersAttempted"))),
                "ftm": int(_num(stats.get("freeThrowsMade"))),
                "fta": int(_num(stats.get("freeThrowsAttempted"))),
            }
        )
    return out


def main() -> None:
    game_rows: list[tuple[str, str]] = []
    with open(GAMES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            game_rows.append((str(row["game_id"]).lstrip("0"), row["date"]))

    unique_games = sorted(set(game_rows))
    records: list[dict] = []

    session = requests.Session()
    session.headers.update(HEADERS)

    for game_id, game_date in unique_games:
        gid10 = game_id.zfill(10)
        url = BOXSCORE_URL.format(game_id=gid10)
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        js = resp.json()
        game = js.get("game") or {}
        for side in ("homeTeam", "awayTeam"):
            for rec in _team_rows(game_id, game, side):
                rec["date"] = game_date
                records.append(rec)

    fieldnames = [
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
    ]

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"Wrote {OUT_CSV} rows={len(records)} games={len(unique_games)}")


if __name__ == "__main__":
    main()
