from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import nba_pages  # noqa: E402
from src.season import SEASON_DIR, SEASON_START_DATE, TAG  # noqa: E402

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", Path(__file__).resolve().parents[1]))
DATA_DIR = ROOT / "data"
GAMES_CSV = DATA_DIR / "adjusted_games.csv"
OUT_CSV = DATA_DIR / "player_boxscore_stats.csv"
# Current season: every listed player per game with "played" or nba.com's DNP
# comment ("Inactive - Injury/Illness", "DNP - Coach's Decision", ...).
STATUS_CSV = SEASON_DIR / f"player_game_status_{TAG}.csv"
STATUS_FIELDS = ["date", "game_id", "team_abbr", "player_id", "player_name", "status"]


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


def _status_rows(game_id: str, game_date: str, game: dict) -> list[dict]:
    out: list[dict] = []
    for side_key in ("homeTeam", "awayTeam"):
        team = game.get(side_key) or {}
        seen: set[int] = set()
        # Some pages list inactive players only under `inactives`, with no reason.
        listed = [(p, False) for p in team.get("players") or []] + \
                 [(p, True) for p in team.get("inactives") or []]
        for p, inactive_only in listed:
            if p.get("personId") is None or int(p["personId"]) in seen:
                continue
            seen.add(int(p["personId"]))
            first = (p.get("firstName") or "").strip()
            last = (p.get("familyName") or p.get("lastName") or "").strip()
            if inactive_only:
                status = "Inactive"
            elif str(p.get("played", "0")) == "1":
                status = "played"
            else:
                reason = str(p.get("comment") or p.get("notPlayingReason") or "").strip()
                if not reason and str(p.get("status") or "").upper() == "INACTIVE":
                    reason = "Inactive"
                status = reason or "DNP"
            out.append({
                "date": game_date,
                "game_id": str(game_id).lstrip("0"),
                "team_abbr": str(team.get("teamTricode") or ""),
                "player_id": int(p["personId"]),
                "player_name": (first + " " + last).strip() or str(p.get("nameI") or ""),
                "status": status,
            })
    return out


def update_status(game_rows: list[tuple[str, str]]) -> None:
    """Fetch player statuses for current-season games not yet in STATUS_CSV."""
    existing: list[dict] = []
    if STATUS_CSV.exists():
        with open(STATUS_CSV, newline="", encoding="utf-8") as f:
            existing = list(csv.DictReader(f))
    done = {r["game_id"] for r in existing}
    todo = [(g, d) for g, d in game_rows if d >= SEASON_START_DATE and g not in done]
    rows = list(existing)
    for idx, (game_id, game_date) in enumerate(todo, start=1):
        if idx == 1 or idx % 25 == 0 or idx == len(todo):
            print(f"Fetching player statuses {idx}/{len(todo)}...")
        try:
            game = nba_pages.boxscore_json(game_id.zfill(10)).get("game") or {}
        except Exception as e:
            print(f"  status fetch failed for {game_id}: {e}")
            continue
        rows.extend(_status_rows(game_id, game_date, game))
    STATUS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(STATUS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=STATUS_FIELDS)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: (r["date"], r["game_id"], r["team_abbr"], r["player_name"])))
    print(f"Wrote {STATUS_CSV} rows={len(rows)} (fetched {len(todo)} games)")


def main() -> None:
    game_rows: list[tuple[str, str]] = []
    with open(GAMES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            game_rows.append((str(row["game_id"]).lstrip("0"), row["date"]))

    unique_games = sorted(set(game_rows))
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

    existing_records: list[dict] = []
    existing_game_ids: set[str] = set()
    if OUT_CSV.exists():
        with open(OUT_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["game_id"] = str(row.get("game_id") or "").lstrip("0")
                existing_records.append(row)
                if row["game_id"]:
                    existing_game_ids.add(row["game_id"])

    records: list[dict] = list(existing_records)
    missing_games = [(game_id, game_date) for game_id, game_date in unique_games if game_id not in existing_game_ids]

    for idx, (game_id, game_date) in enumerate(missing_games, start=1):
        if idx == 1 or idx % 25 == 0 or idx == len(missing_games):
            print(f"Fetching boxscores {idx}/{len(missing_games)}...")
        game = nba_pages.boxscore_json(game_id.zfill(10)).get("game") or {}
        for side in ("homeTeam", "awayTeam"):
            for rec in _team_rows(game_id, game, side):
                rec["date"] = game_date
                records.append(rec)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted(records, key=lambda r: (r["date"], r["game_id"], r["team_id"], r["player_name"])))

    print(
        f"Wrote {OUT_CSV} rows={len(records)} games={len(unique_games)} "
        f"(fetched {len(missing_games)} new games, reused {len(existing_game_ids)} existing)"
    )
    # Same game pages, already cached by nba_pages for new games.
    update_status(unique_games)


if __name__ == "__main__":
    main()
