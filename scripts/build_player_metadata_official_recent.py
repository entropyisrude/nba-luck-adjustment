from __future__ import annotations

import csv
import time
from pathlib import Path

import duckdb
from nba_api.stats.endpoints import commonplayerinfo


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "nba_analytics.duckdb"
OUTPUT_PATH = DATA_DIR / "player_metadata_official_recent.csv"


def height_to_inches(height: str | None) -> int | None:
    if not height or "-" not in height:
        return None
    try:
        feet, inches = height.split("-", 1)
        return int(feet) * 12 + int(inches)
    except Exception:
        return None


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    columns = {row[1] for row in con.execute("PRAGMA table_info('player_game_facts')").fetchall()}
    if "age" in columns:
        targets = con.execute(
            """
            SELECT DISTINCT player_id, player_name
            FROM player_game_facts
            WHERE season = '2025-26'
              AND player_id IS NOT NULL
              AND age IS NULL
            ORDER BY player_name
            """
        ).fetchall()
    else:
        targets = con.execute(
            """
            SELECT DISTINCT player_id, player_name
            FROM player_game_facts
            WHERE season = '2025-26'
              AND player_id IS NOT NULL
            ORDER BY player_name
            """
        ).fetchall()
    con.close()

    existing: dict[int, dict[str, str]] = {}
    if OUTPUT_PATH.exists():
        with OUTPUT_PATH.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                try:
                    existing[int(row["player_id"])] = row
                except Exception:
                    continue

    fieldnames = [
        "player_id",
        "player_name",
        "birthdate",
        "listed_height",
        "height_inches",
        "from_year",
        "draft_year",
        "draft_round",
        "draft_number",
        "draft_overall_pick",
    ]

    rows = list(existing.values())
    fetched = 0
    skipped = 0
    for player_id, player_name in targets:
        existing_row = existing.get(int(player_id))
        if existing_row and all(existing_row.get(key, "") not in ("", None) for key in ["birthdate", "listed_height", "height_inches", "from_year", "draft_year"]):
            skipped += 1
            continue
        data = commonplayerinfo.CommonPlayerInfo(player_id=int(player_id), timeout=20).get_normalized_dict()["CommonPlayerInfo"][0]
        listed_height = data.get("HEIGHT") or ""
        draft_number = data.get("DRAFT_NUMBER") or ""
        row = {
            "player_id": int(player_id),
            "player_name": data.get("DISPLAY_FIRST_LAST") or player_name,
            "birthdate": data.get("BIRTHDATE") or "",
            "listed_height": listed_height,
            "height_inches": height_to_inches(listed_height) or "",
            "from_year": data.get("FROM_YEAR") or "",
            "draft_year": data.get("DRAFT_YEAR") or "",
            "draft_round": data.get("DRAFT_ROUND") or "",
            "draft_number": draft_number,
            "draft_overall_pick": draft_number,
        }
        if existing_row:
            rows = [r for r in rows if int(r["player_id"]) != int(player_id)]
        rows.append(row)
        existing[int(player_id)] = row
        fetched += 1
        if fetched % 10 == 0:
            with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(sorted(rows, key=lambda r: int(r["player_id"])))
            print(f"saved {fetched} fetched rows ({skipped} skipped)")
        time.sleep(0.25)

    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: int(r["player_id"])))
    print(f"wrote {len(rows)} rows to {OUTPUT_PATH} ({fetched} fetched, {skipped} skipped)")


if __name__ == "__main__":
    main()
