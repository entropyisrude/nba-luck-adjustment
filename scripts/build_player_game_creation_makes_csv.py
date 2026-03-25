from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
OUTPUT_PATH = ROOT / "data" / "player_game_creation_makes.csv"
PBP_DIR = Path(
    "/mnt/c/users/dave/Downloads/nba-3pt-adjust-local-backups/untracked_20260312_081227/data/pbp"
)


def default_row() -> dict[str, int]:
    return {
        "assisted_2pm": 0,
        "unassisted_2pm": 0,
        "assisted_3pm": 0,
        "unassisted_3pm": 0,
    }


def is_three(desc: str, shot_value: str) -> bool:
    desc_u = (desc or "").upper()
    return shot_value == "3" or "3PT" in desc_u or "3-PT" in desc_u


def is_assisted(desc: str) -> bool:
    return "AST)" in (desc or "").upper()


def main() -> None:
    stats: dict[tuple[str, int], dict[str, int]] = defaultdict(default_row)

    for year in range(1996, 2026):
        pbp_file = PBP_DIR / f"nbastatsv3_{year}.csv"
        if not pbp_file.exists():
            continue
        print(f"Processing {pbp_file.name}...")
        with pbp_file.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("actionType") != "Made Shot":
                    continue
                game_id = str(row.get("gameId") or "").strip()
                if not game_id:
                    continue
                try:
                    player_id = int(row.get("personId") or "0")
                except ValueError:
                    continue
                if player_id <= 0:
                    continue

                desc = row.get("description") or ""
                shot_value = row.get("shotValue") or ""
                three = is_three(desc, shot_value)
                assisted = is_assisted(desc)

                bucket = stats[(game_id, player_id)]
                if three and assisted:
                    bucket["assisted_3pm"] += 1
                elif three:
                    bucket["unassisted_3pm"] += 1
                elif assisted:
                    bucket["assisted_2pm"] += 1
                else:
                    bucket["unassisted_2pm"] += 1

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "game_id",
                "player_id",
                "assisted_2pm",
                "unassisted_2pm",
                "assisted_3pm",
                "unassisted_3pm",
                "assisted_fgm",
                "unassisted_fgm",
            ]
        )
        for (game_id, player_id), row in sorted(stats.items(), key=lambda x: (x[0][0], x[0][1])):
            assisted_fgm = row["assisted_2pm"] + row["assisted_3pm"]
            unassisted_fgm = row["unassisted_2pm"] + row["unassisted_3pm"]
            writer.writerow(
                [
                    game_id,
                    player_id,
                    row["assisted_2pm"],
                    row["unassisted_2pm"],
                    row["assisted_3pm"],
                    row["unassisted_3pm"],
                    assisted_fgm,
                    unassisted_fgm,
                ]
            )
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
