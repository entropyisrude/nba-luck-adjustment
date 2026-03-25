from __future__ import annotations

import csv
import shutil
from pathlib import Path


FILES = [
    (
        Path("/mnt/c/users/dave/Downloads/nba-onoff-publish/data/adjusted_onoff_historical_pbp_v2.csv"),
        ["game_id", "team_id", "player_id"],
    ),
    (
        Path("/mnt/c/users/dave/Downloads/nba-onoff-publish/data/stints_historical_pbp_v2.csv"),
        ["game_id", "stint_index"],
    ),
    (
        Path("/mnt/c/users/dave/Downloads/nba-onoff-publish/data/possessions_historical_pbp_v2.csv"),
        ["game_id", "poss_index"],
    ),
]


def dedupe_csv(path: Path, key_cols: list[str]) -> None:
    backup = path.with_suffix(path.suffix + ".pre_dedupe")
    if not backup.exists():
        shutil.copy2(path, backup)

    tmp = path.with_suffix(path.suffix + ".tmp")
    seen: set[tuple[str, ...]] = set()
    total = 0
    kept = 0

    with path.open("r", newline="", encoding="utf-8") as src, tmp.open(
        "w", newline="", encoding="utf-8"
    ) as dst:
        reader = csv.DictReader(src)
        if reader.fieldnames is None:
            raise RuntimeError(f"missing header: {path}")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            total += 1
            key = tuple(str(row[col]) for col in key_cols)
            if key in seen:
                continue
            seen.add(key)
            writer.writerow(row)
            kept += 1

    tmp.replace(path)
    print(f"{path.name}: total={total} kept={kept} removed={total-kept}")


def main() -> None:
    for path, key_cols in FILES:
        dedupe_csv(path, key_cols)


if __name__ == "__main__":
    main()
