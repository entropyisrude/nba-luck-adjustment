#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PLAYER_INFO_MAP = DATA_DIR / "player_info_map.json"

TARGETS = [
    DATA_DIR / "adjusted_onoff.csv",
    DATA_DIR / "player_daily_boxscore.csv",
    DATA_DIR / "adjusted_onoff_playoffs.csv",
    DATA_DIR / "player_onoff_history.csv",
]


def load_name_map() -> dict[str, str]:
    if not PLAYER_INFO_MAP.exists():
        raise FileNotFoundError(f"Missing {PLAYER_INFO_MAP}")
    raw = json.loads(PLAYER_INFO_MAP.read_text(encoding="utf-8"))
    name_map: dict[str, str] = {}
    for pid, rec in raw.items():
        name = rec.get("name")
        if isinstance(name, str) and name.strip():
            name_map[str(pid)] = name.strip()
    return name_map


def rewrite_names(path: Path, name_map: dict[str, str]) -> tuple[int, int]:
    tmp = path.with_suffix(path.suffix + ".tmp")
    total = 0
    changed = 0
    with path.open("r", newline="", encoding="utf-8") as f_in, tmp.open(
        "w", newline="", encoding="utf-8"
    ) as f_out:
        reader = csv.DictReader(f_in)
        if not reader.fieldnames or "player_id" not in reader.fieldnames or "player_name" not in reader.fieldnames:
            raise ValueError(f"{path} missing player_id/player_name columns")
        writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            total += 1
            pid = row.get("player_id")
            if pid is not None:
                new_name = name_map.get(str(pid))
                if new_name and row.get("player_name") != new_name:
                    row["player_name"] = new_name
                    changed += 1
            writer.writerow(row)
    tmp.replace(path)
    return total, changed


def main() -> int:
    name_map = load_name_map()
    print(f"Loaded {len(name_map)} names from {PLAYER_INFO_MAP}")
    for path in TARGETS:
        if not path.exists():
            print(f"Skip missing: {path}")
            continue
        total, changed = rewrite_names(path, name_map)
        print(f"{path.name}: {changed}/{total} rows updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
