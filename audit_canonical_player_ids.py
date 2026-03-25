from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def _season_from_date(date_str: str) -> str:
    year, month, _ = map(int, date_str.split("-"))
    start = year if month >= 9 else year - 1
    return f"{start}-{str(start + 1)[2:]}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit likely canonical player-id merges from rebuilt on/off rows.")
    parser.add_argument(
        "--onoff-path",
        default="data/adjusted_onoff_historical_pbp_20260317_vwd.csv",
        help="Rebuilt on/off CSV to inspect.",
    )
    parser.add_argument(
        "--player-info-map",
        default="data/player_info_map.json",
        help="Full-name player info map JSON.",
    )
    args = parser.parse_args()

    onoff_path = Path(args.onoff_path)
    info_path = Path(args.player_info_map)

    with info_path.open(encoding="utf-8") as f:
        player_info = json.load(f)

    stats_by_pid: dict[str, dict[str, object]] = {}
    with onoff_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pid = row["player_id"]
            info = stats_by_pid.setdefault(
                pid,
                {
                    "rows": 0,
                    "first_date": row["date"],
                    "last_date": row["date"],
                    "teams": set(),
                    "seasons": set(),
                    "surname": row["player_name"],
                },
            )
            info["rows"] = int(info["rows"]) + 1
            if row["date"] < str(info["first_date"]):
                info["first_date"] = row["date"]
            if row["date"] > str(info["last_date"]):
                info["last_date"] = row["date"]
            cast_teams = info["teams"]
            cast_seasons = info["seasons"]
            assert isinstance(cast_teams, set)
            assert isinstance(cast_seasons, set)
            cast_teams.add(row["team_id"])
            cast_seasons.add(_season_from_date(row["date"]))

    ids_by_full_name: dict[str, list[str]] = defaultdict(list)
    for pid, meta in stats_by_pid.items():
        full_name = str((player_info.get(pid) or {}).get("name") or "").strip()
        if full_name:
            ids_by_full_name[full_name].append(pid)

    candidates: list[dict[str, object]] = []
    for full_name, pids in ids_by_full_name.items():
        if len(pids) < 2:
            continue
        members = []
        for pid in sorted(pids, key=lambda x: (str(stats_by_pid[x]["first_date"]), int(x))):
            meta = stats_by_pid[pid]
            members.append(
                {
                    "player_id": pid,
                    "rows": int(meta["rows"]),
                    "first_date": str(meta["first_date"]),
                    "last_date": str(meta["last_date"]),
                    "teams": sorted(meta["teams"]),
                    "seasons": sorted(meta["seasons"]),
                }
            )

        non_overlapping = True
        for left, right in zip(members, members[1:]):
            if str(left["last_date"]) >= str(right["first_date"]):
                non_overlapping = False
                break

        tight_gap = True
        same_team_bridge = False
        for left, right in zip(members, members[1:]):
            left_last = str(left["last_date"])
            right_first = str(right["first_date"])
            left_year = int(left_last[:4])
            right_year = int(right_first[:4])
            if right_year - left_year > 2:
                tight_gap = False
            if set(left["teams"]) & set(right["teams"]):
                same_team_bridge = True

        candidates.append(
            {
                "full_name": full_name,
                "non_overlapping": non_overlapping,
                "tight_gap": tight_gap,
                "same_team_bridge": same_team_bridge,
                "members": members,
            }
        )

    candidates.sort(key=lambda c: (not bool(c["non_overlapping"]), not bool(c["tight_gap"]), c["full_name"]))  # type: ignore[index]

    print(f"Candidate full-name groups with multiple ids in {onoff_path}:")
    for group in candidates:
        if group["non_overlapping"] and group["tight_gap"] and group["same_team_bridge"]:
            status = "CONTINUITY_REVIEW"
        elif group["non_overlapping"]:
            status = "SAME_NAME_REVIEW"
        else:
            status = "OVERLAP_REVIEW"
        print(
            f"\n[{status}] {group['full_name']} "
            f"(non_overlapping={group['non_overlapping']} "
            f"tight_gap={group['tight_gap']} "
            f"same_team_bridge={group['same_team_bridge']})"
        )
        for member in group["members"]:  # type: ignore[index]
            print(
                f"  id={member['player_id']} rows={member['rows']} "
                f"first={member['first_date']} last={member['last_date']} "
                f"teams={member['teams']} seasons={member['seasons'][:4]}{'...' if len(member['seasons']) > 4 else ''}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
