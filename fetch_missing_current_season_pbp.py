from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import playbyplayv3


CURRENT_COLUMNS = [
    "actionNumber",
    "clock",
    "period",
    "teamId",
    "teamTricode",
    "personId",
    "playerName",
    "playerNameI",
    "xLegacy",
    "yLegacy",
    "shotDistance",
    "shotResult",
    "isFieldGoal",
    "scoreHome",
    "scoreAway",
    "pointsTotal",
    "location",
    "description",
    "actionType",
    "subType",
    "videoAvailable",
    "shotValue",
    "actionId",
    "gameId",
]


def _full_game_id(stripped_game_id: str) -> str:
    gid = str(stripped_game_id).strip()
    if gid.startswith("00") and len(gid) == 10:
        return gid
    return gid.zfill(10)


def _fetch_actions(stripped_game_id: str, timeout: int = 60) -> list[dict]:
    full_gid = _full_game_id(stripped_game_id)
    pbp = playbyplayv3.PlayByPlayV3(game_id=full_gid, timeout=timeout)
    data = pbp.get_dict() or {}
    return ((data.get("game") or {}).get("actions") or [])


def _missing_current_game_ids(local_pbp_path: Path, stints_path: Path, season_prefix: str) -> list[str]:
    local_ids = set(
        pd.read_csv(local_pbp_path, usecols=["gameId"], dtype={"gameId": str})["gameId"]
        .astype(str)
        .str.zfill(8)
    )
    current = pd.read_csv(stints_path, usecols=["game_id", "date"], dtype={"game_id": str})
    current = current[current["game_id"].str.startswith(season_prefix)]
    current = current[current["date"] >= "2025-10-01"]
    wanted = current[["game_id"]].drop_duplicates()["game_id"].astype(str).str.zfill(8)
    return sorted(gid for gid in wanted if gid not in local_ids)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local-pbp-path",
        default="/mnt/c/users/dave/Downloads/nba-3pt-adjust-local-backups/untracked_20260312_081227/data/pbp/nbastatsv3_2025.csv",
    )
    parser.add_argument(
        "--stints-path",
        default="/mnt/c/users/dave/Downloads/nba-onoff-publish/data/stints.csv",
    )
    parser.add_argument("--season-prefix", default="225")
    parser.add_argument("--sleep-seconds", type=float, default=0.75)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    local_pbp_path = Path(args.local_pbp_path)
    stints_path = Path(args.stints_path)

    missing_ids = _missing_current_game_ids(local_pbp_path, stints_path, args.season_prefix)
    if args.limit > 0:
        missing_ids = missing_ids[: args.limit]

    print(f"missing_games={len(missing_ids)}", flush=True)
    if not missing_ids:
        return 0

    fetched_frames: list[pd.DataFrame] = []
    failed: list[str] = []
    for idx, gid in enumerate(missing_ids, start=1):
        try:
            actions = _fetch_actions(gid, timeout=args.timeout)
            if not actions:
                raise RuntimeError("no actions returned")
            df = pd.DataFrame(actions)
            df["gameId"] = gid
            for col in CURRENT_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            df = df[CURRENT_COLUMNS]
            fetched_frames.append(df)
            print(f"[{idx}/{len(missing_ids)}] fetched {gid} actions={len(df)}", flush=True)
        except Exception as exc:
            failed.append(gid)
            print(f"[{idx}/{len(missing_ids)}] FAILED {gid}: {exc!r}", flush=True)
        time.sleep(args.sleep_seconds)

    if fetched_frames:
        existing = pd.read_csv(local_pbp_path, dtype={"gameId": str})
        combined = pd.concat([existing] + fetched_frames, ignore_index=True)
        combined["gameId"] = combined["gameId"].astype(str).str.zfill(8)
        combined = combined.drop_duplicates(subset=["gameId", "actionNumber", "actionId"], keep="first")
        combined = combined.sort_values(["gameId", "actionNumber", "actionId"], kind="stable")
        combined.to_csv(local_pbp_path, index=False)
        print(f"wrote_rows={len(combined)} wrote_games={combined['gameId'].nunique()}", flush=True)

    if failed:
        print("failed_game_ids=" + ",".join(failed), flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
