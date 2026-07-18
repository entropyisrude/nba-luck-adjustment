"""Finish stalled retry games in isolated, time-bounded worker processes."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
QUEUE = REBUILD / "period_boundary_retry_queues" / "games_2019.csv"
CHECKPOINT = REBUILD / "audit_retry_period_2019.csv"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--timeout", type=float, default=60.0)
    args = ap.parse_args()

    queue = pd.read_csv(QUEUE, dtype={"game_id": str})
    queue["game_id"] = queue.game_id.str.lstrip("0")
    done = set()
    if CHECKPOINT.exists():
        prior = pd.read_csv(CHECKPOINT, dtype={"game_id": str})
        done = set(prior.game_id.str.lstrip("0"))
    games = [gid for gid in queue.game_id.drop_duplicates() if gid not in done]
    print(f"isolated games: {len(games)}", flush=True)

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    env["NBA_STATS_CACHE_ONLY"] = "1"

    def run(gid: str) -> dict[str, object]:
        tag = f"retry_isolated_{gid}"
        cmd = [sys.executable, str(ROOT / "scripts" / "build_canonical_replay.py"),
               "--games", gid, "--tag", tag, "--solver-seconds", "1"]
        try:
            result = subprocess.run(cmd, cwd=ROOT, env=env,
                                    stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL,
                                    timeout=args.timeout)
            return {"game_id": gid, "tag": tag, "returncode": result.returncode,
                    "timed_out": False}
        except subprocess.TimeoutExpired:
            return {"game_id": gid, "tag": tag, "returncode": -1,
                    "timed_out": True}

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run, gid) for gid in games]
        for future in as_completed(futures):
            row = future.result()
            results.append(row)
            print(row, flush=True)

    audit_frames = []
    stint_frames = []
    for row in results:
        audit_path = REBUILD / f"audit_{row['tag']}.csv"
        stint_path = REBUILD / f"stints_{row['tag']}.parquet"
        if audit_path.exists():
            audit_frames.append(pd.read_csv(audit_path))
        else:
            audit_frames.append(pd.DataFrame([{
                "game_id": row["game_id"], "accepted": False,
                "error": "isolated worker timeout" if row["timed_out"]
                         else f"isolated worker returncode {row['returncode']}",
            }]))
        if stint_path.exists():
            stint_frames.append(pd.read_parquet(stint_path))
    pd.concat(audit_frames, ignore_index=True).to_csv(
        REBUILD / "audit_retry_isolated.csv", index=False)
    if stint_frames:
        pd.concat(stint_frames, ignore_index=True).to_parquet(
            REBUILD / "stints_retry_isolated.parquet", index=False)
    pd.DataFrame(results).to_csv(REBUILD / "retry_isolated_process_status.csv", index=False)


if __name__ == "__main__":
    main()
