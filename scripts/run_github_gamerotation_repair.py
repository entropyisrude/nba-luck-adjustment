"""Audit pinned GitHub GameRotation candidates in parallel by season."""
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
SOURCE = REBUILD / "remaining_quarantine_queues" / "github_gamerotation_complete.csv"
CACHE = REBUILD / "github_gamerotation_cache"
QUEUES = REBUILD / "github_gamerotation_queues"
LOGS = ROOT / "outputs" / "contextual_causal" / "canonical_full_logs"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=18)
    args = ap.parse_args()
    games = pd.read_csv(SOURCE, dtype={"game_id": str})
    QUEUES.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)
    work = []
    for season, part in games.groupby("season_year"):
        queue = QUEUES / f"games_{int(season)}.csv"
        part[["game_id"]].to_csv(queue, index=False)
        work.append((int(season), queue, len(part)))

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    env["NBA_STATS_CACHE_ONLY"] = "1"

    def run(item: tuple[int, Path, int]) -> dict[str, object]:
        season, queue, target = item
        tag = f"github_rotation_{season}"
        cmd = [sys.executable, str(ROOT / "scripts" / "build_canonical_replay.py"),
               "--game-file", str(queue), "--tag", tag,
               "--gamerotation-only", "--gamerotation-dir", str(CACHE), "--resume"]
        log_path = LOGS / f"github_rotation_{season}.log"
        with log_path.open("a", encoding="utf-8") as log:
            result = subprocess.run(cmd, cwd=ROOT, env=env, stdout=log,
                                    stderr=subprocess.STDOUT)
        audit_path = REBUILD / f"audit_{tag}.csv"
        audit = pd.read_csv(audit_path) if audit_path.exists() else pd.DataFrame()
        accepted = int(audit.get("accepted", pd.Series(dtype=bool)).astype(str)
                       .str.lower().eq("true").sum())
        return {"season": season, "target": target, "processed": len(audit),
                "accepted": accepted, "returncode": result.returncode}

    rows = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run, item) for item in work]
        for future in as_completed(futures):
            row = future.result()
            rows.append(row)
            print(row, flush=True)
            pd.DataFrame(rows).sort_values("season").to_csv(
                REBUILD / "github_gamerotation_repair_progress.csv", index=False)


if __name__ == "__main__":
    main()
