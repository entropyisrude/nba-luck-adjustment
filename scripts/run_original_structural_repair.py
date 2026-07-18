"""Rebuild original Grade-A games rejected by the independent structural audit."""
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
SOURCE = REBUILD / "remaining_quarantine_queues" / "original_structural_failures.csv"
QA = ROOT / "outputs" / "contextual_causal" / "canonical_game_integrity.parquet"
QUEUES = REBUILD / "original_structural_queues"
LOGS = ROOT / "outputs" / "contextual_causal" / "canonical_full_logs"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=18)
    ap.add_argument("--solver-seconds", type=float, default=2.0)
    args = ap.parse_args()
    games = pd.read_csv(SOURCE, dtype={"game_id": str})
    games["game_id"] = games.game_id.str.lstrip("0")
    qa = pd.read_parquet(QA, columns=["game_id", "season_year"])
    qa["game_id"] = qa.game_id.astype(str).str.lstrip("0")
    games = games.merge(qa.drop_duplicates("game_id"), on="game_id", how="left")
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
        tag = f"structural_{season}"
        cmd = [sys.executable, str(ROOT / "scripts" / "build_canonical_replay.py"),
               "--game-file", str(queue), "--tag", tag,
               "--solver-seconds", str(args.solver_seconds), "--resume"]
        log_path = LOGS / f"structural_{season}.log"
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
                REBUILD / "original_structural_repair_progress.csv", index=False)


if __name__ == "__main__":
    main()
