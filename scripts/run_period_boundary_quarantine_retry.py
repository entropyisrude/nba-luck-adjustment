"""Retry first-pass quarantines with period-aware canonical boundary logic."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from pathlib import Path
import subprocess
import sys
import time

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
QA = ROOT / "outputs" / "contextual_causal" / "canonical_game_integrity.parquet"
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
QUEUES = REBUILD / "period_boundary_retry_queues"
LOGS = ROOT / "outputs" / "contextual_causal" / "canonical_full_logs"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=18)
    ap.add_argument("--solver-seconds", type=float, default=2.0)
    ap.add_argument("--wait", action="store_true",
                    help="wait for the first-pass workers to finish")
    args = ap.parse_args()
    qa = pd.read_parquet(QA)
    targets = (qa.loc[~qa.canonical_grade_a]
               .groupby("season_year").game_id.nunique().astype(int))
    QUEUES.mkdir(parents=True, exist_ok=True); LOGS.mkdir(parents=True, exist_ok=True)
    while True:
        queues: list[tuple[int, Path, int]] = []; incomplete = []
        for season, target in targets.items():
            audit_path = REBUILD / f"audit_full_{int(season)}.csv"
            if not audit_path.exists():
                incomplete.append((int(season), 0, int(target))); continue
            audit = pd.read_csv(audit_path)
            if audit.game_id.nunique() < target:
                incomplete.append((int(season), audit.game_id.nunique(), int(target)))
                continue
            rejected = audit.loc[audit.accepted != True, ["game_id"]].drop_duplicates()
            if rejected.empty: continue
            queue = QUEUES / f"games_{int(season)}.csv"
            rejected.to_csv(queue, index=False)
            queues.append((int(season), queue, len(rejected)))
        if not incomplete: break
        if not args.wait: raise SystemExit(f"first pass incomplete: {incomplete}")
        print(f"waiting for first pass: {incomplete}", flush=True)
        time.sleep(30)

    env = os.environ.copy(); env["PYTHONPATH"] = str(ROOT)
    def run(item: tuple[int, Path, int]) -> dict:
        season, queue, target = item
        tag = f"retry_period_{season}"
        cmd = [sys.executable, str(ROOT / "scripts" / "build_canonical_replay.py"),
               "--game-file", str(queue), "--tag", tag,
               "--solver-seconds", str(args.solver_seconds), "--resume"]
        log_path = LOGS / f"retry_period_{season}.log"
        with log_path.open("a", encoding="utf-8") as log:
            p = subprocess.run(cmd, cwd=ROOT, env=env, stdout=log,
                               stderr=subprocess.STDOUT)
        result_path = REBUILD / f"audit_{tag}.csv"
        result = pd.read_csv(result_path) if result_path.exists() else pd.DataFrame()
        accepted = int((result.get("accepted", pd.Series(dtype=bool)) == True).sum())
        return {"season": season, "target": target, "processed": len(result),
                "accepted": accepted, "returncode": p.returncode}

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run, item) for item in queues]
        for future in as_completed(futures):
            row = future.result(); results.append(row); print(row, flush=True)
            pd.DataFrame(results).sort_values("season").to_csv(
                LOGS / "retry_period_progress.csv", index=False)


if __name__ == "__main__":
    main()
