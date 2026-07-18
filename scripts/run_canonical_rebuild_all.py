"""Run the canonical failed-game rebuild season by season with checkpoints.

This orchestrator writes only versioned derived artifacts.  It is safe to stop
and restart: each season delegates to build_canonical_replay.py --resume.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from pathlib import Path
import subprocess
import sys
import threading

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
AUDIT = ROOT / "outputs" / "contextual_causal" / "canonical_game_integrity.parquet"
OUT = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
LOGS = ROOT / "outputs" / "contextual_causal" / "canonical_full_logs"
LOCK = threading.Lock()


def completed_count(season: int) -> int:
    path = OUT / f"audit_full_{season}.csv"
    if not path.exists():
        return 0
    try:
        return pd.read_csv(path, usecols=["game_id"]).game_id.nunique()
    except Exception:
        return 0


def run_season(season: int, target: int, solver_seconds: float) -> dict:
    before = completed_count(season)
    if before >= target:
        return {"season": season, "target": target, "processed": before,
                "status": "already_complete", "returncode": 0}
    LOGS.mkdir(parents=True, exist_ok=True)
    log_path = LOGS / f"season_{season}.log"
    cmd = [sys.executable, str(ROOT / "scripts" / "build_canonical_replay.py"),
           "--season", str(season), "--failed-only", "--tag", f"full_{season}",
           "--solver-seconds", str(solver_seconds), "--resume"]
    with log_path.open("a", encoding="utf-8") as log:
        log.write(f"\nSTART {' '.join(cmd)}\n"); log.flush()
        env = os.environ.copy()
        prior = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = str(ROOT) + (os.pathsep + prior if prior else "")
        proc = subprocess.run(cmd, cwd=ROOT, env=env, stdout=log,
                              stderr=subprocess.STDOUT)
    after = completed_count(season)
    return {"season": season, "target": target, "processed": after,
            "status": "complete" if after >= target else "incomplete",
            "returncode": proc.returncode}


def write_progress(rows: list[dict], progress_path: Path) -> None:
    with LOCK:
        pd.DataFrame(rows).sort_values("season", ascending=False).to_csv(
            progress_path, index=False)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--solver-seconds", type=float, default=2.0)
    ap.add_argument("--seasons", default="",
                    help="optional comma-separated season start years")
    ap.add_argument("--progress-file", default="progress.csv",
                    help="filename under canonical_full_logs")
    args = ap.parse_args()
    progress_path = LOGS / args.progress_file
    qa = pd.read_parquet(AUDIT)
    failed = qa[~qa.canonical_grade_a]
    targets = failed.groupby("season_year").game_id.nunique().astype(int).to_dict()
    seasons = sorted(targets, reverse=True)
    if args.seasons:
        requested = {int(x) for x in args.seasons.split(",") if x.strip()}
        seasons = [s for s in seasons if s in requested]
    LOGS.mkdir(parents=True, exist_ok=True)
    rows = [{"season": s, "target": targets[s], "processed": completed_count(s),
             "status": "queued", "returncode": None} for s in seasons]
    write_progress(rows, progress_path)
    by_season = {r["season"]: r for r in rows}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        future_map = {pool.submit(run_season, s, targets[s], args.solver_seconds): s
                      for s in seasons}
        for future in as_completed(future_map):
            season = future_map[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {"season": season, "target": targets[season],
                          "processed": completed_count(season), "status": "error",
                          "returncode": None, "error": repr(exc)}
            by_season[season] = result
            write_progress(list(by_season.values()), progress_path)
            print(result, flush=True)


if __name__ == "__main__":
    main()
