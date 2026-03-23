from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
AUDIT_DIR = DATA_DIR / "audits"
LOG_DIR = ROOT / "logs"
PYTHON = Path(sys.executable)


def latest_missing_total() -> int | None:
    path = AUDIT_DIR / "playoff_cleanup_progress.csv"
    if not path.exists():
        return None
    try:
        with path.open(newline="", encoding="utf-8", errors="ignore") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            return None
        return int(rows[-1]["missing_after"])
    except Exception:
        return None


def append_log(message: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with (LOG_DIR / "playoff_cleanup_forever.log").open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat(timespec='seconds')} {message}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Continuously relaunch playoff cleanup blocks.")
    parser.add_argument("--block-batches", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--regenerate-pages-every", type=int, default=5)
    parser.add_argument("--sleep-seconds", type=int, default=5)
    parser.add_argument(
        "--mode-cycle",
        nargs="+",
        default=["minutes", "rows", "defense_only"],
        choices=["minutes", "rows", "defense_only"],
    )
    args = parser.parse_args()

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    block = 0
    while True:
        mode = args.mode_cycle[block % len(args.mode_cycle)]
        before = latest_missing_total()
        append_log(f"starting block={block + 1} mode={mode} missing_before={before}")
        cmd = [
            str(PYTHON),
            str(ROOT / "scripts" / "run_playoff_cleanup_loop.py"),
            "--max-batches",
            str(args.block_batches),
            "--batch-size",
            str(args.batch_size),
            "--regenerate-pages-every",
            str(args.regenerate_pages_every),
            "--selection-mode",
            mode,
        ]
        proc = subprocess.run(cmd)
        after = latest_missing_total()
        append_log(
            f"finished block={block + 1} mode={mode} returncode={proc.returncode} missing_after={after}"
        )
        block += 1
        time.sleep(args.sleep_seconds)


if __name__ == "__main__":
    main()
