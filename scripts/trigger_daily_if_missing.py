"""Desktop backup trigger for the daily GitHub update.

GitHub's scheduler drops or delays the workflow's cron runs (Oct 2026: whole
days with no scheduled run). A Windows scheduled task on the desktop runs this
each morning: if no "Auto-update" commit for yesterday's games has landed today
and no daily run is in progress, it dispatches the workflow. Idempotent, so it
is safe to run several times a morning. Logs to data/../trigger_daily.log.

Usage: python scripts/trigger_daily_if_missing.py
"""
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = "entropyisrude/nba-luck-adjustment"
WORKFLOW = "update-daily.yml"
GH = r"C:\Program Files\GitHub CLI\gh.exe"
LOG = Path(__file__).resolve().parents[1] / "trigger_daily.log"
SEASON_MONTHS = {10, 11, 12, 1, 2, 3, 4, 5, 6}  # same months as the workflow cron


def gh(*args: str) -> str:
    return subprocess.run([GH, *args], capture_output=True, text=True, check=True).stdout


def log(msg: str) -> None:
    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} {msg}"
    print(line)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def main() -> None:
    now = datetime.now(timezone.utc)
    if now.month not in SEASON_MONTHS:
        log("offseason; nothing to do")
        return
    # The workflow's default date is yesterday in UTC; match it.
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    today_start = now.strftime("%Y-%m-%dT00:00:00Z")

    commits = json.loads(gh("api", f"repos/{REPO}/commits?since={today_start}&per_page=100"))
    if any("Auto-update" in c["commit"]["message"] and yesterday in c["commit"]["message"] for c in commits):
        log(f"already updated for {yesterday}")
        return

    runs = json.loads(gh("run", "list", "-R", REPO, "--workflow", WORKFLOW, "-L", "5",
                         "--json", "status,createdAt"))
    if any(r["status"] in ("queued", "in_progress", "waiting", "pending") for r in runs):
        log("a daily run is already in progress")
        return

    gh("workflow", "run", WORKFLOW, "-R", REPO)
    log(f"no update for {yesterday} yet; dispatched {WORKFLOW}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # keep the scheduled task's log useful
        log(f"ERROR {type(exc).__name__}: {exc}")
        raise
