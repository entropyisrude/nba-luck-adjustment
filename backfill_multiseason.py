"""Run multi-season on/off backfill in monthly chunks."""

from __future__ import annotations

import argparse
import subprocess
from datetime import date, timedelta


def month_end(d: date) -> date:
    if d.month == 12:
        return date(d.year, 12, 31)
    first_next = date(d.year, d.month + 1, 1)
    return first_next - timedelta(days=1)


def daterange_months(start: date, end: date):
    cur = date(start.year, start.month, 1)
    if start.day != 1:
        cur = start
    while cur <= end:
        chunk_start = cur
        chunk_end = min(month_end(cur), end)
        yield chunk_start, chunk_end
        cur = chunk_end + timedelta(days=1)


def run(cmd: list[str]) -> None:
    print("RUN:", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--final-history-start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--final-history-end", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    # Fast pass: append game-level rows only.
    for chunk_start, chunk_end in daterange_months(start, end):
        run(
            [
                "python",
                "run_onoff.py",
                "--start",
                chunk_start.isoformat(),
                "--end",
                chunk_end.isoformat(),
                "--skip-history",
                "--skip-boxscore",
            ]
        )

    # Final rebuild for history + daily boxscore over the full selected window.
    run(
        [
            "python",
            "run_onoff.py",
            "--start",
            start.isoformat(),
            "--end",
            end.isoformat(),
            "--history-season-start",
            args.final_history_start,
            "--history-season-end",
            args.final_history_end,
        ]
    )

    run(["python", "generate_onoff_report.py"])
    run(["python", "generate_onoff_daily_boxscore_report.py"])


if __name__ == "__main__":
    main()
