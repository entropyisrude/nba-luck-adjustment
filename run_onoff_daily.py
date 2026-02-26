import argparse
import subprocess
import sys
from datetime import datetime, timedelta


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default="",
        help="Date to process in YYYY-MM-DD (default: yesterday)",
    )
    parser.add_argument(
        "--recompute-existing",
        action="store_true",
        help="Force recompute if date already exists in adjusted_onoff.csv",
    )
    parser.add_argument(
        "--history-season-start",
        default="2025-10-01",
        help="Season start date for history output (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--history-season-end",
        default="2026-06-30",
        help="Season end date for history output (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    if args.date:
        run_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        run_date = datetime.now().date() - timedelta(days=1)

    cmd = [
        sys.executable,
        "run_onoff.py",
        "--start",
        run_date.isoformat(),
        "--end",
        run_date.isoformat(),
        "--history-season-start",
        args.history_season_start,
        "--history-season-end",
        args.history_season_end,
    ]
    if args.recompute_existing:
        cmd.append("--recompute-existing")

    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
