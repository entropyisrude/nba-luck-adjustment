import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

import src.ingest as ingest_module
from src.ingest import _load_stats_boxscore, _load_stats_gamerotation, _load_stats_home_away


def daterange(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def load_game_dates(path: Path) -> dict[str, str]:
    df = pd.read_csv(path, dtype={"game_id": str}, low_memory=False)
    df["game_id"] = df["game_id"].astype(str).str.lstrip("0")
    if "date" not in df.columns:
        return {}
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()
    df = df.sort_values(["game_id", "date"]).drop_duplicates(subset=["game_id"], keep="last")
    return {row["game_id"]: row["date"].date().isoformat() for _, row in df.iterrows()}


def _prefetch_one_game(game_id: str) -> tuple[str, str | None]:
    gid = str(game_id).zfill(10)
    try:
        _load_stats_home_away(gid)
        _load_stats_boxscore(gid)
        _load_stats_gamerotation(gid)
        return str(game_id), None
    except Exception as exc:
        return str(game_id), repr(exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prefetch boxscore/summary/gamerotation stats data into local cache.")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--starter-overrides-path", default="data/stints_historical.csv", help="CSV with authoritative game_id/date mapping")
    parser.add_argument("--pbp-dir", default=None, help="Optional local PBP dir to set on ingest module for consistency")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent fetch workers")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    if args.pbp_dir:
        ingest_module.LOCAL_PBP_DIR = Path(args.pbp_dir)

    game_date_map = load_game_dates(Path(args.starter_overrides_path))
    by_date: dict[str, list[str]] = {}
    for game_id, iso_date in game_date_map.items():
        if start.isoformat() <= iso_date <= end.isoformat():
            by_date.setdefault(iso_date, []).append(game_id)

    fetched = 0
    failed = 0
    for iso_date in sorted(by_date):
        game_ids = sorted(by_date[iso_date])
        print("DATE", iso_date, "GAMES", len(game_ids))
        if int(args.workers) <= 1:
            for game_id in game_ids:
                _, err = _prefetch_one_game(game_id)
                if err is None:
                    fetched += 1
                else:
                    failed += 1
                    print("ERROR prefetching", game_id, "->", err)
            continue
        with ThreadPoolExecutor(max_workers=int(args.workers)) as executor:
            futures = {executor.submit(_prefetch_one_game, game_id): game_id for game_id in game_ids}
            for future in as_completed(futures):
                game_id = futures[future]
                gid_out, err = future.result()
                if err is None:
                    fetched += 1
                else:
                    failed += 1
                    print("ERROR prefetching", gid_out, "->", err)
    print({"fetched": fetched, "failed": failed})


if __name__ == "__main__":
    main()
