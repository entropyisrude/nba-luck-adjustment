from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

import pandas as pd
import yaml

import src.ingest as ingest_module
from src.onoff import compute_adjusted_onoff_for_game


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
CONFIG_PATH = ROOT / "config.yaml"


def append_csv(path: Path, df: pd.DataFrame) -> None:
    if df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    df.to_csv(path, mode="a", header=write_header, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill a selected list of historical regular-season games.")
    parser.add_argument("--game-list", required=True, help="CSV with columns game_id,date(YYYY-MM-DD)")
    parser.add_argument("--pbp-dir", default="data/historical_pbp")
    parser.add_argument("--state-in", default="data/player_state_historical_pbp.csv")
    parser.add_argument("--onoff-out", required=True)
    parser.add_argument("--stints-out", required=True)
    parser.add_argument("--possessions-out", required=True)
    parser.add_argument("--stats-cache-only", action="store_true")
    parser.add_argument("--use-game-rotation", action="store_true")
    args = parser.parse_args()

    if args.stats_cache_only:
        os.environ["NBA_STATS_CACHE_ONLY"] = "1"

    ingest_module.LOCAL_PBP_DIR = Path(args.pbp_dir)

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    player_state = pd.read_csv(args.state_in)

    game_rows: list[tuple[str, str]] = []
    with open(args.game_list, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = str(row.get("game_id") or "").lstrip("0")
            date = str(row.get("date") or "")
            if gid and date:
                game_rows.append((gid, date))

    for gid, iso_date in game_rows:
        mm, dd, yyyy = iso_date[5:7], iso_date[8:10], iso_date[0:4]
        game_date_mmddyyyy = f"{mm}/{dd}/{yyyy}"
        print(f"PROCESS {gid} {iso_date}", flush=True)
        try:
            onoff_df, stints_df, poss_df = compute_adjusted_onoff_for_game(
                game_id=gid,
                game_date_mmddyyyy=game_date_mmddyyyy,
                player_state=player_state,
                orb_rate=float(cfg["orb_rate"]),
                ppp=float(cfg["ppp"]),
                use_game_rotation=bool(args.use_game_rotation),
            )
            if onoff_df.empty:
                print(f"SKIP {gid} no on/off rows", flush=True)
                continue
            onoff_df["date"] = iso_date
            if not stints_df.empty:
                stints_df["date"] = iso_date
            if not poss_df.empty:
                poss_df["date"] = iso_date
            append_csv(Path(args.onoff_out), onoff_df)
            append_csv(Path(args.stints_out), stints_df)
            append_csv(Path(args.possessions_out), poss_df)
            print(
                f"WROTE {gid} onoff={len(onoff_df)} stints={len(stints_df)} poss={len(poss_df)}",
                flush=True,
            )
        except Exception as exc:
            print(f"ERROR {gid} -> {exc!r}", flush=True)


if __name__ == "__main__":
    main()
