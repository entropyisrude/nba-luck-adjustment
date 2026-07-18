from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CONFIG_PATH = ROOT / "config.yaml"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import src.ingest as ingest_module
from src.onoff import compute_adjusted_onoff_for_game
from scripts.run_targeted_teamseason_repairs import game_list_for_team_season


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe which queued team-seasons are runnable with the current local repair path.")
    parser.add_argument("--queue-csv", required=True, help="Ranked queue CSV")
    parser.add_argument("--output", default=str(DATA_DIR / "teamseason_repair_support_probe.csv"))
    parser.add_argument("--limit", type=int, default=25, help="How many queue rows to probe")
    parser.add_argument("--sample-games", type=int, default=3, help="How many games to probe per team-season")
    parser.add_argument("--pbp-dir", default=str(DATA_DIR / "historical_pbp"))
    parser.add_argument("--state-in", default=str(DATA_DIR / "player_state_historical_pbp.csv"))
    parser.add_argument("--stats-cache-only", action="store_true")
    parser.add_argument("--use-game-rotation", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.stats_cache_only:
        os.environ["NBA_STATS_CACHE_ONLY"] = "1"

    ingest_module.LOCAL_PBP_DIR = Path(args.pbp_dir)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    player_state = pd.read_csv(args.state_in)
    queue_df = pd.read_csv(args.queue_csv).head(args.limit)

    rows: list[dict] = []
    for _, row in queue_df.iterrows():
        season = str(row["season"])
        team_id = int(row["team_id"])
        team_abbr = str(row["team_abbr"])
        games = game_list_for_team_season(team_id=team_id, season=season).head(args.sample_games)
        sample_total = 0
        sample_success = 0
        sample_nonempty = 0
        sample_errors = 0
        sample_notes: list[str] = []

        for _, g in games.iterrows():
            gid = str(g["game_id"]).lstrip("0")
            iso_date = str(g["date"])
            mm, dd, yyyy = iso_date[5:7], iso_date[8:10], iso_date[0:4]
            sample_total += 1
            try:
                onoff_df, stints_df, poss_df = compute_adjusted_onoff_for_game(
                    game_id=gid,
                    game_date_mmddyyyy=f"{mm}/{dd}/{yyyy}",
                    player_state=player_state,
                    orb_rate=float(cfg["orb_rate"]),
                    ppp=float(cfg["ppp"]),
                    use_game_rotation=bool(args.use_game_rotation),
                )
                sample_success += 1
                if not onoff_df.empty:
                    sample_nonempty += 1
                    sample_notes.append(f"{gid}:rows={len(onoff_df)}")
                else:
                    sample_notes.append(f"{gid}:empty")
            except Exception as exc:
                sample_errors += 1
                sample_notes.append(f"{gid}:err={type(exc).__name__}")

        if sample_nonempty > 0:
            support = "repairable_now"
        elif sample_success > 0:
            support = "runner_returns_empty"
        elif sample_errors > 0:
            support = "runner_errors"
        else:
            support = "no_games"

        out_row = row.to_dict()
        out_row.update(
            {
                "sample_games_tested": sample_total,
                "sample_success": sample_success,
                "sample_nonempty": sample_nonempty,
                "sample_errors": sample_errors,
                "support_probe_status": support,
                "support_probe_notes": " | ".join(sample_notes),
            }
        )
        rows.append(out_row)
        print(f"{season} {team_abbr}: {support} ({sample_nonempty}/{sample_total} nonempty)", flush=True)

    out_df = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Wrote {out_path} ({len(out_df):,} rows)", flush=True)


if __name__ == "__main__":
    main()
