from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
AUDIT_CSV = DATA_DIR / "audits" / "player_game_missing_on_possessions_by_game.csv"
ONOFF_HIST = DATA_DIR / "adjusted_onoff_historical_pbp.csv"
STINTS_HIST = DATA_DIR / "stints_historical_pbp.csv"
POSSESSIONS_HIST = DATA_DIR / "possessions_historical_pbp.csv"

PYTHON = Path("/tmp/nba-onoff-publish-linux/.venv/bin/python")
BACKFILL_SCRIPT = ROOT / "scripts" / "backfill_selected_historical_games.py"
BUILD_DB_SCRIPT = ROOT / "scripts" / "build_analytics_duckdb.py"
AUDIT_SCRIPT = ROOT / "scripts" / "audit_player_game_data.py"
REPORT_SCRIPT = ROOT / "generate_player_game_search_report.py"
PLAYER_STATE = DATA_DIR / "player_state_historical_pbp.csv"
PBP_DIR = DATA_DIR / "historical_pbp"


def run(cmd: list[str], env: dict[str, str] | None = None) -> None:
    subprocess.run(cmd, check=True, env=env)


def merge_replace(target: Path, add_path: Path) -> int:
    base = pd.read_csv(target)
    add = pd.read_csv(add_path)
    gids = sorted(add["game_id"].astype(str).str.lstrip("0").unique().tolist())
    base = base.loc[~base["game_id"].astype(str).str.lstrip("0").isin(gids)].copy()
    merged = pd.concat([base, add], ignore_index=True, sort=False)
    sort_cols = [c for c in ["date", "game_id"] if c in merged.columns]
    if sort_cols:
        merged = merged.sort_values(sort_cols, kind="stable")
    merged.to_csv(target, index=False)
    return len(gids)


def choose_games(limit: int, max_missing_rows: int | None) -> pd.DataFrame:
    df = pd.read_csv(AUDIT_CSV)
    if max_missing_rows is not None:
        df = df.loc[df["missing_rows"] <= max_missing_rows].copy()
    return df.head(limit)


def main() -> None:
    parser = argparse.ArgumentParser(description="Repeatedly backfill top missing-possession games and rerun audits.")
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--max-missing-rows", type=int, default=6)
    args = parser.parse_args()

    for i in range(1, args.iterations + 1):
        batch = choose_games(args.batch_size, args.max_missing_rows)
        if batch.empty:
            print(f"ITER {i}: no matching games left")
            break

        print(f"ITER {i}: selected {len(batch)} games", flush=True)
        with tempfile.TemporaryDirectory(prefix=f"backfill_iter_{i}_") as tmpdir:
            tmp = Path(tmpdir)
            game_list = tmp / "games.csv"
            onoff_out = tmp / "onoff.csv"
            stints_out = tmp / "stints.csv"
            poss_out = tmp / "poss.csv"

            batch.loc[:, ["game_id", "date"]].to_csv(game_list, index=False)

            env = dict(os.environ)
            env["PYTHONPATH"] = str(ROOT)
            run(
                [
                    str(PYTHON),
                    str(BACKFILL_SCRIPT),
                    "--game-list",
                    str(game_list),
                    "--pbp-dir",
                    str(PBP_DIR),
                    "--state-in",
                    str(PLAYER_STATE),
                    "--onoff-out",
                    str(onoff_out),
                    "--stints-out",
                    str(stints_out),
                    "--possessions-out",
                    str(poss_out),
                    "--stats-cache-only",
                ],
                env=env,
            )

            merged_games = 0
            if onoff_out.exists() and onoff_out.stat().st_size > 0:
                merged_games = merge_replace(ONOFF_HIST, onoff_out)
            if stints_out.exists() and stints_out.stat().st_size > 0:
                merge_replace(STINTS_HIST, stints_out)
            if poss_out.exists() and poss_out.stat().st_size > 0:
                merge_replace(POSSESSIONS_HIST, poss_out)

            print(f"ITER {i}: merged {merged_games} games", flush=True)

        run([str(PYTHON), str(BUILD_DB_SCRIPT)])
        run([str(PYTHON), str(AUDIT_SCRIPT)])
        run([str(PYTHON), str(REPORT_SCRIPT)])

        audit_df = pd.read_csv(AUDIT_CSV)
        total_missing = 0 if audit_df.empty else int((audit_df["missing_rows"]).sum())
        print(f"ITER {i}: remaining missing_rows sum={total_missing}", flush=True)


if __name__ == "__main__":
    main()
