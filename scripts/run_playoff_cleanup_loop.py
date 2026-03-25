from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
AUDIT_DIR = DATA_DIR / "audits"
DB_PATH = DATA_DIR / "nba_analytics_playoffs.duckdb"
PYTHON = Path(sys.executable)


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def missing_total() -> int:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    value = int(con.execute("select count(*) from player_game_facts where on_possessions is null").fetchone()[0])
    con.close()
    return value


def select_parser_games(batch_size: int) -> list[str]:
    path = AUDIT_DIR / "playoff_missing_on_possessions_by_game.csv"
    df = pd.read_csv(path, dtype={"game_id": str})
    df = df[df["parser_lineup_miss_rows"] > 0].copy()
    return df["game_id"].astype(str).head(batch_size).tolist()


def select_games_by_missing_minutes(batch_size: int) -> list[str]:
    detail_path = AUDIT_DIR / "playoff_missing_on_possessions_detail.csv"
    df = pd.read_csv(detail_path, dtype={"game_id": str})
    df = df[df["cause"] == "parser_lineup_miss"].copy()
    df["minutes"] = pd.to_numeric(df["minutes"], errors="coerce").fillna(0.0)
    by_game = (
        df.groupby("game_id", as_index=False)
        .agg(
            missing_minutes=("minutes", "sum"),
            missing_rows=("game_id", "size"),
        )
        .sort_values(["missing_minutes", "missing_rows", "game_id"], ascending=[False, False, True])
    )
    return by_game["game_id"].astype(str).head(batch_size).tolist()


def select_games_by_defense_only(batch_size: int) -> list[str]:
    path = AUDIT_DIR / "playoff_missing_on_possessions_by_game.csv"
    df = pd.read_csv(path, dtype={"game_id": str})
    df = df[df["defense_only_rows"] > 0].copy()
    df = df.sort_values(["defense_only_rows", "missing_rows", "game_id"], ascending=[False, False, True])
    return df["game_id"].astype(str).head(batch_size).tolist()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run repeated playoff possession cleanup batches.")
    parser.add_argument("--max-batches", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--regenerate-pages-every", type=int, default=5)
    parser.add_argument("--selection-mode", choices=["rows", "minutes", "defense_only"], default="rows")
    args = parser.parse_args()

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    progress_rows: list[dict[str, int]] = []

    for batch in range(1, args.max_batches + 1):
        run([str(PYTHON), str(ROOT / "scripts" / "audit_playoff_missing_possessions.py")])
        before = missing_total()
        if args.selection_mode == "minutes":
            game_ids = select_games_by_missing_minutes(args.batch_size)
        elif args.selection_mode == "defense_only":
            game_ids = select_games_by_defense_only(args.batch_size)
        else:
            game_ids = select_parser_games(args.batch_size)
        if not game_ids:
            print(f"batch {batch}: no candidate games left for mode={args.selection_mode}; stopping", flush=True)
            break

        cmd = [str(PYTHON), str(ROOT / "scripts" / "backfill_selected_playoff_possessions.py")]
        for gid in game_ids:
            cmd.extend(["--game-id", gid])
        run(cmd)
        run([str(PYTHON), str(ROOT / "scripts" / "build_playoff_analytics_duckdb.py")])
        run([str(PYTHON), str(ROOT / "scripts" / "audit_playoff_missing_possessions.py")])
        after = missing_total()

        progress_rows.append({
            "batch": batch,
            "games_attempted": len(game_ids),
            "missing_before": before,
            "missing_after": after,
            "delta": before - after,
        })
        pd.DataFrame(progress_rows).to_csv(AUDIT_DIR / "playoff_cleanup_progress.csv", index=False)
        print(
            f"batch {batch}: mode={args.selection_mode} before={before} after={after} delta={before - after} games={len(game_ids)}",
            flush=True,
        )

        if batch % args.regenerate_pages_every == 0:
            run([str(PYTHON), str(ROOT / "generate_player_game_search_playoffs.py")])
            run([str(PYTHON), str(ROOT / "generate_player_span_search_playoffs.py")])

        if after >= before:
            print(f"batch {batch}: no improvement; stopping", flush=True)
            break

    run([str(PYTHON), str(ROOT / "generate_player_game_search_playoffs.py")])
    run([str(PYTHON), str(ROOT / "generate_player_span_search_playoffs.py")])


if __name__ == "__main__":
    main()
