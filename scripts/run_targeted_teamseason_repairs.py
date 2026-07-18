from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
BACKFILL_SCRIPT = ROOT / "scripts" / "backfill_selected_historical_games.py"
TEAM_SEASONS = [
    ("2023-24", 1610612745, "HOU"),
    ("2023-24", 1610612760, "OKC"),
    ("2022-23", 1610612739, "CLE"),
    ("2021-22", 1610612759, "SAS"),
    ("2022-23", 1610612753, "ORL"),
]


def season_window(season: str) -> tuple[str, str]:
    start_year = int(season[:4])
    end_year = start_year + 1
    return f"{start_year}-10-01", f"{end_year}-06-30"


def game_list_for_team_season(team_id: int, season: str) -> pd.DataFrame:
    con = duckdb.connect()
    hist_sources = [
        DATA_DIR / "adjusted_onoff_historical_pbp.csv",
        DATA_DIR / "adjusted_onoff_historical_rebuilt.csv",
    ]
    meta_sources = [
        DATA_DIR / "historical_game_metadata_cache.csv",
        DATA_DIR / "game_metadata_external_2010_2024.csv",
        DATA_DIR / "game_metadata_kaggle_traditional.csv",
    ]
    hist_unions: list[str] = []
    for path in hist_sources:
        if path.exists():
            hist_unions.append(
                f"""
                SELECT
                  CAST(game_id AS VARCHAR) AS game_id,
                  CAST(date AS DATE) AS game_date,
                  CAST(team_id AS BIGINT) AS team_id
                FROM read_csv_auto('{path}', header=true, sample_size=-1, delim=',', quote='\"', strict_mode=false, ignore_errors=true, null_padding=true)
                WHERE TRY_CAST(date AS DATE) IS NOT NULL
                  AND CAST(team_id AS BIGINT) = {team_id}
                """
            )
    unions: list[str] = []
    for path in meta_sources:
        if path.exists():
            unions.append(
                f"""
                SELECT
                  CAST(game_id AS VARCHAR) AS game_id,
                  CAST(date AS DATE) AS game_date,
                  CAST(home_team_id AS BIGINT) AS home_team_id,
                  CAST(away_team_id AS BIGINT) AS away_team_id
                FROM read_csv_auto('{path}', header=true, sample_size=-1, delim=',', quote='\"', strict_mode=false, ignore_errors=true, null_padding=true)
                WHERE TRY_CAST(date AS DATE) IS NOT NULL
                """
            )
    if not unions and not hist_unions:
        raise RuntimeError("No game-list sources found")
    season_start, season_end = season_window(season)
    q = f"""
    WITH hist_games AS (
      {" UNION ALL ".join(hist_unions) if hist_unions else "SELECT NULL::VARCHAR AS game_id, NULL::DATE AS game_date, NULL::BIGINT AS team_id WHERE FALSE"}
    ),
    all_games AS (
      {" UNION ALL ".join(unions)}
    ),
    deduped AS (
      SELECT *
      FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY game_date DESC) AS rn
        FROM all_games
      )
      WHERE rn = 1
    ),
    meta_games AS (
      SELECT
        game_id,
        game_date
      FROM deduped
      WHERE game_date BETWEEN DATE '{season_start}' AND DATE '{season_end}'
        AND ({team_id} IN (home_team_id, away_team_id))
    ),
    combined AS (
      SELECT game_id, game_date
      FROM hist_games
      WHERE game_date BETWEEN DATE '{season_start}' AND DATE '{season_end}'
      UNION
      SELECT game_id, game_date
      FROM meta_games
    )
    SELECT
      game_id,
      STRFTIME(game_date, '%Y-%m-%d') AS date
    FROM combined
    ORDER BY game_date, game_id
    """
    return con.execute(q).fetchdf()


def run_one(team_id: int, season: str, team_abbr: str, pbp_dir: str, state_in: str, use_stats_cache_only: bool, use_game_rotation: bool) -> int:
    games = game_list_for_team_season(team_id, season)
    if games.empty:
        print(f"SKIP {team_abbr} {season}: no games found", flush=True)
        return 0
    with tempfile.TemporaryDirectory(prefix=f"repair_{team_abbr}_{season}_") as tmpdir:
        tmp = Path(tmpdir)
        game_list = tmp / "games.csv"
        games.to_csv(game_list, index=False)
        cmd = [
            sys.executable,
            str(BACKFILL_SCRIPT),
            "--game-list",
            str(game_list),
            "--pbp-dir",
            pbp_dir,
            "--state-in",
            state_in,
            "--onoff-out",
            str(DATA_DIR / "adjusted_onoff_historical_rebuilt.csv"),
            "--stints-out",
            str(DATA_DIR / "stints_historical_rebuilt.csv"),
            "--possessions-out",
            str(DATA_DIR / "possessions_historical_rebuilt.csv"),
        ]
        if use_stats_cache_only:
            cmd.append("--stats-cache-only")
        if use_game_rotation:
            cmd.append("--use-game-rotation")
        print(f"RUN {team_abbr} {season}: {len(games)} games", flush=True)
        env = dict(os.environ)
        env["PYTHONPATH"] = str(ROOT)
        subprocess.run(cmd, check=False, cwd=str(ROOT), env=env)
        return len(games)


def load_queue(queue_csv: Path, limit: int = 0, repair_priority: str | None = None) -> list[tuple[str, int, str]]:
    df = pd.read_csv(queue_csv)
    need = {"season", "team_id", "team_abbr"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"Queue CSV missing required columns: {sorted(missing)}")
    if repair_priority and "repair_priority" in df.columns:
        df = df.loc[df["repair_priority"] == repair_priority]
    triples = []
    for _, row in df.iterrows():
        triples.append((str(row["season"]), int(row["team_id"]), str(row["team_abbr"])))
    seen: set[tuple[str, int, str]] = set()
    ordered: list[tuple[str, int, str]] = []
    for item in triples:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    if limit and limit > 0:
        ordered = ordered[:limit]
    return ordered


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill selected bad historical team-seasons into rebuilt overlay files.")
    parser.add_argument("--pbp-dir", default=str(DATA_DIR / "historical_pbp"))
    parser.add_argument("--state-in", default=str(DATA_DIR / "player_state_historical_pbp.csv"))
    parser.add_argument("--stats-cache-only", action="store_true")
    parser.add_argument("--use-game-rotation", action="store_true")
    parser.add_argument("--queue-csv", default="", help="Optional ranked queue CSV with season,team_id,team_abbr columns")
    parser.add_argument("--limit", type=int, default=0, help="Optional limit when reading queue CSV")
    parser.add_argument("--repair-priority", default="", help="Optional repair_priority filter when reading queue CSV")
    args = parser.parse_args()

    team_seasons = TEAM_SEASONS
    if args.queue_csv:
        team_seasons = load_queue(
            queue_csv=Path(args.queue_csv),
            limit=args.limit,
            repair_priority=args.repair_priority or None,
        )

    total = 0
    for season, team_id, team_abbr in team_seasons:
        total += run_one(
            team_id=team_id,
            season=season,
            team_abbr=team_abbr,
            pbp_dir=args.pbp_dir,
            state_in=args.state_in,
            use_stats_cache_only=args.stats_cache_only,
            use_game_rotation=args.use_game_rotation,
        )
    print(f"TOTAL_GAMES_QUEUED {total}", flush=True)


if __name__ == "__main__":
    main()
