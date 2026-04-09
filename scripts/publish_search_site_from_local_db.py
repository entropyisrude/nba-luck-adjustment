from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"


def run_script(script_name: str, env: dict[str, str]) -> None:
    script_path = ROOT / script_name
    subprocess.run([sys.executable, str(script_path)], check=True, env=env, cwd=str(ROOT))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish regular-season search site assets from a local canonical DuckDB without committing the DB."
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to the local canonical nba_analytics DuckDB used for site generation.",
    )
    parser.add_argument(
        "--source-label",
        default="local canonical regular-season DB",
        help="Human-readable label shown on generated pages.",
    )
    parser.add_argument(
        "--skip-game-search",
        action="store_true",
        help="Only rebuild the player-span search assets.",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    env = os.environ.copy()
    env["NBA_ONOFF_ROOT"] = str(ROOT)
    env["NBA_ANALYTICS_DB_PATH"] = str(db_path)
    env["PLAYER_GAME_SEARCH_SOURCE_LABEL"] = args.source_label
    env["PLAYER_SPAN_SEARCH_SOURCE_LABEL"] = args.source_label

    if not args.skip_game_search:
        run_script("generate_player_game_search_report.py", env)
    run_script("generate_player_span_search_report.py", env)

    print("Published regular-season search assets from local DB:")
    print(f"  DB: {db_path}")
    if not args.skip_game_search:
        print(f"  HTML: {ROOT / 'game-search.html'}")
        print(f"  Chunks: {DATA_DIR / 'player_game_chunks'}")
    print(f"  HTML: {ROOT / 'player-span-search.html'}")
    print(f"  Chunks: {DATA_DIR / 'player_span_chunks'}")
    print("Commit the generated HTML/chunk assets only. Do not commit the DuckDB.")


if __name__ == "__main__":
    main()
