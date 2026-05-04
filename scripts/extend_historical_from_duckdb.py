"""
One-time script: extend adjusted_onoff_historical_pbp.csv from the existing DuckDB.

Use when adjusted_onoff.csv is an LFS pointer (unavailable locally), but the DuckDB
already contains the full raw_adjusted_onoff table from a prior weekly workflow run.

Run once; then switch to extend_historical_onoff.py for ongoing weekly extension.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

DB_PATH = DATA_DIR / "nba_analytics.duckdb"
HIST_ADJUSTED_ONOFF = DATA_DIR / "adjusted_onoff_historical_pbp.csv"

REQUIRED_COLS = [
    "game_id", "team_id", "player_id", "player_name",
    "on_pts_for", "on_pts_against", "on_diff",
    "off_pts_for", "off_pts_against", "off_diff",
    "on_pts_for_adj", "on_pts_against_adj", "on_diff_adj",
    "off_pts_for_adj", "off_pts_against_adj", "off_diff_adj",
    "on_off_diff", "on_off_diff_adj",
    "on_diff_reconstructed", "off_diff_reconstructed", "on_off_diff_reconstructed",
    "minutes_on", "date",
]


def main() -> None:
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found.")
        sys.exit(1)
    if not HIST_ADJUSTED_ONOFF.exists():
        print(f"ERROR: {HIST_ADJUSTED_ONOFF} not found.")
        sys.exit(1)

    hist = pd.read_csv(HIST_ADJUSTED_ONOFF, dtype={"game_id": str, "player_id": int})
    hist["game_id"] = hist["game_id"].astype(str).str.lstrip("0")
    covered_game_ids: set[str] = set(hist["game_id"].unique())
    hist_max_date = pd.to_datetime(hist["date"]).max()
    extension_cutoff = hist_max_date - pd.Timedelta(days=14)

    print(f"Historical file covers through: {hist_max_date.date()}")
    print(f"Will pull games after: {extension_cutoff.date()} from DuckDB")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    new_data = con.execute(f"""
        SELECT
            CAST(game_id AS VARCHAR) AS game_id,
            CAST(team_id AS BIGINT) AS team_id,
            CAST(player_id AS BIGINT) AS player_id,
            CAST(player_name AS VARCHAR) AS player_name,
            CAST(on_pts_for AS DOUBLE) AS on_pts_for,
            CAST(on_pts_against AS DOUBLE) AS on_pts_against,
            CAST(on_diff AS DOUBLE) AS on_diff,
            CAST(off_pts_for AS DOUBLE) AS off_pts_for,
            CAST(off_pts_against AS DOUBLE) AS off_pts_against,
            CAST(off_diff AS DOUBLE) AS off_diff,
            CAST(on_pts_for_adj AS DOUBLE) AS on_pts_for_adj,
            CAST(on_pts_against_adj AS DOUBLE) AS on_pts_against_adj,
            CAST(on_diff_adj AS DOUBLE) AS on_diff_adj,
            CAST(off_pts_for_adj AS DOUBLE) AS off_pts_for_adj,
            CAST(off_pts_against_adj AS DOUBLE) AS off_pts_against_adj,
            CAST(off_diff_adj AS DOUBLE) AS off_diff_adj,
            CAST(on_off_diff AS DOUBLE) AS on_off_diff,
            CAST(on_off_diff_adj AS DOUBLE) AS on_off_diff_adj,
            CAST(on_diff_reconstructed AS DOUBLE) AS on_diff_reconstructed,
            CAST(off_diff_reconstructed AS DOUBLE) AS off_diff_reconstructed,
            CAST(on_off_diff_reconstructed AS DOUBLE) AS on_off_diff_reconstructed,
            CAST(minutes_on AS DOUBLE) AS minutes_on,
            CAST(date AS VARCHAR) AS date
        FROM raw_adjusted_onoff
        WHERE CAST(date AS DATE) > DATE '{extension_cutoff.date()}'
          AND CAST(game_id AS VARCHAR) LIKE '2%'
    """).df()
    con.close()

    if new_data.empty:
        print("No data found in DuckDB for the extension window.")
        return

    new_data["game_id"] = new_data["game_id"].astype(str).str.lstrip("0")
    to_add = new_data[~new_data["game_id"].isin(covered_game_ids)].copy()

    if to_add.empty:
        print("No new games to add — historical file already covers this window.")
        return

    # Validate: all players on same team in same game should have same net margin.
    to_add["_player_net"] = (
        pd.to_numeric(to_add["on_diff_reconstructed"], errors="coerce").fillna(0)
        + pd.to_numeric(to_add["off_diff_reconstructed"], errors="coerce").fillna(0)
    )
    team_net_std = (
        to_add.groupby(["game_id", "team_id"])["_player_net"]
        .std(ddof=0)
        .fillna(0)
    )
    bad_game_ids: set[str] = set(
        team_net_std[team_net_std > 2.0].index.get_level_values("game_id").astype(str)
    )
    validated_game_ids = [g for g in to_add["game_id"].unique() if str(g) not in bad_game_ids]
    to_add = to_add.drop(columns=["_player_net"])

    rejected = len(to_add["game_id"].unique()) - len(validated_game_ids)
    if rejected > 0:
        print(f"WARNING: {rejected} game(s) failed validation and were excluded.")

    valid_to_add = to_add[to_add["game_id"].isin(validated_game_ids)]
    if valid_to_add.empty:
        print("No games passed validation.")
        return

    cols_to_use = [c for c in REQUIRED_COLS if c in valid_to_add.columns]
    valid_to_add = valid_to_add[cols_to_use]

    combined = pd.concat([hist, valid_to_add], ignore_index=True)
    combined = combined.sort_values(["date", "game_id", "team_id", "player_name"])
    combined.to_csv(HIST_ADJUSTED_ONOFF, index=False)

    print(
        f"Extended {HIST_ADJUSTED_ONOFF.name}: added {len(validated_game_ids)} game(s) "
        f"({len(valid_to_add)} rows). Total rows: {len(combined)}"
    )


if __name__ == "__main__":
    main()
