"""
Extend adjusted_onoff_historical_pbp.csv with new regular-season games from adjusted_onoff.csv.

Run after `run_onoff.py` completes. The historical file is the authoritative on/off source
for DuckDB; extending it ensures new games are covered without relying on the corrupted
player_daily_boxscore fallback.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

ADJUSTED_ONOFF = DATA_DIR / "adjusted_onoff.csv"
HIST_ADJUSTED_ONOFF = DATA_DIR / "adjusted_onoff_historical_pbp.csv"

REQUIRED_COLS = [
    "game_id",
    "team_id",
    "player_id",
    "player_name",
    "on_pts_for",
    "on_pts_against",
    "on_diff",
    "off_pts_for",
    "off_pts_against",
    "off_diff",
    "on_pts_for_adj",
    "on_pts_against_adj",
    "on_diff_adj",
    "off_pts_for_adj",
    "off_pts_against_adj",
    "off_diff_adj",
    "on_off_diff",
    "on_off_diff_adj",
    "on_diff_reconstructed",
    "off_diff_reconstructed",
    "on_off_diff_reconstructed",
    "minutes_on",
    "date",
]


def _is_lfs_pointer(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            return b"version https://git-lfs.github.com/spec/v1" in f.read(512)
    except Exception:
        return False


def main() -> None:
    if not ADJUSTED_ONOFF.exists():
        print(f"ERROR: {ADJUSTED_ONOFF} not found — run_onoff.py must complete first.")
        sys.exit(1)

    if _is_lfs_pointer(ADJUSTED_ONOFF):
        print(f"ERROR: {ADJUSTED_ONOFF} is an LFS pointer — git lfs pull must be run first.")
        sys.exit(1)

    if not HIST_ADJUSTED_ONOFF.exists():
        print(f"ERROR: {HIST_ADJUSTED_ONOFF} not found.")
        sys.exit(1)

    hist = pd.read_csv(HIST_ADJUSTED_ONOFF, dtype={"game_id": str, "player_id": int})
    hist["game_id"] = hist["game_id"].astype(str).str.lstrip("0")
    covered_game_ids: set[str] = set(hist["game_id"].unique())

    # Only extend with games after the historical file's most recent date.
    # This prevents accidentally pulling in old-season corrupted data.
    hist_max_date = pd.to_datetime(hist["date"]).max()
    extension_cutoff = hist_max_date - pd.Timedelta(days=14)  # 14-day overlap buffer
    print(f"Historical file covers through: {hist_max_date.date()}")
    print(f"Will only add games after: {extension_cutoff.date()}")

    new_data = pd.read_csv(ADJUSTED_ONOFF, dtype={"game_id": str, "player_id": int})
    new_data["game_id"] = new_data["game_id"].astype(str).str.lstrip("0")
    new_data["date_parsed"] = pd.to_datetime(new_data["date"])

    # Only regular-season games (IDs starting with "2") after the cutoff date.
    new_regular = new_data[
        new_data["game_id"].str.startswith("2")
        & (new_data["date_parsed"] > extension_cutoff)
    ].drop(columns=["date_parsed"]).copy()

    # Keep only games not already in the historical file.
    to_add = new_regular[~new_regular["game_id"].isin(covered_game_ids)].copy()

    if to_add.empty:
        print("No new regular-season games to add — historical file is up to date.")
        return

    # Validate: only include games that pass a basic sanity check.
    # For each player, on_diff_reconstructed + off_diff_reconstructed = team net margin.
    # All players on the same team in the same game should have identical net values.
    # High variance (std > 2) within a team indicates corrupted player_id assignments.
    to_add["_player_net"] = (
        pd.to_numeric(to_add["on_diff_reconstructed"], errors="coerce").fillna(0)
        + pd.to_numeric(to_add["off_diff_reconstructed"], errors="coerce").fillna(0)
    )
    team_net_std = (
        to_add.groupby(["game_id", "team_id"])["_player_net"]
        .std(ddof=0)
        .fillna(0)  # std of a single player is NaN → treat as 0 (fine)
    )
    bad_game_teams = team_net_std[team_net_std > 2.0]
    bad_game_ids: set[str] = set(bad_game_teams.index.get_level_values("game_id").astype(str))
    validated_game_ids = [g for g in to_add["game_id"].unique() if str(g) not in bad_game_ids]
    to_add = to_add.drop(columns=["_player_net"])

    valid_to_add = to_add[to_add["game_id"].isin(validated_game_ids)]

    if valid_to_add.empty:
        print(f"Found {len(to_add['game_id'].unique())} new game(s) but none passed validation — skipping.")
        return

    rejected = len(to_add["game_id"].unique()) - len(validated_game_ids)
    if rejected > 0:
        print(f"WARNING: {rejected} game(s) failed validation and were excluded.")

    # Keep only columns that exist in both files.
    cols_to_use = [c for c in REQUIRED_COLS if c in valid_to_add.columns]
    valid_to_add = valid_to_add[cols_to_use]

    combined = pd.concat([hist, valid_to_add], ignore_index=True)
    combined = combined.sort_values(["date", "game_id", "team_id", "player_name"])
    combined.to_csv(HIST_ADJUSTED_ONOFF, index=False)

    new_games = len(validated_game_ids)
    new_rows = len(valid_to_add)
    print(
        f"Extended {HIST_ADJUSTED_ONOFF.name}: added {new_games} game(s) ({new_rows} rows). "
        f"Total rows: {len(combined)}"
    )


if __name__ == "__main__":
    main()
