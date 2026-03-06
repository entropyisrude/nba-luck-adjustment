"""
Generate on/off aggregates from playoff stint data.

This script computes per-player on/off stats directly from stints_playoffs.csv,
similar to what run_onoff.py does for regular season but working from
pre-computed stint data (including historical backfill).
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path("data")


def compute_onoff_from_stints(stints: pd.DataFrame) -> pd.DataFrame:
    """
    Compute on/off stats for each player in each game from stint data.

    For each player in each game:
    - ON court: Sum points for/against when player is in lineup
    - OFF court: Sum points for/against when player is NOT in lineup (same team)
    """
    results = []

    # Group by game
    games = stints.groupby('game_id')
    total_games = len(games)

    for game_idx, (game_id, game_stints) in enumerate(games):
        if game_idx % 500 == 0:
            print(f"Processing game {game_idx + 1}/{total_games}...")

        # Get team IDs and date
        home_id = game_stints['home_id'].iloc[0]
        away_id = game_stints['away_id'].iloc[0]
        game_date = game_stints['date'].iloc[0]

        # Find all players who appeared in this game, by team
        home_players = set()
        away_players = set()

        for col in ['home_p1', 'home_p2', 'home_p3', 'home_p4', 'home_p5']:
            home_players.update(game_stints[col].dropna().astype(int).unique())
        for col in ['away_p1', 'away_p2', 'away_p3', 'away_p4', 'away_p5']:
            away_players.update(game_stints[col].dropna().astype(int).unique())

        # Process each player
        for player_id in home_players:
            stats = compute_player_game_stats(game_stints, player_id, home_id, is_home=True)
            stats['game_id'] = game_id
            stats['team_id'] = home_id
            stats['player_id'] = player_id
            stats['date'] = game_date
            results.append(stats)

        for player_id in away_players:
            stats = compute_player_game_stats(game_stints, player_id, away_id, is_home=False)
            stats['game_id'] = game_id
            stats['team_id'] = away_id
            stats['player_id'] = player_id
            stats['date'] = game_date
            results.append(stats)

    return pd.DataFrame(results)


def compute_player_game_stats(game_stints: pd.DataFrame, player_id: int, team_id: int, is_home: bool) -> dict:
    """Compute on/off stats for a single player in a single game."""

    # Determine which columns to check based on home/away
    if is_home:
        player_cols = ['home_p1', 'home_p2', 'home_p3', 'home_p4', 'home_p5']
        pts_for_col = 'home_pts'
        pts_against_col = 'away_pts'
        pts_for_adj_col = 'home_pts_adj'
        pts_against_adj_col = 'away_pts_adj'
    else:
        player_cols = ['away_p1', 'away_p2', 'away_p3', 'away_p4', 'away_p5']
        pts_for_col = 'away_pts'
        pts_against_col = 'home_pts'
        pts_for_adj_col = 'away_pts_adj'
        pts_against_adj_col = 'home_pts_adj'

    # Find stints where player is ON court
    on_mask = pd.Series(False, index=game_stints.index)
    for col in player_cols:
        on_mask |= (game_stints[col] == player_id)

    on_stints = game_stints[on_mask]
    off_stints = game_stints[~on_mask]

    # Compute stats
    on_pts_for = on_stints[pts_for_col].sum()
    on_pts_against = on_stints[pts_against_col].sum()
    on_pts_for_adj = on_stints[pts_for_adj_col].sum()
    on_pts_against_adj = on_stints[pts_against_adj_col].sum()
    minutes_on = on_stints['seconds'].sum() / 60.0

    off_pts_for = off_stints[pts_for_col].sum()
    off_pts_against = off_stints[pts_against_col].sum()
    off_pts_for_adj = off_stints[pts_for_adj_col].sum()
    off_pts_against_adj = off_stints[pts_against_adj_col].sum()

    return {
        'on_pts_for': on_pts_for,
        'on_pts_against': on_pts_against,
        'on_diff': on_pts_for - on_pts_against,
        'off_pts_for': off_pts_for,
        'off_pts_against': off_pts_against,
        'off_diff': off_pts_for - off_pts_against,
        'on_pts_for_adj': on_pts_for_adj,
        'on_pts_against_adj': on_pts_against_adj,
        'on_diff_adj': on_pts_for_adj - on_pts_against_adj,
        'off_pts_for_adj': off_pts_for_adj,
        'off_pts_against_adj': off_pts_against_adj,
        'off_diff_adj': off_pts_for_adj - off_pts_against_adj,
        'on_off_diff': (on_pts_for - on_pts_against) - (off_pts_for - off_pts_against),
        'on_off_diff_adj': (on_pts_for_adj - on_pts_against_adj) - (off_pts_for_adj - off_pts_against_adj),
        'minutes_on': minutes_on,
    }


def get_player_names(player_ids: set, stints: pd.DataFrame) -> dict:
    """Get player names from various sources."""
    player_names = {}

    # First try regular season on/off
    regular_path = DATA_DIR / "adjusted_onoff.csv"
    if regular_path.exists():
        df = pd.read_csv(regular_path, dtype={"player_id": int})
        for _, row in df.drop_duplicates(subset=["player_id"]).iterrows():
            pid = int(row["player_id"])
            if pid in player_ids:
                player_names[pid] = row.get("player_name", f"Player {pid}")

    # Then try historical PBP
    missing = player_ids - set(player_names.keys())
    if missing:
        historical_dir = DATA_DIR / "historical_pbp"
        if historical_dir.exists():
            for pbp_file in sorted(historical_dir.glob("nbastats_po_*.csv")):
                if not missing:
                    break
                try:
                    pbp = pd.read_csv(pbp_file, usecols=[
                        "PLAYER1_ID", "PLAYER1_NAME",
                        "PLAYER2_ID", "PLAYER2_NAME",
                        "PLAYER3_ID", "PLAYER3_NAME",
                    ])
                    for player_col, name_col in [
                        ("PLAYER1_ID", "PLAYER1_NAME"),
                        ("PLAYER2_ID", "PLAYER2_NAME"),
                        ("PLAYER3_ID", "PLAYER3_NAME"),
                    ]:
                        subset = pbp[[player_col, name_col]].dropna().drop_duplicates()
                        for _, row in subset.iterrows():
                            pid = int(row[player_col])
                            if pid in missing:
                                player_names[pid] = str(row[name_col])
                                missing.discard(pid)
                except Exception:
                    pass

    # Fill remaining with placeholder
    for pid in player_ids:
        if pid not in player_names:
            player_names[pid] = f"Player {pid}"

    return player_names


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate playoff on/off aggregates from stint data")
    parser.add_argument("--start-season", type=str, default=None, help="Filter from this season (e.g., '2020-21')")
    parser.add_argument("--end-season", type=str, default=None, help="Filter up to this season")
    args = parser.parse_args()

    # Load stint data
    stint_path = DATA_DIR / "stints_playoffs.csv"
    if not stint_path.exists():
        print(f"Error: {stint_path} not found")
        return

    print(f"Loading {stint_path}...")
    stints = pd.read_csv(stint_path, dtype={"game_id": str})
    print(f"Loaded {len(stints)} stints from {stints['game_id'].nunique()} games")

    # Filter by season if specified
    if args.start_season or args.end_season:
        stints['date'] = pd.to_datetime(stints['date'])
        if args.start_season:
            start_year = int(args.start_season.split('-')[0])
            stints = stints[stints['date'] >= f"{start_year}-07-01"]
        if args.end_season:
            end_year = int(args.end_season.split('-')[0]) + 1
            stints = stints[stints['date'] <= f"{end_year}-06-30"]
        print(f"After filtering: {len(stints)} stints")

    # Compute on/off stats
    print("\nComputing on/off stats...")
    onoff = compute_onoff_from_stints(stints)
    print(f"Generated {len(onoff)} player-game records")

    # Add player names
    print("\nLooking up player names...")
    all_player_ids = set(onoff['player_id'].unique())
    player_names = get_player_names(all_player_ids, stints)
    onoff['player_name'] = onoff['player_id'].map(player_names)

    # Reorder columns
    cols = ['game_id', 'team_id', 'player_id', 'player_name',
            'on_pts_for', 'on_pts_against', 'on_diff',
            'off_pts_for', 'off_pts_against', 'off_diff',
            'on_pts_for_adj', 'on_pts_against_adj', 'on_diff_adj',
            'off_pts_for_adj', 'off_pts_against_adj', 'off_diff_adj',
            'on_off_diff', 'on_off_diff_adj', 'minutes_on', 'date']
    onoff = onoff[cols]

    # Sort by date and game
    onoff = onoff.sort_values(['date', 'game_id', 'team_id', 'player_name'])

    # Save
    out_path = DATA_DIR / "adjusted_onoff_playoffs.csv"
    onoff.to_csv(out_path, index=False)
    print(f"\nWrote {len(onoff)} records to {out_path}")

    # Print summary stats
    print("\n" + "="*60)
    print("PLAYOFF ON/OFF SUMMARY")
    print("="*60)
    print(f"Date range: {onoff['date'].min()} to {onoff['date'].max()}")
    print(f"Unique games: {onoff['game_id'].nunique()}")
    print(f"Unique players: {onoff['player_id'].nunique()}")

    # Show top players by total on/off diff (adjusted)
    totals = onoff.groupby(['player_id', 'player_name']).agg({
        'on_off_diff_adj': 'sum',
        'minutes_on': 'sum'
    }).reset_index()
    totals = totals[totals['minutes_on'] >= 100]  # Min 100 minutes
    totals = totals.sort_values('on_off_diff_adj', ascending=False)

    print("\nTop 15 by total adjusted on/off differential (min 100 min):")
    for _, row in totals.head(15).iterrows():
        print(f"  {row['player_name']:25s} {row['on_off_diff_adj']:+8.1f} ({row['minutes_on']:.0f} min)")


if __name__ == "__main__":
    main()
