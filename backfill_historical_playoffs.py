"""
Backfill historical playoff data using shufinskiy/nba_data repository.

Downloads playoff play-by-play data and derives lineups from substitution events.
"""

import os
import sys
import tarfile
import lzma
from pathlib import Path
from collections import defaultdict
import json

import pandas as pd
import numpy as np
import requests
import yaml

DATA_DIR = Path("data")
HISTORICAL_PBP_DIR = DATA_DIR / "historical_pbp"

# Event message types from NBA stats
EVENT_MADE_SHOT = 1
EVENT_MISSED_SHOT = 2
EVENT_FREE_THROW = 3
EVENT_REBOUND = 4
EVENT_TURNOVER = 5
EVENT_FOUL = 6
EVENT_SUBSTITUTION = 8
EVENT_TIMEOUT = 9
EVENT_JUMP_BALL = 10
EVENT_PERIOD_START = 12
EVENT_PERIOD_END = 13


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def download_playoff_data(seasons: list[int]):
    """Download playoff play-by-play data for specified seasons."""
    HISTORICAL_PBP_DIR.mkdir(parents=True, exist_ok=True)

    base_url = "https://github.com/shufinskiy/nba_data/raw/main/datasets"

    for season in seasons:
        season_str = f"{season}-{(season+1) % 100:02d}"
        csv_file = HISTORICAL_PBP_DIR / f"nbastats_po_{season}.csv"

        if csv_file.exists():
            print(f"Season {season_str} playoffs already downloaded")
            continue

        print(f"Downloading {season_str} playoff data...")
        tar_file = HISTORICAL_PBP_DIR / f"nbastats_po_{season}.tar.xz"
        url = f"{base_url}/nbastats_po_{season}.tar.xz"

        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            tar_file.write_bytes(resp.content)

            # Extract
            with lzma.open(tar_file) as xz:
                with tarfile.open(fileobj=xz) as tar:
                    tar.extractall(HISTORICAL_PBP_DIR)

            tar_file.unlink()  # Remove archive
            print(f"  Saved to {csv_file}")
        except Exception as e:
            print(f"  Error downloading {season_str}: {e}")


def get_starters_from_period(pbp: pd.DataFrame, period: int, game_id: str) -> dict[int, list[int]]:
    """
    Infer starters for a period by looking at early events.
    Returns {team_id: [player_ids]}
    """
    period_df = pbp[(pbp["GAME_ID"] == game_id) & (pbp["PERIOD"] == period)].copy()
    if period_df.empty:
        return {}

    starters = defaultdict(set)

    # Look at first few events to find players
    for _, row in period_df.head(50).iterrows():
        for player_col, team_col in [
            ("PLAYER1_ID", "PLAYER1_TEAM_ID"),
            ("PLAYER2_ID", "PLAYER2_TEAM_ID"),
            ("PLAYER3_ID", "PLAYER3_TEAM_ID"),
        ]:
            pid = row.get(player_col)
            tid = row.get(team_col)
            if pd.notna(pid) and pd.notna(tid) and int(pid) > 0 and int(tid) > 0:
                starters[int(tid)].add(int(pid))

    # Also track substitutions to know who was ON court at start
    current_on = defaultdict(set)
    for tid, players in starters.items():
        current_on[tid] = players.copy()

    for _, row in period_df.iterrows():
        if row["EVENTMSGTYPE"] == EVENT_SUBSTITUTION:
            player_in = int(row["PLAYER2_ID"]) if pd.notna(row.get("PLAYER2_ID")) else 0
            player_out = int(row["PLAYER1_ID"]) if pd.notna(row.get("PLAYER1_ID")) else 0
            team_id = int(row["PLAYER1_TEAM_ID"]) if pd.notna(row.get("PLAYER1_TEAM_ID")) else 0

            if player_out > 0 and team_id > 0:
                # Player going out was a starter
                starters[team_id].add(player_out)
            if player_in > 0 and team_id > 0:
                # Player coming in was NOT a starter (unless they were subbed out first)
                pass

    # Limit to 5 per team
    result = {}
    for tid, players in starters.items():
        result[tid] = list(players)[:5]

    return result


def track_lineups_for_game(game_pbp: pd.DataFrame) -> pd.DataFrame:
    """
    Track lineups throughout a game based on substitutions.
    Returns the pbp with added lineup columns.
    """
    game_pbp = game_pbp.sort_values(["PERIOD", "EVENTNUM"]).copy()

    # Get team IDs
    team_ids = set()
    for col in ["PLAYER1_TEAM_ID", "PLAYER2_TEAM_ID", "PLAYER3_TEAM_ID"]:
        team_ids.update(game_pbp[col].dropna().astype(int).unique())
    team_ids = [t for t in team_ids if t > 0]

    if len(team_ids) != 2:
        return pd.DataFrame()

    # Initialize lineups
    lineups = {tid: set() for tid in team_ids}

    # Track by period (lineups reset at period start)
    results = []
    current_period = 0

    for idx, row in game_pbp.iterrows():
        period = int(row["PERIOD"])
        event_type = int(row["EVENTMSGTYPE"])

        # Period change - reset lineups
        if period != current_period:
            current_period = period
            starters = get_starters_from_period(game_pbp, period, row["GAME_ID"])
            for tid in team_ids:
                lineups[tid] = set(starters.get(tid, []))

        # Handle substitution
        if event_type == EVENT_SUBSTITUTION:
            player_out = int(row["PLAYER1_ID"]) if pd.notna(row.get("PLAYER1_ID")) else 0
            player_in = int(row["PLAYER2_ID"]) if pd.notna(row.get("PLAYER2_ID")) else 0
            team_id = int(row["PLAYER1_TEAM_ID"]) if pd.notna(row.get("PLAYER1_TEAM_ID")) else 0

            if team_id in lineups:
                if player_out > 0:
                    lineups[team_id].discard(player_out)
                if player_in > 0:
                    lineups[team_id].add(player_in)

        # Add lineup info to row
        row_dict = row.to_dict()
        for i, tid in enumerate(sorted(team_ids)):
            lineup = sorted(lineups[tid])[:5]
            for j in range(5):
                col = f"TEAM{i+1}_P{j+1}"
                row_dict[col] = lineup[j] if j < len(lineup) else 0
            row_dict[f"TEAM{i+1}_ID"] = tid

        results.append(row_dict)

    return pd.DataFrame(results)


def parse_clock_to_seconds(clock_str: str) -> float:
    """Parse clock string (MM:SS or MM:SS.x) to seconds."""
    if pd.isna(clock_str):
        return 720.0
    clock_str = str(clock_str).strip()
    if ":" in clock_str:
        parts = clock_str.split(":")
        mins = int(parts[0])
        secs = float(parts[1])
        return mins * 60 + secs
    return float(clock_str)


def calculate_elapsed_seconds(period: int, clock_seconds: float) -> float:
    """Calculate elapsed game seconds from period and clock."""
    period_length = 720 if period <= 4 else 300
    if period <= 4:
        return (period - 1) * 720 + (720 - clock_seconds)
    else:
        return 4 * 720 + (period - 5) * 300 + (300 - clock_seconds)


def get_expected_3pt_pct(player_id: int, player_state: pd.DataFrame) -> float:
    """Get expected 3PT% for a player from state."""
    if player_state.empty:
        return 0.36
    row = player_state[player_state["player_id"] == player_id]
    if row.empty:
        return 0.36
    A_r = float(row.iloc[0].get("A_r", 0))
    M_r = float(row.iloc[0].get("M_r", 0))
    if A_r < 10:
        return 0.36
    return M_r / A_r


def process_game_to_stints(
    game_pbp: pd.DataFrame,
    home_team_id: int,
    away_team_id: int,
    player_state: pd.DataFrame,
) -> pd.DataFrame:
    """Convert play-by-play with lineups to stint format."""

    if game_pbp.empty:
        return pd.DataFrame()

    # Determine which team columns are home/away
    team1_id = game_pbp["TEAM1_ID"].iloc[0] if "TEAM1_ID" in game_pbp.columns else 0
    team2_id = game_pbp["TEAM2_ID"].iloc[0] if "TEAM2_ID" in game_pbp.columns else 0

    if home_team_id == team1_id:
        home_cols = [f"TEAM1_P{i}" for i in range(1, 6)]
        away_cols = [f"TEAM2_P{i}" for i in range(1, 6)]
    else:
        home_cols = [f"TEAM2_P{i}" for i in range(1, 6)]
        away_cols = [f"TEAM1_P{i}" for i in range(1, 6)]

    stints = []
    current_stint = None

    for _, row in game_pbp.iterrows():
        period = int(row["PERIOD"])
        clock_str = row.get("PCTIMESTRING", "12:00")
        clock_seconds = parse_clock_to_seconds(clock_str)
        elapsed = calculate_elapsed_seconds(period, clock_seconds)

        # Get current lineup
        try:
            home_lineup = tuple(sorted([int(row[c]) for c in home_cols if pd.notna(row.get(c)) and int(row[c]) > 0]))
            away_lineup = tuple(sorted([int(row[c]) for c in away_cols if pd.notna(row.get(c)) and int(row[c]) > 0]))
        except:
            continue

        if len(home_lineup) != 5 or len(away_lineup) != 5:
            continue

        # Parse score
        score_str = str(row.get("SCORE", "0 - 0") or "0 - 0")
        try:
            if " - " in score_str:
                away_score, home_score = map(int, score_str.split(" - "))
            else:
                home_score, away_score = 0, 0
        except:
            home_score, away_score = 0, 0

        # Check for 3PT shots
        pts_adj_home = 0.0
        pts_adj_away = 0.0

        event_type = int(row.get("EVENTMSGTYPE", 0))
        home_desc = str(row.get("HOMEDESCRIPTION", "") or "")
        away_desc = str(row.get("VISITORDESCRIPTION", "") or "")

        if event_type in [EVENT_MADE_SHOT, EVENT_MISSED_SHOT]:
            is_3pt = "3PT" in home_desc.upper() or "3PT" in away_desc.upper()
            if is_3pt:
                shooter_id = int(row.get("PLAYER1_ID", 0) or 0)
                shooter_team = int(row.get("PLAYER1_TEAM_ID", 0) or 0)

                if shooter_id > 0:
                    exp_pct = get_expected_3pt_pct(shooter_id, player_state)

                    if event_type == EVENT_MADE_SHOT:
                        adj = 3 - (3 * exp_pct)
                    else:
                        adj = 0 - (3 * exp_pct)

                    if shooter_team == home_team_id:
                        pts_adj_home = adj
                    else:
                        pts_adj_away = adj

        lineup_key = (home_lineup, away_lineup)

        if current_stint is None or current_stint["lineup_key"] != lineup_key:
            # Save previous stint
            if current_stint is not None:
                current_stint["end_elapsed"] = elapsed
                current_stint["end_home_score"] = home_score
                current_stint["end_away_score"] = away_score
                if current_stint["end_elapsed"] > current_stint["start_elapsed"]:
                    stints.append(current_stint)

            # Start new stint
            current_stint = {
                "lineup_key": lineup_key,
                "home_lineup": home_lineup,
                "away_lineup": away_lineup,
                "start_elapsed": elapsed,
                "end_elapsed": elapsed,
                "start_period": period,
                "end_period": period,
                "start_clock": clock_str,
                "end_clock": clock_str,
                "start_home_score": home_score,
                "start_away_score": away_score,
                "end_home_score": home_score,
                "end_away_score": away_score,
                "home_pts_adj_total": pts_adj_home,
                "away_pts_adj_total": pts_adj_away,
            }
        else:
            current_stint["end_elapsed"] = elapsed
            current_stint["end_period"] = period
            current_stint["end_clock"] = clock_str
            current_stint["end_home_score"] = home_score
            current_stint["end_away_score"] = away_score
            current_stint["home_pts_adj_total"] += pts_adj_home
            current_stint["away_pts_adj_total"] += pts_adj_away

    # Save last stint
    if current_stint is not None and current_stint["end_elapsed"] > current_stint["start_elapsed"]:
        stints.append(current_stint)

    # Convert to DataFrame
    game_id = game_pbp["GAME_ID"].iloc[0]
    game_date = str(game_pbp.get("GAME_DATE", pd.Series()).iloc[0] if "GAME_DATE" in game_pbp.columns else "")[:10]

    rows = []
    for i, s in enumerate(stints):
        seconds = s["end_elapsed"] - s["start_elapsed"]
        if seconds <= 0:
            continue

        home_pts = s["end_home_score"] - s["start_home_score"]
        away_pts = s["end_away_score"] - s["start_away_score"]

        row = {
            "game_id": str(game_id),
            "stint_index": i,
            "home_id": home_team_id,
            "away_id": away_team_id,
            "home_p1": s["home_lineup"][0] if len(s["home_lineup"]) > 0 else 0,
            "home_p2": s["home_lineup"][1] if len(s["home_lineup"]) > 1 else 0,
            "home_p3": s["home_lineup"][2] if len(s["home_lineup"]) > 2 else 0,
            "home_p4": s["home_lineup"][3] if len(s["home_lineup"]) > 3 else 0,
            "home_p5": s["home_lineup"][4] if len(s["home_lineup"]) > 4 else 0,
            "away_p1": s["away_lineup"][0] if len(s["away_lineup"]) > 0 else 0,
            "away_p2": s["away_lineup"][1] if len(s["away_lineup"]) > 1 else 0,
            "away_p3": s["away_lineup"][2] if len(s["away_lineup"]) > 2 else 0,
            "away_p4": s["away_lineup"][3] if len(s["away_lineup"]) > 3 else 0,
            "away_p5": s["away_lineup"][4] if len(s["away_lineup"]) > 4 else 0,
            "seconds": seconds,
            "home_pts": home_pts,
            "away_pts": away_pts,
            "home_pts_adj": home_pts - s["home_pts_adj_total"],
            "away_pts_adj": away_pts - s["away_pts_adj_total"],
            "start_elapsed": s["start_elapsed"],
            "end_elapsed": s["end_elapsed"],
            "start_period": s["start_period"],
            "start_clock": s["start_clock"],
            "end_period": s["end_period"],
            "end_clock": s["end_clock"],
            "start_home_score": s["start_home_score"],
            "start_away_score": s["start_away_score"],
            "end_home_score": s["end_home_score"],
            "end_away_score": s["end_away_score"],
            "start_home_score_adj": s["start_home_score"],
            "start_away_score_adj": s["start_away_score"],
            "end_home_score_adj": s["end_home_score"] - s["home_pts_adj_total"],
            "end_away_score_adj": s["end_away_score"] - s["away_pts_adj_total"],
            "date": game_date,
        }
        rows.append(row)

    return pd.DataFrame(rows)


def infer_home_away_from_pbp(game_pbp: pd.DataFrame) -> tuple[int, int]:
    """Infer home and away team IDs from the play-by-play."""
    home_votes = defaultdict(int)
    away_votes = defaultdict(int)

    for _, row in game_pbp.iterrows():
        try:
            tid = row.get("PLAYER1_TEAM_ID")
            if pd.isna(tid):
                continue
            team_id = int(tid)
            if team_id <= 0:
                continue

            if pd.notna(row.get("HOMEDESCRIPTION")) and str(row["HOMEDESCRIPTION"]).strip():
                home_votes[team_id] += 1
            if pd.notna(row.get("VISITORDESCRIPTION")) and str(row["VISITORDESCRIPTION"]).strip():
                away_votes[team_id] += 1
        except (ValueError, TypeError):
            continue

    home_id = max(home_votes, key=home_votes.get) if home_votes else 0
    away_id = max(away_votes, key=away_votes.get) if away_votes else 0

    return home_id, away_id


def process_season_playoffs(season: int, player_state: pd.DataFrame) -> pd.DataFrame:
    """Process all playoff games for a season."""
    season_str = f"{season}-{(season+1) % 100:02d}"
    pbp_file = HISTORICAL_PBP_DIR / f"nbastats_po_{season}.csv"

    if not pbp_file.exists():
        print(f"No data file for {season_str} playoffs")
        return pd.DataFrame()

    print(f"Processing {season_str} playoffs...")
    pbp = pd.read_csv(pbp_file)

    games = pbp["GAME_ID"].unique()
    print(f"  Found {len(games)} playoff games")

    all_stints = []
    for game_id in games:
        game_pbp = pbp[pbp["GAME_ID"] == game_id].copy()

        # Track lineups
        game_with_lineups = track_lineups_for_game(game_pbp)
        if game_with_lineups.empty:
            continue

        # Infer home/away
        home_id, away_id = infer_home_away_from_pbp(game_pbp)
        if home_id == 0 or away_id == 0:
            continue

        # Convert to stints
        stints = process_game_to_stints(game_with_lineups, home_id, away_id, player_state)
        if not stints.empty:
            all_stints.append(stints)

    if all_stints:
        result = pd.concat(all_stints, ignore_index=True)
        print(f"  Generated {len(result)} stints from {len(games)} games")
        return result

    return pd.DataFrame()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Backfill historical playoff data")
    parser.add_argument(
        "--seasons", type=str, default="2013-2018",
        help="Season range (e.g., '2013-2018' for 2013-14 through 2018-19)"
    )
    parser.add_argument(
        "--download-only", action="store_true",
        help="Only download data, don't process"
    )
    args = parser.parse_args()

    # Parse season range
    start_year, end_year = map(int, args.seasons.split("-"))
    seasons = list(range(start_year, end_year + 1))

    print(f"Processing playoff seasons: {[f'{s}-{(s+1)%100:02d}' for s in seasons]}")

    # Download data
    download_playoff_data(seasons)

    if args.download_only:
        print("Download complete.")
        return

    # Load player state
    player_state_path = DATA_DIR / "player_state.csv"
    if player_state_path.exists():
        player_state = pd.read_csv(player_state_path)
    else:
        player_state = pd.DataFrame()

    # Process each season
    all_stints = []
    for season in seasons:
        stints = process_season_playoffs(season, player_state)
        if not stints.empty:
            all_stints.append(stints)

    if all_stints:
        combined = pd.concat(all_stints, ignore_index=True)

        # Load existing playoff stints and merge
        existing_path = DATA_DIR / "stints_playoffs.csv"
        if existing_path.exists():
            existing = pd.read_csv(existing_path, dtype={"game_id": str})
            existing_games = set(existing["game_id"].astype(str).unique())
            combined = combined[~combined["game_id"].astype(str).isin(existing_games)]

            if not combined.empty:
                combined = pd.concat([existing, combined], ignore_index=True)
                combined = combined.sort_values(["date", "game_id", "stint_index"])
                combined.to_csv(existing_path, index=False)
                print(f"\nAppended {len(combined) - len(existing)} new stints")
                print(f"Total playoff stints: {len(combined)}")
            else:
                print("\nNo new stints to add")
        else:
            combined = combined.sort_values(["date", "game_id", "stint_index"])
            combined.to_csv(existing_path, index=False)
            print(f"\nWrote {len(combined)} playoff stints to {existing_path}")
    else:
        print("\nNo stint data generated")


if __name__ == "__main__":
    main()
