"""
Compute 3PT-luck-adjusted RAPM (Regularized Adjusted Plus-Minus) from stint data.

This uses the stint-level data with 3PT adjustments applied to run ridge regression
and estimate each player's individual contribution to point differential.
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.linear_model import Ridge

DATA_DIR = Path("data")

# Team abbreviation mapping for display
TEAM_ID_TO_ABBR = {
    1610612737: "ATL", 1610612738: "BOS", 1610612751: "BKN", 1610612766: "CHA",
    1610612741: "CHI", 1610612739: "CLE", 1610612742: "DAL", 1610612743: "DEN",
    1610612765: "DET", 1610612744: "GSW", 1610612745: "HOU", 1610612754: "IND",
    1610612746: "LAC", 1610612747: "LAL", 1610612763: "MEM", 1610612748: "MIA",
    1610612749: "MIL", 1610612750: "MIN", 1610612740: "NOP", 1610612752: "NYK",
    1610612760: "OKC", 1610612753: "ORL", 1610612755: "PHI", 1610612756: "PHX",
    1610612757: "POR", 1610612758: "SAC", 1610612759: "SAS", 1610612761: "TOR",
    1610612762: "UTA", 1610612764: "WAS",
}


def load_stints(stint_path: Path, min_seconds: int = 10) -> pd.DataFrame:
    """Load stint data and filter out very short stints."""
    df = pd.read_csv(stint_path, dtype={"game_id": str})
    # Filter stints with minimum duration
    df = df[df["seconds"] >= min_seconds].copy()
    return df


def get_player_list_and_index(stints: pd.DataFrame):
    """Get sorted list of all players and their index mapping."""
    player_cols = ["home_p1", "home_p2", "home_p3", "home_p4", "home_p5",
                   "away_p1", "away_p2", "away_p3", "away_p4", "away_p5"]
    all_players = set()
    for col in player_cols:
        all_players.update(stints[col].dropna().astype(int).unique())
    player_list = sorted(all_players)
    player_to_idx = {pid: i for i, pid in enumerate(player_list)}
    return player_list, player_to_idx


def build_design_matrix(stints: pd.DataFrame, use_adjusted: bool = True):
    """
    Build the design matrix X and target vector y for RAPM regression.

    For each stint:
    - y = point differential (home_pts - away_pts) per 100 possessions
    - X[i, player_j] = +1 if player_j was on home team
    - X[i, player_j] = -1 if player_j was on away team
    - X[i, player_j] = 0 otherwise

    Weight by possession proxy (seconds / 24 as rough possession estimate).
    """
    player_list, player_to_idx = get_player_list_and_index(stints)
    n_players = len(player_list)
    n_stints = len(stints)

    print(f"Building design matrix: {n_stints} stints, {n_players} players")

    # Build sparse design matrix
    row_indices = []
    col_indices = []
    data = []

    for stint_idx, row in enumerate(stints.itertuples()):
        # Home players get +1
        for col in ["home_p1", "home_p2", "home_p3", "home_p4", "home_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(stint_idx)
                    col_indices.append(player_to_idx[pid])
                    data.append(1.0)

        # Away players get -1
        for col in ["away_p1", "away_p2", "away_p3", "away_p4", "away_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(stint_idx)
                    col_indices.append(player_to_idx[pid])
                    data.append(-1.0)

    X = sparse.csr_matrix((data, (row_indices, col_indices)), shape=(n_stints, n_players))

    # Target: point differential per 100 possessions
    # Estimate possessions as seconds / 24 (rough average possession length)
    possessions = stints["seconds"].values / 24.0
    possessions = np.maximum(possessions, 0.1)  # Avoid division by zero

    if use_adjusted:
        point_diff = stints["home_pts_adj"].values - stints["away_pts_adj"].values
    else:
        point_diff = stints["home_pts"].values - stints["away_pts"].values

    # Per 100 possessions
    y = (point_diff / possessions) * 100.0

    # Weights proportional to possessions (more possessions = more reliable)
    weights = np.sqrt(possessions)

    return X, y, weights, player_list, player_to_idx


def build_design_matrix_orapm(stints: pd.DataFrame, use_adjusted: bool = True):
    """
    Build design matrix for Offensive RAPM (ORAPM).

    Creates TWO rows per stint - one for each team's offense:
    - Row 2i: Home team offense (target = home_pts), home players +1, away players -1
    - Row 2i+1: Away team offense (target = away_pts), away players +1, home players -1

    This captures offensive contribution adjusted for opponent defensive quality.
    """
    player_list, player_to_idx = get_player_list_and_index(stints)
    n_players = len(player_list)
    n_stints = len(stints)
    n_rows = n_stints * 2  # Two observations per stint

    print(f"Building ORAPM design matrix: {n_stints} stints -> {n_rows} rows, {n_players} players")

    row_indices = []
    col_indices = []
    data = []

    for stint_idx, row in enumerate(stints.itertuples()):
        # Row 2*stint_idx: Home team offense
        # Home players (on offense) get +1
        for col in ["home_p1", "home_p2", "home_p3", "home_p4", "home_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(2 * stint_idx)
                    col_indices.append(player_to_idx[pid])
                    data.append(1.0)
        # Away players (defending) get -1
        for col in ["away_p1", "away_p2", "away_p3", "away_p4", "away_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(2 * stint_idx)
                    col_indices.append(player_to_idx[pid])
                    data.append(-1.0)

        # Row 2*stint_idx+1: Away team offense
        # Away players (on offense) get +1
        for col in ["away_p1", "away_p2", "away_p3", "away_p4", "away_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(2 * stint_idx + 1)
                    col_indices.append(player_to_idx[pid])
                    data.append(1.0)
        # Home players (defending) get -1
        for col in ["home_p1", "home_p2", "home_p3", "home_p4", "home_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(2 * stint_idx + 1)
                    col_indices.append(player_to_idx[pid])
                    data.append(-1.0)

    X = sparse.csr_matrix((data, (row_indices, col_indices)), shape=(n_rows, n_players))

    # Possessions per stint (use same for both offense observations)
    possessions = stints["seconds"].values / 24.0
    possessions = np.maximum(possessions, 0.1)

    if use_adjusted:
        home_pts = stints["home_pts_adj"].values
        away_pts = stints["away_pts_adj"].values
    else:
        home_pts = stints["home_pts"].values
        away_pts = stints["away_pts"].values

    # Target: points scored per 100 possessions
    # Interleave home and away offensive outputs
    y = np.zeros(n_rows)
    y[0::2] = (home_pts / possessions) * 100.0  # Home offense
    y[1::2] = (away_pts / possessions) * 100.0  # Away offense

    # Weights - same for both observations from same stint
    weights = np.zeros(n_rows)
    weights[0::2] = np.sqrt(possessions)
    weights[1::2] = np.sqrt(possessions)

    return X, y, weights, player_list, player_to_idx


def run_rapm(X, y, weights, alpha: float = 2500.0):
    """
    Run ridge regression to estimate RAPM values.

    alpha: regularization strength (higher = more shrinkage toward 0)
    """
    # Apply weights
    X_weighted = X.multiply(weights[:, np.newaxis])
    y_weighted = y * weights

    # Ridge regression
    model = Ridge(alpha=alpha, fit_intercept=True)
    model.fit(X_weighted, y_weighted)

    return model.coef_, model.intercept_


def get_player_info(player_ids: list[int], stints: pd.DataFrame, suffix: str = "") -> dict:
    """Get player names and teams from the stint data and onoff data."""
    player_info = {}

    # First load from regular season data (has most complete player names)
    regular_path = DATA_DIR / "adjusted_onoff.csv"
    if regular_path.exists():
        onoff = pd.read_csv(regular_path, dtype={"player_id": int})
        onoff = onoff.sort_values("date").drop_duplicates(subset=["player_id"], keep="last")
        for _, row in onoff.iterrows():
            pid = int(row["player_id"])
            player_info[pid] = {
                "name": row.get("player_name", f"Player {pid}"),
                "team_id": int(row.get("team_id", 0)),
            }

    # Then overlay with playoff-specific data if available (for more accurate team assignments)
    if suffix:
        onoff_path = DATA_DIR / f"adjusted_onoff{suffix}.csv"
        if onoff_path.exists():
            onoff = pd.read_csv(onoff_path, dtype={"player_id": int})
            onoff = onoff.sort_values("date").drop_duplicates(subset=["player_id"], keep="last")
            for _, row in onoff.iterrows():
                pid = int(row["player_id"])
                # Update or add player info from playoffs
                if pid not in player_info:
                    player_info[pid] = {
                        "name": row.get("player_name", f"Player {pid}"),
                        "team_id": int(row.get("team_id", 0)),
                    }
                else:
                    # Update team from playoffs if available
                    player_info[pid]["team_id"] = int(row.get("team_id", player_info[pid]["team_id"]))

    return player_info


def main():
    parser = argparse.ArgumentParser(description="Compute 3PT-adjusted RAPM")
    parser.add_argument(
        "--alpha", type=float, default=2500.0,
        help="Ridge regularization strength (default: 2500)"
    )
    parser.add_argument(
        "--min-seconds", type=int, default=10,
        help="Minimum stint duration in seconds (default: 10)"
    )
    parser.add_argument(
        "--min-minutes", type=float, default=200.0,
        help="Minimum total minutes for a player to be included in output (default: 200)"
    )
    parser.add_argument(
        "--use-raw", action="store_true",
        help="Use raw (non-adjusted) point differential instead of 3PT-adjusted"
    )
    parser.add_argument(
        "--start-date", type=str, default=None,
        help="Filter stints from this date onwards (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", type=str, default=None,
        help="Filter stints up to this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--playoffs", action="store_true",
        help="Use playoff data (stints_playoffs.csv) instead of regular season"
    )
    args = parser.parse_args()

    suffix = "_playoffs" if args.playoffs else ""
    stint_path = DATA_DIR / f"stints{suffix}.csv"
    if not stint_path.exists():
        print(f"Error: {stint_path} not found. Run run_onoff.py first to generate stint data.")
        return

    stints = load_stints(stint_path, min_seconds=args.min_seconds)

    # Filter by date if specified
    if args.start_date:
        stints = stints[stints["date"] >= args.start_date]
    if args.end_date:
        stints = stints[stints["date"] <= args.end_date]

    if stints.empty:
        print("No stints found after filtering.")
        return

    print(f"Loaded {len(stints)} stints")

    # Build design matrix
    use_adjusted = not args.use_raw
    X, y, weights, player_list, player_to_idx = build_design_matrix(stints, use_adjusted=use_adjusted)

    # Run RAPM
    print(f"Running ridge regression with alpha={args.alpha}...")
    coefficients, intercept = run_rapm(X, y, weights, alpha=args.alpha)

    print(f"Intercept (league average): {intercept:.2f}")

    # Get player info
    player_info = get_player_info(player_list, stints, suffix)

    # Calculate minutes per player from stint data
    player_minutes = {}
    for col_set, sign in [(["home_p1", "home_p2", "home_p3", "home_p4", "home_p5"], 1),
                          (["away_p1", "away_p2", "away_p3", "away_p4", "away_p5"], -1)]:
        for col in col_set:
            for _, row in stints.iterrows():
                pid = row[col]
                if pd.notna(pid):
                    pid = int(pid)
                    player_minutes[pid] = player_minutes.get(pid, 0) + row["seconds"] / 60.0

    # Build results DataFrame
    results = []
    for i, pid in enumerate(player_list):
        info = player_info.get(pid, {})
        minutes = player_minutes.get(pid, 0)
        if minutes < args.min_minutes:
            continue
        results.append({
            "player_id": pid,
            "player_name": info.get("name", f"Player {pid}"),
            "team_id": info.get("team_id", 0),
            "team_abbr": TEAM_ID_TO_ABBR.get(info.get("team_id", 0), "???"),
            "minutes": round(minutes, 1),
            "rapm": round(coefficients[i], 2),
        })

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values("rapm", ascending=False)

    # Save results
    output_path = DATA_DIR / f"rapm{suffix}.csv"
    results_df.to_csv(output_path, index=False)
    game_type = "PLAYOFFS" if args.playoffs else "REGULAR SEASON"
    print(f"\nWrote: {output_path} (players={len(results_df)}) [{game_type}]")

    # Print top and bottom players
    adj_label = "3PT-ADJUSTED " if use_adjusted else ""
    playoff_label = "PLAYOFF " if args.playoffs else ""
    print("\n" + "="*60)
    print(f"TOP 20 PLAYERS BY {playoff_label}{adj_label}RAPM")
    print("="*60)
    for _, row in results_df.head(20).iterrows():
        name = row['player_name'].encode('ascii', 'replace').decode('ascii')
        print(f"{name:25s} {row['team_abbr']:4s} {row['minutes']:6.0f} min  {row['rapm']:+6.2f}")

    print("\n" + "="*60)
    print(f"BOTTOM 20 PLAYERS BY {playoff_label}{adj_label}RAPM")
    print("="*60)
    for _, row in results_df.tail(20).iterrows():
        name = row['player_name'].encode('ascii', 'replace').decode('ascii')
        print(f"{name:25s} {row['team_abbr']:4s} {row['minutes']:6.0f} min  {row['rapm']:+6.2f}")


if __name__ == "__main__":
    main()
