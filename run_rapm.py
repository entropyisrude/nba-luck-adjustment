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


def build_design_matrix_drapm(stints: pd.DataFrame, use_adjusted: bool = True):
    """
    Build design matrix for Defensive RAPM (DRAPM).

    Similar to ORAPM but targets points ALLOWED (negated so positive = good defense).
    """
    player_list, player_to_idx = get_player_list_and_index(stints)
    n_players = len(player_list)
    n_stints = len(stints)
    n_rows = n_stints * 2

    print(f"Building DRAPM design matrix: {n_stints} stints -> {n_rows} rows, {n_players} players")

    row_indices = []
    col_indices = []
    data = []

    for stint_idx, row in enumerate(stints.itertuples()):
        # Row 2*stint_idx: Home team defense (target = -away_pts, so positive = good)
        for col in ["home_p1", "home_p2", "home_p3", "home_p4", "home_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(2 * stint_idx)
                    col_indices.append(player_to_idx[pid])
                    data.append(1.0)
        for col in ["away_p1", "away_p2", "away_p3", "away_p4", "away_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(2 * stint_idx)
                    col_indices.append(player_to_idx[pid])
                    data.append(-1.0)

        # Row 2*stint_idx+1: Away team defense (target = -home_pts)
        for col in ["away_p1", "away_p2", "away_p3", "away_p4", "away_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(2 * stint_idx + 1)
                    col_indices.append(player_to_idx[pid])
                    data.append(1.0)
        for col in ["home_p1", "home_p2", "home_p3", "home_p4", "home_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(2 * stint_idx + 1)
                    col_indices.append(player_to_idx[pid])
                    data.append(-1.0)

    X = sparse.csr_matrix((data, (row_indices, col_indices)), shape=(n_rows, n_players))

    possessions = stints["seconds"].values / 24.0
    possessions = np.maximum(possessions, 0.1)

    if use_adjusted:
        home_pts = stints["home_pts_adj"].values
        away_pts = stints["away_pts_adj"].values
    else:
        home_pts = stints["home_pts"].values
        away_pts = stints["away_pts"].values

    # Target: NEGATIVE points allowed per 100 possessions (so positive = good defense)
    y = np.zeros(n_rows)
    y[0::2] = (-away_pts / possessions) * 100.0  # Home defense (negative of away offense)
    y[1::2] = (-home_pts / possessions) * 100.0  # Away defense (negative of home offense)

    weights = np.zeros(n_rows)
    weights[0::2] = np.sqrt(possessions)
    weights[1::2] = np.sqrt(possessions)

    return X, y, weights, player_list, player_to_idx


def build_design_matrix_possession_od(possessions: pd.DataFrame, use_adjusted: bool = True):
    """
    Build unified O/D design matrix from possession-level data.

    This is the proper RAPM formulation where:
    - Each possession is one observation
    - Design matrix has 2*n_players columns (first half for offense, second half for defense)
    - Offensive players get +1 in their offensive column
    - Defensive players get -1 in their defensive column
    - Target is points scored on that possession (per-100 scaled)

    This allows simultaneous estimation of ORAPM and DRAPM with implicit opponent adjustment.
    """
    # Get all unique player IDs from both offense and defense columns
    off_cols = ["off_p1", "off_p2", "off_p3", "off_p4", "off_p5"]
    def_cols = ["def_p1", "def_p2", "def_p3", "def_p4", "def_p5"]

    all_players = set()
    for col in off_cols + def_cols:
        all_players.update(possessions[col].dropna().astype(int).unique())

    player_list = sorted(all_players)
    player_to_idx = {pid: idx for idx, pid in enumerate(player_list)}
    n_players = len(player_list)
    n_poss = len(possessions)

    print(f"Building unified O/D design matrix: {n_poss} possessions, {n_players} players, {2*n_players} columns")

    row_indices = []
    col_indices = []
    data = []

    for poss_idx, row in enumerate(possessions.itertuples()):
        # Offensive players: +1 in their offensive column (first half of matrix)
        for col in off_cols:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(poss_idx)
                    col_indices.append(player_to_idx[pid])  # Offensive column
                    data.append(1.0)

        # Defensive players: -1 in their defensive column (second half of matrix)
        for col in def_cols:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(poss_idx)
                    col_indices.append(n_players + player_to_idx[pid])  # Defensive column
                    data.append(-1.0)

    X = sparse.csr_matrix((data, (row_indices, col_indices)), shape=(n_poss, 2 * n_players))

    # Target: points scored on possession, scaled to per-100
    if use_adjusted:
        y = possessions["points_adj"].values.astype(float) * 100.0
    else:
        y = possessions["points"].values.astype(float) * 100.0

    # Equal weights for all possessions
    weights = np.ones(n_poss)

    return X, y, weights, player_list, player_to_idx, n_players


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


def run_rapm_od(X, y, weights, n_players: int, alpha_off: float = 2500.0, alpha_def: float = 2500.0):
    """
    Run ridge regression with separate regularization for O and D coefficients.

    X has 2*n_players columns: first half for offense, second half for defense.
    """
    # Build regularization vector with different alphas for O and D
    alphas = np.concatenate([
        np.full(n_players, alpha_off),
        np.full(n_players, alpha_def)
    ])

    # Apply weights
    X_weighted = X.multiply(weights[:, np.newaxis])
    y_weighted = y * weights

    # Ridge regression with per-feature regularization
    # Using sklearn's Ridge with a diagonal regularization matrix equivalent
    model = Ridge(alpha=1.0, fit_intercept=True)

    # Scale columns by 1/sqrt(alpha) to achieve per-feature regularization
    scale = 1.0 / np.sqrt(alphas)
    X_scaled = X_weighted.multiply(scale)

    model.fit(X_scaled, y_weighted)

    # Unscale coefficients
    coef = model.coef_ * scale

    coef_off = coef[:n_players]
    coef_def = coef[n_players:]

    return coef_off, coef_def, model.intercept_


def get_player_info(player_ids: list[int], stints: pd.DataFrame, suffix: str = "") -> dict:
    """Get player names and teams from historical PBP (full names) and onoff data."""
    player_info = {}
    needed_ids = set(player_ids)

    def is_full_name(name) -> bool:
        """Check if name appears to be a full name (has space and multiple parts)."""
        if not name or not isinstance(name, str):
            return False
        if name.startswith("Player "):
            return False
        parts = name.strip().split()
        return len(parts) >= 2

    # First, scan historical PBP files for full names (these have "First Last" format)
    print("Loading player names from historical PBP...")
    historical_dir = DATA_DIR / "historical_pbp"
    if historical_dir.exists():
        for pbp_file in sorted(historical_dir.glob("nbastats_po_*.csv")):
            try:
                pbp = pd.read_csv(pbp_file, usecols=[
                    "PLAYER1_ID", "PLAYER1_NAME", "PLAYER1_TEAM_ID",
                    "PLAYER2_ID", "PLAYER2_NAME", "PLAYER2_TEAM_ID",
                    "PLAYER3_ID", "PLAYER3_NAME", "PLAYER3_TEAM_ID",
                ])
                for player_col, name_col, team_col in [
                    ("PLAYER1_ID", "PLAYER1_NAME", "PLAYER1_TEAM_ID"),
                    ("PLAYER2_ID", "PLAYER2_NAME", "PLAYER2_TEAM_ID"),
                    ("PLAYER3_ID", "PLAYER3_NAME", "PLAYER3_TEAM_ID"),
                ]:
                    subset = pbp[[player_col, name_col, team_col]].dropna(subset=[player_col, name_col])
                    subset = subset.drop_duplicates(subset=[player_col])
                    for _, row in subset.iterrows():
                        pid = int(row[player_col])
                        if pid in needed_ids:
                            name = str(row[name_col])
                            # Only update if we don't have a full name yet
                            if pid not in player_info or not is_full_name(player_info[pid].get("name", "")):
                                if is_full_name(name):
                                    player_info[pid] = {
                                        "name": name,
                                        "team_id": int(row[team_col]) if pd.notna(row[team_col]) else 0,
                                    }
            except Exception as e:
                print(f"Warning: Error reading {pbp_file}: {e}")

    # Then load from onoff data for any still missing or to get better names
    for path in [DATA_DIR / "adjusted_onoff.csv", DATA_DIR / f"adjusted_onoff{suffix}.csv"]:
        if not path.exists():
            continue
        onoff = pd.read_csv(path, dtype={"player_id": int})
        onoff = onoff.sort_values("date").drop_duplicates(subset=["player_id"], keep="last")
        for _, row in onoff.iterrows():
            pid = int(row["player_id"])
            if pid not in needed_ids:
                continue
            name = row.get("player_name")
            if pd.isna(name):
                name = f"Player {pid}"
            else:
                name = str(name)
            team_id = int(row.get("team_id", 0)) if pd.notna(row.get("team_id")) else 0
            # Only use if we don't have info yet, or if this is a fuller name
            if pid not in player_info:
                player_info[pid] = {"name": name, "team_id": team_id}
            elif is_full_name(name) and not is_full_name(player_info[pid].get("name", "")):
                player_info[pid]["name"] = name
                player_info[pid]["team_id"] = team_id

    # Fill in any still missing
    for pid in needed_ids:
        if pid not in player_info:
            player_info[pid] = {"name": f"Player {pid}", "team_id": 0}

    found_full = sum(1 for p in player_info.values() if is_full_name(p.get("name", "")))
    print(f"  Found {found_full}/{len(needed_ids)} players with full names")

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

    # Calculate minutes per player and per player-team from stint data
    player_minutes = {}
    player_team_minutes = {}  # {player_id: {team_id: minutes}}
    for col_set, team_col in [(["home_p1", "home_p2", "home_p3", "home_p4", "home_p5"], "home_id"),
                               (["away_p1", "away_p2", "away_p3", "away_p4", "away_p5"], "away_id")]:
        for col in col_set:
            for _, row in stints.iterrows():
                pid = row[col]
                if pd.notna(pid):
                    pid = int(pid)
                    mins = row["seconds"] / 60.0
                    player_minutes[pid] = player_minutes.get(pid, 0) + mins
                    # Track by team
                    team_id = int(row[team_col])
                    if pid not in player_team_minutes:
                        player_team_minutes[pid] = {}
                    player_team_minutes[pid][team_id] = player_team_minutes[pid].get(team_id, 0) + mins

    # Build results DataFrame
    results = []
    for i, pid in enumerate(player_list):
        info = player_info.get(pid, {})
        minutes = player_minutes.get(pid, 0)
        if minutes < args.min_minutes:
            continue
        # Use team where player has most minutes
        team_mins = player_team_minutes.get(pid, {})
        if team_mins:
            primary_team_id = max(team_mins.keys(), key=lambda t: team_mins[t])
        else:
            primary_team_id = info.get("team_id", 0)
        results.append({
            "player_id": pid,
            "player_name": info.get("name", f"Player {pid}"),
            "team_id": primary_team_id,
            "team_abbr": TEAM_ID_TO_ABBR.get(primary_team_id, "???"),
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
