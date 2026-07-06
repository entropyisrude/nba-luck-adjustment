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
OFFICIAL_BOX = Path(r"C:\Users\Dave\Downloads\nba-boxscore-data\kaggle-traditional\traditional.csv")


def _parse_minutes(v) -> float | None:
    """Official MIN comes as '38', '38.5', or '38:24'."""
    if pd.isna(v):
        return None
    s = str(v).strip()
    try:
        if ":" in s:
            mm, ss = s.split(":", 1)
            return int(mm) + int(ss) / 60.0
        return float(s)
    except (TypeError, ValueError):
        return None


def anchor_to_official_pm(onoff: pd.DataFrame) -> pd.DataFrame:
    """Anchor raw plus-minus to the official box scores where available.

    The stint reconstruction misplaces a few points around substitution
    boundaries (official box +/- matched only ~25-40% exactly even after
    chaining stint scores). The official per-player +/- from the Kaggle
    traditional box scores shares our exact game_id/player_id namespace and is
    internally consistent (team sums balance in every game), so where a row
    matches we take:
        on_diff        := official +/-
        off_diff       := official team margin - official +/-
    and carry the possession-model luck adjustment over as a delta:
        on_diff_adj    := on_diff  + (stint on_diff_adj  - stint on_diff)
        off_diff_adj   := off_diff + (stint off_diff_adj - stint off_diff)
    Rows with no official match (e.g. current season not yet in the Kaggle
    dump) keep their stint-based values.
    """
    if not OFFICIAL_BOX.exists():
        print(f"  WARNING: {OFFICIAL_BOX} not found -- skipping official anchoring")
        return onoff

    kag = pd.read_csv(OFFICIAL_BOX, usecols=["gameid", "type", "playerid", "team", "MIN", "PTS", "+/-"],
                      dtype={"gameid": str}, low_memory=False)
    kag = kag[kag["type"].str.lower() == "playoff"].copy()
    kag["player_id"] = pd.to_numeric(kag["playerid"], errors="coerce")
    kag["pm_off"] = pd.to_numeric(kag["+/-"], errors="coerce")
    kag["min_off"] = kag["MIN"].map(_parse_minutes)
    kag = kag.dropna(subset=["player_id", "pm_off"])
    kag["player_id"] = kag["player_id"].astype(int)

    team_pts = kag.groupby(["gameid", "team"], as_index=False)["PTS"].sum()
    game_pts = team_pts.groupby("gameid")["PTS"].transform("sum")
    team_pts["margin_off"] = 2 * team_pts["PTS"] - game_pts  # own - opponent

    kag = kag.merge(team_pts[["gameid", "team", "margin_off"]], on=["gameid", "team"])
    kag = kag[["gameid", "player_id", "pm_off", "margin_off", "min_off"]].rename(columns={"gameid": "game_id"})
    kag = kag.drop_duplicates(subset=["game_id", "player_id"])

    onoff = onoff.merge(kag, on=["game_id", "player_id"], how="left")

    # Secondary source for games newer than the Kaggle dump (current playoffs):
    # the Eoin dataset's official box +/- keyed by (real game date, player id).
    eoin_zip = Path("historical-nba-data-and-player-box-scores.zip")
    if eoin_zip.exists() and onoff["pm_off"].isna().any():
        import zipfile
        with zipfile.ZipFile(eoin_zip) as z:
            with z.open("PlayerStatistics.csv") as f:
                eo = pd.read_csv(f, usecols=["gameId", "gameDateTimeEst", "gameType",
                                             "personId", "points", "plusMinusPoints",
                                             "numMinutes", "playerteamCity", "playerteamName"],
                                 low_memory=False)
        eo = eo[eo["gameType"] == "Playoffs"].copy()
        eo["date"] = pd.to_datetime(eo["gameDateTimeEst"], errors="coerce").dt.strftime("%Y-%m-%d")
        eo["player_id"] = pd.to_numeric(eo["personId"], errors="coerce")
        eo["pm_eoin"] = pd.to_numeric(eo["plusMinusPoints"], errors="coerce")
        eo = eo.dropna(subset=["player_id", "pm_eoin"])
        eo["player_id"] = eo["player_id"].astype(int)
        eo["team_key"] = eo["playerteamCity"].fillna("") + "|" + eo["playerteamName"].fillna("")
        tp = eo.groupby(["gameId", "team_key"], as_index=False)["points"].sum()
        gp = tp.groupby("gameId")["points"].transform("sum")
        tp["margin_eoin"] = 2 * tp["points"] - gp
        eo["min_eoin"] = pd.to_numeric(eo["numMinutes"], errors="coerce")
        eo = eo.merge(tp[["gameId", "team_key", "margin_eoin"]], on=["gameId", "team_key"])
        eo = eo[["date", "player_id", "pm_eoin", "margin_eoin", "min_eoin"]].drop_duplicates(subset=["date", "player_id"])
        onoff["date_str"] = onoff["date"].astype(str).str[:10]
        onoff = onoff.merge(eo, left_on=["date_str", "player_id"],
                            right_on=["date", "player_id"], how="left", suffixes=("", "_eo"))
        fill = onoff["pm_off"].isna() & onoff["pm_eoin"].notna()
        onoff.loc[fill, "pm_off"] = onoff.loc[fill, "pm_eoin"]
        onoff.loc[fill, "margin_off"] = onoff.loc[fill, "margin_eoin"]
        onoff.loc[fill, "min_off"] = onoff.loc[fill, "min_eoin"]
        onoff = onoff.drop(columns=["pm_eoin", "margin_eoin", "min_eoin", "date_str", "date_eo"], errors="ignore")
        print(f"  Eoin secondary anchor: {int(fill.sum())} additional rows")

    hit = onoff["pm_off"].notna()
    print(f"  official anchoring: {hit.sum()}/{len(onoff)} rows matched")

    luck_on = onoff["on_diff_adj"] - onoff["on_diff"]
    luck_off = onoff["off_diff_adj"] - onoff["off_diff"]
    onoff.loc[hit, "on_diff"] = onoff.loc[hit, "pm_off"]
    onoff.loc[hit, "off_diff"] = onoff.loc[hit, "margin_off"] - onoff.loc[hit, "pm_off"]
    onoff.loc[hit, "on_diff_adj"] = onoff.loc[hit, "on_diff"] + luck_on[hit]
    onoff.loc[hit, "off_diff_adj"] = onoff.loc[hit, "off_diff"] + luck_off[hit]
    onoff.loc[hit, "on_off_diff"] = onoff.loc[hit, "on_diff"] - onoff.loc[hit, "off_diff"]
    onoff.loc[hit, "on_off_diff_adj"] = onoff.loc[hit, "on_diff_adj"] - onoff.loc[hit, "off_diff_adj"]

    # Minutes: stint minutes are more precise than the (often whole-number)
    # official MIN, so only take the official value when the stint total is
    # clearly wrong (e.g. games whose stint data dropped an OT period).
    fix_min = hit & onoff["min_off"].notna() & ((onoff["minutes_on"] - onoff["min_off"]).abs() > 1.5)
    if fix_min.any():
        print(f"  minutes repaired from official box: {int(fix_min.sum())} rows")
        onoff.loc[fix_min, "minutes_on"] = onoff.loc[fix_min, "min_off"]
    return onoff.drop(columns=["pm_off", "margin_off", "min_off"])


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


def is_full_name(name) -> bool:
    """Check if name appears to be a full name (has space and multiple parts)."""
    if not name or not isinstance(name, str):
        return False
    if name.startswith("Player "):
        return False
    parts = name.strip().split()
    return len(parts) >= 2


def get_player_names(player_ids: set, stints: pd.DataFrame) -> dict:
    """Get player names from various sources, prioritizing full names from historical PBP."""
    player_names = {}
    needed_ids = set(player_ids)

    # First, scan historical PBP files for full names (these have "First Last" format)
    print("Loading player names from historical PBP...")
    historical_dir = DATA_DIR / "historical_pbp"
    if historical_dir.exists():
        for pbp_file in sorted(historical_dir.glob("nbastats_po_*.csv")):
            if not needed_ids:
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
                        name = str(row[name_col])
                        if pid in needed_ids and is_full_name(name):
                            if pid not in player_names or not is_full_name(player_names[pid]):
                                player_names[pid] = name
                                needed_ids.discard(pid)
            except Exception:
                pass

    found_full = sum(1 for n in player_names.values() if is_full_name(n))
    print(f"  Found {found_full}/{len(player_ids)} players with full names from historical PBP")

    # Fallback to regular season on/off for any remaining players
    missing = player_ids - set(player_names.keys())
    if missing:
        for fallback_path in [DATA_DIR / "player_boxscore_stats.csv", DATA_DIR / "adjusted_onoff.csv"]:
            if not fallback_path.exists() or not missing:
                continue
            df = pd.read_csv(fallback_path, dtype={"player_id": int}, low_memory=False)
            if "player_id" not in df.columns or "player_name" not in df.columns:
                continue
            for _, row in df.drop_duplicates(subset=["player_id"]).iterrows():
                pid = int(row["player_id"])
                if pid in missing:
                    player_names[pid] = row.get("player_name", f"Player {pid}")
                    missing.discard(pid)

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

    # Preserve games that exist only in the current CSV, not in stint data
    # (recent games appended from possessions by
    # scripts/append_finals_onoff_from_possessions.py).
    out_path = DATA_DIR / "adjusted_onoff_playoffs.csv"
    if out_path.exists():
        existing = pd.read_csv(out_path, dtype={"game_id": str})
        extra = existing[~existing["game_id"].isin(set(onoff["game_id"]))]
        if len(extra):
            print(f"  preserving {len(extra)} rows from {extra['game_id'].nunique()} "
                  f"games not present in stint data")
            onoff = pd.concat([onoff, extra[cols]], ignore_index=True)

    # Anchor raw plus-minus to official box scores (see docstring)
    print("\nAnchoring to official box plus-minus...")
    onoff['game_id'] = onoff['game_id'].astype(str)
    onoff = anchor_to_official_pm(onoff)

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
