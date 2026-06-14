"""
Compute on/off stats for Finals games (42500401-42500405) from possessions_playoffs.csv
and append them to adjusted_onoff_playoffs.csv.

These games are missing from adjusted_onoff_playoffs.csv because stints_playoffs.csv
doesn't yet include Finals data. We derive equivalent stats from the possession-level data.
"""

import re
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

FINALS_IDS = ["42500401", "42500402", "42500403", "42500404", "42500405"]

OFF_COLS = ["off_p1", "off_p2", "off_p3", "off_p4", "off_p5"]
DEF_COLS = ["def_p1", "def_p2", "def_p3", "def_p4", "def_p5"]


def parse_minutes(s) -> float:
    if pd.isna(s):
        return 0.0
    m = re.match(r"PT(?:(\d+)M)?(?:([\d.]+)S)?", str(s))
    if not m:
        return 0.0
    mins = int(m.group(1) or 0)
    secs = float(m.group(2) or 0)
    return mins + secs / 60.0


def main():
    # Load Finals possessions
    poss = pd.read_csv(DATA_DIR / "possessions_playoffs.csv", low_memory=False)
    poss["game_id"] = poss["game_id"].astype(str)
    finals_poss = poss[poss["game_id"].isin(FINALS_IDS)].copy()
    print(f"Finals possessions: {len(finals_poss)} across {finals_poss['game_id'].nunique()} games")

    # Load box scores for minutes and player names
    box = pd.read_csv(DATA_DIR / "player_boxscore_stats.csv", low_memory=False)
    box["game_id"] = box["game_id"].astype(str)
    finals_box = box[box["game_id"].isin(FINALS_IDS)].copy()
    finals_box["minutes_parsed"] = finals_box["minutes"].apply(parse_minutes)
    finals_box["player_id"] = finals_box["player_id"].astype(int)
    print(f"Finals box rows: {len(finals_box)}")

    # Build player_name lookup
    player_names: dict[int, str] = {}
    for _, row in finals_box.iterrows():
        pid = int(row["player_id"])
        if pid not in player_names and pd.notna(row.get("player_name")):
            player_names[pid] = str(row["player_name"])

    # Also pull names from existing adjusted_onoff_playoffs for any gaps
    existing = pd.read_csv(DATA_DIR / "adjusted_onoff_playoffs.csv", dtype={"game_id": str})
    for _, row in existing.drop_duplicates("player_id").iterrows():
        pid = int(row["player_id"])
        if pid not in player_names and pd.notna(row.get("player_name")):
            player_names[pid] = str(row["player_name"])

    results = []

    for game_id in FINALS_IDS:
        game_poss = finals_poss[finals_poss["game_id"] == game_id]
        if len(game_poss) == 0:
            print(f"  WARNING: no possessions for {game_id}")
            continue

        game_date = game_poss["date"].iloc[0]
        teams = set(game_poss["offense_team"].unique()) | set(game_poss["defense_team"].unique())

        for team_id in teams:
            team_on_offense = game_poss[game_poss["offense_team"] == team_id]
            team_on_defense = game_poss[game_poss["defense_team"] == team_id]

            # All players who appeared in either role for this team
            players: set[int] = set()
            for col in OFF_COLS:
                players.update(team_on_offense[col].dropna().astype(int).unique())
            for col in DEF_COLS:
                players.update(team_on_defense[col].dropna().astype(int).unique())

            for player_id in players:
                on_off_mask = pd.Series(False, index=team_on_offense.index)
                for col in OFF_COLS:
                    on_off_mask |= (team_on_offense[col] == player_id)

                on_def_mask = pd.Series(False, index=team_on_defense.index)
                for col in DEF_COLS:
                    on_def_mask |= (team_on_defense[col] == player_id)

                on_offense = team_on_offense[on_off_mask]
                off_offense = team_on_offense[~on_off_mask]
                on_defense = team_on_defense[on_def_mask]
                off_defense = team_on_defense[~on_def_mask]

                on_pts_for = float(on_offense["points"].sum())
                on_pts_for_adj = float(on_offense["points_adj"].sum())
                off_pts_for = float(off_offense["points"].sum())
                off_pts_for_adj = float(off_offense["points_adj"].sum())

                on_pts_against = float(on_defense["points"].sum())
                on_pts_against_adj = float(on_defense["points_adj"].sum())
                off_pts_against = float(off_defense["points"].sum())
                off_pts_against_adj = float(off_defense["points_adj"].sum())

                on_diff = on_pts_for - on_pts_against
                off_diff = off_pts_for - off_pts_against
                on_diff_adj = on_pts_for_adj - on_pts_against_adj
                off_diff_adj = off_pts_for_adj - off_pts_against_adj

                # Minutes from box score
                player_box = finals_box[
                    (finals_box["game_id"] == game_id) & (finals_box["player_id"] == player_id)
                ]
                if len(player_box) > 0:
                    minutes_on = float(player_box["minutes_parsed"].iloc[0])
                else:
                    # Estimate: (on-court possessions / total game possessions) * 48
                    total_on = len(on_offense) + len(on_defense)
                    total_poss = len(game_poss)
                    minutes_on = round((total_on / total_poss) * 48.0, 2) if total_poss > 0 else 0.0

                results.append({
                    "game_id": game_id,
                    "team_id": int(team_id),
                    "player_id": int(player_id),
                    "player_name": player_names.get(int(player_id), f"Player {player_id}"),
                    "on_pts_for": on_pts_for,
                    "on_pts_against": on_pts_against,
                    "on_diff": on_diff,
                    "off_pts_for": off_pts_for,
                    "off_pts_against": off_pts_against,
                    "off_diff": off_diff,
                    "on_pts_for_adj": on_pts_for_adj,
                    "on_pts_against_adj": on_pts_against_adj,
                    "on_diff_adj": on_diff_adj,
                    "off_pts_for_adj": off_pts_for_adj,
                    "off_pts_against_adj": off_pts_against_adj,
                    "off_diff_adj": off_diff_adj,
                    "on_off_diff": on_diff - off_diff,
                    "on_off_diff_adj": on_diff_adj - off_diff_adj,
                    "minutes_on": minutes_on,
                    "date": game_date,
                })

    new_rows = pd.DataFrame(results)
    print(f"\nGenerated {len(new_rows)} player-game records for Finals games")
    print(new_rows.groupby("game_id").size().rename("players").to_string())

    # Remove any existing Finals rows then append
    existing_clean = existing[~existing["game_id"].isin(FINALS_IDS)]
    combined = pd.concat([existing_clean, new_rows], ignore_index=True)
    combined = combined.sort_values(["date", "game_id", "team_id", "player_name"])

    out_path = DATA_DIR / "adjusted_onoff_playoffs.csv"
    combined.to_csv(out_path, index=False)
    print(f"\nWrote {len(combined)} total records to {out_path}")
    print(f"Date range: {combined['date'].min()} → {combined['date'].max()}")
    print(f"Unique games: {combined['game_id'].nunique()}")


if __name__ == "__main__":
    main()
