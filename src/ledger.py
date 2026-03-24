
import pandas as pd
import os
import json
import re
from pathlib import Path

LEDGER_PATH = Path("data/master_boxscore_2526.csv")

def update_master_ledger(game_id, player_df, team_df, date_str):
    """
    Ensures every game found in the daily fetch is recorded in a flat,
    non-scrupulous ledger for counting wins and eligibility.
    """
    if not LEDGER_PATH.parent.exists():
        LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)

    # 1. Prepare Player-Game Rows
    # player_df columns usually: ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'MIN', 'PTS', 'FG3A', 'FG3M', ...]
    rows = []
    
    # We also need to know if the team WON this game
    home_tid = team_df.iloc[0]['TEAM_ID']
    away_tid = team_df.iloc[1]['TEAM_ID']
    home_pts = team_df.iloc[0]['PTS']
    away_pts = team_df.iloc[1]['PTS']
    
    winning_team = home_tid if home_pts > away_pts else away_tid

    for _, p in player_df.iterrows():
        # Parse minutes (handle '24:00' or 'PT24M00.00S')
        min_str = str(p['MIN'])
        total_min = 0
        if 'PT' in min_str:
            m = re.search(r'PT(\d+)M', min_str)
            s = re.search(r'M([\d.]+)S', min_str)
            if m: total_min += int(m.group(1))
            if s: total_min += float(s.group(1)) / 60.0
        elif ':' in min_str:
            parts = min_str.split(':')
            total_min = int(parts[0]) + int(parts[1])/60.0
        else:
            try: total_min = float(min_str)
            except: total_min = 0

        rows.append({
            'date': date_str,
            'game_id': str(game_id).lstrip('0'),
            'player_id': p['PLAYER_ID'],
            'player_name': p['PLAYER_NAME'],
            'team_id': p['TEAM_ID'],
            'team_abbr': p['TEAM_ABBREVIATION'],
            'minutes': round(total_min, 2),
            'pts': p['PTS'],
            'fg3a': p['FG3A'],
            'is_win': 1 if p['TEAM_ID'] == winning_team else 0
        })

    new_data = pd.DataFrame(rows)

    if LEDGER_PATH.exists():
        existing = pd.read_csv(LEDGER_PATH, dtype={'game_id': str})
        existing['game_id'] = existing['game_id'].astype(str).str.lstrip('0')
        # Combine and deduplicate
        combined = pd.concat([existing, new_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=['game_id', 'player_id'], keep='last')
    else:
        combined = new_data

    combined.sort_values(['date', 'game_id'], ascending=[False, False], inplace=True)
    combined.to_csv(LEDGER_PATH, index=False)
    print(f"  [LEDGER] Updated with game {game_id} ({len(new_data)} players)")
