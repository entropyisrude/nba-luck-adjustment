
import pandas as pd
import numpy as np
import requests
import json
import os
import re
from pathlib import Path

# CONFIGURATION (Paths relative to repo root)
DATA_DIR = Path("data")
STINTS_PATH = DATA_DIR / "stints.csv"
EMV_PATH = DATA_DIR / "unified_2526_results.csv"
PLAYER_MAP_PATH = DATA_DIR / "player_totals_2025_26.csv"
CACHE_PATH = DATA_DIR / "cdn_boxscore_cache.json"
OUTPUT_HTML = "65-game-tracker.html"

def get_game_minutes_from_cdn(game_id):
    url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            minutes_map = {}
            for team_key in ['homeTeam', 'awayTeam']:
                for p in data['game'][team_key]['players']:
                    pid = p['personId']
                    s = p['statistics']
                    m = re.search(r'PT(\d+)M', s['minutes'])
                    s_sec = re.search(r'M([\d.]+)S', s['minutes'])
                    total_min = 0
                    if m: total_min += int(m.group(1))
                    if s_sec: total_min += float(s_sec.group(1)) / 60.0
                    if total_min > 0:
                        minutes_map[pid] = total_min
            return minutes_map
    except:
        pass
    return None

def build_daily_report():
    print("Building Award Eligibility Report...")
    
    # 1. Start with an empty game_minutes dataframe
    game_minutes = pd.DataFrame(columns=['player_id', 'game_id', 'minutes'])
    processed_gids = set()
    last_date = "2025-10-21"

    # 2. Try to load from Stints if available
    if STINTS_PATH.exists() and os.getsize(STINTS_PATH) > 500:
        print(f"Loading base data from {STINTS_PATH}...")
        try:
            stints = pd.read_csv(STINTS_PATH)
            stints_2526 = stints[stints['game_id'].astype(str).str.contains('225')].copy()
            if not stints_2526.empty:
                player_cols = [f'home_p{i}' for i in range(1, 6)] + [f'away_p{i}' for i in range(1, 6)]
                present_cols = [c for c in player_cols if c in stints_2526.columns]
                stint_melt = stints_2526.melt(id_vars=['game_id', 'seconds'], value_vars=present_cols, value_name='player_id')
                game_minutes = stint_melt.groupby(['player_id', 'game_id'])['seconds'].sum().reset_index()
                game_minutes['minutes'] = game_minutes['seconds'] / 60.0
                processed_gids = set(stints_2526['game_id'].unique())
                last_date = stints_2526['date'].max()
        except Exception as e:
            print(f"Warning: Could not process stints.csv: {e}")

    # 3. Check for Live Games in CDN cache
    if CACHE_PATH.exists():
        with open(CACHE_PATH, 'r') as f:
            cache_ids = json.load(f)
            # Filter for 25-26 season
            all_cached_ids = [int(gid) for gid in cache_ids if str(gid).endswith('225') or str(gid).startswith('00225')]
        
        new_game_ids = [gid for gid in all_cached_ids if gid not in processed_gids]
        if new_game_ids:
            print(f"Found {len(new_game_ids)} new games in cache. Fetching minutes...")
            new_rows = []
            for gid in new_game_ids:
                m_map = get_game_minutes_from_cdn(str(gid).zfill(10))
                if m_map:
                    for pid, mins in m_map.items():
                        new_rows.append({'player_id': pid, 'game_id': gid, 'minutes': mins})
            if new_rows:
                game_minutes = pd.concat([game_minutes, pd.DataFrame(new_rows)], ignore_index=True)

    if game_minutes.empty:
        print("No game data found. Skipping HTML generation.")
        return

    # 4. Logic
    g20 = game_minutes[game_minutes['minutes'] >= 20].groupby('player_id').size().rename('games_20')
    g15 = game_minutes[(game_minutes['minutes'] >= 15) & (game_minutes['minutes'] < 20)].groupby('player_id').size().rename('games_15_20')
    gp = game_minutes[game_minutes['minutes'] > 0].groupby('player_id').size().rename('total_gp')

    # Calculate eligibility
    report = pd.DataFrame(index=game_minutes['player_id'].unique())
    report = report.join(g20).join(g15).join(gp).fillna(0)
    report['eligible_games'] = report['games_20'] + report['games_15_20'].clip(upper=2)
    
    # Estimate Team Games (assume max games played by any player on team-ish)
    # Since we don't have team_gp reliably in GH Action if stints are missing, we'll estimate
    # or better: we'll assume 82 total games and calculate remainder based on current date
    # But for now, let's just use 65 - eligible.
    report['need_to_play'] = (65 - report['eligible_games']).clip(lower=0)
    
    # 5. Load Player Names
    names_df = pd.DataFrame(columns=['player_name', 'player_id', 'total_emv'])
    if PLAYER_MAP_PATH.exists():
        names_df = pd.read_csv(PLAYER_MAP_PATH)[['player_name', 'player_id']]
        if EMV_PATH.exists():
            emv = pd.read_csv(EMV_PATH)[['player_name', 'total_emv']]
            names_df = names_df.merge(emv, on='player_name', how='left')
    else:
        # Fallback to unique IDs if no map
        names_df['player_id'] = report.index
        names_df['player_name'] = names_df['player_id'].astype(str)
    
    names_df['total_emv'] = names_df['total_emv'].fillna(0)
    final = names_df.merge(report, left_on='player_id', right_index=True)
    
    generate_dashboard(final.sort_values('total_emv', ascending=False), last_date)

def generate_dashboard(df, last_date):
    clinched = len(df[df['eligible_games'] >= 65])
    # Filter candidates for summary
    candidates = df[df['total_emv'] > 1.5]
    eliminated = len(candidates[(candidates['eligible_games'] + 15) < 65]) # Rough estimate for summary
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>NBA 65-Game Tracker | EntropyIsRude</title>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
    <style>
        body {{ font-family: -apple-system, system-ui, sans-serif; background: #f4f4f9; color: #333; margin: 0; padding: 20px; }}
        .header {{ background: #1a1a2e; color: white; padding: 30px; border-radius: 12px; text-align: center; margin-bottom: 25px; }}
        .summary {{ display: flex; gap: 15px; margin-bottom: 25px; }}
        .stat-card {{ background: white; flex: 1; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); text-align: center; border-top: 5px solid #ddd; }}
        .stat-val {{ font-size: 2.5em; font-weight: 800; margin: 5px 0; }}
        .table-container {{ background: white; padding: 20px; border-radius: 12px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }}
        .status-badge {{ padding: 4px 10px; border-radius: 20px; font-size: 0.85em; font-weight: 600; }}
        .bg-clinched {{ background: #eafaf1; color: #27ae60; }}
        .bg-bubble {{ background: #fef9e7; color: #f39c12; }}
        .progress-box {{ width: 100px; background: #eee; height: 8px; border-radius: 4px; overflow: hidden; margin-top: 4px; }}
        .progress-fill {{ height: 100%; background: #27ae60; }}
    </style>
</head>
<body>
    <div class="header">
        <h1 style="margin:0;">NBA Awards: 65-Game Tracker</h1>
        <p>Data through: <strong>{last_date}</strong></p>
        <p><a href="index.html" style="color: #4db8ff;">&larr; Back to Luck-Adjusted Standings</a></p>
    </div>

    <div class="table-container">
        <table id="tracker" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>Player</th>
                    <th>Eligible / 65</th>
                    <th>Needs (20m)</th>
                    <th>20m Games</th>
                    <th>15-20m Games</th>
                    <th>Total EMV</th>
                </tr>
            </thead>
            <tbody>
"""
    for _, r in df.iterrows():
        if r['total_emv'] < 0.5 and r['eligible_games'] < 30: continue
        
        perc = (r['eligible_games'] / 65) * 100
        html += f"""
                <tr>
                    <td><strong>{r['player_name']}</strong></td>
                    <td>
                        {int(r['eligible_games'])}
                        <div class="progress-box"><div class="progress-fill" style="width: {min(100, perc)}%"></div></div>
                    </td>
                    <td style="font-weight: bold;">{int(r['need_to_play'])}</td>
                    <td>{int(r['games_20'])}</td>
                    <td>{int(r['games_15_20'])}</td>
                    <td>{r['total_emv']:.2f}</td>
                </tr>
        """

    html += """
            </tbody>
        </table>
    </div>
    <script src="https://code.jquery.com/jquery-3.7.0.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
    <script>
        $(document).ready(function() {
            $('#tracker').DataTable({
                pageLength: 50,
                order: [[5, 'desc']]
            });
        });
    </script>
</body>
</html>
"""
    with open(OUTPUT_HTML, "w", encoding='utf-8') as f:
        f.write(html)
    print(f"Dashboard updated: {OUTPUT_HTML}")

if __name__ == "__main__":
    build_daily_report()
