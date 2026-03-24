
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
BBREF_PATH = DATA_DIR / "bbref_advanced_2526.csv"
TEAM_GP_PATH = DATA_DIR / "bbref_team_gp_2526.csv"
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
    print("Building Award Eligibility Report (Sorted by VORP)...")
    
    game_minutes = pd.DataFrame(columns=['player_id', 'game_id', 'minutes'])
    processed_gids = set()
    last_date = "2025-10-21"

    if STINTS_PATH.exists() and os.path.getsize(STINTS_PATH) > 500:
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

    if CACHE_PATH.exists():
        with open(CACHE_PATH, 'r') as f:
            cache_ids = json.load(f)
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
        print("No game data found.")
        return

    g20 = game_minutes[game_minutes['minutes'] >= 20].groupby('player_id').size().rename('games_20')
    g15 = game_minutes[(game_minutes['minutes'] >= 15) & (game_minutes['minutes'] < 20)].groupby('player_id').size().rename('games_15_20')
    gp = game_minutes[game_minutes['minutes'] > 0].groupby('player_id').size().rename('total_gp')

    report = pd.DataFrame(index=game_minutes['player_id'].unique())
    report = report.join(g20).join(g15).join(gp).fillna(0)
    report['eligible_games'] = report['games_20'] + report['games_15_20'].clip(upper=2)
    report['need_to_play'] = (65 - report['eligible_games']).clip(lower=0)
    
    # 5. Load Names and BBRef Data (VORP + Team GP)
    final = pd.DataFrame()
    team_rem_map = {}
    if TEAM_GP_PATH.exists():
        team_gp = pd.read_csv(TEAM_GP_PATH)
        for _, row in team_gp.iterrows():
            team_rem_map[row['team_abbr']] = max(0, 82 - int(row['team_gp']))

    if PLAYER_MAP_PATH.exists():
        player_map = pd.read_csv(PLAYER_MAP_PATH)[['player_name', 'player_id']]
        final = player_map.merge(report, left_on='player_id', right_index=True)
        
        if BBREF_PATH.exists():
            bbref = pd.read_csv(BBREF_PATH)
            final = final.merge(bbref[['player_name', 'Team', 'VORP', 'BPM']], on='player_name', how='left')
    
    if final.empty:
        return

    final['VORP'] = final['VORP'].fillna(-1.0)
    final['G_Rem'] = final['Team'].map(team_rem_map).fillna(0).astype(int)
    
    generate_dashboard(final.sort_values('VORP', ascending=False), last_date)

def generate_dashboard(df, last_date):
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>NBA 65-Game Tracker | EntropyIsRude</title>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
    <style>
        body {{ font-family: -apple-system, system-ui, sans-serif; background: #f4f4f9; color: #333; margin: 0; padding: 20px; }}
        .header {{ background: #003366; color: white; padding: 30px; border-radius: 12px; text-align: center; margin-bottom: 25px; }}
        .table-container {{ background: white; padding: 20px; border-radius: 12px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }}
        .player-name {{ font-weight: 700; color: #003366; }}
        .vorp-val {{ font-weight: 800; color: #d41111; }}
        .progress-box {{ width: 100px; background: #eee; height: 8px; border-radius: 4px; overflow: hidden; margin-top: 4px; }}
        .progress-fill {{ height: 100%; background: #27ae60; }}
        .bg-eliminated {{ color: #c0392b; font-weight: bold; font-size: 0.85em; }}
        .bg-bubble {{ color: #f39c12; font-weight: bold; font-size: 0.85em; }}
        .bg-clinched {{ color: #27ae60; font-weight: bold; font-size: 0.85em; }}
    </style>
</head>
<body>
    <div class="header">
        <h1 style="margin:0;">Award Eligibility: The 65-Game Tracker</h1>
        <p>Sorted by <strong>BBRef VORP</strong> (2025-26 Season)</p>
        <p>Data through: <strong>{last_date}</strong> | <a href="index.html" style="color: #4db8ff;">Back Home</a></p>
    </div>

    <div class="table-container">
        <table id="tracker" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>Player</th>
                    <th>VORP</th>
                    <th>Team</th>
                    <th>Eligible / 65</th>
                    <th>Needs (20m)</th>
                    <th>Team G Rem</th>
                    <th>Status</th>
                    <th>BPM</th>
                </tr>
            </thead>
            <tbody>
"""
    for _, r in df.iterrows():
        if r['VORP'] < 0.1 and r['eligible_games'] < 40: continue
        
        perc = (r['eligible_games'] / 65) * 100
        
        # Status Logic
        if r['eligible_games'] >= 65:
            status = "CLINCHED"
            status_cls = "bg-clinched"
        elif (r['eligible_games'] + r['G_Rem']) < 65:
            status = "ELIMINATED"
            status_cls = "bg-eliminated"
        else:
            status = "BUBBLE"
            status_cls = "bg-bubble"

        html += f"""
                <tr>
                    <td><div class="player-name">{r['player_name']}</div></td>
                    <td class="vorp-val">{r['VORP']:.1f}</td>
                    <td>{r['Team']}</td>
                    <td>
                        {int(r['eligible_games'])}
                        <div class="progress-box"><div class="progress-fill" style="width: {min(100, perc)}%"></div></div>
                    </td>
                    <td style="font-weight: bold; color: {'#c0392b' if r['need_to_play'] > r['G_Rem'] else '#333'}">{int(r['need_to_play'])}</td>
                    <td>{int(r['G_Rem'])}</td>
                    <td><span class="{status_cls}">{status}</span></td>
                    <td>{r['BPM']:.1f}</td>
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
                order: [[1, 'desc']]
            });
        });
    </script>
</body>
</html>
"""
    with open(OUTPUT_HTML, "w", encoding='utf-8') as f:
        f.write(html)
    print(f"Dashboard updated with Team G Rem: {OUTPUT_HTML}")

if __name__ == "__main__":
    build_daily_report()
