
import pandas as pd
import numpy as np
import requests
import json
import os
import re
from pathlib import Path

# CONFIGURATION
DATA_DIR = Path("data")
STINTS_PATH = DATA_DIR / "stints.csv"
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
    print("Building High-Accuracy Award Eligibility Report...")
    
    # 1. Load Authoritative BBRef Data
    if not BBREF_PATH.exists():
        print("BBRef data missing. Run fetch_bbref_advanced.py first.")
        return
    
    bbref = pd.read_csv(BBREF_PATH)
    team_gp_df = pd.read_csv(TEAM_GP_PATH)
    team_rem_map = {row['team_abbr']: max(0, 82 - int(row['team_gp'])) for _, row in team_gp_df.iterrows()}
    
    # 2. Load Local Minutes for "Low-Minute" cross-check
    # We want to find games where we HAVE proof the player played < 20 mins
    low_min_games = {} # player_id -> list of games < 20 mins
    
    game_minutes = pd.DataFrame()
    if STINTS_PATH.exists() and os.path.getsize(STINTS_PATH) > 500:
        try:
            stints = pd.read_csv(STINTS_PATH)
            stints_2526 = stints[stints['game_id'].astype(str).str.contains('225')].copy()
            player_cols = [f'home_p{i}' for i in range(1, 6)] + [f'away_p{i}' for i in range(1, 6)]
            present_cols = [c for c in player_cols if c in stints_2526.columns]
            stint_melt = stints_2526.melt(id_vars=['game_id', 'seconds'], value_vars=present_cols, value_name='player_id')
            game_minutes = stint_melt.groupby(['player_id', 'game_id'])['seconds'].sum().reset_index()
            game_minutes['minutes'] = game_minutes['seconds'] / 60.0
        except: pass

    # 3. Process Logic
    player_map = pd.read_csv(PLAYER_MAP_PATH)[['player_name', 'player_id']]
    final = bbref.merge(player_map, on='player_name', how='inner')
    
    results = []
    for _, row in final.iterrows():
        pid = row['player_id']
        total_g = int(row['G'])
        
        # Cross-reference with our minutes data to find "disqualified" games
        p_minutes = game_minutes[game_minutes['player_id'] == pid] if not game_minutes.empty else pd.DataFrame()
        
        g_lt_15 = len(p_minutes[p_minutes['minutes'] < 15])
        g_15_20 = len(p_minutes[(p_minutes['minutes'] >= 15) & (p_minutes['minutes'] < 20)])
        
        # High-Accuracy 65 Game Logic:
        # We start with BBRef Total Games.
        # We assume all games are 20+ unless our stints/PBP proves otherwise.
        # This fixes the "Missing Games" issue.
        
        # Eligible = (Total Games - Games confirmed < 20) + min(2, Games confirmed 15-20)
        # Wait, if we don't have PBP for a game, we don't know if it was 15-20 or < 15.
        # For stars, we assume 20+. 
        
        confirmed_low = g_lt_15 + g_15_20
        # Basic estimate: Total Games - Confirmed Low Minutes
        # Then add back the 15-20 buffer
        eligible = (total_g - confirmed_low) + min(2, g_15_20)
        
        g_rem = team_rem_map.get(row['Team'], 0)
        need = max(0, 65 - eligible)
        
        if eligible >= 65: status, cls = "CLINCHED", "bg-clinched"
        elif (eligible + g_rem) < 65: status, cls = "ELIMINATED", "bg-eliminated"
        else: status, cls = "BUBBLE", "bg-bubble"
        
        results.append({
            'name': row['player_name'],
            'vorp': row['VORP'],
            'bpm': row['BPM'],
            'team': row['Team'],
            'eligible': int(eligible),
            'total_g': total_g,
            'need': int(need),
            'g_rem': int(g_rem),
            'status': status,
            'cls': cls
        })

    generate_dashboard(pd.DataFrame(results).sort_values('vorp', ascending=False), "2026-03-24")

def generate_dashboard(df, last_date):
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>65-Game Tracker | EntropyIsRude</title>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
    <style>
        body {{ font-family: -apple-system, system-ui, sans-serif; background: #f4f4f9; color: #333; margin: 0; padding: 20px; }}
        .header {{ background: #003366; color: white; padding: 30px; border-radius: 12px; text-align: center; margin-bottom: 25px; }}
        .table-container {{ background: white; padding: 20px; border-radius: 12px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }}
        .player-name {{ font-weight: 700; color: #003366; }}
        .vorp-val {{ font-weight: 800; color: #d41111; }}
        .progress-box {{ width: 100px; background: #eee; height: 8px; border-radius: 4px; overflow: hidden; margin-top: 4px; }}
        .progress-fill {{ height: 100%; background: #27ae60; }}
        .bg-eliminated {{ color: #c0392b; font-weight: bold; }}
        .bg-bubble {{ color: #f39c12; font-weight: bold; }}
        .bg-clinched {{ color: #27ae60; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="header">
        <h1 style="margin:0;">NBA Award Eligibility Tracker</h1>
        <p>Authoritative Counts from Basketball Reference</p>
        <p>Data as of: <strong>{last_date}</strong> | <a href="index.html" style="color: #4db8ff;">Back Home</a></p>
    </div>

    <div class="table-container">
        <table id="tracker" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>Player</th>
                    <th>VORP</th>
                    <th>Team</th>
                    <th>Eligible / 65</th>
                    <th>GP (Total)</th>
                    <th>Needs (20m)</th>
                    <th>Team G Rem</th>
                    <th>Status</th>
                    <th>BPM</th>
                </tr>
            </thead>
            <tbody>
"""
    for _, r in df.iterrows():
        if r['vorp'] < 0.1 and r['eligible'] < 40: continue
        perc = (r['eligible'] / 65) * 100
        
        html += f"""
                <tr>
                    <td><div class="player-name">{r['name']}</div></td>
                    <td class="vorp-val">{r['vorp']:.1f}</td>
                    <td>{r['team']}</td>
                    <td>
                        {int(r['eligible'])}
                        <div class="progress-box"><div class="progress-fill" style="width: {min(100, perc)}%"></div></div>
                    </td>
                    <td>{int(r['total_g'])}</td>
                    <td style="font-weight: bold; color: {'#c0392b' if r['need'] > r['g_rem'] else '#333'}">{int(r['need'])}</td>
                    <td>{int(r['g_rem'])}</td>
                    <td><span class="{r['cls']}">{r['status']}</span></td>
                    <td>{r['bpm']:.1f}</td>
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
    print(f"Dashboard updated with Authoritative BBRef Counts: {OUTPUT_HTML}")

if __name__ == "__main__":
    build_daily_report()
