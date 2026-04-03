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
EMV_PATH = DATA_DIR / "unified_2526_results.csv" # Adjusted for repo
PLAYER_MAP_PATH = DATA_DIR / "player_totals_2025_26.csv"
CACHE_PATH = Path("cdn_boxscore_cache.json")
if not CACHE_PATH.exists():
    CACHE_PATH = Path("../cdn_boxscore_cache.json")
OUTPUT_HTML = "65-game-tracker.html"

def get_game_data_from_cdn(game_id):
    """Fetches a single boxscore from CDN and returns per-player minutes, team IDs, and game date."""
    url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            minutes_map = {}
            team_ids = []
            game_date = data['game']['gameTimeUTC'].split('T')[0]
            for team_key in ['homeTeam', 'awayTeam']:
                team_id = data['game'][team_key]['teamId']
                team_ids.append(team_id)
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
            return {'minutes': minutes_map, 'teams': team_ids, 'date': game_date}
    except:
        pass
    return None

def build_daily_report():
    print("Building Award Eligibility Report (Live CDN Update)...")
    if not STINTS_PATH.exists():
        print(f"Error: {STINTS_PATH} not found.")
        return
        
    stints = pd.read_csv(STINTS_PATH)
    stints_2526 = stints[stints['game_id'].astype(str).str.contains('225')].copy()
    
    player_cols = [f'home_p{i}' for i in range(1, 6)] + [f'away_p{i}' for i in range(1, 6)]
    player_cols = [c for c in player_cols if c in stints_2526.columns]
    
    stint_melt = stints_2526.melt(id_vars=['game_id', 'seconds'], value_vars=player_cols, value_name='player_id')
    game_minutes = stint_melt.groupby(['player_id', 'game_id'])['seconds'].sum().reset_index()
    game_minutes['minutes'] = game_minutes['seconds'] / 60.0
    
    processed_in_stints = set(stints_2526['game_id'].unique())
    
    home_teams = stints_2526[['game_id', 'home_id']].drop_duplicates().rename(columns={'home_id': 'team_id'})
    away_teams = stints_2526[['game_id', 'away_id']].drop_duplicates().rename(columns={'away_id': 'team_id'})
    all_game_teams = pd.concat([home_teams, away_teams])

    today_str = "2026-04-03"
    latest_valid_date = stints_2526['date'].max()

    new_rows = []
    if CACHE_PATH.exists():
        with open(CACHE_PATH, 'r') as f:
            all_cached_ids = [int(gid) for gid in json.load(f) if gid.startswith('00225')]
        
        new_game_ids = [gid for gid in all_cached_ids if gid not in processed_in_stints]
        if new_game_ids:
            print(f"Found {len(new_game_ids)} new games in CDN cache. Fetching live data...")
            new_team_rows = []
            for gid in new_game_ids:
                gid_str = str(gid).zfill(10)
                g_data = get_game_data_from_cdn(gid_str)
                if g_data:
                    if g_data['date'] > today_str:
                        continue
                    if g_data['date'] > latest_valid_date:
                        latest_valid_date = g_data['date']
                    m_map = g_data['minutes']
                    for pid, mins in m_map.items():
                        new_rows.append({'player_id': pid, 'game_id': gid, 'minutes': mins})
                    for tid in g_data['teams']:
                        new_team_rows.append({'game_id': gid, 'team_id': tid})
            
            if new_rows:
                game_minutes = pd.concat([game_minutes, pd.DataFrame(new_rows)], ignore_index=True)
            if new_team_rows:
                all_game_teams = pd.concat([all_game_teams, pd.DataFrame(new_team_rows)], ignore_index=True)

    g20 = game_minutes[game_minutes['minutes'] >= 20].groupby('player_id').size().rename('games_20')
    g15 = game_minutes[(game_minutes['minutes'] >= 15) & (game_minutes['minutes'] < 20)].groupby('player_id').size().rename('games_15_20')
    gp = game_minutes[game_minutes['minutes'] > 0].groupby('player_id').size().rename('total_gp')

    team_games = all_game_teams.drop_duplicates().groupby('team_id').size().rename('team_gp')
    
    player_game_map = pd.concat([
        stint_melt[['player_id', 'game_id']],
        pd.DataFrame(new_rows)[['player_id', 'game_id']] if new_rows else pd.DataFrame(columns=['player_id', 'game_id'])
    ]).drop_duplicates()
    
    player_team_map = player_game_map.merge(all_game_teams.drop_duplicates(), on='game_id')
    player_primary_team = player_team_map.groupby(['player_id', 'team_id']).size().reset_index(name='c').sort_values('c', ascending=False).drop_duplicates('player_id')

    report = pd.DataFrame(index=game_minutes['player_id'].unique())
    report = report.join(g20).join(g15).join(gp).fillna(0)
    report = report.merge(player_primary_team[['player_id', 'team_id']], left_index=True, right_on='player_id').set_index('player_id')
    report = report.merge(team_games, left_on='team_id', right_index=True)
    
    report['eligible_games'] = report['games_20'] + report['games_15_20'].clip(upper=2)
    report['games_rem'] = 82 - report['team_gp']
    report['max_possible'] = report['eligible_games'] + report['games_rem']
    report['need_to_play'] = (65 - report['eligible_games']).clip(lower=0)
    
    def get_status(row):
        if row['eligible_games'] >= 65: return 'Clinched'
        if row['max_possible'] < 65: return 'Eliminated'
        return 'On the Bubble'
    report['status'] = row_status = report.apply(get_status, axis=1)

    # Load EMV and Name Map
    if not EMV_PATH.exists() or not PLAYER_MAP_PATH.exists():
        print("Warning: EMV or Player Map missing. Dashboard will be incomplete.")
        return

    emv_df = pd.read_csv(EMV_PATH)
    p_totals = pd.read_csv(PLAYER_MAP_PATH)[['player_name', 'player_id']]
    final = emv_df.merge(p_totals, on='player_name').merge(report, left_on='player_id', right_index=True)
    
    generate_dashboard(final.sort_values('total_emv', ascending=False), latest_valid_date)

def generate_dashboard(df, last_date):
    clinched = len(df[df['status'] == 'Clinched'])
    # Contenders eliminated
    eliminated = len(df[(df['status'] == 'Eliminated') & (df['total_emv'] > 2)])
    bubble = len(df[df['status'] == 'On the Bubble'])

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>NBA 65-Game Tracker | EntropyIsRude</title>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
    <style>
        body {{ font-family: 'Inter', sans-serif; background: #fafafa; color: #333; margin: 0; padding: 20px; }}
        .header {{ background: #051c2c; color: white; padding: 30px; border-radius: 12px; text-align: center; margin-bottom: 25px; }}
        .summary {{ display: flex; gap: 15px; margin-bottom: 25px; }}
        .stat-card {{ background: white; flex: 1; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); text-align: center; border-top: 5px solid #ddd; }}
        .clinched {{ border-top-color: #27ae60; }} .eliminated {{ border-top-color: #c0392b; }} .bubble {{ border-top-color: #f39c12; }}
        .stat-val {{ font-size: 2.5em; font-weight: 800; margin: 5px 0; }}
        .table-container {{ background: white; padding: 20px; border-radius: 12px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }}
        .status-badge {{ padding: 4px 10px; border-radius: 20px; font-size: 0.85em; font-weight: 600; }}
        .bg-clinched {{ background: #eafaf1; color: #27ae60; }}
        .bg-eliminated {{ background: #fdf2f2; color: #c0392b; }}
        .bg-bubble {{ background: #fef9e7; color: #f39c12; }}
        .player-name {{ font-weight: 700; color: #051c2c; }}
        .emv-sub {{ font-size: 0.8em; color: #7f8c8d; }}
        .progress-box {{ width: 100px; background: #eee; height: 8px; border-radius: 4px; overflow: hidden; margin-top: 4px; }}
        .progress-fill {{ height: 100%; background: #27ae60; }}
    </style>
</head>
<body>
    <div class="header">
        <h1 style="margin:0; font-size: 2.2em;">NBA Awards: The 65-Game Tracker</h1>
        <p style="opacity: 0.8; margin: 10px 0 0 0;">Daily Status for MVP, All-NBA, and DPOY Eligibility</p>
        <p style="font-size: 0.85em; margin-top: 15px;">Data through: <strong>{last_date}</strong> (Live updates from CDN included)</p>
    </div>

    <div class="summary">
        <div class="stat-card clinched"><div class="stat-label">Clinched</div><div class="stat-val">{clinched}</div></div>
        <div class="stat-card eliminated"><div class="stat-label">Eliminated (Contenders)</div><div class="stat-val">{eliminated}</div></div>
        <div class="stat-card bubble"><div class="stat-label">On the Bubble</div><div class="stat-val">{bubble}</div></div>
    </div>

    <div class="table-container">
        <table id="tracker" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>Player</th>
                    <th>Status</th>
                    <th>Eligible / 65</th>
                    <th>Needs (20m)</th>
                    <th>Games Rem</th>
                    <th>Must Play %</th>
                    <th>Total EMV</th>
                </tr>
            </thead>
            <tbody>
"""
    for _, r in df.iterrows():
        if r['total_emv'] < -0.5 and r['status'] == 'Eliminated': continue
        
        status_cls = "bg-clinched" if r['status'] == 'Clinched' else ("bg-eliminated" if r['status'] == 'Eliminated' else "bg-bubble")
        perc = (r['eligible_games'] / 65) * 100
        
        if r['status'] == 'Clinched': att_req = "0%"
        elif r['status'] == 'Eliminated': att_req = "N/A"
        else:
            ratio = (r['need_to_play'] / r['games_rem']) * 100
            att_req = f"{ratio:.1f}%"

        color = "#333"
        try:
            if "N/A" not in att_req and float(att_req.replace('%','')) > 90: color = "#c0392b"
        except: pass

        html += f"""
                <tr>
                    <td>
                        <div class="player-name">{r['player_name']}</div>
                        <div class="emv-sub">EMV: {r['total_emv']:.2f}</div>
                    </td>
                    <td><span class="status-badge {status_cls}">{r['status']}</span></td>
                    <td>
                        {int(r['eligible_games'])}
                        <div class="progress-box"><div class="progress-fill" style="width: {min(100, perc)}%"></div></div>
                    </td>
                    <td style="font-weight: bold;">{int(r['need_to_play'])}</td>
                    <td>{int(r['games_rem'])}</td>
                    <td><span style="color: {color}">{att_req}</span></td>
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
                order: [[6, 'desc']]
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
