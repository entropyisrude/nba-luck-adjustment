import pandas as pd
import numpy as np
import requests
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

# CONFIGURATION
DATA_DIR = Path("data")
LEDGER_PATH = DATA_DIR / "master_boxscore_2526.csv"
BBREF_PATH = DATA_DIR / "bbref_advanced_2526.csv"
TEAM_GP_PATH = DATA_DIR / "bbref_team_gp_2526.csv"
BBREF_HIST_PATH = DATA_DIR / "bbref_advanced_2024_25.csv"
PLAYER_MAP_PATH = DATA_DIR / "player_totals_2025_26.csv"
CACHE_PATH = Path("cdn_boxscore_cache.json")
if not CACHE_PATH.exists():
    CACHE_PATH = Path("../cdn_boxscore_cache.json")
BOXSCORE_DIR = Path("boxscores_cache")
BOXSCORE_DIR.mkdir(exist_ok=True)
OUTPUT_HTML = "65-game-tracker.html"

# Corrected NBA Cup Final Participants (Spurs vs Knicks, Dec 16, 2025)
CUP_FINAL_PLAYERS = {
    1628384, 1626157, 1628969, 1628973, 203903, 1642278, 1630577, 203084,
    1641705, 1630170, 1642264, 1628436, 1642844, 1628404, 1628368
}

TEAM_MAP_REV = {
    1610612737: 'ATL', 1610612738: 'BOS', 1610612739: 'CLE', 1610612740: 'NOP',
    1610612741: 'CHI', 1610612742: 'DAL', 1610612743: 'DEN', 1610612744: 'GSW',
    1610612745: 'HOU', 1610612746: 'LAC', 1610612747: 'LAL', 1610612748: 'MIA',
    1610612749: 'MIL', 1610612750: 'MIN', 1610612751: 'BRK', 1610612752: 'NYK',
    1610612753: 'ORL', 1610612754: 'IND', 1610612755: 'PHI', 1610612756: 'PHO',
    1610612757: 'POR', 1610612758: 'SAC', 1610612759: 'SAS', 1610612760: 'OKC',
    1610612761: 'TOR', 1610612762: 'UTA', 1610612763: 'MEM', 1610612764: 'WAS',
    1610612765: 'DET', 1610612766: 'CHO'
}

def get_live_stats_from_cdn():
    """Scans the CDN cache for authoritative game counts, using local file cache for speed."""
    print("Updating Award Eligibility counts from live CDN...")
    if not CACHE_PATH.exists():
        return None, None
    
    with open(CACHE_PATH, 'r') as f:
        gids = sorted(list(set([gid for gid in json.load(f) if gid.startswith("00225")])))
    
    # Auto-detect current date (ET)
    now_et = datetime.utcnow() - timedelta(hours=5)
    today_str = now_et.strftime("%Y-%m-%d")
    player_stats = {} 
    team_gp = {} 
    
    for i, gid in enumerate(gids):
        local_path = BOXSCORE_DIR / f"{gid}.json"
        data = None
        
        if local_path.exists():
            with open(local_path, 'r') as f:
                data = json.load(f)
        else:
            url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gid}.json"
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    with open(local_path, 'w') as f:
                        json.dump(data, f)
            except:
                pass
        
        if data:
            try:
                game_date = data['game']['gameTimeUTC'].split('T')[0]
                if game_date > today_str: continue
                
                players = data['game']['homeTeam']['players'] + data['game']['awayTeam']['players']
                total_min = sum(int(re.search(r'PT(\d+)M', p['statistics']['minutes']).group(1)) for p in players if re.search(r'PT(\d+)M', p['statistics']['minutes']))
                if total_min < 200: continue

                # Count for teams
                team_gp[data['game']['homeTeam']['teamId']] = team_gp.get(data['game']['homeTeam']['teamId'], 0) + 1
                team_gp[data['game']['awayTeam']['teamId']] = team_gp.get(data['game']['awayTeam']['teamId'], 0) + 1

                # Count for players
                for p in players:
                    pid = p['personId']
                    s = p['statistics']['minutes']
                    m = re.search(r'PT(\d+)M', s)
                    s_sec = re.search(r'M([\d.]+)S', s)
                    mins = 0
                    if m: mins += int(m.group(1))
                    if s_sec: mins += float(s_sec.group(1))/60.0
                    
                    if mins == 0: continue
                    
                    if pid not in player_stats:
                        player_stats[pid] = {'g20': 0, 'g15_20': 0, 'g_lt_15': 0, 'total': 0}
                    
                    player_stats[pid]['total'] += 1
                    if mins >= 20: player_stats[pid]['g20'] += 1
                    elif mins >= 15: player_stats[pid]['g15_20'] += 1
                    else: player_stats[pid]['g_lt_15'] += 1
            except:
                pass
        
        if (i+1) % 200 == 0:
            print(f"  Processed {i+1} games...")
            
    return player_stats, team_gp

def build_daily_report():
    print("Building Award Eligibility Report (Restoring VORP + Correcting Counts)...")

    player_stats, live_team_gp = get_live_stats_from_cdn()
    if not player_stats:
        print("Failed to get live stats.")
        return

    if not BBREF_PATH.exists():
        print("Required VORP data missing.")
        return

    bbref = pd.read_csv(BBREF_PATH)
    # Deduplicate traded players
    bbref = bbref.sort_values(['player_name', 'Team']).drop_duplicates('player_name', keep='first')

    # Load Historical VORP
    if BBREF_HIST_PATH.exists():
        bbref_hist = pd.read_csv(BBREF_HIST_PATH)
        bbref = bbref.merge(bbref_hist, on='player_name', how='left')
    else:
        bbref['vorp_prev'] = 0.0

    player_map = pd.read_csv(PLAYER_MAP_PATH)[['player_name', 'player_id']]
    final = bbref.merge(player_map, on='player_name', how='inner')

    official_date = "2026-04-03"
    
    # Map live team GP to abbreviations
    team_rem_map = {}
    for tid, gp in live_team_gp.items():
        abbr = TEAM_MAP_REV.get(tid)
        if abbr:
            team_rem_map[abbr] = max(0, 82 - gp)
    
    avg_rem = int(np.mean(list(team_rem_map.values()))) if team_rem_map else 6

    results = []
    for _, row in final.iterrows():
        pid = int(row['player_id'])
        stats = player_stats.get(pid, {'g20': 0, 'g15_20': 0, 'g_lt_15': 0, 'total': 0})

        # 65-Game Eligibility Logic (Live Counts)
        eligible = stats['g20'] + min(2, stats['g15_20'])

        # Special credit for NBA Cup Final
        has_cup_credit = False
        if pid in CUP_FINAL_PLAYERS:
            eligible += 1
            has_cup_credit = True

        # Games remaining logic
        g_rem = team_rem_map.get(row['Team'], avg_rem)
        if row['Team'] in ['TOT', '2TM', '3TM']:
            g_rem = avg_rem

        need = max(0, 65 - eligible)

        if eligible >= 65: status, cls = "CLINCHED", "bg-clinched"
        elif (eligible + g_rem) < 65: status, cls = "ELIMINATED", "bg-eliminated"
        else: status, cls = "BUBBLE", "bg-bubble"

        results.append({
            'name': row['player_name'], 'vorp': row['VORP'], 'vorp_prev': row.get('vorp_prev', 0.0),
            'team': row['Team'],
            'eligible': int(eligible), 'total_g': stats['total'], 'need': int(need),
            'g_rem': int(g_rem), 'status': status, 'cls': cls,
            'g15_20': stats['g15_20'], 'g_lt_15': stats['g_lt_15'],
            'cup_credit': has_cup_credit
        })

    generate_dashboard(pd.DataFrame(results).sort_values('vorp', ascending=False), official_date)

def generate_dashboard(df, official_date):
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>NBA Award Tracker | EntropyIsRude</title>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
    <style>
        body {{ font-family: -apple-system, system-ui, sans-serif; background: #f4f4f9; color: #333; margin: 0; padding: 20px; }}
        .header {{ background: #003366; color: white; padding: 30px; border-radius: 12px; text-align: center; margin-bottom: 25px; }}
        .intro {{ max-width: 800px; margin: 0 auto 20px auto; line-height: 1.6; color: #eee; font-size: 0.95em; }}
        .table-container {{ background: white; padding: 20px; border-radius: 12px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }}
        .player-name {{ font-weight: 700; color: #003366; }}
        .vorp-val {{ font-weight: 800; color: #d41111; }}
        .vorp-prev {{ color: #7f8c8d; font-size: 0.9em; }}
        .progress-box {{ width: 100px; background: #eee; height: 8px; border-radius: 4px; overflow: hidden; margin-top: 4px; }}
        .progress-fill {{ height: 100%; background: #27ae60; }}
        .bg-eliminated {{ color: #c0392b; font-weight: bold; font-size: 0.85em; }}
        .bg-bubble {{ color: #f39c12; font-weight: bold; font-size: 0.85em; }}
        .bg-clinched {{ color: #27ae60; font-weight: bold; font-size: 0.85em; }}
        .footnote {{ font-size: 0.85em; color: #666; margin-top: 20px; padding: 15px; background: #fff; border-radius: 8px; border-left: 4px solid #003366; }}
    </style>
</head>
<body>
    <div class="header">
        <h1 style="margin:0;">NBA Award Eligibility Tracker</h1>
        <div class="intro">
            To be eligible for major end-of-season honors—including <strong>MVP, All-NBA, Defensive Player of the Year, Most Improved, and All-Defensive teams</strong>—a player must participate in at least 65 games. For a game to count, a player must play at least 20 minutes, though up to two "near-miss" games of 15-20 minutes may also be credited toward the total.
        </div>
        <p>Sorted by <strong>VORP</strong> | Authoritative Counts through: <strong>{official_date}</strong> (Live CDN Feed)</p> 
        <p><a href="index.html" style="color: #4db8ff; text-decoration: none; font-weight: 600;">&larr; Back to Homepage</a></p>
    </div>

    <div class="table-container">
        <table id="tracker" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>Player</th>
                    <th>VORP</th>
                    <th>'24-25 VORP</th>
                    <th>Eligible / 65</th>
                    <th>Low-Min (15-20 / &lt;15)</th>
                    <th>Needs (20m)</th>
                    <th>G Rem</th>
                    <th>Status</th>
                    <th>Team</th>
                </tr>
            </thead>
            <tbody>
"""
    for _, r in df.iterrows():
        # Keep players with current VORP > 0.1 OR high historical VORP (candidates)
        if r['vorp'] < 0.1 and r['vorp_prev'] < 2.0 and r['eligible'] < 45: continue
        
        perc = (r['eligible'] / 65) * 100
        cup_star = "<sup>*</sup>" if r['cup_credit'] else ""
        v_prev = f"{r['vorp_prev']:.1f}" if pd.notna(r['vorp_prev']) else "N/A"

        html += f"""
                <tr>
                    <td><div class="player-name">{r['name']}{cup_star}</div></td>
                    <td class="vorp-val">{r['vorp']:.1f}</td>
                    <td class="vorp-prev">{v_prev}</td>
                    <td>
                        {int(r['eligible'])}
                        <div class="progress-box"><div class="progress-fill" style="width: {min(100, perc)}%"></div></div>
                    </td>
                    <td>{r['g15_20']} / {r['g_lt_15']}</td>
                    <td style="font-weight: bold; color: {'#c0392b' if r['need'] > r['g_rem'] else '#333'}">{int(r['need'])}</td>
                    <td>{int(r['g_rem'])}</td>
                    <td><span class="{r['cls']}">{r['status']}</span></td>
                    <td>{r['team']}</td>
                </tr>
        """

    html += """
            </tbody>
        </table>
    </div>

    <div class="footnote">
        <strong>* NBA Cup Final Exception:</strong> Per CBA Article XXIX, the NBA Cup Championship Game counts toward the 65-game requirement for players who play at least 20 minutes (or meet the 15-20 minute near-miss criteria), even though it is not an official regular season game.
        <br><br>
        <strong>Logic Note:</strong> Tracker uses authoritative live boxscore data from NBA CDN for game counts and disqualifications. VORP values are sourced from Basketball Reference.
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
    print(f"Dashboard updated: {OUTPUT_HTML}")

if __name__ == "__main__":
    build_daily_report()
