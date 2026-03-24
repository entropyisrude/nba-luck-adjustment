
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
PLAYER_MAP_PATH = DATA_DIR / "player_totals_2025_26.csv"
OUTPUT_HTML = "65-game-tracker.html"

def build_daily_report():
    print("Building Award Eligibility Report...")
    
    # 1. Load Data
    if not BBREF_PATH.exists() or not TEAM_GP_PATH.exists():
        print("Required BBRef data files missing.")
        return
    
    bbref = pd.read_csv(BBREF_PATH)
    # Handle BBRef TOT (Total) rows for traded players - keep only the 'TOT' row
    bbref = bbref.sort_values(['player_name', 'Team']).drop_duplicates('player_name', keep='first')
    
    team_gp_df = pd.read_csv(TEAM_GP_PATH)
    team_rem_map = {row['team_abbr']: max(0, 82 - int(row['team_gp'])) for _, row in team_gp_df.iterrows()}

    official_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # 2. Ledger stats
    ledger_stats = {} 
    if LEDGER_PATH.exists():
        ledger = pd.read_csv(LEDGER_PATH)
        g15_20 = ledger[(ledger['minutes'] >= 15) & (ledger['minutes'] < 20)].groupby('player_id').size()
        g_lt_15 = ledger[ledger['minutes'] < 15].groupby('player_id').size()
        for pid in ledger['player_id'].unique():
            ledger_stats[int(pid)] = {
                'g15_20': int(g15_20.get(pid, 0)),
                'g_lt_15': int(g_lt_15.get(pid, 0))
            }

    # 3. Merge
    player_map = pd.read_csv(PLAYER_MAP_PATH)[['player_name', 'player_id']]
    final = bbref.merge(player_map, on='player_name', how='inner')
    
    results = []
    for _, row in final.iterrows():
        pid = int(row['player_id'])
        total_g = int(row['G'])
        low_stats = ledger_stats.get(pid, {'g15_20': 0, 'g_lt_15': 0})
        
        # ELIGIBILITY LOGIC
        eligible = (total_g - (low_stats['g15_20'] + low_stats['g_lt_15'])) + min(2, low_stats['g15_20'])
        
        # Use Team Remaining or default to 0 for traded players (TOT)
        g_rem = team_rem_map.get(row['Team'], 0)
        # Fallback for traded players: use average remaining or assume NYK/OKC-ish if team is 'TOT'
        if row['Team'] == 'TOT':
            g_rem = int(np.mean(list(team_rem_map.values())))

        need = max(0, 65 - eligible)
        
        if eligible >= 65: status, cls = "CLINCHED", "bg-clinched"
        elif (eligible + g_rem) < 65: status, cls = "ELIMINATED", "bg-eliminated"
        else: status, cls = "BUBBLE", "bg-bubble"
        
        results.append({
            'name': row['player_name'], 'vorp': row['VORP'], 'team': row['Team'],
            'eligible': int(eligible), 'total_g': total_g, 'need': int(need),
            'g_rem': int(g_rem), 'status': status, 'cls': cls,
            'g15_20': low_stats['g15_20'], 'g_lt_15': low_stats['g_lt_15']
        })

    generate_dashboard(pd.DataFrame(results).sort_values('vorp', ascending=False), official_date)

def generate_dashboard(df, official_date):
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
        <p>Authoritative Counts through: <strong>{official_date}</strong> | <a href="index.html" style="color: #4db8ff;">Back Home</a></p>
    </div>

    <div class="table-container">
        <table id="tracker" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>Player</th>
                    <th>VORP</th>
                    <th>Team</th>
                    <th>Eligible / 65</th>
                    <th>Low-Min Games</th>
                    <th>Needs (20m)</th>
                    <th>Team G Rem</th>
                    <th>Status</th>
                    <th>Total GP</th>
                </tr>
            </thead>
            <tbody>
"""
    for _, r in df.iterrows():
        if r['vorp'] < 0.1 and r['eligible'] < 45: continue
        perc = (r['eligible'] / 65) * 100
        low_min_str = f"{r['g15_20']} / {r['g_lt_15']}"
        low_min_style = "color: #e67e22; font-weight: 600;" if (r['g15_20'] > 2 or r['g_lt_15'] > 0) else ""

        html += f"""
                <tr>
                    <td><div class="player-name">{r['name']}</div></td>
                    <td class="vorp-val">{r['vorp']:.1f}</td>
                    <td>{r['team']}</td>
                    <td>
                        {int(r['eligible'])}
                        <div class="progress-box"><div class="progress-fill" style="width: {min(100, perc)}%"></div></div>
                    </td>
                    <td style="{low_min_style}">{low_min_str} <br><small style="font-weight:normal; color:#888;">(15-20 / &lt;15)</small></td>
                    <td style="font-weight: bold; color: {'#c0392b' if r['need'] > r['g_rem'] else '#333'}">{int(r['need'])}</td>
                    <td>{int(r['g_rem'])}</td>
                    <td><span class="{r['cls']}">{r['status']}</span></td>
                    <td>{int(r['total_g'])}</td>
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
    print(f"Dashboard updated: {OUTPUT_HTML}")

if __name__ == "__main__":
    build_daily_report()
