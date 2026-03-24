
import pandas as pd
import numpy as np
import requests
import json
import os
import re
from pathlib import Path

# CONFIGURATION
DATA_DIR = Path("data")
LEDGER_PATH = DATA_DIR / "master_boxscore_2526.csv" # The New Authority
BBREF_PATH = DATA_DIR / "bbref_advanced_2526.csv"
TEAM_GP_PATH = DATA_DIR / "bbref_team_gp_2526.csv"
PLAYER_MAP_PATH = DATA_DIR / "player_totals_2025_26.csv"
OUTPUT_HTML = "65-game-tracker.html"

def build_daily_report():
    print("Building High-Accuracy Award Eligibility Report from Ledger...")
    
    # 1. Load the Authoritative Ledger
    if not LEDGER_PATH.exists():
        print(f"Ledger missing at {LEDGER_PATH}. Run run_daily.py first.")
        # Fallback to BBRef counts if ledger is totally missing
        use_fallback = True
    else:
        use_fallback = False
        ledger = pd.read_csv(LEDGER_PATH)
        last_date = ledger['date'].max()

    # 2. Load BBRef Metadata
    bbref = pd.read_csv(BBREF_PATH) if BBREF_PATH.exists() else pd.DataFrame()
    team_gp_df = pd.read_csv(TEAM_GP_PATH) if TEAM_GP_PATH.exists() else pd.DataFrame()
    team_rem_map = {row['team_abbr']: max(0, 82 - int(row['team_gp'])) for _, row in team_gp_df.iterrows()}

    # 3. Calculate Eligibility from Ledger
    if not use_fallback:
        # Eligible = 20+ min games + min(2, 15-20 min games)
        g20 = ledger[ledger['minutes'] >= 20].groupby('player_id').size().rename('games_20')
        g15 = ledger[(ledger['minutes'] >= 15) & (ledger['minutes'] < 20)].groupby('player_id').size().rename('games_15_20')
        total_g = ledger.groupby('player_id').size().rename('total_g')
        
        report = pd.DataFrame(index=ledger['player_id'].unique())
        report = report.join(g20).join(g15).join(total_g).fillna(0)
        report['eligible'] = report['games_20'] + report['games_15_20'].clip(upper=2)
    else:
        # Use BBRef counts as total fallback
        print("Using BBRef fallback for counts...")
        report = pd.DataFrame() # We'll handle this in the merge
        last_date = "Check BBRef"

    # 4. Merge and Finalize
    player_map = pd.read_csv(PLAYER_MAP_PATH)[['player_name', 'player_id']]
    
    if not use_fallback:
        final = player_map.merge(report, left_on='player_id', right_index=True)
        if not bbref.empty:
            final = final.merge(bbref[['player_name', 'Team', 'VORP', 'BPM']], on='player_name', how='left')
    else:
        final = player_map.merge(bbref[['player_name', 'Team', 'VORP', 'BPM', 'G']], on='player_name')
        final['eligible'] = final['G'] # Assume all are 20+ if no ledger
        final['games_20'] = final['G']
        final['games_15_20'] = 0
        final['total_g'] = final['G']

    results = []
    for _, row in final.iterrows():
        eligible = int(row['eligible'])
        g_rem = team_rem_map.get(row['Team'], 0)
        need = max(0, 65 - eligible)
        
        if eligible >= 65: status, cls = "CLINCHED", "bg-clinched"
        elif (eligible + g_rem) < 65: status, cls = "ELIMINATED", "bg-eliminated"
        else: status, cls = "BUBBLE", "bg-bubble"
        
        results.append({
            'name': row['player_name'],
            'vorp': row.get('VORP', 0),
            'bpm': row.get('BPM', 0),
            'team': row['Team'],
            'eligible': eligible,
            'total_g': int(row['total_g']),
            'need': int(need),
            'g_rem': int(g_rem),
            'status': status,
            'cls': cls,
            'g20': int(row['games_20']),
            'g15': int(row['games_15_20'])
        })

    generate_dashboard(pd.DataFrame(results).sort_values('vorp', ascending=False), last_date)

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
        <p>Source: Internal Boxscore Ledger + BBRef Standings</p>
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
    print(f"Dashboard updated from Master Ledger: {OUTPUT_HTML}")

if __name__ == "__main__":
    build_daily_report()
