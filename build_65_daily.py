import pandas as pd
import numpy as np
import requests
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

from src.season import SEASON, START_YEAR, TAG

# CONFIGURATION
DATA_DIR = Path("data")
LEDGER_PATH = DATA_DIR / f"master_boxscore_{TAG}.csv"
BBREF_PATH = DATA_DIR / f"bbref_advanced_{TAG}.csv"
TEAM_GP_PATH = DATA_DIR / f"bbref_team_gp_{TAG}.csv"
BBREF_HIST_PATH = DATA_DIR / "bbref_advanced_prev.csv"
PLAYER_MAP_PATH = DATA_DIR / "player_totals_2025_26.csv"
BOX_STATS_PATH = DATA_DIR / "player_boxscore_stats.csv"
OUTPUT_HTML = "65-game-tracker.html"

# NBA Cup final participants get one extra eligible game, keyed by season.
# Add the season's final rosters once the final is played.
CUP_FINAL_PLAYERS_BY_SEASON = {
    # Spurs vs Knicks, Dec 16, 2025
    "2025-26": {
        1628384, 1626157, 1628969, 1628973, 203903, 1642278, 1630577, 203084,
        1641705, 1630170, 1642264, 1628436, 1642844, 1628404, 1628368
    },
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

def get_live_stats():
    """Per-player 65-game rule counts and team games played for the current season.

    Reads data/player_boxscore_stats.csv, which the daily workflow fills from
    nba.com box scores for every game in adjusted_games.csv.
    """
    print(f"Counting {SEASON} award-eligibility games from player box scores...")
    if not BOX_STATS_PATH.exists():
        return None, None
    box = pd.read_csv(BOX_STATS_PATH, dtype={"game_id": str})
    box["game_id"] = box["game_id"].str.lstrip("0")
    # Regular-season game IDs are 2YY#####, with YY the season's start year.
    box = box[box["game_id"].str.startswith(f"2{START_YEAR % 100:02d}")]
    if box.empty:
        return None, None

    def _minutes(value) -> float:
        text = str(value or "")
        m = re.fullmatch(r"PT(\d+)M([\d.]+)S", text)
        if m:
            return int(m.group(1)) + float(m.group(2)) / 60.0
        m = re.fullmatch(r"(\d+):(\d+(?:\.\d+)?)", text)
        return int(m.group(1)) + float(m.group(2)) / 60.0 if m else 0.0

    box["mins"] = box["minutes"].map(_minutes)
    team_gp = box.groupby("team_id")["game_id"].nunique().to_dict()
    played = box[box["mins"] > 0]
    player_stats = {}
    for pid, mins in played.groupby("player_id")["mins"]:
        player_stats[int(pid)] = {
            "g20": int((mins >= 20).sum()),
            "g15_20": int(((mins >= 15) & (mins < 20)).sum()),
            "g_lt_15": int((mins < 15).sum()),
            "total": int(len(mins)),
        }
    return player_stats, team_gp

def build_daily_report():
    print("Building Award Eligibility Report (Restoring VORP + Correcting Counts)...")

    player_stats, live_team_gp = get_live_stats()
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

    # Name -> NBA id from this season's box scores (covers rookies), then the
    # older season-totals map for anyone not yet seen.
    season_names = pd.read_csv(BOX_STATS_PATH)[['player_name', 'player_id']]
    player_map = (pd.concat([season_names, pd.read_csv(PLAYER_MAP_PATH)[['player_name', 'player_id']]])
                  .drop_duplicates('player_name'))
    final = bbref.merge(player_map, on='player_name', how='inner')

    official_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
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
        if pid in CUP_FINAL_PLAYERS_BY_SEASON.get(SEASON, set()):
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
        body {{ font-family: "Segoe UI", Arial, sans-serif; background: linear-gradient(180deg, #eef5ff 0%, #f8fbff 30%, #f2f6fb 100%); color: #192231; margin: 0; padding: 18px; }}
        .wrap {{ max-width: 1100px; margin: 0 auto; }}
        .hero {{ background: radial-gradient(circle at 20% 20%, #154f8b 0%, #0d2f53 45%, #081a2f 100%); color: #f8fbff; border-radius: 14px; padding: 18px 20px; border: 1px solid #254b72; margin-bottom: 18px; text-align: center; }}
        .hero h1 {{ margin: 0; font-size: 24px; }}
        .hero .intro {{ max-width: 800px; margin: 8px auto 0; line-height: 1.5; color: rgba(255,255,255,0.8); font-size: 0.88em; }}
        .hero .meta {{ color: rgba(255,255,255,0.7); font-size: 0.85em; margin-top: 6px; }}
        .nav {{ margin-top: 10px; display: flex; gap: 8px; flex-wrap: wrap; justify-content: center; }}
        .nav a {{ color: #e8f4ff; text-decoration: none; border: 1px solid rgba(255,255,255,.35); border-radius: 7px; padding: 6px 10px; font-size: 12px; }}
        .nav a:hover {{ background: rgba(255,255,255,0.15); border-color: rgba(255,255,255,0.6); }}
        .table-container {{ background: white; padding: 20px; border-radius: 12px; border: 1px solid #d6e1ef; box-shadow: 0 3px 12px rgba(23,38,62,0.06); }}
        .player-name {{ font-weight: 700; color: #0d2f53; }}
        .vorp-val {{ font-weight: 800; color: #d41111; }}
        .vorp-prev {{ color: #7f8c8d; font-size: 0.9em; }}
        .progress-box {{ width: 100px; background: #eee; height: 8px; border-radius: 4px; overflow: hidden; margin-top: 4px; }}
        .progress-fill {{ height: 100%; background: #27ae60; }}
        .bg-eliminated {{ color: #c0392b; font-weight: bold; font-size: 0.85em; }}
        .bg-bubble {{ color: #f39c12; font-weight: bold; font-size: 0.85em; }}
        .bg-clinched {{ color: #27ae60; font-weight: bold; font-size: 0.85em; }}
        .footnote {{ font-size: 0.85em; color: #5b6778; margin-top: 20px; padding: 15px; background: #eef4fb; border-radius: 8px; border: 1px solid #d6e1ef; border-left: 4px solid #154f8b; }}
    </style>
</head>
<body>
<div class="wrap">
    <section class="hero">
        <h1>NBA Award Eligibility Tracker</h1>
        <div class="intro">
            To be eligible for major end-of-season honors&mdash;including <strong>MVP, All-NBA, DPOY, Most Improved, and All-Defensive teams</strong>&mdash;a player must participate in at least 65 games (20+ min, up to two 15&ndash;20 min near-miss games credited).
        </div>
        <p class="meta">Sorted by <strong>VORP</strong> &middot; Authoritative counts through <strong>{official_date}</strong></p>
        <div class="nav">
            <a href="index.html">Overview</a>
            <a href="onoff-daily.html">+/- Games</a>
            <a href="onoff.html">+/- Stats</a>
            <a href="rapm.html">RAPM</a>
            <a href="onoff-playoffs.html">+/- Playoffs</a>
            <a href="rapm-playoffs.html">Playoff RAPM</a>
            <a href="example.html">Method</a>
        </div>
    </section>

    <div class="table-container">
        <table id="tracker" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>Player</th>
                    <th>'25-26 VORP</th>
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
        v_prev_sort = r['vorp_prev'] if pd.notna(r['vorp_prev']) else -10

        html += f"""
                <tr>
                    <td><div class="player-name">{r['name']}{cup_star}</div></td>
                    <td class="vorp-val" data-order="{r['vorp']}">{r['vorp']:.1f}</td>
                    <td class="vorp-prev" data-order="{v_prev_sort}">{v_prev}</td>
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
</div>
</body>
</html>
"""
    with open(OUTPUT_HTML, "w", encoding='utf-8') as f:
        f.write(html)
    print(f"Dashboard updated: {OUTPUT_HTML}")

if __name__ == "__main__":
    build_daily_report()
