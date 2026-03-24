
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

TEMPLATE_PATH = Path("template.html")
BBREF_STANDINGS_PATH = Path("data/bbref_team_gp_2526.csv")

def generate_report():
    print("Generating Luck-Adjusted Report with Authority Split...")
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Template file not found: {TEMPLATE_PATH}")
    template = TEMPLATE_PATH.read_text(encoding='utf-8')

    # 1. Load Data
    df_analyzed = pd.read_csv("data/adjusted_games.csv")
    
    # 2. Load Official Stats (Standings)
    official_standings = {}
    if BBREF_STANDINGS_PATH.exists():
        std_df = pd.read_csv(BBREF_STANDINGS_PATH)
        for _, row in std_df.iterrows():
            official_standings[row['team_abbr']] = {
                'wins': int(row['W']),
                'losses': int(row['L']),
                'total_gp': int(row['team_gp'])
            }

    # 3. Process Analyzed Stats
    team_stats = {}
    for team in set(df_analyzed['home_team'].unique()) | set(df_analyzed['away_team'].unique()):
        team_stats[team] = {
            'analyzed_wins': 0, 'analyzed_adj_wins': 0, 'analyzed_count': 0,
            'opp_3pa': 0, 'opp_3pm': 0, 'opp_3pm_exp': 0
        }

    for _, row in df_analyzed.iterrows():
        home, away = row['home_team'], row['away_team']
        team_stats[home]['analyzed_count'] += 1
        team_stats[away]['analyzed_count'] += 1
        if row['margin_actual'] > 0: team_stats[home]['analyzed_wins'] += 1
        else: team_stats[away]['analyzed_wins'] += 1
        if row['margin_adj'] > 0: team_stats[home]['analyzed_adj_wins'] += 1
        else: team_stats[away]['analyzed_adj_wins'] += 1
        team_stats[home]['opp_3pa'] += row['away_3pa']
        team_stats[home]['opp_3pm'] += row['away_3pm_actual']
        team_stats[home]['opp_3pm_exp'] += row['away_3pm_exp']
        team_stats[away]['opp_3pa'] += row['home_3pa']
        team_stats[away]['opp_3pm'] += row['home_3pm_actual']
        team_stats[away]['opp_3pm_exp'] += row['home_3pm_exp']

    # 4. Build Table Rows
    sorted_teams = sorted(team_stats.keys(), 
                         key=lambda t: official_standings.get(t, {'wins':0})['wins'], 
                         reverse=True)

    team_rows = ""
    for rank, team in enumerate(sorted_teams, 1):
        s = team_stats[team]
        off = official_standings.get(team, {'wins':0, 'losses':0, 'total_gp': s['analyzed_count']})
        actual_record = f"{off['wins']}-{off['losses']}"
        net_swung = s['analyzed_wins'] - s['analyzed_adj_wins']
        adj_wins = off['wins'] - net_swung
        adj_losses = off['losses'] + net_swung
        adj_record = f"{adj_wins}-{adj_losses}"
        diff_class = "positive" if net_swung > 0 else ("negative" if net_swung < 0 else "")
        diff_str = f"{net_swung:+d}" if net_swung != 0 else "0"
        opp_3p_pct = round(s['opp_3pm'] / s['opp_3pa'] * 100, 1) if s['opp_3pa'] > 0 else 0
        opp_3p_exp = round(s['opp_3pm_exp'] / s['opp_3pa'] * 100, 1) if s['opp_3pa'] > 0 else 0
        opp_diff = round(opp_3p_pct - opp_3p_exp, 1)
        coverage_str = f"{s['analyzed_count']}/{off['total_gp']}"

        team_rows += f"""
        <tr data-team="{team}" data-wins="{off['wins']}" data-adjwins="{adj_wins}">
            <td>{rank}</td>
            <td><strong>{team}</strong> <br><small style="color:#888; font-size:0.7em;">({coverage_str} analyzed)</small></td>
            <td>{actual_record}</td>
            <td>{adj_record}</td>
            <td class="{diff_class} clickable-diff" onclick="showFlippedGames('{team}')">{diff_str}</td>
            <td>{opp_3p_exp}%</td>
            <td>{opp_3p_pct}%</td>
            <td>{opp_diff:+.1f}%</td>
        </tr>"""

    # 5. Calendar and Swings
    games_by_date = {}
    for _, row in df_analyzed.iterrows():
        d = row['date']
        if d not in games_by_date: games_by_date[d] = []
        # Crucial: Ensure top_swing_players is handled as JSON/List
        game_dict = row.to_dict()
        if isinstance(game_dict.get('top_swing_players'), str):
            try: game_dict['top_swing_players'] = json.loads(game_dict['top_swing_players'])
            except: game_dict['top_swing_players'] = []
        games_by_date[d].append(game_dict)
    
    df_analyzed['abs_delta'] = df_analyzed['margin_delta'].abs()
    top_swings = df_analyzed.sort_values('abs_delta', ascending=False).head(15)
    swing_rows = ""
    for _, row in top_swings.iterrows():
        flip_warn = " &#9888;" if (row['margin_actual'] > 0) != (row['margin_adj'] > 0) else ""
        swing_rows += f"""
        <tr>
            <td>{row['date']}</td>
            <td>{row['away_team']} @ {row['home_team']}</td>
            <td>{int(row['away_pts_actual'])}-{int(row['home_pts_actual'])} ({int(row['margin_actual']):+d})</td>
            <td>{row['away_pts_adj']:.1f}-{row['home_pts_adj']:.1f} ({row['margin_adj']:.1f}){flip_warn}</td>
            <td class="positive">{row['margin_delta']:.1f}</td>
        </tr>"""

    html = template.replace("{{TEAM_RANKINGS_ROWS}}", team_rows)
    last_game_date = df_analyzed['date'].max()
    html = html.replace("{{SEASON_DATE}}", last_game_date)
    html = html.replace("{{GAME_COUNT}}", str(len(df_analyzed)))
    html = html.replace("{{GENERATED_TIMESTAMP}}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    html = html.replace("{{BIGGEST_SWINGS_ROWS}}", swing_rows)
    html = html.replace("{{GAMES_JSON}}", json.dumps(games_by_date))
    html = html.replace("{{MOST_RECENT_DATE}}", last_game_date)
    
    dates = pd.to_datetime(df_analyzed['date'])
    months = sorted(list(set((d.year, d.month-1) for d in dates)))
    month_json = [{"year": m[0], "month": m[1]} for m in months]
    html = html.replace("{{SEASON_MONTHS_JSON}}", json.dumps(month_json))

    Path("index.html").write_text(html, encoding='utf-8')
    print(f"Homepage fully updated with standings and calendar.")

if __name__ == "__main__":
    generate_report()
