
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

TEMPLATE_PATH = Path("template.html")
BBREF_STANDINGS_PATH = Path("data/bbref_team_gp_2526.csv")

def generate_report():
    print("Generating Luck-Adjusted Report with Official Standings...")
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Template file not found: {TEMPLATE_PATH}")
    template = TEMPLATE_PATH.read_text(encoding='utf-8')

    # 1. Load Analyzed Data
    df = pd.read_csv("data/adjusted_games.csv")

    # 2. Load Official Standings
    official_standings = {}
    if BBREF_STANDINGS_PATH.exists():
        std_df = pd.read_csv(BBREF_STANDINGS_PATH)
        for _, row in std_df.iterrows():
            official_standings[row['team_abbr']] = {
                'wins': int(row['W']),
                'losses': int(row['L']),
                'total_gp': int(row['team_gp'])
            }

    # 3. Calculate Model Stats
    team_records = {}
    for team in set(df['home_team'].unique()) | set(df['away_team'].unique()):
        team_records[team] = {
            'model_wins': 0, 'model_losses': 0, 'adj_wins': 0, 'adj_losses': 0,
            'opp_3pa': 0, 'opp_3pm': 0, 'opp_3pm_exp': 0, 'analyzed_count': 0
        }

    for _, row in df.iterrows():
        home, away = row['home_team'], row['away_team']
        team_records[home]['analyzed_count'] += 1
        team_records[away]['analyzed_count'] += 1
        
        if row['margin_actual'] > 0:
            team_records[home]['model_wins'] += 1
            team_records[away]['model_losses'] += 1
        else:
            team_records[away]['model_wins'] += 1
            team_records[home]['model_losses'] += 1

        if row['margin_adj'] > 0:
            team_records[home]['adj_wins'] += 1
            team_records[away]['adj_losses'] += 1
        else:
            team_records[away]['adj_wins'] += 1
            team_records[home]['adj_losses'] += 1

        team_records[home]['opp_3pa'] += row['away_3pa']
        team_records[home]['opp_3pm'] += row['away_3pm_actual']
        team_records[home]['opp_3pm_exp'] += row['away_3pm_exp']
        team_records[away]['opp_3pa'] += row['home_3pa']
        team_records[away]['opp_3pm'] += row['home_3pm_actual']
        team_records[away]['opp_3pm_exp'] += row['home_3pm_exp']

    # 4. Generate Rows
    sorted_teams = sorted(team_records.keys(), 
                         key=lambda t: official_standings.get(t, {'wins':0})['wins'], 
                         reverse=True)

    team_rows = ""
    for rank, team in enumerate(sorted_teams, 1):
        r = team_records[team]
        if team in official_standings:
            actual_record = f"{official_standings[team]['wins']}-{official_standings[team]['losses']}"
            analyzed_str = f"{r['analyzed_count']}/{official_standings[team]['total_gp']}"
        else:
            actual_record = f"{r['model_wins']}-{r['model_losses']}"
            analyzed_str = f"{r['analyzed_count']} analyzed"

        adj_record = f"{r['adj_wins']}-{r['adj_losses']}"
        win_diff = r['model_wins'] - r['adj_wins']
        diff_class = "positive" if win_diff > 0 else ("negative" if win_diff < 0 else "")
        diff_str = f"{win_diff:+d}" if win_diff != 0 else "0"
        
        opp_3p_pct = (r['opp_3pm'] / r['opp_3pa'] * 100).round(1) if r['opp_3pa'] > 0 else 0
        opp_3p_exp = (r['opp_3pm_exp'] / r['opp_3pa'] * 100).round(1) if r['opp_3pa'] > 0 else 0
        opp_diff = (opp_3p_pct - opp_3p_exp).round(1)
        opp_diff_str = f"{opp_diff:+.1f}%" if opp_diff != 0 else "0.0%"

        team_rows += f"""
        <tr data-team="{team}" data-wins="{official_standings.get(team, {'wins':0})['wins']}" data-adjwins="{r['adj_wins']}">
            <td>{rank}</td>
            <td><strong>{team}</strong> <small style="color:#888; font-size:0.75em;">({analyzed_str})</small></td>
            <td>{actual_record}</td>
            <td>{adj_record}</td>
            <td class="{diff_class} clickable-diff" onclick="showFlippedGames('{team}')">{diff_str}</td>
            <td>{opp_3p_exp}%</td>
            <td>{opp_3p_pct}%</td>
            <td class="">{opp_diff_str}</td>
        </tr>"""

    # 5. Injection
    html = template.replace("{{TEAM_RANKINGS_ROWS}}", team_rows)
    last_game_date = df['date'].max()
    html = html.replace("{{SEASON_DATE}}", last_game_date)
    html = html.replace("{{GAME_COUNT}}", str(len(df)))
    html = html.replace("{{GENERATED_TIMESTAMP}}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    # 6. Reconstruct Calendar Data (Critical for functionality)
    games_by_date = {}
    for _, row in df.iterrows():
        d = row['date']
        if d not in games_by_date: games_by_date[d] = []
        games_by_date[d].append(row.to_dict())
    
    # Top swings rows
    df['abs_delta'] = df['margin_delta'].abs()
    top_swings = df.sort_values('abs_delta', ascending=False).head(15)
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

    html = html.replace("{{BIGGEST_SWINGS_ROWS}}", swing_rows)
    html = html.replace("{{GAMES_JSON}}", json.dumps(games_by_date))
    html = html.replace("{{MOST_RECENT_DATE}}", last_game_date)
    
    # Months for calendar
    dates = pd.to_datetime(df['date'])
    months = sorted(list(set((d.year, d.month-1) for d in dates)))
    month_json = [{"year": m[0], "month": m[1]} for m in months]
    html = html.replace("{{SEASON_MONTHS_JSON}}", json.dumps(month_json))

    output_path = Path("index.html")
    output_path.write_text(html, encoding='utf-8')
    print(f"Homepage updated with Authoritative Standings: {output_path}")

if __name__ == "__main__":
    generate_report()
