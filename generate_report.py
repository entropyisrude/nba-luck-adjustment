"""Generate HTML summary report from adjusted_games.csv"""

import pandas as pd
from pathlib import Path
from datetime import datetime

def generate_report():
    df = pd.read_csv("data/adjusted_games.csv")

    # Calculate team-level stats
    # For home games: positive margin_delta means luck helped home team
    # For away games: negative margin_delta means luck helped away team

    home_luck = df.groupby('home_team').agg({
        'margin_delta': 'sum',
        'home_pts_adj': 'sum',
        'home_pts_actual': 'sum',
        'game_id': 'count'
    }).rename(columns={'game_id': 'home_games', 'margin_delta': 'home_luck'})

    away_luck = df.groupby('away_team').agg({
        'margin_delta': lambda x: -x.sum(),  # Flip sign for away perspective
        'away_pts_adj': 'sum',
        'away_pts_actual': 'sum',
        'game_id': 'count'
    }).rename(columns={'game_id': 'away_games', 'margin_delta': 'away_luck'})

    # Combine
    teams = home_luck.join(away_luck, how='outer').fillna(0)
    teams['total_luck'] = teams['home_luck'] + teams['away_luck']
    teams['total_games'] = teams['home_games'] + teams['away_games']
    teams['luck_per_game'] = teams['total_luck'] / teams['total_games']
    teams = teams.sort_values('total_luck', ascending=False).reset_index()
    teams.columns = ['team'] + list(teams.columns[1:])

    # Biggest swing games (absolute margin_delta)
    df['abs_margin_delta'] = df['margin_delta'].abs()
    biggest_swings = df.nlargest(15, 'abs_margin_delta')[
        ['date', 'home_team', 'away_team', 'home_pts_actual', 'away_pts_actual',
         'margin_actual', 'margin_adj', 'margin_delta']
    ].copy()

    # Recent games (last 2 weeks with significant swings)
    recent = df[df['date'] >= '2026-02-10'].copy()
    recent_notable = recent.nlargest(10, 'abs_margin_delta')[
        ['date', 'home_team', 'away_team', 'home_pts_actual', 'away_pts_actual',
         'margin_actual', 'margin_adj', 'margin_delta']
    ]

    # Generate HTML
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>NBA 3PT Luck Analysis - 2025-26 Season</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1000px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
            color: #333;
        }}
        h1 {{
            color: #1a1a2e;
            border-bottom: 3px solid #e94560;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #16213e;
            margin-top: 40px;
        }}
        .summary {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}
        th {{
            background: #1a1a2e;
            color: white;
            padding: 12px 8px;
            text-align: left;
            font-weight: 600;
        }}
        td {{
            padding: 10px 8px;
            border-bottom: 1px solid #eee;
        }}
        tr:hover {{
            background: #f8f9fa;
        }}
        .positive {{
            color: #28a745;
            font-weight: 600;
        }}
        .negative {{
            color: #dc3545;
            font-weight: 600;
        }}
        .methodology {{
            background: #e8f4f8;
            padding: 15px;
            border-radius: 8px;
            font-size: 0.9em;
            margin-top: 40px;
        }}
        .timestamp {{
            color: #666;
            font-size: 0.85em;
        }}
    </style>
</head>
<body>
    <h1>NBA 3-Point Luck Analysis</h1>
    <p class="timestamp">2025-26 Season through {df['date'].max()} | {len(df)} games analyzed</p>

    <div class="summary">
        <p><strong>What is this?</strong> This analysis adjusts NBA scores based on 3-point shooting luck.
        When a team shoots significantly above or below their expected 3P% (based on each player's historical shooting),
        we calculate what the score "should have been" if shooting regressed to expectation.</p>
        <p><strong>Margin Delta</strong> = Luck-adjusted margin minus actual margin. Positive means the team benefited from luck.</p>
    </div>

    <h2>1. Team Luck Rankings (Season Totals)</h2>
    <p>Cumulative margin points gained/lost due to 3PT variance</p>
    <table>
        <tr>
            <th>Rank</th>
            <th>Team</th>
            <th>Total Luck</th>
            <th>Per Game</th>
            <th>Games</th>
        </tr>
"""

    for i, row in teams.iterrows():
        luck_class = "positive" if row['total_luck'] > 0 else "negative"
        html += f"""        <tr>
            <td>{i+1}</td>
            <td><strong>{row['team']}</strong></td>
            <td class="{luck_class}">{row['total_luck']:+.1f}</td>
            <td class="{luck_class}">{row['luck_per_game']:+.2f}</td>
            <td>{int(row['total_games'])}</td>
        </tr>
"""

    html += """    </table>

    <h2>2. Biggest Luck-Swing Games</h2>
    <p>Games where 3PT variance most dramatically affected the outcome</p>
    <table>
        <tr>
            <th>Date</th>
            <th>Matchup</th>
            <th>Actual</th>
            <th>Adjusted</th>
            <th>Swing</th>
        </tr>
"""

    for _, row in biggest_swings.iterrows():
        swing_class = "positive" if row['margin_delta'] > 0 else "negative"
        winner = row['home_team'] if row['margin_actual'] > 0 else row['away_team']
        adj_winner = row['home_team'] if row['margin_adj'] > 0 else row['away_team']
        flip = "⚠️" if winner != adj_winner else ""
        html += f"""        <tr>
            <td>{row['date']}</td>
            <td>{row['away_team']} @ {row['home_team']}</td>
            <td>{int(row['away_pts_actual'])}-{int(row['home_pts_actual'])}</td>
            <td>{row['margin_adj']:+.1f}</td>
            <td class="{swing_class}">{row['margin_delta']:+.1f} {flip}</td>
        </tr>
"""

    html += """    </table>
    <p><small>⚠️ = Adjusted margin flips the winner</small></p>

    <h2>3. Recent Notable Games</h2>
    <p>Biggest luck swings in the last 2 weeks</p>
    <table>
        <tr>
            <th>Date</th>
            <th>Matchup</th>
            <th>Actual</th>
            <th>Adjusted</th>
            <th>Swing</th>
        </tr>
"""

    for _, row in recent_notable.iterrows():
        swing_class = "positive" if row['margin_delta'] > 0 else "negative"
        winner = row['home_team'] if row['margin_actual'] > 0 else row['away_team']
        adj_winner = row['home_team'] if row['margin_adj'] > 0 else row['away_team']
        flip = "⚠️" if winner != adj_winner else ""
        html += f"""        <tr>
            <td>{row['date']}</td>
            <td>{row['away_team']} @ {row['home_team']}</td>
            <td>{int(row['away_pts_actual'])}-{int(row['home_pts_actual'])}</td>
            <td>{row['margin_adj']:+.1f}</td>
            <td class="{swing_class}">{row['margin_delta']:+.1f} {flip}</td>
        </tr>
"""

    html += f"""    </table>

    <div class="methodology">
        <h3>Methodology</h3>
        <ul>
            <li><strong>Expected 3P%</strong>: Each player's expected make rate uses Bayesian updating with a prior of 36% and κ=400 attempts</li>
            <li><strong>Recency weighting</strong>: Attempts decay with half-life of 2000 3PA to weight recent performance</li>
            <li><strong>ORB correction</strong>: Missed 3s generate offensive rebounds at ~26% rate, worth ~1.12 PPP</li>
            <li><strong>Data source</strong>: cdn.nba.com live boxscores</li>
        </ul>
        <p>Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} | <a href="https://github.com">View on GitHub</a></p>
    </div>
</body>
</html>
"""

    output_path = Path("data/3pt_luck_report.html")
    output_path.write_text(html, encoding='utf-8')
    print(f"Report saved to: {output_path.absolute()}")
    return output_path

if __name__ == "__main__":
    generate_report()
