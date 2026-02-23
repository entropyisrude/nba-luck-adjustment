"""Generate a detailed example page showing how adjustments are calculated."""

import pandas as pd
from pathlib import Path
from datetime import datetime
from src.ingest import get_playbyplay_3pt_shots
from src.adjust import get_player_prior, get_shot_multiplier, LEAGUE_AVG_3P

# Pick an interesting game (PHI @ MIN, Feb 22, 2026 - big 28 point swing)
EXAMPLE_GAME_ID = "0022500824"
EXAMPLE_DATE = "02/22/2026"


def generate_example_page():
    # Load player state - this contains weighted career attempts (A_r) and makes (M_r)
    player_state = pd.read_csv("data/player_state.csv")
    player_state_indexed = player_state.set_index("player_id")

    # Get shot-level data
    shots_df = get_playbyplay_3pt_shots(EXAMPLE_GAME_ID, EXAMPLE_DATE)

    if shots_df.empty:
        print("No shot data found")
        return

    # Get game info from adjusted_games.csv
    games_df = pd.read_csv("data/adjusted_games.csv")
    game_row = games_df[games_df['game_id'].astype(str).str.lstrip('0') == EXAMPLE_GAME_ID.lstrip('0')].iloc[0]

    # Process each shot
    shot_details = []
    for _, shot in shots_df.iterrows():
        pid = shot['PLAYER_ID']
        if pid is None:
            continue
        pid = int(pid)

        # Get player's weighted career stats from player_state
        if pid in player_state_indexed.index:
            A_r = float(player_state_indexed.loc[pid, "A_r"])
            M_r = float(player_state_indexed.loc[pid, "M_r"])
        else:
            A_r = 0.0
            M_r = 0.0

        career_pct = (M_r / A_r * 100) if A_r > 0 else 0

        # Get prior parameters based on weighted attempts
        mu, kappa = get_player_prior(A_r)

        # Calculate Bayesian expected 3P%
        player_exp_pct = (M_r + kappa * mu) / (A_r + kappa)

        # Get shot context multiplier
        area = shot.get('AREA', 'above_break')
        shot_type = shot.get('SHOT_TYPE', 'catch_shoot')
        multiplier = get_shot_multiplier(area, shot_type)

        # Final expected make probability
        final_exp = min(0.55, max(0.15, player_exp_pct * multiplier))

        shot_details.append({
            'player_name': shot['PLAYER_NAME'],
            'team_id': shot['TEAM_ID'],
            'made': shot['MADE'],
            'area': area,
            'shot_type': shot_type,
            'weighted_3pa': round(A_r, 1),
            'weighted_3pm': round(M_r, 1),
            'career_pct': career_pct,
            'prior_mu': mu,
            'prior_kappa': kappa,
            'player_exp_pct': player_exp_pct * 100,
            'multiplier': multiplier,
            'final_exp_pct': final_exp * 100,
            'final_exp_prob': final_exp,
        })

    shots_detail_df = pd.DataFrame(shot_details)

    # Separate by team
    home_team = game_row['home_team']
    away_team = game_row['away_team']

    # Get team IDs from shots
    team_ids = shots_df['TEAM_ID'].unique()
    # Determine which is home/away based on the game data
    home_shots = shots_detail_df[shots_detail_df['team_id'] == team_ids[0]]
    away_shots = shots_detail_df[shots_detail_df['team_id'] == team_ids[1]]

    # Check which is which by comparing totals
    if abs(home_shots['made'].sum() - game_row['home_3pm_actual']) > abs(away_shots['made'].sum() - game_row['home_3pm_actual']):
        home_shots, away_shots = away_shots, home_shots

    # Generate HTML
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Detailed Example: {away_team} @ {home_team} - NBA 3PT Luck Analysis</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
            color: #333;
        }}
        h1 {{ color: #1a1a2e; border-bottom: 3px solid #e94560; padding-bottom: 10px; }}
        h2 {{ color: #16213e; margin-top: 30px; }}
        h3 {{ color: #1a1a2e; margin-top: 25px; }}
        .back-link {{ margin-bottom: 20px; }}
        .back-link a {{ color: #e94560; text-decoration: none; font-weight: 500; }}
        .back-link a:hover {{ text-decoration: underline; }}
        .summary-box {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .game-score {{
            font-size: 1.5em;
            margin: 15px 0;
        }}
        .positive {{ color: #28a745; font-weight: 600; }}
        .negative {{ color: #dc3545; font-weight: 600; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
            font-size: 0.9em;
        }}
        th {{
            background: #1a1a2e;
            color: white;
            padding: 10px 6px;
            text-align: left;
            font-weight: 600;
        }}
        td {{
            padding: 8px 6px;
            border-bottom: 1px solid #eee;
        }}
        tr:hover {{ background: #f8f9fa; }}
        .formula {{
            background: #e8f4f8;
            padding: 15px;
            border-radius: 8px;
            font-family: monospace;
            margin: 15px 0;
            overflow-x: auto;
        }}
        .step {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 15px;
        }}
        .step-number {{
            background: #e94560;
            color: white;
            width: 28px;
            height: 28px;
            border-radius: 50%;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            margin-right: 10px;
        }}
        .totals-row {{
            background: #f0f0f0 !important;
            font-weight: bold;
        }}
        code {{
            background: #e8e8e8;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: monospace;
        }}
    </style>
</head>
<body>
    <div class="back-link"><a href="index.html">&larr; Back to Main Report</a></div>

    <h1>How the Luck Adjustment Works</h1>
    <p>A detailed breakdown using the <strong>{away_team} @ {home_team}</strong> game from <strong>{game_row['date']}</strong></p>

    <div class="summary-box">
        <div class="game-score">
            <strong>Actual Score:</strong> {away_team} {int(game_row['away_pts_actual'])} - {home_team} {int(game_row['home_pts_actual'])}
        </div>
        <div class="game-score">
            <strong>Adjusted Score:</strong> {away_team} {game_row['away_pts_adj']:.1f} - {home_team} {game_row['home_pts_adj']:.1f}
        </div>
        <div class="game-score">
            <strong>Luck Swing:</strong> <span class="{'positive' if game_row['margin_delta'] > 0 else 'negative'}">{game_row['margin_delta']:+.1f} points</span>
            ({"Home team benefited" if game_row['margin_delta'] > 0 else "Away team benefited"})
        </div>
    </div>

    <h2>The Process</h2>

    <div class="step">
        <span class="step-number">1</span>
        <strong>Get Every 3-Point Attempt</strong>
        <p>We fetch play-by-play data and extract each 3-point shot with its context:</p>
        <ul>
            <li><strong>Court location:</strong> Corner 3 vs Above-the-break 3</li>
            <li><strong>Shot type:</strong> Catch-and-shoot, Pullup, Stepback, Running, etc.</li>
        </ul>
        <p>This game had <strong>{len(shots_detail_df)} total 3-point attempts</strong>.</p>
    </div>

    <div class="step">
        <span class="step-number">2</span>
        <strong>Calculate Each Player's Expected 3P%</strong>
        <p>Using Bayesian estimation with a sliding prior. Player stats are weighted with exponential decay (recent shots matter more):</p>
        <div class="formula">
Player Expected 3P% = (Weighted Makes + &kappa; × Prior) / (Weighted Attempts + &kappa;)

Where:
  - Prior (&mu;) scales from 32% (low volume) to 36% (1000+ weighted 3PA)
  - Prior strength (&kappa;) scales from 200 (low volume) to 300 (high volume)
  - Weighted stats use exponential decay so recent performance matters more
        </div>
    </div>

    <div class="step">
        <span class="step-number">3</span>
        <strong>Apply Shot Difficulty Multiplier</strong>
        <p>Each shot type has a difficulty multiplier based on league-average make rates:</p>
        <div class="formula">
Final Expected % = Player Expected % × Shot Difficulty Multiplier

Multipliers (relative to league avg {LEAGUE_AVG_3P*100:.1f}%):
  Corner + Catch-and-shoot: 1.12× (easiest)
  Above-break + Catch-and-shoot: 1.04×
  Corner + Pullup: 1.01×
  Above-break + Pullup: 0.93×
  Above-break + Stepback: 0.90×
  Running shots: ~0.88× (hardest)
        </div>
    </div>

    <div class="step">
        <span class="step-number">4</span>
        <strong>Sum Expected Makes & Calculate Adjustment</strong>
        <p>Sum up expected make probabilities for each team, compare to actual makes:</p>
        <div class="formula">
Luck = Actual Makes - Expected Makes
Point Adjustment = 3 × Luck - (ORB Rate × PPP × Luck)
                 ≈ 2.7 points per make above/below expectation
        </div>
    </div>

    <h2>{home_team} Shot-by-Shot Breakdown</h2>
    <p>Each 3-point attempt by {home_team} with the full calculation:</p>
    <table>
        <tr>
            <th>Player</th>
            <th>Result</th>
            <th>Location</th>
            <th>Shot Type</th>
            <th>Weighted 3P</th>
            <th>Prior (μ, κ)</th>
            <th>Player Exp%</th>
            <th>Multiplier</th>
            <th>Final Exp%</th>
        </tr>
"""

    home_exp_total = 0
    home_made_total = 0
    for _, shot in home_shots.iterrows():
        made_str = "✓ Made" if shot['made'] else "✗ Miss"
        made_class = "positive" if shot['made'] else "negative"
        home_exp_total += shot['final_exp_prob']
        home_made_total += shot['made']
        html += f"""        <tr>
            <td>{shot['player_name']}</td>
            <td class="{made_class}">{made_str}</td>
            <td>{shot['area'].replace('_', ' ').title()}</td>
            <td>{shot['shot_type'].replace('_', ' ').title()}</td>
            <td>{shot['weighted_3pm']}/{shot['weighted_3pa']} ({shot['career_pct']:.1f}%)</td>
            <td>({shot['prior_mu']*100:.1f}%, {shot['prior_kappa']:.0f})</td>
            <td>{shot['player_exp_pct']:.1f}%</td>
            <td>{shot['multiplier']:.3f}</td>
            <td>{shot['final_exp_pct']:.1f}%</td>
        </tr>
"""

    home_luck = home_made_total - home_exp_total
    html += f"""        <tr class="totals-row">
            <td colspan="2"><strong>TOTALS</strong></td>
            <td colspan="6"></td>
            <td><strong>{home_exp_total:.1f} exp</strong></td>
        </tr>
        <tr class="totals-row">
            <td colspan="2"><strong>Actual: {home_made_total} made</strong></td>
            <td colspan="6"></td>
            <td class="{'positive' if home_luck > 0 else 'negative'}"><strong>{home_luck:+.1f} luck</strong></td>
        </tr>
    </table>

    <h3>{home_team} Player Summary</h3>
    <table>
        <tr>
            <th>Player</th>
            <th>Shots</th>
            <th>Made</th>
            <th>Expected Makes</th>
            <th>Luck</th>
            <th>Point Impact</th>
        </tr>
"""

    # Group home shots by player
    home_player_stats = home_shots.groupby('player_name').agg({
        'made': ['count', 'sum'],
        'final_exp_prob': 'sum'
    }).reset_index()
    home_player_stats.columns = ['player_name', 'shots', 'made', 'expected']
    home_player_stats['luck'] = home_player_stats['made'] - home_player_stats['expected']
    home_player_stats['point_impact'] = home_player_stats['luck'] * 2.7
    home_player_stats = home_player_stats.sort_values('luck', ascending=False)

    for _, p in home_player_stats.iterrows():
        luck_class = "positive" if p['luck'] > 0 else "negative" if p['luck'] < 0 else ""
        html += f"""        <tr>
            <td>{p['player_name']}</td>
            <td>{int(p['shots'])}</td>
            <td>{int(p['made'])}</td>
            <td>{p['expected']:.2f}</td>
            <td class="{luck_class}">{p['luck']:+.2f}</td>
            <td class="{luck_class}">{p['point_impact']:+.1f}</td>
        </tr>
"""

    html += f"""        <tr class="totals-row">
            <td><strong>TOTAL</strong></td>
            <td><strong>{len(home_shots)}</strong></td>
            <td><strong>{home_made_total}</strong></td>
            <td><strong>{home_exp_total:.2f}</strong></td>
            <td class="{'positive' if home_luck > 0 else 'negative'}"><strong>{home_luck:+.2f}</strong></td>
            <td class="{'positive' if home_luck > 0 else 'negative'}"><strong>{home_luck * 2.7:+.1f}</strong></td>
        </tr>
    </table>

    <h2>{away_team} Shot-by-Shot Breakdown</h2>
    <p>Each 3-point attempt by {away_team} with the full calculation:</p>
    <table>
        <tr>
            <th>Player</th>
            <th>Result</th>
            <th>Location</th>
            <th>Shot Type</th>
            <th>Weighted 3P</th>
            <th>Prior (μ, κ)</th>
            <th>Player Exp%</th>
            <th>Multiplier</th>
            <th>Final Exp%</th>
        </tr>
"""

    away_exp_total = 0
    away_made_total = 0
    for _, shot in away_shots.iterrows():
        made_str = "✓ Made" if shot['made'] else "✗ Miss"
        made_class = "positive" if shot['made'] else "negative"
        away_exp_total += shot['final_exp_prob']
        away_made_total += shot['made']
        html += f"""        <tr>
            <td>{shot['player_name']}</td>
            <td class="{made_class}">{made_str}</td>
            <td>{shot['area'].replace('_', ' ').title()}</td>
            <td>{shot['shot_type'].replace('_', ' ').title()}</td>
            <td>{shot['weighted_3pm']}/{shot['weighted_3pa']} ({shot['career_pct']:.1f}%)</td>
            <td>({shot['prior_mu']*100:.1f}%, {shot['prior_kappa']:.0f})</td>
            <td>{shot['player_exp_pct']:.1f}%</td>
            <td>{shot['multiplier']:.3f}</td>
            <td>{shot['final_exp_pct']:.1f}%</td>
        </tr>
"""

    away_luck = away_made_total - away_exp_total
    html += f"""        <tr class="totals-row">
            <td colspan="2"><strong>TOTALS</strong></td>
            <td colspan="6"></td>
            <td><strong>{away_exp_total:.1f} exp</strong></td>
        </tr>
        <tr class="totals-row">
            <td colspan="2"><strong>Actual: {away_made_total} made</strong></td>
            <td colspan="6"></td>
            <td class="{'positive' if away_luck > 0 else 'negative'}"><strong>{away_luck:+.1f} luck</strong></td>
        </tr>
    </table>

    <h3>{away_team} Player Summary</h3>
    <table>
        <tr>
            <th>Player</th>
            <th>Shots</th>
            <th>Made</th>
            <th>Expected Makes</th>
            <th>Luck</th>
            <th>Point Impact</th>
        </tr>
"""

    # Group away shots by player
    away_player_stats = away_shots.groupby('player_name').agg({
        'made': ['count', 'sum'],
        'final_exp_prob': 'sum'
    }).reset_index()
    away_player_stats.columns = ['player_name', 'shots', 'made', 'expected']
    away_player_stats['luck'] = away_player_stats['made'] - away_player_stats['expected']
    away_player_stats['point_impact'] = away_player_stats['luck'] * 2.7
    away_player_stats = away_player_stats.sort_values('luck', ascending=False)

    for _, p in away_player_stats.iterrows():
        luck_class = "positive" if p['luck'] > 0 else "negative" if p['luck'] < 0 else ""
        html += f"""        <tr>
            <td>{p['player_name']}</td>
            <td>{int(p['shots'])}</td>
            <td>{int(p['made'])}</td>
            <td>{p['expected']:.2f}</td>
            <td class="{luck_class}">{p['luck']:+.2f}</td>
            <td class="{luck_class}">{p['point_impact']:+.1f}</td>
        </tr>
"""

    html += f"""        <tr class="totals-row">
            <td><strong>TOTAL</strong></td>
            <td><strong>{len(away_shots)}</strong></td>
            <td><strong>{away_made_total}</strong></td>
            <td><strong>{away_exp_total:.2f}</strong></td>
            <td class="{'positive' if away_luck > 0 else 'negative'}"><strong>{away_luck:+.2f}</strong></td>
            <td class="{'positive' if away_luck > 0 else 'negative'}"><strong>{away_luck * 2.7:+.1f}</strong></td>
        </tr>
    </table>

    <h2>Final Calculation</h2>
    <div class="summary-box">
        <h3>{home_team}</h3>
        <ul>
            <li>Actual 3PM: <strong>{home_made_total}</strong></li>
            <li>Expected 3PM: <strong>{home_exp_total:.1f}</strong></li>
            <li>Luck (makes above/below expected): <strong class="{'positive' if home_luck > 0 else 'negative'}">{home_luck:+.1f}</strong></li>
        </ul>

        <h3>{away_team}</h3>
        <ul>
            <li>Actual 3PM: <strong>{away_made_total}</strong></li>
            <li>Expected 3PM: <strong>{away_exp_total:.1f}</strong></li>
            <li>Luck (makes above/below expected): <strong class="{'positive' if away_luck > 0 else 'negative'}">{away_luck:+.1f}</strong></li>
        </ul>

        <h3>ORB Adjustment Calculation</h3>
        <p>Each lucky make isn't worth a full 3 points because missed 3s generate offensive rebounds (~27% ORB rate)
        that lead to additional scoring (~1.1 points per possession). We subtract this "lost opportunity" value:</p>
        <div class="formula">
Point Adjustment = Raw 3PT Value − ORB Opportunity Cost
                 = (3 × Luck) − (ORB Rate × PPP × |Luck|)
                 = (3 × Luck) − (0.27 × 1.1 × |Luck|)
                 ≈ 2.7 × Luck
        </div>

        <table style="max-width: 600px;">
            <tr>
                <th>Team</th>
                <th>Luck</th>
                <th>Raw 3PT Value</th>
                <th>ORB Adjustment</th>
                <th>Net Point Impact</th>
            </tr>
            <tr>
                <td>{home_team}</td>
                <td class="{'positive' if home_luck > 0 else 'negative'}">{home_luck:+.2f}</td>
                <td>{home_luck * 3:+.1f}</td>
                <td>{-home_luck * 0.297:+.1f}</td>
                <td class="{'positive' if home_luck > 0 else 'negative'}"><strong>{home_luck * 2.7:+.1f}</strong></td>
            </tr>
            <tr>
                <td>{away_team}</td>
                <td class="{'positive' if away_luck > 0 else 'negative'}">{away_luck:+.2f}</td>
                <td>{away_luck * 3:+.1f}</td>
                <td>{-away_luck * 0.297:+.1f}</td>
                <td class="{'positive' if away_luck > 0 else 'negative'}"><strong>{away_luck * 2.7:+.1f}</strong></td>
            </tr>
        </table>

        <h3>Net Result</h3>
        <p>The luck differential of <strong>{home_luck - away_luck:+.1f} makes</strong> in favor of {home_team if home_luck > away_luck else away_team}
        translates to <strong>{abs((home_luck - away_luck) * 2.7):.1f} points</strong> of margin swing after ORB adjustment.</p>
        <p><strong>Actual margin:</strong> {home_team} {'+' if (game_row['home_pts_actual'] - game_row['away_pts_actual']) >= 0 else ''}{int(game_row['home_pts_actual'] - game_row['away_pts_actual'])}</p>
        <p><strong>Adjusted margin:</strong> {home_team} {'+' if (game_row['home_pts_adj'] - game_row['away_pts_adj']) >= 0 else ''}{(game_row['home_pts_adj'] - game_row['away_pts_adj']):.1f}</p>
    </div>

    <div class="back-link" style="margin-top: 30px;"><a href="index.html">&larr; Back to Main Report</a></div>

    <p style="color: #666; font-size: 0.85em; margin-top: 40px;">
        Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} |
        <a href="https://github.com/entropyisrude/nba-luck-adjustment" target="_blank">View on GitHub</a>
    </p>
</body>
</html>
"""

    output_path = Path("data/example_breakdown.html")
    output_path.write_text(html, encoding='utf-8')
    print(f"Example page saved to: {output_path.absolute()}")

    # Also copy to root for GitHub Pages
    root_path = Path("example.html")
    root_path.write_text(html, encoding='utf-8')
    print(f"Also saved to: {root_path.absolute()}")


if __name__ == "__main__":
    generate_example_page()
