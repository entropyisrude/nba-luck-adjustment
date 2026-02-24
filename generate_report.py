"""Generate HTML summary report from adjusted_games.csv"""

import pandas as pd
import json
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

    # Calculate actual and adjusted records for each team, plus opponent 3P% stats
    team_records = {}
    for team in set(df['home_team'].unique()) | set(df['away_team'].unique()):
        team_records[team] = {
            'wins': 0, 'losses': 0, 'adj_wins': 0, 'adj_losses': 0,
            'opp_3pa': 0, 'opp_3pm': 0, 'opp_3pm_exp': 0
        }

    for _, row in df.iterrows():
        home = row['home_team']
        away = row['away_team']
        actual_margin = row['margin_actual']
        adj_margin = row['margin_adj']

        # Actual record
        if actual_margin > 0:  # Home win
            team_records[home]['wins'] += 1
            team_records[away]['losses'] += 1
        else:  # Away win
            team_records[away]['wins'] += 1
            team_records[home]['losses'] += 1

        # Adjusted record
        if adj_margin > 0:  # Home would have won
            team_records[home]['adj_wins'] += 1
            team_records[away]['adj_losses'] += 1
        else:  # Away would have won
            team_records[away]['adj_wins'] += 1
            team_records[home]['adj_losses'] += 1

        # Opponent 3P stats (for home team, opponent is away team and vice versa)
        team_records[home]['opp_3pa'] += row['away_3pa']
        team_records[home]['opp_3pm'] += row['away_3pm_actual']
        team_records[home]['opp_3pm_exp'] += row['away_3pm_exp']

        team_records[away]['opp_3pa'] += row['home_3pa']
        team_records[away]['opp_3pm'] += row['home_3pm_actual']
        team_records[away]['opp_3pm_exp'] += row['home_3pm_exp']

    teams = teams.reset_index()
    teams.columns = ['team'] + list(teams.columns[1:])

    # Add records to teams dataframe
    teams['wins'] = teams['team'].map(lambda t: team_records.get(t, {}).get('wins', 0))
    teams['losses'] = teams['team'].map(lambda t: team_records.get(t, {}).get('losses', 0))
    teams['adj_wins'] = teams['team'].map(lambda t: team_records.get(t, {}).get('adj_wins', 0))
    teams['adj_losses'] = teams['team'].map(lambda t: team_records.get(t, {}).get('adj_losses', 0))

    # Add opponent 3P% stats
    teams['opp_3pa'] = teams['team'].map(lambda t: team_records.get(t, {}).get('opp_3pa', 0))
    teams['opp_3pm'] = teams['team'].map(lambda t: team_records.get(t, {}).get('opp_3pm', 0))
    teams['opp_3pm_exp'] = teams['team'].map(lambda t: team_records.get(t, {}).get('opp_3pm_exp', 0))
    teams['opp_3p_pct'] = (teams['opp_3pm'] / teams['opp_3pa'] * 100).round(1)
    teams['opp_3p_exp_pct'] = (teams['opp_3pm_exp'] / teams['opp_3pa'] * 100).round(1)

    teams = teams.sort_values('opp_3p_pct', ascending=True)  # Sort by opponent actual 3P% (lower is better defense)

    # Biggest swing games (absolute margin_delta)
    df['abs_margin_delta'] = df['margin_delta'].abs()
    biggest_swings = df.nlargest(15, 'abs_margin_delta')[
        ['date', 'home_team', 'away_team', 'home_pts_actual', 'away_pts_actual',
         'home_pts_adj', 'away_pts_adj', 'margin_actual', 'margin_adj', 'margin_delta']
    ].copy()

    # Prepare games data as JSON for calendar
    # Check if swing_player columns exist (for backward compatibility)
    has_swing_player = 'swing_player' in df.columns
    has_top_swing_players = 'top_swing_players' in df.columns

    json_cols = ['date', 'home_team', 'away_team', 'home_pts_actual', 'away_pts_actual',
                 'home_pts_adj', 'away_pts_adj', 'margin_actual', 'margin_adj', 'margin_delta',
                 'home_3pa', 'home_3pm_actual', 'home_3pm_exp',
                 'away_3pa', 'away_3pm_actual', 'away_3pm_exp']
    if has_swing_player:
        json_cols += ['swing_player', 'swing_player_delta']
        # Add shooting stats if available
        if 'swing_player_fg3m' in df.columns:
            json_cols += ['swing_player_fg3m', 'swing_player_fg3a']
    if has_top_swing_players:
        json_cols += ['top_swing_players']

    games_for_json = df[json_cols].copy()
    games_for_json = games_for_json.sort_values('date')

    # Group games by date for JSON
    games_by_date = {}
    for _, row in games_for_json.iterrows():
        date = row['date']
        if date not in games_by_date:
            games_by_date[date] = []
        game_obj = {
            'home_team': row['home_team'],
            'away_team': row['away_team'],
            'home_pts_actual': int(row['home_pts_actual']),
            'away_pts_actual': int(row['away_pts_actual']),
            'home_pts_adj': round(row['home_pts_adj'], 1),
            'away_pts_adj': round(row['away_pts_adj'], 1),
            'margin_actual': int(row['margin_actual']),
            'margin_adj': round(row['margin_adj'], 2),
            'margin_delta': round(row['margin_delta'], 1),
            'home_3pa': int(row['home_3pa']),
            'home_3pm': int(row['home_3pm_actual']),
            'home_3pm_exp': round(row['home_3pm_exp'], 1),
            'away_3pa': int(row['away_3pa']),
            'away_3pm': int(row['away_3pm_actual']),
            'away_3pm_exp': round(row['away_3pm_exp'], 1),
        }
        if has_swing_player:
            game_obj['swing_player'] = row['swing_player'] if pd.notna(row['swing_player']) else ''
            game_obj['swing_player_delta'] = round(row['swing_player_delta'], 1) if pd.notna(row['swing_player_delta']) else 0
            # Add shooting stats if available
            if 'swing_player_fg3m' in row and pd.notna(row.get('swing_player_fg3m')):
                game_obj['swing_player_fg3m'] = int(row['swing_player_fg3m'])
                game_obj['swing_player_fg3a'] = int(row['swing_player_fg3a'])
        if has_top_swing_players and pd.notna(row.get('top_swing_players')):
            # Parse the JSON string into a list
            try:
                game_obj['top_swing_players'] = json.loads(row['top_swing_players'])
            except (json.JSONDecodeError, TypeError):
                game_obj['top_swing_players'] = []
        games_by_date[date].append(game_obj)

    games_json = json.dumps(games_by_date)
    most_recent_date = df['date'].max()

    # Determine date range from data for calendar
    min_date = pd.to_datetime(df['date'].min())
    max_date = pd.to_datetime(df['date'].max())

    # Build list of months to display (from first game month to last game month)
    season_months = []
    current = min_date.replace(day=1)
    while current <= max_date:
        season_months.append({'year': current.year, 'month': current.month - 1})  # JS months are 0-indexed
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    season_months_json = json.dumps(season_months)

    # Generate HTML
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>NBA 3PT Luck Analysis - 2025-26 Season</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1100px;
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

        /* Navigation Links */
        .nav-links {{
            display: flex;
            gap: 20px;
            margin: 20px 0;
            flex-wrap: wrap;
        }}
        .nav-links a {{
            background: #1a1a2e;
            color: white;
            padding: 10px 20px;
            border-radius: 6px;
            text-decoration: none;
            font-weight: 500;
            transition: background 0.2s;
        }}
        .nav-links a:hover {{
            background: #e94560;
        }}

        /* Calendar Styles */
        .calendar-container {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .calendar-grid {{
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            justify-content: center;
        }}
        .month {{
            width: 280px;
        }}
        .month-title {{
            text-align: center;
            font-weight: 600;
            color: #1a1a2e;
            margin-bottom: 10px;
            font-size: 1.1em;
        }}
        .weekdays {{
            display: grid;
            grid-template-columns: repeat(7, 1fr);
            text-align: center;
            font-size: 0.8em;
            color: #666;
            margin-bottom: 5px;
        }}
        .days {{
            display: grid;
            grid-template-columns: repeat(7, 1fr);
            gap: 2px;
        }}
        .day {{
            aspect-ratio: 1;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.9em;
            border-radius: 4px;
            color: #999;
        }}
        .day.has-games {{
            background: #e8f4f8;
            color: #1a1a2e;
            cursor: pointer;
            font-weight: 500;
        }}
        .day.has-games:hover {{
            background: #c8e4f0;
        }}
        .day.selected {{
            background: #e94560 !important;
            color: white !important;
        }}
        .day.empty {{
            visibility: hidden;
        }}

        /* Selected Date Games */
        .selected-date-section {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}
        .selected-date-title {{
            font-size: 1.3em;
            font-weight: 600;
            color: #1a1a2e;
            margin-bottom: 15px;
        }}
        #games-table-container {{
            min-height: 100px;
        }}
        .no-games {{
            color: #666;
            font-style: italic;
            padding: 20px;
            text-align: center;
        }}
        .winner-flip {{
            color: #e94560;
            font-weight: bold;
        }}
        tr.winner-flipped {{
            background: #ffe0e0 !important;
        }}
        tr.winner-flipped:hover {{
            background: #ffd0d0 !important;
        }}

        /* Game Box Scoreboard Styles */
        .games-container {{
            display: flex;
            flex-wrap: wrap;
            gap: 15px;
        }}
        .game-box {{
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            width: 340px;
            overflow: hidden;
        }}
        .game-box.flipped {{
            box-shadow: 0 2px 8px rgba(233, 69, 96, 0.4);
            border: 2px solid #e94560;
        }}
        .scores-section {{
            display: flex;
        }}
        .score-column {{
            flex: 1;
            padding: 10px;
            background: white;
        }}
        .score-column.actual {{
            border-right: 1px solid #eee;
        }}
        .column-header {{
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-weight: 700;
            margin-bottom: 8px;
            color: #333;
        }}
        .team-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 4px 0;
            font-size: 15px;
        }}
        .team-abbrev {{
            font-weight: 600;
            width: 40px;
            color: #333;
        }}
        .team-score {{
            font-weight: 700;
            font-size: 18px;
        }}
        .team-score.winner {{
            color: #16a34a;
        }}
        .team-score.loser {{
            color: #666;
        }}
        .details-section {{
            padding: 10px;
            font-size: 12px;
            border-top: 1px solid #eee;
        }}
        .three-pt-row {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 4px;
        }}
        .three-pt-team {{
            font-weight: 600;
            width: 35px;
        }}
        .three-pt-stats {{
            color: #666;
        }}
        .hot {{ color: #dc3545; font-weight: 600; }}
        .cold {{ color: #28a745; font-weight: 600; }}
        .swing-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 8px;
            padding-top: 8px;
            border-top: 1px solid #eee;
        }}
        .luck-label {{
            font-weight: 600;
        }}
        .luck-team {{
            font-weight: 700;
        }}
        .luck-value {{
            font-weight: 700;
            font-size: 14px;
            color: #28a745;
        }}
        .flip-indicator {{
            background: #e94560;
            color: white;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 10px;
            font-weight: 600;
        }}
        .swing-players {{
            font-size: 11px;
            color: #666;
            margin-top: 6px;
        }}
        .swing-players .player-line {{
            margin-top: 3px;
        }}
        .swing-players strong {{
            color: #333;
        }}
        .swing-players .team-label {{
            font-weight: 700;
            color: #444;
        }}

        /* Sortable table headers */
        th.sortable {{
            cursor: pointer;
            user-select: none;
            position: relative;
            padding-right: 20px;
        }}
        th.sortable:hover {{
            background: #2a2a4e;
        }}
        th.sortable::after {{
            content: '⇅';
            position: absolute;
            right: 6px;
            opacity: 0.5;
            font-size: 0.8em;
        }}
        th.sortable.asc::after {{
            content: '↑';
            opacity: 1;
        }}
        th.sortable.desc::after {{
            content: '↓';
            opacity: 1;
        }}

        /* Clickable diff cells */
        .clickable-diff {{
            cursor: pointer;
            text-decoration: underline;
            text-decoration-style: dotted;
        }}
        .clickable-diff:hover {{
            text-decoration-style: solid;
        }}

        /* Modal styles */
        .modal-overlay {{
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.5);
            z-index: 1000;
            justify-content: center;
            align-items: center;
        }}
        .modal-overlay.active {{
            display: flex;
        }}
        .modal {{
            background: white;
            border-radius: 8px;
            max-width: 800px;
            max-height: 80vh;
            overflow-y: auto;
            box-shadow: 0 4px 20px rgba(0,0,0,0.3);
            position: relative;
        }}
        .modal-header {{
            background: #1a1a2e;
            color: white;
            padding: 15px 20px;
            font-weight: 600;
            font-size: 1.1em;
            position: sticky;
            top: 0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .modal-close {{
            background: none;
            border: none;
            color: white;
            font-size: 1.5em;
            cursor: pointer;
            padding: 0 5px;
        }}
        .modal-close:hover {{
            color: #e94560;
        }}
        .modal-body {{
            padding: 20px;
        }}
        .modal table {{
            margin-bottom: 0;
        }}
        .lucky-win {{
            color: #28a745;
        }}
        .unlucky-loss {{
            color: #dc3545;
        }}
    </style>
</head>
<body>
    <h1>NBA 3-Point Luck Analysis</h1>
    <p class="timestamp">2025-26 Season through {df['date'].max()} | {len(df)} games analyzed</p>

    <div class="summary">
        <p><strong>What is this?</strong> How much did the notorious variance of 3pt shooting affect the outcome of the game? This page attempts to take some (but far from all) luck out of the boxscore equation by reimagining every NBA result as if the 3 point gods played no favorites. That is, if every shot went in according to its long term expected outcome. Of course, those outcomes are an imperfect science and this analysis does not use every conceivable piece of tracking data. But it also does not merely resort to league or team average. Instead, it looks at every shot and (1) What player shot it (2) The general shot difficulty (Catch and Shoot, pull-up, step-back etc.) It omits plenty of information, including how closely guarded the shot was according to tracking data. Please see the <a href="example.html" style="color: #e94560; font-weight: 500;">dirty details here</a>.</p>
    </div>

    <div class="nav-links">
        <a href="#team-rankings">Team Rankings</a>
        <a href="#biggest-swings">Highest Variance</a>
        <a href="#methodology">Methodology</a>
    </div>

    <h2>Games by Date</h2>
    <div class="selected-date-section">
        <div class="selected-date-title" id="selected-date-title">Select a date below</div>
        <div id="games-table-container">
            <p class="no-games">Click on a highlighted date to see games</p>
        </div>
    </div>

    <div class="calendar-container">
        <div class="calendar-grid" id="calendar"></div>
    </div>

    <h2 id="team-rankings">Team Rankings</h2>
    <p>Opponent 3P% vs expected based on shooter skill and shot difficulty. Click column headers to sort.</p>
    <table id="team-table">
        <tr>
            <th>#</th>
            <th class="sortable" data-sort="team" data-type="string">Team</th>
            <th class="sortable" data-sort="wins" data-type="number">Record</th>
            <th class="sortable" data-sort="adjwins" data-type="number">Adj Record</th>
            <th class="sortable" data-sort="diff" data-type="number">Net Games Swung</th>
            <th class="sortable" data-sort="oppexp" data-type="number">Opp Exp 3P%</th>
            <th class="sortable asc" data-sort="oppact" data-type="number">Opp Actual 3P%</th>
            <th class="sortable" data-sort="oppdiff" data-type="number">3P% Diff</th>
        </tr>
"""

    for rank, (_, row) in enumerate(teams.iterrows(), 1):
        record = f"{int(row['wins'])}-{int(row['losses'])}"
        adj_record = f"{int(row['adj_wins'])}-{int(row['adj_losses'])}"
        # Diff = actual wins - adjusted wins (positive = lucky, got more wins than deserved)
        win_diff = int(row['wins']) - int(row['adj_wins'])
        diff_class = "positive" if win_diff > 0 else ("negative" if win_diff < 0 else "")
        diff_str = f"{win_diff:+d}" if win_diff != 0 else "0"
        # Opponent 3P% - lower actual than expected is good (opponents shot worse than expected)
        opp_diff = row['opp_3p_pct'] - row['opp_3p_exp_pct']
        opp_diff_class = "positive" if opp_diff < 0 else ("negative" if opp_diff > 0 else "")
        opp_diff_str = f"{opp_diff:+.1f}%"
        html += f"""        <tr data-team="{row['team']}" data-wins="{int(row['wins'])}" data-adjwins="{int(row['adj_wins'])}" data-diff="{win_diff}" data-oppexp="{row['opp_3p_exp_pct']:.1f}" data-oppact="{row['opp_3p_pct']:.1f}" data-oppdiff="{opp_diff:.1f}">
            <td>{rank}</td>
            <td><strong>{row['team']}</strong></td>
            <td>{record}</td>
            <td>{adj_record}</td>
            <td class="{diff_class} clickable-diff" onclick="showFlippedGames('{row['team']}')">{diff_str}</td>
            <td>{row['opp_3p_exp_pct']:.1f}%</td>
            <td>{row['opp_3p_pct']:.1f}%</td>
            <td class="{opp_diff_class}">{opp_diff_str}</td>
        </tr>
"""

    html += """    </table>

    <h2 id="biggest-swings">Highest 3PT Variance Games</h2>
    <p>Games where 3PT variance most dramatically affected the outcome</p>
    <table>
        <tr>
            <th>Date</th>
            <th>Matchup</th>
            <th>Actual</th>
            <th>Adjusted</th>
            <th>3PT Luck</th>
        </tr>
"""

    for _, row in biggest_swings.iterrows():
        winner = row['home_team'] if row['margin_actual'] > 0 else row['away_team']
        adj_winner = row['home_team'] if row['margin_adj'] > 0 else row['away_team']
        flip = "&#9888;" if winner != adj_winner else ""
        # Format adjusted score with winner in bold
        away_adj = f"{row['away_pts_adj']:.1f}"
        home_adj = f"{row['home_pts_adj']:.1f}"
        if row['margin_adj'] > 0:  # Home team wins adjusted
            adj_score = f"{away_adj}-<strong>{home_adj}</strong>"
        else:  # Away team wins adjusted
            adj_score = f"<strong>{away_adj}</strong>-{home_adj}"
        # Calculate lucky team: negative margin_delta = home lucky, positive = away lucky
        lucky_team = row['home_team'] if row['margin_delta'] < 0 else row['away_team']
        luck_amount = abs(row['margin_delta'])
        html += f"""        <tr>
            <td>{row['date']}</td>
            <td>{row['away_team']} @ {row['home_team']}</td>
            <td>{int(row['away_pts_actual'])}-{int(row['home_pts_actual'])}</td>
            <td>{adj_score} {flip}</td>
            <td class="positive">{lucky_team}: +{luck_amount:.1f}</td>
        </tr>
"""

    html += f"""    </table>
    <p><small>&#9888; = Adjusted margin flips the winner</small></p>

    <div class="methodology" id="methodology">
        <h3>Methodology</h3>
        <p>This analysis calculates what NBA scores "should have been" by adjusting for 3-point shooting luck on a <strong>shot-by-shot basis</strong>.
        <a href="example.html" target="_blank" style="color: #e94560; font-weight: 500;">See a detailed example breakdown &rarr;</a></p>

        <h4>Shot Context Model</h4>
        <p>Each 3-point attempt is analyzed using play-by-play data to determine its difficulty:</p>
        <ul>
            <li><strong>Court location</strong>: Corner 3s vs above-the-break 3s</li>
            <li><strong>Shot type</strong>: Catch-and-shoot, pullup, stepback, running, fadeaway, turnaround</li>
        </ul>
        <p>Expected make probability is calculated using a <strong>multiplicative adjustment</strong>:</p>
        <p style="margin-left: 20px;"><code>expected = player_3P% × shot_difficulty_multiplier</code></p>
        <p>Example multipliers (relative to league avg): Corner C&S = 1.12×, Above-break C&S = 1.04×, Pullup = 0.93×, Stepback = 0.90×</p>

        <h4>Player Expected 3P%</h4>
        <p>Each player's baseline expected make rate uses Bayesian estimation with a <strong>sliding prior</strong> based on career experience:</p>
        <ul>
            <li><strong>Prior 3P%</strong>: Scales from 32% (rookies) to 36% (veterans with 1000+ career 3PA)</li>
            <li><strong>Prior strength (kappa)</strong>: Scales from 200 (rookies) to 300 (veterans)</li>
            <li><strong>Formula</strong>: player_3P% = (career_makes + kappa × prior) / (career_attempts + kappa)</li>
        </ul>
        <p>Rookies regress toward a conservative 32% baseline, while veterans' expectations reflect their actual career shooting.</p>

        <h4>In-Season Adjustment</h4>
        <ul>
            <li><strong>Recency weighting</strong>: In-season attempts decay with a half-life of 2000 3PA</li>
            <li>This allows for gradual adjustment if a player's shooting changes mid-season</li>
        </ul>

        <h4>Point Adjustment</h4>
        <ul>
            <li><strong>Base adjustment</strong>: 3 points per made/missed three above/below expectation</li>
            <li><strong>ORB correction</strong>: Missed 3s generate offensive rebounds at ~26% rate, worth ~1.12 PPP, reducing the net impact slightly</li>
        </ul>

        <h4>Data</h4>
        <ul>
            <li><strong>Source</strong>: cdn.nba.com play-by-play and boxscores</li>
            <li><strong>Career stats</strong>: nba_api for historical player data</li>
            <li><strong>Updates</strong>: Automatically refreshed daily</li>
        </ul>

        <p class="timestamp">Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} | <a href="https://github.com/entropyisrude/nba-luck-adjustment" target="_blank">View on GitHub</a></p>
    </div>

    <script>
    // Game data embedded as JSON
    const gamesByDate = {games_json};
    const mostRecentDate = "{most_recent_date}";

    // Month names
    const monthNames = ['January', 'February', 'March', 'April', 'May', 'June',
                        'July', 'August', 'September', 'October', 'November', 'December'];
    const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

    // Season months: dynamically generated from game data
    const seasonMonths = {season_months_json};

    let selectedDate = null;

    function formatDate(year, month, day) {{
        return `${{year}}-${{String(month + 1).padStart(2, '0')}}-${{String(day).padStart(2, '0')}}`;
    }}

    function renderCalendar() {{
        const calendar = document.getElementById('calendar');
        calendar.innerHTML = '';

        seasonMonths.forEach(({{ year, month }}) => {{
            const monthDiv = document.createElement('div');
            monthDiv.className = 'month';

            // Month title
            const title = document.createElement('div');
            title.className = 'month-title';
            title.textContent = `${{monthNames[month]}} ${{year}}`;
            monthDiv.appendChild(title);

            // Weekday headers
            const weekdays = document.createElement('div');
            weekdays.className = 'weekdays';
            dayNames.forEach(d => {{
                const wd = document.createElement('div');
                wd.textContent = d;
                weekdays.appendChild(wd);
            }});
            monthDiv.appendChild(weekdays);

            // Days grid
            const daysDiv = document.createElement('div');
            daysDiv.className = 'days';

            const firstDay = new Date(year, month, 1).getDay();
            const daysInMonth = new Date(year, month + 1, 0).getDate();

            // Empty cells before first day
            for (let i = 0; i < firstDay; i++) {{
                const empty = document.createElement('div');
                empty.className = 'day empty';
                daysDiv.appendChild(empty);
            }}

            // Day cells
            for (let day = 1; day <= daysInMonth; day++) {{
                const dayDiv = document.createElement('div');
                dayDiv.className = 'day';
                dayDiv.textContent = day;

                const dateStr = formatDate(year, month, day);
                if (gamesByDate[dateStr]) {{
                    dayDiv.classList.add('has-games');
                    dayDiv.dataset.date = dateStr;
                    dayDiv.addEventListener('click', () => selectDate(dateStr));
                }}

                daysDiv.appendChild(dayDiv);
            }}

            monthDiv.appendChild(daysDiv);
            calendar.appendChild(monthDiv);
        }});
    }}

    function selectDate(dateStr) {{
        // Remove previous selection
        document.querySelectorAll('.day.selected').forEach(el => el.classList.remove('selected'));

        // Add selection to clicked day
        document.querySelectorAll(`.day[data-date="${{dateStr}}"]`).forEach(el => el.classList.add('selected'));

        selectedDate = dateStr;
        renderGamesTable(dateStr);
    }}

    function renderGamesTable(dateStr) {{
        const container = document.getElementById('games-table-container');
        const titleEl = document.getElementById('selected-date-title');

        const games = gamesByDate[dateStr];

        if (!games || games.length === 0) {{
            titleEl.textContent = dateStr;
            container.innerHTML = '<p class="no-games">No games on this date</p>';
            return;
        }}

        titleEl.textContent = `${{dateStr}} (${{games.length}} game${{games.length > 1 ? 's' : ''}})`;

        // Sort games by absolute margin_delta (largest swing first)
        const sortedGames = [...games].sort((a, b) => Math.abs(b.margin_delta) - Math.abs(a.margin_delta));

        let html = '<div class="games-container">';

        sortedGames.forEach(game => {{
            const actualWinner = game.margin_actual > 0 ? game.home_team : game.away_team;
            const adjWinner = game.margin_adj > 0 ? game.home_team : game.away_team;
            const isFlipped = actualWinner !== adjWinner;

            // Determine actual winners/losers
            const awayActualWinner = game.away_pts_actual > game.home_pts_actual;
            const homeActualWinner = game.home_pts_actual > game.away_pts_actual;
            const awayAdjWinner = game.away_pts_adj > game.home_pts_adj;
            const homeAdjWinner = game.home_pts_adj > game.away_pts_adj;

            // Format 3P% stats
            const away3pPct = game.away_3pa > 0 ? (game.away_3pm / game.away_3pa * 100).toFixed(1) : '0.0';
            const away3pExpPct = game.away_3pa > 0 ? (game.away_3pm_exp / game.away_3pa * 100).toFixed(1) : '0.0';
            const away3pDiff = parseFloat(away3pPct) - parseFloat(away3pExpPct);
            const away3pClass = away3pDiff > 2 ? 'hot' : (away3pDiff < -2 ? 'cold' : '');

            const home3pPct = game.home_3pa > 0 ? (game.home_3pm / game.home_3pa * 100).toFixed(1) : '0.0';
            const home3pExpPct = game.home_3pa > 0 ? (game.home_3pm_exp / game.home_3pa * 100).toFixed(1) : '0.0';
            const home3pDiff = parseFloat(home3pPct) - parseFloat(home3pExpPct);
            const home3pClass = home3pDiff > 2 ? 'hot' : (home3pDiff < -2 ? 'cold' : '');

            // Calculate luck: positive margin_delta means home benefited
            // We want to show the lucky team with a positive number
            const luckAmount = Math.abs(game.margin_delta);
            const luckyTeam = game.margin_delta > 0 ? game.home_team : game.away_team;

            // Format swing players (all with >=5 point impact)
            let swingPlayerHtml = '';
            let topPlayers = game.top_swing_players || [];
            // Fallback to single swing_player if no top_swing_players
            if (topPlayers.length === 0 && game.swing_player && Math.abs(game.swing_player_delta) >= 5) {{
                const playerTeam = game.swing_player_delta > 0 ?
                    (game.margin_delta > 0 ? game.home_team : game.away_team) :
                    (game.margin_delta > 0 ? game.away_team : game.home_team);
                topPlayers = [{{
                    name: game.swing_player,
                    team: playerTeam,
                    delta: game.swing_player_delta,
                    fg3m: game.swing_player_fg3m,
                    fg3a: game.swing_player_fg3a
                }}];
            }}
            if (topPlayers.length > 0) {{
                const playerLines = topPlayers.map(p => {{
                    const deltaSign = p.delta > 0 ? '+' : '';
                    const deltaClass = p.delta > 0 ? 'negative' : 'positive';
                    const shootingStats = p.fg3a !== undefined ? `(${{p.fg3m}}-${{p.fg3a}})` : '';
                    return `<div class="player-line"><span class="team-label">${{p.team}}:</span> ${{p.name}} ${{shootingStats}} <span class="${{deltaClass}}">${{deltaSign}}${{p.delta.toFixed(1)}}</span></div>`;
                }}).join('');
                swingPlayerHtml = `<div class="swing-players">${{playerLines}}</div>`;
            }}

            html += `
                <div class="game-box${{isFlipped ? ' flipped' : ''}}">
                    <div class="scores-section">
                        <div class="score-column actual">
                            <div class="column-header">Actual</div>
                            <div class="team-row">
                                <span class="team-abbrev">${{game.away_team}}</span>
                                <span class="team-score ${{awayActualWinner ? 'winner' : 'loser'}}">${{game.away_pts_actual}}</span>
                            </div>
                            <div class="team-row">
                                <span class="team-abbrev">${{game.home_team}}</span>
                                <span class="team-score ${{homeActualWinner ? 'winner' : 'loser'}}">${{game.home_pts_actual}}</span>
                            </div>
                        </div>
                        <div class="score-column adjusted">
                            <div class="column-header">Adjusted</div>
                            <div class="team-row">
                                <span class="team-abbrev">${{game.away_team}}</span>
                                <span class="team-score ${{awayAdjWinner ? 'winner' : 'loser'}}">${{game.away_pts_adj.toFixed(1)}}</span>
                            </div>
                            <div class="team-row">
                                <span class="team-abbrev">${{game.home_team}}</span>
                                <span class="team-score ${{homeAdjWinner ? 'winner' : 'loser'}}">${{game.home_pts_adj.toFixed(1)}}</span>
                            </div>
                        </div>
                    </div>
                    <div class="details-section">
                        <div class="three-pt-row">
                            <span class="three-pt-team">${{game.away_team}}</span>
                            <span class="three-pt-stats">${{game.away_3pm}}/${{game.away_3pa}} (${{away3pClass ? `<span class="${{away3pClass}}">${{away3pPct}}%</span>` : away3pPct + '%'}} vs ${{away3pExpPct}}% exp)</span>
                        </div>
                        <div class="three-pt-row">
                            <span class="three-pt-team">${{game.home_team}}</span>
                            <span class="three-pt-stats">${{game.home_3pm}}/${{game.home_3pa}} (${{home3pClass ? `<span class="${{home3pClass}}">${{home3pPct}}%</span>` : home3pPct + '%'}} vs ${{home3pExpPct}}% exp)</span>
                        </div>
                        <div class="swing-row">
                            <span><span class="luck-label">3PT Luck:</span> <span class="luck-team">${{luckyTeam}}</span> <span class="luck-value">+${{luckAmount.toFixed(1)}}</span></span>
                            ${{isFlipped ? '<span class="flip-indicator">⚠ FLIPPED</span>' : ''}}
                        </div>
                        ${{swingPlayerHtml}}
                    </div>
                </div>`;
        }});

        html += '</div>';
        html += '<p style="margin-top: 15px;"><small><span style="color: #16a34a; font-weight: 600;">Green</span> = winner | <span style="border: 2px solid #e94560; padding: 1px 4px; border-radius: 4px;">Red border</span> + ⚠ FLIPPED = luck changed the winner</small></p>';
        container.innerHTML = html;
    }}

    // Initialize
    renderCalendar();
    if (gamesByDate[mostRecentDate]) {{
        selectDate(mostRecentDate);
    }}

    // Sortable table functionality
    function setupSortableTable() {{
        const table = document.getElementById('team-table');
        if (!table) return;

        const headers = table.querySelectorAll('th.sortable');
        const tbody = table.querySelector('tbody') || table;

        headers.forEach(header => {{
            header.addEventListener('click', () => {{
                const sortKey = header.dataset.sort;
                const sortType = header.dataset.type;
                const isAsc = header.classList.contains('asc');

                // Remove sort classes from all headers
                headers.forEach(h => h.classList.remove('asc', 'desc'));

                // Toggle sort direction
                const newDir = isAsc ? 'desc' : 'asc';
                header.classList.add(newDir);

                // Get all data rows (skip header row)
                const rows = Array.from(table.querySelectorAll('tr[data-team]'));

                rows.sort((a, b) => {{
                    let aVal = a.dataset[sortKey] || '';
                    let bVal = b.dataset[sortKey] || '';

                    if (sortType === 'number') {{
                        aVal = parseFloat(aVal) || 0;
                        bVal = parseFloat(bVal) || 0;
                    }} else {{
                        aVal = aVal.toLowerCase();
                        bVal = bVal.toLowerCase();
                    }}

                    if (aVal < bVal) return newDir === 'asc' ? -1 : 1;
                    if (aVal > bVal) return newDir === 'asc' ? 1 : -1;
                    return 0;
                }});

                // Re-append rows in sorted order and update ranks
                rows.forEach((row, index) => {{
                    row.querySelector('td:first-child').textContent = index + 1;
                    table.appendChild(row);
                }});
            }});
        }});
    }}

    setupSortableTable();

    // Modal functions for showing flipped games
    function showFlippedGames(team) {{
        const overlay = document.getElementById('modal-overlay');
        const title = document.getElementById('modal-title');
        const body = document.getElementById('modal-body');

        // Find all games where this team was involved and luck flipped the winner
        const flippedGames = [];
        for (const [date, games] of Object.entries(gamesByDate)) {{
            for (const game of games) {{
                const isHome = game.home_team === team;
                const isAway = game.away_team === team;
                if (!isHome && !isAway) continue;

                const actualWinner = game.margin_actual > 0 ? game.home_team : game.away_team;
                const adjWinner = game.margin_adj > 0 ? game.home_team : game.away_team;
                if (actualWinner === adjWinner) continue;  // Not flipped

                // Determine if this was a lucky win or unlucky loss for the team
                const teamActuallyWon = actualWinner === team;
                const teamShouldHaveWon = adjWinner === team;

                flippedGames.push({{
                    date,
                    game,
                    luckyWin: teamActuallyWon && !teamShouldHaveWon,
                    unluckyLoss: !teamActuallyWon && teamShouldHaveWon
                }});
            }}
        }}

        // Sort by date descending
        flippedGames.sort((a, b) => b.date.localeCompare(a.date));

        const luckyWins = flippedGames.filter(g => g.luckyWin).length;
        const unluckyLosses = flippedGames.filter(g => g.unluckyLoss).length;

        title.textContent = `${{team}}: Games Where Luck Changed the Outcome (${{flippedGames.length}})`;

        if (flippedGames.length === 0) {{
            body.innerHTML = '<p>No games where luck flipped the winner for this team.</p>';
        }} else {{
            let html = `<p><span class="lucky-win">Lucky wins: ${{luckyWins}}</span> | <span class="unlucky-loss">Unlucky losses: ${{unluckyLosses}}</span></p>`;
            html += `<table>
                <tr>
                    <th>Date</th>
                    <th>Matchup</th>
                    <th>Actual</th>
                    <th>Adjusted</th>
                    <th>Result</th>
                </tr>`;

            for (const {{ date, game, luckyWin, unluckyLoss }} of flippedGames) {{
                const resultClass = luckyWin ? 'lucky-win' : 'unlucky-loss';
                const resultText = luckyWin ? 'Lucky W' : 'Unlucky L';
                const awayAdj = game.away_pts_adj.toFixed(1);
                const homeAdj = game.home_pts_adj.toFixed(1);

                html += `<tr>
                    <td>${{date}}</td>
                    <td>${{game.away_team}} @ ${{game.home_team}}</td>
                    <td>${{game.away_pts_actual}}-${{game.home_pts_actual}}</td>
                    <td>${{awayAdj}}-${{homeAdj}}</td>
                    <td class="${{resultClass}}">${{resultText}}</td>
                </tr>`;
            }}

            html += '</table>';
            body.innerHTML = html;
        }}

        overlay.classList.add('active');
    }}

    function closeModal(event) {{
        if (event && event.target !== event.currentTarget) return;
        document.getElementById('modal-overlay').classList.remove('active');
    }}

    // Close modal on Escape key
    document.addEventListener('keydown', (e) => {{
        if (e.key === 'Escape') closeModal();
    }});
    </script>
    <!-- Modal for flipped games -->
    <div class="modal-overlay" id="modal-overlay" onclick="closeModal(event)">
        <div class="modal" onclick="event.stopPropagation()">
            <div class="modal-header">
                <span id="modal-title">Games</span>
                <button class="modal-close" onclick="closeModal()">&times;</button>
            </div>
            <div class="modal-body" id="modal-body"></div>
        </div>
    </div>

    <script data-goatcounter="https://entropyisrude.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
</body>
</html>
"""

    output_path = Path("data/3pt_luck_report.html")
    output_path.write_text(html, encoding='utf-8')
    print(f"Report saved to: {output_path.absolute()}")

    # Also save to index.html for GitHub Pages
    index_path = Path("index.html")
    index_path.write_text(html, encoding='utf-8')
    print(f"Also saved to: {index_path.absolute()}")

    return output_path

if __name__ == "__main__":
    generate_report()
