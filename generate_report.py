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
         'margin_actual', 'margin_adj', 'margin_delta']
    ].copy()

    # Prepare games data as JSON for calendar
    # Check if swing_player columns exist (for backward compatibility)
    has_swing_player = 'swing_player' in df.columns

    json_cols = ['date', 'home_team', 'away_team', 'home_pts_actual', 'away_pts_actual',
                 'home_pts_adj', 'away_pts_adj', 'margin_actual', 'margin_adj', 'margin_delta']
    if has_swing_player:
        json_cols += ['swing_player', 'swing_player_delta']
        # Add shooting stats if available
        if 'swing_player_fg3m' in df.columns:
            json_cols += ['swing_player_fg3m', 'swing_player_fg3a']

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
            'margin_delta': round(row['margin_delta'], 1)
        }
        if has_swing_player:
            game_obj['swing_player'] = row['swing_player'] if pd.notna(row['swing_player']) else ''
            game_obj['swing_player_delta'] = round(row['swing_player_delta'], 1) if pd.notna(row['swing_player_delta']) else 0
            # Add shooting stats if available
            if 'swing_player_fg3m' in row and pd.notna(row.get('swing_player_fg3m')):
                game_obj['swing_player_fg3m'] = int(row['swing_player_fg3m'])
                game_obj['swing_player_fg3a'] = int(row['swing_player_fg3a'])
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
        <p><strong>What is this?</strong> This analysis adjusts NBA scores based on 3-point shooting luck.
        Each shot is evaluated based on the shooter's historical accuracy and the shot's difficulty (corner vs above-break, catch-and-shoot vs pullup/stepback).
        We calculate what the score "should have been" if each shot had its expected outcome.</p>
        <p><strong>Luck Swing</strong> = Luck-adjusted margin minus actual margin. Positive means the home team benefited from luck.</p>
    </div>

    <div class="nav-links">
        <a href="#team-rankings">Team Rankings</a>
        <a href="#biggest-swings">Biggest Swings</a>
        <a href="#methodology">Methodology</a>
    </div>

    <h2>Games by Date</h2>
    <div class="calendar-container">
        <div class="calendar-grid" id="calendar"></div>
    </div>

    <div class="selected-date-section">
        <div class="selected-date-title" id="selected-date-title">Select a date above</div>
        <div id="games-table-container">
            <p class="no-games">Click on a highlighted date to see games</p>
        </div>
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

    <h2 id="biggest-swings">Biggest Luck-Swing Games</h2>
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
        flip = "&#9888;" if winner != adj_winner else ""
        html += f"""        <tr>
            <td>{row['date']}</td>
            <td>{row['away_team']} @ {row['home_team']}</td>
            <td>{int(row['away_pts_actual'])}-{int(row['home_pts_actual'])}</td>
            <td>{row['margin_adj']:+.1f}</td>
            <td class="{swing_class}">{row['margin_delta']:+.1f} {flip}</td>
        </tr>
"""

    html += f"""    </table>
    <p><small>&#9888; = Adjusted margin flips the winner</small></p>

    <div class="methodology" id="methodology">
        <h3>Methodology</h3>
        <p>This analysis calculates what NBA scores "should have been" by adjusting for 3-point shooting luck on a <strong>shot-by-shot basis</strong>.</p>

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

        let html = `<table>
            <tr>
                <th>Matchup</th>
                <th>Actual Score</th>
                <th>Adjusted Score</th>
                <th>Luck Swing</th>
                <th>Biggest Swing Player</th>
            </tr>`;

        sortedGames.forEach(game => {{
            const swingClass = game.margin_delta > 0 ? 'positive' : 'negative';
            const actualWinner = game.margin_actual > 0 ? game.home_team : game.away_team;
            const adjWinner = game.margin_adj > 0 ? game.home_team : game.away_team;
            const isFlipped = actualWinner !== adjWinner;
            const flip = isFlipped ? '<span class="winner-flip">&#9888;</span>' : '';
            const rowClass = isFlipped ? 'winner-flipped' : '';

            // Format adjusted score with winner in bold
            const awayAdj = game.away_pts_adj.toFixed(1);
            const homeAdj = game.home_pts_adj.toFixed(1);
            const adjScore = game.margin_adj > 0
                ? `${{awayAdj}}-<strong>${{homeAdj}}</strong>`
                : `<strong>${{awayAdj}}</strong>-${{homeAdj}}`;

            // Format swing player with shooting stats and delta
            let swingPlayer = '';
            if (game.swing_player) {{
                const playerDeltaClass = game.swing_player_delta > 0 ? 'positive' : 'negative';
                const deltaSign = game.swing_player_delta > 0 ? '+' : '';
                // Add shooting stats if available (e.g., "1-8")
                let shootingStats = '';
                if (game.swing_player_fg3a !== undefined) {{
                    shootingStats = ` (${{game.swing_player_fg3m}}-${{game.swing_player_fg3a}})`;
                }}
                swingPlayer = `${{game.swing_player}}${{shootingStats}} <span class="${{playerDeltaClass}}">${{deltaSign}}${{game.swing_player_delta.toFixed(1)}}</span>`;
            }}

            html += `<tr class="${{rowClass}}">
                <td>${{game.away_team}} @ ${{game.home_team}}</td>
                <td>${{game.away_pts_actual}}-${{game.home_pts_actual}}</td>
                <td>${{adjScore}} ${{flip}}</td>
                <td class="${{swingClass}}">${{game.margin_delta > 0 ? '+' : ''}}${{game.margin_delta.toFixed(1)}}</td>
                <td>${{swingPlayer}}</td>
            </tr>`;
        }});

        html += '</table><p><small><strong>Bold</strong> = adjusted winner | <span style="background:#ffe0e0;padding:2px 6px;">Pink row</span> + &#9888; = luck flipped the winner</small></p>';
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
    return output_path

if __name__ == "__main__":
    generate_report()
