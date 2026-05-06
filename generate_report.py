"""Generate HTML summary report from adjusted_games.csv

This script reads the template.html file and injects dynamic data.
To change the UI/styling, edit template.html directly.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

TEMPLATE_PATH = Path("template.html")


def _build_team_rows(source_df: pd.DataFrame) -> str:
    """Build HTML rows for the team rankings table from a given slice of games."""
    if source_df.empty:
        return '<tr><td colspan="12" style="text-align:center;color:#888;">No games yet</td></tr>'

    df = source_df.copy()
    df['game_poss_est'] = (df['home_pts_actual'] + df['away_pts_actual']) / 2

    home_luck = df.groupby('home_team').agg({
        'margin_delta': 'sum',
        'home_pts_adj': 'sum',
        'away_pts_adj': 'sum',
        'home_pts_actual': 'sum',
        'game_poss_est': 'sum',
        'game_id': 'count'
    }).rename(columns={
        'game_id': 'home_games', 'margin_delta': 'home_luck',
        'home_pts_adj': 'home_pts_adj_for', 'away_pts_adj': 'home_pts_adj_against',
        'game_poss_est': 'home_poss_est',
    })

    away_luck = df.groupby('away_team').agg({
        'margin_delta': lambda x: -x.sum(),
        'away_pts_adj': 'sum',
        'home_pts_adj': 'sum',
        'away_pts_actual': 'sum',
        'game_poss_est': 'sum',
        'game_id': 'count'
    }).rename(columns={
        'game_id': 'away_games', 'margin_delta': 'away_luck',
        'away_pts_adj': 'away_pts_adj_for', 'home_pts_adj': 'away_pts_adj_against',
        'game_poss_est': 'away_poss_est',
    })

    teams = home_luck.join(away_luck, how='outer').fillna(0)
    teams['total_luck'] = teams['home_luck'] + teams['away_luck']
    teams['total_games'] = teams['home_games'] + teams['away_games']
    teams['luck_per_game'] = teams['total_luck'] / teams['total_games']

    team_records = {}
    for team in set(source_df['home_team'].unique()) | set(source_df['away_team'].unique()):
        team_records[team] = {
            'wins': 0, 'losses': 0, 'adj_wins': 0, 'adj_losses': 0,
            'opp_3pa': 0, 'opp_3pm': 0, 'opp_3pm_exp': 0,
            'team_3pa': 0, 'team_3pm': 0, 'team_3pm_exp': 0,
        }

    for _, row in source_df.iterrows():
        home = row['home_team']
        away = row['away_team']
        if row['margin_actual'] > 0:
            team_records[home]['wins'] += 1
            team_records[away]['losses'] += 1
        else:
            team_records[away]['wins'] += 1
            team_records[home]['losses'] += 1
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
        team_records[home]['team_3pa'] += row['home_3pa']
        team_records[home]['team_3pm'] += row['home_3pm_actual']
        team_records[home]['team_3pm_exp'] += row['home_3pm_exp']
        team_records[away]['team_3pa'] += row['away_3pa']
        team_records[away]['team_3pm'] += row['away_3pm_actual']
        team_records[away]['team_3pm_exp'] += row['away_3pm_exp']

    teams = teams.reset_index()
    teams.columns = ['team'] + list(teams.columns[1:])
    for col in ('wins', 'losses', 'adj_wins', 'adj_losses',
                'opp_3pa', 'opp_3pm', 'opp_3pm_exp',
                'team_3pa', 'team_3pm', 'team_3pm_exp'):
        teams[col] = teams['team'].map(lambda t, c=col: team_records.get(t, {}).get(c, 0))
    teams['opp_3p_pct'] = (teams['opp_3pm'] / teams['opp_3pa'] * 100).round(1)
    teams['opp_3p_exp_pct'] = (teams['opp_3pm_exp'] / teams['opp_3pa'] * 100).round(1)
    teams['team_3p_pct'] = (teams['team_3pm'] / teams['team_3pa'] * 100).round(1)
    teams['team_3p_exp_pct'] = (teams['team_3pm_exp'] / teams['team_3pa'] * 100).round(1)

    total_poss = teams['home_poss_est'].fillna(0) + teams['away_poss_est'].fillna(0)
    total_adj_for = teams['home_pts_adj_for'].fillna(0) + teams['away_pts_adj_for'].fillna(0)
    total_adj_against = teams['home_pts_adj_against'].fillna(0) + teams['away_pts_adj_against'].fillna(0)
    teams['adj_ortg'] = (total_adj_for / total_poss * 100).round(1)
    teams['adj_drtg'] = (total_adj_against / total_poss * 100).round(1)
    teams['adj_nrtg'] = (teams['adj_ortg'] - teams['adj_drtg']).round(1)

    teams = teams.sort_values('wins', ascending=False)

    rows = ""
    for rank, (_, row) in enumerate(teams.iterrows(), 1):
        record = f"{int(row['wins'])}-{int(row['losses'])}"
        adj_record = f"{int(row['adj_wins'])}-{int(row['adj_losses'])}"
        win_diff = int(row['wins']) - int(row['adj_wins'])
        diff_class = "positive" if win_diff > 0 else ("negative" if win_diff < 0 else "")
        diff_str = f"{win_diff:+d}" if win_diff != 0 else "0"
        opp_diff = row['opp_3p_pct'] - row['opp_3p_exp_pct']
        opp_diff_class = "positive" if opp_diff < 0 else ("negative" if opp_diff > 0 else "")
        opp_diff_str = f"{opp_diff:+.1f}%"
        team_diff = row['team_3p_pct'] - row['team_3p_exp_pct']
        team_diff_class = "positive" if team_diff > 0 else ("negative" if team_diff < 0 else "")
        team_diff_str = f"{team_diff:+.1f}%"
        # net: positive means overall lucky (team shot above expected AND/OR opp shot below expected)
        net_diff = team_diff - opp_diff
        net_diff_class = "positive" if net_diff > 0 else ("negative" if net_diff < 0 else "")
        net_diff_str = f"{net_diff:+.1f}%"
        adj_ortg = row['adj_ortg']
        adj_drtg = row['adj_drtg']
        adj_nrtg = row['adj_nrtg']
        nrtg_class = "negative" if adj_nrtg > 0 else ("positive" if adj_nrtg < 0 else "")
        rows += f"""        <tr data-team="{row['team']}" data-wins="{int(row['wins'])}" data-adjwins="{int(row['adj_wins'])}" data-diff="{win_diff}" data-oppexp="{row['opp_3p_exp_pct']:.1f}" data-oppact="{row['opp_3p_pct']:.1f}" data-oppdiff="{opp_diff:.1f}" data-teamexp="{row['team_3p_exp_pct']:.1f}" data-teamact="{row['team_3p_pct']:.1f}" data-teamdiff="{team_diff:.1f}" data-netdiff="{net_diff:.1f}" data-adjortg="{adj_ortg:.1f}" data-adjdrtg="{adj_drtg:.1f}" data-adjnrtg="{adj_nrtg:.1f}">
            <td>{rank}</td>
            <td><strong>{row['team']}</strong></td>
            <td>{record}</td>
            <td>{adj_record}</td>
            <td class="{diff_class} clickable-diff" onclick="showFlippedGames('{row['team']}')">{diff_str}</td>
            <td>{row['opp_3p_exp_pct']:.1f}%</td>
            <td>{row['opp_3p_pct']:.1f}%</td>
            <td class="{opp_diff_class}">{opp_diff_str}</td>
            <td>{row['team_3p_exp_pct']:.1f}%</td>
            <td>{row['team_3p_pct']:.1f}%</td>
            <td class="{team_diff_class}">{team_diff_str}</td>
            <td class="{net_diff_class}">{net_diff_str}</td>
            <td>{adj_ortg:.1f}</td>
            <td>{adj_drtg:.1f}</td>
            <td class="{nrtg_class}">{adj_nrtg:+.1f}</td>
        </tr>
"""
    return rows


def _build_swing_rows(source_df: pd.DataFrame) -> str:
    if source_df.empty:
        return '        <tr><td colspan="5" style="text-align:center;color:#888;">No games yet</td></tr>\n'
    df = source_df.copy()
    df['abs_margin_delta'] = df['margin_delta'].abs()
    biggest = df.nlargest(15, 'abs_margin_delta')
    rows = ""
    for _, row in biggest.iterrows():
        winner = row['home_team'] if row['margin_actual'] > 0 else row['away_team']
        adj_winner = row['home_team'] if row['margin_adj'] > 0 else row['away_team']
        flip = "&#9888;" if winner != adj_winner else ""
        away_adj = f"{row['away_pts_adj']:.1f}"
        home_adj = f"{row['home_pts_adj']:.1f}"
        adj_score = (f"{away_adj}-<strong>{home_adj}</strong>" if row['margin_adj'] > 0
                     else f"<strong>{away_adj}</strong>-{home_adj}")
        lucky_team = row['home_team'] if row['margin_delta'] < 0 else row['away_team']
        luck_amount = abs(row['margin_delta'])
        rows += f"""        <tr>
            <td>{row['date']}</td>
            <td>{row['away_team']} @ {row['home_team']}</td>
            <td>{int(row['away_pts_actual'])}-{int(row['home_pts_actual'])}</td>
            <td>{adj_score} {flip}</td>
            <td class="positive">{lucky_team}: +{luck_amount:.1f}</td>
        </tr>
"""
    return rows


def generate_report():
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Template file not found: {TEMPLATE_PATH}")
    template = TEMPLATE_PATH.read_text(encoding='utf-8')

    df = pd.read_csv("data/adjusted_games.csv")

    # Rows without game_type were written before the column existed — treat as regular season.
    if 'game_type' in df.columns:
        df['game_type'] = df['game_type'].fillna('regular')
    else:
        df['game_type'] = 'regular'

    rs_df = df[df['game_type'] == 'regular'].copy()
    playoff_df = df[df['game_type'] == 'playoff'].copy()
    # play-in (game_type == 'playin') is excluded from both ranking tables

    team_rows = _build_team_rows(rs_df)
    playoff_team_rows = _build_team_rows(playoff_df)

    swing_rows = _build_swing_rows(rs_df)
    playoff_swing_rows = _build_swing_rows(playoff_df)

    # Calendar game objects
    has_swing_player = 'swing_player' in df.columns
    has_top_swing_players = 'top_swing_players' in df.columns
    games_by_date = {}
    for _, row in df.sort_values('date').iterrows():
        date = row['date']
        if date not in games_by_date:
            games_by_date[date] = []
        game_obj = {
            'home_team': row['home_team'],
            'away_team': row['away_team'],
            'game_type': row['game_type'],
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
            if 'swing_player_fg3m' in row and pd.notna(row.get('swing_player_fg3m')):
                game_obj['swing_player_fg3m'] = int(row['swing_player_fg3m'])
                game_obj['swing_player_fg3a'] = int(row['swing_player_fg3a'])
        if has_top_swing_players and pd.notna(row.get('top_swing_players')):
            try:
                game_obj['top_swing_players'] = json.loads(row['top_swing_players'])
            except (json.JSONDecodeError, TypeError):
                game_obj['top_swing_players'] = []
        games_by_date[date].append(game_obj)

    games_json = json.dumps(games_by_date)
    most_recent_date = df['date'].max()

    min_date = pd.to_datetime(df['date'].min())
    max_date = pd.to_datetime(df['date'].max())
    season_months = []
    current = min_date.replace(day=1)
    while current <= max_date:
        season_months.append({'year': current.year, 'month': current.month - 1})
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    season_months_json = json.dumps(season_months)

    html = template
    html = html.replace('{{SEASON_DATE}}', df['date'].max())
    html = html.replace('{{GAME_COUNT}}', str(len(rs_df)))
    html = html.replace('{{GAMES_JSON}}', games_json)
    html = html.replace('{{MOST_RECENT_DATE}}', most_recent_date)
    html = html.replace('{{SEASON_MONTHS_JSON}}', season_months_json)
    html = html.replace('{{TEAM_RANKINGS_ROWS}}', team_rows)
    html = html.replace('{{PLAYOFF_RANKINGS_ROWS}}', playoff_team_rows)
    html = html.replace('{{BIGGEST_SWINGS_ROWS}}', swing_rows)
    html = html.replace('{{PLAYOFF_BIGGEST_SWINGS_ROWS}}', playoff_swing_rows)
    html = html.replace('{{GENERATED_TIMESTAMP}}', datetime.now().strftime('%Y-%m-%d %H:%M'))

    output_path = Path("data/3pt_luck_report.html")
    output_path.write_text(html, encoding='utf-8')
    print(f"Report saved to: {output_path.absolute()}")

    index_path = Path("index.html")
    index_path.write_text(html, encoding='utf-8')
    print(f"Also saved to: {index_path.absolute()}")

    return output_path


if __name__ == "__main__":
    generate_report()
