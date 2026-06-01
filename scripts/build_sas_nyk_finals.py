"""Build data/sas_nyk_finals.json for the SAS-NYK Finals Explorer page.

Extracts:
  - Stint-level data (lineup + pts per stint) for the 2 RS games
  - Per-game box scores for all players
  - Game metadata
"""

import duckdb, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "nba_analytics.duckdb"
OUT  = ROOT / "data" / "sas_nyk_finals.json"

GAME_IDS = ['22500467', '22500868']
SAS_ID   = 1610612759
NYK_ID   = 1610612752

# Cup Final box scores are manually sourced (no PBP/stints data exists for this game)
CUP_GAME = {"id": "cup_2025_final", "date": "2025-12-16", "home": "NEU", "sas_pts": 113, "nyk_pts": 124, "cup": True, "label": "NBA Cup Final", "total_sec": 0, "total_poss": 0}
CUP_BOX  = {"1628368": {"team": "SAS", "min": 37.0, "pts": 17, "reb": 4, "oreb": 1, "dreb": 3, "ast": 8, "stl": 2, "blk": 0, "tov": 3, "pf": 3, "fgm": 6, "fga": 16, "fg3m": 2, "fg3a": 7, "ftm": 3, "fta": 5, "pm": -11}, "1642264": {"team": "SAS", "min": 37.0, "pts": 18, "reb": 5, "oreb": 0, "dreb": 5, "ast": 12, "stl": 3, "blk": 0, "tov": 2, "pf": 2, "fgm": 7, "fga": 15, "fg3m": 1, "fg3a": 4, "ftm": 3, "fta": 4, "pm": -11}, "1641705": {"team": "SAS", "min": 25.0, "pts": 21, "reb": 8, "oreb": 2, "dreb": 6, "ast": 3, "stl": 1, "blk": 4, "tov": 2, "pf": 3, "fgm": 8, "fga": 18, "fg3m": 3, "fg3a": 9, "ftm": 2, "fta": 3, "pm": -18}, "1630170": {"team": "SAS", "min": 28.0, "pts": 9, "reb": 3, "oreb": 0, "dreb": 3, "ast": 2, "stl": 1, "blk": 0, "tov": 1, "pf": 2, "fgm": 3, "fga": 9, "fg3m": 1, "fg3a": 4, "ftm": 2, "fta": 2, "pm": -11}, "203084": {"team": "SAS", "min": 26.0, "pts": 9, "reb": 4, "oreb": 1, "dreb": 3, "ast": 2, "stl": 0, "blk": 0, "tov": 1, "pf": 2, "fgm": 3, "fga": 8, "fg3m": 2, "fg3a": 5, "ftm": 1, "fta": 1, "pm": -11}, "1642844": {"team": "SAS", "min": 21.0, "pts": 19, "reb": 2, "oreb": 1, "dreb": 1, "ast": 1, "stl": 0, "blk": 0, "tov": 1, "pf": 1, "fgm": 6, "fga": 12, "fg3m": 4, "fg3a": 9, "ftm": 3, "fta": 3, "pm": -11}, "1628436": {"team": "SAS", "min": 20.0, "pts": 10, "reb": 6, "oreb": 2, "dreb": 4, "ast": 1, "stl": 0, "blk": 1, "tov": 1, "pf": 2, "fgm": 5, "fga": 8, "fg3m": 0, "fg3a": 0, "ftm": 0, "fta": 0, "pm": -11}, "1630577": {"team": "SAS", "min": 14.0, "pts": 5, "reb": 2, "oreb": 1, "dreb": 1, "ast": 1, "stl": 1, "blk": 0, "tov": 0, "pf": 1, "fgm": 2, "fga": 5, "fg3m": 1, "fg3a": 3, "ftm": 0, "fta": 0, "pm": -11}, "1629640": {"team": "SAS", "min": 10.0, "pts": 3, "reb": 1, "oreb": 0, "dreb": 1, "ast": 0, "stl": 0, "blk": 0, "tov": 0, "pf": 1, "fgm": 1, "fga": 3, "fg3m": 1, "fg3a": 3, "ftm": 0, "fta": 0, "pm": -11}, "1629162": {"team": "SAS", "min": 9.0, "pts": 2, "reb": 1, "oreb": 0, "dreb": 1, "ast": 1, "stl": 0, "blk": 0, "tov": 0, "pf": 0, "fgm": 1, "fga": 3, "fg3m": 0, "fg3a": 1, "ftm": 0, "fta": 0, "pm": -11}, "1628384": {"team": "NYK", "min": 37.0, "pts": 28, "reb": 7, "oreb": 1, "dreb": 6, "ast": 3, "stl": 3, "blk": 1, "tov": 1, "pf": 2, "fgm": 10, "fga": 21, "fg3m": 4, "fg3a": 10, "ftm": 4, "fta": 5, "pm": 11}, "1628973": {"team": "NYK", "min": 37.0, "pts": 20, "reb": 4, "oreb": 0, "dreb": 4, "ast": 8, "stl": 1, "blk": 0, "tov": 3, "pf": 3, "fgm": 8, "fga": 18, "fg3m": 1, "fg3a": 4, "ftm": 3, "fta": 4, "pm": 11}, "1626157": {"team": "NYK", "min": 32.0, "pts": 19, "reb": 9, "oreb": 2, "dreb": 7, "ast": 2, "stl": 1, "blk": 1, "tov": 2, "pf": 3, "fgm": 7, "fga": 14, "fg3m": 3, "fg3a": 7, "ftm": 2, "fta": 2, "pm": 11}, "1628969": {"team": "NYK", "min": 34.0, "pts": 19, "reb": 5, "oreb": 1, "dreb": 4, "ast": 3, "stl": 2, "blk": 0, "tov": 1, "pf": 2, "fgm": 7, "fga": 16, "fg3m": 2, "fg3a": 7, "ftm": 3, "fta": 3, "pm": 11}, "1628404": {"team": "NYK", "min": 31.0, "pts": 8, "reb": 7, "oreb": 2, "dreb": 5, "ast": 2, "stl": 1, "blk": 0, "tov": 1, "pf": 2, "fgm": 4, "fga": 8, "fg3m": 0, "fg3a": 2, "ftm": 0, "fta": 0, "pm": 11}, "1629011": {"team": "NYK", "min": 22.0, "pts": 2, "reb": 15, "oreb": 7, "dreb": 8, "ast": 0, "stl": 1, "blk": 1, "tov": 2, "pf": 3, "fgm": 1, "fga": 3, "fg3m": 0, "fg3a": 0, "ftm": 0, "fta": 0, "pm": 11}, "203903": {"team": "NYK", "min": 24.0, "pts": 13, "reb": 2, "oreb": 0, "dreb": 2, "ast": 2, "stl": 0, "blk": 0, "tov": 1, "pf": 1, "fgm": 5, "fga": 12, "fg3m": 3, "fg3a": 9, "ftm": 0, "fta": 0, "pm": 11}, "1642278": {"team": "NYK", "min": 17.0, "pts": 12, "reb": 2, "oreb": 0, "dreb": 2, "ast": 4, "stl": 0, "blk": 0, "tov": 1, "pf": 0, "fgm": 4, "fga": 9, "fg3m": 2, "fg3a": 5, "ftm": 2, "fta": 2, "pm": 11}, "1627824": {"team": "NYK", "min": 10.0, "pts": 3, "reb": 2, "oreb": 0, "dreb": 2, "ast": 1, "stl": 0, "blk": 0, "tov": 0, "pf": 1, "fgm": 1, "fga": 3, "fg3m": 1, "fg3a": 3, "ftm": 0, "fta": 0, "pm": 11}, "1630540": {"team": "NYK", "min": 6.0, "pts": 0, "reb": 1, "oreb": 0, "dreb": 1, "ast": 1, "stl": 1, "blk": 0, "tov": 0, "pf": 1, "fgm": 0, "fga": 2, "fg3m": 0, "fg3a": 1, "ftm": 0, "fta": 0, "pm": 11}}

def main():
    con = duckdb.connect(str(DB), read_only=True)

    # ── Player names ────────────────────────────────────────────────────────
    pid_rows = con.execute("""
        SELECT DISTINCT player_id, player_name, team_abbr
        FROM player_game_facts
        WHERE game_id IN ('22500467','22500868')
        ORDER BY team_abbr, player_name
    """).fetchall()
    name_map = {str(r[0]): r[1] for r in pid_rows}
    team_map = {str(r[0]): r[2] for r in pid_rows}

    # ── Per-game box scores ─────────────────────────────────────────────────
    box_rows = con.execute("""
        SELECT game_id, player_id, team_abbr,
               minutes, pts, reb, oreb, dreb, ast, stl, blk, tov, pf,
               fgm, fga, fg3m, fg3a, ftm, fta, plus_minus_actual
        FROM player_game_facts
        WHERE game_id IN ('22500467','22500868')
        ORDER BY game_id, team_abbr, minutes DESC
    """).fetchall()

    box = {}
    for r in box_rows:
        gid = r[0]
        if gid not in box: box[gid] = {}
        pid = str(r[1])
        box[gid][pid] = {
            'team': r[2], 'min': round(r[3], 2),
            'pts': r[4],  'reb': r[5],  'oreb': r[6], 'dreb': r[7],
            'ast': r[8],  'stl': r[9],  'blk': r[10], 'tov': r[11],
            'pf': r[12],  'fgm': r[13], 'fga': r[14], 'fg3m': r[15],
            'fg3a': r[16],'ftm': r[17], 'fta': r[18], 'pm': r[19],
        }

    # ── Stint data (accurate clock-based lineup tracking) ──────────────────
    stint_rows = con.execute("""
        SELECT game_id, home_id,
               home_p1, home_p2, home_p3, home_p4, home_p5,
               away_p1, away_p2, away_p3, away_p4, away_p5,
               home_pts, away_pts, seconds
        FROM raw_hist_stints
        WHERE game_id IN ('22500467','22500868')
        ORDER BY game_id, stint_index
    """).fetchall()

    stints = {}
    for r in stint_rows:
        gid = str(r[0])
        if gid not in stints: stints[gid] = []
        home_id = r[1]
        home_p = [str(p) for p in r[2:7] if p]
        away_p = [str(p) for p in r[7:12] if p]
        home_pts = int(r[12] or 0)
        away_pts = int(r[13] or 0)
        sec = round(float(r[14] or 0), 1)
        if home_id == SAS_ID:
            sas_p, nyk_p = home_p, away_p
            sas_pts, nyk_pts = home_pts, away_pts
        else:
            nyk_p, sas_p = home_p, away_p
            nyk_pts, sas_pts = home_pts, away_pts
        stints[gid].append({'sas': sas_p, 'nyk': nyk_p, 'sp': sas_pts, 'np': nyk_pts, 'sec': sec})

    # ── Game metadata ───────────────────────────────────────────────────────
    meta_rows = con.execute("""
        SELECT DISTINCT game_id, date, home_away, win_loss
        FROM player_game_facts
        WHERE game_id IN ('22500467','22500868') AND team_abbr = 'SAS'
        ORDER BY date
    """).fetchall()

    # Total possessions per game (accurate count; used for per-100 normalization)
    poss_count = {str(r[0]): r[1] for r in con.execute("""
        SELECT game_id, COUNT(*) FROM raw_hist_possessions
        WHERE game_id IN ('22500467','22500868') GROUP BY game_id
    """).fetchall()}

    # Actual game scores from possession totals (authoritative; player box pts have CDN artifacts)
    poss_totals = {}
    for r in con.execute("""
        SELECT game_id, offense_team, SUM(points)
        FROM raw_hist_possessions
        WHERE game_id IN ('22500467','22500868')
        GROUP BY game_id, offense_team
    """).fetchall():
        gid = str(r[0])
        if gid not in poss_totals: poss_totals[gid] = {}
        poss_totals[gid][r[1]] = int(r[2])

    games = []
    for r in meta_rows:
        gid = r[0]
        gs = stints.get(gid, [])
        total_sec = round(sum(s['sec'] for s in gs), 1)
        home = 'SAS' if r[2] == 'home' else 'NYK'
        totals = poss_totals.get(gid, {})
        games.append({
            'id': gid, 'date': str(r[1]),
            'home': home,
            'sas_pts': totals.get(SAS_ID, 0),
            'nyk_pts': totals.get(NYK_ID, 0),
            'total_sec': total_sec,
            'total_poss': poss_count.get(gid, 0),
        })

    # ── Player rosters (appearing in stint data) ───────────────────────────
    sas_pids, nyk_pids = set(), set()
    for gs in stints.values():
        for s in gs:
            for pid in s['sas']: sas_pids.add(pid)
            for pid in s['nyk']: nyk_pids.add(pid)

    sas_players = sorted(
        [{'pid': p, 'name': name_map.get(p, p)} for p in sas_pids if p in name_map],
        key=lambda x: x['name'])
    nyk_players = sorted(
        [{'pid': p, 'name': name_map.get(p, p)} for p in nyk_pids if p in name_map],
        key=lambda x: x['name'])

    con.close()

    # RS games first (sorted by date), Cup Final appended last (tab order matches old JSON)
    all_games = sorted(games, key=lambda g: g['date']) + [CUP_GAME]
    box['cup_2025_final'] = CUP_BOX

    output = {
        'games': all_games,
        'sas_players': sas_players,
        'nyk_players': nyk_players,
        'box': box,
        'stints': stints,
    }

    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(output, f, separators=(',', ':'))

    size_kb = OUT.stat().st_size // 1024
    print(f"Wrote {OUT} ({size_kb} KB)")
    print(f"  {len(games)} games, {len(sas_players)} SAS players, {len(nyk_players)} NYK players")
    total_stints = sum(len(v) for v in stints.values())
    print(f"  {total_stints} stints total")
    for g in games:
        print(f"  {g['date']}: SAS {g['sas_pts']} NYK {g['nyk_pts']}, {g['total_poss']} poss, {g['total_sec']}s")

if __name__ == '__main__':
    main()
