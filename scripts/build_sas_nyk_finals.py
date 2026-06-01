"""Build data/sas_nyk_finals.json for the SAS-NYK Finals Explorer page.

Extracts:
  - Possession-level data (lineup + pts per possession) for the 2 games
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

    # ── Possession data ─────────────────────────────────────────────────────
    poss_rows = con.execute("""
        SELECT game_id, poss_index, offense_team,
               off_p1, off_p2, off_p3, off_p4, off_p5,
               def_p1, def_p2, def_p3, def_p4, def_p5,
               points
        FROM raw_hist_possessions
        WHERE game_id IN ('22500467','22500868')
        ORDER BY game_id, poss_index
    """).fetchall()

    poss = {}
    for r in poss_rows:
        gid = str(r[0])
        if gid not in poss: poss[gid] = []
        off_team = r[2]
        off_players = [str(p) for p in r[3:8] if p]
        def_players = [str(p) for p in r[8:13] if p]
        if off_team == SAS_ID:
            sas_p, nyk_p = off_players, def_players
            off = 'SAS'
        else:
            nyk_p, sas_p = off_players, def_players
            off = 'NYK'
        poss[gid].append({'off': off, 'pts': int(r[13] or 0), 'sas': sas_p, 'nyk': nyk_p})

    # ── Game metadata ───────────────────────────────────────────────────────
    meta_rows = con.execute("""
        SELECT DISTINCT game_id, date, home_away, win_loss
        FROM player_game_facts
        WHERE game_id IN ('22500467','22500868') AND team_abbr = 'SAS'
        ORDER BY date
    """).fetchall()

    games = []
    for r in meta_rows:
        gid = r[0]
        gp = poss.get(gid, [])
        sas_pts = sum(p['pts'] for p in gp if p['off'] == 'SAS')
        nyk_pts = sum(p['pts'] for p in gp if p['off'] == 'NYK')
        home = 'SAS' if r[2] == 'home' else 'NYK'
        games.append({
            'id': gid, 'date': str(r[1]),
            'home': home, 'sas_pts': sas_pts, 'nyk_pts': nyk_pts,
        })

    # ── Player rosters (appearing in possession data) ───────────────────────
    sas_pids, nyk_pids = set(), set()
    for gp in poss.values():
        for p in gp:
            for pid in p['sas']: sas_pids.add(pid)
            for pid in p['nyk']: nyk_pids.add(pid)

    sas_players = sorted(
        [{'pid': p, 'name': name_map.get(p, p)} for p in sas_pids if p in name_map],
        key=lambda x: x['name'])
    nyk_players = sorted(
        [{'pid': p, 'name': name_map.get(p, p)} for p in nyk_pids if p in name_map],
        key=lambda x: x['name'])

    con.close()

    output = {
        'games': games,
        'sas_players': sas_players,
        'nyk_players': nyk_players,
        'box': box,
        'poss': poss,
    }

    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(output, f, separators=(',', ':'))

    size_kb = OUT.stat().st_size // 1024
    print(f"Wrote {OUT} ({size_kb} KB)")
    print(f"  {len(games)} games, {len(sas_players)} SAS players, {len(nyk_players)} NYK players")
    total_poss = sum(len(v) for v in poss.values())
    print(f"  {total_poss} possessions total")

if __name__ == '__main__':
    main()
