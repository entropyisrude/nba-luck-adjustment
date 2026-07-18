"""
NBA Finals Backtest 1996-97 through 2024-25.
Tests three components: RS luck-adj net, PO luck+opp-adj net, H2H luck-adj.
"""
import json, re, os
from collections import defaultdict

DATA = os.path.join(os.path.dirname(__file__), '..', 'data')

# ── Finals ground truth (winner, loser) ──────────────────────────────────────
FINALS = {
    '1996-97': ('CHI', 'UTA'), '1997-98': ('CHI', 'UTA'),
    '1998-99': ('SAS', 'NYK'), '1999-00': ('LAL', 'IND'),
    '2000-01': ('LAL', 'PHI'), '2001-02': ('LAL', 'NJN'),
    '2002-03': ('SAS', 'NJN'), '2003-04': ('DET', 'LAL'),
    '2004-05': ('SAS', 'DET'), '2005-06': ('MIA', 'DAL'),
    '2006-07': ('SAS', 'CLE'), '2007-08': ('BOS', 'LAL'),
    '2008-09': ('LAL', 'ORL'), '2009-10': ('LAL', 'BOS'),
    '2010-11': ('DAL', 'MIA'), '2011-12': ('MIA', 'OKC'),
    '2012-13': ('MIA', 'SAS'), '2013-14': ('SAS', 'MIA'),
    '2014-15': ('GSW', 'CLE'), '2015-16': ('CLE', 'GSW'),
    '2016-17': ('GSW', 'CLE'), '2017-18': ('GSW', 'CLE'),
    '2018-19': ('TOR', 'GSW'), '2019-20': ('LAL', 'MIA'),
    '2020-21': ('MIL', 'PHX'), '2021-22': ('GSW', 'BOS'),
    '2022-23': ('DEN', 'MIA'), '2023-24': ('BOS', 'DAL'),
    '2024-25': ('OKC', 'IND'),
}

# Season label → file slug (e.g. '1996-97' → '1996_97')
def slug(season): return season.replace('-', '_')

def prev_slug(sl):
    """Playoff files are labeled one year behind the data they contain, so load prev year."""
    start = int(sl[:4]) - 1
    end = int(sl[5:]) - 1
    if end < 0:
        end = 99
    return f'{start}_{end:02d}'

# ── Load chunk file ───────────────────────────────────────────────────────────
def load_chunk(path):
    with open(path, encoding='utf-8') as f:
        return json.loads(re.search(r'= (\[.+\]);', f.read(), re.DOTALL).group(1))

# ── Build per-game per-team box stats from a chunk ───────────────────────────
# Returns dict: {team: {game_id: {pts, opp_pts, poss, fg3m, fg3a, opp_team}}}
def game_team_stats(rows):
    # First pass: sum player rows per (game_id, team)
    box = defaultdict(lambda: defaultdict(lambda: {
        'pts': 0, 'opp_pts': 0, 'poss': 0.0,
        'fg3m': 0, 'fg3a': 0, 'opp_team': ''
    }))
    pts_set = defaultdict(dict)   # (gid, team) -> team_pts (from r[7])
    opp_set = defaultdict(dict)

    for r in rows:
        gid, team, opp = str(r[2]), r[5], r[6]
        key = (gid, team)
        b = box[team][gid]
        b['opp_team'] = opp
        # Official score from team_pts field (same for all players on team)
        b['pts']     = float(r[7] or 0)
        b['opp_pts'] = float(r[8] or 0)
        # Possession components (sum across players)
        b['poss']  += float(r[24] or 0) + 0.44*float(r[32] or 0) \
                    - float(r[16] or 0) + float(r[21] or 0)
        b['fg3m']  += int(r[28] or 0)
        b['fg3a']  += int(r[29] or 0)
    return box

# ── Compute team season stats ─────────────────────────────────────────────────
# Returns dict: {team: {pts_for, pts_ag, poss, fg3m, fg3a, opp_fg3m, opp_fg3a}}
def season_stats(box):
    stats = defaultdict(lambda: {
        'pts_for': 0.0, 'pts_ag': 0.0, 'poss': 0.0,
        'fg3m': 0, 'fg3a': 0, 'opp_fg3m': 0, 'opp_fg3a': 0,
        'games': 0
    })
    for team, games in box.items():
        s = stats[team]
        for gid, b in games.items():
            opp = b['opp_team']
            # use average of both teams' poss as per-team estimate
            opp_poss = box[opp][gid]['poss'] if opp in box and gid in box[opp] else b['poss']
            per_team_poss = (b['poss'] + opp_poss) / 2
            if per_team_poss < 50: continue  # sanity check
            s['pts_for'] += b['pts']
            s['pts_ag']  += b['opp_pts']
            s['poss']    += per_team_poss
            s['fg3m']    += b['fg3m']
            s['fg3a']    += b['fg3a']
            # defensive: opponent's 3pt in this game
            if opp in box and gid in box[opp]:
                s['opp_fg3m'] += box[opp][gid]['fg3m']
                s['opp_fg3a'] += box[opp][gid]['fg3a']
            s['games'] += 1
    return stats

# ── Net rating + luck adjustment ──────────────────────────────────────────────
def compute_net(s, lg3):
    if s['poss'] < 1: return None, None
    raw = 100 * (s['pts_for'] - s['pts_ag']) / s['poss']
    off_luck = (s['fg3m']/s['fg3a'] - lg3)*s['fg3a']*3/s['poss']*100 if s['fg3a'] else 0
    def_luck = (s['opp_fg3m']/s['opp_fg3a'] - lg3)*s['opp_fg3a']*3/s['poss']*100 if s['opp_fg3a'] else 0
    # off_luck > 0 means we scored extra points via above-avg 3pt% → subtract (remove the luck)
    # def_luck > 0 means opponents scored extra points via above-avg 3pt% → add back (it hurt our net)
    luck_adj = raw - off_luck + def_luck
    return raw, luck_adj

def league_avg_3pt(stats):
    tot_m = sum(s['fg3m'] for s in stats.values())
    tot_a = sum(s['fg3a'] for s in stats.values())
    return tot_m / tot_a if tot_a else 0.36

# ── Compute playoff ratings per team (opp-adj + luck-adj) ────────────────────
def playoff_ratings(po_box, rs_nets, lg3):
    """
    For each team, compute game-weighted luck-adj + opp-adj net across
    all series (excluding Finals opponent, who we're predicting against).
    Returns {team: luck_opp_adj_net}
    """
    # Build per-series stats
    series = defaultdict(lambda: defaultdict(lambda: {
        'pts_for': 0.0, 'pts_ag': 0.0, 'poss': 0.0,
        'fg3m': 0, 'fg3a': 0, 'opp_fg3m': 0, 'opp_fg3a': 0,
        'games': 0, 'opp': ''
    }))
    for team, games in po_box.items():
        for gid, b in games.items():
            opp = b['opp_team']
            series[team][opp]['opp'] = opp
            opp_b = po_box[opp][gid] if opp in po_box and gid in po_box[opp] else b
            per_team_poss = (b['poss'] + opp_b['poss']) / 2
            if per_team_poss < 50: continue
            s = series[team][opp]
            s['pts_for'] += b['pts']
            s['pts_ag']  += b['opp_pts']
            s['poss']    += per_team_poss
            s['fg3m']    += b['fg3m']
            s['fg3a']    += b['fg3a']
            if opp in po_box and gid in po_box[opp]:
                s['opp_fg3m'] += po_box[opp][gid]['fg3m']
                s['opp_fg3a'] += po_box[opp][gid]['fg3a']
            s['games'] += 1

    team_ratings = {}
    lg = league_avg_3pt({t: {'fg3m': series[t][o]['fg3m'],
                              'fg3a': series[t][o]['fg3a'],
                              'opp_fg3m': 0, 'opp_fg3a': 0}
                          for t in series for o in series[t]})

    for team in series:
        total_poss = 0.0; wtd_net = 0.0
        for opp, s in series[team].items():
            if s['poss'] < 50: continue
            raw, luck = compute_net(s, lg3)
            if raw is None: continue
            opp_rs = rs_nets.get(opp, 0)
            opp_adj = raw + opp_rs   # add opponent strength
            luck_opp = luck + opp_rs
            total_poss += s['poss']
            wtd_net += luck_opp * s['poss']
        if total_poss > 0:
            team_ratings[team] = wtd_net / total_poss
    return team_ratings

# ── H2H luck-adj net between two teams in RS ─────────────────────────────────
def h2h_luck_adj(box, team_a, team_b, lg3):
    """Returns luck-adj net from team_a's perspective vs team_b."""
    pts_a = pts_b = poss = 0.0
    fg3m_a = fg3a_a = fg3m_b = fg3a_b = 0
    for gid, b in box[team_a].items():
        if b['opp_team'] != team_b: continue
        opp_b = box[team_b].get(gid, {})
        per_poss = (b['poss'] + opp_b.get('poss', b['poss'])) / 2
        if per_poss < 50: continue
        pts_a += b['pts']; pts_b += b['opp_pts']; poss += per_poss
        fg3m_a += b['fg3m']; fg3a_a += b['fg3a']
        fg3m_b += opp_b.get('fg3m', 0); fg3a_b += opp_b.get('fg3a', 0)
    if poss < 50:
        return None, 0
    raw = 100 * (pts_a - pts_b) / poss
    off_luck = (fg3m_a/fg3a_a - lg3)*fg3a_a*3/poss*100 if fg3a_a else 0
    def_luck = (fg3m_b/fg3a_b - lg3)*fg3a_b*3/poss*100 if fg3a_b else 0
    luck_adj = raw - off_luck + def_luck
    return luck_adj, poss

# ── Main backtest ─────────────────────────────────────────────────────────────
def run_backtest():
    results = []

    for season, (winner, loser) in sorted(FINALS.items()):
        sl = slug(season)
        rs_path = os.path.join(DATA, 'player_game_chunks', f'{sl}.js')
        po_path = os.path.join(DATA, 'player_game_playoff_chunks', f'{prev_slug(sl)}.js')
        if not os.path.exists(rs_path) or not os.path.exists(po_path):
            continue

        rs_rows = load_chunk(rs_path)
        po_rows = load_chunk(po_path)

        rs_box = game_team_stats(rs_rows)
        po_box = game_team_stats(po_rows)

        rs_stats = season_stats(rs_box)
        lg3_rs   = league_avg_3pt(rs_stats)
        lg3_po   = league_avg_3pt(season_stats(po_box))

        # RS net ratings for all teams
        rs_nets = {}
        rs_luck = {}
        for team, s in rs_stats.items():
            raw, luck = compute_net(s, lg3_rs)
            if raw is not None:
                rs_nets[team] = raw
                rs_luck[team] = luck

        # Playoff ratings (league-avg opp net ≈ 0)
        po_ratings = playoff_ratings(po_box, rs_nets, lg3_po)

        # H2H in RS
        h2h_w, h2h_poss = h2h_luck_adj(rs_box, winner, loser, lg3_rs)

        # Pull numbers for both finalists
        w_rs  = rs_luck.get(winner, 0)
        l_rs  = rs_luck.get(loser,  0)
        w_po  = po_ratings.get(winner, 0)
        l_po  = po_ratings.get(loser,  0)

        results.append({
            'season': season, 'winner': winner, 'loser': loser,
            'w_rs': w_rs,   'l_rs': l_rs,
            'w_po': w_po,   'l_po': l_po,
            'h2h': h2h_w,   'h2h_poss': h2h_poss,
        })

    return results


def predict(r, a_rs, a_po, a_h2h):
    """Predict winner using weighted combo. Returns True if correct."""
    w_score = a_rs*r['w_rs'] + a_po*r['w_po'] + (a_h2h*r['h2h'] if r['h2h'] is not None else 0)
    l_score = a_rs*r['l_rs'] + a_po*r['l_po'] + (a_h2h*(-r['h2h']) if r['h2h'] is not None else 0)
    return w_score > l_score


if __name__ == '__main__':
    print('Building season data...')
    results = run_backtest()
    print(f'Processed {len(results)} Finals\n')

    # Print per-series table
    print(f'{"Season":<10} {"Winner":>5} {"Loser":>5}  {"W_RS":>7} {"L_RS":>7} {"RS_gap":>7}  {"W_PO":>7} {"L_PO":>7} {"PO_gap":>7}  {"H2H":>7}  {"RS?":>4}')
    print('-'*93)
    for r in results:
        h = f'{r["h2h"]:+.1f}' if r['h2h'] is not None else '  n/a'
        rs_gap = r['w_rs'] - r['l_rs']
        po_gap = r['w_po'] - r['l_po']
        rs_correct = 'Y' if rs_gap > 0 else 'N'
        print(f'{r["season"]:<10} {r["winner"]:>5} {r["loser"]:>5}  '
              f'{r["w_rs"]:>+7.2f} {r["l_rs"]:>+7.2f} {rs_gap:>+7.2f}  '
              f'{r["w_po"]:>+7.2f} {r["l_po"]:>+7.2f} {po_gap:>+7.2f}  '
              f'{h:>7}  {rs_correct:>4}')

    print()
    # Test different weight combos
    print('Prediction accuracy by weight combo (RS / PO / H2H):')
    print(f'  {"Weights":<25} {"Correct":>8} {"Total":>6} {"Pct":>6}')
    print('  ' + '-'*50)

    from itertools import product
    best = (0, None)
    combos = []
    for a, b, c in product([0, 0.5, 1.0, 1.5, 2.0], repeat=3):
        if a + b + c == 0: continue
        correct = sum(predict(r, a, b, c) for r in results)
        combos.append((a, b, c, correct))

    # Show top 10 unique accuracy levels + key combos
    combos.sort(key=lambda x: -x[3])
    seen_acc = set()
    shown = 0
    for a, b, c in [(1,0,0),(0,1,0),(0,0,1),(1,1,0),(1,0,1),(0,1,1),(1,1,1),
                    (2,1,0),(1,2,0),(1,1,2),(2,1,1),(1,2,1)]:
        correct = sum(predict(r, a, b, c) for r in results)
        n = len(results)
        print(f'  RS={a:.1f} PO={b:.1f} H2H={c:.1f}     {correct:>8d} {n:>6d} {100*correct/n:>5.1f}%')

    print()
    best_a, best_b, best_c, best_n = combos[0]
    print(f'  Best combo found: RS={best_a} PO={best_b} H2H={best_c} -> {best_n}/{len(results)} ({100*best_n/len(results):.1f}%)')

    # Show where RS-only got it wrong
    print('\nCases where RS-only was WRONG (loser had better RS luck-adj):')
    print(f'  {"Season":<10} {"Winner":>5} {"Loser":>5}  {"W_RS":>7} {"L_RS":>7}  {"W_PO":>7} {"L_PO":>7}  {"H2H":>7}')
    print('  ' + '-'*70)
    for r in results:
        if r['l_rs'] > r['w_rs']:
            h = f'{r["h2h"]:+.1f}' if r['h2h'] is not None else '  n/a'
            print(f'  {r["season"]:<10} {r["winner"]:>5} {r["loser"]:>5}  '
                  f'{r["w_rs"]:>+7.2f} {r["l_rs"]:>+7.2f}  '
                  f'{r["w_po"]:>+7.2f} {r["l_po"]:>+7.2f}  {h:>7}')
            po_right = r['w_po'] > r['l_po']
            h2h_right = r['h2h'] is not None and r['h2h'] > 0
            tags = []
            if po_right: tags.append('PO rescued')
            if h2h_right: tags.append('H2H rescued')
            if tags: print(f'    --> {", ".join(tags)}')

    # Show SAS vs NYK current projection under each weighting
    print('\n--- 2025-26 Finals: SAS vs NYK ---')
    print('(Enter your component values to get probability estimates)')

