import duckdb, numpy as np, pandas as pd

ALPHA, LAMBDA_DECAY, OREB = 0.3, 0.6, 11

con = duckdb.connect('data/nba_analytics.duckdb', read_only=True)

players = {
    101106: 'Andrew Bogut',
    201939: 'Stephen Curry',
    202691: 'Klay Thompson',
    203084: 'Harrison Barnes',
    203110: 'Draymond Green',
}

df = con.execute("""
    SELECT season, player_id,
        SUM(pts) AS pts, SUM(fgm) AS fgm, SUM(fga) AS fga,
        SUM(fta) AS fta, SUM(tov) AS tov,
        SUM(on_possessions) AS on_poss,
        SUM(unassisted_fgm) AS unast_fgm
    FROM player_game_facts
    WHERE game_id LIKE '2%' AND on_possessions > 0
      AND player_id IN (101106, 201939, 202691, 203084, 203110)
    GROUP BY season, player_id
    HAVING SUM(on_possessions) >= 300
    ORDER BY player_id, season
""").fetchdf()

df['yr'] = df['season'].str[:4].astype(int)
df['unast_prop'] = df['unast_fgm'] / df['fgm']
df['fga_p100']   = df['fga'] / df['on_poss'] * 100
df['creation']   = df['fga_p100'] * (df['unast_prop'] + ALPHA * (1 - df['unast_prop']))
shot_att = df['fga'] + 0.44 * df['fta']
df['ts']         = df['pts'] / (2 * shot_att)
df['tov_rate']   = df['tov'] / df['on_poss']

# target_yr=2014 => lineup plays 2014-15 season; priors are yr 2013, 2012, 2011
target_yr = 2014
baselines = {}

for pid, name in players.items():
    print(f'=== {name} ===')
    total_w, creation_wsum, ts_wsum, tov_wsum = 0, 0, 0, 0
    for lag in range(1, 4):
        yr = target_yr - lag
        row = df[(df['player_id'] == pid) & (df['yr'] == yr)]
        if row.empty:
            print(f'  lag {lag} (yr {yr}): no qualifying season')
            continue
        r = row.iloc[0]
        w = (LAMBDA_DECAY ** (lag - 1)) * r['on_poss']
        unast_prop = r['unast_prop']
        fga_p100   = r['fga_p100']
        creation   = r['creation']
        ts         = r['ts']
        on_poss    = r['on_poss']
        fga        = r['fga']
        fgm        = r['fgm']
        fta        = r['fta']
        pts        = r['pts']
        unast_fgm  = r['unast_fgm']
        season     = r['season']

        print(f'  lag {lag}  {season}  on_poss={on_poss:.0f}')
        print(f'    fga={fga:.0f}  unast_fgm={unast_fgm:.0f}  fgm={fgm:.0f}  fta={fta:.0f}  pts={pts:.0f}')
        print(f'    unast_prop = {unast_fgm:.0f} / {fgm:.0f} = {unast_prop:.3f}')
        print(f'    fga_p100   = {fga:.0f} / {on_poss:.0f} x 100 = {fga_p100:.2f}')
        print(f'    creation   = {fga_p100:.2f} x ({unast_prop:.3f} + 0.3 x {1-unast_prop:.3f}) = {fga_p100:.2f} x {unast_prop + ALPHA*(1-unast_prop):.3f} = {creation:.2f}')
        print(f'    ts%        = {pts:.0f} / (2 x ({fga:.0f} + 0.44 x {fta:.0f})) = {ts:.3f}')
        print(f'    weight     = 0.6^{lag-1} x {on_poss:.0f} = {w:.0f}')

        total_w        += w
        creation_wsum  += creation * w
        ts_wsum        += ts * w
        tov_wsum       += r['tov_rate'] * w

    bl_c   = creation_wsum / total_w
    bl_ts  = ts_wsum / total_w
    bl_tov = tov_wsum / total_w
    baselines[pid] = {'name': name, 'bl_creation': bl_c, 'bl_ts': bl_ts, 'bl_tov_rate': bl_tov}
    print(f'  => bl_creation = {creation_wsum:.1f} / {total_w:.1f} = {bl_c:.2f}')
    print(f'  => bl_ts       = {ts_wsum:.2f} / {total_w:.1f} = {bl_ts:.3f}')
    print()

print('=== LINEUP TOTALS ===')
creation_sum = sum(b['bl_creation'] for b in baselines.values())
tov_per_100  = sum(b['bl_tov_rate'] for b in baselines.values()) * 100
shot_rate    = 100 + OREB - tov_per_100

parts = ' + '.join(f"{b['bl_creation']:.2f}" for b in baselines.values())
print(f'lineup_creation = {parts} = {creation_sum:.2f}')
tov_parts = ' + '.join(f"{b['bl_tov_rate']:.4f}" for b in baselines.values())
print(f'lineup_tov/100  = ({tov_parts}) x 100 = {tov_per_100:.2f}')
print(f'shot_rate       = 100 + {OREB} - {tov_per_100:.2f} = {shot_rate:.2f}')
print()
print(f'  {"Player":<18} {"bl_creat":>8} {"share":>7} {"pred_att":>9} {"bl_ts":>6} {"exp_PPP":>8}')
print('  ' + '-'*62)
total_exp = 0
for b in baselines.values():
    share = b['bl_creation'] / creation_sum
    pred  = share * shot_rate
    ec    = b['bl_ts'] * 2 * pred
    total_exp += ec
    print(f'  {b["name"]:<18} {b["bl_creation"]:>8.2f} {share*100:>6.1f}%  {pred:>8.1f}  {b["bl_ts"]:>5.3f}  {ec:>8.2f}')
print('  ' + '-'*62)
print(f'  {"TOTAL":<18} {creation_sum:>8.2f}  100.0%  {shot_rate:>8.1f}         {total_exp:>8.2f}')
print()
print(f'Expected PPP = {total_exp:.1f}')
print(f'Actual PPP   = 116.3')
print(f'Residual     = {116.3 - total_exp:.1f}')
