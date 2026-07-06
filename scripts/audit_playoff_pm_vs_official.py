"""Compare playoff plus_minus_actual vs official box +/- (Eoin plusMinusPoints)."""
import zipfile, unicodedata, re
import pandas as pd
import duckdb

ROOT = r'C:\Users\Dave\Downloads\nba-onoff-publish'
ZIP = ROOT + r'\historical-nba-data-and-player-box-scores.zip'

def norm(name):
    t = unicodedata.normalize('NFKD', str(name)).encode('ascii','ignore').decode()
    t = re.sub(r'[^a-zA-Z ]','', t.replace('.','')).lower()
    w = t.split()
    while w and w[-1] in {'jr','sr','ii','iii','iv','v'}: w.pop()
    return ' '.join(w)

with zipfile.ZipFile(ZIP) as z:
    with z.open('PlayerStatistics.csv') as f:
        eo = pd.read_csv(f, usecols=['gameDateTimeEst','gameType','firstName','lastName',
                                     'numMinutes','plusMinusPoints'], low_memory=False)
eo = eo[eo['gameType']=='Playoffs'].copy()
eo['date'] = pd.to_datetime(eo['gameDateTimeEst'], errors='coerce').dt.strftime('%Y-%m-%d')
eo['key'] = eo['firstName'].fillna('').astype(str) + ' ' + eo['lastName'].fillna('').astype(str)
eo['key'] = eo['key'].map(norm)
eo['pm_off'] = pd.to_numeric(eo['plusMinusPoints'], errors='coerce')
eo = eo.dropna(subset=['pm_off'])
eo = eo.groupby(['date','key'], as_index=False).agg(pm_off=('pm_off','sum'))

con = duckdb.connect(ROOT + r'\data\nba_analytics_playoffs.duckdb', read_only=True)
ours = con.execute("""
    SELECT CAST(date AS VARCHAR) date, player_name, season,
           plus_minus_actual pm, minutes
    FROM player_game_facts WHERE plus_minus_actual IS NOT NULL
""").df()
ours['key'] = ours['player_name'].map(norm)

m = ours.merge(eo, on=['date','key'], how='left')
m['diff'] = (m['pm'] - m['pm_off']).abs()
print('our rows:', len(ours), ' matched:', m['pm_off'].notna().sum())
g = m.dropna(subset=['pm_off']).groupby('season').agg(
    n=('diff','size'),
    exact=('diff', lambda s: (s<=0.5).mean().round(3)),
    mae=('diff','mean'),
    p95=('diff', lambda s: s.quantile(0.95)),
    worst=('diff','max'))
pd.set_option('display.width',150)
print(g.round(2))
w = m.dropna(subset=['pm_off']).nlargest(10, 'diff')[['date','player_name','season','pm','pm_off','minutes']]
print(w.to_string())
