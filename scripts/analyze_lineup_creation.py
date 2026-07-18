"""
Analyze effect of lineup creation proclivity on offensive PPP residuals.

2x2 comparison: ALPHA (0.3 vs 0.5) x aggregation (quadratic vs simple average).
Usage allocation always decoupled from creation (conventional shot-attempt rate).
"""
from __future__ import annotations
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

try:
    import statsmodels.formula.api as smf
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False
    from scipy import stats

LAMBDA_DECAY = 0.6
MAX_PRIOR = 3
MIN_STINT_POSS = 2
MIN_PLAYER_POSS = 300
OREB_PER_100 = 11
DB_PATH = Path("data/nba_analytics.duckdb")


def season_to_year(s: str) -> int:
    return int(s[:4])


def load_player_season(con) -> pd.DataFrame:
    df = con.execute("""
        SELECT
            season, player_id,
            SUM(pts)            AS pts,
            SUM(fgm)            AS fgm,
            SUM(fga)            AS fga,
            SUM(fta)            AS fta,
            SUM(tov)            AS tov,
            SUM(on_possessions) AS on_poss,
            SUM(unassisted_fgm) AS unast_fgm,
            SUM(minutes)        AS minutes
        FROM player_game_facts
        WHERE game_id LIKE '2%' AND on_possessions > 0
        GROUP BY season, player_id
        HAVING SUM(on_possessions) >= ?
    """, [MIN_PLAYER_POSS]).fetchdf()

    df['yr']         = df['season'].apply(season_to_year)
    df['unast_prop'] = np.where(df['fgm'] > 0, df['unast_fgm'] / df['fgm'], 0.5)
    df['fga_p100']   = df['fga'] / df['on_poss'] * 100

    # FTA contribution: 75% self-created, 25% assisted; scaled by 0.44 to match FGA possession units
    df['fta_p100'] = df['fta'] / df['on_poss'] * 100
    ft_03 = df['fta_p100'] * 0.44 * (0.75 + 0.3 * 0.25)
    ft_05 = df['fta_p100'] * 0.44 * (0.75 + 0.5 * 0.25)

    # Creation scores for both alpha values (FGA component + FTA component)
    df['creation_03'] = df['fga_p100'] * (df['unast_prop'] + 0.3 * (1 - df['unast_prop'])) + ft_03
    df['creation_05'] = df['fga_p100'] * (df['unast_prop'] + 0.5 * (1 - df['unast_prop'])) + ft_05

    shot_att = df['fga'] + 0.44 * df['fta']
    df['ts']           = np.where(shot_att > 0, df['pts'] / (2 * shot_att), 0.5)
    df['tov_rate']     = df['tov'] / df['on_poss']
    df['shot_att_p100'] = shot_att / df['on_poss'] * 100
    df['ppp']          = df['pts'] / df['on_poss'] * 100
    return df


def make_baselines(ps: pd.DataFrame) -> pd.DataFrame:
    records = []
    for target_yr in sorted(ps['yr'].unique()):
        lags = []
        for lag in range(1, MAX_PRIOR + 1):
            sub = ps[ps['yr'] == target_yr - lag].copy()
            if sub.empty:
                continue
            sub['w'] = (LAMBDA_DECAY ** (lag - 1)) * sub['on_poss']
            lags.append(sub)
        if not lags:
            continue
        prior = pd.concat(lags, ignore_index=True)
        for col in ['ts', 'tov_rate', 'creation_03', 'creation_05', 'ppp', 'shot_att_p100']:
            prior[f'{col}_w'] = prior[col] * prior['w']
        g = prior.groupby('player_id').agg(
            ts_wsum            = ('ts_w',            'sum'),
            tov_rate_wsum      = ('tov_rate_w',      'sum'),
            creation_03_wsum   = ('creation_03_w',   'sum'),
            creation_05_wsum   = ('creation_05_w',   'sum'),
            ppp_wsum           = ('ppp_w',           'sum'),
            shot_att_p100_wsum = ('shot_att_p100_w', 'sum'),
            w_total            = ('w',               'sum'),
            n_seasons          = ('yr',              'nunique'),
        ).reset_index()
        g['bl_ts']          = g['ts_wsum']            / g['w_total']
        g['bl_tov_rate']    = g['tov_rate_wsum']      / g['w_total']
        g['bl_creation_03'] = g['creation_03_wsum']   / g['w_total']
        g['bl_creation_05'] = g['creation_05_wsum']   / g['w_total']
        g['bl_ppp']         = g['ppp_wsum']           / g['w_total']
        g['bl_shot_att']    = g['shot_att_p100_wsum'] / g['w_total']
        g['target_yr']      = target_yr
        records.append(g[['target_yr', 'player_id', 'bl_ts', 'bl_tov_rate',
                           'bl_creation_03', 'bl_creation_05', 'bl_ppp', 'bl_shot_att']])
    return pd.concat(records, ignore_index=True)


def load_lineup_ppp(con) -> pd.DataFrame:
    """Load individual lineup stints — each continuous stretch of the same 5-man lineup."""
    df = con.execute("""
        WITH ordered AS (
            SELECT
                game_id, poss_index, offense_team, date,
                off_p1, off_p2, off_p3, off_p4, off_p5, points,
                ARRAY_TO_STRING(LIST_SORT([off_p1,off_p2,off_p3,off_p4,off_p5]),'-') AS lineup_id,
                LAG(ARRAY_TO_STRING(LIST_SORT([off_p1,off_p2,off_p3,off_p4,off_p5]),'-'))
                    OVER (PARTITION BY game_id, offense_team ORDER BY poss_index) AS prev_lineup
            FROM raw_hist_possessions
            WHERE game_id BETWEEN 20000000 AND 29999999
        ),
        with_stint AS (
            SELECT *,
                SUM(CASE WHEN lineup_id != prev_lineup OR prev_lineup IS NULL THEN 1 ELSE 0 END)
                    OVER (PARTITION BY game_id, offense_team ORDER BY poss_index
                          ROWS UNBOUNDED PRECEDING) AS stint_id
            FROM ordered
        )
        SELECT
            game_id,
            offense_team,
            stint_id,
            lineup_id,
            off_p1, off_p2, off_p3, off_p4, off_p5,
            CASE WHEN MONTH(MIN(date)) >= 10 THEN YEAR(MIN(date)) ELSE YEAR(MIN(date)) - 1 END AS yr,
            COUNT(*)                             AS poss_count,
            SUM(points)                          AS pts_total,
            CAST(SUM(points) AS DOUBLE)/COUNT(*)*100 AS actual_ppp
        FROM with_stint
        GROUP BY game_id, offense_team, stint_id, lineup_id,
                 off_p1, off_p2, off_p3, off_p4, off_p5
        HAVING COUNT(*) >= ?
    """, [MIN_STINT_POSS]).fetchdf()
    return df.rename(columns={'offense_team': 'team_id'})


def build_result(lineups: pd.DataFrame, baselines: pd.DataFrame) -> pd.DataFrame:
    bl = baselines.rename(columns={'target_yr': 'yr'})

    long = pd.concat([
        lineups[['yr', 'game_id', 'team_id', 'stint_id', 'lineup_id', 'poss_count', 'actual_ppp', p]]
                .rename(columns={p: 'player_id'})
        for p in ['off_p1', 'off_p2', 'off_p3', 'off_p4', 'off_p5']
    ], ignore_index=True)
    long['player_id'] = long['player_id'].astype('int64')

    merged = long.merge(
        bl[['yr', 'player_id', 'bl_ts', 'bl_tov_rate',
            'bl_creation_03', 'bl_creation_05', 'bl_ppp', 'bl_shot_att']],
        on=['yr', 'player_id'], how='left'
    )

    key = ['yr', 'game_id', 'team_id', 'stint_id', 'lineup_id']
    all_covered = merged.groupby(key)['bl_ts'].transform(lambda x: x.notna().all())
    merged = merged[all_covered].copy()

    # Usage: conventional shot-attempt rate, decoupled from creation
    merged['shot_att_sum']       = merged.groupby(key)['bl_shot_att'].transform('sum')
    merged['lineup_tov_per_100'] = merged.groupby(key)['bl_tov_rate'].transform('sum') * 100
    merged['usage_share']        = merged['bl_shot_att'] / merged['shot_att_sum']
    merged['shot_rate']          = (100 + OREB_PER_100 - merged['lineup_tov_per_100']).clip(lower=0)
    merged['pred_shot_att']      = merged['usage_share'] * merged['shot_rate']
    merged['expected_contrib']   = merged['bl_ts'] * 2 * merged['pred_shot_att']

    # Creation aggregates for both alphas
    for a in ['03', '05']:
        col = f'bl_creation_{a}'
        merged[f'c{a}_sum']    = merged.groupby(key)[col].transform('sum')
        merged[f'c{a}_sq_sum'] = merged.groupby(key)[col].transform(lambda x: (x**2).sum())

    grp_key = key + ['poss_count', 'actual_ppp']
    agg = (
        merged.groupby(grp_key).agg(
            expected_ppp  = ('expected_contrib', 'sum'),
            old_expected_ppp = ('bl_ppp',        'sum'),
            c03_sum       = ('c03_sum',           'first'),
            c03_sq_sum    = ('c03_sq_sum',        'first'),
            c05_sum       = ('c05_sum',           'first'),
            c05_sq_sum    = ('c05_sq_sum',        'first'),
        ).reset_index()
    )

    agg['quadratic_03'] = agg['c03_sq_sum'] / agg['c03_sum']  # sum(c²)/sum(c), alpha=0.3
    agg['avg_03']       = agg['c03_sum'] / 5                  # mean(c), alpha=0.3
    agg['quadratic_05'] = agg['c05_sq_sum'] / agg['c05_sum']  # sum(c²)/sum(c), alpha=0.5
    agg['avg_05']       = agg['c05_sum'] / 5                  # mean(c), alpha=0.5

    agg['residual']     = agg['actual_ppp'] - agg['expected_ppp']
    agg['old_residual'] = agg['actual_ppp'] - agg['old_expected_ppp']
    return agg


def _run_regression(result: pd.DataFrame, resid_col: str, creation_col: str, label: str) -> None:
    if HAS_STATSMODELS:
        m = smf.wls(f'{resid_col} ~ {creation_col}', data=result, weights=result['poss_count']).fit()
        print(f"  {label:<40}  beta={m.params[creation_col]:+.4f}  SE={m.bse[creation_col]:.4f}"
              f"  r={np.sqrt(m.rsquared) * np.sign(m.params[creation_col]):.3f}"
              f"  p={m.pvalues[creation_col]:.2e}")
    else:
        slope, intercept, r, p, se = stats.linregress(result[creation_col], result[resid_col])
        print(f"  {label:<40}  beta={slope:+.4f}  SE={se:.4f}  r={r:.3f}  p={p:.2e}")


def _quintile_table(result: pd.DataFrame, creation_col: str) -> pd.DataFrame:
    q_col = f'_q_{creation_col}'
    result[q_col] = pd.qcut(result[creation_col], 5,
                             labels=['Q1 lowest', 'Q2', 'Q3', 'Q4', 'Q5 highest'])
    return (result.groupby(q_col, observed=True)
            .agg(n=('residual', 'count'),
                 mean_creation=(creation_col, 'mean'),
                 mean_residual=('residual', 'mean'),
                 poss_wtd=('residual',
                           lambda x: np.average(x, weights=result.loc[x.index, 'poss_count'])))
            .round(2))


def report(result: pd.DataFrame) -> None:
    print(f"\n{'='*60}")
    print(f"Lineup-season obs : {len(result):,}   Seasons: {result['yr'].nunique()}")
    print(f"Actual PPP        : mean={result['actual_ppp'].mean():.2f}  sd={result['actual_ppp'].std():.2f}")
    print(f"Expected PPP      : mean={result['expected_ppp'].mean():.2f}  sd={result['expected_ppp'].std():.2f}")
    print(f"Residual          : mean={result['residual'].mean():.2f}  sd={result['residual'].std():.2f}")

    metrics = [
        ('quadratic_03', 'quadratic  alpha=0.3'),
        ('avg_03',       'simple avg alpha=0.3'),
        ('quadratic_05', 'quadratic  alpha=0.5'),
        ('avg_05',       'simple avg alpha=0.5'),
    ]

    print(f"\n{'='*60}")
    print("Full-sample regressions (possession-weighted WLS):")
    for col, label in metrics:
        _run_regression(result, 'residual', col, label)

    for col, label in metrics:
        print(f"\n{'='*60}")
        print(f"Quintiles — {label}:")
        print(_quintile_table(result, col).to_string())


if __name__ == "__main__":
    con = duckdb.connect(str(DB_PATH), read_only=True)

    print("Loading player-season stats...")
    ps = load_player_season(con)
    print(f"  {len(ps):,} player-seasons  ({ps['yr'].min()}-{ps['yr'].max()})")

    print("Building prior-season baselines...")
    bl = make_baselines(ps)
    print(f"  {len(bl):,} player x season baseline records")

    print("Loading lineup PPP from PBP possessions...")
    lineups = load_lineup_ppp(con)
    print(f"  {len(lineups):,} lineup-seasons (>= {MIN_STINT_POSS} poss)")

    print("Matching player baselines to lineups...")
    result = build_result(lineups, bl)
    n_dropped = len(lineups) - len(result)
    print(f"  {len(result):,} matched  /  {n_dropped:,} dropped (missing >= 1 player baseline)")

    if len(result) == 0:
        print("DEBUG: no matches — check year alignment")
    else:
        report(result)
        out = Path("data/lineup_creation_analysis.csv")
        result.to_csv(out, index=False)
        print(f"\nSaved -> {out}")
