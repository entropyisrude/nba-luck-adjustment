"""
Empirically estimate the implied alpha for assisted vs. unassisted shots.

Instead of assuming alpha, regress lineup PPP residual on three separate predictors:
  - lineup_unast_rate  : sum of players' unassisted FGA per 100 poss
  - lineup_ast_rate    : sum of players' assisted FGA per 100 poss
  - lineup_ft_rate     : sum of players' possession-equivalent FTA per 100 poss

The ratio of beta_ast / beta_unast is the data-implied alpha.
The ratio of beta_ft / beta_unast is the data-implied FT weight relative to unassisted FGA.

Uses stint-level observations (each continuous stretch of same lineup = one row).
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

LAMBDA_DECAY   = 0.6
MAX_PRIOR      = 3
MIN_PLAYER_POSS = 300
MIN_STINT_POSS  = 2
OREB_PER_100   = 11
DB_PATH        = Path("data/nba_analytics.duckdb")


def season_to_year(s: str) -> int:
    return int(s[:4])


def load_player_season(con) -> pd.DataFrame:
    df = con.execute("""
        SELECT
            season, player_id,
            SUM(pts)              AS pts,
            SUM(fgm)              AS fgm,
            SUM(fga)              AS fga,
            SUM(ftm)              AS ftm,
            SUM(fta)              AS fta,
            SUM(tov)              AS tov,
            SUM(ast)              AS ast,
            SUM(on_possessions)   AS on_poss,
            SUM(unassisted_fgm)   AS unast_fgm,
            SUM(unassisted_2pm)   AS unast_2pm,
            SUM(unassisted_3pm)   AS unast_3pm,
            SUM(assisted_2pm)     AS ast_2pm,
            SUM(assisted_3pm)     AS ast_3pm
        FROM player_game_facts
        WHERE game_id LIKE '2%' AND on_possessions > 0
        GROUP BY season, player_id
        HAVING SUM(on_possessions) >= ?
    """, [MIN_PLAYER_POSS]).fetchdf()

    df['yr']         = df['season'].apply(season_to_year)
    df['unast_prop'] = np.where(df['fgm'] > 0, df['unast_fgm'] / df['fgm'], 0.5)

    # Shot-creation components (receiver side)
    df['unast_rate']     = df['fga'] / df['on_poss'] * 100 * df['unast_prop']
    df['ast_rate']       = df['fga'] / df['on_poss'] * 100 * (1 - df['unast_prop'])
    df['ft_rate']        = df['fta'] / df['on_poss'] * 100 * 0.44
    df['ast_given_rate'] = df['ast'] / df['on_poss'] * 100

    shot_att = df['fga'] + 0.44 * df['fta']
    df['ts']            = np.where(shot_att > 0, df['pts'] / (2 * shot_att), 0.5)
    df['tov_rate']      = df['tov'] / df['on_poss']
    df['shot_att_p100'] = shot_att / df['on_poss'] * 100

    # Shot-type-specific efficiency for typed expected PPP
    df['unast_fga_est'] = df['fga'] * df['unast_prop']
    df['ast_fga_est']   = df['fga'] * (1 - df['unast_prop'])
    df['unast_fg_pts']  = df['unast_2pm'] * 2 + df['unast_3pm'] * 3
    df['ast_fg_pts']    = df['ast_2pm']   * 2 + df['ast_3pm']   * 3
    # pts-per-FGA for each type (2 × eFG%); impute with blended ts×2 if too few attempts
    df['efg_unast'] = np.where(df['unast_fga_est'] >= 10,
                                df['unast_fg_pts'] / df['unast_fga_est'],
                                df['ts'] * 2)
    df['efg_ast']   = np.where(df['ast_fga_est']   >= 10,
                                df['ast_fg_pts']   / df['ast_fga_est'],
                                df['ts'] * 2)
    df['ft_pct']    = np.where(df['fta'] >= 10, df['ftm'] / df['fta'], 0.75)
    # Proportion of total shot_att from each component
    df['unast_fga_prop'] = df['unast_fga_est'] / shot_att
    df['ast_fga_prop']   = df['ast_fga_est']   / shot_att
    df['ft_prop']        = 0.44 * df['fta']    / shot_att
    df['ppp']      = df['pts'] / df['on_poss'] * 100
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
        typed_cols = ['ts', 'tov_rate', 'unast_rate', 'ast_rate', 'ft_rate',
                      'ast_given_rate', 'ppp', 'shot_att_p100',
                      'efg_unast', 'efg_ast', 'ft_pct',
                      'unast_fga_prop', 'ast_fga_prop', 'ft_prop']
        for col in typed_cols:
            prior[f'{col}_w'] = prior[col] * prior['w']
        g = prior.groupby('player_id').agg(
            ts_wsum              = ('ts_w',              'sum'),
            tov_rate_wsum        = ('tov_rate_w',        'sum'),
            unast_rate_wsum      = ('unast_rate_w',      'sum'),
            ast_rate_wsum        = ('ast_rate_w',        'sum'),
            ft_rate_wsum         = ('ft_rate_w',         'sum'),
            ast_given_rate_wsum  = ('ast_given_rate_w',  'sum'),
            ppp_wsum             = ('ppp_w',             'sum'),
            shot_att_p100_wsum   = ('shot_att_p100_w',   'sum'),
            efg_unast_wsum       = ('efg_unast_w',       'sum'),
            efg_ast_wsum         = ('efg_ast_w',         'sum'),
            ft_pct_wsum          = ('ft_pct_w',          'sum'),
            unast_fga_prop_wsum  = ('unast_fga_prop_w',  'sum'),
            ast_fga_prop_wsum    = ('ast_fga_prop_w',    'sum'),
            ft_prop_wsum         = ('ft_prop_w',         'sum'),
            w_total              = ('w',                 'sum'),
        ).reset_index()
        g['bl_ts']             = g['ts_wsum']             / g['w_total']
        g['bl_tov_rate']       = g['tov_rate_wsum']       / g['w_total']
        g['bl_unast_rate']     = g['unast_rate_wsum']     / g['w_total']
        g['bl_ast_rate']       = g['ast_rate_wsum']       / g['w_total']
        g['bl_ft_rate']        = g['ft_rate_wsum']        / g['w_total']
        g['bl_ast_given_rate'] = g['ast_given_rate_wsum'] / g['w_total']
        g['bl_ppp']            = g['ppp_wsum']            / g['w_total']
        g['bl_shot_att']       = g['shot_att_p100_wsum']  / g['w_total']
        g['bl_efg_unast']      = g['efg_unast_wsum']      / g['w_total']
        g['bl_efg_ast']        = g['efg_ast_wsum']        / g['w_total']
        g['bl_ft_pct']         = g['ft_pct_wsum']         / g['w_total']
        g['bl_unast_fga_prop'] = g['unast_fga_prop_wsum'] / g['w_total']
        g['bl_ast_fga_prop']   = g['ast_fga_prop_wsum']   / g['w_total']
        g['bl_ft_prop']        = g['ft_prop_wsum']        / g['w_total']
        g['target_yr']    = target_yr
        records.append(g[['target_yr', 'player_id', 'bl_ts', 'bl_tov_rate',
                           'bl_unast_rate', 'bl_ast_rate', 'bl_ft_rate',
                           'bl_ast_given_rate', 'bl_ppp', 'bl_shot_att',
                           'bl_efg_unast', 'bl_efg_ast', 'bl_ft_pct',
                           'bl_unast_fga_prop', 'bl_ast_fga_prop', 'bl_ft_prop']])
    return pd.concat(records, ignore_index=True)


def load_stints(con) -> pd.DataFrame:
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
            game_id, offense_team, stint_id, lineup_id,
            off_p1, off_p2, off_p3, off_p4, off_p5,
            CASE WHEN MONTH(MIN(date)) >= 10 THEN YEAR(MIN(date)) ELSE YEAR(MIN(date)) - 1 END AS yr,
            COUNT(*) AS poss_count,
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

    bl_cols = ['yr', 'player_id', 'bl_ts', 'bl_tov_rate',
               'bl_unast_rate', 'bl_ast_rate', 'bl_ft_rate', 'bl_ast_given_rate',
               'bl_ppp', 'bl_shot_att',
               'bl_efg_unast', 'bl_efg_ast', 'bl_ft_pct',
               'bl_unast_fga_prop', 'bl_ast_fga_prop', 'bl_ft_prop']
    merged = long.merge(bl[bl_cols], on=['yr', 'player_id'], how='left')

    key = ['yr', 'game_id', 'team_id', 'stint_id', 'lineup_id']
    all_covered = merged.groupby(key)['bl_ts'].transform(lambda x: x.notna().all())
    merged = merged[all_covered].copy()

    # Usage allocation (same for both models)
    merged['shot_att_sum']       = merged.groupby(key)['bl_shot_att'].transform('sum')
    merged['lineup_tov_per_100'] = merged.groupby(key)['bl_tov_rate'].transform('sum') * 100
    merged['usage_share']        = merged['bl_shot_att'] / merged['shot_att_sum']
    merged['shot_rate']          = (100 + OREB_PER_100 - merged['lineup_tov_per_100']).clip(lower=0)
    merged['pred_shot_att']      = merged['usage_share'] * merged['shot_rate']

    # Model A: blended TS% expected PPP (original)
    merged['expected_contrib']   = merged['bl_ts'] * 2 * merged['pred_shot_att']

    # Model typed: shot-type-specific expected PPP
    # Split pred_shot_att into unassisted FGA, assisted FGA, and FTA components
    merged['pred_unast_fga'] = merged['pred_shot_att'] * merged['bl_unast_fga_prop']
    merged['pred_ast_fga']   = merged['pred_shot_att'] * merged['bl_ast_fga_prop']
    merged['pred_ft']        = merged['pred_shot_att'] * merged['bl_ft_prop']
    # Expected pts: efg × FGA + ft_pct × raw_FTA (pred_ft is in 0.44×FTA units)
    merged['expected_contrib_typed'] = (
        merged['bl_efg_unast'] * merged['pred_unast_fga'] +
        merged['bl_efg_ast']   * merged['pred_ast_fga'] +
        merged['bl_ft_pct']    * merged['pred_ft'] / 0.44
    )

    grp_key = key + ['poss_count', 'actual_ppp']
    agg = (
        merged.groupby(grp_key).agg(
            expected_ppp          = ('expected_contrib',       'sum'),
            expected_ppp_typed    = ('expected_contrib_typed', 'sum'),
            lineup_unast_rate     = ('bl_unast_rate',          'sum'),
            lineup_ast_rate       = ('bl_ast_rate',            'sum'),
            lineup_ft_rate        = ('bl_ft_rate',             'sum'),
            lineup_ast_given_rate = ('bl_ast_given_rate',      'sum'),
            lineup_ppp            = ('bl_ppp',                 'mean'),
        ).reset_index()
    )
    agg['residual']       = agg['actual_ppp'] - agg['expected_ppp']
    agg['residual_typed'] = agg['actual_ppp'] - agg['expected_ppp_typed']
    return agg


def report(result: pd.DataFrame) -> None:
    print(f"\n{'='*60}")
    print(f"Stint-level obs : {len(result):,}   Seasons: {result['yr'].nunique()}")
    print(f"Residual        : mean={result['residual'].mean():.2f}  sd={result['residual'].std():.2f}")
    print(f"\nLineup-level predictor means:")
    print(f"  unast_rate      : {result['lineup_unast_rate'].mean():.2f}")
    print(f"  ast_rate        : {result['lineup_ast_rate'].mean():.2f}")
    print(f"  ft_rate         : {result['lineup_ft_rate'].mean():.2f}")
    print(f"  ast_given_rate  : {result['lineup_ast_given_rate'].mean():.2f}")

    print(f"\n{'='*60}")
    print("Model A (original): residual ~ unast_rate + ast_rate + ft_rate")
    print(f"{'='*60}")

    if HAS_STATSMODELS:
        # ── Typed expected PPP comparison ────────────────────────────────
        print(f"\n{'='*60}")
        print("KEY TEST: does shot-type-specific expected PPP change creation beta?")
        print(f"{'='*60}")
        print(f"  Blended residual  : mean={result['residual'].mean():.3f}  "
              f"sd={result['residual'].std():.3f}")
        print(f"  Typed   residual  : mean={result['residual_typed'].mean():.3f}  "
              f"sd={result['residual_typed'].std():.3f}")

        formula3 = 'residual       ~ lineup_unast_rate + lineup_ast_rate + lineup_ft_rate'
        formula3t = 'residual_typed ~ lineup_unast_rate + lineup_ast_rate + lineup_ft_rate'
        m3  = smf.wls(formula3,  data=result, weights=result['poss_count']).fit()
        m3t = smf.wls(formula3t, data=result, weights=result['poss_count']).fit()

        print(f"\n{'Predictor':<26} {'Blended b':>10} {'Typed b':>10} {'Delta':>8}")
        print('-' * 56)
        for p in ['lineup_unast_rate', 'lineup_ast_rate', 'lineup_ft_rate']:
            b  = m3.params[p];  b_t = m3t.params[p]
            print(f"  {p:<24} {b:>10.4f} {b_t:>10.4f} {b_t-b:>+8.4f}")
        print(f"\n  Implied alpha (blended) : {m3.params['lineup_ast_rate']/m3.params['lineup_unast_rate']:.3f}")
        print(f"  Implied alpha (typed)   : {m3t.params['lineup_ast_rate']/m3t.params['lineup_unast_rate']:.3f}")
        print(f"  R2 blended: {m3.rsquared:.4f}   R2 typed: {m3t.rsquared:.4f}")
        print(f"\n  League avg prior efg_unast : "
              f"{result['lineup_unast_rate'].corr(result['residual_typed'] - result['residual']):.4f}"
              f"  (corr of typed-blended gap with unassisted rate)")

        m_orig = smf.wls(
            'residual ~ lineup_unast_rate + lineup_ast_rate + lineup_ft_rate',
            data=result, weights=result['poss_count']
        ).fit()
        print(m_orig.summary().tables[1])
        print(f"\nR2 = {m_orig.rsquared:.4f}   N = {int(m_orig.nobs):,}")
        b_unast = m_orig.params['lineup_unast_rate']
        b_ast   = m_orig.params['lineup_ast_rate']
        b_ft    = m_orig.params['lineup_ft_rate']
        print(f"Implied alpha (beta_ast / beta_unast)    = {b_ast/b_unast:.3f}")
        print(f"Implied FT weight (beta_ft / beta_unast) = {b_ft/b_unast:.3f}")

        print(f"\n{'='*60}")
        print("Model B (+ passing): residual ~ unast_rate + ast_rate + ft_rate + ast_given_rate")
        print(f"{'='*60}")
        m_pass = smf.wls(
            'residual ~ lineup_unast_rate + lineup_ast_rate + lineup_ft_rate + lineup_ast_given_rate',
            data=result, weights=result['poss_count']
        ).fit()
        print(m_pass.summary().tables[1])
        print(f"\nR2 = {m_pass.rsquared:.4f}   N = {int(m_pass.nobs):,}")
        b_unast2  = m_pass.params['lineup_unast_rate']
        b_ast2    = m_pass.params['lineup_ast_rate']
        b_ast_giv = m_pass.params['lineup_ast_given_rate']
        print(f"Implied alpha (beta_ast / beta_unast)         = {b_ast2/b_unast2:.3f}")
        print(f"Passing coef (beta_ast_given / beta_unast)    = {b_ast_giv/b_unast2:.3f}")

        print(f"\n{'='*60}")
        print("Model C (+ quality control): adds lineup avg prior PPP")
        print(f"{'='*60}")
        m_qual = smf.wls(
            'residual ~ lineup_unast_rate + lineup_ast_rate + lineup_ft_rate'
            ' + lineup_ast_given_rate + lineup_ppp',
            data=result, weights=result['poss_count']
        ).fit()
        print(m_qual.summary().tables[1])
        print(f"\nR2 = {m_qual.rsquared:.4f}   N = {int(m_qual.nobs):,}")
        b_u3   = m_qual.params['lineup_unast_rate']
        b_a3   = m_qual.params['lineup_ast_rate']
        b_ag3  = m_qual.params['lineup_ast_given_rate']
        b_ppp3 = m_qual.params['lineup_ppp']
        print(f"Implied alpha (beta_ast / beta_unast)         = {b_a3/b_u3:.3f}")
        print(f"Passing coef (beta_ast_given / beta_unast)    = {b_ag3/b_u3:.3f}")
        print(f"Quality control (lineup_ppp) coef             = {b_ppp3:.4f}")
        print(f"\nPassing coef shift from Model B to C: {b_ag3:.4f} vs {b_ast_giv:.4f}"
              f"  ({(b_ag3-b_ast_giv)/b_ast_giv*100:+.1f}%)")
        print(f"Unassisted coef shift: {b_u3:.4f} vs {b_unast2:.4f}"
              f"  ({(b_u3-b_unast2)/b_unast2*100:+.1f}%)")

        # Correlation of quality control with other predictors
        print(f"\nCorrelation of lineup_ppp with predictors:")
        for col in ['lineup_unast_rate', 'lineup_ast_rate', 'lineup_ft_rate', 'lineup_ast_given_rate']:
            print(f"  {col:<28}  r={result[col].corr(result['lineup_ppp']):.3f}")

        # Univariate checks
        print(f"\n{'='*60}")
        print("Univariate regressions (each predictor alone):")
        for col, label in [
            ('lineup_unast_rate',     'unast_rate'),
            ('lineup_ast_rate',       'ast_rate'),
            ('lineup_ft_rate',        'ft_rate'),
            ('lineup_ast_given_rate', 'ast_given_rate'),
        ]:
            u = smf.wls(f'residual ~ {col}', data=result, weights=result['poss_count']).fit()
            b = u.params[col]
            print(f"  {label:<20}  beta={b:.4f}  SE={u.bse[col]:.4f}  r={np.sqrt(u.rsquared)*np.sign(b):.3f}")

        # Correlation matrix of predictors
        print(f"\n{'='*60}")
        print("Predictor correlations:")
        cols = ['lineup_unast_rate', 'lineup_ast_rate', 'lineup_ft_rate', 'lineup_ast_given_rate']
        print(result[cols].corr().round(3).to_string())

        # Subsample regressions: hold one variable at bottom quintile
        print(f"\n{'='*60}")
        print("Subsample regressions (restricting one variable to bottom quintile)")
        print(f"{'='*60}")

        subsamples = [
            ('ast_given_rate in Q1 (low passing)',
             'lineup_ast_given_rate',
             'residual ~ lineup_unast_rate + lineup_ast_rate + lineup_ft_rate',
             ['lineup_unast_rate', 'lineup_ast_rate', 'lineup_ft_rate']),
            ('unast_rate in Q1 (low self-creation)',
             'lineup_unast_rate',
             'residual ~ lineup_ast_rate + lineup_ft_rate + lineup_ast_given_rate',
             ['lineup_ast_rate', 'lineup_ft_rate', 'lineup_ast_given_rate']),
            ('ast_rate in Q1 (low assisted-shot receiving)',
             'lineup_ast_rate',
             'residual ~ lineup_unast_rate + lineup_ft_rate + lineup_ast_given_rate',
             ['lineup_unast_rate', 'lineup_ft_rate', 'lineup_ast_given_rate']),
        ]

        for label, restrict_col, formula, pred_cols in subsamples:
            q20 = result[restrict_col].quantile(0.20)
            sub = result[result[restrict_col] <= q20].copy()
            m = smf.wls(formula, data=sub, weights=sub['poss_count']).fit()
            print(f"\n--- {label} (N={len(sub):,}, {restrict_col} <= {q20:.1f}) ---")
            print(f"  Residual correlations with restricted col: "
                  f"{sub[restrict_col].corr(sub['residual']):.3f}")
            for col in pred_cols:
                b  = m.params[col]
                se = m.bse[col]
                t  = m.tvalues[col]
                print(f"  {col:<28}  beta={b:+.4f}  SE={se:.4f}  t={t:+.1f}")
            print(f"  R2={m.rsquared:.4f}  N={int(m.nobs):,}")
            # Correlations among remaining predictors in this subsample
            corr = sub[pred_cols].corr().round(3)
            print(f"  Predictor correlations in subsample:")
            for i, c1 in enumerate(pred_cols):
                for c2 in pred_cols[i+1:]:
                    print(f"    {c1} vs {c2}: r={corr.loc[c1,c2]:.3f}")
    else:
        print("statsmodels not available")


if __name__ == "__main__":
    con = duckdb.connect(str(DB_PATH), read_only=True)

    print("Loading player-season stats...")
    ps = load_player_season(con)
    print(f"  {len(ps):,} player-seasons")

    print("Building baselines...")
    bl = make_baselines(ps)
    print(f"  {len(bl):,} player x season baseline records")

    print("Loading stints...")
    lineups = load_stints(con)
    print(f"  {len(lineups):,} stints (>= {MIN_STINT_POSS} poss)")

    print("Building result...")
    result = build_result(lineups, bl)
    print(f"  {len(result):,} matched stints")

    report(result)
