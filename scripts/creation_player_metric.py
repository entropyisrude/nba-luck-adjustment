"""
Single-player offensive contribution (OC) metric.

OC = net_shooting + creation_premium - tov_penalty

  net_shooting     = pts_p100 - shot_att_p100 * MISS_COST
  creation_premium = (creation - league_avg_creation) * BETA_PER_PLAYER
  tov_penalty      = tov_p100 * TOV_PENALTY_RATE

Creation score has three components:
  (1) Unassisted FGA:  1.0 creation unit each
  (2) Assisted FGA:    ALPHA_TOTAL * s_eff each   (shooter side, convex in assisted_prop)
  (3) FTA:             0.44 * (FT_UNAST + alpha_shooter * (1-FT_UNAST))
  (4) Assists given:   ALPHA_TOTAL * p_eff each   (passer side, convex in ast_p100)

Convexity:
  s_eff = S_BASE + K_SHOOTER * (assisted_prop - avg_assisted_prop)   [shooter fraction]
  p_eff = P_BASE + M_PASSER  * (ast_p100      - avg_ast_p100)        [passer fraction]

At league-average specialization, s_eff = S_BASE and p_eff = P_BASE.
Specialists (Klay-type finishers, Chris Paul-type passers) earn above-average fractions;
generalists earn below-average fractions. Budget balances in expectation.
"""
from __future__ import annotations
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

# ── Tunable parameters ────────────────────────────────────────────────────────
ALPHA_TOTAL      = 0.34     # total creation credit for an assisted shot event
S_BASE           = 1/3      # shooter's base fraction of ALPHA_TOTAL
P_BASE           = 2/3      # passer's base fraction of ALPHA_TOTAL
K_SHOOTER        = 0.35     # convexity: extra shooter fraction per unit of excess assisted_prop
M_PASSER         = 0.015    # convexity: extra passer fraction per ast_p100 above average
FT_UNAST         = 0.75     # fraction of FT attempts treated as self-created
BETA_PER_PLAYER  = 0.332    # lineup beta / 5 players  (≈ 1.66 / 5; needs re-est at new formula)
MISS_COST        = 0.795    # cost per shot attempt (0.75 × 1.06, OREB-discounted)
TOV_PENALTY_RATE = 1.272    # tov_p100 multiplier (1.06 × 1.20, transition premium)
MIN_POSS         = 300
DB_PATH          = Path("data/nba_analytics.duckdb")
TARGET_SEASON    = "2025-26"
TOP_N            = 50


def load(con, season: str) -> pd.DataFrame:
    df = con.execute("""
        SELECT
            p.season, p.player_id,
            SUM(p.pts)            AS pts,
            SUM(p.fgm)            AS fgm,
            SUM(p.fga)            AS fga,
            SUM(p.fta)            AS fta,
            SUM(p.tov)            AS tov,
            SUM(p.ast)            AS ast,
            SUM(p.on_possessions) AS on_poss,
            SUM(p.unassisted_fgm) AS unast_fgm,
            COALESCE(MAX(n1.display_first_last), MAX(n2.player_name)) AS name
        FROM player_game_facts p
        LEFT JOIN raw_common_player_info      n1 ON n1.person_id = p.player_id
        LEFT JOIN raw_player_metadata_official_recent n2 ON n2.player_id  = p.player_id
        WHERE p.game_id LIKE '2%'
          AND p.on_possessions > 0
          AND p.season = ?
        GROUP BY p.season, p.player_id
        HAVING SUM(p.on_possessions) >= ?
        ORDER BY SUM(p.on_possessions) DESC
    """, [season, MIN_POSS]).fetchdf()

    df['unast_prop']    = np.where(df['fgm'] > 0, df['unast_fgm'] / df['fgm'], 0.5)
    df['assisted_prop'] = 1.0 - df['unast_prop']
    df['fga_p100']      = df['fga']  / df['on_poss'] * 100
    df['fta_p100']      = df['fta']  / df['on_poss'] * 100
    df['ast_p100']      = df['ast']  / df['on_poss'] * 100
    df['pts_p100']      = df['pts']  / df['on_poss'] * 100
    df['tov_p100']      = df['tov']  / df['on_poss'] * 100

    shot_att             = df['fga'] + 0.44 * df['fta']
    df['ts']             = np.where(shot_att > 0, df['pts'] / (2 * shot_att), np.nan)
    df['shot_att_p100']  = shot_att / df['on_poss'] * 100

    return df


def add_creation(df: pd.DataFrame) -> pd.DataFrame:
    """Add creation score and OC columns in-place; returns df."""
    avg_assisted_prop = df['assisted_prop'].mean()
    avg_ast_p100      = df['ast_p100'].mean()

    # Convex shooter and passer fractions
    df['s_eff'] = (S_BASE + K_SHOOTER * (df['assisted_prop'] - avg_assisted_prop)).clip(0.05, 0.95)
    df['p_eff'] = (P_BASE + M_PASSER  * (df['ast_p100']      - avg_ast_p100)).clip(0.10, 1.50)

    alpha_s = ALPHA_TOTAL * df['s_eff']   # shooter's absolute credit per assisted FGA
    alpha_p = ALPHA_TOTAL * df['p_eff']   # passer's absolute credit per assist given

    fga_cr  = df['fga_p100'] * (df['unast_prop'] + alpha_s * df['assisted_prop'])
    fta_cr  = df['fta_p100'] * 0.44 * (FT_UNAST + alpha_s * (1 - FT_UNAST))
    pass_cr = df['ast_p100'] * alpha_p

    df['creation']       = fga_cr + fta_cr + pass_cr
    league_avg           = df['creation'].mean()
    df['net_shooting']   = df['pts_p100'] - df['shot_att_p100'] * MISS_COST
    df['cr_premium']     = (df['creation'] - league_avg) * BETA_PER_PLAYER
    df['tov_penalty']    = df['tov_p100'] * TOV_PENALTY_RATE
    df['OC']             = df['net_shooting'] + df['cr_premium'] - df['tov_penalty']
    df['_league_avg_cr'] = league_avg
    return df


def report(df: pd.DataFrame) -> None:
    league_avg = df['_league_avg_cr'].iloc[0]
    avg_ast    = df['ast_p100'].mean()
    avg_assprop = df['assisted_prop'].mean()

    print(f"\nSeason: {TARGET_SEASON}   N={len(df):,} qualifying players")
    print(f"League avg creation  : {league_avg:.2f}")
    print(f"League avg ast/100   : {avg_ast:.2f}   avg assisted_prop: {avg_assprop:.3f}")
    print(f"\nParameters:")
    print(f"  ALPHA_TOTAL={ALPHA_TOTAL}  S_BASE={S_BASE:.3f}  P_BASE={P_BASE:.3f}")
    print(f"  K_SHOOTER={K_SHOOTER}  M_PASSER={M_PASSER}")
    print(f"  MISS_COST={MISS_COST}  TOV_RATE={TOV_PENALTY_RATE}  BETA/player={BETA_PER_PLAYER:.3f}")
    print(f"\nFormula: OC = (pts/100 - shot_att/100 x {MISS_COST}) "
          f"+ (creation - {league_avg:.1f}) x {BETA_PER_PLAYER:.3f} "
          f"- tov/100 x {TOV_PENALTY_RATE}")

    top = df.sort_values('OC', ascending=False).head(TOP_N).reset_index(drop=True)

    print(f"\n{'Rk':<3} {'Name':<22} {'pts/100':>7} {'sh_att':>6} {'net_sh':>7} "
          f"{'creat':>6} {'s_eff':>5} {'p_eff':>5} {'ast/100':>7} "
          f"{'cr_pr':>6} {'tov/100':>7} {'tov_pn':>6} {'OC':>7} {'TS%':>5}")
    print('-' * 118)
    for i, r in top.iterrows():
        print(
            f"{i+1:<3} {str(r['name']):<22} "
            f"{r['pts_p100']:>7.1f} {r['shot_att_p100']:>6.1f} {r['net_shooting']:>+7.1f} "
            f"{r['creation']:>6.1f} {r['s_eff']:>5.2f} {r['p_eff']:>5.2f} {r['ast_p100']:>7.1f} "
            f"{r['cr_premium']:>+6.2f} {r['tov_p100']:>7.1f} {r['tov_penalty']:>+6.2f} "
            f"{r['OC']:>7.1f} {r['ts']:>5.3f}"
        )


if __name__ == "__main__":
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df  = load(con, TARGET_SEASON)
    df  = add_creation(df)
    report(df)
