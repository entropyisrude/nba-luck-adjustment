"""
NBA Deficit Analysis -- do leads matter less in the modern era?

All data comes from lineup_stint_facts in nba_analytics.duckdb -- no API calls.
Covers 35K regular-season games, 1996-97 through 2025-26.

Tests:
  1. Pace (total pts/game) by era -- has the game sped up?
  2. Q3 trailing-team win rate by deficit bucket and era
  3. Logistic regression: P(comeback) ~ deficit + era + deficitxera
     Positive interaction -> same deficit is less daunting now (claim supported)
     Negative interaction -> same deficit is MORE dangerous now (claim refuted)
  4. Specific thresholds the claim made: 13-pt Q3 deficit, 8-pt Q3 lead
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.special import expit

DB_PATH = Path(__file__).parent.parent / "data" / "nba_analytics.duckdb"

ERA_ORDER = ["1997-2003", "2004-2010", "2011-2017", "2018-2025"]

def era_label(season: str) -> str | None:
    try:
        yr = int(season[:4])
    except Exception:
        return None
    if yr < 1997 or yr > 2024:
        return None          # exclude partial 2025-26 season
    if yr < 2004: return "1997-2003"
    if yr < 2011: return "2004-2010"
    if yr < 2018: return "2011-2017"
    return "2018-2025"


def load_game_quarter_scores(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    One row per game with scores at end of Q1, Q2, Q3, and final.
    Uses the last stint that ends in each period to get the exact score.
    Regular-season games only (game_id starts with '2').
    """
    df = con.execute("""
        SELECT
            game_id,
            season,
            home_abbr,
            away_abbr,
            -- score at end of each quarter = score when the last stint of that period ends
            MAX(CASE WHEN end_period = 1 THEN end_home_score END) AS home_q1,
            MAX(CASE WHEN end_period = 1 THEN end_away_score END) AS away_q1,
            MAX(CASE WHEN end_period = 2 THEN end_home_score END) AS home_q2,
            MAX(CASE WHEN end_period = 2 THEN end_away_score END) AS away_q2,
            MAX(CASE WHEN end_period = 3 THEN end_home_score END) AS home_q3,
            MAX(CASE WHEN end_period = 3 THEN end_away_score END) AS away_q3,
            MAX(end_home_score) AS home_final,
            MAX(end_away_score) AS away_final
        FROM lineup_stint_facts
        WHERE LEFT(game_id, 1) = '2'    -- regular season only
        GROUP BY game_id, season, home_abbr, away_abbr
        HAVING home_q3 IS NOT NULL
           AND home_final != away_final  -- drop ties
    """).df()

    df["era"] = df["season"].apply(era_label)
    df = df[df["era"].notna()].copy()
    return df


def analyze() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    print("Loading game quarter scores from DuckDB...")
    df = load_game_quarter_scores(con)
    con.close()

    print(f"Games loaded: {len(df):,}  |  Seasons: {df['season'].nunique()}")

    # Deficit at end of Q3 from home team's perspective
    # (positive = home team is trailing)
    df["deficit_q3"] = df["away_q3"] - df["home_q3"]

    # Home team win flag
    df["home_won"] = (df["home_final"] > df["away_final"]).astype(int)

    # Total pts as pace proxy
    df["total_pts"] = df["home_final"] + df["away_final"]

    # -- 1. Pace by era --------------------------------------------------------
    print(f"\n{'='*65}")
    print("1. PACE  (avg total points per game -- proxy for scoring rate)")
    print(f"{'='*65}")
    pace = (df.groupby("era")["total_pts"]
              .agg(mean="mean", std="std", n="count")
              .reindex(ERA_ORDER))
    for era, row in pace.iterrows():
        bar = "#" * int(row["mean"] / 5)
        print(f"  {era}  {row['mean']:5.1f} pts  n={int(row['n']):,}  {bar}")

    # Also show possessions from our data if available
    # (total_pts / league_avg_pts_per_poss ≈ possessions)
    # Use ~1.13 pts/poss as rough constant -- directionally fine
    print()
    for era, row in pace.iterrows():
        approx_poss = row["mean"] / 1.13
        print(f"  {era}  ~{approx_poss:.0f} possessions/game (approx)")

    # -- 2. Q3 deficit -> trailing-team win rate --------------------------------
    print(f"\n{'='*65}")
    print("2. Q3 TRAILING-TEAM WIN RATE BY DEFICIT BUCKET AND ERA")
    print(f"{'='*65}")

    trailing = df[df["deficit_q3"] > 0].copy()
    trailing["def_bucket"] = pd.cut(
        trailing["deficit_q3"],
        bins=[0, 3, 6, 9, 12, 15, 20, 60],
        labels=["1-3", "4-6", "7-9", "10-12", "13-15", "16-20", "21+"]
    )

    pivot = (trailing
             .groupby(["def_bucket", "era"])["home_won"]
             .agg(rate="mean", n="count")
             .unstack("era"))

    print(f"\n  {'Deficit':<8}", end="")
    for era in ERA_ORDER:
        print(f"  {era:>18}", end="")
    print()
    print("  " + "-" * 80)

    for bucket, row in pivot.iterrows():
        print(f"  {str(bucket):<8}", end="")
        for era in ERA_ORDER:
            try:
                rate = row[("rate", era)]
                n    = int(row[("n",    era)])
                if pd.isna(rate):
                    print(f"  {'--':>18}", end="")
                else:
                    print(f"  {rate*100:4.1f}% (n={n:,}){'':<4}", end="")
            except Exception:
                print(f"  {'--':>18}", end="")
        print()

    # -- 3. Logistic regression ------------------------------------------------
    print(f"\n{'='*65}")
    print("3. LOGISTIC REGRESSION: P(comeback) ~ deficit + era + deficitxera")
    print("   (baseline = 1997-2003,  era coded 0/1/2/3)")
    print(f"{'='*65}")

    reg = trailing[trailing["def_bucket"] != "21+"].copy()
    reg["era_code"] = reg["era"].map(
        {"1997-2003": 0, "2004-2010": 1, "2011-2017": 2, "2018-2025": 3}
    ).astype(float)

    Xm = np.column_stack([
        np.ones(len(reg)),
        reg["deficit_q3"].values,
        reg["era_code"].values,
        reg["deficit_q3"].values * reg["era_code"].values,
    ])
    y = reg["home_won"].values

    def neg_ll(b):
        p = np.clip(expit(Xm @ b), 1e-10, 1 - 1e-10)
        return -np.sum(y * np.log(p) + (1 - y) * np.log(1 - p))

    result = minimize(neg_ll, [0.5, -0.1, 0.0, 0.0], method="L-BFGS-B")
    b = result.x

    names = ["intercept", "deficit", "era_linear", "deficit x era"]
    print()
    for name, coef in zip(names, b):
        print(f"  {name:<20} {coef:+.5f}")

    print()
    sign = "POSITIVE" if b[3] > 0 else "NEGATIVE"
    if b[3] > 0:
        interp = "comebacks MORE likely in modern era -> claim SUPPORTED"
    else:
        interp = "comebacks LESS likely in modern era (leads are SAFER) -> claim REFUTED"
    print(f"  deficitxera coefficient is {sign} ({b[3]:+.5f})")
    print(f"  -> {interp}")

    # -- 4. Specific thresholds ------------------------------------------------
    print(f"\n{'='*65}")
    print("4. SPECIFIC CLAIMS")
    print(f"{'='*65}")

    print("\n  A) '13-point Q3 deficit is not that daunting'")
    print(f"  {'Era':<14} {'Win%':>6}  {'n':>5}  {'95% CI':>10}")
    print("  " + "-" * 40)
    for era in ERA_ORDER:
        sub = trailing[(trailing["def_bucket"] == "13-15") & (trailing["era"] == era)]
        if len(sub) < 10:
            continue
        wr = sub["home_won"].mean()
        n  = len(sub)
        ci = 1.96 * np.sqrt(wr * (1 - wr) / n)
        print(f"  {era:<14} {wr*100:5.1f}%  {n:>5}  ±{ci*100:.1f}pp")
        # Also compute model prediction
        era_code = ERA_ORDER.index(era)
        logit_pred = b[0] + b[1]*14 + b[2]*era_code + b[3]*14*era_code
        print(f"  {'(model pred)':<14} {expit(logit_pred)*100:5.1f}%")

    print("\n  B) '8-point Q3 lead is not that daunting'")
    print("  (home team leads by 7-9 at end of Q3 -> home team hold rate)")
    print(f"  {'Era':<14} {'Hold%':>6}  {'n':>5}  {'95% CI':>10}")
    print("  " + "-" * 40)
    leaders = df[(df["deficit_q3"] >= -9) & (df["deficit_q3"] <= -7)].copy()
    for era in ERA_ORDER:
        sub = leaders[leaders["era"] == era]
        if len(sub) < 10:
            continue
        hold = sub["home_won"].mean()
        n    = len(sub)
        ci   = 1.96 * np.sqrt(hold * (1 - hold) / n)
        print(f"  {era:<14} {hold*100:5.1f}%  {n:>5}  ±{ci*100:.1f}pp")

    # -- 5. Era trend for each deficit size ------------------------------------
    print(f"\n{'='*65}")
    print("5. TREND: has win% for trailing team changed over time?")
    print("   (season-by-season for 13-15pt Q3 deficit)")
    print(f"{'='*65}")
    by_season = (trailing[trailing["def_bucket"] == "13-15"]
                 .groupby("season")["home_won"]
                 .agg(rate="mean", n="count")
                 .reset_index()
                 .sort_values("season"))
    by_season["yr"] = by_season["season"].str[:4].astype(int)

    # Print every other season for readability
    print(f"\n  {'Season':<10} {'Win%':>6}  {'n':>4}")
    print("  " + "-" * 25)
    for _, row in by_season.iterrows():
        bar = "|" * int(row["rate"] * 20)
        print(f"  {row['season']:<10} {row['rate']*100:5.1f}%  {int(row['n']):>4}  {bar}")

    # Simple linear trend on win rate vs year
    if len(by_season) > 5:
        from scipy import stats as sci_stats
        slope, intercept, r, p, se = sci_stats.linregress(
            by_season["yr"], by_season["rate"]
        )
        direction = "increasing" if slope > 0 else "decreasing"
        print(f"\n  Linear trend: {slope*100:+.3f} pp/year ({direction}),  p={p:.3f}")
        if p < 0.05:
            print("  -> Statistically significant trend")
        else:
            print("  -> NOT statistically significant (no clear era trend)")

    print(f"\n{'='*65}\n")


if __name__ == "__main__":
    analyze()
