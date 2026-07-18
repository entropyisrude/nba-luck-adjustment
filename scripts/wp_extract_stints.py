"""
Step 1: Extract win-probability training observations from lineup_stint_facts.

Each stint boundary (start + end) gives us one observation:
  (time_remaining_secs, score_diff, home_won)

where:
  time_remaining_secs = seconds left in regulation (2880 - elapsed)
  score_diff          = home_score - away_score  (positive = home leading)
  home_won            = 1 if home team won the game (including any OT)

We only use observations that occur during regulation (elapsed < 2880).
Game outcome uses the actual final score (which may be determined in OT).

Output: data/wp_obs_stints.parquet
"""
from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd

DB  = Path(__file__).parent.parent / "data" / "nba_analytics.duckdb"
OUT = Path(__file__).parent.parent / "data" / "wp_obs_stints.parquet"

REGULATION_SECS = 2880   # 48 min × 60

def era_label(yr: int) -> str:
    if yr < 2004: return "1997-2003"
    if yr < 2011: return "2004-2010"
    if yr < 2018: return "2011-2017"
    return "2018-2025"

def run():
    con = duckdb.connect(str(DB), read_only=True)

    print("Querying lineup_stint_facts ...")
    df = con.execute(f"""
        WITH finals AS (
            -- Final score per game = max scores in the last period
            SELECT
                game_id,
                MAX(end_home_score) AS final_home,
                MAX(end_away_score) AS final_away
            FROM lineup_stint_facts
            WHERE LEFT(game_id,1) = '2'    -- RS only
            GROUP BY game_id
            HAVING MAX(end_home_score) != MAX(end_away_score)  -- drop ties
        ),
        stints_reg AS (
            -- All stint endpoints in regulation time
            SELECT
                s.game_id,
                s.season,
                -- Observation at START of stint
                s.start_elapsed   AS elapsed,
                s.start_home_score - s.start_away_score AS diff,
                'start'           AS endpoint
            FROM lineup_stint_facts s
            WHERE LEFT(s.game_id,1) = '2'
              AND s.start_elapsed >= 0
              AND s.start_elapsed < {REGULATION_SECS}
              AND s.start_home_score IS NOT NULL
              AND s.start_away_score IS NOT NULL

            UNION ALL

            -- Observation at END of stint (if still in regulation)
            SELECT
                s.game_id,
                s.season,
                s.end_elapsed     AS elapsed,
                s.end_home_score - s.end_away_score AS diff,
                'end'             AS endpoint
            FROM lineup_stint_facts s
            WHERE LEFT(s.game_id,1) = '2'
              AND s.end_elapsed > 0
              AND s.end_elapsed <= {REGULATION_SECS}
              AND s.end_home_score IS NOT NULL
              AND s.end_away_score IS NOT NULL
        )
        SELECT
            r.game_id,
            r.season,
            CAST(LEFT(r.season, 4) AS INTEGER)            AS season_yr,
            {REGULATION_SECS} - r.elapsed                 AS time_remaining,
            r.diff                                         AS score_diff,
            CASE WHEN f.final_home > f.final_away
                 THEN 1 ELSE 0 END                         AS home_won,
            r.endpoint
        FROM stints_reg r
        JOIN finals f ON r.game_id = f.game_id
        WHERE {REGULATION_SECS} - r.elapsed >= 0  -- safety
        ORDER BY r.game_id, r.elapsed
    """).df()

    con.close()

    # Add era label
    df["era"] = df["season_yr"].apply(
        lambda y: era_label(y) if 1997 <= y <= 2024 else None
    )
    df = df[df["era"].notna()].copy()

    # Convert types
    df["time_remaining"] = df["time_remaining"].astype(float)
    df["score_diff"]     = df["score_diff"].astype(float)
    df["home_won"]       = df["home_won"].astype(int)
    df["source"]         = "stints"

    print(f"Total observations : {len(df):,}")
    print(f"Unique games       : {df['game_id'].nunique():,}")
    print(f"Season range       : {df['season'].min()} – {df['season'].max()}")
    print(f"Era counts:\n{df['era'].value_counts().sort_index()}")
    print(f"\nSample:\n{df.head(5).to_string()}")

    df.to_parquet(OUT, index=False)
    print(f"\nSaved to {OUT}  ({OUT.stat().st_size/1e6:.1f} MB)")

if __name__ == "__main__":
    run()
