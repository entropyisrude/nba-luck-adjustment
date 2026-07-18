from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


ROOT = Path(r"C:\Users\Dave\Downloads\nba-onoff-publish")
DATA_DIR = ROOT / "data"

POSS_COLS = [
    "game_id",
    "poss_index",
    "offense_team",
    "defense_team",
    "off_p1",
    "off_p2",
    "off_p3",
    "off_p4",
    "off_p5",
    "def_p1",
    "def_p2",
    "def_p3",
    "def_p4",
    "def_p5",
    "points",
    "points_adj",
    "ended_by",
    "period",
    "date",
]

ONOFF_COLS = [
    "game_id",
    "team_id",
    "player_id",
    "player_name",
    "on_pts_for",
    "on_pts_against",
    "on_diff",
    "off_pts_for",
    "off_pts_against",
    "off_diff",
    "on_pts_for_adj",
    "on_pts_against_adj",
    "on_diff_adj",
    "off_pts_for_adj",
    "off_pts_against_adj",
    "off_diff_adj",
    "on_off_diff",
    "on_off_diff_adj",
    "on_diff_reconstructed",
    "off_diff_reconstructed",
    "on_off_diff_reconstructed",
    "minutes_on",
    "date",
]

STINT_COLS = [
    "game_id",
    "stint_index",
    "home_id",
    "away_id",
    "home_p1",
    "home_p2",
    "home_p3",
    "home_p4",
    "home_p5",
    "away_p1",
    "away_p2",
    "away_p3",
    "away_p4",
    "away_p5",
    "seconds",
    "home_pts",
    "away_pts",
    "home_pts_adj",
    "away_pts_adj",
    "start_elapsed",
    "end_elapsed",
    "start_period",
    "start_clock",
    "end_period",
    "end_clock",
    "start_home_score",
    "start_away_score",
    "end_home_score",
    "end_away_score",
    "start_home_score_adj",
    "start_away_score_adj",
    "end_home_score_adj",
    "end_away_score_adj",
    "date",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build hybrid historical on/off, stints, and possessions from current + v2 + rebuilt overlays.")
    parser.add_argument("--threshold", type=float, default=85.0, help="Current avg team possessions below this can trigger v2 replacement.")
    parser.add_argument("--min-delta", type=float, default=3.0, help="Minimum avg possession advantage for v2.")
    parser.add_argument("--season-start", default="1996-97")
    parser.add_argument("--season-end", default="2025-26")
    parser.add_argument("--out-tag", default="hybrid")
    return parser.parse_args()


def season_expr(date_col: str) -> str:
    return (
        f"CASE WHEN TRY_CAST({date_col} AS DATE) IS NULL THEN NULL "
        f"WHEN EXTRACT(month FROM TRY_CAST({date_col} AS DATE)) >= 10 "
        f"THEN CAST(EXTRACT(year FROM TRY_CAST({date_col} AS DATE)) AS VARCHAR) || '-' || RIGHT(CAST(EXTRACT(year FROM TRY_CAST({date_col} AS DATE)) + 1 AS VARCHAR), 2) "
        f"ELSE CAST(EXTRACT(year FROM TRY_CAST({date_col} AS DATE)) - 1 AS VARCHAR) || '-' || RIGHT(CAST(EXTRACT(year FROM TRY_CAST({date_col} AS DATE)) AS VARCHAR), 2) END"
    )


def csv_table_sql(path: Path, quote: bool = False) -> str:
    quote_clause = ", quote='\"'" if quote else ""
    return (
        f"read_csv_auto('{path}', header=true, sample_size=-1, delim=','"
        f"{quote_clause}, strict_mode=false, ignore_errors=true, null_padding=true)"
    )


def select_cols(table_name: str, cols: list[str]) -> str:
    return ", ".join(f"{table_name}.{col}" for col in cols)


def main() -> None:
    args = parse_args()
    current_onoff = DATA_DIR / "adjusted_onoff_historical_pbp.csv"
    current_stints = DATA_DIR / "stints_historical_pbp.csv"
    current_poss = DATA_DIR / "possessions_historical_pbp.csv"
    v2_onoff = DATA_DIR / "adjusted_onoff_historical_pbp_v2.csv"
    v2_stints = DATA_DIR / "stints_historical_pbp_v2.csv"
    v2_poss = DATA_DIR / "possessions_historical_pbp_v2.csv"
    rebuilt_onoff = DATA_DIR / "adjusted_onoff_historical_rebuilt.csv"
    rebuilt_stints = DATA_DIR / "stints_historical_rebuilt.csv"
    rebuilt_poss = DATA_DIR / "possessions_historical_rebuilt.csv"

    out_onoff = DATA_DIR / f"adjusted_onoff_historical_pbp_{args.out_tag}.csv"
    out_stints = DATA_DIR / f"stints_historical_pbp_{args.out_tag}.csv"
    out_poss = DATA_DIR / f"possessions_historical_pbp_{args.out_tag}.csv"
    out_map = DATA_DIR / f"hybrid_historical_source_map_{args.out_tag}.csv"

    con = duckdb.connect()

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE current_poss AS
        SELECT * FROM {csv_table_sql(current_poss)};
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE v2_poss AS
        SELECT * FROM {csv_table_sql(v2_poss)};
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE current_onoff AS
        SELECT * FROM {csv_table_sql(current_onoff, quote=True)};
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE v2_onoff AS
        SELECT * FROM {csv_table_sql(v2_onoff, quote=True)};
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE current_stints AS
        SELECT * FROM {csv_table_sql(current_stints)};
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE v2_stints AS
        SELECT * FROM {csv_table_sql(v2_stints)};
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE team_season_source AS
        WITH current_team AS (
          SELECT
            {season_expr('date')} AS season,
            CAST(offense_team AS BIGINT) AS team_id,
            COUNT(*)::DOUBLE / COUNT(DISTINCT CAST(game_id AS VARCHAR)) AS avg_team_possessions
          FROM current_poss
          GROUP BY 1,2
        ),
        v2_team AS (
          SELECT
            {season_expr('date')} AS season,
            CAST(offense_team AS BIGINT) AS team_id,
            COUNT(*)::DOUBLE / COUNT(DISTINCT CAST(game_id AS VARCHAR)) AS avg_team_possessions
          FROM v2_poss
          GROUP BY 1,2
        )
        SELECT
          COALESCE(c.season, v.season) AS season,
          COALESCE(c.team_id, v.team_id) AS team_id,
          c.avg_team_possessions AS current_avg_team_possessions,
          v.avg_team_possessions AS v2_avg_team_possessions,
          CASE
            WHEN v.avg_team_possessions IS NULL THEN 'current'
            WHEN c.avg_team_possessions IS NULL THEN 'v2'
            WHEN c.avg_team_possessions < {args.threshold} AND v.avg_team_possessions >= c.avg_team_possessions + {args.min_delta} THEN 'v2'
            ELSE 'current'
          END AS preferred_source
        FROM current_team c
        FULL OUTER JOIN v2_team v
          ON c.season = v.season AND c.team_id = v.team_id
        WHERE COALESCE(c.season, v.season) BETWEEN '{args.season_start}' AND '{args.season_end}';
        """
    )

    con.execute(
        f"""
        COPY (
          SELECT *
          FROM team_season_source
          ORDER BY season, team_id
        ) TO '{out_map}' (HEADER, DELIMITER ',');
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE hybrid_possessions_base AS
        WITH current_games AS (
          SELECT
            CAST(game_id AS VARCHAR) AS game_id,
            {season_expr('date')} AS season,
            LEAST(CAST(offense_team AS BIGINT), CAST(defense_team AS BIGINT)) AS team_a,
            GREATEST(CAST(offense_team AS BIGINT), CAST(defense_team AS BIGINT)) AS team_b
          FROM current_poss
          GROUP BY 1,2,3,4
        ),
        v2_games AS (
          SELECT
            CAST(game_id AS VARCHAR) AS game_id,
            {season_expr('date')} AS season,
            LEAST(CAST(offense_team AS BIGINT), CAST(defense_team AS BIGINT)) AS team_a,
            GREATEST(CAST(offense_team AS BIGINT), CAST(defense_team AS BIGINT)) AS team_b
          FROM v2_poss
          GROUP BY 1,2,3,4
        ),
        possession_game_source AS (
          SELECT
            g.game_id,
            g.season,
            g.team_a,
            g.team_b,
            CASE
              WHEN COALESCE(a.preferred_source, 'current') = 'v2'
                OR COALESCE(b.preferred_source, 'current') = 'v2'
                OR (v.game_id IS NOT NULL AND c.game_id IS NULL)
              THEN 'v2'
              ELSE 'current'
            END AS preferred_source
          FROM (
            SELECT * FROM current_games
            UNION
            SELECT * FROM v2_games
          ) g
          LEFT JOIN current_games c
            ON c.game_id = g.game_id
          LEFT JOIN v2_games v
            ON v.game_id = g.game_id
          LEFT JOIN team_season_source a
            ON a.season = g.season AND a.team_id = g.team_a
          LEFT JOIN team_season_source b
            ON b.season = g.season AND b.team_id = g.team_b
        ),
        current_rows AS (
          SELECT {select_cols('p', POSS_COLS)}, 2 AS base_source_priority
          FROM current_poss p
          LEFT JOIN possession_game_source g
            ON g.game_id = CAST(p.game_id AS VARCHAR)
          WHERE COALESCE(g.preferred_source, 'current') = 'current'
        ),
        v2_rows AS (
          SELECT {select_cols('p', POSS_COLS)}, 1 AS base_source_priority
          FROM v2_poss p
          JOIN possession_game_source g
            ON g.game_id = CAST(p.game_id AS VARCHAR)
          WHERE g.preferred_source = 'v2'
        ),
        combined AS (
          SELECT * FROM current_rows
          UNION ALL
          SELECT * FROM v2_rows
        )
        SELECT {", ".join(POSS_COLS)}
        FROM (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY CAST(game_id AS VARCHAR), CAST(poss_index AS BIGINT)
                   ORDER BY base_source_priority ASC
                 ) AS rn
          FROM combined
        )
        WHERE rn = 1;
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE hybrid_onoff_base AS
        WITH current_rows AS (
          SELECT {select_cols('o', ONOFF_COLS)}
          FROM current_onoff o
          LEFT JOIN team_season_source s
            ON s.season = {season_expr('o.date')}
           AND s.team_id = CAST(o.team_id AS BIGINT)
          WHERE COALESCE(s.preferred_source, 'current') = 'current'
        ),
        v2_rows AS (
          SELECT {select_cols('o', ONOFF_COLS)}
          FROM v2_onoff o
          JOIN team_season_source s
            ON s.season = {season_expr('o.date')}
           AND s.team_id = CAST(o.team_id AS BIGINT)
          WHERE s.preferred_source = 'v2'
        )
        SELECT * FROM current_rows
        UNION ALL
        SELECT * FROM v2_rows;
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE game_source AS
        WITH current_games AS (
          SELECT
            CAST(game_id AS VARCHAR) AS game_id,
            {season_expr('date')} AS season,
            CAST(home_id AS BIGINT) AS home_id,
            CAST(away_id AS BIGINT) AS away_id
          FROM current_stints
          GROUP BY 1,2,3,4
        ),
        v2_games AS (
          SELECT
            CAST(game_id AS VARCHAR) AS game_id,
            {season_expr('date')} AS season,
            CAST(home_id AS BIGINT) AS home_id,
            CAST(away_id AS BIGINT) AS away_id
          FROM v2_stints
          GROUP BY 1,2,3,4
        ),
        all_games AS (
          SELECT * FROM current_games
          UNION ALL
          SELECT * FROM v2_games
        )
        SELECT
          g.game_id,
          g.season,
          g.home_id,
          g.away_id,
          CASE
            WHEN COALESCE(home_src.preferred_source, 'current') = 'v2'
              OR COALESCE(away_src.preferred_source, 'current') = 'v2'
            THEN 'v2'
            ELSE 'current'
          END AS preferred_source
        FROM (
          SELECT *
          FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY season DESC) AS rn
            FROM all_games
          )
          WHERE rn = 1
        ) g
        LEFT JOIN team_season_source home_src
          ON home_src.season = g.season AND home_src.team_id = g.home_id
        LEFT JOIN team_season_source away_src
          ON away_src.season = g.season AND away_src.team_id = g.away_id;
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE hybrid_stints_base AS
        WITH current_rows AS (
          SELECT {select_cols('s', STINT_COLS)}
          FROM current_stints s
          LEFT JOIN game_source g
            ON g.game_id = CAST(s.game_id AS VARCHAR)
          WHERE COALESCE(g.preferred_source, 'current') = 'current'
        ),
        v2_rows AS (
          SELECT {select_cols('s', STINT_COLS)}
          FROM v2_stints s
          JOIN game_source g
            ON g.game_id = CAST(s.game_id AS VARCHAR)
          WHERE g.preferred_source = 'v2'
        )
        SELECT * FROM current_rows
        UNION ALL
        SELECT * FROM v2_rows;
        """
    )

    if rebuilt_poss.exists():
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE rebuilt_poss AS
            SELECT {", ".join(POSS_COLS)} FROM {csv_table_sql(rebuilt_poss)};
            """
        )
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE hybrid_possessions AS
            WITH combined AS (
              SELECT *, 2 AS source_priority FROM hybrid_possessions_base
              UNION ALL
              SELECT *, 1 AS source_priority FROM rebuilt_poss
            )
            SELECT {", ".join(POSS_COLS)}
            FROM (
              SELECT *, ROW_NUMBER() OVER (
                PARTITION BY CAST(game_id AS VARCHAR), CAST(poss_index AS BIGINT)
                ORDER BY source_priority ASC
              ) AS rn
              FROM combined
            )
            WHERE rn = 1;
            """
        )
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE hybrid_possessions AS SELECT * FROM hybrid_possessions_base;")

    if rebuilt_onoff.exists():
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE rebuilt_onoff AS
            SELECT {", ".join(ONOFF_COLS)} FROM {csv_table_sql(rebuilt_onoff, quote=True)};
            """
        )
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE hybrid_onoff AS
            WITH combined AS (
              SELECT *, 2 AS source_priority FROM hybrid_onoff_base
              UNION ALL
              SELECT *, 1 AS source_priority FROM rebuilt_onoff
            )
            SELECT {", ".join(ONOFF_COLS)}
            FROM (
              SELECT *, ROW_NUMBER() OVER (
                PARTITION BY CAST(game_id AS VARCHAR), CAST(player_id AS BIGINT)
                ORDER BY source_priority ASC
              ) AS rn
              FROM combined
            )
            WHERE rn = 1;
            """
        )
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE hybrid_onoff AS SELECT * FROM hybrid_onoff_base;")

    if rebuilt_stints.exists():
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE rebuilt_stints AS
            SELECT {", ".join(STINT_COLS)} FROM {csv_table_sql(rebuilt_stints)};
            """
        )
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE hybrid_stints AS
            WITH combined AS (
              SELECT *, 2 AS source_priority FROM hybrid_stints_base
              UNION ALL
              SELECT *, 1 AS source_priority FROM rebuilt_stints
            )
            SELECT {", ".join(STINT_COLS)}
            FROM (
              SELECT *, ROW_NUMBER() OVER (
                PARTITION BY CAST(game_id AS VARCHAR), CAST(stint_index AS BIGINT)
                ORDER BY source_priority ASC
              ) AS rn
              FROM combined
            )
            WHERE rn = 1;
            """
        )
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE hybrid_stints AS SELECT * FROM hybrid_stints_base;")

    con.execute(f"COPY hybrid_onoff TO '{out_onoff}' (HEADER, DELIMITER ',');")
    con.execute(f"COPY hybrid_stints TO '{out_stints}' (HEADER, DELIMITER ',');")
    con.execute(f"COPY hybrid_possessions TO '{out_poss}' (HEADER, DELIMITER ',');")

    print(out_onoff)
    print(out_stints)
    print(out_poss)
    print(out_map)


if __name__ == "__main__":
    main()
