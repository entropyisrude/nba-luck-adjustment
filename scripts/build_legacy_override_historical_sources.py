from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
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

SOURCE_FILES = {
    "current": {
        "poss": DATA_DIR / "possessions_historical_pbp.csv",
        "onoff": DATA_DIR / "adjusted_onoff_historical_pbp.csv",
        "stints": DATA_DIR / "stints_historical_pbp.csv",
    },
    "v2": {
        "poss": DATA_DIR / "possessions_historical_pbp_v2.csv",
        "onoff": DATA_DIR / "adjusted_onoff_historical_pbp_v2.csv",
        "stints": DATA_DIR / "stints_historical_pbp_v2.csv",
    },
    "plain": {
        "poss": DATA_DIR / "possessions_historical_pbp_20260317_plain.csv",
        "onoff": DATA_DIR / "adjusted_onoff_historical_pbp_20260317_plain.csv",
        "stints": DATA_DIR / "stints_historical_pbp_20260317_plain.csv",
    },
    "vwd": {
        "poss": DATA_DIR / "possessions_historical_pbp_20260317_vwd.csv",
        "onoff": DATA_DIR / "adjusted_onoff_historical_pbp_20260317_vwd.csv",
        "stints": DATA_DIR / "stints_historical_pbp_20260317_vwd.csv",
    },
    "vwd_clean": {
        "poss": DATA_DIR / "possessions_historical_pbp_vwd_clean.csv",
        "onoff": DATA_DIR / "adjusted_onoff_historical_pbp_vwd_clean.csv",
        "stints": DATA_DIR / "stints_historical_pbp_vwd_clean.csv",
    },
    "vwd_pure": {
        "poss": DATA_DIR / "possessions_historical_pbp_vwd_pure.csv",
        "onoff": DATA_DIR / "adjusted_onoff_historical_pbp_vwd_pure.csv",
        "stints": DATA_DIR / "stints_historical_pbp_vwd_pure.csv",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build tagged historical source artifacts by selecting the best local legacy source per historical team-season."
    )
    parser.add_argument(
        "--queue-csv",
        default=str(DATA_DIR / "teamseason_repair_queue_override_or_legacy_source.csv"),
        help="Queue CSV that defines the historical backlog to repair",
    )
    parser.add_argument("--out-tag", default="legacybest")
    parser.add_argument("--season-start", default="1996-97")
    parser.add_argument("--season-end", default="2025-26")
    parser.add_argument("--min-games", type=int, default=20)
    parser.add_argument("--low-threshold", type=float, default=85.0)
    parser.add_argument("--high-threshold", type=float, default=110.0)
    return parser.parse_args()


def csv_table_sql(path: Path, quote: bool = False) -> str:
    quote_clause = ", quote='\"'" if quote else ""
    return (
        f"read_csv_auto('{path}', header=true, sample_size=-1, delim=','"
        f"{quote_clause}, strict_mode=false, ignore_errors=true, null_padding=true)"
    )


def season_expr(date_col: str) -> str:
    return (
        f"CASE WHEN TRY_CAST({date_col} AS DATE) IS NULL THEN NULL "
        f"WHEN EXTRACT(month FROM TRY_CAST({date_col} AS DATE)) >= 10 "
        f"THEN CAST(EXTRACT(year FROM TRY_CAST({date_col} AS DATE)) AS VARCHAR) || '-' || RIGHT(CAST(EXTRACT(year FROM TRY_CAST({date_col} AS DATE)) + 1 AS VARCHAR), 2) "
        f"ELSE CAST(EXTRACT(year FROM TRY_CAST({date_col} AS DATE)) - 1 AS VARCHAR) || '-' || RIGHT(CAST(EXTRACT(year FROM TRY_CAST({date_col} AS DATE)) AS VARCHAR), 2) END"
    )


def select_cols(table_name: str, cols: list[str]) -> str:
    return ", ".join(f"{table_name}.{col}" for col in cols)


def main() -> None:
    args = parse_args()
    available_sources = {
        tag: paths
        for tag, paths in SOURCE_FILES.items()
        if paths["poss"].exists() and paths["onoff"].exists() and paths["stints"].exists()
    }
    if "current" not in available_sources:
        raise RuntimeError("Missing required current historical source files")

    out_map = DATA_DIR / f"legacy_override_historical_source_map_{args.out_tag}.csv"
    out_onoff = DATA_DIR / f"adjusted_onoff_historical_pbp_{args.out_tag}.csv"
    out_stints = DATA_DIR / f"stints_historical_pbp_{args.out_tag}.csv"
    out_poss = DATA_DIR / f"possessions_historical_pbp_{args.out_tag}.csv"

    con = duckdb.connect()

    candidate_metrics = []
    for tag, files in available_sources.items():
        candidate_metrics.append(
            f"""
            SELECT
              '{tag}' AS source_tag,
              {season_expr('date')} AS season,
              CAST(offense_team AS BIGINT) AS team_id,
              COUNT(DISTINCT CAST(game_id AS VARCHAR)) AS games,
              COUNT(*)::DOUBLE / COUNT(DISTINCT CAST(game_id AS VARCHAR)) AS avg_team_possessions,
              100.0 * SUM(CASE WHEN 1=1 AND 1=1 THEN CASE WHEN FALSE THEN 1 ELSE 0 END END) / COUNT(*) AS pct_under_0,
              100.0 * SUM(CASE WHEN CAST(poss_index AS BIGINT) IS NOT NULL THEN 0 ELSE 0 END) / COUNT(*) AS zero_stub
            FROM {csv_table_sql(files['poss'])}
            WHERE TRY_CAST(date AS DATE) IS NOT NULL
            GROUP BY 1,2,3
            """
        )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE candidate_poss_metrics AS
        WITH raw_union AS (
          {" UNION ALL ".join(candidate_metrics)}
        )
        SELECT
          source_tag,
          season,
          team_id,
          games,
          avg_team_possessions,
          pct_under_0,
          zero_stub
        FROM raw_union
        WHERE season BETWEEN '{args.season_start}' AND '{args.season_end}'
          AND games >= {args.min_games};
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE candidate_poss_metrics AS
        WITH per_game AS (
          {" UNION ALL ".join(
              f"""
              SELECT
                '{tag}' AS source_tag,
                {season_expr('date')} AS season,
                CAST(offense_team AS BIGINT) AS team_id,
                CAST(game_id AS VARCHAR) AS game_id,
                COUNT(*) AS team_possessions
              FROM {csv_table_sql(files['poss'])}
              WHERE TRY_CAST(date AS DATE) IS NOT NULL
              GROUP BY 1,2,3,4
              """
              for tag, files in available_sources.items()
          )}
        )
        SELECT
          source_tag,
          season,
          team_id,
          COUNT(*) AS games,
          AVG(team_possessions) AS avg_team_possessions,
          100.0 * SUM(CASE WHEN team_possessions < 80 THEN 1 ELSE 0 END) / COUNT(*) AS pct_under_80,
          100.0 * SUM(CASE WHEN team_possessions < 85 THEN 1 ELSE 0 END) / COUNT(*) AS pct_under_85,
          100.0 * SUM(CASE WHEN team_possessions > {args.high_threshold} THEN 1 ELSE 0 END) / COUNT(*) AS pct_over_high
        FROM per_game
        WHERE season BETWEEN '{args.season_start}' AND '{args.season_end}'
        GROUP BY 1,2,3
        HAVING COUNT(*) >= {args.min_games};
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE backlog AS
        SELECT
          season,
          CAST(team_id AS BIGINT) AS team_id,
          team_abbr,
          CAST(queue_rank AS BIGINT) AS queue_rank
        FROM read_csv_auto('{Path(args.queue_csv)}', header=true)
        WHERE season BETWEEN '{args.season_start}' AND '{args.season_end}';
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE legacy_source_choice AS
        WITH scored AS (
          SELECT
            b.season,
            b.team_id,
            b.team_abbr,
            b.queue_rank,
            m.source_tag,
            m.games,
            m.avg_team_possessions,
            m.pct_under_80,
            m.pct_under_85,
            m.pct_over_high,
            (
              CASE
                WHEN m.avg_team_possessions < {args.low_threshold} THEN ({args.low_threshold} - m.avg_team_possessions) * 4.0
                WHEN m.avg_team_possessions > {args.high_threshold} THEN (m.avg_team_possessions - {args.high_threshold}) * 2.0
                ELSE 0.0
              END
              + m.pct_under_80 * 0.8
              + m.pct_under_85 * 0.4
              + m.pct_over_high * 0.2
              + CASE WHEN m.source_tag = 'current' THEN 0.0 ELSE -0.5 END
            ) AS source_score
          FROM backlog b
          JOIN candidate_poss_metrics m
            ON m.season = b.season
           AND m.team_id = b.team_id
        )
        SELECT *
        FROM (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY season, team_id
                   ORDER BY source_score ASC, avg_team_possessions DESC, source_tag ASC
                 ) AS rn
          FROM scored
        )
        WHERE rn = 1;
        """
    )

    con.execute(
        f"""
        COPY (
          SELECT
            season,
            team_id,
            team_abbr,
            queue_rank,
            source_tag AS preferred_source,
            games,
            ROUND(avg_team_possessions, 3) AS avg_team_possessions,
            ROUND(pct_under_80, 3) AS pct_under_80,
            ROUND(pct_under_85, 3) AS pct_under_85,
            ROUND(pct_over_high, 3) AS pct_over_high,
            ROUND(source_score, 3) AS source_score
          FROM legacy_source_choice
          ORDER BY queue_rank, season, team_id
        ) TO '{out_map}' (HEADER, DELIMITER ',');
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE selected_game_source AS
        WITH current_games AS (
          SELECT
            CAST(game_id AS VARCHAR) AS game_id,
            {season_expr('date')} AS season,
            CAST(home_id AS BIGINT) AS home_id,
            CAST(away_id AS BIGINT) AS away_id
          FROM {csv_table_sql(available_sources['current']['stints'])}
          WHERE TRY_CAST(date AS DATE) IS NOT NULL
          GROUP BY 1,2,3,4
        ),
        home_choice AS (
          SELECT season, team_id, preferred_source, source_score
          FROM read_csv_auto('{out_map}', header=true)
        ),
        away_choice AS (
          SELECT season, team_id, preferred_source, source_score
          FROM read_csv_auto('{out_map}', header=true)
        )
        SELECT
          g.game_id,
          g.season,
          g.home_id,
          g.away_id,
          CASE
            WHEN hc.preferred_source IS NULL AND ac.preferred_source IS NULL THEN 'current'
            WHEN hc.preferred_source IS NULL THEN ac.preferred_source
            WHEN ac.preferred_source IS NULL THEN hc.preferred_source
            WHEN hc.preferred_source = ac.preferred_source THEN hc.preferred_source
            WHEN COALESCE(hc.source_score, 999999.0) <= COALESCE(ac.source_score, 999999.0) THEN hc.preferred_source
            ELSE ac.preferred_source
          END AS preferred_source
        FROM current_games g
        LEFT JOIN home_choice hc
          ON hc.season = g.season AND hc.team_id = g.home_id
        LEFT JOIN away_choice ac
          ON ac.season = g.season AND ac.team_id = g.away_id;
        """
    )

    for table_name, cols, kind, quote in [
        ("legacy_possessions", POSS_COLS, "poss", False),
        ("legacy_onoff", ONOFF_COLS, "onoff", True),
        ("legacy_stints", STINT_COLS, "stints", False),
    ]:
        selects = []
        join_col = "game_id"
        part_col = "poss_index" if kind == "poss" else ("player_id" if kind == "onoff" else "stint_index")
        for priority, (tag, files) in enumerate(available_sources.items(), start=1):
            src_path = files[kind]
            alias = "x"
            selects.append(
                f"""
                SELECT
                  {select_cols(alias, cols)},
                  {priority} AS source_priority
                FROM {csv_table_sql(src_path, quote=quote)} {alias}
                LEFT JOIN selected_game_source g
                  ON g.game_id = CAST({alias}.{join_col} AS VARCHAR)
                WHERE COALESCE(g.preferred_source, 'current') = '{tag}'
                """
            )
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {table_name} AS
            WITH combined AS (
              {" UNION ALL ".join(selects)}
            )
            SELECT {", ".join(cols)}
            FROM (
              SELECT *,
                     ROW_NUMBER() OVER (
                       PARTITION BY CAST(game_id AS VARCHAR), CAST({part_col} AS BIGINT)
                       ORDER BY source_priority ASC
                     ) AS rn
              FROM combined
            )
            WHERE rn = 1;
            """
        )

    con.execute(f"COPY legacy_onoff TO '{out_onoff}' (HEADER, DELIMITER ',');")
    con.execute(f"COPY legacy_stints TO '{out_stints}' (HEADER, DELIMITER ',');")
    con.execute(f"COPY legacy_possessions TO '{out_poss}' (HEADER, DELIMITER ',');")

    print(out_map)
    print(out_onoff)
    print(out_stints)
    print(out_poss)


if __name__ == "__main__":
    main()
