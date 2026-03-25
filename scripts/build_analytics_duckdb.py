from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path

import duckdb


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
FINAL_DB_PATH = Path(os.environ.get("NBA_ANALYTICS_DB_PATH", str(DATA_DIR / "nba_analytics.duckdb")))
BUILD_DB_PATH = Path(os.environ.get("NBA_ANALYTICS_BUILD_PATH", "/tmp/nba_analytics_build.duckdb"))

PLAYER_DAILY_BOX = DATA_DIR / "player_daily_boxscore.csv"
ADJUSTED_ONOFF = DATA_DIR / "adjusted_onoff.csv"
HIST_ADJUSTED_ONOFF = DATA_DIR / "adjusted_onoff_historical_pbp.csv"
STINTS = DATA_DIR / "stints.csv"
HIST_STINTS = DATA_DIR / "stints_historical_pbp.csv"
POSSESSIONS = DATA_DIR / "possessions.csv"
HIST_POSSESSIONS = DATA_DIR / "possessions_historical_pbp.csv"
ADJUSTED_GAMES = DATA_DIR / "adjusted_games.csv"
PLAYER_BOX_STATS = DATA_DIR / "player_boxscore_stats.csv"
HIST_PLAYER_BOX_STATS = DATA_DIR / "player_boxscore_stats_historical_cache.csv"
HIST_GAME_META = DATA_DIR / "historical_game_metadata_cache.csv"
EXT_PLAYER_BOX_STATS = DATA_DIR / "player_boxscore_stats_external_2010_2024.csv"
EXT_GAME_META = DATA_DIR / "game_metadata_external_2010_2024.csv"
KAGGLE_PLAYER_BOX_STATS = DATA_DIR / "player_boxscore_stats_kaggle_traditional.csv"
KAGGLE_GAME_META = DATA_DIR / "game_metadata_kaggle_traditional.csv"
PLAYER_GAME_CREATION_MAKES = DATA_DIR / "player_game_creation_makes.csv"
PLAYER_RIM_ASSISTS_BY_SEASON = DATA_DIR / "player_rim_assists_by_season.csv"
PLAYER_RIM_SIGNATURES = Path("/mnt/c/users/dave/player_rim_signatures.csv")
PLAYER_RIM_DEFENSE_BY_SEASON = DATA_DIR / "player_rim_defense_by_season.csv"
PLAYER_HUSTLE_BY_SEASON = DATA_DIR / "player_hustle_by_season.csv"
COMMON_PLAYER_INFO = Path("/mnt/c/users/dave/Downloads/nba-boxscore-data/kaggle-basketball/csv/common_player_info.csv")
DRAFT_HISTORY = Path("/mnt/c/users/dave/Downloads/nba-boxscore-data/kaggle-basketball/csv/draft_history.csv")
PLAYER_METADATA_OFFICIAL_RECENT = DATA_DIR / "player_metadata_official_recent.csv"


def _lineup_id_expr(prefix: str) -> str:
    cols = ", ".join(f"CAST({prefix}_p{i} AS BIGINT)" for i in range(1, 6))
    return f"array_to_string(list_sort([ {cols} ]), '-')"


def _combo_id_expr(players: list[str]) -> str:
    cols = ", ".join(f"CAST({p} AS BIGINT)" for p in players)
    return f"array_to_string(list_sort([ {cols} ]), '-')"


def main() -> None:
    BUILD_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    FINAL_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if BUILD_DB_PATH.exists():
        BUILD_DB_PATH.unlink()

    con = duckdb.connect(str(BUILD_DB_PATH))

    con.execute("PRAGMA threads=4")

    for table in [
        "raw_player_daily_boxscore",
        "raw_adjusted_onoff",
        "raw_hist_adjusted_onoff",
        "raw_stints",
        "raw_hist_stints",
        "raw_possessions",
        "raw_hist_possessions",
        "raw_adjusted_games",
        "raw_player_box_stats",
        "raw_hist_player_box_stats",
        "raw_hist_game_meta",
        "raw_ext_player_box_stats",
        "raw_ext_game_meta",
        "raw_kaggle_player_box_stats",
        "raw_kaggle_game_meta",
        "raw_player_game_creation_makes",
        "raw_player_rim_assists_by_season",
        "raw_player_rim_signatures",
        "raw_player_rim_defense_by_season",
        "raw_player_hustle_by_season",
        "raw_common_player_info",
        "raw_draft_history",
        "raw_player_metadata_official_recent",
        "player_game_facts",
        "lineup_stint_facts",
        "lineup_5man_agg",
        "combo_2man_agg",
        "combo_3man_agg",
    ]:
        con.execute(f"DROP TABLE IF EXISTS {table}")

    con.execute(
        f"""
        CREATE TABLE raw_player_daily_boxscore AS
        SELECT * FROM read_csv_auto('{PLAYER_DAILY_BOX}', header=true);
        """
    )
    con.execute(
        f"""
        CREATE TABLE raw_adjusted_onoff AS
        SELECT * FROM read_csv_auto('{ADJUSTED_ONOFF}', header=true, sample_size=-1, quote='\"');
        """
    )
    if HIST_ADJUSTED_ONOFF.exists():
        con.execute(
            f"""
            CREATE TABLE raw_hist_adjusted_onoff AS
            SELECT * FROM read_csv_auto('{HIST_ADJUSTED_ONOFF}', header=true, sample_size=-1, quote='\"');
            """
        )
    else:
        con.execute("CREATE TABLE raw_hist_adjusted_onoff AS SELECT * FROM raw_adjusted_onoff WHERE FALSE;")
    con.execute(
        f"""
        CREATE TABLE raw_stints AS
        SELECT * FROM read_csv_auto('{STINTS}', header=true);
        """
    )
    if HIST_STINTS.exists():
        con.execute(
            f"""
            CREATE TABLE raw_hist_stints AS
            SELECT * FROM read_csv_auto('{HIST_STINTS}', header=true);
            """
        )
    else:
        con.execute("CREATE TABLE raw_hist_stints AS SELECT * FROM raw_stints WHERE FALSE;")
    con.execute(
        f"""
        CREATE TABLE raw_possessions AS
        SELECT * FROM read_csv_auto('{POSSESSIONS}', header=true);
        """
    )
    if HIST_POSSESSIONS.exists():
        con.execute(
            f"""
            CREATE TABLE raw_hist_possessions AS
            SELECT * FROM read_csv_auto('{HIST_POSSESSIONS}', header=true);
            """
        )
    else:
        con.execute("CREATE TABLE raw_hist_possessions AS SELECT * FROM raw_possessions WHERE FALSE;")
    con.execute(
        f"""
        CREATE TABLE raw_adjusted_games AS
        SELECT * FROM read_csv_auto('{ADJUSTED_GAMES}', header=true);
        """
    )
    if PLAYER_BOX_STATS.exists():
        con.execute(
            f"""
            CREATE TABLE raw_player_box_stats AS
            SELECT * FROM read_csv_auto('{PLAYER_BOX_STATS}', header=true);
            """
        )
    else:
        con.execute(
            """
            CREATE TABLE raw_player_box_stats AS
            SELECT
                CAST(NULL AS DATE) AS date,
                CAST(NULL AS VARCHAR) AS game_id,
                CAST(NULL AS BIGINT) AS team_id,
                CAST(NULL AS VARCHAR) AS team_abbr,
                CAST(NULL AS BIGINT) AS player_id,
                CAST(NULL AS VARCHAR) AS player_name,
                CAST(NULL AS INTEGER) AS starter,
                CAST(NULL AS VARCHAR) AS minutes,
                CAST(NULL AS INTEGER) AS pts,
                CAST(NULL AS INTEGER) AS reb,
                CAST(NULL AS INTEGER) AS oreb,
                CAST(NULL AS INTEGER) AS dreb,
                CAST(NULL AS INTEGER) AS ast,
                CAST(NULL AS INTEGER) AS stl,
                CAST(NULL AS INTEGER) AS blk,
                CAST(NULL AS INTEGER) AS tov,
                CAST(NULL AS INTEGER) AS pf,
                CAST(NULL AS INTEGER) AS fgm,
                CAST(NULL AS INTEGER) AS fga,
                CAST(NULL AS INTEGER) AS fg3m,
                CAST(NULL AS INTEGER) AS fg3a,
                CAST(NULL AS INTEGER) AS ftm,
                CAST(NULL AS INTEGER) AS fta
            WHERE FALSE;
            """
        )
    if HIST_PLAYER_BOX_STATS.exists():
        con.execute(
            f"""
            CREATE TABLE raw_hist_player_box_stats AS
            SELECT * FROM read_csv_auto('{HIST_PLAYER_BOX_STATS}', header=true);
            """
        )
    else:
        con.execute("CREATE TABLE raw_hist_player_box_stats AS SELECT * FROM raw_player_box_stats WHERE FALSE;")
    if HIST_GAME_META.exists():
        con.execute(
            f"""
            CREATE TABLE raw_hist_game_meta AS
            SELECT * FROM read_csv_auto('{HIST_GAME_META}', header=true);
            """
        )
    else:
        con.execute("CREATE TABLE raw_hist_game_meta AS SELECT * FROM raw_adjusted_games WHERE FALSE;")
    if EXT_PLAYER_BOX_STATS.exists():
        con.execute(
            f"""
            CREATE TABLE raw_ext_player_box_stats AS
            SELECT * FROM read_csv_auto('{EXT_PLAYER_BOX_STATS}', header=true);
            """
        )
    else:
        con.execute("CREATE TABLE raw_ext_player_box_stats AS SELECT * FROM raw_player_box_stats WHERE FALSE;")
    if EXT_GAME_META.exists():
        con.execute(
            f"""
            CREATE TABLE raw_ext_game_meta AS
            SELECT * FROM read_csv_auto('{EXT_GAME_META}', header=true);
            """
        )
    else:
        con.execute("CREATE TABLE raw_ext_game_meta AS SELECT * FROM raw_adjusted_games WHERE FALSE;")
    if KAGGLE_PLAYER_BOX_STATS.exists():
        con.execute(
            f"""
            CREATE TABLE raw_kaggle_player_box_stats AS
            SELECT * FROM read_csv_auto('{KAGGLE_PLAYER_BOX_STATS}', header=true);
            """
        )
    else:
        con.execute("CREATE TABLE raw_kaggle_player_box_stats AS SELECT * FROM raw_player_box_stats WHERE FALSE;")
    if KAGGLE_GAME_META.exists():
        con.execute(
            f"""
            CREATE TABLE raw_kaggle_game_meta AS
            SELECT * FROM read_csv_auto('{KAGGLE_GAME_META}', header=true);
            """
        )
    else:
        con.execute("CREATE TABLE raw_kaggle_game_meta AS SELECT * FROM raw_adjusted_games WHERE FALSE;")
    if PLAYER_GAME_CREATION_MAKES.exists():
        con.execute(
            f"""
            CREATE TABLE raw_player_game_creation_makes AS
            SELECT * FROM read_csv_auto('{PLAYER_GAME_CREATION_MAKES}', header=true);
            """
        )
    else:
        con.execute(
            """
            CREATE TABLE raw_player_game_creation_makes AS
            SELECT
                CAST(NULL AS VARCHAR) AS game_id,
                CAST(NULL AS BIGINT) AS player_id,
                CAST(NULL AS BIGINT) AS assisted_2pm,
                CAST(NULL AS BIGINT) AS unassisted_2pm,
                CAST(NULL AS BIGINT) AS assisted_3pm,
                CAST(NULL AS BIGINT) AS unassisted_3pm,
                CAST(NULL AS BIGINT) AS assisted_fgm,
                CAST(NULL AS BIGINT) AS unassisted_fgm
            WHERE FALSE;
            """
        )
    if PLAYER_RIM_ASSISTS_BY_SEASON.exists():
        con.execute(
            f"""
            CREATE TABLE raw_player_rim_assists_by_season AS
            SELECT * FROM read_csv_auto('{PLAYER_RIM_ASSISTS_BY_SEASON}', header=true);
            """
        )
    else:
        con.execute(
            """
            CREATE TABLE raw_player_rim_assists_by_season AS
            SELECT
                CAST(NULL AS VARCHAR) AS season,
                CAST(NULL AS VARCHAR) AS season_type,
                CAST(NULL AS BIGINT) AS player_id,
                CAST(NULL AS VARCHAR) AS player_name,
                CAST(NULL AS DOUBLE) AS season_games,
                CAST(NULL AS DOUBLE) AS layup_assists_created,
                CAST(NULL AS DOUBLE) AS dunk_assists_created,
                CAST(NULL AS DOUBLE) AS other_rim_assists_created,
                CAST(NULL AS DOUBLE) AS rim_assists_strict,
                CAST(NULL AS DOUBLE) AS rim_assists_all,
                CAST(NULL AS DOUBLE) AS layup_assists_created_per_game,
                CAST(NULL AS DOUBLE) AS dunk_assists_created_per_game,
                CAST(NULL AS DOUBLE) AS other_rim_assists_created_per_game,
                CAST(NULL AS DOUBLE) AS rim_assists_strict_per_game,
                CAST(NULL AS DOUBLE) AS rim_assists_all_per_game
            WHERE FALSE;
            """
        )
    if PLAYER_RIM_SIGNATURES.exists():
        con.execute(
            f"""
            CREATE TABLE raw_player_rim_signatures AS
            SELECT * FROM read_csv_auto('{PLAYER_RIM_SIGNATURES}', header=true);
            """
        )
    else:
        con.execute(
            """
            CREATE TABLE raw_player_rim_signatures AS
            SELECT
                CAST(NULL AS BIGINT) AS player_id,
                CAST(NULL AS DOUBLE) AS rim_anchor_signature,
                CAST(NULL AS DOUBLE) AS rim_deterrence_signature
            WHERE FALSE;
            """
        )
    if PLAYER_RIM_DEFENSE_BY_SEASON.exists():
        con.execute(
            f"""
            CREATE TABLE raw_player_rim_defense_by_season AS
            SELECT * FROM read_csv_auto('{PLAYER_RIM_DEFENSE_BY_SEASON}', header=true);
            """
        )
    else:
        con.execute(
            """
            CREATE TABLE raw_player_rim_defense_by_season AS
            SELECT
                CAST(NULL AS VARCHAR) AS season,
                CAST(NULL AS BIGINT) AS player_id,
                CAST(NULL AS VARCHAR) AS player_name,
                CAST(NULL AS VARCHAR) AS team_abbr,
                CAST(NULL AS BIGINT) AS games,
                CAST(NULL AS DOUBLE) AS rim_dfga,
                CAST(NULL AS DOUBLE) AS rim_dfg_pct,
                CAST(NULL AS DOUBLE) AS rim_dfg_pct_expected,
                CAST(NULL AS DOUBLE) AS rim_dfg_pct_diff,
                CAST(NULL AS DOUBLE) AS rim_dfg_plusminus
            WHERE FALSE;
            """
        )
    if PLAYER_HUSTLE_BY_SEASON.exists():
        con.execute(
            f"""
            CREATE TABLE raw_player_hustle_by_season AS
            SELECT * FROM read_csv_auto('{PLAYER_HUSTLE_BY_SEASON}', header=true);
            """
        )
    else:
        con.execute(
            """
            CREATE TABLE raw_player_hustle_by_season AS
            SELECT
                CAST(NULL AS VARCHAR) AS season,
                CAST(NULL AS BIGINT) AS player_id,
                CAST(NULL AS DOUBLE) AS contested_shots,
                CAST(NULL AS DOUBLE) AS contested_shots_2pt,
                CAST(NULL AS DOUBLE) AS contested_shots_3pt,
                CAST(NULL AS DOUBLE) AS deflections,
                CAST(NULL AS DOUBLE) AS charges_drawn,
                CAST(NULL AS DOUBLE) AS screen_assists,
                CAST(NULL AS DOUBLE) AS screen_ast_pts,
                CAST(NULL AS DOUBLE) AS off_loose_balls_recovered,
                CAST(NULL AS DOUBLE) AS def_loose_balls_recovered,
                CAST(NULL AS DOUBLE) AS loose_balls_recovered,
                CAST(NULL AS DOUBLE) AS off_boxouts,
                CAST(NULL AS DOUBLE) AS def_boxouts,
                CAST(NULL AS DOUBLE) AS box_outs
            WHERE FALSE;
            """
        )
    if COMMON_PLAYER_INFO.exists():
        con.execute(
            f"""
            CREATE TABLE raw_common_player_info AS
            SELECT * FROM read_csv_auto('{COMMON_PLAYER_INFO}', header=true, sample_size=-1);
            """
        )
    else:
        con.execute(
            """
            CREATE TABLE raw_common_player_info AS
            SELECT
                CAST(NULL AS BIGINT) AS person_id,
                CAST(NULL AS DATE) AS birthdate,
                CAST(NULL AS VARCHAR) AS height,
                CAST(NULL AS VARCHAR) AS from_year,
                CAST(NULL AS VARCHAR) AS draft_year,
                CAST(NULL AS VARCHAR) AS draft_round,
                CAST(NULL AS VARCHAR) AS draft_number
            WHERE FALSE;
            """
        )
    if DRAFT_HISTORY.exists():
        con.execute(
            f"""
            CREATE TABLE raw_draft_history AS
            SELECT * FROM read_csv_auto('{DRAFT_HISTORY}', header=true, sample_size=-1);
            """
        )
    else:
        con.execute(
            """
            CREATE TABLE raw_draft_history AS
            SELECT
                CAST(NULL AS BIGINT) AS person_id,
                CAST(NULL AS VARCHAR) AS season,
                CAST(NULL AS VARCHAR) AS overall_pick
            WHERE FALSE;
            """
        )
    if PLAYER_METADATA_OFFICIAL_RECENT.exists():
        con.execute(
            f"""
            CREATE TABLE raw_player_metadata_official_recent AS
            SELECT * FROM read_csv_auto('{PLAYER_METADATA_OFFICIAL_RECENT}', header=true, sample_size=-1);
            """
        )
    else:
        con.execute(
            """
            CREATE TABLE raw_player_metadata_official_recent AS
            SELECT
                CAST(NULL AS BIGINT) AS player_id,
                CAST(NULL AS DATE) AS birthdate,
                CAST(NULL AS VARCHAR) AS listed_height,
                CAST(NULL AS INTEGER) AS height_inches,
                CAST(NULL AS INTEGER) AS from_year,
                CAST(NULL AS INTEGER) AS draft_year,
                CAST(NULL AS INTEGER) AS draft_round,
                CAST(NULL AS INTEGER) AS draft_number,
                CAST(NULL AS INTEGER) AS draft_overall_pick
            WHERE FALSE;
            """
        )
    con.execute(
        """
        CREATE TABLE player_game_facts AS
        WITH game_team_raw AS (
            SELECT
                CAST(date AS DATE) AS date,
                CAST(game_id AS VARCHAR) AS game_id,
                home_team,
                away_team,
                home_pts_actual,
                away_pts_actual,
                home_pts_adj,
                away_pts_adj
            FROM raw_adjusted_games
            UNION ALL
            SELECT
                CAST(date AS DATE) AS date,
                CAST(game_id AS VARCHAR) AS game_id,
                CAST(home_team AS VARCHAR) AS home_team,
                CAST(away_team AS VARCHAR) AS away_team,
                CAST(home_pts_actual AS DOUBLE) AS home_pts_actual,
                CAST(away_pts_actual AS DOUBLE) AS away_pts_actual,
                CAST(NULL AS DOUBLE) AS home_pts_adj,
                CAST(NULL AS DOUBLE) AS away_pts_adj
            FROM raw_hist_game_meta
            UNION ALL
            SELECT
                CAST(date AS DATE) AS date,
                CAST(game_id AS VARCHAR) AS game_id,
                CAST(home_team AS VARCHAR) AS home_team,
                CAST(away_team AS VARCHAR) AS away_team,
                CAST(home_pts_actual AS DOUBLE) AS home_pts_actual,
                CAST(away_pts_actual AS DOUBLE) AS away_pts_actual,
                CAST(NULL AS DOUBLE) AS home_pts_adj,
                CAST(NULL AS DOUBLE) AS away_pts_adj
            FROM raw_ext_game_meta
            UNION ALL
            SELECT
                CAST(date AS DATE) AS date,
                CAST(game_id AS VARCHAR) AS game_id,
                CAST(home_team AS VARCHAR) AS home_team,
                CAST(away_team AS VARCHAR) AS away_team,
                CAST(home_pts_actual AS DOUBLE) AS home_pts_actual,
                CAST(away_pts_actual AS DOUBLE) AS away_pts_actual,
                CAST(NULL AS DOUBLE) AS home_pts_adj,
                CAST(NULL AS DOUBLE) AS away_pts_adj
            FROM raw_kaggle_game_meta
        ),
        game_team AS (
            SELECT date, game_id, home_team, away_team, home_pts_actual, away_pts_actual, home_pts_adj, away_pts_adj
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY game_id
                           ORDER BY
                               CASE WHEN home_pts_adj IS NOT NULL OR away_pts_adj IS NOT NULL THEN 1 ELSE 2 END,
                               date DESC
                       ) AS rn
                FROM game_team_raw
            )
            WHERE rn = 1
        ),
        all_possessions AS (
            SELECT DISTINCT
                CAST(game_id AS VARCHAR) AS game_id,
                CAST(poss_index AS BIGINT) AS poss_index,
                CAST(date AS DATE) AS date,
                CAST(off_p1 AS BIGINT) AS off_p1,
                CAST(off_p2 AS BIGINT) AS off_p2,
                CAST(off_p3 AS BIGINT) AS off_p3,
                CAST(off_p4 AS BIGINT) AS off_p4,
                CAST(off_p5 AS BIGINT) AS off_p5,
                CAST(def_p1 AS BIGINT) AS def_p1,
                CAST(def_p2 AS BIGINT) AS def_p2,
                CAST(def_p3 AS BIGINT) AS def_p3,
                CAST(def_p4 AS BIGINT) AS def_p4,
                CAST(def_p5 AS BIGINT) AS def_p5
            FROM (
                SELECT game_id, poss_index, date, off_p1, off_p2, off_p3, off_p4, off_p5, def_p1, def_p2, def_p3, def_p4, def_p5 FROM raw_possessions
                UNION ALL
                SELECT game_id, poss_index, date, off_p1, off_p2, off_p3, off_p4, off_p5, def_p1, def_p2, def_p3, def_p4, def_p5 FROM raw_hist_possessions
            )
        ),
        player_possession_totals AS (
            SELECT date, game_id, CAST(player_id AS BIGINT) AS player_id, COUNT(*) AS on_possessions
            FROM (
                SELECT date, game_id, off_p1 AS player_id FROM all_possessions
                UNION ALL SELECT date, game_id, off_p2 AS player_id FROM all_possessions
                UNION ALL SELECT date, game_id, off_p3 AS player_id FROM all_possessions
                UNION ALL SELECT date, game_id, off_p4 AS player_id FROM all_possessions
                UNION ALL SELECT date, game_id, off_p5 AS player_id FROM all_possessions
            )
            WHERE player_id IS NOT NULL AND player_id > 0
            GROUP BY 1,2,3
        ),
        player_metadata AS (
            WITH draft_pick AS (
                SELECT
                    CAST(person_id AS BIGINT) AS player_id,
                    MIN(TRY_CAST(overall_pick AS INTEGER)) AS draft_overall_pick
                FROM raw_draft_history
                WHERE TRY_CAST(person_id AS BIGINT) IS NOT NULL
                GROUP BY 1
            )
            , local_player_metadata AS (
                SELECT
                    CAST(c.person_id AS BIGINT) AS player_id,
                    TRY_CAST(substr(CAST(c.birthdate AS VARCHAR), 1, 10) AS DATE) AS birthdate,
                    CAST(c.height AS VARCHAR) AS listed_height,
                    CASE
                        WHEN strpos(CAST(c.height AS VARCHAR), '-') > 0
                             AND TRY_CAST(split_part(CAST(c.height AS VARCHAR), '-', 1) AS INTEGER) IS NOT NULL
                             AND TRY_CAST(split_part(CAST(c.height AS VARCHAR), '-', 2) AS INTEGER) IS NOT NULL
                        THEN TRY_CAST(split_part(CAST(c.height AS VARCHAR), '-', 1) AS INTEGER) * 12
                           + TRY_CAST(split_part(CAST(c.height AS VARCHAR), '-', 2) AS INTEGER)
                        ELSE NULL
                    END AS height_inches,
                    COALESCE(
                        TRY_CAST(TRY_CAST(c.from_year AS DOUBLE) AS INTEGER),
                        TRY_CAST(c.from_year AS INTEGER),
                        TRY_CAST(c.draft_year AS INTEGER)
                    ) AS from_year,
                    TRY_CAST(c.draft_year AS INTEGER) AS draft_year,
                    TRY_CAST(c.draft_round AS INTEGER) AS draft_round,
                    TRY_CAST(c.draft_number AS INTEGER) AS draft_number,
                    dp.draft_overall_pick,
                    2 AS source_priority
                FROM raw_common_player_info c
                LEFT JOIN draft_pick dp
                  ON CAST(c.person_id AS BIGINT) = dp.player_id
                WHERE TRY_CAST(c.person_id AS BIGINT) IS NOT NULL
            ),
            combined_metadata AS (
                SELECT
                    CAST(player_id AS BIGINT) AS player_id,
                    TRY_CAST(substr(CAST(birthdate AS VARCHAR), 1, 10) AS DATE) AS birthdate,
                    CAST(listed_height AS VARCHAR) AS listed_height,
                    TRY_CAST(height_inches AS INTEGER) AS height_inches,
                    TRY_CAST(from_year AS INTEGER) AS from_year,
                    TRY_CAST(draft_year AS INTEGER) AS draft_year,
                    TRY_CAST(draft_round AS INTEGER) AS draft_round,
                    TRY_CAST(draft_number AS INTEGER) AS draft_number,
                    TRY_CAST(COALESCE(draft_overall_pick, draft_number) AS INTEGER) AS draft_overall_pick,
                    1 AS source_priority
                FROM raw_player_metadata_official_recent
                UNION ALL
                SELECT
                    player_id, birthdate, listed_height, height_inches, from_year, draft_year, draft_round, draft_number, draft_overall_pick, source_priority
                FROM local_player_metadata
            )
            SELECT
                player_id,
                COALESCE(
                    MAX(CASE WHEN source_priority = 1 THEN birthdate END),
                    MAX(CASE WHEN source_priority = 2 THEN birthdate END)
                ) AS birthdate,
                COALESCE(
                    MAX(CASE WHEN source_priority = 1 THEN listed_height END),
                    MAX(CASE WHEN source_priority = 2 THEN listed_height END)
                ) AS listed_height,
                COALESCE(
                    MAX(CASE WHEN source_priority = 1 THEN height_inches END),
                    MAX(CASE WHEN source_priority = 2 THEN height_inches END)
                ) AS height_inches,
                COALESCE(
                    MAX(CASE WHEN source_priority = 1 THEN from_year END),
                    MAX(CASE WHEN source_priority = 2 THEN from_year END)
                ) AS from_year,
                COALESCE(
                    MAX(CASE WHEN source_priority = 1 THEN draft_year END),
                    MAX(CASE WHEN source_priority = 2 THEN draft_year END)
                ) AS draft_year,
                COALESCE(
                    MAX(CASE WHEN source_priority = 1 THEN draft_round END),
                    MAX(CASE WHEN source_priority = 2 THEN draft_round END)
                ) AS draft_round,
                COALESCE(
                    MAX(CASE WHEN source_priority = 1 THEN draft_number END),
                    MAX(CASE WHEN source_priority = 2 THEN draft_number END)
                ) AS draft_number,
                COALESCE(
                    MAX(CASE WHEN source_priority = 1 THEN draft_overall_pick END),
                    MAX(CASE WHEN source_priority = 2 THEN draft_overall_pick END)
                ) AS draft_overall_pick
            FROM combined_metadata
            GROUP BY 1
        ),
        player_base AS (
            WITH current_games AS (
                SELECT DISTINCT CAST(game_id AS VARCHAR) AS game_id
                FROM raw_player_daily_boxscore
            )
            SELECT
                CAST(b.date AS DATE) AS date,
                CASE
                    WHEN EXTRACT(month FROM CAST(b.date AS DATE)) >= 10
                        THEN CAST(EXTRACT(year FROM CAST(b.date AS DATE)) AS VARCHAR) || '-' ||
                             right(CAST(EXTRACT(year FROM CAST(b.date AS DATE)) + 1 AS VARCHAR), 2)
                    ELSE CAST(EXTRACT(year FROM CAST(b.date AS DATE)) - 1 AS VARCHAR) || '-' ||
                         right(CAST(EXTRACT(year FROM CAST(b.date AS DATE)) AS VARCHAR), 2)
                END AS season,
                CAST(b.game_id AS VARCHAR) AS game_id,
                CAST(b.player_id AS BIGINT) AS player_id,
                CAST(b.player_name AS VARCHAR) AS player_name,
                CAST(b.team_id AS BIGINT) AS team_id,
                CAST(b.minutes_on AS DOUBLE) AS minutes,
                CAST(b.plus_minus_actual AS DOUBLE) AS plus_minus_actual,
                CAST(b.plus_minus_adjusted AS DOUBLE) AS plus_minus_adjusted,
                CAST(b.plus_minus_delta AS DOUBLE) AS plus_minus_delta,
                CAST(b.on_off_actual AS DOUBLE) AS on_off_actual,
                CAST(b.on_off_adjusted AS DOUBLE) AS on_off_adjusted,
                CAST(b.on_off_delta AS DOUBLE) AS on_off_delta,
                1 AS source_priority
            FROM raw_player_daily_boxscore b
            UNION ALL
            SELECT
                CAST(b.date AS DATE) AS date,
                CASE
                    WHEN EXTRACT(month FROM CAST(b.date AS DATE)) >= 10
                        THEN CAST(EXTRACT(year FROM CAST(b.date AS DATE)) AS VARCHAR) || '-' ||
                             right(CAST(EXTRACT(year FROM CAST(b.date AS DATE)) + 1 AS VARCHAR), 2)
                    ELSE CAST(EXTRACT(year FROM CAST(b.date AS DATE)) - 1 AS VARCHAR) || '-' ||
                         right(CAST(EXTRACT(year FROM CAST(b.date AS DATE)) AS VARCHAR), 2)
                END AS season,
                CAST(b.game_id AS VARCHAR) AS game_id,
                CAST(b.player_id AS BIGINT) AS player_id,
                CAST(b.player_name AS VARCHAR) AS player_name,
                CAST(b.team_id AS BIGINT) AS team_id,
                CAST(b.minutes_on AS DOUBLE) AS minutes,
                CAST(b.on_diff_reconstructed AS DOUBLE) AS plus_minus_actual,
                CAST(b.on_diff_adj AS DOUBLE) AS plus_minus_adjusted,
                CAST(b.on_diff_adj AS DOUBLE) - CAST(b.on_diff_reconstructed AS DOUBLE) AS plus_minus_delta,
                CAST(b.on_off_diff_reconstructed AS DOUBLE) AS on_off_actual,
                CAST(b.on_off_diff_adj AS DOUBLE) AS on_off_adjusted,
                CAST(b.on_off_diff_adj AS DOUBLE) - CAST(b.on_off_diff_reconstructed AS DOUBLE) AS on_off_delta,
                2 AS source_priority
            FROM raw_hist_adjusted_onoff b
            WHERE CAST(b.game_id AS VARCHAR) NOT IN (SELECT game_id FROM current_games)
        ),
        enriched AS (
            SELECT
                p.*,
                CAST(o.on_pts_for AS DOUBLE) AS on_pts_for,
                CAST(o.on_pts_against AS DOUBLE) AS on_pts_against,
                CAST(o.off_pts_for AS DOUBLE) AS off_pts_for,
                CAST(o.off_pts_against AS DOUBLE) AS off_pts_against,
                CAST(o.on_diff_reconstructed AS DOUBLE) AS on_diff_reconstructed,
                CAST(o.on_off_diff_reconstructed AS DOUBLE) AS on_off_diff_reconstructed
            FROM player_base p
            LEFT JOIN (
                SELECT * FROM raw_adjusted_onoff
                UNION ALL
                SELECT * FROM raw_hist_adjusted_onoff
            ) o
              ON p.game_id = CAST(o.game_id AS VARCHAR)
             AND p.player_id = CAST(o.player_id AS BIGINT)
        ),
        joined AS (
            SELECT
                e.*,
                bs.team_abbr AS bs_team_abbr,
                bs.player_name AS bs_player_name,
                bs.starter AS bs_starter,
                bs.pts, bs.reb, bs.oreb, bs.dreb, bs.ast, bs.stl, bs.blk, bs.tov, bs.pf,
                bs.fgm, bs.fga, bs.fg3m, bs.fg3a, bs.ftm, bs.fta,
                CAST(hbs.team_abbr AS VARCHAR) AS hbs_team_abbr,
                CAST(hbs.player_name AS VARCHAR) AS hbs_player_name,
                CAST(hbs.starter AS BIGINT) AS hbs_starter,
                CAST(hbs.pts AS BIGINT) AS hbs_pts, CAST(hbs.reb AS BIGINT) AS hbs_reb, CAST(hbs.oreb AS BIGINT) AS hbs_oreb, CAST(hbs.dreb AS BIGINT) AS hbs_dreb,
                CAST(hbs.ast AS BIGINT) AS hbs_ast, CAST(hbs.stl AS BIGINT) AS hbs_stl, CAST(hbs.blk AS BIGINT) AS hbs_blk, CAST(hbs.tov AS BIGINT) AS hbs_tov, CAST(hbs.pf AS BIGINT) AS hbs_pf,
                CAST(hbs.fgm AS BIGINT) AS hbs_fgm, CAST(hbs.fga AS BIGINT) AS hbs_fga, CAST(hbs.fg3m AS BIGINT) AS hbs_fg3m, CAST(hbs.fg3a AS BIGINT) AS hbs_fg3a, CAST(hbs.ftm AS BIGINT) AS hbs_ftm, CAST(hbs.fta AS BIGINT) AS hbs_fta,
                CAST(ebs.team_abbr AS VARCHAR) AS ebs_team_abbr,
                CAST(ebs.player_name AS VARCHAR) AS ebs_player_name,
                CAST(ebs.starter AS BIGINT) AS ebs_starter,
                CAST(ebs.pts AS BIGINT) AS ebs_pts, CAST(ebs.reb AS BIGINT) AS ebs_reb, CAST(ebs.oreb AS BIGINT) AS ebs_oreb, CAST(ebs.dreb AS BIGINT) AS ebs_dreb,
                CAST(ebs.ast AS BIGINT) AS ebs_ast, CAST(ebs.stl AS BIGINT) AS ebs_stl, CAST(ebs.blk AS BIGINT) AS ebs_blk, CAST(ebs.tov AS BIGINT) AS ebs_tov, CAST(ebs.pf AS BIGINT) AS ebs_pf,
                CAST(ebs.fgm AS BIGINT) AS ebs_fgm, CAST(ebs.fga AS BIGINT) AS ebs_fga, CAST(ebs.fg3m AS BIGINT) AS ebs_fg3m, CAST(ebs.fg3a AS BIGINT) AS ebs_fg3a, CAST(ebs.ftm AS BIGINT) AS ebs_ftm, CAST(ebs.fta AS BIGINT) AS ebs_fta,
                CAST(kbs.team_abbr AS VARCHAR) AS kbs_team_abbr,
                CAST(kbs.player_name AS VARCHAR) AS kbs_player_name,
                CAST(kbs.starter AS BIGINT) AS kbs_starter,
                CAST(kbs.pts AS BIGINT) AS kbs_pts, CAST(kbs.reb AS BIGINT) AS kbs_reb, CAST(kbs.oreb AS BIGINT) AS kbs_oreb, CAST(kbs.dreb AS BIGINT) AS kbs_dreb,
                CAST(kbs.ast AS BIGINT) AS kbs_ast, CAST(kbs.stl AS BIGINT) AS kbs_stl, CAST(kbs.blk AS BIGINT) AS kbs_blk, CAST(kbs.tov AS BIGINT) AS kbs_tov, CAST(kbs.pf AS BIGINT) AS kbs_pf,
                CAST(kbs.fgm AS BIGINT) AS kbs_fgm, CAST(kbs.fga AS BIGINT) AS kbs_fga, CAST(kbs.fg3m AS BIGINT) AS kbs_fg3m, CAST(kbs.fg3a AS BIGINT) AS kbs_fg3a, CAST(kbs.ftm AS BIGINT) AS kbs_ftm, CAST(kbs.fta AS BIGINT) AS kbs_fta,
                CAST(cm.assisted_2pm AS BIGINT) AS cm_assisted_2pm,
                CAST(cm.unassisted_2pm AS BIGINT) AS cm_unassisted_2pm,
                CAST(cm.assisted_3pm AS BIGINT) AS cm_assisted_3pm,
                CAST(cm.unassisted_3pm AS BIGINT) AS cm_unassisted_3pm,
                CAST(cm.assisted_fgm AS BIGINT) AS cm_assisted_fgm,
                CAST(cm.unassisted_fgm AS BIGINT) AS cm_unassisted_fgm,
                CAST(ra.layup_assists_created AS DOUBLE) AS ra_layup_assists_created,
                CAST(ra.dunk_assists_created AS DOUBLE) AS ra_dunk_assists_created,
                CAST(ra.other_rim_assists_created AS DOUBLE) AS ra_other_rim_assists_created,
                CAST(ra.rim_assists_strict AS DOUBLE) AS ra_rim_assists_strict,
                CAST(ra.rim_assists_all AS DOUBLE) AS ra_rim_assists_all,
                CAST(ra.season_games AS DOUBLE) AS ra_season_games,
                CAST(ra.layup_assists_created_per_game AS DOUBLE) AS ra_layup_assists_created_per_game,
                CAST(ra.dunk_assists_created_per_game AS DOUBLE) AS ra_dunk_assists_created_per_game,
                CAST(ra.other_rim_assists_created_per_game AS DOUBLE) AS ra_other_rim_assists_created_per_game,
                CAST(ra.rim_assists_strict_per_game AS DOUBLE) AS ra_rim_assists_strict_per_game,
                CAST(ra.rim_assists_all_per_game AS DOUBLE) AS ra_rim_assists_all_per_game,
                CAST(rs.rim_anchor_signature AS DOUBLE) AS rs_rim_anchor_signature,
                CAST(rs.rim_deterrence_signature AS DOUBLE) AS rs_rim_deterrence_signature,
                CAST(rd.rim_dfga AS DOUBLE) AS rd_rim_dfga,
                CAST(rd.games AS DOUBLE) AS rd_rim_tracking_games,
                CAST(rd.rim_dfg_pct AS DOUBLE) AS rd_rim_dfg_pct,
                CAST(rd.rim_dfg_pct_expected AS DOUBLE) AS rd_rim_dfg_pct_expected,
                CAST(rd.rim_dfg_pct_diff AS DOUBLE) AS rd_rim_dfg_pct_diff,
                CAST(rd.rim_dfg_plusminus AS DOUBLE) AS rd_rim_dfg_plusminus,
                CAST(hs.CONTESTED_SHOTS AS DOUBLE) AS hs_contested_shots,
                CAST(hs.CONTESTED_SHOTS_2PT AS DOUBLE) AS hs_contested_shots_2pt,
                CAST(hs.CONTESTED_SHOTS_3PT AS DOUBLE) AS hs_contested_shots_3pt,
                CAST(hs.DEFLECTIONS AS DOUBLE) AS hs_deflections,
                CAST(hs.CHARGES_DRAWN AS DOUBLE) AS hs_charges_drawn,
                CAST(hs.SCREEN_ASSISTS AS DOUBLE) AS hs_screen_assists,
                CAST(hs.SCREEN_AST_PTS AS DOUBLE) AS hs_screen_ast_pts,
                CAST(hs.OFF_LOOSE_BALLS_RECOVERED AS DOUBLE) AS hs_off_loose_balls_recovered,
                CAST(hs.DEF_LOOSE_BALLS_RECOVERED AS DOUBLE) AS hs_def_loose_balls_recovered,
                CAST(hs.LOOSE_BALLS_RECOVERED AS DOUBLE) AS hs_loose_balls_recovered,
                CAST(hs.OFF_BOXOUTS AS DOUBLE) AS hs_off_boxouts,
                CAST(hs.DEF_BOXOUTS AS DOUBLE) AS hs_def_boxouts,
                CAST(hs.BOX_OUTS AS DOUBLE) AS hs_box_outs,
                pm.listed_height AS pm_listed_height,
                pm.height_inches AS pm_height_inches,
                pm.birthdate AS pm_birthdate,
                pm.from_year AS pm_from_year,
                pm.draft_year AS pm_draft_year,
                pm.draft_round AS pm_draft_round,
                pm.draft_number AS pm_draft_number,
                pm.draft_overall_pick AS pm_draft_overall_pick,
                pp.on_possessions AS pp_on_possessions,
                g.home_team, g.away_team, g.home_pts_actual, g.away_pts_actual, g.home_pts_adj, g.away_pts_adj
            FROM enriched e
            LEFT JOIN raw_player_box_stats bs
              ON e.game_id = CAST(bs.game_id AS VARCHAR)
             AND e.player_id = CAST(bs.player_id AS BIGINT)
            LEFT JOIN raw_hist_player_box_stats hbs
              ON e.game_id = CAST(hbs.game_id AS VARCHAR)
             AND e.player_id = CAST(hbs.player_id AS BIGINT)
            LEFT JOIN raw_ext_player_box_stats ebs
              ON e.game_id = CAST(ebs.game_id AS VARCHAR)
             AND e.player_id = CAST(ebs.player_id AS BIGINT)
            LEFT JOIN raw_kaggle_player_box_stats kbs
              ON e.game_id = CAST(kbs.game_id AS VARCHAR)
             AND e.player_id = CAST(kbs.player_id AS BIGINT)
            LEFT JOIN raw_player_game_creation_makes cm
              ON e.game_id = CAST(cm.game_id AS VARCHAR)
             AND e.player_id = CAST(cm.player_id AS BIGINT)
            LEFT JOIN raw_player_rim_assists_by_season ra
              ON e.season = CAST(ra.season AS VARCHAR)
             AND CAST(ra.season_type AS VARCHAR) = 'Regular Season'
             AND e.player_id = CAST(ra.player_id AS BIGINT)
            LEFT JOIN raw_player_rim_signatures rs
              ON e.player_id = CAST(rs.player_id AS BIGINT)
            LEFT JOIN raw_player_rim_defense_by_season rd
              ON e.season = CAST(rd.season AS VARCHAR)
             AND e.player_id = CAST(rd.player_id AS BIGINT)
            LEFT JOIN raw_player_hustle_by_season hs
              ON e.season = CAST(hs.SEASON AS VARCHAR)
             AND e.player_id = CAST(hs.PLAYER_ID AS BIGINT)
            LEFT JOIN player_metadata pm
              ON e.player_id = pm.player_id
            LEFT JOIN player_possession_totals pp
              ON e.game_id = pp.game_id
             AND e.player_id = pp.player_id
            LEFT JOIN game_team g
              ON e.game_id = g.game_id
        ),
        normalized AS (
            SELECT
                date,
                season,
                game_id,
                player_id,
                COALESCE(CAST(bs_player_name AS VARCHAR), CAST(ebs_player_name AS VARCHAR), CAST(kbs_player_name AS VARCHAR), CAST(hbs_player_name AS VARCHAR), player_name) AS player_name,
                team_id,
                COALESCE(
                    CAST(bs_team_abbr AS VARCHAR),
                    CAST(ebs_team_abbr AS VARCHAR),
                    CAST(kbs_team_abbr AS VARCHAR),
                    CAST(hbs_team_abbr AS VARCHAR),
                    CASE
                        WHEN home_team IS NOT NULL AND bs_team_abbr IS NULL THEN
                            CASE WHEN team_id = (
                                CASE home_team
                                    WHEN 'ATL' THEN 1610612737 WHEN 'BOS' THEN 1610612738 WHEN 'BKN' THEN 1610612751
                                    WHEN 'CHA' THEN 1610612766 WHEN 'CHI' THEN 1610612741 WHEN 'CLE' THEN 1610612739
                                    WHEN 'DAL' THEN 1610612742 WHEN 'DEN' THEN 1610612743 WHEN 'DET' THEN 1610612765
                                    WHEN 'GSW' THEN 1610612744 WHEN 'HOU' THEN 1610612745 WHEN 'IND' THEN 1610612754
                                    WHEN 'LAC' THEN 1610612746 WHEN 'LAL' THEN 1610612747 WHEN 'MEM' THEN 1610612763
                                    WHEN 'MIA' THEN 1610612748 WHEN 'MIL' THEN 1610612749 WHEN 'MIN' THEN 1610612750
                                    WHEN 'NOP' THEN 1610612740 WHEN 'NYK' THEN 1610612752 WHEN 'OKC' THEN 1610612760
                                    WHEN 'ORL' THEN 1610612753 WHEN 'PHI' THEN 1610612755 WHEN 'PHX' THEN 1610612756
                                    WHEN 'POR' THEN 1610612757 WHEN 'SAC' THEN 1610612758 WHEN 'SAS' THEN 1610612759
                                    WHEN 'TOR' THEN 1610612761 WHEN 'UTA' THEN 1610612762 WHEN 'WAS' THEN 1610612764
                                    ELSE NULL END
                            ) THEN home_team ELSE away_team END
                        ELSE NULL
                    END
                ) AS team_abbr,
                home_team,
                away_team,
                home_pts_actual,
                away_pts_actual,
                home_pts_adj,
                away_pts_adj,
                CAST(COALESCE(bs_starter, ebs_starter, kbs_starter, hbs_starter) AS BOOLEAN) AS starter,
                minutes,
                CAST(COALESCE(pts, ebs_pts, kbs_pts, hbs_pts) AS INTEGER) AS pts,
                CAST(COALESCE(reb, ebs_reb, kbs_reb, hbs_reb) AS INTEGER) AS reb,
                CAST(COALESCE(oreb, ebs_oreb, kbs_oreb, hbs_oreb) AS INTEGER) AS oreb,
                CAST(COALESCE(dreb, ebs_dreb, kbs_dreb, hbs_dreb) AS INTEGER) AS dreb,
                CAST(COALESCE(ast, ebs_ast, kbs_ast, hbs_ast) AS INTEGER) AS ast,
                CAST(COALESCE(stl, ebs_stl, kbs_stl, hbs_stl) AS INTEGER) AS stl,
                CAST(COALESCE(blk, ebs_blk, kbs_blk, hbs_blk) AS INTEGER) AS blk,
                CAST(COALESCE(tov, ebs_tov, kbs_tov, hbs_tov) AS INTEGER) AS tov,
                CAST(COALESCE(pf, ebs_pf, kbs_pf, hbs_pf) AS INTEGER) AS pf,
                CAST(COALESCE(fgm, ebs_fgm, kbs_fgm, hbs_fgm) AS INTEGER) AS fgm,
                CAST(COALESCE(fga, ebs_fga, kbs_fga, hbs_fga) AS INTEGER) AS fga,
                CAST(COALESCE(fg3m, ebs_fg3m, kbs_fg3m, hbs_fg3m) AS INTEGER) AS fg3m,
                CAST(COALESCE(fg3a, ebs_fg3a, kbs_fg3a, hbs_fg3a) AS INTEGER) AS fg3a,
                CAST(COALESCE(ftm, ebs_ftm, kbs_ftm, hbs_ftm) AS INTEGER) AS ftm,
                CAST(COALESCE(fta, ebs_fta, kbs_fta, hbs_fta) AS INTEGER) AS fta,
                CAST(COALESCE(cm_assisted_2pm, 0) AS INTEGER) AS assisted_2pm,
                CAST(COALESCE(cm_unassisted_2pm, 0) AS INTEGER) AS unassisted_2pm,
                CAST(COALESCE(cm_assisted_3pm, 0) AS INTEGER) AS assisted_3pm,
                CAST(COALESCE(cm_unassisted_3pm, 0) AS INTEGER) AS unassisted_3pm,
                CAST(COALESCE(cm_assisted_fgm, 0) AS INTEGER) AS assisted_fgm,
                CAST(COALESCE(cm_unassisted_fgm, 0) AS INTEGER) AS unassisted_fgm,
                CAST(ra_layup_assists_created AS DOUBLE) AS layup_assists_created,
                CAST(ra_dunk_assists_created AS DOUBLE) AS dunk_assists_created,
                CAST(ra_other_rim_assists_created AS DOUBLE) AS other_rim_assists_created,
                CAST(ra_rim_assists_strict AS DOUBLE) AS rim_assists_strict,
                CAST(ra_rim_assists_all AS DOUBLE) AS rim_assists_all,
                CAST(ra_season_games AS DOUBLE) AS rim_assists_season_games,
                CAST(ra_layup_assists_created_per_game AS DOUBLE) AS layup_assists_created_per_game,
                CAST(ra_dunk_assists_created_per_game AS DOUBLE) AS dunk_assists_created_per_game,
                CAST(ra_other_rim_assists_created_per_game AS DOUBLE) AS other_rim_assists_created_per_game,
                CAST(ra_rim_assists_strict_per_game AS DOUBLE) AS rim_assists_strict_per_game,
                CAST(ra_rim_assists_all_per_game AS DOUBLE) AS rim_assists_all_per_game,
                CAST(rs_rim_anchor_signature AS DOUBLE) AS rim_anchor_signature,
                CAST(rs_rim_deterrence_signature AS DOUBLE) AS rim_deterrence_signature,
                CAST(rd_rim_dfga AS DOUBLE) AS rim_dfga,
                CAST(rd_rim_tracking_games AS DOUBLE) AS rim_tracking_games,
                CAST(rd_rim_dfg_pct AS DOUBLE) AS rim_dfg_pct,
                CAST(rd_rim_dfg_pct_expected AS DOUBLE) AS rim_dfg_pct_expected,
                CAST(rd_rim_dfg_pct_diff AS DOUBLE) AS rim_dfg_pct_diff,
                CAST(rd_rim_dfg_plusminus AS DOUBLE) AS rim_dfg_plusminus,
                CAST(hs_contested_shots AS DOUBLE) AS contested_shots,
                CAST(hs_contested_shots_2pt AS DOUBLE) AS contested_shots_2pt,
                CAST(hs_contested_shots_3pt AS DOUBLE) AS contested_shots_3pt,
                CAST(hs_deflections AS DOUBLE) AS deflections,
                CAST(hs_charges_drawn AS DOUBLE) AS charges_drawn,
                CAST(hs_screen_assists AS DOUBLE) AS screen_assists,
                CAST(hs_screen_ast_pts AS DOUBLE) AS screen_ast_pts,
                CAST(hs_off_loose_balls_recovered AS DOUBLE) AS off_loose_balls_recovered,
                CAST(hs_def_loose_balls_recovered AS DOUBLE) AS def_loose_balls_recovered,
                CAST(hs_loose_balls_recovered AS DOUBLE) AS loose_balls_recovered,
                CAST(hs_off_boxouts AS DOUBLE) AS off_boxouts,
                CAST(hs_def_boxouts AS DOUBLE) AS def_boxouts,
                CAST(hs_box_outs AS DOUBLE) AS box_outs,
                CAST(pm_listed_height AS VARCHAR) AS listed_height,
                CAST(pm_height_inches AS INTEGER) AS height_inches,
                CAST(pm_birthdate AS DATE) AS birthdate,
                CAST(pm_from_year AS INTEGER) AS from_year,
                CAST(pm_draft_year AS INTEGER) AS draft_year,
                CAST(pm_draft_round AS INTEGER) AS draft_round,
                CAST(pm_draft_number AS INTEGER) AS draft_number,
                CAST(pm_draft_overall_pick AS INTEGER) AS draft_overall_pick,
                plus_minus_actual,
                plus_minus_adjusted,
                plus_minus_delta,
                on_off_actual,
                on_off_adjusted,
                on_off_delta,
                CAST(pp_on_possessions AS DOUBLE) AS on_possessions,
                on_pts_for,
                on_pts_against,
                off_pts_for,
                off_pts_against,
                on_diff_reconstructed,
                on_off_diff_reconstructed,
                source_priority
            FROM joined
        ),
        deduped AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY game_id, player_id ORDER BY source_priority ASC) AS rn
                FROM normalized
            )
            WHERE rn = 1
        )
        SELECT
            date,
            season,
            game_id,
            player_id,
            player_name,
            team_id,
            team_abbr,
            CAST(NULL AS BIGINT) AS opp_team_id,
            CASE WHEN team_abbr = home_team THEN away_team
                 WHEN team_abbr = away_team THEN home_team
                 ELSE NULL END AS opp_team_abbr,
            CASE WHEN team_abbr = home_team THEN 'home'
                 WHEN team_abbr = away_team THEN 'away'
                 ELSE NULL END AS home_away,
            CASE
                WHEN team_abbr = home_team AND home_pts_actual > away_pts_actual THEN 'W'
                WHEN team_abbr = home_team AND home_pts_actual < away_pts_actual THEN 'L'
                WHEN team_abbr = away_team AND away_pts_actual > home_pts_actual THEN 'W'
                WHEN team_abbr = away_team AND away_pts_actual < home_pts_actual THEN 'L'
                ELSE NULL
            END AS win_loss,
            starter,
            minutes,
            pts, reb, oreb, dreb, ast, stl, blk, tov, pf,
            fgm, fga, fg3m, fg3a, ftm, fta,
            CAST(COALESCE(fgm, 0) - COALESCE(fg3m, 0) AS INTEGER) AS fg2m,
            CAST(COALESCE(fga, 0) - COALESCE(fg3a, 0) AS INTEGER) AS fg2a,
            CASE
                WHEN COALESCE(fga, 0) - COALESCE(fg3a, 0) > 0
                THEN (COALESCE(fgm, 0) - COALESCE(fg3m, 0)) * 1.0 / (COALESCE(fga, 0) - COALESCE(fg3a, 0))
                ELSE NULL
            END AS fg2_pct,
            CASE
                WHEN COALESCE(fg3a, 0) > 0
                THEN COALESCE(fg3m, 0) * 1.0 / COALESCE(fg3a, 0)
                ELSE NULL
            END AS fg3_pct,
            CASE
                WHEN COALESCE(fta, 0) > 0
                THEN COALESCE(ftm, 0) * 1.0 / COALESCE(fta, 0)
                ELSE NULL
            END AS ft_pct,
            assisted_2pm,
            unassisted_2pm,
            assisted_3pm,
            unassisted_3pm,
            assisted_fgm,
            unassisted_fgm,
            layup_assists_created,
            dunk_assists_created,
            other_rim_assists_created,
            rim_assists_strict,
            rim_assists_all,
            rim_assists_season_games,
            layup_assists_created_per_game,
            dunk_assists_created_per_game,
            other_rim_assists_created_per_game,
            rim_assists_strict_per_game,
            rim_assists_all_per_game,
            rim_anchor_signature,
            rim_deterrence_signature,
            rim_dfga,
            rim_tracking_games,
            rim_dfg_pct,
            rim_dfg_pct_expected,
            rim_dfg_pct_diff,
            rim_dfg_plusminus,
            contested_shots,
            contested_shots_2pt,
            contested_shots_3pt,
            deflections,
            charges_drawn,
            screen_assists,
            screen_ast_pts,
            off_loose_balls_recovered,
            def_loose_balls_recovered,
            loose_balls_recovered,
            off_boxouts,
            def_boxouts,
            box_outs,
            plus_minus_actual,
            plus_minus_adjusted,
            plus_minus_delta,
            on_off_actual,
            on_off_adjusted,
            on_off_delta,
            listed_height,
            height_inches,
            CASE
                WHEN birthdate IS NOT NULL THEN
                    CAST(EXTRACT(year FROM date) - EXTRACT(year FROM birthdate) AS INTEGER)
                    - CASE
                        WHEN (EXTRACT(month FROM date) < EXTRACT(month FROM birthdate))
                          OR (
                               EXTRACT(month FROM date) = EXTRACT(month FROM birthdate)
                               AND EXTRACT(day FROM date) < EXTRACT(day FROM birthdate)
                             )
                        THEN 1 ELSE 0 END
                ELSE NULL
            END AS age,
            from_year,
            CASE
                WHEN from_year IS NOT NULL
                 AND TRY_CAST(substr(season, 1, 4) AS INTEGER) IS NOT NULL
                 AND TRY_CAST(substr(season, 1, 4) AS INTEGER) >= from_year
                THEN TRY_CAST(substr(season, 1, 4) AS INTEGER) - from_year + 1
                ELSE NULL
            END AS career_year,
            draft_year,
            draft_round,
            draft_number,
            draft_overall_pick,
            on_possessions,
            on_pts_for,
            on_pts_against,
            off_pts_for,
            off_pts_against,
            on_diff_reconstructed,
            on_off_diff_reconstructed,
            CASE WHEN team_abbr = home_team THEN home_pts_actual
                 WHEN team_abbr = away_team THEN away_pts_actual
                 ELSE NULL END AS team_pts_actual,
            CASE WHEN team_abbr = home_team THEN away_pts_actual
                 WHEN team_abbr = away_team THEN home_pts_actual
                 ELSE NULL END AS opp_pts_actual,
            CASE WHEN team_abbr = home_team THEN home_pts_adj
                 WHEN team_abbr = away_team THEN away_pts_adj
                 ELSE NULL END AS team_pts_adj,
            CASE WHEN team_abbr = home_team THEN away_pts_adj
                 WHEN team_abbr = away_team THEN home_pts_adj
                 ELSE NULL END AS opp_pts_adj,
            CASE
                WHEN COALESCE(fga, 0) + COALESCE(fta, 0) > 0
                THEN COALESCE(pts, 0) / (2.0 * (COALESCE(fga, 0) + 0.44 * COALESCE(fta, 0)))
                ELSE NULL
            END AS ts_game,
            (
                (CASE WHEN COALESCE(pts, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(reb, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(ast, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(stl, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(blk, 0) >= 10 THEN 1 ELSE 0 END)
            ) >= 2 AS double_double,
            (
                (CASE WHEN COALESCE(pts, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(reb, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(ast, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(stl, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(blk, 0) >= 10 THEN 1 ELSE 0 END)
            ) >= 3 AS triple_double
        FROM deduped
        WHERE COALESCE(minutes, 0) > 0
          AND pts IS NOT NULL;
        """
    )

    con.execute(
        f"""
        CREATE TABLE lineup_stint_facts AS
        WITH stint_base AS (
            SELECT
                game_id, stint_index, home_id, away_id,
                home_p1, home_p2, home_p3, home_p4, home_p5,
                away_p1, away_p2, away_p3, away_p4, away_p5,
                seconds, home_pts, away_pts, home_pts_adj, away_pts_adj,
                start_elapsed, end_elapsed, start_period, start_clock, end_period, end_clock,
                start_home_score, start_away_score, end_home_score, end_away_score,
                start_home_score_adj, start_away_score_adj, end_home_score_adj, end_away_score_adj,
                date,
                1 AS source_priority
            FROM raw_stints
            UNION ALL
            SELECT
                game_id, stint_index, home_id, away_id,
                home_p1, home_p2, home_p3, home_p4, home_p5,
                away_p1, away_p2, away_p3, away_p4, away_p5,
                seconds, home_pts, away_pts, home_pts_adj, away_pts_adj,
                start_elapsed, end_elapsed, start_period, start_clock, end_period, end_clock,
                start_home_score, start_away_score, end_home_score, end_away_score,
                start_home_score_adj, start_away_score_adj, end_home_score_adj, end_away_score_adj,
                date,
                2 AS source_priority
            FROM raw_hist_stints
        ),
        deduped AS (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY CAST(game_id AS VARCHAR), CAST(stint_index AS INTEGER)
                           ORDER BY source_priority ASC
                       ) AS rn
                FROM stint_base
            )
            WHERE rn = 1
        )
        SELECT
            CAST(date AS DATE) AS date,
            CASE
                WHEN EXTRACT(month FROM CAST(date AS DATE)) >= 10
                    THEN CAST(EXTRACT(year FROM CAST(date AS DATE)) AS VARCHAR) || '-' ||
                         right(CAST(EXTRACT(year FROM CAST(date AS DATE)) + 1 AS VARCHAR), 2)
                ELSE CAST(EXTRACT(year FROM CAST(date AS DATE)) - 1 AS VARCHAR) || '-' ||
                     right(CAST(EXTRACT(year FROM CAST(date AS DATE)) AS VARCHAR), 2)
            END AS season,
            CAST(game_id AS VARCHAR) AS game_id,
            CAST(stint_index AS INTEGER) AS stint_index,
            CAST(home_id AS BIGINT) AS home_id,
            CAST(away_id AS BIGINT) AS away_id,
            CAST(NULL AS VARCHAR) AS home_abbr,
            CAST(NULL AS VARCHAR) AS away_abbr,
            CAST(home_p1 AS BIGINT) AS home_p1,
            CAST(home_p2 AS BIGINT) AS home_p2,
            CAST(home_p3 AS BIGINT) AS home_p3,
            CAST(home_p4 AS BIGINT) AS home_p4,
            CAST(home_p5 AS BIGINT) AS home_p5,
            CAST(away_p1 AS BIGINT) AS away_p1,
            CAST(away_p2 AS BIGINT) AS away_p2,
            CAST(away_p3 AS BIGINT) AS away_p3,
            CAST(away_p4 AS BIGINT) AS away_p4,
            CAST(away_p5 AS BIGINT) AS away_p5,
            {_lineup_id_expr('home')} AS home_lineup_id,
            {_lineup_id_expr('away')} AS away_lineup_id,
            CAST(seconds AS DOUBLE) AS seconds,
            CAST(start_elapsed AS INTEGER) AS start_elapsed,
            CAST(end_elapsed AS INTEGER) AS end_elapsed,
            CAST(start_period AS INTEGER) AS start_period,
            CAST(end_period AS INTEGER) AS end_period,
            CAST(start_clock AS VARCHAR) AS start_clock,
            CAST(end_clock AS VARCHAR) AS end_clock,
            CAST(start_home_score AS DOUBLE) AS start_home_score,
            CAST(start_away_score AS DOUBLE) AS start_away_score,
            CAST(end_home_score AS DOUBLE) AS end_home_score,
            CAST(end_away_score AS DOUBLE) AS end_away_score,
            CAST(home_pts AS DOUBLE) AS home_pts,
            CAST(away_pts AS DOUBLE) AS away_pts,
            CAST(home_pts - away_pts AS DOUBLE) AS margin_raw,
            CAST(home_pts_adj AS DOUBLE) AS home_pts_adj,
            CAST(away_pts_adj AS DOUBLE) AS away_pts_adj,
            CAST(home_pts_adj - away_pts_adj AS DOUBLE) AS margin_adj,
            CAST((home_pts_adj - away_pts_adj) - (home_pts - away_pts) AS DOUBLE) AS margin_delta
        FROM deduped;
        """
    )

    con.execute(
        """
        CREATE TABLE lineup_5man_agg AS
        WITH base AS (
            SELECT
                season,
                home_id AS team_id,
                home_abbr AS team_abbr,
                home_lineup_id AS lineup_id,
                home_p1 AS p1, home_p2 AS p2, home_p3 AS p3, home_p4 AS p4, home_p5 AS p5,
                game_id,
                stint_index,
                seconds,
                home_pts AS pts_for_raw,
                away_pts AS pts_against_raw,
                home_pts_adj AS pts_for_adj,
                away_pts_adj AS pts_against_adj
            FROM lineup_stint_facts
            UNION ALL
            SELECT
                season,
                away_id AS team_id,
                away_abbr AS team_abbr,
                away_lineup_id AS lineup_id,
                away_p1 AS p1, away_p2 AS p2, away_p3 AS p3, away_p4 AS p4, away_p5 AS p5,
                game_id,
                stint_index,
                seconds,
                away_pts AS pts_for_raw,
                home_pts AS pts_against_raw,
                away_pts_adj AS pts_for_adj,
                home_pts_adj AS pts_against_adj
            FROM lineup_stint_facts
        )
        SELECT
            season,
            team_id,
            team_abbr,
            lineup_id,
            p1, p2, p3, p4, p5,
            COUNT(DISTINCT game_id) AS games,
            COUNT(*) AS stints,
            SUM(seconds) AS seconds,
            SUM(seconds) / 60.0 AS minutes,
            SUM(seconds) / 24.0 AS poss_est,
            SUM(pts_for_raw) AS pts_for_raw,
            SUM(pts_against_raw) AS pts_against_raw,
            100.0 * (SUM(pts_for_raw) - SUM(pts_against_raw)) / NULLIF(SUM(seconds) / 24.0, 0) AS net_raw,
            SUM(pts_for_adj) AS pts_for_adj,
            SUM(pts_against_adj) AS pts_against_adj,
            100.0 * (SUM(pts_for_adj) - SUM(pts_against_adj)) / NULLIF(SUM(seconds) / 24.0, 0) AS net_adj,
            100.0 * ((SUM(pts_for_adj) - SUM(pts_against_adj)) - (SUM(pts_for_raw) - SUM(pts_against_raw))) / NULLIF(SUM(seconds) / 24.0, 0) AS net_delta
        FROM base
        GROUP BY ALL;
        """
    )

    con.execute(
        """
        CREATE TABLE combo_2man_agg AS
        WITH base AS (
            SELECT season, home_id AS team_id, home_abbr AS team_abbr,
                   [home_p1,home_p2,home_p3,home_p4,home_p5] AS players,
                   game_id, stint_index, seconds,
                   home_pts AS pts_for_raw, away_pts AS pts_against_raw,
                   home_pts_adj AS pts_for_adj, away_pts_adj AS pts_against_adj
            FROM lineup_stint_facts
            UNION ALL
            SELECT season, away_id AS team_id, away_abbr AS team_abbr,
                   [away_p1,away_p2,away_p3,away_p4,away_p5] AS players,
                   game_id, stint_index, seconds,
                   away_pts AS pts_for_raw, home_pts AS pts_against_raw,
                   away_pts_adj AS pts_for_adj, home_pts_adj AS pts_against_adj
            FROM lineup_stint_facts
        ),
        combos AS (
            SELECT
                season, team_id, team_abbr, game_id, stint_index, seconds,
                pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj,
                list_sort([players[1], players[2]]) AS pair FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[1], players[3]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[1], players[4]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[1], players[5]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[2], players[3]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[2], players[4]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[2], players[5]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[3], players[4]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[3], players[5]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[4], players[5]]) FROM base
        )
        SELECT
            season,
            team_id,
            team_abbr,
            array_to_string(pair, '-') AS combo_id,
            pair[1] AS p1,
            pair[2] AS p2,
            COUNT(DISTINCT game_id) AS games,
            COUNT(*) AS stints,
            SUM(seconds) AS seconds,
            SUM(seconds) / 60.0 AS minutes,
            SUM(seconds) / 24.0 AS poss_est,
            SUM(pts_for_raw) AS pts_for_raw,
            SUM(pts_against_raw) AS pts_against_raw,
            100.0 * (SUM(pts_for_raw) - SUM(pts_against_raw)) / NULLIF(SUM(seconds) / 24.0, 0) AS net_raw,
            SUM(pts_for_adj) AS pts_for_adj,
            SUM(pts_against_adj) AS pts_against_adj,
            100.0 * (SUM(pts_for_adj) - SUM(pts_against_adj)) / NULLIF(SUM(seconds) / 24.0, 0) AS net_adj,
            100.0 * ((SUM(pts_for_adj) - SUM(pts_against_adj)) - (SUM(pts_for_raw) - SUM(pts_against_raw))) / NULLIF(SUM(seconds) / 24.0, 0) AS net_delta
        FROM combos
        GROUP BY ALL;
        """
    )

    con.execute(
        """
        CREATE TABLE combo_3man_agg AS
        WITH base AS (
            SELECT season, home_id AS team_id, home_abbr AS team_abbr,
                   [home_p1,home_p2,home_p3,home_p4,home_p5] AS players,
                   game_id, stint_index, seconds,
                   home_pts AS pts_for_raw, away_pts AS pts_against_raw,
                   home_pts_adj AS pts_for_adj, away_pts_adj AS pts_against_adj
            FROM lineup_stint_facts
            UNION ALL
            SELECT season, away_id AS team_id, away_abbr AS team_abbr,
                   [away_p1,away_p2,away_p3,away_p4,away_p5] AS players,
                   game_id, stint_index, seconds,
                   away_pts AS pts_for_raw, home_pts AS pts_against_raw,
                   away_pts_adj AS pts_for_adj, home_pts_adj AS pts_against_adj
            FROM lineup_stint_facts
        ),
        combos AS (
            SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[1], players[2], players[3]]) AS trio FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[1], players[2], players[4]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[1], players[2], players[5]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[1], players[3], players[4]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[1], players[3], players[5]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[1], players[4], players[5]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[2], players[3], players[4]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[2], players[3], players[5]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[2], players[4], players[5]]) FROM base
            UNION ALL SELECT season, team_id, team_abbr, game_id, stint_index, seconds, pts_for_raw, pts_against_raw, pts_for_adj, pts_against_adj, list_sort([players[3], players[4], players[5]]) FROM base
        )
        SELECT
            season,
            team_id,
            team_abbr,
            array_to_string(trio, '-') AS combo_id,
            trio[1] AS p1,
            trio[2] AS p2,
            trio[3] AS p3,
            COUNT(DISTINCT game_id) AS games,
            COUNT(*) AS stints,
            SUM(seconds) AS seconds,
            SUM(seconds) / 60.0 AS minutes,
            SUM(seconds) / 24.0 AS poss_est,
            SUM(pts_for_raw) AS pts_for_raw,
            SUM(pts_against_raw) AS pts_against_raw,
            100.0 * (SUM(pts_for_raw) - SUM(pts_against_raw)) / NULLIF(SUM(seconds) / 24.0, 0) AS net_raw,
            SUM(pts_for_adj) AS pts_for_adj,
            SUM(pts_against_adj) AS pts_against_adj,
            100.0 * (SUM(pts_for_adj) - SUM(pts_against_adj)) / NULLIF(SUM(seconds) / 24.0, 0) AS net_adj,
            100.0 * ((SUM(pts_for_adj) - SUM(pts_against_adj)) - (SUM(pts_for_raw) - SUM(pts_against_raw))) / NULLIF(SUM(seconds) / 24.0, 0) AS net_delta
        FROM combos
        GROUP BY ALL;
        """
    )

    con.close()
    final_tmp = FINAL_DB_PATH.with_suffix(FINAL_DB_PATH.suffix + ".tmp")
    if final_tmp.exists():
        final_tmp.unlink()
    shutil.copy2(BUILD_DB_PATH, final_tmp)
    os.replace(final_tmp, FINAL_DB_PATH)
    print(f"Built {BUILD_DB_PATH}")
    print(f"Published {FINAL_DB_PATH}")


if __name__ == "__main__":
    main()
