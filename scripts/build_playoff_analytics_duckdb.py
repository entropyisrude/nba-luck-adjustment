from __future__ import annotations

import os
import shutil
from pathlib import Path
import csv

import duckdb


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
BOX_ROOT = Path("/mnt/c/users/dave/Downloads/nba-boxscore-data")

FINAL_DB_PATH = Path(os.environ.get("NBA_PLAYOFF_ANALYTICS_DB_PATH", str(DATA_DIR / "nba_analytics_playoffs.duckdb")))
BUILD_DB_PATH = Path(os.environ.get("NBA_PLAYOFF_ANALYTICS_BUILD_PATH", "/tmp/nba_analytics_playoffs_build.duckdb"))

PLAYOFF_ONOFF = DATA_DIR / "adjusted_onoff_playoffs.csv"
PLAYOFF_STINTS = DATA_DIR / "stints_playoffs.csv"
PLAYOFF_POSSESSIONS = DATA_DIR / "possessions_playoffs.csv"
MODERN_PLAYOFF_BOX = BOX_ROOT / "NBA-Data-2010-2024" / "play_off_box_scores_2010_2024.csv"
KAGGLE_TRADITIONAL = BOX_ROOT / "kaggle-traditional" / "traditional.csv"
PLAYER_RIM_SIGNATURES = Path("/mnt/c/users/dave/player_rim_signatures.csv")
PLAYER_RIM_DEFENSE_BY_SEASON = DATA_DIR / "player_rim_defense_by_season.csv"


TEAM_ID_TO_ABBR = {
    1610612737: "ATL", 1610612738: "BOS", 1610612751: "BKN", 1610612766: "CHA",
    1610612741: "CHI", 1610612739: "CLE", 1610612742: "DAL", 1610612743: "DEN",
    1610612765: "DET", 1610612744: "GSW", 1610612745: "HOU", 1610612754: "IND",
    1610612746: "LAC", 1610612747: "LAL", 1610612763: "MEM", 1610612748: "MIA",
    1610612749: "MIL", 1610612750: "MIN", 1610612740: "NOP", 1610612752: "NYK",
    1610612760: "OKC", 1610612753: "ORL", 1610612755: "PHI", 1610612756: "PHX",
    1610612757: "POR", 1610612758: "SAC", 1610612759: "SAS", 1610612761: "TOR",
    1610612762: "UTA", 1610612764: "WAS",
}


def csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        next(reader, None)
        for _ in reader:
            count += 1
    return count


def main() -> None:
    BUILD_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    FINAL_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if BUILD_DB_PATH.exists():
        BUILD_DB_PATH.unlink()

    con = duckdb.connect(str(BUILD_DB_PATH))
    con.execute("PRAGMA threads=4")

    con.execute("CREATE TABLE team_map(team_id BIGINT, team_abbr VARCHAR)")
    con.executemany("INSERT INTO team_map VALUES (?, ?)", list(TEAM_ID_TO_ABBR.items()))

    con.execute(f"CREATE TABLE raw_playoff_onoff AS SELECT * FROM read_csv_auto('{PLAYOFF_ONOFF}', header=true, sample_size=-1)")
    con.execute(f"CREATE TABLE raw_playoff_stints AS SELECT * FROM read_csv_auto('{PLAYOFF_STINTS}', header=true, sample_size=-1)")
    if PLAYOFF_POSSESSIONS.exists():
        con.execute(f"CREATE TABLE raw_playoff_possessions AS SELECT * FROM read_csv_auto('{PLAYOFF_POSSESSIONS}', header=true, sample_size=-1)")
    else:
        con.execute(
            """
            CREATE TABLE raw_playoff_possessions AS
            SELECT
                CAST(NULL AS VARCHAR) AS game_id,
                CAST(NULL AS BIGINT) AS poss_index,
                CAST(NULL AS DATE) AS date,
                CAST(NULL AS BIGINT) AS offense_team,
                CAST(NULL AS BIGINT) AS defense_team,
                CAST(NULL AS BIGINT) AS off_p1,
                CAST(NULL AS BIGINT) AS off_p2,
                CAST(NULL AS BIGINT) AS off_p3,
                CAST(NULL AS BIGINT) AS off_p4,
                CAST(NULL AS BIGINT) AS off_p5,
                CAST(NULL AS BIGINT) AS def_p1,
                CAST(NULL AS BIGINT) AS def_p2,
                CAST(NULL AS BIGINT) AS def_p3,
                CAST(NULL AS BIGINT) AS def_p4,
                CAST(NULL AS BIGINT) AS def_p5,
                CAST(NULL AS DOUBLE) AS points,
                CAST(NULL AS DOUBLE) AS points_adj,
                CAST(NULL AS VARCHAR) AS ended_by,
                CAST(NULL AS BIGINT) AS period
            WHERE FALSE
            """
        )
    con.execute(f"CREATE TABLE raw_playoff_box_modern AS SELECT * FROM read_csv_auto('{MODERN_PLAYOFF_BOX}', header=true, sample_size=-1)")
    con.execute(
        f"""
        CREATE TABLE raw_playoff_box_kaggle AS
        SELECT * FROM read_csv_auto('{KAGGLE_TRADITIONAL}', header=true, sample_size=-1)
        WHERE lower(type) = 'playoff'
        """
    )
    con.execute(f"CREATE TABLE raw_player_rim_signatures AS SELECT * FROM read_csv_auto('{PLAYER_RIM_SIGNATURES}', header=true, sample_size=-1)")
    con.execute(f"CREATE TABLE raw_player_rim_defense_by_season AS SELECT * FROM read_csv_auto('{PLAYER_RIM_DEFENSE_BY_SEASON}', header=true, sample_size=-1)")

    con.execute(
        """
        CREATE TABLE player_game_facts AS
        WITH stint_game AS (
            SELECT
                CAST(game_id AS VARCHAR) AS game_id,
                CAST(MAX(date) AS DATE) AS date,
                CAST(MAX(home_id) AS BIGINT) AS home_id,
                CAST(MAX(away_id) AS BIGINT) AS away_id,
                MAX(CAST(end_home_score AS DOUBLE)) AS home_pts_actual,
                MAX(CAST(end_away_score AS DOUBLE)) AS away_pts_actual,
                MAX(CAST(end_home_score_adj AS DOUBLE)) AS home_pts_adj,
                MAX(CAST(end_away_score_adj AS DOUBLE)) AS away_pts_adj
            FROM raw_playoff_stints
            GROUP BY 1
        ),
        kaggle_game_meta AS (
            SELECT
                CAST(gameid AS VARCHAR) AS game_id,
                MAX(CAST(date AS DATE)) AS date,
                MAX(CAST(home AS VARCHAR)) AS home_team,
                MAX(CAST(away AS VARCHAR)) AS away_team
            FROM raw_playoff_box_kaggle
            GROUP BY 1
        ),
        game_team AS (
            SELECT
                sg.date,
                sg.game_id,
                COALESCE(kg.home_team, th.team_abbr) AS home_team,
                COALESCE(kg.away_team, ta.team_abbr) AS away_team,
                sg.home_pts_actual,
                sg.away_pts_actual,
                sg.home_pts_adj,
                sg.away_pts_adj
            FROM stint_game sg
            LEFT JOIN kaggle_game_meta kg ON sg.game_id = kg.game_id
            LEFT JOIN team_map th ON sg.home_id = th.team_id
            LEFT JOIN team_map ta ON sg.away_id = ta.team_id
        ),
        playoff_possessions AS (
            SELECT DISTINCT
                CAST(game_id AS VARCHAR) AS game_id,
                CAST(poss_index AS BIGINT) AS poss_index,
                CAST(date AS DATE) AS date,
                CAST(offense_team AS BIGINT) AS offense_team,
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
            FROM raw_playoff_possessions
        ),
        player_possession_totals AS (
            SELECT date, game_id, CAST(player_id AS BIGINT) AS player_id, COUNT(*) AS on_possessions
            FROM (
                SELECT date, game_id, off_p1 AS player_id FROM playoff_possessions
                UNION ALL SELECT date, game_id, off_p2 AS player_id FROM playoff_possessions
                UNION ALL SELECT date, game_id, off_p3 AS player_id FROM playoff_possessions
                UNION ALL SELECT date, game_id, off_p4 AS player_id FROM playoff_possessions
                UNION ALL SELECT date, game_id, off_p5 AS player_id FROM playoff_possessions
            )
            WHERE player_id IS NOT NULL AND player_id > 0
            GROUP BY 1,2,3
        ),
        player_possession_any AS (
            SELECT DISTINCT game_id, CAST(player_id AS BIGINT) AS player_id
            FROM (
                SELECT game_id, off_p1 AS player_id FROM playoff_possessions
                UNION ALL SELECT game_id, off_p2 AS player_id FROM playoff_possessions
                UNION ALL SELECT game_id, off_p3 AS player_id FROM playoff_possessions
                UNION ALL SELECT game_id, off_p4 AS player_id FROM playoff_possessions
                UNION ALL SELECT game_id, off_p5 AS player_id FROM playoff_possessions
                UNION ALL SELECT game_id, def_p1 AS player_id FROM playoff_possessions
                UNION ALL SELECT game_id, def_p2 AS player_id FROM playoff_possessions
                UNION ALL SELECT game_id, def_p3 AS player_id FROM playoff_possessions
                UNION ALL SELECT game_id, def_p4 AS player_id FROM playoff_possessions
                UNION ALL SELECT game_id, def_p5 AS player_id FROM playoff_possessions
            )
            WHERE player_id IS NOT NULL AND player_id > 0
        ),
        modern_box AS (
            SELECT
                CAST(game_date AS DATE) AS date,
                CAST(gameId AS VARCHAR) AS game_id,
                CAST(teamId AS BIGINT) AS team_id,
                CAST(teamTricode AS VARCHAR) AS team_abbr,
                CAST(personId AS BIGINT) AS player_id,
                CAST(personName AS VARCHAR) AS player_name,
                CAST(NULL AS BOOLEAN) AS starter,
                CAST(points AS INTEGER) AS pts,
                CAST(reboundsTotal AS INTEGER) AS reb,
                CAST(reboundsOffensive AS INTEGER) AS oreb,
                CAST(reboundsDefensive AS INTEGER) AS dreb,
                CAST(assists AS INTEGER) AS ast,
                CAST(steals AS INTEGER) AS stl,
                CAST(blocks AS INTEGER) AS blk,
                CAST(turnovers AS INTEGER) AS tov,
                CAST(foulsPersonal AS INTEGER) AS pf,
                CAST(fieldGoalsMade AS INTEGER) AS fgm,
                CAST(fieldGoalsAttempted AS INTEGER) AS fga,
                CAST(threePointersMade AS INTEGER) AS fg3m,
                CAST(threePointersAttempted AS INTEGER) AS fg3a,
                CAST(freeThrowsMade AS INTEGER) AS ftm,
                CAST(freeThrowsAttempted AS INTEGER) AS fta
            FROM raw_playoff_box_modern
        ),
        kaggle_box AS (
            SELECT
                CAST(date AS DATE) AS date,
                CAST(gameid AS VARCHAR) AS game_id,
                CAST(NULL AS BIGINT) AS team_id,
                CAST(team AS VARCHAR) AS team_abbr,
                CAST(playerid AS BIGINT) AS player_id,
                CAST(player AS VARCHAR) AS player_name,
                CAST(NULL AS BOOLEAN) AS starter,
                CAST(PTS AS INTEGER) AS pts,
                CAST(REB AS INTEGER) AS reb,
                CAST(OREB AS INTEGER) AS oreb,
                CAST(DREB AS INTEGER) AS dreb,
                CAST(AST AS INTEGER) AS ast,
                CAST(STL AS INTEGER) AS stl,
                CAST(BLK AS INTEGER) AS blk,
                CAST(TOV AS INTEGER) AS tov,
                CAST(PF AS INTEGER) AS pf,
                CAST(FGM AS INTEGER) AS fgm,
                CAST(FGA AS INTEGER) AS fga,
                CAST("3PM" AS INTEGER) AS fg3m,
                CAST("3PA" AS INTEGER) AS fg3a,
                CAST(FTM AS INTEGER) AS ftm,
                CAST(FTA AS INTEGER) AS fta
            FROM raw_playoff_box_kaggle
        ),
        player_base AS (
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
                CAST(player_id AS BIGINT) AS player_id,
                CAST(player_name AS VARCHAR) AS player_name,
                CAST(team_id AS BIGINT) AS team_id,
                CAST(minutes_on AS DOUBLE) AS minutes,
                CAST(on_diff AS DOUBLE) AS plus_minus_actual,
                CAST(on_diff_adj AS DOUBLE) AS plus_minus_adjusted,
                CAST(on_diff_adj AS DOUBLE) - CAST(on_diff AS DOUBLE) AS plus_minus_delta,
                CAST(on_off_diff AS DOUBLE) AS on_off_actual,
                CAST(on_off_diff_adj AS DOUBLE) AS on_off_adjusted,
                CAST(on_off_diff_adj AS DOUBLE) - CAST(on_off_diff AS DOUBLE) AS on_off_delta,
                CAST(on_pts_for AS DOUBLE) AS on_pts_for,
                CAST(on_pts_against AS DOUBLE) AS on_pts_against,
                CAST(off_pts_for AS DOUBLE) AS off_pts_for,
                CAST(off_pts_against AS DOUBLE) AS off_pts_against,
                CAST(on_diff AS DOUBLE) AS on_diff_reconstructed,
                CAST(on_off_diff AS DOUBLE) AS on_off_diff_reconstructed
            FROM raw_playoff_onoff
        ),
        joined AS (
            SELECT
                p.*,
                tm.team_abbr AS tm_team_abbr,
                mb.team_abbr AS mb_team_abbr,
                mb.player_name AS mb_player_name,
                mb.starter AS mb_starter,
                mb.pts AS mb_pts, mb.reb AS mb_reb, mb.oreb AS mb_oreb, mb.dreb AS mb_dreb,
                mb.ast AS mb_ast, mb.stl AS mb_stl, mb.blk AS mb_blk, mb.tov AS mb_tov, mb.pf AS mb_pf,
                mb.fgm AS mb_fgm, mb.fga AS mb_fga, mb.fg3m AS mb_fg3m, mb.fg3a AS mb_fg3a, mb.ftm AS mb_ftm, mb.fta AS mb_fta,
                kb.team_abbr AS kb_team_abbr,
                kb.player_name AS kb_player_name,
                kb.starter AS kb_starter,
                kb.pts AS kb_pts, kb.reb AS kb_reb, kb.oreb AS kb_oreb, kb.dreb AS kb_dreb,
                kb.ast AS kb_ast, kb.stl AS kb_stl, kb.blk AS kb_blk, kb.tov AS kb_tov, kb.pf AS kb_pf,
                kb.fgm AS kb_fgm, kb.fga AS kb_fga, kb.fg3m AS kb_fg3m, kb.fg3a AS kb_fg3a, kb.ftm AS kb_ftm, kb.fta AS kb_fta,
                rs.rim_anchor_signature AS rs_rim_anchor_signature,
                rs.rim_deterrence_signature AS rs_rim_deterrence_signature,
                rd.rim_dfga AS rd_rim_dfga,
                rd.games AS rd_rim_tracking_games,
                rd.rim_dfg_pct AS rd_rim_dfg_pct,
                rd.rim_dfg_pct_expected AS rd_rim_dfg_pct_expected,
                rd.rim_dfg_pct_diff AS rd_rim_dfg_pct_diff,
                rd.rim_dfg_plusminus AS rd_rim_dfg_plusminus,
                pp.on_possessions AS pp_on_possessions,
                pa.player_id AS pa_player_id,
                gt.home_team, gt.away_team, gt.home_pts_actual, gt.away_pts_actual, gt.home_pts_adj, gt.away_pts_adj
            FROM player_base p
            LEFT JOIN team_map tm
              ON p.team_id = tm.team_id
            LEFT JOIN modern_box mb
              ON p.game_id = mb.game_id AND p.player_id = mb.player_id
            LEFT JOIN kaggle_box kb
              ON p.game_id = kb.game_id AND p.player_id = kb.player_id
            LEFT JOIN raw_player_rim_signatures rs
              ON p.player_id = CAST(rs.player_id AS BIGINT)
            LEFT JOIN raw_player_rim_defense_by_season rd
              ON p.season = CAST(rd.season AS VARCHAR) AND p.player_id = CAST(rd.player_id AS BIGINT)
            LEFT JOIN player_possession_totals pp
              ON p.game_id = pp.game_id AND p.player_id = pp.player_id
            LEFT JOIN player_possession_any pa
              ON p.game_id = pa.game_id AND p.player_id = pa.player_id
            LEFT JOIN game_team gt
              ON p.game_id = gt.game_id
        ),
        normalized AS (
            SELECT
                date,
                season,
                game_id,
                player_id,
                COALESCE(mb_player_name, kb_player_name, player_name) AS player_name,
                team_id,
                COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) AS team_abbr,
                CAST(NULL AS BIGINT) AS opp_team_id,
                CASE
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = home_team THEN away_team
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = away_team THEN home_team
                    ELSE NULL
                END AS opp_team_abbr,
                CASE
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = home_team THEN 'home'
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = away_team THEN 'away'
                    ELSE NULL
                END AS home_away,
                CASE
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = home_team AND home_pts_actual > away_pts_actual THEN 'W'
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = home_team AND home_pts_actual < away_pts_actual THEN 'L'
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = away_team AND away_pts_actual > home_pts_actual THEN 'W'
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = away_team AND away_pts_actual < home_pts_actual THEN 'L'
                    ELSE NULL
                END AS win_loss,
                COALESCE(mb_starter, kb_starter) AS starter,
                minutes,
                COALESCE(mb_pts, kb_pts) AS pts,
                COALESCE(mb_reb, kb_reb) AS reb,
                COALESCE(mb_oreb, kb_oreb) AS oreb,
                COALESCE(mb_dreb, kb_dreb) AS dreb,
                COALESCE(mb_ast, kb_ast) AS ast,
                COALESCE(mb_stl, kb_stl) AS stl,
                COALESCE(mb_blk, kb_blk) AS blk,
                COALESCE(mb_tov, kb_tov) AS tov,
                COALESCE(mb_pf, kb_pf) AS pf,
                COALESCE(mb_fgm, kb_fgm) AS fgm,
                COALESCE(mb_fga, kb_fga) AS fga,
                COALESCE(mb_fg3m, kb_fg3m) AS fg3m,
                COALESCE(mb_fg3a, kb_fg3a) AS fg3a,
                COALESCE(mb_ftm, kb_ftm) AS ftm,
                COALESCE(mb_fta, kb_fta) AS fta,
                CAST(COALESCE(mb_fgm, kb_fgm, 0) - COALESCE(mb_fg3m, kb_fg3m, 0) AS INTEGER) AS fg2m,
                CAST(COALESCE(mb_fga, kb_fga, 0) - COALESCE(mb_fg3a, kb_fg3a, 0) AS INTEGER) AS fg2a,
                CASE
                    WHEN COALESCE(mb_fga, kb_fga, 0) - COALESCE(mb_fg3a, kb_fg3a, 0) > 0
                    THEN (COALESCE(mb_fgm, kb_fgm, 0) - COALESCE(mb_fg3m, kb_fg3m, 0)) * 1.0 /
                         (COALESCE(mb_fga, kb_fga, 0) - COALESCE(mb_fg3a, kb_fg3a, 0))
                    ELSE NULL
                END AS fg2_pct,
                CASE
                    WHEN COALESCE(mb_fg3a, kb_fg3a, 0) > 0
                    THEN COALESCE(mb_fg3m, kb_fg3m, 0) * 1.0 / COALESCE(mb_fg3a, kb_fg3a, 0)
                    ELSE NULL
                END AS fg3_pct,
                CASE
                    WHEN COALESCE(mb_fta, kb_fta, 0) > 0
                    THEN COALESCE(mb_ftm, kb_ftm, 0) * 1.0 / COALESCE(mb_fta, kb_fta, 0)
                    ELSE NULL
                END AS ft_pct,
                CAST(0 AS INTEGER) AS assisted_2pm,
                CAST(0 AS INTEGER) AS unassisted_2pm,
                CAST(0 AS INTEGER) AS assisted_3pm,
                CAST(0 AS INTEGER) AS unassisted_3pm,
                CAST(0 AS INTEGER) AS assisted_fgm,
                CAST(0 AS INTEGER) AS unassisted_fgm,
                CAST(rs_rim_anchor_signature AS DOUBLE) AS rim_anchor_signature,
                CAST(rs_rim_deterrence_signature AS DOUBLE) AS rim_deterrence_signature,
                CAST(rd_rim_dfga AS DOUBLE) AS rim_dfga,
                CAST(rd_rim_tracking_games AS DOUBLE) AS rim_tracking_games,
                CAST(rd_rim_dfg_pct AS DOUBLE) AS rim_dfg_pct,
                CAST(rd_rim_dfg_pct_expected AS DOUBLE) AS rim_dfg_pct_expected,
                CAST(rd_rim_dfg_pct_diff AS DOUBLE) AS rim_dfg_pct_diff,
                CAST(rd_rim_dfg_plusminus AS DOUBLE) AS rim_dfg_plusminus,
                plus_minus_actual,
                plus_minus_adjusted,
                plus_minus_delta,
                on_off_actual,
                on_off_adjusted,
                on_off_delta,
                CAST(
                    CASE
                        WHEN pp_on_possessions IS NOT NULL THEN pp_on_possessions
                        WHEN pa_player_id IS NOT NULL THEN 0
                        ELSE NULL
                    END AS DOUBLE
                ) AS on_possessions,
                on_pts_for,
                on_pts_against,
                off_pts_for,
                off_pts_against,
                on_diff_reconstructed,
                on_off_diff_reconstructed,
                CASE WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = home_team THEN home_pts_actual
                     WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = away_team THEN away_pts_actual
                     ELSE NULL END AS team_pts_actual,
                CASE WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = home_team THEN away_pts_actual
                     WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = away_team THEN home_pts_actual
                     ELSE NULL END AS opp_pts_actual,
                CASE WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = home_team THEN home_pts_adj
                     WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = away_team THEN away_pts_adj
                     ELSE NULL END AS team_pts_adj,
                CASE WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = home_team THEN away_pts_adj
                     WHEN COALESCE(mb_team_abbr, kb_team_abbr, tm_team_abbr) = away_team THEN home_pts_adj
                     ELSE NULL END AS opp_pts_adj,
                CASE
                    WHEN COALESCE(mb_fga, kb_fga, 0) + COALESCE(mb_fta, kb_fta, 0) > 0
                    THEN COALESCE(mb_pts, kb_pts, 0) / (2.0 * (COALESCE(mb_fga, kb_fga, 0) + 0.44 * COALESCE(mb_fta, kb_fta, 0)))
                    ELSE NULL
                END AS ts_game,
                (
                    (CASE WHEN COALESCE(mb_pts, kb_pts, 0) >= 10 THEN 1 ELSE 0 END) +
                    (CASE WHEN COALESCE(mb_reb, kb_reb, 0) >= 10 THEN 1 ELSE 0 END) +
                    (CASE WHEN COALESCE(mb_ast, kb_ast, 0) >= 10 THEN 1 ELSE 0 END) +
                    (CASE WHEN COALESCE(mb_stl, kb_stl, 0) >= 10 THEN 1 ELSE 0 END) +
                    (CASE WHEN COALESCE(mb_blk, kb_blk, 0) >= 10 THEN 1 ELSE 0 END)
                ) >= 2 AS double_double,
                (
                    (CASE WHEN COALESCE(mb_pts, kb_pts, 0) >= 10 THEN 1 ELSE 0 END) +
                    (CASE WHEN COALESCE(mb_reb, kb_reb, 0) >= 10 THEN 1 ELSE 0 END) +
                    (CASE WHEN COALESCE(mb_ast, kb_ast, 0) >= 10 THEN 1 ELSE 0 END) +
                    (CASE WHEN COALESCE(mb_stl, kb_stl, 0) >= 10 THEN 1 ELSE 0 END) +
                    (CASE WHEN COALESCE(mb_blk, kb_blk, 0) >= 10 THEN 1 ELSE 0 END)
                ) >= 3 AS triple_double,
                CASE
                    WHEN mb_pts IS NOT NULL THEN 1
                    WHEN kb_pts IS NOT NULL THEN 2
                    ELSE 3
                END AS source_priority
            FROM joined
        ),
        deduped AS (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY game_id, player_id ORDER BY source_priority ASC, minutes DESC) AS rn
                FROM normalized
            )
            WHERE rn = 1
        )
        SELECT
            * EXCLUDE(source_priority, rn)
        FROM deduped
        WHERE COALESCE(minutes, 0) > 0
          AND pts IS NOT NULL
        """
    )

    con.close()
    expected_poss_rows = csv_row_count(PLAYOFF_POSSESSIONS) if PLAYOFF_POSSESSIONS.exists() else 0
    if expected_poss_rows < 150000:
        raise SystemExit(
            f"Refusing to publish playoff analytics DB: possessions_playoffs.csv looks too small "
            f"({expected_poss_rows} rows). Rebuild possessions first."
        )

    final_tmp = FINAL_DB_PATH.with_suffix(FINAL_DB_PATH.suffix + ".tmp")
    if final_tmp.exists():
        final_tmp.unlink()
    shutil.copy2(BUILD_DB_PATH, final_tmp)
    os.replace(final_tmp, FINAL_DB_PATH)
    print(f"Wrote playoff analytics DB to {FINAL_DB_PATH}")


if __name__ == "__main__":
    main()
