from __future__ import annotations

import os
import shutil
from pathlib import Path
import csv

import duckdb

from canonical_onoff_overlay import apply_canonical_counted_onoff


ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
DATA_DIR = ROOT / "data"
# External boxscore data — optional, falls back gracefully when not present.
_BOX_ROOT_CANDIDATES = [
    Path(os.environ["NBA_BOX_ROOT"]) if os.environ.get("NBA_BOX_ROOT") else None,
    Path("/mnt/c/users/dave/Downloads/nba-boxscore-data"),
    Path("C:/Users/Dave/Downloads/nba-boxscore-data"),
]
BOX_ROOT = next((p for p in _BOX_ROOT_CANDIDATES if p is not None and p.exists()), Path(""))

FINAL_DB_PATH = Path(os.environ.get("NBA_PLAYOFF_ANALYTICS_DB_PATH", str(DATA_DIR / "nba_analytics_playoffs.duckdb")))
_default_build = str(Path(os.environ.get("TEMP", "/tmp")) / "nba_analytics_playoffs_build.duckdb")
BUILD_DB_PATH = Path(os.environ.get("NBA_PLAYOFF_ANALYTICS_BUILD_PATH", _default_build))

PLAYOFF_ONOFF = DATA_DIR / "adjusted_onoff_playoffs.csv"
PLAYOFF_STINTS = DATA_DIR / "stints_playoffs.csv"
PLAYOFF_POSSESSIONS = DATA_DIR / "possessions_playoffs.csv"
MODERN_PLAYOFF_BOX = BOX_ROOT / "NBA-Data-2010-2024" / "play_off_box_scores_2010_2024.csv"
KAGGLE_TRADITIONAL = BOX_ROOT / "kaggle-traditional" / "traditional.csv"
_rim_sig_candidates = [
    Path("/mnt/c/users/dave/player_rim_signatures.csv"),
    Path("C:/Users/Dave/player_rim_signatures.csv"),
    DATA_DIR / "player_rim_signatures.csv",
]
PLAYER_RIM_SIGNATURES = next((p for p in _rim_sig_candidates if p.exists()), DATA_DIR / "player_rim_signatures.csv")
PLAYER_RIM_DEFENSE_BY_SEASON = DATA_DIR / "player_rim_defense_by_season.csv"
RECENT_PLAYER_BOX = DATA_DIR / "player_boxscore_stats.csv"
CANONICAL_COUNTED_ONOFF = (
    ROOT
    / "derived"
    / "contextual_causal"
    / "production_counted_onoff"
    / "adjusted_onoff_playoffs_canonical_counted.parquet"
)


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
    if MODERN_PLAYOFF_BOX.exists():
        con.execute(f"CREATE TABLE raw_playoff_box_modern AS SELECT * FROM read_csv_auto('{MODERN_PLAYOFF_BOX}', header=true, sample_size=-1)")
    else:
        print(f"WARNING: {MODERN_PLAYOFF_BOX} not found — creating empty raw_playoff_box_modern.")
        con.execute("""
            CREATE TABLE raw_playoff_box_modern (
                game_date VARCHAR, gameId VARCHAR, teamId BIGINT, teamTricode VARCHAR,
                personId BIGINT, personName VARCHAR, points DOUBLE,
                reboundsTotal DOUBLE, reboundsOffensive DOUBLE, reboundsDefensive DOUBLE,
                assists DOUBLE, steals DOUBLE, blocks DOUBLE, turnovers DOUBLE,
                foulsPersonal DOUBLE, fieldGoalsMade DOUBLE, fieldGoalsAttempted DOUBLE,
                threePointersMade DOUBLE, threePointersAttempted DOUBLE,
                freeThrowsMade DOUBLE, freeThrowsAttempted DOUBLE
            )
        """)
    if KAGGLE_TRADITIONAL.exists():
        con.execute(
            f"""
            CREATE TABLE raw_playoff_box_kaggle AS
            SELECT * RENAME ("3PM" AS fg3m_raw, "3PA" AS fg3a_raw)
            FROM read_csv_auto('{KAGGLE_TRADITIONAL}', header=true, sample_size=-1)
            WHERE lower(type) = 'playoff'
            """
        )
    else:
        print(f"WARNING: {KAGGLE_TRADITIONAL} not found — creating empty raw_playoff_box_kaggle.")
        con.execute("""
            CREATE TABLE raw_playoff_box_kaggle (
                date VARCHAR, gameid VARCHAR, home VARCHAR, away VARCHAR, type VARCHAR,
                team VARCHAR, playerid BIGINT, player VARCHAR,
                PTS DOUBLE, REB DOUBLE, OREB DOUBLE, DREB DOUBLE, AST DOUBLE,
                STL DOUBLE, BLK DOUBLE, TOV DOUBLE, PF DOUBLE,
                FGM DOUBLE, FGA DOUBLE, fg3m_raw DOUBLE, fg3a_raw DOUBLE, FTM DOUBLE, FTA DOUBLE
            )
        """)
    if PLAYER_RIM_SIGNATURES.exists():
        con.execute(f"CREATE TABLE raw_player_rim_signatures AS SELECT * FROM read_csv_auto('{PLAYER_RIM_SIGNATURES}', header=true, sample_size=-1)")
    else:
        con.execute("CREATE TABLE raw_player_rim_signatures (player_id BIGINT, season VARCHAR, rim_attempts DOUBLE, rim_fg_pct DOUBLE)")
    if PLAYER_RIM_DEFENSE_BY_SEASON.exists():
        con.execute(f"CREATE TABLE raw_player_rim_defense_by_season AS SELECT * FROM read_csv_auto('{PLAYER_RIM_DEFENSE_BY_SEASON}', header=true, sample_size=-1)")
    else:
        con.execute("CREATE TABLE raw_player_rim_defense_by_season (player_id BIGINT, season VARCHAR)")
    if RECENT_PLAYER_BOX.exists():
        con.execute(
            f"""
            CREATE TABLE raw_recent_playoff_box AS
            SELECT * FROM read_csv_auto('{RECENT_PLAYER_BOX}', header=true, sample_size=-1)
            WHERE CAST(game_id AS VARCHAR) LIKE '4%'
            """
        )
    else:
        con.execute("CREATE TABLE raw_recent_playoff_box (date VARCHAR, game_id VARCHAR, team_id BIGINT, team_abbr VARCHAR, player_id BIGINT, player_name VARCHAR, starter BIGINT, minutes VARCHAR, pts BIGINT, reb BIGINT, oreb BIGINT, dreb BIGINT, ast BIGINT, stl BIGINT, blk BIGINT, tov BIGINT, pf BIGINT, fgm BIGINT, fga BIGINT, fg3m BIGINT, fg3a BIGINT, ftm BIGINT, fta BIGINT)")

    # Pull player bio data from the regular season DB if available
    regular_db = DATA_DIR / "nba_analytics.duckdb"
    if regular_db.exists():
        con.execute(f"ATTACH '{regular_db}' AS rs_db (READ_ONLY)")
        con.execute("""
            CREATE TABLE player_bio AS
            WITH common AS (
                SELECT
                    CAST(person_id AS BIGINT) AS player_id,
                    CAST(height AS VARCHAR) AS listed_height,
                    TRY_CAST(
                        CASE
                            WHEN height LIKE '%-%' THEN
                                CAST(SPLIT_PART(height, '-', 1) AS INTEGER) * 12
                                + CAST(SPLIT_PART(height, '-', 2) AS INTEGER)
                            ELSE NULL
                        END AS INTEGER
                    ) AS height_inches,
                    TRY_CAST(substr(CAST(birthdate AS VARCHAR), 1, 10) AS DATE) AS birthdate,
                    TRY_CAST(from_year AS INTEGER) AS from_year,
                    TRY_CAST(draft_year AS INTEGER) AS draft_year,
                    TRY_CAST(draft_number AS INTEGER) AS draft_number
                FROM rs_db.raw_common_player_info
            ),
            draft AS (
                -- One row per player: some players were drafted twice (e.g.
                -- Sabonis 1985+1986); keep the most recent draft. Without this
                -- the join fans out and duplicates every game row.
                SELECT player_id, draft_overall_pick FROM (
                    SELECT
                        CAST(person_id AS BIGINT) AS player_id,
                        TRY_CAST(overall_pick AS INTEGER) AS draft_overall_pick,
                        ROW_NUMBER() OVER (
                            PARTITION BY CAST(person_id AS BIGINT)
                            ORDER BY TRY_CAST(season AS INTEGER) DESC
                        ) AS rn
                    FROM rs_db.raw_draft_history
                ) WHERE rn = 1
            ),
            recent AS (
                SELECT
                    player_id,
                    listed_height,
                    height_inches,
                    TRY_CAST(substr(CAST(birthdate AS VARCHAR), 1, 10) AS DATE) AS birthdate,
                    from_year,
                    TRY_CAST(draft_year AS INTEGER) AS draft_year,
                    TRY_CAST(draft_overall_pick AS INTEGER) AS draft_overall_pick
                FROM rs_db.raw_player_metadata_official_recent
            )
            SELECT
                c.player_id,
                COALESCE(r.listed_height, c.listed_height) AS listed_height,
                COALESCE(r.height_inches, c.height_inches) AS height_inches,
                COALESCE(r.birthdate, c.birthdate) AS birthdate,
                COALESCE(r.from_year, c.from_year) AS from_year,
                COALESCE(r.draft_year, c.draft_year) AS draft_year,
                COALESCE(r.draft_overall_pick, d.draft_overall_pick, c.draft_number) AS draft_overall_pick
            FROM common c
            LEFT JOIN draft d ON c.player_id = d.player_id
            LEFT JOIN recent r ON c.player_id = r.player_id
        """)
        con.execute("DETACH rs_db")
    else:
        con.execute("""
            CREATE TABLE player_bio (
                player_id BIGINT,
                listed_height VARCHAR,
                height_inches INTEGER,
                birthdate DATE,
                from_year INTEGER,
                draft_year INTEGER,
                draft_overall_pick INTEGER
            )
        """)

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
        kaggle_team_pts AS (
            SELECT
                CAST(gameid AS VARCHAR) AS game_id,
                CAST(team AS VARCHAR) AS team_abbr,
                SUM(CAST(PTS AS DOUBLE)) AS pts
            FROM raw_playoff_box_kaggle
            GROUP BY 1, 2
        ),
        recent_team_pts AS (
            -- covers seasons newer than the Kaggle dump (e.g. current playoffs)
            SELECT
                CAST(game_id AS VARCHAR) AS game_id,
                CAST(team_abbr AS VARCHAR) AS team_abbr,
                SUM(CAST(pts AS DOUBLE)) AS pts
            FROM raw_recent_playoff_box
            GROUP BY 1, 2
        ),
        game_team AS (
            -- Final scores prefer the official box totals: the stint data is
            -- missing whole OT periods in a few dozen games (e.g. the 2020
            -- DEN-UTA bubble opener stored as a 115-115 "tie"), which corrupts
            -- W/L and displayed scores.
            SELECT
                sg.date,
                sg.game_id,
                COALESCE(kg.home_team, th.team_abbr) AS home_team,
                COALESCE(kg.away_team, ta.team_abbr) AS away_team,
                COALESCE(kh.pts, rh.pts, sg.home_pts_actual) AS home_pts_actual,
                COALESCE(ka.pts, ra.pts, sg.away_pts_actual) AS away_pts_actual,
                sg.home_pts_adj,
                sg.away_pts_adj
            FROM stint_game sg
            LEFT JOIN kaggle_game_meta kg ON sg.game_id = kg.game_id
            LEFT JOIN team_map th ON sg.home_id = th.team_id
            LEFT JOIN team_map ta ON sg.away_id = ta.team_id
            LEFT JOIN kaggle_team_pts kh
              ON sg.game_id = kh.game_id AND COALESCE(kg.home_team, th.team_abbr) = kh.team_abbr
            LEFT JOIN kaggle_team_pts ka
              ON sg.game_id = ka.game_id AND COALESCE(kg.away_team, ta.team_abbr) = ka.team_abbr
            LEFT JOIN recent_team_pts rh
              ON sg.game_id = rh.game_id AND COALESCE(kg.home_team, th.team_abbr) = rh.team_abbr
            LEFT JOIN recent_team_pts ra
              ON sg.game_id = ra.game_id AND COALESCE(kg.away_team, ta.team_abbr) = ra.team_abbr
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
                CAST(fg3m_raw AS INTEGER) AS fg3m,
                CAST(fg3a_raw AS INTEGER) AS fg3a,
                CAST(FTM AS INTEGER) AS ftm,
                CAST(FTA AS INTEGER) AS fta
            FROM raw_playoff_box_kaggle
        ),
        recent_box AS (
            SELECT
                CAST(date AS DATE) AS date,
                CAST(game_id AS VARCHAR) AS game_id,
                CAST(team_id AS BIGINT) AS team_id,
                CAST(team_abbr AS VARCHAR) AS team_abbr,
                CAST(player_id AS BIGINT) AS player_id,
                CAST(player_name AS VARCHAR) AS player_name,
                CAST(CASE WHEN CAST(starter AS VARCHAR) IN ('1','true','True') THEN true ELSE false END AS BOOLEAN) AS starter,
                CAST(pts AS INTEGER) AS pts,
                CAST(reb AS INTEGER) AS reb,
                CAST(oreb AS INTEGER) AS oreb,
                CAST(dreb AS INTEGER) AS dreb,
                CAST(ast AS INTEGER) AS ast,
                CAST(stl AS INTEGER) AS stl,
                CAST(blk AS INTEGER) AS blk,
                CAST(tov AS INTEGER) AS tov,
                CAST(pf AS INTEGER) AS pf,
                CAST(fgm AS INTEGER) AS fgm,
                CAST(fga AS INTEGER) AS fga,
                CAST(fg3m AS INTEGER) AS fg3m,
                CAST(fg3a AS INTEGER) AS fg3a,
                CAST(ftm AS INTEGER) AS ftm,
                CAST(fta AS INTEGER) AS fta
            FROM raw_recent_playoff_box
        ),
        -- Season comes from the game-id prefix (4YY = playoffs of the season starting in
        -- year YY), never from dates: source CSV dates are synthetic and one year early
        -- for every playoff run before 2019-20 (e.g. the 1997 Finals dated June 1996),
        -- which used to shift every pre-2019 season label back by one. Real dates are
        -- restored from the Kaggle traditional box scores where available.
        season_year AS (
            SELECT
                CAST(game_id AS VARCHAR) AS game_id,
                CASE
                    WHEN CAST(substr(CAST(game_id AS VARCHAR), 2, 2) AS INTEGER) >= 90
                        THEN 1900 + CAST(substr(CAST(game_id AS VARCHAR), 2, 2) AS INTEGER)
                    ELSE 2000 + CAST(substr(CAST(game_id AS VARCHAR), 2, 2) AS INTEGER)
                END AS start_year
            FROM (SELECT DISTINCT game_id FROM raw_playoff_onoff)
        ),
        player_base AS (
            SELECT
                CAST(COALESCE(kg.date, o.date) AS DATE) AS date,
                CAST(sy.start_year AS VARCHAR) || '-' ||
                    right(CAST(sy.start_year + 1 AS VARCHAR), 2) AS season,
                CAST(o.game_id AS VARCHAR) AS game_id,
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
            FROM raw_playoff_onoff o
            JOIN season_year sy ON CAST(o.game_id AS VARCHAR) = sy.game_id
            LEFT JOIN kaggle_game_meta kg ON CAST(o.game_id AS VARCHAR) = kg.game_id
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
                rb.team_abbr AS rb_team_abbr,
                rb.player_name AS rb_player_name,
                rb.starter AS rb_starter,
                rb.pts AS rb_pts, rb.reb AS rb_reb, rb.oreb AS rb_oreb, rb.dreb AS rb_dreb,
                rb.ast AS rb_ast, rb.stl AS rb_stl, rb.blk AS rb_blk, rb.tov AS rb_tov, rb.pf AS rb_pf,
                rb.fgm AS rb_fgm, rb.fga AS rb_fga, rb.fg3m AS rb_fg3m, rb.fg3a AS rb_fg3a, rb.ftm AS rb_ftm, rb.fta AS rb_fta,
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
            LEFT JOIN recent_box rb
              ON p.game_id = rb.game_id AND p.player_id = rb.player_id
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
                COALESCE(mb_player_name, kb_player_name, rb_player_name, player_name) AS player_name,
                team_id,
                COALESCE(mb_team_abbr, kb_team_abbr, rb_team_abbr, tm_team_abbr) AS team_abbr,
                CAST(NULL AS BIGINT) AS opp_team_id,
                CASE
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, rb_team_abbr, tm_team_abbr) = home_team THEN away_team
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, rb_team_abbr, tm_team_abbr) = away_team THEN home_team
                    ELSE NULL
                END AS opp_team_abbr,
                CASE
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, rb_team_abbr, tm_team_abbr) = home_team THEN 'home'
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, rb_team_abbr, tm_team_abbr) = away_team THEN 'away'
                    ELSE NULL
                END AS home_away,
                CASE
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, rb_team_abbr, tm_team_abbr) = home_team AND home_pts_actual > away_pts_actual THEN 'W'
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, rb_team_abbr, tm_team_abbr) = home_team AND home_pts_actual < away_pts_actual THEN 'L'
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, rb_team_abbr, tm_team_abbr) = away_team AND away_pts_actual > home_pts_actual THEN 'W'
                    WHEN COALESCE(mb_team_abbr, kb_team_abbr, rb_team_abbr, tm_team_abbr) = away_team AND away_pts_actual < home_pts_actual THEN 'L'
                    ELSE NULL
                END AS win_loss,
                COALESCE(mb_starter, kb_starter, rb_starter) AS starter,
                minutes,
                COALESCE(mb_pts, kb_pts, rb_pts) AS pts,
                COALESCE(mb_reb, kb_reb, rb_reb) AS reb,
                COALESCE(mb_oreb, kb_oreb, rb_oreb) AS oreb,
                COALESCE(mb_dreb, kb_dreb, rb_dreb) AS dreb,
                COALESCE(mb_ast, kb_ast, rb_ast) AS ast,
                COALESCE(mb_stl, kb_stl, rb_stl) AS stl,
                COALESCE(mb_blk, kb_blk, rb_blk) AS blk,
                COALESCE(mb_tov, kb_tov, rb_tov) AS tov,
                COALESCE(mb_pf, kb_pf, rb_pf) AS pf,
                COALESCE(mb_fgm, kb_fgm, rb_fgm) AS fgm,
                COALESCE(mb_fga, kb_fga, rb_fga) AS fga,
                COALESCE(mb_fg3m, kb_fg3m, rb_fg3m) AS fg3m,
                COALESCE(mb_fg3a, kb_fg3a, rb_fg3a) AS fg3a,
                COALESCE(mb_ftm, kb_ftm, rb_ftm) AS ftm,
                COALESCE(mb_fta, kb_fta, rb_fta) AS fta,
                CAST(COALESCE(mb_fgm, kb_fgm, rb_fgm, 0) - COALESCE(mb_fg3m, kb_fg3m, rb_fg3m, 0) AS INTEGER) AS fg2m,
                CAST(COALESCE(mb_fga, kb_fga, rb_fga, 0) - COALESCE(mb_fg3a, kb_fg3a, rb_fg3a, 0) AS INTEGER) AS fg2a,
                CASE
                    WHEN COALESCE(mb_fga, kb_fga, rb_fga, 0) - COALESCE(mb_fg3a, kb_fg3a, rb_fg3a, 0) > 0
                    THEN (COALESCE(mb_fgm, kb_fgm, rb_fgm, 0) - COALESCE(mb_fg3m, kb_fg3m, rb_fg3m, 0)) * 1.0 /
                         (COALESCE(mb_fga, kb_fga, rb_fga, 0) - COALESCE(mb_fg3a, kb_fg3a, rb_fg3a, 0))
                    ELSE NULL
                END AS fg2_pct,
                CASE
                    WHEN COALESCE(mb_fg3a, kb_fg3a, rb_fg3a, 0) > 0
                    THEN COALESCE(mb_fg3m, kb_fg3m, rb_fg3m, 0) * 1.0 / COALESCE(mb_fg3a, kb_fg3a, rb_fg3a, 0)
                    ELSE NULL
                END AS fg3_pct,
                CASE
                    WHEN COALESCE(mb_fta, kb_fta, rb_fta, 0) > 0
                    THEN COALESCE(mb_ftm, kb_ftm, rb_ftm, 0) * 1.0 / COALESCE(mb_fta, kb_fta, rb_fta, 0)
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
        ),
        with_team_poss AS (
            SELECT
                d.* EXCLUDE(source_priority, rn),
                SUM(d.on_possessions) OVER (PARTITION BY d.game_id, d.team_id) / 5.0 AS team_possessions,
                b.listed_height,
                b.height_inches,
                CASE
                    WHEN b.birthdate IS NOT NULL THEN
                        CAST(EXTRACT(year FROM d.date) - EXTRACT(year FROM b.birthdate) AS INTEGER)
                        - CASE
                            WHEN (EXTRACT(month FROM d.date) < EXTRACT(month FROM b.birthdate))
                              OR (EXTRACT(month FROM d.date) = EXTRACT(month FROM b.birthdate)
                                  AND EXTRACT(day FROM d.date) < EXTRACT(day FROM b.birthdate))
                            THEN 1 ELSE 0
                          END
                    ELSE NULL
                END AS age,
                CASE
                    WHEN b.from_year IS NOT NULL THEN
                        CAST(EXTRACT(year FROM d.date) AS INTEGER) - b.from_year
                        - CASE WHEN EXTRACT(month FROM d.date) < 9 THEN 0 ELSE -1 END
                    ELSE NULL
                END AS career_year,
                b.draft_year,
                b.draft_overall_pick
            FROM deduped d
            LEFT JOIN player_bio b ON d.player_id = b.player_id
        )
        SELECT *
        FROM with_team_poss
        WHERE COALESCE(minutes, 0) > 0
          AND pts IS NOT NULL
        """
    )

    apply_canonical_counted_onoff(
        con,
        CANONICAL_COUNTED_ONOFF,
        label="playoffs",
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
