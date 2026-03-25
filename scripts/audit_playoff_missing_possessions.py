from __future__ import annotations

from pathlib import Path

import duckdb


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
AUDIT_DIR = DATA_DIR / "audits"
DB_PATH = DATA_DIR / "nba_analytics_playoffs.duckdb"
STINTS_PATH = DATA_DIR / "stints_playoffs.csv"
POSSESSIONS_PATH = DATA_DIR / "possessions_playoffs.csv"


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute(f"CREATE OR REPLACE TEMP TABLE stints AS SELECT * FROM read_csv_auto('{STINTS_PATH}', header=true, sample_size=-1)")
    con.execute(f"CREATE OR REPLACE TEMP TABLE poss AS SELECT * FROM read_csv_auto('{POSSESSIONS_PATH}', header=true, sample_size=-1)")

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE stint_players AS
        SELECT CAST(game_id AS VARCHAR) AS game_id, CAST(home_p1 AS BIGINT) AS player_id FROM stints
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(home_p2 AS BIGINT) FROM stints
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(home_p3 AS BIGINT) FROM stints
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(home_p4 AS BIGINT) FROM stints
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(home_p5 AS BIGINT) FROM stints
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(away_p1 AS BIGINT) FROM stints
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(away_p2 AS BIGINT) FROM stints
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(away_p3 AS BIGINT) FROM stints
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(away_p4 AS BIGINT) FROM stints
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(away_p5 AS BIGINT) FROM stints
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE poss_any_players AS
        SELECT CAST(game_id AS VARCHAR) AS game_id, CAST(off_p1 AS BIGINT) AS player_id FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(off_p2 AS BIGINT) FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(off_p3 AS BIGINT) FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(off_p4 AS BIGINT) FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(off_p5 AS BIGINT) FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(def_p1 AS BIGINT) FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(def_p2 AS BIGINT) FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(def_p3 AS BIGINT) FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(def_p4 AS BIGINT) FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(def_p5 AS BIGINT) FROM poss
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE poss_off_players AS
        SELECT CAST(game_id AS VARCHAR) AS game_id, CAST(off_p1 AS BIGINT) AS player_id FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(off_p2 AS BIGINT) FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(off_p3 AS BIGINT) FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(off_p4 AS BIGINT) FROM poss
        UNION ALL SELECT CAST(game_id AS VARCHAR), CAST(off_p5 AS BIGINT) FROM poss
        """
    )

    detail = con.execute(
        """
        WITH missing AS (
            SELECT
                CAST(game_id AS VARCHAR) AS game_id,
                CAST(season AS VARCHAR) AS season,
                CAST(player_id AS BIGINT) AS player_id,
                CAST(player_name AS VARCHAR) AS player_name,
                CAST(team_abbr AS VARCHAR) AS team_abbr,
                CAST(minutes AS DOUBLE) AS minutes
            FROM player_game_facts
            WHERE on_possessions IS NULL
        ),
        flags AS (
            SELECT
                m.*,
                EXISTS (
                    SELECT 1 FROM stint_players sp
                    WHERE sp.game_id = m.game_id AND sp.player_id = m.player_id
                ) AS in_stints,
                EXISTS (
                    SELECT 1 FROM poss_any_players pp
                    WHERE pp.game_id = m.game_id AND pp.player_id = m.player_id
                ) AS in_any_possessions,
                EXISTS (
                    SELECT 1 FROM poss_off_players po
                    WHERE po.game_id = m.game_id AND po.player_id = m.player_id
                ) AS in_offensive_possessions
            FROM missing m
        )
        SELECT
            *,
            CASE
                WHEN NOT in_stints THEN 'not_in_stints'
                WHEN in_any_possessions AND NOT in_offensive_possessions THEN 'defense_only_in_possessions'
                WHEN in_stints AND NOT in_any_possessions THEN 'parser_lineup_miss'
                WHEN in_offensive_possessions THEN 'unexpected_null_after_off_possessions'
                ELSE 'unknown'
            END AS cause
        FROM flags
        ORDER BY season, game_id, team_abbr, player_name
        """
    ).fetchdf()
    detail.to_csv(AUDIT_DIR / "playoff_missing_on_possessions_detail.csv", index=False)

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE missing_detail AS
        SELECT * FROM read_csv_auto('/mnt/c/users/dave/Downloads/nba-onoff-publish/data/audits/playoff_missing_on_possessions_detail.csv', header=true, sample_size=-1)
        """
    )
    by_game = con.execute(
        """
        SELECT
            game_id,
            season,
            COUNT(*) AS missing_rows,
            SUM(CASE WHEN cause = 'parser_lineup_miss' THEN 1 ELSE 0 END) AS parser_lineup_miss_rows,
            SUM(CASE WHEN cause = 'not_in_stints' THEN 1 ELSE 0 END) AS not_in_stints_rows,
            SUM(CASE WHEN cause = 'defense_only_in_possessions' THEN 1 ELSE 0 END) AS defense_only_rows,
            SUM(CASE WHEN cause = 'unexpected_null_after_off_possessions' THEN 1 ELSE 0 END) AS unexpected_rows
        FROM missing_detail
        GROUP BY 1,2
        ORDER BY parser_lineup_miss_rows DESC, missing_rows DESC, game_id
        """
    ).fetchdf()
    by_game.to_csv(AUDIT_DIR / "playoff_missing_on_possessions_by_game.csv", index=False)

    by_cause = con.execute(
        """
        SELECT cause, COUNT(*) AS rows
        FROM missing_detail
        GROUP BY 1
        ORDER BY rows DESC, cause
        """
    ).fetchdf()
    by_cause.to_csv(AUDIT_DIR / "playoff_missing_on_possessions_by_cause.csv", index=False)

    summary = con.execute(
        """
        SELECT
            COUNT(*) AS missing_rows_total,
            COUNT(DISTINCT game_id) AS games_with_missing_rows,
            SUM(CASE WHEN cause = 'parser_lineup_miss' THEN 1 ELSE 0 END) AS parser_lineup_miss_rows,
            SUM(CASE WHEN cause = 'not_in_stints' THEN 1 ELSE 0 END) AS not_in_stints_rows,
            SUM(CASE WHEN cause = 'defense_only_in_possessions' THEN 1 ELSE 0 END) AS defense_only_rows,
            SUM(CASE WHEN cause = 'unexpected_null_after_off_possessions' THEN 1 ELSE 0 END) AS unexpected_rows
        FROM missing_detail
        """
    ).fetchdf()
    summary.to_csv(AUDIT_DIR / "playoff_missing_on_possessions_summary.csv", index=False)
    print(summary.to_string(index=False))
    print(by_cause.to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
