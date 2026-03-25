from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
AUDIT_DIR = DATA_DIR / "audits"
DB_PATH = Path(os.environ.get("NBA_ANALYTICS_DB_PATH", str(DATA_DIR / "nba_analytics_v2.duckdb")))


def write_csv(con: duckdb.DuckDBPyConnection, name: str, query: str) -> Path:
    path = AUDIT_DIR / name
    df = con.execute(query).fetchdf()
    df.to_csv(path, index=False)
    return path


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)

    outputs: list[Path] = []

    outputs.append(
        write_csv(
            con,
            "player_game_audit_summary_by_season.csv",
            """
            SELECT
                season,
                COUNT(*) AS rows_total,
                SUM(CASE WHEN on_possessions IS NULL THEN 1 ELSE 0 END) AS rows_missing_on_possessions,
                SUM(CASE WHEN pts IS NULL THEN 1 ELSE 0 END) AS rows_missing_box_stats,
                SUM(CASE WHEN team_abbr IS NULL OR opp_team_abbr IS NULL THEN 1 ELSE 0 END) AS rows_missing_team_meta,
                SUM(CASE WHEN player_name IS NULL OR trim(player_name) = '' THEN 1 ELSE 0 END) AS rows_blank_player_name,
                SUM(CASE WHEN fgm IS NOT NULL AND fga IS NOT NULL AND fgm > fga THEN 1 ELSE 0 END) AS rows_bad_fgm_fga,
                SUM(CASE WHEN fg3m IS NOT NULL AND fg3a IS NOT NULL AND fg3m > fg3a THEN 1 ELSE 0 END) AS rows_bad_fg3m_fg3a,
                SUM(CASE WHEN ftm IS NOT NULL AND fta IS NOT NULL AND ftm > fta THEN 1 ELSE 0 END) AS rows_bad_ftm_fta,
                SUM(
                    CASE
                        WHEN pts IS NOT NULL AND fgm IS NOT NULL AND fg3m IS NOT NULL AND ftm IS NOT NULL
                         AND pts <> (2 * (fgm - fg3m) + 3 * fg3m + ftm)
                        THEN 1 ELSE 0
                    END
                ) AS rows_bad_points_formula,
                SUM(
                    CASE
                        WHEN reb IS NOT NULL AND oreb IS NOT NULL AND dreb IS NOT NULL
                         AND reb <> oreb + dreb
                        THEN 1 ELSE 0
                    END
                ) AS rows_bad_rebound_formula,
                SUM(
                    CASE
                        WHEN plus_minus_actual IS NOT NULL AND on_diff_reconstructed IS NOT NULL
                         AND abs(plus_minus_actual - on_diff_reconstructed) > 0.5
                        THEN 1 ELSE 0
                    END
                ) AS rows_pm_vs_reconstructed_mismatch,
                SUM(
                    CASE
                        WHEN on_possessions IS NOT NULL AND minutes >= 5
                         AND (on_possessions / minutes * 48.0 < 75 OR on_possessions / minutes * 48.0 > 125)
                        THEN 1 ELSE 0
                    END
                ) AS rows_extreme_pace
            FROM player_game_facts
            GROUP BY 1
            ORDER BY season DESC
            """,
        )
    )

    outputs.append(
        write_csv(
            con,
            "player_id_name_collisions.csv",
            """
            SELECT
                player_id,
                COUNT(DISTINCT player_name) AS distinct_names,
                string_agg(DISTINCT player_name, ' | ' ORDER BY player_name) AS player_names,
                COUNT(DISTINCT season) AS seasons_seen
            FROM player_game_facts
            WHERE player_id IS NOT NULL
              AND player_name IS NOT NULL
              AND trim(player_name) <> ''
            GROUP BY 1
            HAVING COUNT(DISTINCT player_name) > 1
            ORDER BY distinct_names DESC, seasons_seen DESC, player_id
            LIMIT 1000
            """,
        )
    )

    outputs.append(
        write_csv(
            con,
            "player_name_id_collisions.csv",
            """
            SELECT
                player_name,
                COUNT(DISTINCT player_id) AS distinct_ids,
                string_agg(DISTINCT CAST(player_id AS VARCHAR), ' | ' ORDER BY CAST(player_id AS VARCHAR)) AS player_ids,
                COUNT(DISTINCT season) AS seasons_seen
            FROM player_game_facts
            WHERE player_id IS NOT NULL
              AND player_name IS NOT NULL
              AND trim(player_name) <> ''
            GROUP BY 1
            HAVING COUNT(DISTINCT player_id) > 1
            ORDER BY distinct_ids DESC, seasons_seen DESC, player_name
            LIMIT 1000
            """,
        )
    )

    outputs.append(
        write_csv(
            con,
            "player_game_missing_on_possessions_by_game.csv",
            """
            SELECT
                season,
                date,
                game_id,
                COUNT(*) AS missing_rows,
                SUM(minutes) AS minutes_total
            FROM player_game_facts
            WHERE on_possessions IS NULL
            GROUP BY 1,2,3
            ORDER BY missing_rows DESC, minutes_total DESC, date DESC
            """,
        )
    )

    outputs.append(
        write_csv(
            con,
            "player_game_missing_box_stats_by_game.csv",
            """
            SELECT
                season,
                date,
                game_id,
                COUNT(*) AS missing_rows,
                SUM(minutes) AS minutes_total
            FROM player_game_facts
            WHERE pts IS NULL
            GROUP BY 1,2,3
            ORDER BY missing_rows DESC, minutes_total DESC, date DESC
            """,
        )
    )

    outputs.append(
        write_csv(
            con,
            "player_game_bad_stat_lines.csv",
            """
            SELECT
                season,
                date,
                game_id,
                player_id,
                player_name,
                team_abbr,
                minutes,
                pts,
                reb,
                oreb,
                dreb,
                fgm,
                fga,
                fg3m,
                fg3a,
                ftm,
                fta,
                CASE WHEN fgm IS NOT NULL AND fga IS NOT NULL AND fgm > fga THEN 1 ELSE 0 END AS bad_fgm_fga,
                CASE WHEN fg3m IS NOT NULL AND fg3a IS NOT NULL AND fg3m > fg3a THEN 1 ELSE 0 END AS bad_fg3m_fg3a,
                CASE WHEN ftm IS NOT NULL AND fta IS NOT NULL AND ftm > fta THEN 1 ELSE 0 END AS bad_ftm_fta,
                CASE WHEN pts IS NOT NULL AND fgm IS NOT NULL AND fg3m IS NOT NULL AND ftm IS NOT NULL
                          AND pts <> (2 * (fgm - fg3m) + 3 * fg3m + ftm)
                     THEN 1 ELSE 0 END AS bad_points_formula,
                CASE WHEN reb IS NOT NULL AND oreb IS NOT NULL AND dreb IS NOT NULL
                          AND reb <> oreb + dreb
                     THEN 1 ELSE 0 END AS bad_rebound_formula
            FROM player_game_facts
            WHERE
                (fgm IS NOT NULL AND fga IS NOT NULL AND fgm > fga)
                OR (fg3m IS NOT NULL AND fg3a IS NOT NULL AND fg3m > fg3a)
                OR (ftm IS NOT NULL AND fta IS NOT NULL AND ftm > fta)
                OR (pts IS NOT NULL AND fgm IS NOT NULL AND fg3m IS NOT NULL AND ftm IS NOT NULL
                    AND pts <> (2 * (fgm - fg3m) + 3 * fg3m + ftm))
                OR (reb IS NOT NULL AND oreb IS NOT NULL AND dreb IS NOT NULL
                    AND reb <> oreb + dreb)
            ORDER BY season DESC, date DESC, game_id, player_name
            """,
        )
    )

    outputs.append(
        write_csv(
            con,
            "player_game_pm_mismatch_top.csv",
            """
            SELECT
                season,
                date,
                game_id,
                player_id,
                player_name,
                team_abbr,
                minutes,
                plus_minus_actual,
                on_diff_reconstructed,
                abs(plus_minus_actual - on_diff_reconstructed) AS abs_pm_gap,
                on_off_actual,
                on_off_diff_reconstructed
            FROM player_game_facts
            WHERE plus_minus_actual IS NOT NULL
              AND on_diff_reconstructed IS NOT NULL
              AND abs(plus_minus_actual - on_diff_reconstructed) > 0.5
            ORDER BY abs_pm_gap DESC, season DESC, date DESC
            LIMIT 1000
            """,
        )
    )

    outputs.append(
        write_csv(
            con,
            "player_game_pm_mismatch_by_game.csv",
            """
            SELECT
                season,
                date,
                game_id,
                COUNT(*) AS mismatch_rows,
                SUM(abs(plus_minus_actual - on_diff_reconstructed)) AS total_abs_pm_gap,
                MAX(abs(plus_minus_actual - on_diff_reconstructed)) AS max_abs_pm_gap
            FROM player_game_facts
            WHERE plus_minus_actual IS NOT NULL
              AND on_diff_reconstructed IS NOT NULL
              AND abs(plus_minus_actual - on_diff_reconstructed) > 0.5
            GROUP BY 1,2,3
            ORDER BY total_abs_pm_gap DESC, mismatch_rows DESC, date DESC
            LIMIT 1000
            """,
        )
    )

    outputs.append(
        write_csv(
            con,
            "player_game_extreme_pace_rows.csv",
            """
            SELECT
                season,
                date,
                game_id,
                player_id,
                player_name,
                team_abbr,
                minutes,
                on_possessions,
                round(on_possessions / minutes * 48.0, 2) AS pace48_equiv
            FROM player_game_facts
            WHERE on_possessions IS NOT NULL
              AND minutes >= 5
              AND (on_possessions / minutes * 48.0 < 75 OR on_possessions / minutes * 48.0 > 125)
            ORDER BY abs(on_possessions / minutes * 48.0 - 100) DESC, season DESC, date DESC
            LIMIT 1000
            """,
        )
    )

    outputs.append(
        write_csv(
            con,
            "player_game_duplicate_raw_rows.csv",
            """
            WITH raw_union AS (
                SELECT CAST(game_id AS VARCHAR) AS game_id, CAST(player_id AS BIGINT) AS player_id, 'current_box' AS src FROM raw_player_daily_boxscore
                UNION ALL
                SELECT CAST(game_id AS VARCHAR), CAST(player_id AS BIGINT), 'current_onoff' AS src FROM raw_adjusted_onoff
                UNION ALL
                SELECT CAST(game_id AS VARCHAR), CAST(player_id AS BIGINT), 'hist_onoff' AS src FROM raw_hist_adjusted_onoff
            )
            SELECT
                game_id,
                player_id,
                COUNT(*) AS raw_rows,
                string_agg(src, ', ' ORDER BY src) AS sources
            FROM raw_union
            GROUP BY 1,2
            HAVING COUNT(*) > 1
            ORDER BY raw_rows DESC, game_id, player_id
            LIMIT 1000
            """,
        )
    )

    outputs.append(
        write_csv(
            con,
            "player_game_team_meta_missing_rows.csv",
            """
            SELECT
                season,
                date,
                game_id,
                player_id,
                player_name,
                team_id,
                team_abbr,
                opp_team_id,
                opp_team_abbr,
                minutes
            FROM player_game_facts
            WHERE team_abbr IS NULL OR opp_team_abbr IS NULL
            ORDER BY season DESC, date DESC, game_id, player_name
            LIMIT 1000
            """,
        )
    )

    print(f"Audit complete. Wrote {len(outputs)} files to {AUDIT_DIR}")
    for path in outputs:
        print(path)


if __name__ == "__main__":
    main()
