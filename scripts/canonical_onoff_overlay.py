from __future__ import annotations

from pathlib import Path

import duckdb


def apply_canonical_counted_onoff(
    con: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    *,
    label: str,
) -> dict[str, int]:
    """Overlay audited counted on/off evidence onto player_game_facts.

    The analytics builders still assemble box scores, bios, and historical
    fallbacks from their original sources.  This final overlay makes the
    canonical counted artifact authoritative wherever it has a player-game.
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Canonical counted on/off file not found: {parquet_path}")

    quoted_path = str(parquet_path).replace("'", "''")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE canonical_onoff_overlay AS
        SELECT
            CAST(game_id AS VARCHAR) AS game_id,
            CAST(team_id AS BIGINT) AS team_id,
            CAST(player_id AS BIGINT) AS player_id,
            CAST(on_diff AS DOUBLE) AS on_diff,
            CAST(on_diff_adj AS DOUBLE) AS on_diff_adj,
            CAST(on_off_diff AS DOUBLE) AS on_off_diff,
            CAST(on_off_diff_adj AS DOUBLE) AS on_off_diff_adj,
            CAST(on_pts_for AS DOUBLE) AS on_pts_for,
            CAST(on_pts_against AS DOUBLE) AS on_pts_against,
            CAST(off_pts_for AS DOUBLE) AS off_pts_for,
            CAST(off_pts_against AS DOUBLE) AS off_pts_against,
            CAST(on_off_poss AS DOUBLE) AS on_possessions
        FROM read_parquet('{quoted_path}')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(game_id AS VARCHAR), CAST(player_id AS BIGINT)
            ORDER BY CAST(team_id AS BIGINT)
        ) = 1
        """
    )

    source_rows, source_games = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT game_id) FROM canonical_onoff_overlay"
    ).fetchone()
    matched_rows, matched_games = con.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT f.game_id)
        FROM player_game_facts f
        JOIN canonical_onoff_overlay c
          ON CAST(f.game_id AS VARCHAR) = c.game_id
         AND CAST(f.player_id AS BIGINT) = c.player_id
        """
    ).fetchone()

    con.execute(
        """
        UPDATE player_game_facts AS f
        SET
            plus_minus_actual = c.on_diff,
            plus_minus_adjusted = c.on_diff_adj,
            plus_minus_delta = c.on_diff_adj - c.on_diff,
            on_off_actual = c.on_off_diff,
            on_off_adjusted = c.on_off_diff_adj,
            on_off_delta = c.on_off_diff_adj - c.on_off_diff,
            on_possessions = c.on_possessions,
            on_pts_for = c.on_pts_for,
            on_pts_against = c.on_pts_against,
            off_pts_for = c.off_pts_for,
            off_pts_against = c.off_pts_against,
            on_diff_reconstructed = c.on_diff,
            on_off_diff_reconstructed = c.on_off_diff
        FROM canonical_onoff_overlay AS c
        WHERE CAST(f.game_id AS VARCHAR) = c.game_id
          AND CAST(f.player_id AS BIGINT) = c.player_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE canonical_team_possessions AS
        SELECT game_id, team_id, SUM(on_possessions) / 5.0 AS team_possessions
        FROM canonical_onoff_overlay
        GROUP BY game_id, team_id
        """
    )
    con.execute(
        """
        UPDATE player_game_facts AS f
        SET team_possessions = t.team_possessions
        FROM canonical_team_possessions AS t
        WHERE CAST(f.game_id AS VARCHAR) = t.game_id
          AND CAST(f.team_id AS BIGINT) = t.team_id
        """
    )

    stats = {
        "source_rows": int(source_rows),
        "source_games": int(source_games),
        "matched_rows": int(matched_rows),
        "matched_games": int(matched_games),
    }
    print(
        f"Canonical counted on/off overlay ({label}): "
        f"{stats['matched_rows']:,}/{stats['source_rows']:,} player-games, "
        f"{stats['matched_games']:,}/{stats['source_games']:,} games matched"
    )
    return stats
