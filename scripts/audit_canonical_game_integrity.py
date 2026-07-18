"""Authoritative regular-season lineup/RAPM input audit.

Unlike the earlier possession audit, every score, minute, and plus-minus
comparison here is against the independent official player-game table.  The
output is one row per game and is the only admission gate for the canonical
rebuild; partial prepared-stint totals are never accepted as ground truth.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "nba_analytics.duckdb"
PBP = Path(r"C:\Users\Dave\Downloads\nba-metric-data\PlayByPlay.parquet")
OUT = ROOT / "outputs" / "contextual_causal"

SCORE_TOL = 0.5
TIME_TOL_SECONDS = 1.0
PLAYER_MINUTE_TOL_SECONDS = 75.0  # official minutes are rounded
PLAYER_PM_TOL = 0.5


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB), read_only=True)
    con.execute("PRAGMA threads=4")
    pbp = PBP.as_posix()
    query = f"""
    WITH game_stints AS (
      SELECT ltrim(CAST(game_id AS VARCHAR), '0') AS game_id,
             min(CAST(date AS DATE)) AS date,
             min(home_id) AS home_id, min(away_id) AS away_id,
             count(*) AS stint_count,
             min(start_elapsed) AS first_elapsed,
             max(end_elapsed) AS last_elapsed,
             sum(seconds) AS covered_seconds,
             sum(home_pts) AS reconstructed_home_points,
             sum(away_pts) AS reconstructed_away_points,
             count(*) FILTER (WHERE home_p1 IS NULL OR home_p2 IS NULL
                               OR home_p3 IS NULL OR home_p4 IS NULL
                               OR home_p5 IS NULL OR away_p1 IS NULL
                               OR away_p2 IS NULL OR away_p3 IS NULL
                               OR away_p4 IS NULL OR away_p5 IS NULL)
               AS incomplete_stints
      FROM lineup_stint_facts
      GROUP BY 1
    ),
    official_game AS (
      SELECT ltrim(CAST(game_id AS VARCHAR), '0') AS game_id,
             max(CASE WHEN home_away = 'home' THEN team_pts_actual END)
               AS official_home_points,
             max(CASE WHEN home_away = 'away' THEN team_pts_actual END)
               AS official_away_points,
             max(CASE WHEN home_away = 'home' THEN team_id END)
               AS official_home_id,
             max(CASE WHEN home_away = 'away' THEN team_id END)
               AS official_away_id
      FROM player_game_facts GROUP BY 1
    ),
    official_player AS (
      SELECT ltrim(CAST(game_id AS VARCHAR), '0') AS game_id,
             player_id, max(team_id) AS team_id,
             max(minutes) * 60.0 AS official_seconds,
             max(plus_minus_actual) AS official_plus_minus
      FROM player_game_facts
      WHERE minutes > 0 AND player_id IS NOT NULL
      GROUP BY 1, 2
    ),
    periods AS (
      SELECT ltrim(CAST(gameId AS VARCHAR), '0') AS game_id,
             max(period) AS max_period
      FROM read_parquet('{pbp}')
      WHERE ltrim(CAST(gameId AS VARCHAR), '0') LIKE '2%'
      GROUP BY 1
    ),
    player_stint_rows AS (
      SELECT ltrim(CAST(game_id AS VARCHAR), '0') game_id, home_p1 player_id,
             seconds, home_pts-away_pts pm FROM lineup_stint_facts UNION ALL
      SELECT ltrim(CAST(game_id AS VARCHAR), '0'), home_p2, seconds,
             home_pts-away_pts FROM lineup_stint_facts UNION ALL
      SELECT ltrim(CAST(game_id AS VARCHAR), '0'), home_p3, seconds,
             home_pts-away_pts FROM lineup_stint_facts UNION ALL
      SELECT ltrim(CAST(game_id AS VARCHAR), '0'), home_p4, seconds,
             home_pts-away_pts FROM lineup_stint_facts UNION ALL
      SELECT ltrim(CAST(game_id AS VARCHAR), '0'), home_p5, seconds,
             home_pts-away_pts FROM lineup_stint_facts UNION ALL
      SELECT ltrim(CAST(game_id AS VARCHAR), '0'), away_p1, seconds,
             away_pts-home_pts FROM lineup_stint_facts UNION ALL
      SELECT ltrim(CAST(game_id AS VARCHAR), '0'), away_p2, seconds,
             away_pts-home_pts FROM lineup_stint_facts UNION ALL
      SELECT ltrim(CAST(game_id AS VARCHAR), '0'), away_p3, seconds,
             away_pts-home_pts FROM lineup_stint_facts UNION ALL
      SELECT ltrim(CAST(game_id AS VARCHAR), '0'), away_p4, seconds,
             away_pts-home_pts FROM lineup_stint_facts UNION ALL
      SELECT ltrim(CAST(game_id AS VARCHAR), '0'), away_p5, seconds,
             away_pts-home_pts FROM lineup_stint_facts
    ),
    reconstructed_player AS (
      SELECT game_id, player_id, sum(seconds) reconstructed_seconds,
             sum(pm) reconstructed_plus_minus
      FROM player_stint_rows WHERE player_id IS NOT NULL GROUP BY 1, 2
    ),
    player_compare AS (
      SELECT coalesce(o.game_id, r.game_id) game_id,
             coalesce(o.player_id, r.player_id) player_id,
             o.official_seconds, r.reconstructed_seconds,
             o.official_plus_minus, r.reconstructed_plus_minus,
             o.player_id IS NULL AS extra_reconstructed_player,
             r.player_id IS NULL AS missing_reconstructed_player
      FROM official_player o FULL OUTER JOIN reconstructed_player r
        USING (game_id, player_id)
    ),
    player_game_audit AS (
      SELECT game_id,
             max(abs(coalesce(reconstructed_seconds,0)-
                     coalesce(official_seconds,0))) AS max_player_seconds_error,
             avg(abs(coalesce(reconstructed_seconds,0)-
                     coalesce(official_seconds,0))) AS mean_player_seconds_error,
             max(abs(coalesce(reconstructed_plus_minus,0)-
                     coalesce(official_plus_minus,0))) AS max_player_pm_error,
             count(*) FILTER (WHERE extra_reconstructed_player)
               AS extra_reconstructed_players,
             count(*) FILTER (WHERE missing_reconstructed_player)
               AS missing_reconstructed_players
      FROM player_compare GROUP BY 1
    )
    SELECT s.*, o.official_home_points, o.official_away_points,
           o.official_home_id, o.official_away_id, p.max_period,
           2880 + greatest(p.max_period-4,0)*300 AS expected_seconds,
           a.max_player_seconds_error, a.mean_player_seconds_error,
           a.max_player_pm_error, a.extra_reconstructed_players,
           a.missing_reconstructed_players
    FROM game_stints s
    JOIN official_game o USING (game_id)
    JOIN periods p USING (game_id)
    LEFT JOIN player_game_audit a USING (game_id)
    """
    df = con.execute(query).df()
    con.close()

    df["season_year"] = (pd.to_datetime(df["date"]).dt.year
                         - (pd.to_datetime(df["date"]).dt.month < 10))
    df["score_error"] = ((df.reconstructed_home_points
                           - df.official_home_points).abs()
                         + (df.reconstructed_away_points
                            - df.official_away_points).abs())
    df["missing_lineup_seconds"] = df.expected_seconds - df.covered_seconds
    df["pass_teams"] = ((df.home_id == df.official_home_id)
                        & (df.away_id == df.official_away_id))
    df["pass_score"] = df.score_error <= SCORE_TOL
    df["pass_time"] = df.missing_lineup_seconds.abs() <= TIME_TOL_SECONDS
    df["pass_lineups"] = df.incomplete_stints.eq(0)
    df["pass_player_minutes"] = (
        df.max_player_seconds_error <= PLAYER_MINUTE_TOL_SECONDS)
    df["pass_player_pm"] = df.max_player_pm_error <= PLAYER_PM_TOL
    df["canonical_grade_a"] = (df.pass_teams & df.pass_score & df.pass_time
                                & df.pass_lineups & df.pass_player_minutes
                                & df.pass_player_pm)
    df["failure_reasons"] = df.apply(
        lambda r: "|".join(name for name, ok in (
            ("team", r.pass_teams), ("score", r.pass_score),
            ("time", r.pass_time), ("lineup", r.pass_lineups),
            ("minutes", r.pass_player_minutes), ("plus_minus", r.pass_player_pm))
            if not ok), axis=1)

    df.to_parquet(OUT / "canonical_game_integrity.parquet", index=False)
    df.to_csv(OUT / "canonical_game_integrity.csv", index=False)
    queue = df[~df.canonical_grade_a].sort_values(
        ["season_year", "score_error", "missing_lineup_seconds"],
        ascending=[False, False, False])
    queue.to_csv(OUT / "canonical_game_repair_queue.csv", index=False)

    by_season = (df.groupby("season_year")
                 .agg(games=("game_id", "size"),
                      grade_a=("canonical_grade_a", "mean"),
                      score=("pass_score", "mean"),
                      time=("pass_time", "mean"),
                      minutes=("pass_player_minutes", "mean"),
                      plus_minus=("pass_player_pm", "mean"),
                      median_missing_seconds=("missing_lineup_seconds", "median"))
                 .reset_index())
    by_season.to_csv(OUT / "canonical_game_integrity_by_season.csv", index=False)
    summary = {
        "games": int(len(df)),
        "grade_a_games": int(df.canonical_grade_a.sum()),
        "grade_a_rate": float(df.canonical_grade_a.mean()),
        "pass_rates": {c: float(df[c].mean()) for c in
                       ["pass_teams", "pass_score", "pass_time",
                        "pass_lineups", "pass_player_minutes", "pass_player_pm"]},
        "thresholds": {"score_points": SCORE_TOL,
                       "time_seconds": TIME_TOL_SECONDS,
                       "player_minute_seconds": PLAYER_MINUTE_TOL_SECONDS,
                       "player_plus_minus_points": PLAYER_PM_TOL},
    }
    (OUT / "canonical_game_integrity_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print("\nRecent seasons:")
    print(by_season.tail(8).round(4).to_string(index=False))


if __name__ == "__main__":
    main()
