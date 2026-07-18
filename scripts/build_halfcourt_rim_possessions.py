"""Build a modern regular-season possession table for rim-defense research.

The table separates first-chance half-court rim attempts from transition and
second-chance attempts. For 2022-23 onward, the NBA feed also supplies
categorical locations for nearly every shooting foul. Restricted Area is the
strict foul-at-rim definition; 0-8-foot non-RA paint fouls are retained as a
broader sensitivity outcome.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PBP = Path(r"C:\Users\Dave\Downloads\nba-metric-data\PlayByPlay.parquet")
DERIVED = ROOT / "derived" / "defense_causal"
OUTPUTS = ROOT / "outputs" / "defense_causal"
OUT = DERIVED / "halfcourt_rim_possessions_2021_22_to_2025_26.parquet"
AUDIT = OUTPUTS / "halfcourt_rim_possessions_audit.json"
START_DATE = "2021-10-01"
RIM_FEET = 4.0


def sql(path: str, start_date: str = START_DATE) -> str:
    return f"""
    WITH raw AS (
      SELECT
        CAST(gameId AS VARCHAR) AS game_id,
        try_cast(gameDateTimeEst AS TIMESTAMP) AS game_date,
        year(try_cast(gameDateTimeEst AS TIMESTAMP))
          - CASE WHEN month(try_cast(gameDateTimeEst AS TIMESTAMP)) < 10 THEN 1 ELSE 0 END AS season_year,
        try_cast(period AS INTEGER) AS period,
        try_cast(orderNumber AS DOUBLE) AS order_number,
        lower(trim(coalesce(actionType, ''))) AS action_type,
        lower(trim(coalesce(subType, ''))) AS subtype,
        lower(trim(coalesce(descriptor, ''))) AS descriptor_name,
        coalesce(description, '') AS description,
        try_cast(teamId AS BIGINT) AS event_team_id,
        try_cast(personId AS BIGINT) AS person_id,
        try_cast(possession AS BIGINT) AS offense_team_id,
        try_cast(shotDistance AS DOUBLE) AS shot_distance,
        lower(trim(coalesce(area, ''))) AS area_name,
        lower(trim(coalesce(areaDetail, ''))) AS area_detail,
        lower(trim(coalesce(shotResult, ''))) AS shot_result,
        try_cast(foulDrawnPersonId AS BIGINT) AS foul_drawn_person_id,
        try_cast(reboundOffensiveTotal AS DOUBLE) AS rebound_offensive_total,
        CASE
          WHEN regexp_matches(coalesce(clock, ''), '^PT[0-9]+M[0-9.]+S$') THEN
            try_cast(regexp_extract(clock, '^PT([0-9]+)M', 1) AS DOUBLE) * 60
            + try_cast(regexp_extract(clock, 'M([0-9.]+)S$', 1) AS DOUBLE)
          ELSE NULL
        END AS clock_seconds
      FROM read_parquet('{path}')
      WHERE try_cast(gameDateTimeEst AS TIMESTAMP) >= TIMESTAMP '{start_date}'
        AND starts_with(CAST(gameId AS VARCHAR), '2')
    ), base AS (
      SELECT *,
        CASE WHEN period <= 4
          THEN (period - 1) * 720 + (720 - clock_seconds)
          ELSE 2880 + (period - 5) * 300 + (300 - clock_seconds)
        END AS elapsed,
        lag(offense_team_id) OVER w AS previous_offense_team_id
      FROM raw
      WHERE period IS NOT NULL AND order_number IS NOT NULL AND clock_seconds IS NOT NULL
      WINDOW w AS (PARTITION BY game_id, period ORDER BY order_number)
    ), marked AS (
      SELECT *,
        CASE WHEN offense_team_id IS NOT NULL AND
          (previous_offense_team_id IS NULL OR offense_team_id <> previous_offense_team_id)
          THEN 1 ELSE 0 END AS new_run
      FROM base
    ), numbered AS (
      SELECT *,
        sum(new_run) OVER (
          PARTITION BY game_id, period ORDER BY order_number
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS possession_run
      FROM marked
      WHERE offense_team_id IS NOT NULL AND offense_team_id > 1000000000
    ), run_raw AS (
      SELECT
        game_id, period, possession_run,
        min(game_date) AS game_date,
        min(season_year) AS season_year,
        min(offense_team_id) AS offense_team_id,
        min(elapsed) AS first_event_elapsed,
        max(elapsed) AS run_end_elapsed,
        arg_min(action_type, order_number) AS first_action_type
      FROM numbered
      GROUP BY game_id, period, possession_run
    ), run_timing AS (
      SELECT *,
        lag(run_end_elapsed) OVER (
          PARTITION BY game_id, period ORDER BY possession_run
        ) AS previous_run_end_elapsed
      FROM run_raw
    ), run_meta AS (
      SELECT *,
        CASE
          WHEN possession_run = 1 THEN
            CASE WHEN period <= 4 THEN (period - 1) * 720 ELSE 2880 + (period - 5) * 300 END
          WHEN first_action_type IN ('rebound', 'jumpball', 'jump ball') THEN first_event_elapsed
          ELSE coalesce(previous_run_end_elapsed, first_event_elapsed)
        END AS estimated_start_elapsed
      FROM run_timing
    ), annotated0 AS (
      SELECT n.*, r.estimated_start_elapsed, r.run_end_elapsed,
        greatest(n.elapsed - r.estimated_start_elapsed, 0) AS seconds_since_possession_start,
        CASE WHEN n.action_type IN ('2pt', 'made shot', 'missed shot')
          AND coalesce(n.shot_distance, 999) <= {RIM_FEET}
          AND NOT regexp_matches(upper(n.description), '3PT')
          THEN 1 ELSE 0 END AS is_rim_attempt,
        CASE WHEN n.action_type IN ('2pt', 'made shot', 'missed shot')
          AND coalesce(n.shot_distance, 999) <= {RIM_FEET}
          AND NOT regexp_matches(upper(n.description), '3PT')
          AND (n.shot_result = 'made' OR n.action_type = 'made shot'
               OR (n.action_type = '2pt' AND NOT regexp_matches(upper(n.description), '^MISS')))
          THEN 1 ELSE 0 END AS is_rim_make,
        CASE WHEN n.action_type = 'rebound' AND
          (n.subtype = 'offensive' OR regexp_matches(lower(n.description), 'offensive rebound'))
          THEN 1 ELSE 0 END AS is_offensive_rebound,
        CASE WHEN n.action_type = 'foul' AND
          (n.subtype = 'shooting' OR n.descriptor_name = 'shooting')
          THEN 1 ELSE 0 END AS is_shooting_foul,
        CASE WHEN n.action_type = 'foul' AND
          (n.subtype = 'shooting' OR n.descriptor_name = 'shooting')
          AND n.area_name = 'restricted area'
          THEN 1 ELSE 0 END AS is_restricted_area_shooting_foul,
        CASE WHEN n.action_type = 'foul' AND
          (n.subtype = 'shooting' OR n.descriptor_name = 'shooting')
          AND (n.area_name = 'restricted area' OR
               (n.area_name = 'in the paint (non-ra)' AND n.area_detail = '0-8 center'))
          THEN 1 ELSE 0 END AS is_broad_0_8_paint_shooting_foul,
        CASE WHEN regexp_matches(n.subtype, 'putback|tip')
          OR regexp_matches(n.descriptor_name, 'putback|tip')
          THEN 1 ELSE 0 END AS explicit_putback
      FROM numbered n
      JOIN run_meta r USING (game_id, period, possession_run)
    ), annotated AS (
      SELECT *,
        coalesce(sum(is_offensive_rebound) OVER (
          PARTITION BY game_id, period, possession_run ORDER BY order_number
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS offensive_rebounds_before,
        max(is_rim_attempt) OVER (
          PARTITION BY game_id, period, possession_run, elapsed
        ) AS same_clock_rim_attempt,
        max(is_rim_make) OVER (
          PARTITION BY game_id, period, possession_run, elapsed
        ) AS same_clock_rim_make
      FROM annotated0
    )
    SELECT
      a.game_id, min(a.game_date) AS game_date, min(a.season_year) AS season_year,
      a.period, a.possession_run, min(a.offense_team_id) AS offense_team_id,
      min(a.estimated_start_elapsed) AS estimated_start_elapsed,
      max(a.run_end_elapsed) AS run_end_elapsed,
      max(a.run_end_elapsed) - min(a.estimated_start_elapsed) AS estimated_duration,
      CAST(max(a.run_end_elapsed) - min(a.estimated_start_elapsed) >= 4 AS INTEGER) AS at_risk_hc4,
      CAST(max(a.run_end_elapsed) - min(a.estimated_start_elapsed) >= 6 AS INTEGER) AS at_risk_hc6,
      CAST(max(a.run_end_elapsed) - min(a.estimated_start_elapsed) >= 8 AS INTEGER) AS at_risk_hc8,
      max(CASE WHEN is_rim_attempt=1 AND seconds_since_possession_start < 6
        AND offensive_rebounds_before=0 THEN 1 ELSE 0 END) AS transition_rim_attempt_lt6,
      max(CASE WHEN is_rim_attempt=1 AND seconds_since_possession_start >= 4
        AND offensive_rebounds_before=0 THEN 1 ELSE 0 END) AS first_chance_rim_hc4,
      max(CASE WHEN is_rim_attempt=1 AND seconds_since_possession_start >= 6
        AND offensive_rebounds_before=0 THEN 1 ELSE 0 END) AS first_chance_rim_hc6,
      max(CASE WHEN is_rim_attempt=1 AND seconds_since_possession_start >= 8
        AND offensive_rebounds_before=0 THEN 1 ELSE 0 END) AS first_chance_rim_hc8,
      sum(CASE WHEN is_rim_attempt=1 AND seconds_since_possession_start >= 6
        AND offensive_rebounds_before=0 THEN 1 ELSE 0 END) AS first_chance_rim_attempts_hc6,
      sum(CASE WHEN is_rim_make=1 AND seconds_since_possession_start >= 6
        AND offensive_rebounds_before=0 THEN 1 ELSE 0 END) AS first_chance_rim_makes_hc6,
      max(CASE WHEN (is_rim_attempt=1 OR is_restricted_area_shooting_foul=1)
        AND seconds_since_possession_start >= 6 AND offensive_rebounds_before=0
        THEN 1 ELSE 0 END) AS first_chance_strict_rim_event_hc6,
      max(CASE WHEN (is_rim_attempt=1 OR is_broad_0_8_paint_shooting_foul=1)
        AND seconds_since_possession_start >= 6 AND offensive_rebounds_before=0
        THEN 1 ELSE 0 END) AS first_chance_broad_rim_event_hc6,
      max(CASE WHEN is_rim_attempt=1 AND offensive_rebounds_before>0 THEN 1 ELSE 0 END) AS second_chance_rim_attempt,
      max(CASE WHEN (is_rim_attempt=1 OR is_restricted_area_shooting_foul=1)
        AND offensive_rebounds_before>0 THEN 1 ELSE 0 END) AS second_chance_strict_rim_event,
      sum(CASE WHEN is_rim_attempt=1 AND offensive_rebounds_before>0 THEN 1 ELSE 0 END) AS second_chance_rim_attempts,
      sum(CASE WHEN is_rim_make=1 AND offensive_rebounds_before>0 THEN 1 ELSE 0 END) AS second_chance_rim_makes,
      max(CASE WHEN is_rim_attempt=1 AND explicit_putback=1 THEN 1 ELSE 0 END) AS explicit_putback_rim_attempt,
      max(is_offensive_rebound) AS possession_has_offensive_rebound,
      max(CASE WHEN is_shooting_foul=1 AND same_clock_rim_attempt=1
        THEN 1 ELSE 0 END) AS high_confidence_rim_shooting_foul,
      max(CASE WHEN is_restricted_area_shooting_foul=1
        AND seconds_since_possession_start>=6 AND offensive_rebounds_before=0
        THEN 1 ELSE 0 END) AS first_chance_restricted_area_foul_hc6,
      max(CASE WHEN is_broad_0_8_paint_shooting_foul=1
        AND seconds_since_possession_start>=6 AND offensive_rebounds_before=0
        THEN 1 ELSE 0 END) AS first_chance_broad_0_8_paint_foul_hc6,
      max(CASE WHEN is_restricted_area_shooting_foul=1
        AND offensive_rebounds_before>0
        THEN 1 ELSE 0 END) AS second_chance_restricted_area_foul,
      max(CASE WHEN is_shooting_foul=1 AND area_name=''
        AND seconds_since_possession_start>=6 AND offensive_rebounds_before=0
        THEN 1 ELSE 0 END) AS location_unknown_shooting_foul_hc6,
      min(CASE WHEN is_rim_attempt=1 AND seconds_since_possession_start>=6
        AND offensive_rebounds_before=0 THEN elapsed ELSE NULL END) AS first_rim_event_elapsed_hc6
    FROM annotated a
    GROUP BY a.game_id, a.period, a.possession_run
    """


def audit(df: pd.DataFrame, dropped_negative_duration_rows: int = 0) -> dict:
    by_season = {}
    for season, g in df.groupby("season_year"):
        risk = g["at_risk_hc6"].sum()
        attempts = g["first_chance_rim_attempts_hc6"].sum()
        makes = g["first_chance_rim_makes_hc6"].sum()
        sc_att = g["second_chance_rim_attempts"].sum()
        sc_make = g["second_chance_rim_makes"].sum()
        by_season[str(int(season))] = {
            "possessions": int(len(g)), "halfcourt_risk_possessions_6s": int(risk),
            "first_chance_rim_attempts_hc6": int(attempts),
            "first_chance_strict_rim_event_possessions_hc6": int(g["first_chance_strict_rim_event_hc6"].sum()),
            "first_chance_broad_rim_event_possessions_hc6": int(g["first_chance_broad_rim_event_hc6"].sum()),
            "first_chance_rim_fg_pct": float(makes / attempts) if attempts else None,
            "second_chance_rim_attempts": int(sc_att),
            "second_chance_rim_fg_pct": float(sc_make / sc_att) if sc_att else None,
            "explicit_putback_rim_possessions": int(g["explicit_putback_rim_attempt"].sum()),
            "high_confidence_rim_shooting_fouls": int(g["high_confidence_rim_shooting_foul"].sum()),
            "first_chance_restricted_area_fouls_hc6": int(g["first_chance_restricted_area_foul_hc6"].sum()),
            "first_chance_broad_0_8_paint_fouls_hc6": int(g["first_chance_broad_0_8_paint_foul_hc6"].sum()),
            "second_chance_restricted_area_fouls": int(g["second_chance_restricted_area_foul"].sum()),
            "location_unknown_shooting_fouls_hc6": int(g["location_unknown_shooting_foul_hc6"].sum()),
        }
    return {
        "definition": {
            "rim_distance_feet": RIM_FEET,
            "primary_halfcourt_cutoff_seconds": 6,
            "sensitivity_cutoffs_seconds": [4, 8],
            "first_chance": "no prior offensive rebound in the possession",
            "second_chance": "at least one prior offensive rebound in the possession",
            "foul_policy": "Restricted Area is the strict near-rim foul definition; Restricted Area plus non-RA 0-8 Center is the broad sensitivity definition. Location is nearly complete from 2022-23 onward and absent in 2021-22.",
        },
        "rows": int(len(df)), "games": int(df["game_id"].nunique()),
        "duplicate_possession_keys": int(df.duplicated(["game_id", "period", "possession_run"]).sum()),
        "negative_duration_rows": int((df["estimated_duration"] < 0).sum()),
        "dropped_negative_duration_rows": dropped_negative_duration_rows,
        "by_season": by_season,
    }


def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--start-date",default=START_DATE)
    parser.add_argument("--out-name",default=OUT.name)
    parser.add_argument("--audit-name",default=AUDIT.name)
    args=parser.parse_args()
    out=DERIVED/args.out_name;audit_path=OUTPUTS/args.audit_name
    DERIVED.mkdir(parents=True, exist_ok=True); OUTPUTS.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    frame = con.execute(sql(PBP.as_posix(),args.start_date)).df()
    dropped_negative = int((frame["estimated_duration"] < 0).sum())
    frame = frame[frame["estimated_duration"] >= 0].copy()
    frame.to_parquet(out, index=False)
    report = audit(frame, dropped_negative_duration_rows=dropped_negative)
    audit_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Wrote {out}")
    print(f"Wrote {audit_path}")


if __name__ == "__main__":
    main()
