"""Attach exact offensive and defensive lineups to half-court rim possessions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
POSSESSIONS = ROOT / "derived" / "defense_causal" / "halfcourt_rim_possessions_2021_22_to_2025_26.parquet"
OUT = ROOT / "derived" / "defense_causal" / "halfcourt_rim_lineup_possessions_2022_23_to_2025_26.parquet"
AUDIT = ROOT / "outputs" / "defense_causal" / "halfcourt_rim_lineup_possessions_audit.json"
STINT_FILES = [ROOT / "data" / f"stints_{year}.csv" for year in range(2022, 2026)]


def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--possession-file",default=str(POSSESSIONS))
    parser.add_argument("--out-name",default=OUT.name)
    parser.add_argument("--audit-name",default=AUDIT.name)
    parser.add_argument("--first-season",type=int,default=2022)
    parser.add_argument("--last-season",type=int,default=2025)
    args=parser.parse_args();possessions=Path(args.possession_file);out=OUT.parent/args.out_name;audit_path=AUDIT.parent/args.audit_name
    out.parent.mkdir(parents=True, exist_ok=True)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    stint_files=[ROOT/"data"/f"stints_{year}.csv" for year in range(args.first_season,args.last_season+1)]
    stint_sources = ", ".join(repr(p.as_posix()) for p in stint_files)
    query = f"""
    WITH stints AS (
      SELECT
        CAST(game_id AS VARCHAR) AS game_id,
        CAST(home_id AS BIGINT) AS home_id,
        CAST(away_id AS BIGINT) AS away_id,
        CAST(start_elapsed AS DOUBLE) AS start_elapsed,
        CAST(end_elapsed AS DOUBLE) AS end_elapsed,
        CAST(stint_index AS INTEGER) AS stint_index,
        CAST(home_lineup_complete AS BOOLEAN) AS home_complete,
        CAST(away_lineup_complete AS BOOLEAN) AS away_complete,
        list_sort([CAST(home_p1 AS BIGINT), CAST(home_p2 AS BIGINT), CAST(home_p3 AS BIGINT),
                   CAST(home_p4 AS BIGINT), CAST(home_p5 AS BIGINT)]) AS home_players,
        list_sort([CAST(away_p1 AS BIGINT), CAST(away_p2 AS BIGINT), CAST(away_p3 AS BIGINT),
                   CAST(away_p4 AS BIGINT), CAST(away_p5 AS BIGINT)]) AS away_players
      FROM read_csv_auto([{stint_sources}], union_by_name=true, header=true)
      WHERE CAST(game_id AS VARCHAR) LIKE '2%' AND CAST(end_elapsed AS DOUBLE) > CAST(start_elapsed AS DOUBLE)
    ), joined AS (
      SELECT p.*, s.stint_index, s.home_id, s.away_id,
        CASE WHEN p.offense_team_id=s.home_id THEN s.home_players ELSE s.away_players END AS offense_players,
        CASE WHEN p.offense_team_id=s.home_id THEN s.away_players ELSE s.home_players END AS defense_players,
        CASE WHEN p.offense_team_id=s.home_id THEN s.away_id ELSE s.home_id END AS defense_team_id,
        CASE WHEN p.offense_team_id=s.home_id THEN s.home_complete ELSE s.away_complete END AS offense_complete,
        CASE WHEN p.offense_team_id=s.home_id THEN s.away_complete ELSE s.home_complete END AS defense_complete,
        row_number() OVER (
          PARTITION BY p.game_id, p.period, p.possession_run
          ORDER BY s.start_elapsed DESC, s.stint_index DESC
        ) AS candidate_number
      FROM read_parquet('{possessions.as_posix()}') p
      JOIN stints s ON p.game_id=s.game_id
        AND p.estimated_start_elapsed >= s.start_elapsed
        AND p.estimated_start_elapsed < s.end_elapsed
      WHERE p.season_year >= {args.first_season}
        AND p.at_risk_hc6=1
        AND p.offense_team_id IN (s.home_id, s.away_id)
    )
    SELECT * EXCLUDE(candidate_number),
      array_to_string(offense_players, '-') AS offense_lineup_id,
      array_to_string(defense_players, '-') AS defense_lineup_id
    FROM joined
    WHERE candidate_number=1 AND offense_complete AND defense_complete
      AND list_unique(offense_players)=5 AND list_unique(defense_players)=5
      AND NOT list_contains(offense_players, 0) AND NOT list_contains(defense_players, 0)
    """
    frame = con.execute(query).df()
    frame.to_parquet(out, index=False)

    source = con.execute(
        f"SELECT count(*) FROM read_parquet('{possessions.as_posix()}') WHERE season_year>={args.first_season} AND at_risk_hc6=1"
    ).fetchone()[0]
    report = {
        "source_halfcourt_possessions": int(source),
        "first_season_year":args.first_season,
        "lineup_attached_complete_possessions": int(len(frame)),
        "coverage": float(len(frame) / source) if source else None,
        "games": int(frame.game_id.nunique()),
        "unique_offense_lineups": int(frame.offense_lineup_id.nunique()),
        "unique_defense_lineups": int(frame.defense_lineup_id.nunique()),
        "duplicate_possession_keys": int(frame.duplicated(["game_id", "period", "possession_run"]).sum()),
    }
    audit_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
