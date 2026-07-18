from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_DB = DATA_DIR / "nba_analytics.duckdb"
DEFAULT_REFERENCE = DATA_DIR / "player_season_onoff_reference.csv"
DEFAULT_OUTPUT = DATA_DIR / "teamseason_repair_queue.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ranked team-season repair queue for historical possession issues.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Path to nba_analytics.duckdb")
    parser.add_argument("--reference", default=str(DEFAULT_REFERENCE), help="Path to player_season_onoff_reference.csv")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV path")
    parser.add_argument("--season-start", default="1996-97")
    parser.add_argument("--season-end", default="2025-26")
    parser.add_argument("--modern-start", default="2021-22", help="Season threshold for rebuildable modern queue")
    parser.add_argument("--historical-backfill-max", default="2023-24", help="Latest season supported by historical overlay backfill")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    con = duckdb.connect(str(args.db), read_only=True)
    q = f"""
with season_game_team as (
  select
    season,
    team_abbr,
    team_id,
    game_id,
    max(team_possessions) as team_possessions
  from player_game_facts
  where season between '{args.season_start}' and '{args.season_end}'
  group by 1,2,3,4
),
team_season as (
  select
    season,
    team_abbr,
    team_id,
    count(*) as games,
    avg(team_possessions) as avg_team_possessions,
    min(team_possessions) as min_team_possessions,
    max(team_possessions) as max_team_possessions,
    100.0 * sum(case when team_possessions < 80 then 1 else 0 end) / count(*) as pct_under_80,
    100.0 * sum(case when team_possessions < 85 then 1 else 0 end) / count(*) as pct_under_85
  from season_game_team
  group by 1,2,3
)
select *
from team_season
"""
    team_df = con.execute(q).fetchdf()
    ref_df = pd.read_csv(args.reference)
    flags = (
        ref_df.loc[ref_df["possession_quality_flag"] == True]
        .groupby(["season", "team_id", "team_abbr"], as_index=False)
        .agg(
            players_flagged=("player_id", "nunique"),
            minutes_flagged=("on_minutes", "sum"),
        )
    )
    out = team_df.merge(flags, on=["season", "team_id", "team_abbr"], how="left")
    out["players_flagged"] = out["players_flagged"].fillna(0).astype(int)
    out["minutes_flagged"] = out["minutes_flagged"].fillna(0.0)
    out["avg_team_possessions"] = out["avg_team_possessions"].round(3)
    out["min_team_possessions"] = out["min_team_possessions"].round(3)
    out["max_team_possessions"] = out["max_team_possessions"].round(3)
    out["pct_under_80"] = out["pct_under_80"].round(3)
    out["pct_under_85"] = out["pct_under_85"].round(3)

    out["possessions_deficit"] = (100.0 - out["avg_team_possessions"]).clip(lower=0)
    out["severity_score"] = (
        out["possessions_deficit"] * 4.0
        + out["pct_under_80"] * 0.8
        + out["pct_under_85"] * 0.4
        + out["players_flagged"] * 1.5
        + out["minutes_flagged"] / 1000.0
    ).round(3)
    out["repair_priority"] = pd.NA
    out.loc[out["season"] < args.modern_start, "repair_priority"] = "override_or_legacy_source"
    out.loc[
        (out["season"] >= args.modern_start) & (out["season"] <= args.historical_backfill_max),
        "repair_priority",
    ] = "historical_overlay_rebuild_now"
    out.loc[out["season"] > args.historical_backfill_max, "repair_priority"] = "recent_pipeline_rebuild_needed"

    out = out.sort_values(
        ["severity_score", "minutes_flagged", "pct_under_80", "avg_team_possessions"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    out["queue_rank"] = range(1, len(out) + 1)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)
    for priority in [
        "recent_pipeline_rebuild_needed",
        "override_or_legacy_source",
        "historical_overlay_rebuild_now",
    ]:
        split_path = output_path.with_name(f"{output_path.stem}_{priority}{output_path.suffix}")
        out.loc[out["repair_priority"] == priority].to_csv(split_path, index=False)
        print(f"Wrote {split_path} ({int((out['repair_priority'] == priority).sum()):,} rows)")
    print(f"Wrote {output_path} ({len(out):,} rows)")
    print()
    print("Top rebuildable rows:")
    print(
        out.loc[out["repair_priority"] == "historical_overlay_rebuild_now"]
        .head(25)[
            [
                "queue_rank",
                "season",
                "team_abbr",
                "team_id",
                "games",
                "avg_team_possessions",
                "pct_under_80",
                "players_flagged",
                "minutes_flagged",
                "severity_score",
            ]
        ]
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
