from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


DEFAULT_DB = Path(r"C:\Users\Dave\Downloads\nba-onoff-publish\data\nba_analytics.duckdb")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit team-season possession quality in nba_analytics.duckdb")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Path to nba_analytics.duckdb")
    parser.add_argument("--season-start", default="2015-16")
    parser.add_argument("--season-end", default="2025-26")
    parser.add_argument("--min-games", type=int, default=20)
    parser.add_argument("--threshold", type=float, default=85.0, help="Flag avg team possessions below this level")
    parser.add_argument("--limit", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    con = duckdb.connect(str(args.db), read_only=True)
    q = f"""
with season_game_team as (
  select
    season,
    team_abbr,
    game_id,
    max(team_possessions) as team_possessions
  from player_game_facts
  where season between '{args.season_start}' and '{args.season_end}'
  group by 1,2,3
),
team_season as (
  select
    season,
    team_abbr,
    count(*) as games,
    avg(team_possessions) as avg_team_possessions,
    min(team_possessions) as min_team_possessions,
    max(team_possessions) as max_team_possessions,
    100.0 * sum(case when team_possessions < 80 then 1 else 0 end) / count(*) as pct_under_80
  from season_game_team
  group by 1,2
)
select
  season,
  team_abbr,
  games,
  round(avg_team_possessions, 1) as avg_team_possessions,
  round(min_team_possessions, 1) as min_team_possessions,
  round(max_team_possessions, 1) as max_team_possessions,
  round(pct_under_80, 1) as pct_under_80
from team_season
where games >= {int(args.min_games)}
  and avg_team_possessions < {float(args.threshold)}
order by avg_team_possessions asc, pct_under_80 desc, season, team_abbr
limit {int(args.limit)}
"""
    print(con.execute(q).fetchdf().to_string(index=False))


if __name__ == "__main__":
    main()
