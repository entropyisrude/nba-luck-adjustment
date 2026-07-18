from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


ROOT = Path(r"C:\Users\Dave\Downloads\nba-onoff-publish")
DATA_DIR = ROOT / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare historical possession artifact quality by team-season.")
    parser.add_argument("--season-start", default="2015-16")
    parser.add_argument("--season-end", default="2025-26")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--threshold", type=float, default=85.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    current = DATA_DIR / "possessions_historical_pbp.csv"
    v2 = DATA_DIR / "possessions_historical_pbp_v2.csv"
    con = duckdb.connect()
    q = f"""
with current_poss as (
  select
    case when extract(month from cast(date as date)) >= 10
         then cast(extract(year from cast(date as date)) as varchar) || '-' || right(cast(extract(year from cast(date as date)) + 1 as varchar), 2)
         else cast(extract(year from cast(date as date)) - 1 as varchar) || '-' || right(cast(extract(year from cast(date as date)) as varchar), 2) end as season,
    cast(offense_team as bigint) as offense_team,
    cast(game_id as varchar) as game_id,
    count(*) as team_possessions
  from read_csv_auto('{current}', header=true, sample_size=-1, delim=',', strict_mode=false, ignore_errors=true, null_padding=true)
  group by 1,2,3
),
v2_poss as (
  select
    case when extract(month from cast(date as date)) >= 10
         then cast(extract(year from cast(date as date)) as varchar) || '-' || right(cast(extract(year from cast(date as date)) + 1 as varchar), 2)
         else cast(extract(year from cast(date as date)) - 1 as varchar) || '-' || right(cast(extract(year from cast(date as date)) as varchar), 2) end as season,
    cast(offense_team as bigint) as offense_team,
    cast(game_id as varchar) as game_id,
    count(*) as team_possessions
  from read_csv_auto('{v2}', header=true, sample_size=-1, delim=',', strict_mode=false, ignore_errors=true, null_padding=true)
  group by 1,2,3
),
agg_current as (
  select season, offense_team, count(*) as games,
         avg(team_possessions) as avg_team_possessions,
         100.0 * sum(case when team_possessions < 80 then 1 else 0 end) / count(*) as pct_under_80
  from current_poss
  where season between '{args.season_start}' and '{args.season_end}'
  group by 1,2
),
agg_v2 as (
  select season, offense_team, count(*) as games,
         avg(team_possessions) as avg_team_possessions,
         100.0 * sum(case when team_possessions < 80 then 1 else 0 end) / count(*) as pct_under_80
  from v2_poss
  where season between '{args.season_start}' and '{args.season_end}'
  group by 1,2
)
select
  coalesce(c.season, v.season) as season,
  coalesce(c.offense_team, v.offense_team) as offense_team,
  coalesce(c.games, v.games) as games,
  round(c.avg_team_possessions, 1) as current_avg,
  round(v.avg_team_possessions, 1) as v2_avg,
  round(c.pct_under_80, 1) as current_pct_under_80,
  round(v.pct_under_80, 1) as v2_pct_under_80,
  round(coalesce(v.avg_team_possessions, 0) - coalesce(c.avg_team_possessions, 0), 1) as avg_delta,
  case
    when v.avg_team_possessions is null then 'current_only'
    when c.avg_team_possessions is null then 'v2_only'
    when v.avg_team_possessions >= {args.threshold} and c.avg_team_possessions < {args.threshold} then 'prefer_v2'
    when c.avg_team_possessions >= {args.threshold} and v.avg_team_possessions < {args.threshold} then 'prefer_current'
    when v.avg_team_possessions > c.avg_team_possessions + 3 then 'lean_v2'
    when c.avg_team_possessions > v.avg_team_possessions + 3 then 'lean_current'
    else 'similar'
  end as recommendation
from agg_current c
full outer join agg_v2 v
  on c.season = v.season and c.offense_team = v.offense_team
where coalesce(c.games, v.games) >= 20
order by
  case recommendation
    when 'prefer_v2' then 0
    when 'prefer_current' then 1
    when 'lean_v2' then 2
    when 'lean_current' then 3
    else 4
  end,
  abs(coalesce(v.avg_team_possessions, 0) - coalesce(c.avg_team_possessions, 0)) desc,
  season,
  offense_team
limit {int(args.limit)}
"""
    print(con.execute(q).fetchdf().to_string(index=False))


if __name__ == "__main__":
    main()
