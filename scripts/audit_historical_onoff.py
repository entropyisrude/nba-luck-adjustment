from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


DEFAULT_DB = Path(r"C:\Users\Dave\Downloads\nba-onoff-publish\data\nba_analytics.duckdb")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit span on/off formulas from the analytics DB")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Path to nba_analytics.duckdb")
    parser.add_argument("--player", action="append", required=True, help="Player name, repeatable")
    parser.add_argument("--player-id", action="append", default=[], help="Player id, repeatable")
    parser.add_argument("--season-start", default="2015-16")
    parser.add_argument("--season-end", default="2025-26")
    parser.add_argument("--show-seasons", action="store_true", help="Show season-level breakdown")
    return parser.parse_args()


def sql_list(items: list[str]) -> str:
    return ", ".join("'" + item.replace("'", "''") + "'" for item in items)


def main() -> None:
    args = parse_args()
    players = args.player
    player_ids = args.player_id
    con = duckdb.connect(str(args.db), read_only=True)
    player_list = sql_list(players)
    player_id_clause = ""
    if player_ids:
        quoted_ids = ", ".join("'" + str(pid).replace("'", "''") + "'" for pid in player_ids)
        player_id_clause = f" OR CAST(p.player_id AS VARCHAR) IN ({quoted_ids})"
    q = f"""
with span as (
  select
    p.player_id,
    any_value(p.player_name) as player_name,
    count(*) as games,
    sum(p.minutes) as minutes,
    sum(p.on_possessions) as on_possessions,
    sum(p.off_possessions) as off_possessions,
    sum(p.plus_minus_actual) as plus_minus_total,
    sum(p.plus_minus_adjusted) as plus_minus_adjusted_total,
    sum((p.team_pts_actual - p.opp_pts_actual) - p.plus_minus_actual) as off_diff_total,
    sum(p.plus_minus_actual) as on_diff_total,
    sum(p.on_pts_for) as on_pts_for,
    sum(p.on_pts_against) as on_pts_against,
    sum(p.off_pts_for) as off_pts_for,
    sum(p.off_pts_against) as off_pts_against
  from player_game_facts p
  where (p.player_name in ({player_list}){player_id_clause})
    and p.season between '{args.season_start}' and '{args.season_end}'
  group by 1
)
select
  player_id,
  player_name,
  games,
  round(minutes, 2) as minutes,
  round(on_possessions, 0) as on_possessions,
  round(off_possessions, 0) as off_possessions,
  round(plus_minus_total * 48.0 / nullif(minutes, 0), 3) as on_court_per48,
  round(off_diff_total * 48.0 / nullif(greatest(5.0 * games * 48.0 - minutes, 0), 0), 3) as off_court_per48_est,
  round((plus_minus_total * 48.0 / nullif(minutes, 0)) - (off_diff_total * 48.0 / nullif(greatest(5.0 * games * 48.0 - minutes, 0), 0)), 3) as onoff_per48,
  round(100.0 * on_diff_total / nullif(on_possessions, 0), 3) as on_net_per100,
  round(100.0 * off_diff_total / nullif(off_possessions, 0), 3) as off_net_per100,
  round(100.0 * (on_diff_total / nullif(on_possessions, 0) - off_diff_total / nullif(off_possessions, 0)), 3) as onoff_per100
from span
order by player_name
"""
    print(con.execute(q).fetchdf().to_string(index=False))
    q_full = f"""
with player_games as (
  select
    p.season,
    p.game_id,
    p.team_id,
    p.player_id,
    any_value(p.player_name) as player_name,
    max(p.minutes) as on_minutes,
    max(p.plus_minus_actual) as on_diff,
    max(p.on_possessions) as on_possessions,
    max(p.team_possessions) as team_possessions,
    max(p.team_pts_actual) as team_pts_actual,
    max(p.opp_pts_actual) as opp_pts_actual
  from player_game_facts p
  where (p.player_name in ({player_list}){player_id_clause})
    and p.season between '{args.season_start}' and '{args.season_end}'
  group by 1,2,3,4
),
player_teams as (
  select distinct season, team_id, player_id, player_name
  from player_games
),
team_games as (
  select distinct season, game_id, team_id, team_pts_actual, opp_pts_actual, team_possessions
  from player_game_facts
),
team_game_minutes as (
  select season, game_id, team_id, sum(minutes) / 5.0 as team_minutes
  from player_game_facts
  group by 1,2,3
),
season_all as (
  select
    pt.player_id,
    any_value(pt.player_name) as player_name,
    pt.season,
    count(case when pg.game_id is not null then 1 end) as games_played,
    count(*) as team_games,
    sum(coalesce(pg.on_minutes, 0.0)) as on_minutes,
    sum(case when pg.game_id is null then tgm.team_minutes else tgm.team_minutes - pg.on_minutes end) as off_minutes_all,
    sum(coalesce(pg.on_possessions, 0.0)) as on_possessions,
    sum(case when pg.game_id is null then tg.team_possessions else tg.team_possessions - pg.on_possessions end) as off_possessions_all,
    sum(coalesce(pg.on_diff, 0.0)) as on_diff_total,
    sum(case when pg.game_id is null then tg.team_pts_actual - tg.opp_pts_actual else (tg.team_pts_actual - tg.opp_pts_actual) - pg.on_diff end) as off_diff_all
  from player_teams pt
  join team_games tg
    on tg.season = pt.season
   and tg.team_id = pt.team_id
  join team_game_minutes tgm
    on tgm.season = tg.season
   and tgm.game_id = tg.game_id
   and tgm.team_id = tg.team_id
  left join player_games pg
    on pg.season = tg.season
   and pg.team_id = tg.team_id
   and pg.game_id = tg.game_id
   and pg.player_id = pt.player_id
  group by 1,3
)
select
  player_id,
  player_name,
  season,
  games_played,
  team_games,
  round(on_minutes, 1) as on_minutes,
  round(off_minutes_all, 1) as off_minutes_all,
  round(on_possessions, 0) as on_possessions,
  round(off_possessions_all, 0) as off_possessions_all,
  round(100.0 * on_diff_total / nullif(on_possessions, 0), 3) as on_net_per100,
  round(100.0 * off_diff_all / nullif(off_possessions_all, 0), 3) as off_net_full_season_per100,
  round(
    100.0 * on_diff_total / nullif(on_possessions, 0)
    - 100.0 * off_diff_all / nullif(off_possessions_all, 0),
    3
  ) as onoff_full_season_per100
from season_all
order by player_id, season
"""
    print()
    print("Full-season off-court sample:")
    print(con.execute(q_full).fetchdf().to_string(index=False))
    if args.show_seasons:
        q2 = f"""
select
  p.player_id,
  any_value(p.player_name) as player_name,
  p.season,
  count(*) as games,
  round(sum(p.minutes), 1) as minutes,
  round(sum(p.on_possessions), 0) as on_possessions,
  round(sum(p.off_possessions), 0) as off_possessions,
  round(100.0 * sum(p.plus_minus_actual) / nullif(sum(p.on_possessions), 0), 3) as on_net_per100,
  round(100.0 * sum((p.team_pts_actual - p.opp_pts_actual) - p.plus_minus_actual) / nullif(sum(p.off_possessions), 0), 3) as off_net_per100,
  round(
    100.0 * sum(p.plus_minus_actual) / nullif(sum(p.on_possessions), 0)
    - 100.0 * sum((p.team_pts_actual - p.opp_pts_actual) - p.plus_minus_actual) / nullif(sum(p.off_possessions), 0),
    3
  ) as onoff_per100
from player_game_facts p
where (p.player_name in ({player_list}){player_id_clause})
  and p.season between '{args.season_start}' and '{args.season_end}'
group by 1,3
order by player_id, season
"""
        print()
        print(con.execute(q2).fetchdf().to_string(index=False))


if __name__ == "__main__":
    main()
