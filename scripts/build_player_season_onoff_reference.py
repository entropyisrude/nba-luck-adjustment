from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


DEFAULT_DB = Path(r"C:\Users\Dave\Downloads\nba-onoff-publish\data\nba_analytics.duckdb")
DEFAULT_OUTPUT = Path(r"C:\Users\Dave\Downloads\nba-onoff-publish\data\player_season_onoff_reference.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build season-level player on/off reference metrics from nba_analytics.duckdb"
    )
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Path to nba_analytics.duckdb")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="CSV output path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    out_path = Path(args.output)
    con = duckdb.connect(str(db_path), read_only=True)

    query = """
with player_games as (
    select
        season,
        game_id,
        date,
        team_id,
        team_abbr,
        player_id,
        player_name,
        minutes,
        plus_minus_actual,
        plus_minus_adjusted,
        on_possessions,
        team_possessions,
        team_pts_actual,
        opp_pts_actual
    from player_game_facts
),
team_games as (
    select distinct
        season,
        game_id,
        date,
        team_id,
        team_abbr,
        team_possessions,
        team_pts_actual,
        opp_pts_actual
    from player_game_facts
),
team_game_minutes as (
    select
        season,
        game_id,
        team_id,
        max(team_minutes) as team_minutes
    from (
        select
            season,
            game_id,
            team_id,
            sum(minutes) / 5.0 as team_minutes
        from player_game_facts
        group by 1,2,3
    )
    group by 1,2,3
),
season_played as (
    select
        pg.season,
        pg.team_id,
        any_value(pg.team_abbr) as team_abbr,
        pg.player_id,
        any_value(pg.player_name) as player_name,
        count(*) as games_played,
        sum(pg.minutes) as on_minutes,
        sum(pg.plus_minus_actual) as on_diff_total,
        sum(pg.plus_minus_adjusted) as on_diff_adj_total,
        sum(pg.on_possessions) as on_possessions,
        sum(pg.team_possessions - pg.on_possessions) as off_possessions_played_games,
        sum((pg.team_pts_actual - pg.opp_pts_actual) - pg.plus_minus_actual) as off_diff_played_games,
        sum(tgm.team_minutes - pg.minutes) as off_minutes_played_games,
        min(pg.date) as first_game_date,
        max(pg.date) as last_game_date
    from player_games pg
    join team_game_minutes tgm
      using (season, game_id, team_id)
    group by 1,2,4
),
season_all_off as (
    select
        sp.season,
        sp.team_id,
        sp.player_id,
        count(*) as team_games,
        sum(case when pg.game_id is null then tg.team_possessions else tg.team_possessions - pg.on_possessions end) as off_possessions_all_games,
        sum(case when pg.game_id is null then tg.team_pts_actual - tg.opp_pts_actual else (tg.team_pts_actual - tg.opp_pts_actual) - pg.plus_minus_actual end) as off_diff_all_games,
        sum(case when pg.game_id is null then tgm.team_minutes else tgm.team_minutes - pg.minutes end) as off_minutes_all_games
    from season_played sp
    join team_games tg
      on tg.season = sp.season
     and tg.team_id = sp.team_id
    join team_game_minutes tgm
      on tgm.season = tg.season
     and tgm.game_id = tg.game_id
     and tgm.team_id = tg.team_id
    left join player_games pg
      on pg.season = tg.season
     and pg.team_id = tg.team_id
     and pg.game_id = tg.game_id
     and pg.player_id = sp.player_id
    group by 1,2,3
)
select
    sp.season,
    sp.team_id,
    sp.team_abbr,
    sp.player_id,
    sp.player_name,
    sp.games_played,
    sao.team_games,
    sp.first_game_date,
    sp.last_game_date,
    cast(sp.on_minutes as double) as on_minutes,
    cast(sp.off_minutes_played_games as double) as off_minutes_played_games,
    cast(sao.off_minutes_all_games as double) as off_minutes_all_games,
    cast(sp.on_possessions as double) as on_possessions,
    cast(sp.off_possessions_played_games as double) as off_possessions_played_games,
    cast(sao.off_possessions_all_games as double) as off_possessions_all_games,
    cast(sp.on_diff_total as double) as on_diff_total,
    cast(sp.off_diff_played_games as double) as off_diff_played_games,
    cast(sao.off_diff_all_games as double) as off_diff_all_games,
    case
        when sp.on_possessions > 0 then 100.0 * sp.on_diff_total / sp.on_possessions
        else null
    end as on_court_per100,
    case
        when sp.on_possessions > 0 and sp.off_possessions_played_games > 0
        then 100.0 * sp.on_diff_total / sp.on_possessions
           - 100.0 * sp.off_diff_played_games / sp.off_possessions_played_games
        else null
    end as onoff_played_games_per100,
    case
        when sp.on_possessions > 0 and sao.off_possessions_all_games > 0
        then 100.0 * sp.on_diff_total / sp.on_possessions
           - 100.0 * sao.off_diff_all_games / sao.off_possessions_all_games
        else null
    end as onoff_full_season_per100,
    case
        when sp.on_minutes > 0 then 48.0 * sp.on_diff_total / sp.on_minutes
        else null
    end as on_court_per48,
    case
        when sp.on_minutes > 0 and sao.off_minutes_all_games > 0
        then 48.0 * sp.on_diff_total / sp.on_minutes
           - 48.0 * sao.off_diff_all_games / sao.off_minutes_all_games
        else null
    end as onoff_full_season_per48,
    case
        when sp.on_minutes > 0 then 48.0 * sp.on_possessions / sp.on_minutes
        else null
    end as implied_on_pace_per48,
    case
        when sao.off_minutes_all_games > 0 then 48.0 * sao.off_possessions_all_games / sao.off_minutes_all_games
        else null
    end as implied_off_pace_per48
from season_played sp
join season_all_off sao
  using (season, team_id, player_id)
order by season, team_abbr, player_name
"""

    df = con.execute(query).fetchdf()
    if not df.empty:
        df["possession_quality_flag"] = (
            (df["implied_on_pace_per48"] < 85)
            | (df["implied_on_pace_per48"] > 110)
            | (df["implied_off_pace_per48"] < 85)
            | (df["implied_off_pace_per48"] > 110)
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} ({len(df):,} rows)")


if __name__ == "__main__":
    main()
