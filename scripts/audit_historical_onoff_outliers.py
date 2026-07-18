from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


DEFAULT_DB = Path(r"C:\Users\Dave\Downloads\nba-onoff-publish\data\nba_analytics.duckdb")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show game-level on/off outliers for a player from the analytics DB")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Path to nba_analytics.duckdb")
    parser.add_argument("--player-id", required=True, help="Player id")
    parser.add_argument("--season-start", default="2015-16")
    parser.add_argument("--season-end", default="2025-26")
    parser.add_argument("--limit", type=int, default=20)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    con = duckdb.connect(str(args.db), read_only=True)
    q = f"""
select
  player_id,
  player_name,
  season,
  date,
  game_id,
  round(minutes, 2) as minutes,
  round(on_possessions, 0) as on_possessions,
  round(off_possessions, 0) as off_possessions,
  round(100.0 * (on_pts_for - on_pts_against) / nullif(on_possessions, 0), 3) as on_net_per100,
  round(100.0 * (off_pts_for - off_pts_against) / nullif(off_possessions, 0), 3) as off_net_per100,
  round(on_off_actual_per100, 3) as on_off_actual_per100,
  round(plus_minus_actual, 1) as plus_minus_actual
from player_game_facts
where cast(player_id as varchar) = '{str(args.player_id).replace("'", "''")}'
  and season between '{args.season_start}' and '{args.season_end}'
order by abs(on_off_actual_per100) desc, date desc
limit {int(args.limit)}
"""
    print(con.execute(q).fetchdf().to_string(index=False))


if __name__ == "__main__":
    main()
