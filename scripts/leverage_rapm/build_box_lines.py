"""
Build per-player, per-bucket (playoff / regular) OFFENSE-ONLY box rate-stat
lines from data/kaggle_temp/PlayByPlay.parquet.

Offense-only because we don't have reliable steals/blocks for most of the
window, which would leave DRB as the only defensive signal -- not enough to
say anything meaningful about defense, so this only extracts the offensive
side (scoring, assists, offensive rebounds).

The parquet mixes two event schemas by era, consistent within a game:
  - "new" (~2019-20+): actionType in ('2pt','3pt','freethrow'), shotResult
    and assistPersonId populated directly.
  - "old" (1996-2019/2020): actionType in ('Made Shot','Missed Shot',
    'Free Throw'). shotResult populated for FG, but EMPTY for free throws
    (make/miss there comes from a 'MISS ' description prefix instead).
    Assist has no structured ID field -- it's embedded in description
    text like "Barros Jump Shot (24 PTS) (Wesley 6 AST)" and has to be
    resolved to a player_id via that game's own roster (last names are
    unique enough within a single game's ~20-25 participants; ambiguous
    ones are dropped rather than guessed).

Rebound events carry CUMULATIVE in-game offensive/defensive running totals,
not a per-event flag (verified against real data) -- so identifying whether
a specific rebound was offensive requires diffing against that same
player's previous rebound total in that game (a LAG window function).

Output: data/leverage_rapm/box_lines.csv
  columns: player_id, bucket, pts, fga, fgm, fg3a, fg3m, fta, ftm,
           ast, orb, unassisted_fgm, ts_pct, unassisted_fg_pct
"""

import re
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PBP_PATH = ROOT / "data" / "kaggle_temp" / "PlayByPlay.parquet"
OUT_PATH = ROOT / "data" / "leverage_rapm" / "box_lines.csv"

BUCKET_EXPR = "case when g.game_id is not null then 'playoff' else 'regular' end"

AST_TEXT_RE = re.compile(r"\(([A-Za-z.' ]+?) (\d+) AST\)")


def connect():
    con = duckdb.connect()
    con.execute(f"create view pbp as select * from read_parquet('{PBP_PATH.as_posix()}')")
    con.execute(f"attach '{ROOT / 'data' / 'nba_analytics_playoffs.duckdb'}' as po (read_only)")
    con.execute(
        "create table playoff_games as "
        "select distinct cast(game_id as varchar) as game_id from po.raw_playoff_stints"
    )
    return con


def build_shot_events(con) -> pd.DataFrame:
    """One row per shot attempt, both schemas unified, with bucket already computed."""
    q = f"""
        with e as (
            select gameId as game_id, personId as player_id, period, clock, scoreHome, scoreAway,
                   (actionType = '3pt') as is_three,
                   (coalesce(shotResult ilike '%made%', false)) as made,
                   assistPersonId as new_assist_id,
                   cast(null as varchar) as ast_text,
                   false as is_old_schema
            from pbp where actionType in ('2pt', '3pt')

            union all

            select gameId, personId, period, clock, scoreHome, scoreAway,
                   (coalesce(shotValue = 3, false)) as is_three,
                   (coalesce(shotResult ilike '%made%', false)) as made,
                   cast(null as varchar) as new_assist_id,
                   description as ast_text,
                   true as is_old_schema
            from pbp where actionType in ('Made Shot', 'Missed Shot')
        )
        select e.*, {BUCKET_EXPR} as bucket
        from e
        left join playoff_games g on g.game_id = e.game_id
    """
    return con.execute(q).df()


def build_ft_agg(con) -> pd.DataFrame:
    q = f"""
        with e as (
            select gameId as game_id, personId as player_id, period, clock, scoreHome, scoreAway,
                   (coalesce(shotResult ilike '%made%', false)) as made
            from pbp where actionType = 'freethrow'

            union all

            select gameId, personId, period, clock, scoreHome, scoreAway,
                   (description not ilike 'MISS %') as made
            from pbp where actionType = 'Free Throw'
        )
        select e.player_id, {BUCKET_EXPR} as bucket,
               count(*) as fta,
               sum(case when e.made then 1 else 0 end) as ftm
        from e
        left join playoff_games g on g.game_id = e.game_id
        group by 1, 2
    """
    return con.execute(q).df()


def build_orb_agg(con) -> pd.DataFrame:
    """
    Count OFFENSIVE rebound events per player per bucket. Both schemas expose
    reboundOffensiveTotal/reboundDefensiveTotal as CUMULATIVE in-game running
    totals per player, not a per-event flag -- so whether a specific rebound
    event was offensive is determined by whether reboundOffensiveTotal
    increased relative to that same player's previous rebound event in that
    game (a LAG window, ordered by actionNumber, partitioned by game+player).
    Team rebounds (dead-ball out-of-bounds etc.) show up with a team id /
    blank name instead of a player and are filtered out.
    """
    q = f"""
        with reb_events as (
            -- reboundOffensiveTotal is populated for the new schema; for the old
            -- schema it's null (same pattern as assists) and has to be parsed out
            -- of the "(Off:X Def:Y)" description text instead.
            select gameId as game_id, personId as player_id, actionNumber,
                   coalesce(
                       reboundOffensiveTotal,
                       cast(nullif(regexp_extract(description, 'Off:(\\d+)', 1), '') as integer)
                   ) as off_total
            from pbp
            where actionType in ('rebound', 'Rebound')
              and personId is not null and personId not in ('', '0')
              and playerName is not null and playerName != ''
        ),
        diffed as (
            select game_id, player_id,
                   off_total - coalesce(
                       lag(off_total) over (partition by game_id, player_id order by actionNumber),
                       0
                   ) > 0 as is_oreb
            from reb_events
        )
        select d.player_id, {BUCKET_EXPR} as bucket,
               sum(case when d.is_oreb then 1 else 0 end) as orb
        from diffed d
        left join playoff_games g on g.game_id = d.game_id
        group by 1, 2
    """
    return con.execute(q).df()


def build_roster_lookup(con) -> dict:
    """Flat 'game_id|lowercased last name' -> player_id, only where unambiguous within that game."""
    df = con.execute("""
        select distinct gameId as game_id, personId as player_id, lower(trim(playerName)) as name
        from pbp
        where personId is not null and personId != ''
    """).df()
    grouped = df.groupby(["game_id", "name"])["player_id"].agg(lambda s: list(set(s)))
    unambiguous = grouped[grouped.apply(len) == 1]
    keys = unambiguous.index.get_level_values(0) + "|" + unambiguous.index.get_level_values(1)
    return dict(zip(keys, unambiguous.apply(lambda ids: int(ids[0]))))


def resolve_old_assists(shots: pd.DataFrame, roster: dict) -> tuple[pd.Series, pd.Series]:
    """
    Vectorized: extract assister name from old-schema description text, map via roster lookup.
    Returns (resolved_player_id, had_ast_text) -- the two are kept separate because a shot can
    have assist TEXT (so it should count as 'assisted' for the shooter's own unassisted_fgm)
    even when the name can't be uniquely resolved to a player_id (so it can't be credited as
    a specific player's AST). Conflating the two would misclassify ambiguous-assist shots as
    unassisted.
    """
    mask = shots["is_old_schema"] & shots["made"]
    extracted = shots.loc[mask, "ast_text"].str.extract(AST_TEXT_RE)[0]
    names = extracted.str.strip().str.lower()
    keys = shots.loc[mask, "game_id"] + "|" + names
    resolved = keys.map(roster)

    found_text = names.notna().sum()
    unresolved = names.notna().sum() - resolved.notna().sum()
    print(f"  Old-schema assist text found: {found_text}, unresolved (ambiguous/no roster match): {unresolved}")

    resolved_out = pd.Series(pd.NA, index=shots.index, dtype="object")
    resolved_out.loc[resolved.index] = resolved
    had_text_out = pd.Series(False, index=shots.index)
    had_text_out.loc[names.index] = names.notna()
    return resolved_out, had_text_out


def main():
    con = connect()

    print("Building shot-side events (scanning ~5.5M shot rows)...")
    shots = build_shot_events(con)
    print(f"  {len(shots)} shot events")

    print("Building per-game roster lookup for old-schema assist resolution...")
    roster = build_roster_lookup(con)

    print("Resolving old-schema assist text (vectorized)...")
    shots["old_assist_id"], shots["old_had_ast_text"] = resolve_old_assists(shots, roster)

    print("Aggregating shooter-side box counts...")
    shots["fga"] = 1
    shots["fgm"] = shots["made"].astype(int)
    shots["fg3a"] = shots["is_three"].astype(int)
    shots["fg3m"] = (shots["is_three"] & shots["made"]).astype(int)
    has_new_assist = shots["new_assist_id"].notna() & (shots["new_assist_id"] != "")
    # 'assisted' (for the shooter's own unassisted_fgm) only needs assist TEXT/field presence,
    # not a successfully resolved passer identity -- see resolve_old_assists docstring.
    assisted = has_new_assist | shots["old_had_ast_text"]
    shots["unassisted_fgm"] = (shots["made"] & ~assisted).astype(int)

    shots["player_id"] = shots["player_id"].astype(int)
    shooter_agg = shots.groupby(["player_id", "bucket"]).agg(
        fga=("fga", "sum"), fgm=("fgm", "sum"),
        fg3a=("fg3a", "sum"), fg3m=("fg3m", "sum"),
        unassisted_fgm=("unassisted_fgm", "sum"),
    ).reset_index()

    print("Aggregating assists (new-schema direct + old-schema resolved)...")
    new_assist_rows = shots.loc[has_new_assist, ["new_assist_id", "bucket"]].rename(
        columns={"new_assist_id": "assist_id"}
    )
    new_assist_rows["assist_id"] = new_assist_rows["assist_id"].astype(int)
    old_assist_rows = shots.loc[shots["old_assist_id"].notna(), ["old_assist_id", "bucket"]].rename(
        columns={"old_assist_id": "assist_id"}
    )
    old_assist_rows["assist_id"] = old_assist_rows["assist_id"].astype(int)

    ast_agg = pd.concat([new_assist_rows, old_assist_rows], ignore_index=True)
    ast_agg = ast_agg.groupby(["assist_id", "bucket"]).size().reset_index(name="ast")
    ast_agg = ast_agg.rename(columns={"assist_id": "player_id"})

    print("Aggregating free throws...")
    ft_agg = build_ft_agg(con)
    ft_agg["player_id"] = ft_agg["player_id"].astype(int)

    print("Aggregating offensive rebounds...")
    orb_agg = build_orb_agg(con)
    orb_agg["player_id"] = orb_agg["player_id"].astype(int)

    box = shooter_agg.merge(ft_agg, on=["player_id", "bucket"], how="outer")
    box = box.merge(ast_agg, on=["player_id", "bucket"], how="outer")
    box = box.merge(orb_agg, on=["player_id", "bucket"], how="outer")
    box = box.fillna(0)
    for col in ["fga", "fgm", "fg3a", "fg3m", "unassisted_fgm", "fta", "ftm", "ast", "orb"]:
        box[col] = box[col].astype(int)

    box["pts"] = (box["fgm"] - box["fg3m"]) * 2 + box["fg3m"] * 3 + box["ftm"]
    box["ts_pct"] = box["pts"] / (2 * (box["fga"] + 0.44 * box["fta"])).replace(0, pd.NA)
    box["unassisted_fg_pct"] = box["unassisted_fgm"] / box["fgm"].replace(0, pd.NA)

    box = box[box["player_id"] > 0].sort_values(["player_id", "bucket"])
    box.to_csv(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH} ({len(box)} player-bucket rows)")


if __name__ == "__main__":
    main()
