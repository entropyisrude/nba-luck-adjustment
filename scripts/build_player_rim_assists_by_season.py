from __future__ import annotations

import csv
import sqlite3
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
SQLITE_PATH = Path("/mnt/c/users/dave/Downloads/nba-boxscore-data/kaggle-basketball/nba.sqlite")
RAW_2012_PBP_PATH = Path("/mnt/c/Users/Dave/Downloads/nba_pbp_2012_13.csv")
REG_DB = DATA_DIR / "nba_analytics.duckdb"
PLAYOFF_DB = DATA_DIR / "nba_analytics_playoffs.duckdb"
OUT_PATH = DATA_DIR / "player_rim_assists_by_season.csv"
RECENT_CACHE_DIR = DATA_DIR / "rim_assist_recent_cache"
V3_GLOB_ROOT = Path("/mnt/c/users/dave/Downloads/nba-3pt-adjust-local-backups/untracked_20260312_081227")
RIM_SHOTDISTANCE_MAX_FEET = 5.0
OTHER_RIM_DISTANCE_MAX = 30.0


def season_type_from_id(season_id: str) -> str | None:
    code = str(season_id)[0]
    return {
        "2": "Regular Season",
        "4": "Playoffs",
        "1": "Preseason",
    }.get(code)


def season_label_from_id(season_id: str) -> str:
    start = int(str(season_id)[1:5])
    return f"{start}-{str(start + 1)[-2:]}"


def season_label_from_start_year(start_year: int) -> str:
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def season_games(db_path: Path, season_type: str) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame(columns=["season", "player_id", "season_games", "season_type"])
    con = duckdb.connect(str(db_path), read_only=True)
    df = con.execute(
        """
        SELECT season, CAST(player_id AS BIGINT) AS player_id, COUNT(*) AS season_games
        FROM player_game_facts
        GROUP BY 1, 2
        """
    ).df()
    con.close()
    df["season_type"] = season_type
    return df


def find_v3_file(start_year: int) -> Path | None:
    matches = sorted(V3_GLOB_ROOT.glob(f"**/nbastatsv3_{start_year}.csv"))
    return matches[0] if matches else None


def load_v3_events(path: Path, wanted_keys: set[tuple[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    events: dict[tuple[str, str], dict[str, str]] = {}
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = str(row["gameId"]).zfill(10)
            action = str(row["actionNumber"])
            key = (gid, action)
            if key in wanted_keys:
                events[key] = row
    return events


def classify_rim(v3: dict[str, str]) -> tuple[int, int, int]:
    subtype = str(v3.get("subType") or "")
    if "Layup" in subtype:
        return 1, 0, 0
    if "Dunk" in subtype or "DUNK" in subtype:
        return 0, 1, 0
    try:
        shot_distance = float(v3.get("shotDistance") or "")
    except Exception:
        shot_distance = None
    if shot_distance is not None:
        return (0, 0, 1) if shot_distance <= RIM_SHOTDISTANCE_MAX_FEET else (0, 0, 0)
    try:
        x = float(v3.get("xLegacy") or "")
        y = float(v3.get("yLegacy") or "")
        dist = (x * x + y * y) ** 0.5
    except Exception:
        dist = None
    if dist is not None and dist <= OTHER_RIM_DISTANCE_MAX:
        return 0, 0, 1
    return 0, 0, 0


def build_historical_regular_season_joined() -> pd.DataFrame:
    conn = sqlite3.connect(SQLITE_PATH)
    season_ids = [f"2{year}" for year in range(1996, 2023)]
    placeholders = ",".join(["?"] * len(season_ids))
    query = f"""
    SELECT
        g.season_id AS season_id,
        pbp.game_id AS game_id,
        pbp.eventnum AS eventnum,
        pbp.player1_id AS shooter_id,
        pbp.player1_name AS shooter_name,
        pbp.player2_id AS player_id,
        COALESCE(NULLIF(pbp.player2_name, ''), 'Unknown') AS player_name,
        COALESCE(pbp.homedescription, pbp.visitordescription, '') AS description
    FROM play_by_play pbp
    JOIN game g
      ON pbp.game_id = g.game_id
    WHERE g.season_id IN ({placeholders})
      AND pbp.eventmsgtype = 1
      AND pbp.player2_id IS NOT NULL
      AND CAST(pbp.player2_id AS VARCHAR) != '0'
      AND COALESCE(pbp.homedescription, pbp.visitordescription, '') NOT LIKE '%3PT%'
    ORDER BY g.season_id, pbp.game_id, pbp.eventnum
    """
    df = pd.read_sql_query(query, conn, params=season_ids)
    conn.close()

    if RAW_2012_PBP_PATH.exists():
        raw_2012 = pd.read_csv(
            RAW_2012_PBP_PATH,
            usecols=[
                "GAME_ID",
                "EVENTNUM",
                "EVENTMSGTYPE",
                "PLAYER2_ID",
                "PLAYER2_NAME",
                "HOMEDESCRIPTION",
                "VISITORDESCRIPTION",
            ],
            dtype=str,
        )
        raw_2012 = raw_2012[
            (raw_2012["EVENTMSGTYPE"] == "1")
            & raw_2012["PLAYER2_ID"].notna()
            & (raw_2012["PLAYER2_ID"] != "0")
        ].copy()
        raw_2012["description"] = (
            raw_2012["HOMEDESCRIPTION"].fillna("").where(raw_2012["HOMEDESCRIPTION"].fillna("") != "", raw_2012["VISITORDESCRIPTION"].fillna(""))
        )
        raw_2012 = raw_2012[~raw_2012["description"].str.contains("3PT", na=False)].copy()
        raw_2012 = raw_2012.rename(
            columns={
                "GAME_ID": "game_id",
                "EVENTNUM": "eventnum",
                "PLAYER2_ID": "player_id",
                "PLAYER2_NAME": "player_name",
            }
        )
        raw_2012["season_id"] = "22012"
        raw_2012["shooter_id"] = pd.NA
        raw_2012["shooter_name"] = pd.NA
        raw_2012 = raw_2012[
            ["season_id", "game_id", "eventnum", "shooter_id", "shooter_name", "player_id", "player_name", "description"]
        ]
        df = pd.concat([df, raw_2012], ignore_index=True)

    df["season_id"] = df["season_id"].astype(str)
    counts: dict[tuple[str, int, str], dict[str, int]] = defaultdict(lambda: {"layup": 0, "dunk": 0, "other": 0})

    for season_id, season_df in df.groupby("season_id", sort=True):
        start_year = int(season_id[1:5])
        v3_path = find_v3_file(start_year)
        if v3_path is None:
            continue
        season_df = season_df.copy()
        season_df["game_id"] = season_df["game_id"].astype(str).str.zfill(10)
        season_df["eventnum"] = season_df["eventnum"].astype(str)
        wanted_keys = set(zip(season_df["game_id"], season_df["eventnum"]))
        v3_events = load_v3_events(v3_path, wanted_keys)
        season_label = season_label_from_start_year(start_year)

        for row in season_df.itertuples(index=False):
            key = (str(row.game_id).zfill(10), str(row.eventnum))
            v3 = v3_events.get(key)
            if not v3:
                continue
            layup, dunk, other = classify_rim(v3)
            if layup + dunk + other <= 0:
                continue
            player_id = int(row.player_id)
            player_name = str(row.player_name or "")
            out_key = (season_label, player_id, player_name)
            counts[out_key]["layup"] += layup
            counts[out_key]["dunk"] += dunk
            counts[out_key]["other"] += other

    rows = []
    for (season, player_id, player_name), c in counts.items():
        strict_total = c["layup"] + c["dunk"]
        total = strict_total + c["other"]
        rows.append(
            {
                "season": season,
                "season_type": "Regular Season",
                "player_id": player_id,
                "player_name": player_name,
                "layup_assists_created": c["layup"],
                "dunk_assists_created": c["dunk"],
                "other_rim_assists_created": c["other"],
                "rim_assists_strict": strict_total,
                "rim_assists_all": total,
            }
        )
    return pd.DataFrame(rows)


def build_historical_playoffs_strict() -> pd.DataFrame:
    conn = sqlite3.connect(SQLITE_PATH)
    query = """
    SELECT
        g.season_id AS season_id,
        pbp.player2_id AS player_id,
        COALESCE(NULLIF(pbp.player2_name, ''), 'Unknown') AS player_name,
        SUM(
            CASE
                WHEN (pbp.homedescription LIKE '%Layup%' OR pbp.visitordescription LIKE '%Layup%')
                THEN 1 ELSE 0
            END
        ) AS layup_assists_created,
        SUM(
            CASE
                WHEN (pbp.homedescription LIKE '%Dunk%' OR pbp.visitordescription LIKE '%Dunk%')
                THEN 1 ELSE 0
            END
        ) AS dunk_assists_created
    FROM play_by_play pbp
    JOIN game g
      ON pbp.game_id = g.game_id
    WHERE pbp.eventmsgtype = 1
      AND CAST(g.season_id AS VARCHAR) LIKE '4%'
      AND pbp.player2_id IS NOT NULL
      AND CAST(pbp.player2_id AS VARCHAR) != '0'
      AND (
        pbp.homedescription LIKE '%Layup%' OR pbp.visitordescription LIKE '%Layup%'
        OR pbp.homedescription LIKE '%Dunk%' OR pbp.visitordescription LIKE '%Dunk%'
      )
    GROUP BY 1, 2, 3
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    df["season_id"] = df["season_id"].astype(str)
    df["season"] = df["season_id"].map(season_label_from_id)
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    df["layup_assists_created"] = pd.to_numeric(df["layup_assists_created"], errors="coerce").fillna(0).astype(int)
    df["dunk_assists_created"] = pd.to_numeric(df["dunk_assists_created"], errors="coerce").fillna(0).astype(int)
    df["other_rim_assists_created"] = 0
    df["rim_assists_strict"] = df["layup_assists_created"] + df["dunk_assists_created"]
    df["rim_assists_all"] = df["rim_assists_strict"]
    df["season_type"] = "Playoffs"
    return df[
        [
            "season",
            "season_type",
            "player_id",
            "player_name",
            "layup_assists_created",
            "dunk_assists_created",
            "other_rim_assists_created",
            "rim_assists_strict",
            "rim_assists_all",
        ]
    ]


def overlay_recent_cache(base: pd.DataFrame) -> pd.DataFrame:
    cache_paths = sorted(RECENT_CACHE_DIR.glob("*.csv"))
    if not cache_paths:
        return base
    cached = []
    for path in cache_paths:
        df = pd.read_csv(path)
        if not df.empty:
            cached.append(df)
    if not cached:
        return base
    recent = pd.concat(cached, ignore_index=True)
    drop_mask = (
        base["season_type"].isin(recent["season_type"].unique())
        & base["season"].isin(recent["season"].unique())
    )
    merged = pd.concat([base.loc[~drop_mask].copy(), recent], ignore_index=True)
    return merged


def add_per_game_columns(df: pd.DataFrame) -> pd.DataFrame:
    drop_existing = [
        "season_games",
        "layup_assists_created_per_game",
        "dunk_assists_created_per_game",
        "other_rim_assists_created_per_game",
        "rim_assists_strict_per_game",
        "rim_assists_all_per_game",
    ]
    df = df.drop(columns=[c for c in drop_existing if c in df.columns], errors="ignore")
    games = pd.concat(
        [
            season_games(REG_DB, "Regular Season"),
            season_games(PLAYOFF_DB, "Playoffs"),
        ],
        ignore_index=True,
    )
    out = df.merge(games, how="left", on=["season", "player_id", "season_type"])
    out["season_games"] = pd.to_numeric(out["season_games"], errors="coerce")
    out["layup_assists_created_per_game"] = out["layup_assists_created"] / out["season_games"]
    out["dunk_assists_created_per_game"] = out["dunk_assists_created"] / out["season_games"]
    out["other_rim_assists_created_per_game"] = out["other_rim_assists_created"] / out["season_games"]
    out["rim_assists_strict_per_game"] = out["rim_assists_strict"] / out["season_games"]
    out["rim_assists_all_per_game"] = out["rim_assists_all"] / out["season_games"]
    return out


def main() -> None:
    combined = pd.concat(
        [
            build_historical_regular_season_joined(),
            build_historical_playoffs_strict(),
        ],
        ignore_index=True,
    )
    combined = overlay_recent_cache(combined)
    combined = add_per_game_columns(combined)
    combined = combined[
        [
            "season",
            "season_type",
            "player_id",
            "player_name",
            "season_games",
            "layup_assists_created",
            "dunk_assists_created",
            "other_rim_assists_created",
            "rim_assists_strict",
            "rim_assists_all",
            "layup_assists_created_per_game",
            "dunk_assists_created_per_game",
            "other_rim_assists_created_per_game",
            "rim_assists_strict_per_game",
            "rim_assists_all_per_game",
        ]
    ].sort_values(["season_type", "season", "rim_assists_all", "player_name"], ascending=[True, True, False, True])
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUT_PATH, index=False)
    print(f"Wrote {len(combined)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
