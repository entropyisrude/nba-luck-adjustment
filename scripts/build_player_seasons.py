#!/usr/bin/env python3
"""
Build player-season data for the Similarity Machine.

Sources:
  - Eoin/Kaggle zip  → 1979-80 through 1995-96 (box stats, no on/off)
  - nba_analytics.duckdb → 1996-97 through present (box + on/off)

Output: data/player_seasons.json

Run from project root:
    python scripts/build_player_seasons.py
"""

import json
import zipfile
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
KAGGLE_ZIP = ROOT / "historical-nba-data-and-player-box-scores.zip"
RS_DB = DATA_DIR / "nba_analytics.duckdb"
OUT = DATA_DIR / "player_seasons.json"

MIN_MINUTES = 500
EOIN_FIRST = "1979-80"
EOIN_LAST = "1995-96"
DB_FIRST = "1996-97"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def date_to_season(d) -> str:
    """Convert a date to NBA season string e.g. 1995-96."""
    y, m = d.year, d.month
    if m >= 10:
        return f"{y}-{str(y + 1)[-2:]}"
    return f"{y - 1}-{str(y)[-2:]}"


def r2(x):
    """Round to 2 dp, return None for NaN/inf."""
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return None
    return round(float(x), 2)


def per36(total, minutes):
    if not minutes or minutes == 0:
        return None
    return r2(total * 36.0 / minutes)


def pct(made, attempted):
    if not attempted or attempted == 0:
        return None
    return r2(made / attempted)


# ---------------------------------------------------------------------------
# Eoin: 1979-80 through 1995-96
# ---------------------------------------------------------------------------

def load_team_abbrevs(zf: zipfile.ZipFile) -> dict[int, dict]:
    """Build teamId → {season → abbrev} lookup from TeamHistories.csv."""
    with zf.open("TeamHistories.csv") as f:
        df = pd.read_csv(f)
    lookup: dict[int, list] = {}
    for _, row in df.iterrows():
        tid = int(row["teamId"])
        lookup.setdefault(tid, []).append({
            "abbrev": row["teamAbbrev"],
            "from": int(row["seasonFounded"]),
            "to": int(row["seasonActiveTill"]),
        })
    return lookup


def team_abbrev(team_id: int, season_year: int, lookup: dict) -> str | None:
    """Return abbreviation for teamId in a given season start-year."""
    for entry in lookup.get(int(team_id), []):
        if entry["from"] <= season_year <= entry["to"]:
            return entry["abbrev"]
    return None


def build_eoin_seasons() -> list[dict]:
    print("Loading Eoin PlayerStatistics.csv …")
    with zipfile.ZipFile(KAGGLE_ZIP) as z:
        team_lookup = load_team_abbrevs(z)
        with z.open("PlayerStatistics.csv") as f:
            df = pd.read_csv(f, low_memory=False)

    df = df[df["gameType"] == "Regular Season"].copy()
    df["gameDate"] = pd.to_datetime(df["gameDate"], errors="coerce")
    df = df.dropna(subset=["gameDate"])
    df["season"] = df["gameDate"].apply(date_to_season)
    df = df[(df["season"] >= EOIN_FIRST) & (df["season"] <= EOIN_LAST)]

    num_cols = [
        "numMinutes", "points", "assists", "blocks", "steals",
        "fieldGoalsAttempted", "fieldGoalsMade",
        "threePointersAttempted", "threePointersMade",
        "freeThrowsAttempted", "freeThrowsMade",
        "reboundsDefensive", "reboundsOffensive", "reboundsTotal",
        "turnovers",
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["is_starter"] = df["startingPosition"].notna() & (df["startingPosition"] != "")
    df["player_name"] = (
        df["firstName"].fillna("") + " " + df["lastName"].fillna("")
    ).str.strip()
    df["playerteamId"] = pd.to_numeric(df["playerteamId"], errors="coerce")

    grp = df.groupby(["personId", "player_name", "season"])
    agg = grp.agg(
        games=("gameId", "count"),
        total_min=("numMinutes", "sum"),
        pts=("points", "sum"),
        ast=("assists", "sum"),
        oreb=("reboundsOffensive", "sum"),
        dreb=("reboundsDefensive", "sum"),
        stl=("steals", "sum"),
        blk=("blocks", "sum"),
        tov=("turnovers", "sum"),
        fgm=("fieldGoalsMade", "sum"),
        fga=("fieldGoalsAttempted", "sum"),
        fg3m=("threePointersMade", "sum"),
        fg3a=("threePointersAttempted", "sum"),
        ftm=("freeThrowsMade", "sum"),
        fta=("freeThrowsAttempted", "sum"),
        starter_games=("is_starter", "sum"),
        team_id=("playerteamId", "first"),
    ).reset_index()

    agg = agg[agg["total_min"] >= MIN_MINUTES].copy()

    records = []
    for _, r in agg.iterrows():
        m = r["total_min"]
        season_year = int(r["season"][:4])
        tid = r["team_id"]
        abbrev = None
        if pd.notna(tid):
            abbrev = team_abbrev(int(tid), season_year, team_lookup)

        records.append({
            "pid":   int(r["personId"]),
            "name":  r["player_name"],
            "season": r["season"],
            "team":  abbrev,
            "games": int(r["games"]),
            "min":   int(m),
            "age":   None,
            "ht":    None,
            "spct":  r2(r["starter_games"] / r["games"]) if r["games"] else None,
            # per-36
            "pts":   per36(r["pts"], m),
            "ast":   per36(r["ast"], m),
            "or":    per36(r["oreb"], m),
            "dr":    per36(r["dreb"], m),
            "stl":   per36(r["stl"], m),
            "blk":   per36(r["blk"], m),
            "tov":   per36(r["tov"], m),
            "fta":   per36(r["fta"], m),
            "fg3a":  per36(r["fg3a"], m),
            # shooting pct
            "fgp":   pct(r["fgm"], r["fga"]),
            "fg3p":  pct(r["fg3m"], r["fg3a"]) if r["fg3a"] > 0 else None,
            "ftp":   pct(r["ftm"], r["fta"]) if r["fta"] > 0 else None,
            # on/off — not available
            "oo":    None,
            "ooa":   None,
        })

    print(f"  Eoin: {len(records):,} player-seasons ({EOIN_FIRST}–{EOIN_LAST})")
    return records


# ---------------------------------------------------------------------------
# DuckDB: 1996-97 onwards
# ---------------------------------------------------------------------------

def build_db_seasons() -> list[dict]:
    print("Querying DuckDB for 1996-97+ …")
    con = duckdb.connect(str(RS_DB), read_only=True)

    query = """
    SELECT
        player_id,
        player_name,
        season,
        mode(team_abbr)                                                AS team,
        COUNT(*)                                                       AS games,
        SUM(minutes)                                                   AS total_min,
        AVG(age)                                                       AS avg_age,
        AVG(height_inches)                                             AS avg_height,
        AVG(CAST(starter AS INTEGER))                                  AS starter_pct,
        SUM(pts)                                                       AS pts_s,
        SUM(ast)                                                       AS ast_s,
        SUM(oreb)                                                      AS oreb_s,
        SUM(dreb)                                                      AS dreb_s,
        SUM(stl)                                                       AS stl_s,
        SUM(blk)                                                       AS blk_s,
        SUM(tov)                                                       AS tov_s,
        SUM(fta)                                                       AS fta_s,
        SUM(fg3a)                                                      AS fg3a_s,
        SUM(fgm)                                                       AS fgm_s,
        SUM(fga)                                                       AS fga_s,
        SUM(fg3m)                                                      AS fg3m_s,
        SUM(ftm)                                                       AS ftm_s,
        SUM(on_off_actual    * on_possessions)
            / NULLIF(SUM(on_possessions), 0)                          AS on_off,
        SUM(on_off_adjusted  * on_possessions)
            / NULLIF(SUM(on_possessions), 0)                          AS on_off_adj
    FROM player_game_facts
    WHERE minutes > 0
    GROUP BY player_id, player_name, season
    HAVING SUM(minutes) >= ?
    ORDER BY season, player_name
    """

    df = con.execute(query, [MIN_MINUTES]).df()
    con.close()

    records = []
    for _, r in df.iterrows():
        m = r["total_min"]
        records.append({
            "pid":   int(r["player_id"]),
            "name":  r["player_name"],
            "season": r["season"],
            "team":  r["team"] if pd.notna(r["team"]) else None,
            "games": int(r["games"]),
            "min":   int(m),
            "age":   r2(r["avg_age"]),
            "ht":    int(r["avg_height"]) if pd.notna(r["avg_height"]) else None,
            "spct":  r2(r["starter_pct"]),
            "pts":   per36(r["pts_s"], m),
            "ast":   per36(r["ast_s"], m),
            "or":    per36(r["oreb_s"], m),
            "dr":    per36(r["dreb_s"], m),
            "stl":   per36(r["stl_s"], m),
            "blk":   per36(r["blk_s"], m),
            "tov":   per36(r["tov_s"], m),
            "fta":   per36(r["fta_s"], m),
            "fg3a":  per36(r["fg3a_s"], m),
            "fgp":   pct(r["fgm_s"], r["fga_s"]),
            "fg3p":  pct(r["fg3m_s"], r["fg3a_s"]) if r["fg3a_s"] > 0 else None,
            "ftp":   pct(r["ftm_s"], r["fta_s"]) if r["fta_s"] > 0 else None,
            "oo":    r2(r["on_off"]),
            "ooa":   r2(r["on_off_adj"]),
        })

    print(f"  DB:   {len(records):,} player-seasons ({DB_FIRST}–present)")
    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    eoin = build_eoin_seasons()
    modern = build_db_seasons()
    all_seasons = eoin + modern

    # Sort by season then name
    all_seasons.sort(key=lambda r: (r["season"], r["name"]))

    output = {
        "generated": "2026-05-31",
        "source": "Eoin/Kaggle (1979-80–1995-96) + nba_analytics.duckdb (1996-97–present)",
        "min_minutes": MIN_MINUTES,
        "cols": ["pts","ast","or","dr","stl","blk","tov","fta","fg3a",
                 "fgp","fg3p","ftp","oo","ooa"],
        "col_labels": {
            "pts":"Pts/36","ast":"Ast/36","or":"OReb/36","dr":"DReb/36",
            "stl":"Stl/36","blk":"Blk/36","tov":"Tov/36","fta":"FTA/36",
            "fg3a":"3PA/36","fgp":"FG%","fg3p":"3P%","ftp":"FT%",
            "oo":"On/Off","ooa":"On/Off Adj"
        },
        "seasons": all_seasons,
    }

    OUT.parent.mkdir(exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = OUT.stat().st_size // 1024
    print(f"\nWrote {len(all_seasons):,} player-seasons to {OUT} ({size_kb} KB)")

    # Quick sanity check
    df = pd.DataFrame(all_seasons)
    print("\nSeason range:", df.season.min(), "–", df.season.max())
    print("Players with on/off:", df["oo"].notna().sum())
    print("Players without:    ", df["oo"].isna().sum())
    print("\nSample (Jordan 95-96 + 96-97):")
    jordan = df[df["name"].str.contains("Michael Jordan", na=False)][
        ["name","season","team","pts","ast","oo"]].head(5)
    print(jordan.to_string(index=False))


if __name__ == "__main__":
    main()
