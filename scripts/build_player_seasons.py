#!/usr/bin/env python3
"""
Build player-season data for the Similarity Machine.

Sources:
  - Eoin/Kaggle zip  → 1979-80 through 1995-96 (box stats, no on/off)
  - nba_analytics.duckdb → 1996-97 through present (box stats)
  - pbpstats_cache.json.gz → 1996-97 through present (on/off split)

Output: data/player_seasons.json

Run from project root:
    python scripts/build_player_seasons.py
"""

import gzip
import json
import zipfile
from collections import defaultdict
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
KAGGLE_ZIP = ROOT / "historical-nba-data-and-player-box-scores.zip"
RS_DB = DATA_DIR / "nba_analytics.duckdb"
PBPSTATS_CACHE = DATA_DIR / "pbpstats_cache.json.gz"
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
            # on/off — not available pre-1996
            "oo":     None,
            "oo_off": None,
            "oo_def": None,
        })

    print(f"  Eoin: {len(records):,} player-seasons ({EOIN_FIRST}–{EOIN_LAST})")
    return records


# ---------------------------------------------------------------------------
# pbpstats: on/off split (offensive + defensive) for 1996-97 onwards
# ---------------------------------------------------------------------------

def load_pbpstats_onoff() -> dict[tuple[int, str], dict]:
    """Return {(player_id, season): {oo, oo_off, oo_def}} from pbpstats cache.

    Computes off-court ratings from team totals, then derives:
      oo_off = (team OffRtg on) - (team OffRtg off)
      oo_def = (opp OffRtg off) - (opp OffRtg on)
      oo     = oo_off + oo_def
    Multi-team players are aggregated across stints.
    """
    if not PBPSTATS_CACHE.exists():
        print(f"  WARNING: {PBPSTATS_CACHE} not found — on/off will be None")
        return {}

    with gzip.open(PBPSTATS_CACHE, "rt", encoding="utf-8") as f:
        cache = json.load(f)

    result: dict[tuple[int, str], dict] = {}

    for season, season_data in cache.items():
        players = season_data.get("players", {}).get("multi_row_table_data", [])
        teams = season_data.get("teams", {}).get("multi_row_table_data", [])

        team_by_id: dict[str, dict] = {str(t.get("TeamId", "")): t for t in teams}

        # One player may have multiple stints (trades); collect per-stint data
        stints: dict[int, list[dict]] = defaultdict(list)
        for p in players:
            pid = int(p.get("EntityId", 0))
            team_id = str(p.get("TeamId", ""))
            team = team_by_id.get(team_id)
            if not team:
                continue
            on_off_poss = float(p.get("OffPoss") or 0)
            on_def_poss = float(p.get("DefPoss") or 0)
            if on_off_poss <= 0 or on_def_poss <= 0:
                continue
            on_off_rtg = float(p.get("OnOffRtg") or 0)
            on_def_rtg = float(p.get("OnDefRtg") or 0)
            team_off_poss = float(team.get("OffPoss") or 0)
            team_def_poss = float(team.get("DefPoss") or 0)
            team_pts = float(team.get("Points") or 0)
            team_opp_pts = float(team.get("OpponentPoints") or 0)

            on_pts_scored = on_off_rtg * on_off_poss / 100.0
            on_pts_allowed = on_def_rtg * on_def_poss / 100.0
            stints[pid].append({
                "on_off_poss":   on_off_poss,
                "on_def_poss":   on_def_poss,
                "on_pts_scored": on_pts_scored,
                "on_pts_allowed": on_pts_allowed,
                "off_off_poss":  max(team_off_poss - on_off_poss, 0.0),
                "off_def_poss":  max(team_def_poss - on_def_poss, 0.0),
                "off_pts_scored":  max(team_pts - on_pts_scored, 0.0),
                "off_pts_allowed": max(team_opp_pts - on_pts_allowed, 0.0),
            })

        for pid, player_stints in stints.items():
            tot_on_off  = sum(s["on_off_poss"]    for s in player_stints)
            tot_on_def  = sum(s["on_def_poss"]    for s in player_stints)
            tot_off_off = sum(s["off_off_poss"]   for s in player_stints)
            tot_off_def = sum(s["off_def_poss"]   for s in player_stints)
            if tot_on_off <= 0 or tot_on_def <= 0 or tot_off_off <= 0 or tot_off_def <= 0:
                continue

            on_off_rtg  = sum(s["on_pts_scored"]   for s in player_stints) / tot_on_off  * 100
            on_def_rtg  = sum(s["on_pts_allowed"]  for s in player_stints) / tot_on_def  * 100
            off_off_rtg = sum(s["off_pts_scored"]  for s in player_stints) / tot_off_off * 100
            off_def_rtg = sum(s["off_pts_allowed"] for s in player_stints) / tot_off_def * 100

            oo_off = on_off_rtg  - off_off_rtg
            oo_def = off_def_rtg - on_def_rtg
            result[(pid, season)] = {
                "oo":     r2(oo_off + oo_def),
                "oo_off": r2(oo_off),
                "oo_def": r2(oo_def),
            }

    total = sum(len(v) for v in cache.values())
    print(f"  pbpstats: {len(result):,} player-season on/off entries ({len(cache)} seasons)")
    return result


# ---------------------------------------------------------------------------
# DuckDB: 1996-97 onwards
# ---------------------------------------------------------------------------

def build_db_seasons(pbp_onoff: dict) -> list[dict]:
    print("Querying DuckDB for 1996-97+ …")
    con = duckdb.connect(str(RS_DB), read_only=True)

    query = """
    SELECT
        player_id,
        player_name,
        season,
        mode(team_abbr)                AS team,
        COUNT(*)                       AS games,
        SUM(minutes)                   AS total_min,
        AVG(age)                       AS avg_age,
        AVG(height_inches)             AS avg_height,
        AVG(CAST(starter AS INTEGER))  AS starter_pct,
        SUM(pts)                       AS pts_s,
        SUM(ast)                       AS ast_s,
        SUM(oreb)                      AS oreb_s,
        SUM(dreb)                      AS dreb_s,
        SUM(stl)                       AS stl_s,
        SUM(blk)                       AS blk_s,
        SUM(tov)                       AS tov_s,
        SUM(fta)                       AS fta_s,
        SUM(fg3a)                      AS fg3a_s,
        SUM(fgm)                       AS fgm_s,
        SUM(fga)                       AS fga_s,
        SUM(fg3m)                      AS fg3m_s,
        SUM(ftm)                       AS ftm_s
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
        pid = int(r["player_id"])
        season = r["season"]
        oo_data = pbp_onoff.get((pid, season), {})
        records.append({
            "pid":    pid,
            "name":   r["player_name"],
            "season": season,
            "team":   r["team"] if pd.notna(r["team"]) else None,
            "games":  int(r["games"]),
            "min":    int(m),
            "age":    r2(r["avg_age"]),
            "ht":     int(r["avg_height"]) if pd.notna(r["avg_height"]) else None,
            "spct":   r2(r["starter_pct"]),
            "pts":    per36(r["pts_s"], m),
            "ast":    per36(r["ast_s"], m),
            "or":     per36(r["oreb_s"], m),
            "dr":     per36(r["dreb_s"], m),
            "stl":    per36(r["stl_s"], m),
            "blk":    per36(r["blk_s"], m),
            "tov":    per36(r["tov_s"], m),
            "fta":    per36(r["fta_s"], m),
            "fg3a":   per36(r["fg3a_s"], m),
            "fgp":    pct(r["fgm_s"], r["fga_s"]),
            "fg3p":   pct(r["fg3m_s"], r["fg3a_s"]) if r["fg3a_s"] > 0 else None,
            "ftp":    pct(r["ftm_s"], r["fta_s"]) if r["fta_s"] > 0 else None,
            "oo":     oo_data.get("oo"),
            "oo_off": oo_data.get("oo_off"),
            "oo_def": oo_data.get("oo_def"),
        })

    print(f"  DB:   {len(records):,} player-seasons ({DB_FIRST}–present)")
    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    pbp_onoff = load_pbpstats_onoff()
    eoin = build_eoin_seasons()
    modern = build_db_seasons(pbp_onoff)
    all_seasons = eoin + modern

    # Sort by season then name
    all_seasons.sort(key=lambda r: (r["season"], r["name"]))

    output = {
        "generated": "2026-05-31",
        "source": "Eoin/Kaggle (1979-80–1995-96) + nba_analytics.duckdb (1996-97–present) + pbpstats on/off",
        "min_minutes": MIN_MINUTES,
        "cols": ["pts","ast","or","dr","stl","blk","tov","fta","fg3a",
                 "fgp","fg3p","ftp","oo","oo_off","oo_def"],
        "col_labels": {
            "pts":"Pts/36","ast":"Ast/36","or":"OReb/36","dr":"DReb/36",
            "stl":"Stl/36","blk":"Blk/36","tov":"Tov/36","fta":"FTA/36",
            "fg3a":"3PA/36","fgp":"FG%","fg3p":"3P%","ftp":"FT%",
            "oo":"On/Off","oo_off":"Off On/Off","oo_def":"Def On/Off",
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
    print("\nSample (LeBron 2012-13, Jordan 96-97):")
    sample = df[df["name"].str.contains("LeBron James|Michael Jordan", na=False)][
        ["name","season","team","pts","oo","oo_off","oo_def"]
    ].query("season in ['2012-13','1996-97']")
    print(sample.to_string(index=False))


if __name__ == "__main__":
    main()
