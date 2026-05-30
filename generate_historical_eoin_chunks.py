"""Generate historical chunk files (1979-80 through 1995-96 RS, 1979-80 through 1994-95 PO)
from the Eoin Kaggle dataset (historical-nba-data-and-player-box-scores.zip).

For these pre-pbpstats seasons we have real game-by-game box scores but no on/off data.
All on/off columns are None; hustle, rim-defence, and assist-split columns are also None.
Bio columns (height, draft) are filled where available from the RS or PO DB.

Run:
    python generate_historical_eoin_chunks.py [--force]

    --force  Overwrite existing chunk files (default: skip seasons that already have files).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import zipfile
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parent)))
DATA_DIR = ROOT / "data"
KAGGLE_ZIP = ROOT / "historical-nba-data-and-player-box-scores.zip"

RS_SPAN_DIR = DATA_DIR / "player_span_chunks"
PO_SPAN_DIR = DATA_DIR / "player_span_playoff_chunks"
RS_GAME_DIR = DATA_DIR / "player_game_chunks"
PO_GAME_DIR = DATA_DIR / "player_game_playoff_chunks"

RS_DB = DATA_DIR / "nba_analytics.duckdb"
PO_DB = DATA_DIR / "nba_analytics_playoffs.duckdb"

# Process RS seasons 1979-80 through 1995-96, PO through 1994-95
RS_FIRST = 1979
RS_LAST  = 1995   # last starting year included (1995-96)
PO_FIRST = 1979
PO_LAST  = 1994   # last starting year included (1994-95); 1995-96 PO already exists

ABBREV_OVERRIDES = {
    "SAN": "SAS",
    "NJ":  "NJN",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _season_slug(starting_year: int) -> str:
    return f"{starting_year}_{str(starting_year + 1)[-2:]}"


def _season_str(starting_year: int) -> str:
    return f"{starting_year}-{str(starting_year + 1)[-2:]}"


def _rs_season_year(dt: pd.Timestamp) -> int:
    """Starting year of the RS season a game date belongs to."""
    return dt.year if dt.month >= 10 else dt.year - 1


def _po_season_year(dt: pd.Timestamp) -> int:
    """Starting year of the PO season (all playoff games April-June)."""
    return dt.year - 1


def _pct(m: float, a: float) -> float | None:
    return m / a if a > 0 else None


def _ts(pts: float, fga: float, fta: float) -> float | None:
    denom = 2 * (fga + 0.44 * fta)
    return pts / denom if denom > 0 else None


def _dd(pts: float, reb: float, ast: float, stl: float, blk: float) -> int:
    return int(sum(1 for v in (pts, reb, ast, stl, blk) if v >= 10) >= 2)


def _td(pts: float, reb: float, ast: float, stl: float, blk: float) -> int:
    return int(sum(1 for v in (pts, reb, ast, stl, blk) if v >= 10) >= 3)


# ---------------------------------------------------------------------------
# Load Eoin zip files
# ---------------------------------------------------------------------------

def _load_team_maps(zf: zipfile.ZipFile) -> tuple[dict, dict]:
    """Returns (abbrev_by_id_year, abbrev_by_city_name_year) from TeamHistories.csv."""
    with zf.open("TeamHistories.csv") as f:
        th = pd.read_csv(f, low_memory=False)
    for col in ("teamAbbrev", "teamCity", "teamName", "league"):
        th[col] = th[col].astype(str).str.strip()
    th = th[th["league"].str.lower() == "nba"]
    abbrev_map: dict[tuple[int, int], str] = {}
    name_map: dict[tuple[str, str, int], tuple[int, str]] = {}
    for _, row in th.iterrows():
        try:
            tid_int = int(row["teamId"])
            year_from = int(row["seasonFounded"])
            year_to = int(row["seasonActiveTill"])
        except (TypeError, ValueError):
            continue
        abbr = ABBREV_OVERRIDES.get(str(row["teamAbbrev"]), str(row["teamAbbrev"]))
        city = str(row["teamCity"]).lower()
        name = str(row["teamName"]).lower()
        for y in range(year_from, year_to + 1):
            abbrev_map[(tid_int, y)] = abbr
            name_map[(city, name, y)] = (tid_int, abbr)
    return abbrev_map, name_map


def _load_player_names(zf: zipfile.ZipFile) -> dict[int, str]:
    """Map personId → display name from Players.csv."""
    with zf.open("Players.csv") as f:
        pl = pd.read_csv(f, low_memory=False)
    out: dict[int, str] = {}
    for r in pl.to_dict("records"):
        try:
            pid = int(float(r["personId"]))
        except (TypeError, ValueError, KeyError):
            continue
        first = str(r.get("firstName") or "").strip()
        last = str(r.get("lastName") or "").strip()
        name = f"{first} {last}".strip() if first or last else ""
        if name:
            out[pid] = name
    return out


def _load_player_stats(zf: zipfile.ZipFile) -> pd.DataFrame:
    with zf.open("PlayerStatistics.csv") as f:
        ps = pd.read_csv(f, low_memory=False)
    ps["gameDateTimeEst"] = pd.to_datetime(ps["gameDateTimeEst"], errors="coerce")
    ps = ps.dropna(subset=["gameDateTimeEst"]).copy()
    for col in ("playerteamCity", "playerteamName", "opponentteamCity", "opponentteamName"):
        ps[col] = ps[col].fillna("").str.strip()
    stat_cols = [
        "numMinutes", "points", "reboundsTotal", "reboundsOffensive",
        "reboundsDefensive", "assists", "steals", "blocks", "turnovers",
        "foulsPersonal", "fieldGoalsMade", "fieldGoalsAttempted",
        "threePointersMade", "threePointersAttempted",
        "freeThrowsMade", "freeThrowsAttempted", "plusMinusPoints",
    ]
    for col in stat_cols:
        ps[col] = pd.to_numeric(ps[col], errors="coerce").fillna(0)
    ps["fg2m"] = (ps["fieldGoalsMade"] - ps["threePointersMade"]).clip(lower=0)
    ps["fg2a"] = (ps["fieldGoalsAttempted"] - ps["threePointersAttempted"]).clip(lower=0)
    ps["starter_val"] = ps["startingPosition"].apply(
        lambda x: 1 if (pd.notna(x) and str(x).strip()) else 0
    )
    ps["home_away_str"] = ps["home"].apply(lambda x: "H" if x == 1 else "A")
    ps["win_loss_str"] = ps["win"].apply(lambda x: "W" if x == 1 else "L")
    ps["personId"] = pd.to_numeric(ps["personId"], errors="coerce")
    ps = ps.dropna(subset=["personId"]).copy()
    ps["personId"] = ps["personId"].astype(int)
    ps["gameId"] = ps["gameId"].astype(str)
    return ps


# ---------------------------------------------------------------------------
# Bio from DBs
# ---------------------------------------------------------------------------

def _load_bio(db_path: Path) -> dict[int, dict]:
    if not db_path.exists():
        return {}
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        rows = con.execute("""
            SELECT player_id,
                   MAX(listed_height)      AS listed_height,
                   MAX(height_inches)      AS height_inches,
                   MAX(draft_year)         AS draft_year,
                   MAX(draft_overall_pick) AS draft_overall_pick
            FROM player_game_facts
            WHERE player_id IS NOT NULL
            GROUP BY player_id
        """).fetchall()
        con.close()
        return {
            int(pid): {
                "listed_height": lh, "height_inches": hi,
                "draft_year": dy, "draft_overall_pick": dp,
            }
            for pid, lh, hi, dy, dp in rows
        }
    except Exception as e:
        print(f"  Warning: could not load bio from {db_path.name}: {e}", file=sys.stderr)
        return {}


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------

def _span_row(
    *, date_str: str, season: str, game_id: str, pid: int, name: str,
    team_abbr: str, opp_abbr: str, home_away: str, win_loss: str,
    starter: int, minutes: float, pts: float, reb: float, oreb: float,
    dreb: float, ast: float, stl: float, blk: float, tov: float, pf: float,
    fgm: float, fga: float, fg2m: float, fg2a: float,
    fg3m: float, fg3a: float, ftm: float, fta: float,
    pm: float, bio: dict,
) -> list:
    """Build a 75-element span-search chunk row."""
    return [
        date_str, season, game_id, pid, name,          # 0-4
        team_abbr, opp_abbr, home_away, win_loss, starter,  # 5-9
        minutes, pts, reb, oreb, dreb, ast, stl, blk, tov, pf,  # 10-19
        fgm, fga, fg2m, fg2a, _pct(fg2m, fg2a),        # 20-24
        fg3m, fg3a, _pct(fg3m, fg3a),                  # 25-27
        ftm, fta, _pct(ftm, fta),                      # 28-30
        None, None, None, None, None, None,             # 31-36 assist splits
        bio.get("listed_height"), bio.get("height_inches"),  # 37-38
        None, None,                                    # 39-40 age, career_year
        bio.get("draft_year"), bio.get("draft_overall_pick"),  # 41-42
        None, None, None, None, None, None,            # 43-48 rim assists counts
        None, None, None, None, None,                  # 49-53 rim assists per game
        None, None, None, None, None, None,            # 54-59 rim defense
        None, None, None, None, None, None, None, None, None,  # 60-68 hustle
        None, None,                                    # 69-70 on/off poss
        pm, pm,                                        # 71-72 pm actual/adj
        None, None,                                    # 73-74 on_off actual/adj
    ]


def _game_row(
    *, date_str: str, season: str, game_id: str, pid: int, name: str,
    team_abbr: str, opp_abbr: str, home_away: str, win_loss: str,
    starter: int, minutes: float, pts: float, reb: float, oreb: float,
    dreb: float, ast: float, stl: float, blk: float, tov: float, pf: float,
    fgm: float, fga: float, fg2m: float, fg2a: float,
    fg3m: float, fg3a: float, ftm: float, fta: float,
    pm: float, bio: dict,
) -> list:
    """Build a 56-element game-search chunk row."""
    ts = _ts(pts, fga, fta)
    dd = _dd(pts, reb, ast, stl, blk)
    td = _td(pts, reb, ast, stl, blk)
    return [
        date_str, season, game_id, pid, name,               # 0-4
        team_abbr, opp_abbr,                                # 5-6
        None, None, None,                                   # 7-9 team_pts, opp_pts, margin
        home_away, win_loss, starter, minutes,              # 10-13
        pts, reb, oreb, dreb, ast, stl, blk, tov, pf,      # 14-22
        fgm, fga, fg2m, fg2a, _pct(fg2m, fg2a),            # 23-27
        fg3m, fg3a, _pct(fg3m, fg3a),                      # 28-30
        ftm, fta, _pct(ftm, fta),                          # 31-33
        None, None, None, None, None, None,                 # 34-39 assist splits
        bio.get("listed_height"), bio.get("height_inches"), # 40-41
        None, None,                                         # 42-43 age, career_year
        bio.get("draft_year"), bio.get("draft_overall_pick"),  # 44-45
        None,                                               # 46 on_possessions
        ts, dd, td,                                         # 47-49
        pm, pm, None,                                       # 50-52 pm actual/adj/delta
        None, None, None,                                   # 53-55 on_off actual/adj/delta
    ]


# ---------------------------------------------------------------------------
# Resolve team abbreviation
# ---------------------------------------------------------------------------

def _resolve_abbr(
    team_id_raw: object, city: str, name: str, season_year: int,
    abbrev_map: dict, name_map: dict,
) -> str:
    if pd.notna(team_id_raw):
        try:
            tid = int(float(team_id_raw))
            abbr = abbrev_map.get((tid, season_year), "")
            if abbr:
                return abbr
        except (TypeError, ValueError):
            pass
    lookup = name_map.get((city.lower(), name.lower(), season_year))
    return lookup[1] if lookup else ""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate(force: bool = False) -> None:
    if not KAGGLE_ZIP.exists():
        sys.exit(f"Kaggle zip not found: {KAGGLE_ZIP}\n"
                 "Download 'historical-nba-data-and-player-box-scores' from Kaggle "
                 "and place the zip in the project root.")

    print("Loading Eoin (Kaggle) data...")
    with zipfile.ZipFile(KAGGLE_ZIP, "r") as zf:
        abbrev_map, name_map = _load_team_maps(zf)
        player_names = _load_player_names(zf)
        ps = _load_player_stats(zf)
    print(f"  {len(ps):,} rows loaded")

    print("Loading bio data from DBs...")
    bio_rs = _load_bio(RS_DB)
    bio_po = _load_bio(PO_DB)
    bio_map: dict[int, dict] = {**bio_po, **bio_rs}  # RS wins on conflict
    print(f"  {len(bio_map):,} player bio records")

    for chunk_dirs, first_year, last_year, game_type, season_year_fn in [
        ((RS_SPAN_DIR, RS_GAME_DIR), RS_FIRST, RS_LAST, "Regular Season", _rs_season_year),
        ((PO_SPAN_DIR, PO_GAME_DIR), PO_FIRST, PO_LAST, "Playoffs", _po_season_year),
    ]:
        span_dir, game_dir = chunk_dirs
        span_dir.mkdir(parents=True, exist_ok=True)
        game_dir.mkdir(parents=True, exist_ok=True)

        subset = ps[ps["gameType"] == game_type].copy()
        subset["_sy"] = subset["gameDateTimeEst"].apply(season_year_fn)
        subset = subset[(subset["_sy"] >= first_year) & (subset["_sy"] <= last_year)].copy()
        subset["_season"] = subset["_sy"].apply(_season_str)
        subset["_date_str"] = subset["gameDateTimeEst"].dt.strftime("%Y-%m-%d")

        target_seasons = sorted(subset["_season"].unique(), key=lambda s: int(s.split("-")[0]))
        print(f"\n{game_type}: {len(target_seasons)} seasons ({target_seasons[0]}–{target_seasons[-1]})")

        for season in target_seasons:
            slug = _season_slug(int(season.split("-")[0]))
            span_path = span_dir / f"{slug}.js"
            game_path = game_dir / f"{slug}.js"

            if not force and span_path.exists() and game_path.exists():
                print(f"  {season}: already exists, skipping (use --force to overwrite)")
                continue

            season_df = subset[subset["_season"] == season]
            season_year = int(season.split("-")[0])

            span_rows: list[list] = []
            game_rows: list[list] = []

            for r in season_df.sort_values("_date_str").to_dict("records"):
                pid = int(r["personId"])
                bio = bio_map.get(pid, {})

                team_abbr = _resolve_abbr(
                    r["playerteamId"], r["playerteamCity"], r["playerteamName"],
                    season_year, abbrev_map, name_map,
                )
                opp_abbr = _resolve_abbr(
                    r["opponentteamId"], r["opponentteamCity"], r["opponentteamName"],
                    season_year, abbrev_map, name_map,
                )

                kwargs = dict(
                    date_str=r["_date_str"],
                    season=season,
                    game_id=r["gameId"],
                    pid=pid,
                    name=player_names.get(pid, str(pid)),
                    team_abbr=team_abbr,
                    opp_abbr=opp_abbr,
                    home_away=r["home_away_str"],
                    win_loss=r["win_loss_str"],
                    starter=int(r["starter_val"]),
                    minutes=float(r["numMinutes"]),
                    pts=float(r["points"]),
                    reb=float(r["reboundsTotal"]),
                    oreb=float(r["reboundsOffensive"]),
                    dreb=float(r["reboundsDefensive"]),
                    ast=float(r["assists"]),
                    stl=float(r["steals"]),
                    blk=float(r["blocks"]),
                    tov=float(r["turnovers"]),
                    pf=float(r["foulsPersonal"]),
                    fgm=float(r["fieldGoalsMade"]),
                    fga=float(r["fieldGoalsAttempted"]),
                    fg2m=float(r["fg2m"]),
                    fg2a=float(r["fg2a"]),
                    fg3m=float(r["threePointersMade"]),
                    fg3a=float(r["threePointersAttempted"]),
                    ftm=float(r["freeThrowsMade"]),
                    fta=float(r["freeThrowsAttempted"]),
                    pm=float(r["plusMinusPoints"]),
                    bio=bio,
                )

                span_rows.append(_span_row(**kwargs))
                game_rows.append(_game_row(**kwargs))

            def _write(path: Path, namespace: str, rows: list[list]) -> None:
                path.write_text(
                    f"window.{namespace} = window.{namespace} || {{}};\n"
                    f"window.{namespace}[{json.dumps(season)}] = "
                    f"{json.dumps(rows, ensure_ascii=False, separators=(',', ':'))};\n",
                    encoding="utf-8",
                )

            _write(span_path, "__PLAYER_SPAN_CHUNKS", span_rows)
            _write(game_path, "__PLAYER_GAME_CHUNKS", game_rows)
            print(f"  {season}: {len(span_rows)} rows -> span + game chunks written")

    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="Overwrite existing chunk files")
    args = parser.parse_args()
    generate(force=args.force)
