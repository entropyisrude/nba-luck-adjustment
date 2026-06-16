"""Generate playoff span search chunks: Kaggle box scores + pbpstats on/off.

Box score source is now PlayerStatistics.csv from the Kaggle dataset
'historical-nba-data-and-player-box-scores', which has complete game-by-game
coverage from 1947 through the current season.  The old DB source was built by
joining box scores WITH play-by-play, so seasons whose PBP had gaps (e.g.
1998-99 SAS had only 4 of 16 games in the source) produced incomplete rows.
Kaggle has no such gap — it's purely box-score-sourced.

The workflow:
1. Load playoff game rows from Kaggle PlayerStatistics.csv.
   Map team IDs → abbreviations via Kaggle TeamHistories.csv.
2. Load bio (height, age, draft) and rim data from the playoff DB.
3. Fetch pbpstats season-level on/off totals.
4. For each pbpstats player-team:
   - If Kaggle game count ≥ pbpstats GamesPlayed − 1: use real Kaggle rows and
     patch on/off columns proportionally by minutes.
   - Otherwise (Kaggle has fewer games than pbpstats): create synthetic rows
     from pbpstats season totals.
5. Players in Kaggle but absent from pbpstats (bench players under the
   minimum possessions threshold): include with on/off zeroed out.
6. Write JS chunk files.

On/off patching formula for real game rows:
  on_poss_game  = pbp_on_poss  × (game_min / season_min)
  off_poss_game = pbp_off_poss × (game_min / season_min)
  pm_game       = actual per-game PM from Kaggle (unchanged)
  on_off_game   = pm_game − pbp_off_diff / n_games
    (ensures sum(pm − on_off) = team_pm − player_pm → correct JS OnOffRtg)

JS on/off formula (player_span_search_playoffs.html):
  offDiffActual = plusMinusActual − onOffActual  (per row)
  g.off_diff_actual_total += offDiffActual
  onOffActual = (100*g.plus_minus_actual/g.on_possessions)
              − (100*g.off_diff_actual_total/g.off_possessions)
Chunk indices: 69=on_poss  70=off_poss  71=pm_actual  72=pm_adj  73=on_off_actual  74=on_off_adj
"""

from __future__ import annotations

import gzip
import json
import os
import time
import zipfile
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd
import requests

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parent)))
DATA_DIR = ROOT / "data"
DB_PATH = Path(
    os.environ.get("NBA_ANALYTICS_DB_PATH", str(DATA_DIR / "nba_analytics_playoffs.duckdb"))
)
CHUNK_DIR = Path(
    os.environ.get("PLAYER_SPAN_SEARCH_CHUNK_DIR", str(DATA_DIR / "player_span_playoff_chunks"))
)
PBPSTATS_PLAYOFFS_CACHE = DATA_DIR / "pbpstats_playoffs_cache.json.gz"
KAGGLE_ZIP = ROOT / "historical-nba-data-and-player-box-scores.zip"

PBP_BASE = "https://api.pbpstats.com"
PBP_TIMEOUT = 45
PBPSTATS_FROM = "1996-97"

# TeamHistories.csv uses different abbreviations than NBA.com/pbpstats for some franchises
ABBREV_OVERRIDES = {
    "SAN": "SAS",  # San Antonio Spurs
    "NJ": "NJN",   # New Jersey Nets
}

# Chunk row indices for on/off columns
IDX_MINUTES = 10
IDX_TEAM_ABBR = 5
IDX_ON_POSS = 69
IDX_OFF_POSS = 70
IDX_PM_ACTUAL = 71
IDX_PM_ADJ = 72
IDX_ON_OFF_ACTUAL = 73
IDX_ON_OFF_ADJ = 74


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _f(v: object) -> float:
    try:
        return float(v)  # type: ignore[arg-type]
    except Exception:
        return 0.0


def _season_start(season: str) -> int:
    return int(str(season).split("-")[0])


def _season_slug(season: str) -> str:
    return season.replace("-", "_")


# ---------------------------------------------------------------------------
# pbpstats fetch + cache
# ---------------------------------------------------------------------------

def _fetch_json(url: str, params: dict, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=PBP_TIMEOUT)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(2 ** attempt)
    return None


def _load_cache() -> dict:
    if PBPSTATS_PLAYOFFS_CACHE.exists():
        try:
            with gzip.open(PBPSTATS_PLAYOFFS_CACHE, "rt", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_cache(cache: dict) -> None:
    with gzip.open(PBPSTATS_PLAYOFFS_CACHE, "wt", encoding="utf-8") as f:
        json.dump(cache, f)


def _fetch_season(season: str, cache: dict, force: bool = False) -> tuple[list[dict], list[dict]]:
    """Returns (player_rows, team_rows) from pbpstats."""
    if not force and season in cache:
        cached = cache[season]
        return (
            cached.get("players", {}).get("multi_row_table_data", []),
            cached.get("teams", {}).get("multi_row_table_data", []),
        )
    players = _fetch_json(
        f"{PBP_BASE}/get-totals/nba",
        {"Season": season, "SeasonType": "Playoffs", "Type": "Player"},
    )
    teams = _fetch_json(
        f"{PBP_BASE}/get-totals/nba",
        {"Season": season, "SeasonType": "Playoffs", "Type": "Team"},
    )
    if players and teams:
        cache[season] = {"players": players, "teams": teams}
    return (
        (players or {}).get("multi_row_table_data", []),
        (teams or {}).get("multi_row_table_data", []),
    )


def _build_pbp_index(
    player_rows: list[dict], team_rows: list[dict]
) -> dict[tuple[str, str], dict]:
    """Returns map keyed by (team_abbr_upper, player_id_str) with player+team records."""
    team_by_id: dict[str, dict] = {str(tr.get("TeamId", "")): tr for tr in team_rows}
    out: dict[tuple[str, str], dict] = {}
    for pr in player_rows:
        team_id = str(pr.get("TeamId", ""))
        player_id = str(pr.get("EntityId", ""))
        team = team_by_id.get(team_id)
        if not team:
            continue
        on_off_poss = _f(pr.get("OffPoss"))
        on_def_poss = _f(pr.get("DefPoss"))
        if on_off_poss <= 0 or on_def_poss <= 0:
            continue
        abbr = str(pr.get("TeamAbbreviation") or "").upper()
        out[(abbr, player_id)] = {"player": pr, "team": team}
    return out


# ---------------------------------------------------------------------------
# Kaggle data loading
# ---------------------------------------------------------------------------

def _load_team_maps(
    zf: zipfile.ZipFile,
) -> tuple[dict[tuple[int, int], str], dict[tuple[str, str, int], tuple[int, str]]]:
    """Returns two maps from TeamHistories.csv:

    id_map:   {(teamId_int, season_year_int): abbrev_str}
    name_map: {(city_lower, name_lower, season_year_int): (teamId_int, abbrev_str)}

    TeamHistories abbreviations have trailing whitespace and a few differ from
    NBA.com/pbpstats (e.g. 'SAN' vs 'SAS').  Both are corrected here.
    Some Kaggle seasons have NaN teamId — name_map provides the fallback.
    """
    with zf.open("TeamHistories.csv") as f:
        th = pd.read_csv(f)
    for col in ("teamAbbrev", "teamCity", "teamName", "league"):
        th[col] = th[col].str.strip()
    th = th[th["league"].str.lower() == "nba"]

    id_map: dict[tuple[int, int], str] = {}
    name_map: dict[tuple[str, str, int], tuple[int, str]] = {}
    for _, row in th.iterrows():
        tid = int(row["teamId"])
        abbrev = ABBREV_OVERRIDES.get(str(row["teamAbbrev"]), str(row["teamAbbrev"]))
        city = str(row["teamCity"]).lower()
        name = str(row["teamName"]).lower()
        founded = int(row["seasonFounded"])
        till = int(row["seasonActiveTill"])
        for yr in range(founded, till + 1):
            id_map[(tid, yr)] = abbrev
            name_map[(city, name, yr)] = (tid, abbrev)
    return id_map, name_map


def _load_player_names(zf: zipfile.ZipFile) -> dict[int, str]:
    """Returns {personId: 'First Last'} from Players.csv."""
    with zf.open("Players.csv") as f:
        df = pd.read_csv(f, usecols=["personId", "firstName", "lastName"])
    names: dict[int, str] = {}
    for _, row in df.iterrows():
        pid = int(row["personId"])
        first = str(row["firstName"]) if pd.notna(row["firstName"]) else ""
        last = str(row["lastName"]) if pd.notna(row["lastName"]) else ""
        names[pid] = f"{first} {last}".strip()
    return names


def _load_kaggle_rows(
    bio_map: dict[int, dict],
    rim_map: dict[tuple[str, int], dict],
) -> dict[str, dict[tuple[str, str], list[list]]]:
    """Load playoff game rows from Kaggle PlayerStatistics.csv.

    Returns {season: {(player_id_str, team_abbr_upper): [75-col row, ...]}}

    Columns 31-36 (assist splits) and 60-68 (hustle) are None; bio (37-42) and
    rim (54-59) are filled from DB lookups passed in.  On/off columns (69-74)
    are placeholders — patched in generate().
    """
    if not KAGGLE_ZIP.exists():
        raise FileNotFoundError(f"Kaggle zip not found: {KAGGLE_ZIP}")

    with zipfile.ZipFile(KAGGLE_ZIP, "r") as zf:
        team_abbr_map, team_name_map = _load_team_maps(zf)
        player_names = _load_player_names(zf)
        with zf.open("PlayerStatistics.csv") as f:
            ps = pd.read_csv(f, low_memory=False)

    ps = ps[ps["gameType"] == "Playoffs"].copy()

    ps["gameDateTimeEst"] = pd.to_datetime(ps["gameDateTimeEst"])
    ps["_season_year"] = ps["gameDateTimeEst"].dt.year.astype(int) - 1
    ps["season"] = ps["_season_year"].apply(lambda y: f"{y}-{str(y + 1)[-2:]}")
    ps["date_str"] = ps["gameDateTimeEst"].dt.strftime("%Y-%m-%d")

    ps["playerteamId"] = pd.to_numeric(ps["playerteamId"], errors="coerce")
    ps["opponentteamId"] = pd.to_numeric(ps["opponentteamId"], errors="coerce")
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
    ps["gameId"] = ps["gameId"].astype(str)
    ps = ps.dropna(subset=["personId"]).copy()
    ps["personId"] = ps["personId"].astype(int)

    def _pct(m: float, a: float) -> float | None:
        return m / a if a > 0 else None

    def _resolve_team(
        team_id_raw: object,
        city: str,
        name: str,
        season_year: int,
    ) -> tuple[str, str]:
        """Returns (team_id_str, team_abbr) using teamId when available, name-based fallback otherwise."""
        if pd.notna(team_id_raw):
            tid = int(team_id_raw)
            abbrev = team_abbr_map.get((tid, season_year), str(tid))
            return str(tid), abbrev
        # NaN team ID — look up by city + name (some seasons have incomplete Kaggle data)
        lookup = team_name_map.get((city.lower(), name.lower(), season_year))
        if lookup:
            return str(lookup[0]), lookup[1]
        return "", ""

    result: dict[str, dict[tuple[str, str], list[list]]] = {}

    for r in ps.to_dict("records"):
        pid = int(r["personId"])
        season = str(r["season"])
        season_year = int(r["_season_year"])
        pid_str = str(pid)

        team_id_str, team_abbr = _resolve_team(
            r["playerteamId"], r["playerteamCity"], r["playerteamName"], season_year
        )
        _, opp_abbr = _resolve_team(
            r["opponentteamId"], r["opponentteamCity"], r["opponentteamName"], season_year
        )

        bio = bio_map.get(pid, {})
        rim = rim_map.get((season, pid), {})

        fgm = float(r["fieldGoalsMade"])
        fga = float(r["fieldGoalsAttempted"])
        fg2m = float(r["fg2m"])
        fg2a = float(r["fg2a"])
        fg3m = float(r["threePointersMade"])
        fg3a = float(r["threePointersAttempted"])
        ftm = float(r["freeThrowsMade"])
        fta = float(r["freeThrowsAttempted"])
        pm = float(r["plusMinusPoints"])

        row: list = [
            r["date_str"],                        # 0: date
            season,                               # 1: season
            r["gameId"],                          # 2: game_id
            pid,                                  # 3: player_id
            player_names.get(pid, pid_str),       # 4: player_name
            team_abbr,                            # 5: team_abbr
            opp_abbr,                             # 6: opp_team_abbr
            r["home_away_str"],                   # 7: home_away
            r["win_loss_str"],                    # 8: win_loss
            r["starter_val"],                     # 9: starter
            float(r["numMinutes"]),               # 10: minutes
            float(r["points"]),                   # 11: pts
            float(r["reboundsTotal"]),            # 12: reb
            float(r["reboundsOffensive"]),        # 13: oreb
            float(r["reboundsDefensive"]),        # 14: dreb
            float(r["assists"]),                  # 15: ast
            float(r["steals"]),                   # 16: stl
            float(r["blocks"]),                   # 17: blk
            float(r["turnovers"]),                # 18: tov
            float(r["foulsPersonal"]),            # 19: pf
            fgm,                                  # 20: fgm
            fga,                                  # 21: fga
            fg2m,                                 # 22: fg2m
            fg2a,                                 # 23: fg2a
            _pct(fg2m, fg2a),                     # 24: fg2_pct
            fg3m,                                 # 25: fg3m
            fg3a,                                 # 26: fg3a
            _pct(fg3m, fg3a),                     # 27: fg3_pct
            ftm,                                  # 28: ftm
            fta,                                  # 29: fta
            _pct(ftm, fta),                       # 30: ft_pct
            None, None, None, None, None, None,   # 31-36: assist splits
            bio.get("listed_height"),             # 37: listed_height
            bio.get("height_inches"),             # 38: height_inches
            bio.get("age"),                       # 39: age
            bio.get("career_year"),               # 40: career_year
            bio.get("draft_year"),                # 41: draft_year
            bio.get("draft_overall_pick"),        # 42: draft_overall_pick
            None, None, None, None, None, None,   # 43-48: rim assists counts
            None, None, None, None, None,         # 49-53: rim assists per game
            rim.get("rim_anchor_signature"),      # 54
            rim.get("rim_deterrence_signature"),  # 55
            rim.get("rim_dfga"),                  # 56
            rim.get("rim_tracking_games"),        # 57
            rim.get("rim_dfg_pct"),               # 58
            rim.get("rim_dfg_pct_diff"),          # 59
            None, None, None, None, None,         # 60-64: hustle
            None, None, None, None,               # 65-68: hustle
            0.0,                                  # 69: on_poss (patched later)
            0.0,                                  # 70: off_poss (patched later)
            pm,                                   # 71: plus_minus_actual
            pm,                                   # 72: plus_minus_adjusted
            0.0,                                  # 73: on_off_actual (patched later)
            0.0,                                  # 74: on_off_adjusted
        ]

        if season not in result:
            result[season] = {}
        key = (pid_str, team_id_str)
        if key not in result[season]:
            result[season][key] = []
        result[season][key].append(row)

    # Sort game rows within each player-team by date ascending
    for season_data in result.values():
        for rows in season_data.values():
            rows.sort(key=lambda r: r[0])

    return result


# ---------------------------------------------------------------------------
# Bio + rim helpers (from DB, for columns not in Kaggle)
# ---------------------------------------------------------------------------

def _load_bio_from_db(con: duckdb.DuckDBPyConnection) -> dict[int, dict]:
    rows = con.execute(
        """
        SELECT
            player_id,
            MAX(listed_height)      AS listed_height,
            MAX(height_inches)      AS height_inches,
            MIN(age)                AS age,
            MIN(career_year)        AS career_year,
            MAX(draft_year)         AS draft_year,
            MAX(draft_overall_pick) AS draft_overall_pick
        FROM player_game_facts
        WHERE player_id IS NOT NULL
        GROUP BY player_id
        """
    ).fetchall()
    return {
        int(pid): {
            "listed_height": ht_str,
            "height_inches": ht_in,
            "age": age,
            "career_year": cy,
            "draft_year": dy,
            "draft_overall_pick": dp,
        }
        for pid, ht_str, ht_in, age, cy, dy, dp in rows
    }


def _load_rim_from_db(con: duckdb.DuckDBPyConnection) -> dict[tuple[str, int], dict]:
    rows = con.execute(
        """
        SELECT
            season,
            player_id,
            MAX(rim_anchor_signature)     AS rim_anchor_signature,
            MAX(rim_deterrence_signature) AS rim_deterrence_signature,
            SUM(rim_dfga)                 AS rim_dfga,
            MAX(rim_tracking_games)       AS rim_tracking_games,
            MAX(rim_dfg_pct)              AS rim_dfg_pct,
            MAX(rim_dfg_pct_diff)         AS rim_dfg_pct_diff
        FROM player_game_facts
        WHERE player_id IS NOT NULL
          AND game_id LIKE '4%'
        GROUP BY season, player_id
        """
    ).fetchall()
    return {
        (season, int(pid)): {
            "rim_anchor_signature": anchor,
            "rim_deterrence_signature": det,
            "rim_dfga": dfga,
            "rim_tracking_games": tgames,
            "rim_dfg_pct": dfg_pct,
            "rim_dfg_pct_diff": dfg_diff,
        }
        for season, pid, anchor, det, dfga, tgames, dfg_pct, dfg_diff in rows
    }


# ---------------------------------------------------------------------------
# Fake row builder (used when Kaggle has fewer games than pbpstats)
# ---------------------------------------------------------------------------

def _make_fake_row(
    *,
    season: str,
    fake_date: str,
    fake_game_id: str,
    player_id: int,
    player_name: str,
    team_abbr: str,
    n: int,
    pts: float,
    reb: float,
    oreb: float,
    dreb: float,
    ast: float,
    stl: float,
    blk: float,
    tov: float,
    pf: float,
    fgm: float,
    fga: float,
    fg2m: float,
    fg2a: float,
    fg3m: float,
    fg3a: float,
    ftm: float,
    fta: float,
    minutes: float,
    bio: dict,
    rim: dict,
    rim_dfga_total: float | None,
    on_poss_row: float,
    off_poss_row: float,
    pm_row: float,
    on_off_row: float,
) -> list:
    def div(a: float, b: float) -> float | None:
        return a / b if b > 0 else None

    rim_dfga_val = (rim_dfga_total / n) if rim_dfga_total is not None else None
    return [
        fake_date,                          # 0: date
        season,                             # 1: season
        fake_game_id,                       # 2: game_id
        player_id,                          # 3: player_id
        player_name,                        # 4: player_name
        team_abbr,                          # 5: team_abbr
        None,                               # 6: opp_team_abbr
        None,                               # 7: home_away
        None,                               # 8: win_loss
        None,                               # 9: starter
        minutes / n,                        # 10: minutes
        pts / n,                            # 11: pts
        reb / n,                            # 12: reb
        oreb / n,                           # 13: oreb
        dreb / n,                           # 14: dreb
        ast / n,                            # 15: ast
        stl / n,                            # 16: stl
        blk / n,                            # 17: blk
        tov / n,                            # 18: tov
        pf / n,                             # 19: pf
        fgm / n,                            # 20: fgm
        fga / n,                            # 21: fga
        fg2m / n,                           # 22: fg2m
        fg2a / n,                           # 23: fg2a
        div(fg2m, fg2a),                    # 24: fg2_pct
        fg3m / n,                           # 25: fg3m
        fg3a / n,                           # 26: fg3a
        div(fg3m, fg3a),                    # 27: fg3_pct
        ftm / n,                            # 28: ftm
        fta / n,                            # 29: fta
        div(ftm, fta),                      # 30: ft_pct
        None, None, None, None, None, None, # 31-36: assist splits
        bio.get("listed_height"),           # 37: listed_height
        bio.get("height_inches"),           # 38: height_inches
        bio.get("age"),                     # 39: age
        bio.get("career_year"),             # 40: career_year
        bio.get("draft_year"),              # 41: draft_year
        bio.get("draft_overall_pick"),      # 42: draft_overall_pick
        None, None, None, None, None, None, # 43-48: rim assists counts
        None, None, None, None, None,       # 49-53: rim assists per game
        rim.get("rim_anchor_signature"),    # 54: rim_anchor_signature
        rim.get("rim_deterrence_signature"),# 55: rim_deterrence_signature
        rim_dfga_val,                       # 56: rim_dfga
        rim.get("rim_tracking_games"),      # 57: rim_tracking_games
        rim.get("rim_dfg_pct"),             # 58: rim_dfg_pct
        rim.get("rim_dfg_pct_diff"),        # 59: rim_dfg_pct_diff
        None, None, None, None, None,       # 60-64: hustle
        None, None, None, None,             # 65-68: hustle
        on_poss_row,                        # 69: on_possessions
        off_poss_row,                       # 70: off_possessions
        pm_row,                             # 71: plus_minus_actual
        pm_row,                             # 72: plus_minus_adjusted
        on_off_row,                         # 73: on_off_actual
        on_off_row,                         # 74: on_off_adjusted
    ]


# ---------------------------------------------------------------------------
# Main generate
# ---------------------------------------------------------------------------

def generate() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    bio_map = _load_bio_from_db(con)
    rim_map = _load_rim_from_db(con)
    con.close()

    print("Loading Kaggle playoff box scores...")
    kaggle_by_season = _load_kaggle_rows(bio_map, rim_map)
    seasons = sorted(kaggle_by_season.keys(), key=_season_start)
    latest_season = max(seasons, key=_season_start) if seasons else None

    cache = _load_cache()
    cache_updated = False

    CHUNK_DIR.mkdir(parents=True, exist_ok=True)

    for season in seasons:
        if _season_start(season) < _season_start(PBPSTATS_FROM):
            print(f"  {season}: skipping (before pbpstats coverage, keeping existing chunk)")
            continue

        force = season == latest_season
        player_rows, team_rows = _fetch_season(season, cache, force=force)
        if not player_rows or not team_rows:
            print(f"  {season}: pbpstats unavailable, skipping")
            continue
        if season not in cache or force:
            cache_updated = True

        pbp_index = _build_pbp_index(player_rows, team_rows)

        season_kaggle = kaggle_by_season.get(season, {})
        chunk_rows: list[list] = []
        handled_kaggle_keys: set[tuple[str, str]] = set()

        real_rows_used = 0
        partial_rows_used = 0
        fake_rows_used = 0
        start_year = _season_start(season)
        fake_date = f"{start_year + 1}-06-15"

        for (abbr_pbp, player_id_str), entry in sorted(
            pbp_index.items(), key=lambda kv: str(kv[1]["player"].get("Name", ""))
        ):
            pr = entry["player"]
            team = entry["team"]

            player_id_int = int(player_id_str) if player_id_str.isdigit() else 0
            pbpstats_n = max(int(_f(pr.get("GamesPlayed", 1))), 1)
            team_abbr = str(pr.get("TeamAbbreviation") or "").upper()
            pbp_team_id = str(entry["player"].get("TeamId", ""))
            player_name = str(pr.get("Name") or "")

            player_pm = _f(pr.get("PlusMinus"))
            team_pm_raw = team.get("PlusMinus")        # keep None distinct from 0
            team_pm = _f(team_pm_raw)
            on_off_poss = _f(pr.get("OffPoss"))
            on_def_poss = _f(pr.get("DefPoss"))
            on_opp_pts  = _f(pr.get("OpponentPoints")) # pts allowed when player on court
            team_off_poss = _f(team.get("OffPoss"))
            team_def_poss = _f(team.get("DefPoss"))
            team_pts    = _f(team.get("Points"))
            team_opp_pts = _f(team.get("OpponentPoints"))

            on_poss_total = (on_off_poss + on_def_poss) / 2.0
            off_poss_total = max((team_off_poss + team_def_poss) / 2.0 - on_poss_total, 0.0)
            pbpstats_off_diff = team_pm - player_pm

            # On/off net rating: (on_OffRtg − on_DefRtg) − (off_OffRtg − off_DefRtg).
            # Use separate OffPoss and DefPoss denominators (not averaged) so the
            # offensive/defensive splits are correct.  Falls back to 0 if data missing.
            on_scored  = player_pm + on_opp_pts          # team pts scored when on court
            off_OffPoss = max(team_off_poss - on_off_poss, 0.0)
            off_DefPoss = max(team_def_poss - on_def_poss, 0.0)
            off_scored  = max(team_pts - on_scored, 0.0)
            off_allowed = max(team_opp_pts - on_opp_pts, 0.0)
            if (on_off_poss > 0 and on_def_poss > 0 and
                    off_OffPoss > 0 and off_DefPoss > 0 and team_pts > 0):
                on_net_rtg  = 100.0 * on_scored / on_off_poss - 100.0 * on_opp_pts / on_def_poss
                off_net_rtg = 100.0 * off_scored / off_OffPoss - 100.0 * off_allowed / off_DefPoss
                on_off_per100 = on_net_rtg - off_net_rtg
            else:
                on_off_per100 = 0.0

            # Match by (player_id, pbpstats team_id) — bypasses abbreviation mismatches
            kaggle_key = (player_id_str, pbp_team_id)
            game_rows = season_kaggle.get(kaggle_key, [])

            use_real = bool(game_rows) and len(game_rows) >= pbpstats_n - 1
            use_partial = bool(game_rows) and not use_real

            if use_real:
                handled_kaggle_keys.add(kaggle_key)
                real_rows_used += 1
                n = len(game_rows)
                season_min = sum(_f(r[IDX_MINUTES]) for r in game_rows)

                for row in game_rows:
                    game_min = _f(row[IDX_MINUTES])
                    min_frac = (game_min / season_min) if season_min > 0 else (1.0 / n)
                    game_pm = _f(row[IDX_PM_ACTUAL])

                    row[IDX_ON_POSS] = on_poss_total * min_frac
                    row[IDX_OFF_POSS] = off_poss_total * min_frac
                    row[IDX_ON_OFF_ACTUAL] = on_off_per100
                    row[IDX_ON_OFF_ADJ] = on_off_per100
                    row[IDX_PM_ADJ] = game_pm
                    # Use pbpstats abbreviation and name (authoritative)
                    row[IDX_TEAM_ABBR] = team_abbr
                    if not row[4]:
                        row[4] = player_name

                chunk_rows.extend(game_rows)

            elif use_partial:
                # Kaggle has some games but not all (e.g. later rounds not yet in Kaggle).
                # Use real rows for available games + synthetic rows for the missing games.
                handled_kaggle_keys.add(kaggle_key)
                partial_rows_used += 1
                n_real = len(game_rows)
                n_total = pbpstats_n
                n_missing = n_total - n_real
                pbp_minutes = _f(pr.get("Minutes"))

                for row in game_rows:
                    game_min = _f(row[IDX_MINUTES])
                    # Use total pbpstats minutes as denominator so fractions sum to <1,
                    # leaving room for the synthetic rows to account for the rest.
                    min_frac = (game_min / pbp_minutes) if pbp_minutes > 0 else (1.0 / n_total)
                    game_pm = _f(row[IDX_PM_ACTUAL])

                    row[IDX_ON_POSS] = on_poss_total * min_frac
                    row[IDX_OFF_POSS] = off_poss_total * min_frac
                    row[IDX_ON_OFF_ACTUAL] = on_off_per100
                    row[IDX_ON_OFF_ADJ] = on_off_per100
                    row[IDX_PM_ADJ] = game_pm
                    row[IDX_TEAM_ABBR] = team_abbr
                    if not row[4]:
                        row[4] = player_name

                chunk_rows.extend(game_rows)

                if n_missing > 0:
                    def _sc(rows: list[list], idx: int) -> float:
                        return sum(_f(r[idx]) for r in rows)

                    pts_rem  = max(_f(pr.get("Points"))       - _sc(game_rows, 11), 0.0)
                    reb_rem  = max(_f(pr.get("Rebounds"))     - _sc(game_rows, 12), 0.0)
                    oreb_rem = max(_f(pr.get("OffRebounds"))  - _sc(game_rows, 13), 0.0)
                    dreb_rem = max(_f(pr.get("DefRebounds"))  - _sc(game_rows, 14), 0.0)
                    ast_rem  = max(_f(pr.get("Assists"))      - _sc(game_rows, 15), 0.0)
                    stl_rem  = max(_f(pr.get("Steals"))       - _sc(game_rows, 16), 0.0)
                    blk_rem  = max(_f(pr.get("Blocks"))       - _sc(game_rows, 17), 0.0)
                    tov_rem  = max(_f(pr.get("Turnovers"))    - _sc(game_rows, 18), 0.0)
                    pf_rem   = max(_f(pr.get("Fouls"))        - _sc(game_rows, 19), 0.0)
                    fg2m_rem = max(_f(pr.get("FG2M"))         - _sc(game_rows, 22), 0.0)
                    fg2a_rem = max(_f(pr.get("FG2A"))         - _sc(game_rows, 23), 0.0)
                    fg3m_rem = max(_f(pr.get("FG3M"))         - _sc(game_rows, 25), 0.0)
                    fg3a_rem = max(_f(pr.get("FG3A"))         - _sc(game_rows, 26), 0.0)
                    ftm_rem  = max(_f(pr.get("FtPoints"))     - _sc(game_rows, 28), 0.0)
                    fta_rem  = max(_f(pr.get("FTA"))          - _sc(game_rows, 29), 0.0)
                    min_rem  = max(pbp_minutes                - _sc(game_rows, IDX_MINUTES), 0.0)
                    pm_rem   = player_pm                      - _sc(game_rows, IDX_PM_ACTUAL)
                    fgm_rem  = fg2m_rem + fg3m_rem
                    fga_rem  = fg2a_rem + fg3a_rem

                    min_frac_rem = (min_rem / pbp_minutes) if pbp_minutes > 0 else (n_missing / n_total)
                    on_poss_rem  = on_poss_total * min_frac_rem
                    off_poss_rem = off_poss_total * min_frac_rem

                    bio = bio_map.get(player_id_int, {})
                    rim = rim_map.get((season, player_id_int), {})
                    rim_dfga_total = rim.get("rim_dfga")

                    for i in range(n_missing):
                        chunk_rows.append(
                            _make_fake_row(
                                season=season,
                                fake_date=fake_date,
                                fake_game_id=f"pbp_{_season_slug(season)}_{player_id_str}_{n_real + i + 1}",
                                player_id=player_id_int,
                                player_name=player_name,
                                team_abbr=team_abbr,
                                n=n_missing,
                                pts=pts_rem, reb=reb_rem, oreb=oreb_rem, dreb=dreb_rem,
                                ast=ast_rem, stl=stl_rem, blk=blk_rem, tov=tov_rem, pf=pf_rem,
                                fgm=fgm_rem, fga=fga_rem, fg2m=fg2m_rem, fg2a=fg2a_rem,
                                fg3m=fg3m_rem, fg3a=fg3a_rem, ftm=ftm_rem, fta=fta_rem,
                                minutes=min_rem,
                                bio=bio, rim=rim, rim_dfga_total=rim_dfga_total,
                                on_poss_row=on_poss_rem / n_missing,
                                off_poss_row=off_poss_rem / n_missing,
                                pm_row=pm_rem / n_missing,
                                on_off_row=on_off_per100,
                            )
                        )

            else:
                # No Kaggle data at all — use synthetic rows for the full season
                if kaggle_key in season_kaggle:
                    handled_kaggle_keys.add(kaggle_key)
                fake_rows_used += 1
                n = pbpstats_n
                pts = _f(pr.get("Points"))
                reb = _f(pr.get("Rebounds"))
                oreb = _f(pr.get("OffRebounds"))
                dreb = _f(pr.get("DefRebounds"))
                ast = _f(pr.get("Assists"))
                stl = _f(pr.get("Steals"))
                blk = _f(pr.get("Blocks"))
                tov = _f(pr.get("Turnovers"))
                pf = _f(pr.get("Fouls"))
                fg2m = _f(pr.get("FG2M"))
                fg2a = _f(pr.get("FG2A"))
                fg3m = _f(pr.get("FG3M"))
                fg3a = _f(pr.get("FG3A"))
                fgm = fg2m + fg3m
                fga = fg2a + fg3a
                ftm = _f(pr.get("FtPoints"))
                fta = _f(pr.get("FTA"))
                minutes = _f(pr.get("Minutes"))

                bio = bio_map.get(player_id_int, {})
                rim = rim_map.get((season, player_id_int), {})
                rim_dfga_total = rim.get("rim_dfga")

                for i in range(n):
                    chunk_rows.append(
                        _make_fake_row(
                            season=season,
                            fake_date=fake_date,
                            fake_game_id=f"pbp_{_season_slug(season)}_{player_id_str}_{i + 1}",
                            player_id=player_id_int,
                            player_name=player_name,
                            team_abbr=team_abbr,
                            n=n,
                            pts=pts, reb=reb, oreb=oreb, dreb=dreb,
                            ast=ast, stl=stl, blk=blk, tov=tov, pf=pf,
                            fgm=fgm, fga=fga, fg2m=fg2m, fg2a=fg2a,
                            fg3m=fg3m, fg3a=fg3a, ftm=ftm, fta=fta,
                            minutes=minutes,
                            bio=bio, rim=rim, rim_dfga_total=rim_dfga_total,
                            on_poss_row=on_poss_total / n,
                            off_poss_row=off_poss_total / n,
                            pm_row=player_pm / n,
                            on_off_row=on_off_per100,
                        )
                    )

        # Players in Kaggle but absent from pbpstats (bench players): include
        # with on/off zeroed (already 0.0 from _load_kaggle_rows)
        kaggle_only = 0
        for kaggle_key, game_rows in season_kaggle.items():
            if kaggle_key not in handled_kaggle_keys:
                kaggle_only += 1
                chunk_rows.extend(game_rows)

        slug = _season_slug(season)
        chunk_js = (
            "window.__PLAYER_SPAN_CHUNKS = window.__PLAYER_SPAN_CHUNKS || {};\n"
            f"window.__PLAYER_SPAN_CHUNKS[{json.dumps(season)}] = "
            f"{json.dumps(chunk_rows, ensure_ascii=False, separators=(',', ':'))};\n"
        )
        (CHUNK_DIR / f"{slug}.js").write_text(chunk_js, encoding="utf-8")
        total_pbp = real_rows_used + partial_rows_used + fake_rows_used
        print(
            f"  {season}: {total_pbp + kaggle_only} player-teams, {len(chunk_rows)} rows"
            f" (real:{real_rows_used} partial:{partial_rows_used} fake:{fake_rows_used} kaggle-only:{kaggle_only})"
        )

    if cache_updated:
        _save_cache(cache)

    print(f"Done. Chunks written to {CHUNK_DIR}")


if __name__ == "__main__":
    generate()
