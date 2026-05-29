"""Generate playoff span search chunks: DB box stats + pbpstats on/off correction.

The workflow:
1. Query player_game_facts (playoff DB) for real game-by-game rows — correct box
   stats, dates, opponents, starters, etc. — but on/off possessions may be wrong
   for seasons where the play-by-play source has incomplete coverage (e.g. 1998-99
   SAS had only 4 of 16 games in the source PBP).
2. Fetch pbpstats season-level totals (on/off possessions, plus/minus).
3. For each pbpstats player-team, match to DB game rows by (player_id, team_abbr):
   - If DB game count is within 1 of pbpstats GamesPlayed: use real DB game rows
     and patch on/off columns proportionally by minutes.
   - Otherwise (DB data incomplete): create pbpstats-based synthetic rows.
4. Players in DB but absent from pbpstats keep their DB on/off unchanged.
5. Write JS chunk files.

For 1995-96 (before pbpstats coverage), existing chunk is left untouched.

On/off patching formula for real game rows:
  on_poss_game  = pbp_on_poss  × (game_min / season_min)
  off_poss_game = pbp_off_poss × (game_min / season_min)
  pm_game       = actual per-game PM from DB (unchanged)
  on_off_game   = pm_game − pbp_off_diff / n_games
    (ensures sum(pm - on_off) = team_pm - player_pm → correct JS OnOffRtg)

JS on/off formula (player_span_search_playoffs.html):
  offDiffActual = plusMinusActual - onOffActual  (per row)
  g.off_diff_actual_total += offDiffActual
  onOffActual = (100*g.plus_minus_actual/g.on_possessions)
              - (100*g.off_diff_actual_total/g.off_possessions)
Chunk indices:  69=on_poss  70=off_poss  71=pm_actual  72=pm_adj  73=on_off_actual  74=on_off_adj
"""

from __future__ import annotations

import gzip
import json
import os
import time
from pathlib import Path

import duckdb
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

PBP_BASE = "https://api.pbpstats.com"
PBP_TIMEOUT = 45
PBPSTATS_FROM = "1996-97"

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
# DB query — same column order as the 75-element chunk row format
# ---------------------------------------------------------------------------

_QUERY = """
SELECT
    CAST(date AS VARCHAR)                          AS date,
    season,
    game_id,
    player_id,
    player_name,
    team_abbr,
    opp_team_abbr,
    home_away,
    win_loss,
    starter,
    minutes,
    pts,
    reb,
    oreb,
    dreb,
    ast,
    stl,
    blk,
    tov,
    pf,
    fgm,
    fga,
    fg2m,
    fg2a,
    fg2_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    {assisted_2pm},
    {unassisted_2pm},
    {assisted_3pm},
    {unassisted_3pm},
    {assisted_fgm},
    {unassisted_fgm},
    {listed_height},
    {height_inches},
    {age},
    {career_year},
    {draft_year},
    {draft_overall_pick},
    {layup_assists_created},
    {dunk_assists_created},
    {other_rim_assists_created},
    {rim_assists_strict},
    {rim_assists_all},
    {rim_assists_season_games},
    {layup_assists_created_per_game},
    {dunk_assists_created_per_game},
    {other_rim_assists_created_per_game},
    {rim_assists_strict_per_game},
    {rim_assists_all_per_game},
    rim_anchor_signature,
    rim_deterrence_signature,
    rim_dfga,
    rim_tracking_games,
    rim_dfg_pct,
    rim_dfg_pct_diff,
    {contested_shots},
    {contested_shots_2pt},
    {contested_shots_3pt},
    {deflections},
    {charges_drawn},
    {screen_assists},
    {screen_ast_pts},
    {loose_balls_recovered},
    {box_outs},
    on_possessions,
    (team_possessions - on_possessions) AS off_possessions,
    plus_minus_actual,
    plus_minus_adjusted,
    on_off_actual,
    on_off_adjusted
FROM player_game_facts
WHERE pts IS NOT NULL
  AND game_id LIKE '4%'
ORDER BY season, player_id, team_abbr, date
"""


def _load_db_rows(con: duckdb.DuckDBPyConnection) -> list[list]:
    """Return all playoff game rows in 75-column chunk format."""
    available = {
        row[1]
        for row in con.execute("PRAGMA table_info('player_game_facts')").fetchall()
    }

    def sc(name: str) -> str:
        return name if name in available else f"NULL AS {name}"

    rows = con.execute(
        _QUERY.format(
            assisted_2pm=sc("assisted_2pm"),
            unassisted_2pm=sc("unassisted_2pm"),
            assisted_3pm=sc("assisted_3pm"),
            unassisted_3pm=sc("unassisted_3pm"),
            assisted_fgm=sc("assisted_fgm"),
            unassisted_fgm=sc("unassisted_fgm"),
            listed_height=sc("listed_height"),
            height_inches=sc("height_inches"),
            age=sc("age"),
            career_year=sc("career_year"),
            draft_year=sc("draft_year"),
            draft_overall_pick=sc("draft_overall_pick"),
            layup_assists_created=sc("layup_assists_created"),
            dunk_assists_created=sc("dunk_assists_created"),
            other_rim_assists_created=sc("other_rim_assists_created"),
            rim_assists_strict=sc("rim_assists_strict"),
            rim_assists_all=sc("rim_assists_all"),
            rim_assists_season_games=sc("rim_assists_season_games"),
            layup_assists_created_per_game=sc("layup_assists_created_per_game"),
            dunk_assists_created_per_game=sc("dunk_assists_created_per_game"),
            other_rim_assists_created_per_game=sc("other_rim_assists_created_per_game"),
            rim_assists_strict_per_game=sc("rim_assists_strict_per_game"),
            rim_assists_all_per_game=sc("rim_assists_all_per_game"),
            contested_shots=sc("contested_shots"),
            contested_shots_2pt=sc("contested_shots_2pt"),
            contested_shots_3pt=sc("contested_shots_3pt"),
            deflections=sc("deflections"),
            charges_drawn=sc("charges_drawn"),
            screen_assists=sc("screen_assists"),
            screen_ast_pts=sc("screen_ast_pts"),
            loose_balls_recovered=sc("loose_balls_recovered"),
            box_outs=sc("box_outs"),
        )
    ).fetchall()
    return [list(row) for row in rows]


# ---------------------------------------------------------------------------
# Bio + rim helpers (used for pbpstats fake-row fallback)
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
    from collections import defaultdict

    con = duckdb.connect(str(DB_PATH), read_only=True)
    seasons_in_db = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT season FROM player_game_facts ORDER BY season"
        ).fetchall()
    ]
    all_db_rows = _load_db_rows(con)
    bio_map = _load_bio_from_db(con)
    rim_map = _load_rim_from_db(con)
    con.close()

    # Group DB rows by (season, player_id_str, team_abbr_upper)
    db_by_season: dict[str, dict[tuple[str, str], list[list]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in all_db_rows:
        season = str(row[1] or "")
        player_id = str(row[3] or "")
        team_abbr = str(row[IDX_TEAM_ABBR] or "").upper()
        db_by_season[season][(player_id, team_abbr)].append(row)

    cache = _load_cache()
    cache_updated = False
    latest_season = max(seasons_in_db, key=_season_start) if seasons_in_db else None

    CHUNK_DIR.mkdir(parents=True, exist_ok=True)

    for season in sorted(seasons_in_db, key=_season_start):
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

        season_db = db_by_season.get(season, {})
        chunk_rows: list[list] = []
        handled_db_keys: set[tuple[str, str]] = set()

        real_rows_used = 0
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
            player_name = str(pr.get("Name") or "")

            player_pm = _f(pr.get("PlusMinus"))
            team_pm = _f(team.get("PlusMinus"))
            on_off_poss = _f(pr.get("OffPoss"))
            on_def_poss = _f(pr.get("DefPoss"))
            team_off_poss = _f(team.get("OffPoss"))
            team_def_poss = _f(team.get("DefPoss"))

            on_poss_total = (on_off_poss + on_def_poss) / 2.0
            off_poss_total = max((team_off_poss + team_def_poss) / 2.0 - on_poss_total, 0.0)
            pbpstats_off_diff = team_pm - player_pm

            # Try to find matching DB game rows
            db_key = (player_id_str, team_abbr)
            game_rows = season_db.get(db_key, [])

            # Accept DB rows only when game count is within 1 of pbpstats GamesPlayed
            # (allows 1-game discrepancies from DNPs or minor source differences)
            use_real = bool(game_rows) and len(game_rows) >= pbpstats_n - 1

            if use_real:
                handled_db_keys.add(db_key)
                real_rows_used += 1
                n = len(game_rows)
                season_min = sum(_f(r[IDX_MINUTES]) for r in game_rows)

                for row in game_rows:
                    game_min = _f(row[IDX_MINUTES])
                    min_frac = (game_min / season_min) if season_min > 0 else (1.0 / n)
                    game_pm = _f(row[IDX_PM_ACTUAL])

                    row[IDX_ON_POSS] = on_poss_total * min_frac
                    row[IDX_OFF_POSS] = off_poss_total * min_frac
                    on_off_game = game_pm - pbpstats_off_diff / n
                    row[IDX_ON_OFF_ACTUAL] = on_off_game
                    row[IDX_ON_OFF_ADJ] = on_off_game
                    row[IDX_PM_ADJ] = game_pm

                chunk_rows.extend(game_rows)

            else:
                # Incomplete or missing DB data — use pbpstats fake rows
                # Mark the DB key as handled so partial DB rows aren't also appended
                if db_key in season_db:
                    handled_db_keys.add(db_key)
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
                            on_off_row=(2.0 * player_pm - team_pm) / n,
                        )
                    )

        # Include DB rows for players not found in pbpstats (keep their DB on/off)
        db_only = 0
        for db_key, game_rows in season_db.items():
            if db_key not in handled_db_keys:
                db_only += 1
                chunk_rows.extend(game_rows)

        slug = _season_slug(season)
        chunk_js = (
            "window.__PLAYER_SPAN_CHUNKS = window.__PLAYER_SPAN_CHUNKS || {};\n"
            f"window.__PLAYER_SPAN_CHUNKS[{json.dumps(season)}] = "
            f"{json.dumps(chunk_rows, ensure_ascii=False, separators=(',', ':'))};\n"
        )
        (CHUNK_DIR / f"{slug}.js").write_text(chunk_js, encoding="utf-8")
        total_pbp = real_rows_used + fake_rows_used
        print(
            f"  {season}: {total_pbp + db_only} player-teams, {len(chunk_rows)} rows"
            f" (real:{real_rows_used} fake:{fake_rows_used} db-only:{db_only})"
        )

    if cache_updated:
        _save_cache(cache)

    print(f"Done. Chunks written to {CHUNK_DIR}")


if __name__ == "__main__":
    generate()
