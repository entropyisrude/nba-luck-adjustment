"""Generate playoff span search chunks using pbpstats for on/off and box stats.

For seasons >= 1996-97, pbpstats provides complete season-level totals including
all games. For each player-season, we create GamesPlayed synthetic rows (each
holding 1/N of the season totals). The JS aggregates rows by summing counts and
reconstructing on/off via the possession-based formula — so N fake rows yield
the correct game count and correct on/off.

For 1995-96, pbpstats coverage is absent; those chunks are left untouched.

JS on/off formula (from player_span_search_playoffs.html):
  offDiffActual = plusMinusActual - onOffActual  (per row)
  g.off_diff_actual_total += offDiffActual
  onOffActual = (100*g.plus_minus_actual/g.on_possessions)
              - (100*g.off_diff_actual_total/g.off_possessions)

Setting per-row values as:
  pm_row       = player.PlusMinus / N
  on_off_row   = (2*player.PlusMinus - team.PlusMinus) / N
  on_poss_row  = (player.OffPoss + player.DefPoss) / 2 / N
  off_poss_row = ((team.OffPoss+team.DefPoss)/2 - on_poss) / N

Gives after JS accumulation:
  g.plus_minus_actual    = player.PlusMinus
  g.on_possessions       = (player.OffPoss+player.DefPoss)/2
  g.off_diff_actual_total = team.PlusMinus - player.PlusMinus
  g.off_possessions      = (team.OffPoss+team.DefPoss)/2 - on_poss
  onOffActual = 100*player.PlusMinus/on_poss - 100*(team.PlusMinus-player.PlusMinus)/off_poss ✓
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


def _f(v: object) -> float:
    try:
        return float(v)  # type: ignore[arg-type]
    except Exception:
        return 0.0


def _season_start(season: str) -> int:
    return int(str(season).split("-")[0])


def _season_slug(season: str) -> str:
    return season.replace("-", "_")


def _fetch_json(url: str, params: dict, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=PBP_TIMEOUT)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(2**attempt)
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
    """Returns (player_rows, team_rows) from pbpstats for a playoff season."""
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
    """Returns map keyed by (team_id_str, player_id_str) with player+team records."""
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
        out[(team_id, player_id)] = {"player": pr, "team": team}
    return out


def _div(a: float, b: float) -> float | None:
    return a / b if b > 0 else None


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


def _make_row(
    *,
    fake_date: str,
    season: str,
    fake_game_id: str,
    player_id: int,
    player_name: str,
    team_abbr: str,
    minutes: float,
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
    fg2_pct: float | None,
    fg3m: float,
    fg3a: float,
    fg3_pct: float | None,
    ftm: float,
    fta: float,
    ft_pct: float | None,
    listed_height: str | None,
    height_inches: int | None,
    age: int | None,
    career_year: int | None,
    draft_year: int | None,
    draft_overall_pick: int | None,
    rim_anchor: float | None,
    rim_det: float | None,
    rim_dfga_row: float | None,
    rim_tracking_games: float | None,
    rim_dfg_pct: float | None,
    rim_dfg_pct_diff: float | None,
    on_poss_row: float,
    off_poss_row: float,
    pm_row: float,
    on_off_row: float,
) -> list:
    return [
        fake_date,          # 0: date
        season,             # 1: season
        fake_game_id,       # 2: game_id
        player_id,          # 3: player_id
        player_name,        # 4: player_name
        team_abbr,          # 5: team_abbr
        None,               # 6: opp_team_abbr
        None,               # 7: home_away
        None,               # 8: win_loss
        None,               # 9: starter
        minutes / n,        # 10: minutes
        pts / n,            # 11: pts
        reb / n,            # 12: reb
        oreb / n,           # 13: oreb
        dreb / n,           # 14: dreb
        ast / n,            # 15: ast
        stl / n,            # 16: stl
        blk / n,            # 17: blk
        tov / n,            # 18: tov
        pf / n,             # 19: pf
        fgm / n,            # 20: fgm
        fga / n,            # 21: fga
        fg2m / n,           # 22: fg2m
        fg2a / n,           # 23: fg2a
        fg2_pct,            # 24: fg2_pct (season-level; JS recomputes from accumulated totals)
        fg3m / n,           # 25: fg3m
        fg3a / n,           # 26: fg3a
        fg3_pct,            # 27: fg3_pct
        ftm / n,            # 28: ftm
        fta / n,            # 29: fta
        ft_pct,             # 30: ft_pct
        None,               # 31: assisted_2pm
        None,               # 32: unassisted_2pm
        None,               # 33: assisted_3pm
        None,               # 34: unassisted_3pm
        None,               # 35: assisted_fgm
        None,               # 36: unassisted_fgm
        listed_height,      # 37: listed_height
        height_inches,      # 38: height_inches
        age,                # 39: age
        career_year,        # 40: career_year
        draft_year,         # 41: draft_year
        draft_overall_pick, # 42: draft_overall_pick
        None,               # 43: layup_assists_created
        None,               # 44: dunk_assists_created
        None,               # 45: other_rim_assists_created
        None,               # 46: rim_assists_strict
        None,               # 47: rim_assists_all
        None,               # 48: rim_assists_season_games
        None,               # 49: layup_assists_created_per_game
        None,               # 50: dunk_assists_created_per_game
        None,               # 51: other_rim_assists_created_per_game
        None,               # 52: rim_assists_strict_per_game
        None,               # 53: rim_assists_all_per_game
        rim_anchor,         # 54: rim_anchor_signature (season-level; JS dedupes by season)
        rim_det,            # 55: rim_deterrence_signature
        rim_dfga_row,       # 56: rim_dfga (prorated per row)
        rim_tracking_games, # 57: rim_tracking_games
        rim_dfg_pct,        # 58: rim_dfg_pct
        rim_dfg_pct_diff,   # 59: rim_dfg_pct_diff
        None,               # 60: contested_shots
        None,               # 61: contested_shots_2pt
        None,               # 62: contested_shots_3pt
        None,               # 63: deflections
        None,               # 64: charges_drawn
        None,               # 65: screen_assists
        None,               # 66: screen_ast_pts
        None,               # 67: loose_balls_recovered
        None,               # 68: box_outs
        on_poss_row,        # 69: on_possessions
        off_poss_row,       # 70: off_possessions
        pm_row,             # 71: plus_minus_actual
        pm_row,             # 72: plus_minus_adjusted (= raw; no independent adj available)
        on_off_row,         # 73: on_off_actual
        on_off_row,         # 74: on_off_adjusted (= raw)
    ]


def generate() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    bio_map = _load_bio_from_db(con)
    rim_map = _load_rim_from_db(con)
    seasons_in_db = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT season FROM player_game_facts ORDER BY season"
        ).fetchall()
    ]
    con.close()

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
        start_year = _season_start(season)
        fake_date = f"{start_year + 1}-06-15"

        chunk_rows: list[list] = []

        for (team_id_str, player_id_str), entry in sorted(
            pbp_index.items(), key=lambda kv: str(kv[1]["player"].get("Name", ""))
        ):
            pr = entry["player"]
            team = entry["team"]

            player_id_int = int(player_id_str) if player_id_str.isdigit() else 0
            n = max(int(_f(pr.get("GamesPlayed", 1))), 1)

            # Box stats
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
            ftm = _f(pr.get("FtPoints"))  # FtPoints = sum of FT makes (each scores 1 pt)
            fta = _f(pr.get("FTA"))
            minutes = _f(pr.get("Minutes"))

            # On/off
            player_pm = _f(pr.get("PlusMinus"))
            team_pm = _f(team.get("PlusMinus"))
            on_off_poss = _f(pr.get("OffPoss"))
            on_def_poss = _f(pr.get("DefPoss"))
            team_off_poss = _f(team.get("OffPoss"))
            team_def_poss = _f(team.get("DefPoss"))

            on_poss_total = (on_off_poss + on_def_poss) / 2.0
            off_poss_total = max((team_off_poss + team_def_poss) / 2.0 - on_poss_total, 0.0)
            on_off_raw_total = 2.0 * player_pm - team_pm

            # Bio and rim from DB
            bio = bio_map.get(player_id_int, {})
            rim = rim_map.get((season, player_id_int), {})
            rim_dfga_total = rim.get("rim_dfga")

            team_abbr = str(pr.get("TeamAbbreviation") or "")
            player_name = str(pr.get("Name") or "")

            for i in range(n):
                chunk_rows.append(
                    _make_row(
                        fake_date=fake_date,
                        season=season,
                        fake_game_id=f"pbp_{_season_slug(season)}_{player_id_str}_{i + 1}",
                        player_id=player_id_int,
                        player_name=player_name,
                        team_abbr=team_abbr,
                        minutes=minutes,
                        n=n,
                        pts=pts,
                        reb=reb,
                        oreb=oreb,
                        dreb=dreb,
                        ast=ast,
                        stl=stl,
                        blk=blk,
                        tov=tov,
                        pf=pf,
                        fgm=fgm,
                        fga=fga,
                        fg2m=fg2m,
                        fg2a=fg2a,
                        fg2_pct=_div(fg2m, fg2a),
                        fg3m=fg3m,
                        fg3a=fg3a,
                        fg3_pct=_div(fg3m, fg3a),
                        ftm=ftm,
                        fta=fta,
                        ft_pct=_div(ftm, fta),
                        listed_height=bio.get("listed_height"),
                        height_inches=bio.get("height_inches"),
                        age=bio.get("age"),
                        career_year=bio.get("career_year"),
                        draft_year=bio.get("draft_year"),
                        draft_overall_pick=bio.get("draft_overall_pick"),
                        rim_anchor=rim.get("rim_anchor_signature"),
                        rim_det=rim.get("rim_deterrence_signature"),
                        rim_dfga_row=(rim_dfga_total / n) if rim_dfga_total is not None else None,
                        rim_tracking_games=rim.get("rim_tracking_games"),
                        rim_dfg_pct=rim.get("rim_dfg_pct"),
                        rim_dfg_pct_diff=rim.get("rim_dfg_pct_diff"),
                        on_poss_row=on_poss_total / n,
                        off_poss_row=off_poss_total / n,
                        pm_row=player_pm / n,
                        on_off_row=on_off_raw_total / n,
                    )
                )

        slug = _season_slug(season)
        filename = f"{slug}.js"
        chunk_js = (
            "window.__PLAYER_SPAN_CHUNKS = window.__PLAYER_SPAN_CHUNKS || {};\n"
            f"window.__PLAYER_SPAN_CHUNKS[{json.dumps(season)}] = "
            f"{json.dumps(chunk_rows, ensure_ascii=False, separators=(',', ':'))};\n"
        )
        (CHUNK_DIR / filename).write_text(chunk_js, encoding="utf-8")
        player_count = len(pbp_index)
        print(f"  {season}: {player_count} player-teams, {len(chunk_rows)} rows")

    if cache_updated:
        _save_cache(cache)

    print(f"Done. Chunks written to {CHUNK_DIR}")


if __name__ == "__main__":
    generate()
