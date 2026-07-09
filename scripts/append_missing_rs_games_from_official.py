"""Fill regular-season gaps from the official (Eoin) box scores.

Two gap classes, both discovered by auditing the DB against official sources:

1. Whole games missing from every box source (7 games across 1997-98 ..
   2023-24, e.g. 29700945 CHH-UTA). Their full box lines are written to
   data/player_boxscore_manual_additions.csv, which the DB build unions into
   raw_player_box_stats.

2. Player-games present in box data but missing plus-minus because the on/off
   pipeline skipped them (15 games on 2026-03-08/09 whose possession data is
   also absent, plus stragglers). Official-box on/off rows are appended to
   adjusted_onoff_historical_pbp.csv: on_diff = official +/-, off_diff = team
   margin - on_diff, adjusted == raw (no possession data -> no luck
   adjustment; a zero delta is the neutral choice), minutes from the official
   box. The *_reconstructed columns mirror the raw ones (the DB publishes
   those). on/off points FOR/AGAINST splits stay empty -- they need
   possession data that does not exist for these games.

Idempotent: existing (game, player) rows are never duplicated.
"""
from __future__ import annotations

import os
import zipfile
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
DB = ROOT / "data" / "nba_analytics.duckdb"
EOIN_ZIP = ROOT / "historical-nba-data-and-player-box-scores.zip"
HIST_ONOFF = ROOT / "data" / "adjusted_onoff_historical_pbp.csv"
BOX_ADDITIONS = ROOT / "data" / "player_boxscore_manual_additions.csv"

ABBREV_OVERRIDES = {"SAN": "SAS", "NJ": "NJN"}


def load_eoin() -> tuple[pd.DataFrame, dict]:
    with zipfile.ZipFile(EOIN_ZIP) as z:
        with z.open("PlayerStatistics.csv") as f:
            eo = pd.read_csv(f, low_memory=False)
        with z.open("TeamHistories.csv") as f:
            th = pd.read_csv(f, low_memory=False)
    eo = eo[eo["gameType"] == "Regular Season"].copy()
    eo["gid"] = eo["gameId"].astype(str).str.lstrip("0")
    eo["d"] = pd.to_datetime(eo["gameDateTimeEst"], errors="coerce")
    eo["date"] = eo["d"].dt.strftime("%Y-%m-%d")
    eo["pid"] = pd.to_numeric(eo["personId"], errors="coerce")
    eo = eo.dropna(subset=["pid"])
    eo["pid"] = eo["pid"].astype(int)
    eo["mins"] = pd.to_numeric(eo["numMinutes"], errors="coerce").fillna(0.0)
    eo["pm"] = pd.to_numeric(eo["plusMinusPoints"], errors="coerce")

    th = th[th["league"].astype(str).str.lower() == "nba"]
    abbr = {}
    for r in th.itertuples(index=False):
        try:
            tid, y0, y1 = int(r.teamId), int(r.seasonFounded), int(r.seasonActiveTill)
        except (TypeError, ValueError):
            continue
        a = ABBREV_OVERRIDES.get(str(r.teamAbbrev).strip(), str(r.teamAbbrev).strip())
        for y in range(y0, y1 + 1):
            abbr[(tid, y)] = a
    return eo, abbr


def main() -> None:
    con = duckdb.connect(str(DB), read_only=True)
    our_games = {r[0] for r in con.execute(
        "SELECT DISTINCT CAST(game_id AS VARCHAR) FROM player_game_facts WHERE CAST(game_id AS VARCHAR) LIKE '2%'").fetchall()}
    missing_pm = con.execute("""
        SELECT CAST(game_id AS VARCHAR) gid, CAST(player_id AS BIGINT) pid
        FROM player_game_facts
        WHERE CAST(game_id AS VARCHAR) LIKE '2%' AND plus_minus_actual IS NULL AND minutes > 0
    """).df()
    con.close()

    print("Loading Eoin...")
    eo, abbr_map = load_eoin()
    eo = eo[eo["d"] >= "1996-09-01"]
    eo["season_year"] = (eo["d"].dt.year - (eo["d"].dt.month < 10)).astype(int)

    played = eo[eo["mins"] > 0]

    # --- 1. whole missing games -> box additions ------------------------------
    miss_games = sorted(set(played["gid"]) - our_games)
    miss_games = [g for g in miss_games if g.startswith("2")]  # RS namespace only
    box_rows = []
    for gid in miss_games:
        g = played[played["gid"] == gid]
        for r in g.itertuples(index=False):
            tid = int(r.playerteamId)
            box_rows.append({
                "date": r.date, "game_id": gid, "team_id": tid,
                "team_abbr": abbr_map.get((tid, int(r.season_year)), ""),
                "player_id": int(r.pid),
                "player_name": f"{r.firstName} {r.lastName}".strip(),
                "starter": 1 if (pd.notna(r.startingPosition) and str(r.startingPosition).strip()) else 0,
                "minutes": round(float(r.mins), 2),
                "pts": int(r.points or 0), "reb": int(r.reboundsTotal or 0),
                "oreb": int(r.reboundsOffensive or 0), "dreb": int(r.reboundsDefensive or 0),
                "ast": int(r.assists or 0), "stl": int(r.steals or 0),
                "blk": int(r.blocks or 0), "tov": int(r.turnovers or 0),
                "pf": int(r.foulsPersonal or 0),
                "fgm": int(r.fieldGoalsMade or 0), "fga": int(r.fieldGoalsAttempted or 0),
                "fg3m": int(r.threePointersMade or 0), "fg3a": int(r.threePointersAttempted or 0),
                "ftm": int(r.freeThrowsMade or 0), "fta": int(r.freeThrowsAttempted or 0),
            })
    box_df = pd.DataFrame(box_rows)
    box_df.to_csv(BOX_ADDITIONS, index=False)
    print(f"missing games: {miss_games}")
    print(f"wrote {len(box_df)} box rows to {BOX_ADDITIONS}")

    # --- 2. official-pm on/off rows for player-games lacking pm ---------------
    # (covers the missing games too, so they get pm the same way)
    targets = set(zip(missing_pm["gid"], missing_pm["pid"]))
    for gid in miss_games:
        for r in played[played["gid"] == gid].itertuples(index=False):
            targets.add((gid, int(r.pid)))

    hist = pd.read_csv(HIST_ONOFF, dtype={"game_id": str}, low_memory=False)
    have = set(zip(hist["game_id"], hist["player_id"].astype(int)))
    targets -= have
    print(f"player-games needing official-pm on/off rows: {len(targets)}")
    if not targets:
        return

    target_gids = {g for g, _ in targets}
    sub = played[played["gid"].isin(target_gids)].copy()
    team_pts = sub.groupby(["gid", "playerteamId"])["points"].sum()
    game_pts = sub.groupby("gid")["points"].sum()

    new_rows = []
    skipped = 0
    for r in sub.itertuples(index=False):
        key = (r.gid, int(r.pid))
        if key not in targets:
            continue
        if pd.isna(r.pm):
            skipped += 1
            continue
        tid = int(r.playerteamId)
        margin = 2 * float(team_pts[(r.gid, tid)]) - float(game_pts[r.gid])
        on = float(r.pm)
        off = margin - on
        new_rows.append({
            "game_id": r.gid, "team_id": tid, "player_id": int(r.pid),
            "player_name": f"{r.firstName} {r.lastName}".strip(),
            "on_pts_for": None, "on_pts_against": None, "on_diff": on,
            "off_pts_for": None, "off_pts_against": None, "off_diff": off,
            "on_pts_for_adj": None, "on_pts_against_adj": None, "on_diff_adj": on,
            "off_pts_for_adj": None, "off_pts_against_adj": None, "off_diff_adj": off,
            "on_off_diff": on - off, "on_off_diff_adj": on - off,
            "on_diff_reconstructed": on, "off_diff_reconstructed": off,
            "on_off_diff_reconstructed": on - off,
            "minutes_on": round(float(r.mins), 2), "date": r.date,
        })
    if skipped:
        print(f"  skipped {skipped} rows with no official +/- value")
    add = pd.DataFrame(new_rows)[hist.columns.tolist()]
    out = pd.concat([hist, add], ignore_index=True)
    out.to_csv(HIST_ONOFF, index=False)
    print(f"appended {len(add)} rows to {HIST_ONOFF} ({len(hist)} -> {len(out)})")


if __name__ == "__main__":
    main()
