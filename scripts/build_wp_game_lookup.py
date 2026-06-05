"""
Build wp_game_lookup.js for the "Find a Real Game" feature.

For each (score_diff, time_bin) cell stores a sample of real RS games that
passed through that state, including the actual score at that moment.

Cell entries: [game_idx, home_score_at_time, away_score_at_time]

Bins:
  - diff: integer points, clamped to ±25
  - time: nearest 30 seconds

Output: data/wp_game_lookup.js
"""
from __future__ import annotations
import json, random
from pathlib import Path
import datetime

import duckdb
import numpy as np
import pandas as pd

ROOT         = Path(__file__).parent.parent
RS_DB        = ROOT / "data" / "nba_analytics.duckdb"
OUT_JS       = ROOT / "data" / "wp_game_lookup.js"

TIME_BIN     = 30
MAX_PER_CELL = 40
DIFF_CLAMP   = 25
MIN_TIME_SEC = 10   # matches wp_fit_model.py

random.seed(42)

def snap_t(t: float) -> int:
    return int(round(float(t) / TIME_BIN) * TIME_BIN)

def snap_d(d: float) -> int:
    return int(max(-DIFF_CLAMP, min(DIFF_CLAMP, round(float(d)))))

def fmt_date(s: str) -> str:
    try:
        dt = datetime.date.fromisoformat(str(s)[:10])
        return dt.strftime(f"%b {dt.day}, %Y")
    except Exception:
        return str(s)[:10]

def run():
    con = duckdb.connect(str(RS_DB), read_only=True)

    # ── Game metadata (teams + final scores) ─────────────────────────────────
    print("Querying game metadata...")
    meta = con.execute("""
        WITH teams AS (
            SELECT game_id,
                   MAX(CASE WHEN home_away = 'home' THEN team_abbr END) AS home_abbr,
                   MAX(CASE WHEN home_away = 'away' THEN team_abbr END) AS away_abbr,
                   ANY_VALUE(CAST(date AS VARCHAR))                      AS date_str
            FROM player_game_facts
            WHERE LEFT(game_id, 1) = '2'
            GROUP BY game_id
        ),
        scores AS (
            SELECT game_id,
                   MAX(end_home_score) AS final_home,
                   MAX(end_away_score) AS final_away
            FROM lineup_stint_facts
            WHERE LEFT(game_id, 1) = '2'
              AND end_home_score IS NOT NULL AND end_away_score IS NOT NULL
            GROUP BY game_id
            HAVING MAX(end_home_score) != MAX(end_away_score)
        )
        SELECT t.game_id, t.home_abbr, t.away_abbr, t.date_str,
               s.final_home, s.final_away
        FROM teams t JOIN scores s ON t.game_id = s.game_id
        WHERE t.home_abbr IS NOT NULL AND t.away_abbr IS NOT NULL
    """).df()
    meta = meta.dropna(subset=["home_abbr", "away_abbr"])
    meta["final_home"] = meta["final_home"].astype(int)
    meta["final_away"] = meta["final_away"].astype(int)
    meta = meta.set_index("game_id")
    print(f"  {len(meta):,} RS games with team data")

    # ── Observations with actual in-game scores ───────────────────────────────
    print("Querying in-game observations with scores...")
    obs = con.execute(f"""
        WITH finals AS (
            SELECT game_id,
                   MAX(end_home_score) AS final_home,
                   MAX(end_away_score) AS final_away
            FROM lineup_stint_facts
            WHERE LEFT(game_id, 1) = '2' AND end_home_score IS NOT NULL
            GROUP BY game_id
            HAVING MAX(end_home_score) != MAX(end_away_score)
        )
        SELECT
            s.game_id,
            2880 - s.start_elapsed                          AS time_remaining,
            s.start_home_score - s.start_away_score         AS score_diff,
            CAST(s.start_home_score AS INTEGER)              AS home_score,
            CAST(s.start_away_score AS INTEGER)              AS away_score,
            CASE WHEN f.final_home > f.final_away THEN 1
                 ELSE 0 END                                  AS home_won
        FROM lineup_stint_facts s
        JOIN finals f ON s.game_id = f.game_id
        WHERE LEFT(s.game_id, 1) = '2'
          AND s.start_elapsed > {MIN_TIME_SEC}
          AND s.start_elapsed < 2880
          AND s.start_home_score IS NOT NULL
          AND s.start_away_score IS NOT NULL
    """).df()
    con.close()

    print(f"  {len(obs):,} obs  |  {obs['game_id'].nunique():,} unique games")

    obs = obs[obs["game_id"].isin(meta.index)].copy()
    obs["t_bin"] = obs["time_remaining"].map(snap_t)
    obs["d_bin"] = obs["score_diff"].map(snap_d)

    # One entry per (game, diff_bin, time_bin) — keep first occurrence for scores
    obs = obs.drop_duplicates(subset=["game_id", "d_bin", "t_bin"])
    print(f"  {len(obs):,} unique (game, diff, time) triples after dedup")

    # ── Master game list ──────────────────────────────────────────────────────
    print("Building master game list...")
    game_ids = obs["game_id"].unique()
    game_idx = {gid: i for i, gid in enumerate(game_ids)}

    games = []
    for gid in game_ids:
        m = meta.loc[gid]
        games.append([
            str(m["home_abbr"]),
            str(m["away_abbr"]),
            fmt_date(m["date_str"]),
            int(m["final_home"]),
            int(m["final_away"]),
        ])

    # ── Cell lookup ───────────────────────────────────────────────────────────
    print("Building cell lookup...")
    cells: dict[str, dict] = {}

    for (d_bin, t_bin), grp in obs.groupby(["d_bin", "t_bin"]):
        if t_bin <= 0 or t_bin > 2880:
            continue

        key = f"{int(d_bin)}_{int(t_bin)}"

        # Each entry: [game_idx, home_score_at_time, away_score_at_time]
        def make_entries(sub):
            return [
                [game_idx[row.game_id], int(row.home_score), int(row.away_score)]
                for row in sub.itertuples()
                if row.game_id in game_idx
            ]

        all_entries = make_entries(grp)

        if d_bin < 0:
            cb_entries = make_entries(grp[grp["home_won"] == 1])
        elif d_bin > 0:
            cb_entries = make_entries(grp[grp["home_won"] == 0])
        else:
            cb_entries = []

        if len(all_entries) > MAX_PER_CELL:
            all_entries = random.sample(all_entries, MAX_PER_CELL)
        if len(cb_entries) > MAX_PER_CELL:
            cb_entries = random.sample(cb_entries, MAX_PER_CELL)

        cells[key] = {"a": all_entries, "c": cb_entries}

    # ── Write ─────────────────────────────────────────────────────────────────
    output = {"games": games, "cells": cells, "time_bin": TIME_BIN}
    payload = json.dumps(output, separators=(",", ":"))
    with open(OUT_JS, "w", encoding="utf-8") as f:
        f.write(f"window.WP_GAME_LOOKUP={payload};")

    kb = OUT_JS.stat().st_size / 1024
    print(f"\nSaved {OUT_JS.name}  ({kb:.0f} KB)")

    sizes_all = [len(v["a"]) for v in cells.values()]
    sizes_cb  = [len(v["c"]) for v in cells.values()]
    print(f"Cells: {len(cells):,}  |  "
          f"avg games/cell: {np.mean(sizes_all):.1f}  |  "
          f"avg comebacks/cell: {np.mean(sizes_cb):.1f}  |  "
          f"cells with comeback: {sum(1 for x in sizes_cb if x > 0):,}")

if __name__ == "__main__":
    run()
