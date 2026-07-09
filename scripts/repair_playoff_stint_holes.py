"""One-off repairs for playoff games whose stint data has coverage holes.

1. Drop the stints of 49600063 (1997 SEA-HOU) and 49700045 (1998 CHI-CHH):
   their source PBP is scrambled (periods/clocks shuffled), which produced
   stints with NEGATIVE total points -- unusable noise for RAPM. Their
   player-level numbers are unaffected: the on/off rows for these games are
   anchored to official box scores and generate_playoff_onoff.py preserves
   rows for games absent from stint data.

2. Synthesize the missing overtime stints for 41900161 (2020 bubble DEN-UTA,
   135-125 OT) from possessions_playoffs.csv, which covers the OT completely
   (23 possessions, exactly the missing 30 points, real 10-man lineups).
   Points come from possession points (and points_adj for the luck-adjusted
   columns); stint seconds are the 300s OT split by possession share (the
   possession log has no clock). After this the game passes calibration.

Run once after chaining, before calibration. Safe to rerun (dropped games
stay dropped; the OT stints are only added if period-5 stints are absent).
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
STINTS_PATH = ROOT / "data" / "stints_playoffs.csv"
POSS_PATH = ROOT / "data" / "possessions_playoffs.csv"

CORRUPTED = ["49600063", "49700045"]
BUBBLE_OT_GAME = "41900161"
OT_SECONDS = 300.0


def synthesize_ot_stints(stints: pd.DataFrame) -> pd.DataFrame:
    g = stints[stints["game_id"] == BUBBLE_OT_GAME]
    if not len(g) or (g["end_period"] >= 5).any():
        print(f"  {BUBBLE_OT_GAME}: OT stints already present or game missing -- skipping")
        return stints

    poss = pd.read_csv(POSS_PATH, dtype={"game_id": str})
    ot = poss[(poss["game_id"] == BUBBLE_OT_GAME) & (poss["period"] > 4)].sort_values("poss_index")
    if not len(ot):
        print(f"  {BUBBLE_OT_GAME}: no OT possessions found -- skipping")
        return stints

    home_id = int(g["home_id"].iloc[0])
    last = g.sort_values("stint_index").iloc[-1]

    # group consecutive possessions with the same 10-man floor unit
    def unit(row):
        off = tuple(sorted(int(row[f"off_p{i}"]) for i in range(1, 6)))
        de = tuple(sorted(int(row[f"def_p{i}"]) for i in range(1, 6)))
        home_side = off if int(row["offense_team"]) == home_id else de
        away_side = de if home_side is off else off
        return (home_side, away_side)

    rows = []
    cur_unit, cur = None, None
    seq = []
    for _, r in ot.iterrows():
        u = unit(r)
        if u != cur_unit:
            if cur is not None:
                seq.append(cur)
            cur_unit = u
            cur = {"home": list(u[0]), "away": list(u[1]), "n": 0,
                   "h": 0.0, "a": 0.0, "h_adj": 0.0, "a_adj": 0.0}
        pts = float(r.get("points") or 0)
        pts_adj = float(r.get("points_adj") or 0)
        if int(r["offense_team"]) == home_id:
            cur["h"] += pts; cur["h_adj"] += pts_adj
        else:
            cur["a"] += pts; cur["a_adj"] += pts_adj
        cur["n"] += 1
    if cur is not None:
        seq.append(cur)

    n_poss = sum(s["n"] for s in seq)
    sh, sa = float(last["end_home_score"]), float(last["end_away_score"])
    sh_adj, sa_adj = float(last["end_home_score_adj"]), float(last["end_away_score_adj"])
    elapsed = float(last["end_elapsed"])
    out = []
    idx = int(last["stint_index"])
    clock = OT_SECONDS
    for s in seq:
        idx += 1
        secs = OT_SECONDS * s["n"] / n_poss
        row = {c: None for c in stints.columns}
        row.update({
            "game_id": BUBBLE_OT_GAME, "stint_index": idx,
            "home_id": home_id, "away_id": int(g["away_id"].iloc[0]),
            "seconds": round(secs, 1),
            "home_pts": s["h"], "away_pts": s["a"],
            "home_pts_adj": s["h_adj"], "away_pts_adj": s["a_adj"],
            "start_elapsed": round(elapsed, 1), "end_elapsed": round(elapsed + secs, 1),
            "start_period": 5, "end_period": 5,
            "start_clock": f"{int(clock//60)}:{int(clock%60):02d}",
            "end_clock": f"{int((clock-secs)//60)}:{int(max(0,clock-secs)%60):02d}",
            "start_home_score": sh, "start_away_score": sa,
            "end_home_score": sh + s["h"], "end_away_score": sa + s["a"],
            "start_home_score_adj": sh_adj, "start_away_score_adj": sa_adj,
            "end_home_score_adj": sh_adj + s["h_adj"], "end_away_score_adj": sa_adj + s["a_adj"],
            "date": g["date"].iloc[0],
        })
        for i, p in enumerate(s["home"], 1):
            row[f"home_p{i}"] = p
        for i, p in enumerate(s["away"], 1):
            row[f"away_p{i}"] = p
        out.append(row)
        sh += s["h"]; sa += s["a"]; sh_adj += s["h_adj"]; sa_adj += s["a_adj"]
        elapsed += secs; clock -= secs

    print(f"  {BUBBLE_OT_GAME}: appended {len(out)} OT stints "
          f"({sum(s['h'] for s in seq):.0f}-{sum(s['a'] for s in seq):.0f} in OT, "
          f"final {sh:.0f}-{sa:.0f})")
    return pd.concat([stints, pd.DataFrame(out)], ignore_index=True)


def main() -> None:
    stints = pd.read_csv(STINTS_PATH, dtype={"game_id": str})
    n0 = len(stints)

    drop = stints["game_id"].isin(CORRUPTED)
    if drop.any():
        print(f"  dropping {int(drop.sum())} unusable stints from {CORRUPTED} "
              "(scrambled source PBP; on/off rows for these games are preserved elsewhere)")
        stints = stints[~drop].copy()

    stints = synthesize_ot_stints(stints)
    stints = stints.sort_values(["game_id", "stint_index"]).reset_index(drop=True)
    stints.to_csv(STINTS_PATH, index=False)
    print(f"Wrote {STINTS_PATH} ({n0} -> {len(stints)} stints)")


if __name__ == "__main__":
    main()
