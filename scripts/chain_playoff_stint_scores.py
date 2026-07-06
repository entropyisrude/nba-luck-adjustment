"""Chain playoff stint score snapshots so every point is attributed to a lineup.

The stint parser snapshots the score at the first tracked event *inside* each
stint, so points scored before that event (game-opening baskets, points right
after a substitution or period break) belong to no stint: ~2.5-3.3 points per
game leaked out of every player's on-court plus-minus (97% of games' first
stints started at a nonzero score; 1,712 mid-game gaps).

Fix: set each stint's start score to the previous stint's end score (0-0 for
the first stint), and recompute the stint's points as end minus start. The
adjusted (luck-adjusted) scores are chained the same way. Gaps larger than
MAX_CHAIN_GAP points are left alone -- those are real coverage holes (e.g.
missing overtime periods) where attributing the points to the next lineup
would be wrong.

Idempotent: chaining already-chained stints is a no-op.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
STINTS_PATH = ROOT / "data" / "stints_playoffs.csv"

MAX_CHAIN_GAP = 8  # points; bigger jumps are real coverage holes, not snapshot drift


def chain_stint_scores(stints: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stints = stints.sort_values(["game_id", "stint_index"]).reset_index(drop=True)
    stats = {"chained_starts": 0, "points_recovered": 0.0, "holes_kept": 0}

    grp = stints.groupby("game_id", sort=False)
    for _, idx in grp.indices.items():
        idx = sorted(idx)
        ph, pa = 0.0, 0.0            # previous end (raw)
        pha, paa = 0.0, 0.0          # previous end (adjusted)
        for i in idx:
            sh = stints.at[i, "start_home_score"]
            sa = stints.at[i, "start_away_score"]
            gap = (sh - ph) + (sa - pa)
            if 0 < gap <= MAX_CHAIN_GAP:
                stints.at[i, "start_home_score"] = ph
                stints.at[i, "start_away_score"] = pa
                stints.at[i, "start_home_score_adj"] = pha
                stints.at[i, "start_away_score_adj"] = paa
                stats["chained_starts"] += 1
                stats["points_recovered"] += gap
            elif gap > MAX_CHAIN_GAP:
                stats["holes_kept"] += 1
            stints.at[i, "home_pts"] = stints.at[i, "end_home_score"] - stints.at[i, "start_home_score"]
            stints.at[i, "away_pts"] = stints.at[i, "end_away_score"] - stints.at[i, "start_away_score"]
            stints.at[i, "home_pts_adj"] = stints.at[i, "end_home_score_adj"] - stints.at[i, "start_home_score_adj"]
            stints.at[i, "away_pts_adj"] = stints.at[i, "end_away_score_adj"] - stints.at[i, "start_away_score_adj"]
            ph = stints.at[i, "end_home_score"]
            pa = stints.at[i, "end_away_score"]
            pha = stints.at[i, "end_home_score_adj"]
            paa = stints.at[i, "end_away_score_adj"]
    return stints, stats


def main() -> None:
    print(f"Loading {STINTS_PATH}...")
    stints = pd.read_csv(STINTS_PATH, dtype={"game_id": str})
    n_games = stints["game_id"].nunique()
    print(f"  {len(stints)} stints, {n_games} games")

    stints, stats = chain_stint_scores(stints)
    print(f"  chained {stats['chained_starts']} stint starts, "
          f"recovered {stats['points_recovered']:.0f} pts, "
          f"kept {stats['holes_kept']} coverage holes")

    # invariant: per game, sum of stint pts == final score minus remaining holes
    check = stints.groupby("game_id").agg(
        sh=("home_pts", "sum"), fh=("end_home_score", "max"),
        sa=("away_pts", "sum"), fa=("end_away_score", "max"))
    leak = ((check["fh"] - check["sh"]) + (check["fa"] - check["sa"]))
    print(f"  residual leakage: {int((leak > 0.5).sum())} games, {leak.sum():.0f} pts total")

    stints.to_csv(STINTS_PATH, index=False)
    print(f"Wrote {STINTS_PATH}")


if __name__ == "__main__":
    main()
