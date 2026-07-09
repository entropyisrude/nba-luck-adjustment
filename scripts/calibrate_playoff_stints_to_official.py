"""Calibrate playoff stint scoring so it reproduces the official box plus-minus.

After chaining (scripts/chain_playoff_stint_scores.py) the stint data still
carries substitution-boundary noise: points scored around a sub get booked to
the neighboring stint, so each player's stint-sum plus-minus was off the
official box +/- by ~0.9 points per game on average. RAPM regresses directly
on per-stint home_pts/away_pts, so that noise flows into RAPM.

Fix: per game, find the minimal L2 adjustment to per-stint (home_pts,
away_pts) such that

  * every player's on-court sum of (own - opp) equals their official box +/-,
  * each team's stint points sum to the official final score.

This is an underdetermined linear system (more stints than constraints), so
the minimum-norm solution exists and spreads each correction over the stints
the affected players share -- no single stint moves much. The luck-adjusted
columns are shifted by the same deltas, preserving each stint's luck delta.
Score snapshots (start_*/end_*) are left as the parser recorded them.

Games whose stint coverage cannot reach the official totals (missing OT
periods / truncated parses -- see data/audits/) are left untouched, as are
player constraints for players the stint data never saw on the floor.

Official sources: Kaggle traditional box scores (exact game_id/player_id
namespace), Eoin PlayerStatistics for games newer than the Kaggle dump.
Safe to rerun (a calibrated file is already at the constraints -> no-op).
"""
from __future__ import annotations

import os
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
STINTS_PATH = ROOT / "data" / "stints_playoffs.csv"
_KAGGLE_CANDIDATES = [
    Path(os.environ["NBA_BOX_ROOT"]) / "kaggle-traditional" / "traditional.csv"
    if os.environ.get("NBA_BOX_ROOT") else None,
    Path(r"C:\Users\Dave\Downloads\nba-boxscore-data\kaggle-traditional\traditional.csv"),
    Path("/mnt/c/users/dave/Downloads/nba-boxscore-data/kaggle-traditional/traditional.csv"),
]
KAGGLE_BOX = next((p for p in _KAGGLE_CANDIDATES if p is not None and p.exists()), None)
EOIN_ZIP = ROOT / "historical-nba-data-and-player-box-scores.zip"

TOTAL_TOL = 3.0     # skip games whose stint totals are further than this from official
HOME_COLS = [f"home_p{i}" for i in range(1, 6)]
AWAY_COLS = [f"away_p{i}" for i in range(1, 6)]


def load_official() -> tuple[dict, dict]:
    """Returns (pm, totals): pm[(gid, pid)] = official +/-,
    totals[gid] = (home_pts, away_pts) keyed by our 8-digit game ids."""
    pm: dict[tuple[str, int], float] = {}
    totals: dict[str, tuple[float, float]] = {}

    if KAGGLE_BOX is not None:
        kag = pd.read_csv(KAGGLE_BOX, usecols=["gameid", "type", "playerid", "team",
                                               "home", "away", "PTS", "+/-"],
                          dtype={"gameid": str}, low_memory=False)
        kag = kag[kag["type"].str.lower() == "playoff"].copy()
        kag["pid"] = pd.to_numeric(kag["playerid"], errors="coerce")
        kag["pm"] = pd.to_numeric(kag["+/-"], errors="coerce")
        kag = kag.dropna(subset=["pid", "pm"])
        for r in kag.itertuples(index=False):
            pm[(r.gameid, int(r.pid))] = float(r.pm)
        tp = kag.groupby(["gameid", "team"])["PTS"].sum()
        meta = kag.drop_duplicates("gameid")[["gameid", "home", "away"]]
        for r in meta.itertuples(index=False):
            try:
                totals[r.gameid] = (float(tp[(r.gameid, r.home)]), float(tp[(r.gameid, r.away)]))
            except KeyError:
                pass
        print(f"  kaggle: {len(totals)} games")

    kaggle_games = {gid for gid, _ in pm}

    if EOIN_ZIP.exists():
        with zipfile.ZipFile(EOIN_ZIP) as z:
            with z.open("PlayerStatistics.csv") as f:
                eo = pd.read_csv(f, usecols=["gameId", "gameType", "personId", "points",
                                             "plusMinusPoints", "home"], low_memory=False)
        eo = eo[eo["gameType"] == "Playoffs"].copy()
        eo["gid"] = eo["gameId"].astype(str).str.lstrip("0")
        # Eoin's game-id numbering for pre-2001 playoffs is a DIFFERENT namespace
        # than ours/Kaggle's (both use 8-digit 4YY ids, numbered differently), so
        # only trust Eoin for whole games Kaggle doesn't cover at all (current
        # season). Never let it fill single players inside a Kaggle-covered game.
        eo = eo[~eo["gid"].isin(kaggle_games)]
        eo["pid"] = pd.to_numeric(eo["personId"], errors="coerce")
        eo["pm"] = pd.to_numeric(eo["plusMinusPoints"], errors="coerce")
        eo = eo.dropna(subset=["pid", "pm"])
        added_games = 0
        team_pts = eo.groupby(["gid", "home"])["points"].sum()
        for gid in eo["gid"].unique():
            if gid in totals:
                continue
            try:
                totals[gid] = (float(team_pts[(gid, 1)]), float(team_pts[(gid, 0)]))
                added_games += 1
            except KeyError:
                continue
            for r in eo[eo["gid"] == gid].itertuples(index=False):
                pm.setdefault((gid, int(r.pid)), float(r.pm))
        print(f"  eoin: +{added_games} games")

    return pm, totals


def calibrate_game(g: pd.DataFrame, pm: dict, totals: tuple[float, float]) -> tuple[np.ndarray, np.ndarray, dict] | None:
    idx = g.index.to_numpy()
    n = len(idx)
    h = g["home_pts"].to_numpy(dtype=float)
    a = g["away_pts"].to_numpy(dtype=float)
    gid = str(g["game_id"].iloc[0])

    if abs(h.sum() - totals[0]) > TOTAL_TOL or abs(a.sum() - totals[1]) > TOTAL_TOL:
        return None  # coverage hole (missing OT etc.) -- leave alone

    # on-court masks per player
    on: dict[tuple[int, bool], np.ndarray] = {}
    for side, cols, is_home in [("h", HOME_COLS, True), ("a", AWAY_COLS, False)]:
        vals = g[cols].to_numpy()
        players = pd.unique(vals[~pd.isna(vals)].astype(int).ravel())
        for p in players:
            on[(int(p), is_home)] = (vals == p).any(axis=1)

    rows, rhs = [], []
    dropped = 0
    for (p, is_home), mask in on.items():
        official = pm.get((gid, p))
        if official is None:
            dropped += 1
            continue
        cur = (h[mask] - a[mask]).sum() if is_home else (a[mask] - h[mask]).sum()
        row = np.zeros(2 * n)
        if is_home:
            row[:n][mask] = 1.0
            row[n:][mask] = -1.0
        else:
            row[:n][mask] = -1.0
            row[n:][mask] = 1.0
        rows.append(row)
        rhs.append(official - cur)
    # team totals
    rh = np.zeros(2 * n); rh[:n] = 1.0
    ra = np.zeros(2 * n); ra[n:] = 1.0
    rows += [rh, ra]
    rhs += [totals[0] - h.sum(), totals[1] - a.sum()]

    A = np.vstack(rows)
    b = np.asarray(rhs, dtype=float)

    # Bounded least squares: a correction may not take any stint's points below
    # zero (plain minimum-norm projection pushed common 0-0 stints negative and
    # clipping then broke the constraints). A tiny ridge term keeps the
    # correction small among the many exact solutions.
    from scipy.optimize import lsq_linear
    lam = 1e-3
    A_aug = np.vstack([A, np.sqrt(lam) * np.eye(2 * n)])
    b_aug = np.concatenate([b, np.zeros(2 * n)])
    lower = np.concatenate([-h, -a])
    upper = np.full(2 * n, np.inf)
    sol = lsq_linear(A_aug, b_aug, bounds=(lower, upper), tol=1e-12)
    x = sol.x

    resid = float(np.abs(A @ x - b).max())
    h_new = np.maximum(h + x[:n], 0.0)
    a_new = np.maximum(a + x[n:], 0.0)
    return h_new, a_new, {"resid": resid, "dropped": dropped,
                          "max_delta": float(np.abs(x).max())}


def main() -> None:
    print(f"Loading {STINTS_PATH}...")
    stints = pd.read_csv(STINTS_PATH, dtype={"game_id": str})
    for col in ("home_pts", "away_pts", "home_pts_adj", "away_pts_adj"):
        stints[col] = stints[col].astype(float)
    print(f"  {len(stints)} stints, {stints['game_id'].nunique()} games")
    print("Loading official box data...")
    pm, totals = load_official()

    stats = {"calibrated": 0, "skipped_no_official": 0, "skipped_coverage": 0,
             "dropped_players": 0, "worst_resid": 0.0, "max_delta": 0.0,
             "games_resid_gt_half": 0}
    for gid, g in stints.groupby("game_id", sort=False):
        g = g.sort_values("stint_index")
        if gid not in totals:
            stats["skipped_no_official"] += 1
            continue
        out = calibrate_game(g, pm, totals[gid])
        if out is None:
            stats["skipped_coverage"] += 1
            continue
        h_new, a_new, info = out
        idx = g.sort_values("stint_index").index
        dh = h_new - stints.loc[idx, "home_pts"].to_numpy(dtype=float)
        da = a_new - stints.loc[idx, "away_pts"].to_numpy(dtype=float)
        stints.loc[idx, "home_pts"] = h_new
        stints.loc[idx, "away_pts"] = a_new
        stints.loc[idx, "home_pts_adj"] = stints.loc[idx, "home_pts_adj"].to_numpy(dtype=float) + dh
        stints.loc[idx, "away_pts_adj"] = stints.loc[idx, "away_pts_adj"].to_numpy(dtype=float) + da
        stats["calibrated"] += 1
        stats["dropped_players"] += info["dropped"]
        stats["worst_resid"] = max(stats["worst_resid"], info["resid"])
        stats["max_delta"] = max(stats["max_delta"], info["max_delta"])
        if info["resid"] > 0.5:
            stats["games_resid_gt_half"] += 1

    print(f"  calibrated {stats['calibrated']} games "
          f"(skipped: {stats['skipped_coverage']} coverage holes, "
          f"{stats['skipped_no_official']} no official data)")
    print(f"  players without official pm (constraints dropped): {stats['dropped_players']}")
    print(f"  games with constraint residual > 0.5: {stats['games_resid_gt_half']}")
    print(f"  worst constraint residual: {stats['worst_resid']:.3f} pts, "
          f"largest single-stint adjustment: {stats['max_delta']:.2f} pts")

    stints.to_csv(STINTS_PATH, index=False)
    print(f"Wrote {STINTS_PATH}")


if __name__ == "__main__":
    main()
