"""Promote self-consistent GameRotation timelines over conflicting PBP evidence.

The rotation is authoritative only when its own durations and PT_DIFF agree
with the official player box. Stint scoring is then exactly recalibrated to
official player plus-minus and team totals; no production artifact is changed.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy.optimize import lsq_linear


ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "nba_analytics.duckdb"
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
AUTHORITY = ROOT / "outputs" / "contextual_causal" / "github_gamerotation_authority_audit.csv"
OUT_AUDIT = REBUILD / "audit_authoritative_gamerotation.csv"
OUT_STINTS = REBUILD / "stints_authoritative_gamerotation.parquet"
HCOLS = [f"home_p{i}" for i in range(1, 6)]
ACOLS = [f"away_p{i}" for i in range(1, 6)]


def exact_calibrate(game: pd.DataFrame, official: pd.DataFrame) -> dict[str, float | bool]:
    game.sort_values("start_elapsed", inplace=True)
    game.reset_index(drop=True, inplace=True)
    h = pd.to_numeric(game.home_pts, errors="coerce").to_numpy(float)
    a = pd.to_numeric(game.away_pts, errors="coerce").to_numpy(float)
    h_adj = pd.to_numeric(game.home_pts_adj, errors="coerce").to_numpy(float)
    a_adj = pd.to_numeric(game.away_pts_adj, errors="coerce").to_numpy(float)
    n = len(game)
    pm = dict(zip(official.player_id.astype(int),
                  official.plus_minus_actual.astype(float)))
    hv = game[HCOLS].to_numpy()
    av = game[ACOLS].to_numpy()
    rows: list[np.ndarray] = []
    rhs: list[float] = []
    for values, is_home in ((hv, True), (av, False)):
        for pid in pd.unique(values.ravel()):
            pid = int(pid)
            if pid not in pm:
                continue
            mask = (values == pid).any(axis=1)
            current = ((h - a)[mask].sum() if is_home else (a - h)[mask].sum())
            row = np.zeros(2 * n)
            row[:n][mask] = 1.0 if is_home else -1.0
            row[n:][mask] = -1.0 if is_home else 1.0
            rows.append(row)
            rhs.append(float(pm[pid] - current))
    home_total = np.zeros(2 * n); home_total[:n] = 1.0
    away_total = np.zeros(2 * n); away_total[n:] = 1.0
    rows.extend([home_total, away_total]); rhs.extend([0.0, 0.0])
    matrix = np.vstack(rows); target = np.asarray(rhs)
    weight = 1e6
    solution = lsq_linear(
        np.vstack([weight * matrix, np.eye(2 * n)]),
        np.concatenate([weight * target, np.zeros(2 * n)]),
        bounds=(np.concatenate([-h, -a]), np.full(2 * n, np.inf)),
        tol=1e-12, lsmr_tol=1e-12, max_iter=2000,
    )
    new_h = np.maximum(h + solution.x[:n], 0.0)
    new_a = np.maximum(a + solution.x[n:], 0.0)
    game["home_pts"] = new_h; game["away_pts"] = new_a
    game["home_pts_adj"] = h_adj + (new_h - h)
    game["away_pts_adj"] = a_adj + (new_a - a)

    pm_errors = []
    for values, is_home in ((hv, True), (av, False)):
        for pid in pd.unique(values.ravel()):
            pid = int(pid)
            if pid not in pm:
                continue
            mask = (values == pid).any(axis=1)
            reconstructed = ((new_h - new_a)[mask].sum()
                             if is_home else (new_a - new_h)[mask].sum())
            pm_errors.append(abs(float(reconstructed) - pm[pid]))

    # Keep cumulative score fields coherent for leverage and downstream joins.
    for home_col, away_col, home_start, away_start, home_end, away_end in (
        ("home_pts", "away_pts", "start_home_score", "start_away_score",
         "end_home_score", "end_away_score"),
        ("home_pts_adj", "away_pts_adj", "start_home_score_adj", "start_away_score_adj",
         "end_home_score_adj", "end_away_score_adj"),
    ):
        hc = game[home_col].cumsum(); ac = game[away_col].cumsum()
        game[home_start] = hc.shift(fill_value=0.0)
        game[away_start] = ac.shift(fill_value=0.0)
        game[home_end] = hc; game[away_end] = ac
    game["stint_index"] = np.arange(len(game))
    return {
        "solver_success": bool(solution.success),
        "max_pm_error": max(pm_errors, default=np.inf),
        "home_total_delta": abs(float(new_h.sum() - h.sum())),
        "away_total_delta": abs(float(new_a.sum() - a.sum())),
    }


def main() -> None:
    authority = pd.read_csv(AUTHORITY, dtype={"game_id": str})
    authority["game_id"] = authority.game_id.str.lstrip("0")
    target = authority.loc[(~authority.strict_accepted) & authority.intrinsic_box_match].copy()
    ids = set(target.game_id)

    parts = []
    for path in REBUILD.glob("stints_github_rotation_*.parquet"):
        frame = pd.read_parquet(path)
        frame["game_id"] = frame.game_id.astype(str).str.lstrip("0")
        frame = frame.loc[frame.game_id.isin(ids)]
        if not frame.empty:
            parts.append(frame)
    stints = pd.concat(parts, ignore_index=True)

    con = duckdb.connect(str(DB), read_only=True)
    official = con.execute("""
        SELECT ltrim(game_id, '0') game_id, player_id, minutes,
               plus_minus_actual, home_away, team_pts_actual
        FROM player_game_facts WHERE minutes > 0
    """).df()
    con.close()
    official = official.loc[official.game_id.isin(ids)]

    audits = []; outputs = []
    for gid, game in stints.groupby("game_id"):
        game = game.copy()
        box = official.loc[official.game_id == gid]
        original_home = float(pd.to_numeric(game.home_pts, errors="coerce").sum())
        original_away = float(pd.to_numeric(game.away_pts, errors="coerce").sum())
        home_total = float(box.loc[box.home_away == "home", "team_pts_actual"].max())
        away_total = float(box.loc[box.home_away == "away", "team_pts_actual"].max())
        original_score_error = abs(original_home - home_total) + abs(original_away - away_total)
        result = exact_calibrate(game, box)
        final_score_error = (abs(float(game.home_pts.sum()) - home_total)
                             + abs(float(game.away_pts.sum()) - away_total))
        accepted = bool(
            original_score_error < .5 and final_score_error < .5
            and result["max_pm_error"] < .5
            and result["home_total_delta"] < .5
            and result["away_total_delta"] < .5
        )
        game["canonical_accepted"] = accepted
        game["canonical_method"] = "authoritative_gamerotation_exact_pm"
        outputs.append(game)
        audits.append({"game_id": gid, "accepted": accepted,
                       "method": "authoritative_gamerotation_exact_pm",
                       "endpoint_intrinsic_box_match": True,
                       "original_score_error": original_score_error,
                       "final_score_error": final_score_error, **result})
    out = pd.concat(outputs, ignore_index=True)
    audit = pd.DataFrame(audits)
    out.to_parquet(OUT_STINTS, index=False)
    audit.to_csv(OUT_AUDIT, index=False)
    print(audit.accepted.value_counts().to_string())
    print(f"wrote {len(out)} stints for {len(audit)} games")


if __name__ == "__main__":
    main()
