"""
Stage 1: fit RAPM separately on the leverage bucket (playoffs + RS clutch)
and the other bucket (everything else), each with its own cross-validated
ridge alpha, using the same O/D design as run_rapm.py.

Output: data/leverage_rapm/rapm_leverage.csv, data/leverage_rapm/rapm_other.csv
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.model_selection import GroupShuffleSplit

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import run_rapm as rr  # noqa: E402

DATA_DIR = ROOT / "data"
BUCKET_PATH = DATA_DIR / "leverage_rapm" / "stints_bucketed.parquet"

ALPHA_GRID = [10.0, 20.0, 50.0, 100.0, 200.0, 350.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0]


def pick_alpha(X, y, weights, n_players, game_ids):
    """
    Hold out 20% of GAMES (not stints) so no game leaks across the split --
    two stints from the same game share context (that night's shooting
    variance, refs, injuries) that would let a low-alpha model "cheat" by
    partially memorizing the game instead of learning real player skill.
    """
    splitter = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=0)
    train_idx, val_idx = next(splitter.split(np.arange(X.shape[0]), groups=game_ids))

    best_alpha, best_mse = None, np.inf
    for alpha in ALPHA_GRID:
        coef_off, coef_def, intercept = rr.run_rapm_od(
            X[train_idx], y[train_idx], weights[train_idx], n_players,
            alpha_off=alpha, alpha_def=alpha,
        )
        coef = np.concatenate([coef_off, coef_def])
        pred = X[val_idx] @ coef + intercept
        resid = y[val_idx] - pred
        mse = np.average(resid**2, weights=weights[val_idx])
        print(f"    alpha={alpha:>8.0f}  holdout weighted MSE={mse:.4f}")
        if mse < best_mse:
            best_mse, best_alpha = mse, alpha
    return best_alpha


def fit_bucket(stints: pd.DataFrame, label: str) -> pd.DataFrame:
    print(f"\n=== {label}: {len(stints)} stints ===")
    X, y, weights, player_list, player_to_idx, n_players = rr.build_design_matrix_stint_od(
        stints, use_adjusted=True
    )

    print("  Selecting alpha via game-level holdout...")
    # build_design_matrix_stint_od emits 2 rows per stint (home-offense, away-offense)
    game_ids_expanded = np.repeat(stints["game_id"].values, 2)
    alpha = pick_alpha(X, y, weights, n_players, game_ids_expanded)
    print(f"  Chosen alpha={alpha}")

    coef_off, coef_def, intercept = rr.run_rapm_od(
        X, y, weights, n_players, alpha_off=alpha, alpha_def=alpha
    )

    orapm = dict(zip(player_list, coef_off))
    drapm = dict(zip(player_list, coef_def))

    # Vectorized minutes-per-player (faster than run_rapm's iterrows version)
    long_frames = []
    for col_set, team_col in [
        (["home_p1", "home_p2", "home_p3", "home_p4", "home_p5"], "home_id"),
        (["away_p1", "away_p2", "away_p3", "away_p4", "away_p5"], "away_id"),
    ]:
        melted = stints.melt(
            id_vars=["seconds", team_col], value_vars=col_set, value_name="player_id"
        )[["player_id", team_col, "seconds"]]
        melted = melted.rename(columns={team_col: "team_id"})
        long_frames.append(melted)
    long_df = pd.concat(long_frames, ignore_index=True)
    long_df = long_df.dropna(subset=["player_id"])
    long_df["player_id"] = long_df["player_id"].astype(int)

    minutes = long_df.groupby("player_id")["seconds"].sum() / 60.0
    team_minutes = long_df.groupby(["player_id", "team_id"])["seconds"].sum() / 60.0
    primary_team = team_minutes.groupby("player_id").idxmax().apply(lambda t: t[1])

    player_info = rr.get_player_info(player_list, stints, suffix=f"_{label}")

    rows = []
    for pid in player_list:
        mins = minutes.get(pid, 0.0)
        info = player_info.get(pid, {})
        team_id = primary_team.get(pid, info.get("team_id", 0))
        o = float(orapm.get(pid, 0.0))
        d = float(drapm.get(pid, 0.0))
        rows.append({
            "player_id": int(pid),
            "player_name": info.get("name", f"Player {pid}"),
            "team_id": int(team_id) if pd.notna(team_id) else 0,
            "team_abbr": rr.TEAM_ID_TO_ABBR.get(int(team_id), "???") if pd.notna(team_id) else "???",
            "bucket_minutes": round(mins, 1),
            "rapm": o + d,
            "orapm": o,
            "drapm": d,
        })

    df = pd.DataFrame(rows).sort_values("rapm", ascending=False)
    out_path = DATA_DIR / "leverage_rapm" / f"rapm_{label}.csv"
    df.to_csv(out_path, index=False)
    print(f"  Wrote {out_path} (alpha={alpha}, players={len(df)})")
    return df


def main():
    stints = pd.read_parquet(BUCKET_PATH)
    stints = stints[stints["seconds"] >= 10].copy()

    # 'playoff' bucket's stints are unchanged from the earlier pass (rapm_playoff.csv
    # already fit on the identical population) -- only 'regular' needs a fresh fit
    # (it now includes what used to be split out as 'clutch').
    for label in ["regular"]:
        subset = stints[stints["bucket"] == label].reset_index(drop=True)
        fit_bucket(subset, label)


if __name__ == "__main__":
    main()
