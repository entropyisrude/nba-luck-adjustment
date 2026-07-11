"""Phase 4b: game-level filtering of box skills with per-skill learning rates.

Every box feature becomes its own exponentially-forgetting filter over a
player's games, with decay measured in POSSESSIONS OF EXPOSURE (not calendar
time) and the half-life chosen per feature by one-game-ahead prediction error
— i.e., each skill gets the learning rate the data says it deserves (shooting
percentages want long memories; usage/minutes move fast).

The filtered feature vector at end of season replaces the season-average
feature vector in the Phase 2 prior regression (identical protocol,
fit_predict imported), which answers the honest question: do recency-weighted
skills predict impact better than season averages?  Both variants are also
scored on next-season single-season RAPM evidence (the Phase 3/4 scoreboard).

Outputs (nba-metric-data/game_kalman/):
  feature_halflives.csv          chosen half-life + skill score per feature
  filtered_eos_features.parquet  end-of-season filtered feature panel
  filtered_prior.parquet         Phase-2-protocol prior from filtered features

Usage: python metric/build_game_kalman.py [--cache-only]
"""
from __future__ import annotations

import argparse
import os
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_box_prior import (COUNT_COLS, PCT_COLS, FEATURES, ERAS, era_of,
                             fit_predict, wcorr, build_features,
                             EOIN_ZIP, TARGET_PATH, TARGET_ALPHA, MIN_FIT_POSS)

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
GAME_CACHE = METRIC_DATA / "features_box_game.parquet"
EVID_PATH = METRIC_DATA / "evidence_season.parquet"
OUT_DIR = METRIC_DATA / "game_kalman"

# features filtered at game level; mpg comes from the filtered mins feature,
# age/height are deterministic and joined at evaluation time
PER75 = ["pts", "ast", "oreb", "dreb", "stl", "blk", "tov", "pf",
         "fouls_drawn", "blocked", "fta", "fg3a"]
GAME_FEATURES = ([c + "_75" for c in PER75]
                 + ["fg3_rate", "ft_pct"] + list(PCT_COLS.values()) + ["mins"])

HL_GRID = np.array([750.0, 1500.0, 3000.0, 6000.0, 12000.0, 24000.0, 1e8])
MIN_SCORE_EXPOSURE = 1000.0   # possessions of prior exposure before a
                              # one-game-ahead prediction is scored


def build_game_cache() -> pd.DataFrame:
    if GAME_CACHE.exists():
        print(f"Using cached {GAME_CACHE}")
        return pd.read_parquet(GAME_CACHE)
    usecols = (["personId", "gameDateTimeEst", "gameType", "numMinutes",
                "possessions", "fieldGoalsAttempted", "freeThrowsMade"]
               + list(COUNT_COLS) + list(PCT_COLS))
    usecols = sorted(set(usecols))
    print("Loading extended box per-game (one-time, cached afterwards)...")
    with zipfile.ZipFile(EOIN_ZIP) as z:
        with z.open("PlayerStatisticsExtended.csv") as f:
            eo = pd.read_csv(f, usecols=usecols, low_memory=False)
    eo = eo[eo["gameType"] == "Regular Season"].copy()
    d = pd.to_datetime(eo["gameDateTimeEst"], errors="coerce")
    eo["date"] = d
    eo["season_year"] = (d.dt.year - (d.dt.month < 10)).astype(int)
    eo = eo[eo["season_year"] >= 1996]
    eo["pid"] = pd.to_numeric(eo["personId"], errors="coerce")
    eo = eo.dropna(subset=["pid"])
    eo["pid"] = eo["pid"].astype(int)
    eo["mins"] = pd.to_numeric(eo["numMinutes"], errors="coerce").fillna(0.0)
    eo["poss"] = pd.to_numeric(eo["possessions"], errors="coerce")
    eo["poss"] = eo["poss"].fillna(eo["mins"] * 2.08)
    eo = eo[(eo["mins"] > 0) & (eo["poss"] > 0)]

    g = pd.DataFrame({"pid": eo["pid"], "season_year": eo["season_year"],
                      "date": eo["date"], "mins": eo["mins"], "poss": eo["poss"]})
    for src, name in COUNT_COLS.items():
        eo[src] = pd.to_numeric(eo[src], errors="coerce")
    for c in PER75:
        src = {v: k for k, v in COUNT_COLS.items()}[c]
        g[c + "_75"] = eo[src] / eo["poss"] * 75.0
    fga = pd.to_numeric(eo["fieldGoalsAttempted"], errors="coerce")
    fg3a = pd.to_numeric(eo["threePointersAttempted"], errors="coerce")
    fta = pd.to_numeric(eo["freeThrowsAttempted"], errors="coerce")
    ftm = pd.to_numeric(eo["freeThrowsMade"], errors="coerce")
    g["fg3_rate"] = np.where(fga > 0, fg3a / fga, np.nan)
    g["ft_pct"] = np.where(fta > 0, ftm / fta, np.nan)
    for src, name in PCT_COLS.items():
        g[name] = pd.to_numeric(eo[src], errors="coerce")
    g = g.sort_values(["pid", "date"]).reset_index(drop=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    g.to_parquet(GAME_CACHE, index=False)
    print(f"Cached {len(g)} player-game feature rows to {GAME_CACHE}")
    return g


def select_halflives(g: pd.DataFrame) -> pd.DataFrame:
    """One streaming pass; per feature x half-life, accumulate poss-weighted
    one-game-ahead squared error (and the career-mean baseline)."""
    F = len(GAME_FEATURES)
    H = len(HL_GRID)
    X = g[GAME_FEATURES].to_numpy(dtype=float)          # (n, F)
    poss = g["poss"].to_numpy(dtype=float)
    pids = g["pid"].to_numpy()
    n = len(g)

    num = np.zeros((F, H)); den = np.zeros((F, H))
    base_num = np.zeros(F); base_den = np.zeros(F)
    sse = np.zeros((F, H)); sse_base = np.zeros(F); wsum = np.zeros(F)

    cur = None
    for i in range(n):
        if pids[i] != cur:
            cur = pids[i]
            num[:] = 0; den[:] = 0
            base_num[:] = 0; base_den[:] = 0
        x = X[i]
        valid = ~np.isnan(x)
        w = poss[i]
        score = valid & (base_den > MIN_SCORE_EXPOSURE)
        if score.any():
            pred = num[score] / np.maximum(den[score], 1e-9)
            err = x[score, None] - pred
            sse[score] += w * err ** 2
            bpred = base_num[score] / base_den[score]
            sse_base[score] += w * (x[score] - bpred) ** 2
            wsum[score] += w
        decay = 0.5 ** (w / HL_GRID)                     # (H,)
        num *= decay; den *= decay
        num[valid] += w * x[valid, None]
        den[valid] += w
        base_num[valid] += w * x[valid]
        base_den[valid] += w

    rows = []
    for f in range(F):
        mse = sse[f] / wsum[f]
        j = int(np.argmin(mse))
        rows.append({"feature": GAME_FEATURES[f], "halflife_poss": HL_GRID[j],
                     "mse": mse[j], "mse_career_mean": sse_base[f] / wsum[f],
                     "skill_gain": 1 - mse[j] / (sse_base[f] / wsum[f])})
    hl = pd.DataFrame(rows)
    return hl


def filter_states(g: pd.DataFrame, hl: pd.DataFrame,
                  full: bool = False) -> pd.DataFrame:
    """Second pass at the chosen half-lives; returns end-of-season states,
    or (full=True) the complete per-game state trajectories with dates."""
    F = len(GAME_FEATURES)
    hls = hl.set_index("feature").loc[GAME_FEATURES, "halflife_poss"].to_numpy()
    X = g[GAME_FEATURES].to_numpy(dtype=float)
    poss = g["poss"].to_numpy(dtype=float)
    pids = g["pid"].to_numpy()
    sy = g["season_year"].to_numpy()
    n = len(g)

    num = np.zeros(F); den = np.zeros(F)
    states = np.full((n, F), np.nan)
    cur = None
    for i in range(n):
        if pids[i] != cur:
            cur = pids[i]
            num[:] = 0; den[:] = 0
        x = X[i]
        valid = ~np.isnan(x)
        w = poss[i]
        decay = 0.5 ** (w / hls)
        num *= decay; den *= decay
        num[valid] += w * x[valid]
        den[valid] += w
        states[i] = num / np.maximum(den, 1e-9)

    out = pd.DataFrame(states.astype(np.float32),
                       columns=[c + "_filt" for c in GAME_FEATURES])
    out["pid"] = pids
    out["season_year"] = sy
    if full:
        out["date"] = g["date"].to_numpy()
        out["poss"] = poss.astype(np.float32)
        return out.reset_index(drop=True)
    # keep the LAST game of each player-season
    out = out.groupby(["pid", "season_year"], as_index=False).tail(1)
    return out.reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cache-only", action="store_true")
    ap.add_argument("--trajectories", action="store_true",
                    help="emit full per-game state trajectories using the "
                         "already-selected half-lives, then exit")
    args = ap.parse_args()

    g = build_game_cache()
    if args.cache_only:
        return
    if args.trajectories:
        hl = pd.read_csv(OUT_DIR / "feature_halflives.csv")
        traj = filter_states(g, hl, full=True)
        traj.to_parquet(OUT_DIR / "state_trajectories.parquet", index=False)
        print(f"wrote {OUT_DIR / 'state_trajectories.parquet'} "
              f"({len(traj)} player-game states)")
        return

    print("Selecting per-feature half-lives (one-game-ahead)...")
    hl = select_halflives(g)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    hl.to_csv(OUT_DIR / "feature_halflives.csv", index=False)
    print(hl.sort_values("halflife_poss").round(4).to_string(index=False))

    print("\nFiltering states at chosen half-lives...")
    eos = filter_states(g, hl)
    eos.to_parquet(OUT_DIR / "filtered_eos_features.parquet", index=False)

    # ---- head-to-head: filtered vs season-average features -----------------
    feats = build_features()          # season aggregates (Phase 2 cache)
    tgt = pd.read_parquet(TARGET_PATH)
    tgt = tgt[tgt["alpha"] == TARGET_ALPHA].copy()
    tgt["season_year"] = tgt["target_season"].str[:4].astype(int)
    df = feats.merge(tgt.rename(columns={"player_id": "pid"}),
                     on=["pid", "season_year"], how="inner")
    df["era"] = df["season_year"].map(era_of)
    df["fit_w"] = df["poss_season"].clip(lower=0)
    df["fit_ok"] = df["poss_season"] >= MIN_FIT_POSS

    dff = df.merge(eos, on=["pid", "season_year"], how="inner")
    # swap in filtered features (mpg <- filtered minutes; age/height stay)
    for c in GAME_FEATURES:
        tgt_col = "mpg" if c == "mins" else c
        dff[tgt_col] = dff[c + "_filt"]
    print(f"\nJoined rows: baseline {len(df)}, filtered {len(dff)}")

    base_fit, _ = fit_predict(df[df["pid"].isin(dff["pid"])].copy())
    filt_fit, _ = fit_predict(dff)

    for label, d in [("season-average", base_fit), ("game-filtered", filt_fit)]:
        v = d[d["fit_ok"] & d["loso"].notna()]
        print(f"  {label:>15}: LOSO total {wcorr(v['loso'], v['rapm'], v['fit_w']):.4f} "
              f"O {wcorr(v['loso_o'], v['orapm'], v['fit_w']):.4f} "
              f"D {wcorr(v['loso_d'], v['drapm'], v['fit_w']):.4f} (n={len(v)})")

    # ---- next-season-evidence scoreboard ------------------------------------
    ev = pd.read_parquet(EVID_PATH)
    ev["prev_year"] = ev["season_year"] - 1
    for label, d in [("season-average", base_fit), ("game-filtered", filt_fit)]:
        j = d.merge(ev.rename(columns={"player_id": "pid"}),
                    left_on=["pid", "season_year"], right_on=["pid", "prev_year"],
                    suffixes=("", "_ev"))
        j = j[(j["ev_poss"] >= 1000) & j["loso"].notna()]
        s = wcorr(j["loso"], j["ev_o"] + j["ev_d"], j["ev_poss"])
        print(f"  {label:>15}: next-season evidence wcorr {s:.4f} (n={len(j)})")

    filt_fit[["pid", "season_year", "prior_o", "prior_d", "prior",
              "loso_o", "loso_d", "loso"]].to_parquet(
        OUT_DIR / "filtered_prior.parquet", index=False)
    print(f"\nWrote {OUT_DIR}")


if __name__ == "__main__":
    main()
