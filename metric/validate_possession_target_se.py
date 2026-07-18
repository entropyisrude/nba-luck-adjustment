"""Game-block bootstrap calibration for the counted-possession target SEs."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_possession_evidence import load_counted_design
from build_rapm_target import (DECAY_HALFLIFE_DAYS, WINDOW_DAYS, SE_ALPHA,
                               season_label)

DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
TARGET = DATA / "targets" / "rapm_target_poss_hl550.parquet"
OUT = Path(__file__).resolve().parents[1] / "outputs" / "contextual_causal"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=2024)
    ap.add_argument("--reps", type=int, default=40)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()

    st, X, y, n_rows, players, hidx, aidx = load_counted_design()
    P = len(players); sy = a.season
    dates = st.date.to_numpy(); sya = st.season_year.to_numpy()
    end = dates[sya == sy].max()
    age = (end - dates).astype("timedelta64[D]").astype(float)
    decay = np.exp(-np.log(2) * age / DECAY_HALFLIFE_DAYS)
    decay[(age < 0) | (age > WINDOW_DAYS)] = 0
    w = n_rows * np.repeat(decay, 2)
    use = w > 0
    used_st = use.reshape(-1, 2)
    active = np.unique(np.concatenate([
        hidx[used_st[:, 0]].ravel(), aidx[used_st[:, 0]].ravel(),
        hidx[used_st[:, 1]].ravel(), aidx[used_st[:, 1]].ravel()]))
    cols = np.r_[active, P + active, 2 * P]
    Xs = X[use][:, cols].tocsr(); ys = y[use]; ws = w[use]
    gids = np.repeat(st.gid_n.to_numpy(), 2)[use]
    _, ginv = np.unique(gids, return_inverse=True); G = ginv.max() + 1
    A = len(active); pen = np.ones(2 * A + 1); pen[-1] = 0
    ridge = SE_ALPHA * np.diag(pen)
    rng = np.random.default_rng(a.seed)
    betas = np.empty((a.reps, 2 * A + 1), np.float32)
    print(f"{season_label(sy)}: {use.sum()} rows, {G} games, "
          f"{A} players, {a.reps} replicates", flush=True)
    for b in range(a.reps):
        mult = np.bincount(rng.integers(0, G, G), minlength=G)
        wr = ws * mult[ginv]
        positive = wr > 0
        ybar = np.average(ys[positive], weights=wr[positive])
        Xw = Xs[positive].multiply(np.sqrt(wr[positive])[:, None]).tocsr()
        XtX = (Xw.T @ Xw).toarray()
        Xty = Xw.T @ ((ys[positive] - ybar) * np.sqrt(wr[positive]))
        betas[b] = np.linalg.solve(XtX + ridge, Xty)
        if (b + 1) % 10 == 0:
            print(f"  {b + 1}/{a.reps}", flush=True)

    boot = pd.DataFrame({"player_id": players[active],
                         "boot_se_o": betas[:, :A].std(0, ddof=1),
                         "boot_se_d": betas[:, A:2*A].std(0, ddof=1),
                         "boot_se_rapm": (betas[:, :A] - betas[:, A:2*A]).std(0, ddof=1)})
    tgt = pd.read_parquet(TARGET)
    tgt = tgt[(tgt.alpha == SE_ALPHA) &
              (tgt.target_season == season_label(sy))]
    cmp = tgt.merge(boot, on="player_id")
    for c in ("o", "d", "rapm"):
        cmp[f"ratio_{c}"] = cmp[f"boot_se_{c}"] / cmp[f"se_{c}"]
    print("\nbootstrap / analytic SE median")
    for lo, hi in ((0, 1700), (1700, 6000), (6000, 12000), (12000, np.inf)):
        z = cmp[(cmp.w_poss >= lo) & (cmp.w_poss < hi)]
        print(f"{lo:5.0f}-{hi:5.0f}: n={len(z):3} "
              f"O={z.ratio_o.median():.3f} D={z.ratio_d.median():.3f} "
              f"T={z.ratio_rapm.median():.3f}")
    print(f"ALL: O={cmp.ratio_o.median():.3f} "
          f"D={cmp.ratio_d.median():.3f} T={cmp.ratio_rapm.median():.3f}")
    OUT.mkdir(parents=True, exist_ok=True)
    cmp.to_parquet(OUT / f"possession_se_validation_{sy}.parquet", index=False)


if __name__ == "__main__":
    main()
