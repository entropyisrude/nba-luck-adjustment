"""Game-block bootstrap validation of the analytic RAPM-target standard
errors (build_rapm_target.py's se_o/se_d/se_rapm columns).

Games — not stints — are the resampling unit: the calibration step couples
all stints within a game (they are jointly adjusted so player sums match
the official box score), so stint-level resampling would break that
correlation structure and understate the SEs. Resampling games with
replacement is implemented as an integer multiplicity per game applied as
a weight multiplier — the design matrix never changes, only W, so each
replicate is one reweighted ridge solve.

Compares bootstrap SDs against the EMITTED SEs by possession bucket.
NOTE: since the 2026-07-16 runs, build_rapm_target.py bakes SE_CALIBRATION
(O 0.76 / D 0.78 / total 0.77 — measured by this script against the 2024-25
and 2010-11 windows, raw analytic ran ~25% conservative because stint
calibration makes within-game residuals negatively correlated) into the
emitted columns. Re-running this script should therefore show ratios near
1.0; divide the reported ratios by SE_CALIBRATION to recover raw-analytic
comparisons.

Usage: python metric/validate_target_se.py [--season 2024] [--reps 150]
       (~5-10s per replicate; run in the background)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_rapm_target import (prepare, assemble_design, season_label,
                               DECAY_HALFLIFE_DAYS, WINDOW_DAYS, SE_ALPHA,
                               OUT_DIR)

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, default=2024)
    ap.add_argument("--reps", type=int, default=150)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()
    enc = sys.stdout.encoding or "utf-8"
    def p(s: str) -> None:
        print(s.encode(enc, errors="replace").decode(enc), flush=True)

    st = prepare()
    st, X, y, players, hidx, aidx = assemble_design(st)
    P = len(players)
    poss = st["poss"].to_numpy()
    dates = st["date"].to_numpy()

    # same window/decay weighting as build_targets for the chosen season
    sy = args.season
    sel_season = (st["season_year"].to_numpy() == sy)
    if not sel_season.any():
        raise SystemExit(f"no stints for season {sy}")
    end = dates[sel_season].max()
    age_days = (end - dates).astype("timedelta64[D]").astype(float)
    w_st = poss * np.exp(-np.log(2) * age_days / DECAY_HALFLIFE_DAYS)
    w_st[(age_days < 0) | (age_days > WINDOW_DAYS)] = 0.0
    used = w_st > 0
    row_mask = np.repeat(used, 2)

    Xs = X[row_mask].tocsr()
    ys = y[row_mask]
    ws_base = w_st[used]
    gids = st["game_id"].to_numpy()[used]
    gcodes, ginv = np.unique(gids, return_inverse=True)
    G = len(gcodes)
    p(f"season {season_label(sy)}: {int(used.sum())} stints in window, "
      f"{G} games, alpha={SE_ALPHA}, reps={args.reps}")

    rng = np.random.default_rng(args.seed)
    pen = np.ones(2 * P + 1)   # home-court column (index 2P) unpenalized
    pen[2 * P] = 0.0
    ridge = SE_ALPHA * np.diag(pen)
    betas = np.empty((args.reps, 2 * P + 1), dtype=np.float32)
    for b in range(args.reps):
        mult = np.bincount(rng.integers(0, G, G), minlength=G).astype(float)
        wr = np.repeat(ws_base * mult[ginv], 2)
        ybar = np.average(ys, weights=wr)
        Xw = Xs.multiply(np.sqrt(wr)[:, None]).tocsr()
        yw = (ys - ybar) * np.sqrt(wr)
        XtX = (Xw.T @ Xw).toarray()
        Xty = Xw.T @ yw
        betas[b] = np.linalg.solve(XtX + ridge, Xty)
        if (b + 1) % 10 == 0:
            p(f"  replicate {b + 1}/{args.reps}")

    boot = pd.DataFrame({
        "player_id": players,
        "boot_se_o": betas[:, :P].std(axis=0, ddof=1),
        "boot_se_d": betas[:, P:2 * P].std(axis=0, ddof=1),
        "boot_se_rapm": (betas[:, :P] - betas[:, P:2 * P]).std(axis=0, ddof=1),
    })

    tgt = pd.read_parquet(OUT_DIR / f"rapm_target_hl{DECAY_HALFLIFE_DAYS}.parquet")
    tgt = tgt[(tgt["alpha"] == SE_ALPHA)
              & (tgt["target_season"] == season_label(sy))]
    cmp_ = tgt.merge(boot, on="player_id")
    cmp_ = cmp_[cmp_["se_o"].notna() & (cmp_["boot_se_o"] > 0)].copy()
    for c in ("o", "d", "rapm"):
        cmp_[f"ratio_{c}"] = cmp_[f"boot_se_{c}"] / cmp_[f"se_{c}"]

    p(f"\n{len(cmp_)} players compared (analytic vs {args.reps}-rep "
      f"game-block bootstrap)")
    p("\nmedian bootstrap/analytic ratio by weighted-poss bucket:")
    buckets = [(0, 2000), (2000, 8000), (8000, 20000), (20000, np.inf)]
    p(f"{'w_poss':>16} {'n':>5} {'O':>6} {'D':>6} {'total':>6}")
    for lo, hi in buckets:
        m = cmp_[(cmp_["w_poss"] >= lo) & (cmp_["w_poss"] < hi)]
        if not len(m):
            continue
        lab = f"{lo:.0f}-{'inf' if np.isinf(hi) else f'{hi:.0f}'}"
        p(f"{lab:>16} {len(m):>5} {m['ratio_o'].median():>6.3f} "
          f"{m['ratio_d'].median():>6.3f} {m['ratio_rapm'].median():>6.3f}")
    p(f"{'ALL':>16} {len(cmp_):>5} {cmp_['ratio_o'].median():>6.3f} "
      f"{cmp_['ratio_d'].median():>6.3f} {cmp_['ratio_rapm'].median():>6.3f}")

    hi = cmp_[cmp_["w_poss"] >= 8000]
    p(f"\ncorr(analytic, bootstrap) among w_poss>=8000 (n={len(hi)}): "
      f"O {np.corrcoef(hi['se_o'], hi['boot_se_o'])[0, 1]:.3f}  "
      f"D {np.corrcoef(hi['se_d'], hi['boot_se_d'])[0, 1]:.3f}")

    p("\nspot checks (highest w_poss):")
    top = cmp_.nlargest(8, "w_poss")
    p(top[["player_name", "w_poss", "rapm", "se_rapm", "boot_se_rapm",
           "ratio_rapm"]].round(3).to_string(index=False))

    out = METRIC_DATA / "targets" / f"se_validation_{sy}.parquet"
    cmp_.to_parquet(out, index=False)
    p(f"\nwrote {out}")


if __name__ == "__main__":
    main()
