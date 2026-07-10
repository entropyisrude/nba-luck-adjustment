"""Playoff translation layer: does playoff over/under-performance persist?

For every playoff stint (calibrated, luck-adjusted), compute the residual
between the actual margin and what the ten on-court players' contemporaneous
NERD values predict (a, b calibrated in-sample, global). Attribute each
stint residual to the five offense players (+) and five defense players (-),
possession-weighted, and aggregate to player-season playoff deltas.

Then the honest questions:
  1. PERSISTENCE: correlate each player's delta in his odd-numbered playoff
     runs vs his even-numbered runs (poss-weighted). If ~0, "playoff risers"
     are noise and the translation layer is (almost) full shrinkage to zero.
  2. PREDICTION: walk-forward — does the shrunk career-to-date delta predict
     the next playoff run's delta? Selects the shrinkage constant K
     (w = poss / (poss + K)).

Output: nba-metric-data/playoffs/playoff_deltas.parquet (per player-season
and career aggregates with shrunk values), playoff_adjust.parquet (the
production per-player-season Playoff NERD adjustment) + console verdict.

RESULTS (2026-07-10): player-level persistence is weak — odd/even-run wcorr
0.099, walk-forward wcorr 0.06 with calibration slope ~0.12 ("playoff
risers" are ~90% noise; max honest player-specific effect ~ +/-0.3 per
100). But TRAIT-level translation is real and era-robust: a ridge on RS
box traits (age-led: old players decline; TS/finesse fall, physicality
gains) trained on 1996-2010 predicts 2011+ playoff deltas at wcorr 0.108
(n=1688, ~4.5 se) with calibration slope TRAIT_SLOPE=0.45. Production
adjustment = TRAIT_SLOPE * trait model + PLAYER_SLOPE * shrunk career
delta, spread roughly +/-0.5 per 100.

Caveat noted: NERD's evidence window includes playoff possessions, which
dampens measured deltas slightly (conservative for finding "risers").

Usage: python metric/build_playoff_translation.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
STINTS_PATH = METRIC_DATA / "prepared_stints.parquet"
METRIC_PATH = METRIC_DATA / "metric" / "metric_v0.parquet"
OUT_DIR = METRIC_DATA / "playoffs"

MIN_SECONDS = 30.0
K_GRID = [1000.0, 2000.0, 4000.0, 8000.0]
MIN_SPLIT_POSS = 1500.0     # per half, for the persistence test

# calibration slopes measured on temporal validation (see docstring);
# rerun the validation before changing these
TRAIT_SLOPE = 0.45
PLAYER_SLOPE = 0.12
TRAITS = ["age", "mpg", "pf_75", "ts", "fg3a_75", "ft_pct", "ast_pct",
          "pct_paint", "fouls_drawn_75", "dreb_pct", "fta_75", "usg",
          "blk_75", "stl_75", "height", "oreb_pct"]
TRAIT_RIDGE = 50.0

HCOLS = [f"home_p{i}" for i in range(1, 6)]
ACOLS = [f"away_p{i}" for i in range(1, 6)]


def main() -> None:
    st = pd.read_parquet(STINTS_PATH)
    st = st[(st["is_playoff"] == 1) & (st["seconds"] >= MIN_SECONDS)].copy()
    st["date"] = pd.to_datetime(st["date"])
    st["season_year"] = st["date"].dt.year - (st["date"].dt.month < 10)
    st["poss"] = np.maximum(st["seconds"].to_numpy() / 24.0, 0.1)
    print(f"{len(st)} playoff stints, {st.season_year.min()}-{st.season_year.max()}")

    m = pd.read_parquet(METRIC_PATH).rename(columns={"player_id": "pid"})
    mo = {(int(r.pid), int(r.season_year)): r.m4000_o
          for r in m.itertuples(index=False)}
    md = {(int(r.pid), int(r.season_year)): r.m4000_d
          for r in m.itertuples(index=False)}

    sy = st["season_year"].to_numpy()
    H = st[HCOLS].to_numpy().astype(int)
    A = st[ACOLS].to_numpy().astype(int)
    lk_o = np.vectorize(lambda p, s: mo.get((p, s), -0.5))
    lk_d = np.vectorize(lambda p, s: md.get((p, s), -0.5))
    ho = sum(lk_o(H[:, k], sy) for k in range(5))
    hd = sum(lk_d(H[:, k], sy) for k in range(5))
    ao = sum(lk_o(A[:, k], sy) for k in range(5))
    ad = sum(lk_d(A[:, k], sy) for k in range(5))

    poss = st["poss"].to_numpy()
    # two rows per stint: home offense, away offense
    y = np.concatenate([st["home_pts_adj"].to_numpy() / poss * 100.0,
                        st["away_pts_adj"].to_numpy() / poss * 100.0])
    strength = np.concatenate([ho - ad, ao - hd])
    w = np.concatenate([poss, poss])
    is_home = np.concatenate([np.ones(len(st)), np.zeros(len(st))])
    OFF = np.vstack([H, A])
    DEF = np.vstack([A, H])
    seas2 = np.concatenate([sy, sy])

    X = np.column_stack([np.ones(len(y)), strength, is_home])
    beta, *_ = np.linalg.lstsq(X * np.sqrt(w)[:, None],
                               y * np.sqrt(w), rcond=None)
    resid = y - X @ beta
    print(f"calibration: a={beta[0]:.2f} b={beta[1]:.3f} hca={beta[2]:.2f}; "
          f"weighted corr(strength, y) = "
          f"{np.corrcoef(strength * np.sqrt(w), y * np.sqrt(w))[0,1]:.3f}")

    # attribute residuals: offense players +resid, defense players -resid
    rows = []
    rw = resid * w
    for k in range(5):
        rows.append(pd.DataFrame({"pid": OFF[:, k], "season_year": seas2,
                                  "rw": rw, "w": w}))
        rows.append(pd.DataFrame({"pid": DEF[:, k], "season_year": seas2,
                                  "rw": -rw, "w": w}))
    at = pd.concat(rows, ignore_index=True)
    ps = at.groupby(["pid", "season_year"]).agg(rw=("rw", "sum"),
                                                poss=("w", "sum")).reset_index()
    ps["delta"] = ps["rw"] / ps["poss"]     # per-100 net over/under vs NERD
    print(f"{len(ps)} player-playoff-seasons")

    # 1. persistence: odd vs even playoff runs per player
    ps = ps.sort_values(["pid", "season_year"]).reset_index(drop=True)
    ps["run_idx"] = ps.groupby("pid").cumcount()
    halves = ps.groupby(["pid", ps["run_idx"] % 2]).apply(
        lambda g: pd.Series({"delta": np.average(g["delta"], weights=g["poss"]),
                             "poss": g["poss"].sum()}), include_groups=False).unstack()
    halves.columns = [f"{a}_{b}" for a, b in halves.columns]
    hv = halves.dropna()
    hv = hv[(hv["poss_0"] >= MIN_SPLIT_POSS) & (hv["poss_1"] >= MIN_SPLIT_POSS)]
    wj = np.minimum(hv["poss_0"], hv["poss_1"])
    a_, b_ = hv["delta_0"], hv["delta_1"]
    am, bm = np.average(a_, weights=wj), np.average(b_, weights=wj)
    cov = np.average((a_ - am) * (b_ - bm), weights=wj)
    pers = cov / np.sqrt(np.average((a_ - am) ** 2, weights=wj)
                         * np.average((b_ - bm) ** 2, weights=wj))
    print(f"\nPERSISTENCE (odd vs even playoff runs, both halves >= "
          f"{MIN_SPLIT_POSS:.0f} poss, n={len(hv)}): wcorr = {pers:.4f}")

    # 2. walk-forward prediction of the next run from shrunk career-to-date
    ps["cum_rw"] = ps.groupby("pid")["rw"].cumsum() - ps["rw"]
    ps["cum_poss"] = ps.groupby("pid")["poss"].cumsum() - ps["poss"]
    hist = ps[(ps["cum_poss"] >= 500) & (ps["poss"] >= 500)].copy()
    print(f"\nWalk-forward (career-to-date -> next run, n={len(hist)}):")
    best = None
    for K in K_GRID:
        pred = (hist["cum_rw"] / hist["cum_poss"]
                * hist["cum_poss"] / (hist["cum_poss"] + K))
        wv = np.minimum(hist["poss"], hist["cum_poss"])
        am_, bm_ = np.average(pred, weights=wv), np.average(hist["delta"], weights=wv)
        cov_ = np.average((pred - am_) * (hist["delta"] - bm_), weights=wv)
        r = cov_ / np.sqrt(np.average((pred - am_) ** 2, weights=wv)
                           * np.average((hist["delta"] - bm_) ** 2, weights=wv))
        slope = cov_ / np.average((pred - am_) ** 2, weights=wv)
        print(f"  K={K:<7.0f}: wcorr {r:.4f}  slope {slope:.2f}")
        if best is None or r > best[0]:
            best = (r, K)
    r, K = best
    print(f"Best K={K:.0f} -> predictive wcorr {r:.4f}")

    # career aggregates with the selected shrinkage
    car = ps.groupby("pid").agg(rw=("rw", "sum"), poss=("poss", "sum"),
                                runs=("season_year", "count")).reset_index()
    car["delta"] = car["rw"] / car["poss"]
    car["delta_shrunk"] = car["delta"] * car["poss"] / (car["poss"] + K)
    names = m.groupby("pid")["player_name"].last()
    car["name"] = car["pid"].map(names)

    # 3. trait model + production adjustment ------------------------------
    # Weighted ridge of RS box traits onto observed playoff deltas (whole
    # sample, standardized); combined with the shrunk career-to-date player
    # delta by their validated calibration slopes. This is the per-player-
    # season Playoff NERD adjustment NERD applies to project RS->playoffs.
    feats = pd.read_parquet(METRIC_DATA / "features_box_season.parquet",
                            columns=["pid", "season_year"] + TRAITS)
    fit = ps.merge(feats, on=["pid", "season_year"]).dropna(subset=TRAITS)
    Xf = fit[TRAITS].to_numpy(float)
    wf = fit["poss"].to_numpy()
    mu = np.average(Xf, axis=0, weights=wf)
    sd = np.sqrt(np.average((Xf - mu) ** 2, axis=0, weights=wf)) + 1e-9
    Xz = (Xf - mu) / sd
    tb = np.linalg.solve((Xz * wf[:, None]).T @ Xz
                         + TRAIT_RIDGE * np.eye(len(TRAITS)),
                         (Xz * wf[:, None]).T @ fit["delta"].to_numpy())
    print(f"\nTrait model coefs (standardized, whole-sample):")
    for t, c in sorted(zip(TRAITS, tb), key=lambda x: -abs(x[1]))[:8]:
        print(f"  {t:<16} {c:+.3f}")

    # apply to every player-season that has traits + a career-to-date delta
    allf = feats.dropna(subset=TRAITS).copy()
    allf["trait_adj"] = ((allf[TRAITS].to_numpy(float) - mu) / sd) @ tb
    ps_ctd = ps[["pid", "season_year", "cum_rw", "cum_poss"]].copy()
    ps_ctd["player_adj"] = np.where(
        ps_ctd["cum_poss"] > 0,
        ps_ctd["cum_rw"] / ps_ctd["cum_poss"].clip(lower=1)
        * ps_ctd["cum_poss"] / (ps_ctd["cum_poss"] + K), 0.0)
    adj = allf[["pid", "season_year", "trait_adj"]].merge(
        ps_ctd[["pid", "season_year", "player_adj"]],
        on=["pid", "season_year"], how="left")
    adj["player_adj"] = adj["player_adj"].fillna(0.0)
    adj["po_adjust"] = (TRAIT_SLOPE * adj["trait_adj"]
                        + PLAYER_SLOPE * adj["player_adj"])
    adj["name"] = adj["pid"].map(names)
    print(f"\nPlayoff adjustment: {len(adj)} player-seasons, "
          f"5th/95th pct {np.percentile(adj['po_adjust'], [5, 95]).round(2)}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ps.drop(columns=["run_idx"]).to_parquet(OUT_DIR / "playoff_deltas.parquet",
                                            index=False)
    car.to_parquet(OUT_DIR / "playoff_deltas_career.parquet", index=False)
    adj.to_parquet(OUT_DIR / "playoff_adjust.parquet", index=False)

    big = car[car["poss"] >= 5000]
    print(f"\nTop 10 playoff RISERS (career, >=5000 PO poss, shrunk K={K:.0f}):")
    print(big.nlargest(10, "delta_shrunk")[["name", "runs", "poss", "delta",
                                            "delta_shrunk"]]
          .round(2).to_string(index=False))
    print("\nTop 10 playoff FALLERS:")
    print(big.nsmallest(10, "delta_shrunk")[["name", "runs", "poss", "delta",
                                             "delta_shrunk"]]
          .round(2).to_string(index=False))


if __name__ == "__main__":
    main()
