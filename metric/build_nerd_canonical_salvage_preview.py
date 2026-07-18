"""Current-season NERD preview using canonical + probabilistic salvage stints.

This is a versioned comparison build.  It does not overwrite production NERD.
Both the chronologically fitted v1 and atomic box centers are applied to the
same repaired six-year stint likelihood with alpha=4000 and 20 imputations.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.linear_model import Ridge

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from test_probabilistic_salvage_rapm_sensitivity import (
    ACOLS, HCOLS, aggregate_design, norm, stint_design)


ROOT = Path(__file__).resolve().parents[1]
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
SALVAGE = ROOT / "derived" / "contextual_causal" / "probabilistic_lineup_salvage"
OUT = ROOT / "outputs" / "contextual_causal"
V1 = OUT / "rolling_prior_v1_poss.parquet"
# Denominator-aware atomic is the default atomic research prior.  Keep the
# unsmoothed model available only under an explicit raw label.
ATOMIC = OUT / "rolling_prior_atomic_denominator_poss.parquet"
ATOMIC_RAW = OUT / "rolling_prior_atomic_poss.parquet"
CURRENT = OUT / "nerd_possession_candidate.parquet"
ALPHA = 4000.0
TARGET_YEAR = 2025


def fit_centered(parts, P: int, beta0: np.ndarray) -> np.ndarray:
    X = sparse.vstack([x for x, _, _ in parts], format="csr")
    y = np.concatenate([y for _, y, _ in parts])
    w = np.concatenate([w for _, _, w in parts])
    y = y - np.average(y, weights=w)
    residual_target = y - X @ beta0
    model = Ridge(alpha=ALPHA, fit_intercept=False, solver="lsqr",
                  tol=1e-7, max_iter=5000)
    model.fit(X, residual_target, sample_weight=w)
    beta = beta0 + model.coef_
    beta[:P] -= beta[:P].mean(); beta[P:] -= beta[P:].mean()
    return beta


def prior_vector(path: Path, pidx: dict[int, int]) -> tuple[np.ndarray, pd.DataFrame]:
    prior = pd.read_parquet(path)
    prior = prior[prior.season_year == TARGET_YEAR].copy()
    P = len(pidx); b = np.zeros(2*P)
    for row in prior.itertuples():
        idx = pidx.get(int(row.pid))
        if idx is None:
            continue
        b[idx] = float(row.po)
        b[P+idx] = -float(row.pd)
    b[:P] -= b[:P].mean(); b[P:] -= b[P:].mean()
    return b, prior


def season_year(date: pd.Series) -> pd.Series:
    date = pd.to_datetime(date)
    return date.dt.year - (date.dt.month < 10)


def main() -> None:
    canonical = pd.read_parquet(REBUILD / "canonical_stints_candidate.parquet")
    imputed = pd.read_parquet(SALVAGE / "rapm_imputed_stints_20.parquet")
    aggregate = pd.read_parquet(SALVAGE / "rapm_aggregate_fallback_design.parquet")
    for frame in (canonical, imputed, aggregate):
        frame["game_id"] = norm(frame.game_id)
    canonical["date"] = pd.to_datetime(canonical.date)
    imputed["date"] = pd.to_datetime(imputed.date)
    aggregate["date"] = pd.to_datetime(aggregate.date)
    end = canonical.loc[season_year(canonical.date) == TARGET_YEAR, "date"].max()

    players = set(canonical[HCOLS+ACOLS].to_numpy(int).ravel())
    players |= set(imputed[HCOLS+ACOLS].to_numpy(int).ravel())
    players |= set(aggregate.player_id.astype(int))
    players = np.array(sorted(players), int); pidx = {p: i for i, p in enumerate(players)}
    P = len(players)
    canonical_part = stint_design(canonical, pidx, end)
    aggregate_part = aggregate_design(aggregate, pidx, end)
    priors = {"v1": prior_vector(V1, pidx),
              "atomic": prior_vector(ATOMIC, pidx),
              "atomic_raw": prior_vector(ATOMIC_RAW, pidx)}

    fits = {name: [] for name in priors}
    for imp in sorted(imputed.imputation_id.unique()):
        supplement = imputed[imputed.imputation_id == imp]
        parts = [canonical_part, stint_design(supplement, pidx, end), aggregate_part]
        for name, (b0, _) in priors.items():
            fits[name].append(fit_centered(parts, P, b0))
        print(f"fit imputation {imp}", flush=True)

    # Current-season exposure is a presentation filter, not a fitting weight.
    current = canonical[season_year(canonical.date) == TARGET_YEAR]
    current_imp = imputed[(imputed.imputation_id == 0)
                          & (season_year(imputed.date) == TARGET_YEAR)]
    exposure = np.zeros(P)
    for frame in (current, current_imp):
        poss = frame.seconds.to_numpy(float) / 24.0
        ids = frame[HCOLS+ACOLS].to_numpy(int)
        for k in range(10):
            np.add.at(exposure, np.vectorize(pidx.get)(ids[:, k]), poss)
    for row in aggregate[season_year(aggregate.date) == TARGET_YEAR].itertuples():
        exposure[pidx[int(row.player_id)]] += abs(float(row.design_value)) * float(row.possessions_proxy)

    out = pd.DataFrame({"player_id": players, "poss_season_proxy": exposure})
    for name, arrays in fits.items():
        b = np.stack(arrays)
        O = b[:, :P]; D = -b[:, P:]
        total = O + D
        out[f"nerd_{name}_o"] = O.mean(axis=0)
        out[f"nerd_{name}_d"] = D.mean(axis=0)
        out[f"nerd_{name}"] = total.mean(axis=0)
        out[f"nerd_{name}_imputation_sd"] = total.std(axis=0, ddof=1)
        b0, _ = priors[name]
        out[f"prior_{name}_o"] = b0[:P]
        out[f"prior_{name}_d"] = -b0[P:]

    con = duckdb.connect(str(ROOT / "data" / "nba_analytics.duckdb"), read_only=True)
    names = con.execute("""
        SELECT CAST(player_id AS BIGINT) player_id,
               max_by(player_name, date) player_name,
               max_by(team_abbr, date) team_abbr
        FROM player_game_facts GROUP BY 1
    """).df(); con.close()
    out = out.merge(names, on="player_id", how="left")
    if CURRENT.exists():
        old = pd.read_parquet(CURRENT)
        old = old[old.season_year == TARGET_YEAR][
            ["player_id", "nerd_o", "nerd_d", "nerd"]].rename(columns={
                "nerd_o": "previous_candidate_o", "nerd_d": "previous_candidate_d",
                "nerd": "previous_candidate"})
        out = out.merge(old, on="player_id", how="left")
    out["atomic_minus_v1"] = out.nerd_atomic - out.nerd_v1
    out["atomic_minus_raw"] = out.nerd_atomic - out.nerd_atomic_raw
    out["canonical_salvage_minus_previous"] = out.nerd_v1 - out.previous_candidate
    out = out[out.poss_season_proxy > 0].sort_values("nerd_atomic", ascending=False)
    OUT.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT / "nerd_canonical_salvage_preview.parquet", index=False)
    out.to_csv(OUT / "nerd_canonical_salvage_preview.csv", index=False)
    print(out[["player_name", "team_abbr", "poss_season_proxy", "nerd_v1",
               "nerd_atomic", "atomic_minus_v1"]].head(25).to_string(index=False))


if __name__ == "__main__":
    main()
