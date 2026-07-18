"""Measure current-season RAPM sensitivity to the probabilistic salvage tier.

This is an isolated diagnostic fit, not the production RAPM builder.  It uses
the production model's six-year window, 550-day decay and alpha=500, then
compares canonical-only, deterministic-best and 20 imputed completions.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.linear_model import Ridge


ROOT = Path(__file__).resolve().parents[1]
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
SALVAGE = ROOT / "derived" / "contextual_causal" / "probabilistic_lineup_salvage"
REPORT = ROOT / "outputs" / "contextual_causal"
HCOLS = [f"home_p{i}" for i in range(1, 6)]
ACOLS = [f"away_p{i}" for i in range(1, 6)]
HALFLIFE = 550.0
WINDOW = 6 * 365
ALPHA = 500.0


def norm(values: pd.Series) -> pd.Series:
    return (values.astype(str).str.split(".").str[0].str.lstrip("0")
            .replace("", "0"))


def stint_design(st: pd.DataFrame, pidx: dict[int, int], end: pd.Timestamp):
    st = st.dropna(subset=HCOLS + ACOLS).copy()
    st["date"] = pd.to_datetime(st.date)
    age = (end - st.date).dt.days.to_numpy(float)
    keep = (st.seconds >= 1) & (age >= 0) & (age <= WINDOW)
    st = st.loc[keep].reset_index(drop=True); age = age[keep]
    n, P = len(st), len(pidx)
    poss = np.maximum(st.seconds.to_numpy(float) / 24.0, .1)
    weight = poss * np.exp(-np.log(2) * age / HALFLIFE)
    lookup = np.vectorize(pidx.get)
    hi = lookup(st[HCOLS].to_numpy(int)); ai = lookup(st[ACOLS].to_numpy(int))
    rows = []; cols = []; vals = []; r = np.arange(n)
    for k in range(5):
        rows.extend([2*r, 2*r, 2*r+1, 2*r+1])
        cols.extend([hi[:, k], P+ai[:, k], ai[:, k], P+hi[:, k]])
        vals.extend([np.ones(n), np.ones(n), np.ones(n), np.ones(n)])
    X = sparse.csr_matrix((np.concatenate(vals),
                           (np.concatenate(rows), np.concatenate(cols))),
                          shape=(2*n, 2*P))
    y = np.empty(2*n)
    y[0::2] = st.home_pts_adj.to_numpy(float) / poss * 100
    y[1::2] = st.away_pts_adj.to_numpy(float) / poss * 100
    return X, y, np.repeat(weight, 2)


def aggregate_design(long: pd.DataFrame, pidx: dict[int, int], end: pd.Timestamp):
    long = long.copy(); long["date"] = pd.to_datetime(long.date)
    obs = long.drop_duplicates("observation_id").set_index("observation_id")
    age = (end - obs.date).dt.days.astype(float)
    obs = obs[(age >= 0) & (age <= WINDOW)]; age = age.loc[obs.index]
    row_map = {key: i for i, key in enumerate(obs.index)}
    frame = long[long.observation_id.isin(row_map)].copy()
    rr = frame.observation_id.map(row_map).to_numpy(int)
    pp = frame.player_id.astype(int).map(pidx).to_numpy(int)
    role_offset = np.where(frame.role.eq("defense"), len(pidx), 0)
    X = sparse.csr_matrix((frame.design_value.to_numpy(float),
                           (rr, pp + role_offset)),
                          shape=(len(obs), 2*len(pidx)))
    y = obs.target_per_100.to_numpy(float)
    w = (obs.possessions_proxy.to_numpy(float)
         * np.exp(-np.log(2) * age.to_numpy(float) / HALFLIFE))
    return X, y, w


def fit(parts, P: int) -> np.ndarray:
    X = sparse.vstack([x for x, _, _ in parts], format="csr")
    y = np.concatenate([y for _, y, _ in parts])
    w = np.concatenate([w for _, _, w in parts])
    y = y - np.average(y, weights=w)
    model = Ridge(alpha=ALPHA, fit_intercept=False, solver="lsqr",
                  tol=1e-7, max_iter=5000)
    model.fit(X, y, sample_weight=w)
    beta = model.coef_
    beta[:P] -= beta[:P].mean(); beta[P:] -= beta[P:].mean()
    return beta


def main() -> None:
    canonical = pd.read_parquet(REBUILD / "canonical_stints_candidate.parquet")
    imputed = pd.read_parquet(SALVAGE / "rapm_imputed_stints_20.parquet")
    bank = pd.read_parquet(SALVAGE / "rapm_score_consistent_candidate_bank.parquet")
    aggregate = pd.read_parquet(SALVAGE / "rapm_aggregate_fallback_design.parquet")
    all_aggregate = pd.read_parquet(
        SALVAGE / "rapm_all_quarantined_aggregate_design.parquet")
    for frame in (canonical, imputed, bank, aggregate, all_aggregate):
        frame["game_id"] = norm(frame.game_id)
    canonical["date"] = pd.to_datetime(canonical.date)
    end = canonical.date.max()

    players = set(canonical[HCOLS + ACOLS].to_numpy().astype(int).ravel())
    players |= set(imputed[HCOLS + ACOLS].to_numpy().astype(int).ravel())
    players |= set(aggregate.player_id.astype(int))
    players = np.array(sorted(players), int); pidx = {p: i for i, p in enumerate(players)}
    P = len(players)
    canonical_part = stint_design(canonical, pidx, end)
    aggregate_part = aggregate_design(aggregate, pidx, end)
    all_aggregate_part = aggregate_design(all_aggregate, pidx, end)
    baseline = fit([canonical_part], P)
    all_aggregate_beta = fit([canonical_part, all_aggregate_part], P)
    age = (end - canonical.date).dt.days.to_numpy(float)
    keep = (canonical.seconds.to_numpy(float) >= 1) & (age >= 0) & (age <= WINDOW)
    exposure_weight = (np.maximum(canonical.loc[keep, "seconds"].to_numpy(float) / 24.0, .1)
                       * np.exp(-np.log(2) * age[keep] / HALFLIFE))
    exposure = np.zeros(P)
    lookup = np.vectorize(pidx.get)
    lineup_idx = lookup(canonical.loc[keep, HCOLS + ACOLS].to_numpy(int))
    for k in range(10):
        np.add.at(exposure, lineup_idx[:, k], exposure_weight)

    probabilities = pd.read_csv(SALVAGE / "rapm_candidate_probabilities.csv",
                                dtype={"game_id": str})
    probabilities["game_id"] = norm(probabilities.game_id)
    probabilities = probabilities[probabilities.rapm_candidate_probability > 0]
    best = (probabilities.sort_values("rapm_candidate_probability", ascending=False)
            .drop_duplicates("game_id")[["game_id", "candidate_id"]])
    deterministic = bank.merge(best, on=["game_id", "candidate_id"], how="inner")
    deterministic_beta = fit([canonical_part, stint_design(deterministic, pidx, end),
                              aggregate_part], P)

    betas = []
    for imp in sorted(imputed.imputation_id.unique()):
        supplement = imputed[imputed.imputation_id == imp]
        betas.append(fit([canonical_part, stint_design(supplement, pidx, end),
                          aggregate_part], P))
        print(f"fit imputation {imp}", flush=True)
    betas = np.vstack(betas)
    rapm_base = baseline[:P] - baseline[P:]
    rapm_det = deterministic_beta[:P] - deterministic_beta[P:]
    rapm_all_aggregate = all_aggregate_beta[:P] - all_aggregate_beta[P:]
    rapm_imp = betas[:, :P] - betas[:, P:]

    exposed_ids = set(imputed.loc[pd.to_datetime(imputed.date) >= end-pd.Timedelta(days=WINDOW),
                                  HCOLS+ACOLS].to_numpy().astype(int).ravel())
    exposed_ids |= set(aggregate.loc[pd.to_datetime(aggregate.date) >= end-pd.Timedelta(days=WINDOW),
                                     "player_id"].astype(int))
    exposed = np.array([p in exposed_ids for p in players])
    out = pd.DataFrame({
        "player_id": players, "exposed_to_salvage": exposed,
        "canonical_weighted_possessions": exposure,
        "canonical_rapm": rapm_base,
        "deterministic_change": rapm_det - rapm_base,
        "all_aggregate_change": rapm_all_aggregate - rapm_base,
        "mi_mean_change": rapm_imp.mean(axis=0) - rapm_base,
        "mi_minus_all_aggregate": rapm_imp.mean(axis=0) - rapm_all_aggregate,
        "mi_between_sd": rapm_imp.std(axis=0, ddof=1),
        "mi_min_change": rapm_imp.min(axis=0) - rapm_base,
        "mi_max_change": rapm_imp.max(axis=0) - rapm_base,
    })
    out.to_csv(REPORT / "probabilistic_salvage_rapm_sensitivity_players.csv", index=False)
    affected = out[out.exposed_to_salvage]
    established = affected[affected.canonical_weighted_possessions >= 1000]
    summary = {
        "target_end_date": str(end.date()), "window_days": WINDOW,
        "alpha": ALPHA, "imputations": len(betas),
        "players_in_fit": P, "salvage_exposed_players": len(affected),
        "canonical_games": int(canonical.game_id.nunique()),
        "score_consistent_salvage_games": int(imputed.game_id.nunique()),
        "aggregate_fallback_games": int(aggregate.game_id.nunique()),
        "mean_abs_mi_change_exposed": float(affected.mi_mean_change.abs().mean()),
        "p95_abs_mi_change_exposed": float(affected.mi_mean_change.abs().quantile(.95)),
        "max_abs_mi_change_exposed": float(affected.mi_mean_change.abs().max()),
        "mean_between_imputation_sd_exposed": float(affected.mi_between_sd.mean()),
        "p95_between_imputation_sd_exposed": float(affected.mi_between_sd.quantile(.95)),
        "established_exposed_players_1000_wposs": int(len(established)),
        "mean_abs_mi_change_established": float(established.mi_mean_change.abs().mean()),
        "p95_abs_mi_change_established": float(established.mi_mean_change.abs().quantile(.95)),
        "max_abs_mi_change_established": float(established.mi_mean_change.abs().max()),
        "p95_between_imputation_sd_established": float(established.mi_between_sd.quantile(.95)),
        "mean_abs_mi_minus_aggregate_established": float(
            established.mi_minus_all_aggregate.abs().mean()),
        "p95_abs_mi_minus_aggregate_established": float(
            established.mi_minus_all_aggregate.abs().quantile(.95)),
        "max_abs_mi_minus_aggregate_established": float(
            established.mi_minus_all_aggregate.abs().max()),
        "canonical_vs_mi_mean_correlation_exposed": float(np.corrcoef(
            affected.canonical_rapm,
            affected.canonical_rapm + affected.mi_mean_change)[0, 1]),
        "production_modified": False,
    }
    (REPORT / "probabilistic_salvage_rapm_sensitivity_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
