"""Shared sparse design for the canonical counted production evidence."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = (ROOT / "derived" / "contextual_causal"
            / "production_counted_evidence")
COUNTED = EVIDENCE / "canonical_counted_stints_production.parquet"
AGGREGATE = EVIDENCE / "canonical_counted_aggregate_production.parquet"
HCOLS = [f"home_p{i}" for i in range(1, 6)]
ACOLS = [f"away_p{i}" for i in range(1, 6)]


def season_year(date: pd.Series) -> pd.Series:
    date = pd.to_datetime(date)
    return date.dt.year - (date.dt.month < 10)


def load_design():
    counted = pd.read_parquet(COUNTED)
    aggregate = pd.read_parquet(AGGREGATE)
    counted["date"] = pd.to_datetime(counted.date)
    aggregate["date"] = pd.to_datetime(aggregate.date)
    counted["season_year"] = season_year(counted.date)
    aggregate["season_year"] = season_year(aggregate.date)

    players = set(counted[HCOLS + ACOLS].to_numpy(int).ravel())
    players |= set(aggregate.player_id.astype(int))
    players = np.array(sorted(players), dtype=int)
    pidx = {p: i for i, p in enumerate(players)}
    P = len(players)
    lookup = np.vectorize(pidx.get)

    # Two directed observations per stint: home offense and away offense.
    n = len(counted)
    hi = lookup(counted[HCOLS].to_numpy(int))
    ai = lookup(counted[ACOLS].to_numpy(int))
    rows, cols, vals = [], [], []
    r = np.arange(n)
    for k in range(5):
        rows.extend([2*r, 2*r, 2*r+1, 2*r+1])
        cols.extend([hi[:, k], P+ai[:, k], ai[:, k], P+hi[:, k]])
        vals.extend([np.ones(n), np.ones(n), np.ones(n), np.ones(n)])
    X_stint = sparse.csr_matrix(
        (np.concatenate(vals), (np.concatenate(rows), np.concatenate(cols))),
        shape=(2*n, 2*P))
    poss_stint_rows = np.empty(2*n)
    poss_stint_rows[0::2] = counted.n_home.to_numpy(float)
    poss_stint_rows[1::2] = counted.n_away.to_numpy(float)
    y_stint = np.zeros(2*n)
    np.divide(counted.points_adjusted_home.to_numpy(float) * 100,
              counted.n_home.to_numpy(float), out=y_stint[0::2],
              where=counted.n_home.to_numpy(float) > 0)
    np.divide(counted.points_adjusted_away.to_numpy(float) * 100,
              counted.n_away.to_numpy(float), out=y_stint[1::2],
              where=counted.n_away.to_numpy(float) > 0)
    dates_stint = np.repeat(counted.date.to_numpy(), 2)
    seasons_stint = np.repeat(counted.season_year.to_numpy(int), 2)

    # Aggregate fallbacks retain exact game possession counts. Each unique
    # observation is one directed team offense against the opposing defense.
    obs = (aggregate.drop_duplicates("observation_id")
           [["observation_id", "date", "season_year", "target_per_100",
             "possessions_proxy"]].reset_index(drop=True))
    row_map = {key: i for i, key in enumerate(obs.observation_id)}
    ar = aggregate.observation_id.map(row_map).to_numpy(int)
    ap = aggregate.player_id.astype(int).map(pidx).to_numpy(int)
    ac = ap + np.where(aggregate.role.eq("defense"), P, 0)
    X_aggregate = sparse.csr_matrix(
        (aggregate.design_value.to_numpy(float), (ar, ac)),
        shape=(len(obs), 2*P))

    X = sparse.vstack([X_stint, X_aggregate], format="csr")
    y = np.concatenate([y_stint, obs.target_per_100.to_numpy(float)])
    poss = np.concatenate(
        [poss_stint_rows, obs.possessions_proxy.to_numpy(float)])
    dates = np.concatenate([dates_stint, obs.date.to_numpy()])
    seasons = np.concatenate(
        [seasons_stint, obs.season_year.to_numpy(int)])

    # Compact exposure structures for player-season output and Kalman noise.
    stint_exposure = ((counted.n_home.to_numpy(float)
                       + counted.n_away.to_numpy(float)) / 2.0)
    aggregate_exposure = aggregate[aggregate.role.eq("offense")].copy()
    aggregate_exposure["player_index"] = (
        aggregate_exposure.player_id.astype(int).map(pidx).astype(int))
    aggregate_exposure["exposure"] = (
        aggregate_exposure.design_value.abs()
        * aggregate_exposure.possessions_proxy)
    return {
        "counted": counted, "aggregate": aggregate, "players": players,
        "pidx": pidx, "P": P, "X": X, "y": y, "poss": poss,
        "dates": dates, "seasons": seasons, "lineup_idx": np.c_[hi, ai],
        "stint_exposure": stint_exposure,
        "aggregate_exposure": aggregate_exposure,
    }


def exposure_arrays(design, target_season: int, end: np.datetime64,
                    half_life_days: float, window_days: int):
    """Return raw target-season and decayed-window exposure by player."""
    P = design["P"]
    raw = np.zeros(P)
    weighted = np.zeros(P)
    counted = design["counted"]
    age = ((end - counted.date.to_numpy())
           .astype("timedelta64[D]").astype(float))
    decay = np.exp(-np.log(2) * age / half_life_days)
    decay[(age < 0) | (age > window_days)] = 0.0
    current = counted.season_year.to_numpy(int) == target_season
    for k in range(10):
        idx = design["lineup_idx"][:, k]
        np.add.at(raw, idx, np.where(current, design["stint_exposure"], 0.0))
        np.add.at(weighted, idx, design["stint_exposure"] * decay)

    agg = design["aggregate_exposure"]
    if not agg.empty:
        agg_age = ((end - agg.date.to_numpy())
                   .astype("timedelta64[D]").astype(float))
        agg_decay = np.exp(-np.log(2) * agg_age / half_life_days)
        agg_decay[(agg_age < 0) | (agg_age > window_days)] = 0.0
        idx = agg.player_index.to_numpy(int)
        exp = agg.exposure.to_numpy(float)
        np.add.at(raw, idx, np.where(
            agg.season_year.to_numpy(int) == target_season, exp, 0.0))
        np.add.at(weighted, idx, exp * agg_decay)
    return raw, weighted


def normal_eqs(X, y, row_weight):
    used = row_weight > 0
    Xs = X[used]
    ys = y[used]
    ws = row_weight[used]
    ybar = np.average(ys, weights=ws)
    Xw = Xs.multiply(np.sqrt(ws)[:, None]).tocsr()
    yw = (ys - ybar) * np.sqrt(ws)
    return (Xw.T @ Xw).toarray(), Xw.T @ yw, used
