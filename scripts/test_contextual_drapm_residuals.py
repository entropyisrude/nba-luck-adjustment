"""Chronological test of structured context missing from additive RAPM.

The unit is an offense-team game. Actual luck-adjusted points per 100 are
compared with an expectation constructed from one-step-ahead (preseason) Kalman
O/D RAPM states, averaged over the lineups actually used. Player traits are from
the immediately preceding season. A small set of offense-by-defense matchup
interactions must beat both frozen RAPM alone and RAPM plus trait main effects.

Claim tier: predictive residual structure, not causal identification.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
sys.path.insert(0, str(ROOT / "metric"))
from build_rapm_target import prepare  # noqa: E402

OUT_PANEL = ROOT / "derived" / "contextual_causal" / "contextual_drapm_team_games.parquet"
OUT_REPORT = ROOT / "outputs" / "contextual_causal" / "contextual_drapm_residual_test.json"

PCOLS = [f"home_p{i}" for i in range(1, 6)] + [f"away_p{i}" for i in range(1, 6)]
HCOLS, ACOLS = PCOLS[:5], PCOLS[5:]
TARGET_SEASONS = list(range(2021, 2026))
MIN_SECONDS = 30.0
MIN_FROZEN_SLOTS = 8
ALPHAS = [0.0, 1.0, 10.0, 100.0, 1000.0, 10000.0]

O_TRAITS = ["o_rim", "o_spacing", "o_oreb", "o_turnover", "o_foul_pressure"]
D_TRAITS = ["d_rim_anchor", "d_size", "d_dreb", "d_disruption", "d_foul_rate"]
INTERACTIONS = {
    "rim_pressure_x_rim_anchor": ("o_rim", "d_rim_anchor"),
    "spacing_x_defensive_size": ("o_spacing", "d_size"),
    "offensive_rebounding_x_defensive_rebounding": ("o_oreb", "d_dreb"),
    "turnover_proneness_x_disruption": ("o_turnover", "d_disruption"),
    "foul_pressure_x_defensive_foul_rate": ("o_foul_pressure", "d_foul_rate"),
}
RATING_SHAPES = ["offense_star_concentration", "defense_weak_link", "defense_anchor", "defense_rating_spread"]
RATING_INTERACTIONS = {
    "star_concentration_x_weak_link": ("offense_star_concentration", "defense_weak_link"),
    "star_concentration_x_anchor": ("offense_star_concentration", "defense_anchor"),
    "offense_concentration_x_defense_spread": ("offense_star_concentration", "defense_rating_spread"),
}


def weighted_z(g: pd.DataFrame, col: str) -> pd.Series:
    x = g[col].astype(float)
    w = g.mins.clip(lower=1).astype(float)
    ok = x.notna()
    if not ok.any():
        return pd.Series(0.0, index=g.index)
    mu = np.average(x[ok], weights=w[ok])
    sd = np.sqrt(np.average((x[ok] - mu) ** 2, weights=w[ok]))
    return ((x - mu) / max(sd, 1e-9)).fillna(0.0)


def trait_table() -> pd.DataFrame:
    cols = ["pid", "season_year", "mins", "pct_paint", "fg3a_75", "oreb_pct", "tov_pct", "fta_75",
            "blk_75", "height", "dreb_pct", "stl_75", "pf_75"]
    f = pd.read_parquet(METRIC_DATA / "features_box_season.parquet", columns=cols)
    f = f[(f.mins >= 300) & f.season_year.between(2020, 2024)].copy()
    raw = [c for c in cols if c not in ["pid", "season_year", "mins"]]
    for col in raw:
        f["z_" + col] = f.groupby("season_year", group_keys=False).apply(
            lambda g: weighted_z(g, col), include_groups=False
        )
    f["o_rim"] = f.z_pct_paint
    f["o_spacing"] = f.z_fg3a_75
    f["o_oreb"] = f.z_oreb_pct
    f["o_turnover"] = f.z_tov_pct
    f["o_foul_pressure"] = f.z_fta_75
    f["d_rim_anchor"] = (f.z_blk_75 + f.z_height) / 2
    f["d_size"] = f.z_height
    f["d_dreb"] = f.z_dreb_pct
    f["d_disruption"] = f.z_stl_75
    f["d_foul_rate"] = f.z_pf_75
    f["event_season"] = f.season_year + 1
    return f[["pid", "event_season"] + O_TRAITS + D_TRAITS]


def lookup_matrix(ids: np.ndarray, seasons: np.ndarray, table: dict, default: float = 0.0) -> np.ndarray:
    out = np.empty(ids.shape, dtype=np.float64)
    for j in range(ids.shape[1]):
        out[:, j] = [table.get((int(p), int(s)), default) for p, s in zip(ids[:, j], seasons[:, j])]
    return out


def build_panel() -> pd.DataFrame:
    st = prepare()
    st["date"] = pd.to_datetime(st.date)
    st["season_year"] = st.date.dt.year - (st.date.dt.month < 10)
    st = st[(st.is_playoff == 0) & st.season_year.isin(TARGET_SEASONS) & (st.seconds >= MIN_SECONDS)].copy()
    st = st[st[PCOLS].notna().all(axis=1)]
    st = st[(st[PCOLS].astype(int) != 0).all(axis=1)].reset_index(drop=True)
    st["poss"] = np.maximum(st.seconds.to_numpy(float) / 24.0, 0.1)

    k = pd.read_parquet(METRIC_DATA / "kalman" / "kalman_states.parquet")
    first = k.groupby("player_id").season_year.min().to_dict()
    k["frozen_ok"] = [int(s) > int(first[p]) for p, s in zip(k.player_id, k.season_year)]
    ko = {(int(r.player_id), int(r.season_year)): float(r.pred_o) for r in k[k.frozen_ok].itertuples()}
    kd = {(int(r.player_id), int(r.season_year)): float(r.pred_d) for r in k[k.frozen_ok].itertuples()}
    valid = set(ko)

    traits = trait_table()
    trait_maps = {
        c: {(int(r.pid), int(r.event_season)): float(getattr(r, c)) for r in traits.itertuples()}
        for c in O_TRAITS + D_TRAITS
    }

    ids = st[PCOLS].to_numpy(int)
    seasons = np.repeat(st.season_year.to_numpy(int)[:, None], 10, axis=1)
    frozen = np.empty(ids.shape, dtype=bool)
    for j in range(10):
        frozen[:, j] = [(int(p), int(s)) in valid for p, s in zip(ids[:, j], seasons[:, j])]
    st["frozen_slots"] = frozen.sum(axis=1)
    st = st[st.frozen_slots >= MIN_FROZEN_SLOTS].copy()
    keep = st.index.to_numpy()
    ids, seasons, frozen = ids[keep], seasons[keep], frozen[keep]

    rating_o = lookup_matrix(ids, seasons, ko, default=-0.5)
    rating_d = lookup_matrix(ids, seasons, kd, default=-0.2)
    trait_arrays = {c: lookup_matrix(ids, seasons, m, default=0.0) for c, m in trait_maps.items()}

    rows = []
    for home_offense in [True, False]:
        oi = slice(0, 5) if home_offense else slice(5, 10)
        di = slice(5, 10) if home_offense else slice(0, 5)
        r = pd.DataFrame({
            "game_id": st.game_id.to_numpy(),
            "season_year": st.season_year.to_numpy(int),
            "date": st.date.to_numpy(),
            "offense_is_home": int(home_offense),
            "poss": st.poss.to_numpy(float),
            "points_adj": st.home_pts_adj.to_numpy(float) if home_offense else st.away_pts_adj.to_numpy(float),
            "rapm_expectation": rating_o[:, oi].sum(axis=1) - rating_d[:, di].sum(axis=1),
            "missing_offense": 5 - frozen[:, oi].sum(axis=1),
            "missing_defense": 5 - frozen[:, di].sum(axis=1),
        })
        offense_ratings = rating_o[:, oi]
        defense_ratings = rating_d[:, di]
        r["offense_star_concentration"] = offense_ratings.max(axis=1) - offense_ratings.mean(axis=1)
        r["defense_weak_link"] = defense_ratings.mean(axis=1) - defense_ratings.min(axis=1)
        r["defense_anchor"] = defense_ratings.max(axis=1) - defense_ratings.mean(axis=1)
        r["defense_rating_spread"] = defense_ratings.std(axis=1)
        for c in O_TRAITS:
            r[c] = trait_arrays[c][:, oi].mean(axis=1)
        for c in D_TRAITS:
            r[c] = trait_arrays[c][:, di].mean(axis=1)
        for name, (oc, dc) in INTERACTIONS.items():
            r[name] = r[oc] * r[dc]
        for name, (oc, dc) in RATING_INTERACTIONS.items():
            r[name] = r[oc] * r[dc]
        rows.append(r)
    long = pd.concat(rows, ignore_index=True)
    value_cols = (["rapm_expectation", "missing_offense", "missing_defense"] + O_TRAITS + D_TRAITS
                  + list(INTERACTIONS) + RATING_SHAPES + list(RATING_INTERACTIONS))
    for c in value_cols:
        long["w_" + c] = long[c] * long.poss
    long["w_points"] = long.points_adj
    ag = long.groupby(["game_id", "season_year", "date", "offense_is_home"], as_index=False)[
        ["poss", "w_points"] + ["w_" + c for c in value_cols]
    ].sum()
    ag["points_per100"] = 100.0 * ag.w_points / ag.poss
    for c in value_cols:
        ag[c] = ag["w_" + c] / ag.poss
    ag = ag.drop(columns=["w_points"] + ["w_" + c for c in value_cols])
    ag["coverage_sensitivity_all10"] = (ag.missing_offense == 0) & (ag.missing_defense == 0)

    # Remove only the contemporaneous league scoring environment. This common
    # nuisance operation is applied identically to every competing model.
    means = ag.groupby("season_year").apply(
        lambda g: pd.Series({"league_pp100": np.average(g.points_per100, weights=g.poss)}), include_groups=False
    )
    ag = ag.merge(means, left_on="season_year", right_index=True)
    ag["target"] = ag.points_per100 - ag.league_pp100
    OUT_PANEL.parent.mkdir(parents=True, exist_ok=True)
    ag.to_parquet(OUT_PANEL, index=False)
    return ag


def standardize(train: pd.DataFrame, other: pd.DataFrame, cols: list[str]) -> tuple[np.ndarray, np.ndarray]:
    a, b = [], []
    w = train.poss.to_numpy(float)
    for c in cols:
        mu = np.average(train[c], weights=w)
        sd = np.sqrt(np.average((train[c] - mu) ** 2, weights=w))
        sd = max(float(sd), 1e-9)
        a.append((train[c].to_numpy(float) - mu) / sd)
        b.append((other[c].to_numpy(float) - mu) / sd)
    return np.column_stack(a), np.column_stack(b)


def ridge(x: np.ndarray, y: np.ndarray, w: np.ndarray, alpha: float) -> np.ndarray:
    xx = np.column_stack([np.ones(len(x)), x])
    penalty = np.eye(xx.shape[1]) * alpha
    penalty[0, 0] = 0.0
    a = xx.T @ (w[:, None] * xx) + penalty
    b = xx.T @ (w * y)
    try:
        return np.linalg.solve(a, b)
    except np.linalg.LinAlgError:
        return np.linalg.pinv(a) @ b


def predict(x: np.ndarray, beta: np.ndarray) -> np.ndarray:
    return np.column_stack([np.ones(len(x)), x]) @ beta


def choose_alpha(train: pd.DataFrame, validation: pd.DataFrame, cols: list[str]) -> float:
    xt, xv = standardize(train, validation, cols)
    scores = []
    for alpha in ALPHAS:
        beta = ridge(xt, train.target.to_numpy(), train.poss.to_numpy(), alpha)
        pv = predict(xv, beta)
        scores.append(np.average((validation.target.to_numpy() - pv) ** 2, weights=validation.poss.to_numpy()))
    return ALPHAS[int(np.argmin(scores))]


def cluster_comparison(test: pd.DataFrame, pa: np.ndarray, pb: np.ndarray, draws: int = 2000) -> dict:
    y, w = test.target.to_numpy(), test.poss.to_numpy()
    q = pd.DataFrame({"game_id": test.game_id, "la": w * (y - pa) ** 2, "lb": w * (y - pb) ** 2, "w": w}).groupby("game_id").sum()
    arr = q[["la", "lb", "w"]].to_numpy()

    def improvement(a: np.ndarray) -> float:
        return float(100 * (1 - np.sqrt(a[:, 1].sum() / a[:, 2].sum()) / np.sqrt(a[:, 0].sum() / a[:, 2].sum())))

    rng = np.random.default_rng(20260713)
    boot = np.empty(draws)
    for i in range(draws):
        boot[i] = improvement(arr[rng.integers(0, len(arr), len(arr))])
    return {"rmse_improvement_pct": improvement(arr), "game_cluster_95pct": [float(np.quantile(boot, .025)), float(np.quantile(boot, .975))]}


def run_sample(panel: pd.DataFrame) -> dict:
    tune_train = panel[panel.season_year <= 2022].copy()
    validation = panel[panel.season_year == 2023].copy()
    train = panel[panel.season_year <= 2023].copy()
    test = panel[panel.season_year >= 2024].copy()
    nuisance = ["rapm_expectation", "offense_is_home", "missing_offense", "missing_defense"]
    additive = nuisance + O_TRAITS + D_TRAITS
    specs = {"frozen_rapm": nuisance, "rapm_plus_additive_traits": additive}
    for interaction in INTERACTIONS:
        specs["additive_plus_" + interaction] = additive + [interaction]
    specs["additive_plus_all_contexts"] = additive + list(INTERACTIONS)

    preds, models = {}, {}
    for name, cols in specs.items():
        alpha = choose_alpha(tune_train, validation, cols)
        xtr, xte = standardize(train, test, cols)
        beta = ridge(xtr, train.target.to_numpy(), train.poss.to_numpy(), alpha)
        preds[name] = predict(xte, beta)
        models[name] = {"features": cols, "alpha": alpha, "coefficients_standardized": [float(v) for v in beta]}

    y, w = test.target.to_numpy(), test.poss.to_numpy()
    for name, p in preds.items():
        models[name]["test_rmse"] = float(np.sqrt(np.average((y - p) ** 2, weights=w)))
        models[name]["by_season_rmse"] = {
            str(int(s)): float(np.sqrt(np.average((test.loc[test.season_year == s, "target"] - p[test.season_year.to_numpy() == s]) ** 2,
                                                  weights=test.loc[test.season_year == s, "poss"])))
            for s in sorted(test.season_year.unique())
        }
    comparisons = {
        "additive_vs_frozen_rapm": cluster_comparison(test, preds["frozen_rapm"], preds["rapm_plus_additive_traits"]),
        "all_contexts_vs_frozen_rapm": cluster_comparison(test, preds["frozen_rapm"], preds["additive_plus_all_contexts"]),
        "all_contexts_vs_additive": cluster_comparison(test, preds["rapm_plus_additive_traits"], preds["additive_plus_all_contexts"]),
    }
    for interaction in INTERACTIONS:
        name = "additive_plus_" + interaction
        comparisons[name + "_vs_additive"] = cluster_comparison(test, preds["rapm_plus_additive_traits"], preds[name])
    return {
        "rows": int(len(panel)), "games": int(panel.game_id.nunique()),
        "train_rows_through_2023": int(len(train)), "test_rows": int(len(test)), "test_games": int(test.game_id.nunique()),
        "models": models, "comparisons": comparisons,
    }


def run_rating_shape_confirmation(panel: pd.DataFrame) -> dict:
    """Use 2024-25 for tuning/selection and 2025-26 only for confirmation."""
    tune_train = panel[panel.season_year <= 2023].copy()
    validation = panel[panel.season_year == 2024].copy()
    train = panel[panel.season_year <= 2024].copy()
    test = panel[panel.season_year == 2025].copy()
    nuisance = ["rapm_expectation", "offense_is_home", "missing_offense", "missing_defense"]
    shape = nuisance + RATING_SHAPES
    specs = {"frozen_rapm": nuisance, "rapm_plus_rating_shape": shape}
    for interaction in RATING_INTERACTIONS:
        specs["shape_plus_" + interaction] = shape + [interaction]
    specs["shape_plus_all_rating_contexts"] = shape + list(RATING_INTERACTIONS)

    preds, models = {}, {}
    for name, cols in specs.items():
        alpha = choose_alpha(tune_train, validation, cols)
        xtr, xte = standardize(train, test, cols)
        beta = ridge(xtr, train.target.to_numpy(), train.poss.to_numpy(), alpha)
        preds[name] = predict(xte, beta)
        models[name] = {"features": cols, "alpha": alpha, "coefficients_standardized": [float(v) for v in beta]}
    y, w = test.target.to_numpy(), test.poss.to_numpy()
    for name, p in preds.items():
        models[name]["confirmation_2025_rmse"] = float(np.sqrt(np.average((y - p) ** 2, weights=w)))
    comparisons = {
        "rating_shape_vs_frozen_rapm": cluster_comparison(test, preds["frozen_rapm"], preds["rapm_plus_rating_shape"]),
        "all_rating_contexts_vs_frozen_rapm": cluster_comparison(test, preds["frozen_rapm"], preds["shape_plus_all_rating_contexts"]),
        "all_rating_contexts_vs_rating_shape": cluster_comparison(test, preds["rapm_plus_rating_shape"], preds["shape_plus_all_rating_contexts"]),
    }
    for interaction in RATING_INTERACTIONS:
        name = "shape_plus_" + interaction
        comparisons[name + "_vs_rating_shape"] = cluster_comparison(test, preds["rapm_plus_rating_shape"], preds[name])
    return {
        "selection_season": 2024, "confirmation_season": 2025,
        "train_rows_through_2024": int(len(train)), "confirmation_rows": int(len(test)),
        "confirmation_games": int(test.game_id.nunique()), "models": models, "comparisons": comparisons,
    }


def main() -> None:
    panel = build_panel()
    report = {
        "claim_tier": "predictive residual structure; not causal identification",
        "unit": "offense-team game aggregated from lineup stints",
        "outcome": "luck-adjusted points per 100 centered on target-season league environment",
        "baseline": "possession-weighted frozen preseason offensive RAPM minus defensive RAPM",
        "feature_timing": "immediately preceding season",
        "interactions_preregistered": INTERACTIONS,
        "rating_shape_interactions_second_family": RATING_INTERACTIONS,
        "primary_at_least_8_of_10_frozen_slots": run_sample(panel),
        "sensitivity_all_10_frozen_slots": run_sample(panel[panel.coverage_sensitivity_all10].copy()),
        "rating_shape_2025_confirmation_primary": run_rating_shape_confirmation(panel),
        "rating_shape_2025_confirmation_all10_sensitivity": run_rating_shape_confirmation(panel[panel.coverage_sensitivity_all10].copy()),
    }
    OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    OUT_REPORT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
