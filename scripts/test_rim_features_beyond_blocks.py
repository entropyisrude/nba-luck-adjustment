"""Test whether frozen rim-defense traits add held-out signal beyond blocks.

Treatment: an adjacent-stint substitution of defender A for defender B while the
offensive five and the other four defenders are unchanged. Outcomes are the
season/period-trend-adjusted difference in first-chance recorded rim-attempt rate
and rim FG% conditional on a recorded attempt.

Models train on 2020-21 through 2023-24 and are evaluated on 2024-25 and 2025-26.
Every player feature comes from the immediately preceding season. The required
benchmark is a blocks-per-75-only model.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

import screen_rim_deterrence_features as screen


ROOT = Path(__file__).resolve().parents[1]
EVENTS = ROOT / "outputs" / "defense_causal" / "adjacent_substitution_rim_events_2020_21_to_2025_26.parquet"
OUT = ROOT / "outputs" / "contextual_causal" / "rim_features_beyond_blocks.json"

ALPHAS = [0.0, 1.0, 10.0, 100.0, 1000.0]
MODELS = {
    "blocks_only": ["blk_75"],
    "blocks_plus_height": ["blk_75", "height"],
    "blocks_plus_contests": ["blk_75", "contest_75"],
    "blocks_plus_fouls": ["blk_75", "pf_75"],
    "blocks_plus_contest_foul_ratio": ["blk_75", "contest_foul_ratio"],
    "blocks_plus_block_foul_ratio": ["blk_75", "block_foul_ratio"],
    "blocks_plus_rim_role": ["blk_75", "rim_dfga_pg"],
    "blocks_plus_prior_rim_fg": ["blk_75", "rim_dfg_pct"],
    "blocks_plus_prior_rim_suppression": ["blk_75", "rim_dfg_pct_diff"],
    "blocks_plus_height_contests_fouls": ["blk_75", "height", "contest_75", "pf_75"],
    "blocks_plus_compact_context": ["blk_75", "height", "contest_75", "pf_75", "rim_dfga_pg", "rim_dfg_pct_diff"],
}


def ridge_fit(x: np.ndarray, y: np.ndarray, w: np.ndarray, alpha: float) -> np.ndarray:
    return np.linalg.solve(x.T @ (w[:, None] * x) + alpha * np.eye(x.shape[1]), x.T @ (w * y))


def select_alpha(e: pd.DataFrame, cols: list[str], train: np.ndarray) -> float:
    x = e[["d_" + c for c in cols]].to_numpy()
    y = e.target.to_numpy()
    w = e.weight.to_numpy()
    scores = []
    for alpha in ALPHAS:
        fold_errors = []
        for season in sorted(e.loc[train, "season_year"].unique()):
            tr = train & (e.season_year != season)
            va = train & (e.season_year == season)
            beta = ridge_fit(x[tr], y[tr], w[tr], alpha)
            fold_errors.append(float(np.average((y[va] - x[va] @ beta) ** 2, weights=w[va])))
        scores.append(float(np.mean(fold_errors)))
    return ALPHAS[int(np.argmin(scores))]


def clustered_rmse_gain(e: pd.DataFrame, base: np.ndarray, candidate: np.ndarray, draws: int = 2000) -> dict:
    y = e.target.to_numpy()
    w = e.weight.to_numpy()
    z = pd.DataFrame({
        "game_id": e.game_id.to_numpy(),
        "base_loss": w * (y - base) ** 2,
        "candidate_loss": w * (y - candidate) ** 2,
        "weight": w,
    }).groupby("game_id", as_index=False).sum()

    def gain(a: pd.DataFrame) -> float:
        base_rmse = np.sqrt(a.base_loss.sum() / a.weight.sum())
        candidate_rmse = np.sqrt(a.candidate_loss.sum() / a.weight.sum())
        return float(100.0 * (1.0 - candidate_rmse / base_rmse))

    point = gain(z)
    arr = z[["base_loss", "candidate_loss", "weight"]].to_numpy()
    rng = np.random.default_rng(20260713)
    boot = np.empty(draws)
    for i in range(draws):
        a = arr[rng.integers(0, len(arr), len(arr))]
        boot[i] = 100.0 * (1.0 - np.sqrt(a[:, 1].sum() / a[:, 2].sum()) / np.sqrt(a[:, 0].sum() / a[:, 2].sum()))
    return {
        "rmse_improvement_pct_vs_blocks": point,
        "game_cluster_95pct": [float(np.quantile(boot, 0.025)), float(np.quantile(boot, 0.975))],
    }


def comparable_blocker_slopes(e: pd.DataFrame, tolerance: float) -> dict:
    z = e[(e.season_year >= 2024) & (e.d_blk_75.abs() <= tolerance)].copy()
    out = {"events": int(len(z)), "games": int(z.game_id.nunique()), "block_difference_tolerance_sd": tolerance, "features": {}}
    candidates = sorted({c for cols in MODELS.values() for c in cols if c != "blk_75"})
    for feature in candidates:
        x = z[["d_blk_75", "d_" + feature]].to_numpy()
        fit = sm.WLS(z.target.to_numpy(), x, weights=z.weight.to_numpy()).fit(
            cov_type="cluster", cov_kwds={"groups": z.game_id.to_numpy()}
        )
        out["features"][feature] = {
            "incremental_slope_per_sd": float(fit.params[1]),
            "cluster_95pct": [float(v) for v in fit.conf_int()[1]],
            "p_value": float(fit.pvalues[1]),
        }
    return out


def selector_head_to_head(e: pd.DataFrame, draws: int = 2000) -> dict:
    """Compare alternative top-tail selectors directly with top-15% blocks."""
    f = screen.feature_table().copy()
    f["score_size_blocks"] = (f.z_height + f.z_blk_75) / 2
    f["score_anchor"] = (
        f.z_height + f.z_blk_75 + f.z_contest_75 + f.z_rim_dfga_pg + f.z_dreb_75 + f.z_boxout_75 - f.z_pf_75
    ) / 7
    f["score_discipline"] = (f.z_blk_75 + f.z_contest_75 - f.z_pf_75) / 3
    selectors = {
        "blocks": "z_blk_75",
        "size_plus_blocks": "score_size_blocks",
        "contests": "z_contest_75",
        "anchor_composite": "score_anchor",
        "block_contest_discipline": "score_discipline",
        "rim_role": "z_rim_dfga_pg",
    }

    def game_totals(name: str, col: str) -> pd.DataFrame:
        parts = []
        for season, g in f.groupby("event_season"):
            threshold = g[col].quantile(0.85)
            parts.append(pd.DataFrame({"pid": g.pid, "season_year": season, "flag": g[col] >= threshold}))
        flag = pd.concat(parts, ignore_index=True)
        a = flag.rename(columns={"pid": "def_player_a", "flag": "flag_a"})
        b = flag.rename(columns={"pid": "def_player_b", "flag": "flag_b"})
        q = e.merge(a, on=["def_player_a", "season_year"]).merge(b, on=["def_player_b", "season_year"])
        q = q[q.flag_a ^ q.flag_b].copy()
        q["difference"] = q.target * np.where(q.flag_a, 1.0, -1.0)
        q["num"] = q.difference * q.weight
        return q.groupby("game_id")[["num", "weight"]].sum()

    totals = {name: game_totals(name, col) for name, col in selectors.items()}
    base = totals["blocks"]
    out = {}
    for name, candidate in totals.items():
        if name == "blocks":
            continue
        games = base.index.union(candidate.index)
        a = base.reindex(games, fill_value=0)[["num", "weight"]].to_numpy()
        b = candidate.reindex(games, fill_value=0)[["num", "weight"]].to_numpy()
        point = b[:, 0].sum() / b[:, 1].sum() - a[:, 0].sum() / a[:, 1].sum()
        rng = np.random.default_rng(20260714)
        boot = np.empty(draws)
        for i in range(draws):
            ix = rng.integers(0, len(games), len(games))
            boot[i] = b[ix, 0].sum() / b[ix, 1].sum() - a[ix, 0].sum() / a[ix, 1].sum()
        out[name] = {
            "candidate_minus_blocks_rate_difference": float(point),
            "negative_means_candidate_selects_better_defenders": True,
            "game_cluster_95pct": [float(np.quantile(boot, 0.025)), float(np.quantile(boot, 0.975))],
            "candidate_games": int(len(candidate)),
        }
    return out


def evaluate(outcome: str) -> dict:
    e, _ = screen.event_panel(outcome)
    train = (e.season_year <= 2023).to_numpy()
    test = (e.season_year >= 2024).to_numpy()
    y = e.target.to_numpy()
    w = e.weight.to_numpy()
    predictions = {}
    fits = {}
    for name, cols in MODELS.items():
        x = e[["d_" + c for c in cols]].to_numpy()
        alpha = select_alpha(e, cols, train)
        beta = ridge_fit(x[train], y[train], w[train], alpha)
        predictions[name] = x[test] @ beta
        fits[name] = {"features": cols, "alpha": alpha, "coefficients": [float(v) for v in beta]}

    test_e = e.loc[test].copy()
    base = predictions["blocks_only"]
    base_rmse = float(np.sqrt(np.average((test_e.target.to_numpy() - base) ** 2, weights=test_e.weight.to_numpy())))
    comparisons = {}
    for name, prediction in predictions.items():
        if name == "blocks_only":
            continue
        comparisons[name] = fits[name] | clustered_rmse_gain(test_e, base, prediction)

    return {
        "train_seasons": [2020, 2021, 2022, 2023],
        "test_seasons": [2024, 2025],
        "train_events": int(train.sum()),
        "test_events": int(test.sum()),
        "test_games": int(test_e.game_id.nunique()),
        "blocks_only": fits["blocks_only"] | {"test_rmse": base_rmse},
        "incremental_model_comparisons": comparisons,
        "comparable_blocker_test_0_25sd": comparable_blocker_slopes(e, 0.25),
        "comparable_blocker_test_0_50sd": comparable_blocker_slopes(e, 0.50),
        "top_15pct_selector_head_to_head_vs_blocks": selector_head_to_head(test_e),
    }


def main() -> None:
    screen.EVENTS = EVENTS
    report = {
        "claim_tier": "predictive/quasi-experimental screen, not causally identified player value",
        "estimand": "incremental held-out signal beyond prior-season blocks in matched adjacent-stint defender substitutions",
        "feature_timing": "immediately preceding season",
        "outcomes": {outcome: evaluate(outcome) for outcome in ["recorded_rim_attempt", "rim_fg_pct"]},
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
