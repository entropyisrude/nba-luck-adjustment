"""Fit and evaluate the first chronological burden-response model.

The target is receiving-player change in composite creation load during a
strict creator-absence event. Model selection uses 2023-24 only; 2024-25 and
2025-26 remain untouched until the final evaluation.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import OneHotEncoder, StandardScaler


ROOT = Path(__file__).resolve().parents[1]
PANEL = ROOT / "derived" / "contextual_causal" / "burden_transfer_player_event_panel.csv.gz"
DERIVED = ROOT / "derived" / "contextual_causal"
OUTPUTS = ROOT / "outputs" / "contextual_causal"

TARGET = "delta_creation_load"
TRAIN_MAX = 2022
VALIDATION_SEASON = 2023
TEST_MIN = 2024
ALPHAS = [1.0, 10.0, 100.0, 1000.0]
NUMERIC = [
    "expected_minutes", "expected_fga", "expected_fta", "expected_fg3a",
    "expected_ast", "expected_tov", "expected_pts", "expected_creation_load",
    "prior_minutes", "prior_creation_p36", "prior_ast_p36", "creator_rank",
    "is_back_to_back", "home_filled", "season_start", "shock_size",
    "proportional_raw",
]


def prepare() -> pd.DataFrame:
    df = pd.read_csv(PANEL, dtype={"game_id": str})
    df = df[df["analysis_eligible"] == 1].copy()
    df["season_start"] = pd.to_numeric(df["season"].str[:4], errors="coerce")
    df["home_filled"] = pd.to_numeric(df["home"], errors="coerce").fillna(0.5)
    df["shock_size"] = df["prior_creation_p36"] * df["prior_minutes"] / 36.0
    denom = df.groupby("event_id")["expected_creation_load"].transform("sum").clip(lower=1e-9)
    df["proportional_raw"] = df["shock_size"] * df["expected_creation_load"] / denom
    return df.sort_values(["date", "event_id", "receiver_player_id"]).reset_index(drop=True)


class Design:
    def __init__(self) -> None:
        self.players = OneHotEncoder(handle_unknown="ignore", sparse_output=True)
        self.scaler = StandardScaler()
        self.shock_mean = 0.0
        self.shock_scale = 1.0

    def fit_transform(self, df: pd.DataFrame) -> sparse.csr_matrix:
        p = self.players.fit_transform(df[["receiver_player_id"]])
        n = sparse.csr_matrix(self.scaler.fit_transform(df[NUMERIC]))
        self.shock_mean = float(df["shock_size"].mean())
        self.shock_scale = float(df["shock_size"].std(ddof=0)) or 1.0
        z = ((df["shock_size"].to_numpy() - self.shock_mean) / self.shock_scale)[:, None]
        return sparse.hstack([n, p, p.multiply(z)], format="csr")

    def transform(self, df: pd.DataFrame) -> sparse.csr_matrix:
        p = self.players.transform(df[["receiver_player_id"]])
        n = sparse.csr_matrix(self.scaler.transform(df[NUMERIC]))
        z = ((df["shock_size"].to_numpy() - self.shock_mean) / self.shock_scale)[:, None]
        return sparse.hstack([n, p, p.multiply(z)], format="csr")


def metrics(y: np.ndarray, prediction: np.ndarray) -> dict[str, float]:
    correlation = float("nan")
    if np.std(y) > 0 and np.std(prediction) > 0:
        correlation = float(np.corrcoef(y, prediction)[0, 1])
    return {
        "mae": float(mean_absolute_error(y, prediction)),
        "rmse": float(mean_squared_error(y, prediction) ** 0.5),
        "bias": float(np.mean(prediction - y)),
        "correlation": correlation,
    }


def event_metrics(frame: pd.DataFrame, prediction: np.ndarray) -> dict[str, float]:
    temp = frame[["event_id", TARGET]].copy()
    temp["prediction"] = prediction
    agg = temp.groupby("event_id")[[TARGET, "prediction"]].sum()
    return metrics(agg[TARGET].to_numpy(), agg["prediction"].to_numpy())


def fit_ridge(train: pd.DataFrame, score: pd.DataFrame, alpha: float) -> tuple[Design, Ridge, np.ndarray]:
    design = Design()
    x_train = design.fit_transform(train)
    model = Ridge(alpha=alpha, solver="lsqr")
    model.fit(x_train, train[TARGET].to_numpy())
    return design, model, model.predict(design.transform(score))


def fit_ablation(train: pd.DataFrame, score: pd.DataFrame, alpha: float, include_player: bool) -> np.ndarray:
    scaler = StandardScaler()
    x_numeric = sparse.csr_matrix(scaler.fit_transform(train[NUMERIC]))
    x_score_numeric = sparse.csr_matrix(scaler.transform(score[NUMERIC]))
    if include_player:
        encoder = OneHotEncoder(handle_unknown="ignore", sparse_output=True)
        x_player = encoder.fit_transform(train[["receiver_player_id"]])
        x_score_player = encoder.transform(score[["receiver_player_id"]])
        x_train = sparse.hstack([x_numeric, x_player], format="csr")
        x_score = sparse.hstack([x_score_numeric, x_score_player], format="csr")
    else:
        x_train, x_score = x_numeric, x_score_numeric
    model = Ridge(alpha=alpha, solver="lsqr")
    model.fit(x_train, train[TARGET].to_numpy())
    return model.predict(x_score)


def baseline_predictions(train: pd.DataFrame, score: pd.DataFrame) -> dict[str, np.ndarray]:
    y = train[TARGET].to_numpy()
    global_mean = float(np.mean(y))
    player_mean = train.groupby("receiver_player_id")[TARGET].mean()
    prop = train["proportional_raw"].to_numpy()
    prop_scale = float(np.dot(prop, y) / max(np.dot(prop, prop), 1e-9))
    return {
        "zero_change": np.zeros(len(score)),
        "global_mean": np.full(len(score), global_mean),
        "receiver_historical_mean": score["receiver_player_id"].map(player_mean).fillna(global_mean).to_numpy(),
        "proportional_redistribution": prop_scale * score["proportional_raw"].to_numpy(),
    }


def evaluate_models(train: pd.DataFrame, score: pd.DataFrame, contextual_prediction: np.ndarray) -> dict:
    predictions = baseline_predictions(train, score)
    predictions["contextual_partial_pooling"] = contextual_prediction
    y = score[TARGET].to_numpy()
    return {
        name: {"receiver_row": metrics(y, pred), "event_total": event_metrics(score, pred)}
        for name, pred in predictions.items()
    }


def player_response_table(design: Design, model: Ridge, fit: pd.DataFrame, test: pd.DataFrame) -> pd.DataFrame:
    n_numeric = len(NUMERIC)
    categories = design.players.categories_[0].astype(int)
    n_players = len(categories)
    intercept_coef = model.coef_[n_numeric:n_numeric + n_players]
    elasticity_coef = model.coef_[n_numeric + n_players:n_numeric + 2 * n_players] / design.shock_scale
    name_map = fit.drop_duplicates("receiver_player_id", keep="last").set_index("receiver_player_id")["receiver_player_name"]
    out = pd.DataFrame({
        "receiver_player_id": categories,
        "receiver_player_name": pd.Series(categories).map(name_map).to_numpy(),
        "partial_pool_intercept": intercept_coef,
        "shock_elasticity_per_removed_load": elasticity_coef,
    })
    fit_count = fit.groupby("receiver_player_id").size()
    test_count = test.groupby("receiver_player_id").size()
    test_actual = test.groupby("receiver_player_id")[TARGET].mean()
    out["fit_rows"] = out["receiver_player_id"].map(fit_count).fillna(0).astype(int)
    out["test_rows"] = out["receiver_player_id"].map(test_count).fillna(0).astype(int)
    out["test_mean_actual_delta"] = out["receiver_player_id"].map(test_actual)
    return out.sort_values("shock_elasticity_per_removed_load", ascending=False).reset_index(drop=True)


def markdown_report(result: dict, responses: pd.DataFrame) -> str:
    test = result["test_metrics"]
    lines = [
        "# First Burden-Response Model Results", "",
        "## Design", "",
        "The target is receiving-player change in composite creation load relative to a frozen 10-appearance baseline. Training ends in 2022-23, model selection uses 2023-24, and the untouched test set is 2024-25 through 2025-26.", "",
        "## Held-out test performance", "",
        "| Model | Receiver MAE | Receiver RMSE | Event-total MAE | Event-total RMSE | Correlation |", "|---|---:|---:|---:|---:|---:|",
    ]
    for name, value in test.items():
        r, e = value["receiver_row"], value["event_total"]
        lines.append(f"| {name} | {r['mae']:.3f} | {r['rmse']:.3f} | {e['mae']:.3f} | {e['rmse']:.3f} | {r['correlation']:.3f} |")
    stable = responses[responses["fit_rows"] >= 50]
    lines += ["", "## Highest estimated shock elasticities", "", "Minimum 50 fitting rows; these are regularized predictive response estimates, not causal player rankings.", "", "| Player | Fit rows | Elasticity | Test mean delta |", "|---|---:|---:|---:|"]
    for row in stable.head(15).itertuples():
        lines.append(f"| {row.receiver_player_name} | {row.fit_rows} | {row.shock_elasticity_per_removed_load:.4f} | {row.test_mean_actual_delta:.3f} |")
    lines += ["", "## Interpretation boundary", "", "A useful result requires the contextual model to beat simple baselines on the untouched seasons and to remain credible at the event-total level. Player elasticities are partial-pooling predictive summaries. Absence announcement timing, lineup plans, and injury context remain unobserved, so causal interpretation is premature."]
    return "\n".join(lines) + "\n"


def main() -> None:
    df = prepare()
    train = df[df["season_start"] <= TRAIN_MAX].copy()
    validation = df[df["season_start"] == VALIDATION_SEASON].copy()
    test = df[df["season_start"] >= TEST_MIN].copy()

    validation_results = {}
    for alpha in ALPHAS:
        _, _, prediction = fit_ridge(train, validation, alpha)
        validation_results[str(alpha)] = metrics(validation[TARGET].to_numpy(), prediction)
    best_alpha = min(ALPHAS, key=lambda a: validation_results[str(a)]["rmse"])

    ablation_validation = {"generic_context": {}, "player_intercept": {}}
    for alpha in ALPHAS:
        generic = fit_ablation(train, validation, alpha, include_player=False)
        player = fit_ablation(train, validation, alpha, include_player=True)
        ablation_validation["generic_context"][str(alpha)] = metrics(validation[TARGET].to_numpy(), generic)
        ablation_validation["player_intercept"][str(alpha)] = metrics(validation[TARGET].to_numpy(), player)
    generic_alpha = min(ALPHAS, key=lambda a: ablation_validation["generic_context"][str(a)]["rmse"])
    player_alpha = min(ALPHAS, key=lambda a: ablation_validation["player_intercept"][str(a)]["rmse"])

    fit = pd.concat([train, validation], ignore_index=True)
    design, model, test_prediction = fit_ridge(fit, test, best_alpha)
    test_metrics = evaluate_models(fit, test, test_prediction)
    generic_prediction = fit_ablation(fit, test, generic_alpha, include_player=False)
    player_prediction = fit_ablation(fit, test, player_alpha, include_player=True)
    y_test = test[TARGET].to_numpy()
    test_metrics["generic_context_ridge"] = {
        "receiver_row": metrics(y_test, generic_prediction),
        "event_total": event_metrics(test, generic_prediction),
    }
    test_metrics["player_intercept_ridge"] = {
        "receiver_row": metrics(y_test, player_prediction),
        "event_total": event_metrics(test, player_prediction),
    }
    responses = player_response_table(design, model, fit, test)
    predictions = test[[
        "date", "season", "event_id", "game_id", "team_abbr", "opponent",
        "absent_player_id", "absent_player_name", "receiver_player_id",
        "receiver_player_name", "shock_size", "creator_rank", "shock_tier",
        "is_back_to_back", TARGET,
    ]].copy()
    predictions["contextual_prediction"] = test_prediction
    for name, values in baseline_predictions(fit, test).items():
        predictions[f"prediction_{name}"] = values

    result = {
        "split": {
            "train_through": "2022-23", "validation": "2023-24", "test": ["2024-25", "2025-26"],
            "train_rows": int(len(train)), "validation_rows": int(len(validation)), "test_rows": int(len(test)),
            "test_events": int(test["event_id"].nunique()),
        },
        "alpha_validation": validation_results,
        "selected_alpha": best_alpha,
        "ablation_validation": ablation_validation,
        "selected_ablation_alphas": {"generic_context": generic_alpha, "player_intercept": player_alpha},
        "test_metrics": test_metrics,
    }
    DERIVED.mkdir(parents=True, exist_ok=True)
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(DERIVED / "first_burden_model_test_predictions.csv", index=False)
    responses.to_csv(DERIVED / "first_burden_model_player_responses.csv", index=False)
    (OUTPUTS / "first_burden_model_results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    (OUTPUTS / "first_burden_model_report.md").write_text(markdown_report(result, responses), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
