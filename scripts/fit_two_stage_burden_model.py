"""Two-stage burden model: predict event total, then capability allocation."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler

from fit_capability_burden_response_model import ALPHAS, TARGET, prepare, predict, tune


ROOT = Path(__file__).resolve().parents[1]
DERIVED = ROOT / "derived" / "contextual_causal"
OUTPUTS = ROOT / "outputs" / "contextual_causal"
ALLOCATION_BLENDS = [0.0, 0.25, 0.5, 0.75, 1.0]
EVENT_FEATURES = [
    "shock_size", "prior_minutes", "prior_creation_p36", "prior_ast_p36",
    "creator_rank", "is_back_to_back", "home_filled", "season_start",
    "receiver_count", "sum_expected_minutes", "sum_expected_creation_load",
    "max_expected_creation_load", "sum_expected_fga", "sum_expected_ast",
]


def event_frame(rows: pd.DataFrame) -> pd.DataFrame:
    grouped = rows.groupby("event_id", sort=False)
    event = grouped.agg(
        date=("date", "first"), season=("season", "first"), season_start=("season_start", "first"),
        shock_size=("shock_size", "first"), prior_minutes=("prior_minutes", "first"),
        prior_creation_p36=("prior_creation_p36", "first"), prior_ast_p36=("prior_ast_p36", "first"),
        creator_rank=("creator_rank", "first"), is_back_to_back=("is_back_to_back", "first"),
        home_filled=("home_filled", "first"), receiver_count=("receiver_player_id", "size"),
        sum_expected_minutes=("expected_minutes", "sum"),
        sum_expected_creation_load=("expected_creation_load", "sum"),
        max_expected_creation_load=("expected_creation_load", "max"),
        sum_expected_fga=("expected_fga", "sum"), sum_expected_ast=("expected_ast", "sum"),
        actual_total=(TARGET, "sum"),
    ).reset_index()
    return event


def fit_total(train_rows: pd.DataFrame, score_rows: pd.DataFrame, alpha: float) -> np.ndarray:
    train, score = event_frame(train_rows), event_frame(score_rows)
    scaler = StandardScaler()
    x = scaler.fit_transform(train[EVENT_FEATURES]); xs = scaler.transform(score[EVENT_FEATURES])
    model = Ridge(alpha=alpha)
    model.fit(x, train["actual_total"])
    pred = model.predict(xs)
    return pd.Series(pred, index=score["event_id"]).reindex(score_rows["event_id"]).to_numpy()


def tune_total(train: pd.DataFrame, validation: pd.DataFrame) -> tuple[float, dict]:
    actual = validation.groupby("event_id")[TARGET].sum()
    results = {}
    for alpha in ALPHAS:
        row_pred = fit_total(train, validation, alpha)
        temp = validation[["event_id"]].copy(); temp["pred"] = row_pred
        pred = temp.groupby("event_id")["pred"].first().reindex(actual.index)
        results[str(alpha)] = values(actual.to_numpy(), pred.to_numpy())
    return min(ALPHAS, key=lambda a: results[str(a)]["rmse"]), results


def reconcile(rows: pd.DataFrame, raw: np.ndarray, total_row_prediction: np.ndarray, blend: float) -> np.ndarray:
    out = rows[["event_id", "expected_creation_load"]].copy()
    out["raw"] = raw; out["total"] = total_row_prediction
    count = out.groupby("event_id")["raw"].transform("size")
    equal = 1.0 / count
    denom = out.groupby("event_id")["expected_creation_load"].transform("sum").clip(lower=1e-9)
    role_share = out["expected_creation_load"] / denom
    weight = (1.0 - blend) * equal + blend * role_share
    raw_total = out.groupby("event_id")["raw"].transform("sum")
    return out["raw"].to_numpy() + weight.to_numpy() * (out["total"].to_numpy() - raw_total.to_numpy())


def values(y: np.ndarray, p: np.ndarray) -> dict:
    corr = float(np.corrcoef(y, p)[0, 1]) if np.std(p) > 0 else float("nan")
    return {"mae": float(mean_absolute_error(y, p)), "rmse": float(mean_squared_error(y, p) ** 0.5), "correlation": corr, "bias": float(np.mean(p-y))}


def evaluate(rows: pd.DataFrame, pred: np.ndarray) -> dict:
    receiver = values(rows[TARGET].to_numpy(), pred)
    temp = rows[["event_id", TARGET]].copy(); temp["pred"] = pred
    event = temp.groupby("event_id")[[TARGET, "pred"]].sum()
    return {"receiver": receiver, "event": values(event[TARGET].to_numpy(), event["pred"].to_numpy())}


def main() -> None:
    df, coverage = prepare()
    train = df[df["season_start"] == 2022].copy()
    validation = df[df["season_start"] == 2023].copy()
    test = df[df["season_start"] >= 2024].copy()

    capability_alpha, capability_validation = tune(train, validation, True)
    total_alpha, total_validation = tune_total(train, validation)
    raw_validation = predict(train, validation, capability_alpha, True)
    total_validation_rows = fit_total(train, validation, total_alpha)
    blend_results = {}
    for blend in ALLOCATION_BLENDS:
        reconciled = reconcile(validation, raw_validation, total_validation_rows, blend)
        blend_results[str(blend)] = evaluate(validation, reconciled)["receiver"]
    best_blend = min(ALLOCATION_BLENDS, key=lambda x: blend_results[str(x)]["rmse"])

    fit = pd.concat([train, validation], ignore_index=True)
    # Retune on the same validation choices only; do not inspect test to choose.
    raw_test = predict(fit, test, capability_alpha, True)
    total_test_rows = fit_total(fit, test, total_alpha)
    two_stage = reconcile(test, raw_test, total_test_rows, best_blend)
    generic_alpha, _ = tune(train, validation, False)
    generic_test = predict(fit, test, generic_alpha, False)
    proportional = test["proportional_raw"].to_numpy()
    scale = float(np.dot(fit["proportional_raw"], fit[TARGET]) / np.dot(fit["proportional_raw"], fit["proportional_raw"]))
    proportional *= scale

    result = {
        "question": "Does total-then-capability allocation improve both team totals and teammate allocation?",
        "coverage": coverage,
        "split": {"train": "2022-23", "validation": "2023-24", "test": ["2024-25", "2025-26"], "test_rows": len(test), "test_events": int(test.event_id.nunique())},
        "selected": {"capability_alpha": capability_alpha, "total_alpha": total_alpha, "allocation_blend_role_share": best_blend},
        "validation": {"capability_alpha_grid": capability_validation, "total_alpha_grid": total_validation, "allocation_blend_grid": blend_results},
        "test": {
            "proportional": evaluate(test, proportional),
            "generic_row_context": evaluate(test, generic_test),
            "raw_capability": evaluate(test, raw_test),
            "two_stage_total_then_capability": evaluate(test, two_stage),
        },
    }
    pred = test[["date", "season", "event_id", "receiver_player_id", "receiver_player_name", "absent_player_name", TARGET]].copy()
    pred["proportional_prediction"] = proportional
    pred["generic_prediction"] = generic_test
    pred["raw_capability_prediction"] = raw_test
    pred["two_stage_prediction"] = two_stage
    DERIVED.mkdir(parents=True, exist_ok=True); OUTPUTS.mkdir(parents=True, exist_ok=True)
    pred.to_csv(DERIVED / "two_stage_burden_model_test_predictions.csv", index=False)
    (OUTPUTS / "two_stage_burden_model_results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
