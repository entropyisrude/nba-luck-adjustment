"""Test capability value separately across burden-transfer mechanisms."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler

from fit_capability_burden_response_model import ALPHAS, BASE, CAPABILITIES, prepare


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs" / "contextual_causal"
TARGETS = ["delta_minutes", "delta_fga", "delta_fta", "delta_fg3a", "delta_ast", "delta_tov", "delta_pts", "delta_creation_load"]


def design(train: pd.DataFrame, score: pd.DataFrame, capability: bool):
    columns = list(BASE)
    train = train.copy(); score = score.copy()
    if capability:
        columns += CAPABILITIES
        for col in CAPABILITIES:
            name = f"interaction_{col}"
            train[name] = train[col] * train["shock_size"]
            score[name] = score[col] * score["shock_size"]
            columns.append(name)
    scaler = StandardScaler()
    return scaler.fit_transform(train[columns]), scaler.transform(score[columns])


def predict(train: pd.DataFrame, score: pd.DataFrame, target: str, alpha: float, capability: bool):
    x, xs = design(train, score, capability)
    model = Ridge(alpha=alpha).fit(x, train[target])
    return model.predict(xs)


def metric(y, p):
    corr = float(np.corrcoef(y, p)[0, 1]) if np.std(p) > 0 else float("nan")
    return {"mae": float(mean_absolute_error(y, p)), "rmse": float(mean_squared_error(y, p) ** 0.5), "correlation": corr}


def event_metric(rows, target, pred):
    x = rows[["event_id", target]].copy(); x["prediction"] = pred
    x = x.groupby("event_id")[[target, "prediction"]].sum()
    return metric(x[target].to_numpy(), x["prediction"].to_numpy())


def tune(train, validation, target, capability):
    results = {}
    for alpha in ALPHAS:
        p = predict(train, validation, target, alpha, capability)
        results[str(alpha)] = metric(validation[target], p)
    return min(ALPHAS, key=lambda a: results[str(a)]["rmse"]), results


def main():
    df, coverage = prepare()
    train = df[df.season_start == 2022].copy()
    validation = df[df.season_start == 2023].copy()
    test = df[df.season_start >= 2024].copy()
    fit = pd.concat([train, validation], ignore_index=True)
    result = {"coverage": coverage, "test_rows": len(test), "test_events": int(test.event_id.nunique()), "targets": {}}
    for target in TARGETS:
        ga, gv = tune(train, validation, target, False)
        ca, cv = tune(train, validation, target, True)
        gp = predict(fit, test, target, ga, False)
        cp = predict(fit, test, target, ca, True)
        gm, cm = metric(test[target], gp), metric(test[target], cp)
        ge, ce = event_metric(test, target, gp), event_metric(test, target, cp)
        result["targets"][target] = {
            "selected_alpha": {"generic": ga, "capability": ca},
            "validation_rmse": {"generic": gv[str(ga)]["rmse"], "capability": cv[str(ca)]["rmse"]},
            "test_receiver": {"generic": gm, "capability": cm, "rmse_improvement": gm["rmse"] - cm["rmse"], "mae_improvement": gm["mae"] - cm["mae"]},
            "test_event": {"generic": ge, "capability": ce, "rmse_improvement": ge["rmse"] - ce["rmse"], "mae_improvement": ge["mae"] - ce["mae"]},
        }
    lines = ["# Capability Models by Burden Component", "", "Positive improvement means prior-season capabilities beat generic context on the untouched 2024-25 and 2025-26 test seasons.", "", "| Target | Receiver RMSE improvement | Receiver MAE improvement | Event RMSE improvement | Event MAE improvement |", "|---|---:|---:|---:|---:|"]
    for target, x in result["targets"].items():
        lines.append(f"| {target} | {x['test_receiver']['rmse_improvement']:.4f} | {x['test_receiver']['mae_improvement']:.4f} | {x['test_event']['rmse_improvement']:.4f} | {x['test_event']['mae_improvement']:.4f} |")
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "capability_component_model_results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    (OUT / "capability_component_model_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
