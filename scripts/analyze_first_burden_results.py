"""Paired uncertainty and subgroup analysis for the first held-out model."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PREDICTIONS = ROOT / "derived" / "contextual_causal" / "first_burden_model_test_predictions.csv"
OUTPUTS = ROOT / "outputs" / "contextual_causal"
RNG = np.random.default_rng(20260712)
BOOTSTRAPS = 2000


def paired_event_bootstrap(df: pd.DataFrame) -> dict:
    event = df.groupby("event_id").agg(
        actual=("delta_creation_load", "sum"),
        contextual=("contextual_prediction", "sum"),
        proportional=("prediction_proportional_redistribution", "sum"),
    )
    context_abs = np.abs(event["contextual"] - event["actual"]).to_numpy()
    prop_abs = np.abs(event["proportional"] - event["actual"]).to_numpy()
    context_sq = ((event["contextual"] - event["actual"]) ** 2).to_numpy()
    prop_sq = ((event["proportional"] - event["actual"]) ** 2).to_numpy()
    n = len(event)
    mae_improvements = np.empty(BOOTSTRAPS)
    rmse_improvements = np.empty(BOOTSTRAPS)
    for i in range(BOOTSTRAPS):
        idx = RNG.integers(0, n, n)
        mae_improvements[i] = prop_abs[idx].mean() - context_abs[idx].mean()
        rmse_improvements[i] = np.sqrt(prop_sq[idx].mean()) - np.sqrt(context_sq[idx].mean())
    return {
        "events": n,
        "event_mae_improvement": float(prop_abs.mean() - context_abs.mean()),
        "event_mae_improvement_95ci": [float(x) for x in np.quantile(mae_improvements, [0.025, 0.975])],
        "event_rmse_improvement": float(np.sqrt(prop_sq.mean()) - np.sqrt(context_sq.mean())),
        "event_rmse_improvement_95ci": [float(x) for x in np.quantile(rmse_improvements, [0.025, 0.975])],
        "probability_contextual_better_mae": float(np.mean(mae_improvements > 0)),
        "probability_contextual_better_rmse": float(np.mean(rmse_improvements > 0)),
    }


def slice_metrics(df: pd.DataFrame) -> dict:
    event = df.groupby("event_id").agg(
        actual=("delta_creation_load", "sum"),
        contextual=("contextual_prediction", "sum"),
        proportional=("prediction_proportional_redistribution", "sum"),
    )
    def values(pred: str) -> dict:
        error = event[pred] - event["actual"]
        return {
            "mae": float(np.abs(error).mean()),
            "rmse": float(np.sqrt(np.mean(error ** 2))),
            "correlation": float(event[["actual", pred]].corr().iloc[0, 1]),
        }
    return {"events": int(len(event)), "contextual": values("contextual"), "proportional": values("proportional")}


def main() -> None:
    df = pd.read_csv(PREDICTIONS, dtype={"game_id": str})
    analysis = {"paired_cluster_bootstrap": paired_event_bootstrap(df), "subgroups": {}}
    slices = {
        "2024-25": df["season"].eq("2024-25"),
        "2025-26": df["season"].eq("2025-26"),
        "non_back_to_back": df["is_back_to_back"].eq(0),
        "back_to_back": df["is_back_to_back"].eq(1),
        "primary_creator_rank_1": df["creator_rank"].eq(1),
        "secondary_creator_rank_2": df["creator_rank"].eq(2),
    }
    for name, mask in slices.items():
        analysis["subgroups"][name] = slice_metrics(df[mask])

    event = df.groupby("event_id").agg(actual=("delta_creation_load", "sum"), prediction=("contextual_prediction", "sum"))
    slope, intercept = np.polyfit(event["prediction"], event["actual"], 1)
    analysis["event_calibration"] = {"slope_actual_on_prediction": float(slope), "intercept": float(intercept)}

    lines = ["# Robustness Analysis: First Burden Model", "", "## Paired event-cluster bootstrap", ""]
    b = analysis["paired_cluster_bootstrap"]
    lines += [
        f"Across {b['events']} untouched events, contextual modeling improves event-total MAE over proportional redistribution by **{b['event_mae_improvement']:.3f}** (95% cluster-bootstrap CI {b['event_mae_improvement_95ci'][0]:.3f} to {b['event_mae_improvement_95ci'][1]:.3f}).",
        "",
        f"Event-total RMSE improves by **{b['event_rmse_improvement']:.3f}** (95% CI {b['event_rmse_improvement_95ci'][0]:.3f} to {b['event_rmse_improvement_95ci'][1]:.3f}).",
        "", "## Subgroups", "", "| Slice | Events | Context MAE | Proportional MAE | Context RMSE | Proportional RMSE | Context corr. |", "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for name, result in analysis["subgroups"].items():
        c, p = result["contextual"], result["proportional"]
        lines.append(f"| {name} | {result['events']} | {c['mae']:.3f} | {p['mae']:.3f} | {c['rmse']:.3f} | {p['rmse']:.3f} | {c['correlation']:.3f} |")
    lines += ["", "## Calibration", "", f"At the event-total level, actual-on-predicted calibration slope is {slope:.3f} with intercept {intercept:.3f}. A slope below one indicates predictions are too dispersed; above one indicates insufficient dispersion.", "", "These checks quantify predictive robustness. They do not resolve absence-timing confounding or turn the player coefficients into causal effects."]
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    (OUTPUTS / "first_burden_model_robustness.json").write_text(json.dumps(analysis, indent=2) + "\n", encoding="utf-8")
    (OUTPUTS / "first_burden_model_robustness.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(analysis, indent=2))


if __name__ == "__main__":
    main()
