"""Cluster-bootstrap capability model versus generic context."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PRED = ROOT / "derived" / "contextual_causal" / "capability_burden_model_test_predictions.csv"
OUT = ROOT / "outputs" / "contextual_causal"
BOOTSTRAPS = 5000
RNG = np.random.default_rng(20260712)


def main() -> None:
    df = pd.read_csv(PRED)
    groups = [g for _, g in df.groupby("event_id", sort=False)]
    receiver_mae = np.empty(BOOTSTRAPS)
    receiver_rmse = np.empty(BOOTSTRAPS)
    event_mae = np.empty(BOOTSTRAPS)
    event_rmse = np.empty(BOOTSTRAPS)
    for b in range(BOOTSTRAPS):
        selected = RNG.integers(0, len(groups), len(groups))
        generic_errors, capability_errors = [], []
        generic_totals, capability_totals = [], []
        for idx in selected:
            g = groups[idx]
            actual = g["delta_creation_load"].to_numpy()
            ge = g["generic_prediction"].to_numpy() - actual
            ce = g["capability_prediction"].to_numpy() - actual
            generic_errors.append(ge); capability_errors.append(ce)
            generic_totals.append(ge.sum()); capability_totals.append(ce.sum())
        ge = np.concatenate(generic_errors); ce = np.concatenate(capability_errors)
        gt = np.asarray(generic_totals); ct = np.asarray(capability_totals)
        receiver_mae[b] = np.abs(ge).mean() - np.abs(ce).mean()
        receiver_rmse[b] = np.sqrt(np.mean(ge ** 2)) - np.sqrt(np.mean(ce ** 2))
        event_mae[b] = np.abs(gt).mean() - np.abs(ct).mean()
        event_rmse[b] = np.sqrt(np.mean(gt ** 2)) - np.sqrt(np.mean(ct ** 2))

    def summary(values: np.ndarray) -> dict:
        return {
            "improvement_capability_over_generic": float(values.mean()),
            "cluster_bootstrap_95ci": [float(x) for x in np.quantile(values, [0.025, 0.975])],
            "probability_improvement_positive": float(np.mean(values > 0)),
        }
    result = {
        "events": len(groups), "rows": len(df), "bootstrap_replicates": BOOTSTRAPS,
        "receiver_mae": summary(receiver_mae), "receiver_rmse": summary(receiver_rmse),
        "event_total_mae": summary(event_mae), "event_total_rmse": summary(event_rmse),
    }
    lines = [
        "# Capability-Response Model Analysis", "",
        "Prior-season tracking capabilities modestly improve individual receiver allocation, but do not improve event-total redistribution.", "",
        "| Comparison | Improvement over generic | 95% event-cluster interval | Probability positive |", "|---|---:|---:|---:|",
    ]
    for label, key in [("Receiver MAE", "receiver_mae"), ("Receiver RMSE", "receiver_rmse"), ("Event-total MAE", "event_total_mae"), ("Event-total RMSE", "event_total_rmse")]:
        x = result[key]
        lines.append(f"| {label} | {x['improvement_capability_over_generic']:.4f} | {x['cluster_bootstrap_95ci'][0]:.4f} to {x['cluster_bootstrap_95ci'][1]:.4f} | {x['probability_improvement_positive']:.3f} |")
    lines += [
        "", "A positive value favors the capability model. The individual-level gain is evidence that basketball traits contain allocation information beyond generic role variables, but its practical size is small. Negative event-total results show that unconstrained row-level fitting does not conserve or correctly calibrate total redistributed burden.", "",
        "The next model should separate burden components and use an event-level allocation structure: predict total redistributed burden first, then predict teammate shares with capability features. This directly respects the team-total versus teammate-allocation distinction revealed here.",
    ]
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "capability_burden_model_robustness.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    (OUT / "capability_burden_model_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
