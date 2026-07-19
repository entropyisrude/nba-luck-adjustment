"""Rebuild the production NERD stack after a shooting-luck definition change.

The component evidence and canonical counted evidence must already exist.
This retrains the possession RAPM target, chronological atomic prior, Bayesian
metric, and covariance-aware Kalman state in dependency order, then promotes
the validated candidate state to the desktop production artifact.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
RUN_NAME = "shooting_luck_100_100_50"


def run(script: str) -> None:
    print(f"\n==> {script}", flush=True)
    subprocess.run([sys.executable, script], cwd=ROOT, check=True)


def main() -> None:
    run("metric/build_rapm_target_poss.py")
    run("metric/evaluate_atomic_denominator_rolling.py")
    run("metric/build_atomic_production_prior.py")
    run("metric/build_metric_v0.py")

    env = os.environ.copy()
    env["MVK_RUN_NAME"] = RUN_NAME
    print("\n==> metric/build_kalman_multivariate.py", flush=True)
    subprocess.run(
        [sys.executable, "metric/build_kalman_multivariate.py"],
        cwd=ROOT,
        env=env,
        check=True,
    )

    candidate = (ROOT / "outputs" / "contextual_causal" /
                 "multivariate_kalman" / RUN_NAME /
                 "multivariate_kalman_states.parquet")
    state = pd.read_parquet(candidate)
    expected = {
        "prior_model": {"atomic_denominator"},
        "evidence_model": {"canonical_counted_possessions_v1"},
        "filter_model": {"multivariate_stint_gaussian_v1"},
    }
    for column, values in expected.items():
        actual = set(state[column].dropna().astype(str))
        if actual != values:
            raise RuntimeError(f"candidate {column}: expected {values}, got {actual}")
    if state.duplicated(["season_year", "player_id"]).any():
        raise RuntimeError("candidate contains duplicate player-seasons")

    production = METRIC_DATA / "kalman" / "kalman_states.parquet"
    production.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(candidate, production)
    print(f"Promoted {len(state):,} covariance-aware states to {production}", flush=True)


if __name__ == "__main__":
    main()
