"""Fail-closed audit for publishing a NERD model release.

Run after ``scripts/build_nerd_data.py`` and before every site publish.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PAYLOAD = ROOT / "data" / "nerd_seasons.js"
EXPECTED_CONSUMERS = {
    "nerd.html",
    "team-projections.html",
    "player-value.html",
}
EXPECTED_PROVENANCE = {
    "prior_model": "atomic_denominator",
    "evidence_model": "canonical_counted_possessions_v1",
    "filter_model": "multivariate_stint_gaussian_v1",
}


def load_payload() -> tuple[dict, pd.DataFrame]:
    text = PAYLOAD.read_text(encoding="utf-8")
    obj = json.loads(text[text.index("=") + 1:].strip().rstrip(";"))
    return obj, pd.DataFrame(obj["rows"], columns=obj["cols"])


def main() -> None:
    failures: list[str] = []
    state = pd.read_parquet(
        METRIC_DATA / "kalman" / "kalman_states.parquet")
    for col, expected in EXPECTED_PROVENANCE.items():
        actual = set(state[col].dropna().astype(str)) if col in state else set()
        if actual != {expected}:
            failures.append(f"production {col}: expected {expected}, got {actual}")
    if state.duplicated(["season_year", "player_id"]).any():
        failures.append("production state contains duplicate player-seasons")
    numeric = state.select_dtypes(include=[np.number]).to_numpy()
    if not np.isfinite(numeric).all():
        failures.append("production state contains non-finite numeric values")

    obj, rows = load_payload()
    if obj.get("model") != EXPECTED_PROVENANCE["prior_model"]:
        failures.append(f"payload prior model is {obj.get('model')}")
    if obj.get("evidence") != EXPECTED_PROVENANCE["evidence_model"]:
        failures.append(f"payload evidence model is {obj.get('evidence')}")
    if obj.get("projection_model") != EXPECTED_PROVENANCE["filter_model"]:
        failures.append(f"payload projection model is {obj.get('projection_model')}")
    if rows.duplicated(["season", "pid"]).any():
        failures.append("payload contains duplicate player-seasons")
    projected = rows[rows.season == rows.season.max()]
    for col in ("o", "d", "nerd", "sd", "poss"):
        if not np.isfinite(pd.to_numeric(projected[col], errors="coerce")).all():
            failures.append(f"projection column {col} contains missing/non-finite values")

    token = hashlib.sha256(PAYLOAD.read_bytes()).hexdigest()[:12]
    pattern = re.compile(r'data/nerd_seasons\.js(?:\?v=([^"\']+))?')
    consumers: dict[str, list[str | None]] = {}
    for path in ROOT.glob("*.html"):
        matches = pattern.findall(path.read_text(encoding="utf-8"))
        if matches:
            consumers[path.name] = matches
    if set(consumers) != EXPECTED_CONSUMERS:
        failures.append(
            f"payload consumer set: expected {sorted(EXPECTED_CONSUMERS)}, "
            f"got {sorted(consumers)}")
    for name, tokens in consumers.items():
        if tokens != [token]:
            failures.append(
                f"{name} is not bound exactly once to payload version {token}: "
                f"{tokens}")

    if failures:
        raise SystemExit("NERD RELEASE AUDIT FAILED\n- " + "\n- ".join(failures))
    print("NERD RELEASE AUDIT PASSED")
    print(f"  state rows: {len(state):,}")
    print(f"  payload rows: {len(rows):,}; projections: {len(projected):,}")
    print(f"  payload version: {token}")
    print(f"  consumers: {', '.join(sorted(consumers))}")


if __name__ == "__main__":
    main()
