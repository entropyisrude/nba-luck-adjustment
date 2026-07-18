"""Test nonlinear offense-style by defense-style residual matchup surfaces.

Each style pair is represented by frozen low/middle/high bins. The marginal
model contains offense-bin and defense-bin effects separately. The joint model
adds the four treatment-coded terms needed to saturate the 3x3 table. Therefore
only improvement of joint over marginal is evidence that styles make fights;
generic offense or defense quality cannot earn that label.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import test_contextual_drapm_residuals as base  # noqa: E402

PANEL = ROOT / "derived" / "contextual_causal" / "contextual_drapm_team_games.parquet"
OUT = ROOT / "outputs" / "contextual_causal" / "styles_make_fights_test.json"

STYLE_PAIRS = {
    "rim_pressure_vs_rim_anchor": ("o_rim", "d_rim_anchor"),
    "spacing_vs_defensive_size": ("o_spacing", "d_size"),
    "offensive_glass_vs_defensive_glass": ("o_oreb", "d_dreb"),
    "turnover_proneness_vs_disruption": ("o_turnover", "d_disruption"),
    "foul_pressure_vs_foul_discipline": ("o_foul_pressure", "d_foul_rate"),
}
CONTINUOUS = ["rapm_expectation", "offense_is_home", "missing_offense", "missing_defense"] + base.O_TRAITS + base.D_TRAITS


def weighted_quantile(x: np.ndarray, w: np.ndarray, q: float) -> float:
    order = np.argsort(x)
    x, w = x[order], w[order]
    return float(np.interp(q * w.sum(), np.cumsum(w), x))


def thresholds(train: pd.DataFrame, pairs: dict[str, tuple[str, str]]) -> dict:
    out = {}
    w = train.poss.to_numpy(float)
    for name, (oc, dc) in pairs.items():
        out[name] = {
            "offense": [weighted_quantile(train[oc].to_numpy(float), w, 1 / 3), weighted_quantile(train[oc].to_numpy(float), w, 2 / 3)],
            "defense": [weighted_quantile(train[dc].to_numpy(float), w, 1 / 3), weighted_quantile(train[dc].to_numpy(float), w, 2 / 3)],
        }
    return out


def category(x: np.ndarray, cuts: list[float]) -> np.ndarray:
    return np.digitize(x, cuts).astype(int)  # 0 low, 1 middle, 2 high


def design(train: pd.DataFrame, other: pd.DataFrame, pairs: dict[str, tuple[str, str]], joint: bool) -> tuple[np.ndarray, np.ndarray, list[str], dict]:
    xt, xo = base.standardize(train, other, CONTINUOUS)
    names = list(CONTINUOUS)
    cuts = thresholds(train, pairs)
    at, ao = [xt], [xo]
    for name, (oc, dc) in pairs.items():
        cot = category(train[oc].to_numpy(float), cuts[name]["offense"])
        cod = category(other[oc].to_numpy(float), cuts[name]["offense"])
        cdt = category(train[dc].to_numpy(float), cuts[name]["defense"])
        cdo = category(other[dc].to_numpy(float), cuts[name]["defense"])
        # Middle is the reference category; low and high are explicit.
        omt = np.column_stack([cot == 0, cot == 2]).astype(float)
        omo = np.column_stack([cod == 0, cod == 2]).astype(float)
        dmt = np.column_stack([cdt == 0, cdt == 2]).astype(float)
        dmo = np.column_stack([cdo == 0, cdo == 2]).astype(float)
        at.extend([omt, dmt]); ao.extend([omo, dmo])
        names += [f"{name}:off_low", f"{name}:off_high", f"{name}:def_low", f"{name}:def_high"]
        if joint:
            it = np.einsum("ni,nj->nij", omt, dmt).reshape(len(train), 4)
            io = np.einsum("ni,nj->nij", omo, dmo).reshape(len(other), 4)
            at.append(it); ao.append(io)
            names += [f"{name}:off_{a}_x_def_{b}" for a in ["low", "high"] for b in ["low", "high"]]
    return np.column_stack(at), np.column_stack(ao), names, cuts


def select_alpha(train: pd.DataFrame, validation: pd.DataFrame, pairs: dict[str, tuple[str, str]], joint: bool) -> float:
    xt, xv, _, _ = design(train, validation, pairs, joint)
    scores = []
    for alpha in base.ALPHAS:
        beta = base.ridge(xt, train.target.to_numpy(), train.poss.to_numpy(), alpha)
        pred = base.predict(xv, beta)
        scores.append(np.average((validation.target.to_numpy() - pred) ** 2, weights=validation.poss.to_numpy()))
    return base.ALPHAS[int(np.argmin(scores))]


def fit_spec(tune_train: pd.DataFrame, validation: pd.DataFrame, train: pd.DataFrame, test: pd.DataFrame,
             pairs: dict[str, tuple[str, str]], joint: bool) -> tuple[np.ndarray, dict]:
    alpha = select_alpha(tune_train, validation, pairs, joint)
    xtr, xte, names, cuts = design(train, test, pairs, joint)
    beta = base.ridge(xtr, train.target.to_numpy(), train.poss.to_numpy(), alpha)
    return base.predict(xte, beta), {
        "alpha": alpha, "features": names, "coefficients": [float(v) for v in beta], "final_train_thresholds": cuts,
    }


def run(panel: pd.DataFrame) -> dict:
    # This nonlinear family was specified after the earlier linear-product test.
    # Use 2024-25 only for tuning and 2025-26 only for confirmation.
    tune_train = panel[panel.season_year <= 2023].copy()
    validation = panel[panel.season_year == 2024].copy()
    train = panel[panel.season_year <= 2024].copy()
    test = panel[panel.season_year == 2025].copy()
    out = {"selection_season": 2024, "confirmation_season": 2025, "confirmation_rows": int(len(test)),
           "confirmation_games": int(test.game_id.nunique()), "pair_tests": {}}
    for name, pair in STYLE_PAIRS.items():
        pairs = {name: pair}
        pm, mm = fit_spec(tune_train, validation, train, test, pairs, False)
        pj, mj = fit_spec(tune_train, validation, train, test, pairs, True)
        out["pair_tests"][name] = {
            "marginal_model": mm,
            "joint_3x3_model": mj,
            "joint_vs_marginal": base.cluster_comparison(test, pm, pj),
        }
    pm, mm = fit_spec(tune_train, validation, train, test, STYLE_PAIRS, False)
    pj, mj = fit_spec(tune_train, validation, train, test, STYLE_PAIRS, True)
    out["all_five_joint"] = {"marginal_model": mm, "joint_3x3_model": mj,
                              "joint_vs_marginal": base.cluster_comparison(test, pm, pj)}
    return out


def main() -> None:
    panel = pd.read_parquet(PANEL)
    report = {
        "claim_tier": "predictive nonlinear matchup screen; 2025-26 confirmation",
        "test_definition": "joint 3x3 offense-defense style cells versus separate marginal style bins",
        "style_pairs": STYLE_PAIRS,
        "primary_at_least_8_of_10_frozen_slots": run(panel),
        "all_10_frozen_slots_sensitivity": run(panel[panel.coverage_sensitivity_all10].copy()),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
