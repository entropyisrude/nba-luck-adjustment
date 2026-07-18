"""Test whether prior-season basketball capabilities improve burden response."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler


ROOT = Path(__file__).resolve().parents[1]
PANEL = ROOT / "derived" / "contextual_causal" / "burden_transfer_player_event_panel.csv.gz"
TRACKING = Path(r"C:\Users\Dave\contextual_tracking_cache")
DERIVED = ROOT / "derived" / "contextual_causal"
OUTPUTS = ROOT / "outputs" / "contextual_causal"
TARGET = "delta_creation_load"
ALPHAS = [0.1, 1.0, 10.0, 100.0, 1000.0]
SOURCE_SEASONS = ["2021-22", "2022-23", "2023-24", "2024-25"]

BASE = [
    "expected_minutes", "expected_fga", "expected_fta", "expected_fg3a", "expected_ast",
    "expected_tov", "expected_pts", "expected_creation_load", "prior_minutes",
    "prior_creation_p36", "prior_ast_p36", "creator_rank", "is_back_to_back",
    "home_filled", "season_start", "shock_size", "proportional_raw",
]
CAPABILITIES = [
    "drives_p36", "drive_fga_p36", "drive_fta_p36", "drive_passes_p36",
    "drive_ast_p36", "drive_tov_p36", "passes_made_p36", "passes_received_p36",
    "potential_ast_p36", "ast_adjusted_p36", "pull_up_fga_p36", "pull_up_fg3a_p36",
    "catch_shoot_fga_p36", "catch_shoot_fg3a_p36", "drive_fg_pct",
    "pull_up_efg_pct", "catch_shoot_efg_pct", "ast_to_pass_pct_adjusted",
]


def aggregate_source(path: Path, totals: list[str], rates: list[str]) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["PLAYER_ID"] = pd.to_numeric(df["PLAYER_ID"], errors="coerce")
    df = df.dropna(subset=["PLAYER_ID"]).copy()
    df["PLAYER_ID"] = df["PLAYER_ID"].astype(int)
    # The endpoint is normally one row per player. Weighted aggregation keeps
    # the code safe if a traded player appears in more than one team row.
    rows = []
    for player_id, group in df.groupby("PLAYER_ID"):
        row = {"receiver_player_id": player_id, "minutes_tracking": group["MIN"].sum()}
        for col in totals:
            row[col] = pd.to_numeric(group[col], errors="coerce").fillna(0).sum()
        weights = pd.to_numeric(group["MIN"], errors="coerce").fillna(0).to_numpy()
        for col in rates:
            values = pd.to_numeric(group[col], errors="coerce").to_numpy()
            valid = np.isfinite(values) & (weights > 0)
            row[col] = float(np.average(values[valid], weights=weights[valid])) if valid.any() else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def load_capabilities() -> pd.DataFrame:
    frames = []
    for season in SOURCE_SEASONS:
        drives = aggregate_source(
            TRACKING / f"drives_{season}.csv",
            ["DRIVES", "DRIVE_FGA", "DRIVE_FTA", "DRIVE_PASSES", "DRIVE_AST", "DRIVE_TOV"],
            ["DRIVE_FG_PCT"],
        )
        passing = aggregate_source(
            TRACKING / f"passing_{season}.csv",
            ["PASSES_MADE", "PASSES_RECEIVED", "POTENTIAL_AST", "AST_ADJ"],
            ["AST_TO_PASS_PCT_ADJ"],
        ).drop(columns="minutes_tracking")
        pull = aggregate_source(
            TRACKING / f"pullupshot_{season}.csv",
            ["PULL_UP_FGA", "PULL_UP_FG3A"], ["PULL_UP_EFG_PCT"],
        ).drop(columns="minutes_tracking")
        catch = aggregate_source(
            TRACKING / f"catchshoot_{season}.csv",
            ["CATCH_SHOOT_FGA", "CATCH_SHOOT_FG3A"], ["CATCH_SHOOT_EFG_PCT"],
        ).drop(columns="minutes_tracking")
        merged = drives.merge(passing, on="receiver_player_id", how="outer")
        merged = merged.merge(pull, on="receiver_player_id", how="outer").merge(catch, on="receiver_player_id", how="outer")
        minutes = merged["minutes_tracking"].clip(lower=1.0)
        mapping = {
            "DRIVES": "drives_p36", "DRIVE_FGA": "drive_fga_p36", "DRIVE_FTA": "drive_fta_p36",
            "DRIVE_PASSES": "drive_passes_p36", "DRIVE_AST": "drive_ast_p36", "DRIVE_TOV": "drive_tov_p36",
            "PASSES_MADE": "passes_made_p36", "PASSES_RECEIVED": "passes_received_p36",
            "POTENTIAL_AST": "potential_ast_p36", "AST_ADJ": "ast_adjusted_p36",
            "PULL_UP_FGA": "pull_up_fga_p36", "PULL_UP_FG3A": "pull_up_fg3a_p36",
            "CATCH_SHOOT_FGA": "catch_shoot_fga_p36", "CATCH_SHOOT_FG3A": "catch_shoot_fg3a_p36",
        }
        for raw, feature in mapping.items():
            merged[feature] = 36.0 * merged[raw] / minutes
        merged = merged.rename(columns={
            "DRIVE_FG_PCT": "drive_fg_pct", "PULL_UP_EFG_PCT": "pull_up_efg_pct",
            "CATCH_SHOOT_EFG_PCT": "catch_shoot_efg_pct",
            "AST_TO_PASS_PCT_ADJ": "ast_to_pass_pct_adjusted",
        })
        start = int(season[:4])
        merged["season_start"] = start + 1  # prior-season traits predict the following season
        frames.append(merged[["receiver_player_id", "season_start", "minutes_tracking"] + CAPABILITIES])
    return pd.concat(frames, ignore_index=True)


def prepare() -> tuple[pd.DataFrame, dict]:
    df = pd.read_csv(PANEL, dtype={"game_id": str})
    df = df[df["analysis_eligible"] == 1].copy()
    df["season_start"] = pd.to_numeric(df["season"].str[:4], errors="coerce")
    df = df[df["season_start"].between(2022, 2025)].copy()
    df["home_filled"] = pd.to_numeric(df["home"], errors="coerce").fillna(0.5)
    df["shock_size"] = df["prior_creation_p36"] * df["prior_minutes"] / 36.0
    denom = df.groupby("event_id")["expected_creation_load"].transform("sum").clip(lower=1e-9)
    df["proportional_raw"] = df["shock_size"] * df["expected_creation_load"] / denom
    before = len(df)
    df = df.merge(load_capabilities(), on=["receiver_player_id", "season_start"], how="left")
    covered = df["minutes_tracking"].notna() & (df["minutes_tracking"] >= 200)
    coverage = {"modern_rows_before_tracking_filter": before, "rows_with_prior_tracking_200_minutes": int(covered.sum())}
    df = df[covered].copy()
    for col in CAPABILITIES:
        df[col] = df[col].replace([np.inf, -np.inf], np.nan)
        df[col] = df.groupby("season_start")[col].transform(lambda s: s.fillna(s.median()))
        df[col] = df[col].fillna(df[col].median())
    return df.sort_values(["date", "event_id", "receiver_player_id"]), coverage


def metric(y: np.ndarray, pred: np.ndarray) -> dict:
    corr = float(np.corrcoef(y, pred)[0, 1]) if np.std(pred) > 0 else float("nan")
    return {"mae": float(mean_absolute_error(y, pred)), "rmse": float(mean_squared_error(y, pred) ** 0.5), "correlation": corr}


def event_metric(df: pd.DataFrame, pred: np.ndarray) -> dict:
    x = df[["event_id", TARGET]].copy(); x["prediction"] = pred
    x = x.groupby("event_id")[[TARGET, "prediction"]].sum()
    return metric(x[TARGET].to_numpy(), x["prediction"].to_numpy())


def matrices(train: pd.DataFrame, score: pd.DataFrame, capability: bool):
    columns = BASE + CAPABILITIES if capability else BASE
    if capability:
        for col in CAPABILITIES:
            train[f"interaction_{col}"] = train[col] * train["shock_size"]
            score[f"interaction_{col}"] = score[col] * score["shock_size"]
        columns += [f"interaction_{col}" for col in CAPABILITIES]
    scaler = StandardScaler()
    return scaler.fit_transform(train[columns]), scaler.transform(score[columns])


def predict(train: pd.DataFrame, score: pd.DataFrame, alpha: float, capability: bool) -> np.ndarray:
    x, xs = matrices(train.copy(), score.copy(), capability)
    model = Ridge(alpha=alpha)
    model.fit(x, train[TARGET])
    return model.predict(xs)


def tune(train: pd.DataFrame, validation: pd.DataFrame, capability: bool) -> tuple[float, dict]:
    results = {}
    for alpha in ALPHAS:
        p = predict(train, validation, alpha, capability)
        results[str(alpha)] = metric(validation[TARGET].to_numpy(), p)
    best = min(ALPHAS, key=lambda a: results[str(a)]["rmse"])
    return best, results


def main() -> None:
    df, coverage = prepare()
    train = df[df["season_start"] == 2022].copy()
    validation = df[df["season_start"] == 2023].copy()
    test = df[df["season_start"] >= 2024].copy()
    base_alpha, base_validation = tune(train, validation, False)
    cap_alpha, cap_validation = tune(train, validation, True)
    fit = pd.concat([train, validation], ignore_index=True)
    base_pred = predict(fit, test, base_alpha, False)
    cap_pred = predict(fit, test, cap_alpha, True)
    result = {
        "question": "Do prior-season capability vectors improve over generic context on the same rows?",
        "coverage": coverage,
        "split": {"train": "2022-23", "validation": "2023-24", "test": ["2024-25", "2025-26"], "train_rows": len(train), "validation_rows": len(validation), "test_rows": len(test), "test_events": int(test.event_id.nunique())},
        "selected_alpha": {"generic": base_alpha, "capability": cap_alpha},
        "validation": {"generic": base_validation, "capability": cap_validation},
        "test": {
            "generic_context": {"receiver": metric(test[TARGET].to_numpy(), base_pred), "event": event_metric(test, base_pred)},
            "capability_response": {"receiver": metric(test[TARGET].to_numpy(), cap_pred), "event": event_metric(test, cap_pred)},
        },
    }
    prediction = test[["date", "season", "event_id", "receiver_player_id", "receiver_player_name", "absent_player_name", TARGET]].copy()
    prediction["generic_prediction"] = base_pred
    prediction["capability_prediction"] = cap_pred
    DERIVED.mkdir(parents=True, exist_ok=True); OUTPUTS.mkdir(parents=True, exist_ok=True)
    prediction.to_csv(DERIVED / "capability_burden_model_test_predictions.csv", index=False)
    (OUTPUTS / "capability_burden_model_results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
