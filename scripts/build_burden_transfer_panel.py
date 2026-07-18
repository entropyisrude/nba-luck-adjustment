"""Build the receiver-player by creator-absence event analysis panel."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from audit_creator_absence_pilot import load_player_games


ROOT = Path(__file__).resolve().parents[1]
COHORT = ROOT / "derived" / "contextual_causal" / "creator_absence_shock_cohort.csv"
DERIVED = ROOT / "derived" / "contextual_causal"
OUTPUTS = ROOT / "outputs" / "contextual_causal"
LOOKBACK = 10
MIN_PERIODS = 5
STATS = ["minutes", "fga", "fta", "fg3a", "ast", "tov", "pts", "creation_load"]


def add_frozen_receiver_baselines(player_games: pd.DataFrame) -> pd.DataFrame:
    pg = player_games.sort_values(["season", "team_abbr", "player_id", "date", "game_id"]).copy()
    pg["creation_load"] = pg["fga"] + 0.44 * pg["fta"] + 1.5 * pg["ast"] + pg["tov"]
    groups = pg.groupby(["season", "team_abbr", "player_id"], sort=False)
    for stat in STATS:
        pg[f"expected_{stat}"] = groups[stat].transform(
            lambda s: s.shift(1).rolling(LOOKBACK, min_periods=MIN_PERIODS).mean()
        )
    pg["receiver_prior_appearances"] = groups.cumcount()
    return pg


def build_panel(cohort: pd.DataFrame, player_games: pd.DataFrame) -> pd.DataFrame:
    cohort = cohort.copy()
    cohort["game_id"] = cohort["game_id"].astype(str)
    pg = add_frozen_receiver_baselines(player_games)
    actual = pg.rename(
        columns={"player_id": "receiver_player_id", "player_name": "receiver_player_name"}
    )
    event_cols = [
        "date", "season", "game_id", "team_abbr", "opponent", "home",
        "absent_player_id", "absent_player_name", "creator_rank", "prior_minutes",
        "prior_creation_p36", "prior_ast_p36", "shock_tier", "is_back_to_back",
        "days_since_last_appearance", "days_until_team_season_end",
    ]
    panel = cohort[event_cols].merge(actual, on=["date", "season", "game_id", "team_abbr"], how="left")
    panel = panel[panel["receiver_player_id"].notna()].copy()
    panel["receiver_player_id"] = panel["receiver_player_id"].astype(int)
    panel = panel[panel["receiver_player_id"] != panel["absent_player_id"]]
    for stat in STATS:
        panel[f"delta_{stat}"] = panel[stat] - panel[f"expected_{stat}"]
    panel["baseline_available"] = panel[[f"expected_{s}" for s in STATS]].notna().all(axis=1).astype(int)
    panel["receiver_rotation_prior"] = (panel["expected_minutes"] >= 12.0).astype(int)
    panel["analysis_eligible"] = (
        panel["baseline_available"].eq(1) & panel["receiver_rotation_prior"].eq(1)
    ).astype(int)
    panel["event_id"] = (
        panel["game_id"].astype(str) + "_" + panel["team_abbr"].astype(str)
        + "_" + panel["absent_player_id"].astype(str)
    )
    ordered = event_cols + [
        "event_id", "receiver_player_id", "receiver_player_name", "receiver_prior_appearances",
        "baseline_available", "receiver_rotation_prior", "analysis_eligible",
    ]
    for stat in STATS:
        ordered.extend([stat, f"expected_{stat}", f"delta_{stat}"])
    return panel[ordered].sort_values(["date", "event_id", "receiver_player_id"]).reset_index(drop=True)


def report(panel: pd.DataFrame) -> dict:
    eligible = panel[panel["analysis_eligible"] == 1]
    return {
        "panel_rows": int(len(panel)),
        "events": int(panel["event_id"].nunique()),
        "receiver_players": int(panel["receiver_player_id"].nunique()),
        "analysis_eligible_rows": int(len(eligible)),
        "analysis_eligible_events": int(eligible["event_id"].nunique()),
        "events_without_any_eligible_receiver": int(
            panel.groupby("event_id")["analysis_eligible"].max().eq(0).sum()
        ),
        "duplicate_event_receiver_rows": int(panel.duplicated(["event_id", "receiver_player_id"]).sum()),
        "missing_baseline_rows": int(panel["baseline_available"].eq(0).sum()),
        "eligible_delta_summary": {
            f"delta_{stat}": {
                "mean": float(eligible[f"delta_{stat}"].mean()),
                "median": float(eligible[f"delta_{stat}"].median()),
                "std": float(eligible[f"delta_{stat}"].std()),
            }
            for stat in STATS
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if not COHORT.exists():
        raise FileNotFoundError(f"Run build_creator_absence_shock_cohort.py --write first: {COHORT}")
    cohort = pd.read_csv(COHORT, dtype={"game_id": str})
    cohort["date"] = pd.to_datetime(cohort["date"], errors="coerce")
    player_games = load_player_games()
    panel = build_panel(cohort, player_games)
    audit = report(panel)
    print(json.dumps(audit, indent=2))
    if args.write:
        DERIVED.mkdir(parents=True, exist_ok=True)
        OUTPUTS.mkdir(parents=True, exist_ok=True)
        panel_path = DERIVED / "burden_transfer_player_event_panel.csv.gz"
        audit_path = OUTPUTS / "burden_transfer_panel_audit.json"
        panel.to_csv(panel_path, index=False, compression="gzip")
        audit_path.write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {panel_path}")
        print(f"Wrote {audit_path}")


if __name__ == "__main__":
    main()
