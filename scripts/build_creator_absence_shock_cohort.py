"""Create a higher-credibility creator-absence shock cohort.

This narrows the broad screening universe produced by
``audit_creator_absence_pilot.py``. It still does not prove that an absence was
unexpected at a particular timestamp; that requires a historical status feed.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from audit_creator_absence_pilot import load_games, load_player_games


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "derived" / "contextual_causal" / "creator_absence_candidates.csv"
DERIVED = ROOT / "derived" / "contextual_causal"
OUTPUTS = ROOT / "outputs" / "contextual_causal"

MIN_SEASON_START = 2010
MIN_PRIOR_MINUTES = 28.0
MAX_DAYS_SINCE_APPEARANCE = 7
END_OF_SEASON_EXCLUSION_DAYS = 7


def season_start(season: pd.Series) -> pd.Series:
    return pd.to_numeric(season.astype(str).str.slice(0, 4), errors="coerce")


def build_team_schedule(games: pd.DataFrame) -> pd.DataFrame:
    schedule = games.sort_values(["season", "team_abbr", "date", "game_id"]).copy()
    grouped = schedule.groupby(["season", "team_abbr"], sort=False)
    schedule["previous_team_game_id"] = grouped["game_id"].shift(1)
    schedule["previous_team_game_date"] = grouped["date"].shift(1)
    schedule["days_since_previous_team_game"] = (
        schedule["date"] - schedule["previous_team_game_date"]
    ).dt.days
    season_end = grouped["date"].transform("max")
    schedule["days_until_team_season_end"] = (season_end - schedule["date"]).dt.days
    schedule["is_back_to_back"] = (schedule["days_since_previous_team_game"] == 1).astype(int)
    return schedule


def add_shock_diagnostics(candidates: pd.DataFrame, player_games: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    candidates = candidates.copy()
    candidates["game_id"] = candidates["game_id"].astype(str)
    candidates["date"] = pd.to_datetime(candidates["date"], errors="coerce")
    schedule = build_team_schedule(games)
    schedule_cols = [
        "game_id", "team_abbr", "previous_team_game_id", "previous_team_game_date",
        "days_since_previous_team_game", "days_until_team_season_end", "is_back_to_back",
    ]
    out = candidates.merge(schedule[schedule_cols], on=["game_id", "team_abbr"], how="left")

    appearances = player_games[["game_id", "team_abbr", "player_id", "date", "minutes"]].copy()
    appearances = appearances.rename(columns={"player_id": "absent_player_id"})
    appeared_keys = appearances[["game_id", "team_abbr", "absent_player_id"]].drop_duplicates().assign(appeared=1)
    previous = appeared_keys.rename(
        columns={"game_id": "previous_team_game_id", "appeared": "played_previous_team_game"}
    )
    out = out.merge(
        previous,
        on=["previous_team_game_id", "team_abbr", "absent_player_id"],
        how="left",
    )
    out["played_previous_team_game"] = out["played_previous_team_game"].fillna(0).astype(int)

    # Last appearance date is computed independently as an auditable recency
    # check. Exact target-date matches are impossible because candidates did
    # not play in the target game.
    left = out.sort_values("date").copy()
    right = appearances[["date", "team_abbr", "absent_player_id"]].drop_duplicates().sort_values("date")
    out = pd.merge_asof(
        left,
        right.rename(columns={"date": "last_appearance_date"}),
        left_on="date",
        right_on="last_appearance_date",
        by=["team_abbr", "absent_player_id"],
        direction="backward",
        allow_exact_matches=False,
    )
    out["days_since_last_appearance"] = (out["date"] - out["last_appearance_date"]).dt.days

    out["eligible_modern_data"] = (season_start(out["season"]) >= MIN_SEASON_START).astype(int)
    out["eligible_rotation_role"] = (out["prior_minutes"] >= MIN_PRIOR_MINUTES).astype(int)
    out["eligible_recent_appearance"] = (
        out["days_since_last_appearance"].between(1, MAX_DAYS_SINCE_APPEARANCE, inclusive="both")
    ).astype(int)
    out["eligible_first_missed_game"] = out["played_previous_team_game"]
    out["eligible_not_shutdown_window"] = (
        out["days_until_team_season_end"] > END_OF_SEASON_EXCLUSION_DAYS
    ).astype(int)
    eligibility = [
        "eligible_modern_data", "eligible_rotation_role", "eligible_recent_appearance",
        "eligible_first_missed_game", "eligible_not_shutdown_window",
    ]
    out["strict_shock_eligible"] = out[eligibility].all(axis=1).astype(int)
    out["shock_tier"] = np.select(
        [
            out["strict_shock_eligible"].eq(1) & out["is_back_to_back"].eq(0),
            out["strict_shock_eligible"].eq(1) & out["is_back_to_back"].eq(1),
        ],
        ["strict_non_b2b", "strict_b2b"],
        default="screening_only",
    )
    return out.sort_values(["date", "game_id", "team_abbr", "creator_rank"]).reset_index(drop=True)


def audit_report(enriched: pd.DataFrame) -> dict:
    strict = enriched[enriched["strict_shock_eligible"] == 1]
    attrition = {
        col: int((enriched[col] == 0).sum())
        for col in [
            "eligible_modern_data", "eligible_rotation_role", "eligible_recent_appearance",
            "eligible_first_missed_game", "eligible_not_shutdown_window",
        ]
    }
    return {
        "claim_level": "higher-credibility realized first-game absence shocks; announcement timing remains unverified",
        "input_screening_rows": int(len(enriched)),
        "strict_rows": int(len(strict)),
        "strict_games": int(strict["game_id"].nunique()),
        "strict_players": int(strict["absent_player_id"].nunique()),
        "parameters": {
            "minimum_season_start": MIN_SEASON_START,
            "minimum_prior_minutes": MIN_PRIOR_MINUTES,
            "maximum_days_since_appearance": MAX_DAYS_SINCE_APPEARANCE,
            "end_of_season_exclusion_days": END_OF_SEASON_EXCLUSION_DAYS,
            "requires_immediately_previous_team_game_played": True,
        },
        "failed_filter_counts_nonexclusive": attrition,
        "strict_by_tier": strict.groupby("shock_tier").size().astype(int).to_dict(),
        "strict_by_season": strict.groupby("season").size().astype(int).to_dict(),
        "strict_by_creator_rank": strict.groupby("creator_rank").size().astype(int).to_dict(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if not INPUT.exists():
        raise FileNotFoundError(f"Run scripts/audit_creator_absence_pilot.py --write first: {INPUT}")

    candidates = pd.read_csv(INPUT, dtype={"game_id": str})
    player_games = load_player_games()
    games = load_games(player_games)
    enriched = add_shock_diagnostics(candidates, player_games, games)
    report = audit_report(enriched)
    print(json.dumps(report, indent=2))

    if args.write:
        DERIVED.mkdir(parents=True, exist_ok=True)
        OUTPUTS.mkdir(parents=True, exist_ok=True)
        enriched_path = DERIVED / "creator_absence_candidates_enriched.csv"
        strict_path = DERIVED / "creator_absence_shock_cohort.csv"
        report_path = OUTPUTS / "creator_absence_shock_cohort_audit.json"
        enriched.to_csv(enriched_path, index=False)
        enriched[enriched["strict_shock_eligible"] == 1].to_csv(strict_path, index=False)
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {enriched_path}")
        print(f"Wrote {strict_path}")
        print(f"Wrote {report_path}")


if __name__ == "__main__":
    main()
