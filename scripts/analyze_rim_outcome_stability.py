"""Measure split-sample and next-season stability of defensive rim outcomes."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "derived" / "defense_causal" / "halfcourt_rim_possessions_2021_22_to_2025_26.parquet"
OUTDIR = ROOT / "outputs" / "defense_causal"
OUT = OUTDIR / "rim_outcome_stability.json"
TABLE = OUTDIR / "rim_team_season_components.csv"

METRICS = [
    "first_chance_rim_frequency_hc6", "first_chance_rim_fg_pct",
    "second_chance_rim_per_oreb_possession", "second_chance_rim_fg_pct",
    "explicit_putback_per_oreb_possession", "unknown_shooting_foul_frequency_hc6",
    "high_confidence_rim_foul_per_rim_attempt",
]


def add_defense_team(df: pd.DataFrame) -> pd.DataFrame:
    teams = df[["game_id", "offense_team_id"]].drop_duplicates()
    counts = teams.groupby("game_id")["offense_team_id"].transform("nunique")
    valid = teams[counts == 2]
    mapping = valid.merge(valid, on="game_id", suffixes=("", "_def"))
    mapping = mapping[mapping["offense_team_id"] != mapping["offense_team_id_def"]]
    mapping = mapping.rename(columns={"offense_team_id_def": "defense_team_id"})
    return df.merge(mapping, on=["game_id", "offense_team_id"], how="inner")


def aggregate(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    out = df.groupby(keys, as_index=False).agg(
        hc6_risk=("at_risk_hc6", "sum"),
        hc6_rim_possessions=("first_chance_rim_hc6", "sum"),
        hc6_rim_attempts=("first_chance_rim_attempts_hc6", "sum"),
        hc6_rim_makes=("first_chance_rim_makes_hc6", "sum"),
        oreb_possessions=("possession_has_offensive_rebound", "sum"),
        second_chance_rim_possessions=("second_chance_rim_attempt", "sum"),
        second_chance_rim_attempts=("second_chance_rim_attempts", "sum"),
        second_chance_rim_makes=("second_chance_rim_makes", "sum"),
        explicit_putback_possessions=("explicit_putback_rim_attempt", "sum"),
        unknown_shooting_fouls_hc6=("location_unknown_shooting_foul_hc6", "sum"),
        high_confidence_rim_fouls=("high_confidence_rim_shooting_foul", "sum"),
    )
    out["first_chance_rim_frequency_hc6"] = out["hc6_rim_possessions"] / out["hc6_risk"].clip(lower=1)
    out["first_chance_rim_fg_pct"] = out["hc6_rim_makes"] / out["hc6_rim_attempts"].clip(lower=1)
    out["second_chance_rim_per_oreb_possession"] = out["second_chance_rim_possessions"] / out["oreb_possessions"].clip(lower=1)
    out["second_chance_rim_fg_pct"] = out["second_chance_rim_makes"] / out["second_chance_rim_attempts"].clip(lower=1)
    out["explicit_putback_per_oreb_possession"] = out["explicit_putback_possessions"] / out["oreb_possessions"].clip(lower=1)
    out["unknown_shooting_foul_frequency_hc6"] = out["unknown_shooting_fouls_hc6"] / out["hc6_risk"].clip(lower=1)
    out["high_confidence_rim_foul_per_rim_attempt"] = out["high_confidence_rim_fouls"] / out["hc6_rim_attempts"].clip(lower=1)
    return out


def correlations(left: pd.DataFrame, right: pd.DataFrame, keys: list[str]) -> dict:
    joined = left.merge(right, on=keys, suffixes=("_a", "_b"))
    result = {}
    for metric in METRICS:
        x, y = joined[f"{metric}_a"], joined[f"{metric}_b"]
        result[metric] = {
            "n": int(len(joined)), "pearson": float(x.corr(y)),
            "spearman": float(x.corr(y, method="spearman")),
        }
    return result


def main() -> None:
    df = pd.read_parquet(INPUT)
    df = df[df["estimated_duration"] >= 0].copy()
    df = add_defense_team(df)
    # Deterministic game split, used only for reliability—not model selection.
    numeric_game = pd.to_numeric(df["game_id"], errors="coerce").fillna(0).astype("int64")
    df["split_half"] = np.where(numeric_game % 2 == 0, "even", "odd")
    team_season = aggregate(df, ["season_year", "defense_team_id"])
    halves = aggregate(df, ["season_year", "defense_team_id", "split_half"])
    even = halves[halves["split_half"] == "even"].drop(columns="split_half")
    odd = halves[halves["split_half"] == "odd"].drop(columns="split_half")
    split = correlations(even, odd, ["season_year", "defense_team_id"])

    current = team_season.copy(); current["next_season_year"] = current["season_year"] + 1
    nxt = team_season.rename(columns={"season_year": "next_season_year"})
    future = correlations(current, nxt, ["next_season_year", "defense_team_id"])
    report = {
        "interpretation": "Team-defense stability is a screening diagnostic for reproducible signal, not player causality.",
        "team_seasons": int(len(team_season)),
        "split_half_same_season": split,
        "next_season": future,
    }
    OUTDIR.mkdir(parents=True, exist_ok=True)
    team_season.to_csv(TABLE, index=False)
    OUT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Wrote {TABLE}")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
