from __future__ import annotations

from pathlib import Path

import pandas as pd


def build_player_daily_boxscore(onoff_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a per-player per-game daily boxscore view from adjusted_onoff data.
    """
    if onoff_df.empty:
        return pd.DataFrame()

    required = [
        "date",
        "game_id",
        "team_id",
        "player_id",
        "player_name",
        "minutes_on",
        "on_diff",
        "on_diff_adj",
        "on_off_diff",
        "on_off_diff_adj",
    ]
    for c in required:
        if c not in onoff_df.columns:
            raise ValueError(f"Missing required column for boxscore build: {c}")

    df = onoff_df.copy()
    df["plus_minus_actual"] = pd.to_numeric(df["on_diff"], errors="coerce")
    df["plus_minus_adjusted"] = pd.to_numeric(df["on_diff_adj"], errors="coerce")
    df["plus_minus_delta"] = df["plus_minus_adjusted"] - df["plus_minus_actual"]
    df["on_off_actual"] = pd.to_numeric(df["on_off_diff"], errors="coerce")
    df["on_off_adjusted"] = pd.to_numeric(df["on_off_diff_adj"], errors="coerce")
    df["on_off_delta"] = df["on_off_adjusted"] - df["on_off_actual"]

    out_cols = [
        "date",
        "game_id",
        "team_id",
        "player_id",
        "player_name",
        "minutes_on",
        "plus_minus_actual",
        "plus_minus_adjusted",
        "plus_minus_delta",
        "on_off_actual",
        "on_off_adjusted",
        "on_off_delta",
    ]
    out = df[out_cols].copy()

    for c in [
        "minutes_on",
        "plus_minus_actual",
        "plus_minus_adjusted",
        "plus_minus_delta",
        "on_off_actual",
        "on_off_adjusted",
        "on_off_delta",
    ]:
        out[c] = pd.to_numeric(out[c], errors="coerce").round(3)

    return out.sort_values(["date", "game_id", "team_id", "player_name"]).reset_index(drop=True)


def write_player_daily_boxscore(
    input_path: Path,
    output_path: Path,
) -> pd.DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"Missing on/off input file: {input_path}")
    onoff_df = pd.read_csv(input_path, dtype={"game_id": str, "player_id": int})
    box_df = build_player_daily_boxscore(onoff_df)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    box_df.to_csv(output_path, index=False)
    return box_df
