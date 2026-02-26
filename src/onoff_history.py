from __future__ import annotations

from pathlib import Path

import pandas as pd


def _maybe_filter_date_window(
    df: pd.DataFrame,
    season_start: str | None,
    season_end: str | None,
) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return df
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    if season_start:
        out = out.loc[out["date"] >= pd.to_datetime(season_start)]
    if season_end:
        out = out.loc[out["date"] <= pd.to_datetime(season_end)]
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out


def build_player_onoff_history(
    onoff_df: pd.DataFrame,
    season_start: str | None = None,
    season_end: str | None = None,
) -> pd.DataFrame:
    """
    Build per-player historical aggregates from adjusted_onoff game logs.
    """
    if onoff_df.empty:
        return pd.DataFrame()

    df = _maybe_filter_date_window(onoff_df, season_start, season_end)
    if df.empty:
        return pd.DataFrame()

    required = [
        "player_id",
        "player_name",
        "team_id",
        "game_id",
        "date",
        "minutes_on",
        "on_diff",
        "off_diff",
        "on_off_diff",
        "on_diff_adj",
        "off_diff_adj",
        "on_off_diff_adj",
    ]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Missing required column for history build: {c}")

    # Use last observed team_id in range to avoid duplication for in-season team changes.
    df = df.sort_values(["date", "game_id"])
    latest_team = (
        df.groupby("player_id", as_index=False)
        .tail(1)[["player_id", "team_id"]]
        .rename(columns={"team_id": "latest_team_id"})
    )

    g = df.groupby(["player_id", "player_name"], as_index=False)
    hist = g.agg(
        games=("game_id", "nunique"),
        minutes_on_total=("minutes_on", "sum"),
        minutes_on_avg=("minutes_on", "mean"),
        on_diff_total=("on_diff", "sum"),
        off_diff_total=("off_diff", "sum"),
        on_off_diff_total=("on_off_diff", "sum"),
        on_diff_avg=("on_diff", "mean"),
        off_diff_avg=("off_diff", "mean"),
        on_off_diff_avg=("on_off_diff", "mean"),
        on_diff_adj_total=("on_diff_adj", "sum"),
        off_diff_adj_total=("off_diff_adj", "sum"),
        on_off_diff_adj_total=("on_off_diff_adj", "sum"),
        on_diff_adj_avg=("on_diff_adj", "mean"),
        off_diff_adj_avg=("off_diff_adj", "mean"),
        on_off_diff_adj_avg=("on_off_diff_adj", "mean"),
        first_game_date=("date", "min"),
        last_game_date=("date", "max"),
    )
    hist = hist.merge(latest_team, on="player_id", how="left")

    # Round numeric display columns for stable CSV diffs.
    round_cols = [
        "minutes_on_total",
        "minutes_on_avg",
        "on_diff_total",
        "off_diff_total",
        "on_off_diff_total",
        "on_diff_avg",
        "off_diff_avg",
        "on_off_diff_avg",
        "on_diff_adj_total",
        "off_diff_adj_total",
        "on_off_diff_adj_total",
        "on_diff_adj_avg",
        "off_diff_adj_avg",
        "on_off_diff_adj_avg",
    ]
    for c in round_cols:
        hist[c] = pd.to_numeric(hist[c], errors="coerce").round(3)

    return hist.sort_values(
        ["on_off_diff_adj_avg", "minutes_on_total"],
        ascending=[False, False],
    ).reset_index(drop=True)


def write_player_onoff_history(
    input_path: Path,
    output_path: Path,
    season_start: str | None = None,
    season_end: str | None = None,
) -> pd.DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"Missing on/off input file: {input_path}")
    onoff_df = pd.read_csv(input_path, dtype={"game_id": str, "player_id": int})
    hist_df = build_player_onoff_history(
        onoff_df=onoff_df,
        season_start=season_start,
        season_end=season_end,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    hist_df.to_csv(output_path, index=False)
    return hist_df
