"""
Fetch and cache multi-season NBA shooting data for 3PT expectation model calibration.

Pulls 2015-16 through 2024-25 regular season data. Run once; subsequent runs
use cached CSVs. Use fetch_all(force=True) to refresh.

Data collected:
  - Season totals: GP, MIN, FG3A/M, FTA/M, FT%, age  (LeagueDashPlayerStats)
  - Catch-and-shoot 3PT splits                        (LeagueDashPtStats)
  - Pull-up 3PT splits derived as total minus C&S

Note: LeagueDashPtStats Pullup endpoint returns a malformed response from the
NBA API (resultSet vs resultSets mismatch in nba_api v1.x). Pull-up is
therefore computed as total_3PA - catch_shoot_3PA rather than fetched directly.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

SEASONS = [f"{y}-{str(y + 1)[2:]}" for y in range(2015, 2025)]  # 2015-16 to 2024-25
SLEEP = 0.8  # polite pause between NBA API calls

# Minimum thresholds for a row to be considered usable in calibration
MIN_3PA = 20   # at least 20 three-point attempts in a season
MIN_FTA = 30   # at least 30 free throw attempts (for a stable FT% estimate)


# ---------------------------------------------------------------------------
# Raw fetchers (one season at a time, with CSV caching)
# ---------------------------------------------------------------------------

def _fetch_totals(season: str, force: bool = False) -> pd.DataFrame:
    """Season-level player totals: GP, MIN, 3PA/M, FTA/M, FT%, age."""
    cache = DATA_DIR / f"totals_{season}.csv"
    if cache.exists() and not force:
        return pd.read_csv(cache)

    from nba_api.stats.endpoints import LeagueDashPlayerStats
    time.sleep(SLEEP)
    df = LeagueDashPlayerStats(
        season=season,
        season_type_all_star="Regular Season",
        per_mode_detailed="Totals",
        measure_type_detailed_defense="Base",
        last_n_games=0,
        month=0,
        opponent_team_id=0,
        pace_adjust="N",
        plus_minus="N",
        rank="N",
        period=0,
    ).get_data_frames()[0]

    keep = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION",
            "AGE", "GP", "MIN", "FG3M", "FG3A", "FG3_PCT",
            "FTM", "FTA", "FT_PCT"]
    df = df[[c for c in keep if c in df.columns]].copy()
    df["SEASON"] = season
    df.to_csv(cache, index=False, encoding="utf-8")
    print(f"  totals {season}: {len(df)} players", flush=True)
    return df


def _fetch_catch_shoot(season: str, force: bool = False) -> pd.DataFrame:
    """Catch-and-shoot 3PT splits per player per season."""
    cache = DATA_DIR / f"catch_shoot_{season}.csv"
    if cache.exists() and not force:
        return pd.read_csv(cache)

    from nba_api.stats.endpoints import LeagueDashPtStats
    time.sleep(SLEEP)
    df = LeagueDashPtStats(
        season=season,
        season_type_all_star="Regular Season",
        per_mode_simple="Totals",
        player_or_team="Player",
        pt_measure_type="CatchShoot",
        last_n_games=0,
        month=0,
        opponent_team_id=0,
    ).get_data_frames()[0]

    keep = ["PLAYER_ID", "PLAYER_NAME",
            "CATCH_SHOOT_FG3M", "CATCH_SHOOT_FG3A", "CATCH_SHOOT_FG3_PCT"]
    df = df[[c for c in keep if c in df.columns]].copy()
    df["SEASON"] = season
    df.to_csv(cache, index=False, encoding="utf-8")
    print(f"  catch-shoot {season}: {len(df)} players", flush=True)
    return df


# ---------------------------------------------------------------------------
# Build combined dataset
# ---------------------------------------------------------------------------

def _merge_season(totals: pd.DataFrame, cs: pd.DataFrame) -> pd.DataFrame:
    """Merge totals + catch-shoot, derive pull-up columns."""
    df = totals.merge(
        cs[["PLAYER_ID", "SEASON", "CATCH_SHOOT_FG3M", "CATCH_SHOOT_FG3A", "CATCH_SHOOT_FG3_PCT"]],
        on=["PLAYER_ID", "SEASON"],
        how="left",
    )

    # Fill missing C&S with 0 (player had no tracking data)
    df["CATCH_SHOOT_FG3A"] = df["CATCH_SHOOT_FG3A"].fillna(0)
    df["CATCH_SHOOT_FG3M"] = df["CATCH_SHOOT_FG3M"].fillna(0)

    # Pull-up = total minus catch-and-shoot
    df["PULLUP_FG3A"] = (df["FG3A"] - df["CATCH_SHOOT_FG3A"]).clip(lower=0)
    df["PULLUP_FG3M"] = (df["FG3M"] - df["CATCH_SHOOT_FG3M"]).clip(lower=0)
    df["PULLUP_FG3_PCT"] = df["PULLUP_FG3M"] / df["PULLUP_FG3A"].replace(0, float("nan"))

    # C&S% — recompute from counts to handle any rounding in API
    df["CATCH_SHOOT_FG3_PCT"] = (
        df["CATCH_SHOOT_FG3M"] / df["CATCH_SHOOT_FG3A"].replace(0, float("nan"))
    )

    # Fraction of 3PA that are catch-and-shoot
    df["CS_RATE"] = df["CATCH_SHOOT_FG3A"] / df["FG3A"].replace(0, float("nan"))

    return df


def fetch_all(force: bool = False) -> pd.DataFrame:
    """
    Fetch all seasons, merge, and return a single calibration DataFrame.
    Also writes shooter_model/data/calibration.csv for offline use.
    """
    all_totals, all_cs = [], []

    for season in SEASONS:
        print(f"\nSeason {season}:")
        try:
            all_totals.append(_fetch_totals(season, force))
        except Exception as e:
            print(f"  ERROR totals: {e}", file=sys.stderr)
        try:
            all_cs.append(_fetch_catch_shoot(season, force))
        except Exception as e:
            print(f"  ERROR catch-shoot: {e}", file=sys.stderr)

    if not all_totals:
        raise RuntimeError("No season data fetched — check API connectivity.")

    totals = pd.concat(all_totals, ignore_index=True)
    cs = pd.concat(all_cs, ignore_index=True) if all_cs else pd.DataFrame()

    df = _merge_season(totals, cs) if not cs.empty else totals

    out_path = DATA_DIR / "calibration.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"\nWrote {len(df)} rows to {out_path}")
    return df


# ---------------------------------------------------------------------------
# Validation summary
# ---------------------------------------------------------------------------

def validate(df: pd.DataFrame) -> None:
    """Print a summary of the fetched data to sanity-check coverage."""
    print(f"\n{'='*50}")
    print(f"Total rows: {len(df)}")
    print(f"Seasons:    {sorted(df['SEASON'].unique().tolist())}")
    print(f"Columns:    {list(df.columns)}")

    usable = df[(df["FG3A"] >= MIN_3PA) & (df["FTA"] >= MIN_FTA)]
    print(f"\nRows with {MIN_3PA}+ 3PA and {MIN_FTA}+ FTA (usable for calibration): {len(usable)}")

    print("\nSample FT% -> 3PT% (usable rows, sorted by FT%):")
    sample = (
        usable[["PLAYER_NAME", "SEASON", "FT_PCT", "FG3_PCT",
                "CATCH_SHOOT_FG3_PCT", "FG3A", "FTA"]]
        .dropna(subset=["FT_PCT", "FG3_PCT"])
        .sort_values("FT_PCT")
    )
    print(sample.head(5).to_string(index=False))
    print("...")
    print(sample.tail(5).to_string(index=False))


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--force", action="store_true", help="Re-fetch even if cached")
    args = p.parse_args()

    print("Fetching NBA shooting data for model calibration...")
    df = fetch_all(force=args.force)
    validate(df)
