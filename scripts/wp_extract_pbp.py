"""
Step 2: Extract win-probability observations from historical playoff PBP.

Files: data/historical_pbp/nbastats_po_YYYY.csv  (1996-2024 playoffs)

Each scored play gives one observation at higher granularity than stints (~125/game).
SCORE format: "visitor_pts - home_pts"  →  score_diff = home - visitor = SCOREMARGIN

Output: data/wp_obs_pbp.parquet  (playoff games only, to complement stint RS data)
"""
from __future__ import annotations
import re
from pathlib import Path
import pandas as pd
import numpy as np

PBP_DIR = Path(__file__).parent.parent / "data" / "historical_pbp"
OUT     = Path(__file__).parent.parent / "data" / "wp_obs_pbp.parquet"

REGULATION_SECS  = 2880   # 48 min
PERIOD_SECS      = 720    # 12 min per regulation quarter
OT_PERIOD_SECS   = 300    # 5 min per OT

def era_label(yr: int) -> str:
    if yr < 2004: return "1997-2003"
    if yr < 2011: return "2004-2010"
    if yr < 2018: return "2011-2017"
    return "2018-2025"

def season_label(yr: int) -> str:
    return f"{yr}-{str(yr+1)[2:]}"

def pctimestring_to_elapsed(pct: str, period: int) -> float | None:
    """Convert 'MM:SS' time-remaining-in-period to total elapsed seconds."""
    try:
        parts = str(pct).strip().split(":")
        mins, secs = int(parts[0]), float(parts[1])
        time_left_in_period = mins * 60 + secs
        if period <= 4:
            period_start = (period - 1) * PERIOD_SECS
            elapsed_in_period = PERIOD_SECS - time_left_in_period
        else:
            period_start = REGULATION_SECS + (period - 5) * OT_PERIOD_SECS
            elapsed_in_period = OT_PERIOD_SECS - time_left_in_period
        return period_start + elapsed_in_period
    except Exception:
        return None

def parse_score(score_str: str) -> tuple[int, int] | None:
    """'visitor - home' → (visitor_pts, home_pts)"""
    try:
        parts = str(score_str).split(" - ")
        return int(parts[0]), int(parts[1])
    except Exception:
        return None

def process_file(path: Path, season_yr: int) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)

    # Keep only rows with a score update
    df = df[df["SCORE"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    # Parse score
    parsed = df["SCORE"].apply(parse_score)
    df["visitor_pts"] = parsed.apply(lambda x: x[0] if x else np.nan)
    df["home_pts"]    = parsed.apply(lambda x: x[1] if x else np.nan)
    df = df[df["visitor_pts"].notna()].copy()

    # score_diff = home - visitor (positive = home leading)
    df["score_diff"] = df["home_pts"] - df["visitor_pts"]

    # Elapsed time
    df["elapsed"] = df.apply(
        lambda r: pctimestring_to_elapsed(r["PCTIMESTRING"], r["PERIOD"]), axis=1
    )
    df = df[df["elapsed"].notna()].copy()
    df["elapsed"] = df["elapsed"].astype(float)

    # time_remaining (capped to regulation for our model)
    df["time_remaining"] = np.clip(REGULATION_SECS - df["elapsed"], 0, REGULATION_SECS)

    # Final score per game = visitor/home pts at max eventnum
    finals = (df.sort_values("EVENTNUM")
                .groupby("GAME_ID")[["visitor_pts","home_pts"]]
                .last()
                .rename(columns={"visitor_pts":"final_visitor","home_pts":"final_home"})
                .reset_index())

    df = df.merge(finals, on="GAME_ID", how="left")
    df["home_won"] = (df["final_home"] > df["final_visitor"]).astype(int)

    # Drop tied games (shouldn't happen after OT)
    df = df[df["final_home"] != df["final_visitor"]].copy()

    # Only include regulation-time observations
    df = df[df["elapsed"] < REGULATION_SECS].copy()

    season_str = season_label(season_yr)
    df["season"]    = season_str
    df["season_yr"] = season_yr
    df["era"]       = era_label(season_yr) if 1997 <= season_yr <= 2024 else None
    df["source"]    = "pbp_playoff"
    df["game_id"]   = df["GAME_ID"].astype(str)

    return df[["game_id","season","season_yr","era","time_remaining",
               "score_diff","home_won","source"]].copy()

def run():
    files = sorted(PBP_DIR.glob("nbastats_po_*.csv"))
    print(f"Found {len(files)} playoff PBP files")

    all_dfs = []
    for f in files:
        m = re.search(r"_po_(\d{4})\.csv$", f.name)
        if not m:
            continue
        yr = int(m.group(1))
        if yr < 1997:
            print(f"  skip {f.name} (pre-1997)")
            continue

        df = process_file(f, yr)
        if df.empty:
            print(f"  {f.name}: empty after filtering")
            continue

        games = df["game_id"].nunique()
        print(f"  {f.name}: {len(df):,} obs from {games} games (era: {df['era'].iloc[0]})")
        all_dfs.append(df)

    if not all_dfs:
        print("No data extracted!")
        return

    out = pd.concat(all_dfs, ignore_index=True)
    out = out[out["era"].notna()].copy()

    print(f"\nTotal PBP observations : {len(out):,}")
    print(f"Unique playoff games   : {out['game_id'].nunique():,}")
    print(f"Season range           : {out['season'].min()} - {out['season'].max()}")
    print(f"Era counts:\n{out['era'].value_counts().sort_index()}")

    out.to_parquet(OUT, index=False)
    print(f"\nSaved to {OUT}  ({OUT.stat().st_size/1e6:.1f} MB)")

if __name__ == "__main__":
    run()
