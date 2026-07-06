"""Anchor per-player-game plus-minus in an on/off CSV to official box scores.

The stint-based reconstruction leaks/misattributes a few points per game
around substitution and period boundaries. The official per-player box +/-
shares our exact game_id/player_id namespace (Kaggle traditional box scores;
Eoin as a secondary source for games newer than the Kaggle dump), so where a
row matches we take:

    on_diff   := official +/-
    off_diff  := official team margin - official +/-

and carry the possession-model luck adjustment over as a delta:

    on_diff_adj  := on_diff  + (on_diff_adj  - old on_diff)
    off_diff_adj := off_diff + (off_diff_adj - old off_diff)

on_off columns are recomputed from the parts. minutes_on is replaced by the
official MIN only when the stint total is clearly wrong (>1.5 min off, e.g.
games whose stint data dropped an OT period). *_reconstructed columns are
kept in lockstep with their base columns when present.

Idempotent: anchoring an already-anchored file is a no-op (the luck delta is
invariant under anchoring), so this is safe to run after every incremental
on/off update. Rows with no official match keep their stint-based values.

Usage:
    python scripts/anchor_onoff_to_official.py data/adjusted_onoff_historical_pbp.csv --game-type regular
    python scripts/anchor_onoff_to_official.py data/adjusted_onoff_playoffs.csv --game-type playoff
"""
from __future__ import annotations

import argparse
import os
import zipfile
from pathlib import Path

import pandas as pd

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
_KAGGLE_CANDIDATES = [
    Path(os.environ["NBA_BOX_ROOT"]) / "kaggle-traditional" / "traditional.csv"
    if os.environ.get("NBA_BOX_ROOT") else None,
    Path(r"C:\Users\Dave\Downloads\nba-boxscore-data\kaggle-traditional\traditional.csv"),
    Path("/mnt/c/users/dave/Downloads/nba-boxscore-data/kaggle-traditional/traditional.csv"),
]
KAGGLE_BOX = next((p for p in _KAGGLE_CANDIDATES if p is not None and p.exists()), None)
EOIN_ZIP = ROOT / "historical-nba-data-and-player-box-scores.zip"

EOIN_GAME_TYPE = {"regular": "Regular Season", "playoff": "Playoffs", "playin": "Play In"}


def _is_lfs_pointer(path: Path) -> bool:
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            return f.readline().startswith("version https://git-lfs")
    except OSError:
        return True


def _parse_minutes(v):
    """Official MIN comes as '38', '38.5', or '38:24'."""
    if pd.isna(v):
        return None
    s = str(v).strip()
    try:
        if ":" in s:
            mm, ss = s.split(":", 1)
            return int(mm) + int(ss) / 60.0
        return float(s)
    except (TypeError, ValueError):
        return None


def load_kaggle(game_type: str) -> pd.DataFrame | None:
    if KAGGLE_BOX is None:
        print("  Kaggle traditional box scores not found -- skipping primary source")
        return None
    kag = pd.read_csv(KAGGLE_BOX, usecols=["gameid", "type", "playerid", "team", "MIN", "PTS", "+/-"],
                      dtype={"gameid": str}, low_memory=False)
    kag = kag[kag["type"].str.lower() == game_type].copy()
    kag["player_id"] = pd.to_numeric(kag["playerid"], errors="coerce")
    kag["pm_off"] = pd.to_numeric(kag["+/-"], errors="coerce")
    kag["min_off"] = kag["MIN"].map(_parse_minutes)
    kag = kag.dropna(subset=["player_id", "pm_off"])
    kag["player_id"] = kag["player_id"].astype(int)

    team_pts = kag.groupby(["gameid", "team"], as_index=False)["PTS"].sum()
    game_pts = team_pts.groupby("gameid")["PTS"].transform("sum")
    team_pts["margin_off"] = 2 * team_pts["PTS"] - game_pts

    kag = kag.merge(team_pts[["gameid", "team", "margin_off"]], on=["gameid", "team"])
    kag = kag[["gameid", "player_id", "pm_off", "margin_off", "min_off"]].rename(columns={"gameid": "game_id"})
    return kag.drop_duplicates(subset=["game_id", "player_id"])


def load_eoin(game_type: str) -> pd.DataFrame | None:
    if not EOIN_ZIP.exists():
        print(f"  {EOIN_ZIP.name} not found -- skipping secondary source")
        return None
    with zipfile.ZipFile(EOIN_ZIP) as z:
        with z.open("PlayerStatistics.csv") as f:
            eo = pd.read_csv(f, usecols=["gameId", "gameDateTimeEst", "gameType", "personId",
                                         "points", "plusMinusPoints", "numMinutes",
                                         "playerteamCity", "playerteamName"],
                             low_memory=False)
    eo = eo[eo["gameType"] == EOIN_GAME_TYPE[game_type]].copy()
    eo["date"] = pd.to_datetime(eo["gameDateTimeEst"], errors="coerce").dt.strftime("%Y-%m-%d")
    eo["player_id"] = pd.to_numeric(eo["personId"], errors="coerce")
    eo["pm_eoin"] = pd.to_numeric(eo["plusMinusPoints"], errors="coerce")
    eo["min_eoin"] = pd.to_numeric(eo["numMinutes"], errors="coerce")
    eo = eo.dropna(subset=["player_id", "pm_eoin"])
    eo["player_id"] = eo["player_id"].astype(int)
    eo["team_key"] = eo["playerteamCity"].fillna("") + "|" + eo["playerteamName"].fillna("")
    tp = eo.groupby(["gameId", "team_key"], as_index=False)["points"].sum()
    gp = tp.groupby("gameId")["points"].transform("sum")
    tp["margin_eoin"] = 2 * tp["points"] - gp
    eo = eo.merge(tp[["gameId", "team_key", "margin_eoin"]], on=["gameId", "team_key"])
    return eo[["date", "player_id", "pm_eoin", "margin_eoin", "min_eoin"]].drop_duplicates(
        subset=["date", "player_id"])


def anchor(path: Path, game_type: str) -> None:
    if _is_lfs_pointer(path):
        print(f"{path} is an LFS pointer -- skipping (pull it first)")
        return
    print(f"Loading {path}...")
    df = pd.read_csv(path, dtype={"game_id": str}, low_memory=False)
    n = len(df)

    kag = load_kaggle(game_type)
    if kag is not None:
        df = df.merge(kag, on=["game_id", "player_id"], how="left")
    else:
        df["pm_off"] = pd.NA
        df["margin_off"] = pd.NA
        df["min_off"] = pd.NA

    if df["pm_off"].isna().any():
        eo = load_eoin(game_type)
        if eo is not None:
            df["date_str"] = df["date"].astype(str).str[:10]
            df = df.merge(eo, left_on=["date_str", "player_id"],
                          right_on=["date", "player_id"], how="left", suffixes=("", "_eo"))
            fill = df["pm_off"].isna() & df["pm_eoin"].notna()
            df.loc[fill, "pm_off"] = df.loc[fill, "pm_eoin"]
            df.loc[fill, "margin_off"] = df.loc[fill, "margin_eoin"]
            df.loc[fill, "min_off"] = df.loc[fill, "min_eoin"]
            df = df.drop(columns=["pm_eoin", "margin_eoin", "min_eoin", "date_str", "date_eo"],
                         errors="ignore")
            print(f"  Eoin secondary anchor: {int(fill.sum())} rows")

    hit = df["pm_off"].notna()
    print(f"  anchored {int(hit.sum())}/{n} rows to official box plus-minus")

    has_recon = "on_diff_reconstructed" in df.columns
    base_on = df["on_diff_reconstructed"] if has_recon else df["on_diff"]
    base_off = df["off_diff_reconstructed"] if has_recon else df["off_diff"]
    luck_on = df["on_diff_adj"] - base_on
    luck_off = df["off_diff_adj"] - base_off

    df.loc[hit, "on_diff"] = df.loc[hit, "pm_off"]
    df.loc[hit, "off_diff"] = df.loc[hit, "margin_off"] - df.loc[hit, "pm_off"]
    df.loc[hit, "on_diff_adj"] = df.loc[hit, "on_diff"] + luck_on[hit]
    df.loc[hit, "off_diff_adj"] = df.loc[hit, "off_diff"] + luck_off[hit]
    df.loc[hit, "on_off_diff"] = df.loc[hit, "on_diff"] - df.loc[hit, "off_diff"]
    df.loc[hit, "on_off_diff_adj"] = df.loc[hit, "on_diff_adj"] - df.loc[hit, "off_diff_adj"]
    if has_recon:
        df.loc[hit, "on_diff_reconstructed"] = df.loc[hit, "on_diff"]
        df.loc[hit, "off_diff_reconstructed"] = df.loc[hit, "off_diff"]
        df.loc[hit, "on_off_diff_reconstructed"] = df.loc[hit, "on_off_diff"]

    fix_min = hit & df["min_off"].notna() & ((df["minutes_on"] - df["min_off"]).abs() > 1.5)
    if fix_min.any():
        print(f"  minutes repaired from official box: {int(fix_min.sum())} rows")
        df.loc[fix_min, "minutes_on"] = df.loc[fix_min, "min_off"]

    df = df.drop(columns=["pm_off", "margin_off", "min_off"])
    df.to_csv(path, index=False)
    print(f"Wrote {path} ({len(df)} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path, help="on/off CSV to anchor in place")
    parser.add_argument("--game-type", choices=sorted(EOIN_GAME_TYPE), required=True)
    args = parser.parse_args()
    anchor(args.csv, args.game_type)


if __name__ == "__main__":
    main()
