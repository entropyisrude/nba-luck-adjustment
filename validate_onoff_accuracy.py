"""Validate adjusted_onoff raw fields against official NBA boxscore public data."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.ingest import get_boxscore_players

INPUT_PATH = Path("data/adjusted_onoff.csv")
OUTPUT_PATH = Path("data/onoff_validation.csv")


def _to_mmddyyyy(iso_date: str) -> str:
    d = pd.to_datetime(iso_date, errors="coerce")
    if pd.isna(d):
        return ""
    return d.strftime("%m/%d/%Y")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="", help="YYYY-MM-DD")
    parser.add_argument("--end", default="", help="YYYY-MM-DD")
    parser.add_argument("--max-games", type=int, default=0, help="Optional cap for faster spot checks")
    args = parser.parse_args()

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH, dtype={"game_id": str, "player_id": int})
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")

    if args.start:
        df = df.loc[df["date_dt"] >= pd.to_datetime(args.start)]
    if args.end:
        df = df.loc[df["date_dt"] <= pd.to_datetime(args.end)]
    if df.empty:
        print("No rows to validate after date filters.")
        return

    game_keys = (
        df[["game_id", "date"]]
        .drop_duplicates()
        .sort_values(["date", "game_id"])
        .to_dict("records")
    )
    if args.max_games and args.max_games > 0:
        game_keys = game_keys[: args.max_games]

    checks: list[dict] = []
    for g in game_keys:
        game_id = str(g["game_id"])
        mmddyyyy = _to_mmddyyyy(str(g["date"]))
        if not mmddyyyy:
            continue

        try:
            off = get_boxscore_players(game_id.zfill(10), mmddyyyy)
        except Exception as e:
            print(f"SKIP game {game_id} ({mmddyyyy}) fetch error: {e!r}")
            continue

        ours = df.loc[df["game_id"].astype(str).str.lstrip("0") == game_id.lstrip("0")].copy()
        if ours.empty or off.empty:
            continue

        off = off[["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "PLUS_MINUS", "MINUTES"]].copy()
        off = off.rename(
            columns={
                "PLAYER_ID": "player_id",
                "PLAYER_NAME": "player_name_official",
                "TEAM_ID": "team_id_official",
                "PLUS_MINUS": "plus_minus_official",
                "MINUTES": "minutes_official",
            }
        )

        merged = ours.merge(off, on="player_id", how="left")
        merged["plus_minus_diff"] = pd.to_numeric(merged["on_diff"], errors="coerce") - pd.to_numeric(
            merged["plus_minus_official"], errors="coerce"
        )
        merged["minutes_diff"] = pd.to_numeric(merged["minutes_on"], errors="coerce") - pd.to_numeric(
            merged["minutes_official"], errors="coerce"
        )
        for _, r in merged.iterrows():
            checks.append(
                {
                    "date": r.get("date"),
                    "game_id": str(r.get("game_id")),
                    "team_id": r.get("team_id"),
                    "player_id": r.get("player_id"),
                    "player_name": r.get("player_name"),
                    "on_diff": r.get("on_diff"),
                    "plus_minus_official": r.get("plus_minus_official"),
                    "plus_minus_diff": r.get("plus_minus_diff"),
                    "minutes_on": r.get("minutes_on"),
                    "minutes_official": r.get("minutes_official"),
                    "minutes_diff": r.get("minutes_diff"),
                }
            )

    out = pd.DataFrame(checks)
    if out.empty:
        print("No validation rows produced.")
        return

    for c in ["plus_minus_diff", "minutes_diff"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").round(4)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_PATH, index=False)

    pm_abs = out["plus_minus_diff"].abs()
    min_abs = out["minutes_diff"].abs()
    print(f"Wrote: {OUTPUT_PATH} (rows={len(out)})")
    print(
        "PLUS_MINUS diff: "
        f"mean_abs={pm_abs.mean():.4f}, max_abs={pm_abs.max():.4f}, exact_match_rate={(pm_abs == 0).mean():.2%}"
    )
    print(
        "MINUTES diff: "
        f"mean_abs={min_abs.mean():.4f}, max_abs={min_abs.max():.4f}, <=0.5min_rate={(min_abs <= 0.5).mean():.2%}"
    )


if __name__ == "__main__":
    main()
