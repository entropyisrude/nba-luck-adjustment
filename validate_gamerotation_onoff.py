import argparse
from pathlib import Path

import pandas as pd

import src.ingest as ingest_module
from src.ingest import _load_stats_boxscore
from src.onoff import compute_adjusted_onoff_for_game


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate GameRotation-based on/off on specific games.")
    parser.add_argument("--pbp-dir", required=True, help="Directory containing nbastatsv3_<year>.csv files")
    parser.add_argument(
        "--game",
        action="append",
        required=True,
        help="Game spec as GAME_ID,MM/DD/YYYY. Repeat for multiple games.",
    )
    args = parser.parse_args()

    ingest_module.LOCAL_PBP_DIR = Path(args.pbp_dir)

    any_failed = False
    for raw in args.game:
        try:
            game_id, game_date = [part.strip() for part in raw.split(",", 1)]
        except ValueError:
            print({"game": raw, "error": "expected GAME_ID,MM/DD/YYYY"})
            any_failed = True
            continue

        df, _, _ = compute_adjusted_onoff_for_game(
            game_id=game_id,
            game_date_mmddyyyy=game_date,
            player_state=pd.DataFrame(),
            orb_rate=0.30,
            ppp=1.05,
            use_game_rotation=True,
        )
        if df.empty:
            print({"game_id": game_id, "date": game_date, "error": "no_rows"})
            any_failed = True
            continue

        box = _load_stats_boxscore(game_id.zfill(10))["players"][["PLAYER_ID", "PLAYER_NAME", "PLUS_MINUS", "MIN"]]
        box = box[box["MIN"].notna()].copy()
        merged = df[["player_id", "player_name", "minutes_on", "on_diff_reconstructed"]].merge(
            box,
            left_on="player_id",
            right_on="PLAYER_ID",
            how="left",
        )
        merged["delta"] = merged["on_diff_reconstructed"] - merged["PLUS_MINUS"]
        max_abs = float(merged["delta"].abs().max())
        sum_abs = float(merged["delta"].abs().sum())
        print(
            {
                "game_id": game_id,
                "date": game_date,
                "rows": len(df),
                "max_abs_pm_delta": max_abs,
                "sum_abs_pm_delta": sum_abs,
            }
        )
        worst = merged.loc[merged["delta"].abs().sort_values(ascending=False).index].head(5)
        if not worst.empty:
            print(
                worst[["player_id", "player_name", "PLUS_MINUS", "on_diff_reconstructed", "delta"]].to_string(
                    index=False
                )
            )
        if max_abs > 2.0:
            any_failed = True

    raise SystemExit(1 if any_failed else 0)


if __name__ == "__main__":
    main()
