import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

from src.ingest import get_game_ids_for_date, get_player_3pt_df_from_pbp
from src.onoff_boxscore import write_player_daily_boxscore
from src.onoff import compute_adjusted_onoff_for_game
from src.onoff_history import write_player_onoff_history
from src.state import load_player_state, save_player_state, ensure_players_exist
from src.adjust import update_player_state_attempt_decay

DATA_DIR = Path("data")
CONFIG_PATH = Path("config.yaml")


def daterange(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD (ET)")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (ET)")
    parser.add_argument(
        "--recompute-existing",
        action="store_true",
        help="Recompute games even if game_id already exists in adjusted_onoff.csv",
    )
    parser.add_argument(
        "--history-season-start",
        default="2025-10-01",
        help="Season start date for player_onoff_history.csv window (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--history-season-end",
        default="2026-06-30",
        help="Season end date for player_onoff_history.csv window (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--skip-history",
        action="store_true",
        help="Skip rebuilding data/player_onoff_history.csv",
    )
    parser.add_argument(
        "--skip-boxscore",
        action="store_true",
        help="Skip rebuilding data/player_daily_boxscore.csv",
    )
    parser.add_argument(
        "--playoffs",
        action="store_true",
        help="Process playoff games instead of regular season (saves to separate files)",
    )
    parser.add_argument(
        "--season",
        type=str,
        default=None,
        help="Override season detection (e.g., '2019-20' for COVID bubble playoffs)",
    )
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    with open(CONFIG_PATH, "r") as f:
        cfg = yaml.safe_load(f)

    DATA_DIR.mkdir(exist_ok=True)
    season_type = "Playoffs" if args.playoffs else "Regular Season"
    suffix = "_playoffs" if args.playoffs else ""
    out_path = DATA_DIR / f"adjusted_onoff{suffix}.csv"
    stint_path = DATA_DIR / f"stints{suffix}.csv"
    state_path = DATA_DIR / f"player_state{suffix}.csv"

    player_state = load_player_state(state_path)
    if out_path.exists():
        existing = pd.read_csv(out_path, dtype={"game_id": str, "player_id": int})
        existing["game_id"] = existing["game_id"].astype(str).str.lstrip("0")
    else:
        existing = pd.DataFrame()

    processed_games = set(existing["game_id"].unique().tolist()) if not existing.empty else set()
    rows = []
    stint_rows = []

    for d in daterange(start, end):
        game_date_mmddyyyy = d.strftime("%m/%d/%Y")
        game_ids = get_game_ids_for_date(game_date_mmddyyyy, season_type, args.season)

        print("DATE", d.isoformat(), "NBA_DATA_DATE", game_date_mmddyyyy, "GAMES", len(game_ids), f"({season_type})")

        for game_id in game_ids:
            game_id_norm = str(game_id).lstrip("0")
            if (not args.recompute_existing) and game_id_norm in processed_games:
                print("SKIP (already processed)", game_id_norm)
                continue
            try:
                player_df = get_player_3pt_df_from_pbp(game_id, game_date_mmddyyyy)
                if not player_df.empty:
                    player_state = ensure_players_exist(player_state, player_df)

                onoff_df, stint_df = compute_adjusted_onoff_for_game(
                    game_id=game_id,
                    game_date_mmddyyyy=game_date_mmddyyyy,
                    player_state=player_state,
                    orb_rate=float(cfg["orb_rate"]),
                    ppp=float(cfg["ppp"]),
                )
                if onoff_df.empty:
                    print("SKIP (no on/off rows)", game_id)
                else:
                    onoff_df["date"] = d.isoformat()
                    rows.append(onoff_df)
                    if not stint_df.empty:
                        stint_df["date"] = d.isoformat()
                        stint_rows.append(stint_df)

                if not player_df.empty:
                    player_state = update_player_state_attempt_decay(
                        player_df=player_df,
                        player_state=player_state,
                        half_life_3pa=float(cfg["half_life_3pa"]),
                    )

            except Exception as e:
                print("ERROR processing game", game_id, "->", repr(e))
                continue

    if rows:
        new_df = pd.concat(rows, ignore_index=True)
        if not existing.empty:
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        combined = (
            combined.drop_duplicates(subset=["game_id", "player_id"], keep="last")
            .sort_values(["date", "game_id", "team_id", "player_name"])
        )
        combined.to_csv(out_path, index=False)
        print(f"Wrote: {out_path} (rows={len(combined)})")
    else:
        if out_path.exists():
            print("No new on/off rows produced; kept existing adjusted_onoff.csv.")
        else:
            print("No on/off rows produced; adjusted_onoff.csv not created.")

    # Save stint data
    if stint_rows:
        stint_combined = pd.concat(stint_rows, ignore_index=True)
        # Load existing stints and append
        if stint_path.exists():
            existing_stints = pd.read_csv(stint_path, dtype={"game_id": str})
            existing_stints["game_id"] = existing_stints["game_id"].astype(str).str.lstrip("0")
            stint_combined = pd.concat([existing_stints, stint_combined], ignore_index=True)
        stint_combined = stint_combined.drop_duplicates(
            subset=["game_id", "home_p1", "home_p2", "home_p3", "home_p4", "home_p5",
                    "away_p1", "away_p2", "away_p3", "away_p4", "away_p5", "seconds"],
            keep="last"
        ).sort_values(["date", "game_id"])
        stint_combined.to_csv(stint_path, index=False)
        print(f"Wrote: {stint_path} (stints={len(stint_combined)})")

    save_player_state(player_state, state_path)
    print(f"Wrote: {state_path} (players={len(player_state)})")

    if not args.skip_history and out_path.exists():
        history_path = DATA_DIR / f"player_onoff_history{suffix}.csv"
        hist = write_player_onoff_history(
            input_path=out_path,
            output_path=history_path,
            season_start=args.history_season_start,
            season_end=args.history_season_end,
        )
        print(f"Wrote: {history_path} (players={len(hist)})")

    if not args.skip_boxscore and out_path.exists():
        boxscore_path = DATA_DIR / f"player_daily_boxscore{suffix}.csv"
        box = write_player_daily_boxscore(
            input_path=out_path,
            output_path=boxscore_path,
        )
        print(f"Wrote: {boxscore_path} (rows={len(box)})")


if __name__ == "__main__":
    main()
