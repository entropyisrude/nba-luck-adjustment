import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

from src.ingest import (
    get_game_ids_for_date,
    get_boxscore_player_df,
    get_boxscore_team_df,
    get_game_home_away_team_ids,
)
from src.state import load_player_state, save_player_state, ensure_players_exist
from src.adjust import (
    compute_team_expected_3pm,
    compute_team_adjusted_points,
    update_player_state_attempt_decay,
)

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
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    with open(CONFIG_PATH, "r") as f:
        cfg = yaml.safe_load(f)

    DATA_DIR.mkdir(exist_ok=True)
    adjusted_path = DATA_DIR / "adjusted_games.csv"
    state_path = DATA_DIR / "player_state.csv"

    player_state = load_player_state(state_path)
    existing = pd.read_csv(adjusted_path) if adjusted_path.exists() else pd.DataFrame()

    rows = []

    for d in daterange(start, end):
        game_date_mmddyyyy = d.strftime("%m/%d/%Y")
        game_ids = get_game_ids_for_date(game_date_mmddyyyy)

        print("DATE", d.isoformat(), "NBA_DATA_DATE", game_date_mmddyyyy, "GAMES", len(game_ids))

        for game_id in game_ids:
            try:
                team_df = get_boxscore_team_df(game_id, game_date_mmddyyyy)
                player_df = get_boxscore_player_df(game_id, game_date_mmddyyyy)

                print("GAME", game_id, "team_df_rows", len(team_df), "player_df_rows", len(player_df))

                if team_df.empty or player_df.empty:
                    print("SKIP (empty df)", game_id)
                    continue

                player_state = ensure_players_exist(player_state, player_df)

                exp_by_team = compute_team_expected_3pm(
                    player_df=player_df,
                    player_state=player_state,
                    mu=float(cfg["mu"]),
                    kappa=float(cfg["kappa"]),
                )

                adjusted_by_team = compute_team_adjusted_points(
                    team_df=team_df,
                    exp_3pm_by_team=exp_by_team,
                    orb_rate=float(cfg["orb_rate"]),
                    ppp=float(cfg["ppp"]),
                )

                home_team_id, away_team_id = get_game_home_away_team_ids(game_id, game_date_mmddyyyy)

                home = team_df.loc[team_df["TEAM_ID"] == home_team_id].iloc[0]
                away = team_df.loc[team_df["TEAM_ID"] == away_team_id].iloc[0]

                home_adj = adjusted_by_team[home_team_id]
                away_adj = adjusted_by_team[away_team_id]

                row = {
                    "date": d.isoformat(),
                    "game_id": str(game_id),
                    "home_team": home["TEAM_ABBREVIATION"],
                    "away_team": away["TEAM_ABBREVIATION"],
                    "home_pts_actual": float(home["PTS"]),
                    "away_pts_actual": float(away["PTS"]),
                    "home_3pa": float(home["FG3A"]),
                    "home_3pm_actual": float(home["FG3M"]),
                    "home_3pm_exp": float(exp_by_team[home_team_id]),
                    "away_3pa": float(away["FG3A"]),
                    "away_3pm_actual": float(away["FG3M"]),
                    "away_3pm_exp": float(exp_by_team[away_team_id]),
                    "home_delta_3m": float(home_adj["delta_3m"]),
                    "away_delta_3m": float(away_adj["delta_3m"]),
                    "home_delta_pts_3": float(home_adj["delta_pts_3"]),
                    "away_delta_pts_3": float(away_adj["delta_pts_3"]),
                    "home_orb_corr_pts": float(home_adj["orb_corr_pts"]),
                    "away_orb_corr_pts": float(away_adj["orb_corr_pts"]),
                    "home_pts_adj": float(home_adj["pts_adj"]),
                    "away_pts_adj": float(away_adj["pts_adj"]),
                }
                row["margin_actual"] = row["home_pts_actual"] - row["away_pts_actual"]
                row["margin_adj"] = row["home_pts_adj"] - row["away_pts_adj"]
                row["margin_delta"] = row["margin_adj"] - row["margin_actual"]

                rows.append(row)

                player_state = update_player_state_attempt_decay(
                    player_df=player_df,
                    player_state=player_state,
                    half_life_3pa=float(cfg["half_life_3pa"]),
                )

            except Exception as e:
                print("ERROR processing game", game_id, "->", repr(e))
                continue

    if rows:
        new_df = pd.DataFrame(rows)
        if not existing.empty:
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df

        combined = (
            combined.drop_duplicates(subset=["game_id"], keep="last")
            .sort_values(["date", "game_id"])
        )
        combined.to_csv(adjusted_path, index=False)
        print(f"Wrote: {adjusted_path} (rows={len(combined)})")
    else:
        print("No game rows produced; adjusted_games.csv not created.")

    save_player_state(player_state, state_path)
    print(f"Wrote: {state_path} (players={len(player_state)})")


if __name__ == "__main__":
    main()