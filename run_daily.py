import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

from src.ingest import (
    get_game_ids_for_date,
    get_boxscore_player_df,
    get_boxscore_team_df,
    get_game_home_away_team_ids,
    get_playbyplay_actions,
    get_playbyplay_3pt_shots,
)
from src.state import load_player_state, save_player_state, ensure_players_exist
from src.adjust import (
    compute_team_expected_3pm,
    compute_team_expected_3pm_with_context,
    compute_team_adjusted_points,
    compute_player_deltas,
    compute_player_deltas_with_context,
    get_biggest_swing_player,
    get_top_swing_players,
    update_player_state_attempt_decay,
)
from src.shooting_luck import (game_components, historical_game_totals,
                               load_state as load_shooting_luck_state,
                               save_state as save_shooting_luck_state)

DATA_DIR = Path("data")
CONFIG_PATH = Path("config.yaml")

# Season types to try each day, in order.
# (nba_api season_type, game_type label, update_player_state)
SEASON_TYPES = [
    ("Regular Season", "regular", True),
    ("PlayIn", "playin", False),
    ("Playoffs", "playoff", False),
]


def daterange(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def process_game(game_id, game_date_mmddyyyy, d, game_type_label, player_state,
                 shooting_luck_state, historical_luck, cfg):
    """Process a single game and return a row dict (or None on failure)."""
    team_df = get_boxscore_team_df(game_id, game_date_mmddyyyy)
    player_df = get_boxscore_player_df(game_id, game_date_mmddyyyy)

    print("GAME", game_id, f"[{game_type_label}]", "team_df_rows", len(team_df), "player_df_rows", len(player_df))

    if team_df.empty or player_df.empty:
        print("SKIP (empty df)", game_id)
        return None, None, player_state, shooting_luck_state

    player_state = ensure_players_exist(player_state, player_df)

    shots_df = get_playbyplay_3pt_shots(game_id, game_date_mmddyyyy)

    if not shots_df.empty:
        exp_by_team = compute_team_expected_3pm_with_context(
            shots_df=shots_df,
            player_state=player_state,
        )
        player_deltas = compute_player_deltas_with_context(
            shots_df=shots_df,
            player_state=player_state,
            orb_rate=float(cfg["orb_rate"]),
            ppp=float(cfg["ppp"]),
        )
        print(f"  Using shot context: {len(shots_df)} 3PT attempts")
    else:
        exp_by_team = compute_team_expected_3pm(
            player_df=player_df,
            player_state=player_state,
            mu=float(cfg["mu"]),
            kappa=float(cfg["kappa"]),
        )
        player_deltas = compute_player_deltas(
            player_df=player_df,
            player_state=player_state,
            mu=float(cfg["mu"]),
            kappa=float(cfg["kappa"]),
            orb_rate=float(cfg["orb_rate"]),
            ppp=float(cfg["ppp"]),
        )
        print("  Fallback: no play-by-play, using legacy method")

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

    actions = get_playbyplay_actions(game_id, game_date_mmddyyyy)
    components, extra_player_luck = game_components(
        game_id, actions, home_team_id, away_team_id,
        shooting_luck_state, historical_luck)
    by_pid = {int(p["player_id"]): p for p in player_deltas}
    for pid, extra in extra_player_luck.items():
        rec = by_pid.setdefault(pid, {
            "player_id": pid,
            "player_name": extra.get("player_name", ""),
            "team_id": int(extra.get("team_id", 0)),
            "fg3a": 0, "fg3m": 0, "exp_3pm": 0.0, "delta_pts": 0.0,
        })
        rec["delta_pts"] += float(extra.get("ft_luck", 0.0)) + 0.5 * float(extra.get("mr_luck", 0.0))
    player_deltas = list(by_pid.values())
    biggest_swing = get_biggest_swing_player(player_deltas)
    top_swing_players = get_top_swing_players(player_deltas, threshold=2.0)

    row = {
        "date": d.isoformat(),
        "game_id": str(game_id).lstrip('0'),
        "game_type": game_type_label,
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
        "home_pts_adj_3pt": float(home_adj["pts_adj"]),
        "away_pts_adj_3pt": float(away_adj["pts_adj"]),
        "home_ft_luck": float(components["ft_luck_home"]),
        "away_ft_luck": float(components["ft_luck_away"]),
        "home_mr_luck": float(components["mr_luck_home"]),
        "away_mr_luck": float(components["mr_luck_away"]),
        "home_pts_adj_3pt_ft": float(home_adj["pts_adj"] - components["ft_luck_home"]),
        "away_pts_adj_3pt_ft": float(away_adj["pts_adj"] - components["ft_luck_away"]),
        "home_pts_adj": float(home_adj["pts_adj"] - components["ft_luck_home"] - 0.5 * components["mr_luck_home"]),
        "away_pts_adj": float(away_adj["pts_adj"] - components["ft_luck_away"] - 0.5 * components["mr_luck_away"]),
    }
    row["margin_actual"] = row["home_pts_actual"] - row["away_pts_actual"]
    row["margin_adj"] = row["home_pts_adj"] - row["away_pts_adj"]
    row["margin_delta"] = row["margin_adj"] - row["margin_actual"]

    if biggest_swing:
        row["swing_player"] = biggest_swing["player_name"]
        row["swing_player_delta"] = round(biggest_swing["delta_pts"], 1)
        row["swing_player_fg3m"] = int(biggest_swing["fg3m"])
        row["swing_player_fg3a"] = int(biggest_swing["fg3a"])
    else:
        row["swing_player"] = ""
        row["swing_player_delta"] = 0.0
        row["swing_player_fg3m"] = 0
        row["swing_player_fg3a"] = 0

    top_players_list = []
    for p in top_swing_players:
        team_abbrev = home["TEAM_ABBREVIATION"] if p["team_id"] == home_team_id else away["TEAM_ABBREVIATION"]
        top_players_list.append({
            "name": p["player_name"],
            "team": team_abbrev,
            "delta": round(p["delta_pts"], 1),
            "fg3m": int(p["fg3m"]),
            "fg3a": int(p["fg3a"]),
        })
    row["top_swing_players"] = json.dumps(top_players_list)

    return row, player_df, player_state, shooting_luck_state


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
    shooting_luck_state_path = DATA_DIR / "shooting_luck_state.json"

    player_state = load_player_state(state_path)
    shooting_luck_state = load_shooting_luck_state(shooting_luck_state_path)
    historical_luck = historical_game_totals(DATA_DIR / "rapm_luck_adjust.parquet")
    if adjusted_path.exists():
        existing = pd.read_csv(adjusted_path, dtype={'game_id': str})
        existing['game_id'] = existing['game_id'].astype(str).str.lstrip('0')
    else:
        existing = pd.DataFrame()

    rows = []

    for d in daterange(start, end):
        game_date_mmddyyyy = d.strftime("%m/%d/%Y")

        for nba_season_type, game_type_label, update_state in SEASON_TYPES:
            try:
                game_ids = get_game_ids_for_date(game_date_mmddyyyy, season_type=nba_season_type)
            except Exception as e:
                print(f"WARN: schedule lookup failed [{game_type_label}] {d.isoformat()} -> {repr(e)}")
                continue

            if not game_ids:
                continue

            print(f"DATE {d.isoformat()} [{game_type_label}] GAMES {len(game_ids)}")

            for game_id in game_ids:
                try:
                    row, player_df, player_state, shooting_luck_state = process_game(
                        game_id, game_date_mmddyyyy, d, game_type_label,
                        player_state, shooting_luck_state, historical_luck, cfg
                    )
                    if row is None:
                        continue

                    rows.append(row)

                    # Only update player priors for regular season games
                    if update_state and player_df is not None:
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
    save_shooting_luck_state(shooting_luck_state, shooting_luck_state_path)
    print(f"Wrote: {shooting_luck_state_path}")


if __name__ == "__main__":
    main()
