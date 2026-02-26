import argparse
from pathlib import Path

import pandas as pd
import yaml

from src.adjust import get_player_prior, get_shot_multiplier
from src.ingest import (
    get_boxscore_players,
    get_game_home_away_team_ids,
    get_playbyplay_actions,
    get_starters_by_team,
)
from src.onoff import (
    _classify_area,
    _classify_shot_type,
    _elapsed_game_seconds,
    _sort_actions,
)
from src.state import load_player_state


def _fmt_clock(period, clock):
    if period is None:
        return ""
    return f"Q{period} {clock or ''}".strip()


def _find_player_id(players_df: pd.DataFrame, player_name: str) -> int | None:
    if players_df.empty:
        return None
    target = player_name.strip().lower()
    for _, r in players_df.iterrows():
        name = str(r.get("PLAYER_NAME", "")).strip().lower()
        if name == target:
            return int(r["PLAYER_ID"])
    for _, r in players_df.iterrows():
        name = str(r.get("PLAYER_NAME", "")).strip().lower()
        if target and target in name:
            return int(r["PLAYER_ID"])
    return None


def _expected_make_prob(player_id: int, player_state: pd.DataFrame, area: str, shot_type: str) -> float:
    st = player_state.set_index("player_id")[["A_r", "M_r"]] if not player_state.empty else pd.DataFrame()
    if player_id in st.index:
        A_r = float(st.loc[player_id, "A_r"])
        M_r = float(st.loc[player_id, "M_r"])
    else:
        A_r = 0.0
        M_r = 0.0
    mu_player, kappa_player = get_player_prior(A_r, player_id=player_id)
    p_hat = (M_r + kappa_player * mu_player) / (A_r + kappa_player)
    multiplier = get_shot_multiplier(area, shot_type)
    return max(0.15, min(0.55, p_hat * multiplier))


def _expected_make_breakdown(player_id: int, st: pd.DataFrame, area: str, shot_type: str) -> dict[str, float]:
    if player_id in st.index:
        A_r = float(st.loc[player_id, "A_r"])
        M_r = float(st.loc[player_id, "M_r"])
    else:
        A_r = 0.0
        M_r = 0.0
    mu_player, kappa_player = get_player_prior(A_r, player_id=player_id)
    p_hat = (M_r + kappa_player * mu_player) / (A_r + kappa_player)
    multiplier = get_shot_multiplier(area, shot_type)
    exp_prob = max(0.15, min(0.55, p_hat * multiplier))
    return {
        "A_r": A_r,
        "M_r": M_r,
        "mu": mu_player,
        "kappa": kappa_player,
        "p_hat": p_hat,
        "multiplier": multiplier,
        "exp_prob": exp_prob,
    }


def _print_table(rows, headers):
    if not rows:
        print("(no rows)")
        return
    cols = list(zip(*([headers] + rows)))
    widths = [max(len(str(x)) for x in col) for col in cols]
    def fmt_row(row):
        return " | ".join(str(val).ljust(w) for val, w in zip(row, widths))
    print(fmt_row(headers))
    print("-+-".join("-" * w for w in widths))
    for row in rows:
        print(fmt_row(row))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--game", required=True, help="Game ID (e.g. 0022500831 or 22500831)")
    parser.add_argument("--date", required=True, help="MM/DD/YYYY (ET)")
    parser.add_argument("--player-name", default="", help="Player name (e.g. Andrew Nembhard)")
    parser.add_argument("--player-id", default="", help="Player ID (optional)")
    args = parser.parse_args()

    game_id = str(args.game)
    game_id = game_id.zfill(10) if len(game_id) < 10 else game_id
    game_date_mmddyyyy = args.date

    players_df = get_boxscore_players(game_id, game_date_mmddyyyy)
    if players_df.empty:
        print("No boxscore players found.")
        return

    if args.player_id:
        player_id = int(args.player_id)
    else:
        player_id = _find_player_id(players_df, args.player_name)
    if player_id is None:
        print("Player not found.")
        return

    player_row = players_df.loc[players_df["PLAYER_ID"] == player_id].iloc[0]
    player_name = player_row.get("PLAYER_NAME", "")
    player_team_id = int(player_row.get("TEAM_ID", 0))

    with open("config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    orb_rate = float(cfg["orb_rate"])
    ppp = float(cfg["ppp"])
    haircut = orb_rate * ppp
    adj_factor = 3.0 - haircut

    player_state = load_player_state(Path("data/player_state.csv"))
    st = player_state.set_index("player_id")[["A_r", "M_r"]] if not player_state.empty else pd.DataFrame()

    actions = _sort_actions(get_playbyplay_actions(game_id, game_date_mmddyyyy))
    if not actions:
        print("No play-by-play actions found.")
        return

    home_id, away_id = get_game_home_away_team_ids(game_id, game_date_mmddyyyy)
    starters = get_starters_by_team(game_id, game_date_mmddyyyy)
    lineups = {
        home_id: set(starters.get(home_id, [])),
        away_id: set(starters.get(away_id, [])),
    }

    def _is_on_court(pid: int) -> bool:
        return pid in lineups.get(player_team_id, set())

    stint_rows = []
    shot_rows = []
    on_score_rows = []
    agg = {
        "on_team_delta": 0.0,
        "on_opp_delta": 0.0,
        "off_team_delta": 0.0,
        "off_opp_delta": 0.0,
        "on_team_3pa": 0,
        "on_opp_3pa": 0,
        "off_team_3pa": 0,
        "off_opp_3pa": 0,
    }

    prev_elapsed = 0
    prev_home = 0
    prev_away = 0
    on_pts_for = 0.0
    on_pts_against = 0.0
    on_pts_for_adj = 0.0
    on_pts_against_adj = 0.0
    total_seconds_on = 0

    on_court = _is_on_court(player_id)

    for i, action in enumerate(actions):
        period = action.get("period")
        clock = action.get("clock")
        elapsed = _elapsed_game_seconds(period, clock)
        if elapsed is not None and elapsed >= prev_elapsed:
            delta_t = elapsed - prev_elapsed
            if delta_t > 0 and on_court:
                total_seconds_on += delta_t
            prev_elapsed = elapsed

        action_type = (action.get("actionType") or "").lower()
        if action_type == "substitution":
            team_id = action.get("teamId")
            if team_id is None:
                continue
            team_id = int(team_id)
            batch = []
            j = i
            while j < len(actions):
                a = actions[j]
                if (a.get("actionType") or "").lower() != "substitution":
                    break
                if a.get("teamId") != team_id or a.get("period") != period or a.get("clock") != clock:
                    break
                batch.append(a)
                j += 1

            # Do not clear lineup for startperiod batches; apply only explicit
            # in/out substitutions to avoid incomplete period-start snapshots.

            outs = [b for b in batch if (b.get("subType") or "").lower() == "out"]
            ins = [b for b in batch if (b.get("subType") or "").lower() == "in"]
            for b in outs:
                pid = b.get("personId")
                if pid is not None:
                    lineups[team_id].discard(int(pid))
            for b in ins:
                pid = b.get("personId")
                if pid is not None:
                    lineups[team_id].add(int(pid))

            now_on = _is_on_court(player_id)
            if on_court and not now_on:
                stint_rows.append([_fmt_clock(period, clock), "OFF"])
                on_court = False
            elif (not on_court) and now_on:
                stint_rows.append([_fmt_clock(period, clock), "ON"])
                on_court = True
            continue

        score_home = action.get("scoreHome")
        score_away = action.get("scoreAway")
        new_home = prev_home
        new_away = prev_away
        try:
            if score_home is not None:
                new_home = int(score_home)
            if score_away is not None:
                new_away = int(score_away)
        except Exception:
            pass

        delta_home = new_home - prev_home
        delta_away = new_away - prev_away
        if delta_home != 0 or delta_away != 0:
            on = _is_on_court(player_id)
            if on:
                if player_team_id == home_id:
                    on_pts_for += delta_home
                    on_pts_against += delta_away
                    on_pts_for_adj += delta_home
                    on_pts_against_adj += delta_away
                else:
                    on_pts_for += delta_away
                    on_pts_against += delta_home
                    on_pts_for_adj += delta_away
                    on_pts_against_adj += delta_home
                on_score_rows.append([
                    _fmt_clock(period, clock),
                    delta_home,
                    delta_away,
                    action.get("description", ""),
                ])
        prev_home, prev_away = new_home, new_away

        if action_type == "3pt":
            team_id = action.get("teamId")
            pid = action.get("personId")
            if team_id is None or pid is None:
                continue
            team_id = int(team_id)
            pid = int(pid)
            shot_type = _classify_shot_type(action)
            area = _classify_area(action)
            b = _expected_make_breakdown(pid, st, area, shot_type)
            exp_prob = b["exp_prob"]
            actual_make = 1 if (action.get("shotResult") or "").lower() == "made" else 0
            adj_delta = (exp_prob - actual_make) * adj_factor
            on = _is_on_court(player_id)
            is_team_shot = (team_id == player_team_id)
            if on and is_team_shot:
                agg["on_team_delta"] += adj_delta
                agg["on_team_3pa"] += 1
            elif on and (not is_team_shot):
                agg["on_opp_delta"] += adj_delta
                agg["on_opp_3pa"] += 1
            elif (not on) and is_team_shot:
                agg["off_team_delta"] += adj_delta
                agg["off_team_3pa"] += 1
            else:
                agg["off_opp_delta"] += adj_delta
                agg["off_opp_3pa"] += 1
            if on:
                if player_team_id == team_id:
                    on_pts_for_adj += adj_delta
                else:
                    on_pts_against_adj += adj_delta
            shot_rows.append([
                _fmt_clock(period, clock),
                str(pid),
                "MADE" if actual_make else "MISS",
                area,
                shot_type,
                f"{b['A_r']:.1f}",
                f"{b['M_r']:.1f}",
                f"{b['mu']:.3f}",
                f"{b['kappa']:.1f}",
                f"{b['p_hat']:.3f}",
                f"{b['multiplier']:.3f}",
                f"{exp_prob:.3f}",
                f"{adj_delta:+.3f}",
                "ON" if on else "OFF",
                action.get("description", ""),
            ])

    print(f"Player: {player_name} (ID {player_id}) Team {player_team_id} Game {game_id}")
    print(f"Total minutes on: {total_seconds_on / 60.0:.2f}")
    print(f"On pts for/against: {on_pts_for:.3f} / {on_pts_against:.3f}")
    print(f"On pts for/against (adj): {on_pts_for_adj:.3f} / {on_pts_against_adj:.3f}")
    print("")
    print("Substitution events for player:")
    _print_table(stint_rows, ["Time", "Event"])
    print("")
    print("Scoring plays while player ON:")
    _print_table(on_score_rows, ["Time", "HomePts", "AwayPts", "Desc"])
    print("")
    print("All 3PT shots (with adjustments; ON/OFF relative to player):")
    _print_table(
        shot_rows,
        [
            "Time", "ShooterId", "Result", "Area", "Type",
            "A_r", "M_r", "mu", "kappa", "p_hat", "Mult", "ExpProb",
            "AdjDelta", "PlayerOn", "Desc",
        ],
    )
    print("")
    print("Adjustment formula:")
    print("  exp_prob = clamp(0.15, 0.55, p_hat * multiplier)")
    print("  p_hat = (M_r + kappa*mu) / (A_r + kappa)")
    print("  adj_delta_points = (exp_prob - actual_make) * (3 - orb_rate*ppp)")
    print(f"  orb_rate*ppp = {haircut:.4f}, so factor = {adj_factor:.4f}")
    print("")
    print("3PT adjustment aggregation (all shooters, split by player ON/OFF):")
    print(f"  ON  team shots: {agg['on_team_3pa']} attempts, total adj delta {agg['on_team_delta']:+.3f}")
    print(f"  ON  opp  shots: {agg['on_opp_3pa']} attempts, total adj delta {agg['on_opp_delta']:+.3f}")
    print(f"  OFF team shots: {agg['off_team_3pa']} attempts, total adj delta {agg['off_team_delta']:+.3f}")
    print(f"  OFF opp  shots: {agg['off_opp_3pa']} attempts, total adj delta {agg['off_opp_delta']:+.3f}")


if __name__ == "__main__":
    main()
