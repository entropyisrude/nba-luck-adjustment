from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.adjust import get_player_prior, get_shot_multiplier
from src.ingest import (
    get_boxscore_players,
    get_game_home_away_team_ids,
    get_playbyplay_actions,
    get_starters_by_team,
)


@dataclass
class PlayerOnOff:
    player_id: int
    player_name: str
    team_id: int
    on_pts_for: float = 0.0
    on_pts_against: float = 0.0
    on_pts_for_adj: float = 0.0
    on_pts_against_adj: float = 0.0
    seconds_on: int = 0


def _period_length_seconds(period: int) -> int:
    return 12 * 60 if period <= 4 else 5 * 60


def _parse_clock_seconds(clock_str: str | None) -> int | None:
    if not clock_str:
        return None
    # clock like "PT11M46.00S"
    try:
        s = clock_str.replace("PT", "").replace("S", "")
        if "M" in s:
            mm, ss = s.split("M")
            minutes = int(mm)
            seconds = float(ss)
        else:
            minutes = 0
            seconds = float(s)
        return int(round(minutes * 60 + seconds))
    except Exception:
        return None


def _elapsed_game_seconds(period: int | None, clock_str: str | None) -> int | None:
    if period is None:
        return None
    remaining = _parse_clock_seconds(clock_str)
    if remaining is None:
        return None
    elapsed_prev = 0
    if period > 1:
        # regulation periods
        elapsed_prev += min(period - 1, 4) * 12 * 60
        # overtime periods
        if period > 5:
            elapsed_prev += (period - 5) * 5 * 60
    return elapsed_prev + (_period_length_seconds(period) - remaining)


def _sort_actions(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _key(a: dict[str, Any]) -> tuple[int, int]:
        order = a.get("orderNumber")
        action = a.get("actionNumber")
        return (int(order) if order is not None else 0, int(action) if action is not None else 0)
    return sorted(actions, key=_key)


def _classify_shot_type(action: dict[str, Any]) -> str:
    descriptor = str(action.get("descriptor") or "").lower()
    desc = str(action.get("description") or "").lower()
    text = descriptor or desc
    if "step back" in text or "stepback" in text:
        return "stepback"
    if "pullup" in text or "pull-up" in text or "pull up" in text:
        return "pullup"
    if "running" in text or "driving" in text:
        return "running"
    if "fadeaway" in text or "fade away" in text:
        return "fadeaway"
    if "turnaround" in text or "turn around" in text:
        return "turnaround"
    return "catch_shoot"


def _classify_area(action: dict[str, Any]) -> str:
    # Use legacy coordinates if present
    x = action.get("xLegacy")
    y = action.get("yLegacy")
    try:
        if x is not None and y is not None:
            x = float(x)
            y = float(y)
            # Rough corner-3 heuristic in legacy coords
            if abs(x) >= 220 and y <= 100:
                return "corner"
    except Exception:
        pass
    return "above_break"


def _infer_home_away_from_actions(actions: list[dict[str, Any]]) -> tuple[int, int]:
    home_votes: dict[int, int] = {}
    away_votes: dict[int, int] = {}
    prev_home = None
    prev_away = None
    for a in actions:
        try:
            team_id = int(a.get("teamId", 0) or 0)
        except Exception:
            team_id = 0
        if team_id <= 0:
            continue
        try:
            score_home = int(a.get("scoreHome", 0) or 0)
            score_away = int(a.get("scoreAway", 0) or 0)
        except Exception:
            continue
        if prev_home is not None and prev_away is not None:
            if score_home > prev_home and score_away == prev_away:
                home_votes[team_id] = home_votes.get(team_id, 0) + 1
            elif score_away > prev_away and score_home == prev_home:
                away_votes[team_id] = away_votes.get(team_id, 0) + 1
        prev_home, prev_away = score_home, score_away

    if home_votes and away_votes:
        home_id = max(home_votes, key=home_votes.get)
        away_id = max(away_votes, key=away_votes.get)
        if home_id != away_id:
            return int(home_id), int(away_id)
    return 0, 0


def _infer_starters_from_actions(actions: list[dict[str, Any]]) -> dict[int, list[int]]:
    # Collect players appearing early in the game as starter proxy.
    starters: dict[int, list[int]] = {}
    cutoff_seconds = [120, 300, 600]  # widen window if needed

    def elapsed(a: dict[str, Any]) -> int | None:
        period = a.get("period")
        clock = a.get("clock")
        return _elapsed_game_seconds(period, clock)

    actions_sorted = _sort_actions(actions)
    for limit in cutoff_seconds:
        starters = {}
        for a in actions_sorted:
            e = elapsed(a)
            if e is None or e > limit:
                continue
            try:
                team_id = int(a.get("teamId", 0) or 0)
                pid = int(a.get("personId", 0) or 0)
            except Exception:
                continue
            if team_id <= 0 or pid <= 0:
                continue
            lst = starters.setdefault(team_id, [])
            if pid not in lst:
                lst.append(pid)
        if starters and all(len(v) >= 5 for v in starters.values()):
            return {k: v[:5] for k, v in starters.items()}
    return {k: v[:5] for k, v in starters.items()}


def _expected_make_prob(
    player_id: int,
    player_state: pd.DataFrame,
    area: str,
    shot_type: str,
) -> float:
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


def compute_adjusted_onoff_for_game(
    game_id: str,
    game_date_mmddyyyy: str,
    player_state: pd.DataFrame,
    orb_rate: float,
    ppp: float,
) -> pd.DataFrame:
    """
    Compute per-player on/off plus-minus for a single game, with 3PT luck adjustment.
    """
    actions = _sort_actions(get_playbyplay_actions(game_id, game_date_mmddyyyy))
    if not actions:
        return pd.DataFrame(), pd.DataFrame()

    home_id, away_id = _infer_home_away_from_actions(actions)
    if home_id == 0 or away_id == 0:
        home_id, away_id = get_game_home_away_team_ids(game_id, game_date_mmddyyyy)

    starters = _infer_starters_from_actions(actions)
    if home_id not in starters or away_id not in starters or len(starters.get(home_id, [])) < 5 or len(starters.get(away_id, [])) < 5:
        starters = get_starters_by_team(game_id, game_date_mmddyyyy)

    lineups: dict[int, set[int]] = {
        home_id: set(starters.get(home_id, [])),
        away_id: set(starters.get(away_id, [])),
    }

    # Build player info from actions to avoid boxscore dependency.
    player_info: dict[int, dict[str, Any]] = {}
    official_plus_minus: dict[int, float] = {}
    for a in actions:
        try:
            pid = int(a.get("personId", 0) or 0)
            team_id = int(a.get("teamId", 0) or 0)
        except Exception:
            continue
        if pid <= 0 or team_id <= 0:
            continue
        if pid not in player_info:
            name = str(a.get("playerName") or a.get("playerNameI") or "")
            player_info[pid] = {"player_name": name, "team_id": team_id}

    stats: dict[int, PlayerOnOff] = {}
    for pid, info in player_info.items():
        stats[pid] = PlayerOnOff(
            player_id=pid,
            player_name=str(info.get("player_name", "")),
            team_id=int(info.get("team_id", 0)),
        )

    def _ensure_player(pid: int, team_id: int) -> None:
        if pid not in stats:
            stats[pid] = PlayerOnOff(
                player_id=pid,
                player_name=str(player_info.get(pid, {}).get("player_name", "")),
                team_id=int(team_id),
            )

    def _apply_points(team_id: int, points: float, adjusted: bool) -> None:
        if points == 0:
            return
        if team_id not in lineups:
            return
        opp_id = away_id if team_id == home_id else home_id
        for pid in lineups.get(team_id, set()):
            _ensure_player(pid, team_id)
            if adjusted:
                stats[pid].on_pts_for_adj += points
            else:
                stats[pid].on_pts_for += points
        for pid in lineups.get(opp_id, set()):
            _ensure_player(pid, opp_id)
            if adjusted:
                stats[pid].on_pts_against_adj += points
            else:
                stats[pid].on_pts_against += points

    prev_elapsed = 0
    prev_home = 0
    prev_away = 0
    adj_home = 0.0
    adj_away = 0.0

    haircut = orb_rate * ppp
    # Match game-level ORB correction:
    # delta_pts = (exp_3m - actual_3m) * (3 - orb_rate*ppp)
    adj_factor = 3.0 - haircut

    # Stint tracking for RAPM
    stints: list[dict] = []
    stint_start_elapsed = 0
    stint_start_home = 0
    stint_start_away = 0
    stint_start_home_adj = 0.0
    stint_start_away_adj = 0.0
    stint_start_period = 1
    stint_start_clock = "PT12M00.00S"
    last_period = 1
    last_clock = "PT12M00.00S"

    def _close_stint() -> None:
        nonlocal stint_start_elapsed, stint_start_home, stint_start_away
        nonlocal stint_start_home_adj, stint_start_away_adj
        nonlocal stint_start_period, stint_start_clock
        duration = prev_elapsed - stint_start_elapsed
        if duration > 0 and len(lineups.get(home_id, set())) == 5 and len(lineups.get(away_id, set())) == 5:
            stints.append({
                "home_lineup": tuple(sorted(lineups[home_id])),
                "away_lineup": tuple(sorted(lineups[away_id])),
                "seconds": duration,
                "home_pts": prev_home - stint_start_home,
                "away_pts": prev_away - stint_start_away,
                "home_pts_adj": adj_home - stint_start_home_adj,
                "away_pts_adj": adj_away - stint_start_away_adj,
                "start_elapsed": stint_start_elapsed,
                "end_elapsed": prev_elapsed,
                "start_period": stint_start_period,
                "start_clock": stint_start_clock,
                "end_period": last_period,
                "end_clock": last_clock,
                "start_home_score": stint_start_home,
                "start_away_score": stint_start_away,
                "end_home_score": prev_home,
                "end_away_score": prev_away,
                "start_home_score_adj": stint_start_home_adj,
                "start_away_score_adj": stint_start_away_adj,
                "end_home_score_adj": adj_home,
                "end_away_score_adj": adj_away,
            })
        stint_start_elapsed = prev_elapsed
        stint_start_home = prev_home
        stint_start_away = prev_away
        stint_start_home_adj = adj_home
        stint_start_away_adj = adj_away
        stint_start_period = last_period
        stint_start_clock = last_clock

    i = 0
    while i < len(actions):
        action = actions[i]
        period = action.get("period")
        clock = action.get("clock")
        elapsed = _elapsed_game_seconds(period, clock)
        if elapsed is not None and elapsed >= prev_elapsed:
            if period is not None:
                try:
                    last_period = int(period)
                except Exception:
                    pass
            if clock:
                last_clock = str(clock)
            delta_t = elapsed - prev_elapsed
            if delta_t > 0:
                for team_id, lineup in lineups.items():
                    for pid in lineup:
                        _ensure_player(pid, team_id)
                        stats[pid].seconds_on += delta_t
            prev_elapsed = elapsed

        action_type = str(action.get("actionType") or "").lower()

        # Batch substitutions at the same time/team
        if action_type == "substitution":
            _close_stint()  # Save stint before lineup changes
            team_id = action.get("teamId")
            if team_id is None:
                i += 1
                continue
            team_id = int(team_id)
            period = action.get("period")
            clock = action.get("clock")
            batch = []
            j = i
            while j < len(actions):
                a = actions[j]
                if str(a.get("actionType") or "").lower() != "substitution":
                    break
                if a.get("teamId") != team_id or a.get("period") != period or a.get("clock") != clock:
                    break
                batch.append(a)
                j += 1

            # Do not reset lineup on period starts. The feed's startperiod batch
            # may be incomplete, so treating it as a full replacement can create
            # transient 1-2 player lineups. Apply only explicit in/out deltas.

            outs = [b for b in batch if str(b.get("subType") or "").lower() == "out"]
            ins = [b for b in batch if str(b.get("subType") or "").lower() == "in"]

            for b in outs:
                pid = b.get("personId")
                if pid is None:
                    continue
                lineups[team_id].discard(int(pid))

            for b in ins:
                pid = b.get("personId")
                if pid is None:
                    continue
                lineups[team_id].add(int(pid))

            i = j
            continue

        # Actual score changes
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
        if delta_home != 0:
            _apply_points(home_id, float(delta_home), adjusted=False)
            _apply_points(home_id, float(delta_home), adjusted=True)
            adj_home += float(delta_home)
        if delta_away != 0:
            _apply_points(away_id, float(delta_away), adjusted=False)
            _apply_points(away_id, float(delta_away), adjusted=True)
            adj_away += float(delta_away)
        prev_home, prev_away = new_home, new_away

        # 3PT adjustment (made or missed)
        if action_type == "3pt":
            team_id = action.get("teamId")
            pid = action.get("personId")
            if team_id is not None and pid is not None:
                team_id = int(team_id)
                pid = int(pid)
                shot_type = _classify_shot_type(action)
                area = _classify_area(action)
                exp_prob = _expected_make_prob(pid, player_state, area, shot_type)
                actual_make = 1 if str(action.get("shotResult") or "").lower() == "made" else 0
                adj_delta = (exp_prob - actual_make) * adj_factor
                _apply_points(team_id, float(adj_delta), adjusted=True)
                if team_id == home_id:
                    adj_home += float(adj_delta)
                elif team_id == away_id:
                    adj_away += float(adj_delta)

        i += 1

    # Close final stint
    _close_stint()

    # Team totals (actual and adjusted)
    team_totals_actual = {home_id: float(prev_home), away_id: float(prev_away)}
    team_totals_adj = {home_id: float(adj_home), away_id: float(adj_away)}

    rows = []
    for pid, p in stats.items():
        team_id = p.team_id
        opp_id = away_id if team_id == home_id else home_id
        team_actual = team_totals_actual.get(team_id, 0.0)
        opp_actual = team_totals_actual.get(opp_id, 0.0)
        team_adj = team_totals_adj.get(team_id, 0.0)
        opp_adj = team_totals_adj.get(opp_id, 0.0)

        off_for = team_actual - p.on_pts_for
        off_against = opp_actual - p.on_pts_against
        off_for_adj = team_adj - p.on_pts_for_adj
        off_against_adj = opp_adj - p.on_pts_against_adj

        on_diff_reconstructed = p.on_pts_for - p.on_pts_against
        off_diff_reconstructed = off_for - off_against
        on_diff_adj = p.on_pts_for_adj - p.on_pts_against_adj
        off_diff_adj = off_for_adj - off_against_adj

        # Use official boxscore plus-minus as authoritative raw on-court differential.
        team_margin_actual = team_actual - opp_actual
        on_diff_official = float(official_plus_minus.get(pid, on_diff_reconstructed))
        off_diff_official = team_margin_actual - on_diff_official

        rows.append({
            "game_id": str(game_id).lstrip("0"),
            "team_id": int(team_id),
            "player_id": int(pid),
            "player_name": p.player_name,
            "on_pts_for": round(p.on_pts_for, 3),
            "on_pts_against": round(p.on_pts_against, 3),
            "on_diff": round(on_diff_official, 3),
            "off_pts_for": round(off_for, 3),
            "off_pts_against": round(off_against, 3),
            "off_diff": round(off_diff_official, 3),
            "on_pts_for_adj": round(p.on_pts_for_adj, 3),
            "on_pts_against_adj": round(p.on_pts_against_adj, 3),
            "on_diff_adj": round(on_diff_adj, 3),
            "off_pts_for_adj": round(off_for_adj, 3),
            "off_pts_against_adj": round(off_against_adj, 3),
            "off_diff_adj": round(off_diff_adj, 3),
            "on_off_diff": round(on_diff_official - off_diff_official, 3),
            "on_off_diff_adj": round(on_diff_adj - off_diff_adj, 3),
            "on_diff_reconstructed": round(on_diff_reconstructed, 3),
            "off_diff_reconstructed": round(off_diff_reconstructed, 3),
            "on_off_diff_reconstructed": round(on_diff_reconstructed - off_diff_reconstructed, 3),
            "minutes_on": round(p.seconds_on / 60.0, 2),
        })

    player_df = pd.DataFrame(rows)

    # Build stint DataFrame
    stint_rows = []
    for idx, s in enumerate(stints):
        home_lineup = s["home_lineup"]
        away_lineup = s["away_lineup"]
        stint_rows.append({
            "game_id": str(game_id).lstrip("0"),
            "stint_index": idx,
            "home_id": home_id,
            "away_id": away_id,
            "home_p1": home_lineup[0] if len(home_lineup) > 0 else None,
            "home_p2": home_lineup[1] if len(home_lineup) > 1 else None,
            "home_p3": home_lineup[2] if len(home_lineup) > 2 else None,
            "home_p4": home_lineup[3] if len(home_lineup) > 3 else None,
            "home_p5": home_lineup[4] if len(home_lineup) > 4 else None,
            "away_p1": away_lineup[0] if len(away_lineup) > 0 else None,
            "away_p2": away_lineup[1] if len(away_lineup) > 1 else None,
            "away_p3": away_lineup[2] if len(away_lineup) > 2 else None,
            "away_p4": away_lineup[3] if len(away_lineup) > 3 else None,
            "away_p5": away_lineup[4] if len(away_lineup) > 4 else None,
            "seconds": s["seconds"],
            "home_pts": round(s["home_pts"], 3),
            "away_pts": round(s["away_pts"], 3),
            "home_pts_adj": round(s["home_pts_adj"], 3),
            "away_pts_adj": round(s["away_pts_adj"], 3),
            "start_elapsed": int(s["start_elapsed"]),
            "end_elapsed": int(s["end_elapsed"]),
            "start_period": int(s["start_period"]),
            "start_clock": str(s["start_clock"]),
            "end_period": int(s["end_period"]),
            "end_clock": str(s["end_clock"]),
            "start_home_score": round(s["start_home_score"], 3),
            "start_away_score": round(s["start_away_score"], 3),
            "end_home_score": round(s["end_home_score"], 3),
            "end_away_score": round(s["end_away_score"], 3),
            "start_home_score_adj": round(s["start_home_score_adj"], 3),
            "start_away_score_adj": round(s["start_away_score_adj"], 3),
            "end_home_score_adj": round(s["end_home_score_adj"], 3),
            "end_away_score_adj": round(s["end_away_score_adj"], 3),
        })
    stint_df = pd.DataFrame(stint_rows)

    return player_df, stint_df
