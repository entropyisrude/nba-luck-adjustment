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
    descriptor = (action.get("descriptor") or "").lower()
    desc = (action.get("description") or "").lower()
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
        return pd.DataFrame()

    starters = get_starters_by_team(game_id, game_date_mmddyyyy)
    home_id, away_id = get_game_home_away_team_ids(game_id, game_date_mmddyyyy)

    lineups: dict[int, set[int]] = {
        home_id: set(starters.get(home_id, [])),
        away_id: set(starters.get(away_id, [])),
    }

    players_df = get_boxscore_players(game_id, game_date_mmddyyyy)
    player_info: dict[int, dict[str, Any]] = {}
    official_plus_minus: dict[int, float] = {}
    for _, r in players_df.iterrows():
        if str(r.get("PLAYED", "0")) != "1":
            continue
        pid = int(r["PLAYER_ID"])
        player_info[pid] = {
            "player_name": r.get("PLAYER_NAME", ""),
            "team_id": int(r.get("TEAM_ID", 0)),
        }
        official_plus_minus[pid] = float(r.get("PLUS_MINUS", 0.0) or 0.0)

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

    i = 0
    while i < len(actions):
        action = actions[i]
        period = action.get("period")
        clock = action.get("clock")
        elapsed = _elapsed_game_seconds(period, clock)
        if elapsed is not None and elapsed >= prev_elapsed:
            delta_t = elapsed - prev_elapsed
            if delta_t > 0:
                for team_id, lineup in lineups.items():
                    for pid in lineup:
                        _ensure_player(pid, team_id)
                        stats[pid].seconds_on += delta_t
            prev_elapsed = elapsed

        action_type = (action.get("actionType") or "").lower()

        # Batch substitutions at the same time/team
        if action_type == "substitution":
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
                if (a.get("actionType") or "").lower() != "substitution":
                    break
                if a.get("teamId") != team_id or a.get("period") != period or a.get("clock") != clock:
                    break
                batch.append(a)
                j += 1

            # Do not reset lineup on period starts. The feed's startperiod batch
            # may be incomplete, so treating it as a full replacement can create
            # transient 1-2 player lineups. Apply only explicit in/out deltas.

            outs = [b for b in batch if (b.get("subType") or "").lower() == "out"]
            ins = [b for b in batch if (b.get("subType") or "").lower() == "in"]

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
                actual_make = 1 if (action.get("shotResult") or "").lower() == "made" else 0
                adj_delta = (exp_prob - actual_make) * adj_factor
                _apply_points(team_id, float(adj_delta), adjusted=True)
                if team_id == home_id:
                    adj_home += float(adj_delta)
                elif team_id == away_id:
                    adj_away += float(adj_delta)

        i += 1

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

    return pd.DataFrame(rows)
