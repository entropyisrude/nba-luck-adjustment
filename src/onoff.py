from __future__ import annotations

from dataclasses import dataclass
import re
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
    def _key(a: dict[str, Any]) -> tuple[int, int, int]:
        elapsed = _elapsed_game_seconds(a.get("period"), a.get("clock"))
        if elapsed is None:
            elapsed = 0
        boundary_rank = 2
        if str(a.get("actionType") or "").lower() == "period":
            desc = str(a.get("description") or "").lower()
            if "end" in desc:
                boundary_rank = 0
            elif "start" in desc:
                boundary_rank = 1
        order = a.get("orderNumber")
        action = a.get("actionNumber")
        return (
            int(elapsed),
            boundary_rank,
            int(order) if order is not None else int(action) if action is not None else 0,
        )
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


def _is_offensive_rebound(action: dict[str, Any]) -> bool | None:
    """
    Determine if a rebound is offensive or defensive.
    Returns True for offensive, False for defensive, None if unclear.
    """
    desc = str(action.get("description") or "").lower()
    # Check for explicit "offensive" or "defensive" keywords
    if "offensive" in desc:
        return True
    if "defensive" in desc:
        return False
    # Check for (Off:X Def:Y) pattern - if Off > 0 at end, it was offensive
    # Actually, the numbers are cumulative. We need to check the last rebound type.
    # A simpler heuristic: if "off:" appears before a non-zero, it's offensive
    import re
    match = re.search(r'\(off:(\d+)\s+def:(\d+)\)', desc)
    if match:
        # This shows cumulative counts, not helpful for single rebound
        # But if only one is non-zero, we can infer
        off_count = int(match.group(1))
        def_count = int(match.group(2))
        # Can't determine from cumulative alone, need context
        pass
    return None


def _is_last_free_throw(action: dict[str, Any]) -> bool:
    """Check if this free throw is the last in a sequence (e.g., '2 of 2')."""
    desc = str(action.get("description") or "")
    import re
    match = re.search(r'(\d+)\s+of\s+(\d+)', desc)
    if match:
        current = int(match.group(1))
        total = int(match.group(2))
        return current == total
    return False


def _get_ft_points(action: dict[str, Any]) -> int:
    """Get points from a free throw (1 if made, 0 if missed)."""
    desc = str(action.get("description") or "").lower()
    if desc.startswith("miss"):
        return 0
    return 1


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
    def elapsed(a: dict[str, Any]) -> int | None:
        period = a.get("period")
        clock = a.get("clock")
        return _elapsed_game_seconds(period, clock)

    def norm_name(name: str | None) -> str:
        return "".join(ch for ch in str(name or "").lower() if ch.isalnum())

    actions_sorted = _sort_actions(actions)
    team_aliases: dict[int, dict[str, int]] = {}
    for a in actions_sorted:
        try:
            team_id = int(a.get("teamId", 0) or 0)
            pid = int(a.get("personId", 0) or 0)
        except Exception:
            continue
        if team_id <= 0 or pid <= 0:
            continue
        aliases = team_aliases.setdefault(team_id, {})
        for raw in [a.get("playerName"), a.get("playerNameI")]:
            text = str(raw or "").strip()
            if not text:
                continue
            key = norm_name(text)
            if key:
                aliases[key] = pid
            parts = [p for p in re.split(r"[\s.]+", text) if p]
            if parts:
                aliases[norm_name(parts[-1])] = pid

    # First try a stronger local-only inference: starters are the players active
    # before a team's first substitution, plus anyone subbed out at that first
    # substitution time. This avoids promoting bench players who touch the ball
    # early while quiet starters have not yet recorded an event.
    first_sub_elapsed: dict[int, int] = {}
    first_subtype_by_player: dict[tuple[int, int], str] = {}
    starters: dict[int, list[int]] = {}
    for a in actions_sorted:
        if str(a.get("actionType") or "").lower() != "substitution":
            continue
        try:
            team_id = int(a.get("teamId", 0) or 0)
            pid = int(a.get("personId", 0) or 0)
        except Exception:
            continue
        e = elapsed(a)
        if team_id > 0 and e is not None and team_id not in first_sub_elapsed:
            first_sub_elapsed[team_id] = e
        sub_type = str(a.get("subType") or "").lower()
        if team_id > 0 and pid > 0 and sub_type in {"in", "out"} and (team_id, pid) not in first_subtype_by_player:
            first_subtype_by_player[(team_id, pid)] = sub_type

    if first_sub_elapsed:
        for (team_id, pid), sub_type in first_subtype_by_player.items():
            if sub_type == "out":
                starters.setdefault(team_id, []).append(pid)
        for a in actions_sorted:
            e = elapsed(a)
            try:
                team_id = int(a.get("teamId", 0) or 0)
                pid = int(a.get("personId", 0) or 0)
            except Exception:
                continue
            if team_id <= 0 or pid <= 0 or e is None:
                continue
            cutoff = first_sub_elapsed.get(team_id)
            if cutoff is None or e > cutoff:
                continue
            if first_subtype_by_player.get((team_id, pid)) == "in":
                continue
            if str(a.get("actionType") or "").lower() == "substitution" and str(a.get("subType") or "").lower() != "out":
                continue
            lst = starters.setdefault(team_id, [])
            if pid not in lst:
                lst.append(pid)
            # Some starters only appear in early descriptions (e.g. as the
            # assister) before recording a primary action row themselves.
            if str(a.get("actionType") or "").lower() != "substitution":
                desc = str(a.get("description") or "")
                aliases = team_aliases.get(team_id, {})
                for token in re.findall(r"[A-Za-z][A-Za-z'.-]+", desc):
                    alias_pid = aliases.get(norm_name(token))
                    if alias_pid and first_subtype_by_player.get((team_id, alias_pid)) != "in" and alias_pid not in lst:
                        lst.append(alias_pid)
        if starters and all(len(v) >= 5 for v in starters.values()):
            return {k: v[:5] for k, v in starters.items()}

    # Fallback: collect players appearing early in the game as a starter proxy.
    starters = {}
    cutoff_seconds = [120, 300, 600]
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
    starters_override: dict[int, list[int]] | None = None,
    period_start_overrides: dict[int, dict[int, list[int]]] | None = None,
    elapsed_lineup_overrides: dict[int, dict[int, list[int]]] | None = None,
) -> pd.DataFrame:
    """
    Compute per-player on/off plus-minus for a single game, with 3PT luck adjustment.
    """
    actions = _sort_actions(get_playbyplay_actions(game_id, game_date_mmddyyyy))
    if not actions:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    home_id, away_id = _infer_home_away_from_actions(actions)
    if home_id == 0 or away_id == 0:
        home_id, away_id = get_game_home_away_team_ids(game_id, game_date_mmddyyyy)

    starters = starters_override or {}
    if home_id not in starters or away_id not in starters or len(starters.get(home_id, [])) < 5 or len(starters.get(away_id, [])) < 5:
        starters = _infer_starters_from_actions(actions)
    if home_id not in starters or away_id not in starters or len(starters.get(home_id, [])) < 5 or len(starters.get(away_id, [])) < 5:
        starters = get_starters_by_team(game_id, game_date_mmddyyyy)

    period_start_overrides = period_start_overrides or {}
    elapsed_lineup_overrides = elapsed_lineup_overrides or {}

    def _normalize_lineup(players: list[int] | tuple[int, ...] | set[int]) -> set[int]:
        lineup: list[int] = []
        for raw in players:
            try:
                pid = int(raw)
            except Exception:
                continue
            if pid > 0 and pid not in lineup:
                lineup.append(pid)
        return set(lineup[:5])

    period1_override = period_start_overrides.get(1, {})
    lineups: dict[int, set[int]] = {
        home_id: _normalize_lineup(period1_override.get(home_id, starters.get(home_id, []))),
        away_id: _normalize_lineup(period1_override.get(away_id, starters.get(away_id, []))),
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

    # Possession tracking for possession-level RAPM
    possessions: list[dict] = []
    possession_team: int | None = None  # Team currently with the ball
    possession_pts: int = 0  # Points scored this possession
    possession_pts_adj: float = 0.0  # Adjusted points this possession
    possession_start_idx: int = 0  # Action index where possession started
    last_shot_team: int | None = None  # Track who took the last shot (for rebound context)

    def _close_possession(ended_by: str) -> None:
        nonlocal possession_team, possession_pts, possession_pts_adj, possession_start_idx
        if possession_team is None:
            return
        if len(lineups.get(home_id, set())) != 5 or len(lineups.get(away_id, set())) != 5:
            # Skip possessions with incomplete lineups
            possession_pts = 0
            possession_pts_adj = 0.0
            return
        offense_team = possession_team
        defense_team = away_id if offense_team == home_id else home_id
        possessions.append({
            "offense_team": offense_team,
            "defense_team": defense_team,
            "offense_lineup": tuple(sorted(lineups[offense_team])),
            "defense_lineup": tuple(sorted(lineups[defense_team])),
            "points": possession_pts,
            "points_adj": possession_pts_adj,
            "ended_by": ended_by,
            "period": last_period,
        })
        possession_pts = 0
        possession_pts_adj = 0.0

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

        # When we have trusted stint boundary lineups, prefer them to any
        # ambiguous local substitution text at that exact elapsed mark.
        if elapsed is not None:
            override = elapsed_lineup_overrides.get(int(elapsed))
            if override:
                home_override = _normalize_lineup(override.get(home_id, []))
                away_override = _normalize_lineup(override.get(away_id, []))
                if len(home_override) == 5 and len(away_override) == 5:
                    if lineups.get(home_id) != home_override or lineups.get(away_id) != away_override:
                        _close_stint()
                        lineups[home_id] = home_override
                        lineups[away_id] = away_override

        action_type = str(action.get("actionType") or "").lower()

        # Batch substitutions at the same time/team
        if action_type == "substitution":
            if elapsed is not None and int(elapsed) in elapsed_lineup_overrides:
                # The trusted stint boundary already defines the on-court players
                # at this timestamp; re-applying the ambiguous local substitution
                # rows can move the lineup away from that known-good state.
                j = i
                team_id = action.get("teamId")
                period = action.get("period")
                clock = action.get("clock")
                while j < len(actions):
                    a = actions[j]
                    if str(a.get("actionType") or "").lower() != "substitution":
                        break
                    if a.get("teamId") != team_id or a.get("period") != period or a.get("clock") != clock:
                        break
                    j += 1
                i = j
                continue
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

            def _resolve_sub_pid(sub_action: dict[str, Any], mode: str) -> int | None:
                pid = sub_action.get("personId")
                try:
                    pid_int = int(pid)
                except Exception:
                    pid_int = 0
                if pid_int > 0:
                    return pid_int
                candidates = sub_action.get("candidatePersonIds") or []
                resolved: list[int] = []
                for raw in candidates:
                    try:
                        cand = int(raw)
                    except Exception:
                        continue
                    if cand <= 0:
                        continue
                    if mode == "out" and cand in lineups[team_id]:
                        resolved.append(cand)
                    elif mode == "in" and cand not in lineups[team_id]:
                        resolved.append(cand)
                if len(resolved) == 1:
                    return resolved[0]
                return None

            for b in outs:
                pid = _resolve_sub_pid(b, "out")
                if pid is None:
                    continue
                lineups[team_id].discard(int(pid))

            for b in ins:
                pid = _resolve_sub_pid(b, "in")
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
        three_pt_adj_delta = 0.0
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
                three_pt_adj_delta = (exp_prob - actual_make) * adj_factor
                _apply_points(team_id, float(three_pt_adj_delta), adjusted=True)
                if team_id == home_id:
                    adj_home += float(three_pt_adj_delta)
                elif team_id == away_id:
                    adj_away += float(three_pt_adj_delta)

        # === POSSESSION TRACKING ===
        action_team_id = action.get("teamId")
        if action_team_id is not None:
            try:
                action_team_id = int(action_team_id)
            except Exception:
                action_team_id = None

        # Jump ball - possession starts
        if action_type == "jump ball" or action_type == "jumpball":
            if action_team_id and action_team_id in (home_id, away_id):
                possession_team = action_team_id
                possession_start_idx = i

        # Period start/end - close any open possession
        elif action_type == "period":
            desc = str(action.get("description") or "").lower()
            if "end" in desc or "start" in desc:
                _close_possession("period")
                possession_team = None
            if "start" in desc:
                try:
                    period_num = int(period or 0)
                except Exception:
                    period_num = 0
                override = period_start_overrides.get(period_num)
                if override:
                    _close_stint()
                    home_override = _normalize_lineup(override.get(home_id, []))
                    away_override = _normalize_lineup(override.get(away_id, []))
                    if len(home_override) == 5 and len(away_override) == 5:
                        lineups[home_id] = home_override
                        lineups[away_id] = away_override

        # Shot attempts (2pt, 3pt, heave)
        elif action_type in ("2pt", "3pt", "heave"):
            shot_result = str(action.get("shotResult") or "").lower()
            if action_team_id:
                last_shot_team = action_team_id
                # If we don't know possession yet, infer from shot
                if possession_team is None:
                    possession_team = action_team_id

            if shot_result == "made":
                # Score the points
                pts = delta_home if action_team_id == home_id else delta_away
                possession_pts += pts
                possession_pts_adj += pts + three_pt_adj_delta
                # Made shot ends possession, other team gets ball
                _close_possession("made_shot")
                possession_team = away_id if action_team_id == home_id else home_id
            # Missed shot - possession continues until rebound

        # Rebounds
        elif action_type == "rebound":
            desc = str(action.get("description") or "").lower()
            is_offensive = "offensive" in desc
            is_defensive = "defensive" in desc

            if is_defensive:
                # Defensive rebound - possession changes
                _close_possession("defensive_rebound")
                if action_team_id:
                    possession_team = action_team_id
            elif is_offensive:
                # Offensive rebound - possession continues with same team
                # Update possession_team if we know it from the rebound
                if action_team_id and action_team_id in (home_id, away_id):
                    possession_team = action_team_id
            else:
                # Can't determine - try to infer from context
                if action_team_id and last_shot_team:
                    if action_team_id == last_shot_team:
                        # Same team got rebound = offensive
                        possession_team = action_team_id
                    else:
                        # Different team = defensive
                        _close_possession("defensive_rebound")
                        possession_team = action_team_id

        # Turnovers
        elif action_type == "turnover":
            _close_possession("turnover")
            # Possession goes to other team
            if action_team_id == home_id:
                possession_team = away_id
            elif action_team_id == away_id:
                possession_team = home_id

        # Free throws
        elif action_type == "free throw" or action_type == "freethrow":
            # Track FT points
            ft_pts = _get_ft_points(action)
            if action_team_id:
                if possession_team is None:
                    possession_team = action_team_id
                possession_pts += ft_pts
                possession_pts_adj += ft_pts

            # If last free throw in sequence, possession changes
            if _is_last_free_throw(action):
                _close_possession("free_throw")
                # Possession goes to other team after last FT
                if action_team_id == home_id:
                    possession_team = away_id
                elif action_team_id == away_id:
                    possession_team = home_id

        # Steals are often logged alongside turnover events. The turnover should
        # terminate the possession; this event should only confirm who has the
        # ball next, not create a second zero-point possession.
        elif action_type == "steal":
            if action_team_id and possession_team is None:
                possession_team = action_team_id

        i += 1

    # Close final stint and possession
    _close_stint()
    _close_possession("end_of_game")

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

    # Build possession DataFrame
    poss_rows = []
    for idx, p in enumerate(possessions):
        off_lineup = p["offense_lineup"]
        def_lineup = p["defense_lineup"]
        poss_rows.append({
            "game_id": str(game_id).lstrip("0"),
            "poss_index": idx,
            "offense_team": p["offense_team"],
            "defense_team": p["defense_team"],
            "off_p1": off_lineup[0] if len(off_lineup) > 0 else None,
            "off_p2": off_lineup[1] if len(off_lineup) > 1 else None,
            "off_p3": off_lineup[2] if len(off_lineup) > 2 else None,
            "off_p4": off_lineup[3] if len(off_lineup) > 3 else None,
            "off_p5": off_lineup[4] if len(off_lineup) > 4 else None,
            "def_p1": def_lineup[0] if len(def_lineup) > 0 else None,
            "def_p2": def_lineup[1] if len(def_lineup) > 1 else None,
            "def_p3": def_lineup[2] if len(def_lineup) > 2 else None,
            "def_p4": def_lineup[3] if len(def_lineup) > 3 else None,
            "def_p5": def_lineup[4] if len(def_lineup) > 4 else None,
            "points": p["points"],
            "points_adj": round(p["points_adj"], 3),
            "ended_by": p["ended_by"],
            "period": p["period"],
        })
    poss_df = pd.DataFrame(poss_rows)

    return player_df, stint_df, poss_df
