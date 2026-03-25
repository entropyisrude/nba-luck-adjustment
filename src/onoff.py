from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

import pandas as pd

from src.adjust import get_player_prior, get_shot_multiplier
from src.ingest import (
    _description_player_keys,
    _name_aliases,
    _normalize_name,
    get_boxscore_players,
    get_game_home_away_team_ids,
    get_playbyplay_actions,
    get_starters_by_team,
    _load_stats_boxscore,
    _load_stats_gamerotation,
    _load_stats_home_away,
)
from src.shot_priors import action_number_from_action


@dataclass
class PlayerOnOff:
    player_id: int
    player_name: str
    team_id: int
    on_pts_for: float = 0.0
    on_pts_against: float = 0.0
    on_pts_for_adj: float = 0.0
    on_pts_against_adj: float = 0.0
    seconds_on: float = 0.0


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


def _parse_clock_seconds_precise(clock_str: str | None) -> float | None:
    if not clock_str:
        return None
    try:
        s = clock_str.replace("PT", "").replace("S", "")
        if "M" in s:
            mm, ss = s.split("M")
            minutes = int(mm)
            seconds = float(ss)
        else:
            minutes = 0
            seconds = float(s)
        return minutes * 60.0 + seconds
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


def _elapsed_game_seconds_precise(period: int | None, clock_str: str | None) -> float | None:
    if period is None:
        return None
    remaining = _parse_clock_seconds_precise(clock_str)
    if remaining is None:
        return None
    elapsed_prev = 0.0
    if period > 1:
        elapsed_prev += float(min(period - 1, 4) * 12 * 60)
        if period > 5:
            elapsed_prev += float((period - 5) * 5 * 60)
    return elapsed_prev + (float(_period_length_seconds(period)) - remaining)


def _norm_action_type(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _is_free_throw_action(action: dict[str, Any]) -> bool:
    action_type = _norm_action_type(action.get("actionType"))
    sub_type = _norm_action_type(action.get("subType"))
    desc = _norm_action_type(action.get("description"))
    return (
        action_type == "freethrow"
        or sub_type.startswith("freethrow")
        or "freethrow" in desc
    )


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


def _sort_actions_precise(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _key(a: dict[str, Any]) -> tuple[float, int, int]:
        elapsed = _elapsed_game_seconds_precise(a.get("period"), a.get("clock"))
        if elapsed is None:
            elapsed = 0.0
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
            float(elapsed),
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
    ignored_alias_tokens = {"jr", "sr", "ii", "iii", "iv", "v"}

    def elapsed(a: dict[str, Any]) -> int | None:
        period = a.get("period")
        clock = a.get("clock")
        return _elapsed_game_seconds(period, clock)

    def norm_name(name: str | None) -> str:
        return _normalize_name(str(name or ""))

    actions_sorted = _sort_actions(actions)
    team_aliases: dict[int, dict[str, set[int]]] = {}
    for a in actions_sorted:
        try:
            team_id = int(a.get("teamId", 0) or 0)
            pid = int(a.get("personId", 0) or 0)
        except Exception:
            continue
        if team_id <= 0 or pid <= 0:
            continue
        aliases = team_aliases.setdefault(team_id, {})
        for key in _name_aliases(
            str(a.get("playerName") or ""),
            str(a.get("playerNameI") or ""),
            "" if str(a.get("actionType") or "").lower() == "substitution" else str(a.get("description") or ""),
        ):
            if key and key not in ignored_alias_tokens:
                aliases.setdefault(key, set()).add(pid)

    def unique_alias_pid(team_id: int, token: str) -> int | None:
        token_key = norm_name(token)
        if not token_key or token_key in ignored_alias_tokens:
            return None
        candidates = team_aliases.get(team_id, {}).get(token_key, set())
        return next(iter(candidates)) if len(candidates) == 1 else None

    # First try a stronger local-only inference: starters are the players active
    # before a team's first substitution, plus anyone subbed out at that first
    # substitution time. Then use later player-local evidence only to fill a
    # team from 4 to 5, never to replace an already-solved lineup.
    first_sub_elapsed: dict[int, int] = {}
    first_sub_by_player: dict[tuple[int, int], tuple[int, str]] = {}
    first_action_elapsed: dict[tuple[int, int], int] = {}
    starters: dict[int, list[int]] = {}
    for a in actions_sorted:
        try:
            team_id = int(a.get("teamId", 0) or 0)
            pid = int(a.get("personId", 0) or 0)
        except Exception:
            continue
        e = elapsed(a)
        if team_id <= 0 or pid <= 0 or e is None:
            continue
        action_type = str(a.get("actionType") or "").lower()
        if action_type == "substitution":
            if team_id not in first_sub_elapsed:
                first_sub_elapsed[team_id] = e
            sub_type = str(a.get("subType") or "").lower()
            if sub_type in {"in", "out"} and (team_id, pid) not in first_sub_by_player:
                first_sub_by_player[(team_id, pid)] = (e, sub_type)
            continue
        if (team_id, pid) not in first_action_elapsed:
            first_action_elapsed[(team_id, pid)] = e

    if first_sub_elapsed:
        for (team_id, pid), (sub_elapsed, sub_type) in first_sub_by_player.items():
            cutoff = first_sub_elapsed.get(team_id)
            # If a player's first recorded substitution in period 1 is an out
            # row, that is strong starter evidence even if another teammate was
            # subbed first.
            if sub_type == "out" and sub_elapsed < 720:
                lst = starters.setdefault(team_id, [])
                if pid not in lst:
                    lst.append(pid)
            if sub_type == "out" and cutoff is not None and sub_elapsed <= cutoff:
                lst = starters.setdefault(team_id, [])
                if pid not in lst:
                    lst.append(pid)
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
            player_first_sub = first_sub_by_player.get((team_id, pid))
            if player_first_sub and player_first_sub[1] == "in" and player_first_sub[0] <= cutoff:
                continue
            if str(a.get("actionType") or "").lower() == "substitution" and str(a.get("subType") or "").lower() != "out":
                continue
            lst = starters.setdefault(team_id, [])
            if pid not in lst:
                lst.append(pid)
            if str(a.get("actionType") or "").lower() != "substitution":
                desc = str(a.get("description") or "")
                for token in _description_player_keys(desc):
                    alias_pid = unique_alias_pid(team_id, token)
                    alias_first_sub = first_sub_by_player.get((team_id, alias_pid)) if alias_pid else None
                    if (
                        alias_pid
                        and not (alias_first_sub and alias_first_sub[1] == "in" and alias_first_sub[0] <= cutoff)
                        and alias_pid not in lst
                    ):
                        lst.append(alias_pid)

        for team_id, lst in starters.items():
            if len(lst) >= 5:
                continue
            cutoff = first_sub_elapsed.get(team_id)
            if cutoff is None:
                continue
            for (t_id, pid), action_elapsed in sorted(first_action_elapsed.items(), key=lambda item: item[1]):
                if t_id != team_id or pid in lst:
                    continue
                first_sub = first_sub_by_player.get((team_id, pid))
                if first_sub and first_sub[1] == "in" and first_sub[0] < action_elapsed:
                    continue
                if action_elapsed <= cutoff:
                    continue
                lst.append(pid)
                if len(lst) >= 5:
                    break

        solved = {k: v[:5] for k, v in starters.items() if len(v) >= 5}
        if solved:
            return solved

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
            player_first_sub = first_sub_by_player.get((team_id, pid))
            if player_first_sub and player_first_sub[1] == "in" and player_first_sub[0] <= limit:
                continue
            if pid not in lst:
                lst.append(pid)
            if str(a.get("actionType") or "").lower() != "substitution":
                desc = str(a.get("description") or "")
                for token in _description_player_keys(desc):
                    alias_pid = unique_alias_pid(team_id, token)
                    alias_first_sub = first_sub_by_player.get((team_id, alias_pid)) if alias_pid else None
                    if (
                        alias_pid
                        and not (alias_first_sub and alias_first_sub[1] == "in" and alias_first_sub[0] <= limit)
                        and alias_pid not in lst
                    ):
                        lst.append(alias_pid)
        solved = {k: v[:5] for k, v in starters.items() if len(v) >= 5}
        if solved:
            return solved
    return {k: v[:5] for k, v in starters.items() if len(v) >= 5}


def _infer_period_start_overrides(
    actions: list[dict[str, Any]],
    home_id: int,
    away_id: int,
) -> dict[int, dict[int, list[int]]]:
    actions_sorted = _sort_actions(actions)
    overrides: dict[int, dict[int, list[int]]] = {}
    for period in sorted({int(a.get("period", 0) or 0) for a in actions_sorted if int(a.get("period", 0) or 0) > 1}):
        period_map: dict[int, list[int]] = {}
        for team_id in (home_id, away_id):
            period_actions = []
            for a in actions_sorted:
                try:
                    a_period = int(a.get("period", 0) or 0)
                    a_team = int(a.get("teamId", 0) or 0)
                except Exception:
                    continue
                if a_period == period and a_team == team_id:
                    period_actions.append(a)
            if not period_actions:
                continue

            first_sub_event_by_player: dict[int, tuple[int, str]] = {}
            first_non_sub_index_by_player: dict[int, int] = {}
            seen_players: list[int] = []
            early_players: list[int] = []
            first_sub_index: int | None = None
            sub_batches: list[list[dict[str, Any]]] = []
            unresolved_sub_candidates: list[tuple[int, str, list[int]]] = []
            i = 0
            while i < len(period_actions):
                action = period_actions[i]
                action_type = str(action.get("actionType") or "").lower()

                for player_key in ("personId", "playerId"):
                    try:
                        pid = int(action.get(player_key, 0) or 0)
                    except Exception:
                        pid = 0
                    if pid <= 0 or pid in seen_players:
                        continue
                    seen_players.append(pid)
                    if first_sub_index is None or i <= first_sub_index:
                        early_players.append(pid)
                    break

                if action_type != "substitution":
                    try:
                        pid = int(action.get("personId", 0) or 0)
                    except Exception:
                        pid = 0
                    if pid > 0 and pid not in first_non_sub_index_by_player:
                        first_non_sub_index_by_player[pid] = i
                    i += 1
                    continue

                if first_sub_index is None:
                    first_sub_index = i
                clock = action.get("clock")
                batch: list[dict[str, Any]] = []
                j = i
                while j < len(period_actions):
                    other = period_actions[j]
                    if str(other.get("actionType") or "").lower() != "substitution" or other.get("clock") != clock:
                        break
                    batch.append(other)
                    try:
                        pid = int(other.get("personId", 0) or 0)
                    except Exception:
                        pid = 0
                    sub_type = str(other.get("subType") or "").lower()
                    if pid > 0 and sub_type in {"in", "out"}:
                        prev = first_sub_event_by_player.get(pid)
                        if prev is None or i < prev[0]:
                            first_sub_event_by_player[pid] = (i, sub_type)
                    elif pid <= 0 and sub_type in {"in", "out"}:
                        candidates: list[int] = []
                        for raw in other.get("candidatePersonIds") or []:
                            try:
                                cand = int(raw)
                            except Exception:
                                continue
                            if cand > 0 and cand not in candidates:
                                candidates.append(cand)
                        if candidates:
                            unresolved_sub_candidates.append((i, sub_type, candidates))
                    j += 1
                sub_batches.append(batch)
                i = j

            earliest_candidate_in_index_by_player: dict[int, int] = {}
            for sub_index, sub_type, candidates in unresolved_sub_candidates:
                if sub_type != "in":
                    continue
                for pid in candidates:
                    prev = earliest_candidate_in_index_by_player.get(pid)
                    if prev is None or sub_index < prev:
                        earliest_candidate_in_index_by_player[pid] = sub_index

            seen_player_set = set(seen_players)
            for sub_index, sub_type, candidates in unresolved_sub_candidates:
                active_candidates = [pid for pid in candidates if pid in seen_player_set]
                if len(active_candidates) != 1:
                    continue
                pid = active_candidates[0]
                prev = first_sub_event_by_player.get(pid)
                if prev is None or sub_index < prev[0]:
                    first_sub_event_by_player[pid] = (sub_index, sub_type)

            first_subtype_by_player = {pid: sub_type for pid, (_, sub_type) in first_sub_event_by_player.items()}

            candidate_order: dict[int, tuple[int, int]] = {}

            def _consider_player(pid: int, rank: int, order: int) -> None:
                if pid <= 0:
                    return
                prev = candidate_order.get(pid)
                marker = (rank, order)
                if prev is None or marker < prev:
                    candidate_order[pid] = marker

            def _positive_action_proves_opening(pid: int) -> bool:
                action_index = first_non_sub_index_by_player.get(pid)
                if action_index is None:
                    return False
                first_sub = first_sub_event_by_player.get(pid)
                if first_sub is not None and first_sub[1] == "in" and first_sub[0] <= action_index:
                    return False
                candidate_in_index = earliest_candidate_in_index_by_player.get(pid)
                if candidate_in_index is not None and candidate_in_index <= action_index:
                    return False
                return True

            # Primary candidates: players active before the first substitution
            # batch, excluding anyone whose first period evidence is a check-in.
            for order, pid in enumerate(early_players):
                if first_subtype_by_player.get(pid) != "in":
                    _consider_player(pid, 0, order)

            # Strong retrodictive evidence: the first substitution batch's outgoing
            # players must have been on court immediately before that batch.
            if sub_batches:
                for order, action in enumerate(sub_batches[0], start=len(candidate_order)):
                    if str(action.get("subType") or "").lower() != "out":
                        continue
                    try:
                        pid = int(action.get("personId", 0) or 0)
                    except Exception:
                        pid = 0
                    if first_subtype_by_player.get(pid) != "in":
                        _consider_player(pid, 0, order)

            # Additional retrodictive evidence: any player whose first period
            # substitution evidence is checking out must have started the period
            # on the floor, even if their first explicit action appears later.
            for pid, (sub_index, sub_type) in sorted(first_sub_event_by_player.items(), key=lambda item: item[1][0]):
                if sub_type != "out":
                    continue
                _consider_player(pid, 2, sub_index)

            # Player-specific lookahead: if a player records a normal action
            # before any credible period check-in evidence, that is positive
            # proof they opened the period on court.
            for pid, action_index in sorted(first_non_sub_index_by_player.items(), key=lambda item: item[1]):
                if _positive_action_proves_opening(pid):
                    _consider_player(pid, 1, action_index)

            # Final fallback: later period-local players in order, still excluding
            # clear early check-ins where possible. Only use this to fill the
            # lineup to five; do not let it grow past five and invalidate an
            # otherwise coherent opening unit.
            for order, pid in enumerate(seen_players):
                if first_subtype_by_player.get(pid) == "in":
                    continue
                _consider_player(pid, 3, order)

            lineup = [pid for pid, _ in sorted(candidate_order.items(), key=lambda item: item[1])]
            if len(lineup) >= 5:
                period_map[team_id] = lineup[:5]

        if period_map:
            overrides[period] = period_map
    return overrides


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


def _resolve_expected_3p_prob(
    action: dict[str, Any],
    expected_3p_probs: dict[int, float] | None,
    player_state: pd.DataFrame,
    area: str,
    shot_type: str,
) -> float:
    if expected_3p_probs:
        action_number = action_number_from_action(action)
        if action_number is not None:
            expected = expected_3p_probs.get(action_number)
            if expected is not None:
                return max(0.10, min(0.60, float(expected)))

    try:
        pid = int(action.get("personId", 0) or 0)
    except Exception:
        pid = 0
    return _expected_make_prob(pid, player_state, area, shot_type)


def compute_adjusted_onoff_for_game(
    game_id: str,
    game_date_mmddyyyy: str,
    player_state: pd.DataFrame,
    orb_rate: float,
    ppp: float,
    expected_3p_probs: dict[int, float] | None = None,
    starters_override: dict[int, list[int]] | None = None,
    period_start_overrides: dict[int, dict[int, list[int]]] | None = None,
    elapsed_lineup_overrides: dict[int, dict[int, list[int]]] | None = None,
    use_game_rotation: bool = False,
    force_elapsed_lineup_overrides: bool = False,
) -> pd.DataFrame:
    """
    Compute per-player on/off plus-minus for a single game, with 3PT luck adjustment.
    """
    if use_game_rotation:
        return _compute_adjusted_onoff_for_game_with_gamerotation(
            game_id=game_id,
            game_date_mmddyyyy=game_date_mmddyyyy,
            player_state=player_state,
            orb_rate=orb_rate,
            ppp=ppp,
            expected_3p_probs=expected_3p_probs,
        )

    gid_norm = str(game_id).zfill(10)
    actions = _sort_actions_precise(get_playbyplay_actions(gid_norm, game_date_mmddyyyy))
    if not actions:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    home_id, away_id = _infer_home_away_from_actions(actions)
    if home_id == 0 or away_id == 0:
        home_id, away_id = get_game_home_away_team_ids(gid_norm, game_date_mmddyyyy)

    starters = dict(starters_override or {})
    inferred_starters = _infer_starters_from_actions(actions)
    for team_id, lineup in inferred_starters.items():
        if len(lineup) >= 5 and len(starters.get(team_id, [])) < 5:
            starters[team_id] = lineup
    if home_id not in starters or away_id not in starters or len(starters.get(home_id, [])) < 5 or len(starters.get(away_id, [])) < 5:
        fallback_starters = get_starters_by_team(gid_norm, game_date_mmddyyyy)
        for team_id, lineup in fallback_starters.items():
            if len(lineup) >= 5 and len(starters.get(team_id, [])) < 5:
                starters[team_id] = lineup

    inferred_period_overrides = _infer_period_start_overrides(actions, home_id, away_id)
    period_start_overrides = {
        **inferred_period_overrides,
        **(period_start_overrides or {}),
    }
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

    prev_elapsed = 0.0
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
    stint_start_elapsed = 0.0
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

    pending_substitutions: dict[tuple[Any, Any], dict[int, list[dict[str, Any]]]] = {}

    def _apply_sub_batch(team_id: int, batch: list[dict[str, Any]]) -> None:
        nonlocal lineups
        outs = [b for b in batch if str(b.get("subType") or "").lower() == "out"]
        ins = [b for b in batch if str(b.get("subType") or "").lower() == "in"]

        def _resolve_sub_pid(sub_action: dict[str, Any], mode: str) -> int | None:
            pid = sub_action.get("personId")
            try:
                pid_int = int(pid)
            except Exception:
                pid_int = 0
            if pid_int > 0:
                if mode == "out" and pid_int in lineups[team_id]:
                    return pid_int
                if mode == "in" and pid_int not in lineups[team_id]:
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

        def _named_sub_pid(sub_action: dict[str, Any]) -> int | None:
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
                if cand > 0:
                    resolved.append(cand)
            if len(resolved) == 1:
                return resolved[0]
            return None

        # Some same-clock tactical swap batches transiently show a player as
        # both exiting and re-entering. Net those to "stays on" before any
        # lineup mutation, e.g. Lowry at 6:05 Q2 in 21500441.
        resolved_out_ids = [pid for pid in (_resolve_sub_pid(b, "out") for b in outs) if pid is not None]
        resolved_in_ids = [pid for pid in (_named_sub_pid(b) for b in ins) if pid is not None]
        neutral_ids = set(resolved_out_ids) & set(resolved_in_ids)
        if neutral_ids:
            outs = [b for b in outs if _resolve_sub_pid(b, "out") not in neutral_ids]
            ins = [b for b in ins if _named_sub_pid(b) not in neutral_ids]

        paired_moves: list[tuple[int, int]] = []
        paired_out_rows: set[int] = set()
        paired_in_rows: set[int] = set()
        outs_by_desc: dict[str, list[dict[str, Any]]] = {}
        ins_by_desc: dict[str, list[dict[str, Any]]] = {}
        for b in outs:
            outs_by_desc.setdefault(str(b.get("description") or ""), []).append(b)
        for b in ins:
            ins_by_desc.setdefault(str(b.get("description") or ""), []).append(b)

        for desc, out_rows in outs_by_desc.items():
            in_rows = ins_by_desc.get(desc, [])
            for out_row, in_row in zip(out_rows, in_rows):
                out_pid = _resolve_sub_pid(out_row, "out")
                in_pid = _resolve_sub_pid(in_row, "in")
                if out_pid is None or in_pid is None or out_pid == in_pid:
                    continue
                paired_moves.append((int(out_pid), int(in_pid)))
                paired_out_rows.add(id(out_row))
                paired_in_rows.add(id(in_row))

        if paired_moves:
            reciprocal_moves = {
                (out_pid, in_pid)
                for out_pid, in_pid in paired_moves
                if (in_pid, out_pid) in paired_moves
            }
            if reciprocal_moves:
                paired_moves = [
                    (out_pid, in_pid)
                    for out_pid, in_pid in paired_moves
                    if (out_pid, in_pid) not in reciprocal_moves
                ]
            if not paired_moves:
                paired_out_rows.clear()
                paired_in_rows.clear()
            else:
                remaining = list(paired_moves)
                applied_moves: list[tuple[int, int]] = []
                while remaining:
                    next_remaining: list[tuple[int, int]] = []
                    progressed = False
                    for out_pid, in_pid in remaining:
                        if out_pid in lineups[team_id] and in_pid not in lineups[team_id]:
                            lineups[team_id].discard(out_pid)
                            lineups[team_id].add(in_pid)
                            applied_moves.append((out_pid, in_pid))
                            progressed = True
                        else:
                            next_remaining.append((out_pid, in_pid))
                    if not progressed:
                        break
                    remaining = next_remaining

                if applied_moves:
                    outs = [b for b in outs if id(b) not in paired_out_rows]
                    ins = [b for b in ins if id(b) not in paired_in_rows]
                else:
                    paired_out_rows.clear()
                    paired_in_rows.clear()

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

    def _flush_pending_substitutions(period: Any, clock: Any) -> None:
        pending = pending_substitutions.pop((period, clock), None)
        if not pending:
            return
        _close_stint()
        for team_id, batch in pending.items():
            _apply_sub_batch(team_id, batch)

    i = 0
    while i < len(actions):
        action = actions[i]
        period = action.get("period")
        clock = action.get("clock")
        elapsed = _elapsed_game_seconds_precise(period, clock)
        current_ts = (period, clock)
        for pending_ts in list(pending_substitutions.keys()):
            if pending_ts != current_ts:
                _flush_pending_substitutions(*pending_ts)

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
                    current_home = lineups.get(home_id, set())
                    current_away = lineups.get(away_id, set())
                    if force_elapsed_lineup_overrides or len(current_home) != 5 or len(current_away) != 5:
                        _close_stint()
                        lineups[home_id] = home_override
                        lineups[away_id] = away_override

        action_type = str(action.get("actionType") or "").lower()

        # Batch substitutions at the same timestamp across both teams.
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
            period = action.get("period")
            clock = action.get("clock")
            batches_by_team: dict[int, list[dict[str, Any]]] = {}
            j = i
            while j < len(actions):
                a = actions[j]
                if str(a.get("actionType") or "").lower() != "substitution":
                    break
                if a.get("period") != period or a.get("clock") != clock:
                    break
                team_id = a.get("teamId")
                if team_id is not None:
                    try:
                        team_id_int = int(team_id)
                    except Exception:
                        team_id_int = 0
                    if team_id_int > 0:
                        batches_by_team.setdefault(team_id_int, []).append(a)
                j += 1

            has_same_clock_ft = False
            k = j
            while k < len(actions):
                other = actions[k]
                if other.get("period") != period or other.get("clock") != clock:
                    break
                if _is_free_throw_action(other):
                    has_same_clock_ft = True
                    break
                k += 1

            if has_same_clock_ft:
                pending_for_ts = pending_substitutions.setdefault((period, clock), {})
                for team_id, batch in batches_by_team.items():
                    if team_id in pending_for_ts:
                        pending_for_ts[team_id].extend(batch)
                    else:
                        pending_for_ts[team_id] = list(batch)
                i = j
                continue

            _close_stint()  # Save stint before lineup changes
            # Do not reset lineup on period starts. The feed's startperiod batch
            # may be incomplete, so treating it as a full replacement can create
            # transient 1-2 player lineups. Apply only explicit in/out deltas.
            for team_id, batch in batches_by_team.items():
                _apply_sub_batch(team_id, batch)

            i = j
            continue

        # Actual score changes
        score_home = action.get("scoreHome")
        score_away = action.get("scoreAway")
        new_home = prev_home
        new_away = prev_away
        parsed_home: int | None = None
        parsed_away: int | None = None
        try:
            if score_home not in (None, ""):
                parsed_home = int(score_home)
            if score_away not in (None, ""):
                parsed_away = int(score_away)
        except Exception:
            parsed_home = None
            parsed_away = None
        if parsed_home is not None and parsed_away is not None:
            # Older local statsv3 regular-season feeds often emit 0-0 on
            # non-scoring actions as a placeholder rather than the live score.
            # Only accept score snapshots that are non-decreasing.
            if parsed_home >= prev_home and parsed_away >= prev_away:
                new_home = parsed_home
                new_away = parsed_away

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
                exp_prob = _resolve_expected_3p_prob(
                    action=action,
                    expected_3p_probs=expected_3p_probs,
                    player_state=player_state,
                    area=area,
                    shot_type=shot_type,
                )
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
                    if len(home_override) == 5:
                        lineups[home_id] = home_override
                    if len(away_override) == 5:
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
        elif _is_free_throw_action(action):
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
    for pending_ts in list(pending_substitutions.keys()):
        _flush_pending_substitutions(*pending_ts)
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
            "game_id": str(gid_norm).lstrip("0"),
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
            "game_id": str(gid_norm).lstrip("0"),
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
            "game_id": str(gid_norm).lstrip("0"),
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

    try:
        max_period = int(
            max(
                int(a.get("period", 0) or 0)
                for a in actions
                if int(a.get("period", 0) or 0) > 0
            )
        )
    except Exception:
        max_period = 4
    expected_game_seconds = 2880 + max(0, max_period - 4) * 300
    actual_stint_end = 0
    if not stint_df.empty and "end_elapsed" in stint_df.columns:
        try:
            actual_stint_end = int(pd.to_numeric(stint_df["end_elapsed"], errors="coerce").max() or 0)
        except Exception:
            actual_stint_end = 0

    # Standard substitution parsing can occasionally miss a final short stint
    # even when the rotation feed has a complete ending lineup. Fall back when
    # stint coverage is incomplete.
    if actual_stint_end < expected_game_seconds - 1:
        try:
            gr_player_df, gr_stint_df, gr_poss_df = _compute_adjusted_onoff_for_game_with_gamerotation(
                game_id=gid_norm,
                game_date_mmddyyyy=game_date_mmddyyyy,
                player_state=player_state,
                orb_rate=orb_rate,
                ppp=ppp,
                expected_3p_probs=expected_3p_probs,
            )
            gr_actual_end = 0
            if not gr_stint_df.empty and "end_elapsed" in gr_stint_df.columns:
                try:
                    gr_actual_end = int(pd.to_numeric(gr_stint_df["end_elapsed"], errors="coerce").max() or 0)
                except Exception:
                    gr_actual_end = 0
            if gr_actual_end > actual_stint_end:
                return gr_player_df, gr_stint_df, gr_poss_df
        except Exception:
            pass

    return player_df, stint_df, poss_df


def _compute_adjusted_onoff_for_game_with_gamerotation(
    game_id: str,
    game_date_mmddyyyy: str,
    player_state: pd.DataFrame,
    orb_rate: float,
    ppp: float,
    expected_3p_probs: dict[int, float] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    actions = _sort_actions_precise(get_playbyplay_actions(game_id, game_date_mmddyyyy))
    if not actions:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    gid = str(game_id).zfill(10)
    try:
        home_id, away_id = _load_stats_home_away(gid)
        rotation = _load_stats_gamerotation(gid)
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    official_plus_minus: dict[int, float] = {}
    try:
        box = _load_stats_boxscore(gid)["players"]
        if not box.empty:
            for row in box.itertuples(index=False):
                try:
                    pid = int(getattr(row, "PLAYER_ID", 0) or 0)
                except Exception:
                    continue
                if pid <= 0:
                    continue
                try:
                    official_plus_minus[pid] = float(getattr(row, "PLUS_MINUS", 0.0) or 0.0)
                except Exception:
                    continue
    except Exception:
        pass

    player_info: dict[int, dict[str, Any]] = {}
    for row in rotation.itertuples(index=False):
        pid = int(row.PERSON_ID)
        if pid <= 0:
            continue
        full_name = " ".join(
            part
            for part in [
                str(getattr(row, "PLAYER_FIRST", "") or "").strip(),
                str(getattr(row, "PLAYER_LAST", "") or "").strip(),
            ]
            if part
        )
        player_info[pid] = {
            "player_name": full_name,
            "team_id": int(row.TEAM_ID),
        }
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
        elif not player_info[pid].get("player_name"):
            player_info[pid]["player_name"] = str(a.get("playerName") or a.get("playerNameI") or "")

    stats: dict[int, PlayerOnOff] = {}
    for pid, info in player_info.items():
        stats[pid] = PlayerOnOff(
            player_id=pid,
            player_name=str(info.get("player_name", "")),
            team_id=int(info.get("team_id", 0)),
        )

    for row in rotation.itertuples(index=False):
        pid = int(row.PERSON_ID)
        if pid <= 0:
            continue
        if pid not in stats:
            stats[pid] = PlayerOnOff(
                player_id=pid,
                player_name=str(player_info.get(pid, {}).get("player_name", "")),
                team_id=int(row.TEAM_ID),
            )
        stats[pid].seconds_on += max(0, int(round((float(row.end_elapsed) - float(row.start_elapsed)))))

    haircut = orb_rate * ppp
    adj_factor = 3.0 - haircut
    prev_home = 0
    prev_away = 0
    adj_home = 0.0
    adj_away = 0.0
    event_points: list[dict[str, Any]] = []
    possessions: list[dict[str, Any]] = []
    possession_team: int | None = None
    possession_pts = 0
    possession_pts_adj = 0.0
    max_period = 4
    last_period = 1

    def _format_clock(seconds_remaining: float) -> str:
        seconds_remaining = max(0.0, seconds_remaining)
        minutes = int(seconds_remaining // 60)
        seconds = seconds_remaining - (minutes * 60)
        return f"PT{minutes}M{seconds:05.2f}S"

    def _period_boundary_metadata(elapsed: float, side: str) -> tuple[int, str]:
        remaining_elapsed = float(elapsed)
        period = 1
        while True:
            length = float(_period_length_seconds(period))
            if remaining_elapsed < length - 1e-9:
                return period, _format_clock(length - remaining_elapsed)
            if abs(remaining_elapsed - length) <= 1e-9:
                if side == "start":
                    next_period = period + 1
                    return next_period, _format_clock(float(_period_length_seconds(next_period)))
                return period, _format_clock(0.0)
            remaining_elapsed -= length
            period += 1

    def _active_lineups(elapsed: float) -> tuple[set[int], set[int]]:
        active = rotation[(rotation["start_elapsed"] < elapsed) & (elapsed <= rotation["end_elapsed"])]
        home = set(int(pid) for pid in active.loc[active["TEAM_ID"] == home_id, "PERSON_ID"].tolist())
        away = set(int(pid) for pid in active.loc[active["TEAM_ID"] == away_id, "PERSON_ID"].tolist())
        return home, away

    sub_batches_by_elapsed: dict[float, dict[int, dict[str, list[int]]]] = {}
    for action in actions:
        if str(action.get("actionType") or "").lower() != "substitution":
            continue
        elapsed = _elapsed_game_seconds_precise(action.get("period"), action.get("clock"))
        if elapsed is None:
            continue
        try:
            team_id = int(action.get("teamId", 0) or 0)
            pid = int(action.get("personId", 0) or 0)
        except Exception:
            continue
        if team_id not in (home_id, away_id) or pid <= 0:
            continue
        team_batch = sub_batches_by_elapsed.setdefault(float(elapsed), {}).setdefault(team_id, {"in": [], "out": []})
        sub_type = str(action.get("subType") or "").lower()
        if sub_type == "in" and pid not in team_batch["in"]:
            team_batch["in"].append(pid)
        elif sub_type == "out" and pid not in team_batch["out"]:
            team_batch["out"].append(pid)

    game_end_elapsed = float(sum(_period_length_seconds(p) for p in range(1, max_period + 1)))
    period_starts = [0.0]
    elapsed_cursor = 0.0
    for period in range(1, max_period):
        elapsed_cursor += float(_period_length_seconds(period))
        period_starts.append(elapsed_cursor)

    period_anchor_lineups: dict[float, tuple[set[int], set[int]]] = {}
    for start in period_starts:
        probe = min(start + 0.01, game_end_elapsed)
        home_lineup, away_lineup = _active_lineups(probe)
        if len(home_lineup) == 5 and len(away_lineup) == 5:
            period_anchor_lineups[float(start)] = (set(home_lineup), set(away_lineup))

    fallback_segments: list[tuple[float, float, set[int], set[int]]] = []
    fallback_boundaries = sorted({0.0, game_end_elapsed, *period_starts, *sub_batches_by_elapsed.keys()})
    current_home, current_away = period_anchor_lineups.get(0.0, (set(), set()))
    prev_boundary = 0.0
    for boundary in fallback_boundaries[1:]:
        if len(current_home) == 5 and len(current_away) == 5 and boundary > prev_boundary:
            fallback_segments.append((prev_boundary, float(boundary), set(current_home), set(current_away)))
        if float(boundary) in period_anchor_lineups and float(boundary) != 0.0:
            anchor_home, anchor_away = period_anchor_lineups[float(boundary)]
            current_home = set(anchor_home)
            current_away = set(anchor_away)
        batch_map = sub_batches_by_elapsed.get(float(boundary), {})
        for team_id, changes in batch_map.items():
            lineup = current_home if team_id == home_id else current_away
            for pid in changes.get("out", []):
                lineup.discard(int(pid))
            for pid in changes.get("in", []):
                lineup.add(int(pid))
        prev_boundary = float(boundary)

    def _fallback_lineups(elapsed: float) -> tuple[set[int], set[int]]:
        for start, end, home_lineup, away_lineup in fallback_segments:
            if start < elapsed <= end:
                return set(home_lineup), set(away_lineup)
        return set(), set()

    def _resolved_lineups(elapsed: float) -> tuple[set[int], set[int]]:
        home_lineup, away_lineup = _active_lineups(elapsed)
        if len(home_lineup) == 5 and len(away_lineup) == 5:
            return home_lineup, away_lineup
        fallback_home, fallback_away = _fallback_lineups(elapsed)
        if len(fallback_home) == 5 and len(fallback_away) == 5:
            return fallback_home, fallback_away
        return home_lineup, away_lineup

    def _apply_points(team_id: int, points: float, adjusted: bool, elapsed: float) -> None:
        if points == 0:
            return
        home_lineup, away_lineup = _resolved_lineups(elapsed)
        if len(home_lineup) != 5 or len(away_lineup) != 5:
            return
        offense = home_lineup if team_id == home_id else away_lineup
        defense = away_lineup if team_id == home_id else home_lineup
        for pid in offense:
            if adjusted:
                stats[pid].on_pts_for_adj += points
            else:
                stats[pid].on_pts_for += points
        for pid in defense:
            if adjusted:
                stats[pid].on_pts_against_adj += points
            else:
                stats[pid].on_pts_against += points

    def _close_possession(ended_by: str, elapsed_for_lineup: float) -> None:
        nonlocal possession_team, possession_pts, possession_pts_adj
        if possession_team is None:
            return
        home_lineup, away_lineup = _resolved_lineups(elapsed_for_lineup)
        if len(home_lineup) != 5 or len(away_lineup) != 5:
            possession_pts = 0
            possession_pts_adj = 0.0
            return
        offense_lineup = home_lineup if possession_team == home_id else away_lineup
        defense_lineup = away_lineup if possession_team == home_id else home_lineup
        defense_team = away_id if possession_team == home_id else home_id
        possessions.append({
            "offense_team": possession_team,
            "defense_team": defense_team,
            "offense_lineup": tuple(sorted(offense_lineup)),
            "defense_lineup": tuple(sorted(defense_lineup)),
            "points": possession_pts,
            "points_adj": possession_pts_adj,
            "ended_by": ended_by,
            "period": last_period,
        })
        possession_pts = 0
        possession_pts_adj = 0.0

    for action in actions:
        elapsed = _elapsed_game_seconds_precise(action.get("period"), action.get("clock"))
        if elapsed is None:
            continue
        elapsed_f = float(elapsed)
        try:
            last_period = int(action.get("period") or last_period)
            max_period = max(max_period, last_period)
        except Exception:
            pass

        score_home = action.get("scoreHome")
        score_away = action.get("scoreAway")
        new_home = prev_home
        new_away = prev_away
        parsed_home: int | None = None
        parsed_away: int | None = None
        try:
            if score_home not in (None, ""):
                parsed_home = int(score_home)
            if score_away not in (None, ""):
                parsed_away = int(score_away)
        except Exception:
            parsed_home = None
            parsed_away = None
        if parsed_home is not None and parsed_away is not None:
            if parsed_home >= prev_home and parsed_away >= prev_away:
                new_home = parsed_home
                new_away = parsed_away

        delta_home = new_home - prev_home
        delta_away = new_away - prev_away
        home_adj_delta = float(delta_home)
        away_adj_delta = float(delta_away)
        if delta_home != 0:
            _apply_points(home_id, float(delta_home), adjusted=False, elapsed=elapsed_f)
            _apply_points(home_id, float(delta_home), adjusted=True, elapsed=elapsed_f)
            adj_home += float(delta_home)
        if delta_away != 0:
            _apply_points(away_id, float(delta_away), adjusted=False, elapsed=elapsed_f)
            _apply_points(away_id, float(delta_away), adjusted=True, elapsed=elapsed_f)
            adj_away += float(delta_away)
        prev_home, prev_away = new_home, new_away

        action_type = str(action.get("actionType") or "").lower()
        action_team_id = action.get("teamId")
        if action_team_id is not None:
            try:
                action_team_id = int(action_team_id)
            except Exception:
                action_team_id = None

        if action_type in {"jump ball", "jumpball"}:
            if action_team_id and action_team_id in (home_id, away_id):
                possession_team = action_team_id

        elif action_type == "period":
            desc = str(action.get("description") or "").lower()
            if "end" in desc or "start" in desc:
                _close_possession("period", elapsed_f)
                possession_team = None

        elif action_type in ("2pt", "3pt", "heave"):
            shot_result = str(action.get("shotResult") or "").lower()
            if action_team_id:
                if possession_team is None:
                    possession_team = action_team_id
            if shot_result == "made":
                pts = delta_home if action_team_id == home_id else delta_away
                possession_pts += pts

        elif action_type == "rebound":
            desc = str(action.get("description") or "").lower()
            if "defensive" in desc:
                _close_possession("defensive_rebound", elapsed_f)
                if action_team_id:
                    possession_team = action_team_id
            elif "offensive" in desc:
                if action_team_id and action_team_id in (home_id, away_id):
                    possession_team = action_team_id
            elif action_team_id:
                if possession_team is None:
                    possession_team = action_team_id
                elif action_team_id != possession_team:
                    _close_possession("defensive_rebound", elapsed_f)
                    possession_team = action_team_id

        elif action_type == "turnover":
            _close_possession("turnover", elapsed_f)
            if action_team_id == home_id:
                possession_team = away_id
            elif action_team_id == away_id:
                possession_team = home_id

        elif _is_free_throw_action(action):
            ft_pts = _get_ft_points(action)
            if action_team_id:
                if possession_team is None:
                    possession_team = action_team_id
                possession_pts += ft_pts

        elif action_type == "steal":
            if action_team_id and possession_team is None:
                possession_team = action_team_id

        if action_type == "3pt":
            team_id = action.get("teamId")
            pid = action.get("personId")
            if team_id is not None and pid is not None:
                try:
                    team_id = int(team_id)
                    pid = int(pid)
                except Exception:
                    team_id = None
                    pid = None
                if team_id is not None and pid is not None:
                    shot_type = _classify_shot_type(action)
                    area = _classify_area(action)
                    exp_prob = _resolve_expected_3p_prob(
                        action=action,
                        expected_3p_probs=expected_3p_probs,
                        player_state=player_state,
                        area=area,
                        shot_type=shot_type,
                    )
                    actual_make = 1 if str(action.get("shotResult") or "").lower() == "made" else 0
                    three_pt_adj_delta = (exp_prob - actual_make) * adj_factor
                    _apply_points(team_id, float(three_pt_adj_delta), adjusted=True, elapsed=elapsed_f)
                    if team_id == home_id:
                        adj_home += float(three_pt_adj_delta)
                        home_adj_delta += float(three_pt_adj_delta)
                    elif team_id == away_id:
                        adj_away += float(three_pt_adj_delta)
                        away_adj_delta += float(three_pt_adj_delta)

        if action_type in ("2pt", "3pt", "heave") and str(action.get("shotResult") or "").lower() == "made":
            if action_team_id == home_id:
                possession_pts_adj += home_adj_delta
                _close_possession("made_shot", elapsed_f)
                possession_team = away_id
            elif action_team_id == away_id:
                possession_pts_adj += away_adj_delta
                _close_possession("made_shot", elapsed_f)
                possession_team = home_id
        elif _is_free_throw_action(action) and action_team_id:
            if action_team_id == home_id:
                possession_pts_adj += home_adj_delta
            elif action_team_id == away_id:
                possession_pts_adj += away_adj_delta
            if _is_last_free_throw(action):
                _close_possession("free_throw", elapsed_f)
                if action_team_id == home_id:
                    possession_team = away_id
                elif action_team_id == away_id:
                    possession_team = home_id

        if delta_home != 0 or abs(home_adj_delta) > 1e-9:
            event_points.append({
                "elapsed": elapsed_f,
                "team_id": home_id,
                "raw_points": float(delta_home),
                "adj_points": float(home_adj_delta),
            })
        if delta_away != 0 or abs(away_adj_delta) > 1e-9:
            event_points.append({
                "elapsed": elapsed_f,
                "team_id": away_id,
                "raw_points": float(delta_away),
                "adj_points": float(away_adj_delta),
            })

    if actions:
        final_elapsed = _elapsed_game_seconds_precise(actions[-1].get("period"), actions[-1].get("clock"))
        if final_elapsed is not None:
            _close_possession("end_of_game", float(final_elapsed))

    period_boundaries: set[float] = {0.0}
    elapsed_total = 0.0
    for period in range(1, max_period + 1):
        elapsed_total += float(_period_length_seconds(period))
        period_boundaries.add(elapsed_total)
    rotation_boundaries = set(float(x) for x in rotation["start_elapsed"].tolist() + rotation["end_elapsed"].tolist())
    boundaries = sorted(period_boundaries | rotation_boundaries)

    event_points = sorted(event_points, key=lambda x: float(x["elapsed"]))
    event_idx = 0
    cumulative_home = 0.0
    cumulative_away = 0.0
    cumulative_home_adj = 0.0
    cumulative_away_adj = 0.0
    game_id_norm = str(game_id).lstrip("0")
    stint_rows: list[dict[str, Any]] = []
    stint_index = 0

    for start_elapsed, end_elapsed in zip(boundaries, boundaries[1:]):
        duration = float(end_elapsed) - float(start_elapsed)
        if duration <= 0:
            continue
        home_lineup, away_lineup = _resolved_lineups(float(end_elapsed))
        if len(home_lineup) != 5 or len(away_lineup) != 5:
            continue
        start_home = cumulative_home
        start_away = cumulative_away
        start_home_adj = cumulative_home_adj
        start_away_adj = cumulative_away_adj
        home_pts = 0.0
        away_pts = 0.0
        home_pts_adj = 0.0
        away_pts_adj = 0.0
        while event_idx < len(event_points) and float(event_points[event_idx]["elapsed"]) <= float(end_elapsed) + 1e-9:
            evt = event_points[event_idx]
            if float(evt["elapsed"]) > float(start_elapsed) + 1e-9:
                if int(evt["team_id"]) == home_id:
                    home_pts += float(evt["raw_points"])
                    home_pts_adj += float(evt["adj_points"])
                else:
                    away_pts += float(evt["raw_points"])
                    away_pts_adj += float(evt["adj_points"])
            event_idx += 1
        cumulative_home += home_pts
        cumulative_away += away_pts
        cumulative_home_adj += home_pts_adj
        cumulative_away_adj += away_pts_adj
        start_period, start_clock = _period_boundary_metadata(float(start_elapsed), "start")
        end_period, end_clock = _period_boundary_metadata(float(end_elapsed), "end")
        home_lineup_sorted = tuple(sorted(home_lineup))
        away_lineup_sorted = tuple(sorted(away_lineup))
        stint_rows.append({
            "game_id": game_id_norm,
            "stint_index": stint_index,
            "home_id": home_id,
            "away_id": away_id,
            "home_p1": home_lineup_sorted[0],
            "home_p2": home_lineup_sorted[1],
            "home_p3": home_lineup_sorted[2],
            "home_p4": home_lineup_sorted[3],
            "home_p5": home_lineup_sorted[4],
            "away_p1": away_lineup_sorted[0],
            "away_p2": away_lineup_sorted[1],
            "away_p3": away_lineup_sorted[2],
            "away_p4": away_lineup_sorted[3],
            "away_p5": away_lineup_sorted[4],
            "seconds": round(duration, 1),
            "home_pts": round(home_pts, 3),
            "away_pts": round(away_pts, 3),
            "home_pts_adj": round(home_pts_adj, 3),
            "away_pts_adj": round(away_pts_adj, 3),
            "start_elapsed": round(float(start_elapsed), 1),
            "end_elapsed": round(float(end_elapsed), 1),
            "start_period": int(start_period),
            "start_clock": start_clock,
            "end_period": int(end_period),
            "end_clock": end_clock,
            "start_home_score": round(start_home, 3),
            "start_away_score": round(start_away, 3),
            "end_home_score": round(cumulative_home, 3),
            "end_away_score": round(cumulative_away, 3),
            "start_home_score_adj": round(start_home_adj, 3),
            "start_away_score_adj": round(start_away_adj, 3),
            "end_home_score_adj": round(cumulative_home_adj, 3),
            "end_away_score_adj": round(cumulative_away_adj, 3),
            "home_lineup_complete": True,
            "away_lineup_complete": True,
        })
        stint_index += 1

    for p in stats.values():
        p.seconds_on = 0
    for stint in stint_rows:
        duration = float(stint["seconds"])
        for key in ("home_p1", "home_p2", "home_p3", "home_p4", "home_p5", "away_p1", "away_p2", "away_p3", "away_p4", "away_p5"):
            try:
                pid = int(stint[key])
            except Exception:
                continue
            if pid in stats:
                stats[pid].seconds_on += int(round(duration))

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
        team_margin_actual = team_actual - opp_actual
        on_diff_official = float(official_plus_minus.get(pid, on_diff_reconstructed))
        off_diff_official = team_margin_actual - on_diff_official
        rows.append({
            "game_id": game_id_norm,
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

    poss_rows = []
    for idx, p in enumerate(possessions):
        off_lineup = p["offense_lineup"]
        def_lineup = p["defense_lineup"]
        poss_rows.append({
            "game_id": game_id_norm,
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

    return pd.DataFrame(rows), pd.DataFrame(stint_rows), pd.DataFrame(poss_rows)
