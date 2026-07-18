"""Whole-game lineup reconstruction from PBP plus official constraints.

This module is intentionally independent of the published stint artifacts.
For each team/game it solves all interval lineups jointly as a mixed-integer
program.  Official starters, minutes and plus-minus are constraints/penalties;
substitution text and player actions define feasible transitions.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

import numpy as np
import pandas as pd
from scipy import sparse
from scipy.optimize import Bounds, LinearConstraint, milp

from src.ingest import _expand_local_substitutions, _normalize_statsv3_actions
from src.onoff import (_elapsed_game_seconds_precise, _parse_clock_seconds_precise,
                       _sort_actions_precise)


@dataclass
class TeamSolution:
    team_id: int
    players: np.ndarray
    boundaries: np.ndarray
    lineups: list[tuple[int, ...]]
    objective: float
    status: int


def prepare_actions(frame: pd.DataFrame) -> list[dict[str, Any]]:
    actions = frame.replace({np.nan: None}).to_dict("records")
    actions = _normalize_statsv3_actions(actions)
    actions = _expand_local_substitutions(actions)
    return _sort_actions_precise(actions)


def resolve_substitutions_from_official_roster(
        actions: list[dict[str, Any]], official: pd.DataFrame
) -> list[dict[str, Any]]:
    """Resolve name-only legacy substitutions against that game's roster.

    Old NBA rows commonly identify the outgoing player structurally while the
    incoming player exists only in text (``SUB: Ceballos FOR Owens``).  The
    official box roster is independent, game-specific identity evidence.  A
    unique surname match is safe; ambiguous surnames remain candidates for the
    whole-game solver instead of being guessed locally.
    """
    def norm(value: Any) -> str:
        return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()

    rosters: dict[int, dict[str, set[int]]] = {}
    if "player_name" not in official.columns:
        return actions
    for row in official.itertuples(index=False):
        try:
            tid, pid = int(row.team_id), int(row.player_id)
        except Exception:
            continue
        name = norm(row.player_name)
        if not name:
            continue
        keys = {name, name.split()[-1]}
        for key in keys:
            rosters.setdefault(tid, {}).setdefault(key, set()).add(pid)

    out: list[dict[str, Any]] = []
    for original in actions:
        a = dict(original)
        if str(a.get("actionType") or "").lower() != "substitution":
            out.append(a); continue
        try:
            tid = int(a.get("teamId") or 0)
        except Exception:
            tid = 0
        match = re.search(r"sub:\s*(.*?)\s+for\s+(.*?)(?:\s*$|\s*\()",
                          str(a.get("description") or ""), re.I)
        mode = str(a.get("subType") or "").lower()
        token = norm(match.group(1 if mode == "in" else 2)) if match else ""
        candidates = set()
        for key, ids in rosters.get(tid, {}).items():
            if token and (token == key or token.endswith(" " + key)):
                candidates |= ids
        existing = a.get("candidatePersonIds") or []
        for value in existing:
            try: candidates.add(int(value))
            except Exception: pass
        try:
            pid = int(a.get("personId") or 0)
        except Exception:
            pid = 0
        if pid > 0:
            candidates.add(pid)
        if pid <= 0 and len(candidates) == 1:
            a["personId"] = next(iter(candidates))
            a["rosterNameResolved"] = True
        if candidates:
            a["candidatePersonIds"] = sorted(candidates)
        out.append(a)
    return out


def game_boundaries(actions: list[dict[str, Any]]) -> np.ndarray:
    max_period = max(int(a.get("period") or 1) for a in actions)
    total = 2880.0 + max(0, max_period - 4) * 300.0
    vals = {0.0, total, 720.0, 1440.0, 2160.0, 2880.0}
    for p in range(5, max_period + 1):
        vals.add(2880.0 + (p - 4) * 300.0)
    for a in actions:
        if str(a.get("actionType") or "").lower() != "substitution":
            continue
        e = _elapsed_game_seconds_precise(a.get("period"), a.get("clock"))
        if e is not None and 0 < e < total:
            vals.add(float(e))
    return np.array(sorted(v for v in vals if 0 <= v <= total), float)


def _interval_index(boundaries: np.ndarray, elapsed: float) -> int:
    return int(np.clip(np.searchsorted(boundaries, elapsed, side="right") - 1,
                       0, len(boundaries) - 2))


def action_interval_index(boundaries: np.ndarray, action: dict[str, Any]) -> int:
    """Map an action to an interval without conflating adjacent periods.

    Q1 0:00 and Q2 12:00 have the same elapsed value but belong to opposite
    sides of the boundary.  The production replay parser retains period/clock;
    the canonical solver must do the same.
    """
    elapsed = _elapsed_game_seconds_precise(action.get("period"), action.get("clock"))
    if elapsed is None:
        return 0
    remaining = _parse_clock_seconds_precise(action.get("clock"))
    side = "left" if remaining is not None and abs(float(remaining)) < 1e-7 else "right"
    return int(np.clip(np.searchsorted(boundaries, float(elapsed), side=side) - 1,
                       0, len(boundaries) - 2))


def _is_true_period_boundary(action: dict[str, Any]) -> bool:
    """Return whether an action is at the end/start seam between periods."""
    remaining = _parse_clock_seconds_precise(action.get("clock"))
    if remaining is None:
        return False
    try:
        period = int(action.get("period") or 1)
    except (TypeError, ValueError):
        period = 1
    period_length = 720.0 if period <= 4 else 300.0
    return (abs(float(remaining)) < 1e-7
            or abs(float(remaining) - period_length) < 1e-7)


def score_points_by_interval(actions: list[dict[str, Any]],
                             boundaries: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    home = np.zeros(len(boundaries) - 1)
    away = np.zeros(len(boundaries) - 1)
    ph = pa = 0
    first_sub_at: dict[tuple[Any, Any], int] = {}
    for ai, a in enumerate(actions):
        if str(a.get("actionType") or "").lower() != "substitution":
            continue
        e = _elapsed_game_seconds_precise(a.get("period"), a.get("clock"))
        if e is not None:
            first_sub_at.setdefault((a.get("period"), a.get("clock")), ai)
    for ai, a in enumerate(actions):
        try:
            h = int(float(a.get("scoreHome"))) if a.get("scoreHome") not in (None, "") else ph
            v = int(float(a.get("scoreAway"))) if a.get("scoreAway") not in (None, "") else pa
        except Exception:
            continue
        if h < ph or v < pa:
            continue
        dh, da = h - ph, v - pa
        ph, pa = h, v
        if dh == 0 and da == 0:
            continue
        e = _elapsed_game_seconds_precise(a.get("period"), a.get("clock"))
        if e is not None:
            ef = float(e); event_key = (a.get("period"), a.get("clock"))
            # Scores at the same clock as a substitution belong to the lineup
            # indicated by event order (not automatically the post-sub unit).
            if (event_key in first_sub_at and ai < first_sub_at[event_key]
                    and not _is_true_period_boundary(a)):
                ii = int(np.clip(np.searchsorted(boundaries, ef, side="left") - 1,
                                 0, len(boundaries) - 2))
            else:
                ii = action_interval_index(boundaries, a)
            home[ii] += dh
            away[ii] += da
    return home, away


def _score_margin_by_interval(actions: list[dict[str, Any]],
                              boundaries: np.ndarray) -> np.ndarray:
    home, away = score_points_by_interval(actions, boundaries)
    return home - away


def solve_team(actions: list[dict[str, Any]], official: pd.DataFrame,
               team_id: int, boundaries: np.ndarray, is_home: bool,
               time_limit: float = 15.0,
               enforce_actions: bool = False,
               seed_stints: pd.DataFrame | None = None,
               seed_hard: bool = False,
               transition_scope: bool = True,
               enforce_resolved_transitions: bool = True,
               enforce_action_presence: bool = True,
               seed_weight: float = 0.25) -> TeamSolution:
    roster = sorted(set(pd.to_numeric(
        official.loc[official.team_id == team_id, "player_id"],
        errors="coerce").dropna().astype(int)))
    for a in actions if enforce_actions else []:
        try:
            if int(a.get("teamId") or 0) == team_id and int(a.get("personId") or 0) > 0:
                roster.append(int(a.get("personId")))
        except Exception:
            pass
    players = np.array(sorted(set(roster)), int)
    pmap = {p: j for j, p in enumerate(players)}
    I, R = len(boundaries) - 1, len(players)
    if R < 5:
        raise ValueError(f"team {team_id}: only {R} roster players")
    nx = I * R
    nchange = max(I - 1, 0) * R
    # x(interval,player), minute absolute deviations, PM absolute deviations
    nvar = nx + 2 * R + nchange
    c = np.zeros(nvar)
    c[nx:nx+R] = 1.0
    # Official plus-minus is exact integer evidence; official minutes are
    # rounded.  Make a one-point PM miss much more expensive than a small
    # minute discrepancy so boundary ambiguity resolves in the right order.
    c[nx+R:nx+2*R] = 20.0
    integrality = np.zeros(nvar, int); integrality[:nx] = 1
    integrality[nx + 2*R:] = 1
    lb = np.zeros(nvar); ub = np.full(nvar, np.inf); ub[:nx] = 1
    ub[nx + 2*R:] = 1

    rows: list[dict[int, float]] = []
    lows: list[float] = []; highs: list[float] = []
    def add(vals: dict[int, float], lo: float, hi: float) -> None:
        rows.append(vals); lows.append(lo); highs.append(hi)
    def xidx(i: int, j: int) -> int:
        return i * R + j
    def yidx(before_i: int, j: int) -> int:
        return nx + 2*R + before_i * R + j

    # Exactly five players in every interval.
    for i in range(I):
        add({xidx(i, j): 1.0 for j in range(R)}, 5.0, 5.0)

    # The modern replay parser is highly informative wherever it emits a
    # complete stint. Freeze those intervals and let the MILP solve only the
    # holes; this prevents an underidentified whole-game rearrangement.
    if seed_stints is not None and not seed_stints.empty:
        prefix = "home" if is_home else "away"
        pcols = [f"{prefix}_p{k}" for k in range(1, 6)]
        seed = seed_stints.sort_values("start_elapsed")
        for i in range(I):
            mid = (boundaries[i] + boundaries[i + 1]) / 2.0
            hit = seed[(seed.start_elapsed <= mid) & (seed.end_elapsed >= mid)]
            if hit.empty:
                continue
            lineup = {int(x) for x in hit.iloc[-1][pcols].tolist()
                      if pd.notna(x) and int(x) in pmap}
            if len(lineup) != 5:
                continue
            for pid, j in pmap.items():
                val = 1.0 if pid in lineup else 0.0
                if seed_hard:
                    add({xidx(i, j): 1.0}, val, val)
                else:
                    # Reward agreement with the replay seed, but permit a
                    # correction when official minutes/PM prove it wrong.
                    c[xidx(i, j)] += -seed_weight if val else seed_weight

    # The consolidated historical starter flag is useful but not consistently
    # authoritative. Reward agreement when it identifies exactly five players;
    # do not let one bad flag make an otherwise evidenced game infeasible.
    starters = official[(official.team_id == team_id)
                        & official.starter.fillna(False)].player_id.astype(int)
    # The consolidated historical player_game_facts table sometimes labels
    # every rotation player as a starter.  Treat this field as hard evidence
    # only when it actually identifies one legal five-man unit.
    if starters.nunique() == 5:
        for pid in starters:
            if pid in pmap:
                c[xidx(0, pmap[pid])] -= 2.0

    # A normal recorded action proves presence, except at a substitution
    # timestamp where ordering (before/after the batch) can be ambiguous.
    sub_times = {(a.get("period"), a.get("clock"))
                 for a in actions
                 if str(a.get("actionType") or "").lower() == "substitution"
                 and _elapsed_game_seconds_precise(a.get("period"), a.get("clock")) is not None}
    proving = {"2pt", "3pt", "heave", "rebound", "turnover", "foul",
               "freethrow", "free throw", "jumpball", "jump ball"}
    for a in actions if enforce_action_presence else []:
        try:
            tid, pid = int(a.get("teamId") or 0), int(a.get("personId") or 0)
        except Exception:
            continue
        if tid != team_id or pid not in pmap:
            continue
        if str(a.get("actionType") or "").lower() not in proving:
            continue
        e = _elapsed_game_seconds_precise(a.get("period"), a.get("clock"))
        if e is None or (a.get("period"), a.get("clock")) in sub_times:
            continue
        i = action_interval_index(boundaries, a)
        add({xidx(i, pmap[pid]): 1.0}, 1.0, 1.0)

    # Substitution transitions. Outside the named/candidate set, lineup state
    # must be unchanged. Resolved outs/ins are hard transitions.
    for b in boundaries[1:-1]:
        before = _interval_index(boundaries, b - 1e-5)
        after = _interval_index(boundaries, b + 1e-5)
        batch = []
        for a in actions:
            e = _elapsed_game_seconds_precise(a.get("period"), a.get("clock"))
            try:
                tid = int(a.get("teamId") or 0)
            except Exception:
                tid = 0
            if (str(a.get("actionType") or "").lower() == "substitution"
                    and tid == team_id and e is not None and abs(float(e)-b) < 1e-4):
                batch.append(a)
        is_period = (b in {720.0, 1440.0, 2160.0, 2880.0}
                     or (b > 2880 and abs((b-2880) % 300) < 1e-6))
        if not batch:
            # At a true period boundary either team may change freely. At the
            # opponent's substitution clock this team must remain unchanged.
            if not is_period:
                for pid, j in pmap.items():
                    add({xidx(before, j): 1.0, xidx(after, j): -1.0}, 0.0, 0.0)
        affected: set[int] = set()
        outs: set[int] = set(); ins: set[int] = set()
        if batch:
            for a in batch:
                try:
                    pid = int(a.get("personId") or 0)
                except Exception:
                    pid = 0
                cand = {int(z) for z in (a.get("candidatePersonIds") or [])
                        if str(z).isdigit()}
                affected |= cand
                mode = str(a.get("subType") or "").lower()
                if pid > 0:
                    affected.add(pid)
                    if mode == "out": outs.add(pid)
                    elif mode == "in": ins.add(pid)
        # Explicit change variables make locally unsupported swaps expensive
        # but not impossible when the full-game official constraints prove the
        # historical name parsing wrong.
        for pid, j in pmap.items():
            yy = yidx(before, j)
            add({yy: 1.0, xidx(after, j): -1.0,
                 xidx(before, j): 1.0}, 0.0, np.inf)
            add({yy: 1.0, xidx(after, j): 1.0,
                 xidx(before, j): -1.0}, 0.0, np.inf)
            c[yy] = (1.0 if is_period else
                     (0.1 if pid in affected else 50.0))
        if not batch:
            continue
        if not affected:
            # Unparsed substitution: permit a change, but do not manufacture
            # player identities locally; minutes/actions will resolve it.
            affected = set(players)
        if transition_scope:
            for pid in players:
                if pid not in affected:
                    j = pmap[pid]
                    add({xidx(before, j): 1.0, xidx(after, j): -1.0}, 0.0, 0.0)
        # A balanced, disjoint batch whose identities all resolve against the
        # official game roster is direct evidence of the transition.  No
        # optimizer should be allowed to replace it with a statistically more
        # convenient swap.  Malformed/duplicated legacy batches remain soft.
        if (not is_period and enforce_resolved_transitions and outs and len(outs) == len(ins)
                and outs.isdisjoint(ins)):
            for pid in outs:
                if pid in pmap:
                    add({xidx(before, pmap[pid]): 1.0}, 1.0, 1.0)
                    add({xidx(after, pmap[pid]): 1.0}, 0.0, 0.0)
            for pid in ins:
                if pid in pmap:
                    add({xidx(before, pmap[pid]): 1.0}, 0.0, 0.0)
                    add({xidx(after, pmap[pid]): 1.0}, 1.0, 1.0)

    durations = np.diff(boundaries)
    home_margin = _score_margin_by_interval(actions, boundaries)
    margin = home_margin if is_home else -home_margin
    off = official[official.team_id == team_id].set_index("player_id")
    for pid, j in pmap.items():
        secs = float(off.loc[pid, "minutes"] * 60.0) if pid in off.index else 0.0
        pm = float(off.loc[pid, "plus_minus_actual"]) if pid in off.index else 0.0
        minute_expr = {xidx(i, j): float(durations[i]) for i in range(I)}
        minute_expr[nx+j] = -1.0
        add(minute_expr, -np.inf, secs)
        minute_expr2 = {xidx(i, j): -float(durations[i]) for i in range(I)}
        minute_expr2[nx+j] = -1.0
        add(minute_expr2, -np.inf, -secs)
        pm_expr = {xidx(i, j): float(margin[i]) for i in range(I)}
        pm_expr[nx+R+j] = -1.0
        add(pm_expr, -np.inf, pm)
        pm_expr2 = {xidx(i, j): -float(margin[i]) for i in range(I)}
        pm_expr2[nx+R+j] = -1.0
        add(pm_expr2, -np.inf, -pm)

    rr, cc, vv = [], [], []
    for ri, vals in enumerate(rows):
        for ci, val in vals.items():
            rr.append(ri); cc.append(ci); vv.append(val)
    A = sparse.csr_matrix((vv, (rr, cc)), shape=(len(rows), nvar))
    res = milp(c, integrality=integrality, bounds=Bounds(lb, ub),
               constraints=LinearConstraint(A, np.array(lows), np.array(highs)),
               options={"time_limit": time_limit, "mip_rel_gap": 0.0})
    if res.x is None:
        raise RuntimeError(f"team {team_id} MILP failed status={res.status}: {res.message}")
    xv = res.x[:nx].reshape(I, R)
    lineups = [tuple(sorted(players[xv[i] > .5].tolist())) for i in range(I)]
    if any(len(z) != 5 for z in lineups):
        raise RuntimeError(f"team {team_id}: non-five-player solution")
    return TeamSolution(team_id, players, boundaries, lineups,
                        float(res.fun), int(res.status))


def solve_game(actions_frame: pd.DataFrame, official: pd.DataFrame,
               home_id: int, away_id: int, time_limit: float = 15.0):
    actions = resolve_substitutions_from_official_roster(
        prepare_actions(actions_frame), official)
    boundaries = game_boundaries(actions)
    home = solve_team(actions, official, home_id, boundaries, True, time_limit)
    away = solve_team(actions, official, away_id, boundaries, False, time_limit)
    margin = _score_margin_by_interval(actions, boundaries)
    return actions, boundaries, home, away, margin


def solve_game_with_seed(actions_frame: pd.DataFrame, official: pd.DataFrame,
                         home_id: int, away_id: int, seed_stints: pd.DataFrame,
                         time_limit: float = 15.0):
    actions = resolve_substitutions_from_official_roster(
        prepare_actions(actions_frame), official)
    boundaries = game_boundaries(actions)
    try:
        home = solve_team(actions, official, home_id, boundaries, True, time_limit,
                          seed_stints=seed_stints, seed_hard=False,
                          transition_scope=False)
        away = solve_team(actions, official, away_id, boundaries, False, time_limit,
                          seed_stints=seed_stints, seed_hard=False,
                          transition_scope=False)
    except RuntimeError:
        # Some historical feeds contain internally contradictory duplicated or
        # reversed substitution rows. Retry without hard directions; the
        # downstream evidence audit still quarantines any violated transition.
        try:
            home = solve_team(actions, official, home_id, boundaries, True, time_limit,
                              seed_stints=seed_stints, seed_hard=False,
                              transition_scope=False,
                              enforce_resolved_transitions=False)
            away = solve_team(actions, official, away_id, boundaries, False, time_limit,
                              seed_stints=seed_stints, seed_hard=False,
                              transition_scope=False,
                              enforce_resolved_transitions=False)
        except RuntimeError:
            # Last diagnostic tier: permit contradictory action attribution to
            # solve, but acceptance still requires zero action-presence errors.
            home = solve_team(actions, official, home_id, boundaries, True, time_limit,
                              seed_stints=seed_stints, seed_hard=False,
                              transition_scope=False,
                              enforce_resolved_transitions=False,
                              enforce_action_presence=False)
            away = solve_team(actions, official, away_id, boundaries, False, time_limit,
                              seed_stints=seed_stints, seed_hard=False,
                              transition_scope=False,
                              enforce_resolved_transitions=False,
                              enforce_action_presence=False)
    margin = _score_margin_by_interval(actions, boundaries)
    return actions, boundaries, home, away, margin
