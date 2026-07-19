"""Stateful free-throw and midrange luck for the daily pipeline.

Expectations match the historical builders: career-to-date rates, strictly
prior games, shrunk by 40 FT attempts or 100 attempts within each midrange
distance band. Historical games use the committed component artifact exactly;
new games update a compact JSON state and cache their result for idempotency.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

FT_K = 40.0
MR_K = 100.0
FT_FINAL_VALUE = 0.86
MR_MAKE_VALUE = 1.707
DEFAULT_LEAGUE = {"ft": 0.757, "mr0": 0.415, "mr1": 0.405}


def load_state(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"version": 1, "league": DEFAULT_LEAGUE.copy(),
            "players": {}, "games": {}}


def save_state(state: dict[str, Any], path: Path) -> None:
    path.write_text(json.dumps(state, separators=(",", ":")), encoding="utf-8")


def historical_game_totals(path: Path) -> dict[str, dict[str, float]]:
    if not path.exists():
        return {}
    frame = pd.read_parquet(path)
    cols = ["ft_luck_home", "ft_luck_away", "mr_luck_home", "mr_luck_away"]
    frame["gid"] = frame.game_id.astype(str).str.lstrip("0")
    totals = frame.groupby("gid")[cols].sum()
    return {str(gid): {k: float(v) for k, v in row.items()}
            for gid, row in totals.to_dict("index").items()}


def _made(action: dict[str, Any]) -> bool:
    result = str(action.get("shotResult") or "").strip().lower()
    if result:
        return result == "made"
    return "miss" not in str(action.get("description") or "").lower()


def game_components(
    game_id: str,
    actions: list[dict[str, Any]],
    home_team_id: int,
    away_team_id: int,
    state: dict[str, Any],
    historical: dict[str, dict[str, float]],
) -> tuple[dict[str, float], dict[int, dict[str, float]]]:
    gid = str(game_id).lstrip("0")
    if gid in historical:
        return historical[gid], {}
    cached = state.setdefault("games", {}).get(gid)
    if cached is not None:
        components = {k: float(v) for k, v in cached["components"].items()}
        players = {int(k): v for k, v in cached.get("players", {}).items()}
        return components, players

    league = {**DEFAULT_LEAGUE, **state.get("league", {})}
    players_state = state.setdefault("players", {})
    components = {"ft_luck_home": 0.0, "ft_luck_away": 0.0,
                  "mr_luck_home": 0.0, "mr_luck_away": 0.0}
    player_luck: dict[int, dict[str, float]] = {}
    updates: dict[tuple[int, str], list[float]] = {}

    for action in actions:
        at = str(action.get("actionType") or "").strip().lower()
        desc = str(action.get("description") or "")
        try:
            pid = int(action.get("personId") or 0)
            team_id = int(action.get("teamId") or 0)
        except (TypeError, ValueError):
            continue
        if pid <= 0 or team_id not in (home_team_id, away_team_id):
            continue

        kind = ""
        value = 0.0
        if at in ("free throw", "freethrow"):
            kind = "ft"
            pos = re.search(r"(\d+) of (\d+)", desc)
            technical = "technical" in desc.lower()
            is_final = bool(pos and pos.group(1) == pos.group(2) and not technical)
            value = FT_FINAL_VALUE if is_final else 1.0
        else:
            is_two = at == "2pt" or (
                at in ("made shot", "missed shot") and "3pt" not in desc.lower())
            try:
                distance = float(action.get("shotDistance"))
            except (TypeError, ValueError):
                continue
            if not is_two or distance < 10.0 or distance > 23.99:
                continue
            kind = "mr0" if distance < 16.0 else "mr1"
            value = MR_MAKE_VALUE

        rec = players_state.get(str(pid), {})
        makes = float(rec.get(f"{kind}_m", 0.0))
        attempts = float(rec.get(f"{kind}_a", 0.0))
        k = FT_K if kind == "ft" else MR_K
        expectation = (makes + k * float(league[kind])) / (attempts + k)
        made = float(_made(action))
        luck = (made - expectation) * value
        side = "home" if team_id == home_team_id else "away"
        component = "ft" if kind == "ft" else "mr"
        components[f"{component}_luck_{side}"] += luck
        prec = player_luck.setdefault(pid, {"ft_luck": 0.0, "mr_luck": 0.0,
                                           "team_id": team_id,
                                           "player_name": str(action.get("playerName") or action.get("playerNameI") or "")})
        prec[f"{component}_luck"] += luck
        upd = updates.setdefault((pid, kind), [0.0, 0.0])
        upd[0] += made
        upd[1] += 1.0

    # Strictly prior-game expectations: apply all attempts only after scoring
    # the complete game.
    for (pid, kind), (makes, attempts) in updates.items():
        rec = players_state.setdefault(str(pid), {})
        rec[f"{kind}_m"] = float(rec.get(f"{kind}_m", 0.0)) + makes
        rec[f"{kind}_a"] = float(rec.get(f"{kind}_a", 0.0)) + attempts

    state["games"][gid] = {
        "components": components,
        "players": {str(k): v for k, v in player_luck.items()},
    }
    return components, player_luck
