#!/usr/bin/env python3
"""Build a quarter-by-quarter replay fixture from historical NBA play-by-play."""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

from build_game_similarity_index import compact


ROOT = Path(__file__).resolve().parents[1]
GAME_ID = "42300113"  # Joel Embiid's 50-point playoff game, 2024-04-25
PBP_PATH = ROOT / "data" / "historical_pbp" / "nbastats_po_2023.csv"
STINT_PATH = ROOT / "data" / "stints_playoffs.csv"
GAME_CHUNK = ROOT / "data" / "player_game_playoff_chunks" / "2023_24.js"
OUTPUT = ROOT / "data" / "live_replay" / f"{GAME_ID}.json"


def chunk_rows(path: Path) -> list[list[object]]:
    match = re.search(r"=\s*(\[.*\])\s*;\s*$", path.read_text(encoding="utf-8"), re.S)
    if not match:
        raise ValueError(f"Could not parse {path}")
    return json.loads(match.group(1))


def pid(value: object) -> int | None:
    try:
        result = int(float(value))
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def description(row: pd.Series) -> str:
    for column in ("HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"):
        value = row.get(column)
        if pd.notna(value) and str(value).strip():
            return str(value)
    return ""


def checkpoint_minutes(stints: pd.DataFrame, checkpoint: float) -> dict[int, float]:
    seconds: dict[int, float] = defaultdict(float)
    lineup_columns = [f"home_p{i}" for i in range(1, 6)] + [f"away_p{i}" for i in range(1, 6)]
    for _, stint in stints.iterrows():
        start = float(stint["start_elapsed"])
        end = float(stint["end_elapsed"])
        overlap = max(0.0, min(end, checkpoint) - start)
        if overlap <= 0:
            continue
        for column in lineup_columns:
            player_id = pid(stint[column])
            if player_id:
                seconds[player_id] += overlap
    return {player_id: round(value / 60, 3) for player_id, value in seconds.items()}


def rates(line: dict[str, float]) -> tuple[float | None, float | None, float | None, float | None]:
    fg3_pct = round(line["fg3m"] / line["fg3a"], 3) if line["fg3a"] else None
    fg2_pct = round(line["fg2m"] / line["fg2a"], 3) if line["fg2a"] else None
    ft_pct = round(line["ftm"] / line["fta"], 3) if line["fta"] else None
    denominator = 2 * (line["fga"] + 0.44 * line["fta"])
    ts = round(line["pts"] / denominator, 3) if denominator else None
    return fg3_pct, fg2_pct, ft_pct, ts


def compact_checkpoint(
    final_row: list[object], line: dict[str, float], minutes: float, home_score: int, away_score: int,
    home_abbr: str,
) -> list[object]:
    fg3_pct, fg2_pct, ft_pct, ts = rates(line)
    team_is_home = str(final_row[5]) == home_abbr
    team_score, opp_score = (home_score, away_score) if team_is_home else (away_score, home_score)
    wl = "W" if team_score > opp_score else "L"
    return [
        final_row[4], final_row[3], final_row[0], final_row[1], final_row[6], round(minutes, 3),
        line["pts"], line["reb"], line["ast"], line["stl"], line["blk"], line["tov"],
        line["fga"], line["fg3a"], line["fg2a"], fg3_pct, fg2_pct, line["fta"], ft_pct, ts,
        None, None, float(team_score), float(opp_score), wl, str(final_row[2]),
    ]


def build() -> dict[str, object]:
    source_rows = [row for row in chunk_rows(GAME_CHUNK) if str(row[2]) == GAME_ID]
    final_by_pid = {int(row[3]): row for row in source_rows}
    pbp = pd.read_csv(PBP_PATH, low_memory=False)
    pbp = pbp[pbp["GAME_ID"].astype(str) == GAME_ID].sort_values("EVENTNUM")
    stints = pd.read_csv(STINT_PATH, low_memory=False)
    stints = stints[stints["game_id"].astype(str) == GAME_ID].sort_values("stint_index")
    if pbp.empty or stints.empty or not source_rows:
        raise RuntimeError("Replay inputs are incomplete")

    home_id = int(stints.iloc[0]["home_id"])
    away_id = int(stints.iloc[0]["away_id"])
    team_abbr: dict[int, str] = {}
    for _, row in pbp.iterrows():
        for suffix in ("1", "2", "3"):
            team = pid(row.get(f"PLAYER{suffix}_TEAM_ID"))
            abbr = row.get(f"PLAYER{suffix}_TEAM_ABBREVIATION")
            if team and pd.notna(abbr):
                team_abbr[team] = str(abbr)
    home_abbr, away_abbr = team_abbr[home_id], team_abbr[away_id]

    lines: dict[int, dict[str, float]] = defaultdict(
        lambda: {key: 0.0 for key in ("pts", "reb", "ast", "stl", "blk", "tov", "fga", "fg3a", "fg2a", "fg3m", "fg2m", "ftm", "fta")}
    )
    checkpoints: list[dict[str, object]] = []
    labels = {1: "End Q1", 2: "Halftime", 3: "End Q3"}

    for period in range(1, 4):
        period_rows = pbp[pbp["PERIOD"] == period]
        for _, event in period_rows.iterrows():
            event_type = int(event["EVENTMSGTYPE"])
            player1, player2, player3 = (pid(event.get(f"PLAYER{i}_ID")) for i in (1, 2, 3))
            desc = description(event).upper()
            is_three = "3PT" in desc
            if event_type == 1 and player1:
                lines[player1]["fga"] += 1
                lines[player1]["fg3a" if is_three else "fg2a"] += 1
                lines[player1]["fg3m" if is_three else "fg2m"] += 1
                lines[player1]["pts"] += 3 if is_three else 2
                if player2:
                    lines[player2]["ast"] += 1
            elif event_type == 2 and player1:
                lines[player1]["fga"] += 1
                lines[player1]["fg3a" if is_three else "fg2a"] += 1
                if player3:
                    lines[player3]["blk"] += 1
            elif event_type == 3 and player1:
                lines[player1]["fta"] += 1
                if "MISS" not in desc:
                    lines[player1]["ftm"] += 1
                    lines[player1]["pts"] += 1
            elif event_type == 4 and player1:
                lines[player1]["reb"] += 1
            elif event_type == 5 and player1:
                lines[player1]["tov"] += 1
                if player2 and "STEAL" in desc:
                    lines[player2]["stl"] += 1

        score_rows = period_rows[period_rows["SCORE"].notna()]
        away_score, home_score = (0, 0)
        if not score_rows.empty:
            away_text, home_text = str(score_rows.iloc[-1]["SCORE"]).split("-")
            away_score, home_score = int(away_text.strip()), int(home_text.strip())
        minutes = checkpoint_minutes(stints, period * 720)
        player_rows = [
            compact_checkpoint(final_by_pid[player_id], lines[player_id], played, home_score, away_score, home_abbr)
            for player_id, played in minutes.items()
            if player_id in final_by_pid and played >= 0.5
        ]
        checkpoints.append({
            "key": f"q{period}", "label": labels[period], "period": period,
            "home_score": home_score, "away_score": away_score, "players": player_rows,
        })

    final_rows = [compact(row) for row in source_rows if float(row[13] or 0) >= 0.5]
    final_home = int(next(row[7] for row in source_rows if row[5] == home_abbr))
    final_away = int(next(row[7] for row in source_rows if row[5] == away_abbr))
    checkpoints.append({
        "key": "final", "label": "Final", "period": 4,
        "home_score": final_home, "away_score": final_away, "players": final_rows,
    })
    return {
        "mode": "replay", "game_id": GAME_ID, "date": source_rows[0][0],
        "home": {"team": home_abbr, "score": final_home},
        "away": {"team": away_abbr, "score": final_away},
        "checkpoints": checkpoints,
        "note": "Historical replay reconstructed from official play-by-play; partial lines are compared with final historical lines at similar minutes.",
    }


def main() -> None:
    payload = build()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {len(payload['checkpoints'])} checkpoints to {OUTPUT}")


if __name__ == "__main__":
    main()
