#!/usr/bin/env python3
"""Build like-for-like playoff player-line indexes at Q1, halftime, and Q3.

The inputs remain untouched.  Browser-ready shards are written beneath
``data/game_similarity/checkpoints`` and a reproducible audit report is written
to ``outputs/game_similarity``.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from build_game_similarity_index import (
    AST,
    BLK,
    DATE,
    FG2A,
    FG3A,
    FGA,
    FTA,
    GAME_ID,
    MIN,
    NAME,
    OPP,
    PF,
    PID,
    PTS,
    REB,
    SCHEMA,
    SEASON,
    STL,
    TEAM,
    TOV,
    chunk_rows,
    number,
)


ROOT = Path(__file__).resolve().parents[1]
PBP_DIR = ROOT / "data" / "historical_pbp"
STINT_PATH = ROOT / "data" / "stints_playoffs_rebuilt.csv"
CHUNK_DIR = ROOT / "data" / "player_game_playoff_chunks"
OUTPUT_DIR = ROOT / "data" / "game_similarity" / "checkpoints"
AUDIT_PATH = ROOT / "outputs" / "game_similarity" / "playoff_checkpoint_audit.json"

CHECKPOINTS = {1: ("q1", "End Q1"), 2: ("q2", "Halftime"), 3: ("q3", "End Q3")}
MINUTE_BANDS = [(start, start + 5) for start in range(5, 45, 5)] + [(45, 1000)]
SIM_STATS = [
    "min", "pts", "reb", "ast", "stl", "blk", "tov", "fga", "fg3a",
    "fg2a", "fg3_pct", "fg2_pct", "fta", "ft_pct", "ts_game",
]
STAT_KEYS = ("pts", "reb", "ast", "stl", "blk", "tov", "fga", "fg3a", "fg2a", "fg3m", "fg2m", "ftm", "fta")
FINAL_FIELDS = {
    "pts": PTS, "reb": REB, "ast": AST, "stl": STL, "blk": BLK,
    "tov": TOV, "fga": FGA, "fg3a": FG3A, "fg2a": FG2A, "fta": FTA,
}


def player_id(value: object) -> int | None:
    try:
        result = int(float(value))
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def event_value(row: object, column: str) -> object:
    if isinstance(row, pd.Series):
        return row.get(column)
    return getattr(row, column, None)


def event_description(row: object) -> str:
    parts = []
    for column in ("HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"):
        value = event_value(row, column)
        if pd.notna(value) and str(value).strip():
            parts.append(str(value))
    return " ".join(parts).upper()


def add_event(lines: dict[int, dict[str, float]], event: object) -> None:
    event_type = int(event_value(event, "EVENTMSGTYPE"))
    player1, player2, player3 = (player_id(event_value(event, f"PLAYER{i}_ID")) for i in (1, 2, 3))
    desc = event_description(event)
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


def checkpoint_minutes(stints: pd.DataFrame, elapsed: float) -> dict[int, float]:
    seconds: dict[int, float] = defaultdict(float)
    lineup_columns = [f"home_p{i}" for i in range(1, 6)] + [f"away_p{i}" for i in range(1, 6)]
    for stint in stints.itertuples(index=False):
        start, end = float(stint.start_elapsed), float(stint.end_elapsed)
        overlap = max(0.0, min(end, elapsed) - start)
        if overlap <= 0:
            continue
        for column in lineup_columns:
            pid = player_id(getattr(stint, column))
            if pid:
                seconds[pid] += overlap
    return {pid: round(value / 60, 3) for pid, value in seconds.items()}


def rates(line: dict[str, float]) -> tuple[float | None, float | None, float | None, float | None]:
    fg3_pct = round(line["fg3m"] / line["fg3a"], 3) if line["fg3a"] else None
    fg2_pct = round(line["fg2m"] / line["fg2a"], 3) if line["fg2a"] else None
    ft_pct = round(line["ftm"] / line["fta"], 3) if line["fta"] else None
    denominator = 2 * (line["fga"] + 0.44 * line["fta"])
    ts = round(line["pts"] / denominator, 3) if denominator else None
    return fg3_pct, fg2_pct, ft_pct, ts


def score_at_period(rows: pd.DataFrame, period: int) -> tuple[int, int]:
    score_rows = rows[(rows["PERIOD"] == period) & rows["SCORE"].notna()]
    if score_rows.empty:
        return 0, 0
    away, home = str(score_rows.iloc[-1]["SCORE"]).split("-")
    return int(away.strip()), int(home.strip())


def compact_snapshot(
    final_row: list[object], line: dict[str, float], minutes: float,
    team_score: int, opp_score: int,
) -> list[object]:
    fg3_pct, fg2_pct, ft_pct, ts = rates(line)
    win_loss = "W" if team_score > opp_score else "L" if team_score < opp_score else "T"
    return [
        final_row[NAME], final_row[PID], final_row[DATE], final_row[SEASON], final_row[OPP], round(minutes, 3),
        line["pts"], line["reb"], line["ast"], line["stl"], line["blk"], line["tov"],
        line["fga"], line["fg3a"], line["fg2a"], fg3_pct, fg2_pct, line["fta"], ft_pct, ts,
        None, None, float(team_score), float(opp_score), win_loss, str(final_row[GAME_ID]),
    ]


def moments(rows: list[list[object]]) -> tuple[list[float], list[float]]:
    positions = {name: index for index, name in enumerate(SCHEMA)}
    means, stds = [], []
    for stat in SIM_STATS:
        values = [float(row[positions[stat]]) for row in rows if row[positions[stat]] is not None]
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / len(values)
        means.append(mean)
        stds.append(math.sqrt(variance) or 1.0)
    return means, stds


def load_final_rows() -> dict[str, dict[int, list[object]]]:
    result: dict[str, dict[int, list[object]]] = defaultdict(dict)
    for path in sorted(CHUNK_DIR.glob("*.js")):
        for row in chunk_rows(path):
            if len(row) <= PF or number(row[MIN]) < 0.5:
                continue
            pid = player_id(row[PID])
            if pid:
                result[str(row[GAME_ID])][pid] = row
    return result


def final_is_consistent(lines: dict[int, dict[str, float]], final_rows: dict[int, list[object]]) -> tuple[bool, int]:
    mismatches = 0
    for pid, final_row in final_rows.items():
        line = lines[pid]
        for key, column in FINAL_FIELDS.items():
            if abs(line[key] - number(final_row[column])) > 1e-6:
                mismatches += 1
    return mismatches == 0, mismatches


def write_shards(key: str, label: str, rows: list[list[object]], generated: str, game_count: int) -> None:
    checkpoint_dir = OUTPUT_DIR / key
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    means, stds = moments(rows)
    shards = []
    for low, high in MINUTE_BANDS:
        filename = f"{low:02d}_{'plus' if high >= 1000 else f'{high:02d}'}.json"
        band_rows = [row for row in rows if low <= float(row[5]) < high]
        (checkpoint_dir / filename).write_text(
            json.dumps(band_rows, ensure_ascii=False, separators=(",", ":")), encoding="utf-8",
        )
        shards.append({"file": filename, "min": low, "max": high, "count": len(band_rows)})
    manifest = {
        "generated": generated, "checkpoint": key, "label": label, "schema": SCHEMA,
        "sim_stats": SIM_STATS, "means": means, "stds": stds,
        "search_shards": shards, "game_count": game_count, "player_line_count": len(rows),
        "scope": "NBA playoffs; only games whose reconstructed final player lines exactly match the published final box score",
    }
    (checkpoint_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, separators=(",", ":")), encoding="utf-8",
    )


def build() -> dict[str, object]:
    final_by_game = load_final_rows()
    stints = pd.read_csv(STINT_PATH, low_memory=False)
    stints["game_id"] = stints["game_id"].astype(str)
    stint_by_game = {game_id: group.sort_values("stint_index") for game_id, group in stints.groupby("game_id")}
    snapshots: dict[str, list[list[object]]] = {key: [] for key, _ in CHECKPOINTS.values()}
    audit: dict[str, object] = {
        "input_pbp_games": 0, "eligible_games": 0, "validated_games": 0,
        "missing_stints": [], "missing_final_box": [], "stat_mismatch_games": 0,
        "stat_mismatch_fields": 0,
    }

    for path in sorted(PBP_DIR.glob("nbastats_po_*.csv")):
        pbp = pd.read_csv(path, low_memory=False)
        pbp["GAME_ID"] = pbp["GAME_ID"].astype(str)
        for game_id, game_rows in pbp.groupby("GAME_ID", sort=False):
            audit["input_pbp_games"] += 1
            if game_id not in stint_by_game:
                audit["missing_stints"].append(game_id)
                continue
            if game_id not in final_by_game:
                audit["missing_final_box"].append(game_id)
                continue
            audit["eligible_games"] += 1
            game_rows = game_rows.sort_values("EVENTNUM")
            game_stints = stint_by_game[game_id]
            final_rows = final_by_game[game_id]
            home_id, away_id = int(game_stints.iloc[0]["home_id"]), int(game_stints.iloc[0]["away_id"])
            team_abbr: dict[int, str] = {}
            for suffix in ("1", "2", "3"):
                ids = game_rows[f"PLAYER{suffix}_TEAM_ID"]
                abbrs = game_rows[f"PLAYER{suffix}_TEAM_ABBREVIATION"]
                for team_id, abbr in zip(ids, abbrs):
                    parsed = player_id(team_id)
                    if parsed and pd.notna(abbr):
                        team_abbr[parsed] = str(abbr)
            if home_id not in team_abbr or away_id not in team_abbr:
                audit["missing_final_box"].append(game_id)
                continue

            lines: dict[int, dict[str, float]] = defaultdict(lambda: {key: 0.0 for key in STAT_KEYS})
            pending: dict[str, list[list[object]]] = {}
            for period in sorted(int(value) for value in game_rows["PERIOD"].dropna().unique()):
                for event in game_rows[game_rows["PERIOD"] == period].itertuples(index=False):
                    add_event(lines, event)
                if period not in CHECKPOINTS:
                    continue
                key, _ = CHECKPOINTS[period]
                away_score, home_score = score_at_period(game_rows, period)
                minutes = checkpoint_minutes(game_stints, period * 720)
                period_rows = []
                for pid, played in minutes.items():
                    if pid not in final_rows or played < 5:
                        continue
                    final_row = final_rows[pid]
                    is_home = str(final_row[TEAM]) == team_abbr[home_id]
                    team_score, opp_score = (home_score, away_score) if is_home else (away_score, home_score)
                    period_rows.append(compact_snapshot(final_row, lines[pid], played, team_score, opp_score))
                pending[key] = period_rows

            consistent, mismatch_count = final_is_consistent(lines, final_rows)
            if not consistent:
                audit["stat_mismatch_games"] += 1
                audit["stat_mismatch_fields"] += mismatch_count
                continue
            audit["validated_games"] += 1
            for key, rows in pending.items():
                snapshots[key].extend(rows)

    generated = datetime.now(timezone.utc).isoformat()
    for _, (key, label) in CHECKPOINTS.items():
        snapshots[key].sort(key=lambda row: (str(row[2]), str(row[25]), str(row[0])))
        write_shards(key, label, snapshots[key], generated, int(audit["validated_games"]))
    audit["generated"] = generated
    audit["checkpoint_player_lines"] = {key: len(rows) for key, rows in snapshots.items()}
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_PATH.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    return audit


def main() -> None:
    audit = build()
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
