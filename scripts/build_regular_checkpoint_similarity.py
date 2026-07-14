#!/usr/bin/env python3
"""Build combined regular-season and playoff Q1/halftime/Q3 indexes.

The 18-million-row regular-season play-by-play source is first sorted into a
small-column derived cache.  Games are then reconstructed and admitted only
when their final counting statistics and player minutes reconcile with the
published final box score.  Existing playoff-only shards are captured as a
reproducible seed before the combined browser shards replace them.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

from build_game_similarity_index import GAME_ID, MIN, NAME, PID, SCHEMA, TEAM, chunk_rows, number
from build_playoff_checkpoint_similarity import (
    CHECKPOINTS,
    FINAL_FIELDS,
    SIM_STATS,
    STAT_KEYS,
    checkpoint_minutes,
    compact_snapshot,
    final_is_consistent,
    player_id,
)


ROOT = Path(__file__).resolve().parents[1]
RAW_PBP = ROOT / "data" / "kaggle_temp" / "PlayByPlay.parquet"
SORTED_PBP = ROOT / "derived" / "contextual_causal" / "game_similarity" / "regular_pbp_sorted.parquet"
DUCKDB_TEMP = ROOT / "derived" / "contextual_causal" / "game_similarity" / "duckdb_temp"
STINT_PATH = ROOT / "data" / "stints_historical_pbp.csv"
CHUNK_DIR = ROOT / "data" / "player_game_chunks"
PLAYOFF_SEED_DIR = ROOT / "derived" / "contextual_causal" / "game_similarity" / "playoff_seed"
OUTPUT_DIR = ROOT / "data" / "game_similarity" / "checkpoints"
AUDIT_PATH = ROOT / "outputs" / "game_similarity" / "regular_checkpoint_audit.json"
MINUTE_BANDS = [(minute, minute + 1) for minute in range(5, 40)] + [(40, 1000)]
PBP_COLUMNS = [
    "gameId", "period", "orderNumber", "actionNumber", "actionType", "shotResult",
    "personId", "teamId", "teamTricode", "scoreHome", "scoreAway", "assistPersonId",
    "stealPersonId", "blockPersonId",
]


def sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def ensure_sorted_cache(rebuild: bool) -> None:
    if SORTED_PBP.exists() and not rebuild:
        return
    SORTED_PBP.parent.mkdir(parents=True, exist_ok=True)
    DUCKDB_TEMP.mkdir(parents=True, exist_ok=True)
    temp_output = SORTED_PBP.with_suffix(".building.parquet")
    if temp_output.exists():
        temp_output.unlink()
    selected = ",".join(f'"{column}"' for column in PBP_COLUMNS)
    connection = duckdb.connect()
    connection.execute("SET memory_limit='8GB'")
    connection.execute("SET threads=4")
    connection.execute(f"SET temp_directory='{sql_path(DUCKDB_TEMP)}'")
    connection.execute(
        f"""COPY (
            SELECT {selected}
            FROM read_parquet('{sql_path(RAW_PBP)}')
            WHERE TRY_CAST(gameId AS BIGINT) BETWEEN 21900000 AND 22599999
            ORDER BY gameId, TRY_CAST(orderNumber AS BIGINT), TRY_CAST(actionNumber AS BIGINT)
        ) TO '{sql_path(temp_output)}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)"""
    )
    connection.close()
    temp_output.replace(SORTED_PBP)


def load_final_rows() -> dict[str, dict[int, list[object]]]:
    result: dict[str, dict[int, list[object]]] = defaultdict(dict)
    for path in sorted(CHUNK_DIR.glob("*.js")):
        for row in chunk_rows(path):
            if len(row) < 26 or number(row[MIN]) < 0.5:
                continue
            pid = player_id(row[PID])
            game_id = str(row[GAME_ID])
            if pid and game_id.startswith("2"):
                result[game_id][pid] = row
    return result


def add_regular_event(lines: dict[int, dict[str, float]], event: object) -> None:
    action_type = str(getattr(event, "actionType", "")).lower()
    pid = player_id(getattr(event, "personId", None))
    made = str(getattr(event, "shotResult", "")).lower() == "made"
    if action_type in {"2pt", "3pt"} and pid:
        is_three = action_type == "3pt"
        lines[pid]["fga"] += 1
        lines[pid]["fg3a" if is_three else "fg2a"] += 1
        if made:
            lines[pid]["fg3m" if is_three else "fg2m"] += 1
            lines[pid]["pts"] += 3 if is_three else 2
            assist = player_id(getattr(event, "assistPersonId", None))
            if assist:
                lines[assist]["ast"] += 1
        blocker = player_id(getattr(event, "blockPersonId", None))
        if blocker:
            lines[blocker]["blk"] += 1
    elif action_type == "freethrow" and pid:
        lines[pid]["fta"] += 1
        if made:
            lines[pid]["ftm"] += 1
            lines[pid]["pts"] += 1
    elif action_type == "rebound" and pid:
        lines[pid]["reb"] += 1
    elif action_type == "turnover" and pid:
        lines[pid]["tov"] += 1
        stealer = player_id(getattr(event, "stealPersonId", None))
        if stealer:
            lines[stealer]["stl"] += 1


def score_at_period(rows: pd.DataFrame, period: int) -> tuple[int, int]:
    period_rows = rows[rows["period"] == period]
    home = period_rows["scoreHome"].dropna()
    away = period_rows["scoreAway"].dropna()
    return (int(away.iloc[-1]) if not away.empty else 0, int(home.iloc[-1]) if not home.empty else 0)


def minutes_are_consistent(stints: pd.DataFrame, final_rows: dict[int, list[object]]) -> tuple[bool, float]:
    reconstructed = checkpoint_minutes(stints, float(stints["end_elapsed"].max()) + 1)
    largest_gap = 0.0
    for pid, row in final_rows.items():
        largest_gap = max(largest_gap, abs(reconstructed.get(pid, 0.0) - number(row[MIN])))
    return largest_gap <= 0.75, round(largest_gap, 3)


def process_game(
    game_id: str, rows: pd.DataFrame, stints: pd.DataFrame, final_rows: dict[int, list[object]],
) -> tuple[dict[str, list[list[object]]] | None, str | None, int, float]:
    home_id, away_id = int(stints.iloc[0]["home_id"]), int(stints.iloc[0]["away_id"])
    team_abbr: dict[int, str] = {}
    for team_id, abbreviation in zip(rows["teamId"], rows["teamTricode"]):
        parsed = player_id(team_id)
        if parsed and pd.notna(abbreviation):
            team_abbr[parsed] = str(abbreviation)
    if home_id not in team_abbr or away_id not in team_abbr:
        return None, "team_mapping", 0, 0.0

    lines: dict[int, dict[str, float]] = defaultdict(lambda: {key: 0.0 for key in STAT_KEYS})
    pending: dict[str, list[list[object]]] = {}
    for period in sorted(int(value) for value in rows["period"].dropna().unique()):
        period_rows = rows[rows["period"] == period]
        for event in period_rows.itertuples(index=False):
            add_regular_event(lines, event)
        if period not in CHECKPOINTS:
            continue
        key, _ = CHECKPOINTS[period]
        away_score, home_score = score_at_period(rows, period)
        minutes = checkpoint_minutes(stints, period * 720)
        snapshots = []
        for pid, played in minutes.items():
            if pid not in final_rows or played < 5:
                continue
            final_row = final_rows[pid]
            is_home = str(final_row[TEAM]) == team_abbr[home_id]
            team_score, opp_score = (home_score, away_score) if is_home else (away_score, home_score)
            snapshots.append(compact_snapshot(final_row, lines[pid], played, team_score, opp_score))
        pending[key] = snapshots

    stats_consistent, mismatch_count = final_is_consistent(lines, final_rows)
    if not stats_consistent:
        return None, "stats", mismatch_count, 0.0
    minutes_consistent, largest_gap = minutes_are_consistent(stints, final_rows)
    if not minutes_consistent:
        return None, "minutes", 0, largest_gap
    return pending, None, 0, largest_gap


def iter_sorted_games() -> tuple[str, pd.DataFrame]:
    parquet = pq.ParquetFile(SORTED_PBP)
    carry: pd.DataFrame | None = None
    for row_group in range(parquet.metadata.num_row_groups):
        frame = parquet.read_row_group(row_group, columns=PBP_COLUMNS).to_pandas()
        if carry is not None:
            frame = pd.concat([carry, frame], ignore_index=True)
        game_ids = frame["gameId"].astype(str)
        last_id = game_ids.iloc[-1]
        complete = frame[game_ids != last_id]
        carry = frame[game_ids == last_id].copy()
        for game_id, rows in complete.groupby(complete["gameId"].astype(str), sort=False):
            yield str(game_id), rows
    if carry is not None and not carry.empty:
        yield str(carry.iloc[0]["gameId"]), carry


def capture_playoff_seed() -> None:
    PLAYOFF_SEED_DIR.mkdir(parents=True, exist_ok=True)
    for _, (key, _) in CHECKPOINTS.items():
        seed_path = PLAYOFF_SEED_DIR / f"{key}.json"
        if seed_path.exists():
            continue
        manifest_path = OUTPUT_DIR / key / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if "NBA playoffs" not in str(manifest.get("scope", "")):
            raise RuntimeError("Cannot capture playoff seed from an already-combined checkpoint index")
        rows: list[list[object]] = []
        for shard in manifest["search_shards"]:
            rows.extend(json.loads((OUTPUT_DIR / key / shard["file"]).read_text(encoding="utf-8")))
        seed_path.write_text(json.dumps(rows, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


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


def write_combined_shards(
    regular: dict[str, list[list[object]]], audit: dict[str, object], generated: str,
) -> None:
    for _, (key, label) in CHECKPOINTS.items():
        playoff_rows = json.loads((PLAYOFF_SEED_DIR / f"{key}.json").read_text(encoding="utf-8"))
        rows = playoff_rows + regular[key]
        rows.sort(key=lambda row: (str(row[2]), str(row[25]), str(row[0])))
        means, stds = moments(rows)
        checkpoint_dir = OUTPUT_DIR / key
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        for stale in checkpoint_dir.glob("*.json"):
            stale.unlink()
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
            "sim_stats": SIM_STATS, "means": means, "stds": stds, "search_shards": shards,
            "game_count": int(audit["validated_regular_games"]) + 2311,
            "regular_game_count": int(audit["validated_regular_games"]), "playoff_game_count": 2311,
            "regular_season_coverage": "2019-20 through 2025-26",
            "player_line_count": len(rows), "minute_band_width": 1,
            "scope": "NBA regular season and playoffs; reconstructed games must reconcile with published final player statistics and minutes",
        }
        (checkpoint_dir / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, separators=(",", ":")), encoding="utf-8",
        )


def build(limit_games: int | None, sample_only: bool) -> dict[str, object]:
    capture_playoff_seed()
    final_by_game = load_final_rows()
    stints = pd.read_csv(STINT_PATH, low_memory=False)
    stints["game_id"] = stints["game_id"].astype(str)
    stint_by_game = {game_id: group.sort_values("stint_index") for game_id, group in stints.groupby("game_id")}
    snapshots: dict[str, list[list[object]]] = {key: [] for key, _ in CHECKPOINTS.values()}
    audit: dict[str, object] = {
        "pbp_games_seen": 0, "eligible_regular_games": 0, "validated_regular_games": 0,
        "unsupported_legacy_schema_games": 0,
        "missing_stints": 0, "missing_final_box": 0, "team_mapping_failures": 0,
        "stat_mismatch_games": 0, "stat_mismatch_fields": 0, "minute_mismatch_games": 0,
        "largest_accepted_minute_gap": 0.0, "largest_rejected_minute_gap": 0.0,
        "failure_examples": [],
    }
    for game_id, rows in iter_sorted_games():
        audit["pbp_games_seen"] += 1
        action_types = {str(value).lower() for value in rows["actionType"].dropna().unique()}
        if not action_types.intersection({"2pt", "3pt"}):
            audit["unsupported_legacy_schema_games"] += 1
            continue
        if game_id not in stint_by_game:
            audit["missing_stints"] += 1
            continue
        if game_id not in final_by_game:
            audit["missing_final_box"] += 1
            continue
        audit["eligible_regular_games"] += 1
        result, failure, mismatch_count, minute_gap = process_game(
            game_id, rows, stint_by_game[game_id], final_by_game[game_id],
        )
        if failure:
            if failure == "stats":
                audit["stat_mismatch_games"] += 1
                audit["stat_mismatch_fields"] += mismatch_count
            elif failure == "minutes":
                audit["minute_mismatch_games"] += 1
                audit["largest_rejected_minute_gap"] = max(float(audit["largest_rejected_minute_gap"]), minute_gap)
            else:
                audit["team_mapping_failures"] += 1
            if len(audit["failure_examples"]) < 20:
                audit["failure_examples"].append({"game_id": game_id, "reason": failure, "minute_gap": minute_gap})
        else:
            audit["validated_regular_games"] += 1
            audit["largest_accepted_minute_gap"] = max(float(audit["largest_accepted_minute_gap"]), minute_gap)
            for key, player_rows in result.items():
                snapshots[key].extend(player_rows)
        if limit_games and int(audit["eligible_regular_games"]) >= limit_games:
            break

    generated = datetime.now(timezone.utc).isoformat()
    audit["generated"] = generated
    audit["regular_season_coverage"] = "2019-20 through 2025-26"
    audit["regular_checkpoint_player_lines"] = {key: len(rows) for key, rows in snapshots.items()}
    audit["sample_only"] = sample_only
    audit["sorted_cache"] = str(SORTED_PBP)
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    output_audit = AUDIT_PATH if not sample_only else AUDIT_PATH.with_name("regular_checkpoint_sample_audit.json")
    output_audit.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    if not sample_only:
        write_combined_shards(snapshots, audit, generated)
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild-sorted-cache", action="store_true")
    parser.add_argument("--limit-games", type=int)
    parser.add_argument("--sample-only", action="store_true")
    args = parser.parse_args()
    if args.sample_only and not args.limit_games:
        parser.error("--sample-only requires --limit-games")
    ensure_sorted_cache(args.rebuild_sorted_cache)
    print(json.dumps(build(args.limit_games, args.sample_only), indent=2))


if __name__ == "__main__":
    main()
