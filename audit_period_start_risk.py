from __future__ import annotations

import argparse
import random
from pathlib import Path

import pandas as pd

import src.ingest as ingest_module
from src.ingest import get_boxscore_players, get_playbyplay_actions
from src.onoff import _sort_actions_precise


def _to_mmddyyyy(iso_date: str) -> str:
    d = pd.to_datetime(iso_date, errors="coerce")
    if pd.isna(d):
        return ""
    return d.strftime("%m/%d/%Y")


def _norm_action_type(value: object) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _lineup_from_stint(row: pd.Series, team_side: str) -> set[int]:
    cols = [f"{team_side}_p{i}" for i in range(1, 6)]
    lineup: set[int] = set()
    for col in cols:
        try:
            pid = int(row[col])
        except Exception:
            continue
        if pid > 0:
            lineup.add(pid)
    return lineup


def _first_sub_batch(actions: list[dict], period: int, team_id: int) -> list[dict]:
    team_actions = []
    for action in actions:
        try:
            action_period = int(action.get("period", 0) or 0)
            action_team = int(action.get("teamId", 0) or 0)
        except Exception:
            continue
        if action_period != period or action_team != team_id:
            continue
        if _norm_action_type(action.get("actionType")) != "substitution":
            continue
        team_actions.append(action)
    if not team_actions:
        return []
    first_clock = team_actions[0].get("clock")
    return [a for a in team_actions if a.get("clock") == first_clock]


def _compute_internal_risk_rows(stints: pd.DataFrame, onoff: pd.DataFrame) -> pd.DataFrame:
    game_meta = (
        stints.sort_values(["game_id", "stint_index"])
        .groupby("game_id", as_index=False)
        .first()[["game_id", "date", "home_id", "away_id"]]
    )
    periods = (
        stints.groupby(["game_id", "start_period"], as_index=False)
        .first()[["game_id", "start_period", "home_p1", "home_p2", "home_p3", "home_p4", "home_p5", "away_p1", "away_p2", "away_p3", "away_p4", "away_p5"]]
    )

    onoff = onoff.copy()
    onoff["game_id"] = onoff["game_id"].astype(str).str.lstrip("0")
    stints = stints.copy()
    stints["game_id"] = stints["game_id"].astype(str).str.lstrip("0")
    game_meta["game_id"] = game_meta["game_id"].astype(str).str.lstrip("0")
    periods["game_id"] = periods["game_id"].astype(str).str.lstrip("0")

    game_meta = game_meta.sort_values(["date", "game_id"]).reset_index(drop=True)
    risk_rows: list[dict] = []
    for meta in game_meta.itertuples(index=False):
        game_id = str(meta.game_id)
        date = str(meta.date)
        per_game_stints = periods[periods["game_id"] == game_id]
        per_game_onoff = onoff[onoff["game_id"] == game_id]
        duplicate_name_rows = int(
            per_game_onoff.groupby(["team_id", "player_name"]).size().gt(1).sum()
        )

        team_minute_targets = 48.0
        try:
            max_period = int(stints.loc[stints["game_id"] == game_id, "end_period"].max())
        except Exception:
            max_period = 4
        overtime_periods = max(0, max_period - 4)
        if max_period > 4:
            team_minute_targets += 5.0 * (max_period - 4)
        minute_total_error = 0.0
        for team_id in [int(meta.home_id), int(meta.away_id)]:
            team_minutes = pd.to_numeric(
                per_game_onoff.loc[per_game_onoff["team_id"] == team_id, "minutes_on"],
                errors="coerce",
            ).sum()
            minute_total_error += abs(team_minutes - team_minute_targets)

        risk_score = duplicate_name_rows
        if minute_total_error > 1.0:
            risk_score += 1
        if overtime_periods > 0:
            risk_score += 1
        risk_rows.append(
            {
                "game_id": game_id,
                "date": date,
                "home_id": int(meta.home_id),
                "away_id": int(meta.away_id),
                "duplicate_name_rows": duplicate_name_rows,
                "overtime_periods": overtime_periods,
                "minute_total_error": round(float(minute_total_error), 4),
                "risk_score": int(risk_score),
            }
        )
    return pd.DataFrame(risk_rows)


def _sample_games(risk_df: pd.DataFrame, sample_per_bucket: int, seed: int) -> pd.DataFrame:
    rng = random.Random(seed)
    risk_df = risk_df.copy()
    risk_df["risk_bucket"] = "low"
    risk_df.loc[risk_df["risk_score"] == 1, "risk_bucket"] = "medium"
    risk_df.loc[risk_df["risk_score"] >= 2, "risk_bucket"] = "high"

    sample_parts: list[pd.DataFrame] = []
    for bucket in ["high", "medium", "low"]:
        bucket_df = risk_df[risk_df["risk_bucket"] == bucket].copy()
        if bucket_df.empty:
            continue
        game_ids = bucket_df["game_id"].tolist()
        rng.shuffle(game_ids)
        chosen = set(game_ids[:sample_per_bucket])
        sample_parts.append(bucket_df[bucket_df["game_id"].isin(chosen)])
    if not sample_parts:
        return pd.DataFrame(columns=risk_df.columns.tolist() + ["risk_bucket"])
    return pd.concat(sample_parts, ignore_index=True).sort_values(
        ["risk_bucket", "date", "game_id"],
        ascending=[True, True, True],
    )


def _compute_period_start_diagnostics(game_id: str, date: str, stints: pd.DataFrame) -> dict[str, object]:
    mmddyyyy = _to_mmddyyyy(date)
    if not mmddyyyy:
        return {
            "checked_batches": 0,
            "sub_out_violations": 0,
            "sub_in_violations": 0,
            "violation_details": "",
        }
    try:
        actions = _sort_actions_precise(get_playbyplay_actions(game_id, mmddyyyy))
    except Exception:
        actions = []

    per_game_stints = (
        stints[stints["game_id"] == game_id]
        .groupby(["game_id", "start_period"], as_index=False)
        .first()
    )
    if per_game_stints.empty:
        return {
            "checked_batches": 0,
            "sub_out_violations": 0,
            "sub_in_violations": 0,
            "violation_details": "",
        }
    home_id = int(per_game_stints.iloc[0]["home_id"])
    away_id = int(per_game_stints.iloc[0]["away_id"])
    sub_out_violations = 0
    sub_in_violations = 0
    checked_batches = 0
    violation_details: list[str] = []

    for period_row in per_game_stints.itertuples(index=False):
        period = int(period_row.start_period)
        if period <= 1:
            continue
        lineups = {
            home_id: _lineup_from_stint(pd.Series(period_row._asdict()), "home"),
            away_id: _lineup_from_stint(pd.Series(period_row._asdict()), "away"),
        }
        for team_id in [home_id, away_id]:
            batch = _first_sub_batch(actions, period, team_id)
            if not batch:
                continue
            checked_batches += 1
            opening = lineups[team_id]
            for action in batch:
                try:
                    pid = int(action.get("personId", 0) or 0)
                except Exception:
                    pid = 0
                if pid <= 0:
                    continue
                subtype = str(action.get("subType") or "").lower()
                player_name = str(action.get("playerName") or action.get("playerNameI") or pid)
                if subtype == "out" and pid not in opening:
                    sub_out_violations += 1
                    violation_details.append(f"P{period} T{team_id} out:{player_name}@{action.get('clock')}")
                elif subtype == "in" and pid in opening:
                    sub_in_violations += 1
                    violation_details.append(f"P{period} T{team_id} in:{player_name}@{action.get('clock')}")
    return {
        "checked_batches": checked_batches,
        "sub_out_violations": sub_out_violations,
        "sub_in_violations": sub_in_violations,
        "violation_details": " | ".join(violation_details[:12]),
    }


def _validate_sample(sample_df: pd.DataFrame, onoff: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    row_checks: list[dict] = []
    game_checks: list[dict] = []

    for row in sample_df.itertuples(index=False):
        game_id = str(row.game_id)
        mmddyyyy = _to_mmddyyyy(str(row.date))
        if not mmddyyyy:
            continue
        try:
            official = get_boxscore_players(game_id.zfill(10), mmddyyyy)
        except Exception as e:
            game_checks.append(
                {
                    "game_id": game_id,
                    "date": row.date,
                    "risk_bucket": row.risk_bucket,
                    "risk_score": row.risk_score,
                    "checked_batches": 0,
                    "sub_out_violations": 0,
                    "sub_in_violations": 0,
                    "violation_details": "",
                    "validation_error": repr(e),
                }
            )
            continue

        ours = onoff[onoff["game_id"] == game_id].copy()
        if ours.empty or official.empty:
            continue

        official = official[["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "PLUS_MINUS", "MINUTES"]].copy()
        official = official.rename(
            columns={
                "PLAYER_ID": "player_id",
                "PLAYER_NAME": "player_name_official",
                "TEAM_ID": "team_id_official",
                "PLUS_MINUS": "plus_minus_official",
                "MINUTES": "minutes_official",
            }
        )
        merged = ours.merge(official, on="player_id", how="left")
        merged["plus_minus_diff"] = pd.to_numeric(merged["on_diff"], errors="coerce") - pd.to_numeric(
            merged["plus_minus_official"], errors="coerce"
        )
        merged["minutes_diff"] = pd.to_numeric(merged["minutes_on"], errors="coerce") - pd.to_numeric(
            merged["minutes_official"], errors="coerce"
        )

        for merged_row in merged.itertuples(index=False):
            row_checks.append(
                {
                    "game_id": game_id,
                    "date": row.date,
                    "risk_bucket": row.risk_bucket,
                    "risk_score": row.risk_score,
                    "player_id": merged_row.player_id,
                    "player_name": merged_row.player_name,
                    "minutes_on": merged_row.minutes_on,
                    "minutes_official": merged_row.minutes_official,
                    "minutes_diff": merged_row.minutes_diff,
                    "on_diff": merged_row.on_diff,
                    "plus_minus_official": merged_row.plus_minus_official,
                    "plus_minus_diff": merged_row.plus_minus_diff,
                }
            )

        diagnostics = _compute_period_start_diagnostics(game_id, str(row.date), stints=STINTS_DF)
        game_checks.append(
            {
                "game_id": game_id,
                "date": row.date,
                "risk_bucket": row.risk_bucket,
                "risk_score": row.risk_score,
                "checked_batches": diagnostics["checked_batches"],
                "sub_out_violations": diagnostics["sub_out_violations"],
                "sub_in_violations": diagnostics["sub_in_violations"],
                "duplicate_name_rows": row.duplicate_name_rows,
                "overtime_periods": row.overtime_periods,
                "minute_total_error_internal": row.minute_total_error,
                "max_abs_pm_diff": pd.to_numeric(merged["plus_minus_diff"], errors="coerce").abs().max(),
                "sum_abs_pm_diff": pd.to_numeric(merged["plus_minus_diff"], errors="coerce").abs().sum(),
                "max_abs_min_diff": pd.to_numeric(merged["minutes_diff"], errors="coerce").abs().max(),
                "sum_abs_min_diff": pd.to_numeric(merged["minutes_diff"], errors="coerce").abs().sum(),
                "violation_details": diagnostics["violation_details"],
                "validation_error": "",
            }
        )

    return pd.DataFrame(game_checks), pd.DataFrame(row_checks)


STINTS_DF = pd.DataFrame()


def main() -> None:
    parser = argparse.ArgumentParser(description="Risk-weighted audit of period-start lineup accuracy.")
    parser.add_argument("--onoff-path", default="data/adjusted_onoff_historical_pbp.csv")
    parser.add_argument("--stints-path", default="data/stints_historical_pbp.csv")
    parser.add_argument("--pbp-dir", required=True, help="Directory containing local nbastatsv3_<year>.csv files")
    parser.add_argument("--sample-per-bucket", type=int, default=5)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--out-game-risk", default="data/period_start_risk_games.csv")
    parser.add_argument("--out-game-audit", default="data/period_start_risk_audit_games.csv")
    parser.add_argument("--out-row-audit", default="data/period_start_risk_audit_rows.csv")
    args = parser.parse_args()

    ingest_module.LOCAL_PBP_DIR = Path(args.pbp_dir)
    ingest_module._local_pbp_cache.clear()
    ingest_module._local_team_alias_cache.clear()

    onoff = pd.read_csv(args.onoff_path, dtype={"game_id": str}, low_memory=False)
    stints = pd.read_csv(args.stints_path, dtype={"game_id": str}, low_memory=False)
    onoff["game_id"] = onoff["game_id"].astype(str).str.lstrip("0")
    stints["game_id"] = stints["game_id"].astype(str).str.lstrip("0")
    global STINTS_DF
    STINTS_DF = stints

    risk_df = _compute_internal_risk_rows(stints=stints, onoff=onoff)
    risk_df = risk_df.sort_values(["risk_score", "date", "game_id"], ascending=[False, True, True])
    Path(args.out_game_risk).parent.mkdir(parents=True, exist_ok=True)
    risk_df.to_csv(args.out_game_risk, index=False)

    sample_df = _sample_games(risk_df, sample_per_bucket=args.sample_per_bucket, seed=args.seed)
    game_audit_df, row_audit_df = _validate_sample(sample_df, onoff)
    game_audit_df.to_csv(args.out_game_audit, index=False)
    row_audit_df.to_csv(args.out_row_audit, index=False)

    print(f"Wrote risk scores: {args.out_game_risk} rows={len(risk_df)}")
    if not risk_df.empty:
        print("Risk buckets:")
        bucket_counts = (
            sample_df.groupby("risk_bucket")["game_id"].nunique().to_dict() if not sample_df.empty else {}
        )
        print(bucket_counts)
    print(f"Wrote sampled game audit: {args.out_game_audit} rows={len(game_audit_df)}")
    print(f"Wrote sampled row audit: {args.out_row_audit} rows={len(row_audit_df)}")
    if not game_audit_df.empty:
        summary = (
            game_audit_df.groupby("risk_bucket")[["max_abs_pm_diff", "max_abs_min_diff"]]
            .agg(["mean", "max"])
            .round(4)
        )
        print(summary.to_string())


if __name__ == "__main__":
    main()
