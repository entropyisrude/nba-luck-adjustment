"""Freeze the effective quarantine after period-boundary retries.

This is a read-only consolidation step: it never changes production data.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
QA = ROOT / "outputs" / "contextual_causal" / "canonical_game_integrity.parquet"
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
ROTATION = ROOT / "data" / "stats_cache" / "gamerotation"
EXTERNAL_BOX = ROOT / "data" / "player_boxscore_stats_external_2010_2024.csv"
QUEUES = REBUILD / "remaining_quarantine_queues"


def _norm(value: object) -> str:
    text = str(value).split(".")[0]
    return text.lstrip("0") or "0"


def _accepted(frame: pd.DataFrame) -> pd.Series:
    return frame["accepted"].astype(str).str.lower().eq("true")


def main() -> None:
    qa = pd.read_parquet(QA, columns=["game_id", "season_year"])
    qa["game_id"] = qa.game_id.map(_norm)
    season_by_game = qa.drop_duplicates("game_id").set_index("game_id").season_year

    full_parts = []
    for path in sorted(REBUILD.glob("audit_full_*.csv")):
        part = pd.read_csv(path)
        part["game_id"] = part.game_id.map(_norm)
        part["season_year"] = int(path.stem.rsplit("_", 1)[-1])
        full_parts.append(part)
    if not full_parts:
        raise SystemExit("no full-pass audits found")
    full = pd.concat(full_parts, ignore_index=True)
    first_rejects = full.loc[~_accepted(full)].copy()

    retry_parts = []
    for path in sorted(REBUILD.glob("audit_retry_period_*.csv")):
        part = pd.read_csv(path)
        part["game_id"] = part.game_id.map(_norm)
        part["season_year"] = int(path.stem.rsplit("_", 1)[-1])
        retry_parts.append(part)
    retry = pd.concat(retry_parts, ignore_index=True) if retry_parts else pd.DataFrame()
    retry = retry.drop_duplicates("game_id", keep="last")
    retry_lookup = retry.set_index("game_id") if not retry.empty else pd.DataFrame()

    rows = []
    for first in first_rejects.to_dict("records"):
        gid = _norm(first["game_id"])
        row = dict(first)
        row["game_id"] = gid
        row["season_year"] = int(row.get("season_year") or season_by_game.get(gid))
        row["retry_processed"] = gid in retry_lookup.index
        if row["retry_processed"]:
            rr = retry_lookup.loc[gid]
            if isinstance(rr, pd.DataFrame):
                rr = rr.iloc[-1]
            row.update({f"retry_{key}": value for key, value in rr.items()
                        if key not in ("game_id", "season_year")})
            row["effective_accepted"] = str(rr.get("accepted", "")).lower() == "true"
        else:
            row["effective_accepted"] = False
        rows.append(row)

    overlay = pd.DataFrame(rows)
    remaining = overlay.loc[~overlay.effective_accepted].copy()
    cached = {_norm(path.stem) for path in ROTATION.glob("*.json")}
    remaining["cached_gamerotation"] = remaining.game_id.isin(cached)

    def metric(name: str) -> pd.Series:
        retry_name = f"retry_{name}"
        values = remaining.get(retry_name)
        if values is None:
            values = remaining.get(name, pd.Series(index=remaining.index, dtype=float))
        return pd.to_numeric(values, errors="coerce")

    remaining["aggregate_good_local_bad"] = (
        metric("score_error").fillna(999).lt(0.5)
        & metric("max_seconds_error").fillna(999).le(75)
        & metric("max_pm_error").fillna(999).lt(0.5)
        & ((metric("unsupported_player_changes").fillna(0) > 0)
           | (metric("recorded_transition_violations").fillna(0) > 0)
           | (metric("action_presence_violations").fillna(0) > 0)))
    remaining["local_good_aggregate_bad"] = (
        metric("unsupported_player_changes").fillna(999).eq(0)
        & metric("recorded_transition_violations").fillna(999).eq(0)
        & metric("action_presence_violations").fillna(999).eq(0)
        & ((metric("score_error").fillna(0) >= 0.5)
           | (metric("max_seconds_error").fillna(0) > 75)
           | (metric("max_pm_error").fillna(0) >= 0.5)))

    QUEUES.mkdir(parents=True, exist_ok=True)
    overlay.to_csv(REBUILD / "period_boundary_effective_overlay.csv", index=False)
    remaining.to_csv(REBUILD / "remaining_quarantine.csv", index=False)
    remaining[["game_id", "season_year"]].to_csv(
        QUEUES / "all_remaining.csv", index=False)
    remaining.loc[remaining.cached_gamerotation, ["game_id", "season_year"]].to_csv(
        QUEUES / "cached_gamerotation.csv", index=False)
    remaining.loc[remaining.aggregate_good_local_bad, ["game_id", "season_year"]].to_csv(
        QUEUES / "retroactive_action_candidates.csv", index=False)
    remaining.loc[remaining.local_good_aggregate_bad, ["game_id", "season_year"]].to_csv(
        QUEUES / "aggregate_crosscheck_candidates.csv", index=False)
    external_ids: set[str] = set()
    if EXTERNAL_BOX.exists():
        external = pd.read_csv(EXTERNAL_BOX, usecols=["game_id"])
        external_ids = set(external.game_id.map(_norm))
    remaining.loc[
        remaining.local_good_aggregate_bad & remaining.game_id.isin(external_ids),
        ["game_id", "season_year"],
    ].to_csv(QUEUES / "external_box_crosscheck.csv", index=False)

    print(f"first rejects: {len(first_rejects)}")
    print(f"retry processed: {overlay.retry_processed.sum()} / {len(overlay)}")
    print(f"retry accepted: {overlay.effective_accepted.sum()}")
    print(f"effective quarantine: {len(remaining)}")
    print(f"cached GameRotation: {remaining.cached_gamerotation.sum()}")
    print(f"retroactive-action candidates: {remaining.aggregate_good_local_bad.sum()}")
    print(f"aggregate cross-check candidates: {remaining.local_good_aggregate_bad.sum()}")
    print("external exact-box cross-check: "
          f"{(remaining.local_good_aggregate_bad & remaining.game_id.isin(external_ids)).sum()}")


if __name__ == "__main__":
    main()
