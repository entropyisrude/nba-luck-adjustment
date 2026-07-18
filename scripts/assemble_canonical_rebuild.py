"""Assemble the audited whole-game canonical candidate without publishing it."""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
QA_PATH = ROOT / "outputs" / "contextual_causal" / "canonical_game_integrity.parquet"
DB_PATH = ROOT / "data" / "nba_analytics.duckdb"
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"


def norm(values: pd.Series) -> pd.Series:
    return values.astype(str).str.split(".").str[0].str.lstrip("0").replace("", "0")


def accepted_ids(path: Path) -> set[str]:
    audit = pd.read_csv(path, dtype={"game_id": str})
    ok = audit.accepted.astype(str).str.lower().eq("true")
    return set(norm(audit.loc[ok, "game_id"]))


def load_tier(audit_pattern: str, stint_pattern: str, source: str) -> pd.DataFrame:
    frames = []
    for audit_path in sorted(REBUILD.glob(audit_pattern)):
        suffix = audit_path.name.removeprefix("audit_").removesuffix(".csv")
        stint_path = REBUILD / f"stints_{suffix}.parquet"
        if not stint_path.exists():
            continue
        ids = accepted_ids(audit_path)
        if not ids:
            continue
        stints = pd.read_parquet(stint_path)
        stints["game_id"] = norm(stints.game_id)
        stints = stints.loc[stints.game_id.isin(ids)].copy()
        stints["canonical_source"] = source
        frames.append(stints)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main() -> None:
    qa = pd.read_parquet(QA_PATH)
    qa["game_id"] = norm(qa.game_id)
    original_ids = set(qa.loc[qa.canonical_grade_a, "game_id"])

    con = duckdb.connect(str(DB_PATH), read_only=True)
    original = con.execute("SELECT * FROM lineup_stint_facts").df()
    totals = con.execute("""
        SELECT ltrim(game_id, '0') game_id, home_away,
               max(team_pts_actual) team_pts_actual
        FROM player_game_facts GROUP BY 1, 2
    """).df()
    con.close()
    original["game_id"] = norm(original.game_id)
    original = original.loc[original.game_id.isin(original_ids)].copy()
    original["canonical_source"] = "original_grade_a"

    tiers = [
        (load_tier("audit_full_*.csv", "stints_full_*.parquet", "full_rebuild"),
         "full_rebuild"),
        (load_tier("audit_retry_period_*.csv", "stints_retry_period_*.parquet",
                   "period_boundary_retry"), "period_boundary_retry"),
        (load_tier("audit_retry_isolated.csv", "stints_retry_isolated.parquet",
                   "isolated_period_retry"), "isolated_period_retry"),
        (load_tier("audit_repair_gamerotation_cached.csv",
                   "stints_repair_gamerotation_cached.parquet",
                   "official_gamerotation"), "official_gamerotation"),
        (load_tier("audit_repair_partial_substitution.csv",
                   "stints_repair_partial_substitution.parquet",
                   "inferred_partial_substitution"), "inferred_partial_substitution"),
        (load_tier("audit_repair_external_box.csv",
                   "stints_repair_external_box.parquet",
                   "external_exact_box"), "external_exact_box"),
        (load_tier("audit_repair_kaggle_minutes.csv",
                   "stints_repair_kaggle_minutes.parquet",
                   "historical_box_minutes"), "historical_box_minutes"),
        (load_tier("audit_repair_multi_partial.csv",
                   "stints_repair_multi_partial.parquet",
                   "inferred_multi_partial_substitution"),
         "inferred_multi_partial_substitution"),
        (load_tier("audit_authoritative_gamerotation.csv",
                   "stints_authoritative_gamerotation.parquet",
                   "authoritative_official_gamerotation"),
         "authoritative_official_gamerotation"),
        (load_tier("audit_github_rotation_*.csv",
                   "stints_github_rotation_*.parquet",
                   "pinned_official_gamerotation_archive"),
         "pinned_official_gamerotation_archive"),
        (load_tier("audit_structural_*.csv", "stints_structural_*.parquet",
                   "original_structural_rebuild"), "original_structural_rebuild"),
    ]

    selected = original
    manifest_rows = [{"game_id": gid, "canonical_source": "original_grade_a"}
                     for gid in sorted(original_ids)]
    chosen = set(original_ids)
    for frame, source in tiers:
        if frame.empty:
            continue
        if source in {"original_structural_rebuild",
                      "pinned_official_gamerotation_archive",
                      "authoritative_official_gamerotation"}:
            replacement_ids = set(frame.game_id)
            selected = selected.loc[~selected.game_id.isin(replacement_ids)].copy()
            manifest_rows = [row for row in manifest_rows
                             if row["game_id"] not in replacement_ids]
            chosen -= replacement_ids
        ids = set(frame.game_id) - chosen
        if not ids:
            continue
        frame = frame.loc[frame.game_id.isin(ids)].copy()
        selected = pd.concat([selected, frame], ignore_index=True, sort=False)
        manifest_rows.extend({"game_id": gid, "canonical_source": source}
                             for gid in sorted(ids))
        chosen |= ids

    selected["stint_index"] = (selected.sort_values(["game_id", "start_elapsed"])
                               .groupby("game_id").cumcount())
    manifest = pd.DataFrame(manifest_rows).drop_duplicates("game_id")

    # Independent structural and score audit of the assembled whole-game file.
    total_wide = totals.pivot(index="game_id", columns="home_away",
                              values="team_pts_actual")
    checks = []
    for gid, game in selected.groupby("game_id"):
        game = game.sort_values("start_elapsed")
        starts = pd.to_numeric(game.start_elapsed, errors="coerce").to_numpy(float)
        ends = pd.to_numeric(game.end_elapsed, errors="coerce").to_numpy(float)
        seconds = pd.to_numeric(game.seconds, errors="coerce").to_numpy(float)
        adjacency = (float(np.abs(starts[1:] - ends[:-1]).sum())
                     if len(game) > 1 else np.inf)
        lineup_ok = True
        for side in ("home", "away"):
            cols = [f"{side}_p{k}" for k in range(1, 6)]
            lineup_ok &= bool(game[cols].notna().all(axis=None))
            lineup_ok &= bool(game[cols].nunique(axis=1).eq(5).all())
        official_home = float(total_wide.loc[gid, "home"]) if gid in total_wide.index else np.nan
        official_away = float(total_wide.loc[gid, "away"]) if gid in total_wide.index else np.nan
        score_error = (abs(pd.to_numeric(game.home_pts, errors="coerce").sum() - official_home)
                       + abs(pd.to_numeric(game.away_pts, errors="coerce").sum() - official_away))
        structural_ok = bool(
            len(game) > 0 and abs(starts[0]) < 1e-4
            and abs(seconds.sum() - (ends[-1] - starts[0])) < 1e-3
            and adjacency < 1e-3 and lineup_ok
            and np.isfinite(score_error) and score_error < 0.5)
        checks.append({"game_id": gid, "canonical_source": game.canonical_source.iloc[0],
                       "stints": len(game), "adjacency_error": adjacency,
                       "score_error": score_error, "lineup_ok": lineup_ok,
                       "structural_ok": structural_ok})
    final_audit = pd.DataFrame(checks)

    structural_rejects = set(final_audit.loc[~final_audit.structural_ok, "game_id"])
    final_audit["selected_for_candidate"] = ~final_audit.game_id.isin(structural_rejects)
    if structural_rejects:
        selected = selected.loc[~selected.game_id.isin(structural_rejects)].copy()
        manifest = manifest.loc[~manifest.game_id.isin(structural_rejects)].copy()
        chosen -= structural_rejects

    all_games = set(qa.game_id)
    unresolved = qa.loc[qa.game_id.isin(all_games - chosen),
                        ["game_id", "season_year"]].drop_duplicates()
    selected.to_parquet(REBUILD / "canonical_stints_candidate.parquet", index=False)
    manifest.to_csv(REBUILD / "canonical_game_source_manifest.csv", index=False)
    final_audit.to_csv(REBUILD / "canonical_candidate_final_audit.csv", index=False)
    unresolved.to_csv(REBUILD / "canonical_unresolved_games.csv", index=False)

    print(manifest.canonical_source.value_counts().to_string())
    print(f"selected games: {len(chosen)} / {len(all_games)}")
    print(f"unresolved games: {len(unresolved)}")
    retained_failures = (~final_audit.structural_ok & final_audit.selected_for_candidate).sum()
    print(f"rejected by final structural audit: {len(structural_rejects)}")
    print(f"structural failures retained: {retained_failures}")


if __name__ == "__main__":
    main()
