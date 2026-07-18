"""Build a versioned multiple-imputation layer for non-canonical lineup games.

The canonical candidate is never modified.  Rejected whole-game reconstructions
are scored by a measurement model trained on analogous games whose final
canonical timeline is known.  Each imputation selects one coherent candidate
per unresolved game; incompatible stints are never spliced within a game.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor


ROOT = Path(__file__).resolve().parents[1]
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
OUT = ROOT / "derived" / "contextual_causal" / "probabilistic_lineup_salvage"
REPORT = ROOT / "outputs" / "contextual_causal"
P_COLS = [f"{side}_p{k}" for side in ("home", "away") for k in range(1, 6)]
FEATURES = [
    "log_score_error", "log_seconds_error", "log_pm_error",
    "log_unsupported", "log_transition", "log_action", "seed_agreement",
    "coverage_ok", "calibrated", "source_gamerotation", "source_replay",
    "source_external_box", "source_structural",
]


def norm(values: pd.Series) -> pd.Series:
    return (values.astype(str).str.split(".").str[0].str.lstrip("0")
            .replace("", "0"))


def candidate_paths() -> list[Path]:
    patterns = [
        "stints_full_*.parquet", "stints_retry_period_*.parquet",
        "stints_retry_isolated.parquet", "stints_repair_*.parquet",
        "stints_github_rotation_*.parquet",
        "stints_authoritative_gamerotation.parquet",
        "stints_structural_*.parquet",
    ]
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(sorted(REBUILD.glob(pattern)))
    return list(dict.fromkeys(paths))


def audit_for(stint_path: Path) -> pd.DataFrame:
    suffix = stint_path.stem.removeprefix("stints_")
    path = REBUILD / f"audit_{suffix}.csv"
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path, dtype={"game_id": str})
    frame["game_id"] = norm(frame.game_id)
    return frame.drop_duplicates("game_id", keep="last").set_index("game_id")


def lineup_signature(game: pd.DataFrame) -> str:
    cols = ["start_elapsed", "end_elapsed", *P_COLS]
    values = pd.util.hash_pandas_object(
        game.sort_values(["start_elapsed", "end_elapsed"])[cols], index=False
    ).to_numpy()
    return hashlib.sha1(values.tobytes()).hexdigest()[:16]


def structurally_complete(game: pd.DataFrame) -> bool:
    game = game.sort_values("start_elapsed")
    if game.empty or game[P_COLS].isna().any(axis=None):
        return False
    for side in ("home", "away"):
        if not game[[f"{side}_p{k}" for k in range(1, 6)]].nunique(axis=1).eq(5).all():
            return False
    starts = pd.to_numeric(game.start_elapsed, errors="coerce").to_numpy(float)
    ends = pd.to_numeric(game.end_elapsed, errors="coerce").to_numpy(float)
    return bool(np.isfinite(starts).all() and np.isfinite(ends).all()
                and abs(starts[0]) < 1e-4 and np.all(ends > starts)
                and (len(game) == 1 or np.abs(starts[1:] - ends[:-1]).sum() < 1e-3))


def agreement(candidate: pd.DataFrame, truth: pd.DataFrame) -> float:
    """Duration-weighted fraction of the ten player slots matching truth."""
    c = candidate.sort_values("start_elapsed").reset_index(drop=True)
    t = truth.sort_values("start_elapsed").reset_index(drop=True)
    boundaries = np.unique(np.r_[c.start_elapsed, c.end_elapsed,
                                 t.start_elapsed, t.end_elapsed].astype(float))
    mids = (boundaries[:-1] + boundaries[1:]) / 2.0
    durations = boundaries[1:] - boundaries[:-1]
    ci = np.clip(np.searchsorted(c.start_elapsed.to_numpy(float), mids,
                                 side="right") - 1, 0, len(c) - 1)
    ti = np.clip(np.searchsorted(t.start_elapsed.to_numpy(float), mids,
                                 side="right") - 1, 0, len(t) - 1)
    matched = total = 0.0
    for duration, cidx, tidx in zip(durations, ci, ti):
        if duration <= 0:
            continue
        cr, tr = c.iloc[cidx], t.iloc[tidx]
        slots = 0
        for side in ("home", "away"):
            cs = {int(cr[f"{side}_p{k}"]) for k in range(1, 6)}
            ts = {int(tr[f"{side}_p{k}"]) for k in range(1, 6)}
            slots += len(cs & ts)
        matched += duration * slots
        total += duration * 10.0
    return matched / total if total else np.nan


def metric(row: pd.Series, name: str, default: float) -> float:
    value = pd.to_numeric(pd.Series([row.get(name, default)]), errors="coerce").iloc[0]
    return float(value) if pd.notna(value) else default


def feature_row(gid: str, source: str, sig: str, game: pd.DataFrame,
                audit: pd.DataFrame) -> dict:
    a = audit.loc[gid] if gid in audit.index else pd.Series(dtype=object)
    score = metric(a, "score_error", 100.0)
    seconds = metric(a, "max_seconds_error", 600.0)
    pm = metric(a, "max_pm_error", 20.0)
    unsupported = metric(a, "unsupported_player_changes", 20.0)
    transition = metric(a, "recorded_transition_violations", 20.0)
    action = metric(a, "action_presence_violations", 20.0)
    seed = metric(a, "seed_agreement", 0.0)
    text = source.lower()
    return {
        "game_id": gid, "source": source, "candidate_id": sig,
        "stints": len(game), "game_seconds": float(game.seconds.sum()),
        "log_score_error": np.log1p(max(score, 0.0)),
        "log_seconds_error": np.log1p(max(seconds, 0.0)),
        "log_pm_error": np.log1p(max(pm, 0.0)),
        "log_unsupported": np.log1p(max(unsupported, 0.0)),
        "log_transition": np.log1p(max(transition, 0.0)),
        "log_action": np.log1p(max(action, 0.0)),
        "seed_agreement": np.clip(seed, 0.0, 1.0),
        "coverage_ok": float(str(a.get("coverage_ok", False)).lower() == "true"),
        "calibrated": float(str(a.get("calibrated", False)).lower() == "true"),
        "source_gamerotation": float("rotation" in text),
        "source_replay": float("full_" in text or "retry_" in text),
        "source_external_box": float("external_box" in text or "kaggle" in text),
        "source_structural": float("structural" in text),
    }


def softmax(values: np.ndarray, temperature: float) -> np.ndarray:
    z = (values - np.max(values)) / temperature
    z = np.exp(np.clip(z, -50, 0))
    return z / z.sum()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    REPORT.mkdir(parents=True, exist_ok=True)
    unresolved = pd.read_csv(REBUILD / "canonical_unresolved_games.csv",
                             dtype={"game_id": str})
    unresolved["game_id"] = norm(unresolved.game_id)
    season_map = unresolved.set_index("game_id").season_year.to_dict()
    canonical = pd.read_parquet(REBUILD / "canonical_stints_candidate.parquet")
    canonical["game_id"] = norm(canonical.game_id)
    canonical_groups = {gid: game for gid, game in canonical.groupby("game_id")}
    target_ids = set(unresolved.game_id) | set(canonical_groups)

    metadata: list[dict] = []
    candidate_frames: dict[tuple[str, str], pd.DataFrame] = {}
    for path in candidate_paths():
        frame = pd.read_parquet(path)
        if frame.empty or "game_id" not in frame:
            continue
        frame["game_id"] = norm(frame.game_id)
        frame = frame[frame.game_id.isin(target_ids)]
        if frame.empty:
            continue
        audit = audit_for(path)
        for gid, game in frame.groupby("game_id"):
            game = game.sort_values(["start_elapsed", "end_elapsed"]).copy()
            if not structurally_complete(game):
                continue
            sig = lineup_signature(game)
            key = (gid, sig)
            row = feature_row(gid, path.stem, sig, game, audit)
            # If two repair tiers produced the same lineup, retain the version
            # with the strongest aggregate evidence and do not double its mass.
            loss = sum(row[x] for x in ("log_score_error", "log_seconds_error",
                                        "log_pm_error", "log_unsupported",
                                        "log_transition", "log_action"))
            prior = next((x for x in metadata
                          if x["game_id"] == gid and x["candidate_id"] == sig), None)
            if prior is not None:
                prior_loss = sum(prior[x] for x in (
                    "log_score_error", "log_seconds_error", "log_pm_error",
                    "log_unsupported", "log_transition", "log_action"))
                if loss >= prior_loss:
                    continue
                metadata.remove(prior)
            metadata.append(row)
            candidate_frames[key] = game

    meta = pd.DataFrame(metadata)
    canonical_sig = {gid: lineup_signature(game)
                     for gid, game in canonical_groups.items()}
    labeled = meta[meta.game_id.isin(canonical_groups)].copy()
    # Simulate quarantine: remove the exact canonical solution and ask the
    # model to rank only rejected/alternative timelines.
    labeled = labeled[labeled.candidate_id != labeled.game_id.map(canonical_sig)]
    labeled["agreement"] = [agreement(candidate_frames[(r.game_id, r.candidate_id)],
                                        canonical_groups[r.game_id])
                              for r in labeled.itertuples()]
    labeled = labeled.dropna(subset=["agreement"])

    # Season comes from the canonical source manifest's game facts.  NBA game
    # IDs encode season in positions 1:3, including the 1996 -> 296 convention.
    def infer_season(gid: str) -> int:
        text = str(gid).zfill(8)
        yy = int(text[1:3])
        return 1900 + yy if yy >= 46 else 2000 + yy
    labeled["season_year"] = labeled.game_id.map(infer_season)
    eligible_cutoffs = []
    for cutoff in sorted(labeled.season_year.unique()):
        n_train = int((labeled.season_year <= cutoff).sum())
        n_valid = int((labeled.season_year > cutoff).sum())
        if n_train >= 500 and n_valid >= 200:
            eligible_cutoffs.append(int(cutoff))
    if not eligible_cutoffs:
        raise RuntimeError("no chronological split has >=500 train and >=200 validation candidates")
    validation_cutoff = max(eligible_cutoffs)
    train = labeled[labeled.season_year <= validation_cutoff].copy()
    valid = labeled[labeled.season_year > validation_cutoff].copy()
    if len(train) < 500 or len(valid) < 200:
        raise RuntimeError(f"insufficient chronological labels: train={len(train)} valid={len(valid)}")
    model = HistGradientBoostingRegressor(
        loss="squared_error", max_iter=250, max_leaf_nodes=15,
        learning_rate=.05, l2_regularization=2.0, random_state=20260718)
    model.fit(train[FEATURES], train.agreement)
    valid["predicted_agreement"] = np.clip(model.predict(valid[FEATURES]), 0, 1)
    evidence_cols = ["log_score_error", "log_seconds_error", "log_pm_error",
                     "log_unsupported", "log_transition", "log_action"]
    valid["evidence_score"] = -valid[evidence_cols].sum(axis=1)

    # Calibrate the softmax temperature by game-level log loss for the candidate
    # with greatest actual agreement in the chronological validation block.
    temperatures = [.05, .10, .20, .50, 1.0, 2.0, 5.0]
    temp_rows = []
    for temp in temperatures:
        losses = []
        for _, game in valid.groupby("game_id"):
            p = softmax(game.evidence_score.to_numpy(), temp)
            winner = int(np.argmax(game.agreement.to_numpy()))
            losses.append(-np.log(max(p[winner], 1e-12)))
        temp_rows.append({"temperature": temp, "log_loss": float(np.mean(losses))})
    temp_table = pd.DataFrame(temp_rows)
    temperature = float(temp_table.loc[temp_table.log_loss.idxmin(), "temperature"])

    validation_rows = []
    validation_probabilities = {}
    for gid, game in valid.groupby("game_id"):
        p = softmax(game.evidence_score.to_numpy(), temperature)
        validation_probabilities.update(zip(game.index, p))
        best = int(np.argmax(game.predicted_agreement.to_numpy()))
        heuristic = int(np.argmin(game[["log_score_error", "log_seconds_error",
                                        "log_pm_error", "log_unsupported",
                                        "log_transition", "log_action"]].sum(axis=1).to_numpy()))
        validation_rows.append({
            "game_id": gid, "candidates": len(game),
            "learned_best_agreement": float(game.agreement.iloc[best]),
            "heuristic_best_agreement": float(game.agreement.iloc[heuristic]),
            "weighted_expected_agreement": float(np.dot(p, game.agreement)),
            "oracle_agreement": float(game.agreement.max()),
            "prediction_mae": float(np.mean(np.abs(
                game.predicted_agreement - game.agreement))),
        })
    validation = pd.DataFrame(validation_rows)
    valid["probability"] = pd.Series(validation_probabilities)
    valid.to_csv(REPORT / "probabilistic_salvage_validation_candidates.csv",
                 index=False)
    validation_bank = []
    for row in valid.itertuples():
        game = candidate_frames[(row.game_id, row.candidate_id)].copy()
        game["candidate_id"] = row.candidate_id
        game["candidate_probability"] = row.probability
        game["actual_canonical_agreement"] = row.agreement
        game["predicted_lineup_agreement"] = row.predicted_agreement
        game["uncertainty_source"] = row.source
        validation_bank.append(game)
    pd.concat(validation_bank, ignore_index=True).to_parquet(
        OUT / "validation_candidate_stint_bank.parquet", index=False)

    unresolved_meta = meta[meta.game_id.isin(set(unresolved.game_id))].copy()
    unresolved_meta["predicted_agreement"] = np.clip(
        model.predict(unresolved_meta[FEATURES]), 0, 1)
    unresolved_meta["evidence_score"] = -unresolved_meta[evidence_cols].sum(axis=1)
    probability = []
    entropy = []
    for gid, game in unresolved_meta.groupby("game_id"):
        p = softmax(game.evidence_score.to_numpy(), temperature)
        probability.extend(zip(game.index, p))
        h = float(-(p * np.log(np.maximum(p, 1e-12))).sum())
        entropy.extend((idx, h) for idx in game.index)
    unresolved_meta["probability"] = pd.Series(dict(probability))
    unresolved_meta["candidate_entropy"] = pd.Series(dict(entropy))
    unresolved_meta["season_year"] = unresolved_meta.game_id.map(season_map)
    unresolved_meta["candidate_count"] = unresolved_meta.groupby("game_id").game_id.transform("size")

    # Store each unique candidate once with its posterior mass.
    bank = []
    for row in unresolved_meta.itertuples():
        game = candidate_frames[(row.game_id, row.candidate_id)].copy()
        game["candidate_id"] = row.candidate_id
        game["candidate_probability"] = row.probability
        game["predicted_lineup_agreement"] = row.predicted_agreement
        game["uncertainty_source"] = row.source
        bank.append(game)
    bank_frame = pd.concat(bank, ignore_index=True)
    bank_frame.to_parquet(OUT / "candidate_stint_bank.parquet", index=False)

    # Reproducible whole-game draws.  The imputation id is explicit so RAPM can
    # be fit separately and combined using between-imputation variance.
    rng = np.random.default_rng(20260718)
    draws = []
    manifests = []
    for imp in range(20):
        for gid, game in unresolved_meta.groupby("game_id"):
            chosen = int(rng.choice(len(game), p=game.probability.to_numpy()))
            row = game.iloc[chosen]
            st = candidate_frames[(gid, row.candidate_id)].copy()
            st["imputation_id"] = imp
            st["candidate_id"] = row.candidate_id
            st["candidate_probability"] = row.probability
            st["predicted_lineup_agreement"] = row.predicted_agreement
            st["canonical_source"] = "probabilistic_lineup_salvage"
            draws.append(st)
            manifests.append({"imputation_id": imp, "game_id": gid,
                              "candidate_id": row.candidate_id,
                              "probability": row.probability,
                              "predicted_lineup_agreement": row.predicted_agreement})
    pd.concat(draws, ignore_index=True).to_parquet(
        OUT / "imputed_stints_20.parquet", index=False)
    pd.DataFrame(manifests).to_csv(OUT / "imputation_manifest_20.csv", index=False)
    unresolved_meta.to_csv(OUT / "candidate_probabilities.csv", index=False)
    validation.to_csv(REPORT / "probabilistic_salvage_validation_games.csv", index=False)
    temp_table.to_csv(REPORT / "probabilistic_salvage_temperature.csv", index=False)

    summary = {
        "canonical_games": int(canonical.game_id.nunique()),
        "unresolved_games": int(unresolved_meta.game_id.nunique()),
        "unresolved_candidates": int(len(unresolved_meta)),
        "games_with_multiple_candidates": int((unresolved_meta.groupby("game_id").size() > 1).sum()),
        "validation_games": int(len(validation)),
        "validation_candidate_rows": int(len(valid)),
        "validation_cutoff_season": validation_cutoff,
        "temperature": temperature,
        "probability_method": "transparent_evidence_score",
        "learned_best_agreement": float(validation.learned_best_agreement.mean()),
        "heuristic_best_agreement": float(validation.heuristic_best_agreement.mean()),
        "weighted_expected_agreement": float(validation.weighted_expected_agreement.mean()),
        "oracle_agreement": float(validation.oracle_agreement.mean()),
        "prediction_mae": float(validation.prediction_mae.mean()),
        "mean_unresolved_predicted_agreement": float(
            np.average(unresolved_meta.predicted_agreement,
                       weights=unresolved_meta.probability)),
        "single_candidate_games": int((unresolved_meta.groupby("game_id").size() == 1).sum()),
        "production_modified": False,
    }
    (REPORT / "probabilistic_salvage_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
