"""Masked-game validation of omission, aggregate and imputed-lineup RAPM."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from test_probabilistic_salvage_rapm_sensitivity import (
    ACOLS, HCOLS, WINDOW, aggregate_design, fit, norm, stint_design)


ROOT = Path(__file__).resolve().parents[1]
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
SALVAGE = ROOT / "derived" / "contextual_causal" / "probabilistic_lineup_salvage"
REPORT = ROOT / "outputs" / "contextual_causal"


def aggregate_from_truth(truth: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for gid, game in truth.groupby("game_id"):
        game = game.sort_values("start_elapsed")
        date = pd.to_datetime(game.date.iloc[0])
        game_seconds = float(game.seconds.sum())
        poss = max(game_seconds / 24.0, .1)
        totals = {"home": float(game.home_pts_adj.sum()),
                  "away": float(game.away_pts_adj.sum())}
        exposure = {}
        for side, cols in (("home", HCOLS), ("away", ACOLS)):
            values = game[cols].to_numpy(int)
            seconds = game.seconds.to_numpy(float)
            for pid in np.unique(values):
                exposure[(side, int(pid))] = float(
                    seconds[(values == pid).any(axis=1)].sum() / game_seconds)
        for offense, defense in (("home", "away"), ("away", "home")):
            obs = f"{gid}_{offense}_offense"
            for role, side in (("offense", offense), ("defense", defense)):
                for (player_side, pid), share in exposure.items():
                    if player_side != side:
                        continue
                    rows.append({
                        "observation_id": obs, "game_id": gid, "date": date,
                        "role": role, "player_id": pid, "design_value": share,
                        "target_per_100": totals[offense] / poss * 100.0,
                        "possessions_proxy": poss,
                    })
    return pd.DataFrame(rows)


def main() -> None:
    canonical = pd.read_parquet(REBUILD / "canonical_stints_candidate.parquet")
    canonical["game_id"] = norm(canonical.game_id)
    canonical["date"] = pd.to_datetime(canonical.date)
    end = canonical.date.max()
    candidates = pd.read_csv(
        REPORT / "probabilistic_salvage_validation_candidates.csv",
        dtype={"game_id": str, "candidate_id": str})
    candidates["game_id"] = norm(candidates.game_id)
    bank = pd.read_parquet(SALVAGE / "validation_candidate_stint_bank.parquet")
    bank["game_id"] = norm(bank.game_id)
    bank["candidate_id"] = bank.candidate_id.astype(str)
    masked_ids = set(candidates.game_id)
    truth = canonical[canonical.game_id.isin(masked_ids)].copy()
    base = canonical[~canonical.game_id.isin(masked_ids)].copy()

    players = np.array(sorted(set(canonical[HCOLS+ACOLS].to_numpy(int).ravel())
                              | set(bank[HCOLS+ACOLS].to_numpy(int).ravel())), int)
    pidx = {p: i for i, p in enumerate(players)}; P = len(players)
    base_part = stint_design(base, pidx, end)
    truth_part = stint_design(truth, pidx, end)
    baseline = fit([base_part, truth_part], P)
    omitted = fit([base_part], P)

    aggregate_long = aggregate_from_truth(truth)
    aggregate_part = aggregate_design(aggregate_long, pidx, end)
    aggregate_beta = fit([base_part, aggregate_part], P)

    candidates["score_consistent"] = ((np.expm1(candidates.log_score_error) < .5)
                                      & (candidates.coverage_ok >= .5))
    any_good = candidates.groupby("game_id").score_consistent.transform("any")
    candidates["rapm_probability"] = np.where(
        any_good & candidates.score_consistent, candidates.probability, 0.0)
    denom = candidates.groupby("game_id").rapm_probability.transform("sum")
    candidates.loc[denom > 0, "rapm_probability"] /= denom[denom > 0]
    good_ids = set(candidates.loc[candidates.rapm_probability > 0, "game_id"])
    fallback_ids = masked_ids - good_ids
    fallback_part = aggregate_design(
        aggregate_long[aggregate_long.game_id.isin(fallback_ids)], pidx, end)
    bank_groups = {(gid, cid): g.copy() for (gid, cid), g in
                   bank.groupby(["game_id", "candidate_id"])}

    rng = np.random.default_rng(20260718)
    imputed_betas = []
    for imp in range(20):
        frames = []
        for gid, game in candidates[candidates.rapm_probability > 0].groupby("game_id"):
            p = game.rapm_probability.to_numpy(float)
            row = game.iloc[int(rng.choice(len(game), p=p))]
            frames.append(bank_groups[(gid, str(row.candidate_id))])
        drawn = pd.concat(frames, ignore_index=True)
        imputed_betas.append(fit([base_part, stint_design(drawn, pidx, end),
                                  fallback_part], P))
    imputed_betas = np.vstack(imputed_betas)

    def rapm(beta):
        return beta[..., :P] - beta[..., P:]
    target = rapm(baseline); omission_error = rapm(omitted) - target
    aggregate_error = rapm(aggregate_beta) - target
    mi = rapm(imputed_betas); mi_mean_error = mi.mean(axis=0) - target
    mi_sd = mi.std(axis=0, ddof=1)
    exposed_ids = set(truth[HCOLS+ACOLS].to_numpy(int).ravel())
    exposed = np.array([p in exposed_ids for p in players])
    result = pd.DataFrame({
        "player_id": players, "masked_game_exposed": exposed,
        "omission_error": omission_error, "aggregate_error": aggregate_error,
        "mi_mean_error": mi_mean_error, "mi_between_sd": mi_sd,
    })
    result.to_csv(REPORT / "probabilistic_salvage_masked_rapm_players.csv", index=False)
    x = result[result.masked_game_exposed]
    summary = {
        "masked_games": len(masked_ids), "score_consistent_imputed_games": len(good_ids),
        "aggregate_fallback_games": len(fallback_ids), "exposed_players": len(x),
        "omission_mae": float(x.omission_error.abs().mean()),
        "aggregate_mae": float(x.aggregate_error.abs().mean()),
        "multiple_imputation_mae": float(x.mi_mean_error.abs().mean()),
        "omission_p95_abs_error": float(x.omission_error.abs().quantile(.95)),
        "aggregate_p95_abs_error": float(x.aggregate_error.abs().quantile(.95)),
        "multiple_imputation_p95_abs_error": float(x.mi_mean_error.abs().quantile(.95)),
        "mean_between_imputation_sd": float(x.mi_between_sd.mean()),
        "validation_is_chronological_after_season": 2017,
        "production_modified": False,
    }
    (REPORT / "probabilistic_salvage_masked_rapm_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
