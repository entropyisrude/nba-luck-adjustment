"""Estimate and cross-validate exact four-man-core defensive swap contrasts.

Each comparison is within one game and one exact offensive five. The two
defensive lineups share four defenders and differ only in the exchanged player.
This is a matched contrast design, not a possession-level all-player regression.
"""

from __future__ import annotations

import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse
from scipy.sparse.linalg import lsqr
from scipy.stats import spearmanr


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "derived" / "defense_causal" / "halfcourt_rim_lineup_possessions_2022_23_to_2025_26.parquet"
EVENTS_OUT = ROOT / "outputs" / "defense_causal" / "exact_lineup_swap_events.csv"
REPORT_OUT = ROOT / "outputs" / "defense_causal" / "exact_lineup_swap_cv.json"

OUTCOMES = {
    "strict_rim_access": ("first_chance_strict_rim_event_hc6", "possessions"),
    "broad_rim_access": ("first_chance_broad_rim_event_hc6", "possessions"),
    "recorded_rim_attempt": ("first_chance_rim_hc6", "possessions"),
    "restricted_area_foul": ("first_chance_restricted_area_foul_hc6", "possessions"),
    "rim_fg_pct": ("first_chance_rim_makes_hc6", "first_chance_rim_attempts_hc6"),
}


def build_events(df: pd.DataFrame) -> pd.DataFrame:
    keys = ["game_id", "season_year", "offense_lineup_id", "defense_lineup_id"]
    agg_spec = {"possessions": ("game_id", "size")}
    for numerator, denominator in OUTCOMES.values():
        agg_spec[numerator] = (numerator, "sum")
        if denominator != "possessions":
            agg_spec[denominator] = (denominator, "sum")
    lineups = df.groupby(keys, as_index=False).agg(**agg_spec)

    expanded = []
    for row in lineups.itertuples(index=False):
        players = tuple(sorted(int(x) for x in row.defense_lineup_id.split("-")))
        base = row._asdict()
        for added in players:
            rec = dict(base)
            rec["core"] = "-".join(map(str, (p for p in players if p != added)))
            rec["added_player"] = added
            expanded.append(rec)
    ex = pd.DataFrame(expanded)

    events = []
    group_keys = ["game_id", "season_year", "offense_lineup_id", "core"]
    for group_key, group in ex.groupby(group_keys, sort=False):
        if len(group) < 2:
            continue
        for ia, ib in itertools.combinations(range(len(group)), 2):
            a, b = group.iloc[ia], group.iloc[ib]
            if a.added_player == b.added_player:
                continue
            if int(a.added_player) > int(b.added_player):
                a, b = b, a
            rec = dict(zip(group_keys, group_key))
            rec.update({
                "player_a": int(a.added_player), "player_b": int(b.added_player),
                "defense_lineup_a": a.defense_lineup_id,
                "defense_lineup_b": b.defense_lineup_id,
            })
            for name, (num, den) in OUTCOMES.items():
                rec[f"{name}_y_a"] = float(a[num])
                rec[f"{name}_n_a"] = float(a[den])
                rec[f"{name}_y_b"] = float(b[num])
                rec[f"{name}_n_b"] = float(b[den])
            events.append(rec)
    return pd.DataFrame(events).drop_duplicates(
        ["game_id", "offense_lineup_id", "core", "player_a", "player_b"]
    )


def fit_scores(train: pd.DataFrame, outcome: str, ridge: float) -> tuple[dict[int, float], set[int]]:
    players = sorted(set(train.player_a).union(train.player_b))
    index = {p: i for i, p in enumerate(players)}
    n = len(train)
    rows = np.repeat(np.arange(n), 2)
    cols = np.r_[[index[p] for p in train.player_a], [index[p] for p in train.player_b]]
    vals = np.r_[np.ones(n), -np.ones(n)]
    # np.r_ order differs from repeated rows; construct explicitly.
    rows = np.ravel(np.column_stack([np.arange(n), np.arange(n)]))
    cols = np.ravel(np.column_stack([[index[p] for p in train.player_a], [index[p] for p in train.player_b]]))
    vals = np.tile([1.0, -1.0], n)
    X = sparse.csr_matrix((vals, (rows, cols)), shape=(n, len(players)))
    na = train[f"{outcome}_n_a"].to_numpy(float)
    nb = train[f"{outcome}_n_b"].to_numpy(float)
    d = train[f"{outcome}_y_a"].to_numpy(float) / na - train[f"{outcome}_y_b"].to_numpy(float) / nb
    weight = na * nb / (na + nb)
    sw = np.sqrt(weight)
    Xw = X.multiply(sw[:, None])
    yw = d * sw
    penalty = sparse.eye(len(players), format="csr") * np.sqrt(ridge)
    beta = lsqr(sparse.vstack([Xw, penalty]), np.r_[yw, np.zeros(len(players))], atol=1e-9, btol=1e-9)[0]
    return {p: float(beta[i]) for p, i in index.items()}, set(players)


def cross_validate(events: pd.DataFrame, outcome: str, ridge: float = 20.0) -> dict:
    valid = events[(events[f"{outcome}_n_a"] >= 3) & (events[f"{outcome}_n_b"] >= 3)].copy()
    if outcome == "rim_fg_pct":
        valid = events[(events[f"{outcome}_n_a"] >= 2) & (events[f"{outcome}_n_b"] >= 2)].copy()
    valid["fold"] = valid.game_id.astype(str).map(lambda x: int(x) % 5)
    predictions = []
    for fold in range(5):
        train, test = valid[valid.fold != fold], valid[valid.fold == fold].copy()
        scores, seen = fit_scores(train, outcome, ridge)
        test = test[test.player_a.isin(seen) & test.player_b.isin(seen)].copy()
        test["prediction"] = test.player_a.map(scores) - test.player_b.map(scores)
        predictions.append(test)
    pred = pd.concat(predictions, ignore_index=True) if predictions else pd.DataFrame()
    if pred.empty:
        return {"events": 0}
    na = pred[f"{outcome}_n_a"].to_numpy(float)
    nb = pred[f"{outcome}_n_b"].to_numpy(float)
    actual = pred[f"{outcome}_y_a"].to_numpy(float) / na - pred[f"{outcome}_y_b"].to_numpy(float) / nb
    predicted = pred.prediction.to_numpy(float)
    weight = na * nb / (na + nb)
    mse_model = np.average((actual - predicted) ** 2, weights=weight)
    mse_zero = np.average(actual**2, weights=weight)
    corr = np.corrcoef(actual, predicted)[0, 1] if len(pred) > 2 else np.nan
    scored = pred[["player_a", "player_b"]].copy()
    scored["fold"] = pred.fold.to_numpy()
    scored["actual"] = actual
    scored["prediction"] = predicted
    scored["weight"] = weight
    player_rows = pd.concat([
        pd.DataFrame({"fold": scored.fold, "player": scored.player_a, "actual": scored.actual,
                      "prediction": scored.prediction, "weight": scored.weight}),
        pd.DataFrame({"fold": scored.fold, "player": scored.player_b, "actual": -scored.actual,
                      "prediction": -scored.prediction, "weight": scored.weight}),
    ], ignore_index=True)
    # Keep fold in the aggregation key. Combining all folds by player would leak:
    # each player's aggregate target would then contain games used to train the
    # predictions for that player's other folds.
    player_agg = player_rows.groupby(["fold", "player"]).apply(
        lambda g: pd.Series({
            "events": len(g), "weight": g.weight.sum(),
            "actual": np.average(g.actual, weights=g.weight),
            "prediction": np.average(g.prediction, weights=g.weight),
        }), include_groups=False
    ).reset_index()
    player_agg = player_agg[player_agg.events >= 5]

    pair_agg = scored.groupby(["fold", "player_a", "player_b"]).apply(
        lambda g: pd.Series({
            "events": len(g), "weight": g.weight.sum(),
            "actual": np.average(g.actual, weights=g.weight),
            "prediction": np.average(g.prediction, weights=g.weight),
        }), include_groups=False
    ).reset_index()
    pair_agg = pair_agg[pair_agg.events >= 2]

    def aggregate_result(g: pd.DataFrame) -> dict:
        if len(g) < 3:
            return {"units": int(len(g))}
        model = np.average((g.actual-g.prediction)**2, weights=g.weight)
        zero = np.average(g.actual**2, weights=g.weight)
        return {
            "units": int(len(g)),
            "weighted_rmse_model": float(np.sqrt(model)),
            "weighted_rmse_zero_contrast": float(np.sqrt(zero)),
            "rmse_improvement_pct": float(100*(1-np.sqrt(model)/np.sqrt(zero))),
            "pearson": float(np.corrcoef(g.actual, g.prediction)[0, 1]),
        }
    return {
        "eligible_events": int(len(valid)),
        "heldout_events": int(len(pred)),
        "players": int(len(set(valid.player_a).union(valid.player_b))),
        "weighted_rmse_model": float(np.sqrt(mse_model)),
        "weighted_rmse_zero_contrast": float(np.sqrt(mse_zero)),
        "rmse_improvement_pct": float(100 * (1 - np.sqrt(mse_model) / np.sqrt(mse_zero))),
        "heldout_pearson": float(corr),
        "heldout_player_fold_aggregate_min_5_events": aggregate_result(player_agg),
        "heldout_pair_fold_aggregate_min_2_events": aggregate_result(pair_agg),
        "ridge": ridge,
    }


def split_game_replication(
    events: pd.DataFrame, outcome: str, ridge: float = 100.0,
    season_specific: bool = False, min_events_each_half: int = 10,
) -> dict:
    threshold = 2 if outcome == "rim_fg_pct" else 3
    valid = events[(events[f"{outcome}_n_a"] >= threshold) &
                   (events[f"{outcome}_n_b"] >= threshold)].copy()
    if season_specific:
        valid["player_a"] = valid.season_year.astype(str) + ":" + valid.player_a.astype(str)
        valid["player_b"] = valid.season_year.astype(str) + ":" + valid.player_b.astype(str)
    valid["half"] = valid.game_id.astype(str).map(lambda x: int(x) % 2)
    left, right = valid[valid.half == 0], valid[valid.half == 1]
    score_left, _ = fit_scores(left, outcome, ridge)
    score_right, _ = fit_scores(right, outcome, ridge)

    def appearances(frame: pd.DataFrame) -> pd.Series:
        return pd.concat([frame.player_a, frame.player_b]).value_counts()

    n_left, n_right = appearances(left), appearances(right)
    common = sorted(set(score_left).intersection(score_right))
    common = [p for p in common if n_left.get(p, 0) >= min_events_each_half and
              n_right.get(p, 0) >= min_events_each_half]
    if len(common) < 3:
        return {"units": len(common)}
    a = np.array([score_left[p] for p in common])
    b = np.array([score_right[p] for p in common])
    return {
        "units": len(common),
        "pearson": float(np.corrcoef(a, b)[0, 1]),
        "spearman": float(spearmanr(a, b).statistic),
        "ridge": ridge,
        "min_events_each_half": min_events_each_half,
        "unit": "player-season" if season_specific else "player pooled across seasons",
    }


def main() -> None:
    EVENTS_OUT.parent.mkdir(parents=True, exist_ok=True)
    if EVENTS_OUT.exists() and EVENTS_OUT.stat().st_mtime >= SOURCE.stat().st_mtime:
        events = pd.read_csv(EVENTS_OUT, dtype={"game_id": str})
    else:
        cols = ["game_id", "season_year", "offense_lineup_id", "defense_lineup_id"]
        for numerator, denominator in OUTCOMES.values():
            cols.append(numerator)
            if denominator != "possessions":
                cols.append(denominator)
        cols = list(dict.fromkeys(cols))
        df = pd.read_parquet(SOURCE, columns=cols)
        events = build_events(df)
        events.to_csv(EVENTS_OUT, index=False)
    report = {
        "design": "within game and exact offensive five; defensive lineups share an exact four-man core",
        "raw_swap_events": int(len(events)),
        "games": int(events.game_id.nunique()),
        "cv": {
            outcome: {str(ridge): cross_validate(events, outcome, ridge) for ridge in (5.0, 20.0, 100.0)}
            for outcome in OUTCOMES
        },
        "disjoint_game_split_replication": {
            outcome: {
                "pooled_player": split_game_replication(events, outcome, season_specific=False),
                "player_season": split_game_replication(events, outcome, season_specific=True),
            }
            for outcome in OUTCOMES
        },
    }
    REPORT_OUT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Wrote {EVENTS_OUT}")
    print(f"Wrote {REPORT_OUT}")


if __name__ == "__main__":
    main()
