"""Chronological evaluation of decomposed foul-generation atoms.

Definitions and the selection gate are preregistered in docs/research_log.md.
This writes versioned research artifacts only.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_atomic_features import ATOMS
from build_box_prior import wcorr
from evaluate_atomic_denominator_rolling import shrink_design

ROOT = Path(__file__).resolve().parents[1]
FEATURES = (ROOT / "derived" / "contextual_causal"
            / "features_atomic_foul_decomposed_season.parquet")
DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
TARGET = DATA / "targets" / "rapm_target_poss_hl550.parquet"
EVIDENCE = DATA / "evidence_poss_season.parquet"
BASE_PRIOR = (ROOT / "outputs" / "contextual_causal"
              / "rolling_prior_atomic_denominator_poss.parquet")
OUT = ROOT / "outputs" / "contextual_causal"

GROUPS = {
    "baseline": [],
    "total_fouls_drawn": ["total_fouls_drawn_75"],
    "ft_trip_only": ["ft_foul_trips_drawn_75"],
    "other_fouls_only": ["other_fouls_drawn_75"],
    "decomposed": ["ft_foul_trips_drawn_75", "other_fouls_drawn_75"],
}
ALPHAS = [0.1, 1.0, 10.0, 50.0, 200.0]
LAMBDA = 0.10
MIN_POSS = 1700.0
TRAIN_YEARS = 10
DEV_END = 2018
TEST_START = 2019


def prep() -> tuple[pd.DataFrame, pd.DataFrame]:
    feat = pd.read_parquet(FEATURES)
    tgt = pd.read_parquet(TARGET)
    tgt = tgt[tgt.alpha == 500].copy()
    tgt["season_year"] = tgt.target_season.str[:4].astype(int)
    tgt = tgt.rename(columns={"player_id": "pid"})
    panel = feat.merge(tgt, on=["pid", "season_year"], how="inner")
    ev = pd.read_parquet(EVIDENCE).rename(columns={"player_id": "pid"})
    return panel, ev


def extra_design(train: pd.DataFrame, test: pd.DataFrame,
                 features: list[str], w: np.ndarray
                 ) -> tuple[np.ndarray, np.ndarray, list[dict]]:
    tr_cols, te_cols, meta = [], [], []
    for feature in features:
        tr = pd.to_numeric(train[feature], errors="coerce").to_numpy(float)
        te = pd.to_numeric(test[feature], errors="coerce").to_numpy(float)
        dc = feature + "__denom"
        dtr = pd.to_numeric(train[dc], errors="coerce").to_numpy(float)
        dte = pd.to_numeric(test[dc], errors="coerce").to_numpy(float)
        valid = np.isfinite(tr) & np.isfinite(dtr) & (dtr > 0)
        if valid.any():
            mean = float(np.average(tr[valid], weights=dtr[valid]))
            median_denom = max(float(np.median(dtr[valid])), 1e-8)
        else:
            mean, median_denom = 0.0, 1.0
        rtr = np.divide(dtr, dtr + LAMBDA * median_denom,
                        out=np.zeros_like(dtr),
                        where=np.isfinite(dtr) & (dtr > 0))
        rte = np.divide(dte, dte + LAMBDA * median_denom,
                        out=np.zeros_like(dte),
                        where=np.isfinite(dte) & (dte > 0))
        tr = np.where(np.isfinite(tr), tr, mean)
        te = np.where(np.isfinite(te), te, mean)
        xs = rtr * tr + (1.0 - rtr) * mean
        zs = rte * te + (1.0 - rte) * mean
        mu = float(np.average(xs, weights=w))
        sd = float(np.sqrt(np.average((xs - mu) ** 2, weights=w)))
        sd = sd if np.isfinite(sd) and sd > 1e-8 else 1.0
        tr_cols.append((xs - mu) / sd)
        te_cols.append((zs - mu) / sd)
        meta.append({"feature": feature, "rate_mean": mean,
                     "median_denom": median_denom,
                     "training_mean_after_shrink": mu,
                     "training_sd_after_shrink": sd})
    return np.column_stack(tr_cols), np.column_stack(te_cols), meta


def predictions(panel: pd.DataFrame, extras: list[str], alpha: float,
                keep_coefficients: bool = False
                ) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows, coefficients = [], []
    for sy in range(2004, 2026):
        train = panel[(panel.season_year < sy)
                      & (panel.season_year >= sy - TRAIN_YEARS)
                      & (panel.poss_season >= MIN_POSS)].copy()
        test = panel[panel.season_year == sy].copy()
        if len(train) < 500 or test.empty:
            continue
        xa, za, w, _ = shrink_design(train, test, LAMBDA)
        meta = []
        if extras:
            xe, ze, meta = extra_design(train, test, extras, w)
            x, z = np.column_stack([xa, xe]), np.column_stack([za, ze])
        else:
            x, z = xa, za
        model = Ridge(alpha=alpha).fit(x, train.orapm, sample_weight=w)
        rows.append(pd.DataFrame({"pid": test.pid.to_numpy(),
                                  "season_year": sy,
                                  "pred": model.predict(z)}))
        if keep_coefficients:
            names = ATOMS + extras
            extra_meta = {item["feature"]: item for item in meta}
            for name, coef in zip(names, model.coef_):
                row = {"prediction_season": sy, "feature": name,
                       "coef_per_training_sd": coef, "alpha": alpha}
                row.update(extra_meta.get(name, {}))
                coefficients.append(row)
    return pd.concat(rows, ignore_index=True), pd.DataFrame(coefficients)


def score(pred: pd.DataFrame, ev: pd.DataFrame, years: tuple[int, int],
          lead: int) -> tuple[float, int]:
    p = pred.copy()
    p["evidence_year"] = p.season_year + lead
    joined = p.merge(ev, left_on=["pid", "evidence_year"],
                     right_on=["pid", "season_year"], suffixes=("", "_ev"))
    joined = joined[(joined.evidence_year >= years[0])
                    & (joined.evidence_year <= years[1])
                    & (joined.ev_poss >= MIN_POSS)]
    return float(wcorr(joined.pred, joined.ev_o, joined.ev_poss)), len(joined)


def by_year(pred: pd.DataFrame, ev: pd.DataFrame, lead: int) -> list[dict]:
    p = pred.copy()
    p["evidence_year"] = p.season_year + lead
    joined = p.merge(ev, left_on=["pid", "evidence_year"],
                     right_on=["pid", "season_year"], suffixes=("", "_ev"))
    joined = joined[(joined.evidence_year >= TEST_START)
                    & (joined.evidence_year <= 2025)
                    & (joined.ev_poss >= MIN_POSS)]
    return [{"horizon": "same" if lead == 0 else "next",
             "evidence_year": int(year),
             "r_o": wcorr(group.pred, group.ev_o, group.ev_poss),
             "n": len(group)}
            for year, group in joined.groupby("evidence_year")]


def main() -> None:
    panel, ev = prep()
    grid = []
    for group, extras in GROUPS.items():
        for alpha in ALPHAS:
            pred, _ = predictions(panel, extras, alpha)
            same, ns = score(pred, ev, (2004, DEV_END), 0)
            nxt, nn = score(pred, ev, (2004, DEV_END), 1)
            grid.append({"group": group, "alpha": alpha,
                         "dev_same_r": same, "dev_next_r": nxt,
                         "selection_score": (same + nxt) / 2,
                         "n_same": ns, "n_next": nn})
    grid = pd.DataFrame(grid)
    selected = grid.sort_values(
        ["selection_score", "dev_next_r"], ascending=False).iloc[0]
    group, alpha = str(selected.group), float(selected.alpha)
    pred, coefficients = predictions(panel, GROUPS[group], alpha, True)
    test_same, ns = score(pred, ev, (TEST_START, 2025), 0)
    test_next, nn = score(pred, ev, (TEST_START, 2025), 1)

    base_d = pd.read_parquet(BASE_PRIOR)[["pid", "season_year", "pd"]]
    prior = pred.rename(columns={"pred": "po"}).merge(
        base_d, on=["pid", "season_year"], how="left")
    prior["pred"] = prior.po + prior.pd
    prior["offense_model"] = "atomic_foul_" + group
    OUT.mkdir(parents=True, exist_ok=True)
    prior.to_parquet(OUT / "rolling_prior_atomic_foul_candidate_poss.parquet",
                     index=False)
    grid.to_csv(OUT / "atomic_foul_candidate_dev_grid.csv", index=False)
    coefficients.to_csv(OUT / "atomic_foul_candidate_coefficients.csv",
                        index=False)
    pd.DataFrame(by_year(pred, ev, 0) + by_year(pred, ev, 1)).to_csv(
        OUT / "atomic_foul_candidate_test_by_year.csv", index=False)
    summary = pd.DataFrame([{
        "selected_group": group, "features": ",".join(GROUPS[group]),
        "alpha": alpha, "dev_same_r": selected.dev_same_r,
        "dev_next_r": selected.dev_next_r,
        "selection_score": selected.selection_score,
        "test_same_r": test_same, "test_next_r": test_next,
        "n_test_same": ns, "n_test_next": nn,
    }])
    summary.to_csv(OUT / "atomic_foul_candidate_scoreboard.csv", index=False)
    print(summary.to_string(index=False))
    print("\nTop development candidates:")
    print(grid.sort_values("selection_score", ascending=False)
          .head(12).to_string(index=False))
    print("\nCurrent added coefficients:")
    print(coefficients[(coefficients.prediction_season == 2025)
                       & coefficients.feature.isin(GROUPS[group])]
          .to_string(index=False))


if __name__ == "__main__":
    main()
