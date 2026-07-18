"""Chronological test of residualized v1 composites atop atomic offense.

The design and selection rule are preregistered in docs/research_log.md.
Nothing here changes the promoted atomic model or site-facing artifacts.
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
DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
ATOMIC_PATH = DATA / "features_atomic_denominator_season.parquet"
V1_PATH = DATA / "features_box_season.parquet"
TARGET = DATA / "targets" / "rapm_target_poss_hl550.parquet"
EVIDENCE = DATA / "evidence_poss_season.parquet"
BASE_PRIOR = ROOT / "outputs" / "contextual_causal" / "rolling_prior_atomic_denominator_poss.parquet"
OUT = ROOT / "outputs" / "contextual_causal"

GROUPS = {
    "atomic_only": [],
    "role_load": ["usg", "ast_pct", "mpg"],
    "efficiency": ["ts", "efg", "ft_pct"],
    "shot_style": ["fg3_rate", "pct_unassisted", "pct_pts3", "pct_paint",
                   "pct_fb", "pct_ft", "pct_fga3"],
    "missing_events": ["fouls_drawn_75", "blocked_75"],
    "compact": ["usg", "ast_pct", "mpg", "ts", "efg", "fg3_rate",
                "pct_unassisted", "fouls_drawn_75", "blocked_75"],
    "all_residual": ["usg", "ast_pct", "mpg", "ts", "efg", "ft_pct",
                     "fg3_rate", "pct_unassisted", "pct_pts3", "pct_paint",
                     "pct_fb", "pct_ft", "pct_fga3", "fouls_drawn_75",
                     "blocked_75"],
}
ALL_COMPOSITES = list(dict.fromkeys(
    feature for group in GROUPS.values() for feature in group))
DENOM_SOURCE = {
    "usg": "poss", "ast_pct": "poss", "mpg": "composite_games",
    "ts": "scoring_attempts", "efg": "fga", "ft_pct": "fta",
    "fg3_rate": "fga", "pct_unassisted": "fgm",
    "pct_pts3": "pts", "pct_paint": "pts", "pct_fb": "pts",
    "pct_ft": "pts", "pct_fga3": "fga",
    "fouls_drawn_75": "poss", "blocked_75": "poss",
}

ALPHAS = [0.1, 1.0, 10.0, 50.0, 200.0]
ATOM_LAMBDA = 0.10
COMPOSITE_LAMBDA = 0.10
RESIDUALIZER_ALPHA = 10.0
MIN_POSS = 1700.0
TRAIN_YEARS = 10
DEV_END = 2018
TEST_START = 2019


def prep() -> tuple[pd.DataFrame, pd.DataFrame]:
    atoms = pd.read_parquet(ATOMIC_PATH)
    keep = (["pid", "season_year", "games", "poss", "pts", "fga", "fgm",
             "fta"] + ALL_COMPOSITES)
    old = pd.read_parquet(V1_PATH)[keep].copy()
    old["scoring_attempts"] = old.fga + 0.44 * old.fta
    old = old.rename(columns={"games": "composite_games",
                              "poss": "composite_poss"})
    # Denominator source names are made unambiguous after the merge.
    old["poss_comp"] = old["composite_poss"]
    tgt = pd.read_parquet(TARGET)
    tgt = tgt[tgt.alpha == 500].copy()
    tgt["season_year"] = tgt.target_season.str[:4].astype(int)
    tgt = tgt.rename(columns={"player_id": "pid"})
    panel = atoms.merge(old, on=["pid", "season_year"], how="left")
    panel = panel.merge(tgt, on=["pid", "season_year"], how="inner")
    ev = pd.read_parquet(EVIDENCE).rename(columns={"player_id": "pid"})
    return panel, ev


def denominator(frame: pd.DataFrame, feature: str) -> np.ndarray:
    source = DENOM_SOURCE[feature]
    if source == "poss":
        source = "poss_comp"
    return pd.to_numeric(frame[source], errors="coerce").to_numpy(float)


def shrunk_composites(train: pd.DataFrame, test: pd.DataFrame,
                      features: list[str]
                      ) -> tuple[np.ndarray, np.ndarray, list[dict]]:
    tr_cols, te_cols, meta = [], [], []
    for feature in features:
        tr = pd.to_numeric(train[feature], errors="coerce").to_numpy(float)
        te = pd.to_numeric(test[feature], errors="coerce").to_numpy(float)
        dtr = denominator(train, feature)
        dte = denominator(test, feature)
        valid = np.isfinite(tr) & np.isfinite(dtr) & (dtr > 0)
        if valid.any():
            mean = float(np.average(tr[valid], weights=dtr[valid]))
            median_denom = max(float(np.median(dtr[valid])), 1e-8)
        else:
            mean, median_denom = 0.0, 1.0
        rtr = np.divide(dtr, dtr + COMPOSITE_LAMBDA * median_denom,
                        out=np.zeros_like(dtr),
                        where=np.isfinite(dtr) & (dtr > 0))
        rte = np.divide(dte, dte + COMPOSITE_LAMBDA * median_denom,
                        out=np.zeros_like(dte),
                        where=np.isfinite(dte) & (dte > 0))
        tr = np.where(np.isfinite(tr), tr, mean)
        te = np.where(np.isfinite(te), te, mean)
        tr_cols.append(rtr * tr + (1.0 - rtr) * mean)
        te_cols.append(rte * te + (1.0 - rte) * mean)
        meta.append({"feature": feature, "source": DENOM_SOURCE[feature],
                     "mean": mean, "median_denom": median_denom})
    return np.column_stack(tr_cols), np.column_stack(te_cols), meta


def hybrid_design(train: pd.DataFrame, test: pd.DataFrame,
                  features: list[str]
                  ) -> tuple[np.ndarray, np.ndarray, np.ndarray, list[dict]]:
    xa, za, w, _ = shrink_design(train, test, ATOM_LAMBDA)
    if not features:
        return xa, za, w, []
    c, q, meta = shrunk_composites(train, test, features)
    cmu = np.average(c, axis=0, weights=w)
    csd = np.sqrt(np.average((c - cmu) ** 2, axis=0, weights=w))
    csd = np.where(np.isfinite(csd) & (csd > 1e-8), csd, 1.0)
    c = (c - cmu) / csd
    q = (q - cmu) / csd

    # Cross-sectional residualization is fit without looking at RAPM.  The
    # added columns therefore contain only composite information that a linear
    # reconstruction from the atomic profile does not already supply.
    residualizer = Ridge(alpha=RESIDUALIZER_ALPHA).fit(
        xa, c, sample_weight=w)
    c_hat = np.asarray(residualizer.predict(xa))
    q_hat = np.asarray(residualizer.predict(za))
    if c_hat.ndim == 1:
        c_hat = c_hat[:, None]
        q_hat = q_hat[:, None]
    cr = c - c_hat
    qr = q - q_hat
    rmu = np.average(cr, axis=0, weights=w)
    rsd = np.sqrt(np.average((cr - rmu) ** 2, axis=0, weights=w))
    rsd = np.where(np.isfinite(rsd) & (rsd > 1e-8), rsd, 1.0)
    cr = (cr - rmu) / rsd
    qr = (qr - rmu) / rsd
    for i, item in enumerate(meta):
        item["residual_sd"] = rsd[i]
    return np.column_stack([xa, cr]), np.column_stack([za, qr]), w, meta


def predictions(panel: pd.DataFrame, features: list[str], alpha: float,
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
        x, z, w, meta = hybrid_design(train, test, features)
        model = Ridge(alpha=alpha).fit(x, train.orapm, sample_weight=w)
        rows.append(pd.DataFrame({"pid": test.pid.to_numpy(),
                                  "season_year": sy,
                                  "pred": model.predict(z)}))
        if keep_coefficients:
            names = ATOMS + ["residual__" + f for f in features]
            for name, coef in zip(names, model.coef_):
                coefficients.append({"prediction_season": sy,
                                     "feature": name,
                                     "coef_per_training_sd": coef,
                                     "alpha": alpha})
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


def yearly_scores(pred: pd.DataFrame, ev: pd.DataFrame, lead: int) -> list[dict]:
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
    OUT.mkdir(parents=True, exist_ok=True)
    grid = []
    for group, features in GROUPS.items():
        for alpha in ALPHAS:
            pred, _ = predictions(panel, features, alpha)
            same, n_same = score(pred, ev, (2004, DEV_END), 0)
            nxt, n_next = score(pred, ev, (2004, DEV_END), 1)
            grid.append({"group": group, "alpha": alpha,
                         "dev_same_r": same, "dev_next_r": nxt,
                         "selection_score": (same + nxt) / 2.0,
                         "n_same": n_same, "n_next": n_next})
    grid_frame = pd.DataFrame(grid)
    selected = grid_frame.sort_values(
        ["selection_score", "dev_next_r"], ascending=False).iloc[0]
    group = str(selected.group)
    alpha = float(selected.alpha)
    features = GROUPS[group]
    pred, coefficients = predictions(panel, features, alpha, True)
    test_same, n_same = score(pred, ev, (TEST_START, 2025), 0)
    test_next, n_next = score(pred, ev, (TEST_START, 2025), 1)

    base = pd.read_parquet(BASE_PRIOR)[["pid", "season_year", "pd"]]
    prior = pred.rename(columns={"pred": "po"}).merge(
        base, on=["pid", "season_year"], how="left")
    prior["pred"] = prior.po + prior.pd
    prior["offense_model"] = "atomic_plus_" + group
    prior.to_parquet(OUT / "rolling_prior_atomic_residual_hybrid_poss.parquet",
                     index=False)
    coefficients.to_csv(
        OUT / "atomic_residual_hybrid_rolling_coefficients.csv", index=False)
    grid_frame.to_csv(OUT / "atomic_residual_hybrid_dev_grid.csv", index=False)
    years = yearly_scores(pred, ev, 0) + yearly_scores(pred, ev, 1)
    pd.DataFrame(years).to_csv(
        OUT / "atomic_residual_hybrid_test_by_year.csv", index=False)
    summary = pd.DataFrame([{
        "selected_group": group, "features": ",".join(features),
        "alpha": alpha, "dev_same_r": selected.dev_same_r,
        "dev_next_r": selected.dev_next_r,
        "selection_score": selected.selection_score,
        "test_same_r": test_same, "test_next_r": test_next,
        "n_test_same": n_same, "n_test_next": n_next,
    }])
    summary.to_csv(OUT / "atomic_residual_hybrid_scoreboard.csv", index=False)
    print(summary.to_string(index=False))
    print("\nTop development candidates:")
    print(grid_frame.sort_values("selection_score", ascending=False)
          .head(12).to_string(index=False))
    print("\nCurrent residual-composite coefficients:")
    print(coefficients[(coefficients.prediction_season == 2025)
                       & coefficients.feature.str.startswith("residual__")]
          .sort_values("coef_per_training_sd", key=abs, ascending=False)
          .to_string(index=False))


if __name__ == "__main__":
    main()
