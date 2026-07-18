"""Audit the current rolling atomic-prior coefficients and player contributions."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from build_atomic_features import ATOMS


ROOT = Path(__file__).resolve().parents[1]
DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
OUT = ROOT / "outputs" / "contextual_causal"
TARGET_YEAR = 2025
TRAIN_YEARS = 10
ALPHA = {"orapm": .1, "drapm": 200.0}

EXPECTED = {
    **{x: "positive" for x in ["rim_ast_m_75", "rim_unast_m_75",
        "flt_ast_m_75", "flt_unast_m_75", "mid_ast_m_75", "mid_unast_m_75",
        "tp_ast_m_75", "tp_unast_m_75", "ftm_75", "ast_rim_75",
        "ast_mid2_75", "ast_tp_75", "oreb_opp_rate"]},
    **{x: "negative" for x in ["rim_miss_75", "flt_miss_75", "mid_miss_75",
        "tp_miss_75", "ft_miss_75", "tov_bp_75", "tov_lb_75", "tov_dead_75"]},
}
DEF_EXPECTED = {
    "dreb_opp_rate": "positive", "def_boxout_75": "positive",
    "stl_75": "positive", "blk_75": "positive", "rim_dfg_diff": "positive",
    "contested_2pt_75": "positive", "contested_3pt_75": "positive",
    "defl_75": "positive", "pf_nonoff_75": "negative",
    "charges_drawn_75": "positive", "height": "positive", "wing_rel": "positive",
}


def panel() -> pd.DataFrame:
    feat = pd.read_parquet(DATA / "features_atomic_season.parquet")
    tgt = pd.read_parquet(DATA / "targets" / "rapm_target_poss_hl550.parquet")
    tgt = tgt[tgt.alpha == 500].copy()
    tgt["season_year"] = tgt.target_season.str[:4].astype(int)
    tgt = tgt.rename(columns={"player_id": "pid"})
    return feat.merge(tgt, on=["pid", "season_year"], how="inner")


def design(train: pd.DataFrame, test: pd.DataFrame):
    tr = train[ATOMS].replace([np.inf, -np.inf], np.nan).astype(float)
    te = test[ATOMS].replace([np.inf, -np.inf], np.nan).astype(float)
    fill = tr.mean().fillna(0.0)
    missing = te.isna()
    tr = tr.fillna(fill); te = te.fillna(fill)
    w = train.poss_season.to_numpy(float)
    w = w / np.mean(w[w > 0])
    mu = np.average(tr.to_numpy(), axis=0, weights=w)
    sd = np.sqrt(np.average((tr.to_numpy() - mu) ** 2, axis=0, weights=w))
    sd = np.where(np.isfinite(sd) & (sd > 1e-8), sd, 1.0)
    return ((tr.to_numpy() - mu) / sd, (te.to_numpy() - mu) / sd,
            w, mu, sd, missing)


def main() -> None:
    df = panel()
    train = df[(df.season_year < TARGET_YEAR)
               & (df.season_year >= TARGET_YEAR-TRAIN_YEARS)
               & (df.poss_season >= 1700)].copy()
    test = df[df.season_year == TARGET_YEAR].copy()
    X, Z, w, mu, sd, missing = design(train, test)
    coef_rows = []; contribution_frames = []
    for target in ("orapm", "drapm"):
        model = Ridge(alpha=ALPHA[target]).fit(X, train[target], sample_weight=w)
        prediction = model.predict(Z)
        contributions = Z * model.coef_[None, :]

        # Refit after leaving out each training season.  This is a coefficient
        # stability diagnostic, not a new hyperparameter search.
        loo = []
        for season in sorted(train.season_year.unique()):
            keep = train.season_year.to_numpy() != season
            m = Ridge(alpha=ALPHA[target]).fit(X[keep], train[target].to_numpy()[keep],
                                               sample_weight=w[keep])
            loo.append(m.coef_)
        loo = np.vstack(loo)
        for j, feature in enumerate(ATOMS):
            expectation = (EXPECTED.get(feature, "near-zero") if target == "orapm"
                           else DEF_EXPECTED.get(feature, "near-zero"))
            sign_ok = (model.coef_[j] > 0 if expectation == "positive" else
                       model.coef_[j] < 0 if expectation == "negative" else
                       abs(model.coef_[j]) < .15)
            coef_rows.append({
                "target": target, "feature": feature,
                "coef_per_training_sd": model.coef_[j],
                "raw_unit_coef": model.coef_[j] / sd[j],
                "training_mean": mu[j], "training_sd": sd[j],
                "expected": expectation, "basketball_sign_ok": bool(sign_ok),
                "loo_sign_agreement": float(np.mean(np.sign(loo[:, j]) == np.sign(model.coef_[j]))),
                "loo_coef_sd": float(loo[:, j].std(ddof=1)),
                "current_missing_rate": float(missing.iloc[:, j].mean()),
                "current_abs_contribution_p95": float(np.quantile(np.abs(contributions[:, j]), .95)),
                "current_abs_contribution_max": float(np.max(np.abs(contributions[:, j]))),
            })
        cf = pd.DataFrame(contributions, columns=ATOMS)
        cf.insert(0, "prediction", prediction)
        cf.insert(0, "season_year", TARGET_YEAR)
        cf.insert(0, "pid", test.pid.to_numpy())
        cf["target"] = target
        contribution_frames.append(cf)

    coefficients = pd.DataFrame(coef_rows)
    contributions = pd.concat(contribution_frames, ignore_index=True)
    coefficients.to_csv(OUT / "atomic_prior_current_coefficients_audit.csv", index=False)
    contributions.to_parquet(OUT / "atomic_prior_current_contributions.parquet", index=False)
    print("Largest standardized coefficients:")
    print(coefficients.assign(a=lambda x:x.coef_per_training_sd.abs())
          .sort_values(["target", "a"], ascending=[True, False])
          .groupby("target").head(15)[
              ["target", "feature", "coef_per_training_sd", "expected",
               "basketball_sign_ok", "loo_sign_agreement",
               "current_abs_contribution_max"]].to_string(index=False))


if __name__ == "__main__":
    main()
