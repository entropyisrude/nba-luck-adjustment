"""Assemble the production-compatible denominator-aware atomic prior.

2004-25 uses the frozen chronological rolling predictions. 1996-2003 uses
within-era leave-one-season-out atomic fits because no ten-year past training
window exists. The legacy v1 prior is never overwritten.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from sklearn.linear_model import Ridge

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evaluate_atomic_denominator_rolling import shrink_design
from build_rapm_target import load_player_names, season_label

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
FEATURES = DATA / "features_atomic_denominator_season.parquet"
TARGET = DATA / "targets" / "rapm_target_poss_hl550.parquet"
ROLLING = (ROOT / "outputs" / "contextual_causal"
           / "rolling_prior_atomic_denominator_poss.parquet")
OUT = DATA / "priors" / "box_prior_atomic_denominator.parquet"
OUT_CSV = DATA / "priors" / "box_prior_atomic_denominator.csv"

EARLY_START, EARLY_END = 1996, 2003
MIN_POSS = 1700.0
PARAMS = {"orapm": {"lambda": 0.10, "alpha": 0.10},
          "drapm": {"lambda": 0.25, "alpha": 200.0}}


def early_predictions(panel: pd.DataFrame, target: str) -> pd.DataFrame:
    rows = []
    param = PARAMS[target]
    era = panel[panel.season_year.between(EARLY_START, EARLY_END)]
    for sy in range(EARLY_START, EARLY_END + 1):
        train = era[(era.season_year != sy)
                    & (era.poss_season >= MIN_POSS)].copy()
        test = era[era.season_year == sy].copy()
        if len(train) < 500 or test.empty:
            raise RuntimeError(f"insufficient early atomic training rows for {sy}")
        x, z, w, _ = shrink_design(train, test, param["lambda"])
        model = Ridge(alpha=param["alpha"]).fit(
            x, train[target], sample_weight=w)
        rows.append(pd.DataFrame({"pid": test.pid.to_numpy(),
                                  "season_year": sy,
                                  "pred": model.predict(z)}))
    return pd.concat(rows, ignore_index=True)


def main() -> None:
    feat = pd.read_parquet(FEATURES)
    tgt = pd.read_parquet(TARGET)
    tgt = tgt[tgt.alpha == 500].copy()
    tgt["season_year"] = tgt.target_season.str[:4].astype(int)
    tgt = tgt.rename(columns={"player_id": "pid"})
    panel = feat.merge(tgt, on=["pid", "season_year"], how="inner")

    eo = early_predictions(panel, "orapm").rename(columns={"pred": "po"})
    ed = early_predictions(panel, "drapm").rename(columns={"pred": "pd"})
    early = eo.merge(ed, on=["pid", "season_year"])
    early["prior_provenance"] = "atomic_denominator_early_era_loso"

    rolling = pd.read_parquet(ROLLING)
    rolling = rolling[rolling.season_year.between(2004, 2025)][
        ["pid", "season_year", "po", "pd"]].copy()
    rolling["prior_provenance"] = "atomic_denominator_rolling_past_only"
    prior = pd.concat([early, rolling], ignore_index=True)

    base = feat[["pid", "season_year", "poss"]].rename(
        columns={"poss": "poss_season"})
    prior = prior.merge(base, on=["pid", "season_year"], how="left")
    names = load_player_names()
    prior["player_name"] = prior.pid.map(names)
    prior["target_season"] = prior.season_year.map(season_label)
    prior["prior_o"] = prior.po
    prior["prior_d"] = prior.pd
    prior["prior"] = prior.po + prior.pd
    # Production consumers historically request LOSO columns. Rolling
    # past-only values are stricter than LOSO and intentionally occupy them.
    prior["loso_o"] = prior.po
    prior["loso_d"] = prior.pd
    prior["loso"] = prior.prior
    cols = ["pid", "player_name", "season_year", "target_season",
            "poss_season", "prior_o", "prior_d", "prior", "loso_o",
            "loso_d", "loso", "prior_provenance"]
    prior = prior[cols].sort_values(["season_year", "pid"])
    if prior.duplicated(["pid", "season_year"]).any():
        raise RuntimeError("duplicate atomic production prior keys")
    if prior[["loso_o", "loso_d"]].isna().any().any():
        raise RuntimeError("missing atomic production prior values")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    prior.to_parquet(OUT, index=False)
    prior.to_csv(OUT_CSV, index=False)
    print(prior.groupby("prior_provenance").agg(
        rows=("pid", "size"), seasons=("season_year", "nunique"),
        first=("season_year", "min"), last=("season_year", "max")))
    print(f"wrote {len(prior)} rows -> {OUT}")


if __name__ == "__main__":
    main()
