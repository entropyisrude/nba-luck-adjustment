"""Phase 4a: aging curves from our own data.

Delta-method aging curves on the Phase 1 multi-year luck-adjusted RAPM target
(alpha 500): for every player with consecutive qualifying seasons, the year-
over-year change in orapm/drapm is attributed to the age at the midpoint;
weighted (harmonic mean of the two seasons' possessions) means per integer
age are then smoothed with a weighted quadratic local fit.

These curves are the drift term of the Phase 4 Kalman filter (how much skill
should be expected to move between seasons, before seeing any new evidence).

Known bias, documented: the delta method conditions on playing both seasons
(survivor bias) — real decline is steeper than measured at old ages, real
improvement slightly understated at young ages. Fine for a drift prior.

Output: nba-metric-data/aging/aging_curves.csv (age, n, w, d_orapm, d_drapm,
smoothed variants) and a per-pair file for later reuse.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
RS_DB = ROOT / "data" / "nba_analytics.duckdb"
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
TARGET_PATH = METRIC_DATA / "targets" / "rapm_target_hl550.parquet"
OUT_DIR = METRIC_DATA / "aging"

TARGET_ALPHA = 500
MIN_POSS = 1500


def load_ages() -> pd.DataFrame:
    con = duckdb.connect(str(RS_DB), read_only=True)
    df = con.execute("""
        SELECT CAST(player_id AS BIGINT) player_id,
               CAST(substr(season, 1, 4) AS INTEGER) season_year,
               avg(age) age
        FROM player_game_facts
        WHERE age IS NOT NULL
        GROUP BY 1, 2
    """).df()
    con.close()
    return df


def smooth(ages: np.ndarray, vals: np.ndarray, w: np.ndarray, grid: np.ndarray,
           bw: float = 2.0) -> np.ndarray:
    """Weighted local quadratic fit (tricube kernel, fixed bandwidth in years)."""
    out = np.empty(len(grid))
    for i, a in enumerate(grid):
        d = np.abs(ages - a) / bw
        k = np.where(d < 1, (1 - d ** 3) ** 3, 0.0) * w
        if k.sum() <= 0:
            out[i] = np.nan
            continue
        Xl = np.column_stack([np.ones_like(ages), ages - a, (ages - a) ** 2])
        WX = Xl * k[:, None]
        beta = np.linalg.lstsq(WX.T @ Xl + 1e-6 * np.eye(3), WX.T @ vals, rcond=None)[0]
        out[i] = beta[0]
    return out


def main() -> None:
    t = pd.read_parquet(TARGET_PATH)
    t = t[t["alpha"] == TARGET_ALPHA].copy()
    t["season_year"] = t["target_season"].str[:4].astype(int)
    ages = load_ages()
    t = t.merge(ages, on=["player_id", "season_year"], how="left")

    a = t[t["poss_season"] >= MIN_POSS][
        ["player_id", "season_year", "age", "orapm", "drapm", "poss_season"]]
    b = a.copy()
    b["season_year"] -= 1
    pairs = a.merge(b, on=["player_id", "season_year"], suffixes=("", "_next"))
    pairs = pairs.dropna(subset=["age"])
    pairs["w"] = 2.0 / (1.0 / pairs["poss_season"] + 1.0 / pairs["poss_season_next"])
    pairs["age_mid"] = pairs["age"] + 0.5
    pairs["d_o"] = pairs["orapm_next"] - pairs["orapm"]
    pairs["d_d"] = pairs["drapm_next"] - pairs["drapm"]
    print(f"{len(pairs)} consecutive-season pairs (>= {MIN_POSS} poss both years)")

    grid = np.arange(19.0, 40.5, 0.5)
    ag = pairs["age_mid"].to_numpy(dtype=float)
    w = pairs["w"].to_numpy(dtype=float)
    sm_o = smooth(ag, pairs["d_o"].to_numpy(dtype=float), w, grid)
    sm_d = smooth(ag, pairs["d_d"].to_numpy(dtype=float), w, grid)

    # raw per-integer-age table for reference
    pairs["age_i"] = pairs["age_mid"].round().astype(int)
    raw = pairs.groupby("age_i").apply(
        lambda g: pd.Series({
            "n": len(g),
            "w": g["w"].sum(),
            "d_orapm_raw": np.average(g["d_o"], weights=g["w"]),
            "d_drapm_raw": np.average(g["d_d"], weights=g["w"]),
        }), include_groups=False).reset_index()

    out = pd.DataFrame({"age": grid, "d_orapm": sm_o, "d_drapm": sm_d,
                        "d_rapm": sm_o + sm_d})
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_DIR / "aging_curves.csv", index=False)
    raw.to_csv(OUT_DIR / "aging_deltas_by_age.csv", index=False)
    pairs.to_parquet(OUT_DIR / "aging_pairs.parquet", index=False)
    print(f"Wrote {OUT_DIR}")
    show = out[out["age"] % 1 == 0]
    print(show.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
