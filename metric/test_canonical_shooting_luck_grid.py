"""Tune 3PT, free-throw, and mid-range luck removal independently.

This is a predictive reliability diagnostic, not a causal value estimate.  It
uses the canonical counted-possession production design, fits one RAPM per
season, and asks how well season-t estimates predict raw season-(t+1) RAPM.
All shooting expectations are strictly pre-game in their upstream builders.

Ridge estimates are linear in the outcome, so each season needs only four
multi-output fits (raw scoring plus the three luck components).  The complete
5 x 5 x 5 lambda grid is then evaluated algebraically.
"""
from __future__ import annotations

import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from counted_production_design import exposure_arrays, load_design


ROOT = Path(__file__).resolve().parents[1]
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
OUT = ROOT / "outputs" / "contextual_causal"
PREPARED = METRIC_DATA / "prepared_stints.parquet"
FT = METRIC_DATA / "ft_stint_adjust.parquet"
MR = METRIC_DATA / "midrange_stint_adjust.parquet"

HCOLS = [f"home_p{i}" for i in range(1, 6)]
ACOLS = [f"away_p{i}" for i in range(1, 6)]
LAMBDAS = (0.0, 0.25, 0.5, 0.75, 1.0)
ALPHA = 150.0
MIN_POSS = 1000.0
COMPONENTS = ("raw", "three", "ft", "mid")


def norm_game(values: pd.Series) -> pd.Series:
    return values.astype(str).str.split(".").str[0].str.lstrip("0").replace("", "0")


def lineup_key(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    arr = np.sort(frame[columns].fillna(-1).to_numpy(dtype=np.int64), axis=1)
    return pd.Series([",".join(map(str, row)) for row in arr], index=frame.index)


def source_luck_groups() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return component totals by game/lineup and by game.

    Raw stint indices are not stable across the canonical reconstruction, so
    the adjustment events first attach to the prepared source stints and are
    then collapsed to the invariant game + sorted-lineup key.
    """
    use = ["game_id", "stint_index", "home_pts", "away_pts",
           "home_pts_adj", "away_pts_adj"] + HCOLS + ACOLS
    st = pd.read_parquet(PREPARED, columns=use)
    st["game_id"] = norm_game(st.game_id)
    st["hk"] = lineup_key(st, HCOLS)
    st["ak"] = lineup_key(st, ACOLS)

    ft = pd.read_parquet(FT)
    mr = pd.read_parquet(MR)
    for frame in (ft, mr):
        frame["game_id"] = norm_game(frame.game_id)
    st = st.merge(ft, on=["game_id", "stint_index"], how="left")
    st = st.merge(mr, on=["game_id", "stint_index"], how="left")
    luck_cols = ["ft_luck_home", "ft_luck_away",
                 "mr_luck_home", "mr_luck_away"]
    st[luck_cols] = st[luck_cols].fillna(0.0)
    st["three_home"] = st.home_pts_adj - st.home_pts
    st["three_away"] = st.away_pts_adj - st.away_pts
    st = st.rename(columns={
        "ft_luck_home": "ft_home", "ft_luck_away": "ft_away",
        "mr_luck_home": "mid_home", "mr_luck_away": "mid_away",
    })
    value_cols = [f"{name}_{side}" for name in ("three", "ft", "mid")
                  for side in ("home", "away")]
    groups = st.groupby(["game_id", "hk", "ak"], as_index=False)[value_cols].sum()
    games = groups.groupby("game_id", as_index=False)[value_cols].sum()
    return groups, games


def allocate_to_counted(counted: pd.DataFrame, groups: pd.DataFrame,
                        games: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Place each game-side component on counted stints without losing totals.

    Matched lineup groups receive their exact component.  Any residual from a
    reconstructed lineup mismatch is spread over that game's counted
    possessions.  This retains exact game totals while avoiding a false stint
    index match.
    """
    c = counted.copy()
    c["game_id"] = norm_game(c.game_id)
    c["hk"] = c.home_lineup_key.str.replace("-", ",", regex=False)
    c["ak"] = c.away_lineup_key.str.replace("-", ",", regex=False)
    c["_row"] = np.arange(len(c))
    c = c.merge(groups, on=["game_id", "hk", "ak"], how="left", indicator=True)
    matched = c._merge.eq("both")
    audit = {
        "counted_row_lineup_match_rate": float(matched.mean()),
        "counted_possession_lineup_match_rate": float(np.average(
            matched, weights=c.n_home.to_numpy(float) + c.n_away.to_numpy(float))),
    }
    game_values = games.set_index("game_id")

    for name in ("three", "ft", "mid"):
        for side in ("home", "away"):
            value = f"{name}_{side}"
            ncol = f"n_{side}"
            group_n = c.groupby(["game_id", "hk", "ak"])[ncol].transform("sum")
            exact = (c[value].fillna(0.0).to_numpy(float)
                     * c[ncol].to_numpy(float)
                     / group_n.replace(0, np.nan).fillna(1).to_numpy(float))
            assigned = pd.Series(exact, index=c.index).groupby(c.game_id).transform("sum")
            wanted = c.game_id.map(game_values[value]).fillna(0.0).to_numpy(float)
            residual = wanted - assigned.to_numpy(float)
            game_n = c.groupby("game_id")[ncol].transform("sum").clip(lower=1e-9)
            c[f"_{value}"] = exact + residual * c[ncol].to_numpy(float) / game_n

            by_game = c.groupby("game_id")[f"_{value}"].sum()
            common = by_game.index.intersection(game_values.index)
            err = (by_game.loc[common] - game_values.loc[common, value]).abs().max()
            audit[f"max_game_total_error_{value}"] = float(err)
    return c.sort_values("_row"), audit


def outcome_components(design: dict) -> tuple[np.ndarray, dict]:
    groups, games = source_luck_groups()
    counted, audit = allocate_to_counted(design["counted"], groups, games)
    n = len(counted)
    total_rows = len(design["y"])
    three = np.zeros(total_rows)
    ft = np.zeros(total_rows)
    mid = np.zeros(total_rows)

    for offset, side in ((0, "home"), (1, "away")):
        poss = counted[f"n_{side}"].to_numpy(float)
        for target, name, sign in ((three, "three", 1.0),
                                   (ft, "ft", -1.0),
                                   (mid, "mid", -1.0)):
            points = counted[f"_{name}_{side}"].to_numpy(float)
            np.divide(sign * points * 100.0, poss,
                      out=target[offset:2*n:2], where=poss > 0)

    # Aggregate fallbacks are one directed offense observation per game-side.
    agg = design["aggregate"]
    obs = (agg.drop_duplicates("observation_id")
           [["observation_id", "game_id", "offense_side",
             "possessions_proxy"]].reset_index(drop=True))
    obs["game_id"] = norm_game(obs.game_id)
    game_values = games.set_index("game_id")
    start = 2 * n
    for i, row in obs.iterrows():
        side = row.offense_side
        poss = float(row.possessions_proxy)
        if poss <= 0 or row.game_id not in game_values.index:
            continue
        three[start+i] = 100.0 * game_values.at[row.game_id, f"three_{side}"] / poss
        ft[start+i] = -100.0 * game_values.at[row.game_id, f"ft_{side}"] / poss
        mid[start+i] = -100.0 * game_values.at[row.game_id, f"mid_{side}"] / poss

    raw = design["y"] - three
    components = np.column_stack([raw, three, ft, mid])
    audit["rows"] = int(total_rows)
    audit["counted_stints"] = int(n)
    audit["aggregate_observations"] = int(len(obs))
    audit["max_base_reconstruction_error"] = float(
        np.max(np.abs(raw + three - design["y"])))
    return components, audit


def fit_component_ratings(design: dict, outcomes: np.ndarray) -> pd.DataFrame:
    rows = []
    P = design["P"]
    for season in sorted(np.unique(design["seasons"])):
        mask = (design["seasons"] == season) & (design["poss"] > 0)
        X = design["X"][mask]
        active = np.asarray(X.getnnz(axis=0)).ravel() > 0
        Xa = X[:, active]
        weight = design["poss"][mask]
        y = outcomes[mask].copy()
        y -= np.average(y, axis=0, weights=weight)
        model = Ridge(alpha=ALPHA, fit_intercept=False, solver="lsqr",
                      tol=1e-6, max_iter=1500)
        model.fit(Xa, y, sample_weight=weight)
        full = np.zeros((len(COMPONENTS), 2 * P))
        full[:, active] = model.coef_
        observed = active[:P] | active[P:]
        for j in range(len(COMPONENTS)):
            full[j, :P][observed] -= full[j, :P][observed].mean()
            full[j, P:][observed] -= full[j, P:][observed].mean()

        season_dates = design["dates"][design["seasons"] == season]
        end = np.max(season_dates)
        exposure, _ = exposure_arrays(design, int(season), end, 550.0, 6*365)
        for idx in np.flatnonzero(observed & (exposure > 0)):
            rec = {"player_id": int(design["players"][idx]),
                   "season_year": int(season), "poss": float(exposure[idx])}
            for j, name in enumerate(COMPONENTS):
                rec[f"{name}_o"] = float(full[j, idx])
                rec[f"{name}_d"] = float(-full[j, P+idx])
            rows.append(rec)
        print(f"fit {season}: {int(observed.sum()):,} players", flush=True)
    return pd.DataFrame(rows)


def wcorr(a: np.ndarray, b: np.ndarray, w: np.ndarray) -> float:
    am, bm = np.average(a, weights=w), np.average(b, weights=w)
    da, db = a-am, b-bm
    den = np.sqrt(np.average(da*da, weights=w) * np.average(db*db, weights=w))
    return float(np.average(da*db, weights=w) / den) if den > 0 else np.nan


def prediction_panel(ratings: pd.DataFrame) -> pd.DataFrame:
    cur = ratings.copy()
    cur["target_season"] = cur.season_year + 1
    target = ratings[["player_id", "season_year", "poss", "raw_o", "raw_d"]].rename(
        columns={"season_year": "target_season", "poss": "target_poss",
                 "raw_o": "target_o", "raw_d": "target_d"})
    panel = cur.merge(target, on=["player_id", "target_season"], how="inner")
    return panel[(panel.poss >= MIN_POSS) & (panel.target_poss >= MIN_POSS)].copy()


def score(panel: pd.DataFrame, l3: float, lft: float, lmid: float,
          by_season: bool = False) -> dict:
    po = (panel.raw_o + l3*panel.three_o + lft*panel.ft_o + lmid*panel.mid_o)
    pd_ = (panel.raw_d + l3*panel.three_d + lft*panel.ft_d + lmid*panel.mid_d)
    to, td = panel.target_o.to_numpy(float), panel.target_d.to_numpy(float)
    w = panel.target_poss.to_numpy(float)
    out = {"lambda_3pt": l3, "lambda_ft": lft, "lambda_mid": lmid,
           "total": wcorr((po+pd_).to_numpy(), to+td, w),
           "offense": wcorr(po.to_numpy(), to, w),
           "defense": wcorr(pd_.to_numpy(), td, w), "n": int(len(panel))}
    if by_season:
        out["season_scores"] = []
        for sy, g in panel.groupby("season_year"):
            if len(g) < 10:
                continue
            q = score(g, l3, lft, lmid, False)
            q["season_year"] = int(sy)
            out["season_scores"].append(q)
    return out


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    design = load_design()
    outcomes, audit = outcome_components(design)
    ratings = fit_component_ratings(design, outcomes)
    ratings.to_parquet(OUT / "shooting_luck_component_ratings.parquet", index=False)
    panel = prediction_panel(ratings)

    grid = pd.DataFrame([score(panel, *values) for values in
                         itertools.product(LAMBDAS, repeat=3)])
    grid.to_csv(OUT / "shooting_luck_lambda_grid.csv", index=False)
    marginal = []
    for component, column in (("3pt", "lambda_3pt"),
                              ("ft", "lambda_ft"),
                              ("mid", "lambda_mid")):
        for value, g in grid.groupby(column):
            marginal.append({"component": component, "lambda": float(value),
                             "mean_total": float(g.total.mean()),
                             "mean_offense": float(g.offense.mean()),
                             "mean_defense": float(g.defense.mean())})
    pd.DataFrame(marginal).to_csv(
        OUT / "shooting_luck_lambda_marginals.csv", index=False)

    best = {metric: grid.loc[grid[metric].idxmax()].to_dict()
            for metric in ("total", "offense", "defense")}
    named = {
        "raw": score(panel, 0.0, 0.0, 0.0, True),
        "current_3pt_only": score(panel, 1.0, 0.0, 0.0, True),
        "old_combined_075": score(panel, 1.0, 0.75, 0.75, True),
        "all_full": score(panel, 1.0, 1.0, 1.0, True),
        "best_total": score(panel, best["total"]["lambda_3pt"],
                            best["total"]["lambda_ft"],
                            best["total"]["lambda_mid"], True),
    }
    summary = {"method": {
        "target": "next-season raw canonical counted-possession RAPM",
        "alpha": ALPHA, "minimum_possessions_each_season": MIN_POSS,
        "lambdas": list(LAMBDAS), "seasons": sorted(map(int, ratings.season_year.unique())),
        "prediction_rows": int(len(panel)),
    }, "attachment_audit": audit, "best": best, "named": named}
    (OUT / "shooting_luck_lambda_grid_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps({"audit": audit, "best": best,
                      "named": {k: {m: v[m] for m in ("lambda_3pt", "lambda_ft",
                                                       "lambda_mid", "total",
                                                       "offense", "defense")}
                                for k, v in named.items()}}, indent=2))


if __name__ == "__main__":
    main()
