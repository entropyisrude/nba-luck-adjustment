"""Phase 5b: the fit/compatibility layer, v1 (hand-crafted trait axes).

Do certain KINDS of players make each other better, beyond what the additive
metric predicts? Model: for each stint team-row (offense lineup vs defense
lineup, luck-adjusted pts per 100), take the residual after the additive
metric_v0 baseline and regress it on pairwise trait-product features:

    synergy_off(lineup) = sum_{i<j} t_i' W_o t_j     (offensive traits)
    synergy_def(lineup) = sum_{i<j} d_i' W_d d_j     (defensive traits)

with W symmetric, so each lineup collapses to K(K+1)/2 pair-sum features
(computed as (ss' - sum_i t_i t_i')/2 with s the lineup trait sum). Linear
trait sums + home indicator ride along as controls so the pair terms can't
absorb additive or HCA effects. Traits are per-season possession-weighted
z-scores of box-profile axes (interpretable v1; learned factors are v2).

Validation (strict temporal, train <=2018, test 2019+):
  * held-out stint residual: poss-weighted correlation / MSE reduction,
  * the headline: aggregate held-out 5-man lineups with >=300 poss and ask
    whether predicted synergy ranks which lineups beat their talent.

Outputs (OUT_DIR): fit_pair_coefficients.csv (the learned W, ranked) and
fit_lineup_validation.parquet.

RESULTS (2026-07-10, v1): no ex-ante signal at this granularity. Held-out
lineup wcorr by config: full-era temporal 0.03; modern-era temporal 0.03;
short-horizon (2019-21 -> 22+) 0.11 (~1.9 se); cross-matchup terms 0.04.
Random game split gives 0.38 — but that is same-period lineups leaking
through their trait coordinates (persistent lineup/team identity), not
transferable fit. Within-period coefficients are interpretable basketball
(playmk*space +, oreb*oreb - in the modern era), so the layer works as a
DESCRIPTIVE tool; as a predictive adjustment it needs richer traits or
multi-season lineup pooling. Judge any v2 on temporal splits only.

Usage: python metric/build_fit_layer.py [--split random] [--min-season Y]
       [--train-max Y] [--cross]
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
STINTS_PATH = METRIC_DATA / "prepared_stints.parquet"
METRIC_PATH = METRIC_DATA / "metric" / "metric_v0.parquet"
FEATURES_PATH = METRIC_DATA / "features_box_season.parquet"
OUT_DIR = METRIC_DATA / "fit"

TRAIN_MAX = 2018          # train <= this season, test after
MIN_SECONDS = 30.0
RIDGE = 1e4               # on the pair features (poss-weighted design)
LINEUP_MIN_POSS = 300.0   # held-out lineup aggregation threshold

# trait axes: (name, source column). z-scored per season, poss-weighted.
OFF_TRAITS = [("usage", "usg"), ("playmk", "ast_pct"), ("space", "fg3a_75"),
              ("rim", "pct_paint"), ("oreb", "oreb_pct")]
DEF_TRAITS = [("rimprot", "blk_75"), ("steal", "stl_75"),
              ("dreb", "dreb_pct"), ("size", "height")]

HCOLS = [f"home_p{i}" for i in range(1, 6)]
ACOLS = [f"away_p{i}" for i in range(1, 6)]


def zscore_by_season(feats: pd.DataFrame, col: str) -> pd.Series:
    def z(g):
        w = g["poss"].clip(lower=1)
        x = g[col].astype(float)
        mask = x.notna()
        mu = np.average(x[mask], weights=w[mask])
        sd = np.sqrt(np.average((x[mask] - mu) ** 2, weights=w[mask])) + 1e-9
        return ((x - mu) / sd).fillna(0.0)
    return feats.groupby("season_year", group_keys=False).apply(z, include_groups=False)


def build_trait_table() -> tuple[pd.DataFrame, list[str], list[str]]:
    feats = pd.read_parquet(FEATURES_PATH)
    onames = [n for n, _ in OFF_TRAITS]
    dnames = [n for n, _ in DEF_TRAITS]
    for name, col in OFF_TRAITS + DEF_TRAITS:
        feats[name] = zscore_by_season(feats, col)
    return feats[["pid", "season_year"] + onames + dnames], onames, dnames


def pair_features(T: np.ndarray) -> np.ndarray:
    """T: (N, 5, K) lineup trait tensor -> (N, K*(K+1)//2) pair-sum features
    sum_{i<j} (t_i t_j' + t_j t_i')/2 vech'd."""
    s = T.sum(axis=1)                                   # (N, K)
    outer_s = np.einsum("nk,nl->nkl", s, s)
    self_ = np.einsum("nik,nil->nkl", T, T)
    M = (outer_s - self_) / 2.0                         # (N, K, K) symmetric
    K = T.shape[2]
    iu = np.triu_indices(K)
    scale = np.where(iu[0] == iu[1], 1.0, 2.0)          # off-diag counted twice
    return M[:, iu[0], iu[1]] * scale


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--split", choices=["temporal", "random"], default="temporal",
                    help="random = hold out 25%% of games (era-matched)")
    ap.add_argument("--min-season", type=int, default=1996,
                    help="restrict to seasons >= this (e.g. 2014 = spacing era)")
    ap.add_argument("--train-max", type=int, default=TRAIN_MAX,
                    help="temporal split boundary (train <= this)")
    ap.add_argument("--cross", action="store_true",
                    help="add offense-trait x opposing-defense-trait matchup "
                         "terms (s_off outer s_def)")
    args = ap.parse_args()

    st = pd.read_parquet(STINTS_PATH)
    st = st[st["seconds"] >= MIN_SECONDS].copy()
    st["date"] = pd.to_datetime(st["date"])
    st["season_year"] = st["date"].dt.year - (st["date"].dt.month < 10)
    st = st[st["season_year"] >= args.min_season]
    st["poss"] = np.maximum(st["seconds"].to_numpy() / 24.0, 0.1)
    print(f"{len(st)} stints, seasons {st.season_year.min()}-{st.season_year.max()}")

    traits, onames, dnames = build_trait_table()
    Ko, Kd = len(onames), len(dnames)

    met = pd.read_parquet(METRIC_PATH).rename(columns={"player_id": "pid"})
    met = met[["pid", "season_year", "m4000_o", "m4000_d"]]

    # (pid, season) -> row index into aligned arrays; 0-row = fallback
    key = traits.merge(met, on=["pid", "season_year"], how="outer")
    key = key.fillna({c: 0.0 for c in onames + dnames})
    key["m4000_o"] = key["m4000_o"].fillna(-0.5)
    key["m4000_d"] = key["m4000_d"].fillna(-0.5)
    key = key.reset_index(drop=True)
    idx = {(int(p), int(s)): i + 1
           for i, (p, s) in enumerate(zip(key["pid"], key["season_year"]))}
    TO = np.vstack([np.zeros((1, Ko)), key[onames].to_numpy(float)]).astype(np.float32)
    TD = np.vstack([np.zeros((1, Kd)), key[dnames].to_numpy(float)]).astype(np.float32)
    MO = np.concatenate([[-0.5], key["m4000_o"].to_numpy(float)]).astype(np.float32)
    MD = np.concatenate([[-0.5], key["m4000_d"].to_numpy(float)]).astype(np.float32)

    sy = st["season_year"].to_numpy()
    lk = np.vectorize(lambda p, s: idx.get((p, s), 0))
    H = np.stack([lk(st[c].to_numpy().astype(int), sy) for c in HCOLS], axis=1)
    A = np.stack([lk(st[c].to_numpy().astype(int), sy) for c in ACOLS], axis=1)
    miss = float((H == 0).mean() + (A == 0).mean()) / 2
    print(f"trait/metric coverage: {1 - miss:.1%} of lineup slots")

    # two rows per stint: (offense lineup, defense lineup, y, is_home)
    poss = st["poss"].to_numpy(np.float32)
    yh = (st["home_pts_adj"].to_numpy(np.float32) / poss * 100.0)
    ya = (st["away_pts_adj"].to_numpy(np.float32) / poss * 100.0)
    OFFI = np.vstack([H, A])            # offense lineup key-rows
    DEFI = np.vstack([A, H])
    y = np.concatenate([yh, ya])
    w = np.concatenate([poss, poss])
    is_home = np.concatenate([np.ones(len(st)), np.zeros(len(st))]).astype(np.float32)
    seas = np.concatenate([sy, sy])
    game = np.concatenate([st["game_id"].to_numpy()] * 2)

    base = MO[OFFI].sum(axis=1) - MD[DEFI].sum(axis=1)
    resid = y - base

    Toff = TO[OFFI]                     # (N, 5, Ko)
    Tdef = TD[DEFI]
    Xo = pair_features(Toff)
    Xd = pair_features(Tdef)
    so, sd = Toff.sum(axis=1), Tdef.sum(axis=1)
    blocks = [Xo, Xd]
    iu_o = np.triu_indices(Ko); iu_d = np.triu_indices(Kd)
    fnames = ([f"O:{onames[i]}*{onames[j]}" for i, j in zip(*iu_o)]
              + [f"D:{dnames[i]}*{dnames[j]}" for i, j in zip(*iu_d)])
    if args.cross:
        blocks.append(np.einsum("nk,nl->nkl", so, sd).reshape(len(so), -1))
        fnames += [f"X:{a}*{b}" for a in onames for b in dnames]
    lin = np.concatenate([so, sd], axis=1)
    n_pair = len(fnames)
    X = np.concatenate(blocks + [lin, is_home[:, None],
                                 np.ones((len(y), 1), np.float32)],
                       axis=1).astype(np.float64)
    fnames += ([f"lin_o:{n}" for n in onames] + [f"lin_d:{n}" for n in dnames]
               + ["home", "intercept"])

    if args.split == "temporal":
        tr = seas <= args.train_max
    else:
        rng = np.random.default_rng(7)
        games = np.unique(game)
        te_games = set(rng.choice(games, size=len(games) // 4, replace=False))
        tr = ~np.isin(game, list(te_games))
    te = ~tr
    print(f"split={args.split} min_season={args.min_season}  "
          f"rows: train {tr.sum()}, test {te.sum()}, features {X.shape[1]}")

    def fit(Xt, rt, wt):
        pen = np.full(Xt.shape[1], RIDGE)
        pen[n_pair:] = 1.0              # controls barely penalized
        A_ = (Xt * wt[:, None]).T @ Xt + np.diag(pen)
        b_ = (Xt * wt[:, None]).T @ rt
        return np.linalg.solve(A_, b_)

    beta = fit(X[tr], resid[tr], w[tr])
    coefs = pd.DataFrame({"feature": fnames, "coef": beta}).iloc[:n_pair]
    coefs = coefs.reindex(coefs["coef"].abs().sort_values(ascending=False).index)

    # held-out: synergy-only prediction of residual (controls excluded so we
    # judge the pair terms specifically)
    syn_cols = np.arange(n_pair)
    syn_te = X[te][:, syn_cols] @ beta[syn_cols]
    ctl_te = X[te][:, n_pair:] @ beta[n_pair:]
    r_te = resid[te]

    def wstats(pred, actual, wts):
        mse0 = np.average((actual - np.average(actual, weights=wts)) ** 2, weights=wts)
        mse1 = np.average((actual - pred - np.average(actual - pred, weights=wts)) ** 2,
                          weights=wts)
        am = np.average(pred, weights=wts); bm = np.average(actual, weights=wts)
        cov = np.average((pred - am) * (actual - bm), weights=wts)
        sda = np.sqrt(np.average((pred - am) ** 2, weights=wts))
        sdb = np.sqrt(np.average((actual - bm) ** 2, weights=wts))
        return cov / (sda * sdb + 1e-12), (mse0 - mse1) / mse0 * 1e4
    c1, bp = wstats(syn_te, r_te - ctl_te, w[te])
    print(f"\nHeld-out stint level (2019+): synergy corr {c1:.4f}, "
          f"MSE reduction {bp:.2f} bp")

    # headline: held-out LINEUP aggregation
    wte = w[te]
    dfte = pd.DataFrame({"season_year": seas[te], "w": wte,
                         "ws": syn_te * wte, "wr": (r_te - ctl_te) * wte})
    lu = np.sort(OFFI[te], axis=1)
    for k in range(5):
        dfte[f"p{k}"] = lu[:, k]
    ag = (dfte.groupby(["season_year"] + [f"p{k}" for k in range(5)])
          [["w", "ws", "wr"]].sum().reset_index())
    ag = ag[ag["w"] >= LINEUP_MIN_POSS].rename(columns={"w": "poss"})
    ag["syn"] = ag["ws"] / ag["poss"]
    ag["resid"] = ag["wr"] / ag["poss"]
    cw, _ = wstats(ag["syn"].to_numpy(), ag["resid"].to_numpy(),
                   ag["poss"].to_numpy())
    print(f"Held-out lineups >= {LINEUP_MIN_POSS:.0f} poss (n={len(ag)}): "
          f"synergy vs talent-residual wcorr {cw:.4f}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"feature": fnames, "coef": beta}).to_csv(
        OUT_DIR / "fit_pair_coefficients.csv", index=False)
    ag.reset_index().to_parquet(OUT_DIR / "fit_lineup_validation.parquet",
                                index=False)
    print(f"\nTop pair terms (train <= {TRAIN_MAX}):")
    print(coefs.head(12).round(4).to_string(index=False))
    print("\nBottom (anti-synergy):")
    print(coefs.tail(4).round(4).to_string(index=False))


if __name__ == "__main__":
    main()
