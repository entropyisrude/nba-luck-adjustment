"""Finish the chain on the atomic possession target: does the
prior-informed re-solve (Phase-3 mechanism) collapse the within-team O/D
seesaws that raw RAPM cannot identify?

Steps: (1) fit the atomic-feature box prior against the atomic target
(honest LOSO values), (2) re-run the possession-target solve for the two
canary target seasons with each player's ridge centered on his prior
((X'WX + aP)b = X'Wy + aP b0; D prior sign-flips into coefficient
space), (3) print raw-target vs prior vs prior-informed O/D for the
canaries (OKC stack 2025-26; Green/Murray/Mobley 2024-25).

Expectation per the standard SPM/RAPM architecture: in directions lineup
data identifies well, data wins; in ambiguous within-stack directions the
prior breaks ties — Ajay Mitchell's +12 D should collapse toward his
profile. If the seesaws SURVIVE this, we genuinely have a problem others
don't.

Usage: python metric/test_prior_informed_atomic.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_rapm_target import (prepare, assemble_design, load_player_names,
                               season_label, DECAY_HALFLIFE_DAYS,
                               WINDOW_DAYS)
from build_box_prior import era_of
from build_box_prior_v3 import fit_predict_w
from build_atomic_features import ATOMS

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
TARGET = METRIC_DATA / "targets" / "rapm_target_poss_hl550.parquet"
COUNTS = METRIC_DATA / "stint_possession_counts.parquet"
V3_CACHE = METRIC_DATA / "features_atomic_season.parquet"

CANARY_SEASONS = [2024, 2025]
SOLVE_ALPHAS = [2000, 4000]
MIN_FIT_POSS = 1700          # both-sides scale ~ old 1000

NAMES_2025 = ["Shai Gilgeous-Alexander", "Jalen Williams", "Chet Holmgren",
              "Isaiah Hartenstein", "Alex Caruso", "Cason Wallace",
              "Ajay Mitchell", "Isaiah Joe", "Aaron Wiggins",
              "Nikola Jokić", "Victor Wembanyama", "Stephen Curry",
              "Rudy Gobert"]
NAMES_2024 = ["Jalen Green", "Jamal Murray", "Evan Mobley",
              "Jalen Williams"]


def main() -> None:
    enc = sys.stdout.encoding or "utf-8"
    def p(s: str) -> None:
        print(s.encode(enc, errors="replace").decode(enc), flush=True)

    # ---- 1. atomic box prior vs atomic target (honest LOSO) -----------
    tgt = pd.read_parquet(TARGET)
    tgt5 = tgt[tgt["alpha"] == 500].copy()
    tgt5["season_year"] = tgt5["target_season"].str[:4].astype(int)
    tgt5 = tgt5.rename(columns={"player_id": "pid"})
    feats = pd.read_parquet(V3_CACHE)
    feats = feats.drop(columns=[c for c in ("games", "mins", "poss")
                                if c in feats.columns])
    df = feats.merge(tgt5, on=["pid", "season_year"], how="inner")
    df["era"] = df["season_year"].map(era_of)
    df = df[df["poss_season"] > 0].reset_index(drop=True)
    w = df["poss_season"].clip(lower=0).to_numpy()
    fit_mask = (df["poss_season"] >= MIN_FIT_POSS).to_numpy()
    p(f"prior fit rows: {len(df)} ({int(fit_mask.sum())} fit-eligible)")
    fit, _ = fit_predict_w(df, ATOMS, w, w, fit_mask)
    prior = fit[["pid", "season_year", "loso_o", "loso_d",
                 "prior_o", "prior_d"]].copy()
    # LOSO where available, in-sample prior as fallback (edge seasons)
    prior["po"] = prior["loso_o"].fillna(prior["prior_o"]).fillna(0.0)
    prior["pd_"] = prior["loso_d"].fillna(prior["prior_d"]).fillna(0.0)

    # ---- 2. atomic design (same as build_rapm_target_poss) ------------
    st_raw = prepare()
    st, X, _y_old, players, hidx, aidx = assemble_design(st_raw)
    P = len(players)
    n = len(st)
    counts = pd.read_parquet(COUNTS)
    st["gid_n"] = st["game_id"].astype(str).str.lstrip("0")
    st = st.merge(counts[["gid_n", "stint_index", "n_home", "n_away",
                          "pts_home", "pts_away"]],
                  on=["gid_n", "stint_index"], how="left")
    for c in ("n_home", "n_away", "pts_home", "pts_away"):
        st[c] = st[c].fillna(0.0)

    hk = pd.Series([",".join(map(str, r)) for r in
                    np.sort(st[[f"home_p{i}" for i in range(1, 6)]]
                            .to_numpy(), 1)], index=st.index)
    ak = pd.Series([",".join(map(str, r)) for r in
                    np.sort(st[[f"away_p{i}" for i in range(1, 6)]]
                            .to_numpy(), 1)], index=st.index)
    st["_luck_h"] = st["home_pts_adj"] - st["home_pts"]
    st["_luck_a"] = st["away_pts_adj"] - st["away_pts"]
    grp = st.groupby(["gid_n", hk, ak])
    for side in ("h", "a"):
        lg = grp[f"_luck_{side}"].transform("sum")
        ng = grp[f"n_{'home' if side == 'h' else 'away'}"].transform("sum")
        nn = st[f"n_{'home' if side == 'h' else 'away'}"]
        share = np.where(ng > 0, lg * nn / ng.replace(0, np.nan), 0.0)
        st[f"pts_adj_{side}"] = (
            st[f"pts_{'home' if side == 'h' else 'away'}"]
            + np.nan_to_num(share))

    n_home = st["n_home"].to_numpy(dtype=float)
    n_away = st["n_away"].to_numpy(dtype=float)
    n_rows = np.empty(2 * n)
    n_rows[0::2] = n_home
    n_rows[1::2] = n_away
    y = np.zeros(2 * n)
    y[0::2] = np.divide(st["pts_adj_h"].to_numpy(), n_home,
                        out=np.zeros(n), where=n_home > 0) * 100.0
    y[1::2] = np.divide(st["pts_adj_a"].to_numpy(), n_away,
                        out=np.zeros(n), where=n_away > 0) * 100.0

    dates = st["date"].to_numpy()
    sy_arr = st["season_year"].to_numpy()
    name_map = load_player_names()
    pidx = {int(pl): i for i, pl in enumerate(players)}
    pen = np.ones(2 * P + 1)
    pen[2 * P] = 0.0

    # ---- 3. prior-informed solves for the canary seasons --------------
    for sy in CANARY_SEASONS:
        label = season_label(sy)
        sel_season = sy_arr == sy
        end = dates[sel_season].max()
        age_days = (end - dates).astype("timedelta64[D]").astype(float)
        decay = np.exp(-np.log(2) * age_days / DECAY_HALFLIFE_DAYS)
        decay[(age_days < 0) | (age_days > WINDOW_DAYS)] = 0.0
        w_rows = n_rows * np.repeat(decay, 2)
        used = w_rows > 0
        Xs = X[used]
        ys = y[used]
        ws = w_rows[used]
        ybar = np.average(ys, weights=ws)
        Xw = Xs.multiply(np.sqrt(ws)[:, None]).tocsr()
        yw = (ys - ybar) * np.sqrt(ws)
        XtX = (Xw.T @ Xw).toarray()
        Xty = Xw.T @ yw

        b0 = np.zeros(2 * P + 1)
        pr = prior[prior["season_year"] == sy]
        n_centered = 0
        for r in pr.itertuples(index=False):
            i = pidx.get(int(r.pid))
            if i is None:
                continue
            b0[i] = r.po
            b0[P + i] = -r.pd_          # D prior sign-flips
            n_centered += 1

        # per-player penalty from the target's own SEs: players whose
        # O/D split the lineup data barely identifies (large SE — the
        # sandwich covariance sees within-stack collinearity that raw
        # possession counts cannot) get pulled harder toward their
        # profile; well-identified players keep their evidence.
        se_rows = tgt[(tgt["alpha"] == 500)
                      & (tgt["target_season"] == label)]
        se_o_map = dict(zip(se_rows["player_id"], se_rows["se_o"]))
        se_d_map = dict(zip(se_rows["player_id"], se_rows["se_d"]))
        med_o = float(np.nanmedian(se_rows["se_o"]))
        med_d = float(np.nanmedian(se_rows["se_d"]))
        pen_pp = np.ones(2 * P + 1)
        pen_pp[2 * P] = 0.0
        for j, pl in enumerate(players):
            so = se_o_map.get(int(pl))
            sd_ = se_d_map.get(int(pl))
            ro = np.clip((so / med_o) ** 2, 0.25, 25.0) \
                if so and np.isfinite(so) else 1.0
            rd = np.clip((sd_ / med_d) ** 2, 0.25, 25.0) \
                if sd_ and np.isfinite(sd_) else 1.0
            pen_pp[j] = ro
            pen_pp[P + j] = rd

        raw = tgt[(tgt["alpha"] == 500)
                  & (tgt["target_season"] == label)].set_index("player_id")
        want = NAMES_2025 if sy == 2025 else NAMES_2024
        p(f"\n===== {label}: {n_centered} players prior-centered =====")
        for alpha, use_pp in [(4000, False), (2000, True), (4000, True)]:
            pv = pen_pp if use_pp else pen
            tag = "per-player SE pen" if use_pp else "uniform"
            beta = np.linalg.solve(XtX + alpha * np.diag(pv),
                                   Xty + alpha * (pv * b0))
            O = beta[:P]
            D = beta[P:2 * P]
            om, dm = O.mean(), D.mean()
            p(f"\nalpha={alpha} ({tag}):")
            p(f"{'player':>24} {'rawO':>6} {'rawD':>6} | "
              f"{'priO':>6} {'priD':>6} | {'PI_O':>6} {'PI_D':>6}")
            for nm in want:
                cand = [pl for pl in players
                        if name_map.get(int(pl)) == nm]
                if not cand:
                    continue
                pid = int(cand[0])
                i = pidx[pid]
                ro = raw["orapm"].get(pid, np.nan)
                rd = raw["drapm"].get(pid, np.nan)
                prow = pr[pr["pid"] == pid]
                po_ = float(prow["po"].iloc[0]) if len(prow) else np.nan
                pd2 = float(prow["pd_"].iloc[0]) if len(prow) else np.nan
                p(f"{nm:>24} {ro:6.2f} {rd:6.2f} | {po_:6.2f} {pd2:6.2f}"
                  f" | {O[i] - om:6.2f} {-(D[i] - dm):6.2f}")


if __name__ == "__main__":
    main()
