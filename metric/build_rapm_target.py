"""Phase 1 of the all-in-one metric: the multi-year luck-adjusted RAPM target.

For every target season 1996-97 .. 2025-26, computes recency-weighted RAPM
over a trailing multi-year window (exponential decay, DECAY_HALFLIFE_DAYS),
from luck-adjusted stint data, regular season + playoffs combined.

The stint inputs get the same hygiene treatment the playoff pipeline got:

  1. chain  — stint start scores chained to the previous stint's end (the RS
     lineup_stint_facts table leaks ~7.7 pts/game through inter-stint gaps);
  2. calibrate — per game, bounded ridge least-squares minimally adjusts
     per-stint points so every player's stint-sum equals the official box +/-
     and team totals equal the official score (scripts/
     calibrate_playoff_stints_to_official.py machinery, RS official sources).

Playoff stints come from data/stints_playoffs.csv, which is already chained
and calibrated on disk. Prepared stints are cached to PREPARED_CACHE (delete
it to force re-preparation).

Output: one parquet/csv per run in OUT_DIR with rows (target_season,
player_id, player_name, alpha, orapm, drapm, rapm, w_poss, poss_season,
po_share, se_o, se_d, se_rapm). orapm = points added per 100 possessions on
offense vs average; drapm = points prevented per 100 on defense vs average;
rapm = sum. se_* are analytic sandwich standard errors (alpha = SE_ALPHA
rows only, None elsewhere): Cov(beta) = A^-1 (X'W S W X) A^-1 with the
decay component of W treated as an importance weight — validated against a
game-block bootstrap by metric/validate_target_se.py.

Usage: python metric/build_rapm_target.py [--prepare-only]
"""
from __future__ import annotations

import argparse
import os
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import duckdb
from scipy import sparse
from scipy.optimize import lsq_linear

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
sys.path.insert(0, str(ROOT / "scripts"))

RS_DB = ROOT / "data" / "nba_analytics.duckdb"
PO_STINTS = ROOT / "data" / "stints_playoffs.csv"
EOIN_ZIP = ROOT / "historical-nba-data-and-player-box-scores.zip"
_KAGGLE_CANDIDATES = [
    Path(r"C:\Users\Dave\Downloads\nba-boxscore-data\kaggle-traditional\traditional.csv"),
    Path("/mnt/c/users/dave/Downloads/nba-boxscore-data/kaggle-traditional/traditional.csv"),
]
KAGGLE_BOX = next((p for p in _KAGGLE_CANDIDATES if p.exists()), None)

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PREPARED_CACHE = METRIC_DATA / "prepared_stints.parquet"
OUT_DIR = METRIC_DATA / "targets"

MAX_CHAIN_GAP = 8          # pts; bigger inter-stint jumps are real coverage holes
TOTAL_TOL = 3.0            # pts; games whose stint totals are further off get no calibration
DECAY_HALFLIFE_DAYS = 550  # ~1.5 seasons
WINDOW_DAYS = 6 * 365      # decay is ~1/16 by then; hard cutoff for tractability
ALPHAS = [150, 500, 2000]
SE_ALPHA = 500       # alpha the analytic standard errors are computed at
                     # (the one build_box_prior.py consumes as its target)
# The analytic sandwich runs ~25% conservative: stint calibration anchors
# each game's scores to fixed official totals, so within-game residuals are
# NEGATIVELY correlated and partially cancel in the solve — c_hat, estimated
# from marginal per-stint residuals, can't see that cancellation. Factors
# measured vs 150-rep game-block bootstraps (validate_target_se.py):
# 2024-25 O/D/total 0.761/0.788/0.779, 2010-11 0.757/0.769/0.751 —
# era-stable, so one global constant per component.
SE_CALIBRATION = {"o": 0.76, "d": 0.78, "rapm": 0.77}
MIN_SECONDS = 1.0

HCOLS = [f"home_p{i}" for i in range(1, 6)]
ACOLS = [f"away_p{i}" for i in range(1, 6)]


# --------------------------------------------------------------------------
# Stage A: prepare (load + chain + calibrate) — cached
# --------------------------------------------------------------------------

def load_official_rs() -> tuple[dict, dict]:
    """pm[(gid,pid)] and totals[gid]=(home,away) for regular-season games."""
    pm: dict[tuple[str, int], float] = {}
    totals: dict[str, tuple[float, float]] = {}
    if KAGGLE_BOX is not None:
        kag = pd.read_csv(KAGGLE_BOX, usecols=["gameid", "type", "playerid", "team",
                                               "home", "away", "PTS", "+/-"],
                          dtype={"gameid": str}, low_memory=False)
        kag = kag[kag["type"].str.lower() == "regular"].copy()
        kag["pid"] = pd.to_numeric(kag["playerid"], errors="coerce")
        kag["pm"] = pd.to_numeric(kag["+/-"], errors="coerce")
        kag = kag.dropna(subset=["pid", "pm"])
        for r in kag.itertuples(index=False):
            pm[(r.gameid, int(r.pid))] = float(r.pm)
        tp = kag.groupby(["gameid", "team"])["PTS"].sum()
        meta = kag.drop_duplicates("gameid")[["gameid", "home", "away"]]
        for r in meta.itertuples(index=False):
            try:
                totals[r.gameid] = (float(tp[(r.gameid, r.home)]), float(tp[(r.gameid, r.away)]))
            except KeyError:
                pass
        print(f"  kaggle RS: {len(totals)} games")

    kaggle_games = {g for g, _ in pm}
    if EOIN_ZIP.exists():
        with zipfile.ZipFile(EOIN_ZIP) as z:
            with z.open("PlayerStatistics.csv") as f:
                eo = pd.read_csv(f, usecols=["gameId", "gameType", "personId", "points",
                                             "plusMinusPoints", "home"], low_memory=False)
        eo = eo[eo["gameType"] == "Regular Season"].copy()
        eo["gid"] = eo["gameId"].astype(str).str.lstrip("0")
        eo = eo[~eo["gid"].isin(kaggle_games)]
        eo["pid"] = pd.to_numeric(eo["personId"], errors="coerce")
        eo["pm"] = pd.to_numeric(eo["plusMinusPoints"], errors="coerce")
        eo = eo.dropna(subset=["pid", "pm"])
        added = 0
        team_pts = eo.groupby(["gid", "home"])["points"].sum()
        for gid in eo["gid"].unique():
            if gid in totals:
                continue
            try:
                totals[gid] = (float(team_pts[(gid, 1)]), float(team_pts[(gid, 0)]))
                added += 1
            except KeyError:
                continue
            for r in eo[eo["gid"] == gid].itertuples(index=False):
                pm.setdefault((gid, int(r.pid)), float(r.pm))
        print(f"  eoin RS: +{added} games")
    return pm, totals


def chain_game(g: pd.DataFrame) -> pd.DataFrame:
    """Chain start scores to previous end; recompute raw pts from the chained
    snapshots and shift adjusted pts by the same delta (no adjusted-score
    snapshots exist in lineup_stint_facts — the luck delta per stint is kept)."""
    g = g.sort_values("stint_index").copy()
    ph = pa = 0.0
    for i in g.index:
        sh, sa = g.at[i, "start_home_score"], g.at[i, "start_away_score"]
        gap = (sh - ph) + (sa - pa)
        if 0 < gap <= MAX_CHAIN_GAP:
            g.at[i, "start_home_score"], g.at[i, "start_away_score"] = ph, pa
        new_h = g.at[i, "end_home_score"] - g.at[i, "start_home_score"]
        new_a = g.at[i, "end_away_score"] - g.at[i, "start_away_score"]
        g.at[i, "home_pts_adj"] = g.at[i, "home_pts_adj"] + (new_h - g.at[i, "home_pts"])
        g.at[i, "away_pts_adj"] = g.at[i, "away_pts_adj"] + (new_a - g.at[i, "away_pts"])
        g.at[i, "home_pts"] = new_h
        g.at[i, "away_pts"] = new_a
        ph, pa = g.at[i, "end_home_score"], g.at[i, "end_away_score"]
    return g


def calibrate_game(g: pd.DataFrame, pm: dict, totals: tuple[float, float]) -> bool:
    """Bounded ridge LS pulling stint points to official player pm + team totals.
    Mutates g's home_pts/away_pts (+adj by same delta). Returns True if applied."""
    n = len(g)
    h = g["home_pts"].to_numpy(dtype=float)
    a = g["away_pts"].to_numpy(dtype=float)
    gid = str(g["game_id"].iloc[0])
    if abs(h.sum() - totals[0]) > TOTAL_TOL or abs(a.sum() - totals[1]) > TOTAL_TOL:
        return False

    hv = g[HCOLS].to_numpy()
    av = g[ACOLS].to_numpy()
    rows, rhs = [], []
    for vals, is_home in [(hv, True), (av, False)]:
        for p in pd.unique(vals[~pd.isna(vals)].astype(int).ravel()):
            official = pm.get((gid, int(p)))
            if official is None:
                continue
            mask = (vals == p).any(axis=1)
            cur = (h[mask] - a[mask]).sum() if is_home else (a[mask] - h[mask]).sum()
            row = np.zeros(2 * n)
            row[:n][mask] = 1.0 if is_home else -1.0
            row[n:][mask] = -1.0 if is_home else 1.0
            rows.append(row)
            rhs.append(official - cur)
    rh = np.zeros(2 * n); rh[:n] = 1.0
    ra = np.zeros(2 * n); ra[n:] = 1.0
    rows += [rh, ra]
    rhs += [totals[0] - h.sum(), totals[1] - a.sum()]

    A = np.vstack(rows)
    b = np.asarray(rhs, dtype=float)
    lam = 1e-3
    A_aug = np.vstack([A, np.sqrt(lam) * np.eye(2 * n)])
    b_aug = np.concatenate([b, np.zeros(2 * n)])
    sol = lsq_linear(A_aug, b_aug, bounds=(np.concatenate([-h, -a]), np.full(2 * n, np.inf)),
                     tol=1e-10, max_iter=200)
    dh, da = sol.x[:n], sol.x[n:]
    h_new = np.maximum(h + dh, 0.0)
    a_new = np.maximum(a + da, 0.0)
    g.loc[:, "home_pts_adj"] = g["home_pts_adj"].to_numpy(dtype=float) + (h_new - h)
    g.loc[:, "away_pts_adj"] = g["away_pts_adj"].to_numpy(dtype=float) + (a_new - a)
    g.loc[:, "home_pts"] = h_new
    g.loc[:, "away_pts"] = a_new
    return True


# Post-hoc luck adjustments applied on top of the cached stints. Each was
# validated on the fixed-target evidence board before shipping:
#   ft: free-throw luck (0.4271 -> 0.4311; autocorr 0.4394 -> 0.4454)
#   mr: mid-range 10-23ft luck (on FT base: total 0.4339 -> 0.4367,
#       O 0.6522 -> 0.6585, D 0.5508 -> 0.5649)
LUCK_FILES = {
    "ft": (METRIC_DATA / "ft_stint_adjust.parquet", "ft_luck_home", "ft_luck_away"),
    "mr": (METRIC_DATA / "midrange_stint_adjust.parquet", "mr_luck_home", "mr_luck_away"),
}
# Fraction of the FT+mid-range luck deviation removed. Lambda grid
# (test_lambda_grid.py, 2026-07-12): full removal over-corrects (some
# above-own-baseline shooting is real in-season form); fixed-target
# boards peak at 0.5-0.75 with 0.75 best on the O and D splits.
# Asymmetric O/D removal tested and rejected (cross-solve incoherence).
LUCK_LAMBDA = 0.75


def _apply_luck(st: pd.DataFrame, adjustments) -> pd.DataFrame:
    for name in adjustments:
        path, ch, ca = LUCK_FILES[name]
        if not path.exists():
            print(f"NOTE: {path.name} missing - {name} luck NOT removed")
            continue
        adj = pd.read_parquet(path)
        st = st.merge(adj, on=["game_id", "stint_index"], how="left")
        st[[ch, ca]] = st[[ch, ca]].fillna(0.0)
        st["home_pts_adj"] = st["home_pts_adj"] - LUCK_LAMBDA * st[ch]
        st["away_pts_adj"] = st["away_pts_adj"] - LUCK_LAMBDA * st[ca]
        st = st.drop(columns=[ch, ca])
    return st


def prepare(adjustments: tuple = ("ft", "mr")) -> pd.DataFrame:
    if PREPARED_CACHE.exists():
        print(f"Using cached {PREPARED_CACHE}")
        st = pd.read_parquet(PREPARED_CACHE)
        return _apply_luck(st, adjustments)

    keep = ["game_id", "date", "stint_index", "seconds",
            "home_pts", "away_pts", "home_pts_adj", "away_pts_adj",
            "start_home_score", "start_away_score",
            "end_home_score", "end_away_score",
            "start_elapsed", "end_elapsed", "start_period"] + HCOLS + ACOLS

    print("Loading RS lineup_stint_facts...")
    con = duckdb.connect(str(RS_DB), read_only=True)
    rs = con.execute(f"SELECT {', '.join(keep)} FROM lineup_stint_facts").df()
    con.close()
    rs["game_id"] = rs["game_id"].astype(str)
    rs["is_playoff"] = 0
    print(f"  {len(rs)} RS stints, {rs['game_id'].nunique()} games")

    print("Loading official RS box data...")
    pm, totals = load_official_rs()

    print("Chaining + calibrating RS games (one-time; cached afterwards)...")
    out = []
    stats = {"cal": 0, "skip_cov": 0, "skip_off": 0}
    for i, (gid, g) in enumerate(rs.groupby("game_id", sort=False)):
        if i % 2000 == 0:
            print(f"  {i}...", flush=True)
        g = chain_game(g)
        if gid in totals:
            if calibrate_game(g, pm, totals[gid]):
                stats["cal"] += 1
            else:
                stats["skip_cov"] += 1
        else:
            stats["skip_off"] += 1
        out.append(g)
    rs = pd.concat(out, ignore_index=True)
    print(f"  calibrated {stats['cal']}, coverage-hole {stats['skip_cov']}, "
          f"no-official {stats['skip_off']}")

    print("Loading playoff stints (already chained+calibrated on disk)...")
    po = pd.read_csv(PO_STINTS, dtype={"game_id": str}, low_memory=False)
    po = po[[c for c in keep if c in po.columns]].copy()
    po["is_playoff"] = 1
    print(f"  {len(po)} playoff stints")

    both = pd.concat([rs, po], ignore_index=True)
    both["date"] = pd.to_datetime(both["date"])
    # stint_index + start scores (post-chaining) + clock fields ride along for
    # leverage weighting and future play-by-play joins (FT variance etc.)
    slim = both[["game_id", "date", "is_playoff", "seconds",
                 "home_pts", "away_pts", "home_pts_adj", "away_pts_adj",
                 "stint_index", "start_home_score", "start_away_score",
                 "start_elapsed", "end_elapsed", "start_period"] + HCOLS + ACOLS]
    METRIC_DATA.mkdir(parents=True, exist_ok=True)
    slim.to_parquet(PREPARED_CACHE, index=False)
    print(f"Cached {len(slim)} stints to {PREPARED_CACHE}")
    return _apply_luck(slim, adjustments)


# --------------------------------------------------------------------------
# Stage B: multi-year decayed weighted ridge RAPM per target season
# --------------------------------------------------------------------------

def season_label(start_year: int) -> str:
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def assemble_design(st: pd.DataFrame):
    """Filter stints and build the global sparse joint design + per-100
    outcome vector. Shared by the target build and the SE validation
    bootstrap (metric/validate_target_se.py) so both see the identical
    design. Returns (st, X, y, players, hidx, aidx)."""
    st = st.dropna(subset=HCOLS + ACOLS).copy()
    st = st[st["seconds"] >= MIN_SECONDS].reset_index(drop=True)
    st["poss"] = np.maximum(st["seconds"].to_numpy() / 24.0, 0.1)
    st["season_year"] = st["date"].dt.year - (st["date"].dt.month < 10)
    print(f"Usable stints: {len(st)}")

    players = np.unique(st[HCOLS + ACOLS].to_numpy().astype(int).ravel())
    pidx = {p: i for i, p in enumerate(players)}
    P = len(players)
    print(f"Players: {P}")

    # player-index arrays (precomputed once)
    n = len(st)
    lookup = np.vectorize(pidx.get)
    hidx = lookup(st[HCOLS].to_numpy().astype(int))   # (n, 5)
    aidx = lookup(st[ACOLS].to_numpy().astype(int))

    # global sparse design: two rows per stint
    rows, cols, vals = [], [], []
    r = np.arange(n)
    for k in range(5):
        # home-offense rows (even): home O (+), away D (+ = allowed)
        rows += [2 * r, 2 * r]
        cols += [hidx[:, k], P + aidx[:, k]]
        vals += [np.ones(n), np.ones(n)]
        # away-offense rows (odd): away O (+), home D (+)
        rows += [2 * r + 1, 2 * r + 1]
        cols += [aidx[:, k], P + hidx[:, k]]
        vals += [np.ones(n), np.ones(n)]
    # home-court column (index 2P, UNPENALIZED in the solves): +0.5 on
    # home-offense rows, -0.5 on away-offense rows, so its coefficient is
    # the full home-minus-away offense gap (~HCA margin per 100). Omitting
    # it leaks a small directional bias into players with >50% home
    # exposure (playoff teams earn extra home games). Zeroed for the
    # 2019-20 bubble playoffs — neutral site, "home" is just a label there.
    d = st["date"].to_numpy()
    bubble = ((d >= np.datetime64("2020-07-01"))
              & (d <= np.datetime64("2020-10-15")))
    hflag = np.where(bubble, 0.0, 0.5)
    rows += [2 * r, 2 * r + 1]
    cols += [np.full(n, 2 * P), np.full(n, 2 * P)]
    vals += [hflag, -hflag]
    X = sparse.csr_matrix((np.concatenate(vals),
                           (np.concatenate(rows), np.concatenate(cols))),
                          shape=(2 * n, 2 * P + 1))

    poss = st["poss"].to_numpy()
    y = np.empty(2 * n)
    y[0::2] = st["home_pts_adj"].to_numpy() / poss * 100.0
    y[1::2] = st["away_pts_adj"].to_numpy() / poss * 100.0
    return st, X, y, players, hidx, aidx


def build_targets(st: pd.DataFrame) -> pd.DataFrame:
    st, X, y, players, hidx, aidx = assemble_design(st)
    P = len(players)
    n = len(st)
    poss = st["poss"].to_numpy()
    dates = st["date"].to_numpy()
    name_map = load_player_names()

    # penalty diagonal: ridge on player columns, home-court column free
    pen = np.ones(2 * P + 1)
    pen[2 * P] = 0.0

    results = []
    for sy in range(1996, 2026):
        label = season_label(sy)
        sel_season = (st["season_year"].to_numpy() == sy)
        if not sel_season.any():
            continue
        end = dates[sel_season].max()
        age_days = (end - dates).astype("timedelta64[D]").astype(float)
        w_st = poss * np.exp(-np.log(2) * age_days / DECAY_HALFLIFE_DAYS)
        w_st[(age_days < 0) | (age_days > WINDOW_DAYS)] = 0.0
        used = w_st > 0
        row_mask = np.repeat(used, 2)

        Xs = X[row_mask]
        ys = y[row_mask]
        ws = np.repeat(w_st[used], 2)
        ybar = np.average(ys, weights=ws)
        Xw = Xs.multiply(np.sqrt(ws)[:, None]).tocsr()
        yw = (ys - ybar) * np.sqrt(ws)

        XtX = (Xw.T @ Xw).toarray()
        Xty = Xw.T @ yw

        # ---- analytic sandwich SEs at SE_ALPHA -------------------------
        # Cov(beta) = A^-1 M A^-1,  A = X'WX + aI,  M = X'W S W X with
        # S = diag(var(y_i)). A stint's per-100 outcome over poss_i
        # possessions has var(y_i) = c / poss_i. The decay part of the
        # weight is an IMPORTANCE weight (old stints are downweighted for
        # staleness, not noisiness), so it enters M squared instead of
        # cancelling like a pure precision weight would — this is why the
        # naive "read variances off A^-1" shortcut is wrong here.
        # c is estimated from the residuals: E[r^2 * poss] = c.
        poss_rows = np.repeat(poss[used], 2)
        A = XtX + SE_ALPHA * np.diag(pen)
        beta0 = np.linalg.solve(A, Xty)
        hca = float(beta0[2 * P])
        resid = (ys - ybar) - Xs @ beta0
        decay_rows = ws / poss_rows
        c_hat = float((decay_rows * resid ** 2 * poss_rows).sum()
                      / decay_rows.sum())
        Xm = Xs.multiply(np.sqrt(ws ** 2 / poss_rows)[:, None]).tocsr()
        M = (Xm.T @ Xm).toarray()
        A_inv = np.linalg.inv(A)
        Cov = A_inv @ M @ A_inv
        Cov *= c_hat
        dg = np.diag(Cov)
        cov_od = Cov[np.arange(P), P + np.arange(P)]
        se_o_arr = SE_CALIBRATION["o"] * np.sqrt(np.maximum(dg[:P], 0.0))
        se_d_arr = SE_CALIBRATION["d"] * np.sqrt(np.maximum(dg[P:2 * P], 0.0))
        # rapm = (O_i - om) - (D_i - dm) -> var = varO + varD - 2cov(O,D)
        se_t_arr = SE_CALIBRATION["rapm"] * np.sqrt(
            np.maximum(dg[:P] + dg[P:2 * P] - 2.0 * cov_od, 0.0))
        del A, A_inv, M, Xm, Cov

        # per-player possession bookkeeping
        w_on = np.zeros(P)
        raw_season = np.zeros(P)
        po_poss = np.zeros(P)
        wsel = w_st[used]
        psel = poss[used]
        in_season = sel_season[used]
        is_po = st["is_playoff"].to_numpy()[used] == 1
        for k in range(5):
            for idxs in (hidx[used, k], aidx[used, k]):
                np.add.at(w_on, idxs, wsel)
                np.add.at(raw_season, idxs, np.where(in_season, psel, 0.0))
                np.add.at(po_poss, idxs, np.where(is_po, wsel, 0.0))

        for alpha in ALPHAS:
            beta = np.linalg.solve(XtX + alpha * np.diag(pen), Xty)
            O = beta[:P]
            D = beta[P:2 * P]
            om, dm = O.mean(), D.mean()
            active = raw_season > 0
            for i in np.nonzero(active)[0]:
                p = int(players[i])
                results.append({
                    "target_season": label, "player_id": p,
                    "player_name": name_map.get(p, str(p)), "alpha": alpha,
                    "orapm": round(O[i] - om, 3),
                    "drapm": round(-(D[i] - dm), 3),
                    "rapm": round((O[i] - om) - (D[i] - dm), 3),
                    "w_poss": round(float(w_on[i]), 1),
                    "poss_season": round(float(raw_season[i]), 1),
                    "po_share": round(float(po_poss[i] / w_on[i]) if w_on[i] > 0 else 0.0, 4),
                    "se_o": round(float(se_o_arr[i]), 3) if alpha == SE_ALPHA else None,
                    "se_d": round(float(se_d_arr[i]), 3) if alpha == SE_ALPHA else None,
                    "se_rapm": round(float(se_t_arr[i]), 3) if alpha == SE_ALPHA else None,
                })
        print(f"  {label}: {int(used.sum())} stints in window, "
              f"{int(active.sum())} active players, c_hat={c_hat:.0f}, "
              f"hca={hca:+.2f}")
    return pd.DataFrame(results)


def load_player_names() -> dict[int, str]:
    con = duckdb.connect(str(RS_DB), read_only=True)
    rows = con.execute("""
        SELECT CAST(player_id AS BIGINT), max_by(player_name, date)
        FROM player_game_facts GROUP BY 1""").fetchall()
    con.close()
    return {int(a): b for a, b in rows}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--prepare-only", action="store_true")
    args = ap.parse_args()

    st = prepare()
    if args.prepare_only:
        return
    out = build_targets(st)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pq = OUT_DIR / f"rapm_target_hl{DECAY_HALFLIFE_DAYS}.parquet"
    out.to_parquet(pq, index=False)
    out.to_csv(pq.with_suffix(".csv"), index=False)
    print(f"Wrote {len(out)} rows to {pq}")

    top = out[(out["target_season"] == "2025-26") & (out["alpha"] == 500)
              & (out["w_poss"] > 4000)].nlargest(15, "rapm")
    print("\nTop 15, 2025-26, alpha=500, min 4000 weighted poss:")
    txt = top[["player_name", "orapm", "drapm", "rapm", "w_poss"]].to_string(index=False)
    # Windows consoles may be cp1252; don't let a diacritic kill the run
    print(txt.encode(sys.stdout.encoding or "utf-8", errors="replace")
             .decode(sys.stdout.encoding or "utf-8"))


if __name__ == "__main__":
    main()
