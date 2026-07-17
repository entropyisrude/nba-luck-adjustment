"""Does the seconds/24 stint-possession approximation misattribute
offensive-rebounding value from the offense ledger to the defense ledger?

Hypothesis (from the v3 coefficient review): oreb_opp_rate shows a near
mirror image (O -0.24 / D +0.25) because OREBs EXTEND true possessions.
poss = seconds/24 overcounts possessions for OREB-heavy floor time, so
points-per-"possession" is diluted for BOTH offenses in those stints:
the OREB lineup's offense looks worse than truth and its defense better
(the opponent really had fewer possessions than seconds/24 implies).
More generally, seconds/24 makes the target points-per-TIME in disguise,
polluting the O/D split with pace/possession-length while leaving totals
(margins) intact.

Test (2019-20+ regular seasons, where PBP carries the `possession` field —
the team id holding the ball, flips on def rebound/turnover/make, stays
fixed across OREBs; see test_firstchance_rapm.py):
  1. Count TRUE possessions per stint per side: a possession is a run of
     constant `possession` value within a game; it is attributed to the
     stint containing its LAST event (possessions that straddle stint
     boundaries go to the stint where they ended — sloppiness accepted
     and documented). Sanity: per-game true possessions per side (~99)
     vs the seconds/24 book (=120 + OT); stint-level discrepancy should
     correlate with the stint's OREB count.
  2. Solve the SAME pooled stint sample twice, per season (alpha 500,
     luck-adjusted y, no decay, no home column, per-season joint solves):
     (i) status quo: y = pts_adj / (seconds/24) * 100, shared denominator
         for both rows, row weight = seconds/24;
     (ii) true: y = pts_adj / (own side's true possessions) * 100,
         PER-SIDE denominators, row weight = own true possessions
         (rows with 0 attributed possessions drop; denominator floored
         at 0.5 to keep rare pts>0/poss=0 rows finite before weighting).
  3. Per-player-season orapm/drapm shift (true - std) correlated with
     oreb_opp_rate / dreb_opp_rate / height. Prediction: Delta-O rises
     and Delta-D falls with OREB rate.
  4. Refit the 35-atom ridge (single 2019+ bucket) against both targets;
     compare oreb_opp_rate's O and D coefficients.

Usage: PYTHONIOENCODING=utf-8 python metric/test_possession_denominator.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.linear_model import Ridge

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_rapm_target import prepare, HCOLS, ACOLS, MIN_SECONDS, RS_DB
from build_atomic_features import ATOMS

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PBP = METRIC_DATA / "PlayByPlay.parquet"
ATOMIC = METRIC_DATA / "features_atomic_season.parquet"
SEASONS = list(range(2019, 2026))
ALPHA = 500
RIDGE_ALPHA = 50.0
# Solves restricted to stints >= this length (BOTH modes, same sample):
# short stints have 0-2 integer possession counts per side, and boundary
# attribution noise (possession assigned to the stint where it ENDED)
# dominates the denominator — the first, unrestricted run produced
# per-side solves whose O/D split was ~50% noise (corr std-vs-true 0.5)
# while totals stayed at 0.96. Selection on SECONDS is exogenous to the
# OREB/pace treatment, so it doesn't bias the comparison.
SOLVE_MIN_SEC = 90.0


def p(s: str) -> None:
    enc = sys.stdout.encoding or "utf-8"
    print(s.encode(enc, errors="replace").decode(enc), flush=True)


def elapsed_from(clock: pd.Series, period: pd.Series):
    m = clock.str.extract(r"PT(\d+)M([\d.]+)S")
    clock_s = (pd.to_numeric(m[0], errors="coerce") * 60
               + pd.to_numeric(m[1], errors="coerce"))
    per = period.astype(float)
    plen = np.where(per <= 4, 720.0, 300.0)
    prior = np.where(per <= 4, (per - 1) * 720.0,
                     2880.0 + (per - 5) * 300.0)
    return prior + (plen - clock_s)


def home_team_map() -> pd.DataFrame:
    con = duckdb.connect(str(RS_DB), read_only=True)
    hm = con.execute("""
        SELECT DISTINCT CAST(game_id AS VARCHAR) game_id,
               CAST(team_id AS BIGINT) team_id, home_away
        FROM player_game_facts WHERE CAST(game_id AS VARCHAR) LIKE '2%'
    """).df()
    con.close()
    ha = hm["home_away"].astype(str).str.upper().str.startswith("H")
    return hm[ha][["game_id", "team_id"]].rename(
        columns={"team_id": "home_tid"}).drop_duplicates("game_id")


def true_possessions_per_stint(st: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect()
    q = f"""
    SELECT gameId, actionNumber, period, clock, possession,
           actionType, subType, gameDateTimeEst
    FROM read_parquet('{PBP.as_posix()}')
    WHERE gameId LIKE '2%' AND possession IS NOT NULL
    """
    d = con.execute(q).df()
    con.close()
    d["season_year"] = (pd.to_datetime(d["gameDateTimeEst"], errors="coerce")
                        .dt.year
                        - (pd.to_datetime(d["gameDateTimeEst"],
                                          errors="coerce").dt.month < 10))
    d = d[d["season_year"].isin(SEASONS)].copy()
    p(f"PBP possession-field rows: {len(d):,} "
      f"({d['season_year'].min()}-{d['season_year'].max()})")
    d["game_id"] = d["gameId"].astype(str).str.lstrip("0")
    d = d.dropna(subset=["actionNumber"])
    d = d.sort_values(["game_id", "actionNumber"])
    d["poss"] = pd.to_numeric(d["possession"], errors="coerce")
    d = d[d["poss"] > 0]

    hm = home_team_map()
    d = d.merge(hm, on="game_id", how="inner")
    share = (d["poss"] == d["home_tid"]).mean()
    d["is_home_poss"] = d["poss"] == d["home_tid"]
    p(f"possession==home_tid share: {share:.3f} (rest away; unmatched ids "
      f"{(~(d['poss'].isin(d['home_tid']) | True)).sum()})")

    d["grp"] = (d.groupby("game_id")["poss"]
                .transform(lambda s: (s != s.shift()).cumsum()))
    d["elapsed"] = elapsed_from(d["clock"], d["period"])
    d = d.dropna(subset=["elapsed"])
    # OREB tally per possession-group for the sanity check
    d["is_orb"] = ((d["actionType"].str.lower() == "rebound")
                   & (d["subType"].astype(str).str.lower() == "offensive"))
    ends = (d.groupby(["game_id", "grp"])
            .agg(end_elapsed=("elapsed", "max"),
                 is_home_poss=("is_home_poss", "last"),
                 n_orb=("is_orb", "sum"),
                 season_year=("season_year", "first"))
            .reset_index())
    p(f"true possessions counted: {len(ends):,} "
      f"({ends['is_home_poss'].mean():.1%} home)")

    stc = st[["game_id", "stint_index", "start_elapsed"]].copy()
    stc = stc.sort_values(["game_id", "start_elapsed"]).reset_index(drop=True)
    ends = ends[ends["game_id"].isin(set(stc["game_id"]))]
    j = pd.merge_asof(ends.sort_values("end_elapsed").reset_index(drop=True),
                      stc.sort_values("start_elapsed"),
                      left_on="end_elapsed", right_on="start_elapsed",
                      by="game_id", direction="backward")
    j = j.dropna(subset=["stint_index"])
    agg = (j.groupby(["game_id", "stint_index", "is_home_poss"])
           .agg(n=("grp", "size"), orb=("n_orb", "sum")).reset_index())
    piv = agg.pivot_table(index=["game_id", "stint_index"],
                          columns="is_home_poss", values=["n", "orb"],
                          aggfunc="sum", fill_value=0)
    piv.columns = [f"{a}_{'h' if b else 'a'}" for a, b in piv.columns]
    return piv.reset_index()


def solve_seasons(st: pd.DataFrame, mode: str) -> pd.DataFrame:
    """Per-season joint O/D solves. mode='std' (seconds/24, shared) or
    'true' (per-side true possessions)."""
    out = []
    for sy in SEASONS:
        g = st[(st["season_year"] == sy)
               & (st["seconds"] >= SOLVE_MIN_SEC)]
        if not len(g):
            continue
        g = g.reset_index(drop=True)
        players = np.unique(g[HCOLS + ACOLS].to_numpy().astype(int).ravel())
        pidx = {q: i for i, q in enumerate(players)}
        P = len(players)
        n = len(g)
        lookup = np.vectorize(pidx.get)
        hidx = lookup(g[HCOLS].to_numpy().astype(int))
        aidx = lookup(g[ACOLS].to_numpy().astype(int))
        rows, cols, vals = [], [], []
        r = np.arange(n)
        for k in range(5):
            rows += [2 * r, 2 * r]
            cols += [hidx[:, k], P + aidx[:, k]]
            vals += [np.ones(n), np.ones(n)]
            rows += [2 * r + 1, 2 * r + 1]
            cols += [aidx[:, k], P + hidx[:, k]]
            vals += [np.ones(n), np.ones(n)]
        X = sparse.csr_matrix(
            (np.concatenate(vals),
             (np.concatenate(rows), np.concatenate(cols))),
            shape=(2 * n, 2 * P))

        if mode == "std":
            den_h = den_a = np.maximum(g["seconds"].to_numpy() / 24.0, 0.1)
        else:
            den_h = g["tp_h"].to_numpy(dtype=float)
            den_a = g["tp_a"].to_numpy(dtype=float)
        y = np.empty(2 * n)
        y[0::2] = (g["home_pts_adj"].to_numpy()
                   / np.maximum(den_h, 0.5) * 100.0)
        y[1::2] = (g["away_pts_adj"].to_numpy()
                   / np.maximum(den_a, 0.5) * 100.0)
        w = np.empty(2 * n)
        w[0::2] = den_h
        w[1::2] = den_a
        keep = w > 0
        Xs, ys, ws = X[keep], y[keep], w[keep]
        ybar = np.average(ys, weights=ws)
        Xw = Xs.multiply(np.sqrt(ws)[:, None]).tocsr()
        yw = (ys - ybar) * np.sqrt(ws)
        XtX = (Xw.T @ Xw).toarray()
        Xty = Xw.T @ yw
        beta = np.linalg.solve(XtX + ALPHA * np.eye(2 * P), Xty)
        O, D = beta[:P], beta[P:]
        om, dm = O.mean(), D.mean()
        w_on = np.zeros(P)
        base = np.maximum(g["seconds"].to_numpy() / 24.0, 0.1)
        for k in range(5):
            for idxs in (hidx[:, k], aidx[:, k]):
                np.add.at(w_on, idxs, base)
        out.append(pd.DataFrame({
            "pid": players, "season_year": sy,
            f"orapm_{mode}": O - om, f"drapm_{mode}": -(D - dm),
            "solve_poss": w_on}))
        p(f"  {sy}: n_stints={n}, players={P} solved ({mode})")
    return pd.concat(out, ignore_index=True)


def wcorr(a, b, w) -> float:
    a, b, w = np.asarray(a, float), np.asarray(b, float), np.asarray(w, float)
    am, bm = np.average(a, weights=w), np.average(b, weights=w)
    cov = np.average((a - am) * (b - bm), weights=w)
    return cov / np.sqrt(np.average((a - am) ** 2, weights=w)
                         * np.average((b - bm) ** 2, weights=w))


def refit_atoms(df: pd.DataFrame, feats: pd.DataFrame, tcol: str) -> pd.Series:
    m = df.merge(feats, on=["pid", "season_year"], how="inner")
    m = m[m["solve_poss"] >= 1000].copy()
    use = [c for c in ATOMS]
    for c in use:
        if m[c].isna().any():
            m[c] = m[c].fillna(m[c].mean())
    X = m[use].to_numpy(dtype=float)
    w = m["solve_poss"].to_numpy()
    mu = np.average(X, axis=0, weights=w)
    sd = np.sqrt(np.average((X - mu) ** 2, axis=0, weights=w)) + 1e-9
    r = Ridge(alpha=RIDGE_ALPHA)
    r.fit((X - mu) / sd, m[tcol].to_numpy(), sample_weight=w)
    return pd.Series(r.coef_, index=use)


def main() -> None:
    st = prepare(adjustments=("ft", "mr"))
    st = st.dropna(subset=HCOLS + ACOLS).copy()
    st = st[st["seconds"] >= MIN_SECONDS]
    st["season_year"] = st["date"].dt.year - (st["date"].dt.month < 10)
    st = st[(st["season_year"].isin(SEASONS)) & (st["is_playoff"] == 0)]
    st = st.reset_index(drop=True)
    p(f"RS stints {SEASONS[0]}-{SEASONS[-1]}: {len(st):,}")

    tp = true_possessions_per_stint(st)
    st = st.merge(tp.rename(columns={"n_h": "tp_h", "n_a": "tp_a",
                                     "orb_h": "orb_h", "orb_a": "orb_a"}),
                  on=["game_id", "stint_index"], how="left")
    st[["tp_h", "tp_a", "orb_h", "orb_a"]] = (
        st[["tp_h", "tp_a", "orb_h", "orb_a"]].fillna(0.0))

    # ---- sanity: game totals + stint-level discrepancy vs OREBs ----------
    gt = st.groupby("game_id").agg(sec=("seconds", "sum"),
                                   tph=("tp_h", "sum"), tpa=("tp_a", "sum"))
    gt["book24"] = gt["sec"] / 24.0
    p(f"\nper-game per-side possessions: true home median "
      f"{gt['tph'].median():.1f}, true away {gt['tpa'].median():.1f}, "
      f"seconds/24 book {gt['book24'].median():.1f}")
    # premise check, rate-based (levels are confounded by stint length and
    # pace): do OREBs lengthen a stint's seconds-per-possession?
    sub = st[(st["seconds"] >= SOLVE_MIN_SEC)
             & ((st["tp_h"] + st["tp_a"]) >= 4)].copy()
    spp = sub["seconds"] / (sub["tp_h"] + sub["tp_a"])
    orb_rate = (sub["orb_h"] + sub["orb_a"]) / (sub["tp_h"] + sub["tp_a"])
    r_prem = np.corrcoef(spp, orb_rate)[0, 1]
    p(f"premise: corr(seconds-per-possession, OREBs-per-possession) = "
      f"{r_prem:+.3f} (n={len(sub):,}; mechanism predicts positive)")

    # ---- the two solves ---------------------------------------------------
    p("\nsolving per season, status quo (seconds/24):")
    a = solve_seasons(st, "std")
    p("solving per season, true per-side possessions:")
    b = solve_seasons(st, "true")
    df = a.merge(b.drop(columns=["solve_poss"]), on=["pid", "season_year"])
    df["d_o"] = df["orapm_true"] - df["orapm_std"]
    df["d_d"] = df["drapm_true"] - df["drapm_std"]

    feats = pd.read_parquet(ATOMIC)
    m = df.merge(feats[["pid", "season_year", "oreb_opp_rate",
                        "dreb_opp_rate", "height"]],
                 on=["pid", "season_year"], how="inner")
    m = m[m["solve_poss"] >= 1500]
    p(f"\nshift analysis (n={len(m)} player-seasons, solve_poss>=1500):")
    for feat in ("oreb_opp_rate", "dreb_opp_rate", "height"):
        ro = wcorr(m["d_o"], m[feat], m["solve_poss"])
        rd = wcorr(m["d_d"], m[feat], m["solve_poss"])
        p(f"  {feat:>14}: corr with Delta-O {ro:+.3f}, Delta-D {rd:+.3f}")
    p("  (hypothesis: OREB rate -> Delta-O positive, Delta-D negative)")
    m["sum_std"] = m["orapm_std"] + m["drapm_std"]
    m["sum_true"] = m["orapm_true"] + m["drapm_true"]
    p(f"  totals stability: corr(total_std, total_true) = "
      f"{wcorr(m['sum_std'], m['sum_true'], m['solve_poss']):+.3f}; "
      f"O: {wcorr(m['orapm_std'], m['orapm_true'], m['solve_poss']):+.3f}; "
      f"D: {wcorr(m['drapm_std'], m['drapm_true'], m['solve_poss']):+.3f}")

    top = m.reindex(m["d_o"].abs().sort_values(ascending=False).index).head(8)
    names = feats[["pid", "season_year"]].copy()
    p("\nlargest O-shifts (pid, season, oreb_rate, d_o, d_d):")
    p(top[["pid", "season_year", "oreb_opp_rate", "d_o", "d_d"]]
      .round(3).to_string(index=False))

    # ---- coefficient check ------------------------------------------------
    p("\n35-atom ridge, single 2019+ bucket, key coefficients:")
    p(f"{'target':>12} {'oreb_opp':>9} {'dreb_opp':>9} {'height':>8} {'blk':>7}")
    for tcol in ("orapm_std", "orapm_true", "drapm_std", "drapm_true"):
        c = refit_atoms(df, feats, tcol)
        p(f"{tcol:>12} {c['oreb_opp_rate']:>9.3f} {c['dreb_opp_rate']:>9.3f} "
          f"{c['height']:>8.3f} {c['blk_75']:>7.3f}")


if __name__ == "__main__":
    main()
