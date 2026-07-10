"""Phase 5a: game-margin backtest harness vs Vegas closing spreads.

Predicts every game's home margin from minutes-weighted pre-game player
ratings and scores the predictions against actual margins and the closing
spread (kaggle ehallmar odds bundle: consensus median across books,
2006-07..2017-18 RS+PO).

Ratings compared, all strictly pre-game (information through season t-1):
  * kalman  — Phase 4a one-step-ahead state (pred_total for season t)
  * static  — Phase 3 metric_v0 total from season t-1
  * bpm     — BBRef BPM from season t-1 (same protocol)
  * raptor  — 538 RAPTOR total from season t-1 (same protocol)
  * vegas   — closing spread (the market's prediction, the gold standard)

Team strength = sum over rostered players of (minutes/48) x rating; the
margin model  margin ~ a + b*(home_str - away_str)  is calibrated
walk-forward on the trailing 4 seasons, never the test season.

Minutes variants (brackets the lineup-information leak — Vegas does not
know actual minutes):
  * actual — the game's real minutes (favors the models)
  * proj   — trailing within-season average of the player's prior games
             (previous-season MPG for his first game), rescaled to 240

Output: nba-metric-data/backtest/backtest_games.parquet + season summary.
Usage: python metric/backtest_margin.py
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
ODDS_DIR = METRIC_DATA / "odds"
KALMAN_PATH = METRIC_DATA / "kalman" / "kalman_states.parquet"
METRIC_PATH = METRIC_DATA / "metric" / "metric_v0.parquet"
BBREF_DIR = METRIC_DATA / "benchmarks" / "bbref_advanced"
RAPTOR_PATH = METRIC_DATA / "benchmarks" / "historical_RAPTOR_by_player.csv"
OUT_DIR = METRIC_DATA / "backtest"

REPLACEMENT = -2.0        # rating for players with no pre-game rating
CALIB_SEASONS = 4         # trailing seasons for the (a, b) calibration
TEST_SEASONS = range(2006, 2018)   # seasons with closing spreads
# kblendX = kalman blended toward replacement -X by prior-season minutes
# (w = min(prev_min/1500, 1)) — consumer-side fix for the fringe-player
# overrating; the filter-side fix failed the player scoreboards, and the
# backtest's affine calibration can't correct this (it's non-affine)
# jointk = metric_v1 candidate: joint solve with Kalman-state ridge centers
MODELS = ["kalman", "static", "bpm", "raptor", "kblend2", "kblend4"]
BLEND_FULL_MIN = 1500.0
JOINTK_PATH = METRIC_DATA / "metric" / "metric_v1_kcenter.parquet"


def load_games() -> pd.DataFrame:
    ga = pd.read_csv(ODDS_DIR / "nba_games_all.csv", dtype={"game_id": str},
                     usecols=["game_id", "game_date", "team_id", "a_team_id",
                              "is_home", "pts", "season_year", "season_type"])
    ga = ga[ga["season_type"].isin(["Regular Season", "Playoffs"])]
    home = ga[ga["is_home"] == "t"].rename(columns={"team_id": "home_id", "pts": "home_pts"})
    away = ga[ga["is_home"] == "f"][["game_id", "team_id", "pts"]].rename(
        columns={"team_id": "away_id", "pts": "away_pts"})
    g = home.merge(away, on="game_id")
    assert (g["away_id"] == g["a_team_id"]).all()
    g["margin"] = g["home_pts"] - g["away_pts"]
    g = g[g["season_year"] >= 1996]
    g = g.drop_duplicates("game_id")

    sp = pd.read_csv(ODDS_DIR / "nba_betting_spread.csv", dtype={"game_id": str})
    med = (sp.groupby(["game_id", "team_id"])["spread1"].median()
           .rename("spread").reset_index())
    med = med.drop_duplicates("game_id")   # one orientation per game
    g = g.merge(med.rename(columns={"team_id": "spread_team"}),
                on="game_id", how="left")
    # spread = points spread_team RECEIVES; orient to the home team, then
    # market predicted home margin = -home_spread
    home_spread = np.where(g["spread_team"] == g["home_id"],
                           g["spread"], -g["spread"])
    g["vegas"] = -home_spread
    both = g.dropna(subset=["vegas"])
    r = np.corrcoef(both["vegas"], both["margin"])[0, 1]
    print(f"Games {len(g)} ({g['season_year'].min()}-{g['season_year'].max()}), "
          f"{len(both)} with spreads; corr(vegas, margin) = {r:.3f}")
    assert r > 0.4, "spread sign convention looks wrong"
    return g


def load_rosters() -> pd.DataFrame:
    pg = pd.read_csv(ODDS_DIR / "nba_players_game_stats.csv", dtype={"game_id": str},
                     usecols=["game_id", "player_id", "team_id", "min",
                              "season_year", "season_type", "game_date"])
    pg = pg[pg["season_type"].isin(["Regular Season", "Playoffs"])]
    pg = pg[(pg["season_year"] >= 1996) & (pg["min"] > 0)].copy()
    pg = pg.rename(columns={"player_id": "pid"})

    # projected minutes: trailing within-season mean of PRIOR games,
    # previous-season MPG for the season opener, 15 for true rookies
    pg = pg.sort_values(["pid", "game_date"]).reset_index(drop=True)
    prev = (pg.groupby(["pid", "season_year"])["min"]
            .agg(prev_mpg="mean", prev_smin="sum").reset_index())
    prev["season_year"] += 1
    pg = pg.merge(prev, on=["pid", "season_year"], how="left")
    grp = pg.groupby(["pid", "season_year"])["min"]
    prior_sum = grp.cumsum() - pg["min"]
    prior_cnt = grp.cumcount()
    trail = prior_sum / prior_cnt.replace(0, np.nan)
    pg["min_proj"] = trail.fillna(pg["prev_mpg"]).fillna(15.0)
    # rescale each team-game to 240 so projected strengths are comparable
    tp = pg.groupby(["game_id", "team_id"])["min_proj"].transform("sum")
    pg["min_proj"] *= 240.0 / tp
    return pg


def load_ratings() -> dict[str, pd.DataFrame]:
    """Each frame: pid, season_year (the season the rating APPLIES TO,
    built from information through season_year - 1), rating."""
    out = {}
    k = pd.read_parquet(KALMAN_PATH)
    out["kalman"] = (k.rename(columns={"player_id": "pid", "pred_total": "rating"})
                     [["pid", "season_year", "rating"]])
    m = pd.read_parquet(METRIC_PATH)
    m = m.rename(columns={"player_id": "pid"}) if "player_id" in m.columns else m
    m = m[["pid", "season_year", "metric"]].copy()
    m["season_year"] += 1
    out["static"] = m.rename(columns={"metric": "rating"})
    if JOINTK_PATH.exists():
        jk = pd.read_parquet(JOINTK_PATH).rename(columns={"player_id": "pid"})
        jk = jk[["pid", "season_year", "metric"]].copy()
        jk["season_year"] += 1
        out["jointk"] = jk.rename(columns={"metric": "rating"})
        if "jointk" not in MODELS:
            MODELS.append("jointk")

    names = (k.groupby("player_id")["player_name"].last().rename_axis("pid")
             .reset_index())
    bb = []
    for p in sorted(BBREF_DIR.glob("advanced_*.csv")):
        yr = int(p.stem.split("_")[1])
        d = pd.read_csv(p)
        if "BPM" not in d.columns or "Player" not in d.columns:
            continue
        d = d[["Player", "BPM", "MP"]].copy()
        d["MP"] = pd.to_numeric(d["MP"], errors="coerce")
        d = d.sort_values("MP", ascending=False).drop_duplicates("Player")
        d["season_year"] = yr    # measured in yr-1, applies to season yr
        bb.append(d)
    bb = pd.concat(bb, ignore_index=True)
    bb["BPM"] = pd.to_numeric(bb["BPM"], errors="coerce")
    bb = bb.merge(names, left_on="Player", right_on="player_name")
    out["bpm"] = (bb.dropna(subset=["BPM"])
                  [["pid", "season_year", "BPM"]].rename(columns={"BPM": "rating"}))

    rap = pd.read_csv(RAPTOR_PATH)
    rap = rap.merge(names, on="player_name")
    rap = rap.rename(columns={"season": "season_year"})  # 538 season = ending yr
    # measured in season_year-1 (their label), applies to the next one: their
    # season 2016 = 2015-16 = our season_year 2015 -> applies to 2016
    out["raptor"] = (rap.dropna(subset=["raptor_total"])
                     .groupby(["pid", "season_year"])["raptor_total"].mean()
                     .rename("rating").reset_index())
    for name, r in out.items():
        out[name] = r.drop_duplicates(["pid", "season_year"])
    return out


def team_strengths(pg: pd.DataFrame, ratings: dict[str, pd.DataFrame]) -> pd.DataFrame:
    df = pg
    for name, r in ratings.items():
        df = df.merge(r.rename(columns={"rating": name}),
                      on=["pid", "season_year"], how="left")
    cov = {name: 1 - df[name].isna().mean() for name in ratings}
    print("Rating coverage of player-games:",
          {k: f"{v:.1%}" for k, v in cov.items()})
    for name in ratings:
        df[name] = df[name].fillna(REPLACEMENT)
    w = (df["prev_smin"] / BLEND_FULL_MIN).clip(0, 1).fillna(0.0)
    df["kblend2"] = w * df["kalman"] + (1 - w) * -2.0
    df["kblend4"] = w * df["kalman"] + (1 - w) * -4.0
    df["ens"] = (df["static"] + df["kalman"]) / 2.0
    if "ens" not in MODELS:
        MODELS.append("ens")
    rows = []
    for mv, mcol in [("actual", "min"), ("proj", "min_proj")]:
        t = df[["game_id", "team_id"]].copy()
        for name in MODELS:
            t[f"{name}_{mv}"] = df[mcol] / 48.0 * df[name]
        rows.append(t.groupby(["game_id", "team_id"]).sum())
    return pd.concat(rows, axis=1).reset_index()


def main() -> None:
    g = load_games()
    pg = load_rosters()
    ratings = load_ratings()
    ts = team_strengths(pg, ratings)

    cols = [c for c in ts.columns if c not in ("game_id", "team_id")]
    g = g.merge(ts.rename(columns={"team_id": "home_id"})
                .rename(columns={c: "h_" + c for c in cols}),
                on=["game_id", "home_id"], how="inner")
    g = g.merge(ts.rename(columns={"team_id": "away_id"})
                .rename(columns={c: "a_" + c for c in cols}),
                on=["game_id", "away_id"], how="inner")
    for c in cols:
        g["d_" + c] = g["h_" + c] - g["a_" + c]

    # walk-forward calibration and prediction
    preds = []
    for t in TEST_SEASONS:
        tr = g[(g["season_year"] >= t - CALIB_SEASONS) & (g["season_year"] < t)]
        te = g[g["season_year"] == t].copy()
        if not len(te):
            continue
        for c in cols:
            X = np.column_stack([np.ones(len(tr)), tr["d_" + c]])
            beta, *_ = np.linalg.lstsq(X, tr["margin"], rcond=None)
            te["pred_" + c] = beta[0] + beta[1] * te["d_" + c]
        preds.append(te)
    p = pd.concat(preds, ignore_index=True)
    ev = p.dropna(subset=["vegas"]).copy()

    def line(tag, err, corr, extra=""):
        print(f"  {tag:>14}: MAE {err:5.2f}  corr {corr:.3f}{extra}")

    for st, label in [("Regular Season", "REGULAR SEASON"), ("Playoffs", "PLAYOFFS")]:
        e = ev[ev["season_type"] == st]
        if not len(e):
            continue
        print(f"\n{label} ({len(e)} games with spreads, "
              f"{e['season_year'].min()}-{e['season_year'].max()}):")
        line("vegas", (e["vegas"] - e["margin"]).abs().mean(),
             np.corrcoef(e["vegas"], e["margin"])[0, 1])
        for mv in ("actual", "proj"):
            for name in MODELS:
                c = f"pred_{name}_{mv}"
                mae = (e[c] - e["margin"]).abs().mean()
                r = np.corrcoef(e[c], e["margin"])[0, 1]
                edge = e[(e[c] - e["vegas"]).abs() > 0.5]
                ats = (np.sign(edge[c] - edge["vegas"])
                       == np.sign(edge["margin"] - edge["vegas"]))
                push = edge["margin"] != edge["vegas"]
                line(f"{name}-{mv}", mae, r,
                     f"  ATS {ats[push].mean():.3f} (n={int(push.sum())})")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    keep = (["game_id", "game_date", "season_year", "season_type", "home_id",
             "away_id", "margin", "vegas"]
            + [f"pred_{n}_{mv}" for n in MODELS for mv in ("actual", "proj")])
    p[keep].to_parquet(OUT_DIR / "backtest_games.parquet", index=False)
    print(f"\nWrote {len(p)} game predictions to {OUT_DIR / 'backtest_games.parquet'}")

    # season-by-season MAE for the headline model vs the market
    print("\nSeason MAE (kalman-actual vs vegas):")
    for t, s in ev.groupby("season_year"):
        print(f"  {t}: ours {(s['pred_kalman_actual'] - s['margin']).abs().mean():.2f}"
              f"   vegas {(s['vegas'] - s['margin']).abs().mean():.2f} (n={len(s)})")


if __name__ == "__main__":
    main()
