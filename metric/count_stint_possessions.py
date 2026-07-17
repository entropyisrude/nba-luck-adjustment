"""Count possessions AND their points per stint per side, 1996-2026.

One pass through the play-by-play per game: possessions and the points
scored on them are recorded together, atomically — a possession that
straddles a substitution carries its points WITH it to whichever stint it
lands in. This removes the seam that broke the first possession target
(points measured from stint score windows + counts measured separately
disagreed at stint edges, producing impossible stint-lines like 5 points
on 1 possession).

Boundary source:
  * 2019-20+ games: the PBP `possession` field (team holding the ball).
  * pre-2019: event state machine (turnover; made FG unless an and-1
    trip follows; made final FT; rebound by the non-shooting team;
    period change). Validated vs the field on a 2019+ sample.
Points: accumulate on make events (3 if three, else 2) and made free
throws (1) into the current possession's run. Technical FT points go to
a pending credit flushed into the shooting team's next possession.

Output: nba-metric-data/stint_possession_counts.parquet
        (game_id, gid_n, stint_index, n_home, n_away, pts_home, pts_away)

Usage: python metric/count_stint_possessions.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
ROOT = Path(__file__).resolve().parents[1]

PBP = Path(r"C:\Users\Dave\Downloads\nba-metric-data\PlayByPlay.parquet")
RS_DB = ROOT / "data" / "nba_analytics.duckdb"
PO_STINTS = ROOT / "data" / "stints_playoffs.csv"
OUT = Path(r"C:\Users\Dave\Downloads\nba-metric-data\stint_possession_counts.parquet")


def load_events() -> pd.DataFrame:
    """All parsing in SQL; pandas holds compact numerics only."""
    con = duckdb.connect()
    q = f"""
    WITH raw AS (
      SELECT ltrim(CAST(gameId AS VARCHAR), '0') AS gid_n,
             period,
             TRY_CAST(regexp_extract(clock, 'PT(\\d+)M', 1) AS DOUBLE) * 60
               + TRY_CAST(regexp_extract(clock, 'M([\\d.]+)S', 1) AS DOUBLE)
               AS clock_s,
             lower(trim(actionType)) AS atype,
             lower(coalesce(subType, '')) AS st,
             TRY_CAST(teamId AS BIGINT) AS team,
             lower(coalesce(CAST(shotResult AS VARCHAR), '')) AS sr,
             lower(coalesce(description, '')) AS descr,
             TRY_CAST(shotValue AS DOUBLE) AS sv,
             coalesce(orderNumber, actionNumber) AS ord,
             TRY_CAST(possession AS DOUBLE) AS possession
      FROM read_parquet('{PBP.as_posix()}')
      WHERE lower(trim(actionType)) IN
            ('made shot','missed shot','2pt','3pt','turnover','rebound',
             'free throw','freethrow')
        AND (ltrim(CAST(gameId AS VARCHAR), '0') LIKE '2%'
             OR ltrim(CAST(gameId AS VARCHAR), '0') LIKE '4%')
    )
    SELECT gid_n, period, ord, team, possession,
           CASE WHEN period <= 4
                THEN (period - 1) * 720.0 + (720.0 - clock_s)
                ELSE 2880.0 + (period - 5) * 300.0 + (300.0 - clock_s)
           END AS elapsed,
           CASE WHEN atype = 'made shot' THEN 'make'
                WHEN atype = 'missed shot' THEN 'miss'
                WHEN atype IN ('2pt','3pt') AND sr = 'made' THEN 'make'
                WHEN atype IN ('2pt','3pt') AND sr = 'missed' THEN 'miss'
                WHEN atype IN ('free throw','freethrow') THEN 'ft'
                WHEN atype = 'turnover' THEN 'tov'
                WHEN atype = 'rebound' THEN 'reb'
                ELSE 'other' END AS kind,
           CASE WHEN atype = '3pt' OR sv = 3 THEN 3.0 ELSE 2.0 END AS mval,
           CASE WHEN regexp_extract(st || ' ' || descr,
                                    '(\\d) of (\\d)', 1) = ''
                THEN TRUE
                ELSE regexp_extract(st || ' ' || descr, '(\\d) of (\\d)', 1)
                     = regexp_extract(st || ' ' || descr,
                                      '(\\d) of (\\d)', 2)
           END AS ft_final,
           CASE WHEN sr IN ('made','missed') THEN sr = 'made'
                ELSE NOT contains(descr, 'miss') END AS ft_made,
           (contains(st, 'technical') OR contains(descr, 'technical'))
               AS ft_tech
    FROM raw
    WHERE clock_s IS NOT NULL AND period IS NOT NULL
    ORDER BY gid_n, period, elapsed, ord
    """
    ev = con.execute(q).df()
    con.close()
    ev = ev[ev["kind"] != "other"].reset_index(drop=True)
    print(f"events: {len(ev)} across {ev['gid_n'].nunique()} games",
          flush=True)
    return ev


def walk_game(g: pd.DataFrame, use_field: bool):
    """Unified walker -> list of (start_elapsed, team, points).

    Boundary detection differs by era; POINT ACCUMULATION is shared and
    travels with the possession — the atomic pairing that matters.
    """
    kind = g["kind"].to_numpy()
    team = g["team"].to_numpy(dtype=float)
    e = g["elapsed"].to_numpy()
    per = g["period"].to_numpy()
    mval = g["mval"].to_numpy(dtype=float)
    ft_final = g["ft_final"].to_numpy()
    ft_made = g["ft_made"].to_numpy()
    ft_tech = g["ft_tech"].to_numpy()
    fld = pd.to_numeric(g["possession"], errors="coerce").to_numpy() \
        if use_field else None
    n = len(g)

    segs: list[tuple[float, float, float]] = []
    offense = np.nan
    start = e[0] if n else 0.0
    run = 0.0                 # points in the current possession
    last_miss = np.nan
    pending: dict[float, float] = {}   # technical-FT credits by team

    def close(tm: float, at_e: float, next_off: float = np.nan) -> None:
        nonlocal offense, start, run, last_miss
        if np.isfinite(tm):
            segs.append((start if np.isfinite(offense) else at_e, tm, run))
        offense = next_off
        start = at_e
        run = pending.pop(next_off, 0.0) if np.isfinite(next_off) else 0.0
        last_miss = np.nan

    for i in range(n):
        if i > 0 and per[i] != per[i - 1]:
            tm = offense if np.isfinite(offense) else last_miss
            if np.isfinite(tm):
                segs.append((start, tm, run))
            offense, last_miss, run, start = np.nan, np.nan, 0.0, e[i]

        k = kind[i]
        t = team[i]

        if use_field:
            f = fld[i]
            if np.isfinite(f) and f > 0:
                if not np.isfinite(offense):
                    offense, start = f, e[i]
                    run = pending.pop(f, 0.0)
                elif f != offense:
                    segs.append((start, offense, run))
                    offense, start = f, e[i]
                    run = pending.pop(f, 0.0)
            # points accumulate below regardless of flip logic

        if not np.isfinite(t):
            continue
        if not use_field and not np.isfinite(offense) \
                and k in ("make", "miss", "ft", "tov"):
            offense = t
            run += pending.pop(t, 0.0)

        # ---- shared point accumulation ---------------------------------
        if k == "make":
            if np.isfinite(offense) and t == offense:
                run += mval[i]
            else:
                pending[t] = pending.get(t, 0.0) + mval[i]
        elif k == "ft" and ft_made[i]:
            if ft_tech[i] or not (np.isfinite(offense) and t == offense):
                pending[t] = pending.get(t, 0.0) + 1.0
            else:
                run += 1.0

        if use_field:
            continue

        # ---- pre-2019 boundary state machine ---------------------------
        if k == "tov":
            close(t, e[i])
        elif k == "make":
            and1 = False
            j = i + 1
            while j < n and per[j] == per[i] and e[j] - e[i] <= 15.0:
                if kind[j] == "ft" and team[j] == t and not ft_tech[j]:
                    and1 = True
                    break
                if kind[j] in ("make", "miss", "tov", "reb"):
                    break
                j += 1
            if not and1:
                close(t, e[i])
        elif k == "miss":
            last_miss = t
        elif k == "ft":
            if ft_tech[i] or not ft_final[i]:
                continue
            if ft_made[i]:
                close(t, e[i])
            else:
                last_miss = t
        elif k == "reb":
            if np.isfinite(last_miss):
                if t != last_miss:
                    close(last_miss, e[i], next_off=t)
                else:
                    last_miss = np.nan

    tm = offense if np.isfinite(offense) else last_miss
    if np.isfinite(tm):
        segs.append((start, tm, run))
    return segs


def main() -> None:
    ev = load_events()

    field_ok = (ev.assign(pnum=pd.to_numeric(ev["possession"],
                                             errors="coerce"))
                .groupby("gid_n")["pnum"]
                .apply(lambda s: s.notna().any()))
    field_games = set(field_ok[field_ok].index)
    print(f"games with possession field: {len(field_games)}")

    con = duckdb.connect(str(RS_DB), read_only=True)
    rs_st = con.execute("""
        SELECT CAST(game_id AS VARCHAR) game_id, stint_index,
               CAST(home_id AS BIGINT) home_id,
               CAST(away_id AS BIGINT) away_id,
               start_elapsed, end_elapsed FROM lineup_stint_facts""").df()
    con.close()
    po_st = pd.read_csv(PO_STINTS, dtype={"game_id": str},
                        usecols=["game_id", "stint_index", "home_id",
                                 "away_id", "start_elapsed", "end_elapsed"])
    stints = pd.concat([rs_st, po_st], ignore_index=True)
    stints["gid_n"] = stints["game_id"].astype(str).str.lstrip("0")
    print(f"stints: {len(stints)} in {stints['gid_n'].nunique()} games")

    seg_rows: list[tuple[str, float, float, float]] = []
    val_rows: list[tuple[str, float, int, int, float, float]] = []
    n_done = 0
    for gid, g in ev.groupby("gid_n", sort=False):
        use_field = gid in field_games
        segs = walk_game(g, use_field)
        seg_rows.extend((gid, s, t, p) for s, t, p in segs)
        if use_field and (hash(gid) % 20 == 0):
            sm = walk_game(g, False)
            fd = pd.DataFrame(segs, columns=["s", "t", "p"])
            sd = pd.DataFrame(sm, columns=["s", "t", "p"])
            fct = fd.groupby("t").agg(n=("p", "size"), pts=("p", "sum"))
            sct = sd.groupby("t").agg(n=("p", "size"), pts=("p", "sum"))
            for tm in set(fct.index) | set(sct.index):
                val_rows.append((
                    gid, tm,
                    int(fct["n"].get(tm, 0)), int(sct["n"].get(tm, 0)),
                    float(fct["pts"].get(tm, 0.0)),
                    float(sct["pts"].get(tm, 0.0))))
        n_done += 1
        if n_done % 5000 == 0:
            print(f"  {n_done} games...", flush=True)

    segs = pd.DataFrame(seg_rows, columns=["gid_n", "elapsed", "team",
                                           "pts"])
    print(f"segments: {len(segs)} possessions "
          f"({len(segs) / segs['gid_n'].nunique() / 2:.1f}/side/game), "
          f"pts/side/game "
          f"{segs.groupby('gid_n')['pts'].sum().median() / 2:.1f}")

    if val_rows:
        v = pd.DataFrame(val_rows, columns=["gid_n", "team", "n_f", "n_s",
                                            "p_f", "p_s"])
        print(f"\nSTATE-MACHINE VALIDATION vs field "
              f"({v['gid_n'].nunique()} games sampled):")
        print(f"  counts: median |diff|/side "
              f"{(v['n_s'] - v['n_f']).abs().median():.1f}  "
              f"p90 {(v['n_s'] - v['n_f']).abs().quantile(0.9):.1f}")
        print(f"  points: median |diff|/side "
              f"{(v['p_s'] - v['p_f']).abs().median():.1f}  "
              f"p90 {(v['p_s'] - v['p_f']).abs().quantile(0.9):.1f}  "
              f"(mean pts/side {v['p_f'].mean():.1f})")

    n0 = len(segs)
    segs = segs.dropna(subset=["elapsed"])
    if len(segs) < n0:
        print(f"  dropped {n0 - len(segs)} segments with null elapsed")
    segs["elapsed"] = segs["elapsed"].astype(float)
    stints["start_elapsed"] = pd.to_numeric(stints["start_elapsed"],
                                            errors="coerce")
    stints["end_elapsed"] = pd.to_numeric(stints["end_elapsed"],
                                          errors="coerce")
    stints = stints.dropna(subset=["start_elapsed"])

    # merge_asof with by= requires GLOBAL sort on the time key alone
    segs = segs.sort_values("elapsed", kind="stable")
    st = stints.sort_values("start_elapsed", kind="stable")
    merged = pd.merge_asof(
        segs, st[["gid_n", "stint_index", "start_elapsed", "end_elapsed",
                  "home_id", "away_id"]],
        left_on="elapsed", right_on="start_elapsed", by="gid_n",
        direction="backward")
    merged = merged.dropna(subset=["stint_index"])
    in_win = merged["elapsed"] <= merged["end_elapsed"] + 1.0
    print(f"stint attach: {in_win.mean() * 100:.1f}% inside matched "
          f"window (rest kept on nearest stint)")

    merged["side"] = np.where(merged["team"] == merged["home_id"], "h",
                     np.where(merged["team"] == merged["away_id"], "a",
                              "?"))
    bad = int((merged["side"] == "?").sum())
    if bad:
        print(f"  {bad} possessions with unmatched team -> dropped")
        merged = merged[merged["side"] != "?"]

    counts = (merged.groupby(["gid_n", "stint_index", "side"])
              .agg(n=("pts", "size"), pts=("pts", "sum"))
              .unstack(fill_value=0))
    counts.columns = [f"{a}_{'home' if b == 'h' else 'away'}"
                      for a, b in counts.columns]
    counts = counts.rename(columns={"n_home": "n_home", "n_away": "n_away",
                                    "pts_home": "pts_home",
                                    "pts_away": "pts_away"}).reset_index()
    for c in ("n_home", "n_away", "pts_home", "pts_away"):
        if c not in counts.columns:
            counts[c] = 0.0

    gid_map = stints.drop_duplicates("gid_n")[["gid_n", "game_id"]]
    counts = counts.merge(gid_map, on="gid_n", how="left")
    counts["stint_index"] = counts["stint_index"].astype(int)
    out = counts[["game_id", "gid_n", "stint_index",
                  "n_home", "n_away", "pts_home", "pts_away"]]
    out.to_parquet(OUT, index=False)
    print(f"\nwrote {len(out)} stint rows -> {OUT}")

    per_game = merged.groupby("gid_n").agg(n=("pts", "size"),
                                           p=("pts", "sum"))
    print(f"per game: possessions/side median {per_game['n'].median()/2:.1f}"
          f"  points/side median {per_game['p'].median()/2:.1f}")


if __name__ == "__main__":
    main()
