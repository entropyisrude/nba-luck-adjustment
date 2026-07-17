"""Count TRUE possessions per stint per side, full window 1996-2026.

Replaces the seconds/24 approximation in the RAPM target. Since every
possession within a stint shares the same ten players, per-(stint, side)
possession COUNTS + the existing chained/calibrated/luck-adjusted stint
points are solve-exact equivalent to one-row-per-possession design rows
(sufficient statistics). This script produces the counts.

Method:
  * 2019-20+ games: the PBP `possession` field directly (team holding the
    ball; flips on def rebound / turnover / made score; fixed across
    OREBs). Each maximal run of one value = one possession, assigned to
    the stint covering its START elapsed time.
  * pre-2019 games (field 100% null): an event state machine over a
    schema-NORMALIZED event stream — possession ends on: turnover; made
    FG (unless an and-1 free throw follows); made final free throw of a
    trip (technicals ignored); a rebound by the NON-shooting team after a
    miss (rebound side inferred from rebounding team vs last-miss team —
    old-schema rebounds carry no off/def subtype); period change closes
    the in-flight possession.
  * VALIDATION GATE: the state machine also runs on a ~5% sample of
    field-era games and is compared game-by-game against the field
    counts. If median absolute disagreement exceeds ~2 possessions/side,
    do NOT trust the pre-2019 counts.

Output: nba-metric-data/stint_possession_counts.parquet
        (game_id native format + gid_n, stint_index, n_home, n_away)

Usage: python metric/count_stint_possessions.py
"""
from __future__ import annotations

import re
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

FT_OF = re.compile(r"(\d) of (\d)")


def elapsed_of(clock: pd.Series, period: pd.Series):
    m = clock.str.extract(r"PT(\d+)M([\d.]+)S")
    clock_s = (pd.to_numeric(m[0], errors="coerce") * 60
               + pd.to_numeric(m[1], errors="coerce"))
    per = period.astype(float)
    plen = np.where(per <= 4, 720.0, 300.0)
    prior = np.where(per <= 4, (per - 1) * 720.0,
                     2880.0 + (per - 5) * 300.0)
    return prior + (plen - clock_s)


def load_events() -> pd.DataFrame:
    """All parsing pushed into SQL so pandas only holds compact columns
    (~10M rows of numerics) — the naive load with description strings was
    getting OOM-killed alongside the user's other jobs."""
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
    WHERE clock_s IS NOT NULL
    ORDER BY gid_n, period, elapsed, ord
    """
    ev = con.execute(q).df()
    con.close()
    ev = ev[ev["kind"] != "other"].reset_index(drop=True)
    print(f"events: {len(ev)} across {ev['gid_n'].nunique()} games",
          flush=True)
    return ev


def segments_from_field(g: pd.DataFrame) -> list[tuple[float, float]]:
    """Vectorized runs of the 2019+ possession field."""
    p = pd.to_numeric(g["possession"], errors="coerce").to_numpy()
    e = g["elapsed"].to_numpy()
    ok = np.isfinite(p) & (p > 0)
    p, e = p[ok], e[ok]
    if not len(p):
        return []
    flips = np.flatnonzero(np.diff(p) != 0) + 1
    starts = np.concatenate([[0], flips])
    return [(e[s], p[s]) for s in starts]


def segments_from_events(g: pd.DataFrame) -> list[tuple[float, float]]:
    """Normalized-event state machine -> (start_elapsed, team) segments."""
    kind = g["kind"].to_numpy()
    team = g["team"].to_numpy(dtype=float)
    e = g["elapsed"].to_numpy()
    per = g["period"].to_numpy()
    ft_final = g["ft_final"].to_numpy()
    ft_made = g["ft_made"].to_numpy()
    ft_tech = g["ft_tech"].to_numpy()
    n = len(g)

    segs: list[tuple[float, float]] = []
    offense = np.nan     # team currently in possession (nan = unknown)
    start = e[0] if n else 0.0
    last_miss = np.nan   # team whose miss awaits a rebound

    def close(tm: float, at_e: float, next_off: float = np.nan) -> None:
        nonlocal offense, start, last_miss
        if np.isfinite(tm):
            segs.append((start if np.isfinite(offense) else at_e, tm))
        offense = next_off
        start = at_e
        last_miss = np.nan

    for i in range(n):
        if i > 0 and per[i] != per[i - 1]:
            # period change closes any in-flight possession
            tm = offense if np.isfinite(offense) else last_miss
            if np.isfinite(tm):
                segs.append((start, tm))
            offense, last_miss, start = np.nan, np.nan, e[i]

        k = kind[i]
        t = team[i]
        if not np.isfinite(t):
            continue
        if not np.isfinite(offense) and k in ("make", "miss", "ft", "tov"):
            offense = t

        if k == "tov":
            close(t, e[i])
        elif k == "make":
            # and-1 lookahead: an FT by the same team shortly after, before
            # any other possession-relevant event, means the possession
            # continues through the trip
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
                    # defensive rebound: shooter's possession ends,
                    # rebounder's begins
                    close(last_miss, e[i], next_off=t)
                else:
                    last_miss = np.nan   # OREB: possession continues

    tm = offense if np.isfinite(offense) else last_miss
    if np.isfinite(tm):
        segs.append((start, tm))
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

    seg_rows: list[tuple[str, float, float]] = []
    val_rows: list[tuple[str, float, int, int]] = []
    n_done = 0
    for gid, g in ev.groupby("gid_n", sort=False):
        if gid in field_games:
            segs = segments_from_field(g)
            if hash(gid) % 20 == 0:      # ~5% validation sample
                sm = segments_from_events(g)
                f_ct = pd.Series([t for _, t in segs]).value_counts()
                s_ct = pd.Series([t for _, t in sm]).value_counts()
                for tm in set(f_ct.index) | set(s_ct.index):
                    val_rows.append((gid, tm, int(f_ct.get(tm, 0)),
                                     int(s_ct.get(tm, 0))))
        else:
            segs = segments_from_events(g)
        seg_rows.extend((gid, s, t) for s, t in segs)
        n_done += 1
        if n_done % 5000 == 0:
            print(f"  {n_done} games...", flush=True)

    segs = pd.DataFrame(seg_rows, columns=["gid_n", "elapsed", "team"])
    print(f"segments: {len(segs)} possessions "
          f"({len(segs) / segs['gid_n'].nunique() / 2:.1f}/side/game)")

    if val_rows:
        v = pd.DataFrame(val_rows, columns=["gid_n", "team", "n_field",
                                            "n_sm"])
        v["diff"] = (v["n_sm"] - v["n_field"]).abs()
        print(f"\nSTATE-MACHINE VALIDATION vs possession field "
              f"({v['gid_n'].nunique()} games sampled):")
        print(f"  median |diff|/side: {v['diff'].median():.1f}   "
              f"p90: {v['diff'].quantile(0.9):.1f}   "
              f"mean field count/side: {v['n_field'].mean():.1f}")

    n0 = len(segs)
    segs = segs.dropna(subset=["elapsed"])
    if len(segs) < n0:
        print(f"  dropped {n0 - len(segs)} segments with null elapsed")
    segs["elapsed"] = segs["elapsed"].astype(float)
    stints["start_elapsed"] = pd.to_numeric(stints["start_elapsed"],
                                            errors="coerce")
    stints["end_elapsed"] = pd.to_numeric(stints["end_elapsed"],
                                          errors="coerce")
    n0 = len(stints)
    stints = stints.dropna(subset=["start_elapsed"])
    if len(stints) < n0:
        print(f"  dropped {n0 - len(stints)} stints with null "
              f"start_elapsed")
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
    print(f"stint attach: {in_win.mean() * 100:.1f}% of possessions inside "
          f"the matched stint window (rest kept on nearest stint)")

    merged["side"] = np.where(merged["team"] == merged["home_id"], "h",
                     np.where(merged["team"] == merged["away_id"], "a",
                              "?"))
    bad = int((merged["side"] == "?").sum())
    if bad:
        print(f"  {bad} possessions with team not matching home/away "
              f"-> dropped")
        merged = merged[merged["side"] != "?"]

    counts = (merged.groupby(["gid_n", "stint_index", "side"])
              .size().unstack(fill_value=0).reset_index())
    counts = counts.rename(columns={"h": "n_home", "a": "n_away"})
    for c in ("n_home", "n_away"):
        if c not in counts.columns:
            counts[c] = 0

    gid_map = stints.drop_duplicates("gid_n")[["gid_n", "game_id"]]
    counts = counts.merge(gid_map, on="gid_n", how="left")
    counts["stint_index"] = counts["stint_index"].astype(int)
    out = counts[["game_id", "gid_n", "stint_index", "n_home", "n_away"]]
    out.to_parquet(OUT, index=False)
    print(f"\nwrote {len(out)} stint-count rows -> {OUT}")

    per_game = merged.groupby("gid_n").size() / 2
    print(f"possessions/side/game overall: median {per_game.median():.1f}, "
          f"p10 {per_game.quantile(0.1):.1f}, "
          f"p90 {per_game.quantile(0.9):.1f}")
    yr2 = merged["gid_n"].str[1:3]
    by_era = (merged.groupby(yr2).size()
              / merged.groupby(yr2)["gid_n"].nunique() / 2).round(1)
    print("possessions/side/game by season code:",
          dict(by_era))


if __name__ == "__main__":
    main()
