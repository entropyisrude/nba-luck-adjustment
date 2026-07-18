"""Atomic box-feature cache for the box-prior v3 rebuild (locked spec,
2026-07-16): one distinct basketball fact per feature, no containment.

35 atoms, player-season (REGULAR SEASON stats only, matching the v1 prior):

  scoring (14): {assisted M, unassisted M, missed A} x {rim 0-4ft,
    floater 4-10ft, mid >10ft 2P, three} per 75 + ftm_75, ft_miss_75.
    Heaves excluded from the three-point atoms entirely (not makes, not
    misses): shotValue==3 AND (distance>40 OR (distance>=30 AND <=3s left
    in period)). 2025+ 'heave' actionType rows are excluded by type.
  playmaking (6): assists BY DESTINATION (to rim makes / to other-2P makes
    / to 3PM) per 75; turnovers 3-way bad-pass / lost-ball / dead-ball.
    Offensive fouls are counted ONCE, inside dead-ball turnovers.
  rebounding (4): oreb_opp_rate = OREB / own-team misses while on floor,
    dreb_opp_rate = DREB / opponent misses while on floor (misses = FG
    misses + final-of-set FT misses, attached to stints by elapsed time,
    same machinery as build_ft_adjust.py); off/def box-outs per 75 (2016+).
  defense (6): stl_75, blk_75, rim_dfg_diff (rim dFG% vs expected,
    rim_dfga-weighted, 2013+), contested_2pt_75, contested_3pt_75,
    defl_75 (2016+).
  fouls (2): pf_nonoff_75 (personal fouls MINUS offensive fouls — those
    live in dead-ball TOs), charges_drawn_75 (2016+ hustle; the PBP
    foulDrawnPersonId path only exists 2019+, and old-schema foul rows
    never name the fouled player, so full-window charges are NOT
    recoverable — spec deviation, documented).
  bio (3): height, age, wing_rel (2001+ combine).

PBP schema-era handling (verified against the parquet, not assumed):
  * old schema ('Made Shot'/'Missed Shot', 1996-2020): assist status =
    description suffix '(Name n AST)' (present on 59% of makes, matching
    league assisted rates; assistPersonId is 100% null here). The assister
    IDENTITY is parsed from that suffix and mapped to a player id within
    (game, shooter's team) by last-name match; ambiguous or unmatched
    names are dropped and counted. This is NOT the biased description
    channel — the known pre-2019 bias affects shot-TYPE keywords
    (pullup/fadeaway), not the assist suffix, and assists exist only on
    makes by definition so there is no make/miss asymmetry to bias.
  * new schema ('2pt'/'3pt', 2019+): assistPersonId directly.
  * three-point identification: shotValue==3 (agrees with description
    '3PT' to within one event over 4.9M old-schema shots).
  * turnover subtype mapping keeps the two eras consistent: bad pass =
    {Bad Pass, Stolen Pass}, lost ball = {Lost Ball, Poss Lost Ball},
    dead = everything else INCLUDING all out-of-bounds variants (the new
    schema can't split OOB into pass/dribble, so the old schema's OOB
    subtypes are classified dead too).

Outputs:
  nba-metric-data/features_atomic_season.parquet (preserved raw-rate artifact)
  nba-metric-data/features_atomic_denominator_season.parquet
Usage:  set PYTHONIOENCODING=utf-8 & python metric/build_atomic_features.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_rapm_target import prepare, HCOLS, ACOLS

ROOT = Path(__file__).resolve().parents[1]
RS_DB = ROOT / "data" / "nba_analytics.duckdb"
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PBP = METRIC_DATA / "PlayByPlay.parquet"
BASE_CACHE = METRIC_DATA / "features_box_season.parquet"
OUT = METRIC_DATA / "features_atomic_season.parquet"
OUT_DENOM = METRIC_DATA / "features_atomic_denominator_season.parquet"

BAD_PASS = {"Bad Pass", "Stolen Pass Turnover",
            "bad pass", "badpass"}
LOST_BALL = {"Lost Ball", "Poss Lost Ball Turnover",
             "lost ball", "lostball"}

ATOMS = ["rim_ast_m_75", "rim_unast_m_75", "rim_miss_75",
         "flt_ast_m_75", "flt_unast_m_75", "flt_miss_75",
         "mid_ast_m_75", "mid_unast_m_75", "mid_miss_75",
         "tp_ast_m_75", "tp_unast_m_75", "tp_miss_75",
         "ftm_75", "ft_miss_75",
         "ast_rim_75", "ast_mid2_75", "ast_tp_75",
         "tov_bp_75", "tov_lb_75", "tov_dead_75",
         "oreb_opp_rate", "dreb_opp_rate",
         "off_boxout_75", "def_boxout_75",
         "stl_75", "blk_75", "rim_dfg_diff",
         "contested_2pt_75", "contested_3pt_75", "defl_75",
         "pf_nonoff_75", "charges_drawn_75",
         "height", "age", "wing_rel"]


def elapsed_from(clock: pd.Series, period: pd.Series) -> pd.Series:
    m = clock.str.extract(r"PT(\d+)M([\d.]+)S")
    clock_s = (pd.to_numeric(m[0], errors="coerce") * 60
               + pd.to_numeric(m[1], errors="coerce"))
    per = period.astype(float)
    plen = np.where(per <= 4, 720.0, 300.0)
    prior = np.where(per <= 4, (per - 1) * 720.0, 2880.0 + (per - 5) * 300.0)
    return prior + (plen - clock_s), clock_s


def season_of(dt: pd.Series) -> pd.Series:
    d = pd.to_datetime(dt, errors="coerce")
    return (d.dt.year - (d.dt.month < 10)).astype("Int64")


def rs_mask(gid: pd.Series) -> pd.Series:
    return gid.astype(str).str.lstrip("0").str.startswith("2")


# --------------------------------------------------------------------------
# shots: zone x assist-status atoms + shot-level frame for reuse
# --------------------------------------------------------------------------

def load_shots() -> pd.DataFrame:
    con = duckdb.connect()
    q = f"""
    SELECT gameId, personId, shotValue, shotDistance, clock, period,
           trim(actionType) AS at, shotResult, description,
           assistPersonId, gameDateTimeEst
    FROM read_parquet('{PBP.as_posix()}')
    WHERE trim(actionType) IN ('Made Shot','Missed Shot','2pt','3pt')
    """
    sh = con.execute(q).df()
    con.close()
    sh = sh[rs_mask(sh["gameId"])].copy()
    sh["pid"] = pd.to_numeric(sh["personId"], errors="coerce")
    sh = sh.dropna(subset=["pid"])
    sh["pid"] = sh["pid"].astype(int)
    sh["season_year"] = season_of(sh["gameDateTimeEst"])
    sh = sh.dropna(subset=["season_year"])
    sh["new_schema"] = sh["at"].isin(["2pt", "3pt"]).to_numpy(bool)
    sh["made"] = np.where(sh["new_schema"],
                          (sh["shotResult"].astype(str).str.lower()
                           == "made").to_numpy(bool),
                          (sh["at"] == "Made Shot").to_numpy(bool))
    sv = pd.to_numeric(sh["shotValue"], errors="coerce").astype("float64")
    # shotValue is populated ONLY in the old schema (verified: 0 non-null
    # among 1.6M new-schema rows) — the new schema's actionType string IS
    # the 3-point identifier. Using shotValue alone silently classified
    # every 2019+ three as a midrange 2P attempt (all-zero 3P atoms).
    sh["is3"] = ((sv == 3.0) | (sh["at"] == "3pt")).to_numpy(bool)
    sh["dist"] = pd.to_numeric(sh["shotDistance"],
                               errors="coerce").astype("float64")
    n_nodist = int((sh["dist"].isna() & ~sh["is3"]).sum())
    if n_nodist:
        print(f"  dropping {n_nodist} 2P rows with null distance")
    sh = sh[sh["is3"] | sh["dist"].notna()].copy()
    sh["elapsed"], clock_s = elapsed_from(sh["clock"], sh["period"])
    heave = sh["is3"] & ((sh["dist"] > 40)
                         | ((sh["dist"] >= 30) & (clock_s <= 3)))
    print(f"shots: {len(sh)} RS events, heaves excluded: {int(heave.sum())}")
    sh = sh[~heave].copy()
    sh["assisted"] = np.where(
        sh["new_schema"], sh["assistPersonId"].notna(),
        sh["description"].str.contains(r" AST\)", regex=True, na=False))
    sh["zone"] = np.where(sh["is3"], "tp",
                 np.where(sh["dist"] <= 4, "rim",
                 np.where(sh["dist"] <= 10, "flt", "mid")))
    sh["game_id"] = sh["gameId"].astype(str).str.lstrip("0")
    return sh


def shot_atoms(sh: pd.DataFrame) -> pd.DataFrame:
    made = sh[sh["made"]]
    g = (made.groupby(["pid", "season_year", "zone", "assisted"])
         .size().rename("n").reset_index())
    g["col"] = (g["zone"] + "_" + np.where(g["assisted"], "ast", "unast")
                + "_m")
    piv = g.pivot_table(index=["pid", "season_year"], columns="col",
                        values="n", aggfunc="sum", fill_value=0)
    miss = (sh[~sh["made"]].groupby(["pid", "season_year", "zone"])
            .size().rename("n").reset_index())
    miss["col"] = miss["zone"] + "_miss"
    piv2 = miss.pivot_table(index=["pid", "season_year"], columns="col",
                            values="n", aggfunc="sum", fill_value=0)
    out = piv.join(piv2, how="outer").fillna(0).reset_index()
    return out


# --------------------------------------------------------------------------
# assist destinations (assister-side)
# --------------------------------------------------------------------------

def load_roster_map() -> pd.DataFrame:
    con = duckdb.connect(str(RS_DB), read_only=True)
    r = con.execute("""
        SELECT DISTINCT CAST(game_id AS VARCHAR) game_id,
               CAST(player_id AS BIGINT) pid, player_name,
               CAST(team_id AS BIGINT) team_id
        FROM player_game_facts WHERE CAST(game_id AS VARCHAR) LIKE '2%'
    """).df()
    con.close()
    r["lname"] = (r["player_name"].str.lower().str.replace(".", "", regex=False)
                  .str.split().str[-1])
    return r


def assist_destinations(sh: pd.DataFrame, roster: pd.DataFrame) -> tuple:
    dest = {"rim": "ast_rim", "flt": "ast_mid2", "mid": "ast_mid2",
            "tp": "ast_tp"}
    made = sh[sh["made"] & sh["assisted"]].copy()
    made["dcol"] = made["zone"].map(dest)

    # new schema: direct
    new = made[made["new_schema"]].copy()
    new["assister"] = pd.to_numeric(new["assistPersonId"],
                                    errors="coerce").astype("Int64")
    new_ok = new.dropna(subset=["assister"])

    # old schema: parse "(Name n AST)", map by last name within the
    # shooter's team roster for that game
    old = made[~made["new_schema"]].copy()
    nm = old["description"].str.extract(r"\(([^()]+?) (\d+) AST\)")
    old["aname"] = nm[0].str.lower().str.replace(".", "", regex=False).str.strip()
    old = old.dropna(subset=["aname"])
    shooter_team = roster[["game_id", "pid", "team_id"]].rename(
        columns={"pid": "pid_sh"})
    old = old.merge(shooter_team, left_on=["game_id", "pid"],
                    right_on=["game_id", "pid_sh"], how="left")
    cand = roster.rename(columns={"pid": "assister"})
    old = old.merge(cand[["game_id", "team_id", "lname", "assister"]],
                    left_on=["game_id", "team_id", "aname"],
                    right_on=["game_id", "team_id", "lname"], how="left")
    # multi-token last names ("van exel"): retry unmatched on last two tokens
    un = old["assister"].isna()
    if un.any():
        cand2 = cand.copy()
        cand2["lname2"] = (cand2["player_name"].str.lower()
                           .str.replace(".", "", regex=False)
                           .str.split().str[-2:].str.join(" "))
        fix = (old.loc[un, ["game_id", "team_id", "aname"]]
               .reset_index()
               .merge(cand2[["game_id", "team_id", "lname2", "assister"]],
                      left_on=["game_id", "team_id", "aname"],
                      right_on=["game_id", "team_id", "lname2"], how="left")
               .set_index("index"))
        old.loc[un, "assister"] = fix["assister"]
    # ambiguity: same (game, team, lname) mapping to 2+ pids duplicates rows
    key = ["game_id", "pid", "elapsed", "dcol"]
    dup = old.duplicated(subset=key, keep=False)
    n_amb = int(dup.sum())
    old = old[~dup]
    matched = old["assister"].notna()
    print(f"assist parse (old schema): {matched.mean():.1%} matched, "
          f"{n_amb} ambiguous-name rows dropped, "
          f"{int((~matched).sum())} unmatched")
    old_ok = old[matched]

    both = pd.concat([
        new_ok[["assister", "season_year", "dcol"]],
        old_ok[["assister", "season_year", "dcol"]],
    ], ignore_index=True)
    both["assister"] = both["assister"].astype(int)
    g = (both.groupby(["assister", "season_year", "dcol"]).size()
         .rename("n").reset_index()
         .pivot_table(index=["assister", "season_year"], columns="dcol",
                      values="n", aggfunc="sum", fill_value=0)
         .reset_index().rename(columns={"assister": "pid"}))
    return g


# --------------------------------------------------------------------------
# turnovers + offensive fouls
# --------------------------------------------------------------------------

def load_turnovers() -> pd.DataFrame:
    con = duckdb.connect()
    q = f"""
    SELECT gameId, personId, subType, clock, period, gameDateTimeEst
    FROM read_parquet('{PBP.as_posix()}')
    WHERE lower(trim(actionType)) = 'turnover'
    """
    t = con.execute(q).df()
    con.close()
    t = t[rs_mask(t["gameId"])].copy()
    t["pid"] = pd.to_numeric(t["personId"], errors="coerce")
    t = t.dropna(subset=["pid"])
    t["pid"] = t["pid"].astype(int)
    t = t[t["pid"] > 0]           # team turnovers carry pid 0
    t["season_year"] = season_of(t["gameDateTimeEst"])
    # dedupe safety: one turnover = one (game, player, period, clock)
    n0 = len(t)
    t = t.drop_duplicates(subset=["gameId", "pid", "period", "clock",
                                  "subType"])
    st = t["subType"].fillna("").str.strip()
    t["cls"] = np.where(st.isin(BAD_PASS) | st.str.lower().str.startswith("bad pass"),
                        "tov_bp",
               np.where(st.isin(LOST_BALL) | st.str.lower().str.startswith("lost ball"),
                        "tov_lb", "tov_dead"))
    print(f"turnovers: {len(t)} RS events ({n0 - len(t)} deduped), "
          f"split: {t['cls'].value_counts().to_dict()}")
    g = (t.groupby(["pid", "season_year", "cls"]).size().rename("n")
         .reset_index()
         .pivot_table(index=["pid", "season_year"], columns="cls",
                      values="n", aggfunc="sum", fill_value=0)
         .reset_index())
    return g


def load_off_fouls() -> pd.DataFrame:
    con = duckdb.connect()
    q = f"""
    SELECT gameId, personId, subType, descriptor, gameDateTimeEst
    FROM read_parquet('{PBP.as_posix()}')
    WHERE lower(trim(actionType)) = 'foul'
    """
    f = con.execute(q).df()
    con.close()
    f = f[rs_mask(f["gameId"])].copy()
    st = f["subType"].fillna("").str.strip().str.lower()
    off = f[st.isin(["offensive", "offensive charge"])].copy()
    off["pid"] = pd.to_numeric(off["personId"], errors="coerce")
    off = off.dropna(subset=["pid"])
    off["pid"] = off["pid"].astype(int)
    off["season_year"] = season_of(off["gameDateTimeEst"])
    g = (off.groupby(["pid", "season_year"]).size()
         .rename("off_fouls").reset_index())
    print(f"offensive fouls committed: {g['off_fouls'].sum():.0f} events")
    return g


# --------------------------------------------------------------------------
# rebounding opportunity denominators (stint attach)
# --------------------------------------------------------------------------

def load_ft_misses_final() -> pd.DataFrame:
    con = duckdb.connect()
    q = f"""
    SELECT gameId, personId, clock, period, description, gameDateTimeEst
    FROM read_parquet('{PBP.as_posix()}')
    WHERE lower(trim(actionType)) IN ('free throw','freethrow')
    """
    ft = con.execute(q).df()
    con.close()
    ft = ft[rs_mask(ft["gameId"])].copy()
    miss = ft["description"].str.contains("MISS", case=False, na=False)
    pos = ft["description"].str.extract(r"(\d) of (\d)")
    final = (pos[0] == pos[1]) & ~ft["description"].str.contains(
        "Technical", case=False, na=False)
    ft = ft[miss & final].copy()
    ft["pid"] = pd.to_numeric(ft["personId"], errors="coerce")
    ft = ft.dropna(subset=["pid"])
    ft["pid"] = ft["pid"].astype(int)
    ft["elapsed"], _ = elapsed_from(ft["clock"], ft["period"])
    ft["season_year"] = season_of(ft["gameDateTimeEst"])
    ft["game_id"] = ft["gameId"].astype(str).str.lstrip("0")
    print(f"final-of-set FT misses: {len(ft)}")
    return ft[["game_id", "pid", "elapsed", "season_year"]]


def opportunity_rates(sh: pd.DataFrame) -> pd.DataFrame:
    """Attach every rebound-able miss to its stint; every on-floor player
    on the shooting side gets an OREB opportunity, the other five a DREB
    opportunity. Rates use per-game box OREB/DREB restricted to the games
    actually covered by the attach (coverage holes drop out of BOTH sides)."""
    misses = sh.loc[~sh["made"], ["game_id", "pid", "elapsed",
                                  "season_year"]]
    misses = pd.concat([misses, load_ft_misses_final()], ignore_index=True)
    misses = misses.dropna(subset=["elapsed"])

    st = prepare(adjustments=())
    stc = st[["game_id", "stint_index", "start_elapsed"] + HCOLS + ACOLS].copy()
    stc = stc.dropna(subset=HCOLS + ACOLS)
    stc = stc.sort_values(["game_id", "start_elapsed"]).reset_index(drop=True)
    misses = misses[misses["game_id"].isin(set(stc["game_id"]))]
    j = pd.merge_asof(misses.sort_values("elapsed").reset_index(drop=True),
                      stc.sort_values("start_elapsed"),
                      left_on="elapsed", right_on="start_elapsed",
                      by="game_id", direction="backward")
    H = j[HCOLS].to_numpy()
    A = j[ACOLS].to_numpy()
    pid = j["pid"].to_numpy()[:, None]
    in_h = (H == pid).any(axis=1)
    in_a = (A == pid).any(axis=1)
    ok = in_h | in_a
    print(f"miss->stint attach: {ok.mean():.1%} of {len(j)} misses matched")
    j = j[ok].copy()
    H, A, in_h = H[ok], A[ok], in_h[ok]

    frames = []
    for k in range(5):
        frames.append(pd.DataFrame({
            "pid": np.where(in_h, H[:, k], A[:, k]),
            "game_id": j["game_id"].to_numpy(),
            "season_year": j["season_year"].to_numpy(),
            "own": 1}))
        frames.append(pd.DataFrame({
            "pid": np.where(in_h, A[:, k], H[:, k]),
            "game_id": j["game_id"].to_numpy(),
            "season_year": j["season_year"].to_numpy(),
            "own": 0}))
    opp = pd.concat(frames, ignore_index=True)
    opp["pid"] = opp["pid"].astype(int)
    per_game = (opp.groupby(["pid", "game_id", "season_year", "own"])
                .size().rename("n").reset_index()
                .pivot_table(index=["pid", "game_id", "season_year"],
                             columns="own", values="n", aggfunc="sum",
                             fill_value=0)
                .rename(columns={1: "own_opps", 0: "opp_opps"})
                .reset_index())

    con = duckdb.connect(str(RS_DB), read_only=True)
    reb = con.execute("""
        SELECT CAST(game_id AS VARCHAR) game_id,
               CAST(player_id AS BIGINT) pid, oreb, dreb
        FROM player_game_facts WHERE CAST(game_id AS VARCHAR) LIKE '2%'
    """).df()
    con.close()
    m = per_game.merge(reb, on=["pid", "game_id"], how="inner")
    agg = m.groupby(["pid", "season_year"]).agg(
        oreb_n=("oreb", "sum"), oreb_d=("own_opps", "sum"),
        dreb_n=("dreb", "sum"), dreb_d=("opp_opps", "sum")).reset_index()
    agg["oreb_opp_rate"] = agg["oreb_n"] / agg["oreb_d"].clip(lower=1)
    agg["dreb_opp_rate"] = agg["dreb_n"] / agg["dreb_d"].clip(lower=1)
    return agg[["pid", "season_year", "oreb_opp_rate", "dreb_opp_rate",
                "oreb_d", "dreb_d"]].rename(columns={
                    "oreb_d": "oreb_opp_rate__denom",
                    "dreb_d": "dreb_opp_rate__denom"})


# --------------------------------------------------------------------------
# duckdb era-limited pulls
# --------------------------------------------------------------------------

def duckdb_extras() -> pd.DataFrame:
    """Coverage-aware per-75 rates for hustle-era columns + rim dFG rate.
    rim_dfg_pct_diff chosen over rim_dfg_plusminus: it is already the
    contest-quality rate (dFG% vs expected); plusminus mixes in volume."""
    cols = ["contested_shots_2pt", "contested_shots_3pt", "deflections",
            "off_boxouts", "def_boxouts", "charges_drawn"]
    sums = ", ".join(
        f"sum({c}) {c}, sum(CASE WHEN {c} IS NOT NULL THEN minutes END) mins_{c}"
        for c in cols)
    con = duckdb.connect(str(RS_DB), read_only=True)
    d = con.execute(f"""
        SELECT CAST(player_id AS BIGINT) pid,
               CAST(substr(season,1,4) AS INTEGER) season_year, {sums},
               max(CASE WHEN rim_dfg_pct_diff IS NOT NULL
                        THEN rim_dfg_pct_diff END) rim_dfg_diff_season,
               max(CASE WHEN rim_dfg_pct_diff IS NOT NULL
                        THEN rim_dfga END) rim_w
        FROM player_game_facts
        WHERE CAST(game_id AS VARCHAR) LIKE '2%'
        GROUP BY 1, 2
    """).df()
    con.close()
    ren = {"contested_shots_2pt": "contested_2pt_75",
           "contested_shots_3pt": "contested_3pt_75",
           "deflections": "defl_75",
           "off_boxouts": "off_boxout_75",
           "def_boxouts": "def_boxout_75",
           "charges_drawn": "charges_drawn_75"}
    for c, out in ren.items():
        cov = d[f"mins_{c}"] * 2.08
        d[out] = np.where(cov > 0, d[c] / cov.clip(lower=1) * 75.0, np.nan)
        d[out + "__denom"] = cov
    # These two rim fields are season aggregates repeated on every game row in
    # player_game_facts.  MAX recovers the one true season value; summing would
    # leave the rate unchanged but falsely multiply its reliability by games.
    d["rim_dfg_diff"] = d["rim_dfg_diff_season"]
    d["rim_dfg_diff__denom"] = d["rim_w"]
    rate_cols = list(ren.values()) + ["rim_dfg_diff"]
    denom_cols = [c + "__denom" for c in rate_cols]
    return d[["pid", "season_year"] + rate_cols + denom_cols]


# --------------------------------------------------------------------------
# assemble
# --------------------------------------------------------------------------

def main() -> None:
    base = pd.read_parquet(BASE_CACHE)
    base = base[["pid", "season_year", "games", "mins", "poss",
                 "pf", "ftm", "fta", "stl", "blk", "oreb", "dreb", "ast",
                 "tov", "fgm", "fg3m", "age", "height", "wing_rel"]].copy()
    sh = load_shots()
    atoms = shot_atoms(sh)
    roster = load_roster_map()
    dests = assist_destinations(sh, roster)
    tos = load_turnovers()
    offf = load_off_fouls()
    opps = opportunity_rates(sh)
    extras = duckdb_extras()

    df = base.merge(atoms, on=["pid", "season_year"], how="left")
    df = df.merge(dests, on=["pid", "season_year"], how="left")
    df = df.merge(tos, on=["pid", "season_year"], how="left")
    df = df.merge(offf, on=["pid", "season_year"], how="left")
    df = df.merge(opps, on=["pid", "season_year"], how="left")
    df = df.merge(extras, on=["pid", "season_year"], how="left")

    cnt_cols = ["rim_ast_m", "rim_unast_m", "rim_miss", "flt_ast_m",
                "flt_unast_m", "flt_miss", "mid_ast_m", "mid_unast_m",
                "mid_miss", "tp_ast_m", "tp_unast_m", "tp_miss",
                "ast_rim", "ast_mid2", "ast_tp",
                "tov_bp", "tov_lb", "tov_dead", "off_fouls"]
    for c in cnt_cols:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = df[c].fillna(0.0)
    p75 = 75.0 / df["poss"].clip(lower=1)
    for c in ["rim_ast_m", "rim_unast_m", "rim_miss", "flt_ast_m",
              "flt_unast_m", "flt_miss", "mid_ast_m", "mid_unast_m",
              "mid_miss", "tp_ast_m", "tp_unast_m", "tp_miss",
              "ast_rim", "ast_mid2", "ast_tp",
              "tov_bp", "tov_lb", "tov_dead"]:
        df[c + "_75"] = df[c] * p75
    df["ftm_75"] = df["ftm"] * p75
    df["ft_miss_75"] = (df["fta"] - df["ftm"]) * p75
    df["stl_75"] = df["stl"] * p75
    df["blk_75"] = df["blk"] * p75
    df["pf_nonoff_75"] = (df["pf"] - df["off_fouls"]).clip(lower=0) * p75

    # ---- cross-checks -----------------------------------------------------
    con = duckdb.connect(str(RS_DB), read_only=True)
    chk = con.execute("""
        SELECT CAST(player_id AS BIGINT) pid,
               CAST(substr(season,1,4) AS INTEGER) season_year,
               sum(assisted_2pm) a2, sum(unassisted_2pm) u2,
               sum(assisted_3pm) a3, sum(unassisted_3pm) u3
        FROM player_game_facts WHERE CAST(game_id AS VARCHAR) LIKE '2%'
        GROUP BY 1, 2
    """).df()
    con.close()
    cc = df.merge(chk, on=["pid", "season_year"], how="inner")
    cc = cc[cc["poss"] >= 500]
    ours_a2 = cc["rim_ast_m"] + cc["flt_ast_m"] + cc["mid_ast_m"]
    ours_a3 = cc["tp_ast_m"]
    print("\ncross-check vs duckdb assisted splits (poss>=500, "
          f"n={len(cc)}):")
    print(f"  assisted 2PM corr: {np.corrcoef(ours_a2, cc['a2'])[0,1]:.4f}"
          f"   assisted 3PM corr: {np.corrcoef(ours_a3, cc['a3'])[0,1]:.4f}")
    tot_pbp = (df[[c for c in df.columns if c.endswith('_m')
                   and not c.endswith('_75')]].sum(axis=1)
               + 0)  # noqa - readability
    # assist volume check vs box
    ast_pbp = df["ast_rim"] + df["ast_mid2"] + df["ast_tp"]
    ok = df["poss"] >= 500
    print(f"  assist volume (PBP-attributed / box): "
          f"{ast_pbp[ok].sum() / df.loc[ok, 'ast'].sum():.3f}")
    tov_pbp = df["tov_bp"] + df["tov_lb"] + df["tov_dead"]
    print(f"  turnover volume (PBP / box): "
          f"{tov_pbp[ok].sum() / df.loc[ok, 'tov'].sum():.3f}")
    by_era = df[ok].copy()
    by_era["era5"] = (by_era["season_year"] // 5) * 5
    er = by_era.groupby("era5").apply(
        lambda g: (g["ast_rim"] + g["ast_mid2"] + g["ast_tp"]).sum()
        / max(g["ast"].sum(), 1), include_groups=False)
    print("  assist coverage by 5-yr era:",
          {int(k): round(v, 3) for k, v in er.items()})

    keep = ["pid", "season_year", "games", "mins", "poss"] + ATOMS
    df[keep].to_parquet(OUT, index=False)
    print(f"\nwrote {len(df)} player-season rows -> {OUT}")

    # Preserve the raw-rate artifact above and write a versioned companion with
    # the natural exposure for every stochastic atom. Bio measurements are
    # intentionally left without denominators and are never EB-shrunk.
    bio = {"height", "age", "wing_rel"}
    special = {"oreb_opp_rate", "dreb_opp_rate", "off_boxout_75",
               "def_boxout_75", "rim_dfg_diff", "contested_2pt_75",
               "contested_3pt_75", "defl_75", "charges_drawn_75"}
    for atom in ATOMS:
        dc = atom + "__denom"
        if atom not in bio and atom not in special:
            df[dc] = df["poss"]
    denom_cols = [a + "__denom" for a in ATOMS if a not in bio]
    missing = [c for c in denom_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"missing atomic denominators: {missing}")
    df[keep + denom_cols].to_parquet(OUT_DENOM, index=False)
    print(f"wrote denominator-aware rows -> {OUT_DENOM}")


if __name__ == "__main__":
    main()
