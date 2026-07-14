"""
Split scoring into first-chance vs. second-chance points per stint, for the
full history (1996-2025) -- the permanent version of the diagnostic built
in test_firstchance_rapm.py (2026-07-14), which showed dreb_75's entire
correlation with defensive RAPM runs through preventing second-chance
points (r=0.321 on the full target vs r=0.018 once second-chance points
are stripped out), not general defensive quality.

This is NOT a luck adjustment (nothing here is "unlucky" and being muted) --
it's a genuine point-attribution split used to build two separate RAPM
targets (first-chance-only, second-chance-only) so the box prior can be fit
against each separately, letting dreb_75 claim credit only for the (real,
but narrow, ~15% of total scoring) component it actually explains.

COVERAGE (checked, not assumed): the `possession` field is 100% null before
the 2019-20 season -- another instance of the PBP schema break already
documented elsewhere in this project (mid-range shot-type keywords, dunk
tagging). Old-schema rebounds also use a different actionType casing
("Rebound" vs "rebound") and their subType is "Unknown"/"Normal Rebound",
not offensive/defensive, so there's no reliable non-text way to classify
them, and reconstructing possession via a hand-built state machine off text
alone is a much riskier, more error-prone build than this project should
take on for the marginal seasons it would add. So this ONLY processes
2019-20+ games; older stints get NO row in the output (not a zero -- an
absence, so downstream consumers must exclude them, not assume zero
second-chance points). Given the RAPM target's own decay (550-day
halflife) and 6-year hard window already concentrate the overwhelming
majority of weight on the last 2-4 years for any current target season,
this costs almost nothing for what this fix is actually for (2023-24
through 2025-26 published ratings).

Mechanics: uses the PBP `possession` field (team ID currently holding the
ball -- flips on a defensive rebound/turnover/made basket, stays fixed
across an offensive rebound; confirmed by direct inspection of a full game)
and `subType` (rebounds are tagged 'offensive'/'defensive' directly, no
text parsing needed) to segment every possession, then tags each scoring
event as first-chance (before any ORB in that possession) or second-chance
(after). Per-event point values come from the scoreHome/scoreAway delta to
the previous row.

Output: nba-metric-data/secondchance_stint_adjust.parquet
        (game_id, stint_index, sc_pts_home, sc_pts_away)
        -- SECOND-CHANCE points only; first-chance = total - this.

Usage: python metric/build_secondchance_adjust.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_rapm_target import prepare, HCOLS, ACOLS

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PBP = METRIC_DATA / "PlayByPlay.parquet"
OUT = METRIC_DATA / "secondchance_stint_adjust.parquet"


def load_second_chance_points() -> pd.DataFrame:
    cols = ["gameId", "actionNumber", "period", "clock", "actionType", "subType",
            "possession", "scoreHome", "scoreAway", "gameDateTimeEst"]
    d = pd.read_parquet(PBP, columns=cols)
    d = d[d["gameId"].astype(str).str.startswith("2")]   # regular season only
    d["date"] = pd.to_datetime(d["gameDateTimeEst"], errors="coerce")
    d = d.dropna(subset=["date", "actionNumber"])
    # possession field (and reliable rebound offensive/defensive tagging) only
    # exists in the post-2019-20 PBP schema -- see module docstring
    d = d[d["possession"].notna()]
    d = d.sort_values(["gameId", "actionNumber"])
    print(f"{len(d):,} regular-season PBP rows (2019-20+ schema only), {d.gameId.nunique():,} games")

    d["poss_change"] = d.groupby("gameId")["possession"].transform(lambda s: (s != s.shift()).cumsum())
    d["is_orb"] = (d["actionType"].str.lower() == "rebound") & (d["subType"].str.lower() == "offensive")
    d["orb_before"] = d.groupby(["gameId", "poss_change"])["is_orb"].cumsum().shift(1).fillna(0) > 0
    same_group = (d["poss_change"] == d.groupby("gameId")["poss_change"].shift(1))
    d["orb_before"] = d["orb_before"] & same_group.fillna(False)

    d["home_delta"] = d.groupby("gameId")["scoreHome"].diff().fillna(0)
    d["away_delta"] = d.groupby("gameId")["scoreAway"].diff().fillna(0)
    d["sc_home"] = np.where(d["orb_before"], d["home_delta"], 0.0)
    d["sc_away"] = np.where(d["orb_before"], d["away_delta"], 0.0)

    m = d["clock"].str.extract(r"PT(\d+)M([\d.]+)S")
    clock_s = pd.to_numeric(m[0], errors="coerce") * 60 + pd.to_numeric(m[1], errors="coerce")
    per = d["period"].astype(float)
    plen = np.where(per <= 4, 720.0, 300.0)
    prior = np.where(per <= 4, (per - 1) * 720.0, 2880.0 + (per - 5) * 300.0)
    d["elapsed"] = prior + (plen - clock_s)
    d["game_id"] = d["gameId"].astype(str)
    d = d.dropna(subset=["elapsed"])

    tot_sc = (d["sc_home"] + d["sc_away"]).sum()
    tot_pts = (d["home_delta"] + d["away_delta"]).sum()
    print(f"second-chance points: {tot_sc:,.0f} of {tot_pts:,.0f} total ({tot_sc/tot_pts:.1%})")
    return d[["game_id", "elapsed", "sc_home", "sc_away"]]


def main() -> None:
    st = prepare(adjustments=())   # attachment geometry only
    sc = load_second_chance_points()

    stc = st[["game_id", "stint_index", "start_elapsed"]].copy()
    stc = stc.sort_values(["game_id", "start_elapsed"]).reset_index(drop=True)
    sc = sc[sc["game_id"].isin(set(stc["game_id"]))]
    j = pd.merge_asof(sc.sort_values("elapsed").reset_index(drop=True),
                      stc.sort_values("start_elapsed"),
                      left_on="elapsed", right_on="start_elapsed",
                      by="game_id", direction="backward")
    j = j.dropna(subset=["stint_index"])

    agg = j.groupby(["game_id", "stint_index"])[["sc_home", "sc_away"]].sum().reset_index()
    agg = agg.rename(columns={"sc_home": "sc_pts_home", "sc_away": "sc_pts_away"})
    agg["stint_index"] = agg["stint_index"].astype(int)
    agg.to_parquet(OUT, index=False)
    print(f"wrote {OUT}: {len(agg):,} stint rows; "
          f"mean sc_pts/stint (home) {agg.sc_pts_home.mean():.3f}, (away) {agg.sc_pts_away.mean():.3f}")


if __name__ == "__main__":
    main()
