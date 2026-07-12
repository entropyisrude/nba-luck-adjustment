"""Apply FT + mid-range shooting luck to RAPM inputs (site generators).

Reads data/rapm_luck_adjust.parquet (built locally by
scripts/build_rapm_luck_file.py; committed so CI can use it) and subtracts
the luck from adjusted points. Matching is by (game_id, sorted lineup
five per side) — index-free. Missing file degrades gracefully (3PT-only
adjustment, with a console note), so CI never breaks.

For possessions the group's luck is spread equally across the group's
rows; this is exact for the solve because every possession of a side
within a lineup-pair has an identical design row.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

LUCK_PATH = Path(__file__).resolve().parent / "data" / "rapm_luck_adjust.parquet"

# Fraction of the luck deviation removed — matches the metric pipeline's
# validated LUCK_LAMBDA (see metric/test_lambda_grid.py: full removal
# over-corrects; boards peak at 0.75).
LUCK_LAMBDA = 0.75

_HCOLS = [f"home_p{i}" for i in range(1, 6)]
_ACOLS = [f"away_p{i}" for i in range(1, 6)]
_OCOLS = [f"off_p{i}" for i in range(1, 6)]
_DCOLS = [f"def_p{i}" for i in range(1, 6)]


def _load():
    if not LUCK_PATH.exists():
        print("NOTE: data/rapm_luck_adjust.parquet missing - "
              "FT/mid-range luck NOT removed (3PT-only adjustment)")
        return None
    return pd.read_parquet(LUCK_PATH)


def _key(frame: pd.DataFrame, cols) -> pd.Series:
    arr = np.sort(frame[cols].fillna(-1).to_numpy().astype(np.int64), axis=1)
    return pd.Series([",".join(map(str, r)) for r in arr], index=frame.index)


def apply_to_stints(stints: pd.DataFrame) -> pd.DataFrame:
    """Subtract lineup-pair luck from home_pts_adj/away_pts_adj. Stints
    sharing a lineup-pair within a game split the luck by seconds."""
    luck = _load()
    if luck is None or not {"home_pts_adj", "away_pts_adj"}.issubset(stints.columns):
        return stints
    st = stints.copy()
    st["_hk"] = _key(st, _HCOLS)
    st["_ak"] = _key(st, _ACOLS)
    st = st.merge(luck.rename(columns={"hk": "_hk", "ak": "_ak"}),
                  on=["game_id", "_hk", "_ak"], how="left")
    st[["luck_home", "luck_away"]] = st[["luck_home", "luck_away"]].fillna(0.0)
    grp = st.groupby(["game_id", "_hk", "_ak"])["seconds"]
    share = st["seconds"] / grp.transform("sum").clip(lower=1e-9)
    st["home_pts_adj"] = st["home_pts_adj"] - LUCK_LAMBDA * st["luck_home"] * share
    st["away_pts_adj"] = st["away_pts_adj"] - LUCK_LAMBDA * st["luck_away"] * share
    n = int((st["luck_home"].ne(0) | st["luck_away"].ne(0)).sum())
    print(f"rapm_luck: FT+mid-range luck removed on {n} stints")
    return st.drop(columns=["_hk", "_ak", "luck_home", "luck_away"])


def apply_to_possessions(poss: pd.DataFrame) -> pd.DataFrame:
    """Subtract lineup-pair luck from points_adj, spread equally across the
    group's possessions (solve-exact: identical design rows)."""
    luck = _load()
    if luck is None or "points_adj" not in poss.columns:
        return poss
    po = poss.copy()
    po["_ok"] = _key(po, _OCOLS)
    po["_dk"] = _key(po, _DCOLS)
    # two orientations: offense = stint's home five, or the away five
    l_home = luck.rename(columns={"hk": "_ok", "ak": "_dk",
                                  "luck_home": "_luck"})[
        ["game_id", "_ok", "_dk", "_luck"]]
    l_away = luck.rename(columns={"ak": "_ok", "hk": "_dk",
                                  "luck_away": "_luck"})[
        ["game_id", "_ok", "_dk", "_luck"]]
    lk = pd.concat([l_home, l_away], ignore_index=True)
    lk = lk.groupby(["game_id", "_ok", "_dk"], as_index=False)["_luck"].sum()
    po = po.merge(lk, on=["game_id", "_ok", "_dk"], how="left")
    po["_luck"] = po["_luck"].fillna(0.0)
    size = po.groupby(["game_id", "_ok", "_dk"])["_luck"].transform("size")
    po["points_adj"] = po["points_adj"] - LUCK_LAMBDA * po["_luck"] / size
    n = int(po["_luck"].ne(0).sum())
    print(f"rapm_luck: FT+mid-range luck removed across {n} possessions")
    return po.drop(columns=["_ok", "_dk", "_luck"])
