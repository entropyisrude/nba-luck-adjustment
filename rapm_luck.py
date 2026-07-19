"""Apply the validated shooting-luck definition to site evidence.

The source score is already fully 3PT-adjusted. Production additionally
removes 100% of FT residual variance and 50% of 10-23-foot mid-range residual
variance. ``include_midrange=False`` supplies the requested 3PT+FT-only view.
Matching uses game plus sorted lineups, so reconstructed stint numbering does
not affect attribution.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

LUCK_PATH = Path(__file__).resolve().parent / "data" / "rapm_luck_adjust.parquet"
FT_LAMBDA = 1.0
MIDRANGE_LAMBDA = 0.5

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


def _component_points(luck: pd.DataFrame, side: str,
                      include_midrange: bool) -> pd.Series:
    """Return points to subtract from an already-3PT-adjusted score."""
    ft_col, mr_col = f"ft_luck_{side}", f"mr_luck_{side}"
    if ft_col in luck.columns:
        ft = luck[ft_col].fillna(0.0)
        mr = luck[mr_col].fillna(0.0) if mr_col in luck.columns else 0.0
        return FT_LAMBDA * ft + (MIDRANGE_LAMBDA * mr if include_midrange else 0.0)
    legacy = f"luck_{side}"
    if legacy in luck.columns:
        print("NOTE: legacy combined RAPM luck payload; rebuild component file")
        return 0.75 * luck[legacy].fillna(0.0)
    return pd.Series(0.0, index=luck.index)


def _key(frame: pd.DataFrame, cols) -> pd.Series:
    arr = np.sort(frame[cols].fillna(-1).to_numpy().astype(np.int64), axis=1)
    return pd.Series([",".join(map(str, r)) for r in arr], index=frame.index)


def apply_to_stints(stints: pd.DataFrame,
                    include_midrange: bool = True) -> pd.DataFrame:
    """Subtract lineup-pair luck from home/away adjusted points."""
    luck = _load()
    if luck is None or not {"home_pts_adj", "away_pts_adj"}.issubset(stints.columns):
        return stints
    luck = luck.copy()
    luck["_luck_home"] = _component_points(luck, "home", include_midrange)
    luck["_luck_away"] = _component_points(luck, "away", include_midrange)
    st = stints.copy()
    st["_hk"] = _key(st, _HCOLS)
    st["_ak"] = _key(st, _ACOLS)
    keep = ["game_id", "hk", "ak", "_luck_home", "_luck_away"]
    st = st.merge(luck[keep].rename(columns={"hk": "_hk", "ak": "_ak"}),
                  on=["game_id", "_hk", "_ak"], how="left")
    st[["_luck_home", "_luck_away"]] = st[["_luck_home", "_luck_away"]].fillna(0.0)
    group = st.groupby(["game_id", "_hk", "_ak"])["seconds"]
    share = st["seconds"] / group.transform("sum").clip(lower=1e-9)
    st["home_pts_adj"] -= st["_luck_home"] * share
    st["away_pts_adj"] -= st["_luck_away"] * share
    n = int((st["_luck_home"].ne(0) | st["_luck_away"].ne(0)).sum())
    label = "FT+50% mid-range" if include_midrange else "FT-only"
    print(f"rapm_luck: {label} luck removed on {n} stints")
    return st.drop(columns=["_hk", "_ak", "_luck_home", "_luck_away"])


def apply_to_possessions(poss: pd.DataFrame,
                         include_midrange: bool = True) -> pd.DataFrame:
    """Subtract lineup-pair luck from possession adjusted points."""
    luck = _load()
    if luck is None or "points_adj" not in poss.columns:
        return poss
    luck = luck.copy()
    luck["_luck_home"] = _component_points(luck, "home", include_midrange)
    luck["_luck_away"] = _component_points(luck, "away", include_midrange)
    po = poss.copy()
    po["_ok"] = _key(po, _OCOLS)
    po["_dk"] = _key(po, _DCOLS)
    l_home = luck.rename(columns={"hk": "_ok", "ak": "_dk",
                                  "_luck_home": "_luck"})[
        ["game_id", "_ok", "_dk", "_luck"]]
    l_away = luck.rename(columns={"ak": "_ok", "hk": "_dk",
                                  "_luck_away": "_luck"})[
        ["game_id", "_ok", "_dk", "_luck"]]
    lk = pd.concat([l_home, l_away], ignore_index=True)
    lk = lk.groupby(["game_id", "_ok", "_dk"], as_index=False)["_luck"].sum()
    po = po.merge(lk, on=["game_id", "_ok", "_dk"], how="left")
    po["_luck"] = po["_luck"].fillna(0.0)
    size = po.groupby(["game_id", "_ok", "_dk"])["_luck"].transform("size")
    po["points_adj"] -= po["_luck"] / size
    n = int(po["_luck"].ne(0).sum())
    label = "FT+50% mid-range" if include_midrange else "FT-only"
    print(f"rapm_luck: {label} luck removed across {n} possessions")
    return po.drop(columns=["_ok", "_dk", "_luck"])
