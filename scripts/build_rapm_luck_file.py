"""Export separate FT and mid-range luck for all site generators.

Combines nba-metric-data's ft_stint_adjust + midrange_stint_adjust with the
prepared stints' lineups and aggregates per (game, lineup-pair), keyed by
the SORTED five on each side — index-free, so the site's own stint and
possession files can join on lineups regardless of stint numbering.

Distribution to possessions is exact for the RAPM solve: every possession
of a given side within a lineup-pair shares the same design row, so how
the group's luck is split among them cannot change the normal equations.

Output (committed): data/rapm_luck_adjust.parquet
  game_id, hk (sorted home five, comma-joined), ak (sorted away five),
  ft_luck_home, ft_luck_away, mr_luck_home, mr_luck_away (points)

Keeping the components separate lets production use the validated default
(100% FT, 50% mid-range) while selected public pages can expose the requested
3PT+FT-only view without rebuilding or approximating the underlying evidence.

Run locally after refreshing the two adjustment builders (desktop-only
inputs); CI consumes the committed output.

Usage: python scripts/build_rapm_luck_file.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "metric"))
from build_rapm_target import prepare, HCOLS, ACOLS

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
OUT = ROOT / "data" / "rapm_luck_adjust.parquet"


def main() -> None:
    st = prepare(adjustments=())[["game_id", "stint_index"] + HCOLS + ACOLS]
    for name in ("ft_stint_adjust", "midrange_stint_adjust"):
        adj = pd.read_parquet(METRIC_DATA / f"{name}.parquet")
        st = st.merge(adj, on=["game_id", "stint_index"], how="left")
    st = st.fillna({c: 0.0 for c in ["ft_luck_home", "ft_luck_away",
                                     "mr_luck_home", "mr_luck_away"]})
    H = np.sort(st[HCOLS].to_numpy().astype(np.int64), axis=1)
    A = np.sort(st[ACOLS].to_numpy().astype(np.int64), axis=1)
    st["hk"] = [",".join(map(str, r)) for r in H]
    st["ak"] = [",".join(map(str, r)) for r in A]

    luck_cols = ["ft_luck_home", "ft_luck_away",
                 "mr_luck_home", "mr_luck_away"]
    agg = st.groupby(["game_id", "hk", "ak"], as_index=False)[luck_cols].sum()
    agg = agg[agg[luck_cols].ne(0).any(axis=1)]
    agg.to_parquet(OUT, index=False)
    print(f"wrote {OUT}: {len(agg)} lineup-pair rows, "
          f"total |luck| {agg[luck_cols].abs().to_numpy().sum():,.0f} pts")


if __name__ == "__main__":
    main()
