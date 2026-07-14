"""
Consolidate current-season O/D splits across NERD, DARKO, EPM, and BPM into
one comparison table, keyed by normalized player name -- the "which metrics
systematically disagree, and where" project (LEBRON/LAKER deprioritized;
LEBRON's free tool is a third-party embed we haven't resolved, LAKER is
paywalled -- see project memory).

Sources (all current-season, not projections):
  NERD  -- metric/metric_v0.parquet, most recent season_year, m4000_o/m4000_d
  DARKO -- nba-metric-data/benchmarks/darko_snapshots/, most recent snapshot
  EPM   -- nba-metric-data/benchmarks/epm_snapshots/, most recent snapshot
  BPM   -- nba-metric-data/benchmarks/bbref_advanced/advanced_{year}.csv,
           OBPM/DBPM (2TM/3TM row kept over per-team rows for traded players)

RAPTOR is deliberately excluded here -- its source died in 2022, so it can't
represent the current season; use it separately for historical-era comparisons.

Output: nba-metric-data/metric_comparison_current.parquet/csv
  (name, nerd_o, nerd_d, darko_o, darko_d, epm_o, epm_d, bpm_o, bpm_d,
   + z-scored versions of each within this table, for apples-to-apples
   disagreement flagging despite each metric's different native scale)

Usage: python metric/build_metric_comparison.py
"""
from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
ROOT = Path(__file__).resolve().parent.parent
OUT = METRIC_DATA / "metric_comparison_current.parquet"

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    text = text.replace(".", "")
    text = re.sub(r"[^a-zA-Z0-9 ]", "", text)
    words = [w.lower() for w in text.split()]
    while words and words[-1] in SUFFIXES:
        words.pop()
    return " ".join(words)


def load_nerd() -> pd.DataFrame:
    m = pd.read_parquet(METRIC_DATA / "metric" / "metric_v0.parquet")
    latest = m["season_year"].max()
    m = m[m["season_year"] == latest].copy()
    m["key"] = m["player_name"].map(normalize_name)
    m = m.rename(columns={"m4000_o": "nerd_o", "m4000_d": "nerd_d"})
    print(f"NERD: season_year={latest}, {len(m)} players")
    return m[["key", "player_name", "nerd_o", "nerd_d"]]


def load_darko() -> pd.DataFrame:
    snaps = sorted((METRIC_DATA / "benchmarks" / "darko_snapshots").glob("darko_*.csv"))
    if not snaps:
        print("WARNING: no DARKO snapshots found")
        return pd.DataFrame(columns=["key", "darko_o", "darko_d"])
    latest = snaps[-1]
    d = pd.read_csv(latest)
    d["key"] = d["player_name"].map(normalize_name)
    d = d.rename(columns={"o_dpm": "darko_o", "d_dpm": "darko_d"})
    print(f"DARKO: {latest.name}, {len(d)} players")
    return d[["key", "darko_o", "darko_d"]]


def load_epm() -> pd.DataFrame:
    snaps = sorted((METRIC_DATA / "benchmarks" / "epm_snapshots").glob("epm_*.csv"))
    if not snaps:
        print("WARNING: no EPM snapshots found")
        return pd.DataFrame(columns=["key", "epm_o", "epm_d"])
    latest = snaps[-1]
    e = pd.read_csv(latest)
    e["key"] = e["name"].map(normalize_name)
    e = e.rename(columns={"off": "epm_o", "def": "epm_d"})
    print(f"EPM: {latest.name}, {len(e)} players")
    return e[["key", "epm_o", "epm_d"]]


def load_bpm() -> pd.DataFrame:
    files = sorted((METRIC_DATA / "benchmarks" / "bbref_advanced").glob("advanced_*.csv"))
    if not files:
        print("WARNING: no BBRef advanced files found")
        return pd.DataFrame(columns=["key", "bpm_o", "bpm_d"])
    latest = files[-1]
    b = pd.read_csv(latest)
    b["key"] = b["Player"].map(normalize_name)
    # traded players have per-team rows plus a 2TM/3TM/4TM total row -- keep the total
    b["is_total"] = b["Team"].astype(str).str.match(r"^\dTM$")
    b = b.sort_values("is_total", ascending=False).drop_duplicates("key", keep="first")
    b = b.rename(columns={"OBPM": "bpm_o", "DBPM": "bpm_d"})
    print(f"BPM: {latest.name}, {len(b)} players")
    return b[["key", "bpm_o", "bpm_d"]]


def zscore(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / s.std()


def main() -> None:
    nerd = load_nerd()
    darko = load_darko()
    epm = load_epm()
    bpm = load_bpm()

    df = nerd.merge(darko, on="key", how="outer") \
             .merge(epm, on="key", how="outer") \
             .merge(bpm, on="key", how="outer")
    df["name"] = df["player_name"].fillna(df["key"])
    df = df.drop(columns=["player_name", "key"])

    n_all = ((df[["nerd_o", "darko_o", "epm_o", "bpm_o"]].notna()).sum(axis=1) == 4).sum()
    print(f"\n{len(df)} total players matched across at least one source; "
          f"{n_all} present in all four")

    for col in ["nerd_o", "nerd_d", "darko_o", "darko_d", "epm_o", "epm_d", "bpm_o", "bpm_d"]:
        df[f"z_{col}"] = zscore(df[col])

    METRIC_DATA.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False)
    df.to_csv(OUT.with_suffix(".csv"), index=False)
    print(f"Wrote {OUT}")

    print("\nPairwise correlation (Pearson) among current-season O and D ratings:")
    for side in ("o", "d"):
        pairs = [("nerd", "darko"), ("nerd", "epm"), ("nerd", "bpm"),
                 ("darko", "epm"), ("darko", "bpm"), ("epm", "bpm")]
        print(f" {side.upper()}:")
        for a, b in pairs:
            ca, cb = f"{a}_{side}", f"{b}_{side}"
            sub = df[[ca, cb]].dropna()
            r = sub[ca].corr(sub[cb]) if len(sub) > 5 else float("nan")
            print(f"   {a:>5} vs {b:<5}: r={r:.3f}  (n={len(sub)})")

    both = df.dropna(subset=["z_nerd_d", "z_epm_d"]).copy()
    both["gap"] = both["z_nerd_d"] - both["z_epm_d"]
    print("\nBiggest NERD-vs-EPM defensive z-score disagreements (NERD higher):")
    print(both.nlargest(8, "gap")[["name", "nerd_d", "epm_d", "gap"]].to_string(index=False))
    print("\nBiggest NERD-vs-EPM defensive z-score disagreements (EPM higher):")
    print(both.nsmallest(8, "gap")[["name", "nerd_d", "epm_d", "gap"]].to_string(index=False))


if __name__ == "__main__":
    sys.exit(main())
