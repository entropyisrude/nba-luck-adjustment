"""Snapshot DARKO's current player table for future benchmarking.

DARKO publishes no historical archive — only the current day's values — so
apples-to-apples backtests against it require accumulating our own history.
This pulls the SvelteKit data payload behind darko.app (devalue-serialized),
flattens the players table, and writes one CSV per DARKO data date to
nba-metric-data/benchmarks/darko_snapshots/darko_YYYY-MM-DD.csv. Re-runs
are no-ops until DARKO's own `date` field advances, so a daily schedule is
safe year-round.

Usage: python metric/snapshot_darko.py
"""
from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

import pandas as pd

URL = "https://www.darko.app/__data.json"
OUT_DIR = Path(r"C:\Users\Dave\Downloads\nba-metric-data\benchmarks\darko_snapshots")


def resolve(data, idx):
    v = data[idx]
    if isinstance(v, dict):
        return {k: resolve(data, i) for k, i in v.items()}
    if isinstance(v, list):
        return [resolve(data, i) for i in v]
    return v


def main() -> None:
    req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
    raw = json.load(urllib.request.urlopen(req, timeout=60))
    node = next(n for n in raw["nodes"] if n and n.get("type") == "data")
    data = node["data"]
    root = data[0]
    players = [resolve(data, i) for i in data[root["players"]]]
    df = pd.DataFrame(players)
    date = df["date"].mode().iat[0]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"darko_{date}.csv"
    if out.exists():
        print(f"unchanged (DARKO date {date} already snapshotted)")
        return
    df.to_csv(out, index=False)
    print(f"wrote {out} ({len(df)} players, DARKO date {date})")


if __name__ == "__main__":
    sys.exit(main())
