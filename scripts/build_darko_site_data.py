"""Export the latest DARKO snapshot for the win-projections page.

Reads the newest CSV from nba-metric-data/benchmarks/darko_snapshots/
(accumulated by metric/snapshot_darko.py) and writes a compact lookup to
data/darko_current.js keyed by normalized player name (same normalization
as build_last_season_mpg.py, so Spotrac roster names + the ALIASES map
resolve against it).

Fetches DARKO's current table live from darko.app (DARKO re-fits its
ratings between data dates, so a same-date snapshot can be stale); the newest
desktop snapshot is only a fallback when the fetch fails.

Usage: python scripts/build_darko_site_data.py
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_last_season_mpg import normalize_name

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "metric"))
from snapshot_darko import fetch_players  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SNAP_DIR = Path(r"C:\Users\Dave\Downloads\nba-metric-data\benchmarks\darko_snapshots")
OUT = ROOT / "data" / "darko_current.js"


def main() -> None:
    try:
        df = fetch_players()
    except Exception as exc:
        snapshots = sorted(SNAP_DIR.glob("darko_*.csv"), key=lambda p: p.stat().st_mtime) if SNAP_DIR.exists() else []
        if not snapshots:
            raise
        print(f"darko.app fetch failed ({exc}); using snapshot {snapshots[-1].name}")
        df = pd.read_csv(snapshots[-1])
    date = df["date"].mode().iat[0]
    players = {}
    for r in df.itertuples(index=False):
        players[normalize_name(r.player_name)] = {
            "dpm": round(float(r.dpm), 2),
            "o": round(float(r.o_dpm), 2),
            "d": round(float(r.d_dpm), 2),
        }
    payload = {"date": date, "source": "darko.app (snapshotted daily)",
               "players": players}
    OUT.write_text("window.DARKO_CURRENT = "
                   + json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
                   + ";\n", encoding="utf-8")
    print(f"wrote {OUT} ({len(players)} players, DARKO date {date})")


if __name__ == "__main__":
    main()
