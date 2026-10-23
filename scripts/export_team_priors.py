"""Export each team's preseason-style offense/defense prior from the win-projections page.

The schedule adjustment on the luck report needs a prior for every team's
offense and defense. team-projections.html already computes one in the
browser (depth-chart minutes x player ratings, the "NERD + DARKO average"
source, all-healthy). Rather than re-implement that logic, this serves the
repo locally, loads the page in headless Chromium, and calls its own
computeTeamsAvg(), so the prior always matches what the page shows.

Output: data/team_priors_<TAG>.json
  {"generated_at", "source", "teams": {ABBR: {"off": .., "def": .., "net": ..}}}
off/def are points per 100 possessions relative to league average (positive =
better; def is points prevented), net = off + def.

Usage: python scripts/export_team_priors.py   (needs: pip install playwright;
       python -m playwright install chromium)
"""
from __future__ import annotations

import functools
import http.server
import json
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.season import SEASON, TAG  # noqa: E402

OUT = ROOT / "data" / f"team_priors_{TAG}.json"


def main() -> None:
    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *args):
            pass

    handler = functools.partial(QuietHandler, directory=str(ROOT))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}/team-projections.html"
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            errors: list[str] = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.goto(url, wait_until="networkidle")
            page.select_option("#health-sel", "healthy")
            teams = page.evaluate(
                "computeTeamsAvg().map(t => ({abbr: t.abbr, off: t.offC, def: t.defC, net: t.netC}))")
            browser.close()
    finally:
        server.shutdown()
    if errors or len(teams) != 30:
        raise SystemExit(f"projection page failed: {len(teams)} teams, errors={errors}")
    payload = {
        "season": SEASON,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "team-projections.html computeTeamsAvg() (NERD + DARKO average, depth-chart minutes, all healthy)",
        "teams": {t["abbr"]: {k: round(t[k], 3) for k in ("off", "def", "net")} for t in teams},
    }
    OUT.write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8")
    best = max(teams, key=lambda t: t["net"])
    print(f"wrote {OUT} (30 teams; top: {best['abbr']} {best['net']:+.1f})")


if __name__ == "__main__":
    main()
