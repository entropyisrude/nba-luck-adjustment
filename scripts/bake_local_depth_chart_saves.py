"""Bake downloaded depth-chart save files into the local editor AND site defaults.

Scans the Downloads folder plus the repo root for depth_chart_save_*.json,
keeps the newest file per team (browser duplicates like
"depth_chart_save_UTA (1).json" count), and writes them to two places:

1. data/depth_chart_local_saves.js (gitignored) — fallback defaults for the
   local editor (its localStorage saves take precedence).
2. data/depth_chart_defaults.js (committed, published) — the hand-tuned
   defaults used by the PUBLISHED depth-chart-team.html and depth-charts.html
   in place of the cap-hit/MPG estimate. Teams without a save keep the
   estimate. Pages reconcile these against the live roster at render time.

Workflow: edit a team on depth-chart-local.html, hit Save Changes (persists
locally + downloads a snapshot), then rerun this script and push to publish.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
OUT_LOCAL = ROOT / "data" / "depth_chart_local_saves.js"
OUT_PUBLISHED = ROOT / "data" / "depth_chart_defaults.js"
SCAN_DIRS = [Path.home() / "Downloads", ROOT]
NAME_RE = re.compile(r"^depth_chart_save_([A-Z]{2,4})(?: \(\d+\))?\.json$")


def main() -> None:
    newest: dict[str, Path] = {}
    for d in SCAN_DIRS:
        if not d.exists():
            continue
        for path in d.iterdir():
            m = NAME_RE.match(path.name)
            if not m:
                continue
            abbr = m.group(1)
            if abbr not in newest or path.stat().st_mtime > newest[abbr].stat().st_mtime:
                newest[abbr] = path

    saves: dict[str, dict] = {}
    for abbr, path in sorted(newest.items()):
        try:
            save = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            print(f"  {abbr}: skipping {path.name} ({e})")
            continue
        if save.get("team") != abbr:
            print(f"  {abbr}: skipping {path.name} (file says team={save.get('team')})")
            continue
        saves[abbr] = save
        print(f"  {abbr}: {path.name} (saved {save.get('savedAt', '?')})")

    body = json.dumps(saves, ensure_ascii=False, separators=(",", ":"))
    OUT_LOCAL.write_text("window.DEPTH_CHART_LOCAL_SAVES = " + body + ";\n", encoding="utf-8")
    OUT_PUBLISHED.write_text("window.DEPTH_CHART_DEFAULTS = " + body + ";\n", encoding="utf-8")
    print(f"Wrote {len(saves)} team saves to {OUT_LOCAL} and {OUT_PUBLISHED}")


if __name__ == "__main__":
    main()
