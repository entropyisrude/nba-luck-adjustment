"""Bake downloaded depth-chart save files into data/depth_chart_local_saves.js.

depth-chart-local.html's Save button used to only download a
depth_chart_save_<ABBR>.json snapshot that had to be re-loaded by hand every
visit. The page now persists saves in localStorage automatically; this script
migrates previously-downloaded snapshots (and acts as a fallback for a fresh
browser profile): it scans the Downloads folder plus the repo root for
depth_chart_save_*.json, keeps the newest file per team (browser duplicates
like "depth_chart_save_UTA (1).json" count), and writes them all into
data/depth_chart_local_saves.js (gitignored, loaded only by the local editor).

Precedence in the page: localStorage save > this baked file > live estimate.

Rerun after downloading new saves on another machine/browser, or just hit
Save Changes in the page itself (localStorage wins anyway).
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

ROOT = Path(os.environ.get("NBA_ONOFF_ROOT", str(Path(__file__).resolve().parents[1])))
OUT = ROOT / "data" / "depth_chart_local_saves.js"
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

    OUT.write_text(
        "window.DEPTH_CHART_LOCAL_SAVES = "
        + json.dumps(saves, ensure_ascii=False, separators=(",", ":"))
        + ";\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(saves)} team saves to {OUT}")


if __name__ == "__main__":
    main()
