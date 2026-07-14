"""
Snapshot the current-season EPM leaderboard (dunksandthrees.com) for the
cross-metric comparison project (alongside our NERD, DARKO, RAPTOR, BPM).

IMPORTANT scope note, confirmed by hands-on inspection (not assumed): this
is CURRENT-SEASON ONLY. dunksandthrees.com's per-player "historical" pages
(season-by-season archives) are paywalled -- only two named players (Jaylen
Brown, Donovan Mitchell) are free previews; every other player's page
returns "<Player>'s player dashboard is for subscribers." The full-league
CURRENT leaderboard at /epm, however, is genuinely free for all ~600
rostered players -- that's what this pulls. There is no EPM projection
product for future seasons on the free tier either, so this is a same-season
retrospective read, comparable in spirit to our DARKO current snapshot (not
comparable to NERD's or DARKO's forward projections).

The site blocks plain HTTP (curl gets 0 bytes -- bot/JS-gated), so this
drives a real headless browser and parses the leaderboard table by header
text (table index isn't stable -- the page has ~25 unrelated small
"recent games" tables before the real leaderboard table).

Output: nba-metric-data/benchmarks/epm_snapshots/epm_YYYY-MM-DD.csv
  (name, team, pos, height, age, mpg, usg, off, def, epm, pts, ts_pct,
   fg2a, fg2p, fg3a, fg3p, fta, ftp, orb, drb, ast, tov, stl, blk, rank)
One file per calendar date; re-running same-day overwrites (values can
shift intraday as games are added), matching the DARKO snapshot convention.

Usage: python scripts/snapshot_epm.py
"""
from __future__ import annotations

import datetime
import re
import sys
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

OUT_DIR = Path(r"C:\Users\Dave\Downloads\nba-metric-data\benchmarks\epm_snapshots")
URL = "https://dunksandthrees.com/epm"

COLS = ["mpg", "usg", "off", "def", "epm", "delta", "pts", "ts_pct",
        "fg2a", "fg2p", "fg3a", "fg3p", "fta", "ftp", "orb", "drb",
        "ast", "tov", "stl", "blk", "rank"]


def parse_identity(cell0: str) -> dict:
    # "Victor Wembanyama\nSAS · F-C 7'4\" 235 · 22"
    lines = cell0.split("\n")
    name = lines[0].strip()
    meta = lines[1] if len(lines) > 1 else ""
    m = re.match(r"([A-Z]{2,3})\s*·\s*([\w-]+)\s+([\d'\"\s]+)\s*·\s*(\d+)", meta)
    if not m:
        return {"name": name, "team": None, "pos": None, "height": None, "age": None}
    return {"name": name, "team": m.group(1), "pos": m.group(2),
            "height": m.group(3).strip(), "age": int(m.group(4))}


def first_line_num(s: str) -> float | None:
    v = s.split("\n")[0].strip()
    if v in ("", "-"):
        return None
    try:
        return float(v.replace("+", ""))
    except ValueError:
        return None


FIND_SCROLLER_JS = """
() => {
    const tables = document.querySelectorAll('table');
    let target = null;
    for (const t of tables) {
        const rows = t.querySelectorAll('tr');
        if (rows.length < 10) continue;
        const hdr = Array.from(rows[1]?.querySelectorAll('th,td') || []).map(c => c.innerText.trim());
        if (hdr.includes('OFF') && hdr.includes('DEF') && hdr.includes('EPM')) { target = t; break; }
    }
    if (!target) return false;
    let el = target.parentElement;
    while (el && getComputedStyle(el).overflowY !== 'auto') el = el.parentElement;
    if (!el) return false;
    window.__epmScroller = el;
    return true;
}
"""

EXTRACT_ROWS_JS = """
() => {
    const el = window.__epmScroller;
    const t = el.querySelector('table');
    if (!t) return [];
    return Array.from(t.querySelectorAll('tr')).slice(2).map(
        r => Array.from(r.querySelectorAll('th,td')).map(c => c.innerText.trim())
    );
}
"""

SCROLL_STATE_JS = """
() => {
    const el = window.__epmScroller;
    return { top: el.scrollTop, max: el.scrollHeight - el.clientHeight };
}
"""

SCROLL_BY_JS = """(dy) => { window.__epmScroller.scrollBy(0, dy); }"""


def main() -> None:
    by_name: dict[str, list[str]] = {}
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(user_agent="Mozilla/5.0", viewport={"width": 1600, "height": 1000})
        page.goto(URL, wait_until="networkidle", timeout=45000)
        page.wait_for_timeout(1500)

        # locate the scrollable ancestor (the leaderboard is virtualized -- only
        # ~15 rows exist in the DOM at once; the rest render as you scroll)
        found = page.evaluate(FIND_SCROLLER_JS)
        if not found:
            print("ERROR: could not find the EPM leaderboard table/scroll container", file=sys.stderr)
            sys.exit(1)

        prev_scroll_top = -1
        stall_count = 0
        for _ in range(400):
            rows = page.evaluate(EXTRACT_ROWS_JS)
            for cells in rows:
                if len(cells) == 22 and cells[0]:
                    name = cells[0].split("\n")[0].strip()
                    by_name[name] = cells
            state = page.evaluate(SCROLL_STATE_JS)
            if state["top"] >= state["max"] - 2:
                break
            if state["top"] == prev_scroll_top:
                stall_count += 1
                if stall_count > 3:
                    break
            else:
                stall_count = 0
            prev_scroll_top = state["top"]
            page.evaluate(SCROLL_BY_JS, 600)
            page.wait_for_timeout(180)
        browser.close()

    records = []
    for cells in by_name.values():
        rec = parse_identity(cells[0])
        for name, cell in zip(COLS, cells[1:]):
            rec[name] = first_line_num(cell)
        records.append(rec)

    df = pd.DataFrame(records)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    date = datetime.date.today().isoformat()
    out = OUT_DIR / f"epm_{date}.csv"
    df.to_csv(out, index=False)
    print(f"wrote {out} ({len(df)} players)")
    if len(df):
        top = df.sort_values("epm", ascending=False).head(5)
        print(top[["name", "team", "off", "def", "epm"]].to_string(index=False))


if __name__ == "__main__":
    main()
