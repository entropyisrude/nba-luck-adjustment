"""
fetch_hoopshype_rumors.py
Scrapes HoopsHype rumors page and prints suggested patch entries
for any transaction items not yet in the current Spotrac snapshot.

Usage:
    python scripts/fetch_hoopshype_rumors.py
    python scripts/fetch_hoopshype_rumors.py --apply   # writes directly to patches file
"""
from __future__ import annotations

import io
import json
import re
import sys
import argparse

# Force UTF-8 output on Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT     = Path(__file__).resolve().parents[1]
SNAP_JS  = ROOT / "data" / "nba_cap_data_2026_27.js"
PATCH_JS = ROOT / "data" / "patches_2026_27.js"
URL      = "https://hoopshype.com/rumors/"

# ── Team name → abbreviation map ─────────────────────────────────────────────
TEAM_MAP = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "LA Clippers": "LAC", "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL",
    "LA Lakers": "LAL", "Memphis Grizzlies": "MEM", "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL", "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP", "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC", "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS", "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA", "Washington Wizards": "WAS",
    # city-only shorthand
    "Atlanta": "ATL", "Boston": "BOS", "Brooklyn": "BKN", "Charlotte": "CHA",
    "Chicago": "CHI", "Cleveland": "CLE", "Dallas": "DAL", "Denver": "DEN",
    "Detroit": "DET", "Golden State": "GSW", "Houston": "HOU", "Indiana": "IND",
    "Clippers": "LAC", "Lakers": "LAL", "Memphis": "MEM", "Miami": "MIA",
    "Milwaukee": "MIL", "Minnesota": "MIN", "New Orleans": "NOP",
    "New York": "NYK", "Knicks": "NYK", "Oklahoma City": "OKC", "Orlando": "ORL",
    "Philadelphia": "PHI", "Phoenix": "PHX", "Portland": "POR",
    "Sacramento": "SAC", "San Antonio": "SAS", "Toronto": "TOR",
    "Utah": "UTA", "Washington": "WAS",
}

# Longest names first so greedy match works
_TEAM_NAMES_SORTED = sorted(TEAM_MAP, key=len, reverse=True)
_TEAM_RE = re.compile(
    r'\b(' + '|'.join(re.escape(t) for t in _TEAM_NAMES_SORTED) + r')\b'
)

TRANSACTION_KEYWORDS = re.compile(
    r'\b(sign|signed|agree|agreed|deal|contract|traded|trade|acquir|waiv|releas|'
    r'extension|extend|option|two-year|three-year|four-year|multi-year|year[,\s])\b',
    re.I
)

# ── Salary parsing ─────────────────────────────────────────────────────────────
_SALARY_RE = re.compile(
    r'\$\s*([\d,.]+)\s*(million|mil\b|m\b)',
    re.I
)
_YEARS_RE = re.compile(
    r'\b(one|two|three|four|five|1|2|3|4|5)-?year',
    re.I
)
_YEARS_MAP = {'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
              '1': 1, '2': 2, '3': 3, '4': 4, '5': 5}


def parse_salary(text: str) -> int | None:
    """Return annual cap hit in dollars, or None."""
    m = _SALARY_RE.search(text)
    if not m:
        return None
    total_m = float(m.group(1).replace(',', ''))
    years_m = _YEARS_RE.search(text[:m.start() + 60])
    if years_m:
        yrs = _YEARS_MAP.get(years_m.group(1).lower(), 1)
        return int(total_m / yrs * 1_000_000)
    # Assume the dollar figure is already annual if no years found
    return int(total_m * 1_000_000)


def parse_team(text: str) -> str | None:
    m = _TEAM_RE.search(text)
    return TEAM_MAP[m.group(1)] if m else None


# ── Load current snapshot player index ───────────────────────────────────────
def load_snapshot_players() -> set[str]:
    """Return set of lowercased player names in current snapshot."""
    raw = SNAP_JS.read_text(encoding='utf-8')
    return set(re.findall(r'"player"\s*:\s*"([^"]+)"', raw, re.I))


# ── HoopsHype fetch ───────────────────────────────────────────────────────────
def fetch_rumors() -> list[dict]:
    resp = requests.get(URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

    items = []
    for p in soup.find_all('p'):
        text = p.get_text(' ', strip=True)
        if len(text) < 40:
            continue
        if not TRANSACTION_KEYWORDS.search(text):
            continue

        # Extract reporter prefix "Name: rest" or "First Last: rest"
        reporter = ''
        body = text
        colon_idx = text.find(':')
        if 0 < colon_idx < 40:
            prefix = text[:colon_idx].strip()
            word_count = len(prefix.split())
            if 1 <= word_count <= 3 and not any(c in prefix for c in '.,!?'):
                reporter = prefix
                body = text[colon_idx + 1:].strip()

        # Try to find story link in same element or parent
        link = ''
        parent = p.parent
        for _ in range(4):
            a = parent.find('a', href=lambda h: h and '/story/' in h) if parent else None
            if a:
                link = a['href']
                break
            parent = parent.parent if parent else None

        items.append({
            'reporter': reporter,
            'text':     body,
            'link':     link,
            'team':     parse_team(body),
            'salary':   parse_salary(body),
        })

    return items


# ── Patch suggestion ──────────────────────────────────────────────────────────
def suggest_patch(item: dict, known_players: set[str]) -> str | None:
    """Return a patch line string, or None if we can't confidently parse it."""
    text = item['text']
    team = item['team']
    salary = item['salary']

    # Look for signing pattern: "Player X has agreed / signed"
    sign_m = re.search(
        r'([A-Z][a-z]+ (?:[A-Z][a-z]+ )?[A-Z][a-z]+) (?:has |have )?(?:agreed|signed)',
        text
    )
    if sign_m and team and salary:
        player = sign_m.group(1)
        return f"  {{ op:'sign', team:'{team}', player:'{player}', cap_hit:{salary} }},"

    # Look for trade pattern: "Player X (?:has been )?traded to Team"
    trade_m = re.search(
        r'([A-Z][a-z]+ (?:[A-Z][a-z]+ )?[A-Z][a-z]+) (?:has been |was |is )?traded to',
        text
    )
    if trade_m and team:
        player = trade_m.group(1)
        return f"  {{ op:'trade', player:'{player}', to:'{team}' }},  // verify 'from' team"

    return None


def fmt_dollars(n: int) -> str:
    return f"${n / 1_000_000:.1f}M"


def _team_block(snap_raw: str, team_key: str) -> str:
    """Return the JSON substring for a single team so player checks are team-scoped."""
    m = re.search(rf'"{re.escape(team_key)}"\s*:\s*\{{', snap_raw)
    if not m:
        return ''
    depth, i = 0, m.start()
    while i < len(snap_raw):
        if snap_raw[i] == '{': depth += 1
        elif snap_raw[i] == '}':
            depth -= 1
            if depth == 0:
                return snap_raw[m.start():i+1]
        i += 1
    return ''


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true',
                        help='Append suggested patches directly to patches_2026_27.js')
    args = parser.parse_args()

    print(f"Fetching {URL} ...")
    rumors = fetch_rumors()
    known  = load_snapshot_players()

    print(f"Found {len(rumors)} transaction items\n")
    print("=" * 70)

    suggestions = []
    for item in rumors:
        reporter = item['reporter'] or 'Unknown'
        print(f"[{reporter}]")
        print(f"  {item['text'][:200]}")
        if item['team']:
            print(f"  -> team: {item['team']}", end='')
            if item['salary']:
                print(f"  salary: {fmt_dollars(item['salary'])}/yr", end='')
            print()
        patch = suggest_patch(item, known)
        if patch:
            # Only skip if the player is already on that specific team
            # (not just anywhere in the snapshot, e.g. as a cap hold on another team)
            player_m = re.search(r"player:'([^']+)'", patch)
            team_m   = re.search(r"team:'([^']+)'", patch)
            player_name = player_m.group(1) if player_m else ''
            dest_team   = team_m.group(1)   if team_m   else ''
            snap_raw = SNAP_JS.read_text(encoding='utf-8')
            already_there = bool(re.search(
                rf'"player"\s*:\s*"{re.escape(player_name)}"',
                _team_block(snap_raw, dest_team)
            )) if dest_team else False
            if already_there:
                print(f"  (already on {dest_team} in snapshot — skipping patch)")
            else:
                print(f"  PATCH SUGGESTION: {patch.strip()}")
                suggestions.append((item, patch))
        if item['link']:
            print(f"  {item['link']}")
        print()

    if not suggestions:
        print("No auto-parseable patches found — review items above manually.")
        return

    print("=" * 70)
    print(f"\n{len(suggestions)} suggested patch(es):\n")
    for _, patch in suggestions:
        print(patch)

    if args.apply:
        _write_patches(suggestions)
    else:
        print("\nRun with --apply to insert these into patches_2026_27.js")


def _write_patches(suggestions: list[tuple]):
    src = PATCH_JS.read_text(encoding='utf-8')
    date_str = datetime.now(timezone.utc).strftime('%B %-d, %Y')
    block = f"\n    // ── {date_str} (auto-imported from HoopsHype) ─────────────\n"
    for item, patch in suggestions:
        block += f"    // {item['reporter']}: {item['text'][:80]}\n"
        block += f"    {patch.strip()}\n"

    # Insert just after the opening of the patches array
    marker = "// ── ADD ENTRIES BELOW"
    idx = src.find(marker)
    if idx < 0:
        print("ERROR: couldn't find insertion point in patches file")
        sys.exit(1)
    end = src.index('\n', idx) + 1
    new_src = src[:end] + block + src[end:]
    PATCH_JS.write_text(new_src, encoding='utf-8')
    print(f"\nWrote {len(suggestions)} patch(es) to {PATCH_JS}")


if __name__ == '__main__':
    main()
