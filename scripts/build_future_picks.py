#!/usr/bin/env python3
"""
Parse NBA future traded picks from RealGM Word document into structured JSON.

Input:  NBA Future Draft Pick Details.docx (in user Documents folder)
Output: data/future_picks.json

Run from project root:
    python scripts/build_future_picks.py
"""

import zipfile
import xml.etree.ElementTree as ET
import json
import re
from pathlib import Path

DOCX_PATH = Path(r"C:\Users\Dave\Documents\NBA Future Draft Pick Details.docx")
OUT_PATH = Path(__file__).parent.parent / "data" / "future_picks.json"

# Team city-name → abbreviation
TEAM_ABBR = {
    "Atlanta": "ATL", "Boston": "BOS", "Brooklyn": "BKN",
    "Charlotte": "CHA", "Chicago": "CHI", "Cleveland": "CLE",
    "Dallas": "DAL", "Denver": "DEN", "Detroit": "DET",
    "Golden State": "GSW", "Houston": "HOU", "Indiana": "IND",
    "L.A. Clippers": "LAC", "L.A. Lakers": "LAL", "Memphis": "MEM",
    "Miami": "MIA", "Milwaukee": "MIL", "Minnesota": "MIN",
    "New Orleans": "NOP", "New York": "NYK", "Oklahoma City": "OKC",
    "Orlando": "ORL", "Philadelphia": "PHI", "Phoenix": "PHX",
    "Portland": "POR", "Sacramento": "SAC", "San Antonio": "SAS",
    "Toronto": "TOR", "Utah": "UTA", "Washington": "WAS",
}

FULL_NAMES = {
    "Atlanta": "Atlanta Hawks", "Boston": "Boston Celtics",
    "Brooklyn": "Brooklyn Nets", "Charlotte": "Charlotte Hornets",
    "Chicago": "Chicago Bulls", "Cleveland": "Cleveland Cavaliers",
    "Dallas": "Dallas Mavericks", "Denver": "Denver Nuggets",
    "Detroit": "Detroit Pistons", "Golden State": "Golden State Warriors",
    "Houston": "Houston Rockets", "Indiana": "Indiana Pacers",
    "L.A. Clippers": "Los Angeles Clippers", "L.A. Lakers": "Los Angeles Lakers",
    "Memphis": "Memphis Grizzlies", "Miami": "Miami Heat",
    "Milwaukee": "Milwaukee Bucks", "Minnesota": "Minnesota Timberwolves",
    "New Orleans": "New Orleans Pelicans", "New York": "New York Knicks",
    "Oklahoma City": "Oklahoma City Thunder", "Orlando": "Orlando Magic",
    "Philadelphia": "Philadelphia 76ers", "Phoenix": "Phoenix Suns",
    "Portland": "Portland Trail Blazers", "Sacramento": "Sacramento Kings",
    "San Antonio": "San Antonio Spurs", "Toronto": "Toronto Raptors",
    "Utah": "Utah Jazz", "Washington": "Washington Wizards",
}

# Build reverse lookup: full name → abbr
FULLNAME_TO_ABBR = {v: TEAM_ABBR[k] for k, v in FULL_NAMES.items()}

# Sorted longest-first for greedy matching
TEAMS_SORTED = sorted(TEAM_ABBR.keys(), key=len, reverse=True)

# Words/phrases that can follow a team name to start the full description
FULL_DESC_STARTERS = (
    "'s ", "' ",  # possessive (New Orleans', L.A. Lakers's)
    " will ", " has ", " may ", " should ", " is ", " was ",
    " are ", " had ", " can ", " could ", " would ", " receives ",
)


# ---------------------------------------------------------------------------
# DOCX text extraction
# ---------------------------------------------------------------------------

def extract_text_from_docx(docx_path: Path) -> list[str]:
    """Extract paragraph text lines from DOCX (preserves paragraph order)."""
    NS = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
    with zipfile.ZipFile(docx_path) as z:
        xml_bytes = z.read('word/document.xml')
    root = ET.fromstring(xml_bytes)
    lines = []
    for para in root.findall('.//w:p', NS):
        runs = para.findall('.//w:t', NS)
        text = ''.join(r.text or '' for r in runs).strip()
        if text:
            lines.append(text)
    return lines


# ---------------------------------------------------------------------------
# Text splitting: short desc + full desc are concatenated in each line
# ---------------------------------------------------------------------------

def split_pick_line(line: str) -> tuple[str, str]:
    """
    Split "short description + full description" from a concatenated pick line.
    Returns (short_desc, full_desc).

    The short description looks like:
      "YYYY [first|second] round draft pick[s]? [from|to] TEAM [optional (parenthetical)]"

    The full description is a proper sentence explaining the pick's details.
    """
    def is_valid_short(before: str) -> bool:
        return bool(re.search(r'^20\d\d (?:first|second) round draft', before))

    def after_is_full_desc(after: str) -> bool:
        after = after.lstrip()
        if after.startswith(("If ", "Only if ")):
            return True
        for team in TEAMS_SORTED:
            if after.startswith(team):
                rest = after[len(team):]
                if any(rest.startswith(s) for s in FULL_DESC_STARTERS):
                    return True
        return False

    # Strategy 1: Find the first ")" that is immediately followed by the start
    # of a full description. This handles parentheticals like "(swap, X incoming)".
    for m in re.finditer(r'\)', line):
        before = line[:m.end()]
        after = line[m.end():]
        if is_valid_short(before) and after_is_full_desc(after):
            return before.strip(), after.strip()

    # Strategy 2: Find a team name that is immediately followed by either:
    #   - Another team name + possessive/action word (different teams)
    #   - The same team name + possessive/action word (doubled)
    #   - "If" or "Only if" (conditional full desc)
    for team_end in TEAMS_SORTED:
        search_pos = 0
        while True:
            idx = line.find(team_end, search_pos)
            if idx < 0:
                break
            before = line[:idx + len(team_end)]
            after = line[idx + len(team_end):]
            if is_valid_short(before) and after_is_full_desc(after):
                return before.strip(), after.strip()
            search_pos = idx + 1

    # Fallback: return empty short and full line as description
    return "", line.strip()


# ---------------------------------------------------------------------------
# Field extraction helpers
# ---------------------------------------------------------------------------

def extract_protection(text: str) -> str | None:
    """Return protection range string like '1-4', '1-14', '56-60', or None."""
    m = re.search(r'protected for (?:selections?\s+)?([\d]+-[\d]+|[\d]+)\b', text)
    return m.group(1) if m else None


def extract_trade_refs(text: str) -> list[dict]:
    """
    Extract trade references from trailing [...] notation.
    Each ref is "{Team1}-{Team2}-..., MM/DD/YYYY".
    Returns list of {"teams": [...], "date": "MM/DD/YYYY"} dicts.
    """
    refs = []
    m = re.search(r'\[(.+?)\]\s*$', text)
    if not m:
        return refs
    for part in m.group(1).split(';'):
        part = part.strip()
        if ',' in part:
            teams_str, date_str = part.rsplit(',', 1)
            teams = [t.strip() for t in teams_str.strip().split('-')]
            refs.append({'teams': teams, 'date': date_str.strip()})
    return refs


def classify_pick(full_desc: str) -> str:
    """
    Classify pick type from the full description text.

    Returns one of:
      "transfer"    - straightforward pick conveyance
      "swap"        - simple two-team swap right
      "multi_swap"  - pick determined by favorability ranking (2+ team comparison)
      "conditional" - pick only conveys if an external condition is met
    """
    lo = full_desc.lower()
    # Any favorability-based ranking = multi_swap (covers 2-team and 3+-team)
    if any(phrase in lo for phrase in (
        'most favorable', 'least favorable', 'more favorable', 'less favorable'
    )):
        return 'multi_swap'
    if 'has the right to swap' in lo or 'will swap' in lo:
        return 'swap'
    if lo.startswith('if ') or lo.startswith('only if '):
        return 'conditional'
    return 'transfer'


def swap_favorability(short_desc: str) -> str | None:
    """
    For swap-related picks, detect if context team gets the more or less
    favorable pick. Returns 'favorable', 'unfavorable', or None.
    """
    lo = short_desc.lower()
    m = re.search(r'\(((?:more|most|less|least) favorable[^)]*)\)', lo)
    if not m:
        return None
    phrase = m.group(1)
    if phrase.startswith(('less', 'least')):
        return 'unfavorable'
    if phrase.startswith(('more', 'most')):
        return 'favorable'
    return None


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

SKIP_LINES = frozenset({
    'NBA Future Draft Pick Details',
    'Year', 'Incoming', 'Outgoing',
    'No picks incoming.', 'No picks outgoing.',
})

PICK_LINE_RE = re.compile(r'^(20\d\d) (?:first|second) round draft pick')
YEAR_LINE_RE = re.compile(r'^(20\d\d)$')
TEAM_HEADER_RE = re.compile(r'^(.+?) Future Traded Pick Details$')


_OVERLAP_ANNOTATION = re.compile(r'\[this overlaps[^\]]*\]', re.IGNORECASE)


def parse_picks(lines: list[str]) -> list[dict]:
    picks = []
    current_team_abbr: str | None = None
    current_year: int | None = None

    for raw_line in lines:
        # Strip RealGM editorial annotations like "[this overlaps with another...]"
        # These appear between the short desc and full desc and break the split.
        line = _OVERLAP_ANNOTATION.sub('', raw_line).strip()
        line = line if line else raw_line
        if not line or line in SKIP_LINES:
            continue

        # Team section header
        m = TEAM_HEADER_RE.match(line)
        if m:
            team_full = m.group(1)
            current_team_abbr = FULLNAME_TO_ABBR.get(team_full)
            if not current_team_abbr:
                # Try partial match
                for short, abbr in TEAM_ABBR.items():
                    if short in team_full:
                        current_team_abbr = abbr
                        break
            current_year = None
            continue

        # Year marker
        m = YEAR_LINE_RE.match(line)
        if m:
            current_year = int(m.group(1))
            continue

        # Pick line
        m = PICK_LINE_RE.match(line)
        if m and current_team_abbr:
            year = int(m.group(1))
            round_num = 1 if 'first round' in line else 2
            direction = 'incoming' if ' from ' in line.split(')')[0][:80] else 'outgoing'

            short_desc, full_desc = split_pick_line(line)

            # Re-check direction from short desc (more reliable)
            if short_desc:
                direction = 'incoming' if ' from ' in short_desc else 'outgoing'

            protection = extract_protection(full_desc) or extract_protection(short_desc)
            trade_refs = extract_trade_refs(full_desc)
            pick_type = classify_pick(full_desc)
            favorability = swap_favorability(short_desc)
            extinction = 'will be extinguished' in full_desc.lower()

            picks.append({
                'context_team': current_team_abbr,
                'year': year,
                'round': round_num,
                'direction': direction,
                'pick_type': pick_type,         # transfer | swap | multi_swap | conditional
                'favorability': favorability,    # favorable | unfavorable | None
                'protection': protection,        # e.g. "1-4", "1-14", None
                'extinction': extinction,        # obligation ends if pick doesn't convey
                'short_desc': short_desc,
                'full_desc': full_desc,
                'trade_refs': trade_refs,
            })

    return picks


# ---------------------------------------------------------------------------
# Validation / diagnostics
# ---------------------------------------------------------------------------

def validate(picks: list[dict]) -> None:
    """Print diagnostics to help spot parsing failures."""
    empty_short = [p for p in picks if not p['short_desc']]
    empty_full = [p for p in picks if not p['full_desc']]
    print(f"  Total picks parsed: {len(picks)}")
    print(f"  Picks with empty short_desc: {len(empty_short)}")
    print(f"  Picks with empty full_desc: {len(empty_full)}")
    for p in empty_short[:5]:
        print(f"    WARN no short_desc: {p['context_team']} {p['year']}r{p['round']} — {p['full_desc'][:80]}")

    by_type = {}
    for p in picks:
        by_type[p['pick_type']] = by_type.get(p['pick_type'], 0) + 1
    print(f"  Pick types: {by_type}")

    # Check that every team has at least some picks
    teams_seen = {p['context_team'] for p in picks}
    all_teams = set(TEAM_ABBR.values())
    missing = all_teams - teams_seen
    if missing:
        print(f"  WARN teams with no picks: {sorted(missing)}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print(f"Reading: {DOCX_PATH}")
    lines = extract_text_from_docx(DOCX_PATH)
    print(f"  {len(lines)} paragraphs extracted")

    picks = parse_picks(lines)
    validate(picks)

    # Group by team for quick sanity check
    from collections import Counter
    team_counts = Counter(p['context_team'] for p in picks)
    print(f"\nPicks per team (sample):")
    for team, count in sorted(team_counts.items()):
        print(f"  {team}: {count}")

    output = {
        "generated": "2026-05-31",
        "source": "RealGM (scraped 2026-05-31)",
        "picks": picks,
    }

    OUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nWrote {len(picks)} pick entries to {OUT_PATH}")


if __name__ == '__main__':
    main()
