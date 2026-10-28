"""Latest official NBA injury report status for every team.

official.nba.com posts the league's injury report as a PDF several times a day
(ak-static.cms.nba.com/referee/injury/Injury-Report_YYYY-MM-DD_HH_MMAM.pdf).
Each report covers that day's games, so a team's current list comes from the
newest report that includes it; we look back a few days for teams that have
not played since.

Writes data/injury_report_<TAG>.json:
  {"generated", "teams": {ABBR: {"report", "game_date", "players": [{name, status, reason}]}}}
"""
from __future__ import annotations

import io
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pdfplumber
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.season import START_YEAR, TAG  # noqa: E402

INDEX_URL = f"https://official.nba.com/nba-injury-report-{START_YEAR}-{(START_YEAR + 1) % 100:02d}-season/"
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OUT_PATH = DATA_DIR / f"injury_report_{TAG}.json"
HEADERS = {"User-Agent": "Mozilla/5.0"}
LOOKBACK_DAYS = 4
STATUSES = {"Out", "Doubtful", "Questionable", "Probable", "Available"}
TEAMS = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN", "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE", "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET", "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "LA Clippers": "LAC", "Los Angeles Lakers": "LAL", "Memphis Grizzlies": "MEM", "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL", "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC", "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX", "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC", "San Antonio Spurs": "SAS", "Toronto Raptors": "TOR", "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}
_SQUASH = {re.sub(r"\W", "", k).lower(): v for k, v in TEAMS.items()}
_LINK_RE = re.compile(r"https://ak-static\.cms\.nba\.com/referee/injury/Injury-Report_"
                      r"(\d{4}-\d{2}-\d{2})_(\d{1,2})(?:_(\d{2}))?(AM|PM)\.pdf")


def report_links() -> list[tuple[datetime, str]]:
    html = requests.get(INDEX_URL, headers=HEADERS, timeout=30).text
    out = {}
    for m in _LINK_RE.finditer(html):
        day, hh, mm, ampm = m.groups()
        h = int(hh) % 12 + (12 if ampm == "PM" else 0)
        out[m.group(0)] = datetime.strptime(day, "%Y-%m-%d") + timedelta(hours=h, minutes=int(mm or 0))
    return sorted(((t, u) for u, t in out.items()), reverse=True)


def _player_name(text: str) -> str:
    """'Leonard, Kawhi' -> 'Kawhi Leonard'; 'Udeh, Jr., Ernest' -> 'Ernest Udeh Jr.'"""
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if len(parts) < 2:
        return text.strip()
    return " ".join([parts[-1], *parts[:-1]])


def _segments(lines: list[dict], x_left: float, x_right: float, top: float, bottom: float) -> list[tuple[float, float]]:
    """Cell spans of one column: the horizontal rules crossing it split it into cells."""
    cuts = sorted({round(l["top"], 1) for l in lines
                   if l["x0"] <= x_left + 2 and l["x1"] >= x_right - 2 and top <= l["top"] <= bottom})
    edges = [top, *cuts, bottom]
    return [(a, b) for a, b in zip(edges, edges[1:]) if b - a > 2]


def parse_report(pdf_bytes: bytes) -> list[dict]:
    """Rows of {game_date, team, name, status, reason}.

    The report is a ruled table whose date/matchup/team cells are merged
    vertically (label centered in the block), so each value is located by the
    cell borders: horizontal rules crossing a column split it into cells."""
    rows: list[dict] = []
    team = game_date = None
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            rules = [l for l in page.lines if abs(l["top"] - l["bottom"]) < 1]
            if not rules:
                continue
            # Column edges are the rules' x positions: date|time|matchup|team|player|status|reason.
            xs = sorted({round(l["x0"]) for l in rules} | {round(l["x1"]) for l in rules})
            if len(xs) < 8:
                continue
            date_x, team_x, name_x, status_x, reason_x, right_x = xs[0], xs[3], xs[4], xs[5], xs[6], xs[-1]
            words = [w for w in page.extract_words(x_tolerance=1.5)
                     if not w["text"].startswith("Page") and not w["text"].startswith("Injury Report")]
            # Cells open at the header row (first page) or the page title; the
            # first rule is only the bottom of the first cell.
            header = [w["bottom"] for w in words if w["text"] == "Matchup"]
            top = header[0] + 1 if header else 60
            words = [w for w in words if w["top"] >= top]
            bottom = max(l["top"] for l in rules)

            def text_in(x0, x1, a, b):
                ws = [w for w in words if x0 <= w["x0"] < x1 and a <= (w["top"] + w["bottom"]) / 2 < b]
                return " ".join(w["text"] for w in sorted(ws, key=lambda w: (round(w["top"]), w["x0"])))

            team_cells = [(a, b, text_in(team_x, name_x, a, b)) for a, b in _segments(rules, team_x, name_x, top, bottom)]
            date_cells = [(a, b, text_in(date_x, xs[1], a, b)) for a, b in _segments(rules, date_x, xs[1], top, bottom)]
            for a, b in _segments(rules, name_x, status_x, top, bottom):
                mid = (a + b) / 2
                for ca, cb, label in date_cells:
                    if ca <= mid < cb and re.search(r"\d{2}/\d{2}/\d{4}", label):
                        game_date = datetime.strptime(re.search(r"\d{2}/\d{2}/\d{4}", label).group(0),
                                                      "%m/%d/%Y").strftime("%Y-%m-%d")
                for ca, cb, label in team_cells:
                    key = re.sub(r"\W", "", label).lower()
                    if ca <= mid < cb and key in _SQUASH:
                        team = _SQUASH[key]
                name = text_in(name_x, status_x, a, b)
                status = text_in(status_x, reason_x, a, b)
                if status in STATUSES and name and team:
                    reason = re.sub(r"\s*/\s*", "/", text_in(reason_x, right_x + 1, a, b))
                    rows.append({"game_date": game_date, "team": team, "name": _player_name(name),
                                 "status": status, "reason": reason})
    return rows


def _fetch(url: str) -> bytes | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        return r.content if r.status_code == 200 and r.content[:4] == b"%PDF" else None
    except requests.RequestException:
        return None


def main() -> None:
    links = report_links()
    if not links:
        print("No injury reports found; keeping previous file.")
        return
    newest_time, newest_url = links[0]
    # The index lists only today's reports; earlier days' files follow the same
    # naming, so take a late-afternoon (pre-tip) report from each of them.
    candidates = [(newest_time, newest_url)]
    for back in range(1, LOOKBACK_DAYS + 1):
        day = (newest_time - timedelta(days=back)).strftime("%Y-%m-%d")
        for hhmm in ("05_30PM", "05_00PM", "06_00PM", "04_00PM", "01_00PM"):
            url = f"https://ak-static.cms.nba.com/referee/injury/Injury-Report_{day}_{hhmm}.pdf"
            candidates.append((datetime.strptime(f"{day} {hhmm}", "%Y-%m-%d %I_%M%p"), url))
    teams: dict[str, dict] = {}
    done_days = set()
    for t, url in candidates:
        if t.date() in done_days:
            continue
        pdf = _fetch(url)
        if pdf is None:
            continue
        done_days.add(t.date())
        try:
            rows = parse_report(pdf)
        except Exception as e:
            print(f"  failed to parse {url}: {e}")
            continue
        print(f"{t:%Y-%m-%d %I:%M %p}: {len(rows)} players")
        for team in sorted({row["team"] for row in rows}):
            if team in teams:
                continue
            team_rows = [row for row in rows if row["team"] == team]
            first_game = min(row["game_date"] or "" for row in team_rows)
            teams[team] = {
                "report": t.strftime("%Y-%m-%d %I:%M %p ET"), "game_date": first_game,
                "players": [{k: row[k] for k in ("name", "status", "reason")}
                            for row in team_rows if (row["game_date"] or "") == first_game],
            }
    if not teams:
        print("No reports parsed; keeping previous file.")
        return
    OUT_PATH.write_text(json.dumps({"generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
                                    "teams": dict(sorted(teams.items()))}, indent=1), encoding="utf-8")
    print(f"Wrote {OUT_PATH}: {len(teams)} teams")


if __name__ == "__main__":
    main()
