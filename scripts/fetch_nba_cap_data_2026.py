from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import time
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "data" / "nba_cap_data_2026_27.json"
DEFAULT_JS_OUTPUT = ROOT / "data" / "nba_cap_data_2026_27.js"
SEASON_YEAR = 2026
USER_AGENT = "Mozilla/5.0"

TEAMS = {
    "ATL": ("Atlanta Hawks", "atlanta-hawks"),
    "BOS": ("Boston Celtics", "boston-celtics"),
    "BKN": ("Brooklyn Nets", "brooklyn-nets"),
    "CHA": ("Charlotte Hornets", "charlotte-hornets"),
    "CHI": ("Chicago Bulls", "chicago-bulls"),
    "CLE": ("Cleveland Cavaliers", "cleveland-cavaliers"),
    "DAL": ("Dallas Mavericks", "dallas-mavericks"),
    "DEN": ("Denver Nuggets", "denver-nuggets"),
    "DET": ("Detroit Pistons", "detroit-pistons"),
    "GSW": ("Golden State Warriors", "golden-state-warriors"),
    "HOU": ("Houston Rockets", "houston-rockets"),
    "IND": ("Indiana Pacers", "indiana-pacers"),
    "LAC": ("Los Angeles Clippers", "la-clippers"),
    "LAL": ("Los Angeles Lakers", "los-angeles-lakers"),
    "MEM": ("Memphis Grizzlies", "memphis-grizzlies"),
    "MIA": ("Miami Heat", "miami-heat"),
    "MIL": ("Milwaukee Bucks", "milwaukee-bucks"),
    "MIN": ("Minnesota Timberwolves", "minnesota-timberwolves"),
    "NOP": ("New Orleans Pelicans", "new-orleans-pelicans"),
    "NYK": ("New York Knicks", "new-york-knicks"),
    "OKC": ("Oklahoma City Thunder", "oklahoma-city-thunder"),
    "ORL": ("Orlando Magic", "orlando-magic"),
    "PHI": ("Philadelphia 76ers", "philadelphia-76ers"),
    "PHX": ("Phoenix Suns", "phoenix-suns"),
    "POR": ("Portland Trail Blazers", "portland-trail-blazers"),
    "SAC": ("Sacramento Kings", "sacramento-kings"),
    "SAS": ("San Antonio Spurs", "san-antonio-spurs"),
    "TOR": ("Toronto Raptors", "toronto-raptors"),
    "UTA": ("Utah Jazz", "utah-jazz"),
    "WAS": ("Washington Wizards", "washington-wizards"),
}


def scalar(value: Any) -> Any:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if hasattr(value, "item"):
        value = value.item()
    return value


def money(value: Any) -> int | None:
    value = scalar(value)
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"-", "—", "nan"}:
        return None
    negative = "-" in text or (text.startswith("(") and text.endswith(")"))
    digits = re.sub(r"[^0-9.]", "", text)
    if not digits:
        return None
    amount = int(round(float(digits)))
    return -amount if negative else amount


_NAME_SUFFIX_RE = re.compile(r"\s+\(([A-Z][A-Z/-]*),\s*(\d+(?:\.\d+)?)\)$")


def clean_player_name(value: Any) -> tuple[str, str | None, int | None]:
    """
    Returns (name, position, age). Some team pages (e.g. a condensed
    "RK | Player | Cap Hit" table layout Spotrac serves for a rotating subset
    of teams) have no separate Pos/Age columns at all -- that info only
    exists as a "(C, 32)" suffix on the player name. Capture it here before
    stripping it, so callers can fall back to it when the dedicated columns
    are missing.
    """
    text = re.sub(r"\s+", " ", str(scalar(value) or "")).strip()
    text = re.sub(r"\s+WAIVED$", "", text, flags=re.IGNORECASE)
    suffix_pos, suffix_age = None, None
    m = _NAME_SUFFIX_RE.search(text)
    if m:
        suffix_pos, suffix_age = m.group(1), int(float(m.group(2)))
        text = text[: m.start()].rstrip()
    words = text.split()
    lowered = [word.lower() for word in words]
    for prefix_length in range(len(words) // 2, 0, -1):
        if lowered[:prefix_length] == lowered[-prefix_length:]:
            text = " ".join(words[prefix_length:])
            break
    return text, suffix_pos, suffix_age


def matching_column(record: dict, prefix: str) -> Any:
    for key, value in record.items():
        if str(key).lower().startswith(prefix.lower()):
            return value
    return None


def dataframe(table) -> pd.DataFrame:
    return pd.read_html(StringIO(str(table)))[0]


def rows_from_contract_table(table, category: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for raw in dataframe(table).to_dict(orient="records"):
        player_value = matching_column(raw, "Player")
        if player_value is None:
            player_value = next(iter(raw.values()))
        player, suffix_pos, suffix_age = clean_player_name(player_value)
        if not player or player.lower() == "nan":
            continue
        values = [str(scalar(value) or "") for value in raw.values()]
        rights = next(
            (value for value in values if any(token in value for token in ("Bird", "Two-Way", "Round Pick"))),
            None,
        )
        position = scalar(matching_column(raw, "Pos")) or suffix_pos
        age = scalar(matching_column(raw, "Age")) or suffix_age
        row = {
            "player": player,
            "category": category,
            "position": position,
            "age": age,
            "contract_type": scalar(matching_column(raw, "Type")),
            "cap_hit": money(matching_column(raw, "Cap Hit")),
            "base_salary": money(matching_column(raw, "Base Salary")),
            "likely_incentives": money(matching_column(raw, "Incentives  Likely")),
            "unlikely_incentives": money(matching_column(raw, "Incentives  Unlikely")),
            "trade_bonus_proration": money(matching_column(raw, "Trade Bonus")),
            "guaranteed": money(matching_column(raw, "Guaranteed")),
            "qualifying_offer": money(matching_column(raw, "Qualifying Offer")),
            "rights": rights,
        }
        result.append(row)
    return result


def exception_rows(table) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in dataframe(table).to_dict(orient="records"):
        rows.append(
            {
                "type": scalar(matching_column(raw, "Type")),
                "reason": scalar(matching_column(raw, "Reason")),
                "used_on": scalar(matching_column(raw, "Used On")),
                "expires": scalar(matching_column(raw, "Expires")),
                "original": money(matching_column(raw, "Original")),
                "available": money(matching_column(raw, "Available")),
            }
        )
    return rows


def total_rows(table) -> dict[str, Any]:
    totals: dict[str, Any] = {}
    labels = {
        "Cap Maximum": "salary_cap",
        "Active Roster": "active_roster",
        "Dead Cap": "dead_money",
        "Cap Hold": "cap_holds",
        "Total Allocations": "total_allocations",
        "Space": "cap_space",
        "1st Apron Maximum": "first_apron",
        "2nd Apron Maximum": "second_apron",
    }
    section = "cap"
    for tr in table.select("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in tr.select("th,td")]
        if not cells:
            continue
        joined = " ".join(cells)
        if "1st Apron Summary" in joined:
            section = "first"
            continue
        if "1st Apron Maximum" in joined and "hard-capped" in joined:
            totals["first_apron_hard_cap"] = True
        if "2nd Apron Maximum" in joined and "hard-capped" in joined:
            totals["second_apron_hard_cap"] = True
        if "2nd Apron Summary" in joined:
            section = "second"
            continue
        for label, key in labels.items():
            matches = cells[0].startswith(label) if "Apron Maximum" in label else cells[0] == label
            if not matches:
                continue
            if label == "Total Allocations" and section != "cap":
                key = f"{section}_apron_allocations"
            elif label == "Space" and section != "cap":
                key = f"{section}_apron_space"
            amount = next((money(cell) for cell in cells[1:] if money(cell) is not None), None)
            if amount is not None:
                totals[key] = amount
            break
    return totals


_SIDEBAR_SALARY_RE = re.compile(r'\$([\d.]+)\s*million', re.I)
_SIDEBAR_YEARS_RE  = re.compile(r'(\d+)\s*year', re.I)
_SIDEBAR_POS_RE    = re.compile(r',\s*([A-Z]{1,3})$')

def _parse_sidebar_signings(soup: BeautifulSoup, abbr: str, existing_names: set[str]) -> list[dict]:
    """
    Parse the 'recent transactions' sidebar on each team cap page.
    Spotrac puts freshly agreed deals here before they appear in the main cap table.
    Returns rows in the same format as rows_from_contract_table.
    """
    added = []
    seen_sidebar: set[str] = set()
    for div in soup.select('div.d-flex.align-items-center.border-bottom'):
        a = div.find('a')
        p = div.find('p')
        if not a or not p:
            continue
        action = p.get_text(' ', strip=True)
        # Only process signings directed AT this team
        if not re.search(r'(Agreed to|Signed)', action, re.I):
            continue
        if f'({abbr})' not in action and abbr not in action:
            continue
        raw_name = a.get_text(', ', strip=True)  # "James Harden, SG"
        pos_m = _SIDEBAR_POS_RE.search(raw_name)
        pos = pos_m.group(1) if pos_m else None
        # Skip coaches and non-player entries
        if pos and pos in ('COA', 'HC', 'AC', 'GM', 'AGM'):
            continue
        player = _SIDEBAR_POS_RE.sub('', raw_name).strip()
        if not player or player in existing_names or player in seen_sidebar:
            continue
        seen_sidebar.add(player)
        # Parse salary
        sal_m   = _SIDEBAR_SALARY_RE.search(action)
        yrs_m   = _SIDEBAR_YEARS_RE.search(action)
        total_m = float(sal_m.group(1)) if sal_m else None
        years   = int(yrs_m.group(1)) if yrs_m else 1
        cap_hit = int(total_m / years * 1_000_000) if total_m else None
        added.append({
            'player':               player,
            'category':             'pending_transaction',
            'position':             pos,
            'age':                  None,
            'contract_type':        'UFA',
            'cap_hit':              cap_hit,
            'base_salary':          cap_hit,
            'likely_incentives':    None,
            'unlikely_incentives':  None,
            'trade_bonus_proration':None,
            'guaranteed':           cap_hit,
            'qualifying_offer':     None,
            'rights':               None,
        })
    return added


def scrape_team(session: requests.Session, abbreviation: str, name: str, slug: str) -> dict[str, Any]:
    url = f"https://www.spotrac.com/nba/{slug}/cap/_/year/{SEASON_YEAR}"
    response = session.get(url, timeout=45)
    if response.status_code == 403:
        time.sleep(2.0)
        response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=45)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    sections: dict[str, Any] = {}
    for table in soup.find_all("table"):
        heading = table.find_previous(["h2", "h3", "h4", "h5"])
        title = heading.get_text(" ", strip=True) if heading else ""
        if "Pending Transactions" in title:
            sections["pending_transactions"] = rows_from_contract_table(table, "pending_transaction")
        elif "Active Roster" in title:
            sections["active_roster"] = rows_from_contract_table(table, "active_roster")
        elif "Cap Hold" in title:
            sections["cap_holds"] = rows_from_contract_table(table, "cap_hold")
        elif "Dead Money" in title or "Dead Cap" in title or "Waiver" in title:
            sections["dead_money"] = rows_from_contract_table(table, "dead_money")
        elif "Retained" in title and "Cap" in title:
            sections["retained_salary"] = rows_from_contract_table(table, "retained_salary")
        elif title.endswith("Exceptions"):
            sections["exceptions"] = exception_rows(table)
        elif "Cap Totals" in title:
            sections["totals"] = total_rows(table)

    if not sections.get("active_roster"):
        raise RuntimeError(f"{abbreviation}: active roster table not found")
    if not sections.get("totals"):
        raise RuntimeError(f"{abbreviation}: cap totals table not found")
    # Supplement pending_transactions with any signings only in the sidebar
    existing_names = {
        r['player']
        for cat in ('active_roster', 'pending_transactions')
        for r in sections.get(cat, [])
    }
    sidebar = _parse_sidebar_signings(soup, abbreviation, existing_names)
    if sidebar:
        sections.setdefault('pending_transactions', []).extend(sidebar)
        print(f"  {abbreviation}: +{len(sidebar)} sidebar signing(s): {[r['player'] for r in sidebar]}", flush=True)

    active_row_total = sum(row.get("cap_hit") or 0 for row in sections["active_roster"])
    published_active_total = sections["totals"].get("active_roster")
    sections["reconciliation"] = {
        "active_roster_row_total": active_row_total,
        "published_active_roster_total": published_active_total,
        "published_less_rows": (
            published_active_total - active_row_total if published_active_total is not None else None
        ),
    }
    return {
        "abbreviation": abbreviation,
        "name": name,
        "source_url": url,
        "source_sha256": hashlib.sha256(response.content).hexdigest(),
        **sections,
    }


def reconcile_cross_team_signings(teams: dict[str, Any]) -> None:
    """
    Spotrac's per-team cap pages update independently, so a player who just
    signed elsewhere can still show up as a free-agent cap hold on his old
    team for a day or two. We already catch new signings as
    'pending_transaction' rows on the *new* team (main table + sidebar), so
    use that to drop the stale cap hold on the old team.
    """
    signed_with: dict[str, set[str]] = {}
    for abbr, data in teams.items():
        for row in data.get("pending_transactions", []):
            if row.get("category") == "pending_transaction":
                signed_with.setdefault(row["player"], set()).add(abbr)

    # A player reported to more than one team at once is a rumor conflict,
    # not a resolved signing -- leave those cap holds alone.
    resolved = {name: next(iter(abbrs)) for name, abbrs in signed_with.items() if len(abbrs) == 1}

    for abbr, data in teams.items():
        holds = data.get("cap_holds")
        if not holds:
            continue
        keep, dropped = [], []
        for row in holds:
            new_team = resolved.get(row["player"])
            (dropped if new_team and new_team != abbr else keep).append(row)
        if dropped:
            data["cap_holds"] = keep
            print(
                f"  {abbr}: removed {len(dropped)} cap hold(s) for players signed elsewhere: "
                f"{[(r['player'], resolved[r['player']]) for r in dropped]}",
                flush=True,
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--js-output", type=Path, default=DEFAULT_JS_OUTPUT)
    parser.add_argument("--delay", type=float, default=0.25)
    parser.add_argument("--teams", nargs="*", choices=sorted(TEAMS))
    args = parser.parse_args()

    selected = args.teams or list(TEAMS)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})
    teams: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for index, abbreviation in enumerate(selected):
        name, slug = TEAMS[abbreviation]
        try:
            teams[abbreviation] = scrape_team(session, abbreviation, name, slug)
            roster_count = len(teams[abbreviation].get("active_roster", []))
            hold_count = len(teams[abbreviation].get("cap_holds", []))
            print(f"{abbreviation}: {roster_count} active, {hold_count} holds", flush=True)
        except Exception as exc:
            errors[abbreviation] = f"{type(exc).__name__}: {exc}"
            print(f"{abbreviation}: ERROR {errors[abbreviation]}", flush=True)
        if index + 1 < len(selected):
            time.sleep(max(0.0, args.delay))

    reconcile_cross_team_signings(teams)

    snapshot = {
        "schema_version": 1,
        "season": "2026-27",
        "season_year": SEASON_YEAR,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": "Spotrac team cap tables",
        "team_count": len(teams),
        "errors": errors,
        "teams": teams,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temp = args.output.with_suffix(args.output.suffix + ".tmp")
    temp.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temp.replace(args.output)
    args.js_output.parent.mkdir(parents=True, exist_ok=True)
    js_temp = args.js_output.with_suffix(args.js_output.suffix + ".tmp")
    js_temp.write_text(
        "window.NBA_CAP_SNAPSHOT = " + json.dumps(snapshot, ensure_ascii=False) + ";\n",
        encoding="utf-8",
    )
    js_temp.replace(args.js_output)
    print(json.dumps({"output": str(args.output), "teams": len(teams), "errors": errors}, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
