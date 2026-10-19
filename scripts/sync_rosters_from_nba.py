"""Reconcile the cap snapshot's rosters against official NBA.com team rosters.

Spotrac (the cap snapshot's source, scripts/fetch_nba_cap_data_2026.py) has
returned 403 to scripts since Oct 2026, so its rosters freeze at the last
successful scrape. NBA.com's public team pages embed the official current
roster; this makes the snapshot's rosters (active_roster + pending_transactions,
what the depth-chart and projection pages treat as the roster) match it:

- a rostered player listed under another team moves there, keeping his row
  (contract figures belong to the player, not the team);
- a player rostered by NBA.com but absent everywhere is added to
  active_roster with no cap figures (contract_type "NBA.com roster");
- a player no team rosters is dropped from active_roster / pending;
- cap holds for anyone now rostered are dropped.

Dead money, exceptions and totals are left as scraped. Writes the same
data/nba_cap_data_2026_27.{json,js} files, and prints every change.
"""
from __future__ import annotations

import argparse
import json
import re
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
JSON_PATH = ROOT / "data" / "nba_cap_data_2026_27.json"
JS_PATH = ROOT / "data" / "nba_cap_data_2026_27.js"
NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', re.S)
ROSTER_CATEGORIES = ("active_roster", "pending_transactions")

TEAM_IDS = {
    "ATL": 1610612737, "BOS": 1610612738, "BKN": 1610612751, "CHA": 1610612766,
    "CHI": 1610612741, "CLE": 1610612739, "DAL": 1610612742, "DEN": 1610612743,
    "DET": 1610612765, "GSW": 1610612744, "HOU": 1610612745, "IND": 1610612754,
    "LAC": 1610612746, "LAL": 1610612747, "MEM": 1610612763, "MIA": 1610612748,
    "MIL": 1610612749, "MIN": 1610612750, "NOP": 1610612740, "NYK": 1610612752,
    "OKC": 1610612760, "ORL": 1610612753, "PHI": 1610612755, "PHX": 1610612756,
    "POR": 1610612757, "SAC": 1610612758, "SAS": 1610612759, "TOR": 1610612761,
    "UTA": 1610612762, "WAS": 1610612764,
}
# NBA.com lists generic positions; map to the depth chart's specific columns.
POSITION_MAP = {"G": "SG", "F": "SF", "C": "C", "G-F": "SG", "F-G": "SF", "F-C": "PF", "C-F": "C"}


def norm_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode().lower()
    text = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", "", text)
    return re.sub(r"[^a-z]", "", text)


# NBA.com name -> spelling in the Spotrac rows, where neither first name is a
# prefix of the other (prefix nicknames like Nic/Nicolas match automatically).
NAME_ALIASES = {"Bones Hyland": "Nah'Shon Hyland"}


def _split(name: str) -> tuple[str, str]:
    parts = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower().replace(",", " ").split()
    parts = [p for p in parts if re.sub(r"[^a-z]", "", p) not in {"jr", "sr", "ii", "iii", "iv", "v"}]
    return (re.sub(r"[^a-z]", "", parts[0]) if parts else "",
            re.sub(r"[^a-z]", "", "".join(parts[1:])))


def match_existing(name: str, existing: dict[str, Any]) -> str | None:
    """Key of the existing row for an NBA.com name: exact, alias, then nickname prefix."""
    key = norm_name(NAME_ALIASES.get(name, name))
    if key in existing:
        return key
    first, last = _split(name)
    hits = [k for k, (_, _, row) in existing.items()
            if _split(row["player"])[1] == last and first and _split(row["player"])[0]
            and (_split(row["player"])[0].startswith(first) or first.startswith(_split(row["player"])[0]))]
    return hits[0] if len(hits) == 1 else None


def fetch_roster(team_id: int) -> list[dict[str, Any]]:
    for attempt in range(4):
        try:
            r = requests.get(f"https://www.nba.com/team/{team_id}",
                             headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            r.raise_for_status()
            props = json.loads(NEXT_DATA_RE.search(r.text).group(1))["props"]["pageProps"]
            roster = props["team"]["roster"]
            if roster:
                return roster
        except Exception:
            if attempt == 3:
                raise
        time.sleep(1.5 * (attempt + 1))
    return []


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="print changes without writing")
    args = parser.parse_args()

    snapshot = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    teams = snapshot["teams"]

    official: dict[str, list[dict[str, Any]]] = {}
    for abbr, team_id in TEAM_IDS.items():
        official[abbr] = fetch_roster(team_id)
        time.sleep(0.5)
    counts = {abbr: len(r) for abbr, r in official.items()}
    if min(counts.values()) < 10:
        raise SystemExit(f"Implausible NBA.com roster sizes, not writing: {counts}")


    # Every existing row by player, so a mover keeps his contract row.
    existing: dict[str, tuple[str, str, dict[str, Any]]] = {}
    for abbr, team in teams.items():
        for cat in ROSTER_CATEGORIES + ("cap_holds",):
            for row in team.get(cat) or []:
                key = norm_name(row["player"])
                # Prefer a signed (active_roster) row over a pending row or hold.
                if key not in existing or (cat == "active_roster" and existing[key][1] != "active_roster"):
                    existing[key] = (abbr, cat, row)

    # Where each player is officially rostered, keyed like the existing rows.
    official_team: dict[str, str] = {}
    for abbr, roster in official.items():
        for p in roster:
            official_team[match_existing(p["PLAYER"], existing) or norm_name(p["PLAYER"])] = abbr

    changes: list[str] = []
    new_rosters: dict[str, dict[str, list[dict[str, Any]]]] = {abbr: {c: [] for c in ROSTER_CATEGORIES} for abbr in teams}
    for abbr, roster in official.items():
        for p in roster:
            key = match_existing(p["PLAYER"], existing)
            if key is not None:
                src_abbr, src_cat, row = existing[key]
                row = dict(row)
                cat = src_cat if src_cat in ROSTER_CATEGORIES else "active_roster"
                if src_abbr != abbr:
                    changes.append(f"{abbr}: + {row['player']} (moved from {src_abbr} {src_cat})")
                elif src_cat == "cap_holds":
                    changes.append(f"{abbr}: + {row['player']} (was a cap hold; now rostered)")
            else:
                pos = POSITION_MAP.get(str(p.get("POSITION") or "").strip())
                row = {
                    "player": p["PLAYER"].replace(", Jr.", " Jr."),
                    "category": "active_roster", "position": pos,
                    "age": p.get("AGE"), "contract_type": "NBA.com roster",
                    "cap_hit": None, "base_salary": None, "likely_incentives": None,
                    "unlikely_incentives": None, "trade_bonus_proration": None,
                    "guaranteed": None, "qualifying_offer": None, "rights": None,
                }
                cat = "active_roster"
                changes.append(f"{abbr}: + {row['player']} (new; {p.get('HOW_ACQUIRED') or 'no acquisition note'})")
            row["category"] = "pending_transaction" if cat == "pending_transactions" else "active_roster"
            new_rosters[abbr][cat].append(row)

    for abbr, team in teams.items():
        for cat in ROSTER_CATEGORIES:
            for row in team.get(cat) or []:
                key = norm_name(row["player"])
                if key not in official_team:
                    changes.append(f"{abbr}: - {row['player']} (not on any NBA.com roster)")
            team[cat] = new_rosters[abbr][cat]
        holds = team.get("cap_holds") or []
        kept = [h for h in holds if norm_name(h["player"]) not in official_team]
        for h in holds:
            if h not in kept:
                changes.append(f"{abbr}: dropped cap hold {h['player']} (rostered by {official_team[norm_name(h['player'])]})")
        team["cap_holds"] = kept
        team["roster_source"] = "NBA.com official roster (synced)"

    snapshot["roster_synced_at"] = datetime.now(timezone.utc).isoformat()
    snapshot["roster_source"] = "NBA.com team rosters; contracts from last Spotrac scrape"
    for line in sorted(changes):
        print(line)
    print(f"{len(changes)} changes; roster sizes {sorted(counts.items(), key=lambda kv: kv[1])[:3]} ... "
          f"{sorted(counts.items(), key=lambda kv: kv[1])[-3:]}")
    if args.dry_run:
        return
    JSON_PATH.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    JS_PATH.write_text("window.NBA_CAP_SNAPSHOT = " + json.dumps(snapshot, ensure_ascii=False) + ";\n",
                       encoding="utf-8")


if __name__ == "__main__":
    main()
