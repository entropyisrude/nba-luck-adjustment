"""Generate season-to-date totals report for adjusted plus-minus and on/off."""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

import requests

DATA_DIR = Path("data")
ONOFF_PATH = DATA_DIR / "adjusted_onoff.csv"
OUTPUT_DATA_PATH = DATA_DIR / "onoff_report.html"
OUTPUT_SITE_PATH = Path("onoff.html")

PBP_BASE = "https://api.pbpstats.com"
PBP_TIMEOUT = 10

TEAM_ID_TO_ABBR = {
    "1610612737": "ATL",
    "1610612738": "BOS",
    "1610612751": "BKN",
    "1610612766": "CHA",
    "1610612741": "CHI",
    "1610612739": "CLE",
    "1610612742": "DAL",
    "1610612743": "DEN",
    "1610612765": "DET",
    "1610612744": "GSW",
    "1610612745": "HOU",
    "1610612754": "IND",
    "1610612746": "LAC",
    "1610612747": "LAL",
    "1610612763": "MEM",
    "1610612748": "MIA",
    "1610612749": "MIL",
    "1610612750": "MIN",
    "1610612740": "NOP",
    "1610612752": "NYK",
    "1610612760": "OKC",
    "1610612753": "ORL",
    "1610612755": "PHI",
    "1610612756": "PHX",
    "1610612757": "POR",
    "1610612758": "SAC",
    "1610612759": "SAS",
    "1610612761": "TOR",
    "1610612762": "UTA",
    "1610612764": "WAS",
}


def _f(v: object) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _season_from_date(date_str: str) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d")
    start = d.year if d.month >= 7 else d.year - 1
    return f"{start}-{(start + 1) % 100:02d}"


def _load_team_player_totals() -> tuple[list[dict], str, int, list[str]]:
    if not ONOFF_PATH.exists():
        raise FileNotFoundError(f"Missing {ONOFF_PATH}")

    rows: list[dict] = []
    latest_date = ""
    game_ids: set[str] = set()
    team_game_minutes: dict[tuple[str, str, str], float] = {}

    with ONOFF_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
            d = str(r["date"])
            if d > latest_date:
                latest_date = d
            gid = str(r["game_id"])
            tid = str(r["team_id"])
            game_ids.add(gid)
            key = (d, gid, tid)
            # summed player minutes / 5 gives team minutes for that game.
            team_game_minutes[key] = team_game_minutes.get(key, 0.0) + _f(r["minutes_on"]) / 5.0

    agg: dict[tuple[str, str, str], dict] = {}
    for r in rows:
        season = _season_from_date(str(r["date"]))
        team_id = str(r["team_id"])
        player_id = str(r["player_id"])
        key = (season, team_id, player_id)
        game_id = str(r["game_id"])
        date = str(r["date"])
        minutes_on = _f(r["minutes_on"])
        team_minutes = team_game_minutes.get((date, game_id, team_id), 0.0)
        minutes_off = max(0.0, team_minutes - minutes_on)

        if key not in agg:
            agg[key] = {
                "player_id": player_id,
                "player_name": str(r["player_name"]),
                "season": season,
                "team_id": team_id,
                "games_set": set(),
                "minutes_on_total": 0.0,
                "minutes_off_total": 0.0,
                "on_diff_total": 0.0,
                "off_diff_total": 0.0,
                "on_diff_adj_total": 0.0,
                "off_diff_adj_total": 0.0,
                "on_pts_for_adj_total": 0.0,
                "on_pts_against_adj_total": 0.0,
                "off_pts_for_adj_total": 0.0,
                "off_pts_against_adj_total": 0.0,
            }

        a = agg[key]
        a["games_set"].add(game_id)
        a["minutes_on_total"] += minutes_on
        a["minutes_off_total"] += minutes_off
        a["on_diff_total"] += _f(r["on_diff"])
        a["off_diff_total"] += _f(r["off_diff"])
        a["on_diff_adj_total"] += _f(r["on_diff_adj"])
        a["off_diff_adj_total"] += _f(r["off_diff_adj"])
        a["on_pts_for_adj_total"] += _f(r["on_pts_for_adj"])
        a["on_pts_against_adj_total"] += _f(r["on_pts_against_adj"])
        a["off_pts_for_adj_total"] += _f(r["off_pts_for_adj"])
        a["off_pts_against_adj_total"] += _f(r["off_pts_against_adj"])

    out: list[dict] = []
    team_ids = sorted({k[1] for k in agg.keys()}, key=lambda x: TEAM_ID_TO_ABBR.get(x, x))
    for a in agg.values():
        out.append(
            {
                "player_id": a["player_id"],
                "player_name": a["player_name"],
                "season": a["season"],
                "team_id": a["team_id"],
                "team_abbr": TEAM_ID_TO_ABBR.get(a["team_id"], a["team_id"]),
                "games": len(a["games_set"]),
                "minutes_total": a["minutes_on_total"],
                "minutes_off_total": a["minutes_off_total"],
                "on_diff_total": a["on_diff_total"],
                "off_diff_total": a["off_diff_total"],
                "on_diff_adj_total": a["on_diff_adj_total"],
                "off_diff_adj_total": a["off_diff_adj_total"],
                "on_pts_for_adj_total": a["on_pts_for_adj_total"],
                "on_pts_against_adj_total": a["on_pts_against_adj_total"],
                "off_pts_for_adj_total": a["off_pts_for_adj_total"],
                "off_pts_against_adj_total": a["off_pts_against_adj_total"],
            }
        )

    return out, latest_date, len(game_ids), team_ids


def _fetch_json(url: str, params: dict) -> dict | None:
    try:
        r = requests.get(url, params=params, timeout=PBP_TIMEOUT)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def _build_pbp_maps(season_to_team_ids: dict[str, list[str]]) -> dict[tuple[str, str, str], dict]:
    """
    Returns map keyed by (season, team_id, player_id) with possession-based raw PM/100 and OnOff/100.
    """
    out: dict[tuple[str, str, str], dict] = {}

    for season, team_ids in season_to_team_ids.items():
        all_players_payload = _fetch_json(
            f"{PBP_BASE}/get-totals/nba",
            {"Season": season, "SeasonType": "Regular Season", "Type": "Player"},
        )
        all_teams_payload = _fetch_json(
            f"{PBP_BASE}/get-totals/nba",
            {"Season": season, "SeasonType": "Regular Season", "Type": "Team"},
        )
        if not all_players_payload or not all_teams_payload:
            continue

        players_by_team: dict[str, list[dict]] = {tid: [] for tid in team_ids}
        for pr in all_players_payload.get("multi_row_table_data", []):
            tid = str(pr.get("TeamId"))
            if tid in players_by_team:
                players_by_team[tid].append(pr)

        team_totals: dict[str, dict] = {}
        for tr in all_teams_payload.get("multi_row_table_data", []):
            tid = str(tr.get("TeamId"))
            if tid in team_ids:
                team_totals[tid] = tr

        for team_id in team_ids:
            team_payload = team_totals.get(team_id)
            players = players_by_team.get(team_id, [])
            if not team_payload or not players:
                continue

            team_pm = _f(team_payload.get("PlusMinus"))
            team_off_poss = _f(team_payload.get("OffPoss"))
            team_def_poss = _f(team_payload.get("DefPoss"))
            team_opp_pts = _f(team_payload.get("OpponentPoints"))
            team_ortg = _f(team_payload.get("OnOffRtg"))

            if team_off_poss <= 0 or team_def_poss <= 0:
                continue

            team_drtg = (team_opp_pts / team_def_poss) * 100.0
            team_net = team_ortg - team_drtg

            for pr in players:
                player_id = str(pr.get("EntityId"))
                player_pm = _f(pr.get("PlusMinus"))
                on_off_poss = _f(pr.get("OffPoss"))
                on_def_poss = _f(pr.get("DefPoss"))
                on_opp_pts = _f(pr.get("OpponentPoints"))
                on_ortg = _f(pr.get("OnOffRtg"))

                if on_off_poss <= 0 or on_def_poss <= 0:
                    continue

                on_drtg = (on_opp_pts / on_def_poss) * 100.0
                on_net = on_ortg - on_drtg

                off_pm = team_pm - player_pm
                off_off_poss = team_off_poss - on_off_poss
                off_def_poss = team_def_poss - on_def_poss
                off_opp_pts = team_opp_pts - on_opp_pts

                if off_off_poss > 0 and off_def_poss > 0:
                    off_team_pts = off_opp_pts + off_pm
                    off_ortg = (off_team_pts / off_off_poss) * 100.0
                    off_drtg = (off_opp_pts / off_def_poss) * 100.0
                    off_net = off_ortg - off_drtg
                    onoff = on_net - off_net
                    off_poss = (off_off_poss + off_def_poss) / 2.0
                else:
                    off_net = team_net
                    onoff = on_net - off_net
                    off_poss = 0.0

                out[(season, team_id, player_id)] = {
                    "pm_actual_100": on_net,
                    "onoff_actual_100": onoff,
                    "on_poss": (on_off_poss + on_def_poss) / 2.0,
                    "off_poss": off_poss,
                    "source": "pbpstats",
                }

    return out


def _finalize_records(raw_rows: list[dict], pbp_map: dict[tuple[str, str, str], dict]) -> list[dict]:
    records: list[dict] = []
    for r in raw_rows:
        season = str(r["season"])
        team_id = r["team_id"]
        player_id = r["player_id"]
        on_min = _f(r["minutes_total"])
        off_min = _f(r["minutes_off_total"])

        pbp = pbp_map.get((season, team_id, player_id))
        if pbp:
            pm_actual_100 = _f(pbp["pm_actual_100"])
            onoff_actual_100 = _f(pbp["onoff_actual_100"])
            on_poss = _f(pbp["on_poss"])
            off_poss = _f(pbp["off_poss"])
            source = "pbpstats"
        else:
            pm_actual_100 = (_f(r["on_diff_total"]) * 48.0 / on_min) if on_min > 0 else 0.0
            if off_min > 0:
                off_actual_100 = _f(r["off_diff_total"]) * 48.0 / off_min
                onoff_actual_100 = pm_actual_100 - off_actual_100
            else:
                onoff_actual_100 = 0.0
            on_poss = 0.0
            off_poss = 0.0
            source = "minutes"

        if on_poss > 0:
            pm_adj_100 = _f(r["on_diff_adj_total"]) * 100.0 / on_poss
        else:
            pm_adj_100 = (_f(r["on_diff_adj_total"]) * 48.0 / on_min) if on_min > 0 else 0.0

        if off_poss > 0:
            off_adj_100 = _f(r["off_diff_adj_total"]) * 100.0 / off_poss
            onoff_adj_100 = pm_adj_100 - off_adj_100
        else:
            if off_min > 0:
                off_adj_100 = _f(r["off_diff_adj_total"]) * 48.0 / off_min
                onoff_adj_100 = pm_adj_100 - off_adj_100
            else:
                onoff_adj_100 = 0.0

        pm_delta_100 = pm_adj_100 - pm_actual_100
        onoff_delta_100 = onoff_adj_100 - onoff_actual_100

        # Break adjusted on-off into offensive and defensive components.
        # Use on_poss from pbpstats for on-court normalization (it correctly
        # reflects only games the player appeared in). For off-court, pbpstats
        # off_poss is inflated — it counts all team possessions without the
        # player, including games where they were DNP. Instead, estimate
        # off_poss by applying the same possessions-per-minute rate to off_min.
        on_for_adj = _f(r["on_pts_for_adj_total"])
        on_against_adj = _f(r["on_pts_against_adj_total"])
        off_for_adj = _f(r["off_pts_for_adj_total"])
        off_against_adj = _f(r["off_pts_against_adj_total"])

        if on_poss > 0 and on_min > 0:
            poss_per_min = on_poss / on_min
            on_ortg_adj = on_for_adj * 100.0 / on_poss
            on_drtg_adj = on_against_adj * 100.0 / on_poss
            off_poss_est = off_min * poss_per_min if off_min > 0 else 0.0
        elif on_min > 0:
            poss_per_min = 100.0 / 48.0  # fallback: ~2.08 poss/min
            on_ortg_adj = on_for_adj * 100.0 / (on_min * poss_per_min)
            on_drtg_adj = on_against_adj * 100.0 / (on_min * poss_per_min)
            off_poss_est = off_min * poss_per_min if off_min > 0 else 0.0
        else:
            on_ortg_adj = on_drtg_adj = 0.0
            off_poss_est = 0.0

        if off_poss_est > 0:
            off_ortg_adj = off_for_adj * 100.0 / off_poss_est
            off_drtg_adj = off_against_adj * 100.0 / off_poss_est
        else:
            off_ortg_adj = off_drtg_adj = 0.0

        # Offensive: team adj ORtg per 100 when ON minus when OFF (positive = helps offense)
        onoff_adj_off_100 = on_ortg_adj - off_ortg_adj
        # Defensive: team adj DRtg per 100 when OFF minus when ON (positive = helps defense)
        onoff_adj_def_100 = off_drtg_adj - on_drtg_adj

        records.append(
            {
                "player_id": player_id,
                "player_name": r["player_name"],
                "season": season,
                "team_id": team_id,
                "team_abbr": r["team_abbr"],
                "games": int(_f(r["games"])),
                "minutes_total": on_min,
                "pm_actual_100": pm_actual_100,
                "pm_adj_100": pm_adj_100,
                "pm_delta_100": pm_delta_100,
                "onoff_actual_100": onoff_actual_100,
                "onoff_adj_100": onoff_adj_100,
                "onoff_adj_off_100": onoff_adj_off_100,
                "onoff_adj_def_100": onoff_adj_def_100,
                "onoff_delta_100": onoff_delta_100,
                "raw_source": source,
            }
        )
    return records


def generate_onoff_report() -> Path:
    raw_rows, latest_date, game_count, team_ids = _load_team_player_totals()
    latest_season = _season_from_date(latest_date)
    season_to_team_ids: dict[str, list[str]] = {}
    for r in raw_rows:
        s = str(r["season"])
        season_to_team_ids.setdefault(s, [])
        tid = str(r["team_id"])
        if tid not in season_to_team_ids[s]:
            season_to_team_ids[s].append(tid)
    pbp_map = _build_pbp_maps(season_to_team_ids)
    records = _finalize_records(raw_rows, pbp_map)

    team_values = sorted({r["team_id"] for r in records}, key=lambda x: TEAM_ID_TO_ABBR.get(x, x))
    season_values = sorted({r["season"] for r in records}, reverse=True)
    generated_ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    page_title = "3PT Luck Adjusted Plus Minus: Totals"

    html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{page_title}</title>
  <style>
    :root {{
      --bg: #f2f6fb;
      --card: #fff;
      --line: #d6e1ef;
      --ink: #192231;
      --muted: #5b6778;
      --good: #0f766e;
      --bad: #b91c1c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: \"Segoe UI\", Arial, sans-serif;
      color: var(--ink);
      background: linear-gradient(180deg, #eef5ff 0%, #f8fbff 30%, #f2f6fb 100%);
    }}
    .wrap {{ max-width: 1400px; margin: 0 auto; padding: 18px; }}
    .hero {{
      background: radial-gradient(circle at 20% 20%, #154f8b 0%, #0d2f53 45%, #081a2f 100%);
      color: #f8fbff;
      border-radius: 14px;
      padding: 18px 20px;
      border: 1px solid #254b72;
      margin-bottom: 14px;
      text-align: center;
    }}
    h1 {{ margin: 0; font-size: 28px; }}
    h2 {{ margin: 0 0 10px; font-size: 20px; }}
    .meta {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 8px; justify-content: center; }}
    .chip {{
      background: rgba(255,255,255,0.14);
      border: 1px solid rgba(255,255,255,0.22);
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
    }}
    .nav {{ margin-top: 10px; display: flex; gap: 12px; flex-wrap: wrap; justify-content: center; }}
    .nav a {{ color: #e5f6ff; text-decoration: underline; text-underline-offset: 3px; font-size: 13px; }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px;
      box-shadow: 0 3px 12px rgba(23, 38, 62, 0.06);
      margin-bottom: 14px;
    }}
    .controls {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 10px; }}
    .controls label {{ font-size: 12px; color: var(--muted); display: grid; gap: 4px; }}
    select, input {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 7px 9px;
      font-size: 13px;
      min-width: 120px;
      background: #fff;
      color: var(--ink);
    }}
    .table-wrap {{
      border: 1px solid var(--line);
      border-radius: 10px;
      overflow: auto;
      max-height: 620px;
      background: #fff;
    }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1100px; font-size: 12px; }}
    th, td {{
      border-bottom: 1px solid #edf2f9;
      padding: 7px 8px;
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child,
    th:nth-child(2), td:nth-child(2) {{ text-align: left; }}
    thead th {{
      position: sticky;
      top: 0;
      z-index: 2;
      background: #edf3fc;
      color: #123154;
    }}
    thead th.sortable {{ cursor: pointer; }}
    thead th.sortable:hover {{ background: #e4edf9; }}
    .pos {{ color: var(--good); font-weight: 600; }}
    .neg {{ color: var(--bad); font-weight: 600; }}
    .muted {{ color: var(--muted); }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <section class=\"hero\">
      <h1>{page_title}</h1>
      <div class=\"meta\">
        <span class=\"chip\">Data through {latest_date}</span>
        <span class=\"chip\">Games: {game_count:,}</span>
        <span class=\"chip\">Rows: {len(records):,}</span>
      </div>
      <div class=\"nav\">
        <a href=\"index.html\">Main 3PT Luck Page</a>
        <a href=\"onoff-daily.html\">3PT Luck Adjust Plus Minus: Games</a>
      </div>
    </section>

    <section class=\"card\">
      <h2>Team-by-Team Season Totals</h2>
      <div class=\"controls\">
        <label>Season
          <select id=\"season-filter\"></select>
        </label>
        <label>Team
          <select id=\"team-filter\"></select>
        </label>
        <label>Min games
          <input id=\"team-min-games\" type=\"number\" min=\"0\" step=\"1\" value=\"1\" />
        </label>
        <label>Min total minutes
          <input id=\"team-min-minutes\" type=\"number\" min=\"0\" step=\"1\" value=\"50\" />
        </label>
      </div>
      <div class=\"table-wrap\">
        <table id=\"team-table\">
          <thead>
            <tr>
              <th class=\"sortable\" data-key=\"player_name\" data-type=\"str\">Player</th>
              <th class=\"sortable\" data-key=\"team_abbr\" data-type=\"str\">Team</th>
              <th class=\"sortable\" data-key=\"games\" data-type=\"num\">G</th>
              <th class=\"sortable\" data-key=\"minutes_total\" data-type=\"num\">Min</th>
              <th class=\"sortable\" data-key=\"pm_actual_100\" data-type=\"num\">PM/100</th>
              <th class=\"sortable\" data-key=\"pm_adj_100\" data-type=\"num\">PM Adj/100</th>
              <th class=\"sortable\" data-key=\"pm_delta_100\" data-type=\"num\">PM Delta/100</th>
              <th class=\"sortable\" data-key=\"onoff_actual_100\" data-type=\"num\">OnOff/100</th>
              <th class=\"sortable\" data-key=\"onoff_adj_100\" data-type=\"num\" title=\"3PT-luck adjusted on-off per 100 possessions\">OnOff Adj/100</th>
              <th class=\"sortable\" data-key=\"onoff_adj_off_100\" data-type=\"num\" title=\"Offensive component: team 3PT-adjusted ORtg per 100 poss when ON minus when OFF. Positive = player improves team offense.\">OnOff Adj Off/100</th>
              <th class=\"sortable\" data-key=\"onoff_adj_def_100\" data-type=\"num\" title=\"Defensive component: team 3PT-adjusted DRtg per 100 poss when OFF minus when ON. Positive = player improves team defense.\">OnOff Adj Def/100</th>
              <th class=\"sortable\" data-key=\"onoff_delta_100\" data-type=\"num\">OnOff Delta/100</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section class=\"card\">
      <h2>Season Leaderboard (Sortable)</h2>
      <div class=\"controls\">
        <label>Min games
          <input id=\"lb-min-games\" type=\"number\" min=\"0\" step=\"1\" value=\"10\" />
        </label>
        <label>Min total minutes
          <input id=\"lb-min-minutes\" type=\"number\" min=\"0\" step=\"1\" value=\"200\" />
        </label>
      </div>
      <div class=\"table-wrap\">
        <table id=\"lb-table\">
          <thead>
            <tr>
              <th class=\"sortable\" data-key=\"player_name\" data-type=\"str\">Player</th>
              <th class=\"sortable\" data-key=\"team_abbr\" data-type=\"str\">Team</th>
              <th class=\"sortable\" data-key=\"games\" data-type=\"num\">G</th>
              <th class=\"sortable\" data-key=\"minutes_total\" data-type=\"num\">Min</th>
              <th class=\"sortable\" data-key=\"pm_actual_100\" data-type=\"num\">PM/100</th>
              <th class=\"sortable\" data-key=\"pm_adj_100\" data-type=\"num\">PM Adj/100</th>
              <th class=\"sortable\" data-key=\"pm_delta_100\" data-type=\"num\">PM Delta/100</th>
              <th class=\"sortable\" data-key=\"onoff_actual_100\" data-type=\"num\">OnOff/100</th>
              <th class=\"sortable\" data-key=\"onoff_adj_100\" data-type=\"num\" title=\"3PT-luck adjusted on-off per 100 possessions\">OnOff Adj/100</th>
              <th class=\"sortable\" data-key=\"onoff_adj_off_100\" data-type=\"num\" title=\"Offensive component: team 3PT-adjusted ORtg per 100 poss when ON minus when OFF. Positive = player improves team offense.\">OnOff Adj Off/100</th>
              <th class=\"sortable\" data-key=\"onoff_adj_def_100\" data-type=\"num\" title=\"Defensive component: team 3PT-adjusted DRtg per 100 poss when OFF minus when ON. Positive = player improves team defense.\">OnOff Adj Def/100</th>
              <th class=\"sortable\" data-key=\"onoff_delta_100\" data-type=\"num\">OnOff Delta/100</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <p class=\"muted\">Generated {generated_ts} | Source: `data/adjusted_onoff.csv` + pbpstats possession totals.</p>
  </div>
  <script>
    const ROWS = {json.dumps(records)};
    const TEAMS = {json.dumps(team_values)};
    const SEASONS = {json.dumps(season_values)};
    const TEAM_MAP = {json.dumps(TEAM_ID_TO_ABBR)};
    const LATEST_SEASON = {json.dumps(latest_season)};

    const fmt = (x, d=1) => (x === null || Number.isNaN(Number(x))) ? "" : Number(x).toFixed(d);
    const cls = (x) => (x > 0 ? "pos" : (x < 0 ? "neg" : ""));

    let teamSortKey = "pm_adj_100";
    let teamSortDir = "desc";
    let lbSortKey = "pm_adj_100";
    let lbSortDir = "desc";

    function rowHtml(r) {{
      return `<tr>
        <td>${{r.player_name}}</td>
        <td>${{r.team_abbr}}</td>
        <td>${{fmt(r.games,0)}}</td>
        <td>${{fmt(r.minutes_total,1)}}</td>
        <td class="${{cls(r.pm_actual_100)}}">${{fmt(r.pm_actual_100,1)}}</td>
        <td class="${{cls(r.pm_adj_100)}}">${{fmt(r.pm_adj_100,1)}}</td>
        <td class="${{cls(r.pm_delta_100)}}">${{fmt(r.pm_delta_100,1)}}</td>
        <td class="${{cls(r.onoff_actual_100)}}">${{fmt(r.onoff_actual_100,1)}}</td>
        <td class="${{cls(r.onoff_adj_100)}}">${{fmt(r.onoff_adj_100,1)}}</td>
        <td class="${{cls(r.onoff_adj_off_100)}}">${{fmt(r.onoff_adj_off_100,1)}}</td>
        <td class="${{cls(r.onoff_adj_def_100)}}">${{fmt(r.onoff_adj_def_100,1)}}</td>
        <td class="${{cls(r.onoff_delta_100)}}">${{fmt(r.onoff_delta_100,1)}}</td>
      </tr>`;
    }}

    function renderTeamTable() {{
      const season = document.getElementById("season-filter").value;
      const team = document.getElementById("team-filter").value;
      const minGames = Number(document.getElementById("team-min-games").value || 0);
      const minMin = Number(document.getElementById("team-min-minutes").value || 0);
      const tbody = document.querySelector("#team-table tbody");

      const rows = ROWS
        .filter(r => r.season === season)
        .filter(r => r.team_id === team)
        .filter(r => Number(r.games || 0) >= minGames)
        .filter(r => Number(r.minutes_total || 0) >= minMin)
        .slice()
        .sort((a,b) => {{
          const dir = teamSortDir === "asc" ? 1 : -1;
          if (teamSortKey === "player_name" || teamSortKey === "team_abbr") {{
            return dir * String(a[teamSortKey]).localeCompare(String(b[teamSortKey]));
          }}
          return dir * (Number(a[teamSortKey] || 0) - Number(b[teamSortKey] || 0));
        }});

      tbody.innerHTML = rows.map(rowHtml).join("");
    }}

    function renderLeaderboard() {{
      const season = document.getElementById("season-filter").value;
      const minGames = Number(document.getElementById("lb-min-games").value || 0);
      const minMin = Number(document.getElementById("lb-min-minutes").value || 0);
      const tbody = document.querySelector("#lb-table tbody");

      const rows = ROWS
        .filter(r => r.season === season)
        .filter(r => Number(r.games || 0) >= minGames)
        .filter(r => Number(r.minutes_total || 0) >= minMin)
        .slice()
        .sort((a,b) => {{
          const dir = lbSortDir === "asc" ? 1 : -1;
          if (lbSortKey === "player_name" || lbSortKey === "team_abbr") {{
            return dir * String(a[lbSortKey]).localeCompare(String(b[lbSortKey]));
          }}
          return dir * (Number(a[lbSortKey] || 0) - Number(b[lbSortKey] || 0));
        }});

      tbody.innerHTML = rows.map(rowHtml).join("");
    }}

    function init() {{
      const seasonSel = document.getElementById("season-filter");
      const teamSel = document.getElementById("team-filter");
      SEASONS.forEach(s => {{
        const o = document.createElement("option");
        o.value = s;
        o.textContent = s;
        seasonSel.appendChild(o);
      }});
      seasonSel.value = LATEST_SEASON;

      function refreshTeamOptions() {{
        const season = seasonSel.value;
        const seasonTeams = [...new Set(ROWS.filter(r => r.season === season).map(r => r.team_id))]
          .sort((a, b) => String(TEAM_MAP[a] || a).localeCompare(String(TEAM_MAP[b] || b)));
        teamSel.innerHTML = "";
        seasonTeams.forEach(tid => {{
          const o = document.createElement("option");
          o.value = tid;
          o.textContent = TEAM_MAP[tid] || tid;
          teamSel.appendChild(o);
        }});
        if (teamSel.options.length > 0) {{
          teamSel.selectedIndex = 0;
        }}
      }}

      refreshTeamOptions();

      ["season-filter","team-filter","team-min-games","team-min-minutes"].forEach(id =>
        document.getElementById(id).addEventListener("input", () => {{
          if (id === "season-filter") {{
            refreshTeamOptions();
          }}
          renderTeamTable();
          renderLeaderboard();
        }})
      );

      ["lb-min-games","lb-min-minutes"].forEach(id =>
        document.getElementById(id).addEventListener("input", renderLeaderboard)
      );

      document.querySelectorAll("#lb-table thead th.sortable").forEach(th => {{
        th.addEventListener("click", () => {{
          const key = th.dataset.key;
          if (lbSortKey === key) {{
            lbSortDir = lbSortDir === "desc" ? "asc" : "desc";
          }} else {{
            lbSortKey = key;
            lbSortDir = key === "player_name" || key === "team_abbr" ? "asc" : "desc";
          }}
          renderLeaderboard();
        }});
      }});
      document.querySelectorAll("#team-table thead th.sortable").forEach(th => {{
        th.addEventListener("click", () => {{
          const key = th.dataset.key;
          if (teamSortKey === key) {{
            teamSortDir = teamSortDir === "desc" ? "asc" : "desc";
          }} else {{
            teamSortKey = key;
            teamSortDir = key === "player_name" || key === "team_abbr" ? "asc" : "desc";
          }}
          renderTeamTable();
        }});
      }});

      renderTeamTable();
      renderLeaderboard();
    }}

    init();
  </script>
</body>
</html>"""

    OUTPUT_DATA_PATH.write_text(html, encoding="utf-8")
    OUTPUT_SITE_PATH.write_text(html, encoding="utf-8")
    print(f"Report saved to: {OUTPUT_DATA_PATH.absolute()}")
    print(f"Also saved to: {OUTPUT_SITE_PATH.absolute()}")
    return OUTPUT_DATA_PATH


if __name__ == "__main__":
    generate_onoff_report()
