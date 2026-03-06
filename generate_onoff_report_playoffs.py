"""Generate playoff season totals report for adjusted plus-minus and on/off."""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

DATA_DIR = Path("data")
ONOFF_PATH = DATA_DIR / "adjusted_onoff_playoffs.csv"
OUTPUT_DATA_PATH = DATA_DIR / "onoff_report_playoffs.html"
OUTPUT_SITE_PATH = Path("onoff-playoffs.html")

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
    # Historical teams
    "1610612747": "LAL",
    "1610612746": "LAC",
    "1610612740": "NOP",
    "1610610031": "NJN",  # New Jersey Nets
    "1610610032": "SEA",  # Seattle SuperSonics
    "1610610030": "VAN",  # Vancouver Grizzlies
}


def _f(v: object) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _season_from_date(date_str: str) -> str:
    """Convert date to season string (e.g., '2020-05-15' -> '2019-20')."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    # Playoffs happen in the second half of the season (April-June)
    # So 2020-05-15 is part of the 2019-20 season
    start = d.year - 1 if d.month <= 8 else d.year
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
    for a in agg.values():
        out.append(
            {
                "player_id": a["player_id"],
                "player_name": a["player_name"],
                "season": a["season"],
                "team_id": a["team_id"],
                "team_abbr": TEAM_ID_TO_ABBR.get(a["team_id"], a["team_id"][:3] if len(a["team_id"]) > 3 else "???"),
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

    return out, latest_date, len(game_ids), sorted({r["team_id"] for r in out})


def _finalize_records(raw_rows: list[dict]) -> list[dict]:
    """Convert raw aggregates to per-100-possession stats."""
    records: list[dict] = []

    for r in raw_rows:
        on_min = _f(r["minutes_total"])
        off_min = _f(r["minutes_off_total"])

        # Use minute-based estimation for all stats (no pbpstats for historical playoffs)
        pm_actual_100 = (_f(r["on_diff_total"]) * 48.0 / on_min) if on_min > 0 else 0.0
        if off_min > 0:
            off_actual_100 = _f(r["off_diff_total"]) * 48.0 / off_min
            onoff_actual_100 = pm_actual_100 - off_actual_100
        else:
            onoff_actual_100 = 0.0

        # Adjusted stats
        pm_adj_100 = (_f(r["on_diff_adj_total"]) * 48.0 / on_min) if on_min > 0 else 0.0
        if off_min > 0:
            off_adj_100 = _f(r["off_diff_adj_total"]) * 48.0 / off_min
            onoff_adj_100 = pm_adj_100 - off_adj_100
        else:
            onoff_adj_100 = 0.0

        pm_delta_100 = pm_adj_100 - pm_actual_100
        onoff_delta_100 = onoff_adj_100 - onoff_actual_100

        # Offensive/defensive components
        on_for_adj = _f(r["on_pts_for_adj_total"])
        on_against_adj = _f(r["on_pts_against_adj_total"])
        off_for_adj = _f(r["off_pts_for_adj_total"])
        off_against_adj = _f(r["off_pts_against_adj_total"])

        poss_per_min = 100.0 / 48.0
        if on_min > 0:
            on_poss_est = on_min * poss_per_min
            on_ortg_adj = on_for_adj * 100.0 / on_poss_est
            on_drtg_adj = on_against_adj * 100.0 / on_poss_est
        else:
            on_ortg_adj = on_drtg_adj = 0.0

        if off_min > 0:
            off_poss_est = off_min * poss_per_min
            off_ortg_adj = off_for_adj * 100.0 / off_poss_est
            off_drtg_adj = off_against_adj * 100.0 / off_poss_est
        else:
            off_ortg_adj = off_drtg_adj = 0.0

        onoff_adj_off_100 = on_ortg_adj - off_ortg_adj
        onoff_adj_def_100 = off_drtg_adj - on_drtg_adj

        records.append(
            {
                "player_id": r["player_id"],
                "player_name": r["player_name"],
                "season": r["season"],
                "team_id": r["team_id"],
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
                "on_ortg_adj": on_ortg_adj,
                "on_drtg_adj": on_drtg_adj,
                "off_ortg_adj": off_ortg_adj,
                "off_drtg_adj": off_drtg_adj,
            }
        )
    return records


def generate_onoff_report_playoffs() -> Path:
    raw_rows, latest_date, game_count, team_ids = _load_team_player_totals()
    latest_season = _season_from_date(latest_date)
    records = _finalize_records(raw_rows)

    team_values = sorted({r["team_id"] for r in records}, key=lambda x: TEAM_ID_TO_ABBR.get(x, x))
    season_values = sorted({r["season"] for r in records}, reverse=True)
    generated_ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    page_title = "3PT Luck Adjusted Plus Minus: Playoffs"

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
      background: radial-gradient(circle at 20% 20%, #6b2d5c 0%, #3d1a35 45%, #1f0d1a 100%);
      color: #f8fbff;
      border-radius: 14px;
      padding: 18px 20px;
      border: 1px solid #6b4263;
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
    .nav a {{ color: #f5d4ef; text-decoration: underline; text-underline-offset: 3px; font-size: 13px; }}
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
      background: #f3edf2;
      color: #3d1a35;
    }}
    thead th.sortable {{ cursor: pointer; }}
    thead th.sortable:hover {{ background: #ebe3ea; }}
    .pos {{ color: var(--good); font-weight: 600; }}
    .neg {{ color: var(--bad); font-weight: 600; }}
    .muted {{ color: var(--muted); }}
    .toggle-row {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }}
    .toggle-btn {{
      background: #f3e8f1;
      border: 1px solid #d4c5d2;
      border-radius: 6px;
      padding: 5px 10px;
      font-size: 12px;
      cursor: pointer;
      color: var(--ink);
    }}
    .toggle-btn:hover {{ background: #e8dbe6; }}
    .toggle-btn.active {{ background: #3d1a35; color: #fff; border-color: #3d1a35; }}
    .col-hidden {{ display: none; }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <section class=\"hero\">
      <h1>{page_title}</h1>
      <div class=\"meta\">
        <span class=\"chip\">Seasons: {season_values[-1]} to {season_values[0]}</span>
        <span class=\"chip\">Games: {game_count:,}</span>
        <span class=\"chip\">Player-seasons: {len(records):,}</span>
      </div>
      <div class=\"nav\">
        <a href=\"index.html\">Main 3PT Luck Page</a>
        <a href=\"onoff.html\">Regular Season On/Off</a>
        <a href=\"rapm-playoffs.html\">Playoff RAPM</a>
      </div>
    </section>

    <section class=\"card\">
      <h2>Playoff Season Totals by Team</h2>
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
          <input id=\"team-min-minutes\" type=\"number\" min=\"0\" step=\"1\" value=\"10\" />
        </label>
      </div>
      <div class=\"toggle-row\">
        <button class=\"toggle-btn\" data-cols=\"ortg-adj\" onclick=\"toggleCols('ortg-adj')\">Show ORtg Adj</button>
        <button class=\"toggle-btn\" data-cols=\"drtg-adj\" onclick=\"toggleCols('drtg-adj')\">Show DRtg Adj</button>
      </div>
      <p class=\"muted\" style=\"margin: 0 0 8px; font-size: 11px;\">All stats per 48 minutes</p>
      <div class=\"table-wrap\">
        <table id=\"team-table\">
          <thead>
            <tr>
              <th class=\"sortable\" data-key=\"player_name\" data-type=\"str\">Player</th>
              <th class=\"sortable\" data-key=\"team_abbr\" data-type=\"str\">Team</th>
              <th class=\"sortable\" data-key=\"games\" data-type=\"num\">G</th>
              <th class=\"sortable\" data-key=\"minutes_total\" data-type=\"num\">Min</th>
              <th class=\"sortable\" data-key=\"pm_actual_100\" data-type=\"num\">PM</th>
              <th class=\"sortable\" data-key=\"pm_adj_100\" data-type=\"num\">PM Adj</th>
              <th class=\"sortable\" data-key=\"pm_delta_100\" data-type=\"num\">PM Delta</th>
              <th class=\"sortable\" data-key=\"onoff_actual_100\" data-type=\"num\">OnOff</th>
              <th class=\"sortable\" data-key=\"onoff_adj_100\" data-type=\"num\">OnOff Adj</th>
              <th class=\"sortable\" data-key=\"onoff_adj_off_100\" data-type=\"num\">OnOff Adj Off</th>
              <th class=\"sortable col-ortg-adj col-hidden\" data-key=\"on_ortg_adj\" data-type=\"num\">On ORtg Adj</th>
              <th class=\"sortable col-ortg-adj col-hidden\" data-key=\"off_ortg_adj\" data-type=\"num\">Off ORtg Adj</th>
              <th class=\"sortable\" data-key=\"onoff_adj_def_100\" data-type=\"num\">OnOff Adj Def</th>
              <th class=\"sortable col-drtg-adj col-hidden\" data-key=\"on_drtg_adj\" data-type=\"num\">On DRtg Adj</th>
              <th class=\"sortable col-drtg-adj col-hidden\" data-key=\"off_drtg_adj\" data-type=\"num\">Off DRtg Adj</th>
              <th class=\"sortable\" data-key=\"onoff_delta_100\" data-type=\"num\">OnOff Delta</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section class=\"card\">
      <h2>Playoff Leaderboard (All Seasons)</h2>
      <div class=\"controls\">
        <label>Min games
          <input id=\"lb-min-games\" type=\"number\" min=\"0\" step=\"1\" value=\"4\" />
        </label>
        <label>Min total minutes
          <input id=\"lb-min-minutes\" type=\"number\" min=\"0\" step=\"1\" value=\"50\" />
        </label>
      </div>
      <div class=\"toggle-row\">
        <button class=\"toggle-btn\" data-cols=\"ortg-adj\" onclick=\"toggleCols('ortg-adj')\">Show ORtg Adj</button>
        <button class=\"toggle-btn\" data-cols=\"drtg-adj\" onclick=\"toggleCols('drtg-adj')\">Show DRtg Adj</button>
      </div>
      <p class=\"muted\" style=\"margin: 0 0 8px; font-size: 11px;\">All stats per 48 minutes</p>
      <div class=\"table-wrap\">
        <table id=\"lb-table\">
          <thead>
            <tr>
              <th class=\"sortable\" data-key=\"player_name\" data-type=\"str\">Player</th>
              <th class=\"sortable\" data-key=\"team_abbr\" data-type=\"str\">Team</th>
              <th class=\"sortable\" data-key=\"season\" data-type=\"str\">Season</th>
              <th class=\"sortable\" data-key=\"games\" data-type=\"num\">G</th>
              <th class=\"sortable\" data-key=\"minutes_total\" data-type=\"num\">Min</th>
              <th class=\"sortable\" data-key=\"pm_actual_100\" data-type=\"num\">PM</th>
              <th class=\"sortable\" data-key=\"pm_adj_100\" data-type=\"num\">PM Adj</th>
              <th class=\"sortable\" data-key=\"pm_delta_100\" data-type=\"num\">PM Delta</th>
              <th class=\"sortable\" data-key=\"onoff_actual_100\" data-type=\"num\">OnOff</th>
              <th class=\"sortable\" data-key=\"onoff_adj_100\" data-type=\"num\">OnOff Adj</th>
              <th class=\"sortable\" data-key=\"onoff_adj_off_100\" data-type=\"num\">OnOff Adj Off</th>
              <th class=\"sortable col-ortg-adj col-hidden\" data-key=\"on_ortg_adj\" data-type=\"num\">On ORtg Adj</th>
              <th class=\"sortable col-ortg-adj col-hidden\" data-key=\"off_ortg_adj\" data-type=\"num\">Off ORtg Adj</th>
              <th class=\"sortable\" data-key=\"onoff_adj_def_100\" data-type=\"num\">OnOff Adj Def</th>
              <th class=\"sortable col-drtg-adj col-hidden\" data-key=\"on_drtg_adj\" data-type=\"num\">On DRtg Adj</th>
              <th class=\"sortable col-drtg-adj col-hidden\" data-key=\"off_drtg_adj\" data-type=\"num\">Off DRtg Adj</th>
              <th class=\"sortable\" data-key=\"onoff_delta_100\" data-type=\"num\">OnOff Delta</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <p class=\"muted\">Generated {generated_ts} | Source: data/adjusted_onoff_playoffs.csv | Data: 1996-97 to present</p>
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

    function toggleCols(colGroup) {{
      const btns = document.querySelectorAll(`.toggle-btn[data-cols="${{colGroup}}"]`);
      const cols = document.querySelectorAll(`.col-${{colGroup}}`);
      const isHidden = cols[0]?.classList.contains('col-hidden');
      btns.forEach(btn => btn.classList.toggle('active', isHidden));
      cols.forEach(col => col.classList.toggle('col-hidden', !isHidden));
      btns.forEach(btn => btn.textContent = isHidden ? btn.textContent.replace('Show', 'Hide') : btn.textContent.replace('Hide', 'Show'));
    }}

    function rowHtml(r, showSeason=false) {{
      const ortgHidden = !document.querySelector('.toggle-btn[data-cols="ortg-adj"]')?.classList.contains('active');
      const drtgHidden = !document.querySelector('.toggle-btn[data-cols="drtg-adj"]')?.classList.contains('active');
      const seasonCol = showSeason ? `<td>${{r.season}}</td>` : '';
      return `<tr>
        <td>${{r.player_name}}</td>
        <td>${{r.team_abbr}}</td>
        ${{seasonCol}}
        <td>${{fmt(r.games,0)}}</td>
        <td>${{fmt(r.minutes_total,1)}}</td>
        <td class="${{cls(r.pm_actual_100)}}">${{fmt(r.pm_actual_100,1)}}</td>
        <td class="${{cls(r.pm_adj_100)}}">${{fmt(r.pm_adj_100,1)}}</td>
        <td class="${{cls(r.pm_delta_100)}}">${{fmt(r.pm_delta_100,1)}}</td>
        <td class="${{cls(r.onoff_actual_100)}}">${{fmt(r.onoff_actual_100,1)}}</td>
        <td class="${{cls(r.onoff_adj_100)}}">${{fmt(r.onoff_adj_100,1)}}</td>
        <td class="${{cls(r.onoff_adj_off_100)}}">${{fmt(r.onoff_adj_off_100,1)}}</td>
        <td class="col-ortg-adj${{ortgHidden ? ' col-hidden' : ''}}">${{fmt(r.on_ortg_adj,1)}}</td>
        <td class="col-ortg-adj${{ortgHidden ? ' col-hidden' : ''}}">${{fmt(r.off_ortg_adj,1)}}</td>
        <td class="${{cls(r.onoff_adj_def_100)}}">${{fmt(r.onoff_adj_def_100,1)}}</td>
        <td class="col-drtg-adj${{drtgHidden ? ' col-hidden' : ''}}">${{fmt(r.on_drtg_adj,1)}}</td>
        <td class="col-drtg-adj${{drtgHidden ? ' col-hidden' : ''}}">${{fmt(r.off_drtg_adj,1)}}</td>
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

      tbody.innerHTML = rows.map(r => rowHtml(r, false)).join("");
    }}

    function renderLeaderboard() {{
      const minGames = Number(document.getElementById("lb-min-games").value || 0);
      const minMin = Number(document.getElementById("lb-min-minutes").value || 0);
      const tbody = document.querySelector("#lb-table tbody");

      const rows = ROWS
        .filter(r => Number(r.games || 0) >= minGames)
        .filter(r => Number(r.minutes_total || 0) >= minMin)
        .slice()
        .sort((a,b) => {{
          const dir = lbSortDir === "asc" ? 1 : -1;
          if (lbSortKey === "player_name" || lbSortKey === "team_abbr" || lbSortKey === "season") {{
            return dir * String(a[lbSortKey]).localeCompare(String(b[lbSortKey]));
          }}
          return dir * (Number(a[lbSortKey] || 0) - Number(b[lbSortKey] || 0));
        }});

      tbody.innerHTML = rows.map(r => rowHtml(r, true)).join("");
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
            lbSortDir = key === "player_name" || key === "team_abbr" || key === "season" ? "asc" : "desc";
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
    generate_onoff_report_playoffs()
