"""Generate season-to-date totals report for adjusted plus-minus and on/off."""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

DATA_DIR = Path("data")
BOXSCORE_PATH = DATA_DIR / "player_daily_boxscore.csv"
HISTORY_PATH = DATA_DIR / "player_onoff_history.csv"
OUTPUT_DATA_PATH = DATA_DIR / "onoff_report.html"
OUTPUT_SITE_PATH = Path("onoff.html")

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


def _f(v: str) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _load_meta() -> tuple[str, int]:
    if not BOXSCORE_PATH.exists():
        return "", 0
    latest_date = ""
    game_ids: set[str] = set()
    with BOXSCORE_PATH.open(newline="", encoding="utf-8") as f:
        rows = csv.DictReader(f)
        for r in rows:
            d = str(r["date"])
            if d > latest_date:
                latest_date = d
            game_ids.add(str(r["game_id"]))
    return latest_date, len(game_ids)


def _load_player_totals() -> list[dict]:
    if not HISTORY_PATH.exists():
        raise FileNotFoundError(f"Missing {HISTORY_PATH}")

    records: list[dict] = []
    with HISTORY_PATH.open(newline="", encoding="utf-8") as f:
        rows = csv.DictReader(f)
        for r in rows:
            minutes_total = _f(r["minutes_on_total"])
            games = int(_f(r["games"]))
            if minutes_total <= 0:
                continue

            # Estimated possessions = minutes * (100 / 48), so per-100 = value * 48 / minutes.
            scale = 48.0 / minutes_total
            pm_actual_100 = _f(r["on_diff_total"]) * scale
            pm_adj_100 = _f(r["on_diff_adj_total"]) * scale
            pm_delta_100 = pm_adj_100 - pm_actual_100
            onoff_actual_100 = _f(r["on_off_diff_total"]) * scale
            onoff_adj_100 = _f(r["on_off_diff_adj_total"]) * scale
            onoff_delta_100 = onoff_adj_100 - onoff_actual_100

            team_id = str(r["latest_team_id"])
            records.append(
                {
                    "player_id": str(r["player_id"]),
                    "player_name": str(r["player_name"]),
                    "team_id": team_id,
                    "team_abbr": TEAM_ID_TO_ABBR.get(team_id, team_id),
                    "games": games,
                    "minutes_total": minutes_total,
                    "pm_actual_100": pm_actual_100,
                    "pm_adj_100": pm_adj_100,
                    "pm_delta_100": pm_delta_100,
                    "onoff_actual_100": onoff_actual_100,
                    "onoff_adj_100": onoff_adj_100,
                    "onoff_delta_100": onoff_delta_100,
                }
            )
    return records


def generate_onoff_report() -> Path:
    records = _load_player_totals()
    latest_date, game_count = _load_meta()
    team_values = sorted({r["team_id"] for r in records}, key=lambda x: TEAM_ID_TO_ABBR.get(x, x))
    generated_ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    page_title = "3PT Luck Adjusted Plus Minus: Totals"

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
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
      font-family: "Segoe UI", Arial, sans-serif;
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
    }}
    h1 {{ margin: 0; font-size: 28px; }}
    h2 {{ margin: 0 0 10px; font-size: 20px; }}
    .meta {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 8px; }}
    .chip {{
      background: rgba(255,255,255,0.14);
      border: 1px solid rgba(255,255,255,0.22);
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
    }}
    .nav {{ margin-top: 10px; display: flex; gap: 12px; flex-wrap: wrap; }}
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
    table {{ width: 100%; border-collapse: collapse; min-width: 960px; font-size: 12px; }}
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
  <div class="wrap">
    <section class="hero">
      <h1>{page_title}</h1>
      <div class="meta">
        <span class="chip">Season through {latest_date}</span>
        <span class="chip">Games: {game_count:,}</span>
        <span class="chip">Players: {len(records):,}</span>
      </div>
      <div class="nav">
        <a href="index.html">Main 3PT Luck Page</a>
        <a href="onoff-daily.html">3PT Luck Adjust Plus Minus: Games</a>
      </div>
    </section>

    <section class="card">
      <h2>Team-by-Team Season Totals</h2>
      <div class="controls">
        <label>Team
          <select id="team-filter"></select>
        </label>
        <label>Min games
          <input id="team-min-games" type="number" min="0" step="1" value="1" />
        </label>
        <label>Min total minutes
          <input id="team-min-minutes" type="number" min="0" step="1" value="50" />
        </label>
      </div>
      <div class="table-wrap">
        <table id="team-table">
          <thead>
            <tr>
              <th>Player</th><th>Team</th><th>G</th><th>Min</th>
              <th>PM/100</th><th>PM Adj/100</th><th>PM Delta/100</th>
              <th>OnOff/100</th><th>OnA/100</th><th>OnOff Delta/100</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h2>Season Leaderboard (Sortable)</h2>
      <div class="controls">
        <label>Min games
          <input id="lb-min-games" type="number" min="0" step="1" value="10" />
        </label>
        <label>Min total minutes
          <input id="lb-min-minutes" type="number" min="0" step="1" value="200" />
        </label>
      </div>
      <div class="table-wrap">
        <table id="lb-table">
          <thead>
            <tr>
              <th class="sortable" data-key="player_name" data-type="str">Player</th>
              <th class="sortable" data-key="team_abbr" data-type="str">Team</th>
              <th class="sortable" data-key="games" data-type="num">G</th>
              <th class="sortable" data-key="minutes_total" data-type="num">Min</th>
              <th class="sortable" data-key="pm_actual_100" data-type="num">PM/100</th>
              <th class="sortable" data-key="pm_adj_100" data-type="num">PM Adj/100</th>
              <th class="sortable" data-key="pm_delta_100" data-type="num">PM Delta/100</th>
              <th class="sortable" data-key="onoff_actual_100" data-type="num">OnOff/100</th>
              <th class="sortable" data-key="onoff_adj_100" data-type="num">OnA/100</th>
              <th class="sortable" data-key="onoff_delta_100" data-type="num">OnOff Delta/100</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <p class="muted">Generated {generated_ts} | Source: `data/player_onoff_history.csv`. Per-100 uses estimated possessions from minutes: possessions = minutes × (100/48).</p>
  </div>
  <script>
    const ROWS = {json.dumps(records)};
    const TEAMS = {json.dumps(team_values)};
    const TEAM_MAP = {json.dumps(TEAM_ID_TO_ABBR)};

    const fmt = (x, d=1) => (x === null || Number.isNaN(Number(x))) ? "" : Number(x).toFixed(d);
    const cls = (x) => (x > 0 ? "pos" : (x < 0 ? "neg" : ""));

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
        <td class="${{cls(r.onoff_delta_100)}}">${{fmt(r.onoff_delta_100,1)}}</td>
      </tr>`;
    }}

    function renderTeamTable() {{
      const team = document.getElementById("team-filter").value;
      const minGames = Number(document.getElementById("team-min-games").value || 0);
      const minMin = Number(document.getElementById("team-min-minutes").value || 0);
      const tbody = document.querySelector("#team-table tbody");

      const rows = ROWS
        .filter(r => r.team_id === team)
        .filter(r => Number(r.games || 0) >= minGames)
        .filter(r => Number(r.minutes_total || 0) >= minMin)
        .sort((a,b) => Number(b.pm_adj_100 || 0) - Number(a.pm_adj_100 || 0));

      tbody.innerHTML = rows.map(rowHtml).join("");
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
          if (lbSortKey === "player_name" || lbSortKey === "team_abbr") {{
            return dir * String(a[lbSortKey]).localeCompare(String(b[lbSortKey]));
          }}
          return dir * (Number(a[lbSortKey] || 0) - Number(b[lbSortKey] || 0));
        }});

      tbody.innerHTML = rows.map(rowHtml).join("");
    }}

    function init() {{
      const teamSel = document.getElementById("team-filter");
      TEAMS.forEach(tid => {{
        const o = document.createElement("option");
        o.value = tid;
        o.textContent = TEAM_MAP[tid] || tid;
        teamSel.appendChild(o);
      }});
      if (teamSel.options.length > 0) {{
        teamSel.selectedIndex = 0;
      }}

      ["team-filter","team-min-games","team-min-minutes"].forEach(id =>
        document.getElementById(id).addEventListener("input", renderTeamTable)
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
