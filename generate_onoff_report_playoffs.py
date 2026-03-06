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
    "1610612737": "ATL", "1610612738": "BOS", "1610612751": "BKN", "1610612766": "CHA",
    "1610612741": "CHI", "1610612739": "CLE", "1610612742": "DAL", "1610612743": "DEN",
    "1610612765": "DET", "1610612744": "GSW", "1610612745": "HOU", "1610612754": "IND",
    "1610612746": "LAC", "1610612747": "LAL", "1610612763": "MEM", "1610612748": "MIA",
    "1610612749": "MIL", "1610612750": "MIN", "1610612740": "NOP", "1610612752": "NYK",
    "1610612760": "OKC", "1610612753": "ORL", "1610612755": "PHI", "1610612756": "PHX",
    "1610612757": "POR", "1610612758": "SAC", "1610612759": "SAS", "1610612761": "TOR",
    "1610612762": "UTA", "1610612764": "WAS",
}


def _f(v: object) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _season_from_date(date_str: str) -> str:
    """Convert date to season string (e.g., '2020-05-15' -> '2019-20')."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    start = d.year - 1 if d.month <= 8 else d.year
    return f"{start}-{(start + 1) % 100:02d}"


def _load_season_totals() -> tuple[list[dict], str, int]:
    """Load and aggregate per-season totals for each player-team."""
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

    # Aggregate by (season, team, player)
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
                "minutes_on": 0.0,
                "minutes_off": 0.0,
                "on_diff": 0.0,
                "off_diff": 0.0,
                "on_diff_adj": 0.0,
                "off_diff_adj": 0.0,
                "on_pts_for_adj": 0.0,
                "on_pts_against_adj": 0.0,
                "off_pts_for_adj": 0.0,
                "off_pts_against_adj": 0.0,
            }

        a = agg[key]
        a["games_set"].add(game_id)
        a["minutes_on"] += minutes_on
        a["minutes_off"] += minutes_off
        a["on_diff"] += _f(r["on_diff"])
        a["off_diff"] += _f(r["off_diff"])
        a["on_diff_adj"] += _f(r["on_diff_adj"])
        a["off_diff_adj"] += _f(r["off_diff_adj"])
        a["on_pts_for_adj"] += _f(r["on_pts_for_adj"])
        a["on_pts_against_adj"] += _f(r["on_pts_against_adj"])
        a["off_pts_for_adj"] += _f(r["off_pts_for_adj"])
        a["off_pts_against_adj"] += _f(r["off_pts_against_adj"])

    # Convert to list with raw totals (client will compute per-100 stats)
    out: list[dict] = []
    for a in agg.values():
        team_id = a["team_id"]
        out.append({
            "player_id": a["player_id"],
            "player_name": a["player_name"],
            "season": a["season"],
            "team_id": team_id,
            "team_abbr": TEAM_ID_TO_ABBR.get(team_id, team_id[:3] if len(team_id) > 3 else "???"),
            "games": len(a["games_set"]),
            "minutes_on": round(a["minutes_on"], 1),
            "minutes_off": round(a["minutes_off"], 1),
            "on_diff": round(a["on_diff"], 1),
            "off_diff": round(a["off_diff"], 1),
            "on_diff_adj": round(a["on_diff_adj"], 2),
            "off_diff_adj": round(a["off_diff_adj"], 2),
            "on_pts_for_adj": round(a["on_pts_for_adj"], 2),
            "on_pts_against_adj": round(a["on_pts_against_adj"], 2),
            "off_pts_for_adj": round(a["off_pts_for_adj"], 2),
            "off_pts_against_adj": round(a["off_pts_against_adj"], 2),
        })

    return out, latest_date, len(game_ids)


def generate_onoff_report_playoffs() -> Path:
    raw_rows, latest_date, game_count = _load_season_totals()
    latest_season = _season_from_date(latest_date)
    season_values = sorted({r["season"] for r in raw_rows}, reverse=True)
    generated_ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    page_title = "3PT Luck Adjusted Plus Minus: Playoffs"

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
    table {{ width: 100%; border-collapse: collapse; min-width: 900px; font-size: 12px; }}
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
      cursor: pointer;
    }}
    thead th:hover {{ background: #ebe3ea; }}
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
        <span class="chip">Seasons: 1996-97 to {latest_season}</span>
        <span class="chip">Games: {game_count:,}</span>
      </div>
      <div class="nav">
        <a href="index.html">Main 3PT Luck Page</a>
        <a href="onoff.html">Regular Season On/Off</a>
        <a href="rapm-playoffs.html">Playoff RAPM</a>
      </div>
    </section>

    <section class="card">
      <h2>Playoff Leaderboard</h2>
      <div class="controls">
        <label>Period
          <select id="period-filter">
            <option value="all">All Time (1996-2025)</option>
            <option value="last3">Last 3 Years</option>
            <option value="last5">Last 5 Years</option>
            <option value="last10">Last 10 Years</option>
            <option value="2020s">2020s</option>
            <option value="2010s">2010s</option>
            <option value="2000s">2000s</option>
            <option value="1990s">1990s</option>
          </select>
        </label>
        <label>Team
          <select id="team-filter">
            <option value="">All Teams</option>
          </select>
        </label>
        <label>Min Games
          <input id="min-games" type="number" min="0" step="1" value="4" />
        </label>
        <label>Min Minutes
          <input id="min-minutes" type="number" min="0" step="10" value="50" />
        </label>
        <label>Search
          <input id="search" type="text" placeholder="Player name..." />
        </label>
      </div>
      <p class="muted" style="margin: 0 0 8px; font-size: 11px;">All stats per 48 minutes. Players aggregated across selected period.</p>
      <div class="table-wrap">
        <table id="main-table">
          <thead>
            <tr>
              <th data-key="player_name">Player</th>
              <th data-key="team_abbr">Team</th>
              <th data-key="games">G</th>
              <th data-key="minutes">Min</th>
              <th data-key="pm_adj" title="Plus-minus per 48 min (3PT adjusted)">PM Adj</th>
              <th data-key="onoff_adj" title="On-Off per 48 min (3PT adjusted)">OnOff Adj</th>
              <th data-key="onoff_adj_off" title="Offensive On-Off (team ORtg impact)">Off</th>
              <th data-key="onoff_adj_def" title="Defensive On-Off (team DRtg impact)">Def</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <p class="muted">Generated {generated_ts} | Source: data/adjusted_onoff_playoffs.csv</p>
  </div>
  <script>
    const RAW_ROWS = {json.dumps(raw_rows)};
    const SEASONS = {json.dumps(season_values)};
    const TEAM_MAP = {json.dumps(TEAM_ID_TO_ABBR)};
    const LATEST_SEASON = {json.dumps(latest_season)};

    let sortKey = "onoff_adj";
    let sortDir = "desc";

    // Get seasons for a period
    function getSeasonsForPeriod(period) {{
      const latestYear = parseInt(LATEST_SEASON.split("-")[0]);
      switch(period) {{
        case "last3":
          return SEASONS.filter(s => parseInt(s.split("-")[0]) >= latestYear - 2);
        case "last5":
          return SEASONS.filter(s => parseInt(s.split("-")[0]) >= latestYear - 4);
        case "last10":
          return SEASONS.filter(s => parseInt(s.split("-")[0]) >= latestYear - 9);
        case "2020s":
          return SEASONS.filter(s => {{
            const y = parseInt(s.split("-")[0]);
            return y >= 2020 && y < 2030;
          }});
        case "2010s":
          return SEASONS.filter(s => {{
            const y = parseInt(s.split("-")[0]);
            return y >= 2010 && y < 2020;
          }});
        case "2000s":
          return SEASONS.filter(s => {{
            const y = parseInt(s.split("-")[0]);
            return y >= 2000 && y < 2010;
          }});
        case "1990s":
          return SEASONS.filter(s => {{
            const y = parseInt(s.split("-")[0]);
            return y >= 1990 && y < 2000;
          }});
        default: // "all"
          return SEASONS;
      }}
    }}

    // Aggregate rows for selected period
    function aggregateForPeriod(period) {{
      const validSeasons = new Set(getSeasonsForPeriod(period));
      const agg = {{}};

      for (const r of RAW_ROWS) {{
        if (!validSeasons.has(r.season)) continue;

        const key = r.player_id;
        if (!agg[key]) {{
          agg[key] = {{
            player_id: r.player_id,
            player_name: r.player_name,
            teams: {{}},
            games: 0,
            minutes_on: 0,
            minutes_off: 0,
            on_diff: 0,
            off_diff: 0,
            on_diff_adj: 0,
            off_diff_adj: 0,
            on_pts_for_adj: 0,
            on_pts_against_adj: 0,
            off_pts_for_adj: 0,
            off_pts_against_adj: 0,
          }};
        }}
        const a = agg[key];
        a.games += r.games;
        a.minutes_on += r.minutes_on;
        a.minutes_off += r.minutes_off;
        a.on_diff += r.on_diff;
        a.off_diff += r.off_diff;
        a.on_diff_adj += r.on_diff_adj;
        a.off_diff_adj += r.off_diff_adj;
        a.on_pts_for_adj += r.on_pts_for_adj;
        a.on_pts_against_adj += r.on_pts_against_adj;
        a.off_pts_for_adj += r.off_pts_for_adj;
        a.off_pts_against_adj += r.off_pts_against_adj;
        // Track minutes per team
        a.teams[r.team_id] = (a.teams[r.team_id] || 0) + r.minutes_on;
      }}

      // Convert to array with computed stats
      return Object.values(agg).map(a => {{
        const onMin = a.minutes_on;
        const offMin = a.minutes_off;

        // Primary team (most minutes)
        let primaryTeam = "";
        let maxMins = 0;
        for (const [tid, mins] of Object.entries(a.teams)) {{
          if (mins > maxMins) {{
            maxMins = mins;
            primaryTeam = tid;
          }}
        }}

        // Per-48 stats
        const pm_adj = onMin > 0 ? (a.on_diff_adj * 48.0 / onMin) : 0;
        const off_pm_adj = offMin > 0 ? (a.off_diff_adj * 48.0 / offMin) : 0;
        const onoff_adj = pm_adj - off_pm_adj;

        // Offensive/defensive components
        const poss_per_min = 100.0 / 48.0;
        const on_poss = onMin * poss_per_min;
        const off_poss = offMin * poss_per_min;
        const on_ortg = on_poss > 0 ? (a.on_pts_for_adj * 100.0 / on_poss) : 0;
        const on_drtg = on_poss > 0 ? (a.on_pts_against_adj * 100.0 / on_poss) : 0;
        const off_ortg = off_poss > 0 ? (a.off_pts_for_adj * 100.0 / off_poss) : 0;
        const off_drtg = off_poss > 0 ? (a.off_pts_against_adj * 100.0 / off_poss) : 0;
        const onoff_adj_off = on_ortg - off_ortg;
        const onoff_adj_def = off_drtg - on_drtg;

        return {{
          player_id: a.player_id,
          player_name: a.player_name,
          team_id: primaryTeam,
          team_abbr: TEAM_MAP[primaryTeam] || "???",
          games: a.games,
          minutes: Math.round(onMin),
          pm_adj: pm_adj,
          onoff_adj: onoff_adj,
          onoff_adj_off: onoff_adj_off,
          onoff_adj_def: onoff_adj_def,
        }};
      }});
    }}

    const fmt = (x, d=1) => Number.isFinite(x) ? x.toFixed(d) : "";
    const cls = (x) => (x > 0 ? "pos" : (x < 0 ? "neg" : ""));

    function render() {{
      const period = document.getElementById("period-filter").value;
      const teamFilter = document.getElementById("team-filter").value;
      const minGames = Number(document.getElementById("min-games").value || 0);
      const minMin = Number(document.getElementById("min-minutes").value || 0);
      const search = document.getElementById("search").value.toLowerCase();

      const rows = aggregateForPeriod(period)
        .filter(r => !teamFilter || r.team_id === teamFilter)
        .filter(r => r.games >= minGames)
        .filter(r => r.minutes >= minMin)
        .filter(r => !search || r.player_name.toLowerCase().includes(search))
        .sort((a, b) => {{
          const dir = sortDir === "asc" ? 1 : -1;
          if (sortKey === "player_name" || sortKey === "team_abbr") {{
            return dir * String(a[sortKey]).localeCompare(String(b[sortKey]));
          }}
          return dir * ((a[sortKey] || 0) - (b[sortKey] || 0));
        }});

      const tbody = document.querySelector("#main-table tbody");
      tbody.innerHTML = rows.map(r => `
        <tr>
          <td>${{r.player_name}}</td>
          <td>${{r.team_abbr}}</td>
          <td>${{r.games}}</td>
          <td>${{r.minutes.toLocaleString()}}</td>
          <td class="${{cls(r.pm_adj)}}">${{fmt(r.pm_adj)}}</td>
          <td class="${{cls(r.onoff_adj)}}">${{fmt(r.onoff_adj)}}</td>
          <td class="${{cls(r.onoff_adj_off)}}">${{fmt(r.onoff_adj_off)}}</td>
          <td class="${{cls(r.onoff_adj_def)}}">${{fmt(r.onoff_adj_def)}}</td>
        </tr>
      `).join("");
    }}

    function init() {{
      // Populate team filter
      const teams = [...new Set(RAW_ROWS.map(r => r.team_id))];
      const teamSel = document.getElementById("team-filter");
      teams.sort((a, b) => (TEAM_MAP[a] || a).localeCompare(TEAM_MAP[b] || b));
      teams.forEach(tid => {{
        const o = document.createElement("option");
        o.value = tid;
        o.textContent = TEAM_MAP[tid] || tid;
        teamSel.appendChild(o);
      }});

      // Event listeners
      ["period-filter", "team-filter", "min-games", "min-minutes", "search"].forEach(id => {{
        document.getElementById(id).addEventListener("input", render);
      }});

      // Sort on header click
      document.querySelectorAll("#main-table thead th").forEach(th => {{
        th.addEventListener("click", () => {{
          const key = th.dataset.key;
          if (sortKey === key) {{
            sortDir = sortDir === "desc" ? "asc" : "desc";
          }} else {{
            sortKey = key;
            sortDir = key === "player_name" || key === "team_abbr" ? "asc" : "desc";
          }}
          render();
        }});
      }});

      render();
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
