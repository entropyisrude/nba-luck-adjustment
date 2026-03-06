"""Generate playoff RAPM HTML report from rapm_playoffs.csv."""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data")
RAPM_CSV = DATA_DIR / "rapm_playoffs.csv"
OUTPUT_PATH = Path("rapm-playoffs.html")

TEAM_ID_TO_ABBR = {
    1610612737: "ATL", 1610612738: "BOS", 1610612751: "BKN", 1610612766: "CHA",
    1610612741: "CHI", 1610612739: "CLE", 1610612742: "DAL", 1610612743: "DEN",
    1610612765: "DET", 1610612744: "GSW", 1610612745: "HOU", 1610612754: "IND",
    1610612746: "LAC", 1610612747: "LAL", 1610612763: "MEM", 1610612748: "MIA",
    1610612749: "MIL", 1610612750: "MIN", 1610612740: "NOP", 1610612752: "NYK",
    1610612760: "OKC", 1610612753: "ORL", 1610612755: "PHI", 1610612756: "PHX",
    1610612757: "POR", 1610612758: "SAC", 1610612759: "SAS", 1610612761: "TOR",
    1610612762: "UTA", 1610612764: "WAS",
}


def generate_rapm_report_playoffs():
    if not RAPM_CSV.exists():
        print(f"Error: {RAPM_CSV} not found. Run: python run_rapm.py --playoffs")
        return

    df = pd.read_csv(RAPM_CSV)
    print(f"Loaded {len(df)} players from {RAPM_CSV}")

    # Convert to records for JSON
    records = []
    for _, row in df.iterrows():
        records.append({
            "player_id": int(row["player_id"]),
            "player_name": str(row["player_name"]),
            "team_abbr": str(row["team_abbr"]),
            "minutes": int(row["minutes"]),
            "rapm": round(float(row["rapm"]), 2),
        })

    generated_ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    page_title = "3PT Luck Adjusted RAPM: Playoffs (1996-2025)"

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
    .wrap {{ max-width: 1000px; margin: 0 auto; padding: 18px; }}
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
      max-height: 700px;
      background: #fff;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{
      border-bottom: 1px solid #edf2f9;
      padding: 8px 10px;
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child {{ text-align: left; }}
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
    .rank {{ color: var(--muted); font-size: 11px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>{page_title}</h1>
      <div class="meta">
        <span class="chip">Players: {len(records):,}</span>
        <span class="chip">Min 100 playoff minutes</span>
        <span class="chip">Alpha: 2500</span>
      </div>
      <div class="nav">
        <a href="index.html">Main 3PT Luck Page</a>
        <a href="onoff-playoffs.html">Playoff On/Off</a>
        <a href="rapm.html">Regular Season RAPM</a>
      </div>
    </section>

    <section class="card">
      <h2>Playoff RAPM Leaderboard</h2>
      <div class="controls">
        <label>Team
          <select id="team-filter">
            <option value="">All Teams</option>
          </select>
        </label>
        <label>Min Minutes
          <input id="min-minutes" type="number" min="0" step="10" value="100" />
        </label>
        <label>Search
          <input id="search" type="text" placeholder="Player name..." />
        </label>
      </div>
      <p class="muted" style="margin: 0 0 8px; font-size: 11px;">
        RAPM = Regularized Adjusted Plus-Minus per 100 possessions, 3PT-luck adjusted
      </p>
      <div class="table-wrap">
        <table id="rapm-table">
          <thead>
            <tr>
              <th data-key="rank">#</th>
              <th data-key="player_name">Player</th>
              <th data-key="team_abbr">Team</th>
              <th data-key="minutes">Min</th>
              <th data-key="rapm">RAPM</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <p class="muted">Generated {generated_ts} | Source: data/rapm_playoffs.csv</p>
  </div>
  <script>
    const ROWS = {json.dumps(records)};

    let sortKey = "rapm";
    let sortDir = "desc";

    const fmt = (x, d=1) => (x === null || Number.isNaN(Number(x))) ? "" : Number(x).toFixed(d);
    const cls = (x) => (x > 0 ? "pos" : (x < 0 ? "neg" : ""));

    function getFilteredRows() {{
      const team = document.getElementById("team-filter").value;
      const minMin = Number(document.getElementById("min-minutes").value || 0);
      const search = document.getElementById("search").value.toLowerCase();

      return ROWS
        .filter(r => !team || r.team_abbr === team)
        .filter(r => r.minutes >= minMin)
        .filter(r => !search || r.player_name.toLowerCase().includes(search))
        .sort((a, b) => {{
          const dir = sortDir === "asc" ? 1 : -1;
          if (sortKey === "player_name" || sortKey === "team_abbr") {{
            return dir * String(a[sortKey]).localeCompare(String(b[sortKey]));
          }}
          return dir * (Number(a[sortKey] || 0) - Number(b[sortKey] || 0));
        }});
    }}

    function render() {{
      const rows = getFilteredRows();
      const tbody = document.querySelector("#rapm-table tbody");

      tbody.innerHTML = rows.map((r, i) => `
        <tr>
          <td class="rank">${{i + 1}}</td>
          <td>${{r.player_name}}</td>
          <td>${{r.team_abbr}}</td>
          <td>${{r.minutes.toLocaleString()}}</td>
          <td class="${{cls(r.rapm)}}">${{fmt(r.rapm, 2)}}</td>
        </tr>
      `).join("");
    }}

    function init() {{
      // Populate team filter
      const teams = [...new Set(ROWS.map(r => r.team_abbr))].sort();
      const teamSel = document.getElementById("team-filter");
      teams.forEach(t => {{
        const o = document.createElement("option");
        o.value = t;
        o.textContent = t;
        teamSel.appendChild(o);
      }});

      // Event listeners
      ["team-filter", "min-minutes", "search"].forEach(id => {{
        document.getElementById(id).addEventListener("input", render);
      }});

      // Sort on header click
      document.querySelectorAll("#rapm-table thead th").forEach(th => {{
        th.addEventListener("click", () => {{
          const key = th.dataset.key;
          if (key === "rank") return;
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

    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Report saved to: {OUTPUT_PATH.absolute()}")


if __name__ == "__main__":
    generate_rapm_report_playoffs()
