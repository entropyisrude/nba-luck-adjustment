"""Generate playoff RAPM HTML report with multi-year period options."""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data")
STINTS_PATH = DATA_DIR / "stints_playoffs.csv"
OUTPUT_PATH = Path("rapm-playoffs.html")
PLAYER_INFO_MAP = DATA_DIR / "player_info_map.json"

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

# Define periods to pre-compute
PERIODS = {
    "all": {"label": "All Time (1996-2025)", "start": None, "end": None},
    "last5": {"label": "Last 5 Years", "years_back": 4},
    "last10": {"label": "Last 10 Years", "years_back": 9},
    "2020s": {"label": "2020s", "start_year": 2020, "end_year": 2029},
    "2010s": {"label": "2010s", "start_year": 2010, "end_year": 2019},
    "2000s": {"label": "2000s", "start_year": 2000, "end_year": 2009},
}
ALPHAS = [10, 500]
DEFAULT_ALPHA = 500

def compute_rapm_for_period(
    stints: pd.DataFrame,
    period_key: str,
    latest_year: int,
    alpha: float,
) -> list[dict]:
    """Compute RAPM for a specific period by filtering stints and running regression."""
    from run_rapm import compute_unified_stint_rapm_rows

    period = PERIODS[period_key]
    df = stints.copy()

    # Filter by period
    if "years_back" in period:
        min_year = latest_year - period["years_back"]
        df = df[df["playoff_year"] >= min_year]
    elif "start_year" in period:
        df = df[(df["playoff_year"] >= period["start_year"]) & (df["playoff_year"] <= period["end_year"])]

    if df.empty:
        return []

    min_minutes = 50 if period_key != "all" else 100
    results = compute_unified_stint_rapm_rows(
        df, alpha=alpha, min_minutes=min_minutes, suffix="_playoffs"
    )
    for row in results:
        row["minutes"] = int(round(row["minutes"]))
        row["rapm"] = round(float(row["rapm"]), 2)
        row["orapm"] = round(float(row["orapm"]), 2)
        row["drapm"] = round(float(row["drapm"]), 2)
        row["rapm_raw"] = round(float(row["rapm_raw"]), 2)
        row["orapm_raw"] = round(float(row["orapm_raw"]), 2)
        row["drapm_raw"] = round(float(row["drapm_raw"]), 2)
        row.pop("team_id", None)

    return sorted(results, key=lambda x: x["rapm"], reverse=True)


def generate_rapm_report_playoffs():
    if not STINTS_PATH.exists():
        print(f"Error: {STINTS_PATH} not found")
        return

    print("Loading stints...")
    stints = pd.read_csv(STINTS_PATH, dtype={"game_id": str})
    stints["date"] = pd.to_datetime(stints["date"])
    # Extract playoff year (playoffs happen in spring, so year of the date is the end of the season)
    stints["playoff_year"] = stints["date"].dt.year

    latest_year = stints["playoff_year"].max()
    print(f"Latest playoff year: {latest_year}")

    # Compute RAPM for each period and alpha
    all_data = {}
    for period_key in PERIODS.keys():
        for alpha in ALPHAS:
            print(f"Computing RAPM for {period_key} (alpha={alpha})...")
            results = compute_rapm_for_period(stints, period_key, latest_year, alpha)
            all_data[f"{period_key}_a{alpha}"] = results
            if alpha == DEFAULT_ALPHA:
                all_data[period_key] = results
            print(f"  {len(results)} players")

    generated_ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    page_title = "3PT Luck Adjusted RAPM: Playoffs"

    # Build period options HTML
    period_options = "\n".join([
        f'<option value="{k}">{v["label"]}</option>'
        for k, v in PERIODS.items()
    ])

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
    th:first-child, td:first-child {{ text-align: center; }}
    th:nth-child(2), td:nth-child(2) {{ text-align: left; }}
    th:nth-child(3), td:nth-child(3) {{ text-align: left; }}
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
        <span class="chip">Alpha: 500</span>
        <span class="chip">3PT Luck Adjusted</span>
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
        <label>Period
          <select id="period-filter">
            {period_options}
          </select>
        </label>
        <label>Alpha
          <select id="alpha-filter">
            <option value="500">α=500 (default)</option>
            <option value="10">α=10</option>
          </select>
        </label>
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
        <button type="button" id="toggle-raw" class="toggle-btn">Show Raw</button>
      </div>
      <p class="muted" style="margin: 0 0 8px; font-size: 11px;">
        RAPM, ORAPM, and DRAPM are per 100 possessions. Adjusted columns use 3PT-luck-adjusted scoring.
      </p>
      <div class="table-wrap">
        <table id="rapm-table">
          <thead>
            <tr>
              <th data-key="rank">#</th>
              <th data-key="player_name">Player</th>
              <th data-key="team_abbr">Team</th>
              <th data-key="minutes">Min</th>
              <th class="group-start" data-key="rapm">RAPM Adj</th>
              <th data-key="orapm">ORAPM Adj</th>
              <th data-key="drapm">DRAPM Adj</th>
              <th class="col-raw group-start col-hidden" data-key="rapm_raw">RAPM Raw</th>
              <th class="col-raw col-hidden" data-key="orapm_raw">ORAPM Raw</th>
              <th class="col-raw col-hidden" data-key="drapm_raw">DRAPM Raw</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <p class="muted">Generated {generated_ts} | Source: data/stints_playoffs.csv</p>
  </div>
  <script>
    const ALL_DATA = {json.dumps(all_data)};
    const DEFAULT_ALPHA = {DEFAULT_ALPHA};

    let sortKey = "rapm";
    let sortDir = "desc";

    const fmt = (x, d=1) => (x === null || Number.isNaN(Number(x))) ? "" : Number(x).toFixed(d);
    const cls = (x) => (x > 0 ? "pos" : (x < 0 ? "neg" : ""));

    function toggleRaw() {{
      const btn = document.getElementById("toggle-raw");
      const cols = document.querySelectorAll(".col-raw");
      const showing = btn.classList.toggle("active");
      cols.forEach(col => col.classList.toggle("col-hidden", !showing));
      btn.textContent = showing ? "Hide Raw" : "Show Raw";
    }}

    function getRows() {{
      const period = document.getElementById("period-filter").value;
      const alphaVal = document.getElementById("alpha-filter").value;
      const key = alphaVal ? `${{period}}_a${{alphaVal}}` : period;
      return ALL_DATA[key] || ALL_DATA[period] || [];
    }}

    function render() {{
      const rows = getRows();
      const team = document.getElementById("team-filter").value;
      const minMin = Number(document.getElementById("min-minutes").value || 0);
      const search = document.getElementById("search").value.toLowerCase();

      const filtered = rows
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

      const tbody = document.querySelector("#rapm-table tbody");
      const rawHidden = !document.getElementById("toggle-raw").classList.contains("active");
      tbody.innerHTML = filtered.map((r, i) => `
        <tr>
          <td class="rank">${{i + 1}}</td>
          <td>${{r.player_name}}</td>
          <td>${{r.team_abbr}}</td>
          <td>${{r.minutes.toLocaleString()}}</td>
          <td class="group-start ${{cls(r.rapm)}}">${{fmt(r.rapm, 2)}}</td>
          <td class="${{cls(r.orapm)}}">${{fmt(r.orapm, 2)}}</td>
          <td class="${{cls(r.drapm)}}">${{fmt(r.drapm, 2)}}</td>
          <td class="col-raw group-start${{rawHidden ? ' col-hidden' : ''}} ${{cls(r.rapm_raw)}}">${{fmt(r.rapm_raw, 2)}}</td>
          <td class="col-raw${{rawHidden ? ' col-hidden' : ''}} ${{cls(r.orapm_raw)}}">${{fmt(r.orapm_raw, 2)}}</td>
          <td class="col-raw${{rawHidden ? ' col-hidden' : ''}} ${{cls(r.drapm_raw)}}">${{fmt(r.drapm_raw, 2)}}</td>
        </tr>
      `).join("");

      // Update team filter for current period
      const teams = [...new Set(rows.map(r => r.team_abbr))].sort();
      const teamSel = document.getElementById("team-filter");
      const currentTeam = teamSel.value;
      teamSel.innerHTML = '<option value="">All Teams</option>';
      teams.forEach(t => {{
        const o = document.createElement("option");
        o.value = t;
        o.textContent = t;
        if (t === currentTeam) o.selected = true;
        teamSel.appendChild(o);
      }});
    }}

    function init() {{
      ["period-filter", "alpha-filter", "team-filter", "min-minutes", "search"].forEach(id => {{
        document.getElementById(id).addEventListener("input", render);
      }});
      document.getElementById("toggle-raw").addEventListener("click", () => {{
        toggleRaw();
        render();
      }});

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
