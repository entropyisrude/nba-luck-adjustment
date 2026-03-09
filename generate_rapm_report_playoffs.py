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
    from run_rapm import (
        build_design_matrix,
        run_rapm,
        get_player_info,
    )

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

    # Build design matrix and run RAPM
    X, y, weights, player_list, player_to_idx = build_design_matrix(df, use_adjusted=True)
    coefficients, intercept = run_rapm(X, y, weights, alpha=alpha)

    # Get player info
    player_info = get_player_info(player_list, df, "_playoffs")

    # Calculate minutes per player per team
    player_minutes = {}
    player_team_minutes = {}
    for col_set, team_col in [(["home_p1", "home_p2", "home_p3", "home_p4", "home_p5"], "home_id"),
                               (["away_p1", "away_p2", "away_p3", "away_p4", "away_p5"], "away_id")]:
        for col in col_set:
            for _, row in df.iterrows():
                pid = row[col]
                if pd.notna(pid):
                    pid = int(pid)
                    mins = row["seconds"] / 60.0
                    player_minutes[pid] = player_minutes.get(pid, 0) + mins
                    team_id = int(row[team_col])
                    if pid not in player_team_minutes:
                        player_team_minutes[pid] = {}
                    player_team_minutes[pid][team_id] = player_team_minutes[pid].get(team_id, 0) + mins

    # Build results
    results = []
    min_minutes = 50 if period_key != "all" else 100
    name_map = {}
    if PLAYER_INFO_MAP.exists():
        try:
            raw = json.loads(PLAYER_INFO_MAP.read_text(encoding="utf-8"))
            name_map = {int(k): v.get("name") for k, v in raw.items() if isinstance(v, dict)}
        except Exception:
            name_map = {}

    for i, pid in enumerate(player_list):
        info = player_info.get(pid, {})
        minutes = player_minutes.get(pid, 0)
        if minutes < min_minutes:
            continue
        # Use team where player has most minutes
        team_mins = player_team_minutes.get(pid, {})
        if team_mins:
            primary_team_id = max(team_mins.keys(), key=lambda t: team_mins[t])
        else:
            primary_team_id = info.get("team_id", 0)
        mapped = name_map.get(int(pid))
        name = mapped or info.get("name", f"Player {pid}")
        results.append({
            "player_id": int(pid),
            "player_name": name,
            "team_abbr": TEAM_ID_TO_ABBR.get(primary_team_id, "???"),
            "minutes": int(round(minutes)),
            "rapm": round(float(coefficients[i]), 2),
        })

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
      </div>
      <p class="muted" style="margin: 0 0 8px; font-size: 11px;">
        RAPM = Regularized Adjusted Plus-Minus per 100 possessions
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

    <p class="muted">Generated {generated_ts} | Source: data/stints_playoffs.csv</p>
  </div>
  <script>
    const ALL_DATA = {json.dumps(all_data)};
    const DEFAULT_ALPHA = {DEFAULT_ALPHA};

    let sortKey = "rapm";
    let sortDir = "desc";

    const fmt = (x, d=1) => (x === null || Number.isNaN(Number(x))) ? "" : Number(x).toFixed(d);
    const cls = (x) => (x > 0 ? "pos" : (x < 0 ? "neg" : ""));

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
      tbody.innerHTML = filtered.map((r, i) => `
        <tr>
          <td class="rank">${{i + 1}}</td>
          <td>${{r.player_name}}</td>
          <td>${{r.team_abbr}}</td>
          <td>${{r.minutes.toLocaleString()}}</td>
          <td class="${{cls(r.rapm)}}">${{fmt(r.rapm, 2)}}</td>
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
