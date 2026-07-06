"""Generate playoff RAPM HTML report matching the regular season RAPM page style."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data")
STINTS_PATH = DATA_DIR / "stints_playoffs.csv"
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

ALPHAS = [10, 500]
DEFAULT_ALPHA = 500
MIN_MINUTES_SEASON = 50


def playoff_season_label(playoff_year: int) -> str:
    """Spring calendar year → season label, e.g. 2026 → '2025-26'."""
    return f"{playoff_year - 1}-{str(playoff_year)[-2:]}"


def compute_rapm(stints_df: pd.DataFrame, alpha: float, min_minutes: int = 50) -> list[dict]:
    from run_rapm import compute_unified_stint_rapm_rows
    results = compute_unified_stint_rapm_rows(
        stints_df, alpha=alpha, min_minutes=min_minutes, suffix="_playoffs"
    )
    for row in results:
        row["minutes"] = int(round(row["minutes"]))
        for col in ("rapm", "orapm", "drapm", "rapm_raw", "orapm_raw", "drapm_raw"):
            row[col] = round(float(row[col]), 2)
        row.pop("team_id", None)
    return sorted(results, key=lambda x: x["rapm"], reverse=True)


def generate_rapm_report_playoffs() -> None:
    if not STINTS_PATH.exists():
        print(f"Error: {STINTS_PATH} not found")
        return

    print("Loading stints...")
    stints = pd.read_csv(STINTS_PATH, dtype={"game_id": str})
    stints["date"] = pd.to_datetime(stints["date"])

    # Derive the playoff year from the game-id prefix (4YY = season starting in
    # year YY, playoffs the following spring). Stint dates are synthetic and one
    # year early for every playoff run before 2019-20, so date.year mislabels
    # every pre-2019 season.
    def _playoff_year(gid: str) -> int:
        yy = int(str(gid).lstrip("0")[1:3])
        return (1900 + yy if yy >= 90 else 2000 + yy) + 1

    stints["playoff_year"] = stints["game_id"].map(_playoff_year)

    playoff_years = sorted(stints["playoff_year"].unique())
    latest_label = playoff_season_label(max(playoff_years))
    print(f"Playoff years: {playoff_years[0]}–{playoff_years[-1]}  ({len(playoff_years)} seasons)")

    data: dict = {}

    # Per-season RAPM
    for yr in playoff_years:
        label = playoff_season_label(yr)
        df = stints[stints["playoff_year"] == yr].copy()
        for alpha in ALPHAS:
            print(f"  {label} alpha={alpha} ...", end=" ")
            rows = compute_rapm(df, alpha, MIN_MINUTES_SEASON)
            data[f"{label}_a{alpha}"] = rows
            print(len(rows), "players")
        data[label] = data[f"{label}_a{DEFAULT_ALPHA}"]

    generated_ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    data_json = json.dumps(data, separators=(",", ":"))

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Playoff RAPM</title>
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
    .nav {{ margin-top: 10px; display: flex; gap: 10px; flex-wrap: wrap; justify-content: center; }}
    .nav a {{
      color: #e8f4ff;
      text-decoration: none;
      border: 1px solid rgba(255,255,255,.35);
      border-radius: 7px;
      padding: 6px 10px;
      font-size: 12px;
    }}
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
      min-width: 100px;
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
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
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
    .explain {{
      background: #f8fafc;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      margin-bottom: 14px;
      font-size: 13px;
      line-height: 1.5;
    }}
    .explain h3 {{ margin: 0 0 8px; font-size: 14px; }}
    .toggle-row {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }}
    .toggle-btn {{
      background: #e8f0fa;
      border: 1px solid #c5d4e8;
      border-radius: 6px;
      padding: 5px 10px;
      font-size: 12px;
      cursor: pointer;
      color: var(--ink);
    }}
    .toggle-btn:hover {{ background: #dbe7f5; }}
    .toggle-btn.active {{ background: #0b2d4d; color: #fff; border-color: #0b2d4d; }}
    .col-hidden {{ display: none; }}
    th.group-start {{ border-left: 2px solid #c5d4e8; }}
    td.group-start {{ border-left: 2px solid #edf2f9; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Playoff RAPM</h1>
      <div class="meta">
        <span class="chip">Regularized Adjusted Plus-Minus</span>
        <span class="chip">Ridge Regression (alpha=500)</span>
        <span class="chip">Playoffs Only</span>
      </div>
      <div class="nav">
        <a href="index.html">Overview</a>
        <a href="onoff-daily.html">+/- Games</a>
        <a href="onoff.html">+/- Stats</a>
        <a href="rapm.html">RAPM</a>
        <a href="onoff-playoffs.html">+/- Playoffs</a>
        <a href="rapm-playoffs.html">Playoff RAPM</a>
        <a href="example.html">Method</a>
      </div>
    </section>

    <section class="explain">
      <h3>Playoff RAPM</h3>
      <p>RAPM computed from playoff stint data only — separate from the regular season model. Each season is estimated independently; the multi-year leaderboard uses minutes-weighted averaging across seasons.</p>
      <p>Playoff samples are much smaller than regular season (~15–20 games vs 82), so estimates are noisier. The 3PT luck adjustment can help reduce variance from hot/cold shooting runs.</p>
      <p><strong>Alpha:</strong> Ridge regularization strength. α=500 (default) applies strong shrinkage, which is especially useful for small playoff samples. α=10 shrinks less and can show more separation among players with large samples.</p>
    </section>

    <section class="card">
      <h2>Single Season</h2>
      <div class="controls">
        <label>Season
          <select id="season-filter"></select>
        </label>
        <label>Alpha
          <select id="alpha-filter">
            <option value="500">α=500 (default)</option>
            <option value="10">α=10</option>
          </select>
        </label>
        <label>Min minutes
          <input id="min-minutes" type="number" min="0" step="10" value="50" />
        </label>
        <label>Search
          <input id="search" type="text" placeholder="Player name…" style="min-width:140px" />
        </label>
      </div>
      <div class="toggle-row">
        <button class="toggle-btn" data-cols="raw" onclick="toggleCols('raw')">Show Raw (non-adjusted)</button>
      </div>
      <div class="table-wrap">
        <table id="rapm-table">
          <thead>
            <tr>
              <th class="sortable" data-table="season" data-key="rank" data-type="num">#</th>
              <th class="sortable" data-table="season" data-key="player_name" data-type="str">Player</th>
              <th class="sortable" data-table="season" data-key="team_abbr" data-type="str">Team</th>
              <th class="sortable" data-table="season" data-key="minutes" data-type="num">Min</th>
              <th class="sortable group-start" data-table="season" data-key="rapm" data-type="num" title="3PT-adjusted RAPM">RAPM Adj</th>
              <th class="sortable" data-table="season" data-key="orapm" data-type="num" title="Offensive RAPM (3PT-adjusted)">ORAPM Adj</th>
              <th class="sortable" data-table="season" data-key="drapm" data-type="num" title="Defensive RAPM (3PT-adjusted)">DRAPM Adj</th>
              <th class="sortable col-raw col-hidden group-start" data-table="season" data-key="rapm_raw" data-type="num">RAPM Raw</th>
              <th class="sortable col-raw col-hidden" data-table="season" data-key="orapm_raw" data-type="num">ORAPM Raw</th>
              <th class="sortable col-raw col-hidden" data-table="season" data-key="drapm_raw" data-type="num">DRAPM Raw</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>

      <h2 style="margin-top: 18px;">Multi-Year Leaderboard</h2>
      <div class="controls">
        <label>Start Season
          <select id="lb-start-year"></select>
        </label>
        <label>End Season
          <select id="lb-end-year"></select>
        </label>
        <label>Time Decay
          <select id="lb-decay-filter">
            <option value="1.0">None</option>
            <option value="0.85">Mild (0.85/yr)</option>
            <option value="0.70">Moderate (0.70/yr)</option>
            <option value="0.50">Strong (0.50/yr)</option>
          </select>
        </label>
        <label>Alpha
          <select id="lb-alpha-filter">
            <option value="500">α=500 (default)</option>
            <option value="10">α=10</option>
          </select>
        </label>
        <label>Min minutes
          <input id="lb-min-minutes" type="number" min="0" step="10" value="200" />
        </label>
        <label>Search
          <input id="lb-search" type="text" placeholder="Player name…" style="min-width:140px" />
        </label>
      </div>
      <div class="toggle-row">
        <button class="toggle-btn" data-cols="lb-raw" onclick="toggleCols('lb-raw')">Show Raw (non-adjusted)</button>
      </div>
      <div class="table-wrap">
        <table id="rapm-lb-table">
          <thead>
            <tr>
              <th class="sortable" data-table="lb" data-key="rank" data-type="num">#</th>
              <th class="sortable" data-table="lb" data-key="player_name" data-type="str">Player</th>
              <th class="sortable" data-table="lb" data-key="team_abbr" data-type="str">Team</th>
              <th class="sortable" data-table="lb" data-key="minutes" data-type="num">Min</th>
              <th class="sortable group-start" data-table="lb" data-key="rapm" data-type="num" title="3PT-adjusted RAPM">RAPM Adj</th>
              <th class="sortable" data-table="lb" data-key="orapm" data-type="num" title="Offensive RAPM (3PT-adjusted)">ORAPM Adj</th>
              <th class="sortable" data-table="lb" data-key="drapm" data-type="num" title="Defensive RAPM (3PT-adjusted)">DRAPM Adj</th>
              <th class="sortable col-lb-raw col-hidden group-start" data-table="lb" data-key="rapm_raw" data-type="num">RAPM Raw</th>
              <th class="sortable col-lb-raw col-hidden" data-table="lb" data-key="orapm_raw" data-type="num">ORAPM Raw</th>
              <th class="sortable col-lb-raw col-hidden" data-table="lb" data-key="drapm_raw" data-type="num">DRAPM Raw</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <p class="muted" style="padding: 0 4px;">Generated {generated_ts} &nbsp;·&nbsp; Source: data/stints_playoffs.csv</p>
  </div>

  <script>
    const DATA = {data_json};
    const LATEST_SEASON = {json.dumps(latest_label)};
    const DEFAULT_ALPHA = {DEFAULT_ALPHA};

    const sortState = {{
      season: {{ key: "rapm", dir: "desc" }},
      lb:     {{ key: "rapm", dir: "desc" }},
    }};

    function getPlayoffSeasonKeys() {{
      return Object.keys(DATA)
        .filter(k => /^\\d{{4}}-\\d{{2}}$/.test(k))
        .sort();
    }}

    function toggleCols(colGroup) {{
      const btns = document.querySelectorAll(`.toggle-btn[data-cols="${{colGroup}}"]`);
      const cols = document.querySelectorAll(`.col-${{colGroup}}`);
      const isHidden = cols[0]?.classList.contains("col-hidden");
      btns.forEach(btn => btn.classList.toggle("active", isHidden));
      cols.forEach(col => col.classList.toggle("col-hidden", !isHidden));
      btns.forEach(btn => btn.textContent = isHidden
        ? btn.textContent.replace("Show", "Hide")
        : btn.textContent.replace("Hide", "Show"));
    }}

    const fmt = (x, d=2) => (x === null || Number.isNaN(Number(x))) ? "" : Number(x).toFixed(d);
    const cls = (x) => (Number(x) > 0.05 ? "pos" : (Number(x) < -0.05 ? "neg" : ""));

    function rowHtml(r, i, colCls) {{
      return `<tr>
        <td class="muted">${{i + 1}}</td>
        <td>${{r.player_name}}</td>
        <td>${{r.team_abbr || ""}}</td>
        <td>${{Number(r.minutes || 0).toLocaleString()}}</td>
        <td class="group-start ${{cls(r.rapm)}}">${{fmt(r.rapm)}}</td>
        <td class="${{cls(r.orapm)}}">${{fmt(r.orapm)}}</td>
        <td class="${{cls(r.drapm)}}">${{fmt(r.drapm)}}</td>
        <td class="col-${{colCls}} col-hidden group-start ${{cls(r.rapm_raw)}}">${{fmt(r.rapm_raw)}}</td>
        <td class="col-${{colCls}} col-hidden ${{cls(r.orapm_raw)}}">${{fmt(r.orapm_raw)}}</td>
        <td class="col-${{colCls}} col-hidden ${{cls(r.drapm_raw)}}">${{fmt(r.drapm_raw)}}</td>
      </tr>`;
    }}

    function renderTable(tableId, rows, state, colCls) {{
      const rawVisible = document.querySelector(`.toggle-btn[data-cols="${{colCls}}"]`)?.classList.contains("active");
      const tbody = document.querySelector(`#${{tableId}} tbody`);
      const sorted = rows.slice().sort((a, b) => {{
        const dir = state.dir === "asc" ? 1 : -1;
        if (state.key === "player_name" || state.key === "team_abbr")
          return dir * String(a[state.key] || "").localeCompare(String(b[state.key] || ""));
        return dir * (Number(a[state.key] || 0) - Number(b[state.key] || 0));
      }});
      tbody.innerHTML = sorted.map((r, i) => rowHtml(r, i, colCls)).join("");
      // sync raw col visibility
      document.querySelectorAll(`#${{tableId}} .col-${{colCls}}`).forEach(
        td => td.classList.toggle("col-hidden", !rawVisible)
      );
    }}

    // ── Single season ──────────────────────────────────────
    function renderSeasonTable() {{
      const season = document.getElementById("season-filter").value;
      const alpha = document.getElementById("alpha-filter").value;
      const minMin = Number(document.getElementById("min-minutes").value || 0);
      const search = document.getElementById("search").value.toLowerCase().trim();
      const key = DATA[`${{season}}_a${{alpha}}`] ? `${{season}}_a${{alpha}}` : season;
      const rows = (DATA[key] || [])
        .filter(r => Number(r.minutes || 0) >= minMin)
        .filter(r => !search || (r.player_name || "").toLowerCase().includes(search));
      renderTable("rapm-table", rows, sortState.season, "raw");
    }}

    // ── Multi-year leaderboard ─────────────────────────────
    function aggregateLeaderboardRows(seasons, alphaVal, decayRate) {{
      const metricKeys = ["rapm", "orapm", "drapm", "rapm_raw", "orapm_raw", "drapm_raw"];
      const map = new Map();
      const sortedSeasons = [...seasons].sort();
      const mostRecentYear = sortedSeasons.length ? Number(sortedSeasons[sortedSeasons.length - 1].slice(0, 4)) : 0;

      seasons.forEach(season => {{
        const yearsBack = mostRecentYear - Number(season.slice(0, 4));
        const decayWeight = Math.pow(decayRate, yearsBack);
        const key = DATA[`${{season}}_a${{alphaVal}}`] ? `${{season}}_a${{alphaVal}}` : season;
        (DATA[key] || []).forEach(row => {{
          const rawMin = Number(row.minutes || 0);
          const wMin = rawMin * decayWeight;
          const pid = row.player_id;
          if (!map.has(pid)) {{
            map.set(pid, {{
              player_id: pid, player_name: row.player_name,
              minutes: 0, wMinutes: 0,
              team_abbrs: new Set(),
              sums: Object.fromEntries(metricKeys.map(k => [k, 0])),
            }});
          }}
          const agg = map.get(pid);
          agg.minutes += rawMin;
          agg.wMinutes += wMin;
          if (row.team_abbr) agg.team_abbrs.add(row.team_abbr);
          metricKeys.forEach(k => {{ agg.sums[k] += wMin * Number(row[k] || 0); }});
        }});
      }});

      return [...map.values()].map(agg => {{
        const out = {{
          player_id: agg.player_id,
          player_name: agg.player_name,
          team_abbr: [...agg.team_abbrs].sort().join(", "),
          minutes: agg.minutes,
        }};
        metricKeys.forEach(k => {{ out[k] = agg.wMinutes ? agg.sums[k] / agg.wMinutes : 0; }});
        return out;
      }});
    }}

    function renderLeaderboardTable() {{
      const startSeason = document.getElementById("lb-start-year").value || "1995-96";
      const endSeason   = document.getElementById("lb-end-year").value   || "2099-99";
      const decay  = Number(document.getElementById("lb-decay-filter").value || 1.0);
      const alpha  = document.getElementById("lb-alpha-filter").value;
      const minMin = Number(document.getElementById("lb-min-minutes").value || 0);
      const search = document.getElementById("lb-search").value.toLowerCase().trim();

      const seasons = getPlayoffSeasonKeys().filter(s => s >= startSeason && s <= endSeason);

      const rows = aggregateLeaderboardRows(seasons, alpha, decay)
        .filter(r => Number(r.minutes || 0) >= minMin)
        .filter(r => !search || (r.player_name || "").toLowerCase().includes(search));
      renderTable("rapm-lb-table", rows, sortState.lb, "lb-raw");
    }}

    function render() {{
      renderSeasonTable();
      renderLeaderboardTable();
    }}

    function init() {{
      const seasons = getPlayoffSeasonKeys().slice().reverse();
      const seasonSel = document.getElementById("season-filter");
      seasons.forEach(s => {{
        const o = document.createElement("option");
        o.value = s; o.textContent = s;
        seasonSel.appendChild(o);
      }});
      seasonSel.value = LATEST_SEASON;

      const sortedAsc = getPlayoffSeasonKeys();
      const lbStartSel = document.getElementById("lb-start-year");
      const lbEndSel   = document.getElementById("lb-end-year");
      sortedAsc.forEach(s => {{
        lbStartSel.appendChild(Object.assign(document.createElement("option"), {{value: s, textContent: s}}));
        lbEndSel.appendChild(Object.assign(document.createElement("option"), {{value: s, textContent: s}}));
      }});
      lbStartSel.value = sortedAsc[Math.max(0, sortedAsc.length - 3)] || "";
      lbEndSel.value   = sortedAsc[sortedAsc.length - 1] || "";

      ["season-filter","alpha-filter","min-minutes","search"].forEach(id =>
        document.getElementById(id).addEventListener("input", renderSeasonTable)
      );
      ["lb-decay-filter","lb-alpha-filter","lb-min-minutes","lb-search"].forEach(id =>
        document.getElementById(id).addEventListener("input", renderLeaderboardTable)
      );
      ["lb-start-year","lb-end-year"].forEach(id =>
        document.getElementById(id).addEventListener("change", renderLeaderboardTable)
      );

      document.querySelectorAll("thead th.sortable").forEach(th => {{
        th.addEventListener("click", () => {{
          const tbl = th.dataset.table || "season";
          const key = th.dataset.key;
          if (key === "rank") return;
          const state = sortState[tbl];
          if (state.key === key) {{
            state.dir = state.dir === "desc" ? "asc" : "desc";
          }} else {{
            state.key = key;
            state.dir = (key === "player_name" || key === "team_abbr") ? "asc" : "desc";
          }}
          tbl === "lb" ? renderLeaderboardTable() : renderSeasonTable();
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
