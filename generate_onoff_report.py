"""Generate HTML report for adjusted plus-minus / on-off metrics."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data")
ONOFF_PATH = DATA_DIR / "adjusted_onoff.csv"
HISTORY_PATH = DATA_DIR / "player_onoff_history.csv"
BOXSCORE_PATH = DATA_DIR / "player_daily_boxscore.csv"
OUTPUT_DATA_PATH = DATA_DIR / "onoff_report.html"
OUTPUT_SITE_PATH = Path("onoff.html")


def _prepare_onoff_df() -> pd.DataFrame:
    if not ONOFF_PATH.exists():
        raise FileNotFoundError(f"Missing {ONOFF_PATH}")
    df = pd.read_csv(ONOFF_PATH, dtype={"game_id": str, "player_id": int})
    df["date"] = df["date"].astype(str)
    numeric_cols = [
        "minutes_on",
        "on_diff",
        "off_diff",
        "on_off_diff",
        "on_diff_adj",
        "off_diff_adj",
        "on_off_diff_adj",
        "on_diff_reconstructed",
        "off_diff_reconstructed",
        "on_off_diff_reconstructed",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _prepare_history_df() -> pd.DataFrame:
    if not HISTORY_PATH.exists():
        raise FileNotFoundError(f"Missing {HISTORY_PATH}")
    df = pd.read_csv(HISTORY_PATH)
    numeric_cols = [
        "games",
        "minutes_on_total",
        "minutes_on_avg",
        "on_off_diff_avg",
        "on_off_diff_adj_avg",
        "on_diff_avg",
        "off_diff_avg",
        "on_diff_adj_avg",
        "off_diff_adj_avg",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _prepare_boxscore_df() -> pd.DataFrame:
    if not BOXSCORE_PATH.exists():
        raise FileNotFoundError(f"Missing {BOXSCORE_PATH}")
    df = pd.read_csv(BOXSCORE_PATH, dtype={"game_id": str, "player_id": int})
    df["date"] = df["date"].astype(str)
    numeric_cols = [
        "minutes_on",
        "plus_minus_actual",
        "plus_minus_adjusted",
        "plus_minus_delta",
        "on_off_actual",
        "on_off_adjusted",
        "on_off_delta",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _to_records(df: pd.DataFrame, cols: list[str]) -> list[dict]:
    out = []
    for _, r in df[cols].iterrows():
        row = {}
        for c in cols:
            v = r[c]
            if pd.isna(v):
                row[c] = None
            elif isinstance(v, (int, float)):
                row[c] = float(v)
            else:
                row[c] = str(v)
        out.append(row)
    return out


def generate_onoff_report() -> Path:
    onoff_df = _prepare_onoff_df()
    hist_df = _prepare_history_df()
    box_df = _prepare_boxscore_df()

    latest_date = onoff_df["date"].max()
    date_values = sorted(onoff_df["date"].unique().tolist())
    team_values = sorted(onoff_df["team_id"].dropna().astype(int).unique().tolist())

    daily_cols = [
        "date",
        "game_id",
        "team_id",
        "player_id",
        "player_name",
        "minutes_on",
        "on_diff",
        "off_diff",
        "on_off_diff",
        "on_diff_adj",
        "off_diff_adj",
        "on_off_diff_adj",
    ]
    hist_cols = [
        "player_id",
        "player_name",
        "latest_team_id",
        "games",
        "minutes_on_total",
        "minutes_on_avg",
        "on_off_diff_avg",
        "on_off_diff_adj_avg",
        "on_diff_avg",
        "off_diff_avg",
        "on_diff_adj_avg",
        "off_diff_adj_avg",
        "first_game_date",
        "last_game_date",
    ]
    player_games_cols = [
        "date",
        "game_id",
        "team_id",
        "player_id",
        "player_name",
        "minutes_on",
        "on_diff",
        "on_off_diff",
        "on_diff_adj",
        "on_off_diff_adj",
    ]
    box_cols = [
        "date",
        "game_id",
        "team_id",
        "player_id",
        "player_name",
        "minutes_on",
        "plus_minus_actual",
        "plus_minus_adjusted",
        "plus_minus_delta",
        "on_off_actual",
        "on_off_adjusted",
        "on_off_delta",
    ]

    daily_records = _to_records(onoff_df, daily_cols)
    history_records = _to_records(hist_df, hist_cols)
    player_game_records = _to_records(
        onoff_df.sort_values(["date", "game_id"], ascending=[False, False]),
        player_games_cols,
    )
    box_records = _to_records(
        box_df.sort_values(["date", "game_id"], ascending=[False, False]),
        box_cols,
    )

    page_title = "NBA Adjusted On-Off and Plus-Minus"
    generated_ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{page_title}</title>
  <style>
    :root {{
      --bg: #f2f6fb;
      --card: #ffffff;
      --line: #d6e1ef;
      --ink: #192231;
      --muted: #5b6778;
      --accent: #0f766e;
      --accent2: #b45309;
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
    h1 {{ margin: 0; font-size: 28px; letter-spacing: 0.3px; }}
    h2 {{ margin: 0 0 10px; font-size: 20px; }}
    .muted {{ color: var(--muted); }}
    .meta {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 8px; }}
    .chip {{
      background: rgba(255,255,255,0.14);
      border: 1px solid rgba(255,255,255,0.22);
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
    }}
    .nav {{ margin-top: 10px; display: flex; gap: 10px; flex-wrap: wrap; }}
    .nav a {{
      color: #e5f6ff;
      text-decoration: none;
      border: 1px solid rgba(255,255,255,0.35);
      padding: 6px 10px;
      border-radius: 7px;
      font-size: 12px;
    }}
    .grid {{ display: grid; gap: 14px; }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px;
      box-shadow: 0 3px 12px rgba(23, 38, 62, 0.06);
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
      max-height: 560px;
      background: #fff;
    }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1000px; font-size: 12px; }}
    th, td {{
      border-bottom: 1px solid #edf2f9;
      padding: 7px 8px;
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child,
    th:nth-child(2), td:nth-child(2),
    th:nth-child(3), td:nth-child(3) {{ text-align: left; }}
    thead th {{
      position: sticky;
      top: 0;
      z-index: 2;
      background: #edf3fc;
      color: #123154;
    }}
    .pos {{ color: var(--accent); font-weight: 600; }}
    .neg {{ color: #b91c1c; font-weight: 600; }}
    .subtle {{ color: var(--muted); font-size: 11px; }}
    @media (max-width: 900px) {{
      h1 {{ font-size: 22px; }}
      .table-wrap {{ max-height: 420px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>{page_title}</h1>
      <div class="meta">
        <span class="chip">Player-games: {len(onoff_df):,}</span>
        <span class="chip">Games: {onoff_df['game_id'].nunique():,}</span>
        <span class="chip">Players: {onoff_df['player_id'].nunique():,}</span>
        <span class="chip">Latest date: {latest_date}</span>
      </div>
      <div class="nav">
        <a href="index.html">Main 3PT Luck Page</a>
        <a href="#daily">Daily View</a>
        <a href="#boxscore">Daily Boxscore</a>
        <a href="#history">Player History</a>
        <a href="#player-games">Player Game Log</a>
      </div>
    </section>

    <section id="boxscore" class="card">
      <h2>Player Daily Boxscore Model (Actual vs Adjusted)</h2>
      <div class="controls">
        <label>Date
          <select id="box-date"></select>
        </label>
        <label>Team ID
          <select id="box-team"></select>
        </label>
        <label>Player name contains
          <input id="box-player" type="text" placeholder="e.g. Maxey" />
        </label>
        <label>Min minutes
          <input id="box-minutes" type="number" min="0" step="0.1" value="0" />
        </label>
      </div>
      <div class="subtle">Source: `data/player_daily_boxscore.csv`.</div>
      <div class="table-wrap">
        <table id="box-table">
          <thead>
            <tr>
              <th>Player</th><th>Team</th><th>Game</th><th>Min</th>
              <th>PM Actual</th><th>PM Adj</th><th>PM Delta</th>
              <th>On-Off Actual</th><th>On-Off Adj</th><th>On-Off Delta</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section id="daily" class="card">
      <h2>Daily Adjusted Plus-Minus / On-Off</h2>
      <div class="controls">
        <label>Date
          <select id="daily-date"></select>
        </label>
        <label>Team ID
          <select id="daily-team"></select>
        </label>
        <label>Player name contains
          <input id="daily-player" type="text" placeholder="e.g. Maxey" />
        </label>
        <label>Min minutes
          <input id="daily-minutes" type="number" min="0" step="0.1" value="0" />
        </label>
      </div>
      <div class="subtle">`on_diff` is official boxscore plus-minus. Adjusted columns are model-based counterfactuals.</div>
      <div class="table-wrap">
        <table id="daily-table">
          <thead>
            <tr>
              <th>Player</th><th>Team</th><th>Game</th><th>Min</th>
              <th>On +/-</th><th>Off +/-</th><th>On-Off</th>
              <th>Adj On +/-</th><th>Adj Off +/-</th><th>Adj On-Off</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section id="history" class="card">
      <h2>Player Season History</h2>
      <div class="controls">
        <label>Player name contains
          <input id="hist-player" type="text" placeholder="e.g. Nembhard" />
        </label>
        <label>Min games
          <input id="hist-min-games" type="number" min="0" step="1" value="10" />
        </label>
        <label>Min total minutes
          <input id="hist-min-minutes" type="number" min="0" step="1" value="100" />
        </label>
        <label>Sort by
          <select id="hist-sort">
            <option value="on_off_diff_adj_avg">Adj On-Off Avg</option>
            <option value="on_off_diff_avg">Raw On-Off Avg</option>
            <option value="minutes_on_total">Total Minutes</option>
            <option value="games">Games</option>
          </select>
        </label>
      </div>
      <div class="table-wrap">
        <table id="hist-table">
          <thead>
            <tr>
              <th>Player</th><th>Team</th><th>Games</th><th>Min Total</th><th>Min Avg</th>
              <th>Raw On-Off Avg</th><th>Adj On-Off Avg</th>
              <th>Raw On +/- Avg</th><th>Adj On +/- Avg</th>
              <th>First</th><th>Last</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section id="player-games" class="card">
      <h2>Player Game Log Explorer</h2>
      <div class="controls">
        <label>Player
          <select id="pg-player"></select>
        </label>
      </div>
      <div class="table-wrap">
        <table id="pg-table">
          <thead>
            <tr>
              <th>Date</th><th>Game</th><th>Team</th><th>Minutes</th>
              <th>On +/-</th><th>On-Off</th><th>Adj On +/-</th><th>Adj On-Off</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <p class="muted">Generated {generated_ts} | Source files: `data/adjusted_onoff.csv`, `data/player_onoff_history.csv`.</p>
  </div>
  <script>
    const DAILY = {json.dumps(daily_records)};
    const BOX = {json.dumps(box_records)};
    const HISTORY = {json.dumps(history_records)};
    const PLAYER_GAMES = {json.dumps(player_game_records)};
    const DATE_VALUES = {json.dumps(date_values)};
    const TEAM_VALUES = {json.dumps(team_values)};
    const LATEST_DATE = {json.dumps(latest_date)};

    const fmt = (x, d = 1) => (x === null || Number.isNaN(Number(x))) ? "" : Number(x).toFixed(d);
    const cls = (x) => (x > 0 ? "pos" : (x < 0 ? "neg" : ""));

    function fillSelect(el, vals, includeAll = true) {{
      if (includeAll) {{
        const o = document.createElement("option");
        o.value = ""; o.textContent = "All";
        el.appendChild(o);
      }}
      vals.forEach(v => {{
        const o = document.createElement("option");
        o.value = String(v);
        o.textContent = String(v);
        el.appendChild(o);
      }});
    }}

    function renderDaily() {{
      const date = document.getElementById("daily-date").value;
      const team = document.getElementById("daily-team").value;
      const name = document.getElementById("daily-player").value.toLowerCase().trim();
      const minMinutes = Number(document.getElementById("daily-minutes").value || 0);
      const tbody = document.querySelector("#daily-table tbody");
      tbody.innerHTML = "";

      const rows = DAILY
        .filter(r => !date || r.date === date)
        .filter(r => !team || String(r.team_id) === team)
        .filter(r => !name || r.player_name.toLowerCase().includes(name))
        .filter(r => Number(r.minutes_on || 0) >= minMinutes)
        .sort((a, b) => Number(b.on_off_diff_adj || 0) - Number(a.on_off_diff_adj || 0));

      rows.forEach(r => {{
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${{r.player_name}}</td>
          <td>${{r.team_id}}</td>
          <td>${{r.game_id}}</td>
          <td>${{fmt(r.minutes_on, 2)}}</td>
          <td class="${{cls(r.on_diff)}}">${{fmt(r.on_diff, 1)}}</td>
          <td class="${{cls(r.off_diff)}}">${{fmt(r.off_diff, 1)}}</td>
          <td class="${{cls(r.on_off_diff)}}">${{fmt(r.on_off_diff, 1)}}</td>
          <td class="${{cls(r.on_diff_adj)}}">${{fmt(r.on_diff_adj, 2)}}</td>
          <td class="${{cls(r.off_diff_adj)}}">${{fmt(r.off_diff_adj, 2)}}</td>
          <td class="${{cls(r.on_off_diff_adj)}}">${{fmt(r.on_off_diff_adj, 2)}}</td>`;
        tbody.appendChild(tr);
      }});
    }}

    function renderBoxscore() {{
      const date = document.getElementById("box-date").value;
      const team = document.getElementById("box-team").value;
      const name = document.getElementById("box-player").value.toLowerCase().trim();
      const minMinutes = Number(document.getElementById("box-minutes").value || 0);
      const tbody = document.querySelector("#box-table tbody");
      tbody.innerHTML = "";

      const rows = BOX
        .filter(r => !date || r.date === date)
        .filter(r => !team || String(r.team_id) === team)
        .filter(r => !name || r.player_name.toLowerCase().includes(name))
        .filter(r => Number(r.minutes_on || 0) >= minMinutes)
        .sort((a, b) => Number(b.plus_minus_delta || 0) - Number(a.plus_minus_delta || 0));

      rows.forEach(r => {{
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${{r.player_name}}</td>
          <td>${{r.team_id}}</td>
          <td>${{r.game_id}}</td>
          <td>${{fmt(r.minutes_on, 2)}}</td>
          <td class="${{cls(r.plus_minus_actual)}}">${{fmt(r.plus_minus_actual, 1)}}</td>
          <td class="${{cls(r.plus_minus_adjusted)}}">${{fmt(r.plus_minus_adjusted, 2)}}</td>
          <td class="${{cls(r.plus_minus_delta)}}">${{fmt(r.plus_minus_delta, 2)}}</td>
          <td class="${{cls(r.on_off_actual)}}">${{fmt(r.on_off_actual, 1)}}</td>
          <td class="${{cls(r.on_off_adjusted)}}">${{fmt(r.on_off_adjusted, 2)}}</td>
          <td class="${{cls(r.on_off_delta)}}">${{fmt(r.on_off_delta, 2)}}</td>`;
        tbody.appendChild(tr);
      }});
    }}

    function renderHistory() {{
      const name = document.getElementById("hist-player").value.toLowerCase().trim();
      const minGames = Number(document.getElementById("hist-min-games").value || 0);
      const minMin = Number(document.getElementById("hist-min-minutes").value || 0);
      const sortBy = document.getElementById("hist-sort").value;
      const tbody = document.querySelector("#hist-table tbody");
      tbody.innerHTML = "";

      const rows = HISTORY
        .filter(r => !name || r.player_name.toLowerCase().includes(name))
        .filter(r => Number(r.games || 0) >= minGames)
        .filter(r => Number(r.minutes_on_total || 0) >= minMin)
        .sort((a, b) => Number(b[sortBy] || 0) - Number(a[sortBy] || 0));

      rows.forEach(r => {{
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${{r.player_name}}</td>
          <td>${{r.latest_team_id || ""}}</td>
          <td>${{fmt(r.games, 0)}}</td>
          <td>${{fmt(r.minutes_on_total, 1)}}</td>
          <td>${{fmt(r.minutes_on_avg, 2)}}</td>
          <td class="${{cls(r.on_off_diff_avg)}}">${{fmt(r.on_off_diff_avg, 2)}}</td>
          <td class="${{cls(r.on_off_diff_adj_avg)}}">${{fmt(r.on_off_diff_adj_avg, 2)}}</td>
          <td class="${{cls(r.on_diff_avg)}}">${{fmt(r.on_diff_avg, 2)}}</td>
          <td class="${{cls(r.on_diff_adj_avg)}}">${{fmt(r.on_diff_adj_avg, 2)}}</td>
          <td>${{r.first_game_date || ""}}</td>
          <td>${{r.last_game_date || ""}}</td>`;
        tbody.appendChild(tr);
      }});
    }}

    function renderPlayerGames() {{
      const pid = document.getElementById("pg-player").value;
      const tbody = document.querySelector("#pg-table tbody");
      tbody.innerHTML = "";
      if (!pid) return;
      const rows = PLAYER_GAMES.filter(r => String(r.player_id) === pid);
      rows.forEach(r => {{
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${{r.date}}</td>
          <td>${{r.game_id}}</td>
          <td>${{r.team_id}}</td>
          <td>${{fmt(r.minutes_on, 2)}}</td>
          <td class="${{cls(r.on_diff)}}">${{fmt(r.on_diff, 1)}}</td>
          <td class="${{cls(r.on_off_diff)}}">${{fmt(r.on_off_diff, 1)}}</td>
          <td class="${{cls(r.on_diff_adj)}}">${{fmt(r.on_diff_adj, 2)}}</td>
          <td class="${{cls(r.on_off_diff_adj)}}">${{fmt(r.on_off_diff_adj, 2)}}</td>`;
        tbody.appendChild(tr);
      }});
    }}

    function init() {{
      const dailyDate = document.getElementById("daily-date");
      const dailyTeam = document.getElementById("daily-team");
      const boxDate = document.getElementById("box-date");
      const boxTeam = document.getElementById("box-team");
      const pgPlayer = document.getElementById("pg-player");

      fillSelect(dailyDate, DATE_VALUES.slice().reverse(), false);
      fillSelect(dailyTeam, TEAM_VALUES, true);
      fillSelect(boxDate, DATE_VALUES.slice().reverse(), false);
      fillSelect(boxTeam, TEAM_VALUES, true);
      dailyDate.value = LATEST_DATE;
      boxDate.value = LATEST_DATE;

      const players = [...new Map(PLAYER_GAMES.map(r => [String(r.player_id), r.player_name])).entries()]
        .map(([player_id, player_name]) => ({{ player_id, player_name }}))
        .sort((a, b) => a.player_name.localeCompare(b.player_name));
      const empty = document.createElement("option");
      empty.value = "";
      empty.textContent = "Select a player";
      pgPlayer.appendChild(empty);
      players.forEach(p => {{
        const o = document.createElement("option");
        o.value = p.player_id;
        o.textContent = `${{p.player_name}} (${{p.player_id}})`;
        pgPlayer.appendChild(o);
      }});

      ["daily-date", "daily-team", "daily-player", "daily-minutes"].forEach(id =>
        document.getElementById(id).addEventListener("input", renderDaily)
      );
      ["box-date", "box-team", "box-player", "box-minutes"].forEach(id =>
        document.getElementById(id).addEventListener("input", renderBoxscore)
      );
      ["hist-player", "hist-min-games", "hist-min-minutes", "hist-sort"].forEach(id =>
        document.getElementById(id).addEventListener("input", renderHistory)
      );
      pgPlayer.addEventListener("input", renderPlayerGames);

      renderDaily();
      renderBoxscore();
      renderHistory();
      renderPlayerGames();
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
