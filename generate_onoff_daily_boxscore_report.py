"""Generate daily game-by-game boxscore page for on/off and adjusted metrics."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data")
BOX_PATH = DATA_DIR / "player_daily_boxscore.csv"
GAMES_PATH = DATA_DIR / "adjusted_games.csv"
OUTPUT_DATA_PATH = DATA_DIR / "onoff_daily_boxscores.html"
OUTPUT_SITE_PATH = Path("onoff-daily.html")

TEAM_ID_TO_ABBR = {
    1610612737: "ATL",
    1610612738: "BOS",
    1610612751: "BKN",
    1610612766: "CHA",
    1610612741: "CHI",
    1610612739: "CLE",
    1610612742: "DAL",
    1610612743: "DEN",
    1610612765: "DET",
    1610612744: "GSW",
    1610612745: "HOU",
    1610612754: "IND",
    1610612746: "LAC",
    1610612747: "LAL",
    1610612763: "MEM",
    1610612748: "MIA",
    1610612749: "MIL",
    1610612750: "MIN",
    1610612740: "NOP",
    1610612752: "NYK",
    1610612760: "OKC",
    1610612753: "ORL",
    1610612755: "PHI",
    1610612756: "PHX",
    1610612757: "POR",
    1610612758: "SAC",
    1610612759: "SAS",
    1610612761: "TOR",
    1610612762: "UTA",
    1610612764: "WAS",
}
TEAM_ABBR_TO_ID = {abbr: tid for tid, abbr in TEAM_ID_TO_ABBR.items()}


def _prepare_box() -> pd.DataFrame:
    if not BOX_PATH.exists():
        raise FileNotFoundError(f"Missing {BOX_PATH}")
    df = pd.read_csv(BOX_PATH, dtype={"game_id": str, "player_id": int})
    df["game_id"] = df["game_id"].astype(str).str.lstrip("0")
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
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _prepare_games_meta() -> dict[str, dict]:
    if not GAMES_PATH.exists():
        return {}
    g = pd.read_csv(GAMES_PATH, dtype={"game_id": str})
    g["game_id"] = g["game_id"].astype(str).str.lstrip("0")
    out: dict[str, dict] = {}
    for _, r in g.iterrows():
        gid = str(r["game_id"])
        away_abbr = str(r.get("away_team", ""))
        home_abbr = str(r.get("home_team", ""))
        out[gid] = {
            "date": str(r.get("date", "")),
            "away_abbr": away_abbr,
            "home_abbr": home_abbr,
            "away_id": TEAM_ABBR_TO_ID.get(away_abbr),
            "home_id": TEAM_ABBR_TO_ID.get(home_abbr),
            "away_pts_actual": r.get("away_pts_actual"),
            "home_pts_actual": r.get("home_pts_actual"),
            "away_pts_adj": r.get("away_pts_adj"),
            "home_pts_adj": r.get("home_pts_adj"),
        }
    return out


def generate_daily_boxscores_report() -> Path:
    box = _prepare_box()
    meta = _prepare_games_meta()

    latest_date = box["date"].max()
    all_dates = sorted(box["date"].unique().tolist())

    records = []
    for _, r in box.iterrows():
        records.append(
            {
                "date": r["date"],
                "game_id": str(r["game_id"]),
                "team_id": int(r["team_id"]),
                "team_abbr": TEAM_ID_TO_ABBR.get(int(r["team_id"]), str(int(r["team_id"]))),
                "player_id": int(r["player_id"]),
                "player_name": str(r["player_name"]),
                "minutes_on": float(r["minutes_on"]) if pd.notna(r["minutes_on"]) else None,
                "plus_minus_actual": float(r["plus_minus_actual"]) if pd.notna(r["plus_minus_actual"]) else None,
                "plus_minus_adjusted": float(r["plus_minus_adjusted"]) if pd.notna(r["plus_minus_adjusted"]) else None,
                "plus_minus_delta": float(r["plus_minus_delta"]) if pd.notna(r["plus_minus_delta"]) else None,
                "on_off_actual": float(r["on_off_actual"]) if pd.notna(r["on_off_actual"]) else None,
                "on_off_adjusted": float(r["on_off_adjusted"]) if pd.notna(r["on_off_adjusted"]) else None,
                "on_off_delta": float(r["on_off_delta"]) if pd.notna(r["on_off_delta"]) else None,
            }
        )

    page_title = "NBA Daily Adjusted On-Off Boxscores"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

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
      --line: #d9e2ef;
      --ink: #172031;
      --muted: #5b6778;
      --good: #0f766e;
      --bad: #b91c1c;
      --top: #0b2d4d;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Arial, sans-serif;
      color: var(--ink);
      background: linear-gradient(180deg, #eaf2ff 0%, #f8fbff 28%, #f2f6fb 100%);
    }}
    .wrap {{ max-width: 1500px; margin: 0 auto; padding: 18px; }}
    .hero {{
      background: linear-gradient(120deg, #0b2d4d 0%, #133f6a 100%);
      color: #f8fbff;
      border: 1px solid #2a5278;
      border-radius: 14px;
      padding: 18px 20px;
      margin-bottom: 14px;
    }}
    h1 {{ margin: 0; font-size: 28px; }}
    .muted {{ color: var(--muted); }}
    .nav {{ margin-top: 10px; display: flex; gap: 10px; flex-wrap: wrap; }}
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
      margin-bottom: 14px;
      box-shadow: 0 3px 10px rgba(22, 39, 66, 0.06);
    }}
    .controls {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 10px; }}
    label {{ font-size: 12px; color: var(--muted); display: grid; gap: 4px; }}
    select, input {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 7px 9px;
      font-size: 13px;
      min-width: 120px;
      background: #fff;
    }}
    .games {{
      display: grid;
      gap: 8px;
      grid-template-columns: repeat(auto-fit, minmax(700px, 1fr));
      align-items: start;
    }}
    .game {{
      border: 2px solid #9cb5d3;
      border-radius: 10px;
      overflow: hidden;
      background: #fff;
      box-shadow: 0 2px 8px rgba(22, 39, 66, 0.08);
    }}
    .game-head {{
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      background: #edf4fe;
      display: flex;
      justify-content: flex-start;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      font-weight: 600;
      color: #143255;
    }}
    .teams {{
      display: grid;
      gap: 4px;
      grid-template-columns: 1fr 1fr;
      padding: 6px;
    }}
    .team-block {{
      border: 2px solid #c1d1e6;
      border-radius: 8px;
      overflow: hidden;
    }}
    .team-title {{
      padding: 8px 10px;
      background: #f6f9ff;
      border-bottom: 1px solid var(--line);
      font-weight: 600;
    }}
    .table-wrap {{ overflow: auto; max-height: 460px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 0;
      font-size: 12px;
      table-layout: auto;
    }}
    th, td {{
      padding: 5px 6px;
      border-bottom: 1px solid #edf2f8;
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child {{
      text-align: left;
      padding-right: 2px;
    }}
    th:nth-child(2), td:nth-child(2) {{ width: 34px; min-width: 34px; padding-left: 2px; }}
    th:nth-child(3), td:nth-child(3) {{ width: 36px; min-width: 36px; }}
    th:nth-child(4), td:nth-child(4) {{ width: 44px; min-width: 44px; }}
    th:nth-child(5), td:nth-child(5) {{ width: 38px; min-width: 38px; }}
    th:nth-child(6), td:nth-child(6) {{
      width: 38px;
      min-width: 38px;
      padding-left: 2px;
      padding-right: 4px;
    }}
    thead th {{ position: sticky; top: 0; background: #eff4fb; color: #173253; z-index: 2; }}
    .key-th {{ font-weight: 800; color: #102a4a; }}
    .key-col {{ font-weight: 800; }}
    .pos {{ color: var(--good); font-weight: 600; }}
    .neg {{ color: var(--bad); font-weight: 600; }}
    @media (max-width: 1500px) {{
      .games {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 1100px) {{
      .teams {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>{page_title}</h1>
      <div class="nav">
        <a href="index.html">Main 3PT Luck Page</a>
        <a href="onoff.html">On-Off Explorer</a>
      </div>
    </section>

    <section class="card">
      <div class="controls">
        <label>Date
          <select id="date"></select>
        </label>
        <label>Min Minutes
          <input id="min-min" type="number" min="0" step="0.1" value="0" />
        </label>
      </div>
      <div class="muted">
        <strong>What is this?</strong> Daily plus-minus is incredibly noisy, and no adjustment can fully solve that.
        This page uses a 3PT expectation model to recalculate each player's plus-minus as if the shooting results
        during their specific stints matched expectation. Like the base model, it adjusts for who shot and shot
        difficulty, and includes a mitigant for the hypothetical change in ORB opportunities.
        <strong>Key columns:</strong> <strong>PM Adj</strong> and <strong>OnA</strong>.
      </div>
    </section>

    <section class="games" id="games"></section>

    <p class="muted">Generated {ts} | Source: `data/player_daily_boxscore.csv`.</p>
  </div>
  <script>
    const ROWS = {json.dumps(records)};
    const DATES = {json.dumps(all_dates)};
    const META = {json.dumps(meta)};
    const LATEST = {json.dumps(latest_date)};

    const fmt = (x, d=0) => (x === null || Number.isNaN(Number(x))) ? "" : Number(x).toFixed(d);
    const cls = (x) => (x > 0 ? "pos" : (x < 0 ? "neg" : ""));

    function render() {{
      const date = document.getElementById("date").value;
      const minMin = Number(document.getElementById("min-min").value || 0);
      const gamesEl = document.getElementById("games");
      gamesEl.innerHTML = "";

      const rows = ROWS.filter(r => r.date === date && Number(r.minutes_on || 0) >= minMin);
      const gameIds = [...new Set(rows.map(r => r.game_id))].sort();

      gameIds.forEach(gid => {{
        const gRows = rows.filter(r => r.game_id === gid);
        const teamIds = [...new Set(gRows.map(r => r.team_id))];
        const m = META[gid] || {{}};
        let leftId = teamIds[0];
        let rightId = teamIds[1];
        if (m.away_id && m.home_id) {{
          leftId = m.away_id;
          rightId = m.home_id;
        }}
        const leftRows = gRows.filter(r => r.team_id === leftId)
          .sort((a,b) => Number(b.minutes_on||0)-Number(a.minutes_on||0));
        const rightRows = gRows.filter(r => r.team_id === rightId)
          .sort((a,b) => Number(b.minutes_on||0)-Number(a.minutes_on||0));

        const leftAbbr = m.away_abbr || (leftRows[0] ? leftRows[0].team_abbr : String(leftId || ""));
        const rightAbbr = m.home_abbr || (rightRows[0] ? rightRows[0].team_abbr : String(rightId || ""));
        const scoreStr = (m.away_pts_actual !== undefined && m.home_pts_actual !== undefined)
          ? `${{leftAbbr}} ${{Number(m.away_pts_actual)}} at ${{rightAbbr}} ${{Number(m.home_pts_actual)}}`
          : `${{leftAbbr}} at ${{rightAbbr}}`;

        const game = document.createElement("article");
        game.className = "game";
        game.innerHTML = `
          <div class="game-head">
            <div>${{scoreStr}}</div>
          </div>
          <div class="teams">
            <section class="team-block">
              <div class="team-title">${{leftAbbr}}</div>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th title="Player name">Player</th>
                      <th title="Minutes played">Min</th>
                      <th title="Actual plus-minus while the player was on court (official boxscore)">PM</th>
                      <th class="key-th" title="Adjusted plus-minus while on court under the 3PT luck-adjusted model">PM Adj</th>
                      <th title="Actual on-off differential: on-court plus-minus minus off-court plus-minus">OnOff</th>
                      <th class="key-th" title="Adjusted on-off differential under the 3PT luck-adjusted model">OnA</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${{
                      leftRows.map(r => `
                        <tr>
                          <td>${{r.player_name}}</td>
                          <td>${{fmt(r.minutes_on)}}</td>
                          <td class="${{cls(r.plus_minus_actual)}}">${{fmt(r.plus_minus_actual)}}</td>
                          <td class="key-col ${{cls(r.plus_minus_adjusted)}}">${{fmt(r.plus_minus_adjusted)}}</td>
                          <td class="${{cls(r.on_off_actual)}}">${{fmt(r.on_off_actual)}}</td>
                          <td class="key-col ${{cls(r.on_off_adjusted)}}">${{fmt(r.on_off_adjusted)}}</td>
                        </tr>`
                      ).join("")
                    }}
                  </tbody>
                </table>
              </div>
            </section>
            <section class="team-block">
              <div class="team-title">${{rightAbbr}}</div>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th title="Player name">Player</th>
                      <th title="Minutes played">Min</th>
                      <th title="Actual plus-minus while the player was on court (official boxscore)">PM</th>
                      <th class="key-th" title="Adjusted plus-minus while on court under the 3PT luck-adjusted model">PM Adj</th>
                      <th title="Actual on-off differential: on-court plus-minus minus off-court plus-minus">OnOff</th>
                      <th class="key-th" title="Adjusted on-off differential under the 3PT luck-adjusted model">OnA</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${{
                      rightRows.map(r => `
                        <tr>
                          <td>${{r.player_name}}</td>
                          <td>${{fmt(r.minutes_on)}}</td>
                          <td class="${{cls(r.plus_minus_actual)}}">${{fmt(r.plus_minus_actual)}}</td>
                          <td class="key-col ${{cls(r.plus_minus_adjusted)}}">${{fmt(r.plus_minus_adjusted)}}</td>
                          <td class="${{cls(r.on_off_actual)}}">${{fmt(r.on_off_actual)}}</td>
                          <td class="key-col ${{cls(r.on_off_adjusted)}}">${{fmt(r.on_off_adjusted)}}</td>
                        </tr>`
                      ).join("")
                    }}
                  </tbody>
                </table>
              </div>
            </section>
          </div>`;
        gamesEl.appendChild(game);
      }});
    }}

    function init() {{
      const dateSel = document.getElementById("date");
      DATES.slice().reverse().forEach(d => {{
        const o = document.createElement("option");
        o.value = d;
        o.textContent = d;
        dateSel.appendChild(o);
      }});
      dateSel.value = LATEST;
      dateSel.addEventListener("input", render);
      document.getElementById("min-min").addEventListener("input", render);
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
    generate_daily_boxscores_report()
