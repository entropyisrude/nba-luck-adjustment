from __future__ import annotations

import json
import unicodedata
from pathlib import Path

import duckdb


ROOT = Path("/mnt/c/Users/Dave/Downloads/nba-onoff-publish")
DB_PATH = ROOT / "data" / "nba_analytics.duckdb"
OUTPUT_PATH = ROOT / "lineup-2man-audit.html"

# team, players, minutes, pf/100, pa/100, net
EXTERNAL_ROWS = [
    ("OKC", "Alex Caruso / Isaiah Hartenstein", 211, 123.1, 90.1, 33.1),
    ("BOS", "Derrick White / Hugo Gonzalez", 499, 120.4, 92.5, 27.9),
    ("CHA", "Grant Williams / Jeff Green", 245, 122.3, 94.4, 27.9),
    ("SAS", "Victor Wembanyama / Dylan Harper", 475, 119.2, 92.0, 27.2),
    ("NYK", "Karl-Anthony Towns / Jose Alvarado", 200, 128.3, 102.5, 25.9),
    ("OKC", "Alex Caruso / Ajay Mitchell", 365, 120.3, 95.6, 24.7),
    ("ATL", "CJ McCollum / Dyson Daniels", 487, 119.6, 95.7, 23.9),
    ("GSW", "Jimmy Butler III / De'Anthony Melton", 244, 123.9, 101.9, 21.9),
    ("OKC", "Isaiah Hartenstein / Isaiah Joe", 290, 117.0, 95.3, 21.7),
    ("OKC", "Alex Caruso / Shai Gilgeous-Alexander", 526, 120.9, 99.6, 21.3),
    ("CHA", "Jeff Green / Moussa Diabate", 298, 124.4, 103.2, 21.3),
    ("OKC", "Shai Gilgeous-Alexander / Jalen Williams", 665, 120.9, 100.0, 20.9),
    ("OKC", "Alex Caruso / Chet Holmgren", 370, 117.7, 96.9, 20.8),
    ("CHA", "Jeff Green / Kon Knueppel", 293, 121.1, 100.3, 20.8),
    ("SAS", "Devin Vassell / Victor Wembanyama", 862, 122.2, 101.5, 20.7),
    ("TOR", "Jamal Shead / Jamison Battle", 223, 121.9, 101.7, 20.2),
    ("OKC", "Alex Caruso / Isaiah Joe", 350, 121.3, 101.6, 19.7),
    ("DEN", "Aaron Gordon / Nikola Jokic", 456, 127.4, 107.9, 19.6),
    ("BOS", "Anfernee Simons / Hugo Gonzalez", 442, 122.7, 103.0, 19.6),
    ("OKC", "Isaiah Hartenstein / Chet Holmgren", 611, 119.9, 100.4, 19.5),
    ("CHA", "Grant Williams / Sion James", 238, 116.8, 97.3, 19.5),
    ("CHA", "Grant Williams / LaMelo Ball", 250, 125.7, 106.5, 19.3),
    ("SAS", "Julian Champagnie / Victor Wembanyama", 1072, 121.0, 101.9, 19.2),
    ("CHA", "LaMelo Ball / Moussa Diabate", 957, 128.8, 109.8, 19.0),
    ("CHA", "Grant Williams / Ryan Kalkbrenner", 272, 116.9, 97.9, 19.0),
]


def normalize_team(team: str) -> str:
    return {"SEA": "OKC", "CHH": "CHA", "NJN": "BKN"}.get(team.upper(), team.upper())


def normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", name)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    for token in [" iii", " jr.", " jr", " sr.", " sr"]:
        text = text.replace(token, "")
    cleaned = []
    for ch in text:
        cleaned.append(ch if ch.isalnum() or ch.isspace() else " ")
    return " ".join("".join(cleaned).split())


def normalize_pair(pair_text: str) -> str:
    return "|".join(sorted(normalize_name(part.strip()) for part in pair_text.split("/")))


def fmt(value: float | None) -> str:
    return "" if value is None else f"{value:.1f}"


SQL = """
WITH player_names AS (
    SELECT CAST(player_id AS BIGINT) AS player_id, any_value(player_name) AS player_name
    FROM player_game_facts
    GROUP BY 1
),
team_names AS (
    SELECT CAST(team_id AS BIGINT) AS team_id, any_value(team_abbr) AS team_abbr
    FROM player_game_facts
    WHERE team_abbr IS NOT NULL AND team_abbr <> ''
    GROUP BY 1
),
split_rollups AS (
    SELECT
        combo_size,
        season,
        team_id,
        combo_id AS unit_id,
        SUM(off_poss) AS off_poss,
        SUM(def_poss) AS def_poss,
        100.0 * SUM(pts_for_raw_off) / NULLIF(SUM(off_poss), 0) AS ortg_raw,
        100.0 * SUM(pts_against_raw_def) / NULLIF(SUM(def_poss), 0) AS drtg_raw
    FROM combo_game_facts
    WHERE combo_size = 2 AND season = '2025-26'
    GROUP BY ALL
)
SELECT
    COALESCE(c.team_abbr, t.team_abbr) AS team_abbr,
    pn1.player_name AS p1_name,
    pn2.player_name AS p2_name,
    c.minutes,
    sr.ortg_raw,
    sr.drtg_raw,
    c.net_raw
FROM combo_2man_agg c
LEFT JOIN team_names t ON c.team_id = t.team_id
LEFT JOIN split_rollups sr
  ON c.season = sr.season
 AND c.team_id = sr.team_id
 AND c.combo_id = sr.unit_id
LEFT JOIN player_names pn1 ON c.p1 = pn1.player_id
LEFT JOIN player_names pn2 ON c.p2 = pn2.player_id
WHERE c.season = '2025-26'
"""


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = con.execute(SQL).fetchall()
    con.close()

    local_map: dict[tuple[str, str], tuple[float, float, float, float]] = {}
    for team_abbr, p1_name, p2_name, minutes, ortg_raw, drtg_raw, net_raw in rows:
        if ortg_raw is None or drtg_raw is None or net_raw is None:
            continue
        key = (normalize_team(team_abbr or ""), normalize_pair(f"{p1_name} / {p2_name}"))
        local_map[key] = (float(minutes), float(ortg_raw), float(drtg_raw), float(net_raw))

    results = []
    for team, players, ext_min, ext_pf, ext_pa, ext_net in EXTERNAL_ROWS:
        local = local_map.get((normalize_team(team), normalize_pair(players)))
        local_min, local_pf, local_pa, local_net = local if local else (None, None, None, None)
        results.append(
            {
                "team": team,
                "players": players,
                "matched": local is not None,
                "ext_min": ext_min,
                "local_min": local_min,
                "min_gap": None if local_min is None else local_min - ext_min,
                "ext_pf": ext_pf,
                "local_pf": local_pf,
                "pf_gap": None if local_pf is None else local_pf - ext_pf,
                "ext_pa": ext_pa,
                "local_pa": local_pa,
                "pa_gap": None if local_pa is None else local_pa - ext_pa,
                "ext_net": ext_net,
                "local_net": local_net,
                "net_gap": None if local_net is None else local_net - ext_net,
            }
        )

    matched = [row for row in results if row["matched"]]

    def mean_abs(key: str) -> float:
        vals = [abs(float(row[key])) for row in matched if row[key] is not None]
        return sum(vals) / len(vals) if vals else 0.0

    worst_net = max(matched, key=lambda row: abs(float(row["net_gap"]))) if matched else None
    summary = {
        "matched": len(matched),
        "total": len(results),
        "mean_abs_min_gap": mean_abs("min_gap"),
        "mean_abs_pf_gap": mean_abs("pf_gap"),
        "mean_abs_pa_gap": mean_abs("pa_gap"),
        "mean_abs_net_gap": mean_abs("net_gap"),
        "worst_net": worst_net,
    }

    rows_html = []
    for row in results:
        gap = row["net_gap"]
        if gap is None:
            gap_class = ""
        elif abs(gap) >= 8:
            gap_class = "bad"
        elif abs(gap) >= 4:
            gap_class = "warn"
        else:
            gap_class = "good"
        rows_html.append(
            f"""
      <tr>
        <td>{row["team"]}</td>
        <td>{row["players"]}</td>
        <td class="{'good' if row['matched'] else 'bad'}">{'Matched' if row['matched'] else 'Missing'}</td>
        <td class="num">{fmt(row["ext_min"])}</td>
        <td class="num">{fmt(row["local_min"])}</td>
        <td class="num">{fmt(row["min_gap"])}</td>
        <td class="num">{fmt(row["ext_pf"])}</td>
        <td class="num">{fmt(row["local_pf"])}</td>
        <td class="num">{fmt(row["ext_pa"])}</td>
        <td class="num">{fmt(row["local_pa"])}</td>
        <td class="num">{fmt(row["ext_net"])}</td>
        <td class="num">{fmt(row["local_net"])}</td>
        <td class="num {gap_class}">{fmt(row["net_gap"])}</td>
      </tr>"""
        )

    worst_text = (
        f'{worst_net["team"]} {worst_net["players"]} ({fmt(worst_net["net_gap"])})'
        if worst_net
        else "n/a"
    )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>2-Man Audit</title>
  <style>
    :root {{
      --bg: #f4f7fb;
      --card: #ffffff;
      --line: #d7e0ea;
      --ink: #16202b;
      --muted: #5a6776;
      --accent: #0b5cab;
      --good: #0f766e;
      --warn: #9a6700;
      --bad: #b42318;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Arial, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #e7f0ff 0%, transparent 28%),
        linear-gradient(180deg, #f8fbff 0%, #f4f7fb 100%);
    }}
    .wrap {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 28px 18px 40px;
    }}
    .hero, .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 20px;
      box-shadow: 0 8px 24px rgba(22, 32, 43, 0.06);
      margin-bottom: 16px;
    }}
    .hero {{
      background: linear-gradient(135deg, #0b5cab 0%, #083b6d 100%);
      color: #fff;
      border-color: #0c4f91;
    }}
    .muted {{ color: var(--muted); }}
    .hero .muted {{ color: rgba(255,255,255,0.88); }}
    h1, h2 {{ margin: 0 0 10px; }}
    p {{ margin: 0 0 12px; line-height: 1.5; }}
    a {{ color: var(--accent); text-decoration: none; font-weight: 600; }}
    .hero a {{ color: #fff; text-decoration: underline; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
    }}
    .stat {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: #fbfdff;
    }}
    .stat .k {{ font-size: 12px; color: var(--muted); }}
    .stat .v {{ font-size: 24px; font-weight: 700; margin-top: 6px; }}
    .table-wrap {{ overflow: auto; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 1200px;
    }}
    th, td {{
      border-bottom: 1px solid #e8edf5;
      padding: 9px 10px;
      text-align: left;
      vertical-align: top;
      font-size: 13px;
    }}
    th {{
      background: #f8fbff;
      position: sticky;
      top: 0;
    }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .good {{ color: var(--good); font-weight: 600; }}
    .warn {{ color: var(--warn); font-weight: 600; }}
    .bad {{ color: var(--bad); font-weight: 600; }}
    code {{
      background: #eef3f8;
      padding: 2px 6px;
      border-radius: 6px;
      font-family: Consolas, monospace;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>2-Man External Audit</h1>
      <p class="muted">
        Side-by-side audit using your pasted NBA advanced lineup table for <code>2025-26</code> two-man units
        at <code>200+</code> minutes. This compares those rows against the possession-based combo source
        queried directly from <code>nba_analytics.duckdb</code>.
      </p>
      <p class="muted">
        Main conclusion: the engine is closer than the old estimate-scaled artifact, but it is still not fully validated.
        Pair detection is mostly there; rating and minute gaps still need explanation.
      </p>
      <p><a href="./lineup-review.html">Back to review hub</a></p>
    </section>

    <section class="card">
      <div class="grid">
        <div class="stat"><div class="k">Pairs Matched</div><div class="v">{summary["matched"]} / {summary["total"]}</div></div>
        <div class="stat"><div class="k">Mean Abs Min Gap</div><div class="v">{fmt(summary["mean_abs_min_gap"])}</div></div>
        <div class="stat"><div class="k">Mean Abs PF/100 Gap</div><div class="v">{fmt(summary["mean_abs_pf_gap"])}</div></div>
        <div class="stat"><div class="k">Mean Abs PA/100 Gap</div><div class="v">{fmt(summary["mean_abs_pa_gap"])}</div></div>
        <div class="stat"><div class="k">Mean Abs Net Gap</div><div class="v">{fmt(summary["mean_abs_net_gap"])}</div></div>
        <div class="stat"><div class="k">Worst Net Gap</div><div class="v" style="font-size:15px;">{worst_text}</div></div>
      </div>
    </section>

    <section class="card table-wrap">
      <table>
        <thead>
          <tr>
            <th>Team</th>
            <th>Players</th>
            <th>Status</th>
            <th class="num">Ext Min</th>
            <th class="num">Our Min</th>
            <th class="num">Min Gap</th>
            <th class="num">Ext PF/100</th>
            <th class="num">Our PF/100</th>
            <th class="num">Ext PA/100</th>
            <th class="num">Our PA/100</th>
            <th class="num">Ext Net</th>
            <th class="num">Our Net</th>
            <th class="num">Net Gap</th>
          </tr>
        </thead>
        <tbody>
{''.join(rows_html)}
        </tbody>
      </table>
    </section>

    <section class="card">
      <h2>Read</h2>
      <p class="muted">
        This page validates only the raw two-man aggregate view. Missing rows mean the pair was not found under the
        same team and normalized player names in our local analytics DB. Small rating gaps are fine. Repeated large
        rating gaps or very large minute gaps are not.
      </p>
    </section>
  </div>
</body>
</html>
"""

    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
