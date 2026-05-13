"""Generate luck-adjusted offensive/defensive ratings per playoff series."""
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime

import pandas as pd

DATA_DIR = Path("data")
GAMES_CSV = DATA_DIR / "adjusted_games.csv"
OUT_DATA = DATA_DIR / "playoff_series_report.html"
OUT_SITE = Path("playoff-series.html")

ROUND_NAMES = {1: "First Round", 2: "Conference Semifinals", 3: "Conference Finals", 4: "NBA Finals"}


def _parse_game_id(game_id: str) -> tuple[str, int, int]:
    """Return (season_label, round_num, series_idx) from a playoff game_id."""
    s = str(game_id).lstrip("0")
    if not s.startswith("4") or len(s) < 8:
        return "", 0, 0
    try:
        yr = int(s[1:3])
        season = f"20{yr:02d}-{yr + 1:02d}"
        round_num = int(s[5])
        series_idx = int(s[6])
        return season, round_num, series_idx
    except (ValueError, IndexError):
        return "", 0, 0


def _series_stats(games: pd.DataFrame) -> dict:
    """Compute per-team luck-adjusted and actual ratings for a series."""
    teams = sorted(set(games["home_team"]) | set(games["away_team"]))
    if len(teams) != 2:
        return {}

    t1, t2 = teams[0], teams[1]
    stats = {t: {"wins": 0, "games": 0,
                 "adj_for": 0.0, "adj_against": 0.0,
                 "act_for": 0.0, "act_against": 0.0,
                 "poss": 0.0} for t in teams}

    for _, row in games.iterrows():
        poss = (row["home_pts_actual"] + row["away_pts_actual"]) / 2.0
        home, away = row["home_team"], row["away_team"]
        home_win = row["home_pts_actual"] > row["away_pts_actual"]
        for team, is_home in [(home, True), (away, False)]:
            opp = away if is_home else home
            s = stats[team]
            s["games"] += 1
            s["poss"] += poss
            if (is_home and home_win) or (not is_home and not home_win):
                s["wins"] += 1
            if is_home:
                s["adj_for"] += row["home_pts_adj"]
                s["adj_against"] += row["away_pts_adj"]
                s["act_for"] += row["home_pts_actual"]
                s["act_against"] += row["away_pts_actual"]
            else:
                s["adj_for"] += row["away_pts_adj"]
                s["adj_against"] += row["home_pts_adj"]
                s["act_for"] += row["away_pts_actual"]
                s["act_against"] += row["home_pts_actual"]

    result = {}
    for team, s in stats.items():
        p = s["poss"]
        games_played = s["games"] // 2  # each game counted once per team
        actual_games = len(games)
        result[team] = {
            "wins": s["wins"],
            "games": actual_games,
            "adj_ortg": round(s["adj_for"] / p * 100, 1) if p else None,
            "adj_drtg": round(s["adj_against"] / p * 100, 1) if p else None,
            "adj_nrtg": round((s["adj_for"] - s["adj_against"]) / p * 100, 1) if p else None,
            "act_ortg": round(s["act_for"] / p * 100, 1) if p else None,
            "act_drtg": round(s["act_against"] / p * 100, 1) if p else None,
            "act_nrtg": round((s["act_for"] - s["act_against"]) / p * 100, 1) if p else None,
        }
    return result


def _series_card(teams_sorted: list[str], stats: dict, in_progress: bool) -> str:
    t1, t2 = teams_sorted
    s1, s2 = stats[t1], stats[t2]
    games = s1["games"]
    w1, w2 = s1["wins"], s2["wins"]

    if in_progress:
        header = f"{t1} vs {t2} &nbsp;<span class='record'>({w1}–{w2})</span>"
    else:
        winner = t1 if w1 > w2 else t2
        loser = t2 if w1 > w2 else t1
        ww = max(w1, w2)
        lw = min(w1, w2)
        header = f"{winner} def. {loser} &nbsp;<span class='record'>({ww}–{lw})</span>"

    def row_html(team: str, s: dict) -> str:
        act_nrtg = s["act_nrtg"]
        act_nrtg_class = "positive" if act_nrtg and act_nrtg > 0 else ("negative" if act_nrtg and act_nrtg < 0 else "")
        act_nrtg_str = f"{act_nrtg:+.1f}" if act_nrtg is not None else "—"
        adj_nrtg = s["adj_nrtg"]
        adj_nrtg_class = "positive" if adj_nrtg and adj_nrtg > 0 else ("negative" if adj_nrtg and adj_nrtg < 0 else "")
        adj_nrtg_str = f"{adj_nrtg:+.1f}" if adj_nrtg is not None else "—"
        act_ortg = f"{s['act_ortg']:.1f}" if s["act_ortg"] is not None else "—"
        act_drtg = f"{s['act_drtg']:.1f}" if s["act_drtg"] is not None else "—"
        adj_ortg = f"{s['adj_ortg']:.1f}" if s["adj_ortg"] is not None else "—"
        adj_drtg = f"{s['adj_drtg']:.1f}" if s["adj_drtg"] is not None else "—"
        return (
            f"<tr>"
            f"<td class='team-cell'><strong>{team}</strong></td>"
            f"<td>{act_ortg}</td><td>{act_drtg}</td>"
            f"<td class='{act_nrtg_class}'>{act_nrtg_str}</td>"
            f"<td class='adj-col'>{adj_ortg}</td>"
            f"<td class='adj-col'>{adj_drtg}</td>"
            f"<td class='adj-col {adj_nrtg_class}'>{adj_nrtg_str}</td>"
            f"</tr>"
        )

    return f"""
<div class="series-card{'  in-progress' if in_progress else ''}">
  <div class="series-header">{header}</div>
  <table class="series-table">
    <thead>
      <tr>
        <th></th>
        <th title="Actual offensive rating">OffRtg</th>
        <th title="Actual defensive rating">DefRtg</th>
        <th title="Actual net rating">NetRtg</th>
        <th class="adj-col" title="3-point luck adjusted offensive rating">Adj OffRtg</th>
        <th class="adj-col" title="3-point luck adjusted defensive rating">Adj DefRtg</th>
        <th class="adj-col" title="3-point luck adjusted net rating">Adj NetRtg</th>
      </tr>
    </thead>
    <tbody>
      {row_html(t1, s1)}
      {row_html(t2, s2)}
    </tbody>
  </table>
</div>"""


def generate_playoff_series_report() -> Path:
    df = pd.read_csv(GAMES_CSV)
    if "game_type" in df.columns:
        df["game_type"] = df["game_type"].fillna("regular")
    else:
        df["game_type"] = "regular"

    playoff = df[df["game_type"] == "playoff"].copy()
    if playoff.empty:
        print("No playoff games found — skipping playoff series report.")
        return OUT_SITE

    parsed = playoff["game_id"].apply(lambda x: pd.Series(_parse_game_id(str(x)),
                                                           index=["season", "round", "series_idx"]))
    playoff = pd.concat([playoff, parsed], axis=1)

    season_label = playoff["season"].mode()[0]

    # Build sections per round
    body_html = ""
    for rnd in sorted(playoff["round"].unique()):
        rnd_games = playoff[playoff["round"] == rnd]
        rnd_name = ROUND_NAMES.get(int(rnd), f"Round {rnd}")
        body_html += f'<h2 class="round-heading">{rnd_name}</h2>\n<div class="series-grid">\n'

        for sidx in sorted(rnd_games["series_idx"].unique()):
            series_games = rnd_games[rnd_games["series_idx"] == sidx].copy()
            teams = sorted(set(series_games["home_team"]) | set(series_games["away_team"]))
            if len(teams) != 2:
                continue
            stats = _series_stats(series_games)
            if not stats:
                continue
            wins = [stats[t]["wins"] for t in teams]
            games_played = stats[teams[0]]["games"]
            max_wins = max(wins)
            in_progress = max_wins < 4
            body_html += _series_card(teams, stats, in_progress)

        body_html += "\n</div>\n"

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>NBA Playoff Series Ratings — {season_label}</title>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    max-width: 1100px;
    margin: 0 auto;
    padding: 20px;
    background: #f5f5f5;
    color: #333;
  }}
  h1 {{ color: #1a1a2e; margin-top: 0; }}
  .nav {{ margin-bottom: 18px; font-size: 0.9em; }}
  .nav a {{ color: #1a1a2e; text-decoration: none; }}
  .nav a:hover {{ text-decoration: underline; }}
  h2.round-heading {{
    color: #16213e;
    margin: 32px 0 12px;
    padding-bottom: 6px;
    border-bottom: 2px solid #1a1a2e;
  }}
  .series-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 16px;
    margin-bottom: 8px;
  }}
  .series-card {{
    background: white;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    overflow: hidden;
  }}
  .series-card.in-progress {{
    border-left: 4px solid #f0a500;
  }}
  .series-header {{
    background: #1a1a2e;
    color: white;
    padding: 10px 14px;
    font-weight: 600;
    font-size: 0.95em;
  }}
  .series-header .record {{
    font-weight: 400;
    opacity: 0.85;
    font-size: 0.9em;
  }}
  .series-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.88em;
  }}
  .series-table th {{
    background: #f0f0f0;
    color: #444;
    padding: 6px 8px;
    text-align: right;
    font-weight: 600;
    font-size: 0.82em;
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }}
  .series-table th:first-child {{ text-align: left; }}
  .series-table td {{
    padding: 8px 8px;
    border-bottom: 1px solid #f0f0f0;
    text-align: right;
  }}
  .series-table td.team-cell {{ text-align: left; }}
  .series-table tr:last-child td {{ border-bottom: none; }}
  .adj-col {{ background: #fafaf2; }}
  .series-table th.adj-col {{ background: #efefdc; }}
  .positive {{ color: #28a745; font-weight: 700; }}
  .negative {{ color: #dc3545; font-weight: 700; }}
  .methodology {{
    background: #e8f4f8;
    padding: 14px 18px;
    border-radius: 8px;
    font-size: 0.87em;
    margin-top: 30px;
    color: #555;
  }}
  .timestamp {{ color: #888; font-size: 0.82em; margin-top: 24px; }}
</style>
</head>
<body>
<div class="nav"><a href="index.html">← Back to main report</a></div>
<h1>NBA Playoff Series — Luck-Adjusted Ratings</h1>
<p style="color:#555;margin-top:-8px;">{season_label} &nbsp;·&nbsp; Adjusted for 3-point shooting luck</p>

{body_html}

<div class="methodology">
  <strong>How it works:</strong> Offensive and defensive ratings are adjusted by replacing each team's actual 3-point makes with their statistically expected makes (based on shot location and volume), then recomputing the score. This removes randomness from 3-point shooting variance, leaving a more stable estimate of team quality.
  Shaded columns (<strong>Adj OffRtg, Adj DefRtg, Adj NetRtg</strong>) are luck-adjusted; the first two columns show unadjusted actuals for comparison.
  In-progress series are highlighted in orange.
</div>

<div class="timestamp">Generated {now}</div>
</body>
</html>"""

    OUT_DATA.parent.mkdir(parents=True, exist_ok=True)
    OUT_DATA.write_text(html, encoding="utf-8")
    OUT_SITE.write_text(html, encoding="utf-8")
    print(f"Playoff series report saved to {OUT_SITE}")
    return OUT_SITE


if __name__ == "__main__":
    generate_playoff_series_report()
