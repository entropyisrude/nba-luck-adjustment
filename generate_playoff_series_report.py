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
BOX_CSV = DATA_DIR / "player_boxscore_stats.csv"
POSS_CSV = DATA_DIR / "possessions_playoffs.csv"


def _load_game_possessions() -> dict[str, float]:
    """Return {game_id: avg_possessions}, preferring PBP counts over four-factor formula."""
    result: dict[str, float] = {}

    # Four-factor formula from player boxscores (covers most games)
    if BOX_CSV.exists():
        box = pd.read_csv(BOX_CSV, usecols=["game_id", "team_abbr", "fga", "fta", "oreb", "tov"])
        box["game_id"] = box["game_id"].astype(str).str.lstrip("0")
        team_game = box.groupby(["game_id", "team_abbr"], as_index=False).sum(numeric_only=True)
        team_game["poss"] = team_game["fga"] - team_game["oreb"] + team_game["tov"] + 0.44 * team_game["fta"]
        for gid, poss in team_game.groupby("game_id")["poss"].mean().items():
            result[str(gid)] = poss

    # PBP actual possession counts — override formula values where available
    if POSS_CSV.exists():
        poss_df = pd.read_csv(POSS_CSV, usecols=["game_id", "offense_team"])
        poss_df["game_id"] = poss_df["game_id"].astype(str).str.lstrip("0")
        pbp_counts = poss_df.groupby(["game_id", "offense_team"]).size().reset_index(name="poss")
        for gid, grp in pbp_counts.groupby("game_id"):
            result[str(gid)] = grp["poss"].mean()

    return result


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


def _series_stats(games: pd.DataFrame, game_poss: dict[str, float]) -> dict:
    """Compute per-team luck-adjusted and actual ratings for a series."""
    teams = sorted(set(games["home_team"]) | set(games["away_team"]))
    if len(teams) != 2:
        return {}

    stats = {t: {"wins": 0, "adj_for": 0.0, "adj_against": 0.0,
                 "act_for": 0.0, "act_against": 0.0, "poss": 0.0}
             for t in teams}

    for _, row in games.iterrows():
        gid = str(row["game_id"]).lstrip("0")
        # Use four-factor possessions when available, fall back to points estimate
        poss = game_poss.get(gid, (row["home_pts_actual"] + row["away_pts_actual"]) / 2.0)
        home, away = row["home_team"], row["away_team"]
        home_win = row["home_pts_actual"] > row["away_pts_actual"]
        for team, is_home in [(home, True), (away, False)]:
            s = stats[team]
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

    actual_games = len(games)
    result = {}
    for team, s in stats.items():
        p = s["poss"]
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
        <th class="adj-col" title="3-point luck adjusted offensive rating">Adj Off</th>
        <th class="adj-col" title="3-point luck adjusted defensive rating">Adj Def</th>
        <th class="adj-col" title="3-point luck adjusted net rating">Adj Net</th>
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
    game_poss = _load_game_possessions()

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
            stats = _series_stats(series_games, game_poss)
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
    grid-template-columns: repeat(auto-fill, minmax(390px, 1fr));
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
    padding: 6px 5px;
    text-align: right;
    font-weight: 600;
    font-size: 0.78em;
    text-transform: uppercase;
    letter-spacing: 0.02em;
    white-space: nowrap;
  }}
  .series-table th:first-child {{ text-align: left; }}
  .series-table td {{
    padding: 8px 5px;
    border-bottom: 1px solid #f0f0f0;
    text-align: right;
    white-space: nowrap;
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
