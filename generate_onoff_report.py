"""Generate season-to-date totals report for adjusted plus-minus and on/off."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
ONOFF_PATH = DATA_DIR / "adjusted_onoff.csv"
ONOFF_PRE2006_PATH = DATA_DIR / "adjusted_onoff_pre2006.csv"
POSS_PATH = DATA_DIR / "possessions.csv"
POSS_HIST_PATH = DATA_DIR / "possessions_historical.csv"
OUTPUT_DATA_PATH = DATA_DIR / "onoff_report.html"
OUTPUT_SITE_PATH = BASE_DIR / "onoff.html"
PLAYER_INFO_MAP = DATA_DIR / "player_info_map.json"

# NBA Cup / In-Season Tournament finals do not count toward regular-season
# totals even though the surrounding knockout games do.
EXCLUDED_REGULAR_SEASON_GAME_IDS = {
    "62300001",
    "62400001",
}

TEAM_ID_TO_ABBR = {
    "1610612737": "ATL",
    "1610612738": "BOS",
    "1610612751": "BKN",
    "1610612766": "CHA",
    "1610612741": "CHI",
    "1610612739": "CLE",
    "1610612742": "DAL",
    "1610612743": "DEN",
    "1610612765": "DET",
    "1610612744": "GSW",
    "1610612745": "HOU",
    "1610612754": "IND",
    "1610612746": "LAC",
    "1610612747": "LAL",
    "1610612763": "MEM",
    "1610612748": "MIA",
    "1610612749": "MIL",
    "1610612750": "MIN",
    "1610612740": "NOP",
    "1610612752": "NYK",
    "1610612760": "OKC",
    "1610612753": "ORL",
    "1610612755": "PHI",
    "1610612756": "PHX",
    "1610612757": "POR",
    "1610612758": "SAC",
    "1610612759": "SAS",
    "1610612761": "TOR",
    "1610612762": "UTA",
    "1610612764": "WAS",
}


def _f(v: object) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _load_player_name_map() -> dict[int, str]:
    if not PLAYER_INFO_MAP.exists():
        return {}
    try:
        raw = json.loads(PLAYER_INFO_MAP.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[int, str] = {}
    for pid, rec in raw.items():
        try:
            pid_int = int(pid)
        except Exception:
            continue
        if isinstance(rec, dict):
            name = rec.get("name")
            if isinstance(name, str) and name.strip():
                out[pid_int] = name.strip()
    return out


def _season_from_date(date_str: str) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d")
    # NBA regular seasons start in October. Using July here mis-buckets the
    # 2020 bubble restart into 2020-21 instead of 2019-20.
    start = d.year if d.month >= 9 else d.year - 1
    return f"{start}-{(start + 1) % 100:02d}"


def _parse_csv_paths(raw: str | None, defaults: list[Path]) -> list[Path]:
    if not raw:
        return defaults
    out: list[Path] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        path = Path(part)
        if not path.is_absolute() and path.parent == Path("."):
            path = DATA_DIR / part
        out.append(path)
    return out


def _load_team_player_totals(onoff_paths: list[Path]) -> tuple[list[dict], str, int, list[str]]:
    if not any(path.exists() for path in onoff_paths):
        raise FileNotFoundError(f"Missing all on/off inputs: {onoff_paths}")

    rows: list[dict] = []
    name_map = _load_player_name_map()
    latest_date = ""
    game_ids: set[str] = set()
    team_game_minutes: dict[tuple[str, str, str], float] = {}

    def ingest(path: Path) -> None:
        nonlocal latest_date
        if not path.exists():
            return
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                gid = str(r.get("game_id") or "").lstrip("0")
                if gid in EXCLUDED_REGULAR_SEASON_GAME_IDS:
                    continue
                pid = r.get("player_id")
                try:
                    pid_int = int(pid) if pid is not None else None
                except Exception:
                    pid_int = None
                if pid_int is not None and pid_int in name_map:
                    r["player_name"] = name_map[pid_int]
                rows.append(r)
                d = str(r["date"])
                if d > latest_date:
                    latest_date = d
                tid = str(r["team_id"])
                game_ids.add(gid)
                key = (d, gid, tid)
                # summed player minutes / 5 gives team minutes for that game.
                team_game_minutes[key] = team_game_minutes.get(key, 0.0) + _f(r["minutes_on"]) / 5.0

    for path in onoff_paths:
        ingest(path)

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
                "minutes_on_total": 0.0,
                "minutes_off_total": 0.0,
                "on_diff_total": 0.0,
                "off_diff_total": 0.0,
                "on_diff_adj_total": 0.0,
                "off_diff_adj_total": 0.0,
                "on_pts_for_adj_total": 0.0,
                "on_pts_against_adj_total": 0.0,
                "off_pts_for_adj_total": 0.0,
                "off_pts_against_adj_total": 0.0,
            }

        a = agg[key]
        a["games_set"].add(game_id)
        a["minutes_on_total"] += minutes_on
        a["minutes_off_total"] += minutes_off
        a["on_diff_total"] += _f(r["on_diff"])
        a["off_diff_total"] += _f(r["off_diff"])
        a["on_diff_adj_total"] += _f(r["on_diff_adj"])
        a["off_diff_adj_total"] += _f(r["off_diff_adj"])
        a["on_pts_for_adj_total"] += _f(r["on_pts_for_adj"])
        a["on_pts_against_adj_total"] += _f(r["on_pts_against_adj"])
        a["off_pts_for_adj_total"] += _f(r["off_pts_for_adj"])
        a["off_pts_against_adj_total"] += _f(r["off_pts_against_adj"])

    out: list[dict] = []
    team_ids = sorted({k[1] for k in agg.keys()}, key=lambda x: TEAM_ID_TO_ABBR.get(x, x))
    for a in agg.values():
        out.append(
            {
                "player_id": a["player_id"],
                "player_name": a["player_name"],
                "season": a["season"],
                "team_id": a["team_id"],
                "team_abbr": TEAM_ID_TO_ABBR.get(a["team_id"], a["team_id"]),
                "games": len(a["games_set"]),
                "minutes_total": a["minutes_on_total"],
                "minutes_off_total": a["minutes_off_total"],
                "on_diff_total": a["on_diff_total"],
                "off_diff_total": a["off_diff_total"],
                "on_diff_adj_total": a["on_diff_adj_total"],
                "off_diff_adj_total": a["off_diff_adj_total"],
                "on_pts_for_adj_total": a["on_pts_for_adj_total"],
                "on_pts_against_adj_total": a["on_pts_against_adj_total"],
                "off_pts_for_adj_total": a["off_pts_for_adj_total"],
                "off_pts_against_adj_total": a["off_pts_against_adj_total"],
            }
        )

    return out, latest_date, len(game_ids), team_ids


def _build_possession_maps(possession_paths: list[Path]) -> dict[tuple[str, str, str], dict]:
    out: dict[tuple[str, str, str], dict] = {}
    team_totals: dict[tuple[str, str], dict[str, float]] = {}

    def _entry(season: str, team_id: str, player_id: str) -> dict[str, float]:
        return out.setdefault(
            (season, team_id, player_id),
            {
                "on_off_poss": 0.0,
                "on_def_poss": 0.0,
            },
        )

    for path in possession_paths:
        if not path.exists():
            continue
        with path.open(newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                gid = str(r.get("game_id") or "").lstrip("0")
                if gid in EXCLUDED_REGULAR_SEASON_GAME_IDS:
                    continue
                season = _season_from_date(str(r["date"]))
                offense_team = str(r["offense_team"])
                defense_team = str(r["defense_team"])
                points = _f(r["points"])

                off_totals = team_totals.setdefault((season, offense_team), {"off_poss": 0.0, "def_poss": 0.0})
                off_totals["off_poss"] += 1.0
                def_totals = team_totals.setdefault((season, defense_team), {"off_poss": 0.0, "def_poss": 0.0})
                def_totals["def_poss"] += 1.0

                for idx in range(1, 6):
                    pid = str(r.get(f"off_p{idx}") or "").strip()
                    if pid:
                        _entry(season, offense_team, pid)["on_off_poss"] += 1.0
                    pid = str(r.get(f"def_p{idx}") or "").strip()
                    if pid:
                        _entry(season, defense_team, pid)["on_def_poss"] += 1.0

    for (season, team_id, player_id), rec in out.items():
        team = team_totals.get((season, team_id), {})
        on_off_poss = _f(rec["on_off_poss"])
        on_def_poss = _f(rec["on_def_poss"])
        team_off_poss = _f(team.get("off_poss"))
        team_def_poss = _f(team.get("def_poss"))
        rec["on_poss"] = (on_off_poss + on_def_poss) / 2.0
        rec["off_poss"] = max(0.0, ((team_off_poss - on_off_poss) + (team_def_poss - on_def_poss)) / 2.0)

    return out


def _finalize_records(raw_rows: list[dict], possession_map: dict[tuple[str, str, str], dict]) -> list[dict]:
    records: list[dict] = []
    for r in raw_rows:
        season = str(r["season"])
        team_id = r["team_id"]
        player_id = r["player_id"]
        on_min = _f(r["minutes_total"])
        off_min = _f(r["minutes_off_total"])

        poss = possession_map.get((season, team_id, player_id))
        on_poss = _f((poss or {}).get("on_poss"))
        off_poss = _f((poss or {}).get("off_poss"))
        source = "rebuilt_possessions" if on_poss > 0 else "minutes"

        if on_poss > 0:
            pm_actual_100 = _f(r["on_diff_total"]) * 100.0 / on_poss
        else:
            pm_actual_100 = (_f(r["on_diff_total"]) * 48.0 / on_min) if on_min > 0 else 0.0

        if off_poss > 0:
            off_actual_100 = _f(r["off_diff_total"]) * 100.0 / off_poss
            onoff_actual_100 = pm_actual_100 - off_actual_100
        elif off_min > 0:
            off_actual_100 = _f(r["off_diff_total"]) * 48.0 / off_min
            onoff_actual_100 = pm_actual_100 - off_actual_100
        else:
            onoff_actual_100 = 0.0

        if on_poss > 0:
            pm_adj_100 = _f(r["on_diff_adj_total"]) * 100.0 / on_poss
        else:
            pm_adj_100 = (_f(r["on_diff_adj_total"]) * 48.0 / on_min) if on_min > 0 else 0.0

        if off_poss > 0:
            off_adj_100 = _f(r["off_diff_adj_total"]) * 100.0 / off_poss
            onoff_adj_100 = pm_adj_100 - off_adj_100
        else:
            if off_min > 0:
                off_adj_100 = _f(r["off_diff_adj_total"]) * 48.0 / off_min
                onoff_adj_100 = pm_adj_100 - off_adj_100
            else:
                onoff_adj_100 = 0.0

        pm_delta_100 = pm_adj_100 - pm_actual_100
        onoff_delta_100 = onoff_adj_100 - onoff_actual_100

        # Break adjusted on-off into offensive and defensive components using
        # our own rebuilt possession counts whenever available.
        on_for_adj = _f(r["on_pts_for_adj_total"])
        on_against_adj = _f(r["on_pts_against_adj_total"])
        off_for_adj = _f(r["off_pts_for_adj_total"])
        off_against_adj = _f(r["off_pts_against_adj_total"])

        if on_min > 0:
            on_poss_est = on_poss if on_poss > 0 else on_min * (100.0 / 48.0)
            on_ortg_adj = on_for_adj * 100.0 / on_poss_est
            on_drtg_adj = on_against_adj * 100.0 / on_poss_est
        else:
            on_ortg_adj = on_drtg_adj = 0.0

        if off_min > 0:
            off_poss_est = off_poss if off_poss > 0 else off_min * (100.0 / 48.0)
            off_ortg_adj = off_for_adj * 100.0 / off_poss_est
            off_drtg_adj = off_against_adj * 100.0 / off_poss_est
        else:
            off_ortg_adj = off_drtg_adj = 0.0

        # Offensive: team adj ORtg per 100 when ON minus when OFF (positive = helps offense)
        onoff_adj_off_100 = on_ortg_adj - off_ortg_adj
        # Defensive: team adj DRtg per 100 when OFF minus when ON (positive = helps defense)
        onoff_adj_def_100 = off_drtg_adj - on_drtg_adj

        records.append(
            {
                "player_id": player_id,
                "player_name": r["player_name"],
                "season": season,
                "team_id": team_id,
                "team_abbr": r["team_abbr"],
                "games": int(_f(r["games"])),
                "minutes_total": on_min,
                "pm_actual_100": pm_actual_100,
                "pm_adj_100": pm_adj_100,
                "pm_delta_100": pm_delta_100,
                "onoff_actual_100": onoff_actual_100,
                "onoff_adj_100": onoff_adj_100,
                "onoff_adj_off_100": onoff_adj_off_100,
                "onoff_adj_def_100": onoff_adj_def_100,
                "onoff_delta_100": onoff_delta_100,
                "on_ortg_adj": on_ortg_adj,
                "on_drtg_adj": on_drtg_adj,
                "off_ortg_adj": off_ortg_adj,
                "off_drtg_adj": off_drtg_adj,
                "raw_source": source,
                "on_poss": on_poss,
                "off_poss": off_poss,
            }
        )
    return records


def generate_onoff_report(
    onoff_paths: list[Path] | None = None,
    possession_paths: list[Path] | None = None,
    output_data_path: Path | None = None,
    output_site_path: Path | None = None,
    skip_pbpstats: bool = False,
) -> Path:
    if skip_pbpstats:
        # Kept only for CLI compatibility. The report now uses rebuilt
        # possession data only and never fetches pbpstats.
        pass
    onoff_paths = onoff_paths or [ONOFF_PRE2006_PATH, ONOFF_PATH]
    possession_paths = possession_paths or [POSS_HIST_PATH, POSS_PATH]
    output_data_path = output_data_path or OUTPUT_DATA_PATH
    output_site_path = output_site_path or OUTPUT_SITE_PATH

    raw_rows, latest_date, game_count, team_ids = _load_team_player_totals(onoff_paths)
    latest_season = _season_from_date(latest_date)
    records = _finalize_records(raw_rows, _build_possession_maps(possession_paths))

    team_values = sorted({r["team_id"] for r in records}, key=lambda x: TEAM_ID_TO_ABBR.get(x, x))
    season_values = sorted({r["season"] for r in records}, reverse=True)
    generated_ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    page_title = "3PT Luck Adjusted Plus Minus: Totals"

    html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
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
      font-family: \"Segoe UI\", Arial, sans-serif;
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
    .nav {{ margin-top: 10px; display: flex; gap: 12px; flex-wrap: wrap; justify-content: center; }}
    .nav a {{ color: #e5f6ff; text-decoration: underline; text-underline-offset: 3px; font-size: 13px; }}
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
    table {{ width: 100%; border-collapse: collapse; min-width: 1100px; font-size: 12px; }}
    th, td {{
      border-bottom: 1px solid #edf2f9;
      padding: 7px 8px;
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child,
    th:nth-child(2), td:nth-child(2) {{ text-align: left; }}
    #multi-table th:nth-child(2),
    #multi-table td:nth-child(2) {{ text-align: right; }}
    td:first-child {{
      position: sticky;
      left: 0;
      z-index: 1;
      background: var(--card);
    }}
    thead th {{
      position: sticky;
      top: 0;
      z-index: 2;
      background: #edf3fc;
      color: #123154;
    }}
    thead th:first-child {{
      left: 0;
      z-index: 3;
    }}
    thead th.sortable {{ cursor: pointer; }}
    thead th.sortable:hover {{ background: #e4edf9; }}
    .pos {{ color: var(--good); font-weight: 600; }}
    .neg {{ color: var(--bad); font-weight: 600; }}
    .muted {{ color: var(--muted); }}
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
    .metric-emph {{ font-weight: 700; }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <section class=\"hero\">
      <h1>{page_title}</h1>
      <div class=\"meta\">
        <span class=\"chip\">Data through {latest_date}</span>
        <span class=\"chip\">Games: {game_count:,}</span>
        <span class=\"chip\">Rows: {len(records):,}</span>
      </div>
      <div class=\"nav\">
        <a href=\"index.html\">Overview</a>
        <a href=\"onoff-daily.html\">+/- Games</a>
        <a href=\"onoff.html\">+/- Stats</a>
        <a href=\"rapm.html\">RAPM</a>
        <a href=\"onoff-playoffs.html\">+/- Playoffs</a>
        <a href=\"rapm-playoffs.html\">Playoff RAPM</a>
        <a href=\"example.html\">Method</a>
      </div>
    </section>

    <section class=\"card\">
      <h2>Team-by-Team Season Totals</h2>
      <div class=\"controls\">
        <label>Season
          <select id=\"season-filter\"></select>
        </label>
        <label>Team
          <select id=\"team-filter\"></select>
        </label>
        <label>Min games
          <input id=\"team-min-games\" type=\"number\" min=\"0\" step=\"1\" value=\"1\" />
        </label>
        <label>Min total minutes
          <input id=\"team-min-minutes\" type=\"number\" min=\"0\" step=\"1\" value=\"50\" />
        </label>
      </div>
      <div class=\"toggle-row\">
        <button class=\"toggle-btn\" data-cols=\"ortg-adj\" onclick=\"toggleCols('ortg-adj')\">Show ORtg Adj</button>
        <button class=\"toggle-btn\" data-cols=\"drtg-adj\" onclick=\"toggleCols('drtg-adj')\">Show DRtg Adj</button>
        <button class=\"toggle-btn\" data-cols=\"pm-delta\" onclick=\"toggleCols('pm-delta')\">Show +/- Delta</button>
        <button class=\"toggle-btn\" data-cols=\"onoff-delta\" onclick=\"toggleCols('onoff-delta')\">Show On/Off Delta</button>
      </div>
      <p class=\"muted\" style=\"margin: 0 0 8px; font-size: 11px;\">All stats per 100 possessions</p>
      <div class=\"table-wrap\">
        <table id=\"team-table\">
          <thead>
            <tr>
              <th class=\"sortable\" data-key=\"player_name\" data-type=\"str\">Player</th>
              <th class=\"sortable\" data-key=\"team_abbr\" data-type=\"str\">Team</th>
              <th class=\"sortable\" data-key=\"games\" data-type=\"num\">G</th>
              <th class=\"sortable\" data-key=\"minutes_total\" data-type=\"num\">Min</th>
              <th class=\"sortable\" data-key=\"pm_actual_100\" data-type=\"num\">+/-</th>
              <th class=\"sortable metric-emph\" data-key=\"pm_adj_100\" data-type=\"num\">+/- Adj</th>
              <th class=\"sortable col-pm-delta col-hidden\" data-key=\"pm_delta_100\" data-type=\"num\">+/- Delta</th>
              <th class=\"sortable\" data-key=\"onoff_actual_100\" data-type=\"num\">On/Off</th>
              <th class=\"sortable metric-emph\" data-key=\"onoff_adj_100\" data-type=\"num\" title=\"3PT-luck adjusted on-off per 100 possessions\">On/Off Adj</th>
              <th class=\"sortable\" data-key=\"onoff_adj_off_100\" data-type=\"num\" title=\"Offensive component: team 3PT-adjusted ORtg when ON minus when OFF. Positive = player improves team offense.\">On/Off Adj Off</th>
              <th class=\"sortable col-ortg-adj col-hidden\" data-key=\"on_ortg_adj\" data-type=\"num\" title=\"Team adjusted ORtg when player is ON court\">On ORtg Adj</th>
              <th class=\"sortable col-ortg-adj col-hidden\" data-key=\"off_ortg_adj\" data-type=\"num\" title=\"Team adjusted ORtg when player is OFF court\">Off ORtg Adj</th>
              <th class=\"sortable\" data-key=\"onoff_adj_def_100\" data-type=\"num\" title=\"Defensive component: team 3PT-adjusted DRtg when OFF minus when ON. Positive = player improves team defense.\">On/Off Adj Def</th>
              <th class=\"sortable col-drtg-adj col-hidden\" data-key=\"on_drtg_adj\" data-type=\"num\" title=\"Team adjusted DRtg when player is ON court\">On DRtg Adj</th>
              <th class=\"sortable col-drtg-adj col-hidden\" data-key=\"off_drtg_adj\" data-type=\"num\" title=\"Team adjusted DRtg when player is OFF court\">Off DRtg Adj</th>
              <th class=\"sortable col-onoff-delta col-hidden\" data-key=\"onoff_delta_100\" data-type=\"num\">On/Off Delta</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section class=\"card\">
      <h2>Multi-Year (All Teams)</h2>
      <div class=\"controls\">
        <label>Season Group
          <select id=\"multi-season-filter\"></select>
        </label>
        <label>Min games
          <input id=\"multi-min-games\" type=\"number\" min=\"0\" step=\"1\" value=\"20\" />
        </label>
        <label>Min total minutes
          <input id=\"multi-min-minutes\" type=\"number\" min=\"0\" step=\"1\" value=\"500\" />
        </label>
      </div>
      <div class=\"toggle-row\">
        <button class=\"toggle-btn\" data-cols=\"ortg-adj\" onclick=\"toggleCols('ortg-adj')\">Show ORtg Adj</button>
        <button class=\"toggle-btn\" data-cols=\"drtg-adj\" onclick=\"toggleCols('drtg-adj')\">Show DRtg Adj</button>
        <button class=\"toggle-btn\" data-cols=\"pm-delta\" onclick=\"toggleCols('pm-delta')\">Show +/- Delta</button>
        <button class=\"toggle-btn\" data-cols=\"onoff-delta\" onclick=\"toggleCols('onoff-delta')\">Show On/Off Delta</button>
      </div>
      <p class=\"muted\" style=\"margin: 0 0 8px; font-size: 11px;\">All stats per 100 possessions</p>
      <div class=\"table-wrap\">
        <table id=\"multi-table\">
          <thead>
            <tr>
              <th class=\"sortable\" data-key=\"player_name\" data-type=\"str\">Player</th>
              <th class=\"sortable\" data-key=\"games\" data-type=\"num\">G</th>
              <th class=\"sortable\" data-key=\"minutes_total\" data-type=\"num\">Min</th>
              <th class=\"sortable\" data-key=\"pm_actual_100\" data-type=\"num\">+/-</th>
              <th class=\"sortable metric-emph\" data-key=\"pm_adj_100\" data-type=\"num\">+/- Adj</th>
              <th class=\"sortable col-pm-delta col-hidden\" data-key=\"pm_delta_100\" data-type=\"num\">+/- Delta</th>
              <th class=\"sortable\" data-key=\"onoff_actual_100\" data-type=\"num\">On/Off</th>
              <th class=\"sortable metric-emph\" data-key=\"onoff_adj_100\" data-type=\"num\" title=\"3PT-luck adjusted on-off per 100 possessions\">On/Off Adj</th>
              <th class=\"sortable\" data-key=\"onoff_adj_off_100\" data-type=\"num\" title=\"Offensive component: team 3PT-adjusted ORtg when ON minus when OFF. Positive = player improves team offense.\">On/Off Adj Off</th>
              <th class=\"sortable col-ortg-adj col-hidden\" data-key=\"on_ortg_adj\" data-type=\"num\" title=\"Team adjusted ORtg when player is ON court\">On ORtg Adj</th>
              <th class=\"sortable col-ortg-adj col-hidden\" data-key=\"off_ortg_adj\" data-type=\"num\" title=\"Team adjusted ORtg when player is OFF court\">Off ORtg Adj</th>
              <th class=\"sortable\" data-key=\"onoff_adj_def_100\" data-type=\"num\" title=\"Defensive component: team 3PT-adjusted DRtg when OFF minus when ON. Positive = player improves team defense.\">On/Off Adj Def</th>
              <th class=\"sortable col-drtg-adj col-hidden\" data-key=\"on_drtg_adj\" data-type=\"num\" title=\"Team adjusted DRtg when player is ON court\">On DRtg Adj</th>
              <th class=\"sortable col-drtg-adj col-hidden\" data-key=\"off_drtg_adj\" data-type=\"num\" title=\"Team adjusted DRtg when player is OFF court\">Off DRtg Adj</th>
              <th class=\"sortable col-onoff-delta col-hidden\" data-key=\"onoff_delta_100\" data-type=\"num\">On/Off Delta</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section class=\"card\">
      <h2>Season Leaderboard (Sortable)</h2>
      <div class=\"controls\">
        <label>Min games
          <input id=\"lb-min-games\" type=\"number\" min=\"0\" step=\"1\" value=\"10\" />
        </label>
        <label>Min total minutes
          <input id=\"lb-min-minutes\" type=\"number\" min=\"0\" step=\"1\" value=\"200\" />
        </label>
      </div>
      <div class=\"toggle-row\">
        <button class=\"toggle-btn\" data-cols=\"ortg-adj\" onclick=\"toggleCols('ortg-adj')\">Show ORtg Adj</button>
        <button class=\"toggle-btn\" data-cols=\"drtg-adj\" onclick=\"toggleCols('drtg-adj')\">Show DRtg Adj</button>
        <button class=\"toggle-btn\" data-cols=\"pm-delta\" onclick=\"toggleCols('pm-delta')\">Show +/- Delta</button>
        <button class=\"toggle-btn\" data-cols=\"onoff-delta\" onclick=\"toggleCols('onoff-delta')\">Show On/Off Delta</button>
      </div>
      <p class=\"muted\" style=\"margin: 0 0 8px; font-size: 11px;\">All stats per 100 possessions</p>
      <div class=\"table-wrap\">
        <table id=\"lb-table\">
          <thead>
            <tr>
              <th class=\"sortable\" data-key=\"player_name\" data-type=\"str\">Player</th>
              <th class=\"sortable\" data-key=\"team_abbr\" data-type=\"str\">Team</th>
              <th class=\"sortable\" data-key=\"games\" data-type=\"num\">G</th>
              <th class=\"sortable\" data-key=\"minutes_total\" data-type=\"num\">Min</th>
              <th class=\"sortable\" data-key=\"pm_actual_100\" data-type=\"num\">+/-</th>
              <th class=\"sortable metric-emph\" data-key=\"pm_adj_100\" data-type=\"num\">+/- Adj</th>
              <th class=\"sortable col-pm-delta col-hidden\" data-key=\"pm_delta_100\" data-type=\"num\">+/- Delta</th>
              <th class=\"sortable\" data-key=\"onoff_actual_100\" data-type=\"num\">On/Off</th>
              <th class=\"sortable metric-emph\" data-key=\"onoff_adj_100\" data-type=\"num\" title=\"3PT-luck adjusted on-off per 100 possessions\">On/Off Adj</th>
              <th class=\"sortable\" data-key=\"onoff_adj_off_100\" data-type=\"num\" title=\"Offensive component: team 3PT-adjusted ORtg when ON minus when OFF. Positive = player improves team offense.\">On/Off Adj Off</th>
              <th class=\"sortable col-ortg-adj col-hidden\" data-key=\"on_ortg_adj\" data-type=\"num\" title=\"Team adjusted ORtg when player is ON court\">On ORtg Adj</th>
              <th class=\"sortable col-ortg-adj col-hidden\" data-key=\"off_ortg_adj\" data-type=\"num\" title=\"Team adjusted ORtg when player is OFF court\">Off ORtg Adj</th>
              <th class=\"sortable\" data-key=\"onoff_adj_def_100\" data-type=\"num\" title=\"Defensive component: team 3PT-adjusted DRtg when OFF minus when ON. Positive = player improves team defense.\">On/Off Adj Def</th>
              <th class=\"sortable col-drtg-adj col-hidden\" data-key=\"on_drtg_adj\" data-type=\"num\" title=\"Team adjusted DRtg when player is ON court\">On DRtg Adj</th>
              <th class=\"sortable col-drtg-adj col-hidden\" data-key=\"off_drtg_adj\" data-type=\"num\" title=\"Team adjusted DRtg when player is OFF court\">Off DRtg Adj</th>
              <th class=\"sortable col-onoff-delta col-hidden\" data-key=\"onoff_delta_100\" data-type=\"num\">On/Off Delta</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <p class=\"muted\">Generated {generated_ts} | Source: rebuilt on/off CSVs + rebuilt possession CSVs.</p>
  </div>
  <script>
    const ROWS = {json.dumps(records)};
    const TEAMS = {json.dumps(team_values)};
    const SEASONS = {json.dumps(season_values)};
    const TEAM_MAP = {json.dumps(TEAM_ID_TO_ABBR)};
    const LATEST_SEASON = {json.dumps(latest_season)};

    const fmt = (x, d=1) => (x === null || Number.isNaN(Number(x))) ? "" : Number(x).toFixed(d);
    const cls = (x) => (x > 0 ? "pos" : (x < 0 ? "neg" : ""));

    let teamSortKey = "pm_adj_100";
    let teamSortDir = "desc";
    let lbSortKey = "pm_adj_100";
    let lbSortDir = "desc";
    let multiSortKey = "pm_adj_100";
    let multiSortDir = "desc";

    const METRIC_KEYS = [
      "pm_actual_100",
      "pm_adj_100",
      "pm_delta_100",
      "onoff_actual_100",
      "onoff_adj_100",
      "onoff_adj_off_100",
      "onoff_adj_def_100",
      "onoff_delta_100",
      "on_ortg_adj",
      "off_ortg_adj",
      "on_drtg_adj",
      "off_drtg_adj",
    ];

    function aggregateRows(rows) {{
      const map = new Map();
      rows.forEach(r => {{
        const pid = r.player_id;
        if (!map.has(pid)) {{
          map.set(pid, {{
            player_id: pid,
            player_name: r.player_name,
            team_abbrs: new Set(),
            games: 0,
            minutes_total: 0,
            sums: Object.fromEntries(METRIC_KEYS.map(k => [k, 0]))
          }});
        }}
        const agg = map.get(pid);
        const mins = Number(r.minutes_total || 0);
        agg.games += Number(r.games || 0);
        agg.minutes_total += mins;
        if (r.team_abbr) agg.team_abbrs.add(r.team_abbr);
        METRIC_KEYS.forEach(k => {{
          agg.sums[k] += mins * Number(r[k] || 0);
        }});
      }});
      return Array.from(map.values()).map(a => {{
        const mins = a.minutes_total || 0;
        const out = {{
          player_id: a.player_id,
          player_name: a.player_name,
          team_abbr: Array.from(a.team_abbrs).filter(Boolean).sort().join(", "),
          games: a.games,
          minutes_total: a.minutes_total
        }};
        METRIC_KEYS.forEach(k => {{
          out[k] = mins ? (a.sums[k] / mins) : 0;
        }});
        return out;
      }});
    }}

    function buildSeasonGroups() {{
      const seasons = SEASONS.slice().sort();
      const last3 = seasons.slice(-3);
      const last5 = seasons.slice(-5);
      const groups = [
        {{ key: "Last3", label: `Last 3 Seasons (${{last3[0]}}-${{last3[last3.length-1]}})`, seasons: last3 }},
        {{ key: "Last5", label: `Last 5 Seasons (${{last5[0]}}-${{last5[last5.length-1]}})`, seasons: last5 }},
        {{ key: "2020s", label: "2020s", seasons: seasons.filter(s => Number(s.slice(0,4)) >= 2020) }},
        {{ key: "2010s", label: "2010s", seasons: seasons.filter(s => Number(s.slice(0,4)) >= 2010 && Number(s.slice(0,4)) < 2020) }},
        {{ key: "2000s", label: "2000s", seasons: seasons.filter(s => Number(s.slice(0,4)) >= 2000 && Number(s.slice(0,4)) < 2010) }},
        {{ key: "1996-99", label: "1996-99", seasons: seasons.filter(s => Number(s.slice(0,4)) < 2000) }},
        {{ key: "All", label: "All Seasons", seasons }}
      ].filter(g => g.seasons.length);
      return groups;
    }}

    function toggleCols(colGroup) {{
      const btns = document.querySelectorAll(`.toggle-btn[data-cols="${{colGroup}}"]`);
      const cols = document.querySelectorAll(`.col-${{colGroup}}`);
      const isHidden = cols[0]?.classList.contains('col-hidden');
      btns.forEach(btn => btn.classList.toggle('active', isHidden));
      cols.forEach(col => col.classList.toggle('col-hidden', !isHidden));
      btns.forEach(btn => btn.textContent = isHidden ? btn.textContent.replace('Show', 'Hide') : btn.textContent.replace('Hide', 'Show'));
    }}

    function rowHtml(r) {{
      const ortgHidden = !document.querySelector('.toggle-btn[data-cols="ortg-adj"]')?.classList.contains('active');
      const drtgHidden = !document.querySelector('.toggle-btn[data-cols="drtg-adj"]')?.classList.contains('active');
      return `<tr>
        <td>${{r.player_name}}</td>
        <td>${{r.team_abbr}}</td>
        <td>${{fmt(r.games,0)}}</td>
        <td>${{fmt(r.minutes_total,1)}}</td>
        <td class="${{cls(r.pm_actual_100)}}">${{fmt(r.pm_actual_100,1)}}</td>
        <td class="${{cls(r.pm_adj_100)}}">${{fmt(r.pm_adj_100,1)}}</td>
        <td class="col-pm-delta col-hidden ${{cls(r.pm_delta_100)}}">${{fmt(r.pm_delta_100,1)}}</td>
        <td class="${{cls(r.onoff_actual_100)}}">${{fmt(r.onoff_actual_100,1)}}</td>
        <td class="${{cls(r.onoff_adj_100)}}">${{fmt(r.onoff_adj_100,1)}}</td>
        <td class="${{cls(r.onoff_adj_off_100)}}">${{fmt(r.onoff_adj_off_100,1)}}</td>
        <td class="col-ortg-adj${{ortgHidden ? ' col-hidden' : ''}}">${{fmt(r.on_ortg_adj,1)}}</td>
        <td class="col-ortg-adj${{ortgHidden ? ' col-hidden' : ''}}">${{fmt(r.off_ortg_adj,1)}}</td>
        <td class="${{cls(r.onoff_adj_def_100)}}">${{fmt(r.onoff_adj_def_100,1)}}</td>
        <td class="col-drtg-adj${{drtgHidden ? ' col-hidden' : ''}}">${{fmt(r.on_drtg_adj,1)}}</td>
        <td class="col-drtg-adj${{drtgHidden ? ' col-hidden' : ''}}">${{fmt(r.off_drtg_adj,1)}}</td>
        <td class="col-onoff-delta col-hidden ${{cls(r.onoff_delta_100)}}">${{fmt(r.onoff_delta_100,1)}}</td>
      </tr>`;
    }}

    function rowHtmlNoTeam(r) {{
      const ortgHidden = !document.querySelector('.toggle-btn[data-cols="ortg-adj"]')?.classList.contains('active');
      const drtgHidden = !document.querySelector('.toggle-btn[data-cols="drtg-adj"]')?.classList.contains('active');
      return `<tr>
        <td>${{r.player_name}}</td>
        <td>${{fmt(r.games,0)}}</td>
        <td>${{fmt(r.minutes_total,1)}}</td>
        <td class="${{cls(r.pm_actual_100)}}">${{fmt(r.pm_actual_100,1)}}</td>
        <td class="${{cls(r.pm_adj_100)}}">${{fmt(r.pm_adj_100,1)}}</td>
        <td class="col-pm-delta col-hidden ${{cls(r.pm_delta_100)}}">${{fmt(r.pm_delta_100,1)}}</td>
        <td class="${{cls(r.onoff_actual_100)}}">${{fmt(r.onoff_actual_100,1)}}</td>
        <td class="${{cls(r.onoff_adj_100)}}">${{fmt(r.onoff_adj_100,1)}}</td>
        <td class="${{cls(r.onoff_adj_off_100)}}">${{fmt(r.onoff_adj_off_100,1)}}</td>
        <td class="col-ortg-adj${{ortgHidden ? ' col-hidden' : ''}}">${{fmt(r.on_ortg_adj,1)}}</td>
        <td class="col-ortg-adj${{ortgHidden ? ' col-hidden' : ''}}">${{fmt(r.off_ortg_adj,1)}}</td>
        <td class="${{cls(r.onoff_adj_def_100)}}">${{fmt(r.onoff_adj_def_100,1)}}</td>
        <td class="col-drtg-adj${{drtgHidden ? ' col-hidden' : ''}}">${{fmt(r.on_drtg_adj,1)}}</td>
        <td class="col-drtg-adj${{drtgHidden ? ' col-hidden' : ''}}">${{fmt(r.off_drtg_adj,1)}}</td>
        <td class="col-onoff-delta col-hidden ${{cls(r.onoff_delta_100)}}">${{fmt(r.onoff_delta_100,1)}}</td>
      </tr>`;
    }}

    function renderTeamTable() {{
      const season = document.getElementById("season-filter").value;
      const team = document.getElementById("team-filter").value;
      const minGames = Number(document.getElementById("team-min-games").value || 0);
      const minMin = Number(document.getElementById("team-min-minutes").value || 0);
      const tbody = document.querySelector("#team-table tbody");

      const base = ROWS.filter(r => r.season === season);
      const filtered = team === "__all__"
        ? aggregateRows(base)
        : base.filter(r => r.team_id === team);

      const rows = filtered
        .filter(r => Number(r.games || 0) >= minGames)
        .filter(r => Number(r.minutes_total || 0) >= minMin)
        .slice()
        .sort((a,b) => {{
          const dir = teamSortDir === "asc" ? 1 : -1;
          if (teamSortKey === "player_name" || teamSortKey === "team_abbr") {{
            return dir * String(a[teamSortKey]).localeCompare(String(b[teamSortKey]));
          }}
          return dir * (Number(a[teamSortKey] || 0) - Number(b[teamSortKey] || 0));
        }});

      tbody.innerHTML = rows.map(rowHtml).join("");
    }}

    function renderLeaderboard() {{
      const season = document.getElementById("season-filter").value;
      const minGames = Number(document.getElementById("lb-min-games").value || 0);
      const minMin = Number(document.getElementById("lb-min-minutes").value || 0);
      const tbody = document.querySelector("#lb-table tbody");

      const rows = ROWS
        .filter(r => r.season === season)
        .filter(r => Number(r.games || 0) >= minGames)
        .filter(r => Number(r.minutes_total || 0) >= minMin)
        .slice()
        .sort((a,b) => {{
          const dir = lbSortDir === "asc" ? 1 : -1;
          if (lbSortKey === "player_name" || lbSortKey === "team_abbr") {{
            return dir * String(a[lbSortKey]).localeCompare(String(b[lbSortKey]));
          }}
          return dir * (Number(a[lbSortKey] || 0) - Number(b[lbSortKey] || 0));
        }});

      tbody.innerHTML = rows.map(rowHtml).join("");
    }}

    function renderMultiTable() {{
      const groupKey = document.getElementById("multi-season-filter").value;
      const minGames = Number(document.getElementById("multi-min-games").value || 0);
      const minMin = Number(document.getElementById("multi-min-minutes").value || 0);
      const tbody = document.querySelector("#multi-table tbody");

      const groups = buildSeasonGroups();
      const group = groups.find(g => g.key === groupKey) || groups[0];
      const rows = aggregateRows(ROWS.filter(r => group.seasons.includes(r.season)))
        .filter(r => Number(r.games || 0) >= minGames)
        .filter(r => Number(r.minutes_total || 0) >= minMin)
        .slice()
        .sort((a,b) => {{
          const dir = multiSortDir === "asc" ? 1 : -1;
          if (multiSortKey === "player_name") {{
            return dir * String(a[multiSortKey]).localeCompare(String(b[multiSortKey]));
          }}
          return dir * (Number(a[multiSortKey] || 0) - Number(b[multiSortKey] || 0));
        }});

      tbody.innerHTML = rows.map(rowHtmlNoTeam).join("");
    }}

    function init() {{
      const seasonSel = document.getElementById("season-filter");
      const teamSel = document.getElementById("team-filter");
      SEASONS.forEach(s => {{
        const o = document.createElement("option");
        o.value = s;
        o.textContent = s;
        seasonSel.appendChild(o);
      }});
      seasonSel.value = LATEST_SEASON;

      function refreshTeamOptions() {{
        const season = seasonSel.value;
        const seasonTeams = [...new Set(ROWS.filter(r => r.season === season).map(r => r.team_id))]
          .sort((a, b) => String(TEAM_MAP[a] || a).localeCompare(String(TEAM_MAP[b] || b)));
        teamSel.innerHTML = "";
        const allOpt = document.createElement("option");
        allOpt.value = "__all__";
        allOpt.textContent = "All Teams";
        teamSel.appendChild(allOpt);
        seasonTeams.forEach(tid => {{
          const o = document.createElement("option");
          o.value = tid;
          o.textContent = TEAM_MAP[tid] || tid;
          teamSel.appendChild(o);
        }});
        if (teamSel.options.length > 0) {{
          teamSel.selectedIndex = 0;
        }}
      }}

      refreshTeamOptions();

      const multiSel = document.getElementById("multi-season-filter");
      buildSeasonGroups().forEach(g => {{
        const o = document.createElement("option");
        o.value = g.key;
        o.textContent = g.label;
        multiSel.appendChild(o);
      }});
      multiSel.value = "Last3";

      ["season-filter","team-filter","team-min-games","team-min-minutes"].forEach(id =>
        document.getElementById(id).addEventListener("input", () => {{
          if (id === "season-filter") {{
            refreshTeamOptions();
          }}
          renderTeamTable();
          renderLeaderboard();
        }})
      );

      ["lb-min-games","lb-min-minutes"].forEach(id =>
        document.getElementById(id).addEventListener("input", renderLeaderboard)
      );

      ["multi-season-filter","multi-min-games","multi-min-minutes"].forEach(id =>
        document.getElementById(id).addEventListener("input", renderMultiTable)
      );

      document.querySelectorAll("#lb-table thead th.sortable").forEach(th => {{
        th.addEventListener("click", () => {{
          const key = th.dataset.key;
          if (lbSortKey === key) {{
            lbSortDir = lbSortDir === "desc" ? "asc" : "desc";
          }} else {{
            lbSortKey = key;
            lbSortDir = key === "player_name" || key === "team_abbr" ? "asc" : "desc";
          }}
          renderLeaderboard();
        }});
      }});
      document.querySelectorAll("#team-table thead th.sortable").forEach(th => {{
        th.addEventListener("click", () => {{
          const key = th.dataset.key;
          if (teamSortKey === key) {{
            teamSortDir = teamSortDir === "desc" ? "asc" : "desc";
          }} else {{
            teamSortKey = key;
            teamSortDir = key === "player_name" || key === "team_abbr" ? "asc" : "desc";
          }}
          renderTeamTable();
        }});
      }});
      document.querySelectorAll("#multi-table thead th.sortable").forEach(th => {{
        th.addEventListener("click", () => {{
          const key = th.dataset.key;
          if (multiSortKey === key) {{
            multiSortDir = multiSortDir === "desc" ? "asc" : "desc";
          }} else {{
            multiSortKey = key;
            multiSortDir = key === "player_name" || key === "team_abbr" ? "asc" : "desc";
          }}
          renderMultiTable();
        }});
      }});

      renderTeamTable();
      renderLeaderboard();
      renderMultiTable();
    }}

    init();
  </script>
</body>
</html>"""

    output_data_path.write_text(html, encoding="utf-8")
    output_site_path.write_text(html, encoding="utf-8")
    print(f"Report saved to: {output_data_path.absolute()}")
    print(f"Also saved to: {output_site_path.absolute()}")
    return output_data_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate regular-season on/off HTML report.")
    parser.add_argument("--onoff-path", default=None, help="Override primary on/off CSV path.")
    parser.add_argument("--onoff-pre2006-path", default=None, help="Override pre-2006 on/off CSV path.")
    parser.add_argument("--onoff-paths", default=None, help="Comma-separated on/off CSV inputs. Relative paths resolve under data/.")
    parser.add_argument("--possession-paths", default=None, help="Comma-separated possession CSV inputs. Relative paths resolve under data/.")
    parser.add_argument("--output-data-path", default=None, help="Override output HTML path under data/.")
    parser.add_argument("--output-site-path", default=None, help="Override local site HTML output path.")
    parser.add_argument("--skip-pbpstats", action="store_true", help="Deprecated compatibility flag; report now uses rebuilt possessions only.")
    args = parser.parse_args()

    if args.onoff_paths:
        onoff_paths = _parse_csv_paths(args.onoff_paths, [])
    else:
        defaults = [Path(args.onoff_pre2006_path)] if args.onoff_pre2006_path else [ONOFF_PRE2006_PATH]
        defaults.append(Path(args.onoff_path) if args.onoff_path else ONOFF_PATH)
        onoff_paths = defaults
    possession_paths = _parse_csv_paths(args.possession_paths, [POSS_HIST_PATH, POSS_PATH])
    output_data_path = Path(args.output_data_path) if args.output_data_path else OUTPUT_DATA_PATH
    output_site_path = Path(args.output_site_path) if args.output_site_path else OUTPUT_SITE_PATH

    generate_onoff_report(
        onoff_paths=onoff_paths,
        possession_paths=possession_paths,
        output_data_path=output_data_path,
        output_site_path=output_site_path,
        skip_pbpstats=args.skip_pbpstats,
    )
