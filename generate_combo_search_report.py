from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DB_PATH = Path(os.environ.get("NBA_ANALYTICS_DB_PATH", str(DATA_DIR / "nba_analytics.duckdb")))
OUTPUT_DATA_PATH = Path(os.environ.get("COMBO_SEARCH_OUTPUT_DATA_PATH", str(DATA_DIR / "combo_search.html")))
OUTPUT_SITE_PATH = Path(os.environ.get("COMBO_SEARCH_OUTPUT_SITE_PATH", str(ROOT / "combo-search.html")))
CHUNK_DIR = Path(os.environ.get("COMBO_SEARCH_CHUNK_DIR", str(DATA_DIR / "combo_chunks")))
PAGE_TITLE = os.environ.get("COMBO_SEARCH_PAGE_TITLE", "Combo Search")
REGULAR_GAME_SEARCH_HREF = os.environ.get("REGULAR_GAME_SEARCH_HREF", "game-search.html")
REGULAR_SPAN_SEARCH_HREF = os.environ.get("REGULAR_SPAN_SEARCH_HREF", "player-span-search.html")
PLAYOFF_GAME_SEARCH_HREF = os.environ.get("PLAYOFF_GAME_SEARCH_HREF", "game-search-playoffs.html")
PLAYOFF_SPAN_SEARCH_HREF = os.environ.get("PLAYOFF_SPAN_SEARCH_HREF", "player-span-search-playoffs.html")
LOG_SEASONS_RAW = os.environ.get("COMBO_SEARCH_LOG_SEASONS", "latest")


def _season_start(season: str) -> int:
    return int(str(season).split("-")[0])


def _season_slug(season: str) -> str:
    return season.replace("-", "_")


def _fetch_all(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[list[str], list[list]]:
    rows = con.execute(sql).fetchall()
    cols = [d[0] for d in con.description]
    return cols, [list(r) for r in rows]


def _log(message: str) -> None:
    print(message, flush=True)


def _stream_query_rows_to_js(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: list | tuple,
    output_path: Path,
    prefix: str,
) -> int:
    cur = con.execute(sql, params)
    row_count = 0
    first = True
    with output_path.open("w", encoding="utf-8") as fh:
        fh.write(prefix)
        while True:
            batch = cur.fetchmany(5000)
            if not batch:
                break
            for row in batch:
                if not first:
                    fh.write(",")
                fh.write(json.dumps(list(row), ensure_ascii=False,
                                    separators=(",", ":"), default=str))
                first = False
                row_count += 1
        fh.write("];\n")
    return row_count


def generate_combo_search_report() -> Path:
    sys.stdout.reconfigure(line_buffering=True)
    _log(f"Opening DB: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH), read_only=True)
    available_tables = {row[0] for row in con.execute("show tables").fetchall()}
    table_specs = [
        (2, "combo_2man_agg", "combo_id", ["p1", "p2"]),
        (3, "combo_3man_agg", "combo_id", ["p1", "p2", "p3"]),
        (4, "combo_4man_agg", "combo_id", ["p1", "p2", "p3", "p4"]),
        (5, "lineup_5man_agg", "lineup_id", ["p1", "p2", "p3", "p4", "p5"]),
    ]
    active_specs = [spec for spec in table_specs if spec[1] in available_tables]
    if not active_specs:
        raise RuntimeError("No combo or lineup aggregate tables found in analytics DB.")
    if "combo_game_facts" not in available_tables:
        raise RuntimeError("combo_game_facts is missing from analytics DB. Rebuild analytics first.")

    union_parts: list[str] = []
    for combo_size, table_name, id_col, players in active_specs:
        padded = players + [None] * (5 - len(players))
        player_exprs = []
        for idx, col in enumerate(padded, start=1):
            if col is None:
                player_exprs.append(f"CAST(NULL AS BIGINT) AS p{idx}")
            else:
                player_exprs.append(f"{col} AS p{idx}")
        union_parts.append(
            f"""
            SELECT
                {combo_size} AS combo_size,
                season,
                c.team_id,
                COALESCE(c.team_abbr, t.team_abbr) AS team_abbr,
                {id_col} AS unit_id,
                {", ".join(player_exprs)},
                games, stints, seconds, minutes, poss_est,
                pts_for_raw, pts_against_raw, net_raw,
                pts_for_adj, pts_against_adj, net_adj, net_delta,
                pts_for_adj_3pt_ft, pts_against_adj_3pt_ft,
                net_adj_3pt_ft, net_delta_3pt_ft
            FROM {table_name} c
            LEFT JOIN team_names t ON c.team_id = t.team_id
            """
        )

    common_cte = f"""
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
        combined AS (
            {" UNION ALL ".join(union_parts)}
        ),
        split_rollups AS (
            SELECT
                combo_size,
                season,
                team_id,
                team_abbr,
                combo_id AS unit_id,
                COUNT(*) AS game_logs,
                SUM(off_poss) AS off_poss,
                SUM(def_poss) AS def_poss,
                SUM(pts_for_raw_off) AS pts_for_raw_off,
                SUM(pts_against_raw_def) AS pts_against_raw_def,
                SUM(pts_for_adj_off) AS pts_for_adj_off,
                SUM(pts_against_adj_def) AS pts_against_adj_def,
                SUM(pts_for_adj_off_3pt_ft) AS pts_for_adj_off_3pt_ft,
                SUM(pts_against_adj_def_3pt_ft) AS pts_against_adj_def_3pt_ft,
                100.0 * SUM(pts_for_raw_off) / NULLIF(SUM(off_poss), 0) AS ortg_raw,
                100.0 * SUM(pts_against_raw_def) / NULLIF(SUM(def_poss), 0) AS drtg_raw,
                100.0 * SUM(pts_for_adj_off) / NULLIF(SUM(off_poss), 0) AS ortg_adj,
                100.0 * SUM(pts_against_adj_def) / NULLIF(SUM(def_poss), 0) AS drtg_adj,
                100.0 * SUM(pts_for_adj_off_3pt_ft) / NULLIF(SUM(off_poss), 0) AS ortg_adj_3pt_ft,
                100.0 * SUM(pts_against_adj_def_3pt_ft) / NULLIF(SUM(def_poss), 0) AS drtg_adj_3pt_ft
            FROM combo_game_facts
            GROUP BY ALL
        )
    """
    aggregate_select = """
        SELECT
            c.combo_size,
            c.season,
            c.team_id,
            c.team_abbr,
            c.unit_id,
            c.p1, pn1.player_name AS p1_name,
            c.p2, pn2.player_name AS p2_name,
            c.p3, pn3.player_name AS p3_name,
            c.p4, pn4.player_name AS p4_name,
            c.p5, pn5.player_name AS p5_name,
            c.games, c.stints, c.seconds, c.minutes, c.poss_est,
            c.pts_for_raw, c.pts_against_raw, c.net_raw,
            c.pts_for_adj, c.pts_against_adj, c.net_adj, c.net_delta,
            c.pts_for_adj_3pt_ft, c.pts_against_adj_3pt_ft,
            c.net_adj_3pt_ft, c.net_delta_3pt_ft,
            sr.game_logs,
            sr.off_poss, sr.def_poss,
            sr.pts_for_raw_off, sr.pts_against_raw_def,
            sr.pts_for_adj_off, sr.pts_against_adj_def,
            sr.pts_for_adj_off_3pt_ft, sr.pts_against_adj_def_3pt_ft,
            sr.ortg_raw, sr.drtg_raw, sr.ortg_adj, sr.drtg_adj,
            sr.ortg_adj_3pt_ft, sr.drtg_adj_3pt_ft
        FROM combined c
        LEFT JOIN split_rollups sr
          ON c.combo_size = sr.combo_size
         AND c.season = sr.season
         AND c.team_id = sr.team_id
         AND c.unit_id = sr.unit_id
        LEFT JOIN player_names pn1 ON c.p1 = pn1.player_id
        LEFT JOIN player_names pn2 ON c.p2 = pn2.player_id
        LEFT JOIN player_names pn3 ON c.p3 = pn3.player_id
        LEFT JOIN player_names pn4 ON c.p4 = pn4.player_id
        LEFT JOIN player_names pn5 ON c.p5 = pn5.player_id
    """
    aggregate_schema_sql = f"{common_cte}\n{aggregate_select}\nLIMIT 0"
    con.execute(aggregate_schema_sql)
    agg_cols = [d[0] for d in con.description]
    _log("Loading aggregate metadata...")
    seasons = [
        row[0]
        for row in con.execute(f"{common_cte}\nSELECT DISTINCT season FROM combined WHERE season IS NOT NULL").fetchall()
    ]
    seasons = sorted(seasons, key=_season_start)
    teams = [
        row[0]
        for row in con.execute(
            f"{common_cte}\nSELECT DISTINCT team_abbr FROM combined WHERE team_abbr IS NOT NULL AND team_abbr <> ''"
        ).fetchall()
    ]
    teams = sorted(teams)
    player_season_rows = con.execute(
        f"""
        {common_cte}
        SELECT lower(trim(pn.player_name)) AS player_name_key, season
        FROM (
            SELECT season, p1 AS player_id FROM combined
            UNION ALL SELECT season, p2 AS player_id FROM combined
            UNION ALL SELECT season, p3 AS player_id FROM combined
            UNION ALL SELECT season, p4 AS player_id FROM combined
            UNION ALL SELECT season, p5 AS player_id FROM combined
        ) x
        JOIN player_names pn ON x.player_id = pn.player_id
        WHERE x.player_id IS NOT NULL AND pn.player_name IS NOT NULL AND trim(pn.player_name) <> ''
        """
    ).fetchall()
    player_seasons_by_name: dict[str, list[str]] = {}
    for player_name_key, season in player_season_rows:
        player_seasons_by_name.setdefault(player_name_key, []).append(season)
    player_seasons_by_name = {
        name: sorted(set(v), key=_season_start)
        for name, v in player_seasons_by_name.items()
    }
    _log(f"Indexed metadata for {len(seasons)} seasons, {len(teams)} teams, {len(player_seasons_by_name)} players.")
    default_agg_season = seasons[-1] if seasons else None
    default_agg_rows: list[list] = []
    if default_agg_season:
        _log(f"Loading inline aggregate rows for default season {default_agg_season}...")
        default_agg_rows = [
            list(row)
            for row in con.execute(
                f"{common_cte}\n{aggregate_select}\nWHERE c.season = ?\nORDER BY c.combo_size, c.minutes DESC",
                [default_agg_season],
            ).fetchall()
        ]
        _log(f"Loaded {len(default_agg_rows):,} inline aggregate rows for {default_agg_season}.")

    log_schema_sql = """
        WITH player_names AS (
            SELECT CAST(player_id AS BIGINT) AS player_id, any_value(player_name) AS player_name
            FROM player_game_facts
            GROUP BY 1
        )
        SELECT
            combo_size,
            date,
            season,
            game_id,
            team_id,
            team_abbr,
            opp_team_id,
            opp_team_abbr,
            combo_id,
            p1, pn1.player_name AS p1_name,
            p2, pn2.player_name AS p2_name,
            p3, pn3.player_name AS p3_name,
            p4, pn4.player_name AS p4_name,
            p5, pn5.player_name AS p5_name,
            stints, seconds, minutes, poss_est,
            off_poss, def_poss,
            pts_for_raw_off, pts_against_raw_def,
            pts_for_adj_off, pts_against_adj_def,
            pts_for_adj_off_3pt_ft, pts_against_adj_def_3pt_ft,
            ortg_raw, drtg_raw, net_raw,
            ortg_adj, drtg_adj, net_adj, net_delta,
            ortg_adj_3pt_ft, drtg_adj_3pt_ft, net_adj_3pt_ft, net_delta_3pt_ft
        FROM combo_game_facts g
        LEFT JOIN player_names pn1 ON g.p1 = pn1.player_id
        LEFT JOIN player_names pn2 ON g.p2 = pn2.player_id
        LEFT JOIN player_names pn3 ON g.p3 = pn3.player_id
        LEFT JOIN player_names pn4 ON g.p4 = pn4.player_id
        LEFT JOIN player_names pn5 ON g.p5 = pn5.player_id
        LIMIT 0
    """
    _log("Loading log schema...")
    con.execute(log_schema_sql)
    log_cols = [d[0] for d in con.description]

    CHUNK_DIR.mkdir(parents=True, exist_ok=True)
    _log(f"Writing aggregate chunks to {CHUNK_DIR} ...")
    agg_files: dict[str, str] = {}
    total_agg_rows = 0
    for season in seasons:
        filename = f"agg_{_season_slug(season)}.js"
        agg_files[season] = filename
        prefix = (
            "window.__COMBO_AGG_CHUNKS = window.__COMBO_AGG_CHUNKS || {};\n"
            f"window.__COMBO_AGG_CHUNKS[{json.dumps(season)}] = "
            "["
        )
        row_count = _stream_query_rows_to_js(
            con,
            f"{common_cte}\n{aggregate_select}\nWHERE c.season = ?\nORDER BY c.combo_size, c.minutes DESC",
            [season],
            CHUNK_DIR / filename,
            prefix,
        )
        total_agg_rows += row_count
        _log(f"  agg {season}: {row_count:,} rows")

    log_seasons_mode = LOG_SEASONS_RAW.strip().lower()
    if log_seasons_mode == "none":
        log_seasons = []
    elif log_seasons_mode == "all":
        log_seasons = seasons
    elif log_seasons_mode == "latest":
        log_seasons = seasons[-1:] if seasons else []
    else:
        requested = {part.strip() for part in LOG_SEASONS_RAW.split(",") if part.strip()}
        log_seasons = [season for season in seasons if season in requested]

    log_files: dict[str, dict[str, str]] = {str(spec[0]): {} for spec in active_specs}
    log_query = """
        WITH player_names AS (
            SELECT CAST(player_id AS BIGINT) AS player_id, any_value(player_name) AS player_name
            FROM player_game_facts
            GROUP BY 1
        )
        SELECT
            combo_size,
            date,
            season,
            game_id,
            team_id,
            team_abbr,
            opp_team_id,
            opp_team_abbr,
            combo_id,
            p1, pn1.player_name AS p1_name,
            p2, pn2.player_name AS p2_name,
            p3, pn3.player_name AS p3_name,
            p4, pn4.player_name AS p4_name,
            p5, pn5.player_name AS p5_name,
            stints, seconds, minutes, poss_est,
            off_poss, def_poss,
            pts_for_raw_off, pts_against_raw_def,
            pts_for_adj_off, pts_against_adj_def,
            pts_for_adj_off_3pt_ft, pts_against_adj_def_3pt_ft,
            ortg_raw, drtg_raw, net_raw,
            ortg_adj, drtg_adj, net_adj, net_delta,
            ortg_adj_3pt_ft, drtg_adj_3pt_ft, net_adj_3pt_ft, net_delta_3pt_ft
        FROM combo_game_facts g
        LEFT JOIN player_names pn1 ON g.p1 = pn1.player_id
        LEFT JOIN player_names pn2 ON g.p2 = pn2.player_id
        LEFT JOIN player_names pn3 ON g.p3 = pn3.player_id
        LEFT JOIN player_names pn4 ON g.p4 = pn4.player_id
        LEFT JOIN player_names pn5 ON g.p5 = pn5.player_id
        WHERE combo_size = ? AND season = ?
        ORDER BY date DESC, minutes DESC
    """
    _log(f"Writing log chunks for {len(log_seasons)} seasons...")
    for combo_size, *_ in active_specs:
        for season in log_seasons:
            filename = f"log_{combo_size}_{_season_slug(season)}.js"
            prefix = (
                "window.__COMBO_LOG_CHUNKS = window.__COMBO_LOG_CHUNKS || {};\n"
                f"window.__COMBO_LOG_CHUNKS[{json.dumps(str(combo_size))}] = "
                f"window.__COMBO_LOG_CHUNKS[{json.dumps(str(combo_size))}] || {{}};\n"
                f"window.__COMBO_LOG_CHUNKS[{json.dumps(str(combo_size))}][{json.dumps(season)}] = ["
            )
            row_count = _stream_query_rows_to_js(
                con,
                log_query,
                [combo_size, season],
                CHUNK_DIR / filename,
                prefix,
            )
            if not row_count:
                try:
                    (CHUNK_DIR / filename).unlink()
                except FileNotFoundError:
                    pass
                continue
            log_files[str(combo_size)][season] = filename
            _log(f"  logs {combo_size}-man {season}: {row_count:,} rows")

    con.close()
    _log("Rendering HTML...")

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    available_sizes = [spec[0] for spec in active_specs]
    size_options = "\n".join(
        f'            <option value="{size}">{size}-Man</option>'
        for size in available_sizes
    )
    if not log_seasons:
        log_note = "Game log chunks are not prebuilt in this artifact."
    elif len(log_seasons) == len(seasons):
        log_note = "Game Logs loads per-season, per-size game rows only for the selected seasons, so player-name searches can stay narrow instead of loading the full universe."
    else:
        season_label = ", ".join(log_seasons)
        log_note = f"Game Logs is prebuilt only for {season_label} in this local review artifact; season combo search spans the full indexed history."

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{PAGE_TITLE}</title>
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
    .wrap {{ max-width: 1640px; margin: 0 auto; padding: 18px; }}
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
    .sub {{ margin-top: 8px; color: #d6e8ff; font-size: 14px; }}
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
    .controls {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 10px;
      margin-bottom: 10px;
    }}
    label {{ display: grid; gap: 5px; font-size: 12px; color: var(--muted); }}
    input, select {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px 10px;
      font: inherit;
      background: #fff;
      color: var(--ink);
    }}
    .actions {{ display:flex; gap:10px; flex-wrap:wrap; align-items:center; margin-top: 6px; }}
    .picker {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 6px;
      margin-top: 6px;
    }}
    .picker select {{
      min-width: 0;
    }}
    button {{
      border: 1px solid #0d4f8b;
      background: #0d4f8b;
      color: #fff;
      border-radius: 8px;
      padding: 9px 14px;
      font-weight: 600;
      cursor: pointer;
    }}
    button.secondary {{
      background: #fff;
      color: var(--ink);
      border-color: var(--line);
    }}
    .meta {{ color: var(--muted); font-size: 12px; }}
    .note {{ color: var(--muted); font-size: 13px; margin-top: 4px; }}
    .table-wrap {{ overflow: auto; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1280px; }}
    th, td {{ border-bottom: 1px solid #e8edf5; padding: 8px 9px; text-align: left; font-size: 13px; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: #f8fbff; z-index: 1; }}
    td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .players {{ font-weight: 600; }}
    .good {{ color: var(--good); }}
    .bad {{ color: var(--bad); }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>{PAGE_TITLE}</h1>
      <div class="sub">Season combos and game logs over the same lineup-derived combo engine, with raw and adjusted offense-defense splits.</div>
      <div class="nav">
        <a href="index.html">Home</a>
        <a href="onoff.html">+/- Seasons</a>
        <a href="onoff-daily.html">+/- Games</a>
        <a href="rapm.html">RAPM</a>
        <a href="playoff-series.html">Playoff Series</a>
        <a href="combo-search.html">Combinations</a>
        <a href="example.html">Method</a>
      </div>
    </div>

    <div class="card">
      <div class="controls">
        <label>Result Mode
          <select id="resultMode">
            <option value="aggregate">Season Combos</option>
            <option value="logs">Game Logs</option>
          </select>
        </label>
        <label>Shooting Luck
          <select id="luckMode">
            <option value="default">3PT + FT + 50% Midrange</option>
            <option value="3pt_ft">3PT + FT Only</option>
          </select>
        </label>
        <label>Combo Size
          <select id="comboSize">
{size_options}
          </select>
        </label>
        <label>Season Start
          <select id="seasonStart"></select>
        </label>
        <label>Season End
          <select id="seasonEnd"></select>
        </label>
        <label>Team
          <select id="teamFilter"></select>
        </label>
        <label>Opponent
          <select id="oppFilter"></select>
        </label>
        <label>Include Players
          <input id="includePlayers" list="playerSuggestions" placeholder="Brunson, Hart" />
          <span class="picker">
            <select id="includePlayerSelect"></select>
            <button type="button" class="secondary" id="includeAddBtn">Add</button>
          </span>
        </label>
        <label>Exclude Players
          <input id="excludePlayers" list="playerSuggestions" placeholder="Randle" />
          <span class="picker">
            <select id="excludePlayerSelect"></select>
            <button type="button" class="secondary" id="excludeAddBtn">Add</button>
          </span>
        </label>
        <label>Min Minutes
          <input id="minMinutes" type="number" min="0" step="1" value="200" />
        </label>
        <label>Min Possessions
          <input id="minPoss" type="number" min="0" step="1" value="400" />
        </label>
        <label>Sort By
          <select id="sortBy">
            <option value="net_adj">Adj Net</option>
            <option value="net_raw">Raw Net</option>
            <option value="net_delta">Luck Delta</option>
            <option value="ortg_adj">PF Adj/100</option>
            <option value="drtg_adj">PA Adj/100</option>
            <option value="ortg_raw">PF Raw/100</option>
            <option value="drtg_raw">PA Raw/100</option>
            <option value="off_poss">Off Poss</option>
            <option value="def_poss">Def Poss</option>
            <option value="minutes">Minutes</option>
            <option value="games">Games</option>
          </select>
        </label>
      </div>
      <div class="actions">
        <button id="searchBtn">Search</button>
        <button class="secondary" id="clearBtn">Clear</button>
        <span class="meta" id="status">{total_agg_rows:,} season combo rows indexed across all sizes</span>
      </div>
      <div class="note">The default removes 100% of estimated 3PT and free-throw luck and 50% of estimated midrange luck. `Season Combos` uses aggregate rows. {log_note} Player filters can be typed manually or added from the filtered player pickers.</div>
    </div>

    <div class="card table-wrap">
      <table>
        <thead><tr id="headerRow"></tr></thead>
        <tbody id="resultsBody"></tbody>
      </table>
    </div>

    <p class="meta">Generated {ts} from {DB_PATH.name}</p>
  </div>

  <script>
    const AGG_COLS = {json.dumps(agg_cols, ensure_ascii=False)};
    const AGG_IDX = Object.fromEntries(AGG_COLS.map((c, i) => [c, i]));
    const LOG_COLS = {json.dumps(log_cols, ensure_ascii=False)};
    const LOG_IDX = Object.fromEntries(LOG_COLS.map((c, i) => [c, i]));
    const SEASONS = {json.dumps(seasons, ensure_ascii=False)};
    const AVAILABLE_SIZES = {json.dumps(available_sizes)};
    const TEAMS = ["", ...{json.dumps(teams, ensure_ascii=False)}];
    const AGG_FILES = {json.dumps(agg_files, ensure_ascii=False)};
    const LOG_FILES = {json.dumps(log_files, ensure_ascii=False)};
    const PLAYER_SEASONS = {json.dumps(player_seasons_by_name, ensure_ascii=False)};
    window.__COMBO_AGG_CHUNKS = window.__COMBO_AGG_CHUNKS || {{}};
    {"window.__COMBO_AGG_CHUNKS[" + json.dumps(default_agg_season, ensure_ascii=False) + "] = " + json.dumps(default_agg_rows, ensure_ascii=False) + ";" if default_agg_season else ""}
    window.__COMBO_LOG_CHUNKS = window.__COMBO_LOG_CHUNKS || {{}};
    const PAGE_PATH = String(window.location.pathname || "").replace(/\\\\/g, "/").toLowerCase();
    const CHUNK_BASE = PAGE_PATH.includes("/data/") ? "combo_chunks" : "data/combo_chunks";

    const AGG_HEADERS = [
      ["Players", "players"],
      ["Season", "season"],
      ["Team", "team_abbr"],
      ["Games", "games", "num"],
      ["Min", "minutes", "num"],
      ["Off Poss", "off_poss", "num"],
      ["Def Poss", "def_poss", "num"],
      ["PF Raw/100", "ortg_raw", "num"],
      ["PA Raw/100", "drtg_raw", "num"],
      ["Raw Net", "net_raw", "num"],
      ["PF Adj/100", "ortg_adj", "num"],
      ["PA Adj/100", "drtg_adj", "num"],
      ["Adj Net", "net_adj", "num"],
      ["Luck Delta", "net_delta", "num"],
    ];
    const LOG_HEADERS = [
      ["Players", "players"],
      ["Date", "date"],
      ["Season", "season"],
      ["Team", "team_abbr"],
      ["Opp", "opp_team_abbr"],
      ["Min", "minutes", "num"],
      ["Off Poss", "off_poss", "num"],
      ["Def Poss", "def_poss", "num"],
      ["PF Raw/100", "ortg_raw", "num"],
      ["PA Raw/100", "drtg_raw", "num"],
      ["Raw Net", "net_raw", "num"],
      ["PF Adj/100", "ortg_adj", "num"],
      ["PA Adj/100", "drtg_adj", "num"],
      ["Adj Net", "net_adj", "num"],
      ["Luck Delta", "net_delta", "num"],
    ];
    const DRTG_FIELDS = new Set(["drtg_raw", "drtg_adj"]);
    const DROPDOWN_SORT_KEYS = new Set(["net_adj", "net_raw", "net_delta", "ortg_adj", "drtg_adj", "ortg_raw", "drtg_raw", "off_poss", "def_poss", "minutes", "games"]);

    const $ = (id) => document.getElementById(id);
    let sortState = {{ key: "net_adj", dir: -1 }};
    let currentLoadedRows = [];

    function titleCaseName(name) {{
      return String(name || "").split(" ").map((part) => {{
        if (!part) return "";
        return part[0].toUpperCase() + part.slice(1);
      }}).join(" ");
    }}

    function buildPlayerSuggestionList(rows = currentLoadedRows) {{
      let list = $("playerSuggestions");
      if (!list) {{
        list = document.createElement("datalist");
        list.id = "playerSuggestions";
        document.body.appendChild(list);
      }}
      const comboSize = Number($("comboSize").value || 2);
      const team = $("teamFilter").value;
      let names = [];
      if (rows.length) {{
        const idx = currentMode() === "logs" ? LOG_IDX : AGG_IDX;
        const seen = new Set();
        for (const row of rows) {{
          if (Number(row[idx.combo_size]) !== comboSize) continue;
          if (team && displayTeamAbbr(row[idx.team_abbr], row[idx.season]) !== team) continue;
          for (const name of rowPlayers(row, idx)) {{
            const normalized = normalizeName(name);
            if (normalized) seen.add(normalized);
          }}
        }}
        names = Array.from(seen);
      }}
      if (!names.length) {{
        names = Object.keys(PLAYER_SEASONS || {{}});
      }}
      names.sort((a, b) => a.localeCompare(b));
      list.innerHTML = names.map((name) => `<option value="${{titleCaseName(name)}}"></option>`).join("");
      fillPlayerSelect($("includePlayerSelect"), names);
      fillPlayerSelect($("excludePlayerSelect"), names);
    }}

    function fillPlayerSelect(el, names) {{
      if (!el) return;
      const options = ['<option value="">Select player...</option>']
        .concat(names.map((name) => `<option value="${{titleCaseName(name)}}">${{titleCaseName(name)}}</option>`));
      el.innerHTML = options.join("");
      el.value = "";
    }}

    function addSelectedPlayer(selectId, inputId) {{
      const select = $(selectId);
      const input = $(inputId);
      const picked = String(select.value || "").trim();
      if (!picked) return;
      const existing = String(input.value || "").split(",").map((v) => v.trim()).filter(Boolean);
      const lowered = new Set(existing.map((v) => v.toLowerCase()));
      if (!lowered.has(picked.toLowerCase())) {{
        existing.push(picked);
        input.value = existing.join(", ");
      }}
      select.value = "";
    }}

    function formatNum(v, digits = 1) {{
      const n = Number(v);
      return Number.isFinite(n) ? n.toFixed(digits) : "";
    }}

    function normalizeName(s) {{
      return String(s || "").trim().toLowerCase();
    }}

    function seasonStartYear(season) {{
      return Number(String(season || "").split("-")[0] || 0);
    }}

    function displayTeamAbbr(teamAbbr, season) {{
      const abbr = String(teamAbbr || "");
      return abbr === "SEA" && seasonStartYear(season) >= 2008 ? "OKC" : abbr;
    }}

    function parseNames(raw) {{
      return String(raw || "").split(",").map(normalizeName).filter(Boolean);
    }}

    function currentMode() {{
      return $("resultMode").value;
    }}

    function idxForMode() {{
      return currentMode() === "logs" ? LOG_IDX : AGG_IDX;
    }}

    function metricKey(baseKey) {{
      if ($("luckMode").value === "3pt_ft" && ["ortg_adj", "drtg_adj", "net_adj", "net_delta"].includes(baseKey)) {{
        return `${{baseKey}}_3pt_ft`;
      }}
      return baseKey;
    }}

    function rowPlayers(r, idx) {{
      const names = [];
      for (const key of ["p1_name","p2_name","p3_name","p4_name","p5_name"]) {{
        const v = r[idx[key]];
        if (v) names.push(String(v));
      }}
      return names;
    }}

    function comboPoss(r, idx) {{
      const off = Number(r[idx.off_poss] || 0);
      const def = Number(r[idx.def_poss] || 0);
      return off + def;
    }}

    async function ensureAggSeasonLoaded(season) {{
      if (window.__COMBO_AGG_CHUNKS[season]) return;
      const file = AGG_FILES[season];
      if (!file) return;
      await new Promise((resolve, reject) => {{
        const s = document.createElement("script");
        s.src = `${{CHUNK_BASE}}/${{file}}`;
        s.onload = resolve;
        s.onerror = () => reject(new Error(`Failed to load aggregate chunk for ${{season}}`));
        document.head.appendChild(s);
      }});
    }}

    async function ensureLogSeasonLoaded(comboSize, season) {{
      const sizeKey = String(comboSize);
      window.__COMBO_LOG_CHUNKS[sizeKey] = window.__COMBO_LOG_CHUNKS[sizeKey] || {{}};
      if (window.__COMBO_LOG_CHUNKS[sizeKey][season]) return;
      const file = (LOG_FILES[sizeKey] || {{}})[season];
      if (!file) return;
      await new Promise((resolve, reject) => {{
        const s = document.createElement("script");
        s.src = `${{CHUNK_BASE}}/${{file}}`;
        s.onload = resolve;
        s.onerror = () => reject(new Error(`Failed to load log chunk for ${{season}}`));
        document.head.appendChild(s);
      }});
    }}

    function selectedSeasons() {{
      const start = $("seasonStart").value;
      const end = $("seasonEnd").value;
      const include = parseNames($("includePlayers").value);
      let selected = SEASONS.filter((s) => (!start || s >= start) && (!end || s <= end));
      if (include.length === 1) {{
        const playerSeasons = PLAYER_SEASONS[include[0]] || [];
        const allowed = new Set(playerSeasons);
        selected = selected.filter((s) => allowed.has(s));
      }}
      return selected;
    }}

    async function loadSelectedRows() {{
      const seasons = selectedSeasons();
      const comboSize = Number($("comboSize").value || 2);
      const mode = currentMode();
      const rows = [];
      for (let i = 0; i < seasons.length; i += 1) {{
        const season = seasons[i];
        $("status").textContent = `Loading combo data (${{i + 1}}/${{seasons.length}}): ${{season}}...`;
        if (mode === "logs") {{
          await ensureLogSeasonLoaded(comboSize, season);
          rows.push(...((window.__COMBO_LOG_CHUNKS[String(comboSize)] || {{}})[season] || []));
        }} else {{
          await ensureAggSeasonLoaded(season);
          rows.push(...(window.__COMBO_AGG_CHUNKS[season] || []));
        }}
      }}
      return rows;
    }}

    function matchFilters(r) {{
      const idx = idxForMode();
      const comboSize = Number($("comboSize").value || 2);
      if (Number(r[idx.combo_size]) !== comboSize) return false;
      const team = $("teamFilter").value;
      if (team && displayTeamAbbr(r[idx.team_abbr], r[idx.season]) !== team) return false;
      if (currentMode() === "logs") {{
        const opp = $("oppFilter").value;
        if (opp && String(r[idx.opp_team_abbr] || "") !== opp) return false;
      }}
      if (Number(r[idx.minutes] || 0) < Number($("minMinutes").value || 0)) return false;
      if (comboPoss(r, idx) < Number($("minPoss").value || 0)) return false;

      const names = rowPlayers(r, idx).map(normalizeName);
      const include = parseNames($("includePlayers").value);
      const exclude = parseNames($("excludePlayers").value);
      if (include.some((n) => !names.includes(n))) return false;
      if (exclude.some((n) => names.includes(n))) return false;
      return true;
    }}

    function renderHeaders() {{
      const headerRow = $("headerRow");
      const headers = currentMode() === "logs" ? LOG_HEADERS : AGG_HEADERS;
      headerRow.innerHTML = headers.map(([label, key, cls]) => {{
        const active = sortState.key === key;
        const arrow = active ? (sortState.dir > 0 ? " ▲" : " ▼") : "";
        return `<th class="${{cls || ""}}" data-sort="${{key}}" style="cursor:pointer">${{label}}${{arrow}}</th>`;
      }}).join("");
      headerRow.querySelectorAll("th[data-sort]").forEach((th) => {{
        th.addEventListener("click", () => {{
          const key = th.getAttribute("data-sort");
          if (sortState.key === key) {{
            sortState.dir *= -1;
          }} else {{
            sortState.key = key;
            sortState.dir = DRTG_FIELDS.has(key) ? 1 : -1;
          }}
          if (DROPDOWN_SORT_KEYS.has(sortState.key)) {{
            $("sortBy").value = sortState.key;
          }}
          runSearch();
        }});
      }});
      $("oppFilter").disabled = currentMode() !== "logs";
    }}

    function renderRows(rows) {{
      const idx = idxForMode();
      const body = $("resultsBody");
      body.innerHTML = "";
      if (!rows.length) {{
        const colspan = currentMode() === "logs" ? LOG_HEADERS.length : AGG_HEADERS.length;
        body.innerHTML = `<tr><td colspan="${{colspan}}">No combos matched.</td></tr>`;
        return;
      }}
      rows.sort((a, b) => {{
        const sortKey = metricKey(sortState.key);
        const av = Number(a[idx[sortKey]] || 0);
        const bv = Number(b[idx[sortKey]] || 0);
        return sortState.dir * (av - bv);
      }});
      for (const r of rows.slice(0, 400)) {{
        const players = rowPlayers(r, idx).join(" / ");
        const ortgAdjKey = metricKey("ortg_adj");
        const drtgAdjKey = metricKey("drtg_adj");
        const netAdjKey = metricKey("net_adj");
        const deltaKey = metricKey("net_delta");
        const delta = Number(r[idx[deltaKey]] || 0);
        const team = displayTeamAbbr(r[idx.team_abbr] || r[idx.team_id] || "", r[idx.season]);
        const opp = r[idx.opp_team_abbr] || "";
        const cells = currentMode() === "logs"
          ? [
              `<td class="players">${{players}}</td>`,
              `<td>${{r[idx.date] || ""}}</td>`,
              `<td>${{r[idx.season] || ""}}</td>`,
              `<td>${{team}}</td>`,
              `<td>${{opp}}</td>`,
              `<td class="num">${{formatNum(r[idx.minutes], 1)}}</td>`,
              `<td class="num">${{formatNum(r[idx.off_poss], 0)}}</td>`,
              `<td class="num">${{formatNum(r[idx.def_poss], 0)}}</td>`,
              `<td class="num">${{formatNum(r[idx.ortg_raw], 1)}}</td>`,
              `<td class="num">${{formatNum(r[idx.drtg_raw], 1)}}</td>`,
              `<td class="num">${{formatNum(r[idx.net_raw], 1)}}</td>`,
              `<td class="num">${{formatNum(r[idx[ortgAdjKey]], 1)}}</td>`,
              `<td class="num">${{formatNum(r[idx[drtgAdjKey]], 1)}}</td>`,
              `<td class="num">${{formatNum(r[idx[netAdjKey]], 1)}}</td>`,
              `<td class="num ${{delta > 0 ? "good" : (delta < 0 ? "bad" : "")}}">${{formatNum(delta, 1)}}</td>`,
            ]
          : [
              `<td class="players">${{players}}</td>`,
              `<td>${{r[idx.season] || ""}}</td>`,
              `<td>${{team}}</td>`,
              `<td class="num">${{Number(r[idx.games] || r[idx.game_logs] || 0)}}</td>`,
              `<td class="num">${{formatNum(r[idx.minutes], 1)}}</td>`,
              `<td class="num">${{formatNum(r[idx.off_poss], 0)}}</td>`,
              `<td class="num">${{formatNum(r[idx.def_poss], 0)}}</td>`,
              `<td class="num">${{formatNum(r[idx.ortg_raw], 1)}}</td>`,
              `<td class="num">${{formatNum(r[idx.drtg_raw], 1)}}</td>`,
              `<td class="num">${{formatNum(r[idx.net_raw], 1)}}</td>`,
              `<td class="num">${{formatNum(r[idx[ortgAdjKey]], 1)}}</td>`,
              `<td class="num">${{formatNum(r[idx[drtgAdjKey]], 1)}}</td>`,
              `<td class="num">${{formatNum(r[idx[netAdjKey]], 1)}}</td>`,
              `<td class="num ${{delta > 0 ? "good" : (delta < 0 ? "bad" : "")}}">${{formatNum(delta, 1)}}</td>`,
            ];
        const tr = document.createElement("tr");
        tr.innerHTML = cells.join("");
        body.appendChild(tr);
      }}
    }}

    async function runSearch() {{
      try {{
        renderHeaders();
        const loaded = await loadSelectedRows();
        currentLoadedRows = loaded;
        buildPlayerSuggestionList();
        const filtered = loaded.filter(matchFilters);
        const label = currentMode() === "logs" ? "game logs" : "season combos";
        $("status").textContent = `${{filtered.length.toLocaleString()}} ${{label}} matched`;
        renderRows(filtered);
      }} catch (err) {{
        $("status").textContent = `Failed to load combo data: ${{err.message}}`;
      }}
    }}

    async function refreshSuggestionsForSelection() {{
      try {{
        const loaded = await loadSelectedRows();
        currentLoadedRows = loaded;
        buildPlayerSuggestionList();
      }} catch (_) {{
        buildPlayerSuggestionList();
      }}
    }}

    function fillSelect(el, values) {{
      el.innerHTML = values.map((v) => `<option value="${{v}}">${{v || "All"}}</option>`).join("");
    }}

    function resetFilters() {{
      $("resultMode").value = "aggregate";
      $("luckMode").value = "default";
      $("comboSize").value = String(AVAILABLE_SIZES[0] || 2);
      $("seasonStart").value = SEASONS[SEASONS.length - 1] || "";
      $("seasonEnd").value = SEASONS[SEASONS.length - 1] || "";
      $("teamFilter").value = "";
      $("oppFilter").value = "";
      $("includePlayers").value = "";
      $("excludePlayers").value = "";
      $("minMinutes").value = "200";
      $("minPoss").value = "400";
      $("sortBy").value = "net_adj";
      sortState = {{ key: "net_adj", dir: -1 }};
    }}

    fillSelect($("seasonStart"), SEASONS);
    fillSelect($("seasonEnd"), SEASONS);
    fillSelect($("teamFilter"), TEAMS);
    fillSelect($("oppFilter"), TEAMS);
    buildPlayerSuggestionList();
    resetFilters();

    $("searchBtn").addEventListener("click", runSearch);
    $("clearBtn").addEventListener("click", () => {{
      resetFilters();
      runSearch();
    }});
    $("includeAddBtn").addEventListener("click", () => addSelectedPlayer("includePlayerSelect", "includePlayers"));
    $("excludeAddBtn").addEventListener("click", () => addSelectedPlayer("excludePlayerSelect", "excludePlayers"));
    $("resultMode").addEventListener("change", () => {{
      if (currentMode() === "logs") {{
        $("minMinutes").value = "1";
        $("minPoss").value = "1";
      }} else {{
        $("minMinutes").value = "200";
        $("minPoss").value = "400";
      }}
      runSearch();
    }});
    $("luckMode").addEventListener("change", runSearch);
    $("sortBy").addEventListener("change", () => {{
      const key = $("sortBy").value;
      sortState = {{
        key,
        dir: DRTG_FIELDS.has(key) ? 1 : -1,
      }};
      runSearch();
    }});
    ["comboSize", "seasonStart", "seasonEnd", "teamFilter"].forEach((id) => {{
      $(id).addEventListener("change", () => {{
        refreshSuggestionsForSelection();
      }});
    }});

    runSearch();
  </script>
</body>
</html>
"""

    OUTPUT_DATA_PATH.write_text(html, encoding="utf-8")
    OUTPUT_SITE_PATH.write_text(html, encoding="utf-8")
    _log(f"Wrote HTML to {OUTPUT_SITE_PATH} and {OUTPUT_DATA_PATH}")
    return OUTPUT_SITE_PATH


if __name__ == "__main__":
    path = generate_combo_search_report()
    print(f"Wrote: {path}")
