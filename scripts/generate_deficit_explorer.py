"""Generate deficit_explorer.html — interactive NBA comeback analysis page."""
import json, math
import pandas as pd
from pathlib import Path

OUT = Path(__file__).parent.parent / "deficit_explorer.html"
CSV = Path(__file__).parent.parent / "data" / "deficit_analysis_games.csv"

df = pd.read_csv(CSV)
df = df.dropna(subset=["deficit_q1","deficit_q2","deficit_q3","home_final","away_final"])

# Compact rows: [season_start_yr, dq1, dq2, dq3, home_won, hpts, apts]
rows = []
for _, r in df.iterrows():
    yr = int(str(r["season"])[:4])
    rows.append([yr, int(r["deficit_q1"]), int(r["deficit_q2"]), int(r["deficit_q3"]),
                 int(r["home_won"]), int(r["home_final"]), int(r["away_final"])])

data_json = json.dumps(rows)

# Season labels for JS
season_labels = {}
for yr in range(1997, 2025):
    season_labels[yr] = f"{yr}-{str(yr+1)[2:]}"
season_labels_json = json.dumps(season_labels)

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>NBA Deficit Explorer</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; }}
  body {{
    font-family: "Segoe UI", Arial, sans-serif;
    background: linear-gradient(180deg,#eef5ff 0%,#f8fbff 30%,#f2f6fb 100%);
    color: #192231; margin: 0; padding: 18px;
  }}
  .hero {{
    background: radial-gradient(circle at 20% 20%,#154f8b 0%,#0d2f53 45%,#081a2f 100%);
    color: #f8fbff; border-radius: 14px; padding: 18px 24px; margin-bottom: 20px;
    border: 1px solid #254b72;
  }}
  .hero h1 {{ margin: 0 0 4px; font-size: 24px; }}
  .hero p  {{ margin: 0; font-size: 13px; color: rgba(255,255,255,.7); }}
  .nav {{ margin-top: 10px; display: flex; gap: 8px; flex-wrap: wrap; }}
  .nav a {{ color:#e8f4ff; text-decoration:none; border:1px solid rgba(255,255,255,.3);
             border-radius:7px; padding:5px 10px; font-size:12px; }}
  .nav a:hover {{ background:rgba(255,255,255,.15); }}

  .controls {{
    background: white; border-radius: 12px; border: 1px solid #d6e1ef;
    padding: 16px 20px; margin-bottom: 18px;
    box-shadow: 0 2px 8px rgba(23,38,62,.06);
    display: flex; flex-wrap: wrap; gap: 24px; align-items: flex-end;
  }}
  .ctrl-group label {{ display: block; font-size: 11px; font-weight: 700;
    text-transform: uppercase; color: #5b6778; margin-bottom: 6px; letter-spacing:.04em; }}
  .ctrl-group select, .ctrl-group input[type=range] {{
    font-size: 14px; padding: 6px 10px; border: 1px solid #c8d8ed;
    border-radius: 8px; background: #f7faff; color: #192231; cursor: pointer;
  }}
  .ctrl-group input[type=range] {{ padding: 4px 0; width: 180px; }}
  .range-val {{ font-size: 13px; font-weight: 600; color: #154f8b; margin-left: 8px; }}

  .tabs {{ display: flex; gap: 4px; margin-bottom: 0; }}
  .tab {{
    padding: 9px 18px; border-radius: 10px 10px 0 0; cursor: pointer;
    font-size: 13px; font-weight: 600; background: #dde9f7; color: #5b6778;
    border: 1px solid #c8d8ed; border-bottom: none; user-select: none;
  }}
  .tab.active {{ background: white; color: #154f8b; border-color: #c8d8ed; }}

  .panel {{
    background: white; border-radius: 0 12px 12px 12px; border: 1px solid #c8d8ed;
    padding: 20px; margin-bottom: 18px; display: none;
    box-shadow: 0 2px 8px rgba(23,38,62,.06);
  }}
  .panel.active {{ display: block; }}

  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ background: #f0f4fb; color: #444; padding: 8px 10px; text-align: right;
        font-weight: 700; font-size: 11px; text-transform: uppercase; letter-spacing:.03em;
        white-space: nowrap; }}
  th:first-child {{ text-align: left; }}
  td {{ padding: 8px 10px; border-bottom: 1px solid #edf2f9; text-align: right;
        white-space: nowrap; }}
  td:first-child {{ text-align: left; font-weight: 600; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #f5f9ff; }}
  .pos {{ color: #1a7a3c; font-weight: 700; }}
  .neg {{ color: #c0392b; font-weight: 700; }}
  .heat-0  {{ background: #fff5f5; }}
  .heat-1  {{ background: #ffe8e8; }}
  .heat-2  {{ background: #ffd0d0; }}
  .heat-3  {{ background: #ffb0b0; }}
  .heat-4  {{ background: #ff8888; color: #fff; }}
  .heat-5  {{ background: #cc4444; color: #fff; }}

  .stat-big {{ text-align: center; padding: 12px 0 20px; }}
  .stat-big .num {{ font-size: 52px; font-weight: 800; color: #154f8b; line-height: 1; }}
  .stat-big .lbl {{ font-size: 13px; color: #5b6778; margin-top: 4px; }}

  svg.chart {{ width: 100%; height: 260px; overflow: visible; }}
  .axis-label {{ font-size: 11px; fill: #888; }}
  .grid-line {{ stroke: #edf2f9; stroke-width: 1; }}
  .series-line {{ fill: none; stroke-width: 2.5; }}
  .dot {{ r: 4; cursor: pointer; }}
  .dot:hover {{ r: 6; }}
  .legend {{ display: flex; flex-wrap: wrap; gap: 14px; margin-top: 10px; font-size: 12px; }}
  .leg-item {{ display: flex; align-items: center; gap: 5px; cursor: pointer; }}
  .leg-swatch {{ width: 18px; height: 4px; border-radius: 2px; }}

  .search-row {{ display: flex; gap: 10px; margin-bottom: 14px; flex-wrap: wrap; align-items: flex-end; }}
  .search-row input[type=text] {{
    padding: 7px 12px; border: 1px solid #c8d8ed; border-radius: 8px;
    font-size: 13px; width: 160px;
  }}
  .search-row select {{ font-size: 13px; padding: 7px 10px; border: 1px solid #c8d8ed;
    border-radius: 8px; background: #f7faff; }}
  .btn {{ padding: 7px 16px; background: #154f8b; color: white; border: none;
    border-radius: 8px; cursor: pointer; font-size: 13px; font-weight: 600; }}
  .btn:hover {{ background: #1a64b0; }}
  #game-table td.win  {{ color: #1a7a3c; font-weight: 700; }}
  #game-table td.loss {{ color: #c0392b; font-weight: 700; }}
  .small {{ font-size: 11px; color: #888; }}
  .note {{ font-size: 12px; color: #5b6778; margin-top: 10px; line-height: 1.6; }}
  .pbar-wrap {{ background: #edf2f9; border-radius: 4px; height: 8px; width: 80px; display: inline-block; vertical-align: middle; margin-left: 6px; }}
  .pbar {{ background: #154f8b; border-radius: 4px; height: 8px; }}
</style>
</head>
<body>

<div class="hero">
  <h1>NBA Deficit Explorer</h1>
  <p>32,955 regular-season games &middot; 1997&ndash;2025 &middot; Does the same deficit mean less in the modern era?</p>
  <div class="nav">
    <a href="index.html">Overview</a>
    <a href="onoff-daily.html">+/- Games</a>
    <a href="playoff-series.html">Series Ratings</a>
    <a href="player-similarity.html">Player Similarity</a>
  </div>
</div>

<!-- Global controls -->
<div class="controls">
  <div class="ctrl-group">
    <label>Quarter to analyse</label>
    <select id="qtr-sel">
      <option value="q3" selected>After Q3 (most informative)</option>
      <option value="q2">After Q2 (halftime)</option>
      <option value="q1">After Q1</option>
    </select>
  </div>
  <div class="ctrl-group">
    <label>Eras shown</label>
    <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:2px">
      <label style="text-transform:none;font-weight:400;font-size:13px"><input type="checkbox" class="era-cb" value="1997-2003" checked> 1997&ndash;2003</label>
      <label style="text-transform:none;font-weight:400;font-size:13px"><input type="checkbox" class="era-cb" value="2004-2010" checked> 2004&ndash;2010</label>
      <label style="text-transform:none;font-weight:400;font-size:13px"><input type="checkbox" class="era-cb" value="2011-2017" checked> 2011&ndash;2017</label>
      <label style="text-transform:none;font-weight:400;font-size:13px"><input type="checkbox" class="era-cb" value="2018-2025" checked> 2018&ndash;2025</label>
    </div>
  </div>
</div>

<!-- Tabs -->
<div class="tabs">
  <div class="tab active" data-tab="winrate">Win Rate Table</div>
  <div class="tab" data-tab="trend">Season Trend</div>
  <div class="tab" data-tab="specific">Specific Deficits</div>
  <div class="tab" data-tab="browse">Game Browser</div>
</div>

<!-- Panel 1: Win Rate Table -->
<div class="panel active" id="panel-winrate">
  <div id="winrate-table-wrap"></div>
  <p class="note">
    <b>How to read:</b> Each cell shows the comeback win % for the trailing team at that deficit, plus sample size.
    Colour intensity = how often comebacks happen. A deficit is from one team's perspective (the home team is trailing by that many points).
  </p>
</div>

<!-- Panel 2: Season Trend -->
<div class="panel" id="panel-trend">
  <div class="ctrl-group" style="margin-bottom:16px">
    <label>Deficit bucket to track</label>
    <select id="bucket-sel">
      <option value="1-3">1&ndash;3 pts</option>
      <option value="4-6">4&ndash;6 pts</option>
      <option value="7-9">7&ndash;9 pts</option>
      <option value="10-12">10&ndash;12 pts</option>
      <option value="13-15" selected>13&ndash;15 pts</option>
      <option value="16-20">16&ndash;20 pts</option>
      <option value="21+">21+ pts</option>
    </select>
  </div>
  <svg class="chart" id="trend-chart"></svg>
  <div class="legend" id="trend-legend"></div>
  <p class="note" id="trend-note"></p>
</div>

<!-- Panel 3: Specific deficit deep-dive -->
<div class="panel" id="panel-specific">
  <div class="ctrl-group" style="margin-bottom:18px">
    <label>Exact deficit to examine</label>
    <input type="range" id="exact-def" min="1" max="30" value="13">
    <span class="range-val" id="exact-def-val">13 pts</span>
  </div>
  <div id="specific-wrap"></div>
</div>

<!-- Panel 4: Game Browser -->
<div class="panel" id="panel-browse">
  <div class="search-row">
    <div>
      <label class="small">Season</label>
      <select id="br-season"><option value="">All seasons</option></select>
    </div>
    <div>
      <label class="small">Q3 deficit (trailing team)</label>
      <select id="br-def">
        <option value="">Any</option>
        <option value="1-5">1&ndash;5</option>
        <option value="6-10">6&ndash;10</option>
        <option value="11-15">11&ndash;15</option>
        <option value="16-20">16&ndash;20</option>
        <option value="21+">21+</option>
      </select>
    </div>
    <div>
      <label class="small">Outcome</label>
      <select id="br-outcome">
        <option value="">All</option>
        <option value="comeback">Comeback wins only</option>
        <option value="held">Lead held</option>
      </select>
    </div>
    <button class="btn" id="br-search">Search</button>
  </div>
  <div id="game-table-wrap"></div>
</div>

<script>
// ── Data ──────────────────────────────────────────────────────────────────────
// Each row: [season_start_yr, dq1, dq2, dq3, home_won, hpts, apts]
const RAW = {data_json};
const SEASON_LABELS = {season_labels_json};

const ERA_COLORS = {{
  "1997-2003": "#6baed6",
  "2004-2010": "#2171b5",
  "2011-2017": "#f6a623",
  "2018-2025": "#e84142"
}};
const ERA_ORDER = ["1997-2003","2004-2010","2011-2017","2018-2025"];

function getEra(yr) {{
  if (yr < 2004) return "1997-2003";
  if (yr < 2011) return "2004-2010";
  if (yr < 2018) return "2011-2017";
  return "2018-2025";
}}
function getSeasonLabel(yr) {{
  return SEASON_LABELS[yr] || (yr + "-" + String(yr+1).slice(2));
}}

// Preprocess once
const GAMES = RAW.map(r => ({{
  yr: r[0], season: getSeasonLabel(r[0]), era: getEra(r[0]),
  dq1: r[1], dq2: r[2], dq3: r[3],
  homeWon: r[4], hpts: r[5], apts: r[6],
  total: r[5]+r[6]
}}));

// ── Helpers ───────────────────────────────────────────────────────────────────
function pct(n,d) {{ return d===0 ? null : n/d; }}
function fmtPct(v) {{ return v==null ? "—" : (v*100).toFixed(1)+"%"; }}
function ci95(p,n) {{ return n<5 ? null : 1.96*Math.sqrt(p*(1-p)/n); }}

function getCheckedEras() {{
  return [...document.querySelectorAll(".era-cb:checked")].map(e=>e.value);
}}
function getQtr() {{ return document.getElementById("qtr-sel").value; }}
function getDef(g) {{ return getQtr()==="q1" ? g.dq1 : getQtr()==="q2" ? g.dq2 : g.dq3; }}

const BUCKETS = [
  {{label:"1-3",  lo:1, hi:3}},
  {{label:"4-6",  lo:4, hi:6}},
  {{label:"7-9",  lo:7, hi:9}},
  {{label:"10-12",lo:10,hi:12}},
  {{label:"13-15",lo:13,hi:15}},
  {{label:"16-20",lo:16,hi:20}},
  {{label:"21+",  lo:21,hi:999}},
];
function getBucket(d) {{
  for (const b of BUCKETS) if (d>=b.lo && d<=b.hi) return b.label;
  return null;
}}

// ── Win Rate Table ─────────────────────────────────────────────────────────────
function buildWinRateTable() {{
  const eras = getCheckedEras();
  const qtr  = getQtr();

  // For trailing team (deficit > 0): what's win rate?
  const data = {{}};  // bucket -> era -> {{wins,total}}
  BUCKETS.forEach(b => {{ data[b.label] = {{}}; eras.forEach(e => data[b.label][e] = {{w:0,t:0}}); }});

  for (const g of GAMES) {{
    if (!eras.includes(g.era)) continue;
    const def = getDef(g);
    if (def <= 0) continue;   // not trailing
    const bkt = getBucket(def);
    if (!bkt) continue;
    // trailing team is the AWAY team if def>0 (home is behind)
    // home_won=1 means home team (the trailing team) came back
    data[bkt][g.era].t++;
    if (g.homeWon) data[bkt][g.era].w++;
  }}

  let html = '<table><thead><tr><th>Deficit</th>';
  eras.forEach(e => html += `<th>${{e}}</th>`);
  html += '</tr></thead><tbody>';

  const allRates = [];
  BUCKETS.forEach(b => {{
    eras.forEach(e => {{
      const {{w,t}} = data[b.label][e];
      if (t>0) allRates.push(w/t);
    }});
  }});
  const maxRate = Math.max(...allRates, 0.01);

  BUCKETS.forEach(b => {{
    html += `<tr><td>${{b.label}} pts</td>`;
    eras.forEach(e => {{
      const {{w,t}} = data[b.label][e];
      if (t===0) {{ html += '<td>—</td>'; return; }}
      const r = w/t;
      const ciV = ci95(r,t);
      const intensity = Math.min(5, Math.floor(r/maxRate*5.99));
      const ciStr = ciV ? ` <span class="small">±${{(ciV*100).toFixed(1)}}pp</span>` : "";
      html += `<td class="heat-${{intensity}}">${{fmtPct(r)}}${{ciStr}}<br><span class="small">n=${{t.toLocaleString()}}</span></td>`;
    }});
    html += '</tr>';
  }});

  html += '</tbody></table>';

  // Add pace row
  html += '<br><table><thead><tr><th>Pace / era</th>';
  eras.forEach(e => html += `<th>${{e}}</th>`);
  html += '</tr></thead><tbody><tr><td>Avg total pts/game</td>';
  eras.forEach(e => {{
    const gs = GAMES.filter(g=>g.era===e);
    const avg = gs.reduce((s,g)=>s+g.total,0)/gs.length;
    html += `<td>${{avg.toFixed(1)}}<br><span class="small">n=${{gs.length.toLocaleString()}}</span></td>`;
  }});
  html += '</tr></tbody></table>';
  document.getElementById("winrate-table-wrap").innerHTML = html;
}}

// ── Season Trend Chart ─────────────────────────────────────────────────────────
function buildTrendChart() {{
  const eras   = getCheckedEras();
  const bucket = document.getElementById("bucket-sel").value;
  const bkt    = BUCKETS.find(b=>b.label===bucket);

  // Group by season
  const bySeasonEra = {{}};
  for (const g of GAMES) {{
    if (!eras.includes(g.era)) continue;
    const def = getDef(g);
    if (def < bkt.lo || def > bkt.hi) continue;
    const key = g.yr;
    if (!bySeasonEra[key]) bySeasonEra[key] = {{}};
    if (!bySeasonEra[key][g.era]) bySeasonEra[key][g.era] = {{w:0,t:0}};
    bySeasonEra[key][g.era].t++;
    if (g.homeWon) bySeasonEra[key][g.era].w++;
  }}

  const years = Object.keys(bySeasonEra).map(Number).sort();
  if (!years.length) {{ document.getElementById("trend-chart").innerHTML=""; return; }}

  // Build series per era
  const series = {{}};
  eras.forEach(e => {{
    series[e] = years.map(yr => {{
      const d = bySeasonEra[yr]?.[e];
      if (!d || d.t<5) return null;
      return {{yr, rate: d.w/d.t, n: d.t}};
    }}).filter(Boolean);
  }});

  // SVG chart
  const W=700, H=210, ML=50, MR=20, MT=15, MB=35;
  const cW=W-ML-MR, cH=H-MT-MB;

  const allRates = Object.values(series).flat().map(p=>p.rate).filter(v=>v!=null);
  const minR=0, maxR=Math.max(...allRates, 0.25);
  const minYr=Math.min(...years), maxYr=Math.max(...years);

  function xOf(yr)  {{ return ML + (yr-minYr)/(maxYr-minYr||1)*cW; }}
  function yOf(r)   {{ return MT + cH - (r-minR)/(maxR-minR)*cH; }}

  let svg = `<svg viewBox="0 0 ${{W}} ${{H}}" class="chart" id="trend-chart">`;

  // Grid + y-axis
  const yTicks = [0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30].filter(v=>v<=maxR+0.02);
  yTicks.forEach(v => {{
    const y = yOf(v);
    svg += `<line x1="${{ML}}" x2="${{W-MR}}" y1="${{y}}" y2="${{y}}" class="grid-line"/>`;
    svg += `<text x="${{ML-4}}" y="${{y+4}}" class="axis-label" text-anchor="end">${{(v*100).toFixed(0)}}%</text>`;
  }});

  // x-axis labels
  years.filter((_,i)=>i%3===0).forEach(yr => {{
    const x = xOf(yr);
    svg += `<text x="${{x}}" y="${{H-5}}" class="axis-label" text-anchor="middle">${{getSeasonLabel(yr)}}</text>`;
  }});

  // Lines + dots per era
  eras.forEach(e => {{
    const pts = series[e];
    if (!pts.length) return;
    const col = ERA_COLORS[e];
    const d = pts.map((p,i) => `${{i===0?"M":"L"}}${{xOf(p.yr)}},${{yOf(p.rate)}}`).join(" ");
    svg += `<path d="${{d}}" class="series-line" stroke="${{col}}"/>`;
    pts.forEach(p => {{
      svg += `<circle cx="${{xOf(p.yr)}}" cy="${{yOf(p.rate)}}" r="4" fill="${{col}}" class="dot">
        <title>${{getSeasonLabel(p.yr)}}: ${{(p.rate*100).toFixed(1)}}% (n=${{p.n}})</title></circle>`;
    }});
  }});

  svg += `</svg>`;
  document.getElementById("trend-chart").outerHTML = svg;

  // Legend
  let legend = "";
  eras.forEach(e => {{
    const pts = series[e];
    const avgRate = pts.length ? pts.reduce((s,p)=>s+p.rate,0)/pts.length : 0;
    legend += `<div class="leg-item"><div class="leg-swatch" style="background:${{ERA_COLORS[e]}}"></div>
      ${{e}} &mdash; avg ${{(avgRate*100).toFixed(1)}}%</div>`;
  }});
  document.getElementById("trend-legend").innerHTML = legend;

  // Trend note
  const allPts = years.map(yr => {{
    const all = eras.flatMap(e => bySeasonEra[yr]?.[e] ? [bySeasonEra[yr][e]] : []);
    const w=all.reduce((s,d)=>s+d.w,0), t=all.reduce((s,d)=>s+d.t,0);
    return {{yr, rate: t>0?w/t:null}};
  }}).filter(p=>p.rate!=null);

  if (allPts.length > 4) {{
    const n=allPts.length;
    const sx=allPts.reduce((s,p)=>s+p.yr,0), sy=allPts.reduce((s,p)=>s+p.rate,0);
    const sxx=allPts.reduce((s,p)=>s+p.yr*p.yr,0), sxy=allPts.reduce((s,p)=>s+p.yr*p.rate,0);
    const slope=(n*sxy-sx*sy)/(n*sxx-sx*sx);
    const dir = slope>0.0001?"slightly increasing":slope<-0.0001?"slightly decreasing":"flat";
    document.getElementById("trend-note").textContent =
      `Overall trend for ${{bucket}}-pt deficit: ${{dir}} at ${{(slope*100).toFixed(3)}} pp/year. ` +
      `Year-to-year noise is large — individual seasons vary by ~3-5pp around the average.`;
  }}
}}

// ── Specific Deficit Deep-Dive ─────────────────────────────────────────────────
function buildSpecific() {{
  const eras = getCheckedEras();
  const def  = parseInt(document.getElementById("exact-def").value);
  document.getElementById("exact-def-val").textContent = def + " pts";

  const data = {{}};
  eras.forEach(e => data[e] = {{w:0,t:0,totals:[]}});

  for (const g of GAMES) {{
    if (!eras.includes(g.era)) continue;
    const d = getDef(g);
    if (d !== def) continue;
    data[g.era].t++;
    if (g.homeWon) data[g.era].w++;
    data[g.era].totals.push(g.total);
  }}

  let html = `<div style="display:flex;flex-wrap:wrap;gap:16px;margin-bottom:20px">`;
  eras.forEach(e => {{
    const {{w,t,totals}} = data[e];
    if (t===0) return;
    const r = w/t;
    const ciV = ci95(r,t);
    const avgPts = totals.length ? (totals.reduce((a,b)=>a+b,0)/totals.length).toFixed(1) : "—";
    html += `<div style="background:#f5f9ff;border:1px solid #d0dff0;border-radius:10px;padding:14px 18px;min-width:160px">
      <div style="font-size:11px;font-weight:700;text-transform:uppercase;color:#5b6778;margin-bottom:6px">${{e}}</div>
      <div style="font-size:36px;font-weight:800;color:#154f8b">${{(r*100).toFixed(1)}}%</div>
      <div style="font-size:12px;color:#5b6778">comeback rate</div>
      <div style="font-size:12px;color:#888;margin-top:6px">n=${{t}} &nbsp; ${{ciV ? "±"+(ciV*100).toFixed(1)+"pp" : ""}}</div>
      <div style="font-size:12px;color:#888">avg total: ${{avgPts}} pts</div>
    </div>`;
  }});
  html += `</div>`;

  // Mini bar chart
  html += `<table><thead><tr><th>Era</th><th>Comeback %</th><th>n</th><th>Visual</th></tr></thead><tbody>`;
  const maxR = Math.max(...eras.map(e=>data[e].t>0?data[e].w/data[e].t:0));
  eras.forEach(e => {{
    const {{w,t}} = data[e];
    if (t===0) return;
    const r=w/t;
    const barW = Math.round(r/Math.max(maxR,0.01)*200);
    html += `<tr><td>${{e}}</td><td>${{(r*100).toFixed(1)}}%</td><td>${{t}}</td>
      <td><div style="background:#154f8b;height:12px;width:${{barW}}px;border-radius:3px;display:inline-block"></div></td></tr>`;
  }});
  html += `</tbody></table>`;

  document.getElementById("specific-wrap").innerHTML = html;
}}

// ── Game Browser ───────────────────────────────────────────────────────────────
function buildBrowserSeasons() {{
  const sel = document.getElementById("br-season");
  const yrs = [...new Set(GAMES.map(g=>g.yr))].sort();
  yrs.forEach(yr => {{
    const o = document.createElement("option");
    o.value = yr; o.textContent = getSeasonLabel(yr);
    sel.appendChild(o);
  }});
}}

function runBrowser() {{
  const season  = document.getElementById("br-season").value;
  const defBkt  = document.getElementById("br-def").value;
  const outcome = document.getElementById("br-outcome").value;

  let games = GAMES.filter(g => {{
    if (season && g.yr != season) return false;
    const d = Math.abs(g.dq3);
    if (defBkt==="1-5"  && !(d>=1  && d<=5))  return false;
    if (defBkt==="6-10" && !(d>=6  && d<=10)) return false;
    if (defBkt==="11-15"&& !(d>=11 && d<=15)) return false;
    if (defBkt==="16-20"&& !(d>=16 && d<=20)) return false;
    if (defBkt==="21+"  && !(d>=21))           return false;
    if (defBkt && d===0) return false;
    // outcome: trailing team comeback = trailing team won
    if (outcome==="comeback") {{
      // trailing team is: home if dq3>0, away if dq3<0
      const trailingWon = g.dq3>0 ? g.homeWon===1 : g.homeWon===0;
      if (!trailingWon) return false;
    }}
    if (outcome==="held") {{
      const leadingWon = g.dq3>0 ? g.homeWon===0 : g.homeWon===1;
      if (!leadingWon) return false;
    }}
    return true;
  }});

  // Sort by deficit descending (biggest deficits first)
  games = games.sort((a,b) => Math.abs(b.dq3)-Math.abs(a.dq3));
  const shown = games.slice(0,200);

  let html = `<div style="margin-bottom:8px;color:#5b6778;font-size:12px">${{games.length.toLocaleString()}} games match &mdash; showing first ${{shown.length}}</div>`;
  html += `<div style="overflow-x:auto"><table id="game-table">
    <thead><tr>
      <th>Season</th><th>Home</th><th>Away</th>
      <th>After Q1</th><th>Halftime</th><th>After Q3</th>
      <th>Final</th><th>Result</th>
    </tr></thead><tbody>`;

  shown.forEach(g => {{
    const q1H=Math.round(g.dq1), q2H=Math.round(g.dq2), q3H=Math.round(g.dq3);
    const trailingAtQ3 = q3H>0 ? "HOME" : q3H<0 ? "AWAY" : "TIE";
    const winner = g.homeWon ? "HOME" : "AWAY";
    const comeback = (trailingAtQ3==="HOME"&&winner==="HOME")||(trailingAtQ3==="AWAY"&&winner==="AWAY");
    const cls = comeback ? "win" : trailingAtQ3==="TIE" ? "" : "loss";
    const resultTxt = comeback ? "COMEBACK" : trailingAtQ3==="TIE" ? g.homeWon?"HOME":"AWAY" : "HELD";

    const fmtDef = d => d===0 ? "Tied" : d>0 ? `Home -${{d}}` : `Away -${{Math.abs(d)}}`;
    html += `<tr>
      <td>${{g.season}}</td><td>Home</td><td>Away</td>
      <td>${{fmtDef(q1H)}}</td><td>${{fmtDef(q2H)}}</td><td>${{fmtDef(q3H)}}</td>
      <td>${{g.hpts}}–${{g.apts}}</td>
      <td class="${{cls}}">${{resultTxt}}</td>
    </tr>`;
  }});
  html += `</tbody></table></div>`;
  document.getElementById("game-table-wrap").innerHTML = html;
}}

// ── Tab switching ─────────────────────────────────────────────────────────────
document.querySelectorAll(".tab").forEach(tab => {{
  tab.addEventListener("click", () => {{
    document.querySelectorAll(".tab").forEach(t=>t.classList.remove("active"));
    document.querySelectorAll(".panel").forEach(p=>p.classList.remove("active"));
    tab.classList.add("active");
    document.getElementById("panel-"+tab.dataset.tab).classList.add("active");
    renderActive();
  }});
}});

function renderActive() {{
  const active = document.querySelector(".tab.active").dataset.tab;
  if (active==="winrate")  buildWinRateTable();
  if (active==="trend")    buildTrendChart();
  if (active==="specific") buildSpecific();
}}

// ── Event bindings ────────────────────────────────────────────────────────────
document.getElementById("qtr-sel").addEventListener("change", renderActive);
document.querySelectorAll(".era-cb").forEach(cb => cb.addEventListener("change", renderActive));
document.getElementById("bucket-sel").addEventListener("change", buildTrendChart);
document.getElementById("exact-def").addEventListener("input", buildSpecific);
document.getElementById("br-search").addEventListener("click", runBrowser);

// ── Init ──────────────────────────────────────────────────────────────────────
buildBrowserSeasons();
buildWinRateTable();
</script>
</body>
</html>"""

OUT.write_text(html, encoding="utf-8")
print(f"Written to {OUT}  ({OUT.stat().st_size/1024:.0f} KB)")
