"""
Inject fresh model data into win_prob_explorer.html.

win_prob_explorer.html is the source of truth for layout and JS.
This script only replaces the `const MODEL = {...};` line so that
re-running wp_fit_model.py + this script updates the data without
overwriting any HTML/JS changes.

Usage:
    python scripts/generate_wp_explorer.py
"""
import json, re
from pathlib import Path

MODEL_JSON = Path(__file__).parent.parent / "data" / "wp_model.json"
HTML_PATH  = Path(__file__).parent.parent / "win_prob_explorer.html"

with open(MODEL_JSON) as f:
    m = json.load(f)

model_json = json.dumps(m, separators=(",",":"))
html       = HTML_PATH.read_text(encoding="utf-8")

marker = "const MODEL = "
start  = html.index(marker)           # raises ValueError if not found
end    = html.index("\n", start)      # MODEL is always on one line
updated = html[:start] + f"{marker}{model_json};" + html[end:]

HTML_PATH.write_text(updated, encoding="utf-8")
print(f"Updated {HTML_PATH.name} with model data ({MODEL_JSON.stat().st_size // 1024} KB JSON)")

### OLD TEMPLATE REMOVED — win_prob_explorer.html is now the source of truth ###
# (kept here as dead code for reference; delete when convenient)
if False:
 html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>NBA Win Probability Explorer</title>
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
.hero p  {{ margin: 0; font-size: 13px; color: rgba(255,255,255,.75); line-height: 1.6; }}
.nav {{ margin-top: 10px; display: flex; gap: 8px; flex-wrap: wrap; }}
.nav a {{ color:#e8f4ff; text-decoration:none; border:1px solid rgba(255,255,255,.3);
           border-radius:7px; padding:5px 10px; font-size:12px; }}
.nav a:hover {{ background:rgba(255,255,255,.15); }}
.controls {{
  background:white; border-radius:12px; border:1px solid #d6e1ef;
  padding:16px 20px; margin-bottom:18px;
  box-shadow:0 2px 8px rgba(23,38,62,.06);
  display:flex; flex-wrap:wrap; gap:24px; align-items:flex-end;
}}
.ctrl-group label {{ display:block; font-size:11px; font-weight:700;
  text-transform:uppercase; color:#5b6778; margin-bottom:6px; letter-spacing:.04em; }}
.ctrl-group select {{ font-size:14px; padding:6px 10px; border:1px solid #c8d8ed;
  border-radius:8px; background:#f7faff; color:#192231; cursor:pointer; }}
input[type=range] {{ width:220px; cursor:pointer; accent-color:#154f8b; }}
.range-val {{ font-size:14px; font-weight:700; color:#154f8b; margin-left:8px; }}
.tabs {{ display:flex; gap:4px; margin-bottom:0; }}
.tab {{
  padding:9px 18px; border-radius:10px 10px 0 0; cursor:pointer;
  font-size:13px; font-weight:600; background:#dde9f7; color:#5b6778;
  border:1px solid #c8d8ed; border-bottom:none; user-select:none;
}}
.tab.active {{ background:white; color:#154f8b; }}
.panel {{
  background:white; border-radius:0 12px 12px 12px; border:1px solid #c8d8ed;
  padding:20px; margin-bottom:18px; display:none;
  box-shadow:0 2px 8px rgba(23,38,62,.06);
}}
.panel.active {{ display:block; }}
svg.chart {{ width:100%; overflow:visible; }}
.grid-line {{ stroke:#edf2f9; stroke-width:1; }}
.axis-label {{ font-size:11px; fill:#888; }}
.axis-title {{ font-size:12px; fill:#555; font-weight:600; }}
.legend {{ display:flex; flex-wrap:wrap; gap:16px; margin-top:12px; font-size:12px; }}
.leg-item {{ display:flex; align-items:center; gap:6px; cursor:pointer; }}
.leg-swatch {{ width:24px; height:4px; border-radius:2px; }}
.stat-cards {{ display:flex; flex-wrap:wrap; gap:14px; margin-bottom:20px; }}
.stat-card {{
  background:#f5f9ff; border:1px solid #d0dff0; border-radius:10px;
  padding:14px 18px; min-width:160px; flex:1;
}}
.stat-card .era-lbl {{ font-size:11px; font-weight:700; text-transform:uppercase; color:#5b6778; margin-bottom:6px; }}
.stat-card .num {{ font-size:34px; font-weight:800; color:#154f8b; line-height:1; }}
.stat-card .sub {{ font-size:12px; color:#888; margin-top:4px; }}
.note {{ font-size:12px; color:#5b6778; margin-top:12px; line-height:1.6; background:#f0f6ff;
         border-radius:8px; padding:10px 14px; border:1px solid #d0dff0; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th {{ background:#f0f4fb; color:#444; padding:8px 10px; text-align:right;
      font-size:11px; text-transform:uppercase; font-weight:700; white-space:nowrap; }}
th:first-child {{ text-align:left; }}
td {{ padding:8px 10px; border-bottom:1px solid #edf2f9; text-align:right; white-space:nowrap; }}
td:first-child {{ text-align:left; font-weight:600; }}
</style>
</head>
<body>

<div class="hero">
  <h1>NBA Win Probability Explorer</h1>
  <p>Gaussian random-walk model fitted to 2M+ observations (1997&ndash;2025).<br>
  P(home wins | diff D, time T) = &Phi;((D + HCA) / (&sigma;<sub>era</sub> &times; &radic;T)) &mdash;
  one parameter per era captures the pace increase.</p>
  <div class="nav">
    <a href="index.html">Overview</a>
    <a href="deficit_explorer.html">Deficit Explorer</a>
    <a href="playoff-series.html">Series Ratings</a>
    <a href="player-similarity.html">Player Similarity</a>
  </div>
</div>

<!-- Controls -->
<div class="controls">
  <div class="ctrl-group">
    <label>Score differential (home perspective)</label>
    <div style="display:flex;align-items:center;gap:6px;margin-top:4px">
      <span style="font-size:12px;color:#888">Away +25</span>
      <input type="range" id="diff-slider" min="-25" max="25" value="-13" step="1">
      <span style="font-size:12px;color:#888">Home +25</span>
      <span class="range-val" id="diff-val"></span>
    </div>
  </div>
  <div class="ctrl-group">
    <label>Time remaining</label>
    <div style="display:flex;align-items:center;gap:6px;margin-top:4px">
      <span style="font-size:12px;color:#888">0</span>
      <input type="range" id="time-slider" min="0" max="48" value="12" step="0.5">
      <span style="font-size:12px;color:#888">48 min</span>
      <span class="range-val" id="time-val"></span>
    </div>
  </div>
  <div class="ctrl-group">
    <label>Era highlight</label>
    <select id="era-sel">
      <option value="all">All eras</option>
      <option value="1997-2003">1997-2003</option>
      <option value="2004-2010">2004-2010</option>
      <option value="2011-2017">2011-2017</option>
      <option value="2018-2025" selected>2018-2025</option>
    </select>
  </div>
</div>

<!-- Tabs -->
<div class="tabs">
  <div class="tab active" data-tab="curve">WP vs Time</div>
  <div class="tab" data-tab="moment">This Moment</div>
  <div class="tab" data-tab="heatmap">Comeback Map</div>
  <div class="tab" data-tab="sigma">Pace Trend</div>
  <div class="tab" data-tab="calib">Calibration</div>
</div>

<!-- Panel 1: WP Curve -->
<div class="panel active" id="panel-curve">
  <div id="curve-cards" class="stat-cards"></div>
  <svg id="curve-svg" class="chart" style="height:280px"></svg>
  <div class="legend" id="curve-legend"></div>
  <p class="note" id="curve-note"></p>
</div>

<!-- Panel 2: This Moment -->
<div class="panel" id="panel-moment">
  <div id="moment-cards" class="stat-cards"></div>
  <svg id="moment-svg" class="chart" style="height:200px"></svg>
  <p class="note" id="moment-note"></p>
</div>

<!-- Panel 3: Heatmap -->
<div class="panel" id="panel-heatmap">
  <div style="margin-bottom:10px;font-size:13px;color:#5b6778">
    Showing era: <strong id="heatmap-era-lbl"></strong> &mdash; P(trailing team wins) at each (deficit, time) combination.
  </div>
  <svg id="heatmap-svg" class="chart" style="height:320px"></svg>
  <div class="legend" id="heatmap-legend" style="margin-top:10px"></div>
</div>

<!-- Panel 4: Sigma Trend -->
<div class="panel" id="panel-sigma">
  <p style="font-size:13px;color:#5b6778;margin-bottom:14px">
    &sigma; (sigma) is the model's scoring-volatility parameter. Higher &sigma; = same deficit is worth less (more scoring expected). It directly captures the NBA's pace increase.
  </p>
  <svg id="sigma-svg" class="chart" style="height:240px"></svg>
  <p class="note">
    Sigma rose from ~0.31 in 1997 to ~0.38 in 2025, a <strong>+22% increase</strong>. This is real and driven by increased pace.
    However even at 2025's sigma, a 13-point deficit with 12 minutes left carries only ~11% comeback probability
    (vs ~7% in 1997-2003). The game is faster, but large leads are not "not daunting."
  </p>
</div>

<!-- Panel 5: Calibration -->
<div class="panel" id="panel-calib">
  <p style="font-size:13px;color:#5b6778;margin-bottom:14px">
    Predicted probability vs actual win rate (binned across all 2M+ observations). Perfect calibration = dots on the diagonal.
  </p>
  <svg id="calib-svg" class="chart" style="height:280px"></svg>
  <p class="note">
    The model is well-calibrated at the extremes. There is a modest 2-4pp positive bias in the 40-65% range &mdash;
    meaning games that look close are actually decided slightly more often than the Gaussian model predicts.
    This is a known characteristic of this model class and does not affect the era-comparison conclusions.
  </p>
</div>

<script>
const MODEL = {model_json};

const ERA_COLORS = {{
  "1997-2003": "#6baed6",
  "2004-2010": "#2171b5",
  "2011-2017": "#f6a623",
  "2018-2025": "#e84142"
}};
const ERA_ORDER = MODEL.era_order;

// ── Lookup helpers ────────────────────────────────────────────────────────────
function wpLookup(era, diff, timeMins) {{
  const surf = MODEL.surfaces[era];
  if (!surf) return 0.5;
  const diffStr = String(Math.round(diff));
  const timeStr = String(Math.round(timeMins * 2) / 2);  // nearest 0.5
  const row = surf[diffStr];
  if (!row) return 0.5;
  return row[timeStr] ?? 0.5;
}}

function getSliderValues() {{
  const diff = parseInt(document.getElementById("diff-slider").value);
  const time = parseFloat(document.getElementById("time-slider").value);
  return {{ diff, time }};
}}

// ── Label formatters ──────────────────────────────────────────────────────────
function diffLabel(d) {{
  if (d === 0) return "Tied";
  const side = d > 0 ? "Home" : "Away";
  return `${{side}} +${{Math.abs(d)}}`;
}}
function timeLabel(t) {{
  const mins = Math.floor(t), secs = Math.round((t - mins) * 60);
  if (secs === 0) return `${{mins}}:00`;
  return `${{mins}}:${{secs.toString().padStart(2,"0")}}`;
}}
function pctLabel(p) {{ return (p * 100).toFixed(1) + "%"; }}

// ── SVG helpers ───────────────────────────────────────────────────────────────
function makeSVG(id, W, H, ML, MR, MT, MB) {{
  const svg = document.getElementById(id);
  svg.setAttribute("viewBox", `0 0 ${{W}} ${{H}}`);
  const cW = W - ML - MR, cH = H - MT - MB;
  return {{ svg, W, H, ML, MR, MT, MB, cW, cH }};
}}
function line(x1,y1,x2,y2,cls,col,width=1) {{
  return `<line x1="${{x1}}" y1="${{y1}}" x2="${{x2}}" y2="${{y2}}" class="${{cls}}" stroke="${{col}}" stroke-width="${{width}}"/>`;
}}
function text(x,y,txt,cls,anchor="middle",col="#888") {{
  return `<text x="${{x}}" y="${{y}}" class="${{cls}}" text-anchor="${{anchor}}" fill="${{col}}">${{txt}}</text>`;
}}

// ── Panel 1: WP Curve ─────────────────────────────────────────────────────────
function renderCurve() {{
  const {{ diff, time }} = getSliderValues();
  const eras = ERA_ORDER;

  // Stat cards
  let cards = "";
  eras.forEach(era => {{
    const p = wpLookup(era, diff, time);
    const homeSide = diff >= 0;
    const trailingP = diff < 0 ? p : 1 - p;
    cards += `<div class="stat-card">
      <div class="era-lbl">${{era}}</div>
      <div class="num">${{pctLabel(p)}}</div>
      <div class="sub">P(home wins)</div>
      ${{diff !== 0 ? `<div class="sub" style="margin-top:4px">Trailing team: ${{pctLabel(Math.min(p, 1-p))}}</div>` : ""}}
    </div>`;
  }});
  document.getElementById("curve-cards").innerHTML = cards;

  // Chart: WP vs time remaining (x = 0..48, y = 0..1)
  const {{ svg, W, H, ML, MR, MT, MB, cW, cH }} = makeSVG("curve-svg", 680, 270, 50, 20, 15, 35);
  const xOf = t => ML + (t / 48) * cW;
  const yOf = p => MT + cH - p * cH;

  let out = "";

  // Grid
  [0, 0.25, 0.5, 0.75, 1.0].forEach(p => {{
    const y = yOf(p);
    out += `<line x1="${{ML}}" x2="${{W - MR}}" y1="${{y}}" y2="${{y}}" class="grid-line"/>`;
    out += text(ML - 4, y + 4, (p*100).toFixed(0)+"%", "axis-label", "end", "#888");
  }});
  // Quarter markers
  [0, 12, 24, 36, 48].forEach(t => {{
    const x = xOf(t);
    out += `<line x1="${{x}}" x2="${{x}}" y1="${{MT}}" y2="${{MT+cH}}" stroke="#e0e8f4" stroke-width="1"/>`;
    const lbl = t === 0 ? "Final" : t === 12 ? "4Q end" : t === 24 ? "3Q end" : t === 36 ? "2Q end" : "Tip-off";
    out += text(x, H - 8, lbl, "axis-label", "middle");
  }});
  out += text(ML - 30, MT + cH/2, "P(home wins)", "axis-title", "middle", "#555") +
         `<g transform="translate(${{ML-30}}, ${{MT+cH/2}}) rotate(-90)">` +
         `<text text-anchor="middle" class="axis-title" fill="#555">P(home wins)</text></g>`;

  // Current time marker
  const tx = xOf(time);
  out += `<line x1="${{tx}}" x2="${{tx}}" y1="${{MT}}" y2="${{MT+cH}}" stroke="#154f8b" stroke-width="1.5" stroke-dasharray="4,3"/>`;
  out += text(tx + 4, MT + 14, timeLabel(time), "axis-label", "start", "#154f8b");

  // 50% reference
  out += `<line x1="${{ML}}" x2="${{W-MR}}" y1="${{yOf(0.5)}}" y2="${{yOf(0.5)}}" stroke="#ccc" stroke-width="1" stroke-dasharray="6,4"/>`;

  // Lines per era
  const hiEra = document.getElementById("era-sel").value;
  eras.forEach(era => {{
    const col  = ERA_COLORS[era];
    const bold = hiEra === "all" || hiEra === era;
    const w    = bold ? 2.5 : 1;
    const op   = bold ? 1 : 0.35;
    const pts  = MODEL.times_min.filter(t => t >= 0);
    const d    = pts.map((t,i) => `${{i===0?"M":"L"}}${{xOf(t).toFixed(1)}},${{yOf(wpLookup(era,diff,t)).toFixed(1)}}`).join(" ");
    out += `<path d="${{d}}" fill="none" stroke="${{col}}" stroke-width="${{w}}" opacity="${{op}}"/>`;

    // Dot at current time
    if (bold) {{
      const p = wpLookup(era, diff, time);
      out += `<circle cx="${{tx}}" cy="${{yOf(p)}}" r="5" fill="${{col}}">
        <title>${{era}}: ${{pctLabel(p)}}</title></circle>`;
    }}
  }});

  svg.innerHTML = out;

  // Legend
  let leg = "";
  eras.forEach(e => {{
    const p = wpLookup(e, diff, time);
    leg += `<div class="leg-item"><div class="leg-swatch" style="background:${{ERA_COLORS[e]}}"></div>
      ${{e}} &mdash; ${{pctLabel(p)}} at cursor</div>`;
  }});
  document.getElementById("curve-legend").innerHTML = leg;

  // Note
  const p97 = wpLookup("1997-2003", diff, time);
  const p25 = wpLookup("2018-2025", diff, time);
  const delta = (p25 - p97) * 100;
  document.getElementById("curve-note").innerHTML =
    `<b>Sigma change effect:</b> At ${{diffLabel(diff)}} with ${{timeLabel(time)}} remaining, the trailing team's win probability ` +
    `is <b>${{pctLabel(Math.min(p97,1-p97))}}</b> in 1997-2003 vs <b>${{pctLabel(Math.min(p25,1-p25))}}</b> in 2018-2025 ` +
    `(difference: ${{(Math.abs(delta)).toFixed(1)}}pp). ` +
    `Home court advantage (HCA) is worth approximately ${{MODEL.mu_global.toFixed(2)}} equivalent points.`;
}}

// ── Panel 2: This Moment ──────────────────────────────────────────────────────
function renderMoment() {{
  const {{ diff, time }} = getSliderValues();

  let cards = "";
  ERA_ORDER.forEach(era => {{
    const p = wpLookup(era, diff, time);
    const ep = MODEL.era_params[era];
    cards += `<div class="stat-card">
      <div class="era-lbl">${{era}}</div>
      <div class="num" style="color:${{ERA_COLORS[era]}}">${{pctLabel(p)}}</div>
      <div class="sub">P(home wins) &mdash; &sigma;=${{ep.sigma.toFixed(4)}}</div>
    </div>`;
  }});
  document.getElementById("moment-cards").innerHTML = cards;

  // Horizontal bar chart of WP by era
  const {{ svg, W, H, ML, MR, MT, MB, cW, cH }} = makeSVG("moment-svg", 680, 180, 100, 20, 20, 30);
  const rowH = cH / ERA_ORDER.length;
  const xOf  = p => ML + p * cW;

  let out = "";
  // Axis
  [0, 0.25, 0.5, 0.75, 1.0].forEach(p => {{
    const x = xOf(p);
    out += `<line x1="${{x}}" x2="${{x}}" y1="${{MT}}" y2="${{MT+cH}}" class="grid-line"/>`;
    out += text(x, MT+cH+14, (p*100).toFixed(0)+"%", "axis-label");
  }});
  out += `<line x1="${{xOf(0.5)}}" x2="${{xOf(0.5)}}" y1="${{MT}}" y2="${{MT+cH}}" stroke="#ccc" stroke-width="1.5" stroke-dasharray="5,3"/>`;

  ERA_ORDER.forEach((era, i) => {{
    const y0 = MT + i * rowH + 4;
    const p  = wpLookup(era, diff, time);
    const bW = Math.abs(p - 0.5) * cW;
    const bX = p >= 0.5 ? xOf(0.5) : xOf(p);
    out += `<rect x="${{bX}}" y="${{y0}}" width="${{bW}}" height="${{rowH-8}}" fill="${{ERA_COLORS[era]}}" opacity="0.85" rx="3"/>`;
    out += text(ML - 6, y0 + rowH/2 - 1, era, "axis-label", "end", "#444");
    out += text(xOf(p) + (p>=0.5?5:-5), y0 + rowH/2 - 1, pctLabel(p), "axis-label", p>=0.5?"start":"end", "#333");
  }});

  svg.innerHTML = out;

  const p97 = wpLookup("1997-2003", diff, time);
  const p25 = wpLookup("2018-2025", diff, time);
  document.getElementById("moment-note").innerHTML =
    `<b>Scenario:</b> ${{diffLabel(diff)}} with ${{timeLabel(time)}} remaining. ` +
    `<b>Trailing team probability:</b> ${{pctLabel(Math.min(p97,1-p97))}} in 1997-2003, ` +
    `${{pctLabel(Math.min(p25,1-p25))}} in 2018-2025. ` +
    `The &sigma; increase of ~17% raises the trailing team's odds by ${{(Math.abs(p25-p97)*100).toFixed(1)}}pp &mdash; ` +
    `real but modest.`;
}}

// ── Panel 3: Heatmap ──────────────────────────────────────────────────────────
function renderHeatmap() {{
  const era = document.getElementById("era-sel").value === "all" ? "2018-2025"
              : document.getElementById("era-sel").value;
  document.getElementById("heatmap-era-lbl").textContent = era;

  const {{ svg, W, H, ML, MR, MT, MB, cW, cH }} = makeSVG("heatmap-svg", 680, 310, 55, 30, 20, 35);

  // Show deficits 1-25 (trailing team perspective) vs time 0-48min
  const DEFS  = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22,25];
  const TIMES = [0.5,2,4,6,8,10,12,15,18,21,24,27,30,33,36,39,42,45,48];

  const cellW = cW / TIMES.length;
  const cellH = cH / DEFS.length;

  function probColor(p) {{
    // 0% = deep red, 50% = white, trailing team P is small here
    // p = trailing team win prob (0 to 0.5 roughly)
    const t = Math.min(p / 0.5, 1);  // 0=no chance, 1=coin flip
    const r = Math.round(220 - t * 140);
    const g = Math.round(80  + t * 140);
    const b = Math.round(80  + t * 80);
    return `rgb(${{r}},${{g}},${{b}})`;
  }}

  let out = "";

  DEFS.forEach((def, di) => {{
    TIMES.forEach((tMin, ti) => {{
      // trailing team = home team is down by `def`
      const p_home_wins = wpLookup(era, -def, tMin);  // home trailing by def
      const p_comeback  = p_home_wins;  // probability trailing team wins

      const x = ML + ti * cellW;
      const y = MT + di * cellH;
      out += `<rect x="${{x}}" y="${{y}}" width="${{cellW}}" height="${{cellH}}"
        fill="${{probColor(p_comeback)}}" stroke="white" stroke-width="0.3">
        <title>-${{def}}pts, ${{timeLabel(tMin)}}: ${{pctLabel(p_comeback)}}</title></rect>`;

      // Label every 5 defs × every 4 times
      if ((di % 4 === 0 || def === 13 || def === 8) && ti % 3 === 1) {{
        out += text(x + cellW/2, y + cellH/2 + 4, (p_comeback*100).toFixed(0)+"%",
                    "axis-label", "middle", p_comeback > 0.25 ? "#fff" : "#333");
      }}
    }});
  }});

  // Axis labels
  TIMES.filter((_,i)=>i%2===0).forEach((t,i) => {{
    const x = ML + (i*2) * cellW + cellW/2;
    out += text(x, MT + cH + 14, timeLabel(t), "axis-label");
  }});
  DEFS.filter((_,i)=>i%2===0).forEach((d,i) => {{
    const y = MT + (i*2) * cellH + cellH;
    out += text(ML - 4, y, `-${{d}}`, "axis-label", "end");
  }});

  // Highlight current state
  const {{ diff, time }} = getSliderValues();
  if (diff < 0) {{
    const di = DEFS.indexOf(Math.min(25, Math.max(1, -diff)));
    const ti = TIMES.findIndex(t => Math.abs(t - time) < 1.5);
    if (di >= 0 && ti >= 0) {{
      const x = ML + ti * cellW, y = MT + di * cellH;
      out += `<rect x="${{x}}" y="${{y}}" width="${{cellW}}" height="${{cellH}}"
        fill="none" stroke="#154f8b" stroke-width="2"/>`;
    }}
  }}

  svg.innerHTML = out;

  // Color legend
  document.getElementById("heatmap-legend").innerHTML =
    `<span style="font-size:12px;color:#5b6778">P(trailing team wins): </span>` +
    [0,5,10,15,20,25,30,40,50].map(v => {{
      const p = v/100;
      return `<span style="background:${{probColor(p)}};padding:3px 8px;border-radius:4px;font-size:11px;color:${{p>0.25?'#fff':'#333'}}">${{v}}%</span>`;
    }}).join(" ");
}}

// ── Panel 4: Sigma Trend ──────────────────────────────────────────────────────
function renderSigma() {{
  const data = MODEL.yearly_sigma;
  const {{ svg, W, H, ML, MR, MT, MB, cW, cH }} = makeSVG("sigma-svg", 680, 230, 55, 20, 15, 35);

  const yrs   = data.map(d => d.season_yr);
  const sigs  = data.map(d => d.sigma);
  const minS  = 0.25, maxS = 0.45;
  const minYr = Math.min(...yrs), maxYr = Math.max(...yrs);

  const xOf = yr => ML + (yr - minYr) / (maxYr - minYr) * cW;
  const yOf = s  => MT + cH - (s - minS) / (maxS - minS) * cH;

  let out = "";

  // Grid
  [0.28,0.30,0.32,0.34,0.36,0.38,0.40].forEach(s => {{
    const y = yOf(s);
    out += `<line x1="${{ML}}" x2="${{W-MR}}" y1="${{y}}" y2="${{y}}" class="grid-line"/>`;
    out += text(ML-4, y+4, s.toFixed(2), "axis-label", "end");
  }});

  // Era shading
  const eras = [
    {{lo:1997,hi:2003,col:"#e8f2ff"}},
    {{lo:2004,hi:2010,col:"#f0f8ff"}},
    {{lo:2011,hi:2017,col:"#fff8ee"}},
    {{lo:2018,hi:2025,col:"#fff0f0"}}
  ];
  eras.forEach(e => {{
    const x1=xOf(e.lo), x2=xOf(e.hi);
    out += `<rect x="${{x1}}" y="${{MT}}" width="${{x2-x1}}" height="${{cH}}" fill="${{e.col}}" opacity="0.7"/>`;
    out += text((x1+x2)/2, MT-4, `${{e.lo}}-${{e.hi}}`, "axis-label");
  }});

  // Line
  const path = data.map((d,i) => `${{i===0?"M":"L"}}${{xOf(d.season_yr).toFixed(1)}},${{yOf(d.sigma).toFixed(1)}}`).join(" ");
  out += `<path d="${{path}}" fill="none" stroke="#154f8b" stroke-width="2.5"/>`;

  // Dots
  data.forEach(d => {{
    const era = d.season_yr < 2004 ? "1997-2003"
              : d.season_yr < 2011 ? "2004-2010"
              : d.season_yr < 2018 ? "2011-2017" : "2018-2025";
    out += `<circle cx="${{xOf(d.season_yr)}}" cy="${{yOf(d.sigma)}}" r="4" fill="${{ERA_COLORS[era]}}">
      <title>${{d.season}}: sigma=${{d.sigma.toFixed(4)}} (n=${{d.n.toLocaleString()}})</title></circle>`;
  }});

  // X-axis
  yrs.filter(y => y % 3 === 0).forEach(yr => {{
    const x = xOf(yr);
    out += text(x, MT+cH+14, `'${{String(yr).slice(2)}}`, "axis-label");
  }});
  out += text(ML-35, MT+cH/2, "sigma", "axis-title", "middle");

  svg.innerHTML = out;
}}

// ── Panel 5: Calibration ─────────────────────────────────────────────────────
function renderCalib() {{
  const data = MODEL.calibration;
  const {{ svg, W, H, ML, MR, MT, MB, cW, cH }} = makeSVG("calib-svg", 680, 270, 50, 20, 15, 35);

  const xOf = p => ML + p * cW;
  const yOf = p => MT + cH - p * cH;

  let out = "";

  // Grid + diagonal
  [0,0.25,0.5,0.75,1.0].forEach(p => {{
    const v = yOf(p);
    out += `<line x1="${{ML}}" x2="${{W-MR}}" y1="${{v}}" y2="${{v}}" class="grid-line"/>`;
    out += text(ML-4, v+4, (p*100).toFixed(0)+"%", "axis-label", "end");
    out += text(xOf(p), MT+cH+14, (p*100).toFixed(0)+"%", "axis-label");
  }});
  out += `<line x1="${{xOf(0)}}" x2="${{xOf(1)}}" y1="${{yOf(0)}}" y2="${{yOf(1)}}" stroke="#aaa" stroke-width="1.5" stroke-dasharray="6,4"/>`;
  out += text(xOf(0.75)+20, yOf(0.8)-8, "Perfect calibration", "axis-label", "start", "#aaa");

  // Points
  data.forEach(d => {{
    const x = xOf(d.pred_mid), y = yOf(d.actual);
    const off = Math.abs(d.actual - d.pred_mid);
    const col = off > 0.03 ? "#e84142" : "#154f8b";
    const r   = Math.min(8, Math.max(4, Math.sqrt(d.n / 5000)));
    out += `<circle cx="${{x}}" cy="${{y}}" r="${{r}}" fill="${{col}}" opacity="0.7">
      <title>Predicted: ${{(d.pred_mid*100).toFixed(1)}}% | Actual: ${{(d.actual*100).toFixed(1)}}% | n=${{d.n.toLocaleString()}}</title></circle>`;
  }});

  out += text(ML+cW/2, MT-4, "Predicted probability", "axis-title");
  out += `<g transform="translate(${{ML-35}},${{MT+cH/2}}) rotate(-90)"><text class="axis-title" text-anchor="middle">Actual win rate</text></g>`;

  svg.innerHTML = out;
}}

// ── Tab switching ─────────────────────────────────────────────────────────────
document.querySelectorAll(".tab").forEach(tab => {{
  tab.addEventListener("click", () => {{
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));
    tab.classList.add("active");
    document.getElementById("panel-" + tab.dataset.tab).classList.add("active");
    renderActive();
  }});
}});

function renderActive() {{
  const tab = document.querySelector(".tab.active").dataset.tab;
  if (tab === "curve")   renderCurve();
  if (tab === "moment")  renderMoment();
  if (tab === "heatmap") renderHeatmap();
  if (tab === "sigma")   renderSigma();
  if (tab === "calib")   renderCalib();
}}

// ── Slider events ─────────────────────────────────────────────────────────────
function updateLabels() {{
  const d = parseInt(document.getElementById("diff-slider").value);
  const t = parseFloat(document.getElementById("time-slider").value);
  document.getElementById("diff-val").textContent = diffLabel(d);
  document.getElementById("time-val").textContent = timeLabel(t);
}}

document.getElementById("diff-slider").addEventListener("input", () => {{ updateLabels(); renderActive(); }});
document.getElementById("time-slider").addEventListener("input", () => {{ updateLabels(); renderActive(); }});
document.getElementById("era-sel").addEventListener("change", () => {{ renderActive(); }});

// ── Init ──────────────────────────────────────────────────────────────────────
updateLabels();
renderCurve();
renderSigma();  // pre-render all static panels
renderCalib();
</script>
</body>
</html>"""

 OUT.write_text(html, encoding="utf-8")
 print(f"Written to {OUT}  ({OUT.stat().st_size/1024:.0f} KB)")
