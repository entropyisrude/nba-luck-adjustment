"""
Generate a self-contained local HTML report for the 3PT expectation model.

Computes estimates for every player in the calibration dataset and embeds
the results as JSON in a searchable, sortable HTML table.

Usage:
    python shooter_model/generate_report.py
    # Then open shooter_local.html in a browser

Output: shooter_local.html  (in repo root — gitignored, never pushed)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from shooter_model.model import ThreePTModel, season_weights

DATA_DIR = Path(__file__).parent / "data"
OUTPUT_PATH = Path(__file__).parent.parent / "shooter_local.html"

MIN_3PA = 20
MIN_FTA = 30
HALF_LIFE = 2.0


# ---------------------------------------------------------------------------
# Build player records
# ---------------------------------------------------------------------------

def build_records() -> list[dict]:
    calib_path = DATA_DIR / "calibration.csv"
    if not calib_path.exists():
        raise FileNotFoundError("calibration.csv not found — run fetch_data.py first.")

    df = pd.read_csv(calib_path)
    usable = df[(df["FG3A"] >= MIN_3PA) & (df["FTA"] >= MIN_FTA)].copy()

    model = ThreePTModel.from_params()

    # Group by player
    records = []
    grouped = usable.groupby("PLAYER_ID")
    n = len(grouped)
    print(f"Computing estimates for {n} players...", flush=True)

    for i, (pid, rows) in enumerate(grouped):
        if (i + 1) % 100 == 0:
            print(f"  {i+1}/{n}", flush=True)

        rows = rows.sort_values("SEASON")
        name = rows["PLAYER_NAME"].iloc[-1]
        seasons = rows["SEASON"].tolist()

        # Robust FT% (recency-weighted)
        ft_rows = rows.dropna(subset=["FT_PCT", "FTA"])
        if ft_rows.empty:
            ft_pct = 0.78
        else:
            sw = season_weights(ft_rows["SEASON"].tolist(), half_life=3.0)
            fta = ft_rows["FTA"].values.astype(float)
            ftm = (ft_rows["FT_PCT"].values * fta)
            w = [s * f for s, f in zip(sw, fta)]
            tw = sum(w)
            ft_pct = sum(wi * (mi / ai) for wi, mi, ai in zip(w, ftm, fta) if ai > 0) / tw if tw > 0 else 0.78

        row_dicts = rows.to_dict("records")
        result = model.estimate_from_season_rows(row_dicts, ft_pct, half_life=HALF_LIFE)

        ov = result.overall
        ov_lo, ov_hi = ov.credible_interval(0.90)

        rec = {
            "id": int(pid),
            "name": name,
            "seasons": seasons,
            "n_seasons": len(seasons),
            "latest_season": seasons[-1],
            "ft_pct": round(ft_pct, 4),
            # Overall
            "prior": round(ov.prior_mean, 4),
            "posterior": round(ov.posterior_mean, 4),
            "ci_lo": round(ov_lo, 4),
            "ci_hi": round(ov_hi, 4),
            "w_attempts": round(ov.obs_attempts, 1),
            # C&S
            "cs_posterior": None,
            "cs_ci_lo": None,
            "cs_ci_hi": None,
            "cs_attempts": None,
            # Pull-up
            "pu_posterior": None,
            "pu_ci_lo": None,
            "pu_ci_hi": None,
            "pu_attempts": None,
            # Luck delta (posterior - prior)
            "delta": round(ov.posterior_mean - ov.prior_mean, 4),
        }

        if result.catch_shoot is not None:
            cs = result.catch_shoot
            lo, hi = cs.credible_interval(0.90)
            rec["cs_posterior"] = round(cs.posterior_mean, 4)
            rec["cs_ci_lo"] = round(lo, 4)
            rec["cs_ci_hi"] = round(hi, 4)
            rec["cs_attempts"] = round(cs.obs_attempts, 1)

        if result.pullup is not None:
            pu = result.pullup
            lo, hi = pu.credible_interval(0.90)
            rec["pu_posterior"] = round(pu.posterior_mean, 4)
            rec["pu_ci_lo"] = round(lo, 4)
            rec["pu_ci_hi"] = round(hi, 4)
            rec["pu_attempts"] = round(pu.obs_attempts, 1)

        records.append(rec)

    records.sort(key=lambda r: r["posterior"], reverse=True)
    print(f"Done. {len(records)} records built.")
    return records


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>3PT Expectation Model</title>
<style>
  :root {
    --bg: #0f1117; --card: #1a1d27; --border: #2a2d3a;
    --text: #e0e0e0; --muted: #888; --accent: #4da3ff;
    --green: #4caf7d; --red: #e05a5a; --yellow: #f0c040;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: system-ui, sans-serif; font-size: 14px; padding: 16px; }
  h1 { font-size: 1.3rem; margin-bottom: 4px; }
  .subtitle { color: var(--muted); font-size: 0.85rem; margin-bottom: 16px; }
  .controls { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; margin-bottom: 14px; }
  input[type=text], select {
    background: var(--card); border: 1px solid var(--border); color: var(--text);
    padding: 6px 10px; border-radius: 6px; font-size: 13px;
  }
  input[type=text] { width: 220px; }
  label { color: var(--muted); font-size: 12px; margin-right: 4px; }
  .count { color: var(--muted); font-size: 12px; }
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; white-space: nowrap; }
  thead th {
    background: var(--card); color: var(--muted); font-size: 11px; font-weight: 600;
    padding: 8px 10px; text-align: right; border-bottom: 1px solid var(--border);
    cursor: pointer; user-select: none; position: sticky; top: 0; z-index: 2;
  }
  thead th:first-child, thead th:nth-child(2) { text-align: left; }
  thead th:first-child { position: sticky; left: 0; z-index: 3; background: var(--card); }
  thead th.sorted-asc::after { content: " ▲"; }
  thead th.sorted-desc::after { content: " ▼"; }
  tbody tr:hover { background: rgba(255,255,255,0.03); }
  td {
    padding: 7px 10px; border-bottom: 1px solid var(--border);
    text-align: right; vertical-align: middle;
  }
  td:first-child { text-align: left; position: sticky; left: 0; z-index: 1; background: var(--bg); }
  tbody tr:hover td:first-child { background: #16192a; }
  td:nth-child(2) { text-align: left; color: var(--muted); font-size: 12px; }
  .pct { font-variant-numeric: tabular-nums; }
  .ci { color: var(--muted); font-size: 11px; }
  .delta-pos { color: var(--green); }
  .delta-neg { color: var(--red); }
  .delta-neu { color: var(--muted); }
  .bar-wrap { display: inline-flex; align-items: center; gap: 6px; }
  .bar-bg { width: 80px; height: 8px; background: var(--border); border-radius: 4px; display: inline-block; position: relative; overflow: visible; }
  .bar-fill { height: 100%; border-radius: 4px; background: var(--accent); }
  .bar-prior { position: absolute; top: -2px; width: 2px; height: 12px; background: var(--yellow); border-radius: 1px; }
  .tooltip { position: relative; }
  .tooltip:hover::after {
    content: attr(data-tip); position: absolute; bottom: 120%; left: 50%; transform: translateX(-50%);
    background: #333; color: #fff; padding: 4px 8px; border-radius: 4px; font-size: 11px;
    white-space: nowrap; z-index: 10; pointer-events: none;
  }
  .no-data { color: var(--muted); }
  .hidden { display: none !important; }
</style>
</head>
<body>

<h1>3PT Expectation Model</h1>
<p class="subtitle">
  Bayesian Beta-Binomial model &mdash; prior from FT%, updated with recency-weighted shot history.
  Calibrated on 10 seasons (2015-16 to 2024-25). Half-life = 2 seasons.
</p>

<div class="controls">
  <div>
    <input type="text" id="search" placeholder="Search player..." oninput="applyFilters()">
  </div>
  <div>
    <label>Season:</label>
    <select id="seasonFilter" onchange="applyFilters()">
      <option value="">All seasons</option>
    </select>
  </div>
  <div>
    <label>Min weighted attempts:</label>
    <select id="minAttempts" onchange="applyFilters()">
      <option value="0">Any</option>
      <option value="50">50+</option>
      <option value="100" selected>100+</option>
      <option value="200">200+</option>
      <option value="400">400+</option>
    </select>
  </div>
  <div>
    <label>Sort by:</label>
    <select id="sortBy" onchange="sortTable(this.value)">
      <option value="posterior">Posterior estimate</option>
      <option value="prior">Prior (FT%-based)</option>
      <option value="delta">Delta (posterior - prior)</option>
      <option value="ft_pct">FT%</option>
      <option value="w_attempts">Weighted attempts</option>
      <option value="name">Name</option>
    </select>
  </div>
  <span class="count" id="rowCount"></span>
</div>

<div class="table-wrap">
<table id="mainTable">
<thead>
<tr>
  <th onclick="sortTable('name')" title="Player name">Player</th>
  <th onclick="sortTable('seasons')" title="Seasons in dataset">Seasons</th>
  <th onclick="sortTable('ft_pct')" title="Recency-weighted FT% (prior signal)">FT%</th>
  <th onclick="sortTable('prior')" title="FT%-based prior mean 3PT%">Prior</th>
  <th onclick="sortTable('w_attempts')" title="Recency-weighted attempt count">Wtd Att</th>
  <th onclick="sortTable('posterior')" title="Posterior mean 3PT% (all contexts)">3PT% Est</th>
  <th onclick="sortTable('ci_lo')" title="90% credible interval">90% CI</th>
  <th onclick="sortTable('delta')" title="Posterior minus prior (how much the data moved the estimate)">Delta</th>
  <th onclick="sortTable('cs_posterior')" title="Catch-and-shoot posterior estimate">C&amp;S Est</th>
  <th onclick="sortTable('cs_attempts')" title="Weighted C&S attempts">C&amp;S Att</th>
  <th onclick="sortTable('pu_posterior')" title="Pull-up posterior estimate">PU Est</th>
  <th onclick="sortTable('pu_attempts')" title="Weighted pull-up attempts">PU Att</th>
</tr>
</thead>
<tbody id="tableBody"></tbody>
</table>
</div>

<script>
const RAW = __DATA_JSON__;

let currentSort = { col: "posterior", dir: -1 };
let filtered = [...RAW];

// Populate season filter
const allSeasons = [...new Set(RAW.flatMap(r => r.seasons))].sort().reverse();
const sel = document.getElementById("seasonFilter");
allSeasons.forEach(s => {
  const o = document.createElement("option");
  o.value = s; o.textContent = s;
  sel.appendChild(o);
});

function fmt(v, digits=3) {
  if (v === null || v === undefined) return '<span class="no-data">-</span>';
  return '<span class="pct">' + (v*100).toFixed(1) + '%</span>';
}

function fmtN(v) {
  if (v === null || v === undefined) return '<span class="no-data">-</span>';
  return Math.round(v);
}

function deltaClass(d) {
  if (d > 0.005) return "delta-pos";
  if (d < -0.005) return "delta-neg";
  return "delta-neu";
}

function fmtDelta(d) {
  if (d === null || d === undefined) return '<span class="no-data">-</span>';
  const sign = d >= 0 ? "+" : "";
  const cls = deltaClass(d);
  return `<span class="${cls}">${sign}${(d*100).toFixed(1)}%</span>`;
}

function barCell(posterior, prior, lo, hi) {
  // Scale: 20% to 55% covers the range
  const minP = 0.20, maxP = 0.55, range = maxP - minP;
  const pct = v => Math.max(0, Math.min(100, (v - minP) / range * 100));
  const fillW = pct(posterior);
  const priorX = pct(prior);
  const tip = `Prior: ${(prior*100).toFixed(1)}%  Post: ${(posterior*100).toFixed(1)}%  90% CI: [${(lo*100).toFixed(1)}%, ${(hi*100).toFixed(1)}%]`;
  return `<div class="bar-wrap tooltip" data-tip="${tip}">
    <div class="bar-bg">
      <div class="bar-fill" style="width:${fillW}%"></div>
      <div class="bar-prior" style="left:${priorX}%"></div>
    </div>
    ${fmt(posterior)}
  </div>`;
}

function renderRows() {
  const tbody = document.getElementById("tableBody");
  tbody.innerHTML = filtered.map(r => `
    <tr>
      <td>${r.name}</td>
      <td>${r.latest_season}</td>
      <td>${fmt(r.ft_pct)}</td>
      <td>${fmt(r.prior)}</td>
      <td>${fmtN(r.w_attempts)}</td>
      <td>${barCell(r.posterior, r.prior, r.ci_lo, r.ci_hi)}</td>
      <td class="ci">[${(r.ci_lo*100).toFixed(1)}, ${(r.ci_hi*100).toFixed(1)}]</td>
      <td>${fmtDelta(r.delta)}</td>
      <td>${r.cs_posterior !== null ? barCell(r.cs_posterior, r.prior, r.cs_ci_lo, r.cs_ci_hi) : '<span class="no-data">-</span>'}</td>
      <td>${fmtN(r.cs_attempts)}</td>
      <td>${r.pu_posterior !== null ? barCell(r.pu_posterior, r.prior, r.pu_ci_lo, r.pu_ci_hi) : '<span class="no-data">-</span>'}</td>
      <td>${fmtN(r.pu_attempts)}</td>
    </tr>`).join("");
  document.getElementById("rowCount").textContent = `${filtered.length} players`;
}

function applyFilters() {
  const q = document.getElementById("search").value.toLowerCase().trim();
  const season = document.getElementById("seasonFilter").value;
  const minAtt = parseFloat(document.getElementById("minAttempts").value) || 0;

  filtered = RAW.filter(r => {
    if (q && !r.name.toLowerCase().includes(q)) return false;
    if (season && !r.seasons.includes(season)) return false;
    if (r.w_attempts < minAtt) return false;
    return true;
  });
  sortData();
  renderRows();
}

function sortTable(col) {
  if (currentSort.col === col) {
    currentSort.dir *= -1;
  } else {
    currentSort.col = col;
    currentSort.dir = col === "name" ? 1 : -1;
  }
  document.getElementById("sortBy").value = col;
  sortData();
  renderRows();
  // Update header indicators
  document.querySelectorAll("thead th").forEach(th => {
    th.classList.remove("sorted-asc", "sorted-desc");
  });
}

function sortData() {
  const { col, dir } = currentSort;
  filtered.sort((a, b) => {
    let va = a[col], vb = b[col];
    if (col === "name") return dir * va.localeCompare(vb);
    if (col === "seasons") return dir * (a.n_seasons - b.n_seasons);
    va = va ?? -999; vb = vb ?? -999;
    return dir * (va - vb);
  });
}

// Initial render
applyFilters();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    records = build_records()
    data_json = json.dumps(records, ensure_ascii=False)

    html = HTML_TEMPLATE.replace("__DATA_JSON__", data_json)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"\nWrote {OUTPUT_PATH}")
    print(f"Open this file in your browser to explore the model.")


if __name__ == "__main__":
    main()
