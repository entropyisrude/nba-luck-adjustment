"""
Consolidate current-season O/D splits across NERD, DARKO, EPM, and BPM into
one comparison table, keyed by normalized player name -- the "which metrics
systematically disagree, and where" project (LEBRON/LAKER deprioritized;
LEBRON's free tool is a third-party embed we haven't resolved, LAKER is
paywalled -- see project memory).

Sources (all current-season, not projections):
  NERD  -- metric/metric_v0.parquet, most recent season_year, m4000_o/m4000_d
  DARKO -- nba-metric-data/benchmarks/darko_snapshots/, most recent snapshot
  EPM   -- nba-metric-data/benchmarks/epm_snapshots/, most recent snapshot
  BPM   -- nba-metric-data/benchmarks/bbref_advanced/advanced_{year}.csv,
           OBPM/DBPM (2TM/3TM row kept over per-team rows for traded players)

RAPTOR is deliberately excluded here -- its source died in 2022, so it can't
represent the current season; use it separately for historical-era comparisons.

Output: nba-metric-data/metric_comparison_current.parquet/csv
  (name, nerd_o, nerd_d, darko_o, darko_d, epm_o, epm_d, bpm_o, bpm_d,
   + z-scored versions of each within this table, for apples-to-apples
   disagreement flagging despite each metric's different native scale)

Usage: python metric/build_metric_comparison.py
"""
from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
ROOT = Path(__file__).resolve().parent.parent
OUT = METRIC_DATA / "metric_comparison_current.parquet"

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    text = text.replace(".", "")
    text = re.sub(r"[^a-zA-Z0-9 ]", "", text)
    words = [w.lower() for w in text.split()]
    while words and words[-1] in SUFFIXES:
        words.pop()
    return " ".join(words)


def load_nerd() -> pd.DataFrame:
    m = pd.read_parquet(METRIC_DATA / "metric" / "metric_v0.parquet")
    latest = m["season_year"].max()
    m = m[m["season_year"] == latest].copy()
    m["key"] = m["player_name"].map(normalize_name)
    m = m.rename(columns={"m4000_o": "nerd_o", "m4000_d": "nerd_d"})
    print(f"NERD: season_year={latest}, {len(m)} players")
    return m[["key", "player_name", "nerd_o", "nerd_d"]]


def load_darko() -> pd.DataFrame:
    snaps = sorted((METRIC_DATA / "benchmarks" / "darko_snapshots").glob("darko_*.csv"))
    if not snaps:
        print("WARNING: no DARKO snapshots found")
        return pd.DataFrame(columns=["key", "darko_o", "darko_d"])
    latest = snaps[-1]
    d = pd.read_csv(latest)
    d["key"] = d["player_name"].map(normalize_name)
    d = d.rename(columns={"o_dpm": "darko_o", "d_dpm": "darko_d"})
    print(f"DARKO: {latest.name}, {len(d)} players")
    return d[["key", "darko_o", "darko_d"]]


def load_epm() -> pd.DataFrame:
    snaps = sorted((METRIC_DATA / "benchmarks" / "epm_snapshots").glob("epm_*.csv"))
    if not snaps:
        print("WARNING: no EPM snapshots found")
        return pd.DataFrame(columns=["key", "epm_o", "epm_d"])
    latest = snaps[-1]
    e = pd.read_csv(latest)
    e["key"] = e["name"].map(normalize_name)
    e = e.rename(columns={"off": "epm_o", "def": "epm_d"})
    print(f"EPM: {latest.name}, {len(e)} players")
    return e[["key", "epm_o", "epm_d"]]


def load_bpm() -> pd.DataFrame:
    files = sorted((METRIC_DATA / "benchmarks" / "bbref_advanced").glob("advanced_*.csv"))
    if not files:
        print("WARNING: no BBRef advanced files found")
        return pd.DataFrame(columns=["key", "bpm_o", "bpm_d"])
    latest = files[-1]
    b = pd.read_csv(latest)
    b["key"] = b["Player"].map(normalize_name)
    # traded players have per-team rows plus a 2TM/3TM/4TM total row -- keep the total
    b["is_total"] = b["Team"].astype(str).str.match(r"^\dTM$")
    b = b.sort_values("is_total", ascending=False).drop_duplicates("key", keep="first")
    b = b.rename(columns={"OBPM": "bpm_o", "DBPM": "bpm_d"})
    print(f"BPM: {latest.name}, {len(b)} players")
    return b[["key", "bpm_o", "bpm_d"]]


def zscore(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / s.std()


HTML_PAGE = Path(__file__).resolve().parent.parent / "metric-comparison-local.html"

HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Metric Comparison (Local)</title>
<style>
:root{--blue:#006BB6;--orange:#F58426;--bg:#fff;--bg2:#f6f8fa;--bg3:#eef0f2;
  --border:#d0d7de;--txt:#1a1d23;--txt2:#57606a;--txt3:#8c959f;--green:#1a7f37;--red:#cf222e}
*{box-sizing:border-box}
body{background:var(--bg2);color:var(--txt);font-family:-apple-system,"Segoe UI",Helvetica,sans-serif;
  font-size:13px;margin:0;padding:16px}
h1{font-size:18px;margin:0 0 4px}
.sub{color:var(--txt3);font-size:11px;margin-bottom:14px}
.controls{display:flex;gap:14px;align-items:center;flex-wrap:wrap;margin-bottom:12px}
.controls input[type=text]{padding:6px 10px;border:1px solid var(--border);border-radius:5px;font-size:13px;min-width:200px}
.controls label{font-size:12px;color:var(--txt2);display:flex;align-items:center;gap:5px;cursor:pointer}
.panes{display:flex;gap:16px;align-items:flex-start;flex-wrap:wrap}
.table-wrap{background:var(--bg);border:1px solid var(--border);border-radius:6px;overflow:auto;max-height:82vh;flex:1;min-width:520px}
table{border-collapse:collapse;width:100%;font-size:12px}
th{position:sticky;top:0;background:var(--bg3);border-bottom:1px solid var(--border);padding:6px 8px;
  text-align:right;cursor:pointer;white-space:nowrap;font-weight:700;color:var(--txt2);user-select:none}
th:first-child,td:first-child{text-align:left;position:sticky;left:0;background:inherit}
th.sorted{color:var(--blue)}
th:hover{background:#e2e7ec}
td{padding:5px 8px;text-align:right;border-bottom:1px solid #eef1f4;white-space:nowrap}
tr:hover td{background:#f4f8ff}
.pos{color:var(--green)}.neg{color:var(--red)}
.chart-wrap{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:12px;width:520px}
.chart-wrap h3{margin:0 0 8px;font-size:13px}
svg text{font-size:9px;fill:var(--txt2)}
.dot{fill:var(--blue);fill-opacity:.65;cursor:pointer}
.dot:hover{fill:var(--orange);fill-opacity:1}
.axis-label{font-size:11px;font-weight:700;fill:var(--txt2)}
#tooltip{position:fixed;background:#152d52;color:#fff;padding:4px 8px;border-radius:4px;font-size:11px;
  pointer-events:none;display:none;z-index:50}
.n{color:var(--txt3);font-weight:400}
</style>
</head>
<body>
<h1>Cross-Metric Comparison (NERD vs DARKO vs EPM vs BPM) &mdash; local only</h1>
<div class="sub" id="meta"></div>
<div class="controls">
  <input type="text" id="search" placeholder="Search player...">
  <label><input type="checkbox" id="all4"> Only players in all 4 sources</label>
  <label>Sort by D-spread: <select id="dspread"><option value="">off</option>
    <option value="nerd_epm">NERD vs EPM</option><option value="nerd_darko">NERD vs DARKO</option>
    <option value="nerd_bpm">NERD vs BPM</option></select></label>
</div>
<div class="panes">
  <div class="table-wrap"><table id="tbl"><thead></thead><tbody></tbody></table></div>
  <div class="chart-wrap">
    <h3>Defense: NERD vs EPM <span class="n" id="chart-n"></span></h3>
    <svg id="chart" width="490" height="490" viewBox="0 0 490 490"></svg>
  </div>
</div>
<div id="tooltip"></div>
<script>
const DATA = __DATA_JSON__;
const COLS = [
  {k:'name', label:'Player'},
  {k:'nerd_o', label:'NERD-O'}, {k:'nerd_d', label:'NERD-D'},
  {k:'darko_o', label:'DARKO-O'}, {k:'darko_d', label:'DARKO-D'},
  {k:'epm_o', label:'EPM-O'}, {k:'epm_d', label:'EPM-D'},
  {k:'bpm_o', label:'BPM-O'}, {k:'bpm_d', label:'BPM-D'},
];
document.getElementById('meta').textContent =
  `${DATA.length} players, generated ${window.GENERATED || ''}. Values are z-scores off native scale where noted; raw values shown otherwise.`;

let sortKey = 'nerd_d', sortDir = -1;

function fmt(v){ return v==null ? '&mdash;' : v.toFixed(2); }
function cls(v){ return v==null ? '' : (v>0?'pos':v<0?'neg':''); }

function computeDspread(pair){
  if(!pair) return null;
  const [a,b] = pair.split('_');
  return r => {
    const va = r[a+'_d'], vb = r[b+'_d'];
    return (va==null||vb==null) ? null : Math.abs(va-vb);
  };
}

function stripAccents(s){ return s.normalize('NFKD').replace(/[̀-ͯ]/g,'').toLowerCase(); }

function filtered(){
  const q = stripAccents(document.getElementById('search').value.trim());
  const all4 = document.getElementById('all4').checked;
  let rows = DATA.filter(r => !q || stripAccents(r.name).includes(q));
  if(all4) rows = rows.filter(r => r.nerd_o!=null && r.darko_o!=null && r.epm_o!=null && r.bpm_o!=null);
  return rows;
}

function render(){
  const dspreadPair = document.getElementById('dspread').value;
  const dspreadFn = computeDspread(dspreadPair);
  let rows = filtered();
  if(dspreadFn){
    rows = rows.map(r => ({...r, __spread: dspreadFn(r)})).filter(r => r.__spread!=null);
    rows.sort((a,b) => b.__spread - a.__spread);
  } else {
    rows.sort((a,b) => {
      const av = a[sortKey], bv = b[sortKey];
      if(av==null && bv==null) return 0;
      if(av==null) return 1;
      if(bv==null) return -1;
      if(typeof av === 'string') return sortDir * av.localeCompare(bv);
      return sortDir * (av - bv);
    });
  }

  const cols = dspreadFn ? [...COLS, {k:'__spread', label:'|Gap|'}] : COLS;
  const thead = document.querySelector('#tbl thead');
  thead.innerHTML = '<tr>' + cols.map(c =>
    `<th data-k="${c.k}" class="${c.k===sortKey && !dspreadFn ?'sorted':''}">${c.label}</th>`).join('') + '</tr>';
  thead.querySelectorAll('th').forEach(th => th.addEventListener('click', () => {
    const k = th.dataset.k;
    if(k==='__spread') return;
    if(sortKey===k) sortDir *= -1; else { sortKey = k; sortDir = k==='name' ? 1 : -1; }
    document.getElementById('dspread').value = '';
    render();
  }));

  const tbody = document.querySelector('#tbl tbody');
  tbody.innerHTML = rows.map(r => '<tr>' + cols.map(c => {
    if(c.k==='name') return `<td>${r.name}</td>`;
    const v = r[c.k];
    return `<td class="${cls(v)}">${fmt(v)}</td>`;
  }).join('') + '</tr>').join('');

  drawChart();
}

function drawChart(){
  const pts = DATA.filter(r => r.nerd_d!=null && r.epm_d!=null);
  document.getElementById('chart-n').textContent = `(n=${pts.length})`;
  const svg = document.getElementById('chart');
  const pad = 40, W = 490, H = 490;
  const xs = pts.map(p=>p.nerd_d), ys = pts.map(p=>p.epm_d);
  const lo = Math.min(...xs, ...ys) - 0.5, hi = Math.max(...xs, ...ys) + 0.5;
  const sx = v => pad + (v-lo)/(hi-lo) * (W-2*pad);
  const sy = v => H-pad - (v-lo)/(hi-lo) * (H-2*pad);
  let html = '';
  html += `<line x1="${sx(lo)}" y1="${sy(lo)}" x2="${sx(hi)}" y2="${sy(hi)}" stroke="#ccc" stroke-dasharray="4,3"/>`;
  html += `<line x1="${pad}" y1="${H-pad}" x2="${W-pad}" y2="${H-pad}" stroke="#999"/>`;
  html += `<line x1="${pad}" y1="${pad}" x2="${pad}" y2="${H-pad}" stroke="#999"/>`;
  html += `<text class="axis-label" x="${W/2}" y="${H-8}" text-anchor="middle">NERD-D</text>`;
  html += `<text class="axis-label" x="14" y="${H/2}" text-anchor="middle" transform="rotate(-90 14 ${H/2})">EPM-D</text>`;
  pts.forEach(p => {
    html += `<circle class="dot" cx="${sx(p.nerd_d)}" cy="${sy(p.epm_d)}" r="3.2" data-name="${p.name}" data-x="${p.nerd_d}" data-y="${p.epm_d}"/>`;
  });
  svg.innerHTML = html;
  const tip = document.getElementById('tooltip');
  svg.querySelectorAll('.dot').forEach(d => {
    d.addEventListener('mousemove', e => {
      tip.style.display = 'block';
      tip.style.left = (e.clientX+12)+'px';
      tip.style.top = (e.clientY+12)+'px';
      tip.textContent = `${d.dataset.name}: NERD ${(+d.dataset.x).toFixed(2)}, EPM ${(+d.dataset.y).toFixed(2)}`;
    });
    d.addEventListener('mouseleave', () => tip.style.display='none');
  });
}

document.getElementById('search').addEventListener('input', render);
document.getElementById('all4').addEventListener('change', render);
document.getElementById('dspread').addEventListener('change', render);
render();
</script>
</body>
</html>
"""


def write_html_page(df: pd.DataFrame) -> None:
    cols = ["name", "nerd_o", "nerd_d", "darko_o", "darko_d", "epm_o", "epm_d", "bpm_o", "bpm_d"]
    slim = df[cols].copy()
    for c in cols[1:]:
        slim[c] = slim[c].astype(float).round(3)
    records = slim.to_dict("records")
    # pandas' float columns can't actually hold None -- missing values survive
    # as np.nan even after a .where(notna, None), and json.dumps then emits
    # the bare (JS-legal but non-null) token NaN, which breaks `v==null`
    # checks and chart math client-side. Swap those for real None here instead.
    for r in records:
        for k, v in r.items():
            if isinstance(v, float) and np.isnan(v):
                r[k] = None
    import datetime
    import json as _json
    html = HTML_TEMPLATE.replace("__DATA_JSON__", _json.dumps(records))
    html = html.replace("window.GENERATED || ''",
                        f"'{datetime.date.today().isoformat()}'")
    HTML_PAGE.write_text(html, encoding="utf-8")
    print(f"Wrote {HTML_PAGE} (local only, gitignored)")


def main() -> None:
    nerd = load_nerd()
    darko = load_darko()
    epm = load_epm()
    bpm = load_bpm()

    df = nerd.merge(darko, on="key", how="outer") \
             .merge(epm, on="key", how="outer") \
             .merge(bpm, on="key", how="outer")
    df["name"] = df["player_name"].fillna(df["key"])
    df = df.drop(columns=["player_name", "key"])
    # a handful of NERD rows fall back to a raw player_id string when the name
    # lookup failed upstream (see load_player_names() in build_rapm_target.py)
    # -- drop those from the comparison view rather than show "1787" as a name
    df = df[~df["name"].str.fullmatch(r"\d+")]

    n_all = ((df[["nerd_o", "darko_o", "epm_o", "bpm_o"]].notna()).sum(axis=1) == 4).sum()
    print(f"\n{len(df)} total players matched across at least one source; "
          f"{n_all} present in all four")

    for col in ["nerd_o", "nerd_d", "darko_o", "darko_d", "epm_o", "epm_d", "bpm_o", "bpm_d"]:
        df[f"z_{col}"] = zscore(df[col])

    METRIC_DATA.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False)
    df.to_csv(OUT.with_suffix(".csv"), index=False)
    print(f"Wrote {OUT}")

    print("\nPairwise correlation (Pearson) among current-season O and D ratings:")
    for side in ("o", "d"):
        pairs = [("nerd", "darko"), ("nerd", "epm"), ("nerd", "bpm"),
                 ("darko", "epm"), ("darko", "bpm"), ("epm", "bpm")]
        print(f" {side.upper()}:")
        for a, b in pairs:
            ca, cb = f"{a}_{side}", f"{b}_{side}"
            sub = df[[ca, cb]].dropna()
            r = sub[ca].corr(sub[cb]) if len(sub) > 5 else float("nan")
            print(f"   {a:>5} vs {b:<5}: r={r:.3f}  (n={len(sub)})")

    both = df.dropna(subset=["z_nerd_d", "z_epm_d"]).copy()
    both["gap"] = both["z_nerd_d"] - both["z_epm_d"]
    print("\nBiggest NERD-vs-EPM defensive z-score disagreements (NERD higher):")
    print(both.nlargest(8, "gap")[["name", "nerd_d", "epm_d", "gap"]].to_string(index=False))
    print("\nBiggest NERD-vs-EPM defensive z-score disagreements (EPM higher):")
    print(both.nsmallest(8, "gap")[["name", "nerd_d", "epm_d", "gap"]].to_string(index=False))

    write_html_page(df)


if __name__ == "__main__":
    sys.exit(main())
