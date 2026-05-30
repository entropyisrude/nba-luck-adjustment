# NBA Analytics Site — Feature Backlog

Items are roughly ordered by impact vs effort within each section.

---

## Eoin Data Opportunities

The Kaggle "historical-nba-data-and-player-box-scores" dataset (PlayerStatistics.csv) provides
complete game-by-game box scores from ~1947 through present for both RS and playoffs, with no
PBP gaps. Currently only wired into playoff span chunks. Opportunities:

### High priority

- **Pre-1996 RS extension** — Extend game-search.html and player-span-search.html back to ~1950
  using Eoin as the RS box-score source. Show basic stats only (pts/reb/ast/stl/blk/fg2/fg3/ft)
  for the historical tier; no on/off, no hustle. The playoff pipeline already proves the pattern.

- **RS data quality parity** — The RS pipeline still depends on PBP joins for pre-2007 coverage;
  some early seasons likely have the same gap problem that was fixed for playoffs. Switch RS
  box-score source to Eoin to close that gap.

### Medium priority

- **All-time single-game records page** — Dedicated search: "all 50-point games since 1950",
  "all 20-20 nights", "all 5×5 games", etc. Right now the game search covers 1996-97+; with Eoin
  it becomes all-time.

- **Consecutive-game streaks** — Since Eoin has every row in chronological order, detect "N
  consecutive games above threshold X" (scoring streaks, consecutive double-doubles, cold runs).
  Not derivable from aggregated data alone.

- **Career arc viewer** — A player page showing game-by-game stats across their entire career
  (e.g. Kareem 1969–1989). Currently impossible for anyone who played before 1996.

### Lower priority / longer term

- Era-adjusted stats (normalize across different pace/scoring eras)
- Head-to-head matchup history (Player A vs Team B over career)
- All-time debut/final game tracking

---

## Navigation / UX

- The Playoffs section in the index.html nav is now empty (no links left after unifying game
  search and span search). Either remove the section or add something meaningful to it
  (e.g. link to onoff-playoffs.html, rapm-playoffs.html).

---

## Data / Pipeline

- Verify RS game chunks pre-2007 for missing-game gaps (same root cause as the playoff fix).
- Consider adding 1995-96 RS season to RS_DATA_FILES once Eoin RS pipeline is built (currently
  RS starts 1996-97; PO starts 1995-96).

---

## Done (recent)

- Unified all span search pages into player-span-search.html with Mode selector (RS / Playoffs /
  Merged / Dual Filter). Dual Filter shows compact side-by-side panels; "Expand all fields"
  switches to stacked full layout.
- Unified all game search pages into game-search.html with Mode selector (RS / Playoffs / Merged).
- Retired combined-span-search.html, player-span-search-playoffs.html,
  player-span-search-combined.html, game-search-playoffs.html — all redirect to unified pages.
- Fixed daily CI crash (git LFS pull for adjusted_onoff CSVs before report generation).
- Switched playoff span chunks to Eoin data source, fixing historical coverage gaps
  (e.g. Robinson 1998-99: 4 → 17 games).
