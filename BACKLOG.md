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

## Franchise Value

Multi-phase project to value each team's total asset base: draft pick cache + (eventually) player value.

### Phase 1 — Draft pick inventory + display
- Maintain a JSON file (`data/draft_picks.json`) with each team's current pick ownership:
  own picks, picks owed out, swap rights, and protections per pick.
- **Data source challenge**: no clean public API — options are (a) manually curate from Tankathon/RealGM
  and update after trades, or (b) scrape Tankathon. Manual curation is the right first-pass approach.
- Build a `franchise-value.html` page showing each team's pick inventory in a readable table.

### Phase 2 — Lottery odds valuation
- 2019-reform lottery odds are fully specified by the 14-team order.
- Given projected standings, compute each lottery team's probability distribution over all 30 picks.
- For protected picks, compute expected value across the protection window (e.g. top-5 protected:
  sum of probabilities of landing picks 6–30 × value at each slot).
- For swap rights: option value (max of 0, value_their_pick − value_own_pick).

### Phase 3 — Draft value chart
- Map pick number → expected career production using the site's own historical data (e.g. career
  BPM or VORP from draft class outcomes). More meaningful than using someone else's chart since it
  reflects the same player universe.

### Phase 4 — Player value combination
- Restrict to controllable players (under-26 on rookie / rookie-scale deals) to focus on upside.
- Combine draft cache value + young player value → single franchise score per team.
- Possible display: ranked table of all 30 teams, with breakdown of draft assets vs player assets.

### Key decision pending
- How to keep pick inventory current after trades — manual JSON update vs automated scraping.

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
