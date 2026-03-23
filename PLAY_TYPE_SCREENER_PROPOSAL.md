# Play Type Screener Proposal

Source:
- `C:\Users\Dave\Downloads\nba-play-types-granular\NBA_Play_Types_12_25.csv`

Coverage:
- `2012-13` onward
- player-season-team rows
- Synergy-style aggregate fields already present:
  - `PLAY_TYPE`
  - `POSS`
  - `FREQ`
  - `PPP`
  - `PTS`
  - `FGM`
  - `FGA`
  - `FG_PCT`
  - `EFG_PCT`
  - `SF_FREQ`
  - `FTA_FREQ`
  - `AND1_FREQ`
  - `TOV_FREQ`
  - `GP`

## Recommended first wave

These are the highest-value play types to add first because they map cleanly to offensive role.

1. `Isolation`
2. `Pick-and-Roll Ball Handler`
3. `Pick-and-Roll Roll Man`
4. `Post-Up`
5. `Spot-Up`
6. `Hand Off`
7. `Cut`
8. `Transition`

## Recommended screener fields

For each selected play type, add:

- `{play_type}_poss`
- `{play_type}_freq`
- `{play_type}_ppp`
- `{play_type}_pts`
- `{play_type}_fga`
- `{play_type}_fg_pct`
- `{play_type}_tov_freq`
- `{play_type}_fta_freq`

That gives both volume and efficiency.

## Best first-screening additions

If only a small number of fields go in first, use these:

- `iso_freq`
- `iso_ppp`
- `pnr_bh_freq`
- `pnr_bh_ppp`
- `pnr_rm_freq`
- `pnr_rm_ppp`
- `spot_up_freq`
- `spot_up_ppp`
- `cut_freq`
- `cut_ppp`
- `handoff_freq`
- `handoff_ppp`

These best separate:
- on-ball creators
- finishers
- spacing wings
- bigs who pressure the rim as rollers/cutters

## Best use in the span screener

These should go into the regular-season span screener first.

Mode behavior:
- `poss` and `pts` should convert with `Totals`, `Per Game`, `Per 36`, and `Per 100`
- `freq`, `ppp`, `fg_pct`, `efg_pct`, `tov_freq`, `fta_freq`, `and1_freq` should stay rate stats

## Derived role metrics worth adding later

These can be built once the base fields are in.

- `Self Creation Load`
  - `iso_poss + pnr_bh_poss + post_up_poss + handoff_poss`

- `Finisher Load`
  - `pnr_rm_poss + cut_poss + putback_poss + spot_up_poss`

- `Creator Efficiency`
  - points or PPP over self-created play types

- `Finisher Efficiency`
  - points or PPP over finisher play types

- `Rim Pressure Proxy`
  - `pnr_bh_fta_freq + iso_fta_freq + transition_fta_freq`

## Best implementation order

1. Build normalized player-season play-type table from the CSV
2. Add first-wave columns to the regular-season span screener
3. Add direct min/max filters for:
   - `iso_freq`
   - `iso_ppp`
   - `pnr_bh_freq`
   - `pnr_bh_ppp`
   - `pnr_rm_freq`
   - `pnr_rm_ppp`
   - `spot_up_freq`
   - `spot_up_ppp`
4. Add remaining play types after the first pass works

## Product value

This is likely the best next screener upgrade because it adds role/context data that is:
- highly useful
- easy to understand
- not already combined with your adjusted plus-minus and on/off filters
