# Production NERD atomic promotion validation

Date: 2026-07-18

## Decision implemented

The denominator-aware atomic box prior now feeds the production joint NERD
solve, the Kalman state model, and the site export. The rejected residual
composites and the exploratory foul feature are not included.

The prior uses rolling, past-only fits for 2004-25 and explicitly labeled
within-era leave-one-season-out backfills for 1996-2003. The previous v1
metric, Kalman output, and site export are preserved under
`production_v1_rollback_20260718/`.

## Integrity gates

- Production metric: 15,090 player-seasons, no duplicate player-season keys,
  no non-finite offense, defense, or total values, and every row labeled
  `prior_model=atomic_denominator`.
- Kalman states: 14,356 player-seasons, no duplicate keys, no non-finite
  prediction/filter values, and every row labeled
  `prior_model=atomic_denominator`.
- Site export: 15,760 rows, nine-column schema, and top-level
  `model=atomic_denominator` provenance.
- Local browser test: `nerd.html` loaded the atomic site export with no console
  warnings or errors and displayed the expected leaderboard values.
- Python compilation and `git diff --check` passed. Existing line-ending
  conversion warnings are unrelated to this promotion.

## Behavior versus v1

For 2025-26 players with at least 1,000 possessions in both models, atomic and
v1 totals correlate at 0.974 with mean absolute movement of 0.392 points per
100. Thus the promotion preserves the broad established-player ordering.

The low-sample v1 failures are removed. Among 72 players below 200 production
possessions, the new maximum NERD is +2.55 and the 95th percentile is +2.02.
Examples: Tristen Newton moved from +8.31 to +1.05, N'Faly Dante from +6.39 to
+0.88, and Norchad Omier from +5.93 to +1.05.

The displayed 2025-26 top five are Nikola Jokic +10.4, Victor Wembanyama +8.9,
Shai Gilgeous-Alexander +8.3, Giannis Antetokounmpo +7.3, and Stephen Curry
+6.9.

## Remaining evidence-universe distinction

This promotion changes the prior throughout the current production pipeline;
it does not replace that pipeline's prepared stint evidence with the newer
canonical counted-possession evidence. Among current players with at least
1,000 possessions in both universes, the two atomic outputs correlate at 0.930
with mean absolute difference 0.693 points per 100. That is a separate future
production migration decision, not a failure of the atomic prior promotion.

