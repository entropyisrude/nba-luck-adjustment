# Production NERD counted-possession migration

Date: 2026-07-18

## Outcome

Production NERD, its single-season evidence, the Kalman state model, and the
site export now use `canonical_counted_possessions_v1`. The production solver
no longer loads `prepared_stints.parquet` and no longer estimates stint
possessions as `seconds / 24`.

The denominator-aware atomic prior remains the prior. This migration changes
the on-court likelihood/evidence beneath it.

## Evidence universe and resolution

- 35,522 regular-season games: 35,185 canonical reconstructions, 289 frozen
  deterministic salvage reconstructions, and 48 no-lineup aggregate games.
- 2,431 repaired playoff games, loaded directly from `stints_playoffs.csv`.
  Their dates are restored from the official `Games.csv` metadata rather than
  inherited from the old prepared-stint cache.
- 37,740 of 37,953 total games retain counted possessions at full stint/lineup
  resolution.
- 165 games with incomplete possession-to-stint reconciliation retain exact
  game possession counts with lineup-minute aggregate design.
- The 48 games without a defensible lineup reconstruction retain exact game
  possession counts with official-minute aggregate design.
- No intended game is omitted and no aggregate fallback uses `seconds / 24`
  as its possession denominator.

For 8,915 predominantly pre-2019 games, the event state machine's home/away
count gap exceeded the frozen four-possession tolerance. Their observed
possession locations are retained, while side totals are calibrated to their
common game mean. This is explicit in `balance_calibrated` in the audit.

Small historical scoring-feed discrepancies of at most five points are
reconciled to the canonical adjusted score. After reconciliation, maximum
absolute game-total error is numerical zero (2.84e-14 points). Larger or
incomplete cases use aggregate evidence.

## Model validation

- Posterior alpha remains 4000, selected only on evidence through 2018.
- Next-season total correlation: 0.508 development and 0.492 on untouched
  2019+ evidence.
- On matched rows, NERD scores 0.504 versus BPM's 0.444 and 0.499 versus
  RAPTOR's 0.449.
- Kalman hyperparameters selected pre-2019 are q=1.0, c=50,000 and prior
  variance 8.0. Its primary predictive correlation is 0.523 development and
  0.497 on 2019+ confirmation.

The final production metric has 14,564 unique player-seasons; the Kalman file
has 14,005. Both contain finite offense, defense, and total values and explicit
prior/evidence provenance.

## Current leaderboard and stability

The 2025-26 top five are Nikola Jokic +11.57, Shai Gilgeous-Alexander +9.80,
Victor Wembanyama +9.66, Giannis Antetokounmpo +8.44, and Kawhi Leonard +8.23.

For current players with at least 1,000 possessions in both versions, the
counted result correlates 0.965 with the provisional atomic/old-evidence build;
mean absolute movement is 0.507 points per 100. Against the earlier canonical
counted preview, correlation is 0.974 and mean absolute movement is 0.407.

The fringe-player fix remains intact: among 76 current players below 200
possessions, maximum NERD is +2.60 and the 95th percentile is +2.18.

## Rollback

- Original v1 production artifacts:
  `production_v1_rollback_20260718/`
- Atomic-prior artifacts using the old evidence:
  `production_atomic_old_evidence_rollback_20260718/`

