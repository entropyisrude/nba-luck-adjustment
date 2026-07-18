# Probabilistic salvage of the 337 non-canonical games

Date: 2026-07-18

## Decision

Do not delete the 337 games, and do not promote them into the canonical lineup
file. Use a separate supplemental RAPM tier:

- 289 games have at least one complete, score-consistent candidate timeline and
  enter through 20 coherent whole-game imputations;
- 48 games have no qualifying scoring timeline and enter only as official
  minute-weighted, game-level offense/defense observations;
- zero games are discarded.

The canonical 35,185-game candidate remains unchanged.

## Candidate evidence

The versioned repair history supplied 719 distinct whole-game timelines for the
337 games. There are multiple candidates for 253 games, including at least three
for 115 games. Eighty-four games have only one observed candidate; 51 of those
pass the stint-level score/coverage gate and 33 use the aggregate fallback.
Single-candidate games are explicitly flagged and must not be described as if
their between-candidate uncertainty were identified.

A measurement model was trained on rejected alternative timelines from games
whose final canonical lineup is known. The chronological validation block holds
out seasons after 2017 and contains 180 games and 267 alternatives. Candidate
quality is measured as duration-weighted agreement across the ten on-court
player slots.

The learned ranker did not beat the transparent evidence rule:

| Candidate rule | Mean canonical lineup-slot agreement |
|---|---:|
| Learned best | 97.09% |
| Transparent evidence best | 97.17% |
| Evidence-weighted alternatives | 97.10% |
| Best available alternative in hindsight | 97.34% |

Accordingly, actual imputation probabilities use the transparent score based on
score, minutes, plus-minus, unsupported changes, recorded transitions and player
action presence. Machine learning is retained only as a diagnostic.

## Masked downstream RAPM validation

The 180 validation games were removed from the canonical file and then restored
three different ways. The target is the diagnostic alpha-500, six-year-decayed
RAPM obtained with their true canonical lineups.

| Treatment of masked games | Exposed-player RAPM MAE | 95th percentile absolute error |
|---|---:|---:|
| Delete the games | 0.0353 | 0.1417 |
| Official-minute game-level aggregate | 0.0321 | 0.1308 |
| Evidence-weighted lineup imputation | **0.0196** | **0.0765** |

Thus aggregate retention is modestly better than deletion, while coherent
lineup imputation recovers substantially more of the player-level signal.

The imputation spread alone is not a calibrated standard error. In the masked
test its mean between-imputation SD is only 0.00026, while the realized RAPM
RMSE is 0.0406. Candidate support omits possible timelines, especially for the
84 single-candidate games. Production uncertainty must therefore add an
empirical reconstruction-error component from masked validation rather than use
Rubin between-imputation variance by itself.

## Current-data sensitivity

Adding the supplemental tier to a diagnostic 2025-26 alpha-500 RAPM fit gives:

- 699 players touched within the six-year window;
- mean absolute change of 0.166 points per 100 among touched players;
- mean absolute change of 0.159 among 511 established players with at least
  1,000 canonical weighted possessions;
- 95th-percentile established-player change of 0.438;
- canonical-versus-completed correlation of 0.9956;
- 95th-percentile between-imputation SD of 0.0276 for established players.

These changes are large enough that non-random whole-game deletion was not
innocuous. They are still sensitivity results, not proof that every individual
change is correct.

## Artifacts

- `derived/contextual_causal/probabilistic_lineup_salvage/candidate_stint_bank.parquet`
- `derived/contextual_causal/probabilistic_lineup_salvage/rapm_imputed_stints_20.parquet`
- `derived/contextual_causal/probabilistic_lineup_salvage/rapm_aggregate_fallback_design.parquet`
- `derived/contextual_causal/probabilistic_lineup_salvage/rapm_candidate_probabilities.csv`
- `outputs/contextual_causal/probabilistic_salvage_masked_rapm_summary.json`
- `outputs/contextual_causal/probabilistic_salvage_rapm_sensitivity_summary.json`

No production stint, possession, RAPM, NERD, database or site artifact was
modified.
