# Data Inventory for the Creator-Absence Pilot

## Repository

The active NBA repository is `C:\Users\Dave\Downloads\nba-onoff-publish`. It is a Git repository with a heavily dirty working tree, including many untracked generated datasets and reports. Preserve all existing changes.

## High-value sources

| Source | Grain | Approximate coverage/use | Pilot value |
|---|---|---|---|
| `data/player_boxscore_stats_external_2010_2024.csv` | player-game | Historical regular-season box scores, IDs and conventional production | Main historical player-game panel and realized participation |
| `data/player_boxscore_stats_kaggle_traditional.csv` | player-game | Alternate historical box-score source | Validation and gap filling |
| `data/game_metadata_external_2010_2024.csv` | game | Dates, teams and game metadata | Calendar, opponent, home/away and joins |
| `data/master_boxscore_2526.csv` | player-game | Current-season box scores | Extends the pilot into 2025-26 |
| `data/possessions_historical_pbp.csv` and variants | possession | Large reconstructed historical possession files | Lineups, possession outcomes and on-court context |
| `data/stints_historical.csv` and season stint files | stint | Five-player lineups, duration and score changes | On-court exposure, lineup substitution and contextual outcomes |
| `data/possessions.csv` / `data/possessions_playoffs.csv` | possession | Recent regular season/playoffs | Higher-confidence recent pilot and validation |
| `data/player_game_creation_makes.csv` | player-game/action-derived | Creation-related made-shot attribution | Game-level burden and assisted/unassisted creation features |
| `data/player_hustle_by_season.csv` | player-season | Hustle components | Secondary traits; not a game-level response outcome |
| `data/player_rim_assists_by_season.csv` | player-season | Rim-assist creation | Passing/creation trait prior |
| `data/player_rim_defense_by_season.csv` | player-season | Rim defense | Future defensive shock studies |
| `data/rapm_regular_*.csv` | player-window | Regular-season RAPM estimates | Baseline/prior only; must be temporally frozen |
| root `contextual_tracking_cache/*.csv` | player-season source files | Catch-and-shoot, pull-up, drive and passing tracking, 2021-22 through 2025-26 | Strong role traits, but current aggregate builder pools seasons and must be revised for time-aware use |
| root `contextual_bpm_spacing_dataset_v2_*.csv.gz` | player-possession derived | 2021-22 through 2025-26 | Existing contextual spacing experiment; useful code and diagnostics, not a causal dataset |

## Existing model code worth reusing

- `metric/build_box_prior.py`: box-score feature construction and prior model.
- `metric/build_rapm_target.py`: RAPM target construction.
- `metric/build_game_kalman.py` and `metric/build_kalman_v0.py`: time-evolving player estimates.
- `metric/build_aging_curves.py`: age adjustment.
- `metric/build_fit_layer.py`: capability-based lineup synergy and temporal validation warnings.
- `metric/backtest_margin.py`: held-out game-margin evaluation and explicit lineup-information leakage analysis.
- `generate_possessions_from_pbp.py`: lineup-aware possession reconstruction.
- Root contextual builders under `C:\Users\Dave`: feature prototypes for spacing, drives, passing, finishing and contextual RAPM.

## Existing risks

1. **No canonical pregame availability table.** `data/availability_2026_27.js` and `data/injury_notes.js` are tiny presentation assets, not historical injury feeds.
2. **Unexpectedness is unidentified.** Participation data reveals who did not play, but not when the absence became known.
3. **Roster inference risk.** A player missing from a box score may be injured, rested, traded, waived, assigned elsewhere, or simply out of the rotation.
4. **Temporal leakage.** Several contextual/tracking files aggregate five seasons. They cannot be used as pregame features without rebuilding them by season or rolling cutoff.
5. **Duplicate source families.** Many historical possession and player-state variants exist. The pilot needs a single canonical source plus quality flags.
6. **Identifier consistency.** NBA player/team/game IDs are generally available, but name-based fallbacks and historical sources require auditing.
7. **Reconstruction quality.** Inferred starters, incomplete lineups and manual patches must be exposed as quality fields and sensitivity filters.
8. **Post-treatment controls.** Actual minutes, realized lineups, teammate usage and in-game role are outcomes/mediators for pregame prediction, not admissible pre-treatment controls.

## Minimum viable dataset

One row per teammate-player × target game × focal-absence event:

- game ID, date, season, team, opponent and home/away;
- focal absent player ID and pregame creator score;
- receiver player ID and prior roster/rotation membership;
- prior rolling burden vector for focal and receiving players;
- actual receiving-player burden vector and change from frozen expectation;
- actual minutes, lineup exposure and team offensive outcome;
- schedule controls: rest, travel proxy, back-to-back, opponent and season phase;
- pregame baseline player/team estimates;
- event eligibility and data-quality flags;
- matched or weighted comparison-game identifier/weight;
- feature cutoff timestamp/date.

## Data still needed

For a genuinely “unexpected absence” design, acquire a timestamped availability source: official injury reports, archived sportsbook/lineup news, or another pregame status feed. Until then, label the first study as a realized-absence response analysis and use conservative eligibility proxies.

