# Canonical-lineup NERD preview and atomic-prior review

Date: 2026-07-18

## Preview definition

This is a non-production current-season preview. It reattaches counted
possessions to the repaired canonical regular-season lineups, retains historical
playoff stints, uses the same six-year/550-day decay and alpha 4000 as the latest
candidate, and adds the quarantined-game supplemental tier. The same likelihood
is solved twice: once around the chronologically fitted v1 box prior and once
around the atomic box prior.

- Games in six-year window: 7,786
- Games passing exact counted-possession gates: 7,238 (92.96%)
- Quarantined/salvage games in the window: 94
- Salvage games passing counted gates: 93
- Remaining salvage games routed to official-minute aggregate fallback: 1
- Production/site files modified: no

## Leading established players

| Player | Team | Current poss. | NERD-O (v1 prior) | NERD-D | NERD | Atomic-prior NERD |
|---|---:|---:|---:|---:|---:|---:|
| Nikola Jokić | DEN | 4,655 | 7.87 | 3.63 | 11.50 | 11.53 |
| Victor Wembanyama | SAS | 3,723 | 2.80 | 7.35 | 10.15 | 9.10 |
| Giannis Antetokounmpo | MIL | 2,130 | 6.49 | 2.64 | 9.13 | 8.72 |
| Shai Gilgeous-Alexander | OKC | 4,581 | 6.67 | 2.32 | 8.99 | 9.48 |
| Kawhi Leonard | LAC | 3,979 | 6.59 | 2.11 | 8.71 | 8.16 |
| Chet Holmgren | OKC | 4,072 | 3.01 | 4.06 | 7.07 | 6.71 |
| Luka Dončić | LAL | 4,613 | 5.86 | 0.64 | 6.50 | 6.51 |
| Jimmy Butler III | GSW | 2,465 | 3.83 | 2.64 | 6.47 | 6.39 |
| Stephen Curry | GSW | 2,812 | 6.96 | -0.69 | 6.27 | 6.04 |
| Donovan Mitchell | CLE | 4,680 | 5.14 | 1.02 | 6.17 | 6.41 |
| Isaiah Hartenstein | OKC | 2,289 | 1.92 | 4.18 | 6.10 | 6.31 |
| Alex Caruso | OKC | 2,148 | 0.89 | 5.16 | 6.05 | 6.10 |
| Derrick White | BOS | 5,059 | 2.26 | 3.62 | 5.88 | 5.85 |
| Neemias Queta | BOS | 3,640 | 2.12 | 3.74 | 5.86 | 5.66 |
| Moussa Diabaté | CHA | 3,759 | 3.15 | 2.16 | 5.31 | 4.92 |
| Franz Wagner | ORL | 2,184 | 3.08 | 2.17 | 5.26 | 5.20 |
| Dyson Daniels | ATL | 5,327 | 2.24 | 2.96 | 5.21 | 5.03 |
| Bam Adebayo | MIA | 4,980 | 1.42 | 3.72 | 5.14 | 4.80 |
| Ausar Thompson | DET | 3,892 | 1.56 | 3.58 | 5.14 | 4.98 |
| Anthony Davis | DAL | 1,296 | 1.46 | 3.66 | 5.13 | 4.44 |

Among 383 players with at least 1,000 current counted possessions, the repaired
v1-prior preview correlates 0.9636 with the previous counted candidate. Mean
absolute change is 0.558 points per 100 and the 95th percentile is 1.411. These
changes combine corrected lineup coverage and the supplemental missing-game
treatment; they should not be attributed to the 337 games alone.

## Atomic coefficient audit

Coefficients below are effects per one training-standard-deviation increase in
the feature, fit chronologically from 2015-16 through 2024-25 for the 2025-26
prior.

### Offense: largest effects

| Atom | Coefficient | Basketball reading |
|---|---:|---|
| Unassisted rim makes/75 | +0.779 | Strong self-created finishing signal |
| Free throws made/75 | +0.740 | Rim pressure and efficient scoring |
| Assisted threes made/75 | +0.627 | Shooting/spacing value |
| Offensive-rebound opportunity rate | +0.584 | Extra-possession creation |
| Assists producing rim makes/75 | +0.439 | High-value creation |
| Floater misses/75 | -0.422 | Inefficient attempts |
| Midrange misses/75 | -0.418 | Inefficient attempts |
| Rim misses/75 | -0.388 | Failed finishing |
| Unassisted threes made/75 | +0.366 | Self-created perimeter scoring |
| Lost-ball turnovers/75 | -0.296 | Direct possession loss |

These signs are largely sensible and stable under leave-one-season-out refits.
Two less satisfying results are that missed threes are almost neutral (-0.010)
and bad-pass turnovers receive only -0.027 after the other creation atoms enter.
The regression is treating those as role/spacing/creation indicators rather
than literal possession values.

### Defense: largest effects

| Atom | Coefficient | Basketball reading |
|---|---:|---|
| Rim FG suppression vs expected | +0.415 | Better tracked rim results |
| Steals/75 | +0.345 | Takeaway creation |
| Contested twos/75 | +0.286 | Defensive activity/role |
| Defensive-rebound opportunity rate | +0.256 | Possession finishing |
| Deflections/75 | +0.197 | Disruption |
| Charges drawn/75 | +0.154 | Possessions ended |
| Blocks/75 | +0.132 | Rim-event disruption |

The basketball-direction signs are mostly reasonable, but defense also places
material weight on age (+0.262), offensive bad-pass turnovers (-0.174), and
assists to threes (+0.169). Those are role/archetype proxies, not clean defensive
skills.

## Main failure: unsmoothed small-sample atoms

The coefficients themselves are not the worst problem. Current-season per-75
inputs are used without adequate sample-size stabilization, so a few events can
be many standard deviations from the training mean:

- Chris Mañon: atomic defensive prior +11.50 and NERD +12.19 on 94 counted possessions;
- Alex Antetokounmpo: atomic NERD +8.46 on 42 possessions;
- Tristen Newton: atomic NERD +8.23 on 24 possessions;
- Mark Sears: atomic NERD +5.63 on 53 possessions.

For individual low-sample players, one atomic feature can contribute 3-7 points
per 100. Examples include deflections (+5.82), free throws made (+7.05),
unassisted rim makes (+5.99), rim suppression (+4.36), and charges (+3.30).
The ridge update cannot correct this because thin RAPM evidence deliberately
stays close to the prior.

## Conclusion

Use the v1-prior column as the interpretable NERD preview. Do not promote the
raw atomic-prior values. The atomic decomposition is conceptually useful and
most major coefficients have plausible directions, but it currently fails two
tests:

1. it underperformed v1 out of sample (same-season total 0.507 vs 0.522;
   next-season 0.416 vs 0.423);
2. its unsmoothed per-75 inputs generate indefensible low-sample priors.

The next atomic iteration should shrink every count/rate atom toward a
historical or position-informed expectation using its actual denominator before
applying the coefficient model. Tracking atoms need their own opportunity
denominators. That shrinkage strength should be selected before 2019 and then
tested unchanged on 2019-25.

## Files

- `outputs/contextual_causal/nerd_canonical_counted_preview.csv`
- `outputs/contextual_causal/nerd_canonical_counted_preview.parquet`
- `outputs/contextual_causal/atomic_prior_current_coefficients_audit.csv`
- `outputs/contextual_causal/atomic_prior_current_contributions.parquet`
- `outputs/contextual_causal/nerd_canonical_counted_game_audit.csv`

