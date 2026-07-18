# Residual-composite offensive hybrid review

## Decision

Reject the selected hybrid. Keep denominator-aware atomic as the default.

| Offensive model | Pre-2019 same | Pre-2019 next | 2019-25 same | 2019-25 next |
|---|---:|---:|---:|---:|
| Atomic baseline | 0.6045 | 0.5161 | **0.5580** | **0.4670** |
| Selected compact hybrid | **0.6114** | **0.5228** | 0.5573 | 0.4603 |

The old ratios looked helpful during development but did not transport into
the confirmation era. Residual eFG% (+0.281), residual TS% (-0.230) and MPG
(+0.230) recreated the same hard-to-interpret suppression structure we were
trying to avoid.

## Useful clue

An exploratory group containing events absent from the atomic specification
did better than the overlapping ratios:

| Added residual events | 2019-25 same | 2019-25 next |
|---|---:|---:|
| None | 0.5580 | 0.4670 |
| Fouls drawn only | 0.5594 | 0.4707 |
| Shots blocked only | 0.5589 | 0.4676 |
| Both | 0.5601 | 0.4708 |

This is post-confirmation exploration, not a new confirmed result. The next
candidate should add fouls drawn as a natural possession-denominated atom and
seek genuinely independent confirmation. The ratio/role residual layer should
not be promoted.
