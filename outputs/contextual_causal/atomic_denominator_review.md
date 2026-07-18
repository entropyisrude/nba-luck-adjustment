# Denominator-aware atomic prior review

## Outcome

The revision fixes the tiny-sample explosions and produces a real untouched
validation gain, concentrated on defense and next-season carry-forward. It is
good enough to continue as the atomic research model, but it does not yet beat
v1 on every objective.

| Model | 2019-25 same-season total r | 2019-25 next-season total r |
|---|---:|---:|
| v1 | 0.5219 | 0.4233 |
| raw atomic | 0.5073 | 0.4162 |
| denominator-aware atomic | 0.5113 | **0.4290** |

For defense alone, denominator-aware atomic reaches 0.5087 in-season and
0.4477 one season ahead, versus 0.5027 and 0.4310 for raw atomic.

## What changed in basketball terms

A rate based on a handful of chances no longer counts as if it were earned over
a full season. A player's steal/block/scoring atoms are stabilized according to
possessions; rebounding according to rebound chances; hustle according to
tracked possessions; and rim field-goal suppression according to defended rim
attempts. With more evidence, the observed rate receives more authority.

The historical data chose modest shrinkage: 0.10 of a typical season for
offense and 0.25 for defense. Those values were selected through 2018 only and
then frozen for the 2019-25 report.

## Low-sample stress test

For current players below 200 feature possessions:

| Diagnostic | Raw atomic | Denominator-aware |
|---|---:|---:|
| Maximum absolute total prior | 13.01 | 2.76 |
| 95th percentile absolute total prior | 8.51 | 2.32 |
| Maximum absolute single-atom contribution | several points | 1.01 |

Chris Mañon moves from a +13.01 raw prior to +2.54; Alex Antetokounmpo from
+8.93 to +1.88; and Tristen Newton from +8.62 to +1.39.

## Current counted NERD leaders

The same counted-possession RAPM evidence centered on the new prior begins:

1. Nikola Jokić — 11.52
2. Shai Gilgeous-Alexander — 9.58
3. Victor Wembanyama — 9.19
4. Giannis Antetokounmpo — 8.58
5. Kawhi Leonard — 8.22
6. Chet Holmgren — 6.80
7. Luka Dončić — 6.54
8. Donovan Mitchell — 6.46
9. Jimmy Butler III — 6.40
10. Isaiah Hartenstein — 6.20

These are versioned research previews, not production/site values.
