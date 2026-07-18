# Contextual Defensive RAPM Residual Test

## Research question

Do actual defensive results systematically depart from frozen additive RAPM
expectations under identifiable basketball contexts, and do those conditions
improve future overall defensive prediction rather than merely describe the
sample in which they were found?

This is the correct bridge from component analysis to overall value. Component
statistics receive no value directly; a context term matters only if it improves
held-out prediction of luck-adjusted points allowed.

## Target and timing

- Unit: offense-team game, aggregated from lineup stints.
- Outcome: luck-adjusted points per 100, using the existing FT/midrange-adjusted
  stint outcome, centered on the season scoring environment.
- Expected result: possession-weighted sum of frozen offensive RAPM for the
  offensive lineup minus frozen defensive RAPM for the defensive lineup.
- Ratings: one-step-ahead Kalman states. Established-player ratings use only
  information through the preceding season.
- Traits: immediately preceding season, minimum 300 minutes.
- Primary coverage: at least eight of ten lineup slots have honest frozen states,
  with missing-slot controls. An all-ten sensitivity sample is also reported.
- Claim tier: predictive residual structure, not causal identification.

The primary modern panel contains 12,196 team-game rows across 6,098 games. The
2024-25/2025-26 test contains 4,854 rows across 2,427 games.

## Preregistered basketball contexts

Five offense-versus-defense interactions were tested:

1. opponent rim pressure versus defensive rim protection/size;
2. opponent spacing versus defensive size;
3. offensive rebounding versus defensive rebounding;
4. turnover proneness versus defensive disruption;
5. foul pressure versus defensive foul rate.

Every contextual model was compared with both frozen RAPM alone and frozen RAPM
plus the same ten offensive/defensive traits additively. Thus an interaction could
not win merely because a box trait supplied missing additive player information.

## Results

Adding the ten traits without interactions improved held-out RMSE by only 0.092%
versus frozen RAPM (game-clustered 95% interval -0.184% to +0.368%). The five
interactions jointly improved only 0.027% versus RAPM and were 0.066% worse than
the additive-trait model.

No individual interaction improved on the additive model:

| Context | RMSE change versus additive | 95% interval |
|---|---:|---:|
| Rim pressure × rim anchor | -0.012% | -0.044% to +0.020% |
| Spacing × defensive size | -0.029% | -0.067% to +0.010% |
| Offensive rebounding × defensive rebounding | -0.003% | -0.023% to +0.018% |
| Turnover proneness × disruption | -0.012% | -0.049% to +0.025% |
| Foul pressure × defensive foul rate | -0.011% | -0.054% to +0.030% |

Negative means worse. None is practically useful or statistically distinguishable
from no improvement.

## Frozen-RAPM distribution contexts

A second, separately motivated family asked whether additive RAPM misses the
distribution of talent: opponent star concentration, the defense's weakest link,
its best defender, and defensive rating spread. Specific matchup terms tested
whether concentrated offense performs differently against a weak link or anchor.
The model used 2024-25 for tuning and 2025-26 alone for confirmation.

This family failed clearly. In 2025-26, rating-shape main effects worsened RMSE by
0.275% versus frozen RAPM (-0.403% to -0.151%). Adding all three rating-shape
interactions worsened it by 0.318% (-0.474% to -0.169%). The all-ten-frozen
sensitivity was also worse.

A season-by-season slope scan did not reveal a stable overlooked condition. Foul
pressure × defensive foul rate had the same sign in four of five seasons but
reversed in 2024-25 and failed held-out prediction. Weak-link severity also had
the same sign in four seasons but reversed sharply in the final confirmation
season. These are examples of plausible in-sample stories that should not be
promoted to contextual adjustments.

## Current conclusion

The residual framework is viable and directly tests the desired question, but
the first plausible condition families do **not** improve defensive RAPM. There is
currently no supported contextual correction to impose on overall ratings.

Future candidates should be admitted only as small, predefined families with a
new confirmation period or materially richer information. Broad searches over
box-profile products or free player-pair effects would reproduce the overfitting
already visible in the repository's earlier random-split synergy experiment.

## Nonlinear “styles make fights” test

The linear-product screen may miss threshold effects, so five matchups were also
represented as frozen 3×3 grids: low/middle/high offense style against
low/middle/high defense style. Each joint grid was compared with a model containing
the offense and defense bins separately. Therefore generic nonlinear offense or
defense quality could not masquerade as a matchup effect. The grids were:

1. rim pressure versus rim anchor;
2. spacing versus defensive size;
3. offensive versus defensive rebounding;
4. turnover proneness versus disruption;
5. foul pressure versus foul discipline.

The family was specified after the linear test, used 2024-25 for tuning, and used
2025-26 alone for confirmation. Results in the primary sample were:

| Joint grid versus separate style bins | 2025-26 RMSE improvement | 95% interval |
|---|---:|---:|
| Rim pressure vs rim anchor | +0.010% | -0.017% to +0.037% |
| Spacing vs defensive size | -0.027% | -0.072% to +0.018% |
| Offensive vs defensive rebounding | +0.041% | -0.002% to +0.086% |
| Turnovers vs disruption | -0.033% | -0.115% to +0.049% |
| Foul pressure vs foul discipline | -0.012% | -0.050% to +0.025% |
| All five grids | -0.052% | -0.157% to +0.057% |

Only the rebounding grid approached a positive result. It does not yet qualify:
rolling one-season tests improved 0.044% in 2023-24, worsened 0.032% in 2024-25,
and improved 0.041% in 2025-26. The underlying cells were not stable. For example,
the high-offensive-rebounding/high-defensive-rebounding cell changed from roughly
-0.97 points per 100 versus the marginal model in 2023-24 to -0.05 in 2024-25 and
+0.63 in 2025-26. The fitted interaction also assigned a similar penalty to weak
defensive rebounding against both low and high offensive rebounding, which looks
more like an incompletely modeled marginal weakness than a specific style matchup.

Conclusion: nonlinear relative-style conditioning was a worthwhile test, but it
does not currently supply a confirmed RAPM correction. The rebounding matchup is
the sole watch-list candidate and should be tested unchanged in a future season,
not refined against the existing sample.
