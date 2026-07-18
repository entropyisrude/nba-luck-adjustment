# Defensive Rim Pilot

## Primary question

How does a defender's presence change the probability that an established half-court possession produces a first-chance rim attempt, and how does it change efficiency once that attempt occurs?

## Possession definitions

- A possession enters the primary half-court risk set after surviving six seconds.
- Four- and eight-second definitions are retained as sensitivity checks.
- A first-chance rim attempt is an observed field-goal attempt within four feet with no earlier offensive rebound in the possession.
- A second-chance rim attempt occurs after at least one offensive rebound.
- Explicit putback and tip labels are retained within the second-chance family.
- The feed supplies categorical foul location (`area`, `areaDetail`) with nearly complete coverage from 2022-23 onward.
- Restricted Area is the strict near-rim foul definition. Restricted Area plus non-RA paint / 0-8 Center is retained as a broader sensitivity definition.
- Foul location is absent in 2021-22, so location-based foul analysis starts in 2022-23.

## Initial data audit

The modern regular-season table covers 1,238,798 possession sequences and 6,135 games from 2021-22 through 2025-26. It contains approximately:

- 36,000–40,000 first-chance half-court rim attempts per season;
- 12,800–13,200 second-chance rim attempts per season;
- 8,000–8,600 explicitly labeled putback-rim possessions per season;
- 7,000–7,850 first-chance half-court Restricted Area fouls per season from 2022-23 onward;
- 11,600–12,700 under the broader Restricted Area plus non-RA 0-8 paint definition;
- essentially complete categorical foul-location coverage from 2022-23 onward.

## Aggregate diagnostic only

These team-defense correlations were computed as an early diagnostic, but they
do not identify player skill. In particular, next-season franchise persistence
is not an evidentiary test because roster and scheme composition can change
substantially. The values are retained only for reproducibility and should not
guide the player-level conclusions.

| Outcome | Same-season split-half correlation | Next-season correlation | Initial interpretation |
|---|---:|---:|---|
| First-chance half-court rim frequency | 0.816 | 0.655 | Strong, reproducible defensive/system signal |
| First-chance rim FG% | 0.586 | 0.514 | Meaningful but noisier signal |
| Second-chance rim frequency after an OREB | 0.532 | 0.339 | Moderate, partly persistent possession-finishing signal |
| Explicit putback frequency after an OREB | 0.419 | 0.211 | Weak future persistence; keep separate |
| Second-chance rim FG% | 0.192 | 0.220 | Too noisy to lead the model |
| Location-unknown shooting-foul frequency | 0.575 | 0.239 | Same-season structure but weak transportability |
| High-confidence rim-foul rate | 0.385 | 0.113 | Sparse/noisy as a standalone outcome |

The half-court cutoff is not driving the main result. Next-season team correlations for first-chance rim frequency are 0.665, 0.655, and 0.655 at four, six, and eight seconds respectively.

## Putback decision

Do not pool putbacks into the primary rim-protection outcome.

The evidence supports three distinct quantities:

1. first-chance half-court rim suppression;
2. rim efficiency conditional on a first-chance attempt;
3. second-chance/putback exposure after the defense failed to end the possession.

Second-chance rim frequency contains some reproducible signal, but materially less than first-chance rim frequency. It is better interpreted as a bridge between defensive rebounding, box-out execution, scramble defense, and rim protection. It can later contribute to total defensive possession value without being mislabeled as primary rim protection.

## Planned causal design

The first player-level estimate should use matched within-game substitution event studies:

- identify a focal defender entering or leaving;
- compare nearby defensive possessions before and after the substitution;
- require substantial stability in teammates and opposing offensive personnel;
- control or match on game, period, score state, possession start type, and opponent lineup;
- estimate rim-attempt frequency and rim efficiency separately;
- test pre-trends and alternative four/six/eight-second definitions;
- validate estimated effects in held-out games for the same player and stable teammate cores, plus credible absence shocks.

No scalar defensive rating should be produced until these pathway estimates demonstrate stability and useful transportability.

## First player-level causal screens

Lineups were attached to 693,290 half-court possessions from 2022-23 onward.
The audit requires five distinct, nonzero player IDs on both teams; the source's
nominal lineup-complete flag alone was not sufficient.

Three matched designs were tested in disjoint games:

1. same game and exact offensive five, with the defensive lineups sharing four players;
2. same game and quarter, allowing one simultaneous offensive substitution with a separate offensive-player contrast;
3. immediately adjacent stints around an actual substitution, with the same four defensive teammates.

The first two designs did not produce useful held-out prediction. In the local
adjacent-stint design, split-game defender-score correlations were 0.110 for
strict foul-inclusive rim access, 0.107 for broad rim access, 0.083 for recorded
rim attempts, 0.066 for Restricted Area fouls, and 0.018 for rim FG%. These are
weak signals. Every outcome was slightly worse than a zero player-difference
prediction at the individual held-out event level (roughly 0.06%-0.22% worse
RMSE).

Current interpretation: local substitutions contain a small repeatable ordering
signal for whether the offense reaches the rim, but not enough precision for a
standalone player metric. Conditional rim efficiency and foul rate are especially
unreliable in this design. The next stage should test larger absence shocks or a
hierarchical event model; it should not publish player rankings from these results.

## Heterogeneous rim-protector test

The all-player test was too blunt. A targeted analysis classified likely rim
protectors using only prior-season information:

- high-block big: at least 6-8 with 1.5 blocks per 75 and 500 minutes;
- high prior rim role: at least five tracked rim DFGA per game over 40 games.

The adjacent-stint comparison was restricted to an exact offensive five and four
shared defensive teammates. Because rim access falls mechanically after a typical
substitution, candidate events were difference-in-differences adjusted using
non-candidate substitutions in the same season and period.

Relative to the replacing player, prior high-block bigs reduced strict
foul-inclusive rim access by 2.00 percentage points (fixed-control-trend clustered
95% interval: -3.08 to -0.91 points). Recorded rim attempts fell 1.88 points.
Restricted Area foul rate and conditional rim FG% did not change detectably.

The broader candidate union reduced strict rim access by 1.23 points (-2.03 to
-0.46), almost entirely through 1.02 points fewer recorded attempts. Corrected
effects were negative both when candidates entered and when they exited, although
the entry effect remained larger.

The access result was not specific to one cutoff. High-block-big definitions from
1.0 through 2.5 blocks per 75 all produced negative estimates. Prior rim-role
thresholds from 4.5 through 6.5 DFGA per game also produced negative estimates,
with larger deterrence among the highest-exposure group.

This is the first evidence in the pilot that survives a basketball-relevant
heterogeneity test. It supports modeling rim deterrence for a selected archetype,
not forcing one common rim-defense signal across every NBA player.

## Multi-field prior selector screen

A broader screen tested prior-season height, blocks, personal fouls, fouls drawn,
defensive rebounds, contests, box-outs, charges, deflections, rim DFGA volume,
rim DFG%, expected rim DFG%, and rim DFG% difference. Rules selected the top 15%
of players before observing the target season. The discovery block contains the
2022-23 and 2023-24 seasons; the immediately following validation block contains
2024-25 and 2025-26. There is no skipped season. Every target season uses features
from the immediately preceding season.

For strict foul-inclusive rim access in the 2024-25 plus 2025-26 validation block:

- prior contest volume selected a group with 1.99 percentage points less rim access;
- size plus blocks selected 1.75 points less;
- contest-to-foul ratio selected 1.69 points less;
- the block/contest/foul-discipline composite selected 1.42 points less;
- blocks alone selected 1.25 points less, with its interval narrowly including zero;
- prior rim-defended volume selected 1.19 points less.

Thus blocks carry signal, but contest responsibility and size/blocks combinations
are better selectors of deterrence than blocks alone. Personal-foul rate and fouls
drawn did not independently produce a comparably robust result.

For conditional rim FG%, the late validation sample is much smaller (roughly
325-400 games per leading selector), but several prior measures produced coherent
results: block-to-foul ratio selected 7.73 percentage points lower opponent rim
FG%, size plus blocks selected 6.23 points lower, prior rim DFG% difference selected
6.09 points lower, and prior raw rim DFG% selected 5.78 points lower. Raw prior rim
suppression replicated better than the simple 200-attempt shrinkage rule; that does
not prove no shrinkage is needed, only that this particular shrinkage was too blunt.

A smooth all-player ridge model and shallow nonlinear model still failed to add
meaningful prediction. The information is concentrated in the tail and appears to
require a hurdle/archetype model. Because multiple selectors were screened, the
make-percentage findings remain exploratory and need another held-out period.

## Six-season extension

The matched substitution panel was extended backward through 2020-21, producing
925,293 valid-lineup half-court possessions across 6,933 games. Categorical foul
location is unavailable in the two added seasons, so the comparable six-season
outcomes are recorded first-chance rim attempts and FG% conditional on those
attempts. Foul-inclusive rim access remains a 2022-23-forward outcome.

The added seasons support the deterrence result. Players in the prior-season top
15% for blocks had fewer recorded rim attempts than their matched replacement in
all six seasons; the six-season pooled difference was -1.74 percentage points
(game-clustered 95% interval -2.63 to -0.80). Size plus blocks was also negative
in all six seasons and pooled at -2.02 points (-3.01 to -1.20). The block/contest/
foul-discipline composite was negative in all six and pooled at -1.35 points
(-2.21 to -0.48). Contest volume was negative in five of six seasons and pooled
at -1.19 points (-2.13 to -0.21).

The chronology matters. For size plus blocks, the recorded-attempt differences
were -2.42 points in the 2020-21/2021-22 extension, -2.31 in the original
2022-23/2023-24 discovery block, and -1.59 in the 2024-25/2025-26 validation
block. Blocks alone weakened from -2.49 to -2.03 to -1.07 points across those
blocks, while contest volume was strongest in the latest block. This suggests a
real archetype signal but not a permanently calibrated coefficient for blocks.

Conditional rim FG% remains noisier because qualifying substitution comparisons
are sparse. Prior raw rim DFG% and prior rim DFG% versus expected nevertheless
selected lower-FG groups in every season. Their pooled differences were -6.89
points (-9.88 to -3.83) and -7.72 points (-11.05 to -4.52), respectively. Blocks,
size plus blocks, contest volume, and block-to-foul ratio were negative in five of
six seasons and significant when pooled. Rim-defended volume alone was essentially
null, and contest-to-foul ratio did not consistently predict conversion defense.

The concrete conclusion is narrower than a finished player rating: prior evidence
identifies a tail of real rim protectors whose presence changes both whether an
offense gets a recorded rim attempt and, less precisely, how often it converts.
Size, blocks, and contest responsibility help identify deterrence; prior opponent
rim shooting adds information about conversion defense. The repeated signs justify
a hierarchical archetype model, but multiple-selector screening, small FG samples,
and possible tracking-era differences still preclude publishing fixed player
values from this screen.
