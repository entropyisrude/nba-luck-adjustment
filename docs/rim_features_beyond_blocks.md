# Do Rim-Defense Features Add Signal Beyond Blocks?

## Decision question

The earlier archetype screen established that prior shot blockers tend to reduce
subsequent matched rim access. That is not a novel result. This test asks the
necessary harder question: after using prior-season blocks, do size, contests,
fouls, rim responsibility, or prior rim shooting defense improve identification
of defenders who change half-court rim outcomes?

## Design

- Treatment contrast: adjacent-stint substitution of defender A for defender B.
- Matching: exact opposing offensive five and the same other four defenders.
- Outcomes: season/period-trend-adjusted first-chance recorded rim-attempt rate
  and FG% conditional on a recorded rim attempt.
- Features: immediately preceding season only.
- Training: 2020-21 through 2023-24.
- Held-out test: 2024-25 and 2025-26.
- Mandatory benchmark: blocks per 75 possessions alone.
- Tests: nested held-out prediction, regression among substitutions with similar
  prior block rates, and direct top-15% selector comparisons against blocks.

This is a predictive/quasi-experimental screen. Adjacent substitutions are not
random, so it does not by itself causally identify player value.

## Recorded rim attempts

The held-out sample contains 14,630 substitution contrasts across 2,141 games.
No candidate produced meaningful improvement over blocks:

| Addition to blocks | Held-out RMSE improvement | Game-clustered 95% interval |
|---|---:|---:|
| Prior rim-defended volume | +0.0050% | -0.0034% to +0.0134% |
| Contest-to-foul ratio | -0.0021% | -0.0156% to +0.0124% |
| Prior raw rim FG% | -0.0025% | -0.0106% to +0.0053% |
| Prior rim suppression vs expected | -0.0036% | -0.0162% to +0.0082% |
| Height | -0.0044% | -0.0095% to +0.0002% |
| Contest volume | -0.0083% | -0.0349% to +0.0179% |
| Compact six-feature context | -0.0068% | -0.0400% to +0.0261% |

Positive means better than blocks. The sole positive point estimate is five
thousandths of one percent and its interval includes zero. It is not practically
or statistically useful.

Among the 3,585 held-out events where the defenders were within 0.25 standard
deviations in prior block rate, no added feature had a conventionally significant
incremental slope. Prior rim-defended volume was the closest: one standard
deviation more volume corresponded to 0.79 percentage points fewer rim attempts,
but its interval ranged from 1.83 points fewer to 0.24 points more (p=0.132).

The direct tail-selection comparison also does not rescue the result. Relative to
selecting the top 15% by blocks, size plus blocks selected a group with 0.52 points
fewer recorded attempts, but the paired game-clustered interval was -1.52 to +0.53
points. Contests appeared 0.70 points better than blocks, with an interval of
-1.90 to +0.51. Neither difference is established.

## Conditional rim FG%

The held-out sample is much smaller: 1,949 contrasts across 1,189 games. Prior raw
rim FG% improved RMSE by 0.0669% versus blocks (-0.0578% to +0.1939%), and prior
rim suppression versus expected improved it by 0.0572% (-0.0573% to +0.1727%).
These directions are plausible, but both intervals include no improvement and
the magnitudes are negligible at the event level.

Among comparable blockers, prior raw rim FG% and suppression-versus-expected had
the expected signs but again included zero. Size plus blocks did not outperform
blocks in the direct selector comparison: -0.51 percentage points with a very
wide -5.33 to +4.20 interval.

## Decision

The current feature set has **not** demonstrated useful information beyond blocks.
The earlier multi-field selector findings should therefore be treated as evidence
that the pipeline recognizes conventional rim protectors, not as discovery of a
new rim-protection metric.

This closes the present aggregate-feature branch unless new data or a different
estimand supplies information unavailable to block rate. A worthwhile continuation
would need to measure an offensive behavior change directly—for example, drives
aborted or redirected, rim opportunities passed up, or finishing changes within
specific help/primary-defender contexts—rather than recombine season-level blocks,
contests, fouls, and rim DFG statistics.
