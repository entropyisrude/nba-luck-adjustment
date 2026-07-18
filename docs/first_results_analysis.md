# First Evaluatable Results

## Bottom line

The first chronological holdout supports one claim and rejects a stronger one.

**Supported:** creator-absence burden redistribution is structured enough to predict materially better than zero change, historical receiver averages, or proportional redistribution.

**Not yet supported:** the present player-game box-score panel identifies stable, player-specific creation-load elasticities that improve prediction beyond generic context.

## Evaluation design

- Target: receiving teammate's realized change in composite creation load versus a frozen rolling 10-appearance expectation.
- Training: 2010-11 through 2022-23.
- Model selection only: 2023-24.
- Untouched test: 2024-25 and 2025-26.
- Test sample: 4,231 receiver-event rows across 530 strict first-game creator-absence shocks.
- Regularization selected on validation RMSE.
- Uncertainty resampled by event, keeping teammates from the same shock together.

## Main held-out results

| Model | Receiver RMSE | Event-total RMSE | Event-total MAE | Event-total correlation |
|---|---:|---:|---:|---:|
| Zero change | 6.847 | 27.430 | 21.932 | undefined |
| Global mean | 6.452 | 22.089 | 17.267 | -0.396 |
| Receiver historical mean | 6.624 | 22.618 | 17.771 | -0.216 |
| Proportional redistribution | 6.389 | 20.215 | 15.786 | 0.141 |
| Generic contextual ridge | 6.185 | **16.952** | **13.259** | **0.623** |
| Context + receiver intercept | **6.176** | 17.012 | 13.329 | 0.615 |
| Context + receiver intercept + player shock elasticity | 6.176 | 16.967 | 13.300 | 0.618 |

Relative to proportional redistribution, the full contextual model improves event-total MAE by 2.486 creation-load units (95% event-cluster bootstrap interval 1.968 to 3.028) and RMSE by 3.249 (95% interval 2.629 to 3.877). Both improvements were positive in all 2,000 bootstrap resamples.

## Robustness

The contextual advantage appears in both held-out seasons, back-to-backs and non-back-to-backs, and rank-one and rank-two creator absences. Event-total contextual correlation ranges from 0.582 to 0.660 across these prespecified slices.

The model is under-dispersed at the event level: actual-on-predicted calibration slope is 1.695 with intercept -15.919. It recognizes which events generate more redistribution but compresses the magnitude range too much.

## What the ablation says

The generic contextual model and both player-effect variants are nearly indistinguishable. Generic context is marginally best on event totals. Therefore:

1. expected receiver role, absent-player load, existing receiver burden, schedule, and related context contain real predictive information;
2. regularized receiver identity adds little after those variables;
3. player-by-shock interactions add no meaningful incremental held-out accuracy;
4. current individual elasticity rankings should not be presented as a player metric.

Some estimated player elasticities also contradict their sparse test outcomes. That is expected when interaction coefficients are weakly identified and reinforces the ablation result.

## Interpretation

This is a valuable first result. It demonstrates that the proposed research direction is empirically visible: teams do not redistribute missing creation merely in proportion to prior usage. A contextual response model predicts aggregate burden transfer substantially better.

But the first player-specific formulation has not earned its existence. The likely reasons are:

- box-score burden is too coarse;
- player-event samples are sparse and uneven;
- the same player occupies different roles across teams and seasons;
- missing lineup plans and availability timing confound identity effects;
- a single composite creation-load target hides different response mechanisms;
- fixed player interactions do not share information through player traits.

## Next research move

Do not tune the current player-ID elasticity model harder. Replace free player identity interactions with capability-mediated response functions and separate targets:

- shot-volume absorption;
- passing/assist absorption;
- free-throw and rim-pressure absorption;
- turnover cost;
- minutes absorption;
- team-level retained offensive output.

Use temporally frozen player traits—pull-up frequency, drives, potential assists, catch-and-shoot dependence, finishing, and spacing—to predict those responses. The decisive next question is whether capability vectors add held-out value beyond the generic contextual model.

