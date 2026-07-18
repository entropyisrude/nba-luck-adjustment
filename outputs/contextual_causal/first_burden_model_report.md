# First Burden-Response Model Results

## Design

The target is receiving-player change in composite creation load relative to a frozen 10-appearance baseline. Training ends in 2022-23, model selection uses 2023-24, and the untouched test set is 2024-25 through 2025-26.

## Held-out test performance

| Model | Receiver MAE | Receiver RMSE | Event-total MAE | Event-total RMSE | Correlation |
|---|---:|---:|---:|---:|---:|
| zero_change | 5.148 | 6.847 | 21.932 | 27.430 | nan |
| global_mean | 4.971 | 6.452 | 17.267 | 22.089 | nan |
| receiver_historical_mean | 5.086 | 6.624 | 17.771 | 22.618 | 0.076 |
| proportional_redistribution | 4.888 | 6.389 | 15.786 | 20.215 | 0.147 |
| contextual_partial_pooling | 4.787 | 6.176 | 13.300 | 16.967 | 0.295 |
| generic_context_ridge | 4.797 | 6.185 | 13.259 | 16.952 | 0.290 |
| player_intercept_ridge | 4.787 | 6.176 | 13.329 | 17.012 | 0.295 |

## Highest estimated shock elasticities

Minimum 50 fitting rows; these are regularized predictive response estimates, not causal player rankings.

| Player | Fit rows | Elasticity | Test mean delta |
|---|---:|---:|---:|
| Jeff Green | 65 | 0.0907 | -4.754 |
| Tobias Harris | 63 | 0.0681 | 2.297 |
| Austin Rivers | 50 | 0.0633 | nan |
| Serge Ibaka | 52 | 0.0592 | nan |
| James Johnson | 54 | 0.0485 | nan |
| Anthony Davis | 50 | 0.0369 | 2.899 |
| Kelly Olynyk | 54 | 0.0338 | 1.189 |
| Bobby Portis | 74 | 0.0336 | 6.548 |
| Josh Richardson | 53 | 0.0335 | nan |
| Brook Lopez | 59 | 0.0091 | 0.800 |
| Jrue Holiday | 59 | 0.0089 | 3.405 |
| Rudy Gay | 56 | 0.0064 | nan |
| Danny Green | 95 | -0.0035 | nan |
| Jonas Valanciunas | 62 | -0.0053 | 2.289 |
| Justin Holiday | 70 | -0.0096 | nan |

## Interpretation boundary

A useful result requires the contextual model to beat simple baselines on the untouched seasons and to remain credible at the event-total level. Player elasticities are partial-pooling predictive summaries. Absence announcement timing, lineup plans, and injury context remain unobserved, so causal interpretation is premature.
