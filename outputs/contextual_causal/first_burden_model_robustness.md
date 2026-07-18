# Robustness Analysis: First Burden Model

## Paired event-cluster bootstrap

Across 530 untouched events, contextual modeling improves event-total MAE over proportional redistribution by **2.486** (95% cluster-bootstrap CI 1.968 to 3.028).

Event-total RMSE improves by **3.249** (95% CI 2.629 to 3.877).

## Subgroups

| Slice | Events | Context MAE | Proportional MAE | Context RMSE | Proportional RMSE | Context corr. |
|---|---:|---:|---:|---:|---:|---:|
| 2024-25 | 275 | 13.423 | 15.884 | 17.321 | 20.253 | 0.605 |
| 2025-26 | 255 | 13.167 | 15.681 | 16.576 | 20.175 | 0.633 |
| non_back_to_back | 366 | 13.280 | 15.397 | 16.661 | 19.574 | 0.599 |
| back_to_back | 164 | 13.343 | 16.654 | 17.629 | 21.578 | 0.660 |
| primary_creator_rank_1 | 296 | 12.707 | 15.488 | 15.865 | 19.279 | 0.638 |
| secondary_creator_rank_2 | 234 | 14.050 | 16.163 | 18.265 | 21.341 | 0.582 |

## Calibration

At the event-total level, actual-on-predicted calibration slope is 1.695 with intercept -15.919. A slope below one indicates predictions are too dispersed; above one indicates insufficient dispersion.

These checks quantify predictive robustness. They do not resolve absence-timing confounding or turn the player coefficients into causal effects.
