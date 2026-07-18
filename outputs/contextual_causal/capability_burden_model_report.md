# Capability-Response Model Analysis

Prior-season tracking capabilities modestly improve individual receiver allocation, but do not improve event-total redistribution.

| Comparison | Improvement over generic | 95% event-cluster interval | Probability positive |
|---|---:|---:|---:|
| Receiver MAE | 0.0440 | 0.0058 to 0.0826 | 0.987 |
| Receiver RMSE | 0.0460 | 0.0030 to 0.0886 | 0.981 |
| Event-total MAE | -0.1301 | -0.4343 to 0.1743 | 0.201 |
| Event-total RMSE | -0.2124 | -0.5060 to 0.0874 | 0.082 |

A positive value favors the capability model. The individual-level gain is evidence that basketball traits contain allocation information beyond generic role variables, but its practical size is small. Negative event-total results show that unconstrained row-level fitting does not conserve or correctly calibrate total redistributed burden.

The next model should separate burden components and use an event-level allocation structure: predict total redistributed burden first, then predict teammate shares with capability features. This directly respects the team-total versus teammate-allocation distinction revealed here.
