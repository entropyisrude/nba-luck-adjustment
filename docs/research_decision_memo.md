# Research Decision Memo

## Decision

**Continue the contextual burden-redistribution project. Do not yet publish or optimize a player-specific elasticity metric.**

The project has made substantial empirical progress: contextual models predict team-level redistribution after creator absences materially better than simple alternatives on two untouched seasons. That result is large, stable across meaningful subgroups, and supported by event-cluster uncertainty intervals.

The player-metric branch has not crossed the same threshold. Prior-season capability vectors produce small, repeatable improvements in identifying which teammate receives additional burden, but the magnitude is modest and does not improve aggregate team redistribution. Free player identity and player-specific elasticity coefficients add effectively no held-out value.

## Evidence supporting continuation

1. On 530 untouched events, contextual event-total RMSE was 16.967 versus 20.215 for proportional redistribution.
2. Event-total MAE improved by 2.486 with a 95% event-cluster bootstrap interval of 1.968 to 3.028.
3. The advantage appeared in both test seasons, both rest groups, and both creator-rank groups.
4. Event-total correlation increased from 0.141 to 0.618.
5. Prior-season capabilities improved individual receiver RMSE by 0.046 on identical modern-era rows; the cluster-bootstrap interval was 0.003 to 0.089.
6. Component models show small individual allocation gains for minutes, FGA, 3PA, points, and composite creation load.

## Evidence against a current player metric

1. Generic contextual features match or slightly beat free player-ID effects at the event level.
2. Player-specific shock interactions do not add meaningful held-out accuracy.
3. Capability improvements are small in practical magnitude.
4. Capability models usually worsen event-total accuracy when fit independently by player row.
5. A two-stage total-then-allocation model also underperformed the simpler generic contextual model.
6. Individual elasticity rankings show weak and sometimes contradictory out-of-sample support.
7. Season-level tracking traits are too coarse to represent current role, team changes, and within-season development.
8. Absence announcement timing and projected lineups remain unavailable.

## What is now established

The empirically supported object is currently a **contextual team adaptation model**, not an all-in-one player rating. It can answer:

> Given the missing creator and the remaining roster's frozen roles, how much additional offensive burden is likely to appear, and approximately where will it go?

It cannot yet answer with adequate reliability:

> Which player has an enduring, portable ability to absorb creation burden?

## Conditions for continuing the player-metric branch

Continue only if at least one of the following becomes available:

- rolling or game-level tracking traits rather than prior-season totals;
- touches and time-of-possession data;
- possession-level action types and lineup-specific opportunity measures;
- timestamped pregame availability and projected lineups;
- a capability-mediated hierarchical model that improves receiver RMSE materially and improves or preserves event totals;
- stable player/capability rankings across seasons and alternative shock definitions.

An appropriate quantitative hurdle is at least a 2% receiver-RMSE improvement over generic context, with a positive event-cluster interval, no degradation in event-total RMSE, and positive replication in both held-out seasons. The current capability improvement is approximately 0.7%, below that hurdle.

## Recommended next work

1. Treat the current contextual model as a research prototype and inspect its largest held-out successes and failures.
2. Build event-level calibration and prediction intervals.
3. Add better pregame availability data if obtainable.
4. Rebuild tracking traits as rolling, temporally frozen measures.
5. Revisit player valuation only after the capability branch clears the stated hurdle.

