# Initial Estimands

## Event definition

For team `t` and game `g`, focal creator `a` is absent if he is on the pregame eligible roster/rotation, records no participation in `g`, and satisfies a frozen pregame primary-creator threshold based only on prior games.

The first implementation should use two nested cohorts:

- **Broad realized-absence cohort:** identifies absences from roster and participation history.
- **Higher-credibility shock cohort:** additionally requires recent participation, no trade/waiver boundary, short absence duration, and no obvious planned-rest signal. This remains a proxy until timestamped status data exists.

## Estimand 1: burden transfer

For receiving teammate `i` and burden component `k`:

`tau_load(i,k) = E[L_i,g(k; creator absent) - L_i,g(k; creator available) | eligible event context]`

The comparison outcome is estimated from pre-event information and matched/weighted games with comparable opponent, rest, season phase, lineup capacity and prior roles.

## Estimand 2: value retention under added burden

`tau_value(i) = E[Y_i,g(delta L) - Y_i,g(no delta L) | eligible support]`

This is not identified by simply regressing realized efficiency on realized load. Load is endogenous. The pilot should first estimate predictive response curves, then pursue causal anchors using absence-induced shifts and explicit assumptions.

## Estimand 3: team adaptation

`tau_team = E[team offensive outcome under focal absence - team offensive outcome under focal availability | eligible event context]`

Report both the total team effect and decomposition summaries. Do not control for realized teammate burden when estimating the total effect.

## Outcomes

- Burden: FGA, FTA, unassisted makes/attempts where available, drives, pull-ups, potential assists, passes, turnovers, and possession involvement.
- Player effectiveness: points/shot efficiency, turnover-adjusted creation, action-value proxy, and on-court offensive result with strong uncertainty labeling.
- Team effectiveness: adjusted points per possession, shot profile, turnover rate, offensive rebound rate, and lineup-level residual outcome.

## Baselines

1. Prior rolling mean by player.
2. Team redistribution proportional to prior minutes/usage.
3. Box-prior or current metric estimate scaled by projected minutes.
4. Team/opponent/date model without player response traits.
5. Contextual response model using frozen player traits and removed-capacity vector.

## Validation

- Chronological event holdout, preferably leave-season-out plus rolling-origin evaluation.
- Group all rows from one absence event in the same fold.
- Evaluate burden prediction and outcome prediction separately.
- Report calibration, MAE/RMSE, rank correlation only where useful, interval coverage and subgroup performance.
- Audit overlap and effective sample size for matched/weighted estimates.
- Run placebo absence dates, future-absence negative controls and alternative absence definitions.
- Separate conclusions into descriptive, predictive, quasi-causal and causally supported tiers.


