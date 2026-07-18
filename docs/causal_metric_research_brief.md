# Contextual Causal NBA Player Valuation

## Research premise

Conventional all-in-one NBA metrics mainly estimate a player's average observed contribution in the contexts in which he played. Even sophisticated adjusted plus-minus models do not automatically answer an intervention question such as: what would change if this player replaced another player, assumed a different role, joined a different lineup, or absorbed responsibility after a teammate became unavailable?

The primitive object for this project is therefore a contextual response function:

`V_i(C) = expected contribution of player i under context C and a declared intervention.`

A scalar rating is a summary of that function over an explicit context distribution, not the starting point.

## Value concepts to keep separate

- **Realized value:** contribution in the player's observed contexts.
- **Standardized value:** expected contribution over a common league context distribution.
- **Portable value:** value retained across plausible changes in role, teammates, opponents, and scheme.
- **Team-specific value:** realized contribution including complementarity with a particular roster and system.
- **Shock-response value:** ability to absorb or redirect responsibility when team capacity changes.

These quantities can disagree without any of them being erroneous.

## Proposed causal hierarchy

1. A lineup and game state create a demand vector: initiation, rim pressure, passing, shooting, finishing, screening, rebounding, and defensive tasks.
2. Player capabilities and coaching choices determine how that demand is allocated.
3. Player actions change possession state and teammate opportunities.
4. Those transitions affect possession and game outcomes.
5. Repeated local responses reveal player-specific response surfaces and structured complementarities.

The desired model links player/action to state transition to expected outcome, while separating prediction from causal credit assignment.

## Relationship to existing work

- Causality-inspired APM addresses lineup-selection confounding but remains primarily an additive player-coefficient model.
- Expected Possession Value and action-valuation models supply a mechanistic state/value layer but are not automatically causal attribution models.
- Tracking models estimate context-sensitive components such as drives, passing, shooting, spacing, contests, and rim protection.
- Lineup and hypergraph models recognize synergy but face severe sparsity when interactions are free parameters.
- The useful synthesis is to estimate interactions through capabilities: `gamma_ij = g(theta_i, theta_j)`.

## First pilot: primary-creator absence shocks

Study games in which a high-creation player who was recently active becomes unavailable. Measure:

1. which teammates absorb specific types of offensive burden;
2. how much each type of burden changes;
3. how individual and team effectiveness respond;
4. whether pre-event traits predict those responses;
5. whether contextual predictions outperform conventional scalar ratings on held-out shocks.

The initial player traits are:

- **creation-load elasticity:** how much responsibility flows to a player when teammate creation capacity is removed;
- **value retention under added load:** how effectiveness changes as that responsibility rises.

Together these distinguish scalable creators, emergency volume absorbers, efficient specialists, and players whose team benefit operates through redistribution rather than personal volume.

## Burden vector

Do not reduce burden to usage alone. Begin with components supported by the data:

- shooting attempts and free-throw pressure;
- unassisted and pull-up attempts;
- drives and drive-derived passes/assists;
- assists, potential assists, and passes;
- turnovers;
- assisted finishing and catch-and-shoot volume;
- time on ball or touches if obtainable at game level;
- lineup and on-court offensive responsibility.

## Validation standard

All estimates used for an event must be frozen using information available before that game. Evaluate held-out absence events chronologically. Compare against simple baselines: recent player averages, minutes-scaled box priors, RAPM/metric priors, and team-only forecasts. The contextual model earns complexity only through improved calibration and prediction of burden transfer and outcomes.

## Important limitations

- Absence is not random; opponent, schedule, injury severity, rest, and coaching plans can confound comparisons.
- A zero-minute game is not necessarily an unexpected absence.
- Teammate outcomes are mediators of the team effect and must not be casually included as pre-treatment controls.
- Lineup reconstruction and historical box-score sources have varying quality.
- Season-aggregated tracking traits cannot by themselves measure within-player game-level response.
- Strong causal language requires overlap and sensitivity evidence; otherwise results should be labeled descriptive or quasi-experimental.

