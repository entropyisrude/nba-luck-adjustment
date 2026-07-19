# Multivariate Kalman production promotion

Date: 2026-07-19

## Decision

Promote the covariance-aware filter. It uses the same atomic box prior and
canonical counted-possession evidence as the previous production model, but
updates all active players jointly from stint-level likelihoods and preserves
the player covariance created by shared lineups. This is a more faithful model
of what the lineup regression actually identifies than treating every player's
season RAPM as an independent observation whose reliability is determined only
by possessions.

Locked parameters, selected on development seasons through 2018:

- state evolution variance `q = 1`
- stint likelihood scale `c = 20000`
- box-prior observation variance `8`

Castle and the 2025 reversal were diagnostic cases, not tuning targets.

## Validation

| Test | Independent production | Multivariate | Result |
|---|---:|---:|---|
| Next-season evidence wcorr, development | 0.5230 | 0.5240 | Slight improvement |
| Next-season evidence wcorr, 2019+ | 0.4970 | 0.4992 | Slight improvement |
| Game margin MAE, 2006-2017 | 9.65 | 9.55 | Improvement |
| Game margin correlation, 2006-2017 | 0.395 | 0.415 | Improvement |
| Game margin MAE, 2019-2025 | 10.838 | 10.784 | Improvement |
| Game margin correlation, 2019-2025 | 0.421 | 0.429 | Improvement |

The modern paired season-block MAE improvement is 0.054 points, with a 95%
interval of approximately 0.014 to 0.086. The model improves in 2019 and
2021-2024, is essentially tied in 2020, and is worse in 2025. That last season
remains a useful monitoring flag, but is not enough to override the structural
advantage and the broader out-of-sample results.

Uncertainty calibration against next-season evidence also favors the joint
model. Standardized residual SD should be near 1.0:

| Split | Independent | Multivariate |
|---|---:|---:|
| Development | 0.716 | 0.997 |
| 2019+ confirmation | 0.637 | 0.895 |

The multivariate confirmation intervals cover 74.2% at one nominal standard
deviation and 96.7% at two, slightly conservative but much closer to nominal
than the independent model's 87.7% and 99.8%.

## Production artifacts and checks

- Builder: `metric/build_kalman_multivariate.py`
- Production state: `nba-metric-data/kalman/kalman_states.parquet` and CSV
- Site payload: `data/nerd_seasons.js`
- Rollback: `outputs/contextual_causal/production_independent_kalman_rollback_20260719/`

The production state has 14,005 rows, no duplicate player-season keys, only
finite numeric state values, nonnegative total posterior variance, and an exact
hash match to the clean promotion candidate. The generated site payload has
15,228 unique rows. All 2026-27 projection values and standard deviations are
finite. Historical O-NERD, D-NERD and NERD values match the rollback payload
exactly; historical missing uncertainty values are pre-existing rows without a
corresponding state and do not occur in the projection season.

The NERD and team-projection pages both loaded from a local server without
console warnings or errors. The rendered Castle projection is -0.8 after
one-decimal display rounding. San Antonio renders at +7.2 net rating and 60
wins under the current depth chart.

## Interpretation

The switch does not assert that Castle is good because he shares the floor with
Wembanyama. It says the data are less certain about how to split their shared
lineup results than the old possession-only reliability formula admitted. The
box priors therefore retain more influence over that allocation, while the
team's jointly identified contribution remains constrained by the stint data.
This is the intended behavior for low-diversity lineup histories.
