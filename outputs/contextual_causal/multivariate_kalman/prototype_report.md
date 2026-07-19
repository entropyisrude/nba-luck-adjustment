# Multivariate lineup-covariance Kalman prototype

Date: 2026-07-19

## Question

Does retaining the lineup regression's player-to-player covariance improve the
season state model, especially for young players whose minutes are heavily
tethered to unusual teammates?

This is a predictive realized-value experiment. It does not identify causal,
portable or context-free player value.

## Method

The production filter at the time of this experiment converted each season to an independently updated RAPM
observation with variance `c / possessions`. The prototype instead applies the
canonical directed-stint likelihood jointly:

`posterior precision = predicted precision + box precision + X'WX / c`

`posterior rhs = predicted precision * predicted mean + box precision * box mean + X'Wy / c`

The full posterior covariance is carried for players appearing in consecutive
seasons. Production aging, process variance, box model, exposure shift and
canonical counted evidence are otherwise retained. Production artifacts were
not modified.

The fixed production scale (`c=50000`, box variance 8) was tested first. A
small sensitivity grid was then selected on development evidence through 2018;
`c=20000`, box variance 8 was the development winner. Castle was not used for
selection.

## Main results

### Next-season player evidence

| Score | Production | Multivariate c=20000 |
|---|---:|---:|
| Development wcorr through 2018 | 0.5230 | 0.5240 |
| Untouched 2019+ wcorr | 0.4970 | 0.4992 |
| Untouched 2019+ affine RMSE | 3.6878 | 3.7220 |

The rank improvement is tiny and changes sign across individual confirmation
seasons. The multivariate model is too optimistic in level on the confirmation
period, so this is not an unqualified player-level win.

For the motivating subgroup, young players with prior-season maximum teammate
share at least 0.60:

| Score, 2019+ | Production | Multivariate |
|---|---:|---:|
| wcorr | 0.4199 | 0.4395 |
| affine RMSE | 3.8467 | 3.8329 |

That subgroup result is directionally supportive but needs a frozen replication
because it was inspected after the overall prototype result.

### Chronological game-margin backtest

The existing walk-forward margin harness (2006-07 through 2017-18) gives:

| Regular season | Production | Multivariate c=20000 |
|---|---:|---:|
| Actual-minutes MAE | 9.72 | 9.62 |
| Actual-minutes correlation | 0.383 | 0.403 |
| Projected-minutes MAE | 9.65 | 9.55 |
| Projected-minutes correlation | 0.395 | 0.415 |

On common regular-season games, multivariate minus production absolute-error
MAE is -0.099 with a season-block 95% interval of approximately
[-0.134, -0.067]. The playoff improvement is similar in point estimate but its
interval crosses zero.

The existing joint Kalman-centered metric remains competitive: projected-
minutes MAE 9.53 and correlation 0.414. Thus covariance improves the standalone
state filter but does not establish a new overall best model.

### Modern 2019-2025 game-margin replication

A second walk-forward test was built from `player_game_facts`: preseason
one-step states, trailing pregame projected minutes, and four prior seasons for
affine calibration. It contains 8,289 regular-season games.

| 2019-2025 | Production | Multivariate | Existing jointk |
|---|---:|---:|---:|
| Actual-minutes MAE | 10.837 | **10.783** | 10.928 |
| Actual-minutes correlation | 0.424 | **0.432** | 0.405 |
| Projected-minutes MAE | 10.838 | **10.784** | 10.927 |
| Projected-minutes correlation | 0.421 | **0.429** | 0.401 |

The projected-minutes paired MAE difference is -0.054 with a season-block 95%
interval approximately [-0.086, -0.014]. Multivariate is better in 2019 and
2021-2024, essentially tied in 2020, and worse in 2025. This is a meaningful
modern confirmation, but the final-season reversal argues against immediate
promotion and for diagnosing time calibration before adoption.

## Castle diagnostic

Castle and Wembanyama's raw season ridge coefficient errors correlate about
-0.36. In the selected joint posterior, priors and the rest of the league
design reduce but preserve the relationship: their current posterior
correlation is about -0.14 per side.

| Castle state | Production | Multivariate c=20000 |
|---|---:|---:|
| Rookie filtered total | -1.72 | -2.55 |
| Current incoming prediction | -1.01 | -1.85 |
| Current filtered total | -0.93 | -0.06 |
| Next-year public-scale projection | -1.45 | -0.75 |

The model does not simply protect Castle from negative evidence. It evaluates
his rookie season more negatively, then allocates the current Spurs' excellent
joint results in a way that lets his strong current box prior and lineup
likelihood move him back near neutral.

The same experimental projection increases the minutes-weighted Spurs player
rating by about 0.55 points. Wembanyama rises as well, from approximately 6.61
to 8.45 on the projected public scale; the method is reallocating and restoring
team-level signal, not merely transferring a fixed amount from Wembanyama to
Castle.

## Limitations and next gate

- Cross-player covariance is retained only for consecutive-season returners;
  covariance through missed seasons is reduced to marginal variances.
- The atomic box observation is treated as independent across players even
  though its fitted coefficients also have uncertainty.
- The model remains additive and does not estimate Castle-Wembanyama synergy.
- The 2019+ confirmation target is another RAPM estimate, not ground truth.
- The new modern game test uses preseason states and pregame minutes, but no
  market/injury information. It validates team-outcome prediction rather than
  betting-market residual prediction.
- The `c=20000` likelihood scale changes meaning relative to production's
  scalar observation variance and needs formal likelihood/calibration work.

Recommendation: retain this as a strong candidate, not yet a production
replacement. The required modern game test now favors multivariate, including
against jointk, but 2025 reverses and player-level interval calibration remains
worse. Next diagnose the 2025 reversal and recalibrate state level/uncertainty
without using Castle or 2025 as the tuning objective. Promotion should require
that those changes preserve the locked historical and 2019-2024 gains.
