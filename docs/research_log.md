# Research Log

## 2026-07-12 — Project initialization

- Imported the conceptual direction from the shared ChatGPT conversation “Causal Inference in NBA Metrics.”
- Identified `C:\Users\Dave\Downloads\nba-onoff-publish` as the active repository.
- Confirmed existing player-game, game, stint, possession, RAPM, tracking and metric-pipeline assets.
- Selected primary-creator absence shocks as the first pilot.
- Identified timestamped pregame availability as the most important missing data source.
- Defined the initial work as a realized-absence response analysis unless a credible availability feed is added.
- No raw data or existing model code was modified.

### Next implementation checkpoint

Build a read-only audit script that selects canonical source files, reports schemas/date coverage/duplicate keys, and constructs candidate absence events without writing derived data by default.

## 2026-07-12 — First executable event audit

- Added `scripts/audit_creator_absence_pilot.py`.
- Unified the historical Kaggle player-game panel with the recent local box-score source.
- Normalized numeric, clock-string, and ISO-duration minute formats.
- Built rolling creator features shifted strictly before each target game.
- Added a 30-day recency bound and regular-season-only eligibility rule.
- Wrote the broad screening cohort to `derived/contextual_causal/creator_absence_candidates.csv`.
- Wrote integrity and coverage results to `outputs/contextual_causal/creator_absence_audit.json`.
- The output remains a realized-absence screening universe, not a verified unexpected-injury sample.

## 2026-07-12 — Strict shock cohort

- Added `scripts/build_creator_absence_shock_cohort.py`.
- Restricted the analytical cohort to seasons beginning in 2010 or later.
- Required at least 28 frozen prior minutes per appearance.
- Required the focal creator to have played in the team's immediately previous game.
- Required the last appearance to be no more than seven days earlier.
- Excluded games in the final seven days of each team's regular season.
- Preserved back-to-back status as a tier rather than discarding those events.
- Reduced 18,333 screening rows to 2,950 higher-credibility first-game shocks across 2,673 games and 306 players.
- Verified no duplicate event-player keys and all strict-filter invariants.
- Announcement timing remains unknown; the cohort supports quasi-experimental sensitivity work, not automatic causal identification.

## 2026-07-12 — Burden-transfer player-event panel

- Added `scripts/build_burden_transfer_panel.py`.
- Created one row per receiving teammate × strict creator-absence event.
- Built rolling 10-appearance expectations shifted before the event game.
- Added realized-minus-expected changes for minutes, FGA, FTA, 3PA, assists, turnovers, points, and composite creation load.
- Required five prior appearances for a complete baseline and flagged receivers with at least 12 expected minutes as the initial rotation analysis set.
- Produced 30,394 total rows and 23,426 eligible receiver-event rows across all 2,950 shock events.
- Verified no duplicate event-receiver rows and no events without an eligible receiver.
- Descriptive first pass shows average eligible-receiver increases of 2.04 minutes, 1.33 FGA, 0.42 assists, and 2.38 creation-load units relative to frozen rolling expectations.

## 2026-07-12 — First chronological model and robustness analysis

- Added `scripts/fit_first_burden_response_model.py` and `scripts/analyze_first_burden_results.py`.
- Trained through 2022-23, selected regularization on 2023-24, and held out 2024-25 plus 2025-26.
- Evaluated 4,231 receiver rows across 530 untouched creator-absence events.
- The full contextual model beat proportional redistribution on event-total MAE by 2.486 (95% event-cluster bootstrap interval 1.968 to 3.028) and RMSE by 3.249 (95% interval 2.629 to 3.877).
- The advantage persisted in both test seasons, back-to-back and non-back-to-back games, and rank-one and rank-two creator absences.
- Event-total correlation improved from 0.141 for proportional redistribution to 0.618 for the full contextual model.
- Ablation showed the generic contextual model was marginally better at event totals than models with receiver identity and player-specific shock elasticity.
- Concluded that contextual redistribution is predictably structured, but current free player-ID elasticity estimates are not supported as a stable player metric.
- Documented detailed interpretation in `docs/first_results_analysis.md`.

## 2026-07-12 — Capability-mediated response test

- Added `scripts/fit_capability_burden_response_model.py` and `scripts/analyze_capability_burden_results.py`.
- Built prior-season player capabilities from drives, passing, pull-up shooting, and catch-and-shoot tracking files.
- Used only the preceding season's traits, required at least 200 tracking minutes, and evaluated capability and generic models on identical rows.
- Training used 2022-23, validation used 2023-24, and 2024-25 plus 2025-26 remained held out.
- Prior tracking covered 7,034 of 8,257 modern eligible receiver rows; the test contained 3,542 rows across all 530 events.
- Capabilities improved receiver MAE by 0.044 and RMSE by 0.046 versus generic context.
- Event-cluster bootstrap intervals were positive for receiver MAE (0.006 to 0.083) and RMSE (0.003 to 0.089), indicating a small but repeatable allocation gain.
- Capability modeling did not improve event totals: event MAE change was -0.130 (95% interval -0.434 to 0.174) and RMSE change was -0.212 (-0.506 to 0.087).
- Concluded that player traits help determine which teammate absorbs burden, but the row-level formulation does not preserve or improve total team redistribution.
- Selected a two-stage next architecture: predict total event burden first, then allocate shares among teammates using capability vectors.

## 2026-07-12 — Two-stage and component decision tests

- Added `scripts/fit_two_stage_burden_model.py` and `scripts/fit_capability_component_models.py`.
- The total-then-capability architecture underperformed generic row-level context: test event RMSE was 17.080 versus 16.454 and receiver RMSE was 6.206 versus 6.212, offering no useful joint improvement.
- Validation selected equal residual reconciliation rather than expected-role weighting, another sign that the proposed allocation constraint was not capturing the mechanism.
- Separate component tests found small receiver-RMSE gains for minutes (0.010), FGA (0.023), 3PA (0.020), assists (0.016), points (0.011), and composite creation load (0.046).
- Most capability component models failed to improve event totals; only small gains appeared for 3PA and points.
- Established a decision: continue contextual team adaptation research, but suspend claims that current player-specific elasticities constitute a useful metric.
- Added `docs/research_decision_memo.md` with explicit evidence and a quantitative hurdle for revisiting the player-metric branch.

## 2026-07-13 — Defensive rim pilot foundation

- Shifted the research program from offensive burden absorption to causally structured defensive mechanisms.
- Added `scripts/build_halfcourt_rim_possessions.py`.
- Built 1,238,799 modern regular-season possession sequences across 6,135 games.
- Defined half-court risk at six seconds with four- and eight-second sensitivity variants.
- Separated first-chance rim attempts, second-chance rim attempts, and explicit putbacks.
- Initially and incorrectly concluded that shooting-foul location was unavailable by checking coordinates and shot distance but overlooking `area` and `areaDetail`. Corrected the pipeline: categorical foul location is nearly complete from 2022-23 onward; Restricted Area is the strict definition and non-RA 0-8 paint is a broader sensitivity definition.
- Added `scripts/analyze_rim_outcome_stability.py`.
- Found strong team-level reproducibility for first-chance half-court rim frequency (0.816 split-half; 0.655 next season) and moderate reproducibility for first-chance rim FG% (0.586; 0.514).
- Found weaker future persistence for second-chance rim frequency (0.339), explicit putbacks (0.211), and second-chance rim FG% (0.220).
- Decided to keep putbacks separate as a possession-finishing/scramble-defense pathway rather than pool them into primary rim protection.
- Documented the design and next causal stage in `docs/defensive_rim_pilot.md`.
- Reclassified the franchise-level stability analysis as a non-causal diagnostic that should not guide player-level inference.
- Attached exact lineups to 693,290 modern half-court possessions after requiring five distinct, nonzero players on both teams; discovered that the nominal lineup-complete flag sometimes retained player ID 0.
- Tested exact-opponent-five swaps, same-quarter contingent swaps, and immediately adjacent substitution stints, always holding an exact defensive four-man core.
- Exact and same-quarter comparisons failed to improve held-out prediction. The adjacent-stint design showed only weak disjoint-game replication: 0.110 for strict foul-inclusive rim access and 0.018 for conditional rim FG%.
- Kept the player-metric hurdle unmet: even the best local design was slightly worse than a zero-difference prediction on held-out substitution events.
- Revisited the all-player conclusion using prior-season rim-protector archetypes. After controlling the generic post-substitution decline with a same-season/period difference-in-differences, prior high-block bigs reduced foul-inclusive rim access by 2.00 percentage points versus their replacement; the broader prior rim-protector group reduced it by 1.23 points.
- The targeted effect was pathway-specific: recorded rim attempts fell, while Restricted Area foul rate and conditional rim FG% remained indistinguishable from zero.
- Threshold sensitivity remained negative across 1.0-2.5 prior blocks per 75 for bigs and 4.5-6.5 prior rim DFGA per game, supporting heterogeneous/archetype-specific modeling rather than an all-player average.
- Expanded the prior selector screen to height, blocks, fouls, fouls drawn, rebounds, contests, box-outs, charges, deflections, tracked rim volume, and prior rim make suppression, with 2024-25 and 2025-26 used as the later validation block.
- Contest volume (1.99 points less rim access), size plus blocks (1.75), and contest-to-foul ratio (1.69) outperformed blocks alone (1.25) as deterrence selectors in the later seasons.
- Found a separate tentative conversion-defense profile: block-to-foul ratio, size plus blocks, and prior rim DFG suppression selected groups allowing roughly 5.8-7.7 percentage points lower rim FG% in the later validation seasons, albeit on much smaller samples and after a multi-selector screen.
- Fouls drawn and personal-foul rate did not emerge as strong independent deterrence indicators. Smooth all-player multivariate models still failed, reinforcing a tail/archetype rather than universal linear model.
- Extended the possession and exact-lineup panels backward through 2020-21. The six-season panel contains 925,293 valid-lineup half-court possessions across 6,933 games; older foul location is unavailable, so six-season comparisons use recorded rim attempts and conditional rim FG% rather than foul-inclusive access.
- Blocks and size-plus-blocks selected fewer recorded rim attempts in all six seasons, pooling at -1.74 and -2.02 percentage points. The size-plus-blocks effect remained negative in the 2020-21/2021-22 extension (-2.42), original discovery block (-2.31), and later validation block (-1.59).
- Prior raw rim DFG% and rim DFG% versus expected selected lower conditional rim FG% in all six seasons, pooling at -6.89 and -7.72 points, but the FG event sample remained much smaller and individual-season intervals were wide.
- Corrected the extended report so its discovery summary contains only 2022-23 and 2023-24; the added 2020-21 and 2021-22 seasons are reported as a separate earlier extension rather than silently pooled into discovery.
- Ran the decisive blocks-controlled test using 2020-21 through 2023-24 for training and 2024-25 plus 2025-26 as held-out seasons. The rim-attempt test contained 14,630 substitution contrasts across 2,141 held-out games.
- No aggregate trait materially improved rim-attempt prediction over blocks alone. The only positive nested-model result, prior rim-defended volume, improved RMSE by 0.0050% with a game-clustered interval spanning -0.0034% to +0.0134%.
- Directly compared alternative top-15% selectors with blocks. Size plus blocks appeared 0.52 percentage points better and contests 0.70 points better, but both paired game-clustered intervals crossed zero. Comparable-blocker regressions also found no incremental feature with a reliable slope.
- Prior rim FG% and prior rim suppression had plausible signs for conditional rim FG%, but improved held-out RMSE by only 0.0669% and 0.0572%, with both uncertainty intervals crossing zero in a much smaller sample.
- Decision: the aggregate multi-field branch has not discovered useful signal beyond conventional block rate. Do not present its composites as a new metric; revisit only with a different behavioral estimand or richer contextual data.
- Reframed the defensive project around the direct valuation question: whether actual luck-adjusted points allowed contain portable contextual residual structure beyond frozen additive offensive and defensive RAPM expectations.
- Built `scripts/test_contextual_drapm_residuals.py` and a 12,196-row offense-team-game panel across 6,098 games. One-step-ahead Kalman RAPM states are frozen before the target season for established players; player traits are shifted from the immediately preceding season.
- Preregistered five plausible matchup conditions: rim pressure/rim anchor, spacing/defensive size, offensive/defensive rebounding, turnover proneness/disruption, and foul pressure/defensive foul rate. Compared them against both frozen RAPM and RAPM plus the same traits additively.
- On 4,854 held-out 2024-25/2025-26 rows, additive traits improved RMSE only 0.092% versus RAPM with an interval crossing zero. All five contexts together were 0.066% worse than the additive model; no individual interaction improved reliably.
- Tested a second family based only on frozen RAPM distribution: opponent star concentration, defensive weak-link severity, anchor strength, and rating spread. Using 2024-25 for selection and 2025-26 for confirmation, rating-shape features worsened RMSE by 0.275%; adding the matchup interactions worsened it by 0.318%.
- A season-stability scan found no residual condition with a stable held-out effect. Plausible stories such as foul pressure versus foul rate and star concentration versus a weak link reversed in at least one late season and failed predictive tests.
- Current decision: the structured-residual framework is the correct test of contextual overall value, but these first condition families provide no supported adjustment to defensive RAPM.
- Replaced the restrictive linear products with an explicit “styles make fights” test: five frozen 3×3 low/middle/high offense-style by defense-style grids. Each joint grid was tested against the corresponding offense and defense bins entered separately, so only incremental joint structure counted.
- Used 2024-25 for tuning and 2025-26 alone for confirmation. Rim pressure/rim anchor, spacing/size, turnovers/disruption, and foul pressure/discipline did not improve reliably; all five grids jointly worsened RMSE by 0.052% versus marginal style bins.
- Offensive versus defensive rebounding was the only near-signal, improving 2025-26 RMSE by 0.041% with an interval of -0.002% to +0.086%. It failed stability: +0.044% in 2023-24, -0.032% in 2024-25, and +0.041% in 2025-26, with important cells reversing across seasons.
- Kept the rebounding grid only as an unchanged future-season watch-list hypothesis. It does not currently justify a contextual RAPM adjustment.

## 2026-07-18 — Preregistered probabilistic salvage of uncertain lineups

- Measurement problem: 337 regular-season games have complete candidate
  five-on-five timelines but fail at least one canonical evidence gate. Whole-game
  deletion creates non-random player exposure; hard selection of one reconstruction
  would create false lineup certainty.
- Target estimand: the time-decayed additive RAPM coefficient that would be obtained
  if the true ten-player design row were observed for every scoring interval. This
  is a measurement-error problem, not a causal claim that RAPM is ground truth.
- Treatment/intervention: none. Outcome is the observed/calibrated stint scoring
  margin (and later possession outcome); the uncertain object is player presence in
  the design matrix. Official score, player minutes and plus-minus are validation
  constraints, not outcomes to be causally attributed.
- Candidate reconstructions are restricted to independently generated, versioned
  repair artifacts. Duplicate whole-game timelines are collapsed. No production
  stint, possession, RAPM or NERD output is modified.
- The measurement model is trained only on rejected alternative reconstructions of
  games whose canonical lineup is known. Candidate-to-canonical time-weighted lineup
  agreement is the target. Validation is chronological, holding out 2023-24 onward,
  and grouped by game.
- For unresolved games, whole-game alternatives receive probabilities derived from
  the frozen measurement model. Multiple completed datasets draw one coherent
  whole-game reconstruction per game; they never mix incompatible stints within a
  game. Games with only one observed candidate receive an explicit uncertainty flag
  rather than being described as multiply identified.
- Required diagnostics before RAPM use: held-out candidate-ranking accuracy,
  calibration of predicted lineup agreement, candidate count/entropy, player-minute
  exposure concentration, and sensitivity of RAPM to canonical-only, deterministic
  best-candidate, and multiple-imputation inputs.

## 2026-07-18 — Probabilistic salvage results

- Collected 719 distinct versioned candidate timelines for all 337 unresolved
  games; 253 games have multiple candidates and 84 have only one.
- Rejected the learned candidate ranker because its held-out lineup-slot
  agreement (97.09%) did not beat the transparent evidence rule (97.17%).
  Imputation probabilities now use only the transparent evidence score.
- Built 20 coherent whole-game imputations for 289 games with a complete,
  score-consistent candidate. Retained the remaining 48 games as official
  minute-weighted game-level RAPM observations. No game is deleted and no
  uncertain game is promoted to the canonical tier.
- In a chronological post-2017 masking test of 180 known-canonical games,
  omission produced exposed-player RAPM MAE 0.0353, aggregate retention 0.0321,
  and lineup imputation 0.0196. Corresponding 95th-percentile absolute errors
  were 0.1417, 0.1308 and 0.0765.
- In a diagnostic 2025-26 alpha-500 fit, adding the supplemental tier changed
  established exposed players by 0.159 points per 100 on average, with a 0.438
  95th percentile and 0.9956 canonical-versus-completed correlation.
- Between-imputation variance badly understates total reconstruction error in
  masking validation. Any production use must add a masked-validation error
  component, particularly for the 84 single-candidate games.
- Full interpretation is recorded in
  `outputs/contextual_causal/probabilistic_lineup_salvage_report.md`. No
  production model or site artifact was changed.

## 2026-07-18 — Canonical counted NERD and atomic-prior review

- Reattached counted possessions to the canonical plus supplemental lineup
  universe for the current six-year NERD window. Of 7,786 games, 7,238 (92.96%)
  pass possession attachment, official scoring and balance gates.
- Ninety-three of 94 supplemental games in the window pass counted gates; the
  remaining game is retained through the official-minute aggregate fallback.
- Produced a non-production alpha-4000 current-season preview centered
  separately on rolling v1 and atomic priors. Among 383 players with at least
  1,000 current counted possessions, the v1 preview correlates 0.9636 with the
  previous candidate; mean absolute change is 0.558 points per 100.
- Audited the 35 atomic coefficients in current rolling form. Major offensive
  and defensive directions are mostly basketball-plausible and stable, but
  atomic offense and total remain worse than v1 on untouched 2019-25 evidence.
- Found the decisive implementation defect: current per-75 atomic inputs are
  not adequately sample-size stabilized. Individual low-sample atoms contribute
  3-7 points per 100 and create priors such as +11.50 defense for Chris Mañon on
  94 counted possessions. The prior-centered update appropriately preserves,
  rather than fixes, this bad prior.
- Decision: the v1-centered values are the usable preview. Do not promote raw
  atomic values until atom-specific empirical-Bayes shrinkage is selected on
  pre-2019 data and confirmed unchanged on 2019-25.

## 2026-07-18 — Preregistered denominator-aware atomic prior

- Measurement question: can natural exposure denominators stabilize each
  atomic rate enough to remove tiny-sample prior explosions while improving or
  preserving prediction of independent RAPM evidence?
- This is predictive measurement work, not a causal intervention. There is no
  treatment or mediator. The outcome is independently fit, counted-possession
  single-season offensive or defensive RAPM evidence; the estimand is the
  pre-RAPM box prior's best prediction of that noisy latent player-season value.
- Potential confounding remains: atoms can encode role, teammates, scheme and
  opponent context. Coefficients will not be interpreted as causal effects.
- Natural denominators are frozen before fitting: possessions for ordinary
  per-75 atoms; rebound opportunities for rebound rates; source-covered
  possessions for hustle atoms; defended rim attempts for rim dFG difference;
  and no shrinkage for age, height or relative wingspan.
- Each rate is shrunk toward its rolling training-window mean with reliability
  `denom / (denom + lambda * median_training_denom)`. Lambda and ridge alpha
  are selected only from chronological predictions through 2018, separately
  for offense and defense, then frozen.
- Untouched evaluation is 2019-25 same-season and next-season counted RAPM
  evidence, compared with the raw atomic and v1 priors. Promotion additionally
  requires a low-sample stress test showing that extreme atom contributions
  and priors no longer dominate players with negligible exposure.

## 2026-07-18 — Denominator-aware atomic prior results

- Corrected a source-shape defect discovered by the denominator audit:
  `rim_dfga` is a season total repeated on each game row. The old rate was
  unaffected because both weighted numerator and denominator repeated, but a
  naive summed denominator overstated reliability by roughly games played.
  The new artifact recovers the single season exposure.
- Pre-2019 selection chose 0.10 typical-player-season shrinkage for offense and
  0.25 for defense. The defensive optimum was broad (0.25 and 0.50 differed in
  development correlation by only 0.00006), not a sharp grid accident.
- On untouched 2019-25 evidence, raw atomic versus denominator-aware total
  correlation improved from 0.5073 to 0.5113 in the same season and from
  0.4162 to 0.4290 one season ahead. The latter exceeds v1's 0.4233.
- Defense drove the gain: same-season defensive correlation rose from 0.5027
  to 0.5087 and next-season from 0.4310 to 0.4477. Denominator-aware defense
  beat raw defense in six of seven individual holdout years at each horizon.
  Offense changed less: 0.5575 to 0.5580 same-season and 0.4583 to 0.4670 next.
- The low-sample failure is substantially repaired. Among 76 current players
  below 200 feature possessions, maximum absolute raw total prior fell from
  13.01 to 2.76 and the 95th percentile from 8.51 to 2.32. Chris Mañon's raw
  +13.01 prior became +2.54; Alex Antetokounmpo +8.93 became +1.88; Tristen
  Newton +8.62 became +1.39. The largest single atom contribution in this
  group is now 1.01 points rather than several points.
- Established-player values are stable: for current players with at least
  1,000 feature possessions, the largest raw-to-denominator total-prior change
  is 0.92 and the bulk are much smaller.
- Decision: denominator-aware atomic is a viable research successor to raw
  atomic and should replace it in further previews. It is not yet a blanket
  replacement for v1: v1 remains better on same-season total prediction, while
  denominator-aware atomic is better on next-season total and defensive
  prediction. Production/site files remain unchanged.

## 2026-07-18 — Denominator-aware atomic promoted in research pipeline

- Changed the default `atomic` prior used by the canonical salvage and canonical
  counted NERD builders to
  `rolling_prior_atomic_denominator_poss.parquet`. The unsmoothed prior remains
  available only under the explicit `atomic_raw` label and comparison columns.
- Rebuilt the standard canonical counted preview. Its `nerd_atomic` columns now
  exactly match the separately validated denominator-aware preview (maximum
  difference 0.0); `nerd_atomic_raw` preserves the old comparison values.
- Rebuilt the chronological counted-possession candidate with explicit
  `prior_model=atomic_denominator`. Alpha 4000 was again selected using only
  pre-2019 evidence. On untouched 2019+ next-season evidence it scores 0.4791
  total, 0.5095 offense and 0.4824 defense.
- Rebuilt the probabilistic-salvage preview with both promoted and raw atomic
  columns. No production database or site-facing artifact was modified.

## 2026-07-18 — Preregistered residual-composite offensive hybrid

- Question: did v1's overlapping offensive ratios add durable nonlinear/load
  information beyond the atomic event counts, or merely same-season role and
  coefficient suppression artifacts?
- This remains predictive measurement, with no treatment or causal claim.
  Outcome and estimand remain independent counted-possession offensive RAPM
  evidence and its box-prior prediction.
- The denominator-aware 35-atom design and its offensive shrinkage strength
  (lambda 0.10) are frozen. Defense is not refit and remains the promoted
  denominator-aware atomic defense prior.
- Candidate composite groups are fixed before viewing 2019+ results:
  `role_load` = usage, assist percentage and MPG; `efficiency` = TS%, eFG% and
  FT%; `shot_style` = 3PA rate, unassisted share and point/attempt location
  shares; `missing_events` = fouls drawn and shots blocked; `compact` = usage,
  assist percentage, MPG, TS%, eFG%, 3PA rate, unassisted share, fouls drawn
  and shots blocked; `all_residual` = their union.
- Ratios/rates are empirically shrunk toward the rolling training mean using
  natural exposures (possessions, games, FGA, FGM, FTA, scoring attempts or
  points). Each composite is then residualized against the shrunk atoms using
  training data only. Thus it can enter the RAPM fit only through information
  not linearly reconstructed from the atoms.
- Model group and target ridge alpha are selected using only evidence through
  2018, maximizing the mean of same-season and next-season offensive
  correlations. The untouched 2019-25 report is confirmatory. Promotion
  requires a next-season offensive improvement without a material same-season
  loss, stable annual direction, and no return of low-sample explosions.

## 2026-07-18 — Residual-composite offensive hybrid results

- Pre-2019 selection chose the nine-feature `compact` residual layer with
  alpha 10. It improved development offense from 0.6045 to 0.6114 in-season
  and from 0.5161 to 0.5228 one season ahead.
- The frozen model failed confirmation on 2019-25: same-season offense was
  0.5573 versus the atomic baseline's 0.5580, and next-season offense was
  0.4603 versus 0.4670. It is rejected and the promoted denominator-aware
  atomic model remains unchanged.
- The current residual coefficients reproduce the old suppression pattern:
  residual eFG% +0.281, residual TS% -0.230 and residual MPG +0.230. Even after
  removing linear atomic content, these composites appear to learn unstable
  era/role contrasts rather than durable player signal.
- Exploratory, post-confirmation group diagnostics found one useful clue. The
  genuinely missing event group (fouls drawn and shots blocked), unlike the
  overlapping ratios, improved 2019-25 offense from 0.5580 to 0.5601 in-season
  and from 0.4670 to 0.4708 next-season. Fouls drawn supplied almost all of the
  gain (0.5594 and 0.4707 alone); blocked shots added little. Fouls drawn beat
  the atomic baseline in five of seven same-season and six of seven
  next-season years.
- Because this clue was identified after inspecting the confirmation period,
  it is not promoted. It should be treated as a preregistered candidate for an
  independent target/future season and, if confirmed, added as a true
  denominator-aware event atom rather than as an overlapping composite.

## 2026-07-18 — Preregistered foul-generation atom decomposition

- Clarification: total fouls drawn already exists in the extended box feed and
  v1 prior. It was omitted—not unavailable—from the 35-atom specification.
  Its historical feed is implausibly sparse through 2004 and reliable from
  2005 onward; pre-2005 values will be missing, not treated as zero.
- New opportunity atoms are defined before viewing new results:
  `ft_foul_trips_drawn_75` counts nontechnical free-throw sets by shooter,
  once per set rather than once per attempt; `other_fouls_drawn_75` is total
  fouls drawn minus FT-producing trips and, where tracked, charges drawn;
  `charges_drawn_75` remains the existing hustle atom. Free throws made and
  missed remain outcome atoms, distinct from the foul opportunity.
- Negative residual other-foul counts caused by source disagreement are
  quarantined as missing rather than clipped into fabricated zeros. Season
  coverage, trip-to-FTA plausibility and subtraction rates are required
  audits. Every new rate uses possessions as its natural reliability
  denominator; charges retain source-covered possessions.
- Candidate offensive sets are fixed as baseline, total-fouls-drawn,
  FT-trip-only, other-fouls-only and the two-atom decomposition. The atomic
  shrinkage rule remains unchanged. Candidate and ridge alpha are selected by
  mean same/next-season offensive correlation through 2018 only; 2019-25 is
  confirmatory. Defense is frozen.
- Because the general fouls-drawn clue was observed in the previous 2019-25
  exploration, this test is mechanistic refinement rather than a wholly fresh
  confirmation. Promotion requires improvement beyond the previously observed
  total-fouls residual result, plausible coefficients, stable yearly direction
  and no low-sample explosion.

## 2026-07-18 — Foul-generation atom decomposition results

- Built 1.75M free-throw events into one nontechnical foul-trip observation per
  set. The trip/FTA ratio is stable near 0.53 in every season, supporting the
  set logic. Total-fouls-drawn coverage is confirmed unusable through 2004 and
  plausible from 2005 onward. Only 50 post-2004 player-seasons have a negative
  cross-source residual; all are retained as missing rather than clipped.
- Pre-2019 selection chose `other_fouls_drawn_75` alone with alpha 1. The
  FT-producing trip atom was worse than baseline, confirming that FTM and FT
  misses already capture nearly all of that information. The useful piece is
  non-FT/other foul pressure.
- On 2019-25 offense, other fouls improve same-season correlation from 0.5580
  to 0.5595 and next-season from 0.4670 to 0.4708. Gains are positive in five
  of seven seasons at both horizons. The current coefficient is a moderate
  +0.185 points per training SD.
- Sample behavior is safe: below 200 current possessions, the maximum offensive
  prior change is 0.21 and the 95th percentile is 0.11. Among established
  players the maximum change is 0.45.
- The decomposition is basketball-coherent—players drawing many shooting
  fouls are already credited through FT outcomes, while players generating
  off-ball, pre-shot, rebounding and bonus pressure receive the new signal.
- Decision: retain `other_fouls_drawn_75` as a versioned candidate atom but do
  not promote it yet. It only improves the already-observed total-fouls result
  by about 0.0001 at either horizon, so it fails the preregistered requirement
  for new predictive confirmation. The decomposition improves interpretation,
  not established predictive strength.

## 2026-07-18 — Decision to promote denominator-aware atomic throughout NERD

- Product/model decision: replace v1 box centers with the confirmed
  denominator-aware atomic prior throughout the production NERD, Kalman state
  and site export. Do not include residual composites or the exploratory
  other-fouls-drawn candidate.
- Rationale: v1's remaining advantage is limited to same-season offense;
  denominator-aware atomic is more interpretable, eliminates fringe prior
  explosions, improves defense and next-season total prediction, and avoids
  unstable suppression coefficients.
- Preserve the current v1-based metric, Kalman state and site JavaScript as a
  timestamped rollback bundle before overwriting any production artifact.
- Use the frozen chronological atomic priors for 2004-25. For 1996-2003,
  where a ten-year past-only training window is unavailable, use within-era
  leave-one-season-out denominator-aware atomic fits, explicitly labeled as
  historical backfill rather than chronological predictions.
- Rebuild the existing production joint solve and Kalman stages against the
  atomic-compatible prior schema, then regenerate `data/nerd_seasons.js`.
  Required gates: complete player-season coverage, no tiny-sample explosions,
  finite O/D/total values, current-season comparison to the canonical counted
  preview, schema-compatible site export, and explicit prior provenance.

## 2026-07-18 — Production atomic promotion completed

- Rebuilt the production prior, joint NERD solve, Kalman states and site export
  with `prior_model=atomic_denominator`. The production metric contains 15,090
  unique player-seasons and the Kalman output 14,356; all required O/D/total
  values are finite. The site export contains 15,760 rows and explicit atomic
  provenance.
- Posterior strength and Kalman hyperparameters are now selected only on
  evidence through 2018, with 2019+ reported untouched. The joint solve chose
  alpha 4000; its next-season correlation is 0.5482 on development evidence
  and 0.5302 on 2019+ evidence.
- Established-player results remain close to v1: among 2025-26 players with at
  least 1,000 possessions in both versions, total NERD correlates at 0.974 and
  mean absolute movement is 0.392 points per 100.
- The fringe-player pathology is removed. Among 72 current players below 200
  possessions, maximum NERD is +2.55 and the 95th percentile +2.02. Tristen
  Newton fell from +8.31 under v1 to +1.05; N'Faly Dante from +6.39 to +0.88.
- Local browser validation loaded `nerd.html` without console warnings/errors
  and displayed the expected atomic leaderboard. The v1 artifacts remain in
  `outputs/contextual_causal/production_v1_rollback_20260718/`.
- Scope note: this promotion replaces the prior across the existing production
  evidence pipeline. It does not yet migrate production to the separate
  canonical counted-possession evidence universe. The current established-
  player outputs from those two atomic evidence universes correlate at 0.930;
  that migration remains a separate decision.

## 2026-07-18 — Canonical counted evidence promoted to production

- Supersedes the scope limitation above. Production NERD, season evidence,
  Kalman states and the site export now use
  `canonical_counted_possessions_v1`; `seconds / 24` and the old
  `prepared_stints.parquet` likelihood are no longer production inputs.
- Built one reusable evidence universe covering all 35,522 intended regular-
  season games and 2,431 playoff games. Of 37,953 total games, 37,740 retain
  full possession-by-stint lineups. The remaining 165 lineup games and 48
  no-lineup games remain included at exact-possession aggregate resolution.
- Playoffs are loaded directly from the repaired playoff stints and joined to
  official `Games.csv` dates, fixing the old cached season-date inheritance.
- Pre-2019 event parsing sometimes leaves a side-count imbalance. For 8,915
  flagged games, observed possession locations are retained and only the two
  side totals are calibrated to their common mean. Small scoring-feed
  discrepancies are reconciled to the canonical adjusted game score; final
  maximum game-total error is 2.84e-14 points.
- Alpha 4000 remains the pre-2019 selection. Next-season total correlation is
  0.508 on development and 0.492 on untouched 2019+ evidence. The counted
  model scores 0.504 versus BPM's 0.444 and 0.499 versus RAPTOR's 0.449 on
  their respective matched rows.
- Kalman selection on pre-2019 evidence chose q=1.0, c=50,000 and prior
  variance 8.0; primary predictive correlation is 0.523 in development and
  0.497 on 2019+ confirmation.
- Final artifacts contain 14,564 unique metric player-seasons and 14,005
  Kalman states with finite O/D/total values and explicit prior/evidence
  provenance. Local browser validation displayed the counted leaderboard with
  no console warnings or errors.

## 2026-07-18 — Public on/off and RAPM canonical counted rebuild

- Replaced the public regular-season and playoff on/off production inputs with
  canonical counted-possession player-game evidence.
- 37,740 games use exact stint-level lineup exposure. The 213 aggregate-resolution
  games retain exact game possession totals and allocate player exposure from
  official minutes; the resolution is explicit on every player-game row.
- On/off offensive and defensive ratings now use their own counted offensive and
  defensive possession denominators. The production report path no longer calls
  pbpstats or estimates possessions as minutes × 100/48.
- Regular-season and playoff RAPM were refit as unified offensive/defensive ridge
  models from the same counted evidence, including alpha 10/500 season estimates
  and the regular multi-season windows. Raw targets restore exact source-game
  scoring while retaining canonical within-game possession placement.
- Official player-game plus-minus is preserved exactly in the on/off artifact.
  Seven regular-season source game IDs have no official player box score and
  therefore cannot produce player-game on/off rows: 22400524, 22400532, 22400537,
  22400538, 22400617, 22400627, and 22400988. Their usable lineup/aggregate
  evidence remains in RAPM.
- Validation: 728,788 regular-season and 48,987 playoff player-game rows; all
  displayed numeric inputs finite, no negative possession exposures, Python
  compilation passed, and all four generated pages rendered headlessly without
  JavaScript errors.

## 2026-07-19 — Multivariate lineup-covariance Kalman prototype specified

- Motivation: the production season filter reduces RAPM reliability to
  `c / possessions` and updates each player independently. That discards the
  lineup design's player-to-player covariance. In 2025-26 evidence, Stephon
  Castle shared roughly 62% of his counted possessions with Victor Wembanyama;
  their ridge coefficient errors are correlated about -0.36 on both sides of
  the ball, so their combined contribution is better identified than its
  individual allocation.
- Estimand: predictive latent player O/D contribution for the contexts
  represented by NBA stint evidence. This remains a predictive realized-value
  estimate, not causal or portable value. Low lineup diversity changes
  uncertainty and prior reliance; it is not itself a negative treatment or
  value penalty.
- Outcome: canonical luck-adjusted points per 100 at directed-stint grain,
  with exact counted possessions as likelihood weights. Aggregate fallback
  games retain their existing exact-possession design.
- Treatment/intervention: none. This is an observational predictive state
  model. Teammates, opponents and realized lineups form the likelihood design;
  box features are an independently regularized observation, not causal
  controls.
- Main comparison: hold the production `q`, `c` and box variance fixed, replace
  the independent season-RAPM measurement update with the joint Gaussian stint
  likelihood. Carry the resulting covariance for players who appear in
  consecutive seasons; initialize new or gap-returning players independently.
- Validation is frozen before fitting: one-step player prediction against
  next-season evidence through 2018, untouched 2019+ confirmation, plus the
  existing chronologically calibrated game-margin test. Report overall,
  young-player and high-entanglement sensitivity. Castle is a diagnostic case,
  not a tuning target.
- Implementation safety: write only an experimental script and artifacts under
  `outputs/contextual_causal/multivariate_kalman/`; do not overwrite production
  metric, Kalman, site or raw-data files.

## 2026-07-19 — Multivariate lineup-covariance prototype completed

- Implemented `metric/build_kalman_multivariate.py`. It updates the full active
  season O/D vector from `X'WX` and carries consecutive-player covariance.
  The selected development configuration is `q=1`, `c=20000`, box variance 8.
  Output has 14,005 unique finite player-seasons; production remains untouched.
- Next-season evidence: development wcorr 0.5240 versus production 0.5230;
  untouched 2019+ 0.4992 versus 0.4970. Confirmation affine RMSE is worse,
  3.7220 versus 3.6878, from an optimistic level shift.
- Existing chronological regular-season margin test improves materially:
  projected-minutes MAE/correlation 9.55/0.415 versus production 9.65/0.395.
  Paired season-block MAE improvement is about 0.099, 95% interval 0.067-0.134.
  Playoff direction is favorable but uncertain. The existing jointk candidate
  remains competitive at 9.53/0.414.
- Young, highly tethered 2019+ players improve from 0.4199 to 0.4395 wcorr and
  3.8467 to 3.8329 RMSE; treat this inspected subgroup as exploratory pending a
  frozen replication.
- Castle's current filtered state moves from -0.93 to -0.06 and his centered
  next-season projection from -1.45 to -0.75. The selected posterior preserves
  about -0.14 Castle-Wembanyama correlation per side. It also raises the Spurs'
  minutes-weighted projection roughly 0.55, so this is not merely pairwise
  credit transfer.
- Decision: promising candidate, not production-ready. Require a modern
  2019-2025 chronological game-outcome test and interval/level recalibration
  before considering promotion. Full details are in
  `outputs/contextual_causal/multivariate_kalman/prototype_report.md`.

## 2026-07-19 — Modern multivariate margin replication

- Added `metric/backtest_multivariate_modern.py`: 8,289 regular-season games,
  2019-20 through 2025-26, using one-step preseason states, trailing pregame
  projected minutes and four-season rolling affine calibration.
- Projected-minutes MAE/correlation improve from production 10.838/0.421 to
  multivariate 10.784/0.429. Existing jointk is 10.927/0.401, partly with lower
  player-game coverage. The paired multivariate MAE gain is 0.054; season-block
  95% interval approximately 0.014-0.086.
- Multivariate wins in 2019 and 2021-2024, is nearly tied in 2020, and loses in
  2025. This confirms that the old-era margin result is not isolated. At this
  stage the most recent reversal and uncertainty calibration were held as
  promotion checks; the following entry records the added calibration test and
  final decision.

## 2026-07-19 — Multivariate lineup-covariance Kalman promoted

- Reconsidered the promotion decision on structural grounds and added an
  uncertainty-calibration check. The independent filter's standard errors are
  substantially too wide against next-season evidence (development standardized
  residual SD 0.716; confirmation 0.637). The multivariate filter is much closer
  to its advertised uncertainty (0.997 and 0.895, respectively), with 68%/95%
  confirmation coverage of 74.2%/96.7%.
- Locked production parameters at the development-selected values: `q=1`,
  `c=20000`, box variance 8. The production state contains 14,005 unique finite
  player-seasons and exactly matches the clean promotion-candidate artifact.
  Provenance is embedded as `atomic_denominator`,
  `canonical_counted_possessions_v1`, and
  `multivariate_stint_gaussian_v1`.
- Promoted the joint stint filter to the production Kalman parquet/CSV and
  regenerated `data/nerd_seasons.js`. Historical published O-NERD, D-NERD and
  NERD values are unchanged; the new state affects the 2026-27 forecasts and
  correctly carries offense-defense covariance into published uncertainty.
- Stephon Castle's centered 2026-27 projection is -0.75 (-0.29 offense, -0.46
  defense); Victor Wembanyama is +8.45. The shared team-projection consumer now
  puts San Antonio at +7.2 net rating and 60 wins under the current depth chart.
- Browser smoke tests passed for both `nerd.html` and
  `team-projections.html`, with no JavaScript warnings or errors. The Castle row
  rendered at -0.8 after display rounding.
- The immediately preceding independent-filter state, CSV and site payload are
  retained under
  `outputs/contextual_causal/production_independent_kalman_rollback_20260719/`.
  Full promotion details are in
  `outputs/contextual_causal/multivariate_kalman/production_promotion_report.md`.

## 2026-07-19 — NERD downstream release contract

- Audited all repository consumers of the production Kalman state and shared
  NERD payload. The covariance filter applies to the forward temporal layer;
  the public consumers are the NERD projection season, team win projections,
  and player value. Raw RAPM, on/off and screening/search pages are separate
  outputs. The local cross-metric screen intentionally uses current-season
  historical `metric_v0`, not the forward Kalman projection.
- Added `scripts/audit_nerd_release.py`, which fails publication on state/payload
  provenance mismatch, duplicate or non-finite projections, an unexpected or
  missing consumer, or a stale cache token.
- `scripts/build_nerd_data.py` now hashes its generated payload and updates all
  three public consumers to that exact version automatically. This prevents the
  stale-asset behavior observed during the covariance deployment.
- Corrected the multivariate builder's no-argument default to the locked
  production likelihood scale `c=20000`; the experimental `50000` default could
  otherwise have silently regenerated a different model.
- Clean release audit: 14,005 state rows, 15,228 payload rows, 664 projections,
  payload version `4267f3e17de5`, and exactly three declared consumers.

## 2026-07-19 — Context-transport study specified

- Question: when a player enters a lineup environment materially different
  from the one represented in his history, can frozen context improve his
  subsequent performance forecast beyond covariance-NERD?
- Claim tier: predictive transport, not causal player value. Team changes,
  injuries and rotation decisions are selected; the study will not interpret
  context coefficients as interventions without a separate design.
- Unit: returning player-season. The player's prior environment is his
  possession-weighted teammate environment in the preceding season. His new
  environment is measured only in an early-season exposure window. Teammate
  capability values are frozen from the preceding season. The target is
  performance in the non-overlapping remainder of the season.
- Baseline: the production covariance model's one-step preseason O/D state.
  Outcome: a late-window luck-adjusted lineup solve, expressed as the player's
  residual contribution beyond the frozen baseline. The first diagnostic asks
  whether context distance predicts absolute forecast error; the primary test
  asks whether a small, partially pooled capability-by-context-change model
  improves signed prediction.
- Context coordinates will remain continuous: creation concentration, spacing,
  rim pressure/finishing, turnover tendency, foul pressure, offensive
  rebounding, size/rim protection, disruption and defensive rebounding.
  Archetype labels may summarize the coordinates but will not create arbitrary
  hard clusters for fitting.
- Timing safeguards: no current-season box outcomes in context features; no
  full-season lineup shares; early and target windows do not overlap. Actual
  early rotations make this an in-season transport forecast, not a preseason
  roster projection.
- Chronology is frozen before modeling: development through 2021-22,
  specification/regularization selection in 2022-23 and 2023-24, untouched
  confirmation in 2024-25 and 2025-26. If historical trait coverage forces a
  later start, the endpoints remain fixed and only development coverage falls.
- Promotion standard: directional stability across both confirmation seasons,
  improvement concentrated in genuinely high-distance transitions, no
  material degradation in ordinary contexts, and improvement in basketball
  process outcomes consistent with the fitted direction. A global average
  interaction without transport-subgroup confirmation is not sufficient.

### Mechanism-first addendum (specified before fitting)

- Because aggregate value can hide offsetting role changes, the final diagnostic
  predicts late-season changes in observable basketball processes: 3PA, FTA,
  turnovers, offensive rebounds, assists, assisted field goals, unassisted field
  goals and 2PA, each per 75 on-court possessions.
- The unit and timing remain the returning player-season. The outcome is the
  player's rate after his team's first 15 games minus his full prior-season
  rate. Prior-season rates and frozen own-player capabilities form the fair
  baseline, so ordinary regression to the mean is not credited to context.
  Context models add only the early measured teammate-environment change and,
  separately, a compact own-capability-by-environment-change interaction.
- Require at least 750 prior-season and 750 late-window possessions. Ridge
  strength is chosen only on 2022-23 and 2023-24; 2024-25 and 2025-26 remain
  confirmation seasons. The combined score gives each standardized process
  outcome equal weight. A useful finding must improve the combined score in
  both confirmation seasons and have interpretable, directionally stable
  outcome-level effects; an isolated pooled win is insufficient.

## 2026-07-19 — Context transport results: role signal, not value signal

- The leakage-safe transport panel contains 5,581 returning player-seasons.
  The direct target was late-season luck-adjusted lineup value beyond the
  frozen preseason covariance-NERD state. Against a calibrated intercept,
  context distance lost 0.16%, directional context change lost 0.10%, and
  capability-by-context interactions lost 0.57% on 2024-25 and 2025-26.
  All three reversed direction between the two confirmation seasons. Actual
  team changers also lost. This rejects a direct context adjustment to NERD.
- Context did not reliably identify underadvertised uncertainty either. A
  directional-context variance model improved Gaussian NLL by only 0.00149;
  the player-cluster 95% interval for that gain was [-0.00213, 0.00532]. No
  production uncertainty change is justified.
- The pre-specified mechanism test was more promising. Before seeing the
  player's current-season statistics, teammate-context change improved the
  combined forecast of late changes in eight per-75 role outcomes by 1.61%
  (player-cluster 95% interval 0.77% to 2.41%). It improved in both
  confirmation seasons, reached 3.70% in the high-distance subgroup, and its
  largest outcome gains were turnovers (4.42%), FTA (2.66%) and 2PA (2.49%).
- Post-result robustness added the player's own first-15-game box rates to the
  baseline, because those rates are available whenever the early lineup
  context is available. The simple context-change model retained a small
  0.23% gain: 0.23% in 2024-25, 0.22% in 2025-26, and a player-cluster 95%
  interval of 0.01% to 0.45%. The gain was concentrated among actual team
  changers (0.82%) and high-distance environments (0.93%), versus 0.05% for
  same-team players. Capability interactions were weaker (0.13%, interval
  -0.14% to 0.40%), so the data favor simple environment changes rather than
  an elaborate archetype interaction surface.
- Basketball interpretation: a different teammate mix modestly helps forecast
  the role a player will settle into, especially after a move, but it has not
  helped forecast how valuable that player will be after RAPM and the existing
  box prior are accounted for. The credible next bridge to player value is not
  a direct context bonus. It is a mover-only test that predicts event-window
  atomic box components, passes those predicted role changes through the
  already-frozen atomic coefficients, and then asks whether the transported
  box prior improves later RAPM/NERD. Until that succeeds chronologically, the
  production model remains unchanged.

## 2026-07-19 — Observed-extremes additivity boundary test specified

- Question: within the unusual lineup compositions NBA coaches actually used,
  does adjusted scoring systematically depart from the sum of player NERD
  offense and defense ratings? This is a predictive calibration/support test,
  not evidence about arbitrary lineups outside the observed distribution and
  not a causal estimate of changing lineup composition.
- Unit: offense-side exact-lineup-pair within game, aggregating repeated stints
  with the same five offensive and five defensive players. Outcome is
  luck-adjusted points per 100 possessions, centered on that season's league
  scoring environment. The baseline expectation is the sum of one-step
  preseason covariance-NERD offense for the five offensive players minus the
  corresponding defense ratings for the five defenders. Ratings and all
  composition traits are frozen before the season; current-season performance
  is never used as a feature.
- Primary sample requires frozen ratings and prior-season traits for all ten
  players. A coverage sensitivity may allow eight of ten only if missingness is
  exposed and results agree. Exact lineup, game, team, opponent, season and
  possession counts remain explicit. Uncertainty is clustered by game and the
  report includes possessions, games, unique lineups and teams in every tail.
- Fixed composition dimensions: offensive sum of prior usage; offensive sum
  of prior 3PA per 75; defensive sum of prior blocks per 75; defensive mean
  height; and defensive count of players at least 81 inches tall. The discrete
  big-count diagnostics explicitly compare no-big and at-least-three-big
  lineups. Continuous low/high tails use possession-weighted development
  10th/90th percentiles, with 5th/95th as sensitivity thresholds.
- Chronology: 2021-22 and earlier define thresholds and calibration
  specification; 2022-23 and 2023-24 extend coefficient fitting without
  changing the family; 2024-25 and 2025-26 are confirmation seasons. Report
  each confirmation season separately.
- Primary diagnostics are (1) tail mean residual from summed frozen NERD,
  relative to the central observed range; (2) out-of-sample RMSE change from
  adding the fixed tail indicators; and (3) tail error dispersion. Because the
  family contains multiple tails, a boundary violation requires the same sign
  in both confirmation seasons, a game-cluster 99% interval excluding zero,
  and meaningful magnitude. A null result supports additivity only through the
  most extreme observed threshold; it does not license extrapolation to five
  point guards or any other unsupported construction.

## 2026-07-19 — Observed-extremes result: a low-usage capacity floor

- The final quality-controlled panel requires every one of the ten players to
  have at least 500 prior-season possessions in addition to a frozen preseason
  covariance-NERD state. It contains 30,396 confirmation lineup-pair spells
  across 1,774 regular-season games in 2024-25 and 2025-26.
- Most observed lineup extremes did not systematically miss summed NERD.
  There was no directionally stable confirmation penalty for high prior usage,
  high shooting, low/high blocks, low/high mean height, no player at least 6-9,
  or at least three such big players. The confirmation sample included 11,392
  possessions with no 6-9 player and 10,179 with at least three, so this is
  meaningful evidence about coach-selected lineups in those observed ranges.
  Low-shooting lineups had only 381 confirmation possessions beyond the frozen
  historical threshold and remain under-supported.
- One asymmetric boundary did emerge. The development 10th-percentile cutoff
  for the five offensive players' summed prior usage was 0.874. Below it, the
  raw confirmation residual was about -2.05 points per 100 relative to the
  central usage range, with the same direction in both seasons; its conservative
  game-cluster 99% interval narrowly included zero. The 5% threshold was -2.59
  and also stable, though less precise.
- The stronger selection sensitivity compares lineups within the same offense
  team-game and removes fourth-quarter high-margin spells. The low-usage gap
  was -2.01 points per 100, almost identical in 2024-25 (-1.99) and 2025-26
  (-1.98), with a game-cluster 99% interval of [-3.57, -0.47]. Thus neither
  team quality, the particular game, nor the simple garbage-time screen explains
  the result. A low-usage penalty estimated only through 2023-24 was -1.44 and
  reduced error inside the low-usage tail in both confirmation seasons, though
  the RMSE gain was only 0.015% because short-spell scoring is extremely noisy.
- Basketball interpretation: excess demonstrated usage appears easy to
  redistribute, but a lineup in which nobody has previously carried enough
  possessions may have a roughly two-point offensive capacity cost beyond the
  sum of the players' ordinary ratings. This is a plausible non-additivity and
  reconnects with the earlier burden-transfer idea. It is not evidence that
  usage itself causally creates value; coaching, role changes and unmeasured
  lineup tasks remain selected.
- The complete fixed family of tail penalties did not improve overall
  confirmation RMSE. Therefore no general context layer or production NERD
  change is warranted. The result supports a narrow lineup-construction warning
  or separately validated low-creation-capacity penalty, not revised individual
  player values. The no-big null supports additivity for no-6-9 lineups coaches
  actually used; it still does not identify an arbitrary five-point-guard lineup
  beyond that support.

### Low-usage historical and playoff extension (specified before fitting)

- Historical regular-season extension uses rolling origin. For every target
  season before 2016-17, the low-usage and central-range thresholds, NERD
  calibration and candidate penalty are estimated only from earlier regular
  seasons. The primary historical summary pools 2009-10 through 2015-16 and
  also reports every season separately; no future fixed threshold is projected
  backward.
- Playoffs are a separate environment. Historical postseason diagnostics use
  earlier regular seasons only. The primary postseason confirmation applies
  the already-frozen modern regular-season definition and through-2023-24
  calibration to the 2024-25 and 2025-26 playoffs without playoff refitting.
- Samples retain the all-ten-player rating/trait coverage rule and 500 prior
  possession minimum. Primary gaps again remove the simple garbage-time screen
  and compare within offense team-game. Report possessions and games because
  extreme postseason lineups may be too rare for a useful test.

### Low-usage historical and playoff extension results

- Seven rolling-origin regular seasons from 2009-10 through 2015-16 contain
  50,350 low-usage possessions across 3,368 games. The pooled within-team-game,
  non-garbage gap is only -0.24 points per 100, with a game-cluster 99%
  interval of [-1.14, 0.76]. Season estimates alternate sign. The capacity
  penalty is therefore not a timeless NBA relationship.
- The relationship becomes visible in the modern period before the untouched
  confirmation. Rolling-origin 2016-17 through 2023-24 tests contain 59,925
  low-usage possessions across 3,906 games and pool to -1.15 points per 100
  (99% interval [-2.05, -0.32]). Seven of eight seasons are negative, although
  individual seasons remain noisy and 2017-18 is strongly positive. Combined
  with the separately frozen 2024-25 and 2025-26 result of about -2.01, the
  evidence favors a modern-era capacity constraint rather than a universal
  lineup law. A schema/model-era explanation remains a falsification risk.
- Playoff samples exist but are thin. Rolling 2016-17 through 2023-24 playoffs
  contain 3,761 low-usage possessions and estimate -1.76, with a wide 99%
  interval [-4.94, 1.20]. The frozen 2024-25 and 2025-26 playoff confirmation
  contains 1,701 possessions and estimates -3.93, interval [-9.44, 1.58]. The
  postseason direction is compatible with the modern regular-season result
  but does not independently confirm it.
- Practical conclusion: the modern regular-season sample is now large enough
  to take the low-usage boundary seriously, but the historical null means it
  should be modeled, if at all, as an era-dependent lineup capacity warning.
  It should not be encoded as a permanent player-value interaction or treated
  as proof that low prior usage causally harms an offense.

### Canonical shooting-luck strength grid (specified before fitting)

- Treat shooting-luck removal as a predictive denoising choice, not a causal
  player-value estimand. Fit separate single-season RAPM evidence on the
  canonical counted-possession design and predict the next season's raw RAPM.
- Vary 3PT, FT and 10-23-foot mid-range residual removal independently over
  {0, .25, .50, .75, 1.0}. The upstream shooter expectations use only prior
  information. Use alpha 150, require 1,000 possessions in both seasons and
  score total, offensive and defensive estimates separately.
- Join legacy FT/mid-range events to reconstructed evidence by game and sorted
  ten-player lineup, not unstable stint index. Exact lineup matches cover
  95.53% of counted possessions; retain exact game component totals by
  spreading the unmatched remainder over that game's counted possessions.

### Canonical shooting-luck strength results

- The validation panel contains 7,909 player-season transitions from 1996-97
  through 2025-26. Raw evidence predicts next-season raw RAPM at .3184 total,
  .3510 offense and .2526 defense. Full 3PT removal alone raises those to
  .3501/.3969/.2747. The 3PT result improves monotonically through every grid
  step and full removal beats no 3PT removal in 24/29 total, 28/29 offensive
  and 26/29 defensive season pairs.
- The best total setting is 3PT=1.0, FT=1.0, mid-range=.50, scoring
  .3587/.4039/.2846. Relative to 3PT-only it improves total/offense/defense by
  .0086/.0070/.0099 and wins 20/29, 24/29 and 22/29 season pairs. A season-block
  bootstrap gives positive 95% intervals on all three improvements.
- FT removal is beneficial but its exact upper setting is unresolved. With
  3PT=1 and mid-range=.50, full FT removal improves total/offense/defense over
  no FT removal by .0038/.0025/.0031. FT=.75 and FT=1.0 are statistically
  indistinguishable; full FT is ahead by only .00006 total and slightly behind
  on offense. Basketball structure therefore supports full FT removal, but the
  data support the broader 75-100% range rather than a precise optimum.
- Mid-range is genuinely partial. With 3PT=FT=1, .50 removal beats zero by
  .0050 total and beats full removal by .0043; both improvements have positive
  season-block bootstrap intervals for total and offense. Defense alone peaks
  at .75 and is nearly flat from .50 to 1.0. A uniform .50 mid-range setting is
  the cleanest total-value choice.
- The formerly intended 1.0/.75/.75 setting remains close (.3578 total), but
  it has no measurable advantage over 1.0/1.0/.50.

### Canonical shooting-luck production adoption

- Production now removes 100% of estimated 3PT and FT make/miss residuals and
  50% of estimated 10-23-foot mid-range residuals. The same default feeds the
  canonical counted-possession evidence, regular-season and playoff on-off,
  public RAPM, NERD's RAPM evidence, daily results, player game/span search,
  playoff series and lineup-combination aggregates.
- The daily luck page, playoff RAPM page and combinations page also expose the
  prespecified comparison that removes 3PT and FT residuals but leaves 2PT
  results untouched. The full model remains the default on all three.
- Canonical evidence contains 1,084,159 counted stints. Recalculation checks
  reproduce both the default and comparison adjusted scores with zero maximum
  algebraic error. The rebuilt analytics database contains 120,745 two-player,
  427,517 three-player, 687,873 four-player, 371,014 five-player and 507,833
  player-combination game rows with both adjustment variants.
