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
