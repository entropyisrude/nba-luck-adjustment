# Project: Contextual Causal NBA Player Valuation

## Objective

Develop an NBA player-valuation framework that estimates how player value changes across roles, lineups, opponents, and availability shocks. The first pilot studies how offensive responsibility is redistributed when a primary creator is absent.

Read `docs/causal_metric_research_brief.md`, `docs/data_inventory.md`, and `docs/estimands.md` before proposing models or changing analysis code.

## Research principles

- Do not treat RAPM, EPM, DARKO, DPM, NERD, or box-score priors as causal ground truth.
- Define the estimand and intervention before fitting a model.
- Distinguish descriptive, predictive, quasi-causal, and causally identified claims.
- Preserve multidimensional role and load variables before compressing them into a scalar.
- Freeze every feature and player estimate at the prediction date; prevent future leakage.
- Prefer chronological and event-level holdouts over random row splits.
- Use natural experiments and local perturbations only where their assumptions are auditable.
- Treat plus-minus as an outcome or calibration target, not the complete definition of value.
- Model complementarity through player capabilities when possible; avoid unconstrained pair effects.
- Report overlap, uncertainty, sensitivity, and negative-control diagnostics.

## Data and workflow safety

- Never modify raw files in `data/` in place.
- Write new derived datasets under `derived/contextual_causal/`.
- Write reports under `docs/` or `outputs/contextual_causal/`.
- Keep player IDs, team IDs, game IDs, dates, and season definitions explicit at every join.
- Before implementation, state treatment, outcome, confounders, mediators, estimand, and validation design.
- Start with the smallest auditable prototype and log results and changed assumptions in `docs/research_log.md`.

