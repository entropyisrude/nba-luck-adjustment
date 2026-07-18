# NERD counted-possession rebuild — 2026-07-17

## Decision

Keep this as an experimental candidate. The corrected possession target is
materially better and produces more faithful offense/defense components, but
the resulting all-in-one total does not yet beat the current production NERD
out of sample. Do not swap the site metric yet.

## Data repairs

- Possession team attribution now falls back from null `teamId` to populated
  `playerteamId`. This fixes 80 recent games that had real possessions but
  were previously scored 0-0.
- Possessions outside a stint's lineup window are no longer attached to the
  preceding stint.
- A game must pass possession-attachment, official-score, and home/away
  possession-balance gates before it enters the counted target.
- 23,900 of 37,984 games pass all gates (62.9%). Coverage is 92.9% in 2025-26
  but substantially lower in early seasons, so a full historical replacement
  would currently throw away too much evidence.

## Target validation

- Rotation-player O/D correlation changed from roughly -0.18 to -0.24 in the
  old target to 0.00 to +0.13 in the corrected target. The mechanical
  offense/defense mirror is gone.
- Holding features and chronological evaluation fixed, a v1 box model trained
  on the corrected target improves total correlation against independent
  counted-possession evidence:
  - same season: 0.477 -> 0.522 (paired difference +0.045; row-bootstrap 95%
    interval +0.035 to +0.057)
  - next season: 0.392 -> 0.423 (+0.031; 95% interval +0.019 to +0.043)
- Thirty-replicate game-block bootstrap for 2024-25 finds bootstrap/analytic
  SE ratios of 0.880 offense, 0.890 defense, and 0.886 total. Raw analytic SEs
  are about 12% conservative. Despite that simple scale correction, inverse-SE
  fitting performs much worse than possession weighting, so it is not used.

## Atomic feature decision

On a rolling-origin test (coefficients use only prior target seasons; ridge
strength selected through 2018; 2019-25 untouched):

- v1 possession-weighted prior: same-season total 0.522; next-season 0.423
- atomic possession-weighted prior: same-season total 0.507; next-season 0.416
- atomic defense alone is modestly better (0.503 vs 0.491), but atomic offense
  is worse (0.558 vs 0.574).
- A v1-offense/atomic-defense hybrid gains only +0.003 same-season and +0.001
  next-season. Both confidence intervals include zero.

Conclusion: retain the existing v1 feature set for the candidate. The atomic
rewrite is useful as an audit and perhaps for later defensive submodels, but
it is not supported as the full box prior.

## Prior-informed candidate

The candidate uses the corrected counted-possession likelihood, rolling v1
box centers, and a centered ridge solve. Alpha 4000 was selected only on
pre-2019 next-season evidence.

On 2019+ future single-season evidence (2,136 common player-seasons):

- total: production 0.4701, candidate 0.4703; difference is not significant
  (95% interval -0.0141 to +0.0143)
- offense: 0.4543 -> 0.5006; difference +0.0463 (95% interval +0.0255 to
  +0.0674)
- defense: 0.4466 -> 0.4750; difference +0.0284 (95% interval +0.0049 to
  +0.0525)

The counted build meaningfully improves component attribution but provides no
demonstrable gain in total player value yet.

## Next promotion gate

Build a mixed-coverage likelihood: counted possessions for trusted games and
the legacy stint estimate for rejected games, with an explicit source flag and
separate residual variance. Then repeat the locked chronological test and a
held-out game-margin test. Promote only if total prediction improves without
giving back the O/D component gains.
