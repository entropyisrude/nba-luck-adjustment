# Canonical possession/lineup rebuild: progress report

Date: 2026-07-18

## What the 62.9% result meant

It was an end-to-end integrity result, not a raw-play-by-play accuracy rate.
Of 35,522 regular-season games, 22,457 (63.22%) had an existing reconstructed
stint record that simultaneously matched independent official evidence for:

- final team score;
- full game time with continuous five-on-five lineups;
- player minutes (within 75 seconds, because official minutes are rounded);
- every player's official game plus-minus (within 0.5 points).

The old artifacts passed score in 74.28% of games, time in 73.95%, minutes in
76.76%, and plus-minus in 64.37%. These are intersections of different data
layers. They do **not** imply that 37% of the underlying event feed is corrupt.

## Concrete root causes found

1. Historical substitution rows frequently identify the outgoing player by ID
   but put the incoming player only in text. Example: `SUB: Ceballos FOR Owens`.
2. Existing historical stint artifacts are stale relative to improvements
   already present in the current parser.
3. Many recent failures are exact-clock boundary defects. The five players are
   right, but a rounded stint boundary no longer coincides with the substitution
   event, which can assign a same-clock score to the wrong unit.
4. Score calibration was sometimes applied to partial reconstructions. It can
   reconcile small scoring-boundary discrepancies; it cannot invent missing
   lineup time.
5. The analytics database selected sources by `(game_id, stint_index)`. Since
   different reconstructions have different boundaries, this could splice two
   incompatible versions of one game. Source selection is now whole-game.

## Repair hierarchy implemented

1. Deterministic replay of the raw PBP with the current parser.
2. Official-roster resolution of unique name-only substitutions.
3. Exact event-time boundary projection while preserving the replayed lineups.
4. A whole-game mixed-integer lineup solver only for the ambiguous remainder.
5. A complete official GameRotation record takes precedence over conflicting
   PBP actor/substitution evidence only when its durations and `PT_DIFF` first
   reconcile independently to the official box, and calibrated stint scoring
   then reproduces the team totals and player plus-minus.
6. Strict quarantine unless the result passes score, time, minutes, plus-minus,
   recorded transition, action-presence, and unsupported-change checks.

The solver is not allowed to enter RAPM merely because it matches aggregate box
totals. A solution that manufactures a locally unsupported substitution is
rejected even if its score, minutes, and plus-minus are perfect.

## Pilot results

| Cohort | Strictly accepted | Important interpretation |
|---|---:|---|
| Difficult 2000 example (`0020000005`) | 1/1 | Recovered 95-104, exact PM, minutes within 19 seconds, and the actual two-player substitution |
| First ten failed 2000 games | 8/10 | Two aggregate-perfect solutions were rejected for unsupported player changes |
| First ten failed 1996 games | 8/10 | Same strict recovery rate in the oldest season currently in scope |
| First 50 failed 2025 games | 48/50 | 6 direct replay, 34 exact-boundary projection, 8 constrained repair; 2 quarantined |
| Selected difficult 2008 game (`0020800035`) | rejected | Aggregate totals can be matched only by violating substantial local evidence; correctly quarantined |

In the 50-game 2025 sample, 40 of 50 were therefore repaired without inferring
any lineup change. One rejected game had no usable replay seed and remained far
from official minutes/plus-minus after inference; the other required extensive
locally unsupported changes. Neither belongs in RAPM.

These are engineering validation samples, not an estimate of the final
league-wide recovery rate. The key result is that the pipeline now distinguishes
recoverable representation defects from underidentified games instead of
silently treating both as valid possessions.

## Full rebuild results

The full repair and validation hierarchy now selects 35,185 of 35,522 games
(99.05%). The versioned candidate retains zero games that fail its final
structural/score audit. The whole-game source manifest is:

| Source | Games |
|---|---:|
| Original records that survived both audits | 22,187 |
| Full deterministic/constrained rebuild | 10,566 |
| Period-aware boundary retry | 1,607 |
| Rebuilds of defects discovered in the original Grade-A set | 262 |
| Box-validated authoritative official GameRotation | 186 |
| Pinned official GameRotation archive | 123 |
| Strict inferred one-player partial substitutions | 106 |
| Historical rounded-minute box cross-check | 70 |
| Independent exact-minute box cross-check | 56 |
| Strict inferred multi-player one-sided substitutions | 19 |
| Isolated retry and official GameRotation | 3 |

The boundary-aware retry recovered 1,607 games. An independent final audit then
found 270 structural defects in games previously called Grade A; 262 rebuilt
cleanly and the remaining eight were removed. This is why the retained original
count is 22,187 rather than 22,457.

The exact-minute cross-check exposed a concrete upstream box problem. In many
2010-24 games the database minute value was 80-110 seconds away from the
independent `MM:SS` source for at least one player, while the reconstructed
lineup was within about half a second and still matched exact official
plus-minus. The rounded historical archive provided the same kind of evidence
outside that exact-source cohort, but was used only when every other gate had
already passed.

The archive authority audit found 210 complete rotations that independently
matched both official player minutes and the endpoint's own `PT_DIFF`, despite
failing a PBP-derived local-evidence gate. Exact scoring calibration recovered
186 of them. Twenty-three could not reproduce every player's official
plus-minus within 0.5 points, and one had a major team-score inconsistency, so
those 24 remain quarantined. This implements a source hierarchy rather than
blindly accepting every GameRotation record.

## Remaining work before production RAPM

- 337 games (0.95%) remain quarantined. Missingness is not fully random: one
  season and two teams still exceed 3% unresolved.
- The current missingness audit finds 46 players with at least 500 database
  minutes and more than 10% of those minutes in unresolved games. RAPM/NERD
  should therefore not be silently regenerated from a complete-case sample.
- The largest remaining single-gate cohorts require genuinely new evidence:
  explicit transition contradictions, unsupported changes that cannot be
  completed from a one-sided substitution batch, or plus-minus disagreement.
- The live NBA GameRotation endpoint failed to return even for a single canary,
  but a pinned GitHub archive of direct endpoint downloads covered 566
  quarantined games through 2022-23. Of 525 structurally complete candidates,
  123 passed every downstream gate directly and another 186 passed the
  box-validated authority rule above. Seasons 2023-24 onward are outside this
  archive.
- Once that source is available—or an explicit missing-data treatment is
  selected—regenerate possessions, RAPM targets, and NERD and compare them
  against the frozen current model.

No production stint, possession, RAPM, NERD, or site file has been replaced by
this work yet.
