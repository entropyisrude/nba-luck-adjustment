# Oracle Recovery Assessment

## What the evidence says

- The current live regular-season historical source files are exactly the March 17 `plain` rebuild outputs:
  - `data/adjusted_onoff_historical_pbp.csv`
  - `data/stints_historical_pbp.csv`
  - `data/possessions_historical_pbp.csv`
- These files hash-match:
  - `data/adjusted_onoff_historical_pbp_20260317_plain.csv`
  - `data/stints_historical_pbp_20260317_plain.csv`
  - `data/possessions_historical_pbp_20260317_plain.csv`
- The likely prior oracle candidates that survived on disk are:
  - `data/adjusted_onoff_historical_pbp.csv.pre_20260317_backup`
  - `data/stints_historical_pbp.csv.pre_20260317_backup`
  - `data/possessions_historical_pbp.csv.pre_20260317_backup`
  - and the earlier:
    - `data/adjusted_onoff_historical_pbp.pre_backfill.csv`
    - `data/stints_historical_pbp.pre_backfill.csv`
    - `data/possessions_historical_pbp.pre_backfill.csv`

## Likely regression event

- `scripts/run_full_rebuild_20260317_fixes.sh` rebuilt the regular-season historical source in two variants:
  - `plain`
  - `vwd`
- The `plain` run used:
  - `--disable-lineup-overrides`
  - `--recompute-existing`
  - `--game-dates-path data/stints_historical_pbp_v2.csv`
- The current live regular-season files now equal that `plain` output.
- Best inference:
  - the live canonical regular-season oracle was displaced by the March 17 `plain` rebuild
  - downstream DB builds continued reading the default filenames, so the displaced source silently became the active base

## Candidate comparison summary

The following pure builds were compared with rebuilt overlays disabled:

- `currentpure`
  - current live files
- `prebackfill`
  - copied from `*.pre_backfill.csv`
- `pre0317backup`
  - copied from `*.csv.pre_20260317_backup`

Generated artifacts:

- `data/nba_analytics_currentpure.duckdb`
- `data/nba_analytics_prebackfill.duckdb`
- `data/nba_analytics_pre0317backup.duckdb`
- `data/player_season_onoff_reference_currentpure.csv`
- `data/player_season_onoff_reference_prebackfill.csv`
- `data/player_season_onoff_reference_pre0317backup.csv`
- `data/teamseason_repair_queue_currentpure.csv`
- `data/teamseason_repair_queue_prebackfill.csv`
- `data/teamseason_repair_queue_pre0317backup.csv`
- `data/oracle_candidate_comparison.txt`

### Result

- `pre0317backup` is the stronger recovery candidate than `prebackfill`.
- Both pre-March candidates are materially better than `currentpure` on the modern "historical overlay rebuild now" surface.
- `pre0317backup` still is not clean enough to call the final oracle.
- Therefore:
  - the project is not hopeless
  - but the current live source is not trustworthy
  - and the long-term answer is not a multi-source chooser

## Single long-term solution

The only acceptable long-term architecture is:

1. One raw historical event source.
2. One deterministic reconstruction pipeline.
3. One canonical historical artifact.
4. One validation suite.
5. Promotion only if the candidate beats the incumbent oracle.

## Practical recovery plan

### Phase 1: Restore a sane base

- Treat `pre0317backup` as the best known regular-season oracle candidate.
- Do not use the current live regular-season files as canonical.
- Do not use `legacybest`, `hybrid`, `v2`, `plain`, or `vwd*` as production sources.
- Keep them only as forensic references.

### Phase 2: Recover the deterministic rebuild path

- Recover the exact regular-season rebuild path that existed before the March 17 overwrite.
- The March 17 `plain` path is suspect because it:
  - disabled lineup overrides
  - recomputed existing rows wholesale
  - appears to have replaced the curated live files
- The right target is a single rebuild flow from raw historical PBP plus explicit override tables, not a file selector.

### Phase 3: Rebuild one new candidate from raw source

- Starting from the recovered pre-March logic:
  - rebuild the full regular season once
  - write to a new tagged candidate
  - never overwrite the live filenames during the experiment

### Phase 4: Validate before promotion

- Minimum gate:
  - team-season possession sanity
  - key player game counts
  - full-season off-court On-Off reference checks
  - spot checks for known failure cases
  - no silent regression on previously-audited seasons

### Phase 5: Promote and freeze

- After a candidate passes:
  - promote one canonical historical artifact
  - make all downstream builds require an explicit source tag or canonical alias
  - retire alternate historical artifacts from active use

## Is this hopeless?

Not yet.

Reasons:

- The likely regression point is identifiable.
- The likely pre-regression source candidate still exists on disk.
- The project failure appears to be loss of canonical control, not proof that a single-path oracle is impossible.

The project becomes hopeless only if:

- the pre-March deterministic rebuild path cannot be recovered,
- and no full rebuild from raw PBP can beat the surviving `pre0317backup` candidate,
- and the only way to improve accuracy remains multi-source cherry-picking.

If that happens, the project should be abandoned rather than maintained as a patchwork oracle.
