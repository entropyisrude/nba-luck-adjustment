# NBA 3PT "Expected Shooting" Adjuster (v1)

This project ingests NBA box scores and produces a game-level CSV with **adjusted scores** under a counterfactual where each player makes threes at their **recency-weighted expected rate**, using an **attempt-based exponential half-life** (default: 2000 3PA).

It also applies a simple league-average **offensive rebound (ORB) correction** in line with the counterfactual change in makes vs misses.

## What it outputs
- `data/adjusted_games.csv` — shareable, one row per game (actual vs adjusted)
- `data/player_state.csv` — incremental player skill state `(A_r, M_r)`
- `data/adjusted_onoff.csv` — per-player, per-game on/off with 3PT luck adjustment
- `data/player_onoff_history.csv` — per-player season aggregate history from `adjusted_onoff.csv`
- `data/player_daily_boxscore.csv` — per-player, per-game actual vs adjusted plus-minus and on-off

## Install
Create a virtualenv and install requirements:

```bash
python -m venv .venv
source .venv/bin/activate   # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
```

## Run
Run for a single date (ET) or a date range:

```bash
python run_daily.py --start 2026-02-21 --end 2026-02-21
# or
python run_daily.py --start 2026-02-01 --end 2026-02-21
```

The script appends to `data/adjusted_games.csv` (deduped by `game_id`) and updates `data/player_state.csv`.

Compute per-player on/off (luck-adjusted):

```bash
python run_onoff.py --start 2026-02-21 --end 2026-02-21
```

This appends to `data/adjusted_onoff.csv` (deduped by `game_id`, `player_id`), updates
`data/player_state.csv`, and rebuilds:
- `data/player_onoff_history.csv`
- `data/player_daily_boxscore.csv`

Backfill a full season window (example 2025-26):

```bash
python run_onoff.py --start 2025-10-21 --end 2026-04-15 --history-season-start 2025-10-01 --history-season-end 2026-06-30
```

Daily updater (defaults to yesterday):

```bash
python run_onoff_daily.py
# or specific date
python run_onoff_daily.py --date 2026-02-24
```

Generate the standalone on/off website page:

```bash
python generate_onoff_report.py
```

This writes both `data/onoff_report.html` and `onoff.html`.

Generate daily game-by-game on/off boxscores page:

```bash
python generate_onoff_daily_boxscore_report.py
```

This writes both `data/onoff_daily_boxscores.html` and `onoff-daily.html`.

Generate/update RAPM page (latest season + rolling last 3 years):

```bash
python generate_rapm_report.py
```

This updates `data/rapm_all.json`, `data/player_info_map.json`, and `rapm.html`.

Multi-season backfill helper (monthly chunks + final rebuild):

```bash
python backfill_multiseason.py \
  --start 2023-10-24 \
  --end 2025-10-20 \
  --final-history-start 2023-10-01 \
  --final-history-end 2026-06-30
```

Validate on/off accuracy vs official boxscore plus-minus/minutes:

```bash
python validate_onoff_accuracy.py --start 2026-02-24 --end 2026-02-24
```

Writes `data/onoff_validation.csv` and prints summary error stats.

## Method summary

### Player expected 3P%
Maintain per-player recency state:
- `A_r`: recency-weighted 3PA
- `M_r`: recency-weighted 3PM

Attempt-based decay factor:
- `gamma = 0.5 ** (1 / half_life_3pa)`

Pre-game expected 3P% (with shrinkage):
- `p_hat = (M_r + kappa*mu) / (A_r + kappa)`

Game expected makes:
- `3PM_exp_team = sum_i(3PA_i * p_hat_i)`

### ORB correction (simple league-average)
Let `delta_3m = 3PM_exp_team - 3PM_actual_team`.

The ORB correction is:
- `orb_corr_pts = -(orb_rate * ppp) * delta_3m`

Total adjustment:
- `delta_pts_total = 3*delta_3m + orb_corr_pts`

### Update player state (after computing expectation)
For each player with `a=3PA`, `m=3PM` in the game:
- `A_r <- (gamma**a) * A_r + a`
- `M_r <- (gamma**a) * M_r + m`

## Notes
- Data source: `nba_api` endpoints (ScoreboardV2 + BoxScoreTraditionalV2).
- This is **box-score-only**; possessions, rebounding cascades, and lineup effects are not modeled beyond the ORB haircut.
