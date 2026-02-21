# NBA 3PT "Expected Shooting" Adjuster (v1)

This project ingests NBA box scores and produces a game-level CSV with **adjusted scores** under a counterfactual where each player makes threes at their **recency-weighted expected rate**, using an **attempt-based exponential half-life** (default: 2000 3PA).

It also applies a simple league-average **offensive rebound (ORB) correction** in line with the counterfactual change in makes vs misses.

## What it outputs
- `data/adjusted_games.csv` — shareable, one row per game (actual vs adjusted)
- `data/player_state.csv` — incremental player skill state `(A_r, M_r)`

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
