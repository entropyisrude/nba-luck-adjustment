"""Reset player_state to career baselines and prepare for full re-run."""

import json
import pandas as pd
from pathlib import Path


def main():
    # Load career stats cache
    cache_path = Path('data/career_stats_cache.json')
    with open(cache_path) as f:
        career_cache = {int(k): v for k, v in json.load(f).items()}

    # Load current player state to get player names
    player_state = pd.read_csv('data/player_state.csv')

    print(f"Resetting {len(player_state)} players to career baselines...")

    # Reset A_r and M_r to just career stats (no in-season accumulation)
    for idx, row in player_state.iterrows():
        pid = int(row['player_id'])
        if pid in career_cache:
            career = career_cache[pid]
            player_state.loc[idx, 'A_r'] = career.get('fg3a', 0.0)
            player_state.loc[idx, 'M_r'] = career.get('fg3m', 0.0)
        else:
            # Keep as zeros for players not in cache
            player_state.loc[idx, 'A_r'] = 0.0
            player_state.loc[idx, 'M_r'] = 0.0

    # Save reset player state
    player_state.to_csv('data/player_state.csv', index=False)
    print("Player state reset to career baselines")

    # Clear adjusted_games.csv
    adjusted_path = Path('data/adjusted_games.csv')
    if adjusted_path.exists():
        adjusted_path.unlink()
        print("Cleared adjusted_games.csv")

    # Show sample
    print("\nSample after reset:")
    for name in ['LeBron James', 'Stephen Curry', 'Victor Wembanyama']:
        row = player_state[player_state['player_name'] == name]
        if not row.empty:
            r = row.iloc[0]
            print(f"  {name}: A_r={r['A_r']:.1f}, M_r={r['M_r']:.1f}")

    print("\nNow run: python run_daily.py --start 2025-10-22 --end 2026-02-23")


if __name__ == '__main__':
    main()
