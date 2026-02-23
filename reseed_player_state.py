"""Re-seed player_state with career stats baselines."""

import json
import pandas as pd
from pathlib import Path


def main():
    # Load career stats cache (now with real data from ESPN)
    cache_path = Path('data/career_stats_cache.json')
    with open(cache_path) as f:
        career_cache = {int(k): v for k, v in json.load(f).items()}

    # Load current player state
    player_state = pd.read_csv('data/player_state.csv')

    print(f"Loaded {len(player_state)} players")
    print(f"Career cache has {len(career_cache)} entries")

    # Add career stats to current A_r and M_r
    # This gives us career baseline + decayed in-season stats
    updated = 0
    for idx, row in player_state.iterrows():
        pid = int(row['player_id'])
        if pid in career_cache:
            career = career_cache[pid]
            career_3pa = career.get('fg3a', 0)
            career_3pm = career.get('fg3m', 0)

            if career_3pa > 0:
                old_ar = row['A_r']
                old_mr = row['M_r']

                # Add career baseline to in-season weighted stats
                player_state.loc[idx, 'A_r'] = old_ar + career_3pa
                player_state.loc[idx, 'M_r'] = old_mr + career_3pm
                updated += 1

    # Save updated player state
    player_state.to_csv('data/player_state.csv', index=False)
    print(f"Updated {updated} players with career baselines")

    # Show some examples
    print("\nSample players after update:")
    samples = ['LeBron James', 'Stephen Curry', 'James Harden', 'Trae Young', 'Victor Wembanyama']
    for name in samples:
        row = player_state[player_state['player_name'] == name]
        if not row.empty:
            r = row.iloc[0]
            pct = 100 * r['M_r'] / r['A_r'] if r['A_r'] > 0 else 0
            print(f"  {name}: A_r={r['A_r']:.1f}, M_r={r['M_r']:.1f} ({pct:.1f}%)")


if __name__ == '__main__':
    main()
