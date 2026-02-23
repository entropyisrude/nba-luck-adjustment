"""Fetch career 3P stats from ESPN for all players."""

import json
import time
import requests
import pandas as pd
from pathlib import Path

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

SEARCH_URL = 'https://site.api.espn.com/apis/common/v3/search'
STATS_URL = 'https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{espn_id}/stats'


def search_player(name: str) -> dict | None:
    """Search ESPN for a player by name, return ESPN ID and name if found."""
    params = {
        'query': name,
        'limit': 5,
        'type': 'player',
        'sport': 'basketball',
        'league': 'nba'
    }
    try:
        resp = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=10)
        if resp.ok:
            data = resp.json()
            items = data.get('items', [])
            if items:
                # Return best match
                return {
                    'espn_id': items[0]['id'],
                    'espn_name': items[0]['displayName']
                }
    except Exception as e:
        print(f"  Search error for {name}: {e}")
    return None


def get_career_3p_stats(espn_id: str) -> dict:
    """Fetch career 3P stats from ESPN."""
    url = STATS_URL.format(espn_id=espn_id)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.ok:
            data = resp.json()
            for cat in data.get('categories', []):
                if cat.get('name') == 'totals':
                    labels = cat.get('labels', [])
                    if '3PT' not in labels:
                        continue
                    pt3_idx = labels.index('3PT')

                    total_3pm = 0
                    total_3pa = 0
                    for season in cat.get('statistics', []):
                        stats = season.get('stats', [])
                        if len(stats) > pt3_idx:
                            pt3_str = stats[pt3_idx]
                            if '-' in str(pt3_str):
                                made, att = pt3_str.split('-')
                                total_3pm += int(made.replace(',', ''))
                                total_3pa += int(att.replace(',', ''))

                    return {'fg3m': float(total_3pm), 'fg3a': float(total_3pa)}
    except Exception as e:
        print(f"  Stats error for ESPN ID {espn_id}: {e}")
    return {'fg3m': 0.0, 'fg3a': 0.0}


def main():
    # Load current player state
    player_state = pd.read_csv('data/player_state.csv')
    print(f"Found {len(player_state)} players in player_state.csv")

    # Load existing cache
    cache_path = Path('data/career_stats_cache.json')
    if cache_path.exists():
        with open(cache_path) as f:
            cache = {int(k): v for k, v in json.load(f).items()}
    else:
        cache = {}

    # Track ESPN ID mappings
    espn_map_path = Path('data/espn_player_map.json')
    if espn_map_path.exists():
        with open(espn_map_path) as f:
            espn_map = {int(k): v for k, v in json.load(f).items()}
    else:
        espn_map = {}

    updated = 0
    failed = []

    for idx, row in player_state.iterrows():
        nba_id = int(row['player_id'])
        name = row['player_name']

        # Skip if we already have good data
        existing = cache.get(nba_id, {})
        if existing.get('fg3a', 0) > 0:
            safe_name = name.encode('ascii', 'replace').decode('ascii')
            print(f"[{idx+1}/{len(player_state)}] {safe_name}: already have data ({existing['fg3a']:.0f} 3PA)")
            continue

        # Handle Unicode names safely
        safe_name = name.encode('ascii', 'replace').decode('ascii')
        print(f"[{idx+1}/{len(player_state)}] {safe_name}...", end=' ')

        # Check if we have ESPN mapping
        if nba_id in espn_map:
            espn_id = espn_map[nba_id]['espn_id']
        else:
            # Search for player
            result = search_player(name)
            if result:
                espn_map[nba_id] = result
                espn_id = result['espn_id']
                # Save mapping periodically
                with open(espn_map_path, 'w') as f:
                    json.dump(espn_map, f)
            else:
                print("not found on ESPN")
                failed.append(name)
                continue

        # Fetch stats
        stats = get_career_3p_stats(espn_id)
        if stats['fg3a'] > 0:
            cache[nba_id] = stats
            updated += 1
            print(f"{stats['fg3m']:.0f}/{stats['fg3a']:.0f} ({100*stats['fg3m']/stats['fg3a']:.1f}%)")
        else:
            print("no 3P data")
            failed.append(name)

        # Rate limiting
        time.sleep(0.3)

        # Save cache periodically
        if updated % 10 == 0:
            with open(cache_path, 'w') as f:
                json.dump(cache, f)

    # Final save
    with open(cache_path, 'w') as f:
        json.dump(cache, f)

    print(f"\nDone! Updated {updated} players")
    print(f"Failed to fetch: {len(failed)}")
    if failed:
        print("Failed players:", failed[:20])


if __name__ == '__main__':
    main()
