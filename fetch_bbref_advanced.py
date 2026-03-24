
import pandas as pd
from pathlib import Path

# Authoritative BBRef Abbreviation Mapping
TEAM_MAP = {
    'Atlanta Hawks': 'ATL', 'Boston Celtics': 'BOS', 'Brooklyn Nets': 'BRK',
    'Charlotte Hornets': 'CHO', 'Chicago Bulls': 'CHI', 'Cleveland Cavaliers': 'CLE',
    'Dallas Mavericks': 'DAL', 'Denver Nuggets': 'DEN', 'Detroit Pistons': 'DET',
    'Golden State Warriors': 'GSW', 'Houston Rockets': 'HOU', 'Indiana Pacers': 'IND',
    'Los Angeles Clippers': 'LAC', 'Los Angeles Lakers': 'LAL', 'Memphis Grizzlies': 'MEM',
    'Miami Heat': 'MIA', 'Milwaukee Bucks': 'MIL', 'Minnesota Timberwolves': 'MIN',
    'New Orleans Pelicans': 'NOP', 'New York Knicks': 'NYK', 'Oklahoma City Thunder': 'OKC',
    'Orlando Magic': 'ORL', 'Philadelphia 76ers': 'PHI', 'Phoenix Suns': 'PHO',
    'Portland Trail Blazers': 'POR', 'Sacramento Kings': 'SAC', 'San Antonio Spurs': 'SAS',
    'Toronto Raptors': 'TOR', 'Utah Jazz': 'UTA', 'Washington Wizards': 'WAS'
}

def fetch_bbref_data():
    print("Fetching 2025-26 Stats and Standings from Basketball Reference...")
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # 1. Fetch Advanced Player Stats
    adv_url = "https://www.basketball-reference.com/leagues/NBA_2026_advanced.html"
    try:
        tables = pd.read_html(adv_url)
        df = tables[0]
        df = df[df['Player'] != 'Player'].copy()
        
        # We need G and MP for the 65-game rule counts
        cols = ['Player', 'Team', 'G', 'MP', 'VORP', 'BPM', 'WS', 'PER', 'TS%']
        df = df[cols].copy()
        for col in ['G', 'MP', 'VORP', 'BPM', 'WS', 'PER', 'TS%']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        df['Player'] = df['Player'].str.replace(r'[*]+$', '', regex=True).str.strip()
        df = df.rename(columns={'Player': 'player_name'})
        
        # Filter out multi-team rows (TOT) to avoid double counting, 
        # BUT we need to handle that BBRef's advanced table often has a 'TOT' row first.
        # Let's keep all for now and handle 'TOT' in the builder.
        
        df.to_csv(data_dir / "bbref_advanced_2526.csv", index=False)
        print("Saved Advanced Player Stats.")
    except Exception as e:
        print(f"Error fetching player stats: {e}")

    # 2. Fetch Official Team Standings
    std_url = "https://www.basketball-reference.com/leagues/NBA_2026.html"
    try:
        tables = pd.read_html(std_url)
        east = tables[0].rename(columns={'Eastern Conference': 'Team'})
        west = tables[1].rename(columns={'Western Conference': 'Team'})
        standings = pd.concat([east, west])
        
        standings['Team'] = standings['Team'].str.replace(r'\s*\(\d+\)$', '', regex=True).str.strip()
        standings['W'] = pd.to_numeric(standings['W'])
        standings['L'] = pd.to_numeric(standings['L'])
        standings['team_gp'] = standings['W'] + standings['L']
        standings['team_abbr'] = standings['Team'].map(TEAM_MAP)
        
        output_df = standings[['team_abbr', 'W', 'L', 'team_gp']].dropna()
        output_df.to_csv(data_dir / "bbref_team_gp_2526.csv", index=False)
        print(f"Saved Authoritative Team Standings ({len(output_df)} teams).")
    except Exception as e:
        print(f"Error fetching standings: {e}")

if __name__ == "__main__":
    fetch_bbref_data()
