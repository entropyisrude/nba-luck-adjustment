
import pandas as pd
import time
from pathlib import Path

def fetch_bbref_advanced():
    print("Fetching 2025-26 Advanced Stats from Basketball Reference...")
    url = "https://www.basketball-reference.com/leagues/NBA_2026_advanced.html"
    
    try:
        # read_html returns a list of dataframes
        tables = pd.read_html(url)
        df = tables[0]
        
        # Clean up repeated headers
        df = df[df['Player'] != 'Player'].copy()
        
        # Select relevant columns
        cols = ['Player', 'Team', 'VORP', 'BPM', 'WS', 'PER', 'TS%']
        df = df[cols].copy()
        
        # Convert numeric columns
        numeric_cols = ['VORP', 'BPM', 'WS', 'PER', 'TS%']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # Clean player names (remove suffixes like * or \xa0)
        df['Player'] = df['Player'].str.replace(r'[*]+$', '', regex=True).str.strip()
        df = df.rename(columns={'Player': 'player_name'})
        
        # Save to repo data folder
        output_path = Path("tmpnba-onoff-publish-push2/data/bbref_advanced_2526.csv")
        df.to_csv(output_path, index=False)
        print(f"Successfully saved BBRef Advanced data to {output_path}")
        return df
        
    except Exception as e:
        print(f"Error fetching BBRef data: {e}")
        return None

if __name__ == "__main__":
    fetch_bbref_advanced()
