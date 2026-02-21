from __future__ import annotations

import pandas as pd
from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2

def get_game_ids_for_date(game_date_mmddyyyy: str) -> list[str]:
    """Return list of NBA game_ids for a given date (MM/DD/YYYY)."""
    sb = scoreboardv2.ScoreboardV2(game_date=game_date_mmddyyyy)
    games = sb.game_header.get_data_frame()
    if games.empty:
        return []
    return [str(x) for x in games["GAME_ID"].tolist()]

def get_boxscore_player_df(game_id: str) -> pd.DataFrame:
    """Player boxscore from BoxScoreTraditionalV2."""
    bs = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
    df = bs.player_stats.get_data_frame()
    # keep only needed columns, and normalize
    keep = ["GAME_ID", "TEAM_ID", "PLAYER_ID", "PLAYER_NAME", "FG3M", "FG3A"]
    df = df[keep].copy()
    # Ensure numeric
    df["FG3M"] = pd.to_numeric(df["FG3M"], errors="coerce").fillna(0.0)
    df["FG3A"] = pd.to_numeric(df["FG3A"], errors="coerce").fillna(0.0)
    return df

def get_boxscore_team_df(game_id: str) -> pd.DataFrame:
    """Team boxscore from BoxScoreTraditionalV2."""
    bs = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
    df = bs.team_stats.get_data_frame()
    keep = ["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "MATCHUP", "PTS", "FG3M", "FG3A"]
    df = df[keep].copy()
    df["PTS"] = pd.to_numeric(df["PTS"], errors="coerce").fillna(0.0)
    df["FG3M"] = pd.to_numeric(df["FG3M"], errors="coerce").fillna(0.0)
    df["FG3A"] = pd.to_numeric(df["FG3A"], errors="coerce").fillna(0.0)
    return df
