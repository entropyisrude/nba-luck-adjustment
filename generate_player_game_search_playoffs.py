from __future__ import annotations

import os

os.environ.setdefault("NBA_ANALYTICS_DB_PATH", "/mnt/c/users/dave/Downloads/nba-onoff-publish/data/nba_analytics_playoffs.duckdb")
os.environ.setdefault("PLAYER_GAME_SEARCH_OUTPUT_DATA_PATH", "/mnt/c/users/dave/Downloads/nba-onoff-publish/data/player_game_search_playoffs.html")
os.environ.setdefault("PLAYER_GAME_SEARCH_OUTPUT_SITE_PATH", "/mnt/c/users/dave/Downloads/nba-onoff-publish/game-search-playoffs.html")
os.environ.setdefault("PLAYER_GAME_SEARCH_CHUNK_DIR", "/mnt/c/users/dave/Downloads/nba-onoff-publish/data/player_game_playoff_chunks")
os.environ.setdefault("PLAYER_GAME_SEARCH_PAGE_TITLE", "Player Game Search: Playoffs")
os.environ.setdefault("PLAYER_SPAN_SEARCH_HREF", "player-span-search-playoffs.html")
os.environ.setdefault("PLAYER_GAME_SEARCH_SOURCE_LABEL", "data/nba_analytics_playoffs.duckdb")

from generate_player_game_search_report import generate_player_game_search_report


if __name__ == "__main__":
    generate_player_game_search_report()
