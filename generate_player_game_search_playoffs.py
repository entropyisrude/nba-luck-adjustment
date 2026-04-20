from __future__ import annotations

import os

ROOT = os.environ.get("NBA_ONOFF_ROOT", os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("NBA_ANALYTICS_DB_PATH", os.path.join(ROOT, "data", "nba_analytics_playoffs.duckdb"))
os.environ.setdefault("PLAYER_GAME_SEARCH_OUTPUT_DATA_PATH", os.path.join(ROOT, "data", "player_game_search_playoffs.html"))
os.environ.setdefault("PLAYER_GAME_SEARCH_OUTPUT_SITE_PATH", os.path.join(ROOT, "game-search-playoffs.html"))
os.environ.setdefault("PLAYER_GAME_SEARCH_CHUNK_DIR", os.path.join(ROOT, "data", "player_game_playoff_chunks"))
os.environ.setdefault("PLAYER_GAME_SEARCH_PAGE_TITLE", "Player Game Search: Playoffs")
os.environ.setdefault("PLAYER_SPAN_SEARCH_HREF", "player-span-search-playoffs.html")
os.environ.setdefault("PLAYER_GAME_SEARCH_SOURCE_LABEL", "data/nba_analytics_playoffs.duckdb")

from generate_player_game_search_report import generate_player_game_search_report


if __name__ == "__main__":
    generate_player_game_search_report()
