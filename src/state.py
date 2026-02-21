from __future__ import annotations

from pathlib import Path
import pandas as pd

STATE_COLS = ["player_id", "player_name", "A_r", "M_r"]

def load_player_state(path: Path) -> pd.DataFrame:
    if path.exists():
        df = pd.read_csv(path)
        # enforce cols
        for c in STATE_COLS:
            if c not in df.columns:
                df[c] = None
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
        df["A_r"] = pd.to_numeric(df["A_r"], errors="coerce").fillna(0.0)
        df["M_r"] = pd.to_numeric(df["M_r"], errors="coerce").fillna(0.0)
        df["player_name"] = df["player_name"].fillna("")
        return df[STATE_COLS].copy()
    return pd.DataFrame(columns=STATE_COLS)

def save_player_state(df: pd.DataFrame, path: Path) -> None:
    df = df.copy()
    df = df[STATE_COLS].sort_values("player_id")
    df.to_csv(path, index=False)

def ensure_players_exist(player_state: pd.DataFrame, player_df: pd.DataFrame) -> pd.DataFrame:
    """Ensure any PLAYER_ID in player_df exists in player_state."""
    st = player_state.copy()
    existing_ids = set(st["player_id"].dropna().astype(int).tolist())
    new_players = []
    for _, r in player_df.iterrows():
        pid = int(r["PLAYER_ID"])
        if pid not in existing_ids:
            new_players.append({"player_id": pid, "player_name": r["PLAYER_NAME"], "A_r": 0.0, "M_r": 0.0})
            existing_ids.add(pid)
    if new_players:
        st = pd.concat([st, pd.DataFrame(new_players)], ignore_index=True)
    return st
