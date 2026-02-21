from __future__ import annotations

import math
import pandas as pd

def compute_team_expected_3pm(
    player_df: pd.DataFrame,
    player_state: pd.DataFrame,
    mu: float,
    kappa: float,
) -> dict[int, float]:
    """Compute expected made threes by team from player 3PA * p_hat (pre-game state)."""
    # Map player_id -> (A_r, M_r)
    st = player_state.set_index("player_id")[["A_r", "M_r"]]
    exp_by_team: dict[int, float] = {}
    for team_id, grp in player_df.groupby("TEAM_ID"):
        team_id = int(team_id)
        exp = 0.0
        for _, r in grp.iterrows():
            a = float(r["FG3A"])
            if a <= 0:
                continue
            pid = int(r["PLAYER_ID"])
            if pid in st.index:
                A_r = float(st.loc[pid, "A_r"])
                M_r = float(st.loc[pid, "M_r"])
            else:
                A_r = 0.0
                M_r = 0.0
            p_hat = (M_r + kappa * mu) / (A_r + kappa)
            exp += a * p_hat
        exp_by_team[team_id] = exp
    return exp_by_team

def compute_team_adjusted_points(
    team_df: pd.DataFrame,
    exp_3pm_by_team: dict[int, float],
    orb_rate: float,
    ppp: float,
) -> dict[int, dict[str, float]]:
    """Compute adjusted points per team including ORB correction."""
    out: dict[int, dict[str, float]] = {}
    haircut = orb_rate * ppp
    for _, r in team_df.iterrows():
        team_id = int(r["TEAM_ID"])
        pts = float(r["PTS"])
        m3 = float(r["FG3M"])
        exp3 = float(exp_3pm_by_team.get(team_id, m3))
        delta_3m = exp3 - m3
        delta_pts_3 = 3.0 * delta_3m
        orb_corr_pts = -haircut * delta_3m
        pts_adj = pts + delta_pts_3 + orb_corr_pts
        out[team_id] = {
            "delta_3m": delta_3m,
            "delta_pts_3": delta_pts_3,
            "orb_corr_pts": orb_corr_pts,
            "pts_adj": pts_adj,
        }
    return out

def update_player_state_attempt_decay(
    player_df: pd.DataFrame,
    player_state: pd.DataFrame,
    half_life_3pa: float,
) -> pd.DataFrame:
    """Update player_state with attempt-based exponential decay (post-game)."""
    gamma = 0.5 ** (1.0 / float(half_life_3pa))
    st = player_state.copy()
    st = st.set_index("player_id")
    for _, r in player_df.iterrows():
        pid = int(r["PLAYER_ID"])
        a = float(r["FG3A"])
        m = float(r["FG3M"])
        if a <= 0:
            continue
        if pid not in st.index:
            # should be rare if ensure_players_exist was called
            st.loc[pid, "player_name"] = r.get("PLAYER_NAME", "")
            st.loc[pid, "A_r"] = 0.0
            st.loc[pid, "M_r"] = 0.0
        A_r = float(st.loc[pid, "A_r"])
        M_r = float(st.loc[pid, "M_r"])
        decay = gamma ** a
        st.loc[pid, "A_r"] = decay * A_r + a
        st.loc[pid, "M_r"] = decay * M_r + m
        if "player_name" in st.columns and (not st.loc[pid, "player_name"]):
            st.loc[pid, "player_name"] = r.get("PLAYER_NAME", "")
    st = st.reset_index()
    # fill NaNs
    st["A_r"] = pd.to_numeric(st["A_r"], errors="coerce").fillna(0.0)
    st["M_r"] = pd.to_numeric(st["M_r"], errors="coerce").fillna(0.0)
    st["player_name"] = st["player_name"].fillna("")
    return st
