from __future__ import annotations

import json
import math
from pathlib import Path
import pandas as pd


# Sliding prior parameters
MU_MIN = 0.32      # Prior for rookies (0 career attempts)
MU_MAX = 0.36      # Prior for veterans (1000+ career attempts)
KAPPA_MIN = 200    # Prior strength for rookies
KAPPA_MAX = 300    # Prior strength for veterans
SCALE_ATTEMPTS = 1000  # Attempts at which prior/kappa reach max

# Assisted/unassisted shot mix adjustments
# These adjust the prior to reflect "true skill" independent of shot difficulty:
# - Unassisted (pull-up) = harder shots depress career %, so RAISE prior
# - Assisted (catch & shoot) = easier shots inflate career %, so LOWER prior
# Derived from: catch&shoot ~38%, pullup ~32%, league avg ~36%
UNASSISTED_MULTIPLIER = 1.12  # ~36/32
ASSISTED_MULTIPLIER = 0.95   # ~36/38

# Cache for assisted/unassisted data (loaded once)
_ASSISTED_UNASSISTED_DATA: dict[int, dict] | None = None


def load_assisted_unassisted_data() -> dict[int, dict]:
    """
    Load assisted/unassisted shot mix data from JSON file.
    Returns dict mapping player_id -> {pct_assisted, pct_unassisted, ...}
    """
    global _ASSISTED_UNASSISTED_DATA
    if _ASSISTED_UNASSISTED_DATA is not None:
        return _ASSISTED_UNASSISTED_DATA

    data_path = Path(__file__).parent.parent / "data" / "assisted_unassisted_stats.json"
    if not data_path.exists():
        _ASSISTED_UNASSISTED_DATA = {}
        return _ASSISTED_UNASSISTED_DATA

    with open(data_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    # Index by player_id for fast lookup
    _ASSISTED_UNASSISTED_DATA = {
        int(p["player_id"]): p for p in raw_data
    }
    return _ASSISTED_UNASSISTED_DATA


def get_shot_mix_adjustment(player_id: int) -> float:
    """
    Get the shot mix adjustment multiplier for a player.

    Returns a multiplier based on their assisted vs unassisted 3PA mix:
    - More assisted (catch & shoot) → higher multiplier (easier shots)
    - More unassisted (pull-up) → lower multiplier (harder shots)
    - Unknown players default to 1.0 (no adjustment)
    """
    data = load_assisted_unassisted_data()
    if player_id not in data:
        return 1.0

    player = data[player_id]
    # pct_assisted and pct_unassisted are stored as percentages (0-100)
    assisted_pct = player.get("pct_assisted", 50.0) / 100.0
    unassisted_pct = player.get("pct_unassisted", 50.0) / 100.0

    # Weighted average of multipliers based on shot mix
    return assisted_pct * ASSISTED_MULTIPLIER + unassisted_pct * UNASSISTED_MULTIPLIER

# Shot context difficulty multipliers (relative to league average 36.5%)
# These are league-average 3P% by shot type, expressed as multipliers
LEAGUE_AVG_3P = 0.365
SHOT_TYPE_MULTIPLIERS = {
    # Corner shots (easier)
    ("corner", "catch_shoot"): 0.41 / LEAGUE_AVG_3P,    # ~1.123
    ("corner", "pullup"): 0.37 / LEAGUE_AVG_3P,         # ~1.014
    ("corner", "stepback"): 0.36 / LEAGUE_AVG_3P,       # ~0.986
    ("corner", "running"): 0.34 / LEAGUE_AVG_3P,        # ~0.932
    ("corner", "fadeaway"): 0.33 / LEAGUE_AVG_3P,       # ~0.904
    ("corner", "turnaround"): 0.33 / LEAGUE_AVG_3P,     # ~0.904
    # Above the break (baseline is league average)
    ("above_break", "catch_shoot"): 0.38 / LEAGUE_AVG_3P,  # ~1.041
    ("above_break", "pullup"): 0.34 / LEAGUE_AVG_3P,       # ~0.932
    ("above_break", "stepback"): 0.33 / LEAGUE_AVG_3P,     # ~0.904
    ("above_break", "running"): 0.32 / LEAGUE_AVG_3P,      # ~0.877
    ("above_break", "fadeaway"): 0.31 / LEAGUE_AVG_3P,     # ~0.849
    ("above_break", "turnaround"): 0.31 / LEAGUE_AVG_3P,   # ~0.849
}
# Default multiplier for unknown shot types
DEFAULT_MULTIPLIER = 1.0


def get_shot_multiplier(area: str, shot_type: str) -> float:
    """Get the difficulty multiplier for a shot based on area and type."""
    return SHOT_TYPE_MULTIPLIERS.get((area, shot_type), DEFAULT_MULTIPLIER)


def get_player_prior(A_r: float, player_id: int | None = None) -> tuple[float, float]:
    """
    Calculate sliding prior (mu) and prior strength (kappa) based on career attempts,
    adjusted for the player's shot mix (assisted vs unassisted 3PA).

    Returns (mu, kappa) where:
    - mu scales from 32% (rookie) to 36% (veteran), then adjusted by shot mix
    - kappa scales from 200 (rookie) to 300 (veteran)

    Shot mix adjustment:
    - More assisted (catch & shoot) → higher mu (easier shots)
    - More unassisted (pull-up) → lower mu (harder shots)
    """
    scale = min(A_r / SCALE_ATTEMPTS, 1.0)
    mu = MU_MIN + (MU_MAX - MU_MIN) * scale
    kappa = KAPPA_MIN + (KAPPA_MAX - KAPPA_MIN) * scale

    # Apply shot mix adjustment if player_id is provided
    if player_id is not None:
        shot_mix_mult = get_shot_mix_adjustment(player_id)
        mu = mu * shot_mix_mult

    return mu, kappa


def compute_team_expected_3pm(
    player_df: pd.DataFrame,
    player_state: pd.DataFrame,
    mu: float,       # Kept for compatibility but not used (sliding prior instead)
    kappa: float,    # Kept for compatibility but not used (sliding kappa instead)
) -> dict[int, float]:
    """Compute expected made threes by team from player 3PA * p_hat (pre-game state).

    This is the legacy method without shot context. Use compute_team_expected_3pm_with_context
    for shot-level adjustments.
    """
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
            # Use sliding prior based on career attempts, adjusted for shot mix
            mu_player, kappa_player = get_player_prior(A_r, player_id=pid)
            p_hat = (M_r + kappa_player * mu_player) / (A_r + kappa_player)
            exp += a * p_hat
        exp_by_team[team_id] = exp
    return exp_by_team


def compute_team_expected_3pm_with_context(
    shots_df: pd.DataFrame,
    player_state: pd.DataFrame,
) -> dict[int, float]:
    """
    Compute expected made threes by team using shot-level context.

    Uses multiplicative adjustment:
        expected_make_prob = player_bayesian_3p% × shot_difficulty_multiplier

    Args:
        shots_df: DataFrame with GAME_ID, TEAM_ID, PLAYER_ID, MADE, AREA, SHOT_TYPE
        player_state: DataFrame with player_id, A_r, M_r

    Returns:
        Dict mapping team_id -> expected 3PM
    """
    if shots_df.empty:
        return {}

    st = player_state.set_index("player_id")[["A_r", "M_r"]]
    exp_by_team: dict[int, float] = {}

    for team_id, grp in shots_df.groupby("TEAM_ID"):
        team_id = int(team_id)
        exp = 0.0

        for _, shot in grp.iterrows():
            pid = shot.get("PLAYER_ID")
            if pid is None:
                continue
            pid = int(pid)

            # Get player's Bayesian expected 3P%
            if pid in st.index:
                A_r = float(st.loc[pid, "A_r"])
                M_r = float(st.loc[pid, "M_r"])
            else:
                A_r = 0.0
                M_r = 0.0

            mu_player, kappa_player = get_player_prior(A_r, player_id=pid)
            player_p_hat = (M_r + kappa_player * mu_player) / (A_r + kappa_player)

            # Get shot difficulty multiplier
            area = shot.get("AREA", "above_break")
            shot_type = shot.get("SHOT_TYPE", "catch_shoot")
            multiplier = get_shot_multiplier(area, shot_type)

            # Multiplicative adjustment: player skill × shot difficulty
            # Clamp to reasonable range [0.15, 0.55]
            expected_make_prob = max(0.15, min(0.55, player_p_hat * multiplier))
            exp += expected_make_prob

        exp_by_team[team_id] = exp

    return exp_by_team


def compute_player_deltas_with_context(
    shots_df: pd.DataFrame,
    player_state: pd.DataFrame,
    orb_rate: float,
    ppp: float,
) -> list[dict]:
    """
    Compute per-player point delta using shot-level context.

    Returns list of dicts with player_id, player_name, team_id, fg3a, fg3m,
    exp_3pm, delta_pts (positive = player was lucky).
    """
    if shots_df.empty:
        return []

    haircut = orb_rate * ppp
    st = player_state.set_index("player_id")[["A_r", "M_r"]]

    # Group shots by player
    player_results: dict[int, dict] = {}

    for _, shot in shots_df.iterrows():
        pid = shot.get("PLAYER_ID")
        if pid is None:
            continue
        pid = int(pid)

        # Get player's Bayesian expected 3P%
        if pid in st.index:
            A_r = float(st.loc[pid, "A_r"])
            M_r = float(st.loc[pid, "M_r"])
        else:
            A_r = 0.0
            M_r = 0.0

        mu_player, kappa_player = get_player_prior(A_r, player_id=pid)
        player_p_hat = (M_r + kappa_player * mu_player) / (A_r + kappa_player)

        # Get shot difficulty multiplier
        area = shot.get("AREA", "above_break")
        shot_type = shot.get("SHOT_TYPE", "catch_shoot")
        multiplier = get_shot_multiplier(area, shot_type)

        # Expected make probability for this shot
        expected_make_prob = max(0.15, min(0.55, player_p_hat * multiplier))

        # Initialize player entry if needed
        if pid not in player_results:
            player_results[pid] = {
                "player_id": pid,
                "player_name": shot.get("PLAYER_NAME", ""),
                "team_id": int(shot.get("TEAM_ID", 0)),
                "fg3a": 0,
                "fg3m": 0,
                "exp_3pm": 0.0,
            }

        # Accumulate
        player_results[pid]["fg3a"] += 1
        player_results[pid]["fg3m"] += int(shot.get("MADE", 0))
        player_results[pid]["exp_3pm"] += expected_make_prob

    # Calculate deltas
    results = []
    for pid, data in player_results.items():
        delta_3m = data["fg3m"] - data["exp_3pm"]
        delta_pts = 3.0 * delta_3m - haircut * (-delta_3m)
        data["delta_pts"] = delta_pts
        results.append(data)

    return results

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

def compute_player_deltas(
    player_df: pd.DataFrame,
    player_state: pd.DataFrame,
    mu: float,       # Kept for compatibility but not used (sliding prior instead)
    kappa: float,    # Kept for compatibility but not used (sliding kappa instead)
    orb_rate: float,
    ppp: float,
) -> list[dict]:
    """Compute per-player point delta (luck impact) for a game.

    Returns list of dicts with player_name, team_id, delta_pts (positive = player was lucky).
    """
    haircut = orb_rate * ppp
    st = player_state.set_index("player_id")[["A_r", "M_r"]]
    results = []

    for _, r in player_df.iterrows():
        a = float(r["FG3A"])
        if a <= 0:
            continue
        pid = int(r["PLAYER_ID"])
        m = float(r["FG3M"])

        if pid in st.index:
            A_r = float(st.loc[pid, "A_r"])
            M_r = float(st.loc[pid, "M_r"])
        else:
            A_r = 0.0
            M_r = 0.0

        # Use sliding prior based on career attempts, adjusted for shot mix
        mu_player, kappa_player = get_player_prior(A_r, player_id=pid)
        p_hat = (M_r + kappa_player * mu_player) / (A_r + kappa_player)
        exp_3pm = a * p_hat
        delta_3m = m - exp_3pm  # positive = made more than expected (lucky)
        delta_pts = 3.0 * delta_3m - haircut * (-delta_3m)  # points gained from luck

        results.append({
            "player_id": pid,
            "player_name": r.get("PLAYER_NAME", ""),
            "team_id": int(r["TEAM_ID"]),
            "fg3a": a,
            "fg3m": m,
            "exp_3pm": exp_3pm,
            "delta_pts": delta_pts,
        })

    return results


def get_biggest_swing_player(player_deltas: list[dict]) -> dict | None:
    """Find the player with the largest absolute point delta (most luck impact)."""
    if not player_deltas:
        return None
    return max(player_deltas, key=lambda x: abs(x["delta_pts"]))


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
