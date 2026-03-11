import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

from run_rapm import (
    TEAM_ID_TO_ABBR,
    build_design_matrix,
    build_design_matrix_orapm,
    build_design_matrix_drapm,
    build_design_matrix_possession_od,
    get_player_info,
    run_rapm,
    run_rapm_od,
)

DATA_DIR = Path("data")
RAPM_JSON = DATA_DIR / "rapm_all.json"
PLAYER_INFO_MAP = DATA_DIR / "player_info_map.json"
RAPM_HTML = Path("rapm.html")


PLAYER_COLS = [
    "home_p1",
    "home_p2",
    "home_p3",
    "home_p4",
    "home_p5",
    "away_p1",
    "away_p2",
    "away_p3",
    "away_p4",
    "away_p5",
]


def season_key(dt: pd.Timestamp) -> str:
    y = dt.year
    if dt.month >= 7:
        start = y
    else:
        start = y - 1
    end2 = (start + 1) % 100
    return f"{start}-{end2:02d}"


def score_name(name: str | None) -> int:
    if not name:
        return -1
    name = name.strip()
    if not name or name.lower().startswith("player "):
        return -1
    words = name.split()
    score = len(name)
    if len(words) >= 2:
        score += 100
    return score


def compute_minutes(df: pd.DataFrame) -> Dict[int, float]:
    players = df[PLAYER_COLS].to_numpy()
    secs = df["seconds"].to_numpy()
    flat_players = players.reshape(-1)
    flat_secs = np.repeat(secs, players.shape[1])
    mask = ~pd.isna(flat_players)
    flat_players = flat_players[mask].astype(int)
    flat_secs = flat_secs[mask]
    minutes = pd.Series(flat_secs / 60.0, index=flat_players).groupby(level=0).sum()
    return minutes.to_dict()


def compute_for_df(df: pd.DataFrame, alpha: float, min_minutes: int, od_alpha_mult: float = 5.0) -> List[dict]:
    """
    Compute RAPM, ORAPM, and DRAPM for all players in the dataframe (stint-level fallback).

    DRAPM uses a SEPARATE regression with different targets than ORAPM:
    - ORAPM: target = points SCORED by offensive team
    - DRAPM: target = points ALLOWED by defensive team (negated so positive = good)

    O/D regressions use higher alpha (od_alpha_mult * alpha) because each player
    only appears in half the observations, requiring more regularization.
    """
    od_alpha = alpha * od_alpha_mult

    X_adj, y_adj, w_adj, players_adj, _ = build_design_matrix(df, use_adjusted=True)
    coef_adj, _ = run_rapm(X_adj, y_adj, w_adj, alpha=alpha)

    X_raw, y_raw, w_raw, players_raw, _ = build_design_matrix(df, use_adjusted=False)
    coef_raw, _ = run_rapm(X_raw, y_raw, w_raw, alpha=alpha)

    Xo_adj, yo_adj, wo_adj, players_o_adj, _ = build_design_matrix_orapm(df, use_adjusted=True)
    coef_o_adj, _ = run_rapm(Xo_adj, yo_adj, wo_adj, alpha=od_alpha)

    Xo_raw, yo_raw, wo_raw, players_o_raw, _ = build_design_matrix_orapm(df, use_adjusted=False)
    coef_o_raw, _ = run_rapm(Xo_raw, yo_raw, wo_raw, alpha=od_alpha)

    # DRAPM uses separate regression with points ALLOWED as target
    Xd_adj, yd_adj, wd_adj, players_d_adj, _ = build_design_matrix_drapm(df, use_adjusted=True)
    coef_d_adj, _ = run_rapm(Xd_adj, yd_adj, wd_adj, alpha=od_alpha)

    Xd_raw, yd_raw, wd_raw, players_d_raw, _ = build_design_matrix_drapm(df, use_adjusted=False)
    coef_d_raw, _ = run_rapm(Xd_raw, yd_raw, wd_raw, alpha=od_alpha)

    rapm_adj = dict(zip(players_adj, coef_adj))
    rapm_raw = dict(zip(players_raw, coef_raw))
    orapm_adj = dict(zip(players_o_adj, coef_o_adj))
    orapm_raw = dict(zip(players_o_raw, coef_o_raw))
    drapm_adj = dict(zip(players_d_adj, coef_d_adj))
    drapm_raw = dict(zip(players_d_raw, coef_d_raw))

    minutes = compute_minutes(df)
    info = get_player_info(players_adj, df)

    rows = []
    for pid in players_adj:
        mins = minutes.get(pid, 0.0)
        if mins < min_minutes:
            continue
        pinfo = info.get(pid, {})
        team_id = pinfo.get("team_id", 0)
        rows.append(
            {
                "player_id": int(pid),
                "player_name": pinfo.get("name", f"Player {pid}"),
                "team_abbr": TEAM_ID_TO_ABBR.get(team_id, "???"),
                "minutes": int(round(mins)),
                "rapm": float(rapm_adj.get(pid, 0.0)),
                "orapm": float(orapm_adj.get(pid, 0.0)),
                "drapm": float(drapm_adj.get(pid, 0.0)),
                "rapm_raw": float(rapm_raw.get(pid, 0.0)),
                "orapm_raw": float(orapm_raw.get(pid, 0.0)),
                "drapm_raw": float(drapm_raw.get(pid, 0.0)),
            }
        )
    return rows


def compute_for_possessions(
    poss_df: pd.DataFrame,
    stints_df: pd.DataFrame,
    alpha: float,
    min_minutes: int,
    alpha_off: float = 2500.0,
    alpha_def: float = 2500.0,
) -> List[dict]:
    """
    Compute RAPM, ORAPM, and DRAPM using possession-level data with unified O/D regression.

    This is the proper RAPM formulation where:
    - Single regression estimates both ORAPM and DRAPM simultaneously
    - Design matrix has 2*n_players columns (offense and defense)
    - Implicit opponent quality adjustment through simultaneous estimation

    Args:
        poss_df: Possession-level DataFrame
        stints_df: Stint-level DataFrame (for combined RAPM and minutes)
        alpha: Regularization for combined RAPM
        min_minutes: Minimum minutes threshold
        alpha_off: Regularization for offensive coefficients
        alpha_def: Regularization for defensive coefficients
    """
    # Combined RAPM from stints (traditional approach)
    X_adj, y_adj, w_adj, players_adj, _ = build_design_matrix(stints_df, use_adjusted=True)
    coef_adj, _ = run_rapm(X_adj, y_adj, w_adj, alpha=alpha)

    X_raw, y_raw, w_raw, players_raw, _ = build_design_matrix(stints_df, use_adjusted=False)
    coef_raw, _ = run_rapm(X_raw, y_raw, w_raw, alpha=alpha)

    rapm_adj = dict(zip(players_adj, coef_adj))
    rapm_raw = dict(zip(players_raw, coef_raw))

    # Unified O/D RAPM from possessions
    X_od_adj, y_od_adj, w_od_adj, players_od, _, n_players = build_design_matrix_possession_od(
        poss_df, use_adjusted=True
    )
    coef_o_adj, coef_d_adj, _ = run_rapm_od(
        X_od_adj, y_od_adj, w_od_adj, n_players, alpha_off=alpha_off, alpha_def=alpha_def
    )

    X_od_raw, y_od_raw, w_od_raw, _, _, _ = build_design_matrix_possession_od(
        poss_df, use_adjusted=False
    )
    coef_o_raw, coef_d_raw, _ = run_rapm_od(
        X_od_raw, y_od_raw, w_od_raw, n_players, alpha_off=alpha_off, alpha_def=alpha_def
    )

    orapm_adj = dict(zip(players_od, coef_o_adj))
    orapm_raw = dict(zip(players_od, coef_o_raw))
    drapm_adj = dict(zip(players_od, coef_d_adj))
    drapm_raw = dict(zip(players_od, coef_d_raw))

    # Minutes from stints
    minutes = compute_minutes(stints_df)
    info = get_player_info(players_adj, stints_df)

    rows = []
    for pid in players_adj:
        mins = minutes.get(pid, 0.0)
        if mins < min_minutes:
            continue
        pinfo = info.get(pid, {})
        team_id = pinfo.get("team_id", 0)
        rows.append(
            {
                "player_id": int(pid),
                "player_name": pinfo.get("name", f"Player {pid}"),
                "team_abbr": TEAM_ID_TO_ABBR.get(team_id, "???"),
                "minutes": int(round(mins)),
                "rapm": float(rapm_adj.get(pid, 0.0)),
                "orapm": float(orapm_adj.get(pid, 0.0)),
                "drapm": float(drapm_adj.get(pid, 0.0)),
                "rapm_raw": float(rapm_raw.get(pid, 0.0)),
                "orapm_raw": float(orapm_raw.get(pid, 0.0)),
                "drapm_raw": float(drapm_raw.get(pid, 0.0)),
            }
        )
    return rows


def update_player_info_map() -> Dict[str, dict]:
    info: Dict[str, dict] = {}
    if PLAYER_INFO_MAP.exists():
        info = json.loads(PLAYER_INFO_MAP.read_text(encoding="utf-8"))

    api_names: Dict[int, str] = {}
    try:
        from nba_api.stats.static import players

        for p in players.get_players():
            pid = p.get("id")
            name = p.get("full_name")
            if pid and name:
                api_names[int(pid)] = name
    except Exception as exc:
        print(f"Warning: nba_api unavailable for player names ({exc})")

    candidates: Dict[int, Tuple[int, str]] = {}
    for path in [DATA_DIR / "adjusted_onoff.csv", DATA_DIR / "player_onoff_history.csv"]:
        if not path.exists():
            continue
        df = pd.read_csv(path, dtype={"player_id": int})
        for _, row in df.iterrows():
            pid = int(row["player_id"])
            name = row.get("player_name")
            if isinstance(name, str) and pid not in api_names:
                s = score_name(name)
                if s >= 0:
                    prev = candidates.get(pid)
                    if not prev or s > prev[0]:
                        candidates[pid] = (s, name)
            team_id = row.get("team_id", row.get("latest_team_id", 0))
            try:
                team_id = int(team_id)
            except Exception:
                team_id = 0
            rec = info.get(str(pid), {})
            if team_id:
                rec["team_id"] = team_id
            info[str(pid)] = rec

    for pid, (_, name) in candidates.items():
        rec = info.get(str(pid), {})
        existing = rec.get("name")
        if score_name(existing) >= score_name(name):
            info[str(pid)] = rec
            continue
        rec["name"] = name
        info[str(pid)] = rec

    for pid, name in api_names.items():
        rec = info.get(str(pid), {})
        rec["name"] = name
        info[str(pid)] = rec

    for pid, rec in info.items():
        team_id = rec.get("team_id", 0) or 0
        rec["team_abbr"] = TEAM_ID_TO_ABBR.get(int(team_id), rec.get("team_abbr"))

    PLAYER_INFO_MAP.write_text(json.dumps(info, separators=(",", ":")), encoding="utf-8")
    return info


def embed_rapm_html(rapm: dict, player_map: dict) -> None:
    if not RAPM_HTML.exists():
        print(f"rapm.html not found at {RAPM_HTML}")
        return
    html = RAPM_HTML.read_text(encoding="utf-8")
    json_data = json.dumps(rapm, separators=(",", ":"))
    json_map = json.dumps(player_map, separators=(",", ":"))

    start = html.find("const DATA = ")
    if start == -1:
        raise RuntimeError("const DATA not found in rapm.html")
    let_idx = html.find("let sortKey", start)
    if let_idx == -1:
        raise RuntimeError("let sortKey not found in rapm.html")

    prefix = html[:start]
    suffix = html[let_idx:]
    new_block = f"const DATA = {json_data};\n    const PLAYER_MAP = {json_map};\n\n    "
    RAPM_HTML.write_text(prefix + new_block + suffix, encoding="utf-8")


def _name_word_count(name: str | None) -> int:
    if not name:
        return 0
    return len(name.strip().split())


def apply_player_names(rapm: dict, player_map: dict) -> None:
    """Force names from player_map when available to avoid mislabels."""
    for _, rows in rapm.items():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            pid = row.get("player_id")
            if pid is None:
                continue
            info = player_map.get(str(pid)) or player_map.get(pid) or {}
            mapped = info.get("name")
            if not mapped:
                continue
            row["player_name"] = mapped


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-minutes", type=int, default=200)
    parser.add_argument(
        "--season-alphas",
        default="10,500",
        help="Comma-separated alphas for latest season",
    )
    parser.add_argument(
        "--last3-alphas",
        default="10,500",
        help="Comma-separated alphas for Last3",
    )
    parser.add_argument(
        "--od-alpha-mult",
        type=float,
        default=5.0,
        help="Multiplier for O/D alpha for stint-level fallback (default: 5.0).",
    )
    parser.add_argument(
        "--alpha-off",
        type=float,
        default=4000.0,
        help="Alpha for offensive coefficients in possession-level RAPM (default: 4000).",
    )
    parser.add_argument(
        "--alpha-def",
        type=float,
        default=6000.0,
        help="Alpha for defensive coefficients in possession-level RAPM (default: 6000).",
    )
    parser.add_argument(
        "--use-possessions",
        action="store_true",
        help="Use possession-level RAPM when possessions.csv is available.",
    )
    parser.add_argument(
        "--recompute-all",
        action="store_true",
        help="Recompute RAPM for all seasons (not just latest).",
    )
    parser.add_argument(
        "--possessions-file",
        type=str,
        default="possessions.csv",
        help="Possession file name in data/ directory (default: possessions.csv).",
    )
    args = parser.parse_args()

    stints_path = DATA_DIR / "stints.csv"
    poss_path = DATA_DIR / args.possessions_file

    if not stints_path.exists():
        print("stints.csv not found; run run_onoff.py first.")
        return

    stints = pd.read_csv(stints_path)
    stints["date"] = pd.to_datetime(stints["date"], errors="coerce")
    stints = stints[stints["date"].notna()].copy()
    stints["season"] = stints["date"].apply(season_key)

    # Load possessions if available and requested
    possessions = None
    if args.use_possessions and poss_path.exists():
        print(f"Loading possession-level data from {poss_path}...")
        possessions = pd.read_csv(poss_path)
        # Handle both formats: with 'date' column or with 'season_year' column
        if "date" in possessions.columns:
            possessions["date"] = pd.to_datetime(possessions["date"], errors="coerce")
            possessions = possessions[possessions["date"].notna()].copy()
            possessions["season"] = possessions["date"].apply(season_key)
        elif "season_year" in possessions.columns:
            # Convert season_year (e.g., 2024) to season format (e.g., "2024-25")
            def year_to_season(y: int) -> str:
                return f"{y}-{(y + 1) % 100:02d}"
            possessions["season"] = possessions["season_year"].apply(year_to_season)
        print(f"  Loaded {len(possessions)} possessions")

    seasons = sorted(stints["season"].dropna().unique())
    seasons = [s for s in seasons if len(s) == 7 and s[4] == "-"]
    if not seasons:
        print("No seasons found in stints.csv")
        return

    latest_season = seasons[-1]
    last3 = seasons[-3:] if len(seasons) >= 3 else seasons

    season_alphas = [float(a) for a in args.season_alphas.split(",") if a.strip()]
    last3_alphas = [float(a) for a in args.last3_alphas.split(",") if a.strip()]

    rapm = json.loads(RAPM_JSON.read_text(encoding="utf-8")) if RAPM_JSON.exists() else {}

    # Keep only alpha=10/500 variants (plus season keys/Last3 base)
    for k in list(rapm.keys()):
        if k == "Last3_seasons":
            rapm.pop(k, None)
            continue
        if "_a" in k:
            if not (k.endswith("_a10") or k.endswith("_a500")):
                rapm.pop(k, None)

    # Update latest season
    season_df = stints[stints["season"] == latest_season].copy()
    season_poss = None
    if possessions is not None:
        season_poss = possessions[possessions["season"] == latest_season].copy()

    for alpha in season_alphas:
        if season_poss is not None and len(season_poss) > 0:
            print(f"Computing possession-level RAPM for {latest_season} (alpha={alpha})...")
            rapm[f"{latest_season}_a{int(alpha)}"] = compute_for_possessions(
                season_poss, season_df, alpha, args.min_minutes,
                alpha_off=args.alpha_off, alpha_def=args.alpha_def
            )
        else:
            rapm[f"{latest_season}_a{int(alpha)}"] = compute_for_df(
                season_df, alpha, args.min_minutes, args.od_alpha_mult
            )
    if f"{latest_season}_a500" in rapm:
        rapm[latest_season] = rapm[f"{latest_season}_a500"]

    # Compute RAPM for all seasons
    for season in seasons:
        df = stints[stints["season"] == season].copy()
        key10 = f"{season}_a10"
        key500 = f"{season}_a500"

        # Check if we have possessions for this season
        season_poss_hist = None
        if possessions is not None:
            season_poss_hist = possessions[possessions["season"] == season].copy()
            if len(season_poss_hist) == 0:
                season_poss_hist = None

        # Recompute if --recompute-all or if using possessions (to get proper O/D RAPM)
        should_recompute = args.recompute_all or (season_poss_hist is not None)

        if should_recompute or key10 not in rapm:
            if season_poss_hist is not None and len(season_poss_hist) > 0:
                print(f"Computing possession-level RAPM for {season} (alpha=10)...")
                rapm[key10] = compute_for_possessions(
                    season_poss_hist, df, 10.0, args.min_minutes,
                    alpha_off=args.alpha_off, alpha_def=args.alpha_def
                )
            else:
                rapm[key10] = compute_for_df(df, 10.0, args.min_minutes, args.od_alpha_mult)

        if should_recompute or key500 not in rapm:
            if season_poss_hist is not None and len(season_poss_hist) > 0:
                print(f"Computing possession-level RAPM for {season} (alpha=500)...")
                rapm[key500] = compute_for_possessions(
                    season_poss_hist, df, 500.0, args.min_minutes,
                    alpha_off=args.alpha_off, alpha_def=args.alpha_def
                )
            else:
                rapm[key500] = compute_for_df(df, 500.0, args.min_minutes, args.od_alpha_mult)

        rapm[season] = rapm[key500]

    # Update rolling Last3
    rolling_df = stints[stints["season"].isin(last3)].copy()
    rolling_poss = None
    if possessions is not None:
        rolling_poss = possessions[possessions["season"].isin(last3)].copy()

    for alpha in last3_alphas:
        if rolling_poss is not None and len(rolling_poss) > 0:
            print(f"Computing possession-level RAPM for Last3 (alpha={alpha})...")
            rapm[f"Last3_a{int(alpha)}"] = compute_for_possessions(
                rolling_poss, rolling_df, alpha, args.min_minutes,
                alpha_off=args.alpha_off, alpha_def=args.alpha_def
            )
        else:
            rapm[f"Last3_a{int(alpha)}"] = compute_for_df(
                rolling_df, alpha, args.min_minutes, args.od_alpha_mult
            )
    if "Last3_a500" in rapm:
        rapm["Last3"] = rapm["Last3_a500"]

    player_map = update_player_info_map()
    apply_player_names(rapm, player_map)

    RAPM_JSON.write_text(json.dumps(rapm, separators=(",", ":")), encoding="utf-8")
    embed_rapm_html(rapm, player_map)

    print(f"Updated RAPM for {latest_season} and Last3 ({', '.join(last3)})")


if __name__ == "__main__":
    main()
