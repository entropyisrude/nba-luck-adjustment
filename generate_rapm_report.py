import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd
from scipy import sparse

import run_rapm as run_rapm_module

TEAM_ID_TO_ABBR = run_rapm_module.TEAM_ID_TO_ABBR
build_design_matrix = run_rapm_module.build_design_matrix
build_design_matrix_orapm = run_rapm_module.build_design_matrix_orapm
build_design_matrix_possession_od = run_rapm_module.build_design_matrix_possession_od
get_player_info = run_rapm_module.get_player_info
run_rapm = run_rapm_module.run_rapm
run_rapm_od = run_rapm_module.run_rapm_od


def _build_design_matrix_drapm_compat(stints: pd.DataFrame, use_adjusted: bool = True):
    player_list, player_to_idx = run_rapm_module.get_player_list_and_index(stints)
    n_players = len(player_list)
    n_stints = len(stints)
    n_rows = n_stints * 2

    row_indices: list[int] = []
    col_indices: list[int] = []
    data: list[float] = []

    for stint_idx, row in enumerate(stints.itertuples()):
        for col in ["home_p1", "home_p2", "home_p3", "home_p4", "home_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(2 * stint_idx)
                    col_indices.append(player_to_idx[pid])
                    data.append(1.0)
        for col in ["away_p1", "away_p2", "away_p3", "away_p4", "away_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(2 * stint_idx)
                    col_indices.append(player_to_idx[pid])
                    data.append(-1.0)
        for col in ["away_p1", "away_p2", "away_p3", "away_p4", "away_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(2 * stint_idx + 1)
                    col_indices.append(player_to_idx[pid])
                    data.append(1.0)
        for col in ["home_p1", "home_p2", "home_p3", "home_p4", "home_p5"]:
            pid = getattr(row, col)
            if pd.notna(pid):
                pid = int(pid)
                if pid in player_to_idx:
                    row_indices.append(2 * stint_idx + 1)
                    col_indices.append(player_to_idx[pid])
                    data.append(-1.0)

    X = sparse.csr_matrix((data, (row_indices, col_indices)), shape=(n_rows, n_players))
    possessions = np.maximum(stints["seconds"].values / 24.0, 0.1)

    if use_adjusted:
        home_pts = stints["home_pts_adj"].values
        away_pts = stints["away_pts_adj"].values
    else:
        home_pts = stints["home_pts"].values
        away_pts = stints["away_pts"].values

    y = np.zeros(n_rows)
    y[0::2] = (-away_pts / possessions) * 100.0
    y[1::2] = (-home_pts / possessions) * 100.0

    weights = np.zeros(n_rows)
    weights[0::2] = np.sqrt(possessions)
    weights[1::2] = np.sqrt(possessions)

    return X, y, weights, player_list, player_to_idx


build_design_matrix_drapm = getattr(
    run_rapm_module,
    "build_design_matrix_drapm",
    _build_design_matrix_drapm_compat,
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


def normalize_game_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "game_id" in df.columns:
        df["game_id"] = df["game_id"].astype(str).str.lstrip("0")
    return df


def load_possessions(paths: list[Path]) -> pd.DataFrame | None:
    frames: list[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            continue
        print(f"Loading possession-level data from {path}...")
        df = normalize_game_ids(pd.read_csv(path))
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df[df["date"].notna()].copy()
            df["season"] = df["date"].apply(season_key)
        elif "season_year" in df.columns:
            df["season"] = df["season_year"].apply(lambda y: f"{int(y)}-{(int(y) + 1) % 100:02d}")
        else:
            print(f"  Skipping {path}: no date or season_year column")
            continue
        frames.append(df)
        print(f"  Loaded {len(df)} possessions")

    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)
    dedupe_cols = [col for col in ["game_id", "poss_index"] if col in combined.columns]
    if dedupe_cols:
        combined = combined.drop_duplicates(subset=dedupe_cols, keep="last")
    return combined


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


def possession_subset_with_coverage(
    poss_df: pd.DataFrame | None,
    stints_df: pd.DataFrame,
    label: str,
    min_ratio: float = 0.98,
) -> pd.DataFrame | None:
    """
    Only use possession-level RAPM when possession rows cover nearly all stint games.

    Partial possession backfills can otherwise overwrite season RAPM with a
    regression fit on a tiny subset of games.
    """
    if poss_df is None or poss_df.empty:
        return None
    if "game_id" not in poss_df.columns or "game_id" not in stints_df.columns:
        return None

    stint_games = set(stints_df["game_id"].dropna().astype(str).str.lstrip("0").unique())
    poss_games = set(poss_df["game_id"].dropna().astype(str).str.lstrip("0").unique())
    if not stint_games:
        return None

    overlap = stint_games & poss_games
    coverage = len(overlap) / len(stint_games)
    if coverage < min_ratio:
        print(
            f"Skipping possession-level RAPM for {label}: "
            f"coverage {len(overlap)}/{len(stint_games)} games ({coverage:.1%}) is below {min_ratio:.0%}"
        )
        return None

    return poss_df[poss_df["game_id"].astype(str).str.lstrip("0").isin(overlap)].copy()


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
) -> List[dict]:
    """
    Compute RAPM, ORAPM, and DRAPM using possession-level data with unified O/D regression.

    This is the proper RAPM formulation where:
    - Single regression estimates both ORAPM and DRAPM simultaneously
    - Design matrix has 2*n_players columns (offense and defense)
    - Implicit opponent quality adjustment through simultaneous estimation

    Args:
        poss_df: Possession-level DataFrame
        stints_df: Stint-level DataFrame (for minutes and player info)
        alpha: Shared regularization for the unified O/D model
        min_minutes: Minimum minutes threshold
    """
    # Unified O/D RAPM from possessions. We use the same alpha for offense and
    # defense so RAPM remains the coherent sum of ORAPM and DRAPM.
    X_od_adj, y_od_adj, w_od_adj, players_od, _, n_players = build_design_matrix_possession_od(
        poss_df, use_adjusted=True
    )
    coef_o_adj, coef_d_adj, _ = run_rapm_od(
        X_od_adj, y_od_adj, w_od_adj, n_players, alpha_off=alpha, alpha_def=alpha
    )

    X_od_raw, y_od_raw, w_od_raw, _, _, _ = build_design_matrix_possession_od(
        poss_df, use_adjusted=False
    )
    coef_o_raw, coef_d_raw, _ = run_rapm_od(
        X_od_raw, y_od_raw, w_od_raw, n_players, alpha_off=alpha, alpha_def=alpha
    )

    orapm_adj = dict(zip(players_od, coef_o_adj))
    orapm_raw = dict(zip(players_od, coef_o_raw))
    drapm_adj = dict(zip(players_od, coef_d_adj))
    drapm_raw = dict(zip(players_od, coef_d_raw))

    # Minutes from stints
    minutes = compute_minutes(stints_df)
    info = get_player_info(players_od, stints_df)

    rows = []
    for pid in players_od:
        mins = minutes.get(pid, 0.0)
        if mins < min_minutes:
            continue
        pinfo = info.get(pid, {})
        team_id = pinfo.get("team_id", 0)
        rapm_adj = float(orapm_adj.get(pid, 0.0) + drapm_adj.get(pid, 0.0))
        rapm_raw = float(orapm_raw.get(pid, 0.0) + drapm_raw.get(pid, 0.0))
        rows.append(
            {
                "player_id": int(pid),
                "player_name": pinfo.get("name", f"Player {pid}"),
                "team_abbr": TEAM_ID_TO_ABBR.get(team_id, "???"),
                "minutes": int(round(mins)),
                "rapm": rapm_adj,
                "orapm": float(orapm_adj.get(pid, 0.0)),
                "drapm": float(drapm_adj.get(pid, 0.0)),
                "rapm_raw": rapm_raw,
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
    marker_idx = -1
    for marker in ("const sortState =", "const DEFAULT_ALPHA =", "function getPlayerInfo("):
        marker_idx = html.find(marker, start)
        if marker_idx != -1:
            break
    if marker_idx == -1:
        raise RuntimeError("RAPM script marker not found in rapm.html")

    prefix = html[:start]
    suffix = html[marker_idx:]
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
        "--possessions-files",
        type=str,
        default="possessions_historical.csv,possessions.csv",
        help="Comma-separated possession file names in data/ (default: possessions_historical.csv,possessions.csv).",
    )
    args = parser.parse_args()

    stints_path = DATA_DIR / "stints.csv"
    poss_paths = [DATA_DIR / name.strip() for name in args.possessions_files.split(",") if name.strip()]

    if not stints_path.exists():
        print("stints.csv not found; run run_onoff.py first.")
        return

    stints = normalize_game_ids(pd.read_csv(stints_path))
    stints["date"] = pd.to_datetime(stints["date"], errors="coerce")
    stints = stints[stints["date"].notna()].copy()
    stints["season"] = stints["date"].apply(season_key)

    # Load possessions if available and requested
    possessions = None
    if args.use_possessions:
        possessions = load_possessions(poss_paths)

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
        season_poss = possession_subset_with_coverage(
            possessions[possessions["season"] == latest_season].copy(),
            season_df,
            latest_season,
        )

    for alpha in season_alphas:
        if season_poss is not None and len(season_poss) > 0:
            print(f"Computing possession-level RAPM for {latest_season} (alpha={alpha})...")
            rapm[f"{latest_season}_a{int(alpha)}"] = compute_for_possessions(
                season_poss, season_df, alpha, args.min_minutes
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
            season_poss_hist = possession_subset_with_coverage(
                possessions[possessions["season"] == season].copy(),
                df,
                season,
            )

        # Recompute if --recompute-all or if using possessions (to get proper O/D RAPM)
        should_recompute = args.recompute_all or (season_poss_hist is not None)

        if should_recompute or key10 not in rapm:
            if season_poss_hist is not None and len(season_poss_hist) > 0:
                print(f"Computing possession-level RAPM for {season} (alpha=10)...")
                rapm[key10] = compute_for_possessions(
                    season_poss_hist, df, 10.0, args.min_minutes
                )
            else:
                rapm[key10] = compute_for_df(df, 10.0, args.min_minutes, args.od_alpha_mult)

        if should_recompute or key500 not in rapm:
            if season_poss_hist is not None and len(season_poss_hist) > 0:
                print(f"Computing possession-level RAPM for {season} (alpha=500)...")
                rapm[key500] = compute_for_possessions(
                    season_poss_hist, df, 500.0, args.min_minutes
                )
            else:
                rapm[key500] = compute_for_df(df, 500.0, args.min_minutes, args.od_alpha_mult)

        rapm[season] = rapm[key500]

    # Update rolling Last3
    rolling_df = stints[stints["season"].isin(last3)].copy()
    rolling_poss = None
    if possessions is not None:
        rolling_poss = possession_subset_with_coverage(
            possessions[possessions["season"].isin(last3)].copy(),
            rolling_df,
            f"Last3 ({', '.join(last3)})",
        )

    for alpha in last3_alphas:
        if rolling_poss is not None and len(rolling_poss) > 0:
            print(f"Computing possession-level RAPM for Last3 (alpha={alpha})...")
            rapm[f"Last3_a{int(alpha)}"] = compute_for_possessions(
                rolling_poss, rolling_df, alpha, args.min_minutes
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
