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
    get_player_info,
    run_rapm,
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


def compute_for_df(df: pd.DataFrame, alpha: float, min_minutes: int) -> List[dict]:
    X_adj, y_adj, w_adj, players_adj, _ = build_design_matrix(df, use_adjusted=True)
    coef_adj, _ = run_rapm(X_adj, y_adj, w_adj, alpha=alpha)

    X_raw, y_raw, w_raw, players_raw, _ = build_design_matrix(df, use_adjusted=False)
    coef_raw, _ = run_rapm(X_raw, y_raw, w_raw, alpha=alpha)

    Xo_adj, yo_adj, wo_adj, players_o_adj, _ = build_design_matrix_orapm(df, use_adjusted=True)
    coef_o_adj, _ = run_rapm(Xo_adj, yo_adj, wo_adj, alpha=alpha)

    Xo_raw, yo_raw, wo_raw, players_o_raw, _ = build_design_matrix_orapm(df, use_adjusted=False)
    coef_o_raw, _ = run_rapm(Xo_raw, yo_raw, wo_raw, alpha=alpha)

    rapm_adj = dict(zip(players_adj, coef_adj))
    rapm_raw = dict(zip(players_raw, coef_raw))
    orapm_adj = dict(zip(players_o_adj, coef_o_adj))
    orapm_raw = dict(zip(players_o_raw, coef_o_raw))

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
                "drapm": float(rapm_adj.get(pid, 0.0) - orapm_adj.get(pid, 0.0)),
                "rapm_raw": float(rapm_raw.get(pid, 0.0)),
                "orapm_raw": float(orapm_raw.get(pid, 0.0)),
                "drapm_raw": float(rapm_raw.get(pid, 0.0) - orapm_raw.get(pid, 0.0)),
            }
        )
    return rows


def update_player_info_map() -> Dict[str, dict]:
    info: Dict[str, dict] = {}
    if PLAYER_INFO_MAP.exists():
        info = json.loads(PLAYER_INFO_MAP.read_text(encoding="utf-8"))

    candidates: Dict[int, Tuple[int, str]] = {}
    for path in [DATA_DIR / "adjusted_onoff.csv", DATA_DIR / "player_onoff_history.csv"]:
        if not path.exists():
            continue
        df = pd.read_csv(path, dtype={"player_id": int})
        for _, row in df.iterrows():
            pid = int(row["player_id"])
            name = row.get("player_name")
            if isinstance(name, str):
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-minutes", type=int, default=200)
    parser.add_argument(
        "--season-alphas",
        default="1,10,50,250,500,1000,2500",
        help="Comma-separated alphas for latest season",
    )
    parser.add_argument(
        "--last3-alphas",
        default="1,500,2500",
        help="Comma-separated alphas for Last3",
    )
    args = parser.parse_args()

    stints_path = DATA_DIR / "stints.csv"
    if not stints_path.exists():
        print("stints.csv not found; run run_onoff.py first.")
        return

    stints = pd.read_csv(stints_path)
    stints["date"] = pd.to_datetime(stints["date"], errors="coerce")
    stints = stints[stints["date"].notna()].copy()
    stints["season"] = stints["date"].apply(season_key)

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

    # Drop unsupported alpha variants
    for k in list(rapm.keys()):
        if k.endswith("_a25") or k.endswith("_a100"):
            rapm.pop(k, None)
        if k == "Last3_seasons":
            rapm.pop(k, None)

    # Update latest season
    season_df = stints[stints["season"] == latest_season].copy()
    for alpha in season_alphas:
        rapm[f"{latest_season}_a{int(alpha)}"] = compute_for_df(season_df, alpha, args.min_minutes)
    if f"{latest_season}_a2500" in rapm:
        rapm[latest_season] = rapm[f"{latest_season}_a2500"]

    # Ensure alpha=1 exists for all seasons if missing (do not recompute existing)
    for season in seasons:
        key = f"{season}_a1"
        if key not in rapm:
            df = stints[stints["season"] == season].copy()
            rapm[key] = compute_for_df(df, 1.0, args.min_minutes)

    # Update rolling Last3
    rolling_df = stints[stints["season"].isin(last3)].copy()
    for alpha in last3_alphas:
        rapm[f"Last3_a{int(alpha)}"] = compute_for_df(rolling_df, alpha, args.min_minutes)
    if "Last3_a2500" in rapm:
        rapm["Last3"] = rapm["Last3_a2500"]

    RAPM_JSON.write_text(json.dumps(rapm, separators=(",", ":")), encoding="utf-8")

    player_map = update_player_info_map()
    embed_rapm_html(rapm, player_map)

    print(f"Updated RAPM for {latest_season} and Last3 ({', '.join(last3)})")


if __name__ == "__main__":
    main()
