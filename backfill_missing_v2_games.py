from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd


BASE = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")

V2_ONOFF = BASE / "data/adjusted_onoff_historical_pbp_v2.csv"
V2_STINTS = BASE / "data/stints_historical_pbp_v2.csv"
V2_POSS = BASE / "data/possessions_historical_pbp_v2.csv"

CUR_ONOFF = BASE / "data/adjusted_onoff.csv"
CUR_STINTS = BASE / "data/stints.csv"
CUR_POSS = BASE / "data/possessions.csv"

HIST_STINTS = BASE / "data/stints_historical.csv"
HIST_POSS = BASE / "data/possessions_historical.csv"

CURRENT_IDS = {
    "22500009",
    "22500010",
    "22500011",
    "22500012",
    "22500013",
    "22500014",
    "22500015",
    "22500016",
    "22500017",
    "22500692",
}

HIST_WITH_POSS_IDS = {
    "21200305",
    "21200360",
    "21201007",
    "21400884",
    "21500664",
    "21600192",
    "22000485",
    "22400667",
}


def backup_once(path: Path) -> None:
    backup = path.with_suffix(path.suffix + ".pre_backfill_20260316")
    if not backup.exists():
        shutil.copy2(path, backup)


def load_csv(path: Path, **kwargs) -> pd.DataFrame:
    return pd.read_csv(path, dtype={"game_id": str}, **kwargs)


def append_missing_rows(target_path: Path, source_df: pd.DataFrame, game_ids: set[str]) -> None:
    target = load_csv(target_path)
    target["game_id"] = target["game_id"].astype(str).str.zfill(8)
    source = source_df.copy()
    source["game_id"] = source["game_id"].astype(str).str.zfill(8)
    add = source[source["game_id"].isin(game_ids)]
    if add.empty:
        return
    combined = pd.concat([target, add], ignore_index=True)
    combined.to_csv(target_path, index=False)


def derive_onoff_from_stints(stints_df: pd.DataFrame) -> pd.DataFrame:
    st = stints_df.copy()
    st["seconds"] = pd.to_numeric(st["seconds"], errors="coerce").fillna(0.0)
    st["home_pts"] = pd.to_numeric(st["home_pts"], errors="coerce").fillna(0.0)
    st["away_pts"] = pd.to_numeric(st["away_pts"], errors="coerce").fillna(0.0)

    rows: list[dict] = []
    for gid, g in st.groupby("game_id"):
        gid = str(gid).zfill(8)
        date = g["date"].iloc[0] if "date" in g.columns else ""
        home_id = int(g["home_id"].iloc[0])
        away_id = int(g["away_id"].iloc[0])
        acc: dict[tuple[int, int], dict[str, float | str | int]] = {}
        for row in g.itertuples(index=False):
            home_players = [int(getattr(row, f"home_p{i}")) for i in range(1, 6)]
            away_players = [int(getattr(row, f"away_p{i}")) for i in range(1, 6)]
            for pid in home_players:
                if pid <= 0:
                    continue
                key = (home_id, pid)
                entry = acc.setdefault(
                    key,
                    {
                        "game_id": gid,
                        "team_id": home_id,
                        "player_id": pid,
                        "player_name": str(pid),
                        "minutes_on": 0.0,
                        "on_pts_for": 0.0,
                        "on_pts_against": 0.0,
                        "date": date,
                    },
                )
                entry["minutes_on"] = float(entry["minutes_on"]) + float(row.seconds) / 60.0
                entry["on_pts_for"] = float(entry["on_pts_for"]) + float(row.home_pts)
                entry["on_pts_against"] = float(entry["on_pts_against"]) + float(row.away_pts)
            for pid in away_players:
                if pid <= 0:
                    continue
                key = (away_id, pid)
                entry = acc.setdefault(
                    key,
                    {
                        "game_id": gid,
                        "team_id": away_id,
                        "player_id": pid,
                        "player_name": str(pid),
                        "minutes_on": 0.0,
                        "on_pts_for": 0.0,
                        "on_pts_against": 0.0,
                        "date": date,
                    },
                )
                entry["minutes_on"] = float(entry["minutes_on"]) + float(row.seconds) / 60.0
                entry["on_pts_for"] = float(entry["on_pts_for"]) + float(row.away_pts)
                entry["on_pts_against"] = float(entry["on_pts_against"]) + float(row.home_pts)
        for entry in acc.values():
            on_for = float(entry["on_pts_for"])
            on_against = float(entry["on_pts_against"])
            rows.append(
                {
                    "game_id": entry["game_id"],
                    "team_id": entry["team_id"],
                    "player_id": entry["player_id"],
                    "player_name": entry["player_name"],
                    "on_pts_for": on_for,
                    "on_pts_against": on_against,
                    "on_diff": on_for - on_against,
                    "off_pts_for": 0.0,
                    "off_pts_against": 0.0,
                    "off_diff": 0.0,
                    "on_ortg": 0.0,
                    "on_drtg": 0.0,
                    "on_net": 0.0,
                    "off_ortg": 0.0,
                    "off_drtg": 0.0,
                    "off_net": 0.0,
                    "ortg_diff": 0.0,
                    "drtg_diff": 0.0,
                    "net_diff": 0.0,
                    "minutes_on": float(entry["minutes_on"]),
                    "minutes_off": 0.0,
                    "minutes_total": float(entry["minutes_on"]),
                    "poss_on": 0.0,
                    "date": entry["date"],
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    for path in [V2_ONOFF, V2_STINTS, V2_POSS]:
        backup_once(path)

    cur_onoff = load_csv(CUR_ONOFF)
    cur_stints = load_csv(CUR_STINTS)
    cur_poss = load_csv(CUR_POSS)
    hist_stints = load_csv(HIST_STINTS)
    hist_poss = load_csv(HIST_POSS)

    append_missing_rows(V2_ONOFF, cur_onoff, CURRENT_IDS)
    append_missing_rows(V2_STINTS, cur_stints, CURRENT_IDS)
    append_missing_rows(V2_POSS, cur_poss, CURRENT_IDS)

    hist_stints["game_id"] = hist_stints["game_id"].astype(str).str.zfill(8)
    hist_poss["game_id"] = hist_poss["game_id"].astype(str).str.zfill(8)

    hist_stints_add = hist_stints[hist_stints["game_id"].isin(HIST_WITH_POSS_IDS)]
    append_missing_rows(V2_STINTS, hist_stints_add, HIST_WITH_POSS_IDS)
    append_missing_rows(V2_POSS, hist_poss, HIST_WITH_POSS_IDS)

    hist_onoff_add = derive_onoff_from_stints(hist_stints_add)
    append_missing_rows(V2_ONOFF, hist_onoff_add, HIST_WITH_POSS_IDS)

    print("backfill_complete")


if __name__ == "__main__":
    main()
