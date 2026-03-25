from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import LeagueDashPtDefend
from nba_api.stats.library.http import NBAStatsHTTP


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
OUTPUT_PATH = ROOT / "data" / "player_rim_defense_by_season.csv"


def season_label(start_year: int) -> str:
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def fetch_one_season(season: str) -> pd.DataFrame:
    try:
        df = LeagueDashPtDefend(
            season=season,
            defense_category="Less Than 6Ft",
        ).get_data_frames()[0]
    except Exception:
        # Older seasons can come back in a legacy "resultSets" payload that the
        # higher-level wrapper does not normalize reliably.
        params = {
            "College": "",
            "Conference": "",
            "Country": "",
            "DateFrom": "",
            "DateTo": "",
            "Division": "",
            "DraftPick": "",
            "DraftYear": "",
            "GameScope": "",
            "GameSegment": "",
            "Height": "",
            "LastNGames": 0,
            "LeagueID": "00",
            "Location": "",
            "Month": 0,
            "OpponentTeamID": 0,
            "Outcome": "",
            "PORound": 0,
            "PaceAdjust": "N",
            "PerMode": "Totals",
            "Period": 0,
            "PlayerExperience": "",
            "PlayerPosition": "",
            "PlusMinus": "N",
            "Rank": "N",
            "Season": season,
            "SeasonSegment": "",
            "SeasonType": "Regular Season",
            "StarterBench": "",
            "TeamID": 0,
            "VsConference": "",
            "VsDivision": "",
            "Weight": "",
            "DefenseCategory": "Less Than 6Ft",
        }
        payload = NBAStatsHTTP().send_api_request(
            endpoint="leaguedashptdefend", parameters=params
        ).get_dict()
        result_sets = payload.get("resultSets") or []
        if not result_sets:
            raise
        df = pd.DataFrame(result_sets[0]["rowSet"], columns=result_sets[0]["headers"])
    keep = [
        "CLOSE_DEF_PERSON_ID",
        "PLAYER_NAME",
        "PLAYER_LAST_TEAM_ABBREVIATION",
        "GP",
        "FGA_LT_06",
        "LT_06_PCT",
        "NS_LT_06_PCT",
        "PLUSMINUS",
    ]
    df = df[keep].copy()
    df.columns = [
        "player_id",
        "player_name",
        "team_abbr",
        "games",
        "rim_dfga",
        "rim_dfg_pct",
        "rim_dfg_pct_expected",
        "rim_dfg_plusminus",
    ]
    df["season"] = season
    df["rim_dfg_pct_diff"] = df["rim_dfg_pct_expected"] - df["rim_dfg_pct"]
    return df[
        [
            "season",
            "player_id",
            "player_name",
            "team_abbr",
            "games",
            "rim_dfga",
            "rim_dfg_pct",
            "rim_dfg_pct_expected",
            "rim_dfg_pct_diff",
            "rim_dfg_plusminus",
        ]
    ]


def main() -> None:
    frames: list[pd.DataFrame] = []
    failed: list[tuple[str, str]] = []
    for start_year in range(2013, 2026):
        season = season_label(start_year)
        try:
            print(f"Fetching {season}...")
            frames.append(fetch_one_season(season))
            time.sleep(0.8)
        except Exception as exc:  # pragma: no cover - network/endpoint variability
            failed.append((season, str(exc)))
            print(f"Failed {season}: {exc}")
            time.sleep(1.5)

    if not frames:
        raise RuntimeError("No rim defense seasons fetched.")

    out = pd.concat(frames, ignore_index=True)
    out["player_id"] = out["player_id"].astype("int64")
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {len(out)} rows to {OUTPUT_PATH}")
    if failed:
        print("Failures:")
        for season, err in failed:
            print(f"  {season}: {err}")


if __name__ == "__main__":
    main()
