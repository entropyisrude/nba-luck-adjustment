"""
Generate possessions.csv from local PBP files for historical seasons.

Usage:
    python generate_possessions_from_pbp.py --start-year 2020 --end-year 2024
    python generate_possessions_from_pbp.py --year 2024
"""

import argparse
from pathlib import Path
import pandas as pd
from src.ingest import _normalize_statsv3_actions


DATA_DIR = Path("data")
PBP_DIR = DATA_DIR / "pbp"


def _elapsed_game_seconds(period: int | None, clock_str: str | None) -> int | None:
    if period is None:
        return None
    if not clock_str:
        return None
    try:
        s = clock_str.replace("PT", "").replace("S", "")
        if "M" in s:
            mm, ss = s.split("M")
            minutes = int(mm)
            seconds = float(ss)
        else:
            minutes = 0
            seconds = float(s)
        remaining = int(round(minutes * 60 + seconds))
    except Exception:
        return None

    period_length = 12 * 60 if period <= 4 else 5 * 60
    elapsed_prev = 0
    if period > 1:
        elapsed_prev += min(period - 1, 4) * 12 * 60
        if period > 5:
            elapsed_prev += (period - 5) * 5 * 60
    return elapsed_prev + (period_length - remaining)


def _infer_home_away_from_actions(actions: list[dict]) -> tuple[int, int]:
    home_votes: dict[int, int] = {}
    away_votes: dict[int, int] = {}
    prev_home = None
    prev_away = None
    for a in actions:
        try:
            team_id = int(a.get("teamId", 0) or 0)
        except Exception:
            team_id = 0
        if team_id <= 0:
            continue
        try:
            score_home = int(a.get("scoreHome", 0) or 0)
            score_away = int(a.get("scoreAway", 0) or 0)
        except Exception:
            continue
        if prev_home is not None and prev_away is not None:
            if score_home > prev_home and score_away == prev_away:
                home_votes[team_id] = home_votes.get(team_id, 0) + 1
            elif score_away > prev_away and score_home == prev_home:
                away_votes[team_id] = away_votes.get(team_id, 0) + 1
        prev_home, prev_away = score_home, score_away

    if home_votes and away_votes:
        home_id = max(home_votes, key=home_votes.get)
        away_id = max(away_votes, key=away_votes.get)
        if home_id != away_id:
            return int(home_id), int(away_id)
    return 0, 0


def _infer_starters_from_actions(actions: list[dict]) -> dict[int, list[int]]:
    starters: dict[int, list[int]] = {}
    cutoff_seconds = [120, 300, 600]

    for limit in cutoff_seconds:
        starters = {}
        for a in actions:
            e = _elapsed_game_seconds(a.get("period"), a.get("clock"))
            if e is None or e > limit:
                continue
            try:
                team_id = int(a.get("teamId", 0) or 0)
                pid = int(a.get("personId", 0) or 0)
            except Exception:
                continue
            if team_id <= 0 or pid <= 0:
                continue
            lst = starters.setdefault(team_id, [])
            if pid not in lst:
                lst.append(pid)
        if starters and all(len(v) >= 5 for v in starters.values()):
            return {k: v[:5] for k, v in starters.items()}
    return {k: v[:5] for k, v in starters.items()}


def process_game_possessions(actions: list[dict], game_id: str) -> pd.DataFrame:
    """Process a single game's actions to extract possessions."""
    actions = _normalize_statsv3_actions(actions)
    actions = sorted(actions, key=lambda a: (
        int(a.get("orderNumber") or a.get("actionNumber") or 0),
        int(a.get("actionNumber") or 0)
    ))

    if not actions:
        return pd.DataFrame()

    home_id, away_id = _infer_home_away_from_actions(actions)
    if home_id == 0 or away_id == 0:
        return pd.DataFrame()

    starters = _infer_starters_from_actions(actions)
    if home_id not in starters or away_id not in starters:
        return pd.DataFrame()
    if len(starters.get(home_id, [])) < 5 or len(starters.get(away_id, [])) < 5:
        return pd.DataFrame()

    lineups: dict[int, set[int]] = {
        home_id: set(starters.get(home_id, [])),
        away_id: set(starters.get(away_id, [])),
    }

    possessions: list[dict] = []
    possession_team: int | None = None
    possession_pts: int = 0
    possession_pts_adj: float = 0.0
    last_shot_team: int | None = None
    last_period = 1

    def _close_possession(ended_by: str) -> None:
        nonlocal possession_team, possession_pts, possession_pts_adj
        if possession_team is None:
            return
        if len(lineups.get(home_id, set())) != 5 or len(lineups.get(away_id, set())) != 5:
            possession_pts = 0
            possession_pts_adj = 0.0
            return
        offense_team = possession_team
        defense_team = away_id if offense_team == home_id else home_id
        possessions.append({
            "offense_team": offense_team,
            "defense_team": defense_team,
            "offense_lineup": tuple(sorted(lineups[offense_team])),
            "defense_lineup": tuple(sorted(lineups[defense_team])),
            "points": possession_pts,
            "points_adj": possession_pts_adj,
            "ended_by": ended_by,
            "period": last_period,
        })
        possession_pts = 0
        possession_pts_adj = 0.0

    def _is_last_free_throw(action: dict) -> bool:
        import re
        desc = str(action.get("description") or "")
        match = re.search(r'(\d+)\s+of\s+(\d+)', desc)
        if match:
            current = int(match.group(1))
            total = int(match.group(2))
            return current == total
        return False

    def _get_ft_points(action: dict) -> int:
        desc = str(action.get("description") or "").lower()
        if desc.startswith("miss"):
            return 0
        return 1

    i = 0
    while i < len(actions):
        action = actions[i]
        period = action.get("period")
        if period is not None:
            try:
                last_period = int(period)
            except:
                pass

        action_type = str(action.get("actionType") or "").lower()

        # Handle substitutions
        if action_type == "substitution":
            team_id = action.get("teamId")
            if team_id is None:
                i += 1
                continue
            team_id = int(team_id)
            clock = action.get("clock")
            batch = []
            j = i
            while j < len(actions):
                a = actions[j]
                if str(a.get("actionType") or "").lower() != "substitution":
                    break
                if a.get("teamId") != team_id or a.get("period") != period or a.get("clock") != clock:
                    break
                batch.append(a)
                j += 1

            outs = [b for b in batch if str(b.get("subType") or "").lower() == "out"]
            ins = [b for b in batch if str(b.get("subType") or "").lower() == "in"]

            for b in outs:
                pid = b.get("personId")
                if pid is not None:
                    lineups[team_id].discard(int(pid))

            for b in ins:
                pid = b.get("personId")
                if pid is not None:
                    lineups[team_id].add(int(pid))

            i = j
            continue

        action_team_id = action.get("teamId")
        if action_team_id is not None:
            try:
                action_team_id = int(action_team_id)
            except:
                action_team_id = None

        # Jump ball
        if action_type in ("jumpball", "jump ball"):
            if action_team_id and action_team_id in (home_id, away_id):
                possession_team = action_team_id

        # Period start/end
        elif action_type == "period":
            desc = str(action.get("description") or "").lower()
            if "end" in desc or "start" in desc:
                _close_possession("period")
                possession_team = None

        # Shot attempts
        elif action_type in ("2pt", "3pt", "heave"):
            shot_result = str(action.get("shotResult") or "").lower()
            if action_team_id:
                last_shot_team = action_team_id
                if possession_team is None:
                    possession_team = action_team_id

            if shot_result == "made":
                try:
                    shot_val = int(action.get("shotValue") or (3 if action_type == "3pt" else 2))
                except:
                    shot_val = 3 if action_type == "3pt" else 2
                possession_pts += shot_val
                possession_pts_adj += shot_val
                _close_possession("made_shot")
                possession_team = away_id if action_team_id == home_id else home_id

        # Rebounds
        elif action_type == "rebound":
            desc = str(action.get("description") or "").lower()
            is_offensive = "offensive" in desc
            is_defensive = "defensive" in desc

            if is_defensive:
                _close_possession("defensive_rebound")
                if action_team_id:
                    possession_team = action_team_id
            elif is_offensive:
                if action_team_id and action_team_id in (home_id, away_id):
                    possession_team = action_team_id
            else:
                if action_team_id and last_shot_team:
                    if action_team_id == last_shot_team:
                        possession_team = action_team_id
                    else:
                        _close_possession("defensive_rebound")
                        possession_team = action_team_id

        # Turnovers
        elif action_type == "turnover":
            _close_possession("turnover")
            if action_team_id == home_id:
                possession_team = away_id
            elif action_team_id == away_id:
                possession_team = home_id

        # Free throws
        elif action_type in ("freethrow", "free throw"):
            ft_pts = _get_ft_points(action)
            if action_team_id:
                if possession_team is None:
                    possession_team = action_team_id
                possession_pts += ft_pts
                possession_pts_adj += ft_pts

            if _is_last_free_throw(action):
                _close_possession("free_throw")
                if action_team_id == home_id:
                    possession_team = away_id
                elif action_team_id == away_id:
                    possession_team = home_id

        # Steals
        elif action_type == "steal":
            if action_team_id:
                _close_possession("steal")
                possession_team = action_team_id

        i += 1

    _close_possession("end_of_game")

    # Build DataFrame
    poss_rows = []
    for idx, p in enumerate(possessions):
        off_lineup = p["offense_lineup"]
        def_lineup = p["defense_lineup"]
        poss_rows.append({
            "game_id": str(game_id).lstrip("0"),
            "poss_index": idx,
            "offense_team": p["offense_team"],
            "defense_team": p["defense_team"],
            "off_p1": off_lineup[0] if len(off_lineup) > 0 else None,
            "off_p2": off_lineup[1] if len(off_lineup) > 1 else None,
            "off_p3": off_lineup[2] if len(off_lineup) > 2 else None,
            "off_p4": off_lineup[3] if len(off_lineup) > 3 else None,
            "off_p5": off_lineup[4] if len(off_lineup) > 4 else None,
            "def_p1": def_lineup[0] if len(def_lineup) > 0 else None,
            "def_p2": def_lineup[1] if len(def_lineup) > 1 else None,
            "def_p3": def_lineup[2] if len(def_lineup) > 2 else None,
            "def_p4": def_lineup[3] if len(def_lineup) > 3 else None,
            "def_p5": def_lineup[4] if len(def_lineup) > 4 else None,
            "points": p["points"],
            "points_adj": round(p["points_adj"], 3),
            "ended_by": p["ended_by"],
            "period": p["period"],
        })

    return pd.DataFrame(poss_rows)


def process_season(year: int) -> pd.DataFrame:
    """Process a full season's PBP file and return possessions DataFrame."""
    pbp_file = PBP_DIR / f"nbastatsv3_{year}.csv"
    if not pbp_file.exists():
        print(f"  PBP file not found: {pbp_file}")
        return pd.DataFrame()

    print(f"  Loading {pbp_file}...")
    df = pd.read_csv(pbp_file)

    # Get unique game IDs
    game_ids = df["gameId"].dropna().unique()
    print(f"  Found {len(game_ids)} games")

    all_possessions = []
    success_count = 0

    for game_id in game_ids:
        game_df = df[df["gameId"] == game_id]
        actions = game_df.to_dict("records")

        poss_df = process_game_possessions(actions, str(game_id))
        if not poss_df.empty:
            all_possessions.append(poss_df)
            success_count += 1

    print(f"  Successfully processed {success_count}/{len(game_ids)} games")

    if all_possessions:
        return pd.concat(all_possessions, ignore_index=True)
    return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(description="Generate possessions from local PBP files")
    parser.add_argument("--year", type=int, help="Single year to process")
    parser.add_argument("--start-year", type=int, default=1996, help="Start year (default: 1996)")
    parser.add_argument("--end-year", type=int, default=2025, help="End year (default: 2025)")
    parser.add_argument("--output", type=str, default="data/possessions_historical.csv",
                        help="Output file path")
    args = parser.parse_args()

    if args.year:
        years = [args.year]
    else:
        years = list(range(args.start_year, args.end_year + 1))

    all_possessions = []

    for year in years:
        print(f"Processing {year}...")
        poss_df = process_season(year)
        if not poss_df.empty:
            poss_df["season_year"] = year
            all_possessions.append(poss_df)
            print(f"  -> {len(poss_df)} possessions")

    if all_possessions:
        combined = pd.concat(all_possessions, ignore_index=True)
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(output_path, index=False)
        print(f"\nWrote: {output_path} ({len(combined)} total possessions)")
    else:
        print("\nNo possessions generated.")


if __name__ == "__main__":
    main()
