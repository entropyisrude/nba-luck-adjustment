import argparse
import csv
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

import src.ingest as ingest_module
from src.adjust import update_player_state_attempt_decay
from src.ingest import get_player_3pt_df_from_pbp
from src.onoff import compute_adjusted_onoff_for_game
from src.shot_priors import SeasonalShotPriorLookup
from src.state import ensure_players_exist, load_player_state, save_player_state

DATA_DIR = Path("data")
CONFIG_PATH = Path("config.yaml")


def daterange(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def load_starter_overrides(path: Path) -> tuple[dict[str, dict[int, list[int]]], dict[str, dict[int, dict[int, list[int]]]], dict[str, dict[int, dict[int, list[int]]]]]:
    starter_overrides: dict[str, dict[int, list[int]]] = {}
    period_start_overrides: dict[str, dict[int, dict[int, list[int]]]] = {}
    elapsed_lineup_overrides: dict[str, dict[int, dict[int, list[int]]]] = {}

    def _int(raw: object, default: int = 0) -> int:
        try:
            if raw in (None, "", "nan"):
                return default
            return int(float(raw))
        except Exception:
            return default

    def starter_list(row: dict[str, str], prefix: str) -> list[int]:
        values: list[int] = []
        for c in [f"{prefix}_p1", f"{prefix}_p2", f"{prefix}_p3", f"{prefix}_p4", f"{prefix}_p5"]:
            pid = _int(row.get(c), 0)
            if pid > 0 and pid not in values:
                values.append(pid)
        return values

    game_stats: dict[str, dict[str, object]] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            game_id = str(row.get("game_id") or "").lstrip("0")
            if not game_id:
                continue

            home_id = _int(row.get("home_id"), 0)
            away_id = _int(row.get("away_id"), 0)
            start_period = _int(row.get("start_period"), 0)
            end_period = _int(row.get("end_period"), 0)
            start_elapsed = _int(row.get("start_elapsed"), 0)
            seconds = float(row.get("seconds") or 0.0)
            home_pts = float(row.get("home_pts") or 0.0)
            away_pts = float(row.get("away_pts") or 0.0)
            home_starters = starter_list(row, "home")
            away_starters = starter_list(row, "away")
            complete_lineup = len(home_starters) == 5 and len(away_starters) == 5

            gs = game_stats.setdefault(
                game_id,
                {
                    "max_period": 4,
                    "total_seconds": 0.0,
                    "neg_pts": False,
                    "complete_lineups": True,
                },
            )
            gs["max_period"] = max(int(gs["max_period"]), end_period or 4)
            gs["total_seconds"] = float(gs["total_seconds"]) + seconds
            gs["neg_pts"] = bool(gs["neg_pts"]) or home_pts < 0 or away_pts < 0
            gs["complete_lineups"] = bool(gs["complete_lineups"]) and complete_lineup

            if game_id not in starter_overrides and complete_lineup and home_id > 0 and away_id > 0:
                starter_overrides[game_id] = {
                    home_id: home_starters,
                    away_id: away_starters,
                }

            if complete_lineup and home_id > 0 and away_id > 0 and start_period > 0:
                game_periods = period_start_overrides.setdefault(game_id, {})
                game_periods.setdefault(
                    start_period,
                    {
                        home_id: home_starters,
                        away_id: away_starters,
                    },
                )
                if start_elapsed > 0:
                    elapsed_overrides = elapsed_lineup_overrides.setdefault(game_id, {})
                    elapsed_overrides.setdefault(
                        start_elapsed,
                        {
                            home_id: home_starters,
                            away_id: away_starters,
                        },
                    )

    trusted_period_games: set[str] = set()
    for game_id, gs in game_stats.items():
        expected_seconds = 2880 + max(0, int(gs["max_period"]) - 4) * 300
        if (
            not bool(gs["neg_pts"])
            and bool(gs["complete_lineups"])
            and abs(float(gs["total_seconds"]) - expected_seconds) <= 5.0
        ):
            trusted_period_games.add(game_id)

    period_start_overrides = {
        game_id: periods for game_id, periods in period_start_overrides.items() if game_id in trusted_period_games
    }
    elapsed_lineup_overrides = {
        game_id: elapseds for game_id, elapseds in elapsed_lineup_overrides.items() if game_id in trusted_period_games
    }

    return starter_overrides, period_start_overrides, elapsed_lineup_overrides


def load_game_dates(path: Path) -> dict[str, str]:
    df = pd.read_csv(path, usecols=["game_id", "date"], dtype={"game_id": str})
    df["game_id"] = df["game_id"].astype(str).str.lstrip("0")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()
    df = df.sort_values(["game_id", "date"]).drop_duplicates(subset=["game_id"], keep="last")
    return {row["game_id"]: row["date"].date().isoformat() for _, row in df.iterrows()}


def append_csv(path: Path, df: pd.DataFrame) -> None:
    if df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    df.to_csv(path, mode="a", header=write_header, index=False)


def main():
    parser = argparse.ArgumentParser(description="Rebuild historical regular-season on/off, stints, and possessions from local PBP.")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD (ET)")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (ET)")
    parser.add_argument("--pbp-dir", required=True, help="Directory containing nbastatsv3_<year>.csv files")
    parser.add_argument("--state-in", default="data/player_state_historical.csv", help="Input player state CSV")
    parser.add_argument("--state-out", default="data/player_state_historical.csv", help="Output player state CSV")
    parser.add_argument("--onoff-out", default="data/adjusted_onoff_historical_rebuilt.csv", help="Output on/off CSV")
    parser.add_argument("--secondary-onoff-out", default=None, help="Optional secondary on/off CSV emitted from the same replay pass")
    parser.add_argument("--stints-out", default="data/stints_historical_rebuilt.csv", help="Output stints CSV")
    parser.add_argument("--possessions-out", default="data/possessions_historical_rebuilt.csv", help="Output possessions CSV")
    parser.add_argument("--starter-overrides-path", default="data/stints_historical.csv", help="Trusted stint CSV used only for opening-lineup overrides")
    parser.add_argument("--game-dates-path", default="data/stints_historical.csv", help="CSV used only for authoritative game_id -> date mapping")
    parser.add_argument("--disable-lineup-overrides", action="store_true", help="Ignore historical stint-based lineup overrides and rebuild from pure PBP")
    parser.add_argument("--season", type=str, default=None, help="Override season detection")
    parser.add_argument("--recompute-existing", action="store_true")
    parser.add_argument("--use-game-rotation", action="store_true", help="Use official GameRotation intervals for lineup truth")
    parser.add_argument("--stats-cache-only", action="store_true", help="Refuse live stats API fetches; require cached boxscore/summary/gamerotation data")
    parser.add_argument("--shot-priors-dir", default=None, help="Optional directory of season-scoped shot priors (vwd_priors_<year>.csv)")
    parser.add_argument("--secondary-shot-priors-dir", default=None, help="Optional secondary season-scoped shot priors dir for a second on/off output")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    ingest_module.LOCAL_PBP_DIR = Path(args.pbp_dir)
    if args.stats_cache_only:
        os.environ["NBA_STATS_CACHE_ONLY"] = "1"

    state_path_in = Path(args.state_in)
    state_path_out = Path(args.state_out)
    onoff_out = Path(args.onoff_out)
    secondary_onoff_out = Path(args.secondary_onoff_out) if args.secondary_onoff_out else None
    stints_out = Path(args.stints_out)
    possessions_out = Path(args.possessions_out)
    starter_override_path = Path(args.starter_overrides_path)
    game_dates_path = Path(args.game_dates_path)

    player_state = load_player_state(state_path_in)
    existing = pd.read_csv(onoff_out) if onoff_out.exists() else pd.DataFrame()
    if not existing.empty and "game_id" in existing.columns:
        existing["game_id"] = existing["game_id"].astype(str).str.lstrip("0")
    processed_games = set(existing["game_id"].unique().tolist()) if not existing.empty else set()

    starter_overrides = {}
    period_start_overrides = {}
    elapsed_lineup_overrides = {}
    game_date_map: dict[str, str] = {}
    if (not args.disable_lineup_overrides) and starter_override_path.exists():
        starter_overrides, period_start_overrides, elapsed_lineup_overrides = load_starter_overrides(starter_override_path)
    if game_dates_path.exists():
        game_date_map = load_game_dates(game_dates_path)
    shot_prior_lookup = SeasonalShotPriorLookup(args.shot_priors_dir)
    secondary_shot_prior_lookup = SeasonalShotPriorLookup(args.secondary_shot_priors_dir)

    updated_game_ids: set[str] = set()

    by_date: dict[str, list[str]] = {}
    for game_id, iso_date in game_date_map.items():
        if start.isoformat() <= iso_date <= end.isoformat():
            by_date.setdefault(iso_date, []).append(game_id)

    for iso_date in sorted(by_date):
        d = datetime.strptime(iso_date, "%Y-%m-%d").date()
        game_date_mmddyyyy = d.strftime("%m/%d/%Y")
        game_ids = sorted(by_date[iso_date])
        print("DATE", iso_date, "NBA_DATA_DATE", game_date_mmddyyyy, "GAMES", len(game_ids), "(Regular Season)")
        rows = []
        secondary_rows = []
        stint_rows = []
        poss_rows = []
        date_updated_game_ids: set[str] = set()

        for game_id in game_ids:
            game_id_norm = str(game_id).lstrip("0")
            if (not args.recompute_existing) and game_id_norm in processed_games:
                print("SKIP (already processed)", game_id_norm)
                continue
            try:
                player_df = get_player_3pt_df_from_pbp(game_id, game_date_mmddyyyy)
                if not player_df.empty:
                    player_state = ensure_players_exist(player_state, player_df)

                onoff_df, stint_df, poss_df = compute_adjusted_onoff_for_game(
                    game_id=game_id,
                    game_date_mmddyyyy=game_date_mmddyyyy,
                    player_state=player_state,
                    orb_rate=float(cfg["orb_rate"]),
                    ppp=float(cfg["ppp"]),
                    expected_3p_probs=shot_prior_lookup.get_game_priors(game_id_norm, game_date_mmddyyyy),
                    starters_override=starter_overrides.get(game_id_norm),
                    period_start_overrides=period_start_overrides.get(game_id_norm),
                    elapsed_lineup_overrides=elapsed_lineup_overrides.get(game_id_norm),
                    use_game_rotation=bool(args.use_game_rotation),
                )
                secondary_onoff_df = pd.DataFrame()
                if secondary_onoff_out is not None:
                    secondary_onoff_df, _, _ = compute_adjusted_onoff_for_game(
                        game_id=game_id,
                        game_date_mmddyyyy=game_date_mmddyyyy,
                        player_state=player_state,
                        orb_rate=float(cfg["orb_rate"]),
                        ppp=float(cfg["ppp"]),
                        expected_3p_probs=secondary_shot_prior_lookup.get_game_priors(game_id_norm, game_date_mmddyyyy),
                        starters_override=starter_overrides.get(game_id_norm),
                        period_start_overrides=period_start_overrides.get(game_id_norm),
                        elapsed_lineup_overrides=elapsed_lineup_overrides.get(game_id_norm),
                        use_game_rotation=bool(args.use_game_rotation),
                    )
                if onoff_df.empty:
                    print("SKIP (no on/off rows)", game_id_norm)
                else:
                    onoff_df["date"] = d.isoformat()
                    rows.append(onoff_df)
                    if not secondary_onoff_df.empty:
                        secondary_onoff_df["date"] = d.isoformat()
                        secondary_rows.append(secondary_onoff_df)
                    updated_game_ids.add(game_id_norm)
                    date_updated_game_ids.add(game_id_norm)
                    if not stint_df.empty:
                        stint_df["date"] = d.isoformat()
                        stint_rows.append(stint_df)
                    if not poss_df.empty:
                        poss_df["date"] = d.isoformat()
                        poss_rows.append(poss_df)

                if not player_df.empty:
                    player_state = update_player_state_attempt_decay(
                        player_df=player_df,
                        player_state=player_state,
                        half_life_3pa=float(cfg["half_life_3pa"]),
                    )
            except Exception as exc:
                print("ERROR processing game", game_id_norm, "->", repr(exc))

        if rows:
            new_df = pd.concat(rows, ignore_index=True)
            new_df = new_df.sort_values(["date", "game_id", "team_id", "player_name"])
            append_csv(onoff_out, new_df)
            processed_games.update(date_updated_game_ids)
            print(f"Appended: {onoff_out} (rows+={len(new_df)})")

        if secondary_rows and secondary_onoff_out is not None:
            secondary_df = pd.concat(secondary_rows, ignore_index=True)
            secondary_df = secondary_df.sort_values(["date", "game_id", "team_id", "player_name"])
            append_csv(secondary_onoff_out, secondary_df)
            print(f"Appended: {secondary_onoff_out} (rows+={len(secondary_df)})")

        if stint_rows:
            stint_combined = pd.concat(stint_rows, ignore_index=True)
            stint_combined = stint_combined.sort_values(["date", "game_id", "stint_index"])
            append_csv(stints_out, stint_combined)
            print(f"Appended: {stints_out} (stints+={len(stint_combined)})")

        if poss_rows:
            poss_combined = pd.concat(poss_rows, ignore_index=True)
            poss_combined = poss_combined.sort_values(["date", "game_id", "poss_index"])
            append_csv(possessions_out, poss_combined)
            print(f"Appended: {possessions_out} (possessions+={len(poss_combined)})")

        save_player_state(player_state, state_path_out)
        print(f"Wrote: {state_path_out} (players={len(player_state)})")


if __name__ == "__main__":
    main()
