import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

from src.ingest import get_game_ids_for_date, get_player_3pt_df_from_pbp
from src.onoff_boxscore import write_player_daily_boxscore
from src.onoff import compute_adjusted_onoff_for_game
from src.onoff_history import write_player_onoff_history
from src.shot_priors import SeasonalShotPriorLookup
from src.state import load_player_state, save_player_state, ensure_players_exist
from src.adjust import update_player_state_attempt_decay

DATA_DIR = Path("data")
CONFIG_PATH = Path("config.yaml")


def daterange(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD (ET)")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (ET)")
    parser.add_argument(
        "--recompute-existing",
        action="store_true",
        help="Recompute games even if game_id already exists in adjusted_onoff.csv",
    )
    parser.add_argument(
        "--starter-overrides-path",
        type=str,
        default=None,
        help="Optional CSV path to trusted stint data used only to seed opening lineups for replayed games.",
    )
    parser.add_argument(
        "--history-season-start",
        default="2025-10-01",
        help="Season start date for player_onoff_history.csv window (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--history-season-end",
        default="2026-06-30",
        help="Season end date for player_onoff_history.csv window (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--skip-history",
        action="store_true",
        help="Skip rebuilding data/player_onoff_history.csv",
    )
    parser.add_argument(
        "--skip-boxscore",
        action="store_true",
        help="Skip rebuilding data/player_daily_boxscore.csv",
    )
    parser.add_argument(
        "--playoffs",
        action="store_true",
        help="Process playoff games instead of regular season (saves to separate files)",
    )
    parser.add_argument(
        "--season",
        type=str,
        default=None,
        help="Override season detection (e.g., '2019-20' for COVID bubble playoffs)",
    )
    parser.add_argument(
        "--shot-priors-dir",
        type=str,
        default=None,
        help="Optional directory of season-scoped shot priors (vwd_priors_<year>.csv).",
    )
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    with open(CONFIG_PATH, "r") as f:
        cfg = yaml.safe_load(f)

    DATA_DIR.mkdir(exist_ok=True)
    season_type = "Playoffs" if args.playoffs else "Regular Season"
    suffix = "_playoffs" if args.playoffs else ""
    out_path = DATA_DIR / f"adjusted_onoff{suffix}.csv"
    stint_path = DATA_DIR / f"stints{suffix}.csv"
    poss_path = DATA_DIR / f"possessions{suffix}.csv"
    state_path = DATA_DIR / f"player_state{suffix}.csv"

    player_state = load_player_state(state_path)
    if out_path.exists():
        existing = pd.read_csv(out_path)
        if "game_id" in existing.columns:
            existing["game_id"] = existing["game_id"].astype(str).str.lstrip("0")
            if "player_id" in existing.columns:
                try:
                    existing["player_id"] = existing["player_id"].astype(int)
                except Exception:
                    pass
        else:
            # Likely an LFS pointer or malformed file; treat as empty.
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()

    processed_games = set(existing["game_id"].unique().tolist()) if not existing.empty else set()
    rows = []
    stint_rows = []
    poss_rows = []
    updated_game_ids: set[str] = set()
    starter_overrides: dict[str, dict[int, list[int]]] = {}
    period_start_overrides: dict[str, dict[int, dict[int, list[int]]]] = {}
    elapsed_lineup_overrides: dict[str, dict[int, dict[int, list[int]]]] = {}
    starter_override_path: Path | None = None
    if args.starter_overrides_path:
        starter_override_path = Path(args.starter_overrides_path)
    elif not args.recompute_existing and stint_path.exists():
        # Avoid circularly reusing a stint file as its own source of truth when
        # recomputing existing games. For normal daily incremental updates this
        # remains a lightweight way to preserve known-good starters.
        starter_override_path = stint_path

    if starter_override_path and starter_override_path.exists():
        try:
            existing_starts = pd.read_csv(starter_override_path, dtype={"game_id": str}, low_memory=False)
            if "game_id" in existing_starts.columns:
                def _starter_list(row, prefix: str) -> list[int]:
                    values: list[int] = []
                    for c in [f"{prefix}_p1", f"{prefix}_p2", f"{prefix}_p3", f"{prefix}_p4", f"{prefix}_p5"]:
                        raw = getattr(row, c, None)
                        if pd.isna(raw):
                            continue
                        try:
                            pid = int(raw)
                        except Exception:
                            continue
                        if pid > 0 and pid not in values:
                            values.append(pid)
                    return values

                existing_starts["game_id"] = existing_starts["game_id"].astype(str).str.lstrip("0")
                if "stint_index" in existing_starts.columns:
                    existing_starts = existing_starts.sort_values(["game_id", "stint_index"])
                trusted_period_games: set[str] = set()
                for game_id, game_df in existing_starts.groupby("game_id"):
                    try:
                        max_period = int(pd.to_numeric(game_df["end_period"], errors="coerce").max())
                    except Exception:
                        max_period = 4
                    expected_seconds = 2880 + max(0, max_period - 4) * 300
                    total_seconds = float(pd.to_numeric(game_df["seconds"], errors="coerce").fillna(0).sum())
                    neg_pts = (
                        pd.to_numeric(game_df.get("home_pts"), errors="coerce").fillna(0).lt(0).any()
                        or pd.to_numeric(game_df.get("away_pts"), errors="coerce").fillna(0).lt(0).any()
                    )
                    complete_lineups = True
                    for prefix in ("home", "away"):
                        cols = [f"{prefix}_p{i}" for i in range(1, 6)]
                        if not set(cols).issubset(game_df.columns):
                            complete_lineups = False
                            break
                        counts = game_df[cols].apply(pd.to_numeric, errors="coerce").fillna(0).gt(0).sum(axis=1)
                        if (counts < 5).any():
                            complete_lineups = False
                            break
                    if not neg_pts and complete_lineups and abs(total_seconds - expected_seconds) <= 5.0:
                        trusted_period_games.add(str(game_id))
                first_rows = existing_starts.groupby("game_id", as_index=False).first()
                for row in first_rows.itertuples():
                    try:
                        home_id = int(getattr(row, "home_id"))
                        away_id = int(getattr(row, "away_id"))
                    except Exception:
                        continue
                    home_starters = _starter_list(row, "home")
                    away_starters = _starter_list(row, "away")
                    if len(home_starters) == 5 and len(away_starters) == 5:
                        starter_overrides[str(getattr(row, "game_id"))] = {
                            home_id: home_starters,
                            away_id: away_starters,
                        }
                for row in existing_starts.itertuples():
                    if str(getattr(row, "game_id")) not in trusted_period_games:
                        continue
                    try:
                        home_id = int(getattr(row, "home_id"))
                        away_id = int(getattr(row, "away_id"))
                        period = int(getattr(row, "start_period"))
                        elapsed = int(getattr(row, "start_elapsed"))
                    except Exception:
                        continue
                    home_starters = _starter_list(row, "home")
                    away_starters = _starter_list(row, "away")
                    if len(home_starters) != 5 or len(away_starters) != 5:
                        continue
                    game_periods = period_start_overrides.setdefault(str(getattr(row, "game_id")), {})
                    # Keep the earliest known lineup for each period.
                    game_periods.setdefault(
                        period,
                        {
                            home_id: home_starters,
                            away_id: away_starters,
                        },
                    )
                    elapsed_overrides = elapsed_lineup_overrides.setdefault(str(getattr(row, "game_id")), {})
                    if elapsed > 0:
                        elapsed_overrides.setdefault(
                            elapsed,
                            {
                                home_id: home_starters,
                                away_id: away_starters,
                            },
                        )
        except Exception as e:
            print(f"Warning: could not load starter overrides from {starter_override_path}: {e}")
    shot_prior_lookup = SeasonalShotPriorLookup(args.shot_priors_dir)

    for d in daterange(start, end):
        game_date_mmddyyyy = d.strftime("%m/%d/%Y")
        game_ids = get_game_ids_for_date(game_date_mmddyyyy, season_type, args.season)

        print("DATE", d.isoformat(), "NBA_DATA_DATE", game_date_mmddyyyy, "GAMES", len(game_ids), f"({season_type})")

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
                )
                if onoff_df.empty:
                    print("SKIP (no on/off rows)", game_id)
                else:
                    onoff_df["date"] = d.isoformat()
                    rows.append(onoff_df)
                    updated_game_ids.add(game_id_norm)
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

            except Exception as e:
                print("ERROR processing game", game_id, "->", repr(e))
                continue

    if rows:
        new_df = pd.concat(rows, ignore_index=True)
        if not existing.empty:
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        combined = (
            combined.drop_duplicates(subset=["game_id", "player_id"], keep="last")
            .sort_values(["date", "game_id", "team_id", "player_name"])
        )
        combined.to_csv(out_path, index=False)
        print(f"Wrote: {out_path} (rows={len(combined)})")
    else:
        if out_path.exists():
            print("No new on/off rows produced; kept existing adjusted_onoff.csv.")
        else:
            print("No on/off rows produced; adjusted_onoff.csv not created.")

    # Save stint data
    if stint_rows:
        stint_combined = pd.concat(stint_rows, ignore_index=True)
        if stint_path.exists():
            existing_stints = pd.read_csv(stint_path, dtype={"game_id": str}, low_memory=False)
            existing_stints["game_id"] = existing_stints["game_id"].astype(str).str.lstrip("0")
            if updated_game_ids:
                existing_stints = existing_stints[~existing_stints["game_id"].isin(updated_game_ids)]
            stint_combined = pd.concat([existing_stints, stint_combined], ignore_index=True)
        if "stint_index" in stint_combined.columns:
            stint_combined = stint_combined.drop_duplicates(subset=["game_id", "stint_index"], keep="last")
        stint_combined = stint_combined.sort_values(["date", "game_id", "stint_index"])
        stint_combined.to_csv(stint_path, index=False)
        print(f"Wrote: {stint_path} (stints={len(stint_combined)})")

    if poss_rows:
        poss_combined = pd.concat(poss_rows, ignore_index=True)
        if poss_path.exists():
            existing_poss = pd.read_csv(poss_path, dtype={"game_id": str}, low_memory=False)
            existing_poss["game_id"] = existing_poss["game_id"].astype(str).str.lstrip("0")
            if updated_game_ids:
                existing_poss = existing_poss[~existing_poss["game_id"].isin(updated_game_ids)]
            poss_combined = pd.concat([existing_poss, poss_combined], ignore_index=True)
        if "poss_index" in poss_combined.columns:
            poss_combined = poss_combined.drop_duplicates(subset=["game_id", "poss_index"], keep="last")
        poss_combined = poss_combined.sort_values(["date", "game_id", "poss_index"])
        poss_combined.to_csv(poss_path, index=False)
        print(f"Wrote: {poss_path} (possessions={len(poss_combined)})")

    save_player_state(player_state, state_path)
    print(f"Wrote: {state_path} (players={len(player_state)})")

    if not args.skip_history and out_path.exists():
        history_path = DATA_DIR / f"player_onoff_history{suffix}.csv"
        hist = write_player_onoff_history(
            input_path=out_path,
            output_path=history_path,
            season_start=args.history_season_start,
            season_end=args.history_season_end,
        )
        print(f"Wrote: {history_path} (players={len(hist)})")

    if not args.skip_boxscore and out_path.exists():
        boxscore_path = DATA_DIR / f"player_daily_boxscore{suffix}.csv"
        box = write_player_daily_boxscore(
            input_path=out_path,
            output_path=boxscore_path,
        )
        print(f"Wrote: {boxscore_path} (rows={len(box)})")


if __name__ == "__main__":
    main()
