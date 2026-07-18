"""
Fix possession undercount in possessions_historical_pbp.csv.

Identifies games with fewer than MIN_POSS possessions (caused by incomplete CDN
PBP data fetched during or immediately after a live game, then permanently cached
by the skip logic), re-fetches their PBP from CDN, and patches the three
historical CSV files in place.

Run from the repo root:
    python scripts/fix_possession_undercount.py [--min-poss N] [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd
import yaml

from src.onoff import compute_adjusted_onoff_for_game
from src.shot_priors import SeasonalShotPriorLookup
from src.state import load_player_state

DATA_DIR = ROOT / "data"
POSS_PATH = DATA_DIR / "possessions_historical_pbp.csv"
ONOFF_PATH = DATA_DIR / "adjusted_onoff_historical_pbp.csv"
STINTS_PATH = DATA_DIR / "stints_historical_pbp.csv"
STATE_PATH = DATA_DIR / "player_state_historical_pbp.csv"
CONFIG_PATH = ROOT / "config.yaml"
SHOT_PRIORS_DIR = DATA_DIR / "vwd_priors_by_season"

# Only check seasons with known CDN-fetch issues (no rebuilt possessions data).
RECENT_GAME_ID_PREFIXES = ("22400", "22500")


def identify_bad_games(min_poss: int) -> dict[str, str]:
    """Return {game_id: iso_date} for games with < min_poss possessions."""
    print(f"Loading {POSS_PATH.name} ...", flush=True)
    poss = pd.read_csv(POSS_PATH, dtype={"game_id": str}, usecols=["game_id", "poss_index"])

    recent_mask = poss["game_id"].str.startswith(RECENT_GAME_ID_PREFIXES)
    recent = poss[recent_mask]
    game_max = recent.groupby("game_id")["poss_index"].max().reset_index()
    game_max["total_poss"] = game_max["poss_index"] + 1
    bad_ids = set(game_max.loc[game_max["total_poss"] < min_poss, "game_id"])
    print(f"  {len(bad_ids)} games with < {min_poss} possessions", flush=True)

    print(f"Loading {ONOFF_PATH.name} for dates ...", flush=True)
    onoff = pd.read_csv(ONOFF_PATH, dtype={"game_id": str}, usecols=["game_id", "date"])
    onoff["game_id"] = onoff["game_id"].str.lstrip("0")
    game_dates = (
        onoff[onoff["game_id"].isin(bad_ids)]
        .drop_duplicates("game_id")
        .set_index("game_id")["date"]
        .to_dict()
    )
    missing_dates = bad_ids - set(game_dates)
    if missing_dates:
        print(f"  Warning: no date found for {len(missing_dates)} game(s): {sorted(missing_dates)[:5]}")
    return game_dates


def recompute_games(
    game_dates: dict[str, str],
    cfg: dict,
    player_state: pd.DataFrame,
    shot_prior_lookup: SeasonalShotPriorLookup,
) -> tuple[list[pd.DataFrame], list[pd.DataFrame], list[pd.DataFrame], set[str]]:
    """Reprocess each game; return (onoff_rows, stint_rows, poss_rows, processed_ids)."""
    new_onoff: list[pd.DataFrame] = []
    new_stints: list[pd.DataFrame] = []
    new_poss: list[pd.DataFrame] = []
    processed: set[str] = set()

    total = len(game_dates)
    for i, (game_id, iso_date) in enumerate(sorted(game_dates.items(), key=lambda x: x[1]), 1):
        mm, dd, yyyy = iso_date[5:7], iso_date[8:10], iso_date[0:4]
        game_date_mmddyyyy = f"{mm}/{dd}/{yyyy}"
        gid_norm = game_id.zfill(10)

        print(f"[{i}/{total}] {game_id} ({iso_date}) ...", flush=True, end=" ")
        try:
            onoff_df, stint_df, poss_df = compute_adjusted_onoff_for_game(
                game_id=gid_norm,
                game_date_mmddyyyy=game_date_mmddyyyy,
                player_state=player_state,
                orb_rate=float(cfg["orb_rate"]),
                ppp=float(cfg["ppp"]),
                expected_3p_probs=shot_prior_lookup.get_game_priors(game_id, game_date_mmddyyyy),
            )
        except Exception as e:
            print(f"ERROR: {e}", flush=True)
            continue

        if onoff_df.empty:
            print("SKIP (no on/off rows)", flush=True)
            continue

        n_poss = len(poss_df)
        print(f"{n_poss} possessions", flush=True)

        onoff_df["date"] = iso_date
        new_onoff.append(onoff_df)
        if not stint_df.empty:
            stint_df["date"] = iso_date
            new_stints.append(stint_df)
        if not poss_df.empty:
            poss_df["date"] = iso_date
            new_poss.append(poss_df)
        processed.add(game_id)

    return new_onoff, new_stints, new_poss, processed


def patch_possessions(new_rows: list[pd.DataFrame], processed_ids: set[str]) -> None:
    print(f"\nPatching {POSS_PATH.name} ...", flush=True)
    poss = pd.read_csv(POSS_PATH, dtype={"game_id": str})
    poss_kept = poss[~poss["game_id"].isin(processed_ids)]
    poss_new = pd.concat(new_rows, ignore_index=True)
    combined = pd.concat([poss_kept, poss_new], ignore_index=True)
    if "poss_index" in combined.columns:
        combined = combined.drop_duplicates(subset=["game_id", "poss_index"], keep="last")
    combined = combined.sort_values(["date", "game_id", "poss_index"])
    combined.to_csv(POSS_PATH, index=False)
    print(f"  {len(poss_kept)} kept + {len(poss_new)} new = {len(combined)} total rows")


def patch_onoff(new_rows: list[pd.DataFrame], processed_ids: set[str]) -> None:
    print(f"Patching {ONOFF_PATH.name} ...", flush=True)
    onoff = pd.read_csv(ONOFF_PATH, dtype={"game_id": str})
    onoff["game_id"] = onoff["game_id"].str.lstrip("0")
    onoff_kept = onoff[~onoff["game_id"].isin(processed_ids)]
    onoff_new = pd.concat(new_rows, ignore_index=True)
    combined = pd.concat([onoff_kept, onoff_new], ignore_index=True)
    combined = (
        combined.drop_duplicates(subset=["game_id", "player_id"], keep="last")
        .sort_values(["date", "game_id", "player_name"])
    )
    combined.to_csv(ONOFF_PATH, index=False)
    print(f"  {len(onoff_kept)} kept + {len(onoff_new)} new = {len(combined)} total rows")


def patch_stints(new_rows: list[pd.DataFrame], processed_ids: set[str]) -> None:
    if not STINTS_PATH.exists():
        print(f"  Skipping stints (file not found: {STINTS_PATH})")
        return
    print(f"Patching {STINTS_PATH.name} ...", flush=True)
    stints = pd.read_csv(STINTS_PATH, dtype={"game_id": str}, low_memory=False)
    stints["game_id"] = stints["game_id"].str.lstrip("0")
    stints_kept = stints[~stints["game_id"].isin(processed_ids)]
    stints_new = pd.concat(new_rows, ignore_index=True)
    combined = pd.concat([stints_kept, stints_new], ignore_index=True)
    if "stint_index" in combined.columns:
        combined = combined.drop_duplicates(subset=["game_id", "stint_index"], keep="last")
    combined = combined.sort_values(["date", "game_id"])
    combined.to_csv(STINTS_PATH, index=False)
    print(f"  {len(stints_kept)} kept + {len(stints_new)} new = {len(combined)} total rows")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--min-poss", type=int, default=150,
                        help="Reprocess games with fewer than this many possessions (default: 150)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Identify bad games and test recompute but do not write files")
    parser.add_argument("--limit", type=int, default=0,
                        help="Process at most N games (0 = no limit, useful for testing)")
    args = parser.parse_args()

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    player_state = load_player_state(STATE_PATH)
    shot_prior_lookup = SeasonalShotPriorLookup(SHOT_PRIORS_DIR)

    game_dates = identify_bad_games(args.min_poss)
    if not game_dates:
        print("No bad games found — nothing to do.")
        return

    if args.limit > 0:
        game_dates = dict(list(game_dates.items())[:args.limit])
        print(f"--limit {args.limit}: processing first {len(game_dates)} games only")

    print(f"\nReprocessing {len(game_dates)} games (fetching fresh CDN PBP)...\n", flush=True)
    new_onoff, new_stints, new_poss, processed = recompute_games(
        game_dates, cfg, player_state, shot_prior_lookup
    )

    print(f"\nSuccessfully reprocessed {len(processed)} / {len(game_dates)} games")

    if args.dry_run:
        print("--dry-run: skipping file writes")
        if new_poss:
            total_new_poss = sum(len(df) for df in new_poss)
            print(f"  Would write {total_new_poss} possession rows for {len(processed)} games")
        return

    if not processed:
        print("No games successfully reprocessed — nothing to patch.")
        return

    if new_poss:
        patch_possessions(new_poss, processed)
    if new_onoff:
        patch_onoff(new_onoff, processed)
    if new_stints:
        patch_stints(new_stints, processed)

    print("\nDone. Next steps:")
    print("  1. python scripts/build_analytics_duckdb.py")
    print("  2. python scripts/build_player_season_onoff_reference.py")
    print("  3. python generate_player_span_search_report.py")
    print("  4. git add data/player_span_chunks/ data/player_span_search.html")
    print("  5. git commit -m 'Fix: Recompute possessions for 261 CDN-incomplete games'")
    print("  6. git push")


if __name__ == "__main__":
    main()
