from __future__ import annotations

import sys
import argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import shutil
import csv

import pandas as pd
import yaml


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
DATA_DIR = ROOT / "data"
CONFIG_PATH = ROOT / "config.yaml"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.onoff import compute_adjusted_onoff_for_game
from src.state import load_player_state


ORB_RATE: float | None = None
PPP: float | None = None
PLAYER_STATE_DF: pd.DataFrame | None = None
STARTER_OVERRIDES_G: dict[str, dict[int, list[int]]] = {}
PERIOD_START_OVERRIDES_G: dict[str, dict[int, dict[int, list[int]]]] = {}
ELAPSED_LINEUP_OVERRIDES_G: dict[str, dict[int, dict[int, list[int]]]] = {}


def csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        next(reader, None)
        for _ in reader:
            count += 1
    return count


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


def load_lineup_overrides(starter_override_path: Path) -> tuple[dict[str, dict[int, list[int]]], dict[str, dict[int, dict[int, list[int]]]], dict[str, dict[int, dict[int, list[int]]]]]:
    starter_overrides: dict[str, dict[int, list[int]]] = {}
    period_start_overrides: dict[str, dict[int, dict[int, list[int]]]] = {}
    elapsed_lineup_overrides: dict[str, dict[int, dict[int, list[int]]]] = {}
    if not starter_override_path.exists():
        return starter_overrides, period_start_overrides, elapsed_lineup_overrides

    existing_starts = pd.read_csv(starter_override_path, dtype={"game_id": str}, low_memory=False)
    if "game_id" not in existing_starts.columns:
        return starter_overrides, period_start_overrides, elapsed_lineup_overrides

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
        gid = str(getattr(row, "game_id"))
        if gid not in trusted_period_games:
            continue
        try:
            home_id = int(getattr(row, "home_id"))
            away_id = int(getattr(row, "away_id"))
            period = int(getattr(row, "start_period"))
            elapsed = int(getattr(row, "start_elapsed"))
        except Exception:
            continue
        home_lineup = _starter_list(row, "home")
        away_lineup = _starter_list(row, "away")
        if len(home_lineup) != 5 or len(away_lineup) != 5:
            continue
        game_periods = period_start_overrides.setdefault(gid, {})
        game_periods.setdefault(period, {home_id: home_lineup, away_id: away_lineup})
        if elapsed > 0:
            elapsed_overrides = elapsed_lineup_overrides.setdefault(gid, {})
            elapsed_overrides.setdefault(elapsed, {home_id: home_lineup, away_id: away_lineup})

    return starter_overrides, period_start_overrides, elapsed_lineup_overrides


def _init_worker(
    orb_rate: float,
    ppp: float,
    state_path: str,
    starter_overrides: dict[str, dict[int, list[int]]],
    period_start_overrides: dict[str, dict[int, dict[int, list[int]]]],
    elapsed_lineup_overrides: dict[str, dict[int, dict[int, list[int]]]],
) -> None:
    global ORB_RATE, PPP, PLAYER_STATE_DF, STARTER_OVERRIDES_G, PERIOD_START_OVERRIDES_G, ELAPSED_LINEUP_OVERRIDES_G
    ORB_RATE = orb_rate
    PPP = ppp
    PLAYER_STATE_DF = load_player_state(Path(state_path))
    STARTER_OVERRIDES_G = starter_overrides
    PERIOD_START_OVERRIDES_G = period_start_overrides
    ELAPSED_LINEUP_OVERRIDES_G = elapsed_lineup_overrides


def _build_game_possessions(task: tuple[str, str]) -> tuple[str, pd.DataFrame | None, str | None]:
    gid, date_iso = task
    mmddyyyy = pd.to_datetime(date_iso).strftime("%m/%d/%Y")
    try:
        _, _, poss_df = compute_adjusted_onoff_for_game(
            game_id=gid,
            game_date_mmddyyyy=mmddyyyy,
            player_state=PLAYER_STATE_DF if PLAYER_STATE_DF is not None else pd.DataFrame(),
            orb_rate=float(ORB_RATE or 0.0),
            ppp=float(PPP or 0.0),
            expected_3p_probs=None,
            starters_override=STARTER_OVERRIDES_G.get(gid),
            period_start_overrides=PERIOD_START_OVERRIDES_G.get(gid),
            elapsed_lineup_overrides=ELAPSED_LINEUP_OVERRIDES_G.get(gid),
            force_elapsed_lineup_overrides=True,
        )
        if poss_df.empty:
            return gid, None, "empty_possessions"
        poss_df["date"] = date_iso
        return gid, poss_df, None
    except Exception as e:
        return gid, None, str(e)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build possessions_playoffs.csv from existing playoff game IDs")
    parser.add_argument("--output", default=str(DATA_DIR / "possessions_playoffs.csv"))
    parser.add_argument("--state-path", default=str(DATA_DIR / "player_state_playoffs.csv"))
    parser.add_argument("--games-limit", type=int, default=None)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--min-row-retention", type=float, default=0.9)
    args = parser.parse_args()

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    onoff_path = DATA_DIR / "adjusted_onoff_playoffs.csv"
    stints_path = DATA_DIR / "stints_playoffs.csv"
    existing = pd.read_csv(onoff_path, dtype={"game_id": str})
    games = (
        existing[["game_id", "date"]]
        .dropna()
        .assign(game_id=lambda d: d["game_id"].astype(str).str.lstrip("0"))
        .drop_duplicates()
        .sort_values(["date", "game_id"])
    )
    if args.games_limit:
        games = games.head(args.games_limit)
    print(f"starting playoff possession build for {len(games)} games", flush=True)

    starter_overrides, period_start_overrides, elapsed_lineup_overrides = load_lineup_overrides(stints_path)
    poss_frames: list[pd.DataFrame] = []
    failures: list[tuple[str, str]] = []
    total = len(games)
    tasks = [(str(row.game_id), str(row.date)) for row in games.itertuples(index=False)]
    if args.workers <= 1:
        _init_worker(
            float(cfg["orb_rate"]),
            float(cfg["ppp"]),
            str(args.state_path),
            starter_overrides,
            period_start_overrides,
            elapsed_lineup_overrides,
        )
        for idx, task in enumerate(tasks, start=1):
            gid, poss_df, err = _build_game_possessions(task)
            if err is None and poss_df is not None:
                poss_frames.append(poss_df)
            else:
                failures.append((gid, err or "unknown_error"))
            if idx % 10 == 0 or idx == total:
                print(f"processed {idx}/{total} games; built={len(poss_frames)} failures={len(failures)}", flush=True)
    else:
        with ProcessPoolExecutor(
            max_workers=args.workers,
            initializer=_init_worker,
            initargs=(
                float(cfg["orb_rate"]),
                float(cfg["ppp"]),
                str(args.state_path),
                starter_overrides,
                period_start_overrides,
                elapsed_lineup_overrides,
            ),
        ) as ex:
            futures = {ex.submit(_build_game_possessions, task): task[0] for task in tasks}
            done = 0
            for fut in as_completed(futures):
                done += 1
                gid, poss_df, err = fut.result()
                if err is None and poss_df is not None:
                    poss_frames.append(poss_df)
                else:
                    failures.append((gid, err or "unknown_error"))
                if done % 10 == 0 or done == total:
                    print(f"processed {done}/{total} games; built={len(poss_frames)} failures={len(failures)}", flush=True)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if poss_frames:
        combined = pd.concat(poss_frames, ignore_index=True)
        if "game_id" in combined.columns:
            combined["game_id"] = combined["game_id"].astype(str).str.lstrip("0")
        tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        backup_path = output_path.with_suffix(output_path.suffix + ".bak")
        combined.to_csv(tmp_path, index=False)
        old_rows = csv_row_count(output_path)
        new_rows = csv_row_count(tmp_path)
        if (
            old_rows > 0
            and not args.allow_shrink
            and new_rows < int(old_rows * float(args.min_row_retention))
        ):
            raise SystemExit(
                f"Refusing to replace {output_path}: new_rows={new_rows} is below retention threshold "
                f"for old_rows={old_rows}. Use --allow-shrink to override."
            )
        if output_path.exists():
            shutil.copy2(output_path, backup_path)
        os.replace(tmp_path, output_path)
        print(f"Wrote {output_path} rows={len(combined)} games={combined['game_id'].nunique() if 'game_id' in combined.columns else 0}", flush=True)
    else:
        print("No possession rows built", flush=True)

    if failures:
        fail_path = output_path.with_name("possessions_playoffs_failures.csv")
        pd.DataFrame(failures, columns=["game_id", "error"]).to_csv(fail_path, index=False)
        print(f"Wrote failures to {fail_path} count={len(failures)}", flush=True)


if __name__ == "__main__":
    main()
