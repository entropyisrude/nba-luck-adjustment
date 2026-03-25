from __future__ import annotations

import argparse
import sys
from pathlib import Path
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

from scripts.build_playoff_possessions import load_lineup_overrides
from src.onoff import compute_adjusted_onoff_for_game
from src.state import load_player_state


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild possessions for selected playoff games and merge them.")
    parser.add_argument("--game-id", action="append", default=[], help="Playoff game id to rebuild. Repeatable.")
    parser.add_argument("--game-file", help="Text file with one game_id per line.")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit after loading ids.")
    parser.add_argument("--output", default=str(DATA_DIR / "possessions_playoffs.csv"))
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--min-row-retention", type=float, default=0.9)
    return parser.parse_args()


def load_game_ids(args: argparse.Namespace) -> list[str]:
    game_ids = [str(g).lstrip("0") for g in args.game_id if str(g).strip()]
    if args.game_file:
        for line in Path(args.game_file).read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                game_ids.append(line.lstrip("0"))
    deduped: list[str] = []
    seen: set[str] = set()
    for gid in game_ids:
        if gid and gid not in seen:
            seen.add(gid)
            deduped.append(gid)
    if args.limit is not None:
        deduped = deduped[: args.limit]
    return deduped


def main() -> None:
    args = parse_args()
    game_ids = load_game_ids(args)
    if not game_ids:
        raise SystemExit("no game ids provided")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    onoff = pd.read_csv(DATA_DIR / "adjusted_onoff_playoffs.csv", dtype={"game_id": str}, low_memory=False)
    game_dates = (
        onoff[["game_id", "date"]]
        .dropna()
        .assign(game_id=lambda d: d["game_id"].astype(str).str.lstrip("0"))
        .drop_duplicates(subset=["game_id"], keep="first")
        .set_index("game_id")["date"]
        .to_dict()
    )

    starter_overrides, period_start_overrides, elapsed_lineup_overrides = load_lineup_overrides(
        DATA_DIR / "stints_playoffs.csv"
    )
    player_state = load_player_state(DATA_DIR / "player_state_playoffs.csv")
    poss_frames: list[pd.DataFrame] = []
    failures: list[tuple[str, str]] = []

    for idx, gid in enumerate(game_ids, start=1):
        date_iso = game_dates.get(gid)
        print(f"[{idx}/{len(game_ids)}] rebuilding playoff game {gid}", flush=True)
        if not date_iso:
            failures.append((gid, "missing_date"))
            print(f"[{idx}/{len(game_ids)}] {gid} failed: missing_date", flush=True)
            continue
        mmddyyyy = pd.to_datetime(date_iso).strftime("%m/%d/%Y")
        try:
            _, _, poss_df = compute_adjusted_onoff_for_game(
                game_id=gid,
                game_date_mmddyyyy=mmddyyyy,
                player_state=player_state,
                orb_rate=float(cfg["orb_rate"]),
                ppp=float(cfg["ppp"]),
                expected_3p_probs=None,
                starters_override=starter_overrides.get(gid),
                period_start_overrides=period_start_overrides.get(gid),
                elapsed_lineup_overrides=elapsed_lineup_overrides.get(gid),
                force_elapsed_lineup_overrides=True,
            )
            if poss_df.empty:
                failures.append((gid, "empty_possessions"))
                print(f"[{idx}/{len(game_ids)}] {gid} failed: empty_possessions", flush=True)
                continue
            poss_df["date"] = date_iso
            poss_frames.append(poss_df)
            print(
                f"[{idx}/{len(game_ids)}] {gid} built {len(poss_df)} possessions; "
                f"totals built={len(poss_frames)} failures={len(failures)}",
                flush=True,
            )
        except Exception as exc:
            failures.append((gid, str(exc)))
            print(f"[{idx}/{len(game_ids)}] {gid} failed: {exc}", flush=True)
        if idx % 10 == 0 or idx == len(game_ids):
            print(
                f"processed {idx}/{len(game_ids)} selected playoff games; "
                f"built={len(poss_frames)} failures={len(failures)}",
                flush=True,
            )

    if not poss_frames:
        raise SystemExit("no possessions rebuilt")

    rebuilt = pd.concat(poss_frames, ignore_index=True)
    rebuilt["game_id"] = rebuilt["game_id"].astype(str).str.lstrip("0")
    output_path = Path(args.output)
    existing = pd.read_csv(output_path, dtype={"game_id": str}, low_memory=False)
    existing["game_id"] = existing["game_id"].astype(str).str.lstrip("0")
    merged = pd.concat([existing[~existing["game_id"].isin(set(game_ids))], rebuilt], ignore_index=True)
    merged = merged.sort_values(["date", "game_id", "poss_index"]).reset_index(drop=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    backup_path = output_path.with_suffix(output_path.suffix + ".bak")
    merged.to_csv(tmp_path, index=False)
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

    if failures:
        fail_path = output_path.with_name("possessions_playoffs_backfill_failures.csv")
        pd.DataFrame(failures, columns=["game_id", "error"]).to_csv(fail_path, index=False)
        print(f"wrote failures to {fail_path}", flush=True)

    print(f"merged {len(poss_frames)} rebuilt playoff games into {output_path}", flush=True)


if __name__ == "__main__":
    main()
