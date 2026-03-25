from __future__ import annotations

import argparse
import csv
from pathlib import Path


SKILL_HALF_LIFE = 1200.0
PENALTY_HALF_LIFE = 800.0
ANCHOR_WEIGHT = 150.0
PENALTY_PRIOR_WEIGHT = 250.0
DEFAULT_LEAGUE_AVG = 0.36
DEFAULT_FT_PCT = 0.75


def _parse_int(raw: object, default: int = 0) -> int:
    try:
        if raw in (None, "", "nan"):
            return default
        return int(float(raw))
    except Exception:
        return default


def _parse_float(raw: object, default: float = 0.0) -> float:
    try:
        if raw in (None, "", "nan"):
            return default
        return float(raw)
    except Exception:
        return default


def load_ft_pct_map(totals_dir: Path) -> dict[int, float]:
    ftm_by_player: dict[int, float] = {}
    fta_by_player: dict[int, float] = {}
    for path in sorted(totals_dir.glob("totals_*.csv")):
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pid = _parse_int(row.get("PLAYER_ID"), 0)
                if pid <= 0:
                    continue
                ftm_by_player[pid] = ftm_by_player.get(pid, 0.0) + _parse_float(row.get("FTM"), 0.0)
                fta_by_player[pid] = fta_by_player.get(pid, 0.0) + _parse_float(row.get("FTA"), 0.0)

    out: dict[int, float] = {}
    for pid, fta in fta_by_player.items():
        if fta > 0:
            out[pid] = ftm_by_player.get(pid, 0.0) / fta
    return out


def iter_regular_season_shots(path: Path):
    season_start_year = int(path.stem.split("_")[-1])
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            desc = str(row.get("description") or "")
            desc_lower = desc.lower()
            action_type = str(row.get("actionType") or "").lower()
            shot_value = _parse_int(row.get("shotValue"), 0)
            is_3pt = action_type == "3pt" or "3pt" in desc_lower or "3-pt" in desc_lower or shot_value == 3
            if not is_3pt:
                continue
            game_id = str(row.get("gameId") or row.get("game_id") or "").lstrip("0")
            action_number = _parse_int(row.get("actionNumber") or row.get("actionId"), 0)
            player_id = _parse_int(row.get("personId"), 0)
            if not game_id or action_number <= 0 or player_id <= 0:
                continue
            shot_result = str(row.get("shotResult") or "").lower()
            is_make = shot_result == "made" or (shot_result == "" and not desc_lower.startswith("miss"))
            is_assisted = "assist by" in desc_lower
            yield {
                "season_start_year": season_start_year,
                "game_id": game_id,
                "action_number": action_number,
                "player_id": player_id,
                "is_assisted": 1 if is_assisted else 0,
                "is_make": 1 if is_make else 0,
                "is_playoff": 0,
            }


def iter_playoff_shots(path: Path):
    season_start_year = int(path.stem.split("_")[-1])
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event_type = _parse_int(row.get("EVENTMSGTYPE"), 0)
            if event_type not in (1, 2):
                continue
            desc = " ".join(
                str(row.get(col) or "")
                for col in ("HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION")
            )
            if "3PT" not in desc.upper() and "3-PT" not in desc.upper():
                continue
            game_id = str(row.get("GAME_ID") or "").lstrip("0")
            action_number = _parse_int(row.get("EVENTNUM"), 0)
            player_id = _parse_int(row.get("PLAYER1_ID"), 0)
            if not game_id or action_number <= 0 or player_id <= 0:
                continue
            player2_id = _parse_int(row.get("PLAYER2_ID"), 0)
            yield {
                "season_start_year": season_start_year,
                "game_id": game_id,
                "action_number": action_number,
                "player_id": player_id,
                "is_assisted": 1 if player2_id > 0 else 0,
                "is_make": 1 if event_type == 1 else 0,
                "is_playoff": 1,
            }


def generate_priors(
    regular_pbp_dir: Path,
    playoff_pbp_dir: Path,
    totals_dir: Path,
    output_dir: Path,
    season_start_year: int | None = None,
    season_end_year: int | None = None,
) -> None:
    ft_pct_map = load_ft_pct_map(totals_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    regular_files = {int(p.stem.split("_")[-1]): p for p in regular_pbp_dir.glob("nbastatsv3_*.csv")}
    playoff_files = {int(p.stem.split("_")[-1]): p for p in playoff_pbp_dir.glob("nbastats_po_*.csv")}
    season_years = sorted(set(regular_files) | set(playoff_files))
    if season_start_year is not None:
        season_years = [y for y in season_years if y >= season_start_year]
    if season_end_year is not None:
        season_years = [y for y in season_years if y <= season_end_year]

    decay_skill = 0.5 ** (1.0 / SKILL_HALF_LIFE)
    decay_penalty = 0.5 ** (1.0 / PENALTY_HALF_LIFE)
    player_state: dict[int, dict[str, float]] = {}

    for season_start_year in season_years:
        season_rows: list[dict[str, object]] = []
        regular_path = regular_files.get(season_start_year)
        playoff_path = playoff_files.get(season_start_year)
        if regular_path is not None:
            season_rows.extend(iter_regular_season_shots(regular_path))
        if playoff_path is not None:
            season_rows.extend(iter_playoff_shots(playoff_path))

        season_rows.sort(key=lambda row: (int(row["game_id"]), int(row["action_number"])))
        out_path = output_dir / f"vwd_priors_{season_start_year}.csv"

        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "game_id",
                    "action_number",
                    "player_id",
                    "is_assisted",
                    "is_make",
                    "expected_3p_prob",
                ],
            )
            writer.writeheader()

            for row in season_rows:
                pid = int(row["player_id"])
                is_assisted = int(row["is_assisted"]) == 1
                is_make = int(row["is_make"]) == 1
                is_playoff = int(row["is_playoff"]) == 1

                if pid not in player_state:
                    ft_pct = ft_pct_map.get(pid, DEFAULT_FT_PCT)
                    rookie_mu = DEFAULT_LEAGUE_AVG - 0.02
                    ft_anchor = (ft_pct * 0.50) - 0.02
                    initial_skill = (rookie_mu + ft_anchor) / 2.0
                    player_state[pid] = {
                        "skill_makes": initial_skill * (ANCHOR_WEIGHT * 2.0),
                        "skill_att": ANCHOR_WEIGHT * 2.0,
                        "u_diff_makes": -0.06 * PENALTY_PRIOR_WEIGHT,
                        "u_diff_att": PENALTY_PRIOR_WEIGHT,
                    }

                state = player_state[pid]
                current_theta = state["skill_makes"] / state["skill_att"]
                current_delta = state["u_diff_makes"] / state["u_diff_att"]
                expected = current_theta if is_assisted else (current_theta + current_delta)
                expected = max(0.10, min(0.60, expected))

                if not is_playoff:
                    writer.writerow(
                        {
                            "game_id": row["game_id"],
                            "action_number": row["action_number"],
                            "player_id": pid,
                            "is_assisted": int(is_assisted),
                            "is_make": int(is_make),
                            "expected_3p_prob": expected,
                        }
                    )

                if is_assisted:
                    state["skill_makes"] = (state["skill_makes"] * decay_skill) + (1.0 if is_make else 0.0)
                    state["skill_att"] = (state["skill_att"] * decay_skill) + 1.0
                else:
                    obs_penalty = (1.0 if is_make else 0.0) - current_theta
                    state["u_diff_makes"] = (state["u_diff_makes"] * decay_penalty) + obs_penalty
                    state["u_diff_att"] = (state["u_diff_att"] * decay_penalty) + 1.0

        print(f"Wrote {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build season-scoped VWD 3PT priors from regular season + playoff shot history.")
    parser.add_argument("--regular-pbp-dir", required=True, help="Directory containing nbastatsv3_<year>.csv files")
    parser.add_argument("--playoff-pbp-dir", required=True, help="Directory containing nbastats_po_<year>.csv files")
    parser.add_argument("--totals-dir", required=True, help="Directory containing totals_<season>.csv files with FTM/FTA")
    parser.add_argument("--output-dir", required=True, help="Directory to write vwd_priors_<year>.csv files")
    parser.add_argument("--season-start-year", type=int, default=None, help="Optional lower bound season start year.")
    parser.add_argument("--season-end-year", type=int, default=None, help="Optional upper bound season start year.")
    args = parser.parse_args()

    generate_priors(
        regular_pbp_dir=Path(args.regular_pbp_dir),
        playoff_pbp_dir=Path(args.playoff_pbp_dir),
        totals_dir=Path(args.totals_dir),
        output_dir=Path(args.output_dir),
        season_start_year=args.season_start_year,
        season_end_year=args.season_end_year,
    )


if __name__ == "__main__":
    main()
