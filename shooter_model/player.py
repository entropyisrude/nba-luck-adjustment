"""
Query the 3PT expectation model for a specific player.

Uses the calibration dataset as the shot-history source. For current-season
live data you would supply rows directly to model.estimate_from_season_rows().

Usage:
    python shooter_model/player.py "Steph Curry"
    python shooter_model/player.py --id 201939
    python shooter_model/player.py "Mo Diawara" --half-life 1.5
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# Allow running as a script from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from shooter_model.model import ThreePTModel

DATA_DIR = Path(__file__).parent / "data"


def load_player_history(
    name: str | None = None,
    player_id: int | None = None,
) -> tuple[pd.DataFrame, str]:
    """Return all calibration rows for a player and their canonical name."""
    calib = DATA_DIR / "calibration.csv"
    if not calib.exists():
        raise FileNotFoundError("calibration.csv not found — run fetch_data.py first.")
    df = pd.read_csv(calib)

    if player_id is not None:
        sub = df[df["PLAYER_ID"] == player_id]
    elif name is not None:
        # Case-insensitive substring match
        mask = df["PLAYER_NAME"].str.lower().str.contains(name.lower(), na=False)
        sub = df[mask]
        if sub.empty:
            # Exact match fallback
            sub = df[df["PLAYER_NAME"].str.lower() == name.lower()]
    else:
        raise ValueError("Provide name or player_id.")

    if sub.empty:
        candidates = df["PLAYER_NAME"].unique()[:10]
        raise ValueError(
            f"Player not found. Sample names: {list(candidates)}"
        )

    # If multiple players match (e.g. "Davis" matches many), disambiguate
    unique_ids = sub["PLAYER_ID"].unique()
    if len(unique_ids) > 1:
        print("Multiple players matched:")
        for pid in unique_ids:
            n = sub[sub["PLAYER_ID"] == pid]["PLAYER_NAME"].iloc[0]
            gp = sub[sub["PLAYER_ID"] == pid]["GP"].sum()
            print(f"  {pid}  {n}  ({gp} games)")
        chosen_id = unique_ids[0]
        print(f"Using first match: {chosen_id}\n")
        sub = sub[sub["PLAYER_ID"] == chosen_id]

    canonical_name = sub["PLAYER_NAME"].iloc[-1]
    return sub.sort_values("SEASON"), canonical_name


def _robust_ft_pct(rows: pd.DataFrame) -> float:
    """
    Compute a robust FT% estimate: weighted by FTA (more attempts = more weight),
    with extra weight on recent seasons.
    """
    rows = rows.dropna(subset=["FT_PCT", "FTA"])
    if rows.empty:
        return 0.78  # league average fallback

    # Use season-weighted FTA as weights
    from shooter_model.model import season_weights
    seasons = rows["SEASON"].tolist()
    sw = season_weights(seasons, half_life=3.0)  # longer half-life for FT% (more stable)

    fta = rows["FTA"].values.astype(float)
    ftm = (rows["FT_PCT"] * fta).values
    w = [s * f for s, f in zip(sw, fta)]
    total_w = sum(w)
    if total_w == 0:
        return 0.78
    weighted_ftm = sum(wi * (ftmi / ftai) for wi, ftmi, ftai in zip(w, ftm, fta) if ftai > 0)
    return float(weighted_ftm / total_w)


def evaluate_player(
    name: str | None = None,
    player_id: int | None = None,
    half_life: float = 2.0,
    verbose: bool = True,
) -> dict:
    """
    Run the 3PT expectation model for a player and return a summary dict.

    Returns keys:
        player_name, player_id, ft_pct, seasons,
        overall_prior, overall_posterior, overall_ci90,
        cs_posterior, cs_ci90,
        pu_posterior, pu_ci90,
        weighted_attempts, weighted_cs_attempts, weighted_pu_attempts
    """
    rows, canonical_name = load_player_history(name, player_id)
    model = ThreePTModel.from_params()

    ft_pct = _robust_ft_pct(rows)
    row_dicts = rows.to_dict("records")

    result = model.estimate_from_season_rows(row_dicts, ft_pct, half_life=half_life)
    ov = result.overall

    out = {
        "player_name": canonical_name,
        "player_id": int(rows["PLAYER_ID"].iloc[0]),
        "ft_pct": round(ft_pct, 4),
        "seasons": sorted(rows["SEASON"].unique().tolist()),
        "overall_prior": round(ov.prior_mean, 4),
        "overall_posterior": round(ov.posterior_mean, 4),
        "overall_ci90": tuple(round(x, 4) for x in ov.credible_interval(0.90)),
        "weighted_attempts": round(ov.obs_attempts, 1),
        "weighted_makes": round(ov.obs_makes, 1),
        "cs_posterior": None,
        "cs_ci90": None,
        "weighted_cs_attempts": None,
        "pu_posterior": None,
        "pu_ci90": None,
        "weighted_pu_attempts": None,
    }

    if result.catch_shoot is not None:
        cs = result.catch_shoot
        out["cs_posterior"] = round(cs.posterior_mean, 4)
        out["cs_ci90"] = tuple(round(x, 4) for x in cs.credible_interval(0.90))
        out["weighted_cs_attempts"] = round(cs.obs_attempts, 1)

    if result.pullup is not None:
        pu = result.pullup
        out["pu_posterior"] = round(pu.posterior_mean, 4)
        out["pu_ci90"] = tuple(round(x, 4) for x in pu.credible_interval(0.90))
        out["weighted_pu_attempts"] = round(pu.obs_attempts, 1)

    if verbose:
        _print_report(out, rows, result)

    return out


def _print_report(summary: dict, rows: pd.DataFrame, result) -> None:
    name = summary["player_name"]
    sep = "=" * 60
    print(f"\n{sep}")
    print(f"3PT Expectation Model: {name}")
    print(sep)

    print(f"\nFT%:  {summary['ft_pct']:.3f}  (weighted across seasons)")
    print(f"Seasons in dataset: {', '.join(summary['seasons'])}")

    print("\n--- Season-level data ---")
    display_cols = ["SEASON", "GP", "FG3A", "FG3M", "FG3_PCT",
                    "CATCH_SHOOT_FG3A", "CATCH_SHOOT_FG3_PCT",
                    "PULLUP_FG3A", "PULLUP_FG3_PCT",
                    "FTA", "FT_PCT"]
    display_cols = [c for c in display_cols if c in rows.columns]
    pd.set_option("display.float_format", "{:.3f}".format)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.width", 120)
    print(rows[display_cols].to_string(index=False))

    print("\n--- Posterior estimates ---")
    ov = result.overall
    lo, hi = summary["overall_ci90"]
    print(f"Overall  : prior={ov.prior_mean:.3f}  "
          f"obs={ov.obs_makes:.1f}/{ov.obs_attempts:.1f} (weighted)  "
          f"posterior={ov.posterior_mean:.3f}  90% CI [{lo:.3f}, {hi:.3f}]")

    if result.catch_shoot is not None:
        cs = result.catch_shoot
        lo, hi = summary["cs_ci90"]
        print(f"C&S      : prior={cs.prior_mean:.3f}  "
              f"obs={cs.obs_makes:.1f}/{cs.obs_attempts:.1f}  "
              f"posterior={cs.posterior_mean:.3f}  90% CI [{lo:.3f}, {hi:.3f}]")

    if result.pullup is not None:
        pu = result.pullup
        lo, hi = summary["pu_ci90"]
        print(f"Pull-up  : prior={pu.prior_mean:.3f}  "
              f"obs={pu.obs_makes:.1f}/{pu.obs_attempts:.1f}  "
              f"posterior={pu.posterior_mean:.3f}  90% CI [{lo:.3f}, {hi:.3f}]")

    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="3PT expectation model for a player")
    parser.add_argument("name", nargs="?", help="Player name (substring match)")
    parser.add_argument("--id", type=int, help="Player ID (exact match)")
    parser.add_argument(
        "--half-life", type=float, default=2.0,
        help="Recency decay half-life in seasons (default: 2.0)"
    )
    args = parser.parse_args()

    if not args.name and not args.id:
        parser.error("Provide a player name or --id")

    evaluate_player(name=args.name, player_id=args.id, half_life=args.half_life)
