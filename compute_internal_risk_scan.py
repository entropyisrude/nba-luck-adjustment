from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute a cheap internal game risk scan.")
    parser.add_argument("--onoff-path", default="data/adjusted_onoff_historical_pbp.csv")
    parser.add_argument("--stints-path", default="data/stints_historical_pbp.csv")
    parser.add_argument("--out-path", default="data/internal_risk_scan.csv")
    args = parser.parse_args()

    onoff = pd.read_csv(args.onoff_path, dtype={"game_id": str}, low_memory=False)
    stints = pd.read_csv(args.stints_path, dtype={"game_id": str}, low_memory=False)
    onoff["game_id"] = onoff["game_id"].astype(str).str.lstrip("0")
    stints["game_id"] = stints["game_id"].astype(str).str.lstrip("0")

    meta = (
        stints.sort_values(["game_id", "stint_index"])
        .groupby("game_id", as_index=False)
        .first()[["game_id", "date", "home_id", "away_id"]]
    )

    rows: list[dict] = []
    total = len(meta)
    for idx, r in enumerate(meta.itertuples(index=False), start=1):
        if idx % 1000 == 0 or idx == total:
            print(f"processed {idx}/{total}")
        game_id = str(r.game_id)
        game_onoff = onoff[onoff["game_id"] == game_id]
        max_period = int(stints.loc[stints["game_id"] == game_id, "end_period"].max())
        overtime_periods = max(0, max_period - 4)
        expected_team_minutes = 48.0 + overtime_periods * 5.0
        minute_total_error = 0.0
        for team_id in [int(r.home_id), int(r.away_id)]:
            team_minutes = pd.to_numeric(
                game_onoff.loc[game_onoff["team_id"] == team_id, "minutes_on"],
                errors="coerce",
            ).sum()
            minute_total_error += abs(team_minutes - expected_team_minutes)
        duplicate_name_rows = int(
            game_onoff.groupby(["team_id", "player_name"]).size().gt(1).sum()
        )
        risk_score = duplicate_name_rows * 2 + (2 if minute_total_error > 1.0 else 0) + (1 if overtime_periods > 0 else 0)
        rows.append(
            {
                "game_id": game_id,
                "date": r.date,
                "risk_score": risk_score,
                "duplicate_name_rows": duplicate_name_rows,
                "minute_total_error": round(float(minute_total_error), 4),
                "overtime_periods": overtime_periods,
            }
        )

    risk = pd.DataFrame(rows).sort_values(
        ["risk_score", "minute_total_error", "date", "game_id"],
        ascending=[False, False, True, True],
    )
    out_path = Path(args.out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    risk.to_csv(out_path, index=False)
    print(f"wrote {out_path} rows={len(risk)}")
    print("top 20")
    print(risk.head(20).to_string(index=False))
    print("\ncounts by risk_score")
    print(risk["risk_score"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()
