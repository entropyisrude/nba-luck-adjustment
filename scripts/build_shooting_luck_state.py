"""Seed the compact live FT/midrange expectation state from historical PBP."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PBP = Path(r"C:\Users\Dave\Downloads\nba-metric-data\PlayByPlay.parquet")
OUT = ROOT / "data" / "shooting_luck_state.json"


def main() -> None:
    cols = ["personId", "description", "actionType", "shotDistance"]
    p = pd.read_parquet(PBP, columns=cols)
    at = p.actionType.fillna("").str.strip().str.lower()
    desc = p.description.fillna("")
    made = (~desc.str.contains("MISS", case=False)).astype(int)
    pid = pd.to_numeric(p.personId, errors="coerce")

    ft_mask = at.isin(["free throw", "freethrow"]) & pid.notna()
    old_two = at.isin(["made shot", "missed shot"]) & ~desc.str.contains("3PT", case=False)
    distance = pd.to_numeric(p.shotDistance, errors="coerce")
    two_mask = (at.eq("2pt") | old_two) & pid.notna() & distance.between(10, 23.99)

    players: dict[str, dict[str, float]] = {}
    for kind, mask in (("ft", ft_mask), ("mr0", two_mask & distance.lt(16)),
                       ("mr1", two_mask & distance.ge(16))):
        f = pd.DataFrame({"pid": pid[mask].astype(int), "made": made[mask]})
        agg = f.groupby("pid").made.agg(["sum", "size"])
        for player_id, row in agg.iterrows():
            rec = players.setdefault(str(int(player_id)), {})
            rec[f"{kind}_m"] = float(row["sum"])
            rec[f"{kind}_a"] = float(row["size"])

    league = {
        "ft": float(made[ft_mask].mean()),
        "mr0": float(made[two_mask & distance.lt(16)].mean()),
        "mr1": float(made[two_mask & distance.ge(16)].mean()),
    }
    OUT.write_text(json.dumps({"version": 1, "league": league,
                               "players": players, "games": {}},
                              separators=(",", ":")), encoding="utf-8")
    print(f"wrote {OUT}: {len(players):,} players; league={league}")


if __name__ == "__main__":
    main()
