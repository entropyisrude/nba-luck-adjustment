from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path


def season_start_year_from_mmddyyyy(date_str: str) -> int:
    d = datetime.strptime(date_str, "%m/%d/%Y")
    return d.year if d.month >= 7 else d.year - 1


def action_number_from_action(action: dict) -> int | None:
    for field in ("actionNumber", "orderNumber", "actionId"):
        raw = action.get(field)
        try:
            value = int(raw)
        except Exception:
            continue
        if value > 0:
            return value
    return None


class SeasonalShotPriorLookup:
    """
    Load shot priors one season at a time to avoid holding the full historical
    shot-level prior table in memory during rebuilds.
    """

    def __init__(self, priors_dir: str | Path | None):
        self.priors_dir = Path(priors_dir) if priors_dir else None
        self._loaded_season: int | None = None
        self._season_map: dict[str, dict[int, float]] = {}

    def _load_season(self, season_start_year: int) -> None:
        self._season_map = {}
        self._loaded_season = season_start_year
        if self.priors_dir is None:
            return

        path = self.priors_dir / f"vwd_priors_{season_start_year}.csv"
        if not path.exists():
            return

        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                game_id = str(row.get("game_id") or "").lstrip("0")
                if not game_id:
                    continue
                try:
                    action_number = int(row.get("action_number") or 0)
                    expected = float(row.get("expected_3p_prob") or 0.0)
                except Exception:
                    continue
                if action_number <= 0:
                    continue
                self._season_map.setdefault(game_id, {})[action_number] = expected

    def get_game_priors(self, game_id: str, game_date_mmddyyyy: str) -> dict[int, float] | None:
        if self.priors_dir is None:
            return None
        season_start_year = season_start_year_from_mmddyyyy(game_date_mmddyyyy)
        if self._loaded_season != season_start_year:
            self._load_season(season_start_year)
        return self._season_map.get(str(game_id).lstrip("0"))
