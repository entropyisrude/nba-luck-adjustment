"""Current-season identifiers, driven by `season` in config.yaml (e.g. "2026-27")."""
from pathlib import Path

import yaml

_CFG = Path(__file__).resolve().parents[1] / "config.yaml"

SEASON = yaml.safe_load(_CFG.read_text())["season"]       # "2026-27"
START_YEAR = int(SEASON[:4])                                # 2026
END_YEAR = START_YEAR + 1                                   # 2027 (BBRef's season year)
TAG = f"{START_YEAR % 100:02d}{END_YEAR % 100:02d}"         # "2627"
PREV_SEASON = f"{START_YEAR - 1}-{START_YEAR % 100:02d}"    # "2025-26"
SEASON_START_DATE = f"{START_YEAR}-10-01"
