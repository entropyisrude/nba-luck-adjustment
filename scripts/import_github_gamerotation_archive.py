"""Import only unresolved NBA GameRotation rows from a pinned GitHub archive."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
UNRESOLVED = REBUILD / "canonical_unresolved_games.csv"
COVERAGE = ROOT / "outputs" / "contextual_causal" / "github_gamerotation_coverage_audit.csv"
OUT = REBUILD / "github_gamerotation_cache"
QUEUE = REBUILD / "remaining_quarantine_queues" / "github_gamerotation_complete.csv"
REPOSITORY = "gabriel1200/shot_data"
COMMIT = "ad8f06c5bd7e95e99291f14358421824572beb31"
RAW_TEMPLATE = ("https://raw.githubusercontent.com/" + REPOSITORY + "/" + COMMIT
                + "/rotations/{year}.csv")
KEEP = ["GAME_ID", "TEAM_ID", "TEAM_CITY", "TEAM_NAME", "PERSON_ID",
        "PLAYER_FIRST", "PLAYER_LAST", "IN_TIME_REAL", "OUT_TIME_REAL",
        "PLAYER_PTS", "PT_DIFF", "USG_PCT"]


def normalize(value: object) -> str:
    text = str(value).split(".")[0]
    return text.lstrip("0") or "0"


def main() -> None:
    unresolved = pd.read_csv(UNRESOLVED, dtype={"game_id": str})
    unresolved["game_id"] = unresolved.game_id.map(normalize)
    coverage = pd.read_csv(COVERAGE, dtype={"game_id": str})
    coverage["game_id"] = coverage.game_id.map(normalize)
    complete_ids = set(coverage.loc[coverage.rotation_complete, "game_id"])
    targets = unresolved.loc[unresolved.game_id.isin(complete_ids)].copy()
    OUT.mkdir(parents=True, exist_ok=True)
    QUEUE.parent.mkdir(parents=True, exist_ok=True)

    yearly = []
    imported: set[str] = set()
    for season, part in sorted(targets.groupby("season_year")):
        year = int(season) + 1
        url = RAW_TEMPLATE.format(year=year)
        request = Request(url, headers={"User-Agent": "nba-onoff-canonical-audit/1.0"})
        with urlopen(request, timeout=60) as response:
            payload = response.read()
        digest = hashlib.sha256(payload).hexdigest()
        frame = pd.read_csv(io.BytesIO(payload))
        missing = set(KEEP) - set(frame.columns)
        if missing:
            raise RuntimeError(f"{year}: missing columns {sorted(missing)}")
        frame["game_id_norm"] = frame.GAME_ID.map(normalize)
        wanted = set(part.game_id)
        subset = frame.loc[frame.game_id_norm.isin(wanted), KEEP + ["game_id_norm"]].copy()
        for gid, game in subset.groupby("game_id_norm"):
            game = game.copy()
            game = game.drop(columns="game_id_norm")
            game["GAME_ID"] = str(gid).zfill(10)
            game["start_elapsed"] = pd.to_numeric(
                game.IN_TIME_REAL, errors="coerce") / 10.0
            game["end_elapsed"] = pd.to_numeric(
                game.OUT_TIME_REAL, errors="coerce") / 10.0
            game.to_json(OUT / f"{str(gid).zfill(10)}.json", orient="split")
            imported.add(str(gid))
        yearly.append({"season_year": int(season), "archive_year": year,
                       "url": url, "sha256": digest,
                       "download_bytes": len(payload),
                       "target_games": len(wanted),
                       "imported_games": subset.game_id_norm.nunique()})
        print(f"{season}: imported {subset.game_id_norm.nunique()} / {len(wanted)}", flush=True)

    missing_games = complete_ids - imported
    if missing_games:
        raise RuntimeError(f"complete archive games missing during import: {sorted(missing_games)[:10]}")
    targets.loc[targets.game_id.isin(imported), ["game_id", "season_year"]].to_csv(
        QUEUE, index=False)
    provenance = {
        "repository": REPOSITORY,
        "commit": COMMIT,
        "license_declared": None,
        "redistribution_policy": "internal filtered cache; do not publish source rows",
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "imported_games": len(imported),
        "yearly_sources": yearly,
    }
    (OUT / "provenance.json").write_text(
        json.dumps(provenance, indent=2), encoding="utf-8")
    print(f"wrote {len(imported)} per-game files to {OUT}")
    print(f"wrote queue to {QUEUE}")


if __name__ == "__main__":
    main()
