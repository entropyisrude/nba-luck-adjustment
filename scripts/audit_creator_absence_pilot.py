"""Audit player-game sources and construct candidate creator-absence events.

The event builder intentionally makes a modest claim: these are realized
absences inferred from recent participation, not verified unexpected injuries.
All creator and rotation features are shifted so that they use only games
strictly before the target game.

By default the script is read-only. Pass ``--write`` to save the audit and
candidate event table under the contextual-causal derived/output folders.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DERIVED = ROOT / "derived" / "contextual_causal"
OUTPUTS = ROOT / "outputs" / "contextual_causal"

HISTORICAL_BOXSCORES = DATA / "player_boxscore_stats_kaggle_traditional.csv"
RECENT_BOXSCORES = DATA / "player_boxscore_stats.csv"
HISTORICAL_GAMES = DATA / "game_metadata_kaggle_traditional.csv"

LOOKBACK_TEAM_GAMES = 10
RECENCY_DAYS = 30
MIN_PRIOR_APPEARANCES = 5
MIN_ROTATION_MINUTES = 24.0
MAX_CREATORS_PER_TEAM = 2


def season_from_date(date: pd.Series) -> pd.Series:
    start = date.dt.year.where(date.dt.month >= 7, date.dt.year - 1)
    return start.astype(str) + "-" + ((start + 1) % 100).astype(str).str.zfill(2)


def parse_minutes(value: pd.Series) -> pd.Series:
    raw = value.astype(str)
    numeric = pd.to_numeric(raw, errors="coerce")
    iso = raw.str.extract(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?")
    iso_minutes = (
        pd.to_numeric(iso[0], errors="coerce").fillna(0) * 60
        + pd.to_numeric(iso[1], errors="coerce").fillna(0)
        + pd.to_numeric(iso[2], errors="coerce").fillna(0) / 60
    )
    clock = raw.str.extract(r"^(\d+):(\d+(?:\.\d+)?)$")
    clock_minutes = (
        pd.to_numeric(clock[0], errors="coerce")
        + pd.to_numeric(clock[1], errors="coerce") / 60
    )
    out = numeric.where(numeric.notna(), iso_minutes.where(raw.str.startswith("PT"), clock_minutes))
    return out.fillna(0.0).astype(float)


def load_player_games() -> pd.DataFrame:
    cols = [
        "date", "game_id", "team_abbr", "player_id", "player_name", "minutes",
        "pts", "ast", "tov", "fga", "fta", "fg3a",
    ]
    hist = pd.read_csv(HISTORICAL_BOXSCORES, usecols=cols, dtype={"game_id": str})
    recent = pd.read_csv(RECENT_BOXSCORES, dtype={"game_id": str})
    for col in cols:
        if col not in recent:
            recent[col] = np.nan
    recent = recent[cols]
    player_games = pd.concat([hist, recent], ignore_index=True)
    player_games["date"] = pd.to_datetime(player_games["date"], errors="coerce")
    player_games["minutes"] = parse_minutes(player_games["minutes"])
    for col in ["pts", "ast", "tov", "fga", "fta", "fg3a"]:
        player_games[col] = pd.to_numeric(player_games[col], errors="coerce").fillna(0.0)
    player_games["player_id"] = pd.to_numeric(player_games["player_id"], errors="coerce")
    player_games = player_games.dropna(subset=["date", "game_id", "team_abbr", "player_id"])
    # NBA regular-season game IDs begin with 2; exclude playoff/play-in and
    # other competition types from this first, deliberately homogeneous pilot.
    player_games = player_games[player_games["game_id"].str.startswith("2")].copy()
    player_games["player_id"] = player_games["player_id"].astype(int)
    player_games["season"] = season_from_date(player_games["date"])
    player_games = player_games.sort_values(["date", "game_id", "team_abbr", "player_id"])
    # Prefer the more recent local source when files overlap.
    player_games = player_games.drop_duplicates(["game_id", "team_abbr", "player_id"], keep="last")
    return player_games.reset_index(drop=True)


def load_games(player_games: pd.DataFrame) -> pd.DataFrame:
    games = pd.read_csv(HISTORICAL_GAMES, dtype={"game_id": str})
    games["date"] = pd.to_datetime(games["date"], errors="coerce")
    historical = pd.concat(
        [
            games[["date", "game_id", "home_team", "away_team"]].rename(
                columns={"home_team": "team_abbr", "away_team": "opponent"}
            ).assign(home=1),
            games[["date", "game_id", "home_team", "away_team"]].rename(
                columns={"away_team": "team_abbr", "home_team": "opponent"}
            ).assign(home=0),
        ],
        ignore_index=True,
    )
    # Reconstruct recent game-team rows from player participation because the
    # current local metadata file stops before the recent box-score source.
    recent_pairs = player_games.loc[
        player_games["date"] > historical["date"].max(),
        ["date", "game_id", "team_abbr"],
    ].drop_duplicates()
    team_counts = recent_pairs.groupby("game_id")["team_abbr"].transform("nunique")
    recent_pairs = recent_pairs[team_counts == 2].copy()
    opponent = recent_pairs.merge(
        recent_pairs, on=["date", "game_id"], suffixes=("", "_opp")
    )
    opponent = opponent[opponent["team_abbr"] != opponent["team_abbr_opp"]]
    recent = opponent[["date", "game_id", "team_abbr", "team_abbr_opp"]].rename(
        columns={"team_abbr_opp": "opponent"}
    )
    recent["home"] = np.nan
    out = pd.concat([historical, recent], ignore_index=True)
    out["season"] = season_from_date(out["date"])
    return out.drop_duplicates(["game_id", "team_abbr"]).sort_values(["date", "game_id", "team_abbr"])


def add_frozen_player_features(player_games: pd.DataFrame) -> pd.DataFrame:
    pg = player_games.sort_values(["player_id", "date", "game_id"]).copy()
    safe_minutes = pg["minutes"].clip(lower=1.0)
    pg["creation_load"] = pg["fga"] + 0.44 * pg["fta"] + 1.5 * pg["ast"] + pg["tov"]
    pg["creation_p36"] = 36.0 * pg["creation_load"] / safe_minutes
    pg["ast_p36"] = 36.0 * pg["ast"] / safe_minutes
    grouped = pg.groupby(["season", "team_abbr", "player_id"], sort=False)
    for source, target in [
        ("minutes", "prior_minutes"),
        ("creation_p36", "prior_creation_p36"),
        ("ast_p36", "prior_ast_p36"),
    ]:
        pg[target] = grouped[source].transform(
            lambda s: s.shift(1).rolling(LOOKBACK_TEAM_GAMES, min_periods=MIN_PRIOR_APPEARANCES).mean()
        )
    pg["prior_appearances"] = grouped.cumcount()
    return pg


def build_candidate_events(player_games: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    frozen = add_frozen_player_features(player_games)
    # Each player's latest prior appearance carries the frozen feature state
    # forward to a team game. merge_asof enforces strictly earlier dates via
    # allow_exact_matches=False, preventing target-game participation leakage.
    states = frozen[
        [
            "date", "season", "team_abbr", "player_id", "player_name",
            "prior_minutes", "prior_creation_p36", "prior_ast_p36", "prior_appearances",
        ]
    ].dropna(subset=["prior_minutes", "prior_creation_p36"])
    states = states.sort_values(["date", "team_abbr", "player_id"])
    event_rows: list[pd.DataFrame] = []
    for (season, team), team_games in games.groupby(["season", "team_abbr"], sort=False):
        team_states = states[(states["season"] == season) & (states["team_abbr"] == team)]
        if team_states.empty:
            continue
        roster_ids = team_states["player_id"].drop_duplicates()
        left = team_games.sort_values("date").assign(_join=1)
        left = left.merge(pd.DataFrame({"player_id": roster_ids, "_join": 1}), on="_join").drop(columns="_join")
        left = left.sort_values("date")
        right = team_states.drop(columns=["season", "team_abbr"]).sort_values("date")
        carried = pd.merge_asof(
            left, right, on="date", by="player_id", direction="backward",
            allow_exact_matches=False, tolerance=pd.Timedelta(days=RECENCY_DAYS),
        )
        event_rows.append(carried)
    if not event_rows:
        return pd.DataFrame()
    exposure = pd.concat(event_rows, ignore_index=True)
    played = player_games[["game_id", "team_abbr", "player_id"]].drop_duplicates().assign(played=1)
    exposure = exposure.merge(played, on=["game_id", "team_abbr", "player_id"], how="left")
    exposure["played"] = exposure["played"].fillna(0).astype(int)
    eligible = exposure[
        (exposure["prior_minutes"] >= MIN_ROTATION_MINUTES)
        & (exposure["prior_appearances"] >= MIN_PRIOR_APPEARANCES)
    ].copy()
    eligible["creator_rank"] = eligible.groupby(["game_id", "team_abbr"])["prior_creation_p36"].rank(
        method="first", ascending=False
    )
    candidates = eligible[
        (eligible["played"] == 0) & (eligible["creator_rank"] <= MAX_CREATORS_PER_TEAM)
    ].copy()
    candidates = candidates.rename(columns={"player_id": "absent_player_id", "player_name": "absent_player_name"})
    keep = [
        "date", "season", "game_id", "team_abbr", "opponent", "home",
        "absent_player_id", "absent_player_name", "creator_rank", "prior_minutes",
        "prior_creation_p36", "prior_ast_p36", "prior_appearances",
    ]
    return candidates[keep].sort_values(["date", "game_id", "team_abbr", "creator_rank"]).reset_index(drop=True)


def duplicate_count(df: pd.DataFrame, keys: list[str]) -> int:
    return int(df.duplicated(keys, keep=False).sum())


def build_audit(player_games: pd.DataFrame, games: pd.DataFrame, events: pd.DataFrame) -> dict:
    return {
        "claim_level": "realized-absence candidates; absence timing and unexpectedness are unverified",
        "competition_scope": "NBA regular season only (game IDs beginning with 2)",
        "parameters": {
            "lookback_appearances": LOOKBACK_TEAM_GAMES,
            "recency_days": RECENCY_DAYS,
            "minimum_prior_appearances": MIN_PRIOR_APPEARANCES,
            "minimum_prior_minutes": MIN_ROTATION_MINUTES,
            "maximum_creator_rank": MAX_CREATORS_PER_TEAM,
        },
        "player_games": {
            "rows": int(len(player_games)),
            "games": int(player_games["game_id"].nunique()),
            "players": int(player_games["player_id"].nunique()),
            "date_min": str(player_games["date"].min().date()),
            "date_max": str(player_games["date"].max().date()),
            "duplicate_player_game_team_rows": duplicate_count(player_games, ["game_id", "team_abbr", "player_id"]),
        },
        "game_team_rows": {
            "rows": int(len(games)),
            "games": int(games["game_id"].nunique()),
            "date_min": str(games["date"].min().date()),
            "date_max": str(games["date"].max().date()),
            "duplicate_game_team_rows": duplicate_count(games, ["game_id", "team_abbr"]),
        },
        "candidate_events": {
            "rows": int(len(events)),
            "games": int(events["game_id"].nunique()) if len(events) else 0,
            "players": int(events["absent_player_id"].nunique()) if len(events) else 0,
            "by_season": events.groupby("season").size().astype(int).to_dict() if len(events) else {},
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="write derived event and audit files")
    args = parser.parse_args()

    player_games = load_player_games()
    games = load_games(player_games)
    events = build_candidate_events(player_games, games)
    audit = build_audit(player_games, games, events)
    print(json.dumps(audit, indent=2))

    if args.write:
        DERIVED.mkdir(parents=True, exist_ok=True)
        OUTPUTS.mkdir(parents=True, exist_ok=True)
        event_path = DERIVED / "creator_absence_candidates.csv"
        audit_path = OUTPUTS / "creator_absence_audit.json"
        events.to_csv(event_path, index=False)
        audit_path.write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
        print(f"\nWrote {event_path}")
        print(f"Wrote {audit_path}")

    print("\nCandidate event sample:")
    sample = events.tail(20).to_string(index=False) if len(events) else "No events found"
    # Windows PowerShell may use a legacy console encoding even when source
    # data correctly contains Unicode player names.
    print(sample.encode("cp1252", errors="replace").decode("cp1252"))


if __name__ == "__main__":
    main()
