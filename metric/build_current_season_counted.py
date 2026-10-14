"""Canonical counted evidence for the current season, from the daily pipeline.

The canonical July build (build_canonical_counted_production_evidence.py +
build_canonical_counted_onoff.py) needs desktop-only historical archives, so
it cannot extend into a new season. This builds the same two artifacts for the
current regular season from what run_onoff.py records each day:

  data/season_<TAG>/stints_<TAG>.csv        lineup stints (3PT-luck adjusted pts)
  data/season_<TAG>/possessions_<TAG>.csv   possessions with on-court lineups
  data/season_<TAG>/adjusted_onoff_<TAG>.csv player-game minutes and +/-
  data/shooting_luck_state.json             per-game FT / midrange luck

Outputs (rebuilt every run, gitignored) under derived/contextual_causal/current_season/:

  counted_stints_<TAG>.parquet  rows at game x home lineup x away lineup grain,
                                schema-compatible with canonical_counted_stints
  onoff_counted_<TAG>.parquet   schema-compatible with
                                adjusted_onoff_regular_canonical_counted

Differences from the canonical build: possessions come from the daily parser
rather than the historical event walker, and FT / midrange luck is known only
per game, so it is spread across a game's lineups by possession share.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.season import SEASON_DIR, TAG  # noqa: E402
from src.shooting_luck import historical_game_totals  # noqa: E402

OUT = ROOT / "derived" / "contextual_causal" / "current_season"
STINTS_OUT = OUT / f"counted_stints_{TAG}.parquet"
ONOFF_OUT = OUT / f"onoff_counted_{TAG}.parquet"
LUCK_STATE = ROOT / "data" / "shooting_luck_state.json"
LUCK_HISTORICAL = ROOT / "data" / "rapm_luck_adjust.parquet"
HCOLS = [f"home_p{i}" for i in range(1, 6)]
ACOLS = [f"away_p{i}" for i in range(1, 6)]
OCOLS = [f"off_p{i}" for i in range(1, 6)]
DCOLS = [f"def_p{i}" for i in range(1, 6)]


def _key(frame: pd.DataFrame, cols: list[str]) -> pd.Series:
    ids = frame[cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype("int64")
    return pd.Series(["-".join(map(str, sorted(r))) for r in ids.to_numpy()], index=frame.index)


def _read(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    frame = pd.read_csv(path, dtype={"game_id": str}, low_memory=False)
    if frame.empty:
        return None
    frame["game_id"] = frame.game_id.astype(str).str.lstrip("0")
    return frame


def build_stints(stints: pd.DataFrame, poss: pd.DataFrame) -> pd.DataFrame:
    stints = stints.copy()
    stints["home_lineup_key"] = _key(stints, HCOLS)
    stints["away_lineup_key"] = _key(stints, ACOLS)
    group_cols = ["game_id", "home_lineup_key", "away_lineup_key"]
    grouped = (stints.groupby(group_cols, sort=False)
               .agg(date=("date", "first"), stint_index=("stint_index", "min"),
                    home_id=("home_id", "first"), away_id=("away_id", "first"),
                    seconds=("seconds", "sum"), start_elapsed=("start_elapsed", "min"),
                    end_elapsed=("end_elapsed", "max"),
                    home_pts=("home_pts", "sum"), away_pts=("away_pts", "sum"),
                    home_pts_adj=("home_pts_adj", "sum"), away_pts_adj=("away_pts_adj", "sum"))
               .reset_index())

    # Attach each possession to its game x lineup-pair group.
    sides = grouped.drop_duplicates("game_id").set_index("game_id")[["home_id", "away_id"]]
    poss = poss[poss.game_id.isin(sides.index)].copy()
    poss["off_key"], poss["def_key"] = _key(poss, OCOLS), _key(poss, DCOLS)
    home_off = poss.offense_team.astype("int64").eq(poss.game_id.map(sides.home_id).astype("int64"))
    poss["side"] = np.where(home_off, "home", "away")
    poss["home_lineup_key"] = np.where(home_off, poss.off_key, poss.def_key)
    poss["away_lineup_key"] = np.where(home_off, poss.def_key, poss.off_key)
    counts = (poss.groupby(group_cols + ["side"])
              .agg(n=("points", "size"), pts=("points", "sum"))
              .unstack("side", fill_value=0))
    counts.columns = [f"{a}_{b}" for a, b in counts.columns]
    out = grouped.merge(counts.reset_index(), on=group_cols, how="left")
    for col in ("n_home", "n_away", "pts_home", "pts_away"):
        out[col] = out[col].fillna(0.0) if col in out else 0.0
    out["n_home_raw"], out["n_away_raw"] = out.n_home, out.n_away

    for i, col in enumerate(HCOLS):
        out[col] = out.home_lineup_key.str.split("-").str[i].astype("int64")
    for i, col in enumerate(ACOLS):
        out[col] = out.away_lineup_key.str.split("-").str[i].astype("int64")
    out["evidence_source"] = "daily_counted"

    # Base target: the daily 3PT-luck-adjusted stint points, as in canonical.
    out["points_adjusted_home"] = out.home_pts_adj.astype(float)
    out["points_adjusted_away"] = out.away_pts_adj.astype(float)

    # Same precedence as src.shooting_luck.game_components: the committed
    # historical artifact first, then games scored by the daily state.
    luck = {}
    if LUCK_STATE.exists():
        state_games = json.loads(LUCK_STATE.read_text(encoding="utf-8")).get("games", {})
        luck = {g: v.get("components", {}) for g, v in state_games.items()}
    luck.update(historical_game_totals(LUCK_HISTORICAL))
    for side in ("home", "away"):
        game_n = out.groupby("game_id")[f"n_{side}"].transform("sum").clip(lower=1e-9)
        share = out[f"n_{side}"] / game_n
        for comp in ("ft", "mr"):
            total = out.game_id.map(
                lambda g: float(luck.get(g, {}).get(f"{comp}_luck_{side}", 0.0)))
            out[f"_{comp}_{side}"] = total * share
        base = f"points_adjusted_{side}"
        out[f"{base}_3pt"] = out[base]
        out[f"{base}_3pt_ft"] = out[base] - out[f"_ft_{side}"]
        out[base] = out[f"{base}_3pt_ft"] - 0.5 * out[f"_mr_{side}"]

    out["date"] = pd.to_datetime(out.date)
    return out.sort_values(["date", "game_id", "stint_index"]).reset_index(drop=True)


def build_onoff(counted: pd.DataFrame, onoff: pd.DataFrame) -> pd.DataFrame:
    """Same construction as build_canonical_counted_onoff.build(), daily inputs."""
    parts = []
    for side, other, cols in (("home", "away", HCOLS), ("away", "home", ACOLS)):
        long = counted[["game_id", f"{side}_id", f"points_adjusted_{side}",
                        f"points_adjusted_{other}", f"n_{side}", f"n_{other}"] + cols]
        long = long.melt(id_vars=long.columns[:6].tolist(), value_name="player_id")
        parts.append(long.groupby(["game_id", f"{side}_id", "player_id"], as_index=False)
                     .agg(on_pts_for_adj=(f"points_adjusted_{side}", "sum"),
                          on_pts_against_adj=(f"points_adjusted_{other}", "sum"),
                          on_off_poss=(f"n_{side}", "sum"), on_def_poss=(f"n_{other}", "sum"))
                     .rename(columns={f"{side}_id": "team_id"}))
    on = pd.concat(parts, ignore_index=True)

    games = counted.groupby("game_id").agg(
        home_id=("home_id", "first"), away_id=("away_id", "first"),
        home_score=("home_pts", "sum"), away_score=("away_pts", "sum"),
        home_adj=("points_adjusted_home", "sum"), away_adj=("points_adjusted_away", "sum"),
        n_home=("n_home", "sum"), n_away=("n_away", "sum"))

    players = onoff[onoff.game_id.isin(games.index)][
        ["game_id", "team_id", "player_id", "player_name", "date", "minutes_on", "on_diff"]].copy()
    players = players[players.minutes_on > 0]
    for col in ("team_id", "player_id"):
        players[col] = pd.to_numeric(players[col], errors="raise").astype("int64")
    on["team_id"] = on.team_id.astype("int64")
    on["player_id"] = on.player_id.astype("int64")
    out = players.merge(on, on=["game_id", "team_id", "player_id"], how="left")

    g = games.loc[out.game_id]
    home = out.team_id.to_numpy() == g.home_id.to_numpy()
    out["opponent_id"] = np.where(home, g.away_id, g.home_id)
    out["team_score"] = np.where(home, g.home_score, g.away_score)
    out["opp_score"] = np.where(home, g.away_score, g.home_score)
    out["team_adj"] = np.where(home, g.home_adj, g.away_adj)
    out["opp_adj"] = np.where(home, g.away_adj, g.home_adj)
    out["team_poss"] = np.where(home, g.n_home, g.n_away).astype(float)
    out["opp_poss"] = np.where(home, g.n_away, g.n_home).astype(float)

    # Players never on court for a complete lineup (e.g. a few seconds at a
    # period seam) fall back to official-minutes shares, as in canonical.
    team_minutes = out.groupby(["game_id", "team_id"]).minutes_on.transform("sum")
    share = (out.minutes_on / (team_minutes / 5.0).clip(lower=1)).clip(0, 1)
    fallback = out.on_off_poss.isna()
    out.loc[fallback, "on_off_poss"] = out.loc[fallback, "team_poss"] * share[fallback]
    out.loc[fallback, "on_def_poss"] = out.loc[fallback, "opp_poss"] * share[fallback]
    volume_share = share * (out.team_adj + out.opp_adj)
    out.loc[fallback, "on_pts_for_adj"] = ((volume_share + out.on_diff) / 2)[fallback]
    out.loc[fallback, "on_pts_against_adj"] = ((volume_share - out.on_diff) / 2)[fallback]
    out["evidence_resolution"] = np.where(
        fallback, "daily_counted_possessions_official_minutes", "daily_stint_counted")

    out["off_off_poss"] = (out.team_poss - out.on_off_poss).clip(lower=0)
    out["off_def_poss"] = (out.opp_poss - out.on_def_poss).clip(lower=0)
    out["off_pts_for_adj"] = out.team_adj - out.on_pts_for_adj
    out["off_pts_against_adj"] = out.opp_adj - out.on_pts_against_adj
    on_volume = out.on_pts_for_adj + out.on_pts_against_adj
    out["on_pts_for"] = (on_volume + out.on_diff) / 2
    out["on_pts_against"] = (on_volume - out.on_diff) / 2
    out["off_pts_for"] = out.team_score - out.on_pts_for
    out["off_pts_against"] = out.opp_score - out.on_pts_against
    out["off_diff"] = out.off_pts_for - out.off_pts_against
    out["on_diff_adj"] = out.on_pts_for_adj - out.on_pts_against_adj
    out["off_diff_adj"] = out.off_pts_for_adj - out.off_pts_against_adj
    out["on_off_diff"] = out.on_diff - out.off_diff
    out["on_off_diff_adj"] = out.on_diff_adj - out.off_diff_adj
    out["gameType"] = "Regular Season"
    out["game_id"] = out.game_id.astype("int64")
    out["date"] = pd.to_datetime(out.date)

    cols = ["game_id", "team_id", "opponent_id", "player_id", "player_name", "date",
            "gameType", "minutes_on", "on_pts_for", "on_pts_against", "on_diff",
            "off_pts_for", "off_pts_against", "off_diff", "on_pts_for_adj",
            "on_pts_against_adj", "on_diff_adj", "off_pts_for_adj",
            "off_pts_against_adj", "off_diff_adj", "on_off_diff", "on_off_diff_adj",
            "on_off_poss", "on_def_poss", "off_off_poss", "off_def_poss",
            "evidence_resolution"]
    return out[cols].sort_values(["date", "game_id", "team_id", "player_id"]).reset_index(drop=True)


def main() -> None:
    stints = _read(SEASON_DIR / f"stints_{TAG}.csv")
    poss = _read(SEASON_DIR / f"possessions_{TAG}.csv")
    onoff = _read(SEASON_DIR / f"adjusted_onoff_{TAG}.csv")
    OUT.mkdir(parents=True, exist_ok=True)
    if stints is None or poss is None or onoff is None:
        for path in (STINTS_OUT, ONOFF_OUT):
            path.unlink(missing_ok=True)
        print(f"No {TAG} daily on/off data yet; current-season counted evidence not built.")
        return
    counted = build_stints(stints, poss)
    counted.to_parquet(STINTS_OUT, index=False)
    onoff_counted = build_onoff(counted, onoff)
    onoff_counted.to_parquet(ONOFF_OUT, index=False)
    print(f"{TAG}: {counted.game_id.nunique()} games, {len(counted):,} lineup-pair rows, "
          f"{len(onoff_counted):,} player-games "
          f"{onoff_counted.evidence_resolution.value_counts().to_dict()}")


if __name__ == "__main__":
    main()
