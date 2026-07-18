"""Build player-game on/off evidence from canonical counted possessions.

The 37,740 trusted lineup games use exact stint-level on-court exposure.  The
213 aggregate-resolution games retain exact game possession totals and allocate
exposure by official minutes.  Raw source files are never modified.
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "derived/contextual_causal/production_counted_evidence"
OUT = ROOT / "derived/contextual_causal/production_counted_onoff"
ZIP = ROOT / "historical-nba-data-and-player-box-scores.zip"


def official_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    with zipfile.ZipFile(ZIP) as zf:
        games = pd.read_csv(zf.open("Games.csv"), usecols=[
            "gameId", "gameDate", "gameType", "hometeamId", "awayteamId",
            "homeScore", "awayScore"])
        players = pd.read_csv(zf.open("PlayerStatistics.csv"), usecols=[
            "firstName", "lastName", "personId", "gameId", "gameDate",
            "gameType", "numMinutes", "plusMinusPoints", "playerteamId",
            "opponentteamId"])
    games = games.rename(columns={"gameId": "game_id", "gameDate": "date"})
    players = players.rename(columns={"gameId": "game_id", "gameDate": "date",
                                      "personId": "player_id",
                                      "playerteamId": "team_id",
                                      "opponentteamId": "opponent_id",
                                      "numMinutes": "minutes_on",
                                      "plusMinusPoints": "on_diff"})
    for frame in (games, players):
        frame["game_id"] = pd.to_numeric(frame.game_id, errors="raise").astype("int64")
        frame["date"] = pd.to_datetime(frame.date).dt.normalize()
    players["player_name"] = (players.firstName.fillna("").str.strip() + " " +
                              players.lastName.fillna("").str.strip()).str.strip()
    players["minutes_on"] = pd.to_numeric(players.minutes_on, errors="coerce").fillna(0.0)
    players["on_diff"] = pd.to_numeric(players.on_diff, errors="coerce").fillna(0.0)
    players = players[players.minutes_on > 0].copy()
    return games, players


def exact_stint_on_totals() -> pd.DataFrame:
    path = EVIDENCE / "canonical_counted_stints_production.parquet"
    con = duckdb.connect()
    q = f"""
    WITH s AS (SELECT * FROM read_parquet('{path.as_posix()}')),
    home_on AS (
      SELECT game_id, home_id AS team_id, player_id,
        SUM(points_adjusted_home) AS on_pts_for_adj,
        SUM(points_adjusted_away) AS on_pts_against_adj,
        SUM(n_home) AS on_off_poss, SUM(n_away) AS on_def_poss
      FROM s, UNNEST([home_p1,home_p2,home_p3,home_p4,home_p5]) u(player_id)
      GROUP BY game_id, team_id, player_id),
    away_on AS (
      SELECT game_id, away_id AS team_id, player_id,
        SUM(points_adjusted_away) AS on_pts_for_adj,
        SUM(points_adjusted_home) AS on_pts_against_adj,
        SUM(n_away) AS on_off_poss, SUM(n_home) AS on_def_poss
      FROM s, UNNEST([away_p1,away_p2,away_p3,away_p4,away_p5]) u(player_id)
      GROUP BY game_id, team_id, player_id)
    SELECT * FROM home_on UNION ALL SELECT * FROM away_on
    """
    out = con.execute(q).fetch_df()
    con.close()
    for c in ("game_id", "team_id", "player_id"):
        out[c] = pd.to_numeric(out[c], errors="raise").astype("int64")
    return out


def game_evidence(games: pd.DataFrame) -> pd.DataFrame:
    audit = pd.read_parquet(EVIDENCE / "canonical_counted_game_audit.parquet")
    aggregate = pd.read_parquet(EVIDENCE / "canonical_counted_aggregate_production.parquet")
    audit["game_id"] = audit.game_id.astype("int64")
    aggregate["game_id"] = aggregate.game_id.astype("int64")
    base = games.merge(audit[["game_id", "adjusted_home_points", "adjusted_away_points",
                              "n_home", "n_away", "trusted_stint_level"]],
                       on="game_id", how="inner")
    missing_ids = sorted(set(aggregate.game_id) - set(base.game_id))
    if missing_ids:
        obs = (aggregate[aggregate.game_id.isin(missing_ids)]
               .drop_duplicates(["game_id", "offense_side"])
               .pivot(index="game_id", columns="offense_side",
                      values=["points", "possessions_proxy"]))
        rows = games[games.game_id.isin(missing_ids)].copy()
        rows["adjusted_home_points"] = rows.game_id.map(obs["points"]["home"])
        rows["adjusted_away_points"] = rows.game_id.map(obs["points"]["away"])
        rows["n_home"] = rows.game_id.map(obs["possessions_proxy"]["home"])
        rows["n_away"] = rows.game_id.map(obs["possessions_proxy"]["away"])
        rows["trusted_stint_level"] = False
        base = pd.concat([base, rows[base.columns]], ignore_index=True)
    # Five canonical source IDs have no Games.csv or PlayerStatistics.csv row;
    # they remain usable in RAPM but cannot produce an official player-game
    # on/off row.  Keep that exclusion explicit rather than inventing a boxscore.
    if base.game_id.nunique() != 37948:
        raise ValueError(f"expected 37,948 official games, found {base.game_id.nunique():,}")
    return base


def build() -> pd.DataFrame:
    games, players = official_data()
    evidence = game_evidence(games)
    players = players[players.game_id.isin(set(evidence.game_id))].copy()
    on = exact_stint_on_totals()
    # Historical official player rows often omit team IDs even though the
    # canonical lineup (or aggregate offense side) identifies them exactly.
    exact_team = (on[["game_id", "player_id", "team_id"]]
                  .drop_duplicates(["game_id", "player_id"]))
    aggregate = pd.read_parquet(EVIDENCE / "canonical_counted_aggregate_production.parquet")
    aggregate["game_id"] = aggregate.game_id.astype("int64")
    agg_team = aggregate[aggregate.role.eq("offense")][
        ["game_id", "player_id", "offense_side"]].drop_duplicates(["game_id", "player_id"])
    side_teams = games.set_index("game_id")
    agg_team["team_id"] = np.where(
        agg_team.offense_side.eq("home"),
        agg_team.game_id.map(side_teams.hometeamId),
        agg_team.game_id.map(side_teams.awayteamId))
    team_map = pd.concat([exact_team, agg_team[["game_id", "player_id", "team_id"]]],
                         ignore_index=True).drop_duplicates(["game_id", "player_id"])
    players = players.merge(team_map.rename(columns={"team_id": "inferred_team_id"}),
                            on=["game_id", "player_id"], how="left")
    players["team_id"] = pd.to_numeric(players.team_id, errors="coerce").fillna(
        players.inferred_team_id)
    players["opponent_id"] = pd.to_numeric(players.opponent_id, errors="coerce")
    gidx = games.set_index("game_id")
    home_ids = players.game_id.map(gidx.hometeamId)
    away_ids = players.game_id.map(gidx.awayteamId)
    team_from_opp = np.where(players.opponent_id.eq(home_ids), away_ids, home_ids)
    players["team_id"] = players.team_id.fillna(pd.Series(team_from_opp, index=players.index))
    is_home_team = players.team_id.eq(players.game_id.map(gidx.hometeamId))
    inferred_opp = np.where(is_home_team, players.game_id.map(gidx.awayteamId),
                            players.game_id.map(gidx.hometeamId))
    players["opponent_id"] = players.opponent_id.fillna(pd.Series(inferred_opp, index=players.index))
    players = players.drop(columns="inferred_team_id")
    # Some source dumps contain duplicate zero/comment records; retain the
    # longest official participation row for a player-game.
    players = (players.sort_values("minutes_on", ascending=False)
               .drop_duplicates(["game_id", "team_id", "player_id"]))
    out = players.merge(on, on=["game_id", "team_id", "player_id"], how="left")
    evcols = ["game_id", "hometeamId", "awayteamId", "homeScore", "awayScore",
              "adjusted_home_points", "adjusted_away_points", "n_home", "n_away",
              "trusted_stint_level"]
    out = out.merge(evidence[evcols], on="game_id", how="left", validate="many_to_one")
    home = out.team_id.eq(out.hometeamId)
    out["team_score"] = np.where(home, out.homeScore, out.awayScore)
    out["opp_score"] = np.where(home, out.awayScore, out.homeScore)
    out["team_adj"] = np.where(home, out.adjusted_home_points, out.adjusted_away_points)
    out["opp_adj"] = np.where(home, out.adjusted_away_points, out.adjusted_home_points)
    out["team_poss"] = np.where(home, out.n_home, out.n_away).astype(float)
    out["opp_poss"] = np.where(home, out.n_away, out.n_home).astype(float)

    team_minutes = out.groupby(["game_id", "team_id"]).minutes_on.transform("sum")
    share = (out.minutes_on / (team_minutes / 5.0).clip(lower=1)).clip(0, 1)
    fallback = out.on_off_poss.isna()
    out.loc[fallback, "on_off_poss"] = out.loc[fallback, "team_poss"] * share[fallback]
    out.loc[fallback, "on_def_poss"] = out.loc[fallback, "opp_poss"] * share[fallback]
    volume_share = share * (out.team_adj + out.opp_adj)
    out.loc[fallback, "on_pts_for_adj"] = ((volume_share + out.on_diff) / 2)[fallback]
    out.loc[fallback, "on_pts_against_adj"] = ((volume_share - out.on_diff) / 2)[fallback]
    out["evidence_resolution"] = np.where(
        fallback, "aggregate_exact_possessions_official_minutes", "stint_exact_counted")

    out["off_off_poss"] = (out.team_poss - out.on_off_poss).clip(lower=0)
    out["off_def_poss"] = (out.opp_poss - out.on_def_poss).clip(lower=0)
    out["off_pts_for_adj"] = out.team_adj - out.on_pts_for_adj
    out["off_pts_against_adj"] = out.opp_adj - out.on_pts_against_adj
    # Preserve official player-game plus-minus while allocating the raw scoring
    # volume according to the canonical adjusted on-court split.
    on_volume = out.on_pts_for_adj + out.on_pts_against_adj
    out["on_pts_for"] = (on_volume + out.on_diff) / 2
    out["on_pts_against"] = (on_volume - out.on_diff) / 2
    out["off_pts_for"] = out.team_score - out.on_pts_for
    out["off_pts_against"] = out.opp_score - out.on_pts_against
    out["off_diff"] = out.off_pts_for - out.off_pts_against
    out["on_pts_for_adj"] = out.on_pts_for_adj.astype(float)
    out["on_pts_against_adj"] = out.on_pts_against_adj.astype(float)
    out["on_diff_adj"] = out.on_pts_for_adj - out.on_pts_against_adj
    out["off_diff_adj"] = out.off_pts_for_adj - out.off_pts_against_adj
    out["on_off_diff"] = out.on_diff - out.off_diff
    out["on_off_diff_adj"] = out.on_diff_adj - out.off_diff_adj

    cols = ["game_id", "team_id", "opponent_id", "player_id", "player_name", "date",
            "gameType", "minutes_on", "on_pts_for", "on_pts_against", "on_diff",
            "off_pts_for", "off_pts_against", "off_diff", "on_pts_for_adj",
            "on_pts_against_adj", "on_diff_adj", "off_pts_for_adj",
            "off_pts_against_adj", "off_diff_adj", "on_off_diff", "on_off_diff_adj",
            "on_off_poss", "on_def_poss", "off_off_poss", "off_def_poss",
            "evidence_resolution"]
    out = out[cols].sort_values(["date", "game_id", "team_id", "player_id"])
    if (out[["on_off_poss", "on_def_poss", "off_off_poss", "off_def_poss"]] < -1e-8).any().any():
        raise ValueError("player-game has negative possession exposure")
    official_err = (out.on_diff - (out.on_pts_for - out.on_pts_against)).abs().max()
    if official_err > 1e-8:
        raise ValueError(f"official plus-minus mismatch: {official_err}")
    return out


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    out = build()
    # NBA Cup labels changed over time; canonical game-ID prefixes are stable.
    regular = out[out.game_id.astype(str).str.startswith("2")].copy()
    playoffs = out[out.game_id.astype(str).str.startswith("4")].copy()
    for label, frame in (("regular", regular), ("playoffs", playoffs)):
        stem = OUT / f"adjusted_onoff_{label}_canonical_counted"
        frame.to_parquet(stem.with_suffix(".parquet"), index=False)
        frame.to_csv(stem.with_suffix(".csv"), index=False)
        print(label, f"{len(frame):,} player-games", f"{frame.game_id.nunique():,} games",
              frame.evidence_resolution.value_counts().to_dict())


if __name__ == "__main__":
    main()
