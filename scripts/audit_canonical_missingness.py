"""Quantify selection bias from games excluded from the canonical candidate."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "nba_analytics.duckdb"
QA = ROOT / "outputs" / "contextual_causal" / "canonical_game_integrity.parquet"
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
OUT = ROOT / "outputs" / "contextual_causal"


def norm(values: pd.Series) -> pd.Series:
    return values.astype(str).str.split(".").str[0].str.lstrip("0").replace("", "0")


def main() -> None:
    qa = pd.read_parquet(QA, columns=["game_id", "season_year"])
    qa["game_id"] = norm(qa.game_id)
    manifest = pd.read_csv(REBUILD / "canonical_game_source_manifest.csv",
                           dtype={"game_id": str})
    selected = set(norm(manifest.game_id))
    qa["selected"] = qa.game_id.isin(selected)

    season = (qa.groupby("season_year")
              .agg(total_games=("game_id", "nunique"),
                   selected_games=("selected", "sum"))
              .reset_index())
    season["unresolved_games"] = season.total_games - season.selected_games
    season["unresolved_rate"] = season.unresolved_games / season.total_games

    con = duckdb.connect(str(DB), read_only=True)
    facts = con.execute("""
        SELECT ltrim(game_id, '0') game_id, season, player_id, player_name,
               team_id, team_abbr, minutes
        FROM player_game_facts WHERE minutes > 0
    """).df()
    con.close()
    facts["game_id"] = norm(facts.game_id)
    facts["unresolved"] = ~facts.game_id.isin(selected)

    team_game = facts[["game_id", "team_id", "team_abbr", "unresolved"]].drop_duplicates()
    team = (team_game.groupby(["team_id", "team_abbr"])
            .agg(total_games=("game_id", "nunique"),
                 unresolved_games=("unresolved", "sum"))
            .reset_index())
    team["unresolved_rate"] = team.unresolved_games / team.total_games

    player = (facts.groupby(["player_id", "player_name"])
              .agg(total_minutes=("minutes", "sum"),
                   total_games=("game_id", "nunique"))
              .reset_index())
    missing_player = (facts.loc[facts.unresolved]
                      .groupby(["player_id", "player_name"])
                      .agg(unresolved_minutes=("minutes", "sum"),
                           unresolved_games=("game_id", "nunique"))
                      .reset_index())
    player = player.merge(missing_player, on=["player_id", "player_name"], how="left")
    player[["unresolved_minutes", "unresolved_games"]] = player[
        ["unresolved_minutes", "unresolved_games"]].fillna(0)
    player["unresolved_minute_rate"] = player.unresolved_minutes / player.total_minutes

    unresolved = qa.loc[~qa.selected, ["game_id", "season_year"]].drop_duplicates()
    unresolved.to_csv(REBUILD / "canonical_unresolved_games.csv", index=False)
    season.to_csv(OUT / "canonical_missingness_by_season.csv", index=False)
    team.sort_values(["unresolved_rate", "unresolved_games"], ascending=False).to_csv(
        OUT / "canonical_missingness_by_team.csv", index=False)
    player.sort_values(["unresolved_minute_rate", "unresolved_minutes"], ascending=False).to_csv(
        OUT / "canonical_missingness_by_player.csv", index=False)

    material = player.loc[player.total_minutes >= 500]
    high = material.loc[material.unresolved_minute_rate > .10]
    lines = [
        "# Canonical candidate missingness audit",
        "",
        f"- Selected games: {qa.selected.sum():,} / {len(qa):,} "
        f"({qa.selected.mean():.2%})",
        f"- Unresolved games: {(~qa.selected).sum():,} ({(~qa.selected).mean():.2%})",
        f"- Seasons with >3% unresolved: {(season.unresolved_rate > .03).sum()}",
        f"- Teams with >3% unresolved: {(team.unresolved_rate > .03).sum()}",
        f"- Players with >=500 total minutes and >10% unresolved: {len(high)}",
        "",
        "## Highest unresolved season rates",
        "",
        season.sort_values("unresolved_rate", ascending=False).head(10).to_markdown(index=False),
        "",
        "## Most exposed established players",
        "",
        material.sort_values("unresolved_minute_rate", ascending=False).head(20).to_markdown(index=False),
    ]
    (OUT / "canonical_missingness_audit.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines[:7]))


if __name__ == "__main__":
    main()
