"""Prepare conservative RAPM inputs from probabilistic lineup salvage.

Score-consistent games receive whole-game lineup draws.  Games for which no
candidate reproduces the official score are retained only as game-level,
minute-weighted observations; this uses their information without fabricating
the timing of missing points.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "nba_analytics.duckdb"
BASE = ROOT / "derived" / "contextual_causal" / "probabilistic_lineup_salvage"
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
REPORT = ROOT / "outputs" / "contextual_causal"


def norm(values: pd.Series) -> pd.Series:
    return (values.astype(str).str.split(".").str[0].str.lstrip("0")
            .replace("", "0"))


def main() -> None:
    probabilities = pd.read_csv(BASE / "candidate_probabilities.csv",
                                dtype={"game_id": str})
    probabilities["game_id"] = norm(probabilities.game_id)
    probabilities["raw_score_error"] = np.expm1(probabilities.log_score_error)
    probabilities["score_consistent"] = ((probabilities.raw_score_error < .5)
                                         & (probabilities.coverage_ok >= .5))
    any_good = probabilities.groupby("game_id").score_consistent.transform("any")
    probabilities["rapm_candidate_probability"] = np.where(
        any_good & probabilities.score_consistent, probabilities.probability, 0.0)
    denom = probabilities.groupby("game_id").rapm_candidate_probability.transform("sum")
    probabilities.loc[denom > 0, "rapm_candidate_probability"] /= denom[denom > 0]
    good_games = set(probabilities.loc[
        probabilities.rapm_candidate_probability > 0, "game_id"])
    all_games = set(probabilities.game_id)
    aggregate_games = all_games - good_games

    bank = pd.read_parquet(BASE / "candidate_stint_bank.parquet")
    bank["game_id"] = norm(bank.game_id)
    keys = probabilities.loc[probabilities.rapm_candidate_probability > 0,
                             ["game_id", "candidate_id",
                              "rapm_candidate_probability"]]
    bank = bank.merge(keys, on=["game_id", "candidate_id"], how="inner")
    bank.to_parquet(BASE / "rapm_score_consistent_candidate_bank.parquet", index=False)

    groups = {(gid, cid): game.copy() for (gid, cid), game in
              bank.groupby(["game_id", "candidate_id"])}
    rng = np.random.default_rng(20260718)
    draws = []; manifest = []
    for imputation_id in range(20):
        for gid, game in keys.groupby("game_id"):
            p = game.rapm_candidate_probability.to_numpy(float)
            chosen = int(rng.choice(len(game), p=p))
            row = game.iloc[chosen]
            stints = groups[(gid, row.candidate_id)].copy()
            stints["imputation_id"] = imputation_id
            stints["rapm_candidate_probability"] = row.rapm_candidate_probability
            stints["canonical_source"] = "probabilistic_score_consistent_salvage"
            draws.append(stints)
            manifest.append({"imputation_id": imputation_id, "game_id": gid,
                             "candidate_id": row.candidate_id,
                             "probability": row.rapm_candidate_probability})
    pd.concat(draws, ignore_index=True).to_parquet(
        BASE / "rapm_imputed_stints_20.parquet", index=False)
    pd.DataFrame(manifest).to_csv(
        BASE / "rapm_imputation_manifest_20.csv", index=False)

    # The fallback is intentionally at game grain.  Player exposure comes from
    # official minutes, while outcome comes from the official final score.
    ids = pd.DataFrame({"game_id": sorted(all_games)})
    con = duckdb.connect(str(DB), read_only=True)
    con.register("wanted", ids)
    facts = con.execute("""
        SELECT ltrim(p.game_id, '0') game_id, p.date, p.player_id, p.team_id,
               p.home_away, p.minutes, p.team_pts_actual
        FROM player_game_facts p JOIN wanted w
          ON ltrim(p.game_id, '0') = w.game_id
        WHERE p.minutes > 0
    """).df()
    con.close()
    rows = []
    for gid, game in facts.groupby("game_id"):
        totals = game.groupby("home_away").team_pts_actual.max()
        team_minutes = game.groupby("home_away").minutes.sum()
        game_minutes = float(team_minutes.mean() / 5.0)
        possessions_proxy = max(game_minutes * 60.0 / 24.0, .1)
        for offense_side, defense_side in (("home", "away"), ("away", "home")):
            obs = f"{gid}_{offense_side}_offense"
            points = float(totals[offense_side])
            for role, side, sign in (("offense", offense_side, 1.0),
                                     ("defense", defense_side, 1.0)):
                players = game[game.home_away == side]
                for player in players.itertuples():
                    rows.append({
                        "observation_id": obs, "game_id": gid,
                        "date": player.date,
                        "offense_side": offense_side, "role": role,
                        "player_id": int(player.player_id),
                        "design_value": sign * float(player.minutes) / game_minutes,
                        "points": points, "possessions_proxy": possessions_proxy,
                        "target_per_100": points / possessions_proxy * 100.0,
                        "sqrt_weight": np.sqrt(possessions_proxy),
                        "information_tier": "aggregate_official_minutes_score",
                    })
    all_aggregate = pd.DataFrame(rows)
    aggregate = all_aggregate[all_aggregate.game_id.isin(aggregate_games)].copy()
    aggregate.to_parquet(BASE / "rapm_aggregate_fallback_design.parquet", index=False)
    all_aggregate.to_parquet(
        BASE / "rapm_all_quarantined_aggregate_design.parquet", index=False)
    probabilities.to_csv(BASE / "rapm_candidate_probabilities.csv", index=False)

    audit = pd.DataFrame([{
        "total_quarantined_games": len(all_games),
        "score_consistent_imputed_games": len(good_games),
        "aggregate_fallback_games": len(aggregate_games),
        "games_left_out": len(all_games - good_games - aggregate_games),
        "imputations": 20,
        "aggregate_observations": aggregate.observation_id.nunique(),
        "aggregate_design_rows": len(aggregate),
    }])
    audit.to_csv(REPORT / "probabilistic_salvage_rapm_input_audit.csv", index=False)
    print(audit.to_string(index=False))


if __name__ == "__main__":
    main()
