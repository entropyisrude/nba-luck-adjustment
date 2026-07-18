"""Apply the frozen denominator-aware atomic prior to cached counted evidence.

This reuses the previously audited counted-stint artifact, so it changes only
the prior center and writes a new non-production comparison artifact.
"""
from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from test_probabilistic_salvage_rapm_sensitivity import aggregate_design, norm
from build_nerd_canonical_salvage_preview import (
    ALPHA, ATOMIC, ATOMIC_RAW, CURRENT, TARGET_YEAR, V1, fit_centered, prior_vector,
    season_year)
from build_nerd_canonical_counted_preview import (
    ACOLS, HCOLS, WINDOW_DAYS, build_counted_part)

ROOT = Path(__file__).resolve().parents[1]
REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
SALVAGE = ROOT / "derived" / "contextual_causal" / "probabilistic_lineup_salvage"
METRIC_DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
OUT = ROOT / "outputs" / "contextual_causal"
DENOM = OUT / "rolling_prior_atomic_denominator_poss.parquet"


def main() -> None:
    canonical = pd.read_parquet(REBUILD / "canonical_stints_candidate.parquet")
    canonical["game_id"] = norm(canonical.game_id)
    canonical["date"] = pd.to_datetime(canonical.date)
    probs = pd.read_csv(SALVAGE / "rapm_candidate_probabilities.csv",
                        dtype={"game_id": str, "candidate_id": str})
    probs["game_id"] = norm(probs.game_id)
    best = (probs[probs.rapm_candidate_probability > 0]
            .sort_values("rapm_candidate_probability", ascending=False)
            .drop_duplicates("game_id")[["game_id", "candidate_id"]])
    bank = pd.read_parquet(
        SALVAGE / "rapm_score_consistent_candidate_bank.parquet")
    bank["game_id"] = norm(bank.game_id)
    bank["candidate_id"] = bank.candidate_id.astype(str)
    salvage = bank.merge(best, on=["game_id", "candidate_id"], how="inner")
    salvage["date"] = pd.to_datetime(salvage.date)
    prepared = pd.read_parquet(METRIC_DATA / "prepared_stints.parquet")
    prepared["game_id"] = norm(prepared.game_id)
    prepared["date"] = pd.to_datetime(prepared.date)
    playoffs = prepared[prepared.game_id.str.startswith("4")].copy()
    end = canonical.loc[season_year(canonical.date) == TARGET_YEAR, "date"].max()
    start = end - pd.Timedelta(days=WINDOW_DAYS)
    st = pd.concat([canonical, salvage, playoffs], ignore_index=True, sort=False)
    st = st[(st.date >= start) & (st.date <= end)].copy()
    st = st.sort_values(["game_id", "start_elapsed"])
    st["stint_index"] = st.groupby("game_id").cumcount()

    audit = pd.read_csv(OUT / "nerd_canonical_counted_game_audit.csv",
                        dtype={"game_id": str})
    audit["game_id"] = norm(audit.game_id)
    trusted = set(audit.loc[audit.trusted.astype(bool), "game_id"])
    counts = pd.read_parquet(SALVAGE / "nerd_canonical_counted_stints.parquet")
    counts["game_id"] = norm(counts.game_id)

    aggregate = pd.read_parquet(
        SALVAGE / "rapm_aggregate_fallback_design.parquet")
    aggregate["game_id"] = norm(aggregate.game_id)
    aggregate["date"] = pd.to_datetime(aggregate.date)
    untrusted_salvage = set(salvage.game_id) - trusted
    if untrusted_salvage:
        extra = pd.read_parquet(
            SALVAGE / "rapm_all_quarantined_aggregate_design.parquet")
        extra["game_id"] = norm(extra.game_id)
        extra["date"] = pd.to_datetime(extra.date)
        extra = extra[extra.game_id.isin(untrusted_salvage)
                      & ~extra.game_id.isin(set(aggregate.game_id))]
        aggregate = pd.concat([aggregate, extra], ignore_index=True)

    players = (set(st[HCOLS + ACOLS].dropna().to_numpy(int).ravel())
               | set(aggregate.player_id.astype(int)))
    players = np.array(sorted(players), int)
    pidx = {p: i for i, p in enumerate(players)}
    counted_part, used_st = build_counted_part(
        st[st.game_id.isin(trusted)], counts, pidx, end)
    aggregate_part = aggregate_design(aggregate, pidx, end)
    priors = {"v1": prior_vector(V1, pidx),
              "atomic_raw": prior_vector(ATOMIC_RAW, pidx),
              "atomic_denom": prior_vector(DENOM, pidx)}

    result = pd.DataFrame({"player_id": players})
    for name, (b0, _) in priors.items():
        beta = fit_centered([counted_part, aggregate_part], len(players), b0)
        result[f"nerd_{name}_o"] = beta[:len(players)]
        result[f"nerd_{name}_d"] = -beta[len(players):]
        result[f"nerd_{name}"] = beta[:len(players)] - beta[len(players):]
        result[f"prior_{name}_o"] = b0[:len(players)]
        result[f"prior_{name}_d"] = -b0[len(players):]

    cur = used_st[season_year(used_st.date) == TARGET_YEAR]
    exposure = np.zeros(len(players))
    for ncol, cols in (("n_home", HCOLS), ("n_away", ACOLS)):
        nn = cur[ncol].to_numpy(float)
        idx = np.vectorize(pidx.get)(cur[cols].to_numpy(int))
        for k in range(5):
            np.add.at(exposure, idx[:, k], nn)
    result["poss_season"] = exposure

    con = duckdb.connect(str(ROOT / "data" / "nba_analytics.duckdb"),
                         read_only=True)
    names = con.execute("""
        SELECT CAST(player_id AS BIGINT) player_id,
               max_by(player_name,date) player_name,
               max_by(team_abbr,date) team_abbr
        FROM player_game_facts GROUP BY 1
    """).df()
    con.close()
    result = result.merge(names, on="player_id", how="left")
    if CURRENT.exists():
        old = pd.read_parquet(CURRENT)
        old = old[old.season_year == TARGET_YEAR][["player_id", "nerd"]]
        result = result.merge(old.rename(columns={"nerd": "previous_candidate"}),
                              on="player_id", how="left")
    result["denom_minus_raw"] = (result.nerd_atomic_denom
                                  - result.nerd_atomic_raw)
    result["denom_minus_v1"] = result.nerd_atomic_denom - result.nerd_v1
    result = result[result.poss_season > 0].sort_values(
        "nerd_atomic_denom", ascending=False)
    path = OUT / "nerd_canonical_counted_denominator_atomic_preview"
    result.to_parquet(path.with_suffix(".parquet"), index=False)
    result.to_csv(path.with_suffix(".csv"), index=False)
    print(result[["player_name", "team_abbr", "poss_season",
                  "nerd_atomic_denom_o", "nerd_atomic_denom_d",
                  "nerd_atomic_denom", "nerd_v1"]].head(30).to_string(
                      index=False))


if __name__ == "__main__":
    main()
