"""Build non-overlapping foul-generation candidates atop atomic features.

Writes only a versioned contextual-causal feature artifact.  The old and
promoted atomic caches are not modified.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(r"C:\Users\Dave\Downloads\nba-metric-data")
PBP = DATA / "PlayByPlay.parquet"
ATOMIC = DATA / "features_atomic_denominator_season.parquet"
BOX = DATA / "features_box_season.parquet"
OUT = (ROOT / "derived" / "contextual_causal"
       / "features_atomic_foul_decomposed_season.parquet")
AUDIT = (ROOT / "outputs" / "contextual_causal"
         / "atomic_foul_feature_coverage_audit.csv")

NEW_ATOMS = ["total_fouls_drawn_75", "ft_foul_trips_drawn_75",
             "other_fouls_drawn_75"]


def load_ft_foul_trips() -> pd.DataFrame:
    con = duckdb.connect()
    ft = con.execute(f"""
        SELECT gameId, actionNumber, personId, gameDateTimeEst,
               coalesce(subType, '') sub_type,
               coalesce(descriptor, '') descriptor,
               coalesce(description, '') description
        FROM read_parquet('{PBP.as_posix()}')
        WHERE trim(actionType) IN ('Free Throw', 'freethrow')
    """).df()
    con.close()
    ft["game_id"] = ft.gameId.astype(str).str.lstrip("0")
    ft = ft[ft.game_id.str.startswith("2")].copy()
    ft = ft.drop_duplicates(["game_id", "actionNumber"])
    text = (ft.sub_type + " " + ft.descriptor + " " + ft.description)
    normalized = text.str.lower().str.replace(r"[^a-z0-9]+", "",
                                               regex=True)
    nontechnical = ~normalized.str.contains("technical", na=False)
    first = normalized.str.contains(r"1of[123]", regex=True, na=False)
    # Some early clear-path one-shot rows have no ordinal in the subtype.
    legacy_clear_path = (normalized.str.contains("freethrowclearpath", na=False)
                         & ~normalized.str.contains(r"[123]of[123]", regex=True,
                                                    na=False))
    ft = ft[nontechnical & (first | legacy_clear_path)].copy()
    ft["pid"] = pd.to_numeric(ft.personId, errors="coerce")
    ft = ft.dropna(subset=["pid"])
    ft["pid"] = ft.pid.astype(int)
    date = pd.to_datetime(ft.gameDateTimeEst, errors="coerce")
    ft["season_year"] = date.dt.year - (date.dt.month < 10)
    return (ft.groupby(["pid", "season_year"]).size()
            .rename("ft_foul_trips").reset_index())


def main() -> None:
    atoms = pd.read_parquet(ATOMIC)
    box = pd.read_parquet(BOX)[
        ["pid", "season_year", "fouls_drawn", "fouls_drawn_75", "fta"]]
    trips = load_ft_foul_trips()
    out = atoms.merge(box, on=["pid", "season_year"], how="left")
    out = out.merge(trips, on=["pid", "season_year"], how="left")
    out["ft_foul_trips"] = out.ft_foul_trips.fillna(0.0)

    reliable_total = out.season_year >= 2005
    out["total_fouls_drawn_75"] = out.fouls_drawn_75.where(reliable_total)
    out["ft_foul_trips_drawn_75"] = (
        out.ft_foul_trips / out.poss.clip(lower=1) * 75.0)

    # Charges are season counts expressed as a coverage-aware per-75 rate.
    # Subtract them only when the source exists; before that era they remain
    # bundled into `other` while the separate charge atom is missing.
    charge_count = (out.charges_drawn_75
                    * out.charges_drawn_75__denom / 75.0)
    charge_count = charge_count.where(
        out.charges_drawn_75__denom.notna(), 0.0).fillna(0.0)
    other_count = out.fouls_drawn - out.ft_foul_trips - charge_count
    disagreement = reliable_total & (other_count < -1e-8)
    out["_negative_other"] = disagreement
    out["other_fouls_drawn_75"] = (
        other_count / out.poss.clip(lower=1) * 75.0).where(
            reliable_total & ~disagreement)

    for feature in NEW_ATOMS:
        out[feature + "__denom"] = out.poss.where(out[feature].notna())

    # Audit source coherence by season before writing the feature artifact.
    audit = (out.groupby("season_year").agg(
        players=("pid", "size"),
        poss=("poss", "sum"),
        total_fouls=("fouls_drawn", "sum"),
        ft_attempts=("fta", "sum"),
        ft_foul_trips=("ft_foul_trips", "sum"),
        negative_other=("_negative_other", "sum"))
        .reset_index())
    audit["trips_per_fta"] = audit.ft_foul_trips / audit.ft_attempts.clip(lower=1)
    audit["total_fouls_per100"] = audit.total_fouls / audit.poss.clip(lower=1) * 100
    AUDIT.parent.mkdir(parents=True, exist_ok=True)
    audit.to_csv(AUDIT, index=False)

    keep = list(atoms.columns) + NEW_ATOMS + [f + "__denom" for f in NEW_ATOMS]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out[keep].to_parquet(OUT, index=False)
    print(f"wrote {len(out)} rows -> {OUT}")
    print(audit.to_string(index=False))


if __name__ == "__main__":
    main()
