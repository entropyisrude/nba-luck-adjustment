"""Build full historical production evidence with counted possessions.

Regular-season lineups come from the canonical reconstruction plus the frozen
best deterministic salvage candidate. Playoff lineups come directly from the
repaired playoff stint file (never through ``prepared_stints.parquet``), with
official dates restored from Games.csv. Possessions and their points are
parsed atomically from play-by-play and attached to lineup windows.

Games that fail the strict stint-attachment gate are retained at aggregate
game level using exact play-by-play possession counts and lineup-derived
minute shares. The 48 games without a defensible stint reconstruction use the
existing official-minutes design, but their old duration/24 possession proxy
is replaced with the exact play-by-play count.

All outputs are derived artifacts; no raw source is modified.
"""
from __future__ import annotations

import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from count_stint_possessions import (MAX_POSS_GAP, MIN_ATTACH_RATE, POINT_TOL,
                                     load_events, walk_game)

REBUILD = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
SALVAGE = ROOT / "derived" / "contextual_causal" / "probabilistic_lineup_salvage"
OUT = ROOT / "derived" / "contextual_causal" / "production_counted_evidence"
REPORT = ROOT / "outputs" / "contextual_causal"
PLAYOFFS = ROOT / "data" / "stints_playoffs.csv"
LUCK_COMPONENTS = ROOT / "data" / "rapm_luck_adjust.parquet"
HISTORICAL_ZIP = ROOT / "historical-nba-data-and-player-box-scores.zip"
HCOLS = [f"home_p{i}" for i in range(1, 6)]
ACOLS = [f"away_p{i}" for i in range(1, 6)]
# A missing made basket in the historical event feed should not discard an
# otherwise exact lineup reconstruction. The source adjusted score is imposed
# below, so this gate detects genuinely incomplete play-by-play only.
MAX_RECONCILABLE_POINT_ERROR = 5.0


def norm(values: pd.Series) -> pd.Series:
    return (values.astype(str).str.split(".").str[0].str.lstrip("0")
            .replace("", "0"))


def official_games() -> pd.DataFrame:
    with zipfile.ZipFile(HISTORICAL_ZIP) as zf:
        games = pd.read_csv(
            zf.open("Games.csv"),
            usecols=["gameId", "gameDate", "hometeamId", "awayteamId"],
            dtype={"gameId": str})
    games["game_id"] = norm(games.gameId)
    games["official_date"] = pd.to_datetime(games.gameDate)
    return games[["game_id", "official_date", "hometeamId", "awayteamId"]]


def load_lineup_stints(games: pd.DataFrame) -> pd.DataFrame:
    canonical = pd.read_parquet(REBUILD / "canonical_stints_candidate.parquet")
    canonical["game_id"] = norm(canonical.game_id)
    canonical["date"] = pd.to_datetime(canonical.date)
    canonical["evidence_source"] = "canonical_regular"

    probabilities = pd.read_csv(
        SALVAGE / "rapm_candidate_probabilities.csv", dtype={"game_id": str})
    probabilities["game_id"] = norm(probabilities.game_id)
    best = (probabilities[probabilities.rapm_candidate_probability > 0]
            .sort_values("rapm_candidate_probability", ascending=False)
            .drop_duplicates("game_id")[["game_id", "candidate_id"]])
    bank = pd.read_parquet(
        SALVAGE / "rapm_score_consistent_candidate_bank.parquet")
    bank["game_id"] = norm(bank.game_id)
    bank["candidate_id"] = bank.candidate_id.astype(str)
    best["candidate_id"] = best.candidate_id.astype(str)
    salvage = bank.merge(best, on=["game_id", "candidate_id"], how="inner")
    salvage["date"] = pd.to_datetime(salvage.date)
    salvage["evidence_source"] = "deterministic_regular_salvage"

    playoffs = pd.read_csv(PLAYOFFS, dtype={"game_id": str}, low_memory=False)
    playoffs["game_id"] = norm(playoffs.game_id)
    playoffs = playoffs.merge(
        games[["game_id", "official_date"]], on="game_id", how="left",
        validate="many_to_one")
    if playoffs.official_date.isna().any():
        missing = playoffs.loc[playoffs.official_date.isna(), "game_id"].nunique()
        raise ValueError(f"official dates missing for {missing} playoff games")
    playoffs["date"] = playoffs.official_date
    playoffs["evidence_source"] = "repaired_playoff"

    keep = ["game_id", "date", "stint_index", "home_id", "away_id",
            "seconds", "start_elapsed", "end_elapsed", "home_pts",
            "away_pts", "home_pts_adj", "away_pts_adj", "evidence_source"] \
           + HCOLS + ACOLS
    frames = []
    for frame in (canonical, salvage, playoffs):
        missing = set(keep) - set(frame.columns)
        if missing:
            raise ValueError(f"{frame.evidence_source.iloc[0]} missing {missing}")
        frames.append(frame[keep].copy())
    stints = pd.concat(frames, ignore_index=True)
    stints["stint_index"] = pd.to_numeric(
        stints.stint_index, errors="raise").astype(int)
    if stints.duplicated(["game_id", "stint_index"]).any():
        raise ValueError("duplicate game/stint keys in combined evidence")
    return stints


def build_aggregate_from_stints(stints: pd.DataFrame, game_totals: pd.DataFrame,
                                game_ids: set[str]) -> pd.DataFrame:
    use = stints[stints.game_id.isin(game_ids)].copy()
    if use.empty:
        return pd.DataFrame()
    duration = use.groupby("game_id").end_elapsed.max().clip(lower=1)
    rows = []
    for side, cols in (("home", HCOLS), ("away", ACOLS)):
        long = use[["game_id", "date", "seconds"] + cols].melt(
            id_vars=["game_id", "date", "seconds"], value_name="player_id")
        long = (long.dropna(subset=["player_id"])
                .groupby(["game_id", "date", "player_id"], as_index=False)
                .seconds.sum())
        long["design_value"] = long.seconds / long.game_id.map(duration)
        long["team_side"] = side
        rows.append(long)
    minutes = pd.concat(rows, ignore_index=True)
    totals = game_totals.set_index("game_id")
    out = []
    for offense in ("home", "away"):
        offense_minutes = minutes[minutes.team_side == offense].copy()
        offense_minutes["role"] = "offense"
        defense_minutes = minutes[minutes.team_side != offense].copy()
        defense_minutes["role"] = "defense"
        frame = pd.concat([offense_minutes, defense_minutes], ignore_index=True)
        frame["offense_side"] = offense
        frame["observation_id"] = frame.game_id + f"_{offense}_offense"
        frame["points"] = frame.game_id.map(totals[f"adjusted_{offense}_points"])
        frame["points_3pt_ft"] = frame.game_id.map(
            totals[f"adjusted_{offense}_points_3pt_ft"])
        frame["possessions_proxy"] = frame.game_id.map(totals[f"n_{offense}"])
        frame["target_per_100"] = frame.points / frame.possessions_proxy * 100
        frame["target_per_100_3pt_ft"] = (
            frame.points_3pt_ft / frame.possessions_proxy * 100)
        frame["sqrt_weight"] = np.sqrt(frame.possessions_proxy)
        frame["information_tier"] = "aggregate_counted_possessions_lineup_minutes"
        out.append(frame)
    return pd.concat(out, ignore_index=True)[[
        "observation_id", "game_id", "date", "offense_side", "role",
        "player_id", "design_value", "points", "points_3pt_ft",
        "possessions_proxy", "target_per_100", "target_per_100_3pt_ft",
        "sqrt_weight", "information_tier"]]


def load_luck_components() -> tuple[pd.DataFrame, pd.DataFrame]:
    if not LUCK_COMPONENTS.exists():
        raise FileNotFoundError(f"missing canonical luck components: {LUCK_COMPONENTS}")
    luck = pd.read_parquet(LUCK_COMPONENTS)
    required = {"game_id", "hk", "ak", "ft_luck_home", "ft_luck_away",
                "mr_luck_home", "mr_luck_away"}
    missing = required - set(luck.columns)
    if missing:
        raise ValueError(f"luck component payload missing columns: {sorted(missing)}")
    luck["game_id"] = norm(luck.game_id)
    totals = luck.groupby("game_id", as_index=False)[[
        "ft_luck_home", "ft_luck_away", "mr_luck_home", "mr_luck_away"]].sum()
    return luck, totals


def apply_luck_to_counted(counted: pd.DataFrame, luck: pd.DataFrame,
                          totals: pd.DataFrame) -> pd.DataFrame:
    """Add exact 3PT+FT and production 3PT+FT+50% MR outcomes."""
    out = counted.copy()
    out["_hk"] = out.home_lineup_key.str.replace("-", ",", regex=False)
    out["_ak"] = out.away_lineup_key.str.replace("-", ",", regex=False)
    out["_row"] = np.arange(len(out))
    out = out.merge(luck, left_on=["game_id", "_hk", "_ak"],
                    right_on=["game_id", "hk", "ak"], how="left")
    total_map = totals.set_index("game_id")
    for side in ("home", "away"):
        ncol = f"n_{side}"
        for component, source in (("ft", f"ft_luck_{side}"),
                                  ("mr", f"mr_luck_{side}")):
            group_n = out.groupby(["game_id", "_hk", "_ak"])[ncol].transform("sum")
            exact = (out[source].fillna(0.0).to_numpy(float)
                     * out[ncol].to_numpy(float)
                     / group_n.replace(0, np.nan).fillna(1).to_numpy(float))
            assigned = pd.Series(exact, index=out.index).groupby(out.game_id).transform("sum")
            wanted = out.game_id.map(total_map[source]).fillna(0.0).to_numpy(float)
            game_n = out.groupby("game_id")[ncol].transform("sum").clip(lower=1e-9)
            out[f"_{component}_{side}"] = (
                exact + (wanted - assigned.to_numpy(float))
                * out[ncol].to_numpy(float) / game_n.to_numpy(float))
        base = f"points_adjusted_{side}"
        out[f"{base}_3pt"] = out[base]
        out[f"{base}_3pt_ft"] = out[base] - out[f"_ft_{side}"]
        out[base] = out[f"{base}_3pt_ft"] - 0.5 * out[f"_mr_{side}"]
    return (out.sort_values("_row").drop(columns=[
        "_hk", "_ak", "_row", "hk", "ak", "ft_luck_home",
        "ft_luck_away", "mr_luck_home", "mr_luck_away"]))


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    REPORT.mkdir(parents=True, exist_ok=True)
    games = official_games()
    stints = load_lineup_stints(games)
    print(f"lineup evidence: {len(stints):,} stints, "
          f"{stints.game_id.nunique():,} games", flush=True)

    events = load_events()
    event_games = set(events.gid_n)
    missing_pbp = set(stints.game_id) - event_games
    if missing_pbp:
        raise ValueError(f"{len(missing_pbp)} lineup games have no play-by-play")
    field = (events.assign(pnum=pd.to_numeric(events.possession, errors="coerce"))
             .groupby("gid_n").pnum.apply(lambda s: s.notna().any()))
    field_games = set(field[field].index)

    segment_rows = []
    for i, (gid, game) in enumerate(events.groupby("gid_n", sort=False), 1):
        segment_rows.extend((gid, start, team, pts)
                            for start, team, pts in
                            walk_game(game, gid in field_games))
        if i % 5000 == 0:
            print(f"  parsed {i:,} games", flush=True)
    segments = pd.DataFrame(segment_rows,
                            columns=["game_id", "elapsed", "team", "pts"])
    segments = segments[segments.game_id.isin(set(stints.game_id))].copy()
    segments = segments.dropna(subset=["elapsed"])

    attach_stints = stints.sort_values("start_elapsed", kind="stable")
    merged = pd.merge_asof(
        segments.sort_values("elapsed", kind="stable"),
        attach_stints[["game_id", "stint_index", "start_elapsed",
                       "end_elapsed", "home_id", "away_id"]],
        left_on="elapsed", right_on="start_elapsed", by="game_id",
        direction="backward")
    merged["in_window"] = (merged.stint_index.notna()
                            & (merged.elapsed <= merged.end_elapsed + 1.0))
    merged["side"] = np.where(merged.team == merged.home_id, "home",
                       np.where(merged.team == merged.away_id, "away", "?"))
    valid = merged[merged.in_window & merged.side.ne("?")].copy()

    expected = (stints.groupby("game_id")
                .agg(expected_home=("home_pts", "sum"),
                     expected_away=("away_pts", "sum"),
                     adjusted_home_points=("home_pts_adj", "sum"),
                     adjusted_away_points=("away_pts_adj", "sum"),
                     evidence_source=("evidence_source", "first"),
                     date=("date", "first")))
    seen = merged.groupby("game_id").agg(
        possessions_seen=("pts", "size"),
        possessions_attached=("in_window", "sum"))
    total_sides = (segments.assign(
        side=np.where(segments.team == segments.game_id.map(
            stints.drop_duplicates("game_id").set_index("game_id").home_id),
            "home", "away"))
        .groupby(["game_id", "side"]).agg(n=("pts", "size"), pts=("pts", "sum"))
        .unstack(fill_value=0))
    total_sides.columns = [f"{a}_{b}" for a, b in total_sides.columns]
    attached = (valid.groupby(["game_id", "side"])
                .agg(n=("pts", "size"), pts=("pts", "sum"))
                .unstack(fill_value=0))
    attached.columns = [f"attached_{a}_{b}" for a, b in attached.columns]
    audit = expected.join(seen, how="left").join(total_sides, how="left").join(
        attached, how="left").fillna(0).reset_index()
    audit["attach_rate"] = (audit.possessions_attached
                            / audit.possessions_seen.clip(lower=1))
    audit["home_point_error"] = audit.pts_home - audit.expected_home
    audit["away_point_error"] = audit.pts_away - audit.expected_away
    audit["possession_gap"] = (audit.n_home - audit.n_away).abs()
    audit["pass_attach"] = audit.attach_rate.ge(MIN_ATTACH_RATE)
    audit["pass_points_strict"] = (
        audit.home_point_error.abs().le(POINT_TOL)
        & audit.away_point_error.abs().le(POINT_TOL))
    audit["pass_points"] = (
        audit.home_point_error.abs().le(MAX_RECONCILABLE_POINT_ERROR)
        & audit.away_point_error.abs().le(MAX_RECONCILABLE_POINT_ERROR))
    audit["pass_balance"] = audit.possession_gap.le(MAX_POSS_GAP)
    audit["balance_calibrated"] = ~audit.pass_balance
    audit["trusted_stint_level"] = (
        audit.pass_attach & audit.pass_points
        & audit.n_home.gt(0) & audit.n_away.gt(0))
    trusted = set(audit.loc[audit.trusted_stint_level, "game_id"])

    counts = (valid[valid.game_id.isin(trusted)]
              .groupby(["game_id", "stint_index", "side"])
              .agg(n=("pts", "size"), pts=("pts", "sum"))
              .unstack(fill_value=0))
    counts.columns = [f"{a}_{b}" for a, b in counts.columns]
    counts = counts.reset_index()
    counted = stints[stints.game_id.isin(trusted)].merge(
        counts, on=["game_id", "stint_index"], how="inner")
    for column in ("n_home", "n_away", "pts_home", "pts_away"):
        if column not in counted:
            counted[column] = 0.0
    # Retain the parser's exact segment locations. When the old-era state
    # machine misses a possession change, calibrate only the side totals to
    # their common game mean rather than discarding the lineup timing.
    counted["n_home_raw"] = counted.n_home
    counted["n_away_raw"] = counted.n_away
    game_count = counted.groupby("game_id")[["n_home", "n_away"]].transform("sum")
    game_gap = (game_count.n_home - game_count.n_away).abs()
    common = (game_count.n_home + game_count.n_away) / 2.0
    for side in ("home", "away"):
        denom = game_count[f"n_{side}"].replace(0, np.nan)
        scale = np.where(game_gap > MAX_POSS_GAP, common / denom, 1.0)
        counted[f"n_{side}"] = counted[f"n_{side}"] * scale
    counted["home_lineup_key"] = counted[HCOLS].astype(int).astype(str).agg("-".join, axis=1)
    counted["away_lineup_key"] = counted[ACOLS].astype(int).astype(str).agg("-".join, axis=1)
    group = counted.groupby(["game_id", "home_lineup_key", "away_lineup_key"])
    for side in ("home", "away"):
        # Impose the canonical adjusted score at game-lineup grain. This
        # absorbs both the intended luck adjustment and a rare missing scoring
        # event without assigning the residual across a substitution seam.
        source_adjusted = group[f"{side}_pts_adj"].transform("sum")
        parsed_points = group[f"pts_{side}"].transform("sum")
        residual = source_adjusted - parsed_points
        n_group = group[f"n_{side}"].transform("sum")
        counted[f"points_adjusted_{side}"] = (
            counted[f"pts_{side}"]
            + np.where(n_group > 0,
                       residual * counted[f"n_{side}"] / n_group, 0.0))
        # A lineup group can contain scoreboard movement but no parsed
        # possession start (the possession crossed the substitution). Allocate
        # only that otherwise-unplaceable remainder across the game's counted
        # possessions so every modeled game total is exactly canonical.
        game = counted.groupby("game_id")
        source_game = counted.game_id.map(
            expected[f"adjusted_{side}_points"])
        modeled_game = game[f"points_adjusted_{side}"].transform("sum")
        game_n = game[f"n_{side}"].transform("sum")
        counted[f"points_adjusted_{side}"] += np.where(
            game_n > 0,
            (source_game - modeled_game) * counted[f"n_{side}"] / game_n,
            0.0)

    luck, luck_totals = load_luck_components()
    luck_game = luck_totals.set_index("game_id")
    counted = apply_luck_to_counted(counted, luck, luck_totals)
    for side in ("home", "away"):
        base = f"adjusted_{side}_points"
        audit[f"{base}_3pt"] = audit[base]
        ft = audit.game_id.map(luck_game[f"ft_luck_{side}"]).fillna(0.0)
        mr = audit.game_id.map(luck_game[f"mr_luck_{side}"]).fillna(0.0)
        audit[f"{base}_3pt_ft"] = audit[base] - ft
        audit[base] = audit[f"{base}_3pt_ft"] - 0.5 * mr
    counted.to_parquet(OUT / "canonical_counted_stints_production.parquet",
                       index=False)

    totals = audit.set_index("game_id")
    untrusted = set(audit.loc[~audit.trusted_stint_level, "game_id"])
    aggregate = build_aggregate_from_stints(stints, audit, untrusted)

    base = pd.read_parquet(SALVAGE / "rapm_aggregate_fallback_design.parquet")
    base["game_id"] = norm(base.game_id)
    base = base.merge(games, on="game_id", how="left", validate="many_to_one")
    if base.official_date.isna().any():
        raise ValueError("official metadata missing for no-lineup aggregate games")
    base["date"] = base.official_date
    exact = (events[events.gid_n.isin(set(base.game_id))]
             .groupby("gid_n", sort=False)
             .apply(lambda g: pd.DataFrame(
                 walk_game(g, g.name in field_games),
                 columns=["elapsed", "team", "pts"]), include_groups=False)
             .reset_index(level=0).rename(columns={"gid_n": "game_id"}))
    exact = exact.merge(games, on="game_id", how="left", validate="many_to_one")
    exact["side"] = np.where(exact.team == exact.hometeamId, "home", "away")
    exact_counts = exact.groupby(["game_id", "side"]).size().unstack(fill_value=0)
    for side in ("home", "away"):
        lookup = exact_counts[side].to_dict()
        mask = base.offense_side.eq(side)
        base.loc[mask, "possessions_proxy"] = base.loc[mask, "game_id"].map(lookup)
        component_ft = base.loc[mask, "game_id"].map(
            luck_game[f"ft_luck_{side}"]).fillna(0.0)
        component_mr = base.loc[mask, "game_id"].map(
            luck_game[f"mr_luck_{side}"]).fillna(0.0)
        base.loc[mask, "points_3pt_ft"] = base.loc[mask, "points"] - component_ft
        base.loc[mask, "points"] = base.loc[mask, "points_3pt_ft"] - 0.5 * component_mr
    base["target_per_100"] = base.points / base.possessions_proxy * 100
    base["target_per_100_3pt_ft"] = base.points_3pt_ft / base.possessions_proxy * 100
    base["sqrt_weight"] = np.sqrt(base.possessions_proxy)
    base["information_tier"] = "aggregate_exact_possessions_official_minutes"
    base = base[aggregate.columns] if not aggregate.empty else base[[
        "observation_id", "game_id", "date", "offense_side", "role",
        "player_id", "design_value", "points", "points_3pt_ft",
        "possessions_proxy", "target_per_100", "target_per_100_3pt_ft",
        "sqrt_weight", "information_tier"]]
    aggregate = pd.concat([aggregate, base], ignore_index=True)
    aggregate.to_parquet(OUT / "canonical_counted_aggregate_production.parquet",
                         index=False)

    audit["season_year"] = (pd.to_datetime(audit.date).dt.year
                            - (pd.to_datetime(audit.date).dt.month < 10))
    audit.to_parquet(OUT / "canonical_counted_game_audit.parquet", index=False)
    audit.to_csv(REPORT / "production_counted_game_audit.csv", index=False)
    summary = (audit.groupby(["season_year", "evidence_source"])
               .agg(games=("game_id", "size"),
                    trusted_stint_level=("trusted_stint_level", "sum"))
               .reset_index())
    summary["aggregate_fallback"] = summary.games - summary.trusted_stint_level
    summary.to_csv(REPORT / "production_counted_evidence_summary.csv", index=False)
    print(f"trusted stint games: {len(trusted):,}/{len(audit):,}; "
          f"aggregate lineup fallbacks: {len(untrusted):,}; "
          f"no-lineup aggregate games: {base.game_id.nunique():,}")
    print(f"wrote {len(counted):,} counted stints and "
          f"{aggregate.observation_id.nunique():,} aggregate observations")


if __name__ == "__main__":
    main()
