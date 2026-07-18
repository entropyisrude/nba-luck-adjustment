from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare candidate historical oracle builds.")
    parser.add_argument("--current-queue", required=True)
    parser.add_argument("--candidate-queue", action="append", default=[])
    parser.add_argument("--current-ref", required=True)
    parser.add_argument("--candidate-ref", action="append", default=[])
    parser.add_argument("--output", default=str(DATA_DIR / "oracle_candidate_comparison.txt"))
    return parser.parse_args()


def named_items(items: list[str]) -> list[tuple[str, Path]]:
    out: list[tuple[str, Path]] = []
    for item in items:
        if "=" not in item:
            raise ValueError(f"Expected name=path format, got: {item}")
        name, raw_path = item.split("=", 1)
        out.append((name.strip(), Path(raw_path.strip())))
    return out


def main() -> None:
    args = parse_args()
    queue_items = named_items(args.candidate_queue)
    ref_items = named_items(args.candidate_ref)
    con = duckdb.connect()

    lines: list[str] = []
    lines.append("Queue comparison against current live build")
    lines.append("")
    for name, path in queue_items:
        q = """
with oldq as (
  select season, team_id, team_abbr, avg_team_possessions as old_avg, pct_under_80 as old_pct80,
         severity_score as old_score, queue_rank as old_rank, repair_priority
  from read_csv_auto(?, header=true)
),
newq as (
  select season, team_id, team_abbr, avg_team_possessions as new_avg, pct_under_80 as new_pct80,
         severity_score as new_score, queue_rank as new_rank, repair_priority
  from read_csv_auto(?, header=true)
)
select
  count(*) as rows_compared,
  round(avg(new_avg - old_avg), 3) as avg_possessions_delta,
  round(avg(new_score - old_score), 3) as avg_severity_delta,
  sum(case when new_score < old_score then 1 else 0 end) as rows_improved,
  sum(case when new_score > old_score then 1 else 0 end) as rows_worsened
from oldq
join newq using(season, team_id, team_abbr)
where oldq.repair_priority='override_or_legacy_source'
"""
        summary = con.execute(q, [args.current_queue, str(path)]).fetchdf().iloc[0].to_dict()
        lines.append(f"[{name}]")
        for k, v in summary.items():
            lines.append(f"{k}: {v}")
        lines.append("top severity improvements:")
        q2 = """
with oldq as (
  select season, team_id, team_abbr, avg_team_possessions as old_avg, pct_under_80 as old_pct80,
         severity_score as old_score, queue_rank as old_rank, repair_priority
  from read_csv_auto(?, header=true)
),
newq as (
  select season, team_id, team_abbr, avg_team_possessions as new_avg, pct_under_80 as new_pct80,
         severity_score as new_score, queue_rank as new_rank
  from read_csv_auto(?, header=true)
)
select old_rank, new_rank, season, team_abbr,
       round(old_avg,3) as old_avg, round(new_avg,3) as new_avg,
       round(new_score-old_score,3) as score_delta
from oldq join newq using(season, team_id, team_abbr)
where repair_priority='override_or_legacy_source'
order by score_delta asc, new_avg-old_avg desc
limit 15
"""
        lines.append(con.execute(q2, [args.current_queue, str(path)]).fetchdf().to_string(index=False))
        lines.append("")

    lines.append("Key player season-reference comparison")
    lines.append("")
    player_checks = [
        ("Nikola Jokic", "2015-16", "2025-26"),
        ("Shai Gilgeous-Alexander", "2018-19", "2025-26"),
        ("Jalen Duren", "2022-23", "2025-26"),
        ("Ryan Kelly", "2013-14", "2016-17"),
    ]
    current_ref = Path(args.current_ref)
    for name, path in ref_items:
        lines.append(f"[{name}]")
        for player_name, season_start, season_end in player_checks:
            q = """
with src as (
  select *
  from read_csv_auto(?, header=true)
  where player_name = ?
    and season between ? and ?
),
agg as (
  select
    player_name,
    sum(games_played) as games_played,
    sum(team_games) as team_games,
    sum(on_diff_total) as on_diff_total,
    sum(on_possessions) as on_possessions,
    sum(off_diff_all_games) as off_diff_all_games,
    sum(off_possessions_all_games) as off_possessions_all_games
  from src
  group by 1
)
select
  player_name,
  games_played,
  team_games,
  round(
    100.0 * on_diff_total / nullif(on_possessions, 0)
    - 100.0 * off_diff_all_games / nullif(off_possessions_all_games, 0),
    3
  ) as onoff_full_season_per100
from agg
"""
            current_row = con.execute(q, [str(current_ref), player_name, season_start, season_end]).fetchdf()
            cand_row = con.execute(q, [str(path), player_name, season_start, season_end]).fetchdf()
            lines.append(f"{player_name} {season_start}-{season_end}")
            lines.append("current:")
            lines.append(current_row.to_string(index=False) if not current_row.empty else "no rows")
            lines.append("candidate:")
            lines.append(cand_row.to_string(index=False) if not cand_row.empty else "no rows")
        lines.append("")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
