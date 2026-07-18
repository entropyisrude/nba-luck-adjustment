"""
Tag every stint (regular season + playoffs) as 'playoff' or 'regular'.

No clutch-time carve-out anymore -- regular-season clutch stints are folded
back into 'regular' along with everything else. This is the simplified
playoffs-vs-regular-season design (clutch sub-splitting turned out to not
behave consistently with the playoff split and is dropped here).

Output: data/leverage_rapm/stints_bucketed.parquet
Columns match what run_rapm.py's build_design_matrix_stint_od expects, plus
`bucket` and `source` (rs/po, same as bucket now, kept for continuity).
"""

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "leverage_rapm" / "stints_bucketed.parquet"

COLS = """
    game_id, stint_index, home_id, away_id,
    home_p1, home_p2, home_p3, home_p4, home_p5,
    away_p1, away_p2, away_p3, away_p4, away_p5,
    seconds, home_pts, away_pts, home_pts_adj, away_pts_adj,
    start_period, start_clock, start_home_score, start_away_score, date
"""


def main():
    con = duckdb.connect()

    con.execute(f"attach '{ROOT / 'data' / 'nba_analytics.duckdb'}' as rs (read_only)")
    con.execute(f"attach '{ROOT / 'data' / 'nba_analytics_playoffs.duckdb'}' as po (read_only)")

    rs_query = f"""
        select {COLS}, 'rs' as source, 'regular' as bucket
        from rs.lineup_stint_facts
        where seconds >= 10
    """

    po_query = f"""
        select {COLS}, 'po' as source, 'playoff' as bucket
        from po.raw_playoff_stints
        where seconds >= 10
    """

    con.execute(f"""
        copy (
            select * from ({rs_query})
            union all
            select * from ({po_query})
        ) to '{OUT.as_posix()}' (format parquet)
    """)

    summary = con.execute(f"""
        select bucket, count(*) as stints, round(sum(seconds)/3600.0, 1) as hours
        from read_parquet('{OUT.as_posix()}')
        group by 1 order by 1
    """).fetchall()

    print(f"Wrote {OUT}")
    for row in summary:
        print(" ", row)


if __name__ == "__main__":
    main()
