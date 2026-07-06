"""Authoritative list of playoff games whose stint-based team scores disagree
with the Kaggle traditional box scores (same game_id namespace, direct join)."""
import duckdb
import pandas as pd

ROOT = r'C:\Users\Dave\Downloads\nba-onoff-publish'
KAG = r'C:\Users\Dave\Downloads\nba-boxscore-data\kaggle-traditional\traditional.csv'

con = duckdb.connect(ROOT + r'\data\nba_analytics_playoffs.duckdb', read_only=True)
con.execute(f"""
    CREATE TEMP TABLE kag AS
    SELECT CAST(gameid AS VARCHAR) game_id, team, SUM(PTS) pts
    FROM read_csv_auto('{KAG}', header=true, sample_size=-1)
    WHERE lower(type)='playoff' GROUP BY 1,2
""")
df = con.execute("""
    WITH ours AS (
        SELECT DISTINCT game_id, season, CAST(date AS VARCHAR) date, team_abbr,
               team_pts_actual, opp_pts_actual
        FROM player_game_facts WHERE team_pts_actual IS NOT NULL
    )
    SELECT o.game_id, o.season, o.date, o.team_abbr,
           o.team_pts_actual AS our_pts, k.pts AS official_pts,
           k.pts - o.team_pts_actual AS diff
    FROM ours o JOIN kag k ON o.game_id = k.game_id AND o.team_abbr = k.team
    WHERE abs(o.team_pts_actual - k.pts) > 0.5
    ORDER BY abs(k.pts - o.team_pts_actual) DESC, o.game_id
""").df()
print(len(df), 'team-game score mismatches vs kaggle official')
print(df.head(30).to_string())
out = ROOT + r'\data\audits\playoff_stint_score_mismatches.csv'
df.to_csv(out, index=False)
print('wrote', out)

# also: how many games affected
print('distinct games:', df['game_id'].nunique())

# characterize pm reconstruction noise: does it cancel within a team-game?
r = con.execute("""
    WITH tg AS (
        SELECT game_id, team_abbr,
               SUM(plus_minus_actual) pm_sum,
               MAX(team_pts_actual - opp_pts_actual) margin
        FROM player_game_facts
        WHERE plus_minus_actual IS NOT NULL AND team_pts_actual IS NOT NULL
        GROUP BY 1,2)
    SELECT
        count(*) n,
        avg(abs(pm_sum - 5*margin)) mae_team,
        quantile_cont(abs(pm_sum - 5*margin), 0.5) med,
        quantile_cont(abs(pm_sum - 5*margin), 0.95) p95
    FROM tg
""").fetchall()
print('team-level |pm_sum - 5*margin|: n/mae/med/p95 =', r)
