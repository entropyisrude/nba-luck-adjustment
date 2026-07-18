import csv, glob, collections

# Build game scores from PBP files (SCORE field = "visitor - home", last value per game = final)
game_scores = {}  # game_id -> (visitor_pts, home_pts)
for fpath in sorted(glob.glob(r"C:\Users\Dave\Downloads\nba-onoff-publish\data\historical_pbp\nbastats_po_*.csv")):
    with open(fpath, encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            score = row["SCORE"]
            if score:
                gid = row["GAME_ID"].lstrip("0")
                v, h = score.split(" - ")
                game_scores[gid] = (int(v), int(h))

# Build team info from stints file (home_id / away_id per game)
game_teams = {}
with open(r"C:\Users\Dave\Downloads\nba-onoff-publish\data\stints_playoffs_rebuilt.csv") as f:
    for row in csv.DictReader(f):
        gid = row["game_id"]
        if gid not in game_teams:
            game_teams[gid] = {"home_id": row["home_id"], "away_id": row["away_id"]}

def get_season(gid):
    s = gid[:3]
    if s[1] == "9": return "19" + s[1:]
    return "20" + f"{int(s[1:]):02d}"

# Group games into series by season + unordered team pair
series_games = collections.defaultdict(list)
for gid, t in game_teams.items():
    if gid not in game_scores:
        continue
    season = get_season(gid)
    pair = frozenset([t["home_id"], t["away_id"]])
    series_games[(season, pair)].append(int(gid))

# Analyze
total_g5 = 0
trailing_home_g5 = trailing_away_g5 = 0
trailing_home_wins = trailing_away_wins = 0
home_wins_g5 = 0
comeback_series = []

for (season, pair), game_ids in series_games.items():
    game_ids.sort()
    team_a = game_teams[str(game_ids[0])]["home_id"]  # home team of game 1
    team_b = game_teams[str(game_ids[0])]["away_id"]
    wins_a = wins_b = 0

    for gid_int in game_ids:
        gid = str(gid_int)
        t = game_teams[gid]
        visitor_pts, home_pts = game_scores[gid]
        home_won = home_pts > visitor_pts

        leading = max(wins_a, wins_b)
        trailing_w = min(wins_a, wins_b)

        if leading == 3 and trailing_w == 1:
            total_g5 += 1
            trailing_team = team_a if wins_a == 1 else team_b
            trailing_is_home = (t["home_id"] == trailing_team)
            trailing_won = (home_won and trailing_is_home) or (not home_won and not trailing_is_home)

            if trailing_is_home:
                trailing_home_g5 += 1
                if trailing_won:
                    trailing_home_wins += 1
            else:
                trailing_away_g5 += 1
                if trailing_won:
                    trailing_away_wins += 1

            if home_won:
                home_wins_g5 += 1

            if trailing_won:
                comeback_series.append({
                    "season": season,
                    "trailing": trailing_team,
                    "leading": team_b if trailing_team == team_a else team_a,
                    "trailing_home": trailing_is_home,
                    "score": f"{home_pts}-{visitor_pts}" if trailing_is_home else f"{visitor_pts}-{home_pts}",
                })

        # Update series score
        if home_won:
            if t["home_id"] == team_a: wins_a += 1
            else: wins_b += 1
        else:
            if t["away_id"] == team_a: wins_a += 1
            else: wins_b += 1

total_wins = trailing_home_wins + trailing_away_wins
print(f"=== Game 5 when a team is down 3-1 (1995-96 to present) ===")
print(f"Total game 5s: {total_g5}")
print()
print(f"Trailing team at HOME: {trailing_home_wins}/{trailing_home_g5} = {trailing_home_wins/trailing_home_g5*100:.1f}%")
print(f"Trailing team AWAY:    {trailing_away_wins}/{trailing_away_g5} = {trailing_away_wins/trailing_away_g5*100:.1f}%")
print(f"Trailing team overall: {total_wins}/{total_g5} = {total_wins/total_g5*100:.1f}%")
print(f"Home team wins g5:     {home_wins_g5}/{total_g5} = {home_wins_g5/total_g5*100:.1f}%")
print()
print("Down 3-1, won game 5:")
for c in comeback_series:
    loc = "HOME" if c["trailing_home"] else "away"
    print(f"  {c['season']}  trailing={c['trailing']}  leading={c['leading']}  loc={loc}  score={c['score']}")
