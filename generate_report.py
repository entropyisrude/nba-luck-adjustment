"""Generate HTML summary report from adjusted_games.csv

This script reads the template.html file and injects dynamic data.
To change the UI/styling, edit template.html directly.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

import re
import unicodedata

import numpy as np

from src.season import SEASON, SEASON_START_DATE, START_YEAR, TAG

TEMPLATE_PATH = Path("template.html")
DATA_DIR = Path("data")
LUCK_COMPONENTS = DATA_DIR / "rapm_luck_adjust.parquet"


def _apply_shooting_luck(df: pd.DataFrame) -> pd.DataFrame:
    """Layer FT and mid-range components onto the stored 3PT-adjusted games."""
    if not LUCK_COMPONENTS.exists():
        raise FileNotFoundError(f"Missing shooting-luck components: {LUCK_COMPONENTS}")
    luck = pd.read_parquet(LUCK_COMPONENTS)
    required = {"game_id", "ft_luck_home", "ft_luck_away",
                "mr_luck_home", "mr_luck_away"}
    missing = required - set(luck.columns)
    if missing:
        raise ValueError(f"Luck payload missing component columns: {sorted(missing)}")
    totals = luck.groupby(luck.game_id.astype(str).str.lstrip("0"))[[
        "ft_luck_home", "ft_luck_away", "mr_luck_home", "mr_luck_away"]].sum()
    out = df.copy()
    gid = out.game_id.astype(str).str.lstrip("0")
    for side in ("home", "away"):
        stored_base = pd.to_numeric(
            out.get(f"{side}_pts_adj_3pt", pd.Series(index=out.index, dtype=float)),
            errors="coerce")
        base = stored_base.fillna(
            pd.to_numeric(out[f"{side}_pts_adj"], errors="coerce"))
        ft_fallback = gid.map(totals[f"ft_luck_{side}"]).fillna(0.0)
        mr_fallback = gid.map(totals[f"mr_luck_{side}"]).fillna(0.0)
        ft = pd.to_numeric(
            out.get(f"{side}_ft_luck", pd.Series(index=out.index, dtype=float)),
            errors="coerce").fillna(ft_fallback)
        mr = pd.to_numeric(
            out.get(f"{side}_mr_luck", pd.Series(index=out.index, dtype=float)),
            errors="coerce").fillna(mr_fallback)
        out[f"{side}_pts_adj_3pt"] = base
        out[f"{side}_pts_adj_3pt_ft"] = base - ft
        out[f"{side}_pts_adj"] = base - ft - 0.5 * mr
    out["margin_actual"] = out.home_pts_actual - out.away_pts_actual
    out["margin_adj"] = out.home_pts_adj - out.away_pts_adj
    out["margin_delta"] = out.margin_adj - out.margin_actual
    out["margin_adj_3pt_ft"] = out.home_pts_adj_3pt_ft - out.away_pts_adj_3pt_ft
    out["margin_delta_3pt_ft"] = out.margin_adj_3pt_ft - out.margin_actual
    return out


def _load_game_possessions() -> dict[str, float]:
    """Return {game_id: avg_possessions}, preferring PBP counts over four-factor formula."""
    result: dict[str, float] = {}

    box_csv = DATA_DIR / "player_boxscore_stats.csv"
    if box_csv.exists():
        box = pd.read_csv(box_csv, usecols=["game_id", "team_abbr", "fga", "fta", "oreb", "tov"])
        box["game_id"] = box["game_id"].astype(str).str.lstrip("0")
        tg = box.groupby(["game_id", "team_abbr"], as_index=False).sum(numeric_only=True)
        tg["poss"] = tg["fga"] - tg["oreb"] + tg["tov"] + 0.44 * tg["fta"]
        for gid, poss in tg.groupby("game_id")["poss"].mean().items():
            result[str(gid)] = poss

    poss_csv = DATA_DIR / "possessions_playoffs.csv"
    if poss_csv.exists():
        poss_df = pd.read_csv(poss_csv, usecols=["game_id", "offense_team"])
        poss_df["game_id"] = poss_df["game_id"].astype(str).str.lstrip("0")
        pbp = poss_df.groupby(["game_id", "offense_team"]).size().reset_index(name="poss")
        for gid, grp in pbp.groupby("game_id"):
            result[str(gid)] = grp["poss"].mean()

    return result


# Team-rating adjustments (team rankings "Ratings adjustment" dropdown).
# Model per team-game offense, with each side's lineup strength that night:
#   pts/100 = league avg + (O[off] - miss_o[off]) - (D[def] - miss_d[def]) + venue
# O and D are full-strength team qualities, shrunk toward the win-projections
# prior with the weight of SOS_PRIOR_GAMES games. miss_o/miss_d are the known
# cost of projected-rotation players who did not play that game: their
# depth-chart minutes go first to the teammates who would absorb them (bench
# players more than starters) and only the overflow to replacement level.
SOS_PRIOR_GAMES = 10.0
SOS_HOME_EDGE = 1.25  # per side, pts/100: a 2.5-point home margin
TEAM_PRIORS_PATH = DATA_DIR / f"team_priors_{TAG}.json"
NAME_ALIASES = {"nahshon hyland": "bones hyland", "yang hansen": "hansen yang"}  # depth-chart -> box-score name


def _load_team_priors() -> dict:
    """scripts/export_team_priors.py payload ({} if missing: zero priors, no injuries)."""
    if TEAM_PRIORS_PATH.exists():
        return json.loads(TEAM_PRIORS_PATH.read_text(encoding="utf-8"))
    return {}


def _name_key(name: str) -> tuple[str, str]:
    """(first, last) normalized; drops periods, accents and suffixes."""
    text = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode().lower()
    text = text.replace(".", "").replace(",", " ")
    plain = re.sub(r"[^a-z ]", "", text).strip()
    text = NAME_ALIASES.get(plain, text)
    parts = [re.sub(r"[^a-z]", "", w) for w in text.split()]
    parts = [w for w in parts if w and w not in {"jr", "sr", "ii", "iii", "iv", "v"}]
    return (parts[0] if parts else "", "".join(parts[1:]))


def _same_player(a: tuple[str, str], b: tuple[str, str]) -> bool:
    """Same last name and one first name a prefix of the other (Nic/Nicolas)."""
    return (a[1] == b[1] and bool(a[0]) and bool(b[0])
            and (a[0].startswith(b[0]) or b[0].startswith(a[0])))


def _headroom(minutes: float) -> float:
    """Extra minutes a healthy rotation player picks up when teammates sit:
    starters a couple, bench players up to 8 (never past 34)."""
    if minutes >= 30:
        return 2.0
    if minutes >= 24:
        return 4.0
    return max(0.0, min(8.0, 34.0 - minutes))


def _lineup_cost(rotation: list[dict], avail: dict[str, float], rep_o: float, rep_d: float) -> tuple[float, float]:
    """(offense, defense) cost in pts/100 versus full strength when each rotation
    player plays the fraction avail[name] (default 1) of his projected minutes.
    Lost minutes go to fully available teammates up to their headroom, in
    proportion to it; the rest go to replacement level. Never below zero: the
    depth chart is taken as the coach's best use of a healthy roster."""
    lost = sum((1 - avail.get(p["name"], 1.0)) * p["minutes"] for p in rotation)
    if lost <= 0:
        return 0.0, 0.0
    heads = {p["name"]: _headroom(p["minutes"]) if avail.get(p["name"], 1.0) >= 1 else 0.0 for p in rotation}
    room = sum(heads.values())
    given = min(lost, room)
    co = cd = 0.0
    for p in rotation:
        extra = given * heads[p["name"]] / room if room else 0.0
        missing = (1 - avail.get(p["name"], 1.0)) * p["minutes"]
        co += (missing - extra) / 48 * p["o"]
        cd += (missing - extra) / 48 * p["d"]
    co -= (lost - given) / 48 * rep_o
    cd -= (lost - given) / 48 * rep_d
    if co + cd < 0:
        return 0.0, 0.0
    return co, cd


def _replacement(priors: dict) -> tuple[float, float]:
    r = priors.get("replacement", {})
    return float(r.get("o", 0.0)), float(r.get("d", 0.0))


def _missing_costs(source_df: pd.DataFrame, priors: dict) -> dict[tuple[str, str], tuple[float, float]]:
    """{(game_id, team): (offense cost, defense cost)} in pts/100 from projected-
    rotation players who did not play that game (injured, rested or benched)."""
    box_csv = DATA_DIR / "player_boxscore_stats.csv"
    teams = priors.get("teams", {})
    if not teams or not box_csv.exists():
        return {}
    rep_o, rep_d = _replacement(priors)
    box = pd.read_csv(box_csv, dtype={"game_id": str}, usecols=["game_id", "team_abbr", "player_name"])
    box["game_id"] = box["game_id"].str.lstrip("0")
    played = {k: [_name_key(n) for n in g]
              for k, g in box.groupby(["game_id", "team_abbr"])["player_name"]}
    costs = {}
    for r in source_df.itertuples(index=False):
        gid = str(r.game_id).lstrip("0")
        for team in (r.home_team, r.away_team):
            names = played.get((gid, team))
            if names is None or team not in teams:
                continue
            rotation = teams[team].get("rotation", [])
            missing = {p["name"]: 0.0 for p in rotation
                       if not any(_same_player(_name_key(p["name"]), n) for n in names)}
            costs[(gid, team)] = _lineup_cost(rotation, missing, rep_o, rep_d)
    return costs


OUT_RECENT_GAMES = 3  # a rotation player missing this many straight team games counts as out
AVAILABILITY_PATH = DATA_DIR / f"availability_{START_YEAR}_{(START_YEAR + 1) % 100:02d}.js"
POWER_JS_PATH = DATA_DIR / "team_power_ratings.js"
SEASON_GAMES = 82
# Expected games missed, from the injury report's wording (first match wins).
# Rough tiers; GAMES_OUT in the availability file overrides any player.
DURATION_RULES = [
    (("acl", "achilles", "season"), None),            # rest of season
    (("surgery", "repair", "fracture", "rupture", "torn", "tear", "dislocat"), 30),
    (("high ankle", "high grade", "grade 2", "grade 3"), 12),
    (("sprain", "strain", "tendinitis", "tendinopathy", "effusion", "bone bruise"), 6),
    (("soreness", "sore", "tightness", "illness", "contusion", "management", "rest",
      "spasm", "inflammation", "swelling", "stiffness", "personal", "not with team"), 2),
]
INJURY_DEFAULT_GAMES = 6   # injury wording that matches no rule
UNLISTED_OUT_GAMES = 10    # out by the missed-games rule or by hand, no injury listed
INJURY_REPORT_PATH = DATA_DIR / f"injury_report_{TAG}.json"  # scripts/fetch_injury_report.py


def _load_out_overrides() -> dict[tuple[str, str], bool]:
    """OUT_NOW map from the availability file: {"Player": true|false} -> {name key: out?}."""
    if not AVAILABILITY_PATH.exists():
        return {}
    # Drop // comments so commented-out examples are ignored.
    text = re.sub(r"//.*", "", AVAILABILITY_PATH.read_text(encoding="utf-8"))
    m = re.search(r"window\.OUT_NOW\s*=\s*\{(.*?)\};", text, re.S)
    if not m:
        return {}
    return {_name_key(name): flag == "true"
            for name, flag in re.findall(r'"([^"]+)"\s*:\s*(true|false)', m.group(1))}


def _load_games_out() -> dict[tuple[str, str], float | None]:
    """GAMES_OUT map: {"Player": games still to miss, or "season"} -> {name key: games}."""
    if not AVAILABILITY_PATH.exists():
        return {}
    # Drop // comments so commented-out examples are ignored.
    text = re.sub(r"//.*", "", AVAILABILITY_PATH.read_text(encoding="utf-8"))
    m = re.search(r"window\.GAMES_OUT\s*=\s*\{(.*?)\};", text, re.S)
    if not m:
        return {}
    return {_name_key(name): (None if val.strip('"') == "season" else float(val))
            for name, val in re.findall(r'"([^"]+)"\s*:\s*("season"|\d+(?:\.\d+)?)', m.group(1))}


def _expected_games_out(reason: str | None) -> float | None:
    """Games a listed-out player is expected to miss from here (None = rest of season)."""
    text = (reason or "").lower()
    for needles, games in DURATION_RULES:
        if any(n in text for n in needles):
            return games
    return INJURY_DEFAULT_GAMES if "injury" in text or "illness" in text else UNLISTED_OUT_GAMES


def _team_quality_fit(source_df: pd.DataFrame, game_poss: dict | None, priors: dict) -> dict:
    """Fit full-strength team offense/defense: the preseason prior (worth
    SOS_PRIOR_GAMES games) updated by every game, with opponent, venue and
    missing players accounted for. Returns the fit plus per-team summaries."""
    df = source_df.copy()
    gid = df["game_id"].astype(str).str.lstrip("0")
    fallback = (df["home_pts_actual"] + df["away_pts_actual"]) / 2
    df["poss"] = gid.map(game_poss or {}).fillna(fallback)
    df["gid"] = gid
    costs = _missing_costs(df, priors)
    team_priors = priors.get("teams", {})
    obs = []  # (offense, defense, pts/100, venue sign, possessions, offense miss_o, defense miss_d)
    for r in df.itertuples(index=False):
        for off, dfn, pts, venue in ((r.home_team, r.away_team, r.home_pts_adj, 1.0),
                                     (r.away_team, r.home_team, r.away_pts_adj, -1.0)):
            mo = costs.get((r.gid, off), (0.0, 0.0))[0]
            md = costs.get((r.gid, dfn), (0.0, 0.0))[1]
            obs.append((off, dfn, 100 * pts / r.poss, venue, r.poss, mo, md))
    teams = sorted({o[0] for o in obs})
    idx = {t: i for i, t in enumerate(teams)}
    n = len(teams)
    # Unknowns: [mu, O_0..O_n-1, D_0..D_n-1]. Each observation weighs one game.
    rows, ys = [], []
    for off, dfn, y, venue, _, mo, md in obs:
        x = np.zeros(1 + 2 * n)
        x[0], x[1 + idx[off]], x[1 + n + idx[dfn]] = 1.0, 1.0, -1.0
        rows.append(x)
        ys.append(y - SOS_HOME_EDGE * venue + mo - md)
    lam = np.sqrt(SOS_PRIOR_GAMES)
    for t in teams:
        prior = team_priors.get(t, {})
        for offset, key in ((1, "off"), (1 + n, "def")):
            x = np.zeros(1 + 2 * n)
            x[offset + idx[t]] = lam
            rows.append(x)
            ys.append(lam * float(prior.get(key, 0.0)))
    coef = np.linalg.lstsq(np.array(rows), np.array(ys), rcond=None)[0]
    mu = float(coef[0])
    off_q = {t: float(coef[1 + idx[t]]) for t in teams}
    def_q = {t: float(coef[1 + n + idx[t]]) for t in teams}
    # Season-to-date ratings (possession-weighted), raw and opponent-adjusted.
    acc = {t: {"po": 0.0, "pd": 0.0, "so": 0.0, "sd": 0.0, "poss": 0.0} for t in teams}
    for off, dfn, y, venue, poss, mo, md in obs:
        acc[off]["po"] += poss * y
        acc[dfn]["pd"] += poss * y
        # The opponent as it actually lined up that night.
        acc[off]["so"] += poss * (y + (def_q[dfn] - md) - SOS_HOME_EDGE * venue)
        acc[dfn]["sd"] += poss * (y - (off_q[off] - mo) - SOS_HOME_EDGE * venue)
        acc[off]["poss"] += poss
    return {"mu": mu, "off": off_q, "def": def_q, "costs": costs, "acc": acc, "teams": teams}


def _load_injury_report(source_df: pd.DataFrame) -> dict[str, list[dict]]:
    """{team: [{key, name, status, reason}]} from scripts/fetch_injury_report.py,
    for teams whose latest report is for a game after their last game played."""
    if not INJURY_REPORT_PATH.exists():
        return {}
    report = json.loads(INJURY_REPORT_PATH.read_text(encoding="utf-8")).get("teams", {})
    last_game = {}
    for side in ("home_team", "away_team"):
        for team, d in source_df.groupby(side)["date"].max().items():
            last_game[team] = max(last_game.get(team, ""), str(d))
    return {team: [dict(p, key=_name_key(p["name"])) for p in info.get("players", [])]
            for team, info in report.items()
            if (info.get("game_date") or "") > last_game.get(team, "")}


def _currently_out(source_df: pd.DataFrame, priors: dict) -> dict[str, list[dict]]:
    """{team: [out player]}. A rotation player is out tonight when the NBA's
    latest injury report lists him Out or Doubtful; players it does not list are
    out after missing the team's last OUT_RECENT_GAMES games (or all of them,
    with 2+ played). Each out player also gets the games he is expected to miss
    (None = rest of season), from the report's wording. The availability file's
    GAMES_OUT (games still to miss, 0 = available) and OUT_NOW override."""
    box_csv = DATA_DIR / "player_boxscore_stats.csv"
    teams = priors.get("teams", {})
    if not teams or not box_csv.exists():
        return {}
    rep_o, rep_d = _replacement(priors)
    overrides = _load_out_overrides()
    games_out = _load_games_out()
    report = _load_injury_report(source_df)
    box = pd.read_csv(box_csv, dtype={"game_id": str}, usecols=["game_id", "team_abbr", "player_name"])
    box["game_id"] = box["game_id"].str.lstrip("0")
    played = {k: [_name_key(n) for n in g]
              for k, g in box.groupby(["game_id", "team_abbr"])["player_name"]}
    games = source_df.assign(gid=source_df["game_id"].astype(str).str.lstrip("0")).sort_values("date")
    out: dict[str, list[dict]] = {}
    for team, info in teams.items():
        rotation = info.get("rotation", [])
        team_games = [g for g in games[(games.home_team == team) | (games.away_team == team)].gid
                      if (g, team) in played]
        for p in rotation:
            key = _name_key(p["name"])
            streak = 0
            for g in reversed(team_games):
                if any(_same_player(key, n) for n in played[(g, team)]):
                    break
                streak += 1
            override = next((flag for k, flag in overrides.items() if _same_player(key, k)), None)
            manual = next((g for k, g in games_out.items() if _same_player(key, k)), "unset")
            if manual != "unset":
                override = manual is None or manual > 0
            need = min(OUT_RECENT_GAMES, len(team_games))
            listed = next((r for r in report.get(team, []) if _same_player(key, r["key"])), None)
            if listed is not None:
                auto = listed["status"] in ("Out", "Doubtful")
            else:
                auto = len(team_games) >= 2 and streak >= need
            if not (override is True or (override is None and auto)):
                continue
            if manual != "unset":
                reason, expected = "games out set by hand", manual
            elif override is True and listed is None:
                reason, expected = "marked out", UNLISTED_OUT_GAMES
            elif listed is not None:
                reason, expected = f"injury report: {listed['status'].lower()}", _expected_games_out(listed["reason"])
            else:
                reason = f"missed last {streak} game{'s' if streak != 1 else ''}"
                expected = UNLISTED_OUT_GAMES
            solo = _lineup_cost(rotation, {p["name"]: 0.0}, rep_o, rep_d)
            out.setdefault(team, []).append({
                "name": p["name"], "minutes": p["minutes"], "reason": reason,
                "status": listed["reason"] if listed is not None else None,
                "games_out": expected, "o": solo[0], "d": solo[1]})
    return out


def _power_payload(luck_dfs: dict[str, pd.DataFrame], game_poss: dict | None, priors: dict) -> dict:
    """Data for team-power-ratings.html, one entry per shooting-luck setting.
    tonight: full strength minus everyone out now. outlook: full strength over
    the rest of the season, each out player missing his expected share of it."""
    team_priors = priors.get("teams", {})
    rep_o, rep_d = _replacement(priors)
    modes = {}
    for luck_key, luck_df in luck_dfs.items():
        if luck_df.empty:
            modes[luck_key] = []
            continue
        fit = _team_quality_fit(luck_df, game_poss, priors)
        mu = fit["mu"]
        out = _currently_out(luck_df, priors)
        rows = []
        for t in fit["teams"]:
            a = fit["acc"][t]
            w = int(((luck_df.home_team == t) & (luck_df.margin_actual > 0)).sum()
                    + ((luck_df.away_team == t) & (luck_df.margin_actual < 0)).sum())
            gp = int(((luck_df.home_team == t) | (luck_df.away_team == t)).sum())
            left = max(SEASON_GAMES - gp, 1)
            rotation = team_priors.get(t, {}).get("rotation", [])
            outs = out.get(t, [])
            share = {p["name"]: 1.0 if p["games_out"] is None else min(p["games_out"], left) / left
                     for p in outs}
            tonight_o, tonight_d = _lineup_cost(rotation, {p["name"]: 0.0 for p in outs}, rep_o, rep_d)
            look_o, look_d = _lineup_cost(rotation, {n: 1 - sh for n, sh in share.items()}, rep_o, rep_d)
            full = (mu + fit["off"][t], mu - fit["def"][t])
            pre = team_priors.get(t, {})
            team_costs = [c for (g, team), c in fit["costs"].items() if team == t]
            rows.append({
                "team": t, "w": w, "l": gp - w, "games_left": left,
                "outlook": [round(full[0] - look_o, 2), round(full[1] + look_d, 2)],
                "tonight": [round(full[0] - tonight_o, 2), round(full[1] + tonight_d, 2)],
                "full": [round(full[0], 2), round(full[1], 2)],
                "season": [round(a["po"] / a["poss"], 2), round(a["pd"] / a["poss"], 2)],
                "sched": [round(a["so"] / a["poss"], 2), round(a["sd"] / a["poss"], 2)],
                "pre": [round(mu + pre.get("off", 0.0), 2), round(mu - pre.get("def", 0.0), 2)],
                "missing_avg": round(sum(o + d for o, d in team_costs) / max(len(team_costs), 1), 2),
                "out": [{"name": p["name"], "minutes": p["minutes"], "reason": p["reason"],
                         "status": p.get("status"), "games_out": p["games_out"],
                         "share": round(share[p["name"]], 3), "net": round(p["o"] + p["d"], 2)}
                        for p in outs],
            })
        modes[luck_key] = rows
    return {"season": SEASON, "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "prior_games": SOS_PRIOR_GAMES, "out_recent_games": OUT_RECENT_GAMES, "modes": modes}


# Injury tracker (team-injuries.html). Same absence cost as the power ratings,
# split by nba.com's reason for each projected-rotation player who sat.
STATUS_CSV = DATA_DIR / f"season_{TAG}" / f"player_game_status_{TAG}.csv"
INJURY_JS_PATH = DATA_DIR / "team_injuries.js"
WINS_PER_POINT = 0.033  # win probability per point of expected margin
ABSENCE_CATEGORIES = [
    ("injury", "Injury / illness"), ("inactive", "Inactive, reason not listed"), ("rest", "Rest"), ("personal", "Personal / not with team"),
    ("suspension", "Suspension"), ("coach", "Coach's decision"), ("gleague", "G League"),
    ("unlisted", "Not on game roster"), ("other", "Other / unspecified"),
]


def _absence_category(status: str | None) -> str:
    s = (status or "").lower()
    if not s:
        return "unlisted"
    if s == "inactive":
        return "inactive"
    for needle, cat in (("injury", "injury"), ("illness", "injury"), ("rest", "rest"),
                        ("personal", "personal"), ("not with team", "personal"),
                        ("suspen", "suspension"), ("coach", "coach"), ("g league", "gleague")):
        if needle in s:
            return cat
    return "other"


def _injury_payload(source_df: pd.DataFrame, game_poss: dict | None, priors: dict) -> dict | None:
    """Data for team-injuries.html: projected-rotation games missed, by reason."""
    teams = priors.get("teams", {})
    if source_df.empty or not teams or not STATUS_CSV.exists():
        return None
    rep_o, rep_d = _replacement(priors)
    status = pd.read_csv(STATUS_CSV, dtype={"game_id": str})
    status["game_id"] = status["game_id"].str.lstrip("0")
    listed = {k: [(_name_key(n), s) for n, s in zip(g.player_name, g.status)]
              for k, g in status.groupby(["game_id", "team_abbr"])}
    games = source_df.assign(gid=source_df["game_id"].astype(str).str.lstrip("0")).sort_values("date")
    cats = [c for c, _ in ABSENCE_CATEGORIES]
    team_out = {}
    weekly: dict[str, dict] = {}
    for team, info in teams.items():
        base = info.get("rotation", [])
        rotation = [dict(p, key=_name_key(p["name"]),
                         # Share of a game's cost when several players sit together.
                         weight=max(0.0, p["minutes"] / 48 * ((p["o"] - rep_o) + (p["d"] - rep_d))),
                         value=sum(_lineup_cost(base, {p["name"]: 0.0}, rep_o, rep_d)))
                    for p in base]
        tot = {c: {"games": 0, "minutes": 0.0, "cost": 0.0, "wins": 0.0} for c in cats}
        players = {p["name"]: {c: 0 for c in cats} for p in rotation}
        gp = healthy = healthy_strict = 0
        last_status = {}
        for r in games[(games.home_team == team) | (games.away_team == team)].itertuples(index=False):
            roster = listed.get((r.gid, team))
            if roster is None:
                continue
            gp += 1
            poss = (game_poss or {}).get(r.gid) or (r.home_pts_actual + r.away_pts_actual) / 2
            week = (pd.Timestamp(r.date) - pd.Timedelta(days=pd.Timestamp(r.date).weekday())).strftime("%Y-%m-%d")
            wk = weekly.setdefault(week, {"team_games": 0, **{c: {"minutes": 0.0, "cost": 0.0} for c in cats}})
            wk["team_games"] += 1
            injured = inactive = False
            statuses = {p["name"]: next((s for k, s in roster if _same_player(p["key"], k)), None)
                        for p in rotation}
            sat = [p for p in rotation if statuses[p["name"]] != "played"]
            game_cost = sum(_lineup_cost(base, {p["name"]: 0.0 for p in sat}, rep_o, rep_d))
            weights = sum(p["weight"] for p in sat)
            for p in rotation:
                st = statuses[p["name"]]
                last_status[p["name"]] = st
                if st == "played":
                    continue
                cost = game_cost * p["weight"] / weights if weights else 0.0
                cat = _absence_category(st)
                injured |= cat == "injury"
                inactive |= cat == "inactive"
                players[p["name"]][cat] += 1
                t = tot[cat]
                t["games"] += 1
                t["minutes"] += p["minutes"]
                t["cost"] += cost
                t["wins"] += cost * poss / 100 * WINS_PER_POINT
                wk[cat]["minutes"] += p["minutes"]
                wk[cat]["cost"] += cost
            healthy += not injured
            healthy_strict += not (injured or inactive)
        team_out[team] = {
            "team": team, "gp": gp, "healthy": healthy, "healthy_with_inactive": healthy_strict,
            "totals": {c: {k: round(v, 3) for k, v in t.items()} for c, t in tot.items()},
            "players": [{"name": p["name"], "minutes": p["minutes"], "value": round(p["value"], 2),
                         "missed": {c: n for c, n in players[p["name"]].items() if n},
                         "last": last_status.get(p["name"])}
                        for p in rotation if any(players[p["name"]].values())],
            "out": [],
        }
    for team, outs in _currently_out(source_df, priors).items():
        if team in team_out:
            team_out[team]["out"] = [
                {"name": p["name"], "minutes": p["minutes"], "reason": p["reason"],
                 "status": p.get("status") or next((pl["last"] for pl in team_out[team]["players"]
                                                    if pl["name"] == p["name"]), None),
                 "games_out": p["games_out"], "value": round(p["o"] + p["d"], 2)} for p in outs]
    return {"season": SEASON, "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "wins_per_point": WINS_PER_POINT, "out_recent_games": OUT_RECENT_GAMES,
            "categories": [{"key": c, "label": label} for c, label in ABSENCE_CATEGORIES],
            "teams": [team_out[t] for t in sorted(team_out)],
            "weekly": [{"week": w, **v} for w, v in sorted(weekly.items())]}


def _build_team_rows(source_df: pd.DataFrame, game_poss: dict | None = None) -> str:
    """Build HTML rows for the team rankings table from a given slice of games."""
    if source_df.empty:
        return '<tr><td colspan="12" style="text-align:center;color:#888;">No games yet</td></tr>'

    df = source_df.copy()
    if game_poss:
        df['game_poss_est'] = df['game_id'].astype(str).str.lstrip("0").map(game_poss)
        fallback = (df['home_pts_actual'] + df['away_pts_actual']) / 2
        df['game_poss_est'] = df['game_poss_est'].fillna(fallback)
    else:
        df['game_poss_est'] = (df['home_pts_actual'] + df['away_pts_actual']) / 2

    home_luck = df.groupby('home_team').agg({
        'margin_delta': 'sum',
        'home_pts_adj': 'sum',
        'away_pts_adj': 'sum',
        'home_pts_actual': 'sum',
        'game_poss_est': 'sum',
        'game_id': 'count'
    }).rename(columns={
        'game_id': 'home_games', 'margin_delta': 'home_luck',
        'home_pts_adj': 'home_pts_adj_for', 'away_pts_adj': 'home_pts_adj_against',
        'game_poss_est': 'home_poss_est',
    })

    away_luck = df.groupby('away_team').agg({
        'margin_delta': lambda x: -x.sum(),
        'away_pts_adj': 'sum',
        'home_pts_adj': 'sum',
        'away_pts_actual': 'sum',
        'game_poss_est': 'sum',
        'game_id': 'count'
    }).rename(columns={
        'game_id': 'away_games', 'margin_delta': 'away_luck',
        'away_pts_adj': 'away_pts_adj_for', 'home_pts_adj': 'away_pts_adj_against',
        'game_poss_est': 'away_poss_est',
    })

    teams = home_luck.join(away_luck, how='outer').fillna(0)
    teams['total_luck'] = teams['home_luck'] + teams['away_luck']
    teams['total_games'] = teams['home_games'] + teams['away_games']
    teams['luck_per_game'] = teams['total_luck'] / teams['total_games']

    team_records = {}
    for team in set(source_df['home_team'].unique()) | set(source_df['away_team'].unique()):
        team_records[team] = {
            'wins': 0, 'losses': 0, 'adj_wins': 0, 'adj_losses': 0,
            'opp_3pa': 0, 'opp_3pm': 0, 'opp_3pm_exp': 0,
            'team_3pa': 0, 'team_3pm': 0, 'team_3pm_exp': 0,
        }

    for _, row in source_df.iterrows():
        home = row['home_team']
        away = row['away_team']
        if row['margin_actual'] > 0:
            team_records[home]['wins'] += 1
            team_records[away]['losses'] += 1
        else:
            team_records[away]['wins'] += 1
            team_records[home]['losses'] += 1
        if row['margin_adj'] > 0:
            team_records[home]['adj_wins'] += 1
            team_records[away]['adj_losses'] += 1
        else:
            team_records[away]['adj_wins'] += 1
            team_records[home]['adj_losses'] += 1
        team_records[home]['opp_3pa'] += row['away_3pa']
        team_records[home]['opp_3pm'] += row['away_3pm_actual']
        team_records[home]['opp_3pm_exp'] += row['away_3pm_exp']
        team_records[away]['opp_3pa'] += row['home_3pa']
        team_records[away]['opp_3pm'] += row['home_3pm_actual']
        team_records[away]['opp_3pm_exp'] += row['home_3pm_exp']
        team_records[home]['team_3pa'] += row['home_3pa']
        team_records[home]['team_3pm'] += row['home_3pm_actual']
        team_records[home]['team_3pm_exp'] += row['home_3pm_exp']
        team_records[away]['team_3pa'] += row['away_3pa']
        team_records[away]['team_3pm'] += row['away_3pm_actual']
        team_records[away]['team_3pm_exp'] += row['away_3pm_exp']

    teams = teams.reset_index()
    teams.columns = ['team'] + list(teams.columns[1:])
    for col in ('wins', 'losses', 'adj_wins', 'adj_losses',
                'opp_3pa', 'opp_3pm', 'opp_3pm_exp',
                'team_3pa', 'team_3pm', 'team_3pm_exp'):
        teams[col] = teams['team'].map(lambda t, c=col: team_records.get(t, {}).get(c, 0))
    teams['opp_3p_pct'] = (teams['opp_3pm'] / teams['opp_3pa'] * 100).round(1)
    teams['opp_3p_exp_pct'] = (teams['opp_3pm_exp'] / teams['opp_3pa'] * 100).round(1)
    teams['team_3p_pct'] = (teams['team_3pm'] / teams['team_3pa'] * 100).round(1)
    teams['team_3p_exp_pct'] = (teams['team_3pm_exp'] / teams['team_3pa'] * 100).round(1)

    total_poss = teams['home_poss_est'].fillna(0) + teams['away_poss_est'].fillna(0)
    total_adj_for = teams['home_pts_adj_for'].fillna(0) + teams['away_pts_adj_for'].fillna(0)
    total_adj_against = teams['home_pts_adj_against'].fillna(0) + teams['away_pts_adj_against'].fillna(0)
    teams['adj_ortg'] = (total_adj_for / total_poss * 100).round(1)
    teams['adj_drtg'] = (total_adj_against / total_poss * 100).round(1)
    teams['adj_nrtg'] = (teams['adj_ortg'] - teams['adj_drtg']).round(1)

    teams = teams.sort_values('wins', ascending=False)

    rows = ""
    for rank, (_, row) in enumerate(teams.iterrows(), 1):
        record = f"{int(row['wins'])}-{int(row['losses'])}"
        adj_record = f"{int(row['adj_wins'])}-{int(row['adj_losses'])}"
        win_diff = int(row['wins']) - int(row['adj_wins'])
        diff_class = "positive" if win_diff > 0 else ("negative" if win_diff < 0 else "")
        diff_str = f"{win_diff:+d}" if win_diff != 0 else "0"
        opp_diff = row['opp_3p_pct'] - row['opp_3p_exp_pct']
        opp_diff_class = "positive" if opp_diff < 0 else ("negative" if opp_diff > 0 else "")
        opp_diff_str = f"{opp_diff:+.1f}%"
        team_diff = row['team_3p_pct'] - row['team_3p_exp_pct']
        team_diff_class = "positive" if team_diff > 0 else ("negative" if team_diff < 0 else "")
        team_diff_str = f"{team_diff:+.1f}%"
        # net: positive means overall lucky (team shot above expected AND/OR opp shot below expected)
        net_diff = team_diff - opp_diff
        net_diff_class = "positive" if net_diff > 0 else ("negative" if net_diff < 0 else "")
        net_diff_str = f"{net_diff:+.1f}%"
        adj_ortg = row['adj_ortg']
        adj_drtg = row['adj_drtg']
        adj_nrtg = row['adj_nrtg']
        nrtg_class = "positive" if adj_nrtg > 0 else ("negative" if adj_nrtg < 0 else "")
        rows += f"""        <tr data-team="{row['team']}" data-wins="{int(row['wins'])}" data-adjwins="{int(row['adj_wins'])}" data-diff="{win_diff}" data-oppexp="{row['opp_3p_exp_pct']:.1f}" data-oppact="{row['opp_3p_pct']:.1f}" data-oppdiff="{opp_diff:.1f}" data-teamexp="{row['team_3p_exp_pct']:.1f}" data-teamact="{row['team_3p_pct']:.1f}" data-teamdiff="{team_diff:.1f}" data-netdiff="{net_diff:.1f}" data-adjortg="{adj_ortg:.1f}" data-adjdrtg="{adj_drtg:.1f}" data-adjnrtg="{adj_nrtg:.1f}">
            <td>{rank}</td>
            <td><strong>{row['team']}</strong></td>
            <td>{record}</td>
            <td>{adj_record}</td>
            <td class="{diff_class} clickable-diff" onclick="showFlippedGames('{row['team']}')">{diff_str}</td>
            <td>{row['opp_3p_exp_pct']:.1f}%</td>
            <td>{row['opp_3p_pct']:.1f}%</td>
            <td class="{opp_diff_class}">{opp_diff_str}</td>
            <td>{row['team_3p_exp_pct']:.1f}%</td>
            <td>{row['team_3p_pct']:.1f}%</td>
            <td class="{team_diff_class}">{team_diff_str}</td>
            <td class="{net_diff_class}">{net_diff_str}</td>
            <td>{adj_ortg:.1f}</td>
            <td>{adj_drtg:.1f}</td>
            <td class="{nrtg_class}">{adj_nrtg:+.1f}</td>
        </tr>
"""
    return rows


def _build_swing_rows(source_df: pd.DataFrame) -> str:
    if source_df.empty:
        return '        <tr><td colspan="5" style="text-align:center;color:#888;">No games yet</td></tr>\n'
    df = source_df.copy()
    df['abs_margin_delta'] = df['margin_delta'].abs()
    biggest = df.nlargest(15, 'abs_margin_delta')
    rows = ""
    for _, row in biggest.iterrows():
        winner = row['home_team'] if row['margin_actual'] > 0 else row['away_team']
        adj_winner = row['home_team'] if row['margin_adj'] > 0 else row['away_team']
        flip = "&#9888;" if winner != adj_winner else ""
        away_adj = f"{row['away_pts_adj']:.1f}"
        home_adj = f"{row['home_pts_adj']:.1f}"
        adj_score = (f"{away_adj}-<strong>{home_adj}</strong>" if row['margin_adj'] > 0
                     else f"<strong>{away_adj}</strong>-{home_adj}")
        lucky_team = row['home_team'] if row['margin_delta'] < 0 else row['away_team']
        luck_amount = abs(row['margin_delta'])
        rows += f"""        <tr>
            <td>{row['date']}</td>
            <td>{row['away_team']} @ {row['home_team']}</td>
            <td>{int(row['away_pts_actual'])}-{int(row['home_pts_actual'])}</td>
            <td>{adj_score} {flip}</td>
            <td class="positive">{lucky_team}: +{luck_amount:.1f}</td>
        </tr>
"""
    return rows


def generate_report():
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Template file not found: {TEMPLATE_PATH}")
    template = TEMPLATE_PATH.read_text(encoding='utf-8')

    df = pd.read_csv("data/adjusted_games.csv")
    # adjusted_games.csv accumulates across seasons; the report covers only the current one.
    df = df[df['date'] >= SEASON_START_DATE]
    if df.empty:
        print(f"No {SEASON} games yet; leaving existing report in place.")
        return False
    df = _apply_shooting_luck(df)

    # Rows without game_type were written before the column existed — treat as regular season.
    if 'game_type' in df.columns:
        df['game_type'] = df['game_type'].fillna('regular')
    else:
        df['game_type'] = 'regular'

    rs_df = df[df['game_type'] == 'regular'].copy()
    playoff_df = df[df['game_type'] == 'playoff'].copy()
    alt_df = df.copy()
    for side in ('home', 'away'):
        alt_df[f'{side}_pts_adj'] = alt_df[f'{side}_pts_adj_3pt_ft']
    alt_df['margin_adj'] = alt_df['margin_adj_3pt_ft']
    alt_df['margin_delta'] = alt_df['margin_delta_3pt_ft']
    alt_rs_df = alt_df[alt_df['game_type'] == 'regular'].copy()
    alt_playoff_df = alt_df[alt_df['game_type'] == 'playoff'].copy()
    # play-in (game_type == 'playin') is excluded from both ranking tables

    game_poss = _load_game_possessions()
    team_rows = _build_team_rows(rs_df, game_poss)
    playoff_team_rows = _build_team_rows(playoff_df, game_poss)
    team_rows_3pt_ft = _build_team_rows(alt_rs_df, game_poss)
    priors = _load_team_priors()
    power = _power_payload({"default": rs_df, "3pt_ft": alt_rs_df}, game_poss, priors)
    POWER_JS_PATH.write_text("window.TEAM_POWER = " + json.dumps(power, separators=(",", ":"))
                             + ";\n", encoding="utf-8")
    injuries = _injury_payload(rs_df, game_poss, priors)
    if injuries:
        INJURY_JS_PATH.write_text("window.TEAM_INJURIES = " + json.dumps(injuries, separators=(",", ":"))
                                  + ";\n", encoding="utf-8")
    playoff_team_rows_3pt_ft = _build_team_rows(alt_playoff_df, game_poss)

    swing_rows = _build_swing_rows(rs_df)
    playoff_swing_rows = _build_swing_rows(playoff_df)
    swing_rows_3pt_ft = _build_swing_rows(alt_rs_df)
    playoff_swing_rows_3pt_ft = _build_swing_rows(alt_playoff_df)

    # Calendar game objects
    has_swing_player = 'swing_player' in df.columns
    has_top_swing_players = 'top_swing_players' in df.columns
    games_by_date = {}
    for _, row in df.sort_values('date').iterrows():
        date = row['date']
        if date not in games_by_date:
            games_by_date[date] = []
        game_obj = {
            'home_team': row['home_team'],
            'away_team': row['away_team'],
            'game_type': row['game_type'],
            'home_pts_actual': int(row['home_pts_actual']),
            'away_pts_actual': int(row['away_pts_actual']),
            'home_pts_adj': round(row['home_pts_adj'], 1),
            'away_pts_adj': round(row['away_pts_adj'], 1),
            'margin_actual': int(row['margin_actual']),
            'margin_adj': round(row['margin_adj'], 2),
            'margin_delta': round(row['margin_delta'], 1),
            'home_pts_adj_default': round(row['home_pts_adj'], 1),
            'away_pts_adj_default': round(row['away_pts_adj'], 1),
            'margin_adj_default': round(row['margin_adj'], 2),
            'margin_delta_default': round(row['margin_delta'], 1),
            'home_pts_adj_3pt_ft': round(row['home_pts_adj_3pt_ft'], 1),
            'away_pts_adj_3pt_ft': round(row['away_pts_adj_3pt_ft'], 1),
            'margin_adj_3pt_ft': round(row['margin_adj_3pt_ft'], 2),
            'margin_delta_3pt_ft': round(row['margin_delta_3pt_ft'], 1),
            'home_3pa': int(row['home_3pa']),
            'home_3pm': int(row['home_3pm_actual']),
            'home_3pm_exp': round(row['home_3pm_exp'], 1),
            'away_3pa': int(row['away_3pa']),
            'away_3pm': int(row['away_3pm_actual']),
            'away_3pm_exp': round(row['away_3pm_exp'], 1),
        }
        if has_swing_player:
            game_obj['swing_player'] = row['swing_player'] if pd.notna(row['swing_player']) else ''
            game_obj['swing_player_delta'] = round(row['swing_player_delta'], 1) if pd.notna(row['swing_player_delta']) else 0
            if 'swing_player_fg3m' in row and pd.notna(row.get('swing_player_fg3m')):
                game_obj['swing_player_fg3m'] = int(row['swing_player_fg3m'])
                game_obj['swing_player_fg3a'] = int(row['swing_player_fg3a'])
        if has_top_swing_players and pd.notna(row.get('top_swing_players')):
            try:
                game_obj['top_swing_players'] = json.loads(row['top_swing_players'])
            except (json.JSONDecodeError, TypeError):
                game_obj['top_swing_players'] = []
        games_by_date[date].append(game_obj)

    games_json = json.dumps(games_by_date)
    most_recent_date = df['date'].max()

    min_date = pd.to_datetime(df['date'].min())
    max_date = pd.to_datetime(df['date'].max())
    season_months = []
    current = min_date.replace(day=1)
    while current <= max_date:
        season_months.append({'year': current.year, 'month': current.month - 1})
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    season_months_json = json.dumps(season_months)

    html = template
    html = html.replace('{{SEASON_LABEL}}', SEASON)
    html = html.replace('{{SEASON_DATE}}', df['date'].max())
    html = html.replace('{{GAME_COUNT}}', str(len(rs_df)))
    html = html.replace('{{GAMES_JSON}}', games_json)
    html = html.replace('{{MOST_RECENT_DATE}}', most_recent_date)
    html = html.replace('{{SEASON_MONTHS_JSON}}', season_months_json)
    html = html.replace('{{TEAM_RANKINGS_ROWS}}', team_rows)
    html = html.replace('{{PLAYOFF_RANKINGS_ROWS}}', playoff_team_rows)
    html = html.replace('{{TEAM_RANKINGS_ROWS_3PT_FT}}', team_rows_3pt_ft)
    html = html.replace('{{PLAYOFF_RANKINGS_ROWS_3PT_FT}}', playoff_team_rows_3pt_ft)
    html = html.replace('{{BIGGEST_SWINGS_ROWS}}', swing_rows)
    html = html.replace('{{PLAYOFF_BIGGEST_SWINGS_ROWS}}', playoff_swing_rows)
    html = html.replace('{{BIGGEST_SWINGS_ROWS_3PT_FT}}', swing_rows_3pt_ft)
    html = html.replace('{{PLAYOFF_BIGGEST_SWINGS_ROWS_3PT_FT}}', playoff_swing_rows_3pt_ft)
    html = html.replace('{{GENERATED_TIMESTAMP}}', datetime.now().strftime('%Y-%m-%d %H:%M'))

    output_path = Path("data/3pt_luck_report.html")
    output_path.write_text(html, encoding='utf-8')
    print(f"Report saved to: {output_path.absolute()}")

    index_path = Path("index.html")
    index_path.write_text(html, encoding='utf-8')
    print(f"Also saved to: {index_path.absolute()}")

    return output_path


if __name__ == "__main__":
    generate_report()
