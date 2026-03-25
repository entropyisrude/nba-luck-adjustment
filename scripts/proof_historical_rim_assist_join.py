from __future__ import annotations

import argparse
import csv
import re
import sqlite3
from pathlib import Path


ROOT = Path("/mnt/c/users/dave/Downloads/nba-onoff-publish")
AUDIT_DIR = ROOT / "data" / "audits"
SQLITE_PATH = Path("/mnt/c/users/dave/Downloads/nba-boxscore-data/kaggle-basketball/nba.sqlite")
V3_GLOB_ROOT = Path("/mnt/c/users/dave/Downloads/nba-3pt-adjust-local-backups/untracked_20260312_081227")
OTHER_RIM_DISTANCE_MAX = 30.0
RIM_SHOTDISTANCE_MAX_FEET = 5.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Proof-of-concept join between historical assist-bearing PBP and v3 coordinate PBP.")
    parser.add_argument("--season-start-year", type=int, default=2003, help="Season start year, e.g. 2003 for 2003-04.")
    return parser.parse_args()


def season_label(start_year: int) -> str:
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def season_id(start_year: int) -> str:
    return f"2{start_year}"


def find_v3_file(start_year: int) -> Path:
    matches = sorted(V3_GLOB_ROOT.glob(f"**/nbastatsv3_{start_year}.csv"))
    if not matches:
        raise FileNotFoundError(f"no v3 file found for {start_year}")
    return matches[0]


def normalize_clock(clock: str) -> str:
    if not clock:
        return ""
    clock = str(clock).strip()
    if clock.startswith("PT"):
        m = re.match(r"PT(?P<m>\d+)M(?P<s>\d+(?:\.\d+)?)S", clock)
        if not m:
            return clock
        minutes = int(m.group("m"))
        seconds = int(float(m.group("s")))
        return f"{minutes}:{seconds:02d}"
    return clock


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ").strip())


def load_v3_events(path: Path, wanted_keys: set[tuple[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    events: dict[tuple[str, str], dict[str, str]] = {}
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = str(row["gameId"]).zfill(10)
            action = str(row["actionNumber"])
            key = (gid, action)
            if key in wanted_keys:
                events[key] = row
    return events


def fetch_old_candidates(start_year: int) -> list[dict[str, str]]:
    con = sqlite3.connect(SQLITE_PATH)
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT
            pbp.game_id,
            pbp.eventnum,
            pbp.period,
            pbp.pctimestring,
            pbp.player1_id,
            pbp.player1_name,
            pbp.player2_id,
            pbp.player2_name,
            COALESCE(pbp.homedescription, pbp.visitordescription, '') AS description
        FROM play_by_play pbp
        JOIN game g
          ON pbp.game_id = g.game_id
        WHERE g.season_id = ?
          AND pbp.eventmsgtype = 1
          AND pbp.player2_id IS NOT NULL
          AND CAST(pbp.player2_id AS VARCHAR) != '0'
          AND COALESCE(pbp.homedescription, pbp.visitordescription, '') NOT LIKE '%3PT%'
        ORDER BY pbp.game_id, pbp.eventnum
        """,
        [season_id(start_year)],
    ).fetchall()
    con.close()
    out = []
    for row in rows:
        out.append(
            {
                "game_id": str(row[0]).zfill(10),
                "eventnum": str(row[1]),
                "period": str(row[2]),
                "clock": normalize_clock(str(row[3])),
                "shooter_id": str(row[4] or ""),
                "shooter_name": str(row[5] or ""),
                "assist_id": str(row[6] or ""),
                "assist_name": str(row[7] or ""),
                "description": normalize_text(str(row[8] or "")),
            }
        )
    return out


def classify_rim(v3: dict[str, str]) -> tuple[int, int, int]:
    subtype = str(v3.get("subType") or "")
    if "Layup" in subtype:
        return 1, 0, 0
    if "Dunk" in subtype or "DUNK" in subtype:
        return 0, 1, 0
    try:
        shot_distance = float(v3.get("shotDistance") or "")
    except Exception:
        shot_distance = None
    if shot_distance is not None:
        return (0, 0, 1) if shot_distance <= RIM_SHOTDISTANCE_MAX_FEET else (0, 0, 0)
    try:
        x = float(v3.get("xLegacy") or "")
        y = float(v3.get("yLegacy") or "")
        dist = (x * x + y * y) ** 0.5
    except Exception:
        dist = None
    if dist is not None and dist <= OTHER_RIM_DISTANCE_MAX:
        return 0, 0, 1
    return 0, 0, 0


def main() -> None:
    args = parse_args()
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    v3_path = find_v3_file(args.season_start_year)
    old_rows = fetch_old_candidates(args.season_start_year)
    wanted_keys = {(row["game_id"], row["eventnum"]) for row in old_rows}
    v3_events = load_v3_events(v3_path, wanted_keys)

    matched_rows = []
    unmatched_rows = []
    provisional_player_counts: dict[tuple[str, str], dict[str, int]] = {}
    period_mismatch = 0
    clock_mismatch = 0
    shooter_mismatch = 0
    description_exact_match = 0

    for row in old_rows:
        v3 = v3_events.get((row["game_id"], row["eventnum"]))
        if not v3:
            unmatched_rows.append(row)
            continue

        v3_period = str(v3.get("period") or "")
        v3_clock = normalize_clock(str(v3.get("clock") or ""))
        v3_shooter = str(v3.get("personId") or "")
        v3_desc = normalize_text(str(v3.get("description") or ""))

        if row["period"] != v3_period:
            period_mismatch += 1
        if row["clock"] != v3_clock:
            clock_mismatch += 1
        if row["shooter_id"] != v3_shooter:
            shooter_mismatch += 1
        if row["description"] == v3_desc:
            description_exact_match += 1

        layup, dunk, other = classify_rim(v3)
        matched_rows.append(
            {
                **row,
                "v3_period": v3_period,
                "v3_clock": v3_clock,
                "v3_shooter_id": v3_shooter,
                "v3_shooter_name": str(v3.get("playerName") or ""),
                "v3_description": v3_desc,
                "shot_distance": str(v3.get("shotDistance") or ""),
                "subType": str(v3.get("subType") or ""),
                "layup": layup,
                "dunk": dunk,
                "other_rim": other,
                "is_rim_any": layup + dunk + other,
            }
        )

        if layup + dunk + other > 0:
            key = (row["assist_id"], row["assist_name"])
            provisional_player_counts.setdefault(key, {"layup": 0, "dunk": 0, "other": 0})
            provisional_player_counts[key]["layup"] += layup
            provisional_player_counts[key]["dunk"] += dunk
            provisional_player_counts[key]["other"] += other

    total = len(old_rows)
    matched = len(matched_rows)
    unmatched = len(unmatched_rows)
    rim_matches = sum(int(r["is_rim_any"]) for r in matched_rows)

    summary_path = AUDIT_DIR / f"historical_rim_assist_join_summary_{args.season_start_year}.txt"
    with summary_path.open("w", encoding="utf-8") as f:
        f.write(f"season={season_label(args.season_start_year)}\n")
        f.write(f"v3_file={v3_path}\n")
        f.write(f"old_candidate_rows={total}\n")
        f.write(f"matched_rows={matched}\n")
        f.write(f"unmatched_rows={unmatched}\n")
        f.write(f"match_rate={matched / total if total else 0:.6f}\n")
        f.write(f"period_mismatch={period_mismatch}\n")
        f.write(f"clock_mismatch={clock_mismatch}\n")
        f.write(f"shooter_mismatch={shooter_mismatch}\n")
        f.write(f"description_exact_match={description_exact_match}\n")
        f.write(f"rim_matches={rim_matches}\n")

    matched_path = AUDIT_DIR / f"historical_rim_assist_join_matched_{args.season_start_year}.csv"
    unmatched_path = AUDIT_DIR / f"historical_rim_assist_join_unmatched_{args.season_start_year}.csv"
    players_path = AUDIT_DIR / f"historical_rim_assist_join_player_counts_{args.season_start_year}.csv"

    if matched_rows:
        with matched_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(matched_rows[0].keys()))
            writer.writeheader()
            writer.writerows(matched_rows)
    if unmatched_rows:
        with unmatched_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(unmatched_rows[0].keys()))
            writer.writeheader()
            writer.writerows(unmatched_rows)

    player_rows = []
    for (assist_id, assist_name), c in sorted(
        provisional_player_counts.items(),
        key=lambda kv: (-(kv[1]["layup"] + kv[1]["dunk"] + kv[1]["other"]), kv[0][1]),
    ):
        strict_total = c["layup"] + c["dunk"]
        total_all = strict_total + c["other"]
        player_rows.append(
            {
                "season": season_label(args.season_start_year),
                "assist_id": assist_id,
                "assist_name": assist_name,
                "layup_assists_created": c["layup"],
                "dunk_assists_created": c["dunk"],
                "other_rim_assists_created": c["other"],
                "rim_assists_strict": strict_total,
                "rim_assists_all": total_all,
            }
        )
    if player_rows:
        with players_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(player_rows[0].keys()))
            writer.writeheader()
            writer.writerows(player_rows)

    print(f"season={season_label(args.season_start_year)}")
    print(f"matched={matched} unmatched={unmatched} match_rate={matched / total if total else 0:.6f}")
    print(f"period_mismatch={period_mismatch} clock_mismatch={clock_mismatch} shooter_mismatch={shooter_mismatch}")
    print(f"rim_matches={rim_matches}")
    print(f"summary={summary_path}")
    print(f"matched_csv={matched_path}")
    print(f"unmatched_csv={unmatched_path}")
    print(f"player_counts_csv={players_path}")


if __name__ == "__main__":
    main()
