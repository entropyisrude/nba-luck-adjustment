"""Replay and repair canonical regular-season lineup stints.

Writes only versioned derived artifacts. Existing site/production CSVs and the
analytics database are never modified.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace

import duckdb
import numpy as np
import pandas as pd
import yaml

import src.onoff as onoff
from metric.build_rapm_target import calibrate_game
from scripts.canonical_lineup_solver import (prepare_actions,
    action_interval_index, game_boundaries, resolve_substitutions_from_official_roster, score_points_by_interval,
    solve_game_with_seed)
from src.onoff import _elapsed_game_seconds_precise

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "nba_analytics.duckdb"
PBP = Path(r"C:\Users\Dave\Downloads\nba-metric-data\PlayByPlay.parquet")
AUDIT = ROOT / "outputs" / "contextual_causal" / "canonical_game_integrity.parquet"
OUT = ROOT / "derived" / "contextual_causal" / "canonical_rebuild"
STATE = ROOT / "data" / "player_state_historical_pbp.csv"

PBP_COLS = ["gameId", "period", "clock", "actionType", "subType", "teamId",
            "personId", "playerName", "playerNameI", "description",
            "scoreHome", "scoreAway", "shotResult", "shotValue",
            "orderNumber", "actionNumber", "qualifiers", "assistPersonId"]


def player_checks(st: pd.DataFrame, official: pd.DataFrame) -> tuple[float, float]:
    rows = []
    for r in st.itertuples(index=False):
        for side, sign in (("home", 1.0), ("away", -1.0)):
            pm = sign * (r.home_pts - r.away_pts)
            for k in range(1, 6):
                rows.append((int(getattr(r, f"{side}_p{k}")), r.seconds, pm))
    rec = (pd.DataFrame(rows, columns=["player_id", "seconds", "pm"])
           .groupby("player_id").sum())
    off = official.set_index("player_id")
    ids = set(rec.index) | set(off.index)
    sec_err = []; pm_err = []
    for pid in ids:
        sec_err.append(abs(float(rec.seconds.get(pid, 0.0))
                           - float(off.minutes.get(pid, 0.0)) * 60.0))
        pm_err.append(abs(float(rec.pm.get(pid, 0.0))
                          - float(off.plus_minus_actual.get(pid, 0.0))))
    return max(sec_err, default=np.inf), max(pm_err, default=np.inf)


def parse_box_minutes(values: pd.Series) -> pd.Series:
    """Parse decimal minutes, MM:SS, or NBA PT##M##S minute fields."""
    text = values.astype(str).str.strip()
    numeric = pd.to_numeric(text, errors="coerce")
    parts = text.str.extract(r"^(?:PT)?(\d+)(?::|M)(\d+(?:\.\d+)?)(?:S)?$")
    parsed = (pd.to_numeric(parts[0], errors="coerce")
              + pd.to_numeric(parts[1], errors="coerce") / 60.0)
    return parsed.where(parsed.notna(), numeric)


def evidence_checks(st: pd.DataFrame, actions: list[dict],
                    seed: pd.DataFrame) -> dict[str, float | int]:
    """Audit local basketball evidence, not merely full-game box totals."""
    sub_times = set()
    for a in actions:
        if str(a.get("actionType") or "").lower() == "substitution":
            e = _elapsed_game_seconds_precise(a.get("period"), a.get("clock"))
            if e is not None: sub_times.add((a.get("period"), a.get("clock")))

    unsupported = recorded = inferred_partial = 0
    for i in range(len(st) - 1):
        before, after = st.iloc[i], st.iloc[i + 1]
        b = float(before.end_elapsed)
        if abs(b - float(after.start_elapsed)) > 1e-5:
            continue
        is_period = (b in {720., 1440., 2160., 2880.}
                     or (b > 2880 and abs((b - 2880) % 300) < 1e-6))
        for side in ("home", "away"):
            tid = int(before[f"{side}_id"])
            old = {int(before[f"{side}_p{k}"]) for k in range(1, 6)}
            new = {int(after[f"{side}_p{k}"]) for k in range(1, 6)}
            changed = old ^ new
            batch = []
            for a in actions:
                e = _elapsed_game_seconds_precise(a.get("period"), a.get("clock"))
                try: atid = int(a.get("teamId") or 0)
                except Exception: atid = 0
                if (str(a.get("actionType") or "").lower() == "substitution"
                        and atid == tid and e is not None
                        and abs(float(e) - b) < 1e-4):
                    batch.append(a)
            affected: set[int] = set(); outs: set[int] = set(); ins: set[int] = set()
            for a in batch:
                try: pid = int(a.get("personId") or 0)
                except Exception: pid = 0
                candidates = set()
                for value in a.get("candidatePersonIds") or []:
                    try: candidates.add(int(value))
                    except Exception: pass
                affected |= candidates
                if pid > 0:
                    affected.add(pid)
                    if str(a.get("subType") or "").lower() == "out": outs.add(pid)
                    if str(a.get("subType") or "").lower() == "in": ins.add(pid)
            if not is_period:
                unexplained = changed - affected
                # A common feed defect records only one side of a substitution.
                # Certify the missing partner only when the proposed lineup
                # change uniquely completes an otherwise valid same-clock batch.
                old_only = old - new
                new_only = new - old
                known_out = outs & old_only
                known_in = ins & new_only
                inferred: set[int] = set()
                if unexplained and len(old_only) == len(new_only):
                    # This also covers a double substitution whose feed lists
                    # both entrants but omits both corresponding exits (or the
                    # reverse).  Never infer missing players on both sides.
                    if (unexplained <= old_only
                            and len(known_in) == len(old_only)
                            and len(known_out) + len(unexplained) == len(old_only)):
                        inferred |= unexplained
                    elif (unexplained <= new_only
                          and len(known_out) == len(new_only)
                          and len(known_in) + len(unexplained) == len(new_only)):
                        inferred |= unexplained
                inferred_partial += len(inferred)
                unsupported += len(unexplained - inferred)
            if not is_period and outs and len(outs) == len(ins) and outs.isdisjoint(ins):
                recorded += len(outs - old) + len(outs & new)
                recorded += len(ins & old) + len(ins - new)

    # A non-substitution action proves that player was on court at that time.
    action_violations = 0
    valid_players = set()
    for side in ("home", "away"):
        for k in range(1, 6):
            valid_players |= set(pd.to_numeric(st[f"{side}_p{k}"],
                                                errors="coerce").dropna().astype(int))
    proving = {"2pt", "3pt", "heave", "rebound", "turnover", "foul",
               "freethrow", "free throw", "jumpball", "jump ball"}
    for a in actions:
        kind = str(a.get("actionType") or "").lower()
        if kind not in proving: continue
        e = _elapsed_game_seconds_precise(a.get("period"), a.get("clock"))
        if e is None or (a.get("period"), a.get("clock")) in sub_times: continue
        try: tid, pid = int(a.get("teamId") or 0), int(a.get("personId") or 0)
        except Exception: continue
        if tid <= 0 or pid <= 0 or pid not in valid_players: continue
        ordered = st.sort_values("start_elapsed").reset_index(drop=True)
        stint_boundaries = np.r_[ordered.start_elapsed.to_numpy(dtype=float),
                                 float(ordered.end_elapsed.iloc[-1])]
        ii = action_interval_index(stint_boundaries, a)
        if ii >= len(ordered): action_violations += 1; continue
        row = ordered.iloc[ii]
        side = "home" if int(row.home_id) == tid else "away"
        lineup = {int(row[f"{side}_p{k}"]) for k in range(1, 6)}
        if pid not in lineup: action_violations += 1

    # Time-weighted agreement with the deterministic replay is diagnostic;
    # disagreement is allowed only when stronger official/local evidence wins.
    agree = covered = 0.0
    if seed is not None and not seed.empty:
        for row in st.itertuples(index=False):
            mid = (float(row.start_elapsed) + float(row.end_elapsed)) / 2
            hit = seed[(seed.start_elapsed <= mid) & (seed.end_elapsed >= mid)]
            if hit.empty: continue
            duration = float(row.end_elapsed) - float(row.start_elapsed)
            covered += duration
            sr = hit.iloc[-1]
            same = True
            for side in ("home", "away"):
                a = {int(getattr(row, f"{side}_p{k}")) for k in range(1, 6)}
                z = {int(sr[f"{side}_p{k}"]) for k in range(1, 6)}
                same &= a == z
            if same: agree += duration
    return {"unsupported_player_changes": unsupported,
            "inferred_partial_substitutions": inferred_partial,
            "recorded_transition_violations": recorded,
            "action_presence_violations": action_violations,
            "seed_agreement": agree / covered if covered else np.nan}


def assess_candidate(st: pd.DataFrame, official: pd.DataFrame,
                     actions: list[dict], seed: pd.DataFrame,
                     gid: str, expected: float) -> dict:
    for col in ("home_pts", "away_pts", "home_pts_adj", "away_pts_adj"):
        st[col] = pd.to_numeric(st[col], errors="coerce").astype(float)
    totals = (float(official[official.home_away == "home"].team_pts_actual.max()),
              float(official[official.home_away == "away"].team_pts_actual.max()))
    pm = {(gid, int(r.player_id)): float(r.plus_minus_actual)
          for r in official.itertuples()}
    calibrated = calibrate_game(st, pm, totals)
    sec_err, pm_err = player_checks(st, official)
    score_err = (abs(st.home_pts.sum() - totals[0])
                 + abs(st.away_pts.sum() - totals[1]))
    ordered = st.sort_values("start_elapsed")
    adjacency_error = (float(np.abs(ordered.start_elapsed.iloc[1:].to_numpy()
                                    - ordered.end_elapsed.iloc[:-1].to_numpy()).sum())
                       if len(ordered) > 1 else np.inf)
    coverage_ok = (not ordered.empty
                   and abs(float(ordered.start_elapsed.iloc[0])) < 1e-4
                   and abs(float(ordered.end_elapsed.iloc[-1]) - expected) < 1e-4
                   and abs(float(ordered.seconds.sum()) - expected) < 1e-3
                   and adjacency_error < 1e-3)
    evidence = evidence_checks(st, actions, seed)
    accepted = (coverage_ok and score_err < .5 and sec_err <= 75
                and pm_err < .5 and calibrated
                and evidence["unsupported_player_changes"] == 0
                and evidence["recorded_transition_violations"] == 0
                and evidence["action_presence_violations"] == 0)
    return {"score_error": score_err, "max_seconds_error": sec_err,
            "max_pm_error": pm_err, "calibrated": calibrated,
            "coverage_ok": coverage_ok, "adjacency_error": adjacency_error,
            **evidence, "accepted": accepted}


def assemble(game_id: str, date, boundaries, home, away, actions) -> pd.DataFrame:
    hp, ap = score_points_by_interval(actions, boundaries)
    rows = []; ch = ca = 0.0
    for i, (start, end) in enumerate(zip(boundaries[:-1], boundaries[1:])):
        if end <= start:
            continue
        hl, al = home.lineups[i], away.lineups[i]
        row = {"game_id": game_id, "stint_index": len(rows),
               "home_id": home.team_id, "away_id": away.team_id,
               "seconds": float(end-start), "home_pts": float(hp[i]),
               "away_pts": float(ap[i]), "home_pts_adj": float(hp[i]),
               "away_pts_adj": float(ap[i]), "start_elapsed": float(start),
               "end_elapsed": float(end), "date": pd.Timestamp(date),
               "start_home_score": ch, "start_away_score": ca}
        for k, pid in enumerate(hl, 1): row[f"home_p{k}"] = int(pid)
        for k, pid in enumerate(al, 1): row[f"away_p{k}"] = int(pid)
        ch += hp[i]; ca += ap[i]
        row["end_home_score"] = ch; row["end_away_score"] = ca
        rows.append(row)
    return pd.DataFrame(rows)


def project_seed_to_event_boundaries(game_id: str, date, seed: pd.DataFrame,
                                     boundaries: np.ndarray, actions: list[dict],
                                     home_id: int, away_id: int) -> pd.DataFrame:
    """Keep replay lineups but put their changes on exact PBP event clocks."""
    lineups: dict[str, list[tuple[int, ...]]] = {"home": [], "away": []}
    ordered = seed.sort_values("start_elapsed")
    for start, end in zip(boundaries[:-1], boundaries[1:]):
        mid = (float(start) + float(end)) / 2.0
        hit = ordered[(ordered.start_elapsed <= mid) & (ordered.end_elapsed >= mid)]
        if hit.empty:
            raise ValueError(f"seed has no lineup at elapsed={mid}")
        row = hit.iloc[-1]
        for side in ("home", "away"):
            lineup = tuple(sorted(int(row[f"{side}_p{k}"]) for k in range(1, 6)))
            if len(set(lineup)) != 5:
                raise ValueError(f"non-five-player {side} seed lineup at {mid}")
            lineups[side].append(lineup)
    home = SimpleNamespace(team_id=home_id, lineups=lineups["home"])
    away = SimpleNamespace(team_id=away_id, lineups=lineups["away"])
    return assemble(game_id, date, boundaries, home, away, actions)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--games", default="", help="comma-separated normalized IDs")
    ap.add_argument("--game-file", default="",
                    help="CSV containing a game_id column (avoids long command lines)")
    ap.add_argument("--failed-only", action="store_true")
    ap.add_argument("--tag", default="pilot")
    ap.add_argument("--solver-seconds", type=float, default=8.0)
    ap.add_argument("--prefer-gamerotation", action="store_true",
                    help="assess the official cached GameRotation lineup timeline before MILP repair")
    ap.add_argument("--gamerotation-only", action="store_true",
                    help="stop after assessing GameRotation instead of rerunning prior repair tiers")
    ap.add_argument("--gamerotation-dir", default="",
                    help="optional directory of per-game split-orient GameRotation JSON files")
    ap.add_argument("--official-box-csv", default="",
                    help="override official minutes/plus-minus from a cross-checked box-score CSV")
    ap.add_argument("--resume", action="store_true",
                    help="resume this tag and checkpoint every 10 games")
    args = ap.parse_args()
    if args.gamerotation_only:
        args.prefer_gamerotation = True
    if args.gamerotation_dir:
        rotation_dir = Path(args.gamerotation_dir).resolve()

        def load_local_gamerotation(game_id: str) -> pd.DataFrame:
            gid = str(game_id)
            candidates = [rotation_dir / f"{gid}.json"]
            if gid.isdigit():
                candidates.extend([rotation_dir / f"{gid.zfill(10)}.json",
                                   rotation_dir / f"{gid.lstrip('0')}.json"])
            for path in dict.fromkeys(candidates):
                if path.exists():
                    frame = pd.read_json(path, orient="split")
                    frame["start_elapsed"] = pd.to_numeric(
                        frame["IN_TIME_REAL"], errors="coerce") / 10.0
                    frame["end_elapsed"] = pd.to_numeric(
                        frame["OUT_TIME_REAL"], errors="coerce") / 10.0
                    return frame
            raise RuntimeError(f"local gamerotation miss for {gid}")

        onoff._load_stats_gamerotation = load_local_gamerotation
    OUT.mkdir(parents=True,exist_ok=True)
    stint_path = OUT / f"stints_{args.tag}.parquet"
    audit_path = OUT / f"audit_{args.tag}.csv"
    qa = pd.read_parquet(AUDIT)
    if args.game_file:
        requested = pd.read_csv(args.game_file)
        if "game_id" not in requested.columns:
            raise SystemExit("--game-file must contain a game_id column")
        requested_ids = set(requested.game_id.astype(str).str.lstrip("0"))
        wanted = qa[qa.game_id.astype(str).isin(requested_ids)]
    elif args.games:
        wanted = qa[qa.game_id.astype(str).isin(args.games.split(","))]
    else:
        wanted = qa.copy()
        if args.season is not None: wanted = wanted[wanted.season_year == args.season]
        if args.failed_only: wanted = wanted[~wanted.canonical_grade_a]
        if args.limit: wanted = wanted.head(args.limit)
    existing_stints = pd.DataFrame(); existing_audits = pd.DataFrame()
    if args.resume and audit_path.exists():
        existing_audits = pd.read_csv(audit_path)
        done = set(existing_audits.game_id.astype(str))
        wanted = wanted[~wanted.game_id.astype(str).isin(done)]
        if stint_path.exists(): existing_stints = pd.read_parquet(stint_path)
    ids = pd.DataFrame({"gid": wanted.game_id.astype(str).unique()})
    if ids.empty: raise SystemExit("no unprocessed games selected")

    con = duckdb.connect(); con.register("wanted", ids)
    actions = con.execute(f"""SELECT {','.join(PBP_COLS)}
      FROM read_parquet('{PBP.as_posix()}') p JOIN wanted w
      ON ltrim(CAST(p.gameId AS VARCHAR),'0')=w.gid""").df(); con.close()
    con = duckdb.connect(str(DB), read_only=True); con.register("wanted", ids)
    official = con.execute("""SELECT DISTINCT ltrim(game_id,'0') game_id,
      date,player_id,player_name,team_id,minutes,plus_minus_actual,starter,home_away,
      team_pts_actual FROM player_game_facts p JOIN wanted w
      ON ltrim(p.game_id,'0')=w.gid WHERE minutes>0""").df(); con.close()
    official_source = "player_game_facts"
    if args.official_box_csv:
        alternate = pd.read_csv(args.official_box_csv)
        required = {"game_id", "player_id", "minutes", "plus_minus_actual"}
        missing = required - set(alternate.columns)
        if missing:
            raise SystemExit(f"--official-box-csv missing columns: {sorted(missing)}")
        alternate["game_id"] = (alternate.game_id.astype(str).str.split(".").str[0]
                                .str.lstrip("0"))
        alternate["minutes_override"] = parse_box_minutes(alternate.minutes)
        alternate["pm_override"] = pd.to_numeric(
            alternate.plus_minus_actual, errors="coerce")
        alternate = alternate.loc[
            alternate.game_id.isin(ids.gid),
            ["game_id", "player_id", "minutes_override", "pm_override"]]
        alternate["player_id"] = pd.to_numeric(
            alternate.player_id, errors="coerce").astype("Int64")
        alternate = alternate.dropna(subset=["player_id"]).drop_duplicates(
            ["game_id", "player_id"], keep="last")
        official = official.merge(alternate, on=["game_id", "player_id"], how="left")
        official["minutes"] = official.minutes_override.where(
            official.minutes_override.notna(), official.minutes)
        official["plus_minus_actual"] = official.pm_override.where(
            official.pm_override.notna(), official.plus_minus_actual)
        official = official.drop(columns=["minutes_override", "pm_override"])
        official_source = Path(args.official_box_csv).name
    state = pd.read_csv(STATE); cfg = yaml.safe_load((ROOT/"config.yaml").read_text())
    onoff.get_starters_by_team = lambda *a, **k: {}

    stints_out=([existing_stints] if not existing_stints.empty else [])
    audits=(existing_audits.to_dict("records") if not existing_audits.empty else [])
    def checkpoint() -> None:
        st_all = pd.concat(stints_out,ignore_index=True) if stints_out else pd.DataFrame()
        pd.DataFrame(audits).to_csv(audit_path,index=False)
        st_all.to_parquet(stint_path,index=False)
    for n,(gid,frame) in enumerate(actions.groupby(actions.gameId.astype(str).str.lstrip("0"))):
        off=official[official.game_id==gid].copy(); date=pd.to_datetime(off.date.iloc[0])
        home_id=int(off[off.home_away=="home"].team_id.iloc[0]); away_id=int(off[off.home_away=="away"].team_id.iloc[0])
        prepared=prepare_actions(frame); onoff.get_playbyplay_actions=lambda *a,_x=prepared,**k:_x
        onoff.get_game_home_away_team_ids=lambda *a,_x=(home_id,away_id),**k:_x
        onoff._load_stats_home_away=lambda *a,_x=(home_id,away_id),**k:_x
        rotation_box = off[["player_id", "plus_minus_actual"]].rename(
            columns={"player_id": "PLAYER_ID", "plus_minus_actual": "PLUS_MINUS"})
        onoff._load_stats_boxscore=lambda *a,_x=rotation_box,**k:{"players":_x}
        try:
            _,seed,_=onoff.compute_adjusted_onoff_for_game(gid,date.strftime("%m/%d/%Y"),state,float(cfg["orb_rate"]),float(cfg["ppp"]))
            resolved = resolve_substitutions_from_official_roster(prepared, off)
            expected = float(game_boundaries(resolved)[-1])
            seed = seed.copy(); seed["game_id"] = gid; seed["date"] = date
            try:
                seed_result = assess_candidate(seed, off, resolved, seed, gid, expected)
            except (KeyError, ValueError) as exc:
                seed_result = {"accepted": False,
                               "assessment_error": repr(exc)}
            result = seed_result
            method = "deterministic_replay"
            st = seed
            if args.prefer_gamerotation and not result["accepted"]:
                try:
                    _, rotation_stints, _ = onoff._compute_adjusted_onoff_for_game_with_gamerotation(
                        gid, date.strftime("%m/%d/%Y"), state,
                        float(cfg["orb_rate"]), float(cfg["ppp"]))
                    if not rotation_stints.empty:
                        rotation_stints = rotation_stints.copy()
                        rotation_stints["game_id"] = gid
                        rotation_stints["date"] = date
                        rotation_result = assess_candidate(
                            rotation_stints, off, resolved, seed, gid, expected)
                        if rotation_result["accepted"] or args.gamerotation_only:
                            result = rotation_result
                            method = ("official_gamerotation"
                                      if rotation_result["accepted"]
                                      else "official_gamerotation_rejected")
                            st = rotation_stints
                except (KeyError, ValueError, RuntimeError):
                    if args.gamerotation_only:
                        method = "official_gamerotation_unavailable"
            if not result["accepted"] and not args.gamerotation_only:
                b = game_boundaries(resolved)
                try:
                    projected = project_seed_to_event_boundaries(
                        gid, date, seed, b, resolved, home_id, away_id)
                    result = assess_candidate(projected, off, resolved, seed, gid, expected)
                    method = "boundary_projected_replay"
                    st = projected
                except (ValueError, KeyError):
                    result = seed_result
            if not result["accepted"] and not args.gamerotation_only:
                acts,b,h,a,_=solve_game_with_seed(frame,off,home_id,away_id,seed,args.solver_seconds)
                st=assemble(gid,date,b,h,a,acts)
                resolved = resolve_substitutions_from_official_roster(acts, off)
                result = assess_candidate(st, off, resolved, seed, gid, expected)
                method = "replay_seeded_milp"
            else:
                if method == "deterministic_replay": st = seed
            accepted = bool(result["accepted"])
            st["canonical_accepted"]=accepted; st["canonical_method"]=method
            stints_out.append(st)
            audits.append({"game_id":gid,"method":method,
                           "official_source": official_source,
                           "seed_coverage":(float(seed["seconds"].sum()) / expected
                                            if "seconds" in seed.columns else 0.0),
                           **{f"seed_{k}": v for k, v in seed_result.items()
                              if k != "seed_agreement"},
                           **result, "error":""})
        except Exception as exc:
            audits.append({"game_id":gid,"accepted":False,"error":repr(exc)})
        if (n+1)%10==0:
            if args.resume: checkpoint()
            print(f"  {n+1}/{len(ids)}",flush=True)
    st_all=pd.concat(stints_out,ignore_index=True) if stints_out else pd.DataFrame()
    au=pd.DataFrame(audits)
    st_all.to_parquet(stint_path,index=False)
    au.to_csv(audit_path,index=False)
    print(au.accepted.value_counts(dropna=False).to_string())
    print(f"wrote {len(st_all)} stints and {len(au)} audits to {OUT}")


if __name__ == "__main__": main()
