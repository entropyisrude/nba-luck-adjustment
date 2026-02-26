import argparse
import html
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.adjust import (
    KAPPA_MAX,
    KAPPA_MIN,
    MU_MAX,
    MU_MIN,
    SCALE_ATTEMPTS,
    get_shot_mix_adjustment,
    get_shot_multiplier,
)
from src.ingest import (
    get_boxscore_players,
    get_game_home_away_team_ids,
    get_playbyplay_actions,
    get_starters_by_team,
)
from src.onoff import _classify_area, _classify_shot_type, _sort_actions
from src.state import load_player_state


def _parse_clock_seconds(clock_str: str | None) -> int | None:
    if not clock_str:
        return None
    try:
        s = clock_str.replace("PT", "").replace("S", "")
        if "M" in s:
            mm, ss = s.split("M")
            minutes = int(mm)
            seconds = float(ss)
        else:
            minutes = 0
            seconds = float(s)
        return int(round(minutes * 60 + seconds))
    except Exception:
        return None


def _period_length_seconds(period: int) -> int:
    return 12 * 60 if period <= 4 else 5 * 60


def _elapsed_game_seconds(period: int | None, clock_str: str | None) -> int | None:
    if period is None:
        return None
    remaining = _parse_clock_seconds(clock_str)
    if remaining is None:
        return None
    elapsed_prev = 0
    if period > 1:
        elapsed_prev += min(period - 1, 4) * 12 * 60
        if period > 5:
            elapsed_prev += (period - 5) * 5 * 60
    return elapsed_prev + (_period_length_seconds(period) - remaining)


def _format_lineup(team_id: int, lineup: dict[int, set[int]], id_to_name: dict[int, str]) -> str:
    ids = sorted(lineup.get(team_id, set()))
    names = [id_to_name.get(pid, str(pid)) for pid in ids]
    return ", ".join(names)


def _shot_breakdown(player_id: int, st: pd.DataFrame, area: str, shot_type: str) -> dict[str, float]:
    if player_id in st.index:
        a_r = float(st.loc[player_id, "A_r"])
        m_r = float(st.loc[player_id, "M_r"])
    else:
        a_r = 0.0
        m_r = 0.0
    scale = min(a_r / SCALE_ATTEMPTS, 1.0)
    mu_base = MU_MIN + (MU_MAX - MU_MIN) * scale
    kappa = KAPPA_MIN + (KAPPA_MAX - KAPPA_MIN) * scale
    shot_mix_mult = get_shot_mix_adjustment(player_id)
    mu_adj = mu_base * shot_mix_mult
    p_hat = (m_r + kappa * mu_adj) / (a_r + kappa)
    mult = get_shot_multiplier(area, shot_type)
    exp = max(0.15, min(0.55, p_hat * mult))
    return {
        "A_r": a_r,
        "M_r": m_r,
        "mu_base": mu_base,
        "shot_mix_mult": shot_mix_mult,
        "mu_adj": mu_adj,
        "kappa": kappa,
        "p_hat": p_hat,
        "multiplier": mult,
        "exp_prob": exp,
    }


def _df_to_html_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "<p>(no rows)</p>"
    return df.to_html(index=False, classes="table", escape=True, border=0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--game", required=True, help="Game ID, with or without leading zeros")
    parser.add_argument("--date", required=True, help="MM/DD/YYYY (ET)")
    parser.add_argument(
        "--out",
        default="",
        help="Output path for HTML (default: data/game_audit_<game_id>.html)",
    )
    args = parser.parse_args()

    game_id = str(args.game)
    game_id = game_id.zfill(10) if len(game_id) < 10 else game_id
    game_id_norm = game_id.lstrip("0")
    out_path = Path(args.out) if args.out else Path("data") / f"game_audit_{game_id_norm}.html"

    actions = _sort_actions(get_playbyplay_actions(game_id, args.date))
    home_id, away_id = get_game_home_away_team_ids(game_id, args.date)
    players_df = get_boxscore_players(game_id, args.date)
    starters = get_starters_by_team(game_id, args.date)
    state_df = load_player_state(Path("data/player_state.csv"))
    st = state_df.set_index("player_id")[["A_r", "M_r"]] if not state_df.empty else pd.DataFrame()

    id_to_name = {
        int(r["PLAYER_ID"]): str(r.get("PLAYER_NAME", ""))
        for _, r in players_df.iterrows()
    }
    team_to_players = {
        int(team_id): grp.sort_values("PLAYER_NAME")[["PLAYER_ID", "PLAYER_NAME", "STARTER", "PLAYED"]]
        for team_id, grp in players_df.groupby("TEAM_ID")
    }

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    orb_rate = float(cfg["orb_rate"])
    ppp = float(cfg["ppp"])
    haircut = orb_rate * ppp
    adj_factor = 3.0 - haircut

    lineup = {
        home_id: set(starters.get(home_id, [])),
        away_id: set(starters.get(away_id, [])),
    }

    pbp_rows: list[dict[str, Any]] = []
    shot_rows: list[dict[str, Any]] = []

    prev_home = 0
    prev_away = 0
    prev_elapsed = 0

    i = 0
    while i < len(actions):
        a = actions[i]
        period = a.get("period")
        clock = a.get("clock")
        action_type = str(a.get("actionType", "")).lower()
        team_id = a.get("teamId")
        pid = a.get("personId")
        elapsed = _elapsed_game_seconds(period, clock)

        if action_type == "substitution" and team_id is not None:
            team_id_int = int(team_id)
            batch = []
            j = i
            while j < len(actions):
                b = actions[j]
                if str(b.get("actionType", "")).lower() != "substitution":
                    break
                if b.get("teamId") != team_id or b.get("period") != period or b.get("clock") != clock:
                    break
                batch.append(b)
                j += 1

            # Do not clear lineup for startperiod batches. Those batches are not
            # guaranteed to contain all five players.

            for b in batch:
                sub_type = str(b.get("subType", "")).lower()
                sub_pid = b.get("personId")
                if sub_pid is None:
                    continue
                sub_pid = int(sub_pid)
                if sub_type == "out":
                    lineup[team_id_int].discard(sub_pid)
                elif sub_type == "in":
                    lineup[team_id_int].add(sub_pid)

            i = j
            # Add one combined row for batched subs
            pbp_rows.append(
                {
                    "period": period,
                    "clock": clock,
                    "elapsed_sec": elapsed,
                    "action": "substitution_batch",
                    "team_id": team_id_int,
                    "player_id": "",
                    "player_name": "",
                    "desc": "Batched substitutions",
                    "score_home": prev_home,
                    "score_away": prev_away,
                    "home_lineup": _format_lineup(home_id, lineup, id_to_name),
                    "away_lineup": _format_lineup(away_id, lineup, id_to_name),
                }
            )
            continue

        new_home = prev_home
        new_away = prev_away
        try:
            if a.get("scoreHome") is not None:
                new_home = int(a.get("scoreHome"))
            if a.get("scoreAway") is not None:
                new_away = int(a.get("scoreAway"))
        except Exception:
            pass
        delta_home = new_home - prev_home
        delta_away = new_away - prev_away
        prev_home = new_home
        prev_away = new_away
        prev_elapsed = elapsed if elapsed is not None else prev_elapsed

        player_name = id_to_name.get(int(pid), "") if pid is not None else ""
        pbp_rows.append(
            {
                "period": period,
                "clock": clock,
                "elapsed_sec": prev_elapsed,
                "action": action_type,
                "team_id": int(team_id) if team_id is not None else "",
                "player_id": int(pid) if pid is not None else "",
                "player_name": player_name,
                "desc": a.get("description", ""),
                "delta_home": delta_home,
                "delta_away": delta_away,
                "score_home": prev_home,
                "score_away": prev_away,
                "home_lineup": _format_lineup(home_id, lineup, id_to_name),
                "away_lineup": _format_lineup(away_id, lineup, id_to_name),
            }
        )

        if action_type == "3pt" and team_id is not None and pid is not None:
            team_id_int = int(team_id)
            pid_int = int(pid)
            area = _classify_area(a)
            shot_type = _classify_shot_type(a)
            b = _shot_breakdown(pid_int, st, area, shot_type)
            actual_make = 1 if str(a.get("shotResult", "")).lower() == "made" else 0
            adj_delta = (b["exp_prob"] - actual_make) * adj_factor
            shot_rows.append(
                {
                    "period": period,
                    "clock": clock,
                    "team_id": team_id_int,
                    "shooter_id": pid_int,
                    "shooter_name": player_name,
                    "result": "MADE" if actual_make else "MISS",
                    "area": area,
                    "shot_type": shot_type,
                    "A_r": round(b["A_r"], 2),
                    "M_r": round(b["M_r"], 2),
                    "mu_base": round(b["mu_base"], 4),
                    "shot_mix_mult": round(b["shot_mix_mult"], 4),
                    "mu_adj": round(b["mu_adj"], 4),
                    "kappa": round(b["kappa"], 2),
                    "p_hat": round(b["p_hat"], 4),
                    "multiplier": round(b["multiplier"], 4),
                    "exp_prob": round(b["exp_prob"], 4),
                    "adj_delta_pts": round(adj_delta, 4),
                    "description": a.get("description", ""),
                    "home_lineup": _format_lineup(home_id, lineup, id_to_name),
                    "away_lineup": _format_lineup(away_id, lineup, id_to_name),
                }
            )

        i += 1

    pbp_df = pd.DataFrame(pbp_rows)
    shot_df = pd.DataFrame(shot_rows)

    onoff_path = Path("data/adjusted_onoff.csv")
    if onoff_path.exists():
        onoff_df = pd.read_csv(onoff_path, dtype={"game_id": str})
        game_onoff = onoff_df.loc[onoff_df["game_id"].astype(str).str.lstrip("0") == game_id_norm].copy()
        game_onoff = game_onoff.sort_values(["team_id", "player_name"])
    else:
        game_onoff = pd.DataFrame()

    players_home = team_to_players.get(home_id, pd.DataFrame())
    players_away = team_to_players.get(away_id, pd.DataFrame())

    title = f"Game Audit {game_id_norm} ({args.date})"
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --card: #ffffff;
      --ink: #1f2937;
      --muted: #6b7280;
      --line: #dbe3ef;
      --accent: #0f766e;
    }}
    body {{ margin: 0; font-family: "Segoe UI", Arial, sans-serif; color: var(--ink); background: var(--bg); }}
    .wrap {{ max-width: 1600px; margin: 0 auto; padding: 20px; }}
    h1, h2 {{ margin: 0 0 10px; }}
    p {{ margin: 4px 0; color: var(--muted); }}
    .card {{ background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 14px; margin-bottom: 16px; }}
    .meta {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 8px; }}
    .table-wrap {{ overflow: auto; border: 1px solid var(--line); border-radius: 8px; }}
    table.table {{ border-collapse: collapse; width: 100%; min-width: 1100px; font-size: 12px; }}
    table.table th, table.table td {{ border-bottom: 1px solid var(--line); padding: 6px 8px; text-align: left; vertical-align: top; white-space: nowrap; }}
    table.table th {{ position: sticky; top: 0; background: #f0f4fa; z-index: 2; }}
    .formula code {{ background: #eef2f7; padding: 2px 4px; border-radius: 4px; }}
    .accent {{ color: var(--accent); font-weight: 600; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>{html.escape(title)}</h1>
    <div class="card">
      <div class="meta">
        <div><strong>Game ID:</strong> {html.escape(game_id)} (normalized {html.escape(game_id_norm)})</div>
        <div><strong>Date (ET):</strong> {html.escape(args.date)}</div>
        <div><strong>Home Team ID:</strong> {home_id}</div>
        <div><strong>Away Team ID:</strong> {away_id}</div>
        <div><strong>ORB*PPP:</strong> {haircut:.4f}</div>
        <div><strong>3PT Adjustment Factor:</strong> <span class="accent">{adj_factor:.4f}</span></div>
      </div>
      <p class="formula">
        Formula: <code>mu_base = f(A_r)</code>,
        <code>mu_adj = mu_base * shot_mix_mult</code>,
        <code>p_hat = (M_r + kappa*mu_adj)/(A_r + kappa)</code>,
        <code>exp_prob = clamp(0.15, 0.55, p_hat*multiplier)</code>,
        <code>adj_delta_pts = (exp_prob - actual_make) * (3 - orb_rate*ppp)</code>.
      </p>
    </div>

    <div class="card">
      <h2>Per-Player Adjusted On/Off (from adjusted_onoff.csv)</h2>
      <div class="table-wrap">{_df_to_html_table(game_onoff)}</div>
    </div>

    <div class="card">
      <h2>Home Roster Snapshot</h2>
      <div class="table-wrap">{_df_to_html_table(players_home)}</div>
      <h2 style="margin-top:14px;">Away Roster Snapshot</h2>
      <div class="table-wrap">{_df_to_html_table(players_away)}</div>
    </div>

    <div class="card">
      <h2>All 3PT Shots with Expected Make and Adjustment</h2>
      <p><strong>Legend:</strong></p>
      <p>
        <strong>period/clock</strong>: game time of shot;
        <strong>team_id</strong>: shooting team;
        <strong>shooter_id/shooter_name</strong>: shooter;
        <strong>result</strong>: made or missed;
        <strong>area</strong>: <code>corner</code> or <code>above_break</code>;
        <strong>shot_type</strong>: classified shot context;
        <strong>A_r/M_r</strong>: pre-game recency-weighted 3PA/3PM state for shooter;
        <strong>mu_base</strong>: baseline prior mean from experience only (before shot-mix adjustment);
        <strong>shot_mix_mult</strong>: multiplier from historical assisted/unassisted 3PA profile;
        <strong>mu_adj</strong>: shot-mix-adjusted prior mean (<code>mu_base * shot_mix_mult</code>);
        <strong>kappa</strong>: prior strength (attempt-equivalent);
        <strong>p_hat</strong>: Bayesian baseline make probability before shot-type multiplier;
        <strong>multiplier</strong>: shot difficulty multiplier from area/type;
        <strong>exp_prob</strong>: final expected make probability for this shot;
        <strong>adj_delta_pts</strong>: points adjustment from this shot
        (<code>(exp_prob - actual_make) * (3 - orb_rate*ppp)</code>);
        <strong>description</strong>: raw play description;
        <strong>home_lineup/away_lineup</strong>: reconstructed 5-man lineups at event time.
      </p>
      <div class="table-wrap">{_df_to_html_table(shot_df)}</div>
    </div>

    <div class="card">
      <h2>Full Play-by-Play with Lineups</h2>
      <div class="table-wrap">{_df_to_html_table(pbp_df)}</div>
    </div>
  </div>
</body>
</html>"""

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_doc, encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
