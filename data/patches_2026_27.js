// ─── NBA CAP PATCHES 2026-27 ──────────────────────────────────────────────────
// Add entries here for same-day transactions (Shams tweets, breaking news).
// The Spotrac scraper will eventually bake confirmed deals into the base snapshot,
// at which point you can remove those entries from here.
//
// Supported ops:
//   trade  — move a player's existing contract to a new team
//   sign   — add a player to a team on a new contract (FA signing or extension)
//   release — remove a player from a team's cap sheet
//   update — correct a player's salary or contract type already in the snapshot
//
// Team keys: ATL BOS BKN CHA CHI CLE DAL DEN DET GSW HOU IND LAC LAL MEM MIA
//            MIL MIN NOP NYK OKC ORL PHI PHX POR SAC SAS TOR UTA WAS
// ─────────────────────────────────────────────────────────────────────────────

(function applyPatches() {
  const snap = window.NBA_CAP_SNAPSHOT;
  if (!snap || !snap.teams) return;

  const patches = [

    // ── ADD ENTRIES BELOW — newest at top ────────────────────────────────────

    // 2026-07-01
    // Ariel Hukporti (C, 24) signed with PHI — was NYK restricted FA (Early Bird, $2.45M hold)
    // ⚠ cap_hit TBD — update with confirmed AAV once known
    { op:'sign',    team:'PHI', player:'Ariel Hukporti', position:'C',  age:24, cap_hit:4000000, contract_type:'UFA' },
    { op:'release', team:'NYK', player:'Ariel Hukporti' },  // removes NYK cap hold

    // Kawhi Leonard traded to TOR — Spotrac still lists him on LAC; remove duplicate
    { op:'release', team:'LAC', player:'Kawhi Leonard' },

    // James Harden declined player option with CLE — show as FA cap hold
    // Replace with op:'sign' once new deal is confirmed
    { op:'hold',    team:'CLE', player:'James Harden', position:'SG', age:36, cap_hit:42300000, contract_type:'UFA' },

    // Marcus Smart (HOU), Thomas Bryant (CLE), Olivier Sarr (CLE) — now in Spotrac, no patch needed

  ];

  // ── Merge engine — no need to edit below this line ───────────────────────

  function findPlayer(teamKey, playerName) {
    const t = snap.teams[teamKey];
    if (!t) return null;
    for (const cat of ['active_roster', 'pending_transactions', 'cap_holds', 'dead_cap', 'retained_salaries']) {
      if (!t[cat]) continue;
      const idx = t[cat].findIndex(p => p.player === playerName);
      if (idx >= 0) return { cat, idx };
    }
    return null;
  }

  patches.forEach(p => {
    try {
      if (p.op === 'trade') {
        const src = findPlayer(p.from, p.player);
        if (!src) { console.warn('[patch] trade: not found:', p.player, 'on', p.from); return; }
        const [row] = snap.teams[p.from][src.cat].splice(src.idx, 1);
        const dest = snap.teams[p.to];
        if (dest) {
          (dest.pending_transactions = dest.pending_transactions || []).push(row);
        }

      } else if (p.op === 'hold') {
        const dest = snap.teams[p.team];
        if (!dest) { console.warn('[patch] hold: unknown team', p.team); return; }
        if (!(dest.cap_holds || []).some(h => h.player === p.player)) {
          (dest.cap_holds = dest.cap_holds || []).push({
            player: p.player, category: 'cap_hold', position: p.position || '',
            age: p.age || null, contract_type: p.contract_type || 'UFA',
            cap_hit: p.cap_hit, base_salary: p.cap_hit,
            likely_incentives: null, unlikely_incentives: null,
            trade_bonus_proration: null, guaranteed: null,
            qualifying_offer: null, rights: null,
          });
        }

      } else if (p.op === 'sign') {
        const dest = snap.teams[p.team];
        if (!dest) { console.warn('[patch] sign: unknown team', p.team); return; }
        // Remove any existing cap hold for this player on this team
        if (dest.cap_holds) dest.cap_holds = dest.cap_holds.filter(h => h.player !== p.player);
        (dest.pending_transactions = dest.pending_transactions || []).push({
          player:               p.player,
          category:             'pending_transaction',
          position:             p.position             || '',
          age:                  p.age                  || null,
          contract_type:        p.contract_type        || 'UFA',
          cap_hit:              p.cap_hit,
          base_salary:          p.cap_hit,
          likely_incentives:    p.likely_incentives    || null,
          unlikely_incentives:  null,
          trade_bonus_proration:null,
          guaranteed:           p.guaranteed           ?? p.cap_hit,
          qualifying_offer:     null,
          rights:               null,
        });

      } else if (p.op === 'release') {
        const src = findPlayer(p.team, p.player);
        if (!src) { console.warn('[patch] release: not found:', p.player, 'on', p.team); return; }
        snap.teams[p.team][src.cat].splice(src.idx, 1);

      } else if (p.op === 'update') {
        const src = findPlayer(p.team, p.player);
        if (!src) { console.warn('[patch] update: not found:', p.player, 'on', p.team); return; }
        const row = snap.teams[p.team][src.cat][src.idx];
        if (p.cap_hit       !== undefined) { row.cap_hit = p.cap_hit; row.base_salary = p.cap_hit; }
        if (p.guaranteed    !== undefined) row.guaranteed     = p.guaranteed;
        if (p.contract_type !== undefined) row.contract_type  = p.contract_type;
        if (p.position      !== undefined) row.position       = p.position;
      }
    } catch (err) {
      console.error('[patch] error applying', p, err);
    }
  });
})();
