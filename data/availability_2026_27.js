// Hand-maintained games-played projections for 2026-27, used by the
// win-projections page's "Injury-adjusted" view. Keys are exact player
// names as they appear in the cap data (Spotrac spelling); values are
// GAMES EXPECTED out of 82. Anyone not listed is assumed healthy (82).
//
// Model: for the games a player misses, his minutes are filled at
// replacement level. Edit freely -- these seed values are rough guesses
// for the known long-term injuries; tune or add players as news breaks.
window.AVAILABILITY = {
  "Donte DiVincenzo": 10,   // torn Achilles Apr 2026 -- out most/all season
  "Moses Moody": 52,        // patellar tendon -- expected to miss the start
  "Johnny Furphy": 55,      // ACL -- expected to miss the start
};

// Team Power Ratings: how many more games a player will miss. Normally this is
// automatic -- ESPN's estimated return date, else the injury report's wording
// (fracture/tear/surgery 30, sprain/strain 6, soreness/illness 2, ACL/Achilles
// rest of season). Add a player here to override it:
//   "season"                         out for the rest of the season
//   {games: 40, from: "2026-10-29"}  40 games starting that date; counts down
//                                    as his team plays, then drops off
//   {until: "2026-12-21"}            expected back that date
//   0                                available now (listed out but actually back)
// Anything but 0 also counts him out tonight. Same name spelling as the depth charts.
window.GAMES_OUT = {
  "Jalen Williams": "season",  // ankle fracture (set 2026-10-29)
  "Cason Wallace": "season",   // shoulder labral tear (set 2026-10-29)
  "Anthony Edwards": {games: 40, from: "2026-10-29"},  // knee surgery (set 2026-10-29)
};

// Older true/false switch for "out tonight" (GAMES_OUT above wins when a
// player is in both): true = out now, false = available.
window.OUT_NOW = {
  // "Player Name": true,
};
