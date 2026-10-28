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

// Who is out RIGHT NOW, for the Team Power Ratings page's "Current strength".
// By default a projected-rotation player counts as out once he has missed his
// team's last 3 games in a row (until he plays again). List a player here to
// override that: true = out now (e.g. just injured, out for weeks), false =
// available (e.g. missed games but is back tonight). Same name spelling as the
// depth charts.
window.OUT_NOW = {
  // "Player Name": true,
};
