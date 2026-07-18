# Atomic foul-generation decomposition

## Result

The useful information is other foul pressure, not free-throw-producing foul
trips.

| Offensive candidate | Pre-2019 selection score | 2019-25 same | 2019-25 next |
|---|---:|---:|---:|
| Atomic baseline | 0.56032 | 0.5580 | 0.4670 |
| FT foul trips | 0.56025 | — | — |
| Total fouls drawn | 0.56051 | — | — |
| Other fouls drawn | **0.56055** | **0.5595** | **0.4708** |

FT foul trips add no value beyond free throws made and missed. Other fouls
capture nonshooting, pre-shot, off-ball, rebounding, bonus and opponent-foul
pressure. Their current coefficient is +0.185 points per training standard
deviation.

The gain is modest and was found after total fouls drawn had already been
examined on the same confirmation years. The candidate is therefore retained
but not promoted.

## Data audit

- Nontechnical foul-trip sets / FTA: approximately 0.53 every season.
- Total fouls drawn: unreliable through 2004; plausible from 2005 onward.
- Negative cross-source residuals: marked missing, never clipped to zero.
- Current players below 200 possessions: maximum prior change 0.21.

The default denominator-aware atomic prior remains unchanged.
