"""
Bayesian Beta-Binomial 3PT shooting expectation model.

Given a player's FT%, their observed shot counts (optionally with recency
weighting and C&S / pull-up split), returns a posterior distribution over
their true 3PT%.

Usage:
    from shooter_model.model import ThreePTModel

    model = ThreePTModel.from_params()

    result = model.estimate(
        ft_pct=0.850,
        cs_attempts=120.0,   # can be fractional (recency-weighted)
        cs_makes=44.0,
        pu_attempts=30.0,
        pu_makes=9.0,
    )
    print(result)
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
from scipy import stats

PARAMS_PATH = Path(__file__).parent / "params.json"

# Clip FT% predictions to a sane 3PT% range
_3PT_LO, _3PT_HI = 0.05, 0.65


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ContextEstimate:
    """Posterior summary for a single shot context (overall / C&S / pull-up)."""
    context: str
    prior_mean: float          # FT%-based prior mean
    kappa: float               # effective prior sample size
    obs_attempts: float        # observed (possibly weighted) attempts
    obs_makes: float           # observed (possibly weighted) makes
    posterior_alpha: float
    posterior_beta: float

    @property
    def posterior_mean(self) -> float:
        return self.posterior_alpha / (self.posterior_alpha + self.posterior_beta)

    @property
    def posterior_median(self) -> float:
        return float(stats.beta.ppf(0.5, self.posterior_alpha, self.posterior_beta))

    def credible_interval(self, level: float = 0.90) -> tuple[float, float]:
        lo = (1 - level) / 2
        hi = 1 - lo
        a, b = self.posterior_alpha, self.posterior_beta
        return (float(stats.beta.ppf(lo, a, b)), float(stats.beta.ppf(hi, a, b)))

    def __str__(self) -> str:
        lo, hi = self.credible_interval(0.90)
        return (
            f"{self.context:10s}  prior={self.prior_mean:.3f}  "
            f"obs={self.obs_makes:.1f}/{self.obs_attempts:.1f}  "
            f"posterior={self.posterior_mean:.3f}  "
            f"90% CI [{lo:.3f}, {hi:.3f}]"
        )


@dataclass
class EstimateResult:
    """Full output of a player 3PT estimation."""
    overall: ContextEstimate
    catch_shoot: Optional[ContextEstimate] = None
    pullup: Optional[ContextEstimate] = None
    ft_pct: float = 0.0

    def __str__(self) -> str:
        lines = [
            f"FT%: {self.ft_pct:.3f}",
            str(self.overall),
        ]
        if self.catch_shoot is not None:
            lines.append(str(self.catch_shoot))
        if self.pullup is not None:
            lines.append(str(self.pullup))
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Recency weighting helpers
# ---------------------------------------------------------------------------

def season_weights(seasons: list[str], current_season: Optional[str] = None,
                   half_life: float = 2.0) -> list[float]:
    """
    Return exponential decay weights for a list of NBA seasons.

    Parameters
    ----------
    seasons : list of "YYYY-YY" strings
    current_season : reference season (defaults to the latest in the list)
    half_life : half-life in seasons (default 2.0 -> season 2 ago gets 0.5 weight)

    Returns
    -------
    list of float weights summing to... well, not constrained; they are
    used as multipliers on attempt counts.
    """
    def _season_year(s: str) -> float:
        return float(s[:4]) + 0.5  # midpoint of the season

    if current_season is None:
        current_season = max(seasons, key=_season_year)

    ref = _season_year(current_season)
    decay = math.log(2) / half_life
    return [math.exp(-decay * max(0.0, ref - _season_year(s))) for s in seasons]


# ---------------------------------------------------------------------------
# Main model class
# ---------------------------------------------------------------------------

class ThreePTModel:
    """
    Bayesian Beta-Binomial 3PT expectation model.

    The prior for each context is:
        Beta(mu * kappa,  (1 - mu) * kappa)
    where:
        mu    = intercept + slope * ft_pct   (from calibrated regression)
        kappa = effective prior sample size   (from Beta-Binomial MLE on historical data)

    After observing `n` weighted attempts with `k` weighted makes the posterior is:
        Beta(mu * kappa + k,  (1 - mu) * kappa + (n - k))
    """

    def __init__(self, params: dict) -> None:
        self._p = params

    @classmethod
    def from_params(cls, path: Path = PARAMS_PATH) -> "ThreePTModel":
        if not path.exists():
            raise FileNotFoundError(
                f"{path} not found — run calibrate.py first."
            )
        with open(path) as f:
            params = json.load(f)
        return cls(params)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _prior_mean(self, ft_pct: float, context: str = "overall") -> float:
        """Compute the prior 3PT% mean for a given FT% and context."""
        reg = self._p[context]
        mu = reg["intercept"] + reg["slope"] * ft_pct
        return float(np.clip(mu, _3PT_LO, _3PT_HI))

    def _prior_kappa(self, context: str = "overall") -> float:
        return float(self._p[context]["kappa"])

    def _make_estimate(
        self,
        context: str,
        ft_pct: float,
        obs_attempts: float,
        obs_makes: float,
    ) -> ContextEstimate:
        mu = self._prior_mean(ft_pct, context)
        kappa = self._prior_kappa(context)
        alpha0 = mu * kappa
        beta0 = (1.0 - mu) * kappa
        alpha_post = alpha0 + obs_makes
        beta_post = beta0 + max(0.0, obs_attempts - obs_makes)
        return ContextEstimate(
            context=context,
            prior_mean=mu,
            kappa=kappa,
            obs_attempts=obs_attempts,
            obs_makes=obs_makes,
            posterior_alpha=alpha_post,
            posterior_beta=beta_post,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def estimate(
        self,
        ft_pct: float,
        *,
        # Overall (fallback if no C&S/PU split)
        total_attempts: float = 0.0,
        total_makes: float = 0.0,
        # Split by context
        cs_attempts: float = 0.0,
        cs_makes: float = 0.0,
        pu_attempts: float = 0.0,
        pu_makes: float = 0.0,
        # Threshold to produce a context estimate
        min_context_attempts: float = 10.0,
    ) -> EstimateResult:
        """
        Compute a posterior 3PT% estimate.

        Pass either (total_attempts, total_makes) for a context-agnostic estimate,
        or (cs_attempts, cs_makes, pu_attempts, pu_makes) for split context estimates.
        You can pass both; the overall estimate always uses totals.

        All attempt/make counts can be fractional (recency-weighted sums).
        """
        # Determine total observed counts
        if total_attempts == 0.0 and (cs_attempts + pu_attempts) > 0:
            total_attempts = cs_attempts + pu_attempts
            total_makes = cs_makes + pu_makes

        overall_est = self._make_estimate("overall", ft_pct, total_attempts, total_makes)

        cs_est = None
        if cs_attempts >= min_context_attempts:
            cs_est = self._make_estimate("catch_shoot", ft_pct, cs_attempts, cs_makes)

        pu_est = None
        if pu_attempts >= min_context_attempts:
            pu_est = self._make_estimate("pullup", ft_pct, pu_attempts, pu_makes)

        return EstimateResult(
            overall=overall_est,
            catch_shoot=cs_est,
            pullup=pu_est,
            ft_pct=ft_pct,
        )

    def estimate_from_season_rows(
        self,
        rows: list[dict],
        ft_pct: float,
        half_life: float = 2.0,
        current_season: Optional[str] = None,
    ) -> EstimateResult:
        """
        Estimate from a list of season-level dicts, applying recency weighting.

        Each dict should have keys:
            SEASON          : "YYYY-YY"
            FG3A, FG3M      : total 3PA/3PM
            CATCH_SHOOT_FG3A, CATCH_SHOOT_FG3M  (optional)
            PULLUP_FG3A, PULLUP_FG3M             (optional)

        Returns an EstimateResult with weighted counts.
        """
        if not rows:
            return self.estimate(ft_pct)

        seasons = [r["SEASON"] for r in rows]
        weights = season_weights(seasons, current_season, half_life)

        total_a = total_m = 0.0
        cs_a = cs_m = pu_a = pu_m = 0.0

        for row, w in zip(rows, weights):
            total_a += w * float(row.get("FG3A", 0) or 0)
            total_m += w * float(row.get("FG3M", 0) or 0)
            cs_a += w * float(row.get("CATCH_SHOOT_FG3A", 0) or 0)
            cs_m += w * float(row.get("CATCH_SHOOT_FG3M", 0) or 0)
            pu_a += w * float(row.get("PULLUP_FG3A", 0) or 0)
            pu_m += w * float(row.get("PULLUP_FG3M", 0) or 0)

        return self.estimate(
            ft_pct=ft_pct,
            total_attempts=total_a,
            total_makes=total_m,
            cs_attempts=cs_a,
            cs_makes=cs_m,
            pu_attempts=pu_a,
            pu_makes=pu_m,
        )
