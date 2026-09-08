"""Current return regime under a Gaussian Kalman local-level model.

Completed daily closes produce non-overlapping daily log returns. The latent
level is a random-walk mean return; observations add independent Gaussian noise.
Bull means that the CURRENT latent average return is positive, not that the next
price change will be positive. All reported states are filtered, never smoothed.

Returns are scaled by a causal exponentially weighted volatility (RiskMetrics
recursion, lambda 0.94) instead of a constant sample standard deviation. Daily
index returns are heteroskedastic and heavy-tailed; time-varying scaling absorbs
volatility clustering so the Gaussian filter sees an approximately
homoskedastic input. Fat tails beyond volatility clustering are not modeled.

The fitted level-to-observation variance ratio implies a steady-state Kalman
gain and a shock half-life, reported as regime persistence. When maximum
likelihood collapses the level variance onto the boundary (a constant-mean
fit), the filtered probability degenerates to a z-test of the sample mean and
no finite persistence exists; the result then reports no half-life instead of
an invented one. Such boundary fits remain valid under the model, and the
report labels them.

The percentile parametric-bootstrap confidence interval approximates fitted
noise-parameter uncertainty, conditional on this model. Synthetic series fix the
initial mean at the fitted first filtered mean: initial-state uncertainty is not
bootstrapped. Each synthetic history is refitted, and its parameters filter the
ORIGINAL history. This is not a calibrated trading confidence or a backtest.
"""

import warnings
from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from scipy.stats import norm
from statsmodels.tsa.statespace.structural import UnobservedComponents

EWMA_LAMBDA = 0.94
# Below this steady-state gain a fitted level is indistinguishable from constant.
MIN_KALMAN_GAIN = 1e-4


class RegimeError(ValueError):
    """Inputs or model fits cannot support a reliable regime estimate."""


@dataclass(frozen=True)
class RegimeResult:
    bull_probability: float
    bear_probability: float
    bull_probability_interval: Tuple[float, float]
    bull_bear_ratio: float
    ratio_interval: Tuple[float, float]
    bootstrap_successes: int
    bootstrap_samples: int
    estimated_mean_return: float
    mean_return_interval: Tuple[float, float]
    signal_noise_ratio: float
    kalman_gain: float
    regime_half_life: Optional[float]


def _model(values):
    return UnobservedComponents(values, level="local level")


def _fit(values, start_params=None):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = _model(values).fit(
            start_params=start_params, disp=False, maxiter=100
        )
    if not result.mle_retvals.get("converged", False):
        raise RegimeError("Kalman model did not converge.")
    params = np.asarray(result.params, dtype=float)
    if params.shape != (2,) or not np.isfinite(params).all() or (params < 0).any():
        raise RegimeError("Kalman model returned invalid noise variances.")
    return result


def _state(result):
    mean = float(result.filtered_state[0, -1])
    variance = float(result.filtered_state_cov[0, 0, -1])
    if not np.isfinite(mean) or not np.isfinite(variance) or variance <= 0:
        raise RegimeError("Kalman model returned an invalid filtered state.")
    return mean, float(np.sqrt(variance))


def _ewma_volatility(returns, lam=EWMA_LAMBDA):
    """Causal per-observation volatility for scaling, without demeaning.

    The variance for observation t is the RiskMetrics recursion over squared
    returns up to t-1. The recursion is seeded with the mean squared return of
    the first at most 20 observations; the seed only scales the earliest
    warmup observations and does not affect the filtered state at the end of
    the history. Volatility is strictly positive or the fit is rejected.
    """
    if not 0.0 < lam < 1.0:
        raise RegimeError("EWMA decay must be between 0 and 1.")
    values = np.asarray(returns, dtype=float)
    variance = np.empty(len(values))
    seed = float(np.mean(values[: min(20, len(values))] ** 2))
    if not np.isfinite(seed) or seed <= 0:
        raise RegimeError("Daily returns have insufficient variation to scale.")
    variance[0] = seed
    for t in range(1, len(values)):
        variance[t] = lam * variance[t - 1] + (1.0 - lam) * values[t - 1] ** 2
    volatility = np.sqrt(variance)
    if not np.isfinite(volatility).all() or (volatility <= 0).any():
        raise RegimeError("Daily returns cannot support a volatility scale.")
    return volatility


def _persistence(observation_variance, level_variance):
    """Steady-state Kalman gain, signal ratio, and shock half-life in sessions.

    For the local-level model the Riccati equation has the closed-form steady
    state K = (sqrt(q^2 + 4q) - q) / 2 with q the level-to-observation variance
    ratio. Between observations a level shock decays by (1 - K), so its
    half-life is ln(2) / -ln(1 - K) observed sessions. Gains at or below
    MIN_KALMAN_GAIN mean maximum likelihood collapsed the level variance onto
    the boundary: the level is estimated as constant, no finite half-life
    exists, and None is returned for it.
    """
    ratio = float(level_variance) / float(observation_variance)
    if not np.isfinite(ratio) or ratio < 0:
        raise RegimeError("Kalman model returned an invalid variance ratio.")
    gain = float(np.clip(0.5 * (np.sqrt(ratio * ratio + 4.0 * ratio) - ratio), 0.0, 1.0))
    if gain < MIN_KALMAN_GAIN:
        return ratio, gain, None
    if gain >= 1.0:
        return ratio, 1.0, 0.0
    half_life = float(np.log(2.0) / -np.log1p(-gain))
    return ratio, gain, half_life


def _probability(mean, sd):
    # Numerical clipping keeps odds finite when the Gaussian CDF rounds to 0/1.
    return float(np.clip(norm.cdf(mean / sd), 1e-12, 1.0 - 1e-12))


def _odds(probability):
    return float(probability / (1.0 - probability))


def analyze_regime(closes: pd.Series, *, bootstrap_samples=100, seed=1729) -> RegimeResult:
    """Analyze the latest mean daily log return from ordered completed closes.

    The caller supplies a monotonic, unique daily DatetimeIndex and handles
    exchange-local completed-bar semantics and corporate-action adjustments.
    Missing prices are rejected, never filled; absent exchange holidays are fine.
    At least 60 closes are required. Returns use consecutive observed sessions;
    exchange-calendar completeness is not verified. An absent trading session
    therefore creates a multi-session return; no calendar-time normalization is
    applied. Intraday observations are not supported.
    No historical regime track is produced: parameters use the full supplied
    history and are appropriate only for its latest timestamp.
    """
    if not isinstance(closes, pd.Series) or len(closes) < 60:
        raise RegimeError("At least 60 daily closing prices are required.")
    if not isinstance(closes.index, pd.DatetimeIndex):
        raise RegimeError("Closing prices require a DatetimeIndex.")
    if closes.index.hasnans or not closes.index.is_unique or not closes.index.is_monotonic_increasing:
        raise RegimeError("Closing timestamps must be valid, unique and increasing.")
    if not closes.index.normalize().is_unique:
        raise RegimeError("Only one completed close per day is supported.")
    if isinstance(bootstrap_samples, bool) or not isinstance(bootstrap_samples, int) or not 10 <= bootstrap_samples <= 500:
        raise RegimeError("bootstrap_samples must be an integer between 10 and 500.")
    if any(isinstance(value, (bool, np.bool_)) for value in closes):
        raise RegimeError("Closing prices must be numeric, not boolean.")
    try:
        prices = closes.to_numpy(dtype=float)
    except (ValueError, TypeError) as exc:
        raise RegimeError("Closing prices must be numeric.") from exc
    if not np.isfinite(prices).all() or (prices <= 0).any():
        raise RegimeError("Closing prices must be finite and strictly positive.")
    returns = np.diff(np.log(prices))
    scale = _ewma_volatility(returns)
    values = returns / scale  # Do not demean: zero is the bull/bear threshold.
    try:
        fitted = _fit(values)
        mean, sd = _state(fitted)
    except (ValueError, np.linalg.LinAlgError, FloatingPointError) as exc:
        raise RegimeError("Could not reliably fit the return regime: {}".format(exc)) from exc
    observation_variance, level_variance = np.asarray(fitted.params, dtype=float)
    ratio, gain, half_life = _persistence(observation_variance, level_variance)
    probability = _probability(mean, sd)
    initial_mean = float(fitted.filtered_state[0, 0])
    rng = np.random.default_rng(seed)
    probabilities = []
    for _ in range(bootstrap_samples):
        latent = initial_mean + np.concatenate((
            [0.0], np.cumsum(rng.normal(0, np.sqrt(level_variance), len(values) - 1))
        ))
        synthetic = latent + rng.normal(0, np.sqrt(observation_variance), len(values))
        try:
            bootstrap_fit = _fit(synthetic, start_params=fitted.params)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                refiltered = _model(values).filter(bootstrap_fit.params)
            probabilities.append(_probability(*_state(refiltered)))
        except (ValueError, np.linalg.LinAlgError, FloatingPointError):
            continue
    if len(probabilities) < int(np.ceil(0.8 * bootstrap_samples)):
        raise RegimeError("Too few bootstrap fits converged ({}/{}); confidence interval unavailable.".format(
            len(probabilities), bootstrap_samples
        ))
    bounds = tuple(float(x) for x in np.percentile(probabilities, [2.5, 97.5]))
    current_scale = float(scale[-1])
    return RegimeResult(
        bull_probability=probability,
        bear_probability=1.0 - probability,
        bull_probability_interval=bounds,
        bull_bear_ratio=_odds(probability),
        ratio_interval=tuple(_odds(x) for x in bounds),
        bootstrap_successes=len(probabilities),
        bootstrap_samples=bootstrap_samples,
        estimated_mean_return=mean * current_scale,
        mean_return_interval=((mean - 1.96 * sd) * current_scale, (mean + 1.96 * sd) * current_scale),
        signal_noise_ratio=ratio,
        kalman_gain=gain,
        regime_half_life=half_life,
    )
