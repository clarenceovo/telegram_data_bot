"""Current return regime under a Gaussian Kalman local-level model.

Completed daily closes produce non-overlapping daily log returns. The latent
level is a random-walk mean return; observations add independent Gaussian noise.
Bull means that the CURRENT latent average return is positive, not that the next
price change will be positive. All reported states are filtered, never smoothed.

The percentile parametric-bootstrap confidence interval approximates fitted
noise-parameter uncertainty, conditional on this model. Synthetic series fix the
initial mean at the fitted first filtered mean: initial-state uncertainty is not
bootstrapped. Each synthetic history is refitted, and its parameters filter the
ORIGINAL history. This is not a calibrated trading confidence or a backtest.
"""

import warnings
from dataclasses import dataclass
from typing import Tuple

import numpy as np
import pandas as pd
from scipy.stats import norm
from statsmodels.tsa.statespace.structural import UnobservedComponents


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
    scale = float(np.std(returns, ddof=1))
    if not np.isfinite(scale) or scale < 1e-12:
        raise RegimeError("Daily returns have insufficient variation to fit a regime.")
    values = returns / scale  # Do not demean: zero is the bull/bear threshold.
    try:
        fitted = _fit(values)
        mean, sd = _state(fitted)
    except (ValueError, np.linalg.LinAlgError, FloatingPointError) as exc:
        raise RegimeError("Could not reliably fit the return regime: {}".format(exc)) from exc
    probability = _probability(mean, sd)
    observation_variance, level_variance = np.asarray(fitted.params, dtype=float)
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
    return RegimeResult(
        bull_probability=probability,
        bear_probability=1.0 - probability,
        bull_probability_interval=bounds,
        bull_bear_ratio=_odds(probability),
        ratio_interval=tuple(_odds(x) for x in bounds),
        bootstrap_successes=len(probabilities),
        bootstrap_samples=bootstrap_samples,
        estimated_mean_return=mean * scale,
        mean_return_interval=((mean - 1.96 * sd) * scale, (mean + 1.96 * sd) * scale),
    )
