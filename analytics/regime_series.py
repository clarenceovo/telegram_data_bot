"""Causal historical bull-probability series for downstream model features.

``analyze_regime`` describes only the latest session. This module produces the
SAME kind of local-level bull probability for every historical session, using
only closes up to that session: expanding-window maximum-likelihood refits at a
fixed cadence, with a manual Kalman recursion carrying the last fitted
parameters between refits. A failed refit keeps the previous parameters; if no
fit has succeeded yet the probability stays NaN.

Probabilities are model states, not tradeable signals. Refits use the same
EWMA-volatility scaling convention as ``analytics.regime`` computed on the
expanding window available at each refit.
"""

import numpy as np
import pandas as pd

from analytics.regime import EWMA_LAMBDA, RegimeError, _ewma_volatility, _fit, _probability


def regime_probability_series(closes: pd.Series, *, refit_every=21,
                              min_closes=60, lam=EWMA_LAMBDA) -> np.ndarray:
    """Return one bull probability per close, using only information up to it.

    ``refit_every`` is the cadence of expanding-window MLE refits in observed
    sessions (between refits, the previous parameters drive an exact Kalman
    update). ``min_closes`` is the shortest history that supports the first
    fit. The returned array has the length of ``closes``; entries before the
    first successful fit are NaN.
    """
    if isinstance(refit_every, bool) or not isinstance(refit_every, int) or not 1 <= refit_every <= 252:
        raise RegimeError("refit_every must be an integer between 1 and 252.")
    if isinstance(min_closes, bool) or not isinstance(min_closes, int) or min_closes < 2:
        raise RegimeError("min_closes must be an integer of at least 2.")
    if not isinstance(closes, pd.Series) or len(closes) < min_closes + 1:
        raise RegimeError("At least {} daily closing prices are required.".format(min_closes + 1))
    if not isinstance(closes.index, pd.DatetimeIndex):
        raise RegimeError("Closing prices require a DatetimeIndex.")
    if closes.index.hasnans or not closes.index.is_unique or not closes.index.is_monotonic_increasing:
        raise RegimeError("Closing timestamps must be valid, unique and increasing.")
    if any(isinstance(value, (bool, np.bool_)) for value in closes):
        raise RegimeError("Closing prices must be numeric, not boolean.")
    try:
        prices = closes.to_numpy(dtype=float)
    except (ValueError, TypeError) as exc:
        raise RegimeError("Closing prices must be numeric.") from exc
    if not np.isfinite(prices).all() or (prices <= 0).any():
        raise RegimeError("Closing prices must be finite and strictly positive.")
    returns = np.diff(np.log(prices))
    if not np.isfinite(returns).all() or float(np.std(returns, ddof=1)) < 1e-12:
        raise RegimeError("Daily returns have insufficient variation for a regime series.")
    values = returns / _ewma_volatility(returns, lam)
    probabilities = np.full(len(closes), np.nan)
    parameters = None
    mean = None
    variance = None
    next_refit = min_closes
    for session in range(min_closes, len(closes)):
        refitted = False
        if session >= next_refit:
            next_refit = session + refit_every
            try:
                fitted = _fit(values[:session])
                parameters = np.asarray(fitted.params, dtype=float)
                mean = float(fitted.filtered_state[0, -1])
                variance = float(fitted.filtered_state_cov[0, 0, -1])
                refitted = True
            except (ValueError, np.linalg.LinAlgError, FloatingPointError, RegimeError):
                pass  # Keep previous parameters; the manual update continues.
        if parameters is None or mean is None or variance is None:
            continue
        if not refitted:
            observation_variance, level_variance = parameters
            predicted_variance = variance + level_variance
            gain = predicted_variance / (predicted_variance + observation_variance)
            mean = mean + gain * (values[session - 1] - mean)
            variance = (1.0 - gain) * predicted_variance
        probabilities[session] = _probability(mean, float(np.sqrt(variance)))
    return probabilities
