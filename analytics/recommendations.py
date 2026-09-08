"""Experimental long-only daily recommendations, evaluated strictly in time order.

Decision after session t; proxy entry at t+1 close and exit at t+h+1 close.
Returns are net of a fixed round-trip cost. Completed, consistently adjusted exchange-local
index closes are the caller's responsibility. No fills or calendar checks
are inferred here. Forecast probabilities are not claimed to be calibrated.
"""

from numbers import Real

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.special import expit

MODEL_VERSION = "daily-logistic-v1"


def _features(prices):
    """Natural-log return features available at each decision close."""
    returns = pd.Series(np.diff(np.log(prices), prepend=np.nan))
    return np.column_stack([
        returns,
        returns.rolling(5).mean(),
        returns.rolling(20).mean(),
        returns.rolling(60).mean(),
        returns.rolling(20).std(ddof=1),
        returns.rolling(60).std(ddof=1),
    ])


def _fit_probability(train_x, train_y, current_x):
    mean = train_x.mean(axis=0)
    scale = train_x.std(axis=0)
    scale[scale < 1e-12] = 1.0
    x = np.column_stack([np.ones(len(train_x)), (train_x - mean) / scale])
    current = np.r_[1.0, (current_x - mean) / scale]
    # Fixed L2 coefficient, applied to slopes only; never tuned on test data.
    def loss(beta):
        z = x @ beta
        objective = np.mean(np.logaddexp(0, z) - train_y * z) + 0.5 * np.sum(beta[1:] ** 2)
        gradient = x.T @ (expit(z) - train_y) / len(x)
        gradient[1:] += beta[1:]
        return objective, gradient
    if np.all(train_y == train_y[0]):
        # Finite intercept-only estimate when a training window has one class.
        return float((train_y.sum() + 1) / (len(train_y) + 2))
    fit = minimize(loss, np.zeros(x.shape[1]), jac=True, method="L-BFGS-B", options={"maxiter": 100})
    if not fit.success or not np.isfinite(fit.x).all():
        raise ValueError("Logistic fit failed to converge.")
    return float(expit(current @ fit.x))


def _mean_interval(returns):
    """Approximate circular moving-block percentile interval; fixed seed."""
    if len(returns) < 2:
        return None
    rng = np.random.default_rng(1729)
    n = len(returns)
    block = min(3, n)
    starts = rng.integers(0, n, size=(1000, (n + block - 1) // block))
    indices = ((starts[..., None] + np.arange(block)) % n).reshape(1000, -1)[:, :n]
    return np.quantile(returns[indices].mean(axis=1), [0.025, 0.975]).tolist()


def evaluate_recommendation(closes: pd.Series, *, cost_bps=35.0, horizon=5,
                            min_train=252, min_trades=30, probability_threshold=0.6) -> dict:
    """Return a candidate only when every predefined historical gate passes.

    Training uses at most 504 matured labeled observations and training-only
    standardization. Test labels never participate in their own prediction.
    Missing values are rejected, not filled. Features need 60 return warmups.
    Index returns minus assumed costs are research proxies, not executable PnL.
    Selection uses nonoverlapping trades (a new entry must follow prior exit).
    The mean-return interval assumes local dependence captured by three trades;
    it is descriptive evidence, not a guarantee or calibrated success bound.
    """
    if not isinstance(closes, pd.Series) or closes.empty:
        raise ValueError("Provide at least one completed daily close.")
    index = closes.index
    if not isinstance(index, pd.DatetimeIndex) or index.tz is None or index.hasnans:
        raise ValueError("Daily closes require valid timezone-aware timestamps.")
    local = index
    if not index.is_unique or not index.is_monotonic_increasing or not local.normalize().is_unique:
        raise ValueError("Closes must have unique increasing exchange-local session dates.")
    if any(isinstance(v, (bool, np.bool_)) for v in closes):
        raise ValueError("Closing prices must be finite positive numbers.")
    try:
        prices = np.asarray(closes, dtype=float)
    except (TypeError, ValueError) as exc:
        raise ValueError("Closing prices must be numeric.") from exc
    if not np.isfinite(prices).all() or (prices <= 0).any():
        raise ValueError("Closing prices must be finite and positive.")
    for name, value, minimum in [("horizon", horizon, 1), ("min_train", min_train, 2), ("min_trades", min_trades, 2)]:
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ValueError(f"{name} must be an integer >= {minimum}.")
    if min_train > 504:
        raise ValueError("min_train cannot exceed the 504-observation training window.")
    if isinstance(cost_bps, (bool, np.bool_)) or not isinstance(cost_bps, Real) or not np.isfinite(cost_bps) or cost_bps < 0 or cost_bps >= 10000:
        raise ValueError("cost_bps must be finite and in [0, 10000).")
    if isinstance(probability_threshold, (bool, np.bool_)) or not isinstance(probability_threshold, Real) or not np.isfinite(probability_threshold) or not 0.5 <= probability_threshold < 1:
        raise ValueError("probability_threshold must be in [0.5, 1).")
    n = len(prices)
    x = _features(prices)
    # labels[j] references only entry j+1 and exit j+horizon+1.
    net = np.full(n, np.nan)
    stop = n - horizon - 1
    if stop > 0:
        with np.errstate(over="ignore", invalid="ignore"):
            net[:stop] = prices[horizon + 1:] / prices[1:n - horizon] - 1 - cost_bps / 10000
        if not np.isfinite(net[:stop]).all():
            raise ValueError("Price ratios overflowed; verify the input price scale.")
    predictions = []
    selected = []
    last_exit = -1
    latest_probability = None
    latest_train = None
    for decision in range(60 + min_train + horizon, n):
        last_mature = decision - horizon - 1
        train = np.arange(max(60, last_mature - 503), last_mature + 1)
        if len(train) < min_train:
            continue
        probability = _fit_probability(x[train], (net[train] > 0).astype(float), x[decision])
        latest_probability = probability
        latest_train = train
        if decision + horizon + 1 >= n:
            continue
        if net[decision] <= -1:
            raise ValueError("Scored net research return is at or below -100%; verify prices and costs.")
        baseline = float(np.mean(net[train] > 0))
        predictions.append((decision, probability, baseline, float(net[decision] > 0)))
        if probability >= probability_threshold and decision + 1 > last_exit:
            selected.append(float(net[decision]))
            last_exit = decision + horizon + 1
    values = np.asarray(selected)
    ci = _mean_interval(values)
    model_brier = float(np.mean([(p - y) ** 2 for _, p, _, y in predictions])) if predictions else None
    baseline_brier = float(np.mean([(b - y) ** 2 for _, _, b, y in predictions])) if predictions else None
    # Sequential all-equity research returns, sampled only at trade closes.
    # Intratrade drawdowns are not measured. Log equity avoids overflow.
    log_equity = np.r_[0.0, np.cumsum(np.log1p(values))]
    log_peaks = np.maximum.accumulate(log_equity)
    drawdown = float(np.min(np.expm1(log_equity - log_peaks))) if len(values) else None
    losses = -values[values < 0].sum()
    metrics = {
        "trade_count": len(values), "hit_rate": float(np.mean(values > 0)) if len(values) else None,
        "mean_net_return": float(values.mean()) if len(values) else None,
        "mean_net_ci": ci, "profit_factor": float(values[values > 0].sum() / losses) if losses > 0 else None,
        "max_drawdown": drawdown, "coverage": len(values) / len(predictions) if predictions else 0.0,
        "model_brier": model_brier, "baseline_brier": baseline_brier,
        "train_start": index[latest_train[0]].isoformat() if latest_train is not None else None,
        "train_end": index[latest_train[-1]].isoformat() if latest_train is not None else None,
        "latest_train_label_maturity": index[-1].isoformat() if latest_train is not None else None,
        "oos_start": index[predictions[0][0]].isoformat() if predictions else None,
        "oos_end": index[predictions[-1][0]].isoformat() if predictions else None,
        "cost_bps": float(cost_bps), "horizon": horizon, "observation_count": n,
        "evaluated_prediction_count": len(predictions), "probability_threshold": float(probability_threshold),
    }
    reasons = []
    if latest_probability is None:
        reasons.append("Insufficient matured training observations after the 60-session feature warmup.")
    elif latest_probability < probability_threshold:
        reasons.append("Current predicted probability is below the selection threshold.")
    if len(values) < min_trades:
        reasons.append(f"Only {len(values)} completed nonoverlapping selected trades; require {min_trades}.")
    if ci is None or ci[0] <= 0:
        reasons.append("Approximate 95% block-bootstrap mean net return lower bound is not positive.")
    if model_brier is None or model_brier >= baseline_brier:
        reasons.append("Out-of-sample Brier score does not improve on the training-frequency baseline.")
    status = "candidate" if not reasons else ("insufficient_data" if latest_probability is None or len(values) < min_trades else "no_trade")
    return {"status": status, "reasons": reasons, "probability": latest_probability,
            "data_asof": index[-1].isoformat(), "metrics": metrics, "model_version": MODEL_VERSION}
