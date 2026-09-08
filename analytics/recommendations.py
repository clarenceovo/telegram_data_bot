"""Experimental long-only daily recommendations, evaluated strictly in time order.

Decision after session t; proxy entry at t+1 close and exit at t+h+1 close.
Returns are net of a fixed round-trip cost. Completed, consistently adjusted exchange-local
index closes are the caller's responsibility. No fills or calendar checks
are inferred here. Forecast probabilities are not claimed to be calibrated.

Besides the base close-return features, the model optionally consumes OHLC
range-based volatilities (Parkinson and Garman-Klass, which dominate
close-to-close variance estimators in efficiency), lag-free-as-of cross-index
returns (each other index contributes its latest COMPLETED session observed
strictly before this index's decision timestamp), and a causal local-level
regime bull probability from ``analytics.regime_series``. Cross-index features
align by timestamp, not calendar date, so an HSI decision never sees a US close
from the same calendar day and a US decision does see that day's HSI close.
"""

from numbers import Real

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.special import expit

MODEL_VERSION = "daily-logistic-v2"


def _features(prices, extra=None):
    """Natural-log return features available at each decision close."""
    returns = pd.Series(np.diff(np.log(prices), prepend=np.nan))
    columns = [
        returns,
        returns.rolling(5).mean(),
        returns.rolling(20).mean(),
        returns.rolling(60).mean(),
        returns.rolling(20).std(ddof=1),
        returns.rolling(60).std(ddof=1),
    ]
    if extra:
        columns.extend(extra)
    return np.column_stack(columns)


def _range_features(frame, prices):
    """Parkinson and Garman-Klass volatility features from OHLC bars."""
    high = frame["high"].to_numpy(dtype=float)
    low = frame["low"].to_numpy(dtype=float)
    open_ = frame["open"].to_numpy(dtype=float)
    hl = np.log(high / low)
    co = np.log(prices / open_)
    parkinson = pd.Series(hl ** 2 / (4.0 * np.log(2.0)))
    garman_klass = pd.Series(0.5 * hl ** 2 - (2.0 * np.log(2.0) - 1.0) * co ** 2)
    return [
        np.sqrt(np.maximum(parkinson.rolling(20).mean(), 0.0)),
        np.sqrt(np.maximum(parkinson.rolling(60).mean(), 0.0)),
        np.sqrt(np.maximum(garman_klass.rolling(20).mean(), 0.0)),
        np.sqrt(np.maximum(garman_klass.rolling(60).mean(), 0.0)),
    ]


def _aligned_other_features(name, other, index):
    """Other-index 1- and 5-session returns as of each decision timestamp.

    ``merge_asof`` backward on UTC timestamps picks the latest other-index
    observation strictly at or before this index's session close instant, so
    faster-closing markets are same-day information and slower ones lag.
    """
    if not isinstance(other, pd.Series) or not isinstance(other.index, pd.DatetimeIndex):
        raise ValueError(f"Other index {name} must supply a DatetimeIndexed Series.")
    if other.index.tz is None or other.index.hasnans or not other.index.is_unique \
            or not other.index.is_monotonic_increasing or len(other) < 6:
        raise ValueError(f"Other index {name} history is unusable for features.")
    if any(isinstance(value, (bool, np.bool_)) for value in other):
        raise ValueError(f"Other index {name} closes must be numeric.")
    try:
        other_prices = other.to_numpy(dtype=float)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Other index {name} closes must be numeric.") from exc
    if not np.isfinite(other_prices).all() or (other_prices <= 0).any():
        raise ValueError(f"Other index {name} closes must be finite and positive.")
    source = pd.DataFrame({"src": np.arange(len(other))}, index=other.index.tz_convert("UTC"))
    target = pd.DataFrame({"row": np.arange(len(index))}, index=index.tz_convert("UTC"))
    merged = pd.merge_asof(target, source, left_index=True, right_index=True, direction="backward")
    positions = merged["src"].to_numpy(dtype=float)
    known = np.isfinite(positions)
    returns = np.r_[np.nan, np.diff(np.log(other_prices))]
    mean5 = pd.Series(returns).rolling(5).mean().to_numpy()

    def _pick(values):
        picked = np.full(len(index), np.nan)
        picked[known] = values[positions[known].astype(int)]
        return picked

    return [_pick(returns), _pick(mean5)]


def _validate_regime(regime, n):
    if isinstance(regime, pd.Series):
        regime = regime.to_numpy(dtype=float)
    else:
        regime = np.asarray(regime, dtype=float)
    if regime.shape != (n,):
        raise ValueError("Regime series must align exactly with the closes.")
    finite = np.isfinite(regime)
    if not finite.any() or not finite[-1]:
        raise ValueError("Regime series must end with a finite probability.")
    if ((regime[finite] < 0.0) | (regime[finite] > 1.0)).any():
        raise ValueError("Regime probabilities must lie in [0, 1] where finite.")
    return regime


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


def evaluate_recommendation(closes: pd.Series, *, ohlc=None, others=None, regime=None,
                            cost_bps=35.0, horizon=5, min_train=252, min_trades=30,
                            probability_threshold=0.6, regime_gate_probability=None) -> dict:
    """Return a candidate only when every predefined historical gate passes.

    Training uses at most 504 matured labeled observations and training-only
    standardization. Test labels never participate in their own prediction.
    Missing values are rejected, not filled. Base features need 60 return
    warmups; optional OHLC, cross-index, and regime features may extend the
    warmup. Index returns minus assumed costs are research proxies, not
    executable PnL. Selection uses nonoverlapping trades (a new entry must
    follow prior exit); when ``regime_gate_probability`` is set, selection
    additionally requires the causal regime bull probability at the decision
    to meet the gate. The mean-return interval assumes local dependence
    captured by three trades; it is descriptive evidence, not a guarantee or
    calibrated success bound.
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
    if regime_gate_probability is not None:
        if isinstance(regime_gate_probability, (bool, np.bool_)) or not isinstance(regime_gate_probability, Real) \
                or not np.isfinite(regime_gate_probability) or not 0.5 <= regime_gate_probability <= 0.99:
            raise ValueError("regime_gate_probability must be in [0.5, 0.99] or None.")
        if regime is None:
            raise ValueError("regime_gate_probability requires a regime probability series.")
    n = len(prices)
    regime_values = _validate_regime(regime, n) if regime is not None else None
    extra = []
    if ohlc is not None:
        if not isinstance(ohlc, pd.DataFrame) or not {"open", "high", "low"}.issubset(ohlc.columns):
            raise ValueError("ohlc must supply open, high, and low columns.")
        if len(ohlc) != n or not ohlc.index.equals(index):
            raise ValueError("ohlc bars must align exactly with the closes.")
        try:
            bars = ohlc[["open", "high", "low"]].to_numpy(dtype=float)
        except (TypeError, ValueError) as exc:
            raise ValueError("ohlc values must be numeric.") from exc
        if not np.isfinite(bars).all() or (bars <= 0).any():
            raise ValueError("ohlc values must be finite and positive.")
        open_, high, low = bars.T
        tolerance = 1e-9 * high
        if (low > np.minimum(open_, prices) + tolerance).any() or (high < np.maximum(open_, prices) - tolerance).any():
            raise ValueError("ohlc bars must bracket their open and close.")
        extra.extend(_range_features(ohlc, prices))
    if others is not None:
        if not isinstance(others, dict) or not others or not all(isinstance(k, str) and k for k in others):
            raise ValueError("others must map index names to close Series.")
        for name in sorted(others):
            extra.extend(_aligned_other_features(name, others[name], index))
    if regime is not None:
        extra.append(pd.Series(regime_values))
    x = _features(prices, extra)
    complete = np.isfinite(x).all(axis=1)
    if not complete.any():
        raise ValueError("No complete feature row: every row is missing a feature value.")
    warmup = int(np.argmax(complete))
    if not complete[warmup:].all():
        raise ValueError("Feature values are missing after the warmup; input histories are inconsistent.")
    gate = None
    if regime_gate_probability is not None:
        gate = np.zeros(n, dtype=bool)
        finite = np.isfinite(regime_values)
        gate[finite] = regime_values[finite] >= regime_gate_probability
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
    for decision in range(warmup + min_train + horizon, n):
        last_mature = decision - horizon - 1
        train = np.arange(max(warmup, last_mature - 503), last_mature + 1)
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
        if probability >= probability_threshold and decision + 1 > last_exit \
                and (gate is None or gate[decision]):
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
        "regime_gate_probability": float(regime_gate_probability) if regime_gate_probability is not None else None,
        "feature_count": int(x.shape[1]), "warmup_sessions": int(warmup),
    }
    reasons = []
    if latest_probability is None:
        reasons.append("Insufficient matured training observations after the feature warmup.")
    elif latest_probability < probability_threshold:
        reasons.append("Current predicted probability is below the selection threshold.")
    if gate is not None and not gate[-1]:
        reasons.append("Current regime bull probability does not meet the regime gate.")
    if len(values) < min_trades:
        reasons.append(f"Only {len(values)} completed nonoverlapping selected trades; require {min_trades}.")
    if ci is None or ci[0] <= 0:
        reasons.append("Approximate 95% block-bootstrap mean net return lower bound is not positive.")
    if model_brier is None or model_brier >= baseline_brier:
        reasons.append("Out-of-sample Brier score does not improve on the training-frequency baseline.")
    status = "candidate" if not reasons else ("insufficient_data" if latest_probability is None or len(values) < min_trades else "no_trade")
    return {"status": status, "reasons": reasons, "probability": latest_probability,
            "data_asof": index[-1].isoformat(), "metrics": metrics, "model_version": MODEL_VERSION}
