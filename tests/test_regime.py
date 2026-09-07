import numpy as np
import pandas as pd
import pytest
from scipy.stats import norm

from analytics import regime


def prices(drift=0.001, seed=12, periods=160):
    returns = np.random.default_rng(seed).normal(drift, 0.003, periods - 1)
    return pd.Series(100 * np.exp(np.r_[0, np.cumsum(returns)]),
                     index=pd.bdate_range("2025-01-01", periods=periods))


@pytest.mark.parametrize("drift,positive", [(0.001, True), (-0.001, False)])
def test_real_model_direction_and_intervals(drift, positive):
    result = regime.analyze_regime(prices(drift), bootstrap_samples=10)
    assert (result.bull_probability > 0.5) == positive
    assert (result.estimated_mean_return > 0) == positive
    assert result.bull_probability + result.bear_probability == pytest.approx(1)
    assert 0 < result.bull_probability_interval[0] <= result.bull_probability_interval[1] < 1
    assert result.bull_bear_ratio == pytest.approx(result.bull_probability / result.bear_probability)
    assert result.ratio_interval == pytest.approx(tuple(p / (1 - p) for p in result.bull_probability_interval))
    assert result.mean_return_interval[0] < result.estimated_mean_return < result.mean_return_interval[1]
    assert result.bootstrap_successes >= 8


def test_deterministic_seed():
    series = prices(drift=0)
    assert regime.analyze_regime(series, bootstrap_samples=10, seed=42) == regime.analyze_regime(series, bootstrap_samples=10, seed=42)


@pytest.mark.parametrize("kind", ["short", "nan", "infinite", "zero", "negative", "flat", "constant_return", "unsorted", "duplicate", "intraday", "bad_index", "nonnumeric"])
def test_reject_invalid_inputs(kind):
    series = prices()
    if kind == "short":
        series = series.iloc[:59]
    elif kind in ("nan", "infinite", "zero", "negative"):
        series.iloc[20] = {"nan": np.nan, "infinite": np.inf, "zero": 0, "negative": -1}[kind]
    elif kind == "flat":
        series[:] = 100
    elif kind == "constant_return":
        series[:] = np.exp(np.arange(len(series)) * .001)
    elif kind == "unsorted":
        series = series.iloc[::-1]
    elif kind == "duplicate":
        series.index = pd.DatetimeIndex([series.index[0]] * len(series))
    elif kind == "intraday":
        series.index = pd.date_range("2025-01-01", periods=len(series), freq="h")
    elif kind == "bad_index":
        series.index = pd.RangeIndex(len(series))
    elif kind == "nonnumeric":
        series = series.astype(str)
        series.iloc[0] = "invalid"
    with pytest.raises(regime.RegimeError):
        regime.analyze_regime(series, bootstrap_samples=10)


@pytest.mark.parametrize("samples", [0, 9, 501, 10.5, True])
def test_bootstrap_bounds(samples):
    with pytest.raises(regime.RegimeError, match="bootstrap_samples"):
        regime.analyze_regime(prices(), bootstrap_samples=samples)


def test_natural_log_returns_and_zero_preserving_scaling(monkeypatch):
    observed = []
    def capture(values, start_params=None):
        observed.append(values)
        raise regime.RegimeError("stop after input transformation")
    monkeypatch.setattr(regime, "_fit", capture)
    series = prices()
    with pytest.raises(regime.RegimeError):
        regime.analyze_regime(series, bootstrap_samples=10)
    natural_returns = np.diff(np.log(series.to_numpy()))
    np.testing.assert_allclose(observed[0], natural_returns / natural_returns.std(ddof=1))
    assert observed[0].mean() > 0


def test_bootstrap_failure_is_explicit(monkeypatch):
    original_fit = regime._fit
    calls = []
    def fail_bootstrap(values, start_params=None):
        calls.append(True)
        if len(calls) > 1:
            raise regime.RegimeError("nonconvergence")
        return original_fit(values)
    monkeypatch.setattr(regime, "_fit", fail_bootstrap)
    with pytest.raises(regime.RegimeError, match="0/10"):
        regime.analyze_regime(prices(), bootstrap_samples=10)


def test_filtered_state_matches_trusted_library():
    series = prices(drift=0)
    returns = np.diff(np.log(series))
    scale = returns.std(ddof=1)
    fitted = regime._fit(returns / scale)
    result = regime.analyze_regime(series, bootstrap_samples=10)
    mean = fitted.filtered_state[0, -1]
    sd = np.sqrt(fitted.filtered_state_cov[0, 0, -1])
    assert result.bull_probability == pytest.approx(norm.cdf(mean / sd), abs=1e-12)
    assert result.estimated_mean_return == pytest.approx(mean * scale)
