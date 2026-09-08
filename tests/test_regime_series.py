import numpy as np
import pandas as pd
import pytest

from analytics import regime
from analytics.regime_series import regime_probability_series


def closes(drift=0.001, seed=5, periods=300):
    returns = np.random.default_rng(seed).normal(drift, 0.01, periods - 1)
    return pd.Series(100 * np.exp(np.r_[0, np.cumsum(returns)]),
                     index=pd.bdate_range("2023-01-02", periods=periods, tz="Asia/Hong_Kong"))


def test_series_shape_warmup_and_range():
    series = closes()
    probabilities = regime_probability_series(series, refit_every=21, min_closes=60)
    assert len(probabilities) == len(series)
    assert np.isnan(probabilities[:60]).all()
    assert np.isfinite(probabilities[60:]).all()
    assert ((probabilities[60:] > 0) & (probabilities[60:] < 1)).all()


def test_series_is_causal():
    original = closes()
    base = regime_probability_series(original, refit_every=21, min_closes=60)
    truncated = original.iloc[:-10].copy()
    trimmed = regime_probability_series(truncated, refit_every=21, min_closes=60)
    # Probabilities before the truncation point are unchanged.
    np.testing.assert_array_equal(base[:-10], trimmed)


def test_series_direction_matches_drift():
    up = regime_probability_series(closes(drift=0.002), refit_every=21, min_closes=60)
    down = regime_probability_series(closes(drift=-0.002), refit_every=21, min_closes=60)
    assert up[-1] > 0.5
    assert down[-1] < 0.5


def test_series_agrees_with_full_history_fit_at_the_end():
    series = closes()
    probabilities = regime_probability_series(series, refit_every=21, min_closes=60)
    full = regime.analyze_regime(series, bootstrap_samples=10)
    # Same convention, expanding fit near the end: the two estimates agree closely.
    assert abs(probabilities[-1] - full.bull_probability) < 0.25


@pytest.mark.parametrize("kind", ["short", "flat", "unsorted", "bad_refit", "bool_refit", "bad_min"])
def test_invalid_inputs_rejected(kind):
    series = closes(periods=120)
    kwargs = {"refit_every": 21, "min_closes": 60}
    if kind == "short":
        series = series.iloc[:50]
    elif kind == "flat":
        series.iloc[:] = 100
    elif kind == "unsorted":
        series = series.iloc[::-1]
    elif kind == "bad_refit":
        kwargs["refit_every"] = 0
    elif kind == "bool_refit":
        kwargs["refit_every"] = True
    elif kind == "bad_min":
        kwargs["min_closes"] = 1
    with pytest.raises(regime.RegimeError):
        regime_probability_series(series, **kwargs)
