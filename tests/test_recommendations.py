import json

import numpy as np
import pandas as pd
import pytest

from analytics import recommendations as model


def closes(n=180):
    rng = np.random.default_rng(29)
    return pd.Series(100 * np.exp(np.cumsum(rng.normal(0.001, 0.01, n))),
                     index=pd.date_range('2020-01-01', periods=n, freq='B', tz='Asia/Hong_Kong'))


def test_natural_log_features_and_warmup():
    prices = np.exp(np.arange(100) * 0.01)
    features = model._features(prices)
    assert np.isnan(features[59, 3])
    np.testing.assert_allclose(features[60:, :4], 0.01, atol=1e-14)
    np.testing.assert_allclose(features[60:, 4:], 0, atol=1e-14)


def test_sparse_data_never_forces_idea():
    result = model.evaluate_recommendation(closes(100))
    assert result['status'] == 'insufficient_data'
    assert result['probability'] is None
    json.dumps(result, allow_nan=False)


def test_numerical_fit_and_deterministic_result():
    a = model.evaluate_recommendation(closes(), min_train=30, min_trades=3)
    b = model.evaluate_recommendation(closes(), min_train=30, min_trades=3)
    assert a == b
    assert 0 < a['probability'] < 1
    assert a['metrics']['model_brier'] >= 0
    json.dumps(a, allow_nan=False)


def test_mature_labels_costs_and_nonoverlap(monkeypatch):
    data = closes(160)
    seen = []
    def prediction(x, y, current):
        seen.append(y.copy())
        return 0.9
    monkeypatch.setattr(model, '_fit_probability', prediction)
    result = model.evaluate_recommendation(data, min_train=20, min_trades=3)
    # First decision 85: rows 60..79 mature on/before 85.
    expected = (data.to_numpy()[66:86] / data.to_numpy()[61:81] - 1 - .0035 > 0)
    np.testing.assert_array_equal(seen[0], expected)
    decisions = list(range(85, 154, 6))
    returns = [data.iloc[d + 6] / data.iloc[d + 1] - 1 - .0035 for d in decisions]
    assert result['metrics']['trade_count'] == len(decisions)
    assert result['metrics']['mean_net_return'] == pytest.approx(np.mean(returns))
    high_cost = model.evaluate_recommendation(data, min_train=20, min_trades=3, cost_bps=135)
    assert high_cost['metrics']['mean_net_return'] == pytest.approx(np.mean(returns) - .01)


def test_future_prefix_predictions_invariant(monkeypatch):
    data = closes(170)
    actual = model._fit_probability
    recorded = []
    def capture(x, y, current):
        p = actual(x, y, current)
        recorded.append(p)
        return p
    monkeypatch.setattr(model, '_fit_probability', capture)
    model.evaluate_recommendation(data.iloc[:140], min_train=20)
    prefix = recorded.copy()
    recorded.clear()
    model.evaluate_recommendation(data, min_train=20)
    np.testing.assert_array_equal(prefix, recorded[:len(prefix)])


@pytest.mark.parametrize('kind', ['nan', 'zero', 'duplicate', 'reverse', 'naive', 'bool'])
def test_invalid_closes_rejected(kind):
    data = closes()
    if kind == 'nan': data.iloc[4] = np.nan
    elif kind == 'zero': data.iloc[4] = 0
    elif kind == 'duplicate': data.index = data.index[:1].append(data.index[:-1])
    elif kind == 'reverse': data = data.iloc[::-1]
    elif kind == 'naive': data.index = data.index.tz_localize(None)
    elif kind == 'bool': data = data.astype(object); data.iloc[4] = True
    with pytest.raises(ValueError): model.evaluate_recommendation(data)


def test_exchange_local_days_supported():
    data = closes(100)
    data.index = pd.date_range('2020-01-01 16:00', periods=100, freq='B', tz='America/New_York')
    assert model.evaluate_recommendation(data)['status'] == 'insufficient_data'


def test_no_trade_when_losses_despite_high_probability(monkeypatch):
    data = closes(200)
    data[:] = 100 * np.exp(-np.arange(200) * .01)
    monkeypatch.setattr(model, '_fit_probability', lambda *args: .9)
    result = model.evaluate_recommendation(data, min_train=20, min_trades=3)
    assert result['status'] == 'no_trade'
    assert result['metrics']['mean_net_ci'][1] < 0
    assert result['metrics']['hit_rate'] == 0
    assert any('Brier' in reason for reason in result['reasons'])


def test_bootstrap_reproducible_and_degenerate():
    assert model._mean_interval(np.array([.02] * 30)) == pytest.approx([.02, .02])
    assert model._mean_interval(np.array([.02])) is None


@pytest.mark.parametrize('parameter', ['cost_bps', 'probability_threshold'])
@pytest.mark.parametrize('value', [True, np.bool_(False), '0.6', 1 + 0j])
def test_parameter_types_rejected_cleanly(parameter, value):
    with pytest.raises(ValueError):
        model.evaluate_recommendation(closes(100), **{parameter: value})


def test_compounded_trade_close_drawdown_and_label_maturity(monkeypatch):
    data = closes(160)
    monkeypatch.setattr(model, '_fit_probability', lambda *args: .9)
    result = model.evaluate_recommendation(data, min_train=20, min_trades=3)
    net = np.array([data.iloc[d + 6] / data.iloc[d + 1] - 1 - .0035
                    for d in range(85, 154, 6)])
    equity = np.r_[1., np.cumprod(1 + net)]
    expected = np.min(equity / np.maximum.accumulate(equity) - 1)
    assert result['metrics']['max_drawdown'] == pytest.approx(expected)
    assert -1 <= result['metrics']['max_drawdown'] <= 0
    assert result['metrics']['latest_train_label_maturity'] == data.index[-1].isoformat()
    assert result['metrics']['train_end'] == data.index[-7].isoformat()


def test_scored_return_below_total_loss_rejected(monkeypatch):
    data = closes(160)
    data.iloc[91:] = .00001
    monkeypatch.setattr(model, '_fit_probability', lambda *args: .9)
    with pytest.raises(ValueError, match='-100%'):
        model.evaluate_recommendation(data, min_train=20)
