from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
import requests

from api_data_service import index_history as h


NOW = pd.Timestamp('2026-09-08 12:00:00', tz='UTC')


def payload(code='SPX', count=550):
    spec = h.INDICES[code]
    dates = h._completed(spec, NOW)[-count:]
    times = [(date + pd.Timedelta(hours=12)).timestamp() for date in dates]
    return {'chart': {'error': None, 'result': [{
        'meta': {'symbol': spec.symbol, 'exchangeTimezoneName': spec.timezone},
        'timestamp': times,
        'indicators': {'quote': [{'close': list(np.linspace(100, 150, count))}]},
    }]}}


def install(monkeypatch, data):
    get = Mock(return_value=Mock(json=Mock(return_value=data)))
    monkeypatch.setattr(h.requests, 'get', get)
    return get


@pytest.mark.parametrize('code', list(h.INDICES))
def test_valid_contract_and_request(monkeypatch, code):
    get = install(monkeypatch, payload(code))
    result = h.fetch_index_history(code, now=NOW)
    assert len(result) == 550
    assert result.name == 'close'
    assert str(result.index.tz) == h.INDICES[code].timezone
    assert result.index.is_monotonic_increasing and result.index.is_unique
    assert result.index[-1] == h.latest_completed_session(code, now=NOW)
    assert (result.index.hour == 0).all()
    assert get.call_args.kwargs['timeout'] == (5, 20)
    assert get.call_args.kwargs['params'] == {'range': '5y', 'interval': '1d'}


@pytest.mark.parametrize('code,now,expected', [
    ('SPX', '2026-09-07 23:00Z', '2026-09-04'),  # Labor Day
    ('SPX', '2026-09-08 20:29:59Z', '2026-09-04'),
    ('SPX', '2026-09-08 20:30:00Z', '2026-09-08'),
    ('SPX', '2026-11-27 18:29:59Z', '2026-11-25'),  # half day EST
    ('SPX', '2026-11-27 18:30:00Z', '2026-11-27'),
    ('HSI', '2026-09-06 23:00Z', '2026-09-04'),
    ('HSI', '2023-09-01 23:00Z', '2023-08-31'),
    ('HSI', '2023-09-08 23:00Z', '2023-09-07'),
    ('HSI', '2026-09-08 08:29:59Z', '2026-09-07'),
    ('HSI', '2026-09-08 08:30:00Z', '2026-09-08'),
    ('N225', '2026-09-08 06:59:59Z', '2026-09-07'),
    ('N225', '2026-09-08 07:00:00Z', '2026-09-08'),
])
def test_completed_session_boundaries(code, now, expected):
    assert str(h.latest_completed_session(code, now=pd.Timestamp(now)).date()) == expected


def test_current_bar_invalid_is_excluded(monkeypatch):
    data = payload()
    result = data['chart']['result'][0]
    result['timestamp'].append(pd.Timestamp('2026-09-08 13:30Z').timestamp())
    result['indicators']['quote'][0]['close'].append(None)
    install(monkeypatch, data)
    assert len(h.fetch_index_history('SPX', now=NOW)) == 550


@pytest.mark.parametrize('invalid', [None, float('nan'), float('inf'), 0, -1, True, '120'])
def test_invalid_completed_close(monkeypatch, invalid):
    data = payload()
    data['chart']['result'][0]['indicators']['quote'][0]['close'][10] = invalid
    install(monkeypatch, data)
    with pytest.raises(h.IndexHistoryError, match='Invalid completed close'):
        h.fetch_index_history('SPX', now=NOW)


@pytest.mark.parametrize('position,match', [(-1, 'Stale'), (200, 'Missing intervening')])
def test_missing_sessions(monkeypatch, position, match):
    data = payload()
    result = data['chart']['result'][0]
    result['timestamp'].pop(position)
    result['indicators']['quote'][0]['close'].pop(position)
    install(monkeypatch, data)
    with pytest.raises(h.IndexHistoryError, match=match):
        h.fetch_index_history('SPX', now=NOW)


@pytest.mark.parametrize('conflict', [False, True])
def test_duplicates(monkeypatch, conflict):
    data = payload()
    result = data['chart']['result'][0]
    result['timestamp'].append(result['timestamp'][0])
    result['indicators']['quote'][0]['close'].append(101 if conflict else 100)
    install(monkeypatch, data)
    if conflict:
        with pytest.raises(h.IndexHistoryError, match='Conflicting duplicate'):
            h.fetch_index_history('SPX', now=NOW)
    else:
        assert len(h.fetch_index_history('SPX', now=NOW)) == 550


@pytest.mark.parametrize('field,value', [('symbol', '^HSI'), ('exchangeTimezoneName', 'UTC')])
def test_metadata_mismatch(monkeypatch, field, value):
    data = payload()
    data['chart']['result'][0]['meta'][field] = value
    install(monkeypatch, data)
    with pytest.raises(h.IndexHistoryError):
        h.fetch_index_history('SPX', now=NOW)


def test_insufficient_history(monkeypatch):
    install(monkeypatch, payload(count=499))
    with pytest.raises(h.IndexHistoryError, match='500'):
        h.fetch_index_history('SPX', now=NOW)


def test_network_error_wrapped(monkeypatch):
    monkeypatch.setattr(h.requests, 'get', Mock(side_effect=requests.Timeout('timeout')))
    with pytest.raises(h.IndexHistoryError, match='Timeout'):
        h.fetch_index_history('SPX', now=NOW)


def test_naive_processing_time_rejected():
    with pytest.raises(h.IndexHistoryError, match='timezone-aware'):
        h.latest_completed_session('SPX', now='2026-09-08')


def test_planned_us_holiday_and_five_returns():
    plan = h.planned_sessions("SPX", "2026-09-04")
    assert plan == {
        "entry_session": "2026-09-08",
        "exit_session": "2026-09-15",
        "expires_at": "2026-09-08T20:00:00+00:00",
    }


def test_planned_hsi_weather_closures_share_history_calendar():
    plan = h.planned_sessions("HSI", "2023-08-31")
    assert plan["entry_session"] == "2023-09-04"
    assert plan["exit_session"] == "2023-09-12"
    assert plan["expires_at"] == "2023-09-04T08:00:00+00:00"
    assert h.planned_sessions("HSI", "2023-09-07")["entry_session"] == "2023-09-11"


@pytest.mark.parametrize("date", ["2023-09-01", "2023-09-08", None])
def test_plan_rejects_non_session(date):
    with pytest.raises(h.IndexHistoryError):
        h.planned_sessions("HSI", date)


def test_plan_interprets_aware_date_in_exchange_timezone():
    assert h.planned_sessions("HSI", "2023-08-30T16:00:00Z") == h.planned_sessions("HSI", "2023-08-31")


@pytest.mark.parametrize("horizon", [0, -1, True, 1.5])
def test_plan_rejects_invalid_horizon(horizon):
    with pytest.raises(h.IndexHistoryError):
        h.planned_sessions("SPX", "2026-09-04", horizon)
