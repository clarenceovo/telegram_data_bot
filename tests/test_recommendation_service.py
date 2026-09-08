import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, AsyncMock

import numpy as np
import pandas as pd
import pytest

import recommendation_service as service

NOW = pd.Timestamp('2026-09-08T13:00:00Z')
ASOF = pd.Timestamp('2026-09-04', tz='America/New_York')

@pytest.fixture
def config_file(tmp_path):
    path = tmp_path / 'config.json'
    path.write_text(json.dumps({'watchlist': ['SPX']}))
    return path

@pytest.fixture
def store(tmp_path):
    return service.RecommendationStore(tmp_path / 'cache.sqlite3')


def result():
    return {'status': 'candidate', 'data_asof': ASOF.isoformat(),
            'model_version': service.MODEL_VERSION, 'probability': .65, 'reasons': [],
            'metrics': {'trade_count': 40, 'hit_rate': .7, 'mean_net_return': .01,
                        'mean_net_ci': [.002, .02], 'max_drawdown': -.1,
                        'model_brier': .2, 'baseline_brier': .25,
                        'oos_start': '2024-01-01', 'oos_end': '2026-08-28'}}


def scan(config_file, store, evaluator=None, fetcher=None):
    frame = pd.DataFrame({'open': [100., 101.], 'high': [101., 102.], 'low': [99., 100.],
                          'close': [100., 101.]},
                         index=pd.date_range('2026-09-03', periods=2, tz='America/New_York'))
    return service.run_scan(service.load_config(config_file), store, now=NOW,
                            evaluator=evaluator or Mock(side_effect=lambda *_a, **_k: result()),
                            fetcher=fetcher or Mock(return_value=frame),
                            regime_series=lambda closes, **_k: np.full(len(closes), 0.6))

@pytest.mark.parametrize('setting', [{'cost_bps': float('nan')}, {'horizon': 2}, {'watchlist': ['SPX', 'SPX']},
                                     {'refresh_seconds': True}, {'unknown': 1},
                                     {'regime_refit_every': 2}, {'regime_refit_every': 64},
                                     {'regime_gate_probability': 0.3}, {'regime_gate_probability': False}])
def test_invalid_config(config_file, setting):
    config_file.write_text(json.dumps(setting))
    with pytest.raises(ValueError):
        service.load_config(config_file)


def test_missing_config_fails(tmp_path):
    with pytest.raises(FileNotFoundError):
        service.load_config(tmp_path / 'missing')


def test_reuse_and_revisions(config_file, store):
    evaluator = Mock(side_effect=lambda *_a, **_k: result())
    scan(config_file, store, evaluator)
    scan(config_file, store, evaluator)
    assert evaluator.call_count == 1
    revised = pd.DataFrame({'open': [99., 101.], 'high': [100., 102.], 'low': [98., 100.],
                            'close': [99., 101.]},
                           index=pd.date_range('2026-09-03', periods=2, tz='America/New_York'))
    scan(config_file, store, evaluator, Mock(return_value=revised))
    assert evaluator.call_count == 2
    config_file.write_text(json.dumps({'watchlist': ['SPX'], 'cost_bps': 40}))
    scan(config_file, store, evaluator)
    assert evaluator.call_count == 3


def test_failure_invalidates_candidate_and_stop_preserves_snapshot(config_file, store):
    scan(config_file, store)
    snapshot = scan(config_file, store, fetcher=Mock(side_effect=ValueError('missing session')))
    assert snapshot['results']['SPX']['status'] == 'data_error'
    assert 'input_hash' not in snapshot['results']['SPX']
    assert service.run_scan(service.load_config(config_file), store, should_stop=lambda: True) is None
    assert store.read() == snapshot


def test_nonfinite_write_preserves_previous(store):
    store.write({'valid': 1})
    with pytest.raises(ValueError):
        store.write({'bad': float('nan')})
    assert store.read() == {'valid': 1}


def test_message_and_expiry(config_file, store):
    assert 'not ready' in service.recommendation_message(config_path=config_file, store=store)
    scan(config_file, store)
    kwargs = dict(config_path=config_file, store=store, now=NOW)
    message = service.recommendation_message(**kwargs)
    assert 'SPX: candidate' in message
    assert '2026-09-08 close; exit: 2026-09-15 close' in message
    assert 'uncalibrated' in message and '40 nonoverlapping' in message
    assert 'stale' in service.recommendation_message(**dict(kwargs, now=NOW + pd.Timedelta(hours=3)))
    # Fresh cache still cannot serve a signal after its planned entry close.
    snapshot = store.read()
    snapshot['completed_at'] = '2026-09-08T20:01:00Z'
    store.write(snapshot)
    assert 'stale' in service.recommendation_message(**dict(kwargs, now=pd.Timestamp(snapshot['completed_at'])))
    config_file.write_text(json.dumps({'watchlist': ['SPX'], 'cost_bps': 40}))
    assert 'settings changed' in service.recommendation_message(**kwargs)


def test_no_trade_detail(config_file, store):
    row = result()
    row.update(status='no_trade', reasons=['Mean interval includes zero.'])
    scan(config_file, store, evaluator=Mock(return_value=row))
    kwargs = dict(config_path=config_file, store=store, now=NOW)
    assert 'No indices currently meet' in service.recommendation_message(**kwargs)
    assert 'Mean interval includes zero' in service.recommendation_message('SPX', **kwargs)
    assert 'Choose a watched index' in service.recommendation_message('INVALID', **kwargs)


@pytest.mark.parametrize('args', [[], ['SPX'], ['SPX', 'extra']])
async def test_command_reads_cache_only(monkeypatch, args):
    import app
    bot = app.financial_data_bot.__new__(app.financial_data_bot)
    bot._financial_data_bot__on_trigger = Mock()
    formatter = Mock(return_value='x' * 4000)
    monkeypatch.setattr(app, 'recommendation_message', formatter)
    update = SimpleNamespace(message=SimpleNamespace(reply_text=AsyncMock()))
    await bot._recommend(update, SimpleNamespace(args=args))
    if len(args) > 1:
        formatter.assert_not_called()
    else:
        formatter.assert_called_once_with(args[0] if args else None)
        assert update.message.reply_text.call_count == 2
        assert all(len(call.args[0]) <= 3900 for call in update.message.reply_text.call_args_list)


def test_redis_watchlist_resolution(config_file):
    config_file.write_text(json.dumps({'watchlist': ['SPX'], 'redis_url': 'redis://localhost:6379/0'}))
    config = service.load_config(config_file)
    resolved, source = service.resolve_runtime_config(config, reader=lambda key: [b'^HSI', b'spx'])
    assert resolved.watchlist == ('HSI', 'SPX')
    assert source == 'redis:telegram_data_bot:watchlist'
    assert resolved.fingerprint != config.fingerprint
    for raw, why in [([], 'empty'), ([b'HSI', b'hsi'], 'duplicate'), ([b'NOPE'], 'unsupported')]:
        _, fallback = service.resolve_runtime_config(config, reader=lambda key, r=raw: r)
        assert fallback == 'file', why
    def unreachable(key):
        raise ConnectionError('redis down')
    _, fallback = service.resolve_runtime_config(config, reader=unreachable)
    assert fallback == 'file'
    plain, source = service.resolve_runtime_config(config, reader=lambda key: [b'NDX'])
    assert plain.watchlist == ('NDX',) and source.startswith('redis:')


def test_redis_disabled_without_url(config_file):
    config = service.load_config(config_file)
    resolved, source = service.resolve_runtime_config(config, reader=lambda key: [b'NDX', b'NDX'])
    assert resolved is config
    assert source == 'file'
    assert resolved.watchlist == ('SPX',)


@pytest.mark.parametrize('setting', [{'redis_url': 'http://localhost'}, {'redis_url': 123},
                                     {'watchlist_key': ''}, {'watchlist_key': 5}])
def test_invalid_redis_config(config_file, setting):
    config_file.write_text(json.dumps(setting))
    with pytest.raises(ValueError):
        service.load_config(config_file)


def test_redis_url_environment_override(config_file, monkeypatch):
    monkeypatch.setenv('RECOMMEND_REDIS_URL', 'redis://cache.internal:6379/1')
    config = service.load_config(config_file)
    assert config.redis_url == 'redis://cache.internal:6379/1'


def test_message_uses_resolved_redis_watchlist(config_file, store):
    config_file.write_text(json.dumps({'watchlist': ['SPX'], 'redis_url': 'redis://localhost:6379/0'}))
    reader = lambda key: [b'^SPX']
    assert 'not ready' in service.recommendation_message(config_path=config_file, store=store,
                                                         watchlist_reader=reader)
    scan(config_file, store)
    message = service.recommendation_message(config_path=config_file, store=store, now=NOW,
                                             watchlist_reader=reader)
    assert 'SPX: candidate' in message
    # A changed Redis list changes the fingerprint, hiding stale ideas.
    changed = lambda key: [b'^NDX']
    assert 'settings changed' in service.recommendation_message(config_path=config_file, store=store,
                                                                now=NOW, watchlist_reader=changed)
