"""Exercise the real bot handler with Telegram and network boundaries mocked."""

import io
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import numpy as np
import pandas as pd
from PIL import Image
import pytest

from analytics.regime import RegimeResult, RegimeError
from analytics.regime_report import render_regime_report
from api_data_service.price_history import PriceHistory, PriceHistoryError
from analytics.volume_profile import analyze_volume_profile, VolumeProfileError


@pytest.fixture
def result():
    return RegimeResult(0.7, 0.3, (0.55, 0.8), 0.7 / 0.3,
                        (0.55 / 0.45, 4.0), 100, 100, 0.001, (-0.001, 0.003))


@pytest.fixture
def closes():
    return pd.Series(np.linspace(100, 120, 252),
                     index=pd.bdate_range("2025-09-08", periods=252, tz="Asia/Hong_Kong"),
                     name="close")


@pytest.fixture
def bot_module():
    import app
    return app


@pytest.fixture
def bot(bot_module):
    instance = bot_module.financial_data_bot.__new__(bot_module.financial_data_bot)
    instance._financial_data_bot__api = "http://example.test"
    instance._financial_data_bot__config = {}
    instance._financial_data_bot__on_trigger = Mock()
    instance._regime_lock = threading.BoundedSemaphore(1)
    return instance


@pytest.mark.parametrize("args,symbol", [([], "HK.HSImain"), (["700"], "HK.00700")])
async def test_regime_success(bot_module, bot, monkeypatch, closes, result, args, symbol):
    fetch = Mock(return_value=PriceHistory(closes, pd.DataFrame(), "Volume missing."))
    model = Mock(return_value=result)
    monkeypatch.setattr(bot_module, "fetch_market_history", fetch)
    monkeypatch.setattr(bot_module, "analyze_regime", model)
    update = SimpleNamespace(message=Mock(reply_text=AsyncMock(), reply_photo=AsyncMock()))
    await bot._regime(update, SimpleNamespace(args=args))
    assert fetch.call_args.args == ("http://example.test", symbol)
    assert fetch.call_args.kwargs["now"].tzinfo is not None
    model.assert_called_once_with(closes)
    reply = update.message.reply_photo.call_args.kwargs
    assert reply["photo"].startswith(b"\x89PNG")
    assert "Bull/Bear ratio:" in reply["caption"]
    assert len(reply["caption"]) <= 1024
    assert bot._regime_lock.acquire(blocking=False)
    bot._regime_lock.release()


@pytest.mark.parametrize("args", [["700", "extra"], ["US.AAPL"], ["bad"]])
async def test_bad_arguments_do_not_fetch(bot_module, bot, monkeypatch, args):
    fetch = Mock()
    monkeypatch.setattr(bot_module, "fetch_market_history", fetch)
    update = SimpleNamespace(message=Mock(reply_text=AsyncMock(), reply_photo=AsyncMock()))
    await bot._regime(update, SimpleNamespace(args=args))
    fetch.assert_not_called()
    update.message.reply_text.assert_called_once()


@pytest.mark.parametrize("error", [PriceHistoryError("stale"), RegimeError("fit failed"), RuntimeError("unexpected")])
async def test_failure_replies_and_releases_lock(bot_module, bot, monkeypatch, error):
    monkeypatch.setattr(bot_module, "fetch_market_history", Mock(side_effect=error))
    update = SimpleNamespace(message=Mock(reply_text=AsyncMock(), reply_photo=AsyncMock()))
    await bot._regime(update, SimpleNamespace(args=["700"]))
    update.message.reply_text.assert_called_once()
    update.message.reply_photo.assert_not_called()
    assert bot._regime_lock.acquire(blocking=False)
    bot._regime_lock.release()


async def test_busy_report_is_bounded(bot_module, bot, monkeypatch):
    fetch = Mock()
    monkeypatch.setattr(bot_module, "fetch_market_history", fetch)
    bot._regime_lock.acquire()
    update = SimpleNamespace(message=Mock(reply_text=AsyncMock(), reply_photo=AsyncMock()))
    await bot._regime(update, SimpleNamespace(args=[]))
    fetch.assert_not_called()
    assert "running" in update.message.reply_text.call_args.args[0]
    bot._regime_lock.release()


def test_command_registered_as_background_handler(bot):
    bot.application = Mock()
    bot.run()
    handlers = [call.args[0] for call in bot.application.add_handler.call_args_list]
    handler = next(h for h in handlers if "regime" in getattr(h, "commands", set()))
    assert handler.block is False
    assert handler.callback == bot._regime


def test_report_png_caption_and_no_pyplot_figures(closes, result):
    import matplotlib.pyplot as plt
    figures = plt.get_fignums()
    png, caption = render_regime_report("HK.HSImain", closes, result,
                                        now=pd.Timestamp("2026-09-07", tz="Asia/Hong_Kong"))
    with Image.open(io.BytesIO(png)) as image:
        image.verify()
    assert "Futures rolls" in caption
    assert "Mean log return" in caption
    assert "parameter uncertainty" in caption
    assert len(caption) <= 1024
    assert plt.get_fignums() == figures


def volume_bars(closes):
    rng = np.random.default_rng(123)
    rows, dates = [], []
    for day in closes.index[-60:]:
        for hour, center in [(10, 105), (11, 110), (13, 125), (14, 130), (16, 120)]:
            rows.append((center + rng.normal(0, 0.5), 1000 if hour != 16 else 10))
            dates.append(day + pd.Timedelta(hours=hour))
    rows[-1] = (120, 10)
    return pd.DataFrame(rows, index=pd.DatetimeIndex(dates), columns=["close", "volume"])


async def test_volume_profile_integrated(bot_module, bot, monkeypatch, closes, result):
    bars = volume_bars(closes)
    fetch = Mock(return_value=PriceHistory(closes, bars, None))
    monkeypatch.setattr(bot_module, "fetch_market_history", fetch)
    monkeypatch.setattr(bot_module, "analyze_regime", Mock(return_value=result))
    bot._financial_data_bot__config = {"regime_volume": {"mode": "cumulative", "sessions": 60}}
    update = SimpleNamespace(message=Mock(reply_text=AsyncMock(), reply_photo=AsyncMock()))
    await bot._regime(update, SimpleNamespace(args=["700"]))
    reply = update.message.reply_photo.call_args.kwargs
    assert "POC" in reply["caption"]
    assert "Daily cumulative volume differenced" in reply["caption"]
    assert fetch.call_args.kwargs["volume_mode"] == "cumulative"
    assert len(reply["caption"]) <= 1024
    with Image.open(io.BytesIO(reply["photo"])) as image:
        image.verify()


async def test_profile_failure_keeps_regime(bot_module, bot, monkeypatch, closes, result):
    monkeypatch.setattr(bot_module, "fetch_market_history", Mock(return_value=PriceHistory(closes, volume_bars(closes), None)))
    monkeypatch.setattr(bot_module, "analyze_regime", Mock(return_value=result))
    monkeypatch.setattr(bot_module, "analyze_volume_profile", Mock(side_effect=VolumeProfileError("Volume is too concentrated.")))
    update = SimpleNamespace(message=Mock(reply_text=AsyncMock(), reply_photo=AsyncMock()))
    await bot._regime(update, SimpleNamespace(args=["700"]))
    caption = update.message.reply_photo.call_args.kwargs["caption"]
    assert "Bull/Bear ratio" in caption
    assert "Volume profile unavailable: Volume is too concentrated." in caption
    update.message.reply_text.assert_not_called()


def test_profile_report_with_zones(closes, result):
    import matplotlib.pyplot as plt
    figures = plt.get_fignums()
    profile = analyze_volume_profile(volume_bars(closes), bandwidth=0.12)
    assert len(profile.supports) == 2
    assert len(profile.resistances) == 2
    png, caption = render_regime_report("HK.HSImain", closes, result, profile=profile,
                                        now=pd.Timestamp("2026-09-07", tz="Asia/Hong_Kong"))
    assert "60 sessions" in caption
    assert "Per-bar volume assumed" in caption
    assert len(caption) <= 1024
    with Image.open(io.BytesIO(png)) as image:
        image.verify()
    assert plt.get_fignums() == figures
