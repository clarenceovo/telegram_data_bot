"""Python 3.14 / Telegram 22 startup and dispatch, without network calls."""

import asyncio
import inspect
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, mock_open

import pandas as pd
import pytest
from telegram import Update
from telegram.ext import Application, CommandHandler
from telegram.request import BaseRequest

import app


class OfflineRequest(BaseRequest):
    def __init__(self):
        self.calls = []

    @property
    def read_timeout(self):
        return 5.0

    async def initialize(self):
        pass

    async def shutdown(self):
        pass

    async def do_request(self, url, method, request_data=None, **kwargs):
        endpoint = url.rsplit("/", 1)[-1]
        self.calls.append(endpoint)
        if endpoint == "getMe":
            result = {"id": 1, "is_bot": True, "first_name": "Test", "username": "test_bot"}
        elif endpoint == "sendMessage":
            result = {"message_id": 2, "date": 1, "chat": {"id": 2, "type": "private"}, "text": "reply"}
        else:
            raise AssertionError("Unexpected Telegram method: " + endpoint)
        return 200, json.dumps({"ok": True, "result": result}).encode()


async def test_real_application_dispatch_awaits_help_reply():
    request = OfflineRequest()
    application = Application.builder().token("123:TEST").request(request).get_updates_request(OfflineRequest()).build()
    bot = app.financial_data_bot.__new__(app.financial_data_bot)
    bot._financial_data_bot__on_trigger = Mock()
    application.add_handler(CommandHandler("help", bot._help))
    update = Update.de_json({
        "update_id": 1,
        "message": {"message_id": 1, "date": 1, "chat": {"id": 2, "type": "private"},
                    "from": {"id": 2, "is_bot": False, "first_name": "User"},
                    "text": "/help", "entities": [{"type": "bot_command", "offset": 0, "length": 5}]}
    }, application.bot)
    async with application:
        await application.process_update(update)
    assert request.calls == ["getMe", "sendMessage"]


def test_startup_selects_token_and_has_explicit_event_loop(monkeypatch):
    config = {"api_endpoint": "localhost:8000", "telegram_token_uat": "123:UAT", "telegram_token_prod": "456:PROD"}
    monkeypatch.setattr("builtins.open", mock_open(read_data=json.dumps(config)))
    monkeypatch.setattr(app, "data_service", Mock())
    monkeypatch.setenv("ENVIRONMENT", "UAT")
    bot = app.financial_data_bot([])
    assert bot.application.bot.token == "123:UAT"
    loops = []
    def polling(self, **kwargs):
        loop = asyncio.get_event_loop()
        assert not loop.is_closed()
        assert kwargs == {"close_loop": False}
        loops.append(loop)
    monkeypatch.setattr(Application, "run_polling", polling)
    bot.run()
    assert loops[0].is_closed()
    handlers = bot.application.handlers[0]
    assert len(handlers) == 14
    assert all(inspect.iscoroutinefunction(handler.callback) for handler in handlers)
    regime = next(handler for handler in handlers if "regime" in getattr(handler, "commands", ()))
    assert regime.block is False


async def test_signal_uses_positional_latest_values_with_pandas3():
    bot = app.financial_data_bot.__new__(app.financial_data_bot)
    bot._financial_data_bot__on_trigger = Mock()
    columns = ["open", "high", "low", "close", "ret", "adx", "dmi_plus", "dmi_minus",
               "vol_1m", "vol_3m", "zscore_plus_1", "zscore_plus_2", "zscore_plus_3",
               "zscore_minus_1", "zscore_minus_2", "zscore_minus_3", "overbought_ceiling", "oversold_ceiling"]
    frame = pd.DataFrame({column: [1.0, 2.0] for column in columns},
                         index=pd.date_range("2026-09-01", periods=2))
    bot.analytic_client = Mock()
    bot.analytic_client.get_instrument_signal.return_value = frame
    message = Mock(text="/signal HK.00700", reply_text=AsyncMock())
    await bot.instrument_signal(SimpleNamespace(message=message), SimpleNamespace(args=["HK.00700"]))
    message.reply_text.assert_awaited_once()
    assert "1Month Vol: 200.0" in message.reply_text.call_args.args[0]
