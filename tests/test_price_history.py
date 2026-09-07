from datetime import datetime, timezone
from unittest.mock import Mock, patch
import unittest

import pandas as pd
import requests

from api_data_service.price_history import (
    PriceHistoryError, fetch_price_history, fetch_market_history, normalize_hk_symbol,
)


NOW = datetime(2026, 9, 7, 4, tzinfo=timezone.utc)


def records(start="2025-09-08", end="2026-09-04"):
    return [{"time": day.strftime("%Y-%m-%d 16:00:00"), "close": 100 + i}
            for i, day in enumerate(pd.bdate_range(start, end))]


class PriceHistoryTests(unittest.TestCase):
    def fetch(self, data, now=NOW):
        response = Mock()
        response.json.return_value = {"data": data}
        with patch("api_data_service.price_history.requests.get", return_value=response) as get:
            result = fetch_price_history("http://example.test/", "700", now=now)
        return result, get, response

    def test_symbols(self):
        for value, expected in [("700", "HK.00700"), (" 5 ", "HK.00005"),
                                ("12345", "HK.12345"), ("HK.00700", "HK.00700"),
                                ("HK.HSImain", "HK.HSImain")]:
            self.assertEqual(normalize_hk_symbol(value), expected)
        for value in ["", "123456", "HK.700", "US.AAPL", "７００", "-700", None]:
            with self.subTest(value=value), self.assertRaises(PriceHistoryError):
                normalize_hk_symbol(value)

    def test_request_contract_and_daily_selection(self):
        data = records()
        data.extend([{"time": "2026-09-04 17:00:00", "close": 987},
                     {"time": "2026-09-04T08:00:00Z", "close": data[-1]["close"]},
                     {"time": "2026-09-07 09:00:00", "close": 9999},
                     {"time": "2025-09-06 16:00:00", "close": 1}])
        data.reverse()
        series, get, response = self.fetch(data)
        self.assertEqual(series.name, "close")
        self.assertEqual(str(series.index.tz), "Asia/Hong_Kong")
        self.assertTrue(series.index.is_monotonic_increasing)
        self.assertTrue(series.index.is_unique)
        self.assertEqual(series.iloc[-1], 987)
        self.assertEqual(len(series), len(records()))
        self.assertEqual(series.index[-1], pd.Timestamp("2026-09-04", tz="Asia/Hong_Kong"))
        get.assert_called_once_with("http://example.test/equity/getTickerHistData",
                                    params={"ticker": "HK.00700", "startDate": "2025-09-07",
                                            "endDate": "2026-09-06"}, timeout=(5, 30))
        response.raise_for_status.assert_called_once()

    def test_hk_day_boundary_and_calendar_year(self):
        _, get, _ = self.fetch(records("2023-03-01", "2024-02-28"),
                              datetime(2024, 2, 28, 17, tzinfo=timezone.utc))
        self.assertEqual(get.call_args.kwargs["params"]["startDate"], "2023-02-28")
        self.assertEqual(get.call_args.kwargs["params"]["endDate"], "2024-02-28")

    def test_invalid_records(self):
        invalid = [{"time": t, "close": 5} for t in ["garbage", None, 12345, "NaT"]]
        invalid += [{"time": "2026-09-04", "close": c}
                    for c in [0, -1, None, "NaN", float("inf"), True, "oops"]]
        invalid += [{"time": "2026-09-04"}, {}, "bad"]
        for record in invalid:
            with self.subTest(record=record), self.assertRaises(PriceHistoryError):
                self.fetch(records() + [record])

    def test_conflicting_duplicates(self):
        with self.assertRaisesRegex(PriceHistoryError, "conflicting"):
            self.fetch(records() + [{"time": "2026-09-04 16:00:00", "close": 999}])

    def test_insufficient_history(self):
        for data, message in [([], "No price"), (records()[:100], "120"),
                              (records("2026-01-01"), "coverage"),
                              (records(end="2026-08-28"), "stale")]:
            with self.subTest(message=message), self.assertRaisesRegex(PriceHistoryError, message):
                self.fetch(data)

    def test_bad_schema_and_network(self):
        for payload in [[], {}, {"data": None}, {"data": {}}]:
            response = Mock()
            response.json.return_value = payload
            with patch("api_data_service.price_history.requests.get", return_value=response):
                with self.assertRaisesRegex(PriceHistoryError, "data list"):
                    fetch_price_history("http://example.test", "700", now=NOW)
        for failure in [requests.Timeout("timeout"), requests.HTTPError("500"), ValueError("JSON")]:
            response = Mock()
            response.raise_for_status.side_effect = failure
            with patch("api_data_service.price_history.requests.get", return_value=response):
                with self.assertRaisesRegex(PriceHistoryError, "request failed"):
                    fetch_price_history("http://example.test", "700", now=NOW)

    def test_naive_now_rejected(self):
        with self.assertRaisesRegex(PriceHistoryError, "timezone-aware"):
            fetch_price_history("http://example.test", "700", now=datetime(2026, 9, 7))


class MarketHistoryTests(unittest.TestCase):
    def fetch(self, data, mode="per_bar"):
        response = Mock()
        response.json.return_value = {"data": data}
        with patch("api_data_service.price_history.requests.get", return_value=response) as get:
            result = fetch_market_history("http://example.test", "700", now=NOW,
                                          volume_mode=mode)
        get.assert_called_once()
        return result

    def data(self):
        return [dict(record, volume=100) for record in records()]

    def test_intraday_bars_and_identical_duplicates_preserved_once(self):
        data = self.data()
        data += [{"time": "2026-09-04 10:00:00", "close": 200, "volume": 50},
                 dict(data[-1])]
        result = self.fetch(list(reversed(data)))
        self.assertIsNone(result.volume_error)
        self.assertEqual(len(result.bars), len(records()) + 1)
        self.assertTrue(result.bars.index.is_unique)
        self.assertTrue(result.bars.index.is_monotonic_increasing)
        self.assertEqual(str(result.bars.index.tz), "Asia/Hong_Kong")
        self.assertEqual(result.bars["volume"].sum(), len(records()) * 100 + 50)
        self.assertEqual(result.closes.iloc[-1], data[-1]["close"])

    def test_invalid_volumes_only_disable_optional_bars(self):
        for value in [None, -1, float("nan"), float("inf"), True, "oops"]:
            with self.subTest(volume=value):
                data = self.data()
                data[-1]["volume"] = value
                result = self.fetch(data)
                self.assertIsNotNone(result.volume_error)
                self.assertTrue(result.bars.empty)
                self.assertEqual(len(result.closes), len(records()))
        result = self.fetch(records())
        self.assertIsNotNone(result.volume_error)
        self.assertTrue(result.bars.empty)

    def test_conflicting_duplicate_volume_disables_bars(self):
        data = self.data()
        data.append(dict(data[-1], volume=101))
        result = self.fetch(data)
        self.assertIn("conflicting volumes", result.volume_error)
        self.assertTrue(result.bars.empty)
        self.assertEqual(len(result.closes), len(records()))

    def test_cumulative_deltas_and_daily_resets(self):
        data = self.data()
        data += [{"time": "2026-09-03 10:00:00", "close": 200, "volume": 20},
                 {"time": "2026-09-04 10:00:00", "close": 200, "volume": 30}]
        result = self.fetch(data, "cumulative")
        self.assertIsNone(result.volume_error)
        self.assertEqual(result.bars.loc["2026-09-03", "volume"].tolist(), [20, 80])
        self.assertEqual(result.bars.loc["2026-09-04", "volume"].tolist(), [30, 70])
        self.assertEqual(result.bars["volume"].sum(), len(records()) * 100)

    def test_cumulative_decrease_rejected_without_autodetection(self):
        data = self.data() + [{"time": "2026-09-04 10:00:00", "close": 200, "volume": 150}]
        cumulative = self.fetch(data, "cumulative")
        self.assertIn("decreases", cumulative.volume_error)
        self.assertTrue(cumulative.bars.empty)
        per_bar = self.fetch(data)
        self.assertIsNone(per_bar.volume_error)
        self.assertEqual(per_bar.bars.loc["2026-09-04", "volume"].sum(), 250)

    def test_invalid_volume_outside_requested_window_is_ignored(self):
        data = self.data() + [{"time": "2026-09-07 10:00:00", "close": 200},
                             {"time": "2025-09-01 10:00:00", "close": 200}]
        self.assertIsNone(self.fetch(data).volume_error)

    def test_invalid_mode_fails_before_api_call(self):
        with patch("api_data_service.price_history.requests.get") as get:
            with self.assertRaisesRegex(PriceHistoryError, "volume_mode"):
                fetch_market_history("http://example.test", "700", now=NOW,
                                     volume_mode="automatic")
        get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
