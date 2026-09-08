"""Completed daily cash-index OHLC bars for research, fetched from Yahoo's chart API.

Yahoo's public endpoint is unofficial. Index closes are not executable prices.
Only exchange sessions closed for at least 30 minutes are accepted; gaps are
errors, never forward-filled. Returned timestamps are exchange-local midnight.
"""
from dataclasses import dataclass
from functools import lru_cache
from urllib.parse import quote
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import numpy as np
import pandas as pd
import requests


class IndexHistoryError(ValueError):
    """Unavailable, stale, or malformed index history."""


@dataclass(frozen=True)
class IndexSpec:
    code: str
    symbol: str
    calendar: str
    timezone: str


INDICES = {
    "HSI": IndexSpec("HSI", "^HSI", "XHKG", "Asia/Hong_Kong"),
    "N225": IndexSpec("N225", "^N225", "XTKS", "Asia/Tokyo"),
    "NDX": IndexSpec("NDX", "^NDX", "XNYS", "America/New_York"),
    "SPX": IndexSpec("SPX", "^GSPC", "XNYS", "America/New_York"),
    "DJI": IndexSpec("DJI", "^DJI", "XNYS", "America/New_York"),
}


def _spec(code: str) -> IndexSpec:
    try:
        return INDICES[code.upper()]
    except (KeyError, AttributeError) as exc:
        raise IndexHistoryError(f"Unsupported index: {code}") from exc


def _utc_now(now) -> pd.Timestamp:
    value = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if pd.isna(value) or value.tzinfo is None:
        raise IndexHistoryError("now must be a valid timezone-aware timestamp")
    return value.tz_convert("UTC")


@lru_cache(maxsize=16)
def _calendar(name: str, year: int):
    return xcals.get_calendar(name, start=f"{year - 6}-01-01", end=f"{year + 1}-12-31")


def _schedule(spec: IndexSpec, year: int) -> pd.DataFrame:
    """Shared exchange schedule, including verified exceptional closures."""
    schedule = _calendar(spec.calendar, year).schedule
    if spec.calendar == "XHKG":
        # Full-day weather closures absent from exchange_calendars 4.13.2.
        # HKEX official notices (not inferred from missing vendor bars):
        # https://www.hkex.com.hk/News/Market-Communications/2023/2309012news?sc_lang=en
        # https://www.hkex.com.hk/News/Market-Communications/2023/2309083news?sc_lang=en
        schedule = schedule.drop(pd.DatetimeIndex(["2023-09-01", "2023-09-08"]), errors="ignore")
    return schedule


def _completed(spec: IndexSpec, now: pd.Timestamp) -> pd.DatetimeIndex:
    try:
        schedule = _schedule(spec, now.year)
        completed = schedule.index[schedule["close"] + pd.Timedelta(minutes=30) <= now]
    except Exception as exc:
        raise IndexHistoryError(f"Cannot determine {spec.code} exchange sessions") from exc
    if len(completed) == 0:
        raise IndexHistoryError(f"No completed sessions for {spec.code}")
    return completed.tz_localize(ZoneInfo(spec.timezone))


def planned_sessions(code: str, data_asof, horizon: int = 5) -> dict:
    """Next-session close entry and exit after ``horizon`` further sessions.

    ``data_asof`` is an exchange session date (or an aware event timestamp,
    interpreted in the exchange timezone). The idea expires at entry close,
    without the 30-minute data-publication delay used for completed history.
    """
    spec = _spec(code)
    if isinstance(horizon, bool) or not isinstance(horizon, int) or horizon < 1:
        raise IndexHistoryError("horizon must be a positive integer")
    try:
        date = pd.Timestamp(data_asof)
        if pd.isna(date):
            raise IndexHistoryError("Invalid data_asof session")
        if date.tzinfo is not None:
            date = date.tz_convert(spec.timezone).tz_localize(None)
        date = date.normalize()
        schedule = _schedule(spec, date.year)
        if date not in schedule.index:
            raise IndexHistoryError("data_asof must identify an exchange session")
        future = schedule.loc[schedule.index > date]
        if len(future) <= horizon:
            raise IndexHistoryError("Not enough future exchange sessions")
        return {
            "entry_session": future.index[0].strftime("%Y-%m-%d"),
            "exit_session": future.index[horizon].strftime("%Y-%m-%d"),
            "expires_at": future.iloc[0]["close"].tz_convert("UTC").isoformat(),
        }
    except IndexHistoryError:
        raise
    except (TypeError, ValueError, OverflowError) as exc:
        raise IndexHistoryError(f"Cannot plan {spec.code} sessions") from exc


def latest_completed_session(code: str, *, now=None) -> pd.Timestamp:
    """Latest session date after the exchange close plus a 30-minute delay."""
    return _completed(_spec(code), _utc_now(now))[-1]


def fetch_index_history(code: str, *, now=None) -> pd.DataFrame:
    """Fetch five years of daily OHLC bars, requiring >=500 contiguous sessions.

    ``now`` is processing time (aware); Yahoo timestamps are event instants in
    epoch seconds, converted to exchange session dates. In-progress bars are
    discarded before price validation. Each completed session must supply
    finite positive open, high, low, and close with low <= open, close <= high
    (index units are positive, so log-range estimators stay defined).
    Historical callers must supply only historical observations to any
    downstream fit. The returned DataFrame has open, high, low, and close
    columns on exchange-local midnight timestamps.
    """
    spec = _spec(code)
    completed = _completed(spec, _utc_now(now))
    try:
        response = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(spec.symbol, safe='')}",
            params={"range": "5y", "interval": "1d"},
            headers={"User-Agent": "telegram-data-bot/1.0"},
            timeout=(5, 20),
        )
        response.raise_for_status()
        chart = response.json()["chart"]
        if chart.get("error"):
            raise IndexHistoryError(f"Yahoo reported an error for {spec.code}")
        results = chart["result"]
        if not isinstance(results, list) or len(results) != 1:
            raise IndexHistoryError("Expected exactly one Yahoo chart result")
        result = results[0]
        meta = result["meta"]
        if meta["symbol"] != spec.symbol:
            raise IndexHistoryError("Yahoo returned a different index symbol")
        if meta["exchangeTimezoneName"] != spec.timezone:
            raise IndexHistoryError("Yahoo exchange timezone does not match the index")
        timestamps = result["timestamp"]
        quotes = result["indicators"]["quote"]
        if not isinstance(quotes, list) or len(quotes) != 1:
            raise IndexHistoryError("Expected exactly one daily quote series")
        fields = {name: quotes[0][name] for name in ("open", "high", "low", "close")}
        if not isinstance(timestamps, list) or len(timestamps) == 0:
            raise IndexHistoryError("Timestamp and close arrays must have equal lengths")
        for name, values in fields.items():
            if not isinstance(values, list) or len(values) != len(timestamps):
                raise IndexHistoryError("Timestamp and close arrays must have equal lengths")
        if any(isinstance(t, bool) or not isinstance(t, (int, float)) or not np.isfinite(t) for t in timestamps):
            raise IndexHistoryError("Invalid epoch-second timestamp")
        dates = pd.to_datetime(timestamps, unit="s", utc=True).tz_convert(spec.timezone).normalize()
        rows = {}
        eligible = set(completed)
        for i, date in enumerate(dates):
            if date not in eligible:
                continue
            bar = {}
            for name, values in fields.items():
                value = values[i]
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not np.isfinite(value) or value <= 0:
                    raise IndexHistoryError(f"Invalid completed {name} for {spec.code} on {date.date()}")
                bar[name] = float(value)
            tolerance = 1e-9 * bar["high"]
            if (bar["low"] > min(bar["open"], bar["close"]) + tolerance
                    or bar["high"] < max(bar["open"], bar["close"]) - tolerance):
                raise IndexHistoryError(f"OHLC bar does not bracket its body for {spec.code} on {date.date()}")
            if date in rows and rows[date] != bar:
                raise IndexHistoryError(f"Conflicting duplicate session: {date.date()}")
            rows[date] = bar
        if len(rows) < 500:
            raise IndexHistoryError(f"{spec.code} requires at least 500 completed sessions")
        frame = pd.DataFrame.from_dict(rows, orient="index", dtype="float64").sort_index()
        frame.index.name = None
        if frame.index[-1] != completed[-1]:
            raise IndexHistoryError(f"Stale {spec.code} history: expected {completed[-1].date()}")
        expected = completed[completed >= frame.index[0]]
        if not frame.index.equals(expected):
            raise IndexHistoryError(f"Missing intervening exchange sessions for {spec.code}")
        return frame
    except IndexHistoryError:
        raise
    except (requests.RequestException, KeyError, TypeError, ValueError, OverflowError) as exc:
        raise IndexHistoryError(f"Cannot load {spec.code} history: {type(exc).__name__}") from exc
