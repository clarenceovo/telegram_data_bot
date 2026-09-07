"""Validated one-year HK history for regime diagnostics.

Naive source timestamps are assumed to be Asia/Hong_Kong event times. Each
completed HK date uses its last observed price; the API does not guarantee an
official or corporate-action-adjusted daily close. Missing dates are never filled.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import re

import numpy as np
import pandas as pd
import requests


HK_TIMEZONE = "Asia/Hong_Kong"


class PriceHistoryError(ValueError):
    """History is unavailable or unsuitable for one-year regime diagnostics."""


def normalize_hk_symbol(symbol: str) -> str:
    """Accept HK equity codes and the existing HSI continuous-future symbol."""
    if not isinstance(symbol, str):
        raise PriceHistoryError("Use an HK ticker such as 700, HK.00700, or HK.HSImain.")
    symbol = symbol.strip()
    if re.fullmatch(r"[0-9]{1,5}", symbol):
        return "HK." + symbol.zfill(5)
    if symbol == "HK.HSImain" or re.fullmatch(r"HK\.[0-9]{5}", symbol):
        return symbol
    raise PriceHistoryError("Use an HK ticker such as 700, HK.00700, or HK.HSImain.")


@dataclass
class PriceHistory:
    """Daily closes and optional event-time bars from the same API response."""

    closes: pd.Series
    bars: pd.DataFrame
    volume_error: Optional[str]


def fetch_price_history(
    base_url: str, symbol: str, *, now: datetime = None
) -> pd.Series:
    """Compatibility wrapper returning only validated daily closes."""
    return fetch_market_history(base_url, symbol, now=now).closes


def fetch_market_history(
    base_url: str, symbol: str, *, now: datetime = None, volume_mode: str = "per_bar"
) -> PriceHistory:
    """Fetch one calendar year ending before today's HK date.

    Return daily closes and original event-time close/volume bars. Volume is
    interpreted explicitly as per-bar increments or cumulative counters that
    reset each HK date. For cumulative counters the first observation supplies
    the first increment; a partial session can therefore misattribute earlier
    volume to its first observed price. No volume semantics are autodetected.
    Invalid volumes disable bars without discarding otherwise valid closes.
    Daily closes have a sorted, unique HK-local midnight DatetimeIndex.
    Require 120 observed dates, coverage starting within 45 days of the requested
    start, and a latest observation no more than seven calendar days old.
    """
    if volume_mode not in ("per_bar", "cumulative"):
        raise PriceHistoryError("volume_mode must be per_bar or cumulative.")
    symbol = normalize_hk_symbol(symbol)
    current = pd.Timestamp.now(tz=HK_TIMEZONE) if now is None else pd.Timestamp(now)
    if pd.isna(current) or current.tzinfo is None:
        raise PriceHistoryError("now must be a valid timezone-aware datetime.")
    today = current.tz_convert(HK_TIMEZONE).normalize()
    start = today - pd.DateOffset(years=1)
    end = today - pd.Timedelta(days=1)
    try:
        response = requests.get(
            base_url.rstrip("/") + "/equity/getTickerHistData",
            params={"ticker": symbol, "startDate": start.strftime("%Y-%m-%d"),
                    "endDate": end.strftime("%Y-%m-%d")},
            timeout=(5, 30),
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise PriceHistoryError("Price history API request failed.") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise PriceHistoryError("Price history API must return a data list.")

    observations = []
    for record in payload["data"]:
        if not isinstance(record, dict) or "time" not in record or "close" not in record:
            raise PriceHistoryError("Each history record must include time and close.")
        try:
            # Numeric timestamps have unspecified units and cannot be interpreted safely.
            if not isinstance(record["time"], (str, datetime, pd.Timestamp)):
                raise ValueError("Timestamp must have explicit date semantics")
            timestamp = pd.Timestamp(record["time"])
            if pd.isna(timestamp):
                raise ValueError("Missing timestamp")
            timestamp = (timestamp.tz_localize(HK_TIMEZONE) if timestamp.tzinfo is None
                         else timestamp.tz_convert(HK_TIMEZONE))
            if isinstance(record["close"], bool):
                raise ValueError("Boolean price")
            close = float(record["close"])
            if not np.isfinite(close) or close <= 0:
                raise ValueError("Invalid close")
        except (ValueError, TypeError, OverflowError) as exc:
            raise PriceHistoryError("History contains an invalid timestamp or nonpositive/nonfinite close.") from exc
        observations.append((timestamp, close, record.get("volume")))

    if not observations:
        raise PriceHistoryError("No price history is available.")
    frame = pd.DataFrame(observations, columns=["time", "close", "volume"])
    if (frame.groupby("time")["close"].nunique() > 1).any():
        raise PriceHistoryError("History has conflicting closes at the same timestamp.")
    frame = frame.sort_values("time")
    frame = frame[(frame["time"] >= start) & (frame["time"] < today)]
    if frame.empty:
        raise PriceHistoryError("No completed dates are available in the requested year.")
    frame["date"] = frame["time"].dt.normalize()
    closes = frame.groupby("date")["close"].last().rename("close")
    if len(closes) < 120:
        raise PriceHistoryError("At least 120 observed daily closes are required.")
    if closes.index[0] > start + pd.Timedelta(days=45):
        raise PriceHistoryError("Insufficient one-year coverage: history starts more than 45 days late.")
    if today - closes.index[-1] > pd.Timedelta(days=7):
        raise PriceHistoryError("Price history is stale: latest close is more than seven days old.")
    empty_bars = pd.DataFrame(
        columns=["close", "volume"],
        index=pd.DatetimeIndex([], tz=HK_TIMEZONE, name="time"), dtype=float,
    )
    volumes = []
    for value in frame["volume"]:
        try:
            if isinstance(value, (bool, np.bool_)):
                raise ValueError("Boolean volume")
            volume = float(value)
            if not np.isfinite(volume) or volume < 0:
                raise ValueError("Invalid volume")
        except (ValueError, TypeError, OverflowError):
            return PriceHistory(closes, empty_bars,
                                "Volume is missing, negative, nonfinite, or invalid.")
        volumes.append(volume)
    frame["volume"] = volumes
    if (frame.groupby("time")["volume"].nunique() > 1).any():
        return PriceHistory(closes, empty_bars,
                            "History has conflicting volumes at the same timestamp.")
    frame = frame.drop_duplicates("time")
    if volume_mode == "cumulative":
        increments = frame.groupby("date")["volume"].diff()
        if (increments.dropna() < 0).any():
            return PriceHistory(closes, empty_bars,
                                "Cumulative volume decreases within an HK date.")
        frame["volume"] = increments.fillna(frame["volume"])
    return PriceHistory(closes, frame.set_index("time")[["close", "volume"]], None)
