"""Descriptive volume-at-close profile for completed HK trading sessions.

Every bar contributes incremental volume at its close; this is an approximation
of traded volume by price, not tick-level trade reconstruction. When a decay
half-life is configured, older sessions contribute geometrically less weight,
reflecting that stale volume nodes lose relevance. KDE peak widths are
half-prominence zones, never confidence intervals. ``stable`` only tests peak
sensitivity to bandwidths 0.8 and 1.2 times the configured value. The value
area (VA) is the price band holding the configured share of (decayed) volume,
built outward from the POC bin, and is a descriptive convention from market
profile analysis, not a statistical interval. The caller must exclude
incomplete sessions; no clock or future data is consulted here. The KDE grid
extends beyond observed prices to detect modes at observed extrema; reported
zones are clipped to the observed positive-volume price range.
"""

from dataclasses import dataclass
from typing import Optional, Tuple
from numbers import Real

import numpy as np
import pandas as pd
from scipy.signal import find_peaks, peak_widths
from scipy.stats import gaussian_kde


class VolumeProfileError(ValueError):
    """Data cannot support the requested volume profile."""


@dataclass(frozen=True)
class VolumeZone:
    lower: float
    upper: float
    center: float
    volume_share: float
    stable: bool


@dataclass(frozen=True)
class VolumeProfile:
    bin_edges: np.ndarray
    volume_shares: np.ndarray
    kde_prices: np.ndarray
    kde_density: np.ndarray
    poc: float
    supports: Tuple[VolumeZone, ...]
    resistances: Tuple[VolumeZone, ...]
    start: pd.Timestamp
    end: pd.Timestamp
    sessions: int
    bar_count: int
    value_area_low: Optional[float]
    value_area_high: Optional[float]
    decay_halflife: Optional[float]


def _peaks(density, separation, prominence):
    return find_peaks(density, distance=separation,
                      prominence=prominence * float(density.max()))[0]


def _value_area(histogram, edges, fraction):
    """Market-profile value area: expand from the POC bin to the volume share."""
    total = float(histogram.sum())
    if total <= 0 or len(histogram) < 1:
        return None
    target = fraction * total
    low = high = int(np.argmax(histogram))
    accumulated = float(histogram[low])
    while accumulated < target and (low > 0 or high < len(histogram) - 1):
        below = float(histogram[low - 1]) if low > 0 else -1.0
        above = float(histogram[high + 1]) if high < len(histogram) - 1 else -1.0
        if above >= below:
            high += 1
            accumulated += max(above, 0.0)
        else:
            low -= 1
            accumulated += max(below, 0.0)
    return float(edges[low]), float(edges[high + 1])


def analyze_volume_profile(bars: pd.DataFrame, *, lookback_sessions=60,
                           bins=48, bandwidth=0.2, prominence=0.1,
                           decay_halflife=None, value_area=0.70) -> VolumeProfile:
    """Find up to two nearby volume zones on either side of the latest close.

    ``bandwidth`` is scipy KDE's dimensionless covariance factor, not a price
    distance. ``prominence`` is a fraction of maximum density. The lookback
    counts observed HK calendar dates; missing sessions/volume are not filled.
    ``decay_halflife`` is in observed sessions; ``None`` weights every session
    equally. ``value_area`` is the target share of (decayed) volume for the
    value-area band. Inputs must already be unique, ordered, timezone-aware
    completed bars with positive close and nonnegative, incremental volume.
    Latest close includes zero-volume bars, but they contribute no profile mass.
    """
    if (isinstance(lookback_sessions, bool) or not isinstance(lookback_sessions, int)
            or not 20 <= lookback_sessions <= 252):
        raise VolumeProfileError("lookback_sessions must be an integer from 20 to 252.")
    if isinstance(bins, bool) or not isinstance(bins, int) or not 8 <= bins <= 256:
        raise VolumeProfileError("bins must be an integer from 8 to 256.")
    for name, value, lower, upper in (("bandwidth", bandwidth, 0.01, 2.0),
                                      ("prominence", prominence, 0.001, 1.0)):
        if (isinstance(value, bool) or not isinstance(value, Real)
                or not np.isfinite(value) or not lower <= value <= upper):
            raise VolumeProfileError("{} must be between {} and {}.".format(name, lower, upper))
    if decay_halflife is not None and (isinstance(decay_halflife, bool) or not isinstance(decay_halflife, Real)
                                       or not np.isfinite(decay_halflife) or not 2.0 <= decay_halflife <= 252.0):
        raise VolumeProfileError("decay_halflife must be a number from 2 to 252 sessions, or null.")
    if (isinstance(value_area, bool) or not isinstance(value_area, Real)
            or not np.isfinite(value_area) or not 0.5 < value_area <= 1.0):
        raise VolumeProfileError("value_area must be between 0.5 and 1.0.")
    if not isinstance(bars, pd.DataFrame) or not {"close", "volume"}.issubset(bars.columns):
        raise VolumeProfileError("Expected a DataFrame with close and volume columns.")
    if (not isinstance(bars.index, pd.DatetimeIndex) or bars.index.tz is None
            or bars.index.hasnans or not bars.index.is_unique
            or not bars.index.is_monotonic_increasing):
        raise VolumeProfileError("Bar timestamps must be unique, ordered and timezone-aware.")
    if not bars.columns.is_unique:
        raise VolumeProfileError("Bar columns must be unique.")
    if any(isinstance(value, (bool, np.bool_)) or isinstance(value, complex)
           for value in bars[["close", "volume"]].to_numpy().flat):
        raise VolumeProfileError("Close and volume must be real numeric values, not booleans.")
    try:
        values = bars[["close", "volume"]].to_numpy(dtype=float)
    except (TypeError, ValueError) as exc:
        raise VolumeProfileError("Close and volume must be numeric.") from exc
    if (not np.isfinite(values).all() or (values[:, 0] <= 0).any()
            or (values[:, 1] < 0).any()):
        raise VolumeProfileError("Close must be positive and volume nonnegative; both must be finite.")
    dates = bars.index.tz_convert("Asia/Hong_Kong").normalize()
    sessions = dates.unique()
    if len(sessions) < lookback_sessions:
        raise VolumeProfileError("Need {} completed observed sessions for volume analysis.".format(lookback_sessions))
    selected = dates >= sessions[-lookback_sessions]
    selected_index = bars.index[selected].tz_convert("Asia/Hong_Kong")
    prices, volumes = values[selected].T
    latest = float(prices[-1])
    if decay_halflife is not None:
        window_sessions = sessions[-lookback_sessions:]
        position = {session: i for i, session in enumerate(window_sessions)}
        age = np.array([len(window_sessions) - 1 - position[d] for d in dates[selected]])
        volumes = volumes * np.power(0.5, age / float(decay_halflife))
    active = volumes > 0
    prices_active, volumes_active = prices[active], volumes[active]
    if len(prices_active) < 2 or np.ptp(prices_active) <= 0:
        raise VolumeProfileError("Need positive volume at at least two distinct prices.")
    # Normalize without overflowing the total if vendor volume units are large.
    weights = volumes_active / volumes_active.max()
    weights /= weights.sum()
    if 1.0 / np.square(weights).sum() <= 1.0 + 1e-10:
        raise VolumeProfileError("Volume is too concentrated to estimate a density.")
    try:
        kde = gaussian_kde(prices_active, weights=weights, bw_method=bandwidth)
        price_min, price_max = float(prices_active.min()), float(prices_active.max())
        radius = float(np.sqrt(kde.covariance[0, 0]))
        grid = np.linspace(price_min - 4 * radius, price_max + 4 * radius, 512)
        density = kde(grid)
    except (ValueError, np.linalg.LinAlgError) as exc:
        raise VolumeProfileError("Volume density could not be estimated.") from exc
    if not np.isfinite(density).all() or density.max() <= 0:
        raise VolumeProfileError("Volume density is invalid.")
    histogram, edges = np.histogram(prices_active, bins=bins, weights=weights)
    poc = float((edges[np.argmax(histogram)] + edges[np.argmax(histogram) + 1]) / 2)
    value_area_bounds = _value_area(histogram, edges, float(value_area))
    separation = max(1, int(np.ceil(radius / (grid[1] - grid[0]))))
    peaks = _peaks(density, separation, prominence)
    perturbed_peaks = []
    for scale in (0.8, 1.2):
        other_density = gaussian_kde(prices_active, weights=weights,
                                     bw_method=bandwidth * scale)(grid)
        other = _peaks(other_density, max(1, int(np.ceil(separation * scale))), prominence)
        perturbed_peaks.append(grid[other])
    zones = []
    if len(peaks):
        _, _, left, right = peak_widths(density, peaks, rel_height=0.5)
        for peak, left_index, right_index in zip(peaks, left, right):
            lower = float(np.interp(left_index, np.arange(len(grid)), grid))
            upper = float(np.interp(right_index, np.arange(len(grid)), grid))
            lower = max(price_min, lower)
            upper = min(price_max, upper)
            if lower >= upper:
                continue
            center = float(np.clip(grid[peak], lower, upper))
            tolerance = max(radius, (upper - lower) / 2)
            stable = all(np.any(np.abs(other - center) <= tolerance)
                         for other in perturbed_peaks)
            share = float(weights[(prices_active >= lower) & (prices_active <= upper)].sum())
            zones.append(VolumeZone(lower, upper, center, share, stable))
    supports = tuple(sorted((z for z in zones if z.upper < latest),
                            key=lambda z: latest - z.upper)[:2])
    resistances = tuple(sorted((z for z in zones if z.lower > latest),
                               key=lambda z: z.lower - latest)[:2])
    area_low, area_high = value_area_bounds if value_area_bounds else (None, None)
    return VolumeProfile(edges, histogram, grid, density, poc, supports, resistances,
                         selected_index[0], selected_index[-1], lookback_sessions,
                         len(selected_index), area_low, area_high,
                         float(decay_halflife) if decay_halflife is not None else None)
