"""Render current regime diagnostics without pyplot's shared figure state."""

import io
import math
from typing import Optional, Tuple

from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
import matplotlib.dates as mdates
import pandas as pd

from analytics.regime import RegimeResult
from analytics.volume_profile import VolumeProfile


def _ratio(value: float) -> str:
    if math.isinf(value):
        return "infinity"
    if value != 0 and (value < 0.01 or value >= 10000):
        return "{:.2e}".format(value)
    return "{:.2f}".format(value)


def render_regime_report(
    symbol: str, closes: pd.Series, result: RegimeResult, *, now: pd.Timestamp,
    profile: Optional[VolumeProfile] = None, volume_error: Optional[str] = None,
    volume_mode: str = "per_bar"
) -> Tuple[bytes, str]:
    """Return a PNG and a Telegram-sized caption; times display in Hong Kong."""
    current = pd.Timestamp(now).tz_convert("Asia/Hong_Kong").normalize()
    dates = closes.index.tz_convert("Asia/Hong_Kong").tz_localize(None)
    fig = Figure(figsize=(12, 9 if profile is not None else 7), dpi=130, facecolor="white")
    FigureCanvasAgg(fig)
    heights = [3.3, 1.1, 1.4] if profile is not None else [3, 1.2]
    grid = fig.add_gridspec(len(heights), 2, height_ratios=heights,
                           width_ratios=[4, 1], hspace=0.68, wspace=0.08)
    price_ax = fig.add_subplot(grid[0, 0] if profile is not None else grid[0, :])
    price_ax.plot(dates, closes.to_numpy(), color="#234e70", linewidth=1.7)
    price_ax.set_title("{} | Closing prices | Past year".format(symbol), loc="left", fontsize=15)
    price_ax.set_ylabel("Price (source units)")
    price_ax.set_xlim((current - pd.DateOffset(years=1)).tz_localize(None),
                      current.tz_localize(None))
    price_ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    price_ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    price_ax.grid(alpha=0.2)
    price_ax.text(0, -0.24, "{} observed closes | {} to {} | Hong Kong dates".format(
        len(closes), dates[0].strftime("%Y-%m-%d"), dates[-1].strftime("%Y-%m-%d")),
        transform=price_ax.transAxes, fontsize=9, color="#555555")

    if profile is not None:
        volume_ax = fig.add_subplot(grid[0, 1], sharey=price_ax)
        edges = profile.bin_edges
        volume_ax.barh((edges[:-1] + edges[1:]) / 2,
                       profile.volume_shares * 100, height=edges[1:] - edges[:-1],
                       color="#7295ae", alpha=0.65)
        in_range = (profile.kde_prices >= edges[0]) & (profile.kde_prices <= edges[-1])
        volume_ax.plot(profile.kde_density[in_range] * (edges[1] - edges[0]) * 100,
                       profile.kde_prices[in_range], color="#234e70", linewidth=1.4)
        volume_ax.tick_params(axis="y", labelleft=False, left=False)
        volume_ax.set_xlabel("Volume share (%)", fontsize=9)
        volume_ax.set_title("{} sessions".format(profile.sessions), fontsize=11)
        volume_ax.grid(axis="x", alpha=0.2)
        start = profile.start.tz_convert("Asia/Hong_Kong").tz_localize(None)
        end = profile.end.tz_convert("Asia/Hong_Kong").tz_localize(None)
        price_ax.hlines(profile.poc, start, end, colors="#a97619", linestyles="--", linewidth=1.5)
        volume_ax.axhline(profile.poc, color="#a97619", linestyle="--", linewidth=1.2)
        if profile.value_area_low is not None and profile.value_area_high is not None:
            price_ax.fill_between([start, end], profile.value_area_low, profile.value_area_high,
                                  color="#6f6bb5", alpha=0.10)
            price_ax.hlines([profile.value_area_low, profile.value_area_high], start, end,
                            colors="#6f6bb5", linestyles=":", linewidth=1.3)
            volume_ax.axhline(profile.value_area_low, color="#6f6bb5", linestyle=":", linewidth=1.0)
            volume_ax.axhline(profile.value_area_high, color="#6f6bb5", linestyle=":", linewidth=1.0)
            price_ax.annotate("VA", (end, profile.value_area_high), xytext=(4, 0),
                              textcoords="offset points", color="#6f6bb5", fontsize=8, va="center")
        rows = []
        for prefix, zones, color in [("S", profile.supports, "#278454"),
                                      ("R", profile.resistances, "#b44747")]:
            for number, zone in enumerate(zones, 1):
                price_ax.fill_between([start, end], zone.lower, zone.upper, color=color, alpha=0.17)
                price_ax.annotate(prefix + str(number), (end, zone.center), xytext=(4, 0),
                                  textcoords="offset points", color=color, fontsize=8, va="center")
                rows.append([prefix + str(number), "{:.2f} - {:.2f}".format(zone.lower, zone.upper),
                             "{:.1%}".format(zone.volume_share),
                             "Persists" if zone.stable else "Sensitive"])
        detail_ax = fig.add_subplot(grid[2, :])
        detail_ax.axis("off")
        detail_ax.set_title("Current candidate zones | POC {:.2f} | {} bars | {} to {}".format(
            profile.poc, profile.bar_count, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
            loc="left", fontsize=10, pad=12)
        if rows:
            table = detail_ax.table(cellText=rows, colLabels=["Zone", "Price range", "Volume share", "Bandwidth check"],
                                    cellLoc="center", loc="upper center", colWidths=[0.12, 0.34, 0.22, 0.32])
            table.auto_set_font_size(False)
            table.set_fontsize(9)
            table.scale(1, 1.2)
        else:
            detail_ax.text(0, 0.65, "No separated support/resistance peaks around the latest close.", fontsize=10)
        missing = []
        if not profile.supports:
            missing.append("No support peak")
        if not profile.resistances:
            missing.append("No resistance peak")
        convention = "per-bar volume assumed" if volume_mode == "per_bar" else "daily cumulative volume differenced"
        fig.text(0.12, 0.065, "Bar-close approximation; {}. KDE line smooths the histogram.".format(convention),
                 fontsize=9, color="#555555")
        fig.text(0.12, 0.04, "Zone widths and bandwidth persistence are descriptive, not confidence or holding probabilities."
                 + (" " + "; ".join(missing) + "." if missing else ""), fontsize=8.5, color="#555555")

    probability_ax = fig.add_subplot(grid[1, :])
    low, high = result.bull_probability_interval
    rows = [(1, result.bull_probability, low, high, "#278454"),
            (0, result.bear_probability, 1 - high, 1 - low, "#b44747")]
    for y, probability, lower, upper, color in rows:
        probability_ax.hlines(y, 100 * lower, 100 * upper, color=color, linewidth=5, alpha=0.5)
        probability_ax.plot(100 * probability, y, "o", color=color, markersize=8)
    probability_ax.set_yticks([0, 1])
    probability_ax.set_yticklabels(["Bear: mean < 0", "Bull: mean > 0"])
    probability_ax.set_xlim(-1, 101)
    probability_ax.set_ylim(-0.6, 1.6)
    probability_ax.set_xlabel("Model probability (%) | lines: approximate 95% bootstrap intervals")
    probability_ax.grid(axis="x", alpha=0.2)
    fig.subplots_adjust(left=0.12, right=0.95, top=0.93, bottom=0.14 if profile is not None else 0.12)
    with io.BytesIO() as output:
        fig.savefig(output, format="png")
        photo = output.getvalue()
    if result.regime_half_life is not None:
        persistence = "Persistence: gain {gain:.3f}; shock half-life ≈ {half:.0f} sessions.".format(
            gain=result.kalman_gain, half=result.regime_half_life)
    else:
        persistence = "Persistence: boundary fit (constant mean); no finite half-life."
    caption = (
        "{symbol} regime | as of {asof} HK\n"
        "Input: observed-session ln(close[t] / close[t-1]); Kalman mean.\n"
        "Mean log return: {mean:.3%} (95% state interval [{mean_lo:.3%}, {mean_hi:.3%}])\n"
        "Bull {bull:.1%} | Bear {bear:.1%}\n"
        "Bull/Bear ratio: {ratio}:1\n"
        "Approx. 95% bootstrap interval for ratio: [{lo}, {hi}]\n"
        "{persistence}\n"
        "Bull = positive current mean return; not next-day odds.\n"
        "Interval measures fitted-parameter uncertainty; model-dependent.\n"
        "{success}/{total} bootstrap fits. API daily last prices; adjustments and missing sessions unverified."
    ).format(symbol=symbol, asof=dates[-1].strftime("%Y-%m-%d"),
             bull=result.bull_probability, bear=result.bear_probability,
             ratio=_ratio(result.bull_bear_ratio), lo=_ratio(result.ratio_interval[0]),
             hi=_ratio(result.ratio_interval[1]), success=result.bootstrap_successes,
             total=result.bootstrap_samples, mean=result.estimated_mean_return,
             mean_lo=result.mean_return_interval[0], mean_hi=result.mean_return_interval[1],
             persistence=persistence)
    if symbol == "HK.HSImain":
        caption += " Futures rolls may affect returns."
    if profile is not None:
        caption += "\nVolume: {} sessions; POC {:.2f}.".format(profile.sessions, profile.poc)
        if profile.value_area_low is not None and profile.value_area_high is not None:
            caption += " Value area: [{:.2f}, {:.2f}].".format(
                profile.value_area_low, profile.value_area_high)
        caption += " Candidate S/R zones and volume shares on chart."
        if profile.decay_halflife is not None:
            caption += " Volume decay half-life {:.0f} sessions.".format(profile.decay_halflife)
        else:
            caption += " Equal-weight volume."
        caption += " Per-bar volume assumed." if volume_mode == "per_bar" else " Daily cumulative volume differenced."
    elif volume_error:
        caption += "\nVolume profile unavailable: " + volume_error[:150]
    return photo, caption
