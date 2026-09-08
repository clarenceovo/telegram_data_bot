"""Shared configuration, atomic research snapshots, and /recommend formatting."""

from dataclasses import asdict, dataclass, replace
import hashlib
import json
import logging
import math
from numbers import Real
import os
from pathlib import Path
import sqlite3

import pandas as pd

from analytics.recommendations import MODEL_VERSION, evaluate_recommendation
from analytics.regime_series import regime_probability_series
from api_data_service.index_history import INDICES, fetch_index_history, latest_completed_session, planned_sessions

logger = logging.getLogger(__name__)
CONFIG_PATH = "config/recommendations.json"


@dataclass(frozen=True)
class RecommendationConfig:
    watchlist: tuple = ("HSI", "N225", "NDX", "SPX", "DJI")
    cost_bps: float = 35.0
    horizon: int = 5
    min_train: int = 252
    min_trades: int = 30
    probability_threshold: float = 0.6
    regime_refit_every: int = 21
    regime_gate_probability: float = 0.5
    refresh_seconds: int = 1800
    max_cache_age_seconds: int = 7200
    redis_url: object = None
    watchlist_key: str = "telegram_data_bot:watchlist"

    @property
    def fingerprint(self):
        contract = {"config": asdict(self), "model": MODEL_VERSION,
                    "source": "yahoo-daily-index-ohlc-v1", "session_delay_minutes": 30}
        return hashlib.sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()


def load_config(path=None):
    location = Path(path or os.environ.get("RECOMMEND_CONFIG", CONFIG_PATH))
    values = json.loads(location.read_text())
    if not isinstance(values, dict):
        raise ValueError("Recommendation configuration must be an object.")
    unknown = set(values) - set(RecommendationConfig.__dataclass_fields__)
    if unknown:
        raise ValueError("Unknown recommendation settings: " + ", ".join(sorted(unknown)))
    watchlist = values.get("watchlist", list(INDICES))
    if not isinstance(watchlist, list) or not watchlist or any(not isinstance(x, str) for x in watchlist):
        raise ValueError("watchlist must be a nonempty list of index codes.")
    watchlist = tuple(x.lstrip("^").strip().upper() for x in watchlist)
    if len(set(watchlist)) != len(watchlist) or not set(watchlist).issubset(INDICES):
        raise ValueError("Watchlist supports unique HSI, N225, NDX, SPX, DJI codes.")
    values["watchlist"] = watchlist
    environment = os.environ.get("RECOMMEND_REDIS_URL")
    if environment is not None:
        values["redis_url"] = environment
    config = RecommendationConfig(**values)
    for field, low, high in [("horizon", 5, 5), ("min_train", 120, 504),
                             ("min_trades", 20, 200), ("refresh_seconds", 300, 86400),
                             ("max_cache_age_seconds", 600, 172800),
                             ("regime_refit_every", 5, 63)]:
        value = getattr(config, field)
        if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
            raise ValueError(f"{field} must be an integer in [{low}, {high}].")
    for field, low, high in [("cost_bps", 0, 1000), ("probability_threshold", 0.5, 0.95)]:
        value = getattr(config, field)
        if isinstance(value, bool) or not isinstance(value, Real) or not math.isfinite(value) or not low <= value <= high:
            raise ValueError(f"{field} must be numeric in [{low}, {high}].")
    gate = config.regime_gate_probability
    if gate is not None and (isinstance(gate, bool) or not isinstance(gate, Real)
                             or not math.isfinite(gate) or not 0.5 <= gate <= 0.95):
        raise ValueError("regime_gate_probability must be numeric in [0.5, 0.95] or null.")
    if config.redis_url is not None and (not isinstance(config.redis_url, str) or not config.redis_url
                                         or not config.redis_url.startswith(("redis://", "rediss://", "unix://"))):
        raise ValueError("redis_url must be a redis://, rediss://, or unix:// URL, or null.")
    if not isinstance(config.watchlist_key, str) or not config.watchlist_key.strip():
        raise ValueError("watchlist_key must be a nonempty string.")
    if config.max_cache_age_seconds < config.refresh_seconds:
        raise ValueError("Cache age must be at least the refresh interval.")
    return config


def resolve_runtime_config(config, *, reader=None):
    """Apply the Redis watchlist list when available; otherwise keep the file.

    The Redis key must hold a LIST of supported index codes (``^HSI`` or ``HSI``
    both accepted). An absent key, an empty list, unsupported codes, duplicates,
    or any connection error falls back to the configured file watchlist, so the
    runner never silently scans nothing. Returns the (possibly replaced) config
    and a source label ("redis:<key>" or "file").
    """
    if config.redis_url is None:
        return config, "file"
    if reader is None:
        def default_reader(key):
            import redis  # Optional at import time; required only when enabled.
            client = redis.Redis.from_url(config.redis_url, socket_connect_timeout=2,
                                          socket_timeout=2)
            return client.lrange(key, 0, -1)
        reader = default_reader
    try:
        raw = reader(config.watchlist_key)

        def normalize(item):
            if isinstance(item, bytes):
                item = item.decode("utf-8", "ignore")
            return str(item).lstrip("^").strip().upper()

        codes = tuple(code for code in (normalize(item) for item in raw) if code)
    except Exception as exc:
        logger.warning("Redis watchlist read failed (%s); using file watchlist.", exc)
        return config, "file"
    if not codes:
        return config, "file"
    if len(set(codes)) != len(codes) or not set(codes).issubset(INDICES):
        logger.warning("Redis watchlist %s is invalid (%s); using file watchlist.",
                       config.watchlist_key, codes)
        return config, "file"
    replaced = replace(config, watchlist=codes)
    return replaced, "redis:{}".format(config.watchlist_key)


def database_path():
    return Path(os.environ.get("RECOMMEND_DB", "data/recommendations.sqlite3"))


class RecommendationStore:
    def __init__(self, path=None):
        self.path = Path(path) if path is not None else database_path()

    def write(self, snapshot):
        payload = json.dumps(snapshot, allow_nan=False)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path, timeout=5) as connection:
            # DELETE mode permits a separate process with a read-only volume mount.
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("CREATE TABLE IF NOT EXISTS snapshot (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL)")
            connection.execute("INSERT OR REPLACE INTO snapshot (id,payload) VALUES (1,?)", (payload,))

    def read(self):
        if not self.path.exists():
            return None
        uri = self.path.absolute().as_uri() + "?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=2) as connection:
            row = connection.execute("SELECT payload FROM snapshot WHERE id=1").fetchone()
        return json.loads(row[0]) if row else None


def _input_hash(code, frame, frames):
    """Hash a code's own OHLC plus every other index's closes it consumes."""
    digest = hashlib.sha256()
    digest.update(code.encode())
    digest.update(frame.index.asi8.tobytes())
    digest.update(frame.to_numpy(dtype="float64").tobytes())
    for other in sorted(frames):
        if other == code:
            continue
        closes = frames[other]["close"]
        digest.update(other.encode())
        digest.update(closes.index.asi8.tobytes())
        digest.update(closes.to_numpy(dtype="float64").tobytes())
    return digest.hexdigest()


def run_scan(config, store, *, now=None, fetcher=fetch_index_history,
             evaluator=evaluate_recommendation, regime_series=regime_probability_series,
             should_stop=lambda: False):
    started = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if started.tzinfo is None:
        raise ValueError("Scan time must be timezone-aware.")
    try:
        previous = store.read()
    except (sqlite3.Error, ValueError):
        previous = None
    old_results = previous.get("results", {}) if previous and previous.get("config_hash") == config.fingerprint else {}
    frames = {}
    results = {}
    for code in config.watchlist:
        if should_stop():
            return None
        try:
            frames[code] = fetcher(code, now=started)
        except Exception as exc:
            logger.exception("Recommendation fetch failed for %s", code)
            results[code] = {"status": "data_error", "reasons": [str(exc)[:180]],
                             "checked_at": started.isoformat(), "data_asof": None}
    for code in config.watchlist:
        if code in results or should_stop():
            continue
        try:
            frame = frames[code]
            closes = frame["close"]
            data_hash = _input_hash(code, frame, frames)
            old = old_results.get(code, {})
            if old.get("input_hash") == data_hash and old.get("model_version") == MODEL_VERSION:
                result = dict(old)
            else:
                others = {other: frames[other]["close"] for other in sorted(frames) if other != code}
                probabilities = regime_series(closes, refit_every=config.regime_refit_every)
                result = evaluator(closes, ohlc=frame[["open", "high", "low"]], others=others,
                                   regime=probabilities, cost_bps=config.cost_bps, horizon=config.horizon,
                                   min_train=config.min_train, min_trades=config.min_trades,
                                   probability_threshold=config.probability_threshold,
                                   regime_gate_probability=config.regime_gate_probability)
                result.update(planned_sessions(code, result["data_asof"], config.horizon))
                result["input_hash"] = data_hash
                result["evaluated_at"] = started.isoformat()
            result["checked_at"] = started.isoformat()
            results[code] = result
            logger.info("%s: %s, %s evaluated trades", code, result["status"], result["metrics"]["trade_count"])
        except Exception as exc:
            logger.exception("Recommendation scan failed for %s", code)
            results[code] = {"status": "data_error", "reasons": [str(exc)[:180]],
                             "checked_at": started.isoformat(), "data_asof": None}
    if should_stop():
        return None
    completed = pd.Timestamp.now(tz="UTC") if now is None else started
    snapshot = {"config_hash": config.fingerprint, "started_at": started.isoformat(),
                "completed_at": completed.isoformat(), "results": results}
    store.write(snapshot)
    return snapshot


def _percent(value):
    return "n/a" if value is None else f"{value:.2%}"


def recommendation_message(code=None, *, config_path=None, store=None, now=None,
                           watchlist_reader=None):
    """Read cached results only; Telegram requests never trigger market scans."""
    config = load_config(config_path)
    config, _source = resolve_runtime_config(config, reader=watchlist_reader)
    if code is not None:
        code = code.upper()
        if code not in config.watchlist:
            return "Choose a watched index: " + ", ".join(config.watchlist)
    snapshot = (store or RecommendationStore()).read()
    if not snapshot:
        return "Recommendations are not ready. Start the signal runner and allow its first scan to finish."
    current = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    age = (current - pd.Timestamp(snapshot["completed_at"])).total_seconds()
    if snapshot.get("config_hash") != config.fingerprint:
        return "Recommendation settings changed. Waiting for the runner to refresh the results."
    if age < 0 or age > config.max_cache_age_seconds:
        return "Recommendation cache is stale. Check the signal runner; old ideas are hidden."
    results = snapshot["results"]
    current_results = {}
    for symbol in config.watchlist:
        row = dict(results.get(symbol, {"status": "data_error", "reasons": ["No cached result."]}))
        if row.get("data_asof"):
            if (pd.Timestamp(row["data_asof"]) != latest_completed_session(symbol, now=current)
                    or current >= pd.Timestamp(row["expires_at"])):
                row.update(status="stale", reasons=["New session or entry cutoff passed; awaiting refreshed data."])
        current_results[symbol] = row
    if code:
        selected = [(code, current_results[code])]
    else:
        selected = [(symbol, row) for symbol, row in current_results.items() if row["status"] == "candidate"]
        selected.sort(key=lambda item: (item[1]["metrics"]["mean_net_ci"][0], item[1]["probability"]), reverse=True)
    lines = ["Index research ideas | LONG | 5 sessions",
             "Checked " + pd.Timestamp(snapshot["completed_at"]).tz_convert("Asia/Hong_Kong").strftime("%Y-%m-%d %H:%M HK")]
    if not selected:
        lines.append("No indices currently meet all evidence gates.")
        for symbol, row in current_results.items():
            lines.append(f"{symbol}: {row['status']} — " + "; ".join(row.get("reasons", []))[:260])
    for symbol, row in selected:
        lines.extend(["", f"{symbol}: {row['status']}"])
        if row["status"] in {"data_error", "stale"}:
            lines.extend(row.get("reasons", []))
            continue
        metrics = row["metrics"]
        lines.append("Signal as of " + pd.Timestamp(row["data_asof"]).strftime("%Y-%m-%d") + " (local exchange date)")
        lines.append(f"Model score: {_percent(row['probability'])} (uncalibrated)")
        lines.append(f"Proxy entry: {row['entry_session']} close; exit: {row['exit_session']} close")
        lines.append(f"OOS: {metrics['trade_count']} nonoverlapping trades; hit rate {_percent(metrics['hit_rate'])}")
        interval = metrics["mean_net_ci"]
        lines.append("Average net move: " + _percent(metrics["mean_net_return"]) +
                     (f"; approx. 95% interval [{_percent(interval[0])}, {_percent(interval[1])}]" if interval else ""))
        lines.append("Trade-close drawdown: " + _percent(metrics["max_drawdown"]))
        if metrics["model_brier"] is not None:
            lines.append(f"Brier {metrics['model_brier']:.3f}; baseline {metrics['baseline_brier']:.3f}")
        if metrics["oos_start"]:
            lines.append(f"OOS decision dates: {metrics['oos_start'][:10]} to {metrics['oos_end'][:10]}")
        if row.get("reasons"):
            lines.append("Not qualified: " + "; ".join(row["reasons"]))
    lines.extend(["", f"Index moves less assumed {config.cost_bps:g} bps round-trip cost; not executed ETF/futures returns.",
                  "SPX/NDX/DJI overlap. These are research candidates, not position-aware orders."])
    return "\n".join(lines)
