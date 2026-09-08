"""Shared configuration, atomic research snapshots, and /recommend formatting."""

from dataclasses import asdict, dataclass
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
    refresh_seconds: int = 1800
    max_cache_age_seconds: int = 7200

    @property
    def fingerprint(self):
        contract = {"config": asdict(self), "model": MODEL_VERSION,
                    "source": "yahoo-daily-index-v1", "session_delay_minutes": 30}
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
    watchlist = tuple(x.upper() for x in watchlist)
    if len(set(watchlist)) != len(watchlist) or not set(watchlist).issubset(INDICES):
        raise ValueError("Watchlist supports unique HSI, N225, NDX, SPX, DJI codes.")
    values["watchlist"] = watchlist
    config = RecommendationConfig(**values)
    for field, low, high in [("horizon", 5, 5), ("min_train", 120, 504),
                             ("min_trades", 20, 200), ("refresh_seconds", 300, 86400),
                             ("max_cache_age_seconds", 600, 172800)]:
        value = getattr(config, field)
        if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
            raise ValueError(f"{field} must be an integer in [{low}, {high}].")
    for field, low, high in [("cost_bps", 0, 1000), ("probability_threshold", 0.5, 0.95)]:
        value = getattr(config, field)
        if isinstance(value, bool) or not isinstance(value, Real) or not math.isfinite(value) or not low <= value <= high:
            raise ValueError(f"{field} must be numeric in [{low}, {high}].")
    if config.max_cache_age_seconds < config.refresh_seconds:
        raise ValueError("Cache age must be at least the refresh interval.")
    return config


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


def run_scan(config, store, *, now=None, fetcher=fetch_index_history,
             evaluator=evaluate_recommendation, should_stop=lambda: False):
    started = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if started.tzinfo is None:
        raise ValueError("Scan time must be timezone-aware.")
    try:
        previous = store.read()
    except (sqlite3.Error, ValueError):
        previous = None
    old_results = previous.get("results", {}) if previous and previous.get("config_hash") == config.fingerprint else {}
    results = {}
    for code in config.watchlist:
        if should_stop():
            return None
        try:
            closes = fetcher(code, now=started)
            data_hash = hashlib.sha256(closes.index.asi8.tobytes() + closes.to_numpy(dtype="float64").tobytes()).hexdigest()
            old = old_results.get(code, {})
            if old.get("input_hash") == data_hash and old.get("model_version") == MODEL_VERSION:
                result = dict(old)
            else:
                result = evaluator(closes, cost_bps=config.cost_bps, horizon=config.horizon,
                                   min_train=config.min_train, min_trades=config.min_trades,
                                   probability_threshold=config.probability_threshold)
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
    completed = pd.Timestamp.now(tz="UTC") if now is None else started
    snapshot = {"config_hash": config.fingerprint, "started_at": started.isoformat(),
                "completed_at": completed.isoformat(), "results": results}
    store.write(snapshot)
    return snapshot


def _percent(value):
    return "n/a" if value is None else f"{value:.2%}"


def recommendation_message(code=None, *, config_path=None, store=None, now=None):
    """Read cached results only; Telegram requests never trigger market scans."""
    config = load_config(config_path)
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
