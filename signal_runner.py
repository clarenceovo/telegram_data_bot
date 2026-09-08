"""Persistent recommendation worker. Run once for verification or loop as a service."""

import argparse
import fcntl
import logging
import signal
import threading

from recommendation_service import CONFIG_PATH, RecommendationStore, load_config, run_scan


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logger = logging.getLogger(__name__)
    store = RecommendationStore()
    store.path.parent.mkdir(parents=True, exist_ok=True)
    stopped = threading.Event()
    for event in (signal.SIGTERM, signal.SIGINT):
        signal.signal(event, lambda *_: stopped.set())
    with store.path.with_suffix(".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("Another recommendation runner already owns this cache.")
        while not stopped.is_set():
            interval = 1800
            try:
                config = load_config(args.config)
                interval = config.refresh_seconds
                snapshot = run_scan(config, store, should_stop=stopped.is_set)
                if snapshot:
                    logger.info("Scan saved: %d indices", len(snapshot["results"]))
            except Exception:
                logger.exception("Recommendation scan failed; existing cache will expire.")
                if args.once:
                    return 1
            if args.once:
                return int(snapshot is None or any(row["status"] == "data_error" for row in snapshot["results"].values()))
            stopped.wait(interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
