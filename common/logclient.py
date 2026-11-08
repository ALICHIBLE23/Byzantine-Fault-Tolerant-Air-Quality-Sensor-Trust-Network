"""Logging setup: every service logs to stdout AND ships logs to the central logging service.

Shipping happens on a background thread with a bounded queue, so logging never blocks or slows
the service. If the logging service is down, records are retried with backoff, and dropped (and
counted) only if the queue fills up.
"""
from __future__ import annotations

import json
import logging
import queue
import sys
import threading
import time
import urllib.request

from common.config import Config


class HttpLogHandler(logging.Handler):
    def __init__(self, url: str, service: str, instance: str, batch_size: int = 200,
                 flush_interval: float = 1.0, max_queue: int = 10_000):
        super().__init__()
        self.url = url
        self.service = service
        self.instance = instance
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.dropped = 0
        self._queue: queue.Queue[dict] = queue.Queue(maxsize=max_queue)
        self._thread = threading.Thread(target=self._run, name="log-shipper", daemon=True)
        self._thread.start()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._queue.put_nowait({
                "ts": record.created,
                "service": self.service,
                "instance": self.instance,
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
            })
        except queue.Full:
            self.dropped += 1
        except Exception:  # never let logging crash the service
            self.handleError(record)

    def _run(self) -> None:
        batch: list[dict] = []
        backoff = 1.0
        while True:
            deadline = time.monotonic() + self.flush_interval
            while len(batch) < self.batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    batch.append(self._queue.get(timeout=remaining))
                except queue.Empty:
                    break
            if not batch:
                continue
            try:
                self._post(batch)
                batch = []
                backoff = 1.0
            except Exception:
                time.sleep(backoff)          # keep the batch and retry later
                backoff = min(backoff * 2, 30.0)

    def _post(self, batch: list[dict]) -> None:
        req = urllib.request.Request(
            self.url, data=json.dumps(batch).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            resp.read()


def mqtt_logger() -> logging.Logger:
    """Logger for the MQTT client library. It warns on every burst of in-flight publishes,
    which is normal backpressure at QoS 1 with hundreds of sensors, so only errors are kept."""
    log = logging.getLogger("trustmesh.mqtt")
    log.setLevel(logging.ERROR)
    return log


def setup_logging(cfg: Config, ship: bool = True) -> logging.Logger:
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s [%(name)s] %(message)s"))
    root.addHandler(stdout)

    if ship and cfg.log_url:
        shipper = HttpLogHandler(cfg.log_url, cfg.service_name, cfg.instance_id)
        shipper.setFormatter(logging.Formatter("%(message)s"))
        root.addHandler(shipper)

    for noisy in ("uvicorn.access", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger(cfg.service_name)
