"""Log simulator driven from the dashboard.

Same generator as producer/logger.py, but controlled over HTTP: background
traffic, service crashes and hand-written logs, all written to Kafka so they
flow through the real Spark pipeline.
"""

import json
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone

from kafka import KafkaProducer

from logiq import config, stream_control, topology
from producer.logger import crash_logs, make_log, normal_log

RECENT_MAX = 50


class DashboardSimulator:
    def __init__(self):
        self._producer: KafkaProducer | None = None
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self.rng = random.Random()
        self.running = False
        self.rate = 200
        self.error_ratio = 0.01
        self.sent = 0
        self.started_at: datetime | None = None
        self.recent: deque[dict] = deque(maxlen=RECENT_MAX)
        self.events: deque[dict] = deque(maxlen=20)

    @property
    def producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode(),
                linger_ms=20,
                batch_size=64 * 1024,
            )
        return self._producer

    def _send(self, logs: list[dict]) -> None:
        for log in logs:
            self.producer.send(config.KAFKA_TOPIC, key=log["service"], value=log)
        with self._lock:
            self.sent += len(logs)
            # Keep a sample of big bursts so the feed stays readable.
            step = max(1, len(logs) // 20)
            self.recent.extendleft(logs[::step][:20])

    def _event(self, text: str) -> None:
        self.events.appendleft({"at": datetime.now(timezone.utc), "event": text})

    # ---- background traffic -------------------------------------------

    def _loop(self) -> None:
        tick = 0.1
        while not self._stop.is_set():
            t0 = time.time()
            now = datetime.now(timezone.utc)
            n = max(1, int(self.rate * tick)) if self.rate > 0 else 0
            if n:
                self._send([normal_log(self.rng, now, self.error_ratio) for _ in range(n)])
            self._stop.wait(max(0.0, tick - (time.time() - t0)))
        self.producer.flush()

    def set_traffic(self, running: bool, rate: int, error_ratio: float) -> None:
        self.rate = max(0, min(rate, 2000))
        self.error_ratio = max(0.0, min(error_ratio, 1.0))
        if running and not self.running:
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, daemon=True, name="dashboard-traffic")
            self._thread.start()
            self.started_at = datetime.now(timezone.utc)
            self._event(f"Traffic started at {self.rate} logs/s")
        elif not running and self.running:
            self._stop.set()
            self._thread = None
            self._event("Traffic stopped")
        self.running = running

    # ---- one-off sends ------------------------------------------------

    def crash(self, service: str, size: int) -> dict:
        logs = crash_logs(service, size, self.rng)
        self._send(logs)
        self.producer.flush()
        per_service: dict[str, int] = {}
        for log in logs:
            per_service[log["service"]] = per_service.get(log["service"], 0) + 1
        self._event(f"Crashed {service}: {len(logs):,} error logs across {len(per_service)} services")
        return {"sent": len(logs), "per_service": per_service}

    def custom(self, service: str, level: str, message: str, repeat: int, vary_ids: bool) -> int:
        now = datetime.now(timezone.utc)
        logs = []
        for _ in range(repeat):
            text = f"{message} (req_id=req-{self.rng.randint(100000, 999999)})" if vary_ids else message
            logs.append(make_log(service, level, text, now))
        self._send(logs)
        self.producer.flush()
        self._event(f"Sent {repeat:,} {level} {'log' if repeat == 1 else 'logs'} from {service}")
        return len(logs)

    def stop_all(self, db) -> int:
        """Stop dashboard traffic and every terminal generator. Returns how many terminal generators were asked."""
        terminals = len(stream_control.running_generators(db))
        stream_control.request_stop(db)
        if self.running:
            self.set_traffic(False, self.rate, self.error_ratio)
        self._event(f"Stopped all streams ({terminals} terminal {'generator' if terminals == 1 else 'generators'})")
        return terminals

    def state(self, db) -> dict:
        with self._lock:
            recent = list(self.recent)
        return {
            "terminal_generators": stream_control.running_generators(db),
            "running": self.running,
            "rate": self.rate,
            "error_ratio": self.error_ratio,
            "sent": self.sent,
            "started_at": self.started_at,
            "recent": recent,
            "events": list(self.events),
            "blast_radius": {s["name"]: [svc for svc, _ in topology.blast_radius(s["name"])] for s in topology.services()},
        }

    def shutdown(self) -> None:
        self._stop.set()
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()


simulator = DashboardSimulator()
