"""LogIQ log simulator ("the spammer").

Streams mock JSON logs from 14 microservices into Kafka.

    python -m producer.logger                         # steady traffic, ~200 logs/s
    python -m producer.logger --crash UserDB          # steady traffic + a cascade after 10s
    python -m producer.logger --crash UserDB --crash-size 2000 --duration 60
    python -m producer.logger --backfill-days 7       # a week of history for the batch layer

While it runs, type a service name (for example `OrderDB`) and press Enter to crash it.
"""

import argparse
import json
import random
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

from logiq import config, templates, topology
from logiq.stream_control import GeneratorHeartbeat


def make_log(service: str, level: str, message: str, ts: datetime, trace_id: str | None = None) -> dict:
    return {
        "id": uuid.uuid4().hex,
        "timestamp": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "service": service,
        "host": f"{service.lower()}-{random.randint(1, 3)}",
        "level": level,
        "message": message,
        "trace_id": trace_id or uuid.uuid4().hex[:16],
    }


def normal_log(rng: random.Random, ts: datetime, error_ratio: float) -> dict:
    service = rng.choice(topology.services())["name"]
    roll = rng.random()
    if roll < error_ratio:
        level, message, _ = templates.sporadic_error(service, rng)
    elif roll < error_ratio + 0.06:
        level, message = "WARN", templates.benign_warning(service, rng)
    else:
        level, message = templates.healthy_message(service, rng)
    return make_log(service, level, message, ts)


def crash_logs(root: str, size: int, rng: random.Random) -> list[dict]:
    """A failure in `root` plus the errors it causes in every service that depends on it."""
    now = datetime.now(timezone.utc)
    logs = []
    for _ in range(size):
        level, message, _ = templates.root_failure(root, rng)
        logs.append(make_log(root, level, message, now))
    # Each hop away from the root logs a bit less (retries, timeouts, circuit breakers).
    share = 0.6
    hop_of = {root: 0}
    for service, failed_dep in topology.blast_radius(root):
        hop_of[service] = hop_of[failed_dep] + 1
        for _ in range(int(size * share ** hop_of[service])):
            level, message, _ = templates.upstream_failure(service, failed_dep, rng)
            logs.append(make_log(service, level, message, now))
    rng.shuffle(logs)
    return logs


class Simulator:
    def __init__(self, rate: int, error_ratio: float, seed: int | None):
        self.rate = rate
        self.error_ratio = error_ratio
        self.rng = random.Random(seed)
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode(),
            linger_ms=20,
            batch_size=64 * 1024,
        )
        self.sent = 0
        self.pending_crashes: list[tuple[str, int]] = []
        self.lock = threading.Lock()

    def send(self, log: dict, topic: str = config.KAFKA_TOPIC) -> None:
        self.producer.send(topic, key=log["service"], value=log)
        self.sent += 1

    def queue_crash(self, service: str, size: int) -> None:
        with self.lock:
            self.pending_crashes.append((service, size))

    def run(self, duration: float | None) -> None:
        started = time.time()
        tick = 0.1
        last_report = started
        last_beat = 0.0
        heartbeat = GeneratorHeartbeat("stream", self.rate)
        while duration is None or time.time() - started < duration:
            # Once a second: tell the dashboard we are alive, and stop if it asked us to.
            if time.time() - last_beat >= 1:
                last_beat = time.time()
                if heartbeat.beat(self.sent):
                    print("[logger] stopped from the dashboard", flush=True)
                    break
            t0 = time.time()
            now = datetime.now(timezone.utc)
            for _ in range(max(1, int(self.rate * tick))):
                self.send(normal_log(self.rng, now, self.error_ratio))
            with self.lock:
                crashes, self.pending_crashes = self.pending_crashes, []
            for service, size in crashes:
                burst = crash_logs(service, size, self.rng)
                for log in burst:
                    self.send(log)
                print(f"[crash] {service} failed: pushed {len(burst)} error logs "
                      f"across {len({l['service'] for l in burst})} services", flush=True)
            if time.time() - last_report >= 5:
                print(f"[logger] {self.sent} logs sent", flush=True)
                last_report = time.time()
            time.sleep(max(0.0, tick - (time.time() - t0)))
        heartbeat.close()
        self.producer.flush()
        print(f"[logger] done, {self.sent} logs sent", flush=True)

    def backfill(self, days: int, per_hour: int) -> None:
        """Historical logs with past timestamps, sent to the batch-only topic."""
        end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        names = [s["name"] for s in topology.services()]
        for hour in range(days * 24, 0, -1):
            base = end - timedelta(hours=hour)
            # Busier during the day, one bad hour per day.
            load = per_hour * (1.5 if 9 <= base.hour <= 18 else 0.6)
            error_ratio = 0.25 if base.hour == (hour // 24 * 7) % 24 else self.error_ratio
            for _ in range(int(load)):
                ts = base + timedelta(seconds=self.rng.uniform(0, 3600))
                self.send(normal_log(self.rng, ts, error_ratio), config.KAFKA_BACKFILL_TOPIC)
            if base.hour == 0:
                print(f"[backfill] {base:%Y-%m-%d} done ({self.sent} logs)", flush=True)
        # A few historical crashes, so the weekly chart has real spikes.
        for day in range(1, days, 2):
            root = self.rng.choice(names)
            ts = end - timedelta(days=day, hours=self.rng.randint(0, 23))
            for log in crash_logs(root, 300, self.rng):
                log["timestamp"] = (ts + timedelta(seconds=self.rng.uniform(0, 120))).isoformat(
                    timespec="milliseconds").replace("+00:00", "Z")
                self.send(log, config.KAFKA_BACKFILL_TOPIC)
        self.producer.flush()
        print(f"[backfill] done, {self.sent} logs sent to {config.KAFKA_BACKFILL_TOPIC}", flush=True)


def read_commands(sim: Simulator, size: int) -> None:
    names = {s["name"].lower(): s["name"] for s in topology.services()}
    for line in sys.stdin:
        name = names.get(line.strip().lower())
        if name:
            sim.queue_crash(name, size)
        elif line.strip():
            print(f"[logger] unknown service. Pick one of: {', '.join(names.values())}", flush=True)


def main() -> None:
    names = [s["name"] for s in topology.services()]
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rate", type=int, default=200, help="normal logs per second")
    parser.add_argument("--error-ratio", type=float, default=0.01, help="share of normal logs that are sporadic errors")
    parser.add_argument("--duration", type=float, help="stop after N seconds (default: run forever)")
    parser.add_argument("--crash", choices=names, help="service to crash")
    parser.add_argument("--crash-after", type=float, default=10, help="seconds before the crash")
    parser.add_argument("--crash-size", type=int, default=1000, help="error logs from the failing service")
    parser.add_argument("--backfill-days", type=int, help="send N days of history to the batch topic and exit")
    parser.add_argument("--backfill-per-hour", type=int, default=400)
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()

    sim = Simulator(args.rate, args.error_ratio, args.seed)
    if args.backfill_days:
        sim.backfill(args.backfill_days, args.backfill_per_hour)
        return
    if args.crash:
        threading.Timer(args.crash_after, sim.queue_crash, (args.crash, args.crash_size)).start()
        print(f"[logger] {args.crash} will crash in {args.crash_after:.0f}s", flush=True)
    threading.Thread(target=read_commands, args=(sim, args.crash_size), daemon=True).start()
    print(f"[logger] streaming ~{args.rate} logs/s to {config.KAFKA_TOPIC}. "
          "Type a service name + Enter to crash it.", flush=True)
    try:
        sim.run(args.duration)
    except KeyboardInterrupt:
        sim.producer.flush()


if __name__ == "__main__":
    main()
