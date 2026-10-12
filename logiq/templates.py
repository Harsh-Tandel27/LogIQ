"""Log message templates for the simulator.

Each error template carries the severity label a senior on-call engineer would give it:
High   - the service itself is broken (pool exhausted, OOM, cluster red)
Medium - the service is failing because something it depends on is broken
Low    - isolated bugs that do not threaten availability

The severity model is not trained on these. It learns from real Loghub logs
(spark/prepare_dataset.py); these labels only measure how well it transfers.
"""

import random

from logiq import topology

PATHS = [
    "/api/v1/orders", "/api/v1/cart", "/api/v1/login", "/api/v1/search",
    "/api/v1/products", "/api/v2/checkout", "/api/v1/profile", "/api/v1/payments",
]


def _fields(rng: random.Random, service: str, dep: str = "") -> dict:
    return {
        "svc": service,
        "dep": dep,
        "ms": rng.choice([rng.randint(20, 400), rng.randint(1000, 30000)]),
        "n": rng.randint(2, 500),
        "pct": rng.randint(41, 99),
        "req": f"req-{rng.randint(100000, 999999)}",
        "order": f"ord_{rng.randint(10000, 99999)}",
        "user": f"u_{rng.randint(1000, 9999)}",
        "ip": f"10.0.{rng.randint(0, 9)}.{rng.randint(2, 254)}",
        "port": rng.choice([5432, 6379, 9200, 8080, 27017]),
        "path": rng.choice(PATHS),
        "line": rng.randint(12, 480),
    }


HEALTHY = [
    ("INFO", "GET {path} 200 in {ms}ms"),
    ("INFO", "POST {path} 201 in {ms}ms"),
    ("INFO", "User {user} logged in from {ip}"),
    ("INFO", "Health check OK ({ms}ms)"),
    ("INFO", "Processed {n} messages from queue"),
    ("DEBUG", "Cache hit for key {req}"),
    ("DEBUG", "Opened connection {n} to {ip}:{port}"),
]

BENIGN_WARNINGS = [
    "Slow request {path} took {ms}ms",
    "GC pause {ms}ms (young gen)",
    "Retrying request {req} (attempt 1/3)",
    "Connection pool at {pct}% capacity",
    "Rate limit close for client {user}: {n} requests in 1m",
]

# Failures that start in the service itself, keyed by service kind.
ROOT_FAILURES = {
    "db": [
        ("FATAL", "Connection pool exhausted on {svc}: {n}/{n} connections busy, waited {ms}ms", "High"),
        ("ERROR", "Query timeout on {svc} after {ms}ms: could not acquire lock on table rows", "High"),
        ("ERROR", "Query timed out on {svc} after {ms}ms: could not acquire lock on table rows (retry)", "High"),
    ],
    "cache": [
        ("ERROR", "{svc} READONLY You can't write against a read only replica {ip}:6379", "High"),
        ("ERROR", "{svc} eviction storm: {n} keys evicted in 1s, memory at {pct}%", "High"),
    ],
    "search": [
        ("ERROR", "{svc} cluster health RED: {n} unassigned primary shards", "High"),
        ("ERROR", "Search rejected by {svc}: thread pool queue full ({n} pending)", "High"),
    ],
    "external": [
        ("ERROR", "{svc} returned HTTP 503 Service Unavailable for charge {order}", "High"),
        ("ERROR", "TLS handshake to {svc} {ip}:443 timed out after {ms}ms", "High"),
    ],
    "api": [
        ("FATAL", "OutOfMemoryError: Java heap space in {svc} worker thread {n}", "High"),
        ("ERROR", "{svc} health check failed 3 times, instance {ip} removed from pool", "High"),
    ],
    "gateway": [
        ("FATAL", "{svc} worker process {n} exited on signal 11 (segmentation fault)", "High"),
        ("ERROR", "{svc} TLS certificate expired, rejecting all HTTPS connections", "High"),
    ],
    "client": [
        ("ERROR", "{svc} bundle failed to load: ChunkLoadError main.{n}.js", "High"),
    ],
}

# How a caller reports a broken dependency.
UPSTREAM_FAILURES = {
    "default": [
        ("ERROR", "Upstream {dep} request failed: 504 Gateway Timeout after {ms}ms (req_id={req})", "Medium"),
        ("ERROR", "Call to {dep} failed: connection refused {ip}:{port}, circuit breaker OPEN", "Medium"),
    ],
    "client": [
        ("ERROR", "502 Bad Gateway from {dep} for {path} (req_id={req})", "Medium"),
    ],
}

SPORADIC = [
    ("ERROR", "NullPointerException in CouponService.apply at line {line}", "Low"),
    ("ERROR", "Validation failed for field email on {path}: invalid format", "Low"),
    ("ERROR", "Failed to send welcome email to {user}: SMTP 421 try again later", "Low"),
    ("ERROR", "Thumbnail generation failed for product {order}: unsupported image format", "Low"),
    ("ERROR", "Deprecated endpoint {path} called by client {user}", "Low"),
    ("ERROR", "Webhook signature mismatch for event {req}, payload discarded", "Medium"),
]

# Extra labelled phrasings the simulator never sends. They widen the transfer
# check in spark/evaluate_model.py beyond the simulator's exact wording.
TRAINING_ONLY = [
    ("Disk full on {svc}: no space left on device, writes failing", "High"),
    ("Kernel OOM killer terminated {svc} process {n}", "High"),
    ("Primary node of {svc} unreachable, failover did not complete", "High"),
    ("Database {svc} is down: too many connections", "High"),
    ("Deadlock detected on {svc}, transaction rolled back after {ms}ms", "High"),
    ("Service {svc} crashed with fatal exception and restarted", "High"),
    ("Timeout waiting for response from {dep} after {ms}ms", "Medium"),
    ("Dependency {dep} unavailable, serving degraded response for {path}", "Medium"),
    ("HTTP 503 from upstream {dep}, retries exhausted (req_id={req})", "Medium"),
    ("Socket hang up while calling {dep} {ip}:{port}", "Medium"),
    ("Circuit breaker for {dep} tripped after {n} failures", "Medium"),
    ("Invalid coupon code submitted by {user}", "Low"),
    ("Missing optional header X-Client-Version on {path}", "Low"),
    ("Failed to parse user agent string for {user}", "Low"),
    ("Analytics event dropped: queue full, {n} events lost", "Low"),
    ("Password reset link expired for {user}", "Low"),
]


def _render(fmt: str, rng: random.Random, service: str, dep: str = "") -> str:
    return fmt.format(**_fields(rng, service, dep))


def healthy_message(service: str, rng: random.Random) -> tuple[str, str]:
    level, fmt = rng.choice(HEALTHY)
    return level, _render(fmt, rng, service)


def benign_warning(service: str, rng: random.Random) -> str:
    return _render(rng.choice(BENIGN_WARNINGS), rng, service)


def benign_warnings(service: str, rng: random.Random) -> list[str]:
    return [_render(fmt, rng, service) for fmt in BENIGN_WARNINGS]


def sporadic_error(service: str, rng: random.Random) -> tuple[str, str, str]:
    level, fmt, severity = rng.choice(SPORADIC)
    return level, _render(fmt, rng, service), severity


def root_failure(service: str, rng: random.Random) -> tuple[str, str, str]:
    level, fmt, severity = rng.choice(ROOT_FAILURES[topology.kind_of(service)])
    return level, _render(fmt, rng, service), severity


def upstream_failure(service: str, dep: str, rng: random.Random) -> tuple[str, str, str]:
    kind = "client" if topology.kind_of(service) == "client" else "default"
    level, fmt, severity = rng.choice(UPSTREAM_FAILURES[kind])
    return level, _render(fmt, rng, service, dep), severity


def training_rows(rng: random.Random, per_template: int = 60) -> list[tuple[str, str, int]]:
    """(message, severity, template_id) rows covering every error template and service.

    Used to check how the Loghub-trained model transfers to the simulator's messages.
    """
    names = [s["name"] for s in topology.services()]
    formats = [(fmt, sev) for group in ROOT_FAILURES.values() for _lvl, fmt, sev in group]
    formats += [(fmt, sev) for group in UPSTREAM_FAILURES.values() for _lvl, fmt, sev in group]
    formats += [(fmt, sev) for _lvl, fmt, sev in SPORADIC]
    formats += TRAINING_ONLY
    rows = []
    for _ in range(per_template):
        for template_id, (fmt, severity) in enumerate(formats):
            rows.append((_render(fmt, rng, rng.choice(names), rng.choice(names)), severity, template_id))
    rng.shuffle(rows)
    return rows
