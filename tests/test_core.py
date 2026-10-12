import random

from logiq import bloom, templates, topology
from logiq.textnorm import template


def test_template_masks_variable_tokens():
    a = template("Upstream UserDB request failed: 504 Gateway Timeout after 2950ms (req_id=req-123456)")
    b = template("Upstream UserDB request failed: 504 Gateway Timeout after 88ms (req_id=req-999999)")
    assert a == b
    assert "UserDB" in a and "<*>" in a


def test_bloom_filter_has_no_false_negatives():
    f = bloom.BloomFilter(capacity=1000, error_rate=0.01)
    keys = [f"key-{i}" for i in range(1000)]
    for k in keys:
        f.add(k)
    assert all(k in f for k in keys)


def test_bloom_filter_false_positive_rate_near_target():
    f = bloom.BloomFilter(capacity=1000, error_rate=0.01)
    for i in range(1000):
        f.add(f"in-{i}")
    false_positives = sum(f"out-{i}" in f for i in range(20000))
    assert false_positives / 20000 < 0.02


def test_healthy_filter_drops_info_and_benign_warnings_only():
    healthy = bloom.build_healthy_filter()
    rng = random.Random(1)
    assert bloom.is_healthy(healthy, "INFO", "GET /api/v1/orders 200 in 12ms")
    assert bloom.is_healthy(healthy, "WARN", templates.benign_warning("OrderAPI", rng))
    for service in ("UserDB", "AuthAPI", "PaymentGateway"):
        level, message, _ = templates.root_failure(service, rng)
        assert not bloom.is_healthy(healthy, level, message)


def test_blast_radius_walks_up_the_dependency_graph():
    affected = dict(topology.blast_radius("UserDB"))
    assert affected["AuthAPI"] == "UserDB"
    assert set(affected) == {"AuthAPI", "APIGateway", "OrderAPI", "WebFrontend", "MobileApp"}
