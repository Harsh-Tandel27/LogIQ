"""A Bloom filter for dropping healthy log lines in the stream (syllabus module 4).

The filter holds the keys of messages we know are healthy: whole log levels
("level:INFO") and templates of benign warnings ("WARN|Slow query took <*>").
Lookups never give false negatives. False positives (a real error dropped) happen
with probability `error_rate`, so it is set low.
"""

import hashlib
import math
import random

from logiq import templates, topology
from logiq.textnorm import template


class BloomFilter:
    def __init__(self, capacity: int, error_rate: float = 0.001):
        self.size = max(8, int(-capacity * math.log(error_rate) / (math.log(2) ** 2)))
        self.hash_count = max(1, round(self.size / capacity * math.log(2)))
        self.bits = bytearray((self.size + 7) // 8)
        self.count = 0

    def _positions(self, key: str):
        digest = hashlib.blake2b(key.encode(), digest_size=16).digest()
        h1 = int.from_bytes(digest[:8], "big")
        h2 = int.from_bytes(digest[8:], "big") | 1
        # Kirsch-Mitzenmacher double hashing: k positions from two hashes.
        for i in range(self.hash_count):
            yield (h1 + i * h2) % self.size

    def add(self, key: str) -> None:
        for pos in self._positions(key):
            self.bits[pos >> 3] |= 1 << (pos & 7)
        self.count += 1

    def __contains__(self, key: str) -> bool:
        return all(self.bits[pos >> 3] & (1 << (pos & 7)) for pos in self._positions(key))


HEALTHY_LEVELS = ("TRACE", "DEBUG", "INFO")


def level_key(level: str) -> str:
    return f"level:{level}"


def template_key(level: str, message: str) -> str:
    return f"{level}|{template(message)}"


def is_healthy(bloom: BloomFilter, level: str, message: str) -> bool:
    return level_key(level) in bloom or template_key(level, message) in bloom


def build_healthy_filter(samples_per_template: int = 40, seed: int = 7) -> BloomFilter:
    """Seed the filter with every healthy level and every benign warning template."""
    rng = random.Random(seed)
    bloom = BloomFilter(capacity=5000, error_rate=0.001)
    for level in HEALTHY_LEVELS:
        bloom.add(level_key(level))
    for service in topology.services():
        for _ in range(samples_per_template):
            for text in templates.benign_warnings(service["name"], rng):
                bloom.add(template_key("WARN", text))
    return bloom
