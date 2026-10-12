"""Service dependency map shared by the log generator and the Neo4j seeder."""

import json
from functools import lru_cache

from logiq.config import TOPOLOGY_FILE


@lru_cache(maxsize=1)
def load() -> dict:
    return json.loads(TOPOLOGY_FILE.read_text())


def services() -> list[dict]:
    return load()["services"]


def kind_of(name: str) -> str:
    return next(s["kind"] for s in services() if s["name"] == name)


def dependencies(name: str) -> list[str]:
    return [dep for caller, dep in load()["depends_on"] if caller == name]


def dependents(name: str) -> list[str]:
    """Services that call `name` directly."""
    return [caller for caller, dep in load()["depends_on"] if dep == name]


def blast_radius(name: str) -> list[tuple[str, str]]:
    """Every (service, failing_dependency) pair affected when `name` fails, nearest first."""
    seen = {name}
    frontier = [name]
    affected = []
    while frontier:
        nxt = []
        for failed in frontier:
            for caller in dependents(failed):
                if caller not in seen:
                    seen.add(caller)
                    affected.append((caller, failed))
                    nxt.append(caller)
        frontier = nxt
    return affected
