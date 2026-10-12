"""MongoDB handle (syllabus module 3, document store)."""

from pymongo import ASCENDING, DESCENDING, MongoClient

from logiq import config

_client = None


def db():
    global _client
    if _client is None:
        _client = MongoClient(config.MONGO_URI, tz_aware=True)
    return _client[config.MONGO_DB]


def ensure_indexes() -> None:
    d = db()
    d.alerts.create_index([("status", ASCENDING), ("incident_id", ASCENDING), ("first_seen", ASCENDING)])
    d.alerts.create_index([("last_seen", DESCENDING)])
    d.incidents.create_index([("status", ASCENDING), ("created_at", DESCENDING)])
    d.pipeline_stats.create_index([("at", DESCENDING)])
