"""Lets the dashboard see and stop log generators started from a terminal.

Each running `producer.logger` process writes a heartbeat document to MongoDB
every second and checks for a stop request. The API lists live heartbeats and
writes the stop request.
"""

import os
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from logiq import config

COLLECTION = "stream_control"
STOP_ID = "stop"
ALIVE_SEC = 5


def _now() -> datetime:
    return datetime.now(timezone.utc)


class GeneratorHeartbeat:
    """Used by a terminal log generator. Never raises: MongoDB being down must not stop the generator."""

    def __init__(self, mode: str, rate: int):
        self.pid = os.getpid()
        self.started_at = _now()
        self.mode = mode
        self.rate = rate
        self.enabled = True
        try:
            client = MongoClient(config.MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=1000)
            self.col = client[config.MONGO_DB][COLLECTION]
        except PyMongoError:
            self.enabled = False

    def beat(self, sent: int) -> bool:
        """Record progress. Returns True when the dashboard asked this generator to stop."""
        if not self.enabled:
            return False
        try:
            self.col.update_one(
                {"_id": f"cli-{self.pid}"},
                {"$set": {"pid": self.pid, "mode": self.mode, "rate": self.rate, "sent": sent,
                          "started_at": self.started_at, "last_beat": _now()}},
                upsert=True,
            )
            stop = self.col.find_one({"_id": STOP_ID})
            return bool(stop and stop["requested_at"] > self.started_at)
        except PyMongoError:
            print("[logger] MongoDB not reachable; the dashboard cannot see or stop this generator", flush=True)
            self.enabled = False
            return False

    def close(self) -> None:
        if self.enabled:
            try:
                self.col.delete_one({"_id": f"cli-{self.pid}"})
            except PyMongoError:
                pass


def running_generators(db) -> list[dict]:
    since = _now() - timedelta(seconds=ALIVE_SEC)
    return list(db[COLLECTION].find({"_id": {"$ne": STOP_ID}, "last_beat": {"$gte": since}}, {"_id": 0}))


def request_stop(db) -> None:
    db[COLLECTION].update_one({"_id": STOP_ID}, {"$set": {"requested_at": _now()}}, upsert=True)
