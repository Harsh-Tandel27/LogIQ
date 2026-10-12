"""Correlator against live MongoDB + Neo4j (docker compose up). Skipped when they are not running.

Uses a throwaway database and a local fake Slack webhook.
"""

import json
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from logiq import config

pymongo = pytest.importorskip("pymongo")


def _services_up() -> bool:
    try:
        pymongo.MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=500).admin.command("ping")
        from api import graph
        graph.driver().verify_connectivity()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _services_up(), reason="MongoDB or Neo4j not running")


@pytest.fixture
def slack(monkeypatch):
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(config, "SLACK_WEBHOOK_URL", f"http://127.0.0.1:{server.server_port}/hook")
    yield received
    server.shutdown()


@pytest.fixture
def mongo(monkeypatch):
    from api import recommender
    from api.db import db

    monkeypatch.setattr(config, "MONGO_DB", "logiq_test")
    assert db().name == "logiq_test"  # never drop the real database
    db().client.drop_database("logiq_test")
    recommender.seed_runbooks()
    yield db()
    db().client.drop_database("logiq_test")


def _alert(db, alert_id, service, severity, count, age_sec):
    seen = datetime.now(timezone.utc) - timedelta(seconds=age_sec)
    db.alerts.insert_one({
        "_id": alert_id, "service": service, "severity": severity, "count": count,
        "message": f"{service} broke", "template": f"{service} broke", "variants": [],
        "first_seen": seen, "last_seen": seen, "status": "open", "incident_id": None,
    })


def test_cascade_pages_once_then_moves_to_deeper_root_then_resolves(mongo, slack):
    from api import correlator

    _alert(mongo, "A1", "AuthAPI", "High", 500, 20)
    _alert(mongo, "A2", "APIGateway", "Medium", 300, 20)
    _alert(mongo, "A3", "OrderAPI", "Medium", 3, 20)  # a trickle: never pages, never counts as failing
    correlator.correlate_once()

    incidents = list(mongo.incidents.find())
    assert len(incidents) == 1
    assert incidents[0]["root_cause"] == "AuthAPI"
    assert incidents[0]["total_logs"] == 800
    assert len(slack) == 1
    assert slack[0]["blocks"][-1]["elements"][0]["url"].endswith("/incident/INC-1001")

    # Errors from a deeper dependency arrive late: same incident, new root, one update.
    _alert(mongo, "A4", "UserDB", "High", 800, 10)
    correlator.correlate_once()

    incidents = list(mongo.incidents.find())
    assert len(incidents) == 1
    assert incidents[0]["root_cause"] == "UserDB"
    assert incidents[0]["chain"] == ["APIGateway", "AuthAPI", "UserDB"]
    assert len(slack) == 2
    assert "moved from AuthAPI" in slack[1]["text"]

    # Quiet long enough: auto-resolved, alerts closed.
    mongo.alerts.update_many({}, {"$set": {"last_seen": datetime.now(timezone.utc) - timedelta(hours=1)}})
    correlator.housekeeping()
    assert mongo.incidents.find_one()["status"] == "resolved"
    assert mongo.alerts.count_documents({"status": "open"}) == 0
