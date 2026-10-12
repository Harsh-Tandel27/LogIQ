"""Turn alerts into incidents: one incident, and one Slack message, per root cause.

Spark writes one alert per cluster of similar errors per service. When a database
fails, every service above it raises its own alerts. The correlator waits a few
seconds for that cascade to arrive, traces each alert down the Neo4j dependency
graph, and files all alerts that share a root cause under one incident.
"""

import logging
from datetime import datetime, timedelta, timezone

from pymongo import ReturnDocument

from api import graph, recommender, slack
from api.db import db
from logiq import config

log = logging.getLogger("logiq.correlator")

SEVERITY_RANK = {"Low": 0, "Medium": 1, "High": 2}
# An alert opens an incident only if it is a burst, not a slow trickle of sporadic errors.
MIN_LOGS = 25
MIN_RATE_PER_SEC = 0.5


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _next_incident_id() -> str:
    counter = db().counters.find_one_and_update(
        {"_id": "incident"}, {"$inc": {"seq": 1}}, upsert=True, return_document=ReturnDocument.AFTER
    )
    return f"INC-{1000 + counter['seq']}"


def _is_page_worthy(alert: dict) -> bool:
    if SEVERITY_RANK.get(alert["severity"], 0) < SEVERITY_RANK["Medium"]:
        return False
    span = max(60.0, (alert["last_seen"] - alert["first_seen"]).total_seconds())
    return alert["count"] >= MIN_LOGS and alert["count"] / span >= MIN_RATE_PER_SEC


def _failing_services(now: datetime) -> list[str]:
    """Services with a burst of errors right now. A trickle of sporadic errors does not count."""
    since = now - timedelta(seconds=config.ALERT_WINDOW_SEC)
    alerts = db().alerts.find({"status": "open", "last_seen": {"$gte": since}, "severity": {"$ne": "Low"}})
    return sorted({a["service"] for a in alerts if _is_page_worthy(a)})


def _refresh(incident_id: str) -> dict:
    """Recompute an incident's totals from its alerts."""
    alerts = list(db().alerts.find({"incident_id": incident_id}))
    incident = db().incidents.find_one({"_id": incident_id}, {"root_cause": 1, "affected_services": 1, "chain": 1})
    affected = sorted({a["service"] for a in alerts})
    chain = incident.get("chain", [])
    if affected != incident.get("affected_services"):
        # Show the longest failure path that ends at the root cause.
        for service in affected:
            traced = graph.find_root_cause(service, affected)
            if traced["root"] == incident["root_cause"] and len(traced["chain"]) > len(chain):
                chain = traced["chain"]
    update = {
        "chain": chain,
        "alert_ids": [a["_id"] for a in alerts],
        "affected_services": affected,
        "total_logs": sum(a["count"] for a in alerts),
        "severity": max((a["severity"] for a in alerts), key=SEVERITY_RANK.get, default="Low"),
        "last_error_at": max((a["last_seen"] for a in alerts), default=None),
    }
    return db().incidents.find_one_and_update(
        {"_id": incident_id}, {"$set": update}, return_document=ReturnDocument.AFTER
    )


def _recommendations(root: str, incident_id: str) -> list[dict]:
    # Only the root service's own errors describe the fault; the rest are echoes of it.
    messages = [a["message"] for a in db().alerts.find({"incident_id": incident_id, "service": root}, {"message": 1})]
    return recommender.recommend(root, messages)


def _attach(alert: dict, incident_id: str, now: datetime) -> None:
    db().alerts.update_one({"_id": alert["_id"]}, {"$set": {"incident_id": incident_id}})
    alert["incident_id"] = incident_id
    db().incidents.update_one(
        {"_id": incident_id},
        {"$push": {"timeline": {"at": now, "event": f"{alert['service']} alert grouped ({alert['count']} logs)"}}},
    )


def _open_incident(alert: dict, root: dict, now: datetime) -> dict:
    incident_id = _next_incident_id()
    db().incidents.insert_one({
        "_id": incident_id,
        "root_cause": root["root"],
        "chain": root["chain"],
        "status": "open",
        "created_at": now,
        "alert_ids": [],
        "affected_services": [],
        "total_logs": 0,
        "severity": alert["severity"],
        "recommendations": [],
        "timeline": [{"at": now, "event": f"Incident opened: root cause traced to {root['root']}"}],
    })
    return db().incidents.find_one({"_id": incident_id})


def correlate_once() -> None:
    now = _now()
    ready = now - timedelta(seconds=config.CORRELATION_DELAY_SEC)
    pending = [a for a in db().alerts.find(
        {"status": "open", "incident_id": None, "first_seen": {"$lte": ready}}
    ).sort("first_seen", 1) if _is_page_worthy(a)]
    if not pending:
        return

    failing = _failing_services(now)
    new_incidents: list[str] = []
    for alert in pending:
        root = graph.find_root_cause(alert["service"], failing)
        incident = db().incidents.find_one({"status": "open", "root_cause": root["root"]})
        if incident is None:
            # An open incident may have been traced to a service that turns out to
            # depend on this deeper root. Move it rather than paging twice.
            for candidate in db().incidents.find({"status": "open"}):
                if graph.depends_on(candidate["root_cause"], root["root"]):
                    previous = candidate["root_cause"]
                    chain = graph.find_root_cause(previous, failing)["chain"]
                    db().incidents.update_one({"_id": candidate["_id"]}, {
                        "$set": {"root_cause": root["root"], "chain": chain},
                        "$push": {"timeline": {"at": now, "event": f"Root cause moved from {previous} to {root['root']}"}},
                    })
                    _attach(alert, candidate["_id"], now)
                    refreshed = _refresh(candidate["_id"])
                    db().incidents.update_one({"_id": candidate["_id"]}, {
                        "$set": {"recommendations": _recommendations(root["root"], candidate["_id"])}})
                    slack.notify_root_cause_changed(refreshed, previous)
                    incident = refreshed
                    break
        if incident is None:
            incident = _open_incident(alert, root, now)
            new_incidents.append(incident["_id"])
        if alert.get("incident_id") is None:
            _attach(alert, incident["_id"], now)
        _refresh(incident["_id"])

    # Page once per new incident, after every alert in this pass is attached.
    for incident_id in new_incidents:
        incident = db().incidents.find_one({"_id": incident_id})
        recs = _recommendations(incident["root_cause"], incident_id)
        incident = db().incidents.find_one_and_update(
            {"_id": incident_id}, {"$set": {"recommendations": recs}}, return_document=ReturnDocument.AFTER)
        sent = slack.notify_incident(incident)
        event = "Slack notified" if sent else "Slack not configured, notification logged only"
        db().incidents.update_one({"_id": incident_id}, {
            "$set": {"notified_at": now, "slack_sent": sent},
            "$push": {"timeline": {"at": _now(), "event": event}},
        })
        log.info("Opened %s: root cause %s, %d logs", incident_id, incident["root_cause"], incident["total_logs"])


def housekeeping() -> None:
    """Refresh live totals, close quiet alerts and auto-resolve quiet incidents."""
    now = _now()
    for incident in db().incidents.find({"status": "open"}, {"_id": 1}):
        refreshed = _refresh(incident["_id"])
        last = refreshed.get("last_error_at")
        if last and last < now - timedelta(seconds=config.INCIDENT_QUIET_SEC):
            resolve(refreshed["_id"], note="Auto-resolved: no new errors")
    db().alerts.update_many(
        {"status": "open", "last_seen": {"$lt": now - timedelta(seconds=config.ALERT_WINDOW_SEC)}},
        {"$set": {"status": "closed"}},
    )


def resolve(incident_id: str, note: str = "Resolved", fix: str | None = None, title: str | None = None) -> dict | None:
    now = _now()
    incident = db().incidents.find_one_and_update(
        {"_id": incident_id, "status": "open"},
        {"$set": {"status": "resolved", "resolved_at": now, "resolution": fix or note},
         "$push": {"timeline": {"at": now, "event": note}}},
        return_document=ReturnDocument.AFTER,
    )
    if incident is None:
        return None
    db().alerts.update_many({"incident_id": incident_id}, {"$set": {"status": "closed"}})
    if fix:
        # The fix becomes a runbook, so the next similar incident gets it as a recommendation.
        messages = [a["message"] for a in db().alerts.find({"incident_id": incident_id}, {"message": 1})]
        db().runbooks.insert_one({
            "_id": f"RB-{incident_id}",
            "service": incident["root_cause"],
            "title": title or f"{incident['root_cause']} failure ({incident_id})",
            "symptoms": " ".join(messages),
            "fix": fix,
            "source": "incident",
        })
    return incident
