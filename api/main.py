"""LogIQ API: serves the dashboard and runs the incident correlator.

    uvicorn api.main:app --reload --port 8000
"""

import asyncio
import logging
import socket
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka.errors import KafkaError
from pydantic import BaseModel, Field

from api import correlator, graph, recommender
from api.db import db, ensure_indexes
from api.simulator import simulator
from logiq import config, topology

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("logiq.api")


async def _correlator_loop() -> None:
    tick = 0
    while True:
        try:
            await asyncio.to_thread(correlator.correlate_once)
            if tick % 5 == 0:
                await asyncio.to_thread(correlator.housekeeping)
        except Exception:
            log.exception("Correlator pass failed")
        tick += 1
        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(_: FastAPI):
    ensure_indexes()
    recommender.seed_runbooks()
    task = asyncio.create_task(_correlator_loop())
    yield
    task.cancel()
    simulator.shutdown()


app = FastAPI(title="LogIQ API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=[config.FRONTEND_URL], allow_methods=["*"], allow_headers=["*"])


def _clean(doc: dict) -> dict:
    doc["id"] = doc.pop("_id")
    return doc


@app.get("/api/health")
def health() -> dict:
    return {"ok": True}


def _tcp_open(host_port: str, timeout: float = 0.5) -> bool:
    host, port = host_port.rsplit(":", 1)
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def _ago(seconds: float) -> str:
    if seconds < 90:
        return f"{int(seconds)}s ago"
    if seconds < 5400:
        return f"{int(seconds // 60)}m ago"
    return f"{int(seconds // 3600)}h ago"


@app.get("/api/components")
def components() -> list[dict]:
    """Status of each piece of the pipeline, for the sidebar."""
    out = []
    out.append({"name": "Kafka", "ok": _tcp_open(config.KAFKA_BOOTSTRAP)})
    last = db().pipeline_stats.find_one({}, {"at": 1}, sort=[("at", -1)])
    age = (datetime.now(timezone.utc) - last["at"]).total_seconds() if last else None
    # Spark skips empty micro-batches, so a quiet stream is not a dead one: check the driver UI port.
    out.append({"name": "Spark stream", "ok": _tcp_open("localhost:4040"),
                "detail": f"last batch {_ago(age)}" if age is not None else "no batches yet"})
    namenode = config.HDFS_URL.split("//", 1)[-1].split(":")[0]
    out.append({"name": "HDFS", "ok": _tcp_open(f"{namenode}:9870")})
    try:
        db().command("ping")
        out.append({"name": "MongoDB", "ok": True})
    except Exception:
        out.append({"name": "MongoDB", "ok": False})
    try:
        graph.driver().verify_connectivity()
        out.append({"name": "Neo4j", "ok": True})
    except Exception:
        out.append({"name": "Neo4j", "ok": False})
    out.append({"name": "Slack", "ok": bool(config.SLACK_WEBHOOK_URL), "detail": None if config.SLACK_WEBHOOK_URL else "webhook not set"})
    return out


@app.get("/api/overview")
def overview() -> dict:
    now = datetime.now(timezone.utc)
    totals = db().pipeline_totals.find_one({"_id": "all"}) or {}
    received = totals.get("received", 0)
    alerts_created = totals.get("alerts_created", 0)
    recent = list(db().pipeline_stats.find(
        {"at": {"$gte": now - timedelta(minutes=10)}},
        {"_id": 0, "at": 1, "received": 1, "bloom_dropped": 1, "errors": 1, "alerts_created": 1},
    ).sort("at", 1))
    return {
        "totals": {
            "received": received,
            "bloom_dropped": totals.get("bloom_dropped", 0),
            "errors": totals.get("errors", 0),
            "alerts": alerts_created,
            "incidents": db().incidents.count_documents({}),
            "open_incidents": db().incidents.count_documents({"status": "open"}),
            # How many raw logs, on average, sit behind each alert an engineer sees.
            "logs_per_alert": round(totals.get("errors", 0) / alerts_created, 1) if alerts_created else None,
        },
        "throughput": [{**r, "at": r["at"].isoformat()} for r in recent],
        "incidents": [_clean(i) for i in db().incidents.find(
            {}, {"timeline": 0, "recommendations": 0}).sort("created_at", -1).limit(20)],
        "alerts": [_clean(a) for a in db().alerts.find({"status": "open"}).sort("last_seen", -1).limit(200)],
    }


@app.get("/api/incidents/{incident_id}")
def incident_detail(incident_id: str) -> dict:
    incident = db().incidents.find_one({"_id": incident_id})
    if incident is None:
        raise HTTPException(404, f"Incident {incident_id} not found")
    alerts = [_clean(a) for a in db().alerts.find({"incident_id": incident_id}).sort("count", -1)]
    topology = graph.full_graph()
    affected = set(incident["affected_services"])
    for node in topology["nodes"]:
        if node["name"] == incident["root_cause"]:
            node["status"] = "root"
        elif node["name"] in affected:
            node["status"] = "affected"
        else:
            node["status"] = "healthy"
    chain = incident.get("chain", [])
    path_edges = set(zip(chain, chain[1:]))
    for edge in topology["edges"]:
        edge["on_path"] = (edge["source"], edge["target"]) in path_edges
    return {"incident": _clean(incident), "alerts": alerts, "graph": topology}


class ResolveRequest(BaseModel):
    fix: str | None = None
    title: str | None = None


@app.post("/api/incidents/{incident_id}/resolve")
def resolve_incident(incident_id: str, body: ResolveRequest) -> dict:
    fix = (body.fix or "").strip() or None
    incident = correlator.resolve(incident_id, note="Resolved by engineer", fix=fix, title=body.title)
    if incident is None:
        raise HTTPException(409, f"Incident {incident_id} is not open")
    return {"ok": True, "saved_runbook": fix is not None}


@app.get("/api/history")
def history(days: int = 7) -> dict:
    """Aggregates written by the MapReduce batch job (spark/batch_mapreduce.py)."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    return {
        "hourly": list(db().error_stats_hourly.find({"hour": {"$gte": since}}, {"_id": 0}).sort("hour", 1)),
        "by_service": list(db().error_stats_service.find({}, {"_id": 0}).sort("errors", -1)),
        "levels": list(db().level_stats_daily.find({"day": {"$gte": since}}, {"_id": 0}).sort("day", 1)),
        "top_templates": list(db().top_templates.find({}, {"_id": 0}).sort("count", -1)),
        "last_run": db().batch_runs.find_one({}, {"_id": 0}, sort=[("ran_at", -1)]),
    }


@app.get("/api/graph")
def service_graph() -> dict:
    return graph.full_graph()


# ---- simulator: send logs into Kafka from the dashboard ------------------

SERVICE_NAMES = {s["name"] for s in topology.services()}
LEVELS = {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}


def _check_service(name: str) -> None:
    if name not in SERVICE_NAMES:
        raise HTTPException(422, f"Unknown service {name}. Pick one of: {', '.join(sorted(SERVICE_NAMES))}")


def _kafka_call(fn, *args):
    try:
        return fn(*args)
    except KafkaError as exc:
        raise HTTPException(503, f"Kafka is not reachable at {config.KAFKA_BOOTSTRAP}: {exc}") from exc


class TrafficRequest(BaseModel):
    running: bool
    rate: int = Field(200, ge=0, le=2000)
    error_ratio: float = Field(0.01, ge=0, le=1)


class CrashRequest(BaseModel):
    service: str
    size: int = Field(1000, ge=10, le=10000)


class LogRequest(BaseModel):
    service: str
    level: str = "ERROR"
    message: str = Field(min_length=1, max_length=500)
    repeat: int = Field(1, ge=1, le=5000)
    vary_ids: bool = False


@app.get("/api/simulator")
def simulator_state() -> dict:
    return simulator.state(db())


@app.post("/api/simulator/traffic")
def simulator_traffic(body: TrafficRequest) -> dict:
    _kafka_call(simulator.set_traffic, body.running, body.rate, body.error_ratio)
    return simulator.state(db())


@app.post("/api/simulator/stop-all")
def simulator_stop_all() -> dict:
    """Stop dashboard traffic and ask every terminal log generator to exit (within ~1 s)."""
    return {"terminal_generators_stopped": simulator.stop_all(db())}


@app.post("/api/simulator/crash")
def simulator_crash(body: CrashRequest) -> dict:
    _check_service(body.service)
    return _kafka_call(simulator.crash, body.service, body.size)


@app.post("/api/simulator/logs")
def simulator_logs(body: LogRequest) -> dict:
    _check_service(body.service)
    if body.level not in LEVELS:
        raise HTTPException(422, f"Level must be one of {', '.join(sorted(LEVELS))}")
    sent = _kafka_call(simulator.custom, body.service, body.level, body.message.strip(), body.repeat, body.vary_ids)
    return {"sent": sent}
