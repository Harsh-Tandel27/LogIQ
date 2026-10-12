"""Central settings. Values come from environment variables or a .env file in the repo root."""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv() -> None:
    env_file = ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()


def _get(name: str, default: str) -> str:
    return os.environ.get(name, default)


KAFKA_BOOTSTRAP = _get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = _get("KAFKA_TOPIC", "logs")
KAFKA_BACKFILL_TOPIC = _get("KAFKA_BACKFILL_TOPIC", "logs.backfill")

MONGO_URI = _get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = _get("MONGO_DB", "logiq")

NEO4J_URI = _get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = _get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = _get("NEO4J_PASSWORD", "logiq-password")

HDFS_URL = _get("HDFS_URL", "hdfs://localhost:8020")
RAW_LOG_PATH = _get("RAW_LOG_PATH", f"{HDFS_URL}/logiq/raw")

SLACK_WEBHOOK_URL = _get("SLACK_WEBHOOK_URL", "")
FRONTEND_URL = _get("FRONTEND_URL", "http://localhost:3000")

# Two error templates with cosine similarity at or above this value become one alert.
SIMILARITY_THRESHOLD = float(_get("SIMILARITY_THRESHOLD", "0.90"))
# An alert stays open (and absorbs new duplicates) while it saw an error in this window.
ALERT_WINDOW_SEC = int(_get("ALERT_WINDOW_SEC", "600"))
# The correlator waits this long after an alert first appears, so errors from
# the rest of the cascade can arrive before the root cause is computed.
CORRELATION_DELAY_SEC = int(_get("CORRELATION_DELAY_SEC", "8"))
# An incident with no new errors for this long is resolved automatically.
INCIDENT_QUIET_SEC = int(_get("INCIDENT_QUIET_SEC", "600"))

MODEL_PATH = _get("MODEL_PATH", str(ROOT / "models" / "severity_model"))
TRAINING_DATA = _get("TRAINING_DATA", str(ROOT / "data" / "severity_dataset"))
CHECKPOINT_DIR = _get("CHECKPOINT_DIR", str(ROOT / "checkpoints" / "stream"))
TOPOLOGY_FILE = ROOT / "config" / "topology.json"
RUNBOOK_FILE = ROOT / "data" / "runbooks.json"
