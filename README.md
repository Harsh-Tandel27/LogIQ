# LogIQ

**A distributed big data framework for smart application monitoring and logging.**

When a large system crashes it produces thousands of near-identical error logs per second. LogIQ drops the healthy logs, merges the duplicates into a handful of alerts, grades each alert, traces the failure to the one service that caused it, and sends the engineer exactly one Slack message with a link to an incident report.

In the stress test (`make crash`), OrderDB fails and 4,782 error logs arrive from 5 services within a few seconds. LogIQ turns them into 9 alerts, 1 incident, and 1 Slack message that names OrderDB as the root cause.

## Architecture (Lambda)

```
producer/logger.py ──► Kafka ─┬─► spark/stream.py  "archive" query ──► HDFS  /logiq/raw/date=YYYY-MM-DD   (cold)
  (mock JSON logs,           │                                              │
   crash + backfill modes)   │                                              ▼
                             │                                spark/batch_mapreduce.py ──► MongoDB (history)
                             │
                             └─► spark/stream.py  "alerts" query, every 3 s                               (speed)
                                   1. Bloom filter drops healthy logs
                                   2. MapReduce: reduceByKey on (service, template)
                                   3. TF-IDF + cosine ≥ 0.90 merges similar templates / open alerts
                                   4. Logistic Regression (trained on real Loghub logs) grades High / Medium / Low
                                   5. upsert alerts ──► MongoDB
                                                           │
api/main.py (FastAPI) ◄────────────────────────────────────┘
  correlator loop, every 2 s:
    Neo4j traversal ──► root cause ──► 1 incident per root cause ──► 1 Slack message
    content-based recommender ──► suggested fixes
        │
        ▼
frontend/ (Next.js) /dashboard and /incident/[id]
```

## Syllabus mapping

| Module | Where |
|---|---|
| 2. MapReduce | `spark/stream.py` (`reduceByKey` on `(service, template)` each micro-batch) and `spark/batch_mapreduce.py` (errors per hour and service, levels per day, top templates over HDFS) |
| 3. NoSQL | MongoDB document store for alerts, incidents, stats, runbooks (`api/db.py`). Neo4j graph store for service dependencies (`api/graph.py`) |
| 4. Stream mining | Bloom filter in `logiq/bloom.py`, broadcast to Spark executors and applied as a UDF. It holds healthy levels (`INFO`, `DEBUG`, `TRACE`) and benign warning templates, sized for a 0.1% false-positive rate |
| 5. Distance measures and ML | TF-IDF vectors (MLlib `HashingTF` + `IDF`), L2-normalised so the dot product is cosine similarity; greedy leader clustering at 0.90. Severity classifier trained on 75,020 real log lines from Loghub: elastic-net Logistic Regression on word + bigram TF-IDF, chosen over Naive Bayes, Linear SVM, Random Forest and a neural network by grouped 5-fold cross-validation (`spark/prepare_dataset.py`, `spark/train_model.py`, `spark/evaluate_model.py`) |
| 6. Graphs and recommendations | Cypher traversal over `DEPENDS_ON` edges to find the deepest failing service (`api/graph.py`). Content-based recommendation of fixes: TF-IDF profiles of runbooks ranked by cosine similarity to the incident (`api/recommender.py`). Fixes typed when resolving an incident become new runbooks |

## Requirements

- Docker Desktop with about 4 GB of memory
- Python 3.13 (PySpark 4.0 does not support Python 3.14 yet)
- Java 17 or 21 (for Spark)
- Node.js 20+

## Setup

```bash
make install     # .venv + pip install + npm install
make infra       # Kafka, HDFS namenode + datanode, MongoDB, Neo4j
make setup       # Kafka topics, HDFS folder, Neo4j graph, download Loghub, train severity model
cp .env.example .env   # optional: paste your Slack webhook URL
```

Slack is optional. Without `SLACK_WEBHOOK_URL` the API logs the message it would have sent. To get a URL: create a Slack app, turn on Incoming Webhooks, add one for a channel.

## Run

Use one terminal per process:

```bash
make stream      # Spark streaming job
make api         # FastAPI on :8000
make web         # dashboard on http://localhost:3000
make logger      # steady traffic; type a service name + Enter to crash it
```

### Simulator page

http://localhost:3000/simulate sends logs into Kafka from the browser: start or stop background traffic, crash any service by clicking it on the dependency graph, or send your own log lines. The banner at the top lists every running log stream, including generators started from a terminal with `make logger`, and **Stop all streams** stops all of them within about a second. Terminal generators report a heartbeat to MongoDB and exit when a stop is requested.

### Stress test

```bash
make crash       # 2,000 OrderDB errors plus the cascade through OrderAPI, APIGateway and the clients
```

Watch the dashboard: the throughput chart spikes, the alerts table fills with the grouped alerts, and about 10 seconds later one incident appears. Open it for the dependency graph with the root cause highlighted, the recommended fix, and the resolve form.

### History charts

```bash
make backfill    # 7 days of logs with past timestamps to the logs.backfill topic (archived to HDFS only)
make batch       # MapReduce over HDFS -> MongoDB; wait ~30 s after backfill for the archive query to flush
```

### Tests

```bash
make test
```

## Design notes

- **One Slack message per incident.** Spark raises one alert per group of similar errors per service, so a single database failure produces alerts in every service above it. The correlator in `api/correlator.py` waits `CORRELATION_DELAY_SEC` (8 s) for the cascade, traces each alert through Neo4j, and files alerts that share a root cause under one incident. Only the incident sends a message.
- **Root cause rule.** Walk `DEPENDS_ON` edges down from the alerting service through services that are failing. The root cause is the deepest failing service whose own dependencies are all healthy.
- **What counts as failing.** Only alerts that are Medium or High *and* a burst (at least 25 logs at 0.5 logs/s or more). Sporadic bugs trickling in stay visible on the dashboard but never page anyone and never affect root-cause tracing.
- **Late root cause.** If errors from a deeper service arrive after an incident is opened, the incident is moved to the new root cause and Slack receives one update, instead of a second incident.
- **Clustering uses templates.** Tokens containing digits (ids, latencies, IPs, paths) are masked before TF-IDF, so `req_id=req-123` and `req_id=req-456` do not split a cluster.
- **Severity model and data.** Trained on real logs only: 75,020 labelled lines from Loghub (BGL, Hadoop, OpenStack, Zookeeper, Apache; Zhu et al., ISSRE 2023, doi:10.5281/zenodo.8196385). High means BGL administrator-tagged alerts or `FATAL`/`CRITICAL`. Medium means `ERROR`. Low means `WARN`. `make evaluate` compares five Spark MLlib classifiers with 5-fold cross-validation grouped by message family:

  | Model | Accuracy | Macro F1 |
  |---|---|---|
  | Logistic Regression (saved) | 0.847 | 0.713 |
  | Random Forest | 0.863 | 0.702 |
  | Neural network (MLP) | 0.808 | 0.700 |
  | Linear SVM | 0.805 | 0.646 |
  | Naive Bayes (old model) | 0.626 | 0.560 |

  Simulator messages come from a different domain, so the model mostly grades them Medium. The stream therefore never grades below the log level's floor (`ERROR` at least Medium, `FATAL` High). Pick another model with `SEVERITY_MODEL=random_forest make train`.
- **Differences from the original blueprint.** Kafka runs in KRaft mode, so there is no ZooKeeper. The API polls MongoDB every 2 s instead of using change streams, which would need a replica set.

## Project layout

```
config/topology.json     14 services and their DEPENDS_ON edges (shared by generator and Neo4j)
data/runbooks.json       seed knowledge base for fix recommendations
logiq/                   shared code: config, Bloom filter, log templates, template masking
producer/logger.py       Kafka log generator
spark/                   stream.py, batch_mapreduce.py, prepare_dataset.py, train_model.py, evaluate_model.py
api/                     FastAPI app, correlator, Neo4j, recommender, Slack
frontend/                Next.js 16 + Tailwind + Recharts + React Flow
tests/                   unit tests for the Bloom filter, template masking and topology
```

## Service UIs

- HDFS namenode: http://localhost:9870
- Neo4j browser: http://localhost:7474 (user `neo4j`, password `logiq-password`)
- API docs: http://localhost:8000/docs
