PY := .venv/bin/python

.PHONY: install infra setup dataset train evaluate stream api web logger crash backfill batch test down

install:            ## Python venv (3.13; PySpark 4.0 does not support 3.14) + frontend deps
	python3.13 -m venv .venv
	.venv/bin/pip install -r requirements.txt
	cd frontend && npm install

infra:              ## Kafka, HDFS, MongoDB, Neo4j in Docker
	docker compose up -d

setup:              ## Kafka topics, HDFS folder, Neo4j graph, severity model
	docker exec logiq-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic logs --partitions 3 --replication-factor 1
	docker exec logiq-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic logs.backfill --partitions 3 --replication-factor 1
	docker exec logiq-namenode hdfs dfs -mkdir -p /logiq/raw
	$(PY) -m api.graph
	$(PY) -m spark.prepare_dataset
	$(PY) -m spark.train_model

dataset:            ## download Loghub (real system logs, ~70 MB) and build the labelled training set
	$(PY) -m spark.prepare_dataset

train:              ## fit the severity model on the Loghub training set
	$(PY) -m spark.train_model

evaluate:           ## compare classifiers with grouped 5-fold CV -> models/metrics.json
	$(PY) -m spark.evaluate_model

stream:             ## Spark: Kafka -> Bloom -> TF-IDF clustering -> severity model -> MongoDB, raw -> HDFS
	$(PY) -m spark.stream

api:                ## FastAPI + correlator (Neo4j root cause, Slack)
	.venv/bin/uvicorn api.main:app --port 8000

web:                ## Next.js dashboard on :3000
	cd frontend && npm run dev

logger:             ## steady traffic; type a service name + Enter to crash it
	$(PY) -m producer.logger

crash:              ## the stress test: 2,000 errors from OrderDB plus the cascade
	$(PY) -m producer.logger --crash OrderDB --crash-size 2000 --duration 90

backfill:           ## a week of history for the batch layer
	$(PY) -m producer.logger --backfill-days 7

batch:              ## MapReduce over HDFS -> history charts
	$(PY) -m spark.batch_mapreduce

test:
	$(PY) -m pytest -q tests

down:
	docker compose down
